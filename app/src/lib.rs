mod commands;
mod constants;
mod crypto;
mod models;
mod repository;
pub mod store;
mod sync;

use crypto::CryptoEngine;
use repository::automerge::AutomergeTodoRepo;
use repository::TodoRepository;
use std::sync::Arc;
use tauri::Manager;

#[cfg(not(debug_assertions))]
use tracing::info;

pub struct AppState {
    pub todo_repo: Arc<dyn TodoRepository>,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let _ = tracing_subscriber::fmt::try_init();

    let builder = commands::get_specta_builder();

    #[allow(unused_mut)]
    let mut app_builder = tauri::Builder::default();

    #[cfg(not(debug_assertions))]
    {
        app_builder = app_builder.plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            info!("Single instance signal received: bringing window to focus.");
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.set_focus();
            }
        }));
    }

    app_builder
        .plugin(tauri_plugin_opener::init())
        .setup(move |app| {
            #[cfg(debug_assertions)]
            let store = tauri::async_runtime::block_on(store::SqliteBackingStore::in_memory())
                .expect("failed to initialize in-memory sqlite backing store");

            #[cfg(not(debug_assertions))]
            let store = {
                let app_data_dir = app
                    .path()
                    .app_data_dir()
                    .expect("failed to resolve app data directory");

                if !app_data_dir.exists() {
                    std::fs::create_dir_all(&app_data_dir)
                        .expect("failed to create app data directory");
                }

                let db_path = app_data_dir.join("store.db");
                tauri::async_runtime::block_on(store::SqliteBackingStore::from_path(db_path))
                    .expect("failed to initialize sqlite backing store")
            };

            let repo = Arc::new(
                tauri::async_runtime::block_on(AutomergeTodoRepo::new(store))
                    .expect("failed to initialize automerge repository"),
            );

            // Shared E2EE Symmetric Key (32 bytes)
            let master_key = [42u8; 32];
            let crypto = Arc::new(CryptoEngine::new(&master_key));

            let sync_tx = sync::start_sync_worker(repo.clone(), crypto, app.handle().clone());
            repo.set_sync_notifier(sync_tx);

            app.manage(AppState { todo_repo: repo });

            Ok(())
        })
        .invoke_handler(builder.invoke_handler())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
