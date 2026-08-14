mod automerge;
pub mod commands;
mod constants;
mod crypto;
mod models;
pub mod store;
mod sync;

use automerge::AutomergeTodoRepo;
use crypto::CryptoEngine;
use std::sync::Arc;
use store::SqliteBackingStore;
use tauri::Manager;

#[cfg(not(debug_assertions))]
use tracing::info;

pub struct AppState {
    pub repo: Arc<AutomergeTodoRepo>,
    pub store: Arc<SqliteBackingStore>,
    pub crypto: Arc<CryptoEngine>,
    pub sync_tx: tokio::sync::mpsc::UnboundedSender<()>,
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

            let store = Arc::new(store);

            // Shared E2EE Symmetric Key (32 bytes)
            let master_key = [42u8; constants::KEY_SIZE];
            let crypto = Arc::new(CryptoEngine::new(&master_key));

            let encrypted_data = tauri::async_runtime::block_on(store.load())
                .expect("failed to load data from store");

            let decrypted_data = match encrypted_data {
                Some(data) => {
                    let payload: shared::EncryptedPayload =
                        serde_json::from_slice(&data).expect("failed to deserialize payload");
                    Some(crypto.decrypt(&payload).expect("failed to decrypt data"))
                }
                None => None,
            };

            let repo = Arc::new(
                AutomergeTodoRepo::new(decrypted_data)
                    .expect("failed to initialize automerge repository"),
            );

            let sync_tx = sync::start_sync_worker(
                repo.clone(),
                crypto.clone(),
                store.clone(),
                app.handle().clone(),
            );

            app.manage(AppState {
                repo,
                store,
                crypto,
                sync_tx,
            });

            Ok(())
        })
        .invoke_handler(builder.invoke_handler())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
