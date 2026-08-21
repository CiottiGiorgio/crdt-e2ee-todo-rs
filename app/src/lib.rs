mod automerge;
pub mod commands;
mod constants;
mod crypto;
mod models;
pub mod storage;
mod sync;

use automerge::AutomergeTodoRepo;
use crypto::CryptoEngine;
use std::sync::Arc;
use storage::SqliteStorage;
use tauri::Manager;

#[cfg(not(debug_assertions))]
use tracing::info;

pub struct AppState {
    pub repo: Arc<AutomergeTodoRepo>,
    pub storage: Arc<SqliteStorage>,
    pub crypto: Arc<CryptoEngine>,
    pub sync_tx: tokio::sync::mpsc::UnboundedSender<()>,
    pub sync_status: Arc<std::sync::RwLock<models::SyncStatus>>,
    pub sync_shutdown_tx: tokio::sync::watch::Sender<()>,
    pub sync_handle: Arc<tokio::sync::Mutex<Option<tauri::async_runtime::JoinHandle<()>>>>,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt::init();

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

    app_builder = app_builder.on_window_event(|window, event| {
        if let tauri::WindowEvent::CloseRequested { api, .. } = event {
            api.prevent_close();
            let window = window.clone();
            tauri::async_runtime::spawn(async move {
                let state = window.state::<AppState>();
                let _ = state.sync_shutdown_tx.send(());
                if let Some(handle) = state.sync_handle.lock().await.take() {
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
                }
                let _ = window.destroy();
            });
        }
    });

    app_builder
        .plugin(tauri_plugin_opener::init())
        .setup(move |app| {
            #[cfg(debug_assertions)]
            let storage = tauri::async_runtime::block_on(storage::SqliteStorage::in_memory())
                .expect("failed to initialize in-memory sqlite storage");

            #[cfg(not(debug_assertions))]
            let storage = {
                let app_data_dir = app
                    .path()
                    .app_data_dir()
                    .expect("failed to resolve app data directory");

                if !app_data_dir.exists() {
                    std::fs::create_dir_all(&app_data_dir)
                        .expect("failed to create app data directory");
                }

                let db_path = app_data_dir.join("storage.db");
                tauri::async_runtime::block_on(storage::SqliteStorage::from_path(db_path))
                    .expect("failed to initialize sqlite storage")
            };

            let storage = Arc::new(storage);

            // Shared E2EE Symmetric Key (32 bytes)
            let master_key = [42u8; constants::KEY_SIZE];
            let crypto = Arc::new(CryptoEngine::new(&master_key));

            // Local at-rest storage holds the raw automerge document: plaintext
            // structure with per-value ciphertext. Whole-document encryption is no
            // longer applied since the sensitive values are already encrypted.
            let stored_data = tauri::async_runtime::block_on(storage.load())
                .expect("failed to load data from storage");

            let repo = Arc::new(
                AutomergeTodoRepo::new(stored_data, crypto.clone())
                    .expect("failed to initialize automerge repository"),
            );

            let (sync_tx, sync_rx) = tokio::sync::mpsc::unbounded_channel();
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
            let sync_status = Arc::new(std::sync::RwLock::new(models::SyncStatus::Connecting));

            let sync_handle = tauri::async_runtime::spawn(sync::sync_engine(
                repo.clone(),
                storage.clone(),
                app.handle().clone(),
                sync_status.clone(),
                sync_rx,
                shutdown_rx,
            ));

            app.manage(AppState {
                repo,
                storage,
                crypto,
                sync_tx,
                sync_status,
                sync_shutdown_tx: shutdown_tx,
                sync_handle: Arc::new(tokio::sync::Mutex::new(Some(sync_handle))),
            });

            Ok(())
        })
        .invoke_handler(builder.invoke_handler())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
