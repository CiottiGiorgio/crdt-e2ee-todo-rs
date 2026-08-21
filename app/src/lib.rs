mod automerge;
pub mod commands;
mod constants;
mod crypto;
mod models;
pub mod storage;
mod sync;

mod sync_reworked;

use crate::constants::TIMEOUT_GRACEFUL_SHUTDOWN_DURATION;
use ::automerge::AutoCommit;
use crypto::CryptoEngine;
use std::sync::Arc;
use storage::SqliteStorage;
use tauri::Manager;
use tokio_util::sync::CancellationToken;

#[cfg(not(debug_assertions))]
use tracing::info;

pub struct AppState {
    // FIXME: Because AutoCommit takes a mut ref to generate a sync message (it commits pending transctions),
    //  we need to acquire a write lock when syncing state with the server.
    //  This is not ideal so we should consider Automerge docs instead of AutoCommit docs.
    doc: Arc<tokio::sync::RwLock<AutoCommit>>,
    storage: Arc<SqliteStorage>,
    crypto: Arc<CryptoEngine>,
    sync_engine_wake_up: tokio::sync::watch::Sender<()>,
    sync_engine_cancel_token: CancellationToken,
    sync_engine_finished_token: CancellationToken,
    sync_engine_status: Arc<tokio::sync::RwLock<models::SyncStatus>>,
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
                state.sync_engine_cancel_token.cancel();
                tokio::select! {
                    _ = state.sync_engine_finished_token.cancelled() => {}
                    _ = tokio::time::sleep(TIMEOUT_GRACEFUL_SHUTDOWN_DURATION) => {}
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

            let doc = match stored_data {
                Some(data) => AutoCommit::load(&data).expect("failed to load doc"),
                None => AutoCommit::new(),
            };
            let doc = Arc::new(tokio::sync::RwLock::new(doc));

            let (sync_engine_wake_up, sync_engine_wake_up_rx) = tokio::sync::watch::channel(());
            let cancel_token = CancellationToken::new();
            let finished_token = CancellationToken::new();
            let sync_engine_status =
                Arc::new(tokio::sync::RwLock::new(models::SyncStatus::Connecting));

            let fin_token = finished_token.clone();
            let c_token = cancel_token.clone();
            let doc_clone = doc.clone();
            let storage_clone = storage.clone();

            tauri::async_runtime::spawn(async move {
                sync_reworked::sync_engine(
                    doc_clone,
                    storage_clone,
                    sync_engine_wake_up_rx,
                    c_token,
                )
                .await;
                fin_token.cancel();
            });

            app.manage(AppState {
                doc,
                storage,
                crypto,
                sync_engine_wake_up,
                sync_engine_cancel_token: cancel_token,
                sync_engine_finished_token: finished_token,
                sync_engine_status,
            });

            Ok(())
        })
        .invoke_handler(builder.invoke_handler())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
