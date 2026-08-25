pub mod commands;
mod constants;
mod crypto;
mod models;
pub mod storage;
mod sync;

use crate::constants::TIMEOUT_GRACEFUL_SHUTDOWN_DURATION;
use ::automerge::transaction::Transactable;
use ::automerge::{Automerge, ObjType};
use std::sync::{Arc, Mutex};
use storage::SqliteStorage;
use tauri::Manager;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[cfg(not(debug_assertions))]
use tracing::info;

pub struct SyncEngineState {
    pub doc_changed_token: watch::Sender<()>,
    pub reconnect_token: watch::Sender<()>,
    pub cancel_token: CancellationToken,
    pub finished_token: CancellationToken,
    pub status: Arc<Mutex<models::SyncStatus>>,
}

pub struct AppState {
    pub doc: Arc<tokio::sync::RwLock<Automerge>>,
    pub todos: Arc<tokio::sync::RwLock<models::TodoDoc>>,
    pub storage: Arc<SqliteStorage>,
    pub sync_engine: SyncEngineState,
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
                state.sync_engine.cancel_token.cancel();
                tokio::select! {
                    _ = state.sync_engine.finished_token.cancelled() => {}
                    _ = tokio::time::sleep(TIMEOUT_GRACEFUL_SHUTDOWN_DURATION) => {}
                }
                let _ = window.destroy();
            });
        }
    });

    app_builder
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_os::init())
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

            // Local at-rest storage holds the raw automerge document: plaintext
            // structure with per-value ciphertext. Whole-document encryption is no
            // longer applied since the sensitive values are already encrypted.
            let stored_data = tauri::async_runtime::block_on(storage.load())
                .expect("failed to load data from storage");

            let doc = match stored_data {
                Some(data) => Automerge::load(&data).expect("failed to load doc"),
                None => {
                    let mut doc = Automerge::new();
                    let mut tx = doc.transaction();
                    tx.put_object(::automerge::ROOT, "todos", ObjType::List)
                        .expect("failed to initialize todos list in automerge doc");
                    tx.commit();
                    doc
                }
            };
            let initial_todos: models::TodoDoc = autosurgeon::hydrate(&doc)
                .expect("failed to hydrate initial todos from doc");
            let todos = Arc::new(tokio::sync::RwLock::new(initial_todos));
            let doc = Arc::new(tokio::sync::RwLock::new(doc));

            let (sync_engine_doc_changed_token, doc_changed_token) = watch::channel(());
            let (sync_engine_reconnect_token, reconnect_token) = watch::channel(());
            let sync_engine_cancel_token = CancellationToken::new();
            let sync_engine_finished_token = CancellationToken::new();
            let sync_engine_status = Arc::new(Mutex::new(models::SyncStatus::Connecting));

            let fin_token = sync_engine_finished_token.clone();
            let c_token = sync_engine_cancel_token.clone();
            let doc_clone = doc.clone();
            let todos_clone = todos.clone();
            let storage_clone = storage.clone();
            let sync_engine_status_clone = sync_engine_status.clone();
            let app_handle = app.handle().clone();

            tauri::async_runtime::spawn(async move {
                sync::sync_engine(
                    doc_clone,
                    todos_clone,
                    doc_changed_token,
                    reconnect_token,
                    app_handle,
                    storage_clone,
                    sync_engine_status_clone,
                    c_token,
                )
                .await;
                fin_token.cancel();
            });

            app.manage(AppState {
                doc,
                todos,
                storage,
                sync_engine: SyncEngineState {
                    doc_changed_token: sync_engine_doc_changed_token,
                    reconnect_token: sync_engine_reconnect_token,
                    cancel_token: sync_engine_cancel_token,
                    finished_token: sync_engine_finished_token,
                    status: sync_engine_status,
                },
            });

            Ok(())
        })
        .invoke_handler(builder.invoke_handler())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
