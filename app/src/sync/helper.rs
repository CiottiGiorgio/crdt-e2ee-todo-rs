use crate::models::SyncStatus;
use std::sync::Mutex;
use tauri::Emitter;
use tracing::error;

/// Updates the shared sync status lock and emits a "sync-status" event to the UI.
pub fn update_sync_status(
    status: &Mutex<SyncStatus>,
    app_handle: &tauri::AppHandle,
    new_status: SyncStatus,
) {
    *status.lock().expect("sync status lock poisoned") = new_status;
    if let Err(e) = app_handle.emit("sync-status", new_status) {
        error!("Failed to emit sync-status event: {}", e);
    }
}

/// Emits a "todos-updated" event to the UI when new changes are received and applied from the server.
pub fn notify_todos_updated(app_handle: &tauri::AppHandle) {
    if let Err(e) = app_handle.emit("todos-updated", ()) {
        error!("Failed to emit todos-updated event: {}", e);
    }
}
