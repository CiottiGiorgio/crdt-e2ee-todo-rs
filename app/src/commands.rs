use crate::automerge::DecryptedView;
use crate::models::{SyncStatus, TodoItem, TodoStatus};
use crate::AppState;
use tauri::State;
use tauri_specta::{collect_commands, Builder};

#[tauri::command]
#[specta::specta]
pub async fn get_sync_status(state: State<'_, AppState>) -> Result<SyncStatus, String> {
    Ok(*state
        .sync_engine_status
        .lock()
        .expect("sync status' lock poisoned"))
}

#[tauri::command]
#[specta::specta]
pub async fn get_todos(state: State<'_, AppState>) -> Result<Vec<TodoItem>, String> {
    let view = DecryptedView::new(state.doc.clone(), state.crypto.clone());
    view.get_all().await
}

#[tauri::command]
#[specta::specta]
pub async fn add_todo(text: String, state: State<'_, AppState>) -> Result<TodoItem, String> {
    let view = DecryptedView::new(state.doc.clone(), state.crypto.clone());
    let item = view.add(text).await?;
    let doc_bytes = view.get_doc_bytes().await;
    state
        .storage
        .save(&doc_bytes)
        .await
        .map_err(|e| e.to_string())?;
    let _ = state.sync_engine_wake_up.send(());
    Ok(item)
}

#[tauri::command]
#[specta::specta]
pub async fn update_todo_status(
    id: String,
    status: TodoStatus,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let view = DecryptedView::new(state.doc.clone(), state.crypto.clone());
    view.update_status(id, status).await?;
    let doc_bytes = view.get_doc_bytes().await;
    state
        .storage
        .save(&doc_bytes)
        .await
        .map_err(|e| e.to_string())?;
    let _ = state.sync_engine_wake_up.send(());
    Ok(())
}

#[tauri::command]
#[specta::specta]
pub async fn delete_todo(id: String, state: State<'_, AppState>) -> Result<(), String> {
    let view = DecryptedView::new(state.doc.clone(), state.crypto.clone());
    view.delete(id).await?;
    let doc_bytes = view.get_doc_bytes().await;
    state
        .storage
        .save(&doc_bytes)
        .await
        .map_err(|e| e.to_string())?;
    let _ = state.sync_engine_wake_up.send(());
    Ok(())
}

pub fn get_specta_builder() -> Builder<tauri::Wry> {
    Builder::<tauri::Wry>::new().commands(collect_commands![
        get_sync_status,
        get_todos,
        add_todo,
        update_todo_status,
        delete_todo
    ])
}
