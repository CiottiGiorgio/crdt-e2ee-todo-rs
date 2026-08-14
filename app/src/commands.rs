use crate::models::{TodoItem, TodoStatus};
use crate::AppState;
use tauri::State;
use tauri_specta::{collect_commands, Builder};

#[tauri::command]
#[specta::specta]
pub async fn get_todos(state: State<'_, AppState>) -> Result<Vec<TodoItem>, String> {
    state.repo.get_all().await
}

#[tauri::command]
#[specta::specta]
pub async fn add_todo(text: String, state: State<'_, AppState>) -> Result<TodoItem, String> {
    let (item, doc_bytes) = state.repo.add(text).await?;
    let encrypted = state.crypto.encrypt(&doc_bytes)?;
    let encrypted_bytes = serde_json::to_vec(&encrypted).map_err(|e| e.to_string())?;
    state.store.save(&encrypted_bytes).await?;
    let _ = state.sync_tx.send(());
    Ok(item)
}

#[tauri::command]
#[specta::specta]
pub async fn update_todo_status(
    id: String,
    status: TodoStatus,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let doc_bytes = state.repo.update_status(id, status).await?;
    let encrypted = state.crypto.encrypt(&doc_bytes)?;
    let encrypted_bytes = serde_json::to_vec(&encrypted).map_err(|e| e.to_string())?;
    state.store.save(&encrypted_bytes).await?;
    let _ = state.sync_tx.send(());
    Ok(())
}

#[tauri::command]
#[specta::specta]
pub async fn delete_todo(id: String, state: State<'_, AppState>) -> Result<(), String> {
    let doc_bytes = state.repo.delete(id).await?;
    let encrypted = state.crypto.encrypt(&doc_bytes)?;
    let encrypted_bytes = serde_json::to_vec(&encrypted).map_err(|e| e.to_string())?;
    state.store.save(&encrypted_bytes).await?;
    let _ = state.sync_tx.send(());
    Ok(())
}

pub fn get_specta_builder() -> Builder<tauri::Wry> {
    Builder::<tauri::Wry>::new().commands(collect_commands![
        get_todos,
        add_todo,
        update_todo_status,
        delete_todo
    ])
}
