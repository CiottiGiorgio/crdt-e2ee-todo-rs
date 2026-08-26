use crate::models::{SyncStatus, TodoItem, TodoStatus};
use crate::AppState;
use tauri::State;
use tauri_specta::{collect_commands, Builder};
use uuid::Uuid;

#[tauri::command]
#[specta::specta]
pub async fn get_sync_status(state: State<'_, AppState>) -> Result<SyncStatus, String> {
    Ok(*state
        .sync_engine
        .status
        .lock()
        .expect("sync status' lock poisoned"))
}

#[tauri::command]
#[specta::specta]
pub async fn get_todos_by_status(
    status: TodoStatus,
    state: State<'_, AppState>,
) -> Result<Vec<TodoItem>, String> {
    Ok(state
        .doc_manager
        .apply(|doc| doc.get_todos_by_status(status))
        .await)
}

#[tauri::command]
#[specta::specta]
pub async fn add_todo(text: String, state: State<'_, AppState>) -> Result<TodoItem, String> {
    let item = TodoItem {
        id: Uuid::new_v4().to_string(),
        text,
        status: TodoStatus::Todo,
    };
    let item_clone = item.clone();
    state
        .doc_manager
        .apply_mut(move |doc| {
            doc.add_todo(item_clone);
        })
        .await
        .map_err(|e| e.to_string())?;
    Ok(item)
}

#[tauri::command]
#[specta::specta]
pub async fn update_todo_status(
    id: String,
    status: TodoStatus,
    state: State<'_, AppState>,
) -> Result<(), String> {
    state
        .doc_manager
        .apply_mut(move |doc| doc.update_todo_status(&id, status))
        .await
        .map_err(|e| e.to_string())?
}

#[tauri::command]
#[specta::specta]
pub async fn delete_todo(id: String, state: State<'_, AppState>) -> Result<(), String> {
    state
        .doc_manager
        .apply_mut(move |doc| doc.delete_todo(&id))
        .await
        .map_err(|e| e.to_string())?
}

#[tauri::command]
#[specta::specta]
pub async fn manual_reconnect(state: State<'_, AppState>) -> Result<(), String> {
    state
        .sync_engine
        .reconnect_token
        .send(())
        .map_err(|err| err.to_string())
}

pub fn get_specta_builder() -> Builder<tauri::Wry> {
    Builder::<tauri::Wry>::new().commands(collect_commands![
        get_sync_status,
        get_todos_by_status,
        add_todo,
        update_todo_status,
        delete_todo,
        manual_reconnect
    ])
}
