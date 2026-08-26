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
pub async fn get_todos(state: State<'_, AppState>) -> Result<Vec<TodoItem>, String> {
    Ok(state.doc_manager.apply(|todo| todo.todos.clone()).await)
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
            let todos = &mut doc.todos;
            todos.push(item_clone)
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
        .apply_mut(move |doc| {
            let todos = &mut doc.todos;
            let item = todos
                .iter_mut()
                .find(|item| item.id == id)
                .ok_or_else(|| format!("Todo item with id {} not found", id))?;
            item.status = status;
            Ok(())
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e: String| e)
}

#[tauri::command]
#[specta::specta]
pub async fn delete_todo(id: String, state: State<'_, AppState>) -> Result<(), String> {
    state
        .doc_manager
        .apply_mut(move |doc| {
            let todos = &mut doc.todos;
            let initial_len = todos.len();
            todos.retain(|item| item.id != id);
            if todos.len() == initial_len {
                Err(format!("Todo item with id {} not found", id))
            } else {
                Ok(())
            }
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e: String| e)
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
        get_todos,
        add_todo,
        update_todo_status,
        delete_todo,
        manual_reconnect
    ])
}
