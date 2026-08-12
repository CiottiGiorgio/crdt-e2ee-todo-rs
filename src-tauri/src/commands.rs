use crate::models::{TodoItem, TodoStatus};
use crate::AppState;
use tauri::State;
use tauri_specta::{collect_commands, Builder};

#[tauri::command]
#[specta::specta]
pub async fn get_todos(state: State<'_, AppState>) -> Result<Vec<TodoItem>, String> {
    state.todo_repo.get_all().await
}

#[tauri::command]
#[specta::specta]
pub async fn add_todo(text: String, state: State<'_, AppState>) -> Result<TodoItem, String> {
    state.todo_repo.add(text).await
}

#[tauri::command]
#[specta::specta]
pub async fn update_todo_status(
    id: String,
    status: TodoStatus,
    state: State<'_, AppState>,
) -> Result<(), String> {
    state.todo_repo.update_status(id, status).await
}

#[tauri::command]
#[specta::specta]
pub async fn delete_todo(id: String, state: State<'_, AppState>) -> Result<(), String> {
    state.todo_repo.delete(id).await
}

pub fn get_specta_builder() -> Builder<tauri::Wry> {
    Builder::<tauri::Wry>::new().commands(collect_commands![
        get_todos,
        add_todo,
        update_todo_status,
        delete_todo
    ])
}
