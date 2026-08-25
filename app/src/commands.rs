use crate::models::{SyncStatus, TodoDoc, TodoItem, TodoStatus};
use crate::AppState;
use autosurgeon::{hydrate, reconcile};
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
    let doc = state.doc.read().await;
    let todo_doc: TodoDoc = hydrate(&*doc).map_err(|err| err.to_string())?;
    Ok(todo_doc.todos)
}

#[tauri::command]
#[specta::specta]
pub async fn add_todo(text: String, state: State<'_, AppState>) -> Result<TodoItem, String> {
    let mut doc = state.doc.write().await;
    let mut tx = doc.transaction();

    let mut todo_doc: TodoDoc = hydrate(&tx).map_err(|e| e.to_string())?;
    let item = TodoItem {
        id: Uuid::new_v4().to_string(),
        text,
        status: TodoStatus::Todo,
    };
    todo_doc.todos.push(item.clone());

    reconcile(&mut tx, &todo_doc).map_err(|e| e.to_string())?;
    tx.commit();

    let doc_bytes = doc.save();
    state
        .storage
        .save(&doc_bytes)
        .await
        .map_err(|e| e.to_string())?;
    let _ = state.sync_engine.doc_changed_token.send(());
    Ok(item)
}

#[tauri::command]
#[specta::specta]
pub async fn update_todo_status(
    id: String,
    status: TodoStatus,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let mut doc = state.doc.write().await;
    let mut tx = doc.transaction();

    let mut todo_doc: TodoDoc = hydrate(&tx).map_err(|e| e.to_string())?;
    let item = todo_doc
        .todos
        .iter_mut()
        .find(|item| item.id == id)
        .ok_or_else(|| format!("Todo item with id {} not found", id))?;
    item.status = status;

    reconcile(&mut tx, &todo_doc).map_err(|e| e.to_string())?;
    tx.commit();

    let doc_bytes = doc.save();
    state
        .storage
        .save(&doc_bytes)
        .await
        .map_err(|e| e.to_string())?;
    let _ = state.sync_engine.doc_changed_token.send(());
    Ok(())
}

#[tauri::command]
#[specta::specta]
pub async fn delete_todo(id: String, state: State<'_, AppState>) -> Result<(), String> {
    let mut doc = state.doc.write().await;
    let mut tx = doc.transaction();

    let mut todo_doc: TodoDoc = hydrate(&tx).map_err(|e| e.to_string())?;
    let initial_len = todo_doc.todos.len();
    todo_doc.todos.retain(|item| item.id != id);

    if todo_doc.todos.len() == initial_len {
        return Err(format!("Todo item with id {} not found", id));
    }

    reconcile(&mut tx, &todo_doc).map_err(|e| e.to_string())?;
    tx.commit();

    let doc_bytes = doc.save();
    state
        .storage
        .save(&doc_bytes)
        .await
        .map_err(|e| e.to_string())?;
    let _ = state.sync_engine.doc_changed_token.send(());
    Ok(())
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
