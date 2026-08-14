use std::sync::Arc;

#[path = "src/models.rs"]
mod models;

use models::{TodoItem, TodoStatus};

#[async_trait::async_trait]
pub trait TodoRepository: Send + Sync {
    async fn get_all(&self) -> Result<Vec<TodoItem>, String>;
    async fn add(&self, text: String) -> Result<TodoItem, String>;
    async fn update_status(&self, id: String, status: TodoStatus) -> Result<(), String>;
    async fn delete(&self, id: String) -> Result<(), String>;
}

pub struct AppState {
    pub todo_repo: Arc<dyn TodoRepository>,
}

#[path = "src/commands.rs"]
mod commands;

fn main() {
    println!("cargo:rerun-if-changed=src/commands.rs");
    println!("cargo:rerun-if-changed=src/models.rs");

    let builder = commands::get_specta_builder();
    builder
        .export(
            specta_typescript::Typescript::default(),
            "../client/src/lib/bindings.ts",
        )
        .expect("Failed to export specta typescript bindings");

    tauri_build::build();
}
