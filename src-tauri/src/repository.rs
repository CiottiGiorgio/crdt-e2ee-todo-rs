pub mod sqlite;

use crate::models::{TodoItem, TodoStatus};

#[async_trait::async_trait]
pub trait TodoRepository: Send + Sync {
    async fn get_all(&self) -> Result<Vec<TodoItem>, String>;
    async fn add(&self, text: String) -> Result<TodoItem, String>;
    async fn update_status(&self, id: i32, status: TodoStatus) -> Result<(), String>;
    async fn delete(&self, id: i32) -> Result<(), String>;
}
