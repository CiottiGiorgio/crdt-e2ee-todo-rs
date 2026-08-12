use serde::{Deserialize, Serialize};
use specta::Type;
use sqlx::FromRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type, sqlx::Type)]
#[serde(rename_all = "camelCase")]
#[sqlx(type_name = "TEXT", rename_all = "snake_case")]
pub enum TodoStatus {
    WorkingSet,
    Backlog,
    Completed,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct TodoItem {
    pub id: i32,
    pub text: String,
    pub status: TodoStatus,
}
