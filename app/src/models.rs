use serde::{Deserialize, Serialize};
use specta::Type;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub enum TodoStatus {
    Todo,
    Archived,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid todo status: {0}")]
pub struct ParseTodoStatusError(pub String);

impl From<TodoStatus> for &'static str {
    fn from(status: TodoStatus) -> Self {
        match status {
            TodoStatus::Todo => "todo",
            TodoStatus::Archived => "archived",
            TodoStatus::Completed => "completed",
        }
    }
}

impl AsRef<str> for TodoStatus {
    fn as_ref(&self) -> &str {
        (*self).into()
    }
}

impl FromStr for TodoStatus {
    type Err = ParseTodoStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "todo" => Ok(TodoStatus::Todo),
            "archived" => Ok(TodoStatus::Archived),
            "completed" => Ok(TodoStatus::Completed),
            _ => Err(ParseTodoStatusError(s.to_string())),
        }
    }
}

impl TryFrom<&str> for TodoStatus {
    type Error = ParseTodoStatusError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct TodoItem {
    pub id: String,
    pub text: String,
    pub status: TodoStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum SyncStatus {
    Connecting,
    Connected,
    Disconnected,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_todo_status_to_str() {
        let s: &'static str = TodoStatus::Todo.into();
        assert_eq!(s, "todo");
        assert_eq!(<&str>::from(TodoStatus::Archived), "archived");
        assert_eq!(TodoStatus::Completed.as_ref(), "completed");
    }

    #[test]
    fn test_str_to_todo_status() {
        assert_eq!("todo".parse::<TodoStatus>(), Ok(TodoStatus::Todo));
        assert_eq!("archived".parse::<TodoStatus>(), Ok(TodoStatus::Archived));
        assert_eq!("completed".parse::<TodoStatus>(), Ok(TodoStatus::Completed));

        assert!("invalid".parse::<TodoStatus>().is_err());
        assert_eq!(
            TodoStatus::try_from("todo"),
            Ok(TodoStatus::Todo)
        );
    }
}
