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

use autosurgeon::{Hydrate, Reconcile};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, Hydrate, Reconcile)]
#[serde(rename_all = "camelCase")]
pub struct TodoItem {
    pub id: String,
    #[autosurgeon(with = "crate::crypto::encrypted_string")]
    pub text: String,
    #[autosurgeon(with = "crate::crypto::encrypted_status")]
    pub status: TodoStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hydrate, Reconcile)]
pub struct  TodoDoc {
    pub todos: Vec<TodoItem>,
}

impl TodoDoc {
    pub fn get_todos(&self) -> Vec<TodoItem> {
        self.todos.clone()
    }

    pub fn add_todo(&mut self, item: TodoItem) {
        self.todos.push(item);
    }

    pub fn update_todo_status(&mut self, id: &str, status: TodoStatus) -> Result<(), String> {
        let item = self
            .todos
            .iter_mut()
            .find(|item| item.id == id)
            .ok_or_else(|| format!("Todo item with id {} not found", id))?;
        item.status = status;
        Ok(())
    }

    pub fn delete_todo(&mut self, id: &str) -> Result<(), String> {
        let initial_len = self.todos.len();
        self.todos.retain(|item| item.id != id);
        if self.todos.len() == initial_len {
            Err(format!("Todo item with id {} not found", id))
        } else {
            Ok(())
        }
    }
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
        assert_eq!(TodoStatus::try_from("todo"), Ok(TodoStatus::Todo));
    }

    #[test]
    fn test_todo_doc_operations() {
        let mut doc = TodoDoc::default();
        assert!(doc.get_todos().is_empty());

        let item1 = TodoItem {
            id: "1".to_string(),
            text: "Task 1".to_string(),
            status: TodoStatus::Todo,
        };
        let item2 = TodoItem {
            id: "2".to_string(),
            text: "Task 2".to_string(),
            status: TodoStatus::Todo,
        };

        doc.add_todo(item1.clone());
        doc.add_todo(item2.clone());
        assert_eq!(doc.get_todos(), vec![item1.clone(), item2.clone()]);

        assert!(doc.update_todo_status("1", TodoStatus::Completed).is_ok());
        assert_eq!(doc.get_todos()[0].status, TodoStatus::Completed);
        assert!(doc.update_todo_status("non-existent", TodoStatus::Archived).is_err());

        assert!(doc.delete_todo("1").is_ok());
        assert_eq!(doc.get_todos(), vec![item2]);
        assert!(doc.delete_todo("1").is_err());
    }
}
