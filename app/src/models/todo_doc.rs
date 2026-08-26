use autosurgeon::{Hydrate, Reconcile};

use super::{TodoItem, TodoStatus};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hydrate, Reconcile)]
pub struct TodoDoc {
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(doc
            .update_todo_status("non-existent", TodoStatus::Archived)
            .is_err());

        assert!(doc.delete_todo("1").is_ok());
        assert_eq!(doc.get_todos(), vec![item2]);
        assert!(doc.delete_todo("1").is_err());
    }
}
