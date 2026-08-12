use automerge::transaction::Transactable;
use automerge::{AutoCommit, ObjType, ReadDoc, ScalarValue, Value};
use std::path::PathBuf;
use std::sync::RwLock;
use uuid::Uuid;

use crate::models::{TodoItem, TodoStatus};
use crate::repository::TodoRepository;

pub struct AutomergeTodoRepo {
    doc: RwLock<AutoCommit>,
    file_path: Option<PathBuf>,
}

impl AutomergeTodoRepo {
    pub fn new(file_path: Option<PathBuf>) -> Result<Self, String> {
        let doc = if let Some(ref path) = file_path {
            if path.exists() {
                let data = std::fs::read(path).map_err(|e| e.to_string())?;
                AutoCommit::load(&data).map_err(|e| format!("Failed to load automerge doc: {}", e))?
            } else {
                AutoCommit::new()
            }
        } else {
            AutoCommit::new()
        };

        Ok(Self {
            doc: RwLock::new(doc),
            file_path,
        })
    }

    fn save_internal(&self, doc: &mut AutoCommit) -> Result<(), String> {
        if let Some(ref path) = self.file_path {
            let data = doc.save();
            std::fs::write(path, data).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn status_to_str(status: TodoStatus) -> &'static str {
        match status {
            TodoStatus::WorkingSet => "workingSet",
            TodoStatus::Backlog => "backlog",
            TodoStatus::Completed => "completed",
            TodoStatus::Deleted => "deleted",
        }
    }

    fn str_to_status(s: &str) -> Option<TodoStatus> {
        match s {
            "workingSet" => Some(TodoStatus::WorkingSet),
            "backlog" => Some(TodoStatus::Backlog),
            "completed" => Some(TodoStatus::Completed),
            "deleted" => Some(TodoStatus::Deleted),
            _ => None,
        }
    }
}

#[async_trait::async_trait]
impl TodoRepository for AutomergeTodoRepo {
    async fn get_all(&self) -> Result<Vec<TodoItem>, String> {
        let doc = self.doc.read().map_err(|e| e.to_string())?;

        let keys: Vec<String> = doc.keys(automerge::ROOT).collect();
        let mut items = Vec::new();

        for id in keys {
            if let Some((Value::Object(ObjType::Map), item_obj)) =
                doc.get(automerge::ROOT, &id).map_err(|e| e.to_string())?
            {
                let text = match doc.get(&item_obj, "text").map_err(|e| e.to_string())? {
                    Some((Value::Scalar(s), _)) => match s.as_ref() {
                        ScalarValue::Str(st) => st.to_string(),
                        _ => continue,
                    },
                    _ => continue,
                };
                let status_str = match doc.get(&item_obj, "status").map_err(|e| e.to_string())? {
                    Some((Value::Scalar(s), _)) => match s.as_ref() {
                        ScalarValue::Str(st) => st.to_string(),
                        _ => continue,
                    },
                    _ => continue,
                };

                if let Some(status) = Self::str_to_status(&status_str) {
                    if status != TodoStatus::Deleted {
                        items.push(TodoItem {
                            id,
                            text,
                            status,
                        });
                    }
                }
            }
        }

        Ok(items)
    }

    async fn add(&self, text: String) -> Result<TodoItem, String> {
        let mut doc = self.doc.write().map_err(|e| e.to_string())?;

        let id = Uuid::new_v4().to_string();
        let item_obj = doc
            .put_object(automerge::ROOT, &id, ObjType::Map)
            .map_err(|e| e.to_string())?;

        let status = TodoStatus::WorkingSet;
        doc.put(&item_obj, "text", text.as_str())
            .map_err(|e| e.to_string())?;
        doc.put(&item_obj, "status", Self::status_to_str(status))
            .map_err(|e| e.to_string())?;

        self.save_internal(&mut doc)?;

        Ok(TodoItem { id, text, status })
    }

    async fn update_status(&self, id: String, status: TodoStatus) -> Result<(), String> {
        let mut doc = self.doc.write().map_err(|e| e.to_string())?;

        if let Some((Value::Object(ObjType::Map), item_obj)) =
            doc.get(automerge::ROOT, &id).map_err(|e| e.to_string())?
        {
            doc.put(&item_obj, "status", Self::status_to_str(status))
                .map_err(|e| e.to_string())?;
            self.save_internal(&mut doc)?;
            Ok(())
        } else {
            Err(format!("Todo item with id {} not found", id))
        }
    }

    async fn delete(&self, id: String) -> Result<(), String> {
        self.update_status(id, TodoStatus::Deleted).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_repo() -> AutomergeTodoRepo {
        AutomergeTodoRepo::new(None).expect("Failed to initialize in-memory Automerge repo")
    }

    #[tokio::test]
    async fn test_add_todo_and_get_all() {
        let repo = setup_repo();

        let added = repo
            .add("Test writing Automerge tests".to_string())
            .await
            .unwrap();
        assert_eq!(added.text, "Test writing Automerge tests");
        assert_eq!(added.status, TodoStatus::WorkingSet);

        let todos = repo.get_all().await.unwrap();
        assert_eq!(todos.len(), 1);
        assert_eq!(todos[0].id, added.id);
    }

    #[tokio::test]
    async fn test_update_status() {
        let repo = setup_repo();

        let todo = repo.add("Buy groceries".to_string()).await.unwrap();
        repo.update_status(todo.id.clone(), TodoStatus::Completed)
            .await
            .unwrap();

        let todos = repo.get_all().await.unwrap();
        assert_eq!(todos[0].status, TodoStatus::Completed);
    }

    #[tokio::test]
    async fn test_delete_is_soft_delete() {
        let repo = setup_repo();

        let todo = repo.add("To be deleted".to_string()).await.unwrap();
        repo.delete(todo.id.clone()).await.unwrap();

        let todos = repo.get_all().await.unwrap();
        assert!(todos.is_empty());
    }

    #[tokio::test]
    async fn test_merge_sync() {
        let repo1 = setup_repo();
        let repo2 = setup_repo();

        let _todo1 = repo1.add("From repo 1".to_string()).await.unwrap();
        let _todo2 = repo2.add("From repo 2".to_string()).await.unwrap();

        // Merge doc1 into doc2
        let mut doc1 = repo1.doc.write().unwrap();
        let mut doc2 = repo2.doc.write().unwrap();
        doc2.merge(&mut doc1).unwrap();
        drop(doc1);
        drop(doc2);

        let merged_todos = repo2.get_all().await.unwrap();
        assert_eq!(merged_todos.len(), 2);
    }
}
