use automerge::transaction::Transactable;
use automerge::{AutoCommit, ObjType, ReadDoc, ScalarValue, Value};
use std::sync::RwLock;
use uuid::Uuid;

use crate::models::{TodoItem, TodoStatus};
use crate::repository::TodoRepository;
use crate::store::SqliteBackingStore;

pub struct AutomergeTodoRepo {
    doc: RwLock<AutoCommit>,
    store: SqliteBackingStore,
    sync_tx: RwLock<Option<tokio::sync::mpsc::UnboundedSender<()>>>,
}

impl AutomergeTodoRepo {
    pub async fn new(store: SqliteBackingStore) -> Result<Self, String> {
        let doc = match store.load().await? {
            Some(data) => AutoCommit::load(&data)
                .map_err(|e| format!("Failed to load automerge doc: {}", e))?,
            None => AutoCommit::new(),
        };

        Ok(Self {
            doc: RwLock::new(doc),
            store,
            sync_tx: RwLock::new(None),
        })
    }

    pub fn set_sync_notifier(&self, tx: tokio::sync::mpsc::UnboundedSender<()>) {
        if let Ok(mut guard) = self.sync_tx.write() {
            *guard = Some(tx);
        }
    }

    pub fn get_doc_bytes(&self) -> Vec<u8> {
        if let Ok(mut doc) = self.doc.write() {
            doc.save()
        } else {
            Vec::new()
        }
    }

    pub async fn merge_incoming(&self, incoming: &mut AutoCommit) -> Result<(), String> {
        let data = {
            let mut doc = self.doc.write().map_err(|e| e.to_string())?;
            doc.merge(incoming).map_err(|e| e.to_string())?;
            doc.save()
        };
        self.store.save(&data).await?;
        // Intentionally NOT calling notify_change here to prevent infinite broadcast loops
        Ok(())
    }

    pub async fn get_sync_state(&self) -> Result<(u64, std::collections::BTreeSet<u64>), String> {
        self.store.get_sync_state().await
    }

    pub async fn save_sync_state(
        &self,
        highest_observed: u64,
        missing_ids: &std::collections::BTreeSet<u64>,
    ) -> Result<(), String> {
        self.store
            .save_sync_state(highest_observed, missing_ids)
            .await
    }

    fn notify_change(&self) {
        if let Ok(guard) = self.sync_tx.read() {
            if let Some(ref tx) = *guard {
                let _ = tx.send(());
            }
        }
    }

    async fn save_local_change(&self, doc_bytes: &[u8]) -> Result<(), String> {
        self.store.save(doc_bytes).await?;
        self.notify_change();
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
                        items.push(TodoItem { id, text, status });
                    }
                }
            }
        }

        Ok(items)
    }

    async fn add(&self, text: String) -> Result<TodoItem, String> {
        let (id, status, doc_bytes) = {
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

            let bytes = doc.save();
            (id, status, bytes)
        };

        self.save_local_change(&doc_bytes).await?;

        Ok(TodoItem { id, text, status })
    }

    async fn update_status(&self, id: String, status: TodoStatus) -> Result<(), String> {
        let doc_bytes = {
            let mut doc = self.doc.write().map_err(|e| e.to_string())?;

            if let Some((Value::Object(ObjType::Map), item_obj)) =
                doc.get(automerge::ROOT, &id).map_err(|e| e.to_string())?
            {
                doc.put(&item_obj, "status", Self::status_to_str(status))
                    .map_err(|e| e.to_string())?;
                doc.save()
            } else {
                return Err(format!("Todo item with id {} not found", id));
            }
        };

        self.save_local_change(&doc_bytes).await?;
        Ok(())
    }

    async fn delete(&self, id: String) -> Result<(), String> {
        self.update_status(id, TodoStatus::Deleted).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn setup_repo() -> AutomergeTodoRepo {
        let store = SqliteBackingStore::in_memory()
            .await
            .expect("Failed to initialize in-memory SQLite store");
        AutomergeTodoRepo::new(store)
            .await
            .expect("Failed to initialize in-memory Automerge repo")
    }

    #[tokio::test]
    async fn test_add_todo_and_get_all() {
        let repo = setup_repo().await;

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
        let repo = setup_repo().await;

        let todo = repo.add("Buy groceries".to_string()).await.unwrap();
        repo.update_status(todo.id.clone(), TodoStatus::Completed)
            .await
            .unwrap();

        let todos = repo.get_all().await.unwrap();
        assert_eq!(todos[0].status, TodoStatus::Completed);
    }

    #[tokio::test]
    async fn test_delete_is_soft_delete() {
        let repo = setup_repo().await;

        let todo = repo.add("To be deleted".to_string()).await.unwrap();
        repo.delete(todo.id.clone()).await.unwrap();

        let todos = repo.get_all().await.unwrap();
        assert!(todos.is_empty());
    }

    #[tokio::test]
    async fn test_merge_sync() {
        let repo1 = setup_repo().await;
        let repo2 = setup_repo().await;

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
