use automerge::transaction::Transactable;
use automerge::{Automerge, ObjType, ReadDoc, ScalarValue, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::crypto::CryptoEngine;
use crate::models::{TodoItem, TodoStatus};

#[derive(Clone)]
pub struct DecryptedView {
    pub doc: Arc<tokio::sync::RwLock<Automerge>>,
    pub crypto: Arc<CryptoEngine>,
}

impl DecryptedView {
    pub fn new(doc: Arc<tokio::sync::RwLock<Automerge>>, crypto: Arc<CryptoEngine>) -> Self {
        Self { doc, crypto }
    }

    fn status_to_str(status: TodoStatus) -> &'static str {
        match status {
            TodoStatus::Todo => "todo",
            TodoStatus::Archived => "archived",
            TodoStatus::Completed => "completed",
        }
    }

    fn str_to_status(s: &str) -> Option<TodoStatus> {
        match s {
            "todo" | "workingSet" => Some(TodoStatus::Todo),
            "archived" | "backlog" => Some(TodoStatus::Archived),
            "completed" => Some(TodoStatus::Completed),
            _ => None,
        }
    }

    fn get_plain_str_from_doc<D: ReadDoc>(
        doc: &D,
        item_obj: &automerge::ObjId,
        key: &str,
    ) -> Result<Option<String>, String> {
        match doc.get(item_obj, key).map_err(|e| e.to_string())? {
            Some((Value::Scalar(s), _)) => match s.as_ref() {
                ScalarValue::Str(sm) => Ok(Some(sm.to_string())),
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    fn get_str_from_doc<D: ReadDoc>(
        doc: &D,
        crypto: &CryptoEngine,
        item_obj: &automerge::ObjId,
        key: &str,
    ) -> Result<Option<String>, String> {
        match doc.get(item_obj, key).map_err(|e| e.to_string())? {
            Some((Value::Scalar(s), _)) => match s.as_ref() {
                ScalarValue::Bytes(bytes) => {
                    let decrypted = crypto.decrypt_value(bytes)?;
                    let value = String::from_utf8(decrypted)
                        .map_err(|e| format!("Invalid UTF-8 in decrypted value: {}", e))?;
                    Ok(Some(value))
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    pub async fn get_doc_bytes(&self) -> Vec<u8> {
        let doc = self.doc.read().await;
        doc.save()
    }

    pub async fn get_all(&self) -> Result<Vec<TodoItem>, String> {
        let doc = self.doc.read().await;
        let todos_obj = match doc
            .get(automerge::ROOT, "todos")
            .map_err(|e| e.to_string())?
        {
            Some((Value::Object(ObjType::List), obj_id)) => obj_id,
            _ => return Ok(Vec::new()),
        };

        let len = doc.length(&todos_obj);
        let mut items = Vec::new();

        for idx in 0..len {
            if let Some((Value::Object(ObjType::Map), item_obj)) =
                doc.get(&todos_obj, idx).map_err(|e| e.to_string())?
            {
                let id = match Self::get_plain_str_from_doc(&*doc, &item_obj, "id")? {
                    Some(id) => id,
                    None => continue,
                };
                let text = match Self::get_str_from_doc(&*doc, &self.crypto, &item_obj, "text")? {
                    Some(text) => text,
                    None => continue,
                };
                let status_str =
                    match Self::get_str_from_doc(&*doc, &self.crypto, &item_obj, "status")? {
                        Some(status) => status,
                        None => continue,
                    };

                if let Some(status) = Self::str_to_status(&status_str) {
                    items.push(TodoItem { id, text, status });
                }
            }
        }
        Ok(items)
    }

    pub async fn add(&self, text: String) -> Result<TodoItem, String> {
        let mut doc = self.doc.write().await;
        let mut tx = doc.transaction();

        let todos_obj = match tx
            .get(automerge::ROOT, "todos")
            .map_err(|e| e.to_string())?
        {
            Some((Value::Object(ObjType::List), obj_id)) => obj_id,
            _ => tx
                .put_object(automerge::ROOT, "todos", ObjType::List)
                .map_err(|e| e.to_string())?,
        };

        let id = Uuid::new_v4().to_string();
        let len = tx.length(&todos_obj);
        let item_obj = tx
            .insert_object(&todos_obj, len, ObjType::Map)
            .map_err(|e| e.to_string())?;

        let status = TodoStatus::Todo;
        let enc_text = self.crypto.encrypt_value(text.as_bytes())?;
        let enc_status = self
            .crypto
            .encrypt_value(Self::status_to_str(status).as_bytes())?;

        tx.put(&item_obj, "id", id.as_str())
            .map_err(|e| e.to_string())?;
        tx.put(&item_obj, "text", ScalarValue::Bytes(enc_text))
            .map_err(|e| e.to_string())?;
        tx.put(&item_obj, "status", ScalarValue::Bytes(enc_status))
            .map_err(|e| e.to_string())?;

        tx.commit();

        Ok(TodoItem { id, text, status })
    }

    pub async fn update_status(&self, id: String, status: TodoStatus) -> Result<(), String> {
        let mut doc = self.doc.write().await;
        let mut tx = doc.transaction();

        let todos_obj = match tx
            .get(automerge::ROOT, "todos")
            .map_err(|e| e.to_string())?
        {
            Some((Value::Object(ObjType::List), obj_id)) => obj_id,
            _ => return Err(format!("Todo item with id {} not found", id)),
        };

        let len = tx.length(&todos_obj);
        let mut found = false;

        for idx in 0..len {
            if let Some((Value::Object(ObjType::Map), item_obj)) =
                tx.get(&todos_obj, idx).map_err(|e| e.to_string())?
            {
                let item_id = Self::get_plain_str_from_doc(&tx, &item_obj, "id")?;
                if item_id.as_deref() == Some(&id) {
                    let enc_status = self
                        .crypto
                        .encrypt_value(Self::status_to_str(status).as_bytes())?;
                    tx.put(&item_obj, "status", ScalarValue::Bytes(enc_status))
                        .map_err(|e| e.to_string())?;
                    found = true;
                    break;
                }
            }
        }

        if found {
            tx.commit();
            Ok(())
        } else {
            Err(format!("Todo item with id {} not found", id))
        }
    }

    pub async fn delete(&self, id: String) -> Result<(), String> {
        let mut doc = self.doc.write().await;
        let mut tx = doc.transaction();

        let todos_obj = match tx
            .get(automerge::ROOT, "todos")
            .map_err(|e| e.to_string())?
        {
            Some((Value::Object(ObjType::List), obj_id)) => obj_id,
            _ => return Err(format!("Todo item with id {} not found", id)),
        };

        let len = tx.length(&todos_obj);
        let mut found_idx = None;

        for idx in 0..len {
            if let Some((Value::Object(ObjType::Map), item_obj)) =
                tx.get(&todos_obj, idx).map_err(|e| e.to_string())?
            {
                let item_id = Self::get_plain_str_from_doc(&tx, &item_obj, "id")?;
                if item_id.as_deref() == Some(&id) {
                    found_idx = Some(idx);
                    break;
                }
            }
        }

        if let Some(idx) = found_idx {
            tx.delete(&todos_obj, idx).map_err(|e| e.to_string())?;
            tx.commit();
            Ok(())
        } else {
            Err(format!("Todo item with id {} not found", id))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::KEY_SIZE;

    fn create_test_view() -> DecryptedView {
        let doc = Arc::new(tokio::sync::RwLock::new(Automerge::new()));
        let key = [42u8; KEY_SIZE];
        let crypto = Arc::new(CryptoEngine::new(&key));
        DecryptedView::new(doc, crypto)
    }

    #[tokio::test]
    async fn test_empty_todos() {
        let view = create_test_view();
        let items = view.get_all().await.unwrap();
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn test_add_maintains_order() {
        let view = create_test_view();
        let t1 = view.add("First todo".to_string()).await.unwrap();
        let t2 = view.add("Second todo".to_string()).await.unwrap();
        let t3 = view.add("Third todo".to_string()).await.unwrap();

        let items = view.get_all().await.unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].id, t1.id);
        assert_eq!(items[0].text, "First todo");
        assert_eq!(items[1].id, t2.id);
        assert_eq!(items[1].text, "Second todo");
        assert_eq!(items[2].id, t3.id);
        assert_eq!(items[2].text, "Third todo");
    }

    #[tokio::test]
    async fn test_update_status() {
        let view = create_test_view();
        let t1 = view.add("Task".to_string()).await.unwrap();
        assert_eq!(t1.status, TodoStatus::Todo);

        view.update_status(t1.id.clone(), TodoStatus::Archived)
            .await
            .unwrap();

        let items = view.get_all().await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status, TodoStatus::Archived);

        view.update_status(t1.id.clone(), TodoStatus::Completed)
            .await
            .unwrap();

        let items = view.get_all().await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status, TodoStatus::Completed);
    }

    #[tokio::test]
    async fn test_delete_todo() {
        let view = create_test_view();
        let t1 = view.add("Task 1".to_string()).await.unwrap();
        let t2 = view.add("Task 2".to_string()).await.unwrap();

        view.delete(t1.id.clone()).await.unwrap();

        let items = view.get_all().await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, t2.id);
        assert_eq!(items[0].text, "Task 2");
    }
}
