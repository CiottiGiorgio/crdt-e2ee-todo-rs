use automerge::transaction::Transactable;
use automerge::{Automerge, ObjType, ReadDoc, ScalarValue, Value};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::crypto::CryptoEngine;
use crate::models::{TodoItem, TodoStatus};

#[derive(Clone)]
pub struct DecryptedView {
    pub doc: Arc<RwLock<Automerge>>,
    pub crypto: Arc<CryptoEngine>,
}

impl DecryptedView {
    pub fn new(doc: Arc<RwLock<Automerge>>, crypto: Arc<CryptoEngine>) -> Self {
        Self { doc, crypto }
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

    fn get_str_from_doc(
        doc: &Automerge,
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
        let keys: Vec<String> = doc.keys(automerge::ROOT).collect();
        let mut items = Vec::new();

        for id in keys {
            if let Some((Value::Object(ObjType::Map), item_obj)) =
                doc.get(automerge::ROOT, &id).map_err(|e| e.to_string())?
            {
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
                    if status != TodoStatus::Deleted {
                        items.push(TodoItem { id, text, status });
                    }
                }
            }
        }
        Ok(items)
    }

    pub async fn add(&self, text: String) -> Result<TodoItem, String> {
        let mut doc = self.doc.write().await;
        let mut tx = doc.transaction();
        let id = Uuid::new_v4().to_string();
        let item_obj = tx
            .put_object(automerge::ROOT, &id, ObjType::Map)
            .map_err(|e| e.to_string())?;

        let status = TodoStatus::WorkingSet;
        let enc_text = self.crypto.encrypt_value(text.as_bytes())?;
        let enc_status = self
            .crypto
            .encrypt_value(Self::status_to_str(status).as_bytes())?;

        tx.put(&item_obj, "text", ScalarValue::Bytes(enc_text))
            .map_err(|e| e.to_string())?;
        tx.put(&item_obj, "status", ScalarValue::Bytes(enc_status))
            .map_err(|e| e.to_string())?;

        tx.commit();

        Ok(TodoItem { id, text, status })
    }

    pub async fn update_status(&self, id: String, status: TodoStatus) -> Result<(), String> {
        let mut doc = self.doc.write().await;
        if let Some((Value::Object(ObjType::Map), item_obj)) =
            doc.get(automerge::ROOT, &id).map_err(|e| e.to_string())?
        {
            let mut tx = doc.transaction();
            let enc_status = self
                .crypto
                .encrypt_value(Self::status_to_str(status).as_bytes())?;
            tx.put(&item_obj, "status", ScalarValue::Bytes(enc_status))
                .map_err(|e| e.to_string())?;
            tx.commit();
            Ok(())
        } else {
            Err(format!("Todo item with id {} not found", id))
        }
    }

    pub async fn delete(&self, id: String) -> Result<(), String> {
        self.update_status(id, TodoStatus::Deleted).await
    }
}
