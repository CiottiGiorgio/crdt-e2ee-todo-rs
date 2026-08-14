use automerge::transaction::Transactable;
use automerge::{AutoCommit, ObjType, ReadDoc, ScalarValue, Value};
use std::sync::RwLock;
use uuid::Uuid;

use crate::models::{TodoItem, TodoStatus};

pub struct AutomergeTodoRepo {
    pub doc: RwLock<AutoCommit>,
}

impl AutomergeTodoRepo {
    pub fn new(data: Option<Vec<u8>>) -> Result<Self, String> {
        let doc = match data {
            Some(data) => AutoCommit::load(&data)
                .map_err(|e| format!("Failed to load automerge doc: {}", e))?,
            None => AutoCommit::new(),
        };

        Ok(Self {
            doc: RwLock::new(doc),
        })
    }

    pub fn get_doc_bytes(&self) -> Vec<u8> {
        if let Ok(mut doc) = self.doc.write() {
            doc.save()
        } else {
            Vec::new()
        }
    }

    pub fn merge_incoming(&self, incoming: &mut AutoCommit) -> Result<Vec<u8>, String> {
        let mut doc = self.doc.write().map_err(|e| e.to_string())?;
        doc.merge(incoming).map_err(|e| e.to_string())?;
        Ok(doc.save())
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

    pub async fn get_all(&self) -> Result<Vec<TodoItem>, String> {
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

    pub async fn add(&self, text: String) -> Result<(TodoItem, Vec<u8>), String> {
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
        Ok((TodoItem { id, text, status }, bytes))
    }

    pub async fn update_status(&self, id: String, status: TodoStatus) -> Result<Vec<u8>, String> {
        let mut doc = self.doc.write().map_err(|e| e.to_string())?;

        if let Some((Value::Object(ObjType::Map), item_obj)) =
            doc.get(automerge::ROOT, &id).map_err(|e| e.to_string())?
        {
            doc.put(&item_obj, "status", Self::status_to_str(status))
                .map_err(|e| e.to_string())?;
            Ok(doc.save())
        } else {
            Err(format!("Todo item with id {} not found", id))
        }
    }

    pub async fn delete(&self, id: String) -> Result<Vec<u8>, String> {
        self.update_status(id, TodoStatus::Deleted).await
    }
}
