use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use automerge::transaction::Transactable;
use automerge::{AutoCommit, ObjType, ReadDoc, ScalarValue, Value};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

use crate::crypto::CryptoEngine;
use crate::models::{TodoItem, TodoStatus};

pub struct AutomergeTodoRepo {
    pub doc: RwLock<AutoCommit>,
    crypto: Arc<CryptoEngine>,
}

impl AutomergeTodoRepo {
    pub fn new(data: Option<Vec<u8>>, crypto: Arc<CryptoEngine>) -> Result<Self, String> {
        let doc = match data {
            Some(data) => AutoCommit::load(&data)
                .map_err(|e| format!("Failed to load automerge doc: {}", e))?,
            None => AutoCommit::new(),
        };

        Ok(Self {
            doc: RwLock::new(doc),
            crypto,
        })
    }

    pub fn get_doc_bytes(&self) -> Vec<u8> {
        if let Ok(mut doc) = self.doc.write() {
            doc.save()
        } else {
            Vec::new()
        }
    }

    /// Generates the next outgoing Automerge sync message for the given peer
    /// `state`, if any. Returns the encoded message bytes, or `None` when there
    /// is nothing to send.
    pub fn generate_sync_message(&self, state: &mut SyncState) -> Result<Option<Vec<u8>>, String> {
        let mut doc = self.doc.write().map_err(|e| e.to_string())?;
        let msg = doc.sync().generate_sync_message(state).map(|m| m.encode());
        Ok(msg)
    }

    /// Decodes and applies an incoming Automerge sync message against the given
    /// peer `state`, advancing the local document as needed. Returns `true` when
    /// the message actually advanced the document (the heads changed), and
    /// `false` when it was a protocol-only exchange (e.g. an acknowledgement)
    /// that left the document untouched. Callers use this to avoid echoing
    /// spurious update notifications for no-op syncs.
    pub fn receive_sync_message(&self, state: &mut SyncState, data: &[u8]) -> Result<bool, String> {
        let msg = SyncMessage::decode(data).map_err(|e| format!("decode sync msg: {}", e))?;
        let mut doc = self.doc.write().map_err(|e| e.to_string())?;
        let heads_before = doc.get_heads();
        doc.sync()
            .receive_sync_message(state, msg)
            .map_err(|e| format!("receive sync msg: {}", e))?;
        let heads_after = doc.get_heads();
        Ok(heads_before != heads_after)
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

    /// Reads an encrypted scalar `Bytes` value stored under `key` inside `item_obj`
    /// and decrypts it into a `String`. Returns `Ok(None)` if the field is missing
    /// or not an encrypted bytes scalar.
    fn read_encrypted_str<R: ReadDoc>(
        &self,
        doc: &R,
        item_obj: &automerge::ObjId,
        key: &str,
    ) -> Result<Option<String>, String> {
        match doc.get(item_obj, key).map_err(|e| e.to_string())? {
            Some((Value::Scalar(s), _)) => match s.as_ref() {
                ScalarValue::Bytes(bytes) => {
                    let decrypted = self.crypto.decrypt_value(bytes)?;
                    let value = String::from_utf8(decrypted)
                        .map_err(|e| format!("Invalid UTF-8 in decrypted value: {}", e))?;
                    Ok(Some(value))
                }
                _ => Ok(None),
            },
            _ => Ok(None),
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
                let text = match self.read_encrypted_str(&*doc, &item_obj, "text")? {
                    Some(text) => text,
                    None => continue,
                };
                let status_str = match self.read_encrypted_str(&*doc, &item_obj, "status")? {
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

    pub async fn add(&self, text: String) -> Result<(TodoItem, Vec<u8>), String> {
        let mut doc = self.doc.write().map_err(|e| e.to_string())?;

        let id = Uuid::new_v4().to_string();
        let item_obj = doc
            .put_object(automerge::ROOT, &id, ObjType::Map)
            .map_err(|e| e.to_string())?;

        let status = TodoStatus::WorkingSet;
        let enc_text = self.crypto.encrypt_value(text.as_bytes())?;
        let enc_status = self
            .crypto
            .encrypt_value(Self::status_to_str(status).as_bytes())?;
        doc.put(&item_obj, "text", ScalarValue::Bytes(enc_text))
            .map_err(|e| e.to_string())?;
        doc.put(&item_obj, "status", ScalarValue::Bytes(enc_status))
            .map_err(|e| e.to_string())?;

        let bytes = doc.save();
        Ok((TodoItem { id, text, status }, bytes))
    }

    pub async fn update_status(&self, id: String, status: TodoStatus) -> Result<Vec<u8>, String> {
        let mut doc = self.doc.write().map_err(|e| e.to_string())?;

        if let Some((Value::Object(ObjType::Map), item_obj)) =
            doc.get(automerge::ROOT, &id).map_err(|e| e.to_string())?
        {
            let enc_status = self
                .crypto
                .encrypt_value(Self::status_to_str(status).as_bytes())?;
            doc.put(&item_obj, "status", ScalarValue::Bytes(enc_status))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::KEY_SIZE;

    fn test_crypto() -> Arc<CryptoEngine> {
        Arc::new(CryptoEngine::new(&[42u8; KEY_SIZE]))
    }

    #[tokio::test]
    async fn test_add_and_get_all_roundtrip() {
        let repo = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        let (item, _) = repo.add("Buy milk".to_string()).await.unwrap();

        let items = repo.get_all().await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, item.id);
        assert_eq!(items[0].text, "Buy milk");
        assert_eq!(items[0].status, TodoStatus::WorkingSet);
    }

    #[tokio::test]
    async fn test_values_are_confidential_in_serialized_bytes() {
        let secret = "top secret todo text";
        let repo = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        let (item, doc_bytes) = repo.add(secret.to_string()).await.unwrap();

        // Plaintext content must never appear in the serialized document.
        assert!(
            !doc_bytes
                .windows(secret.len())
                .any(|w| w == secret.as_bytes()),
            "plaintext todo text leaked into serialized document"
        );
        let status_plain = b"workingSet";
        assert!(
            !doc_bytes
                .windows(status_plain.len())
                .any(|w| w == status_plain),
            "plaintext status leaked into serialized document"
        );

        // Structure (field keys and the item UUID) stays in plaintext.
        assert!(doc_bytes.windows(4).any(|w| w == b"text"));
        assert!(doc_bytes.windows(6).any(|w| w == b"status"));
        assert!(doc_bytes
            .windows(item.id.len())
            .any(|w| w == item.id.as_bytes()));
    }

    /// Runs the Automerge sync protocol between two repositories until both
    /// peers have nothing left to send, driving the handshake to convergence.
    fn sync_to_convergence(repo_a: &AutomergeTodoRepo, repo_b: &AutomergeTodoRepo) {
        let mut state_a = SyncState::new();
        let mut state_b = SyncState::new();

        loop {
            let msg_a = repo_a.generate_sync_message(&mut state_a).unwrap();
            if let Some(ref data) = msg_a {
                repo_b.receive_sync_message(&mut state_b, data).unwrap();
            }

            let msg_b = repo_b.generate_sync_message(&mut state_b).unwrap();
            if let Some(ref data) = msg_b {
                repo_a.receive_sync_message(&mut state_a, data).unwrap();
            }

            if msg_a.is_none() && msg_b.is_none() {
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_concurrent_change_convergence() {
        let repo_a = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        let repo_b = AutomergeTodoRepo::new(None, test_crypto()).unwrap();

        // Concurrent edits on both replicas.
        let (item_a, _) = repo_a.add("todo from A".to_string()).await.unwrap();
        let (item_b, _) = repo_b.add("todo from B".to_string()).await.unwrap();

        // Reconcile via the Automerge sync protocol.
        sync_to_convergence(&repo_a, &repo_b);

        let mut items_a = repo_a.get_all().await.unwrap();
        let mut items_b = repo_b.get_all().await.unwrap();
        items_a.sort_by(|x, y| x.id.cmp(&y.id));
        items_b.sort_by(|x, y| x.id.cmp(&y.id));

        assert_eq!(items_a.len(), 2);
        assert_eq!(items_b.len(), 2);
        // Both replicas converge and decrypt each other's content.
        let texts_a: Vec<_> = items_a.iter().map(|i| i.text.clone()).collect();
        assert!(texts_a.contains(&"todo from A".to_string()));
        assert!(texts_a.contains(&"todo from B".to_string()));
        assert_eq!(
            items_a.iter().map(|i| &i.id).collect::<Vec<_>>(),
            items_b.iter().map(|i| &i.id).collect::<Vec<_>>()
        );
        // Sanity: the two ids are distinct.
        assert_ne!(item_a.id, item_b.id);
    }

    #[tokio::test]
    async fn test_initial_handshake_from_empty_peer() {
        // Peer A already has items; peer B starts empty.
        let repo_a = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        let repo_b = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        repo_a.add("first".to_string()).await.unwrap();
        repo_a.add("second".to_string()).await.unwrap();

        sync_to_convergence(&repo_a, &repo_b);

        let items_b = repo_b.get_all().await.unwrap();
        assert_eq!(items_b.len(), 2);
        let texts: Vec<_> = items_b.iter().map(|i| i.text.clone()).collect();
        assert!(texts.contains(&"first".to_string()));
        assert!(texts.contains(&"second".to_string()));
    }

    #[tokio::test]
    async fn test_incremental_update_after_initial_sync() {
        let repo_a = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        let repo_b = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        repo_a.add("shared".to_string()).await.unwrap();
        sync_to_convergence(&repo_a, &repo_b);
        assert_eq!(repo_b.get_all().await.unwrap().len(), 1);

        // A later local change on A propagates to B on the next sync round.
        repo_a.add("later".to_string()).await.unwrap();
        sync_to_convergence(&repo_a, &repo_b);

        let items_b = repo_b.get_all().await.unwrap();
        assert_eq!(items_b.len(), 2);
        assert!(items_b.iter().any(|i| i.text == "later"));
    }

    #[tokio::test]
    async fn test_no_op_sync_when_already_converged() {
        let repo_a = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        let repo_b = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        repo_a.add("only".to_string()).await.unwrap();
        sync_to_convergence(&repo_a, &repo_b);

        // Fresh states against already-synced docs still converge with nothing
        // new after the handshake completes.
        let mut state_a = SyncState::new();
        let mut state_b = SyncState::new();
        // Drive the handshake once so bloom filters are exchanged.
        loop {
            let msg_a = repo_a.generate_sync_message(&mut state_a).unwrap();
            if let Some(ref data) = msg_a {
                repo_b.receive_sync_message(&mut state_b, data).unwrap();
            }
            let msg_b = repo_b.generate_sync_message(&mut state_b).unwrap();
            if let Some(ref data) = msg_b {
                repo_a.receive_sync_message(&mut state_a, data).unwrap();
            }
            if msg_a.is_none() && msg_b.is_none() {
                break;
            }
        }
        // Now both are in sync: neither peer has anything to send.
        assert!(repo_a.generate_sync_message(&mut state_a).unwrap().is_none());
        assert!(repo_b.generate_sync_message(&mut state_b).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_receive_sync_message_reports_document_change() {
        // Peer A has data; peer B is empty. As the handshake proceeds, peer B
        // observes the message that actually delivers A's change as a
        // document-advancing receive (returns `true`) exactly once; every other
        // protocol-only round leaves the doc untouched and returns `false`.
        let repo_a = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        let repo_b = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        repo_a.add("payload".to_string()).await.unwrap();

        let mut state_a = SyncState::new();
        let mut state_b = SyncState::new();

        let mut b_change_count = 0;
        loop {
            let msg_a = repo_a.generate_sync_message(&mut state_a).unwrap();
            if let Some(ref data) = msg_a {
                if repo_b.receive_sync_message(&mut state_b, data).unwrap() {
                    b_change_count += 1;
                }
            }
            let msg_b = repo_b.generate_sync_message(&mut state_b).unwrap();
            if let Some(ref data) = msg_b {
                repo_a.receive_sync_message(&mut state_a, data).unwrap();
            }
            if msg_a.is_none() && msg_b.is_none() {
                break;
            }
        }

        // The change was delivered exactly once as a real, document-advancing
        // update; no protocol-only round is counted as a change.
        assert_eq!(
            b_change_count, 1,
            "peer B should apply A's change exactly once"
        );
        assert_eq!(repo_b.get_all().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_receive_malformed_sync_message_errors() {
        let repo = AutomergeTodoRepo::new(None, test_crypto()).unwrap();
        let mut state = SyncState::new();
        let result = repo.receive_sync_message(&mut state, &[0xde, 0xad, 0xbe, 0xef]);
        assert!(result.is_err());
    }
}
