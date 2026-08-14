use crate::automerge::AutomergeTodoRepo;
use crate::crypto::CryptoEngine;
use crate::store::SqliteBackingStore;
use automerge::AutoCommit;
use futures_util::SinkExt;
use shared::{ClientMessage, EncryptedPayload};
use std::collections::BTreeSet;
use tokio_tungstenite::tungstenite::protocol::Message;

pub fn get_highest_continuous_seq(highest_observed: u64, missing: &BTreeSet<u64>) -> u64 {
    match missing.first() {
        Some(&lowest_missing) => lowest_missing.saturating_sub(1),
        None => highest_observed,
    }
}

pub fn record_observed_seq(
    seq_id: u64,
    highest_observed_seq: &mut u64,
    missing_deltas: &mut BTreeSet<u64>,
) {
    if seq_id > *highest_observed_seq {
        for seq in (*highest_observed_seq + 1)..=seq_id {
            missing_deltas.insert(seq);
        }
        *highest_observed_seq = seq_id;
    }
}

pub async fn decrypt_merge_and_persist(
    repo: &AutomergeTodoRepo,
    crypto: &CryptoEngine,
    store: &SqliteBackingStore,
    payload: &EncryptedPayload,
) -> bool {
    let Ok(decrypted_bytes) = crypto.decrypt(payload) else {
        return false;
    };
    let Ok(mut incoming_doc) = AutoCommit::load(&decrypted_bytes) else {
        return false;
    };
    let Ok(merged_bytes) = repo.merge_incoming(&mut incoming_doc) else {
        return false;
    };
    if let Ok(encrypted_merged) = crypto.encrypt(&merged_bytes) {
        if let Ok(bytes) = serde_json::to_vec(&encrypted_merged) {
            let _ = store.save(&bytes).await;
        }
    }
    true
}

pub fn get_encrypted_local_doc(
    repo: &AutomergeTodoRepo,
    crypto: &CryptoEngine,
) -> Option<EncryptedPayload> {
    let local_bytes = repo.get_doc_bytes();
    if local_bytes.is_empty() {
        return None;
    }
    crypto.encrypt(&local_bytes).ok()
}

pub async fn send_client_message<S>(
    write: &mut S,
    msg: &ClientMessage,
) -> Result<(), S::Error>
where
    S: SinkExt<Message> + Unpin,
{
    if let Ok(json) = serde_json::to_string(msg) {
        write.send(Message::Text(json.into())).await
    } else {
        Ok(())
    }
}

pub async fn request_sync_if_missing<S>(
    write: &mut S,
    highest_observed_seq: u64,
    missing_deltas: &BTreeSet<u64>,
) where
    S: SinkExt<Message> + Unpin,
{
    if !missing_deltas.is_empty() {
        let continuous_seq = get_highest_continuous_seq(highest_observed_seq, missing_deltas);
        let req_sync = ClientMessage::RequestSync {
            from_seq_id: continuous_seq,
        };
        let _ = send_client_message(write, &req_sync).await;
    }
}
