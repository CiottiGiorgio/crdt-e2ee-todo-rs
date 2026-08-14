use crate::automerge::AutomergeTodoRepo;
use crate::crypto::CryptoEngine;
use crate::store::SqliteBackingStore;
use automerge::AutoCommit;
use futures_util::SinkExt;
use shared::{ClientMessage, EncryptedPayload};
use std::collections::BTreeSet;
use std::pin::Pin;
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::debug;

pub struct PendingSnapshot {
    pub covers_seq_id: u64,
    pub payload: EncryptedPayload,
}

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

pub fn schedule_snapshot_if_eligible(
    repo: &AutomergeTodoRepo,
    crypto: &CryptoEngine,
    highest_observed_seq: u64,
    missing_deltas: &BTreeSet<u64>,
    pending_snapshot: &mut Option<PendingSnapshot>,
    snapshot_debounce: &mut Option<Pin<Box<tokio::time::Sleep>>>,
    debounce_duration: Duration,
) {
    let continuous_seq = get_highest_continuous_seq(highest_observed_seq, missing_deltas);
    if missing_deltas.is_empty() && continuous_seq > 0 {
        let should_update = match pending_snapshot.as_ref() {
            Some(pending) => continuous_seq > pending.covers_seq_id,
            None => true,
        };

        if should_update {
            if let Some(payload) = get_encrypted_local_doc(repo, crypto) {
                *pending_snapshot = Some(PendingSnapshot {
                    covers_seq_id: continuous_seq,
                    payload,
                });
                *snapshot_debounce = Some(Box::pin(tokio::time::sleep(debounce_duration)));
                debug!(
                    "Chambered snapshot covering seq_id: {} (delayed for {}s)",
                    continuous_seq,
                    debounce_duration.as_secs()
                );
            }
        }
    }
}

pub async fn flush_pending_snapshot_on_shutdown<S>(
    write: &mut S,
    pending_snapshot: &mut Option<PendingSnapshot>,
    repo: &AutomergeTodoRepo,
    crypto: &CryptoEngine,
    highest_observed_seq: u64,
    missing_deltas: &BTreeSet<u64>,
) where
    S: SinkExt<Message> + Unpin,
{
    let continuous_seq = get_highest_continuous_seq(highest_observed_seq, missing_deltas);
    let snapshot_to_send = pending_snapshot.take().or_else(|| {
        if missing_deltas.is_empty() && continuous_seq > 0 {
            get_encrypted_local_doc(repo, crypto).map(|payload| PendingSnapshot {
                covers_seq_id: continuous_seq,
                payload,
            })
        } else {
            None
        }
    });

    if let Some(pending) = snapshot_to_send {
        let client_msg = ClientMessage::Snapshot {
            covers_seq_id: pending.covers_seq_id,
            payload: pending.payload,
        };
        if send_client_message(write, &client_msg).await.is_ok() {
            tracing::info!(
                "Pushed immediate snapshot on shutdown (covers seq_id: {}) to server!",
                pending.covers_seq_id
            );
        } else {
            tracing::warn!("Failed to send immediate snapshot on shutdown");
        }
    }
}
