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
) -> Result<(), String> {
    let decrypted_bytes = crypto.decrypt(payload)?;
    let mut incoming_doc = AutoCommit::load(&decrypted_bytes).map_err(|e| e.to_string())?;
    let merged_bytes = repo.merge_incoming(&mut incoming_doc).map_err(|e| e.to_string())?;
    let encrypted_merged = crypto.encrypt(&merged_bytes)?;
    let bytes = serde_json::to_vec(&encrypted_merged).map_err(|e| e.to_string())?;
    store.save(&bytes).await.map_err(|e| e.to_string())?;
    Ok(())
}

pub fn get_encrypted_local_doc(
    repo: &AutomergeTodoRepo,
    crypto: &CryptoEngine,
) -> Result<Option<EncryptedPayload>, String> {
    let local_bytes = repo.get_doc_bytes();
    if local_bytes.is_empty() {
        return Ok(None);
    }
    crypto.encrypt(&local_bytes).map(Some)
}

pub async fn send_client_message<S>(write: &mut S, msg: &ClientMessage) -> Result<(), String>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let json = serde_json::to_string(msg).map_err(|e| e.to_string())?;
    write
        .send(Message::Text(json.into()))
        .await
        .map_err(|e| e.to_string())
}

pub async fn request_sync_if_missing<S>(
    write: &mut S,
    highest_observed_seq: u64,
    missing_deltas: &BTreeSet<u64>,
) -> Result<(), String>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    if !missing_deltas.is_empty() {
        let continuous_seq = get_highest_continuous_seq(highest_observed_seq, missing_deltas);
        let req_sync = ClientMessage::RequestSync {
            from_seq_id: continuous_seq,
        };
        send_client_message(write, &req_sync).await?;
    }
    Ok(())
}

pub fn schedule_snapshot_if_eligible(
    repo: &AutomergeTodoRepo,
    crypto: &CryptoEngine,
    highest_observed_seq: u64,
    missing_deltas: &BTreeSet<u64>,
    pending_snapshot: &mut Option<PendingSnapshot>,
    snapshot_debounce: &mut Option<Pin<Box<tokio::time::Sleep>>>,
    debounce_duration: Duration,
) -> Result<(), String> {
    let continuous_seq = get_highest_continuous_seq(highest_observed_seq, missing_deltas);
    if missing_deltas.is_empty() && continuous_seq > 0 {
        let should_update = match pending_snapshot.as_ref() {
            Some(pending) => continuous_seq > pending.covers_seq_id,
            None => true,
        };

        if should_update {
            if let Some(payload) = get_encrypted_local_doc(repo, crypto)? {
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
    Ok(())
}

pub async fn flush_pending_snapshot_on_shutdown<S>(
    write: &mut S,
    pending_snapshot: &mut Option<PendingSnapshot>,
    repo: &AutomergeTodoRepo,
    crypto: &CryptoEngine,
    highest_observed_seq: u64,
    missing_deltas: &BTreeSet<u64>,
) -> Result<(), String>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let continuous_seq = get_highest_continuous_seq(highest_observed_seq, missing_deltas);
    let snapshot_to_send = match pending_snapshot.take() {
        Some(pending) => Some(pending),
        None => {
            if missing_deltas.is_empty() && continuous_seq > 0 {
                get_encrypted_local_doc(repo, crypto)?
                    .map(|payload| PendingSnapshot { covers_seq_id: continuous_seq, payload })
            } else {
                None
            }
        }
    };

    if let Some(pending) = snapshot_to_send {
        let client_msg = ClientMessage::Snapshot {
            covers_seq_id: pending.covers_seq_id,
            payload: pending.payload,
        };
        send_client_message(write, &client_msg).await?;
        tracing::info!(
            "Pushed immediate snapshot on shutdown (covers seq_id: {}) to server!",
            pending.covers_seq_id
        );
    }
    Ok(())
}
