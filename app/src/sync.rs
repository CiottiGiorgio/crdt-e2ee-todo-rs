mod constants;
mod helper;

use crate::automerge::AutomergeTodoRepo;
use crate::crypto::CryptoEngine;
use crate::store::SqliteBackingStore;
use constants::{RECONNECT_DELAY_SECS, SNAPSHOT_DEBOUNCE_SECS, WS_URL};
use futures_util::StreamExt;
use helper::{
    decrypt_merge_and_persist, flush_pending_snapshot_on_shutdown, get_encrypted_local_doc,
    get_highest_continuous_seq, record_observed_seq, request_sync_if_missing,
    schedule_snapshot_if_eligible, send_client_message, PendingSnapshot,
};
use shared::{ClientMessage, ServerMessage};
use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;
use tauri::Emitter;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

pub async fn sync_engine(
    repo: Arc<AutomergeTodoRepo>,
    crypto: Arc<CryptoEngine>,
    store: Arc<SqliteBackingStore>,
    app_handle: tauri::AppHandle,
    mut rx: mpsc::UnboundedReceiver<()>,
) {
    loop {
        info!("Attempting to connect to sync server at {}", WS_URL);
        let Ok((ws_stream, _)) = connect_async(WS_URL).await.map_err(|e| {
            warn!(
                "Sync server not available ({}). Retrying in {} seconds...",
                e, RECONNECT_DELAY_SECS
            );
        }) else {
            tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
            continue;
        };

        info!("Connected to sync server!");
        let (mut write, mut read) = ws_stream.split();

        let mut highest_observed_seq: u64 = 0;
        let mut missing_deltas: BTreeSet<u64> = BTreeSet::new();

        if let Ok((highest, missing)) = store.get_sync_state().await {
            highest_observed_seq = highest;
            missing_deltas = missing;
        }

        let debounce_duration = Duration::from_secs(SNAPSHOT_DEBOUNCE_SECS);
        let mut pending_snapshot: Option<PendingSnapshot> = None;
        let mut snapshot_debounce: Option<Pin<Box<tokio::time::Sleep>>> = None;

        schedule_snapshot_if_eligible(
            &repo,
            &crypto,
            highest_observed_seq,
            &missing_deltas,
            &mut pending_snapshot,
            &mut snapshot_debounce,
            debounce_duration,
        );

        loop {
            tokio::select! {
                // Debounced Snapshot Timer Triggered
                _ = async { snapshot_debounce.as_mut().unwrap().await }, if snapshot_debounce.is_some() => {
                    snapshot_debounce = None;
                    if let Some(pending) = pending_snapshot.take() {
                        let continuous_seq = get_highest_continuous_seq(highest_observed_seq, &missing_deltas);
                        // A snapshot can ONLY be uploaded if we are fully caught up without any missing holes
                        if missing_deltas.is_empty() && continuous_seq >= pending.covers_seq_id {
                            let client_msg = ClientMessage::Snapshot {
                                covers_seq_id: pending.covers_seq_id,
                                payload: pending.payload,
                            };
                            if send_client_message(&mut write, &client_msg).await.is_ok() {
                                info!("Pushed debounced snapshot (covers seq_id: {}) to server!", pending.covers_seq_id);
                            }
                        } else {
                            debug!("Skipping debounced snapshot: missing_deltas has {} holes, continuous_seq={}", missing_deltas.len(), continuous_seq);
                        }
                    }
                }

                // Incoming message from server
                Some(msg) = read.next() => {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) {
                                match server_msg {
                                    ServerMessage::Snapshot { seq_id, payload } => {
                                        record_observed_seq(seq_id, &mut highest_observed_seq, &mut missing_deltas);

                                        // Cancel any pending snapshot in the chamber covered by this incoming snapshot
                                        if let Some(pending) = &pending_snapshot {
                                            if seq_id >= pending.covers_seq_id {
                                                debug!(
                                                    "Cancelled chambered snapshot (covers seq_id: {}) because incoming snapshot (seq_id: {}) covers it",
                                                    pending.covers_seq_id, seq_id
                                                );
                                                pending_snapshot = None;
                                                snapshot_debounce = None;
                                            }
                                        }

                                        if decrypt_merge_and_persist(&repo, &crypto, &store, &payload).await {
                                            missing_deltas.retain(|&s| s > seq_id);
                                            info!("Successfully merged incoming Snapshot (seq_id: {})", seq_id);
                                            let _ = store.save_sync_state(highest_observed_seq, &missing_deltas).await;
                                            let _ = app_handle.emit("todos-updated", ());
                                        }
                                    }
                                    ServerMessage::DeltaBatch { deltas } => {
                                        let mut merged_any = false;
                                        for (seq_id, payload) in deltas {
                                            record_observed_seq(seq_id, &mut highest_observed_seq, &mut missing_deltas);

                                            if decrypt_merge_and_persist(&repo, &crypto, &store, &payload).await {
                                                merged_any = true;
                                                // Only mark delta as satisfied if decrypt & merge succeeded
                                                missing_deltas.remove(&seq_id);
                                            }
                                        }

                                        let _ = store.save_sync_state(highest_observed_seq, &missing_deltas).await;

                                        if merged_any {
                                            info!("Successfully merged incoming DeltaBatch");
                                            let _ = app_handle.emit("todos-updated", ());
                                            schedule_snapshot_if_eligible(
                                                &repo,
                                                &crypto,
                                                highest_observed_seq,
                                                &missing_deltas,
                                                &mut pending_snapshot,
                                                &mut snapshot_debounce,
                                                debounce_duration,
                                            );
                                        }

                                        request_sync_if_missing(&mut write, highest_observed_seq, &missing_deltas).await;
                                    }
                                    ServerMessage::Ack { seq_id } => {
                                        record_observed_seq(seq_id, &mut highest_observed_seq, &mut missing_deltas);
                                        // Ack indicates a sequence ID position confirmed by server
                                        missing_deltas.remove(&seq_id);
                                        let _ = store.save_sync_state(highest_observed_seq, &missing_deltas).await;

                                        let continuous_seq = get_highest_continuous_seq(highest_observed_seq, &missing_deltas);
                                        debug!("Received Ack from server for seq_id: {} (continuous_seq: {})", seq_id, continuous_seq);

                                        schedule_snapshot_if_eligible(
                                            &repo,
                                            &crypto,
                                            highest_observed_seq,
                                            &missing_deltas,
                                            &mut pending_snapshot,
                                            &mut snapshot_debounce,
                                            debounce_duration,
                                        );

                                        request_sync_if_missing(&mut write, highest_observed_seq, &missing_deltas).await;
                                    }
                                }
                            }
                        }
                        Ok(Message::Close(_)) | Err(_) => {
                            warn!("Server WebSocket connection closed");
                            break;
                        }
                        _ => {}
                    }
                }

                // Local change notification or shutdown when sender drops
                change_msg = rx.recv() => {
                    match change_msg {
                        Some(_) => {
                            if let Some(payload) = get_encrypted_local_doc(&repo, &crypto) {
                                let client_msg = ClientMessage::Delta { payload };
                                if send_client_message(&mut write, &client_msg).await.is_err() {
                                    error!("Failed to push local delta update to server");
                                    break;
                                } else {
                                    info!("Pushed local delta update immediately to server!");
                                    schedule_snapshot_if_eligible(
                                        &repo,
                                        &crypto,
                                        highest_observed_seq,
                                        &missing_deltas,
                                        &mut pending_snapshot,
                                        &mut snapshot_debounce,
                                        debounce_duration,
                                    );
                                }
                            }
                        }
                        None => {
                            info!("Shutdown signal received: firing pending snapshot immediately...");
                            flush_pending_snapshot_on_shutdown(
                                &mut write,
                                &mut pending_snapshot,
                                &repo,
                                &crypto,
                                highest_observed_seq,
                                &missing_deltas,
                            ).await;
                            return;
                        }
                    }
                }

                // Process shutdown signal (Ctrl+C)
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutdown signal (Ctrl+C) received: firing pending snapshot immediately...");
                    flush_pending_snapshot_on_shutdown(
                        &mut write,
                        &mut pending_snapshot,
                        &repo,
                        &crypto,
                        highest_observed_seq,
                        &missing_deltas,
                    ).await;
                    return;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
    }
}
