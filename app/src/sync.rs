mod constants;
mod helper;

use crate::automerge::AutomergeTodoRepo;
use crate::crypto::CryptoEngine;
use crate::store::SqliteBackingStore;
use constants::{RECONNECT_DELAY_SECS, SNAPSHOT_INTERVAL_MINUTES, WS_URL};
use futures_util::StreamExt;
use helper::{
    decrypt_merge_and_persist, get_encrypted_local_doc, get_highest_continuous_seq,
    record_observed_seq, request_sync_if_missing, send_client_message,
};
use shared::{ClientMessage, ServerMessage};
use std::collections::BTreeSet;
use std::sync::Arc;
use tauri::Emitter;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
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

        // Periodic snapshot timer (e.g. every 5 minutes)
        let mut snapshot_interval = interval(Duration::from_secs(SNAPSHOT_INTERVAL_MINUTES * 60));
        // Skip the immediate first tick on connection
        snapshot_interval.tick().await;

        loop {
            tokio::select! {
                // Periodic Snapshot Timer Triggered
                _ = snapshot_interval.tick() => {
                    let continuous_seq = get_highest_continuous_seq(highest_observed_seq, &missing_deltas);
                    // A snapshot can ONLY be uploaded if we are fully caught up without any missing holes
                    if missing_deltas.is_empty() && continuous_seq > 0 {
                        if let Some(payload) = get_encrypted_local_doc(&repo, &crypto) {
                            let client_msg = ClientMessage::Snapshot {
                                covers_seq_id: continuous_seq,
                                payload,
                            };
                            if send_client_message(&mut write, &client_msg).await.is_ok() {
                                info!("Pushed periodic snapshot (covers seq_id: {}) to server!", continuous_seq);
                            }
                        }
                    } else {
                        debug!("Skipping periodic snapshot: missing_deltas has {} holes, continuous_seq={}", missing_deltas.len(), continuous_seq);
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

                // Local change notification triggered from Tauri command
                Some(_) = rx.recv() => {
                    if let Some(payload) = get_encrypted_local_doc(&repo, &crypto) {
                        let client_msg = ClientMessage::Delta { payload };
                        if send_client_message(&mut write, &client_msg).await.is_err() {
                            error!("Failed to push local delta update to server");
                            break;
                        } else {
                            info!("Pushed local delta update immediately to server!");
                        }
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
    }
}
