mod constants;
mod helper;

use crate::automerge::AutomergeTodoRepo;
use crate::crypto::CryptoEngine;
use crate::models::SyncStatus;
use crate::store::SqliteBackingStore;
use constants::{RECONNECT_DELAY_SECS, SNAPSHOT_DEBOUNCE_SECS, WS_URL};
use futures_util::StreamExt;
use helper::{
    decrypt_merge_and_persist, flush_pending_snapshot_on_shutdown, get_encrypted_local_doc,
    get_highest_continuous_seq, record_observed_seq, request_sync_if_missing,
    schedule_snapshot_if_eligible, send_client_message, PendingSnapshot,
};
use shared::{ClientMessage, ServerMessage};
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
        let _ = app_handle.emit("sync-status", SyncStatus::Connecting);
        info!("Attempting to connect to sync server at {}", WS_URL);
        let (ws_stream, _) = match connect_async(WS_URL).await {
            Ok(res) => res,
            Err(e) => {
                warn!(
                    "Sync server not available ({}). Retrying in {} seconds...",
                    e, RECONNECT_DELAY_SECS
                );
                let _ = app_handle.emit("sync-status", SyncStatus::Disconnected);
                tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
                continue;
            }
        };

        info!("Connected to sync server!");
        let (mut write, mut read) = ws_stream.split();

        let (highest_observed_seq, mut missing_deltas) = match store.get_sync_state().await {
            Ok(state) => state,
            Err(e) => {
                error!("Failed to retrieve sync state from SQLite store: {}", e);
                let _ = app_handle.emit(
                    "sync-status",
                    SyncStatus::Error(format!("SQLite sync state error: {}", e)),
                );
                tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
                continue;
            }
        };
        let mut highest_observed_seq = highest_observed_seq;
        let _ = app_handle.emit("sync-status", SyncStatus::Connected);

        let debounce_duration = Duration::from_secs(SNAPSHOT_DEBOUNCE_SECS);
        let mut pending_snapshot: Option<PendingSnapshot> = None;
        let mut snapshot_debounce: Option<Pin<Box<tokio::time::Sleep>>> = None;

        if let Err(e) = schedule_snapshot_if_eligible(
            &repo,
            &crypto,
            highest_observed_seq,
            &missing_deltas,
            &mut pending_snapshot,
            &mut snapshot_debounce,
            debounce_duration,
        ) {
            error!("Failed to schedule initial snapshot: {}", e);
        }

        loop {
            tokio::select! {
                // Incoming message from server
                Some(msg) = read.next() => {
                    let text = match msg {
                        Ok(Message::Text(text)) => text,
                        Ok(Message::Close(frame)) => {
                            warn!("Server WebSocket closed connection: {:?}", frame);
                            let _ = app_handle.emit("sync-status", SyncStatus::Disconnected);
                            break;
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                        Ok(other) => {
                            warn!("Received unexpected WebSocket message: {:?}", other);
                            continue;
                        }
                        Err(e) => {
                            error!("WebSocket read error: {}", e);
                            let _ = app_handle.emit("sync-status", SyncStatus::Disconnected);
                            break;
                        }
                    };

                    let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) else {
                        error!("Failed to deserialize ServerMessage JSON: {}", text);
                        continue;
                    };

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

                            match decrypt_merge_and_persist(&repo, &crypto, &store, &payload).await {
                                Ok(()) => {
                                    missing_deltas.retain(|&s| s > seq_id);
                                    info!("Successfully merged incoming Snapshot (seq_id: {})", seq_id);
                                    if let Err(e) = store.save_sync_state(highest_observed_seq, &missing_deltas).await {
                                        error!("Failed to save sync state to SQLite: {}", e);
                                    }
                                    if let Err(e) = app_handle.emit("todos-updated", ()) {
                                        error!("Failed to emit todos-updated event: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to merge incoming Snapshot (seq_id: {}): {}", seq_id, e);
                                }
                            }
                        }
                        ServerMessage::DeltaBatch { deltas } => {
                            let mut merged_any = false;
                            for (seq_id, payload) in deltas {
                                record_observed_seq(seq_id, &mut highest_observed_seq, &mut missing_deltas);

                                match decrypt_merge_and_persist(&repo, &crypto, &store, &payload).await {
                                    Ok(()) => {
                                        merged_any = true;
                                        // Only mark delta as satisfied if decrypt & merge succeeded
                                        missing_deltas.remove(&seq_id);
                                    }
                                    Err(e) => {
                                        error!("Failed to merge incoming delta (seq_id: {}): {}", seq_id, e);
                                    }
                                }
                            }

                            if let Err(e) = store.save_sync_state(highest_observed_seq, &missing_deltas).await {
                                error!("Failed to save sync state to SQLite: {}", e);
                            }

                            if merged_any {
                                info!("Successfully merged incoming DeltaBatch");
                                if let Err(e) = app_handle.emit("todos-updated", ()) {
                                    error!("Failed to emit todos-updated event: {}", e);
                                }
                                if let Err(e) = schedule_snapshot_if_eligible(
                                    &repo,
                                    &crypto,
                                    highest_observed_seq,
                                    &missing_deltas,
                                    &mut pending_snapshot,
                                    &mut snapshot_debounce,
                                    debounce_duration,
                                ) {
                                    error!("Failed to schedule snapshot: {}", e);
                                }
                            }

                            if let Err(e) = request_sync_if_missing(&mut write, highest_observed_seq, &missing_deltas).await {
                                error!("Failed to send RequestSync to server: {}", e);
                            }
                        }
                        ServerMessage::Ack { seq_id } => {
                            record_observed_seq(seq_id, &mut highest_observed_seq, &mut missing_deltas);
                            // Ack indicates a sequence ID position confirmed by server
                            missing_deltas.remove(&seq_id);
                            if let Err(e) = store.save_sync_state(highest_observed_seq, &missing_deltas).await {
                                error!("Failed to save sync state to SQLite: {}", e);
                            }

                            let continuous_seq = get_highest_continuous_seq(highest_observed_seq, &missing_deltas);
                            debug!("Received Ack from server for seq_id: {} (continuous_seq: {})", seq_id, continuous_seq);

                            if let Err(e) = schedule_snapshot_if_eligible(
                                &repo,
                                &crypto,
                                highest_observed_seq,
                                &missing_deltas,
                                &mut pending_snapshot,
                                &mut snapshot_debounce,
                                debounce_duration,
                            ) {
                                error!("Failed to schedule snapshot: {}", e);
                            }

                            if let Err(e) = request_sync_if_missing(&mut write, highest_observed_seq, &missing_deltas).await {
                                error!("Failed to send RequestSync to server: {}", e);
                            }
                        }
                    }
                }

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
                            match send_client_message(&mut write, &client_msg).await {
                                Ok(()) => {
                                    info!("Pushed debounced snapshot (covers seq_id: {}) to server!", pending.covers_seq_id);
                                }
                                Err(e) => {
                                    error!("Failed to send debounced snapshot to server: {}", e);
                                }
                            }
                        } else {
                            debug!("Skipping debounced snapshot: missing_deltas has {} holes, continuous_seq={}", missing_deltas.len(), continuous_seq);
                        }
                    }
                }

                // Local change notification or shutdown when sender drops
                change_msg = rx.recv() => {
                    match change_msg {
                        Some(_) => {
                            match get_encrypted_local_doc(&repo, &crypto) {
                                Ok(Some(payload)) => {
                                    let client_msg = ClientMessage::Delta { payload };
                                    if let Err(e) = send_client_message(&mut write, &client_msg).await {
                                        error!("Failed to push local delta update to server: {}", e);
                                        break;
                                    } else {
                                        info!("Pushed local delta update immediately to server!");
                                        if let Err(e) = schedule_snapshot_if_eligible(
                                            &repo,
                                            &crypto,
                                            highest_observed_seq,
                                            &missing_deltas,
                                            &mut pending_snapshot,
                                            &mut snapshot_debounce,
                                            debounce_duration,
                                        ) {
                                            error!("Failed to schedule snapshot: {}", e);
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    error!("Failed to encrypt local document for delta update: {}", e);
                                }
                            }
                        }
                        None => {
                            info!("Shutdown signal received: firing pending snapshot immediately...");
                            if let Err(e) = flush_pending_snapshot_on_shutdown(
                                &mut write,
                                &mut pending_snapshot,
                                &repo,
                                &crypto,
                                highest_observed_seq,
                                &missing_deltas,
                            ).await {
                                error!("Failed to flush snapshot on shutdown: {}", e);
                            }
                            return;
                        }
                    }
                }

                // Process shutdown signal (Ctrl+C)
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutdown signal (Ctrl+C) received: firing pending snapshot immediately...");
                    if let Err(e) = flush_pending_snapshot_on_shutdown(
                        &mut write,
                        &mut pending_snapshot,
                        &repo,
                        &crypto,
                        highest_observed_seq,
                        &missing_deltas,
                    ).await {
                        error!("Failed to flush snapshot on shutdown: {}", e);
                    }
                    return;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
    }
}
