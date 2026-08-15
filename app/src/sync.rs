mod constants;
mod helper;

use crate::automerge::AutomergeTodoRepo;
use crate::crypto::CryptoEngine;
use crate::models::SyncStatus;
use crate::storage::SqliteStorage;
use constants::{RECONNECT_DELAY_SECS, WS_URL};
use futures_util::StreamExt;
use helper::{
    decrypt_merge_and_persist, get_encrypted_local_doc, get_highest_continuous_seq,
    record_observed_seq, request_sync_if_missing, send_client_message,
};
use shared::{ClientMessage, ServerMessage};
use std::sync::Arc;
use tauri::Emitter;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};

pub async fn sync_engine(
    repo: Arc<AutomergeTodoRepo>,
    crypto: Arc<CryptoEngine>,
    storage: Arc<SqliteStorage>,
    app_handle: tauri::AppHandle,
    sync_status: Arc<std::sync::RwLock<SyncStatus>>,
    mut rx: mpsc::UnboundedReceiver<()>,
) {
    let set_status = |status: SyncStatus| {
        if let Ok(mut lock) = sync_status.write() {
            *lock = status.clone();
        }
        let _ = app_handle.emit("sync-status", status);
    };

    loop {
        set_status(SyncStatus::Connecting);
        info!("Attempting to connect to sync server at {}", WS_URL);
        let (ws_stream, _) = match connect_async(WS_URL).await {
            Ok(res) => res,
            Err(e) => {
                warn!(
                    "Sync server not available ({}). Retrying in {} seconds...",
                    e, RECONNECT_DELAY_SECS
                );
                set_status(SyncStatus::Disconnected);
                tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
                continue;
            }
        };

        info!("Connected to sync server!");
        let (mut write, mut read) = ws_stream.split();

        let (mut highest_observed_seq, mut missing_deltas) = match storage.get_sync_state().await {
            Ok(state) => state,
            Err(e) => {
                error!("Failed to retrieve sync state from SQLite storage: {}", e);
                set_status(SyncStatus::Error(format!("SQLite sync state error: {}", e)));
                tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
                continue;
            }
        };
        set_status(SyncStatus::Connected);

        // Immediately request missing deltas on connect
        let continuous_seq = get_highest_continuous_seq(highest_observed_seq, &missing_deltas);
        let req_sync = ClientMessage::RequestSync {
            from_seq_id: continuous_seq,
        };
        if let Err(e) = send_client_message(&mut write, &req_sync).await {
            error!("Failed to send initial RequestSync to server: {}", e);
            set_status(SyncStatus::Disconnected);
            tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
            continue;
        }

        loop {
            tokio::select! {
                // Incoming message from server
                Some(msg) = read.next() => {
                    let text = match msg {
                        Ok(Message::Text(text)) => text,
                        Ok(Message::Close(frame)) => {
                            warn!("Server WebSocket closed connection: {:?}", frame);
                            set_status(SyncStatus::Disconnected);
                            break;
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                        Ok(other) => {
                            warn!("Received unexpected WebSocket message: {:?}", other);
                            continue;
                        }
                        Err(e) => {
                            error!("WebSocket read error: {}", e);
                            set_status(SyncStatus::Disconnected);
                            break;
                        }
                    };

                    let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) else {
                        error!("Failed to deserialize ServerMessage JSON: {}", text);
                        continue;
                    };

                    match server_msg {
                        ServerMessage::DeltaBatch { deltas } => {
                            if let Some(max_seq) = deltas.iter().map(|(seq, _)| *seq).max() {
                                record_observed_seq(max_seq, &mut highest_observed_seq, &mut missing_deltas);
                            }

                            let mut merged_any = false;
                            for (seq_id, payload) in deltas {
                                match decrypt_merge_and_persist(&repo, &crypto, &storage, &payload).await {
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

                            if let Err(e) = storage.save_sync_state(highest_observed_seq, &missing_deltas).await {
                                error!("Failed to save sync state to SQLite: {}", e);
                            }

                            if merged_any {
                                info!("Successfully merged incoming DeltaBatch");
                                if let Err(e) = app_handle.emit("todos-updated", ()) {
                                    error!("Failed to emit todos-updated event: {}", e);
                                }
                            }

                            if let Err(e) = request_sync_if_missing(&mut write, highest_observed_seq, &missing_deltas).await {
                                error!("Failed to send RequestSync to server: {}", e);
                            }
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
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    error!("Failed to encrypt local document for delta update: {}", e);
                                }
                            }
                        }
                        None => {
                            info!("Shutdown signal received: closing sync engine...");
                            return;
                        }
                    }
                }

                // Process shutdown signal (Ctrl+C)
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutdown signal (Ctrl+C) received: closing sync engine...");
                    return;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
    }
}
