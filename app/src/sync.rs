mod constants;
mod helper;

use crate::models::SyncStatus;
use crate::storage::SqliteStorage;
use automerge::sync::State as SyncState;
use automerge::{sync::SyncDoc, AutoCommit};
use constants::{RECONNECT_DELAY_SECS, WS_URL};
use futures_util::{SinkExt, StreamExt};
use helper::send_sync_message;
use std::sync::Arc;
use tauri::Emitter;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};

/// Sends this peer's pending outgoing sync message (if any) over the WebSocket
/// as a binary frame.
///
/// This is single-shot rather than a drain loop: after `generate_sync_message`
/// emits a message it sets the peer `state`'s `in_flight` flag, which is only
/// cleared by `receive_sync_message`. With no intervening receive, an immediate
/// second call is guaranteed to return `None`, so a `while let` would iterate
/// exactly once here anyway — one message per triggering event (inbound frame or
/// local change), then the server must respond before more is generated.
async fn send_outgoing<S>(
    doc: &tokio::sync::RwLock<AutoCommit>,
    state: &mut SyncState,
    write: &mut S,
) -> Result<(), String>
where
    S: futures_util::SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut doc_lock = doc.write().await;
    let msg = doc_lock
        .sync()
        .generate_sync_message(state)
        .map(|m| m.encode());
    if let Some(data) = msg {
        send_sync_message(write, &data).await?;
    }
    Ok(())
}

pub async fn sync_engine(
    doc: Arc<tokio::sync::RwLock<AutoCommit>>,
    storage: Arc<SqliteStorage>,
    app_handle: tauri::AppHandle,
    sync_status: Arc<std::sync::RwLock<SyncStatus>>,
    mut rx: mpsc::UnboundedReceiver<()>,
    mut shutdown_rx: tokio::sync::watch::Receiver<()>,
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

        // A fresh sync state is created per connection; peers renegotiate from
        // scratch on reconnect (no head bookkeeping is persisted).
        let mut sync_state = SyncState::new();
        set_status(SyncStatus::Connected);

        // Drive the handshake: send our initial sync message(s) so the server
        // learns what we have and returns what we are missing.
        if let Err(e) = send_outgoing(&doc, &mut sync_state, &mut write).await {
            error!("Failed to send initial sync message to server: {}", e);
            set_status(SyncStatus::Disconnected);
            tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
            continue;
        }

        loop {
            tokio::select! {
                // Incoming binary sync message from server
                Some(msg) = read.next() => {
                    let data = match msg {
                        Ok(Message::Binary(data)) => data,
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

                    let changed = {
                        let mut doc_lock = doc.write().await;
                        let heads_before = doc_lock.get_heads();
                        let msg = automerge::sync::Message::decode(&data).unwrap();
                        doc_lock.sync().receive_sync_message(&mut sync_state, msg).unwrap();
                        let heads_after = doc_lock.get_heads();
                        heads_before != heads_after
                    };

                    // Only persist and notify the frontend when the sync message
                    // actually advanced the document. Protocol-only exchanges
                    // (e.g. acknowledgements after our own local change) leave the
                    // document untouched and must not echo a `todos-updated` event.
                    if changed {
                        // Persist the updated document (plaintext structure,
                        // ciphertext values) at rest.
                        let doc_bytes = doc.write().await.save();
                        if let Err(e) = storage.save(&doc_bytes).await {
                            error!("Failed to persist document after applying sync message: {}", e);
                        }

                        info!("Applied incoming sync message");
                        if let Err(e) = app_handle.emit("todos-updated", ()) {
                            error!("Failed to emit todos-updated event: {}", e);
                        }
                    }

                    // Respond with any follow-up messages the protocol needs.
                    if let Err(e) = send_outgoing(&doc, &mut sync_state, &mut write).await {
                        error!("Failed to send follow-up sync messages: {}", e);
                        set_status(SyncStatus::Disconnected);
                        break;
                    }
                }

                // Explicit shutdown signal
                _ = shutdown_rx.changed() => {
                    info!("Shutdown signal received: closing sync engine...");
                    let _ = write.close().await;
                    return;
                }

                // Local change notification or shutdown when sender drops
                change_msg = rx.recv() => {
                    match change_msg {
                        Some(_) => {
                            if let Err(e) = send_outgoing(&doc, &mut sync_state, &mut write).await {
                                error!("Failed to push local changes to server: {}", e);
                                set_status(SyncStatus::Disconnected);
                                break;
                            }
                            info!("Pushed local changes immediately to server!");
                        }
                        None => {
                            info!("Shutdown signal received: closing sync engine...");
                            let _ = write.close().await;
                            return;
                        }
                    }
                }

                // Process shutdown signal (Ctrl+C)
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutdown signal (Ctrl+C) received: closing sync engine...");
                    let _ = write.close().await;
                    return;
                }
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)) => {}
            _ = shutdown_rx.changed() => {
                return;
            }
        }
    }
}
