mod constants;
mod helper;

use crate::automerge::AutomergeTodoRepo;
use crate::models::SyncStatus;
use crate::storage::SqliteStorage;
use automerge::sync::State as SyncState;
use constants::{RECONNECT_DELAY_SECS, WS_URL};
use futures_util::StreamExt;
use helper::send_sync_message;
use std::sync::Arc;
use tauri::Emitter;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};

/// Drains outgoing sync messages for the current peer `state`, sending each over
/// the WebSocket as a binary frame until `generate_sync_message` yields nothing more.
async fn drain_outgoing<S>(
    repo: &AutomergeTodoRepo,
    state: &mut SyncState,
    write: &mut S,
) -> Result<(), String>
where
    S: futures_util::SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    while let Some(data) = repo.generate_sync_message(state)? {
        send_sync_message(write, &data).await?;
    }
    Ok(())
}

pub async fn sync_engine(
    repo: Arc<AutomergeTodoRepo>,
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

        // A fresh sync state is created per connection; peers renegotiate from
        // scratch on reconnect (no head bookkeeping is persisted).
        let mut sync_state = SyncState::new();
        set_status(SyncStatus::Connected);

        // Drive the handshake: send our initial sync message(s) so the server
        // learns what we have and returns what we are missing.
        if let Err(e) = drain_outgoing(&repo, &mut sync_state, &mut write).await {
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

                    if let Err(e) = repo.receive_sync_message(&mut sync_state, &data) {
                        error!("Failed to apply incoming sync message: {}", e);
                        continue;
                    }

                    // Persist the updated document (plaintext structure,
                    // ciphertext values) at rest.
                    let doc_bytes = repo.get_doc_bytes();
                    if let Err(e) = storage.save(&doc_bytes).await {
                        error!("Failed to persist document after applying sync message: {}", e);
                    }

                    info!("Applied incoming sync message");
                    if let Err(e) = app_handle.emit("todos-updated", ()) {
                        error!("Failed to emit todos-updated event: {}", e);
                    }

                    // Respond with any follow-up messages the protocol needs.
                    if let Err(e) = drain_outgoing(&repo, &mut sync_state, &mut write).await {
                        error!("Failed to send follow-up sync messages: {}", e);
                        set_status(SyncStatus::Disconnected);
                        break;
                    }
                }

                // Local change notification or shutdown when sender drops
                change_msg = rx.recv() => {
                    match change_msg {
                        Some(_) => {
                            if let Err(e) = drain_outgoing(&repo, &mut sync_state, &mut write).await {
                                error!("Failed to push local changes to server: {}", e);
                                set_status(SyncStatus::Disconnected);
                                break;
                            }
                            info!("Pushed local changes immediately to server!");
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
