use crate::{AppState, CLIENT_COUNTER};
use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use std::sync::atomic::Ordering;
use thiserror::Error;
use tokio::sync::watch;
use tracing::{debug, error, info};

#[derive(Debug, Error)]
pub enum SocketHandlerError {
    #[error("Automerge sync error: {0}")]
    Automerge(#[from] automerge::AutomergeError),

    #[error("Database persistence error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("WebSocket transport error: {0}")]
    WebSocket(#[from] axum::Error),

    #[error("Sync wake-up signal error: {0}")]
    WakeUp(#[from] watch::error::RecvError),
}

pub async fn handle_socket(socket: WebSocket, state: AppState) -> Result<(), SocketHandlerError> {
    let my_client_id = CLIENT_COUNTER.fetch_add(1, Ordering::SeqCst);
    info!("Client {} connected", my_client_id);

    let (mut sender, mut receiver) = socket.split();
    let mut wake_up = state.sync_wake_up.subscribe();

    let mut sync_state = SyncState::new();
    loop {
        tokio::select! {
            msg_opt = receiver.next() => {
                let msg = match msg_opt {
                    Some(Ok(msg)) => { msg }
                    Some(Err(e)) => {
                        debug!("WebSocket read error from client {}: {}", my_client_id, e);
                        break;
                    }
                    None => { break; }
                };
                let data = match msg {
                    Message::Binary(data) => data,
                    Message::Close(frame) => {
                        info!("Client {} closed connection: {:?}", my_client_id, frame);
                        break;
                    }
                    Message::Ping(_) | Message::Pong(_) => continue,
                    other => {
                        tracing::warn!(
                            "Received unexpected WebSocket message from client {}: {:?}",
                            my_client_id,
                            other
                        );
                        continue;
                    }
                };
                let msg = match SyncMessage::decode(&data) {
                    Ok(msg) => msg,
                    Err(e) => {
                        error!(
                            "Failed to decode sync message from client {}: {}",
                            my_client_id, e
                        );
                        continue;
                    }
                };

                let (bytes_to_save, response_msg) = {
                    let mut doc_guard = state.doc.write().await;
                    let heads_pre_merge = doc_guard.get_heads();
                    doc_guard.sync().receive_sync_message(&mut sync_state, msg)?;
                    let heads_post_merge = doc_guard.get_heads();

                    let mut bytes_to_save = None;
                    if heads_pre_merge != heads_post_merge {
                        bytes_to_save = Some(doc_guard.save());
                    }

                    let response_msg = doc_guard.sync().generate_sync_message(&mut sync_state);

                    (bytes_to_save, response_msg)
                };

                if let Some(bytes_to_save) = bytes_to_save {
                    state.store.save_doc(&bytes_to_save).await?;
                    let _ = state.sync_wake_up.send(());
                }

                if let Some(response_msg) = response_msg {
                    sender.send(response_msg.encode().into()).await?;
                }
            }
            wake_up_res = wake_up.changed() => {
                wake_up_res?;
                let sync_message = {
                    state.doc.write().await.sync().generate_sync_message(&mut sync_state)
                };
                if let Some(data) = sync_message {
                    sender.send(data.encode().into()).await?;
                }
            }
        }
    }

    info!("Client {} disconnected", my_client_id);

    Ok(())
}
