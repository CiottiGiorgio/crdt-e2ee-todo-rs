use crate::AppState;
use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

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

    #[error("Database persistence ({db}) and WebSocket transport ({ws}) both failed")]
    DatabaseAndWebSocket {
        db: sqlx::Error,
        ws: axum::Error,
    },
}

pub async fn handle_socket(socket: WebSocket, state: AppState) -> Result<(), SocketHandlerError> {
    info!("Connected");

    let (mut sender, mut receiver) = socket.split();
    let mut wake_up = state.sync_wake_up.subscribe();

    let mut sync_state = SyncState::new();
    loop {
        tokio::select! {
            msg_opt = receiver.next() => {
                let msg = match msg_opt {
                    Some(Ok(msg)) => { msg }
                    Some(Err(e)) => {
                        debug!("WebSocket read error: {}", e);
                        break;
                    }
                    None => {
                        debug!("Connection reached EOF");
                        break;
                    }
                };
                let data = match msg {
                    Message::Binary(data) => data,
                    Message::Close(_) => break,
                    Message::Ping(_) => continue,
                    Message::Pong(_) => continue,
                    other => {
                        warn!("Received unexpected WebSocket message: {:?}", other);
                        continue;
                    }
                };
                let msg = match SyncMessage::decode(&data) {
                    Ok(msg) => {
                        debug!("Received a sync message");
                        msg
                    },
                    Err(e) => {
                        error!("Failed to decode sync message {}", e);
                        continue;
                    }
                };

                let (bytes_to_save, response_msg) = {
                    let mut doc_guard = state.doc.write().await;
                    let heads_pre_merge = doc_guard.get_heads();
                    doc_guard.receive_sync_message(&mut sync_state, msg)?;
                    let doc_guard = doc_guard.downgrade();
                    let heads_post_merge = doc_guard.get_heads();

                    let bytes_to_save = if heads_pre_merge != heads_post_merge {
                        info!("Applied sync changes (heads: {:?} -> {:?})", heads_pre_merge, heads_post_merge);
                        let bytes = doc_guard.save();
                        let _ = state.sync_wake_up.send(());

                        Some(bytes)
                    } else {
                        debug!("Processed sync message (heads unchanged)");
                        None
                    };

                    let response_msg = doc_guard.generate_sync_message(&mut sync_state);

                    (bytes_to_save, response_msg)
                };

                let save_fut = async {
                    if let Some(bytes_to_save) = bytes_to_save {
                        state.storage.save_doc(&bytes_to_save).await?;
                        debug!("Document was persisted to the database");
                    }
                    Ok::<(), sqlx::Error>(())
                };

                let send_fut = async {
                    if let Some(response_msg) = response_msg {
                        sender.send(response_msg.encode().into()).await?;
                        info!("Sent a sync response");
                    }
                    Ok::<(), axum::Error>(())
                };

                let (save_res, send_res) = tokio::join!(save_fut, send_fut);

                match (save_res, send_res) {
                    (Ok(()), Ok(())) => {}
                    (Err(db_err), Ok(())) => return Err(SocketHandlerError::Database(db_err)),
                    (Ok(()), Err(ws_err)) => return Err(SocketHandlerError::WebSocket(ws_err)),
                    (Err(db), Err(ws)) => {
                        return Err(SocketHandlerError::DatabaseAndWebSocket { db, ws });
                    }
                }
            }
            wake_up_res = wake_up.changed() => {
                wake_up_res?;
                debug!("Woken up by changes in the document");
                let sync_message = {
                    state.doc.read().await.generate_sync_message(&mut sync_state)
                };
                if let Some(data) = sync_message {
                    sender.send(data.encode().into()).await?;
                    info!("Sent a sync message");
                }
            }
        }
    }

    info!("Disconnected");

    Ok(())
}
