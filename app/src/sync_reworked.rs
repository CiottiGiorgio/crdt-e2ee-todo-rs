use crate::storage::SqliteStorage;
use automerge::sync::{Message as SyncMessage, State as AutomergeServerState, SyncDoc};
use automerge::AutoCommit;
use futures_util::{SinkExt, StreamExt};
use std::cmp::min;
use std::sync::Arc;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

// FIXME: Pull the constant when we finish reworking this module.
const WS_URL: &str = "ws://127.0.0.1:3000/ws";

const EXP_BACKOFF_INITIAL_DURATION: Duration = Duration::from_secs(1);
const EXP_BACKOFF_FACTOR: u32 = 2;
// TODO: Introduce jitter to avoid a thundering herd.
const EXP_BACKOFF_MAX_DURATION: Duration = Duration::from_secs(45);

#[derive(Debug, Error)]
pub enum SyncEngineError {}

#[derive(Debug, Error)]
enum SyncLoopError {
    #[error("Websocket transport error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    #[error("Server closed the connection")]
    ServerClosedConnection,

    #[error("Automerge sync decode error: {0}")]
    SyncMessageDecode(#[from] automerge::sync::ReadMessageError),

    #[error("Automerge sync error: {0}")]
    Automerge(#[from] automerge::AutomergeError),

    #[error("Database persistence error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Database persistence ({db}) and WebSocket transport ({ws}) both failed")]
    DatabaseAndWebSocket {
        db: sqlx::Error,
        ws: tungstenite::Error,
    },
}

// TODO: We are missing:
//  - A way for the sync engine to be woken up when the document changes from user interaction.
//  - A way for the sync engine to fire an event to the UI when it syncs with the server.
//  - A decision on whether a SQLite error is fatal.
//  - A way to signal the sync engine current status to the UI.
pub async fn sync_engine(
    doc: Arc<tokio::sync::RwLock<AutoCommit>>,
    storage: Arc<SqliteStorage>,
    cancellation_token: CancellationToken,
) -> Result<(), SyncEngineError> {
    let mut wait_duration = EXP_BACKOFF_INITIAL_DURATION;

    loop {
        info!("Attempting to connect to sync server at {}", WS_URL);
        if let Ok((ws_stream, _)) = connect_async(WS_URL).await {
            info!("Connected to sync server");
            match sync_loop(
                doc.clone(),
                ws_stream,
                storage.clone(),
                cancellation_token.clone(),
            )
            .await
            {
                Ok(_) => return Ok(()),

                // TODO: Propagate errors not related to connection issues.
                //  Fall through on errors related to connection issues.
                // Should these log statements be error or info?
                Err(SyncLoopError::WebSocket(e)) => {
                    debug!("{}", e);
                }
                Err(SyncLoopError::ServerClosedConnection) => {
                    debug!("Server unexpectedly closed the connection");
                }
                Err(SyncLoopError::SyncMessageDecode(e)) => {
                    debug!("Could not decode sync message");
                }
                Err(SyncLoopError::Automerge(_))
                | Err(SyncLoopError::Database(_))
                | Err(SyncLoopError::DatabaseAndWebSocket { .. }) => break,
            }
        }
        sleep(wait_duration).await;
        wait_duration = min(wait_duration * EXP_BACKOFF_FACTOR, EXP_BACKOFF_MAX_DURATION);
    }
}

async fn sync_loop(
    doc: Arc<tokio::sync::RwLock<AutoCommit>>,
    connection: WebSocketStream<MaybeTlsStream<TcpStream>>,
    storage: Arc<SqliteStorage>,
    cancellation: CancellationToken,
) -> Result<(), SyncLoopError> {
    let (mut tx, mut rx) = connection.split();
    let mut server_state = AutomergeServerState::new();

    if let Some(sync_handshake) = doc
        .write()
        .await
        .sync()
        .generate_sync_message(&mut server_state)
    {
        tx.send(sync_handshake.encode().into()).await?;
    }

    loop {
        tokio::select! {
            Some(msg) = rx.next() => {
                let data = match msg? {
                    Message::Binary(data) => data,
                    Message::Close(_) => return Err(SyncLoopError::ServerClosedConnection),
                    Message::Ping(_) | Message::Pong(_) => continue,
                    other => {
                        warn!("Received unexpected WebSocket message: {:?}", other);
                        continue;
                    }
                };
                let msg = SyncMessage::decode(&data)?;
                debug!("Received a sync message");

                let (bytes_to_save, response_msg) = {
                    let mut doc_guard = doc.write().await;
                    let heads_pre_merge = doc_guard.get_heads();
                    doc_guard.sync().receive_sync_message(&mut server_state, msg)?;
                    let heads_post_merge = doc_guard.get_heads();

                    let bytes_to_save = if heads_pre_merge != heads_post_merge {
                        info!("Applied sync changes (heads: {:?} -> {:?}", heads_pre_merge, heads_post_merge);
                        Some(doc_guard.save())
                    } else {
                        debug!("Processed sync message (heads unchanged)");
                        None
                    };

                    let response_msg = doc_guard.sync().generate_sync_message(&mut server_state);

                    (bytes_to_save, response_msg)
                };

                let save_fut = async {
                    if let Some(bytes_to_save) = bytes_to_save {
                        storage.save(&bytes_to_save).await?;
                        debug!("Document was persisted to the database");
                    }
                    Ok::<(), sqlx::Error>(())
                };

                let send_fut = async {
                    if let Some(response_msg) = response_msg {
                        tx.send(response_msg.encode().into()).await?;
                        info!("Sent a sync response");
                    }
                    Ok::<(), tungstenite::Error>(())
                };

                let (save_res, send_res) = tokio::join!(save_fut, send_fut);

                match (save_res, send_res) {
                    (Ok(()), Ok(())) => {}
                    (Err(db_err), Ok(())) => return Err(SyncLoopError::Database(db_err)),
                    (Ok(()), Err(ws_err)) => return Err(SyncLoopError::WebSocket(ws_err)),
                    (Err(db), Err(ws)) => {
                        return Err(SyncLoopError::DatabaseAndWebSocket { db, ws });
                    }
                }
            }

            _ = cancellation.cancelled() => {
                info!("Cancellation was requested");
                break;
            },
        }
    }

    Ok(())
}
