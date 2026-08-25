mod constants;
mod helper;

use crate::models::SyncStatus;
use crate::storage::SqliteStorage;
use automerge::sync::{Message as SyncMessage, State as AutomergeServerState, SyncDoc};
use automerge::Automerge;
use constants::{
    EXP_BACKOFF_FACTOR, EXP_BACKOFF_INITIAL_DURATION, EXP_BACKOFF_MAX_DURATION,
    EXP_BACKOFF_MAX_RETRIES, WS_URL,
};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use helper::{notify_todos_updated, update_sync_status};
use rand::RngExt;
use std::cmp::min;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

type WsSender = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsReceiver = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

#[derive(Debug, Error)]
enum SyncLoopError {
    #[error("Websocket transport error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    #[error("Server closed the connection")]
    ServerClosedConnection,

    #[error("Automerge sync decode error: {0}")]
    SyncMessageDecode(#[from] automerge::sync::ReadMessageError),

    #[error("Wake up signal error: {0}")]
    WakeUp(#[from] watch::error::RecvError),

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

pub async fn sync_engine(
    doc: Arc<tokio::sync::RwLock<Automerge>>,
    doc_changed_token: watch::Receiver<()>,
    mut reconnect_token: watch::Receiver<()>,
    app_handle: tauri::AppHandle,
    storage: Arc<SqliteStorage>,
    status: Arc<Mutex<SyncStatus>>,
    cancellation_token: CancellationToken,
) {
    loop {
        let mut retry_count: u32 = 0;

        update_sync_status(&status, &app_handle, SyncStatus::Connecting);

        // FIXME: If the sync_loop crashed because of db issues, it also dropped the websocket.
        //  However, after waiting, if we can acquire a websocket again the backoff duration resets.
        //  This is not ideal. We'd also like to keep persisting changes if we can't connect to the server.
        loop {
            info!("Attempting to connect to sync server at {}", WS_URL);

            if let Ok((ws_stream, _)) = connect_async(WS_URL).await {
                info!("Connected to sync server");
                update_sync_status(&status, &app_handle, SyncStatus::Connected);
                let (mut sender, mut receiver) = ws_stream.split();
                retry_count = 0;
                let loop_result = sync_loop(
                    doc.clone(),
                    app_handle.clone(),
                    &mut sender,
                    &mut receiver,
                    storage.clone(),
                    doc_changed_token.clone(),
                    cancellation_token.clone(),
                )
                .await;
                // We want to close gracefully the websocket. The sync loop could've ended in a websocket error
                //  which is why we make this a best-effort operation rather than a fallible one.
                let _ = sender.close().await;

                match loop_result {
                    Ok(()) => break,
                    Err(err) => {
                        error!("Sync loop ended with error: {}", err);
                        update_sync_status(&status, &app_handle, SyncStatus::Connecting);
                    }
                }
            }
            let wait_duration = min(
                EXP_BACKOFF_INITIAL_DURATION * EXP_BACKOFF_FACTOR.pow(retry_count),
                EXP_BACKOFF_MAX_DURATION,
            );
            let jittered_duration = rand::rng().random_range((wait_duration / 2)..=wait_duration);
            tokio::select! {
                _ = sleep(jittered_duration) => retry_count += 1,
                _ = cancellation_token.cancelled() => break,
            }

            if retry_count > EXP_BACKOFF_MAX_RETRIES {
                warn!(
                "Reached maximum retry attempts ({EXP_BACKOFF_MAX_RETRIES}). Stopping sync engine."
            );
                break;
            }
        }

        update_sync_status(&status, &app_handle, SyncStatus::Disconnected);

        tokio::select! {
            reconnect_res = reconnect_token.changed() => {
                reconnect_res.expect("reconnect channel sender dropped before cancellation token was triggered");
                debug!("Woken up for manual reconnection");
            },
            _ = cancellation_token.cancelled() => {
                info!("Cancellation was requested");
                break
            },
        }
    }
}

async fn sync_loop(
    doc: Arc<tokio::sync::RwLock<Automerge>>,
    app_handle: tauri::AppHandle,
    tx: &mut WsSender,
    rx: &mut WsReceiver,
    storage: Arc<SqliteStorage>,
    mut doc_changed_token: watch::Receiver<()>,
    cancellation: CancellationToken,
) -> Result<(), SyncLoopError> {
    let mut server_state = AutomergeServerState::new();

    if let Some(sync_handshake) = doc.read().await.generate_sync_message(&mut server_state) {
        tx.send(sync_handshake.encode().into()).await?;
    }

    loop {
        tokio::select! {
            msg = rx.next() => handle_incoming_message(msg, &doc, &mut server_state, &storage, &app_handle, tx).await?,

            doc_changed_res = doc_changed_token.changed() => {
                doc_changed_res?;
                if let Some(sync_msg) = doc.read().await.generate_sync_message(&mut server_state) {
                    tx.send(sync_msg.encode().into()).await?;
                }
            }

            _ = cancellation.cancelled() => {
                info!("Cancellation was requested");
                break;
            }
        }
    }

    Ok(())
}

async fn handle_incoming_message(
    msg: Option<Result<Message, tungstenite::Error>>,
    doc: &tokio::sync::RwLock<Automerge>,
    server_state: &mut AutomergeServerState,
    storage: &SqliteStorage,
    app_handle: &tauri::AppHandle,
    tx: &mut WsSender,
) -> Result<(), SyncLoopError> {
    let msg = match msg {
        Some(msg) => msg,
        None => return Err(SyncLoopError::ServerClosedConnection),
    };
    let data = match msg? {
        Message::Binary(data) => data,
        Message::Close(_) => return Err(SyncLoopError::ServerClosedConnection),
        Message::Ping(_) | Message::Pong(_) => return Ok(()),
        other => {
            warn!("Received unexpected WebSocket message: {:?}", other);
            return Ok(());
        }
    };
    let msg = SyncMessage::decode(&data)?;
    debug!("Received a sync message");

    let (bytes_to_save, response_msg) = {
        let mut doc_guard = doc.write().await;
        let heads_pre_merge = doc_guard.get_heads();
        doc_guard.receive_sync_message(server_state, msg)?;
        let doc_guard = doc_guard.downgrade();
        let heads_post_merge = doc_guard.get_heads();

        let bytes_to_save = if heads_pre_merge != heads_post_merge {
            info!(
                "Applied sync changes (heads: {:?} -> {:?}",
                heads_pre_merge, heads_post_merge
            );
            Some(doc_guard.save())
        } else {
            debug!("Processed sync message (heads unchanged)");
            None
        };

        let response_msg = doc_guard.generate_sync_message(server_state);

        (bytes_to_save, response_msg)
    };

    let (save_res, send_res) = tokio::join!(
        async {
            if let Some(bytes_to_save) = bytes_to_save {
                storage.save(&bytes_to_save).await?;
                debug!("Document was persisted to the database");
                notify_todos_updated(app_handle);
            }
            Ok(())
        },
        async {
            if let Some(response_msg) = response_msg {
                tx.send(response_msg.encode().into()).await?;
                info!("Sent a sync response");
            }
            Ok(())
        }
    );

    match (save_res, send_res) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(db_err), Ok(())) => Err(SyncLoopError::Database(db_err)),
        (Ok(()), Err(ws_err)) => Err(SyncLoopError::WebSocket(ws_err)),
        (Err(db), Err(ws)) => Err(SyncLoopError::DatabaseAndWebSocket { db, ws }),
    }
}
