mod constants;
mod helper;

use crate::doc_manager::{DocError, DocManager};
use crate::models::SyncStatus;
use automerge::sync::{Message as SyncMessage, State as AutomergeServerState};
use constants::{
    EXP_BACKOFF_FACTOR, EXP_BACKOFF_INITIAL_DURATION, EXP_BACKOFF_MAX_DURATION,
    EXP_BACKOFF_MAX_RETRIES, WS_URL,
};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use helper::update_sync_status;
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

    #[error("Document error: {0}")]
    Doc(#[from] DocError),
}

pub async fn sync_engine(
    doc_manager: Arc<DocManager>,
    mut reconnect_token: watch::Receiver<()>,
    app_handle: tauri::AppHandle,
    status: Arc<Mutex<SyncStatus>>,
    cancellation_token: CancellationToken,
) {
    loop {
        let mut retry_count: u32 = 0;

        update_sync_status(&status, &app_handle, SyncStatus::Connecting);

        loop {
            info!("Attempting to connect to sync server at {}", WS_URL);

            if let Ok((ws_stream, _)) = connect_async(WS_URL).await {
                info!("Connected to sync server");
                update_sync_status(&status, &app_handle, SyncStatus::Connected);
                let (mut sender, mut receiver) = ws_stream.split();
                retry_count = 0;
                let loop_result = sync_loop(
                    &doc_manager,
                    &mut sender,
                    &mut receiver,
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
            if retry_count >= EXP_BACKOFF_MAX_RETRIES {
                warn!(
                    "Reached maximum retry attempts ({EXP_BACKOFF_MAX_RETRIES}). Stopping sync engine."
                );
                break;
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
    doc_manager: &DocManager,
    tx: &mut WsSender,
    rx: &mut WsReceiver,
    cancellation: CancellationToken,
) -> Result<(), SyncLoopError> {
    let mut server_state = AutomergeServerState::new();
    let mut doc_changed_token = doc_manager.subscribe();

    if let Some(sync_handshake) = doc_manager.generate_sync_message(&mut server_state).await {
        tx.send(sync_handshake.encode().into()).await?;
    }

    loop {
        tokio::select! {
            msg = rx.next() => handle_incoming_message(msg, doc_manager, &mut server_state, tx).await?,

            doc_changed_res = doc_changed_token.changed() => {
                doc_changed_res?;
                if let Some(sync_msg) = doc_manager.generate_sync_message(&mut server_state).await {
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
    doc_manager: &DocManager,
    server_state: &mut AutomergeServerState,
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
    let sync_msg = SyncMessage::decode(&data)?;
    debug!("Received a sync message");

    doc_manager.receive_sync(server_state, sync_msg).await?;

    if let Some(response) = doc_manager.generate_sync_message(server_state).await {
        tx.send(response.encode().into()).await?;
        info!("Sent a sync response");
    }

    Ok(())
}
