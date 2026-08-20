use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use automerge::AutoCommit;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use sqlx::sqlite::SqlitePoolOptions;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, RwLock};
use tower_http::cors::CorsLayer;
use tracing::{error, info};

mod sync_store;
use sync_store::SqliteSyncStore;

// FIXME: We don't want sequential numbers for the connected clients as this potentially leaks
//  how many clients are connected at a given time.
static CLIENT_COUNTER: AtomicUsize = AtomicUsize::new(1);

#[derive(Clone)]
struct AppState {
    store: SqliteSyncStore,
    // FIXME: AutoCommit requires &mut for generating sync messages because it closes outstanding transactions.
    //  This requires us to acquire a write lock which means we serialize all the updates to the clients.
    doc: Arc<RwLock<AutoCommit>>,
    /// Wake-up signal carrying the id of the client whose change advanced the
    /// authoritative document, so other connections re-run the sync protocol.
    sync_wake_up: watch::Sender<()>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    #[cfg(debug_assertions)]
    let database_url = "sqlite::memory:";

    #[cfg(not(debug_assertions))]
    let database_url = "sqlite://sync_store.db?mode=rwc";

    info!("Connecting to SQLite database at: {}", database_url);

    let pool = SqlitePoolOptions::new()
        .connect(database_url)
        .await
        .expect("Failed to connect to SQLite with sqlx");

    let sync_store = SqliteSyncStore::new(pool).await;

    // Load the authoritative automerge document from storage (or start fresh).
    let doc = match sync_store.load_doc().await {
        Ok(Some(bytes)) => {
            AutoCommit::load(&bytes).expect("Failed to load persisted automerge document")
        }
        Ok(None) => AutoCommit::new(),
        Err(e) => panic!("Failed to load automerge document from storage: {}", e),
    };

    let (tx, _rx) = watch::channel::<()>(());
    let state = AppState {
        store: sync_store,
        doc: Arc::new(RwLock::new(doc)),
        sync_wake_up: tx,
    };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("Failed to bind server port 3000");

    info!("Automerge sync server listening on ws://0.0.0.0:3000/ws");
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> axum::response::Response {
    ws.on_upgrade(async |socket| {
        if let Err(e) = handle_socket(socket, state).await {
            tracing::debug!("Connection ended with error: {}", e);
        }
    })
}

async fn handle_socket(socket: WebSocket, state: AppState) -> Result<(), Box<dyn Error>> {
    let my_client_id = CLIENT_COUNTER.fetch_add(1, Ordering::SeqCst);
    info!("Client {} connected", my_client_id);

    let (mut sender, mut receiver) = socket.split();
    let mut wake_up = state.sync_wake_up.subscribe();

    let mut sync_state = SyncState::new();
    loop {
        tokio::select! {
            Some(Ok(msg)) = receiver.next() => {
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
                    state.sync_wake_up.send(())?;
                }

                if let Some(response_msg) = response_msg {
                    sender.send(response_msg.encode().into()).await?;
                }
            }
            Ok(_) = wake_up.changed() => {
                if let Some(data) = state.doc.write().await.sync().generate_sync_message(&mut sync_state) {
                    sender.send(data.encode().into()).await?;
                }
            }
        }
    }

    info!("Client {} disconnected", my_client_id);

    Ok(())
}
