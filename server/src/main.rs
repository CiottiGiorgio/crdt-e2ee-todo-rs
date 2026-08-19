use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use automerge::AutoCommit;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use shared::{ClientMessage, ServerMessage};
use sqlx::sqlite::SqlitePoolOptions;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
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
    doc: Arc<Mutex<AutoCommit>>,
    /// Wake-up signal carrying the id of the client whose change advanced the
    /// authoritative document, so other connections re-run the sync protocol.
    tx: broadcast::Sender<usize>,
}

/// Drains all pending outgoing Automerge sync messages for `sync_state` against
/// the authoritative document, returning each encoded message. The locks are
/// released before the caller performs any async send.
fn generate_pending(doc: &Mutex<AutoCommit>, sync_state: &Mutex<SyncState>) -> Vec<Vec<u8>> {
    let mut doc = doc.lock().unwrap();
    let mut state = sync_state.lock().unwrap();
    let mut out = Vec::new();
    while let Some(msg) = doc.sync().generate_sync_message(&mut state) {
        out.push(msg.encode());
    }
    out
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

    let (tx, _rx) = broadcast::channel::<usize>(100);
    let state = AppState {
        store: sync_store,
        doc: Arc::new(Mutex::new(doc)),
        tx,
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
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let my_client_id = CLIENT_COUNTER.fetch_add(1, Ordering::SeqCst);
    info!("Client {} connected", my_client_id);

    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();

    // Per-connection Automerge sync state for THIS peer. Shared between the
    // receive loop (draining follow-up messages) and the send task (reacting to
    // wake-up notifications). In-memory only; renegotiated on reconnect.
    let sync_state = Arc::new(Mutex::new(SyncState::new()));

    // A channel for sending sync messages directly to THIS client's socket.
    let (direct_tx, mut direct_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

    // Helper: enqueue all pending outgoing sync messages for this peer.
    let enqueue_pending = |direct_tx: &tokio::sync::mpsc::UnboundedSender<Vec<u8>>| {
        for data in generate_pending(&state.doc, &sync_state) {
            let _ = direct_tx.send(data);
        }
    };

    // Spawn a task to forward encoded sync messages to the WebSocket. It reacts
    // to wake-up notifications from other clients by generating this peer's next
    // sync message, and forwards directly-queued messages produced by the
    // receive loop.
    let send_task = {
        let doc = state.doc.clone();
        let sync_state = sync_state.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // A wake-up from another client whose change advanced the doc.
                    Ok(sender_id) = rx.recv() => {
                        if sender_id != my_client_id {
                            for data in generate_pending(&doc, &sync_state) {
                                if serialize_and_send(&mut sender, data).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    // A message produced directly by this connection's receive loop.
                    Some(data) = direct_rx.recv() => {
                        if serialize_and_send(&mut sender, data).await.is_err() {
                            return;
                        }
                    }
                    else => break,
                }
            }
        })
    };

    // Drive the handshake from the server side too: enqueue our initial message.
    enqueue_pending(&direct_tx);

    // Handle incoming messages from this client.
    while let Some(Ok(msg)) = receiver.next().await {
        if let Message::Text(text) = msg {
            let client_msg = match serde_json::from_str::<ClientMessage>(&text) {
                Ok(msg) => msg,
                Err(_) => {
                    error!("Failed to parse ClientMessage from client {}", my_client_id);
                    continue;
                }
            };

            match client_msg {
                ClientMessage::Sync { data } => {
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

                    // Apply the incoming sync message to the authoritative
                    // document, then capture the bytes to persist.
                    let saved_bytes = {
                        let mut doc = state.doc.lock().unwrap();
                        let mut ss = sync_state.lock().unwrap();
                        let res = doc.sync().receive_sync_message(&mut ss, msg);
                        if let Err(e) = res {
                            error!(
                                "Failed to receive sync message from client {}: {}",
                                my_client_id, e
                            );
                            continue;
                        }
                        doc.save()
                    };

                    if let Err(e) = state.store.save_doc(&saved_bytes).await {
                        error!("Failed to persist automerge document: {}", e);
                    }

                    info!("Applied sync message from client {}", my_client_id);

                    // Wake up every other client so they sync the new state.
                    let _ = state.tx.send(my_client_id);

                    // Drain any follow-up messages this peer needs to send.
                    enqueue_pending(&direct_tx);
                }
            }
        }
    }

    info!("Client {} disconnected", my_client_id);
    send_task.abort();
}

/// Sends an encoded sync message to a client's WebSocket as a JSON
/// [`ServerMessage::Sync`] envelope.
async fn serialize_and_send(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    data: Vec<u8>,
) -> Result<(), ()> {
    let msg = ServerMessage::Sync { data };
    let json = serde_json::to_string(&msg).map_err(|_| ())?;
    sender.send(Message::Text(json.into())).await.map_err(|_| ())
}
