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
    tx: broadcast::Sender<(usize, ServerMessage)>,
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

    let (tx, _rx) = broadcast::channel::<(usize, ServerMessage)>(100);
    let state = AppState {
        store: sync_store,
        tx,
    };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("Failed to bind server port 3000");

    info!("Dumb Relay Server (powered by SQLx Migrations) listening on ws://0.0.0.0:3000/ws");
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

    // A channel for sending messages directly to THIS client only (e.g. initial sync)
    let (direct_tx, mut direct_rx) = tokio::sync::mpsc::unbounded_channel::<ServerMessage>();

    // Spawn task to forward messages to the WebSocket
    let send_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                // Handle broadcast messages
                Ok((sender_id, msg)) = rx.recv() => {
                    // Do not echo messages back to the sender
                    if sender_id != my_client_id {
                        if let Ok(json) = serde_json::to_string(&msg) {
                            if sender.send(Message::Text(json.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                // Handle direct messages
                Some(msg) = direct_rx.recv() => {
                    if let Ok(json) = serde_json::to_string(&msg) {
                        if sender.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                }
                else => break,
            }
        }
    });

    // Handle incoming messages from this client
    while let Some(Ok(msg)) = receiver.next().await {
        if let Message::Text(text) = msg {
            if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) {
                match client_msg {
                    ClientMessage::RequestSync { from_seq_id } => {
                        if let Ok(deltas) = state.store.get_deltas_after(from_seq_id).await {
                            if !deltas.is_empty() {
                                let _ = direct_tx.send(ServerMessage::DeltaBatch { deltas });
                            }
                        }
                    }
                    ClientMessage::Delta { payload } => {
                        if let Ok(seq_id) = state.store.save_delta(&payload).await {
                            info!(
                                "Received Delta from Client {} -> Assigned SeqId: {}",
                                my_client_id, seq_id
                            );

                            // Broadcast batch to everyone else
                            let _ = state.tx.send((
                                my_client_id,
                                ServerMessage::DeltaBatch {
                                    deltas: vec![(seq_id, payload)],
                                },
                            ));
                        } else {
                            error!("Failed to save Delta from client {}", my_client_id);
                        }
                    }
                }
            } else {
                error!("Failed to parse ClientMessage from client {}", my_client_id);
            }
        }
    }

    info!("Client {} disconnected", my_client_id);
    send_task.abort();
}
