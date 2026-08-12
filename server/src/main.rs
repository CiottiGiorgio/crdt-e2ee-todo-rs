use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use shared::{ClientMessage, EncryptedPayload, ServerMessage};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

static CLIENT_COUNTER: AtomicUsize = AtomicUsize::new(1);

#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<SyncStore>>,
    tx: broadcast::Sender<(usize, ServerMessage)>,
}

struct SyncStore {
    snapshot: Option<(u64, EncryptedPayload)>,
    deltas: Vec<(u64, EncryptedPayload)>,
    next_seq_id: u64,
}

impl SyncStore {
    fn new() -> Self {
        Self {
            snapshot: None,
            deltas: Vec::new(),
            next_seq_id: 1,
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (tx, _rx) = broadcast::channel::<(usize, ServerMessage)>(100);
    let state = AppState {
        store: Arc::new(Mutex::new(SyncStore::new())),
        tx,
    };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("Failed to bind server port 3000");

    info!("Dumb Relay Server listening on ws://0.0.0.0:3000/ws");
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
                    ClientMessage::RequestSync => {
                        let (snapshot, deltas_to_send) = {
                            let store = state.store.lock().unwrap();
                            let snap = store.snapshot.clone();
                            let min_seq = snap.as_ref().map(|(seq, _)| *seq).unwrap_or(0);
                            let deltas: Vec<(u64, EncryptedPayload)> = store
                                .deltas
                                .iter()
                                .filter(|(seq, _)| *seq > min_seq)
                                .cloned()
                                .collect();
                            (snap, deltas)
                        };

                        // Send historical snapshot and deltas DIRECTLY to the requesting client
                        if let Some((seq_id, payload)) = snapshot {
                            let _ = direct_tx.send(ServerMessage::Snapshot { seq_id, payload });
                        }
                        for (seq_id, payload) in deltas_to_send {
                            let _ = direct_tx.send(ServerMessage::Delta { seq_id, payload });
                        }
                    }
                    ClientMessage::Delta { payload, .. } => {
                        let seq_id = {
                            let mut store = state.store.lock().unwrap();
                            let seq = store.next_seq_id;
                            store.next_seq_id += 1;
                            store.deltas.push((seq, payload.clone()));
                            seq
                        };

                        info!(
                            "Received Delta from Client {} -> Assigned SeqId: {}",
                            my_client_id, seq_id
                        );
                        // Broadcast to everyone else
                        let _ = state
                            .tx
                            .send((my_client_id, ServerMessage::Delta { seq_id, payload }));
                    }
                    ClientMessage::Snapshot {
                        covers_seq_id,
                        payload,
                    } => {
                        let updated = {
                            let mut store = state.store.lock().unwrap();
                            let current_snap_seq =
                                store.snapshot.as_ref().map(|(seq, _)| *seq).unwrap_or(0);

                            if covers_seq_id >= current_snap_seq {
                                store.snapshot = Some((covers_seq_id, payload.clone()));
                                // Prune deltas covered by this snapshot
                                store.deltas.retain(|(seq, _)| *seq > covers_seq_id);
                                true
                            } else {
                                false
                            }
                        };

                        if updated {
                            info!(
                                "Accepted Snapshot from Client {} covering up to SeqId: {}",
                                my_client_id, covers_seq_id
                            );
                            let _ = state.tx.send((
                                my_client_id,
                                ServerMessage::Snapshot {
                                    seq_id: covers_seq_id,
                                    payload,
                                },
                            ));
                        } else {
                            info!(
                                "Rejected stale Snapshot from Client {} (covers {}, current is >=)",
                                my_client_id, covers_seq_id
                            );
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
