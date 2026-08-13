use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use shared::{ClientMessage, EncryptedPayload, ServerMessage};
use sqlx::{sqlite::SqlitePoolOptions, Row, SqlitePool};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{broadcast, Mutex};
use tower_http::cors::CorsLayer;
use tracing::{error, info};

// FIXME: We don't want sequential numbers for the connected clients as this potentially leaks
//  how many clients are connected at a given time.
static CLIENT_COUNTER: AtomicUsize = AtomicUsize::new(1);

#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<SyncStore>>,
    tx: broadcast::Sender<(usize, ServerMessage)>,
}

struct SyncStore {
    pool: SqlitePool,
}

impl SyncStore {
    async fn new(pool: SqlitePool) -> Self {
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run database migrations");

        Self { pool }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let pool = SqlitePoolOptions::new()
        .connect("sqlite://sync_store.db?mode=rwc")
        .await
        .expect("Failed to connect to SQLite with sqlx");

    let sync_store = SyncStore::new(pool).await;

    let (tx, _rx) = broadcast::channel::<(usize, ServerMessage)>(100);
    let state = AppState {
        store: Arc::new(Mutex::new(sync_store)),
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

    // Send Welcome message immediately with current highest seq_id straight from SQLite
    let highest_seq_id = {
        let store = state.store.lock().await;
        sqlx::query_scalar::<_, i64>("SELECT highest_seq_id FROM server_state")
            .fetch_one(&store.pool)
            .await
            .map(|v| v as u64)
            .unwrap_or(0)
    };
    let _ = direct_tx.send(ServerMessage::Welcome { highest_seq_id });

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
                        let store = state.store.lock().await;

                        // Query current snapshot from SQLite
                        let snapshot_row =
                            sqlx::query("SELECT seq_id, ciphertext, nonce FROM snapshot WHERE id = 1")
                                .fetch_optional(&store.pool)
                                .await
                                .unwrap_or(None);

                        let snap_seq = snapshot_row
                            .as_ref()
                            .map(|r| r.get::<i64, _>(0) as u64)
                            .unwrap_or(0);

                        if from_seq_id < snap_seq {
                            // Send snapshot first
                            if let Some(row) = snapshot_row {
                                let seq_id: i64 = row.get(0);
                                let ciphertext: Vec<u8> = row.get(1);
                                let nonce_vec: Vec<u8> = row.get(2);
                                if let Ok(nonce) = nonce_vec.try_into() {
                                    let payload = EncryptedPayload { ciphertext, nonce };
                                    let _ = direct_tx.send(ServerMessage::Snapshot {
                                        seq_id: seq_id as u64,
                                        payload,
                                    });
                                }
                            }

                            // Send deltas after snap_seq
                            let delta_rows = sqlx::query(
                                "SELECT seq_id, ciphertext, nonce FROM deltas WHERE seq_id > ? ORDER BY seq_id ASC",
                            )
                            .bind(snap_seq as i64)
                            .fetch_all(&store.pool)
                            .await
                            .unwrap_or_default();

                            for row in delta_rows {
                                let seq_id: i64 = row.get(0);
                                let ciphertext: Vec<u8> = row.get(1);
                                let nonce_vec: Vec<u8> = row.get(2);
                                if let Ok(nonce) = nonce_vec.try_into() {
                                    let payload = EncryptedPayload { ciphertext, nonce };
                                    let _ = direct_tx.send(ServerMessage::Delta {
                                        seq_id: seq_id as u64,
                                        payload,
                                    });
                                }
                            }
                        } else {
                            // Send deltas after from_seq_id
                            let delta_rows = sqlx::query(
                                "SELECT seq_id, ciphertext, nonce FROM deltas WHERE seq_id > ? ORDER BY seq_id ASC",
                            )
                            .bind(from_seq_id as i64)
                            .fetch_all(&store.pool)
                            .await
                            .unwrap_or_default();

                            for row in delta_rows {
                                let seq_id: i64 = row.get(0);
                                let ciphertext: Vec<u8> = row.get(1);
                                let nonce_vec: Vec<u8> = row.get(2);
                                if let Ok(nonce) = nonce_vec.try_into() {
                                    let payload = EncryptedPayload { ciphertext, nonce };
                                    let _ = direct_tx.send(ServerMessage::Delta {
                                        seq_id: seq_id as u64,
                                        payload,
                                    });
                                }
                            }
                        }
                    }
                    ClientMessage::Delta { payload, .. } => {
                        let seq_id = {
                            let store = state.store.lock().await;

                            // Query highest_seq_id from SQLite, increment by 1
                            let highest_seq: u64 =
                                sqlx::query_scalar::<_, i64>("SELECT highest_seq_id FROM server_state")
                                    .fetch_one(&store.pool)
                                    .await
                                    .map(|v| v as u64)
                                    .unwrap_or(0);
                            let seq = highest_seq + 1;

                            // Save to SQLite
                            let mut tx = store.pool.begin().await.unwrap();
                            sqlx::query("INSERT INTO deltas (seq_id, ciphertext, nonce) VALUES (?, ?, ?)")
                                .bind(seq as i64)
                                .bind(&payload.ciphertext)
                                .bind(&payload.nonce[..])
                                .execute(&mut *tx)
                                .await
                                .unwrap();

                            tx.commit().await.unwrap();
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
                            let store = state.store.lock().await;
                            let current_snap_seq: u64 =
                                sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq_id), 0) FROM snapshot")
                                    .fetch_one(&store.pool)
                                    .await
                                    .map(|v| v as u64)
                                    .unwrap_or(0);

                            if covers_seq_id >= current_snap_seq {
                                // Save to SQLite
                                let mut tx = store.pool.begin().await.unwrap();
                                sqlx::query("INSERT OR REPLACE INTO snapshot (id, seq_id, ciphertext, nonce) VALUES (1, ?, ?, ?)")
                                    .bind(covers_seq_id as i64)
                                    .bind(&payload.ciphertext)
                                    .bind(&payload.nonce[..])
                                    .execute(&mut *tx)
                                    .await
                                    .unwrap();

                                sqlx::query("DELETE FROM deltas WHERE seq_id <= ?")
                                    .bind(covers_seq_id as i64)
                                    .execute(&mut *tx)
                                    .await
                                    .unwrap();

                                tx.commit().await.unwrap();
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
