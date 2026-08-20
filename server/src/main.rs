use automerge::Automerge;
use axum::{extract::ws::WebSocketUpgrade, extract::State, routing::get, Router};
use sqlx::sqlite::SqlitePoolOptions;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, RwLock};
use tower_http::cors::CorsLayer;
use tracing::{info, Instrument, error};

mod sync_store;
mod websocket;

use sync_store::SqliteSyncStore;

use websocket::handle_socket;

// FIXME: We don't want sequential numbers for the connected clients as this potentially leaks
//  how many clients are connected at a given time.
static CLIENT_COUNTER: AtomicUsize = AtomicUsize::new(1);

#[derive(Clone)]
struct AppState {
    store: SqliteSyncStore,
    doc: Arc<RwLock<Automerge>>,
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
            Automerge::load(&bytes).expect("Failed to load persisted automerge document")
        }
        Ok(None) => Automerge::new(),
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
    let client_id = CLIENT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let span = tracing::info_span!("ws_session", client_id);

    ws.on_upgrade(move |socket| {
        async move {
            if let Err(e) = handle_socket(socket, state).await {
                error!("Connection ended with error: {}", e);
            }
        }
        .instrument(span)
    })
}
