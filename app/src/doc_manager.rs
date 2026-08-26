use automerge::sync::{Message as SyncMessage, State as AutomergeServerState, SyncDoc};
use automerge::Automerge;
use autosurgeon::{hydrate, reconcile};
use tauri::Emitter;
use thiserror::Error;
use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info};

use crate::models::{TodoDoc, TodoItem};
use crate::storage::SqliteStorage;

#[derive(Debug, Error)]
pub enum DocError {
    #[error("Reconcile error: {0}")]
    Reconcile(#[from] autosurgeon::ReconcileError),

    #[error("Automerge error: {0}")]
    Automerge(#[from] automerge::AutomergeError),
}

pub struct DocManager {
    inner: RwLock<DocInner>,
    storage: SqliteStorage,
    doc_changed: watch::Sender<()>,
    app_handle: tauri::AppHandle,
}

struct DocInner {
    doc: Automerge,
    todos: TodoDoc,
}

impl DocManager {
    pub fn new(
        doc: Automerge,
        todos: TodoDoc,
        storage: SqliteStorage,
        app_handle: tauri::AppHandle,
    ) -> Self {
        let (doc_changed, _) = watch::channel(());
        Self {
            inner: RwLock::new(DocInner { doc, todos }),
            storage,
            doc_changed,
            app_handle,
        }
    }

    /// Returns a receiver that fires whenever the document changes locally.
    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.doc_changed.subscribe()
    }

    /// Apply a local mutation to the todo list.
    /// Reconciles into the Automerge doc, persists (fire-and-forget),
    /// and signals the sync engine.
    pub async fn apply<F, R>(&self, f: F) -> Result<R, DocError>
    where
        F: FnOnce(&mut Vec<TodoItem>) -> R,
    {
        let (result, bytes) = {
            let mut inner = self.inner.write().await;
            let DocInner { doc, todos } = &mut *inner;
            let result = f(&mut todos.todos);

            let mut tx = doc.transaction();
            reconcile(&mut tx, &*todos)?;
            tx.commit();

            (result, doc.save())
        };

        self.persist(&bytes).await;
        let _ = self.doc_changed.send(());
        Ok(result)
    }

    /// Merge an incoming sync message from the server.
    /// Returns whether the document heads changed (new data arrived).
    /// Persists and emits UI events on change (fire-and-forget).
    pub async fn receive_sync(
        &self,
        server_state: &mut AutomergeServerState,
        msg: SyncMessage,
    ) -> Result<bool, DocError> {
        let (changed, bytes_to_save) = {
            let mut inner = self.inner.write().await;
            let heads_before = inner.doc.get_heads();
            inner.doc.receive_sync_message(server_state, msg)?;
            let heads_after = inner.doc.get_heads();

            let changed = heads_before != heads_after;
            let bytes_to_save = if changed {
                info!(
                    "Applied sync changes (heads: {:?} -> {:?}",
                    heads_before, heads_after
                );
                match hydrate::<_, TodoDoc>(&inner.doc) {
                    Ok(hydrated) => inner.todos = hydrated,
                    Err(e) => error!("Failed to hydrate after sync: {}", e),
                }
                Some(inner.doc.save())
            } else {
                debug!("Processed sync message (heads unchanged)");
                None
            };

            (changed, bytes_to_save)
        };

        if let Some(bytes) = bytes_to_save {
            self.persist(&bytes).await;
            if let Err(e) = self.app_handle.emit("todos-updated", ()) {
                error!("Failed to emit todos-updated event: {}", e);
            }
        }
        Ok(changed)
    }

    /// Generate the next sync message for the server, if any.
    pub async fn generate_sync_message(
        &self,
        server_state: &mut AutomergeServerState,
    ) -> Option<SyncMessage> {
        self.inner
            .read()
            .await
            .doc
            .generate_sync_message(server_state)
    }

    /// Read-only snapshot of the current todos.
    pub async fn todos(&self) -> Vec<TodoItem> {
        self.inner.read().await.todos.todos.clone()
    }

    /// Serialize the doc and save to SQLite. On failure, log and
    /// emit a "db-error" toast event to the frontend.
    async fn persist(&self, bytes: &[u8]) {
        match self.storage.save(bytes).await {
            Ok(()) => {
                debug!("Document persisted to database");
            }
            Err(e) => {
                error!("Failed to persist document: {}", e);
                if let Err(emit_err) = self.app_handle.emit("db-error", e.to_string()) {
                    error!("Failed to emit db-error event: {}", emit_err);
                }
            }
        }
    }
}
