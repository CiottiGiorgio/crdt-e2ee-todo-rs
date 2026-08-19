use sqlx::SqlitePool;

/// SQLite-backed persistence for the server's authoritative automerge document.
#[derive(Clone)]
pub struct SqliteSyncStore {
    pool: SqlitePool,
}

impl SqliteSyncStore {
    pub async fn new(pool: SqlitePool) -> Self {
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run database migrations");

        Self { pool }
    }

    /// Loads the persisted automerge document bytes, if any exist.
    pub async fn load_doc(&self) -> Result<Option<Vec<u8>>, sqlx::Error> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT data FROM automerge_doc WHERE id = 1")
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.0))
    }

    /// Persists the authoritative automerge document bytes.
    pub async fn save_doc(&self, data: &[u8]) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT OR REPLACE INTO automerge_doc (id, data) VALUES (1, ?)")
            .bind(data)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::sync::{State as SyncState, SyncDoc};
    use automerge::transaction::Transactable;
    use automerge::{AutoCommit, ReadDoc, ROOT};
    use sqlx::sqlite::SqlitePoolOptions;

    async fn test_store() -> SqliteSyncStore {
        let pool = SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap();
        SqliteSyncStore::new(pool).await
    }

    #[tokio::test]
    async fn test_save_and_load_doc() {
        let store = test_store().await;
        assert_eq!(store.load_doc().await.unwrap(), None);

        let mut doc = AutoCommit::new();
        doc.put(ROOT, "key", "value").unwrap();
        let bytes = doc.save();

        store.save_doc(&bytes).await.unwrap();
        let loaded = store.load_doc().await.unwrap().unwrap();

        // The stored bytes reload into an equivalent document.
        let reloaded = AutoCommit::load(&loaded).unwrap();
        assert!(reloaded.get(ROOT, "key").unwrap().is_some());
    }

    #[tokio::test]
    async fn test_receive_sync_message_then_persist_roundtrip() {
        let store = test_store().await;

        // A source peer holds a change we want the server to receive.
        let mut source = AutoCommit::new();
        source.put(ROOT, "key", "value").unwrap();

        // The server-side document starts empty. Drive the Automerge sync
        // protocol until neither peer has anything left to send.
        let mut server_doc = AutoCommit::new();
        let mut source_state = SyncState::new();
        let mut server_state = SyncState::new();
        loop {
            let from_source = source.sync().generate_sync_message(&mut source_state);
            if let Some(msg) = from_source.clone() {
                server_doc
                    .sync()
                    .receive_sync_message(&mut server_state, msg)
                    .unwrap();
            }

            let from_server = server_doc.sync().generate_sync_message(&mut server_state);
            if let Some(msg) = from_server.clone() {
                source
                    .sync()
                    .receive_sync_message(&mut source_state, msg)
                    .unwrap();
            }

            if from_source.is_none() && from_server.is_none() {
                break;
            }
        }

        // Persist the document the server converged to, then reload it.
        let bytes = server_doc.save();
        store.save_doc(&bytes).await.unwrap();
        let loaded = store.load_doc().await.unwrap().unwrap();

        // The change the server received via the sync protocol survives a
        // persist/reload round-trip.
        let reloaded = AutoCommit::load(&loaded).unwrap();
        let (value, _) = reloaded.get(ROOT, "key").unwrap().unwrap();
        assert_eq!(value.into_string().ok().as_deref(), Some("value"));
    }
}
