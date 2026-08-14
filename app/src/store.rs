use sqlx::SqlitePool;
use std::collections::BTreeSet;

/// SQLite-backed store for binary document data (e.g., Automerge documents)
/// and synchronization metadata.
pub struct SqliteBackingStore {
    pool: SqlitePool,
}

impl SqliteBackingStore {
    pub async fn new(pool: SqlitePool) -> Self {
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run database migrations");

        Self { pool }
    }

    /// Loads document bytes from the store.
    /// Returns `Ok(Some(bytes))` if data exists, `Ok(None)` if no saved data exists, or `Err` on failure.
    pub async fn load(&self) -> Result<Option<Vec<u8>>, String> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT data FROM automerge_doc WHERE id = 1")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        Ok(row.map(|r| r.0))
    }

    /// Saves document bytes to the store.
    pub async fn save(&self, data: &[u8]) -> Result<(), String> {
        sqlx::query("INSERT OR REPLACE INTO automerge_doc (id, data) VALUES (1, ?)")
            .bind(data)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Gets the current synchronization state.
    pub async fn get_sync_state(&self) -> Result<(u64, BTreeSet<u64>), String> {
        let row: Option<(i64, String)> =
            sqlx::query_as("SELECT highest_observed, missing_ids FROM sync_state WHERE id = 1")
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| e.to_string())?;

        if let Some((highest, missing_str)) = row {
            let missing_ids = missing_str
                .split(',')
                .filter_map(|s| s.parse::<u64>().ok())
                .collect();
            Ok((highest as u64, missing_ids))
        } else {
            Ok((0, BTreeSet::new()))
        }
    }

    /// Saves the current synchronization state.
    pub async fn save_sync_state(
        &self,
        highest_observed: u64,
        missing_ids: &BTreeSet<u64>,
    ) -> Result<(), String> {
        let missing_str = missing_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        sqlx::query("INSERT OR REPLACE INTO sync_state (id, highest_observed, missing_ids) VALUES (1, ?, ?)")
            .bind(highest_observed as i64)
            .bind(missing_str)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    #[tokio::test]
    async fn test_sqlite_store() {
        let pool = SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .expect("Failed to connect to SQLite with sqlx");
        let store = SqliteBackingStore::new(pool).await;
        assert_eq!(store.load().await.unwrap(), None);

        let data = vec![1, 2, 3, 4];
        store.save(&data).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(data));

        assert_eq!(store.get_sync_state().await.unwrap(), (0, BTreeSet::new()));

        let mut missing = BTreeSet::new();
        missing.insert(2);
        missing.insert(4);
        store.save_sync_state(5, &missing).await.unwrap();

        assert_eq!(store.get_sync_state().await.unwrap(), (5, missing));
    }
}
