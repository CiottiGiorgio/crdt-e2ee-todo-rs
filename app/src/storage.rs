use sqlx::sqlite::SqlitePoolOptions;
use sqlx::SqlitePool;
use std::collections::BTreeSet;
use std::path::Path;

/// SQLite-backed storage for binary document data (e.g., Automerge documents)
/// and synchronization metadata. Supports both in-memory and file-backed databases.
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Creates an in-memory SQLite storage (useful for debug and tests).
    pub async fn in_memory() -> Result<Self, String> {
        let pool = SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .map_err(|e| format!("Failed to connect to in-memory SQLite: {}", e))?;

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| format!("Failed to run database migrations: {}", e))?;

        Ok(Self { pool })
    }

    /// Creates a file-backed SQLite storage at the specified path.
    pub async fn from_path(db_path: impl AsRef<Path>) -> Result<Self, String> {
        let path = db_path.as_ref();
        let database_url = format!("sqlite://{}?mode=rwc", path.display());

        let pool = SqlitePoolOptions::new()
            .connect(&database_url)
            .await
            .map_err(|e| format!("Failed to connect to SQLite at {}: {}", path.display(), e))?;

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| format!("Failed to run database migrations: {}", e))?;

        Ok(Self { pool })
    }

    /// Loads document bytes from the storage.
    /// Returns `Ok(Some(bytes))` if data exists, `Ok(None)` if no saved data exists, or `Err` on failure.
    pub async fn load(&self) -> Result<Option<Vec<u8>>, String> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT data FROM automerge_doc WHERE id = 1")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        Ok(row.map(|r| r.0))
    }

    /// Saves document bytes to the storage.
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

    #[tokio::test]
    async fn test_sqlite_storage_in_memory() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        assert_eq!(storage.load().await.unwrap(), None);

        let data = vec![1, 2, 3, 4];
        storage.save(&data).await.unwrap();
        assert_eq!(storage.load().await.unwrap(), Some(data));

        assert_eq!(storage.get_sync_state().await.unwrap(), (0, BTreeSet::new()));

        let mut missing = BTreeSet::new();
        missing.insert(2);
        missing.insert(4);
        storage.save_sync_state(5, &missing).await.unwrap();

        assert_eq!(storage.get_sync_state().await.unwrap(), (5, missing));
    }

    #[tokio::test]
    async fn test_sqlite_storage_fails_if_folder_does_not_exist() {
        let nonexistent_path = "/nonexistent_folder_12345/store.db";
        let result = SqliteStorage::from_path(nonexistent_path).await;
        assert!(result.is_err());
    }
}
