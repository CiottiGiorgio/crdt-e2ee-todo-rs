use sqlx::sqlite::SqlitePoolOptions;
use sqlx::SqlitePool;
use std::path::Path;

/// SQLite-backed storage for binary document data (e.g., Automerge documents)
/// and synchronization metadata. Supports both in-memory and file-backed databases.
#[derive(Clone)]
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Creates a new SQLite storage from an existing pool and runs migrations.
    pub async fn new(pool: SqlitePool) -> Result<Self, sqlx::Error> {
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| sqlx::Error::Migrate(Box::new(e)))?;

        Ok(Self { pool })
    }

    /// Creates an in-memory SQLite storage (useful for debug and tests).
    pub async fn in_memory() -> Result<Self, sqlx::Error> {
        let pool = SqlitePoolOptions::new()
            .connect("sqlite::memory:?cache=shared")
            .await?;

        Self::new(pool).await
    }

    /// Creates a file-backed SQLite storage at the specified path.
    pub async fn from_path(db_path: impl AsRef<Path>) -> Result<Self, sqlx::Error> {
        let path = db_path.as_ref();
        let database_url = format!("sqlite://{}?mode=rwc", path.display());

        let pool = SqlitePoolOptions::new().connect(&database_url).await?;

        Self::new(pool).await
    }

    /// Loads document bytes from the storage.
    /// Returns `Ok(Some(bytes))` if data exists, `Ok(None)` if no saved data exists, or `Err` on failure.
    pub async fn load(&self) -> Result<Option<Vec<u8>>, sqlx::Error> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT data FROM automerge_doc WHERE id = 1")
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| r.0))
    }

    /// Saves document bytes to the storage.
    pub async fn save(&self, data: &[u8]) -> Result<(), sqlx::Error> {
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

    #[tokio::test]
    async fn test_sqlite_storage_in_memory() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        assert_eq!(storage.load().await.unwrap(), None);

        let data = vec![1, 2, 3, 4];
        storage.save(&data).await.unwrap();
        assert_eq!(storage.load().await.unwrap(), Some(data));
    }

    #[tokio::test]
    async fn test_sqlite_storage_fails_if_folder_does_not_exist() {
        let nonexistent_path = "/nonexistent_folder_12345/store.db";
        let result = SqliteStorage::from_path(nonexistent_path).await;
        assert!(result.is_err());
    }
}
