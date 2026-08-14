use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::RwLock;
use tokio::fs;

/// Async trait representing a backing store for binary document data (e.g., Automerge documents).
#[async_trait]
pub trait BackingStore: Send + Sync {
    /// Loads document bytes from the store.
    /// Returns `Ok(Some(bytes))` if data exists, `Ok(None)` if no saved data exists, or `Err` on failure.
    async fn load(&self) -> Result<Option<Vec<u8>>, String>;

    /// Saves document bytes to the store.
    async fn save(&self, data: &[u8]) -> Result<(), String>;
}

/// Async file-backed implementation of [`BackingStore`].
pub struct FileBackingStore {
    path: PathBuf,
}

impl FileBackingStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl BackingStore for FileBackingStore {
    async fn load(&self) -> Result<Option<Vec<u8>>, String> {
        if self.path.exists() {
            let data = fs::read(&self.path).await.map_err(|e| e.to_string())?;
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    async fn save(&self, data: &[u8]) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        fs::write(&self.path, data).await.map_err(|e| e.to_string())
    }
}

/// In-memory implementation of [`BackingStore`].
pub struct InMemoryBackingStore {
    bytes: RwLock<Option<Vec<u8>>>,
}

impl InMemoryBackingStore {
    pub fn new() -> Self {
        Self {
            bytes: RwLock::new(None),
        }
    }

    pub fn with_data(data: Vec<u8>) -> Self {
        Self {
            bytes: RwLock::new(Some(data)),
        }
    }
}

impl Default for InMemoryBackingStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BackingStore for InMemoryBackingStore {
    async fn load(&self) -> Result<Option<Vec<u8>>, String> {
        let guard = self.bytes.read().map_err(|e| e.to_string())?;
        Ok(guard.clone())
    }

    async fn save(&self, data: &[u8]) -> Result<(), String> {
        let mut guard = self.bytes.write().map_err(|e| e.to_string())?;
        *guard = Some(data.to_vec());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_store() {
        let store = InMemoryBackingStore::new();
        assert_eq!(store.load().await.unwrap(), None);

        let data = vec![1, 2, 3, 4];
        store.save(&data).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(data));
    }

    #[tokio::test]
    async fn test_file_backing_store() {
        let test_path =
            std::env::temp_dir().join(format!("test_store_{}.bin", uuid::Uuid::new_v4()));

        let store = FileBackingStore::new(&test_path);
        assert_eq!(store.load().await.unwrap(), None);

        let data = vec![10, 20, 30];
        store.save(&data).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(vec![10, 20, 30]));

        // Reload from newly instantiated store pointing to same path
        let store2 = FileBackingStore::new(&test_path);
        assert_eq!(store2.load().await.unwrap(), Some(vec![10, 20, 30]));

        // Clean up
        let _ = tokio::fs::remove_file(&test_path).await;
    }
}
