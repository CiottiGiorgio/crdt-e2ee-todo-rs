use shared::EncryptedPayload;
use sqlx::{Row, SqlitePool};

#[derive(Clone)]
pub struct SyncStore {
    pool: SqlitePool,
}

impl SyncStore {
    pub async fn new(pool: SqlitePool) -> Self {
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run database migrations");

        Self { pool }
    }

    pub async fn get_highest_seq_id(&self) -> Result<u64, sqlx::Error> {
        sqlx::query_scalar::<_, i64>("SELECT highest_seq_id FROM server_state")
            .fetch_one(&self.pool)
            .await
            .map(|v| v as u64)
    }

    pub async fn get_snapshot(&self) -> Result<Option<(u64, EncryptedPayload)>, sqlx::Error> {
        let row = sqlx::query("SELECT seq_id, ciphertext, nonce FROM snapshot WHERE id = 1")
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = row {
            let seq_id: i64 = row.get(0);
            let ciphertext: Vec<u8> = row.get(1);
            let nonce_vec: Vec<u8> = row.get(2);
            if let Ok(nonce) = nonce_vec.try_into() {
                return Ok(Some((seq_id as u64, EncryptedPayload { ciphertext, nonce })));
            }
        }
        Ok(None)
    }

    pub async fn get_deltas_after(
        &self,
        from_seq_id: u64,
    ) -> Result<Vec<(u64, EncryptedPayload)>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT seq_id, ciphertext, nonce FROM deltas WHERE seq_id > ? ORDER BY seq_id ASC",
        )
        .bind(from_seq_id as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut deltas = Vec::new();
        for row in rows {
            let seq_id: i64 = row.get(0);
            let ciphertext: Vec<u8> = row.get(1);
            let nonce_vec: Vec<u8> = row.get(2);
            if let Ok(nonce) = nonce_vec.try_into() {
                deltas.push((seq_id as u64, EncryptedPayload { ciphertext, nonce }));
            }
        }
        Ok(deltas)
    }

    pub async fn save_delta(&self, payload: &EncryptedPayload) -> Result<u64, sqlx::Error> {
        let highest_seq = self.get_highest_seq_id().await.unwrap_or(0);
        let seq = highest_seq + 1;

        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO deltas (seq_id, ciphertext, nonce) VALUES (?, ?, ?)")
            .bind(seq as i64)
            .bind(&payload.ciphertext)
            .bind(&payload.nonce[..])
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(seq)
    }

    pub async fn save_snapshot(
        &self,
        covers_seq_id: u64,
        payload: &EncryptedPayload,
    ) -> Result<bool, sqlx::Error> {
        let current_snap_seq: u64 =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq_id), 0) FROM snapshot")
                .fetch_one(&self.pool)
                .await
                .map(|v| v as u64)
                .unwrap_or(0);

        if covers_seq_id >= current_snap_seq {
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "INSERT OR REPLACE INTO snapshot (id, seq_id, ciphertext, nonce) VALUES (1, ?, ?, ?)",
            )
            .bind(covers_seq_id as i64)
            .bind(&payload.ciphertext)
            .bind(&payload.nonce[..])
            .execute(&mut *tx)
            .await?;

            sqlx::query("DELETE FROM deltas WHERE seq_id <= ?")
                .bind(covers_seq_id as i64)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
