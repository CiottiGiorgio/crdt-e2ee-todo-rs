use crate::models::{TodoItem, TodoStatus};
use crate::repository::TodoRepository;
use sqlx::SqlitePool;

pub struct SqliteTodoRepo {
    pool: SqlitePool,
}

impl SqliteTodoRepo {
    pub async fn new(pool: SqlitePool) -> Result<Self, String> {
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| format!("Failed to run database migrations: {}", e))?;

        Ok(Self { pool })
    }
}

#[async_trait::async_trait]
impl TodoRepository for SqliteTodoRepo {
    async fn get_all(&self) -> Result<Vec<TodoItem>, String> {
        sqlx::query_as::<_, TodoItem>(
            "SELECT id, text, status FROM todos WHERE status != 'deleted' ORDER BY id ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())
    }

    async fn add(&self, text: String) -> Result<TodoItem, String> {
        sqlx::query_as::<_, TodoItem>(
            "INSERT INTO todos (text, status) VALUES (?, 'working_set') RETURNING id, text, status",
        )
        .bind(text)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| e.to_string())
    }

    async fn update_status(&self, id: i32, status: TodoStatus) -> Result<(), String> {
        sqlx::query("UPDATE todos SET status = ? WHERE id = ?")
            .bind(status)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    async fn delete(&self, id: i32) -> Result<(), String> {
        sqlx::query("UPDATE todos SET status = 'deleted' WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }
}
