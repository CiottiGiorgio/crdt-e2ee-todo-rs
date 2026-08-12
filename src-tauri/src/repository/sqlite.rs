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

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_repo() -> SqliteTodoRepo {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("Failed to create in-memory database");

        SqliteTodoRepo::new(pool)
            .await
            .expect("Failed to initialize repo and run migrations")
    }

    #[tokio::test]
    async fn test_add_todo_and_get_all() {
        let repo = setup_repo().await;

        let added = repo.add("Test writing Rust tests".to_string()).await.unwrap();
        assert_eq!(added.text, "Test writing Rust tests");
        assert_eq!(added.status, TodoStatus::WorkingSet);

        let todos = repo.get_all().await.unwrap();
        assert_eq!(todos.len(), 1);
        assert_eq!(todos[0].id, added.id);
    }

    #[tokio::test]
    async fn test_update_status() {
        let repo = setup_repo().await;

        let todo = repo.add("Buy groceries".to_string()).await.unwrap();
        repo.update_status(todo.id, TodoStatus::Completed).await.unwrap();

        let todos = repo.get_all().await.unwrap();
        assert_eq!(todos[0].status, TodoStatus::Completed);
    }

    #[tokio::test]
    async fn test_delete_is_soft_delete() {
        let repo = setup_repo().await;

        let todo = repo.add("To be deleted".to_string()).await.unwrap();
        repo.delete(todo.id).await.unwrap();

        let todos = repo.get_all().await.unwrap();
        assert!(todos.is_empty());
    }
}
