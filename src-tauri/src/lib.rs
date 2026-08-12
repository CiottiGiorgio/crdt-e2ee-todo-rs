mod commands;
mod constants;
mod models;
mod repository;

use repository::sqlite::SqliteTodoRepo;
use repository::TodoRepository;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::sync::Arc;
use tauri::Manager;

pub struct AppState {
    pub todo_repo: Arc<dyn TodoRepository>,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let builder = commands::get_specta_builder();

    #[cfg(debug_assertions)]
    builder
        .export(
            specta_typescript::Typescript::default(),
            "../src/lib/bindings.ts",
        )
        .expect("Failed to export specta typescript bindings");

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(move |app| {
            let app_data_dir = app
                .path()
                .app_data_dir()
                .expect("failed to resolve app data directory");

            if !app_data_dir.exists() {
                std::fs::create_dir_all(&app_data_dir)
                    .expect("failed to create app data directory");
            }

            let db_path = app_data_dir.join(constants::DB_FILE_NAME);
            println!("SQLite database location: {:?}", db_path);

            let options = SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(true);

            tauri::async_runtime::block_on(async move {
                let pool = SqlitePoolOptions::new()
                    .connect_with(options)
                    .await
                    .expect("failed to connect to sqlite database");

                let repo = SqliteTodoRepo::new(pool)
                    .await
                    .expect("failed to initialize repository");

                app.manage(AppState {
                    todo_repo: Arc::new(repo),
                });
            });

            Ok(())
        })
        .invoke_handler(builder.invoke_handler())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
