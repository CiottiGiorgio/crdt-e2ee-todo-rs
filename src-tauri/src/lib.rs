mod constants;
mod models;

use models::TodoItem;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use tauri::Manager;
use tauri_specta::{collect_commands, Builder};

pub struct DbState {
    pub pool: SqlitePool,
}

#[tauri::command]
#[specta::specta]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command]
#[specta::specta]
fn get_todos() -> Vec<TodoItem> {
    vec![
        TodoItem {
            id: 1,
            text: "Read a book".into(),
            completed: false,
            in_working_set: true,
        },
        TodoItem {
            id: 2,
            text: "Buy groceries".into(),
            completed: false,
            in_working_set: false,
        },
        TodoItem {
            id: 3,
            text: "Clean the room".into(),
            completed: true,
            in_working_set: false,
        },
    ]
}

fn get_specta_builder() -> Builder<tauri::Wry> {
    Builder::<tauri::Wry>::new().commands(collect_commands![greet, get_todos])
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let builder = get_specta_builder();

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

                app.manage(DbState { pool });
            });

            Ok(())
        })
        .invoke_handler(builder.invoke_handler())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
