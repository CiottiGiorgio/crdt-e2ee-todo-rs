mod commands;
mod constants;
mod models;
mod repository;

use repository::automerge::AutomergeTodoRepo;
use repository::TodoRepository;
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

            let doc_path = app_data_dir.join(constants::AUTOMERGE_FILE_NAME);
            println!("Automerge document location: {:?}", doc_path);

            let repo = AutomergeTodoRepo::new(Some(doc_path))
                .expect("failed to initialize automerge repository");

            app.manage(AppState {
                todo_repo: Arc::new(repo),
            });

            Ok(())
        })
        .invoke_handler(builder.invoke_handler())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
