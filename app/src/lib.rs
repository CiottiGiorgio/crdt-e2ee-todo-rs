mod commands;
mod constants;
mod crypto;
mod models;
mod repository;
mod sync;

use crypto::CryptoEngine;
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
            "../client/src/lib/bindings.ts",
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

            println!("Initializing in-memory Automerge document for client...");

            let repo = Arc::new(
                AutomergeTodoRepo::new(None)
                    .expect("failed to initialize automerge repository"),
            );

            // Shared E2EE Symmetric Key (32 bytes)
            let master_key = [42u8; 32];
            let crypto = Arc::new(CryptoEngine::new(&master_key));

            let sync_tx = sync::start_sync_worker(
                repo.clone(),
                crypto,
                app.handle().clone(),
            );
            repo.set_sync_notifier(sync_tx);

            app.manage(AppState {
                todo_repo: repo,
            });

            Ok(())
        })
        .invoke_handler(builder.invoke_handler())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
