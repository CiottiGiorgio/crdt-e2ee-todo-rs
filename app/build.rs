use std::sync::Arc;

#[path = "src/models.rs"]
mod models;

use models::{TodoItem, TodoStatus};

pub struct MockRepo;
impl MockRepo {
    pub async fn get_all(&self) -> Result<Vec<TodoItem>, String> {
        unimplemented!()
    }
    pub async fn add(&self, _text: String) -> Result<(TodoItem, Vec<u8>), String> {
        unimplemented!()
    }
    pub async fn update_status(&self, _id: String, _status: TodoStatus) -> Result<Vec<u8>, String> {
        unimplemented!()
    }
    pub async fn delete(&self, _id: String) -> Result<Vec<u8>, String> {
        unimplemented!()
    }
}

pub struct MockCrypto;
impl MockCrypto {
    pub fn encrypt(&self, _data: &[u8]) -> Result<Vec<u8>, String> {
        unimplemented!()
    }
}

pub struct MockStore;
impl MockStore {
    pub async fn save(&self, _data: &[u8]) -> Result<(), String> {
        unimplemented!()
    }
}

pub struct MockSyncTx;
impl MockSyncTx {
    pub fn send(&self, _val: ()) -> Result<(), ()> {
        unimplemented!()
    }
}

pub struct AppState {
    pub repo: Arc<MockRepo>,
    pub crypto: Arc<MockCrypto>,
    pub store: Arc<MockStore>,
    pub sync_tx: MockSyncTx,
}

#[path = "src/commands.rs"]
mod commands;

fn main() {
    println!("cargo:rerun-if-changed=src/commands.rs");
    println!("cargo:rerun-if-changed=src/models.rs");

    let builder = commands::get_specta_builder();
    builder
        .export(
            specta_typescript::Typescript::default(),
            "../client/src/lib/bindings.ts",
        )
        .expect("Failed to export specta typescript bindings");

    tauri_build::build();
}
