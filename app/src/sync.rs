use crate::constants::{SNAPSHOT_INTERVAL_MINUTES, SYNC_DEBOUNCE_MS};
use crate::crypto::CryptoEngine;
use crate::repository::automerge::AutomergeTodoRepo;
use automerge::AutoCommit;
use futures_util::{SinkExt, StreamExt};
use shared::{ClientMessage, ServerMessage};
use std::pin::Pin;
use std::sync::Arc;
use tauri::Emitter;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Duration, Sleep};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

pub fn start_sync_worker(
    repo: Arc<AutomergeTodoRepo>,
    crypto: Arc<CryptoEngine>,
    app_handle: tauri::AppHandle,
) -> mpsc::UnboundedSender<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<()>();

    tauri::async_runtime::spawn(async move {
        let ws_url = "ws://127.0.0.1:3000/ws";

        loop {
            println!("Attempting to connect to sync server at {}", ws_url);
            match connect_async(ws_url).await {
                Ok((ws_stream, _)) => {
                    println!("Connected to sync server!");
                    let (mut write, mut read) = ws_stream.split();

                    // Send RequestSync on initial connection
                    let req_sync = serde_json::to_string(&ClientMessage::RequestSync).unwrap();
                    if let Err(e) = write.send(Message::Text(req_sync.into())).await {
                        eprintln!("Failed to send RequestSync: {}", e);
                        sleep(Duration::from_secs(3)).await;
                        continue;
                    }

                    let mut highest_seq_id: u64 = 0;
                    let mut debounce_timer: Option<Pin<Box<Sleep>>> = None;

                    // Periodic snapshot timer (e.g. every 5 minutes)
                    let mut snapshot_interval = interval(Duration::from_secs(SNAPSHOT_INTERVAL_MINUTES * 60));
                    // Skip the immediate first tick on connection
                    snapshot_interval.tick().await;

                    loop {
                        tokio::select! {
                            // Periodic Snapshot Timer Triggered
                            _ = snapshot_interval.tick() => {
                                let local_bytes = repo.get_doc_bytes();
                                if let Ok(encrypted_payload) = crypto.encrypt(&local_bytes) {
                                    let client_msg = ClientMessage::Snapshot {
                                        covers_seq_id: highest_seq_id,
                                        payload: encrypted_payload,
                                    };
                                    if let Ok(json) = serde_json::to_string(&client_msg) {
                                        if write.send(Message::Text(json.into())).await.is_ok() {
                                            println!("Pushed periodic snapshot (covers seq_id: {}) to server!", highest_seq_id);
                                        }
                                    }
                                }
                            }

                            // Debounce timer completion for local changes
                            _ = async {
                                match debounce_timer.as_mut() {
                                    Some(timer) => timer.await,
                                    None => futures_util::future::pending().await,
                                }
                            } => {
                                debounce_timer = None;
                                let local_bytes = repo.get_doc_bytes();
                                if let Ok(encrypted_payload) = crypto.encrypt(&local_bytes) {
                                    let client_msg = ClientMessage::Delta {
                                        seq_id: None,
                                        payload: encrypted_payload,
                                    };
                                    if let Ok(json) = serde_json::to_string(&client_msg) {
                                        if write.send(Message::Text(json.into())).await.is_err() {
                                            eprintln!("Failed to push debounced update to server");
                                            break;
                                        } else {
                                            println!("Pushed debounced local delta update to server!");
                                        }
                                    }
                                }
                            }

                            // Incoming message from server
                            Some(msg) = read.next() => {
                                match msg {
                                    Ok(Message::Text(text)) => {
                                        if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) {
                                            match server_msg {
                                                ServerMessage::Snapshot { seq_id, payload } | ServerMessage::Delta { seq_id, payload } => {
                                                    highest_seq_id = highest_seq_id.max(seq_id);
                                                    if let Ok(decrypted_bytes) = crypto.decrypt(&payload) {
                                                        if let Ok(mut incoming_doc) = AutoCommit::load(&decrypted_bytes) {
                                                            if repo.merge_incoming(&mut incoming_doc).is_ok() {
                                                                println!("Successfully merged incoming state (seq_id: {})", seq_id);
                                                                let _ = app_handle.emit("todos-updated", ());
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Ok(Message::Close(_)) | Err(_) => {
                                        eprintln!("Server WebSocket connection closed");
                                        break;
                                    }
                                    _ => {}
                                }
                            }

                            // Local change notification triggered from Tauri command
                            Some(_) = rx.recv() => {
                                // Reset/start the debouncing timer
                                debounce_timer = Some(Box::pin(sleep(Duration::from_millis(SYNC_DEBOUNCE_MS))));
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Sync server not available ({}). Retrying in 5 seconds...", e);
                }
            }

            sleep(Duration::from_secs(5)).await;
        }
    });

    tx
}
