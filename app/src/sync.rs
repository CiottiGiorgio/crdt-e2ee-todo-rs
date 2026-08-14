use crate::constants::SNAPSHOT_INTERVAL_MINUTES;
use crate::crypto::CryptoEngine;
use crate::repository::automerge::AutomergeTodoRepo;
use automerge::AutoCommit;
use futures_util::{SinkExt, StreamExt};
use shared::{ClientMessage, ServerMessage};
use std::collections::BTreeSet;
use std::sync::Arc;
use tauri::Emitter;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

fn get_highest_continuous_seq(highest_observed: u64, missing: &BTreeSet<u64>) -> u64 {
    match missing.iter().next() {
        Some(&lowest_missing) => lowest_missing.saturating_sub(1),
        None => highest_observed,
    }
}

fn record_observed_seq(
    seq_id: u64,
    highest_observed_seq: &mut u64,
    missing_deltas: &mut BTreeSet<u64>,
) {
    if seq_id > *highest_observed_seq {
        for seq in (*highest_observed_seq + 1)..=seq_id {
            missing_deltas.insert(seq);
        }
        *highest_observed_seq = seq_id;
    }
}

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

                    let mut highest_observed_seq: u64 = 0;
                    let mut missing_deltas: BTreeSet<u64> = BTreeSet::new();

                    // Periodic snapshot timer (e.g. every 5 minutes)
                    let mut snapshot_interval =
                        interval(Duration::from_secs(SNAPSHOT_INTERVAL_MINUTES * 60));
                    // Skip the immediate first tick on connection
                    snapshot_interval.tick().await;

                    loop {
                        tokio::select! {
                            // Periodic Snapshot Timer Triggered
                            _ = snapshot_interval.tick() => {
                                let continuous_seq = get_highest_continuous_seq(highest_observed_seq, &missing_deltas);
                                // A snapshot can ONLY be uploaded if we are fully caught up without any missing holes
                                if missing_deltas.is_empty() && continuous_seq > 0 {
                                    let local_bytes = repo.get_doc_bytes();
                                    if !local_bytes.is_empty() {
                                        if let Ok(encrypted_payload) = crypto.encrypt(&local_bytes) {
                                            let client_msg = ClientMessage::Snapshot {
                                                covers_seq_id: continuous_seq,
                                                payload: encrypted_payload,
                                            };
                                            if let Ok(json) = serde_json::to_string(&client_msg) {
                                                if write.send(Message::Text(json.into())).await.is_ok() {
                                                    println!("Pushed periodic snapshot (covers seq_id: {}) to server!", continuous_seq);
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    println!("Skipping periodic snapshot: missing_deltas has {} holes, continuous_seq={}", missing_deltas.len(), continuous_seq);
                                }
                            }

                            // Incoming message from server
                            Some(msg) = read.next() => {
                                match msg {
                                    Ok(Message::Text(text)) => {
                                        if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) {
                                            match server_msg {
                                                ServerMessage::Snapshot { seq_id, payload } => {
                                                    record_observed_seq(seq_id, &mut highest_observed_seq, &mut missing_deltas);

                                                    if let Ok(decrypted_bytes) = crypto.decrypt(&payload) {
                                                        if let Ok(mut incoming_doc) = AutoCommit::load(&decrypted_bytes) {
                                                            if repo.merge_incoming(&mut incoming_doc).await.is_ok() {
                                                                // Snapshot satisfies all missing deltas <= seq_id ONLY upon successful merge
                                                                missing_deltas.retain(|&s| s > seq_id);
                                                                println!("Successfully merged incoming Snapshot (seq_id: {})", seq_id);
                                                                let _ = app_handle.emit("todos-updated", ());
                                                            }
                                                        }
                                                    }
                                                }
                                                ServerMessage::DeltaBatch { deltas } => {
                                                    let mut merged_any = false;
                                                    for (seq_id, payload) in deltas {
                                                        record_observed_seq(seq_id, &mut highest_observed_seq, &mut missing_deltas);

                                                        if let Ok(decrypted_bytes) = crypto.decrypt(&payload) {
                                                            if let Ok(mut incoming_doc) = AutoCommit::load(&decrypted_bytes) {
                                                                if repo.merge_incoming(&mut incoming_doc).await.is_ok() {
                                                                    merged_any = true;
                                                                    // Only mark delta as satisfied if decrypt & merge succeeded
                                                                    missing_deltas.remove(&seq_id);
                                                                }
                                                            }
                                                        }
                                                    }

                                                    if merged_any {
                                                        println!("Successfully merged incoming DeltaBatch");
                                                        let _ = app_handle.emit("todos-updated", ());
                                                    }

                                                    if !missing_deltas.is_empty() {
                                                        let continuous_seq = get_highest_continuous_seq(highest_observed_seq, &missing_deltas);
                                                        let req_sync = ClientMessage::RequestSync {
                                                            from_seq_id: continuous_seq,
                                                        };
                                                        if let Ok(json) = serde_json::to_string(&req_sync) {
                                                            let _ = write.send(Message::Text(json.into())).await;
                                                        }
                                                    }
                                                }
                                                ServerMessage::Ack { seq_id } => {
                                                    record_observed_seq(seq_id, &mut highest_observed_seq, &mut missing_deltas);
                                                    // Ack indicates a sequence ID position confirmed by server
                                                    missing_deltas.remove(&seq_id);

                                                    let continuous_seq = get_highest_continuous_seq(highest_observed_seq, &missing_deltas);
                                                    println!("Received Ack from server for seq_id: {} (continuous_seq: {})", seq_id, continuous_seq);

                                                    if !missing_deltas.is_empty() {
                                                        let req_sync = ClientMessage::RequestSync {
                                                            from_seq_id: continuous_seq,
                                                        };
                                                        if let Ok(json) = serde_json::to_string(&req_sync) {
                                                            let _ = write.send(Message::Text(json.into())).await;
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
                                let local_bytes = repo.get_doc_bytes();
                                if !local_bytes.is_empty() {
                                    if let Ok(encrypted_payload) = crypto.encrypt(&local_bytes) {
                                        let client_msg = ClientMessage::Delta {
                                            payload: encrypted_payload,
                                        };
                                        if let Ok(json) = serde_json::to_string(&client_msg) {
                                            if write.send(Message::Text(json.into())).await.is_err() {
                                                eprintln!("Failed to push local delta update to server");
                                                break;
                                            } else {
                                                println!("Pushed local delta update immediately to server!");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    println!(
                        "Sync server not available ({}). Retrying in 5 seconds...",
                        e
                    );
                }
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    tx
}
