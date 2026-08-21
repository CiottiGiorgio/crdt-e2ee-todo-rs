use std::cmp::min;
use std::error::Error;
use std::sync::Arc;
use automerge::Automerge;
use thiserror::{Error};
use futures_util::{Sink, Stream, StreamExt};
use tokio_tungstenite::{connect_async};
use tokio::time::{Duration, sleep};
use tokio_tungstenite::tungstenite::Message;
use tracing::info;
use automerge::sync::{State as AutomergePeerState};
use crate::automerge::AutomergeTodoRepo;

// FIXME: Pull the constant when we finish reworking this module.
const WS_URL: &str = "ws://127.0.0.1:3000/ws";

const EXP_BACKOFF_INITIAL_DURATION: Duration = Duration::from_secs(1);
const EXP_BACKOFF_FACTOR: u32 = 2;
// TODO: Introduce jitter to avoid a thundering herd.
const EXP_BACKOFF_MAX_DURATION: Duration = Duration::from_secs(45);

#[derive(Debug, Error)]
pub enum SyncEngineError {

}

pub async fn sync_engine(repo: Arc<AutomergeTodoRepo>) -> Result<(), SyncEngineError> {
    // In this outer layer we just want to handle connection to the serve with exponential backoff.
    // Whenever we have a connection, we spawn the task that is concerned with synchronization.
    let mut wait_duration = EXP_BACKOFF_INITIAL_DURATION;

    loop {
        info!("Attempting to connect to sync server at {}", WS_URL);
        if let Ok((ws_stream, _)) = connect_async(WS_URL).await {
            info!("Connected to sync server");
            match sync_loop(repo.clone(), ws_stream).await {
                // TODO: Not sure what would be a condition where the sync loop returns without failing.
                //  Document it when it's clear.
                Ok(_) => { wait_duration = EXP_BACKOFF_INITIAL_DURATION }

                // TODO: Propagate errors not related to connection issues.
                //  Continue on errors related to connection issues.
                Err(err) => todo!()
            }
        }
        sleep(wait_duration).await;
        wait_duration = min(wait_duration * EXP_BACKOFF_FACTOR, EXP_BACKOFF_MAX_DURATION);
    }
}

async fn sync_loop(repo: Arc<AutomergeTodoRepo>, connection: impl Sink<Message> + Stream) -> Result<(), Box<dyn Error>> {
    let (mut tx, mut rx) = connection.split();
    let server_state = AutomergePeerState::new();

    repo

    loop {

    }
}
