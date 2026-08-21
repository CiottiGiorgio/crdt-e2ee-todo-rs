use std::time::Duration;

pub const WS_URL: &str = "ws://127.0.0.1:3000/ws";

pub const EXP_BACKOFF_INITIAL_DURATION: Duration = Duration::from_secs(1);
pub const EXP_BACKOFF_FACTOR: u32 = 2;
// TODO: Introduce jitter to avoid a thundering herd.
pub const EXP_BACKOFF_MAX_DURATION: Duration = Duration::from_secs(45);
