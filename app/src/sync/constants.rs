use std::time::Duration;

pub const WS_URL: &str = "ws://127.0.0.1:3000/ws";

pub const EXP_BACKOFF_INITIAL_DURATION: Duration = Duration::from_millis(500);
pub const EXP_BACKOFF_FACTOR: u32 = 2;
pub const EXP_BACKOFF_MAX_DURATION: Duration = Duration::from_secs(10);
pub const EXP_BACKOFF_MAX_RETRIES: u32 = 6;
