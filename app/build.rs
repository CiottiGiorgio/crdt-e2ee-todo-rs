#[allow(dead_code)]
#[path = "src/sync/constants.rs"]
mod constants;

fn main() {
    println!("cargo:rerun-if-changed=src/sync/constants.rs");

    let initial_secs = constants::EXP_BACKOFF_INITIAL_DURATION.as_secs_f64();
    let max_secs = constants::EXP_BACKOFF_MAX_DURATION.as_secs_f64();
    let factor = constants::EXP_BACKOFF_FACTOR as f64;
    let max_retries = constants::EXP_BACKOFF_MAX_RETRIES as f64;

    if initial_secs > 0.0 && factor > 1.0 {
        let ratio = max_secs / initial_secs;
        let min_attempts = ratio.log(factor);
        if max_retries < min_attempts {
            println!(
                "cargo:warning=EXP_BACKOFF_MAX_RETRIES ({}) is set such that exponential backoff will never reach EXP_BACKOFF_MAX_DURATION ({:?}). Minimum attempts required: ceil(log_{}({:.1})) = {}.",
                constants::EXP_BACKOFF_MAX_RETRIES,
                constants::EXP_BACKOFF_MAX_DURATION,
                constants::EXP_BACKOFF_FACTOR,
                ratio,
                min_attempts.ceil() as u32
            );
        }
    }

    tauri_build::build();
}
