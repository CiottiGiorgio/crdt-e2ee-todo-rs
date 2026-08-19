use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Sends raw encoded sync bytes over the WebSocket sink as a binary frame.
pub async fn send_sync_message<S>(write: &mut S, data: &[u8]) -> Result<(), String>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    write
        .send(Message::Binary(data.to_vec().into()))
        .await
        .map_err(|e| e.to_string())
}
