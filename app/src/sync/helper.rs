use futures_util::SinkExt;
use shared::ClientMessage;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Serializes and sends a [`ClientMessage`] over the WebSocket sink.
pub async fn send_client_message<S>(write: &mut S, msg: &ClientMessage) -> Result<(), String>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let json = serde_json::to_string(msg).map_err(|e| e.to_string())?;
    write
        .send(Message::Text(json.into()))
        .await
        .map_err(|e| e.to_string())
}
