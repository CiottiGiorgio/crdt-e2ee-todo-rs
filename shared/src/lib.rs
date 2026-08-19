use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedPayload {
    pub ciphertext: Vec<u8>,
    pub nonce: [u8; 12],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum ClientMessage {
    /// An encoded Automerge `sync::Message`. The sync protocol is symmetric, so
    /// this single variant carries the client's half of the handshake. Scalar
    /// values (todo `text`/`status`) remain encrypted inside the transported
    /// document bytes.
    Sync { data: Vec<u8> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum ServerMessage {
    /// An encoded Automerge `sync::Message` representing the server's half of the
    /// handshake for a given client.
    Sync { data: Vec<u8> },
}
