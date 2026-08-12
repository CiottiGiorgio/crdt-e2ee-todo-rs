use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedPayload {
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum ClientMessage {
    /// Send an incremental Automerge sync message (delta)
    Delta {
        seq_id: Option<u64>,
        payload: EncryptedPayload,
    },
    /// Upload a compacted snapshot covering up to a specific server seq_id
    Snapshot {
        covers_seq_id: u64,
        payload: EncryptedPayload,
    },
    /// Request sync state from server
    RequestSync,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum ServerMessage {
    /// Server sending a snapshot to the client
    Snapshot {
        seq_id: u64,
        payload: EncryptedPayload,
    },
    /// Server broadcasting a delta to clients
    Delta {
        seq_id: u64,
        payload: EncryptedPayload,
    },
}
