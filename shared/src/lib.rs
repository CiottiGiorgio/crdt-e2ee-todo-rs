use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedPayload {
    pub ciphertext: Vec<u8>,
    pub nonce: [u8; 12],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum ClientMessage {
    /// Send an incremental Automerge sync message (delta)
    Delta { payload: EncryptedPayload },
    /// Request sync state from server starting after from_seq_id
    RequestSync { from_seq_id: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum ServerMessage {
    /// Server broadcasting a batch of deltas to clients
    DeltaBatch {
        deltas: Vec<(u64, EncryptedPayload)>,
    },
}
