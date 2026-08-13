use aes_gcm::aead::generic_array::typenum::Unsigned;
use aes_gcm::aead::{AeadCore, KeySizeUser};
use aes_gcm::Aes256Gcm;
use serde::{Deserialize, Serialize};

/// AES-256 Key size in bytes, derived directly from Aes256Gcm (32 bytes)
pub const KEY_SIZE: usize = <Aes256Gcm as KeySizeUser>::KeySize::USIZE;

/// AES-256-GCM Nonce / IV size in bytes, derived directly from Aes256Gcm (12 bytes)
pub const IV_SIZE: usize = <Aes256Gcm as AeadCore>::NonceSize::USIZE;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedPayload {
    pub ciphertext: Vec<u8>,
    pub nonce: [u8; IV_SIZE],
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
    /// Request sync state from server starting after from_seq_id
    RequestSync {
        from_seq_id: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum ServerMessage {
    /// Server handshake message advertising its highest known sequence ID
    Welcome {
        highest_seq_id: u64,
    },
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
