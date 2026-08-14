use aes_gcm::aead::array::typenum::Unsigned;
use aes_gcm::aead::{AeadCore, KeySizeUser};
use aes_gcm::Aes256Gcm;

#[allow(dead_code)]
pub const AUTOMERGE_FILE_NAME: &str = "todo.automerge";

/// AES-256 Key size in bytes, derived directly from Aes256Gcm
pub const KEY_SIZE: usize = <Aes256Gcm as KeySizeUser>::KeySize::USIZE;

/// AES-256-GCM Nonce / IV size in bytes, derived directly from Aes256Gcm
pub const IV_SIZE: usize = <Aes256Gcm as AeadCore>::NonceSize::USIZE;
