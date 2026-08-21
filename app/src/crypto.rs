use crate::constants::{IV_SIZE, KEY_SIZE};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::Rng;

pub struct CryptoEngine {
    cipher: Aes256Gcm,
}

impl CryptoEngine {
    pub fn new(key: &[u8; KEY_SIZE]) -> Self {
        let cipher = Aes256Gcm::new(key.into());
        Self { cipher }
    }

    /// Encrypts a single scalar value, returning a self-describing
    /// `nonce(12) || ciphertext` byte vector suitable for storing inside an
    /// automerge `Bytes` scalar value.
    pub fn encrypt_value(&self, plaintext: &[u8]) -> Result<Vec<u8>, String> {
        let mut nonce_bytes = [0u8; IV_SIZE];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from(nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| format!("Encryption error: {}", e))?;

        let mut out = Vec::with_capacity(IV_SIZE + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypts a value produced by [`encrypt_value`](Self::encrypt_value),
    /// expecting a `nonce(12) || ciphertext` layout.
    pub fn decrypt_value(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        if data.len() < IV_SIZE {
            return Err("Encrypted value is too short to contain a nonce".to_string());
        }
        let (nonce_bytes, ciphertext) = data.split_at(IV_SIZE);
        let nonce_arr: [u8; IV_SIZE] = nonce_bytes
            .try_into()
            .map_err(|_| "Invalid nonce length".to_string())?;
        let nonce = Nonce::from(nonce_arr);
        self.cipher
            .decrypt(&nonce, ciphertext)
            .map_err(|e| format!("Decryption error: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_value_roundtrip() {
        let key = [42u8; KEY_SIZE];
        let engine = CryptoEngine::new(&key);

        let plaintext = b"Buy milk";
        let encrypted = engine.encrypt_value(plaintext).unwrap();

        // The encoding is nonce(12) || ciphertext and must not contain the plaintext.
        assert!(encrypted.len() > IV_SIZE);
        assert!(!encrypted.windows(plaintext.len()).any(|w| w == plaintext));

        let decrypted = engine.decrypt_value(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_decrypt_value_rejects_short_input() {
        let key = [42u8; KEY_SIZE];
        let engine = CryptoEngine::new(&key);
        assert!(engine.decrypt_value(&[0u8; 4]).is_err());
    }
}
