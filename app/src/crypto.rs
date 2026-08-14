use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::Rng;
use shared::{EncryptedPayload, IV_SIZE, KEY_SIZE};

pub struct CryptoEngine {
    cipher: Aes256Gcm,
}

impl CryptoEngine {
    pub fn new(key: &[u8; KEY_SIZE]) -> Self {
        let cipher = Aes256Gcm::new(key.into());
        Self { cipher }
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedPayload, String> {
        let mut nonce_bytes = [0u8; IV_SIZE];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from(nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| format!("Encryption error: {}", e))?;

        Ok(EncryptedPayload {
            ciphertext,
            nonce: nonce_bytes,
        })
    }

    pub fn decrypt(&self, payload: &EncryptedPayload) -> Result<Vec<u8>, String> {
        let nonce = Nonce::from(payload.nonce);
        self.cipher
            .decrypt(&nonce, payload.ciphertext.as_slice())
            .map_err(|e| format!("Decryption error: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = [42u8; KEY_SIZE];
        let engine = CryptoEngine::new(&key);

        let plaintext = b"Hello Automerge E2EE!";
        let encrypted = engine.encrypt(plaintext).unwrap();
        let decrypted = engine.decrypt(&encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }
}
