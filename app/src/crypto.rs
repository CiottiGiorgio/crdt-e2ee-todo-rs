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

pub static DEFAULT_CRYPTO: std::sync::LazyLock<CryptoEngine> = std::sync::LazyLock::new(|| {
    let master_key = [42u8; KEY_SIZE];
    CryptoEngine::new(&master_key)
});

pub mod encrypted_string {
    use super::DEFAULT_CRYPTO;
    use autosurgeon::bytes::ByteVec;
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};

    pub fn hydrate<D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'_>,
    ) -> Result<String, HydrateError> {
        let bytes = ByteVec::hydrate(doc, obj, prop)?;
        let decrypted = DEFAULT_CRYPTO
            .decrypt_value(&bytes)
            .map_err(|e| HydrateError::unexpected("valid encrypted bytes", e))?;
        String::from_utf8(decrypted)
            .map_err(|e| HydrateError::unexpected("valid utf-8 string", e.to_string()))
    }

    pub fn reconcile<R: Reconciler>(val: &String, mut reconciler: R) -> Result<(), R::Error> {
        let encrypted = DEFAULT_CRYPTO
            .encrypt_value(val.as_bytes())
            .expect("encryption failed");
        reconciler.bytes(encrypted)
    }
}

pub mod encrypted_status {
    use super::DEFAULT_CRYPTO;
    use crate::models::TodoStatus;
    use autosurgeon::bytes::ByteVec;
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};

    pub fn hydrate<D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'_>,
    ) -> Result<TodoStatus, HydrateError> {
        let bytes = ByteVec::hydrate(doc, obj, prop)?;
        let decrypted = DEFAULT_CRYPTO
            .decrypt_value(&bytes)
            .map_err(|e| HydrateError::unexpected("valid encrypted bytes", e))?;
        let s = String::from_utf8(decrypted)
            .map_err(|e| HydrateError::unexpected("valid utf-8 string", e.to_string()))?;
        s.parse::<TodoStatus>()
            .map_err(|e| HydrateError::unexpected("valid todo status", e.0))
    }

    pub fn reconcile<R: Reconciler>(val: &TodoStatus, mut reconciler: R) -> Result<(), R::Error> {
        let encrypted = DEFAULT_CRYPTO
            .encrypt_value(val.as_ref().as_bytes())
            .expect("encryption failed");
        reconciler.bytes(encrypted)
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
