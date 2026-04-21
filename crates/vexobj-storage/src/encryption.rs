//! Server-side encryption at rest using AES-256-GCM with deterministic keys.
//!
//! Dedup-friendly: the per-blob key and nonce are derived from the master key
//! and the blob's plaintext SHA-256 via HKDF. Two identical plaintexts produce
//! identical ciphertexts, so content-addressable dedup still works.
//!
//! Trade-off: deterministic AEAD leaks equality of ciphertexts (i.e. "these
//! two blobs are the same"). That's fine for a self-hosted store where the
//! threat model is disk seizure — the content itself is still confidential.

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use hkdf::Hkdf;
use sha2::Sha256;

use crate::error::StorageError;

/// 32-byte master key used to derive per-blob keys and nonces.
pub struct Encryptor {
    master_key: [u8; 32],
}

impl Encryptor {
    /// Parse a hex-encoded 32-byte master key (64 hex chars).
    pub fn from_hex(hex: &str) -> Result<Self, StorageError> {
        let bytes = ::hex::decode(hex.trim()).map_err(|e| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("master_key must be hex: {e}"),
            ))
        })?;
        if bytes.len() != 32 {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "master_key must be 32 bytes (64 hex chars), got {}",
                    bytes.len()
                ),
            )));
        }
        let mut master_key = [0u8; 32];
        master_key.copy_from_slice(&bytes);
        Ok(Self { master_key })
    }

    /// Derive a (key, nonce) pair for the blob whose plaintext hashes to
    /// `sha256_hex`. Same inputs always produce the same outputs, which is
    /// what makes dedup possible.
    fn derive(&self, sha256_hex: &str) -> ([u8; 32], [u8; 12]) {
        let hk = Hkdf::<Sha256>::new(Some(sha256_hex.as_bytes()), &self.master_key);
        let mut okm = [0u8; 44];
        hk.expand(b"vexobj-sse-v1", &mut okm)
            .expect("HKDF expand with <= 255*HashLen output");
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        key.copy_from_slice(&okm[..32]);
        nonce.copy_from_slice(&okm[32..44]);
        (key, nonce)
    }

    pub fn encrypt(&self, sha256_hex: &str, plaintext: &[u8]) -> Result<Vec<u8>, StorageError> {
        let (key, nonce) = self.derive(sha256_hex);
        let cipher = Aes256Gcm::new((&key).into());
        cipher
            .encrypt(Nonce::from_slice(&nonce), plaintext)
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("encryption failed: {e}"))))
    }

    pub fn decrypt(&self, sha256_hex: &str, ciphertext: &[u8]) -> Result<Vec<u8>, StorageError> {
        let (key, nonce) = self.derive(sha256_hex);
        let cipher = Aes256Gcm::new((&key).into());
        cipher
            .decrypt(Nonce::from_slice(&nonce), ciphertext)
            .map_err(|e| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("decryption failed (tampered or wrong key): {e}"),
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_HEX: &str = "0011223344556677889900112233445566778899001122334455667788990011";

    #[test]
    fn round_trip() {
        let e = Encryptor::from_hex(KEY_HEX).unwrap();
        let plain = b"hello encrypted world";
        let ct = e.encrypt("deadbeef", plain).unwrap();
        assert_ne!(ct.as_slice(), plain);
        let pt = e.decrypt("deadbeef", &ct).unwrap();
        assert_eq!(pt, plain);
    }

    #[test]
    fn dedup_friendly() {
        let e = Encryptor::from_hex(KEY_HEX).unwrap();
        let plain = b"identical content";
        let ct1 = e.encrypt("sha1", plain).unwrap();
        let ct2 = e.encrypt("sha1", plain).unwrap();
        assert_eq!(ct1, ct2, "same sha → same ciphertext (dedup)");
    }

    #[test]
    fn different_sha_different_ciphertext() {
        let e = Encryptor::from_hex(KEY_HEX).unwrap();
        let plain = b"same bytes different addr";
        let ct1 = e.encrypt("sha1", plain).unwrap();
        let ct2 = e.encrypt("sha2", plain).unwrap();
        assert_ne!(ct1, ct2);
    }

    #[test]
    fn rejects_tampered_ciphertext() {
        let e = Encryptor::from_hex(KEY_HEX).unwrap();
        let ct = e.encrypt("sha", b"payload").unwrap();
        let mut bad = ct.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0x01;
        assert!(e.decrypt("sha", &bad).is_err());
    }

    #[test]
    fn rejects_bad_hex_or_length() {
        assert!(Encryptor::from_hex("not-hex").is_err());
        assert!(Encryptor::from_hex("0011").is_err());
    }
}
