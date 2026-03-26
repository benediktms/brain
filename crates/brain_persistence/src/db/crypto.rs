//! AES-256-GCM encryption for provider API keys.
//!
//! A 256-bit master key is stored at `$BRAIN_HOME/master.key` with `0600`
//! permissions.  The key is auto-generated on first use.  Each encrypt
//! operation produces a random 96-bit nonce prepended to the ciphertext,
//! so the same plaintext always produces different output.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;

use crate::error::{BrainCoreError, Result};

/// Expected file permission bits for the master key file.
const KEY_FILE_MODE: u32 = 0o600;

/// AES-256-GCM nonce length in bytes.
const NONCE_LEN: usize = 12;

/// AES-256 key length in bytes.
const KEY_LEN: usize = 32;

/// Resolve the master key path: `$BRAIN_HOME/master.key`.
pub fn master_key_path(brain_home: &Path) -> PathBuf {
    brain_home.join("master.key")
}

/// Load the master key from disk, generating it if it doesn't exist.
pub fn load_or_create_master_key(brain_home: &Path) -> Result<[u8; KEY_LEN]> {
    let path = master_key_path(brain_home);
    if path.exists() {
        load_master_key(&path)
    } else {
        generate_master_key(&path)
    }
}

/// Load an existing master key, validating permissions.
fn load_master_key(path: &Path) -> Result<[u8; KEY_LEN]> {
    // Verify permissions aren't too open
    let meta = fs::metadata(path).map_err(BrainCoreError::Io)?;
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(BrainCoreError::Config(format!(
            "master key file {} has overly broad permissions ({:#o}). \
             Fix with: chmod 600 {}",
            path.display(),
            mode,
            path.display()
        )));
    }

    let bytes = fs::read(path).map_err(BrainCoreError::Io)?;
    if bytes.len() != KEY_LEN {
        return Err(BrainCoreError::Config(format!(
            "master key file {} has invalid length ({} bytes, expected {})",
            path.display(),
            bytes.len(),
            KEY_LEN,
        )));
    }

    let mut key = [0u8; KEY_LEN];
    key.copy_from_slice(&bytes);
    Ok(key)
}

/// Generate a new random master key and write it with `0600` permissions.
fn generate_master_key(path: &Path) -> Result<[u8; KEY_LEN]> {
    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(BrainCoreError::Io)?;
    }

    let mut key = [0u8; KEY_LEN];
    OsRng.fill_bytes(&mut key);

    fs::write(path, key).map_err(BrainCoreError::Io)?;
    fs::set_permissions(path, fs::Permissions::from_mode(KEY_FILE_MODE))
        .map_err(BrainCoreError::Io)?;

    Ok(key)
}

/// Encrypt plaintext with AES-256-GCM.  Returns `nonce || ciphertext` encoded
/// as base64.
pub fn encrypt(master_key: &[u8; KEY_LEN], plaintext: &str) -> Result<String> {
    let cipher = Aes256Gcm::new_from_slice(master_key)
        .map_err(|e| BrainCoreError::Config(format!("invalid master key: {e}")))?;

    let mut nonce_bytes = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| BrainCoreError::Config(format!("encryption failed: {e}")))?;

    // Prepend nonce to ciphertext
    let mut combined = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&ciphertext);

    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        &combined,
    ))
}

/// Decrypt a base64-encoded `nonce || ciphertext` back to plaintext.
pub fn decrypt(master_key: &[u8; KEY_LEN], encoded: &str) -> Result<String> {
    let combined = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
        .map_err(|e| BrainCoreError::Config(format!("base64 decode failed: {e}")))?;

    if combined.len() < NONCE_LEN + 1 {
        return Err(BrainCoreError::Config(
            "encrypted data too short".to_string(),
        ));
    }

    let (nonce_bytes, ciphertext) = combined.split_at(NONCE_LEN);
    let nonce = Nonce::from_slice(nonce_bytes);

    let cipher = Aes256Gcm::new_from_slice(master_key)
        .map_err(|e| BrainCoreError::Config(format!("invalid master key: {e}")))?;

    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| BrainCoreError::Config(format!("decryption failed: {e}")))?;

    String::from_utf8(plaintext)
        .map_err(|e| BrainCoreError::Config(format!("decrypted data is not valid UTF-8: {e}")))
}

/// Hash an API key with blake3 for uniqueness enforcement.
pub fn hash_api_key(api_key: &str) -> String {
    blake3::hash(api_key.as_bytes()).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_generate_and_load_master_key() {
        let dir = TempDir::new().unwrap();
        let key1 = load_or_create_master_key(dir.path()).unwrap();
        let key2 = load_or_create_master_key(dir.path()).unwrap();
        assert_eq!(key1, key2, "loading should return the same key");
    }

    #[test]
    fn test_master_key_file_permissions() {
        let dir = TempDir::new().unwrap();
        load_or_create_master_key(dir.path()).unwrap();

        let path = master_key_path(dir.path());
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn test_encrypt_decrypt_round_trip() {
        let dir = TempDir::new().unwrap();
        let key = load_or_create_master_key(dir.path()).unwrap();

        let plaintext = "sk-ant-api03-test-key-12345";
        let encrypted = encrypt(&key, plaintext).unwrap();
        let decrypted = decrypt(&key, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypt_produces_different_ciphertext() {
        let dir = TempDir::new().unwrap();
        let key = load_or_create_master_key(dir.path()).unwrap();

        let plaintext = "sk-ant-api03-test-key-12345";
        let enc1 = encrypt(&key, plaintext).unwrap();
        let enc2 = encrypt(&key, plaintext).unwrap();
        assert_ne!(
            enc1, enc2,
            "random nonce should produce different ciphertext"
        );
    }

    #[test]
    fn test_wrong_key_fails_decryption() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let key1 = load_or_create_master_key(dir1.path()).unwrap();
        let key2 = load_or_create_master_key(dir2.path()).unwrap();

        let encrypted = encrypt(&key1, "secret").unwrap();
        let result = decrypt(&key2, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_hash_api_key_deterministic() {
        let h1 = hash_api_key("sk-test-123");
        let h2 = hash_api_key("sk-test-123");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_api_key_different_keys_differ() {
        let h1 = hash_api_key("sk-test-123");
        let h2 = hash_api_key("sk-test-456");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_overly_open_permissions_rejected() {
        let dir = TempDir::new().unwrap();
        let key_path = master_key_path(dir.path());

        // Write a valid key with bad permissions
        let mut key = [0u8; KEY_LEN];
        OsRng.fill_bytes(&mut key);
        fs::write(&key_path, &key).unwrap();
        fs::set_permissions(&key_path, fs::Permissions::from_mode(0o644)).unwrap();

        let result = load_or_create_master_key(dir.path());
        assert!(result.is_err());
    }
}
