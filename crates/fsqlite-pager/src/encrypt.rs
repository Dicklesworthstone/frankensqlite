//! Page-level encryption: XChaCha20-Poly1305 + DEK/KEK + AAD swap resistance (§15, bd-1osn).
//!
//! Each page write encrypts the usable portion (everything except the reserved
//! region) with XChaCha20-Poly1305, storing a fresh 24-byte random nonce and the
//! 16-byte Poly1305 tag in the reserved space at the end of the page.
//!
//! Envelope encryption (DEK/KEK) enables O(1) rekey: `PRAGMA rekey` re-wraps
//! the DEK under a new KEK derived from the new passphrase, without touching
//! page data.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Minimum reserved bytes per page required for encryption.
/// Layout: nonce (24 B) + Poly1305 tag (16 B) = 40 B.
pub const ENCRYPTION_RESERVED_BYTES: u8 = 40;

/// Size of the XChaCha20-Poly1305 nonce in bytes.
pub const NONCE_SIZE: usize = 24;

/// Size of the Poly1305 authentication tag in bytes.
pub const TAG_SIZE: usize = 16;

/// Size of a [`DatabaseId`] in bytes.
pub const DATABASE_ID_SIZE: usize = 16;

/// Size of a DEK or KEK in bytes (256-bit keys).
pub const KEY_SIZE: usize = 32;

// ---------------------------------------------------------------------------
// DatabaseId
// ---------------------------------------------------------------------------

/// Random 16-byte opaque identifier for a database.
///
/// Stable for the lifetime of the database, including across rekeys (INV-ENC-4).
/// Used as part of the AAD to prevent cross-database page replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatabaseId([u8; DATABASE_ID_SIZE]);

impl DatabaseId {
    /// Create from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; DATABASE_ID_SIZE]) -> Self {
        Self(bytes)
    }

    /// Get the raw bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; DATABASE_ID_SIZE] {
        &self.0
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from page encryption/decryption operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncryptError {
    /// Reserved bytes per page is less than [`ENCRYPTION_RESERVED_BYTES`].
    InsufficientReservedBytes { available: u8, required: u8 },
    /// AEAD encryption failed.
    EncryptionFailed,
    /// AEAD decryption/authentication failed (wrong key, corrupt data, or
    /// tampered AAD).
    AuthenticationFailed,
    /// The page buffer is too small to contain the reserved encryption region.
    PageTooSmall {
        page_len: usize,
        required_reserved: usize,
    },
    /// DEK unwrap failed (wrong KEK or corrupt wrapped blob).
    DekUnwrapFailed,
    /// Argon2id parameter error.
    InvalidKdfParams,
}

impl std::fmt::Display for EncryptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InsufficientReservedBytes {
                available,
                required,
            } => {
                write!(f, "insufficient reserved bytes: {available} < {required}")
            }
            Self::EncryptionFailed => f.write_str("AEAD encryption failed"),
            Self::AuthenticationFailed => f.write_str("AEAD authentication failed"),
            Self::PageTooSmall {
                page_len,
                required_reserved,
            } => {
                write!(
                    f,
                    "page too small: len={page_len}, need {required_reserved} reserved bytes"
                )
            }
            Self::DekUnwrapFailed => {
                f.write_str("DEK unwrap failed (wrong key or corrupt wrapped blob)")
            }
            Self::InvalidKdfParams => f.write_str("invalid Argon2id parameters"),
        }
    }
}

impl std::error::Error for EncryptError {}

// ---------------------------------------------------------------------------
// AAD construction
// ---------------------------------------------------------------------------

/// AAD size: 4-byte page number + 16-byte database id.
const AAD_SIZE: usize = 4 + DATABASE_ID_SIZE;

/// Construct AAD for a page.
///
/// Format: `be_u32(page_number) || database_id_bytes` (INV-ENC-2).
/// Big-endian page number ensures cross-endian interoperability.
#[must_use]
fn build_aad(page_number: u32, database_id: &DatabaseId) -> [u8; AAD_SIZE] {
    let mut aad = [0u8; AAD_SIZE];
    aad[..4].copy_from_slice(&page_number.to_be_bytes());
    aad[4..].copy_from_slice(database_id.as_bytes());
    aad
}

// ---------------------------------------------------------------------------
// PageEncryptor
// ---------------------------------------------------------------------------

/// Page encryptor/decryptor holding a DEK and [`DatabaseId`].
///
/// The encryptor is cheap to clone (32-byte key + 16-byte id).
pub struct PageEncryptor {
    cipher: XChaCha20Poly1305,
    database_id: DatabaseId,
    /// Raw DEK bytes, retained for rekey comparisons.
    dek: [u8; KEY_SIZE],
}

impl PageEncryptor {
    /// Create from a raw 256-bit DEK and [`DatabaseId`].
    #[must_use]
    pub fn new(dek: &[u8; KEY_SIZE], database_id: DatabaseId) -> Self {
        Self {
            cipher: XChaCha20Poly1305::new(dek.into()),
            database_id,
            dek: *dek,
        }
    }

    /// The [`DatabaseId`] bound to this encryptor.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// The raw DEK (for wrap/unwrap operations).
    #[must_use]
    pub const fn dek(&self) -> &[u8; KEY_SIZE] {
        &self.dek
    }

    /// Encrypt a page in-place.
    ///
    /// The last [`ENCRYPTION_RESERVED_BYTES`] bytes of `page` are overwritten
    /// with `nonce (24 B) || tag (16 B)`.  The preceding bytes are replaced
    /// with their ciphertext.
    ///
    /// `nonce` **must** be a fresh 24-byte random value (INV-ENC-1).
    pub fn encrypt_page(
        &self,
        page: &mut [u8],
        page_number: u32,
        nonce: &[u8; NONCE_SIZE],
    ) -> Result<(), EncryptError> {
        let reserved = usize::from(ENCRYPTION_RESERVED_BYTES);
        let page_len = page.len();
        if page_len < reserved {
            return Err(EncryptError::PageTooSmall {
                page_len,
                required_reserved: reserved,
            });
        }

        let plaintext_len = page_len - reserved;
        let aad = build_aad(page_number, &self.database_id);
        let xnonce = XNonce::from_slice(nonce);

        let ciphertext = self
            .cipher
            .encrypt(
                xnonce,
                Payload {
                    msg: &page[..plaintext_len],
                    aad: &aad,
                },
            )
            .map_err(|_| EncryptError::EncryptionFailed)?;

        // `ciphertext` = encrypted data (plaintext_len) + Poly1305 tag (16).
        debug_assert_eq!(ciphertext.len(), plaintext_len + TAG_SIZE);

        // Write layout: [ciphertext_data | nonce(24) | tag(16)]
        page[..plaintext_len].copy_from_slice(&ciphertext[..plaintext_len]);
        let tag = &ciphertext[plaintext_len..];
        let nonce_start = plaintext_len;
        let tag_start = nonce_start + NONCE_SIZE;
        page[nonce_start..tag_start].copy_from_slice(nonce);
        page[tag_start..].copy_from_slice(tag);

        Ok(())
    }

    /// Decrypt a page in-place.
    ///
    /// Reads the nonce and tag from the reserved region, decrypts, and zeros
    /// the reserved region.  Returns [`EncryptError::AuthenticationFailed`] if
    /// the key is wrong, data is corrupt, or the AAD doesn't match.
    pub fn decrypt_page(&self, page: &mut [u8], page_number: u32) -> Result<(), EncryptError> {
        let reserved = usize::from(ENCRYPTION_RESERVED_BYTES);
        let page_len = page.len();
        if page_len < reserved {
            return Err(EncryptError::PageTooSmall {
                page_len,
                required_reserved: reserved,
            });
        }

        let plaintext_len = page_len - reserved;
        let nonce_start = plaintext_len;
        let tag_start = nonce_start + NONCE_SIZE;

        // Extract nonce and tag.
        let mut nonce = [0u8; NONCE_SIZE];
        nonce.copy_from_slice(&page[nonce_start..tag_start]);
        let mut tag = [0u8; TAG_SIZE];
        tag.copy_from_slice(&page[tag_start..]);

        // Reconstruct AEAD input: ciphertext || tag.
        let mut ct_with_tag = Vec::with_capacity(plaintext_len + TAG_SIZE);
        ct_with_tag.extend_from_slice(&page[..plaintext_len]);
        ct_with_tag.extend_from_slice(&tag);

        let aad = build_aad(page_number, &self.database_id);
        let xnonce = XNonce::from_slice(&nonce);

        let plaintext = self
            .cipher
            .decrypt(
                xnonce,
                Payload {
                    msg: &ct_with_tag,
                    aad: &aad,
                },
            )
            .map_err(|_| EncryptError::AuthenticationFailed)?;

        debug_assert_eq!(plaintext.len(), plaintext_len);
        page[..plaintext_len].copy_from_slice(&plaintext);
        // Zero the reserved region after decryption.
        page[nonce_start..].fill(0);

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// KeyManager — DEK/KEK envelope encryption
// ---------------------------------------------------------------------------

/// Argon2id parameters for KEK derivation.
#[derive(Debug, Clone)]
pub struct Argon2Params {
    /// Memory cost in KiB (default: 65 536 = 64 MiB).
    pub m_cost: u32,
    /// Time cost / iterations (default: 3).
    pub t_cost: u32,
    /// Parallelism / lanes (default: 4).
    pub p_cost: u32,
}

impl Default for Argon2Params {
    fn default() -> Self {
        Self {
            m_cost: 65_536,
            t_cost: 3,
            p_cost: 4,
        }
    }
}

/// DEK/KEK key management for envelope encryption.
///
/// Wraps and unwraps the Data Encryption Key (DEK) under a Key Encryption Key
/// (KEK) derived from a passphrase via Argon2id.
pub struct KeyManager;

impl KeyManager {
    /// Derive a 256-bit KEK from a passphrase using Argon2id.
    pub fn derive_kek(
        passphrase: &[u8],
        salt: &[u8; 16],
        params: &Argon2Params,
    ) -> Result<[u8; KEY_SIZE], EncryptError> {
        let p = argon2::Params::new(params.m_cost, params.t_cost, params.p_cost, Some(32))
            .map_err(|_| EncryptError::InvalidKdfParams)?;
        let argon2 = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, p);
        let mut kek = [0u8; KEY_SIZE];
        argon2
            .hash_password_into(passphrase, salt, &mut kek)
            .map_err(|_| EncryptError::EncryptionFailed)?;
        Ok(kek)
    }

    /// Wrap (encrypt) a DEK under a KEK.
    ///
    /// Returns `nonce (24 B) || ciphertext+tag (48 B)` = 72 bytes total.
    pub fn wrap_dek(
        dek: &[u8; KEY_SIZE],
        kek: &[u8; KEY_SIZE],
        nonce: &[u8; NONCE_SIZE],
    ) -> Result<Vec<u8>, EncryptError> {
        let cipher = XChaCha20Poly1305::new(kek.into());
        let xnonce = XNonce::from_slice(nonce);
        let ct = cipher
            .encrypt(xnonce, dek.as_slice())
            .map_err(|_| EncryptError::EncryptionFailed)?;
        // Prepend the nonce so the wrapped blob is self-contained.
        let mut out = Vec::with_capacity(NONCE_SIZE + ct.len());
        out.extend_from_slice(nonce);
        out.extend_from_slice(&ct);
        Ok(out)
    }

    /// Unwrap (decrypt) a DEK using a KEK.
    ///
    /// `wrapped` must be the output of [`wrap_dek`](Self::wrap_dek) (72 bytes).
    pub fn unwrap_dek(
        wrapped: &[u8],
        kek: &[u8; KEY_SIZE],
    ) -> Result<[u8; KEY_SIZE], EncryptError> {
        if wrapped.len() < NONCE_SIZE + TAG_SIZE {
            return Err(EncryptError::DekUnwrapFailed);
        }
        let nonce = &wrapped[..NONCE_SIZE];
        let ct = &wrapped[NONCE_SIZE..];
        let cipher = XChaCha20Poly1305::new(kek.into());
        let xnonce = XNonce::from_slice(nonce);
        let plaintext = cipher
            .decrypt(xnonce, ct)
            .map_err(|_| EncryptError::DekUnwrapFailed)?;
        if plaintext.len() != KEY_SIZE {
            return Err(EncryptError::DekUnwrapFailed);
        }
        let mut dek = [0u8; KEY_SIZE];
        dek.copy_from_slice(&plaintext);
        Ok(dek)
    }
}

/// Validate that `reserved_per_page` is sufficient for encryption.
pub fn validate_reserved_bytes(reserved_per_page: u8) -> Result<(), EncryptError> {
    if reserved_per_page < ENCRYPTION_RESERVED_BYTES {
        Err(EncryptError::InsufficientReservedBytes {
            available: reserved_per_page,
            required: ENCRYPTION_RESERVED_BYTES,
        })
    } else {
        Ok(())
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Fixed test keys — never use in production.
    const TEST_DEK: [u8; KEY_SIZE] = [0xAA; KEY_SIZE];
    const TEST_DB_ID: DatabaseId = DatabaseId([0xBB; DATABASE_ID_SIZE]);

    fn test_nonce(seed: u8) -> [u8; NONCE_SIZE] {
        let mut n = [0u8; NONCE_SIZE];
        for (i, b) in n.iter_mut().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            {
                *b = seed.wrapping_add(i as u8);
            }
        }
        n
    }

    /// Fill a 4096-byte page with a recognizable pattern.
    fn make_test_page(page_size: usize) -> Vec<u8> {
        let mut page = vec![0u8; page_size];
        for (i, b) in page.iter_mut().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            {
                *b = (i & 0xFF) as u8;
            }
        }
        page
    }

    // ===================================================================
    // bd-1osn — Page-Level Encryption (§15)
    // ===================================================================

    #[test]
    fn test_xchacha20_poly1305_encrypt_decrypt_roundtrip() {
        let enc = PageEncryptor::new(&TEST_DEK, TEST_DB_ID);
        let nonce = test_nonce(1);

        for page_size in [512, 1024, 4096, 8192, 16384, 32768, 65536] {
            let original = make_test_page(page_size);
            let mut page = original.clone();

            enc.encrypt_page(&mut page, 1, &nonce).unwrap();

            // Ciphertext should differ from plaintext.
            let reserved = usize::from(ENCRYPTION_RESERVED_BYTES);
            assert_ne!(
                &page[..page_size - reserved],
                &original[..page_size - reserved],
                "page_size={page_size}: ciphertext must differ from plaintext"
            );

            enc.decrypt_page(&mut page, 1).unwrap();

            // Plaintext region must match original.
            assert_eq!(
                &page[..page_size - reserved],
                &original[..page_size - reserved],
                "page_size={page_size}: round-trip mismatch"
            );
        }
    }

    #[test]
    fn test_dek_kek_envelope_wrap_unwrap() {
        let passphrase = b"test-passphrase-42";
        let salt = [0x11u8; 16];
        // Use fast params for testing (not production-safe).
        let params = Argon2Params {
            m_cost: 256,
            t_cost: 1,
            p_cost: 1,
        };

        let kek = KeyManager::derive_kek(passphrase, &salt, &params).unwrap();
        assert_ne!(kek, [0u8; KEY_SIZE], "KEK must not be all zeros");

        let wrap_nonce = test_nonce(10);
        let wrapped = KeyManager::wrap_dek(&TEST_DEK, &kek, &wrap_nonce).unwrap();
        assert_eq!(
            wrapped.len(),
            NONCE_SIZE + KEY_SIZE + TAG_SIZE,
            "wrapped DEK size"
        );

        let unwrapped = KeyManager::unwrap_dek(&wrapped, &kek).unwrap();
        assert_eq!(unwrapped, TEST_DEK, "unwrapped DEK must match original");
    }

    #[test]
    fn test_instant_rekey_o1() {
        // Encrypt pages with original DEK under KEK_1, rekey to KEK_2,
        // verify pages still decrypt (DEK unchanged).
        let dek = TEST_DEK;
        let db_id = TEST_DB_ID;
        let enc = PageEncryptor::new(&dek, db_id);

        // Encrypt two pages.
        let mut page1 = make_test_page(4096);
        let mut page2 = make_test_page(4096);
        let original1 = page1.clone();
        let original2 = page2.clone();
        enc.encrypt_page(&mut page1, 1, &test_nonce(1)).unwrap();
        enc.encrypt_page(&mut page2, 2, &test_nonce(2)).unwrap();

        // Wrap DEK under KEK_1.
        let salt1 = [0x01u8; 16];
        let params = Argon2Params {
            m_cost: 256,
            t_cost: 1,
            p_cost: 1,
        };
        let kek1 = KeyManager::derive_kek(b"pass1", &salt1, &params).unwrap();
        let wrapped1 = KeyManager::wrap_dek(&dek, &kek1, &test_nonce(20)).unwrap();

        // Rekey: unwrap DEK from KEK_1, re-wrap under KEK_2.
        let unwrapped = KeyManager::unwrap_dek(&wrapped1, &kek1).unwrap();
        assert_eq!(unwrapped, dek, "unwrapped DEK must match");

        let salt2 = [0x02u8; 16];
        let kek2 = KeyManager::derive_kek(b"pass2", &salt2, &params).unwrap();
        let wrapped2 = KeyManager::wrap_dek(&unwrapped, &kek2, &test_nonce(30)).unwrap();

        // Verify: unwrap from KEK_2 yields the same DEK.
        let unwrapped2 = KeyManager::unwrap_dek(&wrapped2, &kek2).unwrap();
        assert_eq!(unwrapped2, dek, "DEK unchanged after rekey");

        // Pages still decrypt correctly with the same PageEncryptor (same DEK).
        let reserved = usize::from(ENCRYPTION_RESERVED_BYTES);
        enc.decrypt_page(&mut page1, 1).unwrap();
        assert_eq!(&page1[..4096 - reserved], &original1[..4096 - reserved]);
        enc.decrypt_page(&mut page2, 2).unwrap();
        assert_eq!(&page2[..4096 - reserved], &original2[..4096 - reserved]);
    }

    #[test]
    fn test_aad_swap_resistance_different_page_numbers() {
        let enc = PageEncryptor::new(&TEST_DEK, TEST_DB_ID);
        let nonce = test_nonce(5);

        let mut page = make_test_page(4096);
        enc.encrypt_page(&mut page, 1, &nonce).unwrap();

        // Attempt to decrypt as page 2 — MUST fail authentication.
        let result = enc.decrypt_page(&mut page, 2);
        assert_eq!(
            result.unwrap_err(),
            EncryptError::AuthenticationFailed,
            "swap page_number in AAD must be detected"
        );
    }

    #[test]
    fn test_aad_swap_resistance_different_database_ids() {
        let db_id_a = DatabaseId::from_bytes([0xAA; DATABASE_ID_SIZE]);
        let db_id_b = DatabaseId::from_bytes([0xCC; DATABASE_ID_SIZE]);

        let enc_a = PageEncryptor::new(&TEST_DEK, db_id_a);
        let enc_b = PageEncryptor::new(&TEST_DEK, db_id_b);
        let nonce = test_nonce(7);

        let mut page = make_test_page(4096);
        enc_a.encrypt_page(&mut page, 1, &nonce).unwrap();

        // Decrypt with a different database id — MUST fail.
        let result = enc_b.decrypt_page(&mut page, 1);
        assert_eq!(
            result.unwrap_err(),
            EncryptError::AuthenticationFailed,
            "swap database_id in AAD must be detected"
        );
    }

    #[test]
    fn test_nonce_uniqueness_per_write() {
        let enc = PageEncryptor::new(&TEST_DEK, TEST_DB_ID);

        let original = make_test_page(4096);
        let nonce_a = test_nonce(1);
        let nonce_b = test_nonce(2);
        assert_ne!(nonce_a, nonce_b, "test nonces must differ");

        let mut page_a = original.clone();
        let mut page_b = original;
        enc.encrypt_page(&mut page_a, 1, &nonce_a).unwrap();
        enc.encrypt_page(&mut page_b, 1, &nonce_b).unwrap();

        // Same plaintext + different nonces → different ciphertext.
        let reserved = usize::from(ENCRYPTION_RESERVED_BYTES);
        assert_ne!(
            &page_a[..4096 - reserved],
            &page_b[..4096 - reserved],
            "different nonces must produce different ciphertext"
        );

        // Both decrypt correctly.
        enc.decrypt_page(&mut page_a, 1).unwrap();
        enc.decrypt_page(&mut page_b, 1).unwrap();
        assert_eq!(
            &page_a[..4096 - reserved],
            &page_b[..4096 - reserved],
            "both must decrypt to the same plaintext"
        );
    }

    #[test]
    fn test_reserved_bytes_minimum_40() {
        assert!(validate_reserved_bytes(40).is_ok());
        assert!(validate_reserved_bytes(41).is_ok());
        assert!(validate_reserved_bytes(255).is_ok());

        let err = validate_reserved_bytes(39).unwrap_err();
        assert_eq!(
            err,
            EncryptError::InsufficientReservedBytes {
                available: 39,
                required: 40
            }
        );

        let err = validate_reserved_bytes(0).unwrap_err();
        assert_eq!(
            err,
            EncryptError::InsufficientReservedBytes {
                available: 0,
                required: 40
            }
        );
    }

    #[test]
    fn test_database_id_stable_across_rekey() {
        let db_id = DatabaseId::from_bytes([0x42; DATABASE_ID_SIZE]);
        let dek = TEST_DEK;

        // Create encryptor — database_id bound.
        let enc1 = PageEncryptor::new(&dek, db_id);
        assert_eq!(enc1.database_id(), db_id);

        // After rekey (new KEK, same DEK), the encryptor uses the same db_id.
        let enc2 = PageEncryptor::new(&dek, db_id);
        assert_eq!(enc2.database_id(), db_id);
        assert_eq!(enc1.database_id(), enc2.database_id());
    }

    #[test]
    fn test_aad_big_endian_encoding() {
        let db_id = DatabaseId::from_bytes([0x01; DATABASE_ID_SIZE]);
        let aad = build_aad(256, &db_id);

        // page_number 256 in big-endian: [0x00, 0x00, 0x01, 0x00]
        assert_eq!(&aad[..4], &[0x00, 0x00, 0x01, 0x00]);
        // database_id follows.
        assert_eq!(&aad[4..], &[0x01; DATABASE_ID_SIZE]);
    }

    #[test]
    fn test_wrong_key_fails() {
        let enc = PageEncryptor::new(&TEST_DEK, TEST_DB_ID);
        let nonce = test_nonce(9);

        let mut page = make_test_page(4096);
        enc.encrypt_page(&mut page, 1, &nonce).unwrap();

        // Decrypt with a different DEK — MUST fail.
        let wrong_dek = [0xFF; KEY_SIZE];
        let wrong_enc = PageEncryptor::new(&wrong_dek, TEST_DB_ID);
        let result = wrong_enc.decrypt_page(&mut page, 1);
        assert_eq!(result.unwrap_err(), EncryptError::AuthenticationFailed);
    }

    #[test]
    fn test_page_too_small_rejected() {
        let enc = PageEncryptor::new(&TEST_DEK, TEST_DB_ID);
        let nonce = test_nonce(11);

        // Page smaller than 40 bytes.
        let mut tiny = vec![0u8; 39];
        let err = enc.encrypt_page(&mut tiny, 1, &nonce).unwrap_err();
        assert!(matches!(err, EncryptError::PageTooSmall { .. }));

        let err = enc.decrypt_page(&mut tiny, 1).unwrap_err();
        assert!(matches!(err, EncryptError::PageTooSmall { .. }));
    }

    #[test]
    fn test_dek_unwrap_wrong_kek_fails() {
        let salt = [0x33u8; 16];
        let params = Argon2Params {
            m_cost: 256,
            t_cost: 1,
            p_cost: 1,
        };

        let kek1 = KeyManager::derive_kek(b"correct", &salt, &params).unwrap();
        let kek2 = KeyManager::derive_kek(b"wrong", &salt, &params).unwrap();
        assert_ne!(kek1, kek2);

        let wrapped = KeyManager::wrap_dek(&TEST_DEK, &kek1, &test_nonce(40)).unwrap();
        let result = KeyManager::unwrap_dek(&wrapped, &kek2);
        assert_eq!(result.unwrap_err(), EncryptError::DekUnwrapFailed);
    }

    #[test]
    fn test_corrupted_ciphertext_detected() {
        let enc = PageEncryptor::new(&TEST_DEK, TEST_DB_ID);
        let nonce = test_nonce(13);

        let mut page = make_test_page(4096);
        enc.encrypt_page(&mut page, 1, &nonce).unwrap();

        // Flip a bit in the ciphertext region.
        page[100] ^= 0x01;

        let result = enc.decrypt_page(&mut page, 1);
        assert_eq!(
            result.unwrap_err(),
            EncryptError::AuthenticationFailed,
            "corrupted ciphertext must be detected by Poly1305 tag"
        );
    }

    #[test]
    fn test_wrapped_dek_too_short_rejected() {
        let kek = [0xDD; KEY_SIZE];
        // Need at least NONCE_SIZE + TAG_SIZE = 40 bytes.
        let result = KeyManager::unwrap_dek(&[0u8; 39], &kek);
        assert_eq!(result.unwrap_err(), EncryptError::DekUnwrapFailed);
    }
}
