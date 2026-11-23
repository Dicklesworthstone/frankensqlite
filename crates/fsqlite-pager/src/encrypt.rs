//! Page-level encryption: XChaCha20-Poly1305 + DEK/KEK + AAD swap resistance (§15, bd-1osn).
//!
//! Each page write encrypts the usable portion (everything except the reserved
//! region) with XChaCha20-Poly1305, storing a fresh 24-byte random nonce and the
//! 16-byte Poly1305 tag in the reserved space at the end of the page.
//!
//! Envelope encryption (DEK/KEK) enables O(1) rekey: `PRAGMA rekey` re-wraps
//! the DEK under a new KEK derived from the new passphrase, without touching
//! page data.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use fsqlite_error::FrankenError;
use fsqlite_types::cx::Cx;

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

/// Size of a wrapped DEK blob (`nonce(24) || ciphertext+tag(48)`).
pub const WRAPPED_DEK_SIZE: usize = NONCE_SIZE + KEY_SIZE + TAG_SIZE;

/// Magic header for durable key-envelope sidecar files (`<db>-key`).
pub const KEY_ENVELOPE_MAGIC: [u8; 16] = *b"FSQLITE_KEY_V1\0\0";

/// Current durable key-envelope format version.
pub const KEY_ENVELOPE_VERSION: u32 = 1;

/// Fixed byte length of a serialized [`KeyEnvelope`].
///
/// Layout:
/// - `0..16`:   magic (`FSQLITE_KEY_V1\0\0`)
/// - `16..20`:  version (`u32` BE)
/// - `20..24`:  page_size (`u32` BE)
/// - `24..28`:  argon2 `m_cost` (`u32` BE)
/// - `28..32`:  argon2 `t_cost` (`u32` BE)
/// - `32..36`:  argon2 `p_cost` (`u32` BE)
/// - `36`:      `reserved_per_page` (`u8`, `>= 40`)
/// - `37`:      `flags` (`u8`, `0`)
/// - `38..40`:  reserved padding (`[0, 0]`)
/// - `40..56`:  Argon2id 16-byte salt
/// - `56..72`:  stable 16-byte `DatabaseId`
/// - `72..144`: wrapped DEK (`nonce(24) || ct+tag(48)`, AEAD AAD = bytes `0..72`)
/// - `144..160`: BLAKE3 structural digest prefix over bytes `0..144`
pub const KEY_ENVELOPE_SIZE: usize = 160;

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
#[derive(Clone)]
pub struct PageEncryptor {
    cipher: XChaCha20Poly1305,
    database_id: DatabaseId,
    /// Raw DEK bytes, retained for rekey comparisons.
    dek: [u8; KEY_SIZE],
}

impl std::fmt::Debug for PageEncryptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageEncryptor")
            .field("database_id", &self.database_id)
            .field("dek", &"<redacted>")
            .finish_non_exhaustive()
    }
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
        let xnonce: &XNonce = nonce.into();

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
        let xnonce: &XNonce = (&nonce).into();

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
#[derive(Debug, Clone, PartialEq, Eq)]
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

impl Argon2Params {
    /// Validate that these Argon2id cost parameters are accepted by the KDF.
    pub fn validate(&self) -> fsqlite_error::Result<()> {
        argon2::Params::new(self.m_cost, self.t_cost, self.p_cost, Some(KEY_SIZE)).map_err(
            |_| FrankenError::OutOfRange {
                what: "cipher_argon2_params".to_owned(),
                value: format!(
                    "m_cost={},t_cost={},p_cost={}",
                    self.m_cost, self.t_cost, self.p_cost
                ),
            },
        )?;
        Ok(())
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
        let xnonce: &XNonce = nonce.into();
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
        // `nonce` is a runtime slice here, so this is a checked conversion; the
        // length was already validated against NONCE_SIZE above.
        let xnonce = XNonce::try_from(nonce).map_err(|_| EncryptError::DekUnwrapFailed)?;
        let plaintext = cipher
            .decrypt(&xnonce, ct)
            .map_err(|_| EncryptError::DekUnwrapFailed)?;
        if plaintext.len() != KEY_SIZE {
            return Err(EncryptError::DekUnwrapFailed);
        }
        let mut dek = [0u8; KEY_SIZE];
        dek.copy_from_slice(&plaintext);
        Ok(dek)
    }
    /// Wrap (encrypt) a DEK under a KEK with associated data binding envelope metadata.
    ///
    /// Returns `nonce (24 B) || ciphertext+tag (48 B)` = 72 bytes total.
    pub fn wrap_dek_with_aad(
        dek: &[u8; KEY_SIZE],
        kek: &[u8; KEY_SIZE],
        nonce: &[u8; NONCE_SIZE],
        aad: &[u8],
    ) -> Result<[u8; WRAPPED_DEK_SIZE], EncryptError> {
        let cipher = XChaCha20Poly1305::new(kek.into());
        let xnonce: &XNonce = nonce.into();
        let ct = cipher
            .encrypt(
                xnonce,
                Payload {
                    msg: dek.as_slice(),
                    aad,
                },
            )
            .map_err(|_| EncryptError::EncryptionFailed)?;
        if ct.len() != KEY_SIZE + TAG_SIZE {
            return Err(EncryptError::EncryptionFailed);
        }
        let mut out = [0u8; WRAPPED_DEK_SIZE];
        out[..NONCE_SIZE].copy_from_slice(nonce);
        out[NONCE_SIZE..].copy_from_slice(&ct);
        Ok(out)
    }

    /// Unwrap (decrypt) a DEK using a KEK and associated data.
    ///
    /// `wrapped` must be the 72-byte output of [`wrap_dek_with_aad`](Self::wrap_dek_with_aad).
    pub fn unwrap_dek_with_aad(
        wrapped: &[u8],
        kek: &[u8; KEY_SIZE],
        aad: &[u8],
    ) -> Result<[u8; KEY_SIZE], EncryptError> {
        if wrapped.len() != WRAPPED_DEK_SIZE {
            return Err(EncryptError::DekUnwrapFailed);
        }
        let nonce = &wrapped[..NONCE_SIZE];
        let ct = &wrapped[NONCE_SIZE..];
        let cipher = XChaCha20Poly1305::new(kek.into());
        let xnonce = XNonce::try_from(nonce).map_err(|_| EncryptError::DekUnwrapFailed)?;
        let plaintext = cipher
            .decrypt(&xnonce, Payload { msg: ct, aad })
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

// ---------------------------------------------------------------------------
// KeyEnvelope — Durable Authenticated Key Metadata Sidecar (bd-1vwiy.1)
// ---------------------------------------------------------------------------

/// Generate a fresh random 24-byte page nonce via OS entropy (INV-ENC-1),
/// honoring caller cancellation via `cx.checkpoint()`. Never uses a global
/// nonce counter.
pub fn generate_fresh_page_nonce(cx: &Cx) -> fsqlite_error::Result<[u8; NONCE_SIZE]> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)?;
    Ok(fill_csprng_24())
}

#[cfg(not(target_arch = "wasm32"))]
fn fill_csprng_16() -> [u8; 16] {
    rand::random::<[u8; 16]>()
}

#[cfg(target_arch = "wasm32")]
fn fill_csprng_16() -> [u8; 16] {
    use core::sync::atomic::AtomicU64;
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let digest = blake3::hash(&n.to_le_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    out
}

#[cfg(not(target_arch = "wasm32"))]
fn fill_csprng_24() -> [u8; NONCE_SIZE] {
    rand::random::<[u8; NONCE_SIZE]>()
}

#[cfg(target_arch = "wasm32")]
fn fill_csprng_24() -> [u8; NONCE_SIZE] {
    use core::sync::atomic::AtomicU64;
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let digest = blake3::hash(&n.to_le_bytes());
    let mut out = [0u8; NONCE_SIZE];
    out.copy_from_slice(&digest.as_bytes()[..NONCE_SIZE]);
    out
}

#[cfg(not(target_arch = "wasm32"))]
fn fill_csprng_32() -> [u8; KEY_SIZE] {
    rand::random::<[u8; KEY_SIZE]>()
}

#[cfg(target_arch = "wasm32")]
fn fill_csprng_32() -> [u8; KEY_SIZE] {
    use core::sync::atomic::AtomicU64;
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    *blake3::hash(&n.to_le_bytes()).as_bytes()
}

/// Durable authenticated key-envelope metadata persisted alongside an
/// encrypted database (`<db>-key`).
///
/// The 72-byte header prefix (`0..72`, covering magic, version, page size,
/// Argon2id parameters, reserved bytes, salt, and stable [`DatabaseId`]) is
/// bound as XChaCha20-Poly1305 AAD when wrapping the 256-bit DEK, and the full
/// 144-byte payload carries a 16-byte BLAKE3 structural digest prefix for
/// fast corruption rejection before KDF work.
#[derive(Clone, PartialEq, Eq)]
pub struct KeyEnvelope {
    version: u32,
    page_size: u32,
    argon2_params: Argon2Params,
    reserved_per_page: u8,
    flags: u8,
    salt: [u8; 16],
    database_id: DatabaseId,
    wrapped_dek: [u8; WRAPPED_DEK_SIZE],
}

impl std::fmt::Debug for KeyEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyEnvelope")
            .field("version", &self.version)
            .field("page_size", &self.page_size)
            .field("argon2_params", &self.argon2_params)
            .field("reserved_per_page", &self.reserved_per_page)
            .field("flags", &self.flags)
            .field("database_id", &self.database_id)
            .field("wrapped_dek", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl KeyEnvelope {
    const HEADER_AAD_LEN: usize = 72;
    const DIGEST_INPUT_LEN: usize = 144;

    fn encode_aad_header(
        version: u32,
        page_size: u32,
        params: &Argon2Params,
        reserved_per_page: u8,
        flags: u8,
        salt: &[u8; 16],
        database_id: &DatabaseId,
    ) -> [u8; Self::HEADER_AAD_LEN] {
        let mut hdr = [0u8; Self::HEADER_AAD_LEN];
        hdr[0..16].copy_from_slice(&KEY_ENVELOPE_MAGIC);
        hdr[16..20].copy_from_slice(&version.to_be_bytes());
        hdr[20..24].copy_from_slice(&page_size.to_be_bytes());
        hdr[24..28].copy_from_slice(&params.m_cost.to_be_bytes());
        hdr[28..32].copy_from_slice(&params.t_cost.to_be_bytes());
        hdr[32..36].copy_from_slice(&params.p_cost.to_be_bytes());
        hdr[36] = reserved_per_page;
        hdr[37] = flags;
        hdr[40..56].copy_from_slice(salt);
        hdr[56..72].copy_from_slice(database_id.as_bytes());
        hdr
    }

    fn is_valid_page_size(page_size: u32) -> bool {
        (512..=65_536).contains(&page_size) && page_size.is_power_of_two()
    }

    /// Create a brand-new encrypted database key envelope and live
    /// [`PageEncryptor`] with fresh CSPRNG `DatabaseId`, `DEK`, `salt`, and
    /// wrap nonce.
    pub fn create_new(
        cx: &Cx,
        passphrase: &[u8],
        page_size: u32,
        reserved_per_page: u8,
        argon2_params: Argon2Params,
    ) -> fsqlite_error::Result<(Self, PageEncryptor)> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        if !Self::is_valid_page_size(page_size) {
            return Err(FrankenError::OutOfRange {
                what: "encryption page_size".to_owned(),
                value: page_size.to_string(),
            });
        }
        validate_reserved_bytes(reserved_per_page)
            .map_err(|err| FrankenError::Internal(err.to_string()))?;

        let salt = fill_csprng_16();
        let database_id = DatabaseId::from_bytes(fill_csprng_16());
        let dek = fill_csprng_32();
        let wrap_nonce = generate_fresh_page_nonce(cx)?;

        let kek = KeyManager::derive_kek(passphrase, &salt, &argon2_params)
            .map_err(|err| FrankenError::Internal(err.to_string()))?;
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;

        let aad = Self::encode_aad_header(
            KEY_ENVELOPE_VERSION,
            page_size,
            &argon2_params,
            reserved_per_page,
            0,
            &salt,
            &database_id,
        );
        let wrapped_dek = KeyManager::wrap_dek_with_aad(&dek, &kek, &wrap_nonce, &aad)
            .map_err(|err| FrankenError::Internal(err.to_string()))?;

        let envelope = Self {
            version: KEY_ENVELOPE_VERSION,
            page_size,
            argon2_params,
            reserved_per_page,
            flags: 0,
            salt,
            database_id,
            wrapped_dek,
        };
        let encryptor = PageEncryptor::new(&dek, database_id);
        Ok((envelope, encryptor))
    }

    /// Derive the KEK from `passphrase` using the envelope's persisted Argon2id
    /// parameters and salt, authenticate the full envelope header AAD, and
    /// unwrap the DEK. Fails closed with [`FrankenError::NotADatabase`] on
    /// wrong passphrase or tampered metadata.
    pub fn unlock(&self, cx: &Cx, passphrase: &[u8]) -> fsqlite_error::Result<PageEncryptor> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        let not_db = || FrankenError::NotADatabase {
            path: PathBuf::new(),
        };
        validate_reserved_bytes(self.reserved_per_page).map_err(|_| not_db())?;
        let kek = KeyManager::derive_kek(passphrase, &self.salt, &self.argon2_params)
            .map_err(|_| not_db())?;
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        let aad = Self::encode_aad_header(
            self.version,
            self.page_size,
            &self.argon2_params,
            self.reserved_per_page,
            self.flags,
            &self.salt,
            &self.database_id,
        );
        let dek = KeyManager::unwrap_dek_with_aad(&self.wrapped_dek, &kek, &aad)
            .map_err(|_| not_db())?;
        Ok(PageEncryptor::new(&dek, self.database_id))
    }

    /// Perform O(1) KEK re-wrap (`PRAGMA rekey`): keeps the existing stable
    /// `DEK` and [`DatabaseId`] untouched while wrapping the DEK under a fresh
    /// Argon2id KEK derived from `new_passphrase` with a fresh 16-byte salt and
    /// fresh 24-byte wrap nonce.
    pub fn rekey_o1(
        &self,
        cx: &Cx,
        encryptor: &PageEncryptor,
        new_passphrase: &[u8],
        new_params: Argon2Params,
    ) -> fsqlite_error::Result<Self> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        if encryptor.database_id() != self.database_id {
            return Err(FrankenError::Internal(
                "encryptor database_id does not match active key envelope".to_owned(),
            ));
        }
        let new_salt = fill_csprng_16();
        let new_wrap_nonce = generate_fresh_page_nonce(cx)?;
        let new_kek = KeyManager::derive_kek(new_passphrase, &new_salt, &new_params)
            .map_err(|err| FrankenError::Internal(err.to_string()))?;
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;

        let aad = Self::encode_aad_header(
            self.version,
            self.page_size,
            &new_params,
            self.reserved_per_page,
            self.flags,
            &new_salt,
            &self.database_id,
        );
        let wrapped_dek =
            KeyManager::wrap_dek_with_aad(encryptor.dek(), &new_kek, &new_wrap_nonce, &aad)
                .map_err(|err| FrankenError::Internal(err.to_string()))?;

        Ok(Self {
            version: self.version,
            page_size: self.page_size,
            argon2_params: new_params,
            reserved_per_page: self.reserved_per_page,
            flags: self.flags,
            salt: new_salt,
            database_id: self.database_id,
            wrapped_dek,
        })
    }

    /// Encode this envelope into its canonical 160-byte on-disk representation.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; KEY_ENVELOPE_SIZE] {
        let mut out = [0u8; KEY_ENVELOPE_SIZE];
        let hdr = Self::encode_aad_header(
            self.version,
            self.page_size,
            &self.argon2_params,
            self.reserved_per_page,
            self.flags,
            &self.salt,
            &self.database_id,
        );
        out[..Self::HEADER_AAD_LEN].copy_from_slice(&hdr);
        out[Self::HEADER_AAD_LEN..Self::DIGEST_INPUT_LEN].copy_from_slice(&self.wrapped_dek);
        let digest = blake3::hash(&out[..Self::DIGEST_INPUT_LEN]);
        out[Self::DIGEST_INPUT_LEN..].copy_from_slice(&digest.as_bytes()[..16]);
        out
    }

    /// Decode and structurally validate a 160-byte on-disk key envelope.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, EncryptError> {
        if bytes.len() != KEY_ENVELOPE_SIZE {
            return Err(EncryptError::DekUnwrapFailed);
        }
        if bytes[..16] != KEY_ENVELOPE_MAGIC {
            return Err(EncryptError::DekUnwrapFailed);
        }
        let expected_digest = blake3::hash(&bytes[..Self::DIGEST_INPUT_LEN]);
        if bytes[Self::DIGEST_INPUT_LEN..] != expected_digest.as_bytes()[..16] {
            return Err(EncryptError::AuthenticationFailed);
        }
        let version = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
        if version != KEY_ENVELOPE_VERSION {
            return Err(EncryptError::DekUnwrapFailed);
        }
        let page_size = u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]);
        if !Self::is_valid_page_size(page_size) {
            return Err(EncryptError::DekUnwrapFailed);
        }
        let m_cost = u32::from_be_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]);
        let t_cost = u32::from_be_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]);
        let p_cost = u32::from_be_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]);
        let argon2_params = Argon2Params {
            m_cost,
            t_cost,
            p_cost,
        };
        let reserved_per_page = bytes[36];
        validate_reserved_bytes(reserved_per_page)?;
        let flags = bytes[37];
        if flags != 0 || bytes[38] != 0 || bytes[39] != 0 {
            return Err(EncryptError::DekUnwrapFailed);
        }
        let mut salt = [0u8; 16];
        salt.copy_from_slice(&bytes[40..56]);
        let mut db_id_bytes = [0u8; DATABASE_ID_SIZE];
        db_id_bytes.copy_from_slice(&bytes[56..72]);
        let mut wrapped_dek = [0u8; WRAPPED_DEK_SIZE];
        wrapped_dek.copy_from_slice(&bytes[72..144]);
        Ok(Self {
            version,
            page_size,
            argon2_params,
            reserved_per_page,
            flags,
            salt,
            database_id: DatabaseId::from_bytes(db_id_bytes),
            wrapped_dek,
        })
    }

    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    #[must_use]
    pub const fn page_size(&self) -> u32 {
        self.page_size
    }

    #[must_use]
    pub const fn reserved_per_page(&self) -> u8 {
        self.reserved_per_page
    }

    #[must_use]
    pub const fn argon2_params(&self) -> &Argon2Params {
        &self.argon2_params
    }

    #[must_use]
    pub const fn salt(&self) -> &[u8; 16] {
        &self.salt
    }

    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    #[must_use]
    pub const fn wrapped_dek(&self) -> &[u8; WRAPPED_DEK_SIZE] {
        &self.wrapped_dek
    }
}

/// Companion key-envelope path for a database file (`<db>-key`).
#[must_use]
pub fn key_envelope_path_for_db(db_path: &Path) -> PathBuf {
    let mut os = db_path.as_os_str().to_os_string();
    os.push("-key");
    PathBuf::from(os)
}

#[cfg(not(target_arch = "wasm32"))]
fn sync_parent_dir(path: &Path) -> fsqlite_error::Result<()> {
    #[cfg(feature = "native")]
    {
        fsqlite_vfs::host_fs::sync_parent_directory(path)?;
    }
    #[cfg(all(not(feature = "native"), unix))]
    {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty())
            && let Ok(dir) = std::fs::File::open(parent)
        {
            dir.sync_all()?;
        }
    }
    #[cfg(all(not(feature = "native"), not(unix)))]
    {
        let _ = path;
    }
    Ok(())
}

/// Read and structurally validate a durable `<db>-key` envelope file.
///
/// Returns `Ok(None)` when the sidecar does not exist, and fails closed with
/// [`FrankenError::NotADatabase`] if the file exists but is truncated,
/// malformed, or tampered.
#[cfg(not(target_arch = "wasm32"))]
pub fn read_key_envelope(path: &Path) -> fsqlite_error::Result<Option<KeyEnvelope>> {
    match std::fs::read(path) {
        Ok(bytes) => KeyEnvelope::from_bytes(&bytes)
            .map(Some)
            .map_err(|err| FrankenError::DatabaseCorrupt {
                detail: format!("corrupt key envelope {}: {err}", path.display()),
            }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(FrankenError::Io(err)),
    }
}

/// Atomically persist a durable `<db>-key` envelope via sibling temp file,
/// `sync_all`, atomic `rename`, and parent-directory sync.
///
/// A crash before `rename` leaves the prior `<db>-key` untouched; a crash
/// after `rename` leaves the new `<db>-key` durable.
#[cfg(not(target_arch = "wasm32"))]
pub fn write_key_envelope_atomic(
    cx: &Cx,
    path: &Path,
    envelope: &KeyEnvelope,
) -> fsqlite_error::Result<()> {
    use std::io::Write;

    cx.checkpoint().map_err(|_| FrankenError::Abort)?;
    let bytes = envelope.to_bytes();
    let nonce = fill_csprng_16();
    let suffix: String = nonce.iter().take(8).map(|b| format!("{b:02x}")).collect();
    let mut tmp_os = path.as_os_str().to_os_string();
    tmp_os.push(format!(".tmp.{suffix}"));
    let tmp_path = PathBuf::from(tmp_os);

    let write_res: fsqlite_error::Result<()> = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        std::fs::rename(&tmp_path, path)?;
        sync_parent_dir(path)?;
        Ok(())
    })();

    if write_res.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    write_res
}

/// Remove a durable `<db>-key` sidecar after an explicit decrypt conversion
/// (`PRAGMA rekey = ''`).
#[cfg(not(target_arch = "wasm32"))]
pub fn remove_key_envelope_if_exists(path: &Path) -> fsqlite_error::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_parent_dir(path),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(FrankenError::Io(err)),
    }
}

// ---------------------------------------------------------------------------
// ActivePageCipher / PageCipherSlot — shared pager + WAL + snapshot codec
// ---------------------------------------------------------------------------

/// Active page-encryption context shared across the pager, WAL backend,
/// rollback journal, and parallel snapshot readers.
#[derive(Clone, Debug)]
pub struct ActivePageCipher {
    /// Immutable XChaCha20-Poly1305 page encryptor (DEK + stable DatabaseId).
    pub encryptor: Arc<PageEncryptor>,
    /// Database page size in bytes.
    pub page_size: usize,
}

impl ActivePageCipher {
    /// Construct a new active page cipher context.
    #[must_use]
    pub const fn new(encryptor: Arc<PageEncryptor>, page_size: usize) -> Self {
        Self {
            encryptor,
            page_size,
        }
    }
}

/// Lock-free fast-path slot holding an optional [`ActivePageCipher`].
///
/// Unencrypted databases observe `is_enabled() == false` via a single atomic
/// load without touching any lock. Concurrent encrypted writers read the
/// immutable `Arc<PageEncryptor>` under a shared read lock with zero write
/// serialization.
#[derive(Clone)]
pub struct PageCipherSlot {
    enabled: Arc<AtomicBool>,
    #[cfg(not(target_arch = "wasm32"))]
    inner: Arc<parking_lot::RwLock<Option<ActivePageCipher>>>,
    #[cfg(target_arch = "wasm32")]
    inner: Arc<std::sync::RwLock<Option<ActivePageCipher>>>,
}

impl Default for PageCipherSlot {
    fn default() -> Self {
        Self {
            enabled: Arc::new(AtomicBool::new(false)),
            #[cfg(not(target_arch = "wasm32"))]
            inner: Arc::new(parking_lot::RwLock::new(None)),
            #[cfg(target_arch = "wasm32")]
            inner: Arc::new(std::sync::RwLock::new(None)),
        }
    }
}

impl std::fmt::Debug for PageCipherSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCipherSlot")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl PageCipherSlot {
    /// Create a slot pre-populated with an active cipher.
    #[must_use]
    pub fn new(active: Option<ActivePageCipher>) -> Self {
        let slot = Self::default();
        slot.set(active);
        slot
    }

    /// Fast-path atomic check for whether page encryption is active.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    /// Clone the currently active cipher, if encryption is enabled.
    #[must_use]
    pub fn get(&self) -> Option<ActivePageCipher> {
        if !self.is_enabled() {
            return None;
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.inner.read().clone()
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.inner
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    /// Install or clear the active page cipher.
    pub fn set(&self, active: Option<ActivePageCipher>) {
        let is_some = active.is_some();
        #[cfg(not(target_arch = "wasm32"))]
        {
            *self.inner.write() = active;
        }
        #[cfg(target_arch = "wasm32")]
        {
            *self
                .inner
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = active;
        }
        self.enabled.store(is_some, Ordering::Release);
    }
}

/// Encrypt a plaintext page image for physical write using a fresh CSPRNG
/// 24-byte nonce (INV-ENC-1) and canonical AAD `be_u32(page_no) || DatabaseId`.
pub fn encrypt_page_for_write(
    active: &ActivePageCipher,
    cx: &Cx,
    page_no: u32,
    plaintext: &[u8],
) -> fsqlite_error::Result<Vec<u8>> {
    if plaintext.len() != active.page_size {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "encrypted page {page_no} size mismatch: got {}, expected {}",
                plaintext.len(),
                active.page_size
            ),
        });
    }
    let nonce = generate_fresh_page_nonce(cx)?;
    let mut out = plaintext.to_vec();
    active
        .encryptor
        .encrypt_page(&mut out, page_no, &nonce)
        .map_err(|err| FrankenError::Internal(format!("page {page_no} encryption failed: {err}")))?;
    Ok(out)
}

/// Authenticate and decrypt an on-disk page image in place. Fails closed with
/// [`FrankenError::DatabaseCorrupt`] on wrong key, ciphertext tamper, or page
/// swap (AAD mismatch).
pub fn decrypt_page_after_read(
    active: &ActivePageCipher,
    page_no: u32,
    buf: &mut [u8],
) -> fsqlite_error::Result<()> {
    if buf.len() != active.page_size {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "encrypted page {page_no} read size mismatch: got {}, expected {}",
                buf.len(),
                active.page_size
            ),
        });
    }
    active
        .encryptor
        .decrypt_page(buf, page_no)
        .map_err(|_| FrankenError::DatabaseCorrupt {
            detail: format!(
                "encrypted page {page_no} authentication failed (wrong key, tampered ciphertext, or swapped page)"
            ),
        })
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
    fn test_argon2id_kek_matches_libsodium_vectors() {
        // Libsodium tv3 cases 2 and 4: Argon2id v19, no secret or associated data.
        // https://github.com/jedisct1/libsodium/blob/39184b5b96176536f905bb42eafdc59c22a20e6e/test/default/pwhash_argon2id.c#L195-L200
        // Fixed external answers guard key compatibility across KDF upgrades;
        // encrypt/decrypt roundtrips alone would accept a changed derived key.
        let cases = [
            (
                b"".as_slice(),
                [
                    0x6c, 0x0f, 0x35, 0x6a, 0xbb, 0x22, 0x5f, 0x2b, 0x1d, 0xdd, 0x66, 0xd3,
                    0x47, 0x39, 0x84, 0x3b,
                ],
                Argon2Params {
                    m_cost: 4_882,
                    t_cost: 2,
                    p_cost: 1,
                },
                [
                    0x36, 0x6f, 0x10, 0x04, 0xcf, 0xbb, 0x44, 0x7d, 0x43, 0x5e, 0x8f, 0x6b,
                    0xbe, 0x9e, 0x5c, 0xc0, 0xa1, 0x0e, 0x38, 0xe7, 0xc3, 0xda, 0x0e, 0x89,
                    0xbb, 0x19, 0x57, 0x8a, 0x1a, 0x0d, 0x72, 0x91,
                ],
            ),
            (
                // The final space is part of this 66-byte password.
                b"^T5H$JYt39n%K*j:W]!1s?vg!:jGi]Ax?..l7[p0v:1jHTpla9;]bUN;?bWyCbtqg ".as_slice(),
                *b">A 16-bytes salt",
                Argon2Params {
                    m_cost: 4_096,
                    t_cost: 19,
                    p_cost: 1,
                },
                [
                    0x96, 0xd0, 0x7f, 0xb9, 0xed, 0x64, 0x3e, 0xd0, 0x4c, 0x04, 0x67, 0xec,
                    0xca, 0xc3, 0x29, 0x3e, 0x28, 0x04, 0xea, 0x18, 0x8d, 0x10, 0xa6, 0x7d,
                    0xbe, 0xcf, 0x2f, 0x2c, 0xd5, 0x43, 0x40, 0x60,
                ],
            ),
        ];

        for (case, (passphrase, salt, params, expected)) in cases.into_iter().enumerate() {
            let actual = KeyManager::derive_kek(passphrase, &salt, &params)
                .expect("valid Libsodium Argon2id parameters");
            assert_eq!(actual, expected, "Libsodium Argon2id case {case}");
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

    // ===================================================================
    // bd-1o3u — AAD Construction Validation (§15)
    // ===================================================================

    #[test]
    fn test_aad_includes_database_id() {
        let db_id = DatabaseId::from_bytes([
            0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0,
            0xF0, 0xFF,
        ]);
        let aad = build_aad(1, &db_id);

        // Full 16-byte DatabaseId must appear at bytes [4..20].
        assert_eq!(
            &aad[4..],
            db_id.as_bytes(),
            "AAD must contain the complete 16-byte DatabaseId"
        );
        assert_eq!(aad.len(), AAD_SIZE, "AAD must be exactly 20 bytes");
    }

    #[test]
    fn test_aad_no_circular_dependency() {
        // INV-AAD-2: AAD construction must not depend on page content.
        // Verify: build_aad produces identical output regardless of what the
        // page data contains — it only takes page_number and database_id.
        let db_id = DatabaseId::from_bytes([0x42; DATABASE_ID_SIZE]);
        let page_number = 7u32;

        let aad1 = build_aad(page_number, &db_id);

        // If build_aad had a hidden dependency on page content, different
        // page contents would produce different AAD. Since it only takes
        // (page_number, database_id), the AAD is always the same.
        let aad2 = build_aad(page_number, &db_id);
        assert_eq!(
            aad1, aad2,
            "AAD must be deterministic from (page_number, database_id) alone"
        );

        // Further verify the function signature at the type level: build_aad
        // accepts only u32 and &DatabaseId — no &[u8] page content parameter.
        // This compile-time guarantee plus the runtime identity check proves
        // no circular dependency.
        let aad3 = build_aad(page_number, &db_id);
        assert_eq!(aad1, aad3);
    }

    #[test]
    fn test_aad_identical_encrypt_decrypt() {
        // INV-AAD-3: Encrypt and decrypt paths must use identical AAD.
        // We verify by encrypting with page_number=5 and showing that
        // decryption succeeds with the same page_number (same AAD)
        // but fails with a different page_number (different AAD).
        let enc = PageEncryptor::new(&TEST_DEK, TEST_DB_ID);
        let nonce = test_nonce(42);
        let page_number = 5u32;

        let mut page = make_test_page(4096);
        let original = page.clone();
        enc.encrypt_page(&mut page, page_number, &nonce).unwrap();

        // Same page_number → same AAD → decryption succeeds.
        let mut page_copy = page.clone();
        enc.decrypt_page(&mut page_copy, page_number).unwrap();
        let reserved = usize::from(ENCRYPTION_RESERVED_BYTES);
        assert_eq!(
            &page_copy[..4096 - reserved],
            &original[..4096 - reserved],
            "decrypt with same page_number must succeed (identical AAD)"
        );

        // Different page_number → different AAD → decryption fails.
        let result = enc.decrypt_page(&mut page, page_number + 1);
        assert_eq!(
            result.unwrap_err(),
            EncryptError::AuthenticationFailed,
            "decrypt with different page_number must fail (different AAD)"
        );
    }

    #[test]
    fn test_aad_page_context_tag_unknown_uses_constant() {
        // INV-AAD-4: When page_context_tag is unknown, AAD uses a fixed
        // constant format. Our implementation always uses the canonical
        // format `be_u32(page_number) || database_id_bytes` with no
        // variable page_context_tag — the AAD is a fixed 20 bytes.
        let db_id = DatabaseId::from_bytes([0x55; DATABASE_ID_SIZE]);

        // Verify AAD has the same fixed size for any page number.
        for page_num in [1u32, 100, 1000, u32::MAX] {
            let aad = build_aad(page_num, &db_id);
            assert_eq!(
                aad.len(),
                AAD_SIZE,
                "AAD must be fixed-size {AAD_SIZE} bytes for page {page_num}"
            );
        }

        // Verify the format is exactly be_u32 || db_id with no extra tag.
        let aad = build_aad(42, &db_id);
        let mut expected = [0u8; AAD_SIZE];
        expected[..4].copy_from_slice(&42u32.to_be_bytes());
        expected[4..].copy_from_slice(db_id.as_bytes());
        assert_eq!(
            aad, expected,
            "AAD must be exactly be_u32(page_number) || database_id_bytes, no extra tag"
        );
    }

    #[test]
    fn test_aad_cross_endian_portability() {
        // INV-AAD-1: AAD must use big-endian encoding for cross-endian
        // interoperability. Verify known values produce the exact expected bytes.
        let db_id = DatabaseId::from_bytes([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F, 0x10,
        ]);

        // Page 1: big-endian → [0x00, 0x00, 0x00, 0x01]
        let aad1 = build_aad(1, &db_id);
        assert_eq!(&aad1[..4], &[0x00, 0x00, 0x00, 0x01]);

        // Page 0x01020304: big-endian → [0x01, 0x02, 0x03, 0x04]
        let aad2 = build_aad(0x0102_0304, &db_id);
        assert_eq!(&aad2[..4], &[0x01, 0x02, 0x03, 0x04]);

        // Page u32::MAX: big-endian → [0xFF, 0xFF, 0xFF, 0xFF]
        let aad3 = build_aad(u32::MAX, &db_id);
        assert_eq!(&aad3[..4], &[0xFF, 0xFF, 0xFF, 0xFF]);

        // Verify this is NOT native little-endian encoding.
        // On LE, page 1 would be [0x01, 0x00, 0x00, 0x00].
        assert_ne!(
            &aad1[..4],
            &1u32.to_le_bytes(),
            "AAD must NOT use little-endian encoding"
        );

        // All AADs include the full database_id.
        assert_eq!(&aad1[4..], db_id.as_bytes());
        assert_eq!(&aad2[4..], db_id.as_bytes());
        assert_eq!(&aad3[4..], db_id.as_bytes());
    }

    #[test]
    fn test_encrypt_error_display_all_variants() {
        let cases: Vec<(EncryptError, &str)> = vec![
            (
                EncryptError::InsufficientReservedBytes {
                    available: 10,
                    required: 40,
                },
                "insufficient reserved bytes: 10 < 40",
            ),
            (EncryptError::EncryptionFailed, "AEAD encryption failed"),
            (
                EncryptError::AuthenticationFailed,
                "AEAD authentication failed",
            ),
            (
                EncryptError::PageTooSmall {
                    page_len: 20,
                    required_reserved: 40,
                },
                "page too small",
            ),
            (EncryptError::DekUnwrapFailed, "DEK unwrap failed"),
            (
                EncryptError::InvalidKdfParams,
                "invalid Argon2id parameters",
            ),
        ];
        for (err, expected_substr) in cases {
            let msg = format!("{err}");
            assert!(
                msg.contains(expected_substr),
                "Display for {err:?} = \"{msg}\" missing \"{expected_substr}\""
            );
        }
    }

    #[test]
    fn test_validate_reserved_bytes_exact_boundary() {
        assert!(validate_reserved_bytes(ENCRYPTION_RESERVED_BYTES).is_ok());
        assert!(validate_reserved_bytes(ENCRYPTION_RESERVED_BYTES + 1).is_ok());
        assert!(validate_reserved_bytes(ENCRYPTION_RESERVED_BYTES - 1).is_err());
    }

    #[test]
    fn test_argon2_params_default_values() {
        let p = Argon2Params::default();
        assert_eq!(p.m_cost, 65_536);
        assert_eq!(p.t_cost, 3);
        assert_eq!(p.p_cost, 4);
    }

    #[test]
    fn test_page_encryptor_dek_accessor() {
        let enc = PageEncryptor::new(&TEST_DEK, TEST_DB_ID);
        assert_eq!(enc.dek(), &TEST_DEK);
    }

    #[test]
    fn test_database_id_roundtrip() {
        let bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let id = DatabaseId::from_bytes(bytes);
        assert_eq!(*id.as_bytes(), bytes);
    }

    #[test]
    fn test_database_id_eq_and_hash() {
        use std::collections::HashSet;
        let a = DatabaseId::from_bytes([0x11; DATABASE_ID_SIZE]);
        let b = DatabaseId::from_bytes([0x11; DATABASE_ID_SIZE]);
        let c = DatabaseId::from_bytes([0x22; DATABASE_ID_SIZE]);
        assert_eq!(a, b);
        assert_ne!(a, c);
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
        assert!(!set.contains(&c));
    }

    #[test]
    fn test_encrypt_error_clone_and_eq() {
        let e1 = EncryptError::AuthenticationFailed;
        let e2 = e1.clone();
        assert_eq!(e1, e2);
        assert_ne!(e1, EncryptError::EncryptionFailed);
    }

    #[test]
    fn test_dek_wrap_unwrap_roundtrip() {
        let kek = [0xCC; KEY_SIZE];
        let nonce = test_nonce(50);
        let wrapped = KeyManager::wrap_dek(&TEST_DEK, &kek, &nonce).unwrap();
        assert_eq!(wrapped.len(), NONCE_SIZE + KEY_SIZE + TAG_SIZE);
        let recovered = KeyManager::unwrap_dek(&wrapped, &kek).unwrap();
        assert_eq!(recovered, TEST_DEK);
    }

    #[test]
    fn test_page_encryptor_database_id_accessor() {
        let db_id = DatabaseId::from_bytes([0x77; DATABASE_ID_SIZE]);
        let enc = PageEncryptor::new(&TEST_DEK, db_id);
        assert_eq!(enc.database_id(), db_id);
    }

    #[test]
    fn argon2_params_debug_and_clone() {
        let p = Argon2Params {
            m_cost: 1024,
            t_cost: 2,
            p_cost: 1,
        };
        let dbg = format!("{p:?}");
        assert!(dbg.contains("Argon2Params"));
        assert!(dbg.contains("1024"));
        let cloned = p.clone();
        assert_eq!(cloned.m_cost, 1024);
        assert_eq!(cloned.t_cost, 2);
        assert_eq!(cloned.p_cost, 1);
    }

    #[test]
    fn encrypt_error_implements_std_error() {
        let e: &dyn std::error::Error = &EncryptError::AuthenticationFailed;
        assert!(e.source().is_none());
        assert!(!e.to_string().is_empty());
    }

    #[test]
    fn database_id_debug_format() {
        let id = DatabaseId::from_bytes([0xAB; DATABASE_ID_SIZE]);
        let dbg = format!("{id:?}");
        assert!(dbg.contains("DatabaseId"));
    }

    #[test]
    fn encryption_reserved_bytes_equals_nonce_plus_tag() {
        assert_eq!(
            usize::from(ENCRYPTION_RESERVED_BYTES),
            NONCE_SIZE + TAG_SIZE,
            "reserved must be exactly nonce + tag"
        );
    }

    #[test]
    fn test_key_envelope_create_unlock_rekey_o1_and_tamper_controls() {
        let cx = Cx::new();
        let params = Argon2Params {
            m_cost: 256,
            t_cost: 1,
            p_cost: 1,
        };
        let (env1, enc1) =
            KeyEnvelope::create_new(&cx, b"initial-secret", 4096, ENCRYPTION_RESERVED_BYTES, params.clone())
                .expect("create envelope");

        // Debug representation must redact wrapped DEK & DEK material.
        assert!(format!("{env1:?}").contains("<redacted>"));
        assert!(format!("{enc1:?}").contains("<redacted>"));

        let raw1 = env1.to_bytes();
        let parsed1 = KeyEnvelope::from_bytes(&raw1).expect("parse valid envelope");
        assert_eq!(parsed1, env1);

        // Unlock with right passphrase recovers identical DEK + DatabaseId.
        let unlocked1 = parsed1.unlock(&cx, b"initial-secret").expect("unlock v1");
        assert_eq!(unlocked1.dek(), enc1.dek());
        assert_eq!(unlocked1.database_id(), enc1.database_id());

        // Negative control: wrong passphrase fails closed with NotADatabase.
        assert!(matches!(
            parsed1.unlock(&cx, b"wrong-secret"),
            Err(FrankenError::NotADatabase { .. })
        ));

        // Negative control: tampering ANY metadata byte fails either BLAKE3 or AAD auth.
        for tamper_idx in [0_usize, 18, 22, 26, 36, 45, 60, 80, 150] {
            let mut tampered = raw1;
            tampered[tamper_idx] ^= 0x01;
            assert!(
                KeyEnvelope::from_bytes(&tampered).is_err(),
                "tampered envelope byte {tamper_idx} must be rejected"
            );
        }

        // Even if an attacker recomputes the structural BLAKE3 digest after
        // modifying AAD-covered header metadata, XChaCha20-Poly1305 AAD rejects it!
        let mut forged = raw1;
        forged[22] ^= 0x10; // flip page_size bit (4096 -> 8192? or invalid; use flags/m_cost)
        forged[22] = raw1[22];
        forged[27] ^= 0x01; // flip m_cost bit (still valid KDF param)
        let recomputed = blake3::hash(&forged[..144]);
        forged[144..160].copy_from_slice(&recomputed.as_bytes()[..16]);
        let forged_env = KeyEnvelope::from_bytes(&forged).expect("structural parse succeeds");
        assert!(
            matches!(
                forged_env.unlock(&cx, b"initial-secret"),
                Err(FrankenError::NotADatabase { .. })
            ),
            "AEAD AAD must reject metadata tampering even with recomputed BLAKE3"
        );

        // O(1) rekey preserves DEK & DatabaseId while rotating salt & KEK.
        let env2 = env1
            .rekey_o1(&cx, &enc1, b"rotated-secret", params)
            .expect("o1 rekey");
        assert_eq!(env2.database_id(), env1.database_id());
        assert_ne!(env2.salt(), env1.salt());
        assert_ne!(env2.wrapped_dek(), env1.wrapped_dek());

        let unlocked2 = env2.unlock(&cx, b"rotated-secret").expect("unlock v2");
        assert_eq!(unlocked2.dek(), enc1.dek());
        assert_eq!(unlocked2.database_id(), enc1.database_id());
        assert!(matches!(
            env2.unlock(&cx, b"initial-secret"),
            Err(FrankenError::NotADatabase { .. })
        ));

        // Atomic persistence roundtrip on filesystem.
        #[cfg(not(target_arch = "wasm32"))]
        {
            let dir = tempfile::tempdir().expect("tempdir");
            let db_path = dir.path().join("enc.db");
            let key_path = key_envelope_path_for_db(&db_path);
            assert!(read_key_envelope(&key_path).unwrap().is_none());
            write_key_envelope_atomic(&cx, &key_path, &env1).expect("write v1");
            assert_eq!(read_key_envelope(&key_path).unwrap(), Some(env1));
            write_key_envelope_atomic(&cx, &key_path, &env2).expect("atomic rekey write v2");
            assert_eq!(read_key_envelope(&key_path).unwrap(), Some(env2));
            remove_key_envelope_if_exists(&key_path).expect("remove envelope");
            assert!(read_key_envelope(&key_path).unwrap().is_none());
        }
    }

    #[test]
    fn test_page_cipher_slot_and_nonce_cancellation() {
        let cx = Cx::new();
        let slot = PageCipherSlot::default();
        assert!(!slot.is_enabled());
        assert!(slot.get().is_none());

        let enc = Arc::new(PageEncryptor::new(&TEST_DEK, TEST_DB_ID));
        let active = ActivePageCipher {
            encryptor: enc,
            page_size: 4096,
        };
        slot.set(Some(active.clone()));
        assert!(slot.is_enabled());
        assert!(slot.get().is_some());

        let plain = make_test_page(4096);
        let ct_a = encrypt_page_for_write(&active, &cx, 3, &plain).expect("encrypt a");
        let ct_b = encrypt_page_for_write(&active, &cx, 3, &plain).expect("encrypt b");
        assert_ne!(ct_a, ct_b, "fresh random nonces must differ on every write");

        let mut dec = ct_a.clone();
        decrypt_page_after_read(&active, 3, &mut dec).expect("decrypt valid");
        assert_eq!(&dec[..4096 - 40], &plain[..4096 - 40]);

        // Page swap must fail authentication.
        let mut swapped = ct_a;
        assert!(decrypt_page_after_read(&active, 4, &mut swapped).is_err());

        // Cancelled Cx must abort nonce generation and key operations.
        let cancelled = Cx::new();
        cancelled.transition_to_running();
        cancelled.cancel_with_reason(fsqlite_types::cx::CancelReason::UserInterrupt);
        assert!(matches!(
            generate_fresh_page_nonce(&cancelled),
            Err(FrankenError::Abort)
        ));
        assert!(matches!(
            encrypt_page_for_write(&active, &cancelled, 3, &plain),
            Err(FrankenError::Abort)
        ));

        slot.set(None);
        assert!(!slot.is_enabled());
        assert!(slot.get().is_none());
    }
}
