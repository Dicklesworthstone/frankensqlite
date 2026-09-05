//! Epoch clock, key derivation, and epoch transition barrier (§4.18, bd-3go.12).
//!
//! `ecs_epoch` is a monotone `u64` stored durably in [`RootManifest::ecs_epoch`]
//! and mirrored in `SharedMemoryLayout.ecs_epoch`. Epochs MUST NOT be reused.
//!
//! The [`EpochClock`] provides the in-process monotone counter. The
//! [`EpochBarrier`] coordinates all-or-nothing epoch transitions across
//! participants (WriteCoordinator, SymbolStore, Replicator, CheckpointGc).

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::commit_marker::{
    CommitMarkerRecord, MARKER_SEGMENT_HEADER_BYTES, MarkerSegmentHeader, recover_valid_prefix,
    segment_id_for_commit_seq,
};
use crate::symbol_log::scan_symbol_segment;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{EpochId, ObjectId, SymbolRecord};
use tracing::{debug, error, info, warn};

// ── Domain separation constants (§4.18.2) ──────────────────────────────

/// Domain separator for deriving the symbol auth master key from a DEK.
///
/// `master_key = BLAKE3_KEYED(DEK, "fsqlite:symbol-auth-master:v1")`
const MASTER_KEY_DOMAIN: &[u8] = b"fsqlite:symbol-auth-master:v1";

/// Domain separator for deriving per-epoch auth keys.
///
/// `K_epoch = BLAKE3_KEYED(master_key, "fsqlite:symbol-auth:epoch:v1" || le_u64(ecs_epoch))`
const EPOCH_KEY_DOMAIN: &[u8] = b"fsqlite:symbol-auth:epoch:v1";

/// Domain separator for `ecs/root` authentication tags (§3.5.5).
const ROOT_POINTER_AUTH_DOMAIN: &[u8] = b"fsqlite:ecs-root-auth:v1";

/// Logging bead id for RootManifest bootstrap work.
const ROOT_BOOTSTRAP_BEAD_ID: &str = "bd-1hi.25";
/// Structured logging standard bead reference.
const ROOT_BOOTSTRAP_LOGGING_STANDARD: &str = "bd-1fpm";
/// Process-local suffix counter for `ecs/root` temp file names.
static ROOT_TMP_SUFFIX_COUNTER: AtomicU64 = AtomicU64::new(0);

// ── EpochClock ─────────────────────────────────────────────────────────

/// In-process monotone epoch counter (§4.18).
///
/// Wraps an `AtomicU64` for lock-free reads. Increments are serialized by
/// the coordinator (only one caller should call [`EpochClock::increment`] at a time).
#[derive(Debug)]
pub struct EpochClock {
    current: AtomicU64,
}

impl EpochClock {
    /// Create a new clock initialised at the given epoch.
    #[must_use]
    pub fn new(initial: EpochId) -> Self {
        Self {
            current: AtomicU64::new(initial.get()),
        }
    }

    /// Read the current epoch (Acquire ordering).
    #[must_use]
    pub fn current(&self) -> EpochId {
        EpochId::new(self.current.load(Ordering::Acquire))
    }

    /// Atomically increment the epoch counter by one.
    ///
    /// Returns the *new* epoch on success, or an error if the counter
    /// would overflow (saturated at `u64::MAX`).
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the epoch has reached `u64::MAX`.
    pub fn increment(&self) -> Result<EpochId> {
        loop {
            let old = self.current.load(Ordering::Acquire);
            let new = old.checked_add(1).ok_or_else(|| {
                error!(
                    bead_id = "bd-3go.12",
                    old_epoch = old,
                    "epoch counter overflow — cannot increment past u64::MAX"
                );
                FrankenError::OutOfRange {
                    what: "ecs_epoch".to_owned(),
                    value: old.to_string(),
                }
            })?;
            if self
                .current
                .compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                info!(
                    bead_id = "bd-3go.12",
                    old_epoch = old,
                    new_epoch = new,
                    "epoch incremented"
                );
                return Ok(EpochId::new(new));
            }
        }
    }

    /// Store a specific epoch value (Release ordering).
    ///
    /// Used during bootstrap or recovery to set the epoch from a persisted
    /// `RootManifest.ecs_epoch`.
    pub fn store(&self, epoch: EpochId) {
        self.current.store(epoch.get(), Ordering::Release);
    }
}

// ── Epoch-scoped key derivation (§4.18.2) ──────────────────────────────

/// A 32-byte epoch-scoped symbol authentication key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochAuthKey([u8; 32]);

impl EpochAuthKey {
    /// View the raw key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Derive the symbol auth master key from a DEK with domain separation (§4.18.2).
///
/// `master_key = BLAKE3_KEYED(DEK, "fsqlite:symbol-auth-master:v1")`
///
/// The DEK must be exactly 32 bytes (XChaCha20-Poly1305 key size).
pub fn derive_master_key_from_dek(dek: &[u8; 32]) -> [u8; 32] {
    let keyed_hasher = blake3::Hasher::new_keyed(dek);
    let mut hasher = keyed_hasher;
    hasher.update(MASTER_KEY_DOMAIN);
    let hash = hasher.finalize();
    debug!(
        bead_id = "bd-3go.12",
        domain = std::str::from_utf8(MASTER_KEY_DOMAIN).unwrap_or("<invalid>"),
        "derived master key from DEK with domain separation"
    );
    *hash.as_bytes()
}

/// Derive an epoch-scoped authentication key from a master key (§4.18.2).
///
/// `K_epoch = BLAKE3_KEYED(master_key, "fsqlite:symbol-auth:epoch:v1" || le_u64(ecs_epoch))`
///
/// Deterministic: same `(master_key, epoch)` always produces the same key.
#[must_use]
pub fn derive_epoch_auth_key(master_key: &[u8; 32], epoch: EpochId) -> EpochAuthKey {
    let mut hasher = blake3::Hasher::new_keyed(master_key);
    hasher.update(EPOCH_KEY_DOMAIN);
    hasher.update(&epoch.get().to_le_bytes());
    let hash = hasher.finalize();
    debug!(
        bead_id = "bd-3go.12",
        epoch = epoch.get(),
        domain = std::str::from_utf8(EPOCH_KEY_DOMAIN).unwrap_or("<invalid>"),
        "derived epoch auth key (NOT logging key material)"
    );
    EpochAuthKey(*hash.as_bytes())
}

// ── EpochBarrier (§4.18.4) ──────────────────────────────────────────────

/// Outcome of an epoch barrier attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierOutcome {
    /// All participants arrived; epoch was incremented.
    AllArrived {
        /// The new epoch after increment.
        new_epoch: EpochId,
    },
    /// The barrier timed out before all participants arrived.
    Timeout {
        /// How many participants arrived before timeout.
        arrived: usize,
        /// Total expected participants.
        expected: usize,
    },
    /// The barrier was explicitly cancelled.
    Cancelled,
}

/// Epoch transition barrier (§4.18.4).
///
/// Coordinates quiescence across all correctness-critical services before
/// incrementing the epoch. The barrier is all-or-nothing: either all
/// participants arrive, or the epoch does not advance.
///
/// Participants: WriteCoordinator, SymbolStore, Replicator, CheckpointGc.
#[derive(Debug)]
pub struct EpochBarrier {
    /// The epoch being transitioned *from*.
    current_epoch: EpochId,
    /// Total expected participants.
    expected: usize,
    /// Number of participants that have arrived.
    arrived: AtomicU64,
    /// Whether the barrier has been cancelled.
    cancelled: std::sync::atomic::AtomicBool,
}

impl EpochBarrier {
    /// Create a new epoch barrier.
    ///
    /// `current_epoch` is the epoch being transitioned from.
    /// `participants` is the number of services that must drain and arrive.
    #[must_use]
    pub fn new(current_epoch: EpochId, participants: usize) -> Self {
        info!(
            bead_id = "bd-3go.12",
            epoch = current_epoch.get(),
            participants,
            "epoch barrier created"
        );
        Self {
            current_epoch,
            expected: participants,
            arrived: AtomicU64::new(0),
            cancelled: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The epoch being transitioned from.
    #[must_use]
    pub fn epoch(&self) -> EpochId {
        self.current_epoch
    }

    /// Number of participants that have arrived so far.
    #[must_use]
    pub fn arrived_count(&self) -> usize {
        let val = self.arrived.load(Ordering::Acquire);
        usize::try_from(val).unwrap_or(usize::MAX)
    }

    /// Total expected participants.
    #[must_use]
    pub fn expected_count(&self) -> usize {
        self.expected
    }

    /// Register that a participant has drained in-flight work and arrived.
    ///
    /// Returns `true` if this was the last participant (barrier is complete).
    pub fn arrive(&self, participant_name: &str) -> bool {
        if self.cancelled.load(Ordering::Acquire) {
            warn!(
                bead_id = "bd-3go.12",
                participant = participant_name,
                "participant arrived at cancelled barrier — ignoring"
            );
            return false;
        }
        let prev = self.arrived.fetch_add(1, Ordering::AcqRel);
        let new_count = usize::try_from(prev.saturating_add(1)).unwrap_or(usize::MAX);
        debug!(
            bead_id = "bd-3go.12",
            participant = participant_name,
            arrived = new_count,
            expected = self.expected,
            "barrier participant arrived"
        );
        new_count >= self.expected
    }

    /// Whether all participants have arrived.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.arrived_count() >= self.expected
    }

    /// Cancel the barrier. The epoch will NOT advance.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        warn!(
            bead_id = "bd-3go.12",
            epoch = self.current_epoch.get(),
            arrived = self.arrived_count(),
            expected = self.expected,
            "epoch barrier cancelled — epoch will NOT advance"
        );
    }

    /// Whether the barrier has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Resolve the barrier after a timeout or cancellation check.
    ///
    /// If all participants arrived and the barrier is not cancelled,
    /// the epoch clock is incremented and the new epoch is returned.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the epoch clock overflows.
    pub fn resolve(&self, clock: &EpochClock) -> Result<BarrierOutcome> {
        if self.is_cancelled() {
            return Ok(BarrierOutcome::Cancelled);
        }
        if !self.is_complete() {
            return Ok(BarrierOutcome::Timeout {
                arrived: self.arrived_count(),
                expected: self.expected,
            });
        }
        let new_epoch = clock.increment()?;
        info!(
            bead_id = "bd-3go.12",
            old_epoch = self.current_epoch.get(),
            new_epoch = new_epoch.get(),
            participants = self.expected,
            "epoch transition completed — all participants arrived"
        );
        Ok(BarrierOutcome::AllArrived { new_epoch })
    }
}

// ── Validation helpers ──────────────────────────────────────────────────

/// Validate a symbol's epoch against the current validity window (§4.18.1).
///
/// Fail-closed: rejects symbols with `epoch_id > current_epoch`.
///
/// # Errors
///
/// Returns [`FrankenError::DatabaseCorrupt`] if the symbol epoch is outside
/// the validity window.
pub fn validate_symbol_epoch(
    symbol_epoch: EpochId,
    window: &fsqlite_types::SymbolValidityWindow,
) -> Result<()> {
    if window.contains(symbol_epoch) {
        Ok(())
    } else {
        error!(
            bead_id = "bd-3go.12",
            symbol_epoch = symbol_epoch.get(),
            window_from = window.from_epoch.get(),
            window_to = window.to_epoch.get(),
            "symbol epoch outside validity window — fail-closed rejection"
        );
        Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "symbol epoch {} outside validity window [{}, {}]",
                symbol_epoch.get(),
                window.from_epoch.get(),
                window.to_epoch.get(),
            ),
        })
    }
}

// ── RootManifest bootstrap (§3.5.5, bd-1hi.25) ──────────────────────────

/// Magic bytes for `ecs/root`: `"FSRT"`.
pub const ECS_ROOT_POINTER_MAGIC: [u8; 4] = *b"FSRT";
/// Supported `ecs/root` pointer version.
pub const ECS_ROOT_POINTER_VERSION: u32 = 1;
/// Exact wire size of `EcsRootPointer`.
pub const ECS_ROOT_POINTER_BYTES: usize = 56;
/// Bytes covered by `checksum` in `EcsRootPointer`.
const ECS_ROOT_POINTER_CHECKSUM_INPUT_BYTES: usize = 32;
/// Bytes covered by `root_auth_tag` in `EcsRootPointer`.
const ECS_ROOT_POINTER_AUTH_INPUT_BYTES: usize = 40;

/// Magic bytes for `RootManifest`: `"FSQLROOT"`.
pub const ROOT_MANIFEST_MAGIC: [u8; 8] = *b"FSQLROOT";
/// Supported RootManifest version.
pub const ROOT_MANIFEST_VERSION: u32 = 1;

/// Mutable bootstrap pointer at `ecs/root`.
///
/// The pointer is tiny and atomically updated; it is the only file read before
/// object lookup starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcsRootPointer {
    /// ObjectId of the current RootManifest object.
    pub manifest_object_id: ObjectId,
    /// Bootstrap epoch guard (`root_epoch`).
    pub ecs_epoch: EpochId,
    /// Optional keyed authentication tag for `symbol_auth=on`.
    pub root_auth_tag: [u8; 16],
}

/// Only damage to the root anchor itself permits automatic scan recovery.
/// Keep this classification at the failing check, rather than inspecting error
/// messages or retrying errors from authenticated objects later in bootstrap.
#[derive(Debug)]
enum RootPointerReadFailure {
    Recoverable(FrankenError),
    Terminal(FrankenError),
}

impl RootPointerReadFailure {
    fn into_error(self) -> FrankenError {
        match self {
            Self::Recoverable(error) | Self::Terminal(error) => error,
        }
    }
}

impl From<FrankenError> for RootPointerReadFailure {
    fn from(error: FrankenError) -> Self {
        Self::Terminal(error)
    }
}

fn required_symbol_auth_key(
    symbol_auth_enabled: bool,
    master_key: Option<&[u8; 32]>,
) -> Result<Option<&[u8; 32]>> {
    if symbol_auth_enabled {
        master_key
            .map(Some)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "symbol_auth enabled but master key is missing (reason=auth_failed)"
                    .to_owned(),
            })
    } else {
        Ok(None)
    }
}

impl EcsRootPointer {
    /// Construct an unauthenticated root pointer (`symbol_auth=off`).
    #[must_use]
    pub const fn unauthed(manifest_object_id: ObjectId, ecs_epoch: EpochId) -> Self {
        Self {
            manifest_object_id,
            ecs_epoch,
            root_auth_tag: [0_u8; 16],
        }
    }

    /// Construct an authenticated root pointer (`symbol_auth=on`).
    #[must_use]
    pub fn authed(manifest_object_id: ObjectId, ecs_epoch: EpochId, master_key: &[u8; 32]) -> Self {
        let mut pointer = Self::unauthed(manifest_object_id, ecs_epoch);
        let auth_input = pointer.auth_input_bytes();
        pointer.root_auth_tag = compute_root_pointer_auth_tag(master_key, &auth_input);
        pointer
    }

    /// Encode to exact wire bytes.
    #[must_use]
    pub fn encode(&self) -> [u8; ECS_ROOT_POINTER_BYTES] {
        let mut out = [0_u8; ECS_ROOT_POINTER_BYTES];
        out[0..4].copy_from_slice(&ECS_ROOT_POINTER_MAGIC);
        out[4..8].copy_from_slice(&ECS_ROOT_POINTER_VERSION.to_le_bytes());
        out[8..24].copy_from_slice(self.manifest_object_id.as_bytes());
        out[24..32].copy_from_slice(&self.ecs_epoch.get().to_le_bytes());
        let checksum = xxhash_rust::xxh3::xxh3_64(&out[..ECS_ROOT_POINTER_CHECKSUM_INPUT_BYTES]);
        out[32..40].copy_from_slice(&checksum.to_le_bytes());
        out[40..56].copy_from_slice(&self.root_auth_tag);
        out
    }

    /// Decode and validate `ecs/root`.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::DatabaseCorrupt`] on magic/version/checksum/auth failures.
    pub fn decode(
        bytes: &[u8],
        symbol_auth_enabled: bool,
        master_key: Option<&[u8; 32]>,
    ) -> Result<Self> {
        let auth_key = required_symbol_auth_key(symbol_auth_enabled, master_key)?;
        Self::decode_classified(bytes, auth_key).map_err(RootPointerReadFailure::into_error)
    }

    fn decode_classified(
        bytes: &[u8],
        auth_key: Option<&[u8; 32]>,
    ) -> std::result::Result<Self, RootPointerReadFailure> {
        if bytes.len() != ECS_ROOT_POINTER_BYTES {
            let error = FrankenError::DatabaseCorrupt {
                detail: format!(
                    "ecs/root size mismatch: expected {ECS_ROOT_POINTER_BYTES}, got {}",
                    bytes.len()
                ),
            };
            return Err(if bytes.len() < ECS_ROOT_POINTER_BYTES {
                RootPointerReadFailure::Recoverable(error)
            } else {
                RootPointerReadFailure::Terminal(error)
            });
        }
        if bytes[0..4] != ECS_ROOT_POINTER_MAGIC {
            return Err(RootPointerReadFailure::Terminal(
                FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "invalid ecs/root magic: {:02X?} (reason=bad_magic)",
                        &bytes[0..4]
                    ),
                },
            ));
        }
        let version = read_u32_le_at(bytes, 4, "root.version")?;
        if version != ECS_ROOT_POINTER_VERSION {
            return Err(RootPointerReadFailure::Terminal(
                FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "unsupported ecs/root version {version} (expected {ECS_ROOT_POINTER_VERSION})"
                    ),
                },
            ));
        }

        let mut manifest_id = [0_u8; 16];
        manifest_id.copy_from_slice(&bytes[8..24]);
        let manifest_object_id = ObjectId::from_bytes(manifest_id);
        let ecs_epoch_raw = read_u64_le_at(bytes, 24, "root.ecs_epoch")?;
        let ecs_epoch = EpochId::new(ecs_epoch_raw);

        let stored_checksum = read_u64_le_at(bytes, 32, "root.checksum")?;
        let computed_checksum =
            xxhash_rust::xxh3::xxh3_64(&bytes[..ECS_ROOT_POINTER_CHECKSUM_INPUT_BYTES]);
        if stored_checksum != computed_checksum {
            error!(
                bead_id = ROOT_BOOTSTRAP_BEAD_ID,
                logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
                reason_code = "checksum_mismatch",
                stored_checksum = stored_checksum,
                computed_checksum = computed_checksum,
                "ecs/root checksum verification failed"
            );
            return Err(RootPointerReadFailure::Recoverable(
                FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "ecs/root checksum mismatch (reason=checksum_mismatch): stored={stored_checksum:#018X}, computed={computed_checksum:#018X}"
                    ),
                },
            ));
        }

        let mut root_auth_tag = [0_u8; 16];
        root_auth_tag.copy_from_slice(&bytes[40..56]);

        if let Some(master_key) = auth_key {
            let expected = compute_root_pointer_auth_tag(
                master_key,
                &bytes[..ECS_ROOT_POINTER_AUTH_INPUT_BYTES],
            );
            if root_auth_tag != expected {
                error!(
                    bead_id = ROOT_BOOTSTRAP_BEAD_ID,
                    logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
                    reason_code = "auth_failed",
                    "ecs/root auth-tag verification failed"
                );
                return Err(RootPointerReadFailure::Terminal(
                    FrankenError::DatabaseCorrupt {
                        detail: "ecs/root auth tag verification failed (reason=auth_failed)"
                            .to_owned(),
                    },
                ));
            }
        } else if root_auth_tag != [0_u8; 16] {
            return Err(RootPointerReadFailure::Terminal(
                FrankenError::DatabaseCorrupt {
                    detail: "ecs/root auth tag must be all-zero when symbol_auth=off".to_owned(),
                },
            ));
        }

        Ok(Self {
            manifest_object_id,
            ecs_epoch,
            root_auth_tag,
        })
    }

    /// Bytes used to compute `root_auth_tag`.
    #[must_use]
    fn auth_input_bytes(&self) -> [u8; ECS_ROOT_POINTER_AUTH_INPUT_BYTES] {
        let encoded = self.encode();
        let mut out = [0_u8; ECS_ROOT_POINTER_AUTH_INPUT_BYTES];
        out.copy_from_slice(&encoded[..ECS_ROOT_POINTER_AUTH_INPUT_BYTES]);
        out
    }
}

/// Root bootstrap state object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootManifest {
    /// Human-readable database name.
    pub database_name: String,
    /// Latest commit marker object id.
    pub current_commit: ObjectId,
    /// Latest commit sequence.
    pub commit_seq: u64,
    /// Current schema snapshot object id.
    pub schema_snapshot: ObjectId,
    /// Monotone schema epoch.
    pub schema_epoch: u64,
    /// Monotone ECS coordination epoch.
    pub ecs_epoch: EpochId,
    /// Last full checkpoint base object.
    pub checkpoint_base: ObjectId,
    /// GC horizon commit sequence.
    pub gc_horizon: u64,
    /// Creation timestamp.
    pub created_at: u64,
    /// Last update timestamp.
    pub updated_at: u64,
}

impl RootManifest {
    /// Encode to deterministic bytes with trailing checksum.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if `database_name` is too large.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let name_bytes = self.database_name.as_bytes();
        let name_len = u32::try_from(name_bytes.len()).map_err(|_| FrankenError::OutOfRange {
            what: "root_manifest.database_name_len".to_owned(),
            value: name_bytes.len().to_string(),
        })?;

        let mut out = Vec::with_capacity(name_bytes.len().saturating_add(128));
        out.extend_from_slice(&ROOT_MANIFEST_MAGIC);
        out.extend_from_slice(&ROOT_MANIFEST_VERSION.to_le_bytes());
        out.extend_from_slice(&name_len.to_le_bytes());
        out.extend_from_slice(name_bytes);
        out.extend_from_slice(self.current_commit.as_bytes());
        out.extend_from_slice(&self.commit_seq.to_le_bytes());
        out.extend_from_slice(self.schema_snapshot.as_bytes());
        out.extend_from_slice(&self.schema_epoch.to_le_bytes());
        out.extend_from_slice(&self.ecs_epoch.get().to_le_bytes());
        out.extend_from_slice(self.checkpoint_base.as_bytes());
        out.extend_from_slice(&self.gc_horizon.to_le_bytes());
        out.extend_from_slice(&self.created_at.to_le_bytes());
        out.extend_from_slice(&self.updated_at.to_le_bytes());
        let checksum = xxhash_rust::xxh3::xxh3_64(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        Ok(out)
    }

    /// Decode and validate a `RootManifest`.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::DatabaseCorrupt`] on wire-format violations.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 120 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "root manifest too short: expected >= 120 bytes, got {}",
                    bytes.len()
                ),
            });
        }
        if bytes[0..8] != ROOT_MANIFEST_MAGIC {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("invalid root manifest magic: {:02X?}", &bytes[0..8]),
            });
        }
        let version = read_u32_le_at(bytes, 8, "root_manifest.version")?;
        if version != ROOT_MANIFEST_VERSION {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "unsupported root manifest version {version} (expected {ROOT_MANIFEST_VERSION})"
                ),
            });
        }

        let name_len_u32 = read_u32_le_at(bytes, 12, "root_manifest.database_name_len")?;
        let name_len = u32_to_usize(name_len_u32, "root_manifest.database_name_len")?;
        let mut cursor = 16_usize;
        let name_end = checked_add(cursor, name_len, "root_manifest.database_name_end")?;
        if name_end > bytes.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "root manifest name out of bounds: end={name_end}, len={}",
                    bytes.len()
                ),
            });
        }
        let database_name = std::str::from_utf8(&bytes[cursor..name_end])
            .map_err(|err| FrankenError::DatabaseCorrupt {
                detail: format!("root manifest database_name is not UTF-8: {err}"),
            })?
            .to_owned();
        cursor = name_end;

        let current_commit = read_object_id_at(bytes, cursor, "root_manifest.current_commit")?;
        cursor = checked_add(cursor, 16, "root_manifest.cursor.current_commit")?;
        let commit_seq = read_u64_le_at(bytes, cursor, "root_manifest.commit_seq")?;
        cursor = checked_add(cursor, 8, "root_manifest.cursor.commit_seq")?;
        let schema_snapshot = read_object_id_at(bytes, cursor, "root_manifest.schema_snapshot")?;
        cursor = checked_add(cursor, 16, "root_manifest.cursor.schema_snapshot")?;
        let schema_epoch = read_u64_le_at(bytes, cursor, "root_manifest.schema_epoch")?;
        cursor = checked_add(cursor, 8, "root_manifest.cursor.schema_epoch")?;
        let ecs_epoch_raw = read_u64_le_at(bytes, cursor, "root_manifest.ecs_epoch")?;
        let ecs_epoch = EpochId::new(ecs_epoch_raw);
        cursor = checked_add(cursor, 8, "root_manifest.cursor.ecs_epoch")?;
        let checkpoint_base = read_object_id_at(bytes, cursor, "root_manifest.checkpoint_base")?;
        cursor = checked_add(cursor, 16, "root_manifest.cursor.checkpoint_base")?;
        let gc_horizon = read_u64_le_at(bytes, cursor, "root_manifest.gc_horizon")?;
        cursor = checked_add(cursor, 8, "root_manifest.cursor.gc_horizon")?;
        let created_at = read_u64_le_at(bytes, cursor, "root_manifest.created_at")?;
        cursor = checked_add(cursor, 8, "root_manifest.cursor.created_at")?;
        let updated_at = read_u64_le_at(bytes, cursor, "root_manifest.updated_at")?;
        cursor = checked_add(cursor, 8, "root_manifest.cursor.updated_at")?;

        let checksum_end = checked_add(cursor, 8, "root_manifest.cursor.checksum_end")?;
        if checksum_end != bytes.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "root manifest trailing bytes present: parsed_end={checksum_end}, actual_len={}",
                    bytes.len()
                ),
            });
        }
        let stored_checksum = read_u64_le_at(bytes, cursor, "root_manifest.checksum")?;
        let computed_checksum = xxhash_rust::xxh3::xxh3_64(&bytes[..cursor]);
        if stored_checksum != computed_checksum {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "root manifest checksum mismatch: stored={stored_checksum:#018X}, computed={computed_checksum:#018X}"
                ),
            });
        }

        Ok(Self {
            database_name,
            current_commit,
            commit_seq,
            schema_snapshot,
            schema_epoch,
            ecs_epoch,
            checkpoint_base,
            gc_horizon,
            created_at,
            updated_at,
        })
    }
}

/// Compute `root_auth_tag = Trunc128(BLAKE3_KEYED(master_key, domain || bytes(magic..checksum)))`.
#[must_use]
pub fn compute_root_pointer_auth_tag(master_key: &[u8; 32], magic_to_checksum: &[u8]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new_keyed(master_key);
    hasher.update(ROOT_POINTER_AUTH_DOMAIN);
    hasher.update(magic_to_checksum);
    let digest = hasher.finalize();
    let mut out = [0_u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    out
}

/// Filesystem layout inputs for native bootstrap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeBootstrapLayout {
    /// Path to the `ecs/` directory.
    pub ecs_dir: PathBuf,
}

impl NativeBootstrapLayout {
    /// Construct a layout rooted at `ecs_dir`.
    #[must_use]
    pub fn new(ecs_dir: impl Into<PathBuf>) -> Self {
        Self {
            ecs_dir: ecs_dir.into(),
        }
    }

    /// `ecs/root` path.
    #[must_use]
    pub fn root_path(&self) -> PathBuf {
        self.ecs_dir.join("root")
    }

    /// `ecs/symbols` directory.
    #[must_use]
    pub fn symbols_dir(&self) -> PathBuf {
        self.ecs_dir.join("symbols")
    }

    /// `ecs/markers` directory.
    #[must_use]
    pub fn markers_dir(&self) -> PathBuf {
        self.ecs_dir.join("markers")
    }
}

/// Result of successful native bootstrap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeBootstrapState {
    /// Decoded root pointer from `ecs/root`.
    pub root_pointer: EcsRootPointer,
    /// Decoded RootManifest object.
    pub manifest: RootManifest,
    /// Verified latest marker matching `manifest.current_commit`.
    pub latest_marker: CommitMarkerRecord,
    /// Bytes for `manifest.schema_snapshot`.
    pub schema_snapshot_bytes: Vec<u8>,
    /// Bytes for `manifest.checkpoint_base`.
    pub checkpoint_base_bytes: Vec<u8>,
}

/// Read and decode `ecs/root`.
///
/// # Errors
///
/// Returns [`FrankenError::DatabaseCorrupt`] for malformed root data.
pub fn read_root_pointer(
    root_path: &Path,
    symbol_auth_enabled: bool,
    master_key: Option<&[u8; 32]>,
) -> Result<EcsRootPointer> {
    let auth_key = required_symbol_auth_key(symbol_auth_enabled, master_key)?;
    read_root_pointer_classified(root_path, auth_key).map_err(RootPointerReadFailure::into_error)
}

fn read_root_pointer_classified(
    root_path: &Path,
    auth_key: Option<&[u8; 32]>,
) -> std::result::Result<EcsRootPointer, RootPointerReadFailure> {
    let mut file = fs::File::open(root_path).map_err(|err| {
        error!(
            bead_id = ROOT_BOOTSTRAP_BEAD_ID,
            logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
            reason_code = "scan_failed",
            path = %root_path.display(),
            error = %err,
            "failed reading ecs/root"
        );
        if err.kind() == std::io::ErrorKind::NotFound {
            RootPointerReadFailure::Recoverable(FrankenError::Io(err))
        } else {
            RootPointerReadFailure::Terminal(FrankenError::Io(err))
        }
    })?;
    // One extra byte distinguishes the fixed format from an oversized file.
    // Read the actual bytes, rather than trusting a racy metadata length, and
    // never allocate in proportion to an untrusted root file's size.
    let mut bytes = [0_u8; ECS_ROOT_POINTER_BYTES + 1];
    let mut filled = 0;
    while filled < bytes.len() {
        match file.read(&mut bytes[filled..]) {
            Ok(0) => break,
            Ok(count) => filled += count,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) => return Err(RootPointerReadFailure::Terminal(FrankenError::Io(err))),
        }
    }
    EcsRootPointer::decode_classified(&bytes[..filled], auth_key)
}

/// Crash-safe `ecs/root` update: temp write -> fsync temp -> rename -> fsync dir.
///
/// # Errors
///
/// Returns [`FrankenError::Io`] for filesystem failures.
pub fn write_root_pointer_atomic(root_path: &Path, pointer: EcsRootPointer) -> Result<()> {
    let Some(parent) = root_path.parent() else {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("ecs/root has no parent directory: {}", root_path.display()),
        });
    };
    fs::create_dir_all(parent)?;

    let pid = std::process::id();
    let suffix = ROOT_TMP_SUFFIX_COUNTER.fetch_add(1, Ordering::SeqCst);
    let tmp_name = format!(".root.tmp.{pid}.{suffix}");
    let tmp_path = parent.join(tmp_name);

    let bytes = pointer.encode();
    let mut temp = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)?;
    temp.write_all(&bytes)?;
    temp.sync_all()?;
    fs::rename(&tmp_path, root_path)?;
    let parent_dir = fs::File::open(parent)?;
    parent_dir.sync_all()?;

    info!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        path = %root_path.display(),
        root_epoch = pointer.ecs_epoch.get(),
        "wrote ecs/root atomically"
    );

    Ok(())
}

/// Build `EcsRootPointer` according to `symbol_auth` mode.
///
/// # Errors
///
/// Returns [`FrankenError::DatabaseCorrupt`] when auth is required but key is missing.
pub fn build_root_pointer(
    manifest_object_id: ObjectId,
    ecs_epoch: EpochId,
    symbol_auth_enabled: bool,
    master_key: Option<&[u8; 32]>,
) -> Result<EcsRootPointer> {
    if symbol_auth_enabled {
        let Some(master_key) = master_key else {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "symbol_auth enabled but master key is missing (reason=auth_failed)"
                    .to_owned(),
            });
        };
        Ok(EcsRootPointer::authed(
            manifest_object_id,
            ecs_epoch,
            master_key,
        ))
    } else {
        Ok(EcsRootPointer::unauthed(manifest_object_id, ecs_epoch))
    }
}

/// Bootstrap native mode from on-disk `ecs/root` and ECS objects.
///
/// Implements the §3.5.5 sequence:
/// 1) read root 2) verify auth (optional) 3) capture root epoch
/// 4) fetch manifest with future-epoch guard 5) enforce epoch equality
/// 6) verify marker id 7) load schema snapshot 8) load checkpoint base.
///
/// # Errors
///
/// Returns [`FrankenError::DatabaseCorrupt`] when bootstrap invariants fail.
pub fn bootstrap_native_mode(
    layout: &NativeBootstrapLayout,
    symbol_auth_enabled: bool,
    master_key: Option<&[u8; 32]>,
) -> Result<NativeBootstrapState> {
    let auth_key = required_symbol_auth_key(symbol_auth_enabled, master_key)?;
    debug!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        step = 1_u8,
        root_path = %layout.root_path().display(),
        symbol_auth_enabled = symbol_auth_enabled,
        "bootstrap step 1: reading ecs/root"
    );
    let root_path = layout.root_path();
    let root_pointer = read_root_pointer(&root_path, symbol_auth_enabled, master_key)?;
    info!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        step = 1_u8,
        duration_ms = 0_u64,
        root_epoch = root_pointer.ecs_epoch.get(),
        manifest_object_id = %root_pointer.manifest_object_id,
        "bootstrap steps 1-3 complete"
    );
    bootstrap_from_root_pointer(layout, root_pointer, auth_key)
}

/// Bootstrap native mode, recovering only a missing, torn, or checksum-corrupt root.
/// Authentication, format, I/O, and object-validation failures are terminal.
///
/// # Errors
///
/// Returns [`FrankenError::DatabaseCorrupt`] when neither root nor scan recovery succeeds.
pub fn bootstrap_native_mode_with_recovery(
    layout: &NativeBootstrapLayout,
    symbol_auth_enabled: bool,
    master_key: Option<&[u8; 32]>,
) -> Result<NativeBootstrapState> {
    let auth_key = required_symbol_auth_key(symbol_auth_enabled, master_key)?;
    match read_root_pointer_classified(&layout.root_path(), auth_key) {
        Ok(root_pointer) => bootstrap_from_root_pointer(layout, root_pointer, auth_key),
        Err(RootPointerReadFailure::Terminal(error)) => Err(error),
        Err(RootPointerReadFailure::Recoverable(initial_err)) => {
            debug!(
                bead_id = ROOT_BOOTSTRAP_BEAD_ID,
                logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
                reason_code = "retry_scan_recovery",
                error = %initial_err,
                "bootstrap entering degraded scan-based recovery path"
            );
            warn!(
                bead_id = ROOT_BOOTSTRAP_BEAD_ID,
                logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
                reason_code = "retry_scan_recovery",
                error = %initial_err,
                "ecs/root is missing or damaged; attempting scan-based recovery"
            );

            let recovered_pointer =
                recover_root_pointer_from_scan(layout, symbol_auth_enabled, master_key)?;

            // DATA INTEGRITY: validate the recovered root BEFORE persisting it.
            // Previously, write_root_pointer_atomic was called first, meaning a
            // bogus candidate would be persisted to disk even if validation failed,
            // leaving permanent corruption in ecs/root.
            let state = bootstrap_from_root_pointer(layout, recovered_pointer, auth_key)?;

            // Only persist the root pointer after full validation succeeds.
            write_root_pointer_atomic(&layout.root_path(), recovered_pointer)?;
            Ok(state)
        }
    }
}

/// Recover `ecs/root` by scanning markers and symbols for the latest valid RootManifest.
///
/// # Errors
///
/// Returns [`FrankenError::DatabaseCorrupt`] when no manifest candidate is recoverable.
pub fn recover_root_pointer_from_scan(
    layout: &NativeBootstrapLayout,
    symbol_auth_enabled: bool,
    master_key: Option<&[u8; 32]>,
) -> Result<EcsRootPointer> {
    let auth_key = required_symbol_auth_key(symbol_auth_enabled, master_key)?;
    let marker_tip = scan_latest_marker(layout.markers_dir().as_path())?;
    let mut grouped: BTreeMap<ObjectId, Vec<SymbolRecord>> = BTreeMap::new();
    let symbol_segments = sorted_segment_paths(layout.symbols_dir().as_path())?;
    debug!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        segments = symbol_segments.len(),
        marker_tip_commit_seq = marker_tip.as_ref().map_or(0_u64, |m| m.commit_seq),
        "scan recovery started"
    );

    for (_, segment_path) in &symbol_segments {
        debug!(
            bead_id = ROOT_BOOTSTRAP_BEAD_ID,
            logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
            segment = %segment_path.display(),
            "scan recovery inspecting symbol segment"
        );
        let scan = scan_symbol_segment(segment_path)?;
        let epoch_key =
            auth_key.map(|key| derive_epoch_auth_key(key, EpochId::new(scan.header.epoch_id)));
        for row in scan.records {
            verify_bootstrap_symbol_auth(&row.record, scan.header.epoch_id, epoch_key.as_ref())?;
            grouped
                .entry(row.record.object_id)
                .or_default()
                .push(row.record);
        }
    }

    let mut best: Option<(ObjectId, RootManifest, bool)> = None;
    for (object_id, records) in grouped {
        let payload = match reconstruct_payload_from_source_symbols(records) {
            Ok(payload) => payload,
            // Unavailable decoding or memory cannot establish that a candidate
            // is invalid. Skipping it could silently publish an older root.
            Err(error @ (FrankenError::NotImplemented(_) | FrankenError::OutOfMemory)) => {
                return Err(error);
            }
            Err(_) => continue,
        };
        let Ok(manifest) = RootManifest::decode(&payload) else {
            continue;
        };

        let marker_matches = marker_tip.as_ref().is_some_and(|tip| {
            manifest.current_commit.as_bytes() == &tip.marker_id
                && manifest.commit_seq == tip.commit_seq
        });

        match &best {
            None => best = Some((object_id, manifest, marker_matches)),
            Some((_, best_manifest, best_marker_matches)) => {
                let better_marker_match = marker_matches && !best_marker_matches;
                let better_commit = manifest.commit_seq > best_manifest.commit_seq;
                let better_update = manifest.commit_seq == best_manifest.commit_seq
                    && manifest.updated_at > best_manifest.updated_at;
                if better_marker_match || better_commit || better_update {
                    best = Some((object_id, manifest, marker_matches));
                }
            }
        }
    }

    let Some((manifest_object_id, manifest, _)) = best else {
        error!(
            bead_id = ROOT_BOOTSTRAP_BEAD_ID,
            logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
            reason_code = "scan_failed",
            segments_scanned = symbol_segments.len(),
            "scan recovery could not find a valid RootManifest candidate"
        );
        return Err(FrankenError::DatabaseCorrupt {
            detail: "scan recovery failed: no valid RootManifest candidate (reason=scan_failed)"
                .to_owned(),
        });
    };

    info!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        segments_scanned = symbol_segments.len(),
        best_candidate_commit_seq = manifest.commit_seq,
        chosen_root_pointer = %manifest_object_id,
        "scan recovery selected root manifest candidate"
    );

    build_root_pointer(
        manifest_object_id,
        manifest.ecs_epoch,
        symbol_auth_enabled,
        master_key,
    )
}

#[allow(clippy::too_many_lines)]
fn bootstrap_from_root_pointer(
    layout: &NativeBootstrapLayout,
    root_pointer: EcsRootPointer,
    auth_key: Option<&[u8; 32]>,
) -> Result<NativeBootstrapState> {
    info!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        step = 4_u8,
        root_epoch = root_pointer.ecs_epoch.get(),
        manifest_object_id = %root_pointer.manifest_object_id,
        "bootstrap step 4: loading root manifest object"
    );
    let manifest_bytes = fetch_object_payload(
        layout.symbols_dir().as_path(),
        root_pointer.manifest_object_id,
        root_pointer.ecs_epoch,
        auth_key,
    )?;
    let manifest = RootManifest::decode(&manifest_bytes)?;
    info!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        step = 4_u8,
        duration_ms = 0_u64,
        root_epoch = root_pointer.ecs_epoch.get(),
        object_id = %root_pointer.manifest_object_id,
        "bootstrap step 4 complete"
    );

    if manifest.ecs_epoch != root_pointer.ecs_epoch {
        error!(
            bead_id = ROOT_BOOTSTRAP_BEAD_ID,
            logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
            reason_code = "epoch_mismatch",
            root_epoch = root_pointer.ecs_epoch.get(),
            manifest_epoch = manifest.ecs_epoch.get(),
            "bootstrap step 5 failed: root epoch != manifest epoch"
        );
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "root/manifest epoch mismatch (reason=epoch_mismatch): root={}, manifest={}",
                root_pointer.ecs_epoch.get(),
                manifest.ecs_epoch.get()
            ),
        });
    }
    info!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        step = 5_u8,
        duration_ms = 0_u64,
        root_epoch = root_pointer.ecs_epoch.get(),
        object_id = %root_pointer.manifest_object_id,
        "bootstrap step 5 complete"
    );

    info!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        step = 6_u8,
        commit_seq = manifest.commit_seq,
        "bootstrap step 6: verifying marker"
    );
    let latest_marker = fetch_marker_record(layout.markers_dir().as_path(), manifest.commit_seq)?;
    if latest_marker.marker_id != *manifest.current_commit.as_bytes() {
        error!(
            bead_id = ROOT_BOOTSTRAP_BEAD_ID,
            logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
            reason_code = "marker_mismatch",
            manifest_commit_seq = manifest.commit_seq,
            "bootstrap marker mismatch"
        );
        return Err(FrankenError::DatabaseCorrupt {
            detail:
                "root manifest current_commit does not match marker stream (reason=marker_mismatch)"
                    .to_owned(),
        });
    }
    info!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        step = 6_u8,
        duration_ms = 0_u64,
        root_epoch = root_pointer.ecs_epoch.get(),
        object_id = %manifest.current_commit,
        "bootstrap step 6 complete"
    );

    let schema_snapshot_bytes = fetch_object_payload(
        layout.symbols_dir().as_path(),
        manifest.schema_snapshot,
        root_pointer.ecs_epoch,
        auth_key,
    )?;
    info!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        step = 7_u8,
        duration_ms = 0_u64,
        root_epoch = root_pointer.ecs_epoch.get(),
        object_id = %manifest.schema_snapshot,
        "bootstrap step 7 complete"
    );
    let checkpoint_base_bytes = fetch_object_payload(
        layout.symbols_dir().as_path(),
        manifest.checkpoint_base,
        root_pointer.ecs_epoch,
        auth_key,
    )?;
    info!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        step = 8_u8,
        duration_ms = 0_u64,
        root_epoch = root_pointer.ecs_epoch.get(),
        object_id = %manifest.checkpoint_base,
        "bootstrap step 8 complete"
    );

    info!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        step = 9_u8,
        duration_ms = 0_u64,
        root_epoch = root_pointer.ecs_epoch.get(),
        commit_seq = manifest.commit_seq,
        schema_epoch = manifest.schema_epoch,
        "bootstrap sequence completed"
    );

    Ok(NativeBootstrapState {
        root_pointer,
        manifest,
        latest_marker,
        schema_snapshot_bytes,
        checkpoint_base_bytes,
    })
}

fn fetch_object_payload(
    symbols_dir: &Path,
    object_id: ObjectId,
    root_epoch: EpochId,
    auth_key: Option<&[u8; 32]>,
) -> Result<Vec<u8>> {
    let mut records = Vec::new();
    let segments = sorted_segment_paths(symbols_dir)?;
    debug!(
        bead_id = ROOT_BOOTSTRAP_BEAD_ID,
        logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
        root_epoch = root_epoch.get(),
        object_id = %object_id,
        segment_count = segments.len(),
        "fetching bootstrap object payload from symbol log"
    );

    for (_, segment_path) in segments {
        let scan = scan_symbol_segment(&segment_path)?;
        if scan.header.epoch_id > root_epoch.get() {
            error!(
                bead_id = ROOT_BOOTSTRAP_BEAD_ID,
                logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
                reason_code = "future_epoch",
                segment = %segment_path.display(),
                segment_epoch = scan.header.epoch_id,
                root_epoch = root_epoch.get(),
                "bootstrap rejected future-epoch segment"
            );
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "future-epoch segment rejected (reason=future_epoch): segment_epoch={}, root_epoch={}",
                    scan.header.epoch_id,
                    root_epoch.get()
                ),
            });
        }
        let epoch_key =
            auth_key.map(|key| derive_epoch_auth_key(key, EpochId::new(scan.header.epoch_id)));
        for row in scan.records {
            if row.record.object_id == object_id {
                verify_bootstrap_symbol_auth(
                    &row.record,
                    scan.header.epoch_id,
                    epoch_key.as_ref(),
                )?;
                records.push(row.record);
            }
        }
    }

    if records.is_empty() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("object {object_id} not found in symbol logs"),
        });
    }
    reconstruct_payload_from_source_symbols(records)
}

fn verify_bootstrap_symbol_auth(
    record: &SymbolRecord,
    segment_epoch: u64,
    epoch_key: Option<&EpochAuthKey>,
) -> Result<()> {
    let valid_auth = match epoch_key {
        Some(key) => record.auth_tag != [0_u8; 16] && record.verify_auth(key.as_bytes()),
        None => record.auth_tag == [0_u8; 16],
    };
    // SymbolRecord::verify_auth also serves auth-disabled callers and accepts
    // the zero sentinel. Bootstrap must enforce the selected mode in both
    // directions, including scan recovery when no valid root is available.
    if !valid_auth {
        error!(
            bead_id = ROOT_BOOTSTRAP_BEAD_ID,
            logging_standard = ROOT_BOOTSTRAP_LOGGING_STANDARD,
            reason_code = "auth_failed",
            object_id = %record.object_id,
            segment_epoch,
            esi = record.esi,
            symbol_auth_enabled = epoch_key.is_some(),
            "bootstrap symbol authentication failed"
        );
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "bootstrap symbol auth verification failed (reason=auth_failed): object={}, segment_epoch={segment_epoch}, esi={}",
                record.object_id, record.esi
            ),
        });
    }
    Ok(())
}

fn reconstruct_payload_from_source_symbols(mut records: Vec<SymbolRecord>) -> Result<Vec<u8>> {
    records.sort_unstable_by_key(|record| record.esi);
    let Some(first) = records.first() else {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "cannot reconstruct payload from empty symbol set".to_owned(),
        });
    };
    let first_oti = first.oti;
    let symbol_size_u64 = u64::from(first_oti.t);
    if symbol_size_u64 == 0 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "symbol_size=0 in OTI".to_owned(),
        });
    }
    if first_oti.z == 0 || first_oti.n == 0 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "source_blocks=0 or sub_blocks=0 in OTI".to_owned(),
        });
    }
    // Concatenation preserves object order only for one source block with one
    // sub-block. RFC 6330 sub-blocks need deinterleaving; multiple source blocks
    // additionally need a block identity that this bootstrap path does not carry.
    if first_oti.z != 1 || first_oti.n != 1 {
        return Err(FrankenError::NotImplemented(format!(
            "native bootstrap OTI layout Z={} N={}; only Z=1 N=1 is supported",
            first_oti.z, first_oti.n
        )));
    }

    let source_symbols = first_oti.f.div_ceil(symbol_size_u64);
    let symbol_size_usize = u32_to_usize(first_oti.t, "oti.t")?;
    let mut available_payload_bytes = 0_usize;
    // Validate repair records too, before deciding which ESIs contribute to
    // this systematic reconstruction. No allocation is based on OTI.f yet.
    for record in &records {
        if record.object_id != first.object_id {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "inconsistent object id across object symbols".to_owned(),
            });
        }
        if record.oti != first_oti {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "inconsistent OTI across object symbols".to_owned(),
            });
        }
        if record.symbol_data.len() != symbol_size_usize {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "symbol size does not match OTI.t".to_owned(),
            });
        }
        available_payload_bytes = checked_add(
            available_payload_bytes,
            record.symbol_data.len(),
            "available symbol payload bytes",
        )?;
    }

    // Complete unique source coverage bounds output by bytes already admitted.
    // Identical retries are harmless; conflicting duplicates are equivocation.
    let mut next_source_esi = 0_u64;
    let mut previous: Option<&SymbolRecord> = None;
    for record in &records {
        let esi = u64::from(record.esi);
        if esi >= source_symbols {
            break;
        }
        if esi == next_source_esi {
            next_source_esi += 1;
            previous = Some(record);
        } else if esi < next_source_esi {
            if previous.is_none_or(|prior| prior.symbol_data != record.symbol_data) {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("conflicting source symbols at ESI {esi}"),
                });
            }
        } else {
            break;
        }
    }
    if next_source_esi != source_symbols {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "insufficient source symbols to reconstruct object payload".to_owned(),
        });
    }

    let transfer_len_usize = u64_to_usize(first_oti.f, "oti.f")?;
    let total_bytes = u64_to_usize(source_symbols, "source_symbols")?
        .checked_mul(symbol_size_usize)
        .filter(|total| *total <= available_payload_bytes)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "reconstruction size exceeds available symbol payload bytes".to_owned(),
        })?;
    let mut out = Vec::new();
    out.try_reserve_exact(total_bytes)
        .map_err(|_| FrankenError::OutOfMemory)?;
    next_source_esi = 0;
    for record in &records {
        if next_source_esi < source_symbols && u64::from(record.esi) == next_source_esi {
            out.extend_from_slice(&record.symbol_data);
            next_source_esi += 1;
        }
    }
    out.truncate(transfer_len_usize);
    Ok(out)
}

fn fetch_marker_record(markers_dir: &Path, commit_seq: u64) -> Result<CommitMarkerRecord> {
    let segment_id = segment_id_for_commit_seq(commit_seq);
    let segment_path = markers_dir.join(format!("segment-{segment_id:06}.log"));
    let bytes = fs::read(&segment_path)?;
    if bytes.len() < MARKER_SEGMENT_HEADER_BYTES {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "marker segment {} shorter than header: {} bytes",
                segment_path.display(),
                bytes.len()
            ),
        });
    }
    let header =
        MarkerSegmentHeader::decode(&bytes[..MARKER_SEGMENT_HEADER_BYTES]).map_err(|err| {
            FrankenError::DatabaseCorrupt {
                detail: format!(
                    "marker header decode failed for {}: {err}",
                    segment_path.display()
                ),
            }
        })?;
    let records = recover_valid_prefix(&bytes).map_err(|err| FrankenError::DatabaseCorrupt {
        detail: format!(
            "marker segment recover failed for {}: {err}",
            segment_path.display()
        ),
    })?;

    if commit_seq < header.start_commit_seq {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "commit_seq {commit_seq} precedes segment start {}",
                header.start_commit_seq
            ),
        });
    }
    let index_u64 = commit_seq - header.start_commit_seq;
    let index = u64_to_usize(index_u64, "marker_index")?;
    let Some(record) = records.get(index) else {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "marker for commit_seq {commit_seq} missing in segment {}",
                segment_path.display()
            ),
        });
    };
    if !record.verify_marker_id() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "marker_id verification failed (reason=marker_mismatch)".to_owned(),
        });
    }
    if index > 0 {
        for i in 1..=index {
            if records[i].prev_marker_id != records[i - 1].marker_id {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("marker hash chain gap at index {i} (reason=marker_chain_gap)"),
                });
            }
        }
    }
    Ok(record.clone())
}

fn scan_latest_marker(markers_dir: &Path) -> Result<Option<CommitMarkerRecord>> {
    let segments = sorted_segment_paths(markers_dir)?;
    let mut best: Option<CommitMarkerRecord> = None;
    for (_, segment_path) in segments {
        let bytes = fs::read(&segment_path)?;
        if bytes.len() < MARKER_SEGMENT_HEADER_BYTES {
            continue;
        }
        let Ok(records) = recover_valid_prefix(&bytes) else {
            continue;
        };
        if let Some(last) = records.last() {
            let replace = best
                .as_ref()
                .is_none_or(|existing| last.commit_seq > existing.commit_seq);
            if replace {
                best = Some(last.clone());
            }
        }
    }
    Ok(best)
}

fn sorted_segment_paths(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name_os = entry.file_name();
        let Some(name) = name_os.to_str() else {
            continue;
        };
        let Some(segment_id) = parse_segment_id(name) else {
            continue;
        };
        out.push((segment_id, entry.path()));
    }
    out.sort_by_key(|(segment_id, _)| *segment_id);
    Ok(out)
}

fn parse_segment_id(name: &str) -> Option<u64> {
    let body = name.strip_prefix("segment-")?.strip_suffix(".log")?;
    body.parse::<u64>().ok()
}

fn read_object_id_at(bytes: &[u8], offset: usize, field: &str) -> Result<ObjectId> {
    let end = checked_add(offset, 16, field)?;
    if end > bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("{field} out of bounds: end={end}, len={}", bytes.len()),
        });
    }
    let mut raw = [0_u8; 16];
    raw.copy_from_slice(&bytes[offset..end]);
    Ok(ObjectId::from_bytes(raw))
}

fn read_u32_le_at(bytes: &[u8], offset: usize, field: &str) -> Result<u32> {
    let end = checked_add(offset, 4, field)?;
    if end > bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("{field} out of bounds: end={end}, len={}", bytes.len()),
        });
    }
    Ok(u32::from_le_bytes(
        bytes[offset..end].try_into().expect("fixed 4-byte field"),
    ))
}

fn read_u64_le_at(bytes: &[u8], offset: usize, field: &str) -> Result<u64> {
    let end = checked_add(offset, 8, field)?;
    if end > bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("{field} out of bounds: end={end}, len={}", bytes.len()),
        });
    }
    Ok(u64::from_le_bytes(
        bytes[offset..end].try_into().expect("fixed 8-byte field"),
    ))
}

fn u32_to_usize(value: u32, field: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| FrankenError::OutOfRange {
        what: field.to_owned(),
        value: value.to_string(),
    })
}

fn u64_to_usize(value: u64, field: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| FrankenError::OutOfRange {
        what: field.to_owned(),
        value: value.to_string(),
    })
}

fn checked_add(lhs: usize, rhs: usize, field: &str) -> Result<usize> {
    lhs.checked_add(rhs)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!("{field} overflow"),
        })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use crate::commit_marker::MarkerSegmentHeader;
    use crate::symbol_log::{SymbolSegmentHeader, append_symbol_record, ensure_symbol_segment};
    use fsqlite_types::{ObjectId, Oti, SymbolRecord, SymbolRecordFlags, SymbolValidityWindow};
    use tempfile::TempDir;

    use super::*;

    const BEAD_ID: &str = "bd-3go.12";

    // ── test_epoch_id_monotone ──────────────────────────────────────────

    #[test]
    fn test_epoch_id_monotone() {
        let clock = EpochClock::new(EpochId::ZERO);
        let mut prev = clock.current();
        for i in 0..100 {
            let next_result = clock.increment();
            assert!(
                next_result.is_ok(),
                "bead_id={BEAD_ID} case=epoch_monotone_increment_{i} err={next_result:?}"
            );
            let Ok(next) = next_result else {
                return;
            };
            assert!(
                next > prev,
                "bead_id={BEAD_ID} case=epoch_monotone prev={} next={}",
                prev.get(),
                next.get()
            );
            prev = next;
        }
        assert_eq!(
            clock.current().get(),
            100,
            "bead_id={BEAD_ID} case=epoch_monotone_final"
        );
    }

    // ── test_symbol_validity_window_rejects_future ──────────────────────

    #[test]
    fn test_symbol_validity_window_rejects_future() {
        let current = EpochId::new(5);
        let window = SymbolValidityWindow::default_window(current);
        let future = EpochId::new(6);
        assert!(
            !window.contains(future),
            "bead_id={BEAD_ID} case=validity_window_rejects_future"
        );
        let result = validate_symbol_epoch(future, &window);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=validity_window_future_epoch_error"
        );
    }

    // ── test_symbol_validity_window_accepts_past ────────────────────────

    #[test]
    fn test_symbol_validity_window_accepts_past() {
        let current = EpochId::new(10);
        let window = SymbolValidityWindow::default_window(current);
        for past in [0, 1, 5, 9, 10] {
            let epoch = EpochId::new(past);
            assert!(
                window.contains(epoch),
                "bead_id={BEAD_ID} case=validity_window_accepts_past epoch={past}"
            );
            let result = validate_symbol_epoch(epoch, &window);
            assert!(
                result.is_ok(),
                "bead_id={BEAD_ID} case=validity_window_past_epoch_ok epoch={past}"
            );
        }
    }

    // ── test_epoch_scoped_key_derivation ────────────────────────────────

    #[test]
    fn test_epoch_scoped_key_derivation() {
        let master_key = [0xAB_u8; 32];
        let key_5 = derive_epoch_auth_key(&master_key, EpochId::new(5));
        let key_6 = derive_epoch_auth_key(&master_key, EpochId::new(6));
        assert_ne!(
            key_5, key_6,
            "bead_id={BEAD_ID} case=epoch_keys_differ_across_epochs"
        );
        // Deterministic: same inputs produce the same key.
        let key_5_again = derive_epoch_auth_key(&master_key, EpochId::new(5));
        assert_eq!(
            key_5, key_5_again,
            "bead_id={BEAD_ID} case=epoch_key_deterministic"
        );
    }

    // ── test_epoch_key_derivation_domain_separation ─────────────────────

    #[test]
    fn test_epoch_key_derivation_domain_separation() {
        let dek = [0x42_u8; 32];
        let master_key = derive_master_key_from_dek(&dek);
        // Master key MUST differ from raw DEK (domain separation).
        assert_ne!(
            master_key, dek,
            "bead_id={BEAD_ID} case=master_key_differs_from_dek"
        );
        // Auth key for epoch 0 MUST differ from master key.
        let auth_key = derive_epoch_auth_key(&master_key, EpochId::ZERO);
        assert_ne!(
            auth_key.as_bytes(),
            &master_key,
            "bead_id={BEAD_ID} case=auth_key_differs_from_master"
        );
    }

    // ── test_epoch_transition_barrier_all_arrive ────────────────────────

    #[test]
    fn test_epoch_transition_barrier_all_arrive() {
        let clock = EpochClock::new(EpochId::new(5));
        let barrier = EpochBarrier::new(EpochId::new(5), 4);

        assert!(!barrier.arrive("WriteCoordinator"));
        assert!(!barrier.arrive("SymbolStore"));
        assert!(!barrier.arrive("Replicator"));
        assert!(barrier.arrive("CheckpointGc"));

        assert!(
            barrier.is_complete(),
            "bead_id={BEAD_ID} case=barrier_complete"
        );

        let outcome = barrier.resolve(&clock).expect("resolve must succeed");
        assert_eq!(
            outcome,
            BarrierOutcome::AllArrived {
                new_epoch: EpochId::new(6),
            },
            "bead_id={BEAD_ID} case=barrier_all_arrived_epoch_incremented"
        );
        assert_eq!(
            clock.current().get(),
            6,
            "bead_id={BEAD_ID} case=clock_advanced_after_barrier"
        );
    }

    // ── test_epoch_transition_barrier_timeout ───────────────────────────

    #[test]
    fn test_epoch_transition_barrier_timeout() {
        let clock = EpochClock::new(EpochId::new(5));
        let barrier = EpochBarrier::new(EpochId::new(5), 4);

        barrier.arrive("WriteCoordinator");
        barrier.arrive("SymbolStore");
        barrier.arrive("Replicator");
        // CheckpointGc does NOT arrive.

        let outcome = barrier.resolve(&clock).expect("resolve must succeed");
        assert_eq!(
            outcome,
            BarrierOutcome::Timeout {
                arrived: 3,
                expected: 4,
            },
            "bead_id={BEAD_ID} case=barrier_timeout_epoch_unchanged"
        );
        assert_eq!(
            clock.current().get(),
            5,
            "bead_id={BEAD_ID} case=clock_unchanged_after_timeout"
        );
    }

    // ── test_epoch_bootstrap_from_ecs_root ──────────────────────────────

    #[test]
    fn test_epoch_bootstrap_from_ecs_root() {
        // Before RootManifest is decoded, use EcsRootPointer.ecs_epoch as
        // provisional upper bound. Symbols with epoch_id > root_epoch must
        // be rejected.
        let root_epoch = EpochId::new(7);
        let window = SymbolValidityWindow::default_window(root_epoch);

        // Epoch 8 (future) → rejected.
        assert!(
            !window.contains(EpochId::new(8)),
            "bead_id={BEAD_ID} case=bootstrap_rejects_future"
        );
        // Epoch 7 (current) → accepted.
        assert!(
            window.contains(EpochId::new(7)),
            "bead_id={BEAD_ID} case=bootstrap_accepts_current"
        );
        // Epoch 0 (past) → accepted.
        assert!(
            window.contains(EpochId::ZERO),
            "bead_id={BEAD_ID} case=bootstrap_accepts_zero"
        );
    }

    // ── test_barrier_cancelled ──────────────────────────────────────────

    #[test]
    fn test_barrier_cancelled() {
        let clock = EpochClock::new(EpochId::new(3));
        let barrier = EpochBarrier::new(EpochId::new(3), 2);

        barrier.arrive("WriteCoordinator");
        barrier.cancel();

        let outcome = barrier.resolve(&clock).expect("resolve must succeed");
        assert_eq!(
            outcome,
            BarrierOutcome::Cancelled,
            "bead_id={BEAD_ID} case=barrier_cancelled_epoch_unchanged"
        );
        assert_eq!(
            clock.current().get(),
            3,
            "bead_id={BEAD_ID} case=clock_unchanged_after_cancel"
        );
    }

    // ── test_epoch_clock_store_and_recover ──────────────────────────────

    #[test]
    fn test_epoch_clock_store_and_recover() {
        let clock = EpochClock::new(EpochId::ZERO);
        clock.store(EpochId::new(42));
        assert_eq!(
            clock.current().get(),
            42,
            "bead_id={BEAD_ID} case=clock_store_recovery"
        );
        let next = clock.increment().expect("increment after store");
        assert_eq!(
            next.get(),
            43,
            "bead_id={BEAD_ID} case=clock_increment_after_store"
        );
    }

    // ── test_validity_window_boundary ───────────────────────────────────

    #[test]
    fn test_validity_window_boundary() {
        let window = SymbolValidityWindow::new(EpochId::new(3), EpochId::new(7));
        assert!(
            !window.contains(EpochId::new(2)),
            "bead_id={BEAD_ID} case=window_below_lower_bound"
        );
        assert!(
            window.contains(EpochId::new(3)),
            "bead_id={BEAD_ID} case=window_at_lower_bound"
        );
        assert!(
            window.contains(EpochId::new(5)),
            "bead_id={BEAD_ID} case=window_within_bounds"
        );
        assert!(
            window.contains(EpochId::new(7)),
            "bead_id={BEAD_ID} case=window_at_upper_bound"
        );
        assert!(
            !window.contains(EpochId::new(8)),
            "bead_id={BEAD_ID} case=window_above_upper_bound"
        );
    }

    const ROOT_BEAD_ID: &str = "bd-1hi.25";

    fn make_object_id(seed: u8) -> ObjectId {
        ObjectId::from_bytes([seed; 16])
    }

    fn test_master_key() -> [u8; 32] {
        [0xA5; 32]
    }

    fn create_layout() -> (TempDir, NativeBootstrapLayout) {
        let temp_dir = TempDir::new().expect("tempdir");
        let layout = NativeBootstrapLayout::new(temp_dir.path().join("ecs"));
        std::fs::create_dir_all(layout.symbols_dir()).expect("create symbols dir");
        std::fs::create_dir_all(layout.markers_dir()).expect("create markers dir");
        (temp_dir, layout)
    }

    fn write_single_symbol_object(
        symbols_dir: &Path,
        segment_id: u64,
        epoch_id: EpochId,
        object_id: ObjectId,
        payload: &[u8],
    ) {
        let header =
            SymbolSegmentHeader::new(segment_id, epoch_id.get(), 1_700_000_000 + segment_id);
        let segment_path = symbols_dir.join(format!("segment-{segment_id:06}.log"));
        ensure_symbol_segment(&segment_path, header).expect("ensure symbol segment");
        let symbol_size = u32::try_from(payload.len()).expect("payload fits u32");
        let oti = Oti {
            f: u64::from(symbol_size),
            al: 1,
            t: symbol_size,
            z: 1,
            n: 1,
        };
        let record = SymbolRecord::new(
            object_id,
            oti,
            0,
            payload.to_vec(),
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
        );
        append_symbol_record(symbols_dir, header, &record).expect("append symbol");
    }

    fn write_marker_segment(
        markers_dir: &Path,
        start_commit_seq: u64,
        records: &[CommitMarkerRecord],
    ) {
        let segment_id = segment_id_for_commit_seq(start_commit_seq);
        let header = MarkerSegmentHeader::new(segment_id, start_commit_seq);
        let mut bytes = Vec::from(header.encode());
        for record in records {
            bytes.extend_from_slice(&record.encode());
        }
        let segment_path = markers_dir.join(format!("segment-{segment_id:06}.log"));
        std::fs::write(segment_path, bytes).expect("write marker segment");
    }

    fn make_marker(commit_seq: u64, prev: [u8; 16], salt: u8) -> CommitMarkerRecord {
        CommitMarkerRecord::new(
            commit_seq,
            1_800_000_000_000_000_000 + commit_seq,
            [salt; 16],
            [salt.wrapping_add(1); 16],
            prev,
        )
    }

    fn make_manifest(
        database_name: &str,
        current_commit: ObjectId,
        commit_seq: u64,
        schema_snapshot: ObjectId,
        schema_epoch: u64,
        ecs_epoch: EpochId,
        checkpoint_base: ObjectId,
    ) -> RootManifest {
        RootManifest {
            database_name: database_name.to_owned(),
            current_commit,
            commit_seq,
            schema_snapshot,
            schema_epoch,
            ecs_epoch,
            checkpoint_base,
            gc_horizon: commit_seq,
            created_at: 1_800_000_000,
            updated_at: 1_800_000_123,
        }
    }

    fn must_err_contains<T: std::fmt::Debug>(result: Result<T>, needle: &str, case: &str) {
        let err = result.expect_err(case);
        let detail = err.to_string();
        assert!(
            detail.contains(needle),
            "bead_id={ROOT_BEAD_ID} case={case} expected_substring={needle} actual={detail}"
        );
    }

    fn write_bootstrap_objects(
        layout: &NativeBootstrapLayout,
        root_epoch: EpochId,
        manifest_id: ObjectId,
        manifest: &RootManifest,
        schema_payload: &[u8],
        checkpoint_payload: &[u8],
        markers: &[CommitMarkerRecord],
    ) {
        let manifest_bytes = manifest.encode().expect("encode manifest");
        write_single_symbol_object(
            layout.symbols_dir().as_path(),
            1,
            root_epoch,
            manifest_id,
            &manifest_bytes,
        );
        write_single_symbol_object(
            layout.symbols_dir().as_path(),
            1,
            root_epoch,
            manifest.schema_snapshot,
            schema_payload,
        );
        write_single_symbol_object(
            layout.symbols_dir().as_path(),
            1,
            root_epoch,
            manifest.checkpoint_base,
            checkpoint_payload,
        );
        if let Some(first) = markers.first() {
            write_marker_segment(layout.markers_dir().as_path(), first.commit_seq, markers);
        }
    }

    fn write_valid_bootstrap_fixture(
        layout: &NativeBootstrapLayout,
        root_epoch: EpochId,
    ) -> (ObjectId, RootManifest, CommitMarkerRecord, Vec<u8>, Vec<u8>) {
        let manifest_id = make_object_id(0x70);
        let schema_payload = b"schema-cache-v1".to_vec();
        let checkpoint_payload = b"checkpoint-cache-v1".to_vec();
        let marker = make_marker(0, [0_u8; 16], 0x71);
        let manifest = make_manifest(
            "db-valid",
            ObjectId::from_bytes(marker.marker_id),
            marker.commit_seq,
            make_object_id(0x72),
            1,
            root_epoch,
            make_object_id(0x73),
        );
        write_bootstrap_objects(
            layout,
            root_epoch,
            manifest_id,
            &manifest,
            &schema_payload,
            &checkpoint_payload,
            std::slice::from_ref(&marker),
        );
        (
            manifest_id,
            manifest,
            marker,
            schema_payload,
            checkpoint_payload,
        )
    }

    fn rewrite_bootstrap_fixture_segment(
        layout: &NativeBootstrapLayout,
        segment_epoch: u64,
        mut transform: impl FnMut(SymbolRecord) -> SymbolRecord,
    ) {
        let path = layout.symbols_dir().join("segment-000001.log");
        let scan = scan_symbol_segment(&path).expect("scan fixture segment");
        let mut header = scan.header;
        header.epoch_id = segment_epoch;
        let mut bytes = header.encode().to_vec();
        for row in scan.records {
            bytes.extend_from_slice(&transform(row.record).to_bytes());
        }
        fs::write(path, bytes).expect("rewrite fixture segment");
    }

    fn write_authenticated_bootstrap_fixture(
        layout: &NativeBootstrapLayout,
        root_epoch: EpochId,
        segment_epoch: EpochId,
        master_key: &[u8; 32],
    ) -> NativeBootstrapState {
        let (manifest_id, manifest, marker, schema, checkpoint) =
            write_valid_bootstrap_fixture(layout, root_epoch);
        let epoch_key = derive_epoch_auth_key(master_key, segment_epoch);
        rewrite_bootstrap_fixture_segment(layout, segment_epoch.get(), |record| {
            record.with_auth_tag(epoch_key.as_bytes())
        });
        NativeBootstrapState {
            root_pointer: EcsRootPointer::authed(manifest_id, root_epoch, master_key),
            manifest,
            latest_marker: marker,
            schema_snapshot_bytes: schema,
            checkpoint_base_bytes: checkpoint,
        }
    }

    fn bootstrap_disk_snapshot(layout: &NativeBootstrapLayout) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut snapshot = BTreeMap::new();
        let mut directories = vec![layout.ecs_dir.clone()];
        while let Some(directory) = directories.pop() {
            for entry in fs::read_dir(directory).expect("read fixture directory") {
                let entry = entry.expect("fixture entry");
                if entry.file_type().expect("fixture file type").is_dir() {
                    directories.push(entry.path());
                } else {
                    snapshot.insert(entry.path(), fs::read(entry.path()).expect("fixture bytes"));
                }
            }
        }
        snapshot
    }

    fn assert_bootstrap_auth_rejected_without_writes(
        layout: &NativeBootstrapLayout,
        key: Option<&[u8; 32]>,
        case: &str,
    ) {
        let before = bootstrap_disk_snapshot(layout);
        for recover in [false, true] {
            let result = if recover {
                bootstrap_native_mode_with_recovery(layout, true, key)
            } else {
                bootstrap_native_mode(layout, true, key)
            };
            let error = result.expect_err(case);
            assert!(
                matches!(&error, FrankenError::DatabaseCorrupt { detail } if detail.contains("auth_failed")),
                "case={case} recover={recover} error={error}"
            );
            assert_eq!(
                bootstrap_disk_snapshot(layout),
                before,
                "case={case} recover={recover}"
            );
            eprintln!(
                "bead_id=bd-3mgq5.4 case={case} recover={recover} result=rejected reason=auth_failed disk_unchanged=true"
            );
        }
    }

    #[test]
    fn test_native_bootstrap_authenticated_past_epoch_objects() {
        let (_tmp, layout) = create_layout();
        let key = test_master_key();
        let expected =
            write_authenticated_bootstrap_fixture(&layout, EpochId::new(7), EpochId::new(3), &key);
        write_root_pointer_atomic(&layout.root_path(), expected.root_pointer).expect("write root");
        let before = bootstrap_disk_snapshot(&layout);
        assert_eq!(
            bootstrap_native_mode(&layout, true, Some(&key)).expect("direct open"),
            expected
        );
        assert_eq!(
            bootstrap_native_mode_with_recovery(&layout, true, Some(&key))
                .expect("open with recovery"),
            expected
        );
        assert_eq!(bootstrap_disk_snapshot(&layout), before);
        eprintln!(
            "bead_id=bd-3mgq5.4 case=past_epoch root_epoch=7 segment_epoch=3 authenticated_objects=3 disk_unchanged=true"
        );
    }

    #[test]
    fn test_native_bootstrap_root_auth_failure_is_terminal() {
        for case in [
            "wrong_key",
            "missing_key",
            "tampered_root_tag",
            "zero_root_tag",
        ] {
            let (_tmp, layout) = create_layout();
            let key = test_master_key();
            let expected = write_authenticated_bootstrap_fixture(
                &layout,
                EpochId::new(7),
                EpochId::new(7),
                &key,
            );
            let mut bytes = expected.root_pointer.encode();
            if case == "tampered_root_tag" {
                bytes[40] ^= 1;
            } else if case == "zero_root_tag" {
                bytes[40..].fill(0);
            }
            fs::write(layout.root_path(), bytes).expect("write root");
            let bad_key = [0x5A_u8; 32];
            let supplied_key = match case {
                "wrong_key" => Some(&bad_key),
                "missing_key" => None,
                _ => Some(&key),
            };
            assert_bootstrap_auth_rejected_without_writes(&layout, supplied_key, case);
        }
    }

    #[test]
    fn test_native_bootstrap_recovery_cannot_downgrade_authenticated_root() {
        for root_state in ["valid", "missing", "torn", "checksum", "unsigned"] {
            let (_tmp, layout) = create_layout();
            let key = test_master_key();
            let expected = write_authenticated_bootstrap_fixture(
                &layout,
                EpochId::new(7),
                EpochId::new(7),
                &key,
            );
            let scan = scan_symbol_segment(&layout.symbols_dir().join("segment-000001.log"))
                .expect("signed fixture segment");
            let epoch_key = derive_epoch_auth_key(&key, EpochId::new(scan.header.epoch_id));
            assert!(!scan.records.is_empty());
            assert!(scan.records.iter().all(|row| {
                row.record.auth_tag != [0_u8; 16] && row.record.verify_auth(epoch_key.as_bytes())
            }));
            let mut bytes = expected.root_pointer.encode();
            match root_state {
                "valid" => fs::write(layout.root_path(), bytes).expect("root"),
                "missing" => {}
                "torn" => fs::write(layout.root_path(), &bytes[..17]).expect("torn root"),
                "checksum" => {
                    bytes[12] ^= 1;
                    fs::write(layout.root_path(), bytes).expect("checksum-corrupt root");
                }
                "unsigned" => write_root_pointer_atomic(
                    &layout.root_path(),
                    EcsRootPointer::unauthed(
                        expected.root_pointer.manifest_object_id,
                        expected.root_pointer.ecs_epoch,
                    ),
                )
                .expect("unsigned root with signed body"),
                _ => unreachable!(),
            }
            let before = bootstrap_disk_snapshot(&layout);
            let result = bootstrap_native_mode_with_recovery(&layout, false, None);
            let after = bootstrap_disk_snapshot(&layout);
            eprintln!(
                "bead_id=bd-3mgq5.4 case=auth_off_recovery root_state={root_state} signed_records={} open_succeeded={} disk_unchanged={}",
                scan.records.len(),
                result.is_ok(),
                after == before
            );
            let error = result.expect_err("auth-disabled recovery cannot downgrade signed data");
            assert!(
                matches!(&error, FrankenError::DatabaseCorrupt { detail }
                    if detail.contains("symbol_auth=off") || detail.contains("auth_failed")),
                "root_state={root_state} error={error}"
            );
            assert_eq!(after, before, "root_state={root_state}");
            if root_state == "valid" {
                assert_eq!(
                    bootstrap_native_mode(&layout, true, Some(&key)).expect("authenticated reopen"),
                    expected
                );
            }
        }
    }

    #[test]
    fn test_native_bootstrap_authenticates_every_referenced_object() {
        for object in ["manifest", "schema", "checkpoint"] {
            for damage in ["zero_tag", "wrong_tag", "changed_payload"] {
                let (_tmp, layout) = create_layout();
                let key = test_master_key();
                let expected = write_authenticated_bootstrap_fixture(
                    &layout,
                    EpochId::new(7),
                    EpochId::new(7),
                    &key,
                );
                write_root_pointer_atomic(&layout.root_path(), expected.root_pointer)
                    .expect("root");
                let object_id = match object {
                    "manifest" => expected.root_pointer.manifest_object_id,
                    "schema" => expected.manifest.schema_snapshot,
                    _ => expected.manifest.checkpoint_base,
                };
                rewrite_bootstrap_fixture_segment(&layout, 7, |mut record| {
                    if record.object_id == object_id {
                        match damage {
                            "zero_tag" => record.auth_tag = [0_u8; 16],
                            "wrong_tag" => record.auth_tag[0] ^= 1,
                            _ => {
                                let old_tag = record.auth_tag;
                                record.symbol_data[0] ^= 1;
                                record = SymbolRecord::new(
                                    record.object_id,
                                    record.oti,
                                    record.esi,
                                    record.symbol_data,
                                    record.flags,
                                );
                                record.auth_tag = old_tag;
                            }
                        }
                    }
                    // Even payload tampering has a valid unkeyed frame checksum.
                    assert!(record.verify_integrity());
                    record
                });
                assert_bootstrap_auth_rejected_without_writes(
                    &layout,
                    Some(&key),
                    &format!("{object}_{damage}"),
                );
            }
        }
    }

    #[test]
    fn test_native_bootstrap_recovers_only_authenticated_root_candidates() {
        for damage in ["missing", "torn", "checksum"] {
            let (_tmp, layout) = create_layout();
            let key = test_master_key();
            let expected = write_authenticated_bootstrap_fixture(
                &layout,
                EpochId::new(7),
                EpochId::new(3),
                &key,
            );
            if damage == "torn" {
                fs::write(layout.root_path(), &expected.root_pointer.encode()[..17])
                    .expect("torn root");
            } else if damage == "checksum" {
                let mut bytes = expected.root_pointer.encode();
                bytes[12] ^= 1;
                fs::write(layout.root_path(), bytes).expect("corrupt checksum");
            }
            let before = bootstrap_disk_snapshot(&layout);
            for supplied_key in [None, Some(&[0x5A_u8; 32])] {
                let error = bootstrap_native_mode_with_recovery(&layout, true, supplied_key)
                    .expect_err("recovery must authenticate candidates");
                assert!(
                    matches!(&error, FrankenError::DatabaseCorrupt { detail } if detail.contains("auth_failed"))
                );
                assert_eq!(bootstrap_disk_snapshot(&layout), before, "case={damage}");
            }
            let recovered = bootstrap_native_mode_with_recovery(&layout, true, Some(&key))
                .expect("authenticated recovery");
            assert_eq!(recovered, expected);
            assert_eq!(
                read_root_pointer(&layout.root_path(), true, Some(&key)).expect("persisted root"),
                expected.root_pointer
            );
            assert_eq!(
                bootstrap_native_mode(&layout, true, Some(&key)).expect("reopen recovered root"),
                expected
            );
            eprintln!(
                "bead_id=bd-3mgq5.4 case={damage} bad_keys_rejected=2 result=recovered authenticated_objects=3"
            );
        }
    }

    #[test]
    fn test_native_bootstrap_recovery_rejects_unsigned_candidates() {
        let (_tmp, layout) = create_layout();
        let key = test_master_key();
        write_valid_bootstrap_fixture(&layout, EpochId::new(7));
        fs::write(layout.root_path(), [0_u8; 9]).expect("torn root");
        let before = bootstrap_disk_snapshot(&layout);
        let error = bootstrap_native_mode_with_recovery(&layout, true, Some(&key))
            .expect_err("unsigned manifest cannot be blessed by recovery");
        assert!(
            matches!(&error, FrankenError::DatabaseCorrupt { detail } if detail.contains("auth_failed"))
        );
        assert_eq!(bootstrap_disk_snapshot(&layout), before);
    }

    #[test]
    fn test_native_bootstrap_segment_epoch_is_bound_to_authentication() {
        let (_tmp, layout) = create_layout();
        let key = test_master_key();
        let expected =
            write_authenticated_bootstrap_fixture(&layout, EpochId::new(7), EpochId::new(3), &key);
        write_root_pointer_atomic(&layout.root_path(), expected.root_pointer).expect("write root");
        // Reframe the same valid records under another allowed epoch. The
        // segment checksum is recomputed, but the epoch-3 MACs must fail.
        rewrite_bootstrap_fixture_segment(&layout, 4, |record| record);
        assert_bootstrap_auth_rejected_without_writes(
            &layout,
            Some(&key),
            "relabeled_segment_epoch",
        );
    }

    #[test]
    fn test_native_bootstrap_authenticated_future_epoch_is_rejected() {
        let (_tmp, layout) = create_layout();
        let key = test_master_key();
        let expected =
            write_authenticated_bootstrap_fixture(&layout, EpochId::new(7), EpochId::new(8), &key);
        write_root_pointer_atomic(&layout.root_path(), expected.root_pointer).expect("root");
        let before = bootstrap_disk_snapshot(&layout);
        for recover in [false, true] {
            let result = if recover {
                bootstrap_native_mode_with_recovery(&layout, true, Some(&key))
            } else {
                bootstrap_native_mode(&layout, true, Some(&key))
            };
            let error = result.expect_err("authentication cannot override the epoch upper bound");
            assert!(
                matches!(&error, FrankenError::DatabaseCorrupt { detail } if detail.contains("future_epoch"))
            );
            assert_eq!(bootstrap_disk_snapshot(&layout), before);
        }
        fs::write(layout.root_path(), [0_u8; 9]).expect("torn root");
        let torn_before = bootstrap_disk_snapshot(&layout);
        let error = bootstrap_native_mode_with_recovery(&layout, true, Some(&key))
            .expect_err("scan must validate the recovered manifest's epoch bound");
        assert!(
            matches!(&error, FrankenError::DatabaseCorrupt { detail } if detail.contains("future_epoch"))
        );
        assert_eq!(bootstrap_disk_snapshot(&layout), torn_before);
    }

    #[test]
    fn test_native_bootstrap_root_format_and_io_errors_are_terminal() {
        for case in ["version", "magic", "oversized", "directory"] {
            let (_tmp, layout) = create_layout();
            let key = test_master_key();
            let expected = write_authenticated_bootstrap_fixture(
                &layout,
                EpochId::new(7),
                EpochId::new(7),
                &key,
            );
            let mut bytes = expected.root_pointer.encode();
            match case {
                "version" => {
                    bytes[4..8].copy_from_slice(&2_u32.to_le_bytes());
                    let checksum = xxhash_rust::xxh3::xxh3_64(&bytes[..32]);
                    bytes[32..40].copy_from_slice(&checksum.to_le_bytes());
                    let tag = compute_root_pointer_auth_tag(&key, &bytes[..40]);
                    bytes[40..].copy_from_slice(&tag);
                    fs::write(layout.root_path(), bytes).expect("unknown root version");
                }
                "magic" => {
                    bytes[0] = b'X';
                    fs::write(layout.root_path(), bytes).expect("wrong root format");
                }
                "oversized" => {
                    let mut file = fs::File::create(layout.root_path()).expect("oversized root");
                    file.write_all(&bytes).expect("root prefix");
                    // Sparse file: the reader must inspect only the fixed-size
                    // prefix, without allocating this declared length.
                    file.set_len(4 * 1024 * 1024)
                        .expect("large sparse root length");
                }
                _ => fs::create_dir(layout.root_path()).expect("root is a directory"),
            }
            // Do not snapshot the deliberately huge sparse root into memory.
            let error = bootstrap_native_mode_with_recovery(&layout, true, Some(&key))
                .expect_err("format and I/O failures cannot trigger root replacement");
            if case == "directory" {
                assert!(matches!(error, FrankenError::Io(_)));
                assert!(layout.root_path().is_dir());
            } else {
                assert!(matches!(&error, FrankenError::DatabaseCorrupt { .. }));
                let mut prefix = [0_u8; ECS_ROOT_POINTER_BYTES];
                fs::File::open(layout.root_path())
                    .expect("root still present")
                    .read_exact(&mut prefix)
                    .expect("read retained root");
                assert_eq!(prefix, bytes);
                if case == "oversized" {
                    assert_eq!(
                        fs::metadata(layout.root_path()).expect("root length").len(),
                        4 * 1024 * 1024
                    );
                    assert!(error.to_string().contains("got 57"), "{error}");
                }
            }
            assert!(
                fs::read_dir(&layout.ecs_dir)
                    .expect("ecs entries")
                    .all(|entry| !entry
                        .expect("entry")
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".root.tmp."))
            );
            eprintln!("bead_id=bd-3mgq5.4 case={case} result=terminal root_preserved=true");
        }
    }

    #[test]
    fn test_native_bootstrap_authenticated_manifest_epoch_failure_is_terminal() {
        let (_tmp, layout) = create_layout();
        let key = test_master_key();
        let mut expected =
            write_authenticated_bootstrap_fixture(&layout, EpochId::new(7), EpochId::new(7), &key);
        write_root_pointer_atomic(&layout.root_path(), expected.root_pointer).expect("root");
        expected.manifest.ecs_epoch = EpochId::new(8);
        let payload = expected.manifest.encode().expect("mismatched manifest");
        let epoch_key = derive_epoch_auth_key(&key, EpochId::new(7));
        rewrite_bootstrap_fixture_segment(&layout, 7, |record| {
            if record.object_id == expected.root_pointer.manifest_object_id {
                SymbolRecord::new(
                    record.object_id,
                    record.oti,
                    record.esi,
                    payload.clone(),
                    record.flags,
                )
                .with_auth_tag(epoch_key.as_bytes())
            } else {
                record
            }
        });
        let before = bootstrap_disk_snapshot(&layout);
        let error = bootstrap_native_mode_with_recovery(&layout, true, Some(&key))
            .expect_err("must not rewrite a verified root to match a mismatched manifest");
        assert!(
            matches!(&error, FrankenError::DatabaseCorrupt { detail } if detail.contains("epoch_mismatch"))
        );
        assert_eq!(bootstrap_disk_snapshot(&layout), before);
    }

    #[test]
    fn test_native_bootstrap_recovery_validates_body_before_root_publication() {
        let (_tmp, layout) = create_layout();
        let key = test_master_key();
        let mut expected =
            write_authenticated_bootstrap_fixture(&layout, EpochId::new(7), EpochId::new(7), &key);
        expected.manifest.checkpoint_base = make_object_id(0xF1);
        let payload = expected
            .manifest
            .encode()
            .expect("manifest references absent checkpoint");
        let epoch_key = derive_epoch_auth_key(&key, EpochId::new(7));
        rewrite_bootstrap_fixture_segment(&layout, 7, |record| {
            if record.object_id == expected.root_pointer.manifest_object_id {
                SymbolRecord::new(
                    record.object_id,
                    record.oti,
                    record.esi,
                    payload.clone(),
                    record.flags,
                )
                .with_auth_tag(epoch_key.as_bytes())
            } else {
                record
            }
        });
        fs::write(layout.root_path(), [0_u8; 9]).expect("torn previous root");
        let before = bootstrap_disk_snapshot(&layout);
        let error = bootstrap_native_mode_with_recovery(&layout, true, Some(&key))
            .expect_err("candidate body must validate before publishing repaired root");
        assert!(
            matches!(&error, FrankenError::DatabaseCorrupt { detail } if detail.contains("not found in symbol logs"))
        );
        assert_eq!(bootstrap_disk_snapshot(&layout), before);
    }

    fn reconstruction_symbol(f: u64, t: u32, esi: u32, payload: &[u8]) -> SymbolRecord {
        SymbolRecord::new(
            make_object_id(0xA5),
            Oti {
                f,
                al: 1,
                t,
                z: 1,
                n: 1,
            },
            esi,
            payload.to_vec(),
            SymbolRecordFlags::empty(),
        )
    }

    #[test]
    fn test_reconstruction_bounds_declared_length_before_allocation() {
        let record = reconstruction_symbol(u64::MAX, 1, 0, &[0xA5]);
        let error = reconstruct_payload_from_source_symbols(vec![record])
            .expect_err("one byte cannot admit an unbounded transfer length");
        assert!(matches!(&error, FrankenError::DatabaseCorrupt { detail }
            if detail.contains("insufficient source symbols")));
        eprintln!(
            "bead_id=bd-3mgq5.4 case=reconstruction_bound declared_bytes={} admitted_payload_bytes=1 result=insufficient_sources",
            u64::MAX
        );
    }

    #[test]
    fn test_reconstruction_reorders_deduplicates_and_truncates_sources() {
        let first = reconstruction_symbol(5, 2, 0, &[1, 2]);
        let second = reconstruction_symbol(5, 2, 1, &[3, 4]);
        let third = reconstruction_symbol(5, 2, 2, &[5, 99]);
        let repair = reconstruction_symbol(5, 2, 9, &[80, 81]);
        let mut repeated = second.clone();
        repeated.flags = SymbolRecordFlags::SYSTEMATIC_RUN_START;
        let payload =
            reconstruct_payload_from_source_symbols(vec![repair, third, repeated, first, second])
                .expect("complete unique sources with an identical retry");
        assert_eq!(payload, [1, 2, 3, 4, 5]);
        eprintln!(
            "bead_id=bd-3mgq5.4 case=reconstruction_complete source_symbols=3 identical_duplicates=1 repair_symbols=1 output_bytes=5"
        );
    }

    #[test]
    fn test_reconstruction_rejects_missing_and_conflicting_sources() {
        for case in ["missing_first", "gap", "missing_last", "equivocation"] {
            let mut records = vec![
                reconstruction_symbol(5, 2, 0, &[1, 2]),
                reconstruction_symbol(5, 2, 1, &[3, 4]),
                reconstruction_symbol(5, 2, 2, &[5, 99]),
            ];
            match case {
                "missing_first" => {
                    records.remove(0);
                }
                "gap" => {
                    records.remove(1);
                }
                "missing_last" => {
                    records.remove(2);
                }
                "equivocation" => records.push(reconstruction_symbol(5, 2, 1, &[3, 9])),
                _ => unreachable!(),
            }
            records.push(reconstruction_symbol(5, 2, 8, &[80, 81]));
            let error = reconstruct_payload_from_source_symbols(records)
                .expect_err("repairs and conflicting duplicates cannot supply missing sources");
            let expected = if case == "equivocation" {
                "conflicting source symbols"
            } else {
                "insufficient source symbols"
            };
            assert!(
                matches!(&error, FrankenError::DatabaseCorrupt { detail }
                if detail.contains(expected)),
                "case={case} error={error}"
            );
            eprintln!("bead_id=bd-3mgq5.4 case={case} result=rejected reason={expected}");
        }
    }

    #[test]
    fn test_reconstruction_validates_source_and_repair_metadata() {
        for esi in [0, 9] {
            for damage in ["oti", "payload_length", "object_id"] {
                let source = reconstruction_symbol(2, 2, 0, &[1, 2]);
                let bad = match damage {
                    "oti" => reconstruction_symbol(3, 2, esi, &[1, 2]),
                    "payload_length" => reconstruction_symbol(2, 2, esi, &[1]),
                    "object_id" => SymbolRecord::new(
                        make_object_id(0xA6),
                        source.oti,
                        esi,
                        vec![1, 2],
                        SymbolRecordFlags::empty(),
                    ),
                    _ => unreachable!(),
                };
                assert!(
                    bad.verify_integrity(),
                    "fixture has a valid unkeyed checksum"
                );
                let error = reconstruct_payload_from_source_symbols(vec![source, bad])
                    .expect_err("all records must have consistent metadata before allocation");
                let expected = match damage {
                    "oti" => "inconsistent OTI",
                    "payload_length" => "symbol size does not match",
                    "object_id" => "inconsistent object id",
                    _ => unreachable!(),
                };
                assert!(
                    matches!(&error, FrankenError::DatabaseCorrupt { detail }
                    if detail.contains(expected)),
                    "esi={esi} damage={damage} error={error}"
                );
                eprintln!(
                    "bead_id=bd-3mgq5.4 case=reconstruction_metadata esi={esi} damage={damage} result=rejected"
                );
            }
        }
    }

    #[test]
    fn test_reconstruction_empty_and_zero_length_boundaries() {
        let empty = reconstruct_payload_from_source_symbols(Vec::new())
            .expect_err("no symbols means no OTI");
        assert!(matches!(&empty, FrankenError::DatabaseCorrupt { detail }
            if detail.contains("empty symbol set")));
        let zero_size =
            reconstruct_payload_from_source_symbols(vec![reconstruction_symbol(0, 0, 0, &[])])
                .expect_err("zero symbol size is invalid even for an empty object");
        assert!(
            matches!(&zero_size, FrankenError::DatabaseCorrupt { detail }
            if detail.contains("symbol_size=0"))
        );
        assert_eq!(
            reconstruct_payload_from_source_symbols(vec![reconstruction_symbol(0, 2, 0, &[1, 2])])
                .expect("zero transfer length with valid metadata"),
            Vec::<u8>::new()
        );
        let short =
            reconstruct_payload_from_source_symbols(vec![reconstruction_symbol(0, 2, 0, &[1])])
                .expect_err("zero transfer length does not bypass record validation");
        assert!(matches!(&short, FrankenError::DatabaseCorrupt { detail }
            if detail.contains("symbol size does not match")));
    }

    #[test]
    fn test_native_bootstrap_rejects_unsupported_symbol_layouts_without_writes() {
        let original: Vec<u8> = (0..16).collect();
        // RFC 6330 N=2 source symbols interleave two contiguous sub-blocks.
        let subblock_symbols = [[0, 1, 2, 3, 8, 9, 10, 11], [4, 5, 6, 7, 12, 13, 14, 15]];
        assert_ne!(subblock_symbols.concat(), original);
        for (z, n) in [(1, 2), (2, 1)] {
            for target in ["schema", "candidate"] {
                for root_state in ["signed", "missing", "torn"] {
                    // A direct open need not inspect an unreferenced object.
                    if target == "candidate" && root_state == "signed" {
                        continue;
                    }
                    let (_tmp, layout) = create_layout();
                    let key = test_master_key();
                    let epoch = EpochId::new(7);
                    let expected =
                        write_authenticated_bootstrap_fixture(&layout, epoch, epoch, &key);
                    let object_id = if target == "schema" {
                        expected.manifest.schema_snapshot
                    } else {
                        // Keep a fully readable older manifest, schema and
                        // checkpoint. Recovery must not skip this candidate and
                        // publish that older root just because decoding is absent.
                        make_object_id(0x74)
                    };
                    let path = layout.symbols_dir().join("segment-000001.log");
                    let scan = scan_symbol_segment(&path).expect("fixture segment");
                    let mut bytes = scan.header.encode().to_vec();
                    for row in scan.records {
                        if row.record.object_id != object_id {
                            bytes.extend_from_slice(&row.record.to_bytes());
                        }
                    }
                    let payloads = if n == 2 {
                        subblock_symbols
                    } else {
                        [[0, 1, 2, 3, 4, 5, 6, 7], [8, 9, 10, 11, 12, 13, 14, 15]]
                    };
                    let epoch_key = derive_epoch_auth_key(&key, epoch);
                    for (index, payload) in payloads.into_iter().enumerate() {
                        let oti = Oti {
                            f: 16,
                            al: 4,
                            t: 8,
                            z,
                            n,
                        };
                        // ESI is local to each source block. The current FSEC
                        // envelope carries no separate block identity for Z=2.
                        let esi = if z == 1 {
                            u32::try_from(index).expect("two source symbols")
                        } else {
                            0
                        };
                        let record = SymbolRecord::new(
                            object_id,
                            oti,
                            esi,
                            payload.to_vec(),
                            SymbolRecordFlags::empty(),
                        )
                        .with_auth_tag(epoch_key.as_bytes());
                        assert!(record.verify_integrity());
                        assert!(record.verify_auth(epoch_key.as_bytes()));
                        bytes.extend_from_slice(&record.to_bytes());
                    }
                    fs::write(path, bytes).expect("write authenticated layout fixture");
                    let root_bytes = expected.root_pointer.encode();
                    match root_state {
                        "signed" => fs::write(layout.root_path(), root_bytes).expect("root"),
                        "missing" => {}
                        "torn" => {
                            fs::write(layout.root_path(), &root_bytes[..17]).expect("torn root")
                        }
                        _ => unreachable!(),
                    }
                    let before = bootstrap_disk_snapshot(&layout);
                    let result = bootstrap_native_mode_with_recovery(&layout, true, Some(&key));
                    let error =
                        result.expect_err("unsupported layout must not produce object bytes");
                    assert!(
                        matches!(&error, FrankenError::NotImplemented(detail)
                            if detail.contains(&format!("Z={z} N={n}"))),
                        "target={target} root_state={root_state} error={error}"
                    );
                    assert_eq!(bootstrap_disk_snapshot(&layout), before);
                    eprintln!(
                        "bead_id=bd-3mgq5.4 case=unsupported_layout z={z} n={n} target={target} root_state={root_state} authenticated_symbols=2 naive_misordered={} result=unsupported disk_unchanged=true",
                        n == 2
                    );
                }
            }
        }
    }

    fn reconstruction_symbol(f: u64, t: u32, esi: u32, payload: &[u8]) -> SymbolRecord {
        SymbolRecord::new(
            make_object_id(0xA5),
            Oti {
                f,
                al: 1,
                t,
                z: 1,
                n: 1,
            },
            esi,
            payload.to_vec(),
            SymbolRecordFlags::empty(),
        )
    }

    #[test]
    fn test_reconstruction_bounds_declared_length_before_allocation() {
        let record = reconstruction_symbol(u64::MAX, 1, 0, &[0xA5]);
        let error = reconstruct_payload_from_source_symbols(vec![record])
            .expect_err("one byte cannot admit an unbounded transfer length");
        assert!(matches!(&error, FrankenError::DatabaseCorrupt { detail }
            if detail.contains("insufficient source symbols")));
        eprintln!(
            "bead_id=bd-3mgq5.4 case=reconstruction_bound declared_bytes={} admitted_payload_bytes=1 result=insufficient_sources",
            u64::MAX
        );
    }

    #[test]
    fn test_reconstruction_reorders_deduplicates_and_truncates_sources() {
        let first = reconstruction_symbol(5, 2, 0, &[1, 2]);
        let second = reconstruction_symbol(5, 2, 1, &[3, 4]);
        let third = reconstruction_symbol(5, 2, 2, &[5, 99]);
        let repair = reconstruction_symbol(5, 2, 9, &[80, 81]);
        let mut repeated = second.clone();
        repeated.flags = SymbolRecordFlags::SYSTEMATIC_RUN_START;
        let payload =
            reconstruct_payload_from_source_symbols(vec![repair, third, repeated, first, second])
                .expect("complete unique sources with an identical retry");
        assert_eq!(payload, [1, 2, 3, 4, 5]);
        eprintln!(
            "bead_id=bd-3mgq5.4 case=reconstruction_complete source_symbols=3 identical_duplicates=1 repair_symbols=1 output_bytes=5"
        );
    }

    #[test]
    fn test_reconstruction_rejects_missing_and_conflicting_sources() {
        for case in ["missing_first", "gap", "missing_last", "equivocation"] {
            let mut records = vec![
                reconstruction_symbol(5, 2, 0, &[1, 2]),
                reconstruction_symbol(5, 2, 1, &[3, 4]),
                reconstruction_symbol(5, 2, 2, &[5, 99]),
            ];
            match case {
                "missing_first" => {
                    records.remove(0);
                }
                "gap" => {
                    records.remove(1);
                }
                "missing_last" => {
                    records.remove(2);
                }
                "equivocation" => records.push(reconstruction_symbol(5, 2, 1, &[3, 9])),
                _ => unreachable!(),
            }
            records.push(reconstruction_symbol(5, 2, 8, &[80, 81]));
            let error = reconstruct_payload_from_source_symbols(records)
                .expect_err("repairs and conflicting duplicates cannot supply missing sources");
            let expected = if case == "equivocation" {
                "conflicting source symbols"
            } else {
                "insufficient source symbols"
            };
            assert!(
                matches!(&error, FrankenError::DatabaseCorrupt { detail }
                if detail.contains(expected)),
                "case={case} error={error}"
            );
            eprintln!("bead_id=bd-3mgq5.4 case={case} result=rejected reason={expected}");
        }
    }

    #[test]
    fn test_reconstruction_validates_source_and_repair_metadata() {
        for esi in [0, 9] {
            for damage in ["oti", "payload_length", "object_id"] {
                let source = reconstruction_symbol(2, 2, 0, &[1, 2]);
                let mut bad = match damage {
                    "oti" => reconstruction_symbol(3, 2, esi, &[1, 2]),
                    "payload_length" => reconstruction_symbol(2, 2, esi, &[1, 2]),
                    "object_id" => SymbolRecord::new(
                        make_object_id(0xA6),
                        source.oti,
                        esi,
                        vec![1, 2],
                        SymbolRecordFlags::empty(),
                    ),
                    _ => unreachable!(),
                };
                assert!(
                    bad.verify_integrity(),
                    "fixture has a valid unkeyed checksum"
                );
                if damage == "payload_length" {
                    // The public constructor and checksum verifier require an
                    // exact payload length. Damage the valid in-memory record
                    // afterwards to exercise reconstruction's own length guard.
                    bad.symbol_data.truncate(1);
                    assert_eq!(bad.symbol_data.len(), 1);
                    assert_eq!(bad.oti.t, 2);
                }
                let error = reconstruct_payload_from_source_symbols(vec![source, bad])
                    .expect_err("all records must have consistent metadata before allocation");
                let expected = match damage {
                    "oti" => "inconsistent OTI",
                    "payload_length" => "symbol size does not match",
                    "object_id" => "inconsistent object id",
                    _ => unreachable!(),
                };
                assert!(
                    matches!(&error, FrankenError::DatabaseCorrupt { detail }
                    if detail.contains(expected)),
                    "esi={esi} damage={damage} error={error}"
                );
                eprintln!(
                    "bead_id=bd-3mgq5.4 case=reconstruction_metadata esi={esi} damage={damage} result=rejected"
                );
            }
        }
    }

    #[test]
    fn test_reconstruction_empty_and_zero_length_boundaries() {
        let empty = reconstruct_payload_from_source_symbols(Vec::new())
            .expect_err("no symbols means no OTI");
        assert!(matches!(&empty, FrankenError::DatabaseCorrupt { detail }
            if detail.contains("empty symbol set")));
        let zero_size =
            reconstruct_payload_from_source_symbols(vec![reconstruction_symbol(0, 0, 0, &[])])
                .expect_err("zero symbol size is invalid even for an empty object");
        assert!(
            matches!(&zero_size, FrankenError::DatabaseCorrupt { detail }
            if detail.contains("symbol_size=0"))
        );
        assert_eq!(
            reconstruct_payload_from_source_symbols(vec![reconstruction_symbol(0, 2, 0, &[1, 2])])
                .expect("zero transfer length with valid metadata"),
            Vec::<u8>::new()
        );
        let mut short_record = reconstruction_symbol(0, 2, 0, &[1, 2]);
        assert!(short_record.verify_integrity());
        short_record.symbol_data.truncate(1);
        let short = reconstruct_payload_from_source_symbols(vec![short_record])
            .expect_err("zero transfer length does not bypass record validation");
        assert!(matches!(&short, FrankenError::DatabaseCorrupt { detail }
            if detail.contains("symbol size does not match")));
        for (z, n) in [(0, 1), (1, 0), (0, 0)] {
            let mut zero_layout = reconstruction_symbol(0, 2, 0, &[1, 2]);
            zero_layout.oti.z = z;
            zero_layout.oti.n = n;
            let error = reconstruct_payload_from_source_symbols(vec![zero_layout])
                .expect_err("zero layout dimensions are malformed");
            assert!(matches!(&error, FrankenError::DatabaseCorrupt { detail }
                if detail.contains("source_blocks=0 or sub_blocks=0")));
        }
    }

    #[test]
    fn test_ecs_root_pointer_encode_decode() {
        let pointer = EcsRootPointer::unauthed(make_object_id(0x11), EpochId::new(7));
        let encoded = pointer.encode();
        assert_eq!(encoded.len(), ECS_ROOT_POINTER_BYTES);
        let decoded = EcsRootPointer::decode(&encoded, false, None).expect("decode root pointer");
        assert_eq!(
            decoded, pointer,
            "bead_id={ROOT_BEAD_ID} case=root_roundtrip"
        );
    }

    #[test]
    fn test_ecs_root_pointer_magic() {
        let pointer = EcsRootPointer::unauthed(make_object_id(0x22), EpochId::new(3));
        let mut encoded = pointer.encode();
        encoded[0] = b'X';
        let result = EcsRootPointer::decode(&encoded, false, None);
        assert!(
            result.is_err(),
            "bead_id={ROOT_BEAD_ID} case=root_bad_magic"
        );
    }

    #[test]
    fn test_ecs_root_pointer_checksum_tamper() {
        let pointer = EcsRootPointer::unauthed(make_object_id(0x33), EpochId::new(9));
        let mut encoded = pointer.encode();
        encoded[9] ^= 0xFF;
        let result = EcsRootPointer::decode(&encoded, false, None);
        assert!(
            result.is_err(),
            "bead_id={ROOT_BEAD_ID} case=root_checksum_tamper"
        );
    }

    #[test]
    fn test_root_auth_tag_verification() {
        let key = test_master_key();
        let pointer = EcsRootPointer::authed(make_object_id(0x44), EpochId::new(12), &key);
        let encoded = pointer.encode();
        let decoded =
            EcsRootPointer::decode(&encoded, true, Some(&key)).expect("auth decode succeeds");
        assert_eq!(decoded, pointer);

        let mut tampered = encoded;
        tampered[40] ^= 0x01;
        let result = EcsRootPointer::decode(&tampered, true, Some(&key));
        assert!(
            result.is_err(),
            "bead_id={ROOT_BEAD_ID} case=root_auth_tamper"
        );
    }

    #[test]
    fn test_root_auth_tag_zero_when_off() {
        let pointer = build_root_pointer(make_object_id(0x55), EpochId::new(2), false, None)
            .expect("build unauthed root");
        assert_eq!(pointer.root_auth_tag, [0_u8; 16]);
        let decoded =
            EcsRootPointer::decode(&pointer.encode(), false, None).expect("decode off mode");
        assert_eq!(decoded.root_auth_tag, [0_u8; 16]);
    }

    #[test]
    fn test_root_manifest_encode_decode() {
        let manifest = make_manifest(
            "db-main",
            make_object_id(0x10),
            5,
            make_object_id(0x20),
            3,
            EpochId::new(7),
            make_object_id(0x30),
        );
        let encoded = manifest.encode().expect("encode manifest");
        let decoded = RootManifest::decode(&encoded).expect("decode manifest");
        assert_eq!(decoded, manifest);
    }

    #[test]
    fn test_root_manifest_magic() {
        let manifest = make_manifest(
            "db-main",
            make_object_id(0x10),
            5,
            make_object_id(0x20),
            3,
            EpochId::new(7),
            make_object_id(0x30),
        );
        let mut encoded = manifest.encode().expect("encode manifest");
        encoded[0] = b'X';
        assert!(
            RootManifest::decode(&encoded).is_err(),
            "bead_id={ROOT_BEAD_ID} case=manifest_bad_magic"
        );
    }

    #[test]
    fn test_bootstrap_step_4_epoch_guard() {
        let (_tmp, layout) = create_layout();
        let manifest_id = make_object_id(0x66);
        let schema_id = make_object_id(0x67);
        let checkpoint_id = make_object_id(0x68);
        let marker = make_marker(0, [0_u8; 16], 0x60);
        let manifest = make_manifest(
            "future-segment",
            ObjectId::from_bytes(marker.marker_id),
            marker.commit_seq,
            schema_id,
            1,
            EpochId::new(3),
            checkpoint_id,
        );
        let manifest_bytes = manifest.encode().expect("encode manifest");
        write_single_symbol_object(
            layout.symbols_dir().as_path(),
            1,
            EpochId::new(4),
            manifest_id,
            &manifest_bytes,
        );
        let pointer =
            build_root_pointer(manifest_id, EpochId::new(3), false, None).expect("build root");
        write_root_pointer_atomic(&layout.root_path(), pointer).expect("write root");

        let result = bootstrap_native_mode(&layout, false, None);
        assert!(
            result.is_err(),
            "bead_id={ROOT_BEAD_ID} case=future_epoch_guard"
        );
    }

    #[test]
    fn test_bootstrap_step_5_epoch_invariant() {
        let (_tmp, layout) = create_layout();
        let root_epoch = EpochId::new(7);
        let manifest_id = make_object_id(0x74);
        let marker = make_marker(0, [0_u8; 16], 0x75);
        let manifest = make_manifest(
            "epoch-mismatch",
            ObjectId::from_bytes(marker.marker_id),
            marker.commit_seq,
            make_object_id(0x76),
            1,
            EpochId::new(8),
            make_object_id(0x77),
        );
        write_bootstrap_objects(
            &layout,
            root_epoch,
            manifest_id,
            &manifest,
            b"schema",
            b"checkpoint",
            &[marker],
        );
        let pointer = build_root_pointer(manifest_id, root_epoch, false, None).expect("root");
        write_root_pointer_atomic(&layout.root_path(), pointer).expect("write root");
        must_err_contains(
            bootstrap_native_mode(&layout, false, None),
            "epoch_mismatch",
            "bootstrap_step_5_epoch_invariant",
        );
    }

    #[test]
    fn test_bootstrap_step_6_marker_verification() {
        let (_tmp, layout) = create_layout();
        let root_epoch = EpochId::new(9);
        let manifest_id = make_object_id(0x78);
        let marker = make_marker(0, [0_u8; 16], 0x79);
        let manifest = make_manifest(
            "marker-mismatch",
            make_object_id(0x7A),
            marker.commit_seq,
            make_object_id(0x7B),
            1,
            root_epoch,
            make_object_id(0x7C),
        );
        write_bootstrap_objects(
            &layout,
            root_epoch,
            manifest_id,
            &manifest,
            b"schema",
            b"checkpoint",
            &[marker],
        );
        let pointer = build_root_pointer(manifest_id, root_epoch, false, None).expect("root");
        write_root_pointer_atomic(&layout.root_path(), pointer).expect("write root");
        must_err_contains(
            bootstrap_native_mode(&layout, false, None),
            "marker_mismatch",
            "bootstrap_step_6_marker_verification",
        );
    }

    #[test]
    fn test_bootstrap_full_sequence() {
        let (_tmp, layout) = create_layout();
        let root_epoch = EpochId::new(11);
        let (manifest_id, manifest, marker, schema_payload, checkpoint_payload) =
            write_valid_bootstrap_fixture(&layout, root_epoch);
        let pointer = build_root_pointer(manifest_id, root_epoch, false, None).expect("root");
        write_root_pointer_atomic(&layout.root_path(), pointer).expect("write root");

        let state = bootstrap_native_mode(&layout, false, None).expect("bootstrap ok");
        assert_eq!(state.root_pointer, pointer);
        assert_eq!(state.manifest, manifest);
        assert_eq!(state.latest_marker, marker);
        assert_eq!(state.schema_snapshot_bytes, schema_payload);
        assert_eq!(state.checkpoint_base_bytes, checkpoint_payload);
    }

    #[test]
    fn test_bootstrap_corrupted_root_recovery() {
        let (_tmp, layout) = create_layout();
        let root_epoch = EpochId::new(12);
        let (_manifest_id, manifest, marker, schema_payload, checkpoint_payload) =
            write_valid_bootstrap_fixture(&layout, root_epoch);
        fs::write(layout.root_path(), [0xFF_u8; 7]).expect("write corrupt root");

        let recovered = bootstrap_native_mode_with_recovery(&layout, false, None).expect("recover");
        assert_eq!(recovered.manifest, manifest);
        assert_eq!(recovered.latest_marker, marker);
        assert_eq!(recovered.schema_snapshot_bytes, schema_payload);
        assert_eq!(recovered.checkpoint_base_bytes, checkpoint_payload);
        let persisted =
            read_root_pointer(&layout.root_path(), false, None).expect("read recovered");
        assert_eq!(
            persisted.manifest_object_id,
            recovered.root_pointer.manifest_object_id
        );
        assert_eq!(persisted.ecs_epoch, recovered.root_pointer.ecs_epoch);
    }

    #[test]
    fn test_crash_safe_root_update() {
        let (_tmp, layout) = create_layout();
        let pointer_a = EcsRootPointer::unauthed(make_object_id(0x80), EpochId::new(1));
        let pointer_b = EcsRootPointer::unauthed(make_object_id(0x81), EpochId::new(2));
        write_root_pointer_atomic(&layout.root_path(), pointer_a).expect("write A");
        write_root_pointer_atomic(&layout.root_path(), pointer_b).expect("write B");
        let decoded = read_root_pointer(&layout.root_path(), false, None).expect("decode");
        assert_eq!(
            decoded, pointer_b,
            "bead_id={ROOT_BEAD_ID} case=root_atomic_swap"
        );
        let entries = fs::read_dir(layout.ecs_dir.as_path()).expect("list ecs dir");
        for entry in entries {
            let entry = entry.expect("entry");
            let name = entry.file_name();
            let name = name.to_string_lossy();
            assert!(
                !name.starts_with(".root.tmp."),
                "bead_id={ROOT_BEAD_ID} case=temp_root_file_leaked file={name}"
            );
        }
    }

    #[test]
    fn prop_root_pointer_roundtrip() {
        let key = test_master_key();
        for seed in [0_u8, 1, 17, 99, 255] {
            for epoch in [0_u64, 1, 2, 17, 255, 4_096, 1 << 20] {
                let id = make_object_id(seed);
                let plain = EcsRootPointer::unauthed(id, EpochId::new(epoch));
                let plain_roundtrip =
                    EcsRootPointer::decode(&plain.encode(), false, None).expect("plain decode");
                assert_eq!(plain_roundtrip, plain);
                let authed = EcsRootPointer::authed(id, EpochId::new(epoch), &key);
                let authed_roundtrip = EcsRootPointer::decode(&authed.encode(), true, Some(&key))
                    .expect("auth decode");
                assert_eq!(authed_roundtrip, authed);
            }
        }
    }

    #[test]
    fn test_bootstrap_rejects_marker_chain_gap() {
        let (_tmp, layout) = create_layout();
        let root_epoch = EpochId::new(13);
        let manifest_id = make_object_id(0x82);
        let m0 = make_marker(0, [0_u8; 16], 0x83);
        let m1 = make_marker(1, m0.marker_id, 0x84);
        let m2 = make_marker(2, [0xEE_u8; 16], 0x85);
        let manifest = make_manifest(
            "marker-gap",
            ObjectId::from_bytes(m2.marker_id),
            m2.commit_seq,
            make_object_id(0x86),
            1,
            root_epoch,
            make_object_id(0x87),
        );
        write_bootstrap_objects(
            &layout,
            root_epoch,
            manifest_id,
            &manifest,
            b"schema-gap",
            b"checkpoint-gap",
            &[m0, m1, m2],
        );
        let pointer = build_root_pointer(manifest_id, root_epoch, false, None).expect("root");
        write_root_pointer_atomic(&layout.root_path(), pointer).expect("write root");
        must_err_contains(
            bootstrap_native_mode(&layout, false, None),
            "marker_chain_gap",
            "bootstrap_rejects_marker_chain_gap",
        );
    }

    #[test]
    fn test_bootstrap_schema_snapshot_loads() {
        let (_tmp, layout) = create_layout();
        let root_epoch = EpochId::new(14);
        let (manifest_id, _manifest, _marker, schema_payload, _checkpoint_payload) =
            write_valid_bootstrap_fixture(&layout, root_epoch);
        let pointer = build_root_pointer(manifest_id, root_epoch, false, None).expect("root");
        write_root_pointer_atomic(&layout.root_path(), pointer).expect("write root");
        let state = bootstrap_native_mode(&layout, false, None).expect("bootstrap");
        assert_eq!(state.schema_snapshot_bytes, schema_payload);
    }

    #[test]
    fn test_bootstrap_checkpoint_base_warms_cache() {
        let (_tmp, layout) = create_layout();
        let root_epoch = EpochId::new(15);
        let (manifest_id, _manifest, _marker, _schema_payload, checkpoint_payload) =
            write_valid_bootstrap_fixture(&layout, root_epoch);
        let pointer = build_root_pointer(manifest_id, root_epoch, false, None).expect("root");
        write_root_pointer_atomic(&layout.root_path(), pointer).expect("write root");
        let state = bootstrap_native_mode(&layout, false, None).expect("bootstrap");
        assert_eq!(state.checkpoint_base_bytes, checkpoint_payload);
    }

    #[test]
    fn test_bootstrap_happy_path_from_root() {
        test_bootstrap_full_sequence();
    }

    #[test]
    fn test_bootstrap_corrupt_root_pointer_recovers_by_scan() {
        test_bootstrap_corrupted_root_recovery();
    }

    #[test]
    fn test_bootstrap_root_auth_mismatch_fails() {
        let (_tmp, layout) = create_layout();
        let root_epoch = EpochId::new(16);
        let (manifest_id, _manifest, _marker, _schema_payload, _checkpoint_payload) =
            write_valid_bootstrap_fixture(&layout, root_epoch);
        let good_key = test_master_key();
        let bad_key = [0x5A_u8; 32];
        let pointer =
            build_root_pointer(manifest_id, root_epoch, true, Some(&good_key)).expect("root");
        write_root_pointer_atomic(&layout.root_path(), pointer).expect("write root");
        must_err_contains(
            bootstrap_native_mode(&layout, true, Some(&bad_key)),
            "auth_failed",
            "bootstrap_root_auth_mismatch_fails",
        );
    }

    #[test]
    fn test_bootstrap_root_pointer_corrupt_checksum_fails_then_scan() {
        let (_tmp, layout) = create_layout();
        let root_epoch = EpochId::new(17);
        let (manifest_id, _manifest, _marker, _schema_payload, _checkpoint_payload) =
            write_valid_bootstrap_fixture(&layout, root_epoch);
        let pointer = build_root_pointer(manifest_id, root_epoch, false, None).expect("root");
        let mut root_bytes = pointer.encode();
        root_bytes[10] ^= 0xAA;
        fs::write(layout.root_path(), root_bytes).expect("write corrupt root");
        let state = bootstrap_native_mode_with_recovery(&layout, false, None).expect("recovered");
        assert_eq!(state.root_pointer.manifest_object_id, manifest_id);
        assert_eq!(state.root_pointer.ecs_epoch, root_epoch);
    }

    #[test]
    fn test_e2e_native_mode_open_close_reopen() {
        let (_tmp, layout) = create_layout();
        let root_epoch = EpochId::new(18);
        let (manifest_id, manifest, marker, _schema_payload, _checkpoint_payload) =
            write_valid_bootstrap_fixture(&layout, root_epoch);
        let pointer = build_root_pointer(manifest_id, root_epoch, false, None).expect("root");
        write_root_pointer_atomic(&layout.root_path(), pointer).expect("write root");

        let first_open = bootstrap_native_mode(&layout, false, None).expect("first open");
        let second_open = bootstrap_native_mode(&layout, false, None).expect("second open");
        assert_eq!(first_open.manifest, manifest);
        assert_eq!(first_open.latest_marker, marker);
        assert_eq!(
            second_open.manifest.current_commit,
            first_open.manifest.current_commit
        );
        assert_eq!(second_open.root_pointer, first_open.root_pointer);

        fs::write(layout.root_path(), [0_u8; 9]).expect("corrupt root");
        let recovered = bootstrap_native_mode_with_recovery(&layout, false, None).expect("reopen");
        assert_eq!(
            recovered.manifest.current_commit,
            first_open.manifest.current_commit
        );
        assert_eq!(
            recovered.manifest.commit_seq,
            first_open.manifest.commit_seq
        );
    }

    #[test]
    fn test_e2e_bootstrap_cold_start() {
        test_bootstrap_full_sequence();
    }

    #[test]
    fn test_e2e_bootstrap_after_crash() {
        test_bootstrap_root_pointer_corrupt_checksum_fails_then_scan();
    }

    #[test]
    fn test_e2e_bootstrap_schema_migration() {
        let (_tmp, layout) = create_layout();
        let root_epoch = EpochId::new(19);
        let manifest_id_v1 = make_object_id(0x88);
        let marker0 = make_marker(0, [0_u8; 16], 0x89);
        let marker1 = make_marker(1, marker0.marker_id, 0x8A);
        write_marker_segment(
            layout.markers_dir().as_path(),
            0,
            &[marker0, marker1.clone()],
        );

        let schema_v1 = make_object_id(0x8B);
        let schema_v2 = make_object_id(0x8C);
        let checkpoint_id = make_object_id(0x8D);
        let manifest_v1 = make_manifest(
            "schema-v1",
            ObjectId::from_bytes(marker1.marker_id),
            1,
            schema_v1,
            1,
            root_epoch,
            checkpoint_id,
        );
        let manifest_v2 = make_manifest(
            "schema-v2",
            ObjectId::from_bytes(marker1.marker_id),
            1,
            schema_v2,
            2,
            root_epoch,
            checkpoint_id,
        );
        let manifest_id_v2 = make_object_id(0x8E);
        write_single_symbol_object(
            layout.symbols_dir().as_path(),
            1,
            root_epoch,
            manifest_id_v1,
            &manifest_v1.encode().expect("manifest v1"),
        );
        write_single_symbol_object(
            layout.symbols_dir().as_path(),
            1,
            root_epoch,
            manifest_id_v2,
            &manifest_v2.encode().expect("manifest v2"),
        );
        write_single_symbol_object(
            layout.symbols_dir().as_path(),
            1,
            root_epoch,
            schema_v1,
            b"schema-v1",
        );
        write_single_symbol_object(
            layout.symbols_dir().as_path(),
            1,
            root_epoch,
            schema_v2,
            b"schema-v2",
        );
        write_single_symbol_object(
            layout.symbols_dir().as_path(),
            1,
            root_epoch,
            checkpoint_id,
            b"checkpoint",
        );

        let pointer_v2 = build_root_pointer(manifest_id_v2, root_epoch, false, None).expect("root");
        write_root_pointer_atomic(&layout.root_path(), pointer_v2).expect("write root");
        let state = bootstrap_native_mode(&layout, false, None).expect("bootstrap");
        assert_eq!(state.manifest.schema_epoch, 2);
        assert_eq!(state.schema_snapshot_bytes, b"schema-v2".to_vec());
    }

    #[test]
    fn test_ecs_root_pointer_checksum_roundtrip() {
        test_ecs_root_pointer_encode_decode();
    }

    #[test]
    fn test_ecs_root_pointer_auth_tag_verifies() {
        test_root_auth_tag_verification();
    }

    #[test]
    fn test_bootstrap_future_epoch_guard() {
        test_bootstrap_step_4_epoch_guard();
    }

    #[test]
    fn test_root_manifest_epoch_must_match_root_pointer() {
        test_bootstrap_step_5_epoch_invariant();
    }

    #[test]
    fn test_bootstrap_commit_marker_matches_current_commit() {
        test_bootstrap_step_6_marker_verification();
    }

    #[test]
    fn test_bd_1hi_25_unit_compliance_gate() {
        assert_eq!(ROOT_BOOTSTRAP_BEAD_ID, "bd-1hi.25");
        assert_eq!(ROOT_BOOTSTRAP_LOGGING_STANDARD, "bd-1fpm");
        assert_eq!(ECS_ROOT_POINTER_MAGIC, *b"FSRT");
        assert_eq!(ROOT_MANIFEST_MAGIC, *b"FSQLROOT");
    }

    #[test]
    fn prop_bd_1hi_25_structure_compliance() {
        for name in ["segment-000001.log", "segment-999999.log"] {
            assert!(
                parse_segment_id(name).is_some(),
                "bead_id={ROOT_BEAD_ID} case=parse_segment_id_valid name={name}"
            );
        }
        for name in ["segment.log", "segment-aa.log", "other-000001.log"] {
            assert!(
                parse_segment_id(name).is_none(),
                "bead_id={ROOT_BEAD_ID} case=parse_segment_id_invalid name={name}"
            );
        }
    }

    #[test]
    fn test_e2e_bd_1hi_25_compliance() {
        test_e2e_native_mode_open_close_reopen();
    }
}
