//! Lifetime binding between an opened database and its pathname namespace.
//!
//! Native file VFSes use two persistent sidecars.  The `gate` lock serializes
//! admission, while the `use` lock is shared by every connection bound to the
//! same file identity.  A new generation may replace the identity record only
//! while it owns `use` exclusively.  Reserved-empty bootstrap retains both
//! locks exclusively until [`DatabaseNamespaceBinding::finish_bootstrap`].
//! Sidecars are deliberately never unlinked for ordinary database lifetimes:
//! unlinking a locked file would split the advisory-lock domain on Unix. The
//! sole exception is [`cleanup_abandoned_private_database`], which is limited
//! to a caller-reserved transient candidate after all pager bindings have
//! closed and requires exclusive ownership of both namespace locks.
//!
//! This is a cooperative, trusted-parent protocol.  Native processes that
//! bypass FrankenSQLite can ignore advisory locks, and Unix permits a raw
//! unlink/rename despite an open descriptor.  Callers must not mutate the
//! database namespace or these sidecars outside the library while a binding
//! is live, and must not open one database through multiple hard-link aliases.

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use advisory_lock::{AdvisoryFileLock, FileLockError, FileLockMode};
use fsqlite_error::{FrankenError, Result};

use crate::traits::FileIdentity;

const GATE_SUFFIX: &str = "-fsqlite-ns-gate";
const USE_SUFFIX: &str = "-fsqlite-ns-use";
const RECORD_MAGIC: [u8; 8] = *b"FSQLNS01";
const RECORD_VERSION: u8 = 1;
const IDENTITY_BYTES: usize = 25;
const RECORD_BYTES: usize = 40;
const TRANSITION_MAGIC: [u8; 8] = *b"FSQLNT01";
const TRANSITION_VERSION: u8 = 1;
const TRANSITION_BYTES: usize = 88;
const TRANSITION_CHECKSUM_OFFSET: usize = 80;
const PREPARE_MAGIC: [u8; 8] = *b"FSQLNP01";
const PREPARE_VERSION: u8 = 1;
const PREPARE_BYTES: usize = TRANSITION_BYTES;
const PREPARE_CHECKSUM_OFFSET: usize = TRANSITION_CHECKSUM_OFFSET;
const FINISH_MAGIC: [u8; 8] = *b"FSQLNF01";
const FINISH_VERSION: u8 = 1;
const FINISH_BYTES: usize = TRANSITION_BYTES;
const FINISH_CHECKSUM_OFFSET: usize = TRANSITION_CHECKSUM_OFFSET;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Admission mode for a database namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamespaceOpenIntent {
    /// Join the live generation, or establish a new shared generation when no
    /// connection currently owns the namespace.
    Shared,
    /// Join an existing generation without creating or rewriting namespace
    /// records. Missing or malformed records fail closed.
    ReadOnlyExisting,
    /// Exclusively reserve the namespace through empty-database bootstrap.
    ReservedExclusive,
}

/// Durable result of an exact namespace-generation transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamespaceGenerationTransitionOutcome {
    /// This call durably published the replacement identity.
    Published,
    /// The same exact old-to-replacement request was already published.
    AlreadyPublished,
}

/// Exclusive namespace lease spanning caller-owned generation replacement.
///
/// The guard owns both persistent namespace locks and leaves a durable
/// fail-closed prepare marker until [`Self::finish`] succeeds. It may publish
/// more than one replacement while held, which permits an exact `A -> B`
/// activation followed by an exact `B -> A` rollback before admissions resume.
#[derive(Debug)]
pub struct DatabaseNamespaceGenerationTransition {
    stable_path: PathBuf,
    gate: Option<File>,
    use_file: Option<File>,
    current_identity: FileIdentity,
    last_sequence: u64,
    prepare_offset: u64,
    append_offset: u64,
    interrupted_tail: Vec<u8>,
    finished: bool,
    poisoned: bool,
}

#[derive(Debug)]
enum PendingLease {
    NewShared {
        gate: File,
        use_file: File,
    },
    JoinShared {
        gate: File,
        use_file: File,
        generation_identity: FileIdentity,
    },
    BootstrapExclusive {
        gate: File,
        use_file: File,
    },
    /// GH#140 / bd-daqmp: read-only admission of a database that no
    /// FrankenSQLite ever admitted (no namespace sidecars exist, e.g. a stock
    /// SQLite file). Nothing is created, opened, or locked — the reader
    /// behaves like an external stock process. A namespace created by a peer
    /// AFTER this admission cannot coordinate with it, which is identical to
    /// the peer's exposure to any non-FrankenSQLite reader.
    ReadOnlyUnadmitted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NamespaceBindMode {
    PreserveRecord,
    ReplaceQuiescentRecord,
}

/// Admission guard held while the caller opens and verifies the main file.
///
/// Dropping this value at any error point releases every acquired lock.
#[derive(Debug)]
pub struct PendingNamespaceOpen {
    stable_path: PathBuf,
    lease: Option<PendingLease>,
}

impl PendingNamespaceOpen {
    /// Begin namespace admission for an already-resolved absolute database
    /// path.  This operation is non-blocking; lock contention returns BUSY.
    pub fn begin(stable_path: &Path, intent: NamespaceOpenIntent) -> Result<Self> {
        validate_stable_path(stable_path)?;
        let (gate, mut use_file) = if intent == NamespaceOpenIntent::ReadOnlyExisting {
            // GH#140 / bd-daqmp: a read-only open must be byte-neutral for the
            // whole file family. When the namespace sidecars do not exist (a
            // database never admitted by FrankenSQLite), creating them here
            // would make a read-only open side-effecting, so admit
            // sidecar-less instead. Absence is checked explicitly; any OTHER
            // sidecar-open failure (malformed, permissions) still fails
            // closed through `open_existing_secure_lock_file` below.
            let gate_path = sidecar_path(stable_path, GATE_SUFFIX);
            let use_path = sidecar_path(stable_path, USE_SUFFIX);
            let sidecar_missing = |path: &Path| {
                matches!(
                    std::fs::metadata(path),
                    Err(ref error) if error.kind() == std::io::ErrorKind::NotFound
                )
            };
            if sidecar_missing(&gate_path) || sidecar_missing(&use_path) {
                return Ok(Self {
                    stable_path: stable_path.to_owned(),
                    lease: Some(PendingLease::ReadOnlyUnadmitted),
                });
            }
            (
                open_existing_secure_lock_file(&gate_path)?,
                open_existing_secure_lock_file(&use_path)?,
            )
        } else {
            (
                open_secure_lock_file(&sidecar_path(stable_path, GATE_SUFFIX))?,
                open_secure_lock_file(&sidecar_path(stable_path, USE_SUFFIX))?,
            )
        };
        let gate_mode = if intent == NamespaceOpenIntent::ReadOnlyExisting {
            FileLockMode::Shared
        } else {
            FileLockMode::Exclusive
        };
        try_lock(&gate, gate_mode)?;

        let lease = match intent {
            NamespaceOpenIntent::ReservedExclusive => {
                if let Err(error) = try_lock(&use_file, FileLockMode::Exclusive) {
                    release_namespace_locks(&gate, &use_file);
                    return Err(error);
                }
                PendingLease::BootstrapExclusive { gate, use_file }
            }
            NamespaceOpenIntent::Shared => {
                match AdvisoryFileLock::try_lock(&use_file, FileLockMode::Exclusive) {
                    Ok(()) => PendingLease::NewShared { gate, use_file },
                    Err(FileLockError::AlreadyLocked) => {
                        if let Err(error) = try_lock(&use_file, FileLockMode::Shared) {
                            release_namespace_locks(&gate, &use_file);
                            return Err(error);
                        }
                        let generation_identity =
                            match read_identity_record(&mut use_file, stable_path) {
                                Ok(identity) => identity,
                                Err(error) => {
                                    release_namespace_locks(&gate, &use_file);
                                    return Err(error);
                                }
                            };
                        PendingLease::JoinShared {
                            gate,
                            use_file,
                            generation_identity,
                        }
                    }
                    Err(FileLockError::Io(error)) => {
                        release_namespace_locks(&gate, &use_file);
                        return Err(error.into());
                    }
                }
            }
            NamespaceOpenIntent::ReadOnlyExisting => {
                if let Err(error) = try_lock(&use_file, FileLockMode::Shared) {
                    release_namespace_locks(&gate, &use_file);
                    return Err(error);
                }
                let generation_identity = match read_identity_record(&mut use_file, stable_path) {
                    Ok(identity) => identity,
                    Err(error) => {
                        release_namespace_locks(&gate, &use_file);
                        return Err(error);
                    }
                };
                PendingLease::JoinShared {
                    gate,
                    use_file,
                    generation_identity,
                }
            }
        };

        Ok(Self {
            stable_path: stable_path.to_owned(),
            lease: Some(lease),
        })
    }

    /// Identity of the live generation this admission must join.  When this
    /// returns `Some`, callers must strip CREATE/EXCLUSIVE and open that exact
    /// existing identity before calling [`Self::bind`].
    #[must_use]
    pub fn expected_identity(&self) -> Option<FileIdentity> {
        match self.lease.as_ref() {
            Some(PendingLease::JoinShared {
                generation_identity,
                ..
            }) => Some(*generation_identity),
            _ => None,
        }
    }

    /// Whether this admission exclusively owns a nonempty namespace record.
    ///
    /// `true` identifies the only state in which a caller may need
    /// [`Self::bind_replacing_quiescent_record`]. New namespaces have an empty
    /// record; joined/live namespaces are never reported as quiescent.
    pub fn has_quiescent_record_bytes(&self) -> Result<bool> {
        match self.lease.as_ref() {
            Some(PendingLease::NewShared { use_file, .. }) => Ok(use_file.metadata()?.len() != 0),
            _ => Ok(false),
        }
    }

    /// Bind admission to the identity obtained from the opened main-file
    /// descriptor.  No recovery artifact may be inspected before this step.
    pub fn bind(self, identity: FileIdentity) -> Result<Arc<DatabaseNamespaceBinding>> {
        self.bind_with_gate_release(identity, release_gate)
    }

    /// Bind a newly opened generation after proving that a stale namespace
    /// record has no live owner.
    ///
    /// This is deliberately narrower than [`Self::bind`]: it succeeds only
    /// for a `Shared` admission that owns both namespace locks exclusively.
    /// The caller must already have opened the current main-file descriptor;
    /// its identity is revalidated against the pathname before the stale
    /// record is replaced. A joined/live generation always fails closed.
    pub fn bind_replacing_quiescent_record(
        self,
        identity: FileIdentity,
    ) -> Result<Arc<DatabaseNamespaceBinding>> {
        self.bind_with_gate_release_mode(
            identity,
            release_gate,
            NamespaceBindMode::ReplaceQuiescentRecord,
        )
    }

    fn bind_with_gate_release<F>(
        self,
        identity: FileIdentity,
        release_gate_fn: F,
    ) -> Result<Arc<DatabaseNamespaceBinding>>
    where
        F: FnOnce(&File) -> Result<()>,
    {
        self.bind_with_gate_release_mode(
            identity,
            release_gate_fn,
            NamespaceBindMode::PreserveRecord,
        )
    }

    fn bind_with_gate_release_mode<F>(
        mut self,
        identity: FileIdentity,
        release_gate_fn: F,
        bind_mode: NamespaceBindMode,
    ) -> Result<Arc<DatabaseNamespaceBinding>>
    where
        F: FnOnce(&File) -> Result<()>,
    {
        let lease = self
            .lease
            .take()
            .ok_or_else(|| FrankenError::internal("namespace admission already consumed"))?;

        let state = match lease {
            PendingLease::NewShared { gate, mut use_file } => {
                let write_result = match bind_mode {
                    NamespaceBindMode::PreserveRecord => {
                        write_identity_record(&mut use_file, &self.stable_path, identity)
                    }
                    NamespaceBindMode::ReplaceQuiescentRecord => replace_quiescent_identity_record(
                        &mut use_file,
                        &self.stable_path,
                        identity,
                    ),
                };
                if let Err(error) = write_result {
                    release_namespace_locks(&gate, &use_file);
                    return Err(error);
                }
                // Keep a new generation exclusive through pager
                // initialization.  Otherwise a peer could join a freshly
                // created zero-length file before page 1 is durable.
                BindingLease::BootstrapExclusive { gate, use_file }
            }
            PendingLease::JoinShared {
                gate,
                mut use_file,
                generation_identity,
            } => {
                if bind_mode == NamespaceBindMode::ReplaceQuiescentRecord {
                    release_namespace_locks(&gate, &use_file);
                    return Err(cannot_open(&self.stable_path));
                }
                let observed_identity = match read_identity_record(&mut use_file, &self.stable_path)
                {
                    Ok(identity) => identity,
                    Err(error) => {
                        release_namespace_locks(&gate, &use_file);
                        return Err(error);
                    }
                };
                if observed_identity != generation_identity || identity != generation_identity {
                    release_namespace_locks(&gate, &use_file);
                    return Err(cannot_open(&self.stable_path));
                }
                if let Err(error) = release_gate_fn(&gate) {
                    release_namespace_locks(&gate, &use_file);
                    return Err(error);
                }
                drop(gate);
                BindingLease::Shared { use_file }
            }
            PendingLease::BootstrapExclusive { gate, mut use_file } => {
                if bind_mode == NamespaceBindMode::ReplaceQuiescentRecord {
                    release_namespace_locks(&gate, &use_file);
                    return Err(cannot_open(&self.stable_path));
                }
                if let Err(error) =
                    write_identity_record(&mut use_file, &self.stable_path, identity)
                {
                    release_namespace_locks(&gate, &use_file);
                    return Err(error);
                }
                BindingLease::BootstrapExclusive { gate, use_file }
            }
            PendingLease::ReadOnlyUnadmitted => {
                if bind_mode == NamespaceBindMode::ReplaceQuiescentRecord {
                    return Err(cannot_open(&self.stable_path));
                }
                BindingLease::ReadOnlyUnadmitted
            }
        };

        Ok(Arc::new(DatabaseNamespaceBinding {
            stable_path: std::mem::take(&mut self.stable_path),
            identity,
            lease: Mutex::new(state),
        }))
    }
}

impl Drop for PendingNamespaceOpen {
    fn drop(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        let (gate, use_file) = match lease {
            PendingLease::NewShared { gate, use_file }
            | PendingLease::JoinShared { gate, use_file, .. }
            | PendingLease::BootstrapExclusive { gate, use_file } => (gate, use_file),
            // Sidecar-less admission holds no files and no locks.
            PendingLease::ReadOnlyUnadmitted => return,
        };
        let _ = AdvisoryFileLock::unlock(&use_file);
        let _ = AdvisoryFileLock::unlock(&gate);
    }
}

#[derive(Debug)]
enum BindingLease {
    Shared {
        use_file: File,
    },
    BootstrapExclusive {
        gate: File,
        use_file: File,
    },
    BootstrapUseShared {
        gate: File,
        use_file: File,
    },
    Transitioning,
    /// GH#140 / bd-daqmp sidecar-less read-only binding: no files, no locks.
    ReadOnlyUnadmitted,
    /// bd-97kjm terminal teardown state: every advisory lock has been released
    /// and every retained sidecar descriptor has been closed. A binding enters
    /// this state through [`DatabaseNamespaceBinding::quiesce`] (explicit
    /// pool-drop teardown) or [`DatabaseNamespaceBinding::guard_generation`]
    /// (on a detected main-file quarantine/rename). It is inert and idempotent:
    /// a lingering `Arc` clone can no longer write through the released
    /// descriptors, and the binding's own `Drop` has nothing left to release.
    Quiesced,
}

/// Lifetime lease binding all path-derived companions to one main-file
/// identity.  Keep this value alive for the full connection lifetime.
#[derive(Debug)]
pub struct DatabaseNamespaceBinding {
    stable_path: PathBuf,
    identity: FileIdentity,
    lease: Mutex<BindingLease>,
}

/// Outcome of re-probing whether a binding's bound generation is still
/// installed at its stable path. Distinguishing a *proven* supersession from a
/// transient probe failure is what keeps
/// [`DatabaseNamespaceBinding::guard_generation`] from releasing a still-live
/// writer's advisory locks on a transient stat error (bd-ep8y9).
enum GenerationProbe {
    /// The stable path still names the bound file identity.
    Current,
    /// The stable path provably no longer names this generation: a different
    /// inode, a non-file, or nothing (`ENOENT`) now occupies it.
    Superseded,
    /// The probe itself failed for a transient or ambiguous reason; the bound
    /// generation's liveness is unknown and the lease must be left intact.
    ProbeFailed(FrankenError),
}

impl DatabaseNamespaceBinding {
    /// The single absolute path from which all companion names must derive.
    #[must_use]
    pub fn stable_path(&self) -> &Path {
        &self.stable_path
    }

    /// The main-file identity to which this lease is bound.
    #[must_use]
    pub const fn identity(&self) -> FileIdentity {
        self.identity
    }

    /// Side-effect-free identity validation for operation boundaries.  The
    /// caller obtains the current pathname identity through its VFS first.
    pub fn validate_identity(&self, current: Option<FileIdentity>) -> Result<()> {
        if current == Some(self.identity) {
            Ok(())
        } else {
            Err(cannot_open(&self.stable_path))
        }
    }

    /// Verify that the stable main pathname (without following its final
    /// symlink) still names this binding's file identity.  The probe is
    /// read-only and never creates database or companion files.
    ///
    /// bd-qduu1: on Unix this must NOT open (and then close) a descriptor
    /// for the main database file. POSIX record locks are per-process,
    /// per-file: closing ANY descriptor of a file releases ALL of this
    /// process's `fcntl` locks on it, including the RESERVED byte that
    /// gates cross-process WAL appends. This probe runs on every WAL
    /// backend operation, so the open+close variant silently destroyed the
    /// append gate the group-commit flush had just acquired — two
    /// processes then derived the same WAL append offset and overwrote
    /// each other's committed frames (read-your-own-write returned zero
    /// rows) or tripped the parallel-WAL certificate cross-check. A path
    /// stat creates no descriptor, so no lock is disturbed;
    /// `symlink_metadata` preserves the `O_NOFOLLOW` property by
    /// identifying a final-component symlink itself (rejected as
    /// not-a-file) rather than its target.
    pub fn validate_path_identity(&self) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            let metadata = std::fs::symlink_metadata(&self.stable_path)
                .map_err(|_| cannot_open(&self.stable_path))?;
            if !metadata.is_file() {
                return Err(cannot_open(&self.stable_path));
            }
            self.validate_identity(Some(FileIdentity::from_unix_parts(
                metadata.dev(),
                metadata.ino(),
            )))
        }

        // Windows closes do not release byte-range locks held on other
        // handles, and the robust 128-bit file identifier requires an open
        // handle, so the handle-based probe remains correct there.
        #[cfg(not(unix))]
        {
            let file = open_identity_probe(&self.stable_path)?;
            self.validate_identity(FileIdentity::from_file(&file)?)
        }
    }

    /// Complete reserved bootstrap by converting `use` to shared and then
    /// releasing `gate`.  The transition is idempotent.
    pub fn finish_bootstrap(&self) -> Result<()> {
        self.finish_bootstrap_with_gate_release(release_gate)
    }

    fn finish_bootstrap_with_gate_release<F>(&self, release_gate_fn: F) -> Result<()>
    where
        F: FnOnce(&File) -> Result<()>,
    {
        let mut lease = self
            .lease
            .lock()
            .map_err(|_| FrankenError::internal("namespace lease mutex poisoned"))?;
        if matches!(
            *lease,
            BindingLease::Shared { .. }
                | BindingLease::ReadOnlyUnadmitted
                | BindingLease::Quiesced
        ) {
            return Ok(());
        }
        let old = std::mem::replace(&mut *lease, BindingLease::Transitioning);
        let (gate, use_file, use_is_shared) = match old {
            BindingLease::BootstrapExclusive { gate, use_file } => (gate, use_file, false),
            BindingLease::BootstrapUseShared { gate, use_file } => (gate, use_file, true),
            other => {
                *lease = other;
                return Err(FrankenError::internal(
                    "namespace bootstrap transition re-entered",
                ));
            }
        };

        if !use_is_shared && let Err(error) = downgrade_to_shared(&use_file) {
            *lease = BindingLease::BootstrapExclusive { gate, use_file };
            return Err(error);
        }
        if let Err(error) = release_gate_fn(&gate) {
            *lease = BindingLease::BootstrapUseShared { gate, use_file };
            return Err(error);
        }
        drop(gate);
        *lease = BindingLease::Shared { use_file };
        Ok(())
    }

    /// Whether bootstrap still owns the namespace exclusively.
    #[must_use]
    pub fn bootstrap_is_exclusive(&self) -> bool {
        self.lease.lock().is_ok_and(|lease| {
            matches!(
                *lease,
                BindingLease::BootstrapExclusive { .. } | BindingLease::BootstrapUseShared { .. }
            )
        })
    }

    /// bd-97kjm ask #2 — explicit namespace quiescence/teardown.
    ///
    /// Deterministically release every advisory lock and close every retained
    /// sidecar descriptor owned by this generation **now**, regardless of how
    /// many [`Arc`] clones still reference the binding. The persistent sidecar
    /// files are never unlinked (that would split the advisory-lock domain);
    /// only the process-local descriptors and their `flock` claims are dropped.
    ///
    /// A pool owner calls this once the last connection bound to the file has
    /// closed, so a subsequent generation transition (or a fresh admission)
    /// observes no stale `use` lease even when a background reference (a
    /// detached flusher, a pooled handle) still holds an `Arc` clone. The
    /// operation is idempotent and, after it returns, the binding is inert:
    /// [`Self::finish_bootstrap`] becomes a no-op and no retained descriptor
    /// can write through the released `use`/`gate` handles.
    ///
    /// The caller owns the "no live connection is cut off" contract exactly as
    /// [`cleanup_abandoned_private_database`] does: invoke this only after every
    /// pager/validation connection bound to this generation has closed. For a
    /// teardown that is safe even while a connection may still be live, prefer
    /// [`Self::guard_generation`], which releases only after proving the bound
    /// generation is no longer installed at the path.
    pub fn quiesce(&self) {
        let mut lease = match self.lease.lock() {
            Ok(lease) => lease,
            Err(poisoned) => poisoned.into_inner(),
        };
        Self::quiesce_lease(&mut lease);
    }

    /// Release the descriptors owned by `lease` and leave it [`BindingLease::Quiesced`].
    fn quiesce_lease(lease: &mut BindingLease) {
        // Unlock explicitly before the descriptor closes so the advisory-lock
        // handoff boundary is immediate on every platform, then drop the owned
        // `File`s so their (single) open file descriptions — and thus the
        // retained fds — are gone the instant this returns.
        match std::mem::replace(lease, BindingLease::Quiesced) {
            BindingLease::Shared { use_file } => {
                let _ = AdvisoryFileLock::unlock(&use_file);
                drop(use_file);
            }
            BindingLease::BootstrapExclusive { gate, use_file }
            | BindingLease::BootstrapUseShared { gate, use_file } => {
                let _ = AdvisoryFileLock::unlock(&use_file);
                let _ = AdvisoryFileLock::unlock(&gate);
                drop(use_file);
                drop(gate);
            }
            BindingLease::Transitioning
            | BindingLease::ReadOnlyUnadmitted
            | BindingLease::Quiesced => {}
        }
    }

    /// Whether this binding has been torn down by [`Self::quiesce`] or a
    /// generation-guard release. A quiesced binding owns no descriptors and no
    /// advisory locks.
    #[must_use]
    pub fn is_quiesced(&self) -> bool {
        self.lease
            .lock()
            .is_ok_and(|lease| matches!(*lease, BindingLease::Quiesced))
    }

    /// bd-97kjm ask #3 — generation-bound teardown.
    ///
    /// Re-probe the stable pathname's file identity and act on the three
    /// distinguishable outcomes (bd-ep8y9):
    ///
    /// * **Current** — the path still names this binding's identity: a pure,
    ///   side-effect-free check identical to [`Self::validate_path_identity`].
    /// * **Superseded** — the probe *proves* the generation changed (a different
    ///   inode, a non-file, or nothing at all now occupies the path — the
    ///   recovery quarantine/rename case). Only then does this [`Self::quiesce`]
    ///   the binding, closing the retained `use`/`gate` descriptors so they can
    ///   never write through the superseded generation. Returns `Err`.
    /// * **Probe failure** — the identity could not be read at all (a transient
    ///   `EIO`/`EACCES`/`ESTALE`, an ambiguous open error): liveness is UNKNOWN.
    ///   The lease is left fully intact and `Err` is returned so the caller
    ///   fails the operation *closed* with its advisory locks still held.
    ///
    /// The last case is the whole point of the split-brain fix: a transient stat
    /// error must NOT release the namespace locks of a still-live writer. If it
    /// did, this process would keep writing lock-free while a second process
    /// could win admission on the released `-ns-use`/`-ns-gate` locks. For the
    /// same reason, once the lease is already [`BindingLease::Quiesced`] this
    /// fails closed unconditionally: a later successful probe must never report
    /// a lock-less binding as a live generation.
    ///
    /// This never mutates the main database, the quarantined old inode, or the
    /// persistent sidecars: identity is probed with `symlink_metadata` on Unix
    /// (no descriptor, so no `fcntl` record lock is disturbed — bd-qduu1) and
    /// the release path only unlocks and closes already-held descriptors.
    pub fn guard_generation(&self) -> Result<()> {
        // Fail closed once quiesced: a quiesced lease owns no advisory locks, so
        // reporting its generation as live would let this lock-less binding keep
        // writing while another process holds the namespace locks (bd-ep8y9).
        if self.is_quiesced() {
            return Err(cannot_open(&self.stable_path));
        }
        match self.probe_generation() {
            // bd-r3dt7: re-check quiesced AFTER the probe. Between the leading
            // `is_quiesced()` and this probe a peer could have superseded and
            // quiesced this binding (releasing its advisory locks), then renamed
            // the same identity back onto the stable path — so the probe reads
            // `Current` on a now-lock-less lease. Without this re-check that one
            // call would report the quiesced binding as a live generation (only
            // the NEXT call fails closed on the leading `is_quiesced()`). The
            // re-check runs through the same lease mutex as `quiesce()`, so it
            // linearizes against a concurrent supersession and closes the
            // one-call resurrection window.
            GenerationProbe::Current => {
                if self.is_quiesced() {
                    Err(cannot_open(&self.stable_path))
                } else {
                    Ok(())
                }
            }
            GenerationProbe::Superseded => {
                // The bound generation is provably gone: release the retained
                // descriptors/locks so they cannot write through the superseded
                // inode. A still-live connection is never reached here.
                self.quiesce();
                Err(cannot_open(&self.stable_path))
            }
            // Transient/ambiguous probe failure: identity unknown. Never release
            // the lease — dropping a live writer's locks here is the split-brain
            // bug (bd-ep8y9). Surface the error so the caller fails closed.
            GenerationProbe::ProbeFailed(error) => Err(error),
        }
    }

    /// Classify a generation re-probe into the three outcomes acted on by
    /// [`Self::guard_generation`]. Separating a *proven* supersession from a
    /// transient probe failure is load-bearing: only the former may release the
    /// lease (bd-ep8y9). The probe never opens the main database file on Unix
    /// (`symlink_metadata`, no descriptor — bd-qduu1).
    fn probe_generation(&self) -> GenerationProbe {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            match std::fs::symlink_metadata(&self.stable_path) {
                Ok(metadata) => {
                    if metadata.is_file()
                        && FileIdentity::from_unix_parts(metadata.dev(), metadata.ino())
                            == self.identity
                    {
                        GenerationProbe::Current
                    } else {
                        // Stat succeeded, but a different inode (or a non-file)
                        // now occupies the path: provably superseded.
                        GenerationProbe::Superseded
                    }
                }
                // ENOENT: the main file was renamed/unlinked off the stable path
                // — a proven supersession (the bd-97kjm quarantine case), not a
                // transient read failure.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    GenerationProbe::Superseded
                }
                // EIO / EACCES / ESTALE / …: the identity could not be read.
                // Liveness is unknown — leave the lease intact, fail closed.
                Err(_) => GenerationProbe::ProbeFailed(cannot_open(&self.stable_path)),
            }
        }

        #[cfg(not(unix))]
        {
            // Windows: closing a handle releases *that handle's* byte-range
            // locks, so a probe-error quiesce would drop this binding's
            // `-ns-use`/`-ns-gate` locks exactly as on Unix. Classify
            // conservatively — only a successfully-read mismatching identity
            // supersedes; every open/identity failure is a transient probe
            // failure (fail closed, lease retained), never a supersession.
            match open_identity_probe(&self.stable_path) {
                Ok(file) => match FileIdentity::from_file(&file) {
                    Ok(Some(current)) if current == self.identity => GenerationProbe::Current,
                    Ok(Some(_)) => GenerationProbe::Superseded,
                    Ok(None) | Err(_) => {
                        GenerationProbe::ProbeFailed(cannot_open(&self.stable_path))
                    }
                },
                Err(_) => GenerationProbe::ProbeFailed(cannot_open(&self.stable_path)),
            }
        }
    }
}

impl Drop for DatabaseNamespaceBinding {
    fn drop(&mut self) {
        // The last Arc is the exact end of this generation's lifetime lease.
        // Unlock explicitly at that boundary before the descriptors close so
        // a following generation transition cannot observe a stale shared
        // lease, even on filesystems where close-driven flock handoff is not
        // immediate.
        let lease = match self.lease.get_mut() {
            Ok(lease) => lease,
            Err(poisoned) => poisoned.into_inner(),
        };
        match lease {
            BindingLease::Shared { use_file } => {
                let _ = AdvisoryFileLock::unlock(use_file);
            }
            BindingLease::BootstrapExclusive { gate, use_file }
            | BindingLease::BootstrapUseShared { gate, use_file } => {
                let _ = AdvisoryFileLock::unlock(use_file);
                let _ = AdvisoryFileLock::unlock(gate);
            }
            BindingLease::Transitioning
            | BindingLease::ReadOnlyUnadmitted
            | BindingLease::Quiesced => {}
        }
    }
}

/// Begin an exact namespace-generation transition before mutating the path.
///
/// This opens the existing persistent sidecars without creating them, acquires
/// both namespace locks exclusively, and verifies the durable namespace record.
/// On a fresh transition it also requires the current main pathname to identify
/// `expected_old_identity`, then writes a durable prepare marker before
/// returning. Lock acquisition is non-blocking, so any live binding or
/// concurrent admission returns [`FrankenError::Busy`].
///
/// On restart, an exact existing full or partial prepare marker changes the
/// contract: `expected_old_identity` names the identity still recorded by the
/// ledger, while the main pathname may be absent after quarantine or may
/// already name a candidate replacement. The resumed guard retains any exact
/// partial publication tail. `publish_replacement` accepts only the byte-exact
/// continuation for the supplied replacement identity; `finish` accepts only
/// the recorded identity. A foreign pathname or foreign ledger tail is never
/// adopted.
///
/// The caller must acquire this guard before quarantining or renaming the old
/// main file, retain it across every activation or rollback rename, call
/// [`DatabaseNamespaceGenerationTransition::publish_replacement`] after each
/// exact pathname replacement, and call
/// [`DatabaseNamespaceGenerationTransition::finish`] only when the generation
/// that should become visible is installed. The caller must also exclude
/// non-library pathname mutation while the guard is live.
///
/// Dropping the guard before any finish attempt releases the advisory locks but
/// deliberately retains the durable prepare marker. Ordinary admission then
/// fails closed until recovery resumes this guard for the exact currently
/// recorded identity and finishes or publishes another exact replacement. If a
/// finish attempt mutates the ledger but cannot confirm durability, dropping
/// the poisoned guard fail-stops by retaining both exclusive descriptors for
/// the process lifetime. No namespace sidecar is ever renamed or unlinked.
pub fn begin_database_namespace_generation_transition(
    database_path: &Path,
    expected_old_identity: FileIdentity,
) -> Result<DatabaseNamespaceGenerationTransition> {
    begin_database_namespace_generation_transition_inner(
        database_path,
        expected_old_identity,
        || Ok(()),
    )
}

fn begin_database_namespace_generation_transition_inner<F>(
    database_path: &Path,
    expected_old_identity: FileIdentity,
    before_prepare: F,
) -> Result<DatabaseNamespaceGenerationTransition>
where
    F: FnOnce() -> Result<()>,
{
    validate_stable_path(database_path)?;

    let gate_path = sidecar_path(database_path, GATE_SUFFIX);
    let use_path = sidecar_path(database_path, USE_SUFFIX);
    let gate = open_existing_transition_lock_file(&gate_path)?;
    let mut use_file = open_existing_transition_lock_file(&use_path)?;
    try_lock(&gate, FileLockMode::Exclusive)?;
    if let Err(error) = try_lock(&use_file, FileLockMode::Exclusive) {
        let _ = AdvisoryFileLock::unlock(&gate);
        return Err(error);
    }

    let preparation = (|| {
        let state = read_namespace_record_state(&mut use_file, database_path, true)?;
        if state.current_identity != expected_old_identity {
            return Err(cannot_open(database_path));
        }

        let next_sequence = state
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| cannot_open(database_path))?;
        let (prepare_offset, append_offset, interrupted_tail) = if let Some(prepared_sequence) =
            state.prepared_sequence
        {
            if prepared_sequence != next_sequence {
                return Err(cannot_open(database_path));
            }
            (
                state
                    .prepare_offset
                    .ok_or_else(|| cannot_open(database_path))?,
                state.valid_bytes,
                state.interrupted_tail,
            )
        } else {
            let prepare = encode_prepare_record(next_sequence, expected_old_identity);
            if !state.interrupted_tail.is_empty() && !prepare.starts_with(&state.interrupted_tail) {
                return Err(cannot_open(database_path));
            }

            let resuming_partial_prepare = !state.interrupted_tail.is_empty();
            if !resuming_partial_prepare {
                validate_generation_path_identity(database_path, expected_old_identity)?;
            }
            before_prepare()?;
            if !resuming_partial_prepare {
                validate_generation_path_identity(database_path, expected_old_identity)?;
            }
            if resuming_partial_prepare {
                use_file.set_len(state.valid_bytes)?;
                use_file.sync_data()?;
            }
            use_file.seek(SeekFrom::Start(state.valid_bytes))?;
            use_file.write_all(&prepare)?;
            let append_offset = state
                .valid_bytes
                .checked_add(PREPARE_BYTES as u64)
                .ok_or_else(|| cannot_open(database_path))?;
            use_file.set_len(append_offset)?;
            use_file.flush()?;
            use_file.sync_data()?;
            if !resuming_partial_prepare {
                validate_generation_path_identity(database_path, expected_old_identity)?;
            }
            (state.valid_bytes, append_offset, Vec::new())
        };

        Ok((
            state.last_sequence,
            prepare_offset,
            append_offset,
            interrupted_tail,
        ))
    })();
    let (last_sequence, prepare_offset, append_offset, interrupted_tail) = match preparation {
        Ok(preparation) => preparation,
        Err(error) => {
            let _ = AdvisoryFileLock::unlock(&use_file);
            let _ = AdvisoryFileLock::unlock(&gate);
            return Err(error);
        }
    };

    Ok(DatabaseNamespaceGenerationTransition {
        stable_path: database_path.to_owned(),
        gate: Some(gate),
        use_file: Some(use_file),
        current_identity: expected_old_identity,
        last_sequence,
        prepare_offset,
        append_offset,
        interrupted_tail,
        finished: false,
        poisoned: false,
    })
}

impl DatabaseNamespaceGenerationTransition {
    /// Identity currently recorded by this exclusively leased namespace.
    #[must_use]
    pub const fn current_identity(&self) -> FileIdentity {
        self.current_identity
    }

    /// Durably publish the exact identity currently installed at the path.
    ///
    /// Both namespace locks remain exclusive after publication. A fresh
    /// prepare marker for `replacement_identity` is written in the same
    /// durability unit, so the caller may replace it again (for example, an
    /// exact rollback) before calling [`Self::finish`].
    ///
    pub fn publish_replacement(
        &mut self,
        replacement_identity: FileIdentity,
    ) -> Result<NamespaceGenerationTransitionOutcome> {
        self.publish_replacement_inner(replacement_identity, || Ok(()))
    }

    fn publish_replacement_inner<F>(
        &mut self,
        replacement_identity: FileIdentity,
        before_publish: F,
    ) -> Result<NamespaceGenerationTransitionOutcome>
    where
        F: FnOnce() -> Result<()>,
    {
        if self.finished {
            return Err(FrankenError::internal(
                "namespace generation transition already finished",
            ));
        }
        self.validate_prepare_marker()?;
        self.validate_interrupted_tail()?;
        validate_generation_path_identity(&self.stable_path, replacement_identity)?;

        if replacement_identity == self.current_identity && self.interrupted_tail.is_empty() {
            return Ok(NamespaceGenerationTransitionOutcome::AlreadyPublished);
        }
        if replacement_identity == self.current_identity {
            return Err(cannot_open(&self.stable_path));
        }

        let sequence = self
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| cannot_open(&self.stable_path))?;
        let old_identity = self.current_identity;
        let transition = encode_transition_record(sequence, old_identity, replacement_identity);
        let next_prepare = encode_prepare_record(
            sequence
                .checked_add(1)
                .ok_or_else(|| cannot_open(&self.stable_path))?,
            replacement_identity,
        );
        let mut publication = [0_u8; TRANSITION_BYTES + PREPARE_BYTES];
        publication[..TRANSITION_BYTES].copy_from_slice(&transition);
        publication[TRANSITION_BYTES..].copy_from_slice(&next_prepare);
        if !publication.starts_with(&self.interrupted_tail) {
            return Err(cannot_open(&self.stable_path));
        }

        before_publish()?;
        validate_generation_path_identity(&self.stable_path, replacement_identity)?;

        let append_offset = self.append_offset;
        let use_file = self
            .use_file
            .as_mut()
            .ok_or_else(|| FrankenError::internal("namespace transition lease missing"))?;
        if !self.interrupted_tail.is_empty() {
            use_file.set_len(append_offset)?;
            use_file.sync_data()?;
        }
        use_file.seek(SeekFrom::Start(append_offset))?;
        use_file.write_all(&publication)?;
        let next_append_offset = append_offset
            .checked_add(TRANSITION_BYTES as u64)
            .and_then(|offset| offset.checked_add(PREPARE_BYTES as u64))
            .ok_or_else(|| cannot_open(&self.stable_path))?;
        use_file.set_len(next_append_offset)?;
        use_file.flush()?;
        use_file.sync_data()?;

        self.current_identity = replacement_identity;
        self.last_sequence = sequence;
        self.prepare_offset = append_offset + TRANSITION_BYTES as u64;
        self.append_offset = next_append_offset;
        self.interrupted_tail.clear();
        Ok(NamespaceGenerationTransitionOutcome::Published)
    }

    /// Make the current exact generation visible.
    ///
    /// This method is deliberately non-consuming so the caller can retry after
    /// an error; while such a retryable guard remains alive, both namespace
    /// locks still exclude admissions. A successful finish releases both locks
    /// before returning and subsequent calls return the same identity.
    /// Publication uses an appended, checksummed finish record rather than
    /// deleting the prepare marker, so a torn write remains fail-closed. If an
    /// error occurs after ledger mutation, retry on this same guard. Abandoning
    /// that poisoned guard deliberately retains both locks for the process
    /// lifetime rather than admitting against an unconfirmed finish record.
    pub fn finish(&mut self) -> Result<FileIdentity> {
        self.finish_inner(|| Ok(()))
    }

    fn finish_inner<F>(&mut self, before_sync: F) -> Result<FileIdentity>
    where
        F: FnOnce() -> Result<()>,
    {
        if self.finished {
            return Ok(self.current_identity);
        }
        let stable_path = self.stable_path.clone();
        let current_identity = self.current_identity;
        let append_offset = self.append_offset;
        let mut must_fail_stop_on_drop = false;
        let result = (|| {
            self.validate_prepare_marker()?;
            validate_generation_path_identity(&stable_path, current_identity)?;

            let sequence = self
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| cannot_open(&stable_path))?;
            let finish = encode_finish_record(sequence, current_identity);
            let finish_end = append_offset
                .checked_add(FINISH_BYTES as u64)
                .ok_or_else(|| cannot_open(&stable_path))?;
            if !finish.starts_with(&self.interrupted_tail) {
                return Err(cannot_open(&stable_path));
            }
            let use_file = self
                .use_file
                .as_mut()
                .ok_or_else(|| FrankenError::internal("namespace transition lease missing"))?;
            let file_len = use_file.metadata()?.len();
            if file_len < append_offset || file_len > finish_end {
                return Err(cannot_open(&stable_path));
            }
            let observed_len =
                usize::try_from(file_len - append_offset).map_err(|_| cannot_open(&stable_path))?;
            let mut observed = vec![0_u8; observed_len];
            use_file.seek(SeekFrom::Start(append_offset))?;
            use_file.read_exact(&mut observed)?;
            if !finish.starts_with(&observed) {
                return Err(cannot_open(&stable_path));
            }
            if !observed.is_empty() {
                must_fail_stop_on_drop = true;
                use_file.set_len(append_offset)?;
                use_file.sync_data()?;
            }
            must_fail_stop_on_drop = true;
            use_file.seek(SeekFrom::Start(append_offset))?;
            use_file.write_all(&finish)?;
            use_file.set_len(finish_end)?;
            use_file.flush()?;
            before_sync()?;
            use_file.sync_data()?;
            Ok((sequence, finish_end))
        })();

        match result {
            Ok((sequence, finish_end)) => {
                self.last_sequence = sequence;
                self.append_offset = finish_end;
                self.interrupted_tail.clear();
                self.finished = true;
                self.poisoned = false;
                self.release_locks();
                Ok(current_identity)
            }
            Err(error) => {
                self.poisoned |= must_fail_stop_on_drop;
                Err(error)
            }
        }
    }

    fn validate_prepare_marker(&mut self) -> Result<()> {
        let expected = encode_prepare_record(
            self.last_sequence
                .checked_add(1)
                .ok_or_else(|| cannot_open(&self.stable_path))?,
            self.current_identity,
        );
        let mut observed = [0_u8; PREPARE_BYTES];
        let use_file = self
            .use_file
            .as_mut()
            .ok_or_else(|| FrankenError::internal("namespace transition lease missing"))?;
        use_file.seek(SeekFrom::Start(self.prepare_offset))?;
        use_file.read_exact(&mut observed)?;
        if observed != expected {
            return Err(cannot_open(&self.stable_path));
        }
        Ok(())
    }

    fn validate_interrupted_tail(&mut self) -> Result<()> {
        let expected_len = self
            .append_offset
            .checked_add(
                u64::try_from(self.interrupted_tail.len())
                    .map_err(|_| cannot_open(&self.stable_path))?,
            )
            .ok_or_else(|| cannot_open(&self.stable_path))?;
        let use_file = self
            .use_file
            .as_mut()
            .ok_or_else(|| FrankenError::internal("namespace transition lease missing"))?;
        if use_file.metadata()?.len() != expected_len {
            return Err(cannot_open(&self.stable_path));
        }
        let mut observed = vec![0_u8; self.interrupted_tail.len()];
        use_file.seek(SeekFrom::Start(self.append_offset))?;
        use_file.read_exact(&mut observed)?;
        if observed != self.interrupted_tail {
            return Err(cannot_open(&self.stable_path));
        }
        Ok(())
    }

    fn release_locks(&mut self) {
        // Close is the final authority for releasing these descriptor-owned
        // locks.  Unlock explicitly first so a successful finish or ordinary
        // abandonment has an immediate, platform-consistent handoff boundary.
        if let Some(use_file) = self.use_file.take() {
            let _ = AdvisoryFileLock::unlock(&use_file);
            drop(use_file);
        }
        if let Some(gate) = self.gate.take() {
            let _ = AdvisoryFileLock::unlock(&gate);
            drop(gate);
        }
    }
}

impl Drop for DatabaseNamespaceGenerationTransition {
    fn drop(&mut self) {
        if self.poisoned && !self.finished {
            // An I/O error after mutating the finish record makes durability
            // unknowable. Releasing either descriptor could admit a peer that
            // observes a complete-but-unconfirmed FINISH. Fail-stop instead:
            // leak both descriptors so this process retains the exclusive
            // locks. A successful retry clears `poisoned` and drops normally.
            if let Some(gate) = self.gate.take() {
                std::mem::forget(gate);
            }
            if let Some(use_file) = self.use_file.take() {
                std::mem::forget(use_file);
            }
            return;
        }

        self.release_locks();
    }
}

fn validate_stable_path(path: &Path) -> Result<()> {
    if !path.is_absolute() || path.file_name().is_none() {
        return Err(cannot_open(path));
    }
    Ok(())
}

fn sidecar_path(database_path: &Path, suffix: &str) -> PathBuf {
    let mut path: OsString = database_path.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

fn open_secure_lock_file(path: &Path) -> Result<File> {
    let file = match configured_open_options(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            configured_open_options(false)
                .open(path)
                .map_err(|_| cannot_open(path))?
        }
        Err(_) => return Err(cannot_open(path)),
    };
    validate_secure_lock_file(path, &file)?;
    Ok(file)
}

fn open_existing_secure_lock_file(path: &Path) -> Result<File> {
    let file = configured_existing_readonly_open_options()
        .open(path)
        .map_err(|_| cannot_open(path))?;
    validate_secure_lock_file(path, &file)?;
    Ok(file)
}

fn open_existing_transition_lock_file(path: &Path) -> Result<File> {
    let file = configured_open_options(false)
        .open(path)
        .map_err(|_| cannot_open(path))?;
    validate_secure_lock_file(path, &file)?;
    Ok(file)
}

fn configured_open_options(create_new: bool) -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(create_new);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        options
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options
}

fn configured_existing_readonly_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        options
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options
}

/// Open an existing namespace lock for transient-candidate cleanup.
///
/// Windows cleanup must be able to unlink the two namespace records while the
/// exclusive lock handles are retained. This special-purpose open therefore
/// includes `FILE_SHARE_DELETE`; ordinary namespace opens deliberately keep
/// their stronger no-delete sharing policy.
fn cleanup_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        options
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options
}

fn open_cleanup_lock_file(path: &Path) -> Result<Option<File>> {
    let file = match cleanup_open_options().open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(cannot_open(path)),
    };
    validate_secure_lock_file(path, &file)?;
    Ok(Some(file))
}

fn existing_regular_cleanup_entry(database_path: &Path, path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(cannot_open(database_path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Remove one abandoned caller-reserved transient database and its exact
/// namespace/recovery companions while holding the namespace exclusively.
///
/// This is **not** general database deletion. It exists only for private
/// `VACUUM` discard/rebuild candidates and failed caller-reserved outputs after
/// every pager/validation connection has closed. The parent directory is a
/// trusted cooperative namespace. Contention, a missing namespace record, a
/// generation-record mismatch, pathname identity drift, symlinks, or any
/// non-regular companion all fail closed without removing the main file.
///
/// The caller must retain the descriptor from which `expected_identity` was
/// derived until this function returns. `Ok(false)` means ownership could not
/// be proven and every entry was preserved.
pub fn cleanup_abandoned_private_database(
    database_path: &Path,
    expected_identity: FileIdentity,
) -> Result<bool> {
    validate_stable_path(database_path)?;
    let gate_path = sidecar_path(database_path, GATE_SUFFIX);
    let use_path = sidecar_path(database_path, USE_SUFFIX);
    let Some(gate) = open_cleanup_lock_file(&gate_path)? else {
        return Ok(false);
    };
    let Some(mut use_file) = open_cleanup_lock_file(&use_path)? else {
        return Ok(false);
    };

    match AdvisoryFileLock::try_lock(&gate, FileLockMode::Exclusive) {
        Ok(()) => {}
        Err(FileLockError::AlreadyLocked) => return Ok(false),
        Err(FileLockError::Io(error)) => return Err(error.into()),
    }
    match AdvisoryFileLock::try_lock(&use_file, FileLockMode::Exclusive) {
        Ok(()) => {}
        Err(FileLockError::AlreadyLocked) => return Ok(false),
        Err(FileLockError::Io(error)) => return Err(error.into()),
    }

    if read_identity_record(&mut use_file, database_path)? != expected_identity {
        return Ok(false);
    }
    let main_probe = match open_cleanup_identity_probe(database_path) {
        Ok(file) => file,
        Err(FrankenError::CannotOpen { .. }) => return Ok(false),
        Err(error) => return Err(error),
    };
    if FileIdentity::from_file(&main_probe)? != Some(expected_identity) {
        return Ok(false);
    }

    // Preflight the complete fixed companion set before removing anything.
    // Dynamic WAL segment cleanup is intentionally absent: transient VACUUM
    // candidates never enter WAL mode, and broad prefix deletion would violate
    // the exact-entry ownership boundary of this function.
    //
    // `.fsqlite-migration-state` (bd-zywqc.5 / `fsqlite_core::migration::
    // MIGRATION_MARKER_SUFFIX`) is included here too. `fsqlite-core` depends
    // on `fsqlite-vfs`, not the reverse, so the constant cannot be imported
    // here; it is a literal, same as every other companion suffix in this
    // list. Without it: this function is the exclusive-ownership teardown
    // for a caller-reserved transient candidate, called once every pager
    // binding to `database_path` has closed -- but current fsqlite stamps
    // that marker at birth on every database it creates (marker-at-birth,
    // unconditional), so any database this function tears down that was
    // ever actually opened carries one. Leaving it on disk after this
    // function reports success means a caller who re-lists the directory
    // afterward sees a leftover artifact from ITS OWN prior generation and
    // cannot distinguish it from a genuine new writer having repopulated
    // the namespace mid-cleanup -- observed live: HFDT's SEC XBRL scratch
    // cleanup (hfdt-storage) called this, got `true`, re-listed, found
    // `.fsqlite-migration-state` still present, and correctly (given what
    // it could see) refused with `store.migration_preflight_scratch_
    // cleanup_identity_drift` rather than risk deleting a live writer's
    // file. The false alarm was here: this function's own "complete fixed
    // companion set" was incomplete.
    let companion_paths = [
        sidecar_path(database_path, "-journal"),
        sidecar_path(database_path, "-wal"),
        sidecar_path(database_path, "-wal-fec"),
        sidecar_path(database_path, "-wal-fec").with_extension("wal-fec.tmp"),
        sidecar_path(database_path, "-shm"),
        sidecar_path(database_path, "-lock-shared"),
        sidecar_path(database_path, "-lock-reserved"),
        sidecar_path(database_path, "-lock-pending"),
        sidecar_path(database_path, ".fsqlite-migration-state"),
    ];
    let companion_exists = companion_paths
        .iter()
        .map(|path| existing_regular_cleanup_entry(database_path, path))
        .collect::<Result<Vec<_>>>()?;

    // Revalidate immediately before the first removal while both namespace
    // locks and the expected main descriptor are still live.
    let final_main_probe = match open_cleanup_identity_probe(database_path) {
        Ok(file) => file,
        Err(FrankenError::CannotOpen { .. }) => return Ok(false),
        Err(error) => return Err(error),
    };
    if FileIdentity::from_file(&final_main_probe)? != Some(expected_identity) {
        return Ok(false);
    }

    for (path, exists) in companion_paths.iter().zip(companion_exists) {
        if exists {
            std::fs::remove_file(path)?;
        }
    }
    std::fs::remove_file(database_path)?;
    std::fs::remove_file(&use_path)?;
    std::fs::remove_file(&gate_path)?;

    #[cfg(unix)]
    {
        let parent = database_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
    }
    // Win32 has no portable directory fsync. The caller still invokes the
    // platform VFS namespace-sync hook, whose Windows contract is an explicit
    // no-op matching SQLite's own Windows VFS durability boundary.

    Ok(true)
}

#[cfg(not(unix))]
fn open_identity_probe(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        options
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }

    let file = options.open(path).map_err(|_| cannot_open(path))?;
    let metadata = file.metadata().map_err(|_| cannot_open(path))?;
    if !metadata.is_file() {
        return Err(cannot_open(path));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(cannot_open(path));
        }
    }
    Ok(file)
}

fn open_cleanup_identity_probe(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        options
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }

    let file = options.open(path).map_err(|_| cannot_open(path))?;
    let metadata = file.metadata().map_err(|_| cannot_open(path))?;
    if !metadata.is_file() {
        return Err(cannot_open(path));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(cannot_open(path));
        }
    }
    Ok(file)
}

fn validate_secure_lock_file(path: &Path, file: &File) -> Result<()> {
    let metadata = file.metadata().map_err(|_| cannot_open(path))?;
    if !metadata.is_file() {
        return Err(cannot_open(path));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        // SAFETY: `geteuid` has no preconditions and does not dereference data.
        let effective_uid = unsafe { libc::geteuid() };
        if metadata.uid() != effective_uid || metadata.nlink() != 1 || metadata.mode() & 0o077 != 0
        {
            return Err(cannot_open(path));
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            || metadata.number_of_links() != Some(1)
        {
            return Err(cannot_open(path));
        }
    }
    Ok(())
}

fn validate_generation_path_identity(
    database_path: &Path,
    expected_identity: FileIdentity,
) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        let metadata =
            std::fs::symlink_metadata(database_path).map_err(|_| cannot_open(database_path))?;
        if !metadata.file_type().is_file() || metadata.nlink() != 1 {
            return Err(cannot_open(database_path));
        }
        let current = FileIdentity::from_unix_parts(metadata.dev(), metadata.ino());
        if current != expected_identity {
            return Err(cannot_open(database_path));
        }
        Ok(())
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;

        let file = open_identity_probe(database_path)?;
        if file.metadata()?.number_of_links() != Some(1)
            || FileIdentity::from_file(&file)? != Some(expected_identity)
        {
            return Err(cannot_open(database_path));
        }
        Ok(())
    }
}

fn write_identity_record(
    file: &mut File,
    database_path: &Path,
    identity: FileIdentity,
) -> Result<()> {
    // Preserve the exact transition ledger while this generation remains
    // current. Once a namespace record exists, ordinary admission may only
    // reopen that identity; every replacement must use the transition guard.
    // This also makes a guard dropped during caller-owned mutation fail closed
    // instead of silently rebinding the namespace to the pathname it finds.
    let existing_len = file.metadata()?.len();
    if existing_len != 0 {
        let state = read_namespace_record_state(file, database_path, false)?;
        if state.current_identity != identity {
            return Err(cannot_open(database_path));
        }
        file.sync_data()?;
        return Ok(());
    }

    write_fresh_identity_record(file, identity)
}

fn replace_quiescent_identity_record(
    file: &mut File,
    database_path: &Path,
    identity: FileIdentity,
) -> Result<()> {
    // `NewShared` holds both `gate` and `use` exclusively. Revalidate the
    // caller's already-open main generation against the stable pathname before
    // discarding copied/corrupt machine-local namespace state. A valid,
    // terminal transition ledger is safe to collapse in a copied namespace;
    // incomplete or malformed transition evidence must remain fail-closed.
    let existing_len = file.metadata()?.len();
    validate_generation_path_identity(database_path, identity)?;
    if existing_len >= RECORD_BYTES as u64 {
        match read_namespace_record_state(file, database_path, false) {
            Ok(state) if state.current_identity == identity => {
                file.sync_data()?;
                return Ok(());
            }
            Ok(_) => {}
            Err(FrankenError::CannotOpen { .. }) if existing_len <= RECORD_BYTES as u64 => {}
            Err(FrankenError::CannotOpen { .. }) => return Err(cannot_open(database_path)),
            Err(error) => return Err(error),
        }
    }
    write_fresh_identity_record(file, identity)
}

fn write_fresh_identity_record(file: &mut File, identity: FileIdentity) -> Result<()> {
    let mut record = [0_u8; RECORD_BYTES];
    record[..8].copy_from_slice(&RECORD_MAGIC);
    record[8] = RECORD_VERSION;
    record[9..9 + IDENTITY_BYTES].copy_from_slice(&identity.to_namespace_bytes());
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&record)?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

fn read_identity_record(file: &mut File, database_path: &Path) -> Result<FileIdentity> {
    let state = read_namespace_record_state(file, database_path, false)?;
    Ok(state.current_identity)
}

#[derive(Debug)]
struct NamespaceRecordState {
    current_identity: FileIdentity,
    last_sequence: u64,
    prepared_sequence: Option<u64>,
    prepare_offset: Option<u64>,
    valid_bytes: u64,
    interrupted_tail: Vec<u8>,
}

fn read_namespace_record_state(
    file: &mut File,
    database_path: &Path,
    allow_interrupted_tail: bool,
) -> Result<NamespaceRecordState> {
    let file_len = file.metadata()?.len();
    if file_len < RECORD_BYTES as u64 {
        return Err(cannot_open(database_path));
    }

    let mut record = [0_u8; RECORD_BYTES];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut record)?;
    if record[..8] != RECORD_MAGIC
        || record[8] != RECORD_VERSION
        || record[9 + IDENTITY_BYTES..].iter().any(|byte| *byte != 0)
    {
        return Err(cannot_open(database_path));
    }
    let mut encoded = [0_u8; IDENTITY_BYTES];
    encoded.copy_from_slice(&record[9..9 + IDENTITY_BYTES]);
    let mut current_identity =
        FileIdentity::from_namespace_bytes(encoded).ok_or_else(|| cannot_open(database_path))?;

    let remaining = file_len - RECORD_BYTES as u64;
    let complete_records = remaining / TRANSITION_BYTES as u64;
    let tail_len = usize::try_from(remaining % TRANSITION_BYTES as u64)
        .map_err(|_| cannot_open(database_path))?;

    let mut last_sequence = 0_u64;
    let mut prepared_sequence = None;
    let mut prepare_offset = None;
    for record_index in 0..complete_records {
        let mut ledger_record = [0_u8; TRANSITION_BYTES];
        file.read_exact(&mut ledger_record)?;
        let record_offset = (RECORD_BYTES as u64)
            .checked_add(
                record_index
                    .checked_mul(TRANSITION_BYTES as u64)
                    .ok_or_else(|| cannot_open(database_path))?,
            )
            .ok_or_else(|| cannot_open(database_path))?;

        if let Some((sequence, old_identity, new_identity)) =
            decode_transition_record(&ledger_record)
        {
            let expected_sequence = prepared_sequence.ok_or_else(|| cannot_open(database_path))?;
            if sequence != expected_sequence || old_identity != current_identity {
                return Err(cannot_open(database_path));
            }
            last_sequence = sequence;
            current_identity = new_identity;
            prepared_sequence = None;
            prepare_offset = None;
            continue;
        }

        if let Some((sequence, identity)) = decode_prepare_record(&ledger_record) {
            if prepared_sequence.is_some()
                || sequence
                    != last_sequence
                        .checked_add(1)
                        .ok_or_else(|| cannot_open(database_path))?
                || identity != current_identity
            {
                return Err(cannot_open(database_path));
            }
            prepared_sequence = Some(sequence);
            prepare_offset = Some(record_offset);
            continue;
        }

        if let Some((sequence, identity)) = decode_finish_record(&ledger_record) {
            if prepared_sequence != Some(sequence) || identity != current_identity {
                return Err(cannot_open(database_path));
            }
            last_sequence = sequence;
            prepared_sequence = None;
            prepare_offset = None;
            continue;
        }

        return Err(cannot_open(database_path));
    }

    if !allow_interrupted_tail && (tail_len != 0 || prepared_sequence.is_some()) {
        return Err(cannot_open(database_path));
    }

    if prepared_sequence.is_some() && prepare_offset.is_none() {
        return Err(cannot_open(database_path));
    }

    let mut interrupted_tail = vec![0_u8; tail_len];
    file.read_exact(&mut interrupted_tail)?;
    let valid_bytes = (RECORD_BYTES as u64)
        .checked_add(
            complete_records
                .checked_mul(TRANSITION_BYTES as u64)
                .ok_or_else(|| cannot_open(database_path))?,
        )
        .ok_or_else(|| cannot_open(database_path))?;
    Ok(NamespaceRecordState {
        current_identity,
        last_sequence,
        prepared_sequence,
        prepare_offset,
        valid_bytes,
        interrupted_tail,
    })
}

fn encode_transition_record(
    sequence: u64,
    old_identity: FileIdentity,
    new_identity: FileIdentity,
) -> [u8; TRANSITION_BYTES] {
    let mut record = [0_u8; TRANSITION_BYTES];
    record[..8].copy_from_slice(&TRANSITION_MAGIC);
    record[8] = TRANSITION_VERSION;
    record[16..24].copy_from_slice(&sequence.to_be_bytes());
    record[24..24 + IDENTITY_BYTES].copy_from_slice(&old_identity.to_namespace_bytes());
    record[49..49 + IDENTITY_BYTES].copy_from_slice(&new_identity.to_namespace_bytes());
    let checksum = transition_checksum(&record[..TRANSITION_CHECKSUM_OFFSET]);
    record[TRANSITION_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());
    record
}

fn encode_prepare_record(sequence: u64, current_identity: FileIdentity) -> [u8; PREPARE_BYTES] {
    let mut record = [0_u8; PREPARE_BYTES];
    record[..8].copy_from_slice(&PREPARE_MAGIC);
    record[8] = PREPARE_VERSION;
    record[16..24].copy_from_slice(&sequence.to_be_bytes());
    record[24..24 + IDENTITY_BYTES].copy_from_slice(&current_identity.to_namespace_bytes());
    let checksum = transition_checksum(&record[..PREPARE_CHECKSUM_OFFSET]);
    record[PREPARE_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());
    record
}

fn decode_prepare_record(record: &[u8; PREPARE_BYTES]) -> Option<(u64, FileIdentity)> {
    decode_identity_ledger_record(
        record,
        PREPARE_MAGIC,
        PREPARE_VERSION,
        PREPARE_CHECKSUM_OFFSET,
    )
}

fn encode_finish_record(sequence: u64, current_identity: FileIdentity) -> [u8; FINISH_BYTES] {
    let mut record = [0_u8; FINISH_BYTES];
    record[..8].copy_from_slice(&FINISH_MAGIC);
    record[8] = FINISH_VERSION;
    record[16..24].copy_from_slice(&sequence.to_be_bytes());
    record[24..24 + IDENTITY_BYTES].copy_from_slice(&current_identity.to_namespace_bytes());
    let checksum = transition_checksum(&record[..FINISH_CHECKSUM_OFFSET]);
    record[FINISH_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());
    record
}

fn decode_finish_record(record: &[u8; FINISH_BYTES]) -> Option<(u64, FileIdentity)> {
    decode_identity_ledger_record(record, FINISH_MAGIC, FINISH_VERSION, FINISH_CHECKSUM_OFFSET)
}

fn decode_identity_ledger_record(
    record: &[u8; TRANSITION_BYTES],
    magic: [u8; 8],
    version: u8,
    checksum_offset: usize,
) -> Option<(u64, FileIdentity)> {
    if record[..8] != magic
        || record[8] != version
        || record[9..16].iter().any(|byte| *byte != 0)
        || record[49..checksum_offset].iter().any(|byte| *byte != 0)
    {
        return None;
    }
    let mut checksum_bytes = [0_u8; 8];
    checksum_bytes.copy_from_slice(&record[checksum_offset..]);
    if u64::from_be_bytes(checksum_bytes) != transition_checksum(&record[..checksum_offset]) {
        return None;
    }
    let mut sequence_bytes = [0_u8; 8];
    sequence_bytes.copy_from_slice(&record[16..24]);
    let mut identity_bytes = [0_u8; IDENTITY_BYTES];
    identity_bytes.copy_from_slice(&record[24..24 + IDENTITY_BYTES]);
    Some((
        u64::from_be_bytes(sequence_bytes),
        FileIdentity::from_namespace_bytes(identity_bytes)?,
    ))
}

fn decode_transition_record(
    record: &[u8; TRANSITION_BYTES],
) -> Option<(u64, FileIdentity, FileIdentity)> {
    if record[..8] != TRANSITION_MAGIC
        || record[8] != TRANSITION_VERSION
        || record[9..16].iter().any(|byte| *byte != 0)
        || record[74..TRANSITION_CHECKSUM_OFFSET]
            .iter()
            .any(|byte| *byte != 0)
    {
        return None;
    }
    let mut checksum_bytes = [0_u8; 8];
    checksum_bytes.copy_from_slice(&record[TRANSITION_CHECKSUM_OFFSET..]);
    if u64::from_be_bytes(checksum_bytes)
        != transition_checksum(&record[..TRANSITION_CHECKSUM_OFFSET])
    {
        return None;
    }

    let mut sequence_bytes = [0_u8; 8];
    sequence_bytes.copy_from_slice(&record[16..24]);
    let mut old_encoded = [0_u8; IDENTITY_BYTES];
    old_encoded.copy_from_slice(&record[24..24 + IDENTITY_BYTES]);
    let mut new_encoded = [0_u8; IDENTITY_BYTES];
    new_encoded.copy_from_slice(&record[49..49 + IDENTITY_BYTES]);
    let old_identity = FileIdentity::from_namespace_bytes(old_encoded)?;
    let new_identity = FileIdentity::from_namespace_bytes(new_encoded)?;
    if old_identity == new_identity {
        return None;
    }
    Some((
        u64::from_be_bytes(sequence_bytes),
        old_identity,
        new_identity,
    ))
}

fn transition_checksum(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn try_lock(file: &File, mode: FileLockMode) -> Result<()> {
    AdvisoryFileLock::try_lock(file, mode).map_err(lock_error)
}

fn lock_error(error: FileLockError) -> FrankenError {
    match error {
        FileLockError::AlreadyLocked => FrankenError::Busy,
        FileLockError::Io(error) => FrankenError::Io(error),
    }
}

#[cfg(unix)]
fn downgrade_to_shared(file: &File) -> Result<()> {
    // `flock(LOCK_SH)` atomically converts this open file description's
    // exclusive lock to shared.
    try_lock(file, FileLockMode::Shared)
}

#[cfg(windows)]
fn downgrade_to_shared(file: &File) -> Result<()> {
    // LockFileEx has no atomic conversion operation.  `gate` remains exclusive
    // around this call, so no cooperating opener can observe the short gap.
    AdvisoryFileLock::unlock(file).map_err(lock_error)?;
    try_lock(file, FileLockMode::Shared)
}

fn release_gate(gate: &File) -> Result<()> {
    AdvisoryFileLock::unlock(gate).map_err(lock_error)
}

fn release_namespace_locks(gate: &File, use_file: &File) {
    let _ = AdvisoryFileLock::unlock(use_file);
    let _ = AdvisoryFileLock::unlock(gate);
}

fn cannot_open(path: &Path) -> FrankenError {
    FrankenError::CannotOpen {
        path: path.to_owned(),
    }
}

/// Windows advisory-lock sidecar policy for reserved-empty validation.
///
/// The post-main-open check permits the three sidecars that opening a Windows
/// VFS handle necessarily creates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowsLockSidecarPolicy {
    /// Reject every advisory-lock sidecar before the main handle is opened.
    RejectAll,
    /// Allow only the sidecars created by the accepted main-file handle.
    AllowExpected,
}

/// Validate that no recovery artifact belongs to a caller-reserved empty DB.
/// This function performs reads only and never creates or removes entries.
pub fn validate_reserved_database_artifacts(
    database_path: &Path,
    windows_lock_sidecars: WindowsLockSidecarPolicy,
) -> Result<()> {
    validate_stable_path(database_path)?;
    for suffix in ["-journal", "-wal", "-wal-fec", "-shm"] {
        reject_existing_entry(database_path, &sidecar_path(database_path, suffix))?;
    }

    #[cfg(windows)]
    if windows_lock_sidecars == WindowsLockSidecarPolicy::RejectAll {
        for suffix in ["-lock-shared", "-lock-reserved", "-lock-pending"] {
            reject_existing_entry(database_path, &sidecar_path(database_path, suffix))?;
        }
    }
    #[cfg(not(windows))]
    let _ = windows_lock_sidecars;

    let wal_fec_temp = sidecar_path(database_path, "-wal-fec").with_extension("wal-fec.tmp");
    reject_existing_entry(database_path, &wal_fec_temp)?;

    let parent = database_path
        .parent()
        .ok_or_else(|| cannot_open(database_path))?;
    let db_name = database_path
        .file_name()
        .ok_or_else(|| cannot_open(database_path))?
        .to_string_lossy();
    let segment_prefix = format!("{db_name}-wal-seg-");
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(&segment_prefix)
        {
            // GH#355 (bd-h5oaj) diagnostic: name the WAL-segment leftover that
            // blocked the reserved open.
            tracing::warn!(
                target: "fsqlite_vfs::reserved",
                database = %database_path.display(),
                wal_segment = %entry.file_name().to_string_lossy(),
                "reserved-database WAL segment present; refusing reserved open (CannotOpen)"
            );
            return Err(cannot_open(database_path));
        }
    }
    Ok(())
}

fn reject_existing_entry(database_path: &Path, candidate: &Path) -> Result<()> {
    match std::fs::symlink_metadata(candidate) {
        Ok(_) => {
            // GH#355 (bd-h5oaj) diagnostic: the reserved-builder bootstrap
            // returns an unsourced `CannotOpen`, which forced a black-box
            // elimination sweep on Windows. Name the exact artifact that blocked
            // the reserved open so an instrumented run is self-diagnosing.
            tracing::warn!(
                target: "fsqlite_vfs::reserved",
                database = %database_path.display(),
                artifact = %candidate.display(),
                "reserved-database artifact already present; refusing reserved open (CannotOpen)"
            );
            Err(cannot_open(database_path))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, FileTimes};
    use std::process::Command;
    use std::time::{Duration, UNIX_EPOCH};

    use tempfile::tempdir;

    use super::*;

    fn create_database(path: &Path, bytes: &[u8]) -> FileIdentity {
        fs::write(path, bytes).expect("create test database");
        let file = File::open(path).expect("open test database");
        FileIdentity::from_file(&file)
            .expect("query test database identity")
            .expect("native filesystem identity")
    }

    fn publish_generation(database: &Path, identity: FileIdentity) {
        let binding = PendingNamespaceOpen::begin(database, NamespaceOpenIntent::Shared)
            .expect("admit generation")
            .bind(identity)
            .expect("bind generation");
        binding.finish_bootstrap().expect("publish generation");
    }

    /// Enumerate every pathname the current process has open (`/proc/self/fd`).
    ///
    /// Portable fd-leak detection without the external `lsof` binary. Used to
    /// assert that a namespace teardown leaves no retained descriptor resolving
    /// to a database's sidecars (bd-97kjm asks #2/#3).
    #[cfg(target_os = "linux")]
    fn process_fd_targets() -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir("/proc/self/fd") else {
            return Vec::new();
        };
        entries
            .flatten()
            .filter_map(|entry| std::fs::read_link(entry.path()).ok())
            .collect()
    }

    /// Whether any open descriptor in this process resolves to `path`.
    #[cfg(target_os = "linux")]
    fn fd_open_to(path: &Path) -> bool {
        let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        process_fd_targets().iter().any(|target| {
            std::fs::canonicalize(target).unwrap_or_else(|_| target.clone()) == canonical
        })
    }

    #[test]
    fn new_generation_stays_exclusive_until_bootstrap_finishes() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("bootstrap.db");
        let identity = create_database(&database, b"");

        let pending = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit new generation");
        assert_eq!(pending.expected_identity(), None);
        let binding = pending.bind(identity).expect("bind new generation");
        assert!(binding.bootstrap_is_exclusive());
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));

        assert!(
            binding
                .finish_bootstrap_with_gate_release(|_| {
                    Err(FrankenError::internal(
                        "injected namespace gate release failure",
                    ))
                })
                .is_err()
        );
        assert!(
            matches!(
                *binding.lease.lock().expect("inspect bootstrap lease"),
                BindingLease::BootstrapUseShared { .. }
            ),
            "a gate-release error after downgrade must preserve the exact intermediate lock state"
        );
        assert!(binding.bootstrap_is_exclusive());
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));

        binding.finish_bootstrap().expect("finish bootstrap");
        assert!(!binding.bootstrap_is_exclusive());
        binding
            .finish_bootstrap()
            .expect("finishing bootstrap twice is harmless");

        let join = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("join live generation");
        assert_eq!(join.expected_identity(), Some(identity));
        let peer = join.bind(identity).expect("bind peer");
        assert!(!peer.bootstrap_is_exclusive());
    }

    #[test]
    fn binding_last_arc_drop_releases_shared_lease_cross_process() {
        const CHILD_DATABASE: &str = "FSQLITE_NS_BINDING_DROP_CHILD_DATABASE";
        const CHILD_EXPECT_TRANSITION: &str = "FSQLITE_NS_BINDING_DROP_CHILD_EXPECT_TRANSITION";

        if let Some(database) = std::env::var_os(CHILD_DATABASE) {
            let database = PathBuf::from(database);
            let identity =
                FileIdentity::from_file(&File::open(&database).expect("open child generation"))
                    .expect("query child generation identity")
                    .expect("native child generation identity");
            let transition = begin_database_namespace_generation_transition(&database, identity);
            if std::env::var_os(CHILD_EXPECT_TRANSITION).is_some() {
                transition.expect("last binding Arc drop releases shared lease cross-process");
            } else {
                assert!(matches!(transition, Err(FrankenError::Busy)));
            }
            return;
        }

        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("binding-drop.db");
        let identity = create_database(&database, b"generation");
        let binding = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit generation")
            .bind(identity)
            .expect("bind generation");
        binding.finish_bootstrap().expect("publish generation");
        let final_arc = Arc::clone(&binding);
        drop(binding);

        let run_child = |expect_transition: bool| {
            let mut command =
                Command::new(std::env::current_exe().expect("resolve test executable"));
            command
                .arg("--exact")
                .arg("namespace::tests::binding_last_arc_drop_releases_shared_lease_cross_process")
                .arg("--nocapture")
                .env(CHILD_DATABASE, &database);
            if expect_transition {
                command.env(CHILD_EXPECT_TRANSITION, "1");
            }
            let output = command.output().expect("run binding-drop child");
            assert!(
                output.status.success(),
                "child failed:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        };

        run_child(false);
        drop(final_arc);
        run_child(true);
    }

    #[test]
    fn readonly_existing_generation_preserves_namespace_records() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("readonly-existing.db");
        let identity = create_database(&database, b"existing generation");
        let writer = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit generation")
            .bind(identity)
            .expect("bind generation");
        writer.finish_bootstrap().expect("publish generation");
        drop(writer);

        let gate_path = sidecar_path(&database, GATE_SUFFIX);
        let use_path = sidecar_path(&database, USE_SUFFIX);
        let sentinel_modified = UNIX_EPOCH + Duration::from_hours(262_968);
        File::options()
            .write(true)
            .open(&use_path)
            .expect("open identity record for timestamp sentinel")
            .set_times(FileTimes::new().set_modified(sentinel_modified))
            .expect("set identity-record timestamp sentinel");
        let before_gate = fs::read(&gate_path).expect("snapshot gate record");
        let before_use = fs::read(&use_path).expect("snapshot identity record");
        let before_use_modified = fs::metadata(&use_path)
            .expect("identity record metadata")
            .modified()
            .expect("identity record modification time");

        let failed_pending =
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting)
                .expect("begin injected gate-release failure");
        let retained_gate: std::cell::RefCell<Option<File>> = std::cell::RefCell::new(None);
        assert!(
            failed_pending
                .bind_with_gate_release(identity, |gate| {
                    #[cfg(unix)]
                    retained_gate.replace(Some(gate.try_clone()?));
                    #[cfg(not(unix))]
                    let _ = &gate;
                    Err(FrankenError::internal(
                        "injected namespace gate release failure",
                    ))
                })
                .is_err()
        );

        let pending = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting)
            .expect("explicit cleanup releases the injected failed gate lease");
        assert_eq!(pending.expected_identity(), Some(identity));
        let reader = pending.bind(identity).expect("bind read-only generation");
        reader
            .validate_path_identity()
            .expect("read-only generation remains bound");
        reader
            .finish_bootstrap()
            .expect("shared read-only binding has no bootstrap transition");
        drop(reader);
        drop(retained_gate);

        assert_eq!(fs::read(&gate_path).expect("read gate record"), before_gate);
        assert_eq!(
            fs::read(&use_path).expect("read identity record"),
            before_use
        );
        assert_eq!(
            fs::metadata(&use_path)
                .expect("identity record metadata")
                .modified()
                .expect("identity record modification time"),
            before_use_modified,
            "read-only admission must not rewrite an unchanged identity record"
        );
    }

    #[test]
    fn readonly_admission_of_never_admitted_database_creates_no_sidecars() {
        // GH#140 / bd-daqmp: a read-only open of a database that no
        // FrankenSQLite ever admitted (e.g. a stock SQLite file) must be
        // byte-neutral for the whole family — no sidecar creation, no locks.
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("never-admitted.db");
        let identity = create_database(&database, b"stock-like database");
        let gate_path = sidecar_path(&database, GATE_SUFFIX);
        let use_path = sidecar_path(&database, USE_SUFFIX);
        assert!(!gate_path.exists() && !use_path.exists());

        let pending = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting)
            .expect("sidecar-less read-only admission must succeed");
        assert_eq!(pending.expected_identity(), None);
        assert!(
            !pending
                .has_quiescent_record_bytes()
                .expect("sidecar-less admission has no record")
        );
        let binding = pending
            .bind(identity)
            .expect("bind sidecar-less read-only admission");
        assert!(!binding.bootstrap_is_exclusive());
        binding
            .finish_bootstrap()
            .expect("sidecar-less binding has no bootstrap transition");
        drop(binding);

        assert!(
            !gate_path.exists(),
            "read-only admission must not create the gate sidecar"
        );
        assert!(
            !use_path.exists(),
            "read-only admission must not create the identity sidecar"
        );

        // A later writable admission still creates the namespace normally.
        let writer = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("subsequent shared admission")
            .bind(identity)
            .expect("bind shared generation");
        writer.finish_bootstrap().expect("publish generation");
        drop(writer);
        assert!(gate_path.exists() && use_path.exists());
    }

    #[test]
    fn readonly_admission_blocks_generation_transition_then_holds_use_lease() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("readonly-transition.db");
        let displaced = dir.path().join("readonly-transition.displaced.db");
        let original_identity = create_database(&database, b"original generation");
        let writer = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit generation")
            .bind(original_identity)
            .expect("bind generation");
        writer.finish_bootstrap().expect("publish generation");
        drop(writer);

        let pending_reader =
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting)
                .expect("begin read-only admission");
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));
        let reader = pending_reader
            .bind(original_identity)
            .expect("bind read-only generation");

        fs::rename(&database, &displaced).expect("displace original generation");
        let replacement_identity = create_database(&database, b"replacement generation");
        let stale_writer = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("writer joins the reader-held generation");
        assert_eq!(stale_writer.expected_identity(), Some(original_identity));
        assert!(matches!(
            stale_writer.bind(replacement_identity),
            Err(FrankenError::CannotOpen { .. })
        ));

        drop(reader);
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
                .expect("admission reaches exact identity validation")
                .bind(replacement_identity),
            Err(FrankenError::CannotOpen { .. })
        ));

        let replacement_staging = dir.path().join("readonly-transition.replacement.db");
        fs::rename(&database, &replacement_staging).expect("stage replacement");
        fs::rename(&displaced, &database).expect("restore old generation before guard");
        let mut transition =
            begin_database_namespace_generation_transition(&database, original_identity)
                .expect("guard exact old generation");
        fs::rename(&database, &displaced).expect("quarantine old generation under guard");
        fs::rename(&replacement_staging, &database).expect("activate replacement under guard");
        assert_eq!(
            transition
                .publish_replacement(replacement_identity)
                .expect("publish exact replacement"),
            NamespaceGenerationTransitionOutcome::Published
        );
        transition.finish().expect("finish replacement transition");
    }

    #[test]
    fn pool_drop_teardown_releases_retained_namespace_fd() {
        // bd-97kjm ask #2 (lsof-clean): a pool drop must fully release the
        // namespace state and the retained sidecar descriptor even while a
        // background reference still holds an `Arc` clone. Before teardown the
        // `use` lease blocks a generation transition and its fd is open; after
        // `quiesce()` the lease and its descriptor are gone for every clone.
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("pool-teardown.db");
        let identity = create_database(&database, b"pooled generation");
        let use_path = sidecar_path(&database, USE_SUFFIX);

        let binding = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit generation")
            .bind(identity)
            .expect("bind generation");
        binding.finish_bootstrap().expect("publish generation");

        // A pooled/background reference that outlives the intended pool drop.
        let lingering = Arc::clone(&binding);

        // Precondition: the retained `use` lease blocks a generation transition
        // and its descriptor is open — this is exactly the fd/lock that
        // outlives a pool drop on HEAD.
        assert!(
            matches!(
                begin_database_namespace_generation_transition(&database, identity),
                Err(FrankenError::Busy)
            ),
            "a live `use` lease must block a generation transition"
        );
        #[cfg(target_os = "linux")]
        assert!(
            fd_open_to(&use_path),
            "the retained identity-sidecar descriptor must be open before teardown"
        );

        // Teardown releases everything now, even though `lingering` still lives.
        binding.quiesce();
        assert!(binding.is_quiesced());
        assert!(
            lingering.is_quiesced(),
            "the shared lease is quiesced through every Arc clone"
        );
        assert_eq!(
            Arc::strong_count(&binding),
            2,
            "the background clone still references the binding"
        );

        // The retained descriptor is gone before any new holder opens it...
        #[cfg(target_os = "linux")]
        assert!(
            !fd_open_to(&use_path),
            "no retained descriptor may resolve to the identity sidecar after teardown"
        );
        // ...and the released `use` lock lets a fresh generation transition run.
        let transition = begin_database_namespace_generation_transition(&database, identity)
            .expect("teardown released the retained `use` lease");
        drop(transition);

        // Teardown is idempotent and the eventual Arc drops are pure no-ops.
        binding.quiesce();
        binding.finish_bootstrap().expect("quiesced bootstrap is inert");
        drop(binding);
        drop(lingering);
    }

    #[test]
    fn quarantine_generation_guard_releases_retained_fd() {
        // bd-97kjm ask #3: when a recovery flow quarantines/renames the main
        // file, the generation guard must detect the superseded generation and
        // release the retained sidecar descriptor, so no retained fd survives
        // pointing at the old generation. The quarantined old inode is never
        // written. GH#334/bd-a5zj5 note: `validate_path_identity` semantics are
        // unchanged; the guard only *adds* the on-mismatch release.
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("quarantine-guard.db");
        let quarantined = dir.path().join("quarantine-guard.quarantined.db");
        let identity = create_database(&database, b"live generation");
        let use_path = sidecar_path(&database, USE_SUFFIX);

        let binding = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit generation")
            .bind(identity)
            .expect("bind generation");
        binding.finish_bootstrap().expect("publish generation");

        // While the bound generation is live the guard is side-effect-free and
        // keeps the lease intact (a live connection is never cut off).
        binding
            .guard_generation()
            .expect("live generation passes the guard");
        assert!(!binding.is_quiesced());
        #[cfg(target_os = "linux")]
        assert!(
            fd_open_to(&use_path),
            "the retained descriptor stays open while the generation is live"
        );

        // Simulate recovery: quarantine the main file, install a fresh inode.
        fs::rename(&database, &quarantined).expect("quarantine main file");
        let old_bytes = fs::read(&quarantined).expect("snapshot quarantined inode bytes");
        let replacement = create_database(&database, b"replacement generation");
        assert_ne!(identity, replacement);

        // The guard now proves the bound generation is gone and releases state.
        assert!(matches!(
            binding.guard_generation(),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert!(
            binding.is_quiesced(),
            "a superseded generation releases the retained lease"
        );
        #[cfg(target_os = "linux")]
        assert!(
            !fd_open_to(&use_path),
            "no retained descriptor may survive a quarantined generation"
        );

        // No retained fd wrote to the quarantined old inode.
        assert_eq!(
            fs::read(&quarantined).expect("re-read quarantined inode bytes"),
            old_bytes,
            "the quarantined old inode must remain byte-identical"
        );

        // Idempotent: guarding a quiesced binding stays fail-closed.
        assert!(binding.guard_generation().is_err());
        binding.quiesce();
        drop(binding);
    }

    #[test]
    fn generation_guard_fails_closed_after_supersession_then_restore() {
        // bd-ep8y9: once a probe releases the lease (here via an ENOENT
        // rename-away supersession), a LATER successful probe must NOT report
        // the lock-less binding as a live generation. Before the fix
        // guard_generation returned Ok after the file was renamed back, leaving
        // this process writing with no advisory locks while a second process
        // could win admission on the released -ns-use/-ns-gate locks —
        // cross-process split-brain.
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("fail-closed.db");
        let moved = dir.path().join("fail-closed.moved.db");
        let identity = create_database(&database, b"live generation");

        let binding = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit generation")
            .bind(identity)
            .expect("bind generation");
        binding.finish_bootstrap().expect("publish generation");
        binding
            .guard_generation()
            .expect("live generation passes the guard");
        assert!(!binding.is_quiesced());

        // Rename the main file off the stable path: an ENOENT supersession
        // releases the lease (quiesce) and reports Err.
        fs::rename(&database, &moved).expect("rename main file away");
        assert!(matches!(
            binding.guard_generation(),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert!(
            binding.is_quiesced(),
            "an ENOENT supersession releases the lease"
        );

        // Restore the ORIGINAL inode at the stable path. The path now names the
        // bound identity again, but the lease is already quiesced (lock-free):
        // the guard MUST stay fail-closed rather than resurrect a lock-less
        // binding that a second writer could race.
        fs::rename(&moved, &database).expect("rename main file back");
        assert!(
            binding.guard_generation().is_err(),
            "a quiesced binding must fail closed even once its identity reappears"
        );
        assert!(binding.is_quiesced(), "the lease stays quiesced");

        binding.quiesce();
        drop(binding);
    }

    #[test]
    #[cfg(unix)]
    fn generation_guard_transient_probe_error_keeps_lease() {
        // bd-ep8y9 core: a transient stat failure (EACCES here, via a parent
        // directory stripped of search permission) must NOT quiesce a still-live
        // binding. Before the fix ANY validate_path_identity error quiesced,
        // releasing the namespace locks of a live writer -> split-brain. The
        // generation is installed the whole time; only the probe transiently
        // cannot read it.
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempdir().expect("tempdir");
        let guarded = dir.path().join("transient");
        fs::create_dir(&guarded).expect("create guarded dir");
        let database = guarded.join("transient.db");
        let identity = create_database(&database, b"live generation");

        let binding = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit generation")
            .bind(identity)
            .expect("bind generation");
        binding.finish_bootstrap().expect("publish generation");
        binding
            .guard_generation()
            .expect("live generation passes the guard");
        assert!(!binding.is_quiesced());

        // Strip search permission from the parent directory so symlink_metadata
        // on the child fails with EACCES. If we are root (perms bypassed) the
        // probe still succeeds; skip the assertions rather than assert a false
        // negative.
        let original = fs::metadata(&guarded)
            .expect("stat guarded dir")
            .permissions();
        fs::set_permissions(&guarded, fs::Permissions::from_mode(0o000))
            .expect("strip guarded dir perms");
        let probe_blocked = fs::symlink_metadata(&database).is_err();

        if probe_blocked {
            // The transient probe failure surfaces Err WITHOUT quiescing.
            assert!(
                binding.guard_generation().is_err(),
                "a transient probe failure fails the operation closed"
            );
            assert!(
                !binding.is_quiesced(),
                "a transient probe failure must NOT release a live writer's lease (bd-ep8y9)"
            );
        }

        // Restore perms; the generation was live throughout, so the guard passes
        // and the lease — never released — is still intact.
        fs::set_permissions(&guarded, original).expect("restore guarded dir perms");
        binding
            .guard_generation()
            .expect("guard passes once the probe recovers");
        assert!(!binding.is_quiesced());

        binding.quiesce();
        drop(binding);
    }

    #[test]
    fn readonly_reopen_of_retained_generation_is_byte_neutral() {
        // bd-97kjm ask #1 residual guard (a410c2735 + bd-lcuoc): re-opening a
        // namespace-retained database READ-ONLY must not re-checkpoint or
        // otherwise mutate the main file or the persistent sidecars, and the
        // new teardown/guard primitives must themselves be byte-neutral.
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("readonly-neutral.db");
        let identity = create_database(&database, b"retained generation payload");
        publish_generation(&database, identity);

        let gate_path = sidecar_path(&database, GATE_SUFFIX);
        let use_path = sidecar_path(&database, USE_SUFFIX);
        let before_main = fs::read(&database).expect("snapshot main");
        let before_gate = fs::read(&gate_path).expect("snapshot gate record");
        let before_use = fs::read(&use_path).expect("snapshot identity record");

        let reader = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting)
            .expect("admit read-only generation")
            .bind(identity)
            .expect("bind read-only generation");
        reader
            .validate_path_identity()
            .expect("read-only generation remains bound");
        reader
            .guard_generation()
            .expect("guard is neutral while the generation is live");
        reader
            .finish_bootstrap()
            .expect("read-only binding has no bootstrap transition");
        reader.quiesce();
        assert!(reader.is_quiesced());
        drop(reader);

        assert_eq!(fs::read(&database).expect("re-read main"), before_main);
        assert_eq!(fs::read(&gate_path).expect("re-read gate record"), before_gate);
        assert_eq!(
            fs::read(&use_path).expect("re-read identity record"),
            before_use,
            "read-only teardown must not rewrite the identity record"
        );
    }

    #[test]
    fn readonly_existing_generation_admits_missing_records_without_creating_them() {
        // GH#140 / bd-daqmp contract update: missing records no longer fail
        // closed — a database never admitted by FrankenSQLite admits
        // SIDECAR-LESS. The unchanged core of this keeper is the second half:
        // the directory must stay byte-for-byte pristine either way.
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("readonly-missing-records.db");
        create_database(&database, b"external database");
        let entries_before = fs::read_dir(dir.path())
            .expect("list pristine namespace")
            .map(|entry| entry.expect("namespace entry").file_name())
            .collect::<std::collections::BTreeSet<_>>();

        let pending = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting)
            .expect("missing records admit sidecar-less (GH#140)");
        assert_eq!(pending.expected_identity(), None);
        drop(pending);

        let entries_after = fs::read_dir(dir.path())
            .expect("list namespace after sidecar-less admission")
            .map(|entry| entry.expect("namespace entry").file_name())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(entries_after, entries_before);
        assert!(!sidecar_path(&database, GATE_SUFFIX).exists());
        assert!(!sidecar_path(&database, USE_SUFFIX).exists());
    }

    #[test]
    fn readonly_existing_generation_refuses_corrupt_record_without_repairing_it() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("readonly-corrupt-record.db");
        let identity = create_database(&database, b"existing generation");
        let writer = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit generation")
            .bind(identity)
            .expect("bind generation");
        writer.finish_bootstrap().expect("publish generation");
        drop(writer);

        let use_path = sidecar_path(&database, USE_SUFFIX);
        fs::write(&use_path, b"corrupt identity record").expect("corrupt identity record");
        let before = fs::read(&use_path).expect("snapshot corrupt identity record");

        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(
            fs::read(&use_path).expect("read refused identity record"),
            before,
            "read-only admission must not repair or rewrite a corrupt record"
        );
    }

    #[test]
    fn readonly_existing_generation_refuses_main_identity_drift_without_rebinding() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("readonly-identity-drift.db");
        let displaced = dir.path().join("readonly-identity-drift.displaced.db");
        let original_identity = create_database(&database, b"original generation");
        let writer = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit generation")
            .bind(original_identity)
            .expect("bind generation");
        writer.finish_bootstrap().expect("publish generation");
        drop(writer);

        fs::rename(&database, &displaced).expect("displace original generation");
        let replacement_identity = create_database(&database, b"replacement generation");
        let use_path = sidecar_path(&database, USE_SUFFIX);
        let record_before = fs::read(&use_path).expect("snapshot original identity record");

        let pending = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting)
            .expect("read original recorded identity");
        assert_eq!(pending.expected_identity(), Some(original_identity));
        assert!(matches!(
            pending.bind(replacement_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(
            fs::read(&use_path).expect("read refused identity record"),
            record_before,
            "read-only identity refusal must not rebind the record to a replacement file"
        );
    }

    #[test]
    fn quiescent_rebind_repairs_stale_record_but_live_join_fails_closed() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("quiescent-rebind.db");
        let displaced = dir.path().join("quiescent-rebind.displaced.db");
        let original_identity = create_database(&database, b"original generation");
        let original = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit original generation")
            .bind(original_identity)
            .expect("bind original generation");
        original
            .finish_bootstrap()
            .expect("publish original generation");
        drop(original);

        fs::rename(&database, &displaced).expect("displace original generation");
        let replacement_identity = create_database(&database, b"replacement generation");
        let use_path = sidecar_path(&database, USE_SUFFIX);
        let stale_record = fs::read(&use_path).expect("snapshot stale identity record");

        let ordinary = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("obtain quiescent namespace exclusively");
        assert_eq!(ordinary.expected_identity(), None);
        assert!(ordinary.has_quiescent_record_bytes().unwrap());
        assert!(matches!(
            ordinary.bind(replacement_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(
            fs::read(&use_path).expect("read preserved stale record"),
            stale_record,
            "ordinary admission must retain fail-closed replacement semantics"
        );

        let wrong_generation = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("reacquire quiescent namespace for identity check");
        assert!(matches!(
            wrong_generation.bind_replacing_quiescent_record(original_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(
            fs::read(&use_path).expect("read record after rejected identity"),
            stale_record,
            "repair must validate the current pathname identity before rewriting"
        );

        let mut transition_bearing_record = stale_record.clone();
        transition_bearing_record.push(0x7f);
        fs::write(&use_path, &transition_bearing_record)
            .expect("append simulated transition evidence");
        let transition_bearing =
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
                .expect("reacquire namespace with transition evidence");
        assert!(matches!(
            transition_bearing.bind_replacing_quiescent_record(replacement_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(
            fs::read(&use_path).expect("read preserved transition evidence"),
            transition_bearing_record,
            "repair must never discard namespace transition evidence"
        );
        fs::write(&use_path, &stale_record).expect("restore plain stale admission record");

        let replacement = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("reacquire quiescent namespace exclusively")
            .bind_replacing_quiescent_record(replacement_identity)
            .expect("replace copied machine-local namespace record");
        replacement
            .finish_bootstrap()
            .expect("publish replacement namespace generation");

        let joined = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("join live replacement generation");
        assert_eq!(joined.expected_identity(), Some(replacement_identity));
        assert!(!joined.has_quiescent_record_bytes().unwrap());
        let replacement_record = fs::read(&use_path).expect("snapshot replacement record");
        assert!(matches!(
            joined.bind_replacing_quiescent_record(replacement_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(
            fs::read(&use_path).expect("read live replacement record"),
            replacement_record,
            "a live joined generation must never enter the repair path"
        );
    }

    #[test]
    fn quiescent_rebind_collapses_completed_copied_transition_history() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("completed-ledger-source.db");
        let displaced = dir.path().join("completed-ledger-source.displaced.db");
        let original_identity = create_database(&database, b"original generation");
        publish_generation(&database, original_identity);

        let mut transition =
            begin_database_namespace_generation_transition(&database, original_identity)
                .expect("prepare generation transition");
        fs::rename(&database, &displaced).expect("displace original generation");
        let replacement_identity = create_database(&database, b"replacement generation");
        transition
            .publish_replacement(replacement_identity)
            .expect("publish replacement generation");
        transition.finish().expect("finish replacement generation");

        let source_use_path = sidecar_path(&database, USE_SUFFIX);
        let terminal_ledger_len = fs::metadata(&source_use_path).unwrap().len();
        assert!(
            terminal_ledger_len > RECORD_BYTES as u64,
            "completed transition must leave durable history for this keeper"
        );
        let reopened = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit current terminal namespace")
            .bind_replacing_quiescent_record(replacement_identity)
            .expect("retain current terminal namespace history");
        reopened
            .finish_bootstrap()
            .expect("publish current namespace");
        drop(reopened);
        assert_eq!(
            fs::metadata(&source_use_path).unwrap().len(),
            terminal_ledger_len,
            "an already-current terminal ledger must not be rewritten"
        );
        let copied = dir.path().join("completed-ledger-copy.db");
        fs::copy(&database, &copied).expect("copy replacement main database");
        for suffix in [GATE_SUFFIX, USE_SUFFIX] {
            fs::copy(
                sidecar_path(&database, suffix),
                sidecar_path(&copied, suffix),
            )
            .expect("copy namespace sidecar");
        }
        let copied_file = File::open(&copied).expect("open copied main database");
        let copied_identity = FileIdentity::from_file(&copied_file)
            .expect("query copied main identity")
            .expect("native copied main identity");

        let rebound = PendingNamespaceOpen::begin(&copied, NamespaceOpenIntent::Shared)
            .expect("admit copied completed namespace")
            .bind_replacing_quiescent_record(copied_identity)
            .expect("collapse terminal copied transition history");
        rebound
            .finish_bootstrap()
            .expect("publish copied generation");
        assert_eq!(
            fs::metadata(sidecar_path(&copied, USE_SUFFIX))
                .unwrap()
                .len(),
            RECORD_BYTES as u64,
            "copied terminal history should collapse to one current base record"
        );
    }

    #[test]
    fn live_generation_rejects_replacement_identity_then_requires_guarded_transition() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("replace.db");
        let displaced = dir.path().join("replace.displaced.db");
        let first_identity = create_database(&database, b"first");
        let first = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit first")
            .bind(first_identity)
            .expect("bind first");
        first.finish_bootstrap().expect("finish first bootstrap");

        fs::rename(&database, &displaced).expect("displace main path");
        let replacement_identity = create_database(&database, b"replacement");
        assert!(matches!(
            first.validate_path_identity(),
            Err(FrankenError::CannotOpen { .. })
        ));

        let stale_join = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admission reads live record");
        assert_eq!(stale_join.expected_identity(), Some(first_identity));
        assert!(matches!(
            stale_join.bind(replacement_identity),
            Err(FrankenError::CannotOpen { .. })
        ));

        drop(first);
        let replacement_staging = dir.path().join("replace.replacement.db");
        fs::rename(&database, &replacement_staging).expect("stage replacement");
        fs::rename(&displaced, &database).expect("restore first generation");
        let mut transition =
            begin_database_namespace_generation_transition(&database, first_identity)
                .expect("guard first generation");
        fs::rename(&database, &displaced).expect("quarantine first generation under guard");
        fs::rename(&replacement_staging, &database).expect("activate replacement under guard");
        transition
            .publish_replacement(replacement_identity)
            .expect("publish replacement generation");
        transition.finish().expect("finish guarded transition");

        let replacement = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit published replacement")
            .bind(replacement_identity)
            .expect("bind published replacement");
        replacement
            .finish_bootstrap()
            .expect("finish replacement bootstrap");
        replacement
            .validate_path_identity()
            .expect("replacement remains bound");
    }

    #[test]
    fn guarded_generation_transition_reopens_replacement_and_supports_exact_rollback() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("recover.db");
        let quarantine = dir.path().join("recover.db.corrupt");
        let replacement_staging = dir.path().join("recover.db.replacement");
        let old_identity = create_database(&database, b"corrupt generation");
        publish_generation(&database, old_identity);
        let replacement_identity =
            create_database(&replacement_staging, b"reconstructed generation");

        let mut transition =
            begin_database_namespace_generation_transition(&database, old_identity)
                .expect("guard old namespace generation");
        fs::rename(&database, &quarantine).expect("quarantine old generation");
        fs::rename(&replacement_staging, &database).expect("activate replacement");

        assert_eq!(
            transition
                .publish_replacement(replacement_identity)
                .expect("publish replacement namespace generation"),
            NamespaceGenerationTransitionOutcome::Published
        );
        assert_eq!(
            transition
                .publish_replacement(replacement_identity)
                .expect("classify same-guard exact retry"),
            NamespaceGenerationTransitionOutcome::AlreadyPublished
        );
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));

        fs::rename(&database, &replacement_staging).expect("stage replacement for rollback");
        fs::rename(&quarantine, &database).expect("restore old generation under guard");
        assert_eq!(
            transition
                .publish_replacement(old_identity)
                .expect("publish exact rollback"),
            NamespaceGenerationTransitionOutcome::Published
        );
        assert_eq!(transition.current_identity(), old_identity);

        fs::rename(&database, &quarantine).expect("requarantine old generation");
        fs::rename(&replacement_staging, &database).expect("reactivate replacement");
        assert_eq!(
            transition
                .publish_replacement(replacement_identity)
                .expect("republish replacement after rollback"),
            NamespaceGenerationTransitionOutcome::Published
        );
        assert_eq!(
            transition.finish().expect("finish replacement publication"),
            replacement_identity
        );

        let pending = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting)
            .expect("read-only reopen of replacement");
        assert_eq!(pending.expected_identity(), Some(replacement_identity));
        let replacement = pending
            .bind(replacement_identity)
            .expect("bind replacement identity");
        replacement
            .validate_path_identity()
            .expect("replacement path remains exact");
        drop(replacement);

        let ordinary = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("ordinary reopen after transition");
        assert_eq!(ordinary.expected_identity(), None);
        ordinary
            .bind(replacement_identity)
            .expect("bind ordinary replacement reopen")
            .finish_bootstrap()
            .expect("publish replacement reopen");
        assert_eq!(
            fs::read(&quarantine).expect("read quarantined generation"),
            b"corrupt generation"
        );
        assert!(sidecar_path(&database, GATE_SUFFIX).exists());
        assert!(sidecar_path(&database, USE_SUFFIX).exists());
    }

    #[test]
    fn generation_transition_rejects_live_peer_and_wrong_identities() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("exact.db");
        let unrelated = dir.path().join("unrelated.db");
        let old_identity = create_database(&database, b"old");
        let live = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit old generation")
            .bind(old_identity)
            .expect("bind old generation");
        live.finish_bootstrap().expect("publish old generation");
        let unrelated_identity = create_database(&unrelated, b"unrelated");

        assert!(matches!(
            begin_database_namespace_generation_transition(&database, old_identity),
            Err(FrankenError::Busy)
        ));
        drop(live);

        assert!(matches!(
            begin_database_namespace_generation_transition(&database, unrelated_identity),
            Err(FrankenError::CannotOpen { .. })
        ));

        let mut use_file = open_existing_transition_lock_file(&sidecar_path(&database, USE_SUFFIX))
            .expect("open unchanged namespace record");
        assert_eq!(
            read_identity_record(&mut use_file, &database).expect("read unchanged generation"),
            old_identity
        );
    }

    #[test]
    fn generation_transition_excludes_shared_admission_for_entire_mutation_window() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("admission-race.db");
        let quarantine = dir.path().join("admission-race.db.corrupt");
        let replacement_staging = dir.path().join("admission-race.db.replacement");
        let old_identity = create_database(&database, b"old");
        publish_generation(&database, old_identity);
        let replacement_identity = create_database(&replacement_staging, b"replacement");

        let mut transition =
            begin_database_namespace_generation_transition(&database, old_identity)
                .expect("guard before caller-owned mutation");
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));

        fs::rename(&database, &quarantine).expect("quarantine old generation");
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));
        fs::rename(&replacement_staging, &database).expect("activate replacement");
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));
        transition
            .publish_replacement(replacement_identity)
            .expect("publish replacement while still exclusive");
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));
        transition.finish().expect("finish transition");

        let pending = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting)
            .expect("replacement generation remains readable");
        assert_eq!(pending.expected_identity(), Some(replacement_identity));
        pending
            .bind(replacement_identity)
            .expect("finished transition admits exact replacement");
    }

    #[test]
    fn generation_transition_rejects_missing_and_malformed_records() {
        let dir = tempdir().expect("tempdir");
        let missing_database = dir.path().join("missing.db");
        let missing_old_identity = create_database(&missing_database, b"old");
        assert!(matches!(
            begin_database_namespace_generation_transition(&missing_database, missing_old_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert!(!sidecar_path(&missing_database, GATE_SUFFIX).exists());
        assert!(!sidecar_path(&missing_database, USE_SUFFIX).exists());

        let database = dir.path().join("malformed.db");
        let old_identity = create_database(&database, b"old");
        publish_generation(&database, old_identity);
        let use_path = sidecar_path(&database, USE_SUFFIX);
        fs::write(&use_path, b"malformed namespace record").expect("corrupt namespace record");
        let malformed_before = fs::read(&use_path).expect("snapshot malformed record");

        assert!(matches!(
            begin_database_namespace_generation_transition(&database, old_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(
            fs::read(&use_path).expect("read refused malformed record"),
            malformed_before
        );
    }

    #[test]
    fn generation_transition_detects_path_replacement_before_publication() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("race.db");
        let quarantine = dir.path().join("race.db.corrupt");
        let replacement_staging = dir.path().join("race.db.replacement");
        let displaced_replacement = dir.path().join("race.db.displaced");
        let old_identity = create_database(&database, b"old");
        publish_generation(&database, old_identity);
        let replacement_identity = create_database(&replacement_staging, b"replacement");
        let mut transition =
            begin_database_namespace_generation_transition(&database, old_identity)
                .expect("guard old generation");
        fs::rename(&database, &quarantine).expect("quarantine old generation");
        fs::rename(&replacement_staging, &database).expect("activate replacement");

        let result = transition.publish_replacement_inner(replacement_identity, || {
            fs::rename(&database, &displaced_replacement)
                .expect("displace replacement during transition");
            create_database(&database, b"racing replacement");
            Ok(())
        });
        assert!(matches!(result, Err(FrankenError::CannotOpen { .. })));
        assert!(matches!(
            transition.finish(),
            Err(FrankenError::CannotOpen { .. })
        ));

        drop(transition);
        let racing_identity =
            FileIdentity::from_file(&File::open(&database).expect("open racing replacement"))
                .expect("query racing replacement identity")
                .expect("native racing identity");
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
                .expect("admission reaches fail-closed record validation")
                .bind(racing_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(
            fs::read(displaced_replacement).expect("read displaced replacement"),
            b"replacement"
        );
    }

    #[test]
    fn interrupted_generation_transition_releases_locks_and_retries_exactly() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("interrupt.db");
        let quarantine = dir.path().join("interrupt.db.corrupt");
        let replacement_staging = dir.path().join("interrupt.db.replacement");
        let old_identity = create_database(&database, b"old");
        publish_generation(&database, old_identity);
        let replacement_identity = create_database(&replacement_staging, b"replacement");
        let mut transition =
            begin_database_namespace_generation_transition(&database, old_identity)
                .expect("guard old generation");
        fs::rename(&database, &quarantine).expect("quarantine old generation");
        fs::rename(&replacement_staging, &database).expect("activate replacement");

        let interrupted = transition.publish_replacement_inner(replacement_identity, || {
            Err(FrankenError::internal(
                "injected pre-publication interruption",
            ))
        });
        assert!(matches!(interrupted, Err(FrankenError::Internal(_))));
        drop(transition);
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
                .expect("admission reaches fail-closed record validation")
                .bind(replacement_identity),
            Err(FrankenError::CannotOpen { .. })
        ));

        let mut retry = begin_database_namespace_generation_transition(&database, old_identity)
            .expect("resume prepared transition with replacement already installed");
        assert_eq!(
            retry
                .publish_replacement(replacement_identity)
                .expect("retry interrupted transition"),
            NamespaceGenerationTransitionOutcome::Published
        );
        retry.finish().expect("finish retried transition");
    }

    #[test]
    fn prepared_transition_resumes_while_main_path_is_absent() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("absent.db");
        let quarantine = dir.path().join("absent.db.quarantined");
        let replacement_staging = dir.path().join("absent.db.replacement");
        let old_identity = create_database(&database, b"old");
        publish_generation(&database, old_identity);
        let replacement_identity = create_database(&replacement_staging, b"replacement");

        let transition = begin_database_namespace_generation_transition(&database, old_identity)
            .expect("prepare transition before quarantine");
        fs::rename(&database, &quarantine).expect("quarantine old generation");
        drop(transition);
        assert!(!database.exists());

        let mut resumed = begin_database_namespace_generation_transition(&database, old_identity)
            .expect("resume exact durable prepare while main path is absent");
        fs::rename(&replacement_staging, &database).expect("activate replacement after resume");
        resumed
            .publish_replacement(replacement_identity)
            .expect("publish replacement after absent-path resume");
        resumed.finish().expect("finish resumed transition");
    }

    #[test]
    fn partial_transition_and_prepare_writes_resume_exactly() {
        for (name, transition_prefix, prepare_prefix) in [
            ("partial-transition", 37_usize, 0_usize),
            ("complete-transition", TRANSITION_BYTES, 0_usize),
            ("partial-next-prepare", TRANSITION_BYTES, 37_usize),
        ] {
            let dir = tempdir().expect("tempdir");
            let database = dir.path().join(format!("{name}.db"));
            let quarantine = dir.path().join(format!("{name}.db.quarantined"));
            let replacement_staging = dir.path().join(format!("{name}.db.replacement"));
            let old_identity = create_database(&database, b"old");
            publish_generation(&database, old_identity);
            let replacement_identity = create_database(&replacement_staging, b"replacement");
            let mut transition =
                begin_database_namespace_generation_transition(&database, old_identity)
                    .expect("prepare exact transition");
            fs::rename(&database, &quarantine).expect("quarantine old generation");
            fs::rename(&replacement_staging, &database).expect("activate replacement");

            let record = encode_transition_record(1, old_identity, replacement_identity);
            let next_prepare = encode_prepare_record(2, replacement_identity);
            let append_offset = transition.append_offset;
            let use_file = transition
                .use_file
                .as_mut()
                .expect("transition retains use-sidecar descriptor");
            use_file
                .seek(SeekFrom::Start(append_offset))
                .expect("seek interrupted publication offset");
            use_file
                .write_all(&record[..transition_prefix])
                .expect("write requested transition prefix");
            use_file
                .write_all(&next_prepare[..prepare_prefix])
                .expect("write requested next-prepare prefix");
            use_file
                .sync_data()
                .expect("durably inject interrupted publication");
            drop(transition);

            let expected_recorded_identity = if transition_prefix == TRANSITION_BYTES {
                replacement_identity
            } else {
                old_identity
            };
            let mut resumed = begin_database_namespace_generation_transition(
                &database,
                expected_recorded_identity,
            )
            .expect("resume exact interrupted ledger state");
            if expected_recorded_identity == old_identity {
                assert_eq!(
                    resumed
                        .publish_replacement(replacement_identity)
                        .expect("complete exact interrupted transition"),
                    NamespaceGenerationTransitionOutcome::Published
                );
            }
            resumed.finish().expect("finish resumed publication");

            let pending =
                PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting)
                    .expect("admit completed replacement");
            assert_eq!(pending.expected_identity(), Some(replacement_identity));
        }
    }

    #[test]
    fn finish_error_retains_exclusive_retryable_guard() {
        const POISONED_DROP_CHILD_DATABASE: &str = "FSQLITE_NS_POISONED_DROP_CHILD_DATABASE";

        if let Some(database) = std::env::var_os(POISONED_DROP_CHILD_DATABASE) {
            let database = PathBuf::from(database);
            let identity =
                FileIdentity::from_file(&File::open(&database).expect("open child generation"))
                    .expect("query child generation identity")
                    .expect("native child generation identity");
            let mut dropped = begin_database_namespace_generation_transition(&database, identity)
                .expect("prepare transition for poisoned-drop proof");
            assert!(matches!(
                dropped.finish_inner(|| {
                    Err(FrankenError::internal(
                        "injected failure after complete finish bytes",
                    ))
                }),
                Err(FrankenError::Internal(_))
            ));
            drop(dropped);
            assert!(matches!(
                PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
                Err(FrankenError::Busy)
            ));
            return;
        }

        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("finish-retry.db");
        let identity = create_database(&database, b"generation");
        publish_generation(&database, identity);
        let mut transition = begin_database_namespace_generation_transition(&database, identity)
            .expect("prepare transition");

        let result = transition.finish_inner(|| {
            Err(FrankenError::internal(
                "injected failure after finish write before sync",
            ))
        });
        assert!(matches!(result, Err(FrankenError::Internal(_))));
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));
        assert_eq!(transition.finish().expect("retry exact finish"), identity);
        drop(transition);
        PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admission resumes after confirmed finish");

        let dropped_database = dir.path().join("finish-drop.db");
        let dropped_identity = create_database(&dropped_database, b"generation");
        publish_generation(&dropped_database, dropped_identity);
        let output = Command::new(std::env::current_exe().expect("resolve test executable"))
            .arg("--exact")
            .arg("namespace::tests::finish_error_retains_exclusive_retryable_guard")
            .arg("--nocapture")
            .env(POISONED_DROP_CHILD_DATABASE, &dropped_database)
            .output()
            .expect("run poisoned-drop child");
        assert!(
            output.status.success(),
            "poisoned-drop child failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        PendingNamespaceOpen::begin(&dropped_database, NamespaceOpenIntent::Shared)
            .expect("process exit releases intentionally leaked fail-stop locks");
    }

    #[test]
    fn partial_finish_resumes_exactly_and_foreign_finish_tail_is_rejected() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("partial-finish.db");
        let identity = create_database(&database, b"generation");
        publish_generation(&database, identity);
        let mut transition = begin_database_namespace_generation_transition(&database, identity)
            .expect("prepare finish interruption");
        let finish = encode_finish_record(1, identity);
        let append_offset = transition.append_offset;
        let use_file = transition
            .use_file
            .as_mut()
            .expect("transition retains use-sidecar descriptor");
        use_file
            .seek(SeekFrom::Start(append_offset))
            .expect("seek finish offset");
        use_file
            .write_all(&finish[..37])
            .expect("write exact partial finish");
        use_file.sync_data().expect("sync exact partial finish");
        drop(transition);

        let mut resumed = begin_database_namespace_generation_transition(&database, identity)
            .expect("reacquire prepared transition with partial finish");
        resumed.finish().expect("complete exact partial finish");
        drop(resumed);
        PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit after completed finish");

        let foreign_database = dir.path().join("foreign-finish.db");
        let foreign_identity = create_database(&foreign_database, b"generation");
        publish_generation(&foreign_database, foreign_identity);
        let mut foreign =
            begin_database_namespace_generation_transition(&foreign_database, foreign_identity)
                .expect("prepare foreign-tail proof");
        let foreign_append_offset = foreign.append_offset;
        let foreign_file = foreign
            .use_file
            .as_mut()
            .expect("transition retains use-sidecar descriptor");
        foreign_file
            .seek(SeekFrom::Start(foreign_append_offset))
            .expect("seek foreign finish offset");
        foreign_file
            .write_all(b"foreign finish tail")
            .expect("write foreign finish tail");
        foreign_file.sync_data().expect("sync foreign tail");
        drop(foreign);

        let mut refused =
            begin_database_namespace_generation_transition(&foreign_database, foreign_identity)
                .expect("reacquire guarded foreign tail");
        assert!(matches!(
            refused.finish(),
            Err(FrankenError::CannotOpen { .. })
        ));
        drop(refused);
        assert!(matches!(
            PendingNamespaceOpen::begin(&foreign_database, NamespaceOpenIntent::Shared)
                .expect("ordinary admission reaches fail-closed validation")
                .bind(foreign_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
    }

    #[test]
    fn transition_ledger_remains_usable_beyond_legacy_record_bound() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("long-lived.db");
        let identity = create_database(&database, b"generation");
        publish_generation(&database, identity);
        let use_path = sidecar_path(&database, USE_SUFFIX);
        let mut use_file = OpenOptions::new()
            .append(true)
            .open(&use_path)
            .expect("open long-lived namespace ledger");
        for sequence in 1..=1_025_u64 {
            use_file
                .write_all(&encode_prepare_record(sequence, identity))
                .expect("append historical prepare");
            use_file
                .write_all(&encode_finish_record(sequence, identity))
                .expect("append historical finish");
        }
        use_file.sync_data().expect("sync long-lived ledger");
        drop(use_file);

        let mut transition = begin_database_namespace_generation_transition(&database, identity)
            .expect("begin after more than 1,024 historical records");
        transition
            .finish()
            .expect("finish after legacy bound is exceeded");
    }

    #[test]
    fn exact_partial_prepare_append_is_repaired_but_foreign_tail_is_rejected() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("partial.db");
        let old_identity = create_database(&database, b"old");
        publish_generation(&database, old_identity);
        let use_path = sidecar_path(&database, USE_SUFFIX);
        let prepare = encode_prepare_record(1, old_identity);
        let mut use_file = OpenOptions::new()
            .append(true)
            .open(&use_path)
            .expect("open namespace record for interrupted prepare");
        use_file
            .write_all(&prepare[..37])
            .expect("write exact interrupted prefix");
        use_file.sync_data().expect("sync interrupted prefix");
        drop(use_file);

        begin_database_namespace_generation_transition(&database, old_identity)
            .expect("repair exact interrupted prepare")
            .finish()
            .expect("finish repaired no-op transition");

        let second_database = dir.path().join("foreign-tail.db");
        let second_old_identity = create_database(&second_database, b"second old");
        publish_generation(&second_database, second_old_identity);
        let second_use_path = sidecar_path(&second_database, USE_SUFFIX);
        let mut second_use_file = OpenOptions::new()
            .append(true)
            .open(&second_use_path)
            .expect("open second namespace record");
        second_use_file
            .write_all(b"foreign interrupted bytes")
            .expect("write foreign partial tail");
        second_use_file.sync_data().expect("sync foreign tail");
        drop(second_use_file);
        let foreign_before = fs::read(&second_use_path).expect("snapshot foreign tail");

        assert!(matches!(
            begin_database_namespace_generation_transition(&second_database, second_old_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(
            fs::read(&second_use_path).expect("read refused foreign tail"),
            foreign_before
        );
    }

    #[test]
    fn generation_transition_rejects_corrupt_or_unprepared_complete_transition_record() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("corrupt-transition.db");
        let unrelated = dir.path().join("corrupt-transition.replacement.db");
        let old_identity = create_database(&database, b"old");
        publish_generation(&database, old_identity);
        let replacement_identity = create_database(&unrelated, b"replacement");
        let use_path = sidecar_path(&database, USE_SUFFIX);
        let mut corrupt_transition =
            encode_transition_record(1, old_identity, replacement_identity);
        corrupt_transition[TRANSITION_CHECKSUM_OFFSET] ^= 0xff;
        let mut use_file = OpenOptions::new()
            .append(true)
            .open(&use_path)
            .expect("open namespace record");
        use_file
            .write_all(&corrupt_transition)
            .expect("write corrupt complete transition");
        use_file.sync_data().expect("sync corrupt transition");
        drop(use_file);
        let corrupt_before = fs::read(&use_path).expect("snapshot corrupt transition");

        assert!(matches!(
            begin_database_namespace_generation_transition(&database, old_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(
            fs::read(&use_path).expect("read refused corrupt transition"),
            corrupt_before
        );

        let unprepared_database = dir.path().join("unprepared-transition.db");
        let unprepared_replacement = dir.path().join("unprepared-transition.replacement.db");
        let unprepared_old_identity = create_database(&unprepared_database, b"old");
        publish_generation(&unprepared_database, unprepared_old_identity);
        let unprepared_replacement_identity =
            create_database(&unprepared_replacement, b"replacement");
        let unprepared_use_path = sidecar_path(&unprepared_database, USE_SUFFIX);
        let mut unprepared_use_file = OpenOptions::new()
            .append(true)
            .open(&unprepared_use_path)
            .expect("open unprepared namespace ledger");
        unprepared_use_file
            .write_all(&encode_transition_record(
                1,
                unprepared_old_identity,
                unprepared_replacement_identity,
            ))
            .expect("write valid checksummed transition without prepare");
        unprepared_use_file
            .sync_data()
            .expect("sync unprepared transition");
        drop(unprepared_use_file);

        assert!(matches!(
            begin_database_namespace_generation_transition(
                &unprepared_database,
                unprepared_old_identity
            ),
            Err(FrankenError::CannotOpen { .. })
        ));
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn generation_transition_rejects_hard_linked_replacement() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("hardlink.db");
        let quarantine = dir.path().join("hardlink.db.corrupt");
        let replacement_source = dir.path().join("hardlink-replacement.db");
        let old_identity = create_database(&database, b"old");
        publish_generation(&database, old_identity);
        let mut transition =
            begin_database_namespace_generation_transition(&database, old_identity)
                .expect("guard old generation");
        fs::rename(&database, &quarantine).expect("quarantine old generation");
        let replacement_identity = create_database(&replacement_source, b"replacement");
        fs::hard_link(&replacement_source, &database).expect("hard-link replacement into place");

        assert!(matches!(
            transition.publish_replacement(replacement_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn generation_transition_rejects_final_component_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("symlink.db");
        let quarantine = dir.path().join("symlink.db.corrupt");
        let replacement_source = dir.path().join("symlink-replacement.db");
        let old_identity = create_database(&database, b"old");
        publish_generation(&database, old_identity);
        let mut transition =
            begin_database_namespace_generation_transition(&database, old_identity)
                .expect("guard old generation");
        fs::rename(&database, &quarantine).expect("quarantine old generation");
        let replacement_identity = create_database(&replacement_source, b"replacement");
        symlink(&replacement_source, &database).expect("symlink replacement into place");

        assert!(matches!(
            transition.publish_replacement(replacement_identity),
            Err(FrankenError::CannotOpen { .. })
        ));
    }

    #[test]
    fn generation_transition_cross_process_exclusion_then_finish_releases_locks() {
        const CHILD_DATABASE: &str = "FSQLITE_NS_TRANSITION_CHILD_DATABASE";
        const CHILD_EXPECT_OPEN: &str = "FSQLITE_NS_TRANSITION_CHILD_EXPECT_OPEN";

        if let Some(database) = std::env::var_os(CHILD_DATABASE) {
            let database = PathBuf::from(database);
            let admission =
                PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReadOnlyExisting);
            if std::env::var_os(CHILD_EXPECT_OPEN).is_some() {
                admission.expect("successful finish releases both locks cross-process");
            } else {
                assert!(matches!(admission, Err(FrankenError::Busy)));
            }
            return;
        }

        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("cross-process.db");
        let quarantine = dir.path().join("cross-process.db.corrupt");
        let replacement_staging = dir.path().join("cross-process.db.replacement");
        let old_identity = create_database(&database, b"old");
        publish_generation(&database, old_identity);
        let replacement_identity = create_database(&replacement_staging, b"replacement");
        let mut transition =
            begin_database_namespace_generation_transition(&database, old_identity)
                .expect("guard old generation before mutation");

        let assert_child_admission = |expect_open: bool| {
            let mut command =
                Command::new(std::env::current_exe().expect("resolve test executable"));
            command
                .arg("--exact")
                .arg(
                    "namespace::tests::generation_transition_cross_process_exclusion_then_finish_releases_locks",
                )
                .arg("--nocapture")
                .env(CHILD_DATABASE, &database);
            if expect_open {
                command.env(CHILD_EXPECT_OPEN, "1");
            }
            let output = command.output().expect("run namespace transition child");
            assert!(
                output.status.success(),
                "child failed:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        };

        assert_child_admission(false);
        fs::rename(&database, &quarantine).expect("quarantine old generation");
        assert_child_admission(false);
        fs::rename(&replacement_staging, &database).expect("activate replacement");
        assert_child_admission(false);
        assert_eq!(
            transition
                .publish_replacement(replacement_identity)
                .expect("publish while cross-process admissions remain excluded"),
            NamespaceGenerationTransitionOutcome::Published
        );
        assert_child_admission(false);
        transition.finish().expect("finish exact replacement");
        assert_child_admission(true);
    }

    #[test]
    fn reserved_bootstrap_and_pending_drop_are_raii_exclusive() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("reserved.db");
        let identity = create_database(&database, b"");

        let abandoned =
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReservedExclusive)
                .expect("reserve namespace");
        drop(abandoned);

        let reserved =
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReservedExclusive)
                .expect("reserve after unwind")
                .bind(identity)
                .expect("bind reservation");
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));
        reserved.finish_bootstrap().expect("finish reservation");
        PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("shared admission after reservation")
            .bind(identity)
            .expect("join reserved generation");

        assert!(sidecar_path(&database, GATE_SUFFIX).exists());
        assert!(sidecar_path(&database, USE_SUFFIX).exists());
    }

    #[test]
    fn abandoned_private_cleanup_requires_exclusive_namespace_and_removes_exact_artifacts() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("transient.db");
        let identity = create_database(&database, b"candidate");
        let binding =
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReservedExclusive)
                .expect("reserve namespace")
                .bind(identity)
                .expect("bind reservation");
        binding.finish_bootstrap().expect("finish bootstrap");
        for suffix in [
            "-journal",
            "-wal",
            "-wal-fec",
            "-shm",
            "-lock-shared",
            "-lock-reserved",
            "-lock-pending",
            // hfdt-0117-committed-cleanup-scratch-allowlist-3mekls: current
            // fsqlite stamps every database it creates with this marker at
            // birth (bd-zywqc.5), unconditionally, so any real candidate
            // this function tears down carries one.
            ".fsqlite-migration-state",
        ] {
            fs::write(sidecar_path(&database, suffix), b"candidate artifact")
                .expect("seed exact candidate companion");
        }
        let wal_fec_temp = sidecar_path(&database, "-wal-fec").with_extension("wal-fec.tmp");
        fs::write(&wal_fec_temp, b"candidate rewrite artifact")
            .expect("seed exact WAL-FEC rewrite companion");

        assert!(
            !cleanup_abandoned_private_database(&database, identity)
                .expect("contention must fail closed"),
            "a live namespace binding must prevent transient cleanup"
        );
        assert!(database.exists());
        drop(binding);

        assert!(
            cleanup_abandoned_private_database(&database, identity)
                .expect("exclusive abandoned-candidate cleanup")
        );
        assert!(!database.exists());
        for suffix in [
            "-journal",
            "-wal",
            "-wal-fec",
            "-shm",
            "-lock-shared",
            "-lock-reserved",
            "-lock-pending",
            ".fsqlite-migration-state",
            GATE_SUFFIX,
            USE_SUFFIX,
        ] {
            assert!(
                !sidecar_path(&database, suffix).exists(),
                "cleanup left exact companion {suffix}"
            );
        }
        assert!(!wal_fec_temp.exists());
    }

    #[test]
    fn abandoned_private_cleanup_preserves_replacement_and_namespace_on_identity_drift() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("drift.db");
        let displaced = dir.path().join("drift-owned.db");
        let identity = create_database(&database, b"owned candidate");
        let binding =
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReservedExclusive)
                .expect("reserve namespace")
                .bind(identity)
                .expect("bind reservation");
        binding.finish_bootstrap().expect("finish bootstrap");
        drop(binding);

        fs::rename(&database, &displaced).expect("displace owned candidate");
        fs::write(&database, b"replacement").expect("seed replacement");
        assert!(
            !cleanup_abandoned_private_database(&database, identity)
                .expect("identity drift must fail closed")
        );
        assert_eq!(
            fs::read(&database).expect("read replacement"),
            b"replacement"
        );
        assert_eq!(
            fs::read(&displaced).expect("read owned candidate"),
            b"owned candidate"
        );
        assert!(sidecar_path(&database, GATE_SUFFIX).exists());
        assert!(sidecar_path(&database, USE_SUFFIX).exists());
    }

    #[test]
    fn artifact_validation_rejects_segments_and_wal_fec_rewrite_temp() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("artifacts.db");
        create_database(&database, b"");
        validate_reserved_database_artifacts(&database, WindowsLockSidecarPolicy::RejectAll)
            .expect("artifact-free reservation");

        fs::write(
            dir.path().join("artifacts.db-wal-seg-not-an-epoch"),
            b"segment",
        )
        .expect("seed segment");
        assert!(matches!(
            validate_reserved_database_artifacts(&database, WindowsLockSidecarPolicy::RejectAll),
            Err(FrankenError::CannotOpen { .. })
        ));

        let second = dir.path().join("rewrite.db");
        create_database(&second, b"");
        let temp = sidecar_path(&second, "-wal-fec").with_extension("wal-fec.tmp");
        fs::write(temp, b"partial rewrite").expect("seed WAL-FEC rewrite temp");
        assert!(matches!(
            validate_reserved_database_artifacts(&second, WindowsLockSidecarPolicy::RejectAll),
            Err(FrankenError::CannotOpen { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn namespace_lockfile_symlink_is_rejected_without_following_it() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("nofollow.db");
        create_database(&database, b"");
        let target = dir.path().join("attacker-target");
        fs::write(&target, b"unchanged").expect("seed target");
        symlink(&target, sidecar_path(&database, GATE_SUFFIX)).expect("seed malicious symlink");

        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(fs::read(target).expect("read target"), b"unchanged");
    }
}
