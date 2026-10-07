//! Native journal-to-database restoration in a fresh private namespace.

use std::path::{Path, PathBuf};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{cx::Cx, flags::{SyncFlags, VfsOpenFlags}};
use fsqlite_vfs::{FileIdentity, host_fs, traits::{Vfs, VfsFile}};

#[cfg(unix)]
use fsqlite_vfs::UnixVfs as NativeVfs;
#[cfg(windows)]
use fsqlite_vfs::WindowsVfs as NativeVfs;

use super::{
    BlockHashes, SnapshotImageReceipt, SnapshotImageWriter, SnapshotManifest,
    SnapshotSpool, SnapshotSpoolState, checkpoint, corrupt, image_geometry, read_page,
    validate_header,
};
use crate::connection::Connection;

fn require_private_namespace(vfs: &NativeVfs, cx: &Cx, path: &Path) -> Result<()> {
    // Never join a live or previously admitted database, including an absent
    // main file whose namespace sidecars still belong to another generation.
    for suffix in ["-wal", "-shm", "-journal", "-fsqlite-ns-gate", "-fsqlite-ns-use"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        let sidecar = PathBuf::from(name);
        if vfs.path_entry_exists(cx, &sidecar)? {
            return Err(FrankenError::CannotOpen { path: sidecar });
        }
    }
    Ok(())
}

/// Read the actual final image, including page one, through the same canonical
/// block authenticator used by the writer. One page of scratch, never all rows.
async fn verify_image<F: VfsFile>(
    cx: &Cx,
    file: &F,
    manifest: &SnapshotManifest,
    expected_id: [u8; 32],
    maximum: u64,
) -> Result<SnapshotImageReceipt> {
    let (page_count, byte_len) = image_geometry(manifest, expected_id, maximum)?;
    if file.file_size(cx)? != byte_len {
        return Err(corrupt("restored snapshot file length disagrees with manifest"));
    }
    let mut scratch = vec![0; manifest.page_size() as usize];
    let mut image_hash = blake3::Hasher::new();
    for block in manifest.blocks() {
        let mut hashes = BlockHashes::new(manifest, block);
        for index in 0..block.page_count() {
            checkpoint(cx)?;
            let number = block.first_page() + index;
            let offset = (u64::from(number) - 1) * u64::from(manifest.page_size());
            read_page(file, cx, &mut scratch, offset).await?;
            if number == 1 {
                validate_header(&scratch, manifest.page_size(), page_count)?;
            }
            hashes.page(number, &scratch);
            image_hash.update(&scratch);
        }
        hashes.verify(block)?;
    }
    if file.file_size(cx)? != byte_len {
        return Err(corrupt("restored snapshot changed during verification"));
    }
    Ok(SnapshotImageReceipt {
        manifest_id: expected_id,
        page_size: manifest.page_size(),
        page_count,
        byte_len,
        image_blake3: *image_hash.finalize().as_bytes(),
    })
}

async fn validate_and_confirm(
    cx: &Cx,
    vfs: &NativeVfs,
    destination: &Path,
    identity: FileIdentity,
    manifest: &SnapshotManifest,
    maximum: u64,
) -> Result<SnapshotImageReceipt> {
    checkpoint(cx)?;
    require_private_namespace(vfs, cx, destination)?;
    host_fs::validate_reserved_file_identity(destination, identity)?;
    let name = destination.to_str().ok_or_else(|| FrankenError::CannotOpen {
        path: destination.to_owned(),
    })?;
    let parent = destination.parent().ok_or_else(|| FrankenError::CannotOpen {
        path: destination.to_owned(),
    })?;

    // Schema-only is the core's read-only, migration-free open. A normal open
    // may repair/rewrite a received image, destroying its manifest identity.
    // Do not retain independent main-file descriptors across this connection:
    // closing one can release this process's POSIX database locks.
    let validator = Connection::open_schema_only(name).await?;
    let validation = validator.validate_database_integrity_bounded(parent).await;
    let closed = validator.close().await;
    validation?;
    closed?;
    checkpoint(cx)?;
    require_private_namespace(vfs, cx, destination)?;
    host_fs::validate_reserved_file_identity(destination, identity)?;
    let (mut file, _) = vfs.open(cx, Some(destination), VfsOpenFlags::READWRITE)?;
    if file.file_identity()? != Some(identity) {
        return Err(FrankenError::BusyRecovery);
    }
    let receipt = verify_image(cx, &file, manifest, manifest.id(), maximum).await?;
    checkpoint(cx)?;
    file.sync(cx, SyncFlags::FULL)?;
    vfs.sync_parent_directory(cx, destination)?;
    host_fs::validate_reserved_file_identity(destination, identity)?;
    require_private_namespace(vfs, cx, destination)?;
    checkpoint(cx)?;
    Ok(receipt)
}

impl Connection {
    /// Restore a freshly reopened authenticated snapshot journal to a new
    /// native database file, verify its B-trees/indexes with the bounded image
    /// validator, reauthenticate the final bytes, and sync file and directory.
    ///
    /// Obtain `expected_id` from trusted control-plane state. The journal must
    /// be at the start of replay; its required checkpoint is verified before
    /// an image can finish. Admission rejects identity/size/state errors before
    /// creating a file. Existing files (even empty ones), links and SQLite or
    /// namespace sidecars are never reused, replaced, repaired or deleted.
    ///
    /// `destination` must be inside an already existing, caller-controlled
    /// private namespace. Keep its ancestry and contents exclusively owned
    /// throughout this call, and do not expose the path to database readers
    /// until success. This prepares a verified candidate; it does NOT atomically
    /// activate or replace a running replica or establish replication continuity.
    ///
    /// Failure, cancellation or loss of the response may leave a partial OR
    /// complete candidate. Retain it and the journal; quiesce abandoned backend
    /// I/O before inspecting them. A new restore attempt needs a freshly reopened
    /// journal and another absent destination. A complete, still-private output
    /// can instead use `confirm_restored_snapshot` to recover confirmation
    /// without replaying or rewriting it. No rollback-by-unlink is attempted.
    ///
    /// The byte cap governs the image, not total RSS or validation spool space.
    /// Replay retains the journal's decoder limits; verification uses one page
    /// of scratch and the existing disk-spooled semantic validator. That
    /// validator owns its default `ConnectionEnv`; `cx` governs replay/readback
    /// and is checked before and after validation, not raced against it. The
    /// receipt confirms the native VFS's sync contract, not hardware power-loss
    /// qualification, SQL foreign-key validity or transport authentication.
    pub async fn restore_snapshot_transfer<F: VfsFile>(
        cx: &Cx,
        spool: &mut SnapshotSpool<F>,
        destination: &Path,
        expected_id: [u8; 32],
        max_image_bytes: u64,
    ) -> Result<SnapshotImageReceipt> {
        checkpoint(cx)?;
        let manifest = spool.receiver().manifest().clone();
        image_geometry(&manifest, expected_id, max_image_bytes)?;
        if spool.state() != SnapshotSpoolState::Replaying || spool.record_count() != 0
            || spool.receiver().blocks_decoded() != 0
            || spool.receiver().retained_payload_bytes() != 0
        {
            return Err(FrankenError::BusyRecovery);
        }
        let vfs = NativeVfs::new();
        // Refuse the caller's final entry before pathname resolution can
        // resolve a symlink to a different, possibly absent, destination.
        if vfs.path_entry_exists(cx, destination)? {
            return Err(FrankenError::CannotOpen { path: destination.to_owned() });
        }
        let destination = vfs.full_pathname(cx, destination)?;
        // Reject non-UTF-8 before reservation; Connection uses UTF-8 filenames.
        if destination.to_str().is_none() {
            return Err(FrankenError::CannotOpen { path: destination });
        }
        require_private_namespace(&vfs, cx, &destination)?;
        let reservation = host_fs::reserve_new_file(&destination)?;
        let identity = FileIdentity::from_file(&reservation)?.ok_or(FrankenError::BusyRecovery)?;
        host_fs::validate_reserved_file_identity(&destination, identity)?;
        let (file, _) = vfs.open(cx, Some(&destination), VfsOpenFlags::READWRITE)?;
        if file.file_identity()? != Some(identity) {
            return Err(FrankenError::BusyRecovery);
        }
        let mut image = SnapshotImageWriter::create(
            cx, file, manifest.clone(), expected_id, max_image_bytes,
        )?;
        let written = spool.replay_into_image(cx, &mut image).await?;
        drop(image.into_file());
        drop(reservation);
        let verified = validate_and_confirm(
            cx, &vfs, &destination, identity, &manifest, max_image_bytes,
        ).await?;
        if written != verified {
            return Err(corrupt("restored snapshot changed after journal replay"));
        }
        Ok(verified)
    }

    /// Recover a lost restore confirmation by verifying an existing private
    /// candidate against the ORIGINAL trusted manifest, then freshly syncing
    /// its file and directory. No old receipt is accepted as evidence.
    ///
    /// This never creates, truncates, rewrites, repairs, renames or deletes the
    /// candidate. Every page is authenticated before the SQL engine opens it;
    /// the bounded semantic gate then runs read-only, followed by another exact
    /// byte verification and new sync barriers. Missing/partial/changed files,
    /// aliases and existing database namespaces fail closed. Unsupported image
    /// features rejected by the bounded validator do not trigger a fallback.
    ///
    /// Keep the same trusted manifest identity after an uncertain outcome; do
    /// not construct a replacement manifest from potentially damaged bytes.
    /// Quiesce any abandoned restore I/O BEFORE calling. The caller must own
    /// the candidate and its directory ancestry exclusively, and must not have
    /// activated/opened it as a writable database. Confirmation proves image
    /// contents and the native VFS sync contract, not the provenance of an
    /// arbitrary transfer journal, atomic replica activation or log continuity.
    /// The same resource/cancellation qualifications as restore apply.
    pub async fn confirm_restored_snapshot(
        cx: &Cx,
        destination: &Path,
        manifest: &SnapshotManifest,
        expected_id: [u8; 32],
        max_image_bytes: u64,
    ) -> Result<SnapshotImageReceipt> {
        checkpoint(cx)?;
        image_geometry(manifest, expected_id, max_image_bytes)?;
        let vfs = NativeVfs::new();
        require_private_namespace(&vfs, cx, destination)?;
        // Probe the supplied final entry BEFORE canonical pathname resolution,
        // otherwise a resolving VFS could hide a symlink/reparse-point alias.
        let probe = host_fs::open_existing_regular_file_no_follow(destination)?;
        let identity = FileIdentity::from_file(&probe)?.ok_or(FrankenError::BusyRecovery)?;
        let destination = vfs.full_pathname(cx, destination)?;
        if destination.to_str().is_none() {
            return Err(FrankenError::CannotOpen { path: destination });
        }
        require_private_namespace(&vfs, cx, &destination)?;
        // No CREATE flag occurs on this recovery path. Bind the VFS reader to
        // the exact file admitted by the no-follow probe above.
        let (file, _) = vfs.open(cx, Some(&destination), VfsOpenFlags::READONLY)?;
        if file.file_identity()? != Some(identity) {
            return Err(FrankenError::BusyRecovery);
        }
        let before = verify_image(cx, &file, manifest, expected_id, max_image_bytes).await?;
        // All independent main-file descriptors close BEFORE opening the
        // engine, so their drops cannot release the validator's POSIX locks.
        drop(file);
        drop(probe);
        let after = validate_and_confirm(
            cx, &vfs, &destination, identity, manifest, max_image_bytes,
        ).await?;
        if before != after {
            return Err(corrupt("restored snapshot changed during confirmation"));
        }
        Ok(after)
    }
}
