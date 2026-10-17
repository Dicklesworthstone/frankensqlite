//! Owned destination I/O for native capture and retained-generation restore.
//!
//! Each operation is admitted into the connection context's runtime region and
//! uses its existing blocking pool. No inline or thread-per-call fallback is
//! permitted. A queued/running closure owns its file and buffer; abandonment
//! never lends it storage owned by the dropped capture future. An already
//! running syscall is not preemptible. Partial private files remain unselected.

use std::fs::File;
use std::io::{self, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_vfs::{FileIdentity, host_fs};

use super::{COPY_BYTES, FileDigest, checkpoint};
use super::super::{
    MAIN_FILE, MigrationBackup, RESTORE_PREFIX, WAL_FILE, invalid_backup,
    read_migration_backup_with_check, sync_directory,
};

pub(super) fn preflight(cx: &Cx) -> Result<asupersync::Cx> {
    checkpoint(cx)?;
    let native = cx.attached_native_cx().ok_or_else(|| {
        FrankenError::BackgroundWorkerFailed("backup requires an attached runtime context".to_owned())
    })?;
    native.checkpoint().map_err(|_| FrankenError::Interrupt)?;
    if native.blocking_pool_handle().is_none() {
        return Err(FrankenError::BackgroundWorkerFailed(
            "backup requires the connection runtime's blocking pool".to_owned(),
        ));
    }
    Ok(native)
}

struct Abandonment(Arc<AtomicBool>);

impl Drop for Abandonment {
    fn drop(&mut self) {
        // This cancels only our work, never the caller's context or siblings.
        self.0.store(true, Ordering::Release);
    }
}

/// The worker checks both context cancellation and dropped-awaiter abandonment.
/// Work owns all of its inputs. Once publication begins, its filesystem outcome
/// may outlive cancellation; callers must reconcile the durable selector rather
/// than interpret an error as proof of rollback.
pub(super) async fn run<T, F>(cx: &Cx, work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&dyn Fn() -> Result<()>) -> Result<T> + Send + 'static,
{
    let native = preflight(cx)?;
    let abandoned = Arc::new(AtomicBool::new(false));
    let guard = Abandonment(Arc::clone(&abandoned));
    let worker_cx = cx.create_child_for_spawn();
    let mut task = native.spawn_blocking(move |task_cx| {
        let check = || {
            if abandoned.load(Ordering::Acquire) {
                return Err(FrankenError::Interrupt);
            }
            checkpoint(&worker_cx)?;
            task_cx.checkpoint().map_err(|_| FrankenError::Interrupt)
        };
        check()?;
        work(&check)
    }).map_err(|error| {
        FrankenError::BackgroundWorkerFailed(format!("backup worker admission failed: {error:?}"))
    })?;
    let joined = task.join(&native).await;
    drop(guard);
    match joined {
        Ok(result) => result,
        Err(asupersync::runtime::JoinError::Cancelled(_)) => Err(FrankenError::Interrupt),
        Err(error) => Err(FrankenError::BackgroundWorkerFailed(format!(
            "backup worker failed: {error}"
        ))),
    }
}

/// One sequential stream, with at most one write outstanding. Moving the same
/// buffer through each task gives backpressure without a queue of database data.
pub(super) struct Destination {
    file: File,
    hasher: blake3::Hasher,
    bytes: u64,
}

impl Destination {
    pub(super) async fn create(cx: &Cx, path: &Path) -> Result<Self> {
        let path = path.to_path_buf();
        run(cx, move |check| {
            check()?;
            let file = host_fs::reserve_new_file(&path)?;
            check()?;
            Ok(Self { file, hasher: blake3::Hasher::new(), bytes: 0 })
        }).await
    }

    pub(super) async fn append(
        self,
        cx: &Cx,
        buffer: Vec<u8>,
        count: usize,
    ) -> Result<(Self, Vec<u8>)> {
        self.append_with(cx, buffer, count, |file, bytes| file.write_all(bytes)).await
    }

    async fn append_with<W>(
        mut self,
        cx: &Cx,
        buffer: Vec<u8>,
        count: usize,
        write: W,
    ) -> Result<(Self, Vec<u8>)>
    where
        W: FnOnce(&mut File, &[u8]) -> io::Result<()> + Send + 'static,
    {
        run(cx, move |check| {
            if buffer.len() > COPY_BYTES || count > buffer.len() {
                return Err(FrankenError::TooBig);
            }
            let bytes = self.bytes.checked_add(count as u64).ok_or(FrankenError::TooBig)?;
            check()?;
            write(&mut self.file, &buffer[..count])?;
            self.hasher.update(&buffer[..count]);
            self.bytes = bytes;
            check()?;
            Ok((self, buffer))
        }).await
    }

    pub(super) async fn finish(self, cx: &Cx) -> Result<FileDigest> {
        self.finish_with(cx, |file| file.sync_all()).await
    }

    async fn finish_with<S>(self, cx: &Cx, sync: S) -> Result<FileDigest>
    where
        S: FnOnce(&File) -> io::Result<()> + Send + 'static,
    {
        run(cx, move |check| {
            check()?;
            sync(&self.file)?;
            check()?;
            Ok(FileDigest { bytes: self.bytes, blake3: *self.hasher.finalize().as_bytes() })
        }).await
    }
}

impl MigrationBackup {
    /// Load the selected retained generation without scanning its files on the
    /// async executor. Uses the same receipt parser and verifier as
    /// `read_migration_backup`; absence is not an I/O or cancellation failure.
    ///
    /// Requires the caller's attached native context and blocking pool. This
    /// does not open the source database or create a runtime or destination.
    pub async fn read_selected_async(cx: &Cx, db_path: &str) -> Result<Option<Self>> {
        let db_path = db_path.to_owned();
        run(cx, move |check| read_migration_backup_with_check(&db_path, check)).await
    }

    /// Verify retained bytes on the caller's existing blocking pool, checking
    /// cancellation and abandonment between digest reads. This does not open
    /// a database engine, mutate the generation, or certify SQL integrity.
    pub async fn verify_async(&self, cx: &Cx) -> Result<()> {
        let backup = self.clone();
        run(cx, move |check| backup.verify_with_check(check)).await
    }

    /// Restore into a fresh private child of `parent` on the caller's blocking
    /// pool. All retained-byte verification, copying, readback and syncs run
    /// on that worker. A single 64 KiB application buffer is reused for main,
    /// WAL and readback; memory does not scale with the database size.
    ///
    /// The original generation and live source are never opened for writing.
    /// Only receipt-listed main/WAL files are restored. Required files are
    /// verified before creating output; copied bytes and reserved destination
    /// identities are independently checked before returning a database path.
    /// The existing synchronous restore API remains available without a runtime.
    ///
    /// Missing runtime/pool authority and pre-cancellation fail before output.
    /// Dropping this future abandons the result, not the worker-owned files:
    /// a running syscall can finish before the next cancellation checkpoint.
    /// Partial or complete unacknowledged output is retained, never removed or
    /// returned as a successful restore. No selector or completion marker is
    /// published. Abandonment does not cancel the caller's context.
    ///
    /// `parent` must already exist in a trusted cooperative namespace. Files
    /// are synced and Unix directory entries synchronized before success;
    /// Windows directory durability and power-loss behavior are not certified.
    /// Byte-faithful restoration may preserve pre-existing database corruption.
    pub async fn restore_to_new_directory_async(&self, cx: &Cx, parent: &Path) -> Result<PathBuf> {
        self.restore_with_sync_async(cx, parent, File::sync_all).await
    }

    async fn restore_with_sync_async<S>(
        &self,
        cx: &Cx,
        parent: &Path,
        sync: S,
    ) -> Result<PathBuf>
    where
        S: FnMut(&File) -> io::Result<()> + Send + 'static,
    {
        let backup = self.clone();
        let parent = parent.to_owned();
        run(cx, move |check| restore_owned(&backup, &parent, check, sync)).await
    }
}

/// Retain the actual output descriptor through readback and directory sync.
/// Equal bytes in a replacement file do not confer this reservation's identity.
struct RestoredFile {
    file: File,
    path: PathBuf,
    identity: FileIdentity,
    expected: FileDigest,
}

fn restore_owned(
    backup: &MigrationBackup,
    parent: &Path,
    check: &dyn Fn() -> Result<()>,
    mut sync: impl FnMut(&File) -> io::Result<()>,
) -> Result<PathBuf> {
    backup.verify_with_check(check)?;
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(COPY_BYTES).map_err(|_| FrankenError::OutOfMemory)?;
    buffer.resize(COPY_BYTES, 0);
    check()?;
    let directory = tempfile::Builder::new().prefix(RESTORE_PREFIX).tempdir_in(parent)?.keep();
    check()?;
    let main_path = directory.join(MAIN_FILE);
    let mut main = restore_file(
        &backup.main_path(), &main_path, &backup.receipt.main, &mut buffer, check, &mut sync,
    )?;
    let mut wal = if let Some(expected) = &backup.receipt.wal {
        Some(restore_file(
            &backup.directory.join(WAL_FILE), &directory.join(WAL_FILE), expected,
            &mut buffer, check, &mut sync,
        )?)
    } else {
        None
    };
    // Verify the whole family AFTER all file syncs, not just each file before
    // the next is copied. A later WAL operation must not conceal altered main.
    verify_restored(&mut main, &mut buffer, check)?;
    if let Some(wal) = wal.as_mut() {
        verify_restored(wal, &mut buffer, check)?;
    }
    validate_restore_set(&directory, &main, wal.as_ref())?;
    check()?;
    sync_directory(&directory)?;
    sync_directory(parent)?;
    validate_restore_set(&directory, &main, wal.as_ref())?;
    check()?;
    Ok(main_path)
}

fn restore_file(
    from: &Path,
    to: &Path,
    expected: &FileDigest,
    buffer: &mut [u8],
    check: &dyn Fn() -> Result<()>,
    sync: &mut impl FnMut(&File) -> io::Result<()>,
) -> Result<RestoredFile> {
    check()?;
    let mut source = host_fs::open_existing_regular_file_no_follow(from)?;
    if source.metadata()?.len() != expected.bytes {
        return Err(invalid_backup("retained restore file changed length"));
    }
    check()?;
    let mut file = host_fs::reserve_new_file(to)?;
    let identity = FileIdentity::from_file(&file)?.ok_or(FrankenError::Unsupported)?;
    transfer_exact(&mut source, expected, buffer, check, |bytes| {
        file.write_all(bytes)?;
        Ok(())
    })?;
    host_fs::validate_reserved_file_identity(to, identity)?;
    check()?;
    sync(&file)?;
    check()?;
    host_fs::validate_reserved_file_identity(to, identity)?;
    Ok(RestoredFile { file, path: to.to_owned(), identity, expected: expected.clone() })
}

/// Bounded exact-length read with an EOF check. Used for both copying and
/// descriptor readback so truncation, growth and same-size changes fail alike.
fn transfer_exact(
    input: &mut File,
    expected: &FileDigest,
    buffer: &mut [u8],
    check: &dyn Fn() -> Result<()>,
    mut consume: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let mut remaining = expected.bytes;
    let mut hasher = blake3::Hasher::new();
    while remaining != 0 {
        check()?;
        let wanted = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| FrankenError::TooBig)?;
        let count = match input.read(&mut buffer[..wanted]) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            return Err(invalid_backup("restore file ended before its receipt length"));
        }
        check()?;
        consume(&buffer[..count])?;
        hasher.update(&buffer[..count]);
        remaining -= count as u64;
    }
    loop {
        check()?;
        match input.read(&mut [0_u8; 1]) {
            Ok(0) => break,
            Ok(_) => return Err(invalid_backup("restore file exceeds its receipt length")),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    if input.metadata()?.len() != expected.bytes || hasher.finalize().as_bytes() != &expected.blake3 {
        return Err(invalid_backup("restore file does not match its receipt"));
    }
    check()
}

fn verify_restored(output: &mut RestoredFile, buffer: &mut [u8], check: &dyn Fn() -> Result<()>) -> Result<()> {
    check()?;
    host_fs::validate_reserved_file_identity(&output.path, output.identity)?;
    output.file.seek(SeekFrom::Start(0))?;
    transfer_exact(&mut output.file, &output.expected, buffer, check, |_| Ok(()))?;
    host_fs::validate_reserved_file_identity(&output.path, output.identity)
}

fn validate_restore_set(directory: &Path, main: &RestoredFile, wal: Option<&RestoredFile>) -> Result<()> {
    host_fs::validate_reserved_file_identity(&main.path, main.identity)?;
    if let Some(wal) = wal {
        host_fs::validate_reserved_file_identity(&wal.path, wal.identity)?;
    }
    let entries = host_fs::read_dir_paths(directory)?;
    if entries.len() != 1 + usize::from(wal.is_some())
        || entries.iter().any(|entry| entry != &main.path && wal.is_none_or(|wal| entry != &wal.path))
    {
        return Err(invalid_backup("unexpected file in restored database directory"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::runtime::RuntimeBuilder;
    use std::future::{Future, poll_fn};
    use std::pin::Pin;
    use std::sync::mpsc;
    use std::task::Poll;
    use std::time::{Duration, Instant};

    fn execute<F: Future>(future: F) -> F::Output {
        RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future)
    }

    fn context() -> Cx {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().expect("running test runtime"));
        cx
    }

    async fn entered<F: Future>(
        mut future: Pin<&mut F>,
        receiver: &mpsc::Receiver<std::thread::ThreadId>,
    ) -> std::thread::ThreadId {
        let deadline = Instant::now() + Duration::from_secs(10);
        poll_fn(|task| {
            assert!(future.as_mut().poll(task).is_pending(), "held syscall completed early");
            match receiver.try_recv() {
                Ok(thread) => Poll::Ready(thread),
                Err(mpsc::TryRecvError::Empty) => {
                    assert!(Instant::now() < deadline, "worker was not admitted");
                    task.waker().wake_by_ref();
                    Poll::Pending
                }
                Err(error) => panic!("worker failed before entering the syscall: {error}"),
            }
        }).await
    }

    #[test]
    fn stream_reuses_one_buffer_and_hashes_only_written_bytes() {
        execute(async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("database.db");
            let cx = context();
            let mut destination = Destination::create(&cx, &path).await.unwrap();
            let mut buffer = vec![0_u8; COPY_BYTES];
            let allocation = buffer.as_ptr() as usize;
            let mut expected = Vec::new();
            for (value, count) in [(0x13, COPY_BYTES), (0x29, COPY_BYTES), (0x47, 19)] {
                buffer.fill(value);
                expected.extend_from_slice(&buffer[..count]);
                (destination, buffer) = destination.append(&cx, buffer, count).await.unwrap();
                assert_eq!(buffer.as_ptr() as usize, allocation);
                assert_eq!(buffer.len(), COPY_BYTES);
            }
            let digest = destination.finish(&cx).await.unwrap();
            assert_eq!(host_fs::read(&path).unwrap(), expected);
            assert_eq!(digest.bytes, expected.len() as u64);
            assert_eq!(digest.blake3, *blake3::hash(&expected).as_bytes());
        });
    }

    #[test]
    fn empty_destination_is_synced_with_empty_digest_and_private_permissions() {
        execute(async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("database.db-wal");
            let cx = context();
            let digest = Destination::create(&cx, &path).await.unwrap().finish(&cx).await.unwrap();
            assert_eq!(digest.bytes, 0);
            assert_eq!(digest.blake3, *blake3::hash(&[]).as_bytes());
            assert_eq!(host_fs::metadata(&path).unwrap().len(), 0);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                assert_eq!(host_fs::metadata(&path).unwrap().permissions().mode() & 0o077, 0);
            }
        });
    }

    #[test]
    fn missing_runtime_pool_and_precancellation_never_create_output() {
        execute(async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("database.db");
            let detached = Cx::new();
            assert!(matches!(Destination::create(&detached, &path).await,
                Err(FrankenError::BackgroundWorkerFailed(_))));
            let no_pool = Cx::new();
            no_pool.set_native_cx(asupersync::Cx::current().unwrap().with_blocking_pool_handle(None));
            assert!(matches!(Destination::create(&no_pool, &path).await,
                Err(FrankenError::BackgroundWorkerFailed(_))));
            let cancelled = context();
            cancelled.cancel();
            assert!(matches!(Destination::create(&cancelled, &path).await,
                Err(FrankenError::Interrupt)));
            assert!(!path.exists());
            assert!(host_fs::read_dir_paths(&directory).unwrap().is_empty());
        });
    }

    #[test]
    fn existing_destination_and_partial_write_failure_preserve_bytes() {
        execute(async {
            let directory = tempfile::tempdir().unwrap().keep();
            let existing = directory.join("existing");
            host_fs::write(&existing, b"another owner").unwrap();
            let cx = context();
            assert!(Destination::create(&cx, &existing).await.is_err());
            assert_eq!(host_fs::read(&existing).unwrap(), b"another owner");
            let partial = directory.join("partial");
            let destination = Destination::create(&cx, &partial).await.unwrap();
            let result = destination.append_with(&cx, vec![0x5a; COPY_BYTES], COPY_BYTES, |file, bytes| {
                file.write_all(&bytes[..7])?;
                Err(io::Error::other("injected write failure"))
            }).await;
            assert!(matches!(result, Err(FrankenError::Io(_))));
            assert_eq!(host_fs::read(&partial).unwrap(), vec![0x5a; 7]);
        });
    }

    #[test]
    fn failed_or_cancelled_sync_never_returns_a_success_digest() {
        execute(async {
            for cancel in [false, true] {
                let directory = tempfile::tempdir().unwrap().keep();
                let path = directory.join("database.db");
                let cx = context();
                let destination = Destination::create(&cx, &path).await.unwrap();
                let (destination, _) = destination.append(&cx, b"complete bytes".to_vec(), 14).await.unwrap();
                let cancel_cx = cx.clone();
                let result = destination.finish_with(&cx, move |file| {
                    if cancel {
                        file.sync_all()?;
                        cancel_cx.cancel();
                        Ok(())
                    } else {
                        Err(io::Error::other("injected sync failure"))
                    }
                }).await;
                assert!(result.is_err());
                if cancel { assert!(matches!(result, Err(FrankenError::Interrupt))); }
                assert_eq!(host_fs::read(&path).unwrap(), b"complete bytes");
            }
        });
    }

    struct Completed(mpsc::Sender<()>);
    impl Drop for Completed {
        fn drop(&mut self) { let _ = self.0.send(()); }
    }

    #[test]
    fn dropped_awaiter_leaves_blocked_write_owned_by_worker_not_executor() {
        execute(async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("database.db");
            let cx = context();
            let destination = Destination::create(&cx, &path).await.unwrap();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let (completed_tx, completed_rx) = mpsc::channel();
            let completed = Completed(completed_tx);
            let caller = std::thread::current().id();
            let mut future = Box::pin(destination.append_with(
                &cx, vec![0x6b; COPY_BYTES], COPY_BYTES, move |file, bytes| {
                    let _owner = &completed;
                    entered_tx.send(std::thread::current().id()).map_err(io::Error::other)?;
                    release_rx.recv_timeout(Duration::from_secs(10)).map_err(io::Error::other)?;
                    file.write_all(bytes)
                },
            ));
            assert_ne!(entered(future.as_mut(), &entered_rx).await, caller);
            assert_eq!(host_fs::metadata(&path).unwrap().len(), 0);
            drop(future);
            // Dropping only the awaiter neither closes the worker's descriptor
            // nor destroys the moved buffer. It cannot authorize publication.
            release_tx.send(()).unwrap();
            completed_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            assert_eq!(host_fs::read(&path).unwrap(), vec![0x6b; COPY_BYTES]);
            assert!(!directory.join("receipt.json").exists());
            assert!(cx.checkpoint().is_ok(), "abandonment must not cancel the caller");
        });
    }

    #[test]
    fn blocked_sync_yields_executor_and_observes_cancellation_after_syscall() {
        execute(async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("database.db");
            let cx = context();
            let destination = Destination::create(&cx, &path).await.unwrap();
            let (destination, _) = destination.append(&cx, b"complete".to_vec(), 8).await.unwrap();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let caller = std::thread::current().id();
            let mut future = Box::pin(destination.finish_with(&cx, move |file| {
                entered_tx.send(std::thread::current().id()).map_err(io::Error::other)?;
                release_rx.recv_timeout(Duration::from_secs(10)).map_err(io::Error::other)?;
                file.sync_all()
            }));
            assert_ne!(entered(future.as_mut(), &entered_rx).await, caller);
            cx.cancel();
            release_tx.send(()).unwrap();
            assert!(matches!(future.await, Err(FrankenError::Interrupt)));
            assert_eq!(host_fs::read(&path).unwrap(), b"complete");
        });
    }

    #[test]
    fn dropped_publication_worker_preserves_selector_and_fresh_retry_succeeds() {
        use super::super::super::{
            GENERATION_PREFIX, GENERATION_RECEIPT, MAIN_FILE, capture, file_digest,
            migration_backup_receipt_path, publish_generation, read_migration_backup,
            read_migration_backup_with_check,
        };
        use std::cell::Cell;

        execute(async {
            let parent = tempfile::tempdir().unwrap().keep();
            let db = parent.join("source.db").to_str().unwrap().to_owned();
            host_fs::write(Path::new(&db), b"retained original").unwrap();
            let previous = capture(&db).unwrap();
            let selector = migration_backup_receipt_path(&db);
            let before = host_fs::read(&selector).unwrap();
            let directory = tempfile::Builder::new().prefix(GENERATION_PREFIX)
                .tempdir_in(&parent).unwrap().keep();
            host_fs::write_private_atomic(&directory.join(MAIN_FILE), &vec![0x57; 70001]).unwrap();
            let digest = file_digest(&directory.join(MAIN_FILE)).unwrap();
            let partial = directory.clone();
            let receipt = directory.join(GENERATION_RECEIPT);
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let (completed_tx, completed_rx) = mpsc::channel();
            let cx = context();
            let caller = std::thread::current().id();
            let owned_db = db.clone();
            let owned_previous = previous.clone();
            let mut future = Box::pin(run(&cx, move |check| {
                let held = Cell::new(false);
                let result = publish_generation(
                    &owned_db, directory, Some(owned_previous), digest, None, &|| {
                        check()?;
                        if receipt.exists() && !held.replace(true) {
                            entered_tx.send(std::thread::current().id()).map_err(io::Error::other)?;
                            release_rx.recv_timeout(Duration::from_secs(10)).map_err(io::Error::other)?;
                            check()?;
                        }
                        Ok(())
                    },
                );
                // This receipt comes from the actual closure after the real
                // publisher returned, not from the cancelled wrapper task.
                completed_tx.send(matches!(&result, Err(FrankenError::Interrupt))).unwrap();
                result
            }));
            assert_ne!(entered(future.as_mut(), &entered_rx).await, caller);
            assert!(partial.join(GENERATION_RECEIPT).exists());
            assert_eq!(host_fs::read(&selector).unwrap(), before);
            drop(future);
            release_tx.send(()).unwrap();
            assert!(completed_rx.recv_timeout(Duration::from_secs(10)).unwrap());
            assert_eq!(host_fs::read(&selector).unwrap(), before);
            previous.verify().unwrap();
            assert!(cx.checkpoint().is_ok());

            let retry_dir = tempfile::Builder::new().prefix(GENERATION_PREFIX)
                .tempdir_in(&parent).unwrap().keep();
            assert_ne!(retry_dir, partial);
            host_fs::write_private_atomic(&retry_dir.join(MAIN_FILE), b"new generation").unwrap();
            let digest = file_digest(&retry_dir.join(MAIN_FILE)).unwrap();
            let owned_db = db.clone();
            let owned_previous = previous.clone();
            let (retry, worker) = run(&cx, move |check| {
                let backup = publish_generation(&owned_db, retry_dir, Some(owned_previous), digest, None, check)?;
                Ok((backup, std::thread::current().id()))
            }).await.unwrap();
            assert_ne!(worker, caller);
            let owned_db = db.clone();
            let selected = run(&cx, move |check| {
                read_migration_backup_with_check(&owned_db, check)
            }).await.unwrap().unwrap();
            assert_eq!(selected.directory(), retry.directory());
            assert_eq!(retry.previous().unwrap().unwrap().directory(), previous.directory());
            assert_eq!(read_migration_backup(&db).unwrap().unwrap().directory(), retry.directory());
            assert_eq!(host_fs::read(&partial.join(MAIN_FILE)).unwrap(), vec![0x57; 70001]);
            previous.verify().unwrap();
        });
    }

    fn restore_fixture() -> (PathBuf, String, MigrationBackup) {
        use super::super::super::capture;
        let parent = tempfile::tempdir().unwrap().keep();
        let db = parent.join("source.db").to_str().unwrap().to_owned();
        host_fs::write(Path::new(&db), vec![0x39; 2 * COPY_BYTES + 17]).unwrap();
        host_fs::write(Path::new(&format!("{db}-wal")), vec![0x71; 3 * COPY_BYTES + 5]).unwrap();
        let backup = capture(&db).unwrap();
        (parent, db, backup)
    }

    fn restore_directories(parent: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<_> = host_fs::read_dir_paths(parent).unwrap().into_iter()
            .filter(|path| path.file_name().unwrap().to_string_lossy().starts_with(RESTORE_PREFIX))
            .collect();
        paths.sort();
        paths
    }

    #[test]
    fn async_restore_roundtrips_sqlite_history_and_keeps_evidence_unchanged() {
        use super::super::super::{capture, migration_backup_receipt_path};
        execute(async {
            for wal_mode in [false, true] {
                let parent = tempfile::tempdir().unwrap().keep();
                let db = parent.join("source.db").to_str().unwrap().to_owned();
                let source = rusqlite::Connection::open(&db).unwrap();
                if wal_mode {
                    source.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;").unwrap();
                }
                source.execute_batch(
                    "CREATE TABLE payload(id INTEGER PRIMARY KEY, value TEXT, bytes BLOB);
                     PRAGMA wal_checkpoint(TRUNCATE);
                     INSERT INTO payload VALUES(1, 'before', zeroblob(180001));",
                ).unwrap();
                let first = capture(&db).unwrap();
                source.execute("UPDATE payload SET value='after' WHERE id=1", []).unwrap();
                let second = capture(&db).unwrap();
                let selector = host_fs::read(&migration_backup_receipt_path(&db)).unwrap();
                let cx = context();
                let selected = MigrationBackup::read_selected_async(&cx, &db).await.unwrap().unwrap();
                assert_eq!(selected.directory(), second.directory());
                for (backup, expected) in [(&first, "before"), (&selected, "after")] {
                    backup.verify_async(&cx).await.unwrap();
                    let restored_path = backup.restore_to_new_directory_async(&cx, &parent).await.unwrap();
                    assert_ne!(restored_path, backup.main_path());
                    let restored = rusqlite::Connection::open(&restored_path).unwrap();
                    let row: (String, Vec<u8>) = restored.query_row(
                        "SELECT value, bytes FROM payload WHERE id=1", [], |row| Ok((row.get(0)?, row.get(1)?)),
                    ).unwrap();
                    assert_eq!(row, (expected.to_owned(), vec![0; 180001]));
                    let verdict: String = restored.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
                    assert_eq!(verdict, "ok");
                    restored.execute_batch("UPDATE payload SET value='copy only'; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
                    drop(restored);
                    backup.verify_async(&cx).await.unwrap();
                }
                assert_eq!(host_fs::read(&migration_backup_receipt_path(&db)).unwrap(), selector);
                let value: String = source.query_row("SELECT value FROM payload", [], |row| row.get(0)).unwrap();
                assert_eq!(value, "after");
            }
        });
    }

    #[test]
    fn async_restore_admission_and_tamper_fail_before_output_creation() {
        execute(async {
            let (parent, db, backup) = restore_fixture();
            let detached = Cx::new();
            assert!(matches!(backup.restore_to_new_directory_async(&detached, &parent).await,
                Err(FrankenError::BackgroundWorkerFailed(_))));
            let no_pool = Cx::new();
            no_pool.set_native_cx(asupersync::Cx::current().unwrap().with_blocking_pool_handle(None));
            assert!(matches!(backup.restore_to_new_directory_async(&no_pool, &parent).await,
                Err(FrankenError::BackgroundWorkerFailed(_))));
            let cancelled = context();
            cancelled.cancel();
            assert!(matches!(backup.restore_to_new_directory_async(&cancelled, &parent).await,
                Err(FrankenError::Interrupt)));
            assert!(matches!(MigrationBackup::read_selected_async(&cancelled, &db).await,
                Err(FrankenError::Interrupt)));
            let cx = context();
            let missing = parent.join("missing-parent");
            assert!(backup.restore_to_new_directory_async(&cx, &missing).await.is_err());
            assert!(!missing.exists());
            assert!(MigrationBackup::read_selected_async(&cx, missing.to_str().unwrap()).await.unwrap().is_none());
            let main = backup.main_path();
            let mut bytes = host_fs::read(&main).unwrap();
            bytes[0] ^= 1;
            host_fs::write(&main, &bytes).unwrap();
            assert!(matches!(backup.verify_async(&cx).await, Err(FrankenError::DatabaseCorrupt { .. })));
            assert!(matches!(backup.restore_to_new_directory_async(&cx, &parent).await,
                Err(FrankenError::DatabaseCorrupt { .. })));
            assert!(restore_directories(&parent).is_empty());
            assert_eq!(host_fs::read(&main).unwrap(), bytes);
        });
    }

    #[test]
    fn async_restore_distinguishes_absent_empty_and_nonempty_wal() {
        use super::super::super::capture;
        execute(async {
            for wal in [None, Some(Vec::new()), Some(vec![0x18; COPY_BYTES + 13])] {
                let parent = tempfile::tempdir().unwrap().keep();
                let db = parent.join("source.db").to_str().unwrap().to_owned();
                host_fs::write(Path::new(&db), b"retained main").unwrap();
                if let Some(bytes) = &wal {
                    host_fs::write(Path::new(&format!("{db}-wal")), bytes).unwrap();
                }
                let backup = capture(&db).unwrap();
                let cx = context();
                let path = backup.restore_to_new_directory_async(&cx, &parent).await.unwrap();
                let output = path.parent().unwrap();
                assert_eq!(host_fs::read(&path).unwrap(), b"retained main");
                assert_eq!(host_fs::read_dir_paths(output).unwrap().len(), 1 + usize::from(wal.is_some()));
                match &wal {
                    Some(bytes) => assert_eq!(host_fs::read(&output.join(WAL_FILE)).unwrap(), *bytes),
                    None => assert!(!output.join(WAL_FILE).exists()),
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt as _;
                    for entry in std::iter::once(output.to_owned()).chain(host_fs::read_dir_paths(output).unwrap()) {
                        assert_eq!(host_fs::metadata(&entry).unwrap().permissions().mode() & 0o077, 0);
                    }
                }
                backup.verify_async(&cx).await.unwrap();
            }
        });
    }

    #[test]
    fn async_restore_sync_failure_never_certifies_output_or_changes_selection() {
        use super::super::super::migration_backup_receipt_path;
        execute(async {
            for fail_at in [1, 2] {
                let (parent, db, backup) = restore_fixture();
                let selector = host_fs::read(&migration_backup_receipt_path(&db)).unwrap();
                let cx = context();
                let mut syncs = 0;
                let result = backup.restore_with_sync_async(&cx, &parent, move |file| {
                    syncs += 1;
                    if syncs == fail_at { return Err(io::Error::other("injected restore sync failure")); }
                    file.sync_all()
                }).await;
                assert!(matches!(result, Err(FrankenError::Io(_))));
                assert_eq!(restore_directories(&parent).len(), 1, "failed output is retained");
                assert_eq!(host_fs::read(&migration_backup_receipt_path(&db)).unwrap(), selector);
                backup.verify_async(&cx).await.unwrap();
                backup.restore_to_new_directory_async(&cx, &parent).await.unwrap();
                assert_eq!(restore_directories(&parent).len(), 2, "retry uses a fresh destination");
            }
        });
    }

    #[test]
    fn async_restore_checks_identity_readback_and_unlisted_recovery_files() {
        execute(async {
            for fault in ["replacement", "main-changed", "wal-changed", "journal", "shm", "marker"] {
                let (parent, _db, backup) = restore_fixture();
                let cx = context();
                let fault_parent = parent.clone();
                let mut syncs = 0;
                let result = backup.restore_with_sync_async(&cx, &parent, move |file| {
                    file.sync_all()?;
                    syncs += 1;
                    if syncs != 2 { return Ok(()); }
                    let output = restore_directories(&fault_parent).pop().unwrap();
                    let main = output.join(MAIN_FILE);
                    if fault == "replacement" {
                        let bytes = host_fs::read(&main).unwrap();
                        host_fs::rename(&main, &fault_parent.join("retained-output")).unwrap();
                        host_fs::write(&main, bytes).unwrap();
                    } else if fault == "main-changed" || fault == "wal-changed" {
                        let path = output.join(if fault == "main-changed" { MAIN_FILE } else { WAL_FILE });
                        let mut bytes = host_fs::read(&path).unwrap();
                        bytes[0] ^= 1;
                        host_fs::write(&path, bytes).unwrap();
                    } else {
                        let name = match fault {
                            "journal" => "database.db-journal",
                            "shm" => "database.db-shm",
                            _ => "database.db.fsqlite-migration-state",
                        };
                        host_fs::write(&output.join(name), b"unlisted recovery state").unwrap();
                    }
                    Ok(())
                }).await;
                assert!(result.is_err(), "{fault} must not return a successful restore path");
                backup.verify_async(&cx).await.unwrap();
                assert_eq!(restore_directories(&parent).len(), 1);
            }
        });
    }

    #[test]
    fn async_restore_rechecks_required_wal_after_preverification() {
        execute(async {
            let (parent, _db, backup) = restore_fixture();
            let wal = backup.wal_path().unwrap();
            let cx = context();
            let mut syncs = 0;
            let result = backup.restore_with_sync_async(&cx, &parent, move |file| {
                file.sync_all()?;
                syncs += 1;
                if syncs == 1 {
                    let mut bytes = host_fs::read(&wal).unwrap();
                    bytes[0] ^= 1;
                    host_fs::write(&wal, bytes).unwrap();
                }
                Ok(())
            }).await;
            assert!(matches!(result, Err(FrankenError::DatabaseCorrupt { .. })));
            assert_eq!(restore_directories(&parent).len(), 1);
        });
    }

    #[test]
    fn dropped_restore_awaiter_keeps_worker_ownership_and_stops_before_wal() {
        use super::super::super::migration_backup_receipt_path;
        execute(async {
            let (parent, db, backup) = restore_fixture();
            let selector = host_fs::read(&migration_backup_receipt_path(&db)).unwrap();
            let cx = context();
            let owned_backup = backup.clone();
            let owned_parent = parent.clone();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let (completed_tx, completed_rx) = mpsc::channel();
            let caller = std::thread::current().id();
            let mut future = Box::pin(run(&cx, move |check| {
                let mut first = true;
                let result = restore_owned(&owned_backup, &owned_parent, check, |file| {
                    if first {
                        first = false;
                        entered_tx.send(std::thread::current().id()).map_err(io::Error::other)?;
                        release_rx.recv_timeout(Duration::from_secs(10)).map_err(io::Error::other)?;
                    }
                    file.sync_all()
                });
                completed_tx.send(matches!(&result, Err(FrankenError::Interrupt))).unwrap();
                result
            }));
            assert_ne!(entered(future.as_mut(), &entered_rx).await, caller);
            let partial = restore_directories(&parent).pop().unwrap();
            assert_eq!(host_fs::read(&partial.join(MAIN_FILE)).unwrap(), vec![0x39; 2 * COPY_BYTES + 17]);
            assert!(!partial.join(WAL_FILE).exists());
            drop(future);
            release_tx.send(()).unwrap();
            assert!(completed_rx.recv_timeout(Duration::from_secs(10)).unwrap());
            assert!(!partial.join(WAL_FILE).exists());
            assert!(cx.checkpoint().is_ok(), "abandoning restore must not cancel its caller");
            assert_eq!(host_fs::read(&migration_backup_receipt_path(&db)).unwrap(), selector);
            backup.verify_async(&cx).await.unwrap();
            let retry = backup.restore_to_new_directory_async(&cx, &parent).await.unwrap();
            assert_ne!(retry.parent().unwrap(), partial.as_path());
            assert_eq!(host_fs::read(&retry.parent().unwrap().join(WAL_FILE)).unwrap(), vec![0x71; 3 * COPY_BYTES + 5]);
        });
    }
}
