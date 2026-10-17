//! Owned destination I/O for the native capture path.
//!
//! Each operation is admitted into the connection context's runtime region and
//! uses its existing blocking pool. No inline or thread-per-call fallback is
//! permitted. A queued/running closure owns its file and buffer; abandonment
//! never lends it storage owned by the dropped capture future. An already
//! running syscall is not preemptible. Partial private files remain unselected.

use std::fs::File;
use std::io::{self, Write as _};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_vfs::host_fs;

use super::{COPY_BYTES, FileDigest, checkpoint};

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
}
