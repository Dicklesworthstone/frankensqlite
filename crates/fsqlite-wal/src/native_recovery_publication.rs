//! Owned export publication on the caller's blocking pool.
//!
//! Only verified bytes arrive here; source recovery fences have already been
//! released. No destination descriptor is created on an async worker or held
//! by an awaiter. Once submitted, the blocking task owns the bytes, destination
//! and descriptor through write, sync, readback and directory synchronization.

use std::fs::File;
use std::io;
use std::path::PathBuf;

use asupersync::runtime::spawn_blocking;
use fsqlite_error::Result;
use fsqlite_types::cx::Cx;
use fsqlite_vfs::{Vfs, host_fs};

use super::{
    ExportReport, NativeVfs, checkpoint, refuse_destination_artifacts, verify_image, write_image,
};

pub(super) struct ExportImage {
    pub(super) destination: PathBuf,
    pub(super) bytes: Vec<u8>,
    pub(super) pages: usize,
    pub(super) wal_frames: u32,
    pub(super) repaired_frames: usize,
    pub(super) certificate_anchors: usize,
}

pub(super) async fn publish(cx: &Cx, image: ExportImage) -> Result<ExportReport> {
    publish_with_sync(cx, image, |file| file.sync_all()).await
}

// Keeping the real sync operation injectable makes failure and task-ownership
// tests deterministic without process-global hooks or a second executor.
async fn publish_with_sync<S>(cx: &Cx, image: ExportImage, sync: S) -> Result<ExportReport>
where
    S: FnMut(&mut File) -> io::Result<()> + Send + 'static,
{
    checkpoint(cx)?;
    let worker_cx = cx.create_child_for_spawn();
    spawn_blocking(move || image.write_with_sync(&worker_cx, sync)).await
}

impl ExportImage {
    fn write_with_sync<S>(self, cx: &Cx, mut sync: S) -> Result<ExportReport>
    where
        S: FnMut(&mut File) -> io::Result<()>,
    {
        let vfs = NativeVfs::new();
        checkpoint(cx)?;
        // Recheck after decode/queueing, BEFORE reserving an output. A stale
        // companion that arrived meanwhile must not leave an empty export.
        refuse_destination_artifacts(&vfs, cx, &self.destination)?;
        let mut output = host_fs::reserve_new_file(&self.destination)?;
        let publication: Result<blake3::Hash> = (|| {
            refuse_destination_artifacts(&vfs, cx, &self.destination)?;
            write_image(&mut output, cx, &self.bytes, &mut sync)?;
            let digest = verify_image(&mut output, cx, &self.bytes)?;
            vfs.sync_parent_directory(cx, &self.destination)?;
            Ok(digest)
        })();
        // Close on this worker, not on a potentially cancelled async awaiter.
        drop(output);
        let digest = match publication {
            Ok(digest) => digest,
            Err(error) => {
                eprintln!(
                    "Destination retained at {}; export completion is NOT certified",
                    self.destination.display()
                );
                return Err(error);
            }
        };
        Ok(ExportReport {
            destination: self.destination,
            pages: self.pages,
            wal_frames: self.wal_frames,
            repaired_frames: self.repaired_frames,
            certificate_anchors: self.certificate_anchors,
            digest,
            repaired_in_place: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_recovery::{DATABASE_HEADER_BYTES, IO_CHUNK, companion, request_context};
    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_error::FrankenError;
    use std::future::Future;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;
    use std::sync::mpsc;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    fn run<F: Future>(future: F) -> F::Output {
        RuntimeBuilder::current_thread()
            .blocking_threads(1, 2)
            .build()
            .unwrap()
            .block_on(future)
    }

    fn image(directory: &Path) -> ExportImage {
        let mut bytes = vec![0x3a; 2 * IO_CHUNK + 512];
        bytes[..16].copy_from_slice(b"SQLite format 3\0");
        ExportImage {
            destination: directory.join("published.db"),
            pages: bytes.len() / 512,
            bytes,
            wal_frames: 17,
            repaired_frames: 3,
            certificate_anchors: 1,
        }
    }

    #[test]
    fn publication_returns_exact_readback_and_recovery_counts() {
        run(async {
            let directory = tempfile::tempdir().unwrap();
            let image = image(directory.path());
            let expected = image.bytes.clone();
            let cx = request_context().unwrap();
            let report = publish(&cx, image).await.unwrap();
            assert_eq!(host_fs::read(&report.destination).unwrap(), expected);
            assert_eq!(report.digest, blake3::hash(&expected));
            assert_eq!(report.pages, expected.len() / 512);
            assert_eq!(report.wal_frames, 17);
            assert_eq!(report.repaired_frames, 3);
            assert_eq!(report.certificate_anchors, 1);
            assert!(!report.repaired_in_place);
        });
    }

    #[test]
    fn cancellation_before_submission_does_not_create_a_destination() {
        run(async {
            let directory = tempfile::tempdir().unwrap();
            let image = image(directory.path());
            let destination = image.destination.clone();
            let cx = request_context().unwrap();
            cx.cancel();
            assert!(matches!(publish(&cx, image).await, Err(FrankenError::Interrupt)));
            assert!(!destination.exists());
        });
    }

    #[test]
    fn destination_companions_created_after_decode_are_refused_before_reservation() {
        run(async {
            let directory = tempfile::tempdir().unwrap();
            let image = image(directory.path());
            let destination = image.destination.clone();
            let stale = companion(&destination, "-wal");
            host_fs::write(&stale, b"another database's committed WAL").unwrap();
            let cx = request_context().unwrap();
            assert!(matches!(publish(&cx, image).await,
                Err(FrankenError::CannotOpen { path }) if path == stale));
            assert!(!destination.exists());
            assert_eq!(host_fs::read(&stale).unwrap(), b"another database's committed WAL");
        });
    }

    #[test]
    fn destination_created_after_decode_is_not_overwritten() {
        run(async {
            let directory = tempfile::tempdir().unwrap();
            let image = image(directory.path());
            let destination = image.destination.clone();
            host_fs::write(&destination, b"another owner").unwrap();
            let cx = request_context().unwrap();
            assert!(publish(&cx, image).await.is_err());
            assert_eq!(host_fs::read(&destination).unwrap(), b"another owner");
        });
    }

    #[test]
    fn failed_body_sync_retains_output_with_an_invalid_header() {
        run(async {
            let directory = tempfile::tempdir().unwrap();
            let image = image(directory.path());
            let destination = image.destination.clone();
            let expected = image.bytes.clone();
            let cx = request_context().unwrap();
            assert!(publish_with_sync(&cx, image, |_| {
                Err(io::Error::other("injected body sync failure"))
            }).await.is_err());
            let output = host_fs::read(&destination).unwrap();
            assert_eq!(&output[..DATABASE_HEADER_BYTES], &[0; DATABASE_HEADER_BYTES]);
            assert_eq!(&output[DATABASE_HEADER_BYTES..], &expected[DATABASE_HEADER_BYTES..]);
        });
    }

    #[test]
    fn cancellation_after_body_sync_does_not_publish_a_header_or_receipt() {
        run(async {
            let directory = tempfile::tempdir().unwrap();
            let image = image(directory.path());
            let destination = image.destination.clone();
            let cx = request_context().unwrap();
            let mut syncs = 0;
            let result = image.write_with_sync(&cx, |file| {
                file.sync_all()?;
                syncs += 1;
                cx.cancel();
                Ok(())
            });
            assert!(matches!(result, Err(FrankenError::Interrupt)));
            assert_eq!(syncs, 1);
            let output = host_fs::read(&destination).unwrap();
            assert_eq!(&output[..DATABASE_HEADER_BYTES], &[0; DATABASE_HEADER_BYTES]);
        });
    }

    #[test]
    fn failed_final_sync_never_returns_a_success_receipt() {
        run(async {
            let directory = tempfile::tempdir().unwrap();
            let image = image(directory.path());
            let destination = image.destination.clone();
            let expected = image.bytes.clone();
            let cx = request_context().unwrap();
            let mut syncs = 0;
            assert!(publish_with_sync(&cx, image, move |file| {
                syncs += 1;
                if syncs == 2 { Err(io::Error::other("injected final sync failure")) }
                else { file.sync_all() }
            }).await.is_err());
            // A complete-looking file is still not a successful durability receipt.
            assert_eq!(host_fs::read(&destination).unwrap(), expected);
        });
    }

    #[test]
    fn corrupt_readback_never_returns_a_success_receipt() {
        run(async {
            let directory = tempfile::tempdir().unwrap();
            let image = image(directory.path());
            let destination = image.destination.clone();
            let cx = request_context().unwrap();
            let mut syncs = 0;
            let result = publish_with_sync(&cx, image, move |file| {
                syncs += 1;
                if syncs == 2 {
                    file.seek(SeekFrom::Start(200))?;
                    file.write_all(&[0xff])?;
                }
                file.sync_all()
            }).await;
            assert!(matches!(result, Err(FrankenError::DatabaseCorrupt { .. })));
            assert_eq!(host_fs::read(&destination).unwrap()[200], 0xff);
        });
    }

    struct Completion(mpsc::Sender<()>);

    impl Drop for Completion {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[test]
    fn blocking_sync_does_not_pin_async_worker_and_dropped_awaiter_keeps_output_owner() {
        run(async {
            let directory = tempfile::tempdir().unwrap();
            let image = image(directory.path());
            let destination = image.destination.clone();
            let expected = image.bytes.clone();
            let caller = std::thread::current().id();
            let cx = request_context().unwrap();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let (completed_tx, completed_rx) = mpsc::channel();
            let completion = Completion(completed_tx);
            let mut first_sync = true;
            let mut future = Box::pin(publish_with_sync(&cx, image, move |file| {
                let _keep_alive = &completion;
                if first_sync {
                    first_sync = false;
                    entered_tx.send(std::thread::current().id())
                        .map_err(io::Error::other)?;
                    release_rx.recv_timeout(Duration::from_secs(10))
                        .map_err(io::Error::other)?;
                }
                file.sync_all()
            }));
            let mut task = Context::from_waker(Waker::noop());
            assert!(matches!(future.as_mut().poll(&mut task), Poll::Pending));
            let worker = entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            assert_ne!(worker, caller, "fsync must not run on the async executor thread");
            let mut output = File::open(&destination).unwrap();
            let mut header = [0_u8; DATABASE_HEADER_BYTES];
            output.read_exact(&mut header).unwrap();
            assert_eq!(header, [0; DATABASE_HEADER_BYTES]);
            drop(output);
            // The task, not this suspended future, owns the file and image.
            drop(future);
            release_tx.send(()).unwrap();
            completed_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            let output = host_fs::read(&destination).unwrap();
            // A runtime may propagate abandonment as cancellation. Both a
            // completed export and an explicitly invalid-header partial export
            // are allowed; neither may lose the worker-owned bytes or file.
            assert_eq!(output.len(), expected.len());
            assert_eq!(&output[DATABASE_HEADER_BYTES..], &expected[DATABASE_HEADER_BYTES..]);
            assert!(
                output[..DATABASE_HEADER_BYTES] == expected[..DATABASE_HEADER_BYTES]
                    || output[..DATABASE_HEADER_BYTES] == [0; DATABASE_HEADER_BYTES]
            );
        });
    }
}
