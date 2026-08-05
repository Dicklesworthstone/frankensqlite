//! Linux `io_uring`-backed VFS.
//!
//! This backend preserves Unix lock and SHM semantics by delegating lock/SHM
//! operations to [`UnixFile`]. Data-path read/write can use `io_uring` when it
//! is available at runtime, and transparently falls back to the Unix path when
//! `io_uring` initialization fails.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

#[cfg(feature = "linux-asupersync-uring")]
use asupersync::Cx as NativeCx;
#[cfg(feature = "linux-asupersync-uring")]
use asupersync::channel::oneshot;
#[cfg(feature = "linux-asupersync-uring")]
use asupersync::channel::oneshot::{Receiver, SendPermit};
#[cfg(feature = "linux-asupersync-uring")]
use io_uring::{IoUring, opcode, types};

use fsqlite_error::{FrankenError, Result};
use fsqlite_observability::{
    io_uring_latency_snapshot, record_io_uring_read_latency, record_io_uring_read_unix_fallback,
    record_io_uring_write_latency, record_io_uring_write_unix_fallback,
};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use tracing::{info, trace, warn};

use crate::shm::ShmRegion;
use crate::traits::{FileIdentity, Vfs, VfsFile, VfsWriteCompletion, VfsWriteCompletionSource};
use crate::unix::{UnixFile, UnixVfs};

#[cfg(not(feature = "linux-asupersync-uring"))]
compile_error!("fsqlite-vfs on Linux requires `linux-asupersync-uring`");

const IO_URING_READ_CONFORMAL_BREACH_MSG: &str = "io_uring read conformal tail breach";
const IO_URING_WRITE_CONFORMAL_BREACH_MSG: &str = "io_uring write conformal tail breach";
const IO_URING_READ_ERROR_FALLBACK_MSG: &str = "io_uring read error fallback";
const IO_URING_WRITE_ERROR_FALLBACK_MSG: &str = "io_uring write error fallback";
const IO_URING_DRIVER_FAILED_MSG: &str = "shared io_uring driver failed";
const IO_URING_MAX_RW_CHUNK_BYTES: usize = 64 * 1024;
#[cfg(feature = "linux-asupersync-uring")]
const IO_URING_ASUPERSYNC_INIT_FAILED_MSG: &str = "asupersync shared io_uring backend init failed";
#[cfg(feature = "linux-asupersync-uring")]
const IO_URING_QUEUE_ENTRIES: u32 = 256;
#[cfg(feature = "linux-asupersync-uring")]
const IO_URING_CANCEL_TAG: u64 = 1_u64 << 63;
#[cfg(feature = "linux-asupersync-uring")]
const IO_URING_DRIVER_WAIT: Duration = Duration::from_millis(1);
#[cfg(all(test, feature = "linux-asupersync-uring"))]
static FORCE_ASUPERSYNC_INIT_FAIL: AtomicBool = AtomicBool::new(false);
#[cfg(all(test, feature = "linux-asupersync-uring"))]
static FORCE_ASUPERSYNC_READ_FAIL: AtomicBool = AtomicBool::new(false);
#[cfg(all(test, feature = "linux-asupersync-uring"))]
static FORCE_ASUPERSYNC_READ_ABORT: AtomicBool = AtomicBool::new(false);
#[cfg(all(test, feature = "linux-asupersync-uring"))]
static FORCE_ASUPERSYNC_WRITE_FAIL: AtomicBool = AtomicBool::new(false);
#[cfg(all(test, feature = "linux-asupersync-uring"))]
static FORCE_ASUPERSYNC_WRITE_ABORT: AtomicBool = AtomicBool::new(false);

fn checkpoint_or_abort(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)
}

fn should_fallback_to_unix_on_uring_error(err: &FrankenError) -> bool {
    match err {
        FrankenError::Abort => false,
        FrankenError::Io(io_err) if io_err.kind() == io::ErrorKind::InvalidInput => false,
        _ => true,
    }
}

fn should_disable_runtime_on_uring_fallback(err: &FrankenError) -> bool {
    match err {
        FrankenError::Abort => false,
        FrankenError::Io(io_err)
            if matches!(
                io_err.kind(),
                io::ErrorKind::Unsupported | io::ErrorKind::InvalidInput
            ) =>
        {
            false
        }
        _ => true,
    }
}

fn duration_to_micros_saturated(duration: std::time::Duration) -> u64 {
    #[allow(clippy::cast_possible_truncation)] // clamped to u64::MAX first
    {
        duration.as_micros().min(u128::from(u64::MAX)) as u64
    }
}

fn next_chunk_end(total: usize, len: usize) -> usize {
    let remaining = len - total;
    total + remaining.min(IO_URING_MAX_RW_CHUNK_BYTES)
}

fn enforce_conformal_breach_policy(
    runtime: &IoUringRuntime,
    operation: &'static str,
    observed: Duration,
    conformal_upper_bound_us: u64,
    disable_reason: &'static str,
) {
    runtime.disable(disable_reason);
    info!(
        operation,
        observed_latency_us = duration_to_micros_saturated(observed),
        conformal_upper_bound_us,
        "io_uring latency exceeded conformal upper bound; backend disabled and unix path will be used"
    );
}

#[cfg(feature = "linux-asupersync-uring")]
#[derive(Debug)]
enum DriverCompletion {
    Read { data: Vec<u8>, bytes_read: usize },
    Write { bytes_written: usize },
    Cancelled,
    Failed(io::Error),
}

#[cfg(feature = "linux-asupersync-uring")]
#[derive(Debug)]
enum DriverRequestKind {
    Read(Vec<u8>),
    Write(Vec<u8>),
}

#[cfg(feature = "linux-asupersync-uring")]
impl DriverRequestKind {
    const fn operation(&self) -> &'static str {
        match self {
            Self::Read(_) => "read",
            Self::Write(_) => "write",
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Read(data) | Self::Write(data) => data.len(),
        }
    }
}

#[cfg(feature = "linux-asupersync-uring")]
#[derive(Debug)]
struct DriverRequest {
    id: u64,
    file: Arc<File>,
    offset: u64,
    kind: DriverRequestKind,
    completion: SendPermit<DriverCompletion>,
    write_completion: Option<VfsWriteCompletionSource>,
}

#[cfg(feature = "linux-asupersync-uring")]
impl DriverRequest {
    fn complete(mut self, result: i32) {
        let operation = self.kind.operation();
        let completion_event = match &self.kind {
            DriverRequestKind::Read(_) => "read_at_complete",
            DriverRequestKind::Write(_) => "write_at_complete",
        };
        if let Some(write_completion) = &mut self.write_completion {
            let write_succeeded = result >= 0
                && matches!(
                    &self.kind,
                    DriverRequestKind::Write(data)
                        if usize::try_from(result).ok() == Some(data.len())
                );
            if write_succeeded {
                write_completion.complete_success();
            } else {
                write_completion.complete_error();
            }
        }
        let completion = if result == -libc::ECANCELED {
            DriverCompletion::Cancelled
        } else if result < 0 {
            DriverCompletion::Failed(io::Error::from_raw_os_error(-result))
        } else {
            let transferred = usize::try_from(result).expect("nonnegative i32 must fit usize");
            match self.kind {
                DriverRequestKind::Read(data) if transferred <= data.len() => {
                    DriverCompletion::Read {
                        data,
                        bytes_read: transferred,
                    }
                }
                DriverRequestKind::Write(data) if transferred <= data.len() => {
                    DriverCompletion::Write {
                        bytes_written: transferred,
                    }
                }
                DriverRequestKind::Read(_) | DriverRequestKind::Write(_) => {
                    DriverCompletion::Failed(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "io_uring completion exceeded submitted buffer length",
                    ))
                }
            }
        };
        trace!(
            event = completion_event,
            request_id = self.id,
            operation,
            result,
            "io_uring request completed"
        );
        let _ = self.completion.send(completion);
    }

    fn fail(mut self, kind: io::ErrorKind, message: &str) {
        if let Some(write_completion) = &mut self.write_completion {
            write_completion.complete_error();
        }
        let _ = self
            .completion
            .send(DriverCompletion::Failed(io::Error::new(
                kind,
                message.to_owned(),
            )));
    }

    fn cancel(mut self) {
        if let Some(write_completion) = &mut self.write_completion {
            write_completion.complete_error();
        }
        let _ = self.completion.send(DriverCompletion::Cancelled);
    }
}

#[cfg(feature = "linux-asupersync-uring")]
fn push_submission(ring: &mut IoUring, entry: &io_uring::squeue::Entry) -> io::Result<()> {
    loop {
        // SAFETY: every data-path entry references a heap allocation owned by
        // the corresponding `DriverRequest` in the driver's `inflight` map.
        // Cancellation never releases that request; only its terminal CQE does.
        if unsafe { ring.submission().push(entry) }.is_ok() {
            return Ok(());
        }
        ring.submit()?;
    }
}

#[cfg(feature = "linux-asupersync-uring")]
#[derive(Debug, Default)]
struct DriverQueue {
    pending: VecDeque<DriverRequest>,
    live: HashSet<u64>,
    cancellations: VecDeque<u64>,
    cancellation_set: HashSet<u64>,
    next_request_id: u64,
    active: bool,
}

#[cfg(feature = "linux-asupersync-uring")]
impl DriverQueue {
    fn allocate_request_id(&mut self) -> u64 {
        loop {
            let candidate = self.next_request_id.max(1);
            self.next_request_id = if candidate == IO_URING_CANCEL_TAG - 1 {
                1
            } else {
                candidate + 1
            };
            if self.live.insert(candidate) {
                return candidate;
            }
        }
    }
}

#[cfg(feature = "linux-asupersync-uring")]
struct RequestCancellationGuard {
    runtime: Arc<IoUringRuntime>,
    request_id: u64,
    armed: bool,
}

#[cfg(feature = "linux-asupersync-uring")]
impl RequestCancellationGuard {
    fn new(runtime: Arc<IoUringRuntime>, request_id: u64) -> Self {
        Self {
            runtime,
            request_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(feature = "linux-asupersync-uring")]
impl Drop for RequestCancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.runtime.cancel_request(self.request_id);
        }
    }
}

struct IoUringRuntime {
    #[cfg(feature = "linux-asupersync-uring")]
    ring: Option<Mutex<IoUring>>,
    #[cfg(feature = "linux-asupersync-uring")]
    queue: Mutex<DriverQueue>,
    #[cfg(feature = "linux-asupersync-uring")]
    driver_starts: AtomicU64,
    #[cfg(feature = "linux-asupersync-uring")]
    submitted_requests: AtomicU64,
    #[cfg(feature = "linux-asupersync-uring")]
    submitted_cancellations: AtomicU64,
    #[cfg(feature = "linux-asupersync-uring")]
    largest_submission_batch: AtomicU64,
    initial_status: String,
    disabled: AtomicBool,
    disable_reason: OnceLock<&'static str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IoUringRuntimeStatus {
    pub backend: &'static str,
    pub available: bool,
    pub disabled: bool,
    pub initial_status: String,
    pub status: String,
    pub disable_reason: Option<&'static str>,
}

impl fmt::Debug for IoUringRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(feature = "linux-asupersync-uring")]
        let backend_available = self.ring.is_some();

        f.debug_struct("IoUringRuntime")
            .field("backend", &Self::backend_name())
            .field("backend_available", &backend_available)
            .field("disabled", &self.disabled.load(Ordering::Relaxed))
            .field("status", &self.status())
            .field("disable_reason", &self.disable_reason())
            .finish_non_exhaustive()
    }
}

impl IoUringRuntime {
    fn new() -> Self {
        #[cfg(feature = "linux-asupersync-uring")]
        {
            #[cfg(test)]
            let forced_failure = FORCE_ASUPERSYNC_INIT_FAIL.load(Ordering::Acquire);
            #[cfg(not(test))]
            let forced_failure = false;

            let ring_result = if forced_failure {
                Err(io::Error::other("forced shared io_uring init failure"))
            } else {
                IoUring::new(IO_URING_QUEUE_ENTRIES)
            };
            let (ring, initial_status) = match ring_result {
                Ok(ring) => (
                    Some(Mutex::new(ring)),
                    "available:asupersync-shared-uring".to_owned(),
                ),
                Err(error) => (None, format!("unavailable:asupersync-shared-uring:{error}")),
            };
            let disable_reason = OnceLock::new();
            if forced_failure {
                let _ = disable_reason.set(IO_URING_ASUPERSYNC_INIT_FAILED_MSG);
            }
            Self {
                ring,
                queue: Mutex::new(DriverQueue::default()),
                driver_starts: AtomicU64::new(0),
                submitted_requests: AtomicU64::new(0),
                submitted_cancellations: AtomicU64::new(0),
                largest_submission_batch: AtomicU64::new(0),
                initial_status,
                disabled: AtomicBool::new(forced_failure),
                disable_reason,
            }
        }
    }

    const fn backend_name() -> &'static str {
        #[cfg(feature = "linux-asupersync-uring")]
        {
            "asupersync-shared-uring"
        }
    }

    fn disable(&self, reason: &'static str) {
        if !self.disabled.swap(true, Ordering::AcqRel) {
            let _ = self.disable_reason.set(reason);
            if matches!(
                reason,
                IO_URING_READ_CONFORMAL_BREACH_MSG | IO_URING_WRITE_CONFORMAL_BREACH_MSG
            ) {
                info!(
                    backend = Self::backend_name(),
                    reason, "io_uring backend disabled; falling back to unix path"
                );
            } else {
                warn!(
                    backend = Self::backend_name(),
                    reason, "io_uring backend disabled; falling back to unix path"
                );
            }
        }
    }

    fn disable_reason(&self) -> Option<&'static str> {
        self.disable_reason.get().copied()
    }

    fn status(&self) -> String {
        match self.disable_reason() {
            Some(reason) => format!("disabled:{}:{reason}", Self::backend_name()),
            None => self.initial_status.clone(),
        }
    }

    fn snapshot(&self) -> IoUringRuntimeStatus {
        IoUringRuntimeStatus {
            backend: Self::backend_name(),
            available: self.is_available(),
            disabled: self.disabled.load(Ordering::Acquire),
            initial_status: self.initial_status.clone(),
            status: self.status(),
            disable_reason: self.disable_reason(),
        }
    }

    #[cfg(test)]
    fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::Acquire)
    }

    fn is_available(&self) -> bool {
        #[cfg(feature = "linux-asupersync-uring")]
        {
            self.ring.is_some() && !self.disabled.load(Ordering::Acquire)
        }
    }

    #[cfg(feature = "linux-asupersync-uring")]
    fn enqueue_read(
        self: &Arc<Self>,
        cx: &Cx,
        native_cx: &NativeCx,
        file: Arc<File>,
        len: usize,
        offset: u64,
    ) -> Result<(u64, Receiver<DriverCompletion>)> {
        self.enqueue(
            cx,
            native_cx,
            file,
            offset,
            DriverRequestKind::Read(vec![0_u8; len]),
            None,
        )
    }

    #[cfg(feature = "linux-asupersync-uring")]
    fn enqueue_write(
        self: &Arc<Self>,
        cx: &Cx,
        native_cx: &NativeCx,
        file: Arc<File>,
        data: Vec<u8>,
        offset: u64,
        write_completion: Option<VfsWriteCompletion>,
    ) -> Result<(u64, Receiver<DriverCompletion>)> {
        self.enqueue(
            cx,
            native_cx,
            file,
            offset,
            DriverRequestKind::Write(data),
            write_completion,
        )
    }

    #[cfg(feature = "linux-asupersync-uring")]
    fn enqueue(
        self: &Arc<Self>,
        cx: &Cx,
        native_cx: &NativeCx,
        file: Arc<File>,
        offset: u64,
        kind: DriverRequestKind,
        write_completion: Option<VfsWriteCompletion>,
    ) -> Result<(u64, Receiver<DriverCompletion>)> {
        checkpoint_or_abort(cx)?;
        if !self.is_available() {
            return Err(FrankenError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "shared io_uring runtime is unavailable",
            )));
        }
        let operation = kind.operation();
        let event = match &kind {
            DriverRequestKind::Read(_) => "read_at_start",
            DriverRequestKind::Write(_) => "write_at_start",
        };
        let len = kind.len();
        let (sender, receiver) = oneshot::channel();
        let completion = sender.reserve(native_cx).map_err(|_| FrankenError::Abort)?;
        let id = {
            let mut queue = self
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let id = queue.allocate_request_id();
            queue.pending.push_back(DriverRequest {
                id,
                file,
                offset,
                kind,
                completion,
                write_completion: write_completion.map(VfsWriteCompletionSource::new),
            });
            id
        };
        trace!(
            event,
            request_id = id,
            operation,
            offset,
            len,
            "io_uring request enqueued"
        );
        Ok((id, receiver))
    }

    #[cfg(feature = "linux-asupersync-uring")]
    fn ensure_driver(self: &Arc<Self>, native_cx: &NativeCx) -> io::Result<()> {
        let should_start = {
            let mut queue = self
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if queue.active || queue.pending.is_empty() {
                false
            } else {
                queue.active = true;
                true
            }
        };
        if !should_start {
            return Ok(());
        }

        self.driver_starts.fetch_add(1, Ordering::Relaxed);
        let runtime = Arc::clone(self);
        match native_cx.spawn_blocking(move |_driver_cx| runtime.drive_to_quiescence()) {
            Ok(handle) => {
                // The task is owned by the request's region, not this observer.
                // Dropping the handle does not detach or cancel the task.
                drop(handle);
                Ok(())
            }
            Err(error) => {
                let mut queue = self
                    .queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                queue.active = false;
                Err(io::Error::other(format!(
                    "cannot start shared io_uring driver: {error}"
                )))
            }
        }
    }

    #[cfg(feature = "linux-asupersync-uring")]
    fn cancel_request(&self, request_id: u64) {
        let queued_request = {
            let mut queue = self
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(index) = queue
                .pending
                .iter()
                .position(|request| request.id == request_id)
            {
                let request = queue
                    .pending
                    .remove(index)
                    .expect("located pending request must remain present");
                queue.live.remove(&request_id);
                Some(request)
            } else if queue.live.contains(&request_id) && queue.cancellation_set.insert(request_id)
            {
                queue.cancellations.push_back(request_id);
                None
            } else {
                None
            }
        };

        trace!(
            event = "io_uring_cancel_requested",
            request_id, "io_uring request cancellation requested"
        );
        if let Some(request) = queued_request {
            request.cancel();
        }
    }

    #[cfg(feature = "linux-asupersync-uring")]
    fn drive_to_quiescence(self: Arc<Self>) {
        let Some(ring_mutex) = &self.ring else {
            self.fail_driver(
                &mut HashMap::new(),
                &io::Error::new(io::ErrorKind::Unsupported, "io_uring is unavailable"),
            );
            return;
        };
        let mut ring = ring_mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut inflight = HashMap::<u64, DriverRequest>::new();

        loop {
            let (requests, cancellations) = {
                let mut queue = self
                    .queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let request_limit = usize::try_from(IO_URING_QUEUE_ENTRIES / 2)
                    .expect("io_uring queue size must fit usize");
                let requests = (0..request_limit)
                    .filter_map(|_| queue.pending.pop_front())
                    .collect::<Vec<_>>();
                let cancellations = (0..request_limit)
                    .filter_map(|_| queue.cancellations.pop_front())
                    .collect::<Vec<_>>();
                (requests, cancellations)
            };

            self.largest_submission_batch.fetch_max(
                u64::try_from(requests.len()).expect("batch length must fit u64"),
                Ordering::Relaxed,
            );

            let mut submission_error = None;
            for request in requests {
                let id = request.id;
                let previous = inflight.insert(id, request);
                assert!(previous.is_none(), "request must only enter the ring once");
                let request = inflight
                    .get_mut(&id)
                    .expect("newly inserted request must remain present");
                let len = u32::try_from(request.kind.len())
                    .expect("VFS chunks are bounded below u32::MAX");
                let entry = match &mut request.kind {
                    DriverRequestKind::Read(data) => opcode::Read::new(
                        types::Fd(request.file.as_raw_fd()),
                        data.as_mut_ptr(),
                        len,
                    )
                    .offset(request.offset)
                    .build()
                    .user_data(id),
                    DriverRequestKind::Write(data) => {
                        opcode::Write::new(types::Fd(request.file.as_raw_fd()), data.as_ptr(), len)
                            .offset(request.offset)
                            .build()
                            .user_data(id)
                    }
                };
                if let Err(error) = push_submission(&mut ring, &entry) {
                    submission_error = Some(error);
                    break;
                }
                self.submitted_requests.fetch_add(1, Ordering::Relaxed);
            }

            if submission_error.is_none() {
                for request_id in cancellations {
                    if !inflight.contains_key(&request_id) {
                        continue;
                    }
                    let entry = opcode::AsyncCancel::new(request_id)
                        .build()
                        .user_data(IO_URING_CANCEL_TAG | request_id);
                    if let Err(error) = push_submission(&mut ring, &entry) {
                        submission_error = Some(error);
                        break;
                    }
                    self.submitted_cancellations.fetch_add(1, Ordering::Relaxed);
                }
            }

            if let Some(error) = submission_error {
                drop(ring);
                self.fail_driver(&mut inflight, &error);
                return;
            }

            if let Err(error) = ring.submit() {
                drop(ring);
                self.fail_driver(&mut inflight, &error);
                return;
            }

            let completed = ring
                .completion()
                .map(|entry| (entry.user_data(), entry.result()))
                .collect::<Vec<_>>();
            self.finish_completions(&mut inflight, completed);

            if inflight.is_empty() {
                let should_stop = {
                    let mut queue = self
                        .queue
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if queue.pending.is_empty() && queue.cancellations.is_empty() {
                        queue.active = false;
                        true
                    } else {
                        false
                    }
                };
                if should_stop {
                    return;
                }
                continue;
            }

            let timeout = types::Timespec::from(IO_URING_DRIVER_WAIT);
            let args = types::SubmitArgs::new().timespec(&timeout);
            match ring.submitter().submit_with_args(1, &args) {
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::ETIME | libc::EINTR | libc::EAGAIN)
                    ) => {}
                Err(error) => {
                    drop(ring);
                    self.fail_driver(&mut inflight, &error);
                    return;
                }
            }
            let completed = ring
                .completion()
                .map(|entry| (entry.user_data(), entry.result()))
                .collect::<Vec<_>>();
            self.finish_completions(&mut inflight, completed);
        }
    }

    #[cfg(feature = "linux-asupersync-uring")]
    fn finish_completions(
        &self,
        inflight: &mut HashMap<u64, DriverRequest>,
        completed: Vec<(u64, i32)>,
    ) {
        for (user_data, result) in completed {
            if user_data & IO_URING_CANCEL_TAG != 0 {
                continue;
            }
            let Some(request) = inflight.remove(&user_data) else {
                continue;
            };
            {
                let mut queue = self
                    .queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                queue.live.remove(&user_data);
                queue.cancellation_set.remove(&user_data);
            }
            request.complete(result);
        }
    }

    #[cfg(feature = "linux-asupersync-uring")]
    fn fail_driver(&self, inflight: &mut HashMap<u64, DriverRequest>, error: &io::Error) {
        self.disable(IO_URING_DRIVER_FAILED_MSG);
        let kind = error.kind();
        let message = error.to_string();
        let queued = {
            let mut queue = self
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            queue.active = false;
            queue.live.clear();
            queue.cancellations.clear();
            queue.cancellation_set.clear();
            queue.pending.drain(..).collect::<Vec<_>>()
        };
        for (_, request) in inflight.drain() {
            request.fail(kind, &message);
        }
        for request in queued {
            request.fail(kind, &message);
        }
    }
}

/// Linux VFS that prefers `io_uring` for the data path.
#[derive(Debug)]
pub struct IoUringVfs {
    unix: UnixVfs,
    runtime: Arc<IoUringRuntime>,
}

impl IoUringVfs {
    /// Create a new `io_uring` VFS.
    #[must_use]
    pub fn new() -> Self {
        Self {
            unix: UnixVfs::new(),
            runtime: Arc::new(IoUringRuntime::new()),
        }
    }

    /// Returns whether `io_uring` was successfully initialized.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.runtime.is_available()
    }

    /// Human-readable runtime status.
    #[must_use]
    pub fn status(&self) -> String {
        self.runtime.status()
    }

    /// Runtime status snapshot including disable reason.
    #[must_use]
    pub fn status_snapshot(&self) -> IoUringRuntimeStatus {
        self.runtime.snapshot()
    }

    fn wrap_unix_file(
        &self,
        file: UnixFile,
        out_flags: VfsOpenFlags,
    ) -> (IoUringFile, VfsOpenFlags) {
        (
            IoUringFile {
                inner: file,
                runtime: Arc::clone(&self.runtime),
            },
            out_flags,
        )
    }
}

impl Default for IoUringVfs {
    fn default() -> Self {
        Self::new()
    }
}

/// File handle for [`IoUringVfs`].
#[derive(Debug)]
pub struct IoUringFile {
    inner: UnixFile,
    runtime: Arc<IoUringRuntime>,
}

impl Vfs for IoUringVfs {
    type File = IoUringFile;

    fn name(&self) -> &'static str {
        "io_uring"
    }

    fn open(
        &self,
        cx: &Cx,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (file, out_flags) = self.unix.open(cx, path, flags)?;
        Ok(self.wrap_unix_file(file, out_flags))
    }

    fn open_with_expected_identity(
        &self,
        cx: &Cx,
        path: &Path,
        flags: VfsOpenFlags,
        expected_identity: FileIdentity,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (file, out_flags) =
            self.unix
                .open_with_expected_identity(cx, path, flags, expected_identity)?;
        Ok(self.wrap_unix_file(file, out_flags))
    }

    fn open_reserved_with_expected_identity(
        &self,
        cx: &Cx,
        path: &Path,
        flags: VfsOpenFlags,
        expected_identity: FileIdentity,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (file, out_flags) =
            self.unix
                .open_reserved_with_expected_identity(cx, path, flags, expected_identity)?;
        Ok(self.wrap_unix_file(file, out_flags))
    }

    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()> {
        self.unix.delete(cx, path, sync_dir)
    }

    fn sync_parent_directory(&self, cx: &Cx, path: &Path) -> Result<()> {
        self.unix.sync_parent_directory(cx, path)
    }

    fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool> {
        self.unix.access(cx, path, flags)
    }

    fn path_entry_exists(&self, cx: &Cx, path: &Path) -> Result<bool> {
        self.unix.path_entry_exists(cx, path)
    }

    fn full_pathname(&self, cx: &Cx, path: &Path) -> Result<PathBuf> {
        self.unix.full_pathname(cx, path)
    }

    fn randomness(&self, cx: &Cx, buf: &mut [u8]) {
        self.unix.randomness(cx, buf);
    }

    fn current_time(&self, cx: &Cx) -> f64 {
        self.unix.current_time(cx)
    }

    fn is_memory(&self) -> bool {
        self.unix.is_memory()
    }
}

impl VfsFile for IoUringFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.inner.close(cx)
    }

    fn file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.file_identity()
    }

    fn read<'a>(
        &'a self,
        cx: &'a Cx,
        buf: &'a mut [u8],
        offset: u64,
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a {
        self.read_data_path(cx, buf, offset)
    }

    fn write<'a>(
        &'a self,
        cx: &'a Cx,
        buf: &'a [u8],
        offset: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        self.write_data_path(cx, buf, offset)
    }

    fn write_tracked<'a>(
        &'a self,
        cx: &'a Cx,
        buf: &'a [u8],
        offset: u64,
        completion: VfsWriteCompletion,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        self.write_data_path_tracked(cx, buf, offset, completion)
    }

    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.inner.truncate(cx, size)
    }

    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.inner.sync(cx, flags)
    }

    fn durable_sync(&mut self, cx: &Cx, kind: crate::SyncKind) -> Result<()> {
        self.inner.durable_sync(cx, kind)
    }

    fn file_size(&self, cx: &Cx) -> Result<u64> {
        self.inner.file_size(cx)
    }

    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.lock(cx, level)
    }

    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.unlock(cx, level)
    }

    fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_shared_snapshot(cx)
    }

    fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_shared_snapshot_attempt(cx)
    }

    fn lock_external_maintenance(&mut self, cx: &Cx, wal_mode: bool) -> Result<()> {
        self.inner.lock_external_maintenance(cx, wal_mode)
    }

    fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_maintenance_attempt(cx)
    }

    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
        self.inner.check_reserved_lock(cx)
    }

    fn sector_size(&self) -> u32 {
        self.inner.sector_size()
    }

    fn device_characteristics(&self) -> u32 {
        self.inner.device_characteristics()
    }

    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
        self.inner.shm_map(cx, region, size, extend)
    }

    // bd-trfah/bd-bjm5d: forward the batch write to the wrapped UnixFile.
    // Without this, the trait default loops `self.write`, which falls back
    // per page through the uring gate — one blocking-pool hop per page —
    // and the UnixFile single-hop batch override (4.9x on group-16 batches)
    // is unreachable on Linux, where IoUringFile wraps every file-backed
    // database. The uring data path has no batch submission today; when it
    // grows one, this forward becomes the fallback arm.
    fn write_page_batch<'a>(
        &'a self,
        cx: &'a Cx,
        writes: &'a [(u64, &'a [u8])],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        self.inner.write_page_batch(cx, writes)
    }

    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        self.inner.shm_lock(cx, offset, n, flags)
    }

    fn shm_barrier(&self) {
        self.inner.shm_barrier();
    }

    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
        self.inner.shm_unmap(cx, delete)
    }

    fn set_busy_timeout_ms(&mut self, ms: u64) {
        self.inner.set_busy_timeout_ms(ms);
    }
}

#[cfg(feature = "linux-asupersync-uring")]
impl IoUringFile {
    async fn read_data_path(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        checkpoint_or_abort(cx)?;
        if buf.is_empty() {
            return Ok(0);
        }
        if !self.runtime.is_available() {
            record_io_uring_read_unix_fallback();
            return self.inner.read(cx, buf, offset).await;
        }
        // DO NOT "fix" this by falling back to `NativeCx::current()`.
        //
        // That looks correct — `page_cache.rs:3421` resolves its native context as
        // `Cx::current().or_else(|| cx.attached_native_cx())`, and this gate fails on
        // every production call because nothing on the `Connection::open` -> `execute`
        // path attaches a native `Cx` (measured: 100% unix fallback, bd-fo6xw). It was
        // tried on 2026-07-26 and it breaks the engine:
        //
        //     cannot start tracked shared io_uring write: cannot start shared io_uring
        //     driver: [ASUP-E001] runtime is no longer available — the runtime behind
        //     this handle was dropped or shut down
        //
        // followed by `no such table` on every subsequent statement. The reason is that
        // the ambient `Cx` inside a short-lived `block_on` belongs to a runtime that is
        // torn down when that call returns, and the *shared* io_uring driver needs a
        // spawner that outlives the operation. `page_cache.rs` gets away with it because
        // it spawns a task on that same runtime and joins it before returning.
        //
        // The attached context is therefore a deliberate contract, not an oversight: it
        // is how a caller states "here is a runtime whose lifetime I guarantee". The
        // real defect is upstream — the sync bridge gives every operation its own
        // runtime, so no such guarantee exists. See bd-fo6xw and bd-zavyn.
        let Some(native_cx) = cx.attached_native_cx() else {
            record_io_uring_read_unix_fallback();
            return self.inner.read(cx, buf, offset).await;
        };
        let file = self.inner.canonical_file()?;

        let start = Instant::now();
        let mut total = 0_usize;
        while total < buf.len() {
            checkpoint_or_abort(cx)?;
            let chunk_end = next_chunk_end(total, buf.len());
            let off = offset
                .checked_add(u64::try_from(total).expect("usize must fit into u64"))
                .ok_or_else(|| {
                    FrankenError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "offset overflow during async io_uring read",
                    ))
                })?;

            #[cfg(test)]
            if FORCE_ASUPERSYNC_READ_ABORT.load(Ordering::Acquire) {
                return Err(FrankenError::Abort);
            }

            #[cfg(test)]
            if FORCE_ASUPERSYNC_READ_FAIL.load(Ordering::Acquire) {
                self.runtime.disable(IO_URING_READ_ERROR_FALLBACK_MSG);
                record_io_uring_read_unix_fallback();
                return self.inner.read(cx, buf, offset).await;
            }

            let chunk_len = chunk_end - total;
            let (request_id, mut receiver) =
                self.runtime
                    .enqueue_read(cx, &native_cx, Arc::clone(&file), chunk_len, off)?;
            let mut cancel_guard =
                RequestCancellationGuard::new(Arc::clone(&self.runtime), request_id);
            asupersync::runtime::yield_now().await;
            if let Err(error) = self.runtime.ensure_driver(&native_cx) {
                drop(cancel_guard);
                warn!(
                    error = %error,
                    "shared io_uring driver unavailable for this context; using Unix async I/O"
                );
                record_io_uring_read_unix_fallback();
                return self.inner.read(cx, buf, offset).await;
            }

            let completion = receiver
                .recv(&native_cx)
                .await
                .map_err(|error| match error {
                    oneshot::RecvError::Cancelled => FrankenError::Abort,
                    oneshot::RecvError::Closed | oneshot::RecvError::PolledAfterCompletion => {
                        FrankenError::Io(io::Error::other(format!(
                            "shared io_uring response channel failed: {error}"
                        )))
                    }
                })?;
            cancel_guard.disarm();
            let bytes_read = match completion {
                DriverCompletion::Read { data, bytes_read } => {
                    buf[total..chunk_end].copy_from_slice(&data);
                    bytes_read
                }
                DriverCompletion::Cancelled => return Err(FrankenError::Abort),
                DriverCompletion::Failed(error) => {
                    let error = FrankenError::Io(error);
                    if !should_fallback_to_unix_on_uring_error(&error) {
                        return Err(error);
                    }
                    if should_disable_runtime_on_uring_fallback(&error) {
                        self.runtime.disable(IO_URING_READ_ERROR_FALLBACK_MSG);
                    }
                    record_io_uring_read_unix_fallback();
                    return self.inner.read(cx, buf, offset).await;
                }
                DriverCompletion::Write { .. } => {
                    return Err(FrankenError::Io(io::Error::other(
                        "shared io_uring returned a write completion for a read request",
                    )));
                }
            };
            checkpoint_or_abort(cx)?;

            if bytes_read == 0 {
                break;
            }
            total += bytes_read;
        }

        if total < buf.len() {
            buf[total..].fill(0);
        }

        let elapsed = start.elapsed();
        if record_io_uring_read_latency(elapsed) {
            let snapshot = io_uring_latency_snapshot();
            enforce_conformal_breach_policy(
                &self.runtime,
                "read",
                elapsed,
                snapshot.read_conformal_upper_bound_us,
                IO_URING_READ_CONFORMAL_BREACH_MSG,
            );
        }
        Ok(total)
    }

    async fn write_data_path(&self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        checkpoint_or_abort(cx)?;
        if buf.is_empty() {
            return Ok(());
        }
        if !self.runtime.is_available() {
            record_io_uring_write_unix_fallback();
            return self.inner.write(cx, buf, offset).await;
        }
        // See `read_data_path`: do not substitute `NativeCx::current()` here.
        let Some(native_cx) = cx.attached_native_cx() else {
            record_io_uring_write_unix_fallback();
            return self.inner.write(cx, buf, offset).await;
        };
        let file = self.inner.canonical_file()?;

        let start = Instant::now();
        let mut total = 0_usize;
        while total < buf.len() {
            checkpoint_or_abort(cx)?;
            let chunk_end = next_chunk_end(total, buf.len());
            let off = offset
                .checked_add(u64::try_from(total).expect("usize must fit into u64"))
                .ok_or_else(|| {
                    FrankenError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "offset overflow during async io_uring write",
                    ))
                })?;

            #[cfg(test)]
            if FORCE_ASUPERSYNC_WRITE_ABORT.load(Ordering::Acquire) {
                return Err(FrankenError::Abort);
            }

            #[cfg(test)]
            if FORCE_ASUPERSYNC_WRITE_FAIL.load(Ordering::Acquire) {
                self.runtime.disable(IO_URING_WRITE_ERROR_FALLBACK_MSG);
                record_io_uring_write_unix_fallback();
                return self.inner.write(cx, buf, offset).await;
            }

            let (request_id, mut receiver) = self.runtime.enqueue_write(
                cx,
                &native_cx,
                Arc::clone(&file),
                buf[total..chunk_end].to_vec(),
                off,
                None,
            )?;
            let mut cancel_guard =
                RequestCancellationGuard::new(Arc::clone(&self.runtime), request_id);
            asupersync::runtime::yield_now().await;
            if let Err(error) = self.runtime.ensure_driver(&native_cx) {
                drop(cancel_guard);
                warn!(
                    error = %error,
                    "shared io_uring driver unavailable for this context; using Unix async I/O"
                );
                record_io_uring_write_unix_fallback();
                return self.inner.write(cx, buf, offset).await;
            }

            let completion = receiver
                .recv(&native_cx)
                .await
                .map_err(|error| match error {
                    oneshot::RecvError::Cancelled => FrankenError::Abort,
                    oneshot::RecvError::Closed | oneshot::RecvError::PolledAfterCompletion => {
                        FrankenError::Io(io::Error::other(format!(
                            "shared io_uring response channel failed: {error}"
                        )))
                    }
                })?;
            cancel_guard.disarm();
            let advanced = match completion {
                DriverCompletion::Write { bytes_written } => bytes_written,
                DriverCompletion::Cancelled => return Err(FrankenError::Abort),
                DriverCompletion::Failed(error) => {
                    let error = FrankenError::Io(error);
                    if !should_fallback_to_unix_on_uring_error(&error) {
                        return Err(error);
                    }
                    if should_disable_runtime_on_uring_fallback(&error) {
                        self.runtime.disable(IO_URING_WRITE_ERROR_FALLBACK_MSG);
                    }
                    record_io_uring_write_unix_fallback();
                    return self.inner.write(cx, buf, offset).await;
                }
                DriverCompletion::Read { .. } => {
                    return Err(FrankenError::Io(io::Error::other(
                        "shared io_uring returned a read completion for a write request",
                    )));
                }
            };
            checkpoint_or_abort(cx)?;

            if advanced == 0 {
                return Err(FrankenError::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "async io_uring write advanced by 0 bytes",
                )));
            }
            let remaining = chunk_end - total;
            total += advanced.min(remaining);
        }

        let elapsed = start.elapsed();
        if record_io_uring_write_latency(elapsed) {
            let snapshot = io_uring_latency_snapshot();
            enforce_conformal_breach_policy(
                &self.runtime,
                "write",
                elapsed,
                snapshot.write_conformal_upper_bound_us,
                IO_URING_WRITE_CONFORMAL_BREACH_MSG,
            );
        }
        Ok(())
    }

    async fn write_data_path_tracked(
        &self,
        cx: &Cx,
        buf: &[u8],
        offset: u64,
        completion: VfsWriteCompletion,
    ) -> Result<()> {
        if let Err(error) = checkpoint_or_abort(cx) {
            completion.complete_error();
            return Err(error);
        }
        if buf.is_empty() {
            completion.complete_success();
            return Ok(());
        }
        if !self.runtime.is_available() {
            record_io_uring_write_unix_fallback();
            return self.inner.write_tracked(cx, buf, offset, completion).await;
        }
        // See `read_data_path`: do not substitute `NativeCx::current()` here.
        let Some(native_cx) = cx.attached_native_cx() else {
            record_io_uring_write_unix_fallback();
            return self.inner.write_tracked(cx, buf, offset, completion).await;
        };
        if u32::try_from(buf.len()).is_err() {
            record_io_uring_write_unix_fallback();
            return self.inner.write_tracked(cx, buf, offset, completion).await;
        }
        let write_range_is_valid = u64::try_from(buf.len())
            .ok()
            .and_then(|len| offset.checked_add(len))
            .is_some();
        if !write_range_is_valid {
            completion.complete_error();
            return Err(FrankenError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset overflow during tracked async io_uring write",
            )));
        }
        let file = match self.inner.canonical_file() {
            Ok(file) => file,
            Err(error) => {
                completion.complete_error();
                return Err(error);
            }
        };

        #[cfg(test)]
        if FORCE_ASUPERSYNC_WRITE_ABORT.load(Ordering::Acquire) {
            completion.complete_error();
            return Err(FrankenError::Abort);
        }

        #[cfg(test)]
        if FORCE_ASUPERSYNC_WRITE_FAIL.load(Ordering::Acquire) {
            self.runtime.disable(IO_URING_WRITE_ERROR_FALLBACK_MSG);
            record_io_uring_write_unix_fallback();
            return self.inner.write_tracked(cx, buf, offset, completion).await;
        }

        let start = Instant::now();
        let enqueue_result = self.runtime.enqueue_write(
            cx,
            &native_cx,
            file,
            buf.to_vec(),
            offset,
            Some(completion.clone()),
        );
        let (request_id, mut receiver) = match enqueue_result {
            Ok(request) => request,
            Err(error) => {
                completion.complete_error();
                return Err(error);
            }
        };
        let mut cancel_guard = RequestCancellationGuard::new(Arc::clone(&self.runtime), request_id);
        asupersync::runtime::yield_now().await;
        if let Err(error) = self.runtime.ensure_driver(&native_cx) {
            drop(cancel_guard);
            return Err(FrankenError::Io(io::Error::other(format!(
                "cannot start tracked shared io_uring write: {error}"
            ))));
        }

        let driver_completion = receiver
            .recv(&native_cx)
            .await
            .map_err(|error| match error {
                oneshot::RecvError::Cancelled => FrankenError::Abort,
                oneshot::RecvError::Closed | oneshot::RecvError::PolledAfterCompletion => {
                    FrankenError::Io(io::Error::other(format!(
                        "tracked shared io_uring response channel failed: {error}"
                    )))
                }
            })?;
        cancel_guard.disarm();

        match driver_completion {
            DriverCompletion::Write { bytes_written } if bytes_written == buf.len() => {}
            DriverCompletion::Write { bytes_written } => {
                return Err(FrankenError::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "tracked async io_uring write completed only {bytes_written} of {} bytes",
                        buf.len()
                    ),
                )));
            }
            DriverCompletion::Cancelled => return Err(FrankenError::Abort),
            DriverCompletion::Failed(error) => return Err(FrankenError::Io(error)),
            DriverCompletion::Read { .. } => {
                return Err(FrankenError::Io(io::Error::other(
                    "shared io_uring returned a read completion for a tracked write request",
                )));
            }
        }

        // The CQE driver has already set Success. A cancellation observed here
        // must not erase proof that the bytes reached the completion source.
        checkpoint_or_abort(cx)?;

        let elapsed = start.elapsed();
        if record_io_uring_write_latency(elapsed) {
            let snapshot = io_uring_latency_snapshot();
            enforce_conformal_breach_policy(
                &self.runtime,
                "write",
                elapsed,
                snapshot.write_conformal_upper_bound_us,
                IO_URING_WRITE_CONFORMAL_BREACH_MSG,
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use asupersync::runtime::{Runtime, RuntimeBuilder};
    use fsqlite_observability::{io_uring_latency_snapshot, reset_io_uring_latency_metrics};
    use fsqlite_types::flags::VfsOpenFlags;
    use std::future::Future;
    use std::io::Write;
    use std::sync::{Mutex as StdMutex, MutexGuard as StdMutexGuard};
    use tracing_subscriber::prelude::*;

    static IO_URING_TEST_LOCK: StdMutex<()> = StdMutex::new(());

    const ASYNC_VFS_TRACE_CAPTURE_LIMIT_BYTES: usize = 512 * 1024;
    const ASYNC_VFS_TRACE_TARGET: &str = "fsqlite_vfs::uring";

    #[derive(Clone)]
    struct BoundedTraceWriter {
        bytes: Arc<StdMutex<Vec<u8>>>,
        truncated: Arc<AtomicBool>,
    }

    impl BoundedTraceWriter {
        fn new() -> Self {
            Self {
                bytes: Arc::new(StdMutex::new(Vec::with_capacity(
                    ASYNC_VFS_TRACE_CAPTURE_LIMIT_BYTES,
                ))),
                truncated: Arc::new(AtomicBool::new(false)),
            }
        }

        fn flush_to_stderr(&self) {
            let bytes = self
                .bytes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::io::stderr()
                .write_all(&bytes)
                .expect("the bounded async-VFS trace must be written to stderr");
        }

        fn has_events(&self) -> bool {
            !self
                .bytes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        }

        fn is_truncated(&self) -> bool {
            self.truncated.load(Ordering::Acquire)
        }

        fn event_count(&self, event: &str) -> usize {
            let needle = format!(r#""event":"{event}""#);
            self.bytes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .windows(needle.len())
                .filter(|window| *window == needle.as_bytes())
                .count()
        }
    }

    struct BoundedTraceWriteGuard<'a> {
        bytes: StdMutexGuard<'a, Vec<u8>>,
        truncated: &'a AtomicBool,
    }

    impl Write for BoundedTraceWriteGuard<'_> {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            let remaining = ASYNC_VFS_TRACE_CAPTURE_LIMIT_BYTES.saturating_sub(self.bytes.len());
            self.bytes
                .extend_from_slice(&buffer[..buffer.len().min(remaining)]);
            if buffer.len() > remaining {
                self.truncated.store(true, Ordering::Release);
            }
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BoundedTraceWriter {
        type Writer = BoundedTraceWriteGuard<'a>;

        fn make_writer(&'a self) -> Self::Writer {
            BoundedTraceWriteGuard {
                bytes: self
                    .bytes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                truncated: &self.truncated,
            }
        }
    }

    fn test_runtime() -> &'static Runtime {
        static RUNTIME: OnceLock<Runtime> = OnceLock::new();
        RUNTIME.get_or_init(|| {
            RuntimeBuilder::current_thread()
                .blocking_threads(1, 2)
                .build()
                .expect("VFS test runtime should build")
        })
    }

    fn block_on_test<F: Future>(cx: &Cx, future: F) -> F::Output {
        test_runtime().block_on(async {
            let native_cx = NativeCx::current().expect("runtime block_on should install Cx");
            cx.set_native_cx(native_cx);
            future.await
        })
    }

    fn open_flags_create() -> VfsOpenFlags {
        VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE
    }

    fn open_flags_create_unlocked() -> VfsOpenFlags {
        VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE
    }

    #[cfg(feature = "linux-asupersync-uring")]
    struct ScopedAtomicFlag<'a> {
        flag: &'a AtomicBool,
    }

    #[cfg(feature = "linux-asupersync-uring")]
    impl<'a> ScopedAtomicFlag<'a> {
        fn enable(flag: &'a AtomicBool) -> Self {
            flag.store(true, Ordering::Release);
            Self { flag }
        }
    }

    #[cfg(feature = "linux-asupersync-uring")]
    impl Drop for ScopedAtomicFlag<'_> {
        fn drop(&mut self) {
            self.flag.store(false, Ordering::Release);
        }
    }

    fn io_uring_test_guard() -> StdMutexGuard<'static, ()> {
        IO_URING_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[test]
    fn test_request_id_wrap_skips_live_requests_and_cancel_tag() {
        let mut queue = DriverQueue {
            next_request_id: IO_URING_CANCEL_TAG - 1,
            ..DriverQueue::default()
        };
        queue.live.insert(1);

        assert_eq!(queue.allocate_request_id(), IO_URING_CANCEL_TAG - 1);
        assert_eq!(queue.allocate_request_id(), 2);
        assert!(!queue.live.contains(&IO_URING_CANCEL_TAG));
    }

    #[test]
    fn tracked_write_completion_cqe_survives_observer_drop() {
        use std::future::{Future as _, poll_fn};
        use std::task::Poll;

        let cx = Cx::new();
        let directory = tempfile::tempdir().expect("create io_uring CQE tempdir");
        let path = directory.path().join("tracked-cqe.db");
        let file = Arc::new(File::create(path).expect("create io_uring CQE file"));
        let completion = VfsWriteCompletion::new();
        let source_completion = completion.clone();

        block_on_test(&cx, async {
            let native_cx = NativeCx::current().expect("runtime must install native Cx");
            let (sender, mut receiver) = oneshot::channel();
            let permit = sender
                .reserve(&native_cx)
                .expect("reserve CQE completion permit");
            let mut observer = Box::pin(receiver.recv(&native_cx));
            poll_fn(|task_cx| match observer.as_mut().poll(task_cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("CQE observer cannot complete before the source"),
            })
            .await;
            drop(observer);
            drop(receiver);

            DriverRequest {
                id: 7,
                file,
                offset: 0,
                kind: DriverRequestKind::Write(vec![1, 2, 3, 4]),
                completion: permit,
                write_completion: Some(VfsWriteCompletionSource::new(source_completion)),
            }
            .complete(4);

            assert_eq!(
                completion.wait().await,
                crate::traits::VfsWriteCompletionState::Success
            );
        });
    }

    #[test]
    fn test_io_uring_vfs_name_and_status() {
        let vfs = IoUringVfs::new();
        assert_eq!(vfs.name(), "io_uring");
        assert!(!vfs.status().is_empty());
        assert_eq!(vfs.is_memory(), vfs.unix.is_memory());
    }

    #[test]
    fn test_io_uring_vfs_forwards_expected_identity_open() {
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_identity.db");
        std::fs::write(&path, b"existing database bytes").expect("seed existing file");
        let identity_source = std::fs::File::open(&path).expect("open identity source");
        let expected_identity = FileIdentity::from_file(&identity_source)
            .expect("query identity")
            .expect("Unix files have stable identities");

        let requested_flags = VfsOpenFlags::MAIN_DB
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::EXCLUSIVE;
        let (mut file, out_flags) = vfs
            .open_with_expected_identity(&cx, &path, requested_flags, expected_identity)
            .expect("matching identity should open through io_uring decorator");

        assert_eq!(
            file.file_identity().expect("wrapped identity"),
            Some(expected_identity)
        );
        assert!(!out_flags.intersects(VfsOpenFlags::CREATE | VfsOpenFlags::EXCLUSIVE));
        file.close(&cx).expect("close decorated file");
    }

    #[test]
    fn test_io_uring_vfs_forwards_reserved_identity_open() {
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_reserved.db");
        let identity_source = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create reservation");
        let expected_identity = FileIdentity::from_file(&identity_source)
            .expect("query identity")
            .expect("Unix files have stable identities");
        drop(identity_source);

        let requested_flags = VfsOpenFlags::MAIN_DB
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::EXCLUSIVE;
        let (mut file, out_flags) = vfs
            .open_reserved_with_expected_identity(&cx, &path, requested_flags, expected_identity)
            .expect("matching empty reservation should open through io_uring decorator");

        assert_eq!(
            file.file_identity().expect("wrapped identity"),
            Some(expected_identity)
        );
        assert!(!out_flags.intersects(VfsOpenFlags::CREATE | VfsOpenFlags::EXCLUSIVE));
        file.close(&cx).expect("close decorated file");
    }

    #[test]
    fn test_io_uring_vfs_forwards_no_follow_path_entry_check() {
        use std::os::unix::fs::symlink;

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let dangling = dir.path().join("dangling-wal");
        symlink(dir.path().join("missing-target"), &dangling).expect("create dangling symlink");

        assert!(
            vfs.path_entry_exists(&cx, &dangling)
                .expect("forward no-follow entry check")
        );
    }

    #[test]
    fn test_io_uring_vfs_roundtrip_write_read() {
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_roundtrip.db");

        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed");
        block_on_test(&cx, file.write(&cx, b"hello io_uring", 0)).expect("write should succeed");

        let mut buf = [0_u8; 14];
        let n = block_on_test(&cx, file.read(&cx, &mut buf, 0)).expect("read should succeed");
        assert_eq!(n, 14);
        assert_eq!(&buf, b"hello io_uring");
        file.close(&cx).expect("close should succeed");
    }

    #[test]
    fn test_io_uring_paths_emit_latency_or_fallback_metrics() {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_metrics.db");

        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed");
        block_on_test(&cx, file.write(&cx, b"metrics", 0)).expect("write should succeed");

        let mut buf = [0_u8; 7];
        let _ = block_on_test(&cx, file.read(&cx, &mut buf, 0)).expect("read should succeed");

        let snapshot = io_uring_latency_snapshot();
        if vfs.is_available() {
            assert!(
                snapshot.write_samples_total >= 1 || snapshot.write_unix_fallbacks_total >= 1,
                "write path should either record io_uring latency or fallback"
            );
            assert!(
                snapshot.read_samples_total >= 1 || snapshot.read_unix_fallbacks_total >= 1,
                "read path should either record io_uring latency or fallback"
            );
        }
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_shared_ring_multiplexes_one_hundred_concurrent_reads() {
        const READ_COUNT: usize = 100;
        const READ_SIZE: usize = 4096;

        let _guard = io_uring_test_guard();
        if run_async_vfs_trace_in_subprocess() {
            return;
        }
        let trace_writer = init_async_vfs_test_tracing();
        test_runtime().block_on(async {
            let native_cx = NativeCx::current().expect("runtime block_on should install Cx");
            let cx = Cx::new();
            cx.set_native_cx(native_cx.clone());
            let vfs = IoUringVfs::new();
            assert!(
                vfs.is_available(),
                "strict multiplexing proof requires io_uring: {}",
                vfs.status()
            );

            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("shared_ring_100_reads.db");
            let mut seeded = vec![0_u8; READ_COUNT * READ_SIZE];
            for (page_index, page) in seeded.as_chunks_mut::<READ_SIZE>().0.iter_mut().enumerate() {
                page.fill(u8::try_from(page_index).expect("100 pages fit in u8"));
            }
            std::fs::write(&path, seeded).expect("synchronous seed write should succeed");
            let (file, _) = vfs
                .open(&cx, Some(&path), open_flags_create_unlocked())
                .expect("open should succeed");

            let starts_before = file.runtime.driver_starts.load(Ordering::Relaxed);
            let submitted_before = file.runtime.submitted_requests.load(Ordering::Relaxed);
            let runtime = Arc::clone(&file.runtime);
            let backing_file = file
                .inner
                .canonical_file()
                .expect("opened file must retain its canonical handle");
            let mut requests = Vec::with_capacity(READ_COUNT);
            for page_index in 0..READ_COUNT {
                let offset = u64::try_from(page_index * READ_SIZE).expect("offset fits u64");
                let (request_id, receiver) = runtime
                    .enqueue_read(
                        &cx,
                        &native_cx,
                        Arc::clone(&backing_file),
                        READ_SIZE,
                        offset,
                    )
                    .expect("read request should enqueue");
                requests.push((page_index, request_id, receiver));
            }
            runtime
                .ensure_driver(&native_cx)
                .expect("one driver should start after all reads are queued");
            for (page_index, request_id, mut receiver) in requests {
                let completion = receiver
                    .recv(&native_cx)
                    .await
                    .expect("queued read should complete");
                let DriverCompletion::Read { data, bytes_read } = completion else {
                    panic!("request {request_id} must complete as a read");
                };
                assert_eq!(bytes_read, READ_SIZE);
                assert!(
                    data.iter().all(|byte| *byte
                        == u8::try_from(page_index).expect("100 pages fit in u8"))
                );
            }

            assert_eq!(
                file.runtime.submitted_requests.load(Ordering::Relaxed) - submitted_before,
                u64::try_from(READ_COUNT).expect("read count fits u64"),
                "each page read must produce exactly one SQE"
            );
            assert_eq!(
                file.runtime.driver_starts.load(Ordering::Relaxed) - starts_before,
                1,
                "all concurrent reads must share one driver activation"
            );
            assert!(
                file.runtime
                    .largest_submission_batch
                    .load(Ordering::Relaxed)
                    >= u64::try_from(READ_COUNT).expect("read count fits u64"),
                "all one hundred reads must reach one submission queue batch"
            );
        });
        if let Some(trace_writer) = trace_writer {
            assert!(
                trace_writer.has_events(),
                "FSQLITE_ASYNC_VFS_TRACE must retain bounded JSON trace output"
            );
            assert!(
                !trace_writer.is_truncated(),
                "FSQLITE_ASYNC_VFS_TRACE must retain the complete bounded JSON trace"
            );
            assert_eq!(
                trace_writer.event_count("read_at_start"),
                READ_COUNT,
                "FSQLITE_ASYNC_VFS_TRACE must retain every read start"
            );
            assert_eq!(
                trace_writer.event_count("read_at_complete"),
                READ_COUNT,
                "FSQLITE_ASYNC_VFS_TRACE must retain every read completion"
            );
            trace_writer.flush_to_stderr();
        }
    }

    const ASYNC_VFS_TRACE_ENV: &str = "FSQLITE_ASYNC_VFS_TRACE";
    const ASYNC_VFS_TRACE_SUBPROCESS_ENV: &str = "FSQLITE_ASYNC_VFS_TRACE_SUBPROCESS";
    const SHARED_RING_TRACE_TEST: &str =
        "uring::tests::test_shared_ring_multiplexes_one_hundred_concurrent_reads";

    /// Installs the verbose JSON subscriber only in the dedicated trace child.
    ///
    /// The 100-read workload spans runtime threads, so a thread-local
    /// subscriber would omit events. The parent test therefore starts a child
    /// filtered to this one test when tracing is requested; no later test can
    /// observe this process-global subscriber.
    fn init_async_vfs_test_tracing() -> Option<BoundedTraceWriter> {
        if std::env::var_os(ASYNC_VFS_TRACE_ENV).is_none()
            || std::env::var_os(ASYNC_VFS_TRACE_SUBPROCESS_ENV).is_none()
        {
            return None;
        }

        let writer = BoundedTraceWriter::new();
        assert!(
            !tracing::dispatcher::has_been_set(),
            "the dedicated async-VFS trace process must start without a subscriber"
        );
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(writer.clone())
                    .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                        metadata.target() == ASYNC_VFS_TRACE_TARGET
                    })),
            )
            .try_init()
            .expect("the dedicated async-VFS trace process must install its subscriber");
        Some(writer)
    }

    /// Runs trace-enabled async-VFS work in a process that executes no other
    /// libtest test, retaining the opt-in trace contract without polluting the
    /// normal shared test binary.
    fn run_async_vfs_trace_in_subprocess() -> bool {
        if std::env::var_os(ASYNC_VFS_TRACE_ENV).is_none()
            || std::env::var_os(ASYNC_VFS_TRACE_SUBPROCESS_ENV).is_some()
        {
            return false;
        }

        let status = std::process::Command::new(
            std::env::current_exe().expect("the libtest binary path must be available"),
        )
        .arg(SHARED_RING_TRACE_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env(ASYNC_VFS_TRACE_ENV, "1")
        .env(ASYNC_VFS_TRACE_SUBPROCESS_ENV, "1")
        .status()
        .expect("the dedicated async-VFS trace process must start");

        assert!(
            status.success(),
            "the dedicated async-VFS trace process must complete successfully"
        );
        true
    }

    #[test]
    fn async_vfs_trace_opt_in_does_not_install_a_process_global_subscriber() {
        const PROBE_ENV: &str = "FSQLITE_ASYNC_VFS_TRACE_GLOBAL_SUBSCRIBER_PROBE";
        const TEST_FILTER: &str =
            "async_vfs_trace_opt_in_does_not_install_a_process_global_subscriber";

        if std::env::var_os(PROBE_ENV).is_some() {
            assert!(
                !tracing::dispatcher::has_been_set(),
                "the fresh keeper process must start without a tracing subscriber"
            );
            assert!(
                run_async_vfs_trace_in_subprocess(),
                "the trace opt-in must isolate the traced workload in a child process"
            );
            assert!(
                !tracing::dispatcher::has_been_set(),
                "the trace-enabled child must not leak a subscriber to a later parent test"
            );
            return;
        }

        let status = std::process::Command::new(
            std::env::current_exe().expect("the libtest binary path must be available"),
        )
        .arg(TEST_FILTER)
        .arg("--test-threads=1")
        .env(ASYNC_VFS_TRACE_ENV, "1")
        .env(PROBE_ENV, "1")
        .status()
        .expect("the fresh keeper process must start");

        assert!(
            status.success(),
            "the fresh keeper process must confirm the trace opt-in leaves no global subscriber"
        );
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_local_cx_cancellation_submits_async_cancel_within_five_ms() {
        let _guard = io_uring_test_guard();
        let proof = test_runtime().handle().spawn(async {
            let driver_cx = NativeCx::current().expect("runtime task should install Cx");
            let request_native_cx = NativeCx::for_testing();
            let request_cx = Cx::new();
            request_cx.set_native_cx(request_native_cx.clone());
            let runtime = Arc::new(IoUringRuntime::new());
            assert!(
                runtime.is_available(),
                "strict cancellation proof requires io_uring: {}",
                runtime.status()
            );

            let (read_fd, _write_fd) = nix::unistd::pipe().expect("pipe should open");
            let read_file = Arc::new(File::from(read_fd));
            let (request_id, mut receiver) = runtime
                .enqueue_read(&request_cx, &request_native_cx, read_file, 1, 0)
                .expect("pipe read should enqueue");
            let cancel_guard = RequestCancellationGuard::new(Arc::clone(&runtime), request_id);
            runtime
                .ensure_driver(&driver_cx)
                .expect("driver should start");

            let submit_deadline = Instant::now() + Duration::from_secs(1);
            while runtime.submitted_requests.load(Ordering::Acquire) == 0 {
                assert!(
                    Instant::now() < submit_deadline,
                    "pipe read was not submitted before deadline"
                );
                asupersync::runtime::yield_now().await;
            }

            let cancellation_started = Instant::now();
            request_cx.cancel();
            let recv_error = receiver
                .recv(&request_native_cx)
                .await
                .expect_err("cancelled request context must interrupt receive");
            assert_eq!(recv_error, oneshot::RecvError::Cancelled);
            drop(cancel_guard);

            while runtime.submitted_cancellations.load(Ordering::Acquire) == 0 {
                assert!(
                    cancellation_started.elapsed() < Duration::from_millis(5),
                    "IORING_OP_ASYNC_CANCEL was not submitted within 5ms"
                );
                asupersync::runtime::yield_now().await;
            }
            assert!(
                cancellation_started.elapsed() < Duration::from_millis(5),
                "IORING_OP_ASYNC_CANCEL submission exceeded 5ms"
            );

            let completion_deadline = Instant::now() + Duration::from_secs(1);
            loop {
                let is_live = runtime
                    .queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .live
                    .contains(&request_id);
                if !is_live {
                    break;
                }
                assert!(
                    Instant::now() < completion_deadline,
                    "cancelled pipe read did not reach a terminal CQE"
                );
                asupersync::runtime::yield_now().await;
            }
        });
        test_runtime().block_on(proof);
    }

    #[test]
    fn test_disabled_runtime_records_unix_fallback_metrics() {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        vfs.runtime.disable("test disable before io");
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_disabled_runtime_metrics.db");

        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed via unix fallback");
        block_on_test(&cx, file.write(&cx, b"fallback-metrics", 0))
            .expect("write should succeed via unix path");
        let mut buf = [0_u8; 16];
        let _ = block_on_test(&cx, file.read(&cx, &mut buf, 0))
            .expect("read should succeed via unix path");

        let snapshot = io_uring_latency_snapshot();
        assert!(
            snapshot.unix_fallbacks_total >= 2,
            "disabled runtime should record fallback for both write/read ops"
        );
        assert!(
            snapshot.write_unix_fallbacks_total >= 1,
            "disabled runtime should record write fallback"
        );
        assert!(
            snapshot.read_unix_fallbacks_total >= 1,
            "disabled runtime should record read fallback"
        );
    }

    #[test]
    fn test_runtime_disable_is_sticky() {
        let _guard = io_uring_test_guard();
        let runtime = IoUringRuntime::new();
        assert!(!runtime.is_disabled());
        assert_eq!(runtime.disable_reason(), None);
        runtime.disable("test disable");
        assert!(runtime.is_disabled());
        assert_eq!(runtime.disable_reason(), Some("test disable"));
        assert_eq!(
            runtime.status(),
            "disabled:asupersync-shared-uring:test disable"
        );
        runtime.disable("test disable again");
        assert!(runtime.is_disabled());
        assert_eq!(runtime.disable_reason(), Some("test disable"));
    }

    #[test]
    fn test_invalid_input_errors_propagate_without_fallback_or_disable() {
        let err = FrankenError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "offset overflow during io_uring write",
        ));

        assert!(!should_fallback_to_unix_on_uring_error(&err));
        assert!(!should_disable_runtime_on_uring_fallback(&err));
    }

    #[test]
    fn test_conformal_breach_policy_disables_runtime() {
        let _guard = io_uring_test_guard();
        let runtime = IoUringRuntime::new();
        assert!(!runtime.is_disabled());

        enforce_conformal_breach_policy(
            &runtime,
            "read",
            Duration::from_micros(250),
            100,
            IO_URING_READ_CONFORMAL_BREACH_MSG,
        );

        assert!(runtime.is_disabled());
        assert_eq!(
            runtime.disable_reason(),
            Some(IO_URING_READ_CONFORMAL_BREACH_MSG)
        );
        assert_eq!(
            runtime.status(),
            format!("disabled:asupersync-shared-uring:{IO_URING_READ_CONFORMAL_BREACH_MSG}")
        );
    }

    #[test]
    fn test_vfs_status_snapshot_reflects_disable_reason() {
        let _guard = io_uring_test_guard();
        let vfs = IoUringVfs::new();
        let initial = vfs.status_snapshot();
        assert_eq!(initial.backend, "asupersync-shared-uring");
        assert_eq!(initial.status, initial.initial_status);
        assert_eq!(initial.disable_reason, None);

        vfs.runtime.disable("manual test disable");

        let disabled = vfs.status_snapshot();
        assert!(disabled.disabled);
        assert!(!disabled.available);
        assert_eq!(disabled.disable_reason, Some("manual test disable"));
        assert_eq!(
            disabled.status,
            "disabled:asupersync-shared-uring:manual test disable"
        );
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_temp_file_fallback_does_not_disable_runtime() {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }

        let flags = VfsOpenFlags::TEMP_DB
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::DELETEONCLOSE;
        let (file, _) = vfs.open(&cx, None, flags).expect("open temp file");

        block_on_test(&cx, file.write(&cx, b"temp data", 0))
            .expect("write should fall back without disabling runtime");
        let mut buf = [0_u8; 9];
        let n = block_on_test(&cx, file.read(&cx, &mut buf, 0))
            .expect("read should fall back without disabling runtime");

        assert_eq!(n, 9);
        assert_eq!(&buf, b"temp data");
        assert!(
            vfs.is_available(),
            "temp-file fallback should not disable io_uring"
        );
        assert!(!vfs.runtime.is_disabled());

        let snapshot = io_uring_latency_snapshot();
        assert!(
            snapshot.unix_fallbacks_total >= 2,
            "temp-file fallback should record unix fallback metrics"
        );
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_main_db_open_retains_canonical_fd_for_io_uring() {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("main_db_direct_unix.db");
        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open should succeed");

        let canonical = file
            .inner
            .canonical_file()
            .expect("main DB must retain canonical descriptor");
        let canonical_again = file
            .inner
            .canonical_file()
            .expect("canonical descriptor must remain available");
        assert!(Arc::ptr_eq(&canonical, &canonical_again));

        block_on_test(&cx, file.write(&cx, b"main-db", 0))
            .expect("write should succeed via unix path");
        let mut buf = [0_u8; 7];
        let n = block_on_test(&cx, file.read(&cx, &mut buf, 0)).expect("read should succeed");
        assert_eq!(n, 7);
        assert_eq!(&buf, b"main-db");
        assert!(
            vfs.is_available(),
            "skipping io_uring fd should not disable runtime"
        );

        let snapshot = io_uring_latency_snapshot();
        assert!(
            snapshot.unix_fallbacks_total >= 2,
            "main-db direct unix path should avoid io_uring and record unix-path ops"
        );
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_wal_open_retains_canonical_fd_for_io_uring() {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("main.db-wal");
        let wal_flags = VfsOpenFlags::WAL | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (file, _) = vfs
            .open(&cx, Some(&path), wal_flags)
            .expect("open should succeed");

        let canonical = file
            .inner
            .canonical_file()
            .expect("WAL must retain canonical descriptor");
        let canonical_again = file
            .inner
            .canonical_file()
            .expect("canonical descriptor must remain available");
        assert!(Arc::ptr_eq(&canonical, &canonical_again));

        block_on_test(&cx, file.write(&cx, b"wal", 0)).expect("write should succeed via unix path");
        let mut buf = [0_u8; 3];
        let n = block_on_test(&cx, file.read(&cx, &mut buf, 0)).expect("read should succeed");
        assert_eq!(n, 3);
        assert_eq!(&buf, b"wal");
        assert!(
            vfs.is_available(),
            "skipping io_uring fd should not disable runtime"
        );

        let snapshot = io_uring_latency_snapshot();
        assert!(
            snapshot.unix_fallbacks_total >= 2,
            "wal direct unix path should avoid io_uring and record unix-path ops"
        );
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_write_abort_propagates_without_disabling_runtime_or_fallback() {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("asupersync_abort_propagation.db");
        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed");

        let _force_abort = ScopedAtomicFlag::enable(&FORCE_ASUPERSYNC_WRITE_ABORT);
        let err = block_on_test(&cx, file.write(&cx, b"abort", 0))
            .expect_err("write should propagate abort");

        assert!(matches!(err, FrankenError::Abort));
        assert!(vfs.is_available(), "abort should not disable io_uring");
        assert!(!vfs.runtime.is_disabled());

        let snapshot = io_uring_latency_snapshot();
        assert_eq!(
            snapshot.unix_fallbacks_total, 0,
            "abort should not fall back to unix or record fallback metrics"
        );
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_read_abort_propagates_without_disabling_runtime_or_fallback() -> Result<()> {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return Ok(());
        }

        let dir = tempfile::tempdir().map_err(FrankenError::Io)?;
        let path = dir.path().join("asupersync_read_abort_propagation.db");
        let (file, _) = vfs.open(&cx, Some(&path), open_flags_create_unlocked())?;
        reset_io_uring_latency_metrics();

        let _force_abort = ScopedAtomicFlag::enable(&FORCE_ASUPERSYNC_READ_ABORT);
        let mut buf = [0_u8; 4];
        let err = match block_on_test(&cx, file.read(&cx, &mut buf, 0)) {
            Ok(bytes) => {
                return Err(FrankenError::Io(io::Error::other(format!(
                    "read should propagate abort, read {bytes} bytes"
                ))));
            }
            Err(err) => err,
        };

        assert!(matches!(err, FrankenError::Abort));
        assert!(vfs.is_available(), "abort should not disable io_uring");
        assert!(!vfs.runtime.is_disabled());

        let snapshot = io_uring_latency_snapshot();
        assert_eq!(
            snapshot.unix_fallbacks_total, 0,
            "abort should not fall back to unix or record fallback metrics"
        );
        Ok(())
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_asupersync_init_failure_disables_backend_and_falls_back() {
        let _guard = io_uring_test_guard();
        let cx = Cx::new();
        let _force_init_fail = ScopedAtomicFlag::enable(&FORCE_ASUPERSYNC_INIT_FAIL);
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("asupersync_forced_init_failure.db");

        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed via unix fallback");

        assert!(vfs.runtime.is_disabled());
        assert!(!vfs.is_available());
        let status = vfs.status_snapshot();
        assert_eq!(
            status.disable_reason,
            Some(IO_URING_ASUPERSYNC_INIT_FAILED_MSG)
        );
        assert_eq!(
            status.status,
            format!("disabled:asupersync-shared-uring:{IO_URING_ASUPERSYNC_INIT_FAILED_MSG}")
        );

        block_on_test(&cx, file.write(&cx, b"fallback", 0))
            .expect("write should succeed via unix fallback");
        let mut buf = [0_u8; 8];
        let n = block_on_test(&cx, file.read(&cx, &mut buf, 0))
            .expect("read should succeed via unix fallback");
        assert_eq!(n, 8);
        assert_eq!(&buf, b"fallback");
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_asupersync_write_error_disables_runtime_and_falls_back() {
        let _guard = io_uring_test_guard();
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("asupersync_forced_write_failure.db");
        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed");

        let _force_write_fail = ScopedAtomicFlag::enable(&FORCE_ASUPERSYNC_WRITE_FAIL);
        block_on_test(&cx, file.write(&cx, b"fallback", 0))
            .expect("write should succeed via unix fallback");

        assert!(vfs.runtime.is_disabled());
        assert!(!vfs.is_available());

        let mut buf = [0_u8; 8];
        let n = block_on_test(&cx, file.read(&cx, &mut buf, 0))
            .expect("read should use unix path after runtime disable");
        assert_eq!(n, 8);
        assert_eq!(&buf, b"fallback");
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_asupersync_read_error_disables_runtime_and_falls_back() {
        let _guard = io_uring_test_guard();
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("asupersync_forced_read_failure.db");
        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed");

        block_on_test(&cx, file.write(&cx, b"fallback", 0)).expect("write should seed data");

        let _force_read_fail = ScopedAtomicFlag::enable(&FORCE_ASUPERSYNC_READ_FAIL);
        let mut buf = [0_u8; 8];
        let n = block_on_test(&cx, file.read(&cx, &mut buf, 0))
            .expect("read should succeed via unix fallback");

        assert_eq!(n, 8);
        assert_eq!(&buf, b"fallback");
        assert!(vfs.runtime.is_disabled());
        assert!(!vfs.is_available());
    }

    #[test]
    fn io_uring_runtime_status_clone_eq_debug() {
        let vfs = IoUringVfs::new();
        let snap = vfs.status_snapshot();
        let cloned = snap.clone();
        assert_eq!(cloned, snap);
        let dbg = format!("{snap:?}");
        assert!(dbg.contains("IoUringRuntimeStatus"));
        assert!(dbg.contains(snap.backend));
    }

    #[test]
    fn io_uring_vfs_default_equals_new() {
        let from_new = IoUringVfs::new();
        let from_default = IoUringVfs::default();
        assert_eq!(from_new.name(), from_default.name());
        assert_eq!(from_new.status(), from_default.status());
    }

    #[test]
    fn io_uring_vfs_status_snapshot_fresh_fields() {
        let _guard = io_uring_test_guard();
        let vfs = IoUringVfs::new();
        let snap = vfs.status_snapshot();
        assert!(!snap.backend.is_empty());
        assert!(!snap.disabled);
        assert!(snap.disable_reason.is_none());
        assert!(!snap.initial_status.is_empty());
        assert!(!snap.status.is_empty());
    }

    #[test]
    fn io_uring_vfs_status_contains_backend() {
        let vfs = IoUringVfs::new();
        let status = vfs.status();
        let snap = vfs.status_snapshot();
        assert!(status.contains(snap.backend));
    }
}
