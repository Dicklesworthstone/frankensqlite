use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
#[cfg(all(feature = "native", target_os = "linux"))]
use std::time::Instant;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
#[cfg(all(feature = "native", target_os = "linux"))]
use fsqlite_vfs::IoUringVfs;
#[cfg(all(feature = "native", unix))]
use fsqlite_vfs::UnixVfs;
use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};
#[cfg(all(feature = "native", unix))]
use tempfile::tempdir;

#[cfg(all(feature = "native", target_os = "linux"))]
const PAGE_SIZE: usize = 4096;
#[cfg(all(feature = "native", target_os = "linux"))]
const PAGE_SIZE_U64: u64 = 4096;
#[cfg(all(feature = "native", target_os = "linux"))]
const BENCH_PAGE_COUNT: u64 = 128;
#[cfg(all(feature = "native", target_os = "linux"))]
const DEFAULT_BENCH_ITERS: usize = 1024;

fn open_main_rw_file<V: Vfs>(vfs: &V, cx: &Cx, path: &Path) -> Result<(V::File, VfsOpenFlags)> {
    vfs.open(
        cx,
        Some(path),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB,
    )
}

#[cfg(all(feature = "native", target_os = "linux"))]
fn open_data_path_file<V: Vfs>(vfs: &V, cx: &Cx, path: &Path) -> Result<(V::File, VfsOpenFlags)> {
    vfs.open(
        cx,
        Some(path),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE,
    )
}

fn exercise_named_file_contract<V: Vfs>(vfs: &V, path: &Path) -> Result<()> {
    let cx = Cx::new();
    assert!(!vfs.access(&cx, path, AccessFlags::EXISTS)?);

    let (mut file, out_flags) = open_main_rw_file(vfs, &cx, path)?;
    assert!(out_flags.contains(VfsOpenFlags::READWRITE));

    file.write(&cx, b"xyz", 5)?;
    assert_eq!(file.file_size(&cx)?, 8);

    file.sync(&cx, SyncFlags::NORMAL)?;
    file.sync(&cx, SyncFlags::FULL)?;
    file.sync(&cx, SyncFlags::DATAONLY)?;

    let mut buf = [0_u8; 10];
    let read = file.read(&cx, &mut buf, 0)?;
    assert_eq!(read, 8);
    assert_eq!(&buf[..5], &[0_u8; 5]);
    assert_eq!(&buf[5..8], b"xyz");
    assert_eq!(&buf[8..], &[0_u8; 2]);

    file.close(&cx)?;
    file.close(&cx)?;
    assert!(vfs.access(&cx, path, AccessFlags::EXISTS)?);

    let (mut reopened, _) = open_main_rw_file(vfs, &cx, path)?;
    let mut reopened_buf = [0_u8; 8];
    let reopened_read = reopened.read(&cx, &mut reopened_buf, 0)?;
    assert_eq!(reopened_read, 8);
    assert_eq!(&reopened_buf[..5], &[0_u8; 5]);
    assert_eq!(&reopened_buf[5..], b"xyz");
    reopened.close(&cx)?;

    vfs.delete(&cx, path, false)?;
    assert!(!vfs.access(&cx, path, AccessFlags::EXISTS)?);
    Ok(())
}

fn exercise_delete_on_close_contract<V: Vfs>(vfs: &V, path: &Path) -> Result<()> {
    let cx = Cx::new();
    let (mut file, out_flags) = vfs.open(
        &cx,
        Some(path),
        VfsOpenFlags::READWRITE
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::MAIN_DB
            | VfsOpenFlags::DELETEONCLOSE,
    )?;
    assert!(out_flags.contains(VfsOpenFlags::READWRITE));
    file.write(&cx, b"drop-me", 0)?;
    file.close(&cx)?;
    file.close(&cx)?;
    assert!(!vfs.access(&cx, path, AccessFlags::EXISTS)?);
    Ok(())
}

#[test]
fn memory_vfs_contract_roundtrip_and_delete_on_close() -> Result<()> {
    let vfs = MemoryVfs::new();
    exercise_named_file_contract(&vfs, Path::new("bd_3u7_4_memory_contract.db"))?;
    exercise_delete_on_close_contract(&vfs, Path::new("bd_3u7_4_memory_delete_on_close.db"))?;
    Ok(())
}

#[cfg(all(feature = "native", unix))]
#[test]
fn unix_vfs_contract_roundtrip_and_delete_on_close() -> Result<()> {
    let tempdir = tempdir().map_err(FrankenError::Io)?;
    let vfs = UnixVfs::new();
    exercise_named_file_contract(&vfs, &tempdir.path().join("contract.db"))?;
    exercise_delete_on_close_contract(&vfs, &tempdir.path().join("delete-on-close.db"))?;
    Ok(())
}

#[cfg(all(feature = "native", target_os = "linux"))]
fn throughput_iters() -> usize {
    std::env::var("BD_3U7_4_BENCH_ITERS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|iters| *iters > 0)
        .unwrap_or(DEFAULT_BENCH_ITERS)
}

#[cfg(all(feature = "native", target_os = "linux"))]
fn require_io_uring_ratio_gate() -> bool {
    std::env::var("BD_3U7_4_REQUIRE_IO_URING")
        .is_ok_and(|raw| matches!(raw.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

#[cfg(all(feature = "native", target_os = "linux"))]
fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

#[cfg(all(feature = "native", target_os = "linux"))]
fn run_random_4k_read_benchmark<V: Vfs>(vfs: &V, path: &Path, iterations: usize) -> Result<f64> {
    let cx = Cx::new();
    let (mut file, _) = open_data_path_file(vfs, &cx, path)?;
    let page = [0xAB_u8; PAGE_SIZE];

    for page_index in 0..BENCH_PAGE_COUNT {
        file.write(&cx, &page, page_index * PAGE_SIZE_U64)?;
    }
    file.sync(&cx, SyncFlags::NORMAL)?;

    let mut seed = 0x3A7D_4F4A_u64;
    let mut buf = [0_u8; PAGE_SIZE];
    let started = Instant::now();
    for _ in 0..iterations {
        let page_index = lcg_next(&mut seed) % BENCH_PAGE_COUNT;
        let read = file.read(&cx, &mut buf, page_index * PAGE_SIZE_U64)?;
        assert_eq!(read, PAGE_SIZE);
        std::hint::black_box(buf[0]);
    }

    file.close(&cx)?;
    let elapsed = started.elapsed().as_secs_f64();
    assert!(elapsed > 0.0);
    Ok(iterations as f64 / elapsed)
}

#[cfg(all(feature = "native", target_os = "linux"))]
#[test]
fn io_uring_random_4k_read_throughput_is_measured() -> Result<()> {
    let tempdir = tempdir().map_err(FrankenError::Io)?;
    let unix_vfs = UnixVfs::new();
    let unix_ops = run_random_4k_read_benchmark(
        &unix_vfs,
        &tempdir.path().join("unix-throughput.db"),
        throughput_iters(),
    )?;

    let io_uring_vfs = IoUringVfs::new();
    if !io_uring_vfs.is_available() {
        eprintln!(
            "io_uring unavailable for bd-3u7.4 throughput measurement: {}",
            io_uring_vfs.status()
        );
        return Ok(());
    }

    let io_uring_ops = run_random_4k_read_benchmark(
        &io_uring_vfs,
        &tempdir.path().join("io-uring-throughput.db"),
        throughput_iters(),
    )?;
    let ratio = io_uring_ops / unix_ops;
    eprintln!(
        "bd-3u7.4 throughput unix_ops_per_sec={unix_ops:.2} io_uring_ops_per_sec={io_uring_ops:.2} ratio={ratio:.3} status={}",
        io_uring_vfs.status()
    );

    assert!(unix_ops.is_finite() && unix_ops > 0.0);
    assert!(io_uring_ops.is_finite() && io_uring_ops > 0.0);
    assert!(ratio.is_finite() && ratio > 0.0);
    if require_io_uring_ratio_gate() {
        assert!(
            ratio >= 2.0,
            "expected io_uring read throughput ratio >= 2.0, got {ratio:.3} (unix={unix_ops:.2}, io_uring={io_uring_ops:.2})"
        );
    }

    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum InjectedReadFault {
    Io,
}

#[derive(Clone, Copy, Debug)]
enum InjectedWriteFault {
    Io,
    Partial { valid_bytes: usize },
}

#[derive(Clone, Copy, Debug)]
enum InjectedSyncFault {
    Io,
}

#[derive(Debug, Default)]
#[allow(clippy::struct_field_names)]
struct InjectedFaultState {
    next_read: Option<InjectedReadFault>,
    next_write: Option<InjectedWriteFault>,
    next_sync: Option<InjectedSyncFault>,
}

#[derive(Debug)]
struct TestFaultVfs<V: Vfs> {
    inner: V,
    faults: Arc<Mutex<InjectedFaultState>>,
}

impl<V: Vfs> TestFaultVfs<V> {
    fn new(inner: V) -> Self {
        Self {
            inner,
            faults: Arc::new(Mutex::new(InjectedFaultState::default())),
        }
    }

    fn inject_read_io(&self) {
        self.faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_read = Some(InjectedReadFault::Io);
    }

    fn inject_write_io(&self) {
        self.faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_write = Some(InjectedWriteFault::Io);
    }

    fn inject_partial_write(&self, valid_bytes: usize) {
        self.faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_write = Some(InjectedWriteFault::Partial { valid_bytes });
    }

    fn inject_sync_io(&self) {
        self.faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_sync = Some(InjectedSyncFault::Io);
    }
}

#[derive(Debug)]
struct TestFaultFile<F: VfsFile> {
    inner: F,
    faults: Arc<Mutex<InjectedFaultState>>,
}

fn injected_io_error(message: &'static str) -> FrankenError {
    FrankenError::Io(io::Error::other(message))
}

impl<V: Vfs> Vfs for TestFaultVfs<V> {
    type File = TestFaultFile<V::File>;

    fn name(&self) -> &'static str {
        "test-fault-vfs"
    }

    fn open(
        &self,
        cx: &Cx,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (inner, out_flags) = self.inner.open(cx, path, flags)?;
        Ok((
            TestFaultFile {
                inner,
                faults: Arc::clone(&self.faults),
            },
            out_flags,
        ))
    }

    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()> {
        self.inner.delete(cx, path, sync_dir)
    }

    fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool> {
        self.inner.access(cx, path, flags)
    }

    fn full_pathname(&self, cx: &Cx, path: &Path) -> Result<PathBuf> {
        self.inner.full_pathname(cx, path)
    }

    fn randomness(&self, cx: &Cx, buf: &mut [u8]) {
        self.inner.randomness(cx, buf);
    }

    fn current_time(&self, cx: &Cx) -> f64 {
        self.inner.current_time(cx)
    }

    fn is_memory(&self) -> bool {
        self.inner.is_memory()
    }
}

impl<F: VfsFile> VfsFile for TestFaultFile<F> {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.inner.close(cx)
    }

    fn read(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        let maybe_fault = self
            .faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_read
            .take();
        match maybe_fault {
            Some(InjectedReadFault::Io) => Err(injected_io_error("fault injection: read failure")),
            None => self.inner.read(cx, buf, offset),
        }
    }

    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        let maybe_fault = self
            .faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_write
            .take();
        match maybe_fault {
            Some(InjectedWriteFault::Io) => {
                Err(injected_io_error("fault injection: write failure"))
            }
            Some(InjectedWriteFault::Partial { valid_bytes }) => {
                let applied = valid_bytes.min(buf.len());
                if applied > 0 {
                    self.inner.write(cx, &buf[..applied], offset)?;
                }
                Err(injected_io_error("fault injection: partial write"))
            }
            None => self.inner.write(cx, buf, offset),
        }
    }

    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.inner.truncate(cx, size)
    }

    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        let maybe_fault = self
            .faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_sync
            .take();
        match maybe_fault {
            Some(InjectedSyncFault::Io) => Err(injected_io_error("fault injection: sync failure")),
            None => self.inner.sync(cx, flags),
        }
    }

    fn file_size(&self, cx: &Cx) -> Result<u64> {
        self.inner.file_size(cx)
    }

    fn lock(&mut self, cx: &Cx, level: fsqlite_types::LockLevel) -> Result<()> {
        self.inner.lock(cx, level)
    }

    fn unlock(&mut self, cx: &Cx, level: fsqlite_types::LockLevel) -> Result<()> {
        self.inner.unlock(cx, level)
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

    fn shm_map(
        &mut self,
        cx: &Cx,
        region: u32,
        size: u32,
        extend: bool,
    ) -> Result<fsqlite_vfs::ShmRegion> {
        self.inner.shm_map(cx, region, size, extend)
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

#[test]
fn fault_injection_wrapper_surfaces_read_write_sync_and_partial_write_errors() -> Result<()> {
    let cx = Cx::new();
    let vfs = TestFaultVfs::new(MemoryVfs::new());
    let (mut file, _) = open_main_rw_file(&vfs, &cx, Path::new("bd_3u7_4_faults.db"))?;

    file.write(&cx, b"abcdefgh", 0)?;

    vfs.inject_read_io();
    let mut read_buf = [0_u8; 8];
    let read_err = file
        .read(&cx, &mut read_buf, 0)
        .expect_err("faulted read should fail");
    assert!(matches!(read_err, FrankenError::Io(_)));

    let read = file.read(&cx, &mut read_buf, 0)?;
    assert_eq!(read, 8);
    assert_eq!(&read_buf, b"abcdefgh");

    vfs.inject_write_io();
    let write_err = file
        .write(&cx, b"ZZ", 0)
        .expect_err("faulted write should fail");
    assert!(matches!(write_err, FrankenError::Io(_)));

    let read = file.read(&cx, &mut read_buf, 0)?;
    assert_eq!(read, 8);
    assert_eq!(&read_buf, b"abcdefgh");

    vfs.inject_partial_write(3);
    let partial_err = file
        .write(&cx, b"XYZW", 4)
        .expect_err("partial write fault should fail");
    assert!(matches!(partial_err, FrankenError::Io(_)));

    let read = file.read(&cx, &mut read_buf, 0)?;
    assert_eq!(read, 8);
    assert_eq!(&read_buf, b"abcdXYZh");

    vfs.inject_sync_io();
    let sync_err = file
        .sync(&cx, SyncFlags::FULL)
        .expect_err("faulted sync should fail");
    assert!(matches!(sync_err, FrankenError::Io(_)));

    file.close(&cx)?;
    Ok(())
}
