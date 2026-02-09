//! Unix VFS implementation with POSIX fcntl-based five-level locking.
//!
//! This module implements the `Vfs` and `VfsFile` traits using standard POSIX
//! file I/O and advisory locking. The locking protocol matches C SQLite's
//! `os_unix.c` implementation:
//!
//! **Lock hierarchy:** `None < Shared < Reserved < Pending < Exclusive`
//!
//! **Lock byte ranges (at the 1 GB boundary):**
//! - `PENDING_BYTE`  = `0x4000_0000` (1 byte)
//! - `RESERVED_BYTE` = `0x4000_0001` (1 byte)
//! - `SHARED_FIRST`  = `0x4000_0002` (510 bytes)
//!
//! **Key design:** POSIX fcntl locks are per-process, not per-fd. If one fd in
//! a process holds a lock, closing *any* fd to the same file releases it. We
//! handle this with a global inode table ([`InodeTable`]) that coalesces locks
//! across all file handles in the same process.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
#[cfg(test)]
use tracing::debug;
use tracing::{error, warn};

use crate::shm::{
    SQLITE_SHM_EXCLUSIVE, SQLITE_SHM_LOCK, SQLITE_SHM_SHARED, SQLITE_SHM_UNLOCK, ShmRegion,
    WAL_NREADER_USIZE, WAL_TOTAL_LOCKS, WAL_WRITE_LOCK, wal_lock_byte, wal_read_lock_slot,
};
use crate::traits::{Vfs, VfsFile};

fn checkpoint_or_abort(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)
}

#[cfg(test)]
macro_rules! lock_debug {
    ($($arg:tt)*) => {
        debug!($($arg)*);
    };
}

#[cfg(not(test))]
macro_rules! lock_debug {
    ($($arg:tt)*) => {{};};
}

// ---------------------------------------------------------------------------
// Lock byte constants (must match C SQLite for file-level compatibility)
// ---------------------------------------------------------------------------

/// Byte offset of the pending lock byte.
const PENDING_BYTE: u64 = 0x4000_0000;
/// Byte offset of the reserved lock byte.
const RESERVED_BYTE: u64 = PENDING_BYTE + 1;
/// Byte offset of the first shared lock byte.
const SHARED_FIRST: u64 = PENDING_BYTE + 2;
/// Number of bytes in the shared lock range.
const SHARED_SIZE: u64 = 510;

// ---------------------------------------------------------------------------
// WAL SHM header initialization (legacy SQLite interop)
// ---------------------------------------------------------------------------

/// SQLite WAL-index SHM segment size (`WALINDEX_PGSZ` in upstream SQLite).
///
/// This is always 32 KiB and is required so that legacy SQLite can map
/// the first wal-index page without needing to take `WAL_WRITE_LOCK` just to
/// grow the `*-shm` file. If we hold `WAL_WRITE_LOCK` on a freshly created
/// zero-byte `*-shm`, legacy SQLite will spin in `walTryBeginRead()` and
/// eventually surface `SQLITE_PROTOCOL` ("locking protocol").
const SQLITE_WALINDEX_PGSZ: u64 = 32 * 1024;

/// Bytes in the `*-shm` header region: 2x `WalIndexHdr` (48 bytes each) + `WalCkptInfo` (40 bytes).
const SQLITE_WAL_SHM_HEADER_BYTES: usize = 136;

/// `WalIndexHdr.iVersion` constant (must be 3007000).
const SQLITE_WAL_INDEX_VERSION: u32 = 3_007_000;

/// `WalCkptInfo.aReadMark[i]` value indicating the slot is unused.
const SQLITE_WAL_READMARK_NOT_USED: u32 = 0xffff_ffff;

/// Slot index for the `*-shm` deadman-switch (DMS) byte.
///
/// In C SQLite's unix VFS, this is `UNIX_SHM_DMS = UNIX_SHM_BASE + SQLITE_SHM_NLOCK`
/// and lives at byte offset 128. Holding a SHARED lock on this byte prevents
/// new openers from truncating the `*-shm` file on startup.
const SQLITE_SHM_DMS_SLOT: u32 = WAL_TOTAL_LOCKS;

fn sqlite_wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    PathBuf::from(wal)
}

fn sqlite_shm_dms_lock_byte() -> u64 {
    let base = wal_lock_byte(WAL_WRITE_LOCK).expect("WAL write lock byte must exist");
    base + u64::from(WAL_TOTAL_LOCKS)
}

fn sqlite_page_size_from_db_header(db_header: &[u8]) -> Result<u32> {
    const DB_HEADER_BYTES: usize = 100;
    if db_header.len() < DB_HEADER_BYTES {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "sqlite db header too small: expected >= {DB_HEADER_BYTES}, got {}",
                db_header.len()
            ),
        });
    }

    let raw = u16::from_be_bytes([db_header[16], db_header[17]]);
    let page_size = if raw == 1 { 65_536 } else { u32::from(raw) };
    if !(page_size.is_power_of_two() && (512..=65_536).contains(&page_size)) {
        return Err(FrankenError::WalCorrupt {
            detail: format!("invalid sqlite page size in db header: {page_size}"),
        });
    }
    Ok(page_size)
}

fn sqlite_wal_checksum_native_8byte_chunks(data: &[u8]) -> Result<(u32, u32)> {
    if data.len() % 8 != 0 {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "sqlite wal checksum input must be 8-byte aligned, got {} bytes",
                data.len()
            ),
        });
    }

    let mut s1 = 0_u32;
    let mut s2 = 0_u32;
    for chunk in data.chunks_exact(8) {
        let w1 = u32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let w2 = u32::from_ne_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        s1 = s1.wrapping_add(w1).wrapping_add(s2);
        s2 = s2.wrapping_add(w2).wrapping_add(s1);
    }
    Ok((s1, s2))
}

fn write_ne_u32(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_ne_bytes());
}

fn build_empty_sqlite_wal_shm_header(
    page_size: u32,
    n_page: u32,
) -> Result<[u8; SQLITE_WAL_SHM_HEADER_BYTES]> {
    let sz_page_u16 = if page_size == 65_536 {
        1_u16
    } else {
        u16::try_from(page_size).map_err(|_| FrankenError::WalCorrupt {
            detail: format!("page size too large for wal-index header: {page_size}"),
        })?
    };

    // Build a single WalIndexHdr copy (48 bytes) in native order.
    let mut hdr = [0_u8; 48];
    write_ne_u32(&mut hdr, 0, SQLITE_WAL_INDEX_VERSION);
    write_ne_u32(&mut hdr, 4, 0); // unused
    write_ne_u32(&mut hdr, 8, 0); // iChange
    hdr[12] = 1; // isInit
    hdr[13] = 0; // bigEndCksum (normal little-endian WAL checksums)
    hdr[14..16].copy_from_slice(&sz_page_u16.to_ne_bytes());
    write_ne_u32(&mut hdr, 16, 0); // mxFrame (empty WAL)
    write_ne_u32(&mut hdr, 20, n_page);
    write_ne_u32(&mut hdr, 24, 0); // aFrameCksum[0]
    write_ne_u32(&mut hdr, 28, 0); // aFrameCksum[1]
    write_ne_u32(&mut hdr, 32, 0); // aSalt[0]
    write_ne_u32(&mut hdr, 36, 0); // aSalt[1]

    let (ck1, ck2) = sqlite_wal_checksum_native_8byte_chunks(&hdr[..40])?;
    write_ne_u32(&mut hdr, 40, ck1);
    write_ne_u32(&mut hdr, 44, ck2);

    // Build WalCkptInfo (40 bytes) in native order.
    let mut ckpt = [0_u8; 40];
    write_ne_u32(&mut ckpt, 0, 0); // nBackfill
    // aReadMark[0] is always 0; remaining marks unused for empty WAL.
    write_ne_u32(&mut ckpt, 4, 0);
    for i in 1..5 {
        write_ne_u32(&mut ckpt, 4 + i * 4, SQLITE_WAL_READMARK_NOT_USED);
    }
    // aLock[8] left as zeros (reserved bytes for OS-level locks).
    write_ne_u32(&mut ckpt, 32, 0); // nBackfillAttempted
    write_ne_u32(&mut ckpt, 36, 0); // notUsed0

    let mut out = [0_u8; SQLITE_WAL_SHM_HEADER_BYTES];
    out[..48].copy_from_slice(&hdr);
    out[48..96].copy_from_slice(&hdr);
    out[96..136].copy_from_slice(&ckpt);
    Ok(out)
}

fn sqlite_wal_shm_header_is_valid(buf: &[u8]) -> Result<bool> {
    if buf.len() < SQLITE_WAL_SHM_HEADER_BYTES {
        return Ok(false);
    }

    let h1 = &buf[..48];
    let h2 = &buf[48..96];
    if h1 != h2 {
        return Ok(false);
    }

    if h1[12] == 0 {
        return Ok(false);
    }

    let (expected1, expected2) = sqlite_wal_checksum_native_8byte_chunks(&h1[..40])?;
    let actual1 = u32::from_ne_bytes([h1[40], h1[41], h1[42], h1[43]]);
    let actual2 = u32::from_ne_bytes([h1[44], h1[45], h1[46], h1[47]]);
    Ok(expected1 == actual1 && expected2 == actual2)
}

// ---------------------------------------------------------------------------
// POSIX fcntl helpers
// ---------------------------------------------------------------------------

/// Attempt a non-blocking POSIX advisory lock via `fcntl(F_SETLK)`.
///
/// Uses the `nix` crate for safe syscall wrapping (no `unsafe` needed).
///
/// Returns `Ok(true)` if the lock was acquired, `Ok(false)` if it would
/// block (another process holds a conflicting lock), and `Err` for real
/// I/O errors.
#[allow(clippy::cast_possible_wrap)]
fn posix_lock(file: &impl AsFd, lock_type: i32, start: u64, len: u64) -> Result<bool> {
    let lock_type = i16::try_from(lock_type).expect("fcntl lock type must fit in i16");
    let whence = i16::try_from(libc::SEEK_SET).expect("SEEK_SET must fit in i16");
    let flock = libc::flock {
        l_type: lock_type,
        l_whence: whence,
        l_start: start as libc::off_t,
        l_len: len as libc::off_t,
        l_pid: 0,
    };

    match nix::fcntl::fcntl(
        file.as_fd().as_raw_fd(),
        nix::fcntl::FcntlArg::F_SETLK(&flock),
    ) {
        Ok(_) => Ok(true),
        Err(nix::errno::Errno::EACCES | nix::errno::Errno::EAGAIN) => Ok(false),
        Err(e) => Err(FrankenError::Io(e.into())),
    }
}

/// Release a POSIX advisory lock.
fn posix_unlock(file: &impl AsFd, start: u64, len: u64) -> Result<()> {
    let ok = posix_lock(file, libc::F_UNLCK, start, len)?;
    debug_assert!(ok, "F_UNLCK should never fail with EAGAIN");
    Ok(())
}

/// Query whether a lock would succeed without acquiring it.
///
/// Uses `fcntl(F_GETLK)` and returns the kernel-filled `flock`.
#[allow(clippy::cast_possible_wrap)]
#[allow(dead_code)]
fn posix_getlk(file: &impl AsFd, lock_type: i32, start: u64, len: u64) -> Result<libc::flock> {
    let lock_type = i16::try_from(lock_type).expect("fcntl lock type must fit in i16");
    let whence = i16::try_from(libc::SEEK_SET).expect("SEEK_SET must fit in i16");
    let mut flock = libc::flock {
        l_type: lock_type,
        l_whence: whence,
        l_start: start as libc::off_t,
        l_len: len as libc::off_t,
        l_pid: 0,
    };

    nix::fcntl::fcntl(
        file.as_fd().as_raw_fd(),
        nix::fcntl::FcntlArg::F_GETLK(&mut flock),
    )
    .map_err(|e| FrankenError::Io(e.into()))?;

    Ok(flock)
}

// ---------------------------------------------------------------------------
// Inode table — per-process lock coalescing
// ---------------------------------------------------------------------------

/// Unique identity for an open file (device + inode).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct InodeKey {
    dev: u64,
    ino: u64,
}

/// Per-inode lock state shared across all file handles in this process.
#[derive(Debug)]
struct InodeInfo {
    /// Canonical file descriptor for this inode.
    ///
    /// POSIX fcntl locks are per-process, and closing *any* fd for the file can
    /// release the process' locks. To avoid that, we keep exactly one fd per
    /// inode in-process and share it across handles via `Arc`.
    file: Arc<File>,
    /// Number of handles that hold at least SHARED.
    n_shared: u32,
    /// Number of handles that hold at least RESERVED.
    n_reserved: u32,
    /// Number of handles that hold at least PENDING.
    n_pending: u32,
    /// Number of handles that hold EXCLUSIVE.
    n_exclusive: u32,
    /// Total number of open file handles referencing this inode.
    n_ref: u32,
}

impl InodeInfo {
    fn new(file: Arc<File>) -> Self {
        Self {
            file,
            n_shared: 0,
            n_reserved: 0,
            n_pending: 0,
            n_exclusive: 0,
            n_ref: 0,
        }
    }
}

/// Global per-process table mapping (dev, ino) to shared lock state.
///
/// This prevents the "POSIX close drops all locks" problem: we only issue
/// OS-level lock/unlock calls through one canonical fd per inode, and track
/// how many handles want each lock level.
struct InodeTable {
    map: Mutex<HashMap<InodeKey, Arc<Mutex<InodeInfo>>>>,
}

impl InodeTable {
    fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    /// Get the inode info for the given key if present.
    fn get(&self, key: InodeKey) -> Option<Arc<Mutex<InodeInfo>>> {
        let map = self.map.lock().expect("inode table lock poisoned");
        map.get(&key).cloned()
    }

    /// Get or create the inode info for the given key.
    fn get_or_create(&self, key: InodeKey, file: Arc<File>) -> Arc<Mutex<InodeInfo>> {
        let mut map = self.map.lock().expect("inode table lock poisoned");
        Arc::clone(
            map.entry(key)
                .or_insert_with(|| Arc::new(Mutex::new(InodeInfo::new(file)))),
        )
    }

    /// Remove the inode entry if its refcount reaches zero.
    fn maybe_remove(&self, key: InodeKey) {
        let mut map = self.map.lock().expect("inode table lock poisoned");
        if let Some(info) = map.get(&key) {
            let guard = info.lock().expect("inode info lock poisoned");
            if guard.n_ref == 0 {
                drop(guard);
                map.remove(&key);
            }
        }
    }
}

/// The singleton global inode table for the process.
fn global_inode_table() -> &'static InodeTable {
    static TABLE: OnceLock<InodeTable> = OnceLock::new();
    TABLE.get_or_init(InodeTable::new)
}

// ---------------------------------------------------------------------------
// SHM table — per-process SHM region/lock coalescing
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct ShmSlotState {
    shared_holders: HashMap<u64, u32>,
    exclusive_owner: Option<u64>,
}

#[derive(Debug)]
struct ShmInfo {
    file: Arc<File>,
    regions: HashMap<u32, ShmRegion>,
    slots: Vec<ShmSlotState>,
    owner_refs: HashMap<u64, u32>,
    read_marks: [u32; WAL_NREADER_USIZE],
}

impl ShmInfo {
    fn new(file: Arc<File>) -> Self {
        // Slots 0..WAL_TOTAL_LOCKS are the 8 legacy WAL lock bytes (120-127).
        // Slot WAL_TOTAL_LOCKS is the DMS ("deadman switch") byte (128) used by
        // SQLite to coordinate first-opener truncation of `*-shm`.
        let slot_count =
            usize::try_from(WAL_TOTAL_LOCKS.saturating_add(1)).expect("WAL lock count fits usize");
        Self {
            file,
            regions: HashMap::new(),
            slots: std::iter::repeat_with(ShmSlotState::default)
                .take(slot_count)
                .collect(),
            owner_refs: HashMap::new(),
            read_marks: [0; WAL_NREADER_USIZE],
        }
    }

    fn read_marks(&self) -> [u32; WAL_NREADER_USIZE] {
        self.read_marks
    }
}

struct ShmTable {
    map: Mutex<HashMap<PathBuf, Arc<Mutex<ShmInfo>>>>,
}

impl ShmTable {
    fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    fn get_or_create(&self, path: PathBuf) -> Result<Arc<Mutex<ShmInfo>>> {
        // IMPORTANT: POSIX fcntl locks are per-process. If we open and then close a new
        // fd to an already-locked `*-shm` file, we can drop all locks held by this
        // process on that file. To avoid that, only ever open `*-shm` while holding
        // this mutex and only when we're definitely creating the canonical entry.
        let mut map = self.map.lock().expect("shm table lock poisoned");
        if let Some(existing) = map.get(&path) {
            return Ok(Arc::clone(existing));
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(FrankenError::Io)?;

        let info = Arc::new(Mutex::new(ShmInfo::new(Arc::new(file))));
        map.insert(path, Arc::clone(&info));
        drop(map);
        Ok(info)
    }

    fn remove_if_orphaned(&self, path: &Path) {
        let mut map = self.map.lock().expect("shm table lock poisoned");
        if let Some(entry) = map.get(path) {
            let info = entry.lock().expect("shm info lock poisoned");
            if info.owner_refs.is_empty() {
                drop(info);
                map.remove(path);
            }
        }
    }
}

fn global_shm_table() -> &'static ShmTable {
    static TABLE: OnceLock<ShmTable> = OnceLock::new();
    TABLE.get_or_init(ShmTable::new)
}

static SHM_OWNER_SEQ: AtomicU64 = AtomicU64::new(1);

fn next_shm_owner_id() -> u64 {
    SHM_OWNER_SEQ.fetch_add(1, Ordering::Relaxed)
}

fn sqlite_shm_path(path: &Path) -> PathBuf {
    let mut shm = path.as_os_str().to_owned();
    shm.push("-shm");
    PathBuf::from(shm)
}

// ---------------------------------------------------------------------------
// UnixVfs
// ---------------------------------------------------------------------------

/// A VFS backed by the real Unix filesystem with POSIX advisory locking.
#[derive(Debug)]
pub struct UnixVfs;

impl UnixVfs {
    /// Create a new Unix VFS instance.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for UnixVfs {
    fn default() -> Self {
        Self::new()
    }
}

impl Vfs for UnixVfs {
    type File = UnixFile;

    fn name(&self) -> &'static str {
        "unix"
    }

    fn open(
        &self,
        cx: &Cx,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let is_temp = path.is_none();
        let resolved = if let Some(p) = path {
            p.to_path_buf()
        } else {
            let mut rng_buf = [0u8; 16];
            self.randomness(cx, &mut rng_buf);
            let mut hex = String::with_capacity(32);
            for b in rng_buf {
                write!(hex, "{b:02x}").expect("writing to a String should not fail");
            }
            std::env::temp_dir().join(format!("fsqlite_{hex}.db"))
        };

        let create_new = is_temp
            || (flags.contains(VfsOpenFlags::CREATE) && flags.contains(VfsOpenFlags::EXCLUSIVE));

        // Try to reuse the in-process canonical fd if the file already exists
        // and we're not creating a new exclusive file.
        if !create_new {
            if let Some(inode_key) = inode_key_from_path(&resolved)? {
                if let Some(inode_info) = global_inode_table().get(inode_key) {
                    let file = {
                        let mut info = inode_info.lock().expect("inode info lock poisoned");
                        info.n_ref += 1;
                        Arc::clone(&info.file)
                    };
                    let shm_path = sqlite_shm_path(&resolved);

                    let unix_file = UnixFile {
                        file,
                        path: resolved,
                        lock_level: LockLevel::None,
                        delete_on_close: flags.contains(VfsOpenFlags::DELETEONCLOSE),
                        inode_key,
                        inode_info,
                        shm_owner_id: next_shm_owner_id(),
                        shm_path,
                        shm_info: None,
                    };

                    let mut out_flags = flags;
                    if flags.contains(VfsOpenFlags::CREATE) {
                        out_flags |= VfsOpenFlags::READWRITE;
                    }
                    return Ok((unix_file, out_flags));
                }
            }
        }

        let is_create = is_temp || flags.contains(VfsOpenFlags::CREATE);
        let is_rw = is_temp || flags.contains(VfsOpenFlags::READWRITE) || is_create;

        let file = OpenOptions::new()
            .read(true)
            .write(is_rw)
            .create(is_create)
            .create_new(create_new)
            .open(&resolved)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    FrankenError::CannotOpen {
                        path: resolved.clone(),
                    }
                } else {
                    FrankenError::Io(e)
                }
            })?;

        // Install / reuse inode identity for per-process lock coalescing.
        let opened = Arc::new(file);
        let inode_key = inode_key_from_file(opened.as_ref())?;
        let inode_info = global_inode_table().get_or_create(inode_key, Arc::clone(&opened));
        let file = {
            let mut info = inode_info.lock().expect("inode info lock poisoned");
            info.n_ref += 1;
            Arc::clone(&info.file)
        };

        let mut out_flags = flags;
        if is_create {
            out_flags |= VfsOpenFlags::READWRITE;
        }
        if is_temp {
            // Temp files are always created read-write.
            out_flags |= VfsOpenFlags::READWRITE;
        }
        let shm_path = sqlite_shm_path(&resolved);

        let unix_file = UnixFile {
            file,
            path: resolved,
            lock_level: LockLevel::None,
            delete_on_close: flags.contains(VfsOpenFlags::DELETEONCLOSE),
            inode_key,
            inode_info,
            shm_owner_id: next_shm_owner_id(),
            shm_path,
            shm_info: None,
        };

        Ok((unix_file, out_flags))
    }

    fn delete(&self, _cx: &Cx, path: &Path, sync_dir: bool) -> Result<()> {
        fs::remove_file(path).map_err(FrankenError::Io)?;

        if sync_dir {
            if let Some(parent) = path.parent() {
                // Open the directory and fsync it.
                if let Ok(dir) = File::open(parent) {
                    drop(dir.sync_all());
                }
            }
        }

        Ok(())
    }

    fn access(&self, _cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool> {
        match flags {
            AccessFlags::READWRITE => {
                // Avoid opening the file (opening/closing extra fds can interact
                // poorly with fcntl locks). Use metadata-based heuristics.
                match fs::metadata(path) {
                    Ok(meta) => Ok(!meta.permissions().readonly()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
                    Err(e) => Err(FrankenError::Io(e)),
                }
            }
            _ => Ok(path.exists()),
        }
    }

    fn full_pathname(&self, _cx: &Cx, path: &Path) -> Result<PathBuf> {
        if path.is_absolute() {
            Ok(path.to_path_buf())
        } else {
            let cwd = std::env::current_dir().map_err(FrankenError::Io)?;
            Ok(cwd.join(path))
        }
    }

    fn randomness(&self, _cx: &Cx, buf: &mut [u8]) {
        static FALLBACK_SEQ: AtomicU64 = AtomicU64::new(0);

        // Use /dev/urandom for real randomness; fall back to deterministic
        // xorshift if unavailable (for hermetic test environments).
        if let Ok(mut f) = File::open("/dev/urandom") {
            if f.read_exact(buf).is_ok() {
                return;
            }
        }

        let seq = FALLBACK_SEQ.fetch_add(1, Ordering::Relaxed);
        let mut state: u64 = 0x5DEE_CE66_D1A4_F681 ^ seq.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        for chunk in buf.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let bytes = state.to_le_bytes();
            for (dst, &src) in chunk.iter_mut().zip(bytes.iter()) {
                *dst = src;
            }
        }
    }
}

/// Extract the (device, inode) pair from an open file for lock coalescing.
fn inode_key_from_file(file: &File) -> Result<InodeKey> {
    use std::os::unix::fs::MetadataExt;
    let meta = file.metadata().map_err(FrankenError::Io)?;
    Ok(InodeKey {
        dev: meta.dev(),
        ino: meta.ino(),
    })
}

/// Extract the (device, inode) pair from a path without opening the file.
///
/// Returns `Ok(None)` if the file does not exist.
fn inode_key_from_path(path: &Path) -> Result<Option<InodeKey>> {
    use std::os::unix::fs::MetadataExt;
    match fs::metadata(path) {
        Ok(meta) => Ok(Some(InodeKey {
            dev: meta.dev(),
            ino: meta.ino(),
        })),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(FrankenError::Io(e)),
    }
}

// ---------------------------------------------------------------------------
// UnixFile
// ---------------------------------------------------------------------------

/// A file handle opened by [`UnixVfs`].
#[derive(Debug)]
pub struct UnixFile {
    file: Arc<File>,
    path: PathBuf,
    lock_level: LockLevel,
    delete_on_close: bool,
    inode_key: InodeKey,
    inode_info: Arc<Mutex<InodeInfo>>,
    shm_owner_id: u64,
    shm_path: PathBuf,
    shm_info: Option<Arc<Mutex<ShmInfo>>>,
}

impl UnixFile {
    fn ensure_shm_info(&mut self) -> Result<Arc<Mutex<ShmInfo>>> {
        if let Some(info) = &self.shm_info {
            return Ok(Arc::clone(info));
        }

        let info = global_shm_table().get_or_create(self.shm_path.clone())?;
        {
            let mut guard = info.lock().expect("shm info lock poisoned");
            *guard.owner_refs.entry(self.shm_owner_id).or_insert(0) += 1;
        }
        self.shm_info = Some(Arc::clone(&info));
        Ok(info)
    }

    fn release_shm_owner_state(&mut self, delete: bool) -> Result<()> {
        let Some(info_arc) = self.shm_info.take() else {
            if delete {
                drop(fs::remove_file(&self.shm_path));
            }
            return Ok(());
        };

        {
            let mut info = info_arc.lock().expect("shm info lock poisoned");
            let mut first_error: Option<FrankenError> = None;
            let shm_file = Arc::clone(&info.file);

            for slot in 0..WAL_TOTAL_LOCKS {
                #[allow(clippy::cast_possible_truncation)]
                let slot_idx = slot as usize;
                let slot_state = &mut info.slots[slot_idx];

                if slot_state.exclusive_owner == Some(self.shm_owner_id) {
                    let Some(lock_byte) = wal_lock_byte(slot) else {
                        continue;
                    };
                    if slot_state.shared_holders.is_empty() {
                        if let Err(err) = posix_unlock(&*shm_file, lock_byte, 1) {
                            if first_error.is_none() {
                                first_error = Some(err);
                            }
                        }
                    }
                    slot_state.exclusive_owner = None;
                }

                if slot_state
                    .shared_holders
                    .remove(&self.shm_owner_id)
                    .is_some()
                {
                    let Some(lock_byte) = wal_lock_byte(slot) else {
                        continue;
                    };
                    if slot_state.exclusive_owner.is_none() && slot_state.shared_holders.is_empty()
                    {
                        if let Err(err) = posix_unlock(&*shm_file, lock_byte, 1) {
                            if first_error.is_none() {
                                first_error = Some(err);
                            }
                        }
                    }
                }
            }

            // Release DMS ("deadman switch") lock at byte 128 if held.
            {
                let slot_idx = usize::try_from(SQLITE_SHM_DMS_SLOT).expect("DMS slot fits usize");
                let slot_state = &mut info.slots[slot_idx];
                let lock_byte = sqlite_shm_dms_lock_byte();

                if slot_state.exclusive_owner == Some(self.shm_owner_id) {
                    if slot_state.shared_holders.is_empty() {
                        if let Err(err) = posix_unlock(&*shm_file, lock_byte, 1) {
                            if first_error.is_none() {
                                first_error = Some(err);
                            }
                        }
                    }
                    slot_state.exclusive_owner = None;
                }

                if slot_state
                    .shared_holders
                    .remove(&self.shm_owner_id)
                    .is_some()
                    && slot_state.exclusive_owner.is_none()
                    && slot_state.shared_holders.is_empty()
                {
                    if let Err(err) = posix_unlock(&*shm_file, lock_byte, 1) {
                        if first_error.is_none() {
                            first_error = Some(err);
                        }
                    }
                }
            }

            if let Some(count) = info.owner_refs.get_mut(&self.shm_owner_id) {
                if *count > 1 {
                    *count -= 1;
                } else {
                    info.owner_refs.remove(&self.shm_owner_id);
                }
            }

            let error_to_return = first_error;
            drop(info);
            if let Some(err) = error_to_return {
                return Err(err);
            }
        }

        if delete {
            drop(fs::remove_file(&self.shm_path));
        }
        global_shm_table().remove_if_orphaned(&self.shm_path);
        Ok(())
    }

    fn observed_mode(slot_state: &ShmSlotState) -> &'static str {
        if slot_state.exclusive_owner.is_some() {
            "exclusive"
        } else if slot_state.shared_holders.is_empty() {
            "unlocked"
        } else {
            "shared"
        }
    }

    fn log_lock_conflict(
        slot: u32,
        requested_mode: &'static str,
        observed_mode: &'static str,
        read_marks: [u32; WAL_NREADER_USIZE],
    ) {
        warn!(
            slot,
            lock_byte = wal_lock_byte(slot),
            requested_mode,
            observed_mode,
            ?read_marks,
            "legacy shm lock protocol conflict"
        );
    }

    fn acquire_shm_dms_shared(&self, info: &mut ShmInfo) -> Result<()> {
        let lock_byte = sqlite_shm_dms_lock_byte();
        let slot_idx = usize::try_from(SQLITE_SHM_DMS_SLOT).expect("DMS slot fits usize");
        let read_marks = info.read_marks();
        let slot_state = &mut info.slots[slot_idx];

        if let Some(owner) = slot_state.exclusive_owner {
            if owner != self.shm_owner_id {
                Self::log_lock_conflict(
                    SQLITE_SHM_DMS_SLOT,
                    "shared",
                    Self::observed_mode(slot_state),
                    read_marks,
                );
                return Err(FrankenError::Busy);
            }
            return Ok(());
        }

        let total_shared = slot_state.shared_holders.values().copied().sum::<u32>();
        if total_shared == 0 && !posix_lock(&*info.file, libc::F_RDLCK, lock_byte, 1)? {
            Self::log_lock_conflict(
                SQLITE_SHM_DMS_SLOT,
                "shared",
                Self::observed_mode(slot_state),
                read_marks,
            );
            return Err(FrankenError::Busy);
        }

        *slot_state
            .shared_holders
            .entry(self.shm_owner_id)
            .or_insert(0) += 1;
        lock_debug!(
            slot = SQLITE_SHM_DMS_SLOT,
            lock_byte,
            requested_mode = "shared",
            observed_mode = Self::observed_mode(slot_state),
            ?read_marks,
            "acquired shm DMS shared lock"
        );
        Ok(())
    }

    fn release_shm_dms_shared(&self, info: &mut ShmInfo) -> Result<()> {
        let lock_byte = sqlite_shm_dms_lock_byte();
        let slot_idx = usize::try_from(SQLITE_SHM_DMS_SLOT).expect("DMS slot fits usize");
        let read_marks = info.read_marks();
        let slot_state = &mut info.slots[slot_idx];
        let Some(holder_count) = slot_state.shared_holders.get_mut(&self.shm_owner_id) else {
            Self::log_lock_conflict(
                SQLITE_SHM_DMS_SLOT,
                "unlock-shared",
                Self::observed_mode(slot_state),
                read_marks,
            );
            return Err(FrankenError::LockFailed {
                detail: format!("owner {} does not hold shared DMS slot", self.shm_owner_id),
            });
        };

        if *holder_count > 1 {
            *holder_count -= 1;
        } else {
            slot_state.shared_holders.remove(&self.shm_owner_id);
        }

        if slot_state.exclusive_owner.is_none() && slot_state.shared_holders.is_empty() {
            posix_unlock(&*info.file, lock_byte, 1)?;
        }

        lock_debug!(
            slot = SQLITE_SHM_DMS_SLOT,
            lock_byte,
            requested_mode = "unlock-shared",
            observed_mode = Self::observed_mode(slot_state),
            ?read_marks,
            "released shm DMS shared lock"
        );
        Ok(())
    }

    fn acquire_shm_shared_slot(&self, info: &mut ShmInfo, slot: u32) -> Result<()> {
        let Some(lock_byte) = wal_lock_byte(slot) else {
            error!(slot, "invalid SHM slot for shared lock");
            return Err(FrankenError::LockFailed {
                detail: format!("invalid SHM slot {slot}"),
            });
        };
        let slot_idx = usize::try_from(slot).expect("slot index must fit usize");
        let read_marks = info.read_marks();
        let slot_state = &mut info.slots[slot_idx];

        if let Some(owner) = slot_state.exclusive_owner {
            if owner != self.shm_owner_id {
                Self::log_lock_conflict(
                    slot,
                    "shared",
                    Self::observed_mode(slot_state),
                    read_marks,
                );
                return Err(FrankenError::Busy);
            }
            // Same owner already holds exclusive; no extra transition required.
            return Ok(());
        }

        let total_shared = slot_state.shared_holders.values().copied().sum::<u32>();
        if total_shared == 0 && !posix_lock(&*info.file, libc::F_RDLCK, lock_byte, 1)? {
            Self::log_lock_conflict(slot, "shared", Self::observed_mode(slot_state), read_marks);
            return Err(FrankenError::Busy);
        }

        *slot_state
            .shared_holders
            .entry(self.shm_owner_id)
            .or_insert(0) += 1;
        lock_debug!(
            slot,
            lock_byte,
            requested_mode = "shared",
            observed_mode = Self::observed_mode(slot_state),
            ?read_marks,
            "acquired shm shared lock"
        );
        Ok(())
    }

    fn acquire_shm_exclusive_slot(&self, info: &mut ShmInfo, slot: u32) -> Result<()> {
        let Some(lock_byte) = wal_lock_byte(slot) else {
            error!(slot, "invalid SHM slot for exclusive lock");
            return Err(FrankenError::LockFailed {
                detail: format!("invalid SHM slot {slot}"),
            });
        };
        let slot_idx = usize::try_from(slot).expect("slot index must fit usize");
        let read_marks = info.read_marks();
        let slot_state = &mut info.slots[slot_idx];

        if slot_state.exclusive_owner == Some(self.shm_owner_id) {
            return Ok(());
        }
        if slot_state.exclusive_owner.is_some() {
            Self::log_lock_conflict(
                slot,
                "exclusive",
                Self::observed_mode(slot_state),
                read_marks,
            );
            return Err(FrankenError::Busy);
        }

        let shared_from_others = slot_state
            .shared_holders
            .iter()
            .any(|(owner, count)| *owner != self.shm_owner_id && *count > 0);
        if shared_from_others {
            Self::log_lock_conflict(
                slot,
                "exclusive",
                Self::observed_mode(slot_state),
                read_marks,
            );
            return Err(FrankenError::Busy);
        }

        slot_state.shared_holders.remove(&self.shm_owner_id);
        if !posix_lock(&*info.file, libc::F_WRLCK, lock_byte, 1)? {
            Self::log_lock_conflict(
                slot,
                "exclusive",
                Self::observed_mode(slot_state),
                read_marks,
            );
            return Err(FrankenError::Busy);
        }

        slot_state.exclusive_owner = Some(self.shm_owner_id);
        lock_debug!(
            slot,
            lock_byte,
            requested_mode = "exclusive",
            observed_mode = Self::observed_mode(slot_state),
            ?read_marks,
            "acquired shm exclusive lock"
        );
        Ok(())
    }

    fn release_shm_shared_slot(&self, info: &mut ShmInfo, slot: u32) -> Result<()> {
        let Some(lock_byte) = wal_lock_byte(slot) else {
            error!(slot, "invalid SHM slot for shared unlock");
            return Err(FrankenError::LockFailed {
                detail: format!("invalid SHM slot {slot}"),
            });
        };
        let slot_idx = usize::try_from(slot).expect("slot index must fit usize");
        let read_marks = info.read_marks();
        let slot_state = &mut info.slots[slot_idx];
        let Some(holder_count) = slot_state.shared_holders.get_mut(&self.shm_owner_id) else {
            Self::log_lock_conflict(
                slot,
                "unlock-shared",
                Self::observed_mode(slot_state),
                read_marks,
            );
            return Err(FrankenError::LockFailed {
                detail: format!(
                    "owner {} does not hold shared slot {slot}",
                    self.shm_owner_id
                ),
            });
        };

        if *holder_count > 1 {
            *holder_count -= 1;
        } else {
            slot_state.shared_holders.remove(&self.shm_owner_id);
        }

        if slot_state.exclusive_owner.is_none() && slot_state.shared_holders.is_empty() {
            posix_unlock(&*info.file, lock_byte, 1)?;
        }

        lock_debug!(
            slot,
            lock_byte,
            requested_mode = "unlock-shared",
            observed_mode = Self::observed_mode(slot_state),
            ?read_marks,
            "released shm shared lock"
        );
        Ok(())
    }

    fn release_shm_exclusive_slot(&self, info: &mut ShmInfo, slot: u32) -> Result<()> {
        let Some(lock_byte) = wal_lock_byte(slot) else {
            error!(slot, "invalid SHM slot for exclusive unlock");
            return Err(FrankenError::LockFailed {
                detail: format!("invalid SHM slot {slot}"),
            });
        };
        let slot_idx = usize::try_from(slot).expect("slot index must fit usize");
        let read_marks = info.read_marks();
        let slot_state = &mut info.slots[slot_idx];
        if slot_state.exclusive_owner != Some(self.shm_owner_id) {
            Self::log_lock_conflict(
                slot,
                "unlock-exclusive",
                Self::observed_mode(slot_state),
                read_marks,
            );
            return Err(FrankenError::LockFailed {
                detail: format!(
                    "owner {} does not hold exclusive slot {slot}",
                    self.shm_owner_id
                ),
            });
        }

        slot_state.exclusive_owner = None;
        if slot_state.shared_holders.is_empty() {
            posix_unlock(&*info.file, lock_byte, 1)?;
        } else if !posix_lock(&*info.file, libc::F_RDLCK, lock_byte, 1)? {
            Self::log_lock_conflict(
                slot,
                "unlock-exclusive",
                Self::observed_mode(slot_state),
                read_marks,
            );
            return Err(FrankenError::Busy);
        }

        lock_debug!(
            slot,
            lock_byte,
            requested_mode = "unlock-exclusive",
            observed_mode = Self::observed_mode(slot_state),
            ?read_marks,
            "released shm exclusive lock"
        );
        Ok(())
    }

    fn validate_shm_request(offset: u32, n: u32) -> Result<()> {
        if n == 0 {
            return Err(FrankenError::LockFailed {
                detail: "shm_lock called with n=0".to_string(),
            });
        }
        let Some(end) = offset.checked_add(n) else {
            return Err(FrankenError::LockFailed {
                detail: "shm_lock range overflow".to_string(),
            });
        };
        if end > WAL_TOTAL_LOCKS {
            return Err(FrankenError::LockFailed {
                detail: format!("shm_lock range {offset}..{end} exceeds WAL lock table"),
            });
        }
        Ok(())
    }

    pub fn compat_reader_acquire_wal_read_lock(
        &mut self,
        cx: &Cx,
        reader_slot: u32,
        snapshot_mark: u32,
    ) -> Result<bool> {
        let Some(slot) = wal_read_lock_slot(reader_slot) else {
            return Err(FrankenError::LockFailed {
                detail: format!("invalid WAL reader slot {reader_slot}"),
            });
        };

        let shm_info = self.ensure_shm_info()?;
        let needs_update = {
            let info = shm_info.lock().expect("shm info lock poisoned");
            let slot_idx = usize::try_from(reader_slot).expect("reader slot fits usize");
            info.read_marks[slot_idx] != snapshot_mark
        };

        if !needs_update {
            self.shm_lock(cx, slot, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)?;
            return Ok(false);
        }

        // Legacy protocol: EXCLUSIVE only for aReadMark mutation, then downgrade to SHARED.
        self.shm_lock(cx, slot, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)?;
        {
            let mut info = shm_info.lock().expect("shm info lock poisoned");
            let slot_idx = usize::try_from(reader_slot).expect("reader slot fits usize");
            info.read_marks[slot_idx] = snapshot_mark;
        }
        self.shm_lock(cx, slot, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)?;
        self.shm_lock(cx, slot, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)?;
        Ok(true)
    }

    pub fn compat_writer_hold_wal_write_lock(&mut self, cx: &Cx) -> Result<()> {
        // Hold a SHARED lock on the main db file so legacy SQLite cannot take an
        // EXCLUSIVE lock on close and delete `*-wal`/`*-shm` while our coordinator
        // is alive.
        self.lock(cx, LockLevel::Shared)?;

        // Hold the DMS ("deadman switch") byte in SHARED mode so legacy SQLite
        // openers do not truncate `*-shm` while we hold WAL_WRITE_LOCK.
        if let Err(err) = self.compat_shm_hold_dms_shared(cx) {
            let _ = self.unlock(cx, LockLevel::None);
            return Err(err);
        }

        if let Err(err) = self.shm_lock(
            cx,
            WAL_WRITE_LOCK,
            1,
            SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
        ) {
            let _ = self.compat_shm_release_dms_shared(cx);
            let _ = self.unlock(cx, LockLevel::None);
            return Err(err);
        }

        if let Err(err) = self.compat_writer_init_wal_shm_header_if_needed(cx) {
            let _ = self.shm_lock(
                cx,
                WAL_WRITE_LOCK,
                1,
                SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
            );
            let _ = self.compat_shm_release_dms_shared(cx);
            let _ = self.unlock(cx, LockLevel::None);
            return Err(err);
        }
        Ok(())
    }

    pub fn compat_writer_release_wal_write_lock(&mut self, cx: &Cx) -> Result<()> {
        let mut first_error = self
            .shm_lock(
                cx,
                WAL_WRITE_LOCK,
                1,
                SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
            )
            .err();

        if let Err(err) = self.compat_shm_release_dms_shared(cx) {
            if first_error.is_none() {
                first_error = Some(err);
            }
        }

        if let Err(err) = self.unlock(cx, LockLevel::None) {
            if first_error.is_none() {
                first_error = Some(err);
            }
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn compat_shm_hold_dms_shared(&mut self, cx: &Cx) -> Result<()> {
        checkpoint_or_abort(cx)?;
        let shm_info = self.ensure_shm_info()?;
        let mut info = shm_info.lock().expect("shm info lock poisoned");
        self.acquire_shm_dms_shared(&mut info)
    }

    fn compat_shm_release_dms_shared(&mut self, cx: &Cx) -> Result<()> {
        checkpoint_or_abort(cx)?;
        let shm_info = self.ensure_shm_info()?;
        let mut info = shm_info.lock().expect("shm info lock poisoned");
        self.release_shm_dms_shared(&mut info)
    }

    fn compat_writer_init_wal_shm_header_if_needed(&mut self, cx: &Cx) -> Result<()> {
        checkpoint_or_abort(cx)?;

        // This routine is called while holding WAL_WRITE_LOCK. Its job is to
        // ensure legacy SQLite can start a read transaction without needing to
        // grab WAL_WRITE_LOCK just to initialize `*-shm`.
        let shm_info = self.ensure_shm_info()?;
        let shm_file = {
            let info = shm_info.lock().expect("shm info lock poisoned");
            Arc::clone(&info.file)
        };

        let len = shm_file.metadata().map_err(FrankenError::Io)?.len();
        if len < SQLITE_WALINDEX_PGSZ {
            shm_file
                .set_len(SQLITE_WALINDEX_PGSZ)
                .map_err(FrankenError::Io)?;
        }

        let mut header_buf = [0_u8; SQLITE_WAL_SHM_HEADER_BYTES];
        let read = shm_file
            .read_at(&mut header_buf, 0)
            .map_err(FrankenError::Io)?;
        if read == SQLITE_WAL_SHM_HEADER_BYTES && sqlite_wal_shm_header_is_valid(&header_buf)? {
            return Ok(());
        }

        let wal_path = sqlite_wal_path(&self.path);
        let wal_has_frames = fs::metadata(&wal_path).is_ok_and(|m| m.len() > 0);
        if wal_has_frames {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "cannot initialize shm header while wal file has content: {}",
                    wal_path.display()
                ),
            });
        }

        let mut db_hdr = [0_u8; 100];
        let hdr_read = self
            .file
            .read_at(&mut db_hdr, 0)
            .map_err(FrankenError::Io)?;
        if hdr_read != db_hdr.len() {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "cannot initialize shm header: db header short read (read {hdr_read} bytes)"
                ),
            });
        }
        let page_size = sqlite_page_size_from_db_header(&db_hdr)?;
        let db_len = self.file.metadata().map_err(FrankenError::Io)?.len();
        let n_page_u64 = db_len / u64::from(page_size);
        let n_page = u32::try_from(n_page_u64).unwrap_or(u32::MAX);

        let header = build_empty_sqlite_wal_shm_header(page_size, n_page)?;
        let mut written = 0_usize;
        while written < header.len() {
            let offset = u64::try_from(written).expect("header write offset fits u64");
            let n = shm_file
                .write_at(&header[written..], offset)
                .map_err(FrankenError::Io)?;
            if n == 0 {
                return Err(FrankenError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "unix vfs shm header write_at returned 0",
                )));
            }
            written += n;
        }

        let mut verify = [0_u8; SQLITE_WAL_SHM_HEADER_BYTES];
        let verify_read = shm_file.read_at(&mut verify, 0).map_err(FrankenError::Io)?;
        if verify_read != SQLITE_WAL_SHM_HEADER_BYTES || !sqlite_wal_shm_header_is_valid(&verify)? {
            return Err(FrankenError::WalCorrupt {
                detail: "shm header initialization failed local validation".to_owned(),
            });
        }

        Ok(())
    }

    #[must_use]
    pub fn compat_read_marks(&self) -> Option<[u32; WAL_NREADER_USIZE]> {
        self.shm_info
            .as_ref()
            .map(|info| info.lock().expect("shm info lock poisoned").read_marks())
    }

    fn unlock_with_level(
        lock_level: &mut LockLevel,
        info: &mut InodeInfo,
        file: &File,
        level: LockLevel,
    ) -> Result<()> {
        if *lock_level <= level {
            *lock_level = level;
            return Ok(());
        }

        // Drop EXCLUSIVE first (downgrade SHARED range from write -> read).
        if *lock_level == LockLevel::Exclusive && level < LockLevel::Exclusive {
            debug_assert!(info.n_exclusive > 0, "exclusive count underflow");
            info.n_exclusive = info.n_exclusive.saturating_sub(1);

            if info.n_exclusive == 0 {
                if info.n_shared > 0 {
                    let ok = posix_lock(file, libc::F_RDLCK, SHARED_FIRST, SHARED_SIZE)?;
                    debug_assert!(
                        ok,
                        "downgrading an exclusive lock back to shared should not block"
                    );
                    if !ok {
                        return Err(FrankenError::Busy);
                    }
                } else {
                    // No readers remain in-process: release the shared-range lock entirely.
                    posix_unlock(file, SHARED_FIRST, SHARED_SIZE)?;
                }
            }
        }

        // Drop PENDING (unlock the pending byte).
        if *lock_level >= LockLevel::Pending && level < LockLevel::Pending {
            debug_assert!(info.n_pending > 0, "pending count underflow");
            info.n_pending = info.n_pending.saturating_sub(1);
            if info.n_pending == 0 {
                posix_unlock(file, PENDING_BYTE, 1)?;
            }
        }

        // Drop RESERVED (unlock the reserved byte).
        if *lock_level >= LockLevel::Reserved && level < LockLevel::Reserved {
            debug_assert!(info.n_reserved > 0, "reserved count underflow");
            info.n_reserved = info.n_reserved.saturating_sub(1);
            if info.n_reserved == 0 {
                posix_unlock(file, RESERVED_BYTE, 1)?;
            }
        }

        // Drop SHARED (unlock the shared range).
        if *lock_level >= LockLevel::Shared && level < LockLevel::Shared {
            debug_assert!(info.n_shared > 0, "shared count underflow");
            info.n_shared = info.n_shared.saturating_sub(1);
            if info.n_shared == 0 {
                posix_unlock(file, SHARED_FIRST, SHARED_SIZE)?;
            }
        }

        *lock_level = level;
        Ok(())
    }
}

impl VfsFile for UnixFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        // Downgrade to no lock before closing.
        if self.lock_level != LockLevel::None {
            self.unlock(cx, LockLevel::None)?;
        }
        self.release_shm_owner_state(self.delete_on_close)?;

        // Decrement refcount.
        {
            let mut info = self.inode_info.lock().expect("inode info lock poisoned");
            info.n_ref = info.n_ref.saturating_sub(1);
        }
        global_inode_table().maybe_remove(self.inode_key);

        if self.delete_on_close {
            drop(fs::remove_file(&self.path));
        }

        Ok(())
    }

    fn read(&mut self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        checkpoint_or_abort(cx)?;
        let mut total = 0_usize;
        while total < buf.len() {
            #[allow(clippy::cast_possible_truncation)]
            let off = offset + total as u64;
            let n = self
                .file
                .read_at(&mut buf[total..], off)
                .map_err(FrankenError::Io)?;
            if n == 0 {
                break; // EOF
            }
            total += n;
        }

        // Zero-fill short reads (SQLite requirement).
        if total < buf.len() {
            buf[total..].fill(0);
        }

        Ok(total)
    }

    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        checkpoint_or_abort(cx)?;
        let mut total = 0_usize;
        while total < buf.len() {
            #[allow(clippy::cast_possible_truncation)]
            let off = offset + total as u64;
            let n = self
                .file
                .write_at(&buf[total..], off)
                .map_err(FrankenError::Io)?;
            if n == 0 {
                return Err(FrankenError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "unix vfs write_at returned 0",
                )));
            }
            total += n;
        }
        Ok(())
    }

    fn truncate(&mut self, _cx: &Cx, size: u64) -> Result<()> {
        self.file.set_len(size).map_err(FrankenError::Io)?;
        Ok(())
    }

    fn sync(&mut self, _cx: &Cx, flags: SyncFlags) -> Result<()> {
        if flags.contains(SyncFlags::DATAONLY) {
            self.file.sync_data().map_err(FrankenError::Io)?;
        } else {
            self.file.sync_all().map_err(FrankenError::Io)?;
        }
        Ok(())
    }

    fn file_size(&self, _cx: &Cx) -> Result<u64> {
        let meta = self.file.metadata().map_err(FrankenError::Io)?;
        Ok(meta.len())
    }

    #[allow(clippy::too_many_lines)]
    fn lock(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        // No-op if already at or above the requested level.
        if self.lock_level >= level {
            return Ok(());
        }

        let file = Arc::clone(&self.file);
        let mut info = self.inode_info.lock().expect("inode info lock poisoned");

        let original = self.lock_level;

        if level >= LockLevel::Shared && self.lock_level < LockLevel::Shared {
            // In-process pending/exclusive blocks new readers. (fcntl locks are per-process.)
            if info.n_pending > 0 || info.n_exclusive > 0 {
                return Err(FrankenError::Busy);
            }

            // Check PENDING byte is not write-locked (blocks new readers).
            if !posix_lock(&*file, libc::F_RDLCK, PENDING_BYTE, 1)? {
                return Err(FrankenError::Busy);
            }

            if info.n_shared == 0 {
                debug_assert_eq!(info.n_reserved, 0);
                debug_assert_eq!(info.n_pending, 0);
                debug_assert_eq!(info.n_exclusive, 0);

                // Step 2: Acquire read lock on SHARED range.
                if !posix_lock(&*file, libc::F_RDLCK, SHARED_FIRST, SHARED_SIZE)? {
                    posix_unlock(&*file, PENDING_BYTE, 1)?;
                    return Err(FrankenError::Busy);
                }
            }

            // Release PENDING byte.
            posix_unlock(&*file, PENDING_BYTE, 1)?;

            info.n_shared += 1;
            self.lock_level = LockLevel::Shared;
        }

        if level >= LockLevel::Reserved && self.lock_level < LockLevel::Reserved {
            // RESERVED is exclusive per connection; enforce that within a process.
            if info.n_reserved > 0 {
                Self::unlock_with_level(&mut self.lock_level, &mut info, file.as_ref(), original)?;
                return Err(FrankenError::Busy);
            }
            if info.n_reserved == 0 && !posix_lock(&*file, libc::F_WRLCK, RESERVED_BYTE, 1)? {
                Self::unlock_with_level(&mut self.lock_level, &mut info, file.as_ref(), original)?;
                return Err(FrankenError::Busy);
            }

            info.n_reserved += 1;
            self.lock_level = LockLevel::Reserved;
        }

        if level >= LockLevel::Pending && self.lock_level < LockLevel::Pending {
            // Only one pending/exclusive lock-holder per process.
            if info.n_pending > 0 {
                Self::unlock_with_level(&mut self.lock_level, &mut info, file.as_ref(), original)?;
                return Err(FrankenError::Busy);
            }
            if info.n_pending == 0 && !posix_lock(&*file, libc::F_WRLCK, PENDING_BYTE, 1)? {
                Self::unlock_with_level(&mut self.lock_level, &mut info, file.as_ref(), original)?;
                return Err(FrankenError::Busy);
            }

            info.n_pending += 1;
            self.lock_level = LockLevel::Pending;
        }

        if level >= LockLevel::Exclusive && self.lock_level < LockLevel::Exclusive {
            // Exclusive requires that this handle is the sole shared lock holder in-process.
            if info.n_exclusive > 0 || info.n_shared > 1 {
                Self::unlock_with_level(&mut self.lock_level, &mut info, file.as_ref(), original)?;
                return Err(FrankenError::Busy);
            }
            if info.n_exclusive == 0
                && !posix_lock(&*file, libc::F_WRLCK, SHARED_FIRST, SHARED_SIZE)?
            {
                Self::unlock_with_level(&mut self.lock_level, &mut info, file.as_ref(), original)?;
                return Err(FrankenError::Busy);
            }

            info.n_exclusive += 1;
            self.lock_level = LockLevel::Exclusive;
        }

        drop(info);
        Ok(())
    }

    fn unlock(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        let file = Arc::clone(&self.file);
        let mut info = self.inode_info.lock().expect("inode info lock poisoned");
        Self::unlock_with_level(&mut self.lock_level, &mut info, file.as_ref(), level)
    }

    fn check_reserved_lock(&self, _cx: &Cx) -> Result<bool> {
        let file = Arc::clone(&self.file);
        let info = self.inode_info.lock().expect("inode info lock poisoned");

        // If *this handle* holds reserved or higher, report false (it's us).
        if self.lock_level >= LockLevel::Reserved {
            return Ok(false);
        }

        // If another handle in this process holds reserved, that's "another connection".
        if info.n_reserved > 0 {
            return Ok(true);
        }
        drop(info);

        // Probe the RESERVED byte with a non-blocking F_WRLCK.
        let can_lock = posix_lock(&*file, libc::F_WRLCK, RESERVED_BYTE, 1)?;
        if can_lock {
            // We got it, meaning nobody else has it. Release immediately.
            posix_unlock(&*file, RESERVED_BYTE, 1)?;
            Ok(false)
        } else {
            // Another process holds reserved.
            Ok(true)
        }
    }

    fn shm_map(
        &mut self,
        _cx: &Cx,
        region: u32,
        size: u32,
        extend: bool,
    ) -> Result<crate::shm::ShmRegion> {
        if size == 0 {
            return Err(FrankenError::LockFailed {
                detail: "shm_map size must be > 0".to_string(),
            });
        }

        let shm_info = self.ensure_shm_info()?;
        let mut info = shm_info.lock().expect("shm info lock poisoned");
        if let Some(existing) = info.regions.get(&region) {
            return Ok(existing.clone());
        }
        if !extend {
            return Err(FrankenError::CannotOpen {
                path: self.shm_path.clone(),
            });
        }

        let map_size = usize::try_from(size).map_err(|_| FrankenError::LockFailed {
            detail: format!("shm_map size too large: {size}"),
        })?;
        let new_region = ShmRegion::new(map_size);
        let region_count = u64::from(region) + 1;
        let target_len =
            region_count
                .checked_mul(u64::from(size))
                .ok_or_else(|| FrankenError::LockFailed {
                    detail: "shm_map file length overflow".to_string(),
                })?;
        info.file.set_len(target_len).map_err(FrankenError::Io)?;
        info.regions.insert(region, new_region.clone());
        drop(info);
        Ok(new_region)
    }

    fn shm_lock(&mut self, _cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        Self::validate_shm_request(offset, n)?;
        let lock_requested = flags & SQLITE_SHM_LOCK != 0;
        let unlock_requested = flags & SQLITE_SHM_UNLOCK != 0;
        if lock_requested == unlock_requested {
            error!(
                offset,
                n, flags, "invalid shm_lock request: exactly one of LOCK/UNLOCK is required"
            );
            return Err(FrankenError::LockFailed {
                detail: "invalid shm_lock flags (must set exactly one of LOCK/UNLOCK)".to_string(),
            });
        }

        let shared_mode = flags & SQLITE_SHM_SHARED != 0;
        let exclusive_mode = flags & SQLITE_SHM_EXCLUSIVE != 0;
        if shared_mode == exclusive_mode {
            error!(
                offset,
                n, flags, "invalid shm_lock request: exactly one of SHARED/EXCLUSIVE is required"
            );
            return Err(FrankenError::LockFailed {
                detail: "invalid shm_lock flags (must set exactly one of SHARED/EXCLUSIVE)"
                    .to_string(),
            });
        }

        let shm_info = self.ensure_shm_info()?;
        let mut info = shm_info.lock().expect("shm info lock poisoned");

        if lock_requested {
            let mut acquired = Vec::new();
            for slot in offset..offset + n {
                let result = if exclusive_mode {
                    self.acquire_shm_exclusive_slot(&mut info, slot)
                } else {
                    self.acquire_shm_shared_slot(&mut info, slot)
                };
                match result {
                    Ok(()) => acquired.push(slot),
                    Err(err) => {
                        for acquired_slot in acquired.into_iter().rev() {
                            if exclusive_mode {
                                let _ = self.release_shm_exclusive_slot(&mut info, acquired_slot);
                            } else {
                                let _ = self.release_shm_shared_slot(&mut info, acquired_slot);
                            }
                        }
                        return Err(err);
                    }
                }
            }
            return Ok(());
        }

        for slot in offset..offset + n {
            if exclusive_mode {
                self.release_shm_exclusive_slot(&mut info, slot)?;
            } else {
                self.release_shm_shared_slot(&mut info, slot)?;
            }
        }
        Ok(())
    }

    fn shm_barrier(&self) {}

    fn shm_unmap(&mut self, _cx: &Cx, delete: bool) -> Result<()> {
        self.release_shm_owner_state(delete)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Output};

    fn debug_dump_sqlite_wal_files(coordinator: &mut UnixFile) {
        use std::fmt::Write as _;

        if std::env::var_os("FSQLITE_DEBUG_SQLITE_WAL_INTEROP").is_none() {
            return;
        }

        let db_path = &coordinator.path;
        let shm_path = &coordinator.shm_path;
        let wal_path = sqlite_wal_path(db_path);

        let db_len = fs::metadata(db_path).map_or(0, |m| m.len());
        let shm_len = fs::metadata(shm_path).map_or(0, |m| m.len());
        let wal_len = fs::metadata(&wal_path).map_or(0, |m| m.len());

        eprintln!(
            "[debug] sqlite interop paths:\n  db={}\n  shm={} (len={shm_len})\n  wal={} (len={wal_len})\n  db_len={db_len}",
            db_path.display(),
            shm_path.display(),
            wal_path.display(),
        );

        if let Ok(shm_info) = coordinator.ensure_shm_info() {
            let shm_file = {
                let info = shm_info.lock().expect("shm info lock poisoned");
                Arc::clone(&info.file)
            };
            let mut header = [0_u8; SQLITE_WAL_SHM_HEADER_BYTES];
            let n = shm_file.read_at(&mut header, 0).unwrap_or(0);
            let valid = sqlite_wal_shm_header_is_valid(&header).unwrap_or(false);
            eprintln!("[debug] shm header read_at(0) -> {n} bytes, valid={valid}");

            let mut line = String::new();
            for (i, b) in header.iter().enumerate() {
                if i % 16 == 0 {
                    if !line.is_empty() {
                        eprintln!("{line}");
                        line.clear();
                    }
                    let _ = write!(line, "[debug] {i:04x}: ");
                }
                let _ = write!(line, "{b:02x} ");
            }
            if !line.is_empty() {
                eprintln!("{line}");
            }
        } else {
            eprintln!("[debug] shm file open failed");
        }
    }

    fn make_temp_path(name: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(name);
        (dir, path)
    }

    fn open_flags_create() -> VfsOpenFlags {
        VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE
    }

    fn sqlite3_available() -> bool {
        Command::new("sqlite3").arg("--version").output().is_ok()
    }

    fn sqlite3_exec(db_path: &Path, sql: &str) -> Output {
        Command::new("sqlite3")
            .arg(db_path)
            .arg(sql)
            .output()
            .expect("sqlite3 command should execute")
    }

    fn setup_sqlite_wal_db(path: &Path) {
        let setup = sqlite3_exec(
            path,
            "PRAGMA journal_mode=WAL; \
             DROP TABLE IF EXISTS t; \
             CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); \
             INSERT INTO t(v) VALUES('alpha'),('beta');",
        );
        assert!(
            setup.status.success(),
            "sqlite3 setup failed: {}",
            String::from_utf8_lossy(&setup.stderr)
        );
    }

    // -- Basic I/O --

    #[test]
    fn test_unix_vfs_create_write_close_reopen_read() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("rw_test.db");

        // Create and write.
        let (mut file, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        file.write(&cx, b"hello unix vfs", 0).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 14);
        file.close(&cx).unwrap();

        // Reopen and read.
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(&path), flags).unwrap();
        let mut buf = [0u8; 14];
        let n = file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 14);
        assert_eq!(&buf, b"hello unix vfs");
        file.close(&cx).unwrap();
    }

    #[test]
    fn test_unix_vfs_read_past_end_zeroes() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("short_read.db");

        let (mut file, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        file.write(&cx, b"hi", 0).unwrap();

        let mut buf = [0xFF_u8; 10];
        let n = file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"hi");
        assert!(
            buf[2..].iter().all(|&b| b == 0),
            "short read must zero-fill"
        );
        file.close(&cx).unwrap();
    }

    #[test]
    fn test_unix_vfs_truncate() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("truncate.db");

        let (mut file, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        file.write(&cx, b"hello world!!", 0).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 13);

        file.truncate(&cx, 5).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 5);

        let mut buf = [0u8; 5];
        file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");

        file.close(&cx).unwrap();
    }

    #[test]
    fn test_unix_vfs_delete_nonexistent() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("nonexistent_delete_test.db");
        let result = vfs.delete(&cx, &path, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_unix_vfs_delete_file() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("delete_me.db");

        let (mut file, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        file.write(&cx, b"data", 0).unwrap();
        file.close(&cx).unwrap();

        assert!(vfs.access(&cx, &path, AccessFlags::EXISTS).unwrap());
        vfs.delete(&cx, &path, false).unwrap();
        assert!(!vfs.access(&cx, &path, AccessFlags::EXISTS).unwrap());
    }

    #[test]
    fn test_unix_vfs_open_nonexistent_without_create_fails() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("definitely_not_here.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;
        let result = vfs.open(&cx, Some(&path), flags);
        assert!(result.is_err());
    }

    #[test]
    fn test_unix_vfs_full_pathname() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();

        let abs = vfs.full_pathname(&cx, Path::new("/tmp/test.db")).unwrap();
        assert_eq!(abs, Path::new("/tmp/test.db"));

        let rel = vfs.full_pathname(&cx, Path::new("test.db")).unwrap();
        assert!(rel.is_absolute());
    }

    #[test]
    fn test_unix_vfs_name() {
        let vfs = UnixVfs::new();
        assert_eq!(vfs.name(), "unix");
    }

    // -- Locking --

    #[test]
    fn test_unix_vfs_lock_escalation() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("lock_test.db");

        let (mut file, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        file.write(&cx, b"lock test data", 0).unwrap();

        // Escalate: None -> Shared -> Reserved -> Exclusive.
        file.lock(&cx, LockLevel::Shared).unwrap();
        assert_eq!(file.lock_level, LockLevel::Shared);

        file.lock(&cx, LockLevel::Reserved).unwrap();
        assert_eq!(file.lock_level, LockLevel::Reserved);

        file.lock(&cx, LockLevel::Exclusive).unwrap();
        assert_eq!(file.lock_level, LockLevel::Exclusive);

        // Downgrade: Exclusive -> Shared -> None.
        file.unlock(&cx, LockLevel::Shared).unwrap();
        assert_eq!(file.lock_level, LockLevel::Shared);

        file.unlock(&cx, LockLevel::None).unwrap();
        assert_eq!(file.lock_level, LockLevel::None);

        file.close(&cx).unwrap();
    }

    #[test]
    fn test_unix_vfs_lock_idempotent() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("idem_lock.db");

        let (mut file, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();

        // Requesting the same lock level is a no-op.
        file.lock(&cx, LockLevel::Shared).unwrap();
        file.lock(&cx, LockLevel::Shared).unwrap();
        assert_eq!(file.lock_level, LockLevel::Shared);

        file.unlock(&cx, LockLevel::None).unwrap();
        file.close(&cx).unwrap();
    }

    #[test]
    fn test_unix_vfs_check_reserved_lock() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("check_reserved.db");

        let (mut file, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        file.write(&cx, b"data", 0).unwrap();

        // No reserved lock held by others.
        assert!(!file.check_reserved_lock(&cx).unwrap());

        // If we hold reserved, check_reserved_lock returns false (it's us).
        file.lock(&cx, LockLevel::Shared).unwrap();
        file.lock(&cx, LockLevel::Reserved).unwrap();
        assert!(!file.check_reserved_lock(&cx).unwrap());

        file.unlock(&cx, LockLevel::None).unwrap();
        file.close(&cx).unwrap();
    }

    #[test]
    fn test_unix_vfs_sync() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("sync_test.db");

        let (mut file, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        file.write(&cx, b"sync me", 0).unwrap();

        file.sync(&cx, SyncFlags::NORMAL).unwrap();
        file.sync(&cx, SyncFlags::FULL).unwrap();
        file.sync(&cx, SyncFlags::DATAONLY).unwrap();

        file.close(&cx).unwrap();
    }

    #[test]
    fn test_unix_vfs_delete_on_close() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("auto_delete.db");

        let flags = VfsOpenFlags::MAIN_DB
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::DELETEONCLOSE;
        let (mut file, _) = vfs.open(&cx, Some(&path), flags).unwrap();
        file.write(&cx, b"temp", 0).unwrap();
        assert!(path.exists());

        file.close(&cx).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn test_unix_vfs_write_at_offset() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("offset_write.db");

        let (mut file, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        file.write(&cx, b"AAAA", 0).unwrap();
        file.write(&cx, b"BB", 1).unwrap();

        let mut buf = [0u8; 4];
        file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"ABBA");

        file.close(&cx).unwrap();
    }

    #[test]
    fn test_unix_vfs_page_write_read() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("pages.db");

        let (mut file, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();

        let page1 = vec![0xAA_u8; 4096];
        let page2 = vec![0xBB_u8; 4096];
        file.write(&cx, &page1, 0).unwrap();
        file.write(&cx, &page2, 4096).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 8192);

        let mut buf = vec![0u8; 4096];
        file.read(&cx, &mut buf, 0).unwrap();
        assert!(buf.iter().all(|&b| b == 0xAA));

        file.read(&cx, &mut buf, 4096).unwrap();
        assert!(buf.iter().all(|&b| b == 0xBB));

        file.close(&cx).unwrap();
    }

    #[test]
    fn test_unix_vfs_randomness() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let mut buf1 = [0u8; 16];
        let mut buf2 = [0u8; 16];
        vfs.randomness(&cx, &mut buf1);
        vfs.randomness(&cx, &mut buf2);
        assert_ne!(buf1, buf2, "randomness should produce different outputs");
    }

    #[test]
    fn test_unix_vfs_lock_is_per_connection_in_process() {
        // fcntl locks are per-process. SQLite semantics are per-connection.
        // Verify we enforce per-connection exclusivity within a process.
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("multi_handle_lock.db");

        let (mut f1, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        let (mut f2, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();

        // Connection 1 takes reserved.
        f1.lock(&cx, LockLevel::Shared).unwrap();
        f1.lock(&cx, LockLevel::Reserved).unwrap();

        // Connection 2 should observe a reserved lock held by "another" connection.
        assert!(
            f2.check_reserved_lock(&cx).unwrap(),
            "second handle should see reserved lock held by first handle"
        );

        // And it should not be able to acquire RESERVED itself.
        f2.lock(&cx, LockLevel::Shared).unwrap();
        assert!(f2.lock(&cx, LockLevel::Reserved).is_err());

        f1.unlock(&cx, LockLevel::None).unwrap();
        assert!(!f2.check_reserved_lock(&cx).unwrap());
    }

    #[test]
    fn test_compat_reader_acquires_wal_read_lock() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("compat_reader_join.db");
        let (mut reader1, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        let (mut reader2, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();

        let updated = reader1
            .compat_reader_acquire_wal_read_lock(&cx, 0, 41)
            .unwrap();
        assert!(updated, "first reader must seed aReadMark[0]");

        let joined = reader2
            .compat_reader_acquire_wal_read_lock(&cx, 0, 41)
            .unwrap();
        assert!(
            !joined,
            "second reader should join existing aReadMark[0] with SHARED lock"
        );

        let read_marks = reader1.compat_read_marks().expect("shm state should exist");
        assert_eq!(read_marks[0], 41);

        let slot = wal_read_lock_slot(0).expect("reader slot 0 should exist");
        reader2
            .shm_lock(&cx, slot, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .unwrap();
        reader1
            .shm_lock(&cx, slot, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .unwrap();
    }

    #[test]
    fn test_compat_reader_exclusive_for_update() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("compat_reader_update.db");
        let (mut reader1, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        let (mut reader2, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();

        let first_update = reader1
            .compat_reader_acquire_wal_read_lock(&cx, 0, 7)
            .unwrap();
        assert!(first_update);

        let slot = wal_read_lock_slot(0).expect("reader slot 0 should exist");
        reader1
            .shm_lock(&cx, slot, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .unwrap();

        let second_update = reader1
            .compat_reader_acquire_wal_read_lock(&cx, 0, 9)
            .unwrap();
        assert!(
            second_update,
            "reader must take EXCLUSIVE briefly to update aReadMark then downgrade"
        );
        assert_eq!(reader1.compat_read_marks().expect("shm state exists")[0], 9);

        let joined = reader2
            .compat_reader_acquire_wal_read_lock(&cx, 0, 9)
            .unwrap();
        assert!(!joined, "reader2 should join updated aReadMark");

        reader2
            .shm_lock(&cx, slot, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .unwrap();
        reader1
            .shm_lock(&cx, slot, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .unwrap();
    }

    #[test]
    fn test_compat_writer_holds_wal_write_lock() {
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("compat_writer_lock.db");
        let (mut coordinator, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        let (mut contender, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();

        coordinator.compat_writer_hold_wal_write_lock(&cx).unwrap();
        let contender_err = contender
            .compat_writer_hold_wal_write_lock(&cx)
            .unwrap_err();
        assert!(
            matches!(contender_err, FrankenError::Busy),
            "contender should observe SQLITE_BUSY while coordinator holds WAL_WRITE_LOCK"
        );

        coordinator
            .compat_writer_release_wal_write_lock(&cx)
            .unwrap();
        contender.compat_writer_hold_wal_write_lock(&cx).unwrap();
        contender.compat_writer_release_wal_write_lock(&cx).unwrap();
    }

    #[test]
    fn test_legacy_sqlite_reader_coexists() {
        if !sqlite3_available() {
            return;
        }

        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("legacy_reader_coexists.db");
        setup_sqlite_wal_db(&path);

        let (mut coordinator, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        coordinator.compat_writer_hold_wal_write_lock(&cx).unwrap();
        debug_dump_sqlite_wal_files(&mut coordinator);

        let reader_output = sqlite3_exec(&path, "PRAGMA busy_timeout=0; SELECT COUNT(*) FROM t;");
        assert!(
            reader_output.status.success(),
            "legacy sqlite reader should coexist while coordinator holds WAL_WRITE_LOCK; stderr={}",
            String::from_utf8_lossy(&reader_output.stderr)
        );
        let count_text = String::from_utf8_lossy(&reader_output.stdout);
        assert!(
            count_text.contains('2'),
            "expected reader to observe table rows; stdout={count_text}"
        );

        coordinator
            .compat_writer_release_wal_write_lock(&cx)
            .unwrap();
    }

    #[test]
    fn test_legacy_sqlite_writer_gets_busy() {
        if !sqlite3_available() {
            return;
        }

        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("legacy_writer_busy.db");
        setup_sqlite_wal_db(&path);

        let (mut coordinator, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        coordinator.compat_writer_hold_wal_write_lock(&cx).unwrap();
        debug_dump_sqlite_wal_files(&mut coordinator);

        let writer_output = sqlite3_exec(
            &path,
            "PRAGMA busy_timeout=0; \
             BEGIN IMMEDIATE; INSERT INTO t(v) VALUES('blocked'); COMMIT;",
        );
        assert!(
            !writer_output.status.success(),
            "legacy writer must fail with SQLITE_BUSY while coordinator holds WAL_WRITE_LOCK"
        );
        let busy_text = format!(
            "{}\n{}",
            String::from_utf8_lossy(&writer_output.stdout),
            String::from_utf8_lossy(&writer_output.stderr)
        )
        .to_ascii_lowercase();
        assert!(
            busy_text.contains("database is locked") || busy_text.contains("busy"),
            "expected sqlite busy/locked message, got: {busy_text}"
        );

        coordinator
            .compat_writer_release_wal_write_lock(&cx)
            .unwrap();
    }

    #[test]
    fn test_e2e_hybrid_shm_interop_with_c_sqlite() {
        if !sqlite3_available() {
            return;
        }

        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let (_dir, path) = make_temp_path("hybrid_shm_interop_e2e.db");
        setup_sqlite_wal_db(&path);

        let (mut coordinator, _) = vfs.open(&cx, Some(&path), open_flags_create()).unwrap();
        coordinator.compat_writer_hold_wal_write_lock(&cx).unwrap();
        debug_dump_sqlite_wal_files(&mut coordinator);

        // Reader interop while coordinator holds WAL_WRITE_LOCK.
        let read_output = sqlite3_exec(&path, "PRAGMA busy_timeout=0; SELECT SUM(id) FROM t;");
        assert!(
            read_output.status.success(),
            "reader should succeed during coordinator lifetime; stderr={}",
            String::from_utf8_lossy(&read_output.stderr)
        );
        let read_text = String::from_utf8_lossy(&read_output.stdout);
        assert!(
            read_text.contains('3'),
            "expected deterministic SUM(id)=3 from initial rows; stdout={read_text}"
        );

        // Writer must observe SQLITE_BUSY while coordinator lock is held.
        let blocked_write = sqlite3_exec(
            &path,
            "PRAGMA busy_timeout=0; \
             BEGIN IMMEDIATE; INSERT INTO t(v) VALUES('blocked'); COMMIT;",
        );
        assert!(
            !blocked_write.status.success(),
            "legacy writer must be excluded while coordinator WAL_WRITE_LOCK is held"
        );

        coordinator
            .compat_writer_release_wal_write_lock(&cx)
            .unwrap();

        // After release, legacy writer should proceed.
        let allowed_write = sqlite3_exec(
            &path,
            "PRAGMA busy_timeout=0; \
             BEGIN IMMEDIATE; INSERT INTO t(v) VALUES('allowed'); COMMIT;",
        );
        assert!(
            allowed_write.status.success(),
            "legacy writer should proceed after coordinator releases WAL_WRITE_LOCK; stderr={}",
            String::from_utf8_lossy(&allowed_write.stderr)
        );
    }
}
