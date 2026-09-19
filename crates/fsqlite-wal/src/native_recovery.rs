//! Caller-runtime WAL-FEC recovery for native applications.
//!
//! Default export captures main/WAL under the native recovery fence, then
//! releases source locks before decoding. Neither mode opens an SQL Connection
//! on damaged input. Only `repair_wal` publishes into the source.
//! Source directories must obey the VFS cooperative-namespace contract. Native
//! admission may create lock/SHM companions; database, WAL and FEC data are not
//! rewritten by export. Explicit repair retains the same recovery owner
//! through backup, physical repair and shared-index publication. Neither mode
//! deletes files or overwrites a destination/backup.

#[cfg(unix)]
#[path = "bin/native/repair.rs"]
mod repair;

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use asupersync::runtime::spawn_blocking;
#[cfg(test)]
use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::{FileIdentity, Vfs, VfsFile, host_fs};
use crate::wal_fec::replay::{WalFecReplayLimits, recover_wal_fec_image_with_certificates};

#[cfg(unix)]
type NativeVfs = fsqlite_vfs::unix::UnixVfs;
#[cfg(windows)]
type NativeVfs = fsqlite_vfs::windows::WindowsVfs;
type NativeFile = <NativeVfs as Vfs>::File;

const IO_CHUNK: usize = 64 * 1024;
const DATABASE_HEADER_BYTES: usize = 100;

/// Bounded native recovery request. The destination is a new database for
/// export, or a mandatory new original-WAL backup for in-place repair.
#[derive(Debug, Clone)]
pub struct Options {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub max_database_bytes: usize,
    pub replay: WalFecReplayLimits,
}

impl Options {
    #[must_use]
    pub fn new(source: impl Into<PathBuf>, destination: impl Into<PathBuf>) -> Self {
        Self {
            source: source.into(), destination: destination.into(),
            max_database_bytes: 256 * 1024 * 1024,
            replay: WalFecReplayLimits::default(),
        }
    }
}

/// Export a completely verified main/WAL/FEC snapshot into a new file.
/// The caller supplies an attached native context and its blocking pool;
/// this library never constructs an executor. Source data is not changed.
/// Namespace admission may create lock/SHM companions. On failure a created
/// destination is retained, never silently removed or certified complete.
pub async fn export_database(cx: &Cx, options: &Options) -> Result<ExportReport> {
    preflight(cx, options)?;
    recover_to_new_database(&NativeVfs::new(), cx, options).await
}

/// Repair an existing Unix WAL and rebuild its derived shared index.
/// A synchronized, verified NEW backup is mandatory before source writes.
/// Dropping the future does not release recovery fences while a blocking
/// task settles writes. A returned error may follow durable repair or an
/// indeterminate rollback; preserve the backup and reconcile before reuse.
#[cfg(unix)]
pub async fn repair_wal(cx: &Cx, options: &Options) -> Result<ExportReport> {
    preflight(cx, options)?;
    repair::run(&NativeVfs::new(), cx, options).await
}

fn preflight(cx: &Cx, options: &Options) -> Result<()> {
    checkpoint(cx)?;
    if asupersync::Cx::current().is_none() || cx.attached_native_cx().is_none() {
        return Err(FrankenError::BackgroundWorkerFailed(
            "native recovery requires the caller's active runtime and attached context".to_owned(),
        ));
    }
    if options.source.as_os_str().is_empty() || options.destination.as_os_str().is_empty() {
        return Err(FrankenError::CannotOpen { path: PathBuf::new() });
    }
    Ok(())
}

#[cfg(test)]
fn request_context() -> Result<Cx> {
    let native = asupersync::Cx::current().ok_or_else(|| {
        FrankenError::BackgroundWorkerFailed("recovery requires a caller runtime".to_owned())
    })?;
    let cx = Cx::new();
    cx.set_native_cx(native);
    Ok(cx)
}

fn checkpoint(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)
}

fn companion(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn require_regular_file(path: &Path) -> Result<()> {
    if !host_fs::metadata(path)?.is_file() {
        return Err(FrankenError::CannotOpen { path: path.to_owned() });
    }
    Ok(())
}

fn require_wal_mode(database: &[u8]) -> Result<()> {
    // A checksum-valid leftover WAL is not authority after switching to rollback mode.
    if database.len() < DATABASE_HEADER_BYTES || !database.starts_with(b"SQLite format 3\0")
        || database[18] != 2 || database[19] != 2
    {
        return Err(FrankenError::WalCorrupt {
            detail: "source is not in WAL mode; refusing a potentially stale WAL companion".to_owned(),
        });
    }
    Ok(())
}

/// Native descriptors own cleanup even when an async read is dropped. Never
/// close raw std descriptors for main: POSIX close-any-fd releases peer locks.
struct SourceFile<F: VfsFile> {
    file: F,
    cleanup_cx: Cx,
    maintenance: bool,
    closed: bool,
}

impl<F: VfsFile> SourceFile<F> {
    fn new(file: F, cx: &Cx) -> Self {
        Self { file, cleanup_cx: cx.create_child(), maintenance: false, closed: false }
    }

    fn acquire_recovery(&mut self, cx: &Cx) -> Result<()> {
        self.maintenance = true; // Arm before the first acquisition side effect.
        self.file.lock_external_wal_recovery(cx)?;
        if self.file.locking_downgraded_to_whole_file_flock() {
            return Err(FrankenError::Unsupported);
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.closed { return Ok(()); }
        let _mask = self.cleanup_cx.masked();
        if self.maintenance {
            self.file.restore_external_maintenance_attempt(&self.cleanup_cx)?;
            self.maintenance = false;
        }
        self.file.close(&self.cleanup_cx)?;
        self.closed = true;
        Ok(())
    }
}

impl<F: VfsFile> Drop for SourceFile<F> {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            // Native backend Drop retains/retries its own exact obligations.
            eprintln!("native recovery descriptor cleanup requires retry: {error}");
        }
    }
}

struct Snapshot {
    database: Vec<u8>,
    wal: Vec<u8>,
    sidecar: Vec<u8>,
    certificates: Vec<u8>,
}

/// Transferable owner, not just bytes. Field order closes WAL before main.
struct CapturedSource {
    snapshot: Snapshot,
    wal: SourceFile<NativeFile>,
    main: SourceFile<NativeFile>,
    #[cfg(unix)]
    main_identity: FileIdentity,
    #[cfg(unix)]
    wal_identity: FileIdentity,
}

impl CapturedSource {
    fn finish(&mut self) -> Result<()> {
        self.wal.finish()?;
        self.main.finish()
    }
}

async fn read_vfs_snapshot<F: VfsFile>(file: &F, cx: &Cx, limit: usize) -> Result<Vec<u8>> {
    checkpoint(cx)?;
    let size = file.file_size(cx)?;
    let len = usize::try_from(size).ok().filter(|len| *len <= limit).ok_or(FrankenError::TooBig)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(len).map_err(|_| FrankenError::OutOfMemory)?;
    bytes.resize(len, 0);
    let mut offset = 0;
    while offset < len {
        checkpoint(cx)?;
        let end = len.min(offset.saturating_add(IO_CHUNK));
        let count = file.read(cx, &mut bytes[offset..end], u64::try_from(offset)
            .map_err(|_| FrankenError::TooBig)?).await?;
        if count == 0 || count > end - offset {
            return Err(FrankenError::ShortRead { expected: len, actual: offset });
        }
        offset += count;
    }
    if file.file_size(cx)? != size { return Err(FrankenError::BusyRecovery); }
    Ok(bytes)
}

fn read_exact_snapshot(reader: &mut impl Read, cx: &Cx, len: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(len).map_err(|_| FrankenError::OutOfMemory)?;
    bytes.resize(len, 0);
    let mut offset = 0;
    while offset < len {
        checkpoint(cx)?;
        let end = len.min(offset.saturating_add(IO_CHUNK));
        match reader.read(&mut bytes[offset..end]) {
            Ok(0) => return Err(FrankenError::ShortRead { expected: len, actual: offset }),
            Ok(count) => offset += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    loop {
        checkpoint(cx)?;
        match reader.read(&mut [0_u8; 1]) {
            Ok(0) => return Ok(bytes),
            Ok(_) => return Err(FrankenError::BusyRecovery),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
}

fn read_sidecar(path: &Path, cx: &Cx, limit: usize) -> Result<Vec<u8>> {
    let lock_path = companion(path, ".lock");
    // Stable companion shared with FEC append/migration; never unlink it.
    let guard = match host_fs::open_existing_regular_file_no_follow(&lock_path) {
        Ok(file) => file,
        Err(FrankenError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            match host_fs::reserve_new_file(&lock_path) {
                Ok(file) => file,
                Err(FrankenError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {
                    host_fs::open_existing_regular_file_no_follow(&lock_path)?
                }
                Err(error) => return Err(error),
            }
        }
        Err(error) => return Err(error),
    };
    guard.try_lock_shared().map_err(|error| {
        let error = io::Error::from(error);
        if error.kind() == io::ErrorKind::WouldBlock { FrankenError::Busy }
        else { FrankenError::Io(error) }
    })?;
    let mut file = match host_fs::open_existing_regular_file_no_follow(path) {
        Ok(file) => file,
        Err(FrankenError::Io(error)) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let len = usize::try_from(file.metadata()?.len()).ok()
        .filter(|len| *len <= limit).ok_or(FrankenError::TooBig)?;
    let bytes = read_exact_snapshot(&mut file, cx, len)?;
    drop(file);
    drop(guard);
    Ok(bytes)
}

/// Capture certificates through VFS while the main recovery fence excludes writers.
async fn capture_certificates(
    vfs: &NativeVfs, cx: &Cx, path: &Path, limit: usize,
    main_identity: FileIdentity, wal_identity: FileIdentity,
) -> Result<Vec<u8>> {
    if !vfs.path_entry_exists(cx, path)? { return Ok(Vec::new()); }
    require_regular_file(path)?;
    let (file, _) = vfs.open(cx, Some(path), VfsOpenFlags::READONLY)?;
    let mut certificate = SourceFile::new(file, cx);
    let identity = certificate.file.file_identity()?.ok_or(FrankenError::Unsupported)?;
    if identity == main_identity || identity == wal_identity { return Err(FrankenError::BusyRecovery); }
    let size = certificate.file.file_size(cx)?;
    if usize::try_from(size).ok().is_none_or(|size| size > limit) {
        certificate.finish()?;
        eprintln!("Optional commit certificates exceed the capture limit; not used");
        return Ok(Vec::new());
    }
    let bytes = read_vfs_snapshot(&certificate.file, cx, limit).await?;
    let (probe, _) = vfs.open_with_expected_identity(cx, path, VfsOpenFlags::READONLY, identity)?;
    SourceFile::new(probe, cx).finish()?;
    certificate.finish()?;
    Ok(bytes)
}

async fn capture(vfs: &NativeVfs, cx: &Cx, options: &Options) -> Result<Snapshot> {
    let mut captured = capture_held(vfs, cx, options).await?;
    captured.finish()?;
    Ok(captured.snapshot)
}

async fn capture_held(vfs: &NativeVfs, cx: &Cx, options: &Options) -> Result<CapturedSource> {
    require_regular_file(&options.source)?;
    let (file, flags) = vfs.open(cx, Some(&options.source), VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB)?;
    let mut main = SourceFile::new(file, cx);
    if !flags.contains(VfsOpenFlags::READWRITE) { return Err(FrankenError::ReadOnly); }
    main.acquire_recovery(cx)?;
    let main_identity = main.file.file_identity()?.ok_or(FrankenError::Unsupported)?;
    let database = read_vfs_snapshot(&main.file, cx, options.max_database_bytes).await?;
    require_wal_mode(&database)?;
    let wal_path = companion(&options.source, "-wal");
    require_regular_file(&wal_path)?;
    let (file, _) = vfs.open(cx, Some(&wal_path), VfsOpenFlags::READONLY | VfsOpenFlags::WAL)?;
    let wal = SourceFile::new(file, cx);
    let wal_identity = wal.file.file_identity()?.ok_or(FrankenError::Unsupported)?;
    if main_identity == wal_identity { return Err(FrankenError::BusyRecovery); }
    let wal_bytes = read_vfs_snapshot(&wal.file, cx, options.replay.max_wal_bytes).await?;
    let sidecar_path = companion(&options.source, "-wal-fec");
    let sidecar_cx = cx.create_child_for_spawn();
    let sidecar_limit = options.replay.max_sidecar_bytes;
    let sidecar = spawn_blocking(move || read_sidecar(&sidecar_path, &sidecar_cx, sidecar_limit)).await?;
    let certificates = capture_certificates(vfs, cx, &companion(&options.source, "-wal-cert"),
        options.replay.max_certificate_bytes, main_identity, wal_identity).await?;
    for (path, identity, flags) in [
        (&options.source, main_identity, VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB),
        (&wal_path, wal_identity, VfsOpenFlags::READONLY | VfsOpenFlags::WAL),
    ] {
        let (file, _) = vfs.open_with_expected_identity(cx, path, flags, identity)?;
        SourceFile::new(file, cx).finish()?;
    }
    Ok(CapturedSource {
        snapshot: Snapshot { database, wal: wal_bytes, sidecar, certificates }, wal, main,
        #[cfg(unix)] main_identity,
        #[cfg(unix)] wal_identity,
    })
}

fn refuse_destination_artifacts(vfs: &NativeVfs, cx: &Cx, path: &Path) -> Result<()> {
    for suffix in ["-journal", "-wal", "-shm", "-wal-fec", "-wal-cert", ".fsqlite-shm"] {
        let artifact = companion(path, suffix);
        if vfs.path_entry_exists(cx, &artifact)? { return Err(FrankenError::CannotOpen { path: artifact }); }
    }
    Ok(())
}

/// Make the body durable behind an invalid header, then publish the real header.
fn write_image<W: Write + Seek>(
    file: &mut W, cx: &Cx, image: &[u8], mut sync: impl FnMut(&mut W) -> io::Result<()>,
) -> Result<()> {
    if image.len() < DATABASE_HEADER_BYTES { return Err(FrankenError::TooBig); }
    checkpoint(cx)?;
    file.write_all(&[0_u8; DATABASE_HEADER_BYTES])?;
    for chunk in image[DATABASE_HEADER_BYTES..].chunks(IO_CHUNK) {
        checkpoint(cx)?;
        file.write_all(chunk)?;
    }
    sync(file)?;
    checkpoint(cx)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&image[..DATABASE_HEADER_BYTES])?;
    sync(file)?;
    Ok(())
}

fn verify_image(file: &mut (impl Read + Seek), cx: &Cx, image: &[u8]) -> Result<blake3::Hash> {
    file.seek(SeekFrom::Start(0))?;
    let mut buffer = vec![0_u8; IO_CHUNK.min(image.len())];
    let mut digest = blake3::Hasher::new();
    for expected in image.chunks(IO_CHUNK) {
        checkpoint(cx)?;
        let actual = &mut buffer[..expected.len()];
        file.read_exact(actual)?;
        if &*actual != expected {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "recovered output failed byte-for-byte readback".to_owned(),
            });
        }
        digest.update(actual);
    }
    if file.read(&mut [0_u8; 1])? != 0 {
        return Err(FrankenError::DatabaseCorrupt { detail: "recovered output grew during readback".to_owned() });
    }
    Ok(digest.finalize())
}

/// Completed native recovery operation. The digest is measured by exact
/// output readback, not a claim that untouched B-tree pages are sound.
#[derive(Debug, Clone)]
pub struct ExportReport {
    pub destination: PathBuf,
    pub pages: usize,
    pub wal_frames: u32,
    pub repaired_frames: usize,
    pub certificate_anchors: usize,
    pub digest: blake3::Hash,
    pub repaired_in_place: bool,
}

async fn recover_to_new_database(vfs: &NativeVfs, cx: &Cx, options: &Options) -> Result<ExportReport> {
    let source = vfs.full_pathname(cx, &options.source)?;
    let destination = vfs.full_pathname(cx, &options.destination)?;
    if source == destination || vfs.path_entry_exists(cx, &destination)? {
        return Err(FrankenError::CannotOpen { path: destination });
    }
    refuse_destination_artifacts(vfs, cx, &destination)?;
    let options = Options { source, destination, ..*options };
    let snapshot = capture(vfs, cx, &options).await?;
    let decode_cx = cx.create_child_for_spawn();
    let limits = options.replay;
    let max_database_bytes = options.max_database_bytes;
    let (image, wal_frames, repaired_frames, pages, certificate_anchors) = spawn_blocking(move || {
        checkpoint(&decode_cx)?;
        let mut database_file_id = [0; 16];
        database_file_id.copy_from_slice(&snapshot.database[76..92]);
        let replay = recover_wal_fec_image_with_certificates(
            &snapshot.wal, &snapshot.sidecar, &snapshot.certificates, database_file_id, limits,
        )?;
        let image = replay.database_image(&snapshot.database, max_database_bytes)?;
        checkpoint(&decode_cx)?;
        let pages = image.len() / usize::try_from(replay.header().page_size).map_err(|_| FrankenError::TooBig)?;
        Ok::<_, FrankenError>((image, replay.committed_frames(), replay.repaired_frame_nos().len(),
            pages, replay.certificate_anchors().len()))
    }).await?;
    checkpoint(cx)?;
    let mut output = host_fs::reserve_new_file(&options.destination)?;
    let publication = (|| {
        refuse_destination_artifacts(vfs, cx, &options.destination)?;
        write_image(&mut output, cx, &image, |file| file.sync_all())?;
        let digest = verify_image(&mut output, cx, &image)?;
        vfs.sync_parent_directory(cx, &options.destination)?;
        Ok::<_, FrankenError>(digest)
    })();
    let digest = match publication {
        Ok(digest) => digest,
        Err(error) => {
            eprintln!("Destination retained at {}; export completion is NOT certified", options.destination.display());
            return Err(error);
        }
    };
    drop(output);
    Ok(ExportReport {
        destination: options.destination, pages, wal_frames, repaired_frames, certificate_anchors,
        digest, repaired_in_place: false,
    })
}

#[cfg(test)]
#[path = "native_recovery_tests.rs"]
mod tests;
