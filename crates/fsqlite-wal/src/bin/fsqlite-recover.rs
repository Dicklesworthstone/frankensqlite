//! Non-destructive WAL-FEC recovery into a new standalone database.
//!
//! This administrative command captures main/WAL under the native recovery
//! fence, then releases source locks before decoding. It does not open an SQL
//! Connection on damaged input and never publishes into the source namespace.
//! Source directories must obey the VFS cooperative-namespace contract. Native
//! admission may create lock/SHM companions; database, WAL and FEC data are not
//! rewritten. A retained destination from a failed export is never auto-deleted.

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
fn main() -> std::process::ExitCode {
    native::main()
}

#[cfg(not(all(not(target_arch = "wasm32"), any(unix, windows))))]
fn main() -> std::process::ExitCode {
    eprintln!("fsqlite-recover requires a native Unix or Windows VFS");
    std::process::ExitCode::FAILURE
}

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
mod native {
    use std::ffi::OsString;
    use std::io::{self, Read, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};
    use std::process::ExitCode;

    use asupersync::runtime::{RuntimeBuilder, spawn_blocking};
    use fsqlite_error::{FrankenError, Result};
    use fsqlite_types::cx::Cx;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::{Vfs, VfsFile, host_fs};
    use fsqlite_wal::wal_fec::replay::{WalFecReplayLimits, recover_wal_fec_image};

    #[cfg(unix)]
    type NativeVfs = fsqlite_vfs::unix::UnixVfs;
    #[cfg(windows)]
    type NativeVfs = fsqlite_vfs::windows::WindowsVfs;

    const IO_CHUNK: usize = 64 * 1024;
    const DATABASE_HEADER_BYTES: usize = 100;
    const HELP: &str = "Usage: fsqlite-recover [OPTIONS] SOURCE.db OUTPUT.db

Recover a coherent main/WAL/FEC snapshot into a NEW database; never overwrite.
Close active transactions first: source lock contention is a fail-fast error.
The source must be writable for native recovery-lock/SHM admission. Its main,
WAL and FEC data are preserved. The destination parent must already exist and
must not be concurrently manipulated. Do not open the destination until success.

Options:
  --max-bytes N         Bound each input file and the output (default: main/output
                       268435456, WAL 67108864, FEC 33554432 bytes).
  --max-source-pages N  Maximum source pages per FEC decode (default: 256).
  --                   End options; subsequent arguments are literal paths.
  --help               Show this help.

Requires a WAL-mode source, an existing WAL and complete recovery. Missing repair data, ambiguous
commit anchors, partial WAL tails and unexplained page holes are errors, not
permission to export a partial database. This does not repair unrelated main-file
B-tree corruption. Run PRAGMA integrity_check on the OUTPUT before using it.
A failed output is retained for diagnosis; it is never deleted or overwritten.";

    #[derive(Debug)]
    struct Options {
        source: PathBuf,
        destination: PathBuf,
        max_database_bytes: usize,
        replay: WalFecReplayLimits,
    }

    fn positive_limit(value: &std::ffi::OsStr) -> std::result::Result<usize, String> {
        value.to_str().and_then(|text| text.parse::<usize>().ok())
            .filter(|limit| *limit != 0 && isize::try_from(*limit).is_ok())
            .ok_or_else(|| "limits must be positive addressable byte/page counts".to_owned())
    }

    fn parse_args(args: Vec<OsString>) -> std::result::Result<Option<Options>, String> {
        if args.len() == 1 && args[0] == "--help" {
            return Ok(None);
        }
        let mut options = Options {
            source: PathBuf::new(),
            destination: PathBuf::new(),
            max_database_bytes: 256 * 1024 * 1024,
            replay: WalFecReplayLimits::default(),
        };
        let mut paths = Vec::new();
        let mut literal_paths = false;
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            if !literal_paths && arg == "--" {
                literal_paths = true;
            } else if !literal_paths && (arg == "--max-bytes" || arg == "--max-source-pages") {
                let value = args.next().ok_or_else(|| "missing limit value".to_owned())?;
                let limit = positive_limit(&value)?;
                if arg == "--max-bytes" {
                    options.max_database_bytes = limit;
                    options.replay.max_wal_bytes = limit;
                    options.replay.max_sidecar_bytes = limit;
                } else {
                    options.replay.max_source_pages = limit;
                }
            } else if !literal_paths && arg.to_string_lossy().starts_with('-') {
                return Err(format!("unknown option: {}", arg.to_string_lossy()));
            } else {
                paths.push(PathBuf::from(arg));
            }
        }
        if paths.len() != 2 || paths.iter().any(|path| path.as_os_str().is_empty()) {
            return Err("expected SOURCE.db and a new OUTPUT.db path".to_owned());
        }
        options.destination = paths.pop().expect("two checked paths");
        options.source = paths.pop().expect("two checked paths");
        Ok(Some(options))
    }

    pub fn main() -> ExitCode {
        let options = match parse_args(std::env::args_os().skip(1).collect()) {
            Ok(Some(options)) => options,
            Ok(None) => { println!("{HELP}"); return ExitCode::SUCCESS; }
            Err(error) => { eprintln!("{error}\n\n{HELP}"); return ExitCode::from(2); }
        };
        let runtime = match RuntimeBuilder::current_thread().blocking_threads(1, 2).build() {
            Ok(runtime) => runtime,
            Err(error) => {
                eprintln!("cannot start recovery runtime: {error}");
                return ExitCode::FAILURE;
            }
        };
        let result = runtime.block_on(async {
            let cx = request_context()?;
            recover_to_new_database(&NativeVfs::new(), &cx, &options).await
        });
        match result {
            Ok(report) => {
                println!("Recovered database: {}", report.destination.display());
                println!("Pages: {}; verified WAL frames: {}; repaired frames: {}",
                    report.pages, report.wal_frames, report.repaired_frames);
                println!("Output BLAKE3: {}", report.digest);
                println!("Source data preserved. Output B-tree integrity has not been checked.");
                ExitCode::SUCCESS
            }
            Err(error) => { eprintln!("recovery failed: {error}"); ExitCode::FAILURE }
        }
    }

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
        // Inspect metadata before a normal VFS open could block on a FIFO or
        // expose a device. Namespace stability still requires a trusted parent.
        if !host_fs::metadata(path)?.is_file() {
            return Err(FrankenError::CannotOpen { path: path.to_owned() });
        }
        Ok(())
    }

    fn require_wal_mode(database: &[u8]) -> Result<()> {
        // A checksum-valid leftover WAL is not authority for a main database
        // that has switched back to rollback journaling. Refuse that pairing
        // instead of exporting stale commits over a newer main-file image.
        if database.len() < DATABASE_HEADER_BYTES
            || !database.starts_with(b"SQLite format 3\0")
            || database[18] != 2
            || database[19] != 2
        {
            return Err(FrankenError::WalCorrupt {
                detail: "source is not in WAL mode; refusing a potentially stale WAL companion".to_owned(),
            });
        }
        Ok(())
    }

    /// Native descriptors own lock cleanup even when an async read is dropped.
    /// Never close raw std descriptors for main: POSIX close-any-fd semantics
    /// would release a peer connection's process-scoped fcntl locks.
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
            self.maintenance = true; // Arm BEFORE the first acquisition side effect.
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
                // No destination can be certified after explicit finish fails.
                eprintln!("native recovery descriptor cleanup requires retry: {error}");
            }
        }
    }

    struct Snapshot {
        database: Vec<u8>,
        wal: Vec<u8>,
        sidecar: Vec<u8>,
    }

    async fn read_vfs_snapshot<F: VfsFile>(file: &F, cx: &Cx, limit: usize) -> Result<Vec<u8>> {
        checkpoint(cx)?;
        let size = file.file_size(cx)?;
        let len = usize::try_from(size).ok().filter(|len| *len <= limit)
            .ok_or(FrankenError::TooBig)?;
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
        // This stable companion is also used by WAL-FEC append/migration.
        // Never unlink it, and acquire BEFORE opening the replaceable sidecar.
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
            Err(FrankenError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        };
        let len = usize::try_from(file.metadata()?.len()).ok()
            .filter(|len| *len <= limit).ok_or(FrankenError::TooBig)?;
        let bytes = read_exact_snapshot(&mut file, cx, len)?;
        drop(file);
        drop(guard);
        Ok(bytes)
    }

    async fn capture(vfs: &NativeVfs, cx: &Cx, options: &Options) -> Result<Snapshot> {
        require_regular_file(&options.source)?;
        let (file, flags) = vfs.open(cx, Some(&options.source),
            VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB)?;
        let mut main = SourceFile::new(file, cx);
        if !flags.contains(VfsOpenFlags::READWRITE) { return Err(FrankenError::ReadOnly); }
        main.acquire_recovery(cx)?;
        let main_identity = main.file.file_identity()?.ok_or(FrankenError::Unsupported)?;
        let database = read_vfs_snapshot(&main.file, cx, options.max_database_bytes).await?;
        require_wal_mode(&database)?;
        let wal_path = companion(&options.source, "-wal");
        require_regular_file(&wal_path)?;
        let (file, _) = vfs.open(cx, Some(&wal_path), VfsOpenFlags::READONLY | VfsOpenFlags::WAL)?;
        let mut wal = SourceFile::new(file, cx);
        let wal_identity = wal.file.file_identity()?.ok_or(FrankenError::Unsupported)?;
        if main_identity == wal_identity { return Err(FrankenError::BusyRecovery); }
        let wal_bytes = read_vfs_snapshot(&wal.file, cx, options.replay.max_wal_bytes).await?;
        let sidecar_path = companion(&options.source, "-wal-fec");
        let sidecar_cx = cx.create_child_for_spawn();
        let sidecar_limit = options.replay.max_sidecar_bytes;
        let sidecar = spawn_blocking(move || read_sidecar(&sidecar_path, &sidecar_cx, sidecar_limit)).await?;
        // Recheck both named descriptors before releasing the common fence.
        for (path, identity, flags) in [
            (&options.source, main_identity, VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB),
            (&wal_path, wal_identity, VfsOpenFlags::READONLY | VfsOpenFlags::WAL),
        ] {
            let (file, _) = vfs.open_with_expected_identity(cx, path, flags, identity)?;
            SourceFile::new(file, cx).finish()?;
        }
        wal.finish()?;
        main.finish()?;
        Ok(Snapshot { database, wal: wal_bytes, sidecar })
    }

    fn refuse_destination_artifacts(vfs: &NativeVfs, cx: &Cx, path: &Path) -> Result<()> {
        for suffix in ["-journal", "-wal", "-shm", "-wal-fec", "-wal-cert", ".fsqlite-shm"] {
            let artifact = companion(path, suffix);
            if vfs.path_entry_exists(cx, &artifact)? {
                return Err(FrankenError::CannotOpen { path: artifact });
            }
        }
        Ok(())
    }

    /// First make the body durable behind an invalid header, then publish the
    /// real header. Failure can leave a candidate, never a success receipt.
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
            return Err(FrankenError::DatabaseCorrupt {
                detail: "recovered output grew during readback".to_owned(),
            });
        }
        Ok(digest.finalize())
    }

    #[derive(Debug)]
    struct ExportReport {
        destination: PathBuf,
        pages: usize,
        wal_frames: u32,
        repaired_frames: usize,
        digest: blake3::Hash,
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
        let (image, wal_frames, repaired_frames, pages) = spawn_blocking(move || {
            checkpoint(&decode_cx)?;
            let replay = recover_wal_fec_image(&snapshot.wal, &snapshot.sidecar, limits)?;
            let image = replay.database_image(&snapshot.database, max_database_bytes)?;
            checkpoint(&decode_cx)?;
            let pages = image.len() / usize::try_from(replay.header().page_size)
                .map_err(|_| FrankenError::TooBig)?;
            Ok::<_, FrankenError>((image, replay.committed_frames(), replay.repaired_frame_nos().len(), pages))
        }).await?;
        checkpoint(cx)?;
        // O_EXCL reserves only an absent final entry; existing/dangling links
        // and racing creators fail. No source descriptor survives into export.
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
                eprintln!("Destination retained at {}; export completion is NOT certified",
                    options.destination.display());
                return Err(error);
            }
        };
        drop(output);
        Ok(ExportReport {
            destination: options.destination, pages, wal_frames, repaired_frames,
            digest,
        })
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io::Cursor;
        use fsqlite_types::{ObjectId, Oti};
        use fsqlite_wal::{
            SqliteWalChecksum, WAL_FORMAT_VERSION, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
            WAL_MAGIC_LE, WalFecGroupMeta, WalFecGroupMetaInit, WalFecGroupRecord,
            WalFrameHeader, WalHeader, WalSalts, append_wal_fec_group,
            build_source_page_hashes, generate_wal_fec_repair_symbols,
        };
        use fsqlite_wal::checksum::WalChecksumTransform;

        const PAGE_SIZE: usize = 512;
        const FRAME_SIZE: usize = PAGE_SIZE + WAL_FRAME_HEADER_SIZE;

        fn empty_database(version: u32) -> Vec<u8> {
            let mut page = vec![0_u8; PAGE_SIZE];
            page[..16].copy_from_slice(b"SQLite format 3\0");
            page[16..18].copy_from_slice(&512_u16.to_be_bytes());
            page[18..20].copy_from_slice(&[2, 2]);
            page[21..24].copy_from_slice(&[64, 32, 32]);
            page[24..28].copy_from_slice(&7_u32.to_be_bytes());
            page[28..32].copy_from_slice(&1_u32.to_be_bytes());
            page[44..48].copy_from_slice(&4_u32.to_be_bytes());
            page[56..60].copy_from_slice(&1_u32.to_be_bytes());
            page[60..64].copy_from_slice(&version.to_be_bytes());
            page[92..96].copy_from_slice(&7_u32.to_be_bytes());
            page[100] = 13;
            page[105..107].copy_from_slice(&512_u16.to_be_bytes());
            page
        }

        fn fixture(repairs: u32, corrupt_frames: &[usize]) -> (Options, Vec<u8>) {
            // Preserve test artifacts rather than automatically deleting them.
            let directory = tempfile::tempdir().unwrap().keep();
            let options = parse_args(vec![
                directory.join("source.db").into_os_string(),
                directory.join("output.db").into_os_string(),
            ]).unwrap().unwrap();
            host_fs::write(&options.source, empty_database(0)).unwrap();
            let pages: Vec<_> = (1..=4).map(empty_database).collect();
            let header = WalHeader {
                magic: WAL_MAGIC_LE, format_version: WAL_FORMAT_VERSION, page_size: 512,
                checkpoint_seq: 1, salts: WalSalts { salt1: 123, salt2: 456 },
                checksum: SqliteWalChecksum::default(),
            };
            let mut wal = header.to_bytes().unwrap().to_vec();
            let mut running = WalHeader::from_bytes(&wal).unwrap().checksum;
            for (index, page) in pages.iter().enumerate() {
                let start = wal.len();
                wal.extend_from_slice(&WalFrameHeader {
                    page_number: 1, db_size: u32::from(index == 3), salts: header.salts,
                    checksum: SqliteWalChecksum::default(),
                }.to_bytes());
                wal.extend_from_slice(page);
                running = WalChecksumTransform::for_wal_frame(&wal[start..], PAGE_SIZE, false)
                    .unwrap().apply(running);
                wal[start + 16..start + 20].copy_from_slice(&running.s1.to_be_bytes());
                wal[start + 20..start + 24].copy_from_slice(&running.s2.to_be_bytes());
            }
            let meta = WalFecGroupMeta::from_init(WalFecGroupMetaInit {
                wal_salt1: 123, wal_salt2: 456, start_frame_no: 1, end_frame_no: 4,
                db_size_pages: 1, page_size: 512, k_source: 4, r_repair: repairs,
                oti: Oti { f: 2048, al: 1, t: 512, z: 1, n: 1 },
                object_id: ObjectId::derive_from_canonical_bytes(b"native-recovery-export"),
                page_numbers: vec![1; 4], source_page_xxh3_128: build_source_page_hashes(&pages),
            }).unwrap();
            let symbols = generate_wal_fec_repair_symbols(&meta, &pages).unwrap();
            append_wal_fec_group(&companion(&options.source, "-wal-fec"),
                &WalFecGroupRecord::new(meta, symbols).unwrap()).unwrap();
            for frame in corrupt_frames {
                wal[WAL_HEADER_SIZE + frame * FRAME_SIZE + WAL_FRAME_HEADER_SIZE + 60] ^= 0xff;
            }
            host_fs::write(&companion(&options.source, "-wal"), wal).unwrap();
            (options, pages.last().unwrap().clone())
        }

        fn run_test<F: std::future::Future>(future: F) -> F::Output {
            RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future)
        }

        #[test]
        fn native_export_repairs_real_fec_without_changing_source_data() {
            run_test(async {
                let (options, expected) = fixture(8, &[1]);
                let originals: Vec<_> = ["", "-wal", "-wal-fec"].into_iter().map(|suffix| {
                    let path = companion(&options.source, suffix);
                    let bytes = host_fs::read(&path).unwrap();
                    (path, bytes)
                }).collect();
                let cx = request_context().unwrap();
                let report = recover_to_new_database(&NativeVfs::new(), &cx, &options).await.unwrap();
                assert_eq!(report.wal_frames, 4);
                assert_eq!(report.repaired_frames, 1);
                assert_eq!(report.pages, 1);
                assert_eq!(host_fs::read(&options.destination).unwrap(), expected);
                assert_eq!(report.digest, blake3::hash(&expected));
                for (path, bytes) in originals { assert_eq!(host_fs::read(&path).unwrap(), bytes); }
            });
        }

        #[test]
        fn failed_recovery_and_input_budget_do_not_create_output() {
            run_test(async {
                let (mut options, _) = fixture(2, &[0, 1, 2]);
                let cx = request_context().unwrap();
                let vfs = NativeVfs::new();
                assert!(recover_to_new_database(&vfs, &cx, &options).await.is_err());
                assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
                options.replay.max_wal_bytes = 31;
                assert!(matches!(recover_to_new_database(&vfs, &cx, &options).await, Err(FrankenError::TooBig)));
                assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
            });
        }

        #[test]
        fn output_and_stale_sidecars_are_never_overwritten() {
            run_test(async {
                let (mut options, _) = fixture(8, &[]);
                let cx = request_context().unwrap();
                let vfs = NativeVfs::new();
                host_fs::write(&options.destination, b"existing owner").unwrap();
                assert!(recover_to_new_database(&vfs, &cx, &options).await.is_err());
                assert_eq!(host_fs::read(&options.destination).unwrap(), b"existing owner");
                options.destination = options.destination.with_file_name("other.db");
                let stale = companion(&options.destination, "-wal");
                host_fs::write(&stale, b"stale committed bytes").unwrap();
                assert!(recover_to_new_database(&vfs, &cx, &options).await.is_err());
                assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
                assert_eq!(host_fs::read(&stale).unwrap(), b"stale committed bytes");
            });
        }

        #[test]
        fn source_lock_contention_refuses_then_recovers_after_release() {
            run_test(async {
                let (options, expected) = fixture(8, &[1]);
                let cx = request_context().unwrap();
                let vfs = NativeVfs::new();
                let (file, _) = vfs.open(&cx, Some(&options.source),
                    VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB).unwrap();
                let mut owner = SourceFile::new(file, &cx);
                owner.acquire_recovery(&cx).unwrap();
                assert!(recover_to_new_database(&vfs, &cx, &options).await.is_err());
                assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
                owner.finish().unwrap();
                recover_to_new_database(&vfs, &cx, &options).await.unwrap();
                assert_eq!(host_fs::read(&options.destination).unwrap(), expected);
            });
        }

        #[test]
        fn sidecar_writer_contention_does_not_leak_source_recovery_locks() {
            run_test(async {
                let (options, _) = fixture(8, &[1]);
                let cx = request_context().unwrap();
                let lock = companion(&options.source, "-wal-fec.lock");
                let guard = host_fs::open_existing_regular_file_no_follow(&lock).unwrap();
                guard.try_lock().unwrap();
                let vfs = NativeVfs::new();
                assert!(matches!(recover_to_new_database(&vfs, &cx, &options).await, Err(FrankenError::Busy)));
                assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
                drop(guard);
                recover_to_new_database(&vfs, &cx, &options).await.unwrap();
            });
        }

        #[test]
        fn export_header_is_published_only_after_body_sync() {
            let cx = Cx::new();
            let image = empty_database(17);
            let mut file = Cursor::new(Vec::new());
            let mut synced = Vec::new();
            write_image(&mut file, &cx, &image, |file| {
                synced.push(file.get_ref().clone()); Ok(())
            }).unwrap();
            assert_eq!(synced.len(), 2);
            assert_eq!(&synced[0][..100], &[0; 100]);
            assert_eq!(&synced[0][100..], &image[100..]);
            assert_eq!(synced[1], image);
        }

        #[test]
        fn failed_body_sync_never_writes_the_valid_header() {
            let cx = Cx::new();
            let mut file = Cursor::new(Vec::new());
            assert!(write_image(&mut file, &cx, &empty_database(17), |_| {
                Err(io::Error::other("injected body sync failure"))
            }).is_err());
            assert_eq!(&file.get_ref()[..100], &[0; 100]);
        }

        #[test]
        fn bounded_reader_refuses_short_and_growing_snapshots() {
            let cx = Cx::new();
            assert!(read_exact_snapshot(&mut Cursor::new(vec![1; 3]), &cx, 4).is_err());
            assert!(read_exact_snapshot(&mut Cursor::new(vec![1; 5]), &cx, 4).is_err());
            assert_eq!(read_exact_snapshot(&mut Cursor::new(vec![1; 4]), &cx, 4).unwrap(), vec![1; 4]);
            cx.cancel();
            assert!(matches!(read_exact_snapshot(&mut Cursor::new(vec![]), &cx, 0), Err(FrankenError::Interrupt)));
        }

        #[test]
        fn arguments_keep_paths_literal_and_limits_explicit() {
            let parse = |args: &[&str]| parse_args(args.iter().map(|arg| OsString::from(*arg)).collect());
            assert!(parse(&["--help"]).unwrap().is_none());
            assert!(parse(&[]).is_err());
            assert!(parse(&["--max-bytes", "0", "a", "b"]).is_err());
            assert!(parse(&["--max-source-pages"]).is_err());
            assert!(parse(&["--force", "a", "b"]).is_err());
            let options = parse(&["--max-bytes", "2048", "--", "-a", "-b"]).unwrap().unwrap();
            assert_eq!(options.source, Path::new("-a"));
            assert_eq!(options.destination, Path::new("-b"));
            assert_eq!(options.max_database_bytes, 2048);
            assert_eq!(options.replay.max_wal_bytes, 2048);
        }

        #[test]
        fn source_mode_guard_rejects_stale_wal_pairings() {
            let database = empty_database(19);
            require_wal_mode(&database).unwrap();
            for modes in [[1, 1], [1, 2], [2, 1], [0, 0], [3, 3]] {
                let mut invalid = database.clone();
                invalid[18..20].copy_from_slice(&modes);
                assert!(require_wal_mode(&invalid).is_err());
            }
            assert!(require_wal_mode(&database[..19]).is_err());
            let mut invalid = database;
            invalid[0] = 0;
            assert!(require_wal_mode(&invalid).is_err());
        }

        #[test]
        fn cancelled_export_does_not_reserve_destination() {
            run_test(async {
                let (options, _) = fixture(8, &[1]);
                let cx = request_context().unwrap();
                cx.cancel();
                let vfs = NativeVfs::new();
                assert!(recover_to_new_database(&vfs, &cx, &options).await.is_err());
                assert!(!vfs.path_entry_exists(&Cx::new(), &options.destination).unwrap());
            });
        }

        #[test]
        fn final_sync_failure_does_not_return_a_success_receipt() {
            let cx = Cx::new();
            let mut file = Cursor::new(Vec::new());
            let mut calls = 0;
            let image = empty_database(19);
            assert!(write_image(&mut file, &cx, &image, |_| {
                calls += 1;
                if calls == 2 { Err(io::Error::other("injected final sync failure")) }
                else { Ok(()) }
            }).is_err());
            assert_eq!(calls, 2);
            // Candidate bytes can exist after failure; they are never certified
            // or silently deleted just because the final sync returned error.
            assert_eq!(file.into_inner(), image);
        }

        #[test]
        fn readback_rejects_changed_bytes_and_unexpected_tail() {
            let cx = Cx::new();
            let image = empty_database(19);
            assert_eq!(verify_image(&mut Cursor::new(image.clone()), &cx, &image).unwrap(),
                blake3::hash(&image));
            let mut changed = image.clone();
            changed[200] ^= 1;
            assert!(verify_image(&mut Cursor::new(changed), &cx, &image).is_err());
            let mut grown = image.clone();
            grown.push(0);
            assert!(verify_image(&mut Cursor::new(grown), &cx, &image).is_err());
        }

        #[cfg(unix)]
        #[test]
        fn dangling_destination_and_source_alias_are_refused() {
            use std::os::unix::fs::symlink;
            run_test(async {
                let (mut options, _) = fixture(8, &[]);
                let original = host_fs::read(&options.source).unwrap();
                let cx = request_context().unwrap();
                let vfs = NativeVfs::new();
                let missing = options.destination.with_file_name("absent.db");
                symlink(&missing, &options.destination).unwrap();
                assert!(recover_to_new_database(&vfs, &cx, &options).await.is_err());
                assert_eq!(host_fs::read_link(&options.destination).unwrap(), missing);
                options.destination = options.destination.with_file_name("alias.db");
                symlink(&options.source, &options.destination).unwrap();
                assert!(recover_to_new_database(&vfs, &cx, &options).await.is_err());
                assert_eq!(host_fs::read(&options.source).unwrap(), original);
            });
        }
    }
}
