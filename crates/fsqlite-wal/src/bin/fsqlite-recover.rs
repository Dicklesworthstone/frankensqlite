//! Command-line front end for the caller-runtime native recovery library.

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
    use std::ffi::{OsStr, OsString};
    use std::path::PathBuf;
    use std::process::ExitCode;

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_error::{FrankenError, Result};
    use fsqlite_types::cx::Cx;
    use fsqlite_wal::native_recovery::{Options, export_database};

    const HELP: &str = "Usage: fsqlite-recover [OPTIONS] SOURCE.db OUTPUT.db
       fsqlite-recover --repair-wal [OPTIONS] SOURCE.db NEW_BACKUP.wal

Export a coherent main/WAL/FEC snapshot into a NEW database by default.
--repair-wal explicitly repairs the existing Unix WAL and rebuilds its index.
Its second path is a mandatory NEW original-WAL backup, synced and verified
before any source write. Neither mode overwrites a destination or deletes files.
No database/FEC/certificate writes, WAL reset or truncation occur during repair.

Options:
  --repair-wal         Unix only: repair instead of exporting.
  --max-bytes N        Bound each input and output (default: main/output 268435456,
                       WAL 67108864, FEC/certificates 33554432 bytes).
  --max-source-pages N Maximum source pages per FEC decode (default: 256).
  --                  Treat subsequent arguments as literal paths.
  --help              Show this help.

Requires a WAL-mode source, existing WAL and complete recovery. Source admission
requires a writable namespace. Active reader/writer contention fails fast.
Use cooperative trusted directories; do not manipulate source or destination
during recovery. Destination/backup parent must already exist. Missing repair
data, unanchored commits, partial WAL tails and missing pages are errors.
Failed candidates and backups are retained. An indeterminate repair must be
reconciled before source reuse. Run PRAGMA integrity_check on the recovered
output or repaired source before using it; untouched B-tree pages are not checked.";

    struct Command {
        options: Options,
        repair_wal: bool,
    }

    fn positive_limit(value: &OsStr) -> std::result::Result<usize, String> {
        value.to_str().and_then(|text| text.parse::<usize>().ok())
            .filter(|limit| *limit != 0 && isize::try_from(*limit).is_ok())
            .ok_or_else(|| "limits must be positive addressable byte/page counts".to_owned())
    }

    fn parse_args(args: Vec<OsString>) -> std::result::Result<Option<Command>, String> {
        if args.len() == 1 && args[0] == "--help" {
            return Ok(None);
        }
        let mut command = Command {
            options: Options::new(PathBuf::new(), PathBuf::new()),
            repair_wal: false,
        };
        let mut paths = Vec::new();
        let mut literal_paths = false;
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            if !literal_paths && arg == "--" {
                literal_paths = true;
            } else if !literal_paths && arg == "--repair-wal" {
                command.repair_wal = true;
            } else if !literal_paths && (arg == "--max-bytes" || arg == "--max-source-pages") {
                let value = args.next().ok_or_else(|| "missing limit value".to_owned())?;
                let limit = positive_limit(&value)?;
                if arg == "--max-bytes" {
                    command.options.max_database_bytes = limit;
                    command.options.replay.max_wal_bytes = limit;
                    command.options.replay.max_sidecar_bytes = limit;
                    command.options.replay.max_certificate_bytes = limit;
                } else {
                    command.options.replay.max_source_pages = limit;
                }
            } else if !literal_paths && arg.to_string_lossy().starts_with('-') {
                return Err(format!("unknown option: {}", arg.to_string_lossy()));
            } else {
                paths.push(PathBuf::from(arg));
            }
        }
        if paths.len() != 2 || paths.iter().any(|path| path.as_os_str().is_empty()) {
            return Err("expected SOURCE.db and a new destination/backup path".to_owned());
        }
        command.options.destination = paths.pop().expect("two checked paths");
        command.options.source = paths.pop().expect("two checked paths");
        Ok(Some(command))
    }

    fn request_context() -> Result<Cx> {
        let native = asupersync::Cx::current().ok_or_else(|| {
            FrankenError::BackgroundWorkerFailed("recovery requires a caller runtime".to_owned())
        })?;
        let cx = Cx::new();
        cx.set_native_cx(native);
        Ok(cx)
    }

    pub fn main() -> ExitCode {
        let command = match parse_args(std::env::args_os().skip(1).collect()) {
            Ok(Some(command)) => command,
            Ok(None) => { println!("{HELP}"); return ExitCode::SUCCESS; }
            Err(error) => { eprintln!("{error}\n\n{HELP}"); return ExitCode::from(2); }
        };
        // Only the executable owns an executor; the library uses this caller.
        let runtime = match RuntimeBuilder::current_thread().blocking_threads(1, 2).build() {
            Ok(runtime) => runtime,
            Err(error) => {
                eprintln!("cannot start recovery runtime: {error}");
                return ExitCode::FAILURE;
            }
        };
        let result = runtime.block_on(async {
            let cx = request_context()?;
            if command.repair_wal {
                #[cfg(unix)]
                return fsqlite_wal::native_recovery::repair_wal(&cx, &command.options).await;
                #[cfg(not(unix))]
                return Err(FrankenError::Unsupported);
            }
            export_database(&cx, &command.options).await
        });
        match result {
            Ok(report) => {
                if report.repaired_in_place {
                    println!("Repaired WAL and rebuilt index: {}", command.options.source.display());
                    println!("Original WAL backup: {}", report.destination.display());
                } else {
                    println!("Recovered database: {}", report.destination.display());
                }
                println!("Pages: {}; verified WAL frames: {}; repaired frames: {}",
                    report.pages, report.wal_frames, report.repaired_frames);
                println!("{} BLAKE3: {}",
                    if report.repaired_in_place { "Repaired WAL" } else { "Output" }, report.digest);
                println!("Certificate-validated intervals: {}", report.certificate_anchors);
                if report.repaired_in_place {
                    println!("Main/FEC/certificates preserved. Run integrity_check before using the database.");
                } else {
                    println!("Source data preserved. Output B-tree integrity has not been checked.");
                }
                ExitCode::SUCCESS
            }
            Err(error) => { eprintln!("recovery failed: {error}"); ExitCode::FAILURE }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::path::Path;

        #[test]
        fn arguments_keep_paths_literal_and_limits_explicit() {
            let parse = |args: &[&str]| parse_args(args.iter().map(|arg| OsString::from(*arg)).collect());
            assert!(parse(&["--help"]).unwrap().is_none());
            assert!(parse(&[]).is_err());
            assert!(parse(&["--max-bytes", "0", "a", "b"]).is_err());
            assert!(parse(&["--max-source-pages"]).is_err());
            assert!(parse(&["--force", "a", "b"]).is_err());
            assert!(!parse(&["a", "b"]).unwrap().unwrap().repair_wal);
            assert!(parse(&["--repair-wal", "a", "b"]).unwrap().unwrap().repair_wal);
            assert!(!parse(&["--", "--repair-wal", "b"]).unwrap().unwrap().repair_wal);
            let command = parse(&["--max-bytes", "2048", "--", "-a", "-b"]).unwrap().unwrap();
            assert_eq!(command.options.source, Path::new("-a"));
            assert_eq!(command.options.destination, Path::new("-b"));
            assert_eq!(command.options.max_database_bytes, 2048);
            assert_eq!(command.options.replay.max_wal_bytes, 2048);
            assert_eq!(command.options.replay.max_certificate_bytes, 2048);
        }
    }
}
