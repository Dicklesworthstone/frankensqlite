//! RealDB E2E runner — differential testing of FrankenSQLite vs C SQLite
//! using real-world database fixtures discovered from `/dp`.
//!
//! # Subcommands
//!
//! - `corpus scan` — Discover SQLite databases under `/dp` and list candidates.
//! - `corpus import` — Copy selected databases into `sample_sqlite_db_files/golden/`.
//! - `corpus verify` — Verify golden copies against `sample_sqlite_db_files/checksums.sha256`.
//! - `run` — Execute an OpLog workload against a chosen engine.
//! - `bench` — Run a Criterion-style benchmark matrix.
//! - `corrupt` — Inject corruption into a working copy for recovery testing.

use std::ffi::OsString;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use fsqlite_e2e::oplog::{self, OpLog};
use fsqlite_e2e::report::EngineRunReport;
use fsqlite_e2e::sqlite_executor::{SqliteExecConfig, run_oplog_sqlite};

fn main() {
    let exit_code = run_cli(std::env::args_os());
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn run_cli<I>(os_args: I) -> i32
where
    I: IntoIterator<Item = OsString>,
{
    let raw: Vec<String> = os_args
        .into_iter()
        .map(|a| a.to_string_lossy().into_owned())
        .collect();

    // Skip program name (raw[0]).
    let tail = if raw.len() > 1 { &raw[1..] } else { &[] };

    if tail.is_empty() || tail.iter().any(|a| a == "-h" || a == "--help") {
        print_top_level_help();
        return 0;
    }

    match tail[0].as_str() {
        "corpus" => cmd_corpus(&tail[1..]),
        "run" => cmd_run(&tail[1..]),
        "bench" => cmd_bench(&tail[1..]),
        "corrupt" => cmd_corrupt(&tail[1..]),
        other => {
            eprintln!("error: unknown subcommand `{other}`");
            eprintln!();
            print_top_level_help();
            2
        }
    }
}

// ── Top-level help ──────────────────────────────────────────────────────

fn print_top_level_help() {
    let text = "\
realdb-e2e — Differential testing of FrankenSQLite vs C SQLite

USAGE:
    realdb-e2e <SUBCOMMAND> [OPTIONS]

SUBCOMMANDS:
    corpus scan             Discover SQLite databases under /dp
    corpus import           Copy selected DBs into golden/ with checksums
    corpus verify           Verify golden copies against checksums.sha256
    run                     Execute an OpLog workload against an engine
    bench                   Run the benchmark matrix (Criterion)
    corrupt                 Inject corruption into a working copy

OPTIONS:
    -h, --help              Show this help message

EXAMPLES:
    realdb-e2e corpus scan
    realdb-e2e corpus scan --root /dp --max-depth 4
    realdb-e2e corpus import --db beads.db --tag beads
    realdb-e2e corpus verify
    realdb-e2e run --engine sqlite3 --db beads-proj-a --workload commutative_inserts --concurrency 4
    realdb-e2e run --engine fsqlite --db beads-proj-a --workload hot_page_contention --concurrency 8
    realdb-e2e bench --db beads-proj-a --preset all
    realdb-e2e corrupt --db beads-proj-a --strategy page --page 1 --seed 42
";
    let _ = io::stdout().write_all(text.as_bytes());
}

// ── corpus ──────────────────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
fn cmd_corpus(argv: &[String]) -> i32 {
    if argv.is_empty() || argv.iter().any(|a| a == "-h" || a == "--help") {
        print_corpus_help();
        return if argv.is_empty() { 2 } else { 0 };
    }

    match argv[0].as_str() {
        "scan" => cmd_corpus_scan(&argv[1..]),
        "import" => cmd_corpus_import(&argv[1..]),
        "verify" => cmd_corpus_verify(&argv[1..]),
        other => {
            eprintln!("error: unknown corpus subcommand `{other}`");
            eprintln!();
            print_corpus_help();
            2
        }
    }
}

fn print_corpus_help() {
    let text = "\
realdb-e2e corpus — Manage the SQLite database fixture corpus

USAGE:
    realdb-e2e corpus <ACTION> [OPTIONS]

ACTIONS:
    scan        Discover SQLite databases under configured roots
    import      Copy a discovered database into golden/ with checksums
    verify      Verify all golden copies match their checksums entries

SCAN OPTIONS:
    --root <DIR>        Root directory to scan (default: /dp)
    --max-depth <N>     Maximum traversal depth (default: 6)
    --header-only       Only show files with valid SQLite magic header

IMPORT OPTIONS:
    --db <NAME>         Source database path or discovery name
    --tag <LABEL>       Classification tag (beads, sample, test, etc.)

VERIFY OPTIONS:
    --checksums <PATH>  Path to checksums file (default: sample_sqlite_db_files/checksums.sha256)
";
    let _ = io::stdout().write_all(text.as_bytes());
}

fn cmd_corpus_scan(argv: &[String]) -> i32 {
    let mut root = "/dp".to_owned();
    let mut max_depth: usize = 6;
    let mut header_only = false;

    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--root" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --root requires a directory argument");
                    return 2;
                }
                root.clone_from(&argv[i]);
            }
            "--max-depth" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --max-depth requires an integer argument");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<usize>() else {
                    eprintln!("error: invalid integer for --max-depth: `{}`", argv[i]);
                    return 2;
                };
                max_depth = n;
            }
            "--header-only" => header_only = true,
            "-h" | "--help" => {
                print_corpus_help();
                return 0;
            }
            other => {
                eprintln!("error: unknown option `{other}`");
                return 2;
            }
        }
        i += 1;
    }

    let config = fsqlite_harness::fixture_discovery::DiscoveryConfig {
        roots: vec![root.into()],
        max_depth,
        ..fsqlite_harness::fixture_discovery::DiscoveryConfig::default()
    };

    match fsqlite_harness::fixture_discovery::discover_sqlite_files(&config) {
        Ok(candidates) => {
            let filtered: Vec<_> = if header_only {
                candidates.into_iter().filter(|c| c.header_ok).collect()
            } else {
                candidates
            };

            println!("Found {} candidate(s):", filtered.len());
            for c in &filtered {
                println!("  {c}");
            }
            0
        }
        Err(e) => {
            eprintln!("error: corpus scan failed: {e}");
            1
        }
    }
}

fn cmd_corpus_import(_argv: &[String]) -> i32 {
    println!("corpus import: not yet implemented (see bd-3jrn)");
    0
}

/// Default path for the checksums file (relative to workspace root).
const DEFAULT_CHECKSUMS_PATH: &str = "sample_sqlite_db_files/checksums.sha256";

/// Default directory containing golden database copies.
const DEFAULT_GOLDEN_DIR: &str = "sample_sqlite_db_files/golden";

fn cmd_corpus_verify(argv: &[String]) -> i32 {
    let mut checksums_path = PathBuf::from(DEFAULT_CHECKSUMS_PATH);
    let mut golden_dir = PathBuf::from(DEFAULT_GOLDEN_DIR);

    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--checksums" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --checksums requires a path argument");
                    return 2;
                }
                checksums_path = PathBuf::from(&argv[i]);
            }
            "--golden-dir" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --golden-dir requires a path argument");
                    return 2;
                }
                golden_dir = PathBuf::from(&argv[i]);
            }
            "-h" | "--help" => {
                print_corpus_help();
                return 0;
            }
            other => {
                eprintln!("error: unknown option `{other}`");
                return 2;
            }
        }
        i += 1;
    }

    match verify_golden_checksums(&checksums_path, &golden_dir) {
        Ok(result) => {
            println!(
                "\n{} verified, {} failed, {} missing",
                result.passed, result.failed, result.missing
            );
            i32::from(result.failed > 0 || result.missing > 0)
        }
        Err(e) => {
            eprintln!("error: {e}");
            1
        }
    }
}

struct VerifyResult {
    passed: usize,
    failed: usize,
    missing: usize,
}

/// Read `checksums.sha256`, recompute each hash, and compare.
fn verify_golden_checksums(
    checksums_path: &Path,
    golden_dir: &Path,
) -> Result<VerifyResult, String> {
    let contents = fs::read_to_string(checksums_path)
        .map_err(|e| format!("cannot read {}: {e}", checksums_path.display()))?;

    let mut passed: usize = 0;
    let mut failed: usize = 0;
    let mut missing: usize = 0;

    for (line_no, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Format: "<hex>  <filename>" (two-space separator, sha256sum convention).
        let Some((expected_hex, filename)) = line.split_once("  ") else {
            eprintln!(
                "warning: skipping malformed line {} in checksums file",
                line_no + 1,
            );
            continue;
        };
        let expected_hex = expected_hex.trim();
        let filename = filename.trim();

        let file_path = golden_dir.join(filename);
        if !file_path.exists() {
            eprintln!("MISSING  {filename}");
            missing += 1;
            continue;
        }

        let actual_hex = match sha256_file(&file_path) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("ERROR    {filename}: {e}");
                failed += 1;
                continue;
            }
        };

        if actual_hex == expected_hex {
            println!("OK       {filename}");
            passed += 1;
        } else {
            eprintln!("MISMATCH {filename}");
            eprintln!("  expected: {expected_hex}");
            eprintln!("  actual:   {actual_hex}");
            failed += 1;
        }
    }

    Ok(VerifyResult {
        passed,
        failed,
        missing,
    })
}

/// Compute the SHA-256 hex digest of a file.
fn sha256_file(path: &Path) -> Result<String, String> {
    let data = fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let hash = Sha256::digest(&data);
    Ok(format!("{hash:x}"))
}

// ── run ─────────────────────────────────────────────────────────────────

fn cmd_run(argv: &[String]) -> i32 {
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        print_run_help();
        return 0;
    }

    let mut engine: Option<String> = None;
    let mut db: Option<String> = None;
    let mut workload: Option<String> = None;
    let mut concurrency: u16 = 1;

    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--engine" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --engine requires an argument (sqlite3|fsqlite)");
                    return 2;
                }
                engine = Some(argv[i].clone());
            }
            "--db" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --db requires a database identifier");
                    return 2;
                }
                db = Some(argv[i].clone());
            }
            "--workload" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --workload requires a preset name");
                    return 2;
                }
                workload = Some(argv[i].clone());
            }
            "--concurrency" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --concurrency requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<u16>() else {
                    eprintln!("error: invalid integer for --concurrency: `{}`", argv[i]);
                    return 2;
                };
                concurrency = n;
            }
            other => {
                eprintln!("error: unknown option `{other}`");
                return 2;
            }
        }
        i += 1;
    }

    let Some(engine_str) = engine.as_deref() else {
        eprintln!("error: --engine is required (sqlite3|fsqlite)");
        return 2;
    };
    let Some(db_name) = db.as_deref() else {
        eprintln!("error: --db is required (golden database identifier)");
        return 2;
    };
    let Some(workload_name) = workload.as_deref() else {
        eprintln!("error: --workload is required (preset name)");
        return 2;
    };

    match engine_str {
        "sqlite3" => run_sqlite3_engine(db_name, workload_name, concurrency),
        "fsqlite" => {
            eprintln!("error: fsqlite engine not yet implemented (see bd-1w6k.3.3)");
            1
        }
        other => {
            eprintln!("error: unknown engine `{other}` (expected sqlite3 or fsqlite)");
            2
        }
    }
}

/// Resolve a database identifier to its golden copy path.
///
/// Accepts either a bare name (e.g. `"frankensqlite"`) which maps to
/// `sample_sqlite_db_files/golden/frankensqlite.db`, or an absolute/relative
/// path to an existing `.db` file.
fn resolve_golden_db(db_name: &str) -> Result<PathBuf, String> {
    // If it looks like a path and exists, use it directly.
    let as_path = PathBuf::from(db_name);
    if as_path.exists() {
        return Ok(as_path);
    }

    // Try golden directory with .db extension.
    let golden = PathBuf::from(DEFAULT_GOLDEN_DIR).join(format!("{db_name}.db"));
    if golden.exists() {
        return Ok(golden);
    }

    // Try golden directory without adding .db (user may have included it).
    let golden_bare = PathBuf::from(DEFAULT_GOLDEN_DIR).join(db_name);
    if golden_bare.exists() {
        return Ok(golden_bare);
    }

    Err(format!(
        "cannot find database `{db_name}` (tried {}, {}, and literal path)",
        golden.display(),
        golden_bare.display(),
    ))
}

/// Generate an OpLog from a preset name and concurrency setting.
fn resolve_workload(preset: &str, fixture_id: &str, concurrency: u16) -> Result<OpLog, String> {
    match preset {
        "commutative_inserts_disjoint_keys" | "commutative_inserts" => Ok(
            oplog::preset_commutative_inserts_disjoint_keys(fixture_id, 42, concurrency, 100),
        ),
        "hot_page_contention" | "hot_page" => Ok(oplog::preset_hot_page_contention(
            fixture_id,
            42,
            concurrency,
            10,
        )),
        "mixed_read_write" | "mixed" => Ok(oplog::preset_mixed_read_write(
            fixture_id,
            42,
            concurrency,
            50,
        )),
        other => Err(format!(
            "unknown workload preset `{other}`. Available: \
             commutative_inserts_disjoint_keys, hot_page_contention, mixed_read_write"
        )),
    }
}

/// Execute a workload against C SQLite via rusqlite and print JSON results.
fn run_sqlite3_engine(db_name: &str, workload_name: &str, concurrency: u16) -> i32 {
    // Resolve golden DB path.
    let golden_path = match resolve_golden_db(db_name) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };

    // Copy golden to a working directory so we don't modify the original.
    let work_dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: failed to create temp dir: {e}");
            return 1;
        }
    };
    let work_db = work_dir.path().join("work.db");
    if let Err(e) = fs::copy(&golden_path, &work_db) {
        eprintln!(
            "error: failed to copy {} to {}: {e}",
            golden_path.display(),
            work_db.display()
        );
        return 1;
    }

    // Resolve the workload preset.
    let oplog = match resolve_workload(workload_name, db_name, concurrency) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };

    let config = SqliteExecConfig::default();
    let sqlite_version = rusqlite::version().to_owned();

    eprintln!(
        "Running: engine=sqlite3 (v{sqlite_version}) db={db_name} \
         workload={workload_name} concurrency={concurrency}"
    );
    eprintln!("  golden: {}", golden_path.display());
    eprintln!("  working: {}", work_db.display());
    eprintln!("  ops: {}", oplog.records.len());

    // Execute the workload.
    let report = match run_oplog_sqlite(&work_db, &oplog, &config) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: execution failed: {e}");
            return 1;
        }
    };

    // Build JSON output.
    let output = RunOutput {
        engine: "sqlite3".to_owned(),
        sqlite_version,
        db: db_name.to_owned(),
        golden_path: golden_path.display().to_string(),
        workload: workload_name.to_owned(),
        concurrency,
        ops_count: oplog.records.len(),
        report,
        timestamp_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
    };

    match serde_json::to_string_pretty(&output) {
        Ok(json) => {
            println!("{json}");
            i32::from(output.report.error.is_some())
        }
        Err(e) => {
            eprintln!("error: failed to serialize report: {e}");
            1
        }
    }
}

/// JSON output structure for a single run invocation.
#[derive(serde::Serialize)]
struct RunOutput {
    engine: String,
    sqlite_version: String,
    db: String,
    golden_path: String,
    workload: String,
    concurrency: u16,
    ops_count: usize,
    report: EngineRunReport,
    timestamp_unix_ms: u64,
}

fn print_run_help() {
    let text = "\
realdb-e2e run — Execute an OpLog workload against an engine

USAGE:
    realdb-e2e run --engine <ENGINE> --db <DB_ID> --workload <NAME> [OPTIONS]

OPTIONS:
    --engine <ENGINE>       Engine to use: sqlite3 | fsqlite
    --db <DB_ID>            Database fixture identifier
    --workload <NAME>       OpLog preset name (e.g. commutative_inserts_disjoint_keys)
    --concurrency <N>       Number of concurrent workers (default: 1)
    -h, --help              Show this help message
";
    let _ = io::stdout().write_all(text.as_bytes());
}

// ── bench ───────────────────────────────────────────────────────────────

fn cmd_bench(argv: &[String]) -> i32 {
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        print_bench_help();
        return 0;
    }

    println!("bench: args={argv:?} (not yet implemented — see bd-312d)");
    0
}

fn print_bench_help() {
    let text = "\
realdb-e2e bench — Run the comparative benchmark matrix

USAGE:
    realdb-e2e bench [OPTIONS]

OPTIONS:
    --db <DB_ID>            Database fixture (default: all golden copies)
    --preset <NAME>         Workload preset (default: all)
    --warmup <N>            Warmup iterations (default: 3)
    --repeat <N>            Measurement iterations (default: 10)
    --output <PATH>         Path for report.json output
    -h, --help              Show this help message
";
    let _ = io::stdout().write_all(text.as_bytes());
}

// ── corrupt ─────────────────────────────────────────────────────────────

fn cmd_corrupt(argv: &[String]) -> i32 {
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        print_corrupt_help();
        return 0;
    }

    println!("corrupt: args={argv:?} (not yet implemented — see bd-1w6k.7.2)");
    0
}

fn print_corrupt_help() {
    let text = "\
realdb-e2e corrupt — Inject corruption into a working copy

USAGE:
    realdb-e2e corrupt --db <DB_ID> --strategy <STRATEGY> [OPTIONS]

STRATEGIES:
    bitflip             Flip random bits (--count N)
    zero                Zero out a byte range (--offset N --length N)
    page                Corrupt an entire page (--page N)

OPTIONS:
    --db <DB_ID>            Database fixture to corrupt (uses working/ copy)
    --strategy <STRATEGY>   Corruption strategy (bitflip|zero|page)
    --seed <N>              RNG seed for deterministic corruption (default: 0)
    --count <N>             Number of bits to flip (bitflip strategy)
    --offset <N>            Byte offset (zero strategy)
    --length <N>            Byte count (zero strategy)
    --page <N>              Page number to corrupt (page strategy)
    -h, --help              Show this help message
";
    let _ = io::stdout().write_all(text.as_bytes());
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn run_with(args: &[&str]) -> i32 {
        let os_args: Vec<OsString> = args.iter().map(OsString::from).collect();
        run_cli(os_args)
    }

    #[test]
    fn test_help_flag_exits_zero() {
        assert_eq!(run_with(&["realdb-e2e", "--help"]), 0);
        assert_eq!(run_with(&["realdb-e2e", "-h"]), 0);
    }

    #[test]
    fn test_no_args_shows_help() {
        assert_eq!(run_with(&["realdb-e2e"]), 0);
    }

    #[test]
    fn test_unknown_subcommand_exits_two() {
        assert_eq!(run_with(&["realdb-e2e", "bogus"]), 2);
    }

    #[test]
    fn test_corpus_no_action_exits_two() {
        assert_eq!(run_with(&["realdb-e2e", "corpus"]), 2);
    }

    #[test]
    fn test_corpus_help_exits_zero() {
        assert_eq!(run_with(&["realdb-e2e", "corpus", "--help"]), 0);
    }

    #[test]
    fn test_corpus_scan_help() {
        assert_eq!(run_with(&["realdb-e2e", "corpus", "scan", "--help"]), 0);
    }

    #[test]
    fn test_run_help() {
        assert_eq!(run_with(&["realdb-e2e", "run", "--help"]), 0);
    }

    #[test]
    fn test_bench_help() {
        assert_eq!(run_with(&["realdb-e2e", "bench", "--help"]), 0);
    }

    #[test]
    fn test_corrupt_help() {
        assert_eq!(run_with(&["realdb-e2e", "corrupt", "--help"]), 0);
    }

    #[test]
    fn test_run_parses_all_options() {
        // Use a temporary on-disk database so the test is hermetic and does
        // not depend on any specific golden fixture being present.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let db_path = tmp.path().to_str().unwrap().to_owned();
        rusqlite::Connection::open(&db_path)
            .unwrap()
            .execute_batch("CREATE TABLE seed (id INTEGER PRIMARY KEY);")
            .unwrap();

        let os_args = vec![
            OsString::from("realdb-e2e"),
            OsString::from("run"),
            OsString::from("--engine"),
            OsString::from("sqlite3"),
            OsString::from("--db"),
            OsString::from(db_path),
            OsString::from("--workload"),
            OsString::from("commutative_inserts_disjoint_keys"),
            OsString::from("--concurrency"),
            OsString::from("2"),
        ];
        assert_eq!(run_cli(os_args), 0);
    }

    #[test]
    fn test_corpus_scan_runs_against_tmp() {
        // Scan an empty temp dir — should find 0 candidates.
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            run_with(&[
                "realdb-e2e",
                "corpus",
                "scan",
                "--root",
                dir.path().to_str().unwrap(),
            ]),
            0
        );
    }

    // ── corpus verify tests ────────────────────────────────────────────

    #[test]
    fn test_verify_all_match() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden");
        fs::create_dir(&golden).unwrap();

        // Create a test file.
        let content = b"hello golden world";
        fs::write(golden.join("test.db"), content).unwrap();

        // Compute expected sha256.
        let expected = format!("{:x}", Sha256::digest(content));

        // Write checksums file.
        let checksums = dir.path().join("checksums.sha256");
        fs::write(&checksums, format!("{expected}  test.db\n")).unwrap();

        let result = verify_golden_checksums(&checksums, &golden).unwrap();
        assert_eq!(result.passed, 1);
        assert_eq!(result.failed, 0);
        assert_eq!(result.missing, 0);
    }

    #[test]
    fn test_verify_mismatch_detected() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden");
        fs::create_dir(&golden).unwrap();

        fs::write(golden.join("bad.db"), b"actual content").unwrap();

        let checksums = dir.path().join("checksums.sha256");
        let wrong_hash = "0".repeat(64);
        fs::write(&checksums, format!("{wrong_hash}  bad.db\n")).unwrap();

        let result = verify_golden_checksums(&checksums, &golden).unwrap();
        assert_eq!(result.passed, 0);
        assert_eq!(result.failed, 1);
        assert_eq!(result.missing, 0);
    }

    #[test]
    fn test_verify_missing_file_detected() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden");
        fs::create_dir(&golden).unwrap();

        let checksums = dir.path().join("checksums.sha256");
        let hash = "0".repeat(64);
        fs::write(&checksums, format!("{hash}  nonexistent.db\n")).unwrap();

        let result = verify_golden_checksums(&checksums, &golden).unwrap();
        assert_eq!(result.passed, 0);
        assert_eq!(result.failed, 0);
        assert_eq!(result.missing, 1);
    }

    #[test]
    fn test_verify_empty_checksums_file() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden");
        fs::create_dir(&golden).unwrap();

        let checksums = dir.path().join("checksums.sha256");
        fs::write(&checksums, "\n").unwrap();

        let result = verify_golden_checksums(&checksums, &golden).unwrap();
        assert_eq!(result.passed, 0);
        assert_eq!(result.failed, 0);
        assert_eq!(result.missing, 0);
    }

    #[test]
    fn test_verify_multiple_files() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden");
        fs::create_dir(&golden).unwrap();

        let a_content = b"file a";
        let b_content = b"file b";
        fs::write(golden.join("a.db"), a_content).unwrap();
        fs::write(golden.join("b.db"), b_content).unwrap();

        let a_hash = format!("{:x}", Sha256::digest(a_content));
        let b_hash = format!("{:x}", Sha256::digest(b_content));

        let checksums = dir.path().join("checksums.sha256");
        fs::write(&checksums, format!("{a_hash}  a.db\n{b_hash}  b.db\n")).unwrap();

        let result = verify_golden_checksums(&checksums, &golden).unwrap();
        assert_eq!(result.passed, 2);
        assert_eq!(result.failed, 0);
        assert_eq!(result.missing, 0);
    }

    #[test]
    fn test_verify_via_cli() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden");
        fs::create_dir(&golden).unwrap();

        let content = b"cli test data";
        fs::write(golden.join("x.db"), content).unwrap();

        let hash = format!("{:x}", Sha256::digest(content));
        let checksums = dir.path().join("checksums.sha256");
        fs::write(&checksums, format!("{hash}  x.db\n")).unwrap();

        // Test via CLI interface.
        assert_eq!(
            run_with(&[
                "realdb-e2e",
                "corpus",
                "verify",
                "--checksums",
                checksums.to_str().unwrap(),
                "--golden-dir",
                golden.to_str().unwrap(),
            ]),
            0
        );
    }

    #[test]
    fn test_verify_via_cli_mismatch_exits_one() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden");
        fs::create_dir(&golden).unwrap();

        fs::write(golden.join("y.db"), b"content").unwrap();

        let checksums = dir.path().join("checksums.sha256");
        let wrong = "f".repeat(64);
        fs::write(&checksums, format!("{wrong}  y.db\n")).unwrap();

        assert_eq!(
            run_with(&[
                "realdb-e2e",
                "corpus",
                "verify",
                "--checksums",
                checksums.to_str().unwrap(),
                "--golden-dir",
                golden.to_str().unwrap(),
            ]),
            1
        );
    }

    #[test]
    fn test_sha256_file_computes_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        fs::write(&path, b"hello").unwrap();

        let result = sha256_file(&path).unwrap();
        // Known sha256 of "hello".
        assert_eq!(
            result,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }
}
