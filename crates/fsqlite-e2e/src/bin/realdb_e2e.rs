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
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use rusqlite::{Connection, DatabaseName, OpenFlags};
use serde::Serialize;

use fsqlite_e2e::benchmark::{BenchmarkConfig, BenchmarkMeta, BenchmarkSummary, run_benchmark};
use fsqlite_e2e::corruption::{CorruptionStrategy, inject_corruption};
use fsqlite_e2e::fsqlite_executor::{FsqliteExecConfig, run_oplog_fsqlite};
use fsqlite_e2e::methodology::EnvironmentMeta;
use fsqlite_e2e::oplog::{self, OpLog};
use fsqlite_e2e::report::{EngineInfo, RunRecordV1, RunRecordV1Args};
use fsqlite_e2e::report_render::render_benchmark_summaries_markdown;
use fsqlite_e2e::run_workspace::{WorkspaceConfig, create_workspace};
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
    --db <PATH|NAME>        Source database path (preferred) or discovery filename/stem
    --id <DB_ID>            Override destination fixture id (default: sanitized stem)
    --tag <LABEL>           Classification tag (stored in metadata)
    --golden-dir <DIR>      Destination golden directory
                            (default: sample_sqlite_db_files/golden)
    --metadata-dir <DIR>    Destination metadata directory
                            (default: sample_sqlite_db_files/metadata)
    --checksums <PATH>      Checksums file to update
                            (default: sample_sqlite_db_files/checksums.sha256)
    --root <DIR>            Discovery root (only used when resolving NAME)
                            (default: /dp)
    --max-depth <N>         Discovery max-depth (only used when resolving NAME)
                            (default: 6)
    --allow-bad-header      Allow importing files failing SQLite magic header check
    --no-metadata           Skip metadata generation

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

#[allow(clippy::too_many_lines)]
fn cmd_corpus_import(argv: &[String]) -> i32 {
    if argv.is_empty() || argv.iter().any(|a| a == "-h" || a == "--help") {
        print_corpus_help();
        return if argv.is_empty() { 2 } else { 0 };
    }

    let mut db_arg: Option<String> = None;
    let mut id_override: Option<String> = None;
    let mut tag: Option<String> = None;
    let mut golden_dir = PathBuf::from(DEFAULT_GOLDEN_DIR);
    let mut metadata_dir = PathBuf::from(DEFAULT_METADATA_DIR);
    let mut checksums_path = PathBuf::from(DEFAULT_CHECKSUMS_PATH);
    let mut root = PathBuf::from("/dp");
    let mut max_depth: usize = 6;
    let mut allow_bad_header = false;
    let mut write_metadata = true;

    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--db" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --db requires a path or discovery name");
                    return 2;
                }
                db_arg = Some(argv[i].clone());
            }
            "--id" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --id requires a fixture identifier");
                    return 2;
                }
                id_override = Some(argv[i].clone());
            }
            "--tag" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --tag requires a label");
                    return 2;
                }
                tag = Some(argv[i].clone());
            }
            "--golden-dir" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --golden-dir requires a directory path");
                    return 2;
                }
                golden_dir = PathBuf::from(&argv[i]);
            }
            "--metadata-dir" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --metadata-dir requires a directory path");
                    return 2;
                }
                metadata_dir = PathBuf::from(&argv[i]);
            }
            "--checksums" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --checksums requires a file path");
                    return 2;
                }
                checksums_path = PathBuf::from(&argv[i]);
            }
            "--root" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --root requires a directory path");
                    return 2;
                }
                root = PathBuf::from(&argv[i]);
            }
            "--max-depth" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --max-depth requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<usize>() else {
                    eprintln!("error: invalid integer for --max-depth: `{}`", argv[i]);
                    return 2;
                };
                max_depth = n;
            }
            "--allow-bad-header" => allow_bad_header = true,
            "--no-metadata" => write_metadata = false,
            other => {
                eprintln!("error: unknown option `{other}`");
                return 2;
            }
        }
        i += 1;
    }

    let Some(db_arg) = db_arg.as_deref() else {
        eprintln!("error: --db is required");
        return 2;
    };

    // Resolve source DB path. Prefer literal paths; otherwise do a bounded discovery scan.
    let (source_path, source_tags, header_ok) = match resolve_source_db(db_arg, &root, max_depth) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };

    if !allow_bad_header && !header_ok {
        eprintln!(
            "error: source does not look like a SQLite database (bad magic header): {}",
            source_path.display()
        );
        return 1;
    }

    // Determine destination fixture id.
    let raw_id = id_override.as_deref().unwrap_or_else(|| {
        source_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("fixture")
    });
    let fixture_id = match sanitize_db_id(raw_id) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("error: invalid fixture id `{raw_id}`: {e}");
            return 2;
        }
    };

    if let Err(e) = fs::create_dir_all(&golden_dir) {
        eprintln!(
            "error: failed to create golden dir {}: {e}",
            golden_dir.display()
        );
        return 1;
    }

    let dest_db = golden_dir.join(format!("{fixture_id}.db"));

    if dest_db.exists() {
        // Golden copies are immutable. Never overwrite in-place; use --id for a new fixture.
        println!("Golden already exists: {}", dest_db.display());
    } else {
        // Safety policy (sample_sqlite_db_files/FIXTURES.md): never raw-copy /dp inputs.
        // Use SQLite's backup API to capture a consistent snapshot.
        if let Err(e) = backup_sqlite_file(&source_path, &dest_db) {
            eprintln!(
                "error: failed to back up {} to {}: {e}",
                source_path.display(),
                dest_db.display()
            );
            return 1;
        }
    }

    // Verify integrity immediately after capture (or for existing golden).
    if let Err(e) = sqlite_integrity_check(&dest_db) {
        eprintln!(
            "error: golden DB failed PRAGMA integrity_check: {}: {e}",
            dest_db.display()
        );
        return 1;
    }

    // Best-effort: mark golden copies read-only.
    if let Err(e) = set_read_only(&dest_db) {
        eprintln!(
            "warning: failed to mark read-only {}: {e}",
            dest_db.display()
        );
    }

    // Update checksums file (DB only, not sidecars).
    let dest_sha = match sha256_file(&dest_db) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("error: cannot hash golden db {}: {e}", dest_db.display());
            return 1;
        }
    };
    if let Err(e) = upsert_checksum(&checksums_path, &dest_db, &dest_sha) {
        eprintln!("error: failed to update checksums: {e}");
        return 1;
    }

    // Generate/update metadata JSON unless disabled.
    if write_metadata {
        if let Err(e) = fs::create_dir_all(&metadata_dir) {
            eprintln!(
                "error: failed to create metadata dir {}: {e}",
                metadata_dir.display()
            );
            return 1;
        }

        match profile_database_for_metadata(
            &dest_db,
            &fixture_id,
            Some(&source_path),
            tag.as_deref(),
            &source_tags,
        ) {
            Ok(profile) => {
                let out_path = metadata_dir.join(format!("{fixture_id}.json"));
                match serde_json::to_string_pretty(&profile) {
                    Ok(json) => {
                        if let Err(e) = fs::write(&out_path, json.as_bytes()) {
                            eprintln!(
                                "error: failed to write metadata {}: {e}",
                                out_path.display()
                            );
                            return 1;
                        }
                        println!("Wrote metadata: {}", out_path.display());
                    }
                    Err(e) => {
                        eprintln!("error: failed to serialize metadata: {e}");
                        return 1;
                    }
                }
            }
            Err(e) => {
                eprintln!("error: failed to profile imported DB: {e}");
                return 1;
            }
        }
    }

    // Final summary.
    println!("Imported fixture:");
    println!("  id: {fixture_id}");
    println!("  source: {}", source_path.display());
    println!("  golden: {}", dest_db.display());
    println!("  sha256: {dest_sha}");
    if let Some(tag) = tag.as_deref() {
        println!("  tag: {tag}");
    }
    if !source_tags.is_empty() {
        println!("  tags: {}", source_tags.join(", "));
    }

    0
}

/// Default path for the checksums file (relative to workspace root).
const DEFAULT_CHECKSUMS_PATH: &str = "sample_sqlite_db_files/checksums.sha256";

/// Default directory containing golden database copies.
const DEFAULT_GOLDEN_DIR: &str = "sample_sqlite_db_files/golden";

/// Default directory containing per-fixture metadata JSON.
const DEFAULT_METADATA_DIR: &str = "sample_sqlite_db_files/metadata";

/// Default base directory for per-run working copies.
const DEFAULT_WORKING_DIR: &str = "sample_sqlite_db_files/working";

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

#[allow(clippy::too_many_lines)]
fn cmd_run(argv: &[String]) -> i32 {
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        print_run_help();
        return 0;
    }

    let mut engine: Option<String> = None;
    let mut db: Option<String> = None;
    let mut workload: Option<String> = None;
    let mut concurrency: Vec<u16> = vec![1];
    let mut repeat: usize = 1;
    let mut fsqlite_mvcc: bool = false;
    let mut pretty: bool = false;
    let mut output_jsonl: Option<PathBuf> = None;

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
                    eprintln!("error: --concurrency requires an integer or comma-separated list");
                    return 2;
                }
                match parse_u16_list(&argv[i]) {
                    Ok(v) => concurrency = v,
                    Err(e) => {
                        eprintln!("error: {e}");
                        return 2;
                    }
                }
            }
            "--repeat" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --repeat requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<usize>() else {
                    eprintln!("error: invalid integer for --repeat: `{}`", argv[i]);
                    return 2;
                };
                if n == 0 {
                    eprintln!("error: --repeat must be >= 1");
                    return 2;
                }
                repeat = n;
            }
            "--mvcc" => {
                fsqlite_mvcc = true;
            }
            "--pretty" => {
                pretty = true;
            }
            "--output-jsonl" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --output-jsonl requires a path");
                    return 2;
                }
                output_jsonl = Some(PathBuf::from(argv[i].clone()));
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
        "sqlite3" => run_sqlite3_engine(
            db_name,
            workload_name,
            &concurrency,
            repeat,
            pretty,
            output_jsonl.as_deref(),
        ),
        "fsqlite" => run_fsqlite_engine(
            db_name,
            workload_name,
            &concurrency,
            repeat,
            fsqlite_mvcc,
            pretty,
            output_jsonl.as_deref(),
        ),
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
#[allow(clippy::too_many_lines)]
fn run_sqlite3_engine(
    db_name: &str,
    workload_name: &str,
    concurrency: &[u16],
    repeat: usize,
    pretty: bool,
    output_jsonl: Option<&Path>,
) -> i32 {
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

    let config = SqliteExecConfig::default();
    let sqlite_version = rusqlite::version().to_owned();

    let golden_sha256 = match sha256_file(&golden_path) {
        Ok(h) => Some(h),
        Err(e) => {
            eprintln!("warning: failed to compute golden sha256: {e}");
            None
        }
    };

    let mut results: Vec<RunAgg> = Vec::new();
    let mut any_error = false;

    for &c in concurrency {
        let mut agg = RunAgg::new(c);
        for rep in 0..repeat {
            // Copy golden to a fresh working directory so we don't modify the original.
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

            let oplog = match resolve_workload(workload_name, db_name, c) {
                Ok(o) => o,
                Err(e) => {
                    eprintln!("error: {e}");
                    return 1;
                }
            };

            eprintln!(
                "Running: engine=sqlite3 (v{sqlite_version}) db={db_name} workload={workload_name} \
                 concurrency={c} rep={rep}/{repeat}"
            );
            eprintln!("  golden: {}", golden_path.display());
            eprintln!("  working: {}", work_db.display());
            eprintln!("  ops: {}", oplog.records.len());

            let report = match run_oplog_sqlite(&work_db, &oplog, &config) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: execution failed: {e}");
                    return 1;
                }
            };
            agg.record(&report);
            any_error |= report.error.is_some();

            let recorded_unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

            let record = RunRecordV1::new(RunRecordV1Args {
                recorded_unix_ms,
                environment: EnvironmentMeta::capture("release"),
                engine: EngineInfo {
                    name: "sqlite3".to_owned(),
                    sqlite_version: Some(sqlite_version.clone()),
                    fsqlite_git: None,
                },
                fixture_id: db_name.to_owned(),
                golden_path: Some(golden_path.display().to_string()),
                golden_sha256: golden_sha256.clone(),
                workload: workload_name.to_owned(),
                concurrency: c,
                ops_count: u64::try_from(oplog.records.len()).unwrap_or(u64::MAX),
                report,
            });

            let json = if pretty {
                record.to_pretty_json()
            } else {
                record.to_jsonl_line()
            };

            let text = match json {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("error: failed to serialize report: {e}");
                    return 1;
                }
            };

            if let Some(path) = output_jsonl {
                if let Err(e) = append_jsonl_line(path, &text) {
                    eprintln!("error: failed to append JSONL output: {e}");
                    return 1;
                }
            }
            println!("{text}");
        }
        results.push(agg);
    }

    if results.len() > 1 || repeat > 1 {
        eprintln!("{}", format_scaling_summary("sqlite3", repeat, &results));
    }

    i32::from(any_error)
}

/// Execute a workload against FrankenSQLite and print JSON results.
#[allow(clippy::too_many_lines)]
fn run_fsqlite_engine(
    db_name: &str,
    workload_name: &str,
    concurrency: &[u16],
    repeat: usize,
    mvcc: bool,
    pretty: bool,
    output_jsonl: Option<&Path>,
) -> i32 {
    let golden_path = match resolve_golden_db(db_name) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };

    let golden_sha256 = match sha256_file(&golden_path) {
        Ok(h) => Some(h),
        Err(e) => {
            eprintln!("warning: failed to compute golden sha256: {e}");
            None
        }
    };

    let config = FsqliteExecConfig {
        concurrent_mode: mvcc,
        ..FsqliteExecConfig::default()
    };

    let mut results: Vec<RunAgg> = Vec::new();
    let mut any_error = false;

    for &c in concurrency {
        let mut agg = RunAgg::new(c);
        for rep in 0..repeat {
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

            let oplog = match resolve_workload(workload_name, db_name, c) {
                Ok(o) => o,
                Err(e) => {
                    eprintln!("error: {e}");
                    return 1;
                }
            };

            let mode = if mvcc { "mvcc" } else { "single-writer" };
            eprintln!(
                "Running: engine=fsqlite mode={mode} db={db_name} workload={workload_name} \
                 concurrency={c} rep={rep}/{repeat}"
            );
            eprintln!("  golden: {}", golden_path.display());
            eprintln!("  working: {}", work_db.display());
            eprintln!("  ops: {}", oplog.records.len());

            let report = match run_oplog_fsqlite(&work_db, &oplog, &config) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: execution failed: {e}");
                    return 1;
                }
            };
            agg.record(&report);
            any_error |= report.error.is_some();

            let recorded_unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

            let record = RunRecordV1::new(RunRecordV1Args {
                recorded_unix_ms,
                environment: EnvironmentMeta::capture("release"),
                engine: EngineInfo {
                    name: "fsqlite".to_owned(),
                    sqlite_version: None,
                    fsqlite_git: None,
                },
                fixture_id: db_name.to_owned(),
                golden_path: Some(golden_path.display().to_string()),
                golden_sha256: golden_sha256.clone(),
                workload: workload_name.to_owned(),
                concurrency: c,
                ops_count: u64::try_from(oplog.records.len()).unwrap_or(u64::MAX),
                report,
            });

            let json = if pretty {
                record.to_pretty_json()
            } else {
                record.to_jsonl_line()
            };

            let text = match json {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("error: failed to serialize report: {e}");
                    return 1;
                }
            };

            if let Some(path) = output_jsonl {
                if let Err(e) = append_jsonl_line(path, &text) {
                    eprintln!("error: failed to append JSONL output: {e}");
                    return 1;
                }
            }
            println!("{text}");
        }
        results.push(agg);
    }

    if results.len() > 1 || repeat > 1 {
        eprintln!("{}", format_scaling_summary("fsqlite", repeat, &results));
    }

    i32::from(any_error)
}

fn append_jsonl_line(path: &Path, line: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(f, "{line}")?;
    Ok(())
}

#[derive(Debug, Clone)]
struct RunAgg {
    concurrency: u16,
    wall_time_ms: Vec<u64>,
    ops_per_sec: Vec<f64>,
    retries: Vec<u64>,
    aborts: Vec<u64>,
}

impl RunAgg {
    fn new(concurrency: u16) -> Self {
        Self {
            concurrency,
            wall_time_ms: Vec::new(),
            ops_per_sec: Vec::new(),
            retries: Vec::new(),
            aborts: Vec::new(),
        }
    }

    fn record(&mut self, report: &fsqlite_e2e::report::EngineRunReport) {
        self.wall_time_ms.push(report.wall_time_ms);
        self.ops_per_sec.push(report.ops_per_sec);
        self.retries.push(report.retries);
        self.aborts.push(report.aborts);
    }
}

fn format_scaling_summary(engine: &str, repeat: usize, results: &[RunAgg]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "\n{}", "-".repeat(72));
    let _ = writeln!(out, "  Scaling summary: engine={engine} repeat={repeat}");
    let _ = writeln!(out, "{}", "-".repeat(72));
    let _ = writeln!(
        out,
        "  {:>10} {:>12} {:>12} {:>10} {:>10}",
        "Conc", "p50 ops/s", "p95 ops/s", "p50 ms", "p50 retries"
    );
    let _ = writeln!(out, "  {:-<72}", "");

    for r in results {
        let p50_ops = percentile_f64(&r.ops_per_sec, 50);
        let p95_ops = percentile_f64(&r.ops_per_sec, 95);
        let p50_ms = percentile_u64(&r.wall_time_ms, 50);
        let p50_retries = percentile_u64(&r.retries, 50);
        let _ = writeln!(
            out,
            "  {:>10} {:>12.1} {:>12.1} {:>10} {:>10}",
            r.concurrency, p50_ops, p95_ops, p50_ms, p50_retries
        );
    }

    let _ = writeln!(out, "{}", "-".repeat(72));
    out
}

fn percentile_u64(data: &[u64], pct: u32) -> u64 {
    if data.is_empty() {
        return 0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_unstable();
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let idx = ((f64::from(pct) / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn percentile_f64(data: &[f64], pct: u32) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_by(f64::total_cmp);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let idx = ((f64::from(pct) / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn parse_u16_list(raw: &str) -> Result<Vec<u16>, String> {
    let mut out: Vec<u16> = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(format!("invalid --concurrency list: `{raw}`"));
        }
        let Ok(n) = part.parse::<u16>() else {
            return Err(format!("invalid integer in --concurrency list: `{part}`"));
        };
        out.push(n);
    }
    if out.is_empty() {
        Err(format!("invalid --concurrency list: `{raw}`"))
    } else {
        Ok(out)
    }
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
    --concurrency <N|LIST>  Number of workers, or comma-separated list (default: 1)
    --repeat <N>            Repetitions per concurrency (default: 1)
    --mvcc                  For fsqlite: enable MVCC concurrent_mode
    --output-jsonl <PATH>   Append a single JSONL record to PATH
    --pretty                Pretty-print JSON to stdout (default: JSONL)
    -h, --help              Show this help message
";
    let _ = io::stdout().write_all(text.as_bytes());
}

// ── bench ───────────────────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
fn cmd_bench(argv: &[String]) -> i32 {
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        print_bench_help();
        return 0;
    }

    let mut golden_dir = PathBuf::from(DEFAULT_GOLDEN_DIR);
    let mut fixture_ids: Vec<String> = Vec::new();
    let mut presets: Vec<String> = Vec::new();
    let mut concurrency: Vec<u16> = vec![1, 2, 4, 8];
    let mut engine = "both".to_owned(); // sqlite3|fsqlite|both
    let mut mvcc = false;
    let defaults = BenchmarkConfig::default();
    let mut warmup_iterations = defaults.warmup_iterations;
    let mut min_iterations = defaults.min_iterations;
    let mut measurement_time_secs = defaults.measurement_time_secs;
    let mut output_jsonl: Option<PathBuf> = None;
    let mut output_md: Option<PathBuf> = None;
    let mut pretty = false;

    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--golden-dir" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --golden-dir requires a directory path");
                    return 2;
                }
                golden_dir = PathBuf::from(&argv[i]);
            }
            "--db" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --db requires a fixture id or comma-separated list");
                    return 2;
                }
                for part in argv[i].split(',') {
                    let part = part.trim();
                    if !part.is_empty() {
                        fixture_ids.push(part.to_owned());
                    }
                }
            }
            "--preset" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --preset requires a preset name or comma-separated list");
                    return 2;
                }
                for part in argv[i].split(',') {
                    let part = part.trim();
                    if !part.is_empty() {
                        presets.push(part.to_owned());
                    }
                }
            }
            "--concurrency" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --concurrency requires an integer or comma-separated list");
                    return 2;
                }
                match parse_u16_list(&argv[i]) {
                    Ok(v) => concurrency = v,
                    Err(e) => {
                        eprintln!("error: {e}");
                        return 2;
                    }
                }
            }
            "--engine" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --engine requires sqlite3|fsqlite|both");
                    return 2;
                }
                engine.clone_from(&argv[i]);
            }
            "--mvcc" => mvcc = true,
            "--warmup" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --warmup requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<u32>() else {
                    eprintln!("error: invalid integer for --warmup: `{}`", argv[i]);
                    return 2;
                };
                warmup_iterations = n;
            }
            "--repeat" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --repeat requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<u32>() else {
                    eprintln!("error: invalid integer for --repeat: `{}`", argv[i]);
                    return 2;
                };
                if n == 0 {
                    eprintln!("error: --repeat must be >= 1");
                    return 2;
                }
                min_iterations = n;
                measurement_time_secs = 0;
            }
            "--min-iters" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --min-iters requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<u32>() else {
                    eprintln!("error: invalid integer for --min-iters: `{}`", argv[i]);
                    return 2;
                };
                min_iterations = n;
            }
            "--time-secs" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --time-secs requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<u64>() else {
                    eprintln!("error: invalid integer for --time-secs: `{}`", argv[i]);
                    return 2;
                };
                measurement_time_secs = n;
            }
            "--output-jsonl" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --output-jsonl requires a path");
                    return 2;
                }
                output_jsonl = Some(PathBuf::from(&argv[i]));
            }
            "--output" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --output requires a path");
                    return 2;
                }
                output_jsonl = Some(PathBuf::from(&argv[i]));
            }
            "--output-md" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --output-md requires a path");
                    return 2;
                }
                output_md = Some(PathBuf::from(&argv[i]));
            }
            "--pretty" => pretty = true,
            other => {
                eprintln!("error: unknown option `{other}`");
                return 2;
            }
        }
        i += 1;
    }

    if presets.is_empty() || presets.iter().any(|p| p == "all") {
        presets = vec![
            "commutative_inserts_disjoint_keys".to_owned(),
            "hot_page_contention".to_owned(),
            "mixed_read_write".to_owned(),
        ];
    }

    if fixture_ids.is_empty() {
        match discover_golden_fixture_ids(&golden_dir) {
            Ok(ids) => fixture_ids = ids,
            Err(e) => {
                eprintln!("error: {e}");
                return 1;
            }
        }
    }

    let bench_cfg = BenchmarkConfig {
        warmup_iterations,
        min_iterations,
        measurement_time_secs,
    };

    let cargo_profile = cargo_profile_name();
    let mut summaries: Vec<BenchmarkSummary> = Vec::new();
    let mut any_iteration_error = false;

    // If an output file is specified, truncate it up front so this run produces a
    // clean report artifact (rather than appending to an existing file).
    if let Some(ref path) = output_jsonl {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(e) = fs::create_dir_all(parent) {
                    eprintln!(
                        "error: failed to create output directory {}: {e}",
                        parent.display()
                    );
                    return 1;
                }
            }
        }
        if let Err(e) = fs::File::create(path) {
            eprintln!(
                "error: failed to create output file {}: {e}",
                path.display()
            );
            return 1;
        }
    }

    for fixture_id in &fixture_ids {
        let golden_path = match resolve_golden_db_in(&golden_dir, fixture_id) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("error: {e}");
                return 1;
            }
        };

        for preset in &presets {
            for &c in &concurrency {
                let engines: Vec<(&str, bool)> = match engine.as_str() {
                    "sqlite3" => vec![("sqlite3", false)],
                    "fsqlite" => vec![("fsqlite", mvcc)],
                    "both" => vec![("sqlite3", false), ("fsqlite", mvcc)],
                    other => {
                        eprintln!(
                            "error: unknown --engine `{other}` (expected sqlite3|fsqlite|both)"
                        );
                        return 2;
                    }
                };

                for (engine_name, fsqlite_mvcc) in engines {
                    let engine_label = if engine_name == "fsqlite" && fsqlite_mvcc {
                        "fsqlite_mvcc"
                    } else {
                        engine_name
                    };

                    let meta = BenchmarkMeta {
                        engine: engine_label.to_owned(),
                        workload: preset.to_owned(),
                        fixture_id: fixture_id.to_owned(),
                        concurrency: c,
                        cargo_profile: cargo_profile.to_owned(),
                    };

                    let sqlite_cfg = SqliteExecConfig {
                        run_integrity_check: false,
                        ..SqliteExecConfig::default()
                    };
                    let fsqlite_cfg = FsqliteExecConfig {
                        concurrent_mode: fsqlite_mvcc,
                        run_integrity_check: false,
                        ..FsqliteExecConfig::default()
                    };

                    let summary = run_benchmark(&bench_cfg, &meta, |global_idx| {
                        let _ = global_idx; // currently unused, but kept for future run-id tagging.
                        let td = tempfile::tempdir()
                            .map_err(|e| format!("failed to create temp dir: {e}"))?;
                        let work_db = td.path().join("work.db");
                        copy_db_with_sidecars(&golden_path, &work_db)?;

                        let oplog = resolve_workload(preset, fixture_id, c)?;

                        if engine_name == "sqlite3" {
                            run_oplog_sqlite(&work_db, &oplog, &sqlite_cfg)
                                .map_err(|e| format!("{e}"))
                        } else {
                            run_oplog_fsqlite(&work_db, &oplog, &fsqlite_cfg)
                                .map_err(|e| format!("{e}"))
                        }
                    });

                    any_iteration_error |= summary.iterations.iter().any(|it| it.error.is_some());

                    let line = if pretty {
                        summary
                            .to_pretty_json()
                            .map_err(|e| format!("serialize benchmark: {e}"))
                    } else {
                        summary
                            .to_jsonl()
                            .map_err(|e| format!("serialize benchmark: {e}"))
                    };

                    let text = match line {
                        Ok(t) => t,
                        Err(e) => {
                            eprintln!("error: {e}");
                            return 1;
                        }
                    };

                    if let Some(ref path) = output_jsonl {
                        let compact = match summary.to_jsonl() {
                            Ok(t) => t,
                            Err(e) => {
                                eprintln!(
                                    "error: failed to serialize benchmark for JSONL output: {e}"
                                );
                                return 1;
                            }
                        };
                        if let Err(e) = append_jsonl_line(path, &compact) {
                            eprintln!("error: failed to append JSONL output: {e}");
                            return 1;
                        }
                    }

                    println!("{text}");
                    summaries.push(summary);
                }
            }
        }
    }

    if let Some(path) = output_md.as_deref() {
        let md = render_benchmark_summaries_markdown(&summaries);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(e) = fs::create_dir_all(parent) {
                    eprintln!(
                        "error: failed to create output directory {}: {e}",
                        parent.display()
                    );
                    return 1;
                }
            }
        }
        if let Err(e) = fs::write(path, md.as_bytes()) {
            eprintln!(
                "error: failed to write markdown report {}: {e}",
                path.display()
            );
            return 1;
        }
        eprintln!("Wrote markdown report: {}", path.display());
    }

    i32::from(any_iteration_error)
}

fn print_bench_help() {
    let text = "\
realdb-e2e bench — Run the comparative benchmark matrix

USAGE:
    realdb-e2e bench [OPTIONS]

OPTIONS:
    --golden-dir <DIR>      Golden directory (default: sample_sqlite_db_files/golden)
    --db <DB_ID>            Database fixture id, or comma-separated list (default: all)
    --preset <NAME>         Workload preset, or comma-separated list (default: all)
    --concurrency <N|LIST>  Concurrency levels (default: 1,2,4,8)
    --engine <NAME>         sqlite3 | fsqlite | both (default: both)
    --mvcc                  For fsqlite: enable MVCC concurrent_mode
    --warmup <N>            Warmup iterations discarded (default: methodology default)
    --repeat <N>            Exact measurement iterations (sets --min-iters=N and --time-secs=0)
    --min-iters <N>         Minimum measurement iterations (default: methodology default)
    --time-secs <N>         Measurement time floor in seconds (default: methodology default)
    --output <PATH>         Alias for --output-jsonl
    --output-jsonl <PATH>   Append compact JSONL BenchmarkSummary records to PATH
    --output-md <PATH>      Write a Markdown report to PATH (rendered from summaries)
    --pretty                Pretty-print JSON to stdout (default: JSONL)
    -h, --help              Show this help message
";
    let _ = io::stdout().write_all(text.as_bytes());
}

// ── corrupt ─────────────────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
fn cmd_corrupt(argv: &[String]) -> i32 {
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        print_corrupt_help();
        return 0;
    }

    let mut golden_dir = PathBuf::from(DEFAULT_GOLDEN_DIR);
    let mut working_base = PathBuf::from(DEFAULT_WORKING_DIR);

    let mut db: Option<String> = None;
    let mut strategy: Option<String> = None;
    let mut seed: u64 = 0;
    let mut count: usize = 1;
    let mut offset: Option<usize> = None;
    let mut length: Option<usize> = None;
    let mut page: Option<u32> = None;
    let mut json = false;

    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--golden-dir" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --golden-dir requires a directory path");
                    return 2;
                }
                golden_dir = PathBuf::from(&argv[i]);
            }
            "--working-base" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --working-base requires a directory path");
                    return 2;
                }
                working_base = PathBuf::from(&argv[i]);
            }
            "--db" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --db requires a fixture id");
                    return 2;
                }
                db = Some(argv[i].clone());
            }
            "--strategy" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --strategy requires bitflip|zero|page");
                    return 2;
                }
                strategy = Some(argv[i].clone());
            }
            "--seed" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --seed requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<u64>() else {
                    eprintln!("error: invalid integer for --seed: `{}`", argv[i]);
                    return 2;
                };
                seed = n;
            }
            "--count" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --count requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<usize>() else {
                    eprintln!("error: invalid integer for --count: `{}`", argv[i]);
                    return 2;
                };
                count = n;
            }
            "--offset" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --offset requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<usize>() else {
                    eprintln!("error: invalid integer for --offset: `{}`", argv[i]);
                    return 2;
                };
                offset = Some(n);
            }
            "--length" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --length requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<usize>() else {
                    eprintln!("error: invalid integer for --length: `{}`", argv[i]);
                    return 2;
                };
                length = Some(n);
            }
            "--page" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("error: --page requires an integer");
                    return 2;
                }
                let Ok(n) = argv[i].parse::<u32>() else {
                    eprintln!("error: invalid integer for --page: `{}`", argv[i]);
                    return 2;
                };
                page = Some(n);
            }
            "--json" => json = true,
            other => {
                eprintln!("error: unknown option `{other}`");
                return 2;
            }
        }
        i += 1;
    }

    let Some(db_id) = db.as_deref() else {
        eprintln!("error: --db is required");
        return 2;
    };
    let Some(strategy) = strategy.as_deref() else {
        eprintln!("error: --strategy is required");
        return 2;
    };

    // Create a working workspace containing the selected golden DB.
    let ws_cfg = WorkspaceConfig {
        golden_dir,
        working_base,
    };

    let ws = match create_workspace(&ws_cfg, &[db_id]) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("error: failed to create workspace: {e}");
            return 1;
        }
    };
    let Some(db) = ws.databases.first() else {
        eprintln!("error: workspace contains no databases");
        return 1;
    };

    let work_db = db.db_path.clone();
    let before = match sha256_file(&work_db) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("error: cannot hash working db {}: {e}", work_db.display());
            return 1;
        }
    };

    let (strategy_desc, strat) = match strategy {
        "bitflip" => (
            format!("bitflip(count={count}, seed={seed})"),
            CorruptionStrategy::RandomBitFlip { count },
        ),
        "zero" => {
            let Some(off) = offset else {
                eprintln!("error: zero strategy requires --offset");
                return 2;
            };
            let Some(len) = length else {
                eprintln!("error: zero strategy requires --length");
                return 2;
            };
            (
                format!("zero(offset={off}, length={len})"),
                CorruptionStrategy::ZeroRange {
                    offset: off,
                    length: len,
                },
            )
        }
        "page" => {
            let Some(pg) = page else {
                eprintln!("error: page strategy requires --page");
                return 2;
            };
            (
                format!("page(page_number={pg}, seed={seed})"),
                CorruptionStrategy::PageCorrupt { page_number: pg },
            )
        }
        other => {
            eprintln!("error: unknown strategy `{other}` (expected bitflip|zero|page)");
            return 2;
        }
    };

    if let Err(e) = inject_corruption(&work_db, strat, seed) {
        eprintln!("error: corruption injection failed: {e}");
        return 1;
    }

    let after = match sha256_file(&work_db) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("error: cannot hash corrupted db {}: {e}", work_db.display());
            return 1;
        }
    };

    let report = CorruptReport {
        fixture_id: db_id.to_owned(),
        strategy: strategy_desc,
        workspace_dir: ws.run_dir.display().to_string(),
        db_path: work_db.display().to_string(),
        sha256_before: before,
        sha256_after: after,
    };

    if json {
        match serde_json::to_string_pretty(&report) {
            Ok(text) => println!("{text}"),
            Err(e) => {
                eprintln!("error: failed to serialize report: {e}");
                return 1;
            }
        }
    } else {
        println!("Corruption injected:");
        println!("  fixture: {}", report.fixture_id);
        println!("  strategy: {}", report.strategy);
        println!("  workspace: {}", report.workspace_dir);
        println!("  db: {}", report.db_path);
        println!("  sha256(before): {}", report.sha256_before);
        println!("  sha256(after):  {}", report.sha256_after);
    }

    // Ensure the corruption actually changed bytes (sanity).
    i32::from(report.sha256_before == report.sha256_after)
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
    --golden-dir <DIR>      Golden directory (default: sample_sqlite_db_files/golden)
    --working-base <DIR>    Base directory for working copies
                            (default: sample_sqlite_db_files/working)
    --db <DB_ID>            Database fixture to corrupt (copied from golden/)
    --strategy <STRATEGY>   Corruption strategy (bitflip|zero|page)
    --seed <N>              RNG seed for deterministic corruption (default: 0)
    --count <N>             Number of bits to flip (bitflip strategy)
    --offset <N>            Byte offset (zero strategy)
    --length <N>            Byte count (zero strategy)
    --page <N>              Page number to corrupt (page strategy)
    --json                  Output a structured JSON report
    -h, --help              Show this help message
";
    let _ = io::stdout().write_all(text.as_bytes());
}

// ── Types: corpus import metadata + corrupt report ─────────────────────

#[derive(Debug, Serialize)]
struct CorruptReport {
    fixture_id: String,
    strategy: String,
    workspace_dir: String,
    db_path: String,
    sha256_before: String,
    sha256_after: String,
}

/// Profile of a single SQLite database for metadata JSON.
#[derive(Debug, Serialize)]
struct DbProfile {
    name: String,
    source_path: Option<String>,
    tag: Option<String>,
    discovery_tags: Vec<String>,
    file_size_bytes: u64,
    page_size: u32,
    page_count: u32,
    freelist_count: u32,
    schema_version: u32,
    journal_mode: String,
    user_version: u32,
    application_id: u32,
    tables: Vec<TableProfile>,
    indices: Vec<String>,
    triggers: Vec<String>,
    views: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TableProfile {
    name: String,
    row_count: u64,
    columns: Vec<ColumnProfile>,
}

#[derive(Debug, Serialize)]
struct ColumnProfile {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
    primary_key: bool,
    not_null: bool,
    default_value: Option<String>,
}

fn profile_database_for_metadata(
    db_path: &Path,
    fixture_id: &str,
    source_path: Option<&Path>,
    tag: Option<&str>,
    discovery_tags: &[String],
) -> Result<DbProfile, String> {
    let meta =
        fs::metadata(db_path).map_err(|e| format!("cannot stat {}: {e}", db_path.display()))?;

    let conn = Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| format!("cannot open {}: {e}", db_path.display()))?;

    let page_size: u32 = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .map_err(|e| format!("PRAGMA page_size: {e}"))?;
    let page_count: u32 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .map_err(|e| format!("PRAGMA page_count: {e}"))?;
    let freelist_count: u32 = conn
        .query_row("PRAGMA freelist_count", [], |r| r.get(0))
        .map_err(|e| format!("PRAGMA freelist_count: {e}"))?;
    let schema_version: u32 = conn
        .query_row("PRAGMA schema_version", [], |r| r.get(0))
        .map_err(|e| format!("PRAGMA schema_version: {e}"))?;
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .map_err(|e| format!("PRAGMA journal_mode: {e}"))?;
    let user_version: u32 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(|e| format!("PRAGMA user_version: {e}"))?;
    let application_id: u32 = conn
        .query_row("PRAGMA application_id", [], |r| r.get(0))
        .map_err(|e| format!("PRAGMA application_id: {e}"))?;

    let tables = collect_tables(&conn)?;
    let indices = collect_names(&conn, "index")?;
    let triggers = collect_names(&conn, "trigger")?;
    let views = collect_names(&conn, "view")?;

    Ok(DbProfile {
        name: fixture_id.to_owned(),
        source_path: source_path.map(|p| p.to_string_lossy().into_owned()),
        tag: tag.map(str::to_owned),
        discovery_tags: discovery_tags.to_vec(),
        file_size_bytes: meta.len(),
        page_size,
        page_count,
        freelist_count,
        schema_version,
        journal_mode,
        user_version,
        application_id,
        tables,
        indices,
        triggers,
        views,
    })
}

fn collect_names(conn: &Connection, ty: &str) -> Result<Vec<String>, String> {
    let sql = format!("SELECT name FROM sqlite_master WHERE type='{ty}' ORDER BY name");
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("sqlite_master({ty}) prepare: {e}"))?;
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| format!("sqlite_master({ty}) query: {e}"))?;
    Ok(rows.flatten().collect())
}

fn collect_tables(conn: &Connection) -> Result<Vec<TableProfile>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE type='table' AND name NOT LIKE 'sqlite_%' \
             ORDER BY name",
        )
        .map_err(|e| format!("sqlite_master(table) prepare: {e}"))?;
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| format!("sqlite_master(table) query: {e}"))?;

    let mut out: Vec<TableProfile> = Vec::new();
    for row in rows {
        let Ok(table) = row else { continue };
        let cols = collect_table_columns(conn, &table)?;
        let row_count = count_rows(conn, &table)?;
        out.push(TableProfile {
            name: table,
            row_count,
            columns: cols,
        });
    }
    Ok(out)
}

fn collect_table_columns(conn: &Connection, table: &str) -> Result<Vec<ColumnProfile>, String> {
    let sql = format!("PRAGMA table_info({})", quote_ident(table));
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("PRAGMA table_info({table}) prepare: {e}"))?;

    let mut cols = Vec::new();
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("PRAGMA table_info({table}) query: {e}"))?;

    while let Some(r) = rows
        .next()
        .map_err(|e| format!("PRAGMA table_info({table}) next: {e}"))?
    {
        let name: String = r.get(1).map_err(|e| format!("col.name: {e}"))?;
        let col_type: String = r.get(2).map_err(|e| format!("col.type: {e}"))?;
        let not_null_raw: i32 = r.get(3).map_err(|e| format!("col.not_null flag: {e}"))?;
        let not_null: bool = not_null_raw != 0;
        let default_value: Option<String> =
            r.get(4).map_err(|e| format!("col.default_value: {e}"))?;
        let primary_key_raw: i32 = r.get(5).map_err(|e| format!("col.pk flag: {e}"))?;
        let primary_key: bool = primary_key_raw != 0;
        cols.push(ColumnProfile {
            name,
            col_type,
            primary_key,
            not_null,
            default_value,
        });
    }

    Ok(cols)
}

fn count_rows(conn: &Connection, table: &str) -> Result<u64, String> {
    let sql = format!("SELECT count(*) FROM {}", quote_ident(table));
    conn.query_row(&sql, [], |r| r.get::<_, u64>(0))
        .map_err(|e| format!("count_rows({table}): {e}"))
}

fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn cargo_profile_name() -> &'static str {
    if cfg!(debug_assertions) {
        "dev"
    } else {
        "release"
    }
}

fn sanitize_db_id(raw: &str) -> Result<String, &'static str> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("empty");
    }
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        let c = ch.to_ascii_lowercase();
        if c.is_ascii_alphanumeric() {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    // Trim underscores.
    let trimmed = out.trim_matches('_').to_owned();
    if trimmed.is_empty() {
        Err("no usable characters after sanitization")
    } else {
        Ok(trimmed)
    }
}

fn resolve_source_db(
    db_arg: &str,
    root: &Path,
    max_depth: usize,
) -> Result<(PathBuf, Vec<String>, bool), String> {
    let as_path = PathBuf::from(db_arg);
    if as_path.exists() {
        let header_ok =
            sqlite_magic_header_ok(&as_path).map_err(|e| format!("header check failed: {e}"))?;
        return Ok((as_path, Vec::new(), header_ok));
    }

    let config = fsqlite_harness::fixture_discovery::DiscoveryConfig {
        roots: vec![root.to_path_buf()],
        max_depth,
        ..fsqlite_harness::fixture_discovery::DiscoveryConfig::default()
    };

    let candidates = fsqlite_harness::fixture_discovery::discover_sqlite_files(&config)
        .map_err(|e| format!("discovery scan failed: {e}"))?;

    let mut matches = Vec::new();
    for c in candidates {
        let filename = c.path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let stem = c.path.file_stem().and_then(|n| n.to_str()).unwrap_or("");

        if filename == db_arg || stem == db_arg {
            matches.push(c);
        }
    }

    if matches.is_empty() {
        return Err(format!(
            "cannot resolve `{db_arg}`. Provide a literal path, or run `realdb-e2e corpus scan` and pass an exact filename/stem."
        ));
    }
    if matches.len() > 1 {
        eprintln!("error: `{db_arg}` is ambiguous; matches:");
        for m in &matches {
            eprintln!("  {m}");
        }
        return Err("ambiguous discovery name".to_owned());
    }

    let chosen = matches.remove(0);
    Ok((chosen.path, chosen.tags, chosen.header_ok))
}

fn sqlite_magic_header_ok(path: &Path) -> io::Result<bool> {
    use std::io::Read as _;
    const MAGIC: &[u8; 16] = b"SQLite format 3\0";
    let mut f = std::fs::File::open(path)?;
    let mut buf = [0u8; 16];
    if f.read_exact(&mut buf).is_err() {
        return Ok(false);
    }
    Ok(&buf == MAGIC)
}

fn backup_sqlite_file(src: &Path, dst: &Path) -> Result<(), String> {
    let src_conn = Connection::open_with_flags(src, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| format!("cannot open source DB {} (read-only): {e}", src.display()))?;

    // Uses SQLite backup API (same semantics as `sqlite3 "$SRC" ".backup '$DST'"`).
    src_conn
        .backup(DatabaseName::Main, dst, None)
        .map_err(|e| format!("sqlite backup API failed: {e}"))
}

fn sqlite_integrity_check(db: &Path) -> Result<(), String> {
    let conn = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| format!("cannot open {} for integrity_check: {e}", db.display()))?;

    let mut stmt = conn
        .prepare("PRAGMA integrity_check;")
        .map_err(|e| format!("prepare integrity_check: {e}"))?;

    let mut rows = stmt
        .query([])
        .map_err(|e| format!("query integrity_check: {e}"))?;

    let mut lines: Vec<String> = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("read integrity_check row: {e}"))?
    {
        let msg: String = row.get(0).map_err(|e| format!("read row text: {e}"))?;
        lines.push(msg);
    }

    if lines.len() == 1 && lines[0].trim() == "ok" {
        return Ok(());
    }

    let mut out = String::new();
    for l in &lines {
        let _ = writeln!(out, "{l}");
    }
    Err(format!(
        "integrity_check reported {} line(s):\n{out}",
        lines.len()
    ))
}

fn copy_sidecars(src_db: &Path, dest_db: &Path) -> Result<Vec<PathBuf>, String> {
    const SIDECARS: [&str; 3] = ["-wal", "-shm", "-journal"];
    let mut copied = Vec::new();

    for suffix in SIDECARS {
        let mut src_os = src_db.as_os_str().to_os_string();
        src_os.push(suffix);
        let src = PathBuf::from(src_os);
        if !src.exists() {
            continue;
        }

        let mut dest_os = dest_db.as_os_str().to_os_string();
        dest_os.push(suffix);
        let dest = PathBuf::from(dest_os);

        if dest.exists() {
            // Idempotent: skip if already present.
            copied.push(dest);
            continue;
        }

        fs::copy(&src, &dest).map_err(|e| {
            format!(
                "failed to copy sidecar {} -> {}: {e}",
                src.display(),
                dest.display()
            )
        })?;
        copied.push(dest);
    }

    Ok(copied)
}

fn copy_db_with_sidecars(src_db: &Path, dest_db: &Path) -> Result<(), String> {
    fs::copy(src_db, dest_db).map_err(|e| {
        format!(
            "failed to copy {} -> {}: {e}",
            src_db.display(),
            dest_db.display()
        )
    })?;
    let _ = copy_sidecars(src_db, dest_db)?;
    Ok(())
}

fn upsert_checksum(
    checksums_path: &Path,
    golden_db: &Path,
    sha256_hex: &str,
) -> Result<(), String> {
    let filename = golden_db
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("golden db has no filename")?
        .to_owned();

    let mut lines: Vec<(String, String)> = Vec::new();
    if checksums_path.exists() {
        let contents = fs::read_to_string(checksums_path)
            .map_err(|e| format!("cannot read {}: {e}", checksums_path.display()))?;
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Some((hex, name)) = line.split_once("  ") else {
                continue;
            };
            lines.push((name.trim().to_owned(), hex.trim().to_owned()));
        }
    }

    for (name, hex) in &lines {
        if name == &filename {
            if hex == sha256_hex {
                // Idempotent: already recorded.
                return Ok(());
            }
            return Err(format!(
                "{} already contains an entry for {filename} with a different sha256.\n\
Refusing to overwrite provenance. Golden files are immutable; ingest under a new --id instead.\n\
existing: {hex}\n\
current:  {sha256_hex}",
                checksums_path.display()
            ));
        }
    }

    lines.push((filename, sha256_hex.to_owned()));
    lines.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out = String::new();
    for (name, hex) in &lines {
        let _ = writeln!(out, "{hex}  {name}");
    }
    fs::write(checksums_path, out.as_bytes())
        .map_err(|e| format!("cannot write {}: {e}", checksums_path.display()))?;

    Ok(())
}

fn discover_golden_fixture_ids(golden_dir: &Path) -> Result<Vec<String>, String> {
    let mut ids = Vec::new();
    let entries = fs::read_dir(golden_dir)
        .map_err(|e| format!("cannot read golden dir {}: {e}", golden_dir.display()))?;
    for entry in entries {
        let Ok(entry) = entry else { continue };
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("db") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if !stem.is_empty() {
                    ids.push(stem.to_owned());
                }
            }
        }
    }
    ids.sort();
    Ok(ids)
}

fn resolve_golden_db_in(golden_dir: &Path, db_name: &str) -> Result<PathBuf, String> {
    // If it looks like a path and exists, use it directly.
    let as_path = PathBuf::from(db_name);
    if as_path.exists() {
        return Ok(as_path);
    }

    // Try golden directory with .db extension.
    let golden = golden_dir.join(format!("{db_name}.db"));
    if golden.exists() {
        return Ok(golden);
    }

    // Try golden directory without adding .db (user may have included it).
    let golden_bare = golden_dir.join(db_name);
    if golden_bare.exists() {
        return Ok(golden_bare);
    }

    Err(format!(
        "cannot find database `{db_name}` (tried {}, {}, and literal path)",
        golden.display(),
        golden_bare.display(),
    ))
}

#[cfg(unix)]
fn set_read_only(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path)
        .map_err(|e| format!("cannot stat {}: {e}", path.display()))?
        .permissions();
    perms.set_mode(0o444);
    fs::set_permissions(path, perms)
        .map_err(|e| format!("cannot chmod {}: {e}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_read_only(_path: &Path) -> Result<(), String> {
    Ok(())
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
    fn parse_u16_list_single_and_list() {
        assert_eq!(parse_u16_list("1").unwrap(), vec![1]);
        assert_eq!(parse_u16_list("1,2,4,8,16").unwrap(), vec![1, 2, 4, 8, 16]);
        assert!(parse_u16_list("").is_err());
        assert!(parse_u16_list("1,").is_err());
        assert!(parse_u16_list("nope").is_err());
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
