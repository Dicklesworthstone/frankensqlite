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
use std::io::{self, Write};

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

fn cmd_corpus_verify(_argv: &[String]) -> i32 {
    println!("corpus verify: not yet implemented (see bd-19iw)");
    0
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

    println!(
        "run: engine={} db={} workload={} concurrency={concurrency}",
        engine.as_deref().unwrap_or("(not set)"),
        db.as_deref().unwrap_or("(not set)"),
        workload.as_deref().unwrap_or("(not set)"),
    );
    println!("(executor not yet implemented — see bd-1w6k.3.2, bd-1w6k.3.3)");
    0
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
        assert_eq!(
            run_with(&[
                "realdb-e2e",
                "run",
                "--engine",
                "sqlite3",
                "--db",
                "test-db",
                "--workload",
                "hot_page_contention",
                "--concurrency",
                "8",
            ]),
            0
        );
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
}
