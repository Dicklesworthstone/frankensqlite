//! bd-7m1ep — replay of the bd-105ga captured statement stream.
//!
//! # This is a smoke test, NOT the bd-105ga corruption regression guard
//!
//! Read this before trusting a green run here. The stream below was captured
//! from `br` 0.2.16 (beads_rust, which dogfoods fsqlite) while it corrupted a
//! real 3,235-issue beads database, and bd-105ga originally concluded that the
//! defect was bounded to published fsqlite 0.1.12–0.1.18 because this stream
//! "replays clean on main".
//!
//! That conclusion was retracted on 2026-07-26. A control run replayed this
//! exact corpus against the **v0.1.18 tag tree** — the version the defect was
//! attributed to — and got 5,019 statements, 0 errors, engine
//! `integrity_check` ok, stock sqlite3 ok. The corpus does not corrupt the
//! version it is supposed to corrupt, so a passing run here says nothing about
//! whether the corruption defect is present.
//!
//! The reason is now known: this stream is an **open-only** capture (br's
//! open-time export flush) taken while chasing a `br list --limit 1`
//! hypothesis, and that was the wrong operation. The corruption reproduces
//! deterministically via `br sync --merge --force`, which performs a real
//! three-way merge writing across `issues`, `dependencies` and `labels` —
//! which is why the damage lands in Tree 2 *and* Tree 8, something an
//! `export_hashes`-only flush could never produce.
//!
//! So what this test is actually worth: it drives ~5k real statements
//! (1,024 `REPLACE INTO` upserts inside one `BEGIN IMMEDIATE` … `ROLLBACK`)
//! against an 18 MB real-world database and checks both the engine's own
//! integrity verdict and C SQLite's. That is a genuine end-to-end smoke test
//! of a bulk-upsert-then-rollback workload on a non-toy file — the only test
//! here that exercises a multi-megabyte real-world database. It is not
//! evidence about bd-105ga.
//!
//! Note the *database* fixture is not a dead end even though this stream is:
//! `pristine-input.db.gz` is the exact input the merge reproducer corrupts.
//! See `fixtures/bd105ga/README.md` for the reproducer and full results;
//! attribution work is tracked in bd-105ga (reopened) and bd-nhc6g.

use std::io::Read as _;
use std::path::PathBuf;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use sha2::{Digest as _, Sha256};

const STREAM_GZ: &str = "fixtures/bd105ga/corrupting-stream.sqllog.gz";
const PRISTINE_DB_GZ: &str = "fixtures/bd105ga/pristine-input.db.gz";
/// SHA-256 of the decompressed statement stream (5,019 lines).
const STREAM_SHA256: &str = "9f41c9da8cd4594df971ca845452b27b9c9496af4b7ddf74822af4fea26895b4";
/// SHA-256 of the decompressed pristine input database (18 MB).
const PRISTINE_DB_SHA256: &str = "9bc6c17c69d76db6fd0daa31727c980404d97d3c357800a1c26c4dbfcb58a87b";
/// Exact statement count of the captured stream; drift means the corpus
/// no longer matches the bd-105ga capture.
const STREAM_STATEMENTS: usize = 5_019;

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Read a gzipped fixture and verify the decompressed bytes against the
/// pinned digest. The corpus is evidence in an open investigation, so a
/// mismatch is a hard failure rather than a regeneration prompt.
fn fixture_bytes(rel: &str, expected_sha256: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel);
    let gz = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(gz.as_slice())
        .read_to_end(&mut out)
        .unwrap_or_else(|e| panic!("gunzip {}: {e}", path.display()));
    let digest = hex_sha256(&out);
    assert_eq!(
        digest, expected_sha256,
        "fixture {rel}: decompressed sha256 mismatch — corpus bytes were altered"
    );
    out
}

/// Decode one captured parameter list. Format (per line, after the `\x1f`
/// separator): JSON array of `{"t":"n"}` | `{"t":"i","v":i64}` |
/// `{"t":"f","bits":u64}` | `{"t":"s","v":str}` | `{"t":"b","v":[u8...]}`.
fn parse_params(json: &str) -> Result<Vec<SqliteValue>, String> {
    let vals: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("bad params json: {e}"))?;
    let arr = vals.as_array().ok_or("params not an array")?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        let tag = v["t"].as_str().ok_or("missing tag")?;
        out.push(match tag {
            "n" => SqliteValue::Null,
            "i" => SqliteValue::Integer(v["v"].as_i64().ok_or("bad int")?),
            "f" => SqliteValue::Float(f64::from_bits(v["bits"].as_u64().ok_or("bad float bits")?)),
            "s" => SqliteValue::from(v["v"].as_str().ok_or("bad str")?),
            "b" => {
                let bytes = v["v"]
                    .as_array()
                    .ok_or("bad blob")?
                    .iter()
                    .map(|b| {
                        b.as_u64()
                            .and_then(|x| u8::try_from(x).ok())
                            .ok_or("blob byte out of range")
                    })
                    .collect::<Result<Vec<u8>, _>>()?;
                SqliteValue::from(bytes)
            }
            other => return Err(format!("unknown tag {other}")),
        });
    }
    Ok(out)
}

fn load_stream(bytes: &[u8]) -> Vec<(String, Vec<SqliteValue>)> {
    let text = std::str::from_utf8(bytes).expect("stream fixture is UTF-8");
    let mut statements = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let (sql, params_json) = line
            .split_once('\x1f')
            .unwrap_or_else(|| panic!("stream line {idx}: missing \\x1f separator"));
        let params = parse_params(params_json).unwrap_or_else(|e| panic!("stream line {idx}: {e}"));
        statements.push((sql.to_owned(), params));
    }
    statements
}

/// First ~60 chars of a statement for error context, without splitting a
/// UTF-8 code point.
fn head_of(sql: &str) -> String {
    sql.chars().take(60).collect()
}

/// Bulk-upsert-then-rollback smoke test over a real 18 MB database.
///
/// See the module docs: a green run here is NOT evidence that the bd-105ga
/// corruption defect is absent.
#[test]
fn bd105ga_stream_replays_without_error_or_integrity_loss() {
    let stream_bytes = fixture_bytes(STREAM_GZ, STREAM_SHA256);
    let db_bytes = fixture_bytes(PRISTINE_DB_GZ, PRISTINE_DB_SHA256);
    let statements = load_stream(&stream_bytes);
    assert_eq!(
        statements.len(),
        STREAM_STATEMENTS,
        "corpus statement count drifted from the bd-105ga capture"
    );

    let dir = tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir");
    let db_path = dir.path().join("bd105ga-replay.db");
    std::fs::write(&db_path, &db_bytes).expect("write pristine input db");
    let db_str = db_path.to_str().expect("utf-8 temp path").to_owned();

    asupersync::test_utils::run_test(|| {
        let db_str = db_str.clone();
        let statements = statements.clone();
        async move {
            let conn = Connection::open(db_str).await.expect("open pristine db");

            let mut errors: Vec<String> = Vec::new();
            for (i, (sql, params)) in statements.iter().enumerate() {
                let keyword = sql
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .to_ascii_uppercase();
                let result = if matches!(keyword.as_str(), "SELECT" | "PRAGMA" | "EXPLAIN") {
                    conn.query_with_params(sql, params).await.map(drop)
                } else {
                    conn.execute_with_params(sql, params).await.map(drop)
                };
                if let Err(e) = result {
                    errors.push(format!("stmt {i} ({}…): {e}", head_of(sql)));
                }
            }
            assert!(
                errors.is_empty(),
                "replay produced {} statement error(s); first 20:\n{}",
                errors.len(),
                errors
                    .iter()
                    .take(20)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("\n")
            );

            let rows = conn
                .query("PRAGMA integrity_check;")
                .await
                .expect("engine integrity_check");
            let verdicts: Vec<String> = rows
                .iter()
                .map(|row| match row.values().first() {
                    Some(SqliteValue::Text(s)) => s.to_string(),
                    other => format!("{other:?}"),
                })
                .collect();
            assert_eq!(
                verdicts,
                vec!["ok".to_owned()],
                "engine integrity_check reported corruption after replay"
            );

            conn.close().await.expect("close");
        }
    });

    // Independent verdict: C SQLite must also see an intact file. It recovers
    // the standard-format WAL the engine left behind.
    let c_conn = rusqlite::Connection::open(&db_path).expect("rusqlite open replayed db");
    let c_verdict: String = c_conn
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("rusqlite integrity_check");
    assert_eq!(
        c_verdict, "ok",
        "C SQLite integrity_check reported corruption after replay"
    );
}
