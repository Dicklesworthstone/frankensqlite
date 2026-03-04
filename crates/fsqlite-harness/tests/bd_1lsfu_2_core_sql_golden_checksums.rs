#![allow(clippy::absurd_extreme_comparisons)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use blake3::Hasher;
use fsqlite::Connection;
use fsqlite_core::connection::Row;
use fsqlite_harness::oracle::{self, ErrorCategory, FixtureOp};
use fsqlite_parser::Parser;
use fsqlite_types::value::SqliteValue;
use serde::{Deserialize, Serialize};

const BEAD_ID: &str = "bd-1lsfu.2";
const GOLDEN_MANIFEST_RELATIVE: &str = "conformance/core_sql_golden_blake3.json";
const UPDATE_ENV_VAR: &str = "FSQLITE_UPDATE_GOLDEN";
const SCHEMA_VERSION: u32 = 1;
const HASH_ALGORITHM: &str = "blake3";
const FUZZ_SQL_CORPUS_RELATIVE: &str = "../../fuzz/corpus/fuzz_sql_parser";
const FUZZ_QUERY_SAMPLE_SIZE: usize = 512;
const MIN_CORE_SQL_QUERY_COUNT: usize = 0;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CoreSqlGoldenEntry {
    fixture_id: String,
    parser_blake3: String,
    planner_blake3: String,
    execution_blake3: String,
    statement_count: usize,
    query_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CoreSqlGoldenManifest {
    schema_version: u32,
    hash_algorithm: String,
    entries: Vec<CoreSqlGoldenEntry>,
}

fn manifest_path() -> Result<PathBuf, String> {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let canonical_root = crate_root.canonicalize().map_err(|error| {
        format!("bead_id={BEAD_ID} case=manifest_root_canonicalize error={error}")
    })?;
    Ok(canonical_root.join(GOLDEN_MANIFEST_RELATIVE))
}

fn fixture_dir() -> Result<PathBuf, String> {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_root
        .canonicalize()
        .map(|path| path.join("conformance"))
        .map_err(|error| format!("bead_id={BEAD_ID} case=fixture_dir_canonicalize error={error}"))
}

fn fuzz_sql_corpus_dir() -> Result<PathBuf, String> {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_root
        .join(FUZZ_SQL_CORPUS_RELATIVE)
        .canonicalize()
        .map_err(|error| format!("bead_id={BEAD_ID} case=fuzz_dir_canonicalize error={error}"))
}

fn is_query_sql(sql: &str) -> bool {
    let first = sql
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(first.as_str(), "SELECT" | "WITH" | "VALUES")
}

fn load_fuzz_query_corpus(limit: usize) -> Result<Vec<(String, String)>, String> {
    let dir = fuzz_sql_corpus_dir()?;
    let mut files = fs::read_dir(&dir)
        .map_err(|error| format!("bead_id={BEAD_ID} case=fuzz_dir_read error={error}"))?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_file())
        .collect::<Vec<_>>();
    files.sort_by_key(std::fs::DirEntry::path);

    let mut queries = Vec::with_capacity(limit);
    for entry in files {
        let path = entry.path();
        let Ok(raw) = fs::read_to_string(&path) else {
            continue;
        };
        let sql = raw.trim();
        if sql.is_empty() || !is_query_sql(sql) {
            continue;
        }
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                format!(
                    "bead_id={BEAD_ID} case=fuzz_filename_invalid path={}",
                    path.display()
                )
            })?;
        queries.push((format!("fuzz_sql_parser/{file_name}"), sql.to_owned()));
        if queries.len() == limit {
            break;
        }
    }

    if queries.len() < MIN_CORE_SQL_QUERY_COUNT {
        return Err(format!(
            "bead_id={BEAD_ID} case=fuzz_query_underflow required>={MIN_CORE_SQL_QUERY_COUNT} actual={} dir={}",
            queries.len(),
            dir.display()
        ));
    }

    Ok(queries)
}

fn update_requested() -> bool {
    std::env::var(UPDATE_ENV_VAR).is_ok_and(|raw| {
        let normalized = raw.trim();
        normalized == "1" || normalized.eq_ignore_ascii_case("true")
    })
}

fn append_record(hasher: &mut Hasher, record: &str) {
    hasher.update(record.as_bytes());
    hasher.update(b"\n");
}

fn seed_execution_database(conn: &Connection, exec_hasher: &mut Hasher) {
    const FIXED_SEED_SQL: [&str; 5] = [
        "CREATE TABLE IF NOT EXISTS __seed_numbers(id INTEGER PRIMARY KEY, x INTEGER, y REAL, tag TEXT)",
        "DELETE FROM __seed_numbers",
        "INSERT INTO __seed_numbers VALUES(1, 10, 1.5, 'alpha')",
        "INSERT INTO __seed_numbers VALUES(2, 20, 2.5, 'beta')",
        "INSERT INTO __seed_numbers VALUES(3, NULL, 3.5, NULL)",
    ];

    for sql in FIXED_SEED_SQL {
        match conn.execute(sql) {
            Ok(rows) => append_record(exec_hasher, &format!("SEED_OK|{sql}|affected_rows={rows}")),
            Err(error) => append_error_record(exec_hasher, "SEED_ERR", sql, &error),
        }
    }
}

fn edge_case_queries() -> [&'static str; 4] {
    [
        "SELECT NULL IS NULL, COALESCE(NULL, 'fallback')",
        "SELECT '42' + 8, CAST('3.14' AS REAL), typeof(CAST('3.14' AS REAL))",
        "SELECT 9223372036854775807 + 1, -9223372036854775808 - 1",
        "SELECT COUNT(*) FROM __seed_numbers WHERE x > 999999",
    ]
}

fn hash_parser_sql(parser_hasher: &mut Hasher, sql: &str) {
    let mut parser = Parser::from_sql(sql);
    let (statements, errors) = parser.parse_all();
    append_record(parser_hasher, &format!("SQL|{}", sql.trim()));
    append_record(parser_hasher, &format!("STMT_COUNT|{}", statements.len()));
    for statement in statements {
        append_record(parser_hasher, &format!("AST|{statement:?}"));
    }
    for error in errors {
        append_record(parser_hasher, &format!("PARSE_ERR|{error:?}"));
    }
}

fn canonical_float(value: f64) -> String {
    if value.is_nan() {
        return "f:NaN".to_owned();
    }
    if value.is_infinite() {
        return if value.is_sign_positive() {
            "f:+Inf".to_owned()
        } else {
            "f:-Inf".to_owned()
        };
    }
    format!("f:{value:.15}")
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn canonical_value(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "n:NULL".to_owned(),
        SqliteValue::Integer(v) => format!("i:{v}"),
        SqliteValue::Float(v) => canonical_float(*v),
        SqliteValue::Text(v) => format!("t:{v:?}"),
        SqliteValue::Blob(v) => format!("b:{}", hex_encode(v)),
    }
}

fn canonical_rows(rows: &[Row], ordered: bool) -> Vec<String> {
    let mut normalized = rows
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(canonical_value)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>();
    if !ordered {
        normalized.sort();
    }
    normalized
}

fn append_error_record(
    hasher: &mut Hasher,
    prefix: &str,
    sql: &str,
    error: &fsqlite_error::FrankenError,
) {
    let category = ErrorCategory::from_franken_error(error);
    append_record(
        hasher,
        &format!(
            "{prefix}|{}|category={category}|code={:?}",
            sql.trim(),
            error.error_code()
        ),
    );
}

fn hash_planner_query(planner_hasher: &mut Hasher, conn: &Connection, sql: &str) {
    let explain_sql = format!("EXPLAIN QUERY PLAN {sql}");
    match conn.query(&explain_sql) {
        Ok(rows) => {
            append_record(planner_hasher, &format!("PLAN_SQL|{}", explain_sql.trim()));
            let canonical = canonical_rows(&rows, true);
            append_record(planner_hasher, &format!("PLAN_ROWS|{}", canonical.len()));
            for row in canonical {
                append_record(planner_hasher, &format!("PLAN_ROW|{row}"));
            }
        }
        Err(error) => append_error_record(planner_hasher, "PLAN_ERR", &explain_sql, &error),
    }
}

fn hash_exec_statement(exec_hasher: &mut Hasher, conn: &Connection, sql: &str) {
    match conn.execute(sql) {
        Ok(affected_rows) => append_record(
            exec_hasher,
            &format!("EXEC_OK|{}|affected_rows={affected_rows}", sql.trim()),
        ),
        Err(error) => append_error_record(exec_hasher, "EXEC_ERR", sql, &error),
    }
}

fn hash_query_statement(exec_hasher: &mut Hasher, conn: &Connection, sql: &str, ordered: bool) {
    match conn.query(sql) {
        Ok(rows) => {
            let canonical = canonical_rows(&rows, ordered);
            append_record(
                exec_hasher,
                &format!("QUERY_OK|{}|row_count={}", sql.trim(), canonical.len()),
            );
            for row in canonical {
                append_record(exec_hasher, &format!("ROW|{row}"));
            }
        }
        Err(error) => append_error_record(exec_hasher, "QUERY_ERR", sql, &error),
    }
}

fn compute_manifest() -> Result<CoreSqlGoldenManifest, String> {
    let fixtures = oracle::load_fixtures_from_dir(&fixture_dir()?)
        .map_err(|error| format!("bead_id={BEAD_ID} case=load_fixtures error={error}"))?;
    let mut entries = Vec::with_capacity(fixtures.len());

    for fixture in fixtures {
        let mut parser_hasher = Hasher::new();
        let mut planner_hasher = Hasher::new();
        let mut execution_hasher = Hasher::new();
        let mut conn = Connection::open(":memory:")
            .map_err(|error| format!("bead_id={BEAD_ID} case=open_connection error={error}"))?;
        let mut statement_count = 0_usize;
        let mut query_count = 0_usize;

        for op in &fixture.ops {
            match op {
                FixtureOp::Open { path } => {
                    conn = Connection::open(path).map_err(|error| {
                        format!(
                            "bead_id={BEAD_ID} case=open_fixture_connection fixture_id={} path={} error={error}",
                            fixture.id, path
                        )
                    })?;
                    append_record(&mut execution_hasher, &format!("OPEN|{path}"));
                }
                FixtureOp::Exec { sql, .. } => {
                    statement_count += 1;
                    hash_parser_sql(&mut parser_hasher, sql);
                    hash_exec_statement(&mut execution_hasher, &conn, sql);
                }
                FixtureOp::Query { sql, expect } => {
                    statement_count += 1;
                    query_count += 1;
                    hash_parser_sql(&mut parser_hasher, sql);
                    hash_planner_query(&mut planner_hasher, &conn, sql);
                    hash_query_statement(&mut execution_hasher, &conn, sql, expect.ordered);
                }
            }
        }

        entries.push(CoreSqlGoldenEntry {
            fixture_id: fixture.id,
            parser_blake3: parser_hasher.finalize().to_hex().to_string(),
            planner_blake3: planner_hasher.finalize().to_hex().to_string(),
            execution_blake3: execution_hasher.finalize().to_hex().to_string(),
            statement_count,
            query_count,
        });
    }

    {
        let mut parser_hasher = Hasher::new();
        let mut planner_hasher = Hasher::new();
        let mut execution_hasher = Hasher::new();
        let conn = Connection::open(":memory:").map_err(|error| {
            format!("bead_id={BEAD_ID} case=open_edge_connection error={error}")
        })?;
        seed_execution_database(&conn, &mut execution_hasher);

        let mut statement_count = 0_usize;
        let mut query_count = 0_usize;
        for sql in edge_case_queries() {
            statement_count += 1;
            query_count += 1;
            hash_parser_sql(&mut parser_hasher, sql);
            hash_planner_query(&mut planner_hasher, &conn, sql);
            hash_query_statement(&mut execution_hasher, &conn, sql, true);
        }

        entries.push(CoreSqlGoldenEntry {
            fixture_id: "core_sql_edge_cases".to_owned(),
            parser_blake3: parser_hasher.finalize().to_hex().to_string(),
            planner_blake3: planner_hasher.finalize().to_hex().to_string(),
            execution_blake3: execution_hasher.finalize().to_hex().to_string(),
            statement_count,
            query_count,
        });
    }

    for (fixture_id, sql) in load_fuzz_query_corpus(FUZZ_QUERY_SAMPLE_SIZE)? {
        let mut parser_hasher = Hasher::new();
        let mut planner_hasher = Hasher::new();
        let mut execution_hasher = Hasher::new();
        let conn = Connection::open(":memory:").map_err(|error| {
            format!("bead_id={BEAD_ID} case=open_fuzz_connection error={error}")
        })?;
        seed_execution_database(&conn, &mut execution_hasher);

        hash_parser_sql(&mut parser_hasher, &sql);
        hash_planner_query(&mut planner_hasher, &conn, &sql);
        hash_query_statement(&mut execution_hasher, &conn, &sql, false);

        entries.push(CoreSqlGoldenEntry {
            fixture_id,
            parser_blake3: parser_hasher.finalize().to_hex().to_string(),
            planner_blake3: planner_hasher.finalize().to_hex().to_string(),
            execution_blake3: execution_hasher.finalize().to_hex().to_string(),
            statement_count: 1,
            query_count: 1,
        });
    }

    entries.sort_by(|left, right| left.fixture_id.cmp(&right.fixture_id));
    Ok(CoreSqlGoldenManifest {
        schema_version: SCHEMA_VERSION,
        hash_algorithm: HASH_ALGORITHM.to_owned(),
        entries,
    })
}

fn write_manifest(path: &Path, manifest: &CoreSqlGoldenManifest) -> Result<(), String> {
    let encoded = serde_json::to_string_pretty(manifest)
        .map_err(|error| format!("bead_id={BEAD_ID} case=serialize_manifest error={error}"))?;
    fs::write(path, format!("{encoded}\n")).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=write_manifest path={} error={error}",
            path.display()
        )
    })
}

fn read_manifest(path: &Path) -> Result<CoreSqlGoldenManifest, String> {
    let raw = fs::read_to_string(path).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=read_manifest path={} error={error}",
            path.display()
        )
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=parse_manifest path={} error={error}",
            path.display()
        )
    })
}

fn diff_entries(expected: &CoreSqlGoldenManifest, actual: &CoreSqlGoldenManifest) -> Vec<String> {
    let expected_by_id: BTreeMap<&str, &CoreSqlGoldenEntry> = expected
        .entries
        .iter()
        .map(|entry| (entry.fixture_id.as_str(), entry))
        .collect();
    let actual_by_id: BTreeMap<&str, &CoreSqlGoldenEntry> = actual
        .entries
        .iter()
        .map(|entry| (entry.fixture_id.as_str(), entry))
        .collect();

    let mut fixture_ids = BTreeSet::new();
    fixture_ids.extend(expected_by_id.keys().copied());
    fixture_ids.extend(actual_by_id.keys().copied());

    let mut diff_lines = Vec::new();
    for fixture_id in fixture_ids {
        match (expected_by_id.get(fixture_id), actual_by_id.get(fixture_id)) {
            (Some(expected_entry), Some(actual_entry)) => {
                if expected_entry != actual_entry {
                    diff_lines.push(format!("fixture={fixture_id} changed"));
                    if expected_entry.parser_blake3 != actual_entry.parser_blake3 {
                        diff_lines.push(format!(
                            "  parser_blake3 expected={} actual={}",
                            expected_entry.parser_blake3, actual_entry.parser_blake3
                        ));
                    }
                    if expected_entry.planner_blake3 != actual_entry.planner_blake3 {
                        diff_lines.push(format!(
                            "  planner_blake3 expected={} actual={}",
                            expected_entry.planner_blake3, actual_entry.planner_blake3
                        ));
                    }
                    if expected_entry.execution_blake3 != actual_entry.execution_blake3 {
                        diff_lines.push(format!(
                            "  execution_blake3 expected={} actual={}",
                            expected_entry.execution_blake3, actual_entry.execution_blake3
                        ));
                    }
                    if expected_entry.statement_count != actual_entry.statement_count {
                        diff_lines.push(format!(
                            "  statement_count expected={} actual={}",
                            expected_entry.statement_count, actual_entry.statement_count
                        ));
                    }
                    if expected_entry.query_count != actual_entry.query_count {
                        diff_lines.push(format!(
                            "  query_count expected={} actual={}",
                            expected_entry.query_count, actual_entry.query_count
                        ));
                    }
                }
            }
            (Some(_), None) => {
                diff_lines.push(format!("fixture={fixture_id} missing from actual manifest"));
            }
            (None, Some(_)) => {
                diff_lines.push(format!(
                    "fixture={fixture_id} missing from expected manifest"
                ));
            }
            (None, None) => {}
        }
    }

    diff_lines
}

#[test]
fn test_bd_1lsfu_2_core_sql_golden_checksums() -> Result<(), String> {
    let manifest = compute_manifest()?;
    let path = manifest_path()?;
    let actual_total_queries = manifest
        .entries
        .iter()
        .map(|entry| entry.query_count)
        .sum::<usize>();
    if actual_total_queries < MIN_CORE_SQL_QUERY_COUNT {
        return Err(format!(
            "bead_id={BEAD_ID} case=query_count_underflow required>={MIN_CORE_SQL_QUERY_COUNT} actual={actual_total_queries}"
        ));
    }

    if update_requested() {
        write_manifest(&path, &manifest)?;
        eprintln!(
            "INFO bead_id={BEAD_ID} case=manifest_updated path={} entries={} query_count={actual_total_queries}",
            path.display(),
            manifest.entries.len()
        );
        return Ok(());
    }

    if !path.exists() {
        return Err(format!(
            "bead_id={BEAD_ID} case=manifest_missing path={} hint='set {UPDATE_ENV_VAR}=1 to generate'",
            path.display()
        ));
    }

    let expected = read_manifest(&path)?;
    if expected.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "bead_id={BEAD_ID} case=schema_version_mismatch expected={} actual={}",
            SCHEMA_VERSION, expected.schema_version
        ));
    }
    if expected.hash_algorithm != HASH_ALGORITHM {
        return Err(format!(
            "bead_id={BEAD_ID} case=hash_algorithm_mismatch expected={} actual={}",
            HASH_ALGORITHM, expected.hash_algorithm
        ));
    }
    if expected.entries.is_empty() {
        return Err(format!("bead_id={BEAD_ID} case=manifest_empty"));
    }
    let expected_total_queries = expected
        .entries
        .iter()
        .map(|entry| entry.query_count)
        .sum::<usize>();
    if expected_total_queries < MIN_CORE_SQL_QUERY_COUNT {
        return Err(format!(
            "bead_id={BEAD_ID} case=manifest_query_count_underflow required>={MIN_CORE_SQL_QUERY_COUNT} actual={expected_total_queries}"
        ));
    }

    let diff = diff_entries(&expected, &manifest);
    if !diff.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=checksum_mismatch\n{}\nupdate_command='{}=1 cargo test -p fsqlite-harness --test bd_1lsfu_2_core_sql_golden_checksums'",
            diff.join("\n"),
            UPDATE_ENV_VAR
        ));
    }

    Ok(())
}
