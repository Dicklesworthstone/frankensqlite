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
const AUDIT_ENV_VAR: &str = "FSQLITE_AUDIT_GOLDEN";
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
    let raw_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FUZZ_SQL_CORPUS_RELATIVE);
    if !raw_dir.exists() && MIN_CORE_SQL_QUERY_COUNT == 0 {
        return Ok(Vec::new());
    }
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

fn audit_requested() -> bool {
    std::env::var(AUDIT_ENV_VAR).is_ok_and(|raw| {
        let normalized = raw.trim();
        normalized == "1" || normalized.eq_ignore_ascii_case("true")
    })
}

fn append_record(hasher: &mut Hasher, record: &str) {
    hasher.update(record.as_bytes());
    hasher.update(b"\n");
}

async fn seed_execution_database(conn: &Connection, exec_hasher: &mut Hasher) {
    const FIXED_SEED_SQL: [&str; 5] = [
        "CREATE TABLE IF NOT EXISTS __seed_numbers(id INTEGER PRIMARY KEY, x INTEGER, y REAL, tag TEXT)",
        "DELETE FROM __seed_numbers",
        "INSERT INTO __seed_numbers VALUES(1, 10, 1.5, 'alpha')",
        "INSERT INTO __seed_numbers VALUES(2, 20, 2.5, 'beta')",
        "INSERT INTO __seed_numbers VALUES(3, NULL, 3.5, NULL)",
    ];

    for sql in FIXED_SEED_SQL {
        match conn.execute(sql).await {
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

async fn hash_planner_query(planner_hasher: &mut Hasher, conn: &Connection, sql: &str) {
    let explain_sql = format!("EXPLAIN QUERY PLAN {sql}");
    match conn.query(&explain_sql).await {
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

async fn hash_exec_statement(exec_hasher: &mut Hasher, conn: &Connection, sql: &str) {
    match conn.execute(sql).await {
        Ok(affected_rows) => append_record(
            exec_hasher,
            &format!("EXEC_OK|{}|affected_rows={affected_rows}", sql.trim()),
        ),
        Err(error) => append_error_record(exec_hasher, "EXEC_ERR", sql, &error),
    }
}

async fn hash_query_statement(
    exec_hasher: &mut Hasher,
    conn: &Connection,
    sql: &str,
    ordered: bool,
) {
    match conn.query(sql).await {
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

async fn compute_manifest() -> Result<CoreSqlGoldenManifest, String> {
    let fixtures = oracle::load_fixtures_from_dir(&fixture_dir()?)
        .map_err(|error| format!("bead_id={BEAD_ID} case=load_fixtures error={error}"))?;
    let mut entries = Vec::with_capacity(fixtures.len());

    for fixture in fixtures {
        let mut parser_hasher = Hasher::new();
        let mut planner_hasher = Hasher::new();
        let mut execution_hasher = Hasher::new();
        let mut conn = Connection::open(":memory:")
            .await
            .map_err(|error| format!("bead_id={BEAD_ID} case=open_connection error={error}"))?;
        let mut statement_count = 0_usize;
        let mut query_count = 0_usize;

        for op in &fixture.ops {
            match op {
                FixtureOp::Open { path } => {
                    conn = Connection::open(path).await.map_err(|error| {
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
                    hash_exec_statement(&mut execution_hasher, &conn, sql).await;
                }
                FixtureOp::Query { sql, expect } => {
                    statement_count += 1;
                    query_count += 1;
                    hash_parser_sql(&mut parser_hasher, sql);
                    hash_planner_query(&mut planner_hasher, &conn, sql).await;
                    hash_query_statement(&mut execution_hasher, &conn, sql, expect.ordered).await;
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
        let conn = Connection::open(":memory:").await.map_err(|error| {
            format!("bead_id={BEAD_ID} case=open_edge_connection error={error}")
        })?;
        seed_execution_database(&conn, &mut execution_hasher).await;

        let mut statement_count = 0_usize;
        let mut query_count = 0_usize;
        for sql in edge_case_queries() {
            statement_count += 1;
            query_count += 1;
            hash_parser_sql(&mut parser_hasher, sql);
            hash_planner_query(&mut planner_hasher, &conn, sql).await;
            hash_query_statement(&mut execution_hasher, &conn, sql, true).await;
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
        let conn = Connection::open(":memory:").await.map_err(|error| {
            format!("bead_id={BEAD_ID} case=open_fuzz_connection error={error}")
        })?;
        seed_execution_database(&conn, &mut execution_hasher).await;

        hash_parser_sql(&mut parser_hasher, &sql);
        hash_planner_query(&mut planner_hasher, &conn, &sql).await;
        hash_query_statement(&mut execution_hasher, &conn, &sql, false).await;

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
    let mut outcome: Result<(), String> = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let manifest = compute_manifest().await?;
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
        .await;
    });
    outcome
}

fn rusqlite_error_category(err: &rusqlite::Error) -> ErrorCategory {
    if let rusqlite::Error::SqliteFailure(ffi_err, _) = err {
        return match ffi_err.code {
            rusqlite::ErrorCode::DatabaseBusy => ErrorCategory::Busy,
            rusqlite::ErrorCode::DatabaseLocked => ErrorCategory::Locked,
            rusqlite::ErrorCode::ConstraintViolation => ErrorCategory::Constraint,
            rusqlite::ErrorCode::DatabaseCorrupt => ErrorCategory::Corrupt,
            rusqlite::ErrorCode::SchemaChanged => ErrorCategory::Schema,
            rusqlite::ErrorCode::SystemIoFailure => ErrorCategory::IoErr,
            rusqlite::ErrorCode::AuthorizationForStatementDenied => ErrorCategory::Auth,
            rusqlite::ErrorCode::OperationAborted => ErrorCategory::Abort,
            rusqlite::ErrorCode::ReadOnly => ErrorCategory::ReadOnly,
            rusqlite::ErrorCode::CannotOpen => ErrorCategory::CantOpen,
            rusqlite::ErrorCode::DiskFull => ErrorCategory::Full,
            rusqlite::ErrorCode::ParameterOutOfRange => ErrorCategory::Range,
            rusqlite::ErrorCode::NotADatabase => ErrorCategory::NotADb,
            _ => ErrorCategory::Error,
        };
    }
    ErrorCategory::from_sqlite_error(&err.to_string())
}

fn canonical_rusqlite_value(value: rusqlite::types::ValueRef<'_>) -> String {
    match value {
        rusqlite::types::ValueRef::Null => "n:NULL".to_owned(),
        rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
        rusqlite::types::ValueRef::Real(v) => canonical_float(v),
        rusqlite::types::ValueRef::Text(bytes) => {
            let text = String::from_utf8_lossy(bytes);
            format!("t:{text:?}")
        }
        rusqlite::types::ValueRef::Blob(bytes) => format!("b:{}", hex_encode(bytes)),
    }
}

fn exec_rusqlite(conn: &rusqlite::Connection, sql: &str) -> Result<usize, (ErrorCategory, String)> {
    match conn.prepare(sql) {
        Ok(mut stmt) => {
            if stmt.column_count() > 0 {
                let mut rows = stmt
                    .query([])
                    .map_err(|err| (rusqlite_error_category(&err), err.to_string()))?;
                while rows
                    .next()
                    .map_err(|err| (rusqlite_error_category(&err), err.to_string()))?
                    .is_some()
                {}
            } else {
                stmt.execute([])
                    .map_err(|err| (rusqlite_error_category(&err), err.to_string()))?;
            }
            Ok(usize::try_from(conn.changes()).unwrap_or(0))
        }
        Err(err) => Err((rusqlite_error_category(&err), err.to_string())),
    }
}

fn query_rusqlite(
    conn: &rusqlite::Connection,
    sql: &str,
    ordered: bool,
) -> Result<Vec<String>, (ErrorCategory, String)> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|err| (rusqlite_error_category(&err), err.to_string()))?;
    let col_count = stmt.column_count();
    let mut rows = stmt
        .query([])
        .map_err(|err| (rusqlite_error_category(&err), err.to_string()))?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|err| (rusqlite_error_category(&err), err.to_string()))?
    {
        let mut cols = Vec::with_capacity(col_count);
        for idx in 0..col_count {
            let val = row
                .get_ref(idx)
                .map_err(|err| (rusqlite_error_category(&err), err.to_string()))?;
            cols.push(canonical_rusqlite_value(val));
        }
        out.push(cols.join("|"));
    }
    if !ordered {
        out.sort();
    }
    Ok(out)
}

#[test]
#[allow(clippy::too_many_lines)]
fn test_bd_1lsfu_2_core_sql_fixtures_match_stock_rusqlite_oracle() -> Result<(), String> {
    let audit = audit_requested();
    let mut outcome: Result<(), String> = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let fixtures = oracle::load_fixtures_from_dir(&fixture_dir()?)
                .map_err(|error| format!("bead_id={BEAD_ID} case=load_fixtures error={error}"))?;
            let mut mismatches = Vec::new();

            for fixture in &fixtures {
                let mut fconn = Connection::open(":memory:").await.map_err(|error| {
                    format!("bead_id={BEAD_ID} case=open_connection error={error}")
                })?;
                let mut sconn = rusqlite::Connection::open_in_memory().map_err(|error| {
                    format!("bead_id={BEAD_ID} case=open_stock_connection error={error}")
                })?;

                for (op_idx, op) in fixture.ops.iter().enumerate() {
                    match op {
                        FixtureOp::Open { path } => {
                            fconn = Connection::open(path).await.map_err(|error| {
                                format!(
                                    "bead_id={BEAD_ID} case=open_fixture fixture={} path={} error={error}",
                                    fixture.id, path
                                )
                            })?;
                            sconn = if path == ":memory:" {
                                rusqlite::Connection::open_in_memory()
                            } else {
                                rusqlite::Connection::open(path)
                            }
                            .map_err(|error| {
                                format!(
                                    "bead_id={BEAD_ID} case=open_stock_fixture fixture={} path={} error={error}",
                                    fixture.id, path
                                )
                            })?;
                        }
                        FixtureOp::Exec { sql, expect_error } => {
                            let f_res = fconn.execute(sql).await;
                            let s_res = exec_rusqlite(&sconn, sql);
                            if audit {
                                eprintln!(
                                    "AUDIT_EXEC|{}|op={}|sql={}|fsqlite={:?}|stock={:?}",
                                    fixture.id,
                                    op_idx,
                                    sql.trim(),
                                    f_res.as_ref().map_err(|e| (
                                        ErrorCategory::from_franken_error(e),
                                        e.error_code(),
                                        e.to_string()
                                    )),
                                    s_res,
                                );
                            }
                            match (&f_res, &s_res) {
                                (Ok(f_changes), Ok(s_changes)) => {
                                    if expect_error.is_some() {
                                        mismatches.push(format!(
                                            "{} op#{op_idx} EXEC expected error {:?}, both succeeded: {sql}",
                                            fixture.id, expect_error
                                        ));
                                    }
                                    if f_changes != s_changes {
                                        mismatches.push(format!(
                                            "{} op#{op_idx} EXEC changes mismatch: fsqlite={f_changes} stock={s_changes} sql={sql}",
                                            fixture.id
                                        ));
                                    }
                                }
                                (Err(f_err), Err((s_cat, _))) => {
                                    let f_cat = ErrorCategory::from_franken_error(f_err);
                                    if f_cat != *s_cat {
                                        mismatches.push(format!(
                                            "{} op#{op_idx} EXEC error category mismatch: fsqlite={f_cat} ({f_err}) stock={s_cat} sql={sql}",
                                            fixture.id
                                        ));
                                    }
                                }
                                _ => {
                                    mismatches.push(format!(
                                        "{} op#{op_idx} EXEC ok/err mismatch: fsqlite={f_res:?} stock={s_res:?} sql={sql}",
                                        fixture.id
                                    ));
                                }
                            }
                        }
                        FixtureOp::Query { sql, expect } => {
                            let explain_sql = format!("EXPLAIN QUERY PLAN {sql}");
                            if audit {
                                let f_plan = fconn
                                    .query(&explain_sql)
                                    .await
                                    .map(|r| canonical_rows(&r, true))
                                    .map_err(|e| (ErrorCategory::from_franken_error(&e), e.error_code()));
                                let s_plan = query_rusqlite(&sconn, &explain_sql, true);
                                eprintln!(
                                    "AUDIT_PLAN|{}|op={}|sql={}|fsqlite={:?}|stock={:?}",
                                    fixture.id,
                                    op_idx,
                                    sql.trim(),
                                    f_plan,
                                    s_plan,
                                );
                            }
                            let f_res = fconn
                                .query(sql)
                                .await
                                .map(|rows| canonical_rows(&rows, expect.ordered));
                            let s_res = query_rusqlite(&sconn, sql, expect.ordered);
                            if audit {
                                eprintln!(
                                    "AUDIT_QUERY|{}|op={}|sql={}|fsqlite={:?}|stock={:?}",
                                    fixture.id,
                                    op_idx,
                                    sql.trim(),
                                    f_res.as_ref().map_err(|e| (
                                        ErrorCategory::from_franken_error(e),
                                        e.error_code(),
                                        e.to_string()
                                    )),
                                    s_res,
                                );
                            }
                            match (&f_res, &s_res) {
                                (Ok(f_rows), Ok(s_rows)) => {
                                    if f_rows != s_rows {
                                        mismatches.push(format!(
                                            "{} op#{op_idx} QUERY rows mismatch:\n  sql: {sql}\n  fsqlite: {f_rows:?}\n  stock:   {s_rows:?}",
                                            fixture.id
                                        ));
                                    }
                                }
                                (Err(f_err), Err((s_cat, _))) => {
                                    let f_cat = ErrorCategory::from_franken_error(f_err);
                                    if f_cat != *s_cat {
                                        mismatches.push(format!(
                                            "{} op#{op_idx} QUERY error category mismatch: fsqlite={f_cat} ({f_err}) stock={s_cat} sql={sql}",
                                            fixture.id
                                        ));
                                    }
                                }
                                _ => {
                                    mismatches.push(format!(
                                        "{} op#{op_idx} QUERY ok/err mismatch: fsqlite={f_res:?} stock={s_res:?} sql={sql}",
                                        fixture.id
                                    ));
                                }
                            }
                        }
                    }
                }
            }

            // Also verify core_sql_edge_cases against bundled rusqlite 3.54.1.
            let fconn = Connection::open(":memory:").await.map_err(|error| {
                format!("bead_id={BEAD_ID} case=open_edge_connection error={error}")
            })?;
            let sconn = rusqlite::Connection::open_in_memory().map_err(|error| {
                format!("bead_id={BEAD_ID} case=open_stock_edge_connection error={error}")
            })?;
            let mut dummy_hasher = Hasher::new();
            seed_execution_database(&fconn, &mut dummy_hasher).await;
            for sql in [
                "CREATE TABLE IF NOT EXISTS __seed_numbers(id INTEGER PRIMARY KEY, x INTEGER, y REAL, tag TEXT)",
                "DELETE FROM __seed_numbers",
                "INSERT INTO __seed_numbers VALUES(1, 10, 1.5, 'alpha')",
                "INSERT INTO __seed_numbers VALUES(2, 20, 2.5, 'beta')",
                "INSERT INTO __seed_numbers VALUES(3, NULL, 3.5, NULL)",
            ] {
                let _ = exec_rusqlite(&sconn, sql);
            }
            for sql in edge_case_queries() {
                let f_rows = fconn
                    .query(sql)
                    .await
                    .map(|rows| canonical_rows(&rows, true))
                    .map_err(|error| format!("core_sql_edge_cases fsqlite error on {sql}: {error}"))?;
                let s_rows = query_rusqlite(&sconn, sql, true)
                    .map_err(|error| format!("core_sql_edge_cases stock error on {sql}: {error:?}"))?;
                if f_rows != s_rows {
                    mismatches.push(format!(
                        "core_sql_edge_cases mismatch:\n  sql: {sql}\n  fsqlite: {f_rows:?}\n  stock:   {s_rows:?}"
                    ));
                }
            }

            if !mismatches.is_empty() {
                return Err(format!(
                    "bead_id={BEAD_ID} case=stock_oracle_mismatch count={}\n{}",
                    mismatches.len(),
                    mismatches.join("\n")
                ));
            }
            Ok(())
        }
        .await;
    });
    outcome
}
