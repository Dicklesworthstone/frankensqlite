//! Regression coverage for HFDT's migration-scale DDL reopen failure.
//!
//! The production failure combined two hazards:
//! 1. flat boolean predicates were serialized as recursively nested SQL, so
//!    every catalog parse/render cycle increased physical parenthesis depth;
//! 2. malformed trigger catalog rows were silently skipped during schema
//!    hydration.
//!
//! These tests deliberately use a fixed 2 MiB stack, real file-backed
//! databases, repeated closes/reopens, trigger execution, foreign keys, and
//! SQLite's integrity diagnostics. C SQLite is the behavioral oracle. No mock
//! pager, synthetic catalog, or in-memory-only shortcut participates.

use std::collections::BTreeMap;

use fsqlite_ast::{Expr, Statement};
use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_parser::Parser;
use fsqlite_types::value::SqliteValue;

const STACK_BYTES: usize = 2 * 1024 * 1024;
const FLAT_BOOLEAN_TERMS: usize = 256;
const REOPEN_CYCLES: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
struct DdlFingerprint {
    sql: String,
    hash: String,
    bytes: usize,
    parenthesis_depth: usize,
    outer_expression_depth: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedRows {
    audit: Vec<Vec<String>>,
    view: Vec<Vec<String>>,
    nocase_partial_predicate_rows: Vec<Vec<String>>,
    binary_partial_predicate_rows: Vec<Vec<String>>,
    foreign_key_check: ForeignKeyCheckObservations,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ForeignKeyCheckObservations {
    database_wide: Vec<Vec<String>>,
    rowid_table: Vec<Vec<String>>,
    without_rowid_table: Vec<Vec<String>>,
}

fn maximum_parenthesis_depth(sql: &str) -> usize {
    let mut depth = 0_usize;
    let mut maximum = 0_usize;
    for byte in sql.bytes() {
        match byte {
            b'(' => {
                depth = depth.saturating_add(1);
                maximum = maximum.max(depth);
            }
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    maximum
}

/// Measure the outer expression tree without recursively using the native
/// stack. Subqueries count as one expression node because their SELECT tree is
/// a separate grammar object; the migration predicate's AND/OR spine is fully
/// represented by this metric.
fn outer_expression_depth(root: &Expr) -> usize {
    let mut maximum = 0_usize;
    let mut pending = vec![(root, 1_usize)];

    while let Some((expr, depth)) = pending.pop() {
        maximum = maximum.max(depth);
        let child_depth = depth.saturating_add(1);
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                pending.push((right, child_depth));
                pending.push((left, child_depth));
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::IsNull { expr, .. }
            | Expr::In { expr, .. } => pending.push((expr, child_depth)),
            Expr::Between {
                expr, low, high, ..
            } => {
                pending.push((high, child_depth));
                pending.push((low, child_depth));
                pending.push((expr, child_depth));
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                if let Some(escape) = escape {
                    pending.push((escape, child_depth));
                }
                pending.push((pattern, child_depth));
                pending.push((expr, child_depth));
            }
            Expr::Case {
                operand,
                whens,
                else_expr,
                ..
            } => {
                if let Some(operand) = operand {
                    pending.push((operand, child_depth));
                }
                for (when, then) in whens {
                    pending.push((then, child_depth));
                    pending.push((when, child_depth));
                }
                if let Some(else_expr) = else_expr {
                    pending.push((else_expr, child_depth));
                }
            }
            Expr::JsonAccess { expr, path, .. } => {
                pending.push((path, child_depth));
                pending.push((expr, child_depth));
            }
            Expr::RowValue(values, _) => {
                pending.extend(values.iter().map(|value| (value, child_depth)));
            }
            Expr::Literal(_, _)
            | Expr::Column(_, _)
            | Expr::Exists { .. }
            | Expr::Subquery(_, _)
            | Expr::FunctionCall { .. }
            | Expr::Raise { .. }
            | Expr::Placeholder(_, _) => {}
        }
    }

    maximum
}

fn trigger_when_depth(sql: &str) -> Option<usize> {
    let mut parser = Parser::from_sql(sql);
    let (mut statements, errors) = parser.parse_all();
    assert!(
        errors.is_empty(),
        "catalog SQL must parse while fingerprinting: sql={sql:?}, errors={errors:?}"
    );
    assert_eq!(
        statements.len(),
        1,
        "catalog SQL must contain exactly one statement: {sql:?}"
    );
    match statements.remove(0) {
        Statement::CreateTrigger(trigger) => trigger.when.as_ref().map(outer_expression_depth),
        _ => None,
    }
}

fn fingerprint(kind: &str, sql: String) -> DdlFingerprint {
    let outer_expression_depth = kind
        .eq_ignore_ascii_case("trigger")
        .then(|| trigger_when_depth(&sql))
        .flatten();
    DdlFingerprint {
        hash: blake3::hash(sql.as_bytes()).to_hex().to_string(),
        bytes: sql.len(),
        parenthesis_depth: maximum_parenthesis_depth(&sql),
        outer_expression_depth,
        sql,
    }
}

fn franken_schema_snapshot(conn: &Connection) -> BTreeMap<String, DdlFingerprint> {
    conn.query(
        "SELECT type, name, sql FROM sqlite_master \
         WHERE sql IS NOT NULL ORDER BY type, name",
    )
    .expect("read FrankenSQLite schema SQL")
    .into_iter()
    .map(|row| {
        let [
            SqliteValue::Text(kind),
            SqliteValue::Text(name),
            SqliteValue::Text(sql),
        ] = row.values()
        else {
            panic!(
                "sqlite_master text projection returned an unexpected row: {:?}",
                row.values()
            );
        };
        (format!("{kind}:{name}"), fingerprint(kind, sql.to_string()))
    })
    .collect()
}

fn sqlite_schema_snapshot(conn: &rusqlite::Connection) -> BTreeMap<String, DdlFingerprint> {
    let mut stmt = conn
        .prepare(
            "SELECT type, name, sql FROM sqlite_master \
             WHERE sql IS NOT NULL ORDER BY type, name",
        )
        .expect("prepare C SQLite schema query");
    stmt.query_map([], |row| {
        let kind: String = row.get(0)?;
        let name: String = row.get(1)?;
        let sql: String = row.get(2)?;
        Ok((format!("{kind}:{name}"), fingerprint(&kind, sql)))
    })
    .expect("query C SQLite schema")
    .collect::<Result<_, _>>()
    .expect("decode C SQLite schema")
}

fn tag_franken(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "null".to_owned(),
        SqliteValue::Integer(value) => format!("int:{value}"),
        SqliteValue::Float(value) => format!("real:{value:?}"),
        SqliteValue::Text(value) => format!("text:{value}"),
        SqliteValue::Blob(value) => format!("blob:{}", hex(value)),
    }
}

fn tag_sqlite(value: &rusqlite::types::Value) -> String {
    use rusqlite::types::Value;
    match value {
        Value::Null => "null".to_owned(),
        Value::Integer(value) => format!("int:{value}"),
        Value::Real(value) => format!("real:{value:?}"),
        Value::Text(value) => format!("text:{value}"),
        Value::Blob(value) => format!("blob:{}", hex(value)),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

fn franken_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .unwrap_or_else(|error| panic!("FrankenSQLite query failed: sql={sql:?}, error={error}"))
        .iter()
        .map(|row| row.values().iter().map(tag_franken).collect())
        .collect()
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|error| panic!("C SQLite prepare failed: sql={sql:?}, error={error}"));
    let column_count = stmt.column_count();
    stmt.query_map([], |row| {
        (0..column_count)
            .map(|index| {
                row.get::<_, rusqlite::types::Value>(index)
                    .map(|v| tag_sqlite(&v))
            })
            .collect::<Result<Vec<_>, _>>()
    })
    .unwrap_or_else(|error| panic!("C SQLite query failed: sql={sql:?}, error={error}"))
    .collect::<Result<_, _>>()
    .expect("decode C SQLite rows")
}

fn sorted_rows(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

fn build_schema() -> Vec<String> {
    let flat_predicate = (0..FLAT_BOOLEAN_TERMS)
        .map(|term| format!("NEW.value != {term}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let trigger_predicate = format!(
        "{flat_predicate} \
         AND NOT (NEW.value BETWEEN 0 AND 255) \
         AND NEW.label COLLATE NOCASE = 'fire' \
         AND CASE WHEN NEW.label COLLATE NOCASE = 'fire' THEN 1 ELSE 0 END = 1 \
         AND EXISTS (SELECT 1 FROM parent WHERE parent.id = NEW.parent_id) \
         AND (SELECT COUNT(*) FROM parent WHERE parent.id = NEW.parent_id) = 1"
    );

    vec![
        "CREATE TABLE parent (id INTEGER PRIMARY KEY)".to_owned(),
        "CREATE TABLE audit (\
            seq INTEGER PRIMARY KEY, \
            source TEXT NOT NULL, \
            row_key TEXT NOT NULL, \
            value INTEGER NOT NULL\
         )"
        .to_owned(),
        "CREATE TABLE guarded_rowid (\
            id INTEGER PRIMARY KEY, \
            parent_id INTEGER NOT NULL REFERENCES parent(id), \
            value INTEGER NOT NULL, \
            label TEXT NOT NULL, \
            bucket TEXT GENERATED ALWAYS AS (\
                CASE WHEN value BETWEEN 1000 AND 2000 THEN 'inside' ELSE 'outside' END\
            ) STORED, \
            CHECK (\
                value BETWEEN -1000000 AND 1000000 \
                AND NOT (label COLLATE NOCASE = 'blocked')\
            )\
         )"
        .to_owned(),
        "CREATE TABLE guarded_wor (\
            key TEXT PRIMARY KEY, \
            parent_id INTEGER NOT NULL REFERENCES parent(id), \
            value INTEGER NOT NULL, \
            label TEXT NOT NULL, \
            CHECK (value >= -1000000 AND value <= 1000000)\
         ) WITHOUT ROWID"
            .to_owned(),
        "CREATE INDEX idx_guarded_rowid_nocase ON guarded_rowid(value) \
         WHERE value >= 0 AND label COLLATE NOCASE = 'fire'"
            .to_owned(),
        "CREATE INDEX idx_guarded_rowid_binary ON guarded_rowid(value) \
         WHERE value >= 0 AND label COLLATE BINARY = 'fire'"
            .to_owned(),
        "CREATE VIEW guarded_view AS \
         SELECT id AS object_key, \
                CASE WHEN value BETWEEN 1000 AND 2000 THEN 'inside' ELSE 'outside' END AS bucket \
         FROM guarded_rowid \
         WHERE value >= 0 AND label COLLATE NOCASE = 'fire'"
            .to_owned(),
        format!(
            "CREATE TRIGGER guarded_rowid_audit AFTER INSERT ON guarded_rowid \
             WHEN {trigger_predicate} \
             BEGIN \
                INSERT INTO audit(source, row_key, value) \
                VALUES ('rowid', NEW.id || '', NEW.value); \
             END"
        ),
        format!(
            "CREATE TRIGGER guarded_wor_audit AFTER INSERT ON guarded_wor \
             WHEN {trigger_predicate} \
             BEGIN \
                INSERT INTO audit(source, row_key, value) \
                VALUES ('without_rowid', NEW.key, NEW.value); \
             END"
        ),
        "INSERT INTO parent VALUES (7)".to_owned(),
    ]
}

fn log_schema(engine: &str, phase: &str, snapshot: &BTreeMap<String, DdlFingerprint>) {
    for (object, ddl) in snapshot {
        eprintln!(
            "scenario=ddl_file_reopen engine={engine} phase={phase} object={object} \
             sql_hash={} sql_bytes={} parenthesis_depth={} outer_expression_depth={:?}",
            ddl.hash, ddl.bytes, ddl.parenthesis_depth, ddl.outer_expression_depth
        );
    }
}

fn assert_schema_stable(
    baseline: &BTreeMap<String, DdlFingerprint>,
    replay: &BTreeMap<String, DdlFingerprint>,
    cycle: usize,
) {
    assert_eq!(
        replay.keys().collect::<Vec<_>>(),
        baseline.keys().collect::<Vec<_>>(),
        "schema object set changed during reopen cycle {cycle}"
    );
    for (object, expected) in baseline {
        let actual = replay
            .get(object)
            .unwrap_or_else(|| panic!("schema object disappeared: cycle={cycle}, object={object}"));
        assert_eq!(
            actual.hash, expected.hash,
            "schema SQL hash changed: cycle={cycle}, object={object}"
        );
        assert_eq!(
            actual.bytes, expected.bytes,
            "schema SQL byte length changed: cycle={cycle}, object={object}"
        );
        assert_eq!(
            actual.parenthesis_depth, expected.parenthesis_depth,
            "schema SQL parenthesis depth changed: cycle={cycle}, object={object}"
        );
        assert_eq!(
            actual.outer_expression_depth, expected.outer_expression_depth,
            "schema SQL AST depth changed: cycle={cycle}, object={object}"
        );
        assert_eq!(
            actual.sql, expected.sql,
            "schema SQL bytes changed: cycle={cycle}, object={object}"
        );
    }
}

fn exercise_franken_cycle(conn: &Connection, cycle: usize) -> ObservedRows {
    conn.execute("PRAGMA foreign_keys = ON")
        .expect("enable FrankenSQLite foreign keys");
    let id = i64::try_from(cycle + 1).expect("cycle id fits i64");
    let value = 1000_i64 + i64::try_from(cycle).expect("cycle value fits i64");
    conn.execute(&format!(
        "INSERT INTO guarded_rowid(id, parent_id, value, label) \
         VALUES ({id}, 7, {value}, 'FiRe')"
    ))
    .expect("insert guarded rowid row");
    conn.execute(&format!(
        "INSERT INTO guarded_wor(key, parent_id, value, label) \
         VALUES ('wor-{id}', 7, {value}, 'FiRe')"
    ))
    .expect("insert guarded WITHOUT ROWID row");

    let invalid_id = 10_000_i64 + i64::try_from(cycle).expect("invalid id fits i64");
    let invalid = conn.execute(&format!(
        "INSERT INTO guarded_rowid(id, parent_id, value, label) \
         VALUES ({invalid_id}, 999, {value}, 'FiRe')"
    ));
    assert!(
        invalid.is_err(),
        "missing parent must be rejected by FrankenSQLite: cycle={cycle}"
    );

    let invalid_wor_key = format!("invalid-wor-{cycle}");
    let invalid_wor = conn.execute(&format!(
        "INSERT INTO guarded_wor(key, parent_id, value, label) \
         VALUES ('{invalid_wor_key}', 999, -1, 'skip')"
    ));
    assert!(
        invalid_wor.is_err(),
        "missing parent must be rejected for a WITHOUT ROWID table by FrankenSQLite: cycle={cycle}"
    );

    conn.execute("PRAGMA foreign_keys = OFF")
        .expect("disable FrankenSQLite foreign keys for violation inspection");
    conn.execute(&format!(
        "INSERT INTO guarded_rowid(id, parent_id, value, label) \
         VALUES ({invalid_id}, 999, -1, 'skip')"
    ))
    .expect("seed FrankenSQLite rowid FK violation");
    conn.execute(&format!(
        "INSERT INTO guarded_wor(key, parent_id, value, label) \
         VALUES ('{invalid_wor_key}', 999, -1, 'skip')"
    ))
    .expect("seed FrankenSQLite WITHOUT ROWID FK violation");
    let foreign_key_check = ForeignKeyCheckObservations {
        database_wide: sorted_rows(franken_rows(conn, "PRAGMA foreign_key_check")),
        rowid_table: sorted_rows(franken_rows(
            conn,
            "PRAGMA foreign_key_check(guarded_rowid)",
        )),
        without_rowid_table: sorted_rows(franken_rows(
            conn,
            "PRAGMA foreign_key_check(guarded_wor)",
        )),
    };
    conn.execute(&format!(
        "DELETE FROM guarded_rowid WHERE id = {invalid_id}"
    ))
    .expect("remove FrankenSQLite rowid FK violation");
    conn.execute(&format!(
        "DELETE FROM guarded_wor WHERE key = '{invalid_wor_key}'"
    ))
    .expect("remove FrankenSQLite WITHOUT ROWID FK violation");
    conn.execute("PRAGMA foreign_keys = ON")
        .expect("restore FrankenSQLite foreign keys");

    ObservedRows {
        audit: franken_rows(
            conn,
            "SELECT seq, source, row_key, value FROM audit ORDER BY seq",
        ),
        view: franken_rows(
            conn,
            "SELECT object_key, bucket FROM guarded_view ORDER BY object_key",
        ),
        nocase_partial_predicate_rows: franken_rows(
            conn,
            "SELECT id FROM guarded_rowid \
             WHERE value >= 0 AND label COLLATE NOCASE = 'fire' ORDER BY id",
        ),
        binary_partial_predicate_rows: franken_rows(
            conn,
            "SELECT id FROM guarded_rowid \
             WHERE value >= 0 AND label COLLATE BINARY = 'fire' ORDER BY id",
        ),
        foreign_key_check,
    }
}

fn exercise_sqlite_cycle(conn: &rusqlite::Connection, cycle: usize) -> ObservedRows {
    conn.execute_batch("PRAGMA foreign_keys = ON")
        .expect("enable C SQLite foreign keys");
    let id = i64::try_from(cycle + 1).expect("cycle id fits i64");
    let value = 1000_i64 + i64::try_from(cycle).expect("cycle value fits i64");
    conn.execute(
        "INSERT INTO guarded_rowid(id, parent_id, value, label) VALUES (?1, 7, ?2, 'FiRe')",
        rusqlite::params![id, value],
    )
    .expect("insert C SQLite guarded rowid row");
    conn.execute(
        "INSERT INTO guarded_wor(key, parent_id, value, label) \
         VALUES (?1, 7, ?2, 'FiRe')",
        rusqlite::params![format!("wor-{id}"), value],
    )
    .expect("insert C SQLite guarded WITHOUT ROWID row");

    let invalid_id = 10_000_i64 + i64::try_from(cycle).expect("invalid id fits i64");
    let invalid = conn.execute(
        "INSERT INTO guarded_rowid(id, parent_id, value, label) \
         VALUES (?1, 999, ?2, 'FiRe')",
        rusqlite::params![invalid_id, value],
    );
    assert!(
        invalid.is_err(),
        "missing parent must be rejected by C SQLite: cycle={cycle}"
    );

    let invalid_wor_key = format!("invalid-wor-{cycle}");
    let invalid_wor = conn.execute(
        "INSERT INTO guarded_wor(key, parent_id, value, label) \
         VALUES (?1, 999, -1, 'skip')",
        rusqlite::params![invalid_wor_key],
    );
    assert!(
        invalid_wor.is_err(),
        "missing parent must be rejected for a WITHOUT ROWID table by C SQLite: cycle={cycle}"
    );

    conn.execute_batch("PRAGMA foreign_keys = OFF")
        .expect("disable C SQLite foreign keys for violation inspection");
    conn.execute(
        "INSERT INTO guarded_rowid(id, parent_id, value, label) \
         VALUES (?1, 999, -1, 'skip')",
        rusqlite::params![invalid_id],
    )
    .expect("seed C SQLite rowid FK violation");
    conn.execute(
        "INSERT INTO guarded_wor(key, parent_id, value, label) \
         VALUES (?1, 999, -1, 'skip')",
        rusqlite::params![invalid_wor_key],
    )
    .expect("seed C SQLite WITHOUT ROWID FK violation");
    let foreign_key_check = ForeignKeyCheckObservations {
        database_wide: sorted_rows(sqlite_rows(conn, "PRAGMA foreign_key_check")),
        rowid_table: sorted_rows(sqlite_rows(conn, "PRAGMA foreign_key_check(guarded_rowid)")),
        without_rowid_table: sorted_rows(sqlite_rows(
            conn,
            "PRAGMA foreign_key_check(guarded_wor)",
        )),
    };
    conn.execute(
        "DELETE FROM guarded_rowid WHERE id = ?1",
        rusqlite::params![invalid_id],
    )
    .expect("remove C SQLite rowid FK violation");
    conn.execute(
        "DELETE FROM guarded_wor WHERE key = ?1",
        rusqlite::params![invalid_wor_key],
    )
    .expect("remove C SQLite WITHOUT ROWID FK violation");
    conn.execute_batch("PRAGMA foreign_keys = ON")
        .expect("restore C SQLite foreign keys");

    ObservedRows {
        audit: sqlite_rows(
            conn,
            "SELECT seq, source, row_key, value FROM audit ORDER BY seq",
        ),
        view: sqlite_rows(
            conn,
            "SELECT object_key, bucket FROM guarded_view ORDER BY object_key",
        ),
        nocase_partial_predicate_rows: sqlite_rows(
            conn,
            "SELECT id FROM guarded_rowid \
             WHERE value >= 0 AND label COLLATE NOCASE = 'fire' ORDER BY id",
        ),
        binary_partial_predicate_rows: sqlite_rows(
            conn,
            "SELECT id FROM guarded_rowid \
             WHERE value >= 0 AND label COLLATE BINARY = 'fire' ORDER BY id",
        ),
        foreign_key_check,
    }
}

fn assert_franken_integrity(conn: &Connection, cycle: usize) {
    let integrity = franken_rows(conn, "PRAGMA integrity_check");
    assert_eq!(
        integrity,
        vec![vec!["text:ok".to_owned()]],
        "FrankenSQLite integrity_check failed after cycle {cycle}"
    );
    let foreign_key_violations = franken_rows(conn, "PRAGMA foreign_key_check");
    assert!(
        foreign_key_violations.is_empty(),
        "FrankenSQLite foreign_key_check failed after cycle {cycle}: \
         {foreign_key_violations:?}"
    );
}

fn assert_sqlite_integrity(conn: &rusqlite::Connection, cycle: usize) {
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("C SQLite integrity_check");
    assert_eq!(integrity, "ok", "C SQLite integrity_check cycle={cycle}");
    let foreign_key_violations = sqlite_rows(conn, "PRAGMA foreign_key_check");
    assert!(
        foreign_key_violations.is_empty(),
        "C SQLite foreign_key_check failed after cycle {cycle}: \
         {foreign_key_violations:?}"
    );
}

#[test]
fn migration_scale_ddl_reopens_and_executes_on_fixed_two_mib_stack() {
    std::thread::Builder::new()
        .name("ddl-reopen-2mib".to_owned())
        .stack_size(STACK_BYTES)
        .spawn(|| {
            let dir = tempfile::tempdir().expect("create DDL regression tempdir");
            let franken_path = dir.path().join("franken.db");
            let sqlite_path = dir.path().join("sqlite.db");
            let franken_path = franken_path.to_string_lossy().into_owned();
            let schema = build_schema();

            let franken_baseline = {
                let conn = Connection::open(&franken_path).expect("open FrankenSQLite fixture");
                conn.execute("PRAGMA foreign_keys = ON")
                    .expect("enable fixture foreign keys");
                for sql in &schema {
                    conn.execute(sql)
                        .unwrap_or_else(|error| panic!("FrankenSQLite DDL failed: {sql}\n{error}"));
                }
                let snapshot = franken_schema_snapshot(&conn);
                log_schema("frankensqlite", "baseline", &snapshot);
                for object in ["trigger:guarded_rowid_audit", "trigger:guarded_wor_audit"] {
                    let trigger = snapshot
                        .get(object)
                        .unwrap_or_else(|| panic!("missing trigger catalog row: {object}"));
                    assert!(
                        trigger.parenthesis_depth <= 8,
                        "flat trigger predicate must stay shallow in catalog SQL: \
                         object={object}, depth={}",
                        trigger.parenthesis_depth
                    );
                    assert!(
                        trigger.outer_expression_depth.unwrap_or_default() >= FLAT_BOOLEAN_TERMS,
                        "fixture must retain a migration-scale AST: object={object}, depth={:?}",
                        trigger.outer_expression_depth
                    );
                }
                conn.close().expect("close FrankenSQLite fixture");
                snapshot
            };

            let sqlite_baseline = {
                let conn = rusqlite::Connection::open(&sqlite_path).expect("open C SQLite fixture");
                conn.execute_batch("PRAGMA foreign_keys = ON")
                    .expect("enable C SQLite fixture foreign keys");
                for sql in &schema {
                    conn.execute_batch(sql)
                        .unwrap_or_else(|error| panic!("C SQLite DDL failed: {sql}\n{error}"));
                }
                let snapshot = sqlite_schema_snapshot(&conn);
                log_schema("sqlite", "baseline", &snapshot);
                snapshot
            };
            assert_eq!(
                franken_baseline.keys().collect::<Vec<_>>(),
                sqlite_baseline.keys().collect::<Vec<_>>(),
                "both engines must expose the same schema object set"
            );

            for cycle in 0..REOPEN_CYCLES {
                let conn = Connection::open(&franken_path)
                    .unwrap_or_else(|error| panic!("FrankenSQLite reopen {cycle}: {error}"));
                let replay = franken_schema_snapshot(&conn);
                assert_schema_stable(&franken_baseline, &replay, cycle);
                let franken_observed = exercise_franken_cycle(&conn, cycle);
                assert_franken_integrity(&conn, cycle);

                let sqlite = rusqlite::Connection::open(&sqlite_path)
                    .unwrap_or_else(|error| panic!("C SQLite reopen {cycle}: {error}"));
                let sqlite_observed = exercise_sqlite_cycle(&sqlite, cycle);
                assert_sqlite_integrity(&sqlite, cycle);

                assert_eq!(
                    franken_observed, sqlite_observed,
                    "trigger/view behavior diverged from C SQLite after reopen cycle {cycle}"
                );
                let invalid_id =
                    10_000_i64 + i64::try_from(cycle).expect("expected FK rowid fits i64");
                let expected_rowid_violation = vec![
                    "text:guarded_rowid".to_owned(),
                    format!("int:{invalid_id}"),
                    "text:parent".to_owned(),
                    "int:0".to_owned(),
                ];
                let expected_without_rowid_violation = vec![
                    "text:guarded_wor".to_owned(),
                    "null".to_owned(),
                    "text:parent".to_owned(),
                    "int:0".to_owned(),
                ];
                assert_eq!(
                    franken_observed.foreign_key_check.rowid_table,
                    vec![expected_rowid_violation.clone()],
                    "table-scoped FK check must expose the rowid table's integer locator"
                );
                assert_eq!(
                    franken_observed.foreign_key_check.without_rowid_table,
                    vec![expected_without_rowid_violation.clone()],
                    "table-scoped FK check must expose NULL for a WITHOUT ROWID table locator"
                );
                assert_eq!(
                    franken_observed.foreign_key_check.database_wide,
                    sorted_rows(vec![
                        expected_rowid_violation,
                        expected_without_rowid_violation,
                    ]),
                    "database-wide FK check must include both table kinds"
                );
                assert_eq!(
                    franken_observed.nocase_partial_predicate_rows.len(),
                    cycle + 1,
                    "NOCASE predicate must admit every mixed-case control row"
                );
                assert!(
                    franken_observed.binary_partial_predicate_rows.is_empty(),
                    "BINARY predicate must reject every mixed-case control row"
                );
                let expected_trigger_count = (cycle + 1) * 2;
                assert_eq!(
                    franken_observed.audit.len(),
                    expected_trigger_count,
                    "exactly two triggers must fire per successful cycle"
                );
                let order_bytes = format!("{:?}", franken_observed.audit);
                eprintln!(
                    "scenario=ddl_file_reopen stack_bytes={STACK_BYTES} \
                     flat_boolean_terms={FLAT_BOOLEAN_TERMS} reopen_cycle={cycle} \
                     schema_objects={} trigger_count={} trigger_order_hash={} \
                     partial_index_nocase_matches={} partial_index_binary_matches={} \
                     foreign_key_check={:?} residual_foreign_key_violations=0 integrity=ok",
                    replay.len(),
                    franken_observed.audit.len(),
                    blake3::hash(order_bytes.as_bytes()).to_hex(),
                    franken_observed.nocase_partial_predicate_rows.len(),
                    franken_observed.binary_partial_predicate_rows.len(),
                    franken_observed.foreign_key_check
                );
                conn.close()
                    .unwrap_or_else(|error| panic!("FrankenSQLite close {cycle}: {error}"));
            }
        })
        .expect("spawn fixed-stack DDL regression")
        .join()
        .expect("fixed-stack DDL regression panicked or overflowed");
}

fn assert_catalog_corruption_refused(
    scenario: &str,
    seed_sql: &str,
    object_type: &str,
    object_name: &str,
    replacement_sql: Option<&str>,
    expected_detail: &str,
) {
    let dir = tempfile::tempdir().expect("create catalog corruption tempdir");
    let path = dir.path().join(format!("{scenario}.db"));
    {
        let sqlite = rusqlite::Connection::open(&path).expect("seed C SQLite catalog");
        sqlite.execute_batch(seed_sql).expect("seed valid schema");
        let schema_version: i64 = sqlite
            .query_row("PRAGMA schema_version", [], |row| row.get(0))
            .expect("read schema version");
        sqlite
            .execute_batch("PRAGMA writable_schema = ON")
            .expect("enable writable_schema");
        let changed = match replacement_sql {
            Some(replacement_sql) => sqlite
                .execute(
                    "UPDATE sqlite_master SET sql = ?1 WHERE type = ?2 AND name = ?3",
                    rusqlite::params![replacement_sql, object_type, object_name],
                )
                .expect("corrupt catalog SQL"),
            None => sqlite
                .execute(
                    "UPDATE sqlite_master SET sql = NULL WHERE type = ?1 AND name = ?2",
                    rusqlite::params![object_type, object_name],
                )
                .expect("replace catalog SQL with NULL"),
        };
        assert_eq!(changed, 1, "fixture must corrupt exactly one catalog row");
        sqlite
            .execute_batch(&format!("PRAGMA schema_version = {}", schema_version + 1))
            .expect("bump schema version");
    }

    let path = path.to_string_lossy().into_owned();
    let error = match Connection::open(&path) {
        Ok(conn) => {
            let _ = conn.close();
            panic!(
                "catalog corruption must fail closed: \
                 scenario={scenario}, type={object_type}, name={object_name}"
            );
        }
        Err(error) => error,
    };
    let FrankenError::DatabaseCorrupt { detail } = error else {
        panic!(
            "catalog corruption must return DatabaseCorrupt: \
             scenario={scenario}, error={error}"
        );
    };
    let replacement_hash = replacement_sql.map_or_else(
        || "null".to_owned(),
        |sql| blake3::hash(sql.as_bytes()).to_hex().to_string(),
    );
    eprintln!(
        "scenario=catalog_corruption stack_bytes={STACK_BYTES} case={scenario} \
         object_type={object_type} \
         object_name={object_name} replacement_hash={} error_kind=DatabaseCorrupt \
         error_detail={detail:?}",
        replacement_hash
    );
    assert!(
        detail.contains(object_name) && detail.contains(expected_detail),
        "typed corruption detail must identify the object and failure class: \
         scenario={scenario}, detail={detail:?}"
    );
}

#[test]
fn malformed_or_misclassified_trigger_and_view_catalog_rows_fail_closed() {
    std::thread::Builder::new()
        .name("catalog-corruption-2mib".to_owned())
        .stack_size(STACK_BYTES)
        .spawn(|| {
            let trigger_seed = "\
                CREATE TABLE source (id INTEGER); \
                CREATE TABLE audit (id INTEGER); \
                CREATE TRIGGER source_ai AFTER INSERT ON source \
                BEGIN INSERT INTO audit VALUES (NEW.id); END;";
            let view_seed = "\
                CREATE TABLE source (id INTEGER); \
                CREATE VIEW source_view AS SELECT id FROM source;";
            let over_depth_trigger = format!(
                "CREATE TRIGGER source_ai AFTER INSERT ON source WHEN {}1{} \
                 BEGIN SELECT 1; END",
                "(".repeat(192),
                ")".repeat(192)
            );

            let cases = [
                (
                    "malformed_trigger",
                    trigger_seed,
                    "trigger",
                    "source_ai",
                    Some("CREATE TRIGGER source_ai"),
                    "failed to parse as CREATE TRIGGER",
                ),
                (
                    "wrong_class_trigger",
                    trigger_seed,
                    "trigger",
                    "source_ai",
                    Some("CREATE VIEW source_ai AS SELECT id FROM source"),
                    "did not parse as CREATE TRIGGER",
                ),
                (
                    "name_mismatch_trigger",
                    trigger_seed,
                    "trigger",
                    "source_ai",
                    Some("CREATE TRIGGER other_ai AFTER INSERT ON source BEGIN SELECT 1; END"),
                    "does not match CREATE TRIGGER name",
                ),
                (
                    "null_trigger_sql",
                    trigger_seed,
                    "trigger",
                    "source_ai",
                    None,
                    "SQL must be TEXT",
                ),
                (
                    "over_depth_trigger",
                    trigger_seed,
                    "trigger",
                    "source_ai",
                    Some(over_depth_trigger.as_str()),
                    "parser recursion limit exceeded",
                ),
                (
                    "malformed_view",
                    view_seed,
                    "view",
                    "source_view",
                    Some("CREATE VIEW source_view AS"),
                    "failed to parse as CREATE VIEW",
                ),
                (
                    "wrong_class_view",
                    view_seed,
                    "view",
                    "source_view",
                    Some("CREATE TRIGGER source_view AFTER INSERT ON source BEGIN SELECT 1; END"),
                    "did not parse as CREATE VIEW",
                ),
                (
                    "name_mismatch_view",
                    view_seed,
                    "view",
                    "source_view",
                    Some("CREATE VIEW other_view AS SELECT id FROM source"),
                    "does not match CREATE VIEW name",
                ),
                (
                    "null_view_sql",
                    view_seed,
                    "view",
                    "source_view",
                    None,
                    "SQL must be TEXT",
                ),
            ];

            for (scenario, seed, object_type, object_name, replacement, expected_detail) in cases {
                assert_catalog_corruption_refused(
                    scenario,
                    seed,
                    object_type,
                    object_name,
                    replacement,
                    expected_detail,
                );
            }
        })
        .expect("spawn fixed-stack catalog corruption regression")
        .join()
        .expect("catalog corruption regression panicked or overflowed");
}
