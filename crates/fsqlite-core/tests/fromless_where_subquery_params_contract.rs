//! frankensim-lnbzs: parameters referenced inside subqueries under a
//! WHERE clause attached to a FROM-less SELECT must bind. sqlite3
//! returns rows for every shape below; fsqlite silently returned zero
//! rows for (A) and (B) — the guarded `INSERT INTO … SELECT ?1 WHERE
//! EXISTS(… ?2 …)` idiom becoming a silent no-op for valid guards.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn seeded() -> Connection {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE edges(op INTEGER, artifact BLOB, role TEXT);")
        .expect("create edges");
    conn.execute("CREATE TABLE seals(artifact BLOB, op INTEGER);")
        .expect("create seals");
    conn.execute_with_params(
        "INSERT INTO edges VALUES (?1, ?2, 'out');",
        &[
            SqliteValue::Integer(1),
            SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
        ],
    )
    .expect("seed edge");
    conn
}

/// Repro (A): FROM-less SELECT whose WHERE EXISTS subquery references
/// the same parameter slots as the projection.
#[test]
fn fromless_select_binds_params_inside_where_exists_subquery() {
    let conn = seeded();
    let rows = conn
        .query_with_params(
            "SELECT ?1, ?2 WHERE EXISTS(SELECT 1 FROM edges WHERE artifact = ?1 AND op = ?2 \
             LIMIT 1);",
            &[
                SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
                SqliteValue::Integer(1),
            ],
        )
        .expect("query");
    assert_eq!(
        rows.len(),
        1,
        "sqlite3 returns one row: the EXISTS guard is satisfied by the seeded edge"
    );
    assert_eq!(
        rows[0].values()[0],
        SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32]))
    );
    assert_eq!(rows[0].values()[1], SqliteValue::Integer(1));
}

/// Repro (B): the guarded INSERT…SELECT idiom with DISTINCT parameter
/// slots inside the guard.
#[test]
fn guarded_fromless_insert_select_binds_guard_params() {
    let conn = seeded();
    let inserted = conn
        .execute_with_params(
            "INSERT INTO seals SELECT ?1, ?2 WHERE EXISTS(SELECT 1 FROM edges WHERE artifact = \
             ?3 AND op = ?4 LIMIT 1);",
            &[
                SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
                SqliteValue::Integer(1),
                SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
                SqliteValue::Integer(1),
            ],
        )
        .expect("guarded insert");
    assert_eq!(inserted, 1, "the guard is satisfied; the row must insert");
    let rows = conn.query("SELECT COUNT(*) FROM seals;").expect("count");
    assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
}

/// The guard must also refuse correctly: unmatched parameters insert
/// nothing (no over-match once binding works).
#[test]
fn guarded_fromless_insert_select_still_refuses_unmatched_guards() {
    let conn = seeded();
    let inserted = conn
        .execute_with_params(
            "INSERT INTO seals SELECT ?1, ?2 WHERE EXISTS(SELECT 1 FROM edges WHERE artifact = \
             ?3 AND op = ?4 LIMIT 1);",
            &[
                SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
                SqliteValue::Integer(1),
                SqliteValue::Blob(std::sync::Arc::from(vec![0xBB_u8; 32])), // no such artifact
                SqliteValue::Integer(1),
            ],
        )
        .expect("guarded insert");
    assert_eq!(inserted, 0, "an unsatisfied guard inserts nothing");
}

/// Regression pin: params as a projection inside SELECT EXISTS(...)
/// keep working.
#[test]
fn projection_exists_params_keep_working() {
    let conn = seeded();
    let projected = conn
        .query_with_params(
            "SELECT EXISTS(SELECT 1 FROM edges WHERE artifact = ?1 AND op = ?2 LIMIT 1);",
            &[
                SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
                SqliteValue::Integer(1),
            ],
        )
        .expect("projection form");
    assert_eq!(projected[0].values()[0], SqliteValue::Integer(1));
}

/// ARMED KNOWN-RED (run with --ignored): parameters inside ANY
/// subquery under a FROM-FUL outer WHERE never bind. Probe matrix
/// (2026-07-16): literal predicates return rows; ?N int/blob, aliased/
/// unaliased EXISTS, and scalar COUNT(*) subqueries all return zero
/// rows; top-level params on the same statement bind fine; the
/// prepared-statement route fails identically; EXPLAIN emits a CORRECT
/// program (Column/Variable/Eq over the inline subquery loop), so the
/// bindings are dropped at execution — suspect the canonicalized
/// program-cache layer executes with an empty binding set for this
/// shape. sqlite3 returns one row for every case below.
#[test]
#[ignore = "live engine bug: subquery params under FROM-ful WHERE do not bind"]
fn fromful_where_subquery_params_bind() {
    let conn = seeded();
    let fromful = conn
        .query_with_params(
            "SELECT role FROM edges WHERE EXISTS(SELECT 1 FROM edges e2 WHERE e2.artifact = ?1 \
             AND e2.op = ?2);",
            &[
                SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
                SqliteValue::Integer(1),
            ],
        )
        .expect("FROM-ful form");
    assert_eq!(fromful.len(), 1);
    let scalar = conn
        .query_with_params(
            "SELECT role FROM edges WHERE (SELECT COUNT(*) FROM edges e2 WHERE e2.op = ?1) > 0;",
            &[SqliteValue::Integer(1)],
        )
        .expect("scalar-count form");
    assert_eq!(scalar.len(), 1);
}
