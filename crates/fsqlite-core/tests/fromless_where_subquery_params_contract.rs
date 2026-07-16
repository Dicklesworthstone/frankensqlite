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

/// frankensim-kl17o (fixed): parameters inside ANY subquery under a
/// FROM-ful outer WHERE must bind. ROOT CAUSE was not execution-side
/// binding at all: `prepare()` runs the eager subquery rewrite with
/// `params = None`, which evaluated the uncorrelated subquery with every
/// placeholder reading NULL and baked `WHERE 0` into the compiled
/// statement — and the single-statement ad-hoc `query_with_params` route
/// silently reuses the prepared pipeline. The fix defers folding of
/// parameter-dependent subqueries (any subquery containing a
/// placeholder) past prepare time and routes them through dispatch
/// paths that bind at execution. sqlite3 returns one row for every case
/// below.
#[test]
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

/// The baked-literal trap: one prepared statement re-executed with
/// DIFFERENT bindings must track the bindings. The old bug froze the
/// prepare-time (NULL-bound) subquery verdict into the program, so every
/// execution returned the same wrong answer regardless of parameters.
#[test]
fn prepared_fromful_subquery_rebinds_per_execution() {
    let conn = seeded();
    let prepared = conn
        .prepare(
            "SELECT role FROM edges WHERE EXISTS(SELECT 1 FROM edges e2 WHERE e2.artifact = ?1 \
             AND e2.op = ?2);",
        )
        .expect("prepare");
    let matching = prepared
        .query_with_params(&[
            SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
            SqliteValue::Integer(1),
        ])
        .expect("matching execution");
    assert_eq!(
        matching.len(),
        1,
        "matching bindings must satisfy the guard"
    );
    let unmatched = prepared
        .query_with_params(&[
            SqliteValue::Blob(std::sync::Arc::from(vec![0xBB_u8; 32])),
            SqliteValue::Integer(1),
        ])
        .expect("unmatched execution");
    assert_eq!(
        unmatched.len(),
        0,
        "the SAME prepared statement with unmatched bindings must refuse"
    );
    let matching_again = prepared
        .query_with_params(&[
            SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
            SqliteValue::Integer(1),
        ])
        .expect("matching re-execution");
    assert_eq!(matching_again.len(), 1, "rebinding must not be sticky");
}

/// Guarded INSERT...SELECT via the prepared route (the fs-ledger seal
/// idiom that surfaced frankensim-lnbzs) must honor per-execution
/// bindings in both directions.
#[test]
fn prepared_guarded_insert_select_rebinds_per_execution() {
    let conn = seeded();
    let prepared = conn
        .prepare(
            "INSERT INTO seals SELECT ?1, ?2 WHERE EXISTS(SELECT 1 FROM edges WHERE artifact = \
             ?3 AND op = ?4 LIMIT 1);",
        )
        .expect("prepare guarded insert");
    let refused = prepared
        .execute_with_params(&[
            SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
            SqliteValue::Integer(1),
            SqliteValue::Blob(std::sync::Arc::from(vec![0xBB_u8; 32])), // no such artifact
            SqliteValue::Integer(1),
        ])
        .expect("unsatisfied guard executes");
    assert_eq!(refused, 0, "unsatisfied guard must insert nothing");
    let inserted = prepared
        .execute_with_params(&[
            SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
            SqliteValue::Integer(1),
            SqliteValue::Blob(std::sync::Arc::from(vec![0xAA_u8; 32])),
            SqliteValue::Integer(1),
        ])
        .expect("satisfied guard executes");
    assert_eq!(inserted, 1, "satisfied guard must insert exactly one row");
}
