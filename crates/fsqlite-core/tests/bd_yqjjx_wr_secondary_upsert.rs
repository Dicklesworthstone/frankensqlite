//! bd-yqjjx — ON CONFLICT (<secondary-unique>) DO UPDATE on WITHOUT ROWID tables.
//!
//! Previously the WITHOUT ROWID upsert codegen refused an EXPLICIT secondary-
//! UNIQUE conflict target with `CodegenError::Unsupported` (only the PRIMARY KEY
//! and the omitted "any-constraint" target were emittable). The explicit target
//! now probes just the named index: a conflict on it runs DO UPDATE, while a
//! PRIMARY-KEY or other-index collision is NOT the named arbiter and falls
//! through to the ordinary insert (aborting under the statement default).
//!
//! Expected rows are hand-derived from stock SQLite upsert-arbiter semantics;
//! the rusqlite oracle form lives in
//! fsqlite-e2e/tests/without_rowid_dml_oracle_e2e.rs (bd-yqjjx cases).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn t(s: &str) -> SqliteValue {
    SqliteValue::Text(s.into())
}
fn i(n: i64) -> SqliteValue {
    SqliteValue::Integer(n)
}

async fn rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap()
        .iter()
        .map(|r| r.values().to_vec())
        .collect()
}

/// The named secondary-UNIQUE arbiter fires DO UPDATE on a conflict against it;
/// a fresh secondary value inserts normally.
#[test]
fn wr_explicit_secondary_target_fires_and_inserts() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE wr (a TEXT PRIMARY KEY, b INTEGER UNIQUE) WITHOUT ROWID")
            .await
            .unwrap();
        conn.execute("INSERT INTO wr VALUES ('x',1),('y',2)")
            .await
            .unwrap();
        // b=1 already on row 'x' -> DO UPDATE that row (a='updated').
        conn.execute("INSERT INTO wr VALUES ('z',1) ON CONFLICT(b) DO UPDATE SET a='updated'")
            .await
            .unwrap();
        // b=9 is new -> ordinary insert of ('w',9); DO UPDATE never runs.
        conn.execute("INSERT INTO wr VALUES ('w',9) ON CONFLICT(b) DO UPDATE SET a='never'")
            .await
            .unwrap();
        assert_eq!(
            rows(&conn, "SELECT a, b FROM wr ORDER BY b").await,
            vec![
                vec![t("updated"), i(1)],
                vec![t("y"), i(2)],
                vec![t("w"), i(9)],
            ]
        );
    });
}

/// A PRIMARY KEY collision under a secondary-UNIQUE target is not the arbiter:
/// it falls through to the ordinary insert and aborts, leaving the row intact.
#[test]
fn wr_explicit_secondary_target_pk_conflict_aborts() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE wr (a TEXT PRIMARY KEY, b INTEGER UNIQUE) WITHOUT ROWID")
            .await
            .unwrap();
        conn.execute("INSERT INTO wr VALUES ('x',1)").await.unwrap();
        // Collides on the PK a='x' (b=5 is new) -> not the b-arbiter -> ABORT.
        let res = conn
            .execute("INSERT INTO wr VALUES ('x',5) ON CONFLICT(b) DO UPDATE SET a='no'")
            .await;
        assert!(
            res.is_err(),
            "a PK collision under a secondary-UNIQUE target must abort, not update"
        );
        assert_eq!(
            rows(&conn, "SELECT a, b FROM wr ORDER BY b").await,
            vec![vec![t("x"), i(1)]]
        );
    });
}

/// `excluded.*` and a `WHERE` guard resolve inside the explicit-target DO
/// UPDATE: a WHERE-false collision leaves the existing row untouched.
#[test]
fn wr_explicit_secondary_target_excluded_and_where() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(
            "CREATE TABLE wr (a TEXT PRIMARY KEY, b INTEGER UNIQUE, n INTEGER) WITHOUT ROWID",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO wr VALUES ('x',1,10),('y',2,20)")
            .await
            .unwrap();
        // b=1 conflict; excluded.n(77) > n(10) -> set n=77.
        conn.execute(
            "INSERT INTO wr VALUES ('z',1,77) ON CONFLICT(b) DO UPDATE SET n=excluded.n \
             WHERE excluded.n > n",
        )
        .await
        .unwrap();
        // b=2 conflict; excluded.n(5) > n(20) is false -> untouched.
        conn.execute(
            "INSERT INTO wr VALUES ('q',2,5) ON CONFLICT(b) DO UPDATE SET n=excluded.n \
             WHERE excluded.n > n",
        )
        .await
        .unwrap();
        assert_eq!(
            rows(&conn, "SELECT a, b, n FROM wr ORDER BY b").await,
            vec![vec![t("x"), i(1), i(77)], vec![t("y"), i(2), i(20)]]
        );
    });
}

/// A composite secondary-UNIQUE arbiter (UNIQUE(b,c)) fires on a full-key
/// conflict.
#[test]
fn wr_explicit_composite_secondary_target() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(
            "CREATE TABLE wr (a TEXT PRIMARY KEY, b INTEGER, c INTEGER, UNIQUE(b,c)) WITHOUT ROWID",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO wr VALUES ('x',1,1),('y',1,2)")
            .await
            .unwrap();
        // (b,c)=(1,1) conflicts with row 'x' -> DO UPDATE that row.
        conn.execute("INSERT INTO wr VALUES ('z',1,1) ON CONFLICT(b,c) DO UPDATE SET a='updated'")
            .await
            .unwrap();
        assert_eq!(
            rows(&conn, "SELECT a, b, c FROM wr ORDER BY b, c").await,
            vec![vec![t("updated"), i(1), i(1)], vec![t("y"), i(1), i(2)]]
        );
    });
}
