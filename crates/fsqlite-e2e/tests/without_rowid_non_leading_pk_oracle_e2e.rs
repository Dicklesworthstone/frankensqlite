//! Oracle-parity keeper suite for WITHOUT ROWID tables whose PRIMARY KEY is NOT
//! the leading declared columns (bd-v6pjf / bd-0ntuc), plus omitted-target
//! upsert conflict probing on such tables (bd-xe3nb).
//!
//! C SQLite physically stores a WITHOUT ROWID record as
//!   [PRIMARY KEY columns in PK order] ++ [remaining columns in declared order]
//! (oracle-verified by hexdump: `(v, k PRIMARY KEY)` -> record `[k, v]`;
//! `(a,b,c, PRIMARY KEY(b,a))` -> record `[b,a,c]`). fsqlite currently stores WR
//! records in DECLARED order and refuses non-leading PK at the first INSERT
//! (`without_rowid_pk_indices` is_leading guard, codegen.rs). Supporting these
//! shapes is a structural storage change: reorder declared<->physical(PK-leading)
//! on the write/read/index/vacuum/reload paths (see
//! `fsqlite_types::without_rowid_storage_order`).
//!
//! These tests assert the target (stock-parity) behaviour and are `#[ignore]`d
//! until that structural core lands (bd-v6pjf) — un-ignore them then. Each is
//! cross-checked against the C-SQLite oracle on identical DDL/DML.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => {
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
        }
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn oracle_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => {
                    format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    )
                }
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

/// Run `setup` (DDL + DML) on both engines, then assert every query in
/// `queries` returns identical rows from fsqlite and the C-SQLite oracle.
async fn assert_parity(setup: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open oracle");
    for s in setup {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("oracle `{s}`: {e}"));
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, oracle_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => {
                mismatches.push(format!("FRANK_ERR {q}\n  frank: ERROR({e})\n  csql: {b:?}"))
            }
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {}\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn wr_single_non_leading_pk_parity() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "CREATE TABLE t(v TEXT, k INTEGER PRIMARY KEY) WITHOUT ROWID",
                "INSERT INTO t VALUES('hello', 42), ('world', 7), ('mid', 20)",
            ],
            &[
                "SELECT v, k FROM t",            // declared order, natural (PK) scan order
                "SELECT * FROM t",               // '*' expands to declared order
                "SELECT k, v FROM t ORDER BY k", // explicit PK order
                "SELECT v FROM t WHERE k = 20",  // PK point lookup
                "SELECT count(*) FROM t",
            ],
            "wr_single_non_leading_pk_parity",
        )
        .await;
    });
}

#[test]
fn wr_reordered_composite_pk_parity() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "CREATE TABLE u(a, b, c, PRIMARY KEY(b, a)) WITHOUT ROWID",
                "INSERT INTO u VALUES(1, 2, 30), (3, 2, 20), (1, 1, 10)",
            ],
            &[
                "SELECT a, b, c FROM u", // declared order, natural PK(b,a) order
                "SELECT * FROM u",
                "SELECT c FROM u WHERE b = 2 AND a = 3",
            ],
            "wr_reordered_composite_pk_parity",
        )
        .await;
    });
}

#[test]
fn wr_single_trailing_pk_parity() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "CREATE TABLE w(a, b, c, PRIMARY KEY(c)) WITHOUT ROWID",
                "INSERT INTO w VALUES(1, 2, 'z'), (3, 4, 'a'), (5, 6, 'm')",
            ],
            &[
                "SELECT a, b, c FROM w",
                "SELECT * FROM w",
                "SELECT a FROM w WHERE c = 'm'",
            ],
            "wr_single_trailing_pk_parity",
        )
        .await;
    });
}

#[test]
fn wr_non_leading_text_pk_collation_parity() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "CREATE TABLE x(v, k TEXT PRIMARY KEY) WITHOUT ROWID",
                "INSERT INTO x VALUES(1, 'B'), (2, 'a'), (3, 'C')",
            ],
            &["SELECT k, v FROM x", "SELECT * FROM x"],
            "wr_non_leading_text_pk_collation_parity",
        )
        .await;
    });
}

#[test]
fn wr_non_leading_pk_update_delete_parity() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "CREATE TABLE t(v TEXT, k INTEGER PRIMARY KEY) WITHOUT ROWID",
                "INSERT INTO t VALUES('a', 1), ('b', 2), ('c', 3)",
                "UPDATE t SET v = 'B2' WHERE k = 2",
                "DELETE FROM t WHERE k = 1",
            ],
            &["SELECT k, v FROM t", "SELECT * FROM t"],
            "wr_non_leading_pk_update_delete_parity",
        )
        .await;
    });
}

#[test]
fn wr_non_leading_pk_secondary_unique_parity() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "CREATE TABLE t(v TEXT, k INTEGER PRIMARY KEY, u TEXT UNIQUE) WITHOUT ROWID",
                "INSERT INTO t VALUES('a', 1, 'x'), ('b', 2, 'y')",
            ],
            &[
                "SELECT v, k, u FROM t",
                "SELECT v FROM t WHERE u = 'y'", // secondary-index lookup
            ],
            "wr_non_leading_pk_secondary_unique_parity",
        )
        .await;
    });
}

#[test]
#[ignore = "bd-xe3nb: omitted-target DO UPDATE with UNIQUE secondaries on WITHOUT ROWID not yet landed"]
fn wr_non_leading_pk_omitted_target_upsert_parity() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "CREATE TABLE t(v TEXT, k INTEGER PRIMARY KEY, u TEXT UNIQUE) WITHOUT ROWID",
                "INSERT INTO t VALUES('a', 1, 'x')",
                // Omitted conflict target must probe PK and every UNIQUE index.
                "INSERT INTO t VALUES('a2', 1, 'x2') ON CONFLICT DO UPDATE SET v = excluded.v",
                "INSERT INTO t VALUES('a3', 2, 'x') ON CONFLICT DO NOTHING",
            ],
            &["SELECT v, k, u FROM t"],
            "wr_non_leading_pk_omitted_target_upsert_parity",
        )
        .await;
    });
}

/// File-format parity: a VACUUM INTO of a non-leading-PK WITHOUT ROWID database
/// must produce an image the C-SQLite oracle reads as `integrity_check == ok`
/// with identical rows, and fsqlite must round-trip it on reopen.
#[test]
fn wr_non_leading_pk_vacuum_into_oracle_readable() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src.db").to_string_lossy().into_owned();
        let cand = dir.path().join("cand.db").to_string_lossy().into_owned();

        let conn = Connection::open(&src).await.expect("open source");
        conn.execute("CREATE TABLE t(v TEXT, k INTEGER PRIMARY KEY) WITHOUT ROWID")
            .await
            .expect("create");
        for (v, k) in [("alpha", 3), ("beta", 1), ("gamma", 2)] {
            conn.execute(&format!("INSERT INTO t VALUES('{v}', {k})"))
                .await
                .expect("insert");
        }
        conn.execute(&format!("VACUUM INTO '{cand}'"))
            .await
            .expect("vacuum into");
        conn.close().await.expect("close source");

        // Oracle adjudicates the candidate bytes.
        let oracle = rusqlite::Connection::open(&cand).expect("oracle open candidate");
        let verdict: String = oracle
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("oracle integrity_check");
        assert_eq!(verdict, "ok", "oracle rejects fsqlite WR candidate bytes");
        let oracle_rows_out = oracle_rows(&oracle, "SELECT k, v FROM t").expect("oracle read");
        assert_eq!(
            oracle_rows_out,
            vec![
                vec!["1".to_owned(), "'beta'".to_owned()],
                vec!["2".to_owned(), "'gamma'".to_owned()],
                vec!["3".to_owned(), "'alpha'".to_owned()],
            ],
            "oracle rows on candidate must be PK-ordered and correct"
        );

        // fsqlite reopens its own VACUUM output.
        let reopened = Connection::open(&cand)
            .await
            .expect("fsqlite reopen candidate");
        let frank = frank_rows(&reopened, "SELECT k, v FROM t")
            .await
            .expect("frank read");
        assert_eq!(frank.len(), 3, "fsqlite must read back all rows");
        reopened.close().await.expect("close candidate");
    });
}
