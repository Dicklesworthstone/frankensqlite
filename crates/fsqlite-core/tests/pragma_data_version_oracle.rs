//! bd-pragma-data-version-approximation-b3dpn: `PRAGMA data_version` oracle parity.
//!
//! Stock semantics (sqlite pragma docs): `data_version` is a per-connection
//! change counter that ADVANCES when ANOTHER connection commits a change to the
//! database, and is UNCHANGED by commits — DML *or* DDL — made on the same
//! connection. FrankenSQLite previously approximated it with the schema cookie,
//! which both missed peer DML (schema cookie does not move on ordinary
//! INSERT/UPDATE/DELETE) and false-positived on own DDL (schema cookie moves on
//! own CREATE/DROP). The fix binds it to the shared MVCC commit clock
//! (`stable_commit_seq`) minus this connection's own finalized write commits.
//!
//! This test drives the identical four-case scenario against a file-backed
//! FrankenSQLite database (two connections A/B) and a file-backed stock SQLite
//! database (rusqlite, two connections), and asserts the change/no-change
//! vector matches stock on all four:
//!   (a) other-connection data commit  -> CHANGES
//!   (b) own data commit               -> unchanged
//!   (c) own DDL commit                -> unchanged
//!   (d) after reopen, peer commit     -> CHANGES

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn frank_dv(conn: &Connection) -> i64 {
    let rows = conn
        .query("PRAGMA data_version;")
        .await
        .expect("fsqlite PRAGMA data_version must succeed");
    assert_eq!(rows.len(), 1, "data_version returns exactly one row");
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("data_version must be an integer, got {other:?}"),
    }
}

fn stock_dv(conn: &rusqlite::Connection) -> i64 {
    conn.query_row("PRAGMA data_version;", [], |row| row.get(0))
        .expect("stock PRAGMA data_version must succeed")
}

/// The scenario's expected change vector, identical for stock and FrankenSQLite.
///   [ other-commit-changed, own-commit-changed, own-ddl-changed, reopen-peer-changed ]
const EXPECTED: [bool; 4] = [true, false, false, true];

#[test]
fn data_version_matches_stock_on_all_four_cases() {
    // ── Stock SQLite reference (rusqlite), file-backed, two connections ──
    let stock_vec = {
        let dir = tempfile::tempdir().expect("stock temp dir");
        let db = dir.path().join("dv_stock.db");
        let a = rusqlite::Connection::open(&db).expect("open stock A");
        a.execute_batch("CREATE TABLE t(x INTEGER);")
            .expect("stock setup DDL");
        let b = rusqlite::Connection::open(&db).expect("open stock B");

        let dv0 = stock_dv(&a);
        b.execute("INSERT INTO t VALUES (1);", [])
            .expect("stock peer insert");
        let dv1 = stock_dv(&a);
        a.execute("INSERT INTO t VALUES (2);", [])
            .expect("stock own insert");
        let dv2 = stock_dv(&a);
        a.execute_batch("CREATE TABLE t2(y INTEGER);")
            .expect("stock own DDL");
        let dv3 = stock_dv(&a);

        drop(a);
        let a2 = rusqlite::Connection::open(&db).expect("reopen stock A");
        let dv4 = stock_dv(&a2);
        b.execute("INSERT INTO t VALUES (3);", [])
            .expect("stock peer insert after reopen");
        let dv5 = stock_dv(&a2);

        [dv1 != dv0, dv2 != dv1, dv3 != dv2, dv5 != dv4]
    };
    assert_eq!(
        stock_vec, EXPECTED,
        "stock SQLite reference must exhibit the documented data_version semantics"
    );

    // ── FrankenSQLite under test, file-backed, two connections ──
    // `run_test` requires `Future<Output = ()>`, so the parity assertions live
    // inside the async block, comparing against the stock vector captured above.
    asupersync::test_utils::run_test(move || async move {
        let dir = tempfile::tempdir().expect("fsqlite temp dir");
        let db = dir
            .path()
            .join("dv_frank.db")
            .to_string_lossy()
            .into_owned();

        let a = Connection::open(&db).await.expect("open fsqlite A");
        a.execute("CREATE TABLE t(x INTEGER);")
            .await
            .expect("fsqlite setup DDL");
        let b = Connection::open(&db).await.expect("open fsqlite B");

        let dv0 = frank_dv(&a).await;
        // (a) other-connection data commit.
        b.execute("INSERT INTO t VALUES (1);")
            .await
            .expect("fsqlite peer insert");
        let dv1 = frank_dv(&a).await;
        // (b) own data commit.
        a.execute("INSERT INTO t VALUES (2);")
            .await
            .expect("fsqlite own insert");
        let dv2 = frank_dv(&a).await;
        // (c) own DDL commit.
        a.execute("CREATE TABLE t2(y INTEGER);")
            .await
            .expect("fsqlite own DDL");
        let dv3 = frank_dv(&a).await;

        // (d) reopen behavior: a fully-closed-and-reopened connection still
        // observes subsequent peer commits.
        a.close().await.expect("close fsqlite A");
        let a2 = Connection::open(&db).await.expect("reopen fsqlite A");
        let dv4 = frank_dv(&a2).await;
        b.execute("INSERT INTO t VALUES (3);")
            .await
            .expect("fsqlite peer insert after reopen");
        let dv5 = frank_dv(&a2).await;

        let frank_vec = [dv1 != dv0, dv2 != dv1, dv3 != dv2, dv5 != dv4];
        assert_eq!(
            frank_vec, stock_vec,
            "FrankenSQLite data_version change-vector must match stock SQLite: \
             [other-commit, own-commit, own-ddl, reopen-peer] \
             (frank={frank_vec:?} stock={stock_vec:?})"
        );
        assert!(
            frank_vec[0],
            "(a) another connection's data commit must bump data_version"
        );
        assert!(
            !frank_vec[1],
            "(b) this connection's own data commit must NOT bump data_version"
        );
        assert!(
            !frank_vec[2],
            "(c) this connection's own DDL must NOT bump data_version"
        );
        assert!(
            frank_vec[3],
            "(d) after reopen, a peer commit must still bump data_version"
        );
    });
}
