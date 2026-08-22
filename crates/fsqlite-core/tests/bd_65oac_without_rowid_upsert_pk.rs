//! bd-65oac: `INSERT ... ON CONFLICT(<pk>) DO UPDATE/NOTHING` on a WITHOUT
//! ROWID table must accept the PRIMARY KEY as a valid conflict arbiter.
//!
//! A WITHOUT ROWID table's PRIMARY KEY is the clustering key (stored in
//! `primary_key_constraints`), not a `table.indexes` UNIQUE index nor an
//! `is_ipk` rowid alias — so both `find_upsert_target_index` and
//! `upsert_target_matches_rowid_primary_key` miss it, and the execute-time
//! target validator (bd-prepare-time-validation-bypass / d010b1c69) wrongly
//! rejected it with "ON CONFLICT clause does not match any PRIMARY KEY or
//! UNIQUE constraint". Oracle: sqlite3 3.46.1.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn cells(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<SqliteValue>> {
    rows.iter().map(|r| r.values().to_vec()).collect()
}

#[test]
fn bd_65oac_without_rowid_pk_is_valid_conflict_target() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // Single-column PRIMARY KEY on a WITHOUT ROWID table.
        conn.execute("CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID;")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES('a', 10), ('b', 2);")
            .await
            .unwrap();

        // ON CONFLICT(k) DO UPDATE — accepted, updates the conflicting row.
        conn.execute(
            "INSERT INTO t VALUES('a', 100) ON CONFLICT(k) DO UPDATE SET v = v + excluded.v;",
        )
        .await
        .unwrap();
        // ON CONFLICT(k) DO NOTHING — accepted, leaves the row.
        conn.execute("INSERT INTO t VALUES('a', 999) ON CONFLICT(k) DO NOTHING;")
            .await
            .unwrap();
        // ON CONFLICT(k) on a non-conflicting row — plain insert.
        conn.execute("INSERT INTO t VALUES('z', 26) ON CONFLICT(k) DO UPDATE SET v = excluded.v;")
            .await
            .unwrap();

        let rows = conn.query("SELECT k, v FROM t ORDER BY k;").await.unwrap();
        assert_eq!(
            cells(&rows),
            vec![
                vec![SqliteValue::Text("a".into()), SqliteValue::Integer(110)],
                vec![SqliteValue::Text("b".into()), SqliteValue::Integer(2)],
                vec![SqliteValue::Text("z".into()), SqliteValue::Integer(26)],
            ]
        );

        // Composite WITHOUT ROWID PRIMARY KEY — order-independent target match.
        conn.execute("CREATE TABLE c(a INT, b INT, n INT, PRIMARY KEY(a, b)) WITHOUT ROWID;")
            .await
            .unwrap();
        conn.execute("INSERT INTO c VALUES(1, 2, 10);")
            .await
            .unwrap();
        conn.execute(
            "INSERT INTO c VALUES(1, 2, 5) ON CONFLICT(b, a) DO UPDATE SET n = n + excluded.n;",
        )
        .await
        .unwrap();
        let rows = conn.query("SELECT n FROM c;").await.unwrap();
        assert_eq!(cells(&rows), vec![vec![SqliteValue::Integer(15)]]);

        // Regression guards: a non-PK/non-unique target still errors, and an
        // unknown target column still errors "no such column".
        assert!(
            conn.execute("INSERT INTO t VALUES('a', 1) ON CONFLICT(v) DO NOTHING;")
                .await
                .is_err(),
            "non-unique target column must still be rejected"
        );
        let err = conn
            .execute("INSERT INTO t VALUES('a', 1) ON CONFLICT(nope) DO NOTHING;")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("no such column: nope"),
            "unknown target column must error 'no such column', got: {err}"
        );

        conn.close().await.unwrap();
    });
}
