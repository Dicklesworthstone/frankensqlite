//! bd-sque1: an UPSERT `DO UPDATE SET` that rewrites the PRIMARY KEY / UNIQUE
//! key onto a DIFFERENT existing row must raise a constraint error and leave the
//! table unchanged — never silently clobber (REPLACE) and lose the other row.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn ints(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<i64>> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Integer(n) => *n,
                    other => panic!("expected integer, got {other:?}"),
                })
                .collect()
        })
        .collect()
}

#[test]
fn bd_sque1_do_update_pk_collision_aborts_without_corruption() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1,10),(2,20);")
            .await
            .unwrap();

        // DO UPDATE SET k=2 rewrites row 1's PK onto existing row 2 → must error.
        let err = conn
            .execute("INSERT INTO t VALUES(1,5) ON CONFLICT(k) DO UPDATE SET k=2;")
            .await;
        assert!(
            err.is_err(),
            "PK collision on DO UPDATE must raise an error"
        );

        // The table must be UNCHANGED: both original rows intact, nothing lost.
        let rows = conn.query("SELECT k, v FROM t ORDER BY k;").await.unwrap();
        assert_eq!(
            ints(&rows),
            vec![vec![1, 10], vec![2, 20]],
            "table must be unchanged after the aborted upsert (no row lost)"
        );

        // A UNIQUE (non-PK) collision via DO UPDATE must also error and preserve rows.
        conn.execute("CREATE TABLE u(k INTEGER PRIMARY KEY, uq INTEGER UNIQUE);")
            .await
            .unwrap();
        conn.execute("INSERT INTO u VALUES(1,10),(2,20);")
            .await
            .unwrap();
        let err2 = conn
            .execute("INSERT INTO u VALUES(1,0) ON CONFLICT(k) DO UPDATE SET uq=20;")
            .await;
        assert!(err2.is_err(), "UNIQUE collision on DO UPDATE must raise");
        let urows = conn.query("SELECT k, uq FROM u ORDER BY k;").await.unwrap();
        assert_eq!(ints(&urows), vec![vec![1, 10], vec![2, 20]], "u unchanged");

        // Regression: a normal same-key upsert (no key change) still updates.
        conn.execute("CREATE TABLE w(k INTEGER PRIMARY KEY, v INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO w VALUES(1,10);").await.unwrap();
        conn.execute("INSERT INTO w VALUES(1,5) ON CONFLICT(k) DO UPDATE SET v=v+excluded.v;")
            .await
            .unwrap();
        let wrows = conn.query("SELECT k, v FROM w;").await.unwrap();
        assert_eq!(
            ints(&wrows),
            vec![vec![1, 15]],
            "same-key upsert still works"
        );

        // Regression: DO UPDATE that changes the PK to a NON-colliding value works.
        conn.execute("CREATE TABLE x(k INTEGER PRIMARY KEY, v INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO x VALUES(1,10);").await.unwrap();
        conn.execute("INSERT INTO x VALUES(1,5) ON CONFLICT(k) DO UPDATE SET k=9;")
            .await
            .unwrap();
        let xrows = conn.query("SELECT k, v FROM x;").await.unwrap();
        assert_eq!(
            ints(&xrows),
            vec![vec![9, 10]],
            "non-colliding PK change works"
        );

        conn.close().await.unwrap();
    });
}
