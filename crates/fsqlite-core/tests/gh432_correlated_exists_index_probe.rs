//! GH#432: on a file-backed database, a correlated EXISTS whose probe key is an
//! indexed non-rowid column (TEXT PRIMARY KEY, UNIQUE or plain index) seeks the
//! index from the compiled path, instead of running the per-outer-row
//! interpreter fallback.
#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &str = "
    CREATE TABLE repos (path TEXT PRIMARY KEY);
    INSERT INTO repos VALUES ('a'), ('b'), ('c');
    CREATE TABLE repos2 (path TEXT NOT NULL);
    INSERT INTO repos2 SELECT path FROM repos;
    CREATE UNIQUE INDEX repos2_path ON repos2(path);
    CREATE TABLE repos3 (path TEXT, n INTEGER);
    INSERT INTO repos3 VALUES ('a', 1), ('a', 2), ('b', 3);
    CREATE INDEX repos3_path ON repos3(path);
    CREATE TABLE repos_nc (path TEXT COLLATE NOCASE PRIMARY KEY);
    INSERT INTO repos_nc VALUES ('a'), ('b'), ('c');
    CREATE TABLE t_txt (s TEXT UNIQUE);
    INSERT INTO t_txt VALUES ('1'), ('2');
    CREATE TABLE evidence (id INTEGER PRIMARY KEY, repo TEXT, n INTEGER);
    INSERT INTO evidence VALUES (1, 'a', 1), (2, 'b', 5), (3, 'z', 1), (4, NULL, 1), (5, 'A', 1);
";

async fn ids(conn: &Connection, sql: &str) -> Vec<i64> {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let mut ids: Vec<i64> = rows
        .iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Integer(id) => id,
            ref other => panic!("{sql}: non-integer id {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn correlated_exists_seeks_an_index_on_the_probed_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gh432.db");
    let path = path.to_str().unwrap().to_owned();
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&path).await.unwrap();
        conn.execute_batch(SETUP).await.unwrap();
        conn.close().await.unwrap();
        // A fresh connection reads rows through the pager, where the
        // interpreter fallback would run a nested statement per outer row.
        let conn = Connection::open(&path).await.unwrap();

        // Indexed probes must stay on the compiled path: the strict mode turns
        // any interpreter fallback into an error.
        conn.set_reject_mem_fallback(true);
        conn.set_strict_mem_fallback_rejection(true);
        for (sql, expected) in [
            (
                "SELECT id FROM evidence WHERE NOT EXISTS \
                 (SELECT 1 FROM repos WHERE repos.path = evidence.repo)",
                vec![3, 4, 5],
            ),
            (
                "SELECT id FROM evidence WHERE NOT EXISTS \
                 (SELECT 1 FROM repos WHERE evidence.repo = repos.path)",
                vec![3, 4, 5],
            ),
            (
                "SELECT id FROM evidence WHERE NOT EXISTS \
                 (SELECT 1 FROM repos2 WHERE repos2.path = evidence.repo)",
                vec![3, 4, 5],
            ),
            (
                "SELECT id FROM evidence WHERE EXISTS \
                 (SELECT 1 FROM repos2 WHERE repos2.path = evidence.repo)",
                vec![1, 2],
            ),
            // A plain index with duplicate keys and an inner-only residual.
            (
                "SELECT id FROM evidence WHERE EXISTS \
                 (SELECT 1 FROM repos3 WHERE repos3.path = evidence.repo AND repos3.n > 1)",
                vec![1, 2],
            ),
            // The NOCASE column is the left operand, so its collation (the
            // index's) decides the comparison.
            (
                "SELECT id FROM evidence WHERE NOT EXISTS \
                 (SELECT 1 FROM repos_nc WHERE repos_nc.path = evidence.repo)",
                vec![3, 4],
            ),
            (
                "SELECT e.id FROM evidence AS e WHERE e.repo IS NOT NULL AND NOT EXISTS \
                 (SELECT 1 FROM repos AS r WHERE r.path = e.repo)",
                vec![3, 5],
            ),
        ] {
            assert_eq!(ids(&conn, sql).await, expected, "{sql}");
        }
        let rows = conn
            .query(
                "SELECT EXISTS (SELECT 1 FROM evidence WHERE repo IS NOT NULL \
                 AND NOT EXISTS (SELECT 1 FROM repos WHERE repos.path = evidence.repo) LIMIT 1)",
            )
            .await
            .unwrap();
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        conn.set_strict_mem_fallback_rejection(false);
        conn.set_reject_mem_fallback(false);

        // Shapes an index seek would answer wrongly keep their SQL results.
        for (sql, expected) in [
            // The BINARY left operand decides; the NOCASE index cannot seek.
            (
                "SELECT id FROM evidence WHERE NOT EXISTS \
                 (SELECT 1 FROM repos_nc WHERE evidence.repo = repos_nc.path)",
                vec![3, 4, 5],
            ),
            // TEXT column against an INTEGER column compares numerically, so
            // '1' = 1 although the index orders '1' among the text keys.
            (
                "SELECT id FROM evidence WHERE EXISTS \
                 (SELECT 1 FROM t_txt WHERE t_txt.s = evidence.n)",
                vec![1, 3, 4, 5],
            ),
        ] {
            assert_eq!(ids(&conn, sql).await, expected, "{sql}");
        }
        conn.close().await.unwrap();
    });
}

/// `(id, value)` pairs in id order.
async fn pairs(conn: &Connection, sql: &str) -> Vec<(i64, SqliteValue)> {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let mut pairs: Vec<(i64, SqliteValue)> = rows
        .iter()
        .map(|row| match row.values() {
            [SqliteValue::Integer(id), value] => (*id, value.clone()),
            other => panic!("{sql}: unexpected row {other:?}"),
        })
        .collect();
    pairs.sort_by_key(|(id, _)| *id);
    pairs
}

#[test]
fn correlated_scalar_subquery_seeks_an_index_on_the_probed_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gh432_scalar.db");
    let path = path.to_str().unwrap().to_owned();
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&path).await.unwrap();
        conn.execute_batch(SETUP).await.unwrap();
        conn.close().await.unwrap();
        let conn = Connection::open(&path).await.unwrap();
        let int = SqliteValue::Integer;
        let text = |s: &str| SqliteValue::Text(s.into());
        let null = SqliteValue::Null;

        conn.set_reject_mem_fallback(true);
        conn.set_strict_mem_fallback_rejection(true);
        for (sql, expected) in [
            (
                "SELECT id, (SELECT rowid FROM repos WHERE repos.path = evidence.repo) \
                 FROM evidence",
                vec![int(1), int(2), null.clone(), null.clone(), null.clone()],
            ),
            // Duplicate keys: the first match in rowid order, as a scan finds.
            (
                "SELECT id, (SELECT n FROM repos3 WHERE repos3.path = evidence.repo) \
                 FROM evidence",
                vec![int(1), int(3), null.clone(), null.clone(), null.clone()],
            ),
            (
                "SELECT id, (SELECT n FROM repos3 WHERE repos3.path = evidence.repo AND n > 1) \
                 FROM evidence",
                vec![int(2), int(3), null.clone(), null.clone(), null.clone()],
            ),
            (
                "SELECT id, (SELECT path FROM repos_nc WHERE repos_nc.path = evidence.repo) \
                 FROM evidence",
                vec![text("a"), text("b"), null.clone(), null.clone(), text("a")],
            ),
        ] {
            let values: Vec<SqliteValue> = pairs(&conn, sql)
                .await
                .into_iter()
                .map(|(_, value)| value)
                .collect();
            assert_eq!(values, expected, "{sql}");
        }
        assert_eq!(
            ids(
                &conn,
                "SELECT id FROM evidence \
                 WHERE (SELECT n FROM repos3 WHERE repos3.path = evidence.repo) = 3",
            )
            .await,
            vec![2],
        );
        conn.set_strict_mem_fallback_rejection(false);
        conn.set_reject_mem_fallback(false);

        // TEXT key probed by an INTEGER column: '1' = 1, which a seek misses.
        let values: Vec<SqliteValue> = pairs(
            &conn,
            "SELECT id, (SELECT s FROM t_txt WHERE t_txt.s = evidence.n) FROM evidence",
        )
        .await
        .into_iter()
        .map(|(_, value)| value)
        .collect();
        assert_eq!(
            values,
            vec![text("1"), null.clone(), text("1"), text("1"), text("1")]
        );
        conn.close().await.unwrap();
    });
}
