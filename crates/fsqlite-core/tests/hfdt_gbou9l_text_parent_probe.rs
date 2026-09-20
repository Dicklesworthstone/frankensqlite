//! Generic SQL regression for indexed text-key FK probes; not provider evidence.
use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

struct ProfileGuard;
impl Drop for ProfileGuard {
    fn drop(&mut self) {
        set_hot_path_profile_enabled(false);
    }
}

#[test]
fn indexed_text_parent_lookup_avoids_a_scan_per_child() {
    asupersync::test_utils::run_test(|| async {
        for suffix in ["", " WITHOUT ROWID"] {
            for collation in ["BINARY", "NOCASE"] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("keys.db");
                let stock = rusqlite::Connection::open(&path).unwrap();
                let child_key = if collation == "NOCASE" {
                    "upper(key)"
                } else {
                    "key"
                };
                stock.execute_batch(&format!(
                    "PRAGMA foreign_keys=OFF;
                     CREATE TABLE parent(key TEXT COLLATE {collation} PRIMARY KEY){suffix};
                     CREATE TABLE child(id INTEGER PRIMARY KEY, key TEXT COLLATE BINARY REFERENCES parent(key));
                     WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<255)
                     INSERT INTO parent SELECT printf('key-%04d',i) FROM n;
                     INSERT INTO child(key) SELECT {child_key} FROM parent;
                     INSERT INTO child VALUES(257,'missing-key'),(258,NULL);"
                )).unwrap();
                let expected = stock
                    .prepare("PRAGMA foreign_key_check")
                    .unwrap()
                    .query_map([], |r| {
                        Ok(vec![
                            SqliteValue::Text(r.get::<_, String>(0)?.into()),
                            SqliteValue::Integer(r.get(1)?),
                            SqliteValue::Text(r.get::<_, String>(2)?.into()),
                            SqliteValue::Integer(r.get(3)?),
                        ])
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(
                    expected.len(),
                    1,
                    "orphan rejected; NULL and parent collation respected"
                );
                let comparisons = [
                    "c.key = p.key",
                    "p.key = c.key COLLATE BINARY",
                    "p.key = c.key COLLATE NOCASE",
                ];
                let controls = comparisons.map(|comparison| {
                    let sql = format!(
                        "SELECT c.id FROM main.child AS c WHERE c.key IS NOT NULL AND NOT EXISTS \
                         (SELECT 1 FROM main.parent AS p WHERE {comparison}) ORDER BY c.id"
                    );
                    let expected = stock
                        .prepare(&sql)
                        .unwrap()
                        .query_map([], |r| r.get::<_, i64>(0))
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    if collation == "NOCASE" && comparison != "p.key = c.key COLLATE NOCASE" {
                        assert_eq!(
                            expected.len(),
                            257,
                            "negative collation control must distinguish operand precedence"
                        );
                    }
                    (sql, expected)
                });
                stock.close().unwrap();
                let conn = Connection::open(path.to_string_lossy()).await.unwrap();
                set_hot_path_profile_enabled(true);
                let guard = ProfileGuard;
                let before = hot_path_profile_snapshot().vdbe.opcodes_executed_total;
                let actual = conn.query("PRAGMA main.foreign_key_check").await.unwrap();
                let ops = hot_path_profile_snapshot().vdbe.opcodes_executed_total - before;
                drop(guard);
                assert_eq!(
                    actual
                        .iter()
                        .map(|r| r.values().to_vec())
                        .collect::<Vec<_>>(),
                    expected
                );
                for (sql, expected) in &controls {
                    let rows = conn.query(sql).await.unwrap();
                    assert_eq!(
                        rows.iter()
                            .map(|r| r.values()[0].clone())
                            .collect::<Vec<_>>(),
                        expected
                            .iter()
                            .copied()
                            .map(SqliteValue::Integer)
                            .collect::<Vec<_>>(),
                        "operand order and explicit COLLATE must retain their meaning: {sql}"
                    );
                }
                conn.execute("CREATE TEMP TABLE parent(key INTEGER)")
                    .await
                    .unwrap();
                let shadowed = conn.query("PRAGMA main.foreign_key_check").await.unwrap();
                assert_eq!(
                    shadowed
                        .iter()
                        .map(|r| r.values().to_vec())
                        .collect::<Vec<_>>(),
                    expected
                );
                conn.close().await.unwrap();
                eprintln!(
                    "text_parent suffix={suffix:?} collation={collation} children=258 parents=256 opcodes={ops}"
                );
                assert!(
                    ops < 256 * 200,
                    "indexed text parent lookup scanned repeatedly: {ops}"
                );
            }
        }
    });
}
