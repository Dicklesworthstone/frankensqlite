// Keeper for bd-pragma-index-list-partial-0lqvb: PRAGMA index_list's `partial`
// column must be 1 for a partial index (created with a WHERE clause) and 0
// otherwise. Oracle: sqlite3 3.46.1.
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn pragma_index_list_partial_flag_0lqvb() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("CREATE TABLE t(a INTEGER, b INTEGER)")
            .await
            .unwrap();
        c.execute("CREATE INDEX idx_partial ON t(a) WHERE b>0")
            .await
            .unwrap();
        c.execute("CREATE INDEX idx_full ON t(a)").await.unwrap();

        let rows = c
            .query_with_params("PRAGMA index_list(t)", &[])
            .await
            .unwrap();
        // columns: seq, name, unique, origin, partial
        let mut partial_by_name = std::collections::HashMap::new();
        for r in &rows {
            let v = r.values();
            if let (SqliteValue::Text(name), SqliteValue::Integer(partial)) = (&v[1], &v[4]) {
                partial_by_name.insert(name.to_string(), *partial);
            }
        }
        assert_eq!(
            partial_by_name.get("idx_partial"),
            Some(&1),
            "a partial index must report partial=1"
        );
        assert_eq!(
            partial_by_name.get("idx_full"),
            Some(&0),
            "a full index must report partial=0"
        );
    });
}
