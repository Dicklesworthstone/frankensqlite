// Keeper for bd-insert-select-returning-param-77yt5: a bind parameter in the
// RETURNING clause of `INSERT ... SELECT` must resolve to its bound value, not
// bind out of range during the per-row replay. Oracle: sqlite3 3.46.1 returns
// [[5,6,7]] for `INSERT INTO t(id,a) SELECT ?,? RETURNING id,a,?` bound [5,6,7].
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn insert_select_returning_bind_param_77yt5() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        f.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)")
            .await
            .unwrap();

        // RETURNING carries a bind param `?` (global index 3).
        let rows = f
            .query_with_params(
                "INSERT INTO t(id, a) SELECT ?, ? RETURNING id, a, ?",
                &[
                    SqliteValue::Integer(5),
                    SqliteValue::Integer(6),
                    SqliteValue::Integer(7),
                ],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let got: Vec<SqliteValue> = rows[0].values().to_vec();
        assert_eq!(
            got,
            vec![
                SqliteValue::Integer(5),
                SqliteValue::Integer(6),
                SqliteValue::Integer(7)
            ]
        );

        // RETURNING with no bind param still works (regression).
        let rows2 = f
            .query_with_params(
                "INSERT INTO t(id, a) SELECT ?, ? RETURNING id, a",
                &[SqliteValue::Integer(8), SqliteValue::Integer(9)],
            )
            .await
            .unwrap();
        let got2: Vec<SqliteValue> = rows2[0].values().to_vec();
        assert_eq!(got2, vec![SqliteValue::Integer(8), SqliteValue::Integer(9)]);
    });
}
