//! Original correlated JSON-key anti-join, without rewriting caller SQL.
//! Ordinary SQL values; these tests do not certify a provider or migration.
use fsqlite_core::connection::{Connection, hot_path_profile_snapshot, set_hot_path_profile_enabled};
use fsqlite_types::value::SqliteValue;

struct ProfileGuard;
impl Drop for ProfileGuard { fn drop(&mut self) { set_hot_path_profile_enabled(false); } }

async fn compare(conn: &Connection, stock: &rusqlite::Connection, sql: &str, json: &str) {
    let expected = stock.prepare(sql).unwrap().query_map([json], |row| row.get::<_,i64>(0)).unwrap().collect::<Result<Vec<_>,_>>().unwrap();
    let actual = conn.query_with_params(sql, &[SqliteValue::Text(json.into())]).await.unwrap();
    assert_eq!(actual.iter().map(|row| row.values()[0].clone()).collect::<Vec<_>>(), expected.into_iter().map(SqliteValue::Integer).collect::<Vec<_>>(), "{sql} {json}");
}

#[test]
fn original_json_key_exists_reuses_membership_and_preserves_sqlite_results() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let values=(0..128).map(|n| format!("({n})")).collect::<Vec<_>>().join(",");
        let setup=format!("CREATE TABLE members(ordinal INTEGER); INSERT INTO members VALUES {values}");
        conn.execute(&setup).await.unwrap(); stock.execute_batch(&setup).unwrap();
        let json=format!("[{}]",(0..128).map(|n|n.to_string()).collect::<Vec<_>>().join(","));
        let sql="SELECT m.rowid FROM members AS m WHERE NOT EXISTS (SELECT 1 FROM json_each(?1) AS item WHERE item.key=m.ordinal) ORDER BY m.rowid";
        set_hot_path_profile_enabled(true); let profile=ProfileGuard;
        let before=hot_path_profile_snapshot().statement_dispatch_background_gates;
        compare(&conn,&stock,sql,&json).await;
        let dispatches=hot_path_profile_snapshot().statement_dispatch_background_gates-before;
        eprintln!("json_exists outer_rows=128 nested_statement_dispatches={dispatches}");
        assert!((1..=16).contains(&dispatches), "repeated JSON scan: {dispatches} dispatches");
        drop(profile);
        conn.execute("INSERT INTO members VALUES(NULL)").await.unwrap(); stock.execute_batch("INSERT INTO members VALUES(NULL)").unwrap();
        for predicate in ["item.key=m.ordinal", "m.ordinal=item.key"] {
            for not in ["", "NOT "] {
                let sql=format!("SELECT m.rowid FROM members AS m WHERE {not}EXISTS (SELECT 1 FROM json_each(?1) AS item WHERE {predicate}) ORDER BY m.rowid");
                for json in ["[]", "[null,7]", "null", "42", r#"{"0":4,"01":5,"x":6}"#] {
                    compare(&conn,&stock,&sql,json).await;
                }
            }
        }
        assert!(conn.query_with_params(sql,&[SqliteValue::Text("[".into())]).await.is_err());
        assert!(stock.prepare(sql).unwrap().query(["["]).unwrap().next().is_err());
        compare(&conn,&stock,sql,&json).await;
        // A malformed input on a skipped boolean branch must stay skipped.
        compare(&conn,&stock,"SELECT m.rowid FROM members AS m WHERE 1 OR NOT EXISTS (SELECT 1 FROM json_each(?1) AS item WHERE item.key=m.ordinal) ORDER BY m.rowid", "[").await;
        // Per-row JSON is correlated and must never be shared across rows.
        compare(&conn,&stock,"SELECT m.rowid FROM members AS m WHERE NOT EXISTS (SELECT 1 FROM json_each(json_array(m.ordinal)) AS item WHERE item.key=m.ordinal) AND ?1 IS NOT NULL ORDER BY m.rowid", "[]").await;
        // Existing module overrides retain their own row/NULL semantics.
        conn.register_module("JSON_EACH", Box::new(fsqlite_func::vtab::module_factory_from::<fsqlite_ext_json::JsonTreeVtab>()));
        let expected_sql=sql.replace("json_each", "json_tree");
        let expected=stock.prepare(&expected_sql).unwrap().query_map([json.as_str()], |r|r.get::<_,i64>(0)).unwrap().collect::<Result<Vec<_>,_>>().unwrap();
        let actual=conn.query_with_params(sql,&[SqliteValue::Text(json.into())]).await.unwrap();
        assert_eq!(actual.iter().map(|r|r.values()[0].clone()).collect::<Vec<_>>(),expected.into_iter().map(SqliteValue::Integer).collect::<Vec<_>>());
        conn.close().await.unwrap();
    });
}
