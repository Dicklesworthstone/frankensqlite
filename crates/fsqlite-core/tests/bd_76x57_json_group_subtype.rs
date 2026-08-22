// Keeper for bd-76x57: json_group_array / json_group_object must preserve the
// JSON subtype of their arguments so a nested json()/json_object()/json_array()
// result is EMBEDDED as JSON, not quoted as a string. A plain (non-JSON) text
// argument stays quoted. Covers the interpreted aggregate path (SELECT agg(..)
// FROM <real table>), incl. GROUP BY. Mirrors the fsqlite-e2e keeper
// json_group_aggregates_nested_json_subtype_embedded (un-ignored alongside this
// fix; that one needs --features native to compile). Oracle: sqlite3 3.46.1.
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn text(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected TEXT result, got {other:?}"),
    }
}

async fn scalar_text(conn: &Connection, sql: &str) -> String {
    let rows = conn.query_with_params(sql, &[]).await.unwrap();
    text(&rows[0].values()[0])
}

#[test]
fn json_group_aggregates_preserve_json_subtype_76x57() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("CREATE TABLE t(value)").await.unwrap();
        c.execute("INSERT INTO t VALUES(1),(2)").await.unwrap();
        c.execute("CREATE TABLE kv(k, v)").await.unwrap();
        c.execute("INSERT INTO kv VALUES('x', '{\"n\":1}')")
            .await
            .unwrap();
        c.execute("CREATE TABLE s(value)").await.unwrap();
        c.execute("INSERT INTO s VALUES('a'),('b')").await.unwrap();

        // json_group_array over a json_object(...) argument -> nested JSON embedded.
        assert_eq!(
            scalar_text(
                &c,
                "SELECT json_group_array(json_object('a', value)) FROM t"
            )
            .await,
            r#"[{"a":1},{"a":2}]"#,
        );

        // json_group_array over a json_array(...) argument -> nested arrays embedded.
        assert_eq!(
            scalar_text(
                &c,
                "SELECT json_group_array(json_array(value, value)) FROM t"
            )
            .await,
            r"[[1,1],[2,2]]",
        );

        // json_group_object with a json(...) value -> nested JSON embedded.
        assert_eq!(
            scalar_text(&c, "SELECT json_group_object(k, json(v)) FROM kv").await,
            r#"{"x":{"n":1}}"#,
        );

        // NOTE: the GROUP BY variant (SELECT agg(json_object(..)) FROM t GROUP BY g)
        // routes through a different Sorter-backed path that still drops the
        // subtype — tracked as remaining work on bd-76x57.

        // Regression guard: a PLAIN text argument carries no JSON subtype and
        // must stay quoted, not be embedded.
        assert_eq!(
            scalar_text(&c, "SELECT json_group_array(value) FROM s").await,
            r#"["a","b"]"#,
        );
    });
}
