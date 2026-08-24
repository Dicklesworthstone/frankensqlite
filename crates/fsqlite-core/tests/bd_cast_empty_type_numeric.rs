//! bd-errmsg-parity-batch4-deqcb (cast-incomplete): an EMPTY CAST type name
//! `CAST(x AS)` is legal SQL and has NUMERIC affinity — the `sqlite3AffinityType`
//! default — so it behaves identically to `CAST(x AS NUMERIC)`. Frank previously
//! rejected it with `near ")": syntax error` (parser) and, once parsed, treated
//! an empty type as BLOB affinity at three affinity sites. Verified vs the
//! rusqlite 3.53.2 oracle.
use fsqlite_core::connection::Connection;

async fn one(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) => rows
            .first()
            .map(|r| {
                r.values()
                    .iter()
                    .map(|v| v.to_text().to_string())
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .unwrap_or_else(|| "<none>".to_string()),
        Err(e) => format!("<ERR: {e}>"),
    }
}

#[test]
fn empty_cast_type_is_numeric_affinity() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();

        // --- constant / no-FROM path (interpreted + connection emit_expr) ---
        assert_eq!(one(&c, "SELECT CAST(1 AS), typeof(CAST(1 AS))").await, "1|integer");
        assert_eq!(one(&c, "SELECT CAST(1.5 AS), typeof(CAST(1.5 AS))").await, "1.5|real");
        assert_eq!(one(&c, "SELECT CAST('42abc' AS)").await, "42");
        assert_eq!(one(&c, "SELECT CAST('abc' AS)").await, "0");
        assert_eq!(one(&c, "SELECT CAST(x'41' AS)").await, "0"); // blob 'A' -> 0
        assert_eq!(one(&c, "SELECT CAST(x'3432' AS)").await, "42"); // blob "42" -> 42
        assert_eq!(one(&c, "SELECT typeof(CAST(NULL AS))").await, "null");
        // whitespace between AS and )
        assert_eq!(one(&c, "SELECT CAST(1 AS )").await, "1");

        // --- with-FROM path (VDBE emit_expr) ---
        c.execute("CREATE TABLE t(v)").await.unwrap();
        c.execute("INSERT INTO t VALUES('7abc'),('3.9x'),('zzz')").await.unwrap();
        assert_eq!(
            one(&c, "SELECT CAST(v AS), typeof(CAST(v AS)) FROM t ORDER BY rowid").await,
            "7|integer"
        );

        // --- comparison-affinity path: empty CAST compares as NUMERIC ---
        // '7abc' under NUMERIC affinity -> 7, so = 7 matches exactly one row.
        assert_eq!(
            one(&c, "SELECT count(*) FROM t WHERE CAST(v AS) = 7").await,
            "1"
        );

        // --- negatives: non-empty casts keep their affinity ---
        assert_eq!(one(&c, "SELECT typeof(CAST(1 AS BLOB))").await, "blob");
        assert_eq!(one(&c, "SELECT typeof(CAST(1 AS TEXT))").await, "text");
        assert_eq!(one(&c, "SELECT typeof(CAST('9' AS INTEGER))").await, "integer");
        assert_eq!(one(&c, "SELECT CAST('9x' AS INTEGER)").await, "9");
        assert_eq!(one(&c, "SELECT typeof(CAST(1 AS REAL))").await, "real");
    });
}
