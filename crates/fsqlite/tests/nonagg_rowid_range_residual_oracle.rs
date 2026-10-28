//! bd-nonagg-rowid-range-residual: `SELECT ... FROM t WHERE <rowid> <range> AND <residual>` walks only
//! the [lower, upper] rowid slice (rowid order = full-scan order) and filters the residual per row,
//! instead of full-scanning. The planner emits a FullTableScan directive (it models neither the rowid
//! range nor a non-indexed residual as seekable), which this hoists a residual-aware range scan ahead
//! of. Byte-identical to the full scan: same rows, same order, fewer visited. No secondary index here,
//! so every residual is non-seekable and the directive stays FullTableScan. Byte-set-identical to
//! C SQLite. An integer-literal LIMIT/OFFSET keeps the walk (bd-qdve2); those cases compare in
//! order.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;
fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
async fn frank(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    let mut r: Vec<Vec<String>> = c
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    r.sort();
    r
}
fn sqlite(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    let mut r: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                    rusqlite::types::Value::Null => "NULL".to_owned(),
                    rusqlite::types::Value::Integer(x) => x.to_string(),
                    rusqlite::types::Value::Real(f) => format!("{f:?}"),
                    rusqlite::types::Value::Text(s) => format!("'{s}'"),
                    rusqlite::types::Value::Blob(b) => format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    ),
                });
            }
            Ok(out)
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    r.sort();
    r
}
async fn has_seek(c: &Connection, sql: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}")).await.unwrap().iter().any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with("Seek")))
}
async fn setup(ddl: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in ddl {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}
async fn ins(f: &Connection, r: &rusqlite::Connection, s: &str) {
    f.execute(s).await.unwrap();
    r.execute_batch(s).unwrap();
}
/// Compares rows in the order each engine returns them, with integer parameters bound.
async fn cmp_ordered(f: &Connection, r: &rusqlite::Connection, sql: &str, params: &[i64], l: &str) {
    let fparams: Vec<SqliteValue> = params.iter().map(|&p| SqliteValue::Integer(p)).collect();
    let frank_rows: Vec<Vec<String>> = f
        .query_with_params(sql, &fparams)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    let mut stmt = r.prepare(sql).unwrap();
    let n = stmt.column_count();
    let sqlite_rows: Vec<Vec<String>> = stmt
        .query_map(rusqlite::params_from_iter(params), |row| {
            Ok((0..n)
                .map(|i| match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                    rusqlite::types::Value::Integer(x) => SqliteValue::Integer(x),
                    rusqlite::types::Value::Text(s) => SqliteValue::Text(s.into()),
                    rusqlite::types::Value::Null => SqliteValue::Null,
                    other => panic!("unexpected value {other:?}"),
                })
                .map(|v| render(&v))
                .collect())
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(frank_rows, sqlite_rows, "[{l}] diverged: `{sql}`");
}
async fn cmp(f: &Connection, r: &rusqlite::Connection, sql: &str, l: &str) {
    assert_eq!(
        frank(f, sql).await,
        sqlite(r, sql),
        "[{l}] diverged: `{sql}`"
    );
}
#[test]
fn nonagg_rowid_range_residual_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // No secondary index: every residual predicate is non-seekable -> FullTableScan directive.
        let (f, r) =
            setup(&["CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);"])
                .await;
        for i in 1..=600_i64 {
            let a = if i % 17 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", i % 20)
            };
            let c = if i % 19 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", i % 12)
            };
            ins(
                &f,
                &r,
                &format!("INSERT INTO t VALUES ({i}, {a}, {c}, 'v{}');", i % 7),
            )
            .await;
        }
        // Every case has a LOWER bound -> SeekGE/SeekGT is emitted (has_seek proves the hoist fired).
        let seeks = [
            "SELECT x FROM t WHERE id BETWEEN 50 AND 120 AND c = 5",
            "SELECT id FROM t WHERE id > 400 AND id < 450 AND a > 10",
            "SELECT * FROM t WHERE id >= 100 AND id <= 200 AND x = 'v3'",
            "SELECT c FROM t WHERE id BETWEEN 1 AND 50 AND c IS NULL",
            "SELECT id FROM t WHERE id BETWEEN 5 AND 10 AND c = 999", // residual FALSE for all -> empty
            "SELECT id, a FROM t WHERE id BETWEEN 300 AND 320 AND a = 6",
            "SELECT id FROM t WHERE id > 590 AND c = 3", // open upper, lower-bounded
            "SELECT x FROM t WHERE id BETWEEN 100 AND 300 AND c > 3 AND x = 'v2'", // multi-conjunct residual
            "SELECT id FROM t WHERE id >= 17 AND id <= 40 AND a IS NULL", // NULL residual (34 % 17 == 0)
        ];
        for sql in seeks {
            cmp(&f, &r, sql, "rowid-range-resid").await;
            assert!(
                has_seek(&f, sql).await,
                "rowid range+residual must seek: `{sql}`"
            );
        }
        // A literal LIMIT/OFFSET keeps the residual walk (bd-qdve2). LIMIT picks rows by scan
        // order, so these compare in order.
        let limited = [
            "SELECT id, x FROM t WHERE id > 100 AND c = 5 LIMIT 7",
            "SELECT id FROM t WHERE id > 100 AND c = 5 LIMIT 7 OFFSET 3",
            "SELECT id, a FROM t WHERE id > 250 AND a > 10 ORDER BY id LIMIT 20",
            "SELECT id FROM t WHERE id < 500 AND a > 10 ORDER BY id DESC LIMIT 15 OFFSET 2",
            "SELECT * FROM t WHERE id BETWEEN 50 AND 400 AND x = 'v3' LIMIT 0",
            "SELECT id FROM t WHERE id >= 590 AND c IS NULL LIMIT 100",
            "SELECT id FROM t WHERE id > 10 AND c = 999 LIMIT 5",
            "SELECT t.id, t.c FROM t WHERE t.rowid > 300 AND (t.c > 3 OR t.a IS NULL) \
             ORDER BY t.rowid LIMIT 25",
            // Two bounds on one side: the seek takes the tighter, the filter checks both.
            "SELECT id FROM t WHERE t.rowid > 400 AND id > 0 ORDER BY t.rowid LIMIT 30",
            "SELECT id FROM t WHERE id > 450 AND rowid >= 100 AND c <> 1 LIMIT 12",
            "SELECT id, a FROM t WHERE id BETWEEN 10 AND 500 AND id < 200 AND a = 3",
            "SELECT id FROM t WHERE id <= 580 AND id < 590 AND id > 560 AND id >= 300",
            "SELECT id FROM t WHERE id > 100 AND id > -5 AND id < 400 AND a = 7 \
             ORDER BY id DESC LIMIT 6",
        ];
        for sql in limited {
            cmp_ordered(&f, &r, sql, &[], "rowid-range-resid-limit").await;
            assert!(
                has_seek(&f, sql).await,
                "rowid range+residual with a literal LIMIT must seek: `{sql}`"
            );
        }
        // A residual placeholder under a literal LIMIT keeps its number; a placeholder LIMIT
        // keeps the full scan.
        cmp_ordered(
            &f,
            &r,
            "SELECT id FROM t WHERE id > 100 AND c = ? LIMIT 4",
            &[5],
            "resid-param",
        )
        .await;
        cmp_ordered(
            &f,
            &r,
            "SELECT id FROM t WHERE id > ?2 AND c = ?1 LIMIT 4",
            &[5, 200],
            "resid-param-numbered",
        )
        .await;
        cmp_ordered(
            &f,
            &r,
            "SELECT id FROM t WHERE id > 100 AND c = 5 LIMIT ?",
            &[3],
            "param-limit",
        )
        .await;
        // A placeholder bound beside a literal one stays in the filter.
        cmp_ordered(
            &f,
            &r,
            "SELECT id FROM t WHERE id > ? AND id > 300 AND c = ? LIMIT 9",
            &[350, 4],
            "param-bound-and-literal-bound",
        )
        .await;
        for sql in [
            "SELECT count(*), sum(c) FROM t WHERE id > 100 AND id > 200 AND c > 2",
            "SELECT max(a), min(x) FROM t WHERE id >= 50 AND rowid < 400 AND id < 300 AND a < 9",
        ] {
            cmp_ordered(&f, &r, sql, &[], "aggregate-two-bounds").await;
        }
        // Bare rowid range (no residual) still seeks byte-identically.
        for sql in [
            "SELECT id FROM t WHERE id BETWEEN 10 AND 20",
            "SELECT x FROM t WHERE id > 595",
        ] {
            cmp(&f, &r, sql, "bare").await;
            assert!(
                has_seek(&f, sql).await,
                "bare rowid range must seek: `{sql}`"
            );
        }
    });
}
