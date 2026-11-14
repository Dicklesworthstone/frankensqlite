//! bd-z62ru: an aggregate whose WHERE is exactly `col IS NULL`, on an index whose ascending first
//! key term is `col`, walks the index's leading NULL keys and stops at the first non-NULL one, as
//! stock's `SEARCH t USING COVERING INDEX t_n (n=?)` does, instead of scanning the table. The
//! answers match rusqlite in memory and on a file database in strict mode; a DESC key, a partial
//! index, IS NOT NULL, `IS ?1` and a residual keep their other plans and still match.

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

async fn frank_rows(c: &Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    c.query_with_params(sql, params)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}` {params:?}: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows(c: &rusqlite::Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    let to_value = |value: &SqliteValue| match value {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    };
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map(
        rusqlite::params_from_iter(params.iter().map(to_value)),
        |row| {
            Ok((0..n)
                .map(|i| match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                    rusqlite::types::Value::Null => "NULL".to_owned(),
                    rusqlite::types::Value::Integer(x) => x.to_string(),
                    rusqlite::types::Value::Real(f) => format!("{f:?}"),
                    rusqlite::types::Value::Text(s) => format!("'{s}'"),
                    rusqlite::types::Value::Blob(b) => format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    ),
                })
                .collect())
        },
    )
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// EXPLAIN rows as (opcode, p4 text).
async fn program(c: &Connection, sql: &str) -> Vec<(String, String)> {
    c.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN `{sql}`: {e}"))
        .iter()
        .map(|row| {
            let text = |i: usize| match row.values().get(i) {
                Some(SqliteValue::Text(s)) => s.to_string(),
                _ => String::new(),
            };
            (text(1), text(5))
        })
        .collect()
}

#[test]
fn aggregate_over_is_null_walks_the_leading_null_keys_and_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let schema = [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT, a INTEGER, b INTEGER, x INTEGER, \
             y INTEGER, c REAL);",
            "CREATE INDEX t_n ON t(n);",
            // a leads only a composite index.
            "CREATE INDEX t_ab ON t(a, b);",
            // NULLs sort last under a DESC first key term.
            "CREATE INDEX t_x ON t(x DESC);",
            // A partial index holds no NULL y.
            "CREATE INDEX t_y ON t(y) WHERE y IS NOT NULL;",
            "CREATE TABLE e (id INTEGER PRIMARY KEY, n TEXT);",
            "CREATE INDEX e_n ON e(n);",
            "CREATE TABLE f (id INTEGER PRIMARY KEY, n TEXT);",
            "CREATE INDEX f_n ON f(n);",
            "INSERT INTO f VALUES (1, 'a'), (2, ''), (3, 'b');",
        ];
        let mut rows = Vec::new();
        for i in 1..=1500_i64 {
            let nullable = |every: i64, value: String| {
                if i % every == 0 {
                    "NULL".to_owned()
                } else {
                    value
                }
            };
            rows.push(format!(
                "INSERT INTO t VALUES ({i}, {}, {}, {}, {}, {}, {});",
                nullable(10, format!("'n{}'", i % 300)),
                nullable(7, format!("{}", i % 50)),
                i % 11,
                nullable(9, format!("{}", i % 40)),
                nullable(8, format!("{}", i % 30)),
                nullable(5, format!("{}.5", i % 20)),
            ));
        }
        let seeks = [
            "SELECT count(*) FROM t WHERE n IS NULL",
            "SELECT count(n), count(id) FROM t WHERE n IS NULL",
            "SELECT min(id), max(id), sum(id) FROM t WHERE n IS NULL",
            "SELECT group_concat(id) FROM t WHERE n IS NULL",
            "SELECT sum(c), max(a) FROM t WHERE n IS NULL",
            "SELECT count(*) FROM t WHERE a IS NULL",
            "SELECT sum(b) FROM t WHERE a IS NULL",
            "SELECT count(*) FROM e WHERE n IS NULL",
            "SELECT count(*), max(id) FROM f WHERE n IS NULL",
        ];
        // (query, the index it must not walk)
        let others = [
            ("SELECT count(*) FROM t WHERE x IS NULL", "t_x"),
            ("SELECT count(*) FROM t WHERE y IS NULL", "t_y"),
            ("SELECT count(*) FROM t WHERE n IS NULL AND id > 700", "t_n"),
            ("SELECT count(*) FROM t WHERE c IS NULL", "t_n"),
        ];
        let rows_only = ["SELECT count(*) FROM t WHERE n IS NOT NULL"];
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("is_null_index_seek.db");
        for path in [":memory:", file.to_str().unwrap()] {
            let f = Connection::open(path).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for stmt in schema
                .iter()
                .copied()
                .chain(rows.iter().map(String::as_str))
            {
                f.execute(stmt).await.unwrap();
                r.execute_batch(stmt).unwrap();
            }
            if path != ":memory:" {
                f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                    .await
                    .unwrap();
            }
            for sql in seeks
                .iter()
                .chain(others.iter().map(|(sql, _)| sql))
                .chain(&rows_only)
            {
                assert_eq!(
                    frank_rows(&f, sql, &[]).await,
                    sqlite_rows(&r, sql, &[]),
                    "`{sql}` at {path}"
                );
            }
            for sql in seeks {
                let ops = program(&f, sql).await;
                let walks_index = ops
                    .iter()
                    .any(|(op, p4)| op == "OpenRead" && (p4.contains("_n") || p4.contains("t_ab")));
                assert!(
                    walks_index && ops.iter().any(|(op, _)| op == "NotNull"),
                    "`{sql}` must walk the index's NULL keys: {ops:?}"
                );
            }
            // Covering: the count reads no table row; a column the index lacks does.
            let count = program(&f, "SELECT count(*) FROM t WHERE n IS NULL").await;
            assert!(
                !count.iter().any(|(op, _)| op == "SeekRowid"),
                "count(*) WHERE n IS NULL must read only t_n: {count:?}"
            );
            let sum = program(&f, "SELECT sum(c), max(a) FROM t WHERE n IS NULL").await;
            assert!(
                sum.iter().any(|(op, _)| op == "SeekRowid"),
                "sum(c) WHERE n IS NULL must read the row: {sum:?}"
            );
            for (sql, index) in others {
                let ops = program(&f, sql).await;
                assert!(
                    !ops.iter()
                        .any(|(op, p4)| op == "OpenRead" && p4.contains(index)),
                    "`{sql}` must not walk {index}: {ops:?}"
                );
            }
            // `n IS ?1` is an IS comparison, not this walk.
            for value in [SqliteValue::Null, SqliteValue::Text("n7".into())] {
                let sql = "SELECT count(*) FROM t WHERE n IS ?1";
                assert_eq!(
                    frank_rows(&f, sql, std::slice::from_ref(&value)).await,
                    sqlite_rows(&r, sql, std::slice::from_ref(&value)),
                    "`{sql}` with {value:?} at {path}"
                );
            }
            f.close().await.unwrap();
        }
    });
}
