//! A WHERE that is an OR of terms, each pinning an index's equality prefix (`a = 5 OR s = 'k77'`),
//! seeks one index per term instead of scanning the table, as stock's MULTI-INDEX OR does: a rowid
//! set drops a row an earlier term produced, and the whole WHERE still filters every row. Rows come
//! out term by term, each term's in its index's order, so they match stock's as a set (stock may
//! pick another index for a term). A term with no equality seek, an OR under an AND, and an ORDER
//! BY keep their other plans.

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

fn to_rusqlite(value: &SqliteValue) -> rusqlite::types::Value {
    match value {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
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
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map(
        rusqlite::params_from_iter(params.iter().map(to_rusqlite)),
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

/// With no ORDER BY the row order follows each term's index (stock takes the
/// covering `t_ab` for `a = 5`, in `b` order), so the rows compare as a set.
/// Under a LIMIT, which rows come out depends on that order: their count must
/// match and every one must be in the unlimited answer.
fn assert_same_rows(
    mut got: Vec<Vec<String>>,
    r: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
    label: &str,
) {
    let mut want = sqlite_rows(r, sql, params);
    if let Some(pos) = sql.find(" LIMIT ") {
        let all = sqlite_rows(r, &sql[..pos], params);
        assert_eq!(got.len(), want.len(), "{label}: row count of `{sql}`");
        for row in &got {
            assert!(
                all.contains(row),
                "{label}: `{sql}` returned {row:?}, not in its answer"
            );
        }
        return;
    }
    got.sort();
    want.sort();
    assert_eq!(got, want, "{label}: `{sql}`");
}

async fn opcodes(c: &Connection, sql: &str) -> Vec<String> {
    c.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .map(|row| match row.values().get(1) {
            Some(SqliteValue::Text(op)) => op.to_string(),
            _ => String::new(),
        })
        .collect()
}

async fn setup(path: &str) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(path).await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let mut ddl = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s TEXT, c REAL, \
         x TEXT COLLATE NOCASE);"
            .to_owned(),
        "CREATE INDEX t_a ON t(a);".to_owned(),
        "CREATE INDEX t_ab ON t(a, b);".to_owned(),
        "CREATE INDEX t_s ON t(s);".to_owned(),
        "CREATE INDEX t_x ON t(x);".to_owned(),
    ];
    for i in 1..=3000_i64 {
        // NULLs and text in the INTEGER column; `a = 5` and `s = 'k5'` share rows.
        let a = if i % 97 == 0 {
            "NULL".to_owned()
        } else if i % 101 == 0 {
            "'abc'".to_owned()
        } else {
            format!("{}", i % 1000)
        };
        let s = if i % 89 == 0 {
            "NULL".to_owned()
        } else {
            format!("'k{}'", i % 2500)
        };
        let x = format!("'{}{}'", if i % 2 == 0 { "K" } else { "k" }, i % 300);
        ddl.push(format!(
            "INSERT INTO t VALUES ({i}, {a}, {}, {s}, {}, {x});",
            i % 7,
            i as f64 * 0.5
        ));
    }
    for statement in &ddl {
        f.execute(statement).await.unwrap();
        r.execute_batch(statement).unwrap();
    }
    (f, r)
}

#[test]
fn or_of_indexed_equalities_seeks_each_index_and_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let text = |s: &str| SqliteValue::Text(s.into());
        let union: Vec<(&str, Vec<SqliteValue>)> = vec![
            ("SELECT id FROM t WHERE a = 5 OR s = 'k77'", vec![]),
            ("SELECT id, a, s FROM t WHERE s = 'k77' OR a = 5", vec![]),
            ("SELECT id, a, s FROM t WHERE a = 5 OR s = 'k5'", vec![]),
            ("SELECT id FROM t WHERE a = 1 OR a = 2 OR s = 'k3'", vec![]),
            (
                "SELECT id, b FROM t WHERE (a = 5 AND b = 5) OR s = 'k77'",
                vec![],
            ),
            (
                "SELECT id, c FROM t WHERE (a = 5 AND c > 100) OR s = 'k77'",
                vec![],
            ),
            // One column twice, the second term with a residual: no IN list.
            (
                "SELECT id FROM t WHERE a = 5 OR a = 7 AND id < 2000",
                vec![],
            ),
            (
                "SELECT id FROM t WHERE a = 5 OR s = 'k77' LIMIT 7 OFFSET 3",
                vec![],
            ),
            ("SELECT id FROM t WHERE a = 5 OR s = 'k77' LIMIT 0", vec![]),
            ("SELECT id FROM t WHERE a = 4000 OR s = 'nothing'", vec![]),
            (
                "SELECT id FROM t WHERE a = ?1 OR s = ?2",
                vec![SqliteValue::Integer(5), text("k77")],
            ),
            (
                "SELECT id FROM t WHERE a = ?1 OR s = ?2",
                vec![SqliteValue::Null, text("k77")],
            ),
            (
                "SELECT id FROM t WHERE a = ?1 OR s = ?2",
                vec![text("5"), SqliteValue::Integer(77)],
            ),
        ];
        // Terms whose equality seek depends on the literal's type or the key's
        // collation, and a NULL or unconvertible value: rows only.
        let either: Vec<&str> = vec![
            // One column's equalities: the planner reads them as an IN list.
            "SELECT id FROM t WHERE a = 5 OR a = 7",
            "SELECT id FROM t WHERE a = '5' OR s = 77",
            "SELECT id, x FROM t WHERE x = 'K5' OR a = 9",
            "SELECT id FROM t WHERE a = NULL OR s = 'k77'",
            "SELECT id FROM t WHERE a = 'abc' OR s = 'k9'",
        ];
        // A term with no equality seek, an OR under an AND, and an ORDER BY.
        let other: Vec<&str> = vec![
            "SELECT id FROM t WHERE a = 5 OR b = 3",
            "SELECT id FROM t WHERE a IS NULL OR s = 'k77'",
            "SELECT id FROM t WHERE a > 990 OR s = 'k77'",
            "SELECT id FROM t WHERE (a = 5 OR s = 'k77') AND c > 100",
            "SELECT id FROM t WHERE a = 5 OR s = 'k77' ORDER BY id DESC",
        ];
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("or_index_union.db");
        for path in [":memory:", file.to_str().unwrap()] {
            let (f, r) = setup(path).await;
            // Strict mode turns any fallback into an error, so the rows come
            // from the program under test.
            f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                .await
                .unwrap();
            for (sql, params) in &union {
                assert_same_rows(
                    frank_rows(&f, sql, params).await,
                    &r,
                    sql,
                    params,
                    &format!("{params:?} at {path}"),
                );
                let ops = opcodes(&f, sql).await;
                assert!(
                    ops.iter().any(|op| op == "RowSetTest") && !ops.iter().any(|op| op == "Rewind"),
                    "`{sql}` must seek each OR term's index, not scan the table: {ops:?}"
                );
            }
            for sql in &either {
                assert_same_rows(frank_rows(&f, sql, &[]).await, &r, sql, &[], path);
            }
            for sql in &other {
                assert_same_rows(frank_rows(&f, sql, &[]).await, &r, sql, &[], path);
                let ops = opcodes(&f, sql).await;
                assert!(
                    !ops.iter().any(|op| op == "RowSetTest"),
                    "`{sql}` must keep its other plan: {ops:?}"
                );
            }
            // A prepared statement runs twice with fresh rowid sets.
            let sql = "SELECT id FROM t WHERE a = ?1 OR s = ?2";
            let params = [SqliteValue::Integer(5), text("k5")];
            let stmt = f.prepare(sql).await.unwrap();
            for _ in 0..2 {
                let got: Vec<Vec<String>> = stmt
                    .query_with_params(&params)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(render).collect())
                    .collect();
                assert_same_rows(got, &r, sql, &params, &format!("prepared at {path}"));
            }
            drop(stmt);
            f.close().await.unwrap();
        }
    });
}
