//! bd-hyoms: an IN list on one key term of a composite index, with `col = constant` conjuncts
//! pinning the terms before it (and any right after it), seeks `(prefix.., member, suffix..)` per
//! member, as stock's `SEARCH t USING INDEX t_ab (a=? AND b=?)` does, instead of walking the IN
//! column's (or the leading equality's) whole run. The rows match rusqlite's as a set (without an
//! ORDER BY the order follows the chosen index), on a file database in strict mode so they come
//! from the program under test; the opcode checks read the probe record's field count.

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
    let mut rows: Vec<Vec<String>> = c
        .query_with_params(sql, params)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}` {params:?}: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    rows.sort();
    rows
}

fn sqlite_rows(c: &rusqlite::Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    let mut rows: Vec<Vec<String>> = stmt
        .query_map(
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
        .collect();
    rows.sort();
    rows
}

/// EXPLAIN rows as (opcode, p1, p2, p3).
async fn program(c: &Connection, sql: &str) -> Vec<(String, i64, i64, i64)> {
    let int = |v: Option<&SqliteValue>| match v {
        Some(SqliteValue::Integer(n)) => *n,
        _ => -1,
    };
    c.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN `{sql}`: {e}"))
        .iter()
        .map(|row| {
            let values = row.values();
            let op = match values.get(1) {
                Some(SqliteValue::Text(op)) => op.to_string(),
                _ => String::new(),
            };
            (
                op,
                int(values.get(2)),
                int(values.get(3)),
                int(values.get(4)),
            )
        })
        .collect()
}

/// The widest record a `SeekGE` probes with: the field count of the
/// `MakeRecord` that writes its key register.
fn widest_probe(ops: &[(String, i64, i64, i64)]) -> i64 {
    ops.iter()
        .filter(|(op, ..)| op == "SeekGE")
        .filter_map(|(_, _, _, key)| {
            ops.iter()
                .filter(|(op, _, _, dest)| op == "MakeRecord" && dest == key)
                .map(|(_, _, fields, _)| *fields)
                .max()
        })
        .max()
        .unwrap_or(0)
}

async fn setup(path: &str) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(path).await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let mut ddl = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, s TEXT, \
         x TEXT COLLATE NOCASE, r REAL, u INTEGER);"
            .to_owned(),
        "CREATE INDEX t_ab ON t(a, b);".to_owned(),
        "CREATE INDEX t_asd ON t(a, s DESC);".to_owned(),
        "CREATE INDEX t_ax ON t(a, x);".to_owned(),
        "CREATE INDEX t_ar ON t(a, r);".to_owned(),
        "CREATE INDEX t_uvw ON t(u, b, c);".to_owned(),
    ];
    for i in 1..=3000_i64 {
        // NULLs and text in the INTEGER columns; reals with integral values.
        let a = if i % 97 == 0 {
            "NULL".to_owned()
        } else if i % 101 == 0 {
            "'abc'".to_owned()
        } else {
            format!("{}", i % 40)
        };
        let b = if i % 89 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 7)
        };
        let s = if i % 83 == 0 {
            "NULL".to_owned()
        } else {
            format!("'k{}'", i % 60)
        };
        let x = format!("'{}{}'", if i % 2 == 0 { "K" } else { "k" }, i % 5);
        let r = if i % 3 == 0 {
            format!("{}", i % 4)
        } else {
            format!("{}.5", i % 4)
        };
        ddl.push(format!(
            "INSERT INTO t VALUES ({i}, {a}, {b}, {}, {s}, {x}, {r}, {});",
            i % 11,
            i % 13
        ));
    }
    for statement in &ddl {
        f.execute(statement).await.unwrap();
        r.execute_batch(statement).unwrap();
    }
    (f, r)
}

#[test]
fn in_list_with_key_equalities_probes_the_composite_prefix_and_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let int = SqliteValue::Integer;
        let text = |s: &str| SqliteValue::Text(s.into());
        // (sql, params, probe width the seek must reach)
        let probes: Vec<(&str, Vec<SqliteValue>, i64)> = vec![
            (
                "SELECT id FROM t WHERE a IN (3, 17, 39) AND b = 3",
                vec![],
                2,
            ),
            (
                "SELECT id, c FROM t WHERE a = 5 AND b IN (1, 2, 3)",
                vec![],
                2,
            ),
            ("SELECT id FROM t WHERE b IN (1, 2, 3) AND a = 5", vec![], 2),
            (
                "SELECT id FROM t WHERE a = 5 AND (b = 1 OR b = 4)",
                vec![],
                2,
            ),
            // A residual, duplicate and equal-after-affinity members, a NULL member.
            (
                "SELECT id FROM t WHERE a = 5 AND b IN (1, 2, 3) AND c > 4",
                vec![],
                2,
            ),
            (
                "SELECT id FROM t WHERE a = 5 AND b IN (1, 1, '1', 1.0, 6)",
                vec![],
                2,
            ),
            ("SELECT id FROM t WHERE a = 5 AND b IN (NULL, 2)", vec![], 2),
            (
                "SELECT id FROM t WHERE a = 'abc' AND b IN (1, 5)",
                vec![],
                2,
            ),
            (
                "SELECT id FROM t WHERE a = '5' AND b IN ('1', 2)",
                vec![],
                2,
            ),
            ("SELECT id FROM t WHERE a = NULL AND b IN (1, 2)", vec![], 2),
            (
                "SELECT id FROM t WHERE a = 5.0 AND b IN (2.0, 3)",
                vec![],
                2,
            ),
            (
                "SELECT id FROM t WHERE a = 5 AND b IN (100, 200)",
                vec![],
                2,
            ),
            // A DESC second key term; TEXT affinity on the members.
            (
                "SELECT id, s FROM t WHERE a = 5 AND s IN ('k5', 'k45', 5)",
                vec![],
                2,
            ),
            (
                "SELECT id, s FROM t WHERE a IN (5, 7, 9) AND s = 'k5'",
                vec![],
                2,
            ),
            // A NOCASE key term pinned under its own collation.
            (
                "SELECT id, x FROM t WHERE a IN (5, 7) AND x = 'K0'",
                vec![],
                2,
            ),
            // REAL: integral members and equalities.
            (
                "SELECT id, r FROM t WHERE a = 5 AND r IN (1.5, 3, 2)",
                vec![],
                2,
            ),
            (
                "SELECT id, r FROM t WHERE a IN (4, 5, 6) AND r = 1",
                vec![],
                2,
            ),
            // Three key terms, and the IN in the middle.
            (
                "SELECT id FROM t WHERE u = 3 AND b IN (2, 3) AND c = 6",
                vec![],
                3,
            ),
            (
                "SELECT id FROM t WHERE u IN (3, 4) AND b = 2 AND c = 9",
                vec![],
                3,
            ),
            // Numbered parameters.
            (
                "SELECT id FROM t WHERE a = ?1 AND b IN (?2, ?3)",
                vec![int(5), int(1), int(3)],
                2,
            ),
            (
                "SELECT id FROM t WHERE a IN (?1, ?2) AND b = ?3",
                vec![int(5), text("7"), int(2)],
                2,
            ),
            (
                "SELECT id FROM t WHERE a = ?1 AND b IN (?2, ?3)",
                vec![SqliteValue::Null, int(1), int(3)],
                2,
            ),
            // Aggregates; the first two enforce the whole WHERE from the index.
            (
                "SELECT count(*) FROM t WHERE a = 5 AND b IN (1, 2, 3)",
                vec![],
                2,
            ),
            (
                "SELECT count(*) FROM t WHERE a IN (3, 17, 39) AND b = 3",
                vec![],
                2,
            ),
            (
                "SELECT sum(c), max(id) FROM t WHERE a IN (3, 17) AND b = 3",
                vec![],
                2,
            ),
            (
                "SELECT count(*) FROM t WHERE a = 5 AND b IN (1, 2) AND c < 5",
                vec![],
                2,
            ),
        ];
        // The probe cannot pin a second term: the IN column has a NOCASE
        // collation, the equality compares under another collation than the
        // key term's, or a key term between them is not pinned.
        let one_field: Vec<&str> = vec![
            "SELECT id, x FROM t WHERE a = 5 AND x IN ('K1', 'k2')",
            "SELECT id, x FROM t WHERE a IN (5, 7) AND x = 'K0' COLLATE BINARY",
            "SELECT id FROM t WHERE u IN (3, 4) AND c = 9",
            "SELECT id FROM t WHERE a IN (5, 7) AND b > 3",
        ];
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("in_list_composite_probe.db");
        for path in [":memory:", file.to_str().unwrap()] {
            let (f, r) = setup(path).await;
            // Strict mode turns any fallback into an error.
            f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                .await
                .unwrap();
            for (sql, params, width) in &probes {
                assert_eq!(
                    frank_rows(&f, sql, params).await,
                    sqlite_rows(&r, sql, params),
                    "`{sql}` {params:?} at {path}"
                );
                let ops = program(&f, sql).await;
                assert_eq!(
                    widest_probe(&ops),
                    *width,
                    "`{sql}` must probe {width} key terms: {ops:?}"
                );
                assert!(
                    !ops.iter().any(|(op, ..)| op == "Rewind"),
                    "`{sql}` must not scan: {ops:?}"
                );
            }
            for sql in &one_field {
                assert_eq!(
                    frank_rows(&f, sql, &[]).await,
                    sqlite_rows(&r, sql, &[]),
                    "`{sql}` at {path}"
                );
                let ops = program(&f, sql).await;
                assert!(
                    widest_probe(&ops) < 2,
                    "`{sql}` must not pin a second key term: {ops:?}"
                );
            }
            // A COUNT(*) the probe answers alone reads no table row.
            for sql in [
                "SELECT count(*) FROM t WHERE a = 5 AND b IN (1, 2, 3)",
                "SELECT count(*) FROM t WHERE a IN (3, 17, 39) AND b = 3",
            ] {
                let ops = program(&f, sql).await;
                assert!(
                    !ops.iter().any(|(op, ..)| op == "SeekRowid"),
                    "`{sql}` must count index entries: {ops:?}"
                );
            }
            // GROUP BY: on a file database the substrate runs the program
            // (strict refuses its in-memory fallback); in memory the mirror
            // groups, so only the rows are compared there.
            for sql in [
                "SELECT b, count(*) FROM t WHERE a = 5 AND b IN (1, 2, 3) GROUP BY b",
                "SELECT a, sum(c) FROM t WHERE a IN (3, 17, 39) AND b = 3 GROUP BY a",
            ] {
                assert_eq!(
                    frank_rows(&f, sql, &[]).await,
                    sqlite_rows(&r, sql, &[]),
                    "`{sql}` at {path}"
                );
                assert_eq!(
                    widest_probe(&program(&f, sql).await),
                    2,
                    "`{sql}` must probe two key terms"
                );
            }
            // A prepared statement runs twice with fresh probe sets.
            let sql = "SELECT id FROM t WHERE a = ?1 AND b IN (?2, ?3)";
            let stmt = f.prepare(sql).await.unwrap();
            for params in [[int(5), int(1), int(3)], [int(7), int(2), int(2)]] {
                let mut got: Vec<Vec<String>> = stmt
                    .query_with_params(&params)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(render).collect())
                    .collect();
                got.sort();
                assert_eq!(
                    got,
                    sqlite_rows(&r, sql, &params),
                    "prepared {params:?} at {path}"
                );
            }
            drop(stmt);
            f.close().await.unwrap();
        }
    });
}
