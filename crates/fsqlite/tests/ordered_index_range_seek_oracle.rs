//! An ORDER BY scan over an index with no equality prefix and a range on the
//! leading key (`WHERE s > ? ORDER BY s LIMIT n`) seeks the range's near bound
//! and leaves past its far bound, as stock's `SEARCH t USING INDEX t_s (s>?)`
//! does, instead of walking the index from its start and rejecting every row
//! below the range. Every case compares rows in the order each engine returns
//! them against C SQLite; the plan check proves the seek, and the controls
//! prove the lane declines where the index order is not the comparison order.
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

fn rusqlite_value(value: &SqliteValue) -> rusqlite::types::Value {
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
        rusqlite::params_from_iter(params.iter().map(rusqlite_value)),
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

/// The opcodes of the program, in address order.
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

async fn setup() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let mut ddl = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, s TEXT, c REAL, x TEXT COLLATE NOCASE);"
            .to_owned(),
        "CREATE INDEX t_a ON t(a);".to_owned(),
        "CREATE INDEX t_s ON t(s);".to_owned(),
        "CREATE INDEX t_c ON t(c);".to_owned(),
        "CREATE INDEX t_x ON t(x);".to_owned(),
    ];
    for i in 1..=3000_i64 {
        // Mixed storage classes in the INTEGER column: NULLs sort first in the
        // index, text after every number.
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
            "INSERT INTO t VALUES ({i}, {a}, {s}, {}, {x});",
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
fn leading_key_range_seeks_the_ordered_index_scan_and_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        let text = |s: &str| SqliteValue::Text(s.into());
        // (query, params, the walk's seek: `None` starts at the index's end).
        // An exclusive near bound seeks past its equal keys; an ascending walk
        // bounded only above seeks past the NULL keys.
        let seeking: Vec<(&str, Vec<SqliteValue>, Option<&str>)> = vec![
            (
                "SELECT s FROM t WHERE s > 'k2490' ORDER BY s LIMIT 3",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT s FROM t WHERE s >= 'k2490' ORDER BY s LIMIT 3",
                vec![],
                Some("SeekGE"),
            ),
            (
                "SELECT s FROM t WHERE 'k2490' < s ORDER BY s LIMIT 3",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT s FROM t WHERE s < 'k1' ORDER BY s DESC LIMIT 3",
                vec![],
                Some("SeekLT"),
            ),
            (
                "SELECT s FROM t WHERE s <= 'k1' ORDER BY s DESC LIMIT 3",
                vec![],
                Some("SeekLE"),
            ),
            (
                "SELECT id, s FROM t WHERE s BETWEEN 'k20' AND 'k21' ORDER BY s",
                vec![],
                Some("SeekGE"),
            ),
            (
                "SELECT id, s FROM t WHERE s > 'k20' AND s < 'k21' ORDER BY s DESC",
                vec![],
                Some("SeekLT"),
            ),
            // The equality is the better seek (t_a, then a sort), as stock's
            // plan is too.
            (
                "SELECT s FROM t WHERE s > 'k2' AND a = 5 ORDER BY s LIMIT 3",
                vec![],
                Some("SeekGE"),
            ),
            (
                "SELECT s FROM t WHERE s > 'k2' ORDER BY s LIMIT 3 OFFSET 5",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT s FROM t WHERE s > 'zzz' ORDER BY s",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT id, a FROM t WHERE a > 990 ORDER BY a LIMIT 50",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT id, a FROM t WHERE a < 5 ORDER BY a DESC LIMIT 20",
                vec![],
                Some("SeekLT"),
            ),
            (
                "SELECT id, a FROM t WHERE a > '990' ORDER BY a LIMIT 5",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT id, a FROM t WHERE a > 990.5 ORDER BY a LIMIT 5",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT id, a FROM t WHERE a > -5 ORDER BY a LIMIT 3",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT id, a FROM t WHERE a > NULL ORDER BY a",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT id, c FROM t WHERE c > 1490.0 ORDER BY c",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT id, c FROM t WHERE c >= 3 AND c <= 4 ORDER BY c DESC",
                vec![],
                Some("SeekLE"),
            ),
            (
                "SELECT s FROM t WHERE s > ?1 ORDER BY s LIMIT 3",
                vec![text("k2490")],
                Some("SeekGT"),
            ),
            (
                "SELECT id, a FROM t WHERE a > ?1 ORDER BY a LIMIT 3",
                vec![text("990")],
                Some("SeekGT"),
            ),
            (
                "SELECT id, a FROM t WHERE a >= ?1 AND a < ?2 ORDER BY a",
                vec![SqliteValue::Integer(10), SqliteValue::Integer(12)],
                Some("SeekGE"),
            ),
            (
                "SELECT s FROM t WHERE s > ?1 ORDER BY s",
                vec![SqliteValue::Null],
                Some("SeekGT"),
            ),
            (
                "SELECT s FROM t WHERE s > ?1 ORDER BY s DESC",
                vec![SqliteValue::Null],
                None,
            ),
            // The connection numbers an anonymous `?` before code generation,
            // so it seeks like `?1`.
            (
                "SELECT s FROM t WHERE s > ? ORDER BY s LIMIT 3",
                vec![text("k2490")],
                Some("SeekGT"),
            ),
            // Only a far bound: ascending starts past the NULL keys at the
            // index's start; descending starts at its end and leaves at the
            // bound, or at the NULL keys.
            (
                "SELECT id, a FROM t WHERE a < 3 ORDER BY a",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT id, s FROM t WHERE s < 'k1' ORDER BY s LIMIT 40",
                vec![],
                Some("SeekGT"),
            ),
            (
                "SELECT id, a FROM t WHERE a > 995 ORDER BY a DESC",
                vec![],
                None,
            ),
            (
                "SELECT id, s FROM t WHERE s > 'k0' ORDER BY s DESC",
                vec![],
                None,
            ),
        ];
        for (sql, params, seek) in &seeking {
            assert_eq!(
                frank_rows(&f, sql, params).await,
                sqlite_rows(&r, sql, params),
                "`{sql}` {params:?}"
            );
            let ops = opcodes(&f, sql).await;
            let first_seek = ops.iter().find(|op| SEEKS.contains(&op.as_str()));
            assert_eq!(
                first_seek.map(String::as_str),
                *seek,
                "`{sql}` seek: {ops:?}"
            );
            if seek.is_none() {
                assert!(
                    ops.iter().any(|op| op == "Last"),
                    "`{sql}` must start at the index's end: {ops:?}"
                );
            }
        }
        // Outside the lane, answers must still match: a NOCASE key (index order
        // is not BINARY order), a bound under OR, and a bound with an affinity.
        let other: Vec<(&str, Vec<SqliteValue>)> = vec![
            ("SELECT x FROM t WHERE x > 'K2' ORDER BY x LIMIT 5", vec![]),
            (
                "SELECT s FROM t WHERE s > 'k2' OR a = 5 ORDER BY s LIMIT 3",
                vec![],
            ),
            // A bound with an affinity of its own converts the column instead
            // (s compares numerically here), so the index order does not apply.
            (
                "SELECT s FROM t WHERE s > CAST(5 AS INTEGER) ORDER BY s LIMIT 3",
                vec![],
            ),
        ];
        for (sql, params) in &other {
            assert_eq!(
                frank_rows(&f, sql, params).await,
                sqlite_rows(&r, sql, params),
                "`{sql}` {params:?}"
            );
            let ops = opcodes(&f, sql).await;
            assert!(
                !ops.iter().any(|op| SEEKS.contains(&op.as_str())),
                "`{sql}` must not seek the leading key: {ops:?}"
            );
        }
    });
}

/// The index seek opcodes an ordered walk can start with.
const SEEKS: [&str; 4] = ["SeekGE", "SeekGT", "SeekLE", "SeekLT"];

/// The program's first index seek and the field count of its probe record (the
/// last `MakeRecord` before it).
async fn seek_probe_fields(c: &Connection, sql: &str) -> Option<(String, i64)> {
    let rows = c.query(&format!("EXPLAIN {sql}")).await.unwrap();
    let ops: Vec<(String, i64)> = rows
        .iter()
        .map(|row| {
            let values = row.values();
            let op = match values.get(1) {
                Some(SqliteValue::Text(op)) => op.to_string(),
                _ => String::new(),
            };
            let p2 = match values.get(3) {
                Some(SqliteValue::Integer(n)) => *n,
                _ => -1,
            };
            (op, p2)
        })
        .collect();
    let seek = ops
        .iter()
        .position(|(op, _)| SEEKS.contains(&op.as_str()))?;
    ops[..seek]
        .iter()
        .rev()
        .find(|(op, _)| op == "MakeRecord")
        .map(|(_, fields)| (ops[seek].0.clone(), *fields))
}

/// The same with an equality prefix on a composite index: a range on the key
/// column after the prefix (`a = ? AND b > ? ORDER BY b LIMIT n`) becomes the
/// probe's next field, and the walk leaves the prefix block past the far bound.
#[test]
fn next_key_range_after_an_equality_prefix_seeks_and_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut ddl = vec![
            "CREATE TABLE u (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s TEXT);".to_owned(),
            "CREATE INDEX u_ab ON u(a, b);".to_owned(),
            "CREATE INDEX u_as ON u(a, s);".to_owned(),
        ];
        for i in 1..=3000_i64 {
            let b = if i % 53 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", (i * 7) % 100)
            };
            ddl.push(format!(
                "INSERT INTO u VALUES ({i}, {}, {b}, 'x{}');",
                i % 10,
                i % 300
            ));
        }
        for statement in &ddl {
            f.execute(statement).await.unwrap();
            r.execute_batch(statement).unwrap();
        }
        let int = SqliteValue::Integer;
        // (query, params, first seek, probe fields): an exclusive near bound
        // seeks past its equal keys, and an ascending walk bounded only above
        // seeks past the block's NULL keys with a NULL probe field.
        let cases: Vec<(&str, Vec<SqliteValue>, &str, i64)> = vec![
            (
                "SELECT id, b FROM u WHERE a = 7 AND b > 50 ORDER BY b LIMIT 10",
                vec![],
                "SeekGT",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = 7 AND b >= 50 AND b < 60 ORDER BY b",
                vec![],
                "SeekGE",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = 7 AND b < 60 ORDER BY b DESC LIMIT 5",
                vec![],
                "SeekLT",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = 7 AND b <= 60 ORDER BY b DESC LIMIT 5",
                vec![],
                "SeekLE",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = 7 AND b > 90 ORDER BY b DESC",
                vec![],
                "SeekLE",
                1,
            ),
            (
                "SELECT id, b FROM u WHERE a = 7 AND b < 3 ORDER BY b",
                vec![],
                "SeekGT",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = 7 AND b <= 3 ORDER BY b LIMIT 4",
                vec![],
                "SeekGT",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = 7 AND 50 < b ORDER BY b LIMIT 4",
                vec![],
                "SeekGT",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = 7 AND b > '50' ORDER BY b LIMIT 4",
                vec![],
                "SeekGT",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = 7 AND b BETWEEN 10 AND 12 ORDER BY b DESC",
                vec![],
                "SeekLE",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = 7 AND b > 50 AND id > 2000 ORDER BY b LIMIT 5",
                vec![],
                "SeekGT",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = 7 AND b > 500 ORDER BY b",
                vec![],
                "SeekGT",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = ?1 AND b > ?2 ORDER BY b LIMIT 5",
                vec![int(7), int(50)],
                "SeekGT",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = ?1 AND b > ?2 ORDER BY b LIMIT 5",
                vec![int(7), SqliteValue::Null],
                "SeekGT",
                2,
            ),
            (
                "SELECT id, b FROM u WHERE a = ?1 AND b < ?2 ORDER BY b DESC",
                vec![int(7), SqliteValue::Null],
                "SeekLT",
                2,
            ),
            (
                "SELECT id, s FROM u WHERE a = 3 AND s > 'x250' ORDER BY s LIMIT 6",
                vec![],
                "SeekGT",
                2,
            ),
            (
                "SELECT id, s FROM u WHERE a = 3 AND s < 'x1' ORDER BY s DESC LIMIT 6",
                vec![],
                "SeekLT",
                2,
            ),
        ];
        for (sql, params, seek, fields) in &cases {
            assert_eq!(
                frank_rows(&f, sql, params).await,
                sqlite_rows(&r, sql, params),
                "`{sql}` {params:?}"
            );
            assert_eq!(
                seek_probe_fields(&f, sql).await,
                Some(((*seek).to_owned(), *fields)),
                "`{sql}` first seek and probe record"
            );
        }
    });
}
