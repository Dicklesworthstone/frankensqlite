//! bd-4uurt: a prepared statement on a file database runs a GROUP BY with a
//! HAVING through the VDBE grouping substrate when its output registers can
//! supply the predicate. Previously an IS comparison read columns without a
//! group context. Thus
//!
//! ```sql
//! SELECT a, b, count(*) FROM t GROUP BY a, b HAVING a IS 2;
//! ```
//!
//! kept no group where stock SQLite 3.54.1 keeps 2|2|1, and so did `a + 1 =
//! 2`, `abs(a) = 1`, `NOT (a = 2)`, BETWEEN, IN, CASE, `||`, `a IS TRUE`.
//! Direct queries formerly used the interpreted path even for simple
//! predicates. Strict file queries also exercise the compiled path directly.
//!
//! The general matrix covers memory and file databases. A separate strict
//! file matrix checks nullable keys and projected aggregates. Both compare
//! the direct and prepared results with bundled SQLite 3.54.1.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> String {
    let result = (|| -> rusqlite::Result<Vec<Vec<String>>> {
        let mut statement = conn.prepare(sql)?;
        let columns = statement.column_count();
        let mut rows = Vec::new();
        let mut cursor = statement.query([])?;
        while let Some(row) = cursor.next()? {
            rows.push(
                (0..columns)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect(),
            );
        }
        Ok(rows)
    })();
    match result {
        Ok(rows) => format!("rows={rows:?}"),
        Err(
            rusqlite::Error::SqliteFailure(_, Some(message))
            | rusqlite::Error::SqlInputError { msg: message, .. },
        ) => format!("error: {message}"),
        Err(error) => format!("error: {error}"),
    }
}

async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let result = if prepared {
        match conn.prepare(sql).await {
            Ok(statement) => statement.query().await,
            Err(error) => Err(error),
        }
    } else {
        conn.query(sql).await
    };
    match result {
        Ok(rows) => {
            let rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.values().iter().map(render_fsqlite).collect())
                .collect();
            format!("rows={rows:?}")
        }
        Err(error) => format!("error: {error}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(a, b, v)",
    "INSERT INTO t VALUES (1, 2, 10), (1, 2, 11), (1, 3, 12), (2, 2, 13)",
];

const G12: &str = "[\"1\", \"2\", \"2\"]";
const G13: &str = "[\"1\", \"3\", \"1\"]";
const G22: &str = "[\"2\", \"2\", \"1\"]";

/// Each HAVING with the groups stock keeps (as `a|b|count` rows in order).
const HAVINGS: &[(&str, &[&str])] = &[
    ("a IS 2", &[G22]),
    ("a IS NOT 1", &[G22]),
    ("(a, b) IS (2, 2)", &[G22]),
    ("count(*) >= 1 AND a IS 2", &[G22]),
    ("a IS TRUE", &[G12, G13, G22]),
    ("a + 1 = 2", &[G12, G13]),
    ("b - a = 1 OR a = 2", &[G12, G22]),
    ("abs(a) = 1", &[G12, G13]),
    ("typeof(a) = 'integer' AND b = 3", &[G13]),
    ("NOT (a = 2)", &[G12, G13]),
    ("a BETWEEN 1 AND 1", &[G12, G13]),
    ("a IN (2)", &[G22]),
    ("CASE WHEN a = 2 THEN 1 ELSE 0 END", &[G22]),
    ("b || '' = '3'", &[G13]),
    // Controls the substrate already coded right, and an aggregate inside an
    // expression (bd-23kpa's interpreted shape).
    ("a = 1", &[G12, G13]),
    ("count(*) > 1", &[G12]),
    ("sum(v) > 20", &[G12]),
    ("a IS NULL", &[]),
    ("count(*) * 2 > 3", &[G12]),
];

#[test]
fn prepared_group_by_having_keeps_stocks_groups() {
    let queries: Vec<(String, String)> = HAVINGS
        .iter()
        .map(|(having, groups)| {
            (
                format!("SELECT a, b, count(*) FROM t GROUP BY a, b HAVING {having} ORDER BY a, b"),
                format!("rows=[{}]", groups.join(", ")),
            )
        })
        .collect();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    // Not vacuous: stock keeps each pinned set of groups.
    for (sql, pinned) in &queries {
        assert_eq!(&stock_outcome(&stock, sql), pinned, "stock on `{sql}`");
    }
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_4uurt.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for (sql, expected) in &queries {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected,
                        "file_backed={file_backed} prepared={prepared}: `{sql}`"
                    );
                }
                conn.close().await.unwrap();
            }
        }
    });
}

#[test]
fn strict_file_having_uses_finalized_keys_and_aggregates() {
    const DATA: &str = "INSERT INTO t VALUES (NULL,NULL),(1,0),(2,3),(2,4)";
    const CREATE: &str = "CREATE TABLE t(k INTEGER,v INTEGER)";
    let cases: &[(&str, &[Option<i64>])] = &[
        ("k IS '2'", &[Some(2)]),
        ("k = '2'", &[Some(2)]),
        ("src.K IS '2'", &[Some(2)]),
        ("sum(src.V) IS 7", &[Some(2)]),
        ("k IS NOT 2", &[None, Some(1)]),
        ("k IS NULL", &[None]),
        ("k IS NOT NULL", &[Some(1), Some(2)]),
        ("k IS TRUE", &[Some(1), Some(2)]),
        ("k IS NOT TRUE", &[None]),
        ("sum(v) IS 7", &[Some(2)]),
        ("sum(v) = 7", &[Some(2)]),
        ("sum(v) IS NOT 7", &[None, Some(1)]),
        ("sum(v) IS NULL", &[None]),
        ("sum(v) IS NOT NULL", &[Some(1), Some(2)]),
        ("k IS sum(v)", &[None]),
        ("k = sum(v)", &[]),
        ("kept IS 1 AND count(v) IS 2", &[Some(2)]),
    ];
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    stock.execute_batch(CREATE).unwrap();
    stock.execute_batch(DATA).unwrap();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("strict.db");
        let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
        conn.execute(CREATE).await.unwrap();
        conn.execute(DATA).await.unwrap();
        conn.execute("PRAGMA fsqlite.parity_cert_strict=ON")
            .await
            .unwrap();
        assert_eq!(
            fsqlite_outcome(&conn, "PRAGMA fsqlite.parity_cert_strict", false).await,
            "rows=[[\"1\"]]"
        );
        for &(predicate, keys) in cases {
            let sql = format!(
                "SELECT k, sum(v), count(v), count(v) FILTER (WHERE v>3) AS kept FROM t AS src GROUP BY k HAVING {predicate} ORDER BY k"
            );
            let expected = stock_outcome(&stock, &sql);
            let actual_keys = stock
                .prepare(&sql)
                .unwrap()
                .query_map([], |row| row.get::<_, Option<i64>>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(actual_keys, keys, "stock on {predicate}");
            for prepared in [false, true] {
                assert_eq!(
                    fsqlite_outcome(&conn, &sql, prepared).await,
                    expected,
                    "prepared={prepared}: {sql}"
                );
            }
        }
        conn.close().await.unwrap();
    });
}

/// bd-w4pr1.13.32: strict file-backed GROUP BY + HAVING (zero fallback) AND
/// unloaded `:memory:` storage-substrate execution across HAVING-only
/// aggregates, aggregates inside arbitrary expressions, aliases inside
/// expressions, unprojected group keys, DISTINCT/FILTER HAVING aggregates,
/// TEXT-vs-aggregate affinity, indexed streaming + LIMIT/OFFSET, parameterized
/// HAVING, and empty tables — all compared live against bundled SQLite 3.54.1.
#[test]
fn strict_file_and_memory_substrate_group_by_having_expressions_match_stock() {
    const SCHEMA: &[&str] = &[
        "CREATE TABLE hav(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER, tag TEXT)",
        "INSERT INTO hav VALUES \
         (1,'a',10,'x'),(2,'a',20,'y'),(3,'a',30,'x'),\
         (4,'b',40,'z'),(5,'b',50,'z'),(6,'c',5,'w'),(7,NULL,100,'n')",
        "CREATE TABLE hav_idx(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
        "INSERT INTO hav_idx VALUES (1,'a',10),(2,'a',20),(3,'a',30),(4,'b',40),(5,'b',50),(6,'c',5)",
        "CREATE INDEX idx_hav_grp_val ON hav_idx(grp, val)",
        "CREATE TABLE hav_empty(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
        "CREATE TABLE num_txt(k TEXT, v INTEGER)",
        "INSERT INTO num_txt VALUES ('2', 10), ('2', 20), ('1', 5), ('02', 99)",
    ];
    let queries: &[&str] = &[
        // All bd-23kpa / bd-w4pr1.13.32 expression and HAVING-only shapes:
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING count(*) * 2 > 3 ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING 2 * count(*) > 3 ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING count(*) + 0 > 1 ORDER BY grp",
        "SELECT grp, count(*) c FROM hav GROUP BY grp HAVING c + 0 > 1 ORDER BY grp",
        "SELECT grp, count(*) c FROM hav GROUP BY grp HAVING c * 2 > 3 ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING sum(val) > 50 ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING sum(val) + 1 > 50 ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING -count(*) < -2 ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING abs(count(*)) > 2 ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING count(*) BETWEEN 2 AND 3 ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING count(*) IN (2, 3) ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING max(val) - min(val) > 15 ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING count(*) > 2 AND sum(val) > 50 ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING count(*) > 2 OR sum(val) > 80 ORDER BY grp",
        // Aggregate ONLY in HAVING (no aggregate in SELECT list):
        "SELECT grp FROM hav GROUP BY grp HAVING count(*) = 2 ORDER BY grp",
        "SELECT grp FROM hav GROUP BY grp HAVING count(*) != 2 ORDER BY grp",
        "SELECT grp FROM hav GROUP BY grp HAVING sum(val) > 50 ORDER BY grp",
        "SELECT grp FROM hav GROUP BY grp HAVING count(*) > 1 AND sum(val) < 100 ORDER BY grp",
        "SELECT grp FROM hav GROUP BY grp HAVING count(*) = 1 OR sum(val) > 80 ORDER BY grp",
        // Unprojected GROUP BY key referenced in HAVING:
        "SELECT max(id) FROM hav GROUP BY grp HAVING count(*) > 1 ORDER BY grp",
        "SELECT count(*) FROM hav GROUP BY grp HAVING grp = 'a' ORDER BY grp",
        "SELECT count(*) FROM hav GROUP BY grp HAVING grp IS NULL ORDER BY grp",
        "SELECT count(*) FROM hav GROUP BY grp HAVING grp || '!' = 'b!' ORDER BY grp",
        "SELECT count(*) FROM hav GROUP BY grp HAVING CASE WHEN grp IS NULL THEN 1 ELSE count(*) > 2 END ORDER BY grp",
        // DISTINCT and FILTER aggregates in HAVING:
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING count(DISTINCT tag) > 1 ORDER BY grp",
        "SELECT grp, sum(val), sum(DISTINCT val) FROM hav GROUP BY grp HAVING sum(DISTINCT val) = 60 ORDER BY grp",
        "SELECT grp, count(*) FROM hav GROUP BY grp HAVING count(*) FILTER (WHERE val >= 20) >= 2 ORDER BY grp",
        "SELECT grp FROM hav GROUP BY grp HAVING sum(val) FILTER (WHERE val > 15) > 50 ORDER BY grp",
        // Expression GROUP BY key + HAVING referencing expression key & aggregate:
        "SELECT val % 20, count(*) FROM hav GROUP BY val % 20 HAVING (val % 20) + 1 > 1 AND count(*) >= 1 ORDER BY val % 20",
        // TEXT column compared against computed aggregate (aggregate has NO declared affinity -> TEXT coercion applies):
        "SELECT k, count(*) FROM num_txt GROUP BY k HAVING k = count(*) ORDER BY k",
        "SELECT k, count(*) FROM num_txt GROUP BY k HAVING count(*) = k ORDER BY k",
        "SELECT k, count(*) FROM num_txt GROUP BY k HAVING k COLLATE NOCASE = '2' AND count(*) * 10 = 20 ORDER BY k",
        // Covering / streaming index + HAVING + LIMIT/OFFSET:
        "SELECT grp, count(*) FROM hav_idx GROUP BY grp HAVING count(*) * 2 > 3 ORDER BY grp LIMIT 1 OFFSET 1",
        "SELECT grp, count(*) FROM hav_idx GROUP BY grp HAVING sum(val) > 50 ORDER BY grp LIMIT 1",
        "SELECT grp FROM hav_idx GROUP BY grp HAVING count(*) >= 2 ORDER BY grp",
        // Empty table:
        "SELECT grp, count(*) FROM hav_empty GROUP BY grp HAVING count(*) * 2 >= 0 ORDER BY grp",
        "SELECT grp FROM hav_empty GROUP BY grp HAVING count(*) > 0 ORDER BY grp",
        // Single-group aggregate (no GROUP BY) with compound HAVING:
        "SELECT count(*) FROM hav HAVING sum(val) * 2 > 400",
        "SELECT count(*) FROM hav HAVING sum(val) * 2 > 9999",
        "SELECT sum(val) FROM hav HAVING count(*) FILTER (WHERE val >= 30) = 4",
    ];

    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SCHEMA {
        stock.execute_batch(sql).unwrap();
    }
    let expected: Vec<(&str, String)> = queries
        .iter()
        .map(|&sql| (sql, stock_outcome(&stock, sql)))
        .collect();

    asupersync::test_utils::run_test(|| async {
        // 1. Strict file-backed (parity_cert_strict=ON proves zero fallback!)
        for prepared in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("bd_w4pr1_13_32_strict.db");
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            for sql in SCHEMA {
                conn.execute(sql).await.unwrap();
            }
            conn.execute("PRAGMA fsqlite.parity_cert_strict = ON")
                .await
                .unwrap();
            for &(sql, ref want) in &expected {
                let got = fsqlite_outcome(&conn, sql, prepared).await;
                assert_eq!(
                    &got, want,
                    "strict file prepared={prepared}: `{sql}`"
                );
            }
            // Parameterized HAVING on strict file-backed prepared & direct:
            let param_sql =
                "SELECT grp, count(*) FROM hav GROUP BY grp HAVING count(*) * ?1 > ?2 ORDER BY grp";
            let stock_param: Vec<Vec<String>> = stock
                .prepare(param_sql)
                .unwrap()
                .query_map([2_i64, 3_i64], |row| {
                    Ok((0..2)
                        .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                        .collect())
                })
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            let got_param: Vec<Vec<String>> = if prepared {
                let stmt = conn.prepare(param_sql).await.unwrap();
                stmt.query_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(3)])
                    .await
                    .unwrap()
            } else {
                conn.query_with_params(
                    param_sql,
                    &[SqliteValue::Integer(2), SqliteValue::Integer(3)],
                )
                .await
                .unwrap()
            }
            .iter()
            .map(|r| r.values().iter().map(render_fsqlite).collect())
            .collect();
            assert_eq!(
                got_param, stock_param,
                "strict file parameterized HAVING prepared={prepared}"
            );
            conn.close().await.unwrap();
        }

        // 2. Unloaded `:memory:` (takes storage substrate via
        //    group_by_substrate_replaces_unloaded_memory_mirror!)
        for prepared in [false, true] {
            let conn = Connection::open(":memory:").await.unwrap();
            for sql in SCHEMA {
                conn.execute(sql).await.unwrap();
            }
            for &(sql, ref want) in &expected {
                let got = fsqlite_outcome(&conn, sql, prepared).await;
                assert_eq!(
                    &got, want,
                    "memory substrate prepared={prepared}: `{sql}`"
                );
            }
            conn.close().await.unwrap();
        }
    });
}
