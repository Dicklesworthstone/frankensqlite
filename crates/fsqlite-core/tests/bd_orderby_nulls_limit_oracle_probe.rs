#![recursion_limit = "512"]

//! ORDER BY NULLS / LIMIT-OFFSET leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over NULL ordering placement (default per ASC/DESC, explicit NULLS
//! FIRST/LAST, 3.30+), multi-key ordering, ORDER BY by ordinal/expression/
//! COLLATE, and LIMIT/OFFSET edges (negative=unlimited, zero, past-end offset,
//! over-count). Full ordered result sets compared. Pass = coverage keeper.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = conn.prepare(sql).unwrap();
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

#[test]
fn orderby_nulls_limit_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE o(id INTEGER PRIMARY KEY, x INTEGER, y TEXT)",
            "INSERT INTO o VALUES (1,30,'c'),(2,NULL,'a'),(3,10,NULL),(4,10,'b'),(5,NULL,'d'),(6,20,'a')",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // default NULL placement: ASC -> NULLs first, DESC -> NULLs last
            "SELECT id FROM o ORDER BY x, id",
            "SELECT id FROM o ORDER BY x DESC, id",
            // explicit NULLS FIRST / LAST (3.30+)
            "SELECT id FROM o ORDER BY x NULLS LAST, id",
            "SELECT id FROM o ORDER BY x NULLS FIRST, id",
            "SELECT id FROM o ORDER BY x DESC NULLS FIRST, id",
            "SELECT id FROM o ORDER BY x DESC NULLS LAST, id",
            // text column with NULL + collation
            "SELECT id FROM o ORDER BY y NULLS LAST, id",
            "SELECT id FROM o ORDER BY y COLLATE NOCASE NULLS FIRST, id",
            // multi-key + ordinal + expression
            "SELECT id FROM o ORDER BY x, y, id",
            "SELECT x, id FROM o ORDER BY 1 NULLS LAST, 2",
            "SELECT id FROM o ORDER BY (x % 20) NULLS FIRST, id",
            // LIMIT / OFFSET edges
            "SELECT id FROM o ORDER BY id LIMIT 3",
            "SELECT id FROM o ORDER BY id LIMIT -1",
            "SELECT id FROM o ORDER BY id LIMIT 0",
            "SELECT id FROM o ORDER BY id LIMIT 100",
            "SELECT id FROM o ORDER BY id LIMIT 2 OFFSET 2",
            "SELECT id FROM o ORDER BY id LIMIT 2 OFFSET 10",
            "SELECT id FROM o ORDER BY id LIMIT -1 OFFSET 3",
            "SELECT id FROM o ORDER BY x NULLS LAST, id LIMIT 3 OFFSET 1",
            "SELECT id FROM o ORDER BY id DESC LIMIT 2",
            // LIMIT with expression bound
            "SELECT id FROM o ORDER BY id LIMIT 1+1",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} order-by-nulls/limit divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// 400 rows whose sort keys repeat (`a`), differ only in case (`b`), hold
/// NULLs (`a`, `c`) and mix storage classes (`d`), so a bounded sorter meets
/// ties, NULLs and cross-class comparisons when it turns rows away. The rows
/// come from a recursive CTE: the bundled stock library has no
/// generate_series.
const TOP_N_SCHEMA: &str = "\
CREATE TABLE k(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d);\
INSERT INTO k WITH RECURSIVE s(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM s \
  WHERE value < 400) \
  SELECT value, CASE WHEN value % 37 = 0 THEN NULL ELSE (value * 7) % 9 END, \
  CASE value % 3 WHEN 0 THEN 'Row-' WHEN 1 THEN 'row-' ELSE 'ROW-' END || ((value * 13) % 50), \
  CASE WHEN value % 11 = 0 THEN NULL ELSE ((value * 17) % 23) * 0.5 END, \
  CASE value % 5 WHEN 0 THEN NULL WHEN 1 THEN value % 7 WHEN 2 THEN (value % 7) * 1.0 \
    WHEN 3 THEN 'v' || (value % 7) ELSE x'07' END \
  FROM s;";

/// Every statement bounds a sorter with LIMIT, so each row meets the top-N
/// preflight. Ties must keep the earlier row, as stock's bounded sorter does.
const TOP_N_STATEMENTS: &[&str] = &[
    "SELECT id, b FROM k ORDER BY b DESC LIMIT 10",
    "SELECT id, b FROM k ORDER BY b LIMIT 7",
    "SELECT id, a FROM k ORDER BY a LIMIT 15",
    "SELECT id, a FROM k ORDER BY a DESC LIMIT 15",
    "SELECT id FROM k ORDER BY a, b DESC LIMIT 12",
    "SELECT id FROM k ORDER BY a DESC, b LIMIT 12",
    "SELECT id, b FROM k ORDER BY b COLLATE NOCASE LIMIT 9",
    "SELECT id, b FROM k ORDER BY b COLLATE NOCASE DESC, a LIMIT 9",
    "SELECT id, d FROM k ORDER BY d LIMIT 20",
    "SELECT id, d FROM k ORDER BY d DESC LIMIT 20",
    "SELECT id, c FROM k ORDER BY c NULLS LAST LIMIT 11",
    "SELECT id, c FROM k ORDER BY c DESC NULLS FIRST LIMIT 11",
    "SELECT id FROM k ORDER BY a DESC NULLS FIRST, id LIMIT 8 OFFSET 5",
    "SELECT id FROM k ORDER BY b DESC LIMIT 3 OFFSET 397",
    "SELECT count(*), sum(id) FROM (SELECT id FROM k ORDER BY b DESC LIMIT 10)",
    "SELECT id FROM k WHERE a > 3 ORDER BY b LIMIT 6",
    "SELECT id FROM k ORDER BY a + 0, c LIMIT 13",
    "SELECT id FROM k ORDER BY b DESC LIMIT (SELECT 4)",
    "SELECT DISTINCT a FROM k ORDER BY a DESC LIMIT 5",
    "SELECT id, a IN (SELECT a FROM k ORDER BY b DESC LIMIT 3) FROM k ORDER BY id LIMIT 40",
    "SELECT id FROM k WHERE id IN (SELECT id FROM k ORDER BY d DESC, id LIMIT 6) ORDER BY id",
];

async fn top_n_diffs(file_backed: bool) -> Vec<String> {
    let dir = tempfile::tempdir().unwrap();
    let (f, r) = if file_backed {
        (
            Connection::open(dir.path().join("f.db").to_str().unwrap())
                .await
                .unwrap(),
            rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
        )
    } else {
        (
            Connection::open(":memory:").await.unwrap(),
            rusqlite::Connection::open_in_memory().unwrap(),
        )
    };
    f.execute_batch(TOP_N_SCHEMA).await.unwrap();
    r.execute_batch(TOP_N_SCHEMA).unwrap();
    if file_backed {
        // Strict mode refuses the connection's fallback evaluators, so each
        // statement answers from its VDBE program.
        f.execute("PRAGMA fsqlite.parity_cert_strict = ON")
            .await
            .unwrap();
        // Not vacuous: the bounded sorter's program has a top-N preflight.
        let explain = fq(&f, "EXPLAIN SELECT id, b FROM k ORDER BY b DESC LIMIT 10").await;
        assert!(
            explain
                .iter()
                .any(|row| row.get(1).is_some_and(|op| op == "text:SorterCompare")),
            "ORDER BY ... LIMIT must run a top-N preflight: {explain:?}"
        );
    }
    let label = if file_backed {
        "file, strict"
    } else {
        "memory"
    };
    let mut diffs = Vec::new();
    for sql in TOP_N_STATEMENTS {
        let stock = rq(&r, sql);
        let adhoc = f.query(sql).await.map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        });
        let prepared = match f.prepare(sql).await {
            Ok(stmt) => stmt.query().await.map(|rows| {
                rows.iter()
                    .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                    .collect::<Vec<_>>()
            }),
            Err(e) => Err(e),
        };
        for (path, frank) in [("ad hoc", adhoc), ("prepared", prepared)] {
            match frank {
                Ok(rows) if rows == stock => {}
                // Strict mode refuses the connection evaluator an aggregate
                // over a derived table runs on; only that shape may be
                // refused, and it is compared in memory, where strict is off.
                Err(e)
                    if file_backed
                        && sql.starts_with("SELECT count(*), sum(id) FROM (")
                        && e.to_string().contains(
                            "in-memory fallback disabled in strict parity-cert mode",
                        ) => {}
                other => diffs.push(format!(
                    "  [{label}, {path}] `{sql}`\n     frank= {other:?}\n     stock= {stock:?}"
                )),
            }
        }
    }
    f.close().await.unwrap();
    diffs
}

#[test]
fn top_n_limit_rows_match_stock_file_backed_and_in_memory() {
    asupersync::test_utils::run_test(|| async {
        let mut diffs = top_n_diffs(true).await;
        diffs.extend(top_n_diffs(false).await);
        assert!(
            diffs.is_empty(),
            "{} top-N divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
