#![recursion_limit = "512"]

//! Non-recursive CTE composition leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over WITH-clause corners that aren't recursion — several CTEs in one
//! WITH where a later CTE references an earlier one, a CTE whose name SHADOWS a
//! real table (the CTE must win inside its query), a CTE self-joined / reused
//! twice, explicit column-list renaming on the CTE, a CTE feeding an aggregate,
//! WITH nested inside a subquery / inside a FROM subquery, and a CTE referenced
//! from a DML statement. Ordered result sets compared. Pass = coverage keeper.

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
    match conn.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else {
        return vec![vec!["ERR".to_owned()]];
    };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    }) {
        Ok(rows) => rows
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
async fn ex(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let _ = f.execute(sql).await;
    let _ = r.execute(sql, []);
}

#[test]
fn dysy4_forward_cte_shadows_outer_table() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in ["CREATE TABLE a(v INTEGER)", "INSERT INTO a VALUES (1)"] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        for (sql, expected) in [
            ("WITH q AS (SELECT v FROM a), a(v) AS (VALUES(2)) SELECT v FROM q", 2),
            ("WITH q AS (SELECT v FROM later), later(v) AS (VALUES(3)) SELECT v FROM q", 3),
            ("WITH q AS (SELECT v FROM main.a) SELECT v FROM q", 1),
            ("WITH q AS (WITH a(v) AS (VALUES(4)) SELECT v FROM a), a(v) AS (VALUES(2)) SELECT v FROM q", 4),
            ("WITH q AS (SELECT (SELECT v FROM a) AS v), a(v) AS (VALUES(2)) SELECT v FROM q", 2),
            ("WITH q AS (SELECT v FROM b), b AS (SELECT v+1 AS v FROM a), a(v) AS (VALUES(2)) SELECT v FROM q", 3),
            ("WITH q AS (WITH a(v) AS (VALUES(4)) SELECT v FROM a), a AS (SELECT v FROM q) SELECT v FROM a", 4),
            ("WITH q AS (WITH unused AS (SELECT v FROM a) SELECT 5 AS v), a AS (SELECT v FROM q) SELECT v FROM a", 5),
            ("WITH q AS (SELECT 2 IN a AS v), a(v) AS (VALUES(2)) SELECT v FROM q", 1),
            ("WITH q AS (SELECT v FROM a), a(v) AS (SELECT 2 WHERE 0) SELECT count(*) FROM q", 0),
            ("WITH q AS (SELECT v FROM a), a(v) AS (SELECT CAST(2 AS TEXT)) SELECT v=2 FROM q", 1),
        ] {
            let stock: i64 = r.query_row(sql, [], |row| row.get(0)).unwrap();
            assert_eq!(stock, expected, "stock control: {sql}");
            let actual = f.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert_eq!(actual.len(), 1, "{sql}");
            assert_eq!(actual[0].values(), &[SqliteValue::Integer(stock)], "{sql}");
            // Materialization must restore the real table after every statement.
            let restored = f.query("SELECT v FROM a").await
                .unwrap_or_else(|e| panic!("persistent table after {sql}: {e}"));
            assert_eq!(restored.len(), 1, "persistent table after {sql}");
            assert_eq!(restored[0].values(), &[SqliteValue::Integer(1)], "persistent table after {sql}");
        }
    });
}

#[test]
fn dysy4_qualified_cte_collision_keeps_existing_explicit_refusal() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in ["CREATE TABLE a(v INTEGER)", "INSERT INTO a VALUES(1)"] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        // This pre-existing namespace limitation is outside the unqualified
        // sibling-resolution fix. Do not silently read the CTE as main.a.
        let sql = "WITH q AS (SELECT v FROM main.a), a(v) AS (VALUES(2)) SELECT v FROM q";
        let stock: i64 = r.query_row(sql, [], |row| row.get(0)).unwrap();
        assert_eq!(stock, 1);
        let error = f.query(sql).await.expect_err("qualified collision remains unsupported");
        assert!(matches!(error, fsqlite_error::FrankenError::NotImplemented(_)), "{error}");
        let rows = f.query("SELECT v FROM main.a").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values(), &[SqliteValue::Integer(stock)]);
    });
}

#[test]
fn dysy4_cte_cycles_fail_without_leaking_materialized_tables() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in ["CREATE TABLE a(v)", "INSERT INTO a VALUES(1)"] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        for cycle in [
            "WITH q AS (SELECT v FROM a), a AS (SELECT v FROM q) SELECT v FROM q",
            "WITH x AS (SELECT v FROM q), q AS (SELECT v FROM a), a AS (SELECT v FROM q) SELECT v FROM x",
        ] {
            let stock = r.prepare(cycle).expect_err("stock detects sibling cycle");
            assert!(stock.to_string().contains("circular reference: q"), "{stock}");
            let actual = f.query(cycle).await.expect_err("sibling cycle must fail");
            assert!(actual.to_string().contains("circular reference: q"), "{actual}");
            assert_eq!(fq(&f, "SELECT v FROM a").await, vec![vec!["int:1"]]);
        }
        for sql in [
            "WITH q AS (SELECT * FROM z), z AS (SELECT * FROM q) SELECT 1",
            "WITH RECURSIVE q(v) AS (VALUES(1) UNION ALL SELECT v+1 FROM q WHERE v<3) SELECT sum(v) FROM q",
        ] {
            let stock: i64 = r.query_row(sql, [], |row| row.get(0)).unwrap();
            let actual = f.query(sql).await.unwrap();
            assert_eq!(actual.len(), 1, "{sql}");
            assert_eq!(actual[0].values(), &[SqliteValue::Integer(stock)], "{sql}");
        }
    });
}

#[test]
fn cte_composition_edges_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE base(id INTEGER PRIMARY KEY, g TEXT, v INTEGER)",
            "INSERT INTO base VALUES (1,'x',10),(2,'x',20),(3,'y',30),(4,'y',40),(5,'z',50)",
        ] {
            ex(&f, &r, s).await;
        }
        let mut diffs = Vec::new();
        let check =
            |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
                if fr != rr {
                    d.push(format!(
                        "  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            };

        // later CTE references an earlier CTE
        check(
            "chained ctes",
            fq(
                &f,
                "WITH a AS (SELECT id,v FROM base WHERE v>=20), \
                  b AS (SELECT id, v*2 AS w FROM a WHERE id<5) \
             SELECT id,w FROM b ORDER BY id",
            )
            .await,
            rq(
                &r,
                "WITH a AS (SELECT id,v FROM base WHERE v>=20), \
                  b AS (SELECT id, v*2 AS w FROM a WHERE id<5) \
             SELECT id,w FROM b ORDER BY id",
            ),
            &mut diffs,
        );

        // CTE name shadows a real table -> the CTE wins inside its query
        check(
            "cte shadows table",
            fq(
                &f,
                "WITH base AS (SELECT 99 AS id, 'shadow' AS g, 0 AS v) \
             SELECT id,g,v FROM base",
            )
            .await,
            rq(
                &r,
                "WITH base AS (SELECT 99 AS id, 'shadow' AS g, 0 AS v) \
             SELECT id,g,v FROM base",
            ),
            &mut diffs,
        );

        // CTE self-joined (reused twice under different aliases)
        check("cte reused twice", fq(&f,
            "WITH c AS (SELECT id,g,v FROM base) \
             SELECT c1.id, c2.id FROM c c1 JOIN c c2 ON c1.g=c2.g AND c1.id<c2.id ORDER BY c1.id,c2.id").await,
            rq(&r,
            "WITH c AS (SELECT id,g,v FROM base) \
             SELECT c1.id, c2.id FROM c c1 JOIN c c2 ON c1.g=c2.g AND c1.id<c2.id ORDER BY c1.id,c2.id"), &mut diffs);

        // explicit column-list renaming on the CTE
        check(
            "cte explicit col names",
            fq(
                &f,
                "WITH renamed(pk, grp, val) AS (SELECT id,g,v FROM base) \
             SELECT pk, grp, val FROM renamed WHERE val>25 ORDER BY pk",
            )
            .await,
            rq(
                &r,
                "WITH renamed(pk, grp, val) AS (SELECT id,g,v FROM base) \
             SELECT pk, grp, val FROM renamed WHERE val>25 ORDER BY pk",
            ),
            &mut diffs,
        );

        // CTE feeding an aggregate + GROUP BY
        check(
            "cte aggregate",
            fq(
                &f,
                "WITH g AS (SELECT g AS grp, v FROM base) \
             SELECT grp, count(*), sum(v) FROM g GROUP BY grp ORDER BY grp",
            )
            .await,
            rq(
                &r,
                "WITH g AS (SELECT g AS grp, v FROM base) \
             SELECT grp, count(*), sum(v) FROM g GROUP BY grp ORDER BY grp",
            ),
            &mut diffs,
        );

        // WITH nested inside a FROM subquery
        check("with inside from subquery", fq(&f,
            "SELECT total FROM (WITH s AS (SELECT sum(v) AS total FROM base) SELECT total FROM s)").await,
            rq(&r,
            "SELECT total FROM (WITH s AS (SELECT sum(v) AS total FROM base) SELECT total FROM s)"), &mut diffs);

        // WITH inside a scalar subquery in the SELECT list
        check("with inside scalar subquery", fq(&f,
            "SELECT id, (WITH m AS (SELECT max(v) AS mx FROM base) SELECT mx FROM m) AS mx FROM base WHERE id<=2 ORDER BY id").await,
            rq(&r,
            "SELECT id, (WITH m AS (SELECT max(v) AS mx FROM base) SELECT mx FROM m) AS mx FROM base WHERE id<=2 ORDER BY id"), &mut diffs);

        // CTE referenced inside an IN subquery
        check(
            "cte in-subquery",
            fq(
                &f,
                "WITH hi AS (SELECT id FROM base WHERE v>=30) \
             SELECT id,g FROM base WHERE id IN (SELECT id FROM hi) ORDER BY id",
            )
            .await,
            rq(
                &r,
                "WITH hi AS (SELECT id FROM base WHERE v>=30) \
             SELECT id,g FROM base WHERE id IN (SELECT id FROM hi) ORDER BY id",
            ),
            &mut diffs,
        );

        // compound (UNION) query as a CTE body
        check(
            "compound cte body",
            fq(
                &f,
                "WITH u AS (SELECT id FROM base WHERE g='x' UNION SELECT id FROM base WHERE g='z') \
             SELECT id FROM u ORDER BY id",
            )
            .await,
            rq(
                &r,
                "WITH u AS (SELECT id FROM base WHERE g='x' UNION SELECT id FROM base WHERE g='z') \
             SELECT id FROM u ORDER BY id",
            ),
            &mut diffs,
        );

        // CTE feeding a DML statement (INSERT ... SELECT with a leading WITH)
        ex(&f, &r, "CREATE TABLE sink(id INTEGER, doubled INTEGER)").await;
        ex(&f, &r, "WITH d AS (SELECT id, v*2 AS w FROM base WHERE g='y') INSERT INTO sink(id,doubled) SELECT id,w FROM d").await;
        check(
            "after cte-fed insert",
            fq(&f, "SELECT id,doubled FROM sink ORDER BY id").await,
            rq(&r, "SELECT id,doubled FROM sink ORDER BY id"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} CTE-composition divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
