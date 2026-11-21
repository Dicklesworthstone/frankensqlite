#![recursion_limit = "512"]

//! Differential oracle sweep (pane af49, 2026-08-20): frank vs rusqlite over
//! query-structure semantics — CTEs (incl. RECURSIVE), set operations
//! (UNION/UNION ALL/INTERSECT/EXCEPT with ORDER BY/LIMIT), and subqueries
//! (scalar, correlated, EXISTS/NOT EXISTS, IN (subquery)). Pass = parity
//! coverage keeper; a mismatch is a leaf divergence. Full ordered sets compared.

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
fn cte_setops_subquery_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE a(id INTEGER, v INTEGER)",
            "CREATE TABLE b(id INTEGER, w INTEGER)",
            "INSERT INTO a VALUES (1,10),(2,20),(3,30),(4,NULL)",
            "INSERT INTO b VALUES (2,200),(3,300),(5,500)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // CTE
            "WITH c AS (SELECT id, v*2 AS d FROM a WHERE v IS NOT NULL) SELECT * FROM c ORDER BY id",
            "WITH c(x) AS (SELECT v FROM a) SELECT sum(x) FROM c",
            // recursive CTE — counter
            "WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n<5) SELECT n FROM cnt ORDER BY n",
            // recursive CTE — factorial-ish accumulation
            "WITH RECURSIVE f(n,acc) AS (SELECT 1,1 UNION ALL SELECT n+1,acc*(n+1) FROM f WHERE n<5) SELECT n,acc FROM f ORDER BY n",
            // recursive CTE — string build
            "WITH RECURSIVE s(i,txt) AS (SELECT 1,'x' UNION ALL SELECT i+1,txt||'x' FROM s WHERE i<4) SELECT txt FROM s ORDER BY i",
            // set operations
            "SELECT id FROM a UNION SELECT id FROM b ORDER BY id",
            "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id",
            "SELECT id FROM a INTERSECT SELECT id FROM b ORDER BY id",
            "SELECT id FROM a EXCEPT SELECT id FROM b ORDER BY id",
            "SELECT id FROM a UNION SELECT id FROM b ORDER BY id DESC LIMIT 2",
            "SELECT v FROM a UNION SELECT w FROM b ORDER BY 1",
            // subqueries
            "SELECT id, (SELECT max(w) FROM b) AS mx FROM a ORDER BY id",
            "SELECT id, (SELECT w FROM b WHERE b.id=a.id) AS cw FROM a ORDER BY id",
            "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id=a.id) ORDER BY id",
            "SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id=a.id) ORDER BY id",
            "SELECT id FROM a WHERE id IN (SELECT id FROM b) ORDER BY id",
            "SELECT id FROM a WHERE id NOT IN (SELECT id FROM b WHERE w<400) ORDER BY id",
            "SELECT id FROM a WHERE v > (SELECT avg(v) FROM a) ORDER BY id",
            // correlated aggregate
            "SELECT a.id, (SELECT count(*) FROM b WHERE b.w > a.v) FROM a ORDER BY a.id",
            // NULL in NOT IN (three-valued)
            "SELECT id FROM a WHERE id NOT IN (SELECT id FROM b UNION SELECT NULL) ORDER BY id",
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
            "{} cte/setop/subquery divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

// bd-wiffk: the INSERT's leading WITH must keep the entire SELECT source,
// including every compound arm and its final ORDER BY/LIMIT/OFFSET. Compare
// the written rows as well as the affected-row count; executing successfully
// is not enough when a SELECT arm can silently disappear.
struct CteInsertCase {
    name: &'static str,
    sql: &'static str,
    expected: &'static [i64],
    ordered: bool,
}

const CTE_INSERT_CASES: &[CteInsertCase] = &[
    CteInsertCase {
        name: "union_all",
        sql: "WITH c(x) AS (SELECT 5) INSERT INTO dst SELECT x FROM c UNION ALL SELECT 7",
        expected: &[5, 7],
        ordered: false,
    },
    CteInsertCase {
        name: "union_all_repeated_cte",
        sql: "WITH c(x) AS (VALUES (5),(5),(9)) INSERT INTO dst \
              SELECT x FROM c UNION ALL SELECT x+1 FROM c",
        expected: &[5, 5, 6, 6, 9, 10],
        ordered: false,
    },
    CteInsertCase {
        name: "union_distinct",
        sql: "WITH c(x) AS (VALUES (5),(5),(9)) INSERT INTO dst \
              SELECT x FROM c UNION SELECT 5",
        expected: &[5, 9],
        ordered: false,
    },
    CteInsertCase {
        name: "intersect",
        sql: "WITH c(x) AS (VALUES (5),(7),(9)) INSERT INTO dst \
              SELECT x FROM c INTERSECT SELECT x FROM src",
        expected: &[7],
        ordered: false,
    },
    CteInsertCase {
        name: "except",
        sql: "WITH c(x) AS (VALUES (5),(7),(9)) INSERT INTO dst \
              SELECT x FROM c EXCEPT SELECT x FROM src",
        expected: &[5, 9],
        ordered: false,
    },
    CteInsertCase {
        name: "real_table_first_arm",
        sql: "WITH c(x) AS (SELECT 5) INSERT INTO dst \
              SELECT x FROM src UNION ALL SELECT x FROM c ORDER BY 1 DESC",
        expected: &[8, 7, 5, 4],
        ordered: true,
    },
    CteInsertCase {
        name: "fromless_first_arm",
        sql: "WITH c(x) AS (SELECT 5) INSERT INTO dst \
              SELECT 7 UNION ALL SELECT x FROM c",
        expected: &[5, 7],
        ordered: false,
    },
    CteInsertCase {
        name: "compound_order_limit",
        sql: "WITH c(x) AS (SELECT 5) INSERT INTO dst \
              SELECT x FROM c UNION SELECT 10 ORDER BY 1 DESC LIMIT 1",
        expected: &[10],
        ordered: true,
    },
    CteInsertCase {
        name: "compound_order_limit_offset",
        sql: "WITH c(x) AS (VALUES (5),(9),(7)) INSERT INTO dst \
              SELECT x FROM c UNION ALL SELECT x FROM src \
              ORDER BY 1 DESC LIMIT 3 OFFSET 1",
        expected: &[8, 7, 7],
        ordered: true,
    },
    CteInsertCase {
        name: "simple_order_limit_offset",
        sql: "WITH c(x) AS (VALUES (9),(5),(7)) INSERT INTO dst \
              SELECT x FROM c ORDER BY x LIMIT 1 OFFSET 1",
        expected: &[7],
        ordered: true,
    },
    CteInsertCase {
        name: "recursive_source",
        sql: "WITH RECURSIVE c(x) AS \
              (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<3) INSERT INTO dst \
              SELECT x FROM c UNION ALL SELECT x+10 FROM c \
              ORDER BY 1 DESC LIMIT 4 OFFSET 1",
        expected: &[12, 11, 3, 2],
        ordered: true,
    },
    CteInsertCase {
        name: "mixed_compound_operators",
        sql: "WITH c(x) AS (VALUES (5),(7),(9)) INSERT INTO dst \
              SELECT x FROM c UNION ALL SELECT x FROM src EXCEPT SELECT 7 ORDER BY 1",
        expected: &[4, 5, 8, 9],
        ordered: true,
    },
    CteInsertCase {
        name: "zero_limit",
        sql: "WITH c(x) AS (SELECT 5) INSERT INTO dst \
              SELECT x FROM c UNION ALL SELECT 7 LIMIT 0",
        expected: &[],
        ordered: false,
    },
    CteInsertCase {
        name: "same_name_target",
        sql: "WITH dst(x) AS (VALUES (5),(7)) INSERT INTO dst \
              SELECT x FROM dst UNION ALL SELECT 9",
        expected: &[5, 7, 9],
        ordered: false,
    },
    // Controls: a simple source and a WITH inside the SELECT already work.
    CteInsertCase {
        name: "simple_source_control",
        sql: "WITH c(x) AS (SELECT 5) INSERT INTO dst SELECT x FROM c",
        expected: &[5],
        ordered: false,
    },
    CteInsertCase {
        name: "with_inside_select_control",
        sql: "INSERT INTO dst WITH c(x) AS (SELECT 5) \
              SELECT x FROM c UNION ALL SELECT 7 ORDER BY 1 DESC LIMIT 1",
        expected: &[7],
        ordered: true,
    },
    CteInsertCase {
        name: "nested_source_with",
        sql: "WITH c(x) AS (SELECT 5) INSERT INTO dst \
              WITH c(x) AS (SELECT 9) SELECT x FROM c UNION ALL SELECT 7 ORDER BY 1",
        expected: &[7, 9],
        ordered: true,
    },
];

async fn check_cte_insert_compound_sources(file_backed: bool, prepared: bool) {
    let directory = tempfile::tempdir().unwrap();
    let frank_path = directory.path().join("frank.db");
    let stock_path = directory.path().join("stock.db");
    let f = Connection::open(if file_backed {
        frank_path.to_str().unwrap()
    } else {
        ":memory:"
    })
    .await
    .unwrap();
    let r = if file_backed {
        rusqlite::Connection::open(&stock_path)
    } else {
        rusqlite::Connection::open_in_memory()
    }
    .unwrap();
    for sql in [
        "CREATE TABLE dst(x INTEGER)",
        "CREATE TABLE src(x INTEGER)",
        "INSERT INTO src VALUES (4),(7),(8)",
    ] {
        f.execute(sql).await.unwrap();
        r.execute(sql, []).unwrap();
    }
    for case in CTE_INSERT_CASES {
        f.execute("DELETE FROM dst").await.unwrap();
        r.execute("DELETE FROM dst", []).unwrap();
        let label = format!(
            "{}, file_backed={file_backed}, prepared={prepared}: {}",
            case.name, case.sql
        );
        let expected_changes = r.execute(case.sql, []).expect("stock INSERT succeeds");
        assert_eq!(
            expected_changes,
            case.expected.len(),
            "stock fixture: {label}"
        );
        let actual_changes = if prepared {
            f.prepare(case.sql).await.unwrap().execute().await
        } else {
            f.execute(case.sql).await
        }
        .unwrap_or_else(|error| panic!("{label}: {error}"));
        // A source ORDER BY determines insertion order, visible through rowid.
        // For unordered sources, compare the written rows as a multiset.
        let query = if case.ordered {
            "SELECT x FROM dst ORDER BY rowid"
        } else {
            "SELECT x FROM dst ORDER BY x"
        };
        let expected = rq(&r, query);
        let pinned: Vec<Vec<String>> = case
            .expected
            .iter()
            .map(|value| vec![format!("int:{value}")])
            .collect();
        assert_eq!(expected, pinned, "stock fixture: {label}");
        assert_eq!(fq(&f, query).await, expected, "written rows: {label}");
        assert_eq!(actual_changes, expected_changes, "changed rows: {label}");
        assert_eq!(
            fq(&f, "SELECT changes()").await,
            rq(&r, "SELECT changes()"),
            "{label}"
        );
    }
    f.close().await.unwrap();
}

#[test]
fn cte_insert_compound_source_memory_direct() {
    asupersync::test_utils::run_test(|| check_cte_insert_compound_sources(false, false));
}

#[test]
fn cte_insert_compound_source_memory_prepared() {
    asupersync::test_utils::run_test(|| check_cte_insert_compound_sources(false, true));
}

#[test]
fn cte_insert_compound_source_file_direct() {
    asupersync::test_utils::run_test(|| check_cte_insert_compound_sources(true, false));
}

#[test]
fn cte_insert_compound_source_file_prepared() {
    asupersync::test_utils::run_test(|| check_cte_insert_compound_sources(true, true));
}

#[test]
fn cte_insert_compound_source_attached_target() {
    asupersync::test_utils::run_test(|| async {
        for prepared in [false, true] {
            let f = Connection::open(":memory:").await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "ATTACH DATABASE ':memory:' AS aux",
                "CREATE TABLE aux.dst(x INTEGER)",
            ] {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            // Attached CTE execution installs a snapshot of the CTE rows and
            // suppresses schema refresh. That scope must retain the same full
            // SELECT semantics as the ordinary INSERT source materializer.
            let sql = "WITH c(x) AS (VALUES (5),(9),(7)) INSERT INTO aux.dst \
                       SELECT x FROM c UNION ALL SELECT 10 \
                       ORDER BY 1 DESC LIMIT 2 OFFSET 1";
            let expected_changes = r.execute(sql, []).unwrap();
            let actual_changes = if prepared {
                f.prepare(sql).await.unwrap().execute().await
            } else {
                f.execute(sql).await
            }
            .unwrap();
            let query = "SELECT x FROM aux.dst ORDER BY rowid";
            let expected = rq(&r, query);
            assert_eq!(expected, vec![vec!["int:9"], vec!["int:7"]]);
            assert_eq!(fq(&f, query).await, expected, "prepared={prepared}");
            assert_eq!(actual_changes, expected_changes, "prepared={prepared}");
            f.close().await.unwrap();
        }
    });
}
