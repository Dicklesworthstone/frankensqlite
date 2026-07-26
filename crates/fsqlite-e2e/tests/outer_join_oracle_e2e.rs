//! bd-2xhcb — Oracle-parity e2e: RIGHT / FULL OUTER JOIN vs rusqlite.
//!
//! RIGHT JOIN and FULL OUTER JOIN were added in SQLite 3.39 (join_types_oracle
//! only covers INNER / LEFT / CROSS / USING / NATURAL). RIGHT JOIN keeps every
//! row of the right table (NULL-extending unmatched left columns); FULL OUTER
//! JOIN keeps matched rows plus left-only and right-only rows; the `USING` form
//! coalesces the join column across both sides. Each scenario asserts
//! per-statement agreement with rusqlite (bundled SQLite ~3.46), then compares
//! query results; row order is pinned with COALESCE-based ORDER BY so NULL
//! placement is deterministic.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

// l: 3 rows (id 3 has no matching r); r: 4 rows (l_id 99 is an orphan).
const LR: [&str; 4] = [
    "CREATE TABLE l (id INTEGER PRIMARY KEY, name TEXT)",
    "INSERT INTO l VALUES (1,'a'),(2,'b'),(3,'c')",
    "CREATE TABLE r (id INTEGER PRIMARY KEY, l_id INTEGER, tag TEXT)",
    "INSERT INTO r VALUES (10,1,'x'),(11,1,'y'),(12,2,'z'),(13,99,'orphan')",
];

#[test]
fn right_join_keeps_all_right_rows() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &LR,
            &[
                // Every r row appears; the orphan (l_id 99) NULL-extends l.
                "SELECT l.name, r.tag FROM l RIGHT JOIN r ON l.id = r.l_id ORDER BY r.id",
                "SELECT count(*) FROM l RIGHT JOIN r ON l.id = r.l_id", // 4
                // l.id projects correctly across the null-extension (matched + orphan).
                "SELECT l.id, r.tag FROM l RIGHT JOIN r ON l.id = r.l_id ORDER BY r.id",
                "SELECT count(*) FROM l RIGHT JOIN r ON l.id = r.l_id WHERE l.id IS NOT NULL", // 3
            ],
            "right_join_keeps_all_right_rows",
        )
        .await;
    });
}

#[test]
fn right_join_mirrors_swapped_left_join() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &LR,
            &[
                // `r RIGHT JOIN l` keeps all l rows == `l LEFT JOIN r`.
                "SELECT l.name, r.tag FROM r RIGHT JOIN l ON l.id = r.l_id ORDER BY l.id, r.id",
                "SELECT l.name, r.tag FROM l LEFT JOIN r ON l.id = r.l_id ORDER BY l.id, r.id",
            ],
            "right_join_mirrors_swapped_left_join",
        )
        .await;
    });
}

#[test]
fn full_outer_join_matched_left_only_right_only() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &LR,
            &[
                // Matched (a,x),(a,y),(b,z); left-only (c,NULL); right-only (NULL,orphan).
                "SELECT l.name, r.tag FROM l FULL OUTER JOIN r ON l.id = r.l_id \
                 ORDER BY coalesce(l.id, 999), coalesce(r.id, 999)",
                "SELECT count(*) FROM l FULL OUTER JOIN r ON l.id = r.l_id", // 5
                // Filtering on the preserved side (r.id IS NULL) works correctly.
                "SELECT count(*) FROM l FULL OUTER JOIN r ON l.id = r.l_id WHERE r.id IS NULL", // 1
            ],
            "full_outer_join_matched_left_only_right_only",
        )
        .await;
    });
}

#[test]
fn full_outer_join_no_overlap() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE a (id INTEGER PRIMARY KEY)",
                "CREATE TABLE b (id INTEGER PRIMARY KEY)",
                "INSERT INTO a VALUES (1),(2)",
                "INSERT INTO b VALUES (3),(4)",
            ],
            &[
                // No matches: every row from both sides, NULL-extended.
                "SELECT a.id, b.id FROM a FULL OUTER JOIN b ON a.id = b.id \
                 ORDER BY coalesce(a.id, b.id)",
                "SELECT count(*) FROM a FULL OUTER JOIN b ON a.id = b.id", // 4
            ],
            "full_outer_join_no_overlap",
        )
        .await;
    });
}

#[test]
fn full_outer_join_using_coalesces_column() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v1 TEXT)",
                "CREATE TABLE t2 (id INTEGER PRIMARY KEY, v2 TEXT)",
                "INSERT INTO t1 VALUES (1,'a'),(2,'b')",
                "INSERT INTO t2 VALUES (2,'x'),(3,'y')",
            ],
            &[
                // USING(id) yields a single coalesced id column.
                "SELECT id, v1, v2 FROM t1 FULL OUTER JOIN t2 USING(id) ORDER BY id",
            ],
            "full_outer_join_using_coalesces_column",
        )
        .await;
    });
}

#[test]
fn aggregate_over_right_join() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &LR,
            &[
                // count(r.id) per left group; the orphan forms a NULL-name group.
                "SELECT l.name, count(r.id) FROM l RIGHT JOIN r ON l.id = r.l_id \
                 GROUP BY l.name ORDER BY l.name",
            ],
            "aggregate_over_right_join",
        )
        .await;
    });
}

/// Bug A (bd-41syy): `<outer_col> IS NULL` in WHERE over a RIGHT/FULL join matches
/// EVERY row instead of just the NULL-extended ones. The companion test
/// `right_join_keeps_all_right_rows` confirms the same column projects correctly
/// and that `IS NOT NULL` works — so the `IS NULL` predicate is independently
/// mis-compiled to constant-true for the null-extendable side.
#[test]
fn outer_join_where_outer_col_is_null() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &LR,
            &[
                "SELECT count(*) FROM l RIGHT JOIN r ON l.id = r.l_id WHERE l.id IS NULL", // 1
                "SELECT r.tag FROM l RIGHT JOIN r ON l.id = r.l_id WHERE l.id IS NULL",    // orphan
                "SELECT count(*) FROM l FULL OUTER JOIN r ON l.id = r.l_id WHERE l.id IS NULL", // 1
            ],
            "outer_join_where_outer_col_is_null",
        )
        .await;
    });
}
