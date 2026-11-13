#![recursion_limit = "512"]

//! Wrong answers found reviewing the 2026-10-05 lane commits, compared with
//! rusqlite (bundled SQLite 3.53.2), ad hoc and prepared, in memory and
//! file-backed (the third, time travel, is described on its test).
//!
//! 1. FROM-subquery flattening rewrote an outer bare ORDER BY name into the
//!    inner column it stands for. In the flattened statement a bare ORDER BY
//!    name matches a result alias first (SQLite's resolveAsName), so a result
//!    alias spelled like that inner column captured it:
//!    `SELECT * FROM (SELECT a AS b, b AS a FROM x) ORDER BY a` ordered by
//!    x.a (the alias `b`) instead of x.b.
//! 2. `typeless_col IN (SELECT text_col ...)` converted the typeless side's
//!    integers to text. SQLite's `exprINAffinity` compares the two declared
//!    affinities with `sqlite3CompareAffinity`: when both sides carry one and
//!    neither is numeric, nothing converts (GH#428's rule for `=`, which the
//!    IN-subquery lanes did not apply).

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

fn render_f(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Null => "null".to_owned(),
                    SqliteValue::Integer(i) => format!("i:{i}"),
                    SqliteValue::Float(f) => format!("r:{f}"),
                    SqliteValue::Text(t) => format!("t:{t}"),
                    SqliteValue::Blob(b) => format!("b:{b:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn render_stock(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap_or_else(|e| panic!("stock prepare `{sql}`: {e}"));
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(n);
        for i in 0..n {
            cells.push(match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => "null".to_owned(),
                rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
                rusqlite::types::ValueRef::Real(v) => format!("r:{v}"),
                rusqlite::types::ValueRef::Text(v) => format!("t:{}", String::from_utf8_lossy(v)),
                rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
            });
        }
        Ok(cells.join("|"))
    })
    .unwrap_or_else(|e| panic!("stock query `{sql}`: {e}"))
    .collect::<Result<Vec<_>, _>>()
    .unwrap_or_else(|e| panic!("stock rows `{sql}`: {e}"))
}

const SETUP: &[&str] = &[
    "CREATE TABLE x(a, b)",
    "INSERT INTO x VALUES (1,'c'),(2,'a'),(3,'b'),(4,'A')",
    "CREATE TABLE g(k)",
    "INSERT INTO g VALUES (1),('1'),(1.0),('01'),(2),('x'),(NULL)",
    "CREATE TABLE h(t TEXT, i INTEGER, n, r REAL)",
    "INSERT INTO h VALUES ('1', 1, 1, 1.0),('2', 2, '2', 2.0),('01', 3, 'x', 3.5)",
    "CREATE INDEX h_t ON h(t)",
    "CREATE TABLE gi(k INTEGER, t TEXT)",
    "INSERT INTO gi VALUES (1, '1'),(2, '2'),(3, '01')",
];

const QUERIES: &[&str] = &[
    // 1. Alias capture after flattening.
    "SELECT * FROM (SELECT a AS b, b AS a FROM x) ORDER BY a",
    "SELECT * FROM (SELECT a AS b, b AS a FROM x) ORDER BY b",
    "SELECT * FROM (SELECT a AS b, b AS a FROM x) ORDER BY a DESC",
    "SELECT * FROM (SELECT a AS b, b AS a FROM x) ORDER BY a COLLATE NOCASE, b",
    "SELECT b, a FROM (SELECT a AS b, b AS a FROM x) ORDER BY a",
    "SELECT * FROM (SELECT a AS b, b AS a FROM x) WHERE a > 'a' ORDER BY a",
    "SELECT * FROM (SELECT a AS b, b AS a FROM x) ORDER BY a LIMIT 2",
    "SELECT * FROM (SELECT a AS b, b AS a FROM x) ORDER BY a || ''",
    "SELECT s.a FROM (SELECT a AS b, b AS a FROM x) AS s ORDER BY s.a",
    "SELECT a FROM (SELECT a AS b, b AS a FROM x) ORDER BY a",
    "SELECT * FROM (SELECT b AS a FROM x) ORDER BY a",
    "SELECT * FROM (SELECT a, b FROM x) ORDER BY a",
    // 2. IN subquery comparison affinity.
    "SELECT rowid FROM g WHERE g.k IN (SELECT t FROM h) ORDER BY rowid",
    "SELECT rowid FROM g WHERE g.k NOT IN (SELECT t FROM h) ORDER BY rowid",
    "SELECT rowid FROM g WHERE g.k IN (SELECT n FROM h) ORDER BY rowid",
    "SELECT rowid FROM g WHERE g.k IN (SELECT i FROM h) ORDER BY rowid",
    "SELECT rowid FROM g WHERE g.k IN (SELECT r FROM h) ORDER BY rowid",
    "SELECT rowid FROM g WHERE g.k IN (SELECT t || '' FROM h) ORDER BY rowid",
    "SELECT rowid FROM g WHERE g.k IN (SELECT CAST(i AS TEXT) FROM h) ORDER BY rowid",
    "SELECT rowid FROM g WHERE g.k IN (SELECT t FROM h WHERE i > 1) ORDER BY rowid",
    "SELECT rowid FROM g WHERE g.k + 0 IN (SELECT t FROM h) ORDER BY rowid",
    "SELECT rowid FROM g WHERE CAST(g.k AS TEXT) IN (SELECT t FROM h) ORDER BY rowid",
    "SELECT rowid FROM gi WHERE gi.k IN (SELECT t FROM h) ORDER BY rowid",
    "SELECT rowid FROM gi WHERE gi.t IN (SELECT n FROM h) ORDER BY rowid",
    "SELECT rowid FROM gi WHERE gi.t IN (SELECT i FROM h) ORDER BY rowid",
    "SELECT count(*) FROM g WHERE g.k IN (SELECT t FROM h)",
    "SELECT g.k IN (SELECT t FROM h) FROM g ORDER BY rowid",
    "SELECT rowid FROM h WHERE t IN (SELECT k FROM g) ORDER BY rowid",
];

#[test]
fn flattened_alias_capture_and_in_subquery_affinity_match_stock() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(move || async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path().join("rv3.db").to_string_lossy().into_owned()
            } else {
                ":memory:".to_owned()
            };
            let conn = Connection::open(&target).await.unwrap();
            let stock = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                conn.execute(sql).await.unwrap();
                stock.execute_batch(sql).unwrap();
            }
            let mut mismatches = Vec::new();
            for sql in QUERIES {
                let expected = render_stock(&stock, sql);
                let ad_hoc = conn.query(sql).await.map(|rows| render_f(&rows));
                let prepared = match conn.prepare(sql).await {
                    Ok(stmt) => stmt.query().await.map(|rows| render_f(&rows)),
                    Err(e) => Err(e),
                };
                for (mode, got) in [("ad hoc", ad_hoc), ("prepared", prepared)] {
                    let got = got.map_err(|e| e.to_string());
                    if got.as_ref() != Ok(&expected) {
                        mismatches.push(format!(
                            "file={file_backed} {mode} `{sql}`\n  stock:   {expected:?}\n  fsqlite: {got:?}"
                        ));
                    }
                }
            }
            assert!(mismatches.is_empty(), "{} mismatches:\n{}", mismatches.len(), mismatches.join("\n"));
        });
    }
}

/// 3. A window query over a single `FOR SYSTEM_TIME` table compiled a program
///    that read the live pager, so it answered with today's rows instead of
///    the snapshot's (the GROUP BY executor already stayed on the snapshot).
///    Each historical answer must equal stock SQLite frozen at that commit.
#[test]
fn window_query_over_a_time_travel_table_reads_the_snapshot() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let shadow = rusqlite::Connection::open_in_memory().unwrap();
        let steps: &[&[&str]] = &[
            &["CREATE TABLE e(x)"],
            &["CREATE TABLE t(a INTEGER, b TEXT)"],
            &["BEGIN", "INSERT INTO t VALUES (1,'x'),(2,'y'),(3,'x'),(4,'y')", "COMMIT"],
            &["BEGIN", "UPDATE t SET b = 'z' WHERE a = 2", "DELETE FROM t WHERE a = 4", "COMMIT"],
        ];
        let queries: &[&str] = &[
            "SELECT a, sum(a) OVER (ORDER BY a) FROM t {AS_OF} ORDER BY a",
            "SELECT a, b, row_number() OVER (PARTITION BY b ORDER BY a) FROM t {AS_OF} ORDER BY a",
            "SELECT a, count(*) OVER () FROM t {AS_OF} WHERE a > 1 ORDER BY a",
            "SELECT b, lag(a) OVER (ORDER BY a) FROM t {AS_OF} ORDER BY a",
        ];
        let mut history = Vec::new();
        let mut next = 1_u64;
        for step in steps {
            for sql in *step {
                conn.execute(sql).await.unwrap();
                shadow.execute_batch(sql).unwrap();
            }
            let mut found = None;
            for candidate in next..next + 16 {
                if conn
                    .query(&format!("SELECT x FROM e FOR SYSTEM_TIME AS OF COMMITSEQ {candidate}"))
                    .await
                    .is_ok()
                {
                    found = Some(candidate);
                    break;
                }
            }
            let Some(seq) = found else { continue };
            next = seq + 1;
            // Before `t` exists every query fails in stock too.
            let answers: Vec<Option<Vec<String>>> = queries
                .iter()
                .map(|q| {
                    let sql = q.replace(" {AS_OF}", "");
                    shadow.prepare(&sql).is_ok().then(|| render_stock(&shadow, &sql))
                })
                .collect();
            history.push((seq, answers));
        }
        assert!(history.len() >= 3, "expected a snapshot per commit, got {}", history.len());
        // A later live write must not leak into any historical answer.
        conn.execute("INSERT INTO t VALUES (9, 'live')").await.unwrap();
        let mut mismatches = Vec::new();
        for (seq, answers) in &history {
            for (query, expected) in queries.iter().zip(answers) {
                let sql = query.replace("{AS_OF}", &format!("FOR SYSTEM_TIME AS OF COMMITSEQ {seq}"));
                let got = conn.query(&sql).await.ok().map(|rows| render_f(&rows));
                if &got != expected {
                    mismatches.push(format!("seq {seq}: {sql}\n  stock:   {expected:?}\n  fsqlite: {got:?}"));
                }
            }
        }
        assert!(mismatches.is_empty(), "{} mismatches:\n{}", mismatches.len(), mismatches.join("\n"));
    });
}
