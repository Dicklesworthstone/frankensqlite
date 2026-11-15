#![recursion_limit = "512"]

//! Derived-table alias parity keeper (pane af49, 2026-08-21): frank vs rusqlite
//! over FROM-clause table/subquery aliasing.
//!
//! FINDING (verified, NOT a leaf): SQLite does *not* support a column-list alias
//! on a subquery/VALUES/table in the FROM clause — `(<subquery>) AS name(a, b)`
//! is a syntax error in stock SQLite (that positional column-rename form is only
//! available on CTEs, `WITH name(a,b) AS (...)`, and on `CREATE VIEW`). Frank
//! rejects the same construct at the same source offset, so an oracle over these
//! forms sees error-vs-error (a match). Frank's parser emits a `parse recovery`
//! WARN while rejecting, but the rejection itself is correct and stock-faithful.
//!
//! This keeper therefore guards two things: (1) frank keeps REJECTING the
//! FROM-clause column-list-alias forms that SQLite rejects (so nobody
//! accidentally "supports" a non-SQLite syntax and diverges), and (2) frank
//! keeps SUPPORTING the aliasing SQLite does support — a bare subquery alias, and
//! CTE / VIEW column-list aliases — with identical results. Error-vs-error is
//! normalized so only rows-vs-error (a real divergence) fails.

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
        Err(e) => vec![vec![format!("ERR:{e}")]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => return vec![vec![format!("ERR:{e}")]],
    };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    }) {
        Ok(rows) => rows
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|e| vec![vec![format!("ERR:{e}")]]),
        Err(e) => vec![vec![format!("ERR:{e}")]],
    }
}

// Collapse any engine-specific error string to a bare "ERR" so error-vs-error
// matches (the two engines phrase syntax errors differently) while error-vs-rows
// still diverges — that rows/error asymmetry is the only real leaf here.
fn norm(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    if rows.len() == 1 && rows[0].len() == 1 && rows[0][0].starts_with("ERR") {
        return vec![vec!["ERR".to_owned()]];
    }
    for r in &mut rows {
        for c in r.iter_mut() {
            if c.starts_with("ERR:") {
                "ERR".clone_into(c);
            }
        }
    }
    rows
}

#[test]
fn derived_table_column_alias_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE base(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
            "INSERT INTO base VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // ── FROM-clause column-list aliases: UNSUPPORTED in SQLite -> both error ──
            "SELECT p, q FROM (SELECT a, b FROM base) AS t(p, q) ORDER BY p",
            "SELECT v FROM (VALUES (1),(2),(3)) AS z(v) ORDER BY v",
            "SELECT x, y FROM (VALUES (1,'a'),(2,'b')) AS z(x, y) ORDER BY x",
            "SELECT sum(v) FROM (VALUES (9223372036854775807),(9223372036854775807)) AS z(v)",
            "SELECT dbl FROM (SELECT a*2 AS orig FROM base) AS t(dbl) ORDER BY dbl",
            "SELECT t1.p FROM (SELECT id, a FROM base) AS t1(p, q) JOIN base ON t1.p = base.id",
            // ── Forms SQLite DOES support: must produce identical rows ──
            // bare subquery alias (no column list) keeps the source column names
            "SELECT a, b FROM (SELECT a, b FROM base) AS t ORDER BY a",
            "SELECT t.a, t.b FROM (SELECT a, b FROM base) t WHERE t.a > 15 ORDER BY t.a",
            // CTE column-list alias (the supported positional-rename surface)
            "WITH t(p, q) AS (SELECT a, b FROM base) SELECT p, q FROM t ORDER BY p",
            "WITH t(p, q) AS (SELECT a, b FROM base) SELECT q FROM t WHERE p > 15 ORDER BY q",
            // CTE over VALUES with a column-list alias
            "WITH z(x, y) AS (VALUES (1,'a'),(2,'b')) SELECT x, y FROM z ORDER BY x",
            // nested bare-alias subqueries
            "SELECT m FROM (SELECT a+1 AS m FROM (SELECT a FROM base) AS inner_t) AS outer_t ORDER BY m",
            // scalar subquery with a bare-aliased derived table
            "SELECT (SELECT max(a) FROM (SELECT a FROM base) AS t)",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = norm(fq(&f, q).await);
            let rr = norm(rq(&r, q));
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} derived-table alias divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// bd-w4pr1.12 lever W2: a join or derived-table projection that is one plain
/// column reads its source slot directly instead of evaluating the column on
/// every row. Each query below projects through that loop: derived tables
/// summed or grouped in an outer SELECT (window results among them), qualified
/// and unqualified references, a column named in two sources, rowid aliases,
/// NULL and mixed-type values. USING and NATURAL joins coalesce their shared
/// columns (a RIGHT or FULL join's value is the right row's when the left one
/// is missing), which a slot read from the left source gets wrong: those keep
/// the evaluator and are compared too. File-backed and `:memory:`.
#[test]
fn join_and_derived_table_projections_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("w2.db");
        let file = Connection::open(path.to_str().expect("utf-8 path"))
            .await
            .unwrap();
        let memory = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut p_rows = Vec::new();
        let mut q_rows = Vec::new();
        for i in 1..=400_i64 {
            let k = match i % 5 {
                0 => "NULL".to_owned(),
                1 => format!("{}", i % 7),
                2 => format!("{}.5", i % 7),
                3 => format!("'k{}'", i % 7),
                _ => format!("x'{:02x}'", i % 7),
            };
            let c = if i % 6 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}.25", i % 13)
            };
            p_rows.push(format!("({i}, {}, 'b{}', {c}, {k})", i % 9, i % 4));
            if i % 3 != 0 {
                let b = if i % 4 == 0 {
                    "NULL".to_owned()
                } else {
                    format!("'b{}'", i % 4)
                };
                q_rows.push(format!("({i}, {}, {b}, {k})", i % 11));
            }
        }
        let setup = [
            "CREATE TABLE p(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, k)".to_owned(),
            "CREATE TABLE q(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, k)".to_owned(),
            format!("INSERT INTO p VALUES {}", p_rows.join(", ")),
            format!("INSERT INTO q VALUES {}", q_rows.join(", ")),
        ];
        for s in &setup {
            file.execute(s).await.unwrap();
            memory.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }
        let queries = [
            "SELECT sum(s), count(*) FROM (SELECT sum(c) OVER (PARTITION BY a) s FROM p)",
            "SELECT sum(rn), max(rn) FROM (SELECT row_number() OVER (PARTITION BY a ORDER BY c) rn FROM p)",
            "SELECT count(*), sum(a), total(c), max(b), min(k) FROM (SELECT a, b, c, k FROM p)",
            "SELECT a, count(*), sum(x) FROM (SELECT a, c x FROM p) GROUP BY a ORDER BY a",
            "SELECT count(*), sum(id), max(b) FROM (SELECT id, b FROM p WHERE a > 3)",
            "SELECT p.id, q.id, p.a, q.a, p.b, q.b FROM p JOIN q ON q.id = p.id ORDER BY p.id",
            "SELECT p.k, q.k, c FROM p LEFT JOIN q ON q.id = p.id WHERE p.id < 40 ORDER BY p.id",
            "SELECT count(*), sum(p.a), sum(q.a), max(q.b) FROM p JOIN q ON q.a = p.a",
            "SELECT p.rowid, q.oid, p._rowid_ FROM p JOIN q ON q.id = p.id WHERE p.id % 50 = 1 ORDER BY 1",
            "SELECT id, p.b, q.b FROM p JOIN q USING (id) WHERE id < 30 ORDER BY id",
            "SELECT id, a FROM p LEFT JOIN q USING (id, a) WHERE id < 30 ORDER BY id",
            "SELECT id, q.b, p.k FROM q RIGHT JOIN p USING (id) WHERE id < 30 ORDER BY id",
            "SELECT id, p.b, q.a FROM q FULL JOIN p USING (id) WHERE id < 30 ORDER BY id",
            "SELECT id, q.a FROM q LEFT JOIN p USING (id) ORDER BY id DESC LIMIT 20",
            "SELECT id, a, b, k FROM p NATURAL JOIN q ORDER BY id",
            "SELECT count(*), sum(id) FROM (SELECT id FROM q FULL JOIN p USING (id))",
            "SELECT x.a, y.a FROM (SELECT a FROM p WHERE id < 5) x, (SELECT a FROM q WHERE id < 5) y ORDER BY 1, 2",
        ];
        let mut diffs = Vec::new();
        for (label, conn) in [("file", &file), ("memory", &memory)] {
            for q in queries {
                let fr = norm(fq(conn, q).await);
                let rr = norm(rq(&r, q));
                if fr != rr {
                    diffs.push(format!(
                        "  [{label}] `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            }
        }
        assert!(
            diffs.is_empty(),
            "{} projection divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
