//! GH #407 — `COUNT(*)` over a grouped/`HAVING` `IN`-subquery must keep
//! returning the count once further predicates follow the subquery.
//!
//! Reported against fsqlite 0.3.15 from beads_rust: the multi-label AND filter
//!
//! ```sql
//! SELECT COUNT(*) FROM issues WHERE issues.id IN (
//!     SELECT issue_id FROM labels WHERE label IN (?, ?)
//!     GROUP BY issue_id HAVING COUNT(DISTINCT label) = ?)
//! ```
//!
//! answered 1, but the same query with `1=1 AND` in front and
//! `AND status NOT IN (...) AND (is_template = 0 OR is_template IS NULL)`
//! behind it answered NULL / no row. Only the trailing predicates differ, and
//! stock SQLite answers 1 for both.
//!
//! Every query here is checked against bundled stock SQLite, and the shapes
//! sweep the neighbourhood: the subquery first / last / in the middle, with
//! and without `GROUP BY` on the outer query, with `AND` and with `OR`
//! trailing predicates, with literals and with bound parameters, and for
//! `COUNT(*)`, `COUNT(col)` and `SUM`.

#![recursion_limit = "512"]

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

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"));
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"));
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

/// The reporter's fixture, verbatim in shape: three issues, two labels, and a
/// `labels` table with a composite primary key.
async fn data() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE issues (id TEXT PRIMARY KEY, status TEXT NOT NULL, is_template INTEGER)",
        "CREATE TABLE labels (issue_id TEXT NOT NULL, label TEXT NOT NULL, \
         PRIMARY KEY (issue_id, label))",
        "INSERT INTO issues VALUES ('bd-both','open',NULL),('bd-one','open',NULL),\
         ('bd-other','open',NULL)",
        "INSERT INTO labels VALUES ('bd-both','backend'),('bd-both','urgent'),\
         ('bd-one','backend'),('bd-other','urgent')",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

const SUB: &str = "SELECT issue_id FROM labels WHERE label IN ('backend','urgent') \
                   GROUP BY issue_id HAVING COUNT(DISTINCT label) = 2";

#[test]
fn gh407_count_with_trailing_predicates_after_the_in_subquery() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        let a = format!("SELECT COUNT(*) FROM issues WHERE issues.id IN ({SUB})");
        // The exact failing statement from the report.
        let b = format!(
            "SELECT COUNT(*) FROM issues WHERE 1=1 AND issues.id IN ({SUB}) \
             AND status NOT IN ('closed','tombstone','deferred') \
             AND (is_template = 0 OR is_template IS NULL)"
        );
        // The trailing predicates without the leading `1=1`.
        let c = format!(
            "SELECT COUNT(*) FROM issues WHERE issues.id IN ({SUB}) \
             AND status NOT IN ('closed','tombstone','deferred')"
        );
        // Leading `1=1` only.
        let d = format!("SELECT COUNT(*) FROM issues WHERE 1=1 AND issues.id IN ({SUB})");
        check(
            &f,
            &r,
            &[&a, &b, &c, &d],
            "gh407_count_with_trailing_predicates",
        )
        .await;
    });
}

#[test]
fn gh407_neighbouring_predicate_placements() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        let queries = [
            // Subquery in the middle of a conjunction.
            format!(
                "SELECT COUNT(*) FROM issues WHERE status = 'open' AND issues.id IN ({SUB}) \
                 AND is_template IS NULL"
            ),
            // Trailing OR instead of AND.
            format!(
                "SELECT COUNT(*) FROM issues WHERE issues.id IN ({SUB}) \
                 OR status = 'nonexistent'"
            ),
            // A parenthesised trailing disjunction, the reporter's last clause.
            format!(
                "SELECT COUNT(*) FROM issues WHERE issues.id IN ({SUB}) \
                 AND (is_template = 0 OR is_template IS NULL)"
            ),
            // NOT IN over the same grouped subquery.
            format!(
                "SELECT COUNT(*) FROM issues WHERE issues.id NOT IN ({SUB}) \
                 AND status NOT IN ('closed','tombstone')"
            ),
            // A trailing predicate that eliminates every row: still one row, 0.
            format!("SELECT COUNT(*) FROM issues WHERE issues.id IN ({SUB}) AND status = 'closed'"),
            // Other aggregates over the same shape.
            format!(
                "SELECT COUNT(id), COUNT(is_template) FROM issues WHERE issues.id IN ({SUB}) \
                 AND status NOT IN ('closed')"
            ),
            format!(
                "SELECT SUM(1) FROM issues WHERE 1=1 AND issues.id IN ({SUB}) \
                 AND status NOT IN ('closed')"
            ),
            // With an outer GROUP BY.
            format!(
                "SELECT status, COUNT(*) FROM issues WHERE 1=1 AND issues.id IN ({SUB}) \
                 AND status NOT IN ('closed','tombstone','deferred') \
                 AND (is_template = 0 OR is_template IS NULL) GROUP BY status ORDER BY status"
            ),
            // With an outer GROUP BY + HAVING.
            format!(
                "SELECT status, COUNT(*) FROM issues WHERE issues.id IN ({SUB}) \
                 AND status NOT IN ('closed') GROUP BY status HAVING COUNT(*) >= 1 \
                 ORDER BY status"
            ),
            // The plain row list the report says already worked, as a control.
            format!(
                "SELECT id FROM issues WHERE 1=1 AND issues.id IN ({SUB}) \
                 AND status NOT IN ('closed','tombstone','deferred') \
                 AND (is_template = 0 OR is_template IS NULL) ORDER BY id"
            ),
        ];
        let refs: Vec<&str> = queries.iter().map(String::as_str).collect();
        check(&f, &r, &refs, "gh407_neighbouring_predicate_placements").await;
    });
}

#[test]
fn gh407_count_with_bound_parameters() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        let sql = "SELECT COUNT(*) FROM issues WHERE 1=1 AND issues.id IN (\
                   SELECT issue_id FROM labels WHERE label IN (?, ?) \
                   GROUP BY issue_id HAVING COUNT(DISTINCT label) = ?) \
                   AND status NOT IN ('closed','tombstone','deferred') \
                   AND (is_template = 0 OR is_template IS NULL)";
        let params = vec![
            SqliteValue::Text("backend".into()),
            SqliteValue::Text("urgent".into()),
            SqliteValue::Integer(2),
        ];
        let rows = f.query_with_params(sql, &params).await.unwrap();
        assert_eq!(rows.len(), 1, "an aggregate query always returns one row");
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(1),
            "bound-parameter form must count the one matching issue"
        );

        let expected: i64 = r
            .query_row(sql, rusqlite::params!["backend", "urgent", 2_i64], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(expected, 1, "stock SQLite agrees");
    });
}
