#![recursion_limit = "512"]

//! bd-pvv2k: a seek through an index whose root page is damaged returned no
//! row (or a wrong count) instead of SQLITE_CORRUPT, and a DELETE driven by
//! such a seek removed a live row. `br doctor --repair` (beads_rust-n8pdn)
//! deleted live rows this way: its orphan check `dirty_issues LEFT JOIN issues
//! ... WHERE i.id IS NULL` saw one orphan on a damaged PRIMARY KEY autoindex.
//! The engine opened a root page with an unknown page-type byte as a table
//! cursor, and a table cursor answers a record-key seek with "no rows" without
//! reading the page. It now takes the kind from the schema, so the first read
//! of the page reports corruption, as stock does.
//!
//! Each file is built by stock SQLite (rusqlite, bundled), its PRIMARY KEY
//! autoindex root is overwritten with 0xff, and every statement's outcome --
//! rows, or SQLITE_CORRUPT -- is compared with stock's on its own copy: a
//! single-leaf index and a 3000-row index whose root is an interior page, ad
//! hoc and prepared. Statements that do not read the damaged index must still
//! answer like stock.

use std::path::Path;

use fsqlite_core::connection::{Connection, Row};
use fsqlite_error::ErrorCode;
use fsqlite_types::value::SqliteValue;

const PAGE_SIZE: usize = 4096;
const INDEX: &str = "sqlite_autoindex_issues_1";

const SCHEMA: &str = "PRAGMA page_size=4096; PRAGMA journal_mode=DELETE;
    CREATE TABLE issues (id TEXT PRIMARY KEY, title TEXT NOT NULL);
    CREATE TABLE dirty_issues (issue_id TEXT PRIMARY KEY, marked_at TEXT NOT NULL);";

const SINGLE: &str = "INSERT INTO issues VALUES ('bd-live', 'live');
    INSERT INTO dirty_issues VALUES ('bd-live', '2026-10-08');";

const MULTI: &str = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3000)
    INSERT INTO issues SELECT printf('bd-x%05d', i), 'title ' || i FROM n;
    INSERT INTO dirty_issues SELECT id, '2026-10-08' FROM issues WHERE rowid % 100 = 0;";

/// `(statement, reads the damaged index)`: stock must report SQLITE_CORRUPT
/// for the first kind and answer the second.
const SINGLE_QUERIES: &[(&str, bool)] = &[
    (
        "SELECT COUNT(*) FROM dirty_issues d LEFT JOIN issues i ON d.issue_id = i.id WHERE i.id IS NULL",
        true,
    ),
    (
        "SELECT d.issue_id FROM dirty_issues d LEFT JOIN issues i ON d.issue_id = i.id WHERE i.id IS NULL",
        true,
    ),
    (
        "SELECT COUNT(*) FROM dirty_issues WHERE NOT EXISTS \
         (SELECT 1 FROM issues WHERE issues.id = dirty_issues.issue_id)",
        true,
    ),
    ("SELECT id FROM issues WHERE id = 'bd-live'", true),
    ("SELECT title FROM issues NOT INDEXED WHERE id = 'bd-live'", false),
    ("SELECT issue_id, marked_at FROM dirty_issues ORDER BY 1", false),
];

const MULTI_QUERIES: &[(&str, bool)] = &[
    (
        "SELECT COUNT(*) FROM dirty_issues d LEFT JOIN issues i ON d.issue_id = i.id WHERE i.id IS NULL",
        true,
    ),
    (
        "SELECT COUNT(*) FROM dirty_issues WHERE NOT EXISTS \
         (SELECT 1 FROM issues WHERE issues.id = dirty_issues.issue_id)",
        true,
    ),
    ("SELECT id FROM issues WHERE id = 'bd-x01500'", true),
    ("SELECT COUNT(*) FROM issues WHERE id > 'bd-x01000'", true),
    ("SELECT title FROM issues NOT INDEXED WHERE id = 'bd-x01500'", false),
    ("SELECT COUNT(*) FROM dirty_issues", false),
];

const DELETE_ORPHANS: &str = "DELETE FROM dirty_issues WHERE NOT EXISTS \
     (SELECT 1 FROM issues WHERE issues.id = dirty_issues.issue_id)";
const DIRTY_STATE: &str = "SELECT issue_id, marked_at FROM dirty_issues NOT INDEXED ORDER BY rowid";

#[derive(Debug, PartialEq)]
enum Outcome {
    Rows(Vec<Vec<String>>),
    Corrupt,
    Error(String),
}

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn frank_outcome(result: fsqlite_error::Result<Vec<Row>>) -> Outcome {
    match result {
        Ok(rows) => Outcome::Rows(
            rows.iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect(),
        ),
        Err(e) if e.error_code() == ErrorCode::Corrupt => Outcome::Corrupt,
        Err(e) => Outcome::Error(e.to_string()),
    }
}

fn stock_outcome(r: &rusqlite::Connection, sql: &str) -> Outcome {
    let result = r.prepare(sql).and_then(|mut statement| {
        let n = statement.column_count();
        statement
            .query_map([], |row| {
                Ok((0..n)
                    .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect::<Vec<_>>())
            })
            .and_then(Iterator::collect)
    });
    match result {
        Ok(rows) => Outcome::Rows(rows),
        Err(e) if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseCorrupt) => {
            Outcome::Corrupt
        }
        Err(e) => Outcome::Error(e.to_string()),
    }
}

/// Build `path` with stock SQLite, then overwrite the first 200 bytes of the
/// index root with 0xff. Returns the root's page-type byte before the damage.
fn build_damaged(path: &Path, rows: &str) -> u8 {
    let root: i64 = {
        let c = rusqlite::Connection::open(path).expect("stock open");
        c.execute_batch(SCHEMA).expect("stock schema");
        c.execute_batch(rows).expect("stock rows");
        c.query_row(
            "SELECT rootpage FROM sqlite_master WHERE name = ?1",
            [INDEX],
            |row| row.get(0),
        )
        .expect("index root")
    };
    let mut bytes = std::fs::read(path).expect("read db");
    let start = (usize::try_from(root).expect("root") - 1) * PAGE_SIZE;
    let type_byte = bytes[start];
    bytes[start..start + 200].fill(0xff);
    std::fs::write(path, bytes).expect("write db");
    type_byte
}

async fn check_fixture(
    dir: &Path,
    name: &str,
    rows: &str,
    root_type: u8,
    queries: &[(&str, bool)],
    failures: &mut Vec<String>,
) {
    let frank_path = dir.join(format!("{name}_frank.db"));
    let stock_path = dir.join(format!("{name}_stock.db"));
    assert_eq!(build_damaged(&frank_path, rows), root_type, "{name}: index root page type");
    std::fs::copy(&frank_path, &stock_path).expect("copy db");
    let r = rusqlite::Connection::open(&stock_path).expect("stock reopen");
    let f = Connection::open(frank_path.to_str().expect("utf-8 path"))
        .await
        .expect("frank open");
    for &(sql, reads_damage) in queries {
        let stock = stock_outcome(&r, sql);
        assert_eq!(
            stock == Outcome::Corrupt,
            reads_damage,
            "{name}: stock outcome for `{sql}`: {stock:?}"
        );
        let direct = frank_outcome(f.query(sql).await);
        if direct != stock {
            failures.push(format!("[{name}] `{sql}` (ad hoc): frank {direct:?} vs stock {stock:?}"));
        }
        let prepared = frank_outcome(match f.prepare(sql).await {
            Ok(statement) => statement.query().await,
            Err(e) => Err(e),
        });
        if prepared != stock {
            failures.push(format!(
                "[{name}] `{sql}` (prepared): frank {prepared:?} vs stock {stock:?}"
            ));
        }
    }
    let integrity = frank_outcome(f.query("PRAGMA integrity_check").await);
    if integrity == Outcome::Rows(vec![vec!["'ok'".to_owned()]]) {
        failures.push(format!("[{name}] PRAGMA integrity_check reports ok on the damaged file"));
    }
    f.close().await.expect("close");
}

/// The orphan DELETE fails like stock's and leaves every row in place.
async fn check_delete(dir: &Path, name: &str, rows: &str, prepared: bool, failures: &mut Vec<String>) {
    let frank_path = dir.join(format!("{name}_delete_{prepared}_frank.db"));
    let stock_path = dir.join(format!("{name}_delete_{prepared}_stock.db"));
    build_damaged(&frank_path, rows);
    std::fs::copy(&frank_path, &stock_path).expect("copy db");
    let r = rusqlite::Connection::open(&stock_path).expect("stock reopen");
    let stock = match r.execute(DELETE_ORPHANS, []) {
        Ok(n) => format!("Ok({n})"),
        Err(e) if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseCorrupt) => {
            "Corrupt".to_owned()
        }
        Err(e) => format!("Err({e})"),
    };
    assert_eq!(stock, "Corrupt", "{name}: stock DELETE");
    let f = Connection::open(frank_path.to_str().expect("utf-8 path"))
        .await
        .expect("frank open");
    let result = if prepared {
        match f.prepare(DELETE_ORPHANS).await {
            Ok(statement) => statement.execute().await,
            Err(e) => Err(e),
        }
    } else {
        f.execute(DELETE_ORPHANS).await
    };
    let frank = match result {
        Ok(n) => format!("Ok({n})"),
        Err(e) if e.error_code() == ErrorCode::Corrupt => "Corrupt".to_owned(),
        Err(e) => format!("Err({e})"),
    };
    if frank != stock {
        failures.push(format!(
            "[{name} prepared={prepared}] `{DELETE_ORPHANS}`: frank {frank} vs stock {stock}"
        ));
    }
    let frank_state = frank_outcome(f.query(DIRTY_STATE).await);
    let stock_state = stock_outcome(&r, DIRTY_STATE);
    if frank_state != stock_state {
        failures.push(format!(
            "[{name} prepared={prepared}] dirty_issues after the DELETE: frank {frank_state:?} vs \
             stock {stock_state:?}"
        ));
    }
    f.close().await.expect("close");
}

#[test]
fn a_damaged_index_root_reports_corruption_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut failures = Vec::new();
        // 0x0a: leaf index page; 0x02: interior index page.
        check_fixture(dir.path(), "single", SINGLE, 0x0a, SINGLE_QUERIES, &mut failures).await;
        check_fixture(dir.path(), "multi", MULTI, 0x02, MULTI_QUERIES, &mut failures).await;
        for (name, rows) in [("single", SINGLE), ("multi", MULTI)] {
            for prepared in [false, true] {
                check_delete(dir.path(), name, rows, prepared, &mut failures).await;
            }
        }
        assert!(
            failures.is_empty(),
            "{} failures:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
