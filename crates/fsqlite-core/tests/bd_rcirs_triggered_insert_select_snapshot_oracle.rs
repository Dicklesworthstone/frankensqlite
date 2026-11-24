//! bd-rcirs: an INSERT ... SELECT into a table with triggers read its source
//! 10,000 rows at a time, inserting (and firing the triggers for) each chunk
//! before reading the next, so a trigger that wrote the source changed what the
//! later chunks read: an UPDATE showed through (`10001:seen`), an INSERT added
//! rows past the end, and with an OFFSET-paged source a DELETE shifted rows out.
//! Stock reads the whole SELECT into a temporary table before the first row
//! whenever the target has INSERT triggers, so no row sees a trigger's write.
//!
//! When the target has any trigger, the fallback now reads every chunk before
//! inserting, as stock does, into a buffer that spills to a temp file under the
//! temp_store budget, and inserts from it. The keepers run against bundled
//! SQLite 3.54.1 on `:memory:` and on a file: sources on both sides of every
//! chunk boundary (9,999 / 10,000 / 10,001 / 20,001 rows) with triggers that
//! UPDATE, DELETE and INSERT the source; a keyset-paged and an OFFSET-paged
//! source; the source written through a second table's trigger, under a quoted
//! and a bracketed name, through a view's INSTEAD OF trigger, and by a REPLACE;
//! and a buffer forced to spill by a one-page temp cache.

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

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn.prepare(sql).expect("stock verify prepares");
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock verify runs")
}

async fn fsqlite_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(render_fsqlite).collect())
                .collect()
        })
        .map_err(|e| e.to_string())
}

/// Triggers on the target that UPDATE, DELETE and INSERT the source.
const DIRECT_TRIGGERS: &[&str] = &[
    "CREATE TRIGGER tu AFTER INSERT ON t WHEN NEW.n % 5 = 0 BEGIN \
       UPDATE src SET tag = 'seen' WHERE i = NEW.id + 1; END",
    "CREATE TRIGGER td BEFORE INSERT ON t WHEN NEW.n % 7 = 0 BEGIN \
       DELETE FROM src WHERE i = NEW.id + 2; END",
    "CREATE TRIGGER tn AFTER INSERT ON t WHEN NEW.n % 11 = 0 BEGIN \
       INSERT INTO src VALUES (NEW.id + 1000000, NEW.n, 'new'); END",
];

/// The target's trigger writes `u`, and `u`'s trigger writes the source.
const CHAINED_TRIGGERS: &[&str] = &[
    "CREATE TABLE u(k INTEGER PRIMARY KEY, v INTEGER)",
    "CREATE TRIGGER tt AFTER INSERT ON t WHEN NEW.n % 5 = 0 BEGIN \
       INSERT INTO u VALUES (NEW.id, NEW.n); END",
    "CREATE TRIGGER uu AFTER INSERT ON u BEGIN \
       UPDATE src SET tag = 'seen' WHERE i = NEW.k + 1; \
       DELETE FROM src WHERE i = NEW.k + 3; END",
];

/// The source named quoted and bracketed.
const QUOTED_TRIGGERS: &[&str] = &[
    "CREATE TRIGGER tq AFTER INSERT ON t WHEN NEW.n % 5 = 0 BEGIN \
       UPDATE \"src\" SET tag = 'seen' WHERE i = NEW.id + 1; END",
    "CREATE TRIGGER tb BEFORE INSERT ON t WHEN NEW.n % 7 = 0 BEGIN \
       DELETE FROM [src] WHERE i = NEW.id + 2; END",
];

/// The source written through a view's INSTEAD OF trigger.
const VIEW_TRIGGERS: &[&str] = &[
    "CREATE VIEW vsrc AS SELECT i, n, tag FROM src",
    "CREATE TRIGGER vu INSTEAD OF UPDATE ON vsrc BEGIN \
       UPDATE src SET tag = NEW.tag WHERE i = OLD.i; END",
    "CREATE TRIGGER tv AFTER INSERT ON t WHEN NEW.n % 5 = 0 BEGIN \
       UPDATE vsrc SET tag = 'seen' WHERE i = NEW.id + 1; END",
];

/// A REPLACE in the body overwrites a later source row.
const REPLACE_TRIGGERS: &[&str] = &[
    "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.n % 5 = 0 BEGIN \
       INSERT OR REPLACE INTO src VALUES (NEW.id + 1, -NEW.n, 'rep'); END",
];

/// Keyset-paged (rowid order, an expression column so the simple stream
/// declines) and OFFSET-paged (ORDER BY) sources.
const KEYSET_SELECT: &str = "INSERT INTO t SELECT i, n + 0, tag FROM src";
const KEYSET_WHERE_SELECT: &str = "INSERT INTO t SELECT i, n, tag FROM src WHERE n > 0";
const OFFSET_SELECT: &str = "INSERT INTO t SELECT i, n * 1, tag FROM src ORDER BY i";

const VERIFY: &[&str] = &[
    "SELECT count(*), sum(id), sum(n) FROM t",
    "SELECT tag, count(*), sum(id) FROM t GROUP BY tag ORDER BY tag",
    "SELECT count(*), sum(i), sum(tag = 'seen'), sum(tag = 'new') FROM src",
    "SELECT max(id), min(id) FROM t",
];

struct Case {
    name: &'static str,
    rows: i64,
    setup: &'static [&'static str],
    triggers: &'static [&'static str],
    insert: &'static str,
}

fn cases() -> Vec<Case> {
    let mut cases: Vec<Case> = [9_999, 10_000, 10_001, 20_001]
        .into_iter()
        .map(|rows| Case {
            name: "keyset",
            rows,
            setup: &[],
            triggers: DIRECT_TRIGGERS,
            insert: KEYSET_SELECT,
        })
        .collect();
    cases.extend([
        Case {
            name: "keyset_where",
            rows: 10_001,
            setup: &[],
            triggers: DIRECT_TRIGGERS,
            insert: KEYSET_WHERE_SELECT,
        },
        Case {
            name: "offset",
            rows: 20_001,
            setup: &[],
            triggers: DIRECT_TRIGGERS,
            insert: OFFSET_SELECT,
        },
        Case {
            name: "chained",
            rows: 10_001,
            setup: &[],
            triggers: CHAINED_TRIGGERS,
            insert: KEYSET_SELECT,
        },
        Case {
            name: "quoted",
            rows: 10_001,
            setup: &[],
            triggers: QUOTED_TRIGGERS,
            insert: KEYSET_SELECT,
        },
        Case {
            name: "view",
            rows: 10_001,
            setup: &[],
            triggers: VIEW_TRIGGERS,
            insert: KEYSET_SELECT,
        },
        Case {
            name: "replace",
            rows: 10_001,
            setup: &[],
            triggers: REPLACE_TRIGGERS,
            insert: KEYSET_SELECT,
        },
        Case {
            name: "spilled",
            rows: 20_001,
            setup: &["PRAGMA temp_store = FILE", "PRAGMA temp.cache_size = 1"],
            triggers: DIRECT_TRIGGERS,
            insert: KEYSET_SELECT,
        },
    ]);
    cases
}

fn schema(rows: i64) -> Vec<String> {
    vec![
        "CREATE TABLE src(i INTEGER PRIMARY KEY, n INTEGER, tag TEXT)".to_owned(),
        format!(
            "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < {rows}) \
             INSERT INTO src SELECT i, i, 'g' FROM s"
        ),
        "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, tag TEXT)".to_owned(),
    ]
}

fn database_path(dir: &tempfile::TempDir, engine: &str, file_backed: bool) -> String {
    if file_backed {
        dir.path()
            .join(format!("{engine}.db"))
            .to_str()
            .unwrap()
            .to_owned()
    } else {
        ":memory:".to_owned()
    }
}

#[test]
fn triggered_insert_select_reads_its_source_before_any_trigger_fires() {
    let cases = cases();
    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for case in &cases {
            for file_backed in [false, true] {
                let label = format!("{} rows={} file_backed={file_backed}", case.name, case.rows);
                let dir = tempfile::tempdir().unwrap();
                let stock =
                    rusqlite::Connection::open(database_path(&dir, "stock", file_backed)).unwrap();
                let conn = Connection::open(&database_path(&dir, "fsqlite", file_backed))
                    .await
                    .unwrap();
                let statements = schema(case.rows)
                    .into_iter()
                    .chain(case.setup.iter().map(|sql| (*sql).to_owned()))
                    .chain(case.triggers.iter().map(|sql| (*sql).to_owned()));
                for sql in statements {
                    stock.execute_batch(&sql).unwrap();
                    conn.execute(&sql)
                        .await
                        .unwrap_or_else(|e| panic!("{label}: `{sql}`: {e}"));
                }
                let spills_before = conn.temp_store_stats().temp_table_spill_events;
                stock.execute_batch(case.insert).unwrap();
                if let Err(e) = conn.execute(case.insert).await {
                    mismatches.push(format!("{label}: the insert failed: {e}"));
                    continue;
                }
                let spills = conn.temp_store_stats().temp_table_spill_events - spills_before;
                if case.name == "spilled" && spills == 0 {
                    mismatches.push(format!(
                        "{label}: the one-page temp cache did not spill the staged source"
                    ));
                }
                let expected: Vec<_> = VERIFY.iter().map(|sql| stock_rows(&stock, sql)).collect();
                // Not vacuous: on stock no inserted row saw a trigger's write.
                let tags = &expected[1];
                if tags.len() != 1 || tags[0][0] != "'g'" {
                    mismatches.push(format!("{label}: stock tags {tags:?}"));
                }
                for (sql, want) in VERIFY.iter().zip(&expected) {
                    match fsqlite_rows(&conn, sql).await {
                        Ok(got) if &got == want => {}
                        got => mismatches.push(format!(
                            "{label}: `{sql}`: stock {want:?}, fsqlite {got:?}"
                        )),
                    }
                }
            }
        }
        assert!(
            mismatches.is_empty(),
            "bd-rcirs: {} mismatch(es) against stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}
