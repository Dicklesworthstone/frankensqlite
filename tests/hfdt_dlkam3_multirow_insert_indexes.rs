//! hfdt-dlkam3: one multi-row `INSERT ... VALUES (...), (...)` into a table whose
//! columns carry CHECK constraints and RESTRICT foreign keys must put every row
//! into every index. On 0.4.6 a 1,425-row statement left rows 1,425.. with
//! index records "missing trailing integer rowid" in all seven indexes.
#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "
CREATE TABLE intents (intent_id TEXT PRIMARY KEY NOT NULL);
CREATE TABLE captures (capture_id TEXT PRIMARY KEY NOT NULL);
CREATE TABLE facts (fact_id TEXT PRIMARY KEY NOT NULL);
CREATE TABLE capture_facts (capture_fact_id TEXT PRIMARY KEY NOT NULL);
CREATE TABLE members (
    intent_id TEXT NOT NULL REFERENCES intents (intent_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    intent_ordinal INTEGER NOT NULL CHECK (intent_ordinal >= 0),
    capture_id TEXT NOT NULL REFERENCES captures (capture_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    capture_ordinal INTEGER NOT NULL CHECK (capture_ordinal >= 0),
    fact_id TEXT NOT NULL REFERENCES facts (fact_id) ON UPDATE RESTRICT ON DELETE RESTRICT
        CHECK ((LENGTH(fact_id) > 0) AND (fact_id = TRIM(fact_id))),
    capture_fact_id TEXT NOT NULL
        REFERENCES capture_facts (capture_fact_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    natural_key TEXT NOT NULL
        CHECK ((LENGTH(CAST(natural_key AS BLOB)) BETWEEN 1 AND 262144)
            AND (natural_key = TRIM(natural_key))),
    before_content_hash TEXT CHECK ((before_content_hash IS NULL)
        OR ((LENGTH(before_content_hash) > 0) AND (before_content_hash = TRIM(before_content_hash)))),
    after_content_hash TEXT NOT NULL
        CHECK ((LENGTH(after_content_hash) > 0) AND (after_content_hash = TRIM(after_content_hash))),
    revision INTEGER NOT NULL CHECK (revision > 0),
    persisted_at TEXT NOT NULL
        CHECK ((LENGTH(persisted_at) > 0) AND (persisted_at = TRIM(persisted_at))),
    PRIMARY KEY (intent_id, intent_ordinal),
    UNIQUE (intent_id, capture_ordinal),
    UNIQUE (intent_id, fact_id),
    UNIQUE (capture_fact_id)
);
CREATE INDEX members_capture ON members (capture_id, capture_ordinal, intent_id);
CREATE INDEX members_fact ON members (fact_id, intent_id);
CREATE INDEX members_intent_capture ON members (intent_id, capture_id);
CREATE TRIGGER members_no_update BEFORE UPDATE ON members
    BEGIN SELECT RAISE(ABORT, 'append-only'); END;
CREATE TRIGGER members_no_delete BEFORE DELETE ON members
    BEGIN SELECT RAISE(ABORT, 'append-only'); END;
";

fn text(value: String) -> SqliteValue {
    SqliteValue::Text(value.into())
}

fn insert_sql(rows: usize) -> String {
    let values = (0..rows)
        .map(|row| {
            let first = row * 11 + 1;
            let placeholders: Vec<String> = (first..first + 11).map(|i| format!("?{i}")).collect();
            format!("({})", placeholders.join(", "))
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO members (intent_id, intent_ordinal, capture_id, capture_ordinal, fact_id, \
         capture_fact_id, natural_key, before_content_hash, after_content_hash, revision, \
         persisted_at) VALUES {values};"
    )
}

fn params(rows: usize) -> Vec<SqliteValue> {
    let mut params = Vec::with_capacity(rows * 11);
    for row in 0..rows {
        let ordinal = i64::try_from(row).expect("row");
        params.extend([
            text("intent-1".to_owned()),
            SqliteValue::Integer(ordinal),
            text("capture-1".to_owned()),
            SqliteValue::Integer(ordinal),
            text(format!("fact-{row:06}")),
            text(format!("capture-fact-{row:06}")),
            text(format!("natural-key-{row:06}")),
            SqliteValue::Null,
            text(format!("blake3:{row:064x}")),
            SqliteValue::Integer(1),
            text("2026-09-28T23:30:46.162199335+00:00".to_owned()),
        ]);
    }
    params
}

async fn run_case(path: &str, rows: usize, foreign_keys: bool) -> Vec<String> {
    let conn = Connection::open(path).await.expect("open");
    conn.execute_batch(SCHEMA).await.expect("schema");
    conn.execute(if foreign_keys {
        "PRAGMA foreign_keys = ON"
    } else {
        "PRAGMA foreign_keys = OFF"
    })
    .await
    .expect("fk");
    conn.execute("BEGIN IMMEDIATE").await.expect("begin");
    conn.execute_with_params("INSERT INTO intents VALUES (?1)", &[text("intent-1".to_owned())])
        .await
        .expect("intent");
    conn.execute_with_params("INSERT INTO captures VALUES (?1)", &[text("capture-1".to_owned())])
        .await
        .expect("capture");
    for row in 0..rows {
        conn.execute_with_params("INSERT INTO facts VALUES (?1)", &[text(format!("fact-{row:06}"))])
            .await
            .expect("fact");
        conn.execute_with_params(
            "INSERT INTO capture_facts VALUES (?1)",
            &[text(format!("capture-fact-{row:06}"))],
        )
        .await
        .expect("capture fact");
    }
    let affected = conn
        .execute_with_params(&insert_sql(rows), &params(rows))
        .await
        .expect("bulk insert");
    conn.execute("COMMIT").await.expect("commit");
    assert_eq!(affected, rows);
    let lines: Vec<String> = conn
        .query("PRAGMA integrity_check")
        .await
        .expect("integrity")
        .iter()
        .map(|row| format!("{:?}", row.values()[0]))
        .collect();
    conn.close().await.expect("close");
    lines
}

#[test]
fn multirow_insert_with_checks_and_fks_indexes_every_row() {
    let dir = tempfile::tempdir().expect("tempdir");
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for rows in [1_024_usize, 1_424, 1_425, 2_048] {
            for foreign_keys in [true, false] {
                let path = dir.path().join(format!("m_{rows}_{foreign_keys}.db"));
                let lines = run_case(path.to_str().expect("utf-8"), rows, foreign_keys).await;
                let ok = lines.len() == 1 && lines[0] == "Text(\"ok\")";
                println!(
                    "PROBE rows={rows} fk={foreign_keys} ok={ok} lines={} first={:?}",
                    lines.len(),
                    lines.first()
                );
                if !ok {
                    failures.push(format!("rows={rows} fk={foreign_keys}: {:?}", lines.first()));
                }
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
    });
}
