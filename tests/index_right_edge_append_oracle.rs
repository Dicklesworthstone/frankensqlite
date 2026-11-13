//! bd-reality-check-1022-llk6g.8 — a non-UNIQUE `IdxInsert` whose key sorts
//! after the previous right-edge key appends from the cursor's position
//! instead of seeking from the root (CREATE INDEX over rows already in key
//! order, monotonic ingest). The append trusts the cursor's cached path and a
//! key comparison, so this keeper pins the index contents against stock SQLite
//! wherever that trust could go wrong: three-level trees, duplicate keys
//! (rowid breaks the tie), DESC columns, keys crossing storage classes, keys
//! that overflow their leaf cell, WITHOUT ROWID suffixes, UTF-16 text whose
//! byte order differs from code-point order, and writes that move or delete
//! index entries between two appends (REPLACE, UPDATE, DELETE, savepoint
//! rollback). Each scenario runs on both engines; queries through every index
//! must return stock's rows and stock's integrity_check must accept the file
//! fsqlite wrote.

#![recursion_limit = "512"]

use std::fmt::Write as _;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use rusqlite::types::ValueRef;

fn render(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(integer) => integer.to_string(),
        SqliteValue::Float(float) => format!("{float}r"),
        SqliteValue::Blob(blob) => hex(blob),
        SqliteValue::Text(_) => format!("'{}'", value.as_text().unwrap_or_default()),
    }
}

fn render_stock(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => "NULL".to_owned(),
        ValueRef::Integer(integer) => integer.to_string(),
        ValueRef::Real(float) => format!("{float}r"),
        ValueRef::Text(text) => format!("'{}'", String::from_utf8_lossy(text)),
        ValueRef::Blob(blob) => hex(blob),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold("X'".to_owned(), |mut out, byte| {
        let _ = write!(out, "{byte:02X}");
        out
    }) + "'"
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|error| panic!("fsqlite `{sql}`: {error}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn
        .prepare(sql)
        .unwrap_or_else(|error| panic!("stock prepare `{sql}`: {error}"));
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            (0..width)
                .map(|index| row.get_ref(index).map(render_stock))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap_or_else(|error| panic!("stock `{sql}`: {error}"))
        .map(|row| row.expect("stock row"))
        .collect()
}

/// `INSERT INTO <table> VALUES` statements, 500 rows each.
fn inserts(table: &str, rows: impl IntoIterator<Item = String>) -> Vec<String> {
    let rows: Vec<String> = rows.into_iter().collect();
    rows.chunks(500)
        .map(|chunk| format!("INSERT INTO {table} VALUES {}", chunk.join(", ")))
        .collect()
}

struct Scenario {
    label: &'static str,
    statements: Vec<String>,
    /// Ordered queries through the indexes the statements built.
    checks: Vec<&'static str>,
}

/// Keys rising with rowid, over enough rows for a three-level index tree, with
/// the indexes built after the load (the CREATE INDEX backfill).
fn rising_backfill() -> Scenario {
    let mut statements = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, d INTEGER, s INTEGER)".to_owned(),
        "BEGIN".to_owned(),
    ];
    statements.extend(inserts(
        "t",
        (1..=30_000).map(|id| format!("({id}, '{id:040}', {}, {})", id / 50, -id)),
    ));
    statements.extend(
        [
            "COMMIT",
            "CREATE INDEX ix_k ON t(k)",
            "CREATE INDEX ix_d ON t(d)",
            "CREATE INDEX ix_dk ON t(d, k DESC)",
            "CREATE INDEX ix_s_desc ON t(s DESC)",
            "CREATE INDEX ix_s ON t(s)",
        ]
        .map(str::to_owned),
    );
    Scenario {
        label: "rising keys, CREATE INDEX after the load",
        statements,
        checks: vec![
            "SELECT k, id FROM t INDEXED BY ix_k ORDER BY k",
            "SELECT d, id FROM t INDEXED BY ix_d ORDER BY d, id",
            "SELECT d, k, id FROM t INDEXED BY ix_dk WHERE d % 7 = 0 ORDER BY d, k DESC",
            "SELECT s, id FROM t INDEXED BY ix_s_desc WHERE s > -3000 ORDER BY s DESC",
            "SELECT s, id FROM t INDEXED BY ix_s WHERE s < -27000 ORDER BY s",
        ],
    }
}

/// Keys that rise across storage classes: NULL, INTEGER, REAL (with an
/// INTEGER/REAL tie), TEXT, then BLOB, into an index built before the rows
/// arrive and another built after.
fn storage_classes() -> Scenario {
    let mut values: Vec<String> = vec!["NULL".to_owned(); 3];
    values.extend((1..=1_000).map(|n| n.to_string()));
    values.extend((1_000..1_400).map(|n| format!("{n}.5")));
    values.extend(["2000".to_owned(), "2000.0".to_owned(), "2000".to_owned()]);
    values.extend((0..800).map(|n| format!("'v{n:05}'")));
    values.extend((0..600).map(|n| format!("X'{n:06X}'")));
    let rows: Vec<String> = values
        .iter()
        .enumerate()
        .map(|(index, value)| format!("({}, {value})", index + 1))
        .collect();
    let mut statements = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v)".to_owned(),
        "CREATE INDEX ix_v ON t(v)".to_owned(),
        "CREATE TABLE u (id INTEGER PRIMARY KEY, v)".to_owned(),
        "BEGIN".to_owned(),
    ];
    statements.extend(inserts("t", rows.iter().cloned()));
    statements.extend(inserts("u", rows));
    statements.push("COMMIT".to_owned());
    statements.push("CREATE INDEX ix_u_v ON u(v)".to_owned());
    Scenario {
        label: "rising keys across storage classes",
        statements,
        checks: vec![
            "SELECT v, id FROM t INDEXED BY ix_v ORDER BY v, id",
            "SELECT v, id FROM u INDEXED BY ix_u_v ORDER BY v, id",
            "SELECT typeof(v), count(*) FROM t INDEXED BY ix_v WHERE v >= 2000 \
             GROUP BY typeof(v) ORDER BY 1",
        ],
    }
}

/// Rows arriving one statement at a time and in multi-row statements, in
/// autocommit and inside a transaction, with a smaller key every seventh row
/// so the append path disarms and re-arms.
fn ingest() -> Scenario {
    let row = |id: i64| {
        let a = if id % 7 == 0 { id - 1_000_000 } else { id };
        format!("({id}, {a}, 'g{:04}')", id / 100)
    };
    let mut statements = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)".to_owned(),
        "CREATE INDEX ix_a ON t(a)".to_owned(),
        "CREATE INDEX ix_ba ON t(b, a)".to_owned(),
    ];
    statements.extend((1..=200).map(|id| format!("INSERT INTO t VALUES {}", row(id))));
    statements.push("BEGIN".to_owned());
    statements.extend((201..=3_000).map(|id| format!("INSERT INTO t VALUES {}", row(id))));
    statements.push("COMMIT".to_owned());
    statements.extend(inserts("t", (3_001..=9_000).map(row)));
    Scenario {
        label: "ingest into indexed tables",
        statements,
        checks: vec![
            "SELECT a, id FROM t INDEXED BY ix_a ORDER BY a, id",
            "SELECT b, a, id FROM t INDEXED BY ix_ba ORDER BY b, a, id",
        ],
    }
}

/// Writes that delete or move entries of the appended index between two of
/// its appends: INSERT OR REPLACE evicting rows through a UNIQUE column,
/// UPDATE lifting keys to the top, DELETE, then more rising rows.
fn replace_update_delete() -> Scenario {
    let mut statements = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE, b INTEGER)".to_owned(),
        "CREATE INDEX ix_b ON t(b)".to_owned(),
        "CREATE INDEX ix_ub ON t(u, b)".to_owned(),
    ];
    statements.extend(inserts("t", (1..=2_000).map(|id| format!("({id}, {id}, {id})"))));
    statements.extend(
        inserts(
            "t",
            (1..=600).map(|n| format!("({}, {}, {})", 2_000 + n, n * 3, 2_000 + n)),
        )
        .into_iter()
        .map(|sql| sql.replacen("INSERT INTO", "INSERT OR REPLACE INTO", 1)),
    );
    statements.push("UPDATE t SET b = b + 100000 WHERE id % 5 = 0".to_owned());
    statements.push("DELETE FROM t WHERE id % 11 = 0".to_owned());
    statements.extend(inserts(
        "t",
        (1..=800).map(|n| format!("({}, {}, {})", 5_000 + n, 5_000 + n, 200_000 + n / 3)),
    ));
    Scenario {
        label: "REPLACE, UPDATE and DELETE between appends",
        statements,
        checks: vec![
            "SELECT b, id FROM t INDEXED BY ix_b ORDER BY b, id",
            "SELECT u, b, id FROM t INDEXED BY ix_ub ORDER BY u, b, id",
            "SELECT count(*), sum(b), sum(u) FROM t",
        ],
    }
}

/// Rising keys larger than an index leaf cell holds locally.
fn overflow_keys() -> Scenario {
    let pad = "x".repeat(1_500);
    let rows: Vec<String> = (1..=400)
        .map(|id| format!("({id}, '{id:06}{pad}', {})", id / 4))
        .collect();
    let mut statements = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, d INTEGER)".to_owned(),
        "CREATE INDEX ix_k ON t(k)".to_owned(),
        "BEGIN".to_owned(),
    ];
    statements.extend(inserts("t", rows));
    statements.push("COMMIT".to_owned());
    statements.push("CREATE INDEX ix_dk ON t(d, k)".to_owned());
    Scenario {
        label: "rising overflow keys",
        statements,
        checks: vec![
            "SELECT substr(k, 1, 6), length(k), id FROM t INDEXED BY ix_k ORDER BY k",
            "SELECT d, substr(k, 1, 6), id FROM t INDEXED BY ix_dk ORDER BY d, k",
        ],
    }
}

/// Secondary indexes of WITHOUT ROWID tables, whose keys end in the primary
/// key instead of a rowid.
fn without_rowid() -> Scenario {
    let mut statements = vec![
        "CREATE TABLE w (k INTEGER PRIMARY KEY, v INTEGER) WITHOUT ROWID".to_owned(),
        "CREATE INDEX ix_w_v ON w(v)".to_owned(),
        "CREATE TABLE z (k INTEGER PRIMARY KEY DESC, v INTEGER) WITHOUT ROWID".to_owned(),
        "BEGIN".to_owned(),
    ];
    statements.extend(inserts("w", (1..=6_000).map(|k| format!("({k}, {})", k / 10))));
    statements.extend(inserts("z", (1..=6_000).map(|k| format!("({k}, {k})"))));
    statements.push("COMMIT".to_owned());
    statements.push("CREATE INDEX ix_z_v ON z(v)".to_owned());
    Scenario {
        label: "WITHOUT ROWID secondary indexes",
        statements,
        checks: vec![
            "SELECT v, k FROM w INDEXED BY ix_w_v ORDER BY v, k",
            "SELECT v, k FROM z INDEXED BY ix_z_v ORDER BY v",
        ],
    }
}

/// UTF-16LE text whose stored byte order is not its code-point order: the
/// U+00FF group stores bytes FF 00, which sort after U+0100's 00 01, so the
/// later, code-point-larger group must not append after it.
fn utf16_byte_order() -> Scenario {
    let mut statements = vec![
        "PRAGMA encoding = 'UTF-16le'".to_owned(),
        "CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT)".to_owned(),
        "CREATE INDEX ix_k ON t(k)".to_owned(),
        "BEGIN".to_owned(),
    ];
    statements.extend(inserts(
        "t",
        (1..=600).map(|id| {
            let lead = if id <= 300 { '\u{ff}' } else { '\u{100}' };
            format!("({id}, '{lead}{id:04}')")
        }),
    ));
    statements.push("COMMIT".to_owned());
    statements.push("CREATE INDEX ix_k2 ON t(k, id)".to_owned());
    Scenario {
        label: "UTF-16LE byte order",
        statements,
        checks: vec![
            "SELECT k, id FROM t INDEXED BY ix_k ORDER BY k",
            "SELECT k, id FROM t INDEXED BY ix_k2 ORDER BY k, id",
        ],
    }
}

/// Appends on both sides of a savepoint rolled back mid-transaction.
fn savepoint_rollback() -> Scenario {
    let mut statements = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)".to_owned(),
        "CREATE INDEX ix_a ON t(a)".to_owned(),
        "BEGIN".to_owned(),
    ];
    statements.extend(inserts("t", (1..=1_500).map(|id| format!("({id}, {id})"))));
    statements.push("SAVEPOINT s".to_owned());
    statements.extend(inserts("t", (1_501..=3_000).map(|id| format!("({id}, {id})"))));
    statements.push("ROLLBACK TO s".to_owned());
    statements.extend(inserts(
        "t",
        (1_501..=2_500).map(|id| format!("({id}, {})", id * 2)),
    ));
    statements.push("RELEASE s".to_owned());
    statements.push("COMMIT".to_owned());
    Scenario {
        label: "savepoint rollback between appends",
        statements,
        checks: vec!["SELECT a, id FROM t INDEXED BY ix_a ORDER BY a, id"],
    }
}

async fn run(scenario: Scenario) -> Vec<String> {
    let label = scenario.label;
    let dir = tempfile::tempdir().expect("tempdir");
    let frank_path = dir.path().join("frank.db");
    let stock_path = dir.path().join("stock.db");
    let frank = Connection::open(frank_path.to_str().expect("utf-8 path"))
        .await
        .expect("fsqlite open");
    let stock = rusqlite::Connection::open(&stock_path).expect("stock open");
    for sql in &scenario.statements {
        stock
            .execute_batch(sql)
            .unwrap_or_else(|error| panic!("{label}: stock `{sql:.120}`: {error}"));
        frank
            .execute(sql)
            .await
            .unwrap_or_else(|error| panic!("{label}: fsqlite `{sql:.120}`: {error}"));
    }
    let mut failures = Vec::new();
    for sql in &scenario.checks {
        let expected = stock_rows(&stock, sql);
        assert!(!expected.is_empty(), "{label}: `{sql}` must select rows");
        let actual = frank_rows(&frank, sql).await;
        if actual != expected {
            failures.push(format!(
                "{label}: `{sql}` fsqlite {} rows, stock {} rows, first difference {:?}",
                actual.len(),
                expected.len(),
                actual.iter().zip(&expected).find(|(a, b)| a != b)
            ));
        }
    }
    let frank_check = frank_rows(&frank, "PRAGMA integrity_check").await;
    if frank_check != vec![vec!["'ok'".to_owned()]] {
        failures.push(format!("{label}: fsqlite integrity_check {frank_check:?}"));
    }
    frank.close().await.expect("fsqlite close");
    let reread = rusqlite::Connection::open(&frank_path).expect("stock opens the fsqlite file");
    let stock_check = stock_rows(&reread, "PRAGMA integrity_check");
    if stock_check != vec![vec!["'ok'".to_owned()]] {
        failures.push(format!(
            "{label}: stock integrity_check of the fsqlite file {stock_check:?}"
        ));
    }
    for sql in &scenario.checks {
        if stock_rows(&reread, sql) != stock_rows(&stock, sql) {
            failures.push(format!("{label}: stock on the fsqlite file differs for `{sql}`"));
        }
    }
    eprintln!("{label}: {} statements, {} failures", scenario.statements.len(), failures.len());
    failures
}

#[test]
fn right_edge_index_appends_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for scenario in [
            rising_backfill(),
            storage_classes(),
            ingest(),
            replace_update_delete(),
            overflow_keys(),
            without_rowid(),
            utf16_byte_order(),
            savepoint_rollback(),
        ] {
            failures.extend(run(scenario).await);
        }
        assert!(failures.is_empty(), "{failures:#?}");
    });
}
