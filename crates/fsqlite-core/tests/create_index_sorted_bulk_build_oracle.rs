//! bd-reality-check-1022-llk6g.8 — CREATE INDEX and REINDEX sort the index's
//! records and lay its pages out left to right (`IdxBulkLoad`) instead of
//! inserting one key at a time in table order.
//!
//! The build trusts a sort and writes b-tree pages without the balancer, so
//! this keeper pins the result against stock SQLite wherever that could go
//! wrong: keys unrelated to rowid order, duplicates and NULLs, DESC and
//! multi-column keys, NOCASE, expression and partial indexes, UNIQUE indexes,
//! keys crossing storage classes, keys that overflow their cell, WITHOUT ROWID
//! suffixes, every row count around a leaf boundary (including none and one),
//! REINDEX, ordinary writes into the pages the build left full, builds undone
//! by ROLLBACK, and VIRTUAL and STORED generated columns. Each scenario runs on
//! both engines. Queries through every index must return
//! stock's rows, stock's `integrity_check` must accept the file fsqlite wrote,
//! and each index must take no more than 1.3 times the pages stock gives it.
//! A UNIQUE build over duplicate keys must fail with stock's message and leave
//! no index behind.

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

/// Pages stock's `dbstat` counts for one index, and the fewest cells on any
/// of its leaves.
fn index_pages(conn: &rusqlite::Connection, index: &str) -> (i64, i64) {
    conn.query_row(
        "SELECT count(*), coalesce(min(ncell) FILTER (WHERE pagetype = 'leaf'), 0) \
         FROM dbstat WHERE name = ?1",
        [index],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap_or_else(|error| panic!("dbstat for {index}: {error}"))
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
    checks: Vec<String>,
    /// Indexes whose page count is compared with stock's.
    indexes: Vec<String>,
}

fn owned(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

/// 20,000 rows whose keys have nothing to do with rowid order: a unique
/// scattered integer, a 97-value integer, mixed-case text and a column that is
/// NULL in every ninth row.
fn scattered_table(table: &str) -> Vec<String> {
    let mut statements = vec![
        format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, r INTEGER, d INTEGER, s TEXT, n)"),
        "BEGIN".to_owned(),
    ];
    statements.extend(inserts(
        table,
        (1..=20_000_i64).map(|id| {
            let r = (id * 7919) % 20_011;
            let d = (id * 31) % 97;
            let word = (id * 7919) % 5_000;
            let s = if id % 2 == 0 {
                format!("Key{word:05}")
            } else {
                format!("kEY{word:05}")
            };
            let n = if id % 9 == 0 {
                "NULL".to_owned()
            } else {
                ((id * 13) % 1_000).to_string()
            };
            format!("({id}, {r}, {d}, '{s}', {n})")
        }),
    ));
    statements.push("COMMIT".to_owned());
    statements
}

fn scattered_keys() -> Scenario {
    let mut statements = scattered_table("t");
    statements.extend(owned(&[
        "CREATE INDEX ix_r ON t(r)",
        "CREATE INDEX ix_d ON t(d)",
        "CREATE INDEX ix_d_desc_r ON t(d DESC, r)",
        "CREATE INDEX ix_s_nocase ON t(s COLLATE NOCASE)",
        "CREATE INDEX ix_s_desc ON t(s DESC)",
        "CREATE INDEX ix_n ON t(n)",
        "CREATE INDEX ix_expr ON t(r + d % 7)",
        "CREATE INDEX ix_part ON t(r) WHERE d < 20",
        "CREATE UNIQUE INDEX ux_r ON t(r)",
        "CREATE UNIQUE INDEX ux_n_id ON t(n, id)",
    ]));
    Scenario {
        label: "keys scattered against rowid order",
        statements,
        checks: owned(&[
            "SELECT r, id FROM t INDEXED BY ix_r ORDER BY r",
            "SELECT d, id FROM t INDEXED BY ix_d ORDER BY d, id",
            "SELECT d, r, id FROM t INDEXED BY ix_d_desc_r WHERE d % 5 = 0 ORDER BY d DESC, r",
            "SELECT s, id FROM t INDEXED BY ix_s_nocase WHERE s COLLATE NOCASE >= 'key04000' \
             ORDER BY s COLLATE NOCASE, id",
            "SELECT s, id FROM t INDEXED BY ix_s_desc WHERE s < 'Key00200' ORDER BY s DESC, id",
            "SELECT n, id FROM t INDEXED BY ix_n WHERE n IS NULL OR n < 40 ORDER BY n, id",
            "SELECT r + d % 7, id FROM t INDEXED BY ix_expr WHERE r + d % 7 < 900 \
             ORDER BY r + d % 7, id",
            "SELECT r, id FROM t INDEXED BY ix_part WHERE d < 20 ORDER BY r",
            "SELECT r, id FROM t INDEXED BY ux_r WHERE r BETWEEN 500 AND 9000 ORDER BY r",
            "SELECT n, id FROM t INDEXED BY ux_n_id WHERE n > 990 ORDER BY n, id",
        ]),
        indexes: owned(&[
            "ix_r",
            "ix_d",
            "ix_d_desc_r",
            "ix_s_nocase",
            "ix_s_desc",
            "ix_n",
            "ix_expr",
            "ix_part",
            "ux_r",
            "ux_n_id",
        ]),
    }
}

/// NULL, INTEGER, REAL (with INTEGER/REAL ties), TEXT and BLOB keys, stored in
/// an order that mixes the classes.
fn storage_classes() -> Scenario {
    let mut values: Vec<String> = vec!["NULL".to_owned(); 5];
    values.extend((1..=1_000).map(|n| n.to_string()));
    values.extend((1_000..1_400).map(|n| format!("{n}.5")));
    values.extend(["2000".to_owned(), "2000.0".to_owned(), "2000".to_owned()]);
    values.extend((0..800).map(|n| format!("'v{n:05}'")));
    values.extend((0..600).map(|n| format!("X'{n:06X}'")));
    let count = values.len();
    // 7919 is prime and larger than `count`, so this visits every value once.
    let rows: Vec<String> = (0..count)
        .map(|position| format!("({}, {})", position + 1, values[(position * 7919) % count]))
        .collect();
    let mut statements = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v)".to_owned(),
        "BEGIN".to_owned(),
    ];
    statements.extend(inserts("t", rows));
    statements.push("COMMIT".to_owned());
    statements.push("CREATE INDEX ix_v ON t(v)".to_owned());
    statements.push("CREATE INDEX ix_v_desc ON t(v DESC)".to_owned());
    Scenario {
        label: "keys across storage classes",
        statements,
        checks: owned(&[
            "SELECT v, id FROM t INDEXED BY ix_v ORDER BY v, id",
            "SELECT v, id FROM t INDEXED BY ix_v_desc ORDER BY v DESC, id",
            "SELECT typeof(v), count(*) FROM t INDEXED BY ix_v WHERE v >= 2000 \
             GROUP BY typeof(v) ORDER BY 1",
        ]),
        indexes: owned(&["ix_v", "ix_v_desc"]),
    }
}

/// Keys larger than an index cell holds locally, in scattered order, so some
/// of them become dividers on interior pages.
fn overflow_keys() -> Scenario {
    let pad = "x".repeat(1_500);
    let rows: Vec<String> = (1..=400_i64)
        .map(|id| {
            let key = (id * 7919) % 401;
            format!("({id}, '{key:06}{pad}', {})", id / 4)
        })
        .collect();
    let mut statements = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, d INTEGER)".to_owned(),
        "BEGIN".to_owned(),
    ];
    statements.extend(inserts("t", rows));
    statements.push("COMMIT".to_owned());
    statements.push("CREATE INDEX ix_k ON t(k)".to_owned());
    statements.push("CREATE INDEX ix_dk ON t(d, k)".to_owned());
    Scenario {
        label: "overflowing keys",
        statements,
        checks: owned(&[
            "SELECT substr(k, 1, 6), length(k), id FROM t INDEXED BY ix_k ORDER BY k",
            "SELECT d, substr(k, 1, 6), id FROM t INDEXED BY ix_dk ORDER BY d, k",
        ]),
        indexes: owned(&["ix_k", "ix_dk"]),
    }
}

/// Secondary indexes of WITHOUT ROWID tables end in the PRIMARY KEY columns
/// the key does not already hold; the table is scanned in PRIMARY KEY order,
/// which is descending for `z` and two columns wide for `c`.
///
/// `z.v` has no repeated value. Where two entries share a key, stock orders
/// the appended PRIMARY KEY column in the PRIMARY KEY's own direction (DESC
/// for `z`) and fsqlite orders it ascending, on the per-row path as much as on
/// this one; stock's integrity_check rejects such an index. That predates the
/// sorted build and is not what this keeper pins.
fn without_rowid() -> Scenario {
    let mut statements = owned(&[
        "CREATE TABLE w (k INTEGER PRIMARY KEY, v INTEGER) WITHOUT ROWID",
        "CREATE TABLE z (k INTEGER PRIMARY KEY DESC, v INTEGER) WITHOUT ROWID",
        "CREATE TABLE c (a TEXT, b INTEGER, v INTEGER, w TEXT, PRIMARY KEY (a, b)) WITHOUT ROWID",
        "BEGIN",
    ]);
    statements.extend(inserts(
        "w",
        (1..=6_000_i64).map(|k| format!("({k}, {})", (k * 7919) % 53)),
    ));
    statements.extend(inserts(
        "z",
        (1..=6_000_i64).map(|k| format!("({k}, {})", (k * 31) % 6_007)),
    ));
    statements.extend(inserts(
        "c",
        (1..=6_000_i64).map(|n| {
            format!(
                "('a{:03}', {n}, {}, 'w{:04}')",
                (n * 7919) % 211,
                (n * 17) % 89,
                (n * 7) % 1_000
            )
        }),
    ));
    statements.extend(owned(&[
        "COMMIT",
        "CREATE INDEX ix_w_v ON w(v)",
        "CREATE INDEX ix_z_v ON z(v)",
        "CREATE INDEX ix_c_v ON c(v)",
        "CREATE INDEX ix_c_wb ON c(w DESC, b)",
        "CREATE UNIQUE INDEX ux_c_ba ON c(b, a)",
    ]));
    Scenario {
        label: "WITHOUT ROWID secondary indexes",
        statements,
        checks: owned(&[
            "SELECT v, k FROM w INDEXED BY ix_w_v ORDER BY v, k",
            "SELECT v, k FROM z INDEXED BY ix_z_v ORDER BY v, k",
            "SELECT v, a, b FROM c INDEXED BY ix_c_v ORDER BY v, a, b",
            "SELECT w, b, a FROM c INDEXED BY ix_c_wb WHERE w > 'w0900' ORDER BY w DESC, b, a",
            "SELECT b, a FROM c INDEXED BY ux_c_ba WHERE b < 300 ORDER BY b, a",
        ]),
        indexes: owned(&["ix_w_v", "ix_z_v", "ix_c_v", "ix_c_wb", "ux_c_ba"]),
    }
}

/// Every row count from one leaf short of full to several keys past it, plus
/// none, one, two and three rows. With a 20-character key a 4 KiB leaf holds
/// about 140 cells, so one of these counts ends exactly on the key that did
/// not fit the first leaf.
fn leaf_boundaries() -> Scenario {
    let counts: Vec<i64> = [0, 1, 2, 3].into_iter().chain(120..=165).collect();
    let mut statements = vec![
        "CREATE TABLE src (id INTEGER PRIMARY KEY, k TEXT)".to_owned(),
        "BEGIN".to_owned(),
    ];
    statements.extend(inserts(
        "src",
        (1..=200_i64).map(|id| format!("({id}, '{:020}')", (id * 7919) % 211)),
    ));
    let mut checks = Vec::new();
    let mut indexes = Vec::new();
    for count in &counts {
        statements.push(format!("CREATE TABLE b{count} (id INTEGER PRIMARY KEY, k TEXT)"));
        statements.push(format!(
            "INSERT INTO b{count} SELECT id, k FROM src WHERE id <= {count}"
        ));
    }
    statements.push("COMMIT".to_owned());
    for count in &counts {
        statements.push(format!("CREATE INDEX ix_b{count} ON b{count}(k)"));
        checks.push(format!(
            "SELECT count(*), min(k), max(k) FROM b{count} INDEXED BY ix_b{count}"
        ));
        if *count > 0 {
            checks.push(format!(
                "SELECT k, id FROM b{count} INDEXED BY ix_b{count} ORDER BY k"
            ));
            indexes.push(format!("ix_b{count}"));
        }
    }
    Scenario {
        label: "row counts around a leaf boundary",
        statements,
        checks,
        indexes,
    }
}

/// REINDEX rebuilds through the same program, and rows written afterwards
/// split the full pages the build left.
fn reindex_then_write() -> Scenario {
    let mut statements = scattered_table("t");
    statements.extend(owned(&[
        "CREATE INDEX ix_d ON t(d)",
        "CREATE INDEX ix_s_nocase ON t(s COLLATE NOCASE)",
        "CREATE UNIQUE INDEX ux_r ON t(r)",
        "REINDEX ix_d",
        "REINDEX t",
        "REINDEX",
        "BEGIN",
    ]));
    statements.extend(inserts(
        "t",
        (20_001..=24_000_i64).map(|id| {
            format!(
                "({id}, {}, {}, 'late{:05}', {})",
                id + 100_000,
                (id * 31) % 97,
                (id * 7919) % 4_000,
                id % 50
            )
        }),
    ));
    statements.extend(owned(&[
        "UPDATE t SET d = d + 200 WHERE id % 11 = 0",
        "DELETE FROM t WHERE id % 13 = 0",
        "COMMIT",
        "REINDEX ix_s_nocase",
    ]));
    Scenario {
        label: "REINDEX, then writes into the built pages",
        statements,
        checks: owned(&[
            "SELECT d, id FROM t INDEXED BY ix_d ORDER BY d, id",
            "SELECT s, id FROM t INDEXED BY ix_s_nocase WHERE s COLLATE NOCASE < 'key00300' \
             ORDER BY s COLLATE NOCASE, id",
            "SELECT r, id FROM t INDEXED BY ux_r WHERE r > 15000 ORDER BY r",
            "SELECT count(*), sum(d), sum(r) FROM t",
        ]),
        indexes: owned(&["ix_d", "ix_s_nocase", "ux_r"]),
    }
}

/// A build undone by ROLLBACK or ROLLBACK TO leaves no index and no stray
/// page behind; builds before and after it stand.
fn rolled_back_builds() -> Scenario {
    let mut statements = scattered_table("t");
    statements.extend(owned(&[
        "BEGIN",
        "CREATE INDEX ix_r ON t(r)",
        "ROLLBACK",
        "CREATE INDEX ix_d ON t(d)",
        "BEGIN",
        "SAVEPOINT s",
        "CREATE INDEX ix_r ON t(r)",
        "CREATE UNIQUE INDEX ux_r ON t(r)",
        "ROLLBACK TO s",
        "CREATE INDEX ix_n ON t(n)",
        "RELEASE s",
        "COMMIT",
        "CREATE INDEX ix_s ON t(s)",
    ]));
    Scenario {
        label: "builds undone by ROLLBACK and ROLLBACK TO",
        statements,
        checks: owned(&[
            "SELECT type, name, tbl_name FROM sqlite_master ORDER BY name",
            "SELECT d, id FROM t INDEXED BY ix_d ORDER BY d, id",
            "SELECT n, id FROM t INDEXED BY ix_n WHERE n IS NULL OR n < 40 ORDER BY n, id",
            "SELECT s, id FROM t INDEXED BY ix_s WHERE s < 'Key00200' ORDER BY s, id",
        ]),
        indexes: owned(&["ix_d", "ix_n", "ix_s"]),
    }
}

/// Indexes on generated columns: a VIRTUAL column is not in the stored row, so
/// the build has to compute its key, and a STORED one is read from the row.
/// The indexes are built after the rows, rebuilt by REINDEX, and written to
/// afterwards.
fn generated_columns() -> Scenario {
    let mut statements = owned(&[
        "CREATE TABLE g (id INTEGER PRIMARY KEY, a INTEGER, \
         v INTEGER GENERATED ALWAYS AS (a * 7 % 101) VIRTUAL, b TEXT, \
         s TEXT GENERATED ALWAYS AS (b || '-' || a) STORED, c INTEGER)",
        "BEGIN",
    ]);
    statements.extend(inserts(
        "g(id, a, b, c)",
        (1..=8_000_i64).map(|id| {
            format!(
                "({id}, {}, 'b{:03}', {})",
                (id * 7919) % 4_003,
                (id * 31) % 211,
                (id * 13) % 37
            )
        }),
    ));
    statements.extend(owned(&[
        "COMMIT",
        "CREATE INDEX ix_g_v ON g(v)",
        "CREATE INDEX ix_g_s ON g(s)",
        "CREATE INDEX ix_g_cv ON g(c, v DESC)",
        "CREATE UNIQUE INDEX ux_g_vid ON g(v, id)",
        "REINDEX g",
        "UPDATE g SET a = a + 1 WHERE id % 9 = 0",
        "DELETE FROM g WHERE id % 17 = 0",
    ]));
    Scenario {
        label: "indexes on VIRTUAL and STORED generated columns",
        statements,
        checks: owned(&[
            "SELECT v, id FROM g INDEXED BY ix_g_v WHERE v < 30 ORDER BY v, id",
            "SELECT s, id FROM g INDEXED BY ix_g_s WHERE s < 'b020' ORDER BY s, id",
            "SELECT c, v, id FROM g INDEXED BY ix_g_cv WHERE c = 5 ORDER BY c, v DESC, id",
            "SELECT v, id FROM g INDEXED BY ux_g_vid WHERE v BETWEEN 40 AND 60 ORDER BY v, id",
            "SELECT count(*), sum(v), sum(length(s)) FROM g",
        ]),
        indexes: owned(&["ix_g_v", "ix_g_s", "ix_g_cv", "ux_g_vid"]),
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
    for index in &scenario.indexes {
        let (frank_pages, frank_min_cells) = index_pages(&reread, index);
        let (stock_pages, _) = index_pages(&stock, index);
        // One page of slack so a one-page index may take two.
        if frank_pages * 10 > stock_pages * 13 + 10 || frank_min_cells < 1 {
            failures.push(format!(
                "{label}: index {index} takes {frank_pages} pages (stock {stock_pages}), \
                 fewest leaf cells {frank_min_cells}"
            ));
        }
    }
    eprintln!(
        "{label}: {} statements, {} checks, {} failures",
        scenario.statements.len(),
        scenario.checks.len(),
        failures.len()
    );
    failures
}

#[test]
fn sorted_bulk_index_builds_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for scenario in [
            scattered_keys(),
            storage_classes(),
            overflow_keys(),
            without_rowid(),
            leaf_boundaries(),
            reindex_then_write(),
            rolled_back_builds(),
            generated_columns(),
        ] {
            failures.extend(run(scenario).await);
        }
        assert!(failures.is_empty(), "{failures:#?}");
    });
}

/// The names in `sqlite_master`, to show a failed build left nothing behind.
const SCHEMA_NAMES: &str = "SELECT type, name FROM sqlite_master ORDER BY name";

/// A UNIQUE build over rows that share a key must fail on both engines with
/// the same message and leave the schema as it was; a build both engines
/// accept must succeed on both. The duplicates sit thousands of rows apart, so
/// they meet only once the keys are sorted.
#[test]
fn unique_index_builds_fail_and_succeed_as_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let frank_path = dir.path().join("frank.db");
        let frank = Connection::open(frank_path.to_str().expect("utf-8 path"))
            .await
            .expect("fsqlite open");
        let stock = rusqlite::Connection::open(dir.path().join("stock.db")).expect("stock open");

        let mut setup = owned(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER, s TEXT, v, n INTEGER)",
            "CREATE TABLE w (k INTEGER PRIMARY KEY, v INTEGER) WITHOUT ROWID",
            "BEGIN",
        ]);
        setup.extend(inserts(
            "t",
            (1..=6_000_i64).map(|id| {
                // `u` repeats once, at ids 17 and 5417. `s` differs only in
                // case at ids 40 and 4040. `v` is 5 at id 100 and 5.0 at 5100.
                // `n` is NULL in every third row and distinct elsewhere.
                let u = if id == 5_417 { (17 * 7919) % 6_007 } else { (id * 7919) % 6_007 };
                let s = match id {
                    40 => "'Duplicate'".to_owned(),
                    4_040 => "'dUPLICATE'".to_owned(),
                    _ => format!("'s{:05}'", (id * 31) % 6_007),
                };
                let v = match id {
                    100 => "5".to_owned(),
                    5_100 => "5.0".to_owned(),
                    _ => format!("{}", 10 + (id * 7) % 6_007),
                };
                let n = if id % 3 == 0 {
                    "NULL".to_owned()
                } else {
                    ((id * 7919) % 6_007).to_string()
                };
                format!("({id}, {u}, {s}, {v}, {n})")
            }),
        ));
        setup.extend(inserts(
            "w",
            (1..=3_000_i64).map(|k| format!("({k}, {})", if k == 2_900 { 44 } else { k })),
        ));
        setup.push("COMMIT".to_owned());
        for sql in &setup {
            stock
                .execute_batch(sql)
                .unwrap_or_else(|error| panic!("stock `{sql:.120}`: {error}"));
            frank
                .execute(sql)
                .await
                .unwrap_or_else(|error| panic!("fsqlite `{sql:.120}`: {error}"));
        }

        let mut failures = Vec::new();
        // The last field says whether fsqlite's message must be stock's. For
        // an index on an expression stock names the index (`index 'ux_expr'`)
        // and fsqlite names the expression (`t.u % 2`), on every path that
        // reports the violation; only the failure itself is compared there.
        for (sql, must_fail, same_message) in [
            ("CREATE UNIQUE INDEX ux_u ON t(u)", true, true),
            ("CREATE UNIQUE INDEX ux_s ON t(s COLLATE NOCASE)", true, true),
            ("CREATE UNIQUE INDEX ux_s_binary ON t(s)", false, true),
            ("CREATE UNIQUE INDEX ux_v ON t(v)", true, true),
            ("CREATE UNIQUE INDEX ux_n ON t(n)", false, true),
            ("CREATE UNIQUE INDEX ux_expr ON t(u % 2)", true, false),
            ("CREATE UNIQUE INDEX ux_us ON t(u, s)", false, true),
            ("CREATE UNIQUE INDEX ux_w_v ON w(v)", true, true),
            ("CREATE UNIQUE INDEX ux_w_vk ON w(v, k)", false, true),
            // The name a failed build asked for is free again.
            ("CREATE INDEX ux_u ON t(u)", false, true),
        ] {
            let stock_result = stock.execute_batch(sql);
            let frank_result = frank.execute(sql).await;
            assert_eq!(
                stock_result.is_err(),
                must_fail,
                "stock `{sql}`: {stock_result:?}"
            );
            match (&stock_result, &frank_result) {
                (Ok(()), Ok(_)) => {}
                (Err(stock_error), Err(frank_error)) => {
                    let stock_message = stock_error.to_string();
                    let frank_message = frank_error.to_string();
                    let wanted = if same_message {
                        stock_message.as_str()
                    } else {
                        "UNIQUE constraint failed: "
                    };
                    if !stock_message.starts_with("UNIQUE constraint failed: ")
                        || !frank_message.contains(wanted)
                    {
                        failures.push(format!(
                            "`{sql}`: stock says `{stock_message}`, fsqlite says `{frank_message}`"
                        ));
                    }
                }
                (stock_result, frank_result) => failures.push(format!(
                    "`{sql}`: stock {stock_result:?}, fsqlite {:?}",
                    frank_result.as_ref().map(|_| ())
                )),
            }
            let stock_schema = stock_rows(&stock, SCHEMA_NAMES);
            let frank_schema = frank_rows(&frank, SCHEMA_NAMES).await;
            if stock_schema != frank_schema {
                failures.push(format!(
                    "after `{sql}`: stock schema {stock_schema:?}, fsqlite schema {frank_schema:?}"
                ));
            }
        }

        for sql in [
            "SELECT s, id FROM t INDEXED BY ux_s_binary WHERE s < 's00100' ORDER BY s",
            "SELECT n, id FROM t INDEXED BY ux_n WHERE n IS NULL OR n < 50 ORDER BY n, id",
            "SELECT u, s, id FROM t INDEXED BY ux_us WHERE u < 200 ORDER BY u, s",
            "SELECT v, k FROM w INDEXED BY ux_w_vk WHERE v < 60 ORDER BY v, k",
            "SELECT u, id FROM t INDEXED BY ux_u WHERE u < 200 ORDER BY u, id",
        ] {
            let expected = stock_rows(&stock, sql);
            assert!(!expected.is_empty(), "`{sql}` must select rows");
            let actual = frank_rows(&frank, sql).await;
            if actual != expected {
                failures.push(format!(
                    "`{sql}` fsqlite {} rows, stock {} rows",
                    actual.len(),
                    expected.len()
                ));
            }
        }
        frank.close().await.expect("fsqlite close");
        let reread = rusqlite::Connection::open(&frank_path).expect("stock opens the fsqlite file");
        let stock_check = stock_rows(&reread, "PRAGMA integrity_check");
        if stock_check != vec![vec!["'ok'".to_owned()]] {
            failures.push(format!(
                "stock integrity_check of the fsqlite file {stock_check:?}"
            ));
        }
        assert!(failures.is_empty(), "{failures:#?}");
    });
}
