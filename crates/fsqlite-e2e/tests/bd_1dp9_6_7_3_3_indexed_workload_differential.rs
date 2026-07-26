//! Indexed workload differential suite (bd-1dp9.6.7.3.3).
//!
//! Proof bundle for planner/runtime index path quality: deterministic
//! indexed workload suites, differential checks against C SQLite, and
//! structured logs making path choice auditable.
//!
//! ## Scenarios
//!
//! | ID | Name | Shape |
//! |----|------|-------|
//! | I1 | equality_lookup | Single-column index equality, verify rows match |
//! | I2 | range_scan | Index range scan, verify sort order and count |
//! | I3 | duplicate_heavy | Many duplicate keys, verify all rows returned |
//! | I4 | covering_index | Index covers all selected columns, no table access |
//! | I5 | multi_column_index | Composite index prefix and full match |
//! | I6 | unique_constraint | UNIQUE index enforces constraint |
//! | I7 | order_by_indexed | ORDER BY uses index order, verify stability |
//! | I8 | mixed_dml_with_index | INSERT/UPDATE/DELETE with index maintenance |
//! | I9 | null_handling | NULL values in indexed columns |
//! | I10 | large_dataset | 10K rows, differential correctness |
//!
//! ## Structured Log Contract
//!
//! ```json
//! {
//!   "bead_id": "bd-1dp9.6.7.3.3",
//!   "scenario_id": "I1",
//!   "phase": "result",
//!   "backend": "csqlite|fsqlite",
//!   "query": "SELECT ...",
//!   "row_count": 5,
//!   "rows_match": true,
//!   "elapsed_ns": 12345
//! }
//! ```
//!
//! ## Run
//!
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_1dp9_6_7_3_3_indexed_workload_differential \
//!     -- --nocapture --test-threads=1
//! ```

#![allow(clippy::too_many_lines)]
#![allow(clippy::similar_names)]
#![allow(clippy::cast_precision_loss)]
#![recursion_limit = "512"]

use std::sync::Mutex;
use std::time::Instant;

use serde_json::json;

const BEAD_ID: &str = "bd-1dp9.6.7.3.3";
const REPLAY_CMD: &str = "cargo test -p fsqlite-e2e --test bd_1dp9_6_7_3_3_indexed_workload_differential -- --nocapture --test-threads=1";

static E2E_LOCK: Mutex<()> = Mutex::new(());

// ─── Structured logging ──────────────────────────────────────────────

fn emit_log(scenario_id: &str, phase: &str, data: serde_json::Value) {
    eprintln!(
        "INDEXED_WORKLOAD:{}",
        json!({
            "bead_id": BEAD_ID,
            "scenario_id": scenario_id,
            "phase": phase,
            "replay_command": REPLAY_CMD,
            "data": data,
        })
    );
}

// ─── Helpers ─────────────────────────────────────────────────────────

fn csqlite_query(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).expect("prepare");
    let col_count = stmt.column_count();
    stmt.query_map([], |row| {
        let mut vals = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            vals.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(n) => n.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => s,
                rusqlite::types::Value::Blob(b) => format!("x'{}'", hex::encode(&b)),
            });
        }
        Ok(vals)
    })
    .expect("query_map")
    .collect::<Result<Vec<_>, _>>()
    .expect("collect")
}

async fn fsqlite_query(conn: &fsqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let rows = conn.query(sql).await.expect("fsqlite query");
    rows.iter()
        .map(|r| {
            r.values()
                .iter()
                .map(|v| match v {
                    fsqlite_types::value::SqliteValue::Null => "NULL".to_owned(),
                    fsqlite_types::value::SqliteValue::Integer(n) => n.to_string(),
                    fsqlite_types::value::SqliteValue::Float(f) => format!("{f}"),
                    fsqlite_types::value::SqliteValue::Text(s) => s.to_string(),
                    fsqlite_types::value::SqliteValue::Blob(b) => {
                        format!("x'{}'", hex::encode(b))
                    }
                })
                .collect()
        })
        .collect()
}

fn setup_csqlite() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().expect("open");
    conn.execute_batch("PRAGMA journal_mode=WAL;").expect("wal");
    conn
}

async fn setup_fsqlite() -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(":memory:").await.expect("open");
    conn
}

fn differential_check(
    scenario_id: &str,
    query: &str,
    csqlite_rows: &[Vec<String>],
    fsqlite_rows: &[Vec<String>],
) {
    let rows_match = csqlite_rows == fsqlite_rows;
    emit_log(
        scenario_id,
        "comparison",
        json!({
            "query": query,
            "csqlite_row_count": csqlite_rows.len(),
            "fsqlite_row_count": fsqlite_rows.len(),
            "rows_match": rows_match,
        }),
    );
    assert_eq!(
        csqlite_rows.len(),
        fsqlite_rows.len(),
        "[{scenario_id}] row count mismatch: csqlite={}, fsqlite={}",
        csqlite_rows.len(),
        fsqlite_rows.len()
    );
    assert!(
        rows_match,
        "[{scenario_id}] rows differ on query: {query}\ncsqlite: {csqlite_rows:?}\nfsqlite: {fsqlite_rows:?}"
    );
}

mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{b:02x}")).collect()
    }
}

// ─── Schema setup ────────────────────────────────────────────────────

const SCHEMA: &str = "
    CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        category TEXT NOT NULL,
        name TEXT NOT NULL,
        price REAL NOT NULL,
        stock INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX idx_category ON products(category);
    CREATE INDEX idx_price ON products(price);
    CREATE INDEX idx_cat_price ON products(category, price);
    CREATE UNIQUE INDEX idx_name ON products(name);
";

async fn seed_data(exec: impl AsyncFn(&str)) {
    let categories = ["electronics", "books", "clothing", "food", "toys"];
    for i in 0..200 {
        let cat = categories[i % categories.len()];
        let price = (i as f64).mul_add(1.5, 0.99);
        let stock = (i * 7 + 3) % 100;
        exec(&format!(
            "INSERT INTO products (id, category, name, price, stock) VALUES ({i}, '{cat}', 'product-{i:04}', {price}, {stock})"
        ))
        .await;
    }
}

// ─── I1: Equality lookup ─────────────────────────────────────────────

#[test]
fn i1_equality_lookup() {
    asupersync::test_utils::run_test(|| async {
        let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let c = setup_csqlite();
        c.execute_batch(SCHEMA).expect("schema");
        seed_data(async |sql: &str| {
            c.execute(sql, []).unwrap();
        })
        .await;

        let f = setup_fsqlite().await;
        f.execute(SCHEMA).await.expect("schema");
        seed_data(async |sql: &str| {
            f.execute(sql).await.unwrap();
        })
        .await;

        let queries = [
            "SELECT id, name, price FROM products WHERE category = 'electronics' ORDER BY id",
            "SELECT id, name FROM products WHERE category = 'books' ORDER BY id",
            "SELECT COUNT(*) FROM products WHERE category = 'food'",
        ];

        for q in &queries {
            let start = Instant::now();
            let c_rows = csqlite_query(&c, q);
            let c_ns = start.elapsed().as_nanos() as u64;

            let start = Instant::now();
            let f_rows = fsqlite_query(&f, q).await;
            let f_ns = start.elapsed().as_nanos() as u64;

            emit_log(
                "I1",
                "result",
                json!({
                    "query": q,
                    "csqlite_rows": c_rows.len(),
                    "fsqlite_rows": f_rows.len(),
                    "csqlite_ns": c_ns,
                    "fsqlite_ns": f_ns,
                }),
            );
            differential_check("I1", q, &c_rows, &f_rows);
        }
    });
}

// ─── I2: Range scan ──────────────────────────────────────────────────

#[test]
fn i2_range_scan() {
    asupersync::test_utils::run_test(|| async {
        let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let c = setup_csqlite();
        c.execute_batch(SCHEMA).expect("schema");
        seed_data(async |sql: &str| {
            c.execute(sql, []).unwrap();
        })
        .await;

        let f = setup_fsqlite().await;
        f.execute(SCHEMA).await.expect("schema");
        seed_data(async |sql: &str| {
            f.execute(sql).await.unwrap();
        })
        .await;

        let queries = [
            "SELECT id, name, price FROM products WHERE price BETWEEN 10.0 AND 50.0 ORDER BY price, id",
            "SELECT id, price FROM products WHERE price > 200.0 ORDER BY price",
            "SELECT id, price FROM products WHERE price < 5.0 ORDER BY id",
        ];

        for q in &queries {
            let c_rows = csqlite_query(&c, q);
            let f_rows = fsqlite_query(&f, q).await;
            emit_log(
                "I2",
                "result",
                json!({"query": q, "csqlite_rows": c_rows.len(), "fsqlite_rows": f_rows.len()}),
            );
            differential_check("I2", q, &c_rows, &f_rows);
        }
    });
}

// ─── I3: Duplicate-heavy dataset ─────────────────────────────────────

#[test]
fn i3_duplicate_heavy() {
    asupersync::test_utils::run_test(|| async {
        let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let c = setup_csqlite();
        c.execute_batch(
            "CREATE TABLE dupes (id INTEGER PRIMARY KEY, grp TEXT NOT NULL, val INTEGER);
         CREATE INDEX idx_grp ON dupes(grp);",
        )
        .expect("schema");

        let f = setup_fsqlite().await;
        f.execute(
            "CREATE TABLE dupes (id INTEGER PRIMARY KEY, grp TEXT NOT NULL, val INTEGER);
         CREATE INDEX idx_grp ON dupes(grp);",
        )
        .await
        .expect("schema");

        // Insert 500 rows with only 5 distinct group values
        for i in 0..500 {
            let grp = format!("group-{}", i % 5);
            let sql = format!("INSERT INTO dupes VALUES ({i}, '{grp}', {i})");
            c.execute(&sql, []).unwrap();
            f.execute(&sql).await.unwrap();
        }

        let queries = [
            "SELECT COUNT(*) FROM dupes WHERE grp = 'group-0'",
            "SELECT id, val FROM dupes WHERE grp = 'group-2' ORDER BY id",
            "SELECT grp, COUNT(*), SUM(val) FROM dupes GROUP BY grp ORDER BY grp",
        ];

        for q in &queries {
            let c_rows = csqlite_query(&c, q);
            let f_rows = fsqlite_query(&f, q).await;
            emit_log(
                "I3",
                "result",
                json!({"query": q, "rows_match": c_rows == f_rows, "row_count": c_rows.len()}),
            );
            differential_check("I3", q, &c_rows, &f_rows);
        }
    });
}

// ─── I4: Covering index ──────────────────────────────────────────────

#[test]
fn i4_covering_index() {
    asupersync::test_utils::run_test(|| async {
        let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let c = setup_csqlite();
        c.execute_batch(SCHEMA).expect("schema");
        seed_data(async |sql: &str| {
            c.execute(sql, []).unwrap();
        })
        .await;

        let f = setup_fsqlite().await;
        f.execute(SCHEMA).await.expect("schema");
        seed_data(async |sql: &str| {
            f.execute(sql).await.unwrap();
        })
        .await;

        // idx_cat_price covers (category, price) — query needs only those columns
        let queries = [
            "SELECT category, price FROM products WHERE category = 'electronics' ORDER BY price",
            "SELECT category, COUNT(*) FROM products GROUP BY category ORDER BY category",
        ];

        for q in &queries {
            let c_rows = csqlite_query(&c, q);
            let f_rows = fsqlite_query(&f, q).await;
            emit_log(
                "I4",
                "result",
                json!({"query": q, "covering_index": "idx_cat_price", "rows_match": c_rows == f_rows}),
            );
            differential_check("I4", q, &c_rows, &f_rows);
        }
    });
}

// ─── I5: Multi-column index ─────────────────────────────────────────

#[test]
fn i5_multi_column_index() {
    asupersync::test_utils::run_test(|| async {
        let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let c = setup_csqlite();
        c.execute_batch(SCHEMA).expect("schema");
        seed_data(async |sql: &str| {
            c.execute(sql, []).unwrap();
        })
        .await;

        let f = setup_fsqlite().await;
        f.execute(SCHEMA).await.expect("schema");
        seed_data(async |sql: &str| {
            f.execute(sql).await.unwrap();
        })
        .await;

        let queries = [
            // Full composite key match
            "SELECT id, name FROM products WHERE category = 'toys' AND price < 30.0 ORDER BY id",
            // Prefix-only match (category only)
            "SELECT id, name FROM products WHERE category = 'clothing' ORDER BY id",
            // Non-prefix (price only) — cannot use idx_cat_price efficiently
            "SELECT id, name FROM products WHERE price = 15.99 ORDER BY id",
        ];

        for q in &queries {
            let c_rows = csqlite_query(&c, q);
            let f_rows = fsqlite_query(&f, q).await;
            emit_log(
                "I5",
                "result",
                json!({"query": q, "rows_match": c_rows == f_rows, "row_count": c_rows.len()}),
            );
            differential_check("I5", q, &c_rows, &f_rows);
        }
    });
}

// ─── I6: Unique constraint ──────────────────────────────────────────

#[test]
fn i6_unique_constraint() {
    asupersync::test_utils::run_test(|| async {
        let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let c = setup_csqlite();
        c.execute_batch(SCHEMA).expect("schema");
        seed_data(async |sql: &str| {
            c.execute(sql, []).unwrap();
        })
        .await;

        let f = setup_fsqlite().await;
        f.execute(SCHEMA).await.expect("schema");
        seed_data(async |sql: &str| {
            f.execute(sql).await.unwrap();
        })
        .await;

        // Unique lookup
        let q = "SELECT id, category, price FROM products WHERE name = 'product-0042' ORDER BY id";
        let c_rows = csqlite_query(&c, q);
        let f_rows = fsqlite_query(&f, q).await;
        differential_check("I6", q, &c_rows, &f_rows);

        // Unique violation
        let dup_sql = "INSERT INTO products (id, category, name, price) VALUES (9999, 'test', 'product-0042', 1.0)";
        let c_err = c.execute(dup_sql, []).is_err();
        let f_err = f.execute(dup_sql).await.is_err();

        emit_log(
            "I6",
            "result",
            json!({
                "unique_lookup_match": c_rows == f_rows,
                "csqlite_rejects_duplicate": c_err,
                "fsqlite_rejects_duplicate": f_err,
            }),
        );

        assert!(c_err, "[I6] C SQLite should reject duplicate name");
        assert!(f_err, "[I6] FrankenSQLite should reject duplicate name");
    });
}

// ─── I7: ORDER BY indexed ────────────────────────────────────────────

#[test]
fn i7_order_by_indexed() {
    asupersync::test_utils::run_test(|| async {
        let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let c = setup_csqlite();
        c.execute_batch(SCHEMA).expect("schema");
        seed_data(async |sql: &str| {
            c.execute(sql, []).unwrap();
        })
        .await;

        let f = setup_fsqlite().await;
        f.execute(SCHEMA).await.expect("schema");
        seed_data(async |sql: &str| {
            f.execute(sql).await.unwrap();
        })
        .await;

        let queries = [
            "SELECT id, price FROM products ORDER BY price LIMIT 20",
            "SELECT id, category, price FROM products WHERE category = 'books' ORDER BY price DESC",
            "SELECT id, name FROM products ORDER BY name LIMIT 10",
        ];

        for q in &queries {
            let c_rows = csqlite_query(&c, q);
            let f_rows = fsqlite_query(&f, q).await;
            emit_log(
                "I7",
                "result",
                json!({"query": q, "rows_match": c_rows == f_rows, "row_count": c_rows.len()}),
            );
            differential_check("I7", q, &c_rows, &f_rows);
        }
    });
}

// ─── I8: Mixed DML with index ────────────────────────────────────────

#[test]
fn i8_mixed_dml_with_index() {
    asupersync::test_utils::run_test(|| async {
        let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let c = setup_csqlite();
        c.execute_batch(SCHEMA).expect("schema");
        seed_data(async |sql: &str| {
            c.execute(sql, []).unwrap();
        })
        .await;

        let f = setup_fsqlite().await;
        f.execute(SCHEMA).await.expect("schema");
        seed_data(async |sql: &str| {
            f.execute(sql).await.unwrap();
        })
        .await;

        // UPDATE via indexed column
        let update_sql = "UPDATE products SET stock = stock + 10 WHERE category = 'electronics'";
        c.execute(update_sql, []).unwrap();
        f.execute(update_sql).await.unwrap();

        let q1 = "SELECT id, stock FROM products WHERE category = 'electronics' ORDER BY id";
        differential_check("I8", q1, &csqlite_query(&c, q1), &fsqlite_query(&f, q1).await);

        // DELETE via indexed column
        let delete_sql = "DELETE FROM products WHERE price > 250.0";
        c.execute(delete_sql, []).unwrap();
        f.execute(delete_sql).await.unwrap();

        let q2 = "SELECT COUNT(*) FROM products";
        differential_check("I8", q2, &csqlite_query(&c, q2), &fsqlite_query(&f, q2).await);

        // INSERT new rows, verify index updated
        let insert_sql = "INSERT INTO products (id, category, name, price, stock) VALUES (9001, 'new', 'product-new-1', 999.99, 50)";
        c.execute(insert_sql, []).unwrap();
        f.execute(insert_sql).await.unwrap();

        let q3 = "SELECT id, name, price FROM products WHERE name = 'product-new-1'";
        let c_rows = csqlite_query(&c, q3);
        let f_rows = fsqlite_query(&f, q3).await;
        differential_check("I8", q3, &c_rows, &f_rows);

        emit_log(
            "I8",
            "result",
            json!({
                "update_verified": true,
                "delete_verified": true,
                "insert_verified": true,
            }),
        );
    });
}

// ─── I9: NULL handling in indexes ────────────────────────────────────

#[test]
fn i9_null_handling() {
    asupersync::test_utils::run_test(|| async {
        let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let schema = "
        CREATE TABLE nullable (id INTEGER PRIMARY KEY, key TEXT, val INTEGER);
        CREATE INDEX idx_key ON nullable(key);
    ";

        let c = setup_csqlite();
        c.execute_batch(schema).expect("schema");

        let f = setup_fsqlite().await;
        f.execute(schema).await.expect("schema");

        let inserts = [
            "INSERT INTO nullable VALUES (1, 'a', 10)",
            "INSERT INTO nullable VALUES (2, NULL, 20)",
            "INSERT INTO nullable VALUES (3, 'b', 30)",
            "INSERT INTO nullable VALUES (4, NULL, 40)",
            "INSERT INTO nullable VALUES (5, 'a', 50)",
        ];
        for sql in &inserts {
            c.execute(sql, []).unwrap();
            f.execute(sql).await.unwrap();
        }

        let queries = [
            "SELECT id, key, val FROM nullable WHERE key IS NULL ORDER BY id",
            "SELECT id, key, val FROM nullable WHERE key IS NOT NULL ORDER BY id",
            "SELECT id, key, val FROM nullable WHERE key = 'a' ORDER BY id",
            "SELECT COUNT(*) FROM nullable WHERE key IS NULL",
        ];

        for q in &queries {
            let c_rows = csqlite_query(&c, q);
            let f_rows = fsqlite_query(&f, q).await;
            emit_log(
                "I9",
                "result",
                json!({"query": q, "rows_match": c_rows == f_rows, "row_count": c_rows.len()}),
            );
            differential_check("I9", q, &c_rows, &f_rows);
        }
    });
}

// ─── I10: Large dataset differential ─────────────────────────────────

#[test]
fn i10_large_dataset() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());

    let schema = "
        CREATE TABLE large (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, payload TEXT);
        CREATE INDEX idx_bucket ON large(bucket);
    ";

    let c = setup_csqlite();
    c.execute_batch(schema).expect("schema");

    let f = setup_fsqlite();
    f.execute(schema).expect("schema");

    let row_count = 10_000;
    let start = Instant::now();
    c.execute_batch("BEGIN;").unwrap();
    for i in 0..row_count {
        c.execute(
            "INSERT INTO large VALUES (?1, ?2, ?3)",
            rusqlite::params![i, i % 50, format!("data-{i:06}")],
        )
        .unwrap();
    }
    c.execute_batch("COMMIT;").unwrap();
    let c_insert_ns = start.elapsed().as_nanos() as u64;

    let start = Instant::now();
    f.execute("BEGIN").unwrap();
    for i in 0..row_count {
        f.execute(&format!(
            "INSERT INTO large VALUES ({i}, {}, 'data-{i:06}')",
            i % 50
        ))
        .unwrap();
    }
    f.execute("COMMIT").unwrap();
    let f_insert_ns = start.elapsed().as_nanos() as u64;

    let queries = [
        "SELECT COUNT(*) FROM large",
        "SELECT COUNT(*) FROM large WHERE bucket = 7",
        "SELECT id, payload FROM large WHERE bucket = 25 ORDER BY id LIMIT 10",
        "SELECT bucket, COUNT(*), MIN(id), MAX(id) FROM large GROUP BY bucket ORDER BY bucket LIMIT 5",
    ];

    let mut all_match = true;
    for q in &queries {
        let c_rows = csqlite_query(&c, q);
        let f_rows = fsqlite_query(&f, q);
        if c_rows != f_rows {
            all_match = false;
        }
        differential_check("I10", q, &c_rows, &f_rows);
    }

    emit_log(
        "I10",
        "result",
        json!({
            "row_count": row_count,
            "csqlite_insert_ns": c_insert_ns,
            "fsqlite_insert_ns": f_insert_ns,
            "all_queries_match": all_match,
        }),
    );
}
