//! Randomized b-tree write stress across page sizes and reserved bytes, with
//! stock SQLite (rusqlite) as the oracle.
//!
//! Review probe for the GH#441 overflow-append fit gate (157826d97), the
//! GH#426 usable-size cell bounds (540740799) and the per-page cell-slot cache
//! reuse (c23ca7fb4). The same deterministic op stream runs against an
//! fsqlite file and a stock file that start from the same empty image:
//! rowid appends (rightmost-leaf hints), random-rowid inserts, size-changing
//! updates that cross the local/overflow boundary, deletes, an index whose
//! keys overflow, and a final VACUUM. Contents must match, stock
//! `integrity_check` must accept the fsqlite file, and after VACUUM the
//! fsqlite file must have no free pages.

#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn stock_rows(path: &std::path::Path, sql: &str) -> Vec<Vec<String>> {
    let conn = rusqlite::Connection::open(path).expect("stock open");
    let mut stmt = conn.prepare(sql).expect("stock prepare");
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width)
            .map(|i| {
                let value: rusqlite::types::Value = row.get(i)?;
                Ok(match value {
                    rusqlite::types::Value::Null => "NULL".to_owned(),
                    rusqlite::types::Value::Integer(v) => v.to_string(),
                    rusqlite::types::Value::Real(v) => v.to_string(),
                    rusqlite::types::Value::Text(v) => v,
                    rusqlite::types::Value::Blob(v) => format!("{v:?}"),
                })
            })
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .expect("stock query")
    .map(|row| row.expect("stock row"))
    .collect()
}

fn render(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(v) => v.to_string(),
        SqliteValue::Float(v) => v.to_string(),
        SqliteValue::Text(v) => v.as_ref().to_owned(),
        SqliteValue::Blob(v) => format!("{:?}", v.as_ref()),
    }
}

async fn fsqlite_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|err| panic!("fsqlite query {sql}: {err}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

/// An empty stock image with `page_size` and `reserved` trailer bytes, laid
/// out the way stock `zeroPage` does for a reserved-bytes database.
fn build_empty_image(path: &std::path::Path, page_size: usize, reserved: u8) {
    {
        let conn = rusqlite::Connection::open(path).expect("stock create");
        conn.execute_batch(&format!(
            "PRAGMA page_size={page_size}; PRAGMA journal_mode=DELETE; \
             CREATE TABLE seed(x); DROP TABLE seed; VACUUM;"
        ))
        .expect("stock empty image");
    }
    let mut bytes = std::fs::read(path).expect("read empty image");
    assert_eq!(bytes.len(), page_size, "empty image must be one page");
    bytes[20] = reserved;
    let usable = page_size - usize::from(reserved);
    // Content-area start of the empty page-1 b-tree; 65536 is stored as 0.
    let content_start = u16::try_from(usable).unwrap_or(0);
    bytes[105..107].copy_from_slice(&content_start.to_be_bytes());
    std::fs::write(path, &bytes).expect("write reserved image");
    // Stock writes schema format 0 for an empty schema; create the tables
    // with stock so both engines start from the same populated image.
    let conn = rusqlite::Connection::open(path).expect("stock reopen");
    conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB, k BLOB); CREATE INDEX t_k ON t(k);")
        .expect("stock schema");
}

/// Deterministic value of `len` bytes, rendered as a SQL blob literal.
fn blob_literal(seed: u64, len: usize) -> String {
    let mut rng = Rng(seed | 1);
    let mut out = String::with_capacity(3 + len * 2);
    out.push_str("x'");
    for _ in 0..len {
        out.push_str(&format!("{:02x}", rng.below(256)));
    }
    out.push('\'');
    out
}

/// Payload length mix: mostly local, regularly just past the local/overflow
/// boundary, sometimes several overflow pages.
fn payload_len(rng: &mut Rng, usable: usize) -> usize {
    match rng.below(10) {
        0..=4 => 1 + rng.below(64) as usize,
        5 | 6 => usable / 4 + rng.below(64) as usize,
        7 | 8 => usable - 40 + rng.below(80) as usize,
        _ => usable * (2 + rng.below(3) as usize) + rng.below(500) as usize,
    }
}

fn op_stream(seed: u64, usable: usize, ops: usize) -> Vec<String> {
    let mut rng = Rng(seed);
    let mut stmts = Vec::new();
    let mut next_append = 1_i64;
    let mut in_txn = false;
    for op in 0..ops {
        if !in_txn && rng.below(4) == 0 {
            stmts.push("BEGIN".to_owned());
            in_txn = true;
        }
        let vseed = rng.next();
        let vlen = payload_len(&mut rng, usable);
        let klen_cap = if rng.below(5) == 0 { usable as u64 } else { 24 };
        let klen = 1 + rng.below(klen_cap) as usize;
        match rng.below(10) {
            // Rowid-ordered appends: rightmost-leaf and hinted-leaf fast paths.
            0..=3 => {
                let burst = 1 + rng.below(12);
                for _ in 0..burst {
                    let vlen = payload_len(&mut rng, usable);
                    stmts.push(format!(
                        "INSERT INTO t(id, v, k) VALUES ({next_append}, {}, {})",
                        blob_literal(rng.next(), vlen),
                        blob_literal(rng.next(), klen)
                    ));
                    next_append += 1 + rng.below(3) as i64;
                }
            }
            // Random rowid below the frontier (general insert path).
            4 => {
                let id = 1 + rng.below(next_append.max(1) as u64 + 64) as i64;
                stmts.push(format!(
                    "INSERT OR REPLACE INTO t(id, v, k) VALUES ({id}, {}, {})",
                    blob_literal(vseed, vlen),
                    blob_literal(rng.next(), klen)
                ));
                next_append = next_append.max(id + 1);
            }
            // Size-changing updates, often across the overflow boundary.
            5 | 6 => {
                let id = 1 + rng.below(next_append.max(1) as u64) as i64;
                stmts.push(format!(
                    "UPDATE t SET v = {} WHERE id = {id}",
                    blob_literal(vseed, vlen)
                ));
            }
            7 => {
                let lo = 1 + rng.below(next_append.max(1) as u64) as i64;
                let span = rng.below(8) as i64;
                stmts.push(format!(
                    "UPDATE t SET k = {}, v = substr(v, 1, {}) WHERE id BETWEEN {lo} AND {}",
                    blob_literal(vseed, klen),
                    1 + rng.below(usable as u64 * 2),
                    lo + span
                ));
            }
            // Deletes: single rows and short ranges.
            _ => {
                let lo = 1 + rng.below(next_append.max(1) as u64) as i64;
                let span = rng.below(6) as i64;
                stmts.push(format!("DELETE FROM t WHERE id BETWEEN {lo} AND {}", lo + span));
            }
        }
        if in_txn && (rng.below(6) == 0 || op + 1 == ops) {
            stmts.push("COMMIT".to_owned());
            in_txn = false;
        }
    }
    stmts
}

const CHECK: &str = "SELECT count(*), sum(id), sum(length(v)), sum(length(k)), \
                     group_concat(id || ':' || hex(substr(v, 1, 4)) || hex(substr(v, -4)), ',') \
                     FROM (SELECT * FROM t ORDER BY id)";

async fn run_config(page_size: usize, reserved: u8, seed: u64, ops: usize) {
    let usable = page_size - usize::from(reserved);
    let label = format!("page_size={page_size} reserved={reserved} seed={seed}");
    let dir = tempfile::tempdir().expect("temp dir");
    let fpath = dir.path().join("fsqlite.db");
    let spath = dir.path().join("stock.db");
    build_empty_image(&fpath, page_size, reserved);
    std::fs::copy(&fpath, &spath).expect("copy empty image");

    let stmts = op_stream(seed, usable, ops);
    {
        let stock = rusqlite::Connection::open(&spath).expect("stock open");
        for sql in &stmts {
            stock.execute_batch(sql).unwrap_or_else(|err| panic!("{label}: stock {sql}: {err}"));
        }
    }

    let db = fpath.to_string_lossy().into_owned();
    let conn = Connection::open(&db).await.expect("fsqlite open");
    conn.execute("PRAGMA journal_mode=DELETE;").await.expect("journal_mode");
    for sql in &stmts {
        conn.execute(sql)
            .await
            .unwrap_or_else(|err| panic!("{label}: fsqlite {}: {err}", &sql[..sql.len().min(120)]));
    }
    let before_vacuum = fsqlite_rows(&conn, CHECK).await;
    assert_eq!(before_vacuum, stock_rows(&spath, CHECK), "{label}: content before VACUUM");
    let idx = "SELECT count(*), sum(length(k)) FROM t INDEXED BY t_k WHERE k IS NOT NULL";
    assert_eq!(fsqlite_rows(&conn, idx).await, stock_rows(&spath, idx), "{label}: index scan");
    conn.close().await.expect("fsqlite close");
    assert_eq!(
        stock_rows(&fpath, "PRAGMA integrity_check"),
        vec![vec!["ok".to_owned()]],
        "{label}: stock integrity_check before VACUUM"
    );

    let conn = Connection::open(&db).await.expect("fsqlite reopen");
    conn.execute("PRAGMA journal_mode=DELETE;").await.expect("journal_mode");
    conn.execute("VACUUM;").await.expect("fsqlite vacuum");
    let after_vacuum = fsqlite_rows(&conn, CHECK).await;
    let freelist = fsqlite_rows(&conn, "PRAGMA freelist_count").await;
    conn.close().await.expect("fsqlite close");
    assert_eq!(after_vacuum, before_vacuum, "{label}: VACUUM changed content");
    assert_eq!(freelist, vec![vec!["0".to_owned()]], "{label}: free pages after VACUUM");
    assert_eq!(
        stock_rows(&fpath, "PRAGMA integrity_check"),
        vec![vec!["ok".to_owned()]],
        "{label}: stock integrity_check after VACUUM"
    );
    assert_eq!(std::fs::read(&fpath).expect("reread")[20], reserved, "{label}: reserve byte");

    {
        let stock = rusqlite::Connection::open(&spath).expect("stock open");
        stock.execute_batch("VACUUM").expect("stock vacuum");
    }
    // Leaf packing differs slightly from stock's balancer, so allow a small
    // margin; a leaked overflow chain per row (GH#441) is far outside it.
    let pages = |path: &std::path::Path| -> usize {
        stock_rows(path, "PRAGMA page_count")[0][0].parse().expect("page_count")
    };
    let (fsqlite_pages, stock_pages) = (pages(&fpath), pages(&spath));
    assert!(
        fsqlite_pages <= stock_pages + stock_pages / 20 + 2,
        "{label}: page_count after VACUUM {fsqlite_pages} vs stock {stock_pages}"
    );
}

#[test]
fn btree_writes_match_stock_across_page_sizes_and_reserved_bytes() {
    asupersync::test_utils::run_test(|| async {
        let ops: usize = std::env::var("BTREE_STRESS_OPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        let seeds: u64 = std::env::var("BTREE_STRESS_SEEDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        for seed in 0..seeds {
            for (page_size, reserved) in [
                (512, 0),
                (512, 32),
                (1024, 8),
                (4096, 0),
                (4096, 32),
                (4096, 255),
                (65536, 0),
                (65536, 40),
            ] {
                run_config(page_size, reserved, 0x9E37_79B9_7F4A_7C15 ^ (seed * 7919 + 1), ops).await;
            }
        }
    });
}
