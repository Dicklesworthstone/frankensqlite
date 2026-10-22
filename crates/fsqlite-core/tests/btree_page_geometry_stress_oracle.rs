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
//!
//! GH#521 additionally measures live index geometry before any rebuild, with
//! the reported 12,000 descending integer inserts, adversarial insertion
//! orders, overflow-key churn, rollback, and abrupt transaction interruption.

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
    conn.execute_batch(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB, k BLOB); CREATE INDEX t_k ON t(k);",
    )
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
                stmts.push(format!(
                    "DELETE FROM t WHERE id BETWEEN {lo} AND {}",
                    lo + span
                ));
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
            stock
                .execute_batch(sql)
                .unwrap_or_else(|err| panic!("{label}: stock {sql}: {err}"));
        }
    }

    let db = fpath.to_string_lossy().into_owned();
    let conn = Connection::open(&db).await.expect("fsqlite open");
    conn.execute("PRAGMA journal_mode=DELETE;")
        .await
        .expect("journal_mode");
    for sql in &stmts {
        conn.execute(sql)
            .await
            .unwrap_or_else(|err| panic!("{label}: fsqlite {}: {err}", &sql[..sql.len().min(120)]));
    }
    let before_vacuum = fsqlite_rows(&conn, CHECK).await;
    assert_eq!(
        before_vacuum,
        stock_rows(&spath, CHECK),
        "{label}: content before VACUUM"
    );
    let idx = "SELECT count(*), sum(length(k)) FROM t INDEXED BY t_k WHERE k IS NOT NULL";
    assert_eq!(
        fsqlite_rows(&conn, idx).await,
        stock_rows(&spath, idx),
        "{label}: index scan"
    );
    conn.close().await.expect("fsqlite close");
    assert_eq!(
        stock_rows(&fpath, "PRAGMA integrity_check"),
        vec![vec!["ok".to_owned()]],
        "{label}: stock integrity_check before VACUUM"
    );

    let conn = Connection::open(&db).await.expect("fsqlite reopen");
    conn.execute("PRAGMA journal_mode=DELETE;")
        .await
        .expect("journal_mode");
    conn.execute("VACUUM;").await.expect("fsqlite vacuum");
    let after_vacuum = fsqlite_rows(&conn, CHECK).await;
    let freelist = fsqlite_rows(&conn, "PRAGMA freelist_count").await;
    conn.close().await.expect("fsqlite close");
    assert_eq!(
        after_vacuum, before_vacuum,
        "{label}: VACUUM changed content"
    );
    assert_eq!(
        freelist,
        vec![vec!["0".to_owned()]],
        "{label}: free pages after VACUUM"
    );
    assert_eq!(
        stock_rows(&fpath, "PRAGMA integrity_check"),
        vec![vec!["ok".to_owned()]],
        "{label}: stock integrity_check after VACUUM"
    );
    assert_eq!(
        std::fs::read(&fpath).expect("reread")[20],
        reserved,
        "{label}: reserve byte"
    );

    {
        let stock = rusqlite::Connection::open(&spath).expect("stock open");
        stock.execute_batch("VACUUM").expect("stock vacuum");
    }
    // Leaf packing differs slightly from stock's balancer, so allow a small
    // margin; a leaked overflow chain per row (GH#441) is far outside it.
    let pages = |path: &std::path::Path| -> usize {
        stock_rows(path, "PRAGMA page_count")[0][0]
            .parse()
            .expect("page_count")
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
                run_config(
                    page_size,
                    reserved,
                    0x9E37_79B9_7F4A_7C15 ^ (seed * 7919 + 1),
                    ops,
                )
                .await;
            }
        }
    });
}

mod gh521 {
    use std::collections::HashSet;
    use std::path::Path;
    use std::time::{Duration, Instant};

    use super::{Connection, Rng, SqliteValue, fsqlite_rows, stock_rows};

    const ROWS: i64 = 12_000;
    const PAGE_SIZE: usize = 4096;
    const INTEGER_SCHEMA: &str = "CREATE TABLE a(x INTEGER); CREATE INDEX ia ON a(x);";
    const CRASH_PATH: &str = "FSQLITE_GH521_CRASH_DB";
    const CRASH_READY: &str = "GH521_UNCOMMITTED_INDEX_READY";

    #[derive(Debug)]
    struct IndexGeometry {
        database_bytes: usize,
        database_pages: usize,
        interior_pages: usize,
        leaf_pages: usize,
        one_cell_leaves: usize,
        min_leaf_cells: usize,
        max_leaf_cells: usize,
        index_cells: usize,
        overflow_cells: usize,
        depth: usize,
    }

    impl IndexGeometry {
        fn measurement(&self, version: &str, elapsed: Option<Duration>) -> serde_json::Value {
            serde_json::json!({
                "version": version,
                "insert_elapsed_us": elapsed.map(|duration| duration.as_micros()),
                "database_bytes": self.database_bytes,
                "database_pages": self.database_pages,
                "interior_pages": self.interior_pages,
                "leaf_pages": self.leaf_pages,
                "one_cell_leaves": self.one_cell_leaves,
                "min_leaf_cells": self.min_leaf_cells,
                "max_leaf_cells": self.max_leaf_cells,
                "index_cells": self.index_cells,
                "overflow_cells": self.overflow_cells,
                "depth": self.depth,
            })
        }
    }

    fn read_u16(bytes: &[u8], offset: usize) -> usize {
        usize::from(u16::from_be_bytes(
            bytes[offset..offset + 2].try_into().expect("u16 bytes"),
        ))
    }

    fn read_u32(bytes: &[u8], offset: usize) -> usize {
        usize::try_from(u32::from_be_bytes(
            bytes[offset..offset + 4].try_into().expect("u32 bytes"),
        ))
        .expect("page number fits usize")
    }

    fn read_varint(bytes: &[u8], offset: usize) -> (usize, usize) {
        let mut value = 0_u64;
        for index in 0..9 {
            let byte = bytes[offset + index];
            if index == 8 {
                value = (value << 8) | u64::from(byte);
                return (usize::try_from(value).expect("payload length"), 9);
            }
            value = (value << 7) | u64::from(byte & 0x7f);
            if byte & 0x80 == 0 {
                return (usize::try_from(value).expect("payload length"), index + 1);
            }
        }
        unreachable!("SQLite varints have at most nine bytes")
    }

    /// Walk only pages reachable from this index root. Counting every 0x0a
    /// byte in the file would incorrectly include freed or overflow pages.
    /// This reader is independent of FrankenSQLite's balancing/page decoder.
    fn index_geometry(path: &Path, index_name: &str, expected_rows: usize) -> IndexGeometry {
        let stock =
            rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .expect("stock opens persisted image read-only");
        let root: i64 = stock
            .query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name=?1 AND type='index'",
                [index_name],
                |row| row.get(0),
            )
            .expect("index root");
        let root = usize::try_from(root).expect("index root page is positive");
        let integrity = stock
            .prepare("PRAGMA integrity_check")
            .expect("stock integrity prepare")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("stock integrity query")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("stock integrity rows");
        assert_eq!(integrity, ["ok"], "stock integrity of {}", path.display());
        let bytes = std::fs::read(path).expect("read persisted image");
        assert_eq!(&bytes[..16], b"SQLite format 3\0");
        assert_eq!(read_u16(&bytes, 16), PAGE_SIZE);
        assert_eq!(bytes.len() % PAGE_SIZE, 0);
        let usable = PAGE_SIZE - usize::from(bytes[20]);
        let mut stats = IndexGeometry {
            database_bytes: bytes.len(),
            database_pages: bytes.len() / PAGE_SIZE,
            interior_pages: 0,
            leaf_pages: 0,
            one_cell_leaves: 0,
            min_leaf_cells: usize::MAX,
            max_leaf_cells: 0,
            index_cells: 0,
            overflow_cells: 0,
            depth: 0,
        };
        let mut pending = vec![(root, 0)];
        let mut visited = HashSet::new();
        let mut leaf_depth = None;
        while let Some((number, depth)) = pending.pop() {
            assert!(
                number > 0 && number <= stats.database_pages,
                "child {number}"
            );
            assert!(
                visited.insert(number),
                "index child {number} has two parents or a cycle"
            );
            let page = &bytes[(number - 1) * PAGE_SIZE..number * PAGE_SIZE];
            let header = if number == 1 { 100 } else { 0 };
            let interior = match page[header] {
                0x02 => true,
                0x0a => false,
                kind => panic!("index page {number} has type {kind:#x}"),
            };
            let cells = read_u16(page, header + 3);
            assert!(
                cells > 0 || number == root,
                "empty non-root index page {number}"
            );
            stats.index_cells += cells;
            let pointer_start = header + if interior { 12 } else { 8 };
            let pointer_end = pointer_start + 2 * cells;
            let content_start = read_u16(page, header + 5);
            assert!(pointer_end <= content_start && content_start <= usable);
            let mut intervals = Vec::with_capacity(cells);
            for cell in 0..cells {
                let offset = read_u16(page, pointer_start + cell * 2);
                assert!(
                    offset >= content_start && offset < usable,
                    "cell on page {number}"
                );
                if interior {
                    pending.push((read_u32(page, offset), depth + 1));
                }
                let prefix = usize::from(interior) * 4;
                let (payload, varint_bytes) = read_varint(page, offset + prefix);
                let max_local = ((usable - 12) * 64 / 255) - 23;
                let min_local = ((usable - 12) * 32 / 255) - 23;
                let local = if payload <= max_local {
                    payload
                } else {
                    stats.overflow_cells += 1;
                    let candidate = min_local + (payload - min_local) % (usable - 4);
                    if candidate <= max_local {
                        candidate
                    } else {
                        min_local
                    }
                };
                let end =
                    offset + prefix + varint_bytes + local + 4 * usize::from(payload > max_local);
                assert!(end <= usable, "cell exceeds usable bytes on page {number}");
                intervals.push((offset, end));
            }
            intervals.sort_unstable();
            assert!(
                intervals.windows(2).all(|pair| pair[0].1 <= pair[1].0),
                "overlapping cells on index page {number}"
            );
            if interior {
                stats.interior_pages += 1;
                pending.push((read_u32(page, header + 8), depth + 1));
            } else {
                stats.leaf_pages += 1;
                stats.one_cell_leaves += usize::from(cells == 1);
                stats.min_leaf_cells = stats.min_leaf_cells.min(cells);
                stats.max_leaf_cells = stats.max_leaf_cells.max(cells);
                assert_eq!(
                    *leaf_depth.get_or_insert(depth),
                    depth,
                    "unequal leaf depth"
                );
                stats.depth = depth;
            }
        }
        // Index parent dividers are records, not copies of leaf records.
        assert_eq!(stats.index_cells, expected_rows, "{index_name}: {stats:?}");
        assert!(stats.leaf_pages > 0);
        stats
    }

    async fn execute_pair(conn: &Connection, stock: &rusqlite::Connection, sql: &str) {
        stock
            .execute_batch(sql)
            .unwrap_or_else(|err| panic!("stock {sql}: {err}"));
        conn.execute_batch(sql)
            .await
            .unwrap_or_else(|err| panic!("fsqlite {sql}: {err}"));
    }

    async fn open_pair(
        fpath: &Path,
        spath: &Path,
        schema: &str,
    ) -> (Connection, rusqlite::Connection) {
        let conn = Connection::open(fpath.to_str().expect("database path"))
            .await
            .expect("open");
        let stock = rusqlite::Connection::open(spath).expect("stock open");
        execute_pair(
            &conn,
            &stock,
            "PRAGMA page_size=4096; PRAGMA journal_mode=DELETE;",
        )
        .await;
        execute_pair(&conn, &stock, schema).await;
        assert_eq!(
            fsqlite_rows(&conn, "PRAGMA page_size").await,
            [vec!["4096"]]
        );
        (conn, stock)
    }

    async fn assert_queries(conn: &Connection, spath: &Path, queries: &[String]) {
        for sql in queries {
            let expected = stock_rows(spath, sql);
            let actual = fsqlite_rows(conn, sql).await;
            assert_eq!(actual.len(), expected.len(), "row count for {sql}");
            for (position, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
                assert_eq!(actual, expected, "row {position} for {sql}");
            }
        }
        assert_eq!(
            fsqlite_rows(conn, "PRAGMA integrity_check").await,
            [vec!["ok"]]
        );
    }

    fn assert_persisted_queries(fpath: &Path, spath: &Path, queries: &[String]) {
        for sql in queries {
            assert_eq!(
                stock_rows(fpath, sql),
                stock_rows(spath, sql),
                "persisted {sql}"
            );
        }
    }

    fn integer_queries() -> Vec<String> {
        let mut queries = vec![
            "SELECT rowid,x FROM a INDEXED BY ia ORDER BY x,rowid".to_owned(),
            "SELECT rowid,x FROM a INDEXED BY ia ORDER BY x DESC,rowid DESC".to_owned(),
            "SELECT rowid,x FROM a NOT INDEXED ORDER BY rowid".to_owned(),
        ];
        for value in [-1, 0, 1, 255, 256, 1023, 4096, 8191, 11_999, 12_000, 12_001] {
            queries.push(format!(
                "SELECT rowid,x FROM a INDEXED BY ia WHERE x={value} ORDER BY rowid"
            ));
        }
        for order in ["ASC", "DESC"] {
            for (lo, hi) in [(-3000, 10), (250, 900), (4090, 4300), (11_800, 12_010)] {
                queries.push(format!(
                    "SELECT rowid,x FROM a INDEXED BY ia WHERE x BETWEEN {lo} AND {hi} \
                     ORDER BY x {order},rowid {order}"
                ));
            }
        }
        queries
    }

    fn emit_measurement(
        label: &str,
        ours: &IndexGeometry,
        stock: &IndexGeometry,
        times: Option<[Duration; 2]>,
    ) {
        eprintln!(
            "GH521 {}",
            serde_json::json!({
                "case": label,
                "page_size": PAGE_SIZE,
                "fsqlite": ours.measurement(env!("CARGO_PKG_VERSION"), times.map(|values| values[0])),
                "sqlite": stock.measurement(rusqlite::version(), times.map(|values| values[1])),
            })
        );
    }

    async fn integer_case(label: &str, keys: &[i64], autocommit: bool) {
        let dir = tempfile::tempdir().expect("tempdir");
        let fpath = dir.path().join("fsqlite.db");
        let spath = dir.path().join("stock.db");
        let (conn, stock) = open_pair(&fpath, &spath, INTEGER_SCHEMA).await;
        // Literal, single-row statements reproduce the reporter's CLI workload.
        // Timings are diagnostic; machine-dependent ratios are not test gates.
        let statements: Vec<String> = keys
            .iter()
            .map(|key| format!("INSERT INTO a VALUES({key})"))
            .collect();
        let started = Instant::now();
        if !autocommit {
            stock.execute_batch("BEGIN").expect("stock begin");
        }
        for sql in &statements {
            assert_eq!(stock.execute(sql, []).expect("stock insert"), 1);
        }
        if !autocommit {
            stock.execute_batch("COMMIT").expect("stock commit");
        }
        let stock_time = started.elapsed();
        let started = Instant::now();
        if !autocommit {
            conn.execute("BEGIN").await.expect("begin");
        }
        for sql in &statements {
            assert_eq!(conn.execute(sql).await.expect("insert"), 1);
        }
        if !autocommit {
            conn.execute("COMMIT").await.expect("commit");
        }
        let our_time = started.elapsed();
        let queries = integer_queries();
        assert_queries(&conn, &spath, &queries).await;
        conn.close().await.expect("close");
        drop(stock);
        let ours = index_geometry(&fpath, "ia", keys.len());
        let stock = index_geometry(&spath, "ia", keys.len());
        emit_measurement(label, &ours, &stock, Some([our_time, stock_time]));
        // With tiny integer entries, nearly-empty leaves are the regression,
        // even though integrity_check accepts them. Do not VACUUM first.
        assert!(ours.leaf_pages < 100, "{label}: {ours:?}");
        assert_eq!(ours.one_cell_leaves, 0, "{label}: {ours:?}");
        assert!(
            ours.database_bytes <= stock.database_bytes * 2,
            "{label}: ours={ours:?}, stock={stock:?}"
        );
        assert_persisted_queries(&fpath, &spath, &queries);
        let reopened = Connection::open(fpath.to_str().expect("path"))
            .await
            .expect("reopen");
        assert_queries(&reopened, &spath, &queries).await;
        reopened.close().await.expect("close reopened");
    }

    #[test]
    fn gh521_reported_12000_descending_autocommit() {
        asupersync::test_utils::run_test(|| async {
            integer_case(
                "descending-autocommit",
                &(1..=ROWS).rev().collect::<Vec<_>>(),
                true,
            )
            .await;
        });
    }

    #[test]
    fn gh521_descending_transaction() {
        asupersync::test_utils::run_test(|| async {
            integer_case(
                "descending-transaction",
                &(1..=ROWS).rev().collect::<Vec<_>>(),
                false,
            )
            .await;
        });
    }

    #[test]
    fn gh521_ascending_transaction() {
        asupersync::test_utils::run_test(|| async {
            let ascending: Vec<_> = (1..=ROWS).collect();
            integer_case("ascending-transaction", &ascending, false).await;
        });
    }

    #[test]
    fn gh521_alternating_extremes_transaction() {
        asupersync::test_utils::run_test(|| async {
            let alternating: Vec<_> = (0..ROWS / 2)
                .flat_map(|offset| [ROWS - offset, offset + 1])
                .collect();
            integer_case("alternating-extremes", &alternating, false).await;
        });
    }

    #[test]
    fn gh521_permuted_transaction() {
        asupersync::test_utils::run_test(|| async {
            let mut shuffled: Vec<_> = (1..=ROWS).collect();
            let mut rng = Rng(0x0521_1200_00ab_cdef);
            for index in (1..shuffled.len()).rev() {
                let other =
                    usize::try_from(rng.below(u64::try_from(index + 1).expect("shuffle range")))
                        .expect("shuffle index");
                shuffled.swap(index, other);
            }
            integer_case("deterministic-permutation", &shuffled, false).await;
        });
    }

    fn variable_key(id: i64, generation: u8) -> String {
        // Adjacent rowids duplicate the whole key. Lengths straddle the index
        // local-payload boundary and include multiple overflow pages.
        let group = usize::try_from(id / 2).expect("positive key");
        let lengths = [0, 8, 40, 480, 960, 980, 1000, 1024, 1900, 4097, 9000];
        let suffix = char::from(b'a' + generation)
            .to_string()
            .repeat(lengths[group % lengths.len()]);
        format!("{group:08}:{generation}:{suffix}")
    }

    #[test]
    fn gh521_composite_descending_timestamp_index() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("tempdir");
            let fpath = dir.path().join("fsqlite.db");
            let spath = dir.path().join("stock.db");
            let (conn, stock) = open_pair(
                &fpath,
                &spath,
                "CREATE TABLE snapshots(id INTEGER PRIMARY KEY,tool_name TEXT,collected_ts INTEGER); \
                 CREATE INDEX snapshot_order ON snapshots(tool_name ASC,collected_ts DESC);",
            )
            .await;
            let mut statements = Vec::new();
            for id in 1..=4096 {
                let tool_name = ["metrics", "read", "WRITE", "savepoint"][(id / 2) % 4];
                // Repeated tool/time pairs require the rowid tie breaker.
                // Increasing time means descending index keys in each tool.
                statements.push(format!(
                    "INSERT INTO snapshots VALUES({id},'{tool_name}',{})",
                    id / 8
                ));
            }
            let started = Instant::now();
            stock.execute_batch("BEGIN").expect("stock begin");
            for sql in &statements {
                stock.execute(sql, []).expect("stock insert");
            }
            stock.execute_batch("COMMIT").expect("stock commit");
            let stock_time = started.elapsed();
            let started = Instant::now();
            conn.execute("BEGIN").await.expect("begin");
            for sql in &statements {
                conn.execute(sql).await.expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");
            let our_time = started.elapsed();
            let mut queries = vec![
                "SELECT id,tool_name,collected_ts FROM snapshots INDEXED BY snapshot_order \
                 ORDER BY tool_name,collected_ts DESC,id"
                    .to_owned(),
                "SELECT id,tool_name,collected_ts FROM snapshots INDEXED BY snapshot_order \
                 ORDER BY tool_name DESC,collected_ts,id DESC"
                    .to_owned(),
                "SELECT id,tool_name,collected_ts FROM snapshots NOT INDEXED ORDER BY id"
                    .to_owned(),
            ];
            for tool_name in ["metrics", "read", "WRITE", "savepoint", "absent"] {
                for order in ["ASC", "DESC"] {
                    queries.push(format!(
                        "SELECT id,tool_name,collected_ts FROM snapshots INDEXED BY snapshot_order \
                         WHERE tool_name='{tool_name}' AND collected_ts BETWEEN 97 AND 331 \
                         ORDER BY collected_ts {order},id {order}"
                    ));
                }
                queries.push(format!(
                    "SELECT id,tool_name,collected_ts FROM snapshots INDEXED BY snapshot_order \
                     WHERE tool_name='{tool_name}' AND collected_ts=256 ORDER BY id"
                ));
            }
            assert_queries(&conn, &spath, &queries).await;
            conn.close().await.expect("close");
            drop(stock);
            let ours = index_geometry(&fpath, "snapshot_order", 4096);
            let stock = index_geometry(&spath, "snapshot_order", 4096);
            emit_measurement(
                "composite-tool-timestamp-desc",
                &ours,
                &stock,
                Some([our_time, stock_time]),
            );
            assert!(
                ours.leaf_pages <= stock.leaf_pages * 2 + 2,
                "ours={ours:?}, stock={stock:?}"
            );
            assert_eq!(ours.one_cell_leaves, 0, "{ours:?}");
            assert!(ours.database_bytes <= stock.database_bytes * 2);
            assert_persisted_queries(&fpath, &spath, &queries);
            let reopened = Connection::open(fpath.to_str().expect("path"))
                .await
                .expect("reopen");
            assert_queries(&reopened, &spath, &queries).await;
            reopened.close().await.expect("close reopened");
        });
    }

    async fn variable_insert(
        conn: &Connection,
        stock: &rusqlite::Connection,
        id: i64,
        generation: u8,
    ) {
        let key = variable_key(id, generation);
        let sql = "INSERT INTO v(id,k,tag) VALUES(?1,?2,?3)";
        stock
            .execute(sql, rusqlite::params![id, &key, i64::from(generation)])
            .expect("stock variable insert");
        conn.execute_with_params(
            sql,
            &[
                SqliteValue::Integer(id),
                SqliteValue::Text(key.into()),
                SqliteValue::Integer(i64::from(generation)),
            ],
        )
        .await
        .expect("variable insert");
    }

    fn variable_queries() -> Vec<String> {
        let mut queries = vec![
            "SELECT id,k,tag FROM v INDEXED BY vk ORDER BY k,id".to_owned(),
            "SELECT id,k,tag FROM v INDEXED BY vk ORDER BY k DESC,id DESC".to_owned(),
            "SELECT id,k,tag FROM v NOT INDEXED ORDER BY id".to_owned(),
        ];
        for order in ["ASC", "DESC"] {
            queries.push(format!(
                "SELECT id,k,tag FROM v INDEXED BY vk WHERE k >= '00000100' AND k < '00000300' \
                 ORDER BY k {order},id {order}"
            ));
        }
        for id in [2, 20, 21, 255, 511, 700, 767] {
            for generation in [0, 1] {
                queries.push(format!(
                    "SELECT id,k,tag FROM v INDEXED BY vk WHERE k='{}' ORDER BY id",
                    variable_key(id, generation)
                ));
            }
        }
        queries
    }

    #[test]
    fn gh521_variable_overflow_keys_delete_reinsert_and_rollback() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("tempdir");
            let fpath = dir.path().join("fsqlite.db");
            let spath = dir.path().join("stock.db");
            let (conn, stock) = open_pair(
                &fpath,
                &spath,
                "CREATE TABLE v(id INTEGER PRIMARY KEY,k TEXT NOT NULL,tag INTEGER NOT NULL); \
                 CREATE INDEX vk ON v(k);",
            )
            .await;
            execute_pair(&conn, &stock, "BEGIN").await;
            for id in (1..=768).rev() {
                variable_insert(&conn, &stock, id, 0).await;
            }
            execute_pair(&conn, &stock, "COMMIT").await;
            let queries = variable_queries();
            assert_queries(&conn, &spath, &queries).await;
            // Empty many adjacent leaves, then exercise reuse in reverse order.
            execute_pair(
                &conn,
                &stock,
                "BEGIN; DELETE FROM v WHERE id BETWEEN 100 AND 550 OR id%7=0; COMMIT;",
            )
            .await;
            assert_queries(&conn, &spath, &queries).await;
            execute_pair(&conn, &stock, "BEGIN").await;
            for id in (1..=768)
                .rev()
                .filter(|id| (100..=550).contains(id) || id % 7 == 0)
            {
                variable_insert(&conn, &stock, id, 1).await;
            }
            execute_pair(&conn, &stock, "COMMIT").await;
            assert_queries(&conn, &spath, &queries).await;
            for savepoint in [false, true] {
                execute_pair(&conn, &stock, "BEGIN").await;
                if savepoint {
                    execute_pair(
                        &conn,
                        &stock,
                        "UPDATE v SET tag=tag+1 WHERE id=1; SAVEPOINT rebalance;",
                    )
                    .await;
                }
                execute_pair(&conn, &stock, "DELETE FROM v WHERE id<=550").await;
                for id in (769..=1100).rev() {
                    variable_insert(&conn, &stock, id, 2).await;
                }
                // Compare through the writer: the stock file's other readers
                // correctly still see its committed snapshot during this txn.
                let expected = stock
                    .prepare("SELECT count(*) FROM v INDEXED BY vk")
                    .expect("count prepare")
                    .query_row([], |row| row.get::<_, i64>(0))
                    .expect("count");
                assert_eq!(
                    fsqlite_rows(&conn, "SELECT count(*) FROM v INDEXED BY vk").await,
                    [vec![expected.to_string()]]
                );
                execute_pair(
                    &conn,
                    &stock,
                    if savepoint {
                        "ROLLBACK TO rebalance; RELEASE rebalance; COMMIT;"
                    } else {
                        "ROLLBACK;"
                    },
                )
                .await;
                assert_queries(&conn, &spath, &queries).await;
            }
            conn.close().await.expect("close");
            drop(stock);
            let ours = index_geometry(&fpath, "vk", 768);
            let stock = index_geometry(&spath, "vk", 768);
            emit_measurement(
                "variable-overflow-delete-reinsert-rollback",
                &ours,
                &stock,
                None,
            );
            assert!(ours.overflow_cells > 0 && stock.overflow_cells > 0);
            assert!(
                ours.interior_pages > 1 && stock.interior_pages > 1,
                "fixture must exchange interior dividers"
            );
            // Large keys and deferred delete balancing can legitimately leave
            // singleton pages; bound total growth without outlawing those.
            assert!(
                ours.leaf_pages <= stock.leaf_pages * 2 + 8,
                "ours={ours:?}, stock={stock:?}"
            );
            assert!(
                ours.database_bytes <= stock.database_bytes * 2 + PAGE_SIZE * 8,
                "ours={ours:?}, stock={stock:?}"
            );
            assert_persisted_queries(&fpath, &spath, &queries);
            let reopened = Connection::open(fpath.to_str().expect("path"))
                .await
                .expect("reopen");
            assert_queries(&reopened, &spath, &queries).await;
            reopened.close().await.expect("close reopened");
        });
    }

    #[test]
    fn gh521_index_balancing_survives_interrupted_transaction() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fpath = dir.path().join("fsqlite.db");
        let spath = dir.path().join("stock.db");
        asupersync::test_utils::run_test(|| async {
            let (conn, stock) = open_pair(&fpath, &spath, INTEGER_SCHEMA).await;
            execute_pair(&conn, &stock, "BEGIN").await;
            for key in (1..=1200).rev() {
                execute_pair(&conn, &stock, &format!("INSERT INTO a VALUES({key})")).await;
            }
            execute_pair(&conn, &stock, "COMMIT").await;
            conn.close().await.expect("close before subprocess");
        });
        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "gh521::gh521_index_uncommitted_abort_helper",
                "--ignored",
                "--nocapture",
            ])
            .env(CRASH_PATH, &fpath)
            .output()
            .expect("spawn index crash writer");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.lines().any(|line| line == CRASH_READY),
            "helper failed before staged writes: {stderr}"
        );
        assert!(!output.status.success(), "helper must abort");
        #[cfg(unix)]
        assert!(
            output.status.code().is_none(),
            "helper must terminate by signal, not a test panic"
        );
        {
            let stock = rusqlite::Connection::open(&spath).expect("oracle open");
            stock.execute_batch("BEGIN").expect("oracle begin");
            for key in (-512..=-1).rev() {
                stock
                    .execute("INSERT INTO a VALUES(?1)", [key])
                    .expect("oracle committed tail");
            }
            stock.execute_batch("COMMIT").expect("oracle commit");
        }
        let queries = integer_queries();
        // Stock is the first reader of the crashed database. This also checks
        // durable divider/overflow ownership without FrankenSQLite recovery.
        assert_persisted_queries(&fpath, &spath, &queries);
        assert_eq!(stock_rows(&fpath, "PRAGMA integrity_check"), [vec!["ok"]]);
        asupersync::test_utils::run_test(|| async {
            let reopened = Connection::open(fpath.to_str().expect("path"))
                .await
                .expect("recover");
            assert_queries(&reopened, &spath, &queries).await;
            reopened.close().await.expect("close recovered");
        });
        let ours = index_geometry(&fpath, "ia", 1712);
        let stock = index_geometry(&spath, "ia", 1712);
        emit_measurement("interrupted-wal-transaction", &ours, &stock, None);
    }

    #[test]
    #[ignore = "subprocess entrypoint for gh521_index_balancing_survives_interrupted_transaction"]
    fn gh521_index_uncommitted_abort_helper() {
        let Ok(path) = std::env::var(CRASH_PATH) else {
            return;
        };
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&path).await.expect("crash writer open");
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0; BEGIN;")
                .await.expect("durable WAL begin");
            for key in (-512..=-1).rev() {
                conn.execute(&format!("INSERT INTO a VALUES({key})"))
                    .await
                    .expect("committed tail insert");
            }
            conn.execute_batch("COMMIT; BEGIN; DELETE FROM a WHERE x BETWEEN 200 AND 800;")
                .await
                .expect("stage delete");
            for key in (-3072..=-513).rev() {
                conn.execute(&format!("INSERT INTO a VALUES({key})"))
                    .await
                    .expect("uncommitted split");
            }
            assert_eq!(
                fsqlite_rows(&conn, "SELECT count(*) FROM a INDEXED BY ia").await,
                [vec!["3671"]]
            );
            assert_eq!(
                fsqlite_rows(&conn, "PRAGMA integrity_check").await,
                [vec!["ok"]]
            );
            eprintln!("{CRASH_READY}");
            std::process::abort();
        });
    }
}
