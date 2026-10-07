#![recursion_limit = "512"]

//! bd-obwsy: successful unindexed multi-row VALUES loads must not corrupt the
//! persisted table. Keep the owner-reported 6,000-row, seed-1, 50..=3,200-byte
//! workload: small/fixed-width or pre-indexed fixtures are controls, not substitutes.
//! No SELECT, integrity check, or index construction interrupts the load. Check
//! the checkpointed file with stock SQLite BEFORE reopening it in FrankenSQLite.
//!
//! Local keeper (not an assertion that disabled GitHub Actions executed it):
//! cargo test --locked -p fsqlite-core --test bd_obwsy_bulk_insert_corruption \
//!     -- --test-threads=1 --nocapture

use std::fmt::Write;
use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use rusqlite::{Connection as StockConnection, OpenFlags};

/// The owner's Python Random(1).randint(50, 3200) fixture, generated without a
/// version-dependent rand crate. CPython seeds MT19937 with init_by_array([1]),
/// then uses rejection sampling on the high 12 bits for randrange(3151).
/// This is TEST DATA generation, not an engine RNG or a RaptorQ implementation.
fn owner_lengths() -> Vec<usize> {
    let mut state = [0_u32; 624];
    state[0] = 19_650_218;
    for i in 1..624 {
        state[i] = 1_812_433_253_u32
            .wrapping_mul(state[i - 1] ^ (state[i - 1] >> 30))
            .wrapping_add(u32::try_from(i).unwrap());
    }
    let mut i = 1;
    for _ in 0..624 {
        state[i] = (state[i]
            ^ (state[i - 1] ^ (state[i - 1] >> 30)).wrapping_mul(1_664_525))
        .wrapping_add(1);
        i += 1;
        if i == 624 {
            state[0] = state[623];
            i = 1;
        }
    }
    for _ in 0..623 {
        state[i] = (state[i]
            ^ (state[i - 1] ^ (state[i - 1] >> 30)).wrapping_mul(1_566_083_941))
        .wrapping_sub(u32::try_from(i).unwrap());
        i += 1;
        if i == 624 {
            state[0] = state[623];
            i = 1;
        }
    }
    state[0] = 0x8000_0000;
    let mut index = 624;
    let mut lengths = Vec::with_capacity(6_000);
    while lengths.len() < 6_000 {
        if index == 624 {
            for j in 0..624 {
                let y = (state[j] & 0x8000_0000) | (state[(j + 1) % 624] & 0x7fff_ffff);
                state[j] = state[(j + 397) % 624]
                    ^ (y >> 1)
                    ^ if y & 1 == 0 { 0 } else { 0x9908_b0df };
            }
            index = 0;
        }
        let mut y = state[index];
        index += 1;
        y ^= y >> 11;
        y ^= (y << 7) & 0x9d2c_5680;
        y ^= (y << 15) & 0xefc6_0000;
        y ^= y >> 18;
        let candidate = usize::try_from(y >> 20).unwrap();
        if candidate < 3_151 {
            lengths.push(50 + candidate);
        }
    }
    // Independent Python 3 Random(1) reference values, not values computed
    // from this generator inside the assertion.
    assert_eq!(&lengths[..10], &[600, 2381, 3178, 308, 1094, 532, 2079, 3166, 1891, 1984]);
    assert_eq!(lengths.iter().sum::<usize>(), 9_725_060);
    lengths
}

fn insert_sql(first: usize, lengths: &[usize]) -> String {
    let mut sql = String::from("INSERT INTO t(id,a) VALUES ");
    for (offset, length) in lengths.iter().copied().enumerate() {
        if offset != 0 {
            sql.push(',');
        }
        write!(&mut sql, "({},'", first + offset + 1).unwrap();
        sql.extend(std::iter::repeat_n('x', length));
        sql.push_str("')");
    }
    sql.push(';');
    sql
}

fn assert_stock_file(path: &Path, lengths: &[usize], label: &str) {
    // A read-only oracle cannot repair the file or construct a hiding index.
    let db = StockConnection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap_or_else(|error| panic!("{label}: stock open: {error}"));
    let integrity: Vec<String> = db
        .prepare("PRAGMA integrity_check")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(integrity, vec!["ok".to_owned()], "{label}: stock integrity");
    let count: i64 = db.query_row("SELECT count(*) FROM t", [], |row| row.get(0)).unwrap();
    assert_eq!(count, i64::try_from(lengths.len()).unwrap(), "{label}: phantom/missing rows");
    let mut statement = db.prepare("SELECT id,a FROM t ORDER BY id").unwrap();
    let mut rows = statement.query([]).unwrap();
    for (offset, expected_length) in lengths.iter().copied().enumerate() {
        let row = rows.next().unwrap_or_else(|error| panic!("{label}: row {}: {error}", offset + 1))
            .unwrap_or_else(|| panic!("{label}: missing row {}", offset + 1));
        let id: i64 = row.get(0).unwrap();
        let text: String = row.get(1).unwrap();
        assert_eq!(id, i64::try_from(offset + 1).unwrap(), "{label}: row order");
        assert_eq!(text.len(), expected_length, "{label}: payload length at row {id}");
        assert!(text.bytes().all(|byte| byte == b'x'), "{label}: payload at row {id}");
    }
    assert!(rows.next().unwrap().is_none(), "{label}: trailing phantom row");
}

async fn exercise(rows: usize, batch: usize, indexed: bool, fixed: bool, build_index_after: bool) {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("franken.db");
    let control = directory.path().join("stock.db");
    let mut lengths = owner_lengths();
    lengths.truncate(rows);
    if fixed {
        lengths.fill(1_024);
    }
    let label = format!("bd-obwsy rows={rows} batch={batch} indexed={indexed} fixed={fixed}");
    eprintln!("{label}");
    let conn = Connection::open(&target.to_string_lossy()).await.unwrap();
    let stock = StockConnection::open(&control).unwrap();
    // Deliberately leave FrankenSQLite's default concurrent-writer setting on.
    for sql in ["PRAGMA page_size=4096", "PRAGMA journal_mode=WAL", "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT)"] {
        conn.execute(sql).await.unwrap_or_else(|error| panic!("{label}: {sql}: {error}"));
        stock.execute_batch(sql).unwrap();
    }
    if indexed {
        let sql = "CREATE INDEX t_a ON t(a)";
        conn.execute(sql).await.unwrap();
        stock.execute_batch(sql).unwrap();
    }
    conn.execute("BEGIN").await.unwrap();
    stock.execute_batch("BEGIN").unwrap();
    for (ordinal, chunk) in lengths.chunks(batch).enumerate() {
        let sql = insert_sql(ordinal * batch, chunk);
        conn.execute(&sql).await.unwrap_or_else(|error| {
            panic!("{label}: batch {ordinal}, first row {}: {error}", ordinal * batch + 1)
        });
        stock.execute_batch(&sql).unwrap();
    }
    conn.execute("COMMIT").await.unwrap();
    stock.execute_batch("COMMIT; PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(stock);
    let checkpoint = conn.query("PRAGMA wal_checkpoint(TRUNCATE)").await.unwrap();
    assert_eq!(checkpoint.len(), 1, "{label}: checkpoint result");
    assert!(matches!(checkpoint[0].values().first(), Some(SqliteValue::Integer(0))),
        "{label}: checkpoint must not be busy");
    drop(conn);
    // Validate identical SQL against the independent engine before blaming the
    // engine under test; then inspect the actual persisted FrankenSQLite file.
    assert_stock_file(&control, &lengths, &format!("{label} stock-built control"));
    assert_stock_file(&target, &lengths, &label);
    if build_index_after {
        let reopened = Connection::open(&target.to_string_lossy()).await.unwrap();
        reopened.execute("CREATE INDEX after_load ON t(a)").await
            .unwrap_or_else(|error| panic!("{label}: CREATE INDEX after load: {error}"));
        let checkpoint = reopened.query("PRAGMA wal_checkpoint(TRUNCATE)").await.unwrap();
        assert!(matches!(checkpoint[0].values().first(), Some(SqliteValue::Integer(0))));
        drop(reopened);
        assert_stock_file(&target, &lengths, &format!("{label} after index build"));
    }
}

#[test]
fn owner_seed_one_fixture_matches_python() {
    let _ = owner_lengths();
}

#[test]
fn variable_unindexed_6000_rows_in_100_row_statements() {
    asupersync::test_utils::run_test(|| async { exercise(6_000, 100, false, false, false).await });
}

#[test]
fn variable_unindexed_6000_rows_in_200_row_statements_and_later_index() {
    asupersync::test_utils::run_test(|| async { exercise(6_000, 200, false, false, true).await });
}

#[test]
fn reported_small_batch_controls_remain_correct() {
    asupersync::test_utils::run_test(|| async {
        for batch in [1, 10, 50] {
            exercise(6_000, batch, false, false, false).await;
        }
    });
}

#[test]
fn reported_index_width_and_row_count_controls_remain_correct() {
    asupersync::test_utils::run_test(|| async {
        exercise(6_000, 200, true, false, false).await;
        exercise(6_000, 200, false, true, false).await;
        exercise(3_000, 200, false, false, false).await;
    });
}
