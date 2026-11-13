#![recursion_limit = "512"]

//! bd-obwsy: successful unindexed VALUES loads must not corrupt the persisted
//! table. Keep the owner-reported 6,000-row, seed-1, 50..=3,200-byte workload:
//! small/fixed-width or pre-indexed fixtures are controls, not substitutes.
//! No SELECT, integrity check, or index construction interrupts the load. Check
//! the checkpointed file with stock SQLite BEFORE reopening it in FrankenSQLite,
//! then read it back through FrankenSQLite too.
//!
//! Root cause (fixed with this keeper): the prepared direct-INSERT lane retains
//! a right-edge leaf image across rows and appends to it without rereading the
//! page. When a row did not fit and the cursor's own insert balanced the leaf,
//! moving the right edge to another page, the hint refreshed its leaf page and
//! rowid but carried the OLD leaf image forward. The next row that fit was
//! appended to a page that was no longer the right edge, at slots computed from
//! a stale header: offset-0 pointers, out-of-order rowids, overlapping cells.
//! Both public statement APIs reach that lane, by different routes. On Linux at
//! 84ce236, through `Connection::query` (the CLI's per-statement call) seeds 1
//! and 2 corrupted at 64+ rows per statement, the morsel-replay threshold (63
//! stayed clean), exactly the owner's macOS 0.4.9 CLI matrix. Through
//! `Connection::execute` the same seeds corrupted at ONE row per statement
//! instead (seed 1: first bad insert row 4775 onto leaf 2577). Seed 1 at 6,000
//! rows gave the owner's exact `Tree 2 page 2832 cell 3: Rowid 5258 out of
//! order` on both routes. Every case below runs through both APIs.
//!
//! Local keeper (not an assertion that disabled GitHub Actions executed it):
//! cargo test --locked -p fsqlite-core --test bd_obwsy_bulk_insert_corruption \
//!     -- --test-threads=1 --nocapture

use std::fmt::Write;
use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use rusqlite::{Connection as StockConnection, OpenFlags};

/// Independent CPython 3 `Random(seed).randint(50, 3200)` references for the
/// 6,000-row fixtures: the first ten lengths and the sum of all of them,
/// computed by Python itself, never by the generator below.
const PYTHON_REFERENCES: [(u32, [usize; 10], usize); 3] = [
    (
        1,
        [600, 2381, 3178, 308, 1094, 532, 2079, 3166, 1891, 1984],
        9_725_060,
    ),
    (
        2,
        [281, 425, 397, 1528, 742, 3064, 2793, 1312, 1080, 2531],
        9_796_583,
    ),
    (
        3,
        [1024, 2477, 2279, 584, 1565, 2523, 1991, 2612, 2429, 318],
        9_692_623,
    ),
];

/// The owner's Python `Random(seed).randint(50, 3200)` fixture, generated
/// without a version-dependent rand crate. CPython seeds MT19937 with
/// `init_by_array([seed])`, then uses rejection sampling on the high 12 bits
/// for `randrange(3151)`. This is TEST DATA generation, not an engine RNG.
fn owner_lengths(seed: u32) -> Vec<usize> {
    let mut state = [0_u32; 624];
    state[0] = 19_650_218;
    for i in 1..624 {
        state[i] = 1_812_433_253_u32
            .wrapping_mul(state[i - 1] ^ (state[i - 1] >> 30))
            .wrapping_add(u32::try_from(i).unwrap());
    }
    let mut i = 1;
    for _ in 0..624 {
        state[i] = (state[i] ^ (state[i - 1] ^ (state[i - 1] >> 30)).wrapping_mul(1_664_525))
            .wrapping_add(seed);
        i += 1;
        if i == 624 {
            state[0] = state[623];
            i = 1;
        }
    }
    for _ in 0..623 {
        state[i] = (state[i] ^ (state[i - 1] ^ (state[i - 1] >> 30)).wrapping_mul(1_566_083_941))
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
                state[j] =
                    state[(j + 397) % 624] ^ (y >> 1) ^ if y & 1 == 0 { 0 } else { 0x9908_b0df };
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
    let (_, first_ten, sum) = PYTHON_REFERENCES
        .iter()
        .find(|(reference_seed, _, _)| *reference_seed == seed)
        .unwrap_or_else(|| panic!("seed {seed} has no independent Python reference"));
    assert_eq!(&lengths[..10], first_ten, "seed {seed}: Python prefix");
    assert_eq!(
        lengths.iter().sum::<usize>(),
        *sum,
        "seed {seed}: Python sum"
    );
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
    let count: i64 = db
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        count,
        i64::try_from(lengths.len()).unwrap(),
        "{label}: phantom/missing rows"
    );
    let mut statement = db.prepare("SELECT id,a FROM t ORDER BY id").unwrap();
    let mut rows = statement.query([]).unwrap();
    for (offset, expected_length) in lengths.iter().copied().enumerate() {
        let row = rows
            .next()
            .unwrap_or_else(|error| panic!("{label}: row {}: {error}", offset + 1))
            .unwrap_or_else(|| panic!("{label}: missing row {}", offset + 1));
        let id: i64 = row.get(0).unwrap();
        let text: String = row.get(1).unwrap();
        assert_eq!(id, i64::try_from(offset + 1).unwrap(), "{label}: row order");
        assert_eq!(
            text.len(),
            expected_length,
            "{label}: payload length at row {id}"
        );
        assert!(
            text.bytes().all(|byte| byte == b'x'),
            "{label}: payload at row {id}"
        );
    }
    assert!(
        rows.next().unwrap().is_none(),
        "{label}: trailing phantom row"
    );
}

/// The same proof through FrankenSQLite's own reader, after the stock oracle
/// has already judged the untouched file.
async fn assert_franken_file(path: &Path, lengths: &[usize], label: &str) {
    let conn = Connection::open(path.to_string_lossy().into_owned())
        .await
        .unwrap_or_else(|error| panic!("{label}: FrankenSQLite reopen: {error}"));
    let integrity: Vec<String> = conn
        .query("PRAGMA integrity_check")
        .await
        .unwrap_or_else(|error| panic!("{label}: FrankenSQLite integrity_check: {error}"))
        .iter()
        .map(|row| row.values()[0].as_text().unwrap_or("<non-text>").to_owned())
        .collect();
    assert_eq!(
        integrity,
        vec!["ok".to_owned()],
        "{label}: FrankenSQLite integrity"
    );
    let count = conn.query("SELECT count(*) FROM t").await.unwrap();
    assert!(
        matches!(count[0].values(), [SqliteValue::Integer(n)] if *n == i64::try_from(lengths.len()).unwrap()),
        "{label}: FrankenSQLite count {:?}",
        count[0].values()
    );
    let rows = conn
        .query("SELECT id,a FROM t ORDER BY id")
        .await
        .unwrap_or_else(|error| panic!("{label}: FrankenSQLite ordered scan: {error}"));
    assert_eq!(
        rows.len(),
        lengths.len(),
        "{label}: FrankenSQLite scan row count"
    );
    for (offset, (row, expected_length)) in rows.iter().zip(lengths).enumerate() {
        let expected_id = i64::try_from(offset + 1).unwrap();
        assert!(
            matches!(row.values()[0], SqliteValue::Integer(id) if id == expected_id),
            "{label}: FrankenSQLite row order at {expected_id}: {:?}",
            row.values()[0]
        );
        let text = row.values()[1].as_text().unwrap_or_else(|| {
            panic!("{label}: FrankenSQLite payload at row {expected_id} is not TEXT")
        });
        assert_eq!(
            text.len(),
            *expected_length,
            "{label}: FrankenSQLite length at {expected_id}"
        );
        assert!(
            text.bytes().all(|byte| byte == b'x'),
            "{label}: FrankenSQLite payload at {expected_id}"
        );
    }
    conn.close()
        .await
        .unwrap_or_else(|error| panic!("{label}: FrankenSQLite close: {error}"));
}

/// The public statement call each load statement goes through.
#[derive(Clone, Copy, Debug)]
enum Api {
    /// `Connection::query`, as the CLI runs every piped statement.
    Query,
    /// `Connection::execute`.
    Execute,
}

async fn run(conn: &Connection, api: Api, sql: &str) -> Result<(), String> {
    match api {
        Api::Query => conn.query(sql).await.map(drop),
        Api::Execute => conn.execute(sql).await.map(drop),
    }
    .map_err(|error| error.to_string())
}

#[derive(Clone, Copy)]
struct Load {
    api: Api,
    seed: u32,
    rows: usize,
    batch: usize,
    indexed: bool,
    fixed: bool,
    build_index_after: bool,
}

impl Load {
    const fn owner(api: Api, batch: usize) -> Self {
        Self {
            api,
            seed: 1,
            rows: 6_000,
            batch,
            indexed: false,
            fixed: false,
            build_index_after: false,
        }
    }
}

const APIS: [Api; 2] = [Api::Query, Api::Execute];

async fn exercise(load: Load) {
    let Load {
        api,
        seed,
        rows,
        batch,
        indexed,
        fixed,
        build_index_after,
    } = load;
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("franken.db");
    let control = directory.path().join("stock.db");
    let mut lengths = owner_lengths(seed);
    lengths.truncate(rows);
    if fixed {
        lengths.fill(1_024);
    }
    let label = format!(
        "bd-obwsy api={api:?} seed={seed} rows={rows} batch={batch} indexed={indexed} fixed={fixed}"
    );
    eprintln!("{label}");
    let conn = Connection::open(target.to_string_lossy().into_owned())
        .await
        .unwrap();
    let stock = StockConnection::open(&control).unwrap();
    // Deliberately leave FrankenSQLite's default concurrent-writer setting on.
    for sql in [
        "PRAGMA page_size=4096",
        "PRAGMA journal_mode=WAL",
        "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT)",
    ] {
        run(&conn, api, sql)
            .await
            .unwrap_or_else(|error| panic!("{label}: {sql}: {error}"));
        stock.execute_batch(sql).unwrap();
    }
    if indexed {
        let sql = "CREATE INDEX t_a ON t(a)";
        run(&conn, api, sql).await.unwrap();
        stock.execute_batch(sql).unwrap();
    }
    run(&conn, api, "BEGIN").await.unwrap();
    stock.execute_batch("BEGIN").unwrap();
    for (ordinal, chunk) in lengths.chunks(batch).enumerate() {
        let sql = insert_sql(ordinal * batch, chunk);
        run(&conn, api, &sql).await.unwrap_or_else(|error| {
            panic!(
                "{label}: batch {ordinal}, first row {}: {error}",
                ordinal * batch + 1
            )
        });
        stock.execute_batch(&sql).unwrap();
    }
    run(&conn, api, "COMMIT").await.unwrap();
    stock
        .execute_batch("COMMIT; PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    drop(stock);
    let checkpoint = conn.query("PRAGMA wal_checkpoint(TRUNCATE)").await.unwrap();
    assert_eq!(checkpoint.len(), 1, "{label}: checkpoint result");
    assert!(
        matches!(
            checkpoint[0].values().first(),
            Some(SqliteValue::Integer(0))
        ),
        "{label}: checkpoint must not be busy"
    );
    conn.close().await.unwrap();
    // Validate identical SQL against the independent engine before blaming the
    // engine under test; then inspect the actual persisted FrankenSQLite file.
    assert_stock_file(&control, &lengths, &format!("{label} stock-built control"));
    assert_stock_file(&target, &lengths, &label);
    assert_franken_file(&target, &lengths, &label).await;
    if build_index_after {
        let reopened = Connection::open(target.to_string_lossy().into_owned())
            .await
            .unwrap();
        reopened
            .execute("CREATE INDEX after_load ON t(a)")
            .await
            .unwrap_or_else(|error| panic!("{label}: CREATE INDEX after load: {error}"));
        let checkpoint = reopened
            .query("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .unwrap();
        assert!(matches!(
            checkpoint[0].values().first(),
            Some(SqliteValue::Integer(0))
        ));
        reopened.close().await.unwrap();
        assert_stock_file(&target, &lengths, &format!("{label} after index build"));
    }
}

#[test]
fn owner_fixture_matches_python_for_every_seed() {
    for (seed, _, _) in PYTHON_REFERENCES {
        let _ = owner_lengths(seed);
    }
}

/// The `execute` reproducer: one row per statement through the prepared
/// direct-INSERT lane's retained right-edge leaf (corrupt at 84ce236).
#[test]
fn variable_unindexed_6000_rows_in_1_row_statements() {
    asupersync::test_utils::run_test(|| async {
        for api in APIS {
            exercise(Load::owner(api, 1)).await;
        }
    });
}

/// The owner's reported trigger (corrupt at 84ce236 through `query`).
#[test]
fn variable_unindexed_6000_rows_in_100_row_statements() {
    asupersync::test_utils::run_test(|| async {
        for api in APIS {
            exercise(Load::owner(api, 100)).await;
        }
    });
}

#[test]
fn variable_unindexed_6000_rows_in_200_row_statements_and_later_index() {
    asupersync::test_utils::run_test(|| async {
        for api in APIS {
            exercise(Load {
                build_index_after: true,
                ..Load::owner(api, 200)
            })
            .await;
        }
    });
}

/// 64 rows per statement is the first size replayed through morsels; 63 is
/// the last that is not (at 84ce236: 63 clean, 64 corrupt through `query`).
#[test]
fn morsel_threshold_boundary_63_and_64_row_statements() {
    asupersync::test_utils::run_test(|| async {
        for api in APIS {
            for batch in [63, 64] {
                exercise(Load::owner(api, batch)).await;
            }
        }
    });
}

/// A second independent layout: seed 2 reproduced on both routes at 84ce236
/// (`query` at 64 rows per statement, `execute` at one).
#[test]
fn second_seed_reproducers_remain_correct() {
    asupersync::test_utils::run_test(|| async {
        exercise(Load {
            seed: 2,
            ..Load::owner(Api::Query, 64)
        })
        .await;
        exercise(Load {
            seed: 2,
            ..Load::owner(Api::Execute, 1)
        })
        .await;
    });
}

#[test]
fn reported_small_batch_controls_remain_correct() {
    asupersync::test_utils::run_test(|| async {
        for api in APIS {
            for batch in [10, 50] {
                exercise(Load::owner(api, batch)).await;
            }
        }
    });
}

#[test]
fn reported_index_width_and_row_count_controls_remain_correct() {
    asupersync::test_utils::run_test(|| async {
        for api in APIS {
            exercise(Load {
                indexed: true,
                ..Load::owner(api, 200)
            })
            .await;
            exercise(Load {
                fixed: true,
                ..Load::owner(api, 200)
            })
            .await;
            exercise(Load {
                rows: 3_000,
                ..Load::owner(api, 200)
            })
            .await;
        }
    });
}
