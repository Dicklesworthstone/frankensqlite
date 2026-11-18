//! bd-hi7qg keepers: the implicit rowid after a table's maximum row was
//! deleted, against a live stock-SQLite oracle (bundled rusqlite), and two
//! writers on one file never storing a rowid twice.
//!
//! Without AUTOINCREMENT stock takes `max(rowid) + 1` at every insert, so a
//! deleted maximum is reused. A file-backed implicit-rowid INSERT that runs
//! as a VDBE program (here: with `RETURNING`; also DEFAULT VALUES and INSERT
//! ... SELECT) draws its rowid from the shared concurrent allocator, whose tip
//! used to only rise. It now falls back to `max(rowid) + 1` when no live
//! session holds a claim on the table and no write commit became visible since
//! the inserting transaction began. The single-row INSERT that `execute` and
//! `prepare` run takes `max(rowid) + 1` from its own cursor and never asks the
//! allocator. Each case runs on `:memory:`, on a file through `execute`,
//! through `prepare`, and through `query` with `RETURNING id`, and compares
//! every inserted rowid, the table, `sqlite_sequence` and the integrity check
//! with stock's.
//!
//! The allocator's refusals (a live claim of another session, a write commit
//! since BEGIN, AUTOINCREMENT) are pinned by the fsqlite-mvcc unit tests
//! `a_deleted_maximum_is_reused_only_when_provably_free` and
//! `a_rewind_after_a_lowering_never_reissues_a_peers_rowid`. Through SQL two
//! writers that pick the same rowid always meet on the leaf that holds it
//! first, as a page-lock refusal or a snapshot conflict, so the two-writer test
//! here checks the end state: every committed row stored exactly once, every
//! refused one not at all, and the file intact, on both insert lanes.
//!
//! bd-mkon7's original shape (an emptied table, 20,000 prepared inserts a
//! round, rowids restarting at 1 as in stock) runs on both lanes. Its
//! negatives (an emptied table keeps its old floor while another session
//! holds a claim on it, or after a write commit since the BEGIN sample) are
//! allocator cases in `a_deleted_maximum_is_reused_only_when_provably_free`:
//! through SQL a peer's live rows hold the leaf, and a concurrent BEGIN's
//! snapshot still shows the rows a later commit deleted.
//!
//! Left out, a recorded divergence (bd-hi7qg): a transaction that inserts the
//! maximum itself, deletes it and inserts again in the same transaction keeps
//! the gap on the allocator lane.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

/// The hot-path profile counters are process-global, so the tests here run
/// one at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

type Rows = Vec<Vec<String>>;

fn tag_f(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}

fn tag_r(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

fn stock_rows(db: &rusqlite::Connection, sql: &str) -> Rows {
    let mut statement = db
        .prepare(sql)
        .unwrap_or_else(|e| panic!("stock prepare `{sql}`: {e}"));
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .unwrap_or_else(|e| panic!("stock query `{sql}`: {e}"))
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap_or_else(|e| panic!("stock rows `{sql}`: {e}"))
}

async fn frank_rows(conn: &Connection, sql: &str) -> Rows {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite query `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Lane {
    /// `:memory:` through `execute`.
    Memory,
    /// A file through `execute`.
    File,
    /// A file through `prepare` and `execute`.
    Prepared,
    /// A file; every INSERT, the setup's included, carries `RETURNING id`
    /// and runs through `query`: a VDBE program and the shared allocator.
    Returning,
}

const SETUP: [&str; 3] = [
    "CREATE TABLE k(id INTEGER PRIMARY KEY, v TEXT)",
    "CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
    "INSERT INTO k(v) VALUES ('one'), ('two'), ('three')",
];

const DELETE_K_MAX: &str = "DELETE FROM k WHERE id = (SELECT max(id) FROM k)";

/// (name, steps). Every step is a statement of its own.
const CASES: &[(&str, &[&str])] = &[
    (
        "delete the max, then insert, across transactions",
        &[DELETE_K_MAX, "INSERT INTO k(v) VALUES ('after')"],
    ),
    (
        "delete the max, then insert, in one transaction",
        &[
            "BEGIN",
            DELETE_K_MAX,
            "INSERT INTO k(v) VALUES ('after')",
            "COMMIT",
        ],
    ),
    (
        "insert, delete that row, insert, each its own transaction",
        &[
            "INSERT INTO k(v) VALUES ('own')",
            DELETE_K_MAX,
            "INSERT INTO k(v) VALUES ('again')",
        ],
    ),
    (
        "an explicit maximum deleted, then an implicit insert",
        &[
            "INSERT INTO k(id, v) VALUES (100, 'explicit')",
            "INSERT INTO k(v) VALUES ('above')",
            DELETE_K_MAX,
            DELETE_K_MAX,
            "INSERT INTO k(v) VALUES ('after')",
        ],
    ),
    (
        "a rolled-back insert's rowid is taken again",
        &[
            "BEGIN",
            "INSERT INTO k(v) VALUES ('rolled back')",
            "ROLLBACK",
            "INSERT INTO k(v) VALUES ('after')",
        ],
    ),
    (
        "a row below the maximum deleted keeps max + 1",
        &[
            "DELETE FROM k WHERE id = 2",
            "INSERT INTO k(v) VALUES ('after')",
        ],
    ),
    (
        "a table emptied entirely starts over at 1",
        &[
            "DELETE FROM k WHERE id > 0",
            "INSERT INTO k(v) VALUES ('first')",
        ],
    ),
    (
        "AUTOINCREMENT never reuses a deleted maximum",
        &[
            "INSERT INTO a(v) VALUES ('x'), ('y'), ('z')",
            "DELETE FROM a WHERE id = (SELECT max(id) FROM a)",
            "INSERT INTO a(v) VALUES ('after')",
            "DELETE FROM a WHERE id = (SELECT max(id) FROM a)",
            "INSERT INTO a(v) VALUES ('again')",
        ],
    ),
];

const CHECKS: [&str; 4] = [
    "SELECT id, v FROM k ORDER BY id",
    "SELECT id, v FROM a ORDER BY id",
    "SELECT name, seq FROM sqlite_sequence ORDER BY name",
    "PRAGMA integrity_check",
];

/// Run `sql` on both engines the way `lane` runs it, comparing the rowids an
/// INSERT takes.
async fn run_step(stock: &rusqlite::Connection, conn: &Connection, lane: Lane, sql: &str) {
    let label = format!("{lane:?} `{sql}`");
    if !sql.starts_with("INSERT") {
        stock
            .execute_batch(sql)
            .unwrap_or_else(|e| panic!("stock {label}: {e}"));
        conn.execute(sql)
            .await
            .unwrap_or_else(|e| panic!("fsqlite {label}: {e}"));
        return;
    }
    match lane {
        Lane::Returning => {
            let returning = format!("{sql} RETURNING id");
            assert_eq!(
                frank_rows(conn, &returning).await,
                stock_rows(stock, &returning),
                "the rowids {label} took"
            );
        }
        Lane::Memory | Lane::File | Lane::Prepared => {
            stock
                .execute_batch(sql)
                .unwrap_or_else(|e| panic!("stock {label}: {e}"));
            let result = if matches!(lane, Lane::Prepared) {
                match conn.prepare(sql).await {
                    Ok(statement) => statement.execute().await.map(|_| ()),
                    Err(error) => Err(error),
                }
            } else {
                conn.execute(sql).await.map(|_| ())
            };
            result.unwrap_or_else(|e| panic!("fsqlite {label}: {e}"));
            assert_eq!(
                frank_rows(conn, "SELECT last_insert_rowid()").await,
                stock_rows(stock, "SELECT last_insert_rowid()"),
                "the rowid {label} took"
            );
        }
    }
}

#[test]
fn implicit_rowid_after_a_deleted_maximum_matches_stock() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        for &(name, steps) in CASES {
            for lane in [Lane::Memory, Lane::File, Lane::Prepared, Lane::Returning] {
                let stock = rusqlite::Connection::open_in_memory().expect("stock");
                let dir = tempfile::tempdir().expect("tempdir");
                let path = match lane {
                    Lane::Memory => ":memory:".to_owned(),
                    Lane::File | Lane::Prepared | Lane::Returning => {
                        dir.path().join("rowid.db").to_string_lossy().into_owned()
                    }
                };
                let conn = Connection::open(path).await.expect("fsqlite");
                for sql in SETUP.into_iter().chain(steps.iter().copied()) {
                    run_step(&stock, &conn, lane, sql).await;
                }
                for query in CHECKS {
                    assert_eq!(
                        frank_rows(&conn, query).await,
                        stock_rows(&stock, query),
                        "[{name}] {lane:?}: {query}"
                    );
                }
            }
        }
    });
}

/// The rows every two-writer schedule starts from.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// Rows 1..=3, short: one leaf holds them all.
    Short,
    /// Rows 1..=4, then 4 deleted, every row through the allocator lane: the
    /// shared allocator's tip sat above `max(rowid) + 1` before bd-hi7qg.
    ShortTipAboveMax,
    /// Rows 1..=3, each about a third of a page wide: the next insert splits
    /// the leaf.
    Wide,
}

impl Shape {
    fn pad(self) -> String {
        match self {
            Self::Short | Self::ShortTipAboveMax => String::new(),
            Self::Wide => "x".repeat(1300),
        }
    }
}

/// One step of a two-writer schedule. Writer 0 is A, writer 1 is B.
#[derive(Clone, Copy, Debug)]
enum Step {
    Begin(usize),
    /// `SELECT max(id)`: binds the writer's snapshot.
    Read(usize),
    /// A single-row implicit-rowid INSERT of the writer's marker: through
    /// `prepare` (the direct lane, `max(rowid) + 1` from its own cursor), or
    /// with `RETURNING id` through `query` (a VDBE program and the shared
    /// allocator).
    Insert {
        writer: usize,
        prepared: bool,
    },
    Commit(usize),
}

struct Writer {
    conn: Connection,
    marker: &'static str,
    in_txn: bool,
    /// A statement of the open transaction was refused: it can only end.
    doomed: bool,
    pending: Vec<&'static str>,
}

/// bd-hi7qg: the prepared single-row INSERT lane takes `max(rowid) + 1` from
/// its own cursor and never asks the shared allocator, so two writers on one
/// file can pick the same rowid there, on that lane alone or against an
/// allocator insert. That may end in a refused write (the leaf's page lock),
/// a refused write or COMMIT (first-committer-wins on the leaf), never in a
/// rowid stored twice or a committed row lost. Each schedule runs over short
/// rows, over a tip above the maximum, and over rows wide enough that the
/// insert splits the leaf; every committed marker must be stored exactly once,
/// every refused one not at all, and the rows before the schedule unchanged.
#[test]
fn two_writers_on_one_file_never_store_a_rowid_twice() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        use Step::{Begin, Commit, Insert, Read};
        let prepared = |writer: usize| Insert {
            writer,
            prepared: true,
        };
        let allocator = |writer: usize| Insert {
            writer,
            prepared: false,
        };
        let schedules: &[(&str, &[Step])] = &[
            (
                "a live peer transaction holds the leaf",
                &[Begin(0), prepared(0), prepared(1), Commit(0)],
            ),
            (
                "a commit after the snapshot bound",
                &[Begin(0), Read(0), prepared(1), prepared(0), Commit(0)],
            ),
            (
                "both open, both insert, both commit",
                &[
                    Begin(0),
                    Begin(1),
                    Read(0),
                    Read(1),
                    prepared(0),
                    prepared(1),
                    Commit(0),
                    Commit(1),
                ],
            ),
            (
                "both open, the later inserter commits first",
                &[
                    Begin(0),
                    Read(0),
                    Begin(1),
                    Read(1),
                    prepared(1),
                    Commit(1),
                    prepared(0),
                    Commit(0),
                ],
            ),
            (
                "an allocator insert held open against a prepared peer",
                &[Begin(0), allocator(0), prepared(1), Commit(0)],
            ),
            (
                "an allocator commit after a prepared writer's snapshot",
                &[Begin(0), Read(0), allocator(1), prepared(0), Commit(0)],
            ),
            (
                "a prepared commit after an allocator writer's snapshot",
                &[Begin(0), Read(0), prepared(1), allocator(0), Commit(0)],
            ),
            (
                "allocator writers: a live claim held open",
                &[Begin(0), allocator(0), allocator(1), Commit(0)],
            ),
            (
                "allocator writers: a commit after the snapshot bound",
                &[Begin(0), Read(0), allocator(1), allocator(0), Commit(0)],
            ),
        ];
        for &(name, steps) in schedules {
            for shape in [Shape::Short, Shape::ShortTipAboveMax, Shape::Wide] {
                run_two_writer_schedule(name, steps, shape).await;
            }
        }
    });
}

async fn run_two_writer_schedule(name: &str, steps: &[Step], shape: Shape) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lanes.db").to_string_lossy().into_owned();
    let pad = shape.pad();
    let setup = Connection::open(path.clone()).await.expect("setup");
    setup
        .execute("CREATE TABLE k(id INTEGER PRIMARY KEY, v TEXT, pad TEXT)")
        .await
        .expect("create");
    let mut markers = vec!["one", "two", "three"];
    if matches!(shape, Shape::ShortTipAboveMax) {
        markers.push("four");
    }
    for marker in markers {
        setup
            .query(&format!(
                "INSERT INTO k(v, pad) VALUES ('{marker}', '{pad}') RETURNING id"
            ))
            .await
            .expect("setup row");
    }
    if matches!(shape, Shape::ShortTipAboveMax) {
        setup.execute(DELETE_K_MAX).await.expect("delete max");
    }
    drop(setup);
    let mut writers = Vec::new();
    for marker in ["a", "b"] {
        let conn = Connection::open(path.clone()).await.expect("writer");
        conn.execute("PRAGMA busy_timeout = 0")
            .await
            .expect("busy_timeout");
        writers.push(Writer {
            conn,
            marker,
            in_txn: false,
            doomed: false,
            pending: Vec::new(),
        });
    }

    let mut committed: Vec<&'static str> = Vec::new();
    let mut attempted: Vec<&'static str> = Vec::new();
    let mut prepared_attempts = 0_u64;
    let mut outcomes = Vec::new();
    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();
    for &step in steps {
        match step {
            Step::Begin(w) => {
                writers[w].conn.execute("BEGIN").await.expect("begin");
                writers[w].in_txn = true;
            }
            Step::Read(w) => {
                writers[w]
                    .conn
                    .query("SELECT max(id) FROM k")
                    .await
                    .expect("read");
            }
            Step::Insert {
                writer: w,
                prepared,
            } => {
                let writer = &mut writers[w];
                if writer.doomed {
                    continue;
                }
                let sql = format!(
                    "INSERT INTO k(v, pad) VALUES ('{}', '{pad}')",
                    writer.marker
                );
                attempted.push(writer.marker);
                let result = if prepared {
                    prepared_attempts += 1;
                    match writer.conn.prepare(&sql).await {
                        Ok(statement) => statement.execute().await.map(|_| ()),
                        Err(error) => Err(error),
                    }
                } else {
                    writer
                        .conn
                        .query(&format!("{sql} RETURNING id"))
                        .await
                        .map(|_| ())
                };
                match result {
                    Ok(()) => {
                        let rowid = frank_rows(&writer.conn, "SELECT last_insert_rowid()").await;
                        outcomes.push(format!("{} took {rowid:?}", writer.marker));
                        if writer.in_txn {
                            writer.pending.push(writer.marker);
                        } else {
                            committed.push(writer.marker);
                        }
                    }
                    Err(error) => {
                        outcomes.push(format!("{} insert refused: {error}", writer.marker));
                        if writer.in_txn {
                            writer.doomed = true;
                        }
                    }
                }
            }
            Step::Commit(w) => {
                let writer = &mut writers[w];
                if writer.doomed {
                    let _ = writer.conn.execute("ROLLBACK").await;
                } else {
                    match writer.conn.execute("COMMIT").await {
                        Ok(_) => committed.append(&mut writer.pending),
                        Err(error) => {
                            outcomes.push(format!("{} commit refused: {error}", writer.marker));
                            let _ = writer.conn.execute("ROLLBACK").await;
                            writer.pending.clear();
                        }
                    }
                }
                writer.in_txn = false;
                writer.doomed = false;
            }
        }
    }
    let profile = hot_path_profile_snapshot();
    set_hot_path_profile_enabled(false);
    let label = format!("[{name}] {shape:?}");
    eprintln!("bd-hi7qg two writers {label}: committed {committed:?}; {outcomes:?}");
    assert_eq!(
        profile.prepared_direct_insert_executions, prepared_attempts,
        "{label}: every prepared INSERT, and nothing else, ran on the direct lane"
    );
    drop(writers);

    let check = Connection::open(path).await.expect("check");
    assert_eq!(
        frank_rows(&check, "PRAGMA integrity_check").await,
        vec![vec!["text:ok".to_owned()]],
        "{label}"
    );
    assert_eq!(
        frank_rows(&check, "SELECT count(*) = count(DISTINCT id) FROM k").await,
        vec![vec!["int:1".to_owned()]],
        "{label}: no rowid is stored twice"
    );
    assert_eq!(
        frank_rows(&check, "SELECT id, v FROM k WHERE id <= 3 ORDER BY id").await,
        vec![
            vec!["int:1".to_owned(), "text:one".to_owned()],
            vec!["int:2".to_owned(), "text:two".to_owned()],
            vec!["int:3".to_owned(), "text:three".to_owned()],
        ],
        "{label}: the rows before the schedule are intact"
    );
    for marker in &attempted {
        let expected = i64::from(committed.contains(marker));
        assert_eq!(
            frank_rows(
                &check,
                &format!("SELECT count(*) FROM k WHERE v = '{marker}'")
            )
            .await,
            vec![vec![format!("int:{expected}")]],
            "{label}: marker {marker} is stored {expected} times (committed {committed:?})"
        );
    }
    assert_eq!(
        frank_rows(&check, "SELECT count(*) FROM k").await,
        vec![vec![format!("int:{}", 3 + committed.len())]],
        "{label}: no other row appeared or vanished"
    );
}

/// `min(id)`, `max(id)`, `count(*)` and `last_insert_rowid()` after a round.
const ROUND_CHECK: &str = "SELECT min(id), max(id), count(*), last_insert_rowid() FROM u";

/// bd-mkon7's original shape (the blocking_vs_rusqlite_bench cross-check,
/// 2026-11-10): one connection on a file-backed WAL database empties `u`
/// with an unconditional DELETE, then inserts 20,000 rows through one
/// prepared `INSERT INTO u(a, b) VALUES (?1, ?2)` inside BEGIN ... COMMIT.
/// Stock restarts at rowid 1 every round (an emptied table's
/// `max(rowid) + 1`); fsqlite used to continue from the old high-water
/// (round 1: 40000 where stock says 20000). The same rounds run again with
/// `RETURNING id` through `query_with_params`, the allocator lane, and every
/// rowid it hands out is compared with stock's.
#[test]
fn an_emptied_table_restarts_prepared_rowids_at_one_like_stock() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        const SCHEMA: &str = "CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)";
        const CLEAR: &str = "DELETE FROM u";
        const INSERT: &str = "INSERT INTO u(a, b) VALUES (?1, ?2)";
        let dir = tempfile::tempdir().expect("tempdir");
        let stock = rusqlite::Connection::open(dir.path().join("stock.db")).expect("stock");
        stock
            .execute_batch(&format!("PRAGMA journal_mode = WAL; {SCHEMA};"))
            .expect("stock schema");
        let conn = Connection::open(dir.path().join("frank.db").to_string_lossy().into_owned())
            .await
            .expect("fsqlite");
        conn.execute("PRAGMA journal_mode = WAL")
            .await
            .expect("journal_mode");
        conn.execute(SCHEMA).await.expect("schema");
        for (lane, rows) in [("prepared execute", 20_000_i64), ("RETURNING", 2_000)] {
            for round in 0..3 {
                let label = format!("{lane}, round {round}");
                stock.execute_batch(CLEAR).expect("stock clear");
                conn.execute(CLEAR).await.expect("clear");
                stock.execute_batch("BEGIN").expect("stock begin");
                conn.execute("BEGIN").await.expect("begin");
                let statement = if lane == "RETURNING" {
                    format!("{INSERT} RETURNING id")
                } else {
                    INSERT.to_owned()
                };
                let mut stock_statement = stock.prepare(&statement).expect("stock prepare");
                let frank_statement = conn.prepare(&statement).await.expect("prepare");
                for i in 0..rows {
                    let params = [SqliteValue::Integer(i), SqliteValue::from("payload")];
                    if lane == "RETURNING" {
                        let expected: i64 = stock_statement
                            .query_row(rusqlite::params![i, "payload"], |row| row.get(0))
                            .expect("stock insert");
                        let taken = frank_statement
                            .query_with_params(&params)
                            .await
                            .expect("insert");
                        assert_eq!(
                            taken
                                .iter()
                                .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                                .collect::<Rows>(),
                            vec![vec![format!("int:{expected}")]],
                            "[{label}] the rowid of insert {i}"
                        );
                    } else {
                        stock_statement
                            .execute(rusqlite::params![i, "payload"])
                            .expect("stock insert");
                        frank_statement
                            .execute_with_params(&params)
                            .await
                            .expect("insert");
                    }
                }
                drop(stock_statement);
                drop(frank_statement);
                stock.execute_batch("COMMIT").expect("stock commit");
                conn.execute("COMMIT").await.expect("commit");
                assert_eq!(
                    frank_rows(&conn, ROUND_CHECK).await,
                    stock_rows(&stock, ROUND_CHECK),
                    "[{label}] min, max, count and last_insert_rowid()"
                );
            }
        }
        assert_eq!(
            frank_rows(&conn, "PRAGMA integrity_check").await,
            vec![vec!["text:ok".to_owned()]]
        );
    });
}
