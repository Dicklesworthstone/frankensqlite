#![recursion_limit = "512"]

//! bd-73pfl: a split that leaves the old right-edge leaf byte-identical must
//! conflict, across processes, with an append into that leaf.
//!
//! Eight fsqlite CLI processes ran 300 x `BEGIN; INSERT; COMMIT` each on one
//! table. About one run in 7-16 ended with stock `integrity_check` reporting
//! "Rowid N out of order". In every kept file, rowid N is stored twice by
//! two processes:
//! - in the last cell of the right-edge leaf P, written there by an append
//!   whose (smaller) cell fit;
//! - in the first cell of a new leaf Q, made by the peer's quick balance,
//!   whose cell did not fit. The root's divider for P is N-1.
//!
//! The quick balance caps P's keys without changing P's bytes. bd-ujp0h made
//! it claim P as a write conflict, but the claim lived only in the in-process
//! MVCC tables (page locks, commit index). Cross-process first-committer-wins
//! sees only staged pages and WAL frames, so a peer process never saw the
//! claim, in either commit order. In process the claim works. 250db23d7
//! restages P in file-backed transactions, and makes a frame of a new WAL
//! generation conflict even when its bytes equal the snapshot's.
//!
//! concurrent_writer_mvcc_oracle_e2e's
//! `cross_process_right_edge_split_conflicts_with_old_leaf_append_bd_73pfl`
//! drives a 3500-byte split from a stock-seeded, checkpointed (cold) WAL.
//! This file adds the bead's own shape on an fsqlite-built table (implicit
//! rowids, a quick balance between small cells), the warm WAL, and the
//! in-process arms.
//!
//! The deterministic shape: the table's right-edge leaf has 9 <= free < 161
//! bytes. X inserts a 9-byte cell (it fits and is appended in place); Y
//! inserts a 162-byte cell (a quick balance onto a new leaf). Both allocate
//! the same rowid from the same snapshot. One commits first and the other
//! second, in both orders and from two WAL states:
//! - warm: a prior commit left frames in the WAL, so everything happens in
//!   one WAL generation;
//! - cold: the WAL had no frames, so the first COMMIT writes a new WAL header
//!   and the second COMMIT takes the generation-change path.
//!
//! On 908c771bc (250db23d7's parent) all four cross-process combinations
//! committed both rows, and stock reported "Tree 2 page 11 cell 76: Rowid 422
//! out of order"; the in-process arms passed. Required:
//! the first committer commits, and the second COMMIT is refused with
//! BusySnapshot, which ends its transaction. The file stays intact, and the
//! durable rows are exactly the reported commits.
//!
//! The in-process arms run the same steps on two connections of one process,
//! then threads writing one table, each followed by a stock `integrity_check`.
//!
//! bd-1epn5: the splitter can also be a stock process. Stock's quick balance
//! never writes the old leaf, so no restaging on fsqlite's side can surface
//! it; the appender must notice that the route to its leaf changed. On main
//! a12e944af both stock-splitter arms committed both rows and stock reported
//! "Tree 2 page 11 cell 76: Rowid 422 out of order". The fix makes the leaf's
//! parent a route dependency of the append: a parent changed after the
//! snapshot refuses the COMMIT only when the current tree no longer routes the
//! appended key to the leaf. The middle-split arm is the other side: a stock
//! split under the same parent that leaves the leaf the right edge is no
//! conflict.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use fsqlite::{Connection, SqliteValue};

const CHILD_ENV: &str = "FSQLITE_SPLIT_CLAIM_CHILD";
const SQLITE_BUSY_SNAPSHOT: i32 = 517;
/// The appender's row: w = 0 is a zero-byte integer and s is empty, so the
/// cell (payload length, two-byte rowid, four-byte record) plus its pointer
/// takes 9 bytes.
const SMALL_INSERT: &str = "INSERT INTO e0(w, s) VALUES (0, '')";
/// The free-space window the fixture targets: the small cell fits, and the
/// large one (162 bytes with its pointer) does not.
const WINDOW: std::ops::Range<usize> = 9..161;
/// A WAL that `wal_checkpoint(TRUNCATE)` emptied keeps its 32-byte header.
const WAL_HEADER_BYTES: u64 = 32;

fn large_insert() -> String {
    format!("INSERT INTO e0(w, s) VALUES (2, '{}')", "y".repeat(150))
}

fn insert_sql(role: Role) -> String {
    match role {
        Role::Appender => SMALL_INSERT.to_owned(),
        Role::Splitter => large_insert(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// X: its cell fits the right-edge leaf.
    Appender,
    /// Y: its cell does not fit; a quick balance moves the right edge.
    Splitter,
}

impl Role {
    const fn name(self) -> &'static str {
        match self {
            Self::Appender => "appender",
            Self::Splitter => "splitter",
        }
    }

    fn parse(name: &str) -> Self {
        match name {
            "appender" => Self::Appender,
            "splitter" => Self::Splitter,
            other => panic!("unknown role {other}"),
        }
    }
}

/// Which transaction commits first. The other one begins and inserts first,
/// holds its transaction open across the peer's commit, then commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Order {
    SplitterFirst,
    AppenderFirst,
}

impl Order {
    const fn first(self) -> Role {
        match self {
            Self::SplitterFirst => Role::Splitter,
            Self::AppenderFirst => Role::Appender,
        }
    }

    const fn second(self) -> Role {
        match self {
            Self::SplitterFirst => Role::Appender,
            Self::AppenderFirst => Role::Splitter,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::SplitterFirst => "splitter-first",
            Self::AppenderFirst => "appender-first",
        }
    }

    fn parse(name: &str) -> Self {
        match name {
            "splitter-first" => Self::SplitterFirst,
            "appender-first" => Self::AppenderFirst,
            other => panic!("unknown order {other}"),
        }
    }
}

/// The engine of the splitter's process. The appender is always fsqlite: it
/// is the side whose stale snapshot can append past a new divider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    Fsqlite,
    /// bd-1epn5: stock's quick balance (`balance_quick`) moves the right edge
    /// off the leaf without staging the leaf at all: its commit carries the
    /// parent and the new leaf only, so no claim of the old leaf ever reaches
    /// the WAL for the appender to see.
    Stock,
}

impl Engine {
    const fn name(self) -> &'static str {
        match self {
            Self::Fsqlite => "fsqlite",
            Self::Stock => "stock",
        }
    }

    fn parse(name: &str) -> Self {
        match name {
            "fsqlite" => Self::Fsqlite,
            "stock" => Self::Stock,
            other => panic!("unknown engine {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalState {
    Cold,
    Warm,
}

impl WalState {
    const fn name(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Warm => "warm",
        }
    }

    fn parse(name: &str) -> Self {
        match name {
            "cold" => Self::Cold,
            "warm" => Self::Warm,
            other => panic!("unknown wal state {other}"),
        }
    }
}

/// The fixture's state once the right-edge leaf is in the window.
#[derive(Debug, Clone, Copy)]
struct Fixture {
    rows: i64,
    max_id: i64,
    /// Root page of `e0`.
    root: u32,
    /// `e0`'s right-edge leaf and its free bytes.
    leaf: u32,
    free: usize,
}

/// What one transaction (begin, insert, commit) reported.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Outcome {
    insert_error: Option<String>,
    /// `last_insert_rowid()` inside the transaction, after its INSERT.
    rowid: i64,
    /// `None` when COMMIT succeeded; otherwise its extended code and text.
    commit_error: Option<(i32, String)>,
    in_transaction_after_commit: bool,
}

impl Outcome {
    const fn committed(&self) -> bool {
        self.insert_error.is_none() && self.commit_error.is_none()
    }

    fn encode(&self) -> String {
        let commit = match &self.commit_error {
            None => "ok".to_owned(),
            Some((code, text)) => format!("{code}:{}", text.replace('\n', " ")),
        };
        format!(
            "insert={} rowid={} in_txn_after={} commit={commit}",
            self.insert_error
                .as_deref()
                .map_or_else(|| "ok".to_owned(), |e| e.replace([' ', '\n'], "_")),
            self.rowid,
            self.in_transaction_after_commit,
        )
    }

    fn decode(line: &str) -> Self {
        let (head, commit) = line
            .split_once(" commit=")
            .unwrap_or_else(|| panic!("malformed outcome {line}"));
        let field = |name: &str| {
            head.split_whitespace()
                .find_map(|f| f.strip_prefix(name))
                .unwrap_or_else(|| panic!("outcome lacks {name}: {line}"))
        };
        let insert = field("insert=");
        Self {
            insert_error: (insert != "ok").then(|| insert.to_owned()),
            rowid: field("rowid=").parse().expect("rowid"),
            in_transaction_after_commit: field("in_txn_after=") == "true",
            commit_error: (commit != "ok").then(|| {
                let (code, text) = commit.split_once(':').expect("code:text");
                (code.parse().expect("code"), text.to_owned())
            }),
        }
    }
}

async fn scalar(conn: &Connection, sql: &str) -> i64 {
    match conn.query(sql).await.expect("scalar query")[0].values()[0] {
        SqliteValue::Integer(value) => value,
        ref other => panic!("{sql} did not return an integer: {other:?}"),
    }
}

fn be16(page: &[u8], at: usize) -> usize {
    usize::from(u16::from_be_bytes([page[at], page[at + 1]]))
}

/// The right-edge leaf of the table rooted at `root` in the main database
/// file, and its free bytes. The caller checkpointed every frame into the
/// file. Appends leave no freeblocks, so the free space is the gap between
/// the cell pointer array and the cell content area, plus fragments.
fn right_edge_free(db: &Path, root: u32) -> (u32, usize) {
    let data = std::fs::read(db).expect("read database file");
    let page_size = match be16(&data, 16) {
        1 => 65536,
        size => size,
    };
    let mut page_no = root;
    loop {
        let start = (usize::try_from(page_no).expect("page number") - 1) * page_size;
        let page = &data[start..start + page_size];
        let cells = be16(page, 3);
        match page[0] {
            5 => {
                page_no = u32::from_be_bytes([page[8], page[9], page[10], page[11]]);
            }
            13 => {
                assert_eq!(be16(page, 1), 0, "an append-only leaf has no freeblocks");
                let content = match be16(page, 5) {
                    0 => 65536,
                    offset => offset,
                };
                return (page_no, content - (8 + 2 * cells) + usize::from(page[7]));
            }
            other => panic!("page {page_no} has b-tree type {other}"),
        }
    }
}

fn wal_bytes(db: &Path) -> u64 {
    let mut wal = db.as_os_str().to_owned();
    wal.push("-wal");
    std::fs::metadata(PathBuf::from(wal)).map_or(0, |meta| meta.len())
}

/// Build `e0` and `side` with fsqlite and fill `e0` until its right-edge
/// leaf has a free-space count inside [`WINDOW`]. Every step checkpoints
/// with TRUNCATE, so the main file holds every page when it is parsed and
/// the WAL is left with no frames.
async fn build_fixture(db: &Path) -> Fixture {
    let path = db.to_str().expect("utf8 path");
    let conn = Connection::open(path).await.expect("fixture open");
    for sql in [
        "PRAGMA journal_mode=WAL",
        "CREATE TABLE e0(id INTEGER PRIMARY KEY, w INTEGER, s TEXT)",
        "CREATE TABLE side(x)",
        "BEGIN",
    ] {
        conn.execute(sql).await.expect("fixture schema");
    }
    let filler = "f".repeat(20);
    for _ in 0..400 {
        conn.execute(&format!("INSERT INTO e0(w, s) VALUES (2, '{filler}')"))
            .await
            .expect("fixture rows");
    }
    conn.execute("COMMIT").await.expect("fixture commit");
    let root = u32::try_from(
        scalar(
            &conn,
            "SELECT rootpage FROM sqlite_master WHERE name = 'e0'",
        )
        .await,
    )
    .expect("root page");
    for _ in 0..200 {
        conn.query("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .expect("fixture checkpoint");
        assert!(
            wal_bytes(db) <= WAL_HEADER_BYTES,
            "TRUNCATE left WAL frames behind ({} bytes)",
            wal_bytes(db)
        );
        let (leaf, free) = right_edge_free(db, root);
        if WINDOW.contains(&free) {
            let fixture = Fixture {
                rows: scalar(&conn, "SELECT count(*) FROM e0").await,
                max_id: scalar(&conn, "SELECT max(id) FROM e0").await,
                root,
                leaf,
                free,
            };
            conn.close().await.expect("fixture close");
            return fixture;
        }
        // A filler row with w = 2 and an n-byte s takes n + 10 bytes with its
        // pointer (rowid >= 128). Leave about 40 bytes free; when the leaf
        // is already below the window, overflow it so a split starts a
        // fresh right edge.
        let take = if free < WINDOW.start {
            120
        } else {
            (free - 40).min(110)
        };
        conn.execute(&format!(
            "INSERT INTO e0(w, s) VALUES (2, '{}')",
            "f".repeat(take.saturating_sub(10))
        ))
        .await
        .expect("fixture filler");
    }
    panic!("the fixture never reached the free-space window");
}

/// One transaction: BEGIN; the role's INSERT; then, once `before_commit`
/// returns, COMMIT. Reports what each statement returned.
async fn transaction<F: std::future::Future<Output = ()>>(
    conn: &Connection,
    role: Role,
    before_commit: impl FnOnce() -> F,
) -> Outcome {
    transaction_with_insert(conn, &insert_sql(role), before_commit).await
}

/// [`transaction`] with the INSERT `insert`.
async fn transaction_with_insert<F: std::future::Future<Output = ()>>(
    conn: &Connection,
    insert: &str,
    before_commit: impl FnOnce() -> F,
) -> Outcome {
    conn.execute("BEGIN").await.expect("BEGIN");
    let insert_error = conn
        .execute(insert)
        .await
        .err()
        .map(|error| error.to_string());
    let rowid = scalar(conn, "SELECT last_insert_rowid()").await;
    before_commit().await;
    let commit_error = conn
        .execute("COMMIT")
        .await
        .err()
        .map(|error| (error.extended_error_code(), error.to_string()));
    Outcome {
        insert_error,
        rowid,
        commit_error,
        in_transaction_after_commit: conn.in_transaction(),
    }
}

/// [`transaction`] on a stock connection: BEGIN; the role's INSERT;
/// `last_insert_rowid()`; COMMIT.
fn stock_transaction(conn: &rusqlite::Connection, role: Role) -> Outcome {
    conn.execute_batch("BEGIN").expect("stock BEGIN");
    let insert_error = conn
        .execute(&insert_sql(role), [])
        .err()
        .map(|error| error.to_string());
    let rowid = conn
        .query_row("SELECT last_insert_rowid()", [], |row| row.get(0))
        .expect("stock last_insert_rowid");
    let commit_error = conn.execute_batch("COMMIT").err().map(|error| {
        let code = error.sqlite_error().map_or(-1, |error| error.extended_code);
        (code, error.to_string())
    });
    Outcome {
        insert_error,
        rowid,
        commit_error,
        in_transaction_after_commit: !conn.is_autocommit(),
    }
}

fn wait_for(path: &Path) {
    let started = Instant::now();
    while !path.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The child process body; a no-op unless the parent set `CHILD_ENV`.
///
/// Protocol (files in the work dir): both children open, set busy_timeout
/// and create `ready-<role>`. On `go`, the second committer (warm: after a
/// commit to `side`) begins and inserts, then creates `held`. The first
/// committer waits for `held`, runs its whole transaction, and creates
/// `first-done`; the second then commits. A stock splitter is always the
/// first committer.
#[test]
fn split_claim_child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let fields: Vec<&str> = spec.split('|').collect();
    let [role, order, wal, db, dir, splitter_engine] = fields[..] else {
        panic!("malformed child spec {spec}");
    };
    let (role, order, wal) = (Role::parse(role), Order::parse(order), WalState::parse(wal));
    let (db, dir) = (db.to_owned(), PathBuf::from(dir));
    if role == Role::Splitter && Engine::parse(splitter_engine) == Engine::Stock {
        assert_eq!(
            order,
            Order::SplitterFirst,
            "a stock splitter commits first"
        );
        let conn = rusqlite::Connection::open(&db).expect("stock child open");
        conn.busy_timeout(Duration::from_millis(2000))
            .expect("stock busy_timeout");
        std::fs::write(dir.join(format!("ready-{}", role.name())), b"").expect("ready");
        wait_for(&dir.join("go"));
        wait_for(&dir.join("held"));
        let outcome = stock_transaction(&conn, role);
        std::fs::write(dir.join("first-done"), b"").expect("first-done");
        // Stay open until the appender has exited, so this close is never the
        // last one (which would checkpoint and reset the WAL under it).
        wait_for(&dir.join("appender-exited"));
        drop(conn);
        println!("event=split_claim_outcome {}", outcome.encode());
        return;
    }
    let outcome = fsqlite_e2e::block_on(async move {
        let conn = Connection::open(&db).await.expect("child open");
        conn.execute("PRAGMA busy_timeout=2000")
            .await
            .expect("busy_timeout");
        std::fs::write(dir.join(format!("ready-{}", role.name())), b"").expect("ready");
        wait_for(&dir.join("go"));
        let outcome = if role == order.second() {
            if wal == WalState::Warm {
                conn.execute("INSERT INTO side VALUES (1)")
                    .await
                    .expect("warm the WAL");
            }
            let held = dir.join("held");
            let first_done = dir.join("first-done");
            transaction(&conn, role, || async move {
                std::fs::write(&held, b"").expect("held");
                wait_for(&first_done);
            })
            .await
        } else {
            wait_for(&dir.join("held"));
            let outcome = transaction(&conn, role, || async {}).await;
            std::fs::write(dir.join("first-done"), b"").expect("first-done");
            outcome
        };
        conn.close().await.expect("child close");
        outcome
    });
    println!("event=split_claim_outcome {}", outcome.encode());
}

fn child_outcome(output: &std::process::Output, role: Role) -> Outcome {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let marker = "event=split_claim_outcome ";
    let line = stdout
        .lines()
        .find_map(|line| line.find(marker).map(|at| &line[at + marker.len()..]))
        .unwrap_or_else(|| {
            panic!(
                "{} child produced no outcome (status {:?}):\n{stdout}\n{}",
                role.name(),
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            )
        });
    Outcome::decode(line)
}

/// The rows a stock connection reads past the fixture, and stock's verdicts.
struct Durable {
    rows: i64,
    duplicate_ids: i64,
    /// `w` of every row past the fixture's max id, in id order.
    new_w: Vec<i64>,
    integrity: Vec<String>,
}

fn stock_durable(db: &Path, fixture: &Fixture) -> Durable {
    let stock = rusqlite::Connection::open(db).expect("stock open");
    let (rows, duplicate_ids) = stock
        .query_row(
            "SELECT count(*), count(*) - count(DISTINCT id) FROM e0",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("counts");
    let new_w = stock
        .prepare("SELECT w FROM e0 WHERE id > ?1 ORDER BY id")
        .expect("prepare")
        .query_map([fixture.max_id], |row| row.get(0))
        .expect("query")
        .collect::<rusqlite::Result<_>>()
        .expect("new rows");
    let integrity = stock
        .prepare("PRAGMA integrity_check")
        .expect("prepare integrity")
        .query_map([], |row| row.get(0))
        .expect("integrity")
        .collect::<rusqlite::Result<_>>()
        .expect("integrity rows");
    Durable {
        rows,
        duplicate_ids,
        new_w,
        integrity,
    }
}

const fn role_w(role: Role) -> i64 {
    match role {
        Role::Appender => 0,
        Role::Splitter => 2,
    }
}

/// The verdict shared by every arm: the file is intact and the durable rows
/// past the fixture are exactly the transactions that reported a commit.
fn assert_durable_matches(
    label: &str,
    db: &Path,
    fixture: &Fixture,
    outcomes: &[(Role, &Outcome)],
) {
    let durable = stock_durable(db, fixture);
    let reported: Vec<i64> = outcomes
        .iter()
        .filter(|(_, outcome)| outcome.committed())
        .map(|(role, _)| role_w(*role))
        .collect();
    assert_eq!(
        durable.integrity,
        vec!["ok".to_owned()],
        "{label}: stock integrity_check (outcomes {outcomes:?})"
    );
    assert_eq!(durable.duplicate_ids, 0, "{label}: duplicate rowids");
    assert_eq!(
        durable.rows,
        fixture.rows + i64::try_from(reported.len()).expect("count"),
        "{label}: durable rows == fixture + reported commits ({outcomes:?})"
    );
    let mut durable_w = durable.new_w;
    let mut reported_w = reported;
    durable_w.sort_unstable();
    reported_w.sort_unstable();
    assert_eq!(
        durable_w, reported_w,
        "{label}: the durable new rows are exactly the reported commits"
    );
}

/// Both transactions really raced: each INSERT succeeded and took the same
/// rowid from the same snapshot, and the committed tree shows which cell
/// split the leaf. The splitter's commit moves `e0`'s right edge off the
/// fixture's leaf; the appender's 9-byte cell, committed alone, stays on it.
/// The fixture's build leaves pages on the freelist, and the split takes its
/// new leaf from there, so the page count does not change. The check reads
/// the checkpointed main file instead.
fn assert_raced(label: &str, db: &Path, fixture: &Fixture, appender: &Outcome, splitter: &Outcome) {
    assert!(
        appender.insert_error.is_none() && splitter.insert_error.is_none(),
        "{label}: both INSERTs must succeed: {appender:?} {splitter:?}"
    );
    assert_eq!(
        (appender.rowid, splitter.rowid),
        (fixture.max_id + 1, fixture.max_id + 1),
        "{label}: non-vacuity: both transactions allocate rowid max+1 from one snapshot"
    );
    let stock = rusqlite::Connection::open(db).expect("stock open");
    let (busy, _, _): (i64, i64, i64) = stock
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("stock checkpoint");
    assert_eq!(busy, 0, "{label}: the verification checkpoint completed");
    drop(stock);
    let (edge, edge_free) = right_edge_free(db, fixture.root);
    if splitter.committed() {
        assert_ne!(
            edge, fixture.leaf,
            "{label}: non-vacuity: the 162-byte cell did not fit leaf {} ({} bytes free), so \
             its commit moves the right edge to a new leaf",
            fixture.leaf, fixture.free
        );
    } else if appender.committed() {
        assert_eq!(
            (edge, edge_free),
            (fixture.leaf, fixture.free - WINDOW.start),
            "{label}: non-vacuity: the 9-byte cell fits leaf {} in place",
            fixture.leaf
        );
    }
}

fn cross_process_case(wal: WalState, order: Order, splitter_engine: Engine) {
    let label = format!(
        "cross-process {} {} {}-splitter",
        wal.name(),
        order.name(),
        splitter_engine.name()
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("split.db");
    let fixture = fsqlite_e2e::block_on(build_fixture(&db));
    let exe = std::env::current_exe().expect("test binary path");
    let children: Vec<(Role, std::process::Child)> = [Role::Appender, Role::Splitter]
        .into_iter()
        .map(|role| {
            let child = Command::new(&exe)
                .args([
                    "--exact",
                    "split_claim_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(
                    CHILD_ENV,
                    format!(
                        "{}|{}|{}|{}|{}|{}",
                        role.name(),
                        order.name(),
                        wal.name(),
                        db.display(),
                        dir.path().display(),
                        splitter_engine.name()
                    ),
                )
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn child");
            (role, child)
        })
        .collect();
    for role in [Role::Appender, Role::Splitter] {
        wait_for(&dir.path().join(format!("ready-{}", role.name())));
    }
    std::fs::write(dir.path().join("go"), b"go").expect("go");
    // Each child waits on the other, so both children's pipes are drained at
    // once: one blocked on a full pipe would otherwise stall the pair.
    let draining: Vec<_> = children
        .into_iter()
        .map(|(role, child)| (role, std::thread::spawn(|| child.wait_with_output())))
        .collect();
    let mut appender = None;
    let mut splitter = None;
    for (role, drain) in draining {
        let output = drain.join().expect("drain thread").expect("child output");
        let outcome = child_outcome(&output, role);
        match role {
            Role::Appender => {
                appender = Some(outcome);
                std::fs::write(dir.path().join("appender-exited"), b"").expect("appender-exited");
            }
            Role::Splitter => splitter = Some(outcome),
        }
    }
    let (appender, splitter) = (appender.expect("appender"), splitter.expect("splitter"));
    eprintln!(
        "event=split_claim_case label={label:?} fixture={fixture:?} appender={} splitter={}",
        appender.encode(),
        splitter.encode()
    );
    assert_durable_matches(
        &label,
        &db,
        &fixture,
        &[(Role::Appender, &appender), (Role::Splitter, &splitter)],
    );
    assert_raced(&label, &db, &fixture, &appender, &splitter);
    let (first, second) = match order {
        Order::SplitterFirst => (&splitter, &appender),
        Order::AppenderFirst => (&appender, &splitter),
    };
    assert!(
        first.committed(),
        "{label}: the first committer commits: {first:?}"
    );
    let refused = second
        .commit_error
        .as_ref()
        .unwrap_or_else(|| panic!("{label}: the second COMMIT must be refused: {second:?}"));
    assert_eq!(
        refused.0, SQLITE_BUSY_SNAPSHOT,
        "{label}: the second COMMIT is refused with BusySnapshot: {refused:?}"
    );
    assert!(
        !second.in_transaction_after_commit,
        "{label}: the refused COMMIT ends its transaction"
    );
}

#[test]
fn cross_process_split_claim_warm_splitter_first() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    cross_process_case(WalState::Warm, Order::SplitterFirst, Engine::Fsqlite);
}

#[test]
fn cross_process_split_claim_warm_appender_first() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    cross_process_case(WalState::Warm, Order::AppenderFirst, Engine::Fsqlite);
}

#[test]
fn cross_process_split_claim_cold_splitter_first() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    cross_process_case(WalState::Cold, Order::SplitterFirst, Engine::Fsqlite);
}

#[test]
fn cross_process_split_claim_cold_appender_first() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    cross_process_case(WalState::Cold, Order::AppenderFirst, Engine::Fsqlite);
}

/// bd-1epn5: the splitter is a stock process. Its quick balance commits the
/// root (the new divider and right child) and the new leaf, and never the old
/// right-edge leaf, so the old leaf has no frame for the fsqlite appender's
/// first-committer-wins check to find. The appender's insert depended on the
/// root routing its rowid to that leaf, so its COMMIT must still be refused.
#[test]
fn cross_process_split_claim_warm_stock_splitter_first() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    cross_process_case(WalState::Warm, Order::SplitterFirst, Engine::Stock);
}

#[test]
fn cross_process_split_claim_cold_stock_splitter_first() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    cross_process_case(WalState::Cold, Order::SplitterFirst, Engine::Stock);
}

const MIDDLE_SPLIT_CHILD_ENV: &str = "FSQLITE_MIDDLE_SPLIT_CHILD";
/// The middle-split fixture's 1000-byte rows: four fill a 4 KiB leaf.
const MIDDLE_SPLIT_SEED_IDS: std::ops::RangeInclusive<i64> = 10..=140;
const MIDDLE_SPLIT_APPEND: &str = "INSERT INTO g(payload) VALUES (zeroblob(16))";

/// The `(left child, divider)` cells and the right child of the table
/// interior page `page_no` in the main database file. Dividers here are below
/// 128, one-byte varints.
fn interior_shape(db: &Path, page_no: u32) -> (Vec<(u32, u8)>, u32) {
    let data = std::fs::read(db).expect("read database file");
    let start = (usize::try_from(page_no).expect("page") - 1) * 4096;
    let page = &data[start..start + 4096];
    assert_eq!(page[0], 0x05, "page {page_no} is a table interior page");
    let cells = (0..be16(page, 3))
        .map(|index| {
            let cell = be16(page, 12 + 2 * index);
            let child = u32::from_be_bytes(page[cell..cell + 4].try_into().expect("child"));
            assert!(page[cell + 4] < 0x80, "a one-byte divider");
            (child, page[cell + 4])
        })
        .collect();
    (
        cells,
        u32::from_be_bytes(page[8..12].try_into().expect("right child")),
    )
}

/// The fsqlite appender of
/// [`cross_process_middle_split_keeps_a_right_edge_append_bd_1epn5`]: it
/// appends a 16-byte row, creates `staged`, commits once `commit` exists, and
/// prints its outcome.
#[test]
fn middle_split_appender_child() {
    let Ok(spec) = std::env::var(MIDDLE_SPLIT_CHILD_ENV) else {
        return;
    };
    let (db, dir) = spec.split_once('|').expect("db|dir");
    let (db, dir) = (db.to_owned(), PathBuf::from(dir));
    let outcome = fsqlite_e2e::block_on(async move {
        let conn = Connection::open(&db).await.expect("appender open");
        conn.execute("PRAGMA busy_timeout=2000")
            .await
            .expect("busy_timeout");
        let outcome = transaction_with_insert(&conn, MIDDLE_SPLIT_APPEND, || async {
            std::fs::write(dir.join("staged"), b"").expect("staged");
            wait_for(&dir.join("commit"));
        })
        .await;
        conn.close().await.expect("appender close");
        outcome
    });
    println!("event=middle_split_outcome {}", outcome.encode());
}

/// bd-1epn5, the other side of the fix: a changed parent alone is no
/// conflict. The root has four leaves, A B C and the right edge L. An fsqlite
/// process appends a row to L and holds its transaction; a stock process
/// inserts into the full leaf B, whose split rebalances A B C and rewrites
/// the root, but leaves L as the root's right child and leaves L's bytes
/// alone. The append's route to L still holds, so it commits: stock's commit
/// touched no page the append wrote, and nothing moved L's keys.
#[test]
fn cross_process_middle_split_keeps_a_right_edge_append_bd_1epn5() {
    if std::env::var_os(CHILD_ENV).is_some() || std::env::var_os(MIDDLE_SPLIT_CHILD_ENV).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("middle.db");
    let stock = rusqlite::Connection::open(&db).expect("stock open");
    stock
        .execute_batch(
            "PRAGMA page_size=4096; PRAGMA journal_mode=WAL;
             CREATE TABLE g(id INTEGER PRIMARY KEY, payload BLOB);",
        )
        .expect("schema");
    for id in MIDDLE_SPLIT_SEED_IDS.step_by(10) {
        stock
            .execute(
                "INSERT INTO g(id, payload) VALUES (?1, zeroblob(1000))",
                [id],
            )
            .expect("seed row");
    }
    stock
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("seed checkpoint");
    let (seed_cells, edge) = interior_shape(&db, 2);
    assert_eq!(
        seed_cells
            .iter()
            .map(|(_, divider)| *divider)
            .collect::<Vec<_>>(),
        vec![40, 80, 120],
        "fixture: leaves A B C of four rows and the right edge L"
    );

    let mut child = Command::new(std::env::current_exe().expect("test binary path"))
        .args([
            "--exact",
            "middle_split_appender_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(
            MIDDLE_SPLIT_CHILD_ENV,
            format!("{}|{}", db.display(), dir.path().display()),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn appender");
    let started = Instant::now();
    while !dir.path().join("staged").exists() {
        assert!(
            child.try_wait().expect("appender status").is_none(),
            "the appender exited before staging its row"
        );
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "the appender never staged its row"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    stock
        .execute_batch("BEGIN; INSERT INTO g(id, payload) VALUES (55, zeroblob(1000)); COMMIT;")
        .expect("stock splits the middle leaf B");
    std::fs::write(dir.path().join("commit"), b"").expect("commit");
    let output = child.wait_with_output().expect("appender output");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let marker = "event=middle_split_outcome ";
    let appender = stdout
        .lines()
        .find_map(|line| {
            line.find(marker)
                .map(|at| Outcome::decode(&line[at + marker.len()..]))
        })
        .unwrap_or_else(|| {
            panic!(
                "the appender produced no outcome (status {:?}):\n{stdout}\n{}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            )
        });
    eprintln!("event=middle_split_case appender={}", appender.encode());
    assert!(
        appender.committed() && appender.rowid == 141,
        "the right-edge append commits after a split that left its route alone: {appender:?}"
    );

    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    assert_eq!(integrity, "ok", "stock integrity_check");
    let ids: Vec<i64> = stock
        .prepare("SELECT id FROM g ORDER BY id")
        .expect("prepare")
        .query_map([], |row| row.get(0))
        .expect("ids")
        .collect::<rusqlite::Result<_>>()
        .expect("ids");
    let mut expected: Vec<i64> = MIDDLE_SPLIT_SEED_IDS.step_by(10).collect();
    expected.extend([55, 141]);
    expected.sort_unstable();
    assert_eq!(ids, expected, "both commits are durable");
    stock
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("verification checkpoint");
    let (cells, right_child) = interior_shape(&db, 2);
    assert!(
        cells.len() > seed_cells.len() && right_child == edge,
        "non-vacuity: the split rewrote the root ({seed_cells:?} -> {cells:?}) and L ({edge}) \
         stayed its right child ({right_child})"
    );
}

/// The same steps on two connections of one process. The in-process page
/// lock serializes the two writers of the leaf: the second actor's INSERT
/// (or its COMMIT) fails, never both commits.
#[test]
fn in_process_split_claim_two_connections() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    for order in [Order::SplitterFirst, Order::AppenderFirst] {
        let label = format!("in-process {}", order.name());
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("split.db");
        let path = db.to_str().expect("utf8 path").to_owned();
        let (fixture, appender, splitter) = fsqlite_e2e::block_on(async {
            let fixture = build_fixture(&db).await;
            let first = Connection::open(&path).await.expect("open first");
            let second = Connection::open(&path).await.expect("open second");
            for conn in [&first, &second] {
                conn.execute("PRAGMA busy_timeout=200")
                    .await
                    .expect("busy_timeout");
            }
            second.execute("BEGIN").await.expect("BEGIN second");
            let second_insert = second
                .execute(&insert_sql(order.second()))
                .await
                .err()
                .map(|error| error.to_string());
            let second_rowid = scalar(&second, "SELECT last_insert_rowid()").await;
            let first_outcome = transaction(&first, order.first(), || async {}).await;
            let second_commit = second
                .execute("COMMIT")
                .await
                .err()
                .map(|error| (error.extended_error_code(), error.to_string()));
            let second_outcome = Outcome {
                insert_error: second_insert,
                rowid: second_rowid,
                commit_error: second_commit,
                in_transaction_after_commit: second.in_transaction(),
            };
            if second.in_transaction() {
                second.execute("ROLLBACK").await.expect("ROLLBACK second");
            }
            first.close().await.expect("close first");
            second.close().await.expect("close second");
            match order {
                Order::SplitterFirst => (fixture, second_outcome, first_outcome),
                Order::AppenderFirst => (fixture, first_outcome, second_outcome),
            }
        });
        eprintln!(
            "event=split_claim_case label={label:?} fixture={fixture:?} appender={} splitter={}",
            appender.encode(),
            splitter.encode()
        );
        assert_durable_matches(
            &label,
            &db,
            &fixture,
            &[(Role::Appender, &appender), (Role::Splitter, &splitter)],
        );
        assert!(
            !(appender.committed() && splitter.committed()),
            "{label}: both writers of the leaf committed: {appender:?} {splitter:?}"
        );
    }
}

/// Threads of one process, each with its own connection, running explicit
/// `BEGIN; INSERT; COMMIT` transactions on one table with cell sizes that
/// differ per thread (so an append fits where a peer's split does not).
/// Every reported commit is durable, nothing else is, and the file is intact.
#[test]
fn in_process_threads_split_one_table_in_explicit_transactions() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    const THREADS: usize = 8;
    const TXNS: usize = 200;
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("threads.db");
    let path = db.to_str().expect("utf8 path").to_owned();
    fsqlite_e2e::block_on(async {
        let conn = Connection::open(&path).await.expect("open");
        conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        conn.execute("CREATE TABLE e0(id INTEGER PRIMARY KEY, w INTEGER, s TEXT)")
            .await
            .expect("schema");
        conn.close().await.expect("close");
    });
    let barrier = Arc::new(Barrier::new(THREADS));
    let workers: Vec<_> = (0..THREADS)
        .map(|worker| {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                fsqlite_e2e::block_on(async move {
                    let conn = Connection::open(&path).await.expect("worker open");
                    conn.execute("PRAGMA busy_timeout=5000")
                        .await
                        .expect("busy_timeout");
                    barrier.wait();
                    let mut committed = Vec::new();
                    let mut refused = 0usize;
                    for seq in 0..TXNS {
                        let pad = "p".repeat((worker * 7 + seq) % 23);
                        let begin = conn.execute("BEGIN").await;
                        let insert = conn
                            .execute(&format!(
                                "INSERT INTO e0(w, s) VALUES ({worker}, '{worker}-{seq}-{pad}')"
                            ))
                            .await;
                        let commit = conn.execute("COMMIT").await;
                        if begin.is_ok() && insert.is_ok() && commit.is_ok() {
                            committed.push(format!("{worker}-{seq}-{pad}"));
                        } else {
                            refused += 1;
                            if conn.in_transaction() {
                                conn.execute("ROLLBACK").await.expect("ROLLBACK");
                            }
                        }
                    }
                    conn.close().await.expect("worker close");
                    (committed, refused)
                })
            })
        })
        .collect();
    let mut reported = Vec::new();
    let mut refused = 0;
    for worker in workers {
        let (committed, worker_refused) = worker.join().expect("worker panicked");
        reported.extend(committed);
        refused += worker_refused;
    }
    let stock = rusqlite::Connection::open(&db).expect("stock open");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    let duplicate_ids: i64 = stock
        .query_row("SELECT count(*) - count(DISTINCT id) FROM e0", [], |row| {
            row.get(0)
        })
        .expect("duplicates");
    let mut durable: Vec<String> = stock
        .prepare("SELECT s FROM e0")
        .expect("prepare")
        .query_map([], |row| row.get(0))
        .expect("query")
        .collect::<rusqlite::Result<_>>()
        .expect("rows");
    eprintln!(
        "event=split_claim_threads committed={} refused={refused} durable={} integrity={integrity}",
        reported.len(),
        durable.len()
    );
    assert_eq!(integrity, "ok", "in-process threads: stock integrity_check");
    assert_eq!(duplicate_ids, 0, "in-process threads: duplicate rowids");
    reported.sort_unstable();
    durable.sort_unstable();
    assert_eq!(
        durable, reported,
        "in-process threads: the durable rows are exactly the reported commits"
    );
}
