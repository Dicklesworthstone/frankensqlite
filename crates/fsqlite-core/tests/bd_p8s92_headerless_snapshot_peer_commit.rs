#![recursion_limit = "512"]

//! bd-p8s92 probe: a transaction whose snapshot is a header-less WAL (the
//! bd-yxqs9 observation) must not commit a page over a peer's committed,
//! uncheckpointed frame of the same page.
//!
//! Each case:
//! - stock SQLite (rusqlite) builds a WAL database, table
//!   `t(id INTEGER PRIMARY KEY, v INTEGER)` with rows 1..=5 (v = id) on one
//!   leaf, then runs `wal_checkpoint(TRUNCATE)` and stays open: the WAL is
//!   zero bytes beside a live `-shm`, which fsqlite binds header-less
//!   (26950fa4c, .5.6);
//! - connection A opens (normally, or schema-only), begins (CONCURRENT or
//!   IMMEDIATE) and reads;
//! - peer B (that stock connection, or a second fsqlite connection) commits a
//!   change to page 1 (`user_version = 7`) or to the leaf (row 2, v + 100);
//! - A writes only that same page (`application_id = 8` on page 1, or row 1
//!   v + 1 on the leaf) and commits.
//!
//! Stock serializes this: A's COMMIT is refused, or A's write lands on top of
//! B's. B's change must never be lost. Stock reads the final file.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug)]
enum Peer {
    Stock,
    Fsqlite,
}

#[derive(Clone, Copy, Debug)]
enum Target {
    PageOne,
    Leaf,
}

#[derive(Clone, Copy, Debug)]
enum Open {
    Normal,
    SchemaOnly,
    /// The bd-70qrk negative's shape: an fsqlite seed in DELETE mode whose
    /// header is then switched to WAL, opened schema-only, so the WAL is
    /// header-less beside a fresh, unindexed `-shm` (no stock restart).
    FsqliteSeedSchemaOnly,
}

fn wal_path(db: &Path) -> PathBuf {
    let mut os = db.as_os_str().to_os_string();
    os.push("-wal");
    PathBuf::from(os)
}

/// The fixture, left as stock leaves it after `wal_checkpoint(TRUNCATE)`; the
/// returned connection keeps the `-shm` alive.
fn build(db: &Path) -> rusqlite::Connection {
    let stock = rusqlite::Connection::open(db).expect("create fixture");
    let mode: String = stock
        .query_row("PRAGMA journal_mode = WAL;", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    stock
        .execute_batch(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
             INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);",
        )
        .unwrap();
    let busy: i64 = stock
        .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |r| r.get(0))
        .unwrap();
    assert_eq!(busy, 0);
    assert_eq!(std::fs::metadata(wal_path(db)).unwrap().len(), 0);
    stock.busy_timeout(std::time::Duration::ZERO).unwrap();
    stock
}

/// The bd-70qrk negative's fixture without its freelist damage: fsqlite
/// writes the table in DELETE mode, then the header's journal bytes are
/// switched to WAL with no `-wal` file.
async fn build_fsqlite_seed(db: &Path) {
    let db_str = db.to_string_lossy().into_owned();
    let seed = Connection::open(&db_str).await.unwrap();
    seed.execute("PRAGMA journal_mode=DELETE;").await.unwrap();
    seed.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);")
        .await
        .unwrap();
    seed.execute("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);")
        .await
        .unwrap();
    seed.close().await.unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(db)
        .unwrap();
    let mut header = [0_u8; 100];
    std::io::Read::read_exact(&mut file, &mut header).unwrap();
    header[18] = 2;
    header[19] = 2;
    std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(0)).unwrap();
    std::io::Write::write_all(&mut file, &header).unwrap();
    file.sync_all().unwrap();
}

fn scalar(rows: &[Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected an integer, got {other:?}"),
    }
}

/// B's commit inside A's window; whether it committed.
async fn peer_commits(peer: Peer, stock: &rusqlite::Connection, db_str: &str, sql: &str) -> bool {
    match peer {
        Peer::Stock => match stock.execute_batch(sql) {
            Ok(()) => true,
            Err(error) => {
                eprintln!("bd-p8s92 stock peer refused: {error}");
                false
            }
        },
        Peer::Fsqlite => {
            let b = Connection::open(db_str).await.unwrap();
            let committed = match b.execute(sql).await {
                Ok(_) => true,
                Err(error) => {
                    eprintln!("bd-p8s92 fsqlite peer refused: {error:?}");
                    false
                }
            };
            b.close().await.unwrap();
            committed
        }
    }
}

async fn run_case(open: Open, begin: &str, peer: Peer, target: Target) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("p8s92.db");
    let db_str = db.to_string_lossy().into_owned();
    let stock = match open {
        Open::FsqliteSeedSchemaOnly => {
            build_fsqlite_seed(&db).await;
            None
        }
        Open::Normal | Open::SchemaOnly => Some(build(&db)),
    };

    let a = match open {
        Open::Normal => Connection::open(&db_str).await.unwrap(),
        Open::SchemaOnly | Open::FsqliteSeedSchemaOnly => {
            Connection::open_existing_schema_only(&db_str)
                .await
                .unwrap()
        }
    };
    a.execute(begin).await.unwrap();
    assert_eq!(
        scalar(&a.query("SELECT v FROM t WHERE id = 1;").await.unwrap()),
        1
    );
    let wal_len = std::fs::metadata(wal_path(&db)).map_or(0, |m| m.len());
    assert!(
        wal_len < 32,
        "the shape needs a header-less WAL at A's snapshot; the WAL is {wal_len} bytes"
    );

    let stock = stock.unwrap_or_else(|| {
        let stock = rusqlite::Connection::open(&db).unwrap();
        stock.busy_timeout(std::time::Duration::ZERO).unwrap();
        stock
    });
    let peer_sql = match target {
        Target::PageOne => "PRAGMA user_version = 7;",
        Target::Leaf => "UPDATE t SET v = v + 100 WHERE id = 2;",
    };
    let b_committed = peer_commits(peer, &stock, &db_str, peer_sql).await;

    let a_sql = match target {
        Target::PageOne => "PRAGMA application_id = 8;",
        Target::Leaf => "UPDATE t SET v = v + 1 WHERE id = 1;",
    };
    let a_commit: Result<(), String> = match a.execute(a_sql).await {
        Ok(_) => a
            .execute("COMMIT;")
            .await
            .map(|_| ())
            .map_err(|error| format!("commit: {error:?}")),
        Err(error) => Err(format!("write: {error:?}")),
    };
    let a_committed = a_commit.is_ok();
    if a.in_transaction() {
        a.execute("ROLLBACK;").await.unwrap();
    }
    a.close().await.unwrap();

    let (b_value, a_value): (i64, i64) = match target {
        Target::PageOne => (
            stock
                .query_row("PRAGMA user_version;", [], |r| r.get(0))
                .unwrap(),
            stock
                .query_row("PRAGMA application_id;", [], |r| r.get(0))
                .unwrap(),
        ),
        Target::Leaf => (
            stock
                .query_row("SELECT v FROM t WHERE id = 2;", [], |r| r.get(0))
                .unwrap(),
            stock
                .query_row("SELECT v FROM t WHERE id = 1;", [], |r| r.get(0))
                .unwrap(),
        ),
    };
    let integrity: String = stock
        .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
        .unwrap();
    let (b_expected, a_expected) = match target {
        Target::PageOne => (7, 8),
        Target::Leaf => (102, 2),
    };
    let outcome = format!(
        "open={open:?} begin={begin} peer={peer:?} target={target:?} b_committed={b_committed} \
         a_commit={a_commit:?} final_b={b_value} final_a={a_value} integrity={integrity}"
    );
    eprintln!("bd-p8s92 {outcome}");
    assert_eq!(integrity, "ok", "{outcome}");
    if b_committed {
        assert_eq!(
            b_value, b_expected,
            "LOST UPDATE: B's committed change is gone: {outcome}"
        );
    }
    if a_committed {
        assert_eq!(
            a_value, a_expected,
            "A's committed change is missing: {outcome}"
        );
    }
}

/// One test per shape; the doc comment before each name says what it guards.
macro_rules! case {
    ($(#[$doc:meta])* $name:ident, $open:expr, $begin:expr, $peer:expr, $target:expr) => {
        $(#[$doc])*
        #[test]
        fn $name() {
            asupersync::test_utils::run_test(|| async {
                run_case($open, $begin, $peer, $target).await;
            });
        }
    };
}

const CONCURRENT: &str = "BEGIN CONCURRENT;";
const IMMEDIATE: &str = "BEGIN IMMEDIATE;";

case!(
    /// The bead's shape on a normal open: A (BEGIN CONCURRENT) writes page 1
    /// only, after stock committed page 1 inside A's window.
    open_concurrent_stock_page_one,
    Open::Normal,
    CONCURRENT,
    Peer::Stock,
    Target::PageOne
);
case!(
    /// A table leaf instead of page 1: stock updates row 2, A then updates
    /// row 1 of the same leaf.
    open_concurrent_stock_leaf,
    Open::Normal,
    CONCURRENT,
    Peer::Stock,
    Target::Leaf
);
case!(
    /// Page 1 with a second fsqlite connection as the peer, so the peer's
    /// frame comes from fsqlite's own WAL writer.
    open_concurrent_fsqlite_page_one,
    Open::Normal,
    CONCURRENT,
    Peer::Fsqlite,
    Target::PageOne
);
case!(
    /// The shared leaf with an fsqlite peer.
    open_concurrent_fsqlite_leaf,
    Open::Normal,
    CONCURRENT,
    Peer::Fsqlite,
    Target::Leaf
);
case!(
    /// A under BEGIN IMMEDIATE, stock commits page 1 inside A's window: A must
    /// not write over it. Whether stock may commit there at all is
    /// bd-reality-check-1022-llk6g.5.8's question, not this test's.
    open_immediate_stock_page_one,
    Open::Normal,
    IMMEDIATE,
    Peer::Stock,
    Target::PageOne
);
case!(
    /// BEGIN IMMEDIATE beside stock, the shared leaf.
    open_immediate_stock_leaf,
    Open::Normal,
    IMMEDIATE,
    Peer::Stock,
    Target::Leaf
);
case!(
    /// BEGIN IMMEDIATE beside an fsqlite peer, page 1: whichever side is
    /// refused, no committed change is lost.
    open_immediate_fsqlite_page_one,
    Open::Normal,
    IMMEDIATE,
    Peer::Fsqlite,
    Target::PageOne
);
case!(
    /// BEGIN IMMEDIATE beside an fsqlite peer, the shared leaf.
    open_immediate_fsqlite_leaf,
    Open::Normal,
    IMMEDIATE,
    Peer::Fsqlite,
    Target::Leaf
);
case!(
    /// A opened schema-only (26950fa4c), the open that filed bd-p8s92: its
    /// snapshot is the header-less observation. Stock commits page 1.
    schema_only_concurrent_stock_page_one,
    Open::SchemaOnly,
    CONCURRENT,
    Peer::Stock,
    Target::PageOne
);
case!(
    /// Schema-only open, stock commits the shared leaf.
    schema_only_concurrent_stock_leaf,
    Open::SchemaOnly,
    CONCURRENT,
    Peer::Stock,
    Target::Leaf
);
case!(
    /// Schema-only open, an fsqlite peer commits page 1.
    schema_only_concurrent_fsqlite_page_one,
    Open::SchemaOnly,
    CONCURRENT,
    Peer::Fsqlite,
    Target::PageOne
);
case!(
    /// Schema-only open, an fsqlite peer commits the shared leaf.
    schema_only_concurrent_fsqlite_leaf,
    Open::SchemaOnly,
    CONCURRENT,
    Peer::Fsqlite,
    Target::Leaf
);
case!(
    /// The bd-70qrk negative's fixture without its freelist damage: an
    /// fsqlite seed switched to WAL, a fresh unindexed `-shm`, no stock
    /// restart. Stock commits page 1.
    seeded_concurrent_stock_page_one,
    Open::FsqliteSeedSchemaOnly,
    CONCURRENT,
    Peer::Stock,
    Target::PageOne
);
case!(
    /// The seeded fixture, stock commits the shared leaf.
    seeded_concurrent_stock_leaf,
    Open::FsqliteSeedSchemaOnly,
    CONCURRENT,
    Peer::Stock,
    Target::Leaf
);
case!(
    /// The seeded fixture, an fsqlite peer commits page 1.
    seeded_concurrent_fsqlite_page_one,
    Open::FsqliteSeedSchemaOnly,
    CONCURRENT,
    Peer::Fsqlite,
    Target::PageOne
);
case!(
    /// The seeded fixture, an fsqlite peer commits the shared leaf.
    seeded_concurrent_fsqlite_leaf,
    Open::FsqliteSeedSchemaOnly,
    CONCURRENT,
    Peer::Fsqlite,
    Target::Leaf
);
case!(
    /// The seeded fixture under BEGIN IMMEDIATE, stock commits page 1.
    seeded_immediate_stock_page_one,
    Open::FsqliteSeedSchemaOnly,
    IMMEDIATE,
    Peer::Stock,
    Target::PageOne
);
