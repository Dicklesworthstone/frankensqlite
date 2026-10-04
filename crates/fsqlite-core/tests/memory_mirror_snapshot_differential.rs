#![recursion_limit = "512"]

//! Randomized differential test of `:memory:` connections against stock SQLite
//! (review of c52c2f112, bd-jdjee).
//!
//! bd-jdjee made the `:memory:` row mirror lazy: COMMIT and DDL no longer
//! rebuild it, BEGIN no longer hydrates it, and a capture point arms a one-time
//! rebuild for the next read. Time-travel snapshots became shared page images,
//! and MemTable rows and UNIQUE state became copy-on-write. Each of those can
//! surface as a stale or wrong read, so this runs seeded random sequences of
//! DDL, DML, transactions, savepoints, TEMP and attached tables, and checks:
//! - every live read (plain, aggregate, join, held prepared statements)
//!   against stock;
//! - the newest snapshot right after each capture point against stock's state
//!   at that moment;
//! - random older snapshots against stock frozen at their commit.
//!
//! Historical reads list rows of one table; aggregates and TEMP tables that
//! shadow main tables over a snapshot are known-wrong independently of bd-jdjee
//! (see the bd-jdjee keeper), so they are not generated.
//!
//! The random histories are `#[ignore]`d: at any useful scale they hit
//! pre-existing `:memory:` bugs that are not about bd-jdjee (described on
//! `memory_connection_reads_match_stock_across_random_histories`), so they
//! cannot be a green keeper yet. They are the tool for those bugs and for
//! bisecting `:memory:` read paths:
//! - `MEMDIFF_SEEDS` (default 6), `MEMDIFF_FIRST_SEED` (1) and `MEMDIFF_OPS`
//!   (250) scale the run;
//! - `MEMDIFF_TRACE=<path>` writes every statement for replay in the CLI;
//! - `MEMDIFF_STOP_AT_FIRST_DIVERGENCE=1` logs each seed's first divergence
//!   instead of failing, so two builds can be compared seed by seed
//!   (`MEMDIFF_SKIP_TEMP_HISTORY=1` for builds before the TEMP INTEGER PRIMARY
//!   KEY history fix). Reviewing bd-jdjee that way (300 seeds x 600 operations
//!   per variant against its parent) found no divergence it introduced; it
//!   removed every snapshot-history divergence the parent showed.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

/// `MEMDIFF_TRACE=<path>` appends every statement sent to fsqlite (reads and
/// probes included, held prepared reads with their parameter inlined), so a
/// failure can be replayed and minimized with the CLI.
fn trace(sql: &str) {
    if let Some(path) = std::env::var_os("MEMDIFF_TRACE") {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("trace file");
        writeln!(file, "{sql};").expect("trace write");
    }
}

thread_local! {
    /// Set when `MEMDIFF_STOP_AT_FIRST_DIVERGENCE` is on and a check diverged:
    /// the seed ends there (logged) instead of failing. Two builds run the same
    /// statement and read sequence up to that point, so comparing their logs
    /// seed by seed attributes every divergence to the commits between them,
    /// despite the pre-existing bugs described on the ignored variant.
    static FIRST_DIVERGENCE: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

/// Report a divergence: fail, or under `MEMDIFF_STOP_AT_FIRST_DIVERGENCE`
/// record it and let the seed stop.
fn diverged(message: String) {
    assert!(
        std::env::var_os("MEMDIFF_STOP_AT_FIRST_DIVERGENCE").is_some(),
        "{message}"
    );
    FIRST_DIVERGENCE.with(|slot| {
        slot.borrow_mut().get_or_insert(message);
    });
}

fn stopped_at_divergence() -> bool {
    FIRST_DIVERGENCE.with(|slot| slot.borrow().is_some())
}

struct Rng {
    state: u64,
    /// Generate the shapes that trip known pre-existing `:memory:` bugs:
    /// payloads large enough to spill to overflow pages, and DDL inside an
    /// explicit transaction (which may then roll back).
    known_bugs: bool,
}

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

fn render_value(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "null".to_owned(),
        SqliteValue::Integer(i) => format!("i:{i}"),
        SqliteValue::Float(f) => format!("r:{f}"),
        SqliteValue::Text(t) => render_text(t.as_bytes()),
        SqliteValue::Blob(b) => render_blob(b),
    }
}

fn render_text(bytes: &[u8]) -> String {
    if bytes.len() > 40 {
        format!("t:{}..len{}", String::from_utf8_lossy(&bytes[..16]), bytes.len())
    } else {
        format!("t:{}", String::from_utf8_lossy(bytes))
    }
}

fn render_blob(bytes: &[u8]) -> String {
    let sum: u64 = bytes.iter().map(|&b| u64::from(b)).sum();
    format!("b:len{}sum{sum}", bytes.len())
}

fn render_fsqlite(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| row.values().iter().map(render_value).collect::<Vec<_>>().join("|"))
        .collect()
}

fn render_stock(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<String>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let columns = stmt.column_count();
    stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(columns);
        for i in 0..columns {
            cells.push(match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => "null".to_owned(),
                rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
                rusqlite::types::ValueRef::Real(v) => format!("r:{v}"),
                rusqlite::types::ValueRef::Text(v) => render_text(v),
                rusqlite::types::ValueRef::Blob(v) => render_blob(v),
            });
        }
        Ok(cells.join("|"))
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

/// A table the generator knows how to read and write.
#[derive(Clone)]
struct TableModel {
    /// Name as written in SQL (`aux.t5` for the attached table).
    name: &'static str,
    /// Column list used for listings; the history check uses the list as it
    /// stood at the snapshot.
    columns: Vec<String>,
    /// ORDER BY key that totally orders the table.
    order_by: &'static str,
    /// TEMP or attached: snapshots carry TEMP tables, attached ones are not
    /// part of `main`'s history.
    historical: bool,
    exists: bool,
}

struct Snapshot {
    seq: u64,
    stock: rusqlite::Connection,
    tables: Vec<TableModel>,
}

const BASE_DDL: &[&str] = &[
    "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INT, b TEXT, c BLOB)",
    "CREATE TABLE t2 (k TEXT PRIMARY KEY, v INT, w TEXT UNIQUE) WITHOUT ROWID",
    "CREATE TABLE t3 (id INTEGER PRIMARY KEY, a, b UNIQUE)",
    "CREATE TEMP TABLE tmp1 (id INTEGER PRIMARY KEY, a, b)",
    "CREATE INDEX t1_a ON t1(a)",
    "CREATE INDEX t3_a ON t3(a)",
    "CREATE INDEX tmp1_a ON tmp1(a)",
];

fn initial_tables() -> Vec<TableModel> {
    let cols = |list: &[&str]| list.iter().map(|c| (*c).to_owned()).collect::<Vec<_>>();
    vec![
        TableModel { name: "t1", columns: cols(&["id", "a", "b", "c"]), order_by: "id", historical: true, exists: true },
        TableModel { name: "t2", columns: cols(&["k", "v", "w"]), order_by: "k", historical: true, exists: true },
        TableModel { name: "t3", columns: cols(&["id", "a", "b"]), order_by: "id", historical: true, exists: true },
        TableModel { name: "tmp1", columns: cols(&["id", "a", "b"]), order_by: "id", historical: true, exists: true },
        TableModel { name: "aux.t5", columns: cols(&["id", "a"]), order_by: "id", historical: false, exists: true },
    ]
}

fn padded_text(rng: &mut Rng) -> String {
    let len = match rng.below(10) {
        0 if rng.known_bugs => 3000 + rng.below(6000) as usize, // overflow chains
        0 => 600 + rng.below(900) as usize,
        1 => 200 + rng.below(400) as usize,
        _ => rng.below(12) as usize,
    };
    let seed = rng.below(26) as u8;
    let text: String = (0..len).map(|i| char::from(b'a' + ((seed as usize + i) % 26) as u8)).collect();
    format!("'{text}'")
}

fn small_value(rng: &mut Rng) -> String {
    match rng.below(8) {
        0 => "NULL".to_owned(),
        1 => format!("'{}'", rng.below(30)),
        2 => format!("{}.5", rng.below(30)),
        3 => format!("'s{}'", rng.below(30)),
        _ => format!("{}", rng.below(40)),
    }
}

/// A plain INSERT can fail on a constraint. A statement that fails past
/// parsing is a known-bug trigger (see the ignored variant), so the default run
/// only writes with conflict clauses that cannot fail.
fn insert_verb(rng: &mut Rng) -> &'static str {
    let verbs: &[&'static str] = if rng.known_bugs {
        &["INSERT OR REPLACE", "INSERT OR IGNORE", "INSERT"]
    } else {
        &["INSERT OR REPLACE", "INSERT OR IGNORE"]
    };
    verbs[rng.below(verbs.len() as u64) as usize]
}

/// One random statement, plus whether it can end in a time-travel capture.
fn random_statement(rng: &mut Rng, tables: &mut [TableModel], in_txn: bool) -> (String, bool) {
    let roll = rng.below(100);
    let allocates_root = matches!(roll, 21..=23 | 25..=26);
    if !rng.known_bugs && (allocates_root || ((21..=26).contains(&roll) && in_txn)) {
        // DDL that allocates or frees a root page after the setup, and any DDL
        // inside a transaction, are known-bug shapes (see the ignored
        // variant); the default run builds its tables and indexes at setup and
        // keeps ALTER TABLE ADD COLUMN.
        return ("SELECT 1".to_owned(), false);
    }
    let t1_cols = tables[0].columns.len();
    match roll {
        // Transaction control.
        0..=5 => ("BEGIN".to_owned(), false),
        6..=10 => ("COMMIT".to_owned(), true),
        11..=13 => ("ROLLBACK".to_owned(), false),
        14..=16 => (format!("SAVEPOINT sp{}", rng.below(3)), false),
        17..=18 => (format!("RELEASE sp{}", rng.below(3)), true),
        19..=20 => (format!("ROLLBACK TO sp{}", rng.below(3)), false),
        // DDL.
        21..=22 => {
            let col = ["a", "b"][rng.below(2) as usize];
            // A UNIQUE index can fail to build, which is a known-bug shape.
            let unique = if rng.known_bugs && rng.chance(30) { "UNIQUE " } else { "" };
            let table = ["t1", "t3"][rng.below(2) as usize];
            let col = if table == "t3" && col == "b" { "a" } else { col };
            (format!("CREATE {unique}INDEX IF NOT EXISTS ix_{table}_{col}_{} ON {table}({col})", rng.below(3)), true)
        }
        23 => (format!("DROP INDEX IF EXISTS ix_t1_a_{}", rng.below(3)), true),
        24 if t1_cols < 7 => {
            let name = format!("d{t1_cols}");
            tables[0].columns.push(name.clone());
            (format!("ALTER TABLE t1 ADD COLUMN {name} DEFAULT {}", rng.below(9)), true)
        }
        25 => ("CREATE TABLE IF NOT EXISTS extra (x INTEGER PRIMARY KEY, y)".to_owned(), true),
        26 => ("DROP TABLE IF EXISTS extra".to_owned(), true),
        27 => (format!("INSERT OR IGNORE INTO tmp1 SELECT id + {}, a, b FROM t3 WHERE id < 5", 1000 + rng.below(1000)), false),
        // DML on t1 (rowid table with overflow-sized text and blobs).
        28..=42 => {
            let id = rng.below(60) + 1;
            let blob = match (rng.chance(15), rng.known_bugs) {
                (true, true) => format!("zeroblob({})", 2000 + rng.below(5000)),
                (true, false) => format!("zeroblob({})", 100 + rng.below(1200)),
                (false, _) => "x'0102'".to_owned(),
            };
            let verb = insert_verb(rng);
            let mut values = vec![id.to_string(), small_value(rng), padded_text(rng), blob];
            values.extend((4..t1_cols).map(|_| small_value(rng)));
            (format!("{verb} INTO t1 VALUES ({})", values.join(", ")), false)
        }
        43..=48 => (
            format!("UPDATE t1 SET a = coalesce(a, 0) + 1, b = {} WHERE id % {} = {}", padded_text(rng), rng.below(4) + 1, rng.below(3)),
            false,
        ),
        49..=52 => (format!("DELETE FROM t1 WHERE id % {} = {}", rng.below(6) + 2, rng.below(3)), false),
        // DML on t2 (WITHOUT ROWID with a UNIQUE column).
        53..=60 => {
            let verb = insert_verb(rng);
            (format!("{verb} INTO t2 VALUES ('k{}', {}, 'w{}')", rng.below(40), small_value(rng), rng.below(25)), false)
        }
        61..=63 => (format!("UPDATE OR IGNORE t2 SET w = 'w{}' WHERE k = 'k{}'", rng.below(25), rng.below(40)), false),
        64..=65 => (format!("DELETE FROM t2 WHERE v > {}", rng.below(30)), false),
        // DML on t3 (rowid table with a UNIQUE column: MemTable UNIQUE state).
        66..=73 => {
            let verb = insert_verb(rng);
            (format!("{verb} INTO t3 VALUES ({}, {}, {})", rng.below(50) + 1, small_value(rng), small_value(rng)), false)
        }
        74..=75 => (format!("UPDATE OR REPLACE t3 SET b = {} WHERE id = {}", small_value(rng), rng.below(50) + 1), false),
        76 => ("DELETE FROM t3 WHERE id % 2 = 0".to_owned(), false),
        77 => ("DELETE FROM t3".to_owned(), false),
        // TEMP table.
        78..=82 => (format!("INSERT OR REPLACE INTO tmp1 VALUES ({}, {}, {})", rng.below(30) + 1, small_value(rng), padded_text(rng)), false),
        83 => (format!("DELETE FROM tmp1 WHERE id > {}", rng.below(30)), false),
        84 => ("UPDATE tmp1 SET a = 'u' || coalesce(a, '')".to_owned(), false),
        // Attached database.
        85..=87 => (format!("INSERT OR REPLACE INTO aux.t5 VALUES ({}, {})", rng.below(20) + 1, small_value(rng)), false),
        88 => (format!("DELETE FROM aux.t5 WHERE id > {}", rng.below(20)), false),
        // Cross-table writes.
        89..=90 => ("INSERT OR IGNORE INTO t3 (id, a, b) SELECT id + 100, a, 'from-t1-' || id FROM t1 WHERE id < 10".to_owned(), false),
        91 if rng.known_bugs => ("INSERT OR REPLACE INTO extra SELECT id, a FROM t1".to_owned(), false),
        _ => ("SELECT 1".to_owned(), false),
    }
}

fn live_queries(tables: &[TableModel]) -> Vec<String> {
    let mut queries: Vec<String> = tables
        .iter()
        .filter(|t| t.exists)
        .map(|t| format!("SELECT {} FROM {} ORDER BY {}", t.columns.join(", "), t.name, t.order_by))
        .collect();
    queries.extend(
        [
            "SELECT count(*), sum(a), max(length(b)) FROM t1",
            "SELECT a, count(*) FROM t1 GROUP BY a ORDER BY a",
            "SELECT count(*) FROM t3",
            "SELECT count(*), min(k), max(w) FROM t2",
            "SELECT t1.id, t3.b FROM t1 JOIN t3 ON t1.a = t3.a ORDER BY 1, 2",
            "SELECT id FROM t1 WHERE a > 3 ORDER BY id",
            "SELECT count(*) FROM tmp1",
        ]
        .map(str::to_owned),
    );
    queries
}

async fn compare_live(
    conn: &Connection,
    stock: &rusqlite::Connection,
    sql: &str,
    context: &dyn Fn() -> String,
) {
    if stopped_at_divergence() {
        return;
    }
    trace(sql);
    let ours = conn.query(sql).await.map(|rows| render_fsqlite(&rows));
    let theirs = render_stock(stock, sql);
    match (ours, theirs) {
        (Ok(a), Ok(b)) if a == b => {}
        (Err(_), Err(_)) => {}
        (a, b) => diverged(format!("live read diverged: {sql}\nfsqlite={a:?}\nstock={b:?}\n{}", context())),
    }
}

async fn compare_prepared(
    held: &[(fsqlite_core::connection::PreparedStatement<'_>, &'static str)],
    stock: &rusqlite::Connection,
    rng: &mut Rng,
    context: &dyn Fn() -> String,
) {
    if stopped_at_divergence() {
        return;
    }
    for (stmt, sql) in held {
        let param = rng.below(50) as i64 + 1;
        trace(&sql.replace("?1", &param.to_string()));
        let ours = stmt
            .query_with_params(&[SqliteValue::Integer(param)])
            .await
            .map(|rows| render_fsqlite(&rows));
        let stock_sql = sql.replace("?1", &param.to_string());
        let theirs = render_stock(stock, &stock_sql);
        match (ours, theirs) {
            (Ok(a), Ok(b)) if a == b => {}
            (Err(_), Err(_)) => {}
            (a, b) => {
                diverged(format!("held prepared read diverged: {sql} [{param}]\nfsqlite={a:?}\nstock={b:?}\n{}", context()));
                return;
            }
        }
    }
}

fn snapshot_stock(stock: &rusqlite::Connection) -> rusqlite::Connection {
    let copy = rusqlite::Connection::open_in_memory().expect("copy");
    for (schema, sql) in [("main", "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND sql IS NOT NULL"),
                          ("temp", "SELECT sql FROM temp.sqlite_master WHERE type = 'table' AND sql IS NOT NULL")] {
        let mut stmt = stock.prepare(sql).expect("schema");
        let ddls: Vec<String> = stmt.query_map([], |r| r.get(0)).expect("q").collect::<Result<_, _>>().expect("rows");
        for ddl in ddls {
            let ddl = if schema == "temp" { ddl.replacen("CREATE TABLE", "CREATE TEMP TABLE", 1) } else { ddl };
            copy.execute_batch(&ddl).expect("copy ddl");
        }
    }
    // Copy rows table by table through value lists (avoids the backup API's
    // handling of TEMP).
    for (schema, list) in [("main", "SELECT name FROM main.sqlite_master WHERE type = 'table'"),
                           ("temp", "SELECT name FROM temp.sqlite_master WHERE type = 'table'")] {
        let mut stmt = stock.prepare(list).expect("names");
        let names: Vec<String> = stmt.query_map([], |r| r.get(0)).expect("q").collect::<Result<_, _>>().expect("rows");
        for name in names {
            let mut rows_stmt = stock.prepare(&format!("SELECT * FROM {schema}.\"{name}\"")).expect("rows");
            let cols = rows_stmt.column_count();
            let rows: Vec<Vec<rusqlite::types::Value>> = rows_stmt
                .query_map([], |r| (0..cols).map(|i| r.get::<_, rusqlite::types::Value>(i)).collect())
                .expect("q")
                .collect::<Result<_, _>>()
                .expect("rows");
            let placeholders = vec!["?"; cols].join(", ");
            let insert = format!("INSERT INTO {schema}.\"{name}\" VALUES ({placeholders})");
            for row in rows {
                copy.execute(&insert, rusqlite::params_from_iter(row)).expect("copy row");
            }
        }
    }
    copy
}

fn stock_columns(stock: &rusqlite::Connection, table: &str) -> Vec<String> {
    let mut stmt = stock.prepare(&format!("PRAGMA table_info({table})")).expect("table_info");
    stmt.query_map([], |row| row.get::<_, String>(1))
        .expect("table_info rows")
        .collect::<Result<_, _>>()
        .expect("columns")
}

async fn compare_history(conn: &Connection, snap: &Snapshot, context: &dyn Fn() -> String) {
    if stopped_at_divergence() {
        return;
    }
    for table in snap.tables.iter().filter(|t| t.historical && t.exists) {
        let cols = table.columns.join(", ");
        let historical = format!(
            "SELECT {cols} FROM {} FOR SYSTEM_TIME AS OF COMMITSEQ {} ORDER BY {}",
            table.name, snap.seq, table.order_by
        );
        let stock_sql = format!("SELECT {cols} FROM {} ORDER BY {}", table.name, table.order_by);
        trace(&historical);
        let ours = conn.query(&historical).await.map(|rows| render_fsqlite(&rows));
        let theirs = render_stock(&snap.stock, &stock_sql);
        match (ours, theirs) {
            (Ok(a), Ok(b)) if a == b => {}
            (a, b) => {
                diverged(format!("historical read diverged: {historical}\nfsqlite={a:?}\nstock={b:?}\n{}", context()));
                return;
            }
        }
    }
}

async fn run_seed(seed: u64, ops: usize, known_bugs: bool) {
    let conn = Connection::open(":memory:").await.expect("open");
    let stock = rusqlite::Connection::open_in_memory().expect("stock");
    for sql in BASE_DDL.iter().copied().chain(["ATTACH ':memory:' AS aux", "CREATE TABLE aux.t5 (id INTEGER PRIMARY KEY, a)"]) {
        trace(sql);
        conn.execute(sql).await.unwrap_or_else(|e| panic!("seed {seed}: {sql}: {e}"));
        stock.execute_batch(sql).unwrap_or_else(|e| panic!("seed {seed} stock: {sql}: {e}"));
    }
    let held_sql: [&'static str; 3] = [
        "SELECT id, a, length(b) FROM t1 WHERE id = ?1",
        "SELECT count(*) FROM t3 WHERE id >= ?1",
        "SELECT k, v, w FROM t2 WHERE k = 'k' || ?1",
    ];
    let mut held = Vec::new();
    for sql in held_sql {
        held.push((conn.prepare(sql).await.expect("prepare held"), sql));
    }

    let mut rng = Rng { state: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1, known_bugs };
    let mut tables = initial_tables();
    // Bisection aid: builds before the TEMP INTEGER PRIMARY KEY history fix
    // read that column back as NULL from a snapshot.
    if std::env::var_os("MEMDIFF_SKIP_TEMP_HISTORY").is_some() {
        tables.iter_mut().filter(|t| t.name == "tmp1").for_each(|t| t.historical = false);
    }
    let mut snapshots: Vec<Snapshot> = Vec::new();
    let mut log: Vec<String> = Vec::new();
    // Coverage counters, asserted at the end so a vacuous run fails.
    let (mut live_reads, mut prepared_reads, mut captures, mut rechecks) = (0_u32, 0_u32, 0_u32, 0_u32);

    for step in 0..ops {
        let (sql, may_capture) = random_statement(&mut rng, &mut tables, !stock.is_autocommit());
        let seq_before = conn.last_local_commit_seq();
        trace(&sql);
        let ours = conn.execute(&sql).await;
        let theirs = stock.execute_batch(&sql);
        // ALTER ... ADD COLUMN may fail or be rolled back later; take t1's
        // column list from stock after every statement.
        tables[0].columns = stock_columns(&stock, "t1");
        log.push(format!("{step}: {sql} -> fsqlite {} / stock {}",
            ours.as_ref().map_or_else(|e| format!("err({e})"), |_| "ok".to_owned()),
            theirs.as_ref().map_or_else(|e| format!("err({e})"), |()| "ok".to_owned())));
        let context = || format!("seed {seed}\n{}", log.iter().rev().take(400).rev().cloned().collect::<Vec<_>>().join("\n"));
        if ours.is_ok() != theirs.is_ok() {
            diverged(format!("statement outcome diverged: {sql:.200}\nfsqlite={ours:?}\nstock={theirs:?}\n{}", context()));
            break;
        }
        let txn_control = ["BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE"]
            .iter()
            .any(|word| sql.starts_with(word));
        if !(known_bugs || ours.is_ok() || txn_control) {
            diverged(format!(
                "the default run must not generate failing statements (a known-bug trigger): {sql:.200}\n{}",
                context()
            ));
            break;
        }

        let in_txn = !stock.is_autocommit();
        if may_capture || (!in_txn && ours.is_ok() && (sql.starts_with("CREATE") || sql.starts_with("DROP") || sql.starts_with("ALTER"))) {
            // A snapshot at the seq this very statement committed was captured
            // by it, so it must equal stock's state now. Probing is itself a
            // read, which consumes the armed mirror rebuild, so check only
            // half the time and let the stale window after the other half
            // meet the random reads.
            let seq_after = conn.last_local_commit_seq();
            if !in_txn && ours.is_ok() && seq_after != seq_before && rng.chance(50)
                && let Some(seq) = seq_after
            {
                let probe = format!("SELECT count(*) FROM t3 FOR SYSTEM_TIME AS OF COMMITSEQ {seq}");
                trace(&probe);
                if conn.query(&probe).await.is_ok() {
                    let snap = Snapshot { seq, stock: snapshot_stock(&stock), tables: tables.clone() };
                    compare_history(&conn, &snap, &context).await;
                    captures += 1;
                    snapshots.push(snap);
                    if snapshots.len() > 200 {
                        snapshots.remove(0);
                    }
                }
            }
        }

        if rng.chance(45) {
            let queries = live_queries(&tables);
            let pick = rng.below(queries.len() as u64) as usize;
            compare_live(&conn, &stock, &queries[pick], &context).await;
            live_reads += 1;
        }
        if rng.chance(25) {
            compare_prepared(&held, &stock, &mut rng, &context).await;
            prepared_reads += 1;
        }
        if !snapshots.is_empty() && rng.chance(8) {
            let pick = rng.below(snapshots.len() as u64) as usize;
            compare_history(&conn, &snapshots[pick], &context).await;
            rechecks += 1;
        }
        if stopped_at_divergence() {
            break;
        }
    }
    if let Some(stop) = FIRST_DIVERGENCE.with(|slot| slot.borrow_mut().take()) {
        let first_line = stop.lines().next().unwrap_or_default();
        let detail = stop.lines().nth(1).unwrap_or_default();
        eprintln!("seed {seed}: STOPPED at step {}: {first_line:.160} | {detail:.200}", log.len());
        return;
    }

    // Final sweep: every live query, and every retained snapshot.
    let context = || format!("seed {seed} (final sweep)\n{}", log.iter().rev().take(400).rev().cloned().collect::<Vec<_>>().join("\n"));
    for sql in live_queries(&tables) {
        compare_live(&conn, &stock, &sql, &context).await;
    }
    for snap in &snapshots {
        compare_history(&conn, snap, &context).await;
    }
    if let Some(stop) = FIRST_DIVERGENCE.with(|slot| slot.borrow_mut().take()) {
        let first_line = stop.lines().next().unwrap_or_default();
        let detail = stop.lines().nth(1).unwrap_or_default();
        eprintln!("seed {seed}: STOPPED in the final sweep: {first_line:.160} | {detail:.200}");
        return;
    }
    eprintln!(
        "seed {seed}: {ops} ops, {live_reads} live reads, {prepared_reads} held-prepared rounds, \
         {captures} snapshots checked at capture, {rechecks} historical rechecks"
    );
    if ops >= 200 {
        assert!(
            live_reads > 0 && prepared_reads > 0 && captures > 0,
            "seed {seed}: the run exercised too little to mean anything"
        );
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn run_seeds(known_bugs: bool) {
    let seeds = env_u64("MEMDIFF_SEEDS", 6);
    let first = env_u64("MEMDIFF_FIRST_SEED", 1);
    let ops = env_u64("MEMDIFF_OPS", 250) as usize;
    for seed in first..first + seeds {
        asupersync::test_utils::run_test(move || async move {
            run_seed(seed, ops, known_bugs).await;
        });
    }
}

/// Random histories with the narrower generator: no root-page DDL after the
/// setup, no DDL inside transactions, and no statement that fails past
/// parsing. Ignored because pre-existing `:memory:` bugs (all reproduce on
/// fc1f6a537, before bd-jdjee; file-backed databases are unaffected) still
/// fire, in about one seed in five at 600 operations:
/// - page splits inside an explicit transaction can leak a page. Minimal
///   form: grow a table past one page, then `BEGIN; INSERT <row that splits a
///   leaf>; COMMIT; CREATE TABLE x (...)` leaves "page N is never used"
///   (stock: ok). Later page reuse then corrupts: "invalid B-tree page type
///   flag: 0x00", clobbered overflow chains, or "sqlite_master ... uses free
///   rootpage N" (the last surfaces sooner since bd-jdjee, whose deferred
///   mirror rebuild validates the catalog at the next read);
/// - an attached `:memory:` database loses a committed row:
///   `ATTACH ':memory:' AS aux; CREATE TABLE aux.t (id INTEGER PRIMARY KEY, a);
///   SAVEPOINT s; INSERT INTO aux.t VALUES (16, 1); RELEASE s; BEGIN;
///   ROLLBACK;` leaves `aux.t` empty (stock keeps the row).
#[test]
#[ignore = "pre-existing :memory: page-accounting and attached-database bugs (see doc comment)"]
fn memory_connection_reads_match_stock_across_random_histories() {
    run_seeds(false);
}

/// Same, with the generator's known-bug shapes left in, which trip the same
/// bugs far more often, plus:
/// - deleting a row whose payload spilled to overflow pages leaks those pages
///   (`freelist_count` stays 0; stock `integrity_check`: "page N is never
///   used"), and a later CREATE INDEX can be handed one of them as its root
///   ("sqlite_master index ... uses free rootpage N");
/// - `BEGIN; CREATE INDEX ...; COMMIT;` leaks the index root the same way as
///   a page split. After any later statement that fails past parsing (no such
///   table, wrong value count, a UNIQUE violation), the leaked page is on the
///   pager's live freelist while a CREATE TABLE or CREATE INDEX holds it as
///   its root, and reads and writes fail with "sqlite_master ... uses free
///   rootpage N".
#[test]
#[ignore = "pre-existing :memory: page-accounting bugs (see doc comment)"]
fn memory_connection_reads_match_stock_across_random_histories_with_known_bug_shapes() {
    run_seeds(true);
}

/// A TEMP rowid table's INTEGER PRIMARY KEY reads back in a snapshot. The
/// mirror keeps it in the rowid, not the column slot; historical reads used to
/// return NULL for it (found by the random histories above).
#[test]
fn temp_table_integer_primary_key_reads_back_from_a_snapshot() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        for sql in [
            "CREATE TABLE m (id INTEGER PRIMARY KEY, a)",
            "CREATE TEMP TABLE tmp1 (id INTEGER PRIMARY KEY, a)",
            "BEGIN",
            "INSERT INTO tmp1 VALUES (4, 'x'), (20, 'y')",
            "INSERT INTO m VALUES (7, 'm7')",
            "COMMIT",
        ] {
            conn.execute(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        let latest = " FOR SYSTEM_TIME AS OF '4000000000'";
        let rows = conn
            .query(&format!("SELECT id, a FROM tmp1{latest} ORDER BY id"))
            .await
            .expect("historical temp read");
        assert_eq!(render_fsqlite(&rows), vec!["i:4|t:x", "i:20|t:y"]);
        let rows = conn
            .query(&format!("SELECT id, a FROM m{latest} ORDER BY id"))
            .await
            .expect("historical main read");
        assert_eq!(render_fsqlite(&rows), vec!["i:7|t:m7"]);
    });
}
