//! `prepared-dml-probe` — an instruction-count harness for prepared-statement
//! levers (bd-w4pr1.13.35).
//!
//! Runs N executions of one prepared single-row INSERT through
//! `fsqlite::blocking` on a file database, the API and statement shape of
//! `blocking_vs_rusqlite_bench`'s `insert_autocommit` and `insert_txn` rows:
//! either each execution in its own autocommit transaction, or all of them in
//! one explicit transaction. Run it under `perf stat` or callgrind at two
//! values of N; the difference divided by the difference in N is the cost of
//! one prepared execution, free of the open, schema and exit costs the two
//! runs share. It prints the final row count and `sum(a)` so two builds can
//! be checked for the same answer.
//!
//! The `select-rowid` and `select-index` modes run the bench's `point_rowid`
//! and `point_index` rows instead (bd-w4pr1.13.18): N executions of one
//! prepared lookup, each in its own autocommit transaction, over the bench's
//! 100,000-row table, which the first run on a new file creates. They print
//! the sum of what the lookups returned.
//!
//! usage: `prepared-dml-probe DB_PATH N [autocommit|txn|select-rowid|select-index]`
//! (default autocommit)

use fsqlite::blocking::Connection;

/// Rows of the lookup modes' table, as in `blocking_vs_rusqlite_bench`.
const LOOKUP_ROWS: i64 = 100_000;

/// Run `count` prepared lookups by rowid (`by_index == false`) or by the
/// indexed column `a`, with the bench's key sequence for its round 0.
fn run_lookups(conn: &Connection, count: i64, by_index: bool) -> fsqlite::blocking::Result<i64> {
    let loaded: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name = 't'",
        [],
        |row| row.get(0),
    )?;
    if loaded == 0 {
        conn.execute_batch(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);
             CREATE INDEX t_a ON t(a);
             BEGIN;",
        )?;
        {
            let mut insert = conn.prepare("INSERT INTO t VALUES (?1, ?2, ?3)")?;
            for id in 1..=LOOKUP_ROWS {
                insert.execute((id, (id * 7_919) % LOOKUP_ROWS, format!("row-{id}")))?;
            }
        }
        conn.execute_batch("COMMIT")?;
    }
    let key = |i: i64| (i * 104_729) % LOOKUP_ROWS + 1;
    let mut sum = 0_i64;
    if by_index {
        let mut stmt = conn.prepare("SELECT id FROM t WHERE a = ?1")?;
        for i in 0..count {
            let mut rows = stmt.query([key(i) - 1])?;
            while let Some(row) = rows.next()? {
                sum += row.get::<_, i64>(0)?;
            }
        }
    } else {
        let mut stmt = conn.prepare("SELECT b FROM t WHERE id = ?1")?;
        for i in 0..count {
            let b: String = stmt.query_row([key(i)], |row| row.get(0))?;
            sum += i64::try_from(b.len()).unwrap_or(i64::MAX);
        }
    }
    Ok(sum)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or("usage: prepared-dml-probe DB_PATH N [autocommit|txn|select-rowid|select-index]")?;
    let count: i64 = args.next().ok_or("missing N")?.parse()?;
    let mode = args.next();
    let lookup = match mode.as_deref() {
        Some("select-rowid") => Some(false),
        Some("select-index") => Some(true),
        _ => None,
    };
    if let Some(by_index) = lookup {
        let conn = Connection::open(&path)?;
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
        println!("{}", run_lookups(&conn, count, by_index)?);
        return Ok(());
    }
    let in_transaction = match mode.as_deref() {
        None | Some("autocommit") => false,
        Some("txn") => true,
        Some(other) => return Err(format!("unknown mode {other}").into()),
    };
    let conn = Connection::open(&path)?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;
         CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);",
    )?;
    if in_transaction {
        conn.execute_batch("BEGIN")?;
    }
    {
        let mut stmt = conn.prepare("INSERT INTO u(a, b) VALUES (?1, ?2)")?;
        for i in 0..count {
            stmt.execute((i, "one"))?;
        }
    }
    if in_transaction {
        conn.execute_batch("COMMIT")?;
    }
    let (rows, sum): (i64, i64) = conn.query_row("SELECT count(*), sum(a) FROM u", [], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?;
    println!("{rows}|{sum}");
    Ok(())
}
