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
//! usage: `prepared-dml-probe DB_PATH N [autocommit|txn]` (default autocommit)

use fsqlite::blocking::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or("usage: prepared-dml-probe DB_PATH N [autocommit|txn]")?;
    let count: i64 = args.next().ok_or("missing N")?.parse()?;
    let in_transaction = match args.next().as_deref() {
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
