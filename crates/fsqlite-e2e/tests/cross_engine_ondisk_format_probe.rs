//! Cross-engine on-disk format DIVERGENCE PROBE (not a keeper): the core
//! clean-room promise is that a database FILE frank writes is byte-compatible
//! with stock C SQLite. This probe writes each fixture with frank to a real
//! file, checkpoints + closes it, then opens the SAME file with rusqlite (= C
//! SQLite) and checks that (a) `PRAGMA integrity_check` passes and (b) the data
//! reads back identical to the same fixture written by stock itself. It also
//! runs the reverse direction (stock writes → frank reads) for the read path.
//!
//! This is the storage-layer surface that pure-SQL (:memory:) probing cannot
//! reach: page-size headers, journal/WAL sidecar format, overflow-page chains,
//! freelist, and b-tree on-disk layout. Per the corruption-fixture rule, every
//! fixture is stock-oracled first (stock-write → stock-read is the baseline that
//! frank-write → stock-read must match), so a silently-broken fixture cannot
//! read "ok".
//!
//! `#[ignore]` by default; run with:
//!   cargo test -p fsqlite-e2e --test cross_engine_ondisk_format_probe -- --ignored --nocapture

use std::path::Path;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("i:{n}"),
        SqliteValue::Float(f) => format!("f:{f}"),
        SqliteValue::Text(s) => format!("t:{s}"),
        SqliteValue::Blob(b) => format!("x:{}", b.iter().map(|x| format!("{x:02x}")).collect::<String>()),
    }
}

/// Write a fixture with frank to `path`: set page_size + journal_mode, run the
/// schema/data statements, checkpoint (fold any WAL into the main db), close.
async fn frank_write(path: &Path, page_size: u32, journal_mode: &str, stmts: &[&str]) -> Result<(), String> {
    let conn = Connection::open(path).await.map_err(|e| format!("open: {e}"))?;
    conn.execute(&format!("PRAGMA page_size={page_size};"))
        .await
        .map_err(|e| format!("page_size: {e}"))?;
    conn.execute(&format!("PRAGMA journal_mode={journal_mode};"))
        .await
        .map_err(|e| format!("journal_mode: {e}"))?;
    for s in stmts {
        conn.execute(s).await.map_err(|e| format!("stmt `{s}`: {e}"))?;
    }
    // Fold the WAL back into the main database so a plain reader sees everything.
    let _ = conn.query("PRAGMA wal_checkpoint(TRUNCATE);").await;
    conn.close().await.map_err(|e| format!("close: {e}"))?;
    Ok(())
}

/// Write the same fixture with stock C SQLite (rusqlite) to `path`.
fn stock_write(path: &Path, page_size: u32, journal_mode: &str, stmts: &[&str]) -> Result<(), String> {
    let conn = rusqlite::Connection::open(path).map_err(|e| format!("open: {e}"))?;
    conn.execute_batch(&format!("PRAGMA page_size={page_size}; PRAGMA journal_mode={journal_mode};"))
        .map_err(|e| format!("pragmas: {e}"))?;
    for s in stmts {
        conn.execute_batch(s).map_err(|e| format!("stmt `{s}`: {e}"))?;
    }
    let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
    Ok(())
}

/// With stock C SQLite: integrity_check string + sorted rows of `verify`.
fn stock_read(path: &Path, verify: &str) -> (String, Result<Vec<String>, String>) {
    let conn = match rusqlite::Connection::open(path) {
        Ok(c) => c,
        Err(e) => return (format!("open-err: {e}"), Err(format!("open: {e}"))),
    };
    let integ: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap_or_else(|e| format!("integ-err: {e}"));
    let rows = read_rows_rusqlite(&conn, verify);
    (integ, rows)
}

fn read_rows_rusqlite(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<String>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| format!("prep: {e}"))?;
    let n = stmt.column_count();
    let iter = stmt
        .query_map([], |row| {
            let mut cells = Vec::with_capacity(n);
            for i in 0..n {
                cells.push(match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                    rusqlite::types::ValueRef::Integer(x) => format!("i:{x}"),
                    rusqlite::types::ValueRef::Real(f) => format!("f:{f}"),
                    rusqlite::types::ValueRef::Text(t) => format!("t:{}", String::from_utf8_lossy(t)),
                    rusqlite::types::ValueRef::Blob(b) => {
                        format!("x:{}", b.iter().map(|x| format!("{x:02x}")).collect::<String>())
                    }
                });
            }
            Ok(cells.join(","))
        })
        .map_err(|e| format!("map: {e}"))?;
    let mut rows: Vec<String> = iter.collect::<Result<_, _>>().map_err(|e| format!("run: {e}"))?;
    rows.sort();
    Ok(rows)
}

/// With frank: sorted rows of `verify` from an on-disk file.
async fn frank_read(path: &Path, verify: &str) -> Result<Vec<String>, String> {
    let conn = Connection::open(path).await.map_err(|e| format!("open: {e}"))?;
    let out = match conn.query(verify).await {
        Ok(rs) => {
            let mut rows: Vec<String> = rs
                .iter()
                .map(|row| row.values().iter().map(render_frank).collect::<Vec<_>>().join(","))
                .collect();
            rows.sort();
            Ok(rows)
        }
        Err(e) => Err(e.to_string()),
    };
    conn.close_without_checkpoint().await.ok();
    out
}

/// (name, page_size, journal_mode, setup stmts, verify query).
type Fixture = (&'static str, u32, &'static str, &'static [&'static str], &'static str);

fn fixtures() -> Vec<Fixture> {
    vec![
        ("basic_delete_4096", 4096, "DELETE", &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, v REAL)",
            "INSERT INTO t VALUES (1,'a',1.5),(2,'b',2.5),(3,'c',3.5)",
        ], "SELECT id,name,v FROM t"),
        ("basic_wal_4096", 4096, "WAL", &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO t VALUES (1,'x'),(2,'y'),(3,'z')",
        ], "SELECT id,name FROM t"),
        ("page_size_512", 512, "DELETE", &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES (1,100),(2,200),(3,300)",
        ], "SELECT id,v FROM t"),
        ("page_size_65536", 65536, "DELETE", &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES (1,100),(2,200)",
        ], "SELECT id,v FROM t"),
        ("index_and_multitable", 4096, "WAL", &[
            "CREATE TABLE a(id INTEGER PRIMARY KEY, k TEXT)",
            "CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER, val INTEGER)",
            "CREATE INDEX ix ON b(aid)",
            "INSERT INTO a VALUES (1,'one'),(2,'two')",
            "INSERT INTO b VALUES (10,1,111),(11,1,222),(12,2,333)",
        ], "SELECT b.val, a.k FROM b JOIN a ON b.aid=a.id"),
        ("overflow_large_text", 4096, "DELETE", &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, big TEXT)",
            // A 20000-char string forces an overflow-page chain in the on-disk
            // b-tree cell (printf width conformance already proven by bd-cgkwp).
            "INSERT INTO t VALUES (1, printf('%020000d', 7))",
            "INSERT INTO t VALUES (2, 'small')",
        ], "SELECT id, length(big), substr(big,1,4) FROM t"),
        ("blobs_and_nulls", 4096, "DELETE", &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB, n INTEGER)",
            "INSERT INTO t VALUES (1, x'deadbeef', NULL)",
            "INSERT INTO t VALUES (2, x'00', 5)",
            "INSERT INTO t VALUES (3, NULL, NULL)",
        ], "SELECT id, b, n FROM t"),
        ("without_rowid", 4096, "DELETE", &[
            "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
            "INSERT INTO t VALUES ('alpha',1),('beta',2),('gamma',3)",
        ], "SELECT k,v FROM t"),
        ("many_rows_btree_split", 4096, "DELETE", &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<500) INSERT INTO t SELECT i, i*i FROM c",
        ], "SELECT count(*), sum(v), max(v) FROM t"),
        ("delete_then_freelist", 4096, "DELETE", &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<300) INSERT INTO t SELECT i, i FROM c",
            "DELETE FROM t WHERE id % 2 = 0",
        ], "SELECT count(*), sum(v) FROM t"),
        ("autoincrement", 4096, "DELETE", &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
            "INSERT INTO t(v) VALUES ('a'),('b'),('c')",
            "DELETE FROM t WHERE id=3",
            "INSERT INTO t(v) VALUES ('d')",
        ], "SELECT id,v FROM t"),
    ]
}

#[test]
#[ignore = "cross-engine on-disk format probe (not a keeper): frank-written files must be stock-readable"]
fn cross_engine_ondisk_format_probe() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut diverged = 0usize;
        for (name, page_size, jmode, stmts, verify) in fixtures() {
            let frank_db = dir.path().join(format!("{name}_frank.db"));
            let stock_db = dir.path().join(format!("{name}_stock.db"));

            // Stock baseline (stock-write -> stock-read): establishes the oracle.
            if let Err(e) = stock_write(&stock_db, page_size, jmode, stmts) {
                println!("STOCK-WRITE-FAIL [{name}] {e}");
                diverged += 1;
                continue;
            }
            let (st_integ, st_rows) = stock_read(&stock_db, verify);

            // Frank writes the same fixture.
            match frank_write(&frank_db, page_size, jmode, stmts).await {
                Ok(()) => {}
                Err(e) => {
                    println!("FRANK-WRITE-FAIL [{name}] {e}");
                    diverged += 1;
                    continue;
                }
            }

            // (1) Can stock C SQLite read the file frank wrote?
            let (fr_integ, fr_rows) = stock_read(&frank_db, verify);
            if !fr_integ.starts_with("ok") {
                println!("FRANK-FILE-INTEGRITY [{name}] stock integrity_check on frank file: {fr_integ}");
                diverged += 1;
            }
            match (&fr_rows, &st_rows) {
                (Ok(a), Ok(b)) if a != b => {
                    println!("STOCK-READS-FRANK-DIFF [{name}] {verify}\n    frank-file: {a:?}\n    stock-file: {b:?}");
                    diverged += 1;
                }
                (Err(e), _) => {
                    println!("STOCK-READS-FRANK-ERR [{name}] {e}");
                    diverged += 1;
                }
                _ => {}
            }
            // Sanity: stock baseline integrity must itself be ok (else fixture is broken).
            if !st_integ.starts_with("ok") {
                println!("FIXTURE-BROKEN [{name}] stock-own integrity_check: {st_integ}");
            }

            // (2) Can frank read the file stock wrote (reverse direction)?
            let fr_of_stock = frank_read(&stock_db, verify).await;
            match (&fr_of_stock, &st_rows) {
                (Ok(a), Ok(b)) if a != b => {
                    println!("FRANK-READS-STOCK-DIFF [{name}] {verify}\n    frank-read: {a:?}\n    stock-read: {b:?}");
                    diverged += 1;
                }
                (Err(e), _) => {
                    println!("FRANK-READS-STOCK-ERR [{name}] {e}");
                    diverged += 1;
                }
                _ => {}
            }
        }
        println!("\nPROBE SUMMARY: {} fixtures, {} divergences", fixtures().len(), diverged);
    });
}
