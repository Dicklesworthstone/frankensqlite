//! bd-5310l follow-on: the OTHER measured gap — 10000 INSERTs in one txn = 11.5s vs sqlite3 39ms
//! (~295x). Profile-first: is per-insert cost FLAT (O(n) total, a constant per-insert overhead) or
//! GROWING (O(n^2) — a per-insert cost that scales with rows-so-far)? The bucketed ns/insert answers
//! it. Prepared arm isolates pure storage execution (no compile-miss); ad-hoc arm is the bead's CLI
//! scenario (unique literals -> compile-miss each).
//!
//! Run:
//!   RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!     cargo test -p fsqlite-e2e --test bulk_insert_scaling -- --ignored --nocapture

#![allow(clippy::cast_precision_loss)]

use std::time::Instant;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

#[test]
#[ignore = "profile; run explicitly"]
fn bulk_insert_scaling_profile() {
    let n: i64 = 20_000;
    let bucket: i64 = 2_000;

    eprintln!("\n########## bd-5310l bulk INSERT scaling (all in ONE txn) ##########");

    // --- PREPARED INSERT: bound params, compiled once -> pure storage execute scaling ---
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE tp (id INTEGER PRIMARY KEY, a INTEGER, b TEXT);")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let stmt = conn
        .prepare("INSERT INTO tp VALUES (?1, ?2, ?3)")
        .expect("prepare");
    eprintln!("=== PREPARED INSERT (bound params, no compile-miss) ===");
    let mut t = Instant::now();
    for i in 1..=n {
        let params = [
            SqliteValue::Integer(i),
            SqliteValue::Integer(i * 7),
            SqliteValue::Text(format!("r{i}").into()),
        ];
        conn.execute_prepared_with_params(&stmt, &params).unwrap();
        if i % bucket == 0 {
            eprintln!(
                "  inserts {:>6}-{:>6}: {:9.1} ns/insert",
                i - bucket + 1,
                i,
                t.elapsed().as_nanos() as f64 / bucket as f64
            );
            t = Instant::now();
        }
    }
    drop(stmt);
    conn.execute("COMMIT").unwrap();

    // --- AD-HOC unique INSERT: literal values -> compile-miss each (the bead's CLI scenario) ---
    let conn2 = Connection::open(":memory:").expect("open2");
    conn2
        .execute("CREATE TABLE ta (id INTEGER PRIMARY KEY, a INTEGER, b TEXT);")
        .unwrap();
    conn2.execute("BEGIN").unwrap();
    eprintln!("=== AD-HOC unique INSERT (literal values, compile-miss each) ===");
    let mut t = Instant::now();
    for i in 1..=n {
        conn2
            .execute(&format!("INSERT INTO ta VALUES ({i}, {}, 'r{i}');", i * 7))
            .unwrap();
        if i % bucket == 0 {
            eprintln!(
                "  inserts {:>6}-{:>6}: {:9.1} ns/insert",
                i - bucket + 1,
                i,
                t.elapsed().as_nanos() as f64 / bucket as f64
            );
            t = Instant::now();
        }
    }
    conn2.execute("COMMIT").unwrap();

    // --- PREPARED UPDATE by PK: bd-4cldv (re-seek per modification?) scaling ---
    let conn3 = Connection::open(":memory:").expect("open3");
    conn3
        .execute("CREATE TABLE tu (id INTEGER PRIMARY KEY, a INTEGER);")
        .unwrap();
    conn3.execute("BEGIN").unwrap();
    let ins = conn3.prepare("INSERT INTO tu VALUES (?1, 0)").unwrap();
    for i in 1..=n {
        conn3
            .execute_prepared_with_params(&ins, &[SqliteValue::Integer(i)])
            .unwrap();
    }
    drop(ins);
    let upd = conn3.prepare("UPDATE tu SET a = ?1 WHERE id = ?2").unwrap();
    eprintln!("=== PREPARED UPDATE by PK (single-row) ===");
    let mut t = Instant::now();
    for i in 1..=n {
        conn3
            .execute_prepared_with_params(
                &upd,
                &[SqliteValue::Integer(i * 3), SqliteValue::Integer(i)],
            )
            .unwrap();
        if i % bucket == 0 {
            eprintln!(
                "  updates {:>6}-{:>6}: {:9.1} ns/update",
                i - bucket + 1,
                i,
                t.elapsed().as_nanos() as f64 / bucket as f64
            );
            t = Instant::now();
        }
    }
    drop(upd);
    conn3.execute("COMMIT").unwrap();

    // --- PREPARED DELETE by PK: scaling ---
    let conn4 = Connection::open(":memory:").expect("open4");
    conn4
        .execute("CREATE TABLE td (id INTEGER PRIMARY KEY, a INTEGER);")
        .unwrap();
    conn4.execute("BEGIN").unwrap();
    let ins2 = conn4.prepare("INSERT INTO td VALUES (?1, ?2)").unwrap();
    for i in 1..=n {
        conn4
            .execute_prepared_with_params(
                &ins2,
                &[SqliteValue::Integer(i), SqliteValue::Integer(i)],
            )
            .unwrap();
    }
    drop(ins2);
    let del = conn4.prepare("DELETE FROM td WHERE id = ?1").unwrap();
    eprintln!("=== PREPARED DELETE by PK (single-row) ===");
    let mut t = Instant::now();
    for i in 1..=n {
        conn4
            .execute_prepared_with_params(&del, &[SqliteValue::Integer(i)])
            .unwrap();
        if i % bucket == 0 {
            eprintln!(
                "  deletes {:>6}-{:>6}: {:9.1} ns/delete",
                i - bucket + 1,
                i,
                t.elapsed().as_nanos() as f64 / bucket as f64
            );
            t = Instant::now();
        }
    }
    drop(del);
    conn4.execute("COMMIT").unwrap();

    // --- SAME-WIDTH prepared UPDATE by PK: init `a` to a wide (8-byte) value, update to another
    //     wide value so the record size is UNCHANGED -> the in-place overwrite fast path should
    //     engage (no delete+insert). Discriminates a general UPDATE cost from my growing-record test.
    let conn5 = Connection::open(":memory:").expect("open5");
    conn5
        .execute("CREATE TABLE tw (id INTEGER PRIMARY KEY, a INTEGER);")
        .unwrap();
    conn5.execute("BEGIN").unwrap();
    let ins3 = conn5.prepare("INSERT INTO tw VALUES (?1, ?2)").unwrap();
    for i in 1..=n {
        conn5
            .execute_prepared_with_params(
                &ins3,
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Integer(5_000_000_000 + i),
                ],
            )
            .unwrap();
    }
    drop(ins3);
    let upd2 = conn5.prepare("UPDATE tw SET a = ?1 WHERE id = ?2").unwrap();
    eprintln!("=== SAME-WIDTH prepared UPDATE by PK (8-byte -> 8-byte, in-place) ===");
    let mut t = Instant::now();
    for i in 1..=n {
        conn5
            .execute_prepared_with_params(
                &upd2,
                &[
                    SqliteValue::Integer(6_000_000_000 + i),
                    SqliteValue::Integer(i),
                ],
            )
            .unwrap();
        if i % bucket == 0 {
            eprintln!(
                "  updates {:>6}-{:>6}: {:9.1} ns/update",
                i - bucket + 1,
                i,
                t.elapsed().as_nanos() as f64 / bucket as f64
            );
            t = Instant::now();
        }
    }
    drop(upd2);
    conn5.execute("COMMIT").unwrap();
    eprintln!("########## end bulk INSERT/UPDATE/DELETE scaling ##########\n");
}
