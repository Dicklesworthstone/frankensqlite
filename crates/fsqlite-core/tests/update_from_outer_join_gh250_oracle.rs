//! GH #250 (bd-gh-update-from-outer-join-wusp5): `UPDATE ... FROM` whose FROM
//! clause contains an OUTER JOIN source used to error with "UPDATE ... FROM with
//! an OUTER JOIN source is not yet supported". The connection-layer AST rewrite
//! `hoist_update_from_outer_join_to_cte` turns the fully-qualified, plain-table
//! shape into an equivalent CTE-materialized form (the outer join runs inside a
//! CTE, where SELECT-join codegen null-extends correctly), which the existing
//! `codegen_update_from` already drives.
//!
//! rusqlite (stock SQLite) is the oracle: for each scenario the final
//! `SELECT * FROM tgt ORDER BY id` from frank must equal stock SQLite's, byte
//! for byte across every value.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// A storage-class-exact cell value for oracle comparison. Floats compare by
/// their bit pattern so `-0.0`/`NaN`/rounding never make a spurious "match".
#[derive(Debug, Clone, PartialEq, Eq)]
enum Cell {
    Null,
    Int(i64),
    Real(u64),
    Text(String),
    Blob(Vec<u8>),
}

fn frank_cell(v: &SqliteValue) -> Cell {
    match v {
        SqliteValue::Null => Cell::Null,
        SqliteValue::Integer(n) => Cell::Int(*n),
        SqliteValue::Float(f) => Cell::Real(f.to_bits()),
        SqliteValue::Text(t) => Cell::Text(t.to_string()),
        SqliteValue::Blob(b) => Cell::Blob(b.to_vec()),
    }
}

fn sqlite_cell(v: rusqlite::types::ValueRef<'_>) -> Cell {
    use rusqlite::types::ValueRef;
    match v {
        ValueRef::Null => Cell::Null,
        ValueRef::Integer(n) => Cell::Int(n),
        ValueRef::Real(f) => Cell::Real(f.to_bits()),
        ValueRef::Text(t) => Cell::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Cell::Blob(b.to_vec()),
    }
}

/// Run `setup` (DDL + seed rows) then `update` in frank, returning the final
/// `SELECT * FROM tgt ORDER BY id`.
async fn frank_final(setup: &str, update: &str) -> Vec<Vec<Cell>> {
    let conn = Connection::open(":memory:")
        .await
        .expect("frank open :memory:");
    conn.execute_batch(setup).await.expect("frank setup");
    conn.execute(update).await.expect("frank UPDATE");
    conn.query("SELECT * FROM tgt ORDER BY id")
        .await
        .expect("frank SELECT")
        .iter()
        .map(|row| row.values().iter().map(frank_cell).collect())
        .collect()
}

/// Same in stock SQLite (rusqlite), as the oracle.
fn sqlite_final(setup: &str, update: &str) -> Vec<Vec<Cell>> {
    let conn = rusqlite::Connection::open_in_memory().expect("rusqlite open");
    conn.execute_batch(setup).expect("rusqlite setup");
    conn.execute(update, []).expect("rusqlite UPDATE");
    let mut stmt = conn
        .prepare("SELECT * FROM tgt ORDER BY id")
        .expect("rusqlite prepare");
    let column_count = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut cells = Vec::with_capacity(column_count);
            for i in 0..column_count {
                cells.push(sqlite_cell(row.get_ref(i)?));
            }
            Ok(cells)
        })
        .expect("rusqlite query_map")
        .map(Result::unwrap)
        .collect();
    rows
}

/// Assert frank matches the stock-SQLite oracle for `setup` + `update`, and
/// return the shared result so callers can additionally pin exact values.
async fn assert_oracle(setup: &str, update: &str) -> Vec<Vec<Cell>> {
    let frank = frank_final(setup, update).await;
    let sqlite = sqlite_final(setup, update);
    assert_eq!(
        frank, sqlite,
        "frank diverged from stock SQLite\n  setup:  {setup}\n  update: {update}"
    );
    frank
}

const SETUP: &str = "\
CREATE TABLE tgt(id INTEGER PRIMARY KEY, value);
CREATE TABLE ls(id);
CREATE TABLE src(id, value);
INSERT INTO tgt VALUES(1,10),(2,20);
INSERT INTO ls VALUES(1),(2);
INSERT INTO src VALUES(1,100);";

/// The exact GH#250 repro: matched left row -> src.value (100); unmatched left
/// row null-extends src.value -> NULL -> COALESCE(...,-1) -> -1.
#[test]
fn update_from_left_join_coalesce_repro_gh250() {
    asupersync::test_utils::run_test(|| async {
        let update = "UPDATE tgt SET value=COALESCE(src.value,-1) \
             FROM ls LEFT JOIN src ON src.id=ls.id WHERE tgt.id=ls.id;";
        let rows = assert_oracle(SETUP, update).await;
        assert_eq!(
            rows,
            vec![
                vec![Cell::Int(1), Cell::Int(100)],
                vec![Cell::Int(2), Cell::Int(-1)],
            ],
            "expected final tgt (1,100),(2,-1)"
        );
    });
}

/// LEFT JOIN produces NULL and the SET writes the RAW null (no COALESCE): the
/// unmatched target row's value must become NULL, exactly like stock SQLite.
#[test]
fn update_from_left_join_raw_null_gh250() {
    asupersync::test_utils::run_test(|| async {
        let update = "UPDATE tgt SET value=src.value \
             FROM ls LEFT JOIN src ON src.id=ls.id WHERE tgt.id=ls.id;";
        let rows = assert_oracle(SETUP, update).await;
        assert_eq!(
            rows,
            vec![
                vec![Cell::Int(1), Cell::Int(100)],
                vec![Cell::Int(2), Cell::Null],
            ],
            "expected final tgt (1,100),(2,NULL)"
        );
    });
}

/// Control (regression guard): the SAME query with INNER JOIN — which does NOT
/// take the new rewrite path — must still match stock SQLite. The unmatched row
/// (id=2) has no join row, so it is never updated and keeps its seed value 20.
#[test]
fn update_from_inner_join_control_gh250() {
    asupersync::test_utils::run_test(|| async {
        let update = "UPDATE tgt SET value=COALESCE(src.value,-1) \
             FROM ls INNER JOIN src ON src.id=ls.id WHERE tgt.id=ls.id;";
        let rows = assert_oracle(SETUP, update).await;
        assert_eq!(
            rows,
            vec![
                vec![Cell::Int(1), Cell::Int(100)],
                vec![Cell::Int(2), Cell::Int(20)],
            ],
            "INNER JOIN: only id=1 updated -> (1,100),(2,20)"
        );
    });
}

/// Control: a LEFT JOIN where EVERY left row matches (no null-extension) must
/// still match stock SQLite — the rewrite must not perturb the all-matched case.
#[test]
fn update_from_left_join_all_matched_gh250() {
    asupersync::test_utils::run_test(|| async {
        // src now has a row for every ls id, so no null-extension occurs.
        let setup = "\
CREATE TABLE tgt(id INTEGER PRIMARY KEY, value);
CREATE TABLE ls(id);
CREATE TABLE src(id, value);
INSERT INTO tgt VALUES(1,10),(2,20);
INSERT INTO ls VALUES(1),(2);
INSERT INTO src VALUES(1,100),(2,200);";
        let update = "UPDATE tgt SET value=COALESCE(src.value,-1) \
             FROM ls LEFT JOIN src ON src.id=ls.id WHERE tgt.id=ls.id;";
        let rows = assert_oracle(setup, update).await;
        assert_eq!(
            rows,
            vec![
                vec![Cell::Int(1), Cell::Int(100)],
                vec![Cell::Int(2), Cell::Int(200)],
            ],
            "all-matched LEFT JOIN -> (1,100),(2,200)"
        );
    });
}
