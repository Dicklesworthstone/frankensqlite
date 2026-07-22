//! Oracle for bd-distinct-loose-scan: `SELECT DISTINCT <indexed col>` served by a loose/skip index
//! scan must be byte-exact vs rusqlite across NULLs, duplicate runs, empty/all-same/all-NULL tables,
//! and must DECLINE (fall back, still correct) for NOCASE columns and non-indexed columns.
#![allow(clippy::uninlined_format_args)]
use fsqlite::Connection;
use fsqlite_types::SqliteValue;

/// Format one fsqlite result set as ordered `\n`-joined rows of `|`-joined cells.
fn f_rows(c: &Connection, sql: &str) -> String {
    let rows = c.query(sql).unwrap();
    let mut out = Vec::new();
    for r in rows.iter() {
        let cells: Vec<String> = r
            .values()
            .iter()
            .map(|v| match v {
                SqliteValue::Null => "NULL".to_string(),
                SqliteValue::Integer(i) => format!("i{i}"),
                SqliteValue::Float(x) => format!("f{x}"),
                SqliteValue::Text(t) => format!("t{}", t),
                SqliteValue::Blob(b) => format!("b{:?}", b),
            })
            .collect();
        out.push(cells.join("|"));
    }
    out.join("\n")
}

/// Same formatting for a rusqlite result set.
fn r_rows(c: &rusqlite::Connection, sql: &str) -> String {
    let mut stmt = c.prepare(sql).unwrap();
    let ncol = stmt.column_count();
    let rows: Vec<String> = stmt
        .query_map([], |row| {
            let cells: Vec<String> = (0..ncol)
                .map(|i| match row.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Null => "NULL".to_string(),
                    rusqlite::types::ValueRef::Integer(v) => format!("i{v}"),
                    rusqlite::types::ValueRef::Real(x) => format!("f{x}"),
                    rusqlite::types::ValueRef::Text(t) => {
                        format!("t{}", String::from_utf8_lossy(t))
                    }
                    rusqlite::types::ValueRef::Blob(b) => format!("b{:?}", b),
                })
                .collect();
            Ok(cells.join("|"))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    rows.join("\n")
}

fn has_op(c: &Connection, sql: &str, op: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .any(|r| matches!(r.values().get(1), Some(SqliteValue::Text(o)) if o.to_string() == op))
}

/// Build the same schema+data in both engines. `ddl`+`rows` are applied verbatim to each.
fn both(ddl: &[&str], inserts: &[String]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for stmt in ddl {
        f.execute(stmt).unwrap();
        r.execute(stmt, []).unwrap();
    }
    for stmt in inserts {
        f.execute(stmt).unwrap();
        r.execute(stmt, []).unwrap();
    }
    (f, r)
}

fn assert_same(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let fv = f_rows(f, sql);
    let rv = r_rows(r, sql);
    assert_eq!(
        fv, rv,
        "\nMISMATCH for `{sql}`\n fsqlite:\n{fv}\n rusqlite:\n{rv}\n"
    );
}

#[test]
fn distinct_loose_scan_matches_rusqlite() {
    // ---- Main eligible table: few distinct values among many rows, with a NULL run. ----
    let mut ins = Vec::new();
    for i in 1..=2000 {
        let a = if i % 37 == 0 {
            "NULL".to_string()
        } else {
            (i % 12).to_string()
        };
        ins.push(format!("INSERT INTO t VALUES ({i}, {a}, {});", i % 5));
    }
    let (f, r) = both(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
            "CREATE INDEX idx_a ON t(a);",
        ],
        &ins,
    );
    // The loose scan must have fired (no dedup sorter) for the eligible shape.
    assert!(
        !has_op(&f, "SELECT DISTINCT a FROM t", "SorterOpen"),
        "loose scan should serve SELECT DISTINCT a (no SorterOpen)"
    );
    assert_same(&f, &r, "SELECT DISTINCT a FROM t");
    assert_same(&f, &r, "SELECT DISTINCT a FROM t ORDER BY a");

    // ---- Text column, dup runs + NULLs. ----
    let mut tins = Vec::new();
    for i in 1..=500 {
        let s = if i % 23 == 0 {
            "NULL".to_string()
        } else {
            format!("'k{:02}'", i % 8)
        };
        tins.push(format!("INSERT INTO tt VALUES ({i}, {s});"));
    }
    let (f2, r2) = both(
        &[
            "CREATE TABLE tt (id INTEGER PRIMARY KEY, s TEXT);",
            "CREATE INDEX idx_s ON tt(s);",
        ],
        &tins,
    );
    assert!(!has_op(&f2, "SELECT DISTINCT s FROM tt", "SorterOpen"));
    assert_same(&f2, &r2, "SELECT DISTINCT s FROM tt");

    // ---- Edge tables. ----
    for (label, rows) in [
        ("empty", vec![]),
        ("single", vec!["INSERT INTO e VALUES (1, 42);".to_string()]),
        (
            "all-same",
            (1..=50)
                .map(|i| format!("INSERT INTO e VALUES ({i}, 7);"))
                .collect(),
        ),
        (
            "all-null",
            (1..=50)
                .map(|i| format!("INSERT INTO e VALUES ({i}, NULL);"))
                .collect(),
        ),
    ] {
        let (fe, re) = both(
            &[
                "CREATE TABLE e (id INTEGER PRIMARY KEY, a INTEGER);",
                "CREATE INDEX idx_e ON e(a);",
            ],
            &rows,
        );
        assert_same(&fe, &re, "SELECT DISTINCT a FROM e");
        let _ = label;
    }

    // ---- Control: NOCASE column must DECLINE (fall back) but still be correct. ----
    let (fc, rc) = both(
        &[
            "CREATE TABLE nc (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE);",
            "CREATE INDEX idx_nc ON nc(s);",
        ],
        &[
            "INSERT INTO nc VALUES (1, 'Apple');".to_string(),
            "INSERT INTO nc VALUES (2, 'apple');".to_string(),
            "INSERT INTO nc VALUES (3, 'BANANA');".to_string(),
            "INSERT INTO nc VALUES (4, 'banana');".to_string(),
        ],
    );
    assert_same(&fc, &rc, "SELECT DISTINCT s FROM nc");

    // ---- Control: non-indexed column falls back. ----
    let (fn_, rn) = both(
        &["CREATE TABLE ni (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);"],
        &(1..=100)
            .map(|i| format!("INSERT INTO ni VALUES ({i}, {}, {});", i % 6, i % 6))
            .collect::<Vec<_>>(),
    );
    assert_same(&fn_, &rn, "SELECT DISTINCT b FROM ni");
}

/// Cross-storage-class runs: a typeless (BLOB-affinity) column stores `2` and `2.0` as DISTINCT
/// storage classes that COMPARE equal, so one loose-scan skip must clear the mixed run exactly like
/// C SQLite's index scan. REAL columns exercise float-key runs, and negative/boundary integers pin
/// the varint edges of the probe record.
#[test]
fn distinct_loose_scan_mixed_storage_classes_match_rusqlite() {
    // ---- Typeless column: int 2 and float 2.0 compare equal but store differently. ----
    let mut ins = Vec::new();
    for i in 1..=300 {
        let v = match i % 6 {
            0 => "2".to_string(),
            1 => "2.0".to_string(),
            2 => "7".to_string(),
            3 => "7.5".to_string(),
            4 => "'txt'".to_string(),
            _ => "NULL".to_string(),
        };
        ins.push(format!("INSERT INTO m VALUES ({i}, {v});"));
    }
    let (f, r) = both(
        &[
            "CREATE TABLE m (id INTEGER PRIMARY KEY, v);",
            "CREATE INDEX idx_m ON m(v);",
        ],
        &ins,
    );
    assert_same(&f, &r, "SELECT DISTINCT v FROM m");

    // ---- REAL column: float duplicate runs, negatives, and integer-valued floats. ----
    let mut rins = Vec::new();
    for i in 1..=400 {
        let v = match i % 5 {
            0 => "-3.25",
            1 => "-3.25",
            2 => "0.0",
            3 => "9007199254740993.0",
            _ => "1.5",
        };
        rins.push(format!("INSERT INTO fr VALUES ({i}, {v});"));
    }
    let (f2, r2) = both(
        &[
            "CREATE TABLE fr (id INTEGER PRIMARY KEY, v REAL);",
            "CREATE INDEX idx_fr ON fr(v);",
        ],
        &rins,
    );
    assert_same(&f2, &r2, "SELECT DISTINCT v FROM fr");

    // ---- Integer boundary values: i64::MIN/MAX runs must not confuse the probe. ----
    let mut bins = Vec::new();
    for i in 1..=90 {
        let v = match i % 3 {
            0 => i64::MIN.to_string(),
            1 => i64::MAX.to_string(),
            _ => "0".to_string(),
        };
        bins.push(format!("INSERT INTO bt VALUES ({i}, {v});"));
    }
    let (f3, r3) = both(
        &[
            "CREATE TABLE bt (id INTEGER PRIMARY KEY, v INTEGER);",
            "CREATE INDEX idx_bt ON bt(v);",
        ],
        &bins,
    );
    assert_same(&f3, &r3, "SELECT DISTINCT v FROM bt");
}

/// File-backed variant of the main eligible shape: the loose scan must behave identically through
/// the pager/transaction cursor stack (`CursorBackend::Txn`), not just the `:memory:` image.
#[test]
fn distinct_loose_scan_file_backed_matches_rusqlite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("loose_scan.db");
    let f = Connection::open(path.to_str().unwrap()).unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();

    for stmt in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER);",
        "CREATE INDEX idx_a ON t(a);",
    ] {
        f.execute(stmt).unwrap();
        r.execute(stmt, []).unwrap();
    }
    f.execute("BEGIN;").unwrap();
    for i in 1..=1200 {
        let a = if i % 31 == 0 {
            "NULL".to_string()
        } else {
            (i % 9).to_string()
        };
        let stmt = format!("INSERT INTO t VALUES ({i}, {a});");
        f.execute(&stmt).unwrap();
        r.execute(&stmt, []).unwrap();
    }
    f.execute("COMMIT;").unwrap();

    assert_same(&f, &r, "SELECT DISTINCT a FROM t");
    assert_same(&f, &r, "SELECT DISTINCT a FROM t ORDER BY a");
}
