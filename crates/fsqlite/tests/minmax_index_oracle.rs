//! bd-minmax-index-seek: `SELECT MIN(col)`/`MAX(col)` on a secondary-indexed column now resolves to
//! a single index seek (was an O(n) scan). HARD GATE: the result must be byte-identical to C SQLite
//! (rusqlite) across the correctness surface a value extremum touches — leading NULLs (MIN must skip
//! them), all-NULL and empty tables, INT/REAL/TEXT/BLOB columns, NOCASE collation, DESC index
//! (uses direction-aware index-end seeks), the COALESCE wrapper, and mixed sign/dup values. Plus an
//! opcode gate: the fast path opens the index and emits the direction-appropriate seek/NULL-skip
//! opcodes with NO Rowid-scan of the table.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// True when the EXPLAIN of `sql` opens an index cursor and contains no `Rowid` (full table walk of
/// the aggregate column) — i.e. the MIN/MAX-via-index fast path fired.
fn uses_index_seek(conn: &Connection, sql: &str) -> bool {
    let rows = conn.query(&format!("EXPLAIN {sql}")).unwrap();
    let mut opened_index = false;
    let mut has_rowid_walk = false;
    for row in &rows {
        let vals = row.values();
        if let Some(SqliteValue::Text(op)) = vals.get(1) {
            let op = op.to_string();
            // OpenRead on an index is marked by a P4 index name (col 6 in EXPLAIN is p4/comment).
            if op == "OpenRead" {
                // p3 (col 4) is 0 for the index-only open; distinguish via the P4 comment.
                if let Some(SqliteValue::Text(p4)) = vals.get(5) {
                    if p4.to_string().to_lowercase().contains("idx") {
                        opened_index = true;
                    }
                }
            }
            if op == "Rowid" {
                has_rowid_walk = true;
            }
        }
    }
    opened_index && !has_rowid_walk
}

fn has_op(conn: &Connection, sql: &str, want: &str) -> bool {
    conn.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(
        |row| matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string() == want),
    )
}

#[test]
fn minmax_index_seek_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    let schema = [
        // v INT indexed ASC, w TEXT indexed ASC, x REAL indexed ASC, c TEXT indexed NOCASE,
        // d INT indexed DESC (direction-aware end seek), u INT NOT indexed (control).
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, w TEXT, x REAL, c TEXT COLLATE NOCASE, d INTEGER, u INTEGER);",
        "CREATE INDEX idx_v ON t(v);",
        "CREATE INDEX idx_w ON t(w);",
        "CREATE INDEX idx_x ON t(x);",
        "CREATE INDEX idx_c ON t(c);",
        "CREATE INDEX idx_d ON t(d DESC);",
    ];
    for s in schema {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    // Rows with leading NULLs (v NULL for the first several ids) so MIN must skip the NULL region;
    // pseudo-shuffled values; mixed sign; text case variety for NOCASE.
    for i in 1..=400_i64 {
        let has_null = i <= 5; // ids 1..=5 have NULL v / w / x / c
        let v = if has_null {
            "NULL".to_owned()
        } else {
            format!("{}", ((i.wrapping_mul(7)) % 211) - 100)
        };
        let w = if has_null {
            "NULL".to_owned()
        } else {
            format!("'w{:04}'", (i.wrapping_mul(13)) % 500)
        };
        let x = if has_null {
            "NULL".to_owned()
        } else {
            format!("{}.{}", (i.wrapping_mul(3)) % 90, i % 10)
        };
        let c = if has_null {
            "NULL".to_owned()
        } else if i % 2 == 0 {
            format!("'ITEM{:03}'", (i.wrapping_mul(11)) % 300)
        } else {
            format!("'item{:03}'", (i.wrapping_mul(11)) % 300)
        };
        let d = ((i.wrapping_mul(17)) % 333) - 150;
        let u = (i.wrapping_mul(5)) % 400;
        let stmt = format!("INSERT INTO t VALUES ({i}, {v}, {w}, {x}, {c}, {d}, {u});");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }

    let cmp = |sql: &str| {
        assert_eq!(
            frank_rows(&f, sql),
            sqlite_rows(&r, sql),
            "MIN/MAX diverged: `{sql}`"
        );
    };
    let queries = [
        // INT indexed: MIN skips leading NULLs; MAX is the last entry.
        "SELECT MIN(v) FROM t",
        "SELECT MAX(v) FROM t",
        // TEXT indexed.
        "SELECT MIN(w) FROM t",
        "SELECT MAX(w) FROM t",
        // REAL indexed.
        "SELECT MIN(x) FROM t",
        "SELECT MAX(x) FROM t",
        // NOCASE-collated TEXT: the fast path must use the NOCASE index (matching collation).
        "SELECT MIN(c) FROM t",
        "SELECT MAX(c) FROM t",
        // DESC index: seeks the opposite index ends from ASC, and must still match.
        "SELECT MIN(d) FROM t",
        "SELECT MAX(d) FROM t",
        // NOT indexed control.
        "SELECT MIN(u) FROM t",
        "SELECT MAX(u) FROM t",
        // COALESCE wrapper around the extremum (applied after finalize).
        "SELECT COALESCE(MAX(v), -1) FROM t",
        "SELECT COALESCE(MIN(v), -1) FROM t",
        // Explicit COLLATE forcing BINARY on a NOCASE column: must NOT use the NOCASE index.
        "SELECT MIN(c COLLATE BINARY) FROM t",
        "SELECT MAX(c COLLATE BINARY) FROM t",
    ];
    for sql in queries {
        cmp(sql);
    }

    // All-NULL column: MIN and MAX are both NULL.
    for s in [
        "CREATE TABLE n (id INTEGER PRIMARY KEY, v INTEGER);",
        "CREATE INDEX idx_nv ON n(v);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=50 {
        let stmt = format!("INSERT INTO n VALUES ({i}, NULL);");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }
    cmp("SELECT MIN(v) FROM n");
    cmp("SELECT MAX(v) FROM n");

    // Empty table: MIN/MAX → NULL.
    for s in [
        "CREATE TABLE e (id INTEGER PRIMARY KEY, v INTEGER);",
        "CREATE INDEX idx_ev ON e(v);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    cmp("SELECT MIN(v) FROM e");
    cmp("SELECT MAX(v) FROM e");
    cmp("SELECT COALESCE(MIN(v), 99) FROM e");

    // Opcode gate: ASC and DESC indexed MIN/MAX use index-end seeks; unsafe shapes still decline.
    assert!(
        uses_index_seek(&f, "SELECT MIN(v) FROM t"),
        "MIN(v) (indexed ASC) must use the index seek"
    );
    assert!(
        uses_index_seek(&f, "SELECT MAX(v) FROM t"),
        "MAX(v) (indexed ASC) must use the index seek"
    );
    assert!(
        uses_index_seek(&f, "SELECT MIN(c) FROM t"),
        "MIN(c) (NOCASE index, NOCASE compare) must use the index seek"
    );
    assert!(
        !uses_index_seek(&f, "SELECT MIN(u) FROM t"),
        "MIN(u) (no index) must full-scan"
    );
    assert!(
        uses_index_seek(&f, "SELECT MIN(d) FROM t")
            && has_op(&f, "SELECT MIN(d) FROM t", "Last")
            && has_op(&f, "SELECT MIN(d) FROM t", "Prev"),
        "MIN(d) (DESC index) must seek Last and walk back through trailing NULLs with Prev"
    );
    assert!(
        uses_index_seek(&f, "SELECT MAX(d) FROM t")
            && has_op(&f, "SELECT MAX(d) FROM t", "Rewind")
            && !has_op(&f, "SELECT MAX(d) FROM t", "Next"),
        "MAX(d) (DESC index) must use a single Rewind end seek without a Next scan"
    );
    assert!(
        !uses_index_seek(&f, "SELECT MIN(c COLLATE BINARY) FROM t"),
        "MIN(c COLLATE BINARY) (NOCASE index, BINARY compare) must not use the mismatched index"
    );
}
