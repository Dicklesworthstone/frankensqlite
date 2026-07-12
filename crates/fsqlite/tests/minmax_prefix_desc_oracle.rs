//! bd-minmax-prefix-seek follow-on: `SELECT MAX(b) FROM t WHERE a = <const>` on a `(a, b DESC)` index
//! now seeks the block's FIRST entry (`SeekGE`, O(log n) — no O(block) walk), since a DESC second term
//! keeps the max `b` at the front of the `a=?` block. HARD GATE: byte-identical to C SQLite (rusqlite)
//! across a NULL-mixed group, an all-NULL-`b` group and an absent group (→ NULL), boundary `a` values,
//! and the COALESCE wrapper; `MIN(b)` over a DESC second term declines to the scan and must still match.
//! Opcode gate: MAX(b) uses SeekGE, covering (no SeekLE / no SeekRowid); MIN(b) declines.

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

/// (has SeekGE, has SeekLE, has SeekRowid) in the EXPLAIN of `sql`.
fn seek_shape(conn: &Connection, sql: &str) -> (bool, bool, bool) {
    let rows = conn.query(&format!("EXPLAIN {sql}")).unwrap();
    let (mut ge, mut le, mut rowid) = (false, false, false);
    for row in &rows {
        if let Some(SqliteValue::Text(op)) = row.values().get(1) {
            match op.to_string().as_str() {
                "SeekGE" => ge = true,
                "SeekLE" => le = true,
                "SeekRowid" => rowid = true,
                _ => {}
            }
        }
    }
    (ge, le, rowid)
}

#[test]
fn minmax_prefix_desc_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, u INTEGER);",
        "CREATE INDEX idx_ab_desc ON t(a, b DESC);", // DESC second term -> MAX(b) via SeekGE
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    // 10 a-groups. a=3 has some NULL b; a=8 has ALL NULL b.
    for i in 1..=600_i64 {
        let a = i % 10;
        let raw = (i.wrapping_mul(2_654_435_761) >> 8) & 0x3ff;
        let b = if a == 8 || (a == 3 && i % 3 == 0) {
            "NULL".to_owned()
        } else {
            format!("{}", (raw as i64) - 500)
        };
        let u = (i.wrapping_mul(5)) % 40;
        let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {u});");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }

    let cmp = |sql: &str| {
        assert_eq!(
            frank_rows(&f, sql),
            sqlite_rows(&r, sql),
            "diverged: `{sql}`"
        );
    };
    for sql in [
        "SELECT MAX(b) FROM t WHERE a = 5",
        "SELECT MAX(b) FROM t WHERE a = 3", // some NULL b
        "SELECT MAX(b) FROM t WHERE a = 8", // all NULL b -> NULL
        "SELECT MAX(b) FROM t WHERE a = 0",
        "SELECT MAX(b) FROM t WHERE a = 9",
        "SELECT MAX(b) FROM t WHERE a = 999", // absent -> NULL
        "SELECT MAX(b) FROM t WHERE a = -1",  // below all -> NULL
        "SELECT COALESCE(MAX(b), -1) FROM t WHERE a = 999",
        "SELECT COALESCE(MAX(b), -1) FROM t WHERE a = 8",
        // MIN(b) over a DESC second term declines to the scan; must still match.
        "SELECT MIN(b) FROM t WHERE a = 5",
        "SELECT MIN(b) FROM t WHERE a = 3",
        // Controls.
        "SELECT COUNT(*) FROM t WHERE a = 5",
    ] {
        cmp(sql);
    }

    // Empty table.
    for stmt in [
        "CREATE TABLE e (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
        "CREATE INDEX idx_eab_desc ON e(a, b DESC);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    cmp("SELECT MAX(b) FROM e WHERE a = 1");

    // Opcode gate: MAX(b) uses the SeekGE prefix seek (covering, no SeekLE/SeekRowid); MIN(b) declines.
    assert_eq!(
        seek_shape(&f, "SELECT MAX(b) FROM t WHERE a = 5"),
        (true, false, false),
        "MAX(b) on (a, b DESC) must SeekGE the block front, covering (no SeekLE / SeekRowid)"
    );
    let (min_ge, _le, min_rowid) = seek_shape(&f, "SELECT MIN(b) FROM t WHERE a = 5");
    assert!(
        !min_ge || min_rowid,
        "MIN(b) over a DESC second term must not use the covering SeekGE prefix seek"
    );
}
