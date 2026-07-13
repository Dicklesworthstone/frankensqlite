//! Composite-prefix MIN/MAX differential oracle against C SQLite.
//!
//! Covers `(a, b ASC)` and `(a, b DESC)` across ordinary hits, mixed/all-NULL `b` groups, absent and
//! boundary prefixes, `a = NULL`, wrappers, and empty tables. The DESC `MAX(b)` and both ASC extrema
//! use the single-row covering seek. DESC `MIN(b)` uses the covering equality-prefix walk; its
//! partial-key probe must enter the `a=?` block regardless of the next term's direction.

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

fn populate_prefix_table(
    f: &Connection,
    r: &rusqlite::Connection,
    table: &str,
    index: &str,
    direction: &str,
) {
    for stmt in [
        format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, u INTEGER);"),
        format!("CREATE INDEX {index} ON {table}(a, b {direction});"),
    ] {
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }

    // Ten a-groups. a=3 mixes NULL/non-NULL b; a=8 has only NULL b.
    for i in 1..=600_i64 {
        let a = i % 10;
        let raw = (i.wrapping_mul(2_654_435_761) >> 8) & 0x3ff;
        let b = if a == 8 || (a == 3 && i % 3 == 0) {
            "NULL".to_owned()
        } else {
            format!("{}", raw - 500)
        };
        let u = (i.wrapping_mul(5)) % 40;
        let stmt = format!("INSERT INTO {table} VALUES ({i}, {a}, {b}, {u});");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }
}

#[test]
fn minmax_prefix_desc_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for (table, index, direction) in [
        ("t_desc", "idx_ab_desc", "DESC"),
        ("t_asc", "idx_ab_asc", "ASC"),
    ] {
        populate_prefix_table(&f, &r, table, index, direction);
    }

    let cmp = |sql: &str| {
        assert_eq!(
            frank_rows(&f, sql),
            sqlite_rows(&r, sql),
            "diverged: `{sql}`"
        );
    };

    for table in ["t_desc", "t_asc"] {
        for aggregate in ["MIN", "MAX"] {
            for predicate in [
                "a = 5",    // ordinary hit
                "a = 3",    // mixed NULL/non-NULL b
                "a = 8",    // all-NULL b -> NULL
                "a = 0",    // lowest live prefix
                "a = 9",    // highest live prefix
                "a = 999",  // absent above range -> NULL
                "a = -1",   // absent below range -> NULL
                "a = NULL", // SQL equality with NULL matches nothing
            ] {
                cmp(&format!(
                    "SELECT {aggregate}(b) FROM {table} WHERE {predicate}"
                ));
            }
            for predicate in ["a = 999", "a = 8"] {
                cmp(&format!(
                    "SELECT COALESCE({aggregate}(b), -1) FROM {table} WHERE {predicate}"
                ));
            }
        }

        // These drive the same general equality-prefix probe directly. In particular, COUNT(*) must
        // not lose rows whose trailing `b` key is NULL on the ASC index, nor miss a DESC block.
        for predicate in ["a = 5", "a = 3", "a = 8", "a = 999", "a = NULL"] {
            cmp(&format!("SELECT COUNT(*) FROM {table} WHERE {predicate}"));
        }
    }

    // Empty tables in both index directions.
    for (table, index, direction) in [
        ("e_desc", "idx_eab_desc", "DESC"),
        ("e_asc", "idx_eab_asc", "ASC"),
    ] {
        for stmt in [
            format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);"),
            format!("CREATE INDEX {index} ON {table}(a, b {direction});"),
        ] {
            f.execute(&stmt).unwrap();
            r.execute_batch(&stmt).unwrap();
        }
        for aggregate in ["MIN", "MAX"] {
            cmp(&format!("SELECT {aggregate}(b) FROM {table} WHERE a = 1"));
        }
    }

    // DESC MAX: one covering seek to the block front. DESC MIN: covering prefix walk over index keys.
    assert_eq!(
        seek_shape(&f, "SELECT MAX(b) FROM t_desc WHERE a = 5"),
        (true, false, false),
        "MAX(b) on (a, b DESC) must SeekGE the block front, covering (no SeekLE / SeekRowid)"
    );
    assert_eq!(
        seek_shape(&f, "SELECT MIN(b) FROM t_desc WHERE a = 5"),
        (true, false, false),
        "MIN(b) on (a, b DESC) must walk the sought prefix using covering index keys"
    );

    // ASC extrema each use their one-row covering end seek.
    assert_eq!(
        seek_shape(&f, "SELECT MIN(b) FROM t_asc WHERE a = 5"),
        (true, false, false),
        "MIN(b) on (a, b ASC) must SeekGE the block front without a table lookup"
    );
    assert_eq!(
        seek_shape(&f, "SELECT MAX(b) FROM t_asc WHERE a = 5"),
        (false, true, false),
        "MAX(b) on (a, b ASC) must SeekLE the block end without a table lookup"
    );
}
