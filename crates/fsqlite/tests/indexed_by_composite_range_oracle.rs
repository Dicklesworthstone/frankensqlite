//! Regression for GH #291: `INDEXED BY` naming a composite index must take the
//! bounded equality-prefix + trailing-range seek.
//!
//! Pre-fix, ANY index hint declined the composite seek, and because the
//! IndexRange planner directive also rejects composite key shapes, the hinted
//! query fell all the way back to a FULL TABLE SCAN — a 593-row range lookup
//! on a 9 GB database allocated multi-gigabyte RSS while native SQLite
//! answered instantly. The plan assertions below pin the seek; the oracle
//! comparison pins row-level correctness against C SQLite.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn opcodes(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN {sql}"))
        .unwrap_or_else(|e| panic!("explain `{sql}`: {e}"))
        .iter()
        .filter_map(|row| match row.values().get(1) {
            Some(SqliteValue::Text(op)) => Some(op.to_string()),
            _ => None,
        })
        .collect()
}

fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f:?}"),
                    SqliteValue::Text(s) => format!("'{s}'"),
                    SqliteValue::Blob(_) => "blob".to_owned(),
                })
                .collect()
        })
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
                rusqlite::types::Value::Blob(_) => "blob".to_owned(),
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

fn assert_rows_match(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let mut a = frank_rows(f, sql);
    let mut b = sqlite_rows(r, sql);
    a.sort();
    b.sort();
    assert_eq!(a, b, "result diverged from C SQLite: `{sql}`");
}

fn seed(f: &Connection, r: &rusqlite::Connection) {
    // The exact CASS shape from GH #291: UNIQUE(conversation_id, idx) gives
    // the two-key-term autoindex `sqlite_autoindex_messages_1`.
    for stmt in [
        "CREATE TABLE messages (id INTEGER PRIMARY KEY, conversation_id TEXT NOT NULL, \
         idx INTEGER NOT NULL, body TEXT, UNIQUE(conversation_id, idx));",
        "CREATE TABLE events (id INTEGER PRIMARY KEY, kind INTEGER NOT NULL, \
         seq INTEGER NOT NULL, note TEXT);",
        "CREATE INDEX idx_events_kind_seq ON events(kind, seq);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    let mut id = 0_i64;
    for conv in 0..25_i64 {
        for idx in 0..40_i64 {
            id += 1;
            let stmt =
                format!("INSERT INTO messages VALUES ({id}, 'c{conv}', {idx}, 'm-{conv}-{idx}');");
            f.execute(&stmt).unwrap();
            r.execute_batch(&stmt).unwrap();
            let stmt = format!(
                "INSERT INTO events VALUES ({id}, {}, {}, 'e-{id}');",
                conv % 7,
                idx
            );
            f.execute(&stmt).unwrap();
            r.execute_batch(&stmt).unwrap();
        }
    }
}

#[test]
fn indexed_by_composite_range_seeks_instead_of_full_scan() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    seed(&f, &r);

    let hinted_autoindex = "SELECT idx FROM messages INDEXED BY sqlite_autoindex_messages_1 \
         WHERE conversation_id = 'c7' AND idx >= 5 AND idx <= 34";
    let unhinted = "SELECT idx FROM messages \
         WHERE conversation_id = 'c7' AND idx >= 5 AND idx <= 34";
    let hinted_named = "SELECT seq FROM events INDEXED BY idx_events_kind_seq \
         WHERE kind = 3 AND seq >= 2 AND seq <= 30";
    let not_indexed = "SELECT idx FROM messages NOT INDEXED \
         WHERE conversation_id = 'c7' AND idx >= 5 AND idx <= 34";

    // GH #291 plan pin: the hinted queries must SEEK the composite index and
    // must not Rewind (full-scan) the table.
    for sql in [hinted_autoindex, unhinted, hinted_named] {
        let ops = opcodes(&f, sql);
        assert!(
            ops.iter().any(|op| op.starts_with("Seek")),
            "expected a composite index seek for `{sql}`, got ops {ops:?}"
        );
        assert!(
            !ops.iter().any(|op| op == "Rewind"),
            "hinted composite range must not degrade to a full scan (GH #291) \
             for `{sql}`, got ops {ops:?}"
        );
    }

    // `NOT INDEXED` demands a table scan by definition — it must keep one.
    let ops = opcodes(&f, not_indexed);
    assert!(
        ops.iter().any(|op| op == "Rewind"),
        "NOT INDEXED must full-scan, got ops {ops:?}"
    );

    // Row-level oracle: all four shapes match C SQLite exactly.
    for sql in [hinted_autoindex, unhinted, hinted_named, not_indexed] {
        assert_rows_match(&f, &r, sql);
    }

    // Hinting an index that cannot serve the shape must not mis-seek: a hint
    // naming a different table's index is a prepare-time error in SQLite, so
    // use a mismatched shape instead (leading-column range) and just require
    // oracle-identical rows.
    let mismatched = "SELECT idx FROM messages INDEXED BY sqlite_autoindex_messages_1 \
         WHERE idx >= 5 AND idx <= 6";
    assert_rows_match(&f, &r, mismatched);
}
