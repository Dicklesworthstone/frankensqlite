//! bd-zq6dp — Oracle-parity e2e: a secondary-index range predicate on a numeric column
//! now seeks the index instead of full-scanning.
//!
//! `index_range_fast_path_is_safe` used to gate the non-aggregate index-range seek on
//! `(cmp_p5 & !0x80) == 0`, which an INTEGER/REAL/NUMERIC column carries a NUMERIC
//! comparison affinity against — so `SELECT ... FROM t WHERE <num col> BETWEEN a AND b`
//! silently degraded to a full table scan (EQP claimed `SEARCH USING INDEX` while the
//! emitted bytecode was `Rewind`+`Next`). The gate now also accepts a NUMERIC affinity
//! against a numeric *literal* bound, where the coercion is the identity — the same
//! proven-safe subset the IN-list seek accepts.
//!
//! This asserts (1) results stay bit-identical to real SQLite across many range shapes
//! (covering + non-covering, integer/real/numeric columns, real bounds, negatives, NULL
//! rows, empty ranges, ORDER BY, and every decline case), and (2) the emitted program
//! actually seeks for the accepted shapes and still scans for the declined ones — so the
//! optimization changes speed, not semantics.

// Row-count / small-int casts in fixtures; precision loss is irrelevant to the assertions.
#![allow(clippy::cast_precision_loss)]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

fn setup() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    let mut stmts = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, rr REAL, nn NUMERIC, w TEXT);"
            .to_owned(),
        "CREATE INDEX idx_t_k ON t(k);".to_owned(),
        "CREATE INDEX idx_t_rr ON t(rr);".to_owned(),
        "CREATE INDEX idx_t_nn ON t(nn);".to_owned(),
        "CREATE INDEX idx_t_w ON t(w);".to_owned(),
        // Expression index: `is_index_range_constant` (broadened to accept negated numeric
        // literals) is shared by the expression-index range extractor, so a `(k - 5) BETWEEN
        // -8 AND 0` bound must also stay bit-identical whether it seeks or scans.
        "CREATE INDEX idx_t_kx ON t(k - 5);".to_owned(),
    ];
    for i in 1..=60_i64 {
        let k = (i % 12) - 3; // -3..8, duplicates, negatives
        let rr = (i as f64) * 0.5 - 4.0; // negatives and fractional
        let nn = if i % 2 == 0 {
            format!("{}", i - 20)
        } else {
            format!("{}.5", i - 20)
        };
        stmts.push(format!(
            "INSERT INTO t VALUES ({i}, {k}, {rr}, {nn}, 'r{:02}');",
            i % 20
        ));
    }
    // NULL in every indexed column.
    stmts.push("INSERT INTO t VALUES (200, NULL, NULL, NULL, NULL);".to_owned());
    for s in &stmts {
        f.execute(s).unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

fn opcodes(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN {sql}"))
        .unwrap_or_else(|e| panic!("EXPLAIN `{sql}`: {e}"))
        .iter()
        .filter_map(|row| match row.values().get(1) {
            Some(SqliteValue::Text(op)) => Some(op.to_string()),
            _ => None,
        })
        .collect()
}

/// The index-range scan (`codegen_select_index_range_scan`) always dereferences the
/// index entry's rowid via `IdxRowid` on a rowid table (for covering reads and table
/// lookups alike), while a full table scan (`codegen_select_full_scan`) never emits it.
/// So `IdxRowid` presence is the clean signal that the seek engaged rather than scanned —
/// robust across both a SeekGE-anchored range and a one-sided upper bound (which Rewinds
/// the *index* cursor, not the table).
fn uses_index(ops: &[String]) -> bool {
    ops.iter().any(|o| o == "IdxRowid")
}

#[test]
fn index_range_seek_matches_sqlite() {
    let (f, r) = setup();
    let sorted = |res: Result<Vec<Vec<String>>, String>| -> Result<Vec<Vec<String>>, String> {
        res.map(|mut rows| {
            rows.sort();
            rows
        })
    };

    // No ORDER BY -> compare as SETS (row order unspecified; a declined case may scan in
    // rowid order while the seek yields index order — both valid).
    let set_eq = [
        // INTEGER column, covering (id = rowid via IdxRowid) — the shape the gate rejected.
        "SELECT id FROM t WHERE k > 5",
        "SELECT id FROM t WHERE k >= 5",
        "SELECT id FROM t WHERE k < 0",
        "SELECT id FROM t WHERE k <= 0",
        "SELECT id FROM t WHERE k BETWEEN 2 AND 5",
        "SELECT id FROM t WHERE k > 2 AND k < 6",
        "SELECT id FROM t WHERE k > -3 AND k <= 0",
        "SELECT id, k FROM t WHERE k >= 6",
        "SELECT k FROM t WHERE k BETWEEN 1 AND 4",
        // Real-valued bounds on an INTEGER column (NUMERIC coercion, still a numeric literal).
        "SELECT id FROM t WHERE k > 2.5",
        "SELECT id FROM t WHERE k <= 3.0",
        "SELECT id FROM t WHERE k BETWEEN -2.5 AND 4.5",
        // REAL column.
        "SELECT id FROM t WHERE rr > 2.5",
        "SELECT id, rr FROM t WHERE rr BETWEEN -1.0 AND 4.0",
        "SELECT rr FROM t WHERE rr >= 3",
        // NUMERIC column (mixed int/real stored).
        "SELECT id FROM t WHERE nn > 2.5",
        "SELECT id, nn FROM t WHERE nn BETWEEN -5 AND 5",
        // Empty / edge ranges.
        "SELECT id FROM t WHERE k > 9999",
        "SELECT id FROM t WHERE k < -9999",
        "SELECT id FROM t WHERE k BETWEEN 100 AND 200",
        // Non-covering (w not in idx_t_k) — must still match whatever path is chosen.
        "SELECT id, w FROM t WHERE k BETWEEN 2 AND 5",
        "SELECT rr, w FROM t WHERE k > 5",
        // Declines that must still be correct: text literal on a numeric column (NUMERIC
        // affinity but not a numeric literal -> scan), NOT INDEXED, DISTINCT.
        "SELECT id FROM t WHERE k > '3'",
        "SELECT id FROM t NOT INDEXED WHERE k BETWEEN 2 AND 5",
        // TEXT column range (TEXT comparison affinity -> declines to a scan; still exact).
        "SELECT id FROM t WHERE w > 'r05'",
        "SELECT id FROM t WHERE w BETWEEN 'r03' AND 'r08'",
        // Expression-index range with NEGATED numeric literal bounds (broadened extraction).
        "SELECT id FROM t WHERE k - 5 BETWEEN -8 AND 0",
        "SELECT id FROM t WHERE k - 5 > -6",
        "SELECT id FROM t WHERE k - 5 <= -1",
    ];
    for sql in set_eq {
        assert_eq!(
            sorted(frank_rows(&f, sql)),
            sorted(sqlite_rows(&r, sql)),
            "index-range seek (row set) diverged from SQLite for `{sql}`"
        );
    }

    // ORDER BY / DISTINCT / LIMIT -> exact comparison.
    let exact = [
        "SELECT id FROM t WHERE k > 5 ORDER BY id",
        "SELECT id, k FROM t WHERE k BETWEEN 2 AND 5 ORDER BY k, id",
        "SELECT id FROM t WHERE k >= 6 ORDER BY id DESC",
        "SELECT id FROM t WHERE rr > 2.5 ORDER BY id",
        "SELECT DISTINCT k FROM t WHERE k BETWEEN 1 AND 4 ORDER BY k",
        "SELECT id FROM t WHERE k BETWEEN 2 AND 8 ORDER BY id LIMIT 3",
        "SELECT COUNT(*) FROM t WHERE k > 5",
    ];
    for sql in exact {
        assert_eq!(
            frank_rows(&f, sql),
            sqlite_rows(&r, sql),
            "index-range seek (ordered/declined) diverged from SQLite for `{sql}`"
        );
    }
}

#[test]
fn index_range_seek_emits_seek_for_numeric_literals() {
    let (f, _r) = setup();

    // Accepted numeric-literal shapes must actually walk the index (IdxRowid), proving the
    // optimization engages rather than silently full-scanning the table.
    for sql in [
        "SELECT id FROM t WHERE k BETWEEN 5 AND 55",
        "SELECT id, k FROM t WHERE k > 5",
        "SELECT id FROM t WHERE k < 0",
        "SELECT id FROM t WHERE k > 2.5",
        "SELECT id FROM t WHERE rr BETWEEN -1.0 AND 4.0",
        "SELECT id FROM t WHERE nn > 2.5",
    ] {
        let ops = opcodes(&f, sql);
        assert!(
            uses_index(&ops),
            "accepted numeric-literal range must seek the index for `{sql}`; ops = {ops:?}"
        );
    }

    // Declined shapes must keep the correct full scan (no index walk): a placeholder bound
    // (runtime type unknown), NOT INDEXED, and a text literal on a numeric column.
    for sql in [
        "SELECT id FROM t WHERE k > ?1",
        "SELECT id, k FROM t WHERE k BETWEEN ?1 AND ?2",
        "SELECT id FROM t NOT INDEXED WHERE k BETWEEN 2 AND 5",
        "SELECT id FROM t WHERE k > '3'",
    ] {
        let ops = opcodes(&f, sql);
        assert!(
            !uses_index(&ops),
            "declined range must NOT seek (keeps the full scan) for `{sql}`; ops = {ops:?}"
        );
    }
}
