//! bd-ozk14 — Oracle-parity e2e: index features vs rusqlite (real SQLite).
//!
//! Index usage is invisible at the result level, so these verify that queries
//! over indexed tables produce the SAME rows as rusqlite regardless of plan —
//! exercising affinity in indexed `=`/range seeks, DESC indexes, multi-column
//! indexes, COLLATE indexes, partial indexes (`CREATE INDEX ... WHERE`), and
//! expression indexes (`CREATE INDEX ... ON t(expr)`). Each scenario asserts
//! per-statement success/failure agreement (a DDL rejection for an unsupported
//! index form is itself a finding), then compares query results.
#![recursion_limit = "512"]

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

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
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

async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn index_affinity_in_seek() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, i INTEGER, s TEXT)",
                "CREATE INDEX idx_i ON t(i)",
                "CREATE INDEX idx_s ON t(s)",
                "INSERT INTO t VALUES (1,5,'5'),(2,10,'10'),(3,20,'20'),(4,100,'100')",
            ],
            &[
                // text literal coerced to the indexed INTEGER column's affinity.
                "SELECT id FROM t WHERE i = '10' ORDER BY id",
                "SELECT id FROM t WHERE i > '5' ORDER BY i",
                // integer literal coerced to the indexed TEXT column's affinity.
                "SELECT id FROM t WHERE s = 10 ORDER BY id",
                "SELECT s FROM t WHERE s < '7' ORDER BY s",
            ],
            "index_affinity_in_seek",
        )
        .await;
    });
}

/// IN-list literals are not coerced to the column's comparison affinity, so an
/// INTEGER column tested against a text IN-list matches nothing. Tracked in
/// bd-cfmf6 (sibling of the BETWEEN-affinity gap bd-36kv6).
#[test]
fn index_in_list_affinity() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, i INTEGER)",
                "CREATE INDEX idx_i ON t(i)",
                "INSERT INTO t VALUES (1,5),(2,10),(3,20),(4,100)",
            ],
            &["SELECT id FROM t WHERE i IN ('10','100') ORDER BY id"],
            "index_in_list_affinity",
        )
        .await;
    });
}

#[test]
fn index_descending_and_multicolumn() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
                "CREATE INDEX idx_v_desc ON t(a DESC)",
                "CREATE INDEX idx_ab ON t(a, b)",
                "INSERT INTO t VALUES (1,2,9),(2,1,8),(3,2,7),(4,3,6),(5,1,5)",
            ],
            &[
                "SELECT id, a FROM t ORDER BY a DESC, id",
                "SELECT id FROM t WHERE a = 2 ORDER BY id",
                "SELECT id FROM t WHERE a = 1 AND b > 5 ORDER BY id",
                "SELECT a, b FROM t WHERE a >= 2 ORDER BY a, b",
            ],
            "index_descending_and_multicolumn",
        )
        .await;
    });
}

#[test]
fn index_collate_nocase() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
                "CREATE INDEX idx_name ON t(name COLLATE NOCASE)",
                "INSERT INTO t VALUES (1,'Alice'),(2,'BOB'),(3,'alice'),(4,'carol')",
            ],
            &[
                // A NOCASE index lookup must still respect the query's collation.
                "SELECT id FROM t WHERE name = 'ALICE' COLLATE NOCASE ORDER BY id",
                "SELECT id, name FROM t ORDER BY name COLLATE NOCASE, id",
                "SELECT count(*) FROM t WHERE name >= 'b' COLLATE NOCASE",
            ],
            "index_collate_nocase",
        )
        .await;
    });
}

#[test]
fn index_partial() {
    // Partial index: only rows satisfying the WHERE are indexed; query results
    // must be identical with or without using it.
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, active INTEGER)",
                "CREATE INDEX idx_active_v ON t(v) WHERE active = 1",
                "INSERT INTO t VALUES (1,10,1),(2,20,0),(3,30,1),(4,40,0),(5,50,1)",
            ],
            &[
                // Query whose predicate matches the partial-index condition.
                "SELECT id, v FROM t WHERE active = 1 AND v > 15 ORDER BY id",
                // Full result (must include non-indexed rows too).
                "SELECT id, v, active FROM t ORDER BY id",
                "SELECT v FROM t WHERE active = 1 ORDER BY v",
            ],
            "index_partial",
        )
        .await;
    });
}

/// GH#196 keeper (partial index): `INSERT OR REPLACE` must delete every index
/// entry derived from the victim row, including index forms whose keys cannot
/// be rebuilt from the victim payload by direct-column metadata.
///
/// Partial and expression indexes take the fallback path in
/// `fsqlite_vdbe::codegen::register_table_index_meta`, which signals an empty
/// `column_indices` list so the engine locates the victim entry by its
/// trailing rowid instead.
///
/// `PRAGMA integrity_check` is the load-bearing assertion here. A forced
/// `INDEXED BY` lookup is NOT sufficient on its own: a stale index entry still
/// carries the victim's rowid, which is re-seeked into the table, finds no row
/// once the victim is gone, and is silently filtered — so the forced lookup can
/// report zero rows while the stale entry is still on disk. Only the
/// cross-reference level of `integrity_check` compares index entries against
/// table rows and detects the orphan. The forced lookups are kept as secondary
/// checks, and the survivor probe proves the index is still usable afterwards
/// rather than merely empty.
#[test]
fn index_replace_victim_cleanup_partial_index() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE tp (id INTEGER PRIMARY KEY, u TEXT UNIQUE, a TEXT, flag INTEGER)",
                "CREATE INDEX pidx ON tp(a) WHERE flag = 1",
                "INSERT INTO tp VALUES (1,'k','victim',1)",
                // Conflict is on the secondary UNIQUE; the victim is the indexed row.
                "INSERT OR REPLACE INTO tp VALUES (2,'k','newrow',1)",
            ],
            &[
                // Primary assertion: orphaned index entries are cross-referenced.
                "PRAGMA integrity_check",
                // Table-row replacement, kept separate from index cleanup so a
                // failure distinguishes the two.
                "SELECT id, u, a, flag FROM tp ORDER BY id",
                // Secondary: forced lookup for the victim's old partial-index key.
                "SELECT count(*) FROM tp INDEXED BY pidx WHERE flag = 1 AND a = 'victim'",
                // Survivor must remain reachable *through pidx*, not just present.
                "SELECT id FROM tp INDEXED BY pidx WHERE flag = 1 AND a = 'newrow'",
            ],
            "index_replace_victim_cleanup_partial_index",
        )
        .await;
    });
}

/// GH#196 keeper (expression index): same schedule and same reasoning as
/// [`index_replace_victim_cleanup_partial_index`], over `lower(a)`.
#[test]
fn index_replace_victim_cleanup_expression_index() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE te (id INTEGER PRIMARY KEY, u TEXT UNIQUE, a TEXT)",
                "CREATE INDEX eidx ON te(lower(a))",
                "INSERT INTO te VALUES (1,'k2','VICTIM')",
                "INSERT OR REPLACE INTO te VALUES (2,'k2','NEWROW')",
            ],
            &[
                "PRAGMA integrity_check",
                "SELECT id, u, a FROM te ORDER BY id",
                "SELECT count(*) FROM te INDEXED BY eidx WHERE lower(a) = 'victim'",
                "SELECT id FROM te INDEXED BY eidx WHERE lower(a) = 'newrow'",
            ],
            "index_replace_victim_cleanup_expression_index",
        )
        .await;
    });
}

#[test]
fn index_on_expression() {
    // Expression index: CREATE INDEX ... ON t(expr); queries on the expression
    // should match regardless of whether the index is used.
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
                "CREATE INDEX idx_sum ON t(a + b)",
                "INSERT INTO t VALUES (1,1,9),(2,5,5),(3,2,3),(4,7,3),(5,4,6)",
            ],
            &[
                "SELECT id FROM t WHERE a + b = 10 ORDER BY id",
                "SELECT id, a + b AS s FROM t ORDER BY a + b, id",
                "SELECT count(*) FROM t WHERE a + b > 5",
            ],
            "index_on_expression",
        )
        .await;
    });
}

#[test]
fn index_covering_query() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)",
                "CREATE INDEX idx_kv ON t(k, v)",
                "INSERT INTO t VALUES (1,'a',10),(2,'b',20),(3,'a',30),(4,'b',5)",
            ],
            &[
                // Covering: both selected columns are in the index.
                "SELECT k, v FROM t WHERE k = 'a' ORDER BY v",
                "SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k",
                "SELECT v FROM t WHERE k = 'b' AND v < 10 ORDER BY v",
            ],
            "index_covering_query",
        )
        .await;
    });
}
