//! bd-pt5co — Oracle-parity e2e: WITHOUT ROWID DML & indexing vs rusqlite.
//!
//! rowid_oracle covers WITHOUT ROWID storage/ordering and the no-rowid-column
//! rule; this exercises mutation on that distinct storage model (the PK *is* the
//! key, there is no rowid B-tree): UPDATE/DELETE of non-key columns, UPDATE of
//! the PRIMARY KEY itself (re-keying + re-ordering), a secondary index lookup,
//! duplicate-PK conflict (error and INSERT OR REPLACE), and an INTEGER-PK
//! WITHOUT ROWID table's PK ordering. Each scenario asserts per-statement
//! agreement with rusqlite, then compares the resulting rows.

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
fn without_rowid_update_and_delete() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO wr VALUES ('banana',1),('apple',2),('cherry',3),('date',4)",
                "UPDATE wr SET v = v * 10 WHERE k = 'apple'", // apple -> 20
                "DELETE FROM wr WHERE k = 'cherry'",
            ],
            &["SELECT k, v FROM wr ORDER BY k"], // (apple,20),(banana,1),(date,4)
            "without_rowid_update_and_delete",
        )
        .await;
    });
}

#[test]
fn without_rowid_update_primary_key() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO wr VALUES ('apple',1),('banana',2)",
                "UPDATE wr SET k = 'zebra' WHERE k = 'apple'", // re-key + re-order
            ],
            &["SELECT k, v FROM wr ORDER BY k"], // (banana,2),(zebra,1)
            "without_rowid_update_primary_key",
        )
        .await;
    });
}

#[test]
fn without_rowid_secondary_index() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX idx_v ON wr(v)",
                "INSERT INTO wr VALUES ('a',30),('b',10),('c',20),('d',10)",
            ],
            &[
                "SELECT k FROM wr WHERE v = 10 ORDER BY k",    // b,d
                "SELECT k FROM wr WHERE v > 15 ORDER BY v, k", // c(20),a(30)
                "SELECT k, v FROM wr ORDER BY v, k",
            ],
            "without_rowid_secondary_index",
        )
        .await;
    });
}

#[test]
fn without_rowid_pk_conflict_and_replace() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO wr VALUES ('apple',1),('banana',2)",
                "INSERT INTO wr VALUES ('apple',99)", // duplicate PK -> error both
                "INSERT OR REPLACE INTO wr VALUES ('apple',99)", // replaces apple
            ],
            &["SELECT k, v FROM wr ORDER BY k"], // (apple,99),(banana,2)
            "without_rowid_pk_conflict_and_replace",
        )
        .await;
    });
}

#[test]
fn without_rowid_integer_pk_ordering() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID",
                "INSERT INTO wr VALUES (3,'c'),(1,'a'),(2,'b'),(10,'j')",
                "UPDATE wr SET v = 'B' WHERE id = 2",
                "DELETE FROM wr WHERE id = 10",
            ],
            &["SELECT id, v FROM wr ORDER BY id"], // (1,a),(2,B),(3,c)
            "without_rowid_integer_pk_ordering",
        )
        .await;
    });
}

/// bd-yuj70 — `UPDATE OR REPLACE` that re-keys a WITHOUT ROWID row onto a
/// primary key already held by a *different* row.
///
/// The defect: the rewrite deleted the OLD secondary-index entries and the OLD
/// clustered row before probing the NEW primary key, so the collision reached
/// `IdxInsert` with `OE_REPLACE` and skipped the victim's secondary-index
/// cleanup. Row-level `SELECT`s still agreed with SQLite, so this scenario is
/// deliberately index-discriminating: every query below is answered from a
/// secondary index (`ORDER BY`/equality on an indexed non-key column), which is
/// where a stale victim entry surfaces as a phantom or duplicate row.
#[test]
fn without_rowid_update_or_replace_pk_victim_and_secondary_indexes() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, tag TEXT NOT NULL, n INTEGER NOT NULL) \
                 WITHOUT ROWID",
                "CREATE INDEX wr_tag ON wr(tag)",
                "CREATE UNIQUE INDEX wr_n ON wr(n)",
                "INSERT INTO wr VALUES ('a','alpha',1),('b','beta',2),('c','gamma',3)",
                // Re-key 'a' onto 'b': 'b' is the OTHER victim and must be
                // removed along with BOTH of its secondary entries.
                "UPDATE OR REPLACE wr SET k = 'b', tag = 'moved', n = 20 WHERE k = 'a'",
            ],
            &[
                "SELECT k, tag, n FROM wr ORDER BY k",
                // Secondary-index driven: a stale victim entry ('beta'/2)
                // survives here even when the table scan looks correct.
                "SELECT tag, k FROM wr ORDER BY tag",
                "SELECT n, k FROM wr ORDER BY n",
                "SELECT k FROM wr WHERE tag = 'beta'",
                "SELECT k FROM wr WHERE n = 2",
                "SELECT count(*) FROM wr",
                "SELECT count(*) FROM wr WHERE tag IS NOT NULL",
            ],
            "without_rowid_update_or_replace_pk_victim",
        )
        .await;
    });
}

/// bd-yuj70 — the self-rewrite and no-conflict arms must not regress while the
/// victim path is fixed, and `OR IGNORE` / `OR ABORT` must leave OLD intact.
#[test]
fn without_rowid_update_or_replace_self_and_abort_arms() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, tag TEXT NOT NULL, n INTEGER NOT NULL) \
                 WITHOUT ROWID",
                "CREATE INDEX wr_tag ON wr(tag)",
                "CREATE UNIQUE INDEX wr_n ON wr(n)",
                "INSERT INTO wr VALUES ('a','alpha',1),('b','beta',2),('c','gamma',3)",
                // Self rewrite: PK unchanged, secondary keys move.
                "UPDATE OR REPLACE wr SET tag = 'ALPHA', n = 11 WHERE k = 'a'",
                // Re-key onto a free slot: no victim at all.
                "UPDATE OR REPLACE wr SET k = 'z' WHERE k = 'c'",
                // OR IGNORE onto an occupied PK: must be a no-op, OLD intact.
                "UPDATE OR IGNORE wr SET k = 'b' WHERE k = 'z'",
            ],
            &[
                "SELECT k, tag, n FROM wr ORDER BY k",
                "SELECT tag, k FROM wr ORDER BY tag",
                "SELECT n, k FROM wr ORDER BY n",
                "SELECT k FROM wr WHERE tag = 'alpha'",
                "SELECT k FROM wr WHERE n = 1",
                "SELECT count(*) FROM wr",
            ],
            "without_rowid_update_or_replace_self_and_abort",
        )
        .await;
    });
}

/// bd-yuj70 — stock-SQLite structural verification of the same rewrite.
///
/// The row-parity scenarios above cannot see a secondary-index entry that
/// points at a deleted clustered record when the query planner happens to
/// answer from the table. FrankenSQLite writes the file, closes it, and stock
/// SQLite is the sole oracle for `integrity_check` plus an index-vs-table
/// cross-check.
#[test]
fn without_rowid_update_or_replace_stays_stock_integrity_clean() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let db_path = dir.path().join("wr_replace_victim.db");
        let path_str = db_path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(path_str).await.expect("open frank");
            for stmt in [
                "CREATE TABLE wr (k TEXT PRIMARY KEY, tag TEXT NOT NULL, n INTEGER NOT NULL) \
                 WITHOUT ROWID",
                "CREATE INDEX wr_tag ON wr(tag)",
                "CREATE UNIQUE INDEX wr_n ON wr(n)",
                "INSERT INTO wr VALUES ('a','alpha',1),('b','beta',2),('c','gamma',3),\
                 ('d','delta',4)",
                "UPDATE OR REPLACE wr SET k = 'b', tag = 'moved', n = 20 WHERE k = 'a'",
                "UPDATE OR REPLACE wr SET k = 'd', tag = 'moved2', n = 30 WHERE k = 'c'",
            ] {
                conn.execute(stmt).await.expect("frank statement");
            }
        }

        let stock = rusqlite::Connection::open(&db_path).expect("stock reopen");
        let integrity: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("integrity_check");
        assert_eq!(
            integrity, "ok",
            "bd-yuj70: UPDATE OR REPLACE victim handling left a malformed image"
        );

        // Every secondary-index entry must resolve to a live clustered row.
        // A stale victim entry makes the index-driven count exceed the table
        // count, which `integrity_check` alone does not always surface.
        let table_rows: i64 = stock
            .query_row("SELECT count(*) FROM wr", [], |row| row.get(0))
            .expect("table count");
        let tag_rows: i64 = stock
            .query_row(
                "SELECT count(*) FROM (SELECT k FROM wr WHERE tag IS NOT NULL ORDER BY tag)",
                [],
                |row| row.get(0),
            )
            .expect("tag index count");
        let n_rows: i64 = stock
            .query_row(
                "SELECT count(*) FROM (SELECT k FROM wr WHERE n IS NOT NULL ORDER BY n)",
                [],
                |row| row.get(0),
            )
            .expect("n index count");
        assert_eq!(table_rows, 2, "bd-yuj70: both victims must be replaced");
        assert_eq!(
            tag_rows, table_rows,
            "bd-yuj70: wr_tag retained an entry for a replaced victim"
        );
        assert_eq!(
            n_rows, table_rows,
            "bd-yuj70: wr_n retained an entry for a replaced victim"
        );

        let survivors: Vec<String> = stock
            .prepare("SELECT k || '/' || tag || '/' || n FROM wr ORDER BY k")
            .expect("prepare survivors")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query survivors")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect survivors");
        assert_eq!(
            survivors,
            vec!["b/moved/20".to_owned(), "d/moved2/30".to_owned()],
            "bd-yuj70: unexpected surviving rows after victim replacement"
        );
    });
}
