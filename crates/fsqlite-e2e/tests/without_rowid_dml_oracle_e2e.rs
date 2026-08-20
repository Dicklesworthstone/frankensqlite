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
                // The ABORT family must actually raise, with OLD still intact.
                // `scenario` asserts frank and rusqlite agree on Ok/Err per
                // statement, so these are real assertions on the raise itself
                // rather than on a swallowed error.
                "UPDATE OR ABORT wr SET k = 'b' WHERE k = 'z'",
                "UPDATE OR ROLLBACK wr SET k = 'b' WHERE k = 'z'",
                "UPDATE OR FAIL wr SET k = 'b' WHERE k = 'z'",
                // Bare UPDATE (statement-level default is ABORT) onto an
                // occupied PK must raise identically.
                "UPDATE wr SET k = 'b' WHERE k = 'z'",
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
            // Await the close explicitly: relying on Drop cancels region tasks
            // without rolling back open transactions or running a checkpoint,
            // so the bytes stock SQLite then reads would not be a settled image.
            conn.close().await.expect("await frank close");
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

/// bd-yuj70 — `UPDATE OR REPLACE` whose NEW row conflicts on a **secondary
/// UNIQUE index** rather than on the primary key.
///
/// `emit_without_rowid_update_rewrite` preflights only the PK victim; the
/// secondary-unique conflict is left to the engine's `IdxInsert` with the
/// statement conflict action. SQLite semantics require REPLACE to delete the
/// conflicting *row* (and all of its index entries), not merely to overwrite
/// the index slot. This scenario is the probe for that: the NEW primary key
/// `'z'` is free, so the PK preflight does nothing and only the secondary-index
/// path is exercised.
#[test]
fn without_rowid_update_or_replace_secondary_unique_victim() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, tag TEXT NOT NULL, n INTEGER NOT NULL) \
                 WITHOUT ROWID",
                "CREATE INDEX wr_tag ON wr(tag)",
                "CREATE UNIQUE INDEX wr_n ON wr(n)",
                "INSERT INTO wr VALUES ('a','alpha',1),('b','beta',2),('c','gamma',3)",
                // PK 'z' is free; n = 2 collides with row 'b' on wr_n.
                "UPDATE OR REPLACE wr SET k = 'z', n = 2 WHERE k = 'a'",
            ],
            &[
                "SELECT k, tag, n FROM wr ORDER BY k",
                "SELECT n, k FROM wr ORDER BY n",
                "SELECT tag, k FROM wr ORDER BY tag",
                "SELECT k FROM wr WHERE n = 2",
                "SELECT count(*) FROM wr",
            ],
            "without_rowid_update_or_replace_secondary_unique_victim",
        )
        .await;
    });
}

/// bd-yuj70 — the same victim-safe rewrite driven through `UPDATE ... FROM`.
///
/// `codegen_update_from_without_rowid` shares
/// `emit_without_rowid_update_rewrite` but computes the NEW image during its
/// pass-1 join, so the OLD/NEW register plumbing is distinct and needs its own
/// oracle. Row-count parity alone would not discriminate — a stale victim
/// entry keeps the clustered row count correct — so the queries below read
/// through the secondary keys.
#[test]
fn without_rowid_update_from_or_replace_pk_victim_and_secondary_indexes() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, tag TEXT NOT NULL, n INTEGER NOT NULL) \
                 WITHOUT ROWID",
                "CREATE INDEX wr_tag ON wr(tag)",
                "CREATE UNIQUE INDEX wr_n ON wr(n)",
                "INSERT INTO wr VALUES ('a','alpha',1),('b','beta',2),('c','gamma',3),\
                 ('d','delta',4)",
                "CREATE TABLE moves (from_k TEXT PRIMARY KEY, to_k TEXT NOT NULL, \
                 new_tag TEXT NOT NULL, new_n INTEGER NOT NULL)",
                // 'a' -> 'b' re-keys onto a live victim; 'c' -> 'y' is a free
                // slot, so both the victim and no-victim arms run in one join.
                "INSERT INTO moves VALUES ('a','b','moved',20),('c','y','shifted',30)",
                "UPDATE OR REPLACE wr SET k = moves.to_k, tag = moves.new_tag, n = moves.new_n \
                 FROM moves WHERE wr.k = moves.from_k",
            ],
            &[
                "SELECT k, tag, n FROM wr ORDER BY k",
                "SELECT tag, k FROM wr ORDER BY tag",
                "SELECT n, k FROM wr ORDER BY n",
                "SELECT k FROM wr WHERE tag = 'beta'",
                "SELECT k FROM wr WHERE n = 2",
                "SELECT count(*) FROM wr",
            ],
            "without_rowid_update_from_or_replace_pk_victim",
        )
        .await;
    });
}

/// bd-yuj70 — `UPDATE ... FROM` self-rewrite and `OR IGNORE`/ABORT arms.
#[test]
fn without_rowid_update_from_self_and_conflict_arms() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, tag TEXT NOT NULL, n INTEGER NOT NULL) \
                 WITHOUT ROWID",
                "CREATE INDEX wr_tag ON wr(tag)",
                "CREATE UNIQUE INDEX wr_n ON wr(n)",
                "INSERT INTO wr VALUES ('a','alpha',1),('b','beta',2),('c','gamma',3)",
                "CREATE TABLE moves (from_k TEXT PRIMARY KEY, to_k TEXT NOT NULL, \
                 new_tag TEXT NOT NULL, new_n INTEGER NOT NULL)",
                "INSERT INTO moves VALUES ('a','a','ALPHA',11)",
                // PK unchanged through the join: the self-rewrite arm.
                "UPDATE OR REPLACE wr SET k = moves.to_k, tag = moves.new_tag, n = moves.new_n \
                 FROM moves WHERE wr.k = moves.from_k",
                "DELETE FROM moves",
                "INSERT INTO moves VALUES ('c','b','clash',77)",
                // OR IGNORE onto an occupied PK through the join: no-op.
                "UPDATE OR IGNORE wr SET k = moves.to_k, tag = moves.new_tag \
                 FROM moves WHERE wr.k = moves.from_k",
                // The ABORT family must raise with OLD intact.
                "UPDATE OR ABORT wr SET k = moves.to_k FROM moves WHERE wr.k = moves.from_k",
                "UPDATE wr SET k = moves.to_k FROM moves WHERE wr.k = moves.from_k",
            ],
            &[
                "SELECT k, tag, n FROM wr ORDER BY k",
                "SELECT tag, k FROM wr ORDER BY tag",
                "SELECT n, k FROM wr ORDER BY n",
                "SELECT k FROM wr WHERE n = 1",
                "SELECT count(*) FROM wr",
            ],
            "without_rowid_update_from_self_and_conflict",
        )
        .await;
    });
}

/// bd-yuj70 — stock-SQLite structural verification of the `UPDATE ... FROM`
/// rewrite.
///
/// This is the arm that *forces* index discrimination. The in-memory scenarios
/// above read through secondary keys, but whether FrankenSQLite's planner
/// actually serves them from the index is not a contract this suite can pin —
/// `INDEXED BY` is documented as not a hard contract for stock WITHOUT ROWID
/// indexes (frankensqlite#137), so it is deliberately not used here. Stock
/// SQLite's `integrity_check` on a WITHOUT ROWID table validates index-vs-table
/// consistency independently of any query plan, which is the property that
/// makes an orphaned victim entry unmissable.
#[test]
fn without_rowid_update_from_or_replace_stays_stock_integrity_clean() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let db_path = dir.path().join("wr_from_replace_victim.db");
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
                "CREATE TABLE moves (from_k TEXT PRIMARY KEY, to_k TEXT NOT NULL, \
                 new_tag TEXT NOT NULL, new_n INTEGER NOT NULL)",
                "INSERT INTO moves VALUES ('a','b','moved',20),('c','d','moved2',30)",
                "UPDATE OR REPLACE wr SET k = moves.to_k, tag = moves.new_tag, n = moves.new_n \
                 FROM moves WHERE wr.k = moves.from_k",
            ] {
                conn.execute(stmt).await.expect("frank statement");
            }
            conn.close().await.expect("await frank close");
        }

        let stock = rusqlite::Connection::open(&db_path).expect("stock reopen");
        let integrity: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("integrity_check");
        assert_eq!(
            integrity, "ok",
            "bd-yuj70: UPDATE ... FROM victim handling left a malformed image"
        );

        let table_rows: i64 = stock
            .query_row("SELECT count(*) FROM wr", [], |row| row.get(0))
            .expect("table count");
        assert_eq!(
            table_rows, 2,
            "bd-yuj70: both UPDATE ... FROM victims must be replaced"
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
            "bd-yuj70: unexpected surviving rows after UPDATE ... FROM replacement"
        );
    });
}

/// bd-yuj70 — the non-REPLACE conflict actions on a secondary UNIQUE index.
///
/// The preflight must skip the row for `OR IGNORE` and raise for the ABORT
/// family, in both cases leaving OLD exactly as it was. `scenario` asserts
/// frank and rusqlite agree on Ok/Err per statement, so the raises are real
/// assertions rather than swallowed errors.
#[test]
fn without_rowid_update_secondary_unique_ignore_and_abort_arms() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, tag TEXT NOT NULL, n INTEGER NOT NULL) \
                 WITHOUT ROWID",
                "CREATE UNIQUE INDEX wr_n ON wr(n)",
                "INSERT INTO wr VALUES ('a','alpha',1),('b','beta',2),('c','gamma',3)",
                // OR IGNORE: n = 2 is held by 'b', so the row is skipped whole.
                "UPDATE OR IGNORE wr SET tag = 'skipped', n = 2 WHERE k = 'a'",
                // The ABORT family must raise, leaving 'a' untouched.
                "UPDATE OR ABORT wr SET n = 2 WHERE k = 'a'",
                "UPDATE OR FAIL wr SET n = 2 WHERE k = 'a'",
                "UPDATE OR ROLLBACK wr SET n = 2 WHERE k = 'a'",
                // Bare UPDATE defaults to ABORT.
                "UPDATE wr SET n = 2 WHERE k = 'a'",
                // A self-conflict on the unique key must NOT be treated as a
                // victim: rewriting 'a' to its own current n is a no-op probe.
                "UPDATE OR ABORT wr SET tag = 'self', n = 1 WHERE k = 'a'",
            ],
            &[
                "SELECT k, tag, n FROM wr ORDER BY k",
                "SELECT n, k FROM wr ORDER BY n",
                "SELECT k FROM wr WHERE n = 2",
                "SELECT count(*) FROM wr",
            ],
            "without_rowid_update_secondary_unique_ignore_and_abort",
        )
        .await;
    });
}

/// bd-yuj70 — two distinct UNIQUE indexes, each contributing a *different*
/// victim in one statement, plus a partial UNIQUE index whose predicate
/// excludes the NEW row.
///
/// This is the multi-victim shape: the preflight must clear both victims (and
/// every one of their secondary entries) before OLD is deleted, and must not
/// treat the partial index as constraining a row its predicate does not admit.
#[test]
fn without_rowid_update_replace_multiple_unique_victims() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, \
                 lab TEXT NOT NULL) WITHOUT ROWID",
                "CREATE UNIQUE INDEX wr_a ON wr(a)",
                "CREATE UNIQUE INDEX wr_b ON wr(b)",
                "CREATE INDEX wr_lab ON wr(lab)",
                // Partial UNIQUE index that only admits lab = 'p'.
                "CREATE UNIQUE INDEX wr_part ON wr(a) WHERE lab = 'p'",
                "INSERT INTO wr VALUES ('r1',1,10,'x'),('r2',2,20,'y'),('r3',3,30,'z'),\
                 ('r4',4,40,'w')",
                // r1 takes r2's `a` and r3's `b`: two victims, two indexes, one
                // statement. NEW lab = 'q' keeps it outside wr_part.
                "UPDATE OR REPLACE wr SET a = 2, b = 30, lab = 'q' WHERE k = 'r1'",
            ],
            &[
                "SELECT k, a, b, lab FROM wr ORDER BY k",
                "SELECT a, k FROM wr ORDER BY a",
                "SELECT b, k FROM wr ORDER BY b",
                "SELECT lab, k FROM wr ORDER BY lab, k",
                "SELECT k FROM wr WHERE a = 2",
                "SELECT k FROM wr WHERE b = 30",
                "SELECT count(*) FROM wr",
            ],
            "without_rowid_update_replace_multiple_unique_victims",
        )
        .await;
    });
}

/// bd-yuj70 — NULL keys never collide on a UNIQUE index, and a NOCASE
/// collation must decide the conflict, not raw bytes.
#[test]
fn without_rowid_update_secondary_unique_null_and_collation() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, u TEXT, c TEXT COLLATE NOCASE) \
                 WITHOUT ROWID",
                "CREATE UNIQUE INDEX wr_u ON wr(u)",
                "CREATE UNIQUE INDEX wr_c ON wr(c)",
                "INSERT INTO wr VALUES ('a',NULL,'Alpha'),('b',NULL,'Beta'),('c','cc','Gamma')",
                // Multiple NULLs coexist on a UNIQUE index: this must not be
                // read as a conflict and must not delete a victim.
                "UPDATE OR REPLACE wr SET u = NULL WHERE k = 'c'",
                // NOCASE: 'BETA' collides with 'Beta' on wr_c, so REPLACE must
                // remove row 'b' even though the bytes differ.
                "UPDATE OR REPLACE wr SET c = 'BETA' WHERE k = 'a'",
            ],
            &[
                "SELECT k, u, c FROM wr ORDER BY k",
                "SELECT c, k FROM wr ORDER BY c, k",
                "SELECT k FROM wr WHERE c = 'beta'",
                "SELECT count(*) FROM wr",
                "SELECT count(*) FROM wr WHERE u IS NULL",
            ],
            "without_rowid_update_secondary_unique_null_and_collation",
        )
        .await;
    });
}

/// bd-yuj70 — stock-SQLite structural proof for the secondary-UNIQUE victim
/// path, mirroring the PK-victim integrity arm.
#[test]
fn without_rowid_update_secondary_unique_stays_stock_integrity_clean() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let db_path = dir.path().join("wr_secondary_unique_victim.db");
        let path_str = db_path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(path_str).await.expect("open frank");
            for stmt in [
                "CREATE TABLE wr (k TEXT PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, \
                 lab TEXT NOT NULL) WITHOUT ROWID",
                "CREATE UNIQUE INDEX wr_a ON wr(a)",
                "CREATE UNIQUE INDEX wr_b ON wr(b)",
                "CREATE INDEX wr_lab ON wr(lab)",
                "INSERT INTO wr VALUES ('r1',1,10,'x'),('r2',2,20,'y'),('r3',3,30,'z'),\
                 ('r4',4,40,'w')",
                "UPDATE OR REPLACE wr SET a = 2, b = 30, lab = 'q' WHERE k = 'r1'",
            ] {
                conn.execute(stmt).await.expect("frank statement");
            }
            conn.close().await.expect("await frank close");
        }

        let stock = rusqlite::Connection::open(&db_path).expect("stock reopen");
        let integrity: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("integrity_check");
        assert_eq!(
            integrity, "ok",
            "bd-yuj70: secondary-UNIQUE victim handling left a malformed image"
        );

        let survivors: Vec<String> = stock
            .prepare("SELECT k || '/' || a || '/' || b || '/' || lab FROM wr ORDER BY k")
            .expect("prepare survivors")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query survivors")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect survivors");
        assert_eq!(
            survivors,
            vec!["r1/2/30/q".to_owned(), "r4/4/40/w".to_owned()],
            "bd-yuj70: both secondary-UNIQUE victims must be replaced"
        );
    });
}

/// bd-yuj70 — mixed per-index conflict actions must not produce a partial
/// mutation: a REPLACE decision on one index followed by IGNORE on another
/// leaves the database completely untouched.
///
/// A statement-level `OR <algo>` overrides every constraint action, so mixing
/// requires a bare `UPDATE` plus constraint-level `ON CONFLICT` clauses. Index
/// order is declaration order, so `UNIQUE(a)` is decided before `UNIQUE(b)`:
/// the `a` victim is captured first, then `b` says IGNORE. Under a
/// delete-as-you-go design the `a` victim would already be gone; here nothing
/// may change.
#[test]
fn without_rowid_update_mixed_actions_replace_then_ignore_is_a_no_op() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, \
                 UNIQUE(a) ON CONFLICT REPLACE, UNIQUE(b) ON CONFLICT IGNORE) WITHOUT ROWID",
                "INSERT INTO wr VALUES ('r1',1,10),('r2',2,20),('r3',3,30)",
                // Collides on `a` with r2 (REPLACE) and on `b` with r3
                // (IGNORE). The IGNORE wins the row, so r2 must survive.
                "UPDATE wr SET a = 2, b = 30 WHERE k = 'r1'",
            ],
            &[
                "SELECT k, a, b FROM wr ORDER BY k",
                "SELECT a, k FROM wr ORDER BY a",
                "SELECT b, k FROM wr ORDER BY b",
                "SELECT k FROM wr WHERE a = 2",
                "SELECT k FROM wr WHERE b = 30",
                "SELECT count(*) FROM wr",
            ],
            "without_rowid_update_mixed_replace_then_ignore",
        )
        .await;
    });
}

/// bd-yuj70 — the same shape with ABORT second: the raise must happen before
/// the captured REPLACE victim is deleted, so the statement is atomic.
#[test]
fn without_rowid_update_mixed_actions_replace_then_abort_preserves_victim() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, \
                 UNIQUE(a) ON CONFLICT REPLACE, UNIQUE(b) ON CONFLICT ABORT) WITHOUT ROWID",
                "INSERT INTO wr VALUES ('r1',1,10),('r2',2,20),('r3',3,30)",
                // `a` captures r2 as a REPLACE victim, then `b` aborts on r3.
                // Both r2 and r3 must be intact afterwards.
                "UPDATE wr SET a = 2, b = 30 WHERE k = 'r1'",
            ],
            &[
                "SELECT k, a, b FROM wr ORDER BY k",
                "SELECT a, k FROM wr ORDER BY a",
                "SELECT b, k FROM wr ORDER BY b",
                "SELECT k FROM wr WHERE a = 2",
                "SELECT count(*) FROM wr",
            ],
            "without_rowid_update_mixed_replace_then_abort",
        )
        .await;
    });
}

/// bd-yuj70 — one row reached as a victim through two different UNIQUE
/// indexes. Both decisions capture the *same* primary key, so phase B must
/// delete it once and skip the second seek rather than double-deleting.
#[test]
fn without_rowid_update_same_victim_through_two_unique_indexes() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, \
                 lab TEXT NOT NULL) WITHOUT ROWID",
                "CREATE UNIQUE INDEX wr_a ON wr(a)",
                "CREATE UNIQUE INDEX wr_b ON wr(b)",
                "CREATE INDEX wr_lab ON wr(lab)",
                "INSERT INTO wr VALUES ('r1',1,10,'x'),('r2',2,20,'y'),('r3',3,30,'z')",
                // r2 holds BOTH a = 2 and b = 20: one victim, two indexes.
                "UPDATE OR REPLACE wr SET a = 2, b = 20, lab = 'merged' WHERE k = 'r1'",
            ],
            &[
                "SELECT k, a, b, lab FROM wr ORDER BY k",
                "SELECT a, k FROM wr ORDER BY a",
                "SELECT b, k FROM wr ORDER BY b",
                "SELECT lab, k FROM wr ORDER BY lab, k",
                "SELECT k FROM wr WHERE a = 2",
                "SELECT k FROM wr WHERE b = 20",
                "SELECT count(*) FROM wr",
            ],
            "without_rowid_update_same_victim_two_indexes",
        )
        .await;
    });
}

/// bd-yuj70 — the primary-key victim and a secondary victim are the same row,
/// so the PK capture and the secondary capture name one primary key. Phase B
/// must still delete exactly once.
#[test]
fn without_rowid_update_pk_and_secondary_victim_are_same_row() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, a INTEGER NOT NULL, lab TEXT NOT NULL) \
                 WITHOUT ROWID",
                "CREATE UNIQUE INDEX wr_a ON wr(a)",
                "CREATE INDEX wr_lab ON wr(lab)",
                "INSERT INTO wr VALUES ('r1',1,'x'),('r2',2,'y'),('r3',3,'z')",
                // Re-key r1 onto r2 AND take r2's unique `a`: r2 is both the
                // primary-key victim and the wr_a victim.
                "UPDATE OR REPLACE wr SET k = 'r2', a = 2, lab = 'merged' WHERE k = 'r1'",
            ],
            &[
                "SELECT k, a, lab FROM wr ORDER BY k",
                "SELECT a, k FROM wr ORDER BY a",
                "SELECT lab, k FROM wr ORDER BY lab, k",
                "SELECT k FROM wr WHERE a = 2",
                "SELECT count(*) FROM wr",
            ],
            "without_rowid_update_pk_and_secondary_same_victim",
        )
        .await;
    });
}

/// bd-yuj70 — composite (multi-column) PRIMARY KEY: the victim's key is more
/// than one column, so PK capture, victim re-seek, and self-conflict detection
/// must all operate on the full composite key record, not a single term.
#[test]
fn without_rowid_update_or_replace_composite_pk_victim() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (a TEXT NOT NULL, b INTEGER NOT NULL, v TEXT NOT NULL, \
                 PRIMARY KEY (a, b)) WITHOUT ROWID",
                "CREATE INDEX wr_v ON wr(v)",
                "INSERT INTO wr VALUES ('x',1,'one'),('x',2,'two'),('y',1,'three')",
                // Re-key ('x',1) onto ('x',2): composite-key victim with a
                // secondary entry that must disappear with it.
                "UPDATE OR REPLACE wr SET b = 2, v = 'moved' WHERE a = 'x' AND b = 1",
                // Self-rewrite of a composite key must not treat the row as
                // its own victim.
                "UPDATE OR REPLACE wr SET a = 'y', b = 1, v = 'kept' WHERE a = 'y' AND b = 1",
            ],
            &[
                "SELECT a, b, v FROM wr ORDER BY a, b",
                "SELECT v, a, b FROM wr ORDER BY v",
                "SELECT a, b FROM wr WHERE v = 'two'",
                "SELECT count(*) FROM wr",
            ],
            "without_rowid_update_or_replace_composite_pk_victim",
        )
        .await;
    });
}

/// bd-yuj70 — `UPDATE OR REPLACE ... RETURNING` on a WITHOUT ROWID table must
/// return the updated row (never the replaced victim), matching stock SQLite.
#[test]
fn without_rowid_update_or_replace_returning() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        for s in [
            "CREATE TABLE wr (k TEXT PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID",
            "CREATE UNIQUE INDEX wr_n ON wr(n)",
            "INSERT INTO wr VALUES ('a',1),('b',2)",
        ] {
            f.execute(s).await.expect("frank setup");
            r.execute_batch(s).expect("csql setup");
        }
        // Re-keys 'a' onto 'b' (PK victim) while also taking b's unique n.
        let dml = "UPDATE OR REPLACE wr SET k = 'b', n = 2 WHERE k = 'a' RETURNING k, n";
        let frank = frank_rows(&f, dml).await.expect("frank returning");
        let csql = sqlite_rows(&r, dml).expect("csql returning");
        assert_eq!(
            frank, csql,
            "without_rowid_update_or_replace_returning: RETURNING mismatch"
        );
        let verify = "SELECT k, n FROM wr ORDER BY k";
        assert_eq!(
            frank_rows(&f, verify).await.expect("frank verify"),
            sqlite_rows(&r, verify).expect("csql verify"),
            "without_rowid_update_or_replace_returning: post-state mismatch"
        );
    });
}

/// bd-yuj70 — trigger semantics around the REPLACE victim. With default
/// `recursive_triggers = OFF`, stock SQLite fires the UPDATE triggers for the
/// updated row but does NOT fire DELETE triggers for the replaced victim; the
/// firing log must match exactly.
#[test]
fn without_rowid_update_or_replace_victim_triggers_default() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID",
                "CREATE TABLE log (ev TEXT, key TEXT)",
                "CREATE TRIGGER wr_del AFTER DELETE ON wr BEGIN \
                 INSERT INTO log VALUES ('delete', OLD.k); END",
                "CREATE TRIGGER wr_upd AFTER UPDATE ON wr BEGIN \
                 INSERT INTO log VALUES ('update', NEW.k); END",
                "INSERT INTO wr VALUES ('a',1),('b',2)",
                "UPDATE OR REPLACE wr SET k = 'b' WHERE k = 'a'",
            ],
            &[
                "SELECT k, n FROM wr ORDER BY k",
                "SELECT ev, key FROM log ORDER BY ev, key",
            ],
            "without_rowid_update_or_replace_victim_triggers_default",
        )
        .await;
    });
}

/// bd-yuj70 — with `PRAGMA recursive_triggers = ON`, stock SQLite DOES fire
/// DELETE triggers for the replaced victim.
#[test]
fn without_rowid_update_or_replace_victim_triggers_recursive() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "PRAGMA recursive_triggers = ON",
                "CREATE TABLE wr (k TEXT PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID",
                "CREATE TABLE log (ev TEXT, key TEXT)",
                "CREATE TRIGGER wr_del AFTER DELETE ON wr BEGIN \
                 INSERT INTO log VALUES ('delete', OLD.k); END",
                "INSERT INTO wr VALUES ('a',1),('b',2)",
                "UPDATE OR REPLACE wr SET k = 'b' WHERE k = 'a'",
            ],
            &[
                "SELECT k, n FROM wr ORDER BY k",
                "SELECT ev, key FROM log ORDER BY ev, key",
            ],
            "without_rowid_update_or_replace_victim_triggers_recursive",
        )
        .await;
    });
}

/// bd-yuj70 — foreign keys: deleting the REPLACE victim must respect inbound
/// FK constraints. A victim parent with dependent children makes the statement
/// fail in stock SQLite (immediate NO ACTION); a childless victim succeeds.
#[test]
fn without_rowid_update_or_replace_victim_fk_enforcement() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE parent (k TEXT PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID",
                "CREATE TABLE child (c TEXT PRIMARY KEY, pk TEXT NOT NULL REFERENCES parent(k)) \
                 WITHOUT ROWID",
                "INSERT INTO parent VALUES ('a',1),('b',2),('c',3)",
                "INSERT INTO child VALUES ('c1','b')",
                // Victim 'b' has a child: both engines must reject.
                "UPDATE OR REPLACE parent SET k = 'b' WHERE k = 'a'",
                // Victim 'c' is childless: both engines must replace it.
                "UPDATE OR REPLACE parent SET k = 'c' WHERE k = 'a'",
            ],
            &[
                "SELECT k, n FROM parent ORDER BY k",
                "SELECT c, pk FROM child ORDER BY c",
                "PRAGMA foreign_key_check",
            ],
            "without_rowid_update_or_replace_victim_fk_enforcement",
        )
        .await;
    });
}

// bd-yqjjx: ON CONFLICT (<secondary-unique-col>) DO UPDATE on a WITHOUT ROWID
// table. Previously refused as Unsupported (only the PRIMARY KEY target was
// emittable); the explicit secondary-UNIQUE arbiter now probes just that index.

/// The named secondary-UNIQUE arbiter fires DO UPDATE when the attempted row
/// collides on that index (and not the PK).
#[test]
fn without_rowid_explicit_secondary_upsert_target_bd_yqjjx() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (a TEXT PRIMARY KEY, b INTEGER UNIQUE) WITHOUT ROWID",
                "INSERT INTO wr VALUES ('x',1),('y',2)",
                // b=1 already exists on row 'x'; target is b -> DO UPDATE that row.
                "INSERT INTO wr VALUES ('z',1) ON CONFLICT(b) DO UPDATE SET a='updated'",
                // b=9 is new: no b-conflict -> ordinary insert.
                "INSERT INTO wr VALUES ('w',9) ON CONFLICT(b) DO UPDATE SET a='never'",
            ],
            &["SELECT a, b FROM wr ORDER BY b"],
            "without_rowid_explicit_secondary_upsert_target_bd_yqjjx",
        )
        .await;
    });
}

/// A PRIMARY KEY collision under a secondary-UNIQUE target is NOT the named
/// arbiter: it falls through to the ordinary insert and aborts (both engines).
#[test]
fn without_rowid_explicit_secondary_target_pk_conflict_aborts_bd_yqjjx() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (a TEXT PRIMARY KEY, b INTEGER UNIQUE) WITHOUT ROWID",
                "INSERT INTO wr VALUES ('x',1)",
                // Collides on the PK a='x', not on b -> ABORT, no DO UPDATE.
                "INSERT INTO wr VALUES ('x',5) ON CONFLICT(b) DO UPDATE SET a='no'",
            ],
            &["SELECT a, b FROM wr ORDER BY b"],
            "without_rowid_explicit_secondary_target_pk_conflict_aborts_bd_yqjjx",
        )
        .await;
    });
}

/// `excluded.*` and a `WHERE` guard resolve inside an explicit-secondary-target
/// DO UPDATE: the WHERE-false collision leaves the row untouched.
#[test]
fn without_rowid_explicit_secondary_upsert_excluded_and_where_bd_yqjjx() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (a TEXT PRIMARY KEY, b INTEGER UNIQUE, n INTEGER) WITHOUT ROWID",
                "INSERT INTO wr VALUES ('x',1,10),('y',2,20)",
                // b=1 conflict; WHERE true -> set n from excluded.
                "INSERT INTO wr VALUES ('z',1,77) ON CONFLICT(b) DO UPDATE SET n=excluded.n \
                 WHERE excluded.n > n",
                // b=2 conflict; WHERE false (excluded.n < existing) -> untouched.
                "INSERT INTO wr VALUES ('q',2,5) ON CONFLICT(b) DO UPDATE SET n=excluded.n \
                 WHERE excluded.n > n",
            ],
            &["SELECT a, b, n FROM wr ORDER BY b"],
            "without_rowid_explicit_secondary_upsert_excluded_and_where_bd_yqjjx",
        )
        .await;
    });
}

/// A composite secondary-UNIQUE arbiter (UNIQUE(b,c)) on a WITHOUT ROWID table.
#[test]
fn without_rowid_explicit_multicol_secondary_upsert_target_bd_yqjjx() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (a TEXT PRIMARY KEY, b INTEGER, c INTEGER, UNIQUE(b,c)) \
                 WITHOUT ROWID",
                "INSERT INTO wr VALUES ('x',1,1),('y',1,2)",
                // (b,c)=(1,1) conflict on row 'x' -> DO UPDATE that row.
                "INSERT INTO wr VALUES ('z',1,1) ON CONFLICT(b,c) DO UPDATE SET a='updated'",
                // (b,c)=(1,3) new -> ordinary insert.
                "INSERT INTO wr VALUES ('w',1,3) ON CONFLICT(b,c) DO UPDATE SET a='never'",
            ],
            &["SELECT a, b, c FROM wr ORDER BY b, c"],
            "without_rowid_explicit_multicol_secondary_upsert_target_bd_yqjjx",
        )
        .await;
    });
}
