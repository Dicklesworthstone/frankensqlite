//! Keeper for bd-p1h2r (parity): rowid-key collisions and `UPDATE ... SET
//! rowid = <expr>` on rowid tables, checked against stock SQLite.
//!
//! Two defects were fixed together:
//!
//! 1. A rowid collision on a table WITHOUT a named INTEGER PRIMARY KEY alias
//!    (`CREATE TABLE t(a, b)`, then an explicit `INSERT INTO t(rowid, ...)`)
//!    was reported as the bare `PRIMARY KEY constraint failed` because
//!    `ipk_label_by_root_page` (connection.rs, `table_execution_metadata`) was
//!    populated only for `column.is_ipk`. Stock says
//!    `UNIQUE constraint failed: <table>.rowid`; the label map now registers
//!    `<table>.rowid` for every rowid table lacking an alias. Companion to
//!    bd-977wx (the alias case) and bd-a506j (WITHOUT ROWID PK labels, guarded
//!    here so they stay correct).
//!
//! 2. `UPDATE t SET rowid = <expr>` on such a table failed at codegen with
//!    `no such column: rowid`: `collect_update_assignment_columns` /
//!    `emit_update_assignments` (fsqlite-vdbe codegen) resolved SET targets
//!    through `TableSchema::column_index` only. They now resolve
//!    `rowid` / `_rowid_` / `oid` the way stock does — a declared column of that
//!    name shadows the alias; on an INTEGER PRIMARY KEY table the alias is
//!    that column; otherwise it is the hidden rowid, evaluated into its own
//!    register, gated by `MustBeInt` ("datatype mismatch"), and used as the
//!    re-insertion key with every index re-keyed.
//!
//! Boundary (stated, not covered): a table with UPDATE triggers takes the
//! trigger-snapshot lane in connection.rs, which cannot carry a rewritten
//! hidden rowid yet; it reports a clean "not implemented" instead of an
//! internal error. WITHOUT ROWID tables have no rowid, so `SET rowid =` is
//! "no such column: rowid" on both engines.
//!
//! Oracle: the bundled rusqlite (the message text is compared to stock at run
//! time AND pinned to the literal wording, which has been stable across the
//! 3.x series; sqlite3 3.51.0 on the authoring host agreed on every case).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
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

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

/// Run one statement on FrankenSQLite; `<OK>` or the error text.
async fn frank_exec(f: &Connection, sql: &str) -> String {
    match f.execute(sql).await {
        Ok(_) => "<OK>".to_owned(),
        Err(e) => e.to_string(),
    }
}

/// Run one statement on stock; `<OK>` or the engine's own message text.
fn stock_exec(r: &rusqlite::Connection, sql: &str) -> String {
    match r.execute_batch(sql) {
        Ok(()) => "<OK>".to_owned(),
        Err(rusqlite::Error::SqliteFailure(_, Some(msg))) => msg,
        Err(e) => format!("<ERR {e}>"),
    }
}

async fn frank_rows(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(rusqlite::Error::SqliteFailure(_, Some(msg))) => return vec![vec![format!("<ERR {msg}>")]],
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Differential DML case: both engines run `setup`, then `dml` (its outcome —
/// `<OK>` or the exact error text — must agree), then every `probe` query must
/// return identical rows, and both must pass `integrity_check`.
async fn agree_dml(setup: &[&str], dml: &str, probes: &[&str], msg: &str) -> String {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        f.execute(s).await.unwrap_or_else(|e| panic!("{msg}: frank setup `{s}` failed: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("{msg}: stock setup `{s}` failed: {e}"));
    }
    let fo = frank_exec(&f, dml).await;
    let so = stock_exec(&r, dml);
    assert_eq!(fo, so, "{msg}\n  dml   ={dml}\n  frank ={fo:?}\n  sqlite={so:?}");
    assert!(
        !fo.starts_with("internal error:"),
        "{msg}: must never surface as Internal (was: {fo:?})"
    );
    for probe in probes {
        let fr = frank_rows(&f, probe).await;
        let rr = stock_rows(&r, probe);
        assert_eq!(fr, rr, "{msg}\n  probe ={probe}\n  frank ={fr:?}\n  sqlite={rr:?}");
    }
    let fi = frank_rows(&f, "PRAGMA integrity_check").await;
    let ri = stock_rows(&r, "PRAGMA integrity_check");
    assert_eq!(fi, vec![vec!["'ok'".to_owned()]], "{msg}: frank integrity_check");
    assert_eq!(ri, vec![vec!["'ok'".to_owned()]], "{msg}: stock integrity_check");
    fo
}

/// A collision case: the DML must fail on both engines with the same text,
/// which must also be the pinned stock wording.
async fn agree_collision(setup: &[&str], dml: &str, expected: &str) {
    let got = agree_dml(setup, dml, &[], expected).await;
    assert_eq!(got, expected, "message must match the pinned stock wording");
    assert!(
        !got.contains("PRIMARY KEY constraint failed"),
        "rowid/PK collision must read as a UNIQUE violation (was: {got:?})"
    );
}

const ROWS: &str = "SELECT rowid, * FROM t ORDER BY rowid";

#[test]
fn rowid_collision_reports_unique_constraint_p1h2r() {
    asupersync::test_utils::run_test(|| async {
        let base = &[
            "CREATE TABLE t(a, b)",
            "INSERT INTO t(rowid,a,b) VALUES(1,'x','y')",
        ];
        // (1) Implicit-rowid table (no INTEGER PRIMARY KEY alias): an explicit
        // colliding rowid is a UNIQUE violation on the hidden `rowid` column,
        // under every spelling of the alias and at any column-list position.
        for dml in [
            "INSERT INTO t(rowid,a,b) VALUES(1,'p','q')",
            "INSERT INTO t(_rowid_,a,b) VALUES(1,'p','q')",
            "INSERT INTO t(oid,a,b) VALUES(1,'p','q')",
            "INSERT INTO t(a,b,rowid) VALUES('p','q',1)",
            "INSERT OR ABORT INTO t(rowid,a,b) VALUES(1,'p','q')",
            "INSERT OR FAIL INTO t(rowid,a,b) VALUES(1,'p','q')",
        ] {
            agree_collision(base, dml, "UNIQUE constraint failed: t.rowid").await;
        }

        // (1d) UPDATE that moves a row's rowid onto an existing rowid — the
        // shape that used to fail earlier with `no such column: rowid`.
        let two = &[
            "CREATE TABLE t(a, b)",
            "INSERT INTO t(rowid,a,b) VALUES(1,10,10),(2,20,20)",
        ];
        for dml in [
            "UPDATE t SET rowid=1 WHERE rowid=2",
            "UPDATE t SET _rowid_=1 WHERE rowid=2",
            "UPDATE t SET oid=1 WHERE rowid=2",
            "UPDATE t SET rowid=rowid+1",
        ] {
            agree_collision(two, dml, "UNIQUE constraint failed: t.rowid").await;
        }

        // (1e) A rowid table with a COMPOSITE PRIMARY KEY still reports
        // `t.rowid` for an explicit-rowid collision (the composite PK is a
        // separate UNIQUE auto-index; the rowid conflict is distinct).
        agree_collision(
            &[
                "CREATE TABLE t(a, b, PRIMARY KEY(a,b))",
                "INSERT INTO t(rowid,a,b) VALUES(1,'x','y')",
            ],
            "INSERT INTO t(rowid,a,b) VALUES(1,'p','q')",
            "UNIQUE constraint failed: t.rowid",
        )
        .await;

        // Contrast: an INTEGER PRIMARY KEY alias keeps naming its own column,
        // even when the collision is expressed through the `rowid` keyword
        // (bd-977wx path — must not regress to `u.rowid`).
        let ipk = &[
            "CREATE TABLE u(k INTEGER PRIMARY KEY, v)",
            "INSERT INTO u(rowid,v) VALUES(1,'a'),(2,'b')",
        ];
        agree_collision(
            ipk,
            "INSERT INTO u(rowid,v) VALUES(1,'c')",
            "UNIQUE constraint failed: u.k",
        )
        .await;
        agree_collision(
            ipk,
            "UPDATE u SET rowid=1 WHERE k=2",
            "UNIQUE constraint failed: u.k",
        )
        .await;

        // Contrast: a composite-PK-value collision (not an explicit rowid)
        // names the PK columns via the auto-index path, unaffected by the
        // rowid label.
        agree_collision(
            &[
                "CREATE TABLE p(a, b, PRIMARY KEY(a,b))",
                "INSERT INTO p VALUES('x','y')",
            ],
            "INSERT INTO p VALUES('x','y')",
            "UNIQUE constraint failed: p.a, p.b",
        )
        .await;

        // Guard (bd-a506j — already correct): WITHOUT ROWID PK collisions
        // report the qualified PK label, single- and multi-column, with and
        // without an INTEGER type on the key.
        agree_collision(
            &[
                "CREATE TABLE w(k PRIMARY KEY, v) WITHOUT ROWID",
                "INSERT INTO w VALUES(1,10)",
            ],
            "INSERT INTO w VALUES(1,20)",
            "UNIQUE constraint failed: w.k",
        )
        .await;
        agree_collision(
            &[
                "CREATE TABLE w(k INTEGER PRIMARY KEY, v) WITHOUT ROWID",
                "INSERT INTO w VALUES(1,10)",
            ],
            "INSERT INTO w VALUES(1,20)",
            "UNIQUE constraint failed: w.k",
        )
        .await;
        agree_collision(
            &[
                "CREATE TABLE w2(a, b, v, PRIMARY KEY(a,b)) WITHOUT ROWID",
                "INSERT INTO w2 VALUES(1,2,10)",
            ],
            "INSERT INTO w2 VALUES(1,2,20)",
            "UNIQUE constraint failed: w2.a, w2.b",
        )
        .await;
    });
}

#[test]
fn update_set_hidden_rowid_matches_stock_p1h2r() {
    asupersync::test_utils::run_test(|| async {
        let two = &[
            "CREATE TABLE t(a, b)",
            "INSERT INTO t(rowid,a,b) VALUES(1,'x','y'),(2,'p','q')",
        ];

        // Plain re-key through each alias spelling; the row keeps its payload.
        for dml in [
            "UPDATE t SET rowid=7 WHERE rowid=1",
            "UPDATE t SET _rowid_=7 WHERE rowid=1",
            "UPDATE t SET oid=7 WHERE rowid=1",
            "UPDATE t SET rowid=7 WHERE a='x'",
        ] {
            agree_dml(two, dml, &[ROWS], "re-key one row").await;
        }

        // Integer coercion of the new key: integral REAL and numeric TEXT
        // convert; NULL, non-numeric TEXT, and a fractional REAL are
        // "datatype mismatch" and leave the table untouched.
        agree_dml(two, "UPDATE t SET rowid=7.0 WHERE rowid=1", &[ROWS], "integral real key").await;
        agree_dml(
            two,
            "UPDATE t SET rowid='7' WHERE rowid=1",
            &[ROWS, "SELECT typeof(rowid) FROM t WHERE a='x'"],
            "numeric text key",
        )
        .await;
        for dml in [
            "UPDATE t SET rowid=NULL WHERE rowid=1",
            "UPDATE t SET rowid='abc' WHERE rowid=1",
            "UPDATE t SET rowid=2.5 WHERE rowid=1",
        ] {
            let got = agree_dml(two, dml, &[ROWS], "non-integer key").await;
            assert_eq!(got, "datatype mismatch");
        }

        // Full-scan rewrite that moves every row (the two-pass rowset must not
        // revisit rows at their new keys), and one that reads the old key.
        agree_dml(two, "UPDATE t SET rowid=rowid+10", &[ROWS], "shift all rowids").await;
        agree_dml(two, "UPDATE t SET rowid=-rowid", &[ROWS], "negate all rowids").await;
        agree_dml(
            two,
            "UPDATE t SET rowid=rowid*100, a=a||rowid",
            &[ROWS],
            "SET reads the old rowid",
        )
        .await;

        // Every index is re-keyed (index entries carry the rowid): lookups
        // through the index and the table agree afterwards, and both engines'
        // integrity_check stays ok.
        let indexed = &[
            "CREATE TABLE t(a, b)",
            "CREATE INDEX t_a ON t(a)",
            "CREATE INDEX t_ba ON t(b, a)",
            "INSERT INTO t(rowid,a,b) VALUES(1,'x','y'),(2,'p','q'),(3,'x','z')",
        ];
        agree_dml(
            indexed,
            "UPDATE t SET rowid=rowid+5 WHERE a='x'",
            &[
                ROWS,
                "SELECT rowid FROM t INDEXED BY t_a WHERE a='x' ORDER BY rowid",
                "SELECT rowid FROM t INDEXED BY t_ba WHERE b='y'",
                "SELECT count(*) FROM t INDEXED BY t_a WHERE a='p'",
            ],
            "indexes re-keyed",
        )
        .await;

        // Conflict resolution on the re-key.
        agree_dml(
            two,
            "UPDATE OR IGNORE t SET rowid=2 WHERE rowid=1",
            &[ROWS],
            "OR IGNORE keeps both rows",
        )
        .await;
        agree_dml(
            two,
            "UPDATE OR REPLACE t SET rowid=2 WHERE rowid=1",
            &[ROWS],
            "OR REPLACE evicts the victim",
        )
        .await;

        // RETURNING sees the new key.
        {
            let f = Connection::open(":memory:").await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for s in two {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
            let sql = "UPDATE t SET rowid=5 WHERE rowid=1 RETURNING rowid, a";
            let fr = frank_rows(&f, sql).await;
            let rr = stock_rows(&r, sql);
            assert_eq!(fr, rr, "RETURNING after re-key\n  frank ={fr:?}\n  sqlite={rr:?}");
            assert_eq!(fr, vec![vec!["5".to_owned(), "'x'".to_owned()]]);
        }

        // Multi-column SET carrying the rowid, as a row value and as a
        // subquery source.
        agree_dml(
            two,
            "UPDATE t SET (rowid, a) = (5, 'z') WHERE rowid=1",
            &[ROWS],
            "row-value SET with rowid",
        )
        .await;
        agree_dml(
            two,
            "UPDATE t SET (a, rowid) = (SELECT 'w', 9) WHERE rowid=2",
            &[ROWS],
            "subquery SET with rowid",
        )
        .await;

        // UPDATE ... FROM: the new key comes from the joined source.
        agree_dml(
            &[
                "CREATE TABLE t(a, b)",
                "INSERT INTO t(rowid,a,b) VALUES(1,'x','y'),(2,'p','q')",
                "CREATE TABLE s(a, n)",
                "INSERT INTO s VALUES('x', 40),('p', 50)",
            ],
            "UPDATE t SET rowid = s.n FROM s WHERE s.a = t.a",
            &[ROWS],
            "UPDATE FROM re-key",
        )
        .await;

        // A declared column literally named `rowid` shadows the alias: SET
        // rowid touches that column, the real key (`_rowid_`) is unchanged.
        agree_dml(
            &[
                "CREATE TABLE t(a, rowid)",
                "INSERT INTO t(a, rowid) VALUES('x', 100)",
            ],
            "UPDATE t SET rowid=5",
            &["SELECT _rowid_, rowid, a FROM t"],
            "declared rowid column shadows the alias",
        )
        .await;

        // On an INTEGER PRIMARY KEY table `SET rowid =` is the alias column.
        agree_dml(
            &[
                "CREATE TABLE u(k INTEGER PRIMARY KEY, v)",
                "INSERT INTO u VALUES(1,'a'),(2,'b')",
            ],
            "UPDATE u SET rowid=9 WHERE k=1",
            &["SELECT rowid, k, v FROM u ORDER BY k"],
            "IPK alias table",
        )
        .await;

        // WITHOUT ROWID: no rowid to assign, on either engine.
        let got = agree_dml(
            &[
                "CREATE TABLE w(k PRIMARY KEY, v) WITHOUT ROWID",
                "INSERT INTO w VALUES(1,10)",
            ],
            "UPDATE w SET rowid=5",
            &["SELECT * FROM w"],
            "WITHOUT ROWID rejects the alias",
        )
        .await;
        assert_eq!(got, "no such column: rowid");

        // Boundary: a table with an UPDATE trigger takes the trigger-snapshot
        // lane, which cannot carry a rewritten hidden rowid yet. The refusal
        // must be a clean not-implemented error (never Internal) and must not
        // mutate the table.
        {
            let f = Connection::open(":memory:").await.unwrap();
            for s in [
                "CREATE TABLE t(a, b)",
                "INSERT INTO t(rowid,a,b) VALUES(1,'x','y'),(2,'p','q')",
                "CREATE TABLE log(n)",
                "CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN INSERT INTO log VALUES(new.a); END",
            ] {
                f.execute(s).await.unwrap();
            }
            let got = frank_exec(&f, "UPDATE t SET rowid=7 WHERE rowid=1").await;
            assert!(
                got.starts_with("not implemented:") && got.contains("rowid"),
                "trigger-lane boundary must be a clean not-implemented error (was: {got:?})"
            );
            let rows = frank_rows(&f, ROWS).await;
            assert_eq!(rows[0][0], "1", "refused UPDATE must not mutate (rows: {rows:?})");
            let log = frank_rows(&f, "SELECT count(*) FROM log").await;
            assert_eq!(log, vec![vec!["0".to_owned()]]);
        }
    });
}
