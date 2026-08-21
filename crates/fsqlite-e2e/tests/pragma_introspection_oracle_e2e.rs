//! bd-00aan — Oracle-parity e2e: schema-introspection PRAGMAs vs rusqlite.
//!
//! `PRAGMA table_info` / `table_xinfo` / `foreign_key_list` / `index_list` /
//! `index_info` / `index_xinfo` expose schema metadata with well-defined column
//! layouts. These are exactly the surfaces ORMs and tooling read, so divergence
//! in column counts, notnull/pk flags, default-value rendering, FK action
//! strings, or index origin/uniqueness is user-visible. All schemas are fixed.

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

async fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
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
fn pragma_table_info_columns() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&["CREATE TABLE t (\
           id INTEGER PRIMARY KEY, \
           name TEXT NOT NULL, \
           qty INTEGER DEFAULT 0, \
           price REAL DEFAULT 1.5, \
           note TEXT, \
           tag TEXT NOT NULL DEFAULT 'x')"])
        .await;
        check(
            &f,
            &r,
            &["PRAGMA table_info(t)"],
            "pragma_table_info_columns",
        )
        .await;
    });
}

#[test]
fn pragma_table_info_composite_pk() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&["CREATE TABLE t (\
           a INTEGER, b TEXT, c INTEGER, \
           PRIMARY KEY (b, a))"])
        .await;
        // The `pk` column reflects the position within the composite primary key.
        check(
            &f,
            &r,
            &["PRAGMA table_info(t)"],
            "pragma_table_info_composite_pk",
        )
        .await;
    });
}

#[test]
fn pragma_table_xinfo_hidden_column() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) =
            setup(&["CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER AS (a + b) STORED)"]).await;
        // table_xinfo adds the trailing `hidden` column (generated => 2/3).
        check(
            &f,
            &r,
            &["PRAGMA table_xinfo(t)"],
            "pragma_table_xinfo_hidden_column",
        )
        .await;
    });
}

#[test]
fn pragma_foreign_key_list() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
            "CREATE TABLE child (\
           id INTEGER PRIMARY KEY, \
           pid INTEGER REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL, \
           pcode TEXT REFERENCES parent(code) ON DELETE RESTRICT)",
        ])
        .await;
        check(
            &f,
            &r,
            &["PRAGMA foreign_key_list(child)"],
            "pragma_foreign_key_list",
        )
        .await;
    });
}

#[test]
fn pragma_index_info() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)",
            "CREATE UNIQUE INDEX idx_a ON t(a)",
            "CREATE INDEX idx_bc ON t(b, c DESC)",
        ])
        .await;
        check(
            &f,
            &r,
            &["PRAGMA index_info(idx_a)", "PRAGMA index_info(idx_bc)"],
            "pragma_index_info",
        )
        .await;
    });
}

#[test]
fn pragma_index_list() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)",
            "CREATE UNIQUE INDEX idx_a ON t(a)",
            "CREATE INDEX idx_bc ON t(b, c DESC)",
        ])
        .await;
        check(&f, &r, &["PRAGMA index_list(t)"], "pragma_index_list").await;
    });
}

#[test]
fn pragma_index_xinfo_with_direction() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
            "CREATE INDEX idx_ab ON t(a DESC, b COLLATE NOCASE)",
        ])
        .await;
        // index_xinfo exposes seqno, cid, name, desc, coll, key for index + covered cols.
        check(
            &f,
            &r,
            &["PRAGMA index_xinfo(idx_ab)"],
            "pragma_index_xinfo_with_direction",
        )
        .await;
    });
}

#[test]
fn pragma_index_list_unique_origin() {
    asupersync::test_utils::run_test(|| async {
        // A UNIQUE constraint creates an auto-index with origin 'u'; an explicit
        // CREATE INDEX has origin 'c'; the PK auto-index has origin 'pk'.
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
            "CREATE INDEX idx_name ON t(name)",
        ])
        .await;
        check(
            &f,
            &r,
            &["PRAGMA index_list(t)"],
            "pragma_index_list_unique_origin",
        )
        .await;
    });
}

/// GH #344 (bd-sn184) -- FIXED. Regression guard for `PRAGMA index_list` on a
/// WITHOUT ROWID table reporting the PRIMARY KEY auto-index.
///
/// The defect (now fixed): `PRAGMA index_list` on a WITHOUT ROWID table omitted
/// the PRIMARY KEY auto-index that stock SQLite reports. Measured against the
/// oracle in this same harness, the engine used to return:
///     frank: [_3|u, _2|u]
///     csql:  [_3|u, _2|u, sqlite_autoindex_w_1|pk]
/// i.e. the `pk` row was ABSENT.
///
/// ROOT CAUSE: the CREATE handler (connection.rs, is_hidden_without_rowid_
/// primary_key) correctly skips the hidden WITHOUT ROWID primary-key slot when
/// building `implicit_indexes` -- stock does not materialise a separate b-tree
/// for it either -- but the `index_list` reporting handler derived `origin` only
/// from entries already present in `t.indexes`, so it had nothing to label. The
/// engine conflated "not materialised as a separate b-tree" (correct) with "not
/// reportable by introspection" (incorrect -- stock reports it).
///
/// THE FIX (connection.rs `index_list`): synthesise the primary-key entry for
/// WITHOUT ROWID tables at the REPORTING layer only -- at its canonical ordinal
/// (the auto-index number missing from the persisted set), spliced into creation
/// order so the reverse ordering matches stock. Root allocation is untouched:
/// `sqlite_master` for `w` still contains only sqlite_autoindex_w_2 and _3 -- no
/// new schema row, no new root page. The `sqlite_master` invariant is what
/// distinguishes this reporting fix from the wrong one.
///
/// DO NOT make this green by deleting the CREATE-time skip: that would allocate
/// a real root b-tree for an index that must not have one -- a corruption risk,
/// not a fix. DO NOT relax this assertion or add #[ignore]; it is the signal
/// that the reporting stays correct.
///
/// The sibling `pragma_index_list_rowid_composite_pk` runs the identical DDL
/// without the WITHOUT ROWID clause and also passes, scoping the original defect
/// to the WITHOUT ROWID path rather than to origin `pk` generally.
#[test]
fn pragma_index_list_without_rowid_composite_pk() {
    asupersync::test_utils::run_test(|| async {
        // Stock SQLite 3.46.1 on this DDL:
        //     0|sqlite_autoindex_w_3|1|u|0
        //     1|sqlite_autoindex_w_2|1|u|0
        //     2|sqlite_autoindex_w_1|1|pk|0
        //
        // No pre-existing case in this file uses a WITHOUT ROWID table, which
        // is why the divergence survived on an otherwise oracle-covered
        // surface.
        let (f, r) = setup(&[
            "CREATE TABLE w (a TEXT NOT NULL, b INTEGER NOT NULL, c TEXT NOT NULL, d TEXT NOT NULL, \
             PRIMARY KEY(a,b), UNIQUE(a,c), UNIQUE(d)) WITHOUT ROWID",
        ])
        .await;
        check(
            &f,
            &r,
            &["PRAGMA index_list(w)"],
            "pragma_index_list_without_rowid_composite_pk",
        )
        .await;
    });
}

#[test]
fn pragma_index_list_rowid_composite_pk() {
    asupersync::test_utils::run_test(|| async {
        // Diagnostic sibling of the WITHOUT ROWID case above: identical DDL,
        // no WITHOUT ROWID clause. This exercises origin 'pk' for the first
        // time anywhere in this file -- `pragma_index_list_unique_origin`
        // claims to cover it but its `id INTEGER PRIMARY KEY` fixture is a
        // rowid alias that creates no auto-index at all, so on 3.46.1 that DDL
        // yields only `idx_name|c` and `sqlite_autoindex_t_1|u` and never a
        // 'pk' row.
        //
        // Read together with the WITHOUT ROWID case: this one GREEN means the
        // defect is specific to WITHOUT ROWID; this one RED means origin 'pk'
        // is unreported generally, which is a much larger blast radius.
        let (f, r) = setup(&[
            "CREATE TABLE r (a TEXT NOT NULL, b INTEGER NOT NULL, c TEXT NOT NULL, d TEXT NOT NULL, \
             PRIMARY KEY(a,b), UNIQUE(a,c), UNIQUE(d))",
        ])
        .await;
        check(
            &f,
            &r,
            &["PRAGMA index_list(r)"],
            "pragma_index_list_rowid_composite_pk",
        )
        .await;
    });
}

/// GH #352 (bd-btdr7) -- the residual half of #344. `index_list` now reports the
/// WITHOUT ROWID PRIMARY KEY auto-index (bd-sn184), but `index_xinfo` on that
/// slot returned NO ROWS, so a consumer reading key columns saw a PK that
/// "exists but has no columns". Stock enumerates the PK columns (key=1) followed
/// by the remaining table columns (key=0):
///
///     index_xinfo(sqlite_autoindex_w_1) -> a|1, b|1, c|0, d|0
///
/// `index_info` on the same slot reports only the key columns (a, b). Both are
/// compared against the C-SQLite oracle on the identical DDL used by the #344
/// sibling above.
#[test]
fn pragma_index_xinfo_without_rowid_pk_gh352() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE w (a TEXT NOT NULL, b INTEGER NOT NULL, c TEXT NOT NULL, d TEXT NOT NULL, \
             PRIMARY KEY(a,b), UNIQUE(a,c), UNIQUE(d)) WITHOUT ROWID",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "PRAGMA index_xinfo(sqlite_autoindex_w_1)",
                "PRAGMA index_info(sqlite_autoindex_w_1)",
            ],
            "pragma_index_xinfo_without_rowid_pk",
        )
        .await;
    });
}

/// The WITHOUT ROWID PK auto-index synthesised for `index_xinfo` reported the
/// PK columns' sort direction as ASC (desc=0) unconditionally, because the
/// reporting path hard-coded desc=0 with a stale "PK sort order is not retained"
/// comment -- even though the direction IS retained (without_rowid_pk_desc,
/// bd-w9r11 / GH#222/#223), which the storage-cursor metadata path already
/// honors. So `PRIMARY KEY(x DESC)` on a WITHOUT ROWID table returned desc=0
/// where stock returns desc=1. Covers single-column DESC and a multi-column
/// mixed-direction PK (desc flags must track PK column order, and the trailing
/// covered columns stay desc=0).
#[test]
fn pragma_index_xinfo_without_rowid_desc_pk() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE w1 (x TEXT, y INTEGER, PRIMARY KEY(x DESC)) WITHOUT ROWID",
            "CREATE TABLE w2 (a INTEGER, b TEXT, c INTEGER, PRIMARY KEY(a DESC, b, c DESC)) \
             WITHOUT ROWID",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "PRAGMA index_xinfo(sqlite_autoindex_w1_1)",
                "PRAGMA index_info(sqlite_autoindex_w1_1)",
                "PRAGMA index_xinfo(sqlite_autoindex_w2_1)",
                "PRAGMA index_info(sqlite_autoindex_w2_1)",
            ],
            "pragma_index_xinfo_without_rowid_desc_pk",
        )
        .await;
    });
}
