#![recursion_limit = "512"]

//! bd-scbx6 (GH#353, P0): WITHOUT ROWID + composite UNIQUE auto-index maintenance.
//!
//! DML on a WITHOUT ROWID table must maintain the auto-indexes backing composite
//! UNIQUE constraints so that (a) index scans / `INDEXED BY` queries return the
//! correct rows and (b) the UNIQUE constraint actually fires on a duplicate.
//! This is a differential-vs-stock (rusqlite = bundled SQLite) oracle for the
//! query-visibility + phantom-constraint angle of GH#353 (bd-5ava1 fixed the
//! on-disk key layout / integrity_check angle in 3d3cdda45).

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

#[test]
fn named_index_record_key_probe_is_search() {
    use fsqlite_core::explain::program_seeks_named_index;
    use fsqlite_types::opcode::{Opcode, P4};
    use fsqlite_vdbe::ProgramBuilder;

    for opcode in [Opcode::NoConflict, Opcode::NotFound, Opcode::Rewind] {
        for cursor in [0, 1] {
            let mut b = ProgramBuilder::new();
            b.emit_op(Opcode::OpenRead, 0, 2, 0, P4::Table("facts".to_owned()), 0);
            b.emit_op(
                Opcode::OpenRead,
                1,
                3,
                0,
                P4::Index("facts_unique".to_owned()),
                0,
            );
            b.emit_op(opcode, cursor, 0, 2, P4::Int(2), 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            let program = b.finish().unwrap();
            // Neither a table PK probe nor an index scan proves a named
            // secondary-index SEARCH, even when that index is open.
            assert_eq!(
                program_seeks_named_index(&program, "facts_unique"),
                cursor == 1 && opcode != Opcode::Rewind,
                "{opcode:?} on cursor {cursor}"
            );
            assert!(!program_seeks_named_index(&program, "another_index"));
        }
    }
}

struct AsciiCaseFoldBinary;

impl fsqlite_func::collation::CollationFunction for AsciiCaseFoldBinary {
    fn name(&self) -> &str {
        "BINARY"
    }

    fn compare(&self, left: &[u8], right: &[u8]) -> std::cmp::Ordering {
        left.to_ascii_lowercase().cmp(&right.to_ascii_lowercase())
    }
}

#[test]
fn wr_unique_point_probe_honors_overridden_binary() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for setup in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,b INTEGER,UNIQUE(a,b)) WITHOUT ROWID",
            "INSERT INTO t VALUES(1,'stored',7)",
        ] {
            f.execute(setup).await.unwrap();
            r.execute_batch(setup).unwrap();
        }
        let sql = "SELECT id FROM t WHERE a='STORED' AND b=7";
        let prepared = f.prepare(sql).await.unwrap();
        assert!(prepared.query().await.unwrap().is_empty());
        f.register_collation_function(AsciiCaseFoldBinary);
        // FrankenSQLite permits overriding BINARY. Match that declared ASCII
        // comparison against explicit stock NOCASE, not SQLite's built-in
        // BINARY override behavior (which differs on some SQLite versions).
        let expected = stock_rows(
            &r,
            "SELECT id FROM t WHERE a='STORED' COLLATE NOCASE AND b=7",
        )
        .unwrap();
        assert!(matches!(
            prepared.query().await.unwrap_err(),
            fsqlite_error::FrankenError::SchemaChanged
        ));
        for rows in [
            f.prepare(sql).await.unwrap().query().await.unwrap(),
            f.query(sql).await.unwrap(),
        ] {
            let actual: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.values().iter().map(render).collect())
                .collect();
            assert_eq!(
                actual, expected,
                "overridden BINARY must govern the index probe"
            );
        }
        assert!(
            f.query("SELECT id FROM t WHERE a='STORED' AND b=8")
                .await
                .unwrap()
                .is_empty()
        );
    });
}

const SCHEMA: &str = "CREATE TABLE members(\
    a TEXT, b INTEGER, c INTEGER, \
    PRIMARY KEY(a, b), UNIQUE(a, c)\
) WITHOUT ROWID";

fn render(v: &SqliteValue) -> String {
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
    let rows = conn.query(sql).await.map_err(|e| format!("{e:?}"))?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect())
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => {
                    format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    )
                }
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

// hfdt-gbou9l: complete unique keys must not scan all rows sharing the first
// term. Exercise the actual VM and stock oracle, including a PK column shared
// with the secondary key (its physical suffix is deduplicated).
#[test]
fn wr_composite_unique_point_seek_preserves_results_and_avoids_scan() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            SCHEMA,
            "INSERT INTO members VALUES ('shared', 0, 100), ('shared', 1, 101), ('shared', 2, NULL), ('other', 3, 101)",
        ] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        for sql in [
            "SELECT b FROM members WHERE a='shared' AND c=101",
            "SELECT b FROM members WHERE c=101 AND a='shared'",
            "SELECT b FROM members WHERE a='shared' AND c=999",
            "SELECT b FROM members WHERE a='missing' AND c=101",
            "SELECT b FROM members WHERE a='shared' AND c=NULL",
            "SELECT b FROM members WHERE a='shared' AND c=101 AND b=0",
            "SELECT b FROM members WHERE a='shared' AND c=101 AND c=100",
            "SELECT b FROM members WHERE a='shared' AND c=101 LIMIT 0",
            "SELECT b FROM members WHERE a='shared' AND c=101 LIMIT 1 OFFSET 1",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await.unwrap(),
                stock_rows(&r, sql).unwrap(),
                "{sql}"
            );
        }
        let sql = "SELECT b FROM members WHERE c=?1 AND a=?2";
        for (ordinal, prefix) in [("101", "shared"), ("999", "shared"), ("101", "other")] {
            let rows = f
                .query_with_params(
                    sql,
                    &[SqliteValue::from(ordinal), SqliteValue::from(prefix)],
                )
                .await
                .unwrap();
            let actual: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.values().iter().map(render).collect())
                .collect();
            let mut stmt = r.prepare(sql).unwrap();
            let expected: Vec<Vec<String>> = stmt
                .query_map([ordinal, prefix], |row| {
                    Ok(vec![row.get::<_, i64>(0)?.to_string()])
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(
                actual, expected,
                "numbered parameters and affinity: {ordinal}, {prefix}"
            );
        }
        let plan = f
            .query("EXPLAIN SELECT b FROM members WHERE a='shared' AND c=101")
            .await
            .unwrap();
        let opcodes: Vec<String> = plan
            .iter()
            .map(|row| match &row.values()[1] {
                SqliteValue::Text(op) => op.to_string(),
                other => panic!("unexpected opcode {other:?}"),
            })
            .collect();
        assert_eq!(
            opcodes
                .iter()
                .filter(|op| op.as_str() == "NoConflict")
                .count(),
            2,
            "one full secondary-key probe and one primary-key probe: {opcodes:?}"
        );
        assert!(
            !opcodes
                .iter()
                .any(|op| matches!(op.as_str(), "Rewind" | "Next" | "IdxRowid")),
            "a unique point lookup must not scan a duplicate prefix or assume a rowid: {opcodes:?}"
        );
    });
}

#[test]
fn wr_composite_unique_index_scan_and_constraint_bd_scbx6() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            SCHEMA,
            "INSERT INTO members VALUES ('shared', 0, 100)",
            "INSERT INTO members VALUES ('shared', 1, 101)",
            "INSERT INTO members VALUES ('shared', 2, 102)",
        ] {
            f.execute(s)
                .await
                .unwrap_or_else(|e| panic!("frank `{s}`: {e:?}"));
            r.execute_batch(s)
                .unwrap_or_else(|e| panic!("stock `{s}`: {e}"));
        }

        // (1) Index-scan / covering reads over the composite-UNIQUE(a,c) auto-index
        // must match stock. If the auto-index is not maintained, these return
        // wrong/empty rows.
        let mut mismatches = Vec::new();
        for q in [
            "SELECT b FROM members WHERE a = 'shared' AND c = 101",
            "SELECT b FROM members WHERE a = 'shared' AND c = 102",
            "SELECT a, b, c FROM members WHERE a = 'shared' AND c = 100",
            "SELECT c FROM members WHERE a = 'shared' ORDER BY c",
            "SELECT b FROM members INDEXED BY sqlite_autoindex_members_2 WHERE a = 'shared' AND c = 102",
        ] {
            match (frank_rows(&f, q).await, stock_rows(&r, q)) {
                (Ok(a), Ok(b)) if a == b => {}
                (fa, sb) => mismatches.push(format!("`{q}` -> frank={fa:?} stock={sb:?}")),
            }
        }
        assert!(
            mismatches.is_empty(),
            "bd-scbx6 index-scan divergence(s):\n{}",
            mismatches.join("\n")
        );

        // (2) Phantom UNIQUE(a,c): inserting a duplicate (a,c) pair must be
        // rejected. c=100 already exists under a='shared'.
        let dup = "INSERT INTO members VALUES ('shared', 3, 100)";
        let frank_err = f.execute(dup).await.is_err();
        let stock_err = r.execute_batch(dup).is_err();
        assert!(stock_err, "sanity: stock must reject the duplicate (a,c)");
        assert_eq!(
            frank_err, stock_err,
            "bd-scbx6 phantom UNIQUE(a,c): frank rejected={frank_err}, stock rejected={stock_err} \
             (a composite-UNIQUE auto-index that is not maintained lets the duplicate through)"
        );

        // A non-conflicting distinct (a,c) still inserts in both.
        let ok = "INSERT INTO members VALUES ('shared', 3, 103)";
        assert!(
            f.execute(ok).await.is_ok(),
            "frank must accept a distinct (a,c)"
        );
        assert!(
            r.execute_batch(ok).is_ok(),
            "stock must accept a distinct (a,c)"
        );

        // (3) UPDATE must maintain the composite-UNIQUE auto-index: move c 101 -> 200.
        let upd = "UPDATE members SET c = 200 WHERE a = 'shared' AND b = 1";
        f.execute(upd)
            .await
            .unwrap_or_else(|e| panic!("frank update: {e:?}"));
        r.execute_batch(upd)
            .unwrap_or_else(|e| panic!("stock update: {e}"));
        let mut upd_mismatch = Vec::new();
        for q in [
            "SELECT b FROM members WHERE a = 'shared' AND c = 200", // new value -> b=1
            "SELECT b FROM members WHERE a = 'shared' AND c = 101", // old value -> gone
        ] {
            match (frank_rows(&f, q).await, stock_rows(&r, q)) {
                (Ok(a), Ok(b)) if a == b => {}
                (fa, sb) => upd_mismatch.push(format!("`{q}` -> frank={fa:?} stock={sb:?}")),
            }
        }
        assert!(
            upd_mismatch.is_empty(),
            "bd-scbx6 UPDATE auto-index divergence:\n{}",
            upd_mismatch.join("\n")
        );
        // phantom after UPDATE: c=200 now occupied, a fresh dup must fail in both.
        let dup2 = "INSERT INTO members VALUES ('shared', 4, 200)";
        assert_eq!(
            f.execute(dup2).await.is_err(),
            r.execute_batch(dup2).is_err(),
            "bd-scbx6 phantom UNIQUE after UPDATE"
        );

        // (4) DELETE must remove the auto-index entry: after deleting b=1 (c=200),
        // c=200 becomes free and its index scan is empty in both.
        let del = "DELETE FROM members WHERE a = 'shared' AND b = 1";
        f.execute(del)
            .await
            .unwrap_or_else(|e| panic!("frank delete: {e:?}"));
        r.execute_batch(del)
            .unwrap_or_else(|e| panic!("stock delete: {e}"));
        let q = "SELECT b FROM members WHERE a = 'shared' AND c = 200";
        assert_eq!(
            frank_rows(&f, q).await.unwrap(),
            stock_rows(&r, q).unwrap(),
            "bd-scbx6 DELETE auto-index divergence (stale index entry?): `{q}`"
        );
        // c=200 is now free: re-insert must succeed in both.
        let reins = "INSERT INTO members VALUES ('shared', 5, 200)";
        assert_eq!(
            f.execute(reins).await.is_ok(),
            r.execute_batch(reins).is_ok(),
            "bd-scbx6 re-insert after DELETE freed (a,c)"
        );
    });
}

#[test]
fn wr_unique_probe_with_separate_pk_and_trigger_guard() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE records(id TEXT PRIMARY KEY, batch TEXT, ordinal INTEGER, digest TEXT, body TEXT, UNIQUE(batch,ordinal), UNIQUE(batch,digest,body)) WITHOUT ROWID",
            "CREATE TRIGGER duplicate_record BEFORE INSERT ON records WHEN EXISTS(SELECT 1 FROM records WHERE batch=NEW.batch AND ordinal=NEW.ordinal) BEGIN SELECT RAISE(ABORT,'duplicate record guard'); END",
            "INSERT INTO records VALUES ('first','group',1,'hash','body'), ('second','group',2,'hash','different body')",
        ] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        for sql in [
            "SELECT id FROM records WHERE batch='group' AND ordinal=2",
            "SELECT id FROM records WHERE batch='group' AND digest='hash' AND body='different body'",
            "SELECT id FROM records WHERE batch='group' AND digest='hash' AND body='absent'",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await.unwrap(),
                stock_rows(&r, sql).unwrap(),
                "{sql}"
            );
            let plan = f.query(&format!("EXPLAIN {sql}")).await.unwrap();
            let opcodes: Vec<_> = plan.iter().map(|row| render(&row.values()[1])).collect();
            assert_eq!(
                opcodes
                    .iter()
                    .filter(|op| op.as_str() == "'NoConflict'")
                    .count(),
                2,
                "{sql}: {opcodes:?}"
            );
            assert!(
                !opcodes.iter().any(|op| op == "'Next'" || op == "'Rewind'"),
                "{sql}: {opcodes:?}"
            );
        }
        let duplicate = "INSERT INTO records VALUES ('third','group',2,'other hash','other body')";
        let frank_error = f.execute(duplicate).await.unwrap_err().to_string();
        let stock_error = r.execute_batch(duplicate).unwrap_err().to_string();
        assert!(
            frank_error.contains("duplicate record guard"),
            "{frank_error}"
        );
        assert!(
            stock_error.contains("duplicate record guard"),
            "{stock_error}"
        );
        assert_eq!(
            frank_rows(&f, "SELECT count(*) FROM records")
                .await
                .unwrap(),
            stock_rows(&r, "SELECT count(*) FROM records").unwrap()
        );
    });
}

#[test]
fn wr_unique_seek_declines_unsafe_index_shapes() {
    asupersync::test_utils::run_test(|| async {
        for (schema, index, inserts, query) in [
            (
                "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,b INTEGER) WITHOUT ROWID",
                "CREATE INDEX idx ON t(a,b)",
                "INSERT INTO t VALUES(1,'x',2),(2,'x',2)",
                "SELECT id FROM t WHERE a='x' AND b=2",
            ),
            (
                "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,b INTEGER) WITHOUT ROWID",
                "CREATE UNIQUE INDEX idx ON t(a,b) WHERE id>1",
                "INSERT INTO t VALUES(1,'x',2),(2,'x',2)",
                "SELECT id FROM t WHERE a='x' AND b=2",
            ),
            (
                "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT COLLATE NOCASE,b INTEGER) WITHOUT ROWID",
                "CREATE UNIQUE INDEX idx ON t(a,b)",
                "INSERT INTO t VALUES(1,'X',2),(2,'Y',2)",
                "SELECT id FROM t WHERE a='x' AND b=2",
            ),
            (
                "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,b INTEGER) WITHOUT ROWID",
                "CREATE UNIQUE INDEX idx ON t(a DESC,b DESC)",
                "INSERT INTO t VALUES(1,'x',2),(2,'x',3)",
                "SELECT id FROM t WHERE a='x' AND b=2",
            ),
        ] {
            let f = Connection::open(":memory:").await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [schema, index, inserts] {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            let mut actual = frank_rows(&f, query).await.unwrap();
            let mut expected = stock_rows(&r, query).unwrap();
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected, "{schema}; {index}; {query}");
        }
    });
}
