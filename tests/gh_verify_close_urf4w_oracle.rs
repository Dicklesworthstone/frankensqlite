#![recursion_limit = "512"]

//! bd-urf4w GH-verify-close batch — empirical verification at HEAD of the three
//! issues that carry a concrete reproduction, each differential vs rusqlite
//! (SQLite 3.46.1). #289 is structure-only (no repro) and #342 is design-gated
//! (read-only VACUUM INTO snapshot path), so neither is covered here.
//!
//!  #179  CTAS must preserve each row's storage class (mixed types), not coerce
//!        the whole column to TEXT.
//!  #196  INSERT OR REPLACE must remove the victim's partial- AND expression-
//!        index entries (no stale entries / integrity failure).
//!  #340  VACUUM INTO must correctly persist a WITHOUT ROWID table (its compat
//!        serializer previously produced an invalid root and fail-closed).

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

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

/// #179: CTAS preserves per-row storage class for a mixed-type source column.
#[test]
fn ctas_preserves_storage_class_gh179() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        for sql in [
            "CREATE TABLE s(a)",
            "INSERT INTO s(a) VALUES ('1')", // TEXT
            "INSERT INTO s(a) VALUES (2)",   // INTEGER
            "CREATE TABLE t AS SELECT a FROM s",
        ] {
            f.execute(sql).await.unwrap();
        }
        let types = frank_rows(&f, "SELECT typeof(a) FROM t ORDER BY rowid").await;
        assert_eq!(
            types,
            vec![vec!["'text'".to_owned()], vec!["'integer'".to_owned()]],
            "GH#179: CTAS must keep text then integer, not coerce both to text"
        );
    });
}

/// #196: INSERT OR REPLACE removes the victim's partial + expression index
/// entries — verified by PRAGMA integrity_check reporting ok.
#[test]
fn replace_cleans_partial_and_expression_index_gh196() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, u INTEGER UNIQUE, a TEXT)",
            "CREATE INDEX p ON t(a) WHERE a IS NOT NULL",
            "CREATE INDEX e ON t(lower(a))",
            "INSERT INTO t(id, u, a) VALUES (1, 100, 'Victim')",
            // REPLACE via the UNIQUE conflict on u=100: victim row id=1 is deleted,
            // replacement id=2 inserted. Its partial/expression entries must go too.
            "INSERT OR REPLACE INTO t(id, u, a) VALUES (2, 100, 'Winner')",
        ] {
            f.execute(sql).await.unwrap();
        }
        // Only the replacement row remains.
        assert_eq!(
            frank_rows(&f, "SELECT id, a FROM t ORDER BY id").await,
            vec![vec!["2".to_owned(), "'Winner'".to_owned()]],
            "GH#196: only the replacement row must remain"
        );
        // No stale index entries: integrity_check is ok, and a forced lookup of
        // the victim's old expression key finds nothing.
        assert_eq!(
            frank_rows(&f, "PRAGMA integrity_check").await,
            vec![vec!["'ok'".to_owned()]],
            "GH#196: integrity_check must be ok (no stale partial/expression entries)"
        );
        assert!(
            frank_rows(&f, "SELECT id FROM t WHERE lower(a) = 'victim'")
                .await
                .is_empty(),
            "GH#196: the victim's old expression-index key must not resolve"
        );
        assert!(
            frank_rows(&f, "SELECT id FROM t WHERE a = 'Victim'")
                .await
                .is_empty(),
            "GH#196: the victim's old partial-index key must not resolve"
        );
    });
}

/// #340: VACUUM INTO correctly persists a WITHOUT ROWID table; a fresh stock
/// SQLite reader reads it back cleanly.
#[test]
fn vacuum_into_without_rowid_gh340() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let src = dir.path().join("src.db");
        let out = dir.path().join("out.db");
        let f = Connection::open(src.to_str().unwrap()).await.unwrap();
        for sql in [
            "CREATE TABLE wr(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
            "INSERT INTO wr(k, v) VALUES ('alpha', 1), ('bravo', 2), ('charlie', 3)",
        ] {
            f.execute(sql).await.unwrap();
        }
        // Previously fail-closed with "root page N is not an index b-tree page".
        f.execute(&format!("VACUUM INTO '{}'", out.display()))
            .await
            .expect("GH#340: VACUUM INTO of a WITHOUT ROWID db must succeed");

        // A fresh stock SQLite reader validates the output.
        let stock = rusqlite::Connection::open(&out).unwrap();
        let ic: String = stock
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            ic, "ok",
            "GH#340: stock integrity_check on VACUUM INTO output"
        );
        let mut st = stock.prepare("SELECT k, v FROM wr ORDER BY k").unwrap();
        let rows: Vec<(String, i64)> = st
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("alpha".to_owned(), 1),
                ("bravo".to_owned(), 2),
                ("charlie".to_owned(), 3)
            ],
            "GH#340: WITHOUT ROWID rows must round-trip through VACUUM INTO"
        );
    });
}
