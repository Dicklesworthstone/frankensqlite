#![recursion_limit = "512"]

//! bd-44nbk old-tail GH triage — wave 3.
//!  #148 legacy double-quoted-string fallback (`SELECT "no_such" FROM t`) — is
//!       frank's strict rejection consistent with the bundled rusqlite oracle?
//!  #140 a native READ-ONLY / schema-only open must not create or mutate any
//!       directory artifact on a clean stock-created fixture.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::collections::BTreeMap;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

/// #148: frank's handling of an unknown double-quoted token must agree with the
/// bundled rusqlite reference (both error, or both fall back to the string).
#[test]
fn gh148_double_quoted_string_matches_reference() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in ["CREATE TABLE t(x)", "INSERT INTO t VALUES (1),(2)"] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        let q = "SELECT \"no_such\" FROM t ORDER BY x";
        let frank = match f.query(q).await {
            Ok(rows) => Ok(rows.iter().map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>()).collect::<Vec<_>>()),
            Err(e) => Err(format!("{e:?}")),
        };
        let stock = {
            let prepared = r.prepare(q);
            match prepared {
                Err(e) => Err(e.to_string()),
                Ok(mut st) => {
                    let n = st.column_count();
                    match st.query_map([], |row| {
                        Ok((0..n).map(|i| match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                            rusqlite::types::Value::Text(s) => format!("'{s}'"),
                            rusqlite::types::Value::Integer(v) => v.to_string(),
                            other => format!("{other:?}"),
                        }).collect::<Vec<_>>())
                    }) {
                        Err(e) => Err(e.to_string()),
                        Ok(rows) => Ok(rows.collect::<Result<Vec<_>, _>>().unwrap()),
                    }
                }
            }
        };
        // Compare only whether both accept-or-reject (error identity/text differs
        // across engines); if both accept, the row sets must match.
        assert_eq!(
            frank.is_ok(), stock.is_ok(),
            "GH#148: frank and the bundled rusqlite reference must agree on accepting/rejecting a bare double-quoted token. frank={frank:?} stock={stock:?}"
        );
        if let (Ok(fr), Ok(sr)) = (&frank, &stock) {
            assert_eq!(fr, sr, "GH#148: both accepted the DQS token but returned different rows");
        }
    });
}

fn snapshot_dir(dir: &std::path::Path) -> BTreeMap<String, (u64, Vec<u8>)> {
    let mut m = BTreeMap::new();
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let entry = entry.expect("dir entry");
        if entry.file_type().expect("file type").is_file() {
            let path = entry.path();
            let bytes = std::fs::read(&path).unwrap_or_default();
            m.insert(entry.file_name().to_string_lossy().into_owned(), (bytes.len() as u64, bytes));
        }
    }
    m
}

/// #140: a READ-ONLY / schema-only open of a clean stock-created database must
/// create, write, truncate, rename, or delete nothing in the directory.
#[test]
fn gh140_readonly_open_does_not_mutate_dir() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ro.db");
        // Build the fixture with stock SQLite so it carries NO frank sidecars.
        {
            let s = rusqlite::Connection::open(&path).unwrap();
            s.execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1,'a'),(2,'b');",
            )
            .unwrap();
        }
        let before = snapshot_dir(dir.path());
        assert_eq!(before.len(), 1, "fixture should be a single main file (stock, no WAL after close)");

        // schema-only (read-only) open + a read query.
        let conn = Connection::open_schema_only(path.to_str().unwrap())
            .await
            .expect("schema-only open of a clean stock db");
        let rows = conn.query("SELECT id, v FROM t ORDER BY id").await.expect("read query");
        assert_eq!(rows.len(), 2, "read-only open must still read the two rows");
        drop(conn);

        let after = snapshot_dir(dir.path());
        assert_eq!(
            after.keys().collect::<Vec<_>>(),
            before.keys().collect::<Vec<_>>(),
            "GH#140: read-only/schema-only open must not create or delete any directory artifact (added: {:?})",
            after.keys().filter(|k| !before.contains_key(*k)).collect::<Vec<_>>()
        );
        assert_eq!(after, before, "GH#140: read-only/schema-only open must not mutate any existing file bytes");
    });
}
