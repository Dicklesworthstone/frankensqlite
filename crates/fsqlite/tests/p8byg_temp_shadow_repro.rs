//! bd-p8byg pinpoint repro: run the GH#144 main-qualified-DML-vs-TEMP-shadow
//! statements one at a time and report exactly which one frank rejects (the
//! full conformance test just `.unwrap()`s the first failure). NOT a keeper —
//! a diagnostic to localize the PRIMARY KEY constraint failure.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;

#[test]
#[ignore = "bd-p8byg diagnostic (not a keeper): pinpoints that unqualified INSERT INTO t (TEMP-shadowed temp.t with non-leading temp_id INTEGER PRIMARY KEY + decoy rowid/_rowid_/oid columns) mis-resolves the rowid alias -> PK collision. Qualified temp.t DML works. Fix is TEMP-shadow name resolution (connection.rs, GH#144/bd-yuj70 family)."]
fn p8byg_pinpoint_failing_statement() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let setup = [
            "PRAGMA foreign_keys=ON",
            "CREATE TABLE main.t(id INTEGER PRIMARY KEY, value TEXT)",
            "INSERT INTO main.t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            "CREATE TEMP TABLE t(rowid TEXT, _rowid_ TEXT, oid TEXT, temp_id INTEGER PRIMARY KEY, temp_value TEXT)",
            "INSERT INTO temp.t VALUES (NULL, NULL, NULL, 9, 'temp')",
        ];
        let mutations = [
            "INSERT INTO main.t VALUES (4, 'four')",
            "UPDATE main.t SET value = 'ONE' WHERE id = 1",
            "DELETE FROM main.t WHERE id = 2",
            "INSERT INTO temp.t VALUES (NULL, NULL, NULL, 10, 'temporary')",
            "UPDATE temp.t SET temp_value = 'TEMP' WHERE temp_id = 9",
            "DELETE FROM temp.t WHERE temp_id = 10",
            "INSERT INTO t VALUES (NULL, NULL, NULL, 11, 'unqualified')",
            "UPDATE t SET temp_value = 'UNQUALIFIED' WHERE temp_id = 11",
            "DELETE FROM t WHERE temp_id = 11",
            "INSERT INTO main.t VALUES (5, 'five')",
            "UPDATE main.t SET value = 'FOUR' WHERE id = 4",
            "DELETE FROM main.t WHERE id = 3",
            "INSERT INTO temp.t VALUES (NULL, NULL, NULL, 12, 'prepared')",
            "UPDATE temp.t SET temp_value = 'PREPARED' WHERE temp_id = 12",
            "DELETE FROM temp.t WHERE temp_id = 12",
        ];
        for s in setup {
            let r = f.execute(s).await;
            println!(
                "SETUP  {:<70} => {:?}",
                s,
                r.as_ref().map(|_| "ok").map_err(|e| e.to_string())
            );
            r.unwrap_or_else(|e| panic!("setup failed: `{s}`: {e}"));
        }
        let render = |v: &SqliteValue| match v {
            SqliteValue::Null => "NULL".to_owned(),
            SqliteValue::Integer(n) => n.to_string(),
            SqliteValue::Text(t) => format!("'{t}'"),
            other => format!("{other:?}"),
        };
        // Probe: which column is the rowid alias? `rowid`/`_rowid_`/`oid` are
        // DECOY regular TEXT columns; the real rowid alias is `temp_id` (INTEGER
        // PRIMARY KEY). Stock: `SELECT rowid, temp_id FROM temp.t` => 9|9.
        for q in [
            "SELECT rowid, temp_id, typeof(rowid) FROM temp.t",
            "SELECT _rowid_, oid FROM temp.t",
        ] {
            let rows: Vec<String> = f
                .query(q)
                .await
                .map(|rs| {
                    rs.iter()
                        .map(|row| {
                            row.values()
                                .iter()
                                .map(render)
                                .collect::<Vec<_>>()
                                .join(",")
                        })
                        .collect()
                })
                .unwrap_or_else(|e| vec![format!("<err: {e}>")]);
            println!("PROBE {q} => {rows:?}");
        }
        for s in mutations {
            let r = f.execute(s).await;
            match &r {
                Ok(_) => println!("MUT ok  {s}"),
                Err(e) => {
                    println!("MUT ERR {s}\n   => {e}");
                    // dump both tables to see routing
                    for q in [
                        "SELECT id, value FROM main.t ORDER BY id",
                        "SELECT temp_id, temp_value FROM temp.t ORDER BY temp_id",
                    ] {
                        let rows: Vec<String> = f
                            .query(q)
                            .await
                            .map(|rs| {
                                rs.iter()
                                    .map(|row| {
                                        row.values()
                                            .iter()
                                            .map(render)
                                            .collect::<Vec<_>>()
                                            .join(",")
                                    })
                                    .collect()
                            })
                            .unwrap_or_else(|e| vec![format!("<query err: {e}>")]);
                        println!("   {q} => {rows:?}");
                    }
                    panic!("FIRST FAILING STATEMENT: `{s}` -> {e}");
                }
            }
        }
        println!("ALL MUTATIONS SUCCEEDED (no divergence in error behavior)");
    });
}
