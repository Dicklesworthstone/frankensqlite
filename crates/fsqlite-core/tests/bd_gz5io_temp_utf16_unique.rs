//! BINARY uniqueness compares native UTF-16 units. D800 0041 and D800 DC41
//! produce the same UTF-8 projection but are distinct stored keys.
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn utf16_unique_keeps_native_key_identity_on_temp_and_persistent_tables() {
    asupersync::test_utils::run_test(|| async {
        for (encoding, malformed, valid) in [
            ("UTF-16le", "00D84100", "00D841DC"),
            ("UTF-16be", "D8000041", "D800DC41"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let stock_path = dir.path().join("stock.db");
            let engine_path = dir.path().join("engine.db");
            // Stock stores both byte sequences before either engine exercises
            // UNIQUE, independent of the connection's SQL literal conversions.
            {
                let seed = rusqlite::Connection::open(&stock_path).unwrap();
                seed.execute_batch(&format!(
                    "PRAGMA encoding='{encoding}';
                     CREATE TABLE raw(id INTEGER PRIMARY KEY, k TEXT);
                     INSERT INTO raw VALUES
                       (1, CAST(x'{malformed}' AS TEXT)), (2, CAST(x'{valid}' AS TEXT));"
                ))
                .unwrap();
            }
            std::fs::copy(&stock_path, &engine_path).unwrap();
            let conn = Connection::open(engine_path.to_str().unwrap())
                .await
                .unwrap();
            let stock = rusqlite::Connection::open(stock_path).unwrap();

            for (name, schema) in [("t", "TEMP"), ("p", "")] {
                for (suffix, table_constraint) in [("declared", " UNIQUE"), ("indexed", "")] {
                    let name = format!("{name}_{suffix}");
                    let ddl = format!(
                        "CREATE {schema} TABLE {name}(id INTEGER PRIMARY KEY, k TEXT{table_constraint})"
                    );
                    let insert = |verb: &str, id: i64, source: i64| {
                        format!("{verb} INTO {name} SELECT {id}, k FROM raw WHERE id = {source}")
                    };
                    let mut steps = vec![ddl, insert("INSERT", 1, 1)];
                    if table_constraint.is_empty() {
                        // Building the index must retain both pre-existing keys.
                        steps.push(insert("INSERT", 2, 2));
                        steps.push(format!("CREATE UNIQUE INDEX i_{name} ON {name}(k)"));
                    }
                    steps.extend([
                        insert("INSERT OR REPLACE", 3, 2),
                        insert("INSERT", 4, 1), // genuine UNIQUE violation
                        format!("DELETE FROM {name} WHERE id=3"),
                        insert("INSERT", 5, 2), // distinct beside the malformed key
                        insert("INSERT OR REPLACE", 6, 1), // replace only the exact key
                    ]);
                    let query = format!("SELECT id, hex(CAST(k AS BLOB)) FROM {name} ORDER BY id");
                    for step in steps {
                        let expected = stock.execute_batch(&step);
                        let got = conn.execute(&step).await;
                        assert_eq!(
                            got.is_ok(),
                            expected.is_ok(),
                            "{encoding} {step}: stock={expected:?}, fsqlite={got:?}"
                        );
                        let mut stmt = stock.prepare(&query).unwrap();
                        let stock_rows = stmt
                            .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
                            .unwrap()
                            .collect::<rusqlite::Result<Vec<_>>>()
                            .unwrap();
                        let rows = conn.query(&query).await.unwrap();
                        let got_rows = rows
                            .iter()
                            .map(|row| {
                                let [SqliteValue::Integer(id), SqliteValue::Text(hex)] = row.values()
                                else {
                                    panic!("unexpected result row: {row:?}");
                                };
                                (*id, hex.to_string())
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(got_rows, stock_rows, "{encoding} after {step}");
                    }
                }
            }
        }
    });
}
