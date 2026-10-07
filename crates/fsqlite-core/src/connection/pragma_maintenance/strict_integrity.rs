//! Stored datatype validation for both integrity-check PRAGMAs.

use super::*;

impl Connection {
    pub(super) async fn integrity_check_strict_type_predicates(
        &self,
        table: &TableSchema,
    ) -> Result<Vec<(String, String)>> {
        if !table.strict {
            return Ok(Vec::new());
        }

        // Use the declared type, not its affinity: INT and INTEGER share an
        // affinity but SQLite distinguishes them in integrity diagnostics.
        // xinfo includes generated columns, which table_info would omit.
        // Qualifying main is essential when a TEMP table shadows this table.
        let metadata = self
            .query(&format!(
                "PRAGMA main.table_xinfo({})",
                quote_identifier(&table.name)
            ))
            .await?;
        if metadata.len() != table.columns.len() {
            return Err(FrankenError::Internal(format!(
                "incomplete STRICT column metadata for {}",
                table.name
            )));
        }
        let mut predicates = Vec::new();
        for row in metadata {
            let [_, SqliteValue::Text(column), SqliteValue::Text(declared_type), ..] = row.values()
            else {
                return Err(FrankenError::Internal(format!(
                    "invalid STRICT column metadata for {}",
                    table.name
                )));
            };
            let declared_type = declared_type.trim().to_ascii_uppercase();
            if let Some(predicate) = strict_type_predicate(column, &declared_type)? {
                predicates.push((
                    predicate,
                    format!("non-{declared_type} value in {}.{column}", table.name),
                ));
            }
        }
        Ok(predicates)
    }
}

fn strict_type_predicate(column: &str, declared_type: &str) -> Result<Option<String>> {
    let storage_classes = match declared_type {
        "INT" | "INTEGER" => "'null', 'integer'",
        // An integral REAL may use an integer serial type on disk. Neither
        // that encoding nor a NULL in a nullable column is corruption.
        "REAL" => "'null', 'integer', 'real'",
        "TEXT" => "'null', 'text'",
        "BLOB" => "'null', 'blob'",
        "ANY" => return Ok(None),
        _ => {
            return Err(FrankenError::Internal(format!(
                "invalid STRICT datatype {declared_type} for column {column}"
            )));
        }
    };
    // Do not CAST the value: a numeric-looking TEXT value is still a stored
    // datatype violation. NOT NULL is checked separately by the caller.
    Ok(Some(format!(
        "typeof({}) NOT IN ({storage_classes})",
        quote_identifier(column)
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_reports(path: &std::path::Path, sql: &str) -> Vec<String> {
        let conn = rusqlite::Connection::open(path).unwrap();
        let mut statement = conn.prepare(sql).unwrap();
        let mut reports = statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        reports.sort();
        reports
    }

    fn messages(rows: &[Row]) -> Vec<String> {
        let mut reports = rows
            .iter()
            .map(|row| match &row.values()[0] {
                SqliteValue::Text(text) => text.to_string(),
                other => panic!("non-text integrity diagnostic: {other:?}"),
            })
            .collect::<Vec<_>>();
        reports.sort();
        reports
    }

    fn write_strict_fixture(path: &std::path::Path) {
        let stock = rusqlite::Connection::open(path).unwrap();
        // Populate an ordinary table first, then change only its schema.
        // Reopening gives both engines identical, structurally sound B-trees
        // containing values that a STRICT INSERT would have rejected.
        stock
            .execute_batch(
                "CREATE TABLE typed(id INTEGER PRIMARY KEY, i INT, j INTEGER,
                     r REAL, t TEXT, b BLOB, a ANY);
                 INSERT INTO typed VALUES(1, 'bad', 'bad', 'bad', x'31', 42, 'anything');
                 INSERT INTO typed VALUES(2, NULL, NULL, NULL, NULL, NULL, NULL);
                 INSERT INTO typed VALUES(3, 1, 2, 3, 'text', x'00', x'ff');
                 CREATE TABLE dynamic_types(i INT, r REAL, t TEXT, b BLOB);
                 INSERT INTO dynamic_types VALUES('bad', 'bad', x'31', 42);
                 CREATE TABLE clean_types(i INT, r REAL, t TEXT, b BLOB, a ANY) STRICT;
                 INSERT INTO clean_types VALUES(NULL, NULL, NULL, NULL, NULL);
                 INSERT INTO clean_types VALUES(1, 2, 'text', x'00', '000123');
                 INSERT INTO clean_types VALUES(2, 2.5, '', x'', x'ff');
                 PRAGMA writable_schema=ON;
                 UPDATE sqlite_schema SET sql=sql || ' STRICT' WHERE name='typed';",
            )
            .unwrap();
    }

    #[test]
    fn strict_integrity_reports_stored_datatypes_like_stock() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("strict-types.db");
        write_strict_fixture(&path);
        let cases = [
            "PRAGMA integrity_check",
            "PRAGMA quick_check",
            "PRAGMA main.integrity_check('typed')",
            "PRAGMA quick_check(typed)",
            "PRAGMA integrity_check(clean_types)",
            "PRAGMA quick_check(dynamic_types)",
            "PRAGMA integrity_check(1)",
            "PRAGMA quick_check(3)",
        ];
        let expected = cases
            .iter()
            .map(|sql| reference_reports(&path, sql))
            .collect::<Vec<_>>();
        assert_eq!(expected[0].len(), 5, "fixture must contain five bad types");
        assert!(expected[0].contains(&"non-INT value in typed.i".to_owned()));
        assert!(expected[0].contains(&"non-INTEGER value in typed.j".to_owned()));
        assert_eq!(expected[4], ["ok"]);
        assert_eq!(expected[5], ["ok"]);

        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            for prepared in [false, true] {
                for (sql, expected) in cases.iter().zip(&expected) {
                    let rows = if prepared {
                        conn.prepare(sql).await.unwrap().query().await.unwrap()
                    } else {
                        conn.query(sql).await.unwrap()
                    };
                    assert_eq!(messages(&rows), *expected, "{sql}, prepared={prepared}");
                }
            }
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn strict_integrity_checks_attachments_with_one_budget() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("strict-aux.db");
        write_strict_fixture(&path);
        let expected = reference_reports(&path, "PRAGMA integrity_check");
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(&format!(
                "ATTACH '{}' AS aux",
                path.to_string_lossy().replace('\'', "''")
            ))
            .await
            .unwrap();
            for pragma in ["integrity_check", "quick_check"] {
                for schema in ["", "aux."] {
                    let sql = format!("PRAGMA {schema}{pragma}");
                    assert_eq!(messages(&conn.query(&sql).await.unwrap()), expected);
                }
                let main = conn.query(&format!("PRAGMA main.{pragma}")).await.unwrap();
                assert_eq!(messages(&main), ["ok"]);
                let limited = conn.query(&format!("PRAGMA {pragma}(2)")).await.unwrap();
                assert_eq!(limited.len(), 2);
                assert!(messages(&limited).iter().all(|report| expected.contains(report)));
            }
            conn.execute("DETACH aux").await.unwrap();
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn strict_storage_predicates_preserve_null_any_and_real_encodings() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE sample(\"a\"\"b\" BLOB)").unwrap();
        for declared_type in ["INT", "INTEGER", "REAL", "TEXT", "BLOB", "ANY"] {
            let predicate = strict_type_predicate("a\"b", declared_type).unwrap();
            if declared_type == "ANY" {
                assert!(predicate.is_none());
                continue;
            }
            let sql = format!("SELECT {} FROM sample", predicate.unwrap());
            for (literal, storage) in [
                ("NULL", "null"),
                ("1", "integer"),
                ("1.5", "real"),
                ("'1'", "text"),
                ("x'31'", "blob"),
            ] {
                conn.execute_batch(&format!("DELETE FROM sample; INSERT INTO sample VALUES({literal})"))
                    .unwrap();
                let invalid: bool = conn.query_row(&sql, [], |row| row.get(0)).unwrap();
                let valid = storage == "null"
                    || match declared_type {
                        "INT" | "INTEGER" => storage == "integer",
                        "REAL" => matches!(storage, "integer" | "real"),
                        "TEXT" => storage == "text",
                        "BLOB" => storage == "blob",
                        _ => unreachable!(),
                    };
                assert_eq!(invalid, !valid, "{declared_type}: {literal}");
            }
        }
        assert!(strict_type_predicate("a", "NUMERIC").is_err());
    }

    #[test]
    fn integrity_scan_does_not_assume_stored_not_null_constraints_hold() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("stored-nulls.db");
        {
            let stock = rusqlite::Connection::open(&path).unwrap();
            stock
                .execute_batch(
                    "CREATE TABLE nullable(id INTEGER PRIMARY KEY, a TEXT, b INT);
                     INSERT INTO nullable VALUES(1, NULL, -1),(2, 'valid', NULL),(3, NULL, NULL);
                     CREATE INDEX nullable_cover ON nullable(a, b);
                     PRAGMA writable_schema=ON;
                     UPDATE sqlite_schema SET sql='CREATE TABLE nullable(
                         id INTEGER PRIMARY KEY, a TEXT NOT NULL, b INT NOT NULL)'
                         WHERE name='nullable';",
                )
                .unwrap();
        }
        let expected = reference_reports(&path, "PRAGMA integrity_check");
        assert_eq!(expected.len(), 4, "fixture must contain four stored NULLs");
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            for prepared in [false, true] {
                for pragma in ["integrity_check", "quick_check"] {
                    let sql = format!("PRAGMA {pragma}");
                    let rows = if prepared {
                        conn.prepare(&sql).await.unwrap().query().await.unwrap()
                    } else {
                        conn.query(&sql).await.unwrap()
                    };
                    assert_eq!(messages(&rows), expected, "{sql}, prepared={prepared}");
                    let limited = conn.query(&format!("PRAGMA {pragma}(2)")).await.unwrap();
                    assert_eq!(limited.len(), 2);
                    assert!(messages(&limited).iter().all(|report| expected.contains(report)));
                }
            }
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn strict_integrity_enforces_implicit_primary_key_not_null() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("strict-primary-key.db");
        {
            let stock = rusqlite::Connection::open(&path).unwrap();
            stock
                .execute_batch(
                    "CREATE TABLE composite_key(a TEXT, b INT, PRIMARY KEY(a, b));
                     INSERT INTO composite_key VALUES(NULL, 1),('key', NULL);
                     CREATE TABLE inline_key(a TEXT PRIMARY KEY);
                     INSERT INTO inline_key VALUES(NULL);
                     CREATE TABLE ordinary_key(a TEXT PRIMARY KEY);
                     INSERT INTO ordinary_key VALUES(NULL);
                     PRAGMA writable_schema=ON;
                     UPDATE sqlite_schema SET sql=sql || ' STRICT'
                         WHERE name IN ('composite_key', 'inline_key');",
                )
                .unwrap();
        }
        let expected = reference_reports(&path, "PRAGMA integrity_check");
        assert_eq!(expected.len(), 3, "only STRICT primary keys forbid these NULLs");
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            for pragma in ["integrity_check", "quick_check"] {
                let rows = conn.query(&format!("PRAGMA {pragma}")).await.unwrap();
                assert_eq!(messages(&rows), expected);
                let ordinary = conn
                    .query(&format!("PRAGMA {pragma}(ordinary_key)"))
                    .await
                    .unwrap();
                assert_eq!(messages(&ordinary), ["ok"]);
            }
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn strict_integrity_checks_generated_without_rowid_and_shadowed_columns() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("strict-layouts.db");
        {
            let stock = rusqlite::Connection::open(&path).unwrap();
            stock
                .execute_batch(
                    r#"CREATE TABLE generated(v INT, s INT AS (v) STORED, g INT AS (v) VIRTUAL);
                       INSERT INTO generated(v) VALUES(x'31'),(NULL),(42);
                       CREATE TABLE compact(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
                       INSERT INTO compact VALUES('key', x'31');
                       CREATE TABLE "odd' table"("a""b" TEXT);
                       INSERT INTO "odd' table" VALUES(x'31');
                       PRAGMA writable_schema=ON;
                       UPDATE sqlite_schema SET sql=sql || ' STRICT'
                           WHERE name IN ('generated', 'odd'' table');
                       UPDATE sqlite_schema SET sql=sql || ', STRICT' WHERE name='compact';"#,
                )
                .unwrap();
        }
        let expected = reference_reports(&path, "PRAGMA integrity_check");
        assert_eq!(expected.len(), 5, "fixture covers all three stored layouts");
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            // The MAIN table and its xinfo must remain visible to the checker.
            conn.execute("CREATE TEMP TABLE generated(unrelated TEXT)")
                .await
                .unwrap();
            for pragma in ["integrity_check", "quick_check"] {
                let rows = conn.query(&format!("PRAGMA main.{pragma}")).await.unwrap();
                assert_eq!(messages(&rows), expected);
            }
            conn.execute("DROP TABLE temp.generated").await.unwrap();
            conn.close().await.unwrap();
        });
    }
}
