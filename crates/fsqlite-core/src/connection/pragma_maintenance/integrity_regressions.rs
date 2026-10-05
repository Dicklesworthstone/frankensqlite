use super::*;

fn stock_reports(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let mut statement = conn.prepare(sql).unwrap();
    let mut reports = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    reports.sort();
    reports
}

async fn native_reports(conn: &Connection, sql: &str, prepared: bool) -> Vec<String> {
    let rows = if prepared {
        conn.prepare(sql).await.unwrap().query().await.unwrap()
    } else {
        conn.query(sql).await.unwrap()
    };
    let mut reports = rows
        .iter()
        .map(|row| match &row.values()[0] {
            SqliteValue::Text(text) => text.to_string(),
            other => panic!("{sql}: non-text integrity diagnostic {other:?}"),
        })
        .collect::<Vec<_>>();
    reports.sort();
    reports
}

/// Real stored violations: the bytes precede the constraints. Never substitute
/// mocked validation results or rely on the native write path admitting damage.
fn create_constraint_fixture(path: &std::path::Path) {
    let stock = rusqlite::Connection::open(path).unwrap();
    stock
        .execute_batch(
            "CREATE TABLE broken(id INTEGER PRIMARY KEY, value TEXT, n INT);
             INSERT INTO broken VALUES(1,NULL,-1),(2,'present',-2);
             CREATE TABLE clean(value INT NOT NULL CHECK(value>0));
             INSERT INTO clean VALUES(1);
             PRAGMA writable_schema=ON;
             UPDATE sqlite_schema SET sql='CREATE TABLE broken(id INTEGER PRIMARY KEY, \
                 value TEXT NOT NULL, n INT CHECK(n>0))' WHERE name='broken';",
        )
        .unwrap();
}

fn attach_sql(path: &std::path::Path, schema: &str) -> String {
    format!(
        "ATTACH DATABASE '{}' AS {};",
        path.to_string_lossy().replace('\'', "''"),
        quote_identifier(schema)
    )
}

#[test]
fn attached_row_constraint_checks_match_stock_direct_prepared_and_error_budgets() {
    let dir = tempfile::tempdir().unwrap();
    let paths = [dir.path().join("first.db"), dir.path().join("second.db")];
    for path in &paths {
        create_constraint_fixture(path);
    }
    let mut cases = Vec::new();
    {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        stock
            .execute_batch("CREATE TABLE clean(value INT NOT NULL CHECK(value>0));")
            .unwrap();
        for (schema, path) in ["first", "second"].iter().zip(&paths) {
            stock.execute_batch(&attach_sql(path, schema)).unwrap();
        }
        for pragma in ["integrity_check", "quick_check"] {
            for scope in ["", "main.", "first.", "second."] {
                for argument in ["", "(1)", "(2)", "(4)", "(0)", "(-1)", "(+1)"] {
                    let sql = format!("PRAGMA {scope}{pragma}{argument};");
                    let expected = stock_reports(&stock, &sql);
                    if scope.is_empty() && argument.is_empty() {
                        assert_eq!(expected.len(), 6, "fixture must expose both attachments");
                        assert!(expected.iter().any(|row| row.starts_with("NULL value")));
                        assert!(expected.iter().any(|row| row.starts_with("CHECK constraint")));
                    }
                    cases.push((sql, expected));
                }
            }
            for sql in [
                format!("PRAGMA {pragma}(clean);"),
                format!("PRAGMA first.{pragma}(clean);"),
                format!("PRAGMA second.{pragma}('broken');"),
            ] {
                let expected = stock_reports(&stock, &sql);
                cases.push((sql, expected));
            }
        }
    }
    // The stock handles are closed before native opens; the two engines never
    // concurrently own the same file in this process.
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE clean(value INT NOT NULL CHECK(value>0));")
            .await
            .unwrap();
        for (schema, path) in ["first", "second"].iter().zip(&paths) {
            conn.execute(&attach_sql(path, schema)).await.unwrap();
        }
        for prepared in [false, true] {
            for (sql, expected) in &cases {
                assert_eq!(
                    &native_reports(&conn, sql, prepared).await,
                    expected,
                    "{sql}, prepared={prepared}"
                );
            }
        }
        conn.close().await.unwrap();
    });
}

#[test]
fn main_findings_consume_the_attached_row_constraint_budget() {
    let dir = tempfile::tempdir().unwrap();
    let main = dir.path().join("main.db");
    let aux = dir.path().join("aux.db");
    create_constraint_fixture(&main);
    create_constraint_fixture(&aux);
    let mut cases = Vec::new();
    {
        let stock = rusqlite::Connection::open(&main).unwrap();
        stock.execute_batch(&attach_sql(&aux, "aux")).unwrap();
        for pragma in ["integrity_check", "quick_check"] {
            for limit in [1, 2, 3, 4, 5, 6, 7] {
                let sql = format!("PRAGMA {pragma}({limit});");
                cases.push((sql.clone(), stock_reports(&stock, &sql)));
            }
        }
    }
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(main.to_str().unwrap()).await.unwrap();
        conn.execute(&attach_sql(&aux, "aux")).await.unwrap();
        for prepared in [false, true] {
            for (sql, expected) in &cases {
                assert_eq!(
                    &native_reports(&conn, sql, prepared).await,
                    expected,
                    "{sql}, prepared={prepared}"
                );
            }
        }
        conn.close().await.unwrap();
    });
}
