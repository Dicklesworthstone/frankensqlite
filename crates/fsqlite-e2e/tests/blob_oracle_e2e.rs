//! bd-kyvrd — Oracle-parity e2e: BLOB I/O & semantics vs rusqlite (real SQLite).
//!
//! Covers blob literals `X'..'`, zeroblob(n), storage round-trip, hex/length/
//! substr/quote on blobs, the storage-class sort order (blobs sort after text),
//! byte-wise blob comparison, blobs in WHERE / DISTINCT / GROUP BY, CAST to/from
//! blob, the empty blob, and `||` concatenation involving blobs. All inputs are
//! fixed and deterministic; outputs render as X'..' hex on both sides.
#![recursion_limit = "512"]

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

async fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    check(&f, &r, queries, label).await;
}

async fn assert_text_bytes(
    f: &Connection,
    r: &rusqlite::Connection,
    expression: &str,
    expected_hex: &str,
) {
    // Inspect through hex() so neither Rust driver's UTF-8 conversion can
    // conceal a changed byte. Pin the type too: returning BLOB is not a fix.
    let sql = format!("SELECT typeof({expression}), hex({expression})");
    let expected = vec![vec!["'text'".to_owned(), format!("'{expected_hex}'")]];
    assert_eq!(
        sqlite_rows(r, &sql).unwrap_or_else(|error| panic!("stock `{sql}`: {error}")),
        expected,
        "stock oracle changed: {sql}"
    );
    assert_eq!(
        frank_rows(f, &sql)
            .await
            .unwrap_or_else(|error| panic!("frank `{sql}`: {error}")),
        expected,
        "raw TEXT bytes differ: {sql}"
    );
}

#[test]
fn blob_literals_and_functions() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT X'48656C6C6F'",               // 'Hello' bytes
                "SELECT typeof(X'00FF')",             // 'blob'
                "SELECT length(X'00010203')",         // 4
                "SELECT hex(X'DEADBEEF')",            // 'DEADBEEF'
                "SELECT quote(X'4142')",              // X'4142'
                "SELECT X''",                         // empty blob
                "SELECT length(X'')",                 // 0
                "SELECT substr(X'0102030405', 2, 2)", // X'0203'
                "SELECT hex(zeroblob(3))",            // '000000'
                "SELECT length(zeroblob(5))",         // 5
            ],
            "blob_literals_and_functions",
        )
        .await;
    });
}

#[test]
fn blob_storage_roundtrip() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)",
            "INSERT INTO t VALUES (1, X'01020304'),(2, X''),(3, X'FF'),(4, NULL)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT id, data, typeof(data), length(data) FROM t ORDER BY id",
                "SELECT id FROM t WHERE data = X'FF'",
                "SELECT id FROM t WHERE data IS NULL",
                "SELECT hex(data) FROM t WHERE id = 1",
            ],
            "blob_storage_roundtrip",
        )
        .await;
    });
}

#[test]
fn blob_ordering_storage_class() {
    // Storage-class order: NULL < numbers < text < blob. Among blobs, memcmp.
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v)",
            "INSERT INTO t(v) VALUES (NULL),(42),('text'),(X'00'),(X'FF'),(X'0102'),(3.5)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT typeof(v), v FROM t ORDER BY v, id",
                "SELECT typeof(v) FROM t ORDER BY v DESC, id",
                // Blob byte-wise comparison.
                "SELECT X'01' < X'02'",
                "SELECT X'0102' < X'02'",     // first byte 01 < 02
                "SELECT X'0102' < X'010203'", // prefix < longer
                "SELECT X'41' = X'41'",
            ],
            "blob_ordering_storage_class",
        )
        .await;
    });
}

#[test]
fn blob_distinct_and_group_by() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, b BLOB)",
            "INSERT INTO t VALUES (1,X'01'),(2,X'01'),(3,X'02'),(4,X'02'),(5,X'03')",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT DISTINCT b FROM t ORDER BY b",
                "SELECT count(DISTINCT b) FROM t",
                "SELECT b, count(*) FROM t GROUP BY b ORDER BY b",
            ],
            "blob_distinct_and_group_by",
        )
        .await;
    });
}

#[test]
fn blob_cast() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                // CAST text to blob keeps the bytes; hex shows them.
                "SELECT hex(CAST('AB' AS BLOB))",   // '4142'
                "SELECT typeof(CAST('x' AS BLOB))", // 'blob'
                // CAST a blob to text interprets bytes as the string.
                "SELECT CAST(X'414243' AS TEXT)", // 'ABC'
                // CAST a blob of digits to integer (via text).
                "SELECT CAST(X'313233' AS INTEGER)", // 123
                "SELECT CAST(X'312E35' AS REAL)",    // 1.5
                // CAST integer to blob.
                "SELECT typeof(CAST(65 AS BLOB)), hex(CAST(65 AS BLOB))",
            ],
            "blob_cast",
        )
        .await;
    });
}

#[test]
fn blob_concatenation() {
    // `||` semantics with blob operands (let the oracle define the result).
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT typeof(X'41' || X'42'), hex(CAST(X'41' || X'42' AS BLOB))",
                "SELECT X'41' || X'42'",
                "SELECT 'pre' || X'4142'",
                "SELECT typeof(X'00' || 'x')",
                // Bytes that are not valid UTF-8 must survive `||` unchanged
                // (a lossy decode yields EFBFBD replacement sequences).
                "SELECT hex(X'00FF' || X'AA'), typeof(X'00FF' || X'AA')",
                "SELECT hex(X'C0FFEE' || 'x'), hex('a' || X'FF' || 'b')",
                "SELECT hex(X'80' || 1), hex(2.5 || X'FE')",
                "SELECT hex(CAST(X'FF' AS TEXT) || CAST(X'FE' AS TEXT))",
                "SELECT X'FF' || NULL IS NULL",
            ],
            "blob_concatenation",
        )
        .await;
    });
}

#[test]
fn string_functions_keep_raw_bytes() {
    // bd-w4pr1.18: functions stock treats as byte-transparent must not turn a
    // BLOB or non-UTF-8 TEXT byte into EF BF BD.
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT hex(replace(X'41FF42', 'A', 'Z')), hex(replace(CAST(X'41FF' AS TEXT), X'FF', 'q'))",
                "SELECT hex(trim(X'20FF20')), hex(ltrim(X'FFFE41', 'A')), hex(rtrim(CAST(X'41FF20' AS TEXT)))",
                "SELECT hex(trim(CAST(X'C3FFC3A9' AS TEXT), 'é'))",
                "SELECT hex(upper(X'61FF')), hex(lower(X'41FF'))",
                "SELECT hex(concat(X'FF', 'a')), hex(concat_ws('-', X'FF', X'FE')), hex(concat(X'FF', NULL, 'z'))",
                "SELECT hex(quote(CAST(X'FF' AS TEXT))), hex(quote(CAST(X'6127FF' AS TEXT)))",
                "SELECT hex(group_concat(v)) FROM (SELECT X'FF' AS v UNION ALL SELECT X'FE')",
                "SELECT hex(string_agg(v, X'00')) FROM (SELECT X'C0' AS v UNION ALL SELECT X'C1')",
                "SELECT hex(upper('abc')), trim('  x  '), replace('aXa', 'X', 'y')",
            ],
            "string_functions_keep_raw_bytes",
        )
        .await;
    });
}

#[test]
fn format_string_conversions_preserve_raw_bytes() {
    // bd-w4pr1.18: exercise both SQL entry points and both raw-value storage
    // classes. Ordinary precision may split a UTF-8 code point; ! precision
    // and width count SQLite characters instead of bytes.
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[]).await;
        let cases = [
            ("%s", "FF00FE", "FF"),
            ("%z", "C000FE", "C0"),
            ("%q", "27FF00", "2727FF"),
            ("%Q", "27FF00", "272727FF27"),
            ("%w", "22FF00", "2222FF"),
            ("%.1s", "C3A9", "C3"),
            ("%!.1s", "C3A9", "C3A9"),
            ("%3s", "C3A9", "20C3A9"),
            ("%!3s", "C3A9", "2020C3A9"),
            ("%4.1s", "C3A9", "202020C3"),
            ("%!4.1s", "C3A9", "202020C3A9"),
            ("%!-4.1s", "C3A9", "C3A9202020"),
            ("%.1q", "C3A927", "C3"),
            ("%!.1q", "C3A927", "C3A9"),
            ("%4.1Q", "C3A927", "2027C327"),
            ("%!4.1Q", "C3A927", "2027C3A927"),
            ("%.1w", "C3A922", "C3"),
            ("%!.1w", "C3A922", "C3A9"),
            ("%6q", "C3A927", "2020C3A92727"),
            ("%!6q", "C3A927", "202020C3A92727"),
            ("%!.1s", "C0AF80FF", "C0AF80"),
            ("%!.1s", "80BF41", "80"),
            ("%!.1s", "F88880808041", "F888808080"),
            (
                "%#Q",
                "FF01275C",
                "756E697374722827FF5C753030303127275C5C2729",
            ),
            ("%#q", "FF01275C", "FF5C753030303127275C5C"),
            ("%#Q", "FF5C", "27FF5C27"),
            ("%#q", "FF5C", "FF5C5C"),
            ("%#w", "FF01225C", "FF0122225C"),
        ];
        for name in ["printf", "format"] {
            for (format, bytes, expected) in cases {
                for value in [format!("X'{bytes}'"), format!("CAST(X'{bytes}' AS TEXT)")] {
                    let expression = format!("{name}('{format}', {value})");
                    assert_text_bytes(&f, &r, &expression, expected).await;
                }
            }
            // NULL's printable placeholder is still subject to precision,
            // but %Q must not surround that placeholder with quotes.
            for (format, expected) in [
                ("%.0q", ""),
                ("%.1q", "28"),
                ("%.1Q", "4E"),
                ("%5.2Q", "2020204E55"),
                ("%#.0w", ""),
                ("%#!.1Q", "4E"),
            ] {
                assert_text_bytes(&f, &r, &format!("{name}('{format}', NULL)"), expected).await;
            }
        }
    });
}

#[test]
fn format_raw_format_strings_and_nul_termination() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[]).await;
        for name in ["printf", "format"] {
            for (bytes, arguments, expected) in [
                ("FF2573", "'a'", "FF61"),
                ("FF2525FE", "'unused'", "FF25FE"),
                ("2564002564", "1, 2", "31"),
                ("FF002573", "'ignored'", "FF"),
            ] {
                for format in [format!("X'{bytes}'"), format!("CAST(X'{bytes}' AS TEXT)")] {
                    let expression = format!("{name}({format}, {arguments})");
                    assert_text_bytes(&f, &r, &expression, expected).await;
                }
            }
            for format in ["X''", "X'002573'", "CAST(X'002573' AS TEXT)", "NULL"] {
                let sql = format!("SELECT {name}({format}, 'unused') IS NULL");
                let expected = vec![vec!["1".to_owned()]];
                assert_eq!(sqlite_rows(&r, &sql).expect("stock empty format"), expected);
                assert_eq!(
                    frank_rows(&f, &sql).await.expect("frank empty format"),
                    expected,
                    "empty format must produce NULL: {sql}"
                );
            }
            assert_text_bytes(&f, &r, &format!("{name}('%s', NULL)"), "").await;
            assert_text_bytes(&f, &r, &format!("{name}('%s', '')"), "").await;
        }
    });
}

#[test]
fn format_character_conversion_preserves_sqlite_byte_boundaries() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[]).await;
        for name in ["printf", "format"] {
            for (format, bytes, expected) in [
                ("%c", "FF00", "FF"),
                ("%c", "4180", "41"),
                ("%c", "8080", "80"),
                // Stock writes all but the last repeated character before
                // applying its UTF-8 width adjustment. A stray continuation
                // byte can therefore put padding before the final repeat.
                ("%3c", "80", "20202080"),
                ("%3.2c", "80", "208080"),
                ("%3.3c", "80", "80802080"),
                ("%-3.3c", "80", "80808020"),
                ("%c", "C080FF", "C080"),
                ("%c", "F88880808041", "F8888080"),
                ("%.3c", "C080FF", "C080C080C080"),
                ("%5.3c", "C080FF", "2020C080C080C080"),
                ("%c", "00FF", "00"),
                ("%.2c", "00FF", "0000"),
            ] {
                for value in [format!("X'{bytes}'"), format!("CAST(X'{bytes}' AS TEXT)")] {
                    let expression = format!("{name}('{format}', {value})");
                    assert_text_bytes(&f, &r, &expression, expected).await;
                }
            }
            for value in ["NULL", "''", "X''"] {
                assert_text_bytes(&f, &r, &format!("{name}('%c', {value})"), "00").await;
                assert_text_bytes(&f, &r, &format!("{name}('%3.2c', {value})"), "200000").await;
            }
        }
    });
}

#[test]
fn unistr_quote_preserves_raw_text_and_control_escapes() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[]).await;
        for (value, expected) in [
            ("CAST(X'FF' AS TEXT)", "27FF27"),
            ("CAST(X'FF5C' AS TEXT)", "27FF5C27"),
            (
                "CAST(X'FF01275C' AS TEXT)",
                "756E697374722827FF5C753030303127275C5C2729",
            ),
            ("CAST(X'FF005C01' AS TEXT)", "27FF27"),
            ("CAST(X'00FF01' AS TEXT)", "2727"),
            ("X'FF01275C'", "5827464630313237354327"),
            ("NULL", "4E554C4C"),
            ("123", "313233"),
        ] {
            assert_text_bytes(&f, &r, &format!("unistr_quote({value})"), expected).await;
        }
    });
}

#[test]
fn formatted_raw_text_survives_storage_and_reopen() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temporary raw TEXT database");
        let path = dir.path().join("formatted.sqlite");
        let path_str = path.to_str().expect("UTF-8 database path");
        let f = Connection::open(path_str).await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open stock");
        let setup = [
            "CREATE TABLE formatted(id INTEGER PRIMARY KEY, value TEXT)",
            "INSERT INTO formatted VALUES (1, printf('%s', X'FF00FE'))",
            "INSERT INTO formatted VALUES (2, format('%.1s', 'é'))",
            "INSERT INTO formatted VALUES (3, printf('%Q', CAST(X'27FF00' AS TEXT)))",
            "INSERT INTO formatted VALUES (4, format('%c', NULL))",
            "INSERT INTO formatted VALUES (5, unistr_quote(CAST(X'FF01275C' AS TEXT)))",
            "INSERT INTO formatted VALUES (6, printf(CAST(X'FF2573' AS TEXT), 'a'))",
        ];
        for sql in setup {
            f.execute(sql).await.expect("store frank formatted text");
            r.execute_batch(sql).expect("store stock formatted text");
        }
        let query = "SELECT id, typeof(value), hex(value) FROM formatted ORDER BY id";
        let expected = sqlite_rows(&r, query).expect("stock formatted rows");
        assert_eq!(
            frank_rows(&f, query).await.expect("frank formatted rows"),
            expected
        );
        f.close().await.expect("close frank formatted database");

        let reopened = Connection::open(path_str).await.expect("reopen frank");
        assert_eq!(
            frank_rows(&reopened, query)
                .await
                .expect("reopened frank formatted rows"),
            expected
        );
        reopened.close().await.expect("close reopened frank");

        let stock_reader = rusqlite::Connection::open(&path).expect("stock opens frank file");
        assert_eq!(
            sqlite_rows(&stock_reader, query).expect("stock reads frank formatted rows"),
            expected
        );
        assert_eq!(
            sqlite_rows(&stock_reader, "PRAGMA integrity_check").expect("stock integrity check"),
            vec![vec!["'ok'".to_owned()]]
        );
    });
}

#[test]
fn blob_concatenation_stored_value_keeps_bytes() {
    // The concatenated TEXT is written to a table and read back.
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE b(v)",
            "INSERT INTO b VALUES (X'C0FFEE' || X'00'), ('k' || X'FF')",
        ])
        .await;
        check(
            &f,
            &r,
            &["SELECT hex(v), typeof(v) FROM b ORDER BY rowid"],
            "blob_concatenation_stored_value_keeps_bytes",
        )
        .await;
    });
}
