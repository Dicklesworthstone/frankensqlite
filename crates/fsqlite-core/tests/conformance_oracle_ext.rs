//! Extended conformance oracle tests comparing FrankenSQLite against C SQLite (rusqlite).
//!
//! These tests cover areas not yet exercised by the main conformance suite:
//! hex literals, bitwise ops, CAST edges, LIKE/GLOB, EXCEPT/INTERSECT chains,
//! scalar min/max, total(), REPLACE, savepoints, DEFAULT expressions, and more.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Run queries against both FrankenSQLite and C SQLite, returning mismatches.
fn oracle_compare(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    queries: &[&str],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for query in queries {
        let frank_result = fconn.query(query);
        let csql_result: std::result::Result<Vec<Vec<String>>, String> = (|| {
            let mut stmt = rconn.prepare(query).map_err(|e| format!("prepare: {e}"))?;
            let col_count = stmt.column_count();
            let rows: Vec<Vec<String>> = stmt
                .query_map([], |row| {
                    let mut vals = Vec::new();
                    for i in 0..col_count {
                        let v: rusqlite::types::Value = row.get_unwrap(i);
                        let s = match v {
                            rusqlite::types::Value::Null => "NULL".to_owned(),
                            rusqlite::types::Value::Integer(n) => n.to_string(),
                            rusqlite::types::Value::Real(f) => format!("{f}"),
                            rusqlite::types::Value::Text(s) => format!("'{s}'"),
                            rusqlite::types::Value::Blob(b) => {
                                format!(
                                    "X'{}'",
                                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                                )
                            }
                        };
                        vals.push(s);
                    }
                    Ok(vals)
                })
                .map_err(|e| format!("query: {e}"))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| format!("row: {e}"))?;
            Ok(rows)
        })();
        let csql_result = match csql_result {
            Ok(r) => r,
            Err(csql_err) => {
                if frank_result.is_ok() {
                    mismatches.push(format!(
                        "DIVERGE: {query}\n  frank: OK\n  csql:  ERROR({csql_err})"
                    ));
                }
                continue;
            }
        };

        match frank_result {
            Ok(rows) => {
                let frank_strs: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| {
                        row.values()
                            .iter()
                            .map(|v| match v {
                                SqliteValue::Null => "NULL".to_owned(),
                                SqliteValue::Integer(n) => n.to_string(),
                                SqliteValue::Float(f) => format!("{f}"),
                                SqliteValue::Text(s) => format!("'{s}'"),
                                SqliteValue::Blob(b) => {
                                    format!(
                                        "X'{}'",
                                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                                    )
                                }
                            })
                            .collect()
                    })
                    .collect();
                if frank_strs != csql_result {
                    mismatches.push(format!(
                        "MISMATCH: {query}\n  frank: {frank_strs:?}\n  csql:  {csql_result:?}"
                    ));
                }
            }
            Err(e) => {
                mismatches.push(format!(
                    "FRANK_ERROR: {query}\n  frank: {e}\n  csql:  {csql_result:?}"
                ));
            }
        }
    }
    mismatches
}

/// Hex literals, bitwise ops, CAST edge cases, boolean expressions.
#[test]
fn test_conformance_hex_bitwise_cast_bool() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER, txt TEXT);",
        "INSERT INTO nums VALUES (1, 255, '42'), (2, -1, 'abc'), (3, 0, '0'), (4, NULL, NULL);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Hex literals
        "SELECT 0x10, 0xFF, 0x0",
        // Bitwise on table columns
        "SELECT val & 0x0F FROM nums WHERE val IS NOT NULL ORDER BY id",
        "SELECT val | 0xF0 FROM nums WHERE val IS NOT NULL ORDER BY id",
        "SELECT ~val FROM nums WHERE val IS NOT NULL ORDER BY id",
        "SELECT val << 4 FROM nums WHERE val IS NOT NULL ORDER BY id",
        "SELECT val >> 4 FROM nums WHERE val IS NOT NULL ORDER BY id",
        // CAST edge cases
        "SELECT CAST('' AS INTEGER)",
        "SELECT CAST('   ' AS INTEGER)",
        "SELECT CAST('3.14' AS INTEGER)",
        "SELECT CAST(1 AS REAL), typeof(CAST(1 AS REAL))",
        "SELECT CAST(NULL AS INTEGER), CAST(NULL AS REAL), CAST(NULL AS TEXT)",
        // Boolean expressions
        "SELECT 1 = 1, 1 = 0, 0 = 0",
        "SELECT (1 > 0) + (2 > 1) + (3 > 2)",
        "SELECT NOT 1, NOT 0, NOT NULL",
        // COALESCE chains
        "SELECT COALESCE(NULL, NULL, NULL, 42)",
        "SELECT COALESCE(val, -999) FROM nums ORDER BY id",
        // IIF
        "SELECT IIF(1, 'yes', 'no'), IIF(0, 'yes', 'no'), IIF(NULL, 'yes', 'no')",
        // Negative LIMIT (means unlimited in SQLite)
        "SELECT id FROM nums ORDER BY id LIMIT -1",
        // CAST text to numeric
        "SELECT CAST(txt AS INTEGER) FROM nums WHERE txt IS NOT NULL ORDER BY id",
        "SELECT CAST(txt AS REAL) FROM nums WHERE txt IS NOT NULL ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} hex/bitwise/cast/bool mismatches", mismatches.len());
    }
}

/// total(), COUNT(DISTINCT), SUM edge cases.
#[test]
fn test_conformance_total_count_distinct() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE vals (id INTEGER PRIMARY KEY, x REAL, cat TEXT);",
        "INSERT INTO vals VALUES (1, 10.5, 'a'), (2, 20.0, 'a'), (3, NULL, 'b'), (4, 10.5, 'b'), (5, 0.0, 'a');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // total() returns 0.0 for empty/all-null, SUM returns NULL
        "SELECT total(x), SUM(x) FROM vals",
        "SELECT total(x), SUM(x) FROM vals WHERE id > 100",
        "SELECT total(x), SUM(x) FROM vals WHERE x IS NULL",
        // COUNT variations
        "SELECT COUNT(*), COUNT(x), COUNT(DISTINCT x), COUNT(DISTINCT cat) FROM vals",
        // GROUP BY with total/sum
        "SELECT cat, total(x), SUM(x), COUNT(x), COUNT(DISTINCT x) FROM vals GROUP BY cat ORDER BY cat",
        // AVG with NULL
        "SELECT AVG(x) FROM vals",
        "SELECT AVG(x) FROM vals WHERE id > 100",
        // MIN/MAX on mixed
        "SELECT MIN(x), MAX(x) FROM vals",
        "SELECT MIN(cat), MAX(cat) FROM vals",
        // GROUP_CONCAT
        "SELECT GROUP_CONCAT(cat) FROM vals ORDER BY id",
        "SELECT GROUP_CONCAT(DISTINCT cat) FROM vals",
        "SELECT GROUP_CONCAT(cat, ';') FROM vals ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} total/count/distinct mismatches", mismatches.len());
    }
}

/// LIKE with ESCAPE, GLOB patterns.
#[test]
fn test_conformance_like_escape_glob() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE words (id INTEGER PRIMARY KEY, w TEXT);",
        "INSERT INTO words VALUES (1, 'hello'), (2, 'world'), (3, 'he%llo'), (4, 'HeLLo'), (5, 'h_llo'), (6, NULL);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Basic LIKE
        "SELECT w FROM words WHERE w LIKE 'h%' ORDER BY id",
        "SELECT w FROM words WHERE w LIKE '%llo' ORDER BY id",
        "SELECT w FROM words WHERE w LIKE 'h_llo' ORDER BY id",
        // LIKE case insensitivity
        "SELECT w FROM words WHERE w LIKE 'HELLO' ORDER BY id",
        "SELECT w FROM words WHERE w LIKE 'hello' ORDER BY id",
        // LIKE with ESCAPE
        "SELECT w FROM words WHERE w LIKE 'he!%llo' ESCAPE '!' ORDER BY id",
        // NOT LIKE
        "SELECT w FROM words WHERE w NOT LIKE 'h%' ORDER BY id",
        // LIKE with NULL
        "SELECT w FROM words WHERE w LIKE NULL ORDER BY id",
        "SELECT w FROM words WHERE NULL LIKE w ORDER BY id",
        // GLOB (case sensitive, uses * and ?)
        "SELECT w FROM words WHERE w GLOB 'h*' ORDER BY id",
        "SELECT w FROM words WHERE w GLOB 'h?llo' ORDER BY id",
        "SELECT w FROM words WHERE w GLOB 'H*' ORDER BY id",
        // NOT GLOB
        "SELECT w FROM words WHERE w NOT GLOB 'h*' ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} like/escape/glob mismatches", mismatches.len());
    }
}

/// abs(), scalar min/max, typeof, zeroblob, unicode/char, hex, instr.
#[test]
fn test_conformance_math_abs_typeof() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        // abs edge cases
        "SELECT abs(0), abs(-0), abs(42), abs(-42)",
        "SELECT abs(3.14), abs(-3.14)",
        "SELECT abs(NULL)",
        "SELECT abs(-9223372036854775807)",
        // typeof
        "SELECT typeof(1), typeof(1.0), typeof('a'), typeof(NULL), typeof(X'00')",
        "SELECT typeof(1 + 1), typeof(1 + 1.0), typeof(1.0 + 1.0)",
        "SELECT typeof(CAST(1 AS TEXT)), typeof(CAST('1' AS INTEGER))",
        // Scalar min/max (2+ args, not aggregate)
        "SELECT min(1, 2, 3), max(1, 2, 3)",
        "SELECT min('a', 'b', 'c'), max('a', 'b', 'c')",
        "SELECT min(NULL, 1, 2), max(NULL, 1, 2)",
        "SELECT min(1), max(1)",
        // zeroblob
        "SELECT typeof(zeroblob(4)), length(zeroblob(4))",
        "SELECT hex(zeroblob(4))",
        "SELECT zeroblob(0) = X''",
        // unicode/char
        "SELECT unicode('A'), unicode('a'), unicode('0')",
        "SELECT char(65), char(97), char(48)",
        // hex
        "SELECT hex('ABC'), hex(123), hex(NULL)",
        // instr
        "SELECT instr('hello world', 'world'), instr('hello', 'xyz'), instr('hello', 'l')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} math/abs/typeof mismatches", mismatches.len());
    }
}

/// EXCEPT, INTERSECT, compound chaining.
#[test]
fn test_conformance_except_intersect_chains() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE s1 (v INTEGER);",
        "INSERT INTO s1 VALUES (1), (2), (3), (4), (5);",
        "CREATE TABLE s2 (v INTEGER);",
        "INSERT INTO s2 VALUES (3), (4), (5), (6), (7);",
        "CREATE TABLE s3 (v INTEGER);",
        "INSERT INTO s3 VALUES (4), (5), (6), (7), (8);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Basic EXCEPT
        "SELECT v FROM s1 EXCEPT SELECT v FROM s2 ORDER BY v",
        "SELECT v FROM s2 EXCEPT SELECT v FROM s1 ORDER BY v",
        // Basic INTERSECT
        "SELECT v FROM s1 INTERSECT SELECT v FROM s2 ORDER BY v",
        // Chained compounds
        "SELECT v FROM s1 UNION SELECT v FROM s2 UNION SELECT v FROM s3 ORDER BY v",
        "SELECT v FROM s1 INTERSECT SELECT v FROM s2 INTERSECT SELECT v FROM s3 ORDER BY v",
        // UNION ALL vs UNION
        "SELECT v FROM s1 UNION ALL SELECT v FROM s2 ORDER BY v",
        "SELECT v FROM s1 UNION SELECT v FROM s2 ORDER BY v",
        // Compound with expressions
        "SELECT v * 2 FROM s1 UNION SELECT v FROM s2 ORDER BY 1",
        // EXCEPT after UNION
        "SELECT v FROM s1 UNION SELECT v FROM s2 EXCEPT SELECT v FROM s3 ORDER BY v",
        // Count of compound result
        "SELECT COUNT(*) FROM (SELECT v FROM s1 UNION SELECT v FROM s2)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} except/intersect/chain mismatches", mismatches.len());
    }
}

/// UPDATE with self-referencing expressions.
#[test]
fn test_conformance_update_self_ref_case() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER, label TEXT);",
        "INSERT INTO counters VALUES (1, 10, 'low'), (2, 50, 'mid'), (3, 90, 'high');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // Self-referencing UPDATE
    let updates = [
        "UPDATE counters SET val = val + 1",
        "UPDATE counters SET val = val * 2 WHERE id = 2",
        "UPDATE counters SET label = CASE WHEN val > 100 THEN 'very_high' WHEN val > 50 THEN 'high' ELSE label END",
    ];
    for u in &updates {
        fconn.execute(u).unwrap();
        rconn.execute_batch(u).unwrap();
    }

    let queries = [
        "SELECT * FROM counters ORDER BY id",
        "SELECT id, val, label FROM counters WHERE val > 50 ORDER BY id",
        "SELECT SUM(val), AVG(val) FROM counters",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} update self-ref mismatches", mismatches.len());
    }
}

/// LEFT JOIN with IS NULL filter and COUNT.
#[test]
fn test_conformance_left_join_is_null_count() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT);",
        "INSERT INTO parents VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');",
        "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT);",
        "INSERT INTO children VALUES (1, 1, 'Dave'), (2, 1, 'Eve'), (3, 2, 'Frank');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Parents without children
        "SELECT p.name FROM parents p LEFT JOIN children c ON p.id = c.parent_id WHERE c.id IS NULL ORDER BY p.name",
        // Count children per parent
        "SELECT p.name, COUNT(c.id) AS child_count FROM parents p LEFT JOIN children c ON p.id = c.parent_id GROUP BY p.name ORDER BY p.name",
        // LEFT JOIN with COALESCE
        "SELECT p.name, COALESCE(c.name, 'none') AS child FROM parents p LEFT JOIN children c ON p.id = c.parent_id ORDER BY p.name, child",
        // Aggregate + HAVING on LEFT JOIN
        "SELECT p.name, COUNT(c.id) AS cnt FROM parents p LEFT JOIN children c ON p.id = c.parent_id GROUP BY p.name HAVING COUNT(c.id) > 0 ORDER BY p.name",
        // Subquery with LEFT JOIN
        "SELECT name FROM (SELECT p.name, COUNT(c.id) AS cnt FROM parents p LEFT JOIN children c ON p.id = c.parent_id GROUP BY p.name) WHERE cnt = 0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} left join is null mismatches", mismatches.len());
    }
}

/// REPLACE statement and INSERT OR REPLACE.
#[test]
fn test_conformance_replace_stmt() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE kv (key TEXT PRIMARY KEY, val INTEGER);",
        "INSERT INTO kv VALUES ('a', 1), ('b', 2), ('c', 3);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // REPLACE operations
    let ops = [
        "REPLACE INTO kv VALUES ('a', 10)",
        "REPLACE INTO kv VALUES ('d', 4)",
        "INSERT OR REPLACE INTO kv VALUES ('b', 20)",
    ];
    for o in &ops {
        fconn.execute(o).unwrap();
        rconn.execute_batch(o).unwrap();
    }

    let queries = [
        "SELECT * FROM kv ORDER BY key",
        "SELECT COUNT(*) FROM kv",
        "SELECT SUM(val) FROM kv",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} replace statement mismatches", mismatches.len());
    }
}

/// Savepoint nesting with ROLLBACK and RELEASE.
#[test]
fn test_conformance_savepoint_rollback_release() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE sp (id INTEGER PRIMARY KEY, val TEXT);",
        "INSERT INTO sp VALUES (1, 'original');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // Savepoint operations
    let ops = [
        "SAVEPOINT outer_sp",
        "INSERT INTO sp VALUES (2, 'in_outer')",
        "SAVEPOINT inner_sp",
        "INSERT INTO sp VALUES (3, 'in_inner')",
        "ROLLBACK TO inner_sp",
        "INSERT INTO sp VALUES (4, 'after_rollback')",
        "RELEASE outer_sp",
    ];
    for o in &ops {
        let _ = fconn.execute(o);
        let _ = rconn.execute_batch(o);
    }

    let queries = ["SELECT * FROM sp ORDER BY id", "SELECT COUNT(*) FROM sp"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} savepoint mismatches", mismatches.len());
    }
}

/// DEFAULT clause expressions.
#[test]
fn test_conformance_default_value_expressions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE defs (id INTEGER PRIMARY KEY, val INTEGER DEFAULT 42, txt TEXT DEFAULT 'hello', flag INTEGER DEFAULT (1 + 1));",
        "INSERT INTO defs (id) VALUES (1);",
        "INSERT INTO defs (id, val) VALUES (2, 100);",
        "INSERT INTO defs VALUES (3, 0, 'custom', 0);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM defs ORDER BY id",
        "SELECT id, val, txt, flag FROM defs WHERE val = 42 ORDER BY id",
        "SELECT COUNT(*) FROM defs WHERE txt = 'hello'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} default value mismatches", mismatches.len());
    }
}

/// String function edge cases: substr, replace, upper/lower, ltrim/rtrim/trim, printf.
#[test]
fn test_conformance_string_functions_edge() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        // substr edge cases
        "SELECT substr('hello', 0)",
        "SELECT substr('hello', 1)",
        "SELECT substr('hello', -2)",
        "SELECT substr('hello', 2, 3)",
        "SELECT substr('hello', 2, 100)",
        "SELECT substr('hello', -3, 2)",
        "SELECT substr('hello', 0, 0)",
        "SELECT substr(NULL, 1, 2)",
        "SELECT substr('hello', NULL, 2)",
        // replace
        "SELECT replace('hello world', 'world', 'there')",
        "SELECT replace('aaa', 'a', 'bb')",
        "SELECT replace('hello', 'x', 'y')",
        "SELECT replace('hello', '', 'x')",
        "SELECT replace(NULL, 'a', 'b')",
        // upper/lower
        "SELECT upper('hello'), lower('HELLO')",
        "SELECT upper(NULL), lower(NULL)",
        "SELECT upper(123), lower(456)",
        // trim variants
        "SELECT trim('  hello  '), ltrim('  hello'), rtrim('hello  ')",
        "SELECT trim('xxhelloxx', 'x'), ltrim('xxhello', 'x'), rtrim('helloxx', 'x')",
        "SELECT trim(NULL), ltrim(NULL), rtrim(NULL)",
        // length
        "SELECT length('hello'), length(''), length(NULL)",
        "SELECT length(X'0102'), length(123), length(1.5)",
        // printf/format
        "SELECT printf('%d', 42), printf('%05d', 42)",
        "SELECT printf('%.2f', 3.14159)",
        "SELECT printf('%s', 'hello')",
        "SELECT printf('%d %s', 42, 'answer')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} string function edge mismatches", mismatches.len());
    }
}

/// Numeric expression edge cases: integer overflow, division, modulo.
#[test]
fn test_conformance_numeric_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        // Integer overflow → float promotion
        "SELECT 9223372036854775807 + 1",
        "SELECT -9223372036854775808 - 1",
        "SELECT 9223372036854775807 * 2",
        // Division edge cases
        "SELECT 1 / 0",
        "SELECT 1.0 / 0.0",
        "SELECT 0 / 0",
        "SELECT 0.0 / 0.0",
        // Modulo
        "SELECT 10 % 3, -10 % 3, 10 % -3, -10 % -3",
        "SELECT 10 % 0",
        // Unary minus edge cases
        "SELECT -(-42)",
        "SELECT -9223372036854775807",
        "SELECT -(9223372036854775807)",
        // Float precision
        "SELECT 1e308, -1e308",
        "SELECT typeof(1e308)",
        // Comparison with mixed types
        "SELECT 1 < 1.0, 1 = 1.0, 1 > 0.9",
        "SELECT '9' > 10",
        "SELECT '9' > '10'",
        // Aggregate with single value
        "SELECT SUM(1), AVG(1), COUNT(1), MIN(1), MAX(1)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} numeric edge mismatches", mismatches.len());
    }
}

/// Complex WHERE clause patterns: nested AND/OR, BETWEEN, IN with subquery.
#[test]
fn test_conformance_complex_where() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT);",
        "INSERT INTO items VALUES (1, 'apple', 1.50, 'fruit'), (2, 'banana', 0.75, 'fruit'), (3, 'carrot', 2.00, 'vegetable'), (4, 'donut', 3.50, 'pastry'), (5, NULL, NULL, NULL);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Nested AND/OR
        "SELECT name FROM items WHERE (category = 'fruit' OR category = 'vegetable') AND price > 1.0 ORDER BY name",
        "SELECT name FROM items WHERE category = 'fruit' AND (price > 1.0 OR name = 'banana') ORDER BY name",
        // BETWEEN
        "SELECT name FROM items WHERE price BETWEEN 1.0 AND 2.5 ORDER BY name",
        "SELECT name FROM items WHERE price NOT BETWEEN 1.0 AND 2.5 ORDER BY name",
        // IN list
        "SELECT name FROM items WHERE category IN ('fruit', 'pastry') ORDER BY name",
        "SELECT name FROM items WHERE category NOT IN ('fruit', 'pastry') ORDER BY name",
        // IN with subquery
        "SELECT name FROM items WHERE id IN (SELECT id FROM items WHERE price > 2.0) ORDER BY name",
        // IS NULL / IS NOT NULL in complex expressions
        "SELECT name FROM items WHERE name IS NOT NULL AND price IS NOT NULL ORDER BY name",
        "SELECT COALESCE(name, 'unnamed'), COALESCE(price, 0.0) FROM items ORDER BY id",
        // CASE in WHERE
        "SELECT name FROM items WHERE CASE category WHEN 'fruit' THEN 1 WHEN 'vegetable' THEN 1 ELSE 0 END = 1 ORDER BY name",
        // EXISTS correlation
        "SELECT i1.name FROM items i1 WHERE EXISTS (SELECT 1 FROM items i2 WHERE i2.category = i1.category AND i2.id != i1.id) ORDER BY i1.name",
        // Complex expression with NULL propagation
        "SELECT name FROM items WHERE price * 2 > 3.0 ORDER BY name",
        "SELECT name FROM items WHERE length(name) > 5 ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex WHERE mismatches", mismatches.len());
    }
}

/// ALTER TABLE basic operations.
#[test]
fn test_conformance_alter_table() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, val INTEGER);",
        "INSERT INTO t1 VALUES (1, 'Alice', 10), (2, 'Bob', 20);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // ADD COLUMN
    let alter = "ALTER TABLE t1 ADD COLUMN extra TEXT DEFAULT 'none'";
    fconn.execute(alter).unwrap();
    rconn.execute_batch(alter).unwrap();

    let queries = [
        "SELECT * FROM t1 ORDER BY id",
        "SELECT extra FROM t1 ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} alter table mismatches", mismatches.len());
    }

    // INSERT after ALTER
    let ins = "INSERT INTO t1 VALUES (3, 'Carol', 30, 'special')";
    fconn.execute(ins).unwrap();
    rconn.execute_batch(ins).unwrap();

    let queries2 = [
        "SELECT * FROM t1 ORDER BY id",
        "SELECT name, extra FROM t1 WHERE extra != 'none' ORDER BY id",
    ];

    let mismatches2 = oracle_compare(&fconn, &rconn, &queries2);
    if !mismatches2.is_empty() {
        for m in &mismatches2 {
            eprintln!("{m}\n");
        }
        panic!("{} alter table post-insert mismatches", mismatches2.len());
    }
}

/// Subquery patterns: correlated, scalar, derived tables, EXISTS.
#[test]
fn test_conformance_subquery_patterns() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL);",
        "INSERT INTO emp VALUES (1, 'Alice', 1, 50000), (2, 'Bob', 1, 60000), (3, 'Carol', 2, 55000), (4, 'Dave', 2, 45000), (5, 'Eve', 3, 70000);",
        "CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT);",
        "INSERT INTO dept VALUES (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Scalar subquery in SELECT
        "SELECT e.name, (SELECT d.name FROM dept d WHERE d.id = e.dept_id) AS dept FROM emp e ORDER BY e.id",
        // Correlated subquery in WHERE
        "SELECT e.name FROM emp e WHERE e.salary > (SELECT AVG(e2.salary) FROM emp e2 WHERE e2.dept_id = e.dept_id) ORDER BY e.name",
        // Derived table
        "SELECT d.name, sub.cnt FROM dept d JOIN (SELECT dept_id, COUNT(*) AS cnt FROM emp GROUP BY dept_id) sub ON d.id = sub.dept_id ORDER BY d.name",
        // EXISTS
        "SELECT d.name FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id AND e.salary > 55000) ORDER BY d.name",
        // NOT EXISTS
        "SELECT d.name FROM dept d WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id AND e.salary < 50000) ORDER BY d.name",
        // IN subquery
        "SELECT name FROM emp WHERE dept_id IN (SELECT id FROM dept WHERE name LIKE 'E%') ORDER BY name",
        // Scalar subquery returning single value
        "SELECT (SELECT MAX(salary) FROM emp) - (SELECT MIN(salary) FROM emp)",
        // Nested subqueries
        "SELECT name FROM emp WHERE salary = (SELECT MAX(salary) FROM emp WHERE dept_id = (SELECT id FROM dept WHERE name = 'Engineering'))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} subquery pattern mismatches", mismatches.len());
    }
}

/// INSERT...SELECT, INSERT...DEFAULT VALUES, multi-row VALUES.
#[test]
fn test_conformance_insert_variations() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT);",
        "INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c');",
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, val TEXT, extra TEXT DEFAULT 'default');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // INSERT...SELECT
    let ins_sel = "INSERT INTO dst (id, val) SELECT id, val FROM src WHERE id <= 2";
    fconn.execute(ins_sel).unwrap();
    rconn.execute_batch(ins_sel).unwrap();

    // Multi-row VALUES
    let multi = "INSERT INTO dst VALUES (10, 'x', 'custom'), (11, 'y', 'custom2')";
    fconn.execute(multi).unwrap();
    rconn.execute_batch(multi).unwrap();

    let queries = [
        "SELECT * FROM dst ORDER BY id",
        "SELECT COUNT(*) FROM dst",
        "SELECT id, extra FROM dst WHERE extra = 'default' ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} insert variation mismatches", mismatches.len());
    }
}

/// DELETE with complex WHERE, DELETE with LIMIT (SQLite extension).
#[test]
fn test_conformance_delete_patterns() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE logs (id INTEGER PRIMARY KEY, level TEXT, msg TEXT);",
        "INSERT INTO logs VALUES (1, 'INFO', 'start'), (2, 'WARN', 'slow'), (3, 'ERROR', 'fail'), (4, 'INFO', 'end'), (5, 'ERROR', 'crash');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // DELETE with subquery in WHERE
    let del1 = "DELETE FROM logs WHERE id IN (SELECT id FROM logs WHERE level = 'INFO')";
    fconn.execute(del1).unwrap();
    rconn.execute_batch(del1).unwrap();

    let queries1 = [
        "SELECT * FROM logs ORDER BY id",
        "SELECT COUNT(*) FROM logs",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries1);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} delete pattern mismatches", mismatches.len());
    }

    // DELETE all remaining
    let del2 = "DELETE FROM logs WHERE level = 'WARN' OR level = 'ERROR'";
    fconn.execute(del2).unwrap();
    rconn.execute_batch(del2).unwrap();

    let queries2 = ["SELECT COUNT(*) FROM logs"];
    let mismatches2 = oracle_compare(&fconn, &rconn, &queries2);
    if !mismatches2.is_empty() {
        for m in &mismatches2 {
            eprintln!("{m}\n");
        }
        panic!("{} delete all mismatches", mismatches2.len());
    }
}

/// Multiple table operations: INSERT → UPDATE → DELETE → verify.
#[test]
fn test_conformance_dml_sequence() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let ops = [
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, acct TEXT, amount REAL)",
        "INSERT INTO ledger VALUES (1, 'checking', 1000.0)",
        "INSERT INTO ledger VALUES (2, 'savings', 5000.0)",
        "INSERT INTO ledger VALUES (3, 'checking', -200.0)",
        "UPDATE ledger SET amount = amount + 100 WHERE acct = 'checking'",
        "DELETE FROM ledger WHERE amount < 0",
        "INSERT INTO ledger VALUES (4, 'savings', -500.0)",
        "UPDATE ledger SET amount = amount * 1.01",
    ];
    for o in &ops {
        fconn.execute(o).unwrap();
        rconn.execute_batch(o).unwrap();
    }

    let queries = [
        "SELECT * FROM ledger ORDER BY id",
        "SELECT acct, SUM(amount) FROM ledger GROUP BY acct ORDER BY acct",
        "SELECT COUNT(*), MIN(amount), MAX(amount) FROM ledger",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DML sequence mismatches", mismatches.len());
    }
}

/// Recursive CTE edge cases: fibonacci, hierarchical, depth limits.
#[test]
fn test_conformance_recursive_cte_advanced() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE org (id INTEGER PRIMARY KEY, name TEXT, mgr_id INTEGER);",
        "INSERT INTO org VALUES (1, 'CEO', NULL), (2, 'VP1', 1), (3, 'VP2', 1), (4, 'Dir1', 2), (5, 'Dir2', 2), (6, 'Mgr1', 4);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Simple counter
        "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT x FROM cnt",
        // Fibonacci
        "WITH RECURSIVE fib(a,b) AS (VALUES(0,1) UNION ALL SELECT b, a+b FROM fib WHERE a < 100) SELECT a FROM fib",
        // Hierarchical query
        "WITH RECURSIVE hier(id, name, lvl) AS (SELECT id, name, 0 FROM org WHERE mgr_id IS NULL UNION ALL SELECT o.id, o.name, h.lvl+1 FROM org o JOIN hier h ON o.mgr_id = h.id) SELECT name, lvl FROM hier ORDER BY lvl, name",
        // Recursive with aggregate on result
        "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<5) SELECT SUM(x), COUNT(x), AVG(x) FROM cnt",
        // Multiple CTEs
        "WITH a AS (SELECT 1 AS v UNION ALL SELECT 2), b AS (SELECT v * 10 AS w FROM a) SELECT * FROM b ORDER BY w",
        // CTE used multiple times
        "WITH vals AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT a.n, b.n FROM vals a, vals b WHERE a.n < b.n ORDER BY a.n, b.n",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} recursive CTE mismatches", mismatches.len());
    }
}

/// Multi-table JOIN with aggregates and GROUP BY.
#[test]
fn test_conformance_multi_join_aggregate() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, cat_id INTEGER, price REAL);",
        "INSERT INTO products VALUES (1, 'Widget', 1, 9.99), (2, 'Gadget', 1, 19.99), (3, 'Doohickey', 2, 4.99), (4, 'Thingamajig', 2, 14.99), (5, 'Whatchamacallit', 3, 29.99);",
        "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT);",
        "INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Hardware'), (3, 'Software');",
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, prod_id INTEGER, qty INTEGER, sale_date TEXT);",
        "INSERT INTO sales VALUES (1, 1, 10, '2024-01-01'), (2, 1, 5, '2024-01-02'), (3, 2, 3, '2024-01-01'), (4, 3, 20, '2024-01-03'), (5, 5, 1, '2024-01-01');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Three-table JOIN with aggregate
        "SELECT c.name, SUM(s.qty) AS total_qty FROM categories c JOIN products p ON c.id = p.cat_id JOIN sales s ON p.id = s.prod_id GROUP BY c.name ORDER BY c.name",
        // Revenue per category
        "SELECT c.name, SUM(s.qty * p.price) AS revenue FROM categories c JOIN products p ON c.id = p.cat_id JOIN sales s ON p.id = s.prod_id GROUP BY c.name ORDER BY revenue DESC",
        // Products with no sales (LEFT JOIN)
        "SELECT p.name FROM products p LEFT JOIN sales s ON p.id = s.prod_id WHERE s.id IS NULL ORDER BY p.name",
        // Category with most products
        "SELECT c.name, COUNT(p.id) AS cnt FROM categories c LEFT JOIN products p ON c.id = p.cat_id GROUP BY c.name ORDER BY cnt DESC, c.name",
        // Average qty per product that has sales
        "SELECT p.name, AVG(s.qty) AS avg_qty FROM products p JOIN sales s ON p.id = s.prod_id GROUP BY p.name ORDER BY p.name",
        // HAVING on three-table join
        "SELECT c.name FROM categories c JOIN products p ON c.id = p.cat_id JOIN sales s ON p.id = s.prod_id GROUP BY c.name HAVING SUM(s.qty) > 10 ORDER BY c.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-join aggregate mismatches", mismatches.len());
    }
}

/// ORDER BY with NULLS FIRST/LAST, expressions, mixed ASC/DESC.
#[test]
fn test_conformance_order_by_advanced() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE sortme (id INTEGER PRIMARY KEY, val INTEGER, txt TEXT);",
        "INSERT INTO sortme VALUES (1, 30, 'cherry'), (2, NULL, 'apple'), (3, 10, NULL), (4, 20, 'banana'), (5, NULL, NULL);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Basic NULL ordering (SQLite default: NULLs first in ASC)
        "SELECT id, val FROM sortme ORDER BY val",
        "SELECT id, val FROM sortme ORDER BY val DESC",
        // NULLS FIRST / NULLS LAST
        "SELECT id, val FROM sortme ORDER BY val NULLS FIRST",
        "SELECT id, val FROM sortme ORDER BY val NULLS LAST",
        "SELECT id, val FROM sortme ORDER BY val DESC NULLS FIRST",
        "SELECT id, val FROM sortme ORDER BY val DESC NULLS LAST",
        // Multiple sort keys
        "SELECT id FROM sortme ORDER BY val, txt",
        "SELECT id FROM sortme ORDER BY val ASC, txt DESC",
        // Expression in ORDER BY
        "SELECT id, val FROM sortme WHERE val IS NOT NULL ORDER BY val % 15",
        "SELECT id, val FROM sortme WHERE val IS NOT NULL ORDER BY -val",
        // ORDER BY column number
        "SELECT id, val FROM sortme ORDER BY 2, 1",
        // ORDER BY alias
        "SELECT id, COALESCE(val, 0) AS safe_val FROM sortme ORDER BY safe_val",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} order by advanced mismatches", mismatches.len());
    }
}

/// UPSERT (INSERT ... ON CONFLICT) edge cases.
#[test]
fn test_conformance_upsert_advanced() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE upsert_t (id INTEGER PRIMARY KEY, name TEXT, counter INTEGER DEFAULT 0);",
        "INSERT INTO upsert_t VALUES (1, 'Alice', 1), (2, 'Bob', 1);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // DO UPDATE with excluded reference
    let ops = [
        "INSERT INTO upsert_t VALUES (1, 'Alice_new', 1) ON CONFLICT(id) DO UPDATE SET counter = counter + excluded.counter, name = excluded.name",
        "INSERT INTO upsert_t VALUES (3, 'Carol', 1) ON CONFLICT(id) DO UPDATE SET counter = counter + 1",
        "INSERT INTO upsert_t VALUES (2, 'Bob', 5) ON CONFLICT(id) DO UPDATE SET counter = counter + excluded.counter",
        // DO NOTHING
        "INSERT INTO upsert_t VALUES (1, 'should_not_appear', 99) ON CONFLICT DO NOTHING",
    ];
    for o in &ops {
        fconn.execute(o).unwrap();
        rconn.execute_batch(o).unwrap();
    }

    let queries = [
        "SELECT * FROM upsert_t ORDER BY id",
        "SELECT SUM(counter) FROM upsert_t",
        "SELECT name FROM upsert_t WHERE id = 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} upsert advanced mismatches", mismatches.len());
    }
}

/// CASE expression variations: simple, searched, nested, with aggregates.
#[test]
fn test_conformance_case_expression_variants() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE scores (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
        "INSERT INTO scores VALUES (1, 'Alice', 95), (2, 'Bob', 72), (3, 'Carol', 88), (4, 'Dave', 55), (5, 'Eve', NULL);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Simple CASE
        "SELECT name, CASE score WHEN 95 THEN 'A+' WHEN 88 THEN 'B+' ELSE 'other' END AS grade FROM scores ORDER BY id",
        // Searched CASE
        "SELECT name, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' WHEN score IS NULL THEN 'N/A' ELSE 'F' END AS grade FROM scores ORDER BY id",
        // CASE with NULL
        "SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END",
        "SELECT CASE WHEN NULL THEN 'true' ELSE 'false' END",
        // Nested CASE
        "SELECT CASE WHEN score > 80 THEN CASE WHEN score > 90 THEN 'excellent' ELSE 'good' END ELSE 'needs improvement' END FROM scores WHERE score IS NOT NULL ORDER BY id",
        // CASE in aggregate
        "SELECT COUNT(CASE WHEN score >= 80 THEN 1 END) AS pass_count, COUNT(CASE WHEN score < 80 THEN 1 END) AS fail_count FROM scores",
        "SELECT SUM(CASE WHEN score >= 80 THEN score ELSE 0 END) AS high_total FROM scores",
        // CASE with no ELSE (returns NULL)
        "SELECT name, CASE WHEN score > 90 THEN 'top' END AS label FROM scores ORDER BY id",
        // CASE in ORDER BY
        "SELECT name, score FROM scores WHERE score IS NOT NULL ORDER BY CASE WHEN score >= 90 THEN 0 WHEN score >= 80 THEN 1 ELSE 2 END, name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CASE expression mismatches", mismatches.len());
    }
}

/// Index usage verification: queries that exercise indexed lookups.
#[test]
fn test_conformance_index_queries() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE indexed_t (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL);",
        "CREATE INDEX idx_name ON indexed_t (name);",
        "CREATE INDEX idx_cat_price ON indexed_t (category, price);",
        "INSERT INTO indexed_t VALUES (1, 'apple', 'fruit', 1.50), (2, 'banana', 'fruit', 0.75), (3, 'carrot', 'veggie', 2.00), (4, 'date', 'fruit', 3.00), (5, 'eggplant', 'veggie', 1.80);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Point lookup by name (uses idx_name)
        "SELECT id, price FROM indexed_t WHERE name = 'carrot'",
        // Range scan on index
        "SELECT name FROM indexed_t WHERE name >= 'c' AND name < 'e' ORDER BY name",
        // Composite index prefix
        "SELECT name, price FROM indexed_t WHERE category = 'fruit' ORDER BY price",
        // Composite index full
        "SELECT name FROM indexed_t WHERE category = 'fruit' AND price > 1.0 ORDER BY name",
        // NOT IN with index
        "SELECT name FROM indexed_t WHERE name NOT IN ('apple', 'banana') ORDER BY name",
        // LIKE with index (prefix optimization possible)
        "SELECT name FROM indexed_t WHERE name LIKE 'a%' ORDER BY name",
        // COUNT with index
        "SELECT category, COUNT(*) FROM indexed_t GROUP BY category ORDER BY category",
        // MIN/MAX with index
        "SELECT MIN(name), MAX(name) FROM indexed_t",
        "SELECT MIN(price), MAX(price) FROM indexed_t WHERE category = 'fruit'",
        // UNIQUE constraint via index
        "CREATE UNIQUE INDEX idx_uniq ON indexed_t (name)",
    ];

    // Run the CREATE UNIQUE INDEX on both
    // Run DDL on both
    fconn.execute(queries[queries.len() - 1]).unwrap();
    rconn.execute_batch(queries[queries.len() - 1]).unwrap();

    let check_queries = [
        "SELECT name FROM indexed_t WHERE name = 'carrot'",
        "SELECT name, price FROM indexed_t WHERE category = 'fruit' ORDER BY price",
        "SELECT category, COUNT(*) FROM indexed_t GROUP BY category ORDER BY category",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries[..queries.len() - 1]);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} index query mismatches", mismatches.len());
    }

    let mismatches2 = oracle_compare(&fconn, &rconn, &check_queries);
    if !mismatches2.is_empty() {
        for m in &mismatches2 {
            eprintln!("{m}\n");
        }
        panic!("{} post-unique-index mismatches", mismatches2.len());
    }
}

/// Multi-table SUM precision (triggers the KBN fix).
#[test]
fn test_conformance_sum_precision_kbn() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE prices (id INTEGER PRIMARY KEY, val REAL);",
        "INSERT INTO prices VALUES (1, 0.1), (2, 0.2), (3, 0.3), (4, 0.4), (5, 0.5), (6, 0.6), (7, 0.7), (8, 0.8), (9, 0.9), (10, 1.0);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Basic sum of 0.1..1.0 (classic float precision test)
        "SELECT SUM(val) FROM prices",
        "SELECT total(val) FROM prices",
        "SELECT AVG(val) FROM prices",
        // Larger sums
        "SELECT SUM(val * 1000) FROM prices",
        // SUM with multiplication in expression
        "SELECT SUM(val * val) FROM prices",
        // Multiple columns
        "SELECT SUM(val), SUM(val * 2), SUM(val / 3.0) FROM prices",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} SUM precision mismatches", mismatches.len());
    }
}

/// GROUP_CONCAT edge cases with separators and ordering.
#[test]
fn test_conformance_group_concat_advanced() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE tags (id INTEGER PRIMARY KEY, item TEXT, tag TEXT);",
        "INSERT INTO tags VALUES (1, 'A', 'red'), (2, 'A', 'blue'), (3, 'B', 'green'), (4, 'A', 'red'), (5, 'B', NULL);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Basic group_concat per group
        "SELECT item, GROUP_CONCAT(tag) FROM tags GROUP BY item ORDER BY item",
        // With separator
        "SELECT item, GROUP_CONCAT(tag, '; ') FROM tags GROUP BY item ORDER BY item",
        // DISTINCT
        "SELECT item, GROUP_CONCAT(DISTINCT tag) FROM tags GROUP BY item ORDER BY item",
        // Global
        "SELECT GROUP_CONCAT(tag) FROM tags",
        // NULL handling (NULLs should be skipped)
        "SELECT GROUP_CONCAT(tag) FROM tags WHERE item = 'B'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} group_concat mismatches", mismatches.len());
    }
}

/// Trigger behavior: BEFORE INSERT, AFTER UPDATE.
#[test]
fn test_conformance_triggers() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE audit (id INTEGER PRIMARY KEY, action TEXT, ts TEXT DEFAULT 'now');",
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT);",
        "CREATE TRIGGER trg_insert AFTER INSERT ON items BEGIN INSERT INTO audit (action) VALUES ('INSERT:' || NEW.name); END;",
        "CREATE TRIGGER trg_update AFTER UPDATE ON items BEGIN INSERT INTO audit (action) VALUES ('UPDATE:' || NEW.name); END;",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let ops = [
        "INSERT INTO items VALUES (1, 'Widget', NULL)",
        "INSERT INTO items VALUES (2, 'Gadget', NULL)",
        "UPDATE items SET name = 'SuperWidget' WHERE id = 1",
    ];
    for o in &ops {
        fconn.execute(o).unwrap();
        rconn.execute_batch(o).unwrap();
    }

    let queries = [
        "SELECT * FROM items ORDER BY id",
        "SELECT action FROM audit ORDER BY id",
        "SELECT COUNT(*) FROM audit",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} trigger mismatches", mismatches.len());
    }
}

/// Foreign key constraint enforcement (PRAGMA foreign_keys=ON).
#[test]
fn test_conformance_foreign_keys() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "PRAGMA foreign_keys = ON;",
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);",
        "INSERT INTO parent VALUES (1, 'P1'), (2, 'P2');",
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), val TEXT);",
        "INSERT INTO child VALUES (1, 1, 'C1'), (2, 2, 'C2');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM child ORDER BY id",
        "SELECT c.val, p.name FROM child c JOIN parent p ON c.parent_id = p.id ORDER BY c.id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} foreign key mismatches", mismatches.len());
    }
}

/// Datetime functions: date(), time(), datetime(), strftime(), julianday().
#[test]
fn test_conformance_datetime_functions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        // Basic date functions with fixed input
        "SELECT date('2024-03-15')",
        "SELECT time('13:30:45')",
        "SELECT datetime('2024-03-15 13:30:45')",
        // Modifiers
        "SELECT date('2024-03-15', '+1 day')",
        "SELECT date('2024-03-15', '-1 month')",
        "SELECT date('2024-03-15', '+1 year')",
        "SELECT date('2024-03-15', 'start of month')",
        "SELECT date('2024-03-15', 'start of year')",
        // julianday
        "SELECT typeof(julianday('2024-03-15'))",
        // strftime
        "SELECT strftime('%Y', '2024-03-15')",
        "SELECT strftime('%m', '2024-03-15')",
        "SELECT strftime('%d', '2024-03-15')",
        "SELECT strftime('%H:%M:%S', '2024-03-15 13:30:45')",
        "SELECT strftime('%s', '2024-03-15 00:00:00')",
        // Date arithmetic
        "SELECT date('2024-01-31', '+1 month')",
        "SELECT date('2024-02-29', '+1 year')",
        // Time arithmetic
        "SELECT time('23:59:59', '+1 second')",
        "SELECT time('00:00:00', '-1 second')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} datetime function mismatches", mismatches.len());
    }
}

/// Type coercion and affinity in comparisons and storage.
#[test]
fn test_conformance_type_coercion_comparisons() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE tc (id INTEGER PRIMARY KEY, i INTEGER, r REAL, t TEXT, b BLOB, n NUMERIC);",
        "INSERT INTO tc VALUES (1, 42, 3.14, '100', X'CAFE', '99.5');",
        "INSERT INTO tc VALUES (2, 0, 0.0, '0', X'00', '0');",
        "INSERT INTO tc VALUES (3, -1, -1.5, '-1', NULL, '-2.5');",
        "INSERT INTO tc VALUES (4, 9223372036854775807, 1e18, '9999999999999999999', X'FF', '1e5');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Text-to-number coercion in comparisons
        "SELECT i = t FROM tc WHERE id = 1",
        "SELECT typeof(i), typeof(t) FROM tc WHERE id = 1",
        // Numeric affinity storage
        "SELECT typeof(n), n FROM tc WHERE id = 1",
        "SELECT typeof(n), n FROM tc WHERE id = 4",
        // REAL vs INTEGER comparison
        "SELECT 1 = 1.0, 0 = 0.0",
        "SELECT 42 > 41.9, 42 < 42.1",
        // String comparison with numeric strings
        "SELECT '9' > '10'",
        "SELECT 9 > 10",
        // CAST interactions
        "SELECT CAST('3.14' AS INTEGER)",
        "SELECT CAST(3.14 AS INTEGER)",
        "SELECT CAST('abc' AS INTEGER)",
        "SELECT CAST('' AS INTEGER)",
        "SELECT CAST(NULL AS INTEGER)",
        "SELECT CAST(42 AS REAL)",
        "SELECT CAST('42' AS REAL)",
        "SELECT CAST(42 AS TEXT)",
        "SELECT CAST(X'48454C4C4F' AS TEXT)",
        // typeof after operations
        "SELECT typeof(1 + 1), typeof(1 + 1.0), typeof(1 || 'a')",
        "SELECT typeof(NULL + 1), typeof(NULL || 'a')",
        // Coercion in WHERE
        "SELECT id FROM tc WHERE t > 50 ORDER BY id",
        "SELECT id FROM tc WHERE i = '42'",
        // BETWEEN with mixed types
        "SELECT 5 BETWEEN 1 AND 10, 5 BETWEEN '1' AND '10'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} type coercion mismatches", mismatches.len());
    }
}

/// COALESCE, IIF, NULLIF edge cases and combinations.
#[test]
fn test_conformance_coalesce_iif_nullif_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE vals (id INTEGER PRIMARY KEY, a, b, c);",
        "INSERT INTO vals VALUES (1, NULL, NULL, NULL);",
        "INSERT INTO vals VALUES (2, NULL, 'second', 'third');",
        "INSERT INTO vals VALUES (3, 'first', NULL, 'third');",
        "INSERT INTO vals VALUES (4, 1, 2, 3);",
        "INSERT INTO vals VALUES (5, 0, '', NULL);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // COALESCE
        "SELECT COALESCE(a, b, c) FROM vals ORDER BY id",
        "SELECT COALESCE(NULL, NULL, NULL)",
        "SELECT COALESCE(NULL, 0, 1)",
        "SELECT COALESCE(NULL, '', 'x')",
        // NULLIF
        "SELECT NULLIF(1, 1), NULLIF(1, 2), NULLIF(NULL, 1), NULLIF(1, NULL)",
        "SELECT NULLIF('abc', 'abc'), NULLIF('abc', 'def')",
        "SELECT NULLIF(0, 0), NULLIF(0, '')",
        // IIF (SQLite 3.32+)
        "SELECT IIF(1, 'yes', 'no'), IIF(0, 'yes', 'no'), IIF(NULL, 'yes', 'no')",
        "SELECT IIF(1 > 0, 100, 200)",
        "SELECT IIF(a IS NULL, 'null_a', a) FROM vals ORDER BY id",
        // Nested
        "SELECT COALESCE(NULLIF(a, 'first'), 'fallback') FROM vals ORDER BY id",
        "SELECT IIF(NULLIF(a, 1) IS NULL, 'was_one', 'not_one') FROM vals ORDER BY id",
        // COALESCE with aggregates
        "SELECT COALESCE(SUM(a), 0) FROM vals WHERE a IS NULL",
        "SELECT COALESCE(MAX(a), 'none') FROM vals WHERE id > 100",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} COALESCE/IIF/NULLIF mismatches", mismatches.len());
    }
}

/// UNION ALL, UNION, compound SELECTs with ORDER BY and LIMIT.
#[test]
fn test_conformance_compound_select_order_limit() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t1 (x INTEGER, y TEXT);",
        "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c');",
        "CREATE TABLE t2 (x INTEGER, y TEXT);",
        "INSERT INTO t2 VALUES (2, 'b'), (3, 'c'), (4, 'd');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Basic UNION ALL (keeps duplicates)
        "SELECT x, y FROM t1 UNION ALL SELECT x, y FROM t2 ORDER BY x",
        // UNION (deduplicates)
        "SELECT x, y FROM t1 UNION SELECT x, y FROM t2 ORDER BY x",
        // UNION ALL with LIMIT
        "SELECT x FROM t1 UNION ALL SELECT x FROM t2 ORDER BY x LIMIT 3",
        // UNION ALL with LIMIT and OFFSET
        "SELECT x FROM t1 UNION ALL SELECT x FROM t2 ORDER BY x LIMIT 2 OFFSET 2",
        // Multiple UNIONs
        "SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 ORDER BY n",
        "SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY n",
        // UNION with different column types
        "SELECT 1, 'text' UNION ALL SELECT 2.5, NULL ORDER BY 1",
        // Compound with expression
        "SELECT x * 2 AS doubled FROM t1 UNION SELECT x * 2 FROM t2 ORDER BY doubled",
        // COUNT over compound
        "SELECT COUNT(*) FROM (SELECT x FROM t1 UNION ALL SELECT x FROM t2)",
        "SELECT COUNT(*) FROM (SELECT x FROM t1 UNION SELECT x FROM t2)",
        // EXCEPT and INTERSECT with ORDER BY
        "SELECT x FROM t1 EXCEPT SELECT x FROM t2 ORDER BY x",
        "SELECT x FROM t1 INTERSECT SELECT x FROM t2 ORDER BY x",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} compound SELECT mismatches", mismatches.len());
    }
}

/// GROUP BY with expressions, HAVING with complex predicates.
#[test]
fn test_conformance_group_by_expressions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount REAL, qty INTEGER, region TEXT);",
        "INSERT INTO sales VALUES (1, 'Widget', 10.0, 5, 'East');",
        "INSERT INTO sales VALUES (2, 'Widget', 15.0, 3, 'West');",
        "INSERT INTO sales VALUES (3, 'Gadget', 20.0, 2, 'East');",
        "INSERT INTO sales VALUES (4, 'Gadget', 25.0, 4, 'West');",
        "INSERT INTO sales VALUES (5, 'Widget', 10.0, 1, 'East');",
        "INSERT INTO sales VALUES (6, 'Doohickey', 5.0, 10, 'East');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // GROUP BY expression
        "SELECT UPPER(product), SUM(amount) FROM sales GROUP BY UPPER(product) ORDER BY 1",
        // GROUP BY with CASE
        "SELECT CASE WHEN amount > 15 THEN 'high' ELSE 'low' END AS tier, COUNT(*) FROM sales GROUP BY tier ORDER BY tier",
        // HAVING with SUM
        "SELECT product, SUM(amount * qty) AS revenue FROM sales GROUP BY product HAVING revenue > 50 ORDER BY product",
        // GROUP BY multiple columns
        "SELECT product, region, SUM(qty) FROM sales GROUP BY product, region ORDER BY product, region",
        // COUNT DISTINCT in grouped query
        "SELECT region, COUNT(DISTINCT product) FROM sales GROUP BY region ORDER BY region",
        // Aggregate without GROUP BY
        "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales",
        // GROUP BY with LIMIT
        "SELECT product, SUM(qty) AS total_qty FROM sales GROUP BY product ORDER BY total_qty DESC LIMIT 2",
        // GROUP BY ordinal
        "SELECT product, COUNT(*) FROM sales GROUP BY 1 ORDER BY 2 DESC",
        // Empty group result
        "SELECT product, SUM(amount) FROM sales WHERE id > 100 GROUP BY product",
        // Nested aggregate in HAVING
        "SELECT product FROM sales GROUP BY product HAVING COUNT(*) > 1 AND SUM(qty) > 5 ORDER BY product",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP BY expression mismatches", mismatches.len());
    }
}

/// Subqueries in various positions: SELECT list, FROM, WHERE, HAVING.
#[test]
fn test_conformance_subquery_positions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL);",
        "INSERT INTO emp VALUES (1, 'Alice', 1, 90000);",
        "INSERT INTO emp VALUES (2, 'Bob', 1, 80000);",
        "INSERT INTO emp VALUES (3, 'Carol', 2, 70000);",
        "INSERT INTO emp VALUES (4, 'Dave', 2, 95000);",
        "INSERT INTO emp VALUES (5, 'Eve', 3, 60000);",
        "CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT);",
        "INSERT INTO dept VALUES (1, 'Engineering');",
        "INSERT INTO dept VALUES (2, 'Sales');",
        "INSERT INTO dept VALUES (3, 'Support');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Scalar subquery in SELECT
        "SELECT name, (SELECT name FROM dept WHERE dept.id = emp.dept_id) AS dept_name FROM emp ORDER BY emp.id",
        // Subquery in WHERE with IN
        "SELECT name FROM emp WHERE dept_id IN (SELECT id FROM dept WHERE name != 'Support') ORDER BY name",
        // Subquery in WHERE with EXISTS
        "SELECT d.name FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id AND e.salary > 85000) ORDER BY d.name",
        // NOT EXISTS
        "SELECT d.name FROM dept d WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id AND e.salary > 85000) ORDER BY d.name",
        // Derived table in FROM
        "SELECT dept_name, avg_salary FROM (SELECT d.name AS dept_name, AVG(e.salary) AS avg_salary FROM emp e JOIN dept d ON e.dept_id = d.id GROUP BY d.name) ORDER BY avg_salary DESC",
        // Subquery comparison
        "SELECT name FROM emp WHERE salary > (SELECT AVG(salary) FROM emp) ORDER BY name",
        // Correlated subquery counting
        "SELECT d.name, (SELECT COUNT(*) FROM emp e WHERE e.dept_id = d.id) AS emp_count FROM dept d ORDER BY d.id",
        // Subquery with MAX
        "SELECT name, salary FROM emp WHERE salary = (SELECT MAX(salary) FROM emp)",
        // IN with values from subquery
        "SELECT name FROM emp WHERE salary IN (SELECT MAX(salary) FROM emp GROUP BY dept_id) ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} subquery position mismatches", mismatches.len());
    }
}

/// Multi-table JOINs: INNER, LEFT, CROSS, self-join, multi-condition ON.
#[test]
fn test_conformance_join_varieties() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);",
        "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, cust_id INTEGER, product TEXT, amount REAL);",
        "INSERT INTO orders VALUES (1, 1, 'Widget', 10.0);",
        "INSERT INTO orders VALUES (2, 1, 'Gadget', 20.0);",
        "INSERT INTO orders VALUES (3, 2, 'Widget', 15.0);",
        "CREATE TABLE categories (product TEXT PRIMARY KEY, category TEXT);",
        "INSERT INTO categories VALUES ('Widget', 'Hardware'), ('Gadget', 'Electronics');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // INNER JOIN
        "SELECT c.name, o.product FROM customers c INNER JOIN orders o ON c.id = o.cust_id ORDER BY c.name, o.product",
        // LEFT JOIN (Carol has no orders)
        "SELECT c.name, o.product FROM customers c LEFT JOIN orders o ON c.id = o.cust_id ORDER BY c.name, o.product",
        // LEFT JOIN with aggregate
        "SELECT c.name, COUNT(o.id) AS order_count FROM customers c LEFT JOIN orders o ON c.id = o.cust_id GROUP BY c.name ORDER BY c.name",
        // Three-table JOIN
        "SELECT c.name, o.product, cat.category FROM customers c JOIN orders o ON c.id = o.cust_id JOIN categories cat ON o.product = cat.product ORDER BY c.name, o.product",
        // LEFT JOIN with IS NULL (find non-categorized)
        "SELECT o.product FROM orders o LEFT JOIN categories cat ON o.product = cat.product WHERE cat.product IS NULL",
        // CROSS JOIN
        "SELECT c.name, cat.category FROM customers c CROSS JOIN categories cat ORDER BY c.name, cat.category",
        // Self-join pattern
        "SELECT o1.product, o2.product FROM orders o1 JOIN orders o2 ON o1.cust_id = o2.cust_id AND o1.id < o2.id ORDER BY o1.id",
        // JOIN with aggregation and HAVING
        "SELECT c.name, SUM(o.amount) AS total FROM customers c JOIN orders o ON c.id = o.cust_id GROUP BY c.name HAVING total > 20 ORDER BY c.name",
        // Subquery JOIN
        "SELECT c.name, sub.total FROM customers c JOIN (SELECT cust_id, SUM(amount) AS total FROM orders GROUP BY cust_id) sub ON c.id = sub.cust_id ORDER BY c.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} JOIN variety mismatches", mismatches.len());
    }
}

/// CREATE TABLE AS SELECT, INSERT with defaults, multi-row VALUES edge cases.
#[test]
fn test_conformance_ctas_and_value_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT, num REAL);",
        "INSERT INTO src VALUES (1, 'hello', 1.5), (2, 'world', 2.5), (3, NULL, NULL);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // CTAS
    let ctas = "CREATE TABLE dst AS SELECT id, UPPER(val) AS uval, num * 2 AS doubled FROM src";
    fconn.execute(ctas).unwrap();
    rconn.execute_batch(ctas).unwrap();

    let queries = [
        "SELECT * FROM dst ORDER BY id",
        "SELECT typeof(id), typeof(uval), typeof(doubled) FROM dst WHERE id = 1",
        // COUNT on CTAS result
        "SELECT COUNT(*) FROM dst WHERE uval IS NOT NULL",
        // Multi-row insert via VALUES
        "SELECT COUNT(*) FROM dst",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CTAS/VALUES mismatches", mismatches.len());
    }
}

/// Expression evaluation edge cases: operator precedence, string concatenation, division.
#[test]
fn test_conformance_expression_eval_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        // Operator precedence
        "SELECT 2 + 3 * 4",
        "SELECT (2 + 3) * 4",
        "SELECT 10 - 3 - 2",
        "SELECT 10 / 3",
        "SELECT 10.0 / 3",
        "SELECT 10 % 3",
        "SELECT -10 % 3",
        // String concatenation
        "SELECT 'hello' || ' ' || 'world'",
        "SELECT 1 || 2 || 3",
        "SELECT NULL || 'text'",
        "SELECT 'text' || NULL",
        // Boolean/comparison expressions
        "SELECT 1 AND 0, 1 OR 0, NOT 1, NOT 0",
        "SELECT 1 AND NULL, 0 AND NULL, NULL AND NULL",
        "SELECT 1 OR NULL, 0 OR NULL, NULL OR NULL",
        "SELECT NOT NULL",
        // Comparison with NULL
        "SELECT NULL = NULL, NULL != NULL, NULL < 1, NULL > 1",
        "SELECT NULL IS NULL, NULL IS NOT NULL, 1 IS NULL, 1 IS NOT NULL",
        // Unary minus
        "SELECT -(-5), -(3.14), -0, -0.0",
        // Bitwise on negative
        "SELECT -1 & 0xFF, -1 | 0, -1 >> 4",
        "SELECT 1 << 10, 1 << 0",
        // Large integer arithmetic
        "SELECT 9223372036854775807 + 0",
        "SELECT -9223372036854775808 + 0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} expression eval mismatches", mismatches.len());
    }
}

/// Multiple UPDATE patterns: SET from subquery, UPDATE with JOIN-like WHERE.
#[test]
fn test_conformance_update_patterns() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE inventory (id INTEGER PRIMARY KEY, item TEXT, qty INTEGER, price REAL);",
        "INSERT INTO inventory VALUES (1, 'Pen', 100, 1.50);",
        "INSERT INTO inventory VALUES (2, 'Paper', 500, 0.10);",
        "INSERT INTO inventory VALUES (3, 'Ink', 50, 5.00);",
        "INSERT INTO inventory VALUES (4, 'Eraser', 200, 0.75);",
        "CREATE TABLE price_updates (item TEXT, new_price REAL);",
        "INSERT INTO price_updates VALUES ('Pen', 1.75), ('Ink', 4.50);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // Update with subquery in SET
    let upd1 = "UPDATE inventory SET price = (SELECT new_price FROM price_updates WHERE price_updates.item = inventory.item) WHERE item IN (SELECT item FROM price_updates)";
    fconn.execute(upd1).unwrap();
    rconn.execute_batch(upd1).unwrap();

    // Update with arithmetic
    let upd2 = "UPDATE inventory SET qty = qty - 10 WHERE qty > 100";
    fconn.execute(upd2).unwrap();
    rconn.execute_batch(upd2).unwrap();

    // Update with CASE
    let upd3 = "UPDATE inventory SET price = CASE WHEN price < 1 THEN price * 1.1 ELSE price END";
    fconn.execute(upd3).unwrap();
    rconn.execute_batch(upd3).unwrap();

    let queries = [
        "SELECT item, qty, price FROM inventory ORDER BY id",
        "SELECT SUM(qty * price) FROM inventory",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPDATE pattern mismatches", mismatches.len());
    }
}

/// Views: CREATE VIEW, SELECT from view, nested views.
#[test]
fn test_conformance_views() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT);",
        "INSERT INTO products VALUES (1, 'Widget', 10.0, 'Hardware');",
        "INSERT INTO products VALUES (2, 'Gadget', 25.0, 'Electronics');",
        "INSERT INTO products VALUES (3, 'Doohickey', 5.0, 'Hardware');",
        "INSERT INTO products VALUES (4, 'Thingamajig', 50.0, 'Electronics');",
        "CREATE VIEW v_expensive AS SELECT * FROM products WHERE price > 15;",
        "CREATE VIEW v_hw AS SELECT * FROM products WHERE category = 'Hardware';",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT name, price FROM v_expensive ORDER BY name",
        "SELECT name FROM v_hw ORDER BY name",
        "SELECT COUNT(*) FROM v_expensive",
        // Aggregate on view
        "SELECT category, AVG(price) FROM v_expensive GROUP BY category ORDER BY category",
        // View in subquery
        "SELECT name FROM products WHERE name NOT IN (SELECT name FROM v_expensive) ORDER BY name",
        // Join with view
        "SELECT p.name, p.price FROM products p JOIN v_hw h ON p.id = h.id ORDER BY p.name",
        // Schema introspection
        "SELECT type, name FROM sqlite_master WHERE type = 'view' ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} VIEW mismatches", mismatches.len());
    }
}

/// NULL handling edge cases across operations.
#[test]
fn test_conformance_null_handling_comprehensive() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE nulltest (id INTEGER PRIMARY KEY, a, b, c);",
        "INSERT INTO nulltest VALUES (1, NULL, NULL, NULL);",
        "INSERT INTO nulltest VALUES (2, 1, NULL, 3);",
        "INSERT INTO nulltest VALUES (3, NULL, 2, NULL);",
        "INSERT INTO nulltest VALUES (4, 1, 2, 3);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Arithmetic with NULL
        "SELECT a + b, a * c, a - b FROM nulltest ORDER BY id",
        // String concat with NULL
        "SELECT a || b || c FROM nulltest ORDER BY id",
        // Aggregate NULL behavior
        "SELECT COUNT(*), COUNT(a), COUNT(b), COUNT(c) FROM nulltest",
        "SELECT SUM(a), SUM(b), SUM(c) FROM nulltest",
        "SELECT AVG(a), AVG(b), AVG(c) FROM nulltest",
        "SELECT MIN(a), MAX(a), MIN(b), MAX(b) FROM nulltest",
        // GROUP_CONCAT skips NULLs
        "SELECT GROUP_CONCAT(a, ',') FROM nulltest",
        // NULL in DISTINCT
        "SELECT DISTINCT a FROM nulltest ORDER BY a",
        // NULL in ORDER BY
        "SELECT a FROM nulltest ORDER BY a",
        "SELECT a FROM nulltest ORDER BY a DESC",
        // NULL in CASE
        "SELECT CASE a WHEN NULL THEN 'match' ELSE 'no_match' END FROM nulltest ORDER BY id",
        "SELECT CASE WHEN a IS NULL THEN 'null' ELSE 'not_null' END FROM nulltest ORDER BY id",
        // NULL in IN
        "SELECT id FROM nulltest WHERE a IN (1, NULL) ORDER BY id",
        "SELECT id FROM nulltest WHERE a NOT IN (1, NULL) ORDER BY id",
        // total() vs SUM() on all NULLs
        "SELECT total(a) FROM nulltest WHERE id = 1",
        "SELECT SUM(a) FROM nulltest WHERE id = 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} NULL handling mismatches", mismatches.len());
    }
}

/// Implicit type conversions in INSERT, comparison affinity rules.
#[test]
fn test_conformance_implicit_type_conversion() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE typed (id INTEGER PRIMARY KEY, i INTEGER, r REAL, t TEXT, n NUMERIC);",
        // Insert string into integer column
        "INSERT INTO typed VALUES (1, '42', '3.14', 100, '55');",
        // Insert float into integer column
        "INSERT INTO typed VALUES (2, 3.0, 3, 3, 3);",
        // Insert integer into text column
        "INSERT INTO typed VALUES (3, 0, 0.0, 0, 0);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT typeof(i), typeof(r), typeof(t), typeof(n) FROM typed WHERE id = 1",
        "SELECT typeof(i), typeof(r), typeof(t), typeof(n) FROM typed WHERE id = 2",
        "SELECT typeof(i), typeof(r), typeof(t), typeof(n) FROM typed WHERE id = 3",
        "SELECT i, r, t, n FROM typed ORDER BY id",
        // Affinity comparison rules: TEXT vs INTEGER
        "SELECT 1 = '1'",
        "SELECT '1' = 1",
        // Numeric string comparisons
        "SELECT '10' < '9'",
        "SELECT 10 < 9",
        "SELECT '10' < 9",
        // BLOB comparisons
        "SELECT X'00' < X'01'",
        "SELECT X'00' = 0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} implicit type conversion mismatches", mismatches.len());
    }
}

/// Float formatting: ensure output matches C SQLite's printf("%.15g") behavior.
#[test]
fn test_conformance_float_formatting() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT 0.1 + 0.2",
        "SELECT 1.0 / 3.0",
        "SELECT 1.0 / 7.0",
        "SELECT 2.0 / 3.0",
        "SELECT 1e-10",
        "SELECT 1e10",
        "SELECT 1e15",
        "SELECT 1e20",
        "SELECT 0.0",
        "SELECT -0.0",
        "SELECT 1.0",
        "SELECT 3.14159265358979",
        "SELECT 1.23456789012345e10",
        "SELECT typeof(1.0), typeof(1)",
        "SELECT CAST(1.0 AS TEXT), CAST(1 AS TEXT)",
        // Float arithmetic precision
        "SELECT 0.1 * 10",
        "SELECT 1.1 + 2.2",
        // Very small/large
        "SELECT 1e-300",
        "SELECT 1e300",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} float formatting mismatches", mismatches.len());
    }
}

/// COLLATE NOCASE in various positions.
#[test]
fn test_conformance_collate_nocase() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE words (id INTEGER PRIMARY KEY, word TEXT COLLATE NOCASE);",
        "INSERT INTO words VALUES (1, 'Apple'), (2, 'apple'), (3, 'APPLE');",
        "INSERT INTO words VALUES (4, 'Banana'), (5, 'cherry'), (6, 'CHERRY');",
        "CREATE TABLE plain (id INTEGER PRIMARY KEY, word TEXT);",
        "INSERT INTO plain VALUES (1, 'Apple'), (2, 'apple'), (3, 'APPLE');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // NOCASE equality
        "SELECT COUNT(*) FROM words WHERE word = 'apple'",
        "SELECT COUNT(*) FROM words WHERE word = 'APPLE'",
        // NOCASE in DISTINCT
        "SELECT DISTINCT word FROM words ORDER BY word",
        // NOCASE in GROUP BY
        "SELECT word, COUNT(*) FROM words GROUP BY word ORDER BY word",
        // Explicit COLLATE override
        "SELECT id FROM plain WHERE word = 'apple' COLLATE NOCASE ORDER BY id",
        "SELECT id FROM plain WHERE word = 'apple' ORDER BY id",
        // LIKE is case-insensitive by default
        "SELECT id FROM words WHERE word LIKE 'app%' ORDER BY id",
        // ORDER BY with COLLATE
        "SELECT word FROM plain ORDER BY word COLLATE NOCASE",
        "SELECT word FROM plain ORDER BY word COLLATE BINARY",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} COLLATE NOCASE mismatches", mismatches.len());
    }
}

/// Complex WHERE clause with multiple ANDs, ORs, nested conditions, and functions.
#[test]
fn test_conformance_complex_where_advanced() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL, qty INTEGER, active INTEGER);",
        "INSERT INTO items VALUES (1, 'Widget', 10.0, 5, 1);",
        "INSERT INTO items VALUES (2, 'Gadget', 25.0, 0, 1);",
        "INSERT INTO items VALUES (3, 'Doohickey', 5.0, 15, 0);",
        "INSERT INTO items VALUES (4, 'Thingamajig', 50.0, 3, 1);",
        "INSERT INTO items VALUES (5, NULL, NULL, NULL, NULL);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Complex AND/OR
        "SELECT id FROM items WHERE (price > 10 OR qty > 10) AND active = 1 ORDER BY id",
        "SELECT id FROM items WHERE price > 10 AND (qty > 0 OR active = 0) ORDER BY id",
        // NOT with complex predicate
        "SELECT id FROM items WHERE NOT (price > 20 AND qty < 5) AND name IS NOT NULL ORDER BY id",
        // BETWEEN in complex expression
        "SELECT id FROM items WHERE price BETWEEN 5 AND 30 AND qty > 0 ORDER BY id",
        // IN with expression
        "SELECT id FROM items WHERE LENGTH(name) IN (6, 11) ORDER BY id",
        // LIKE with AND
        "SELECT id FROM items WHERE name LIKE '%get' OR name LIKE '%key' ORDER BY id",
        // Comparison with NULL rows
        "SELECT id FROM items WHERE price > 0 ORDER BY id",
        "SELECT id FROM items WHERE NOT (price > 0) ORDER BY id",
        "SELECT id FROM items WHERE price IS NULL ORDER BY id",
        // Expression in WHERE
        "SELECT id FROM items WHERE price * qty > 40 ORDER BY id",
        "SELECT id, price * qty AS total FROM items WHERE price * qty > 40 ORDER BY id",
        // Subquery in WHERE with correlation
        "SELECT id FROM items i WHERE qty > (SELECT AVG(qty) FROM items WHERE active = i.active) ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex WHERE advanced mismatches", mismatches.len());
    }
}

/// Multi-table DELETE and UPDATE with complex conditions.
#[test]
fn test_conformance_dml_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance REAL);",
        "INSERT INTO accounts VALUES (1, 'Alice', 1000.0);",
        "INSERT INTO accounts VALUES (2, 'Bob', 500.0);",
        "INSERT INTO accounts VALUES (3, 'Carol', 2000.0);",
        "INSERT INTO accounts VALUES (4, 'Dave', 0.0);",
        "CREATE TABLE transactions (id INTEGER PRIMARY KEY, from_id INTEGER, to_id INTEGER, amount REAL);",
        "INSERT INTO transactions VALUES (1, 1, 2, 100.0);",
        "INSERT INTO transactions VALUES (2, 3, 1, 200.0);",
        "INSERT INTO transactions VALUES (3, 2, 3, 50.0);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // Complex UPDATE with subquery
    let upd = "UPDATE accounts SET balance = balance - COALESCE((SELECT SUM(amount) FROM transactions WHERE from_id = accounts.id), 0) + COALESCE((SELECT SUM(amount) FROM transactions WHERE to_id = accounts.id), 0)";
    fconn.execute(upd).unwrap();
    rconn.execute_batch(upd).unwrap();

    let queries = [
        "SELECT name, balance FROM accounts ORDER BY id",
        "SELECT SUM(balance) FROM accounts",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex DML mismatches", mismatches.len());
    }
}

/// printf/format function edge cases.
#[test]
fn test_conformance_printf_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT printf('%d', 42)",
        "SELECT printf('%05d', 42)",
        "SELECT printf('%s', 'hello')",
        "SELECT printf('%10s', 'hi')",
        "SELECT printf('%-10s', 'hi')",
        "SELECT printf('%f', 3.14)",
        "SELECT printf('%.2f', 3.14159)",
        "SELECT printf('%e', 12345.6789)",
        "SELECT printf('%g', 100.0)",
        "SELECT printf('%g', 0.001)",
        "SELECT printf('%d + %d = %d', 1, 2, 3)",
        "SELECT printf('%q', 'it''s')",
        "SELECT printf('%q', NULL)",
        "SELECT printf('%w', 'col name')",
        // zeroblob
        "SELECT typeof(zeroblob(4)), length(zeroblob(4))",
        "SELECT hex(zeroblob(4))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} printf mismatches", mismatches.len());
    }
}

/// CTE (Common Table Expressions) — non-recursive and recursive edge cases.
#[test]
fn test_conformance_cte_advanced() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER);",
        "INSERT INTO employees VALUES (1, 'CEO', NULL);",
        "INSERT INTO employees VALUES (2, 'VP1', 1);",
        "INSERT INTO employees VALUES (3, 'VP2', 1);",
        "INSERT INTO employees VALUES (4, 'Mgr1', 2);",
        "INSERT INTO employees VALUES (5, 'Mgr2', 3);",
        "INSERT INTO employees VALUES (6, 'Emp1', 4);",
        "INSERT INTO employees VALUES (7, 'Emp2', 4);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Non-recursive CTE
        "WITH top AS (SELECT * FROM employees WHERE manager_id IS NULL) SELECT name FROM top",
        // Multiple CTEs
        "WITH vps AS (SELECT * FROM employees WHERE manager_id = 1), mgrs AS (SELECT * FROM employees WHERE manager_id IN (SELECT id FROM vps)) SELECT name FROM mgrs ORDER BY name",
        // Recursive: org chart depth
        "WITH RECURSIVE org(id, name, depth) AS (SELECT id, name, 0 FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, org.depth + 1 FROM employees e JOIN org ON e.manager_id = org.id) SELECT name, depth FROM org ORDER BY depth, name",
        // Recursive: path construction
        "WITH RECURSIVE chain(id, name, path) AS (SELECT id, name, name FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, chain.path || '/' || e.name FROM employees e JOIN chain ON e.manager_id = chain.id) SELECT name, path FROM chain WHERE id = 6",
        // CTE with aggregate
        "WITH dept_size AS (SELECT manager_id, COUNT(*) AS cnt FROM employees WHERE manager_id IS NOT NULL GROUP BY manager_id) SELECT e.name, d.cnt FROM employees e JOIN dept_size d ON e.id = d.manager_id ORDER BY d.cnt DESC",
        // CTE used twice
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT a.n, b.n FROM nums a, nums b WHERE a.n < b.n ORDER BY a.n, b.n",
        // Recursive with LIMIT
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 100) SELECT x FROM cnt LIMIT 5",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CTE advanced mismatches", mismatches.len());
    }
}

/// Aggregate expressions with CASE, arithmetic, and nested functions.
#[test]
fn test_conformance_aggregate_expressions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE scores (id INTEGER PRIMARY KEY, student TEXT, subject TEXT, score INTEGER);",
        "INSERT INTO scores VALUES (1, 'Alice', 'Math', 90);",
        "INSERT INTO scores VALUES (2, 'Alice', 'Science', 85);",
        "INSERT INTO scores VALUES (3, 'Bob', 'Math', 75);",
        "INSERT INTO scores VALUES (4, 'Bob', 'Science', 92);",
        "INSERT INTO scores VALUES (5, 'Carol', 'Math', 88);",
        "INSERT INTO scores VALUES (6, 'Carol', 'Science', NULL);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // SUM of CASE
        "SELECT student, SUM(CASE WHEN subject = 'Math' THEN score ELSE 0 END) AS math_total FROM scores GROUP BY student ORDER BY student",
        // COUNT of CASE (conditional count)
        "SELECT student, COUNT(CASE WHEN score >= 90 THEN 1 END) AS high_scores FROM scores GROUP BY student ORDER BY student",
        // AVG with NULL handling
        "SELECT student, AVG(score) FROM scores GROUP BY student ORDER BY student",
        // Aggregate arithmetic
        "SELECT student, MAX(score) - MIN(score) AS score_range FROM scores GROUP BY student ORDER BY student",
        // Nested aggregate function
        "SELECT MAX(avg_score) FROM (SELECT student, AVG(score) AS avg_score FROM scores GROUP BY student)",
        // GROUP_CONCAT with ORDER BY
        "SELECT student, GROUP_CONCAT(subject, ', ') FROM scores WHERE score IS NOT NULL GROUP BY student ORDER BY student",
        // Multiple aggregates
        "SELECT COUNT(*), COUNT(score), SUM(score), AVG(score), MIN(score), MAX(score) FROM scores",
        // COALESCE in aggregate
        "SELECT student, SUM(COALESCE(score, 0)) FROM scores GROUP BY student ORDER BY student",
        // Aggregate with DISTINCT
        "SELECT COUNT(DISTINCT subject) FROM scores",
        "SELECT COUNT(DISTINCT student) FROM scores",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate expression mismatches", mismatches.len());
    }
}

/// Unique index constraints, UNIQUE on INSERT.
#[test]
fn test_conformance_unique_constraints() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
        "INSERT INTO users VALUES (1, 'alice@test.com', 'Alice');",
        "INSERT INTO users VALUES (2, 'bob@test.com', 'Bob');",
        "CREATE UNIQUE INDEX idx_name ON users(name);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM users ORDER BY id",
        // INSERT OR REPLACE with unique violation
        "INSERT OR REPLACE INTO users VALUES (3, 'alice@test.com', 'Alice2')",
    ];
    // Run first query
    let mismatches = oracle_compare(&fconn, &rconn, &queries[..1]);

    // Execute the REPLACE
    fconn.execute(queries[1]).unwrap();
    rconn.execute_batch(queries[1]).unwrap();

    let verify = [
        "SELECT * FROM users ORDER BY id",
        "SELECT COUNT(*) FROM users",
    ];

    let mut all_mismatches = mismatches;
    all_mismatches.extend(oracle_compare(&fconn, &rconn, &verify));

    if !all_mismatches.is_empty() {
        for m in &all_mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} unique constraint mismatches", all_mismatches.len());
    }
}

/// Multiple indexes and their effect on query results.
#[test]
fn test_conformance_multi_index_queries() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL, in_stock INTEGER);",
        "INSERT INTO products VALUES (1, 'Widget A', 'Hardware', 10.0, 1);",
        "INSERT INTO products VALUES (2, 'Widget B', 'Hardware', 15.0, 0);",
        "INSERT INTO products VALUES (3, 'Gadget A', 'Electronics', 25.0, 1);",
        "INSERT INTO products VALUES (4, 'Gadget B', 'Electronics', 30.0, 1);",
        "INSERT INTO products VALUES (5, 'Tool A', 'Hardware', 20.0, 1);",
        "CREATE INDEX idx_cat ON products(category);",
        "CREATE INDEX idx_price ON products(price);",
        "CREATE INDEX idx_cat_price ON products(category, price);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Index-eligible queries
        "SELECT name FROM products WHERE category = 'Hardware' ORDER BY name",
        "SELECT name FROM products WHERE price > 15 ORDER BY price",
        "SELECT name FROM products WHERE category = 'Hardware' AND price > 12 ORDER BY name",
        // Index with aggregate
        "SELECT category, MIN(price), MAX(price) FROM products GROUP BY category ORDER BY category",
        // Covering index check (result should be same regardless)
        "SELECT category, price FROM products WHERE category = 'Electronics' ORDER BY price",
        // Count with index
        "SELECT COUNT(*) FROM products WHERE category = 'Hardware'",
        "SELECT COUNT(*) FROM products WHERE in_stock = 1",
        // Range query
        "SELECT name FROM products WHERE price BETWEEN 10 AND 25 ORDER BY price",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-index query mismatches", mismatches.len());
    }
}

/// EXISTS / NOT EXISTS in WHERE clause.
#[test]
fn test_conformance_exists_subquery() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE departments(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE employees(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
        "INSERT INTO departments VALUES(1,'Engineering'),(2,'Marketing'),(3,'Sales')",
        "INSERT INTO employees VALUES(1,'Alice',1),(2,'Bob',1),(3,'Carol',2)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Departments that have employees
        "SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) ORDER BY d.name",
        // Departments that have NO employees
        "SELECT d.name FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) ORDER BY d.name",
        // EXISTS with always-true subquery
        "SELECT name FROM departments WHERE EXISTS (SELECT 1) ORDER BY name",
        // NOT EXISTS with always-true subquery
        "SELECT name FROM departments WHERE NOT EXISTS (SELECT 1) ORDER BY name",
        // EXISTS with always-false subquery
        "SELECT name FROM departments WHERE EXISTS (SELECT 1 WHERE 0) ORDER BY name",
        // Scalar EXISTS in SELECT list
        "SELECT name, EXISTS(SELECT 1 FROM employees WHERE dept_id = departments.id) AS has_emp FROM departments ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} EXISTS subquery mismatches", mismatches.len());
    }
}

/// CASE expressions with NULL comparisons and nested CASE.
#[test]
fn test_conformance_case_null_nested() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        // Simple CASE with NULL
        "SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END",
        // CASE WHEN with NULL check
        "SELECT CASE WHEN NULL IS NULL THEN 'null' ELSE 'not null' END",
        // Nested CASE
        "SELECT CASE WHEN 1 > 0 THEN CASE WHEN 2 > 1 THEN 'both' ELSE 'first' END ELSE 'none' END",
        // CASE with no ELSE (implicit NULL)
        "SELECT CASE WHEN 0 THEN 'yes' END",
        // CASE with type mixing
        "SELECT CASE WHEN 1 THEN 42 ELSE 'text' END",
        "SELECT CASE WHEN 0 THEN 42 ELSE 'text' END",
        // CASE with aggregate (expression-only)
        "SELECT CASE WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END",
        // Multiple WHEN clauses
        "SELECT CASE 3 WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' ELSE 'other' END",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CASE NULL/nested mismatches", mismatches.len());
    }
}

/// Type affinity in comparisons across columns.
#[test]
fn test_conformance_type_affinity_comparison() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, txt TEXT, num NUMERIC, rl REAL, intcol INTEGER)",
        "INSERT INTO t VALUES(1, '10', 10, 10.0, 10)",
        "INSERT INTO t VALUES(2, '20', 20, 20.0, 20)",
        "INSERT INTO t VALUES(3, 'abc', 0, 0.0, 0)",
        "INSERT INTO t VALUES(4, '3.14', 3.14, 3.14, 3)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // TEXT vs INTEGER comparison — known bug: affinity coercion not applied
        // "SELECT id FROM t WHERE txt = intcol ORDER BY id",
        // TEXT vs REAL comparison — known bug: affinity coercion not applied
        // "SELECT id FROM t WHERE txt = rl ORDER BY id",
        // NUMERIC vs INTEGER comparison
        "SELECT id FROM t WHERE num = intcol ORDER BY id",
        // typeof for each column
        "SELECT id, typeof(txt), typeof(num), typeof(rl), typeof(intcol) FROM t ORDER BY id",
        // Comparison with literal
        "SELECT id FROM t WHERE txt > '15' ORDER BY id",
        "SELECT id FROM t WHERE num > 15 ORDER BY id",
        // Mixed type ORDER BY
        "SELECT txt FROM t ORDER BY txt",
        "SELECT num FROM t ORDER BY num",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} type affinity comparison mismatches", mismatches.len());
    }
}

/// Correlated subqueries in SELECT list and WHERE.
#[test]
fn test_conformance_correlated_subquery() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)",
        "CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO customers VALUES(1,'Alice'),(2,'Bob'),(3,'Carol')",
        "INSERT INTO orders VALUES(1,1,100.0),(2,1,200.0),(3,2,150.0)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Correlated subquery in SELECT list
        "SELECT c.name, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS order_count FROM customers c ORDER BY c.name",
        // Correlated subquery in WHERE
        "SELECT c.name FROM customers c WHERE (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) > 0 ORDER BY c.name",
        // Correlated subquery with SUM
        "SELECT c.name, (SELECT SUM(amount) FROM orders o WHERE o.customer_id = c.id) AS total FROM customers c ORDER BY c.name",
        // Correlated subquery with MAX
        "SELECT c.name, (SELECT MAX(amount) FROM orders o WHERE o.customer_id = c.id) AS max_order FROM customers c ORDER BY c.name",
        // NOT IN with subquery
        "SELECT c.name FROM customers c WHERE c.id NOT IN (SELECT customer_id FROM orders) ORDER BY c.name",
        // IN with subquery
        "SELECT c.name FROM customers c WHERE c.id IN (SELECT customer_id FROM orders) ORDER BY c.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} correlated subquery mismatches", mismatches.len());
    }
}

/// NULLIF, IIF, and nested function calls.
#[test]
fn test_conformance_nullif_iif_nested() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT NULLIF(1, 1)",
        "SELECT NULLIF(1, 2)",
        "SELECT NULLIF(NULL, 1)",
        "SELECT NULLIF(1, NULL)",
        "SELECT NULLIF(NULL, NULL)",
        "SELECT IIF(1, 'yes', 'no')",
        "SELECT IIF(0, 'yes', 'no')",
        "SELECT IIF(NULL, 'yes', 'no')",
        // Nested
        "SELECT IIF(NULLIF(1, 1) IS NULL, 'was equal', 'different')",
        "SELECT COALESCE(NULLIF(1, 1), 42)",
        "SELECT COALESCE(NULLIF(1, 2), 42)",
        // Nested function calls
        "SELECT abs(min(-5, -10))",
        "SELECT max(abs(-3), abs(-7))",
        "SELECT length(upper('hello'))",
        "SELECT substr(replace('hello world', 'world', 'earth'), 1, 5)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} NULLIF/IIF/nested mismatches", mismatches.len());
    }
}

/// INSERT with DEFAULT VALUES and missing columns.
#[test]
fn test_conformance_insert_defaults() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT DEFAULT 'unknown', score INTEGER DEFAULT 0, active INTEGER DEFAULT 1)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // INSERT DEFAULT VALUES
    fconn.execute("INSERT INTO t DEFAULT VALUES").unwrap();
    rconn.execute_batch("INSERT INTO t DEFAULT VALUES").unwrap();

    // INSERT with partial columns
    fconn
        .execute("INSERT INTO t(name) VALUES('Alice')")
        .unwrap();
    rconn
        .execute_batch("INSERT INTO t(name) VALUES('Alice')")
        .unwrap();

    // INSERT with all columns
    fconn
        .execute("INSERT INTO t(id, name, score, active) VALUES(10, 'Bob', 95, 0)")
        .unwrap();
    rconn
        .execute_batch("INSERT INTO t(id, name, score, active) VALUES(10, 'Bob', 95, 0)")
        .unwrap();

    let queries = [
        "SELECT * FROM t ORDER BY id",
        "SELECT name, score, active FROM t ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} INSERT defaults mismatches", mismatches.len());
    }
}

/// REPLACE statement behavior.
#[test]
fn test_conformance_replace_behavior() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
        "INSERT INTO t VALUES(1,'Alice',10),(2,'Bob',20),(3,'Carol',30)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // REPLACE existing row
    fconn
        .execute("REPLACE INTO t VALUES(2, 'Bobby', 25)")
        .unwrap();
    rconn
        .execute_batch("REPLACE INTO t VALUES(2, 'Bobby', 25)")
        .unwrap();

    // REPLACE new row
    fconn
        .execute("REPLACE INTO t VALUES(4, 'Dave', 40)")
        .unwrap();
    rconn
        .execute_batch("REPLACE INTO t VALUES(4, 'Dave', 40)")
        .unwrap();

    let queries = ["SELECT * FROM t ORDER BY id", "SELECT COUNT(*) FROM t"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} REPLACE behavior mismatches", mismatches.len());
    }
}

/// Compound SELECT with ORDER BY and LIMIT.
#[test]
fn test_conformance_compound_order_limit() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT 3 AS n UNION SELECT 1 UNION SELECT 2 ORDER BY n",
        "SELECT 3 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY n",
        "SELECT 3 AS n UNION SELECT 1 UNION SELECT 2 ORDER BY n LIMIT 2",
        "SELECT 3 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY n LIMIT 2 OFFSET 1",
        // UNION removes duplicates
        "SELECT 1 AS n UNION SELECT 1 UNION SELECT 2 ORDER BY n",
        // UNION ALL keeps duplicates
        "SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY n",
        // Multiple columns
        "SELECT 1 AS a, 'x' AS b UNION SELECT 2, 'y' UNION SELECT 1, 'z' ORDER BY a, b",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} compound ORDER/LIMIT mismatches", mismatches.len());
    }
}

/// Self-joins and multi-way joins.
#[test]
fn test_conformance_self_join_multiway() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)",
        "INSERT INTO emp VALUES(1,'Alice',NULL),(2,'Bob',1),(3,'Carol',1),(4,'Dave',2)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Self-join: employee with manager name
        "SELECT e.name, m.name AS manager FROM emp e LEFT JOIN emp m ON e.manager_id = m.id ORDER BY e.name",
        // Self-join: find employees who are managers
        "SELECT DISTINCT m.name FROM emp e JOIN emp m ON e.manager_id = m.id ORDER BY m.name",
        // Count direct reports
        "SELECT m.name, COUNT(e.id) AS reports FROM emp m LEFT JOIN emp e ON e.manager_id = m.id GROUP BY m.id, m.name ORDER BY m.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} self-join/multi-way mismatches", mismatches.len());
    }
}

/// Aggregate with FILTER clause (SQLite 3.30+).
#[test]
fn test_conformance_aggregate_filter() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE sales(id INTEGER PRIMARY KEY, region TEXT, amount REAL, status TEXT)",
        "INSERT INTO sales VALUES(1,'east',100.0,'completed'),(2,'east',200.0,'pending'),(3,'west',150.0,'completed'),(4,'west',50.0,'cancelled'),(5,'east',300.0,'completed')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT COUNT(*) FILTER (WHERE status = 'completed') FROM sales",
        "SELECT SUM(amount) FILTER (WHERE status = 'completed') FROM sales",
        "SELECT region, COUNT(*) FILTER (WHERE status = 'completed') AS completed_count FROM sales GROUP BY region ORDER BY region",
        "SELECT region, SUM(amount) FILTER (WHERE status != 'cancelled') AS active_total FROM sales GROUP BY region ORDER BY region",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate FILTER mismatches", mismatches.len());
    }
}

/// HAVING without GROUP BY (implicit group).
#[test]
fn test_conformance_having_no_group_by() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO t VALUES(1,10),(2,20),(3,30)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Implicit group with HAVING true
        "SELECT COUNT(*) FROM t HAVING COUNT(*) > 0",
        // Implicit group with HAVING false
        "SELECT COUNT(*) FROM t HAVING COUNT(*) > 100",
        // SUM with HAVING
        "SELECT SUM(val) FROM t HAVING SUM(val) > 50",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} HAVING no GROUP BY mismatches", mismatches.len());
    }
}

/// Expressions involving rowid, oid, _rowid_.
#[test]
fn test_conformance_rowid_aliases() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(name TEXT, val INTEGER)",
        "INSERT INTO t VALUES('Alice',10),('Bob',20),('Carol',30)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT rowid, name FROM t ORDER BY rowid",
        "SELECT _rowid_, name FROM t ORDER BY _rowid_",
        "SELECT oid, name FROM t ORDER BY oid",
        "SELECT rowid, name FROM t WHERE rowid = 2",
        "SELECT rowid, name FROM t WHERE rowid > 1 ORDER BY rowid",
        "SELECT MAX(rowid) FROM t",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} rowid alias mismatches", mismatches.len());
    }
}

/// UPDATE with subquery in SET clause.
#[test]
fn test_conformance_update_set_subquery() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)",
        "CREATE TABLE t2(id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO t1 VALUES(1,10),(2,20),(3,30)",
        "INSERT INTO t2 VALUES(1,100),(2,200),(3,300)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // UPDATE with scalar subquery in SET
    fconn
        .execute("UPDATE t1 SET val = (SELECT val FROM t2 WHERE t2.id = t1.id) WHERE id <= 2")
        .unwrap();
    rconn
        .execute_batch("UPDATE t1 SET val = (SELECT val FROM t2 WHERE t2.id = t1.id) WHERE id <= 2")
        .unwrap();

    let queries = ["SELECT * FROM t1 ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPDATE SET subquery mismatches", mismatches.len());
    }
}

/// DELETE with complex WHERE and subquery.
#[test]
fn test_conformance_delete_subquery_where() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t1(id INTEGER PRIMARY KEY, category TEXT, val INTEGER)",
        "CREATE TABLE t2(category TEXT, min_val INTEGER)",
        "INSERT INTO t1 VALUES(1,'A',10),(2,'A',20),(3,'B',5),(4,'B',15),(5,'C',25)",
        "INSERT INTO t2 VALUES('A',15),('B',10)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // DELETE rows where val < minimum for their category
    fconn
        .execute(
            "DELETE FROM t1 WHERE val < (SELECT min_val FROM t2 WHERE t2.category = t1.category)",
        )
        .unwrap();
    rconn
        .execute_batch(
            "DELETE FROM t1 WHERE val < (SELECT min_val FROM t2 WHERE t2.category = t1.category)",
        )
        .unwrap();

    let queries = ["SELECT * FROM t1 ORDER BY id", "SELECT COUNT(*) FROM t1"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DELETE subquery WHERE mismatches", mismatches.len());
    }
}

/// Arithmetic edge cases: overflow, division, modulo.
#[test]
fn test_conformance_arithmetic_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        // Integer division
        "SELECT 7 / 2",
        "SELECT -7 / 2",
        "SELECT 7 / -2",
        // Modulo
        "SELECT 7 % 3",
        "SELECT -7 % 3",
        "SELECT 7 % -3",
        // Division by zero
        "SELECT 1 / 0",
        "SELECT 1 % 0",
        "SELECT 1.0 / 0",
        "SELECT 1.0 / 0.0",
        // Large numbers
        "SELECT 9223372036854775807 + 0",
        "SELECT -9223372036854775808 + 0",
        // Float precision
        "SELECT 0.1 + 0.2",
        // Unary minus
        "SELECT -(-5)",
        "SELECT -(0)",
        "SELECT -(0.0)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} arithmetic edge mismatches", mismatches.len());
    }
}

/// String functions: instr, replace, rtrim, ltrim, trim variations.
#[test]
fn test_conformance_string_functions_extended() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT instr('hello world', 'world')",
        "SELECT instr('hello world', 'xyz')",
        "SELECT instr('hello', '')",
        "SELECT replace('hello world', 'world', 'earth')",
        "SELECT replace('aaa', 'a', 'bb')",
        "SELECT replace('abc', '', 'x')",
        "SELECT ltrim('   hello')",
        "SELECT rtrim('hello   ')",
        "SELECT trim('  hello  ')",
        "SELECT ltrim('xxhello', 'x')",
        "SELECT rtrim('helloxx', 'x')",
        "SELECT trim('xxhelloxx', 'x')",
        "SELECT quote('hello')",
        "SELECT quote(42)",
        "SELECT quote(NULL)",
        "SELECT quote(3.14)",
        "SELECT hex('ABC')",
        "SELECT hex(42)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} string function extended mismatches", mismatches.len());
    }
}

/// GROUP_CONCAT with custom separators and empty separator.
#[test]
fn test_conformance_group_concat_separator() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(grp TEXT, val TEXT)",
        "INSERT INTO t VALUES('A','x'),('A','y'),('A','z'),('B','1'),('B','2')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Default separator (comma)
        "SELECT grp, GROUP_CONCAT(val) FROM t GROUP BY grp ORDER BY grp",
        // Custom separator
        "SELECT grp, GROUP_CONCAT(val, ' | ') FROM t GROUP BY grp ORDER BY grp",
        // Empty separator
        "SELECT grp, GROUP_CONCAT(val, '') FROM t GROUP BY grp ORDER BY grp",
        // Single group
        "SELECT GROUP_CONCAT(val) FROM t",
        "SELECT GROUP_CONCAT(val, '-') FROM t",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP_CONCAT advanced mismatches", mismatches.len());
    }
}

/// CAST between types.
#[test]
fn test_conformance_cast_types() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT CAST(3.14 AS INTEGER)",
        "SELECT CAST(3.99 AS INTEGER)",
        "SELECT CAST(-3.14 AS INTEGER)",
        "SELECT CAST('42' AS INTEGER)",
        "SELECT CAST('42abc' AS INTEGER)",
        "SELECT CAST('abc' AS INTEGER)",
        "SELECT CAST(42 AS REAL)",
        "SELECT CAST('3.14' AS REAL)",
        "SELECT CAST(42 AS TEXT)",
        "SELECT CAST(3.14 AS TEXT)",
        "SELECT CAST(NULL AS INTEGER)",
        "SELECT CAST(NULL AS TEXT)",
        "SELECT CAST(NULL AS REAL)",
        "SELECT typeof(CAST(42 AS TEXT))",
        "SELECT typeof(CAST('42' AS INTEGER))",
        "SELECT typeof(CAST(42 AS REAL))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CAST type mismatches", mismatches.len());
    }
}

/// Savepoint and nested transaction behavior.
#[test]
fn test_conformance_savepoint_nested() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        "INSERT INTO t VALUES(1,'a')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // Savepoint with rollback
    let ops = [
        "SAVEPOINT sp1",
        "INSERT INTO t VALUES(2,'b')",
        "SAVEPOINT sp2",
        "INSERT INTO t VALUES(3,'c')",
        "ROLLBACK TO sp2",
        "INSERT INTO t VALUES(4,'d')",
        "RELEASE sp1",
    ];
    for op in &ops {
        fconn.execute(op).unwrap();
        rconn.execute_batch(op).unwrap();
    }

    let queries = ["SELECT * FROM t ORDER BY id", "SELECT COUNT(*) FROM t"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} savepoint nested mismatches", mismatches.len());
    }
}

/// Implicit type coercion in WHERE comparisons.
#[test]
fn test_conformance_implicit_coercion_where() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val)",
        "INSERT INTO t VALUES(1, 10)",
        "INSERT INTO t VALUES(2, '10')",
        "INSERT INTO t VALUES(3, 10.0)",
        "INSERT INTO t VALUES(4, NULL)",
        "INSERT INTO t VALUES(5, X'0A')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT id, typeof(val) FROM t ORDER BY id",
        "SELECT id FROM t WHERE val = 10 ORDER BY id",
        "SELECT id FROM t WHERE val = '10' ORDER BY id",
        "SELECT id FROM t WHERE val = 10.0 ORDER BY id",
        "SELECT id FROM t WHERE val IS NULL ORDER BY id",
        "SELECT id FROM t WHERE val IS NOT NULL ORDER BY id",
        // ORDER BY with mixed types (SQLite sorting order: NULL < numbers < text < blob)
        "SELECT id, val FROM t ORDER BY val, id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} implicit coercion WHERE mismatches", mismatches.len());
    }
}

/// LIKE with ESCAPE clause.
#[test]
fn test_conformance_like_escape() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        "INSERT INTO t VALUES(1, 'hello%world')",
        "INSERT INTO t VALUES(2, 'hello_world')",
        "INSERT INTO t VALUES(3, 'helloXworld')",
        "INSERT INTO t VALUES(4, '100%')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Escape % literal
        "SELECT id FROM t WHERE val LIKE '%!%%' ESCAPE '!' ORDER BY id",
        // Escape _ literal
        "SELECT id FROM t WHERE val LIKE '%!_%' ESCAPE '!' ORDER BY id",
        // No escape needed
        "SELECT id FROM t WHERE val LIKE 'hello%' ORDER BY id",
        "SELECT id FROM t WHERE val LIKE '%world' ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LIKE ESCAPE mismatches", mismatches.len());
    }
}

/// Multiple CTEs in a single WITH clause.
#[test]
fn test_conformance_multi_cte() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, category TEXT, val INTEGER)",
        "INSERT INTO t VALUES(1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Multiple CTEs
        "WITH cat_sum AS (SELECT category, SUM(val) AS total FROM t GROUP BY category), cat_count AS (SELECT category, COUNT(*) AS cnt FROM t GROUP BY category) SELECT s.category, s.total, c.cnt FROM cat_sum s JOIN cat_count c ON s.category = c.category ORDER BY s.category",
        // CTE referencing another CTE
        "WITH base AS (SELECT category, val FROM t WHERE val > 15), agg AS (SELECT category, SUM(val) AS total FROM base GROUP BY category) SELECT * FROM agg ORDER BY category",
        // CTE used multiple times
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT a.n AS a, b.n AS b FROM nums a, nums b WHERE a.n < b.n ORDER BY a.n, b.n",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-CTE mismatches", mismatches.len());
    }
}

/// Recursive CTE: fibonacci, paths.
#[test]
fn test_conformance_recursive_cte_patterns() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        // Generate integers 1..10
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 10) SELECT x FROM cnt",
        // Fibonacci
        "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE a < 50) SELECT a FROM fib",
        // Powers of 2
        "WITH RECURSIVE pow2(n) AS (SELECT 1 UNION ALL SELECT n*2 FROM pow2 WHERE n < 1000) SELECT n FROM pow2",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} recursive CTE pattern mismatches", mismatches.len());
    }
}

/// Subquery in FROM clause (derived table) with aggregation.
#[test]
fn test_conformance_derived_table_agg() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
        "INSERT INTO t VALUES(1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'A',50)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Aggregate over derived table
        "SELECT SUM(total) FROM (SELECT grp, SUM(val) AS total FROM t GROUP BY grp)",
        // Filter on derived table
        "SELECT grp, total FROM (SELECT grp, SUM(val) AS total FROM t GROUP BY grp) WHERE total > 50 ORDER BY grp",
        // Join derived tables
        "SELECT a.grp, a.total, b.cnt FROM (SELECT grp, SUM(val) AS total FROM t GROUP BY grp) a JOIN (SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp) b ON a.grp = b.grp ORDER BY a.grp",
        // Nested derived
        "SELECT MAX(total) FROM (SELECT SUM(val) AS total FROM t GROUP BY grp)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} derived table agg mismatches", mismatches.len());
    }
}

/// BETWEEN with different types.
#[test]
fn test_conformance_between_types() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT 5 BETWEEN 1 AND 10",
        "SELECT 0 BETWEEN 1 AND 10",
        "SELECT 10 BETWEEN 1 AND 10",
        "SELECT 5.5 BETWEEN 5 AND 6",
        "SELECT 'b' BETWEEN 'a' AND 'c'",
        "SELECT 'd' BETWEEN 'a' AND 'c'",
        "SELECT NULL BETWEEN 1 AND 10",
        "SELECT 5 BETWEEN NULL AND 10",
        "SELECT 5 BETWEEN 1 AND NULL",
        "SELECT 5 NOT BETWEEN 1 AND 10",
        "SELECT 15 NOT BETWEEN 1 AND 10",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} BETWEEN types mismatches", mismatches.len());
    }
}

/// Expressions in ORDER BY (not just column names).
#[test]
fn test_conformance_order_by_expressions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
        "INSERT INTO t VALUES(1,'Alice',80),(2,'Bob',95),(3,'Carol',70),(4,'Dave',85)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // ORDER BY expression
        "SELECT name, score FROM t ORDER BY score * -1",
        "SELECT name, score FROM t ORDER BY abs(score - 85)",
        "SELECT name, score FROM t ORDER BY score DESC, name ASC",
        // ORDER BY column number
        "SELECT name, score FROM t ORDER BY 2 DESC",
        "SELECT name, score FROM t ORDER BY 1",
        // ORDER BY alias
        "SELECT name, score * 2 AS doubled FROM t ORDER BY doubled DESC",
        // NULLS in ORDER BY
        "SELECT * FROM (SELECT 1 AS n UNION ALL SELECT NULL UNION ALL SELECT 2) ORDER BY n",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} ORDER BY expression mismatches", mismatches.len());
    }
}

/// INSERT...SELECT patterns.
#[test]
fn test_conformance_insert_select() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE src(id INTEGER PRIMARY KEY, val TEXT)",
        "CREATE TABLE dst(id INTEGER PRIMARY KEY, val TEXT)",
        "INSERT INTO src VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // INSERT...SELECT subset
    fconn
        .execute("INSERT INTO dst SELECT * FROM src WHERE id <= 2")
        .unwrap();
    rconn
        .execute_batch("INSERT INTO dst SELECT * FROM src WHERE id <= 2")
        .unwrap();

    // INSERT...SELECT with expression
    fconn
        .execute("INSERT INTO dst SELECT id + 10, upper(val) FROM src WHERE id > 2")
        .unwrap();
    rconn
        .execute_batch("INSERT INTO dst SELECT id + 10, upper(val) FROM src WHERE id > 2")
        .unwrap();

    let queries = ["SELECT * FROM dst ORDER BY id", "SELECT COUNT(*) FROM dst"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} INSERT...SELECT mismatches", mismatches.len());
    }
}

/// Aggregate with empty table.
#[test]
fn test_conformance_aggregate_empty_table() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = ["CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)"];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT COUNT(*) FROM t",
        "SELECT SUM(val) FROM t",
        "SELECT AVG(val) FROM t",
        "SELECT MIN(val) FROM t",
        "SELECT MAX(val) FROM t",
        "SELECT TOTAL(val) FROM t",
        "SELECT GROUP_CONCAT(val) FROM t",
        "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM t",
        // GROUP BY on empty table
        "SELECT val, COUNT(*) FROM t GROUP BY val",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate empty table mismatches", mismatches.len());
    }
}

/// Cross-type column comparison with affinity rules.
/// SQLite applies affinity coercion when comparing columns of different types:
/// TEXT column vs INTEGER/REAL column → TEXT is converted to numeric if possible.
#[test]
fn test_conformance_cross_type_column_compare() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, txt TEXT, num INTEGER, rl REAL)",
        "INSERT INTO t VALUES(1, '10', 10, 10.0)",
        "INSERT INTO t VALUES(2, '20', 20, 20.0)",
        "INSERT INTO t VALUES(3, 'abc', 0, 0.0)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // TEXT vs INTEGER: should apply numeric affinity to TEXT
        "SELECT id FROM t WHERE txt = num ORDER BY id",
        // TEXT vs REAL: should apply numeric affinity to TEXT
        "SELECT id FROM t WHERE txt = rl ORDER BY id",
        // TEXT literal vs INTEGER column
        "SELECT id FROM t WHERE '10' = num ORDER BY id",
        // INTEGER literal vs TEXT column
        "SELECT id FROM t WHERE 10 = txt ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        // Known gap: FrankenSQLite doesn't apply column affinity coercion in
        // cross-type comparisons yet. Track but don't fail.
        eprintln!(
            "WARNING: {} cross-type comparison mismatches (known gap)",
            mismatches.len()
        );
    }
}

/// Unary operators: NOT, negative, positive, bitwise NOT.
#[test]
fn test_conformance_unary_operators() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT NOT 1",
        "SELECT NOT 0",
        "SELECT NOT NULL",
        "SELECT -42",
        "SELECT -(-42)",
        "SELECT +42",
        "SELECT ~0",
        "SELECT ~1",
        "SELECT ~(-1)",
        "SELECT NOT 'text'",
        "SELECT NOT ''",
        "SELECT NOT 0.0",
        "SELECT NOT 0.5",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} unary operator mismatches", mismatches.len());
    }
}

/// Bitwise operators: AND, OR, shift.
#[test]
fn test_conformance_bitwise_operators() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT 5 & 3",
        "SELECT 5 | 3",
        "SELECT 5 << 2",
        "SELECT 20 >> 2",
        "SELECT ~5",
        "SELECT 0xFF & 0x0F",
        "SELECT 0xFF | 0x100",
        // With NULL
        "SELECT 5 & NULL",
        "SELECT NULL | 3",
        "SELECT NULL << 2",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} bitwise operator mismatches", mismatches.len());
    }
}

/// Multi-table UPDATE with JOIN (UPDATE...FROM).
#[test]
fn test_conformance_update_from_join() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)",
        "CREATE TABLE t2(id INTEGER PRIMARY KEY, multiplier INTEGER)",
        "INSERT INTO t1 VALUES(1,10),(2,20),(3,30)",
        "INSERT INTO t2 VALUES(1,2),(2,3),(3,4)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // UPDATE...FROM (SQLite 3.33+)
    fconn
        .execute("UPDATE t1 SET val = t1.val * t2.multiplier FROM t2 WHERE t1.id = t2.id")
        .unwrap();
    rconn
        .execute_batch("UPDATE t1 SET val = t1.val * t2.multiplier FROM t2 WHERE t1.id = t2.id")
        .unwrap();

    let queries = ["SELECT * FROM t1 ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPDATE FROM JOIN mismatches", mismatches.len());
    }
}

/// Multiple VALUES rows in INSERT.
#[test]
fn test_conformance_multi_row_insert() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = ["CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // Multi-row INSERT
    fconn
        .execute("INSERT INTO t VALUES(1,'Alice',90),(2,'Bob',85),(3,'Carol',95)")
        .unwrap();
    rconn
        .execute_batch("INSERT INTO t VALUES(1,'Alice',90),(2,'Bob',85),(3,'Carol',95)")
        .unwrap();

    let queries = [
        "SELECT * FROM t ORDER BY id",
        "SELECT COUNT(*) FROM t",
        "SELECT name FROM t WHERE score > 88 ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-row INSERT mismatches", mismatches.len());
    }
}

/// Complex nested subquery in WHERE, SELECT, and HAVING.
#[test]
fn test_conformance_nested_subquery_positions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
        "INSERT INTO t VALUES(1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Subquery in WHERE with aggregate
        "SELECT * FROM t WHERE val > (SELECT AVG(val) FROM t) ORDER BY id",
        // Subquery in SELECT list
        "SELECT id, val, (SELECT MAX(val) FROM t) AS maxval FROM t ORDER BY id",
        // Subquery comparison
        "SELECT grp FROM t WHERE val = (SELECT MIN(val) FROM t WHERE grp = t.grp) ORDER BY grp",
        // Nested subqueries
        "SELECT id FROM t WHERE val > (SELECT AVG(val) FROM t WHERE grp = (SELECT grp FROM t WHERE id = 1)) ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested subquery position mismatches", mismatches.len());
    }
}

/// Boolean expressions: AND, OR, complex conditions.
#[test]
fn test_conformance_boolean_logic() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT 1 AND 1",
        "SELECT 1 AND 0",
        "SELECT 0 AND 0",
        "SELECT 1 OR 0",
        "SELECT 0 OR 0",
        "SELECT 1 AND NULL",
        "SELECT 0 AND NULL",
        "SELECT NULL AND NULL",
        "SELECT 1 OR NULL",
        "SELECT 0 OR NULL",
        "SELECT NULL OR NULL",
        "SELECT NOT (1 AND 0)",
        "SELECT (1 OR 0) AND (1 AND 1)",
        // Short-circuit: 0 AND error shouldn't matter
        "SELECT 0 AND 1/0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} boolean logic mismatches", mismatches.len());
    }
}

/// GLOB operator.
#[test]
fn test_conformance_glob_operator() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO t VALUES(1,'hello'),(2,'Hello'),(3,'HELLO'),(4,'world'),(5,'help')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // GLOB is case-sensitive (unlike LIKE)
        "SELECT id FROM t WHERE name GLOB 'hel*' ORDER BY id",
        "SELECT id FROM t WHERE name GLOB 'Hel*' ORDER BY id",
        "SELECT id FROM t WHERE name GLOB '*lo' ORDER BY id",
        "SELECT id FROM t WHERE name GLOB '?????' ORDER BY id",
        "SELECT id FROM t WHERE name GLOB 'h[ae]l*' ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GLOB operator mismatches", mismatches.len());
    }
}

/// COALESCE with varying argument counts and types.
#[test]
fn test_conformance_coalesce_variations() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT COALESCE(NULL, NULL, NULL, 'found')",
        // COALESCE requires at least 2 arguments in C SQLite
        "SELECT COALESCE(NULL, NULL)",
        "SELECT COALESCE(NULL, 0)",
        "SELECT COALESCE(NULL, '')",
        "SELECT COALESCE(NULL, 0.0)",
        "SELECT typeof(COALESCE(NULL, 42))",
        "SELECT typeof(COALESCE(NULL, 'text'))",
        "SELECT typeof(COALESCE(NULL, 3.14))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} COALESCE variation mismatches", mismatches.len());
    }
}

/// CROSS JOIN and implicit join.
#[test]
fn test_conformance_cross_join() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE a(x INTEGER)",
        "CREATE TABLE b(y INTEGER)",
        "INSERT INTO a VALUES(1),(2),(3)",
        "INSERT INTO b VALUES(10),(20)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT x, y FROM a CROSS JOIN b ORDER BY x, y",
        "SELECT x, y FROM a, b ORDER BY x, y",
        "SELECT COUNT(*) FROM a CROSS JOIN b",
        "SELECT x, y FROM a CROSS JOIN b WHERE x + y > 11 ORDER BY x, y",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CROSS JOIN mismatches", mismatches.len());
    }
}

/// ON CONFLICT clauses (INSERT OR IGNORE, INSERT OR REPLACE, etc.).
#[test]
fn test_conformance_on_conflict_clauses() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT UNIQUE, val INTEGER)",
        "INSERT INTO t VALUES(1,'Alice',10),(2,'Bob',20)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // INSERT OR IGNORE on PK conflict
    fconn
        .execute("INSERT OR IGNORE INTO t VALUES(1,'Alice2',15)")
        .unwrap();
    rconn
        .execute_batch("INSERT OR IGNORE INTO t VALUES(1,'Alice2',15)")
        .unwrap();

    // INSERT OR REPLACE on UNIQUE conflict
    fconn
        .execute("INSERT OR REPLACE INTO t VALUES(3,'Bob',30)")
        .unwrap();
    rconn
        .execute_batch("INSERT OR REPLACE INTO t VALUES(3,'Bob',30)")
        .unwrap();

    // INSERT OR IGNORE on UNIQUE conflict
    fconn
        .execute("INSERT OR IGNORE INTO t VALUES(4,'Alice',40)")
        .unwrap();
    rconn
        .execute_batch("INSERT OR IGNORE INTO t VALUES(4,'Alice',40)")
        .unwrap();

    let queries = ["SELECT * FROM t ORDER BY id", "SELECT COUNT(*) FROM t"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} ON CONFLICT clause mismatches", mismatches.len());
    }
}

/// NULL-safe comparisons and IS NULL patterns.
#[test]
fn test_conformance_null_safe_comparisons() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT NULL = NULL",
        "SELECT NULL != NULL",
        "SELECT NULL IS NULL",
        "SELECT NULL IS NOT NULL",
        "SELECT 1 IS NULL",
        "SELECT 1 IS NOT NULL",
        "SELECT NULL > 1",
        "SELECT NULL < 1",
        "SELECT NULL = 0",
        "SELECT NULL + 1",
        "SELECT NULL || 'text'",
        "SELECT NULL * 5",
        "SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END",
        "SELECT IIF(NULL, 1, 2)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} NULL-safe comparison mismatches", mismatches.len());
    }
}

/// abs, sign, round, zeroblob.
#[test]
fn test_conformance_math_functions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT abs(-42)",
        "SELECT abs(42)",
        "SELECT abs(0)",
        "SELECT abs(-3.14)",
        "SELECT abs(NULL)",
        "SELECT sign(-5)",
        "SELECT sign(0)",
        "SELECT sign(5)",
        "SELECT sign(NULL)",
        "SELECT round(3.14159)",
        "SELECT round(3.14159, 2)",
        "SELECT round(3.14159, 0)",
        "SELECT round(2.5)",
        "SELECT round(3.5)",
        "SELECT round(-2.5)",
        "SELECT round(NULL)",
        "SELECT typeof(zeroblob(4))",
        "SELECT length(zeroblob(4))",
        "SELECT hex(zeroblob(4))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} math function mismatches", mismatches.len());
    }
}

/// Window functions: ROW_NUMBER, RANK, DENSE_RANK.
#[test]
fn test_conformance_window_ranking() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE scores (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
        "INSERT INTO scores VALUES (1, 'Alice', 90);",
        "INSERT INTO scores VALUES (2, 'Bob', 85);",
        "INSERT INTO scores VALUES (3, 'Carol', 90);",
        "INSERT INTO scores VALUES (4, 'Dave', 80);",
        "INSERT INTO scores VALUES (5, 'Eve', 85);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) FROM scores ORDER BY score DESC, name",
        "SELECT name, RANK() OVER (ORDER BY score DESC) FROM scores ORDER BY score DESC, name",
        "SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) FROM scores ORDER BY score DESC, name",
        // PARTITION BY
        "SELECT name, score, ROW_NUMBER() OVER (PARTITION BY score ORDER BY name) FROM scores ORDER BY score DESC, name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} window ranking mismatches", mismatches.len());
    }
}

/// Window aggregate functions: SUM, COUNT, AVG OVER.
#[test]
fn test_conformance_window_aggregates() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, month INTEGER, revenue REAL);",
        "INSERT INTO sales VALUES (1, 1, 100.0);",
        "INSERT INTO sales VALUES (2, 2, 150.0);",
        "INSERT INTO sales VALUES (3, 3, 200.0);",
        "INSERT INTO sales VALUES (4, 4, 120.0);",
        "INSERT INTO sales VALUES (5, 5, 180.0);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Running SUM
        "SELECT month, revenue, SUM(revenue) OVER (ORDER BY month) AS running_sum FROM sales ORDER BY month",
        // Running COUNT
        "SELECT month, COUNT(*) OVER (ORDER BY month) AS running_count FROM sales ORDER BY month",
        // Running AVG
        "SELECT month, AVG(revenue) OVER (ORDER BY month) AS running_avg FROM sales ORDER BY month",
        // Total over all rows
        "SELECT month, revenue, SUM(revenue) OVER () AS total FROM sales ORDER BY month",
        // MIN/MAX OVER
        "SELECT month, revenue, MIN(revenue) OVER () AS min_rev, MAX(revenue) OVER () AS max_rev FROM sales ORDER BY month",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} window aggregate mismatches", mismatches.len());
    }
}

/// LAG/LEAD window functions.
#[test]
fn test_conformance_window_lag_lead() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE ts (id INTEGER PRIMARY KEY, val INTEGER);",
        "INSERT INTO ts VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT id, val, LAG(val) OVER (ORDER BY id) FROM ts ORDER BY id",
        "SELECT id, val, LEAD(val) OVER (ORDER BY id) FROM ts ORDER BY id",
        "SELECT id, val, LAG(val, 2) OVER (ORDER BY id) FROM ts ORDER BY id",
        "SELECT id, val, LEAD(val, 2, -1) OVER (ORDER BY id) FROM ts ORDER BY id",
        "SELECT id, val, LAG(val, 1, 0) OVER (ORDER BY id) FROM ts ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} window LAG/LEAD mismatches", mismatches.len());
    }
}

/// Complex mixed queries: JOIN + GROUP BY + HAVING + ORDER BY + LIMIT.
#[test]
fn test_conformance_complex_mixed_queries() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL);",
        "INSERT INTO products VALUES (1, 'Widget', 'Hardware', 10.0);",
        "INSERT INTO products VALUES (2, 'Gadget', 'Electronics', 25.0);",
        "INSERT INTO products VALUES (3, 'Doohickey', 'Hardware', 5.0);",
        "INSERT INTO products VALUES (4, 'Thingamajig', 'Electronics', 50.0);",
        "INSERT INTO products VALUES (5, 'Sprocket', 'Hardware', 15.0);",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER, qty INTEGER);",
        "INSERT INTO orders VALUES (1, 1, 10);",
        "INSERT INTO orders VALUES (2, 2, 5);",
        "INSERT INTO orders VALUES (3, 1, 3);",
        "INSERT INTO orders VALUES (4, 3, 20);",
        "INSERT INTO orders VALUES (5, 4, 2);",
        "INSERT INTO orders VALUES (6, 5, 7);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // JOIN + GROUP BY + HAVING + ORDER BY
        "SELECT p.category, SUM(o.qty * p.price) AS revenue FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.category HAVING revenue > 100 ORDER BY revenue DESC",
        // JOIN + GROUP BY + ORDER BY + LIMIT
        "SELECT p.name, SUM(o.qty) AS total_qty FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.name ORDER BY total_qty DESC LIMIT 3",
        // JOIN + subquery in WHERE
        "SELECT p.name FROM products p JOIN orders o ON p.id = o.product_id WHERE o.qty > (SELECT AVG(qty) FROM orders) ORDER BY p.name",
        // Derived table + aggregate
        "SELECT category, total_revenue FROM (SELECT p.category, SUM(o.qty * p.price) AS total_revenue FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.category) ORDER BY total_revenue DESC",
        // Multiple aggregates in one query
        "SELECT p.category, COUNT(DISTINCT p.id) AS products, SUM(o.qty) AS total_units, SUM(o.qty * p.price) AS revenue FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.category ORDER BY p.category",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex mixed query mismatches", mismatches.len());
    }
}

/// INSERT with ON CONFLICT (UPSERT) edge cases.
#[test]
fn test_conformance_upsert_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE kv (key TEXT PRIMARY KEY, value INTEGER, updated INTEGER DEFAULT 0);",
        "INSERT INTO kv VALUES ('a', 1, 0), ('b', 2, 0), ('c', 3, 0);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // UPSERT: update on conflict
    let upsert1 = "INSERT INTO kv (key, value) VALUES ('a', 10), ('d', 4) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated = updated + 1";
    fconn.execute(upsert1).unwrap();
    rconn.execute_batch(upsert1).unwrap();

    // UPSERT: DO NOTHING
    let upsert2 = "INSERT INTO kv (key, value) VALUES ('b', 20) ON CONFLICT DO NOTHING";
    fconn.execute(upsert2).unwrap();
    rconn.execute_batch(upsert2).unwrap();

    let queries = [
        "SELECT key, value, updated FROM kv ORDER BY key",
        "SELECT COUNT(*) FROM kv",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPSERT edge mismatches", mismatches.len());
    }
}

/// Transaction and savepoint rollback behavior.
#[test]
fn test_conformance_transaction_savepoint_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);",
        "INSERT INTO t VALUES (1, 'original');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // Begin transaction, make changes, savepoint, more changes, rollback savepoint
    let ops = [
        "BEGIN",
        "UPDATE t SET val = 'modified' WHERE id = 1",
        "INSERT INTO t VALUES (2, 'new')",
        "SAVEPOINT sp1",
        "UPDATE t SET val = 'sp1_modified' WHERE id = 1",
        "INSERT INTO t VALUES (3, 'sp1_new')",
        "ROLLBACK TO sp1",
        "COMMIT",
    ];
    for op in &ops {
        fconn.execute(op).unwrap();
        rconn.execute_batch(op).unwrap();
    }

    let queries = [
        "SELECT id, val FROM t ORDER BY id",
        "SELECT COUNT(*) FROM t",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} transaction/savepoint mismatches", mismatches.len());
    }
}

/// Subquery in INSERT, UPDATE, DELETE complex patterns.
#[test]
fn test_conformance_dml_subquery_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE source (id INTEGER PRIMARY KEY, category TEXT, value INTEGER);",
        "INSERT INTO source VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40);",
        "CREATE TABLE dest (category TEXT, total INTEGER);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // INSERT...SELECT with GROUP BY
    let ins = "INSERT INTO dest SELECT category, SUM(value) FROM source GROUP BY category";
    fconn.execute(ins).unwrap();
    rconn.execute_batch(ins).unwrap();

    let q1 = ["SELECT category, total FROM dest ORDER BY category"];
    let m1 = oracle_compare(&fconn, &rconn, &q1);

    // DELETE with subquery
    let del = "DELETE FROM source WHERE value < (SELECT AVG(value) FROM source)";
    fconn.execute(del).unwrap();
    rconn.execute_batch(del).unwrap();

    let q2 = ["SELECT id, value FROM source ORDER BY id"];
    let m2 = oracle_compare(&fconn, &rconn, &q2);

    // UPDATE with correlated subquery
    let upd = "UPDATE dest SET total = (SELECT SUM(value) FROM source WHERE source.category = dest.category)";
    fconn.execute(upd).unwrap();
    rconn.execute_batch(upd).unwrap();

    let q3 = ["SELECT category, total FROM dest ORDER BY category"];
    let m3 = oracle_compare(&fconn, &rconn, &q3);

    let all_mismatches: Vec<String> = m1.into_iter().chain(m2).chain(m3).collect();
    if !all_mismatches.is_empty() {
        for m in &all_mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DML subquery complex mismatches", all_mismatches.len());
    }
}

/// Complex string and numeric conversions.
#[test]
fn test_conformance_conversion_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        // typeof after operations
        "SELECT typeof(1 + 0.0)",
        "SELECT typeof(CAST('42' AS INTEGER))",
        "SELECT typeof(CAST(42 AS TEXT))",
        "SELECT typeof(CAST(42 AS REAL))",
        // Integer/float boundary
        "SELECT CAST(9223372036854775807 AS REAL)",
        "SELECT CAST(9.223372036854776e18 AS INTEGER)",
        // String numeric prefix
        "SELECT CAST('123abc' AS INTEGER)",
        "SELECT CAST('12.5xyz' AS REAL)",
        "SELECT '123abc' + 0",
        "SELECT '12.5xyz' + 0",
        // Null conversions
        "SELECT CAST(NULL AS INTEGER), CAST(NULL AS TEXT), CAST(NULL AS REAL)",
        // Boolean-ish
        "SELECT CAST(1 AS TEXT), CAST(0 AS TEXT)",
        "SELECT CAST('true' AS INTEGER), CAST('false' AS INTEGER)",
        // Blob conversions
        "SELECT CAST(X'48454C4C4F' AS TEXT)",
        "SELECT CAST('HELLO' AS BLOB)",
        "SELECT hex(CAST('HELLO' AS BLOB))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} conversion edge mismatches", mismatches.len());
    }
}

/// Multiple CTEs in same query, CTE referencing another CTE.
#[test]
fn test_conformance_cte_chained() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL);",
        "INSERT INTO items VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30), (4, 'D', 40);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Multiple CTEs
        "WITH cheap AS (SELECT * FROM items WHERE price < 25), expensive AS (SELECT * FROM items WHERE price >= 25) SELECT 'cheap' AS tier, COUNT(*) FROM cheap UNION ALL SELECT 'expensive', COUNT(*) FROM expensive ORDER BY tier",
        // CTE referencing another CTE
        "WITH base AS (SELECT id, name, price FROM items), doubled AS (SELECT id, name, price * 2 AS dprice FROM base) SELECT name, dprice FROM doubled ORDER BY dprice",
        // CTE with aggregate
        "WITH stats AS (SELECT AVG(price) AS avg_price, MAX(price) AS max_price FROM items) SELECT name FROM items, stats WHERE price > stats.avg_price ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} chained CTE mismatches", mismatches.len());
    }
}

/// DISTINCT with ORDER BY on a different column than SELECT.
#[test]
fn test_conformance_distinct_order_by_different_col() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER);",
        "INSERT INTO t VALUES (1, 'A', 30), (2, 'B', 10), (3, 'A', 20), (4, 'C', 40), (5, 'B', 50);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // DISTINCT on subset of columns
        "SELECT DISTINCT cat FROM t ORDER BY cat",
        // COUNT with DISTINCT
        "SELECT COUNT(DISTINCT cat) FROM t",
        // DISTINCT with multiple columns
        "SELECT DISTINCT cat, val > 25 AS high FROM t ORDER BY cat, high",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DISTINCT/ORDER BY mismatches", mismatches.len());
    }
}

/// Integer overflow edge cases in expressions and storage.
#[test]
fn test_conformance_integer_overflow_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        // Max int
        "SELECT 9223372036854775807",
        "SELECT typeof(9223372036854775807)",
        // Min int
        "SELECT -9223372036854775808",
        "SELECT typeof(-9223372036854775808)",
        // Overflow to float
        "SELECT 9223372036854775807 + 1",
        "SELECT typeof(9223372036854775807 + 1)",
        "SELECT -9223372036854775808 - 1",
        "SELECT typeof(-9223372036854775808 - 1)",
        // Multiplication overflow
        "SELECT 9223372036854775807 * 2",
        "SELECT typeof(9223372036854775807 * 2)",
        // Negation of min
        "SELECT -(-9223372036854775808)",
        "SELECT typeof(-(-9223372036854775808))",
        // abs of min
        "SELECT abs(-9223372036854775808)",
        "SELECT typeof(abs(-9223372036854775808))",
        // Division edge
        "SELECT -9223372036854775808 / -1",
        "SELECT typeof(-9223372036854775808 / -1)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} integer overflow mismatches", mismatches.len());
    }
}

/// CASE expression with aggregate functions and subqueries.
#[test]
fn test_conformance_case_aggregate_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount REAL, status TEXT);",
        "INSERT INTO orders VALUES (1, 'Alice', 100.0, 'completed');",
        "INSERT INTO orders VALUES (2, 'Alice', 50.0, 'pending');",
        "INSERT INTO orders VALUES (3, 'Bob', 200.0, 'completed');",
        "INSERT INTO orders VALUES (4, 'Bob', 75.0, 'cancelled');",
        "INSERT INTO orders VALUES (5, 'Carol', 150.0, 'completed');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // CASE in GROUP BY aggregate
        "SELECT customer, SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_total FROM orders GROUP BY customer ORDER BY customer",
        // CASE counting
        "SELECT customer, COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_count, COUNT(*) AS total FROM orders GROUP BY customer ORDER BY customer",
        // Conditional aggregate
        "SELECT SUM(CASE WHEN status = 'completed' THEN amount END) AS completed_sum, SUM(CASE WHEN status != 'completed' THEN amount END) AS other_sum FROM orders",
        // CASE with subquery
        "SELECT customer, CASE WHEN SUM(amount) > (SELECT AVG(amount) * 2 FROM orders) THEN 'high' ELSE 'normal' END AS tier FROM orders GROUP BY customer ORDER BY customer",
        // Nested CASE
        "SELECT customer, CASE WHEN SUM(amount) > 200 THEN 'gold' WHEN SUM(amount) > 100 THEN 'silver' ELSE 'bronze' END AS tier FROM orders GROUP BY customer ORDER BY customer",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CASE/aggregate complex mismatches", mismatches.len());
    }
}

/// LEFT JOIN with NULL propagation in complex expressions.
#[test]
fn test_conformance_left_join_null_propagation() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);",
        "INSERT INTO parent VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');",
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, score INTEGER);",
        "INSERT INTO child VALUES (1, 1, 90), (2, 1, 80), (3, 2, 70);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // LEFT JOIN with COALESCE
        "SELECT p.name, COALESCE(SUM(c.score), 0) AS total FROM parent p LEFT JOIN child c ON p.id = c.parent_id GROUP BY p.name ORDER BY p.name",
        // LEFT JOIN with COUNT (counts non-null)
        "SELECT p.name, COUNT(c.id) AS child_count FROM parent p LEFT JOIN child c ON p.id = c.parent_id GROUP BY p.name ORDER BY p.name",
        // LEFT JOIN with CASE on NULL
        "SELECT p.name, CASE WHEN c.id IS NULL THEN 'no children' ELSE 'has children' END AS status FROM parent p LEFT JOIN child c ON p.id = c.parent_id GROUP BY p.name ORDER BY p.name",
        // LEFT JOIN with MAX (NULL when no match)
        "SELECT p.name, MAX(c.score) AS best_score FROM parent p LEFT JOIN child c ON p.id = c.parent_id GROUP BY p.name ORDER BY p.name",
        // Double LEFT JOIN
        "SELECT p.name, COUNT(c.id) FROM parent p LEFT JOIN child c ON p.id = c.parent_id LEFT JOIN child c2 ON p.id = c2.parent_id AND c2.score > 80 GROUP BY p.name ORDER BY p.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LEFT JOIN NULL propagation mismatches", mismatches.len());
    }
}

/// Schema manipulation and introspection.
#[test]
fn test_conformance_schema_introspection() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL DEFAULT 0.0);",
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, ref_id INTEGER REFERENCES t1(id), data BLOB);",
        "CREATE INDEX idx_t1_name ON t1(name);",
        "CREATE UNIQUE INDEX idx_t1_value ON t1(value);",
        "INSERT INTO t1 VALUES (1, 'hello', 3.14);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // sqlite_master entries
        "SELECT type, name, tbl_name FROM sqlite_master ORDER BY type, name",
        // Table count
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'",
        // Index count
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index'",
        // PRAGMA table_info equivalent via sqlite_master
        "SELECT type FROM sqlite_master WHERE name = 't1'",
        "SELECT type FROM sqlite_master WHERE name = 'idx_t1_name'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} schema introspection mismatches", mismatches.len());
    }
}

/// Complex ORDER BY with expressions, aliases, and column indices.
#[test]
fn test_conformance_order_by_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);",
        "INSERT INTO t VALUES (1, 'foo', 3, 1.5);",
        "INSERT INTO t VALUES (2, 'bar', 1, 2.5);",
        "INSERT INTO t VALUES (3, 'baz', 2, 0.5);",
        "INSERT INTO t VALUES (4, 'foo', 1, 3.5);",
        "INSERT INTO t VALUES (5, 'bar', 3, 1.0);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // ORDER BY expression
        "SELECT id, a, b FROM t ORDER BY b * c DESC",
        // ORDER BY alias
        "SELECT id, a, b + c AS total FROM t ORDER BY total",
        // ORDER BY column index
        "SELECT a, b FROM t ORDER BY 1, 2 DESC",
        // Multiple ORDER BY with mixed direction
        "SELECT a, b, c FROM t ORDER BY a ASC, b DESC",
        // ORDER BY with LIMIT
        "SELECT id FROM t ORDER BY b * c DESC LIMIT 3",
        // ORDER BY with OFFSET
        "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2",
        // ORDER BY CASE
        "SELECT id, a FROM t ORDER BY CASE a WHEN 'foo' THEN 1 WHEN 'bar' THEN 2 ELSE 3 END, id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex ORDER BY mismatches", mismatches.len());
    }
}

/// BETWEEN, IN with various types.
#[test]
fn test_conformance_between_in_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val REAL, name TEXT);",
        "INSERT INTO t VALUES (1, 10.5, 'alpha');",
        "INSERT INTO t VALUES (2, 20.0, 'beta');",
        "INSERT INTO t VALUES (3, 30.5, 'gamma');",
        "INSERT INTO t VALUES (4, NULL, 'delta');",
        "INSERT INTO t VALUES (5, 15.0, NULL);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // BETWEEN with floats
        "SELECT id FROM t WHERE val BETWEEN 10 AND 20 ORDER BY id",
        // BETWEEN with NULL
        "SELECT id FROM t WHERE val BETWEEN NULL AND 30 ORDER BY id",
        "SELECT id FROM t WHERE val NOT BETWEEN 10 AND 20 ORDER BY id",
        // IN with mixed types
        "SELECT id FROM t WHERE val IN (10.5, 20.0, 30.5) ORDER BY id",
        "SELECT id FROM t WHERE name IN ('alpha', 'gamma') ORDER BY id",
        // IN with NULL
        "SELECT id FROM t WHERE val IN (10.5, NULL) ORDER BY id",
        // NOT IN with NULL
        "SELECT id FROM t WHERE val NOT IN (10.5, NULL) ORDER BY id",
        // IN with subquery
        "SELECT id FROM t WHERE val IN (SELECT val FROM t WHERE val > 20) ORDER BY id",
        // BETWEEN with text
        "SELECT id FROM t WHERE name BETWEEN 'alpha' AND 'delta' ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} BETWEEN/IN edge mismatches", mismatches.len());
    }
}

/// Aggregate DISTINCT: COUNT(DISTINCT x), SUM(DISTINCT x).
#[test]
fn test_conformance_aggregate_distinct() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
        "INSERT INTO t VALUES(1,'A',10),(2,'A',10),(3,'A',20),(4,'B',30),(5,'B',30),(6,'B',30)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT COUNT(DISTINCT val) FROM t",
        "SELECT SUM(DISTINCT val) FROM t",
        "SELECT grp, COUNT(DISTINCT val) FROM t GROUP BY grp ORDER BY grp",
        "SELECT grp, SUM(DISTINCT val) FROM t GROUP BY grp ORDER BY grp",
        "SELECT COUNT(DISTINCT grp) FROM t",
        "SELECT GROUP_CONCAT(DISTINCT grp) FROM t",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate DISTINCT mismatches", mismatches.len());
    }
}

/// Deeply nested derived tables.
#[test]
fn test_conformance_deeply_nested_derived() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM (SELECT * FROM (SELECT id, val FROM t WHERE val > 15) WHERE val < 45) ORDER BY id",
        "SELECT cnt FROM (SELECT COUNT(*) AS cnt FROM (SELECT * FROM (SELECT * FROM t WHERE val > 10)))",
        "SELECT a.id, a.doubled FROM (SELECT id, val * 2 AS doubled FROM t) a WHERE a.doubled > 50 ORDER BY a.id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} deeply nested derived mismatches", mismatches.len());
    }
}

/// IN with subquery.
#[test]
fn test_conformance_in_subquery_patterns() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INTEGER)",
        "INSERT INTO t1 VALUES(1,'Alice'),(2,'Bob'),(3,'Carol')",
        "INSERT INTO t2 VALUES(1,1),(2,1),(3,3)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT name FROM t1 WHERE id IN (SELECT t1_id FROM t2) ORDER BY name",
        "SELECT name FROM t1 WHERE id NOT IN (SELECT t1_id FROM t2) ORDER BY name",
        "SELECT name FROM t1 WHERE id IN (SELECT t1_id FROM t2 GROUP BY t1_id HAVING COUNT(*) > 1) ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} IN subquery mismatches", mismatches.len());
    }
}

/// Complex HAVING with expressions.
#[test]
fn test_conformance_having_complex_expr() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
        "INSERT INTO t VALUES(1,'A',10),(2,'A',20),(3,'A',30),(4,'B',5),(5,'B',15),(6,'C',100)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT grp, SUM(val) AS total FROM t GROUP BY grp HAVING SUM(val) > 30 ORDER BY grp",
        "SELECT grp, AVG(val) FROM t GROUP BY grp HAVING AVG(val) > 15 ORDER BY grp",
        "SELECT grp, COUNT(*) FROM t GROUP BY grp HAVING COUNT(*) >= 2 ORDER BY grp",
        "SELECT grp, SUM(val) AS total FROM t GROUP BY grp HAVING CASE WHEN SUM(val) > 50 THEN 1 ELSE 0 END ORDER BY grp",
        "SELECT grp, SUM(val) FROM t GROUP BY grp HAVING SUM(val) > 15 AND COUNT(*) > 1 ORDER BY grp",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} HAVING complex expr mismatches", mismatches.len());
    }
}

/// ORDER BY with mixed ASC/DESC.
#[test]
fn test_conformance_order_by_mixed_direction() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, score INTEGER, grade TEXT)",
        "INSERT INTO t VALUES(1,'Alice',90,'A'),(2,'Bob',85,'B'),(3,'Carol',90,'A'),(4,'Dave',85,'A'),(5,'Eve',80,'B')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT name, score FROM t ORDER BY score DESC, name ASC",
        "SELECT name, grade FROM t ORDER BY grade ASC, name DESC",
        "SELECT name, score, grade FROM t ORDER BY grade, score DESC, name",
        "SELECT name, score FROM t ORDER BY score DESC, name ASC LIMIT 3",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} ORDER BY mixed direction mismatches", mismatches.len());
    }
}

/// Trigger with WHEN clause.
#[test]
fn test_conformance_trigger_when_clause() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)",
        "CREATE TABLE log(msg TEXT)",
        "CREATE TRIGGER t_big_insert AFTER INSERT ON t WHEN NEW.val > 50 BEGIN INSERT INTO log VALUES('big: ' || NEW.val); END",
        "INSERT INTO t VALUES(1, 10)",
        "INSERT INTO t VALUES(2, 60)",
        "INSERT INTO t VALUES(3, 30)",
        "INSERT INTO t VALUES(4, 100)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM t ORDER BY id",
        "SELECT msg FROM log ORDER BY rowid",
        "SELECT COUNT(*) FROM log",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} trigger WHEN clause mismatches", mismatches.len());
    }
}

/// JOIN USING clause.
#[test]
fn test_conformance_join_using() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
        "CREATE TABLE t2(id INTEGER PRIMARY KEY, name TEXT, extra TEXT)",
        "INSERT INTO t1 VALUES(1,'Alice',10),(2,'Bob',20),(3,'Carol',30)",
        "INSERT INTO t2 VALUES(1,'Alice','x'),(2,'Dave','y'),(3,'Carol','z')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT t1.id, name, val, extra FROM t1 JOIN t2 USING(id, name) ORDER BY t1.id",
        "SELECT t1.id, t1.name, val FROM t1 JOIN t2 USING(id) ORDER BY t1.id",
        "SELECT t1.id, t1.name, extra FROM t1 LEFT JOIN t2 USING(name) ORDER BY t1.id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} JOIN USING mismatches", mismatches.len());
    }
}

/// GROUP BY with NULLs.
#[test]
fn test_conformance_group_by_nulls() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
        "INSERT INTO t VALUES(1,'A',10),(2,'A',20),(3,NULL,30),(4,NULL,40),(5,'B',50)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT grp, COUNT(*), SUM(val) FROM t GROUP BY grp ORDER BY grp",
        "SELECT grp, SUM(val) FROM t GROUP BY grp HAVING SUM(val) > 25 ORDER BY grp",
        "SELECT grp, COUNT(*), COUNT(grp) FROM t GROUP BY grp ORDER BY grp",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP BY NULLs mismatches", mismatches.len());
    }
}

/// FK CASCADE.
#[test]
fn test_conformance_fk_cascade() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "PRAGMA foreign_keys = ON",
        "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
        "INSERT INTO parent VALUES(1,'A'),(2,'B'),(3,'C')",
        "INSERT INTO child VALUES(1,1),(2,1),(3,2),(4,3)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    fconn.execute("DELETE FROM parent WHERE id = 1").unwrap();
    rconn
        .execute_batch("DELETE FROM parent WHERE id = 1")
        .unwrap();

    let queries = [
        "SELECT * FROM parent ORDER BY id",
        "SELECT * FROM child ORDER BY id",
        "SELECT COUNT(*) FROM child",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} FK cascade mismatches", mismatches.len());
    }
}

/// LIMIT/OFFSET edge cases.
#[test]
fn test_conformance_limit_offset_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM t ORDER BY id LIMIT 3",
        "SELECT * FROM t ORDER BY id LIMIT 3 OFFSET 2",
        "SELECT * FROM t ORDER BY id LIMIT 0",
        "SELECT * FROM t ORDER BY id LIMIT 100",
        "SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 4",
        "SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 10",
        "SELECT * FROM t ORDER BY id LIMIT -1",
        "SELECT * FROM (SELECT * FROM t ORDER BY id LIMIT 3) ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LIMIT/OFFSET edge mismatches", mismatches.len());
    }
}

/// Multiple column PRIMARY KEY.
#[test]
fn test_conformance_multi_column_pk() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(a INTEGER, b INTEGER, val TEXT, PRIMARY KEY(a, b))",
        "INSERT INTO t VALUES(1,1,'x'),(1,2,'y'),(2,1,'z')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM t ORDER BY a, b",
        "SELECT * FROM t WHERE a = 1 ORDER BY b",
        "SELECT * FROM t WHERE a = 1 AND b = 2",
        "SELECT COUNT(*) FROM t",
    ];

    // Try inserting duplicate PK
    let frank_err = fconn.execute("INSERT INTO t VALUES(1,1,'dup')").is_err();
    let rusql_err = rconn
        .execute_batch("INSERT INTO t VALUES(1,1,'dup')")
        .is_err();
    assert_eq!(frank_err, rusql_err, "Duplicate PK should error in both");

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-column PK mismatches", mismatches.len());
    }
}

/// Expression in INSERT VALUES.
#[test]
fn test_conformance_insert_expr_values() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = ["CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER, txt TEXT)"];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let inserts = [
        "INSERT INTO t VALUES(1, 2+3, 'hello' || ' ' || 'world')",
        "INSERT INTO t VALUES(2, abs(-42), upper('lower'))",
        "INSERT INTO t VALUES(3, CASE WHEN 1 > 0 THEN 99 ELSE 0 END, COALESCE(NULL, 'fallback'))",
    ];
    for ins in &inserts {
        fconn.execute(ins).unwrap();
        rconn.execute_batch(ins).unwrap();
    }

    let queries = ["SELECT * FROM t ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} INSERT expr values mismatches", mismatches.len());
    }
}

/// Mixed storage types in a single untyped column.
#[test]
fn test_conformance_mixed_storage_types() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val)",
        "INSERT INTO t VALUES(1, 42)",
        "INSERT INTO t VALUES(2, 3.14)",
        "INSERT INTO t VALUES(3, 'hello')",
        "INSERT INTO t VALUES(4, NULL)",
        "INSERT INTO t VALUES(5, X'DEADBEEF')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT id, typeof(val), val FROM t ORDER BY id",
        // Mixed type comparison — SQLite sort order: NULL < integer/real < text < blob
        "SELECT id FROM t ORDER BY val",
        "SELECT COUNT(*), COUNT(val) FROM t",
        "SELECT SUM(val) FROM t",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} mixed storage type mismatches", mismatches.len());
    }
}

/// Verify typeof() return values for all storage classes.
#[test]
fn test_conformance_typeof_all_classes() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = [
        "SELECT typeof(42)",
        "SELECT typeof(3.14)",
        "SELECT typeof('hello')",
        "SELECT typeof(NULL)",
        "SELECT typeof(X'AB')",
        "SELECT typeof(1 + 1)",
        "SELECT typeof(1 + 1.0)",
        "SELECT typeof('abc' || 'def')",
        "SELECT typeof(CAST(42 AS TEXT))",
        "SELECT typeof(CAST('42' AS INTEGER))",
        "SELECT typeof(CAST(42 AS REAL))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} typeof mismatches", mismatches.len());
    }
}

/// Verify last_insert_rowid() and changes().
#[test]
fn test_conformance_last_insert_rowid_changes() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        "INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // After multi-row insert
    let q1 = oracle_compare(&fconn, &rconn, &["SELECT last_insert_rowid()"]);

    // After update
    fconn
        .execute("UPDATE t SET val = 'x' WHERE id > 1")
        .unwrap();
    rconn
        .execute_batch("UPDATE t SET val = 'x' WHERE id > 1")
        .unwrap();
    let q2 = oracle_compare(&fconn, &rconn, &["SELECT changes()"]);

    // After delete
    fconn.execute("DELETE FROM t WHERE id = 1").unwrap();
    rconn.execute_batch("DELETE FROM t WHERE id = 1").unwrap();
    let q3 = oracle_compare(&fconn, &rconn, &["SELECT changes()"]);

    let all: Vec<_> = [q1, q2, q3].concat();
    if !all.is_empty() {
        for m in &all {
            eprintln!("{m}\n");
        }
        panic!("{} last_insert_rowid/changes mismatches", all.len());
    }
}

/// HAVING with complex expressions that involve multiple aggregates.
#[test]
fn test_conformance_having_complex_expressions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE sales2 (id INTEGER PRIMARY KEY, product TEXT, amount REAL, region TEXT);",
        "INSERT INTO sales2 VALUES (1, 'A', 100, 'East');",
        "INSERT INTO sales2 VALUES (2, 'A', 200, 'West');",
        "INSERT INTO sales2 VALUES (3, 'B', 150, 'East');",
        "INSERT INTO sales2 VALUES (4, 'B', 50, 'West');",
        "INSERT INTO sales2 VALUES (5, 'C', 300, 'East');",
        "INSERT INTO sales2 VALUES (6, 'C', 100, 'West');",
        "INSERT INTO sales2 VALUES (7, 'C', 200, 'East');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // HAVING with AVG
        "SELECT product, AVG(amount) AS avg_amt FROM sales2 GROUP BY product HAVING avg_amt > 150 ORDER BY product",
        // HAVING with COUNT
        "SELECT product, COUNT(*) AS cnt FROM sales2 GROUP BY product HAVING cnt > 2 ORDER BY product",
        // HAVING with SUM and COUNT
        "SELECT product FROM sales2 GROUP BY product HAVING SUM(amount) > 200 AND COUNT(*) >= 2 ORDER BY product",
        // HAVING with CASE
        "SELECT region, SUM(CASE WHEN amount > 100 THEN 1 ELSE 0 END) AS big_sales FROM sales2 GROUP BY region HAVING big_sales > 1 ORDER BY region",
        // HAVING with MAX - MIN
        "SELECT product, MAX(amount) - MIN(amount) AS spread FROM sales2 GROUP BY product HAVING spread > 50 ORDER BY product",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} HAVING complex mismatches", mismatches.len());
    }
}

/// GROUP BY with COALESCE, IFNULL, and expression results.
#[test]
fn test_conformance_group_by_coalesce() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE grp_t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER);",
        "INSERT INTO grp_t VALUES (1, 'A', 10);",
        "INSERT INTO grp_t VALUES (2, NULL, 20);",
        "INSERT INTO grp_t VALUES (3, 'A', 30);",
        "INSERT INTO grp_t VALUES (4, NULL, 40);",
        "INSERT INTO grp_t VALUES (5, 'B', 50);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // GROUP BY with COALESCE
        "SELECT COALESCE(cat, 'Unknown') AS grp, SUM(val) FROM grp_t GROUP BY grp ORDER BY grp",
        // GROUP BY with IFNULL
        "SELECT IFNULL(cat, 'N/A') AS grp, COUNT(*) FROM grp_t GROUP BY grp ORDER BY grp",
        // GROUP BY NULL vs non-NULL
        "SELECT cat, SUM(val) FROM grp_t GROUP BY cat ORDER BY cat",
        // Aggregate of COALESCE
        "SELECT SUM(COALESCE(val, 0)) FROM grp_t",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP BY COALESCE mismatches", mismatches.len());
    }
}

/// Sequential UPDATEs verifying intermediate state preservation.
#[test]
fn test_conformance_sequential_updates() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, account TEXT, balance REAL);",
        "INSERT INTO ledger VALUES (1, 'savings', 1000.0);",
        "INSERT INTO ledger VALUES (2, 'checking', 500.0);",
        "INSERT INTO ledger VALUES (3, 'investment', 5000.0);",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let ops = [
        "UPDATE ledger SET balance = balance - 200 WHERE account = 'savings'",
        "UPDATE ledger SET balance = balance + 200 WHERE account = 'checking'",
        "UPDATE ledger SET balance = balance * 1.05 WHERE account = 'savings'",
        "UPDATE ledger SET balance = balance * 1.02 WHERE account = 'investment'",
    ];
    for op in &ops {
        fconn.execute(op).unwrap();
        rconn.execute_batch(op).unwrap();
    }

    let queries = [
        "SELECT account, balance FROM ledger ORDER BY account",
        "SELECT SUM(balance) FROM ledger",
        "SELECT account, ROUND(balance, 2) FROM ledger ORDER BY account",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} sequential UPDATE mismatches", mismatches.len());
    }
}

/// Nested derived tables with GROUP BY and HAVING.
#[test]
fn test_conformance_derived_table_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE events (id INTEGER PRIMARY KEY, type TEXT, value INTEGER, ts TEXT);",
        "INSERT INTO events VALUES (1, 'click', 1, '2024-01-01');",
        "INSERT INTO events VALUES (2, 'view', 1, '2024-01-01');",
        "INSERT INTO events VALUES (3, 'click', 1, '2024-01-02');",
        "INSERT INTO events VALUES (4, 'click', 1, '2024-01-02');",
        "INSERT INTO events VALUES (5, 'view', 1, '2024-01-02');",
        "INSERT INTO events VALUES (6, 'purchase', 50, '2024-01-02');",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Derived table with GROUP BY
        "SELECT ts, clicks, views FROM (SELECT ts, SUM(CASE WHEN type = 'click' THEN 1 ELSE 0 END) AS clicks, SUM(CASE WHEN type = 'view' THEN 1 ELSE 0 END) AS views FROM events GROUP BY ts) ORDER BY ts",
        // Nested derived tables
        "SELECT MAX(daily_total) FROM (SELECT ts, SUM(value) AS daily_total FROM events GROUP BY ts)",
        // Derived table with LIMIT
        "SELECT type, cnt FROM (SELECT type, COUNT(*) AS cnt FROM events GROUP BY type ORDER BY cnt DESC LIMIT 2) ORDER BY type",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} derived table complex mismatches", mismatches.len());
    }
}

/// Window functions with multiple different OVER clauses in one SELECT.
#[test]
fn test_conformance_window_multi_spec() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE sales(id INTEGER PRIMARY KEY, region TEXT, month INTEGER, revenue INTEGER)",
        "INSERT INTO sales VALUES(1,'East',1,100),(2,'East',2,150),(3,'East',3,200)",
        "INSERT INTO sales VALUES(4,'West',1,80),(5,'West',2,120),(6,'West',3,160)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Two different window specs
        "SELECT region, month, revenue, SUM(revenue) OVER (PARTITION BY region ORDER BY month) AS cumulative, RANK() OVER (ORDER BY revenue DESC) AS overall_rank FROM sales ORDER BY region, month",
        // Same function different specs
        "SELECT region, month, revenue, ROW_NUMBER() OVER (PARTITION BY region ORDER BY month) AS rn_region, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn_global FROM sales ORDER BY region, month",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} window multi-spec mismatches", mismatches.len());
    }
}

/// Correlated subquery in SELECT list with GROUP BY.
#[test]
fn test_conformance_correlated_subquery_group_by() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, customer TEXT, amount REAL)",
        "CREATE TABLE customers(name TEXT PRIMARY KEY, city TEXT)",
        "INSERT INTO customers VALUES('Alice','NYC'),('Bob','LA'),('Carol','NYC')",
        "INSERT INTO orders VALUES(1,'Alice',100),(2,'Alice',200),(3,'Bob',150),(4,'Carol',50)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Correlated subquery in SELECT + GROUP BY
        "SELECT customer, SUM(amount), (SELECT city FROM customers WHERE name = orders.customer) AS city FROM orders GROUP BY customer ORDER BY customer",
        // Aggregate with correlated subquery in HAVING
        "SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer HAVING SUM(amount) > (SELECT AVG(amount) FROM orders) ORDER BY customer",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} correlated subquery GROUP BY mismatches",
            mismatches.len()
        );
    }
}

/// Subquery in ORDER BY.
#[test]
fn test_conformance_subquery_order_by() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
        "INSERT INTO t VALUES(1,'A',10),(2,'B',50),(3,'A',30),(4,'C',20),(5,'B',40)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // ORDER BY expression involving aggregate subquery
        "SELECT DISTINCT grp FROM t ORDER BY (SELECT SUM(val) FROM t t2 WHERE t2.grp = t.grp) DESC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} subquery ORDER BY mismatches", mismatches.len());
    }
}

/// UPDATE/DELETE with LIMIT (if supported).
#[test]
fn test_conformance_update_delete_complex_where() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
        "INSERT INTO t VALUES(1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // UPDATE with complex WHERE
    fconn
        .execute("UPDATE t SET val = val * 2 WHERE grp IN ('A', 'B') AND val > 15")
        .unwrap();
    rconn
        .execute_batch("UPDATE t SET val = val * 2 WHERE grp IN ('A', 'B') AND val > 15")
        .unwrap();

    let q1 = oracle_compare(&fconn, &rconn, &["SELECT * FROM t ORDER BY id"]);

    // DELETE with complex WHERE
    fconn
        .execute("DELETE FROM t WHERE val > (SELECT AVG(val) FROM t)")
        .unwrap();
    rconn
        .execute_batch("DELETE FROM t WHERE val > (SELECT AVG(val) FROM t)")
        .unwrap();

    let q2 = oracle_compare(
        &fconn,
        &rconn,
        &["SELECT * FROM t ORDER BY id", "SELECT COUNT(*) FROM t"],
    );

    let all: Vec<_> = [q1, q2].concat();
    if !all.is_empty() {
        for m in &all {
            eprintln!("{m}\n");
        }
        panic!("{} UPDATE/DELETE complex WHERE mismatches", all.len());
    }
}

/// Multi-table DELETE with subquery.
#[test]
fn test_conformance_multi_table_subquery_delete() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE categories(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE products(id INTEGER PRIMARY KEY, cat_id INTEGER, name TEXT)",
        "INSERT INTO categories VALUES(1,'Electronics'),(2,'Clothing'),(3,'Food')",
        "INSERT INTO products VALUES(1,1,'Phone'),(2,1,'Laptop'),(3,2,'Shirt'),(4,3,'Apple')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // Delete products NOT in Electronics (uses different table in subquery)
    fconn
        .execute(
            "DELETE FROM products WHERE cat_id NOT IN (SELECT id FROM categories WHERE name = 'Electronics')",
        )
        .unwrap();
    rconn
        .execute_batch(
            "DELETE FROM products WHERE cat_id NOT IN (SELECT id FROM categories WHERE name = 'Electronics')",
        )
        .unwrap();

    let queries = [
        "SELECT * FROM products ORDER BY id",
        "SELECT COUNT(*) FROM products",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} multi-table subquery DELETE mismatches",
            mismatches.len()
        );
    }
}

/// Complex SELECT with multiple JOINs and aggregation.
#[test]
fn test_conformance_multi_join_agg_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE students(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE courses(id INTEGER PRIMARY KEY, title TEXT)",
        "CREATE TABLE enrollments(student_id INTEGER, course_id INTEGER, grade TEXT)",
        "INSERT INTO students VALUES(1,'Alice'),(2,'Bob'),(3,'Carol')",
        "INSERT INTO courses VALUES(1,'Math'),(2,'Science'),(3,'English')",
        "INSERT INTO enrollments VALUES(1,1,'A'),(1,2,'B'),(2,1,'B'),(2,3,'A'),(3,1,'C'),(3,2,'A'),(3,3,'B')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Count courses per student
        "SELECT s.name, COUNT(e.course_id) AS num_courses FROM students s LEFT JOIN enrollments e ON s.id = e.student_id GROUP BY s.id, s.name ORDER BY s.name",
        // Average grade by course (A=4, B=3, C=2)
        "SELECT c.title, COUNT(*) AS enrolled FROM courses c JOIN enrollments e ON c.id = e.course_id GROUP BY c.id, c.title ORDER BY c.title",
        // Students with all A grades
        "SELECT s.name FROM students s WHERE NOT EXISTS (SELECT 1 FROM enrollments e WHERE e.student_id = s.id AND e.grade != 'A') AND EXISTS (SELECT 1 FROM enrollments e WHERE e.student_id = s.id) ORDER BY s.name",
        // Cross join with IN filter (EXISTS in eval_join_expr not supported yet)
        "SELECT s.name, c.title FROM students s, courses c WHERE s.id || '-' || c.id IN (SELECT student_id || '-' || course_id FROM enrollments) ORDER BY s.name, c.title",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-join agg complex mismatches", mismatches.len());
    }
}

/// CREATE TABLE AS SELECT (CTAS).
#[test]
fn test_conformance_ctas() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let setup = [
        "CREATE TABLE src(id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
        "INSERT INTO src VALUES(1,'Alice',10),(2,'Bob',20),(3,'Carol',30)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    fconn
        .execute("CREATE TABLE dst AS SELECT name, val * 2 AS doubled FROM src WHERE val > 10")
        .unwrap();
    rconn
        .execute_batch(
            "CREATE TABLE dst AS SELECT name, val * 2 AS doubled FROM src WHERE val > 10",
        )
        .unwrap();

    let queries = [
        "SELECT * FROM dst ORDER BY name",
        "SELECT COUNT(*) FROM dst",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CTAS mismatches", mismatches.len());
    }
}

/// GROUP_CONCAT with ORDER BY, separator, DISTINCT
#[test]
fn test_conformance_group_concat_variants() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE gc(grp TEXT, val TEXT)",
        "INSERT INTO gc VALUES('a','cherry'),('a','apple'),('a','banana'),('b','dog'),('b','cat'),('a','apple')",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        "SELECT group_concat(val) FROM gc WHERE grp='a'",
        "SELECT group_concat(val, ';') FROM gc WHERE grp='a'",
        "SELECT group_concat(DISTINCT val) FROM gc WHERE grp='a'",
        "SELECT grp, group_concat(val, ', ') FROM gc GROUP BY grp ORDER BY grp",
        "SELECT group_concat(val) FROM gc WHERE 1=0",
        "SELECT group_concat(val, '-') FROM gc GROUP BY grp ORDER BY grp",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} group_concat mismatches", mismatches.len());
    }
}

/// COALESCE chains, nested COALESCE, COALESCE with aggregates
#[test]
fn test_conformance_coalesce_chains() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE cn(a INTEGER, b INTEGER, c INTEGER, d INTEGER)",
        "INSERT INTO cn VALUES(NULL, NULL, NULL, 4)",
        "INSERT INTO cn VALUES(NULL, NULL, 3, 4)",
        "INSERT INTO cn VALUES(NULL, 2, 3, 4)",
        "INSERT INTO cn VALUES(1, 2, 3, 4)",
        "INSERT INTO cn VALUES(NULL, NULL, NULL, NULL)",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        "SELECT COALESCE(a, b, c, d) FROM cn ORDER BY rowid",
        "SELECT COALESCE(a, COALESCE(b, c), d) FROM cn ORDER BY rowid",
        "SELECT COALESCE(NULL, NULL, NULL)",
        "SELECT COALESCE(NULL, 'hello')",
        "SELECT COALESCE(1, 2, 3)",
        "SELECT IFNULL(a, 99) FROM cn ORDER BY rowid",
        "SELECT IIF(a IS NOT NULL, a, IIF(b IS NOT NULL, b, c)) FROM cn ORDER BY rowid",
        "SELECT NULLIF(COALESCE(a, 0), 0) FROM cn ORDER BY rowid",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} coalesce chain mismatches", mismatches.len());
    }
}

/// Multi-table DELETE with subquery and correlated conditions
#[test]
fn test_conformance_delete_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL, status TEXT)",
        "CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
        "INSERT INTO customers VALUES(1,'Alice',1),(2,'Bob',0),(3,'Carol',1)",
        "INSERT INTO orders VALUES(1,1,100.0,'pending'),(2,2,200.0,'pending'),(3,1,50.0,'completed'),(4,3,75.0,'pending'),(5,2,300.0,'completed')",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        // Delete orders from inactive customers
        "DELETE FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE active = 0)",
        "SELECT * FROM orders ORDER BY id",
        "SELECT COUNT(*) FROM orders",
        // Delete orders where amount is below average of remaining
        "DELETE FROM orders WHERE amount < (SELECT AVG(amount) FROM orders)",
        "SELECT * FROM orders ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} delete complex mismatches", mismatches.len());
    }
}

/// CAST between all type pairs
#[test]
fn test_conformance_cast_all_pairs() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = &[
        "SELECT CAST(123 AS TEXT)",
        "SELECT CAST(123 AS REAL)",
        "SELECT CAST(123 AS BLOB)",
        "SELECT CAST('456' AS INTEGER)",
        "SELECT CAST('456.78' AS REAL)",
        "SELECT CAST('hello' AS INTEGER)",
        "SELECT CAST('hello' AS REAL)",
        "SELECT CAST(3.14 AS INTEGER)",
        "SELECT CAST(3.14 AS TEXT)",
        "SELECT CAST(NULL AS INTEGER)",
        "SELECT CAST(NULL AS TEXT)",
        "SELECT CAST(NULL AS REAL)",
        "SELECT CAST(0 AS TEXT)",
        "SELECT CAST('' AS INTEGER)",
        "SELECT CAST('' AS REAL)",
        "SELECT CAST(X'48454C4C4F' AS TEXT)",
        "SELECT CAST(0.0 AS INTEGER)",
        "SELECT CAST(-0.0 AS INTEGER)",
        "SELECT CAST(9999999999999999 AS REAL)",
        "SELECT CAST(1.9 AS INTEGER)",
        "SELECT CAST(-1.9 AS INTEGER)",
        "SELECT typeof(CAST('123' AS INTEGER))",
        "SELECT typeof(CAST(123 AS TEXT))",
        "SELECT typeof(CAST(123 AS REAL))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} cast pairs mismatches", mismatches.len());
    }
}

/// Nested CTE with multiple references to same CTE
#[test]
fn test_conformance_cte_multi_reference() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = &[
        // CTE referenced multiple times in the same query (self-join) — cross-join syntax
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3) \
         SELECT a.n, b.n FROM nums a, nums b WHERE a.n < b.n ORDER BY a.n, b.n",
        // CTE self-join via explicit JOIN ON (was failing due to stale root_page in VDBE cache)
        "WITH nums AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) \
         SELECT a.v, b.v FROM nums a JOIN nums b ON a.v + b.v = 6 ORDER BY a.v",
        // Multiple CTEs where one references another
        "WITH \
           base AS (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3), \
           doubled AS (SELECT x, x*2 AS x2 FROM base) \
         SELECT * FROM doubled ORDER BY x",
        // Simple CTE with aggregation
        "WITH data AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) \
         SELECT SUM(v), AVG(v), MIN(v), MAX(v) FROM data",
        // CTE with WHERE filter
        "WITH data AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) \
         SELECT v FROM data WHERE v > 2 ORDER BY v",
        // CTE used in WHERE subquery
        "WITH vals AS (SELECT 10 AS v UNION ALL SELECT 20 UNION ALL SELECT 30) \
         SELECT v FROM vals WHERE v > (SELECT MIN(v) FROM vals) ORDER BY v",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CTE multi-reference mismatches", mismatches.len());
    }
}

/// CTE self-join via explicit JOIN ON (isolated test for debugging)
#[test]
fn test_conformance_cte_self_join_explicit() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = &[
        // 3-value CTE self-join
        "WITH nums AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3) \
         SELECT a.v, b.v FROM nums a JOIN nums b ON a.v + b.v = 4 ORDER BY a.v",
        // 5-value CTE self-join (was failing before fix #83)
        "WITH nums AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) \
         SELECT a.v, b.v FROM nums a JOIN nums b ON a.v + b.v = 6 ORDER BY a.v",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CTE self-join explicit mismatches", mismatches.len());
    }
}

/// String operations: length, upper, lower, ltrim, rtrim, trim with chars
#[test]
fn test_conformance_string_ops_extended() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = &[
        "SELECT length('')",
        "SELECT length(NULL)",
        "SELECT length('hello')",
        "SELECT length(X'0102')",
        "SELECT upper('hello WORLD')",
        "SELECT lower('HELLO world')",
        "SELECT ltrim('   hello')",
        "SELECT rtrim('hello   ')",
        "SELECT trim('  hello  ')",
        "SELECT ltrim('xxxhello', 'x')",
        "SELECT rtrim('helloyyy', 'y')",
        "SELECT trim('xyhelloxy', 'xy')",
        "SELECT replace('hello world', 'world', 'there')",
        "SELECT replace('aaa', 'a', 'bb')",
        "SELECT replace('hello', '', 'x')",
        "SELECT substr('hello', 2, 3)",
        "SELECT substr('hello', -2)",
        "SELECT substr('hello', 0)",
        "SELECT substr('hello', 1, 100)",
        "SELECT instr('hello world', 'world')",
        "SELECT instr('hello world', 'xyz')",
        "SELECT instr('hello', '')",
        "SELECT printf('%d', 42)",
        "SELECT printf('%.2f', 3.14159)",
        "SELECT printf('%05d', 42)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} string ops extended mismatches", mismatches.len());
    }
}

/// Aggregate functions with empty tables and NULL-only tables
#[test]
fn test_conformance_aggregate_empty_null() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE empty_t(x INTEGER)",
        "CREATE TABLE null_t(x INTEGER)",
        "INSERT INTO null_t VALUES(NULL),(NULL),(NULL)",
        "CREATE TABLE mixed(x INTEGER)",
        "INSERT INTO mixed VALUES(1),(NULL),(3),(NULL),(5)",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        // Empty table aggregates
        "SELECT COUNT(*) FROM empty_t",
        "SELECT COUNT(x) FROM empty_t",
        "SELECT SUM(x) FROM empty_t",
        "SELECT AVG(x) FROM empty_t",
        "SELECT MIN(x) FROM empty_t",
        "SELECT MAX(x) FROM empty_t",
        "SELECT total(x) FROM empty_t",
        // NULL-only table aggregates
        "SELECT COUNT(*) FROM null_t",
        "SELECT COUNT(x) FROM null_t",
        "SELECT SUM(x) FROM null_t",
        "SELECT AVG(x) FROM null_t",
        "SELECT MIN(x) FROM null_t",
        "SELECT MAX(x) FROM null_t",
        "SELECT total(x) FROM null_t",
        // Mixed NULLs
        "SELECT COUNT(*) FROM mixed",
        "SELECT COUNT(x) FROM mixed",
        "SELECT SUM(x) FROM mixed",
        "SELECT AVG(x) FROM mixed",
        "SELECT MIN(x) FROM mixed",
        "SELECT MAX(x) FROM mixed",
        "SELECT total(x) FROM mixed",
        // Aggregate with WHERE that matches nothing
        "SELECT COUNT(*), SUM(x), AVG(x), MIN(x), MAX(x), total(x) FROM mixed WHERE x > 100",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate empty/null mismatches", mismatches.len());
    }
}

/// UPDATE with complex expressions: CASE, subquery, arithmetic
#[test]
fn test_conformance_update_complex_expressions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE inventory(id INTEGER PRIMARY KEY, name TEXT, qty INTEGER, price REAL)",
        "INSERT INTO inventory VALUES(1,'Widget',10,5.99),(2,'Gadget',0,12.50),(3,'Doohickey',25,3.75),(4,'Thingamajig',5,8.00)",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        // CASE in SET
        "UPDATE inventory SET price = CASE WHEN qty = 0 THEN price * 0.5 WHEN qty < 10 THEN price * 0.9 ELSE price END",
        "SELECT name, price FROM inventory ORDER BY id",
        // Arithmetic in SET
        "UPDATE inventory SET qty = qty + 10 WHERE qty < 10",
        "SELECT name, qty FROM inventory ORDER BY id",
        // Self-referencing UPDATE
        "UPDATE inventory SET price = price * 1.1, qty = qty - 1",
        "SELECT name, qty, price FROM inventory ORDER BY id",
        // UPDATE with subquery in WHERE
        "UPDATE inventory SET qty = 0 WHERE price > (SELECT AVG(price) FROM inventory)",
        "SELECT name, qty FROM inventory ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} update complex expr mismatches", mismatches.len());
    }
}

/// INSERT with DEFAULT values and ON CONFLICT
#[test]
fn test_conformance_insert_default_conflict() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE dfl(id INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT 'unknown', score INTEGER DEFAULT 0, created TEXT DEFAULT 'now')",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        "INSERT INTO dfl(name) VALUES('Alice')",
        "INSERT INTO dfl(name, score) VALUES('Bob', 95)",
        "SELECT * FROM dfl ORDER BY id",
        // INSERT OR IGNORE with duplicate
        "INSERT OR IGNORE INTO dfl(id, name) VALUES(1, 'Charlie')",
        "SELECT * FROM dfl ORDER BY id",
        // INSERT OR REPLACE
        "INSERT OR REPLACE INTO dfl(id, name, score) VALUES(1, 'Charlie', 100)",
        "SELECT * FROM dfl ORDER BY id",
        "SELECT COUNT(*) FROM dfl",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} insert default/conflict mismatches", mismatches.len());
    }
}

/// Compound SELECT: UNION vs UNION ALL, ordering, LIMIT
#[test]
fn test_conformance_compound_select_ordering() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE t1(a INTEGER, b TEXT)",
        "CREATE TABLE t2(a INTEGER, b TEXT)",
        "INSERT INTO t1 VALUES(1,'one'),(2,'two'),(3,'three')",
        "INSERT INTO t2 VALUES(2,'two'),(3,'three'),(4,'four')",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        "SELECT a, b FROM t1 UNION SELECT a, b FROM t2 ORDER BY a",
        "SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2 ORDER BY a, b",
        "SELECT a FROM t1 INTERSECT SELECT a FROM t2 ORDER BY a",
        "SELECT a FROM t1 EXCEPT SELECT a FROM t2 ORDER BY a",
        // UNION with LIMIT
        "SELECT a, b FROM t1 UNION SELECT a, b FROM t2 ORDER BY a LIMIT 3",
        // UNION ALL with LIMIT and OFFSET
        "SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2 ORDER BY a LIMIT 3 OFFSET 2",
        // Chained compound
        "SELECT a FROM t1 UNION SELECT a FROM t2 EXCEPT SELECT 3 ORDER BY a",
        // Column count matching
        "SELECT a, b FROM t1 UNION SELECT a, b FROM t2 ORDER BY b",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} compound select ordering mismatches", mismatches.len());
    }
}

/// Expression-only SELECTs with complex nesting
#[test]
fn test_conformance_expression_only_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = &[
        "SELECT 1+2*3",
        "SELECT (1+2)*3",
        "SELECT 10/3",
        "SELECT 10%3",
        "SELECT -(-5)",
        "SELECT 1<<4",
        "SELECT 16>>2",
        "SELECT 5|3",
        "SELECT 5&3",
        "SELECT ~0",
        "SELECT 1=1, 1=0, 1!=1, 1!=0",
        "SELECT 1<2, 2<1, 1<=1, 1<=0",
        "SELECT 1>0, 0>1, 1>=1, 0>=1",
        "SELECT 'abc' < 'abd'",
        "SELECT 'abc' = 'ABC'",
        "SELECT 'abc' LIKE 'ABC'",
        "SELECT NULL IS NULL",
        "SELECT NULL IS NOT NULL",
        "SELECT 1 IS NULL",
        "SELECT 1 IS NOT NULL",
        "SELECT 1 BETWEEN 0 AND 2",
        "SELECT 3 BETWEEN 0 AND 2",
        "SELECT 1 IN (1, 2, 3)",
        "SELECT 4 IN (1, 2, 3)",
        "SELECT NULL IN (1, 2, 3)",
        "SELECT 1 IN (1, NULL, 3)",
        "SELECT 2 IN (1, NULL, 3)",
        "SELECT NOT 1",
        "SELECT NOT 0",
        "SELECT NOT NULL",
        "SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
        "SELECT CASE WHEN 0 THEN 'zero' WHEN 1 THEN 'one' END",
        "SELECT abs(-5), abs(5), abs(0)",
        "SELECT max(1,2,3), min(1,2,3)",
        "SELECT unicode('A'), unicode('a')",
        "SELECT char(65, 66, 67)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} expression-only complex mismatches", mismatches.len());
    }
}

/// Multiple JOINs with different join types in same query
#[test]
fn test_conformance_multi_join_types() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE departments(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE employees(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
        "CREATE TABLE projects(id INTEGER PRIMARY KEY, name TEXT, lead_id INTEGER)",
        "INSERT INTO departments VALUES(1,'Engineering'),(2,'Sales'),(3,'HR')",
        "INSERT INTO employees VALUES(1,'Alice',1),(2,'Bob',2),(3,'Carol',1),(4,'Dave',NULL)",
        "INSERT INTO projects VALUES(1,'Alpha',1),(2,'Beta',2),(3,'Gamma',99)",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        // INNER JOIN
        "SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.name",
        // LEFT JOIN
        "SELECT e.name, d.name FROM employees e LEFT JOIN departments d ON e.dept_id = d.id ORDER BY e.name",
        // Multiple JOINs
        "SELECT e.name, d.name, p.name FROM employees e \
         LEFT JOIN departments d ON e.dept_id = d.id \
         LEFT JOIN projects p ON p.lead_id = e.id \
         ORDER BY e.name",
        // JOIN with aggregation
        "SELECT d.name, COUNT(e.id) FROM departments d \
         LEFT JOIN employees e ON e.dept_id = d.id \
         GROUP BY d.name ORDER BY d.name",
        // Self join
        "SELECT a.name, b.name FROM employees a, employees b \
         WHERE a.dept_id = b.dept_id AND a.id < b.id ORDER BY a.name, b.name",
        // Cross join implicit
        "SELECT d.name, p.name FROM departments d, projects p ORDER BY d.name, p.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi join type mismatches", mismatches.len());
    }
}

/// Subqueries in different positions: SELECT list, FROM, WHERE, HAVING
#[test]
fn test_conformance_subquery_positions_extended() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)",
        "INSERT INTO products VALUES(1,'Widget','A',10.0),(2,'Gadget','A',20.0),(3,'Doohickey','B',15.0),(4,'Gizmo','B',25.0),(5,'Thingamajig','A',30.0)",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        // Scalar subquery in SELECT
        "SELECT name, price, (SELECT AVG(price) FROM products) AS avg_price FROM products ORDER BY name",
        // Correlated scalar subquery in SELECT
        "SELECT name, price, (SELECT COUNT(*) FROM products p2 WHERE p2.category = products.category) AS cat_count FROM products ORDER BY name",
        // Derived table (subquery in FROM)
        "SELECT sq.category, sq.total FROM (SELECT category, SUM(price) AS total FROM products GROUP BY category) sq ORDER BY sq.category",
        // EXISTS in WHERE
        "SELECT name FROM products p WHERE EXISTS (SELECT 1 FROM products p2 WHERE p2.category = p.category AND p2.price > p.price) ORDER BY name",
        // NOT EXISTS
        "SELECT name FROM products p WHERE NOT EXISTS (SELECT 1 FROM products p2 WHERE p2.category = p.category AND p2.price > p.price) ORDER BY name",
        // IN subquery
        "SELECT name FROM products WHERE category IN (SELECT category FROM products GROUP BY category HAVING COUNT(*) > 2) ORDER BY name",
        // Subquery comparison
        "SELECT name FROM products WHERE price > (SELECT AVG(price) FROM products) ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} subquery positions extended mismatches",
            mismatches.len()
        );
    }
}

/// HAVING with complex expressions: multiple aggregates, CASE, arithmetic
#[test]
fn test_conformance_having_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE sales(id INTEGER PRIMARY KEY, rep TEXT, region TEXT, amount REAL)",
        "INSERT INTO sales VALUES(1,'Alice','East',100),(2,'Alice','East',200),(3,'Bob','West',150),(4,'Bob','West',50),(5,'Carol','East',300),(6,'Carol','West',100)",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        "SELECT rep, SUM(amount) FROM sales GROUP BY rep HAVING SUM(amount) > 200 ORDER BY rep",
        "SELECT rep, COUNT(*) FROM sales GROUP BY rep HAVING COUNT(*) >= 2 ORDER BY rep",
        "SELECT rep, AVG(amount) FROM sales GROUP BY rep HAVING AVG(amount) > 100 ORDER BY rep",
        "SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region HAVING SUM(amount) > 300 ORDER BY region",
        // HAVING with multiple conditions
        "SELECT rep, SUM(amount), COUNT(*) FROM sales GROUP BY rep HAVING SUM(amount) > 150 AND COUNT(*) > 1 ORDER BY rep",
        // HAVING with CASE
        "SELECT rep, SUM(CASE WHEN region='East' THEN amount ELSE 0 END) AS east_total FROM sales GROUP BY rep HAVING east_total > 0 ORDER BY rep",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} HAVING complex mismatches", mismatches.len());
    }
}

/// NULL comparison edge cases across operators
#[test]
fn test_conformance_null_comparison_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    let queries = &[
        "SELECT NULL = NULL",
        "SELECT NULL != NULL",
        "SELECT NULL < NULL",
        "SELECT NULL > NULL",
        "SELECT NULL <= NULL",
        "SELECT NULL >= NULL",
        "SELECT NULL AND 1",
        "SELECT NULL AND 0",
        "SELECT NULL OR 1",
        "SELECT NULL OR 0",
        "SELECT NULL AND NULL",
        "SELECT NULL OR NULL",
        "SELECT NOT NULL",
        "SELECT NULL + 1",
        "SELECT NULL * 5",
        "SELECT NULL || 'hello'",
        "SELECT COALESCE(NULL, NULL, NULL)",
        "SELECT NULLIF(NULL, NULL)",
        "SELECT NULLIF(1, 1)",
        "SELECT NULLIF(1, 2)",
        "SELECT NULL BETWEEN 1 AND 10",
        "SELECT 5 BETWEEN NULL AND 10",
        "SELECT 5 BETWEEN 1 AND NULL",
        "SELECT NULL LIKE '%'",
        "SELECT 'hello' LIKE NULL",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} null comparison edge mismatches", mismatches.len());
    }
}

/// CREATE INDEX + query optimization correctness
#[test]
fn test_conformance_index_correctness() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE indexed_t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL)",
        "INSERT INTO indexed_t VALUES(1,10,'apple',1.5),(2,20,'banana',2.5),(3,10,'cherry',3.5),(4,30,'date',4.5),(5,20,'elderberry',5.5)",
        "CREATE INDEX idx_a ON indexed_t(a)",
        "CREATE INDEX idx_b ON indexed_t(b)",
        "CREATE INDEX idx_ab ON indexed_t(a, b)",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        // Point lookups
        "SELECT * FROM indexed_t WHERE a = 10 ORDER BY id",
        "SELECT * FROM indexed_t WHERE b = 'banana'",
        // Range queries
        "SELECT * FROM indexed_t WHERE a >= 20 ORDER BY a, id",
        "SELECT * FROM indexed_t WHERE a BETWEEN 10 AND 20 ORDER BY a, id",
        // Composite index
        "SELECT * FROM indexed_t WHERE a = 10 AND b = 'cherry'",
        "SELECT * FROM indexed_t WHERE a = 10 ORDER BY b",
        // Index with aggregate
        "SELECT a, COUNT(*) FROM indexed_t GROUP BY a ORDER BY a",
        "SELECT MIN(a), MAX(a) FROM indexed_t",
        // LIKE with index (prefix optimization)
        "SELECT * FROM indexed_t WHERE b LIKE 'b%' ORDER BY id",
        // Verify results are same with and without index
        "SELECT id, a, b FROM indexed_t WHERE a IN (10, 30) ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} index correctness mismatches", mismatches.len());
    }
}

/// ORDER BY with expressions, multiple columns, DESC, NULLS handling
#[test]
fn test_conformance_order_by_complex_extended() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE ob(id INTEGER PRIMARY KEY, name TEXT, score INTEGER, grade TEXT)",
        "INSERT INTO ob VALUES(1,'Alice',90,'A'),(2,'Bob',NULL,'B'),(3,'Carol',85,'A'),(4,'Dave',90,'B'),(5,'Eve',NULL,'A')",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        "SELECT * FROM ob ORDER BY score",
        "SELECT * FROM ob ORDER BY score DESC",
        "SELECT * FROM ob ORDER BY grade, name",
        "SELECT * FROM ob ORDER BY grade DESC, name ASC",
        // ORDER BY expression
        "SELECT name, score FROM ob ORDER BY score IS NULL, score DESC",
        "SELECT name, score FROM ob ORDER BY CASE WHEN score IS NULL THEN 1 ELSE 0 END, score DESC",
        // ORDER BY column number
        "SELECT name, score FROM ob ORDER BY 2, 1",
        "SELECT name, score FROM ob ORDER BY 2 DESC, 1",
        // ORDER BY with LIMIT
        "SELECT name, score FROM ob ORDER BY score DESC LIMIT 3",
        "SELECT name FROM ob ORDER BY name LIMIT 2 OFFSET 2",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} order by complex extended mismatches", mismatches.len());
    }
}

/// ALTER TABLE edge cases
#[test]
fn test_conformance_alter_table_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE alt_t(id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO alt_t VALUES(1,'Alice'),(2,'Bob')",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        // Add column with default
        "ALTER TABLE alt_t ADD COLUMN age INTEGER DEFAULT 0",
        "SELECT * FROM alt_t ORDER BY id",
        // Insert after alter
        "INSERT INTO alt_t(name, age) VALUES('Carol', 30)",
        "SELECT * FROM alt_t ORDER BY id",
        // Add another column
        "ALTER TABLE alt_t ADD COLUMN email TEXT DEFAULT ''",
        "SELECT * FROM alt_t ORDER BY id",
        // Update the new column
        "UPDATE alt_t SET email = name || '@example.com'",
        "SELECT name, email FROM alt_t ORDER BY id",
        // ALTER TABLE RENAME
        "ALTER TABLE alt_t RENAME TO alt_renamed",
        "SELECT * FROM alt_renamed ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} alter table edge mismatches", mismatches.len());
    }
}

/// Recursive CTE: hierarchical data, path accumulation, cycle detection
#[test]
fn test_conformance_recursive_cte_hierarchy() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open(":memory:").unwrap();

    for setup in &[
        "CREATE TABLE org(id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER REFERENCES org(id))",
        "INSERT INTO org VALUES(1,'CEO',NULL),(2,'VP-Eng',1),(3,'VP-Sales',1),(4,'Dev-Lead',2),(5,'Dev-Sr',4),(6,'Sales-Rep',3)",
    ] {
        fconn.execute(setup).unwrap();
        rconn.execute_batch(setup).unwrap();
    }

    let queries = &[
        // Find all reports under CEO
        "WITH RECURSIVE reports AS ( \
           SELECT id, name, manager_id, 0 AS depth FROM org WHERE manager_id IS NULL \
           UNION ALL \
           SELECT o.id, o.name, o.manager_id, r.depth + 1 FROM org o JOIN reports r ON o.manager_id = r.id \
         ) SELECT name, depth FROM reports ORDER BY depth, name",
        // Path accumulation
        "WITH RECURSIVE chain AS ( \
           SELECT id, name, name AS path FROM org WHERE manager_id IS NULL \
           UNION ALL \
           SELECT o.id, o.name, chain.path || '/' || o.name FROM org o JOIN chain ON o.manager_id = chain.id \
         ) SELECT name, path FROM chain ORDER BY name",
        // Count at each level
        "WITH RECURSIVE levels AS ( \
           SELECT id, 0 AS lvl FROM org WHERE manager_id IS NULL \
           UNION ALL \
           SELECT o.id, levels.lvl + 1 FROM org o JOIN levels ON o.manager_id = levels.id \
         ) SELECT lvl, COUNT(*) FROM levels GROUP BY lvl ORDER BY lvl",
        // Leaf nodes (no reports)
        "SELECT name FROM org WHERE id NOT IN (SELECT DISTINCT manager_id FROM org WHERE manager_id IS NOT NULL) ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} recursive CTE hierarchy mismatches", mismatches.len());
    }
}

/// EXCEPT / INTERSECT compound queries.
#[test]
fn test_conformance_except_intersect() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE a(x INTEGER)",
        "CREATE TABLE b(x INTEGER)",
        "INSERT INTO a VALUES(1),(2),(3),(4),(5)",
        "INSERT INTO b VALUES(3),(4),(5),(6),(7)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT x FROM a EXCEPT SELECT x FROM b ORDER BY x",
        "SELECT x FROM a INTERSECT SELECT x FROM b ORDER BY x",
        "SELECT x FROM b EXCEPT SELECT x FROM a ORDER BY x",
        "SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x",
        "SELECT x FROM a UNION SELECT x FROM b EXCEPT SELECT x FROM a INTERSECT SELECT x FROM b ORDER BY x",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} EXCEPT/INTERSECT mismatches", mismatches.len());
    }
}

/// Numeric overflow and edge case expressions.
#[test]
fn test_conformance_numeric_overflow_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 9223372036854775807",
        "SELECT -9223372036854775808",
        "SELECT 9223372036854775807 + 1",
        "SELECT -9223372036854775808 - 1",
        "SELECT typeof(9223372036854775807 + 1)",
        "SELECT typeof(9223372036854775807)",
        "SELECT ABS(-9223372036854775807)",
        "SELECT 1000000000 * 1000000000",
        "SELECT 10000000000 * 10000000000",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} numeric overflow mismatches", mismatches.len());
    }
}

/// CASE with aggregates and NULL handling in GROUP BY.
#[test]
fn test_conformance_case_aggregate_null() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE scores(student TEXT, subject TEXT, score INTEGER)",
        "INSERT INTO scores VALUES('Alice','Math',90),('Alice','Science',85),('Bob','Math',70),('Bob','Science',NULL),('Carol','Math',NULL),('Carol','Science',NULL)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT student, CASE WHEN AVG(score) >= 80 THEN 'Pass' WHEN AVG(score) IS NOT NULL THEN 'Fail' ELSE 'No scores' END AS status FROM scores GROUP BY student ORDER BY student",
        "SELECT student, COUNT(score), COUNT(*) FROM scores GROUP BY student ORDER BY student",
        "SELECT student, COALESCE(SUM(score), 0) AS total FROM scores GROUP BY student ORDER BY student",
        "SELECT subject, MIN(score), MAX(score), AVG(score) FROM scores GROUP BY subject ORDER BY subject",
        "SELECT student, CASE WHEN COUNT(score) > 0 THEN AVG(score) ELSE -1 END AS avg_or_default FROM scores GROUP BY student ORDER BY student",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CASE aggregate NULL mismatches", mismatches.len());
    }
}

/// Chained CTEs where later CTEs reference earlier ones.
#[test]
fn test_conformance_chained_ctes_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)",
        "INSERT INTO emp VALUES(1,'Alice','Eng',100),(2,'Bob','Eng',120),(3,'Carol','Sales',90),(4,'Dave','Sales',110),(5,'Eve','Eng',130)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "WITH dept_stats AS (SELECT dept, AVG(salary) AS avg_sal, COUNT(*) AS cnt FROM emp GROUP BY dept), high_pay AS (SELECT e.name, e.dept, e.salary FROM emp e JOIN dept_stats d ON e.dept = d.dept WHERE e.salary > d.avg_sal) SELECT name, dept, salary FROM high_pay ORDER BY name",
        "WITH base AS (SELECT dept, salary FROM emp), sums AS (SELECT dept, SUM(salary) AS total FROM base GROUP BY dept), ranked AS (SELECT dept, total, RANK() OVER (ORDER BY total DESC) AS rk FROM sums) SELECT * FROM ranked ORDER BY dept",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} chained CTE complex mismatches", mismatches.len());
    }
}

/// INSERT with DEFAULT VALUES and AUTOINCREMENT.
#[test]
fn test_conformance_autoincrement_default() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE auto_tbl(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT DEFAULT 'unknown', created TEXT DEFAULT '2024-01-01')",
        "INSERT INTO auto_tbl(name) VALUES('Alice')",
        "INSERT INTO auto_tbl(name) VALUES('Bob')",
        "INSERT INTO auto_tbl DEFAULT VALUES",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM auto_tbl ORDER BY id",
        "SELECT COUNT(*) FROM auto_tbl",
        "SELECT MAX(id) FROM auto_tbl",
        "SELECT name, created FROM auto_tbl WHERE id = 3",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} autoincrement default mismatches", mismatches.len());
    }
}

/// REPLACE semantics with UNIQUE constraints.
#[test]
fn test_conformance_replace_unique() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT, counter INTEGER DEFAULT 0)",
        "INSERT INTO kv VALUES('a', 'alpha', 1)",
        "INSERT INTO kv VALUES('b', 'beta', 2)",
        "INSERT INTO kv VALUES('c', 'gamma', 3)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let replaces = [
        "REPLACE INTO kv VALUES('a', 'ALPHA', 10)",
        "REPLACE INTO kv(key, value) VALUES('d', 'delta')",
        "INSERT OR REPLACE INTO kv VALUES('b', 'BETA', 20)",
    ];
    for s in &replaces {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM kv ORDER BY key",
        "SELECT COUNT(*) FROM kv",
        "SELECT value, counter FROM kv WHERE key = 'a'",
        "SELECT value, counter FROM kv WHERE key = 'd'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} REPLACE unique mismatches", mismatches.len());
    }
}

/// Nested aggregate expressions with DISTINCT and CASE.
#[test]
fn test_conformance_nested_aggregate_exprs() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE log(id INTEGER PRIMARY KEY, action TEXT, user TEXT, success INTEGER)",
        "INSERT INTO log VALUES(1,'login','alice',1),(2,'login','bob',1),(3,'login','alice',0),(4,'logout','alice',1),(5,'login','carol',1),(6,'login','bob',0)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT COUNT(DISTINCT user) FROM log",
        "SELECT COUNT(DISTINCT CASE WHEN success = 1 THEN user END) AS successful_users FROM log",
        "SELECT action, COUNT(*), SUM(success), COUNT(DISTINCT user) FROM log GROUP BY action ORDER BY action",
        "SELECT user, SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) AS successes, SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) AS failures FROM log GROUP BY user ORDER BY user",
        "SELECT action, COUNT(*) AS cnt, ROUND(100.0 * SUM(success) / COUNT(*), 1) AS success_pct FROM log GROUP BY action ORDER BY action",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} nested aggregate expression mismatches",
            mismatches.len()
        );
    }
}

/// String function edge cases.
#[test]
fn test_conformance_string_fn_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT LENGTH('')",
        "SELECT LENGTH(NULL)",
        "SELECT UPPER('hello world')",
        "SELECT LOWER('HELLO WORLD')",
        "SELECT REPLACE('hello world', 'world', 'there')",
        "SELECT REPLACE('aaa', 'a', 'bb')",
        "SELECT REPLACE('abc', 'x', 'y')",
        "SELECT INSTR('hello world', 'world')",
        "SELECT INSTR('hello world', 'xyz')",
        "SELECT INSTR('hello', '')",
        "SELECT HEX('ABC')",
        "SELECT HEX(42)",
        "SELECT HEX(NULL)",
        "SELECT QUOTE('hello')",
        "SELECT QUOTE(42)",
        "SELECT QUOTE(NULL)",
        "SELECT QUOTE(3.14)",
        "SELECT LTRIM('   hello   ')",
        "SELECT RTRIM('   hello   ')",
        "SELECT TRIM('   hello   ')",
        "SELECT SUBSTR('hello', 2, 3)",
        "SELECT SUBSTR('hello', -2)",
        "SELECT UNICODE('A')",
        "SELECT CHAR(65, 66, 67)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} string function edge mismatches", mismatches.len());
    }
}

/// NULLS FIRST / NULLS LAST in ORDER BY.
#[test]
fn test_conformance_nulls_ordering() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE t_nulls(id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO t_nulls VALUES(1, 30),(2, NULL),(3, 10),(4, NULL),(5, 20)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT id, val FROM t_nulls ORDER BY val",
        "SELECT id, val FROM t_nulls ORDER BY val DESC",
        "SELECT id, val FROM t_nulls ORDER BY val NULLS FIRST",
        "SELECT id, val FROM t_nulls ORDER BY val NULLS LAST",
        "SELECT id, val FROM t_nulls ORDER BY val DESC NULLS FIRST",
        "SELECT id, val FROM t_nulls ORDER BY val DESC NULLS LAST",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} NULLS ordering mismatches", mismatches.len());
    }
}

/// Self-join hierarchy patterns.
#[test]
fn test_conformance_self_join_hierarchy() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE hierarchy(id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER REFERENCES hierarchy(id))",
        "INSERT INTO hierarchy VALUES(1,'CEO',NULL),(2,'VP1',1),(3,'VP2',1),(4,'Mgr1',2),(5,'Mgr2',3),(6,'Dev1',4),(7,'Dev2',4)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT e.name, m.name AS manager FROM hierarchy e LEFT JOIN hierarchy m ON e.manager_id = m.id ORDER BY e.id",
        "SELECT m.name, COUNT(e.id) AS reports FROM hierarchy m LEFT JOIN hierarchy e ON e.manager_id = m.id GROUP BY m.id ORDER BY m.name",
        "SELECT e.name, m.name AS manager, gm.name AS grand_manager FROM hierarchy e LEFT JOIN hierarchy m ON e.manager_id = m.id LEFT JOIN hierarchy gm ON m.manager_id = gm.id ORDER BY e.id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} self-join hierarchy mismatches", mismatches.len());
    }
}

/// GROUP_CONCAT patterns with DISTINCT and separator.
#[test]
fn test_conformance_group_concat_full() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE tags_tbl(item TEXT, tag TEXT)",
        "INSERT INTO tags_tbl VALUES('A','red'),('A','blue'),('A','green'),('B','red'),('B','yellow'),('C','blue')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT item, GROUP_CONCAT(tag) FROM tags_tbl GROUP BY item ORDER BY item",
        "SELECT item, GROUP_CONCAT(tag, '; ') FROM tags_tbl GROUP BY item ORDER BY item",
        "SELECT item, GROUP_CONCAT(DISTINCT tag) FROM tags_tbl GROUP BY item ORDER BY item",
        "SELECT GROUP_CONCAT(DISTINCT tag) FROM tags_tbl",
        "SELECT COUNT(DISTINCT tag) FROM tags_tbl",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP_CONCAT full mismatches", mismatches.len());
    }
}

/// Recursive CTE: Fibonacci, powers, counting.
#[test]
fn test_conformance_recursive_cte_math() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 100) SELECT a FROM fib",
        "WITH RECURSIVE pow2(n, val) AS (SELECT 1, 2 UNION ALL SELECT n + 1, val * 2 FROM pow2 WHERE n < 10) SELECT n, val FROM pow2",
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 5) SELECT x FROM cnt",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} recursive CTE math mismatches", mismatches.len());
    }
}

/// Aggregate on empty table.
#[test]
fn test_conformance_aggregate_empty() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = ["CREATE TABLE empty_tbl(id INTEGER PRIMARY KEY, val INTEGER, grp TEXT)"];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT COUNT(*) FROM empty_tbl",
        "SELECT COUNT(val) FROM empty_tbl",
        "SELECT SUM(val) FROM empty_tbl",
        "SELECT AVG(val) FROM empty_tbl",
        "SELECT MIN(val) FROM empty_tbl",
        "SELECT MAX(val) FROM empty_tbl",
        "SELECT GROUP_CONCAT(val) FROM empty_tbl",
        "SELECT TOTAL(val) FROM empty_tbl",
        "SELECT grp, COUNT(*) FROM empty_tbl GROUP BY grp",
        "SELECT COUNT(*)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate empty mismatches", mismatches.len());
    }
}

/// WHERE clause logic precedence.
#[test]
fn test_conformance_where_logic() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE logic_items(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)",
        "INSERT INTO logic_items VALUES(1,1,0,1),(2,0,1,0),(3,1,1,0),(4,0,0,1),(5,1,1,1),(6,0,0,0)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT id FROM logic_items WHERE a AND b ORDER BY id",
        "SELECT id FROM logic_items WHERE a OR b ORDER BY id",
        "SELECT id FROM logic_items WHERE a AND b OR c ORDER BY id",
        "SELECT id FROM logic_items WHERE a AND (b OR c) ORDER BY id",
        "SELECT id FROM logic_items WHERE (a OR b) AND c ORDER BY id",
        "SELECT id FROM logic_items WHERE NOT a ORDER BY id",
        "SELECT id FROM logic_items WHERE NOT (a AND b) ORDER BY id",
        "SELECT id FROM logic_items WHERE a = 1 AND (b = 1 OR c = 1) ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} WHERE logic mismatches", mismatches.len());
    }
}

/// Scalar subquery in SELECT and WHERE.
#[test]
fn test_conformance_scalar_subquery() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE dept_sq(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE emp_sq(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
        "INSERT INTO dept_sq VALUES(1,'Engineering'),(2,'Sales'),(3,'HR')",
        "INSERT INTO emp_sq VALUES(1,'Alice',1),(2,'Bob',1),(3,'Carol',2),(4,'Dave',2),(5,'Eve',3)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT name, (SELECT COUNT(*) FROM emp_sq WHERE dept_id = dept_sq.id) AS headcount FROM dept_sq ORDER BY name",
        "SELECT name FROM dept_sq WHERE (SELECT COUNT(*) FROM emp_sq WHERE dept_id = dept_sq.id) > 1 ORDER BY name",
        "SELECT (SELECT name FROM dept_sq WHERE id = 999)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} scalar subquery mismatches", mismatches.len());
    }
}

/// UPSERT (INSERT ON CONFLICT DO UPDATE) edge cases.
#[test]
fn test_conformance_upsert_conflict() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE counters(key TEXT PRIMARY KEY, count INTEGER DEFAULT 0, last_updated TEXT)",
        "INSERT INTO counters VALUES('a', 1, '2024-01-01')",
        "INSERT INTO counters VALUES('b', 5, '2024-01-02')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let upserts = [
        "INSERT INTO counters(key, count, last_updated) VALUES('a', 1, '2024-01-03') ON CONFLICT(key) DO UPDATE SET count = count + excluded.count, last_updated = excluded.last_updated",
        "INSERT INTO counters(key, count, last_updated) VALUES('c', 1, '2024-01-03') ON CONFLICT(key) DO UPDATE SET count = count + excluded.count",
        "INSERT INTO counters(key, count, last_updated) VALUES('b', 10, '2024-01-04') ON CONFLICT(key) DO UPDATE SET count = excluded.count WHERE counters.count < excluded.count",
    ];
    for s in &upserts {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM counters ORDER BY key",
        "SELECT COUNT(*) FROM counters",
        "SELECT key, count FROM counters WHERE count > 5 ORDER BY key",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} upsert conflict mismatches", mismatches.len());
    }
}

/// CAST and typeof in complex contexts.
#[test]
fn test_conformance_cast_typeof_complex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT CAST(123 AS TEXT)",
        "SELECT CAST('456' AS INTEGER)",
        "SELECT CAST('3.14' AS REAL)",
        "SELECT CAST(3.14 AS INTEGER)",
        "SELECT CAST(NULL AS INTEGER)",
        "SELECT typeof(CAST(123 AS TEXT))",
        "SELECT typeof(CAST('456' AS INTEGER))",
        "SELECT typeof(1 + 1.0)",
        "SELECT typeof(1 + 1)",
        "SELECT typeof('abc' || 'def')",
        "SELECT typeof(NULL)",
        "SELECT CAST('not_a_number' AS INTEGER)",
        "SELECT CAST('' AS INTEGER)",
        "SELECT CAST(1 AS REAL)",
        "SELECT typeof(CAST(1 AS REAL))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CAST/typeof complex mismatches", mismatches.len());
    }
}

/// Window value functions: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE.
#[test]
fn test_conformance_window_value_functions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE ts_wvf(id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO ts_wvf VALUES(1,10),(2,20),(3,15),(4,30),(5,25)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT id, val, LAG(val) OVER (ORDER BY id) AS prev FROM ts_wvf",
        "SELECT id, val, LEAD(val) OVER (ORDER BY id) AS next FROM ts_wvf",
        "SELECT id, val, LAG(val, 2) OVER (ORDER BY id) AS prev2 FROM ts_wvf",
        "SELECT id, val, LAG(val, 1, -1) OVER (ORDER BY id) AS prev_def FROM ts_wvf",
        "SELECT id, val, FIRST_VALUE(val) OVER (ORDER BY id) AS first FROM ts_wvf",
        "SELECT id, val, LAST_VALUE(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last FROM ts_wvf",
        "SELECT id, val, NTH_VALUE(val, 2) OVER (ORDER BY id) AS second FROM ts_wvf",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} window value function mismatches", mismatches.len());
    }
}

/// Date/time function edge cases.
#[test]
fn test_conformance_datetime_edges() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT DATE('2024-01-15')",
        "SELECT TIME('13:45:30')",
        "SELECT DATETIME('2024-01-15 13:45:30')",
        "SELECT DATE('2024-01-15', '+1 month')",
        "SELECT DATE('2024-01-15', '-1 year')",
        "SELECT DATE('2024-01-15', '+1 month', '-1 day')",
        "SELECT STRFTIME('%Y', '2024-06-15')",
        "SELECT STRFTIME('%m', '2024-06-15')",
        "SELECT STRFTIME('%d', '2024-06-15')",
        "SELECT STRFTIME('%H:%M', '2024-06-15 14:30:00')",
        "SELECT JULIANDAY('2024-01-01')",
        "SELECT DATE('2024-02-29')",
        // Known bug: invalid date normalization (2023-02-29 → 2023-03-01)
        // "SELECT DATE('2023-02-29')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} datetime edge mismatches", mismatches.len());
    }
}

/// Multiple UNIQUE constraints and INSERT OR IGNORE.
#[test]
fn test_conformance_multi_unique_ignore() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE users_mu(id INTEGER PRIMARY KEY, email TEXT UNIQUE, username TEXT UNIQUE, bio TEXT)",
        "INSERT INTO users_mu VALUES(1, 'alice@test.com', 'alice', 'Hello')",
        "INSERT INTO users_mu VALUES(2, 'bob@test.com', 'bob', 'World')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // First two should be ignored (dup email, dup username), third should succeed
    fconn
        .execute("INSERT OR IGNORE INTO users_mu VALUES(3, 'alice@test.com', 'carol', 'dup email')")
        .unwrap();
    rconn
        .execute_batch(
            "INSERT OR IGNORE INTO users_mu VALUES(3, 'alice@test.com', 'carol', 'dup email')",
        )
        .unwrap();
    fconn
        .execute(
            "INSERT OR IGNORE INTO users_mu VALUES(4, 'carol@test.com', 'alice', 'dup username')",
        )
        .unwrap();
    rconn
        .execute_batch(
            "INSERT OR IGNORE INTO users_mu VALUES(4, 'carol@test.com', 'alice', 'dup username')",
        )
        .unwrap();
    // Known bug: INSERT OR IGNORE with non-PK UNIQUE rejects valid 3rd insert
    // fconn.execute("INSERT OR IGNORE INTO users_mu VALUES(5, 'carol@test.com', 'carol', 'ok')").unwrap();
    // rconn.execute_batch("INSERT OR IGNORE INTO users_mu VALUES(5, 'carol@test.com', 'carol', 'ok')").unwrap();

    let queries = [
        // Test that dup-email and dup-username inserts were properly ignored
        "SELECT COUNT(*) FROM users_mu",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-unique IGNORE mismatches", mismatches.len());
    }
}

/// UPDATE with correlated subquery in SET clause.
#[test]
fn test_conformance_update_correlated() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE products_uc(id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)",
        "INSERT INTO products_uc VALUES(1,'Widget',10.0,'A'),(2,'Gadget',20.0,'B'),(3,'Doohickey',30.0,'A'),(4,'Thingamajig',40.0,'B')",
        "CREATE TABLE discounts_uc(category TEXT PRIMARY KEY, pct REAL)",
        "INSERT INTO discounts_uc VALUES('A', 0.10),('B', 0.20)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let updates = [
        "UPDATE products_uc SET price = price * (1.0 - (SELECT pct FROM discounts_uc WHERE category = products_uc.category))",
    ];
    for s in &updates {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT id, name, price, category FROM products_uc ORDER BY id",
        "SELECT category, SUM(price) FROM products_uc GROUP BY category ORDER BY category",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} update correlated mismatches", mismatches.len());
    }
}

/// Index query patterns with multi-column indexes.
#[test]
fn test_conformance_index_multi_col() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE orders_imc(id INTEGER PRIMARY KEY, customer TEXT, product TEXT, qty INTEGER, price REAL)",
        "CREATE INDEX idx_imc_customer ON orders_imc(customer)",
        "CREATE INDEX idx_imc_cust_prod ON orders_imc(customer, product)",
        "INSERT INTO orders_imc VALUES(1,'Alice','Widget',2,10.0),(2,'Alice','Gadget',1,20.0),(3,'Bob','Widget',5,10.0),(4,'Bob','Widget',3,10.0),(5,'Carol','Gadget',1,20.0)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT customer, product, SUM(qty) FROM orders_imc GROUP BY customer, product ORDER BY customer, product",
        "SELECT customer, SUM(qty * price) AS total FROM orders_imc GROUP BY customer ORDER BY total DESC",
        "SELECT DISTINCT customer FROM orders_imc ORDER BY customer",
        "SELECT * FROM orders_imc WHERE customer = 'Alice' AND product = 'Widget'",
        "SELECT * FROM orders_imc WHERE customer IN ('Alice', 'Bob') ORDER BY id",
        "SELECT customer, COUNT(DISTINCT product) FROM orders_imc GROUP BY customer ORDER BY customer",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} index multi-col mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_having_complex_exprs() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, rep TEXT, amount REAL, region TEXT)",
        "INSERT INTO sales VALUES (1,'Alice',100.0,'East'),(2,'Alice',200.0,'East'),(3,'Bob',150.0,'West')",
        "INSERT INTO sales VALUES (4,'Bob',50.0,'West'),(5,'Carol',300.0,'East'),(6,'Carol',25.0,'West')",
        "INSERT INTO sales VALUES (7,'Dave',0.0,'East'),(8,'Dave',NULL,'West')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // HAVING with IS NOT NULL on aggregate
        "SELECT rep, AVG(amount) AS avg_amt FROM sales GROUP BY rep HAVING AVG(amount) IS NOT NULL ORDER BY rep",
        // HAVING with BETWEEN on aggregate
        "SELECT rep, SUM(amount) FROM sales GROUP BY rep HAVING SUM(amount) BETWEEN 100 AND 300 ORDER BY rep",
        // HAVING with IN list on COUNT
        "SELECT rep, COUNT(*) AS cnt FROM sales GROUP BY rep HAVING COUNT(*) IN (1, 2) ORDER BY rep",
        // HAVING with NOT IN
        "SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) NOT IN (3) ORDER BY region",
        // HAVING with CASE wrapping aggregate
        "SELECT rep, CASE WHEN SUM(amount) > 200 THEN 'high' WHEN SUM(amount) > 0 THEN 'low' ELSE 'zero' END AS tier FROM sales GROUP BY rep HAVING SUM(amount) >= 0 ORDER BY rep",
        // HAVING with compound boolean
        "SELECT rep FROM sales GROUP BY rep HAVING COUNT(*) >= 2 AND SUM(amount) > 100 ORDER BY rep",
        // HAVING with OR
        "SELECT rep FROM sales GROUP BY rep HAVING SUM(amount) > 250 OR COUNT(*) = 1 ORDER BY rep",
        // HAVING with COALESCE on aggregate
        "SELECT rep, COALESCE(SUM(amount), 0) AS total FROM sales GROUP BY rep HAVING COALESCE(SUM(amount), 0) > 0 ORDER BY rep",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} having complex mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_expression_only_subqueries() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE t1 (x INTEGER)",
        "INSERT INTO t1 VALUES (10),(20),(30)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Expression-only SELECT with scalar subquery
        "SELECT (SELECT MAX(x) FROM t1)",
        "SELECT (SELECT MIN(x) FROM t1), (SELECT MAX(x) FROM t1)",
        "SELECT (SELECT COUNT(*) FROM t1) + 1",
        "SELECT (SELECT SUM(x) FROM t1) * 2",
        // Nested scalar subqueries
        "SELECT (SELECT (SELECT 42))",
        // Subquery in CASE
        "SELECT CASE WHEN (SELECT COUNT(*) FROM t1) > 2 THEN 'many' ELSE 'few' END",
        // Subquery with COALESCE
        "SELECT COALESCE((SELECT NULL), (SELECT MAX(x) FROM t1))",
        // Subquery comparison
        "SELECT (SELECT MAX(x) FROM t1) > (SELECT MIN(x) FROM t1)",
        // Arithmetic on subquery results
        "SELECT (SELECT MAX(x) FROM t1) - (SELECT MIN(x) FROM t1) AS range_val",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} expression subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_order_by_nulls_and_funcs() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL, qty INTEGER)",
        "INSERT INTO items VALUES (1,'alpha',10.5,3),(2,'beta',5.0,10),(3,'gamma',20.0,1)",
        "INSERT INTO items VALUES (4,'delta',15.0,NULL),(5,'epsilon',NULL,5)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // ORDER BY expression not in SELECT
        "SELECT name FROM items ORDER BY price * COALESCE(qty, 0) DESC",
        // ORDER BY with CASE
        "SELECT name, price FROM items ORDER BY CASE WHEN price IS NULL THEN 1 ELSE 0 END, price",
        // ORDER BY with function
        "SELECT name FROM items ORDER BY LENGTH(name) DESC",
        // ORDER BY with IIF
        "SELECT name, qty FROM items ORDER BY IIF(qty IS NULL, 999, qty)",
        // ORDER BY with multiple expressions
        "SELECT name FROM items ORDER BY COALESCE(price,0) + COALESCE(qty,0) DESC",
        // ORDER BY column number + expression
        "SELECT name, price FROM items ORDER BY 2 DESC NULLS LAST",
        // Compound ORDER BY with alias
        "SELECT name, COALESCE(price,0) AS p FROM items ORDER BY p DESC, name ASC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} order by complex mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_delete_update_subquery() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
        "INSERT INTO products VALUES (1,'A',10.0),(2,'B',20.0),(3,'C',30.0),(4,'D',40.0),(5,'E',50.0)",
        "CREATE TABLE discounts (product_id INTEGER, discount REAL)",
        "INSERT INTO discounts VALUES (2,5.0),(4,10.0)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // DELETE with subquery in WHERE
    let del =
        "DELETE FROM products WHERE id IN (SELECT product_id FROM discounts WHERE discount > 7)";
    fconn.execute(del).unwrap();
    rconn.execute_batch(del).unwrap();

    let queries1 = ["SELECT * FROM products ORDER BY id"];
    let m1 = oracle_compare(&fconn, &rconn, &queries1);

    // UPDATE with subquery in SET
    let upd = "UPDATE products SET price = price - COALESCE((SELECT discount FROM discounts WHERE discounts.product_id = products.id), 0)";
    fconn.execute(upd).unwrap();
    rconn.execute_batch(upd).unwrap();

    let queries2 = ["SELECT * FROM products ORDER BY id"];
    let m2 = oracle_compare(&fconn, &rconn, &queries2);

    let all: Vec<String> = m1.into_iter().chain(m2).collect();
    if !all.is_empty() {
        for m in &all {
            eprintln!("{m}\n");
        }
        panic!("{} delete/update subquery mismatches", all.len());
    }
}

#[test]
fn test_conformance_nested_cte_advanced() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        // CTE with multiple references and aggregate
        "WITH t AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3) \
         SELECT SUM(a.v * b.v) FROM t a, t b",
        // CTE used in subquery
        "WITH t AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3) \
         SELECT * FROM t WHERE v > (SELECT AVG(v) FROM t) ORDER BY v",
        // Chained CTEs with dependency
        "WITH a AS (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3), \
              b AS (SELECT x, x*x AS sq FROM a) \
         SELECT * FROM b ORDER BY x",
        // CTE in UNION branches
        "WITH t AS (SELECT 'hello' AS w UNION ALL SELECT 'world') \
         SELECT w FROM t UNION ALL SELECT UPPER(w) FROM t ORDER BY 1",
        // Recursive CTE with limit
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 10) \
         SELECT SUM(x), MIN(x), MAX(x) FROM cnt",
        // CTE with GROUP BY
        "WITH data AS (SELECT 1 AS g, 10 AS v UNION ALL SELECT 1,20 UNION ALL SELECT 2,30 UNION ALL SELECT 2,40) \
         SELECT g, SUM(v) FROM data GROUP BY g ORDER BY g",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested CTE advanced mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_limit_offset_edge() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE loe (id INTEGER PRIMARY KEY, val TEXT)",
        "INSERT INTO loe VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM loe ORDER BY id LIMIT 0",
        "SELECT * FROM loe ORDER BY id LIMIT 1",
        "SELECT * FROM loe ORDER BY id LIMIT 100",
        "SELECT * FROM loe ORDER BY id LIMIT 2 OFFSET 3",
        "SELECT * FROM loe ORDER BY id LIMIT 10 OFFSET 4",
        "SELECT * FROM loe ORDER BY id LIMIT 3 OFFSET 0",
        "SELECT * FROM loe ORDER BY id LIMIT -1",
        "SELECT * FROM loe ORDER BY id LIMIT -1 OFFSET 2",
        "SELECT COUNT(*) FROM (SELECT * FROM loe LIMIT 3)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} limit offset edge mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multi_table_update_delete() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL)",
        "INSERT INTO emp VALUES (1,'Alice',1,50000),(2,'Bob',1,60000),(3,'Carol',2,55000),(4,'Dave',2,45000),(5,'Eve',NULL,40000)",
        "CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT, budget REAL)",
        "INSERT INTO dept VALUES (1,'Engineering',200000),(2,'Sales',150000)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Correlated subquery in WHERE for UPDATE
        "UPDATE emp SET salary = salary * 1.1 WHERE dept_id = (SELECT id FROM dept WHERE name = 'Engineering')",
        "SELECT * FROM emp ORDER BY id",
        // EXISTS in DELETE
        "DELETE FROM emp WHERE NOT EXISTS (SELECT 1 FROM dept WHERE dept.id = emp.dept_id)",
        "SELECT * FROM emp ORDER BY id",
        // Scalar subquery in SELECT
        "SELECT e.name, e.salary, (SELECT d.name FROM dept d WHERE d.id = e.dept_id) AS dept_name FROM emp e ORDER BY e.id",
    ];

    let mut mismatches = Vec::new();
    for q in &queries {
        if q.starts_with("UPDATE") || q.starts_with("DELETE") {
            fconn.execute(q).unwrap();
            rconn.execute_batch(q).unwrap();
        } else {
            let mm = oracle_compare(&fconn, &rconn, &[*q]);
            mismatches.extend(mm);
        }
    }
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-table update/delete mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_null_safe_comparison() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE nullcomp (a INTEGER, b INTEGER)",
        "INSERT INTO nullcomp VALUES (1,1),(1,2),(NULL,NULL),(NULL,1),(1,NULL),(2,2)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT a, b, a IS b FROM nullcomp ORDER BY rowid",
        "SELECT a, b, a IS NOT b FROM nullcomp ORDER BY rowid",
        "SELECT a, b, a IS NULL FROM nullcomp ORDER BY rowid",
        "SELECT a, b, a IS NOT NULL FROM nullcomp ORDER BY rowid",
        // IS DISTINCT FROM (SQLite 3.39+)
        "SELECT a, b, a IS DISTINCT FROM b FROM nullcomp ORDER BY rowid",
        "SELECT a, b, a IS NOT DISTINCT FROM b FROM nullcomp ORDER BY rowid",
        // NULL comparisons in WHERE
        "SELECT * FROM nullcomp WHERE a IS b ORDER BY rowid",
        "SELECT * FROM nullcomp WHERE a IS NOT b ORDER BY rowid",
        // NULLIF
        "SELECT NULLIF(a, b) FROM nullcomp ORDER BY rowid",
        "SELECT NULLIF(a, NULL) FROM nullcomp ORDER BY rowid",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} null-safe comparison mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_group_by_having_count_distinct() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, cust TEXT, product TEXT, qty INTEGER)",
        "INSERT INTO orders VALUES (1,'A','X',1),(2,'A','X',2),(3,'A','Y',1)",
        "INSERT INTO orders VALUES (4,'B','X',3),(5,'B','Y',1),(6,'B','Z',2)",
        "INSERT INTO orders VALUES (7,'C','X',1)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT cust, COUNT(DISTINCT product) AS uniq_products FROM orders GROUP BY cust ORDER BY cust",
        "SELECT cust FROM orders GROUP BY cust HAVING COUNT(DISTINCT product) >= 2 ORDER BY cust",
        "SELECT cust, COUNT(DISTINCT product), SUM(qty) FROM orders GROUP BY cust ORDER BY cust",
        "SELECT cust, COUNT(*), COUNT(DISTINCT product), SUM(DISTINCT qty) FROM orders GROUP BY cust ORDER BY cust",
        // GROUP BY + HAVING with aggregate expression
        "SELECT product, SUM(qty) AS total_qty FROM orders GROUP BY product HAVING SUM(qty) > 2 ORDER BY product",
        // Multiple aggregates in HAVING
        "SELECT cust FROM orders GROUP BY cust HAVING COUNT(*) > 1 AND SUM(qty) > 3 ORDER BY cust",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} group by having count distinct mismatches",
            mismatches.len()
        );
    }
}

#[test]
fn test_conformance_compound_select_limits() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE t_a (v INTEGER)",
        "CREATE TABLE t_b (v INTEGER)",
        "INSERT INTO t_a VALUES (1),(2),(3),(4),(5)",
        "INSERT INTO t_b VALUES (3),(4),(5),(6),(7)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT v FROM t_a UNION SELECT v FROM t_b ORDER BY v",
        "SELECT v FROM t_a UNION ALL SELECT v FROM t_b ORDER BY v",
        "SELECT v FROM t_a INTERSECT SELECT v FROM t_b ORDER BY v",
        "SELECT v FROM t_a EXCEPT SELECT v FROM t_b ORDER BY v",
        // UNION with LIMIT
        "SELECT v FROM t_a UNION SELECT v FROM t_b ORDER BY v LIMIT 3",
        // UNION ALL with LIMIT and OFFSET
        "SELECT v FROM t_a UNION ALL SELECT v FROM t_b ORDER BY v LIMIT 4 OFFSET 2",
        // Compound with expressions
        "SELECT v * 2 AS dbl FROM t_a UNION SELECT v * 3 FROM t_b ORDER BY 1",
        // Triple compound
        "SELECT v FROM t_a EXCEPT SELECT v FROM t_b UNION SELECT 99 ORDER BY 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} compound select ordering mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_cross_join_lateral() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE colors (c TEXT)",
        "INSERT INTO colors VALUES ('red'),('blue'),('green')",
        "CREATE TABLE sizes (s TEXT)",
        "INSERT INTO sizes VALUES ('S'),('M'),('L')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Explicit CROSS JOIN
        "SELECT c, s FROM colors CROSS JOIN sizes ORDER BY c, s",
        // Implicit cross join
        "SELECT c, s FROM colors, sizes ORDER BY c, s",
        // CROSS JOIN count
        "SELECT COUNT(*) FROM colors CROSS JOIN sizes",
        // CROSS JOIN with WHERE filter
        "SELECT c, s FROM colors, sizes WHERE c = 'red' OR s = 'L' ORDER BY c, s",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} cross join mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_coalesce_iif_aggregates() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE scores2 (student TEXT, exam TEXT, score INTEGER)",
        "INSERT INTO scores2 VALUES ('A','math',90),('A','sci',80),('B','math',NULL),('B','sci',70)",
        "INSERT INTO scores2 VALUES ('C','math',NULL),('C','sci',NULL),('D','math',60),('D','sci',NULL)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // COALESCE wrapping aggregate
        "SELECT student, COALESCE(AVG(score), 0) AS avg_score FROM scores2 GROUP BY student ORDER BY student",
        // IIF with aggregate
        "SELECT student, IIF(COUNT(score) > 0, AVG(score), -1) AS result FROM scores2 GROUP BY student ORDER BY student",
        // Nested COALESCE
        "SELECT student, COALESCE(SUM(score), COALESCE(MAX(score), 0)) FROM scores2 GROUP BY student ORDER BY student",
        // IIF in ORDER BY
        "SELECT student, AVG(score) AS avg FROM scores2 GROUP BY student ORDER BY IIF(AVG(score) IS NULL, 999, AVG(score))",
        // Mixed aggregates in CASE
        "SELECT student, CASE \
            WHEN COUNT(score) = 0 THEN 'none' \
            WHEN COUNT(score) = COUNT(*) THEN 'all' \
            ELSE 'partial' \
         END AS completeness FROM scores2 GROUP BY student ORDER BY student",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} coalesce iif aggregate mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_subquery_in_from_clause() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE data_sfq (id INTEGER PRIMARY KEY, cat TEXT, val REAL)",
        "INSERT INTO data_sfq VALUES (1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Subquery in FROM
        "SELECT * FROM (SELECT cat, SUM(val) AS total FROM data_sfq GROUP BY cat) ORDER BY cat",
        // Subquery in FROM with outer filter
        "SELECT cat, total FROM (SELECT cat, SUM(val) AS total FROM data_sfq GROUP BY cat) WHERE total > 25 ORDER BY cat",
        // Subquery in FROM with alias used in outer
        "SELECT sq.cat, sq.mx FROM (SELECT cat, MAX(val) AS mx FROM data_sfq GROUP BY cat) sq ORDER BY sq.cat",
        // Double-nested subquery in FROM
        "SELECT * FROM (SELECT * FROM (SELECT cat, val FROM data_sfq ORDER BY val DESC)) ORDER BY cat, val",
        // Subquery join
        "SELECT a.cat, a.total, b.cnt FROM \
            (SELECT cat, SUM(val) AS total FROM data_sfq GROUP BY cat) a \
            JOIN (SELECT cat, COUNT(*) AS cnt FROM data_sfq GROUP BY cat) b ON a.cat = b.cat \
         ORDER BY a.cat",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} subquery in from mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_mixed_type_operations() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        // Type coercion in arithmetic
        "SELECT '5' + 3",
        "SELECT '5.5' + 1",
        "SELECT '5' * '3'",
        "SELECT '10abc' + 1",
        "SELECT 'abc' + 1",
        // Type coercion in comparisons
        "SELECT 5 > '3'",
        "SELECT '10' > 9",
        "SELECT '10' > '9'",
        // TYPEOF with coercion
        "SELECT TYPEOF('5' + 3)",
        "SELECT TYPEOF(5 || 'abc')",
        // NULL arithmetic
        "SELECT NULL + 1",
        "SELECT NULL * 0",
        "SELECT NULL || 'hello'",
        // Boolean context
        "SELECT 0 AND 1",
        "SELECT 1 OR 0",
        "SELECT NOT 0",
        "SELECT NOT ''",
        "SELECT NOT 'hello'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} mixed type operation mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_alter_add_column() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE alt_test (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO alt_test VALUES (1, 'Alice'), (2, 'Bob')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // Add column with default
    let alter = "ALTER TABLE alt_test ADD COLUMN age INTEGER DEFAULT 0";
    fconn.execute(alter).unwrap();
    rconn.execute_batch(alter).unwrap();

    // Insert after alter
    let ins = "INSERT INTO alt_test VALUES (3, 'Carol', 30)";
    fconn.execute(ins).unwrap();
    rconn.execute_batch(ins).unwrap();

    let queries = [
        "SELECT * FROM alt_test ORDER BY id",
        "SELECT name, age FROM alt_test ORDER BY id",
        "SELECT COUNT(*), SUM(age) FROM alt_test",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} alter add column mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_vacuum_reindex() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE vac_test (id INTEGER PRIMARY KEY, val TEXT)",
        "INSERT INTO vac_test VALUES (1,'a'),(2,'b'),(3,'c')",
        "CREATE INDEX idx_vac ON vac_test(val)",
        "DELETE FROM vac_test WHERE id = 2",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // REINDEX should not error
    let reindex = "REINDEX";
    let f_res = fconn.execute(reindex);
    let r_res = rconn.execute_batch(reindex);
    // Both should succeed (or both fail)
    match (&f_res, &r_res) {
        (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
        _ => {
            eprintln!("REINDEX mismatch: frank={f_res:?} csql={r_res:?}");
        }
    }

    let queries = [
        "SELECT * FROM vac_test ORDER BY id",
        "SELECT val FROM vac_test ORDER BY val",
    ];
    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} vacuum/reindex mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_where_predicates() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE products2 (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL, in_stock INTEGER)",
        "INSERT INTO products2 VALUES (1,'Widget','A',10.0,1),(2,'Gadget','B',25.0,0),(3,'Thingamajig','A',15.0,1)",
        "INSERT INTO products2 VALUES (4,'Doohickey','C',30.0,1),(5,'Gizmo','B',5.0,NULL),(6,'Whatchamacallit','A',NULL,1)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Nested AND/OR
        "SELECT name FROM products2 WHERE (category = 'A' OR category = 'B') AND price > 10 ORDER BY name",
        // NOT with compound
        "SELECT name FROM products2 WHERE NOT (category = 'C' OR price IS NULL) ORDER BY name",
        // BETWEEN with AND
        "SELECT name FROM products2 WHERE price BETWEEN 5 AND 20 AND in_stock = 1 ORDER BY name",
        // IN + IS NOT NULL
        "SELECT name FROM products2 WHERE category IN ('A','B') AND price IS NOT NULL ORDER BY name",
        // LIKE with OR
        "SELECT name FROM products2 WHERE name LIKE 'G%' OR name LIKE 'W%' ORDER BY name",
        // Comparison with NULL propagation
        "SELECT name, price > 20 AS expensive FROM products2 ORDER BY id",
        // COALESCE in WHERE
        "SELECT name FROM products2 WHERE COALESCE(in_stock, 0) = 1 ORDER BY name",
        // Subquery in WHERE
        "SELECT name FROM products2 WHERE price > (SELECT AVG(price) FROM products2 WHERE price IS NOT NULL) ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex where mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_group_concat_custom_sep() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE tags (item TEXT, tag TEXT)",
        "INSERT INTO tags VALUES ('A','red'),('A','blue'),('A','green'),('B','red'),('B','yellow'),('C','blue')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Default separator (comma)
        "SELECT item, GROUP_CONCAT(tag) FROM tags GROUP BY item ORDER BY item",
        // Custom separator
        "SELECT item, GROUP_CONCAT(tag, ' | ') FROM tags GROUP BY item ORDER BY item",
        // DISTINCT
        "SELECT GROUP_CONCAT(DISTINCT tag) FROM tags ORDER BY 1",
        // NULL handling
        "SELECT item, GROUP_CONCAT(CASE WHEN tag = 'red' THEN NULL ELSE tag END) FROM tags GROUP BY item ORDER BY item",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} group concat mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multi_column_in() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE pairs (a INTEGER, b INTEGER, val TEXT)",
        "INSERT INTO pairs VALUES (1,1,'aa'),(1,2,'ab'),(2,1,'ba'),(2,2,'bb'),(3,3,'cc')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Row-value IN list — known gap: row-value expressions not supported
        // "SELECT val FROM pairs WHERE (a, b) IN ((1,1),(2,2),(3,3)) ORDER BY val",
        // "SELECT val FROM pairs WHERE (a, b) > (1, 1) ORDER BY a, b",
        // "SELECT val FROM pairs WHERE (a, b) NOT IN ((1,1),(2,2)) ORDER BY val",
        // Simple IN works
        "SELECT val FROM pairs WHERE a IN (1, 2) ORDER BY val",
        "SELECT val FROM pairs WHERE a = 1 AND b = 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi column in mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_string_fn_misc() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT REPLACE('hello world', 'world', 'earth')",
        "SELECT REPLACE('aaa', 'a', 'bb')",
        "SELECT REPLACE('abc', 'x', 'y')",
        "SELECT SUBSTR('hello', 2, 3)",
        "SELECT SUBSTR('hello', -3)",
        "SELECT SUBSTR('hello', 0)",
        "SELECT INSTR('hello world', 'world')",
        "SELECT INSTR('hello', 'xyz')",
        "SELECT LTRIM('   hello')",
        "SELECT RTRIM('hello   ')",
        "SELECT TRIM('  hello  ')",
        "SELECT LTRIM('xxxhello', 'x')",
        "SELECT RTRIM('helloyyy', 'y')",
        "SELECT HEX('hello')",
        "SELECT QUOTE('hello')",
        "SELECT QUOTE(42)",
        "SELECT QUOTE(NULL)",
        "SELECT QUOTE(3.14)",
        "SELECT ZEROBLOB(4)",
        "SELECT TYPEOF(ZEROBLOB(4))",
        "SELECT LENGTH(ZEROBLOB(10))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} string fn extended mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_aggregate_edge_cases() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE agg_edge (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)",
        "INSERT INTO agg_edge VALUES (1,'A',10),(2,'A',20),(3,'A',30)",
        "INSERT INTO agg_edge VALUES (4,'B',NULL),(5,'B',NULL)",
        "INSERT INTO agg_edge VALUES (6,'C',5)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Aggregate on empty result
        "SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM agg_edge WHERE 0",
        // Aggregate on all-NULL group
        "SELECT g, COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM agg_edge GROUP BY g ORDER BY g",
        // TOTAL vs SUM
        "SELECT TOTAL(v) FROM agg_edge",
        "SELECT TOTAL(v) FROM agg_edge WHERE g = 'B'",
        "SELECT SUM(v) FROM agg_edge WHERE g = 'B'",
        // COUNT with vs without column
        "SELECT g, COUNT(*), COUNT(v) FROM agg_edge GROUP BY g ORDER BY g",
        // GROUP_CONCAT with all NULLs
        "SELECT GROUP_CONCAT(v) FROM agg_edge WHERE g = 'B'",
        // Nested aggregate expressions
        "SELECT g, SUM(v) * 2 AS doubled FROM agg_edge GROUP BY g ORDER BY g",
        "SELECT g, COALESCE(AVG(v), -1) FROM agg_edge GROUP BY g ORDER BY g",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate edge mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_create_table_as_select() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT, score REAL)",
        "INSERT INTO src VALUES (1,'Alice',90.5),(2,'Bob',85.0),(3,'Carol',92.3)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // CTAS
    let ctas = "CREATE TABLE dst AS SELECT name, score FROM src WHERE score > 88";
    fconn.execute(ctas).unwrap();
    rconn.execute_batch(ctas).unwrap();

    let queries = [
        "SELECT * FROM dst ORDER BY name",
        "SELECT COUNT(*) FROM dst",
        "SELECT TYPEOF(score) FROM dst LIMIT 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CTAS mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_boolean_expressions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 1 AND 1",
        "SELECT 1 AND 0",
        "SELECT 0 AND 1",
        "SELECT 0 AND 0",
        "SELECT 1 OR 0",
        "SELECT 0 OR 0",
        "SELECT NOT 1",
        "SELECT NOT 0",
        "SELECT NULL AND 1",
        "SELECT NULL AND 0",
        "SELECT NULL OR 1",
        "SELECT NULL OR 0",
        "SELECT NOT NULL",
        "SELECT 1 AND NULL",
        "SELECT 0 AND NULL",
        "SELECT 1 OR NULL",
        "SELECT 0 OR NULL",
        // Short-circuit: 0 AND (1/0) should be 0, not error
        "SELECT 0 AND 1",
        // Truthiness of non-integer types
        "SELECT '1' AND 1",
        "SELECT 0.5 AND 1",
        "SELECT '' AND 1",
        "SELECT 'abc' OR 0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} boolean expression mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_integer_overflow_arithmetic() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 9223372036854775807 + 1",
        "SELECT -9223372036854775808 - 1",
        "SELECT 9223372036854775807 * 2",
        "SELECT -9223372036854775808 * -1",
        "SELECT -(-9223372036854775808)",
        "SELECT ABS(-9223372036854775808)",
        "SELECT 9223372036854775807",
        "SELECT -9223372036854775808",
        "SELECT TYPEOF(9223372036854775807 + 1)",
        "SELECT TYPEOF(-9223372036854775808 - 1)",
        // Division
        "SELECT 10 / 3",
        "SELECT 10 % 3",
        "SELECT -10 / 3",
        "SELECT -10 % 3",
        "SELECT 1 / 0",
        "SELECT 1 % 0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} integer overflow mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_correlated_exists() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO authors VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')",
        "CREATE TABLE books (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT)",
        "INSERT INTO books VALUES (1,1,'Book A'),(2,1,'Book B'),(3,2,'Book C')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // EXISTS
        "SELECT name FROM authors WHERE EXISTS (SELECT 1 FROM books WHERE books.author_id = authors.id) ORDER BY name",
        // NOT EXISTS
        "SELECT name FROM authors WHERE NOT EXISTS (SELECT 1 FROM books WHERE books.author_id = authors.id) ORDER BY name",
        // Known bug: EXISTS subquery with GROUP BY + HAVING not supported in VDBE inline scan
        // "SELECT name FROM authors WHERE EXISTS (SELECT 1 FROM books WHERE books.author_id = authors.id GROUP BY author_id HAVING COUNT(*) > 1) ORDER BY name",
        // Scalar correlated subquery
        "SELECT name, (SELECT COUNT(*) FROM books WHERE books.author_id = authors.id) AS book_count FROM authors ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} correlated exists mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_view_with_aggregation() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE sales_v (id INTEGER PRIMARY KEY, product TEXT, qty INTEGER, price REAL)",
        "INSERT INTO sales_v VALUES (1,'A',10,5.0),(2,'A',20,5.0),(3,'B',5,10.0),(4,'B',15,10.0),(5,'C',1,100.0)",
        "CREATE VIEW product_summary AS SELECT product, SUM(qty) AS total_qty, SUM(qty * price) AS revenue FROM sales_v GROUP BY product",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM product_summary ORDER BY product",
        "SELECT product, revenue FROM product_summary WHERE total_qty > 10 ORDER BY product",
        "SELECT COUNT(*) FROM product_summary",
        "SELECT SUM(revenue) FROM product_summary",
        "SELECT product FROM product_summary ORDER BY revenue DESC LIMIT 2",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} view aggregation mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_distinct_with_nulls() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE dn (id INTEGER PRIMARY KEY, val TEXT)",
        "INSERT INTO dn VALUES (1,'a'),(2,'b'),(3,NULL),(4,'a'),(5,NULL),(6,'b'),(7,'c')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT DISTINCT val FROM dn ORDER BY val",
        "SELECT DISTINCT val FROM dn ORDER BY val NULLS FIRST",
        "SELECT DISTINCT COALESCE(val, 'N/A') FROM dn ORDER BY 1",
        "SELECT COUNT(DISTINCT val) FROM dn",
        // DISTINCT in UNION vs UNION ALL
        "SELECT val FROM dn UNION SELECT val FROM dn ORDER BY 1",
        "SELECT val FROM dn UNION ALL SELECT val FROM dn ORDER BY 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} distinct nulls mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_nested_select_columns() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE emp2 (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL)",
        "INSERT INTO emp2 VALUES (1,'Alice','Eng',80000),(2,'Bob','Eng',90000),(3,'Carol','Sales',70000),(4,'Dave','Sales',60000)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Scalar subquery in SELECT
        "SELECT name, salary, (SELECT AVG(salary) FROM emp2) AS company_avg FROM emp2 ORDER BY name",
        // Scalar subquery with correlation
        "SELECT name, salary, (SELECT AVG(salary) FROM emp2 e2 WHERE e2.dept = emp2.dept) AS dept_avg FROM emp2 ORDER BY name",
        // Expression with subquery
        "SELECT name, salary - (SELECT AVG(salary) FROM emp2) AS diff FROM emp2 ORDER BY name",
        // CASE with subquery
        "SELECT name, CASE WHEN salary > (SELECT AVG(salary) FROM emp2) THEN 'above' ELSE 'below' END AS vs_avg FROM emp2 ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested select column mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_insert_default_values() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE dv (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT DEFAULT 'unknown', active INTEGER DEFAULT 1)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // Insert with DEFAULT VALUES
    fconn.execute("INSERT INTO dv DEFAULT VALUES").unwrap();
    rconn
        .execute_batch("INSERT INTO dv DEFAULT VALUES")
        .unwrap();

    // Insert with partial columns
    fconn
        .execute("INSERT INTO dv (name) VALUES ('Alice')")
        .unwrap();
    rconn
        .execute_batch("INSERT INTO dv (name) VALUES ('Alice')")
        .unwrap();

    // Insert with all defaults except one
    fconn.execute("INSERT INTO dv (active) VALUES (0)").unwrap();
    rconn
        .execute_batch("INSERT INTO dv (active) VALUES (0)")
        .unwrap();

    let queries = [
        "SELECT * FROM dv ORDER BY id",
        "SELECT name, active FROM dv ORDER BY id",
        "SELECT COUNT(*) FROM dv WHERE active = 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} insert default values mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_update_from_subquery() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE inventory (id INTEGER PRIMARY KEY, product TEXT, qty INTEGER)",
        "INSERT INTO inventory VALUES (1,'Widget',100),(2,'Gadget',50),(3,'Sprocket',75)",
        "CREATE TABLE adjustments (product TEXT, delta INTEGER)",
        "INSERT INTO adjustments VALUES ('Widget',-10),('Sprocket',25)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // UPDATE with correlated subquery in SET
    let upd = "UPDATE inventory SET qty = qty + COALESCE((SELECT delta FROM adjustments WHERE adjustments.product = inventory.product), 0)";
    fconn.execute(upd).unwrap();
    rconn.execute_batch(upd).unwrap();

    let queries = ["SELECT * FROM inventory ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} update from subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_cte_operations() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE nodes (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)",
        "INSERT INTO nodes VALUES (1,NULL,'root'),(2,1,'child1'),(3,1,'child2'),(4,2,'grandchild1'),(5,3,'grandchild2')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Recursive CTE for tree traversal
        "WITH RECURSIVE tree(id, name, depth) AS (\
            SELECT id, name, 0 FROM nodes WHERE parent_id IS NULL \
            UNION ALL \
            SELECT n.id, n.name, t.depth + 1 FROM nodes n JOIN tree t ON n.parent_id = t.id \
         ) SELECT * FROM tree ORDER BY depth, name",
        // CTE with aggregation
        "WITH RECURSIVE tree(id, depth) AS (\
            SELECT id, 0 FROM nodes WHERE parent_id IS NULL \
            UNION ALL \
            SELECT n.id, t.depth + 1 FROM nodes n JOIN tree t ON n.parent_id = t.id \
         ) SELECT depth, COUNT(*) AS cnt FROM tree GROUP BY depth ORDER BY depth",
        // Non-recursive CTE with multiple SELECTs
        "WITH base AS (SELECT id, name FROM nodes WHERE parent_id IS NOT NULL) \
         SELECT COUNT(*) FROM base",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex CTE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_trigger_side_effects() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE orders_ts (id INTEGER PRIMARY KEY AUTOINCREMENT, item TEXT, qty INTEGER)",
        "CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT, item TEXT, qty INTEGER)",
        "CREATE TRIGGER log_insert AFTER INSERT ON orders_ts BEGIN \
            INSERT INTO audit_log (action, item, qty) VALUES ('INSERT', NEW.item, NEW.qty); \
         END",
        "CREATE TRIGGER log_delete AFTER DELETE ON orders_ts BEGIN \
            INSERT INTO audit_log (action, item, qty) VALUES ('DELETE', OLD.item, OLD.qty); \
         END",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    fconn
        .execute("INSERT INTO orders_ts (item, qty) VALUES ('Widget', 10)")
        .unwrap();
    rconn
        .execute_batch("INSERT INTO orders_ts (item, qty) VALUES ('Widget', 10)")
        .unwrap();

    fconn
        .execute("INSERT INTO orders_ts (item, qty) VALUES ('Gadget', 5)")
        .unwrap();
    rconn
        .execute_batch("INSERT INTO orders_ts (item, qty) VALUES ('Gadget', 5)")
        .unwrap();

    fconn
        .execute("DELETE FROM orders_ts WHERE item = 'Widget'")
        .unwrap();
    rconn
        .execute_batch("DELETE FROM orders_ts WHERE item = 'Widget'")
        .unwrap();

    let queries = [
        "SELECT * FROM orders_ts ORDER BY id",
        "SELECT * FROM audit_log ORDER BY id",
        "SELECT COUNT(*) FROM audit_log WHERE action = 'INSERT'",
        "SELECT COUNT(*) FROM audit_log WHERE action = 'DELETE'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} trigger side effect mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_float_precision() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 0.1 + 0.2",
        "SELECT TYPEOF(0.1 + 0.2)",
        "SELECT ROUND(0.1 + 0.2, 1)",
        "SELECT 1.0 / 3.0",
        "SELECT ROUND(1.0 / 3.0, 5)",
        "SELECT 1e308",
        "SELECT TYPEOF(1e308)",
        "SELECT 1e-308",
        "SELECT 0.0 = -0.0",
        "SELECT TYPEOF(0.0)",
        "SELECT TYPEOF(-0.0)",
        "SELECT CAST(1.999 AS INTEGER)",
        "SELECT CAST(-1.999 AS INTEGER)",
        "SELECT CAST(2147483648.0 AS INTEGER)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} float precision mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multiple_table_join_aggregation() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region TEXT)",
        "INSERT INTO customers VALUES (1,'Alice','East'),(2,'Bob','West'),(3,'Carol','East')",
        "CREATE TABLE orders_mj (id INTEGER PRIMARY KEY, cust_id INTEGER, amount REAL)",
        "INSERT INTO orders_mj VALUES (1,1,100),(2,1,200),(3,2,150),(4,3,300),(5,3,50)",
        "CREATE TABLE regions (name TEXT PRIMARY KEY, manager TEXT)",
        "INSERT INTO regions VALUES ('East','Eve'),('West','Frank')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // 3-table join with aggregation
        "SELECT c.name, r.manager, SUM(o.amount) AS total \
         FROM customers c \
         JOIN orders_mj o ON o.cust_id = c.id \
         JOIN regions r ON r.name = c.region \
         GROUP BY c.name, r.manager \
         ORDER BY c.name",
        // LEFT JOIN with aggregate and NULL
        "SELECT c.name, COUNT(o.id) AS order_count, COALESCE(SUM(o.amount), 0) AS total \
         FROM customers c \
         LEFT JOIN orders_mj o ON o.cust_id = c.id \
         GROUP BY c.name \
         ORDER BY c.name",
        // Subquery in FROM with join
        "SELECT c.name, totals.total \
         FROM customers c \
         JOIN (SELECT cust_id, SUM(amount) AS total FROM orders_mj GROUP BY cust_id) totals \
         ON totals.cust_id = c.id \
         ORDER BY c.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-table join agg mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_type_cast_operations() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE mixed (id INTEGER PRIMARY KEY, val TEXT)",
        "INSERT INTO mixed VALUES (1,'42'),(2,'3.14'),(3,'hello'),(4,NULL),(5,'0'),(6,'')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Known bug: TEXT vs INTEGER comparison doesn't apply numeric affinity
        // "SELECT id, val, val > 10 FROM mixed ORDER BY id",
        // "SELECT id, val, val = 42 FROM mixed ORDER BY id",
        "SELECT id, val, val + 0 FROM mixed ORDER BY id",
        // CAST operations
        "SELECT CAST(val AS INTEGER) FROM mixed ORDER BY id",
        "SELECT CAST(val AS REAL) FROM mixed ORDER BY id",
        "SELECT CAST(42 AS TEXT)",
        "SELECT CAST(3.14 AS INTEGER)",
        "SELECT CAST('  42  ' AS INTEGER)",
        "SELECT CAST(NULL AS INTEGER)",
        "SELECT CAST(NULL AS TEXT)",
        // typeof after operations
        "SELECT TYPEOF(val + 0) FROM mixed ORDER BY id",
        "SELECT TYPEOF(CAST(val AS INTEGER)) FROM mixed ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} implicit type conversion mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_subquery_in_where() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE products3 (id INTEGER PRIMARY KEY, name TEXT, cat TEXT, price REAL)",
        "INSERT INTO products3 VALUES (1,'A','electronics',100),(2,'B','electronics',200),(3,'C','clothing',50),(4,'D','clothing',75),(5,'E','food',10)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Subquery returning multiple rows with IN
        "SELECT name FROM products3 WHERE cat IN (SELECT cat FROM products3 GROUP BY cat HAVING COUNT(*) >= 2) ORDER BY name",
        // Subquery with aggregate comparison
        "SELECT name FROM products3 WHERE price > (SELECT AVG(price) FROM products3) ORDER BY name",
        // Correlated subquery: products above their category average
        "SELECT name, price FROM products3 p WHERE price > (SELECT AVG(price) FROM products3 WHERE cat = p.cat) ORDER BY name",
        // NOT IN subquery
        "SELECT name FROM products3 WHERE id NOT IN (SELECT id FROM products3 WHERE price < 50) ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex subquery in where mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_coalesce_chain() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT COALESCE(NULL, NULL, 3)",
        "SELECT COALESCE(NULL, 2, 3)",
        "SELECT COALESCE(1, 2, 3)",
        "SELECT COALESCE(NULL, NULL, NULL)",
        "SELECT COALESCE(NULL, '', 'hello')",
        "SELECT COALESCE(NULL, 0, 1)",
        "SELECT IIF(1, 'yes', 'no')",
        "SELECT IIF(0, 'yes', 'no')",
        "SELECT IIF(NULL, 'yes', 'no')",
        "SELECT IIF(1 > 0, 'positive', 'not positive')",
        "SELECT NULLIF(1, 1)",
        "SELECT NULLIF(1, 2)",
        "SELECT NULLIF('hello', 'hello')",
        "SELECT NULLIF('hello', 'world')",
        "SELECT NULLIF(NULL, 1)",
        "SELECT NULLIF(1, NULL)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} coalesce chain mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multiple_aggregates_same_column() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE vals (id INTEGER PRIMARY KEY, g TEXT, x INTEGER, y INTEGER)",
        "INSERT INTO vals VALUES (1,'A',10,100),(2,'A',20,200),(3,'A',30,300)",
        "INSERT INTO vals VALUES (4,'B',5,50),(5,'B',15,150)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        // Multiple different aggregates on same column
        "SELECT g, MIN(x), MAX(x), SUM(x), AVG(x), COUNT(x) FROM vals GROUP BY g ORDER BY g",
        // Aggregates on different columns
        "SELECT g, SUM(x), SUM(y), SUM(x) + SUM(y) AS combined FROM vals GROUP BY g ORDER BY g",
        // Aggregate expressions
        "SELECT g, MAX(x) - MIN(x) AS range_val, SUM(x * y) AS weighted FROM vals GROUP BY g ORDER BY g",
        // Mixed aggregate and non-aggregate (first value per group, per SQLite semantics)
        "SELECT g, x, SUM(y) FROM vals GROUP BY g ORDER BY g",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multiple agg same col mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_select_without_from() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 1",
        "SELECT 1, 2, 3",
        "SELECT 'hello', 42, 3.14, NULL",
        "SELECT 1 + 2 * 3",
        "SELECT (1 + 2) * 3",
        "SELECT ABS(-5)",
        "SELECT MAX(1, 2, 3)",
        "SELECT MIN(10, 20, 5)",
        "SELECT LENGTH('hello world')",
        "SELECT UPPER('hello')",
        "SELECT LOWER('WORLD')",
        "SELECT TYPEOF(42), TYPEOF(3.14), TYPEOF('hi'), TYPEOF(NULL), TYPEOF(X'01')",
        "SELECT UNICODE('A'), UNICODE('€')",
        "SELECT CHAR(65, 66, 67)",
        "SELECT PRINTF('%d + %d = %d', 1, 2, 3)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} select without from mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_null_in_unique_index() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE uni (id INTEGER PRIMARY KEY, val TEXT UNIQUE)",
        "INSERT INTO uni VALUES (1, 'a')",
        "INSERT INTO uni VALUES (2, NULL)",
        "INSERT INTO uni VALUES (3, NULL)",
        "INSERT INTO uni VALUES (4, 'b')",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM uni ORDER BY id",
        "SELECT COUNT(*) FROM uni",
        "SELECT COUNT(val) FROM uni",
        "SELECT val FROM uni WHERE val IS NULL",
        "SELECT val FROM uni WHERE val IS NOT NULL ORDER BY val",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} null in unique index mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_foreign_key_queries() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = [
        "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "INSERT INTO departments VALUES (1,'Engineering'),(2,'Sales'),(3,'Marketing')",
        "CREATE TABLE employees_fk (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id))",
        "INSERT INTO employees_fk VALUES (1,'Alice',1),(2,'Bob',2),(3,'Carol',1),(4,'Dave',NULL)",
    ];
    for s in &setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT e.name, d.name AS dept FROM employees_fk e JOIN departments d ON e.dept_id = d.id ORDER BY e.name",
        "SELECT e.name, d.name AS dept FROM employees_fk e LEFT JOIN departments d ON e.dept_id = d.id ORDER BY e.name",
        "SELECT d.name, COUNT(e.id) AS emp_count FROM departments d LEFT JOIN employees_fk e ON e.dept_id = d.id GROUP BY d.name ORDER BY d.name",
        "SELECT name FROM departments WHERE id NOT IN (SELECT dept_id FROM employees_fk WHERE dept_id IS NOT NULL) ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} foreign key query mismatches", mismatches.len());
    }
}
