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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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

/// Datetime functions: date(), time(), datetime(), strftime(), julianday().
#[test]
fn test_conformance_datetime_functions() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

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
