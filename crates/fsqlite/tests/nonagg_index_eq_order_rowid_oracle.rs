//! bd-nonagg-index-eq-order-rowid: `SELECT ... WHERE <single-col-indexed> = <val> ORDER BY <rowid>
//! [ASC|DESC] [LIMIT k]` is served by the index-EQUALITY seek — for the one eq value its index entries
//! ARE rowid order, so ASC seeks `(val, i64::MIN)` + walks forward and DESC seeks `(val, i64::MAX)` +
//! walks backward (Prev) — WITHOUT a sorter and WITHOUT full-scanning every row. A COMPOSITE index would
//! order by its trailing key column, so it declines. Compared IN OUTPUT ORDER against C SQLite.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;
fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
fn frank_ord(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    c.query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}
fn sqlite_ord(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}
fn opcode_present(c: &Connection, sql: &str, prefix: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with(prefix)))
}
fn opcode_count(c: &Connection, sql: &str, opcode: &str) -> usize {
    c.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .filter(
            |row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.as_ref() == opcode),
        )
        .count()
}
fn has_seek(c: &Connection, sql: &str) -> bool {
    opcode_present(c, sql, "Seek")
}
fn has_sorter(c: &Connection, sql: &str) -> bool {
    opcode_present(c, sql, "Sorter")
}
fn setup(ddl: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in ddl {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}
fn ins(f: &Connection, r: &rusqlite::Connection, s: &str) {
    f.execute(s).unwrap();
    r.execute_batch(s).unwrap();
}
fn cmp_ord(f: &Connection, r: &rusqlite::Connection, sql: &str, l: &str) {
    assert_eq!(
        frank_ord(f, sql),
        sqlite_ord(r, sql),
        "[{l}] order diverged: `{sql}`"
    );
}
#[test]
fn nonagg_index_eq_order_rowid_matches_sqlite() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);",
        "CREATE INDEX idx_c ON t(c);",
    ]);
    for ordinal in 1..=600_i64 {
        // A full-period permutation makes insertion order differ from rowid order.
        let i = (ordinal * 137) % 601;
        let a = if i % 17 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 20)
        };
        let c = if i % 19 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 12)
        };
        ins(
            &f,
            &r,
            &format!("INSERT INTO t VALUES ({i}, {a}, {c}, 'v{}');", i % 7),
        );
    }
    // Single-col-indexed eq + ORDER BY rowid (ASC or DESC): index seek, NO sorter, verified in output order.
    let seeks = [
        // ASC (seek `(val, MIN)` + walk forward)
        "SELECT * FROM t WHERE c = 5 ORDER BY id",
        "SELECT * FROM t WHERE c = 3 ORDER BY id LIMIT 3",
        "SELECT id, c FROM t WHERE c = 7 ORDER BY id",
        "SELECT c FROM t WHERE c = 2 ORDER BY id LIMIT 5", // covering output
        "SELECT * FROM t WHERE c = 0 ORDER BY id ASC",
        "SELECT id FROM t WHERE c = 4 ORDER BY rowid", // explicit rowid keyword
        "SELECT * FROM t WHERE c = 11 ORDER BY id LIMIT 100", // limit > match count
        "SELECT * FROM t WHERE c = 8 ORDER BY id LIMIT 3 OFFSET 2",
        // DESC (seek `(val, MAX)` + walk backward with Prev)
        "SELECT * FROM t WHERE c = 5 ORDER BY id DESC",
        "SELECT * FROM t WHERE c = 3 ORDER BY id DESC LIMIT 3", // latest-3 matching
        "SELECT id, c FROM t WHERE c = 7 ORDER BY id DESC",
        "SELECT c FROM t WHERE c = 2 ORDER BY id DESC LIMIT 5", // covering output, DESC
        "SELECT id FROM t WHERE c = 4 ORDER BY rowid DESC",
        "SELECT * FROM t WHERE c = 99 ORDER BY id DESC", // zero-match value, DESC
        "SELECT * FROM t WHERE c = 8 ORDER BY id DESC LIMIT 3 OFFSET 2",
    ];
    for sql in seeks {
        cmp_ord(&f, &r, sql, "idx-eq-order");
        assert!(
            has_seek(&f, sql),
            "index-eq + ORDER BY rowid must seek: `{sql}`"
        );
        assert!(
            !has_sorter(&f, sql),
            "index-eq + ORDER BY rowid must NOT sort: `{sql}`"
        );
        if sql.contains(" DESC") {
            assert!(
                opcode_count(&f, sql, "SeekLE") > 0,
                "DESC path must SeekLE: `{sql}`"
            );
            assert!(
                opcode_count(&f, sql, "Prev") > 0,
                "DESC path must walk with Prev: `{sql}`"
            );
        } else {
            assert!(
                opcode_count(&f, sql, "SeekGE") > 0,
                "ASC path must SeekGE: `{sql}`"
            );
            assert!(
                opcode_count(&f, sql, "Next") > 0,
                "ASC path must walk with Next: `{sql}`"
            );
        }
    }
    // Declines (still correct): ORDER BY the eq column, ORDER BY non-rowid.
    for sql in [
        "SELECT * FROM t WHERE c = 5 ORDER BY c",
        "SELECT * FROM t WHERE c = 5 ORDER BY x",
        "SELECT * FROM t WHERE c = 5 ORDER BY x DESC",
    ] {
        cmp_ord(&f, &r, sql, "idx-eq-declines");
    }
    // Bare eq (no ORDER BY) still seeks (unchanged).
    assert!(
        has_seek(&f, "SELECT * FROM t WHERE c = 5"),
        "bare index eq must still seek"
    );

    // Composite index: `WHERE cc = <v> ORDER BY id` must NOT use the eq seek (its trailing key column
    // `dd` would order the rows, not rowid) — it declines and stays correct.
    let (f2, r2) = setup(&[
        "CREATE TABLE u (id INTEGER PRIMARY KEY, cc INTEGER, dd INTEGER, x TEXT);",
        "CREATE INDEX idx_ccdd ON u(cc, dd);",
    ]);
    for i in 1..=400_i64 {
        // dd deliberately anti-correlated with id so a (cc,dd)-ordered walk would diverge from id order.
        ins(
            &f2,
            &r2,
            &format!(
                "INSERT INTO u VALUES ({i}, {}, {}, 'w{}');",
                i % 8,
                (400 - i) % 50,
                i % 3
            ),
        );
    }
    for sql in [
        "SELECT * FROM u WHERE cc = 3 ORDER BY id",
        "SELECT id, dd FROM u WHERE cc = 1 ORDER BY id LIMIT 7",
        "SELECT * FROM u WHERE cc = 3 ORDER BY id DESC", // composite must not walk backward either
        "SELECT id, dd FROM u WHERE cc = 1 ORDER BY id DESC LIMIT 7",
    ] {
        cmp_ord(&f2, &r2, sql, "composite-declines");
    }
}

#[test]
fn index_eq_rowid_order_respects_hints_and_collations() {
    let (h, hr) = setup(&[
        "CREATE TABLE h (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);",
        "CREATE INDEX idx_h_c ON h(c);",
        "CREATE INDEX idx_h_a ON h(a);",
    ]);
    for i in [8_i64, 1, 13, 5, 3, 21, 2, 34] {
        ins(
            &h,
            &hr,
            &format!("INSERT INTO h VALUES ({i}, {}, {}, 'v{i}');", i % 3, i % 2),
        );
    }
    for sql in [
        "SELECT id, c FROM h NOT INDEXED WHERE c = 1 ORDER BY id DESC",
        "SELECT id, c FROM h INDEXED BY idx_h_a WHERE c = 1 ORDER BY id DESC",
    ] {
        cmp_ord(&h, &hr, sql, "hint-isolation");
        assert_eq!(
            opcode_count(&h, sql, "SeekGE"),
            0,
            "hinted path must not take idx_h_c: `{sql}`"
        );
        assert_eq!(
            opcode_count(&h, sql, "SeekLE"),
            0,
            "hinted path must not take idx_h_c: `{sql}`"
        );
    }

    let (binary, binary_r) = setup(&[
        "CREATE TABLE b (id INTEGER PRIMARY KEY, c TEXT COLLATE BINARY);",
        "CREATE INDEX idx_b_nocase ON b(c COLLATE NOCASE);",
    ]);
    for sql in [
        "INSERT INTO b VALUES (7, 'a');",
        "INSERT INTO b VALUES (2, 'A');",
        "INSERT INTO b VALUES (9, 'b');",
    ] {
        ins(&binary, &binary_r, sql);
    }
    let binary_sql = "SELECT id, c FROM b WHERE c = 'a' ORDER BY id DESC";
    cmp_ord(&binary, &binary_r, binary_sql, "binary-column-nocase-index");
    assert_eq!(
        opcode_count(&binary, binary_sql, "SeekLE"),
        0,
        "mismatched NOCASE index must decline"
    );
    assert_eq!(
        opcode_count(&binary, binary_sql, "SeekGE"),
        0,
        "mismatched NOCASE index must decline"
    );

    let (nocase, nocase_r) = setup(&[
        "CREATE TABLE n (id INTEGER PRIMARY KEY, c TEXT COLLATE NOCASE);",
        "CREATE INDEX idx_n_binary ON n(c COLLATE BINARY);",
    ]);
    for sql in [
        "INSERT INTO n VALUES (7, 'a');",
        "INSERT INTO n VALUES (2, 'A');",
        "INSERT INTO n VALUES (9, 'b');",
    ] {
        ins(&nocase, &nocase_r, sql);
    }
    let nocase_sql = "SELECT id, c FROM n WHERE c = 'a' ORDER BY id DESC";
    cmp_ord(&nocase, &nocase_r, nocase_sql, "nocase-column-binary-index");
    assert_eq!(
        opcode_count(&nocase, nocase_sql, "SeekLE"),
        0,
        "mismatched BINARY index must decline"
    );
    assert_eq!(
        opcode_count(&nocase, nocase_sql, "SeekGE"),
        0,
        "mismatched BINARY index must decline"
    );

    let (matched, matched_r) = setup(&[
        "CREATE TABLE m (id INTEGER PRIMARY KEY, c TEXT COLLATE NOCASE);",
        "CREATE INDEX idx_m_wrong_first ON m(c COLLATE BINARY);",
        "CREATE INDEX idx_m_matching ON m(c COLLATE NOCASE);",
    ]);
    for sql in [
        "INSERT INTO m VALUES (7, 'a');",
        "INSERT INTO m VALUES (2, 'A');",
        "INSERT INTO m VALUES (9, 'b');",
    ] {
        ins(&matched, &matched_r, sql);
    }
    let matched_sql = "SELECT id, c FROM m WHERE c = 'a' ORDER BY id DESC";
    cmp_ord(
        &matched,
        &matched_r,
        matched_sql,
        "matching-index-after-mismatch",
    );
    assert!(
        opcode_count(&matched, matched_sql, "SeekLE") > 0,
        "matching NOCASE index must SeekLE"
    );
    assert!(
        opcode_count(&matched, matched_sql, "Prev") > 0,
        "matching NOCASE index must walk backward"
    );
    assert!(
        !has_sorter(&matched, matched_sql),
        "matching NOCASE index must avoid sorter"
    );
}
