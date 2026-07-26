//! bd-zq6dp — Oracle-parity e2e: a secondary-index range predicate on a numeric column
//! now seeks the index instead of full-scanning.
//!
//! `index_range_fast_path_is_safe` used to gate the non-aggregate index-range seek on
//! `(cmp_p5 & !0x80) == 0`, which an INTEGER/REAL/NUMERIC column carries a NUMERIC
//! comparison affinity against — so `SELECT ... FROM t WHERE <num col> BETWEEN a AND b`
//! silently degraded to a full table scan (EQP claimed `SEARCH USING INDEX` while the
//! emitted bytecode was `Rewind`+`Next`). The gate now also accepts a NUMERIC affinity
//! against a numeric *literal* bound, where the coercion is the identity — the same
//! proven-safe subset the IN-list seek accepts.
//!
//! This asserts (1) results stay bit-identical to real SQLite across many range shapes
//! (covering + non-covering, integer/real/numeric columns, real bounds, negatives, NULL
//! rows, empty ranges, ORDER BY, and every decline case), and (2) the emitted program
//! actually seeks for the accepted shapes and still scans for the declined ones — so the
//! optimization changes speed, not semantics.

// Row-count / small-int casts in fixtures; precision loss is irrelevant to the assertions.
#![allow(clippy::cast_precision_loss)]
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

async fn setup() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    let mut stmts = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, rr REAL, nn NUMERIC, w TEXT);"
            .to_owned(),
        "CREATE INDEX idx_t_k ON t(k);".to_owned(),
        "CREATE INDEX idx_t_rr ON t(rr);".to_owned(),
        "CREATE INDEX idx_t_nn ON t(nn);".to_owned(),
        "CREATE INDEX idx_t_w ON t(w);".to_owned(),
        // Expression index: `is_index_range_constant` (broadened to accept negated numeric
        // literals) is shared by the expression-index range extractor, so a `(k - 5) BETWEEN
        // -8 AND 0` bound must also stay bit-identical whether it seeks or scans.
        "CREATE INDEX idx_t_kx ON t(k - 5);".to_owned(),
    ];
    for i in 1..=60_i64 {
        let k = (i % 12) - 3; // -3..8, duplicates, negatives
        let rr = (i as f64).mul_add(0.5, -4.0); // negatives and fractional
        let nn = if i % 2 == 0 {
            format!("{}", i - 20)
        } else {
            format!("{}.5", i - 20)
        };
        stmts.push(format!(
            "INSERT INTO t VALUES ({i}, {k}, {rr}, {nn}, 'r{:02}');",
            i % 20
        ));
    }
    // NULL in every indexed column.
    stmts.push("INSERT INTO t VALUES (200, NULL, NULL, NULL, NULL);".to_owned());
    for s in &stmts {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

async fn opcodes(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN `{sql}`: {e}"))
        .iter()
        .filter_map(|row| match row.values().get(1) {
            Some(SqliteValue::Text(op)) => Some(op.to_string()),
            _ => None,
        })
        .collect()
}

/// The index-range scan (`codegen_select_index_range_scan`) always dereferences the
/// index entry's rowid via `IdxRowid` on a rowid table (for covering reads and table
/// lookups alike), while a full table scan (`codegen_select_full_scan`) never emits it.
/// So `IdxRowid` presence is the clean signal that the seek engaged rather than scanned —
/// robust across both a SeekGE-anchored range and a one-sided upper bound (which Rewinds
/// the *index* cursor, not the table).
fn uses_index(ops: &[String]) -> bool {
    ops.iter().any(|o| o == "IdxRowid")
}

#[test]
fn index_range_seek_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        let sorted = |res: Result<Vec<Vec<String>>, String>| -> Result<Vec<Vec<String>>, String> {
            res.map(|mut rows| {
                rows.sort();
                rows
            })
        };

        // No ORDER BY -> compare as SETS (row order unspecified; a declined case may scan in
        // rowid order while the seek yields index order — both valid).
        let set_eq = [
            // INTEGER column, covering (id = rowid via IdxRowid) — the shape the gate rejected.
            "SELECT id FROM t WHERE k > 5",
            "SELECT id FROM t WHERE k >= 5",
            "SELECT id FROM t WHERE k < 0",
            "SELECT id FROM t WHERE k <= 0",
            "SELECT id FROM t WHERE k BETWEEN 2 AND 5",
            "SELECT id FROM t WHERE k > 2 AND k < 6",
            "SELECT id FROM t WHERE k > -3 AND k <= 0",
            "SELECT id, k FROM t WHERE k >= 6",
            "SELECT k FROM t WHERE k BETWEEN 1 AND 4",
            // Real-valued bounds on an INTEGER column (NUMERIC coercion, still a numeric literal).
            "SELECT id FROM t WHERE k > 2.5",
            "SELECT id FROM t WHERE k <= 3.0",
            "SELECT id FROM t WHERE k BETWEEN -2.5 AND 4.5",
            // REAL column.
            "SELECT id FROM t WHERE rr > 2.5",
            "SELECT id, rr FROM t WHERE rr BETWEEN -1.0 AND 4.0",
            "SELECT rr FROM t WHERE rr >= 3",
            // NUMERIC column (mixed int/real stored).
            "SELECT id FROM t WHERE nn > 2.5",
            "SELECT id, nn FROM t WHERE nn BETWEEN -5 AND 5",
            // Empty / edge ranges.
            "SELECT id FROM t WHERE k > 9999",
            "SELECT id FROM t WHERE k < -9999",
            "SELECT id FROM t WHERE k BETWEEN 100 AND 200",
            // Non-covering (w not in idx_t_k) — must still match whatever path is chosen.
            "SELECT id, w FROM t WHERE k BETWEEN 2 AND 5",
            "SELECT rr, w FROM t WHERE k > 5",
            // Declines that must still be correct: text literal on a numeric column (NUMERIC
            // affinity but not a numeric literal -> scan), explicit index directives, DISTINCT.
            "SELECT id FROM t WHERE k > '3'",
            "SELECT id FROM t NOT INDEXED WHERE k BETWEEN 2 AND 5",
            "SELECT id FROM t INDEXED BY idx_t_w WHERE k BETWEEN 2 AND 5",
            "SELECT id FROM t INDEXED BY idx_t_w WHERE id BETWEEN 2 AND 5",
            // TEXT column range on a BINARY-collated index (bd-xiojw): now seeks; must stay exact.
            "SELECT id FROM t WHERE w > 'r05'",
            "SELECT id FROM t WHERE w BETWEEN 'r03' AND 'r08'",
            // Expression-index range with NEGATED numeric literal bounds (broadened extraction).
            "SELECT id FROM t WHERE k - 5 BETWEEN -8 AND 0",
            "SELECT id FROM t WHERE k - 5 > -6",
            "SELECT id FROM t WHERE k - 5 <= -1",
        ];
        for sql in set_eq {
            assert_eq!(
                sorted(frank_rows(&f, sql).await),
                sorted(sqlite_rows(&r, sql)),
                "index-range seek (row set) diverged from SQLite for `{sql}`"
            );
        }

        // ORDER BY / DISTINCT / LIMIT -> exact comparison.
        let exact = [
            "SELECT id FROM t WHERE k > 5 ORDER BY id",
            "SELECT id, k FROM t WHERE k BETWEEN 2 AND 5 ORDER BY k, id",
            "SELECT id FROM t WHERE k >= 6 ORDER BY id DESC",
            "SELECT id FROM t WHERE rr > 2.5 ORDER BY id",
            "SELECT DISTINCT k FROM t WHERE k BETWEEN 1 AND 4 ORDER BY k",
            "SELECT id FROM t WHERE k BETWEEN 2 AND 8 ORDER BY id LIMIT 3",
            "SELECT COUNT(*) FROM t WHERE k > 5",
        ];
        for sql in exact {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "index-range seek (ordered/declined) diverged from SQLite for `{sql}`"
            );
        }
    });
}

#[test]
fn index_range_seek_emits_seek_for_numeric_literals() {
    asupersync::test_utils::run_test(|| async {
        let (f, _r) = setup().await;

        // Accepted shapes must actually walk the index (IdxRowid), proving the optimization engages
        // rather than silently full-scanning. Includes bd-u6tbr placeholder bounds, which now seek
        // via a runtime Affinity coercion (the last two).
        for sql in [
            "SELECT id FROM t WHERE k BETWEEN 5 AND 55",
            "SELECT id, k FROM t WHERE k > 5",
            "SELECT id FROM t WHERE k < 0",
            "SELECT id FROM t WHERE k > 2.5",
            "SELECT id FROM t WHERE rr BETWEEN -1.0 AND 4.0",
            "SELECT id FROM t WHERE nn > 2.5",
            "SELECT id FROM t WHERE k > ?1",
            "SELECT id, k FROM t WHERE k BETWEEN ?1 AND ?2",
        ] {
            let ops = opcodes(&f, sql).await;
            assert!(
                uses_index(&ops),
                "accepted range must seek the index for `{sql}`; ops = {ops:?}"
            );
        }

        // A placeholder bound that now seeks must carry an Affinity coercion op (so a runtime-typed
        // bind is normalized to the column affinity before the seek).
        for sql in [
            "SELECT id FROM t WHERE k > ?1",
            "SELECT id, k FROM t WHERE k BETWEEN ?1 AND ?2",
        ] {
            let ops = opcodes(&f, sql).await;
            assert!(
                ops.iter().any(|o| o == "Affinity"),
                "placeholder range must coerce the bound with an Affinity op for `{sql}`; ops = {ops:?}"
            );
        }

        // Declined shapes keep the correct full scan (no unhinted index walk): NOT INDEXED,
        // a forced unrelated index, and a text literal on a numeric column. In particular,
        // INDEXED BY idx_t_w must not be silently replaced by the idx_t_k range heuristic.
        for sql in [
            "SELECT id FROM t NOT INDEXED WHERE k BETWEEN 2 AND 5",
            "SELECT id FROM t INDEXED BY idx_t_w WHERE k BETWEEN 2 AND 5",
            "SELECT id FROM t WHERE k > '3'",
        ] {
            let ops = opcodes(&f, sql).await;
            assert!(
                !uses_index(&ops),
                "declined range must NOT seek (keeps the full scan) for `{sql}`; ops = {ops:?}"
            );
        }
    });
}

async fn frank_bound(conn: &Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    let stmt = conn
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("frank prepare `{sql}`: {e}"));
    let rows = stmt
        .query_with_params(params)
        .await
        .unwrap_or_else(|e| panic!("frank bind `{sql}`: {e}"));
    let mut out: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect();
    out.sort();
    out
}

fn sqlite_bound(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Vec<Vec<String>> {
    let rp: Vec<rusqlite::types::Value> = params
        .iter()
        .map(|v| match v {
            SqliteValue::Null => rusqlite::types::Value::Null,
            SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
            SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
            SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
            SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
        })
        .collect();
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    let mut out: Vec<Vec<String>> = stmt
        .query_map(rusqlite::params_from_iter(rp), |row| {
            let mut r = Vec::with_capacity(n);
            for i in 0..n {
                let v: rusqlite::types::Value = row.get_unwrap(i);
                r.push(match v {
                    rusqlite::types::Value::Null => "NULL".to_owned(),
                    rusqlite::types::Value::Integer(x) => x.to_string(),
                    rusqlite::types::Value::Real(fl) => format!("{fl}"),
                    rusqlite::types::Value::Text(s) => format!("'{s}'"),
                    rusqlite::types::Value::Blob(b) => format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    ),
                });
            }
            Ok(r)
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    out.sort();
    out
}

/// bd-u6tbr: a placeholder-bound secondary-index range now seeks (runtime Affinity coercion of
/// the bind to the column affinity). Every bind type — ints, reals, numeric-looking text,
/// non-numeric text, empty text, blob, NULL — must stay bit-identical to the full-scan filter,
/// including the empty-result edges (a non-numeric text/blob bind seeks nothing, exactly as the
/// filter's numeric comparison excludes it).
#[test]
fn index_range_seek_placeholder_bounds_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        let vals = [
            SqliteValue::Integer(0),
            SqliteValue::Integer(5),
            SqliteValue::Integer(-3),
            SqliteValue::Float(2.5),
            SqliteValue::Float(-1.5),
            SqliteValue::Text("3".into()),
            SqliteValue::Text("2.5".into()),
            SqliteValue::Text("abc".into()),
            SqliteValue::Text("".into()),
            SqliteValue::Blob(vec![1, 2, 3].into()),
            SqliteValue::Null,
        ];
        let one_param = [
            "SELECT id FROM t WHERE k > ?1",
            "SELECT id FROM t WHERE k >= ?1",
            "SELECT id FROM t WHERE k < ?1",
            "SELECT id FROM t WHERE k <= ?1",
            "SELECT id, k FROM t WHERE k > ?1",
            "SELECT id FROM t WHERE rr > ?1",
            "SELECT id FROM t WHERE nn >= ?1",
            "SELECT id, w FROM t WHERE k < ?1",
        ];
        for sql in one_param {
            for v in &vals {
                assert_eq!(
                    frank_bound(&f, sql, std::slice::from_ref(v)).await,
                    sqlite_bound(&r, sql, std::slice::from_ref(v)),
                    "placeholder range diverged for `{sql}` with param {v:?}"
                );
            }
        }

        let two_param: [(SqliteValue, SqliteValue); 6] = [
            (SqliteValue::Integer(-2), SqliteValue::Integer(4)),
            (SqliteValue::Float(-1.5), SqliteValue::Float(3.5)),
            (SqliteValue::Text("1".into()), SqliteValue::Text("5".into())),
            (SqliteValue::Integer(5), SqliteValue::Integer(2)), // empty (lo > hi)
            (SqliteValue::Null, SqliteValue::Integer(5)),       // NULL bound -> empty
            (SqliteValue::Text("abc".into()), SqliteValue::Integer(5)),
        ];
        for (a, b) in &two_param {
            let sql = "SELECT id, k FROM t WHERE k BETWEEN ?1 AND ?2";
            assert_eq!(
                frank_bound(&f, sql, &[a.clone(), b.clone()]).await,
                sqlite_bound(&r, sql, &[a.clone(), b.clone()]),
                "placeholder BETWEEN diverged for params {a:?}, {b:?}"
            );
        }
    });
}

/// bd-xiojw: a text-literal range on a BINARY-collated text index now seeks (no coercion —
/// text-vs-text under `P4::None` is the index's own BINARY order). A NOCASE-collated index, or a
/// non-text-literal bound, still scans. All bit-identical to C SQLite.
#[test]
fn index_range_seek_text_binary_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        let schema = [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, w TEXT, wc TEXT COLLATE NOCASE);",
            "CREATE INDEX idx_w ON t(w);",
            "CREATE INDEX idx_wc ON t(wc);",
        ];
        for s in schema {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // Mixed case so BINARY vs NOCASE ordering actually differs.
        let words = [
            "Apple", "apple", "Banana", "banana", "Cherry", "cherry", "Date", "date", "eGG", "fig",
        ];
        for (i, w) in words.iter().enumerate() {
            let sql = format!("INSERT INTO t VALUES ({}, '{w}', '{w}');", i + 1);
            f.execute(&sql).await.unwrap();
            r.execute_batch(&sql).unwrap();
        }
        let null_row = "INSERT INTO t VALUES (100, NULL, NULL);";
        f.execute(null_row).await.unwrap();
        r.execute_batch(null_row).unwrap();

        let sorted = |mut v: Vec<Vec<String>>| {
            v.sort();
            v
        };
        for sql in [
            // BINARY (idx_w) text literals — now seek.
            "SELECT id FROM t WHERE w > 'Cherry'",
            "SELECT id FROM t WHERE w >= 'banana'",
            "SELECT id FROM t WHERE w < 'Date'",
            "SELECT id FROM t WHERE w BETWEEN 'B' AND 'd'",
            "SELECT id, w FROM t WHERE w > 'a'",
            "SELECT id FROM t WHERE w > 'zzz'", // empty
            "SELECT id FROM t WHERE w BETWEEN 'z' AND 'a'", // empty (lo > hi)
            // NOCASE (idx_wc) — must decline the BINARY seek but stay correct.
            "SELECT id FROM t WHERE wc > 'cherry'",
            "SELECT id FROM t WHERE wc BETWEEN 'b' AND 'd'",
            // Non-text-literal bound on a text column -> decline, still correct.
            "SELECT id FROM t WHERE w > 5",
        ] {
            assert_eq!(
                sorted(frank_rows(&f, sql).await.unwrap()),
                sorted(sqlite_rows(&r, sql).unwrap()),
                "text range diverged for `{sql}`"
            );
        }
        for sql in [
            "SELECT id, w FROM t WHERE w > 'Cherry' ORDER BY w, id",
            "SELECT id, wc FROM t WHERE wc > 'cherry' ORDER BY wc, id",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await.unwrap(),
                sqlite_rows(&r, sql).unwrap(),
                "ordered text range diverged for `{sql}`"
            );
        }

        // Opcode gate: BINARY text-literal ranges seek (IdxRowid); NOCASE and numeric-on-text scan.
        assert!(
            uses_index(&opcodes(&f, "SELECT id FROM t WHERE w > 'Cherry'").await),
            "BINARY text range must seek"
        );
        assert!(
            uses_index(&opcodes(&f, "SELECT id FROM t WHERE w BETWEEN 'B' AND 'd'").await),
            "BINARY text BETWEEN must seek"
        );
        assert!(
            !uses_index(&opcodes(&f, "SELECT id FROM t WHERE wc > 'cherry'").await),
            "NOCASE text range must NOT seek (collation != BINARY)"
        );
        assert!(
            !uses_index(&opcodes(&f, "SELECT id FROM t WHERE w > 5").await),
            "numeric literal on a text column must NOT seek"
        );

        // A placeholder bound on a BINARY text column now seeks via a runtime Affinity 'B' coercion;
        // every bind type must stay bit-identical to the full-scan filter (a numeric bind becomes its
        // text form; a blob seeks past the text keys, empty, exactly as the filter excludes it).
        let binds = [
            SqliteValue::Text("Cherry".into()),
            SqliteValue::Text("cherry".into()),
            SqliteValue::Text("B".into()),
            SqliteValue::Text("".into()),
            SqliteValue::Integer(5),
            SqliteValue::Float(2.5),
            SqliteValue::Blob(vec![0x62, 0x62].into()),
            SqliteValue::Null,
        ];
        for sql in [
            "SELECT id FROM t WHERE w > ?1",
            "SELECT id FROM t WHERE w >= ?1",
            "SELECT id FROM t WHERE w < ?1",
            "SELECT id, w FROM t WHERE w <= ?1",
            // NOCASE column with a placeholder: declines the BINARY seek, still correct.
            "SELECT id FROM t WHERE wc > ?1",
        ] {
            for b in &binds {
                let params = std::slice::from_ref(b);
                assert_eq!(
                    frank_bound(&f, sql, params).await,
                    sqlite_bound(&r, sql, params),
                    "text placeholder range diverged for `{sql}` with {params:?}"
                );
            }
        }
        {
            let sql = "SELECT id, w FROM t WHERE w BETWEEN ?1 AND ?2";
            let params = &[SqliteValue::Text("B".into()), SqliteValue::Text("d".into())];
            assert_eq!(
                frank_bound(&f, sql, params).await,
                sqlite_bound(&r, sql, params),
                "text placeholder range diverged for `{sql}` with {params:?}"
            );
        }

        // Opcode gate: BINARY text placeholder seeks (IdxRowid + Affinity); NOCASE placeholder scans.
        let ph_ops = opcodes(&f, "SELECT id FROM t WHERE w > ?1").await;
        assert!(
            uses_index(&ph_ops),
            "BINARY text placeholder range must seek"
        );
        assert!(
            ph_ops.iter().any(|o| o == "Affinity"),
            "text placeholder must carry an Affinity coercion; ops = {ph_ops:?}"
        );
        assert!(
            !uses_index(&opcodes(&f, "SELECT id FROM t WHERE wc > ?1").await),
            "NOCASE text placeholder must NOT seek"
        );
    });
}

/// bd-zqkrp: a composite-index equality-prefix + trailing-range seek (`WHERE a = 2 AND b > 5` on
/// `index(a, b)`) now seeks instead of full-scanning. Bit-identical to C SQLite across covering /
/// non-covering, negatives, NULL trailing, one-sided/empty ranges, prefix miss, multi-eq-prefix
/// (idx on (a,b,c)), ORDER BY, placeholders, and declines.
#[test]
fn index_range_seek_composite_prefix_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT);",
            "CREATE INDEX idx_ab ON t(a, b);",
            "CREATE INDEX idx_abc ON t(a, b, c);",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        let mut id = 1_i64;
        for a in [-1_i64, 0, 1, 2] {
            for b in [-3_i64, 0, 2, 5, 8, 12] {
                let sql = format!(
                    "INSERT INTO t VALUES ({id}, {a}, {b}, 'c{}');",
                    (a + b).rem_euclid(5)
                );
                f.execute(&sql).await.unwrap();
                r.execute_batch(&sql).unwrap();
                id += 1;
            }
        }
        for a in [1_i64, 2] {
            let sql = format!("INSERT INTO t VALUES ({id}, {a}, NULL, 'n');");
            f.execute(&sql).await.unwrap();
            r.execute_batch(&sql).unwrap();
            id += 1;
        }

        let sorted = |mut v: Vec<Vec<String>>| {
            v.sort();
            v
        };
        for sql in [
            "SELECT id FROM t WHERE a = 2 AND b > 5",
            "SELECT id, a, b FROM t WHERE a = 2 AND b >= 5",
            "SELECT id FROM t WHERE a = 2 AND b < 8",
            "SELECT id FROM t WHERE a = 2 AND b <= 8",
            "SELECT id FROM t WHERE a = 2 AND b BETWEEN 0 AND 8",
            "SELECT id FROM t WHERE a = 2 AND b > 0 AND b < 12",
            "SELECT b FROM t WHERE a = 1 AND b > -3", // covering b
            "SELECT id FROM t WHERE a = -1 AND b >= 0", // negative prefix
            "SELECT id FROM t WHERE a = 2 AND b > 999", // empty range
            "SELECT id FROM t WHERE a = 99 AND b > 0", // prefix miss
            "SELECT id, c FROM t WHERE a = 2 AND b > 2", // non-covering (c -> table lookup)
            "SELECT id FROM t WHERE a = 1 AND b = 2 AND c > 'a'", // multi-eq-prefix on idx_abc
            // Declines that must still be correct.
            "SELECT id FROM t WHERE b > 5", // no equality prefix
            "SELECT id FROM t WHERE a > 1 AND b > 0", // prefix col not equality
            "SELECT id FROM t NOT INDEXED WHERE a = 2 AND b > 5",
        ] {
            assert_eq!(
                sorted(frank_rows(&f, sql).await.unwrap()),
                sorted(sqlite_rows(&r, sql).unwrap()),
                "composite range diverged for `{sql}`"
            );
        }
        for sql in [
            "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b, id",
            "SELECT id FROM t WHERE a = 1 AND b BETWEEN -3 AND 8 ORDER BY id",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await.unwrap(),
                sqlite_rows(&r, sql).unwrap(),
                "composite ordered diverged for `{sql}`"
            );
        }

        // Opcode gate: composite prefix+range seeks (IdxRowid + IdxGT prefix bound, no table Rewind).
        let ops = opcodes(&f, "SELECT id FROM t WHERE a = 2 AND b > 5").await;
        assert!(
            ops.iter().any(|o| o == "IdxRowid"),
            "composite must seek; ops = {ops:?}"
        );
        assert!(
            ops.iter().any(|o| o == "IdxGT"),
            "composite must bound the prefix with IdxGT; ops = {ops:?}"
        );
        assert!(
            !ops.iter().any(|o| o == "Rewind"),
            "composite must not full-scan; ops = {ops:?}"
        );

        // Placeholder binds on the prefix and the range (affinity-coerced).
        for bnd in [
            vec![SqliteValue::Integer(2), SqliteValue::Integer(5)],
            vec![SqliteValue::Integer(1), SqliteValue::Integer(-3)],
            vec![SqliteValue::Integer(2), SqliteValue::Text("3".into())],
            vec![SqliteValue::Integer(99), SqliteValue::Integer(0)],
        ] {
            let sql = "SELECT id, b FROM t WHERE a = ?1 AND b > ?2";
            assert_eq!(
                frank_bound(&f, sql, &bnd).await,
                sqlite_bound(&r, sql, &bnd),
                "composite placeholder diverged for {bnd:?}"
            );
        }

        // bd-zqkrp ORDER-BY-via-index: `ORDER BY <range col>, id` is the deterministic (range_col, rowid)
        // total order the seek streams — no sorter — and LIMIT/OFFSET stream straight off it.
        for sql in [
            "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b, id",
            "SELECT id, b FROM t WHERE a = 1 AND b BETWEEN -3 AND 12 ORDER BY b, id",
            "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b, id LIMIT 2",
            "SELECT id, b FROM t WHERE a = 1 AND b >= -3 ORDER BY b, id LIMIT 3 OFFSET 1",
            // Deterministic declines (fall back to the sorter, still bit-identical).
            "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b DESC, id",
            "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY id, b",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await.unwrap(),
                sqlite_rows(&r, sql).unwrap(),
                "composite order-by-via-index diverged for `{sql}`"
            );
        }
        // Bare `ORDER BY b` is tie-ambiguous (declines to the sorter); compare as a set — both must
        // return the same rows, and both must be b-ascending.
        assert_eq!(
            sorted(
                frank_rows(&f, "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b")
                    .await
                    .unwrap()
            ),
            sorted(
                sqlite_rows(&r, "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b").unwrap()
            ),
            "composite bare ORDER BY row set diverged"
        );

        // The satisfied ORDER BY streams from the seek without a sorter.
        assert!(
            !opcodes(
                &f,
                "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b, id"
            )
            .await
            .iter()
            .any(|o| o.starts_with("Sorter")),
            "composite ORDER BY <range col>, id must avoid the sorter"
        );
        assert!(
            uses_index(
                &opcodes(
                    &f,
                    "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b, id LIMIT 2"
                )
                .await
            ),
            "composite ORDER BY + LIMIT must seek"
        );

        // bd-6x9z0 follow-up: composite DESC. `WHERE a = v AND b <range> ORDER BY b DESC, id DESC`
        // (composite keyset "most recent first" pagination) streams off a reverse index walk with no
        // sorter — the exact `(b DESC, id DESC)` order must match C SQLite.
        for sql in [
            "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b DESC, id DESC",
            "SELECT id, b FROM t WHERE a = 2 AND b >= 0 ORDER BY b DESC, id DESC",
            "SELECT id, b FROM t WHERE a = 2 AND b < 8 ORDER BY b DESC, id DESC", // exclusive upper -> SeekLT
            "SELECT id, b FROM t WHERE a = 2 AND b <= 8 ORDER BY b DESC, id DESC", // inclusive upper -> SeekLE
            "SELECT id, b FROM t WHERE a = 1 AND b BETWEEN -3 AND 8 ORDER BY b DESC, id DESC",
            "SELECT id, b FROM t WHERE a = 2 AND b > 0 AND b < 12 ORDER BY b DESC, id DESC",
            "SELECT id, b FROM t WHERE a = 1 AND b > -3 ORDER BY b DESC, id DESC LIMIT 3",
            "SELECT id, b FROM t WHERE a = 1 AND b >= -3 ORDER BY b DESC, id DESC LIMIT 2 OFFSET 1",
            "SELECT b FROM t WHERE a = 1 AND b > -3 ORDER BY b DESC, id DESC", // covering b
            "SELECT id, c FROM t WHERE a = 2 AND b > 2 ORDER BY b DESC, id DESC", // non-covering (c -> table)
            "SELECT id FROM t WHERE a = -1 AND b >= 0 ORDER BY b DESC, id DESC",  // negative prefix
            "SELECT id FROM t WHERE a = 2 AND b > 999 ORDER BY b DESC, id DESC",  // empty range
            "SELECT id FROM t WHERE a = 99 AND b > 0 ORDER BY b DESC, id DESC",   // prefix miss
            "SELECT id, c FROM t WHERE a = 1 AND b = 2 AND c > 'a' ORDER BY c DESC, id DESC", // multi-eq-prefix, idx_abc
            // Deterministic declines (mixed direction / rowid-first) -> sorter, still bit-identical.
            "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b DESC, id ASC",
            "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY id DESC, b DESC",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await.unwrap(),
                sqlite_rows(&r, sql).unwrap(),
                "composite DESC diverged for `{sql}`"
            );
        }
        // Bare `ORDER BY b DESC` on the non-unique idx_ab is tie-ambiguous (declines to the sorter);
        // set comparison — both must return the same rows.
        assert_eq!(
            sorted(
                frank_rows(
                    &f,
                    "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b DESC"
                )
                .await
                .unwrap()
            ),
            sorted(
                sqlite_rows(
                    &r,
                    "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b DESC"
                )
                .unwrap()
            ),
            "composite bare DESC row set diverged"
        );
        // Placeholder binds on the prefix + range in DESC (affinity-coerced).
        for bnd in [
            vec![SqliteValue::Integer(2), SqliteValue::Integer(0)],
            vec![SqliteValue::Integer(1), SqliteValue::Integer(-3)],
            vec![SqliteValue::Integer(2), SqliteValue::Text("3".into())],
        ] {
            let sql = "SELECT id, b FROM t WHERE a = ?1 AND b > ?2 ORDER BY b DESC, id DESC";
            assert_eq!(
                frank_bound(&f, sql, &bnd).await,
                sqlite_bound(&r, sql, &bnd),
                "composite DESC placeholder diverged for {bnd:?}"
            );
        }
        // Opcode gate: composite DESC reverse-walks (Prev) with no sorter, still seeks (IdxRowid), and
        // never full-scans (no Rewind); exclusive upper anchors with SeekLT.
        let dops = opcodes(
            &f,
            "SELECT id, b FROM t WHERE a = 2 AND b > 0 ORDER BY b DESC, id DESC",
        )
        .await;
        assert!(
            !dops.iter().any(|o| o.starts_with("Sorter")),
            "composite DESC must avoid the sorter; ops = {dops:?}"
        );
        assert!(
            dops.iter().any(|o| o == "Prev"),
            "composite DESC must reverse-walk (Prev); ops = {dops:?}"
        );
        assert!(
            dops.iter().any(|o| o == "IdxRowid"),
            "composite DESC must seek; ops = {dops:?}"
        );
        assert!(
            !dops.iter().any(|o| o == "Rewind"),
            "composite DESC must not full-scan; ops = {dops:?}"
        );
        assert!(
            opcodes(
                &f,
                "SELECT id FROM t WHERE a = 2 AND b < 8 ORDER BY b DESC, id DESC"
            )
            .await
            .iter()
            .any(|o| o == "SeekLT"),
            "composite DESC with an exclusive upper bound must SeekLT"
        );

        // Composite DESC on a UNIQUE index: within the a-block each b is unique, so a bare
        // `ORDER BY b DESC` is already a total order and streams off the reverse walk with no sorter.
        for s in [
            "CREATE TABLE u (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
            "CREATE UNIQUE INDEX idx_u_ab ON u(a, b);",
            "INSERT INTO u VALUES (1,7,10),(2,7,20),(3,7,30),(4,8,5),(5,7,NULL);",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        for sql in [
            "SELECT id, b FROM u WHERE a = 7 AND b > 5 ORDER BY b DESC",
            "SELECT id, b FROM u WHERE a = 7 AND b BETWEEN 10 AND 30 ORDER BY b DESC",
            "SELECT id, b FROM u WHERE a = 7 AND b > 5 ORDER BY b DESC LIMIT 2",
            "SELECT id, b FROM u WHERE a = 7 AND b >= 10 ORDER BY b DESC, id DESC",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await.unwrap(),
                sqlite_rows(&r, sql).unwrap(),
                "composite unique DESC diverged for `{sql}`"
            );
        }
        assert!(
            !opcodes(
                &f,
                "SELECT id, b FROM u WHERE a = 7 AND b > 5 ORDER BY b DESC"
            )
            .await
            .iter()
            .any(|o| o.starts_with("Sorter")),
            "composite unique bare DESC must avoid the sorter"
        );
    });
}

/// bd-wimmv follow-up: a single-column indexed range with a deterministic `ORDER BY <col>, id`
/// streams off the range seek in `(col, rowid)` order — no sorter — where it previously fell to a
/// sorter (the ordered-scan bails on rowid order terms). LIMIT/OFFSET stream too. Bit-identical.
#[test]
fn index_range_seek_single_col_order_by_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        for sql in [
            "SELECT id, k FROM t WHERE k > 5 ORDER BY k, id",
            "SELECT id, k FROM t WHERE k BETWEEN 2 AND 8 ORDER BY k, id",
            "SELECT id, k FROM t WHERE k > 0 ORDER BY k, id LIMIT 3",
            "SELECT id, k FROM t WHERE k >= 1 ORDER BY k, id LIMIT 4 OFFSET 2",
            "SELECT id, rr FROM t WHERE rr > 0 ORDER BY rr, id",
            "SELECT id FROM t WHERE w > 'r05' ORDER BY w, id",
            // Deterministic declines (fall back to the sorter, still bit-identical).
            "SELECT id, k FROM t WHERE k > 5 ORDER BY k DESC, id",
            "SELECT id, k FROM t WHERE k > 5 ORDER BY id, k",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await.unwrap(),
                sqlite_rows(&r, sql).unwrap(),
                "single-col range order-by diverged for `{sql}`"
            );
        }
        // Bare `ORDER BY k` is tie-ambiguous (declines to the sorter); compare as a set.
        let sorted = |mut v: Vec<Vec<String>>| {
            v.sort();
            v
        };
        assert_eq!(
            sorted(
                frank_rows(&f, "SELECT id, k FROM t WHERE k > 5 ORDER BY k")
                    .await
                    .unwrap()
            ),
            sorted(sqlite_rows(&r, "SELECT id, k FROM t WHERE k > 5 ORDER BY k").unwrap()),
            "bare single-col ORDER BY row set diverged"
        );
        // The satisfied ORDER BY streams from the seek without a sorter.
        assert!(
            !opcodes(&f, "SELECT id, k FROM t WHERE k > 5 ORDER BY k, id")
                .await
                .iter()
                .any(|o| o.starts_with("Sorter")),
            "single-col ORDER BY <col>, id must avoid the sorter"
        );
        assert!(
            uses_index(
                &opcodes(&f, "SELECT id, k FROM t WHERE k > 5 ORDER BY k, id LIMIT 3").await
            ),
            "single-col ORDER BY + LIMIT must seek"
        );
    });
}

/// bd-ss48y follow-up: on a UNIQUE index there are no ties within a range, so a bare `ORDER BY col`
/// is already a total order — the range seek streams it (no sorter), bit-identical. On a non-unique
/// index a bare `ORDER BY col` is tie-ambiguous and still declines to the sorter.
#[test]
fn index_range_seek_unique_bare_order_by_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, uu INTEGER, gg INTEGER);",
            "CREATE UNIQUE INDEX idx_uu ON t(uu);",
            "CREATE INDEX idx_gg ON t(gg);",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        for i in 1..=40_i64 {
            let uu = i * 3 - 60; // unique, spans negatives
            let gg = (i % 8) - 3; // duplicates
            let sql = format!("INSERT INTO t VALUES ({i}, {uu}, {gg});");
            f.execute(&sql).await.unwrap();
            r.execute_batch(&sql).unwrap();
        }
        for sql in [
            // UNIQUE index: bare ORDER BY uu is a total order (no ties) -> streams off the seek.
            "SELECT id, uu FROM t WHERE uu > 0 ORDER BY uu",
            "SELECT id, uu FROM t WHERE uu BETWEEN -30 AND 30 ORDER BY uu",
            "SELECT id, uu FROM t WHERE uu > 0 ORDER BY uu LIMIT 5",
            "SELECT uu FROM t WHERE uu >= -10 ORDER BY uu",
            "SELECT id, uu FROM t WHERE uu > 0 ORDER BY uu, id",
            // Non-unique index: the deterministic 2-term ORDER BY gg, id still seeks.
            "SELECT id, gg FROM t WHERE gg > 0 ORDER BY gg, id",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await.unwrap(),
                sqlite_rows(&r, sql).unwrap(),
                "unique bare order-by diverged for `{sql}`"
            );
        }
        // Bare `ORDER BY gg` on the non-unique index is tie-ambiguous (declines to the sorter); set.
        let sorted = |mut v: Vec<Vec<String>>| {
            v.sort();
            v
        };
        assert_eq!(
            sorted(
                frank_rows(&f, "SELECT id, gg FROM t WHERE gg > 0 ORDER BY gg")
                    .await
                    .unwrap()
            ),
            sorted(sqlite_rows(&r, "SELECT id, gg FROM t WHERE gg > 0 ORDER BY gg").unwrap()),
            "non-unique bare ORDER BY row set diverged"
        );
        // The unique bare ORDER BY range-seeks (SeekGE) and streams without a sorter.
        let ops = opcodes(&f, "SELECT id, uu FROM t WHERE uu > 0 ORDER BY uu").await;
        assert!(
            !ops.iter().any(|o| o.starts_with("Sorter")),
            "unique bare ORDER BY must avoid the sorter; ops = {ops:?}"
        );
        assert!(
            ops.iter().any(|o| o == "SeekGE"),
            "unique bare ORDER BY must range-seek (SeekGE); ops = {ops:?}"
        );
    });
}

/// bd-ln7dp: reverse (DESC) single-column range seek. `WHERE col <range> ORDER BY col DESC, id DESC`
/// (keyset "most recent first" pagination) streams in `(col DESC, rowid DESC)` order off a reverse
/// index walk (SeekLE/Last + Prev) with no sorter. Bit-identical to C SQLite across bounds, LIMIT/
/// OFFSET, covering/non-covering, NULL handling, empty ranges, and declines.
#[test]
fn index_range_seek_desc_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        for sql in [
            "SELECT id, k FROM t WHERE k < 5 ORDER BY k DESC, id DESC",
            "SELECT id, k FROM t WHERE k <= 5 ORDER BY k DESC, id DESC",
            "SELECT id, k FROM t WHERE k > 2 ORDER BY k DESC, id DESC",
            "SELECT id, k FROM t WHERE k >= 2 ORDER BY k DESC, id DESC",
            "SELECT id, k FROM t WHERE k BETWEEN 0 AND 6 ORDER BY k DESC, id DESC",
            "SELECT id, k FROM t WHERE k < 5 ORDER BY k DESC, id DESC LIMIT 3",
            "SELECT id, k FROM t WHERE k > -2 ORDER BY k DESC, id DESC LIMIT 4 OFFSET 2",
            "SELECT k FROM t WHERE k >= -3 ORDER BY k DESC, id DESC", // covering k
            "SELECT id, rr FROM t WHERE rr < 3 ORDER BY rr DESC, id DESC",
            "SELECT id FROM t WHERE w < 'r10' ORDER BY w DESC, id DESC", // text BINARY
            "SELECT id, w FROM t WHERE k > 3 ORDER BY k DESC, id DESC", // non-covering (w -> table lookup)
            "SELECT id FROM t WHERE k > 999 ORDER BY k DESC, id DESC",  // empty
            // Deterministic declines (mixed direction / rowid-first) -> sorter, still bit-identical.
            "SELECT id, k FROM t WHERE k < 5 ORDER BY k DESC, id ASC",
            "SELECT id, k FROM t WHERE k < 5 ORDER BY id DESC, k DESC",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await.unwrap(),
                sqlite_rows(&r, sql).unwrap(),
                "desc range diverged for `{sql}`"
            );
        }
        // Bare `ORDER BY k DESC` (non-unique) is tie-ambiguous (declines to the sorter); set comparison.
        let sorted = |mut v: Vec<Vec<String>>| {
            v.sort();
            v
        };
        assert_eq!(
            sorted(
                frank_rows(&f, "SELECT id, k FROM t WHERE k < 5 ORDER BY k DESC")
                    .await
                    .unwrap()
            ),
            sorted(sqlite_rows(&r, "SELECT id, k FROM t WHERE k < 5 ORDER BY k DESC").unwrap()),
            "bare DESC row set diverged"
        );
        // Opcode gate: DESC reverse-walks (Prev) with no sorter; upper-bounded uses SeekLE, otherwise Last.
        let ops_u = opcodes(
            &f,
            "SELECT id, k FROM t WHERE k < 5 ORDER BY k DESC, id DESC",
        )
        .await;
        assert!(
            !ops_u.iter().any(|o| o.starts_with("Sorter")),
            "DESC must avoid the sorter; ops = {ops_u:?}"
        );
        assert!(
            ops_u.iter().any(|o| o == "Prev"),
            "DESC must reverse-walk (Prev); ops = {ops_u:?}"
        );
        assert!(
            ops_u.iter().any(|o| o == "SeekLE"),
            "DESC with an upper bound must SeekLE; ops = {ops_u:?}"
        );
        let ops_l = opcodes(
            &f,
            "SELECT id, k FROM t WHERE k > 2 ORDER BY k DESC, id DESC",
        )
        .await;
        assert!(
            ops_l.iter().any(|o| o == "Last"),
            "DESC with no upper bound must start at Last; ops = {ops_l:?}"
        );
        assert!(
            uses_index(
                &opcodes(
                    &f,
                    "SELECT id, k FROM t WHERE k < 5 ORDER BY k DESC, id DESC LIMIT 3"
                )
                .await
            ),
            "DESC + LIMIT must seek"
        );
    });
}
