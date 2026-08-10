//! Oracle for bd-distinct-loose-scan: `SELECT DISTINCT <indexed col>` served by a loose/skip index
//! scan must be byte-exact vs rusqlite across NULLs, duplicate runs, empty/all-same/all-NULL tables,
//! and must DECLINE (fall back, still correct) for NOCASE columns and non-indexed columns.
#![allow(clippy::uninlined_format_args)]
use fsqlite::Connection;
use fsqlite_types::SqliteValue;

/// Format one fsqlite result set as ordered `\n`-joined rows of `|`-joined cells.
async fn f_rows(c: &Connection, sql: &str) -> String {
    let rows = c.query(sql).await.unwrap();
    let mut out = Vec::new();
    for r in rows.iter() {
        let cells: Vec<String> = r
            .values()
            .iter()
            .map(|v| match v {
                SqliteValue::Null => "NULL".to_string(),
                SqliteValue::Integer(i) => format!("i{i}"),
                SqliteValue::Float(x) => format!("f{x}"),
                SqliteValue::Text(t) => format!("t{}", t),
                SqliteValue::Blob(b) => format!("b{:?}", b),
            })
            .collect();
        out.push(cells.join("|"));
    }
    out.join("\n")
}

/// Same formatting for a rusqlite result set.
fn r_rows(c: &rusqlite::Connection, sql: &str) -> String {
    let mut stmt = c.prepare(sql).unwrap();
    let ncol = stmt.column_count();
    let rows: Vec<String> = stmt
        .query_map([], |row| {
            let cells: Vec<String> = (0..ncol)
                .map(|i| match row.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Null => "NULL".to_string(),
                    rusqlite::types::ValueRef::Integer(v) => format!("i{v}"),
                    rusqlite::types::ValueRef::Real(x) => format!("f{x}"),
                    rusqlite::types::ValueRef::Text(t) => {
                        format!("t{}", String::from_utf8_lossy(t))
                    }
                    rusqlite::types::ValueRef::Blob(b) => format!("b{:?}", b),
                })
                .collect();
            Ok(cells.join("|"))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    rows.join("\n")
}

async fn has_op(c: &Connection, sql: &str, op: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .any(|r| matches!(r.values().get(1), Some(SqliteValue::Text(o)) if o.to_string() == op))
}

/// Build the same schema+data in both engines. `ddl`+`rows` are applied verbatim to each.
async fn both(ddl: &[&str], inserts: &[String]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for stmt in ddl {
        f.execute(stmt).await.unwrap();
        r.execute(stmt, []).unwrap();
    }
    for stmt in inserts {
        f.execute(stmt).await.unwrap();
        r.execute(stmt, []).unwrap();
    }
    (f, r)
}

async fn assert_same(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let fv = f_rows(f, sql).await;
    let rv = r_rows(r, sql);
    assert_eq!(
        fv, rv,
        "\nMISMATCH for `{sql}`\n fsqlite:\n{fv}\n rusqlite:\n{rv}\n"
    );
}

/// Order-insensitive comparison for DECLINE-control shapes. The ordinary
/// DISTINCT fallback preserves first-seen scan order, while an index-backed
/// loose scan emits index order. These controls only need to prove the set is
/// correct and the shape fell back without semantic drift.
async fn assert_same_set(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let mut fv: Vec<String> = f_rows(f, sql).await.lines().map(str::to_owned).collect();
    let mut rv: Vec<String> = r_rows(r, sql).lines().map(str::to_owned).collect();
    fv.sort();
    rv.sort();
    assert_eq!(
        fv, rv,
        "\nSET MISMATCH for `{sql}`\n fsqlite:\n{fv:?}\n rusqlite:\n{rv:?}\n"
    );
}

/// `SELECT DISTINCT <col>` served by the loose/skip scan over a COMPOSITE index whose LEADING key
/// term is the DISTINCT column. Only index column 0 is read and the probe is a 1-field prefix, so
/// entries sharing the leading value (with different trailing terms) are all cleared by one
/// `SeekGT [value]` exactly like a single-column index. Byte-exact vs rusqlite, and the skip scan
/// must fire (no ephemeral DISTINCT membership index) even though only a composite index exists.
#[test]
fn distinct_loose_scan_composite_leading_term_matches_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        // ---- Composite (a, b) index, NO single-column index on `a`; many dup `a` runs + a NULL run. ----
        let mut ins = Vec::new();
        for i in 1..=2000 {
            let a = if i % 41 == 0 {
                "NULL".to_string()
            } else {
                (i % 13).to_string()
            };
            // `b` varies within each `a` group so the leading-value run spans multiple composite entries.
            ins.push(format!("INSERT INTO ct VALUES ({i}, {a}, {});", i % 7));
        }
        let (f, r) = both(
            &[
                "CREATE TABLE ct (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
                "CREATE INDEX idx_ab ON ct(a, b);",
            ],
            &ins,
        )
        .await;
        assert!(
            !has_op(&f, "SELECT DISTINCT a FROM ct", "OpenAutoindex").await,
            "loose scan should serve SELECT DISTINCT a over the composite index idx_ab(a, b) (no DISTINCT membership index)"
        );
        assert_same(&f, &r, "SELECT DISTINCT a FROM ct").await;
        assert_same(&f, &r, "SELECT DISTINCT a FROM ct ORDER BY a").await;

        // ---- TEXT leading term, composite (s, n); dup runs + NULLs. ----
        let mut tins = Vec::new();
        for i in 1..=600 {
            let s = if i % 29 == 0 {
                "NULL".to_string()
            } else {
                format!("'g{:02}'", i % 9)
            };
            tins.push(format!("INSERT INTO cts VALUES ({i}, {s}, {});", i % 4));
        }
        let (f2, r2) = both(
            &[
                "CREATE TABLE cts (id INTEGER PRIMARY KEY, s TEXT, n INTEGER);",
                "CREATE INDEX idx_sn ON cts(s, n);",
            ],
            &tins,
        )
        .await;
        assert!(
            !has_op(&f2, "SELECT DISTINCT s FROM cts", "OpenAutoindex").await,
            "loose scan should serve SELECT DISTINCT s over composite idx_sn(s, n)"
        );
        assert_same(&f2, &r2, "SELECT DISTINCT s FROM cts").await;

        // ---- A trailing-DESC term must not disqualify: only the ASC BINARY LEADING term governs. ----
        let mut dins = Vec::new();
        for i in 1..=800 {
            dins.push(format!(
                "INSERT INTO cd VALUES ({i}, {}, {});",
                i % 6,
                i % 3
            ));
        }
        let (f3, r3) = both(
            &[
                "CREATE TABLE cd (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
                "CREATE INDEX idx_ad ON cd(a ASC, b DESC);",
            ],
            &dins,
        )
        .await;
        assert!(
            !has_op(&f3, "SELECT DISTINCT a FROM cd", "OpenAutoindex").await,
            "loose scan should serve SELECT DISTINCT a over idx_ad(a ASC, b DESC) — only leading term governs"
        );
        assert_same(&f3, &r3, "SELECT DISTINCT a FROM cd").await;

        // ---- Both a single-column AND a composite index exist: still correct (narrowest preferred). ----
        let mut bins = Vec::new();
        for i in 1..=500 {
            bins.push(format!(
                "INSERT INTO cb VALUES ({i}, {}, {});",
                i % 10,
                i % 5
            ));
        }
        let (f4, r4) = both(
            &[
                "CREATE TABLE cb (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
                "CREATE INDEX idx_b_ab ON cb(a, b);",
                "CREATE INDEX idx_b_a ON cb(a);",
            ],
            &bins,
        )
        .await;
        assert!(!has_op(&f4, "SELECT DISTINCT a FROM cb", "OpenAutoindex").await);
        assert_same(&f4, &r4, "SELECT DISTINCT a FROM cb").await;
    });
}

/// `SELECT DISTINCT a, b, …` (N columns) served by the loose/skip scan over an index whose LEADING N
/// key terms are exactly those columns in SELECT order: the scan reads index columns 0..N-1, emits the
/// tuple once, and `SeekGT [v0..vN-1]` skips the whole run. Byte-exact vs rusqlite (both walk the same
/// covering index in the same order); the skip scan must fire (no ephemeral DISTINCT membership
/// index). Reversed / non-prefix column orders correctly DECLINE to the ordinary membership path
/// (still correct — asserted as a set).
#[test]
fn distinct_loose_scan_multi_column_prefix_matches_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        // ---- Two-column DISTINCT over index (a, b); dup (a,b) pairs + NULLs in each column. ----
        let mut ins = Vec::new();
        for i in 1..=3000 {
            let a = if i % 43 == 0 {
                "NULL".to_string()
            } else {
                (i % 9).to_string()
            };
            let b = if i % 31 == 0 {
                "NULL".to_string()
            } else {
                (i % 5).to_string()
            };
            ins.push(format!("INSERT INTO mt VALUES ({i}, {a}, {b});"));
        }
        let (f, r) = both(
            &[
                "CREATE TABLE mt (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
                "CREATE INDEX idx_ab ON mt(a, b);",
            ],
            &ins,
        )
        .await;
        assert!(
            !has_op(&f, "SELECT DISTINCT a, b FROM mt", "OpenAutoindex").await,
            "multi-col loose scan should serve SELECT DISTINCT a, b over idx_ab(a, b) (no DISTINCT membership index)"
        );
        assert_same(&f, &r, "SELECT DISTINCT a, b FROM mt").await;
        assert_same(&f, &r, "SELECT DISTINCT a, b FROM mt ORDER BY a, b").await;

        // ---- Two-column DISTINCT as a PREFIX of a 3-term index (a, b, c). ----
        let mut pins = Vec::new();
        for i in 1..=2000 {
            pins.push(format!(
                "INSERT INTO pt VALUES ({i}, {}, {}, {});",
                i % 7,
                i % 4,
                i % 3
            ));
        }
        let (f2, r2) = both(
            &[
                "CREATE TABLE pt (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
                "CREATE INDEX idx_abc ON pt(a, b, c);",
            ],
            &pins,
        )
        .await;
        assert!(
            !has_op(&f2, "SELECT DISTINCT a, b FROM pt", "OpenAutoindex").await,
            "multi-col loose scan should serve SELECT DISTINCT a, b over the 3-term prefix idx_abc(a, b, c)"
        );
        assert_same(&f2, &r2, "SELECT DISTINCT a, b FROM pt").await;

        // ---- TEXT + INTEGER two-column DISTINCT over (s, n). ----
        let mut tins = Vec::new();
        for i in 1..=1500 {
            let s = if i % 37 == 0 {
                "NULL".to_string()
            } else {
                format!("'g{}'", i % 6)
            };
            tins.push(format!("INSERT INTO st VALUES ({i}, {s}, {});", i % 4));
        }
        let (f3, r3) = both(
            &[
                "CREATE TABLE st (id INTEGER PRIMARY KEY, s TEXT, n INTEGER);",
                "CREATE INDEX idx_sn ON st(s, n);",
            ],
            &tins,
        )
        .await;
        assert!(
            !has_op(&f3, "SELECT DISTINCT s, n FROM st", "OpenAutoindex").await,
            "multi-col loose scan should serve SELECT DISTINCT s, n over idx_sn(s, n)"
        );
        assert_same(&f3, &r3, "SELECT DISTINCT s, n FROM st").await;

        // ---- DECLINE control: reversed / non-prefix column order falls back to the ordinary
        // DISTINCT membership index (still correct as a set). ----
        // `DISTINCT b, a` is NOT the leading prefix of idx_ab(a, b) in order, so it must decline.
        assert!(
            has_op(&f, "SELECT DISTINCT b, a FROM mt", "OpenAutoindex").await,
            "reversed column order must decline the loose scan (uses DISTINCT membership index)"
        );
        assert_same_set(&f, &r, "SELECT DISTINCT b, a FROM mt").await;
    });
}

#[test]
fn distinct_loose_scan_matches_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        // ---- Main eligible table: few distinct values among many rows, with a NULL run. ----
        let mut ins = Vec::new();
        for i in 1..=2000 {
            let a = if i % 37 == 0 {
                "NULL".to_string()
            } else {
                (i % 12).to_string()
            };
            ins.push(format!("INSERT INTO t VALUES ({i}, {a}, {});", i % 5));
        }
        let (f, r) = both(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
                "CREATE INDEX idx_a ON t(a);",
            ],
            &ins,
        )
        .await;
        // The loose scan must have fired (no DISTINCT membership index) for the eligible shape.
        assert!(
            !has_op(&f, "SELECT DISTINCT a FROM t", "OpenAutoindex").await,
            "loose scan should serve SELECT DISTINCT a (no DISTINCT membership index)"
        );
        assert_same(&f, &r, "SELECT DISTINCT a FROM t").await;
        assert_same(&f, &r, "SELECT DISTINCT a FROM t ORDER BY a").await;

        // ---- Text column, dup runs + NULLs. ----
        let mut tins = Vec::new();
        for i in 1..=500 {
            let s = if i % 23 == 0 {
                "NULL".to_string()
            } else {
                format!("'k{:02}'", i % 8)
            };
            tins.push(format!("INSERT INTO tt VALUES ({i}, {s});"));
        }
        let (f2, r2) = both(
            &[
                "CREATE TABLE tt (id INTEGER PRIMARY KEY, s TEXT);",
                "CREATE INDEX idx_s ON tt(s);",
            ],
            &tins,
        )
        .await;
        assert!(!has_op(&f2, "SELECT DISTINCT s FROM tt", "OpenAutoindex").await);
        assert_same(&f2, &r2, "SELECT DISTINCT s FROM tt").await;

        // ---- Edge tables. ----
        for (label, rows) in [
            ("empty", vec![]),
            ("single", vec!["INSERT INTO e VALUES (1, 42);".to_string()]),
            (
                "all-same",
                (1..=50)
                    .map(|i| format!("INSERT INTO e VALUES ({i}, 7);"))
                    .collect(),
            ),
            (
                "all-null",
                (1..=50)
                    .map(|i| format!("INSERT INTO e VALUES ({i}, NULL);"))
                    .collect(),
            ),
        ] {
            let (fe, re) = both(
                &[
                    "CREATE TABLE e (id INTEGER PRIMARY KEY, a INTEGER);",
                    "CREATE INDEX idx_e ON e(a);",
                ],
                &rows,
            )
            .await;
            assert_same(&fe, &re, "SELECT DISTINCT a FROM e").await;
            let _ = label;
        }

        // ---- Control: NOCASE column must DECLINE (fall back) but still be correct. ----
        let (fc, rc) = both(
            &[
                "CREATE TABLE nc (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE);",
                "CREATE INDEX idx_nc ON nc(s);",
            ],
            &[
                "INSERT INTO nc VALUES (1, 'Apple');".to_string(),
                "INSERT INTO nc VALUES (2, 'apple');".to_string(),
                "INSERT INTO nc VALUES (3, 'BANANA');".to_string(),
                "INSERT INTO nc VALUES (4, 'banana');".to_string(),
            ],
        )
        .await;
        assert!(
            has_op(&fc, "SELECT DISTINCT s FROM nc", "OpenAutoindex").await,
            "NOCASE DISTINCT must decline the BINARY-only loose scan"
        );
        assert_same_set(&fc, &rc, "SELECT DISTINCT s FROM nc").await;

        // ---- Control: non-indexed column falls back. ----
        let (fn_, rn) = both(
            &["CREATE TABLE ni (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);"],
            &(1..=100)
                .map(|i| format!("INSERT INTO ni VALUES ({i}, {}, {});", i % 6, i % 6))
                .collect::<Vec<_>>(),
        )
        .await;
        assert!(
            has_op(&fn_, "SELECT DISTINCT b FROM ni", "OpenAutoindex").await,
            "non-indexed DISTINCT must use the ordinary membership path"
        );
        assert_same_set(&fn_, &rn, "SELECT DISTINCT b FROM ni").await;
    });
}

/// Cross-storage-class runs: a typeless (BLOB-affinity) column stores `2` and `2.0` as DISTINCT
/// storage classes that COMPARE equal, so one loose-scan skip must clear the mixed run exactly like
/// C SQLite's index scan. REAL columns exercise float-key runs, and negative/boundary integers pin
/// the varint edges of the probe record.
#[test]
fn distinct_loose_scan_mixed_storage_classes_match_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        // ---- Typeless column: int 2 and float 2.0 compare equal but store differently. ----
        let mut ins = Vec::new();
        for i in 1..=300 {
            let v = match i % 6 {
                0 => "2".to_string(),
                1 => "2.0".to_string(),
                2 => "7".to_string(),
                3 => "7.5".to_string(),
                4 => "'txt'".to_string(),
                _ => "NULL".to_string(),
            };
            ins.push(format!("INSERT INTO m VALUES ({i}, {v});"));
        }
        let (f, r) = both(
            &[
                "CREATE TABLE m (id INTEGER PRIMARY KEY, v);",
                "CREATE INDEX idx_m ON m(v);",
            ],
            &ins,
        )
        .await;
        assert_same(&f, &r, "SELECT DISTINCT v FROM m").await;

        // ---- REAL column: float duplicate runs, negatives, and integer-valued floats. ----
        let mut rins = Vec::new();
        for i in 1..=400 {
            let v = match i % 5 {
                0 | 1 => "-3.25",
                2 => "0.0",
                3 => "9007199254740993.0",
                _ => "1.5",
            };
            rins.push(format!("INSERT INTO fr VALUES ({i}, {v});"));
        }
        let (f2, r2) = both(
            &[
                "CREATE TABLE fr (id INTEGER PRIMARY KEY, v REAL);",
                "CREATE INDEX idx_fr ON fr(v);",
            ],
            &rins,
        )
        .await;
        assert_same(&f2, &r2, "SELECT DISTINCT v FROM fr").await;

        // ---- All-distinct column: the adaptive Next-probe path (no seeks) must stay byte-exact. ----
        let (fh, rh) = both(
            &[
                "CREATE TABLE hd (id INTEGER PRIMARY KEY, v INTEGER);",
                "CREATE INDEX idx_hd ON hd(v);",
            ],
            &(1..=500)
                .map(|i| format!("INSERT INTO hd VALUES ({i}, {});", i * 13 % 4999))
                .collect::<Vec<_>>(),
        )
        .await;
        assert_same(&fh, &rh, "SELECT DISTINCT v FROM hd").await;

        // ---- Short runs (2-3 per value): the boundary between Next-probe exit and SeekGT. ----
        let (fs, rs) = both(
            &[
                "CREATE TABLE sr (id INTEGER PRIMARY KEY, v INTEGER);",
                "CREATE INDEX idx_sr ON sr(v);",
            ],
            &(1..=300)
                .map(|i| format!("INSERT INTO sr VALUES ({i}, {});", i / 3))
                .collect::<Vec<_>>(),
        )
        .await;
        assert_same(&fs, &rs, "SELECT DISTINCT v FROM sr").await;

        // ---- Integer boundary values: i64::MIN/MAX runs must not confuse the probe. ----
        let mut bins = Vec::new();
        for i in 1..=90 {
            let v = match i % 3 {
                0 => i64::MIN.to_string(),
                1 => i64::MAX.to_string(),
                _ => "0".to_string(),
            };
            bins.push(format!("INSERT INTO bt VALUES ({i}, {v});"));
        }
        let (f3, r3) = both(
            &[
                "CREATE TABLE bt (id INTEGER PRIMARY KEY, v INTEGER);",
                "CREATE INDEX idx_bt ON bt(v);",
            ],
            &bins,
        )
        .await;
        assert_same(&f3, &r3, "SELECT DISTINCT v FROM bt").await;
    });
}

/// File-backed variant of the main eligible shape: the loose scan must behave identically through
/// the pager/transaction cursor stack (`CursorBackend::Txn`), not just the `:memory:` image.
#[test]
fn distinct_loose_scan_file_backed_matches_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("loose_scan.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER);",
            "CREATE INDEX idx_a ON t(a);",
        ] {
            f.execute(stmt).await.unwrap();
            r.execute(stmt, []).unwrap();
        }
        f.execute("BEGIN;").await.unwrap();
        for i in 1..=1200 {
            let a = if i % 31 == 0 {
                "NULL".to_string()
            } else {
                (i % 9).to_string()
            };
            let stmt = format!("INSERT INTO t VALUES ({i}, {a});");
            f.execute(&stmt).await.unwrap();
            r.execute(&stmt, []).unwrap();
        }
        f.execute("COMMIT;").await.unwrap();

        assert_same(&f, &r, "SELECT DISTINCT a FROM t").await;
        assert_same(&f, &r, "SELECT DISTINCT a FROM t ORDER BY a").await;
    });
}
