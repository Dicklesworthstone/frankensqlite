#![recursion_limit = "512"]

//! Randomized differential check of the index-seek paths reworked by the
//! 2026-10-04/05 lane fleet, against rusqlite (bundled SQLite):
//!
//! - composite ON-equality join seeks (07cf6c5a6),
//! - composite WHERE equality scans that seek every pinned key term (55b658f15),
//! - the in-place stored-key comparison of an index seek (e8cdc0530),
//! - correlated EXISTS / scalar probes that reuse parked read cursors
//!   (0c2929769).
//!
//! Its first run found an older bug none of those commits introduced: a bare
//! `count(*)` / `sum()` over equalities sought an index whose key collation
//! differed from the comparison's and answered from the wrong rows (see
//! `index_equality_collation_mismatch_oracle`).
//!
//! Each seed builds two tables whose key columns get random declared types
//! (INTEGER, TEXT, REAL, NUMERIC, BLOB, none), a composite index with random
//! ASC/DESC terms and collations, and rows drawn from a pool of values that
//! type conversion and collation treat differently. Every query runs ad hoc
//! and prepared, in memory and file-backed, and must return stock's rows or
//! fail where stock fails.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        let len = u64::try_from(items.len()).expect("len");
        &items[usize::try_from(self.next() % len).expect("index")]
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

const TYPES: &[&str] = &["INTEGER", "TEXT", "REAL", "NUMERIC", "BLOB", ""];
const COLLATIONS: &[&str] = &["", "", "", " COLLATE NOCASE", " COLLATE RTRIM"];
const VALUES: &[&str] = &[
    "NULL",
    "0",
    "1",
    "2",
    "-1",
    "2.0",
    "2.5",
    "'2'",
    "' 2'",
    "'2.0'",
    "'a'",
    "'A'",
    "'a '",
    "'b'",
    "x'32'",
    "1e20",
    "-0.0",
    "9223372036854775807",
];
const KEYS: &[&str] = &["a", "b", "c"];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{b:?}"),
    }
}

fn rows_r(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let ncol = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            Ok((0..ncol)
                .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                .collect::<Vec<_>>())
        })
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

async fn rows_f(conn: &Connection, sql: &str, prepared: bool) -> Result<Vec<Vec<String>>, String> {
    let rows = if prepared {
        conn.prepare(sql)
            .await
            .map_err(|e| format!("{e:?}"))?
            .query()
            .await
            .map_err(|e| format!("{e:?}"))?
    } else {
        conn.query(sql).await.map_err(|e| format!("{e:?}"))?
    };
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect())
}

/// One seed: schema statements plus the queries to compare.
fn scenario(seed: u64) -> (Vec<String>, Vec<String>) {
    let mut rng = Lcg(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03);
    let mut setup = Vec::new();
    let decl = |rng: &mut Lcg| {
        let ty = *rng.pick(TYPES);
        let coll = if matches!(ty, "TEXT" | "") && rng.chance(25) {
            " COLLATE NOCASE"
        } else {
            ""
        };
        format!("{ty}{coll}")
    };
    let (ta, tb, tc) = (decl(&mut rng), decl(&mut rng), decl(&mut rng));
    let (qa, qb, qc) = (decl(&mut rng), decl(&mut rng), decl(&mut rng));
    setup.push(format!(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a {ta}, b {tb}, c {tc}, v INTEGER)"
    ));
    setup.push(format!(
        "CREATE TABLE q(id INTEGER PRIMARY KEY, a {qa}, b {qb}, c {qc}, w INTEGER)"
    ));

    // A composite index of 2 or 3 distinct key columns.
    let mut cols: Vec<&str> = KEYS.to_vec();
    for i in (1..cols.len()).rev() {
        let j = usize::try_from(rng.next() % (u64::try_from(i).unwrap() + 1)).unwrap();
        cols.swap(i, j);
    }
    let width = if rng.chance(50) { 2 } else { 3 };
    let terms: Vec<String> = cols[..width]
        .iter()
        .map(|col| {
            let coll = *rng.pick(COLLATIONS);
            let dir = if rng.chance(25) { " DESC" } else { "" };
            format!("{col}{coll}{dir}")
        })
        .collect();
    let unique = if rng.chance(20) { "UNIQUE " } else { "" };
    setup.push(format!(
        "CREATE {unique}INDEX t_k ON t({})",
        terms.join(", ")
    ));

    let value = |rng: &mut Lcg| (*rng.pick(VALUES)).to_owned();
    for id in 1..=60 {
        setup.push(format!(
            "INSERT OR IGNORE INTO t VALUES ({id}, {}, {}, {}, {})",
            value(&mut rng),
            value(&mut rng),
            value(&mut rng),
            id % 7
        ));
    }
    for id in 1..=25 {
        setup.push(format!(
            "INSERT INTO q VALUES ({id}, {}, {}, {}, {})",
            value(&mut rng),
            value(&mut rng),
            value(&mut rng),
            id % 3
        ));
    }

    let (k0, k1) = (cols[0], cols[1]);
    let mut queries = Vec::new();
    let pair = |rng: &mut Lcg| {
        // Probe columns of q, sometimes crossed, sometimes operands swapped.
        let (p0, p1) = if rng.chance(30) {
            (*rng.pick(KEYS), *rng.pick(KEYS))
        } else {
            (k0, k1)
        };
        if rng.chance(50) {
            format!("t.{k0} = q.{p0} AND t.{k1} = q.{p1}")
        } else {
            format!("q.{p1} = t.{k1} AND q.{p0} = t.{k0}")
        }
    };
    for _ in 0..3 {
        let on = pair(&mut rng);
        queries.push(format!(
            "SELECT q.id, t.id FROM q JOIN t ON {on} ORDER BY q.id, t.id"
        ));
        let on = pair(&mut rng);
        queries.push(format!(
            "SELECT q.id, t.id FROM q LEFT JOIN t ON {on} AND t.v > 2 ORDER BY q.id, t.id"
        ));
        let on = pair(&mut rng);
        queries.push(format!(
            "SELECT count(*), sum(t.v), count(t.id) FROM q JOIN t ON {on}"
        ));
        queries.push(format!(
            "SELECT q.id, t.id FROM q JOIN t ON t.{k0} = q.{} ORDER BY q.id, t.id",
            rng.pick(KEYS)
        ));
        let on = pair(&mut rng).replace("t.", "t2.");
        queries.push(format!(
            "SELECT q.id, EXISTS (SELECT 1 FROM t AS t2 WHERE {on}), \
             (SELECT count(*) FROM t AS t2 WHERE t2.{k0} = q.{}) FROM q ORDER BY q.id",
            rng.pick(KEYS)
        ));
        queries.push(format!(
            "SELECT id FROM t WHERE {k0} = {} AND {k1} = {} ORDER BY id",
            value(&mut rng),
            value(&mut rng)
        ));
        queries.push(format!(
            "SELECT count(*) FROM t WHERE {k0} = {} AND {k1} = {}",
            value(&mut rng),
            value(&mut rng)
        ));
    }
    (setup, queries)
}

/// `SEEK_DIFF_DUMP=<seed> cargo test ... -- --ignored dump_seed --nocapture`
/// prints one seed's schema, rows and queries for reproducing a mismatch.
#[test]
#[ignore = "debug helper"]
fn dump_seed() {
    let seed: u64 = std::env::var("SEEK_DIFF_DUMP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let (setup, queries) = scenario(seed);
    for sql in setup.iter().chain(&queries) {
        println!("{sql};");
    }
}

#[test]
fn random_composite_seeks_match_stock() {
    // By default 40 random seeds plus the three (93, 120, 122) that found the
    // collation mismatch, about a minute in a debug build; SEEK_DIFF_SEEDS=N
    // sweeps seeds 0..N instead (400 seeds ran clean on 2026-10-05).
    let seeds: Vec<u64> = match std::env::var("SEEK_DIFF_SEEDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(n) => (0..n).collect(),
        None => (0..40).chain([93, 120, 122]).collect(),
    };
    let mut mismatches = Vec::new();
    // [stock returned no rows, stock returned rows, stock failed]: proves the
    // comparison is not vacuous.
    let stats = std::rc::Rc::new(std::cell::RefCell::new([0_u64; 3]));
    for seed in seeds {
        let (setup, queries) = scenario(seed);
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in &setup {
            r.execute(sql, []).unwrap();
        }
        for file_backed in [false, true] {
            let found = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let sink = std::rc::Rc::clone(&found);
            let stats = std::rc::Rc::clone(&stats);
            asupersync::test_utils::run_test(|| {
                let setup = setup.clone();
                let queries = queries.clone();
                let stock: Vec<Result<Vec<Vec<String>>, String>> =
                    queries.iter().map(|sql| rows_r(&r, sql)).collect();
                async move {
                    let dir = tempfile::tempdir().unwrap();
                    let target = if file_backed {
                        dir.path()
                            .join("seek_diff.db")
                            .to_string_lossy()
                            .into_owned()
                    } else {
                        ":memory:".to_owned()
                    };
                    let f = Connection::open(&target).await.unwrap();
                    for sql in &setup {
                        f.execute(sql)
                            .await
                            .unwrap_or_else(|e| panic!("seed {seed} setup `{sql}`: {e:?}"));
                    }
                    let mut out = Vec::new();
                    for (sql, expected) in queries.iter().zip(&stock) {
                        for prepared in [false, true] {
                            let got = rows_f(&f, sql, prepared).await;
                            let same = match (&got, expected) {
                                (Ok(a), Ok(b)) => a == b,
                                (Err(_), Err(_)) => true,
                                _ => false,
                            };
                            stats.borrow_mut()[match expected {
                                Ok(rows) if rows.is_empty() => 0,
                                Ok(_) => 1,
                                Err(_) => 2,
                            }] += 1;
                            if !same {
                                out.push(format!(
                                    "seed {seed} file={file_backed} prepared={prepared}\n  \
                                     schema: {}\n  sql: {sql}\n  fsqlite: {got:?}\n  stock:   {expected:?}",
                                    setup[..3].join("; ")
                                ));
                            }
                        }
                    }
                    sink.borrow_mut().extend(out);
                }
            });
            mismatches.extend(found.take());
        }
    }
    let [empty, nonempty, failed] = *stats.borrow();
    eprintln!(
        "[seek-diff] comparisons: stock-empty={empty} stock-rows={nonempty} stock-error={failed}"
    );
    assert!(
        nonempty > empty && nonempty > failed,
        "the generated queries must mostly return rows to prove anything"
    );
    assert!(
        mismatches.is_empty(),
        "{} mismatches; first ones:\n{}",
        mismatches.len(),
        mismatches
            .iter()
            .take(12)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}
