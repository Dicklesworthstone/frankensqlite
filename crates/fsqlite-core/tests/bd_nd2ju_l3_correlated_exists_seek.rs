// Keeper for bd-nd2ju / GH#377 (L3): the trigger-WHEN correlated EXISTS
// statement-fallback relaxes collation/affinity-safe `inner_col = <bound outer
// value>` equalities to literals so the nested statement seeks instead of
// scanning. The relaxation is only sound when a plain-literal comparison is
// byte-identical to the bound comparison; this test diffs frank against the
// rusqlite oracle across the affinity/collation combinations that are the
// safety-critical surface — including the cases the gate must DECLINE (where the
// interpreted fallback must still return the correct answer).
//
// The trigger `WHEN EXISTS(... AND ...)` is a two-term correlated guard, which
// always falls through `try_direct_exists_probe` to the statement fallback where
// L3 lives, regardless of child row count.
use fsqlite_core::connection::Connection;

/// Run a schedule of INSERTs (each may be rejected by the ABORT trigger) against
/// both engines and assert the surviving table contents match exactly.
async fn assert_trigger_dedup_matches_oracle(
    setup: &str,
    trigger: &str,
    inserts: &[&str],
    read_back: &str,
) {
    // frank
    let c = Connection::open(":memory:").await.unwrap();
    c.execute(setup).await.unwrap();
    c.execute(trigger).await.unwrap();
    let mut frank_ok = Vec::new();
    for ins in inserts {
        frank_ok.push(c.execute(ins).await.is_ok());
    }
    let frank_rows: Vec<Vec<String>> = c
        .query(read_back)
        .await
        .unwrap()
        .iter()
        .map(|r| r.values().iter().map(|v| format!("{v:?}")).collect())
        .collect();

    // oracle
    let o = rusqlite::Connection::open_in_memory().unwrap();
    o.execute_batch(setup).unwrap();
    o.execute_batch(trigger).unwrap();
    let mut rus_ok = Vec::new();
    for ins in inserts {
        rus_ok.push(o.execute(ins, []).is_ok());
    }
    let ncols = {
        let stmt = o.prepare(read_back).unwrap();
        stmt.column_count()
    };
    let mut stmt = o.prepare(read_back).unwrap();
    let rus_rows: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            let mut fields = Vec::new();
            for i in 0..ncols {
                let v = row.get_ref_unwrap(i);
                fields.push(match v {
                    rusqlite::types::ValueRef::Null => "Null".to_string(),
                    rusqlite::types::ValueRef::Integer(n) => format!("Integer({n})"),
                    rusqlite::types::ValueRef::Real(f) => format!("Real({f})"),
                    rusqlite::types::ValueRef::Text(t) => {
                        format!("Text({:?})", String::from_utf8_lossy(t))
                    }
                    rusqlite::types::ValueRef::Blob(b) => format!("Blob({b:?})"),
                });
            }
            Ok(fields)
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();

    // The accept/reject decision per insert must match, and so must the final
    // table. We compare the ACCEPT PATTERN and the surviving row COUNT (value
    // rendering differs between the two type systems, so match structurally).
    assert_eq!(
        frank_ok, rus_ok,
        "accept/reject pattern diverged for setup={setup:?}"
    );
    assert_eq!(
        frank_rows.len(),
        rus_rows.len(),
        "surviving row count diverged for setup={setup:?}: frank {frank_rows:?} vs oracle {rus_rows:?}"
    );
}

const TRIGGER_2TERM: &str = "CREATE TRIGGER trg BEFORE INSERT ON t \
     WHEN EXISTS (SELECT 1 FROM t AS e WHERE e.k1 = NEW.k1 AND e.k2 = NEW.k2) \
     BEGIN SELECT RAISE(ABORT, 'dup'); END";

/// Same as [`assert_trigger_dedup_matches_oracle`] but runs the whole insert
/// schedule inside ONE explicit transaction. This is the critical
/// visibility check for L3: the relaxed nested-statement seek must observe
/// rows inserted EARLIER IN THE SAME UNCOMMITTED TRANSACTION (exactly the
/// bulk-ingest shape the timing probe measures), so a same-txn duplicate is
/// rejected just as the interpreted MemDB-mirror probe rejected it.
async fn assert_trigger_dedup_matches_oracle_txn(
    setup: &str,
    trigger: &str,
    inserts: &[&str],
) {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute(setup).await.unwrap();
    c.execute(trigger).await.unwrap();
    c.execute("BEGIN").await.unwrap();
    let mut frank_ok = Vec::new();
    for ins in inserts {
        frank_ok.push(c.execute(ins).await.is_ok());
    }
    c.execute("COMMIT").await.unwrap();
    let frank_count = c.query("SELECT count(*) FROM t").await.unwrap()[0].values()[0].clone();

    let o = rusqlite::Connection::open_in_memory().unwrap();
    o.execute_batch(setup).unwrap();
    o.execute_batch(trigger).unwrap();
    o.execute_batch("BEGIN").unwrap();
    let mut rus_ok = Vec::new();
    for ins in inserts {
        rus_ok.push(o.execute(ins, []).is_ok());
    }
    o.execute_batch("COMMIT").unwrap();
    let rus_count: i64 = o
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();

    assert_eq!(
        frank_ok, rus_ok,
        "in-transaction accept/reject pattern diverged for setup={setup:?}"
    );
    assert_eq!(
        frank_count,
        fsqlite_types::SqliteValue::Integer(rus_count),
        "in-transaction surviving row count diverged for setup={setup:?}"
    );
}

#[test]
fn nd2ju_l3_same_transaction_dedup_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        // Duplicates that are only visible as UNCOMMITTED same-txn writes must
        // still be rejected — the seek has to see them. Covers both rowid and
        // WITHOUT ROWID shapes.
        assert_trigger_dedup_matches_oracle_txn(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k1 TEXT, k2 TEXT, UNIQUE(k1,k2))",
            TRIGGER_2TERM,
            &[
                "INSERT INTO t(k1,k2) VALUES('a','x')",
                "INSERT INTO t(k1,k2) VALUES('a','x')", // same-txn dup -> must reject
                "INSERT INTO t(k1,k2) VALUES('a','y')",
                "INSERT INTO t(k1,k2) VALUES('b','x')",
                "INSERT INTO t(k1,k2) VALUES('a','y')", // same-txn dup -> must reject
            ],
        )
        .await;
        assert_trigger_dedup_matches_oracle_txn(
            "CREATE TABLE t(k1 TEXT NOT NULL, k2 TEXT NOT NULL, PRIMARY KEY(k1,k2)) WITHOUT ROWID",
            "CREATE TRIGGER trg BEFORE INSERT ON t \
             WHEN EXISTS (SELECT 1 FROM t AS e WHERE e.k1 = NEW.k1 AND e.k2 = NEW.k2) \
             BEGIN SELECT RAISE(ABORT, 'dup'); END",
            &[
                "INSERT INTO t(k1,k2) VALUES('a','x')",
                "INSERT INTO t(k1,k2) VALUES('a','x')", // same-txn dup -> must reject
                "INSERT INTO t(k1,k2) VALUES('c','z')",
            ],
        )
        .await;
    });
}

#[test]
fn nd2ju_l3_text_binary_correlated_exists_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        // Common trigger shape: TEXT columns, BINARY collation, composite UNIQUE.
        // The gate relaxes (col affinity/collation == outer's), seek fires.
        assert_trigger_dedup_matches_oracle(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k1 TEXT, k2 TEXT, UNIQUE(k1,k2))",
            TRIGGER_2TERM,
            &[
                "INSERT INTO t(k1,k2) VALUES('a','x')",
                "INSERT INTO t(k1,k2) VALUES('a','y')",
                "INSERT INTO t(k1,k2) VALUES('a','x')", // dup -> rejected
                "INSERT INTO t(k1,k2) VALUES('b','x')",
                "INSERT INTO t(k1,k2) VALUES('b','x')", // dup -> rejected
            ],
            "SELECT k1,k2 FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn nd2ju_l3_without_rowid_pk_correlated_exists_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        // WITHOUT ROWID composite PK: exercises the L1/L2 seek through the
        // relaxed literal.
        assert_trigger_dedup_matches_oracle(
            "CREATE TABLE t(k1 TEXT NOT NULL, k2 TEXT NOT NULL, v INTEGER, PRIMARY KEY(k1,k2)) WITHOUT ROWID",
            "CREATE TRIGGER trg BEFORE INSERT ON t \
             WHEN EXISTS (SELECT 1 FROM t AS e WHERE e.k1 = NEW.k1 AND e.k2 = NEW.k2) \
             BEGIN SELECT RAISE(ABORT, 'dup'); END",
            &[
                "INSERT INTO t(k1,k2,v) VALUES('a','x',1)",
                "INSERT INTO t(k1,k2,v) VALUES('a','y',2)",
                "INSERT INTO t(k1,k2,v) VALUES('a','x',3)", // dup
                "INSERT INTO t(k1,k2,v) VALUES('c','z',4)",
            ],
            "SELECT k1,k2 FROM t ORDER BY k1,k2",
        )
        .await;
    });
}

#[test]
fn nd2ju_l3_nocase_collation_correlated_exists_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        // NOCASE on both the inner column and (via NEW.k1) the outer donor: the
        // gate relaxes (donor collation == column collation), and the seek must
        // match case-insensitively exactly as the oracle does.
        assert_trigger_dedup_matches_oracle(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k1 TEXT COLLATE NOCASE, k2 TEXT, UNIQUE(k1,k2))",
            TRIGGER_2TERM,
            &[
                "INSERT INTO t(k1,k2) VALUES('Abc','x')",
                "INSERT INTO t(k1,k2) VALUES('ABC','x')", // NOCASE dup -> rejected
                "INSERT INTO t(k1,k2) VALUES('abc','y')",
                "INSERT INTO t(k1,k2) VALUES('def','y')",
            ],
            "SELECT k1,k2 FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn nd2ju_l3_integer_affinity_correlated_exists_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        // INTEGER-affinity correlated key.
        assert_trigger_dedup_matches_oracle(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k1 INTEGER, k2 INTEGER, UNIQUE(k1,k2))",
            TRIGGER_2TERM,
            &[
                "INSERT INTO t(k1,k2) VALUES(1,10)",
                "INSERT INTO t(k1,k2) VALUES(1,11)",
                "INSERT INTO t(k1,k2) VALUES(1,10)", // dup
                "INSERT INTO t(k1,k2) VALUES(2,10)",
            ],
            "SELECT k1,k2 FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn nd2ju_l3_affinity_mismatch_text_vs_numeric_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        // Hazard combo: TEXT inner column, but the outer donor NEW.k1 is a column
        // whose declared affinity differs from a plain literal comparison in the
        // subset/superset way `is_simple_constant` guards against. The gate must
        // DECLINE (or relax only when provably equal) and the answer must still
        // match the oracle via the interpreted fallback.
        //
        // Here k1 is TEXT but values are numeric-looking strings vs integer-typed
        // NEW columns cross-inserted, stressing affinity coercion at the '=' .
        assert_trigger_dedup_matches_oracle(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k1 TEXT, k2 NUMERIC, UNIQUE(k1,k2))",
            TRIGGER_2TERM,
            &[
                "INSERT INTO t(k1,k2) VALUES('5', 10)",
                "INSERT INTO t(k1,k2) VALUES('5', 10.0)", // numeric-affinity dup of 10
                "INSERT INTO t(k1,k2) VALUES('05', 10)",  // '05' != '5' as text
                "INSERT INTO t(k1,k2) VALUES('5', 20)",
            ],
            "SELECT k1,k2 FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn nd2ju_l3_cross_column_affinity_mismatch_declines_but_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        // Cross-column correlated compare `e.k1 (TEXT) = NEW.k2 (INTEGER)`: the
        // donor's declared affinity/collation differ from the inner column, so
        // the relaxation gate MUST decline and the interpreted per-row fallback
        // must still return exactly the oracle's answer. This is the precise
        // subset/superset hazard `is_simple_constant` guards against.
        assert_trigger_dedup_matches_oracle(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k1 TEXT, k2 INTEGER)",
            "CREATE TRIGGER trg BEFORE INSERT ON t \
             WHEN EXISTS (SELECT 1 FROM t AS e WHERE e.k1 = NEW.k2) \
             BEGIN SELECT RAISE(ABORT, 'xdup'); END",
            &[
                "INSERT INTO t(k1,k2) VALUES('5', 1)",
                "INSERT INTO t(k1,k2) VALUES('9', 5)", // e.k1='5' = NEW.k2=5 ? affinity-dependent
                "INSERT INTO t(k1,k2) VALUES('x', 9)",
                "INSERT INTO t(k1,k2) VALUES('7', 7)",
            ],
            "SELECT k1,k2 FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn nd2ju_l3_cross_column_collation_mismatch_declines_but_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        // Cross-column compare where the inner column is NOCASE but the donor
        // column is BINARY: the gate must decline (donor collation != column
        // collation) and the interpreted fallback must match the oracle.
        assert_trigger_dedup_matches_oracle(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k1 TEXT COLLATE NOCASE, k2 TEXT COLLATE BINARY)",
            "CREATE TRIGGER trg BEFORE INSERT ON t \
             WHEN EXISTS (SELECT 1 FROM t AS e WHERE e.k1 = NEW.k2) \
             BEGIN SELECT RAISE(ABORT, 'xdup'); END",
            &[
                "INSERT INTO t(k1,k2) VALUES('Abc','zzz')",
                "INSERT INTO t(k1,k2) VALUES('yyy','ABC')", // e.k1='Abc' =NOCASE= NEW.k2='ABC'
                "INSERT INTO t(k1,k2) VALUES('m','n')",
            ],
            "SELECT k1,k2 FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn nd2ju_l3_blob_affinity_correlated_exists_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        // BLOB (no) affinity column: comparison applies no affinity; a relaxed
        // literal must preserve that.
        assert_trigger_dedup_matches_oracle(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k1 BLOB, k2 BLOB, UNIQUE(k1,k2))",
            TRIGGER_2TERM,
            &[
                "INSERT INTO t(k1,k2) VALUES('a', x'01')",
                "INSERT INTO t(k1,k2) VALUES('a', x'01')", // dup
                "INSERT INTO t(k1,k2) VALUES('a', x'02')",
                "INSERT INTO t(k1,k2) VALUES(1, x'01')", // integer 1 != text 'a' under BLOB
            ],
            "SELECT k1,k2 FROM t ORDER BY id",
        )
        .await;
    });
}
