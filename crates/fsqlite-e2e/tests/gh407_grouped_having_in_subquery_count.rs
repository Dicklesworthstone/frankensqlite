//! GH #407 — `COUNT(*)` over a grouped/`HAVING` `IN`-subquery must keep
//! returning the count once further predicates follow the subquery.
//!
//! Reported against fsqlite 0.3.15 from beads_rust: the multi-label AND filter
//!
//! ```sql
//! SELECT COUNT(*) FROM issues WHERE issues.id IN (
//!     SELECT issue_id FROM labels WHERE label IN (?, ?)
//!     GROUP BY issue_id HAVING COUNT(DISTINCT label) = ?)
//! ```
//!
//! answered 1, but the same query with `1=1 AND` in front and
//! `AND status NOT IN (...) AND (is_template = 0 OR is_template IS NULL)`
//! behind it answered NULL / no row. Only the trailing predicates differ, and
//! stock SQLite answers 1 for both.
//!
//! Every query here is checked against bundled stock SQLite, and the shapes
//! sweep the neighbourhood: the subquery first / last / in the middle, with
//! and without `GROUP BY` on the outer query, with `AND` and with `OR`
//! trailing predicates, with literals and with bound parameters, and for
//! `COUNT(*)`, `COUNT(col)` and `SUM`.

#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_core::connection::{
    hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::SqliteValue;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountRhsCalls(Arc<AtomicUsize>);

impl fsqlite_func::ScalarFunction for CountRhsCalls {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(args[0].clone())
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "count_rhs_calls"
    }
}

#[test]
fn gh407_grouped_rhs_is_lazy_and_reused_within_each_execution() {
    asupersync::test_utils::run_test(|| async {
        for size in [30_i64, 90, 270] {
            let f = Connection::open(":memory:").await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            f.register_deterministic_scalar_function(CountRhsCalls(Arc::clone(&calls)));
            let setup = "CREATE TABLE issues(id INTEGER PRIMARY KEY, status TEXT); \
                         CREATE TABLE labels(issue_id INTEGER PRIMARY KEY);";
            f.execute(setup).await.unwrap();
            r.execute_batch(setup).unwrap();
            f.execute("BEGIN").await.unwrap();
            for id in 1..=size {
                let insert = format!(
                    "INSERT INTO issues VALUES({id},'open'); INSERT INTO labels VALUES({id});"
                );
                f.execute(&insert).await.unwrap();
                r.execute_batch(&insert).unwrap();
            }
            f.execute("COMMIT").await.unwrap();

            // The identity callback counts real RHS projection work. Its stock
            // equivalent projects issue_id directly; both must return INTEGER.
            let absorbed = f.query("SELECT COUNT(*) FROM issues WHERE status='open' OR id IN (SELECT count_rhs_calls(issue_id) FROM labels GROUP BY issue_id)").await.unwrap();
            assert_eq!(absorbed[0].values(), &[SqliteValue::Integer(size)]);
            assert_eq!(calls.load(Ordering::Relaxed), 0, "absorbed RHS executed");

            let query = "SELECT COUNT(*) FROM issues WHERE status='open' AND id IN (SELECT count_rhs_calls(issue_id) FROM labels WHERE issue_id <= ?1 GROUP BY issue_id HAVING COUNT(*) > 0)";
            let prepared = f.prepare(query).await.unwrap();
            for ceiling in [size, size / 3, 0, size] {
                calls.store(0, Ordering::Relaxed);
                let actual = prepared
                    .query_with_params(&[SqliteValue::Integer(ceiling)])
                    .await
                    .unwrap();
                let stock = sqlite_rows(&r, &format!("SELECT COUNT(*) FROM issues WHERE status='open' AND id IN (SELECT issue_id FROM labels WHERE issue_id <= {ceiling} GROUP BY issue_id HAVING COUNT(*) > 0)")).unwrap();
                let actual: Vec<Vec<String>> = actual
                    .iter()
                    .map(|row| row.values().iter().map(render_frank).collect())
                    .collect();
                assert_eq!(actual, stock);
                let observed = calls.load(Ordering::Relaxed);
                assert_eq!(
                    observed,
                    usize::try_from(ceiling).unwrap(),
                    "RHS rescanned or stale: size={size}, ceiling={ceiling}"
                );
                eprintln!(
                    "event=gh407_grouped_rhs_work size={size} ceiling={ceiling} calls={observed}"
                );
            }
            // A prepared statement must see intervening writes, not a prior
            // execution's membership set or a pointer-reused nested SELECT.
            f.execute("DELETE FROM labels WHERE issue_id > 1")
                .await
                .unwrap();
            calls.store(0, Ordering::Relaxed);
            let rows = prepared
                .query_with_params(&[SqliteValue::Integer(size)])
                .await
                .unwrap();
            assert_eq!(rows[0].values(), &[SqliteValue::Integer(1)]);
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            drop(prepared);
            f.close().await.unwrap();
        }
    });
}

#[test]
fn gh407_lazy_membership_preserves_values_scopes_and_errors() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let setup = "CREATE TABLE outer_values(id INTEGER PRIMARY KEY, v); \
                     CREATE TABLE rhs(v); \
                     INSERT INTO outer_values VALUES \
                     (1,1),(2,1.0),(3,'1'),(4,NULL),(5,'ALPHA'),(6,'alpha'), \
                     (7,'alpha '),(8,X'616C706861'),(9,9007199254740993), \
                     (10,9007199254740992.0),(11,'a'||char(0)||'b'); \
                     INSERT INTO rhs VALUES (1),(NULL),('alpha'), \
                     (9007199254740993),('a'||char(0)||'b');";
        f.execute(setup).await.unwrap();
        r.execute_batch(setup).unwrap();
        check(&f, &r, &[
            "SELECT id FROM outer_values WHERE id>0 AND v IN (SELECT v FROM rhs GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND v NOT IN (SELECT v FROM rhs GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND v NOT IN (SELECT v FROM rhs WHERE 0 GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND CAST(v AS TEXT) IN (SELECT v FROM rhs GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND CAST(v AS NUMERIC) IN (SELECT v FROM rhs GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND CAST(v AS TEXT) IN (SELECT 1 FROM rhs GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND '1' IN (SELECT CAST(v AS TEXT) FROM rhs GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND CAST(v AS NUMERIC) IN (SELECT 0 FROM rhs GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND CAST(v AS BLOB) IN (SELECT CAST(v AS TEXT) FROM rhs GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND v COLLATE NOCASE IN (SELECT v FROM rhs GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND v COLLATE RTRIM IN (SELECT v FROM rhs GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND v IN (SELECT v FROM rhs WHERE v IN (SELECT outer_values.v) GROUP BY v) ORDER BY id",
            "SELECT id FROM outer_values WHERE id>0 AND v IN (SELECT v FROM rhs GROUP BY v) AND id IN (SELECT id FROM outer_values WHERE id>1 GROUP BY id) ORDER BY id",
        ], "lazy membership values and nested scopes").await;

        let prepared = f.prepare("SELECT COUNT(*) FROM outer_values WHERE id>0 AND v IN (SELECT abs(?1) FROM rhs GROUP BY v)").await.unwrap();
        assert!(
            prepared
                .query_with_params(&[SqliteValue::Integer(i64::MIN)])
                .await
                .is_err()
        );
        let after_error = prepared
            .query_with_params(&[SqliteValue::Integer(1)])
            .await
            .unwrap();
        assert_eq!(after_error[0].values(), &[SqliteValue::Integer(2)]);
        assert_eq!(sqlite_rows(&r, "SELECT COUNT(*) FROM outer_values WHERE id>0 AND v IN (SELECT abs(1) FROM rhs GROUP BY v)").unwrap(), vec![vec!["INTEGER(2)".to_owned()]]);
        drop(prepared);
        f.close().await.unwrap();
    });
}

struct FoldedBinary;

impl fsqlite_func::CollationFunction for FoldedBinary {
    fn name(&self) -> &str {
        "BINARY"
    }

    fn compare(&self, left: &[u8], right: &[u8]) -> std::cmp::Ordering {
        left.to_ascii_lowercase().cmp(&right.to_ascii_lowercase())
    }
}

#[test]
fn gh407_lazy_membership_honors_overridden_binary_collation() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let setup = "CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT); \
                     CREATE TABLE rhs(v TEXT); \
                     INSERT INTO items VALUES (1,'ALPHA'),(2,'alpha'),(3,'beta'); \
                     INSERT INTO rhs VALUES ('alpha');";
        f.execute(setup).await.unwrap();
        r.execute_batch(setup).unwrap();
        f.register_collation_function(FoldedBinary);
        let actual = frank_rows(
            &f,
            "SELECT COUNT(*) FROM items WHERE id>0 AND v IN (SELECT v FROM rhs GROUP BY v)",
        )
        .await
        .unwrap();
        let stock = sqlite_rows(&r, "SELECT COUNT(*) FROM items WHERE id>0 AND v COLLATE NOCASE IN (SELECT v FROM rhs GROUP BY v)").unwrap();
        assert_eq!(actual, vec![vec!["INTEGER(2)".to_owned()]]);
        assert_eq!(actual, stock);
        f.close().await.unwrap();
    });
}

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("INTEGER({n})"),
        SqliteValue::Float(f) => format!("REAL({f})"),
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
                rusqlite::types::Value::Integer(x) => format!("INTEGER({x})"),
                rusqlite::types::Value::Real(f) => format!("REAL({f})"),
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

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"));
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"));
            }
            (Err(a), Err(b)) => mismatches.push(format!(
                "BOTH_ERR for a query required to succeed: {q}\n  frank: {a}\n  csql: {b}"
            )),
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

/// The reporter's fixture, verbatim in shape: three issues, two labels, and a
/// `labels` table with a composite primary key.
async fn data() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE issues (id TEXT PRIMARY KEY, status TEXT NOT NULL, is_template INTEGER)",
        "CREATE TABLE labels (issue_id TEXT NOT NULL, label TEXT NOT NULL, \
         PRIMARY KEY (issue_id, label))",
        "INSERT INTO issues VALUES ('bd-both','open',NULL),('bd-one','open',NULL),\
         ('bd-other','open',NULL)",
        "INSERT INTO labels VALUES ('bd-both','backend'),('bd-both','urgent'),\
         ('bd-one','backend'),('bd-other','urgent')",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

const SUB: &str = "SELECT issue_id FROM labels WHERE label IN ('backend','urgent') \
                   GROUP BY issue_id HAVING COUNT(DISTINCT label) = 2";

#[test]
fn gh407_count_with_trailing_predicates_after_the_in_subquery() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        let a = format!("SELECT COUNT(*) FROM issues WHERE issues.id IN ({SUB})");
        // The exact failing statement from the report.
        let b = format!(
            "SELECT COUNT(*) FROM issues WHERE 1=1 AND issues.id IN ({SUB}) \
             AND status NOT IN ('closed','tombstone','deferred') \
             AND (is_template = 0 OR is_template IS NULL)"
        );
        // The trailing predicates without the leading `1=1`.
        let c = format!(
            "SELECT COUNT(*) FROM issues WHERE issues.id IN ({SUB}) \
             AND status NOT IN ('closed','tombstone','deferred')"
        );
        // Leading `1=1` only.
        let d = format!("SELECT COUNT(*) FROM issues WHERE 1=1 AND issues.id IN ({SUB})");
        check(
            &f,
            &r,
            &[&a, &b, &c, &d],
            "gh407_count_with_trailing_predicates",
        )
        .await;
    });
}

#[test]
fn gh407_neighbouring_predicate_placements() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        let queries = [
            // Subquery in the middle of a conjunction.
            format!(
                "SELECT COUNT(*) FROM issues WHERE status = 'open' AND issues.id IN ({SUB}) \
                 AND is_template IS NULL"
            ),
            // Trailing OR instead of AND.
            format!(
                "SELECT COUNT(*) FROM issues WHERE issues.id IN ({SUB}) \
                 OR status = 'nonexistent'"
            ),
            // A parenthesised trailing disjunction, the reporter's last clause.
            format!(
                "SELECT COUNT(*) FROM issues WHERE issues.id IN ({SUB}) \
                 AND (is_template = 0 OR is_template IS NULL)"
            ),
            // NOT IN over the same grouped subquery.
            format!(
                "SELECT COUNT(*) FROM issues WHERE issues.id NOT IN ({SUB}) \
                 AND status NOT IN ('closed','tombstone')"
            ),
            // A trailing predicate that eliminates every row: still one row, 0.
            format!("SELECT COUNT(*) FROM issues WHERE issues.id IN ({SUB}) AND status = 'closed'"),
            // Other aggregates over the same shape.
            format!(
                "SELECT COUNT(id), COUNT(is_template) FROM issues WHERE issues.id IN ({SUB}) \
                 AND status NOT IN ('closed')"
            ),
            format!(
                "SELECT SUM(1) FROM issues WHERE 1=1 AND issues.id IN ({SUB}) \
                 AND status NOT IN ('closed')"
            ),
            // With an outer GROUP BY.
            format!(
                "SELECT status, COUNT(*) FROM issues WHERE 1=1 AND issues.id IN ({SUB}) \
                 AND status NOT IN ('closed','tombstone','deferred') \
                 AND (is_template = 0 OR is_template IS NULL) GROUP BY status ORDER BY status"
            ),
            // With an outer GROUP BY + HAVING.
            format!(
                "SELECT status, COUNT(*) FROM issues WHERE issues.id IN ({SUB}) \
                 AND status NOT IN ('closed') GROUP BY status HAVING COUNT(*) >= 1 \
                 ORDER BY status"
            ),
            // The plain row list the report says already worked, as a control.
            format!(
                "SELECT id FROM issues WHERE 1=1 AND issues.id IN ({SUB}) \
                 AND status NOT IN ('closed','tombstone','deferred') \
                 AND (is_template = 0 OR is_template IS NULL) ORDER BY id"
            ),
        ];
        let refs: Vec<&str> = queries.iter().map(String::as_str).collect();
        check(&f, &r, &refs, "gh407_neighbouring_predicate_placements").await;
    });
}

#[test]
fn gh407_count_with_bound_parameters() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        let sql = "SELECT COUNT(*) FROM issues WHERE 1=1 AND issues.id IN (\
                   SELECT issue_id FROM labels WHERE label IN (?, ?) \
                   GROUP BY issue_id HAVING COUNT(DISTINCT label) = ?) \
                   AND status NOT IN ('closed','tombstone','deferred') \
                   AND (is_template = 0 OR is_template IS NULL)";
        let params = vec![
            SqliteValue::Text("backend".into()),
            SqliteValue::Text("urgent".into()),
            SqliteValue::Integer(2),
        ];
        let rows = f.query_with_params(sql, &params).await.unwrap();
        assert_eq!(rows.len(), 1, "an aggregate query always returns one row");
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(1),
            "bound-parameter form must count the one matching issue"
        );

        let expected: i64 = r
            .query_row(sql, rusqlite::params!["backend", "urgent", 2_i64], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(expected, 1, "stock SQLite agrees");
    });
}

#[test]
fn gh407_aggregate_preserves_shortcircuit_before_grouped_probe() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        let probe = "SELECT abs(-9223372036854775808) FROM labels GROUP BY label";
        // The RHS must really fail when evaluated. Otherwise an accidentally
        // eager rewrite could pass every absorption case below.
        assert!(
            sqlite_rows(&r, probe)
                .unwrap_err()
                .contains("integer overflow")
        );
        assert!(frank_rows(&f, probe).await.is_err());
        let queries = [
            format!("SELECT COUNT(*) FROM issues WHERE status='open' OR id IN ({probe})"),
            format!(
                "SELECT COUNT(1), COUNT(id), SUM(1) FROM issues WHERE status='open' OR id IN ({probe})"
            ),
            format!("SELECT COUNT(*) FROM issues WHERE NULL AND id IN ({probe})"),
            format!("SELECT COUNT(*) FROM issues WHERE NOT (status='open' OR id IN ({probe}))"),
        ];
        let refs: Vec<&str> = queries.iter().map(String::as_str).collect();
        check(&f, &r, &refs, "gh407 aggregate short-circuit").await;
        f.close().await.unwrap();
    });
}

#[test]
fn gh407_exact_release_reproduction_and_null_duplicate_inputs() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE issues(id TEXT PRIMARY KEY,status TEXT,is_template INT)",
            "CREATE TABLE labels(issue_id TEXT,label TEXT,PRIMARY KEY(issue_id,label))",
            "INSERT INTO issues VALUES('a','open',0),('b','closed',0)",
            "INSERT INTO labels VALUES('a','backend'),('a','urgent')",
        ] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        let exact = "SELECT count(*) FROM issues WHERE 1=1 AND id IN (SELECT issue_id FROM labels WHERE label IN ('backend','urgent') GROUP BY issue_id HAVING count(DISTINCT label)=2)";
        eprintln!("event=gh407_stock_oracle sqlite={}", rusqlite::version());
        assert_eq!(
            sqlite_rows(&r, exact).unwrap(),
            vec![vec!["INTEGER(1)".to_owned()]]
        );
        check(&f, &r, &[exact], "gh407 exact released-binary reproduction").await;
        for sql in [
            "CREATE TABLE label_bag(issue_id TEXT,label TEXT)",
            "INSERT INTO label_bag VALUES('a','backend'),('a','backend'),('a','urgent'),('a',NULL),(NULL,'backend'),(NULL,'urgent'),('b',NULL)",
        ] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        let sub = "SELECT issue_id FROM label_bag GROUP BY issue_id HAVING COUNT(DISTINCT label)=2";
        let mut queries = Vec::new();
        for aggregate in ["COUNT(*)", "COUNT(1)", "COUNT(id)"] {
            for predicate in [
                format!("id IN ({sub})"),
                format!("1=1 AND id IN ({sub})"),
                format!("id IN ({sub}) AND 1=1"),
                format!("status='closed' AND id IN ({sub})"),
                format!("id NOT IN ({sub}) AND 1=1"),
            ] {
                queries.push(format!("SELECT {aggregate} FROM issues WHERE {predicate}"));
            }
        }
        queries.push(format!(
            "SELECT id FROM issues WHERE 1=1 AND id IN ({sub}) ORDER BY id"
        ));
        let refs: Vec<&str> = queries.iter().map(String::as_str).collect();
        check(&f, &r, &refs, "gh407 duplicate and NULL inputs").await;
        f.close().await.unwrap();
    });
}

#[test]
fn gh407_runtime_routing_keeps_simple_probes_compiled() {
    const CHILD: &str = "FSQLITE_GH407_RUNTIME_CHILD";
    if std::env::var(CHILD).as_deref() != Ok("1") {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "gh407_runtime_routing_keeps_simple_probes_compiled",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("{stdout}\n{stderr}");
        assert!(
            output.status.success(),
            "isolated execution-path keeper failed"
        );
        assert!(stderr.contains("event=gh407_runtime_routing_verified"));
        return;
    }
    // Profiling flags and counters are process-global in library builds.
    // The child keeps other concurrently running tests out of the observation.
    asupersync::test_utils::run_test(|| async {
        let mut opcode_counts = Vec::new();
        for size in [30_i64, 300, 3_000] {
            let f = Connection::open(":memory:").await.unwrap();
            assert!(f.is_concurrent_mode_default());
            f.execute("CREATE TABLE issues(id INTEGER PRIMARY KEY,status TEXT); CREATE TABLE labels(issue_id INTEGER PRIMARY KEY,label TEXT); BEGIN")
                .await
                .unwrap();
            {
                let issue = f
                    .prepare("INSERT INTO issues VALUES(?1,'open')")
                    .await
                    .unwrap();
                let label = f
                    .prepare("INSERT INTO labels VALUES(?1,'backend')")
                    .await
                    .unwrap();
                for id in 0..size {
                    issue
                        .execute_with_params(&[SqliteValue::Integer(id)])
                        .await
                        .unwrap();
                    label
                        .execute_with_params(&[SqliteValue::Integer(id)])
                        .await
                        .unwrap();
                }
            }
            f.execute("COMMIT").await.unwrap();
            f.set_reject_mem_fallback(true);
            f.set_strict_mem_fallback_rejection(true);
            f.reset_fallback_decision_evidence();
            set_hot_path_profile_enabled(true);
            reset_hot_path_profile();
            let rows = f.query("SELECT COUNT(*) FROM issues WHERE status='open' AND id IN (SELECT issue_id FROM labels WHERE label='backend')")
                .await
                .unwrap();
            let profile = hot_path_profile_snapshot();
            set_hot_path_profile_enabled(false);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values(), &[SqliteValue::Integer(size)]);
            assert!(f.fallback_execution_snapshot().events.is_empty());
            assert!(profile.vdbe.statements_total > 0);
            assert!(
                profile
                    .vdbe
                    .opcode_execution_totals
                    .iter()
                    .any(|entry| { entry.opcode == "ResultRow" && entry.total > 0 })
            );
            opcode_counts.push(profile.vdbe.opcodes_executed_total);
            eprintln!(
                "event=gh407_compiled_probe size={size} statements={} opcodes={}",
                profile.vdbe.statements_total, profile.vdbe.opcodes_executed_total
            );

            f.set_strict_mem_fallback_rejection(false);
            f.reset_fallback_decision_evidence();
            let rows = f.query("SELECT COUNT(*) FROM issues WHERE status='open' OR id IN (SELECT abs(-9223372036854775808) FROM labels GROUP BY label)")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values(), &[SqliteValue::Integer(size)]);
            let events = f.fallback_execution_snapshot().events;
            assert!(events.iter().any(|event| {
                event.decision_reason == "where_shortcircuit_subquery_fallback"
                    && event.statement_id > 0
                    && event.decision_outcome == "allowed_compatibility_fallback"
            }));
            f.close().await.unwrap();
        }
        // Ten times the data must not induce per-outer-row rescans of the RHS.
        // Dynamic work bounds avoid a scheduler-dependent wall-clock assertion.
        for pair in opcode_counts.windows(2) {
            assert!(
                pair[1] <= pair[0] * 12,
                "simple probe work became superlinear: {pair:?}"
            );
        }
        eprintln!(
            "event=gh407_runtime_routing_verified sqlite={}",
            rusqlite::version()
        );
    });
}
