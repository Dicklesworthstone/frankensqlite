//! `param-tax-controls` — bd-5zeai control battery, controls 1 and 2.
//!
//! Isolates WHERE the ~50-60µs parameterized-execution gap lives without any
//! engine changes, per the agreed measurement design (agent-mail 4469):
//!
//! * **Control 1 (pure Option gate):** the same prepared, placeholder-free
//!   one-row statement (`SELECT 40 + 2`) executed via `query_row()` versus
//!   `query_row_with_params(&[])`. Identical program, identical output; the
//!   only difference is `None` vs `Some(&[])` plumbing. A near-zero delta
//!   here rules out the params `Option` itself.
//!
//! * **Control 2 (literal fast-lane gate):** the literal indexed
//!   COUNT-rowid-probe statement (the benchmark shape with `c.id <= K`
//!   baked in as a literal) through the same two APIs. `query_row()` may
//!   take the prepared count-probe fast lane; `query_row_with_params(&[])`
//!   falls past the `params.is_none()` gates into the generic
//!   pager+VDBE lane. The delta between the two arms IS the
//!   missing-fast-path tax if the attribution is right (~50µs), and ~0 if
//!   it is wrong.
//!
//! * **A/A null:** `query_row()` vs `query_row()` on the control-2
//!   statement bounds host noise.
//!
//! Cache symmetry: each cluster prepares a FRESH statement with a cycling
//! literal threshold so the single-entry count-probe memo (keyed by the
//! resolved bound) cannot serve across clusters; within a cluster the ABBA
//! order (AB then BA) cancels first-touch effects.
//!
//! One runtime entry per timed sample (bd-zavyn discipline). DIAGNOSTIC
//! ONLY: single host, no CPU pinning enforced; the split between control 1
//! and control 2 is the evidence, not the absolute numbers.

// bd-mnlk2 / bd-zavyn: the hoisted timed bodies await fsqlite-core's
// deliberately large, deeply nested engine futures inside one runtime entry
// per sample; boxing them would put an allocation inside the timed window.
#![allow(clippy::large_futures)]

use std::time::{Duration, Instant};

use sha2::Digest as _;

const CLUSTERS: usize = 24;
const SAMPLES_PER_ARM_PER_CLUSTER: usize = 8;
const CATEGORY_COUNT: i64 = 500;
const PRODUCT_COUNT: i64 = 10_000;

fn median(values: &mut [f64]) -> f64 {
    assert!(!values.is_empty(), "median requires at least one sample");
    values.sort_by(f64::total_cmp);
    let upper = values.len() / 2;
    if values.len() % 2 == 0 {
        f64::midpoint(values[upper - 1], values[upper])
    } else {
        values[upper]
    }
}

fn self_provenance() {
    let exe = std::env::current_exe().ok();
    let (sha, len) = exe.as_ref().map_or_else(
        || ("unavailable".to_owned(), 0),
        |path| {
            std::fs::read(path).map_or_else(
                |_| ("unreadable".to_owned(), 0),
                |bytes| {
                    let mut hasher = sha2::Sha256::new();
                    hasher.update(&bytes);
                    let digest = hasher.finalize();
                    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
                    (hex, bytes.len())
                },
            )
        },
    );
    println!(
        "provenance: elf_sha256={sha} bytes={len} path={} profile=DIAGNOSTIC-ONLY",
        exe.as_deref()
            .map_or_else(|| "?".into(), |p| p.display().to_string())
    );
}

struct TimedArm {
    label: &'static str,
    nanos: Vec<f64>,
}

impl TimedArm {
    fn new(label: &'static str) -> Self {
        Self {
            label,
            nanos: Vec::new(),
        }
    }

    fn report(&mut self) {
        let med = median(&mut self.nanos);
        let n = self.nanos.len();
        let min = self.nanos.first().copied().unwrap_or(f64::NAN);
        let max = self.nanos.last().copied().unwrap_or(f64::NAN);
        println!(
            "arm={} samples={n} median_us={:.2} min_us={:.2} max_us={:.2}",
            self.label,
            med / 1_000.0,
            min / 1_000.0,
            max / 1_000.0
        );
    }
}

fn setup_probe_schema(conn: &fsqlite::Connection) {
    fsqlite_e2e::block_on(async {
        conn.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, category_id INTEGER NOT NULL)",
        )
        .await
        .expect("create products");
        conn.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .expect("create categories");
        conn.execute("CREATE INDEX idx_prod_cat ON products(category_id)")
            .await
            .expect("create index");
        conn.execute("BEGIN").await.expect("begin populate");
        let cat = conn
            .prepare("INSERT INTO categories VALUES (?1, ('cat_' || ?1))")
            .await
            .expect("prepare categories insert");
        for i in 1..=CATEGORY_COUNT {
            cat.execute_with_params(&[fsqlite::SqliteValue::Integer(i)])
                .await
                .expect("insert category");
        }
        let prod = conn
            .prepare("INSERT INTO products VALUES (?1, ((?1 % 500) + 1))")
            .await
            .expect("prepare products insert");
        for i in 1..=PRODUCT_COUNT {
            prod.execute_with_params(&[fsqlite::SqliteValue::Integer(i)])
                .await
                .expect("insert product");
        }
        conn.execute("COMMIT").await.expect("commit populate");
    });
}

/// Run one timed sample: a single runtime entry around one execution.
fn time_query_row(stmt: &fsqlite::PreparedStatement<'_>, with_params: bool) -> (Duration, i64) {
    let start = Instant::now();
    let row = if with_params {
        fsqlite_e2e::block_on(stmt.query_row_with_params(&[])).expect("query_row_with_params")
    } else {
        fsqlite_e2e::block_on(stmt.query_row()).expect("query_row")
    };
    let elapsed = start.elapsed();
    let value = match row.get(0) {
        Some(fsqlite::SqliteValue::Integer(v)) => *v,
        other => panic!("expected integer result, got {other:?}"),
    };
    (elapsed, value)
}

#[allow(clippy::cast_precision_loss)]
fn run_control(
    name: &str,
    conn: &fsqlite::Connection,
    sql_for_cluster: impl Fn(usize) -> (String, i64),
    arm_b_is_query_row: bool,
) {
    let mut arm_a = TimedArm::new("query_row");
    let mut arm_b = TimedArm::new(if arm_b_is_query_row {
        "query_row_again"
    } else {
        "query_row_with_params_empty"
    });

    for cluster in 0..CLUSTERS {
        let (sql, expected) = sql_for_cluster(cluster);
        let stmt = fsqlite_e2e::block_on(conn.prepare(&sql)).expect("prepare control statement");
        // Warm both arms once so first-touch page faults and memo priming
        // land outside the timed samples, symmetrically.
        let _ = time_query_row(&stmt, false);
        let _ = time_query_row(&stmt, !arm_b_is_query_row);

        // ABBA within the cluster: A first on even clusters, B first on odd.
        for s in 0..SAMPLES_PER_ARM_PER_CLUSTER {
            let a_first = (cluster + s) % 2 == 0;
            if a_first {
                let (d, v) = time_query_row(&stmt, false);
                assert_eq!(v, expected, "{name}: arm A wrong result");
                arm_a.nanos.push(d.as_nanos() as f64);
                let (d, v) = time_query_row(&stmt, !arm_b_is_query_row);
                assert_eq!(v, expected, "{name}: arm B wrong result");
                arm_b.nanos.push(d.as_nanos() as f64);
            } else {
                let (d, v) = time_query_row(&stmt, !arm_b_is_query_row);
                assert_eq!(v, expected, "{name}: arm B wrong result");
                arm_b.nanos.push(d.as_nanos() as f64);
                let (d, v) = time_query_row(&stmt, false);
                assert_eq!(v, expected, "{name}: arm A wrong result");
                arm_a.nanos.push(d.as_nanos() as f64);
            }
        }
    }

    println!("== {name} ==");
    arm_a.report();
    arm_b.report();
    let med_a = median(&mut arm_a.nanos);
    let med_b = median(&mut arm_b.nanos);
    println!(
        "delta_us={:.2} ratio_b_over_a={:.3}",
        (med_b - med_a) / 1_000.0,
        med_b / med_a
    );
}

fn main() {
    self_provenance();
    println!(
        "config: clusters={CLUSTERS} samples_per_arm_per_cluster={SAMPLES_PER_ARM_PER_CLUSTER} \
         products={PRODUCT_COUNT} categories={CATEGORY_COUNT}"
    );

    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).expect("open :memory:");
    setup_probe_schema(&conn);

    // Control 1: pure Option-gate on a placeholder-free expression statement.
    run_control(
        "control1_pure_option_gate (SELECT 40 + 2)",
        &conn,
        |_cluster| ("SELECT 40 + 2".to_owned(), 42),
        false,
    );

    // Control 2: literal count-probe shape; cycling literal per cluster for
    // memo symmetry. Expected count: products whose category_id has a
    // category with id <= K. category_id = (id % 500) + 1 spans 1..=500
    // uniformly: 20 products per category.
    run_control(
        "control2_literal_fast_lane_gate (COUNT EXISTS probe)",
        &conn,
        |cluster| {
            let k = i64::try_from(cluster % 10).unwrap() + 1;
            (
                format!(
                    "SELECT COUNT(*) FROM products p WHERE EXISTS \
                     (SELECT 1 FROM categories c WHERE c.id = p.category_id AND c.id <= {k})"
                ),
                k * 20,
            )
        },
        false,
    );

    // A/A null on the control-2 shape: bounds host noise for this run.
    run_control(
        "aa_null (control-2 shape, query_row both arms)",
        &conn,
        |cluster| {
            let k = i64::try_from(cluster % 10).unwrap() + 1;
            (
                format!(
                    "SELECT COUNT(*) FROM products p WHERE EXISTS \
                     (SELECT 1 FROM categories c WHERE c.id = p.category_id AND c.id <= {k})"
                ),
                k * 20,
            )
        },
        true,
    );

    // Control 3: literal vs placeholder, SAME semantic query per cluster.
    // The literal statement gets a prepared_query_fast_path; the placeholder
    // one gets None and takes the generic pager+VDBE lane. This is the
    // decisive fast-path-presence arm.
    control3_literal_vs_placeholder(&conn);

    println!(
        "interpretation: control1 = Option-gate cost; control2 = params plumbing \
         on a fast-pathed statement; control3 = fast-path presence (literal) vs \
         absence (placeholder) on the same query. If control3's delta dominates \
         (and controls 1-2 plus the A/A spread are ~zero), the ~50-60us gap is \
         the missing prepared_query_fast_path on placeholder statements, not \
         parameter passing (bd-5zeai)."
    );
}

#[allow(clippy::cast_precision_loss)]
fn control3_literal_vs_placeholder(conn: &fsqlite::Connection) {
    let mut arm_literal = TimedArm::new("literal_bound_fast_path");
    let mut arm_placeholder = TimedArm::new("placeholder_bound_no_fast_path");

    let placeholder_sql = "SELECT COUNT(*) FROM products p WHERE EXISTS \
         (SELECT 1 FROM categories c WHERE c.id = p.category_id AND c.id <= ?1)";
    let placeholder_stmt =
        fsqlite_e2e::block_on(conn.prepare(placeholder_sql)).expect("prepare placeholder stmt");

    for cluster in 0..CLUSTERS {
        let k = i64::try_from(cluster % 10).unwrap() + 1;
        let expected = k * 20;
        let literal_sql = format!(
            "SELECT COUNT(*) FROM products p WHERE EXISTS \
             (SELECT 1 FROM categories c WHERE c.id = p.category_id AND c.id <= {k})"
        );
        let literal_stmt =
            fsqlite_e2e::block_on(conn.prepare(&literal_sql)).expect("prepare literal stmt");
        let params = [fsqlite::SqliteValue::Integer(k)];

        // Symmetric warm-up.
        let _ = time_query_row(&literal_stmt, false);
        let _ =
            fsqlite_e2e::block_on(placeholder_stmt.query_row_with_params(&params)).expect("warm");

        for s in 0..SAMPLES_PER_ARM_PER_CLUSTER {
            let literal_first = (cluster + s) % 2 == 0;
            let run_literal = |arm: &mut TimedArm| {
                let (d, v) = time_query_row(&literal_stmt, false);
                assert_eq!(v, expected, "control3: literal arm wrong result");
                arm.nanos.push(d.as_nanos() as f64);
            };
            let run_placeholder = |arm: &mut TimedArm| {
                let start = Instant::now();
                let row = fsqlite_e2e::block_on(placeholder_stmt.query_row_with_params(&params))
                    .expect("placeholder query");
                let elapsed = start.elapsed();
                match row.get(0) {
                    Some(fsqlite::SqliteValue::Integer(v)) => {
                        assert_eq!(*v, expected, "control3: placeholder arm wrong result");
                    }
                    other => panic!("control3: expected integer, got {other:?}"),
                }
                arm.nanos.push(elapsed.as_nanos() as f64);
            };
            if literal_first {
                run_literal(&mut arm_literal);
                run_placeholder(&mut arm_placeholder);
            } else {
                run_placeholder(&mut arm_placeholder);
                run_literal(&mut arm_literal);
            }
        }
    }

    println!("== control3_fast_path_presence (literal vs placeholder, same query) ==");
    arm_literal.report();
    arm_placeholder.report();
    let med_a = median(&mut arm_literal.nanos);
    let med_b = median(&mut arm_placeholder.nanos);
    println!(
        "delta_us={:.2} ratio_placeholder_over_literal={:.3}",
        (med_b - med_a) / 1_000.0,
        med_b / med_a
    );
}
