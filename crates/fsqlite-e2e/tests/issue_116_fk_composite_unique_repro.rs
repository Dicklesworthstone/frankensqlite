//! Issue #116 reproduction: FK-on prepared INSERTs are super-linear
//! (near-quadratic) when the child table has a composite
//! `UNIQUE(parent_id, seq)` constraint.
//!
//! This is an INVESTIGATION repro (no engine source modified). It runs the
//! explicit-rowid child shape at N=500/1000/2000 in a 2x2 matrix:
//!   (UNIQUE on/off) x (FK on/off)
//! and prints elapsed wall time + per-row time so the scaling can be read off.
//!
//! Run:
//!   rch exec -- cargo test -p fsqlite-e2e \
//!     --test issue_116_fk_composite_unique_repro -- --nocapture
#![recursion_limit = "512"]

use std::time::Instant;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

const PARENT_DDL: &str = "CREATE TABLE parent (id TEXT PRIMARY KEY NOT NULL, kind TEXT NOT NULL, item_count INTEGER NOT NULL)";

const ITEM_DDL_WITH_UNIQUE: &str = "CREATE TABLE item (
  id INTEGER PRIMARY KEY,
  parent_id TEXT NOT NULL REFERENCES parent(id) ON DELETE CASCADE,
  seq INTEGER NOT NULL,
  v1 REAL, v2 REAL, v3 REAL, v4 REAL, v5 REAL, v6 REAL, v7 REAL, v8 REAL,
  extra TEXT NOT NULL DEFAULT 'null',
  orig_seq INTEGER,
  UNIQUE(parent_id, seq)
)";

const ITEM_DDL_NO_UNIQUE: &str = "CREATE TABLE item (
  id INTEGER PRIMARY KEY,
  parent_id TEXT NOT NULL REFERENCES parent(id) ON DELETE CASCADE,
  seq INTEGER NOT NULL,
  v1 REAL, v2 REAL, v3 REAL, v4 REAL, v5 REAL, v6 REAL, v7 REAL, v8 REAL,
  extra TEXT NOT NULL DEFAULT 'null',
  orig_seq INTEGER
)";

const INSERT_SQL: &str = "INSERT INTO item (id,parent_id,seq,v1,v2,v3,v4,v5,v6,v7,v8,extra,orig_seq) \
     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)";

async fn run_shape(n: i64, fk_on: bool, composite_unique: bool) -> f64 {
    let conn = Connection::open(":memory:").await.unwrap();
    if fk_on {
        conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
    } else {
        conn.execute("PRAGMA foreign_keys=OFF").await.unwrap();
    }
    conn.execute(PARENT_DDL).await.unwrap();
    let item_ddl = if composite_unique {
        ITEM_DDL_WITH_UNIQUE
    } else {
        ITEM_DDL_NO_UNIQUE
    };
    conn.execute(item_ddl).await.unwrap();

    // One parent that all children reference.
    conn.execute_with_params(
        "INSERT INTO parent (id,kind,item_count) VALUES (?1,?2,?3)",
        &[
            SqliteValue::Text("p1".into()),
            SqliteValue::Text("generic".into()),
            SqliteValue::Integer(n),
        ],
    )
    .await
    .unwrap();

    conn.execute("BEGIN").await.unwrap();
    let stmt = conn.prepare(INSERT_SQL).await.unwrap();
    let start = Instant::now();
    for i in 0..n {
        let params = [
            SqliteValue::Integer(i + 1),    // id (explicit rowid)
            SqliteValue::Text("p1".into()), // parent_id
            SqliteValue::Integer(i),        // seq (unique per row)
            SqliteValue::Float(1.0),
            SqliteValue::Float(2.0),
            SqliteValue::Float(3.0),
            SqliteValue::Float(4.0),
            SqliteValue::Float(5.0),
            SqliteValue::Float(6.0),
            SqliteValue::Float(7.0),
            SqliteValue::Float(8.0),
            SqliteValue::Text("x".into()), // extra
            SqliteValue::Integer(i),       // orig_seq
        ];
        conn.execute_prepared_with_params(&stmt, &params)
            .await
            .unwrap();
    }
    let elapsed = start.elapsed();
    drop(stmt);
    conn.execute("COMMIT").await.unwrap();

    let rows = conn.query("SELECT COUNT(*) FROM item").await.unwrap();
    assert_eq!(rows[0].values()[0], SqliteValue::Integer(n));

    let secs = elapsed.as_secs_f64();
    println!(
        "  N={n:>5}  fk={:<3}  unique={:<3}  elapsed={secs:>9.4}s  per_row={:>8.2}us",
        if fk_on { "ON" } else { "off" },
        if composite_unique { "ON" } else { "off" },
        secs * 1_000_000.0 / n as f64,
    );
    secs
}

#[test]
fn issue_116_quadratic_scaling_repro() {
    asupersync::test_utils::run_test(|| async {
        println!("\n=== Issue #116: FK + composite UNIQUE quadratic INSERT repro ===\n");

        let sizes = [500_i64, 1000, 2000];

        println!("--- FK ON, composite UNIQUE ON (the bug shape) ---");
        let mut fk_unique = Vec::new();
        for &n in &sizes {
            fk_unique.push((n, run_shape(n, true, true).await));
        }

        println!("\n--- 2x2 isolation at N=2000 ---");
        let iso_n = 2000_i64;
        let t_fk_uniq = run_shape(iso_n, true, true).await;
        let t_fk_nouniq = run_shape(iso_n, true, false).await;
        let t_nofk_uniq = run_shape(iso_n, false, true).await;
        let t_nofk_nouniq = run_shape(iso_n, false, false).await;

        println!("\n=== Summary ===");
        println!("Bug-shape (FK ON + UNIQUE ON) scaling:");
        for w in fk_unique.windows(2) {
            let (n0, t0) = w[0];
            let (n1, t1) = w[1];
            let size_ratio = n1 as f64 / n0 as f64;
            let time_ratio = t1 / t0;
            println!(
                "  N {n0} -> {n1} (x{size_ratio:.1}): time x{time_ratio:.2}  (linear=x{size_ratio:.1}, quadratic=x{:.1})",
                size_ratio * size_ratio
            );
        }
        println!("\n2x2 at N={iso_n}:");
        println!("  FK ON  + UNIQUE ON  : {t_fk_uniq:.4}s");
        println!("  FK ON  + UNIQUE off : {t_fk_nouniq:.4}s");
        println!("  FK off + UNIQUE ON  : {t_nofk_uniq:.4}s");
        println!("  FK off + UNIQUE off : {t_nofk_nouniq:.4}s");
        println!(
            "  => FK-on+UNIQUE-on is {:.1}x slower than FK-off+UNIQUE-off",
            t_fk_uniq / t_nofk_nouniq.max(1e-9)
        );
        println!(
            "  => FK-on+UNIQUE-on is {:.1}x slower than FK-on+UNIQUE-off (isolates composite UNIQUE)",
            t_fk_uniq / t_fk_nouniq.max(1e-9)
        );
        println!(
            "  => FK-on+UNIQUE-on is {:.1}x slower than FK-off+UNIQUE-on (isolates FK)",
            t_fk_uniq / t_nofk_uniq.max(1e-9)
        );

        // ── Regression gate ──
        // The fix makes the FK-enforced INSERT loop linear again. Before it, this
        // shape was near-quadratic (per-row time grew ~3x per doubling) and
        // FK-on+UNIQUE-on was ~60x slower than FK-off+UNIQUE-on. These bounds are
        // deliberately loose so they hold on a contended host while still catching
        // a return of the super-linear cliff.
        let (n_lo, t_lo) = fk_unique[0];
        let (n_hi, t_hi) = *fk_unique.last().unwrap();
        let size_ratio = n_hi as f64 / n_lo as f64; // 4.0 for 500 -> 2000
        let time_ratio = t_hi / t_lo.max(1e-9);
        assert!(
            time_ratio < 2.0 * size_ratio,
            "FK+UNIQUE INSERT scaling is super-linear: N x{size_ratio:.1} gave time x{time_ratio:.2} \
             (quadratic would be x{:.1}); the GH#116 cliff appears to have returned",
            size_ratio * size_ratio,
        );
        let fk_vs_nofk = t_fk_uniq / t_nofk_uniq.max(1e-9);
        assert!(
            fk_vs_nofk < 8.0,
            "FK-on+UNIQUE-on is {fk_vs_nofk:.1}x slower than FK-off+UNIQUE-on (was ~63x before the \
             GH#116 fix); the per-row memdb reload appears to have returned",
        );
    });
}
