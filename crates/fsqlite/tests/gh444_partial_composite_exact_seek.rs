//! GH#444: a zero-match equality seek on a partial index whose predicate the WHERE implies
//! (`k = 'v'` implies `k IS NOT NULL`), or on the leading column of a composite index, finalizes
//! directly instead of falling back to a full table scan. A trigger WHEN guard that probes such an
//! index on every row turned bulk UPDATE quadratic.
//!
//! HARD GATE: results byte-identical to C SQLite (rusqlite) for absent and present keys, including
//! the shapes that must KEEP the fallback (affinity conversion, NOCASE, typeless column, predicate
//! not implied). The EXPLAIN checks pin which shapes lost the fallback scan and which kept it.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(_) => "blob".into(),
    }
}

async fn fr(c: &Connection, s: &str) -> Vec<Vec<String>> {
    c.query(s)
        .await
        .unwrap_or_else(|e| panic!("frank `{s}`: {e}"))
        .iter()
        .map(|r| r.values().iter().map(render).collect())
        .collect()
}

fn sq(c: &rusqlite::Connection, s: &str) -> Vec<Vec<String>> {
    let mut st = c.prepare(s).unwrap();
    let n = st.column_count();
    st.query_map([], |row| {
        let mut o = Vec::new();
        for i in 0..n {
            o.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(_) => "blob".to_owned(),
            });
        }
        Ok(o)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// Whether control flow from the program entry can reach a full-table `Rewind`. An exact seek still
/// emits the fallback block, but no path leads into it.
async fn has_reachable_rewind(c: &Connection, s: &str) -> bool {
    // Opcodes whose P2 is a jump target. Every other opcode only falls through.
    const JUMPS: &[&str] = &[
        "Init",
        "Goto",
        "If",
        "IfNot",
        "IsNull",
        "NotNull",
        "SeekGE",
        "SeekGT",
        "SeekLE",
        "SeekLT",
        "IdxGT",
        "IdxGE",
        "IdxLT",
        "IdxLE",
        "Eq",
        "Ne",
        "Lt",
        "Le",
        "Gt",
        "Ge",
        "Next",
        "Prev",
        "DecrJumpZero",
        "IfPos",
        "SeekRowid",
        "NotExists",
        "Found",
        "NotFound",
        "NoConflict",
        "Rewind",
        "Last",
    ];
    let program: Vec<(String, Option<usize>)> = c
        .query(&format!("EXPLAIN {s}"))
        .await
        .unwrap_or_else(|e| panic!("explain `{s}`: {e}"))
        .iter()
        .map(|row| {
            let v = row.values();
            let op = render(&v[1]).trim_matches('\'').to_owned();
            let p2 = match &v[3] {
                SqliteValue::Integer(n) if JUMPS.contains(&op.as_str()) => usize::try_from(*n).ok(),
                _ => None,
            };
            (op, p2)
        })
        .collect();
    let mut seen = vec![false; program.len()];
    let mut work = vec![0_usize];
    while let Some(addr) = work.pop() {
        if addr >= program.len() || std::mem::replace(&mut seen[addr], true) {
            continue;
        }
        let (op, p2) = &program[addr];
        if op == "Rewind" {
            return true;
        }
        if op != "Goto" && op != "Halt" {
            work.push(addr + 1);
        }
        work.extend(*p2);
    }
    false
}

const SCHEMA: &[&str] = &[
    // The issue's shape: a partial UNIQUE index whose predicate `k = <lit>` implies.
    "CREATE TABLE t (id TEXT PRIMARY KEY, k TEXT);",
    "CREATE UNIQUE INDEX t_k ON t (k) WHERE k IS NOT NULL;",
    // Composite index probed on its leading column; `a` holds integer-looking text too.
    "CREATE TABLE c (id INTEGER PRIMARY KEY, a TEXT, b TEXT, v);",
    "CREATE INDEX c_ab ON c (a, b);",
    // Partial composite UNIQUE index.
    "CREATE TABLE p (id TEXT PRIMARY KEY, a TEXT, k TEXT);",
    "CREATE UNIQUE INDEX p_ak ON p (a, k) WHERE k IS NOT NULL;",
    // Must keep the fallback: NOCASE column, typeless column, predicate the probe does not imply.
    "CREATE TABLE n (id INTEGER PRIMARY KEY, k TEXT COLLATE NOCASE);",
    "CREATE INDEX n_k ON n (k) WHERE k IS NOT NULL;",
    "CREATE TABLE u (id INTEGER PRIMARY KEY, k);",
    "CREATE INDEX u_k ON u (k) WHERE k IS NOT NULL;",
    "CREATE TABLE g (id INTEGER PRIMARY KEY, k TEXT, flag INTEGER);",
    "CREATE INDEX g_k ON g (k) WHERE flag = 1;",
];

const ROWS: &[&str] = &[
    "INSERT INTO t VALUES ('a', 'v1'), ('b', 'v2'), ('c', NULL), ('d', '7'), ('e', 8);",
    "INSERT INTO c VALUES (1, 'A', 'x', 10), (2, 'A', 'y', 20), (3, 'B', 'x', 30), \
     (4, '1', 'x', 40), (5, 1, 'z', 50), (6, NULL, 'x', 60);",
    "INSERT INTO p VALUES ('a', 'A', 'k1'), ('b', 'A', NULL), ('c', 'B', 'k1'), ('d', '2', 'k2');",
    "INSERT INTO n VALUES (1, 'abc'), (2, 'ABD'), (3, NULL);",
    "INSERT INTO u VALUES (1, 1), (2, '1'), (3, 2.0), (4, 'x'), (5, NULL);",
    "INSERT INTO g VALUES (1, 'a', 1), (2, 'b', 0), (3, 'c', NULL);",
];

#[test]
fn gh444_partial_and_composite_seeks_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in SCHEMA.iter().chain(ROWS) {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        for s in [
            // Partial index, implied predicate: absent / present / NULL-key rows.
            "SELECT 1 FROM t WHERE id IS NOT 'x' AND k = 'absent' LIMIT 1",
            "SELECT id FROM t WHERE id IS NOT 'x' AND k = 'v1'",
            "SELECT id FROM t WHERE id IS NOT 'a' AND k = 'v1'",
            "SELECT id FROM t WHERE k = 'v2' AND id IS NOT 'zz'",
            "SELECT EXISTS(SELECT 1 FROM t WHERE id IS NOT 'b' AND k = 'v2')",
            "SELECT id FROM t WHERE k = '7' AND id IS NOT 'q' ORDER BY id",
            // Integer probe into the TEXT column: affinity conversion, keeps the fallback.
            "SELECT id FROM t WHERE k = 7 AND id IS NOT 'q' ORDER BY id",
            "SELECT id FROM t WHERE k = 8 AND id IS NOT 'q' ORDER BY id",
            "SELECT id FROM t WHERE k = '8' AND id IS NOT 'q' ORDER BY id",
            "SELECT id FROM t WHERE k IS NULL AND id IS NOT 'q'",
            // Composite leading column, with and without a residual.
            "SELECT id FROM c WHERE a = 'Z' AND v > 0",
            "SELECT id FROM c WHERE a = 'A' AND v > 15",
            "SELECT 1 FROM c WHERE a = 'Q' AND id IS NOT 3 LIMIT 1",
            "SELECT id FROM c WHERE a = '1' AND v > 0 ORDER BY id",
            "SELECT id FROM c WHERE a = 1 AND v > 0 ORDER BY id",
            "SELECT count(*) FROM c WHERE a = 'A' AND b = 'y'",
            "SELECT count(*) FROM c WHERE a = 'A' AND b = 'nope'",
            "SELECT count(*), sum(v) FROM c WHERE a = 'nope' AND b = 'x'",
            "SELECT count(*) FROM c WHERE a = '1' AND b = 'x'",
            "SELECT count(*) FROM c WHERE a = 1 AND b = 'z'",
            // Partial composite.
            "SELECT 1 FROM p WHERE a = 'A' AND k = 'nope' AND id IS NOT 'a' LIMIT 1",
            "SELECT id FROM p WHERE a = 'A' AND k = 'k1' AND id IS NOT 'zz'",
            "SELECT id FROM p WHERE a = 'Q' AND k = 'k1' AND id IS NOT 'zz'",
            "SELECT id FROM p WHERE a = '2' AND k = 'k2' AND id IS NOT 'zz'",
            // Keep-the-fallback shapes.
            "SELECT id FROM n WHERE k = 'ABC' AND id > 0",
            "SELECT id FROM n WHERE k = 'abd' AND id > 0",
            "SELECT id FROM u WHERE k = '1' AND id > 0 ORDER BY id",
            "SELECT id FROM u WHERE k = 1 AND id > 0 ORDER BY id",
            "SELECT id FROM u WHERE k = 2 AND id > 0 ORDER BY id",
            "SELECT id FROM g WHERE k = 'b' AND id > 0",
            "SELECT id FROM g WHERE k = 'a' AND flag = 1 AND id > 0",
        ] {
            assert_eq!(fr(&f, s).await, sq(&r, s), "diverged: `{s}`");
        }
    });
}

#[test]
fn gh444_exact_shapes_drop_the_fallback_scan() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        for s in SCHEMA {
            f.execute(s).await.unwrap();
        }
        for s in [
            "SELECT 1 FROM t WHERE id IS NOT 'x' AND k = 'v' LIMIT 1",
            "SELECT 1 FROM c WHERE a = 'Q' AND id IS NOT 3 LIMIT 1",
            "SELECT count(*) FROM c WHERE a = 'A' AND b = 'y'",
            "SELECT 1 FROM p WHERE a = 'A' AND k = 'k1' AND id IS NOT 'x' LIMIT 1",
        ] {
            assert!(
                !has_reachable_rewind(&f, s).await,
                "exact seek still falls back to a scan: `{s}`"
            );
        }
        for s in [
            // An integer probe into a TEXT column implies a conversion.
            "SELECT 1 FROM t WHERE id IS NOT 'x' AND k = 7 LIMIT 1",
            // NOCASE: the index order is not the BINARY probe's order.
            "SELECT id FROM n WHERE k = 'ABC' AND id > 0",
        ] {
            assert!(
                has_reachable_rewind(&f, s).await,
                "non-exact seek lost its fallback: `{s}`"
            );
        }
    });
}

/// The issue's workload: a BEFORE UPDATE guard whose WHEN probes the partial UNIQUE index for
/// another row holding the new key. Results and the RAISE must match SQLite.
#[test]
fn gh444_trigger_guarded_update_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id TEXT PRIMARY KEY, k TEXT);",
            "CREATE UNIQUE INDEX t_k ON t (k) WHERE k IS NOT NULL;",
            "CREATE TRIGGER g BEFORE UPDATE ON t WHEN EXISTS \
             (SELECT 1 FROM t WHERE id IS NOT NEW.id AND k = NEW.k) \
             BEGIN SELECT RAISE(ABORT, 'dup'); END;",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        for i in 1..=300 {
            let k = if i % 10 == 0 {
                "NULL".to_owned()
            } else {
                format!("'k{i}'")
            };
            let s = format!("INSERT INTO t VALUES ('id{i}', {k});");
            f.execute(&s).await.unwrap();
            r.execute_batch(&s).unwrap();
        }
        let update = "UPDATE t SET k = k || '_x'";
        f.execute(update).await.unwrap();
        r.execute_batch(update).unwrap();
        let check = "SELECT id, k FROM t ORDER BY id";
        assert_eq!(fr(&f, check).await, sq(&r, check));

        let dup = "UPDATE t SET k = 'k2_x' WHERE id = 'id3'";
        let frank_err = f
            .execute(dup)
            .await
            .expect_err("guard must raise")
            .to_string();
        let stock_err = r
            .execute_batch(dup)
            .expect_err("guard must raise")
            .to_string();
        assert!(frank_err.contains("dup"), "fsqlite: {frank_err}");
        assert!(stock_err.contains("dup"), "sqlite: {stock_err}");
        assert_eq!(fr(&f, check).await, sq(&r, check));
    });
}
