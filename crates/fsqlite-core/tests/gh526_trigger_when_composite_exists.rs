#![recursion_limit = "512"]

//! GH#526: correlated composite EXISTS guards should keep their selective seek
//! in both WHEN and trigger-body forms. The normal tests compare SQL results
//! with stock SQLite and bound executed instructions instead of wall time.
//! The ignored benchmark reproduces the issue's file-backed workload with a
//! live stock control in the same invocation.

use std::time::Instant;

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL, c1 TEXT NOT NULL, c2 TEXT NOT NULL,
 c3 TEXT NOT NULL, c4 TEXT NOT NULL, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT NOT NULL,
 c9 TEXT, c10 TEXT, c11 INTEGER, c12 TEXT, c13 TEXT, c14 TEXT, c15 INTEGER NOT NULL);
CREATE INDEX x1 ON t (c3, c4, c8 DESC, c1);
CREATE INDEX x2 ON t (c1, c8 DESC);
CREATE INDEX x3 ON t (c2, c8 DESC);
CREATE INDEX x4 ON t (c5);
CREATE UNIQUE INDEX x5 ON t (c14) WHERE c14 IS NOT NULL;
CREATE INDEX x6 ON t (c9, c8 DESC);
CREATE INDEX x7 ON t (c10);
CREATE INDEX x8 ON t (c15);
CREATE INDEX x9 ON t (c6);
CREATE UNIQUE INDEX x10 ON t (c1, c2, c3, c4, c5, COALESCE(c6, ''), c8, c7,
 c9, c10, COALESCE(c11, -1), COALESCE(c12, ''), COALESCE(c13, ''))
 WHERE c1 IS NOT NULL AND c2 IS NOT NULL;";

const GUARDS: &str = "NEW.c1 IS NOT NULL AND NEW.c2 IS NOT NULL";
const EXISTS_SELECT: &str = "\
SELECT 1 FROM t AS v WHERE v.id IS NOT OLD.id AND v.c14 IS NOT NULL
 AND v.c1 = NEW.c1 AND v.c2 = NEW.c2 AND v.c3 = NEW.c3 AND v.c4 = NEW.c4
 AND v.c5 IS NEW.c5 AND v.c6 IS NEW.c6 AND v.c7 IS NEW.c7 AND v.c8 = NEW.c8
 AND v.c9 IS NEW.c9 AND v.c10 IS NEW.c10 AND v.c11 IS NEW.c11
 AND v.c12 IS NEW.c12 AND v.c13 IS NEW.c13
 ORDER BY v.c3, v.c4, v.c8 DESC LIMIT 1";
const INSERT: &str = "INSERT INTO t VALUES \
    (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)";
const UPDATE: &str = "UPDATE t SET c15 = ?1 WHERE id = ?2";

// Hot-path profiling is process-global. Keep all tests in this binary serial
// with respect to those counters, including the ignored timing test.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[derive(Clone, Copy, Debug)]
enum Arm {
    None,
    When,
    Body,
}

impl Arm {
    fn trigger(self) -> Option<String> {
        match self {
            Self::None => None,
            Self::When => Some(format!(
                "CREATE TRIGGER trg BEFORE UPDATE ON t WHEN {GUARDS} AND EXISTS \
                 ({EXISTS_SELECT}) BEGIN SELECT RAISE(ABORT, 'identity exists'); END;"
            )),
            Self::Body => Some(format!(
                "CREATE TRIGGER trg BEFORE UPDATE ON t BEGIN SELECT RAISE(ABORT, \
                 'identity exists') WHERE {GUARDS} AND EXISTS ({EXISTS_SELECT}); END;"
            )),
        }
    }
}

fn text(value: impl Into<String>) -> SqliteValue {
    SqliteValue::Text(value.into().into())
}

fn generated_row(i: usize) -> Vec<SqliteValue> {
    vec![
        text(format!("id{i:08}")),
        text("s1"),
        text("p1"),
        text(if i.is_multiple_of(7) { "ns-a" } else { "ns-b" }),
        text(format!("tag{}", i % 800)),
        text(["u-a", "u-b", "u-c", "u-d"][(i / 800) % 4]),
        if i.is_multiple_of(2) {
            text(format!("20{:02}-01-01", 10 + (i / 3200) % 15))
        } else {
            SqliteValue::Null
        },
        if i.is_multiple_of(3) {
            text("x")
        } else {
            SqliteValue::Null
        },
        text(format!(
            "20{:02}-{:02}-{:02}",
            10 + (i / 3200) % 15,
            1 + (i / 200) % 12,
            1 + i % 28
        )),
        text(if i.is_multiple_of(4) {
            "form-a"
        } else {
            "form-b"
        }),
        text(format!("20{:02}-12-31", 10 + (i / 3200) % 15)),
        if i.is_multiple_of(5) {
            SqliteValue::Null
        } else {
            SqliteValue::Integer(2010 + (i / 3200) as i64 % 15)
        },
        text("fp"),
        if i.is_multiple_of(6) {
            text("fr")
        } else {
            SqliteValue::Null
        },
        text(format!("cf{i:08}")),
        SqliteValue::Integer(0),
    ]
}

fn stock_value(value: &SqliteValue) -> rusqlite::types::Value {
    match value {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(value) => rusqlite::types::Value::Integer(*value),
        SqliteValue::Float(value) => rusqlite::types::Value::Real(*value),
        SqliteValue::Text(value) => rusqlite::types::Value::Text(value.to_string()),
        SqliteValue::Blob(value) => rusqlite::types::Value::Blob(value.to_vec()),
    }
}

struct Fixture {
    conn: Connection,
    stock: rusqlite::Connection,
    _dir: tempfile::TempDir,
}

impl Fixture {
    async fn open() -> Self {
        let dir = tempfile::tempdir().expect("fixture directory");
        let conn = Connection::open(dir.path().join("franken.db").to_str().expect("path"))
            .await
            .expect("FrankenSQLite open");
        let stock = rusqlite::Connection::open(dir.path().join("stock.db")).expect("stock open");
        Self {
            conn,
            stock,
            _dir: dir,
        }
    }

    async fn new(n: usize, arm: Arm, twins: bool) -> Self {
        let fixture = Self::open().await;
        fixture.execute_both(SCHEMA).await;
        let mut rows: Vec<_> = (0..n).map(generated_row).collect();
        if twins {
            // Matches come after all unrelated rows. c7 is NULL in these
            // identities, so the report's unique expression index admits them.
            for i in [1, 5] {
                let mut twin = generated_row(i);
                twin[0] = text(format!("twin{i}"));
                twin[14] = text(format!("twin-cf{i}"));
                rows.push(twin);
            }
        }
        fixture
            .conn
            .execute("BEGIN IMMEDIATE")
            .await
            .expect("begin seed");
        assert_eq!(
            fixture
                .conn
                .execute_many_with_params_skip_statement_savepoint_in_explicit_txn(INSERT, &rows)
                .await
                .expect("FrankenSQLite seed"),
            rows.len()
        );
        fixture.conn.execute("COMMIT").await.expect("commit seed");
        fixture
            .stock
            .execute_batch("BEGIN IMMEDIATE")
            .expect("stock begin seed");
        {
            let mut insert = fixture.stock.prepare(INSERT).expect("stock insert");
            for row in &rows {
                assert_eq!(
                    insert
                        .execute(rusqlite::params_from_iter(row.iter().map(stock_value)))
                        .expect("stock seed"),
                    1
                );
            }
        }
        fixture
            .stock
            .execute_batch("COMMIT")
            .expect("stock commit seed");
        if let Some(trigger) = arm.trigger() {
            fixture.execute_both(&trigger).await;
        }
        fixture
    }

    async fn execute_both(&self, sql: &str) {
        self.stock
            .execute_batch(sql)
            .unwrap_or_else(|error| panic!("stock {sql}: {error}"));
        self.conn
            .execute_batch(sql)
            .await
            .unwrap_or_else(|error| panic!("FrankenSQLite {sql}: {error}"));
    }

    async fn assert_query(&self, sql: &str) {
        assert_bound_query(&self.conn, &self.stock, sql, &[]).await;
    }

    async fn assert_aborts(&self, sql: &str) {
        let expected = self
            .stock
            .execute_batch(sql)
            .expect_err("stock rejects identity");
        let actual = self
            .conn
            .execute_batch(sql)
            .await
            .expect_err("FrankenSQLite rejects identity");
        assert!(
            expected.to_string().contains("identity exists"),
            "{sql}: {expected}"
        );
        assert!(
            actual.to_string().contains("identity exists"),
            "{sql}: {actual}"
        );
    }
}

async fn assert_bound_query(
    conn: &Connection,
    stock: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) {
    let mut statement = stock.prepare(sql).expect("stock query prepare");
    let columns = statement.column_count();
    let expected = statement
        .query_map(
            rusqlite::params_from_iter(params.iter().map(stock_value)),
            |row| {
                (0..columns)
                    .map(|i| row.get::<_, rusqlite::types::Value>(i))
                    .collect::<Result<Vec<_>, _>>()
            },
        )
        .expect("stock query")
        .collect::<Result<Vec<_>, _>>()
        .expect("stock rows");
    let actual = conn
        .query_with_params(sql, params)
        .await
        .unwrap_or_else(|error| panic!("FrankenSQLite {sql}; {params:?}: {error}"));
    let actual: Vec<Vec<_>> = actual
        .iter()
        .map(|row| row.values().iter().map(stock_value).collect())
        .collect();
    assert_eq!(actual, expected, "{sql}; params={params:?}");
}

struct ProfileGuard;

impl ProfileGuard {
    fn enable() -> Self {
        set_hot_path_profile_enabled(true);
        Self
    }
}

impl Drop for ProfileGuard {
    fn drop(&mut self) {
        set_hot_path_profile_enabled(false);
    }
}

#[test]
fn composite_trigger_when_probes_match_stock_and_do_bounded_work() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    asupersync::test_utils::run_test(|| async {
        for arm in [Arm::When, Arm::Body] {
            for n in [128, 2048] {
                let fixture = Fixture::new(n, arm, true).await;
                fixture.execute_both("BEGIN IMMEDIATE").await;
                for (i, matches) in [(0, false), (1, true), (2, false), (5, true)] {
                    let params = [SqliteValue::Integer(17), text(format!("id{i:08}"))];
                    let expected = fixture.stock.execute(
                        UPDATE,
                        rusqlite::params_from_iter(params.iter().map(stock_value)),
                    );
                    let guard = ProfileGuard::enable();
                    let before = hot_path_profile_snapshot().vdbe.opcodes_executed_total;
                    let hydration_before = fixture.conn.memdb_row_hydration_count();
                    let actual = fixture.conn.execute_with_params(UPDATE, &params).await;
                    let ops = hot_path_profile_snapshot().vdbe.opcodes_executed_total - before;
                    let hydrated = fixture.conn.memdb_row_hydration_count() - hydration_before;
                    drop(guard);
                    if matches {
                        assert!(
                            expected
                                .expect_err("stock rejects twin")
                                .to_string()
                                .contains("identity exists")
                        );
                        assert!(
                            actual
                                .expect_err("FrankenSQLite rejects twin")
                                .to_string()
                                .contains("identity exists")
                        );
                    } else {
                        assert_eq!(expected.expect("stock accepts unique identity"), 1);
                        assert_eq!(actual.expect("FrankenSQLite accepts unique identity"), 1);
                    }
                    eprintln!(
                        "GH526_WORK arm={arm:?} rows={n} id={i} matches={matches} opcodes={ops} hydrated={hydrated}"
                    );
                    assert!(
                        (1..4096).contains(&ops),
                        "GH#526 {arm:?} rows={n} matching={matches}: {ops} opcodes indicate a broad scan"
                    );
                    assert_eq!(
                        hydrated, 0,
                        "GH#526 must not hydrate table rows to evaluate an indexed guard"
                    );
                }
                fixture.assert_query("SELECT id,c15 FROM t WHERE id IN ('id00000000','id00000001','id00000002','id00000005') ORDER BY id").await;
                fixture.execute_both("ROLLBACK").await;
                fixture
                    .assert_query("SELECT count(*),sum(c15) FROM t")
                    .await;
                fixture.conn.close().await.expect("close");
            }
        }
    });
}

#[test]
fn composite_trigger_when_keeps_old_new_ordering_and_rollback() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    asupersync::test_utils::run_test(|| async {
        for arm in [Arm::When, Arm::Body] {
            let fixture = Fixture::new(32, Arm::None, true).await;
            fixture.execute_both(
                "UPDATE t SET c5=NULL,c6=NULL,c7=NULL,c9=NULL,c10=NULL,c11=NULL,c12=NULL,c13=NULL
                 WHERE id IN ('id00000001','twin1')",
            ).await;
            fixture.execute_both(&arm.trigger().expect("guard")).await;
            // These newer BEFORE triggers fire before the guard. ABORT must
            // undo their writes, and AFTER triggers must not run on that row.
            fixture
                .execute_both(
                    "CREATE TABLE audit(seq INTEGER PRIMARY KEY,phase TEXT,old_id TEXT,
                   new_id TEXT,old_value INTEGER,new_value INTEGER);
                 CREATE TRIGGER before_a BEFORE UPDATE ON t BEGIN
                   INSERT INTO audit(phase,old_id,new_id,old_value,new_value)
                   VALUES ('before-a',OLD.id,NEW.id,OLD.c15,NEW.c15); END;
                 CREATE TRIGGER before_b BEFORE UPDATE ON t BEGIN
                   INSERT INTO audit(phase,old_id,new_id,old_value,new_value)
                   VALUES ('before-b',OLD.id,NEW.id,OLD.c15,NEW.c15); END;
                 CREATE TRIGGER after_a AFTER UPDATE ON t BEGIN
                   INSERT INTO audit(phase,old_id,new_id,old_value,new_value)
                   VALUES ('after-a',OLD.id,NEW.id,OLD.c15,NEW.c15); END;
                 CREATE TRIGGER after_b AFTER UPDATE ON t BEGIN
                   INSERT INTO audit(phase,old_id,new_id,old_value,new_value)
                   VALUES ('after-b',OLD.id,NEW.id,OLD.c15,NEW.c15); END;",
                )
                .await;
            fixture.execute_both("BEGIN IMMEDIATE").await;
            fixture
                .execute_both("UPDATE t SET c15=1 WHERE id='id00000000'")
                .await;
            fixture
                .assert_aborts("UPDATE t SET c15=99 WHERE id='id00000001'")
                .await;
            fixture
                .assert_query("SELECT * FROM audit ORDER BY seq")
                .await;

            // Each IS residual must match two NULLs; a changed non-NULL value
            // must break that match. Restoring NULL reintroduces the twin.
            for column in ["c5", "c6", "c7", "c9", "c10", "c11", "c12", "c13"] {
                fixture.execute_both("SAVEPOINT nullable_case").await;
                let value = if column == "c11" { "2026" } else { "'changed'" };
                fixture
                    .execute_both(&format!(
                        "UPDATE t SET {column}={value} WHERE id='id00000001'"
                    ))
                    .await;
                fixture
                    .execute_both("UPDATE t SET c15=2 WHERE id='id00000001'")
                    .await;
                fixture
                    .assert_aborts(&format!("UPDATE t SET {column}=NULL WHERE id='id00000001'"))
                    .await;
                fixture
                    .assert_query(&format!(
                        "SELECT id,{column},c15 FROM t WHERE id='id00000001'"
                    ))
                    .await;
                fixture
                    .execute_both("ROLLBACK TO nullable_case; RELEASE nullable_case")
                    .await;
                fixture
                    .assert_query("SELECT * FROM audit ORDER BY seq")
                    .await;
            }

            fixture.execute_both("SAVEPOINT changed_new").await;
            fixture
                .execute_both("UPDATE t SET c4='changed-tag' WHERE id='id00000001'")
                .await;
            fixture
                .execute_both("UPDATE t SET c15=3 WHERE id='id00000001'")
                .await;
            fixture
                .assert_aborts("UPDATE t SET c4='tag1' WHERE id='id00000001'")
                .await;
            fixture
                .assert_query("SELECT id,c4,c15 FROM t WHERE id='id00000001'")
                .await;
            fixture
                .execute_both("ROLLBACK TO changed_new; RELEASE changed_new")
                .await;

            // During BEFORE UPDATE the table still contains OLD.id. Excluding
            // NEW.id would spuriously discover the row being renamed.
            fixture
                .execute_both("UPDATE t SET id='renamed-self' WHERE id='id00000000'")
                .await;
            // The earlier row's successful update and all audit writes must
            // roll back when the later row's matching twin aborts the statement.
            fixture
                .assert_aborts("UPDATE t SET c15=c15+100 WHERE id IN ('renamed-self','id00000005')")
                .await;
            fixture
                .assert_query("SELECT id,c15 FROM t ORDER BY id")
                .await;
            fixture
                .assert_query("SELECT * FROM audit ORDER BY seq")
                .await;
            fixture
                .execute_both("SAVEPOINT continued; UPDATE t SET c15=7 WHERE id='renamed-self'; ROLLBACK TO continued; RELEASE continued")
                .await;
            fixture
                .assert_query("SELECT id,c15 FROM t ORDER BY id")
                .await;
            fixture.execute_both("ROLLBACK").await;
            fixture
                .assert_query("SELECT id,c15 FROM t ORDER BY id")
                .await;
            fixture
                .assert_query("SELECT * FROM audit ORDER BY seq")
                .await;
            fixture
                .execute_both("UPDATE t SET c15=8 WHERE id='id00000002'; BEGIN IMMEDIATE; UPDATE t SET c15=9 WHERE id='id00000002'; COMMIT")
                .await;
            fixture
                .assert_query("SELECT id,c15 FROM t ORDER BY id")
                .await;
            fixture
                .assert_query("SELECT * FROM audit ORDER BY seq")
                .await;
            fixture.conn.close().await.expect("close");
        }
    });
}

#[test]
fn parameterized_composite_exists_preserves_affinity_and_null_semantics() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    asupersync::test_utils::run_test(|| async {
        let fixture = Fixture::open().await;
        fixture
            .execute_both(
                "CREATE TABLE probe(id INTEGER PRIMARY KEY,a TEXT,b NUMERIC,c BLOB);
             CREATE INDEX probe_abc ON probe(a,b,c);
             INSERT INTO probe VALUES (1,'1',1,X'78'),(2,'1.0',1.0,X'78'),
               (3,'01',2,'x'),(4,'word',3,NULL),(5,NULL,4,X'78'),
               (6,'none',NULL,NULL),(7,'word',4,1),(8,'word',4,1.0),
               (9,'space','not-a-number',X'00');",
            )
            .await;
        let blob = |bytes: &[u8]| SqliteValue::Blob(bytes.to_vec().into());
        let cases = vec![
            vec![SqliteValue::Integer(1), text("1"), blob(b"x")],
            vec![SqliteValue::Float(1.0), text("1.0"), blob(b"x")],
            vec![text("1"), SqliteValue::Float(1.0), blob(b"x")],
            vec![text("1"), SqliteValue::Integer(1), text("x")],
            vec![text("01"), text("2"), text("x")],
            vec![text("word"), text("3"), SqliteValue::Null],
            vec![SqliteValue::Null, SqliteValue::Integer(4), blob(b"x")],
            vec![text("none"), SqliteValue::Null, SqliteValue::Null],
            vec![text("word"), text("4"), SqliteValue::Integer(1)],
            vec![text("word"), text("4"), SqliteValue::Float(1.0)],
            vec![text("space"), text("not-a-number"), blob(&[0])],
            vec![text("absent"), SqliteValue::Integer(1), SqliteValue::Null],
        ];
        for sql in [
            "SELECT 1 FROM probe AS v WHERE v.a=?1 AND v.b=?2 AND v.c IS ?3 LIMIT 1",
            "SELECT 1 FROM probe AS v WHERE ?1=v.a AND ?2=v.b AND ?3 IS v.c LIMIT 1",
            "SELECT 1 FROM probe AS v WHERE v.a=?1 AND v.b=?2 AND v.c=?3 LIMIT 1",
            "SELECT 1 FROM probe AS v WHERE v.a IS ?1 AND v.b=?2 AND v.c IS ?3 LIMIT 1",
            "SELECT 1 FROM probe AS v WHERE v.a=?1 AND v.b IS ?2 AND v.c IS ?3 LIMIT 1",
        ] {
            // The second pass changes every binding of the reused program in
            // reverse order, including transitions into and out of NULL.
            for params in cases.iter().chain(cases.iter().rev()) {
                assert_bound_query(&fixture.conn, &fixture.stock, sql, params).await;
            }
        }
        for value in [SqliteValue::Integer(1), text("1"), SqliteValue::Null] {
            assert_bound_query(
                &fixture.conn,
                &fixture.stock,
                "SELECT 1 FROM probe WHERE a=?1 AND b=?1 LIMIT 1",
                &[value],
            )
            .await;
        }

        // Typeless NEW columns expose the probe columns' comparison affinity;
        // the binding itself must retain its original storage class.
        fixture
            .execute_both(
                "CREATE TABLE input(id INTEGER PRIMARY KEY,a,b,c);
             INSERT INTO input VALUES (1,NULL,NULL,NULL);
             CREATE TRIGGER typed_guard BEFORE UPDATE ON input WHEN EXISTS (
               SELECT 1 FROM probe AS v WHERE v.a=NEW.a AND v.b=NEW.b
                 AND v.c IS NEW.c LIMIT 1)
             BEGIN SELECT RAISE(ABORT,'identity exists'); END;",
            )
            .await;
        let update = "UPDATE input SET a=?1,b=?2,c=?3 WHERE id=1";
        for params in cases.iter().chain(cases.iter().rev()) {
            let expected = fixture.stock.execute(
                update,
                rusqlite::params_from_iter(params.iter().map(stock_value)),
            );
            let actual = fixture.conn.execute_with_params(update, params).await;
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => assert_eq!(actual, expected, "{params:?}"),
                (Err(actual), Err(expected)) => {
                    assert!(
                        actual.to_string().contains("identity exists"),
                        "{params:?}: {actual}"
                    );
                    assert!(
                        expected.to_string().contains("identity exists"),
                        "{params:?}: {expected}"
                    );
                }
                (actual, expected) => {
                    panic!("trigger params={params:?}: {actual:?} vs {expected:?}")
                }
            }
            fixture
                .assert_query("SELECT id,a,typeof(a),b,typeof(b),c,typeof(c) FROM input")
                .await;
        }
        fixture.conn.close().await.expect("close");
    });
}

fn benchmark_usizes(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name).map_or_else(
        |_| default.to_vec(),
        |value| {
            value
                .split(',')
                .map(|part| part.parse().expect("positive benchmark integer"))
                .collect()
        },
    )
}

async fn time_franken(fixture: &Fixture, params: &[Vec<SqliteValue>]) -> f64 {
    fixture
        .conn
        .execute("BEGIN IMMEDIATE")
        .await
        .expect("benchmark begin");
    let start = Instant::now();
    let affected = fixture
        .conn
        .execute_many_with_params_skip_statement_savepoint_in_explicit_txn(UPDATE, params)
        .await
        .expect("benchmark updates");
    let elapsed = start.elapsed();
    assert_eq!(affected, params.len());
    fixture
        .conn
        .execute("ROLLBACK")
        .await
        .expect("benchmark rollback");
    elapsed.as_secs_f64() * 1000.0 / params.len() as f64
}

fn time_stock(fixture: &Fixture, params: &[Vec<SqliteValue>]) -> f64 {
    let params: Vec<Vec<_>> = params
        .iter()
        .map(|row| row.iter().map(stock_value).collect())
        .collect();
    fixture
        .stock
        .execute_batch("BEGIN IMMEDIATE")
        .expect("stock benchmark begin");
    let mut statement = fixture
        .stock
        .prepare(UPDATE)
        .expect("stock benchmark prepare");
    let start = Instant::now();
    for row in &params {
        assert_eq!(
            statement
                .execute(rusqlite::params_from_iter(row))
                .expect("stock benchmark update"),
            1
        );
    }
    let elapsed = start.elapsed();
    drop(statement);
    fixture
        .stock
        .execute_batch("ROLLBACK")
        .expect("stock benchmark rollback");
    elapsed.as_secs_f64() * 1000.0 / params.len() as f64
}

/// Run explicitly with --profile release-perf --ignored --nocapture.
/// Defaults reproduce 2k/8k/32k rows, 250 updates and three fresh-file reps.
/// GH526_BENCH_ROWS, GH526_BENCH_UPDATES and GH526_BENCH_REPS select a smaller
/// diagnostic run; every effective value is printed in its provenance.
#[test]
#[ignore = "file-backed performance comparison; run explicitly in release mode"]
fn composite_trigger_when_benchmark_against_stock() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let sizes = benchmark_usizes("GH526_BENCH_ROWS", &[2000, 8000, 32000]);
    let updates = benchmark_usizes("GH526_BENCH_UPDATES", &[250])[0];
    let reps = benchmark_usizes("GH526_BENCH_REPS", &[3])[0];
    assert!(updates > 0 && reps > 0 && sizes.iter().all(|&n| n >= updates));
    eprintln!(
        "GH526_CONFIG sqlite={} rows={sizes:?} updates={updates} reps={reps} debug_assertions={} stock_warmups=1 franken_warmups=0",
        rusqlite::version(),
        cfg!(debug_assertions)
    );
    asupersync::test_utils::run_test(|| async {
        let arms = [Arm::None, Arm::When, Arm::Body];
        let cells: Vec<_> = sizes
            .iter()
            .flat_map(|&n| arms.iter().map(move |&arm| (n, arm)))
            .collect();
        let mut timings = vec![(Vec::new(), Vec::new()); cells.len()];
        let params: Vec<_> = (0..updates)
            .map(|i| vec![SqliteValue::Integer(1), text(format!("id{i:08}"))])
            .collect();
        for rep in 0..reps {
            for step in 0..cells.len() {
                let index = (step + rep) % cells.len();
                let (n, arm) = cells[index];
                let fixture = Fixture::new(n, arm, false).await;
                let _warmup = time_stock(&fixture, &params);
                let (franken, stock) = if rep.is_multiple_of(2) {
                    (
                        time_franken(&fixture, &params).await,
                        time_stock(&fixture, &params),
                    )
                } else {
                    let stock = time_stock(&fixture, &params);
                    (time_franken(&fixture, &params).await, stock)
                };
                fixture
                    .assert_query("SELECT count(*),sum(c15) FROM t")
                    .await;
                fixture.conn.close().await.expect("benchmark close");
                eprintln!(
                    "GH526_CELL rep={} rows={n} arm={arm:?} franken_ms={franken:.6} stock_ms={stock:.6}",
                    rep + 1
                );
                timings[index].0.push(franken);
                timings[index].1.push(stock);
            }
        }
        for ((n, arm), (mut franken, mut stock)) in cells.into_iter().zip(timings) {
            franken.sort_by(f64::total_cmp);
            stock.sort_by(f64::total_cmp);
            let median =
                |values: &[f64]| values[(values.len() - 1) / 2].midpoint(values[values.len() / 2]);
            eprintln!(
                "GH526_MEDIAN rows={n} arm={arm:?} franken_ms={:.6} stock_ms={:.6} reps={reps}",
                median(&franken),
                median(&stock)
            );
        }
    });
}
