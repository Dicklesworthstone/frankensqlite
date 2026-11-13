#![recursion_limit = "512"]

//! bd-8c68u: a trigger guard `WHEN EXISTS (SELECT 1 FROM p AS journal JOIN m
//! AS member ON member.id = journal.mid WHERE journal.pid = NEW.pid AND
//! member.intent = NEW.intent)` scanned both tables for every guarded row.
//!
//! The EXISTS probe carried `LIMIT 1`, which keeps a join off the VDBE, so it
//! ran in the materializing join executor (both tables read in full per
//! probe: a bulk insert of 4,000 guarded rows took minutes). The probe now
//! stays on the VDBE join, whose outer loop seeks the outer table through an
//! index (or its rowid) when the WHERE pins an outer column with `=` to a
//! literal or parameter, instead of scanning it.
//!
//! The keepers pin the complexity (every probe is one VDBE statement that
//! runs a bounded number of opcodes) and the semantics of the seek against
//! stock SQLite: storage classes and affinities of the probe, NULL, NOCASE
//! and untyped columns, rowid seeks, LEFT JOIN and aggregates. The hot-path
//! profile counters are process-global, so the tests here run one at a time.

use fsqlite_core::connection::{
    Connection, Row, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

const GUARDED_ROWS: i64 = 300;

/// Opcodes allowed per guarded row (the INSERT's own work plus one seeking
/// probe). Scanning the 2,000-row outer table costs tens of thousands.
const MAX_OPCODES_PER_ROW: u64 = 1_500;

#[test]
fn join_exists_guard_probe_seeks_instead_of_scanning() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute_batch(
            "CREATE TABLE m(id INTEGER PRIMARY KEY, intent TEXT, owner INTEGER);\
             CREATE INDEX m_intent ON m(intent);\
             CREATE TABLE p(id INTEGER PRIMARY KEY, pid INTEGER, mid INTEGER);\
             CREATE INDEX p_pid ON p(pid);\
             CREATE TABLE ev(id INTEGER PRIMARY KEY, pid INTEGER, intent TEXT);\
             INSERT INTO m SELECT value, 'intent' || (value % 500), value % 37 \
                 FROM generate_series(1, 2000);\
             INSERT INTO p SELECT value, value % 1000, (value * 7) % 2000 + 1 \
                 FROM generate_series(1, 2000);\
             CREATE TRIGGER ev_guard BEFORE INSERT ON ev \
               WHEN EXISTS (SELECT 1 FROM p AS journal JOIN m AS member \
                 ON member.id = journal.mid \
                 WHERE journal.pid = NEW.pid AND member.intent = NEW.intent) \
             BEGIN SELECT RAISE(IGNORE); END;",
        )
        .await
        .expect("schema");

        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        let inserted = conn
            .execute(&format!(
                "INSERT INTO ev SELECT value, value % 1500, 'intent' || (value % 700) \
                 FROM generate_series(1, {GUARDED_ROWS});"
            ))
            .await
            .expect("guarded insert");
        let profile = hot_path_profile_snapshot();
        set_hot_path_profile_enabled(false);
        let statements = profile.vdbe.statements_total;
        let opcodes = profile.vdbe.opcodes_executed_total;
        eprintln!(
            "bd-8c68u: {inserted} rows inserted, {statements} VDBE statements, {opcodes} opcodes"
        );
        assert!(
            statements >= GUARDED_ROWS as u64,
            "bd-8c68u: {statements} VDBE statements for {GUARDED_ROWS} guarded rows; the join \
             probe left the VDBE for the materializing join executor"
        );
        assert!(
            opcodes <= MAX_OPCODES_PER_ROW * GUARDED_ROWS as u64,
            "bd-8c68u: {opcodes} opcodes for {GUARDED_ROWS} guarded rows; the join probe scans \
             its outer table instead of seeking it (limit {MAX_OPCODES_PER_ROW} per row)"
        );

        let rows = conn
            .query(
                "SELECT count(*), sum(id) FROM ev WHERE NOT EXISTS (SELECT 1 FROM p JOIN m \
                 ON m.id = p.mid WHERE p.pid = ev.pid AND m.intent = ev.intent)",
            )
            .await
            .expect("count");
        let all = conn.query("SELECT count(*) FROM ev").await.expect("all");
        assert_eq!(rows[0].values()[0], all[0].values()[0], "only unguarded rows were inserted");
    });
}

/// Scalar-subquery WHEN guards: the comparison operand and the bare truthy
/// leaf bind OLD/NEW as parameters and reuse one compiled program, instead of
/// one compile per guarded row; the guarded rows match stock.
const SCALAR_GUARD_SCHEMA: &str = "\
CREATE TABLE u(k INTEGER, flag);\
INSERT INTO u VALUES (3, 1), (7, 0), (1011, 1), (1020, 1), (12, 1);\
CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER);\
CREATE TABLE log(v);\
CREATE TRIGGER t_g1 BEFORE INSERT ON t \
    WHEN (SELECT count(*) FROM u WHERE u.k = NEW.k) > 0 BEGIN SELECT RAISE(IGNORE); END;\
CREATE TRIGGER t_g2 AFTER INSERT ON t \
    WHEN (SELECT flag FROM u WHERE u.k = NEW.k + 1000) BEGIN INSERT INTO log VALUES (NEW.id); END;\
CREATE TRIGGER t_g3 AFTER INSERT ON t \
    WHEN NEW.id % 7 = 0 AND (SELECT max(k) FROM u WHERE k < NEW.k) IS NOT NULL \
    BEGIN INSERT INTO log VALUES (-NEW.id); END;";

const SCALAR_GUARD_INSERT: &str = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 \
    FROM c WHERE x < 300) INSERT INTO t SELECT x, x % 50 FROM c";

const SCALAR_GUARD_DUMPS: &[&str] = &[
    "SELECT count(*), sum(id), sum(k) FROM t",
    "SELECT v FROM log ORDER BY rowid",
];

/// Compilations allowed for the 300-row guarded insert: a constant handful
/// per guard, not one per row.
const MAX_SCALAR_GUARD_COMPILATIONS: u64 = 60;

#[test]
fn scalar_subquery_when_guards_compile_once_and_match_stock() {
    let _serial = serial();
    let stock = {
        let conn = rusqlite::Connection::open_in_memory().expect("stock open");
        conn.execute_batch(SCALAR_GUARD_SCHEMA).expect("stock schema");
        conn.execute_batch(SCALAR_GUARD_INSERT).expect("stock insert");
        SCALAR_GUARD_DUMPS
            .iter()
            .map(|sql| {
                let mut stmt = conn.prepare(sql).expect("stock prepare");
                let width = stmt.column_count();
                stmt.query_map([], |row| {
                    Ok((0..width)
                        .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                        .collect::<Vec<_>>()
                        .join("|"))
                })
                .expect("stock query")
                .collect::<Result<Vec<_>, _>>()
                .expect("stock rows")
            })
            .collect::<Vec<_>>()
    };
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute_batch(SCALAR_GUARD_SCHEMA).await.expect("schema");
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        conn.execute(SCALAR_GUARD_INSERT).await.expect("guarded insert");
        let compilations = hot_path_profile_snapshot().parser.compiled_cache_misses;
        set_hot_path_profile_enabled(false);
        eprintln!("bd-8c68u: scalar WHEN guards compiled {compilations} programs");
        assert!(
            compilations <= MAX_SCALAR_GUARD_COMPILATIONS,
            "bd-8c68u: {compilations} compilations for 300 guarded rows; scalar-subquery \
             WHEN guards compile per row (limit {MAX_SCALAR_GUARD_COMPILATIONS})"
        );
        for (sql, want) in SCALAR_GUARD_DUMPS.iter().zip(&stock) {
            let rows = conn.query(sql).await.expect("dump");
            let got: Vec<String> = rows
                .iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(render_fsqlite)
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect();
            assert_eq!(&got, want, "bd-8c68u scalar guard parity: {sql}");
        }
    });
}

#[derive(Debug, Clone, Copy)]
enum P {
    Int(i64),
    Real(f64),
    Text(&'static str),
    Null,
}

impl P {
    fn fsqlite(self) -> SqliteValue {
        match self {
            Self::Int(v) => SqliteValue::Integer(v),
            Self::Real(v) => SqliteValue::Float(v),
            Self::Text(v) => SqliteValue::Text(v.into()),
            Self::Null => SqliteValue::Null,
        }
    }

    fn rusqlite(self) -> rusqlite::types::Value {
        match self {
            Self::Int(v) => rusqlite::types::Value::Integer(v),
            Self::Real(v) => rusqlite::types::Value::Real(v),
            Self::Text(v) => rusqlite::types::Value::Text(v.to_owned()),
            Self::Null => rusqlite::types::Value::Null,
        }
    }
}

const SCHEMA: &str = "\
CREATE TABLE i(id INTEGER PRIMARY KEY, tag TEXT);\
INSERT INTO i VALUES (1, 'i1'), (2, 'i2'), (3, 'i3'), (4, 'i4');\
CREATE TABLE o(id INTEGER PRIMARY KEY, iref INTEGER, k INTEGER, s TEXT, \
    n TEXT COLLATE NOCASE, r REAL, u);\
CREATE INDEX o_k ON o(k);\
CREATE INDEX o_s ON o(s);\
CREATE INDEX o_n ON o(n);\
CREATE INDEX o_r ON o(r);\
CREATE INDEX o_u ON o(u);\
INSERT INTO o VALUES \
    (1, 1, 2, 'a', 'Abc', 2.0, 5), \
    (2, 2, '2', '1', 'abc', 2.5, '5'), \
    (3, 3, 'x', 1, 'ABC', 3, 5.0), \
    (4, 9, NULL, NULL, NULL, NULL, NULL), \
    (5, 4, 2.5, 'A', 'abd', '2', x'05'), \
    (6, 1, 7, 'a', 'Abc', 7.5, 'x');";

/// (query, parameter sets): every query joins the inner table by its rowid
/// and pins an outer column, so the outer loop takes the seek.
const QUERIES: &[(&str, &[&[P]])] = &[
    (
        "SELECT o.id, i.tag FROM o JOIN i ON i.id = o.iref WHERE o.k = ?1",
        &[
            &[P::Int(2)],
            &[P::Text("2")],
            &[P::Real(2.0)],
            &[P::Real(2.5)],
            &[P::Text("x")],
            &[P::Null],
        ],
    ),
    (
        "SELECT o.id, i.tag FROM o JOIN i ON i.id = o.iref WHERE o.s = ?1",
        &[&[P::Text("1")], &[P::Int(1)], &[P::Text("a")], &[P::Text("A")], &[P::Null]],
    ),
    (
        "SELECT o.id, i.tag FROM o JOIN i ON i.id = o.iref WHERE o.n = ?1",
        &[&[P::Text("abc")], &[P::Text("ABD")]],
    ),
    (
        "SELECT o.id, i.tag FROM o JOIN i ON i.id = o.iref WHERE o.r = ?1",
        &[&[P::Int(2)], &[P::Text("2.5")], &[P::Real(3.0)]],
    ),
    (
        "SELECT o.id, i.tag FROM o JOIN i ON i.id = o.iref WHERE o.u = ?1",
        &[&[P::Int(5)], &[P::Text("5")], &[P::Real(5.0)], &[P::Text("x")]],
    ),
    (
        "SELECT o.id, i.tag FROM o JOIN i ON i.id = o.iref WHERE o.id = ?1",
        &[&[P::Int(3)], &[P::Text("3")], &[P::Real(3.5)], &[P::Text("x")], &[P::Null]],
    ),
    (
        "SELECT o.id, i.tag FROM o LEFT JOIN i ON i.id = o.iref WHERE o.k = ?1",
        &[&[P::Int(2)], &[P::Null]],
    ),
    (
        "SELECT o.id, i.tag FROM o LEFT JOIN i ON i.id = o.iref WHERE o.k = ?1 AND i.tag IS NULL",
        &[&[P::Int(2)]],
    ),
    (
        "SELECT count(*), sum(o.id) FROM o JOIN i ON i.id = o.iref WHERE ?1 = o.k AND i.tag <> 'i9'",
        &[&[P::Int(2)], &[P::Int(7)]],
    ),
    (
        "SELECT o.id FROM o JOIN i ON i.id = o.iref WHERE o.k = 2",
        &[&[]],
    ),
    (
        "SELECT o.id FROM o JOIN i ON i.id = o.iref WHERE o.s = 'a' AND o.id > 1",
        &[&[]],
    ),
    (
        "SELECT o.id FROM o JOIN i ON i.id = o.iref WHERE o.k = '2'",
        &[&[]],
    ),
    (
        "SELECT EXISTS (SELECT 1 FROM o JOIN i ON i.id = o.iref WHERE o.k = ?1 AND i.tag = ?2)",
        &[&[P::Int(2), P::Text("i2")], &[P::Int(2), P::Text("i3")]],
    ),
];

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Integer(v) => format!("i:{v}"),
        SqliteValue::Float(v) => format!("f:{v}"),
        SqliteValue::Text(v) => format!("t:{v}"),
        SqliteValue::Blob(v) => format!("b:{:?}", v.to_vec()),
        SqliteValue::Null => "null".to_owned(),
    }
}

fn render_rusqlite(value: rusqlite::types::ValueRef<'_>) -> String {
    match value {
        rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
        rusqlite::types::ValueRef::Real(v) => format!("f:{v}"),
        rusqlite::types::ValueRef::Text(v) => format!("t:{}", String::from_utf8_lossy(v)),
        rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
        rusqlite::types::ValueRef::Null => "null".to_owned(),
    }
}

fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

fn stock_transcript() -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(SCHEMA).expect("stock schema");
    let mut transcript = Vec::new();
    for (sql, param_sets) in QUERIES {
        for params in *param_sets {
            let mut stmt = conn.prepare(sql).expect("stock prepare");
            let width = stmt.column_count();
            let rows = stmt
                .query_map(
                    rusqlite::params_from_iter(params.iter().map(|p| p.rusqlite())),
                    |row| {
                        Ok((0..width)
                            .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                            .collect::<Vec<_>>()
                            .join("|"))
                    },
                )
                .expect("stock query")
                .collect::<Result<Vec<_>, _>>()
                .expect("stock rows");
            transcript.push(format!("{sql} {params:?} => {:?}", sorted(rows)));
        }
    }
    transcript
}

async fn fsqlite_transcript(conn: &Connection) -> Vec<String> {
    conn.execute_batch(SCHEMA).await.expect("schema");
    let mut transcript = Vec::new();
    for (sql, param_sets) in QUERIES {
        for params in *param_sets {
            let bound: Vec<SqliteValue> = params.iter().map(|p| p.fsqlite()).collect();
            let rows: Vec<Row> = conn
                .query_with_params(sql, &bound)
                .await
                .unwrap_or_else(|error| panic!("{sql} {params:?}: {error}"));
            let rendered = rows
                .iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(render_fsqlite)
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect();
            transcript.push(format!("{sql} {params:?} => {:?}", sorted(rendered)));
        }
    }
    transcript
}

fn assert_matches_stock(label: &str, got: &[String], want: &[String]) {
    for (line, (got, want)) in got.iter().zip(want).enumerate() {
        assert_eq!(got, want, "bd-8c68u {label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "bd-8c68u {label} transcript length");
}

#[test]
fn outer_equality_seek_matches_stock_in_memory() {
    let _serial = serial();
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn outer_equality_seek_matches_stock_file_backed() {
    let _serial = serial();
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("8c68u.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(&path_str).await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("file-backed", &got, &expected);
    });
}
