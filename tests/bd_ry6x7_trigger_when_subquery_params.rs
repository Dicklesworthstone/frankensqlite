#![recursion_limit = "512"]

//! bd-ry6x7: a trigger `WHEN` clause whose subquery references OLD/NEW was
//! compiled from scratch for every firing row.
//!
//! The WHEN binder replaced each OLD/NEW reference with a literal, so every
//! row produced a different nested statement that was validated, planned and
//! compiled anew (hfdt ds4pq: bulk persistence through 60 such validators ran
//! for 89 minutes at 97% CPU). OLD/NEW inside the clause's EXISTS guards now
//! bind as numbered parameters, so each guard compiles once and its program is
//! reused. Parameter probes keep the seeks literal probes had: a single-column
//! index miss is authoritative through a run-time storage-class check, and
//! WITHOUT ROWID primary-key point and prefix probes coerce the parameter to
//! the key's affinity, so a guard that finds nothing (the common case) still
//! seeks instead of scanning the table.
//!
//! The keepers pin the complexity (compilations do not grow with the row
//! count) and the semantics (outcomes and final state match stock SQLite for
//! mixed-type, NULL, collation and nested-subquery guards). They live in their
//! own test binary because the hot-path profile counters are process-global.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

const GUARDED_ROWS: i64 = 400;

/// Compilations allowed for the whole ingestion: fewer than one per row.
/// Compiling the guards per row costs at least three per row (1,786 for 400
/// rows before the fix); with parameters the guards recompile only when the
/// statement cache is invalidated (163 measured).
const MAX_COMPILATIONS: u64 = GUARDED_ROWS as u64;

#[test]
fn trigger_when_subqueries_compile_once_not_per_row() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute_batch(
            "CREATE TABLE evidence(\
                 id TEXT PRIMARY KEY, claim TEXT NOT NULL, kind TEXT NOT NULL, ord INTEGER NOT NULL);\
             CREATE TABLE ledger(seq INTEGER PRIMARY KEY, id TEXT, ord INTEGER);\
             CREATE TRIGGER evidence_no_replace BEFORE INSERT ON evidence \
                 WHEN EXISTS (SELECT 1 FROM evidence AS cur WHERE cur.id = NEW.id) \
                 BEGIN SELECT RAISE(ABORT, 'evidence append-only'); END;\
             CREATE TRIGGER evidence_conflict BEFORE INSERT ON evidence \
                 WHEN EXISTS (SELECT 1 FROM evidence AS cur WHERE (cur.id = NEW.id) \
                     AND ((cur.claim != NEW.claim) OR (cur.kind != NEW.kind))) \
                 BEGIN SELECT RAISE(ABORT, 'evidence conflict'); END;\
             CREATE TRIGGER evidence_ord_guard BEFORE INSERT ON evidence \
                 WHEN NOT EXISTS (SELECT 1 FROM evidence AS cur WHERE cur.ord IS NEW.ord) \
                     AND EXISTS (SELECT 1 FROM ledger WHERE ledger.ord = NEW.ord) \
                 BEGIN SELECT RAISE(ABORT, 'ord already ledgered'); END;\
             CREATE TRIGGER evidence_kind_guard BEFORE INSERT ON evidence \
                 WHEN NOT (NEW.kind = 'filing' OR EXISTS ( \
                     SELECT 1 FROM ledger WHERE ledger.id = NEW.id AND ledger.ord = NEW.ord)) \
                 BEGIN SELECT RAISE(ABORT, 'unknown kind'); END;",
        )
        .await
        .expect("schema");
        conn.execute(
            "INSERT INTO ledger(id, ord) SELECT 'ledger-' || value, 100000 + value \
             FROM generate_series(1, 200);",
        )
        .await
        .expect("seed ledger");

        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        conn.execute("BEGIN;").await.expect("begin");
        for i in 0..GUARDED_ROWS {
            conn.execute_with_params(
                "INSERT INTO evidence(id, claim, kind, ord) VALUES (?1, ?2, 'filing', ?3);",
                &[
                    SqliteValue::Text(format!("ev-{i:06}").into()),
                    SqliteValue::Text(format!("claim-{}", i % 17).into()),
                    SqliteValue::Integer(i),
                ],
            )
            .await
            .unwrap_or_else(|error| panic!("guarded insert {i} must not refuse: {error:?}"));
        }
        conn.execute("COMMIT;").await.expect("commit");
        let compilations = hot_path_profile_snapshot().parser.compiled_cache_misses;
        set_hot_path_profile_enabled(false);
        eprintln!("bd-ry6x7: {compilations} compilations for {GUARDED_ROWS} guarded inserts");
        assert!(
            compilations <= MAX_COMPILATIONS,
            "bd-ry6x7: {compilations} statement compilations for {GUARDED_ROWS} inserts through \
             four EXISTS-guarded triggers; the guard subqueries are compiled once per row"
        );

        let count = conn
            .query("SELECT count(*) FROM evidence;")
            .await
            .expect("count");
        assert_eq!(count[0].values(), &[SqliteValue::Integer(GUARDED_ROWS)]);

        // Every guard still fires, with values that differ from the ones its
        // cached program was first run with.
        for (sql, message) in [
            (
                "INSERT INTO evidence VALUES ('ev-000007', 'claim-7', 'filing', 7);",
                "evidence append-only",
            ),
            (
                "INSERT INTO evidence VALUES ('ev-x', 'claim', 'filing', 100150);",
                "ord already ledgered",
            ),
            (
                "INSERT INTO evidence VALUES ('ev-y', 'claim', 'memo', 5000);",
                "unknown kind",
            ),
        ] {
            let error = conn
                .execute(sql)
                .await
                .expect_err("the guard must RAISE(ABORT)");
            assert!(
                error.to_string().contains(message),
                "{sql}: expected {message:?}, got {error:?}"
            );
        }
        // ... and admits what they should. ledger-3 is linked to ord 100003, so
        // a memo for (ledger-3, 7000) fails the kind guard until the ledger
        // links that pair; 7000 is already an evidence ord by then, so the ord
        // guard's NOT EXISTS arm lets it through.
        conn.execute("INSERT INTO evidence VALUES ('ev-7000', 'claim', 'filing', 7000);")
            .await
            .expect("an unledgered ord passes");
        let unlinked = conn
            .execute("INSERT INTO evidence VALUES ('ledger-3', 'claim', 'memo', 7000);")
            .await
            .expect_err("(ledger-3, 7000) is not linked yet");
        assert!(
            unlinked.to_string().contains("unknown kind"),
            "unexpected error: {unlinked:?}"
        );
        conn.execute("UPDATE ledger SET ord = 7000 WHERE id = 'ledger-3';")
            .await
            .expect("link");
        conn.execute("INSERT INTO evidence VALUES ('ledger-3', 'claim', 'memo', 7000);")
            .await
            .expect("a linked memo passes both guards");
    });
}

/// Guards over NULLs, a NOCASE column, a typeless column, OLD references, a
/// scalar subquery, a view, a WITHOUT ROWID composite key and a nested
/// EXISTS. Every statement's outcome and the final contents must match stock
/// SQLite.
///
/// Inserted values already carry their column's storage class, and every
/// guard compares columns of the same affinity: fsqlite binds OLD/NEW without
/// the column affinity stock gives them (a separate, pre-existing deviation
/// that literal and parameter binding share), so cross-affinity guards would
/// test that instead of this change. Mixed storage classes are covered by the
/// direct parameter probes below, which exercise the seek paths this change
/// opened to parameters.
const PARITY_SCHEMA: &str = "\
CREATE TABLE t(id TEXT PRIMARY KEY, k INTEGER, n, c TEXT COLLATE NOCASE, r REAL);
CREATE INDEX t_k ON t(k);
CREATE INDEX t_n ON t(n);
CREATE INDEX t_c ON t(c);
CREATE INDEX t_r ON t(r);
CREATE TABLE log(seq INTEGER PRIMARY KEY, tag TEXT, val);
CREATE TRIGGER g_id BEFORE INSERT ON t
  WHEN EXISTS (SELECT 1 FROM t AS e WHERE e.id = NEW.id)
  BEGIN SELECT RAISE(ABORT, 'dup-id'); END;
CREATE TRIGGER g_k AFTER INSERT ON t
  WHEN EXISTS (SELECT 1 FROM t AS e WHERE e.k = NEW.k AND e.id <> NEW.id)
  BEGIN INSERT INTO log(tag, val) VALUES ('k', NEW.k); END;
CREATE TRIGGER g_n AFTER INSERT ON t
  WHEN NOT EXISTS (SELECT 1 FROM t AS e WHERE e.n = NEW.n AND e.id IS NOT NEW.id)
  BEGIN INSERT INTO log(tag, val) VALUES ('n-new', NEW.n); END;
CREATE TRIGGER g_c AFTER INSERT ON t
  WHEN (SELECT count(*) FROM t AS e WHERE e.c = NEW.c) > 1
  BEGIN INSERT INTO log(tag, val) VALUES ('c', NEW.c); END;
CREATE TRIGGER g_r AFTER INSERT ON t
  WHEN EXISTS (SELECT 1 FROM t AS e WHERE e.r = NEW.r AND e.id <> NEW.id)
  BEGIN INSERT INTO log(tag, val) VALUES ('r', NEW.r); END;
CREATE TRIGGER g_upd BEFORE UPDATE OF k ON t
  WHEN NEW.k IS NOT OLD.k AND EXISTS (SELECT 1 FROM t AS e WHERE e.k = OLD.k AND e.id <> OLD.id)
  BEGIN INSERT INTO log(tag, val) VALUES ('upd-shared', OLD.k || '->' || NEW.k); END;
CREATE TRIGGER g_del BEFORE DELETE ON t
  WHEN NOT EXISTS (SELECT 1 FROM t AS e WHERE e.id <> OLD.id AND e.k = OLD.k)
  BEGIN INSERT INTO log(tag, val) VALUES ('del-last', OLD.k); END;
CREATE TABLE w(cap TEXT NOT NULL, ord INTEGER NOT NULL, nk TEXT NOT NULL, v,
  PRIMARY KEY (cap, ord), UNIQUE (cap, nk)) WITHOUT ROWID;
CREATE TRIGGER w_key BEFORE INSERT ON w
  WHEN EXISTS (SELECT 1 FROM w AS e WHERE e.cap = NEW.cap AND e.ord = NEW.ord)
  BEGIN SELECT RAISE(ABORT, 'dup-id'); END;
CREATE TRIGGER w_cap AFTER INSERT ON w
  WHEN EXISTS (SELECT 1 FROM w AS e WHERE e.cap = NEW.cap AND e.v IS NOT NEW.v)
  BEGIN INSERT INTO log(tag, val) VALUES ('w-cap', NEW.cap); END;
CREATE TRIGGER w_nk AFTER INSERT ON w
  WHEN EXISTS (SELECT 1 FROM w AS e WHERE e.cap = NEW.cap AND e.nk = NEW.nk AND e.ord <> NEW.ord)
  BEGIN INSERT INTO log(tag, val) VALUES ('w-nk', NEW.nk); END;
CREATE VIEW vk AS SELECT id, k FROM t WHERE k IS NOT NULL;
CREATE TRIGGER g_view AFTER INSERT ON t
  WHEN (SELECT count(*) FROM vk WHERE vk.k = NEW.k) > 1
  BEGIN INSERT INTO log(tag, val) VALUES ('view', NEW.k); END;
CREATE TRIGGER g_nested AFTER INSERT ON t
  WHEN EXISTS (SELECT 1 FROM log WHERE log.tag = 'n-new' AND log.seq = NEW.k
               AND EXISTS (SELECT 1 FROM t AS e2 WHERE e2.id = NEW.id AND e2.k = NEW.k))
  BEGIN INSERT INTO log(tag, val) VALUES ('nested', NEW.id); END;
";

const PARITY_STATEMENTS: &[&str] = &[
    "INSERT INTO t VALUES ('a', 1, 1, 'X', 1.0)",
    "INSERT INTO t VALUES ('b', 1, '1', 'x', 2.5)",
    "INSERT INTO t VALUES ('c', 1, 1.0, 'Y', 1.0)",
    "INSERT INTO t VALUES ('1', 2, 'abc', NULL, NULL)",
    "INSERT INTO t VALUES ('1', 3, x'00', 'y', 2.0)",
    "INSERT INTO t VALUES ('d', NULL, NULL, NULL, NULL)",
    "INSERT INTO t VALUES ('e', NULL, NULL, 'z', 3.0)",
    "INSERT INTO t VALUES ('a', 9, 9, 'q', 9.0)",
    "INSERT INTO t VALUES ('f', 3, '3', 'Z', 3.0)",
    "INSERT INTO t VALUES ('2.0', 2, 2, 'w', 2.5)",
    "INSERT INTO t VALUES ('g', 4, x'00', 'ABC', 4.5)",
    "INSERT INTO t VALUES ('h', 4, 4, 'abc', 1.0)",
    "UPDATE t SET k = 7 WHERE id = 'a'",
    "UPDATE t SET k = k WHERE id = 'b'",
    "UPDATE t SET k = 2 WHERE id = 'c'",
    "DELETE FROM t WHERE id = 'c'",
    "DELETE FROM t WHERE id = '1'",
    "DELETE FROM t WHERE id = '2.0'",
    "DELETE FROM t WHERE id = 'e'",
    "INSERT INTO w VALUES ('c1', 1, 'n1', 1)",
    "INSERT INTO w VALUES ('c1', 2, 'n2', 1)",
    "INSERT INTO w VALUES ('c1', 1, 'n3', 2)",
    "INSERT INTO w VALUES ('c1', 3, 'n1', NULL)",
    "INSERT INTO w VALUES ('5', 1, 'n1', 'x')",
    "INSERT INTO w VALUES ('5', 2, 'n9', 'x')",
    "INSERT INTO w VALUES ('5', 2, 'n8', 'y')",
    "INSERT INTO w VALUES ('c2', 1.5, 'n1', 1)",
    "INSERT INTO w VALUES ('c2', 2, 'n1', 1)",
];

const PARITY_DUMPS: &[&str] = &[
    "SELECT id, typeof(id), k, typeof(k), n, typeof(n), c, r, typeof(r) FROM t ORDER BY id",
    "SELECT seq, tag, val, typeof(val) FROM log ORDER BY seq",
    "SELECT cap, typeof(cap), ord, typeof(ord), nk, v FROM w ORDER BY cap, ord",
];

/// Parameter probes of a single-column index, including the storage classes
/// the run-time exactness check must NOT treat as authoritative.
const PARITY_PROBES: &[(&str, &[ProbeParam])] = &[
    ("SELECT id FROM t WHERE id = ?1 ORDER BY id", &[ProbeParam::Int(1)]),
    ("SELECT id FROM t WHERE id = ?1 ORDER BY id", &[ProbeParam::Text("1")]),
    ("SELECT id FROM t WHERE id = ?1 ORDER BY id", &[ProbeParam::Real(2.0)]),
    ("SELECT id FROM t WHERE id = ?1 ORDER BY id", &[ProbeParam::Text("zz")]),
    ("SELECT id FROM t WHERE k = ?1 ORDER BY id", &[ProbeParam::Text("1")]),
    ("SELECT id FROM t WHERE k = ?1 ORDER BY id", &[ProbeParam::Int(7)]),
    ("SELECT id FROM t WHERE k = ?1 ORDER BY id", &[ProbeParam::Real(4.0)]),
    ("SELECT id FROM t WHERE k = ?1 ORDER BY id", &[ProbeParam::Int(99)]),
    ("SELECT id FROM t WHERE k = ?1 ORDER BY id", &[ProbeParam::Text("abc")]),
    ("SELECT id FROM t WHERE k = ?1 ORDER BY id", &[ProbeParam::Null]),
    ("SELECT id FROM t WHERE c = ?1 ORDER BY id", &[ProbeParam::Text("abc")]),
    ("SELECT id FROM t WHERE c = ?1 ORDER BY id", &[ProbeParam::Text("Q")]),
    ("SELECT id FROM t WHERE n = ?1 ORDER BY id", &[ProbeParam::Int(1)]),
    ("SELECT id FROM t WHERE n = ?1 ORDER BY id", &[ProbeParam::Text("3")]),
    (
        "SELECT 1 FROM w WHERE cap = ?1 AND ord = ?2 LIMIT 1",
        &[ProbeParam::Text("c1"), ProbeParam::Text("2")],
    ),
    (
        "SELECT 1 FROM w WHERE cap = ?1 AND ord = ?2 LIMIT 1",
        &[ProbeParam::Int(5), ProbeParam::Real(2.0)],
    ),
    (
        "SELECT 1 FROM w WHERE cap = ?1 AND ord = ?2 LIMIT 1",
        &[ProbeParam::Text("c1"), ProbeParam::Int(9)],
    ),
    (
        "SELECT 1 FROM w WHERE cap = ?1 AND v IS NOT ?2 LIMIT 1",
        &[ProbeParam::Int(5), ProbeParam::Text("x")],
    ),
    (
        "SELECT 1 FROM w WHERE cap = ?1 AND v IS NOT ?2 LIMIT 1",
        &[ProbeParam::Text("c2"), ProbeParam::Int(1)],
    ),
    (
        "SELECT 1 FROM w WHERE cap = ?1 AND v IS NOT ?2 LIMIT 1",
        &[ProbeParam::Null, ProbeParam::Int(1)],
    ),
];

#[derive(Debug, Clone, Copy)]
enum ProbeParam {
    Int(i64),
    Real(f64),
    Text(&'static str),
    Null,
}

impl ProbeParam {
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

/// `ok`, or the RAISE message the statement aborted with.
fn outcome(result: Result<(), String>) -> String {
    match result {
        Ok(()) => "ok".to_owned(),
        Err(message) if message.contains("dup-id") => "abort:dup-id".to_owned(),
        Err(message) => format!("error:{message}"),
    }
}

fn stock_transcript() -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(PARITY_SCHEMA).expect("stock schema");
    let mut transcript = Vec::new();
    for sql in PARITY_STATEMENTS {
        let result = conn.execute(sql, []).map(|_| ()).map_err(|e| e.to_string());
        transcript.push(format!("{sql} => {}", outcome(result)));
    }
    let query = |sql: &str, params: Vec<rusqlite::types::Value>| -> Vec<String> {
        let mut stmt = conn.prepare(sql).expect("stock prepare");
        let width = stmt.column_count();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .expect("stock query");
        let mut out = Vec::new();
        while let Some(row) = rows.next().expect("stock row") {
            let cells: Vec<String> = (0..width)
                .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                .collect();
            out.push(cells.join("|"));
        }
        out
    };
    for sql in PARITY_DUMPS {
        transcript.push(format!("{sql} => {:?}", query(sql, Vec::new())));
    }
    for (sql, params) in PARITY_PROBES {
        let bound = params.iter().map(|p| p.rusqlite()).collect();
        transcript.push(format!("{sql} {params:?} => {:?}", query(sql, bound)));
    }
    transcript
}

#[test]
fn trigger_when_subquery_guards_match_stock() {
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute_batch(PARITY_SCHEMA).await.expect("schema");
        let mut transcript = Vec::new();
        for sql in PARITY_STATEMENTS {
            let result = conn.execute(sql).await.map(|_| ()).map_err(|e| e.to_string());
            transcript.push(format!("{sql} => {}", outcome(result)));
        }
        let render_rows = |rows: Vec<fsqlite_core::connection::Row>| -> Vec<String> {
            rows.iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(render_fsqlite)
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect()
        };
        for sql in PARITY_DUMPS {
            let rows = conn.query(sql).await.expect("dump");
            transcript.push(format!("{sql} => {:?}", render_rows(rows)));
        }
        for (sql, params) in PARITY_PROBES {
            let bound: Vec<SqliteValue> = params.iter().map(|p| p.fsqlite()).collect();
            let rows = conn.query_with_params(sql, &bound).await.expect("probe");
            transcript.push(format!("{sql} {params:?} => {:?}", render_rows(rows)));
        }
        for (line, (got, want)) in transcript.iter().zip(&expected).enumerate() {
            assert_eq!(got, want, "bd-ry6x7 parity line {line}");
        }
        assert_eq!(transcript.len(), expected.len());
    });
}
