#![recursion_limit = "512"]

//! Adversarial stock parity for the UPDATE family landed 2026-10-02/03
//! (in-place rewrite a31225e8b, exact conflict restore 36b2bbf09, two-pass
//! UPDATE ... FROM 033d49ac0, UPDATE ... FROM trigger replay 2b6215390,
//! parameterized replay f41221f85), in memory and file-backed, plus stock
//! integrity_check on the file.
//!
//! The case that failed: an `UPDATE ... FROM` whose target row several FROM
//! rows match applied the FIRST match when the table had UPDATE triggers
//! (row-by-row replay and the trigger OLD/NEW collector), but the LAST match
//! without triggers (the compiled lane) and in stock. Covered here with rowid,
//! WITHOUT ROWID NOCASE and single-target shapes.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

struct Case {
    name: &'static str,
    schema: &'static str,
    statements: &'static [&'static str],
    dumps: &'static [&'static str],
}

const CASES: &[Case] = &[
    Case {
        name: "in_place_or_ignore_not_null_check",
        schema: "CREATE TABLE t(id INTEGER PRIMARY KEY, a NOT NULL, b CHECK (b < 100), c);\
                 CREATE INDEX t_c ON t(c);\
                 INSERT INTO t SELECT value, value, value % 90, value % 7 FROM nums WHERE value <= 300;",
        statements: &[
            "UPDATE OR IGNORE t SET a = CASE WHEN id % 3 = 0 THEN NULL ELSE a + 1 END, c = c + 1",
            "UPDATE OR IGNORE t SET b = b + 50, c = c * 2",
            "UPDATE OR FAIL t SET b = b + 10 WHERE id > 50",
            "UPDATE OR REPLACE t SET a = NULL WHERE id < 5",
        ],
        dumps: &["SELECT id, a, b, c FROM t ORDER BY id", "SELECT count(*) FROM t WHERE c > 3"],
    },
    Case {
        name: "in_place_replace_victims",
        schema: "CREATE TABLE u(id INTEGER PRIMARY KEY, k UNIQUE, v, w);\
                 CREATE INDEX u_w ON u(w);\
                 INSERT INTO u SELECT value, value, 'v' || value, value % 5 FROM nums WHERE value <= 400;",
        statements: &[
            "UPDATE OR REPLACE u SET k = k + 1 WHERE id % 10 = 0",
            "UPDATE OR REPLACE u SET k = 1, v = 'one' WHERE id = 200",
            "UPDATE OR REPLACE u SET k = k - 1, w = w + 1 WHERE id BETWEEN 100 AND 140",
            "UPDATE OR IGNORE u SET k = k + 1, w = w + 7",
        ],
        dumps: &["SELECT id, k, v, w FROM u ORDER BY id", "SELECT count(*), sum(w) FROM u"],
    },
    Case {
        name: "in_place_overflow_grow_shrink",
        schema: "CREATE TABLE o(id INTEGER PRIMARY KEY, v, w);\
                 CREATE INDEX o_w ON o(w);\
                 INSERT INTO o SELECT value, randomblob(0), value FROM nums WHERE value <= 200;",
        statements: &[
            "UPDATE o SET v = zeroblob(5000 + id) WHERE id % 2 = 0",
            "UPDATE o SET v = zeroblob(9000 + id), w = -w WHERE id % 3 = 0",
            "UPDATE o SET v = 'short' || id WHERE id % 4 = 0",
            "UPDATE o SET v = zeroblob(length(v)) WHERE id % 5 = 0",
            "UPDATE o SET w = w + 1",
        ],
        dumps: &["SELECT id, length(v), typeof(v), w FROM o ORDER BY id"],
    },
    Case {
        name: "in_place_savepoints",
        schema: "CREATE TABLE s(id INTEGER PRIMARY KEY, v, k UNIQUE);\
                 INSERT INTO s SELECT value, value, value FROM nums WHERE value <= 200;",
        statements: &[
            "BEGIN",
            "UPDATE s SET v = v * 2",
            "SAVEPOINT a",
            "UPDATE s SET v = v + 1000 WHERE id > 100",
            "UPDATE s SET k = 5 WHERE id = 1",
            "ROLLBACK TO a",
            "UPDATE s SET v = -v WHERE id < 10",
            "RELEASE a",
            "COMMIT",
            "BEGIN",
            "UPDATE s SET v = 0",
            "ROLLBACK",
        ],
        dumps: &["SELECT id, v, k FROM s ORDER BY id"],
    },
    Case {
        name: "in_place_generated_and_where_subquery",
        schema: "CREATE TABLE g(id INTEGER PRIMARY KEY, a, b AS (a * 2) STORED, c AS (a + 1));\
                 CREATE INDEX g_b ON g(b);\
                 INSERT INTO g(id, a) SELECT value, value FROM nums WHERE value <= 150;",
        statements: &[
            "UPDATE g SET a = a + 1 WHERE b > 100",
            "UPDATE g SET a = a * 3 WHERE id IN (SELECT id FROM g WHERE b % 4 = 0)",
            "UPDATE g SET a = (SELECT max(a) FROM g) WHERE id = 7",
        ],
        dumps: &["SELECT id, a, b, c FROM g ORDER BY id"],
    },
    Case {
        name: "from_superseded_match_constraints_rowid",
        schema: "CREATE TABLE t(id INTEGER PRIMARY KEY, v NOT NULL, k UNIQUE);\
                 INSERT INTO t VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3);\
                 CREATE TABLE s(seq INTEGER PRIMARY KEY, tid, nv, nk);\
                 INSERT INTO s(tid, nv, nk) VALUES (1, NULL, 10), (1, 'x', 11), \
                     (2, 'y', 3), (2, 'z', 20), (3, 'q', 30), (3, NULL, 31);",
        statements: &[
            "UPDATE t SET v = s.nv FROM s WHERE s.tid = t.id AND t.id = 1",
            "UPDATE OR IGNORE t SET k = s.nk FROM s WHERE s.tid = t.id AND t.id = 2",
            "UPDATE OR IGNORE t SET v = s.nv FROM s WHERE s.tid = t.id AND t.id = 3",
            "UPDATE t SET v = q.nv || '!' FROM (SELECT * FROM s ORDER BY seq DESC) AS q \
                 WHERE q.tid = t.id AND q.nv IS NOT NULL RETURNING id, v",
        ],
        dumps: &["SELECT id, v, k FROM t ORDER BY id"],
    },
    Case {
        name: "from_superseded_match_constraints_without_rowid",
        schema: "CREATE TABLE t(id TEXT PRIMARY KEY COLLATE NOCASE, v NOT NULL, k UNIQUE) WITHOUT ROWID;\
                 INSERT INTO t VALUES ('a', 'a', 1), ('b', 'b', 2), ('c', 'c', 3);\
                 CREATE TABLE s(seq INTEGER PRIMARY KEY, tid, nv, nk);\
                 INSERT INTO s(tid, nv, nk) VALUES ('A', NULL, 10), ('a', 'x', 11), \
                     ('b', 'y', 3), ('B', 'z', 20), ('c', 'q', 30), ('C', NULL, 31);",
        statements: &[
            "UPDATE t SET v = s.nv FROM s WHERE s.tid = t.id AND t.id = 'a'",
            "UPDATE OR IGNORE t SET k = s.nk FROM s WHERE s.tid = t.id AND t.id = 'b'",
            "UPDATE OR IGNORE t SET v = s.nv FROM s WHERE s.tid = t.id AND t.id = 'c'",
            "UPDATE t SET v = s.nv || '!' FROM s WHERE s.tid = t.id AND s.nv IS NOT NULL \
                 RETURNING id, v",
        ],
        dumps: &["SELECT id, v, k FROM t ORDER BY id"],
    },
    Case {
        name: "from_self_join_and_cte",
        schema: "CREATE TABLE t(id INTEGER PRIMARY KEY, v, w);\
                 CREATE INDEX t_v ON t(v);\
                 INSERT INTO t SELECT value, value * 10, value % 3 FROM nums WHERE value <= 300;",
        statements: &[
            "UPDATE t SET v = n.v + 1 FROM t AS n WHERE n.id = t.id + 1",
            "WITH agg AS (SELECT w, sum(v) AS sv FROM t GROUP BY w) \
                 UPDATE t SET v = agg.sv FROM agg WHERE agg.w = t.w AND t.id % 50 = 0",
            "UPDATE t SET id = t.id + 1000 FROM (SELECT 1 AS x) WHERE t.id % 97 = 0",
            "UPDATE t SET rowid = s.x FROM (SELECT 2.5 AS x UNION ALL SELECT 5000) AS s WHERE t.id = 1",
        ],
        dumps: &["SELECT id, v, w FROM t ORDER BY id"],
    },
    Case {
        name: "from_with_triggers_multi_match",
        schema: "CREATE TABLE log(seq INTEGER PRIMARY KEY, msg);\
                 CREATE TABLE t(id INTEGER PRIMARY KEY, v, k UNIQUE);\
                 INSERT INTO t VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3), (4, 'd', 4);\
                 CREATE TABLE s(seq INTEGER PRIMARY KEY, tid, nv);\
                 INSERT INTO s(tid, nv) VALUES (1, 'x1'), (1, 'y1'), (2, 'x2'), (3, 'x3'), (3, 'y3'), (3, 'z3');\
                 CREATE TRIGGER t_bu BEFORE UPDATE ON t BEGIN \
                     INSERT INTO log(msg) VALUES ('bu ' || OLD.id || ' ' || OLD.v || '>' || NEW.v); \
                     DELETE FROM s WHERE tid = OLD.id + 1; END;\
                 CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN \
                     INSERT INTO log(msg) VALUES ('au ' || NEW.id || ' ' || NEW.v || ' ' || NEW.k); END;",
        statements: &[
            "UPDATE t SET v = s.nv FROM s WHERE s.tid = t.id RETURNING id, v",
            "UPDATE OR IGNORE t SET k = t.k + 1 FROM (SELECT 1) WHERE t.id < 3",
            "WITH c AS (SELECT id AS cid, id * 100 AS nk FROM t) \
                 UPDATE t SET k = c.nk FROM c WHERE c.cid = t.id",
        ],
        dumps: &["SELECT seq, msg FROM log ORDER BY seq", "SELECT id, v, k FROM t ORDER BY id",
                 "SELECT seq, tid, nv FROM s ORDER BY seq"],
    },
    Case {
        name: "from_with_triggers_multi_match_without_rowid",
        schema: "CREATE TABLE log(seq INTEGER PRIMARY KEY, msg);\
                 CREATE TABLE t(id TEXT PRIMARY KEY COLLATE NOCASE, v, k UNIQUE) WITHOUT ROWID;\
                 INSERT INTO t VALUES ('a', 'a', 1), ('b', 'b', 2), ('c', 'c', 3);\
                 CREATE TABLE s(seq INTEGER PRIMARY KEY, tid, nv);\
                 INSERT INTO s(tid, nv) VALUES ('a', 'x1'), ('A', 'y1'), ('b', 'x2'), ('c', 'x3'), ('C', 'y3');\
                 CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN \
                     INSERT INTO log(msg) VALUES ('au ' || NEW.id || ' ' || OLD.v || '>' || NEW.v); END;",
        statements: &[
            "UPDATE t SET v = s.nv FROM s WHERE s.tid = t.id RETURNING id, v",
            "UPDATE t SET v = q.nv || '!' FROM (SELECT * FROM s ORDER BY seq DESC) AS q WHERE q.tid = t.id",
            "UPDATE t SET v = s.nv FROM s WHERE s.tid = t.id AND t.id = 'a'",
        ],
        dumps: &["SELECT seq, msg FROM log ORDER BY seq", "SELECT id, v, k FROM t ORDER BY id"],
    },
    Case {
        name: "from_with_triggers_single_target_multi_match",
        schema: "CREATE TABLE log(seq INTEGER PRIMARY KEY, msg);\
                 CREATE TABLE t(id INTEGER PRIMARY KEY, v);\
                 INSERT INTO t VALUES (1, 'a'), (2, 'b');\
                 CREATE TABLE s(seq INTEGER PRIMARY KEY, tid, nv);\
                 INSERT INTO s(tid, nv) VALUES (1, 'x1'), (1, 'y1'), (1, 'z1');\
                 CREATE TRIGGER t_bu BEFORE UPDATE ON t BEGIN \
                     INSERT INTO log(msg) VALUES ('bu ' || OLD.id || ' ' || OLD.v || '>' || NEW.v); END;\
                 CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN \
                     INSERT INTO log(msg) VALUES ('au ' || NEW.id || ' ' || NEW.v); END;",
        statements: &[
            "UPDATE t SET v = s.nv FROM s WHERE s.tid = t.id RETURNING id, v",
        ],
        dumps: &["SELECT seq, msg FROM log ORDER BY seq", "SELECT id, v FROM t ORDER BY id"],
    },
    Case {
        name: "or_rollback_semantics",
        schema: "CREATE TABLE log(seq INTEGER PRIMARY KEY, msg);\
                 CREATE TABLE t(id INTEGER PRIMARY KEY, k UNIQUE, v);\
                 INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd');\
                 CREATE TABLE r(id INTEGER PRIMARY KEY, k UNIQUE);\
                 INSERT INTO r VALUES (1, 1), (2, 2), (3, 3);\
                 CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN \
                     INSERT INTO log(msg) VALUES ('au ' || NEW.id || ' ' || NEW.k); END;\
                 CREATE TABLE q(id INTEGER PRIMARY KEY, k UNIQUE);\
                 INSERT INTO q VALUES (1, 1), (2, 2);\
                 CREATE TRIGGER q_ai AFTER INSERT ON log BEGIN \
                     UPDATE OR ROLLBACK q SET k = 2 WHERE id = 1 AND NEW.msg = 'boom'; END;",
        statements: &[
            "UPDATE OR ROLLBACK t SET k = k + 1",
            "BEGIN",
            "INSERT INTO r VALUES (10, 10)",
            "UPDATE OR ROLLBACK r SET k = 1 WHERE id = 2",
            "COMMIT",
            "BEGIN",
            "INSERT INTO r VALUES (11, 11)",
            "UPDATE OR ROLLBACK t SET k = 4 WHERE id < 3",
            "COMMIT",
            "BEGIN",
            "INSERT INTO r VALUES (12, 12)",
            "INSERT INTO log(msg) VALUES ('boom')",
            "COMMIT",
            "INSERT INTO log(msg) VALUES ('boom')",
            "UPDATE OR ROLLBACK r SET k = k + 1 WHERE id >= 2",
        ],
        dumps: &["SELECT seq, msg FROM log ORDER BY seq", "SELECT id, k, v FROM t ORDER BY id",
                 "SELECT id, k FROM r ORDER BY id", "SELECT id, k FROM q ORDER BY id"],
    },
    Case {
        name: "conflict_restore_overflow_expr_partial_upsert",
        schema: "CREATE TABLE c(id INTEGER PRIMARY KEY, k UNIQUE, big, e, p);\
                 CREATE INDEX c_e ON c(lower(e));\
                 CREATE INDEX c_p ON c(p) WHERE p > 5;\
                 CREATE INDEX c_big ON c(substr(big, 1, 3), p);\
                 INSERT INTO c SELECT value, value, CASE WHEN value % 2 = 0 THEN zeroblob(6000) ELSE 'sm' END, \
                     'E' || value, value % 10 FROM nums WHERE value <= 120;",
        statements: &[
            "UPDATE OR IGNORE c SET k = 5, big = zeroblob(7000), e = 'X', p = 9 WHERE id BETWEEN 2 AND 12",
            "UPDATE OR FAIL c SET k = k + 200, big = 'tiny', p = p + 1 WHERE id % 3 = 0 AND id < 60",
            "UPDATE OR FAIL c SET k = 7, e = 'f' WHERE id > 100",
            "BEGIN",
            "UPDATE c SET p = p + 100, e = upper(e) WHERE id < 20",
            "UPDATE OR ABORT c SET k = k + 1, big = zeroblob(9000) WHERE id > 110",
            "UPDATE c SET e = 'after' WHERE id = 1",
            "COMMIT",
            "INSERT INTO c(id, k, big, e, p) VALUES (500, 4, 'u', 'U', 7) \
                 ON CONFLICT(k) DO UPDATE SET k = 3, e = 'up', p = 8",
            "INSERT INTO c(id, k, big, e, p) VALUES (501, 50, 'u', 'U', 7) \
                 ON CONFLICT(k) DO UPDATE SET big = zeroblob(8000), e = 'up2', p = 6",
            "UPDATE OR REPLACE c SET k = 30, big = zeroblob(6500) WHERE id = 31",
        ],
        dumps: &["SELECT id, k, length(big), e, p FROM c ORDER BY id",
                 "SELECT count(*) FROM c WHERE p > 5", "SELECT count(*) FROM c WHERE lower(e) = 'x'"],
    },
    Case {
        name: "pinned_where_edges",
        schema: "CREATE TABLE log(seq INTEGER PRIMARY KEY, msg);\
                 CREATE TABLE p(id INTEGER PRIMARY KEY, v);\
                 INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c');\
                 CREATE TRIGGER p_au AFTER UPDATE ON p BEGIN INSERT INTO log(msg) VALUES ('p ' || NEW.id || NEW.v); END;\
                 CREATE TABLE d(id INTEGER PRIMARY KEY DESC, v);\
                 INSERT INTO d VALUES (1, 'a'), (2, 'b');\
                 CREATE TRIGGER d_au AFTER UPDATE ON d BEGIN INSERT INTO log(msg) VALUES ('d ' || NEW.id || NEW.v); END;\
                 CREATE TABLE w(a TEXT, b TEXT COLLATE NOCASE, v, PRIMARY KEY (a, b)) WITHOUT ROWID;\
                 INSERT INTO w VALUES ('x', 'Q', 1), ('x', 'r', 2), ('y', 'q', 3);\
                 CREATE TRIGGER w_au AFTER UPDATE ON w BEGIN INSERT INTO log(msg) VALUES ('w ' || NEW.a || NEW.b || NEW.v); END;\
                 CREATE TABLE o(oid INTEGER, v);\
                 INSERT INTO o VALUES (1, 'a'), (1, 'b'), (2, 'c');\
                 CREATE TRIGGER o_au AFTER UPDATE ON o BEGIN INSERT INTO log(msg) VALUES ('o ' || NEW.oid || NEW.v); END;",
        statements: &[
            "UPDATE p SET v = v || '1' WHERE id = 1.0",
            "UPDATE p SET v = v || '2' WHERE id = '2'",
            "UPDATE p SET v = v || '3' WHERE id = 1.5",
            "UPDATE p SET v = v || '4' WHERE rowid = 3 AND _rowid_ = 3",
            "UPDATE d SET v = v || '5' WHERE id = 2",
            "UPDATE w SET v = v + 10 WHERE a = 'x' AND b = 'q'",
            "UPDATE w SET v = v + 100 WHERE b = 'Q' AND a = 'x' COLLATE NOCASE",
            "UPDATE o SET v = v || '6' WHERE oid = 1",
            "DELETE FROM o WHERE oid = 1",
        ],
        dumps: &["SELECT seq, msg FROM log ORDER BY seq", "SELECT id, v FROM p ORDER BY id",
                 "SELECT id, v FROM d ORDER BY id", "SELECT a, b, v FROM w ORDER BY a, b",
                 "SELECT rowid, oid, v FROM o ORDER BY rowid"],
    },
];

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Integer(v) => format!("i:{v}"),
        SqliteValue::Float(v) => format!("f:{v}"),
        SqliteValue::Text(v) => format!("t:{v}"),
        SqliteValue::Blob(v) => format!("b:{}", v.len()),
        SqliteValue::Null => "null".to_owned(),
    }
}

fn render_rusqlite(value: rusqlite::types::ValueRef<'_>) -> String {
    match value {
        rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
        rusqlite::types::ValueRef::Real(v) => format!("f:{v}"),
        rusqlite::types::ValueRef::Text(v) => format!("t:{}", String::from_utf8_lossy(v)),
        rusqlite::types::ValueRef::Blob(v) => format!("b:{}", v.len()),
        rusqlite::types::ValueRef::Null => "null".to_owned(),
    }
}

fn render_rows(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect::<Vec<_>>().join("|"))
        .collect()
}

fn outcome_error(message: &str) -> String {
    for kind in ["UNIQUE", "NOT NULL", "CHECK", "datatype mismatch"] {
        if message.contains(kind) {
            return format!("error:{kind}");
        }
    }
    format!("error:{message}")
}

const NUMS: &str = "CREATE TABLE nums(value INTEGER PRIMARY KEY); WITH RECURSIVE g(v) AS (SELECT 1 UNION ALL SELECT v + 1 FROM g WHERE v < 500) INSERT INTO nums SELECT v FROM g;";

fn stock_transcript(case: &Case) -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(NUMS).expect("nums");
    conn.execute_batch(case.schema).expect("stock schema");
    let query = |sql: &str| -> Result<Vec<String>, String> {
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let width = stmt.column_count();
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            out.push(
                (0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("cell")))
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
        Ok(out)
    };
    let mut transcript = Vec::new();
    for sql in case.statements {
        let result = query(sql);
        let changes = query("SELECT changes()").expect("changes");
        transcript.push(match result {
            Ok(mut rows) => {
                rows.sort();
                format!("{sql} => ok {rows:?} changes={changes:?}")
            }
            Err(message) => format!("{sql} => {} changes={changes:?}", outcome_error(&message)),
        });
    }
    for sql in case.dumps {
        transcript.push(format!("{sql} => {:?}", query(sql).expect("stock dump")));
    }
    transcript
}

async fn fsqlite_transcript(conn: &Connection, case: &Case) -> Vec<String> {
    conn.execute_batch(NUMS).await.expect("nums");
    conn.execute_batch(case.schema).await.expect("schema");
    let mut transcript = Vec::new();
    for sql in case.statements {
        let result = conn.query(sql).await;
        let changes = render_rows(&conn.query("SELECT changes()").await.expect("changes"));
        transcript.push(match result {
            Ok(rows) => {
                let mut rows = render_rows(&rows);
                rows.sort();
                format!("{sql} => ok {rows:?} changes={changes:?}")
            }
            Err(error) => format!("{sql} => {} changes={changes:?}", outcome_error(&error.to_string())),
        });
    }
    for sql in case.dumps {
        let rows = conn.query(sql).await.expect("dump");
        transcript.push(format!("{sql} => {:?}", render_rows(&rows)));
    }
    transcript
}

#[test]
fn update_family_review_probe() {
    let mut failures = Vec::new();
    for case in CASES {
        let want = stock_transcript(case);
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().expect("temp dir");
            let path = dir.path().join("probe.db");
            let target = if file_backed {
                path.to_str().expect("utf-8").to_owned()
            } else {
                ":memory:".to_owned()
            };
            let out = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let sink = std::sync::Arc::clone(&out);
            asupersync::test_utils::run_test(|| async move {
                let conn = Connection::open(&target).await.expect("open");
                *sink.lock().expect("sink") = fsqlite_transcript(&conn, case).await;
            });
            let got = std::mem::take(&mut *out.lock().expect("out"));
            for (line, (g, w)) in got.iter().zip(&want).enumerate() {
                if g != w {
                    failures.push(format!(
                        "[{} file={file_backed}] line {line}\n  got:  {g}\n  want: {w}",
                        case.name
                    ));
                }
            }
            if file_backed {
                let checked = rusqlite::Connection::open(&path).expect("stock open");
                let verdict: String = checked
                    .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                    .expect("integrity_check");
                if verdict != "ok" {
                    failures.push(format!("[{}] integrity_check: {verdict}", case.name));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{} divergences:\n{}", failures.len(), failures.join("\n"));
}

#[derive(Clone)]
enum P {
    Int(i64),
    Text(&'static str),
}

const PARAM_SCHEMA: &str = "CREATE TABLE log(seq INTEGER PRIMARY KEY, msg);\
    CREATE TABLE t(id INTEGER PRIMARY KEY, v);\
    INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');\
    CREATE TABLE s(seq INTEGER PRIMARY KEY, tid, nv);\
    INSERT INTO s(tid, nv) VALUES (1, 'x1'), (2, 'x2'), (2, 'y2'), (3, 'x3');\
    CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN \
        INSERT INTO log(msg) VALUES ('au ' || NEW.id || ' ' || OLD.v || '>' || NEW.v); END;\
    CREATE TRIGGER t_bd BEFORE DELETE ON t BEGIN \
        INSERT INTO log(msg) VALUES ('bd ' || OLD.id || ' n=' || (SELECT count(*) FROM t)); END;\
    CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN \
        INSERT INTO log(msg) VALUES ('ad ' || OLD.id); END;";

fn param_statements() -> Vec<(&'static str, Vec<P>)> {
    let mut wide = vec![P::Int(0); 32766];
    wide[0] = P::Int(1);
    wide[32765] = P::Text("w:");
    let mut wide_from = vec![P::Int(0); 32766];
    wide_from[32765] = P::Text("f:");
    vec![
        ("UPDATE t SET v = :a || v WHERE id > :b RETURNING id, v, :a", vec![P::Text("n:"), P::Int(2)]),
        ("UPDATE t SET v = ?3 || v WHERE id > ?1 RETURNING id, ?2", vec![P::Int(1), P::Text("r"), P::Text("p:")]),
        ("UPDATE t SET v = ? || v WHERE id IN (?, ?) RETURNING ?, id", vec![P::Text("q:"), P::Int(1), P::Int(4), P::Text("ret")]),
        ("UPDATE t SET v = ?32766 || v WHERE id > ?1", wide),
        ("UPDATE t SET v = s.nv || ?32766 FROM s WHERE s.tid = t.id", wide_from),
        ("UPDATE t SET v = s.nv || ? FROM s WHERE s.tid = t.id AND s.seq > ? RETURNING t.id, t.v, ?", vec![P::Text("!"), P::Int(1), P::Text("r2")]),
        ("UPDATE t SET v = :x || s.nv FROM s WHERE s.tid = t.id AND s.nv <> :x", vec![P::Text("x2")]),
        ("DELETE FROM t WHERE id >= ?1 AND v <> ?2 RETURNING id, ?2", vec![P::Int(2), P::Text("zzz")]),
    ]
}

fn param_render(sql: &str, result: Result<Vec<String>, String>, changes: Vec<String>) -> String {
    match result {
        Ok(mut rows) => {
            rows.sort();
            format!("{sql} => ok {rows:?} changes={changes:?}")
        }
        Err(message) => format!("{sql} => {} changes={changes:?}", outcome_error(&message)),
    }
}

fn param_stock() -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(PARAM_SCHEMA).expect("schema");
    let query = |sql: &str, params: &[P]| -> Result<Vec<String>, String> {
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let width = stmt.column_count();
        let values: Vec<rusqlite::types::Value> = params
            .iter()
            .map(|p| match p {
                P::Int(v) => rusqlite::types::Value::Integer(*v),
                P::Text(v) => rusqlite::types::Value::Text((*v).to_owned()),
            })
            .collect();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(values))
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            out.push(
                (0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("cell")))
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
        Ok(out)
    };
    let mut transcript = Vec::new();
    for (sql, params) in param_statements() {
        let result = query(sql, &params);
        let changes = query("SELECT changes()", &[]).expect("changes");
        transcript.push(param_render(sql, result, changes));
    }
    for sql in ["SELECT seq, msg FROM log ORDER BY seq", "SELECT id, v FROM t ORDER BY id"] {
        transcript.push(format!("{sql} => {:?}", query(sql, &[]).expect("dump")));
    }
    transcript
}

async fn param_fsqlite(conn: &Connection) -> Vec<String> {
    conn.execute_batch(PARAM_SCHEMA).await.expect("schema");
    let mut transcript = Vec::new();
    for (sql, params) in param_statements() {
        let bound: Vec<SqliteValue> = params
            .iter()
            .map(|p| match p {
                P::Int(v) => SqliteValue::Integer(*v),
                P::Text(v) => SqliteValue::Text((*v).into()),
            })
            .collect();
        let result = conn
            .query_with_params(sql, &bound)
            .await
            .map(|rows| render_rows(&rows))
            .map_err(|e| e.to_string());
        let changes = render_rows(&conn.query("SELECT changes()").await.expect("changes"));
        transcript.push(param_render(sql, result, changes));
    }
    for sql in ["SELECT seq, msg FROM log ORDER BY seq", "SELECT id, v FROM t ORDER BY id"] {
        let rows = conn.query(sql).await.expect("dump");
        transcript.push(format!("{sql} => {:?}", render_rows(&rows)));
    }
    transcript
}

#[test]
fn update_family_review_probe_params() {
    let want = param_stock();
    let mut failures = Vec::new();
    for file_backed in [false, true] {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("params.db");
        let target = if file_backed {
            path.to_str().expect("utf-8").to_owned()
        } else {
            ":memory:".to_owned()
        };
        let out = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = std::sync::Arc::clone(&out);
        asupersync::test_utils::run_test(|| async move {
            let conn = Connection::open(&target).await.expect("open");
            *sink.lock().expect("sink") = param_fsqlite(&conn).await;
        });
        let got = std::mem::take(&mut *out.lock().expect("out"));
        for (line, (g, w)) in got.iter().zip(&want).enumerate() {
            if g != w {
                failures.push(format!("[params file={file_backed}] line {line}\n  got:  {g}\n  want: {w}"));
            }
        }
        assert_eq!(got.len(), want.len(), "transcript length");
    }
    assert!(failures.is_empty(), "{} divergences:\n{}", failures.len(), failures.join("\n"));
}
