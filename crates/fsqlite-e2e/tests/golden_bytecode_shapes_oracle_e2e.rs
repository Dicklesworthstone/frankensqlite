//! Oracle-parity e2e for the SQL shapes pinned by
//! `fsqlite-vdbe/tests/golden_bytecode_snapshots.rs`.
//!
//! Those goldens pin OPCODES, which means a codegen change shows up there as a
//! diff long before anyone knows whether the diff is an improvement or a
//! regression. This file is the companion that answers that question: it runs
//! the golden families' exact SQL, over the goldens' exact schema (same columns,
//! same declared affinities, same indexes), against BOTH FrankenSQLite and
//! bundled stock SQLite (`rusqlite`), and requires identical results and
//! identical resulting table state.
//!
//! Three accepted codegen changes are covered here, each of which moved a
//! golden:
//!
//! * `0bbe20448` — a JOIN projecting an INTEGER PRIMARY KEY now emits `Rowid`
//!   instead of `Column <cur> 0`. Stock does the same, and the old form
//!   projected NULL for TEMP / shadowed-main tables. Covered by the join
//!   families, including a join whose ON term reads the IPK.
//! * `5f6920022` (GH #169) — DML applies column affinity BEFORE CHECK/NOT NULL,
//!   which adds an `Affinity` op ahead of the index-delete/rewrite in UPDATE.
//!   Covered by the update/delete family, including a CHECK constraint and a
//!   TEXT-affinity column fed an integer.
//! * `9847fb9d1` (bd-aap9u) — each ON CONFLICT clause probes its own target
//!   (`NotExists` + `Goto`), and the trailing plain insert runs under the
//!   statement-level algorithm (`Insert` p5 `OE_ABORT`) instead of a blanket
//!   `OE_IGNORE`. Covered by the upsert family, including the untargeted
//!   conflict that must now raise rather than be swallowed.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
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
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
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

/// The goldens' schema, spelled as SQL. Column order, declared types (hence
/// affinities `XDBBD` / `XB` / `XDD`) and index sets match `snapshot_schema()`
/// in `golden_bytecode_snapshots.rs`.
const SCHEMA: &[&str] = &[
    "CREATE TABLE categories(id INTEGER PRIMARY KEY, name TEXT)",
    "CREATE INDEX idx_categories_name ON categories(name)",
    "CREATE TABLE docs(id INTEGER PRIMARY KEY, category_id INTEGER, title TEXT, body TEXT, score INTEGER)",
    "CREATE INDEX idx_docs_category ON docs(category_id)",
    "CREATE INDEX idx_docs_title ON docs(title)",
    "CREATE TABLE events(id INTEGER PRIMARY KEY, category_id INTEGER, score INTEGER)",
    "CREATE INDEX idx_events_category ON events(category_id)",
];

const SEED: &[&str] = &[
    "INSERT INTO categories(id, name) VALUES (3, 'docs'), (7, 'guides'), (9, 'notes')",
    "INSERT INTO docs(id, category_id, title, body, score) VALUES \
        (1, 7, 'alpha', 'alpha body', 5), \
        (2, 7, 'beta', 'beta body', 12), \
        (3, 3, 'gamma', 'gamma body', 2), \
        (4, 9, 'delta', 'delta body', 30), \
        (5, 7, NULL, NULL, NULL), \
        (6, 3, 'epsilon', 'epsilon body', 12)",
    "INSERT INTO events(id, category_id, score) VALUES \
        (10, 7, 1), (11, 7, 40), (12, 3, 11), (13, 9, 7), (14, 7, NULL), (15, 3, 4)",
];

/// Run `stmts` against both engines, asserting each statement's OK/ERROR outcome
/// agrees, then compare `queries` results row-for-row.
async fn scenario(extra_schema: &[&str], stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");

    for s in SCHEMA.iter().chain(extra_schema).chain(SEED).chain(stmts) {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }

    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
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
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

/// Full table state, so a write scenario compares the whole resulting image and
/// not just the rows a narrow SELECT happens to touch.
const STATE: &[&str] = &[
    "SELECT id, category_id, title, body, score FROM docs ORDER BY id",
    "SELECT id, name FROM categories ORDER BY id",
    "SELECT id, category_id, score FROM events ORDER BY id",
];

fn state_plus(extra: &[&'static str]) -> Vec<&'static str> {
    let mut v: Vec<&'static str> = STATE.to_vec();
    v.extend_from_slice(extra);
    v
}

// ---------------------------------------------------------------------------
// select_join_lookup / multi_join_three_way_ordered / multi_join_filtered
// (golden_vdbe_select_bytecode_recent_stub_shapes,
//  golden_vdbe_multi_table_join_bytecode_family)
//
// Both goldens moved because an is_ipk projection is now `Rowid`, not
// `Column <cur> 0`. The three-way join also reads `categories.id` via `Rowid`
// inside its ON terms, so equality routing is exercised too.
// ---------------------------------------------------------------------------

#[test]
fn join_lookup_ipk_projection_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[],
            &[
                // golden: select_join_lookup
                "SELECT docs.id, categories.name \
                 FROM docs JOIN categories ON docs.category_id = categories.id \
                 WHERE docs.category_id = 7 ORDER BY docs.id",
                // The IPK on the RHS of the join, and on both sides at once.
                "SELECT categories.id, docs.id \
                 FROM docs JOIN categories ON docs.category_id = categories.id \
                 ORDER BY categories.id, docs.id",
                // A category with no docs must still drop out (inner join), and
                // docs.id must not be NULL anywhere.
                "SELECT count(*), count(docs.id) \
                 FROM docs JOIN categories ON docs.category_id = categories.id",
                // `SELECT *` walks the Star loop, the other site 0bbe20448 changed.
                "SELECT * FROM docs JOIN categories ON docs.category_id = categories.id \
                 ORDER BY docs.id",
            ],
            "join_lookup_ipk_projection",
        )
        .await;
    });
}

#[test]
fn multi_table_join_ipk_projection_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[],
            &[
                // golden: multi_join_three_way_ordered
                "SELECT docs.id, categories.name, events.score \
                 FROM docs \
                 JOIN categories ON docs.category_id = categories.id \
                 JOIN events ON events.category_id = categories.id \
                 WHERE docs.category_id = 7 \
                 ORDER BY events.score DESC, docs.id",
                // golden: multi_join_filtered_predicates
                "SELECT categories.name, docs.title \
                 FROM categories \
                 JOIN docs ON docs.category_id = categories.id \
                 JOIN events ON events.category_id = docs.category_id \
                 WHERE events.score > 10 AND docs.category_id = 3 \
                 ORDER BY categories.name, docs.title",
                // Same three-way shape, but projecting every IPK in the join.
                "SELECT docs.id, categories.id, events.id \
                 FROM docs \
                 JOIN categories ON docs.category_id = categories.id \
                 JOIN events ON events.category_id = categories.id \
                 ORDER BY docs.id, categories.id, events.id",
            ],
            "multi_table_join_ipk_projection",
        )
        .await;
    });
}

// ---------------------------------------------------------------------------
// update_with_where_predicate / delete_with_where_predicate
// (golden_vdbe_update_delete_where_bytecode_family)
//
// The golden moved because UPDATE now applies column affinity BEFORE the
// CHECK/NOT NULL constraints and before the index-delete + row rewrite (GH #169).
// The scenarios below make that ordering observable: a CHECK constraint on a
// TEXT-affinity column fed an integer only agrees with stock if affinity ran
// first, and a rewrite that changes an indexed column must leave both indexes
// consistent.
// ---------------------------------------------------------------------------

#[test]
fn update_where_predicate_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &["UPDATE docs SET score = score + 1, title = 'updated' WHERE category_id = 7 AND score < 12"],
            &state_plus(&[
                // Reads that must be served through both touched indexes.
                "SELECT id, title FROM docs WHERE title = 'updated' ORDER BY id",
                "SELECT id, score FROM docs WHERE category_id = 7 ORDER BY id",
                "SELECT id, title FROM docs WHERE title > 'a' ORDER BY title, id",
            ]),
            "update_where_predicate",
        )
        .await;
    });
}

#[test]
fn update_affinity_runs_before_constraints_like_stock() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE aff(id INTEGER PRIMARY KEY, t TEXT NOT NULL CHECK(length(t) > 1), n INTEGER)",
                "CREATE INDEX idx_aff_t ON aff(t)",
            ],
            &[
                "INSERT INTO aff(id, t, n) VALUES (1, 'aa', 1), (2, 'bb', 2)",
                // 42 acquires TEXT affinity -> '42' -> length 2 -> CHECK passes.
                // If the CHECK saw the integer first it would compare differently.
                "UPDATE aff SET t = 42 WHERE id = 1",
                // 7 -> '7' -> length 1 -> CHECK must FAIL on both engines.
                "UPDATE aff SET t = 7 WHERE id = 2",
            ],
            &[
                "SELECT id, t, typeof(t), n FROM aff ORDER BY id",
                "SELECT id FROM aff WHERE t = '42'",
            ],
            "update_affinity_before_constraints",
        )
        .await;
    });
}

#[test]
fn delete_where_predicate_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &["DELETE FROM docs WHERE category_id = 7 AND score < 12"],
            &state_plus(&[
                "SELECT id FROM docs WHERE category_id = 7 ORDER BY id",
                "SELECT id, title FROM docs WHERE title > '' ORDER BY title, id",
            ]),
            "delete_where_predicate",
        )
        .await;
    });
}

// ---------------------------------------------------------------------------
// upsert_on_conflict_do_nothing / upsert_on_conflict_do_update
// (golden_vdbe_upsert_on_conflict_bytecode_family)
//
// The golden moved because DO NOTHING is now an explicit `NotExists` probe of
// its own conflict target followed by a `Goto` past the insert, and the
// fall-through plain insert runs at the statement-level algorithm (OE_ABORT)
// instead of a blanket OE_IGNORE. Both halves are observable: a conflict on the
// TARGET must be skipped silently, and a conflict on a constraint no clause
// targets must now RAISE.
// ---------------------------------------------------------------------------

#[test]
fn upsert_do_nothing_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[
                // golden: upsert_on_conflict_do_nothing (conflicting rowid)
                "INSERT INTO docs (id, category_id, title, body, score) \
                 VALUES (1, 2, 'title', 'body', 10) ON CONFLICT (id) DO NOTHING",
                // ...and the non-conflicting case, which must still insert.
                "INSERT INTO docs (id, category_id, title, body, score) \
                 VALUES (100, 2, 'title', 'body', 10) ON CONFLICT (id) DO NOTHING",
            ],
            &state_plus(&["SELECT id, title FROM docs WHERE id IN (1, 100) ORDER BY id"]),
            "upsert_do_nothing",
        )
        .await;
    });
}

#[test]
fn upsert_untargeted_conflict_raises_like_stock() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE u(id INTEGER PRIMARY KEY, k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO u(id, k, v) VALUES (1, 'a', 10), (2, 'b', 20)",
            ],
            &[
                // Conflicts on UNIQUE(k), which no clause targets: stock raises,
                // and so must we (this is the OE_IGNORE -> OE_ABORT half).
                "INSERT INTO u(id, k, v) VALUES (3, 'a', 30) ON CONFLICT (id) DO NOTHING",
                // Conflicts on the targeted PK: silently skipped.
                "INSERT INTO u(id, k, v) VALUES (1, 'c', 30) ON CONFLICT (id) DO NOTHING",
            ],
            &["SELECT id, k, v FROM u ORDER BY id"],
            "upsert_untargeted_conflict",
        )
        .await;
    });
}

#[test]
fn upsert_do_update_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[
                // golden: upsert_on_conflict_do_update, with the WHERE satisfied
                // (excluded.score 99 > docs.score 5) -> the row is rewritten.
                "INSERT INTO docs (id, category_id, title, body, score) \
                 VALUES (1, 2, 'new title', 'new body', 99) \
                 ON CONFLICT (id) DO UPDATE SET title = excluded.title, score = excluded.score \
                 WHERE excluded.score > docs.score",
                // ...and with the WHERE unsatisfied -> the DO UPDATE is skipped.
                "INSERT INTO docs (id, category_id, title, body, score) \
                 VALUES (4, 2, 'ignored', 'ignored', 1) \
                 ON CONFLICT (id) DO UPDATE SET title = excluded.title, score = excluded.score \
                 WHERE excluded.score > docs.score",
                // ...and with no conflict at all -> a plain insert.
                "INSERT INTO docs (id, category_id, title, body, score) \
                 VALUES (200, 2, 'fresh', 'fresh', 3) \
                 ON CONFLICT (id) DO UPDATE SET title = excluded.title, score = excluded.score \
                 WHERE excluded.score > docs.score",
            ],
            &state_plus(&[
                "SELECT id, title, score FROM docs WHERE id IN (1, 4, 200) ORDER BY id",
                "SELECT id FROM docs WHERE title = 'new title'",
            ]),
            "upsert_do_update",
        )
        .await;
    });
}

// ---------------------------------------------------------------------------
// Bound-parameter forms. The goldens compile `?1`/`?2`; the literal scenarios
// above compile constants. Run the parameterized statements too so the accepted
// opcode shapes are checked on the path the goldens actually render.
// ---------------------------------------------------------------------------

#[test]
fn parameterized_update_and_delete_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        for s in SCHEMA.iter().chain(SEED) {
            f.execute(s).await.expect("frank ddl/seed");
            r.execute_batch(s).expect("csql ddl/seed");
        }

        f.execute_with_params(
            "UPDATE docs SET score = score + 1, title = 'updated' WHERE category_id = ?1 AND score < ?2",
            &[SqliteValue::Integer(7), SqliteValue::Integer(12)],
        )
        .await
        .expect("frank update");
        r.execute(
            "UPDATE docs SET score = score + 1, title = 'updated' WHERE category_id = ?1 AND score < ?2",
            rusqlite::params![7, 12],
        )
        .expect("csql update");

        f.execute_with_params(
            "DELETE FROM docs WHERE category_id = ?1 AND score < ?2",
            &[SqliteValue::Integer(3), SqliteValue::Integer(12)],
        )
        .await
        .expect("frank delete");
        r.execute(
            "DELETE FROM docs WHERE category_id = ?1 AND score < ?2",
            rusqlite::params![3, 12],
        )
        .expect("csql delete");

        for q in STATE {
            assert_eq!(
                frank_rows(&f, q).await.expect("frank query"),
                sqlite_rows(&r, q).expect("csql query"),
                "parameterized update/delete diverged on `{q}`"
            );
        }
    });
}

#[test]
fn parameterized_upsert_do_update_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        for s in SCHEMA.iter().chain(SEED) {
            f.execute(s).await.expect("frank ddl/seed");
            r.execute_batch(s).expect("csql ddl/seed");
        }

        const UPSERT: &str = "INSERT INTO docs (id, category_id, title, body, score) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT (id) DO UPDATE SET title = excluded.title, score = excluded.score \
             WHERE excluded.score > docs.score";

        // (conflicting id, WHERE satisfied), (conflicting id, WHERE not
        // satisfied), (fresh id).
        let cases: [(i64, i64, &str, &str, i64); 3] = [
            (1, 2, "p new title", "p new body", 99),
            (4, 2, "p ignored", "p ignored", 1),
            (300, 2, "p fresh", "p fresh", 3),
        ];
        for (id, cat, title, body, score) in cases {
            f.execute_with_params(
                UPSERT,
                &[
                    SqliteValue::Integer(id),
                    SqliteValue::Integer(cat),
                    SqliteValue::Text(title.into()),
                    SqliteValue::Text(body.into()),
                    SqliteValue::Integer(score),
                ],
            )
            .await
            .unwrap_or_else(|e| panic!("frank upsert id={id}: {e}"));
            r.execute(UPSERT, rusqlite::params![id, cat, title, body, score])
                .unwrap_or_else(|e| panic!("csql upsert id={id}: {e}"));
        }

        for q in STATE {
            assert_eq!(
                frank_rows(&f, q).await.expect("frank query"),
                sqlite_rows(&r, q).expect("csql query"),
                "parameterized upsert diverged on `{q}`"
            );
        }
    });
}
