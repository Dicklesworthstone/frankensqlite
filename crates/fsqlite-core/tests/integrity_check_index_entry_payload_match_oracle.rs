#![recursion_limit = "512"]

//! bd-o499g: fsqlite `PRAGMA integrity_check` must agree with stock sqlite3 on a
//! valid, actively-written multi-column index.
//!
//! Split from bd-y2aog symptom 2. The checker must not false-flag "index `X`
//! entry for rowid N does not match the table row payload" on an index stock
//! validates clean.
//!
//! The checker (connection.rs) compares the index entry against a key record it
//! rebuilds from the table row with a RAW BYTE comparison. If the rebuild
//! serializes a key value differently than the live writer did (serial-type
//! width, TEXT/BLOB affinity, collation, NULLs, REAL), a semantically-valid
//! index false-flags while stock — which compares semantically — passes.
//!
//! This keeper drives several serial-type/affinity/collation shapes through
//! INSERT + UPDATE churn on multi-column plain / UNIQUE / partial indexes and
//! asserts fsqlite's integrity_check agrees with rusqlite ("ok").

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &str = "\
CREATE TABLE facts (
    id TEXT NOT NULL,
    period TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    amount REAL,
    tag TEXT COLLATE NOCASE,
    note TEXT
);
CREATE INDEX idx_identity ON facts(id, period, ordinal);
CREATE UNIQUE INDEX idx_unique_identity ON facts(id, ordinal);
CREATE INDEX idx_tag_nocase ON facts(tag);
CREATE INDEX idx_partial ON facts(amount) WHERE amount IS NOT NULL;";

// Serial-type variance: integers spanning every SQLite serial-type width, reals,
// texts (incl. NOCASE-collated), and NULLs — the values whose on-disk encoding
// is most likely to diverge from a naive rebuild.
const INSERTS: &[&str] = &[
    "INSERT INTO facts VALUES ('acct-1','2026-Q1',0,   0.0,      'Alpha', 'a')",
    "INSERT INTO facts VALUES ('acct-1','2026-Q1',1,   127.0,    'ALPHA', NULL)",
    "INSERT INTO facts VALUES ('acct-1','2026-Q2',128, 32767.5,  'beta',  'b')",
    "INSERT INTO facts VALUES ('acct-2','2026-Q1',32768, NULL,   'BETA',  NULL)",
    "INSERT INTO facts VALUES ('acct-2','2026-Q2',8388608, -1.0, NULL,    'c')",
    "INSERT INTO facts VALUES ('acct-3','2026-Q1',2147483647, 1e300, 'g', 'd')",
    "INSERT INTO facts VALUES ('acct-3','2026-Q3',9223372036854775807, -0.0, '', 'e')",
];

// Rewrite index entries: UPDATE key columns (moves entries), non-key columns
// (rewrites payloads), and toggle the partial-index predicate.
const UPDATES: &[&str] = &[
    "UPDATE facts SET ordinal = ordinal + 1000 WHERE id = 'acct-1'",
    "UPDATE facts SET amount = amount * 2 WHERE amount IS NOT NULL",
    "UPDATE facts SET amount = NULL WHERE ordinal = 128 + 1000",
    "UPDATE facts SET amount = 42.0 WHERE amount IS NULL AND id = 'acct-2'",
    "UPDATE facts SET tag = 'ReTagged' WHERE tag = 'g'",
    "UPDATE facts SET period = '2026-Q4' WHERE id = 'acct-3'",
];

fn stock_integrity() -> String {
    let c = rusqlite::Connection::open_in_memory().expect("open stock");
    c.execute_batch(SETUP).expect("stock setup");
    for stmt in INSERTS.iter().chain(UPDATES.iter()) {
        c.execute(stmt, [])
            .unwrap_or_else(|e| panic!("stock `{stmt}`: {e}"));
    }
    c.query_row("PRAGMA integrity_check;", [], |r| r.get::<_, String>(0))
        .expect("stock integrity_check")
}

#[test]
fn integrity_check_agrees_with_stock_on_serial_type_variant_indexes() {
    // Premise: stock sqlite3 finds this actively-written shape consistent.
    assert_eq!(
        stock_integrity(),
        "ok",
        "premise: stock integrity_check is ok"
    );

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open fsqlite");
        for stmt in SETUP.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            conn.execute(stmt).await.expect("fsqlite setup");
        }
        for stmt in INSERTS.iter().chain(UPDATES.iter()) {
            conn.execute(stmt)
                .await
                .unwrap_or_else(|e| panic!("fsqlite `{stmt}`: {e:?}"));
        }

        let rows = conn
            .query("PRAGMA integrity_check;")
            .await
            .expect("fsqlite integrity_check");
        let report = match &rows[0].values()[0] {
            SqliteValue::Text(s) => s.as_ref().to_owned(),
            other => panic!("integrity_check did not return text: {other:?}"),
        };
        assert_eq!(
            report, "ok",
            "fsqlite integrity_check must agree with stock (bd-o499g); \
             a byte-strict index entry/payload check false-flags a valid index"
        );
    });
}

fn render_fsq(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => {
            let mut hex = String::with_capacity(b.len() * 2 + 3);
            hex.push_str("X'");
            for byte in b.iter() {
                use std::fmt::Write as _;
                let _ = write!(&mut hex, "{byte:02X}");
            }
            hex.push('\'');
            hex
        }
    }
}

fn render_stock(v: rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => {
            let mut hex = String::with_capacity(b.len() * 2 + 3);
            hex.push_str("X'");
            for byte in b {
                use std::fmt::Write as _;
                let _ = write!(&mut hex, "{byte:02X}");
            }
            hex.push('\'');
            hex
        }
    }
}

async fn run_exec_mode(conn: &Connection, sql: &str, prepared: bool) {
    if prepared {
        let stmt = conn
            .prepare(sql)
            .await
            .unwrap_or_else(|e| panic!("fsq prepare `{sql}`: {e:?}"));
        stmt.execute()
            .await
            .unwrap_or_else(|e| panic!("fsq prepared exec `{sql}`: {e:?}"));
    } else {
        conn.execute(sql)
            .await
            .unwrap_or_else(|e| panic!("fsq exec `{sql}`: {e:?}"));
    }
}

async fn run_query_mode(conn: &Connection, sql: &str, prepared: bool) -> Vec<Vec<String>> {
    let rows = if prepared {
        let stmt = conn
            .prepare(sql)
            .await
            .unwrap_or_else(|e| panic!("fsq prepare query `{sql}`: {e:?}"));
        stmt.query()
            .await
            .unwrap_or_else(|e| panic!("fsq prepared query `{sql}`: {e:?}"))
    } else {
        conn.query(sql)
            .await
            .unwrap_or_else(|e| panic!("fsq query `{sql}`: {e:?}"))
    };
    rows.iter()
        .map(|row| row.values().iter().map(render_fsq).collect())
        .collect()
}

fn stock_query_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("stock prepare `{sql}`: {e}"));
    let ncols = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(ncols);
        for i in 0..ncols {
            out.push(render_stock(row.get_unwrap::<_, rusqlite::types::Value>(i)));
        }
        Ok(out)
    })
    .unwrap_or_else(|e| panic!("stock query `{sql}`: {e}"))
    .map(Result::unwrap)
    .collect()
}

/// bd-0t3y0: an index on a column whose name contains a space (or punctuation /
/// SQL keyword) must store and probe the column's real value rather than NULL
/// across INSERT, UPDATE, REPLACE, UPSERT, DELETE, composite/expression/partial
/// indexes, UNIQUE autoindexes, and ALTER TABLE RENAME COLUMN to a spaced name,
/// in both `:memory:` and file-backed DBs, ad hoc and prepared, with both
/// fsqlite and stock SQLite 3.54.1 reporting `PRAGMA integrity_check = ok`.
#[test]
fn bd_0t3y0_spaced_column_index_keys_match_stock_and_pass_integrity() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        for (idx, (use_file, prepared)) in [
            (false, false),
            (false, true),
            (true, false),
            (true, true),
        ]
        .into_iter()
        .enumerate()
        {
            let label = format!("file={use_file}, prepared={prepared}");
            let db_path = dir.path().join(format!("bd_0t3y0_{idx}.db"));
            let open_target = if use_file {
                db_path.to_str().expect("utf8 path").to_owned()
            } else {
                ":memory:".to_owned()
            };

            let f = Connection::open(&open_target)
                .await
                .unwrap_or_else(|e| panic!("open fsq ({label}): {e:?}"));
            let s = rusqlite::Connection::open_in_memory().expect("open stock");

            let ddl_and_dml = [
                // 1. Exact bd-0t3y0 repro + composite + expression + partial index
                //    + tricky non-identifier column names (`a+b`, `select`, `null`, `1`, `na"me`).
                "CREATE TABLE p (id INTEGER PRIMARY KEY, [na me] TEXT, w INT, [a+b] INT, [select] TEXT, [null] TEXT, [1] TEXT, \"na\"\"me\" TEXT);",
                "CREATE INDEX pi ON p ([na me]);",
                "CREATE INDEX pi_comp ON p ([na me], w);",
                "CREATE INDEX pi_comp_rev ON p (w, \"na me\");",
                "CREATE INDEX pi_expr ON p (lower([na me]));",
                "CREATE INDEX pi_part ON p ([na me]) WHERE [na me] IS NOT NULL;",
                "CREATE INDEX pi_weird ON p ([a+b], [select], [null], [1], \"na\"\"me\");",
                "INSERT INTO p VALUES (1, 'a', 3, 10, 'kw', 'not_null', 'one', 'qt1');",
                "INSERT INTO p VALUES (2, 'B', 7, 20, 'kw2', 'nn2', 'two', 'qt2');",
                "INSERT INTO p VALUES (3, NULL, 9, 30, 'kw3', NULL, 'three', 'qt3');",
                // 2. UNIQUE autoindex on double-quoted spaced column + REPLACE + UPSERT + UPDATE + DELETE.
                "CREATE TABLE u (id INTEGER PRIMARY KEY, \"na me\" TEXT UNIQUE, [w col] INT);",
                "CREATE INDEX u_comp ON u (\"na me\", [w col]);",
                "INSERT INTO u VALUES (1, 'alpha', 10);",
                "INSERT INTO u VALUES (2, 'beta', 20);",
                "INSERT OR REPLACE INTO u VALUES (3, 'alpha', 11);",
                "INSERT INTO u VALUES (4, 'beta', 21) ON CONFLICT(\"na me\") DO UPDATE SET [w col] = excluded.[w col] + 100;",
                "INSERT INTO u VALUES (5, 'gamma', 30) ON CONFLICT(id) DO UPDATE SET \"na me\" = 'gamma_new';",
                "INSERT INTO u VALUES (5, 'gamma_conflict', 31) ON CONFLICT(id) DO UPDATE SET \"na me\" = excluded.\"na me\", [w col] = excluded.[w col];",
                "UPDATE u SET \"na me\" = 'alpha_upd', [w col] = 12 WHERE id = 3;",
                "UPDATE p SET [na me] = 'a_upd', [a+b] = 15, [null] = 'still_not_null' WHERE id = 1;",
                "DELETE FROM u WHERE \"na me\" = 'gamma_conflict';",
                // 3. ALTER TABLE RENAME COLUMN to a spaced name with plain, composite, and expression indexes.
                "CREATE TABLE r (id INTEGER PRIMARY KEY, plain_col TEXT UNIQUE, v INT);",
                "CREATE INDEX ri ON r (plain_col);",
                "CREATE INDEX ri_comp ON r (plain_col, v);",
                "CREATE INDEX ri_expr ON r (lower(plain_col));",
                "INSERT INTO r VALUES (1, 'Init', 5);",
                "ALTER TABLE r RENAME COLUMN plain_col TO \"sp ac ed\";",
                "INSERT INTO r VALUES (2, 'Second', 6);",
                "INSERT OR REPLACE INTO r VALUES (3, 'Init', 55);",
                "INSERT INTO r VALUES (4, 'Second', 66) ON CONFLICT(\"sp ac ed\") DO UPDATE SET v = excluded.v + 1;",
                "UPDATE r SET \"sp ac ed\" = 'Second_Upd' WHERE id = 2;",
                "DELETE FROM r WHERE \"sp ac ed\" = 'Init';",
            ];

            for sql in ddl_and_dml {
                run_exec_mode(&f, sql, prepared).await;
                s.execute_batch(sql)
                    .unwrap_or_else(|e| panic!("stock `{sql}` ({label}): {e}"));
            }

            // Also verify prepared DML with bound parameters on spaced indexed columns.
            if prepared {
                let ins_p = f
                    .prepare("INSERT INTO p VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);")
                    .await
                    .expect("prepare parameterized insert p");
                ins_p
                    .execute_with_params(&[
                        SqliteValue::Integer(4),
                        SqliteValue::Text("ParamVal".into()),
                        SqliteValue::Integer(40),
                        SqliteValue::Integer(44),
                        SqliteValue::Text("sel_p".into()),
                        SqliteValue::Text("null_p".into()),
                        SqliteValue::Text("one_p".into()),
                        SqliteValue::Text("qt_p".into()),
                    ])
                    .await
                    .expect("exec parameterized insert p");
                s.execute(
                    "INSERT INTO p VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);",
                    rusqlite::params![4, "ParamVal", 40, 44, "sel_p", "null_p", "one_p", "qt_p"],
                )
                .expect("stock parameterized insert p");

                let upd_r = f
                    .prepare("UPDATE r SET \"sp ac ed\" = ?1 WHERE id = ?2;")
                    .await
                    .expect("prepare parameterized update r");
                upd_r
                    .execute_with_params(&[
                        SqliteValue::Text("RenamedParam".into()),
                        SqliteValue::Integer(2),
                    ])
                    .await
                    .expect("exec parameterized update r");
                s.execute(
                    "UPDATE r SET \"sp ac ed\" = ?1 WHERE id = ?2;",
                    rusqlite::params!["RenamedParam", 2],
                )
                .expect("stock parameterized update r");
            }

            let checks = [
                // Exact bd-0t3y0 repro probes (index probe vs IS NULL vs NOT INDEXED vs INDEXED BY key read).
                "SELECT count(*) FROM p WHERE \"na me\" = 'a_upd';",
                "SELECT count(*) FROM p WHERE \"na me\" = 'B';",
                "SELECT count(*) FROM p WHERE \"na me\" IS NULL;",
                "SELECT count(*) FROM p NOT INDEXED WHERE \"na me\" = 'a_upd';",
                "SELECT id, \"na me\" FROM p INDEXED BY pi ORDER BY \"na me\", id;",
                "SELECT id, \"na me\", w FROM p INDEXED BY pi_comp WHERE \"na me\" = 'a_upd' AND w = 3;",
                "SELECT id, \"na me\", w FROM p INDEXED BY pi_comp_rev WHERE w = 7 AND \"na me\" = 'B';",
                "SELECT id, \"na me\" FROM p INDEXED BY pi_expr WHERE lower(\"na me\") = 'b';",
                "SELECT id, \"na me\" FROM p INDEXED BY pi_part WHERE \"na me\" IS NOT NULL ORDER BY \"na me\", id;",
                "SELECT id FROM p INDEXED BY pi_weird WHERE \"a+b\" = 15 AND \"select\" = 'kw' AND \"null\" = 'still_not_null' AND \"1\" = 'one' AND \"na\"\"me\" = 'qt1';",
                // UNIQUE autoindex + REPLACE + UPSERT + UPDATE + DELETE table `u`.
                "SELECT count(*) FROM u WHERE \"na me\" = 'alpha_upd';",
                "SELECT count(*) FROM u WHERE \"na me\" = 'beta';",
                "SELECT count(*) FROM u WHERE \"na me\" IS NULL;",
                "SELECT id, \"na me\", [w col] FROM u ORDER BY id;",
                "SELECT id, \"na me\", [w col] FROM u INDEXED BY u_comp ORDER BY \"na me\", [w col];",
                // Renamed-to-spaced table `r`.
                "SELECT count(*) FROM r WHERE \"sp ac ed\" IS NULL;",
                "SELECT id, \"sp ac ed\", v FROM r INDEXED BY ri ORDER BY \"sp ac ed\", id;",
                "SELECT id, \"sp ac ed\", v FROM r INDEXED BY ri_comp ORDER BY \"sp ac ed\", v;",
                "SELECT id, \"sp ac ed\" FROM r INDEXED BY ri_expr WHERE lower(\"sp ac ed\") IN ('second_upd', 'renamedparam') ORDER BY id;",
                // fsqlite PRAGMA integrity_check.
                "PRAGMA integrity_check;",
            ];

            for sql in checks {
                let got = run_query_mode(&f, sql, prepared).await;
                let want = stock_query_rows(&s, sql);
                assert_eq!(got, want, "mismatch on `{sql}` ({label})");
            }

            f.close().await.expect("close fsqlite");

            if use_file {
                // Reopen fsqlite's file with stock SQLite 3.54.1 and verify
                // both PRAGMA integrity_check and indexed reads directly!
                let stock_on_fsq_file =
                    rusqlite::Connection::open(&db_path).expect("stock open fsqlite file");
                assert_eq!(
                    stock_query_rows(&stock_on_fsq_file, "PRAGMA integrity_check;"),
                    vec![vec!["'ok'".to_owned()]],
                    "stock SQLite 3.54.1 reported fsqlite's file corrupt ({label})"
                );
                for sql in checks {
                    assert_eq!(
                        stock_query_rows(&stock_on_fsq_file, sql),
                        stock_query_rows(&s, sql),
                        "stock reading fsqlite's file diverged on `{sql}` ({label})"
                    );
                }
            }
        }
    });
}
