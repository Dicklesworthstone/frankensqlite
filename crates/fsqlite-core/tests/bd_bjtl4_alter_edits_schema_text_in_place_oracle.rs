#![recursion_limit = "512"]

//! bd-bjtl4: ALTER TABLE RENAME TO, RENAME COLUMN and DROP COLUMN edit each
//! affected `sqlite_master.sql` text in place, as stock's `sqlite_rename_table`,
//! `sqlite_rename_column` and `sqlite_drop_column` do, instead of re-rendering it:
//! whitespace, comments, keyword case and the quoting of every other
//! identifier stay as written.
//!
//! - A renamed token becomes the new name bare when it was a bare identifier
//!   and the ALTER's new name was not quoted; otherwise `"new"` (`[x]`,
//!   `` `x` `` and `"x"` tokens included). RENAME TO always writes `"new"`.
//! - DROP COLUMN removes the column's text from its name up to the next
//!   column's name; the last column goes from the comma before it up to the
//!   table constraints or the closing parenthesis.
//!
//! Every statement runs on FrankenSQLite and on stock SQLite (rusqlite 3.54.1),
//! both file-backed. After each ALTER the `sqlite_master` (and
//! `sqlite_temp_master`) rows are compared byte for byte, and
//! `PRAGMA integrity_check` after every statement; at the end the tables'
//! rows, the schema text again after FrankenSQLite reopens its file, and
//! stock's `integrity_check` of FrankenSQLite's file.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

async fn frank(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

fn stock(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

const MASTER: &str = "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name";
const TEMP_MASTER: &str =
    "SELECT type, name, tbl_name, sql FROM sqlite_temp_master ORDER BY type, name";

/// One scenario: statements in order. Every ALTER is followed by a schema
/// comparison; `reads` run at the end (and again after the reopen).
struct Scenario {
    name: &'static str,
    statements: &'static [&'static str],
    reads: &'static [&'static str],
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "quoted, bracketed and backquoted names with dependents",
        // `[na me]` and `"Key Id"` stay out of every index here: an index on a
        // column whose name holds a space stores NULL keys on INSERT, a
        // separate defect (SQL lane round 15 finding) this keeper does not
        // cover. The quoted names of scenario 3 are indexed.
        statements: &[
            "CREATE TABLE  \"Parent\"  ( id  INTEGER PRIMARY KEY  /* pk */ , [na me] TEXT  NOT NULL , \
             `v` REAL DEFAULT 1.5, w  CHECK( w > 0 ), g AS (w * 2) VIRTUAL , UNIQUE ( w ) )",
            "CREATE TABLE child ( cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES \"Parent\" ( id ) \
             ON DELETE CASCADE, note TEXT /* trailing */ )",
            "CREATE INDEX  ix_w  ON  \"Parent\" ( w  DESC ) WHERE w > 1",
            "CREATE VIEW  vw  AS  SELECT  id , [na me] AS nm , w   FROM  \"Parent\"  WHERE  w > 0",
            "CREATE TRIGGER  tr  AFTER INSERT ON  \"Parent\"  BEGIN  INSERT INTO child ( pid , note ) \
             VALUES ( new.id , new.[na me] ); END",
            "INSERT INTO \"Parent\" (id, [na me], v, w) VALUES (1, 'a', 2.5, 3), (2, 'b', 3.5, 4)",
            "ALTER TABLE \"Parent\" RENAME COLUMN w TO weight",
            "ALTER TABLE \"Parent\" RENAME COLUMN \"na me\" TO label",
            "ALTER TABLE \"Parent\" RENAME COLUMN id TO \"Key Id\"",
            "ALTER TABLE \"Parent\" RENAME TO p2",
            "ALTER TABLE p2 DROP COLUMN v",
            "ALTER TABLE child DROP COLUMN note",
        ],
        reads: &[
            "SELECT * FROM p2 ORDER BY 1",
            "SELECT * FROM child ORDER BY 1",
            "SELECT * FROM vw ORDER BY 1",
        ],
    },
    Scenario {
        name: "line comments, newlines and odd spacing",
        statements: &[
            "CREATE TABLE  w (\n  k  INTEGER  PRIMARY KEY , -- the key\n  v  TEXT  , /* v */\n  \
             c  INTEGER DEFAULT 7 -- seven\n)",
            "CREATE INDEX wc ON w ( c ) -- index tail",
            "CREATE VIEW wv AS\n  SELECT k, -- k\n    c\n  FROM w",
            "INSERT INTO w VALUES (1, 'x', 2), (2, 'y', 3)",
            "ALTER TABLE w RENAME COLUMN c TO c2",
            "ALTER TABLE w RENAME TO w2",
            "ALTER TABLE w2 DROP COLUMN v",
        ],
        reads: &["SELECT * FROM w2 ORDER BY 1", "SELECT * FROM wv ORDER BY 1"],
    },
    Scenario {
        name: "how the ALTER spells the new name",
        statements: &[
            "CREATE TABLE q ( a INT, \"b\" INT, [c] INT, `d` INT, e INT, f INT )",
            "CREATE INDEX qi ON q ( a, b, c, d, e, f )",
            "INSERT INTO q VALUES (1, 2, 3, 4, 5, 6)",
            "ALTER TABLE q RENAME COLUMN a TO A",
            "ALTER TABLE q RENAME COLUMN b TO bb",
            "ALTER TABLE q RENAME COLUMN c TO [cc]",
            "ALTER TABLE q RENAME COLUMN d TO \"order\"",
            "ALTER  TABLE  q  RENAME  COLUMN  e  TO  'ee'",
            "ALTER TABLE q RENAME f TO `f f`",
            "ALTER TABLE q RENAME TO [q two]",
        ],
        reads: &["SELECT * FROM \"q two\""],
    },
    Scenario {
        name: "constraints, generated columns, expression index and self reference",
        statements: &[
            "CREATE TABLE tree ( id INTEGER PRIMARY KEY, parent INTEGER REFERENCES tree ( id ), \
             nm TEXT COLLATE NOCASE, n INT  DEFAULT ( 1 + 2 ), s AS ( n * 10 ) STORED, \
             CONSTRAINT ck CHECK ( n <> 0 ) , UNIQUE ( nm , n ) )",
            "CREATE INDEX te ON tree ( lower( nm ) , n + 1 )",
            "CREATE TRIGGER tt BEFORE UPDATE OF n ON tree WHEN new.n > old.n BEGIN \
             SELECT RAISE ( IGNORE ) WHERE new.n > 100 ; END",
            "INSERT INTO tree (id, parent, nm, n) VALUES (1, NULL, 'Root', 1), (2, 1, 'Leaf', 2)",
            "ALTER TABLE tree RENAME COLUMN n TO num",
            "ALTER TABLE tree RENAME COLUMN id TO node",
            "ALTER TABLE tree RENAME TO forest",
            "ALTER TABLE forest DROP COLUMN s",
        ],
        reads: &["SELECT * FROM forest ORDER BY 1"],
    },
    Scenario {
        name: "DROP COLUMN first, middle and last, with table constraints after",
        statements: &[
            "CREATE TABLE d ( a INT , b TEXT /* b */ , c BLOB , e INT , PRIMARY KEY ( c ) , CHECK ( e >= 0 ) )",
            "CREATE TABLE d2 ( x , y , z )",
            "CREATE TABLE d3 ( k TEXT , a INT , b INT , PRIMARY KEY ( k ) ) WITHOUT ROWID",
            "INSERT INTO d VALUES (1, 'b', x'00', 1)",
            "INSERT INTO d2 VALUES (1, 2, 3)",
            "INSERT INTO d3 VALUES ('k', 1, 2)",
            "ALTER TABLE d DROP COLUMN a",
            "ALTER TABLE d DROP COLUMN b",
            "ALTER TABLE d2 DROP COLUMN z",
            "ALTER TABLE d2 DROP COLUMN x",
            "ALTER TABLE d3 DROP COLUMN b",
        ],
        reads: &["SELECT * FROM d", "SELECT * FROM d2", "SELECT * FROM d3"],
    },
    Scenario {
        name: "views and triggers that reach the column through aliases and subqueries",
        statements: &[
            "CREATE TABLE m ( k INTEGER PRIMARY KEY , val INT )",
            "CREATE TABLE log ( what TEXT )",
            "CREATE VIEW mv AS SELECT x.val AS v , ( SELECT max( val ) FROM m ) AS top FROM m AS x \
             WHERE x.val IN ( SELECT val FROM m WHERE k > 0 )",
            "CREATE TRIGGER mt AFTER UPDATE OF val ON m BEGIN INSERT INTO log SELECT 'u' || new.val \
             FROM m WHERE m.val = new.val ; END",
            "INSERT INTO m VALUES (1, 10), (2, 20)",
            "ALTER TABLE m RENAME COLUMN val TO amount",
            "ALTER TABLE m RENAME TO money",
            "UPDATE money SET amount = 30 WHERE k = 1",
        ],
        reads: &["SELECT * FROM mv ORDER BY 1", "SELECT * FROM log"],
    },
    Scenario {
        name: "TEMP table with its own index, view and trigger",
        statements: &[
            "CREATE TEMP TABLE tt ( a  INT , [b] TEXT /* b */ , c INT )",
            "CREATE INDEX temp.ti ON tt ( a , [b] )",
            "CREATE TEMP VIEW tv AS SELECT  count( * ) AS n  FROM tt",
            "CREATE TEMP TRIGGER ttr AFTER INSERT ON tt BEGIN SELECT 1 ; END",
            "INSERT INTO tt VALUES (1, 'x', 2)",
            "ALTER TABLE tt RENAME COLUMN a TO a2",
            "ALTER TABLE tt RENAME COLUMN b TO \"b b\"",
            "ALTER TABLE tt RENAME TO tt2",
            "ALTER TABLE tt2 DROP COLUMN c",
        ],
        reads: &["SELECT * FROM tt2", "SELECT * FROM tv"],
    },
];

const INTEGRITY: &str = "PRAGMA integrity_check";

/// Run each query on both sides and record every difference.
async fn compare(
    f: &Connection,
    r: &rusqlite::Connection,
    label: &str,
    queries: &[&str],
    failures: &mut Vec<String>,
) {
    for sql in queries {
        let (fv, sv) = (frank(f, sql).await, stock(r, sql));
        if fv != sv {
            failures.push(format!("{label} `{sql}`:\n  frank {fv:?}\n  stock {sv:?}"));
        }
    }
}

/// The schema text, and the file's integrity, after an ALTER.
async fn compare_schema(
    f: &Connection,
    r: &rusqlite::Connection,
    label: &str,
    failures: &mut Vec<String>,
) {
    compare(f, r, label, &[MASTER, TEMP_MASTER, INTEGRITY], failures).await;
}

#[test]
fn alter_table_edits_the_stored_schema_text_in_place_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut failures = Vec::new();
        let mut alters = 0_usize;
        for (n, scenario) in SCENARIOS.iter().enumerate() {
            let fpath = dir.path().join(format!("frank{n}.db"));
            let spath = dir.path().join(format!("stock{n}.db"));
            let f = Connection::open(fpath.to_str().expect("utf-8 path"))
                .await
                .expect("open");
            let r = rusqlite::Connection::open(&spath).expect("stock open");
            for sql in scenario.statements {
                let fo = f.execute(sql).await.map(|_| ()).map_err(|e| e.to_string());
                let so = r.execute_batch(sql).map_err(|e| e.to_string());
                if fo.is_ok() != so.is_ok() {
                    failures.push(format!(
                        "[{}] `{sql}`: frank {fo:?} vs stock {so:?}",
                        scenario.name
                    ));
                }
                let label = format!("[{}] after `{sql}`", scenario.name);
                if sql.trim_start().to_ascii_uppercase().starts_with("ALTER") {
                    alters += 1;
                    compare_schema(&f, &r, &label, &mut failures).await;
                } else {
                    compare(&f, &r, &label, &[INTEGRITY], &mut failures).await;
                }
            }
            let reads: Vec<&str> = scenario.reads.iter().copied().chain([INTEGRITY]).collect();
            compare(
                &f,
                &r,
                &format!("[{}]", scenario.name),
                &reads,
                &mut failures,
            )
            .await;
            f.close().await.expect("close");

            // The edited text is what was persisted: FrankenSQLite reopens its
            // file to the same schema text and rows. (TEMP objects end with the
            // connection, on both sides.)
            let f = Connection::open(fpath.to_str().expect("utf-8 path"))
                .await
                .expect("reopen");
            let fv = frank(&f, MASTER).await;
            let sv = stock(&r, MASTER);
            if fv != sv {
                failures.push(format!(
                    "[{}] after reopen `{MASTER}`:\n  frank {fv:?}\n  stock {sv:?}",
                    scenario.name
                ));
            }
            f.close().await.expect("close");

            // Stock opens FrankenSQLite's file and finds it intact.
            let s2 = rusqlite::Connection::open(&fpath).expect("stock opens frank's file");
            let check = stock(&s2, INTEGRITY);
            if check != [["'ok'"]] {
                failures.push(format!(
                    "[{}] stock integrity_check of frank's file: {check:?}",
                    scenario.name
                ));
            }
        }
        assert_eq!(alters, 31, "every scenario's ALTER statements ran");
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

/// The edit is per token: a name that merely contains the old name, a string
/// literal spelling it, and a different table's column of the same name stay
/// as written, and the ALTER's error cases still fail as stock fails them.
#[test]
fn only_the_renamed_identifiers_change_and_failures_change_nothing() {
    const STATEMENTS: &[&str] = &[
        "CREATE TABLE a ( x INT , xx INT , note TEXT DEFAULT 'x' )",
        "CREATE TABLE b ( x INT , y INT )",
        "CREATE VIEW ab AS SELECT a.x , a.xx , b.x AS bx , 'x' AS lit FROM a JOIN b ON a.x = b.y",
        "CREATE INDEX axx ON a ( xx , x )",
        "ALTER TABLE a RENAME COLUMN x TO z",
        "ALTER TABLE a RENAME COLUMN nosuch TO q",
        "ALTER TABLE a RENAME COLUMN xx TO z",
        "ALTER TABLE a DROP COLUMN z",
        "ALTER TABLE b RENAME TO ab",
        "ALTER TABLE b RENAME TO b2",
    ];
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let fpath = dir.path().join("frank.db");
        let spath = dir.path().join("stock.db");
        let f = Connection::open(fpath.to_str().expect("utf-8 path"))
            .await
            .expect("open");
        let r = rusqlite::Connection::open(&spath).expect("stock open");
        let mut failures = Vec::new();
        for sql in STATEMENTS {
            let fo = f.execute(sql).await.map(|_| ()).map_err(|e| e.to_string());
            let so = r.execute_batch(sql).map_err(|e| e.to_string());
            if fo.is_ok() != so.is_ok() {
                failures.push(format!("`{sql}`: frank {fo:?} vs stock {so:?}"));
            }
            compare_schema(&f, &r, &format!("after `{sql}`"), &mut failures).await;
        }
        f.close().await.expect("close");
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
