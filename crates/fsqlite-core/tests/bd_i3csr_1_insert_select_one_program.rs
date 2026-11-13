#![recursion_limit = "512"]

//! bd-i3csr.1 (GH#491): `INSERT INTO t [(cols)] SELECT ... FROM s [WHERE ...]`
//! from one base table runs as one VDBE program that scans `s` and inserts
//! each row as it is read, instead of materializing the source and replaying
//! it as INSERT ... VALUES. Every scenario runs in memory and in a file
//! against rusqlite's bundled SQLite: each statement's result or error text,
//! then `changes()`, `total_changes()` and `last_insert_rowid()`, then the
//! checks (contents, `PRAGMA integrity_check`, reads through each index).
//!
//! The admitted shapes cover target affinity over NULLs and mixed types,
//! rowid allocation with and without an explicit rowid or INTEGER PRIMARY
//! KEY, NOT NULL / CHECK / UNIQUE / PRIMARY KEY conflicts part way through the
//! source under every OR action and under column-level conflict clauses,
//! AUTOINCREMENT, generated columns on either side, STRICT, every kind of
//! index, a runtime error part way through the source, and an INSERT ...
//! SELECT inside a trigger body. The shapes the route must decline run
//! alongside (a self-referencing source, ORDER BY, LIMIT, DISTINCT, an
//! aggregate, a join, a WHERE an index serves, a subquery, WITHOUT ROWID, a
//! TEMP table). `insert_select_routes_as_one_program` checks which way each
//! statement went, so the oracle cannot pass by never taking the route.
//!
//! bd-i3csr.2: a `generate_series` source runs as the same program, scanning
//! the series through a virtual-table cursor (the scorecard's bulk-load
//! shape). rusqlite has no generate_series, so the stock side reads a
//! recursive CTE with stock's series semantics (`stock_sql`); the series
//! values themselves are checked against the interpreted scan, which matches
//! the stock sqlite3 shell on the same argument cases, by
//! `series_route_matches_the_interpreted_scan`.
//!
//! Not covered, as fsqlite differs from stock there with or without the route
//! (checked on the replay build df65b45b3): a negative zero stored in a REAL
//! column keeps its sign (stock stores it as the integer 0, bd-kqb76); after a
//! statement that inserted explicit rowids above the table's largest fails,
//! the next automatic rowid of a file-backed table (or in an explicit
//! transaction) skips past them (bd-u65rh); and a statement that fails to
//! prepare resets `changes()` (bd-ulgsj), so the error scenario compares the
//! other two counters.
//!
//! bd-jlbrk: the dedicated conflict-order oracles below cover a rowid REPLACE
//! competing with secondary UNIQUE constraints, including conflicts on the
//! same row, statement overrides, and partial/expression index maintenance.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn fq(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|r| r.values().iter().map(tag_f).collect())
                .collect()
        })
        .map_err(|e| e.to_string())
}

/// Stock's error message. For an error found while preparing, rusqlite's
/// Display appends the SQL text and the error offset; keep the message alone.
fn stock_err(e: rusqlite::Error) -> String {
    match e {
        rusqlite::Error::SqlInputError { msg, .. }
        | rusqlite::Error::SqliteFailure(_, Some(msg)) => msg,
        other => other.to_string(),
    }
}

fn rq(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut st = conn.prepare(sql).map_err(stock_err)?;
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(stock_err)?
    .collect::<Result<Vec<_>, _>>()
    .map_err(stock_err)
}

/// rusqlite's bundled SQLite has no generate_series, so stock reads the same
/// rows from a recursive CTE with stock's series semantics for integer literal
/// arguments: STOP defaults to 4294967295, a zero STEP is 1, a negative STEP
/// counts down from START to STOP. The subquery is named `generate_series`
/// unless the call has an alias, so `generate_series.value` resolves.
fn stock_sql(sql: &str) -> String {
    const CALL: &str = "generate_series(";
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(start) = rest.find(CALL) {
        out.push_str(&rest[..start]);
        let args_start = start + CALL.len();
        let close = args_start + rest[args_start..].find(')').expect("closing paren");
        let args: Vec<&str> = rest[args_start..close].split(',').map(str::trim).collect();
        let (lo, hi, step) = match args.as_slice() {
            [lo] => (*lo, "4294967295", "1"),
            [lo, hi] => (*lo, *hi, "1"),
            [lo, hi, step] => (*lo, *hi, *step),
            _ => panic!("generate_series takes one to three arguments"),
        };
        let stride = format!("(CASE WHEN ({step}) = 0 THEN 1 ELSE ({step}) END)");
        let alias = if rest[close + 1..]
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("AS ")
        {
            ""
        } else {
            " AS generate_series"
        };
        out.push_str(&format!(
            "(WITH RECURSIVE gs(value) AS (SELECT {lo} WHERE {stride} > 0 AND {lo} <= {hi} \
             OR {stride} < 0 AND {lo} >= {hi} UNION ALL SELECT value + {stride} FROM gs \
             WHERE {stride} > 0 AND value + {stride} <= {hi} \
             OR {stride} < 0 AND value + {stride} >= {hi}) SELECT value FROM gs){alias}"
        ));
        rest = &rest[close + 1..];
    }
    out.push_str(rest);
    out
}

struct Scenario {
    name: &'static str,
    /// Run in order; each statement's outcome is compared.
    steps: &'static [&'static str],
    /// Compared after the steps (besides the change counters).
    checks: &'static [&'static str],
}

/// The source most scenarios read: `x` = 1..=60, `y` = 'y' || x.
const SOURCE: &str = "CREATE TABLE s(x INT, y TEXT)";
const FILL: &str = "INSERT INTO s SELECT value, 'y' || value FROM generate_series(1, 60)";

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "copy between rowid-alias tables (the bulk shape), filtered and computed",
        steps: &[
            "CREATE TABLE m(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL)",
            "INSERT INTO m SELECT value, (value * 7919) % 1000, 'row-' || value, value * 1.5 \
             FROM generate_series(1, 300)",
            "CREATE TABLE m2(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL)",
            "INSERT INTO m2 SELECT * FROM m",
            "INSERT INTO m2(a, b, c) SELECT a, b, c FROM m WHERE id % 7 = 0",
            "INSERT INTO m2 SELECT id + 1000, a, b, c FROM m AS src WHERE src.a < 100",
            "INSERT INTO m2 SELECT m.id + 2000, m.a, upper(m.b), round(m.c) FROM m \
             WHERE m.b LIKE 'row-1%'",
            "INSERT INTO m2 SELECT id + 3000, a, b, c FROM m WHERE id > 1000",
        ],
        checks: &[
            "SELECT count(*), sum(id), sum(a), sum(length(b)), sum(c) FROM m2",
            "SELECT * FROM m2 WHERE id % 37 = 0 ORDER BY id",
            "PRAGMA integrity_check",
        ],
    },
    Scenario {
        name: "target affinity over NULLs and mixed types",
        steps: &[
            "CREATE TABLE v(v)",
            "INSERT INTO v VALUES (1), (1.0), (2.5), ('3'), ('3.0'), ('4.5'), (' 5 '), \
             ('abc'), (x'0102'), (NULL), (9223372036854775807), (1e20), ('1e3'), \
             ('0x10'), (0.0), (''), (-7.25), ('-12')",
            "CREATE TABLE t(i INTEGER, r REAL, n NUMERIC, x TEXT, b BLOB, u, \
             vc VARCHAR(10), d DOUBLE)",
            "INSERT INTO t SELECT v, v, v, v, v, v, v, v FROM v",
            "INSERT INTO t(n, i) SELECT v, v || '' FROM v WHERE v IS NOT NULL",
        ],
        checks: &[
            "SELECT rowid, typeof(i), i, typeof(r), r, typeof(n), n, typeof(x), x, \
             typeof(b), quote(b), typeof(u), quote(u), typeof(vc), vc, typeof(d), d \
             FROM t ORDER BY rowid",
        ],
    },
    Scenario {
        name: "rowid allocation with and without an explicit rowid",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE n(a INT, b TEXT)",
            "INSERT INTO n SELECT x, y FROM s WHERE x <= 10",
            "INSERT INTO n(rowid, a) SELECT CASE WHEN x % 3 = 0 THEN NULL ELSE x * 100 END, x \
             FROM s WHERE x <= 40",
            "INSERT INTO n(oid, a) SELECT x * 1000, x FROM s WHERE x > 55",
            "INSERT INTO n(_rowid_, b) SELECT '77' || x, 'text rowid' FROM s WHERE x < 3",
            "INSERT INTO n(b) SELECT y FROM s WHERE x <= 3",
            "INSERT INTO n(rowid, a) SELECT x + 0.5, x FROM s WHERE x = 2",
            // Last: no automatic rowid follows a failure after explicit rowids
            // (see the module notes).
            "INSERT INTO n(rowid, a) SELECT CASE WHEN x = 5 THEN 'abc' ELSE x + 90000 END, x \
             FROM s WHERE x < 8",
            "CREATE TABLE k(id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO k SELECT CASE WHEN x % 4 = 0 THEN NULL ELSE x * 10 END, y FROM s",
            "INSERT INTO k(v) SELECT y FROM s WHERE x < 5",
            "INSERT INTO k SELECT '9' || x, y FROM s WHERE x < 3",
            "INSERT INTO k SELECT 3.0, 'real key' FROM s WHERE x = 1",
            "INSERT INTO k SELECT x * 10, y FROM s WHERE x BETWEEN 2 AND 4",
        ],
        checks: &[
            "SELECT rowid, * FROM n ORDER BY rowid",
            "SELECT * FROM k ORDER BY id",
            "PRAGMA integrity_check",
        ],
    },
    Scenario {
        name: "ABORT: NOT NULL, CHECK and UNIQUE failures part way through the source",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE t(a INT UNIQUE, b TEXT NOT NULL, c INT CHECK (c < 50))",
            "INSERT INTO t VALUES (30, 'old', 0)",
            "INSERT INTO t SELECT x, y, 0 FROM s",
            "INSERT INTO t SELECT x + 100, CASE WHEN x = 20 THEN NULL ELSE y END, 0 FROM s",
            "INSERT INTO t SELECT x + 200, y, x FROM s",
            "INSERT INTO t SELECT x + 300, y, 1 FROM s WHERE x <= 10",
            "BEGIN",
            "INSERT INTO t SELECT x + 400, y, 1 FROM s WHERE x <= 5",
            "INSERT INTO t SELECT x + 400, y, 1 FROM s WHERE x <= 8",
            "INSERT OR ABORT INTO t SELECT x + 500, y, 1 FROM s WHERE x <= 3",
            "COMMIT",
        ],
        checks: &[
            "SELECT rowid, * FROM t ORDER BY rowid",
            "PRAGMA integrity_check",
        ],
    },
    Scenario {
        name: "OR FAIL keeps the rows before the failing one",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE t(a INT UNIQUE, b TEXT NOT NULL, c INT CHECK (c < 50))",
            "INSERT INTO t VALUES (30, 'old', 0)",
            "INSERT OR FAIL INTO t SELECT x, y, 0 FROM s",
            "INSERT OR FAIL INTO t SELECT x + 100, CASE WHEN x = 20 THEN NULL ELSE y END, 0 \
             FROM s",
            "INSERT OR FAIL INTO t SELECT x + 200, y, x FROM s",
            "BEGIN",
            "INSERT OR FAIL INTO t SELECT x + 300, y, 1 FROM s WHERE x <= 6",
            "INSERT OR FAIL INTO t SELECT x + 303, y, 1 FROM s WHERE x <= 6",
            "COMMIT",
        ],
        checks: &[
            "SELECT rowid, * FROM t ORDER BY rowid",
            "PRAGMA integrity_check",
        ],
    },
    Scenario {
        name: "OR IGNORE: rowid, UNIQUE, NOT NULL and CHECK conflicts, duplicates in the source",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT UNIQUE, b TEXT NOT NULL, \
             c INT CHECK (c < 50))",
            "CREATE INDEX tb ON t(b)",
            "INSERT INTO t VALUES (5, 30, 'old', 0)",
            "INSERT OR IGNORE INTO t SELECT x, x, y, 0 FROM s",
            "INSERT OR IGNORE INTO t SELECT x + 100, x + 100, \
             CASE WHEN x % 10 = 0 THEN NULL ELSE y END, x FROM s",
            "INSERT OR IGNORE INTO t(a, b, c) SELECT x % 7 + 1000, y, 0 FROM s",
        ],
        checks: &[
            "SELECT * FROM t ORDER BY id",
            "SELECT b, id FROM t WHERE b > 'y5' ORDER BY b, id",
            "PRAGMA integrity_check",
        ],
    },
    Scenario {
        name: "OR REPLACE: rowid and UNIQUE victims, NOT NULL default, CHECK aborts",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT UNIQUE, \
             b TEXT NOT NULL DEFAULT 'dflt', c INT CHECK (c < 50))",
            "CREATE INDEX tb ON t(b)",
            "INSERT INTO t VALUES (5, 30, 'old', 0), (6, 31, 'old6', 0), (200, 1, 'x', 0)",
            "INSERT OR REPLACE INTO t SELECT x, x, y, 0 FROM s WHERE x <= 40",
            "INSERT OR REPLACE INTO t(id, a, b, c) SELECT x + 100, x % 5, NULL, 1 \
             FROM s WHERE x <= 12",
            "INSERT OR REPLACE INTO t SELECT x + 300, x + 300, y, x FROM s WHERE x >= 45",
        ],
        checks: &[
            "SELECT * FROM t ORDER BY id",
            "SELECT b, id FROM t WHERE b >= 'd' ORDER BY b, id",
            "PRAGMA integrity_check",
        ],
    },
    Scenario {
        name: "OR ROLLBACK ends the transaction",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE t(a INT UNIQUE, b TEXT)",
            "INSERT INTO t VALUES (30, 'old')",
            "BEGIN",
            "INSERT INTO t SELECT x + 100, y FROM s WHERE x <= 5",
            "INSERT OR ROLLBACK INTO t SELECT x, y FROM s",
            "COMMIT",
            "INSERT OR ROLLBACK INTO t SELECT x + 1000, y FROM s WHERE x <= 3",
            "INSERT OR ROLLBACK INTO t SELECT x + 1000, y FROM s WHERE x <= 6",
        ],
        checks: &["SELECT rowid, * FROM t ORDER BY rowid"],
    },
    Scenario {
        name: "column-level conflict clauses",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE t(id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
             a INT UNIQUE ON CONFLICT IGNORE, \
             b TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'nn', \
             c TEXT NOT NULL ON CONFLICT IGNORE)",
            "INSERT INTO t VALUES (1, 15, 'b', 'c'), (50, 500, 'b50', 'c50'), \
             (60, 70, 'b60', 'c60')",
            "INSERT INTO t SELECT x, x * 10, CASE WHEN x = 3 THEN NULL ELSE y END, \
             CASE WHEN x = 4 THEN NULL ELSE y END FROM s WHERE x <= 8",
            "INSERT INTO t SELECT x + 40, x * 100, y, y FROM s WHERE x BETWEEN 9 AND 11",
            "INSERT OR ABORT INTO t SELECT x + 60, x * 10, y, y FROM s WHERE x <= 2",
        ],
        checks: &["SELECT * FROM t ORDER BY id", "PRAGMA integrity_check"],
    },
    Scenario {
        name: "every kind of index is maintained",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE t(a INT, b TEXT, c REAL, d)",
            "CREATE UNIQUE INDEX tu ON t(a)",
            "CREATE INDEX tp ON t(c) WHERE c > 10",
            "CREATE INDEX te ON t(lower(b))",
            "CREATE INDEX tm ON t(b DESC, a)",
            "CREATE INDEX tn ON t(b COLLATE NOCASE)",
            "INSERT INTO t SELECT x, 'Y' || x, x / 2.0, NULL FROM s",
            "INSERT INTO t SELECT x + 100, NULL, NULL, x FROM s WHERE x % 2 = 0",
            "INSERT INTO t SELECT x, 'dup', 0, 0 FROM s WHERE x = 7",
        ],
        checks: &[
            "PRAGMA integrity_check",
            "SELECT count(*) FROM t",
            "SELECT a FROM t WHERE a BETWEEN 10 AND 20 ORDER BY a",
            "SELECT a, c FROM t WHERE c > 25 ORDER BY c",
            "SELECT a FROM t WHERE lower(b) = 'y7'",
            "SELECT b, a FROM t WHERE b > 'Y5' ORDER BY b DESC, a",
            "SELECT a FROM t WHERE b = 'y9' COLLATE NOCASE",
        ],
    },
    Scenario {
        name: "generated columns on the target and on the source",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE g(a INT, b INT GENERATED ALWAYS AS (a * 2) VIRTUAL, \
             c INT GENERATED ALWAYS AS (a + 1) STORED, d TEXT)",
            "CREATE INDEX gb ON g(b)",
            "INSERT INTO g SELECT x, y FROM s WHERE x <= 20",
            "INSERT INTO g(d, a) SELECT y, x + 100 FROM s WHERE x <= 5",
            "CREATE TABLE h(a INT, b INT, c INT, d TEXT)",
            "INSERT INTO h SELECT * FROM g",
            "INSERT INTO h(d, c) SELECT d, b FROM g WHERE a > 100",
        ],
        checks: &[
            "SELECT rowid, * FROM g ORDER BY rowid",
            "SELECT rowid, * FROM h ORDER BY rowid",
            "SELECT a FROM g WHERE b = 10",
            "PRAGMA integrity_check",
        ],
    },
    Scenario {
        name: "AUTOINCREMENT",
        steps: &[
            "CREATE TABLE src(x INT)",
            "INSERT INTO src VALUES (10), (5), (20), (NULL), (15)",
            "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v INT)",
            "INSERT INTO t SELECT x, x FROM src",
            "DELETE FROM t WHERE id >= 20",
            "INSERT INTO t(v) SELECT x FROM src WHERE x < 15",
            "INSERT OR IGNORE INTO t SELECT x, -x FROM src WHERE x = 5",
        ],
        checks: &[
            "SELECT name, seq FROM sqlite_sequence",
            "SELECT * FROM t ORDER BY id",
        ],
    },
    Scenario {
        name: "STRICT target",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE st(id INTEGER PRIMARY KEY, n INTEGER, r REAL, t TEXT, a ANY) STRICT",
            "INSERT INTO st SELECT x, x, x, y, y FROM s WHERE x <= 10",
            "INSERT INTO st SELECT x, '12', '1.5', y, x'00' FROM s WHERE x = 11",
            "INSERT INTO st(n, a) SELECT NULL, x / 2.0 FROM s WHERE x BETWEEN 12 AND 14",
        ],
        checks: &[
            "SELECT id, typeof(n), n, typeof(r), r, typeof(t), t, typeof(a), quote(a) \
             FROM st ORDER BY id",
        ],
    },
    Scenario {
        name: "an INSERT ... SELECT in a trigger body",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE log(n INT, x INT)",
            "CREATE TABLE t(a INT, b TEXT)",
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
             INSERT INTO log SELECT NEW.a, x FROM s WHERE x <= 2; END",
            "INSERT INTO t VALUES (7, 'seven')",
            "INSERT INTO t SELECT x, y FROM s WHERE x BETWEEN 3 AND 4",
        ],
        checks: &[
            "SELECT rowid, * FROM log ORDER BY rowid",
            "SELECT * FROM t ORDER BY rowid",
        ],
    },
    Scenario {
        name: "generate_series source: the bulk-load shape, steps, aliases, conflicts",
        steps: &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL)",
            "INSERT INTO t SELECT value, (value * 7919) % 1000, 'row-' || value, value * 1.5 \
             FROM generate_series(1, 300)",
            "INSERT INTO t(a, b) SELECT value, 'x' || value FROM generate_series(5, 50, 5)",
            "INSERT INTO t SELECT value + 1000, value, NULL, NULL FROM generate_series(10, 1, -3)",
            "INSERT INTO t SELECT g.value + 2000, g.value % 7, 'g', 0.5 \
             FROM generate_series(1, 40) AS g WHERE g.value % 3 = 0",
            "INSERT INTO t SELECT generate_series.value + 3000, 1, 'q', 1 \
             FROM generate_series(1, 3)",
            "INSERT INTO t SELECT value + 4000, 0, 'zero step', 0 FROM generate_series(1, 4, 0)",
            "INSERT INTO t VALUES (5305, 0, 'pre', 0)",
            "INSERT INTO t SELECT value, value, 'abort', 0 FROM generate_series(5301, 5310)",
            "INSERT OR IGNORE INTO t SELECT value, value, 'ignore', 0 \
             FROM generate_series(5301, 5310)",
            "INSERT OR REPLACE INTO t SELECT value, -value, 'replace', 0 FROM generate_series(1, 5)",
            "INSERT INTO t SELECT value, value, 'empty', 0 FROM generate_series(5, 1)",
            "INSERT INTO t(b) SELECT 'no column read' FROM generate_series(1, 2)",
            "BEGIN",
            "INSERT INTO t SELECT value + 6000, (value * 7919) % 1000, 'row-' || value, \
             value * 1.5 FROM generate_series(1, 100)",
            "COMMIT",
        ],
        checks: &[
            "SELECT count(*), sum(id), sum(a), sum(length(b)), sum(c), max(id) FROM t",
            "SELECT * FROM t WHERE id % 23 = 0 OR id > 5000 ORDER BY id",
            "PRAGMA integrity_check",
        ],
    },
    Scenario {
        name: "generate_series source: constraints, generated columns, affinity, AUTOINCREMENT",
        steps: &[
            "CREATE TABLE u(a INT UNIQUE, b TEXT NOT NULL, c REAL CHECK (c < 100), \
             d INT GENERATED ALWAYS AS (a * 2) STORED)",
            "INSERT INTO u(a, b, c) SELECT value, 'v' || value, value * 1.5 \
             FROM generate_series(1, 80)",
            "INSERT INTO u(a, b, c) SELECT value, 'v' || value, value FROM generate_series(1, 50)",
            "INSERT OR IGNORE INTO u(a, b, c) SELECT value % 60, \
             CASE WHEN value % 11 = 0 THEN NULL ELSE 'w' END, value FROM generate_series(40, 99)",
            "INSERT OR FAIL INTO u(a, b, c) SELECT value + 100, 'f', value \
             FROM generate_series(1, 120)",
            "CREATE TABLE au(id INTEGER PRIMARY KEY AUTOINCREMENT, r REAL, t TEXT, n NUMERIC)",
            "INSERT INTO au(r, t, n) SELECT value, value, value * 0.5 FROM generate_series(1, 6)",
            "INSERT INTO au SELECT value * 10, value, value, value FROM generate_series(1, 3)",
        ],
        checks: &[
            "SELECT rowid, * FROM u ORDER BY rowid",
            "SELECT id, typeof(r), r, typeof(t), t, typeof(n), n FROM au ORDER BY id",
            "SELECT name, seq FROM sqlite_sequence",
            "PRAGMA integrity_check",
        ],
    },
    Scenario {
        name: "declined shapes keep stock's answers",
        steps: &[
            SOURCE,
            FILL,
            "CREATE TABLE t(a INT, b TEXT)",
            "INSERT INTO t SELECT x, y FROM s WHERE x <= 10",
            "INSERT INTO t SELECT s.x, t2.b FROM s JOIN t AS t2 ON t2.a = s.x WHERE s.x <= 3",
            "INSERT INTO t SELECT a + 100, b FROM t",
            "INSERT INTO t SELECT x, y FROM s ORDER BY x DESC LIMIT 5",
            "INSERT INTO t SELECT x, y FROM s LIMIT 3 OFFSET 7",
            "INSERT INTO t SELECT DISTINCT x % 3, 'd' FROM s",
            "INSERT INTO t SELECT count(*), max(y) FROM s",
            "INSERT INTO t SELECT x % 4, count(*) FROM s GROUP BY x % 4",
            "INSERT INTO t SELECT x, (SELECT max(a) FROM t) FROM s WHERE x <= 2",
            "INSERT INTO t SELECT x, (SELECT max(b) FROM t AS u WHERE u.a = s.x) FROM s \
             WHERE x <= 3",
            "CREATE INDEX sx ON s(x)",
            "INSERT INTO t SELECT x, y FROM s WHERE x > 55",
            "CREATE TABLE wr(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID",
            "INSERT INTO wr SELECT y, x FROM s",
            "INSERT OR IGNORE INTO wr SELECT 'y' || (x * 2), -x FROM s",
            "CREATE TEMP TABLE tt(a INT, b TEXT)",
            "INSERT INTO tt SELECT x, y FROM s WHERE x <= 5",
            "INSERT INTO t SELECT a + 900, b FROM tt",
        ],
        checks: &[
            "SELECT rowid, * FROM t ORDER BY rowid",
            "SELECT * FROM wr ORDER BY k",
            "SELECT rowid, * FROM tt ORDER BY rowid",
        ],
    },
];

/// Statements that fail to prepare. A failed prepare resets `changes()`
/// (bd-ulgsj), so these compare `total_changes()` and `last_insert_rowid()`.
const PREPARE_ERRORS: Scenario = Scenario {
    name: "column-count, name and generated-column errors",
    steps: &[
        SOURCE,
        FILL,
        "CREATE TABLE t(a INT, b TEXT)",
        "INSERT INTO t SELECT x FROM s",
        "INSERT INTO t(a) SELECT x, y FROM s",
        "INSERT INTO t SELECT nosuch, y FROM s",
        "INSERT INTO t SELECT upper(nosuch), y FROM s",
        "INSERT INTO t SELECT x, y FROM s WHERE nosuch > 1",
        "INSERT INTO t SELECT x, y FROM s WHERE length(s.nosuch) > 1",
        "INSERT INTO t SELECT q.x, y FROM s",
        "INSERT INTO t SELECT x, y FROM s AS q WHERE s.x > 1",
        "INSERT INTO t(a, nosuch) SELECT x, y FROM s",
        "CREATE TABLE g(a INT, b INT GENERATED ALWAYS AS (a * 2) VIRTUAL, d TEXT)",
        "INSERT INTO g(a, b) SELECT 1, 2 FROM s WHERE x = 1",
        "INSERT INTO g SELECT x FROM s",
        "INSERT INTO t SELECT nosuch, 1 FROM generate_series(1, 3)",
        "INSERT INTO t SELECT value, 1 FROM generate_series(1, 3) WHERE nosuch > 1",
        "INSERT INTO t SELECT q.value, 1 FROM generate_series(1, 3)",
        "INSERT INTO t SELECT value FROM generate_series(1, 3)",
        "INSERT INTO t SELECT x, y FROM s WHERE x = 1",
    ],
    checks: &[
        "SELECT rowid, * FROM t ORDER BY rowid",
        "SELECT count(*) FROM g",
    ],
};

/// A runtime error part way through the source: stock has inserted the rows
/// before the failing one when it stops, and rolls the statement back.
const SOURCE_ERROR: Scenario = Scenario {
    name: "a runtime error part way through the source",
    steps: &[
        "CREATE TABLE src(x INT)",
        "INSERT INTO src SELECT value FROM generate_series(1, 60)",
        "CREATE TABLE t(a INT UNIQUE, b INT)",
        "INSERT INTO t SELECT x, CASE WHEN x = 50 \
         THEN abs(-9223372036854775807 - 1) ELSE x END FROM src",
        "INSERT INTO t SELECT x, x FROM src WHERE x <= 40",
        "BEGIN",
        "INSERT INTO t SELECT x + 100, x FROM src WHERE x <= 5",
        "INSERT INTO t SELECT x + 200, CASE WHEN x = 4 \
         THEN abs(-9223372036854775807 - 1) ELSE x END FROM src",
        "COMMIT",
        "INSERT INTO t SELECT value + 300, CASE WHEN value = 7 \
         THEN abs(-9223372036854775807 - 1) ELSE value END FROM generate_series(1, 10)",
        "INSERT INTO t SELECT value + 400, value FROM generate_series(1, 3)",
    ],
    checks: &["SELECT count(*), sum(a) FROM t", "PRAGMA integrity_check"],
};

const COUNTERS: &str = "SELECT changes(), total_changes(), last_insert_rowid()";
const COUNTERS_AFTER_PREPARE_ERRORS: &str = "SELECT total_changes(), last_insert_rowid()";

async fn open_pair(
    file_backed: bool,
    dir: &tempfile::TempDir,
) -> (Connection, rusqlite::Connection) {
    let (f, r) = if file_backed {
        (
            Connection::open(dir.path().join("f.db").to_str().unwrap())
                .await
                .unwrap(),
            rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
        )
    } else {
        (
            Connection::open(":memory:").await.unwrap(),
            rusqlite::Connection::open_in_memory().unwrap(),
        )
    };
    // Bundled rusqlite enables foreign keys by default; stock's default, and
    // fsqlite's, is off.
    r.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
    (f, r)
}

/// Runs every scenario on a fresh connection pair, comparing each step's
/// outcome and the change counters after it, then the checks. Returns the
/// divergences and the number of statements compared.
async fn compare_scenarios(file_backed: bool) -> (Vec<String>, usize) {
    let mode = if file_backed { "file" } else { "memory" };
    let mut diffs = Vec::new();
    let mut statements = 0usize;
    let runs = SCENARIOS
        .iter()
        .map(|scenario| (scenario, COUNTERS))
        .chain([
            (&SOURCE_ERROR, COUNTERS),
            (&PREPARE_ERRORS, COUNTERS_AFTER_PREPARE_ERRORS),
        ]);
    for (scenario, counters) in runs {
        let dir = tempfile::tempdir().unwrap();
        let (f, r) = open_pair(file_backed, &dir).await;
        let mut compare = async |sql: &str| {
            statements += 1;
            let fr = fq(&f, sql).await;
            let rr = rq(&r, &stock_sql(sql));
            if fr != rr {
                diffs.push(format!(
                    "  [{mode}: {}] `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}",
                    scenario.name
                ));
            }
        };
        for step in scenario.steps {
            compare(step).await;
            compare(counters).await;
        }
        for check in scenario.checks {
            compare(check).await;
        }
        f.close().await.unwrap();
    }
    (diffs, statements)
}

#[test]
fn insert_select_one_program_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut statements) = compare_scenarios(false).await;
        let (file_diffs, file_statements) = compare_scenarios(true).await;
        diffs.extend(file_diffs);
        statements += file_statements;
        assert!(
            diffs.is_empty(),
            "{} of {statements} statement(s) diverge from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// A prepared INSERT ... SELECT run several times with different parameters,
/// in the SELECT list and the WHERE clause, numbered and named; the last run
/// conflicts part way through.
#[test]
fn prepared_insert_select_one_program_matches_stock() {
    // The program emits the WHERE clause first: a placeholder numbered where
    // it is emitted would bind `x > ?` to the first value.
    const STATEMENTS: [&str; 3] = [
        "INSERT INTO t(a, b) SELECT x + ?1, ?2 || y FROM s WHERE x > ?3",
        "INSERT INTO t(a, b) SELECT x + :off, :tag || y FROM s WHERE x > :low",
        "INSERT INTO t(a, b) SELECT x + ?, ? || y FROM s WHERE x > ?",
    ];
    let runs: [(i64, &str, i64); 4] = [(0, "p", 50), (100, "q", 55), (1000, "r", 0), (0, "z", 45)];
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            for sql in STATEMENTS {
                let dir = tempfile::tempdir().unwrap();
                let (f, r) = open_pair(file_backed, &dir).await;
                for setup in [
                    SOURCE,
                    FILL,
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT UNIQUE, b TEXT)",
                ] {
                    f.execute(setup).await.unwrap();
                    r.execute_batch(&stock_sql(setup)).unwrap();
                }
                let fp = f.prepare(sql).await.unwrap();
                let mut rp = r.prepare(sql).unwrap();
                for (off, tag, low) in runs {
                    let values = [
                        SqliteValue::Integer(off),
                        SqliteValue::Text(tag.into()),
                        SqliteValue::Integer(low),
                    ];
                    let fr = fp
                        .execute_with_params(&values)
                        .await
                        .map_err(|e| e.to_string());
                    let rr = rp
                        .execute(rusqlite::params![off, tag, low])
                        .map_err(stock_err);
                    if fr != rr {
                        diffs.push(format!(
                            "  [file={file_backed}] `{sql}` ({off}, {tag}, {low})\n     \
                             frank= {fr:?}\n     stock= {rr:?}"
                        ));
                    }
                    for check in [COUNTERS, "SELECT * FROM t ORDER BY id"] {
                        let fr = fq(&f, check).await;
                        let rr = rq(&r, check);
                        if fr != rr {
                            diffs.push(format!(
                                "  [file={file_backed}] after `{sql}` ({off}, {tag}, {low}): \
                                 `{check}`\n     frank= {fr:?}\n     stock= {rr:?}"
                            ));
                        }
                    }
                }
                drop(fp);
                f.close().await.unwrap();
            }
        }
        assert!(
            diffs.is_empty(),
            "{} divergence(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

const REPLAYED: &str = "insert_select_row_by_row_fallback";

/// Which way each INSERT ... SELECT went: the admitted shapes record no
/// row-by-row replay, the declined ones do.
#[test]
fn insert_select_routes_as_one_program() {
    let one_program = [
        "INSERT INTO t SELECT x, y FROM s",
        "INSERT INTO t(b, a) SELECT y, x FROM s WHERE x % 2 = 0",
        "INSERT OR IGNORE INTO u SELECT x, y FROM s",
        "INSERT OR REPLACE INTO u SELECT x, y FROM s",
        "INSERT OR FAIL INTO t SELECT x * 2, upper(y) FROM s AS q WHERE q.x < 5",
        "INSERT INTO t SELECT * FROM s",
        // e has an index on x: it neither serves this WHERE nor covers y
        "INSERT INTO t SELECT x + 1, y FROM e WHERE y > 'y1'",
        // bd-i3csr.2: generate_series through a virtual-table cursor
        "INSERT INTO t SELECT value, 'g' FROM generate_series(1, 3)",
        "INSERT INTO t(a) SELECT g.value * 2 FROM generate_series(1, 9, 2) AS g WHERE g.value > 3",
        "INSERT OR IGNORE INTO u SELECT value, 'i' FROM generate_series(1, 6)",
        "INSERT INTO t(b) SELECT 'none read' FROM generate_series(4294967294)",
    ];
    let replayed = [
        // self-reference, ORDER BY, LIMIT, DISTINCT, aggregate, join, subquery
        "INSERT INTO t SELECT a, b FROM t",
        "INSERT INTO t SELECT x, y FROM s ORDER BY x",
        "INSERT INTO t SELECT x, y FROM s LIMIT 2",
        "INSERT INTO t SELECT DISTINCT x, y FROM s",
        "INSERT INTO t SELECT count(*), 'n' FROM s",
        "INSERT INTO t SELECT s.x, u.b FROM s JOIN u ON u.a = s.x",
        "INSERT INTO t SELECT x, (SELECT b FROM u WHERE u.a = s.x) FROM s",
        "INSERT INTO t SELECT changes(), y FROM s",
        // a WHERE the index on e(x) serves, and that index covering the read
        "INSERT INTO t SELECT x, y FROM e WHERE x > 3",
        "INSERT INTO t(a) SELECT x FROM e",
        // an index hint, a qualified source, WITHOUT ROWID target, RETURNING
        "INSERT INTO t SELECT x, y FROM s NOT INDEXED",
        "INSERT INTO t SELECT x, y FROM main.s",
        "INSERT INTO w SELECT y, x FROM s",
        "INSERT INTO t SELECT x, y FROM s RETURNING a",
        "INSERT INTO u SELECT x, y FROM s WHERE true ON CONFLICT(a) DO NOTHING",
        // a DELETE trigger on the target (an OR REPLACE victim would fire it)
        "INSERT INTO d SELECT x, y FROM s",
        // generate_series: a hidden column, ORDER BY, LIMIT, DISTINCT, an
        // aggregate, a join; another table-valued function
        "INSERT INTO t SELECT value, start FROM generate_series(1, 3)",
        "INSERT INTO t SELECT value, 'o' FROM generate_series(1, 3) ORDER BY value DESC",
        "INSERT INTO t SELECT value, 'l' FROM generate_series(1, 3) LIMIT 2",
        "INSERT INTO t SELECT DISTINCT value % 2, 'd' FROM generate_series(1, 3)",
        "INSERT INTO t SELECT count(*), 'c' FROM generate_series(1, 3)",
        "INSERT INTO t SELECT g.value, s.y FROM generate_series(1, 3) AS g JOIN s ON s.x = g.value",
        "INSERT INTO t SELECT key, value FROM json_each('[1, 2]')",
    ];
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        f.execute_batch(
            "CREATE TABLE s(x INT, y TEXT);
             INSERT INTO s VALUES (1, 'y1'), (2, 'y2'), (3, 'y3'), (4, 'y4');
             CREATE TABLE e(x INT, y TEXT);
             CREATE INDEX ex ON e(x);
             INSERT INTO e SELECT * FROM s;
             CREATE TABLE t(a INT, b TEXT);
             CREATE TABLE u(a INT PRIMARY KEY, b TEXT);
             CREATE TABLE w(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
             CREATE TABLE d(a INT, b TEXT);
             CREATE TABLE gone(a INT);
             CREATE TRIGGER dt AFTER DELETE ON d BEGIN INSERT INTO gone VALUES (OLD.a); END;",
        )
        .await
        .unwrap();
        f.set_fallback_capture_enabled(true);
        let mut wrong = Vec::new();
        for (sql, expect_replayed) in one_program
            .iter()
            .map(|sql| (sql, false))
            .chain(replayed.iter().map(|sql| (sql, true)))
        {
            f.reset_fallback_decision_evidence();
            if let Err(error) = f.query(sql).await {
                wrong.push(format!("  `{sql}` failed: {error}"));
                continue;
            }
            let was_replayed = f
                .fallback_decision_snapshot()
                .decisions
                .iter()
                .any(|decision| decision.decision_reason == REPLAYED);
            if was_replayed != expect_replayed {
                wrong.push(format!(
                    "  `{sql}`: replayed = {was_replayed}, expected {expect_replayed}"
                ));
            }
        }
        // An application's own generate_series (here the same table type,
        // registered again) keeps the interpreted scan: the program opens only
        // the builtin module.
        f.register_module(
            "generate_series",
            Box::new(fsqlite_func::vtab::module_factory_from::<
                fsqlite_ext_misc::GenerateSeriesTable,
            >()),
        );
        let sql = "INSERT INTO t SELECT value, 'r' FROM generate_series(1, 3)";
        f.reset_fallback_decision_evidence();
        match f.query(sql).await {
            Err(error) => wrong.push(format!("  `{sql}` failed: {error}")),
            Ok(_) => {
                if !f
                    .fallback_decision_snapshot()
                    .decisions
                    .iter()
                    .any(|decision| decision.decision_reason == REPLAYED)
                {
                    wrong.push(format!(
                        "  `{sql}` with a registered module: replayed = false, expected true"
                    ));
                }
            }
        }
        f.close().await.unwrap();
        assert!(wrong.is_empty(), "routing:\n{}", wrong.join("\n"));
    });
}

/// bd-i3csr.2: the series a routed INSERT ... SELECT inserts equals what the
/// interpreted scan inserts (the same statement with `LIMIT -1`, which keeps
/// the replay) on argument cases where that scan matches the stock sqlite3
/// shell: negative, zero and NULL steps, text, real and NULL bounds, the
/// default STOP, and the ends of the integer range. The error text and change
/// count agree too, and each statement went the expected way.
#[test]
fn series_route_matches_the_interpreted_scan() {
    let cases = [
        "10, 1, -2",
        "1, 10, -2",
        "1, 10, 0",
        "5, 1",
        "NULL, 3",
        "'2', '4.9'",
        "1.7, 3.2",
        "4294967290",
        "9223372036854775800, 9223372036854775807, 3",
        "-9223372036854775807, -9223372036854775805",
        "1, 3, NULL",
        "'abc', 2",
        "3, 1, -1",
        "-3, 3, 4",
        "1, 3, 9223372036854775807",
        "x'01', 3",
    ];
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        f.set_fallback_capture_enabled(true);
        let mut wrong = Vec::new();
        for (n, args) in cases.iter().enumerate() {
            let (routed, replayed) = (format!("r{n}"), format!("f{n}"));
            for table in [&routed, &replayed] {
                f.execute(&format!("CREATE TABLE {table}(a, b)"))
                    .await
                    .unwrap();
            }
            let mut outcomes = Vec::new();
            for (table, suffix, expect_replayed) in
                [(&routed, "", false), (&replayed, " LIMIT -1", true)]
            {
                let sql = format!(
                    "INSERT INTO {table} SELECT value, typeof(value) \
                     FROM generate_series({args}){suffix}"
                );
                f.reset_fallback_decision_evidence();
                let outcome = f.execute(&sql).await.map_err(|e| e.to_string());
                let was_replayed = f
                    .fallback_decision_snapshot()
                    .decisions
                    .iter()
                    .any(|decision| decision.decision_reason == REPLAYED);
                if was_replayed != expect_replayed {
                    wrong.push(format!(
                        "  `{sql}`: replayed = {was_replayed}, expected {expect_replayed}"
                    ));
                }
                let rows = fq(
                    &f,
                    &format!("SELECT rowid, a, b FROM {table} ORDER BY rowid"),
                )
                .await;
                outcomes.push((outcome, rows));
            }
            if outcomes[0] != outcomes[1] {
                wrong.push(format!(
                    "  generate_series({args})\n     routed=   {:?}\n     replayed= {:?}",
                    outcomes[0], outcomes[1]
                ));
            }
        }
        f.close().await.unwrap();
        assert!(wrong.is_empty(), "series:\n{}", wrong.join("\n"));
    });
}

/// bd-i3csr.2: a prepared INSERT ... SELECT whose series bounds are
/// parameters, run several times; the last run conflicts part way through.
/// Stock's text spells the series as the recursive CTE with the same
/// numbered parameters.
#[test]
fn prepared_series_insert_matches_stock() {
    const SQL: &str = "INSERT INTO t(a, b) SELECT value + ?1, ?2 || value \
                       FROM generate_series(?3, ?4)";
    const STOCK: &str = "INSERT INTO t(a, b) SELECT value + ?1, ?2 || value \
                         FROM (WITH RECURSIVE gs(value) AS (SELECT ?3 WHERE ?3 <= ?4 \
                         UNION ALL SELECT value + 1 FROM gs WHERE value < ?4) SELECT value FROM gs)";
    let runs: [(i64, &str, i64, i64); 4] = [
        (0, "p", 1, 5),
        (100, "q", 3, 4),
        (1000, "r", 6, 5),
        (0, "z", 4, 8),
    ];
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = open_pair(file_backed, &dir).await;
            let setup = "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT UNIQUE, b TEXT)";
            f.execute(setup).await.unwrap();
            r.execute_batch(setup).unwrap();
            let fp = f.prepare(SQL).await.unwrap();
            let mut rp = r.prepare(STOCK).unwrap();
            for (off, tag, lo, hi) in runs {
                let values = [
                    SqliteValue::Integer(off),
                    SqliteValue::Text(tag.into()),
                    SqliteValue::Integer(lo),
                    SqliteValue::Integer(hi),
                ];
                let fr = fp
                    .execute_with_params(&values)
                    .await
                    .map_err(|e| e.to_string());
                let rr = rp
                    .execute(rusqlite::params![off, tag, lo, hi])
                    .map_err(stock_err);
                if fr != rr {
                    diffs.push(format!(
                        "  [file={file_backed}] ({off}, {tag}, {lo}, {hi})\n     \
                         frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
                for check in [COUNTERS, "SELECT * FROM t ORDER BY id"] {
                    let fr = fq(&f, check).await;
                    let rr = rq(&r, check);
                    if fr != rr {
                        diffs.push(format!(
                            "  [file={file_backed}] after ({off}, {tag}, {lo}, {hi}): `{check}`\n     \
                             frank= {fr:?}\n     stock= {rr:?}"
                        ));
                    }
                }
            }
            drop(fp);
            f.close().await.unwrap();
        }
        assert!(
            diffs.is_empty(),
            "{} divergence(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

async fn assert_rowid_replace_statement_matches_stock(
    f: &Connection,
    r: &rusqlite::Connection,
    sql: &str,
    prepared: bool,
    context: &str,
) {
    let statement = if prepared {
        Some(f.prepare(sql).await.map_err(|error| error.to_string()))
    } else {
        None
    };
    // Reuse the same prepared statement across both executions. The second
    // execution also exercises flag resets after a conflict or source loop.
    for execution in 0..2 {
        let frank = match &statement {
            Some(Ok(statement)) => statement
                .query()
                .await
                .map(|rows| {
                    rows.iter()
                        .map(|row| row.values().iter().map(tag_f).collect())
                        .collect()
                })
                .map_err(|error| error.to_string()),
            Some(Err(error)) => Err(error.clone()),
            None => fq(f, sql).await,
        };
        assert_eq!(
            frank,
            rq(r, sql),
            "{context}, execution={execution}: statement outcome for {sql}"
        );
        for check in [
            "SELECT id, a, payload FROM t ORDER BY id",
            "SELECT id, payload FROM t INDEXED BY t_payload ORDER BY payload, id",
            "SELECT * FROM marker",
            COUNTERS,
            "PRAGMA integrity_check",
        ] {
            assert_eq!(
                fq(f, check).await,
                rq(r, check),
                "{context}, execution={execution}: {check} after {sql}"
            );
        }
    }
}

/// bd-jlbrk: a rowid REPLACE must not erase the secondary UNIQUE conflict
/// that should reject or ignore the candidate. The multi-row cases also
/// distinguish FAIL (keep prior rows), ABORT (undo the statement), and
/// ROLLBACK (undo the transaction's earlier marker insert).
#[test]
fn rowid_replace_unique_conflict_order_matches_stock() {
    let cases = [
        ("same victim", "VALUES (1, 'ALPHA', 'candidate')"),
        ("different victim", "VALUES (1, 'beta', 'candidate')"),
        ("nonconflicting literal", "VALUES (9, 'theta', 'literal')"),
        ("SELECT without FROM", "SELECT 1, 'ALPHA', 'candidate'"),
        ("SELECT one row", "SELECT id, a, payload FROM s WHERE id = 1"),
        (
            "multiple VALUES",
            "VALUES (3, 'gamma', 'head'), (1, 'ALPHA', 'candidate'), (4, 'delta', 'tail')",
        ),
        ("SELECT source loop", "SELECT id, a, payload FROM s"),
        (
            "NULL unique keys",
            "VALUES (1, NULL, 'null one'), (2, NULL, 'null two')",
        ),
        ("DEFAULT VALUES", "DEFAULT VALUES"),
    ];
    let policies = [
        ("IGNORE", ""),
        ("ABORT", ""),
        ("FAIL", ""),
        ("ROLLBACK", ""),
        ("REPLACE", ""),
        ("IGNORE", " OR REPLACE"),
        ("REPLACE", " OR IGNORE"),
    ];
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                for (unique_action, statement_action) in policies {
                    for (case, source) in cases {
                        let dir = tempfile::tempdir().unwrap();
                        let (f, r) = open_pair(file_backed, &dir).await;
                        let setup = format!(
                            "CREATE TABLE t(\
                             id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
                             a TEXT COLLATE NOCASE DEFAULT 'alpha' \
                             UNIQUE ON CONFLICT {unique_action}, \
                             payload TEXT DEFAULT 'default'); \
                             CREATE INDEX t_payload ON t(payload); \
                             CREATE TABLE s(id INTEGER, a TEXT, payload TEXT); \
                             CREATE TABLE marker(value INTEGER); \
                             INSERT INTO t VALUES (1, 'alpha', 'one'), (2, 'beta', 'two'); \
                             INSERT INTO s VALUES (3, 'gamma', 'head'), \
                             (1, 'ALPHA', 'candidate'), (4, 'delta', 'tail'); \
                             BEGIN; INSERT INTO marker VALUES (7);"
                        );
                        f.execute_batch(&setup).await.unwrap();
                        r.execute_batch(&setup).unwrap();
                        let sql = format!(
                            "INSERT{statement_action} INTO t {source} RETURNING id, a, payload"
                        );
                        let context = format!(
                            "file={file_backed}, prepared={prepared}, \
                             UNIQUE {unique_action}, statement={statement_action:?}, {case}"
                        );
                        assert_rowid_replace_statement_matches_stock(
                            &f, &r, &sql, prepared, &context,
                        )
                        .await;
                        f.close().await.unwrap();
                    }
                }
            }
        }
    });
}

/// Literal VALUES can carry the table record in P4 while MakeRecord keeps
/// the unique-index record in its sideband and leaves its register NULL.
/// The preflight inclusion flag must not mistake that record for an index
/// entry excluded by a partial predicate.
#[test]
fn rowid_replace_literal_insert_keeps_unique_index_entry() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = open_pair(file_backed, &dir).await;
            let setup = "CREATE TABLE t(id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
                         a TEXT UNIQUE ON CONFLICT IGNORE)";
            f.execute(setup).await.unwrap();
            r.execute_batch(setup).unwrap();
            for sql in [
                "INSERT INTO t VALUES (1, 'literal') RETURNING id, a",
                "INSERT INTO t VALUES (1, 'different') RETURNING id, a",
                "INSERT INTO t VALUES (2, 'literal') RETURNING id, a",
            ] {
                assert_eq!(fq(&f, sql).await, rq(&r, sql), "file={file_backed}: {sql}");
                for check in [
                    "SELECT id, a FROM t ORDER BY id",
                    "SELECT id, a FROM t INDEXED BY sqlite_autoindex_t_1 ORDER BY a",
                    COUNTERS,
                    "PRAGMA integrity_check",
                ] {
                    assert_eq!(
                        fq(&f, check).await,
                        rq(&r, check),
                        "file={file_backed}: {check} after {sql}"
                    );
                }
            }
            f.close().await.unwrap();
        }
    });
}

/// Preflight records must follow each source row's partial predicate, retain
/// NULL-distinct UNIQUE semantics, and use the index's expression, collation
/// and sort metadata. Repeating a prepared statement must not reuse a record
/// from an earlier source row or execution.
#[test]
fn rowid_replace_preflight_preserves_partial_and_expression_indexes() {
    const SETUP: &str = "\
        CREATE TABLE t(id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
                      a TEXT COLLATE NOCASE, payload TEXT); \
        CREATE INDEX t_payload ON t(payload); \
        CREATE UNIQUE INDEX t_partial ON t(lower(a)) WHERE id % 2 = 1; \
        CREATE UNIQUE INDEX t_composite ON t(a COLLATE NOCASE, payload DESC); \
        CREATE TABLE s(id INTEGER, a TEXT, payload TEXT); \
        CREATE TABLE marker(value INTEGER); \
        INSERT INTO t VALUES (1, 'alpha', 'one'), (2, 'alpha', 'two'), \
                             (3, NULL, 'three'), (4, NULL, 'four'); \
        INSERT INTO s VALUES (5, 'beta', 'five'), (6, 'beta', 'six'), \
                             (7, NULL, 'seven'), (8, NULL, 'eight');";
    const STEPS: &[&str] = &[
        "INSERT INTO t SELECT id, a, payload FROM s RETURNING id, a, payload",
        "INSERT INTO t VALUES (2, 'ALPHA', 'replacement') RETURNING id, a, payload",
        "INSERT INTO t VALUES (5, 'ALPHA', 'conflicting partial') RETURNING id, a, payload",
        "INSERT INTO t VALUES (5, 'ALPHA', 'unmatched upsert target') \
         ON CONFLICT(a, payload) DO NOTHING RETURNING id, a, payload",
        "INSERT INTO t VALUES (2, 'ALPHA', 'replacement') RETURNING id, a, payload",
        "INSERT INTO t VALUES (3, NULL, 'three new'), (4, NULL, 'four new') \
         RETURNING id, a, payload",
        "INSERT OR REPLACE INTO t VALUES (1, 'alpha', 'one new') RETURNING id, a, payload",
        "INSERT INTO t SELECT id, a, payload FROM s RETURNING id, a, payload",
    ];
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let (f, r) = open_pair(file_backed, &dir).await;
                f.execute_batch(SETUP).await.unwrap();
                r.execute_batch(SETUP).unwrap();
                let context = format!("file={file_backed}, prepared={prepared}, partial indexes");
                for sql in STEPS {
                    assert_rowid_replace_statement_matches_stock(&f, &r, sql, prepared, &context)
                        .await;
                    let check = "SELECT id, lower(a) FROM t INDEXED BY t_partial \
                                 WHERE id % 2 = 1 ORDER BY id";
                    assert_eq!(fq(&f, check).await, rq(&r, check), "{context}: {check}");
                }
                f.close().await.unwrap();
            }
        }
    });
}

/// Existing-row preflight must preserve a healthy AUTOINCREMENT sequence;
/// free rowids retain the normal allocation and ignored-row bookkeeping.
/// Deleting the high row leaves a sequence above the table's maximum without
/// manually modifying sqlite_sequence.
#[test]
fn rowid_replace_preflight_preserves_autoincrement_sequence() {
    const SETUP: &str = "\
        CREATE TABLE t(id INTEGER PRIMARY KEY ON CONFLICT REPLACE AUTOINCREMENT, \
                      a TEXT UNIQUE ON CONFLICT IGNORE, payload TEXT); \
        CREATE INDEX t_payload ON t(payload); \
        CREATE TABLE marker(value INTEGER); \
        INSERT INTO t VALUES (1, 'alpha', 'one'), (2, 'beta', 'two'), \
                             (100, 'reserved', 'deleted'); \
        DELETE FROM t WHERE id = 100;";
    const STEPS: &[&str] = &[
        "INSERT INTO t VALUES (1, 'alpha', 'same victim') RETURNING id, a, payload",
        "INSERT INTO t VALUES (1, 'beta', 'different victim') RETURNING id, a, payload",
        "INSERT INTO t VALUES (99, 'alpha', 'free rowid') RETURNING id, a, payload",
        "INSERT INTO t VALUES (200, 'alpha', 'free high rowid') RETURNING id, a, payload",
        "INSERT INTO t VALUES (NULL, 'alpha', 'burned rowid') RETURNING id, a, payload",
        "INSERT INTO t VALUES (NULL, 'gamma', 'next rowid') RETURNING id, a, payload",
        "INSERT OR REPLACE INTO t VALUES (2, 'beta', 'override') RETURNING id, a, payload",
    ];
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let (f, r) = open_pair(file_backed, &dir).await;
                f.execute_batch(SETUP).await.unwrap();
                r.execute_batch(SETUP).unwrap();
                let context = format!("file={file_backed}, prepared={prepared}, AUTOINCREMENT");
                for sql in STEPS {
                    assert_rowid_replace_statement_matches_stock(&f, &r, sql, prepared, &context)
                        .await;
                    let check = "SELECT name, seq FROM sqlite_sequence ORDER BY name";
                    assert_eq!(fq(&f, check).await, rq(&r, check), "{context}: {check}");
                }
                f.close().await.unwrap();
            }
        }
    });
}
