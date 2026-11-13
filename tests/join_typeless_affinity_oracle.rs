#![recursion_limit = "512"]

//! Join keys compare under SQLite's comparison affinity on every join lane
//! (bd-kr6hf, bd-y5mc8).
//!
//! - A numeric column against a typeless one compares under NUMERIC affinity,
//!   so the integer 2 equals the TEXT '2', ' 2' and '2.0' stored in a typeless
//!   column. The index lookup lane used to seek only the raw number and missed
//!   that text.
//! - A TEXT column against a typeless one compares without conversion
//!   (GH#428), so the integer 2 in a typeless column does not equal '2'. The
//!   hash join used to convert the typeless side to TEXT.
//!
//! Every column of one table is joined to every column of another, across
//! INTEGER, TEXT, REAL, NUMERIC, BLOB and typeless affinities, with stored
//! values that conversion changes. Each query runs as an inner and a LEFT
//! join, as row output with and without ORDER BY, `SELECT *`, an aggregate, a
//! grouped aggregate, a comma join (the hash join) and a projection only the
//! interpreted join evaluates, with and without single-column indexes, in
//! memory and file-backed. Three-table chains go through UNIQUE indexes and
//! the rowid. Results are compared with rusqlite.
//!
//! The ordered queries also cover ORDER BY ordinals, DESC and NULLS FIRST/LAST.
//! The VDBE join lanes emitted ordinals as constant sort keys (`ORDER BY 2`
//! kept scan order) and opened their sorter in a form that dropped DESC.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("i{n}"),
        SqliteValue::Float(f) => format!("r{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("i{n}"),
        rusqlite::types::Value::Real(f) => format!("r{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:?}"),
    }
}

/// Column name and declared type, shared by `p` and `c`.
const COLUMNS: &[(&str, &str)] = &[
    ("i", "INTEGER"),
    ("t", "TEXT"),
    ("r", "REAL"),
    ("n", "NUMERIC"),
    ("b", "BLOB"),
    ("x", ""),
];

/// Stored values. Each row puts the same literal in every column, so each
/// column's affinity decides what is stored.
const VALUES: &[&str] = &[
    "1",
    "2",
    "2.0",
    "2.5",
    "'2'",
    "' 2'",
    "'2.0'",
    "'02'",
    "'2abc'",
    "'abc'",
    "'+2'",
    "'2e0'",
    "x'32'",
    "NULL",
    "-3",
    "'-3'",
    "9223372036854775807",
    "'9223372036854775807'",
];

fn setup() -> Vec<String> {
    let cols = COLUMNS
        .iter()
        .map(|(name, ty)| format!("{name} {ty}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = vec![
        format!("CREATE TABLE p(id INTEGER PRIMARY KEY, {cols})"),
        format!("CREATE TABLE c(id INTEGER PRIMARY KEY, {cols})"),
        // Chain target: distinct keys behind UNIQUE indexes.
        "CREATE TABLE q(id INTEGER PRIMARY KEY, k TEXT UNIQUE, m INTEGER UNIQUE, x UNIQUE)"
            .to_owned(),
        "INSERT INTO q VALUES (1,'1',1,'1'),(2,'2',2,2),(3,'abc',3,' 2'),(4,'2.0',4,2.5),\
         (5,' 2',5,'abc')"
            .to_owned(),
    ];
    for (table, offset) in [("p", 0), ("c", 100)] {
        for (i, value) in VALUES.iter().enumerate() {
            let values = std::iter::repeat_n(*value, COLUMNS.len())
                .collect::<Vec<_>>()
                .join(", ");
            sql.push(format!(
                "INSERT INTO {table} VALUES ({}, {values})",
                offset + i + 1
            ));
        }
    }
    sql
}

fn indexes() -> Vec<String> {
    let mut sql = Vec::new();
    for table in ["p", "c"] {
        for (name, _) in COLUMNS {
            sql.push(format!("CREATE INDEX {table}_{name} ON {table}({name})"));
        }
    }
    sql
}

/// `(sql, ordered)`: unordered results are compared as sorted multisets.
fn queries() -> Vec<(String, bool)> {
    let keys = COLUMNS
        .iter()
        .map(|(name, _)| *name)
        .chain(std::iter::once("id"))
        .collect::<Vec<_>>();
    let mut queries = Vec::new();
    for pc in &keys {
        for cc in &keys {
            let on = format!("c.{cc} = p.{pc}");
            for join in ["JOIN", "LEFT JOIN"] {
                queries.push((
                    format!("SELECT p.id, c.id FROM p {join} c ON {on} ORDER BY 1, 2"),
                    true,
                ));
                queries.push((format!("SELECT p.id, c.id FROM p {join} c ON {on}"), false));
                queries.push((format!("SELECT * FROM p {join} c ON {on}"), false));
                // Ordinals name result columns, counting those `*` expands.
                queries.push((
                    format!("SELECT p.id, c.id FROM p {join} c ON {on} ORDER BY 2 DESC, 1"),
                    true,
                ));
                queries.push((
                    format!("SELECT * FROM p {join} c ON {on} ORDER BY 8 DESC, 1"),
                    true,
                ));
                queries.push((
                    format!(
                        "SELECT p.id, c.id, c.{cc} FROM p {join} c ON {on} \
                         ORDER BY c.{cc} DESC NULLS FIRST, 2 NULLS LAST, p.id DESC"
                    ),
                    true,
                ));
                queries.push((
                    format!("SELECT count(*), sum(c.id), max(p.id) FROM p {join} c ON {on}"),
                    true,
                ));
            }
            queries.push((
                format!(
                    "SELECT p.id, count(*), sum(c.id) FROM p JOIN c ON {on} GROUP BY p.id \
                     ORDER BY 1"
                ),
                true,
            ));
            queries.push((
                format!("SELECT p.id, count(*), sum(c.id) FROM p JOIN c ON {on} GROUP BY p.id"),
                false,
            ));
            queries.push((
                format!("SELECT p.id, c.id FROM p, c WHERE {on} ORDER BY 1, 2"),
                true,
            ));
            queries.push((
                format!(
                    "SELECT p.id, typeof(p.{pc}), c.id, typeof(c.{cc}) FROM p JOIN c ON {on} \
                     ORDER BY 1, 3"
                ),
                true,
            ));
        }
    }
    // Chains: p -> q through each UNIQUE key, then back to c by rowid.
    for pc in &keys {
        for qk in ["k", "m", "x", "id"] {
            queries.push((
                format!(
                    "SELECT p.id, q.id, c.id FROM p JOIN q ON q.{qk} = p.{pc} \
                     JOIN c ON c.id = q.id + 100 ORDER BY 1, 2, 3"
                ),
                true,
            ));
            queries.push((
                format!(
                    "SELECT p.id, q.id, c.id FROM p JOIN q ON q.{qk} = p.{pc} \
                     JOIN c ON c.x = q.m ORDER BY 1, 2, 3"
                ),
                true,
            ));
        }
    }
    queries
}

async fn franken_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect()
        })
        .map_err(|error| format!("{error:?}"))
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|error| error.to_string())?;
    let ncol = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..ncol)
            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect())
    })
    .map_err(|error| error.to_string())?
    .collect::<Result<_, _>>()
    .map_err(|error| error.to_string())
}

async fn mismatches(f: &Connection, r: &rusqlite::Connection, label: &str) -> Vec<String> {
    let mut found = Vec::new();
    for (sql, ordered) in queries() {
        let mut ff = franken_rows(f, &sql).await;
        let mut rr = stock_rows(r, &sql);
        if !ordered {
            if let Ok(rows) = &mut ff {
                rows.sort();
            }
            if let Ok(rows) = &mut rr {
                rows.sort();
            }
        }
        if ff != rr {
            found.push(format!(
                "[{label}] `{sql}`\n  fsqlite: {ff:?}\n  sqlite:  {rr:?}"
            ));
        }
    }
    found
}

#[test]
fn join_keys_compare_under_sqlite_comparison_affinity() {
    for file_backed in [false, true] {
        for indexed in [false, true] {
            asupersync::test_utils::run_test(|| async move {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("join_typeless_affinity.db");
                let f = if file_backed {
                    Connection::open(path.to_str().unwrap()).await.unwrap()
                } else {
                    Connection::open(":memory:").await.unwrap()
                };
                let r = rusqlite::Connection::open_in_memory().unwrap();
                let mut sql = setup();
                if indexed {
                    sql.extend(indexes());
                }
                for statement in &sql {
                    f.execute(statement).await.unwrap();
                    r.execute(statement, []).unwrap();
                }
                let label = format!(
                    "{} {}",
                    if file_backed { "file" } else { "memory" },
                    if indexed { "indexed" } else { "unindexed" }
                );
                let found = mismatches(&f, &r, &label).await;
                assert!(
                    found.is_empty(),
                    "{} queries differ from SQLite; first ones:\n{}",
                    found.len(),
                    found.iter().take(25).cloned().collect::<Vec<_>>().join("\n")
                );
            });
        }
    }
}

async fn opcodes(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .filter_map(|row| match row.values().get(1) {
            Some(SqliteValue::Text(op)) => Some(op.to_string()),
            _ => None,
        })
        .collect()
}

/// The exact shapes stay lookups, not nested loops: an inner untyped foreign
/// key drives from the child and seeks the parent by rowid, and a LEFT join
/// (which cannot be reordered) seeks the typeless index and then its TEXT
/// keys.
#[test]
fn typeless_join_keys_keep_lookup_plans() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("join_typeless_affinity_plans.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, x)",
            "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id, y)",
            "CREATE INDEX child_p ON child(parent_id)",
            "INSERT INTO parent VALUES (1,7),(2,14),(3,21)",
            "INSERT INTO child VALUES (10,1,0),(20,'2',0),(30,' 3',0),(31,'3.0',0),(32,3.0,0),\
             (33,'3abc',0),(34,x'33',0),(35,NULL,0),(36,'abc',0)",
        ] {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }
        let inner = "SELECT parent.id, child.id FROM parent JOIN child \
                     ON child.parent_id = parent.id ORDER BY 1, 2";
        let left = "SELECT parent.id, child.id FROM parent LEFT JOIN child \
                    ON child.parent_id = parent.id ORDER BY 1, 2";
        let star = "SELECT * FROM parent JOIN child ON child.parent_id = parent.id ORDER BY 1, 4";
        for sql in [inner, left, star] {
            assert_eq!(
                franken_rows(&f, sql).await,
                stock_rows(&r, sql),
                "mismatch on `{sql}`"
            );
        }
        let ops = opcodes(&f, inner).await;
        assert!(
            ops.iter().any(|op| op == "SeekRowid")
                && !ops.iter().any(|op| op == "SeekGE")
                && ops.iter().filter(|op| *op == "Rewind").count() == 1,
            "the inner join must scan child once and seek parent by rowid: {ops:?}"
        );
        // Once before the loop into child_p's TEXT keys, indexing the numeric
        // ones by number in an ephemeral map (bd-673gw, bd-k8ebx), then per
        // probe for the number, for its entries in the map, and to position
        // child_p on each of them.
        for sql in [left, star] {
            let ops = opcodes(&f, sql).await;
            assert!(
                ops.iter().filter(|op| *op == "SeekGE").count() == 4
                    && ops.iter().filter(|op| *op == "OpenAutoindex").count() == 1
                    && ops.iter().filter(|op| *op == "Rewind").count() == 1,
                "`{sql}` must seek child_p for the number and the map for its TEXT keys: {ops:?}"
            );
        }
    });
}
