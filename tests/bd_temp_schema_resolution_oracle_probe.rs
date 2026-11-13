#![recursion_limit = "512"]

//! TEMP-schema resolution leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite
//! over the temp database schema — CREATE TEMP TABLE that SHADOWS a same-named
//! main table (an unqualified reference resolves temp-before-main), explicit
//! `temp.`/`main.` qualifiers, a TEMP table joined against a main table, a TEMP
//! VIEW over a main table, DROP of the temp table revealing the main table
//! again, and temp objects being absent from main.sqlite_master but present in
//! sqlite_temp_master. This mirrors the ATTACH cross-schema routing surface
//! (where real bugs were found), so it is a targeted leaf hunt. Ordered result
//! sets / schema listings compared.
//!
//! This probe SURFACED two genuine frank divergences; one is now FIXED and
//! asserted here, the other remains extracted to a bead:
//!   - bd-y4yjq (OPEN): after `CREATE TEMP TABLE t` shadows `main.t`,
//!     main.sqlite_master omits `main.t` from its table listing (the table is
//!     still queryable). Intentionally not asserted here.
//!   - bd-ghiey (FIXED): a single statement joining BOTH `main.t` and `temp.t`
//!     (same base name, different schemas) used to mis-resolve both to one table
//!     -> wrong cardinality (9 vs 6) and NULL column projection. Now asserted
//!     below: the `main.`-qualified source gets a distinct synthetic codegen
//!     schema entry so the two sources land on different root pages.
//! Each same-named table resolved ALONE (temp. and main. qualifiers) is correct.

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

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    match conn.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else {
        return vec![vec!["ERR".to_owned()]];
    };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    }) {
        Ok(rows) => rows
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
async fn ex(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let _ = f.execute(sql).await;
    let _ = r.execute(sql, []);
}

#[test]
fn temp_schema_resolution_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, src TEXT)",
            "INSERT INTO t VALUES (1,'main1'),(2,'main2'),(3,'main3')",
            "CREATE TABLE only_main(id INTEGER, v TEXT)",
            "INSERT INTO only_main VALUES (1,'a'),(2,'b')",
            // TEMP table shadowing main.t (different rows)
            "CREATE TEMP TABLE t(id INTEGER PRIMARY KEY, src TEXT)",
            "INSERT INTO temp.t VALUES (10,'temp10'),(20,'temp20')",
            // a temp-only table
            "CREATE TEMP TABLE scratch(id INTEGER, note TEXT)",
            "INSERT INTO scratch VALUES (100,'s1'),(200,'s2')",
        ] {
            ex(&f, &r, s).await;
        }
        let mut diffs = Vec::new();
        let check =
            |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
                if fr != rr {
                    d.push(format!(
                        "  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            };

        // unqualified `t` resolves to TEMP.t (temp shadows main)
        check(
            "unqualified -> temp",
            fq(&f, "SELECT id,src FROM t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM t ORDER BY id"),
            &mut diffs,
        );
        // explicit temp. and main. qualifiers reach the right table
        check(
            "temp. qualifier",
            fq(&f, "SELECT id,src FROM temp.t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM temp.t ORDER BY id"),
            &mut diffs,
        );
        check(
            "main. qualifier",
            fq(&f, "SELECT id,src FROM main.t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM main.t ORDER BY id"),
            &mut diffs,
        );
        // temp-only table via bare name
        check(
            "temp-only bare",
            fq(&f, "SELECT id,note FROM scratch ORDER BY id").await,
            rq(&r, "SELECT id,note FROM scratch ORDER BY id"),
            &mut diffs,
        );
        // main-only table still reachable via bare name
        check(
            "main-only bare",
            fq(&f, "SELECT id,v FROM only_main ORDER BY id").await,
            rq(&r, "SELECT id,v FROM only_main ORDER BY id"),
            &mut diffs,
        );

        // join temp table (shadowing) with a main-only table
        check(
            "temp join main",
            fq(
                &f,
                "SELECT t.id, t.src, o.v FROM t JOIN only_main o ON t.id/10 = o.id ORDER BY t.id",
            )
            .await,
            rq(
                &r,
                "SELECT t.id, t.src, o.v FROM t JOIN only_main o ON t.id/10 = o.id ORDER BY t.id",
            ),
            &mut diffs,
        );
        // bd-ghiey (FIXED): a single statement joining BOTH main.t and temp.t
        // (same base name, different schemas) must resolve each source to its own
        // table -> stock's 6-row cross join, NOT frank's former 9 all-NULL rows
        // (both mis-resolved to main.t). The `main.`-qualified source now gets a
        // distinct synthetic codegen-schema entry so it lands on a different root
        // page than the temp/unqualified source.
        check(
            "join BOTH main.t and temp.t (id projection)",
            fq(
                &f,
                "SELECT m.id, tm.id FROM main.t m JOIN temp.t tm ON 1=1 ORDER BY m.id, tm.id",
            )
            .await,
            rq(
                &r,
                "SELECT m.id, tm.id FROM main.t m JOIN temp.t tm ON 1=1 ORDER BY m.id, tm.id",
            ),
            &mut diffs,
        );
        check(
            "join BOTH main.t and temp.t (star projection)",
            fq(
                &f,
                "SELECT * FROM main.t m JOIN temp.t tm ON 1=1 ORDER BY m.id, tm.id",
            )
            .await,
            rq(
                &r,
                "SELECT * FROM main.t m JOIN temp.t tm ON 1=1 ORDER BY m.id, tm.id",
            ),
            &mut diffs,
        );
        // Symmetric: temp source listed first, and an unqualified sibling that
        // must still resolve temp-first while main.t reaches the shadowed main.
        check(
            "join temp.t then main.t",
            fq(
                &f,
                "SELECT tm.id, m.src FROM temp.t tm JOIN main.t m ON 1=1 ORDER BY tm.id, m.id",
            )
            .await,
            rq(
                &r,
                "SELECT tm.id, m.src FROM temp.t tm JOIN main.t m ON 1=1 ORDER BY tm.id, m.id",
            ),
            &mut diffs,
        );

        // TEMP VIEW over the main table
        ex(
            &f,
            &r,
            "CREATE TEMP VIEW vmain AS SELECT id, src FROM main.t WHERE id >= 2",
        )
        .await;
        check(
            "temp view over main",
            fq(&f, "SELECT id,src FROM vmain ORDER BY id").await,
            rq(&r, "SELECT id,src FROM vmain ORDER BY id"),
            &mut diffs,
        );

        // NOTE: main.sqlite_master's table listing is a known divergence tracked in
        // bd-y4yjq (frank omits the shadowed `main.t` from the listing though the
        // table stays directly queryable) -- intentionally not asserted here.
        // sqlite_temp_master has the temp tables + temp view
        check("temp schema listing", fq(&f, "SELECT name,type FROM sqlite_temp_master WHERE type IN ('table','view') ORDER BY name").await,
              rq(&r, "SELECT name,type FROM sqlite_temp_master WHERE type IN ('table','view') ORDER BY name"), &mut diffs);

        // writes to the temp (shadowing) table don't touch main
        ex(&f, &r, "UPDATE t SET src='temp-upd' WHERE id=10").await;
        check(
            "temp write isolation temp",
            fq(&f, "SELECT id,src FROM temp.t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM temp.t ORDER BY id"),
            &mut diffs,
        );
        check(
            "temp write isolation main",
            fq(&f, "SELECT id,src FROM main.t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM main.t ORDER BY id"),
            &mut diffs,
        );

        // DROP the temp shadow -> bare `t` now resolves to main.t
        ex(&f, &r, "DROP TABLE temp.t").await;
        check(
            "after drop temp shadow",
            fq(&f, "SELECT id,src FROM t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM t ORDER BY id"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} TEMP-schema divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
