// Keeper for bd-76x57 (file-backed GROUP BY residual): json_group_array /
// json_group_object must preserve the JSON subtype of their arguments through a
// FILE-BACKED GROUP BY query, so a nested json()/json_object()/json_array()
// result is EMBEDDED (`[{"a":1},...]`), not quoted (`["{\"a\":1}",...]`).
//
// A plain-table GROUP BY on a file-backed database is "storage-substrate VDBE
// eligible", so it compiles to a Sorter-backed VDBE program (chosen both by
// `prepared_select_requires_dispatch` and by the interpreted-dispatch ladder,
// which share `select_group_by_storage_substrate_is_vdbe_eligible`). The Sorter
// serializes each aggregate argument into a record, dropping the per-value JSON
// subtype, so before the fix the grouped nested JSON was quoted. The fix marks
// a GROUP BY whose aggregate argument carries a JSON subtype as substrate-
// INELIGIBLE, routing it to `execute_group_by_select`, whose expr-derived
// subtype channel (`aggregate_arg_json_subtypes`) embeds it correctly — the
// same escape hatch the NOCASE-collation and min/max-bare-column cases already
// use. Plain (non-JSON) aggregate arguments keep the fast substrate.
//
// The in-memory GROUP BY path already embeds (it is never substrate-eligible,
// so it always defers to the interpreter); this keeper guards the file-backed
// path specifically. Oracle: sqlite3 3.51 (matches rusqlite 3.46.1):
//   SELECT g, json_group_array(json_object('a', v)) FROM t GROUP BY g ORDER BY g
//     -> a|[{"a":1},{"a":2}]   b|[{"a":3}]
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn text(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected TEXT result, got {other:?}"),
    }
}

async fn grouped(conn: &Connection, sql: &str) -> Vec<(String, String)> {
    let rows = conn.query_with_params(sql, &[]).await.unwrap();
    rows.iter()
        .map(|row| {
            let vals = row.values();
            (text(&vals[0]), text(&vals[1]))
        })
        .collect()
}

#[test]
fn json_group_aggregates_preserve_subtype_file_backed_group_by_76x57() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir
            .path()
            .join("group_subtype.db")
            .to_string_lossy()
            .into_owned();
        let c = Connection::open(&db).await.expect("open file-backed db");
        c.execute("CREATE TABLE t(g TEXT, v INTEGER, s TEXT)")
            .await
            .unwrap();
        c.execute("INSERT INTO t VALUES('a',1,'p'),('a',2,'q'),('b',3,'r')")
            .await
            .unwrap();

        // GROUP BY over a json_object(...) argument -> nested objects embedded.
        assert_eq!(
            grouped(
                &c,
                "SELECT g, json_group_array(json_object('a', v)) FROM t GROUP BY g ORDER BY g",
            )
            .await,
            vec![
                ("a".to_owned(), r#"[{"a":1},{"a":2}]"#.to_owned()),
                ("b".to_owned(), r#"[{"a":3}]"#.to_owned()),
            ],
        );

        // GROUP BY over a json_array(...) argument -> nested arrays embedded.
        assert_eq!(
            grouped(
                &c,
                "SELECT g, json_group_array(json_array(v, v)) FROM t GROUP BY g ORDER BY g",
            )
            .await,
            vec![
                ("a".to_owned(), r"[[1,1],[2,2]]".to_owned()),
                ("b".to_owned(), r"[[3,3]]".to_owned()),
            ],
        );

        // GROUP BY json_group_object with a json_object(...) value -> embedded.
        assert_eq!(
            grouped(
                &c,
                "SELECT g, json_group_object('k' || v, json_object('a', v)) \
                 FROM t GROUP BY g ORDER BY g",
            )
            .await,
            vec![
                ("a".to_owned(), r#"{"k1":{"a":1},"k2":{"a":2}}"#.to_owned()),
                ("b".to_owned(), r#"{"k3":{"a":3}}"#.to_owned()),
            ],
        );

        // Regression guard: a PLAIN text argument carries no JSON subtype, so it
        // stays quoted (and keeps the fast substrate — the disqualification must
        // NOT fire here). Both the substrate and the interpreter quote plain
        // text identically.
        assert_eq!(
            grouped(
                &c,
                "SELECT g, json_group_array(s) FROM t GROUP BY g ORDER BY g",
            )
            .await,
            vec![
                ("a".to_owned(), r#"["p","q"]"#.to_owned()),
                ("b".to_owned(), r#"["r"]"#.to_owned()),
            ],
        );

        c.close().await.expect("close");
    });
}
