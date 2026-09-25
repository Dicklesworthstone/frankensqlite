#![recursion_limit = "512"]

//! SQL-engine regression for truth-tested JSON membership; no financial data.
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use fsqlite_core::connection::Connection;
use fsqlite_func::ScalarFunction;
use fsqlite_types::value::SqliteValue;

struct CountJson(Arc<AtomicUsize>);
impl ScalarFunction for CountJson {
    fn name(&self) -> &str { "count_json" }
    fn num_args(&self) -> i32 { 1 }
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(args[0].clone())
    }
}

#[test]
fn truth_tested_json_membership_materializes_once_and_preserves_nulls() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let setup = "CREATE TABLE probes(value); INSERT INTO probes VALUES(0),(1),(3),(NULL)";
        conn.execute(setup).await.unwrap();
        stock.execute_batch(setup).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        conn.register_nondeterministic_scalar_function(CountJson(Arc::clone(&calls)));
        for wrapper in ["IS TRUE", "IS NOT TRUE", "IS FALSE", "IS NOT FALSE"] {
            for json in ["[0,1]", "[3,null]", "[]"] {
                let sql = format!("SELECT rowid FROM probes WHERE (value IN (SELECT value FROM json_each(count_json(?1)))) {wrapper} ORDER BY rowid");
                let oracle_sql = sql.replace("count_json(?1)", "?1");
                let expected = stock.prepare(&oracle_sql).unwrap().query_map([json], |r| r.get::<_,i64>(0)).unwrap().collect::<Result<Vec<_>,_>>().unwrap();
                calls.store(0, Ordering::Relaxed);
                let actual = conn.query_with_params(&sql, &[SqliteValue::Text(json.into())]).await.unwrap();
                let actual = actual.iter().map(|r| r.values()[0].clone()).collect::<Vec<_>>();
                assert_eq!(actual, expected.into_iter().map(SqliteValue::Integer).collect::<Vec<_>>(), "{wrapper} {json}");
                let count = calls.load(Ordering::Relaxed);
                eprintln!("membership wrapper={wrapper:?} json={json:?} materializations={count}");
                assert_eq!(count, 1, "uncorrelated RHS must be reused within this WHERE loop");
            }
        }
        // Correlated input must stay per-row, including through truth tests.
        calls.store(0, Ordering::Relaxed);
        let sql = "SELECT rowid FROM probes AS p WHERE (p.value IN (SELECT value FROM json_each(count_json(json_array(p.value))))) IS TRUE ORDER BY rowid";
        let rows = conn.query(sql).await.unwrap();
        assert_eq!(rows.iter().map(|r| r.values()[0].clone()).collect::<Vec<_>>(), vec![SqliteValue::Integer(1),SqliteValue::Integer(2),SqliteValue::Integer(3)]);
        assert_eq!(calls.load(Ordering::Relaxed), 4, "correlated RHS cannot reuse another row's array");
        // A skipped malformed RHS stays skipped; a reached one must error.
        calls.store(0, Ordering::Relaxed);
        assert_eq!(conn.query("SELECT rowid FROM probes WHERE 1 OR (value IN (SELECT value FROM json_each(count_json('[')))) IS NOT TRUE").await.unwrap().len(), 4);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert!(conn.query("SELECT rowid FROM probes WHERE (value IN (SELECT value FROM json_each(count_json('[')))) IS NOT TRUE").await.is_err());
        // Error cleanup must leave the next execution with a fresh memo.
        calls.store(0, Ordering::Relaxed);
        assert_eq!(conn.query("SELECT rowid FROM probes WHERE (value IN (SELECT value FROM json_each(count_json('[3]')))) IS TRUE").await.unwrap().len(), 1);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        // Even an override with compatible columns must not inherit the
        // built-in identity proof. Use the real JSON-tree implementation.
        conn.register_module("JSON_EACH", Box::new(fsqlite_func::vtab::module_factory_from::<fsqlite_ext_json::JsonTreeVtab>()));
        calls.store(0, Ordering::Relaxed);
        let rows = conn.query("SELECT rowid FROM probes WHERE (value IN (SELECT value FROM json_each(count_json('[0,1]')))) IS TRUE ORDER BY rowid").await.unwrap();
        assert_eq!(rows.iter().map(|r| r.values()[0].clone()).collect::<Vec<_>>(), vec![SqliteValue::Integer(1), SqliteValue::Integer(2)]);
        assert_eq!(calls.load(Ordering::Relaxed), 4, "module overrides must retain conservative correlation handling");
        conn.close().await.unwrap();
    });
}
