#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
#[test]
fn rp() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("rp.db");
        let s = p.to_string_lossy().into_owned();
        let conn = Connection::open(&s).await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE f USING fts5(body, content='');").await.unwrap();
        conn.execute("INSERT INTO f(rowid, body) VALUES (1, 'hello world');").await.unwrap();
        let rows = conn.query("SELECT name, type, rootpage FROM sqlite_master ORDER BY name;").await.unwrap();
        for r in &rows { eprintln!("{:?}", r.values()); }
        conn.close().await.unwrap();
        let stock = rusqlite::Connection::open(&p).unwrap();
        let mut st = stock.prepare("SELECT name, type, rootpage FROM sqlite_master ORDER BY name").unwrap();
        let out: Vec<(String,String,i64)> = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap().collect::<Result<_,_>>().unwrap();
        eprintln!("STOCK {out:?}");
    });
}
