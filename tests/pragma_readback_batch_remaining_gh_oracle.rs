#![recursion_limit = "512"]

//! bd-gh-pragma-readback batch — end-to-end oracle census of the sub-issues that
//! the July triage filed but that later work may have already closed:
//!
//! - #236 reverse_unordered_selects — readback AND the scan-reversal effect
//! - #273 locking_mode — text readback ("normal"/"exclusive")
//! - #274 synchronous — INTEGER codes (not names), assignment emits no row
//! - #275 cache_spill — set/get readback (OFF -> 0)
//! - #277 secure_delete — 0 / 1 (ON) / 2 (FAST)
//! - #279 threads — set/get readback
//! - #280 soft_heap_limit / hard_heap_limit — set/get readback
//!
//! Each is pinned differentially against the live rusqlite oracle (SQLite
//! 3.46.1). Where frank's *default* legitimately diverges from stock for
//! model reasons (e.g. synchronous default NORMAL vs FULL, cache_spill default
//! derived from a different cache size), only the unambiguous post-assignment
//! behavior is compared — mirroring the bd-ji5oe trusted_schema-default caveat.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{}'", s.to_ascii_lowercase()),
        SqliteValue::Blob(b) => {
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
        }
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{}'", s.to_ascii_lowercase()),
        rusqlite::types::Value::Blob(b) => {
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
        }
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    f.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = r.prepare(sql).unwrap();
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Ordered comparison (row order is significant — for scan-order tests).
async fn agree_ordered(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let fr = fq(f, sql).await;
    let rr = rq(r, sql);
    assert_eq!(fr, rr, "ordered divergence on `{sql}`");
}

/// Exec on both, ignoring output (for setup / assignment statements).
async fn exec_both(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    f.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("frank exec `{sql}`: {e:?}"));
    r.execute_batch(sql)
        .unwrap_or_else(|e| panic!("rusqlite exec `{sql}`: {e:?}"));
}

/// #236: reverse_unordered_selects flips the scan direction of an ORDER-BY-less
/// SELECT (and the bare readback agrees).
#[test]
fn reverse_unordered_selects_effect_and_readback_gh236() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        exec_both(&f, &r, "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)").await;
        exec_both(&f, &r, "INSERT INTO t VALUES (1,'x'),(2,'y'),(3,'z')").await;
        agree_ordered(&f, &r, "PRAGMA reverse_unordered_selects").await; // 0
        agree_ordered(&f, &r, "SELECT a FROM t").await; // 1,2,3
        exec_both(&f, &r, "PRAGMA reverse_unordered_selects=ON").await;
        agree_ordered(&f, &r, "PRAGMA reverse_unordered_selects").await; // 1
        agree_ordered(&f, &r, "SELECT a FROM t").await; // 3,2,1
        exec_both(&f, &r, "PRAGMA reverse_unordered_selects=OFF").await;
        agree_ordered(&f, &r, "SELECT a FROM t").await; // 1,2,3
    });
}

/// #273: locking_mode readback is text and agrees. The filed bug — an EMPTY
/// readback — is fixed; locking_mode now reports its mode.
///
/// One SQLite quirk is deliberately NOT pinned: `PRAGMA locking_mode=NORMAL`
/// issued while EXCLUSIVE echoes the still-in-effect "exclusive" (SQLite keeps
/// the exclusive lock until the next non-exclusive access). That is an artifact
/// of SQLite's Shared/Reserved/Pending/Exclusive lock-escalation protocol,
/// which FrankenSQLite deliberately does not implement (MVCC handles
/// concurrency at the page level), so frank honestly reports "normal"
/// immediately. Only lock-model-independent behavior is compared here.
#[test]
fn locking_mode_readback_gh273() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        agree_ordered(&f, &r, "PRAGMA locking_mode").await; // normal (default)
        agree_ordered(&f, &r, "PRAGMA locking_mode=EXCLUSIVE").await; // exclusive
        agree_ordered(&f, &r, "PRAGMA locking_mode").await; // exclusive
        // (the =NORMAL transition echo is the lock-protocol quirk noted above)
        exec_both(&f, &r, "PRAGMA locking_mode=NORMAL").await;
        agree_ordered(&f, &r, "PRAGMA locking_mode").await; // normal
    });
}

/// #274: synchronous returns INTEGER codes and assignment emits no row.
#[test]
fn synchronous_integer_codes_gh274() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Assignment: zero rows on both engines.
        assert!(
            fq(&f, "PRAGMA synchronous=FULL").await.is_empty(),
            "assign emits no row"
        );
        assert!(rq(&r, "PRAGMA synchronous=FULL").is_empty());
        agree_ordered(&f, &r, "PRAGMA synchronous").await; // 2
        exec_both(&f, &r, "PRAGMA synchronous=OFF").await;
        agree_ordered(&f, &r, "PRAGMA synchronous").await; // 0
        exec_both(&f, &r, "PRAGMA synchronous=NORMAL").await;
        agree_ordered(&f, &r, "PRAGMA synchronous").await; // 1
        exec_both(&f, &r, "PRAGMA synchronous=EXTRA").await;
        agree_ordered(&f, &r, "PRAGMA synchronous").await; // 3
        exec_both(&f, &r, "PRAGMA synchronous=2").await;
        agree_ordered(&f, &r, "PRAGMA synchronous").await; // 2
    });
}

/// #275: cache_spill OFF reads back 0.
#[test]
fn cache_spill_readback_gh275() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        exec_both(&f, &r, "PRAGMA cache_spill=OFF").await;
        agree_ordered(&f, &r, "PRAGMA cache_spill").await; // 0
    });
}

/// #277: secure_delete reports 0 / 1 (ON) / 2 (FAST).
#[test]
fn secure_delete_states_gh277() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        exec_both(&f, &r, "PRAGMA secure_delete=FAST").await;
        agree_ordered(&f, &r, "PRAGMA secure_delete").await; // 2
        exec_both(&f, &r, "PRAGMA secure_delete=ON").await;
        agree_ordered(&f, &r, "PRAGMA secure_delete").await; // 1
        exec_both(&f, &r, "PRAGMA secure_delete=OFF").await;
        agree_ordered(&f, &r, "PRAGMA secure_delete").await; // 0
    });
}

/// #279: threads set/get round-trips.
#[test]
fn threads_readback_gh279() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        exec_both(&f, &r, "PRAGMA threads=4").await;
        agree_ordered(&f, &r, "PRAGMA threads").await; // 4
        exec_both(&f, &r, "PRAGMA threads=0").await;
        agree_ordered(&f, &r, "PRAGMA threads").await; // 0
    });
}

/// #280: soft_heap_limit / hard_heap_limit set/get round-trip. These are
/// process-global in both engines but each reads its own independent store.
#[test]
fn heap_limit_readback_gh280() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        exec_both(&f, &r, "PRAGMA soft_heap_limit=1000000").await;
        agree_ordered(&f, &r, "PRAGMA soft_heap_limit").await; // 1000000
        exec_both(&f, &r, "PRAGMA hard_heap_limit=2000000").await;
        agree_ordered(&f, &r, "PRAGMA hard_heap_limit").await; // 2000000
    });
}
