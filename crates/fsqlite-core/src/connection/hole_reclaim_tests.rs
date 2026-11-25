//! bd-ioq6x R3: a clean `Connection::close` folds the connection's own
//! abandonment pool, so a file closed cleanly has no EOF holes at rest.
//!
//! The shape is the Q1 rollback case inside the connection's OWN pager: a
//! CONCURRENT holder allocates EOF pages (two written plus an unused lease), a
//! CONCURRENT grower allocates past them and commits, and the holder rolls
//! back. Its pages, now below the page count, sit in the connection's pool.
//! Without the close-time fold the pool is disowned at drop and dies with the
//! process: stock then reports every held page as "never used" (red on the
//! parent and on the no-fold mutant). The grower's page is opaque payload, so
//! stock reports it as never used on every arm; the keeper measures only the
//! held pages.

#[allow(clippy::wildcard_imports)]
use super::*;

use std::collections::BTreeSet;

const PAGE: usize = 4096;

/// The holder allocates (and writes) until it holds an unused EOF lease; pages
/// it takes from the freelist first go back to the freelist on rollback, so
/// the held set is only what lies past `base` (the page count before) plus
/// the lease.
async fn q1_rollback_shape<V>(pager: &SimplePager<V>, cx: &Cx, base: u32) -> BTreeSet<u32>
where
    V: Vfs + Send + Sync + 'static,
    V::File: Send + Sync + 'static,
{
    let mut holder = pager
        .begin(cx, TransactionMode::Concurrent)
        .await
        .expect("holder begin");
    let mut written = Vec::new();
    let mut lease = BTreeSet::new();
    for _ in 0..64 {
        let page = holder.allocate_page(cx).await.expect("holder allocate");
        holder
            .write_page(cx, page, &[0x52; PAGE])
            .await
            .expect("holder write");
        written.push(page.get());
        lease = holder
            .live_reserved_pages()
            .iter()
            .map(|page| page.get())
            .collect();
        if !lease.is_empty() {
            break;
        }
    }
    assert!(
        !lease.is_empty(),
        "precondition: the holder must hold an unused EOF lease (allocated {written:?}, base {base})"
    );
    let mut grow = pager
        .begin(cx, TransactionMode::Concurrent)
        .await
        .expect("grow begin");
    let g = grow.allocate_page(cx).await.expect("g");
    grow.write_page(cx, g, &[0x54; PAGE])
        .await
        .expect("g write");
    assert!(
        written.iter().chain(&lease).all(|page| *page < g.get()),
        "precondition: the grower allocates past the holder (written={written:?} lease={lease:?} g={g:?})"
    );
    grow.commit(cx).await.expect("grow commit");
    drop(grow);
    holder.rollback(cx).await.expect("holder rollback");
    drop(holder);
    written
        .into_iter()
        .filter(|page| *page > base)
        .chain(lease)
        .collect()
}

fn stock_never_used(path: &std::path::Path) -> (BTreeSet<u32>, Vec<String>, i64) {
    let stock = rusqlite::Connection::open(path).expect("stock open");
    let report: Vec<String> = {
        let mut stmt = stock
            .prepare("PRAGMA integrity_check(100000)")
            .expect("prepare integrity_check");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("integrity_check")
            .collect::<std::result::Result<_, _>>()
            .expect("integrity rows")
    };
    let mut never_used = BTreeSet::new();
    for line in report.iter().flat_map(|row| row.lines()) {
        if line.contains("never used") {
            let digits: String = line
                .trim_start_matches(|c: char| !c.is_ascii_digit())
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            if let Ok(page) = digits.parse() {
                never_used.insert(page);
            }
        }
    }
    let rows = stock
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .expect("row count");
    (never_used, report, rows)
}

#[test]
fn clean_close_folds_rolled_back_eof_pages_below_a_peer_growth() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("r3.db");
        let conn = Connection::open(path.to_string_lossy().into_owned())
            .await
            .expect("open");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("create");
        conn.execute("BEGIN").await.expect("begin");
        for id in 0..60 {
            conn.execute(&format!(
                "INSERT INTO t VALUES({id}, '{}')",
                "r3-row-".repeat(40)
            ))
            .await
            .expect("insert");
        }
        conn.execute("COMMIT").await.expect("commit");
        let base = match conn.query("PRAGMA page_count").await.expect("page_count")[0].values()[0] {
            SqliteValue::Integer(count) => u32::try_from(count).expect("page count"),
            ref other => panic!("page_count returned {other:?}"),
        };
        let cx = Cx::new();
        let held = match &conn.pager {
            #[cfg(target_os = "linux")]
            PagerBackend::IoUring(pager) => q1_rollback_shape(pager, &cx, base).await,
            PagerBackend::Unix(pager) => q1_rollback_shape(pager, &cx, base).await,
            PagerBackend::Memory(_) => panic!("a file-backed pager is expected"),
        };
        assert!(!held.is_empty(), "precondition: held EOF pages");
        conn.close().await.expect("close");
        let (never_used, report, rows) = stock_never_used(&path);
        let leaked: Vec<u32> = held
            .iter()
            .copied()
            .filter(|page| never_used.contains(page))
            .collect();
        assert!(
            leaked.is_empty(),
            "bd-ioq6x R3: a clean close must fold the connection's own pool; stock still reports \
             held pages as never used: leaked={leaked:?} held={held:?} report={report:?}"
        );
        assert_eq!(rows, 60, "rows intact");
    });
}
