//! GH#521 keeper: index B-trees must stay dense when inserts land left of
//! full siblings.
//!
//! Index leaves used a left-packing distribution with no equalization pass,
//! so each balance filled the first three siblings to capacity. When the
//! insert hot spot sat on the leftmost of them (descending keys, or a DESC
//! column in a composite index such as `(tool_name, collected_ts DESC)`), the
//! very next insert overflowed that leaf again and every balance stranded one
//! more leaf holding a single cell. 12,000 descending integers produced 5,384
//! index leaves (5,381 with one cell) and a 22 MB file where stock SQLite
//! needs 56 leaves and 344 KB. `PRAGMA integrity_check` stayed `ok`.
//!
//! These tests run the reported workloads through FrankenSQLite, then inspect
//! the committed file with stock SQLite's `dbstat` next to a stock-SQLite
//! oracle built from the same statements.

use tempfile::TempDir;

#[derive(Debug, Clone, Copy)]
struct LeafStats {
    leaves: i64,
    single_cell_leaves: i64,
    cells: i64,
}

fn index_leaf_stats(conn: &rusqlite::Connection, index: &str) -> LeafStats {
    conn.query_row(
        "SELECT count(*), coalesce(sum(ncell = 1), 0), coalesce(sum(ncell), 0)
         FROM dbstat WHERE name = ?1 AND pagetype = 'leaf'",
        [index],
        |row| {
            Ok(LeafStats {
                leaves: row.get(0)?,
                single_cell_leaves: row.get(1)?,
                cells: row.get(2)?,
            })
        },
    )
    .unwrap_or_else(|error| panic!("dbstat for {index}: {error}"))
}

/// Run `schema` and then `inserts` inside one transaction through
/// FrankenSQLite, checkpoint so the main file holds the committed image, and
/// build a stock-SQLite oracle from the same statements. Returns
/// `(frank, oracle)` read-only stock connections.
async fn build_pair(
    dir: &TempDir,
    name: &str,
    schema: &str,
    inserts: &[String],
) -> (rusqlite::Connection, rusqlite::Connection) {
    let frank_path = dir.path().join(format!("{name}-frank.db"));
    let oracle_path = dir.path().join(format!("{name}-oracle.db"));

    {
        let conn = fsqlite::Connection::open(frank_path.to_string_lossy().into_owned())
            .await
            .expect("open fsqlite database");
        conn.execute_batch(schema).await.expect("fsqlite schema");
        conn.execute("BEGIN").await.expect("fsqlite BEGIN");
        for statement in inserts {
            conn.execute(statement)
                .await
                .unwrap_or_else(|error| panic!("fsqlite `{statement}`: {error}"));
        }
        conn.execute("COMMIT").await.expect("fsqlite COMMIT");
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .expect("fsqlite checkpoint");
        conn.close().await.expect("fsqlite close");
    }

    {
        let mut oracle = rusqlite::Connection::open(&oracle_path).expect("open oracle");
        oracle.execute_batch(schema).expect("oracle schema");
        let tx = oracle.transaction().expect("oracle BEGIN");
        for statement in inserts {
            tx.execute_batch(statement).expect("oracle insert");
        }
        tx.commit().expect("oracle COMMIT");
    }

    let frank = rusqlite::Connection::open(&frank_path).expect("stock reopen of fsqlite file");
    let integrity: String = frank
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("stock integrity_check");
    assert_eq!(integrity, "ok", "{name}: stock integrity_check");
    let oracle = rusqlite::Connection::open(&oracle_path).expect("reopen oracle");
    (frank, oracle)
}

fn assert_index_as_dense_as_sqlite(
    name: &str,
    index: &str,
    entries: i64,
    frank: &rusqlite::Connection,
    oracle: &rusqlite::Connection,
) {
    let produced = index_leaf_stats(frank, index);
    let expected = index_leaf_stats(oracle, index);
    eprintln!("{name}: {index} leaf pages: fsqlite {produced:?}, sqlite {expected:?}");
    // Every interior cell of an index B-tree is an entry, and a B-tree with
    // L leaves has exactly L - 1 interior cells.
    for (engine, stats) in [("fsqlite", produced), ("sqlite", expected)] {
        assert_eq!(
            stats.cells + stats.leaves - 1,
            entries,
            "{name}: {engine} {index} must hold every entry exactly once ({stats:?})"
        );
    }
    assert_eq!(
        produced.single_cell_leaves, 0,
        "{name}: {index} has single-cell leaves (fsqlite {produced:?}, sqlite {expected:?})"
    );
    assert!(
        produced.leaves * 4 <= expected.leaves * 5,
        "{name}: {index} uses {} leaves, more than 1.25x stock SQLite's {} \
         (fsqlite {produced:?}, sqlite {expected:?})",
        produced.leaves,
        expected.leaves
    );
}

/// The issue's reproduction: 12,000 descending integers into a plain index.
#[test]
fn gh521_descending_integer_index_stays_dense() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().expect("tempdir");
        let inserts: Vec<String> = (1..=12_000)
            .rev()
            .map(|value| format!("INSERT INTO a VALUES({value})"))
            .collect();
        let (frank, oracle) = build_pair(
            &dir,
            "descending",
            "CREATE TABLE a(x INTEGER); CREATE INDEX ia ON a(x);",
            &inserts,
        )
        .await;
        assert_index_as_dense_as_sqlite("descending", "ia", 12_000, &frank, &oracle);

        let via_index: Vec<i64> = frank
            .prepare("SELECT x FROM a INDEXED BY ia ORDER BY x")
            .and_then(|mut stmt| {
                stmt.query_map([], |row| row.get(0))?
                    .collect::<rusqlite::Result<Vec<i64>>>()
            })
            .expect("scan index");
        assert_eq!(via_index, (1..=12_000).collect::<Vec<i64>>());
    });
}

/// The production shape: a composite index with a DESC column, fed
/// increasing timestamps round-robin across a few groups, so every insert
/// lands at the front of its group in the middle of the index.
#[test]
fn gh521_composite_desc_index_hot_spots_stay_dense() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().expect("tempdir");
        let tools = ["fetch_inbox", "send_message", "search", "reserve_paths"];
        let inserts: Vec<String> = (0..8_000_i64)
            .map(|ts| {
                let tool = tools[usize::try_from(ts).expect("small") % tools.len()];
                format!(
                    "INSERT INTO snapshots(tool_name, collected_ts, payload) \
                     VALUES('{tool}', {}, 'p{ts}')",
                    1_700_000_000_000_i64 + ts
                )
            })
            .collect();
        let (frank, oracle) = build_pair(
            &dir,
            "composite-desc",
            "CREATE TABLE snapshots(id INTEGER PRIMARY KEY, tool_name TEXT NOT NULL, \
             collected_ts INTEGER NOT NULL, payload TEXT); \
             CREATE INDEX snapshots_tool_ts ON snapshots(tool_name, collected_ts DESC);",
            &inserts,
        )
        .await;
        assert_index_as_dense_as_sqlite(
            "composite-desc",
            "snapshots_tool_ts",
            8_000,
            &frank,
            &oracle,
        );
    });
}
