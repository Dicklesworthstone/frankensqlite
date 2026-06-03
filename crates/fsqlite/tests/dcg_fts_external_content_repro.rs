//! Repro for the dcg (destructive_command_guard) history-FTS failure: an
//! external-content FTS5 table (`content='...', content_rowid='id'`) over an
//! `INTEGER PRIMARY KEY AUTOINCREMENT` base table, mirrored by an AFTER INSERT
//! trigger, raises `Sqlite(PrimaryKeyViolation)` on the 2nd insert and on
//! rebuild (DELETE + row-by-row re-insert). See frankensqlite#94.
//!
//! Run: `cargo test -p fsqlite --features fts5 --test dcg_fts_external_content_repro`

#![cfg(feature = "fts5")]

use fsqlite::Connection;

fn setup(conn: &Connection) {
    conn.execute(
        "CREATE TABLE commands (\
            id INTEGER PRIMARY KEY AUTOINCREMENT,\
            command TEXT NOT NULL\
        )",
    )
    .expect("create commands");
    conn.execute(
        "CREATE VIRTUAL TABLE commands_fts USING fts5(\
            command,\
            content='commands',\
            content_rowid='id'\
        )",
    )
    .expect("create external-content fts5");
    conn.execute(
        "CREATE TRIGGER commands_fts_insert AFTER INSERT ON commands BEGIN \
            INSERT INTO commands_fts(rowid, command) VALUES (new.id, new.command); \
        END",
    )
    .expect("create insert trigger");
}

#[test]
fn dcg_log_command_sequence_does_not_violate_pk() {
    let conn = Connection::open(":memory:").expect("open");
    setup(&conn);

    // 1st insert (dcg test_prune logs old_entry here) — should be id=1.
    conn.execute("INSERT INTO commands(command) VALUES ('first command')")
        .expect("1st insert");
    let n1 = conn
        .query("SELECT id FROM commands")
        .expect("count after 1st")
        .len();
    assert_eq!(n1, 1, "exactly one base row after first insert");

    // 2nd insert (dcg test_prune logs recent_entry here) — should be id=2.
    // This is the line that panics in dcg with Sqlite(PrimaryKeyViolation).
    conn.execute("INSERT INTO commands(command) VALUES ('second command')")
        .expect("2nd insert must not raise PrimaryKeyViolation");

    let ids: Vec<i64> = conn
        .query("SELECT id FROM commands ORDER BY id")
        .expect("select ids")
        .iter()
        .map(|r| match &r.values()[0] {
            fsqlite::SqliteValue::Integer(i) => *i,
            other => panic!("unexpected id value: {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2], "autoincrement must yield distinct rowids");
}

// frankensqlite#94: `DELETE FROM <external-content fts5>` with no WHERE clause
// must enumerate every live rowid and route each through the FTS5 module's
// xUpdate delete branch, clearing both the in-memory documents/index and the
// backing storage records. Without that, a subsequent re-INSERT of the same
// rowid tripped the PrimaryKeyViolation guard in `Fts5Table::update`. The
// connection now intercepts live virtual-table DELETE (see
// `Connection::execute_live_vtab_delete`) so the rebuild pattern works.
#[test]
fn dcg_rebuild_fts_does_not_violate_pk() {
    let conn = Connection::open(":memory:").expect("open");
    setup(&conn);

    for i in 0..4 {
        conn.execute(&format!("INSERT INTO commands(command) VALUES ('cmd {i}')"))
            .expect("seed insert");
    }

    // Mirror dcg::rebuild_fts: drop trigger, clear external-content index,
    // repopulate row-by-row, recreate trigger.
    conn.execute("DROP TRIGGER IF EXISTS commands_fts_insert")
        .expect("drop trigger");
    conn.execute("DELETE FROM commands_fts").expect("clear fts");

    let rows = conn
        .query("SELECT id, command FROM commands")
        .expect("select source rows");
    for row in &rows {
        let vals = row.values();
        conn.execute_with_params(
            "INSERT INTO commands_fts(rowid, command) VALUES (?1, ?2)",
            &[vals[0].clone(), vals[1].clone()],
        )
        .expect("rebuild re-insert must not raise PrimaryKeyViolation");
    }

    let fts_count = conn
        .query("SELECT rowid FROM commands_fts")
        .expect("count fts")
        .len();
    assert_eq!(fts_count, 4, "all rows re-indexed after rebuild");
}
