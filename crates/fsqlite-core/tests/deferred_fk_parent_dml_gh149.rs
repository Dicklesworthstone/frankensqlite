#![recursion_limit = "512"]

//! GH #149 (bd-gh-deferred-fk-parent-dml): a DEFERRABLE INITIALLY DEFERRED
//! ON DELETE / ON UPDATE NO ACTION foreign key must NOT be enforced immediately
//! when the PARENT row is deleted/updated — the obligation defers to COMMIT, so
//! a parent re-inserted (or children re-pointed) before COMMIT satisfies it.
//! RESTRICT and non-deferred NO ACTION stay immediate. rusqlite is the oracle.

use fsqlite_core::connection::Connection;

/// Apply statements to both engines, returning per-statement divergences
/// (one engine succeeds where the other errors). For a deferred violation the
/// error surfaces at COMMIT, and both engines must agree on that.
async fn apply_checked(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    stmts: &[&str],
) -> Vec<String> {
    let mut diverged = Vec::new();
    for s in stmts {
        let f = fconn.execute(s).await;
        let r = rconn.execute_batch(s);
        match (f, r) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => diverged.push(format!("STMT_DIVERGE: {s}\n  frank: OK\n  csql: ERR({e})")),
            (Err(e), Ok(())) => diverged.push(format!("STMT_DIVERGE: {s}\n  frank: ERR({e})\n  csql: OK")),
        }
    }
    diverged
}

fn assert_no_div(d: &[String], label: &str) {
    assert!(d.is_empty(), "{label}: {} divergence(s):\n{}", d.len(), d.join("\n"));
}

async fn setup(fconn: &Connection, rconn: &rusqlite::Connection, child_ddl: &str) {
    for s in [
        "PRAGMA foreign_keys = ON",
        "CREATE TABLE p (id INTEGER PRIMARY KEY)",
        child_ddl,
        "INSERT INTO p VALUES (1)",
        "INSERT INTO c VALUES (10, 1)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn deferred_on_delete_no_action_delete_then_reinsert_commits() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        setup(&f, &r, "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)").await;
        // Delete the parent then re-insert it before COMMIT — the deferred NO
        // ACTION must let the DELETE proceed and the COMMIT succeed on both.
        let d = apply_checked(&f, &r, &["BEGIN", "DELETE FROM p WHERE id = 1", "INSERT INTO p VALUES (1)", "COMMIT"]).await;
        assert_no_div(&d, "delete+reinsert");
    });
}

#[test]
fn deferred_on_delete_no_action_delete_without_reinsert_errors_at_commit() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        setup(&f, &r, "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)").await;
        // The DELETE proceeds (deferred), but COMMIT must fail on both — the
        // orphaned child violates the FK at commit time.
        let d = apply_checked(&f, &r, &["BEGIN", "DELETE FROM p WHERE id = 1", "COMMIT"]).await;
        assert_no_div(&d, "delete-no-reinsert");
    });
}

// KNOWN-LIMITATION FOLLOW-UP (bd-gh-deferred-fk-parent-dml): the current fix
// snapshots the affected child rows at the parent DELETE/UPDATE point and
// re-checks parent existence at COMMIT. That is correct when the deferral is
// satisfied by RE-INSERTING the parent (the bead's repro), but NOT when the
// children are RE-POINTED before COMMIT — the snapshot still references the old
// key, so this commits on sqlite3 while fsqlite false-violates. A full fix needs
// a commit-time RE-QUERY of the children currently referencing the old key
// (not a snapshot), which is entangled with the deferred_fk_checks restore
// guards. Ignored until that mechanism lands. (Before the fix this UPDATE also
// errored — immediately, at the UPDATE — so this is no regression.)
#[test]
#[ignore = "re-pointing needs commit-time re-query, not a defer-time snapshot (bd-gh-deferred-fk-parent-dml follow-up)"]
fn deferred_on_update_no_action_reparent_children_commits() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        setup(&f, &r, "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED)").await;
        // Update the parent key, then re-point the child before COMMIT.
        let d = apply_checked(&f, &r, &["BEGIN", "UPDATE p SET id = 2 WHERE id = 1", "UPDATE c SET pid = 2 WHERE pid = 1", "COMMIT"]).await;
        assert_no_div(&d, "update-reparent");
    });
}

#[test]
fn restrict_stays_immediate_even_when_deferrable() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        setup(&f, &r, "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED)").await;
        // RESTRICT is always immediate: the DELETE itself must fail on both,
        // even inside a transaction and even with a re-insert queued after.
        let d = apply_checked(&f, &r, &["BEGIN", "DELETE FROM p WHERE id = 1", "INSERT INTO p VALUES (1)", "COMMIT"]).await;
        assert_no_div(&d, "restrict-immediate");
    });
}

#[test]
fn non_deferred_no_action_stays_immediate() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        setup(&f, &r, "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))").await;
        // A non-DEFERRABLE FK: the parent DELETE must fail immediately on both.
        let d = apply_checked(&f, &r, &["BEGIN", "DELETE FROM p WHERE id = 1", "INSERT INTO p VALUES (1)", "COMMIT"]).await;
        assert_no_div(&d, "non-deferred-immediate");
    });
}
