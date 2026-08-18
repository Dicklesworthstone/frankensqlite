//! GH #184 (bd-qfgsa): a *persistent* (non-TEMP) `CREATE VIEW` whose body
//! references an ATTACHED database must be rejected at prepare, matching stock
//! SQLite. A `CREATE TEMP VIEW` may reference an attached database.
//!
//! Oracle (sqlite3 3.46.1, verified by hand):
//! ```text
//! ATTACH 'aux.db' AS aux; CREATE VIEW v AS SELECT * FROM aux.t;
//!   => Error: view v cannot reference objects in database aux
//! CREATE TEMP VIEW tv AS SELECT * FROM aux.t;      => allowed, selects rows
//! CREATE VIEW okv AS SELECT * FROM keep;           => allowed
//! CREATE VIEW vjoin AS SELECT * FROM keep
//!     WHERE x IN (SELECT a FROM aux.t);            => rejected (subquery ref)
//! ```
//!
//! The error text is matched verbatim: `view <name> cannot reference objects in
//! database <schema>`, where `<name>` is the view's bare name and `<schema>` is
//! the offending (attached) schema qualifier.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static UNIQ: AtomicU64 = AtomicU64::new(0);

/// A fresh, unique scratch directory under the system temp dir. Left in place
/// on purpose (deletion-averse): scratch files are intentionally not cleaned.
fn scratch_dir() -> PathBuf {
    let n = UNIQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "fsqlite-gh184-{}-{}-{}",
        std::process::id(),
        n,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// Build a file-backed main DB plus a real attached `aux.db` file that holds a
/// single-row table `t(a)`. Returns the opened connection with `aux` attached.
async fn setup_persistent_main_with_attached_aux() -> (Connection, PathBuf) {
    let dir = scratch_dir();
    let main_path = dir.join("main.db");
    let aux_path = dir.join("aux.db");

    // Seed the attached database file first with a real table + row.
    {
        let aux = Connection::open(aux_path.to_str().unwrap())
            .await
            .expect("open aux.db");
        aux.execute("CREATE TABLE t(a INTEGER);")
            .await
            .expect("create aux.t");
        aux.execute("INSERT INTO t VALUES (42);")
            .await
            .expect("insert aux.t");
    }

    // File-backed (persistent) main DB with its own table, then ATTACH aux.
    let conn = Connection::open(main_path.to_str().unwrap())
        .await
        .expect("open main.db");
    conn.execute("CREATE TABLE keep(x INTEGER);")
        .await
        .expect("create main.keep");
    conn.execute("INSERT INTO keep VALUES (1);")
        .await
        .expect("insert main.keep");
    conn.execute(&format!("ATTACH '{}' AS aux;", aux_path.to_str().unwrap()))
        .await
        .expect("attach aux");

    (conn, dir)
}

#[test]
fn test_persistent_view_referencing_attached_db_is_rejected() {
    asupersync::test_utils::run_test(|| async {
        let (conn, _dir) = setup_persistent_main_with_attached_aux().await;

        let err = conn
            .execute("CREATE VIEW v AS SELECT * FROM aux.t;")
            .await
            .expect_err("persistent view referencing attached aux must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("cannot reference objects in database aux"),
            "unexpected error message: {msg:?} (err = {err:?})"
        );
        // Stock SQLite uses the view's bare name; confirm the full phrasing.
        assert!(
            msg.contains("view v cannot reference objects in database aux"),
            "error text must match stock SQLite verbatim, got: {msg:?}"
        );
    });
}

#[test]
fn test_persistent_view_referencing_attached_db_in_subquery_is_rejected() {
    asupersync::test_utils::run_test(|| async {
        let (conn, _dir) = setup_persistent_main_with_attached_aux().await;

        let err = conn
            .execute("CREATE VIEW vjoin AS SELECT * FROM keep WHERE x IN (SELECT a FROM aux.t);")
            .await
            .expect_err("persistent view referencing attached aux via subquery must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("view vjoin cannot reference objects in database aux"),
            "unexpected error message: {msg:?} (err = {err:?})"
        );
    });
}

#[test]
fn test_temp_view_referencing_attached_db_is_allowed_and_selects() {
    asupersync::test_utils::run_test(|| async {
        let (conn, _dir) = setup_persistent_main_with_attached_aux().await;

        conn.execute("CREATE TEMP VIEW tv AS SELECT a FROM aux.t;")
            .await
            .expect("temp view referencing attached aux must be allowed");

        let rows = conn
            .query("SELECT a FROM tv;")
            .await
            .expect("temp view over attached aux must select");
        assert_eq!(rows.len(), 1, "expected exactly one row from tv");
        assert_eq!(rows[0].values(), &[SqliteValue::Integer(42)]);
    });
}

#[test]
fn test_persistent_view_referencing_only_main_is_allowed() {
    asupersync::test_utils::run_test(|| async {
        let (conn, _dir) = setup_persistent_main_with_attached_aux().await;

        // Unqualified + explicit main. references are fine on a persistent view.
        conn.execute("CREATE VIEW okv AS SELECT x FROM keep;")
            .await
            .expect("persistent view over main-only tables must be allowed");
        conn.execute("CREATE VIEW okv_main AS SELECT x FROM main.keep;")
            .await
            .expect("persistent view using main. qualifier must be allowed");

        let rows = conn
            .query("SELECT x FROM okv;")
            .await
            .expect("main-only persistent view must select");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values(), &[SqliteValue::Integer(1)]);
    });
}
