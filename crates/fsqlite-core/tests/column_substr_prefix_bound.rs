//! GH#400: a bounded prefix projection `substr(col, 1, N)` over a large,
//! overflow-backed value must (a) return exactly what stock SQLite returns —
//! including for non-ASCII TEXT — and (b) read only a bounded slice of the
//! overflow chain, never the whole payload.
//!
//! Like `tests/octet_length_ingress_probe.rs`, this reads the PROCESS-GLOBAL
//! `btree_copy_profile` counters, so it lives in its own integration binary
//! (a fresh process no other test can perturb) and is single-threaded.
//!
//! Run:
//!   cargo test -p fsqlite-core --test column_substr_prefix_bound -- --test-threads=1

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const PREFIX_CHARS: usize = 32;

/// One big row: `id INTEGER PRIMARY KEY, content TEXT`. `content` is
/// `unit` repeated to `>= payload_bytes`, forcing an overflow chain.
async fn seed(db: &std::path::Path, unit: &str, payload_bytes: usize) -> (Connection, String) {
    let conn = Connection::open(db.to_string_lossy().into_owned())
        .await
        .unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, content TEXT NOT NULL);")
        .await
        .unwrap();
    let repeats = payload_bytes.div_ceil(unit.len());
    let content = unit.repeat(repeats);
    assert!(content.len() >= payload_bytes);
    conn.execute("BEGIN EXCLUSIVE").await.unwrap();
    conn.execute_with_params(
        "INSERT INTO t (id, content) VALUES (1, ?1);",
        &[SqliteValue::Text(content.clone().into())],
    )
    .await
    .unwrap();
    conn.execute("COMMIT").await.unwrap();
    (conn, content)
}

/// The exact `substr(content, 1, N)` stock SQLite yields for `content`.
fn stock_prefix(db: &std::path::Path, n: usize) -> String {
    let sqlite = rusqlite::Connection::open(db).expect("stock sqlite open");
    sqlite
        .query_row(
            "SELECT substr(content, 1, ?1) FROM t WHERE id = 1",
            [i64::try_from(n).unwrap()],
            |row| row.get::<_, String>(0),
        )
        .expect("stock substr")
}

async fn frank_prefix(conn: &Connection, n: usize) -> SqliteValue {
    let rows = conn
        .query(&format!("SELECT substr(content, 1, {n}) FROM t WHERE id = 1;"))
        .await
        .expect("frank substr");
    assert_eq!(rows.len(), 1);
    rows[0].values()[0].clone()
}

/// Run one `SELECT substr(content, 1, N)` under the copy profile and return
/// (result, overflow_bytes_read).
async fn measured_prefix(conn: &Connection, n: usize) -> (SqliteValue, u64) {
    fsqlite_btree::reset_btree_copy_profile();
    fsqlite_btree::set_btree_copy_profile_enabled(true);
    let value = frank_prefix(conn, n).await;
    let profile = fsqlite_btree::btree_copy_profile_snapshot();
    fsqlite_btree::set_btree_copy_profile_enabled(false);
    (value, profile.overflow_chain_overflow_bytes)
}

#[test]
fn large_unicode_substr_prefix_is_exact_and_bounded() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("substr-unicode.db");
        // 4-byte code points so char counting genuinely differs from bytes.
        let payload_bytes = 1024 * 1024;
        let (conn, content) = seed(&db, "\u{1D518}", payload_bytes).await;

        let (value, overflow_bytes) = measured_prefix(&conn, PREFIX_CHARS).await;

        // Correctness: exactly the first PREFIX_CHARS code points, and byte-
        // identical to stock SQLite.
        let expected: String = content.chars().take(PREFIX_CHARS).collect();
        assert_eq!(value, SqliteValue::Text(expected.clone().into()));
        drop(conn);
        assert_eq!(stock_prefix(&db, PREFIX_CHARS), expected);

        // Resource bound: the fast path read only a tiny window of the
        // overflow chain, not the ~1 MiB payload. Pre-fix (full read) this is
        // ~payload_bytes; the mutation-red discriminator.
        assert!(
            overflow_bytes < 64 * 1024,
            "bounded Unicode substr prefix read {overflow_bytes} overflow bytes of a \
             {payload_bytes}-byte payload; expected a small bounded window"
        );
    });
}

#[test]
fn large_ascii_substr_prefix_is_exact_and_bounded() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("substr-ascii.db");
        let payload_bytes = 1024 * 1024;
        let (conn, content) = seed(&db, "abcd", payload_bytes).await;

        let (value, overflow_bytes) = measured_prefix(&conn, PREFIX_CHARS).await;

        let expected: String = content.chars().take(PREFIX_CHARS).collect();
        assert_eq!(value, SqliteValue::Text(expected.clone().into()));
        drop(conn);
        assert_eq!(stock_prefix(&db, PREFIX_CHARS), expected);
        assert!(
            overflow_bytes < 64 * 1024,
            "bounded ASCII substr prefix read {overflow_bytes} overflow bytes; \
             expected a small bounded window"
        );
    });
}

#[test]
fn mixed_width_substr_prefix_matches_stock_across_lengths() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("substr-mixed.db");
        // 1-, 2-, 3-, and 4-byte code points interleaved.
        let payload_bytes = 512 * 1024;
        let (conn, content) = seed(&db, "a\u{00e9}\u{20ac}\u{1D518}", payload_bytes).await;

        // Sweep lengths that straddle the internal read budget (4*N+8 bytes)
        // and small-value fast-path threshold.
        for n in [0usize, 1, 2, 5, 31, 32, 33, 200, 4096] {
            let value = frank_prefix(&conn, n).await;
            let expected: String = content.chars().take(n).collect();
            assert_eq!(
                value,
                SqliteValue::Text(expected.clone().into()),
                "frank substr(content,1,{n}) mismatch"
            );
        }
        drop(conn);
        for n in [0usize, 1, 2, 5, 31, 32, 33, 200, 4096] {
            let expected: String = content.chars().take(n).collect();
            assert_eq!(stock_prefix(&db, n), expected, "stock substr(content,1,{n})");
        }
    });
}

#[test]
fn small_unicode_value_substr_prefix_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        // Below the internal budget: exercises the deferred generic path.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("substr-small.db");
        let conn = Connection::open(db.to_string_lossy().into_owned())
            .await
            .unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, content TEXT NOT NULL);")
            .await
            .unwrap();
        let content = "\u{00e9}\u{20ac}\u{1D518}heart\u{2764}tail".to_owned();
        conn.execute_with_params(
            "INSERT INTO t (id, content) VALUES (1, ?1);",
            &[SqliteValue::Text(content.clone().into())],
        )
        .await
        .unwrap();

        for n in [0usize, 1, 3, 6, 100] {
            let value = frank_prefix(&conn, n).await;
            let expected: String = content.chars().take(n).collect();
            assert_eq!(value, SqliteValue::Text(expected.into()));
        }
        drop(conn);
        for n in [0usize, 1, 3, 6, 100] {
            let expected: String = content.chars().take(n).collect();
            assert_eq!(stock_prefix(&db, n), expected);
        }
    });
}

// GH#400 (remaining gap): the prefix LENGTH is a BOUND PARAMETER, e.g.
// `substr(content, 1, ?1)`. Before the fix this fell to the generic
// full-materialize scalar path (hydrating the whole overflow-backed value);
// the register-mode `ColumnSubstrPrefix` opcode now reads only a bounded
// window while staying byte-identical to stock SQLite.

/// `substr(content, 1, ?1)` in FrankenSQLite with the length supplied as a
/// bound parameter (not folded into the SQL text).
async fn frank_prefix_bound(conn: &Connection, len: SqliteValue) -> SqliteValue {
    let rows = conn
        .query_with_params("SELECT substr(content, 1, ?1) FROM t WHERE id = 1;", &[len])
        .await
        .expect("frank substr (bound param)");
    assert_eq!(rows.len(), 1);
    rows[0].values()[0].clone()
}

/// Run one bound-parameter `substr` under the copy profile and return
/// (result, overflow_bytes_read).
async fn measured_prefix_bound(conn: &Connection, len: SqliteValue) -> (SqliteValue, u64) {
    fsqlite_btree::reset_btree_copy_profile();
    fsqlite_btree::set_btree_copy_profile_enabled(true);
    let value = frank_prefix_bound(conn, len).await;
    let profile = fsqlite_btree::btree_copy_profile_snapshot();
    fsqlite_btree::set_btree_copy_profile_enabled(false);
    (value, profile.overflow_chain_overflow_bytes)
}

/// Stock `substr(content, 1, ?1)` with the length bound to an arbitrary value.
/// Returns `None` when stock yields NULL.
fn stock_prefix_bound(db: &std::path::Path, len: rusqlite::types::Value) -> Option<String> {
    let sqlite = rusqlite::Connection::open(db).expect("stock sqlite open");
    sqlite
        .query_row(
            "SELECT substr(content, 1, ?1) FROM t WHERE id = 1",
            [len],
            |row| row.get::<_, Option<String>>(0),
        )
        .expect("stock substr (bound)")
}

#[test]
fn large_unicode_substr_prefix_bound_param_is_exact_and_bounded() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("substr-unicode-bound.db");
        // 4-byte code points so char counting genuinely differs from bytes.
        let payload_bytes = 1024 * 1024;
        let (conn, content) = seed(&db, "\u{1D518}", payload_bytes).await;

        let prefix = i64::try_from(PREFIX_CHARS).unwrap();
        let (value, overflow_bytes) =
            measured_prefix_bound(&conn, SqliteValue::Integer(prefix)).await;

        // Correctness: exactly the first PREFIX_CHARS code points, byte-identical
        // to stock SQLite (bound the same way).
        let expected: String = content.chars().take(PREFIX_CHARS).collect();
        assert_eq!(value, SqliteValue::Text(expected.clone().into()));
        drop(conn);
        assert_eq!(
            stock_prefix_bound(&db, rusqlite::types::Value::Integer(prefix)),
            Some(expected)
        );

        // Resource bound: the register-mode fast path read only a tiny window
        // of the overflow chain, not the ~1 MiB payload. Pre-fix (bound param =
        // full generic read) this is ~payload_bytes; the RED discriminator.
        assert!(
            overflow_bytes < 64 * 1024,
            "bounded bound-param Unicode substr prefix read {overflow_bytes} overflow bytes of a \
             {payload_bytes}-byte payload; expected a small bounded window"
        );
    });
}

#[test]
fn bound_param_substr_prefix_matches_stock_across_lengths() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("substr-bound-mixed.db");
        // 1-, 2-, 3-, and 4-byte code points interleaved.
        let payload_bytes = 512 * 1024;
        let (conn, content) = seed(&db, "a\u{00e9}\u{20ac}\u{1D518}", payload_bytes).await;

        // Non-negative lengths straddling the internal read budget and the
        // small-value fast-path threshold, supplied as bound parameters.
        for n in [0i64, 1, 2, 5, 31, 32, 33, 200, 4096] {
            let value = frank_prefix_bound(&conn, SqliteValue::Integer(n)).await;
            let expected: String = content.chars().take(usize::try_from(n).unwrap()).collect();
            assert_eq!(
                value,
                SqliteValue::Text(expected.into()),
                "frank substr(content,1,?1={n}) mismatch"
            );
        }

        // A negative length with start position 1 yields the empty string, not
        // NULL and not a full read (stock parity).
        let neg = frank_prefix_bound(&conn, SqliteValue::Integer(-3)).await;
        assert_eq!(neg, SqliteValue::Text(String::new().into()));

        // A NULL length yields NULL (stock parity). The generic scalar path
        // mishandles this shape, so the bounded direct opcode must decide it.
        let null_len = frank_prefix_bound(&conn, SqliteValue::Null).await;
        assert_eq!(null_len, SqliteValue::Null);

        drop(conn);
        for n in [0usize, 1, 2, 5, 31, 32, 33, 200, 4096] {
            let expected: String = content.chars().take(n).collect();
            assert_eq!(
                stock_prefix_bound(&db, rusqlite::types::Value::Integer(i64::try_from(n).unwrap())),
                Some(expected),
                "stock substr(content,1,?1={n})"
            );
        }
        assert_eq!(
            stock_prefix_bound(&db, rusqlite::types::Value::Integer(-3)),
            Some(String::new()),
            "stock substr(content,1,?1=-3) is the empty string"
        );
        assert_eq!(
            stock_prefix_bound(&db, rusqlite::types::Value::Null),
            None,
            "stock substr(content,1,?1=NULL) is NULL"
        );
    });
}
