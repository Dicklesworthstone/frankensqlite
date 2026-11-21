//! End-to-end acceptance suite and planted negative controls for compatibility
//! page-level encryption (`PRAGMA key`, `PRAGMA rekey`, O(1) KEK re-wrap,
//! WAL/rollback/checkpoint/MVCC composition) — `bd-1vwiy.1` / `bd-1vwiy.2`.

#![cfg(all(not(target_arch = "wasm32"), feature = "native"))]

use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_pager::{
    Argon2Params, ENCRYPTION_RESERVED_BYTES, KeyEnvelope, key_envelope_path_for_db,
};
use fsqlite_types::cx::{CancelReason, Cx};
use fsqlite_types::value::SqliteValue;
use tempfile::tempdir;

fn run_async<F: std::future::Future<Output = ()>>(fut: F) {
    asupersync::test_utils::run_test(|| fut);
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|w| w == needle)
}

fn assert_no_plaintext_on_disk(db_path: &Path, sentinels: &[&[u8]]) {
    let mut files = vec![db_path.to_path_buf(), key_envelope_path_for_db(db_path)];
    for suffix in [
        "-wal",
        "-shm",
        "-journal",
        "-wal-fec",
        "-wal-cert",
        "-wal-cert-head",
    ] {
        let mut s = db_path.as_os_str().to_os_string();
        s.push(suffix);
        files.push(PathBuf::from(s));
    }
    for path in files {
        let Ok(bytes) = fs::read(&path) else {
            continue;
        };
        for sentinel in sentinels {
            assert!(
                !contains_subslice(&bytes, sentinel),
                "plaintext sentinel {:?} leaked into on-disk file {}",
                String::from_utf8_lossy(sentinel),
                path.display()
            );
        }
    }
}

async fn configure_fast_test_argon2(conn: &Connection) {
    conn.execute("PRAGMA fsqlite.cipher_argon2_m_cost = 1024;")
        .await
        .expect("set m_cost");
    conn.execute("PRAGMA fsqlite.cipher_argon2_t_cost = 1;")
        .await
        .expect("set t_cost");
    conn.execute("PRAGMA fsqlite.cipher_argon2_p_cost = 1;")
        .await
        .expect("set p_cost");
}

async fn assert_integrity_ok(conn: &Connection) {
    let rows = conn
        .query("PRAGMA integrity_check;")
        .await
        .expect("integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[SqliteValue::Text("ok".into())][..]);
}

#[test]
fn test_encrypted_wal_and_rollback_roundtrip_and_zero_plaintext_on_disk() {
    run_async(async {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("vault_wal.db");
        let db_str = db_path.to_str().expect("utf8 path").to_owned();

        let sentinel_a = "FSQLITE_ENCRYPT_SENTINEL_ALPHA_99887766";
        let sentinel_b = "FSQLITE_ENCRYPT_SENTINEL_BETA_55443322";
        let sentinel_c = "FSQLITE_ROLLBACK_SENTINEL_GAMMA_11223344";

        // 1. Fresh encrypted DB in WAL mode.
        {
            let conn = Connection::open(&db_str).await.expect("open fresh db");
            configure_fast_test_argon2(&conn).await;
            let res = conn
                .query("PRAGMA key = 'alpha-passphrase';")
                .await
                .expect("PRAGMA key on fresh db");
            assert_eq!(res[0].values(), &[SqliteValue::Text("ok".into())][..]);

            conn.execute("CREATE TABLE secret_vault(id INTEGER PRIMARY KEY, secret TEXT NOT NULL);")
                .await
                .expect("create table");
            conn.execute("CREATE INDEX idx_secret_vault_text ON secret_vault(secret);")
                .await
                .expect("create index");
            conn.execute(&format!(
                "INSERT INTO secret_vault(id, secret) VALUES (1, '{sentinel_a}'), (2, '{sentinel_b}');"
            ))
            .await
            .expect("insert encrypted WAL rows");

            // Check on-disk bytes WHILE live WAL frames exist (before checkpoint).
            assert_no_plaintext_on_disk(
                &db_path,
                &[
                    sentinel_a.as_bytes(),
                    sentinel_b.as_bytes(),
                    b"secret_vault",
                    b"alpha-passphrase",
                ],
            );

            let raw_db = fs::read(&db_path).expect("read raw db");
            assert!(raw_db.len() >= 4096);
            assert_ne!(
                &raw_db[..16],
                b"SQLite format 3\0",
                "encrypted DB page 1 must not expose plaintext SQLite magic"
            );

            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("checkpoint encrypted WAL");
            assert_integrity_ok(&conn).await;
            conn.close().await.expect("close");
        }

        // Verify zero plaintext after checkpoint + close.
        assert_no_plaintext_on_disk(
            &db_path,
            &[
                sentinel_a.as_bytes(),
                sentinel_b.as_bytes(),
                b"secret_vault",
                b"alpha-passphrase",
            ],
        );

        // 2. Reopen locked: fail closed without key and with wrong key; unlock with right key.
        {
            let conn = Connection::open(&db_str).await.expect("open locked db");

            let err = conn
                .query("SELECT secret FROM secret_vault;")
                .await
                .expect_err("query while locked must fail closed");
            assert!(
                matches!(err, FrankenError::NotADatabase { .. }),
                "expected NotADatabase while locked, got {err:?}"
            );

            let prep_err = conn
                .prepare("SELECT 1;")
                .await
                .expect_err("prepare while locked must fail closed");
            assert!(matches!(prep_err, FrankenError::NotADatabase { .. }));

            let export_err = conn
                .export_bytes()
                .await
                .expect_err("export_bytes while locked must fail closed");
            assert!(matches!(export_err, FrankenError::NotADatabase { .. }));

            let wrong_key_err = conn
                .query("PRAGMA key = 'wrong-passphrase';")
                .await
                .expect_err("wrong key must fail closed");
            assert!(
                matches!(wrong_key_err, FrankenError::NotADatabase { .. }),
                "expected NotADatabase for wrong key, got {wrong_key_err:?}"
            );

            // Still locked after wrong key.
            let still_locked = conn
                .query("SELECT count(*) FROM secret_vault;")
                .await
                .expect_err("must remain locked after wrong key");
            assert!(matches!(still_locked, FrankenError::NotADatabase { .. }));

            // Unlock with right key.
            conn.query("PRAGMA key = 'alpha-passphrase';")
                .await
                .expect("unlock with correct passphrase");

            let rows = conn
                .query("SELECT id, secret FROM secret_vault ORDER BY id;")
                .await
                .expect("read decrypted rows");
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0].values(),
                &[
                    SqliteValue::Integer(1),
                    SqliteValue::Text(sentinel_a.into())
                ][..]
            );
            assert_eq!(
                rows[1].values(),
                &[
                    SqliteValue::Integer(2),
                    SqliteValue::Text(sentinel_b.into())
                ][..]
            );
            assert_integrity_ok(&conn).await;

            // 3. Switch to rollback journal mode (DELETE) and write more rows.
            conn.execute("PRAGMA journal_mode = DELETE;")
                .await
                .expect("switch encrypted DB to DELETE journal mode");
            conn.execute(&format!(
                "INSERT INTO secret_vault(id, secret) VALUES (3, '{sentinel_c}');"
            ))
            .await
            .expect("insert under encrypted rollback journal");
            assert_integrity_ok(&conn).await;
            conn.close().await.expect("close rollback mode");
        }

        assert_no_plaintext_on_disk(
            &db_path,
            &[
                sentinel_a.as_bytes(),
                sentinel_b.as_bytes(),
                sentinel_c.as_bytes(),
                b"secret_vault",
            ],
        );

        // 4. Reopen encrypted rollback-mode DB and verify all 3 rows.
        {
            let conn = Connection::open(&db_str).await.expect("reopen rollback db");
            conn.query("PRAGMA key = 'alpha-passphrase';")
                .await
                .expect("unlock rollback db");
            let rows = conn
                .query("SELECT id, secret FROM secret_vault ORDER BY id;")
                .await
                .expect("query rollback rows");
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[2].values()[1], SqliteValue::Text(sentinel_c.into()));
            assert_integrity_ok(&conn).await;
            conn.close().await.expect("close");
        }
    });
}

#[test]
fn test_production_default_argon2_params_end_to_end() {
    run_async(async {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("prod_argon2_defaults.db");
        let db_str = db_path.to_str().expect("utf8 path").to_owned();

        // Do NOT override any cipher_argon2_* pragma: exercises production defaults.
        {
            let conn = Connection::open(&db_str).await.expect("open db");
            conn.query("PRAGMA key = 'production-default-kdf-secret';")
                .await
                .expect("init encrypted db with production Argon2id defaults");
            conn.execute("CREATE TABLE t(x TEXT); INSERT INTO t VALUES ('prod_kdf_ok');")
                .await
                .expect("write row");
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("checkpoint");
            conn.close().await.expect("close");
        }

        let key_bytes = fs::read(key_envelope_path_for_db(&db_path)).expect("read -key sidecar");
        let env = KeyEnvelope::from_bytes(&key_bytes).expect("parse -key envelope");
        assert_eq!(env.argon2_params(), &Argon2Params::default());
        assert_eq!(env.reserved_per_page(), ENCRYPTION_RESERVED_BYTES);

        {
            let conn = Connection::open(&db_str).await.expect("reopen db");
            let bad = conn
                .query("PRAGMA key = 'wrong-prod-secret';")
                .await
                .expect_err("wrong key must fail");
            assert!(matches!(bad, FrankenError::NotADatabase { .. }));

            conn.query("PRAGMA key = 'production-default-kdf-secret';")
                .await
                .expect("unlock with production default KDF");
            let rows = conn.query("SELECT x FROM t;").await.expect("select");
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].values(),
                &[SqliteValue::Text("prod_kdf_ok".into())][..]
            );
            assert_integrity_ok(&conn).await;
            conn.close().await.expect("close");
        }
    });
}

#[test]
fn test_nonce_freshness_across_rewrites_and_checkpoints() {
    run_async(async {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("nonce_freshness.db");
        let db_str = db_path.to_str().expect("utf8 path").to_owned();

        let conn = Connection::open(&db_str).await.expect("open");
        configure_fast_test_argon2(&conn).await;
        conn.query("PRAGMA key = 'nonce-test-key';")
            .await
            .expect("key");
        conn.execute("CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create");
        conn.execute("INSERT INTO kv VALUES (1, 'v0');")
            .await
            .expect("insert");
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("ckpt 0");

        let mut seen_nonces = std::collections::HashSet::new();
        for i in 1..=5_u32 {
            conn.execute(&format!("UPDATE kv SET v = 'v{i}' WHERE k = 1;"))
                .await
                .expect("update");
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("ckpt");
            let raw = fs::read(&db_path).expect("read db");
            assert!(raw.len() >= 8192, "expected at least 2 pages");
            // Page 2 holds table `kv`: inspect its 24-byte nonce at [page_size-40..page_size-16].
            let p2 = &raw[4096..8192];
            let mut nonce = [0_u8; 24];
            nonce.copy_from_slice(&p2[4096 - 40..4096 - 16]);
            assert_ne!(nonce, [0_u8; 24], "nonce must not be all zeros");
            assert!(
                seen_nonces.insert(nonce),
                "reused nonce detected on write iteration {i}"
            );
        }
        conn.close().await.expect("close");
    });
}

#[test]
fn test_tamper_resistance_page_key_envelope_and_cross_page_swap() {
    run_async(async {
        let dir = tempdir().expect("tempdir");
        let base_db = dir.path().join("tamper_base.db");
        let base_str = base_db.to_str().expect("utf8 path").to_owned();

        {
            let conn = Connection::open(&base_str).await.expect("open");
            configure_fast_test_argon2(&conn).await;
            conn.query("PRAGMA key = 'tamper-guard-key';")
                .await
                .expect("key");
            conn.execute("PRAGMA journal_mode = DELETE;")
                .await
                .expect("delete mode");
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, payload TEXT);")
                .await
                .expect("create t1");
            conn.execute("CREATE TABLE t2(id INTEGER PRIMARY KEY, payload TEXT);")
                .await
                .expect("create t2");
            conn.execute("INSERT INTO t1 VALUES (1, 'table_one_payload');")
                .await
                .expect("insert t1");
            conn.execute("INSERT INTO t2 VALUES (1, 'table_two_payload');")
                .await
                .expect("insert t2");
            conn.close().await.expect("close");
        }

        let base_bytes = fs::read(&base_db).expect("read base db");
        let base_key = fs::read(key_envelope_path_for_db(&base_db)).expect("read base key");
        assert!(
            base_bytes.len() >= 4096 * 3,
            "expected at least 3 pages (page 1 master + page 2 t1 + page 3 t2), got {} bytes",
            base_bytes.len()
        );

        // Negative control A: flip 1 bit in page 2 ciphertext, tag, and nonce.
        for (label, offset) in [
            ("ciphertext", 4096 + 64),
            ("nonce", 4096 + (4096 - 40) + 5),
            ("tag", 4096 + (4096 - 16) + 7),
        ] {
            let tampered_db = dir.path().join(format!("tamper_{label}.db"));
            let mut corrupted = base_bytes.clone();
            corrupted[offset] ^= 0x01;
            fs::write(&tampered_db, &corrupted).expect("write tampered db");
            fs::write(key_envelope_path_for_db(&tampered_db), &base_key).expect("write key");

            // Eager open hydrates table pages on unlock and immediately rejects tampered page 2.
            let eager = Connection::open(tampered_db.to_str().unwrap())
                .await
                .expect("open locked tampered db");
            let eager_err = eager
                .query("PRAGMA key = 'tamper-guard-key';")
                .await
                .expect_err("eager unlock must reject tampered page 2");
            assert!(
                matches!(&eager_err, FrankenError::DatabaseCorrupt { detail } if detail.contains("encrypted page 2")),
                "tampered {label} eager unlock expected DatabaseCorrupt on page 2, got {eager_err:?}"
            );
            let locked_err = eager
                .query("SELECT payload FROM t1;")
                .await
                .expect_err("failed unlock must keep connection locked");
            assert!(matches!(locked_err, FrankenError::NotADatabase { .. }));
            eager.close().await.expect("close eager");

            // Schema-only open unlocks page 1 schema, then rejects tampered page 2 on read.
            let lazy = Connection::open_existing_schema_only(tampered_db.to_str().unwrap())
                .await
                .expect("open schema-only locked tampered db");
            lazy.query("PRAGMA key = 'tamper-guard-key';")
                .await
                .expect("page 1 intact so schema-only unlock succeeds");
            let err = lazy
                .query("SELECT payload FROM t1;")
                .await
                .expect_err("tampered page 2 must fail AEAD authentication on read");
            assert!(
                matches!(&err, FrankenError::DatabaseCorrupt { detail } if detail.contains("encrypted page 2")),
                "tampered {label} expected DatabaseCorrupt on page 2, got {err:?}"
            );
            lazy.close().await.expect("close lazy");
        }

        // Negative control B: cross-page AAD swap attack (copy encrypted page 2 onto page 3).
        {
            let swapped_db = dir.path().join("tamper_swap.db");
            let mut swapped = base_bytes.clone();
            let p2_copy = base_bytes[4096..8192].to_vec();
            swapped[8192..12288].copy_from_slice(&p2_copy);
            fs::write(&swapped_db, &swapped).expect("write swapped db");
            fs::write(key_envelope_path_for_db(&swapped_db), &base_key).expect("write key");

            // Eager open catches cross-page AAD mismatch on page 3 immediately during unlock.
            let eager = Connection::open(swapped_db.to_str().unwrap())
                .await
                .expect("open locked swapped db");
            let eager_err = eager
                .query("PRAGMA key = 'tamper-guard-key';")
                .await
                .expect_err("eager unlock must reject cross-page swap on page 3");
            assert!(
                matches!(&eager_err, FrankenError::DatabaseCorrupt { detail } if detail.contains("encrypted page 3")),
                "cross-page swap eager unlock expected DatabaseCorrupt on page 3, got {eager_err:?}"
            );
            eager.close().await.expect("close eager");

            // Schema-only open unlocks page 1, reads intact page 2 (t1), and rejects swapped page 3 (t2).
            let lazy = Connection::open_existing_schema_only(swapped_db.to_str().unwrap())
                .await
                .expect("open schema-only locked swapped db");
            lazy.query("PRAGMA key = 'tamper-guard-key';")
                .await
                .expect("schema-only unlock swapped db");
            let t1_rows = lazy
                .query("SELECT payload FROM t1;")
                .await
                .expect("intact page 2 remains readable");
            assert_eq!(
                t1_rows[0].values(),
                &[SqliteValue::Text("table_one_payload".into())][..]
            );
            let err = lazy
                .query("SELECT payload FROM t2;")
                .await
                .expect_err("cross-page AAD swap onto page 3 must fail closed");
            assert!(
                matches!(&err, FrankenError::DatabaseCorrupt { detail } if detail.contains("encrypted page 3")),
                "cross-page swap expected DatabaseCorrupt on page 3, got {err:?}"
            );
            lazy.close().await.expect("close lazy");
        }

        // Negative control C: tamper `<db>-key` sidecar digest/header.
        {
            let bad_env_db = dir.path().join("tamper_env.db");
            fs::write(&bad_env_db, &base_bytes).expect("write db");
            let mut bad_key = base_key.clone();
            bad_key[24] ^= 0x80; // flip Argon2 parameter byte inside envelope header
            fs::write(key_envelope_path_for_db(&bad_env_db), &bad_key).expect("write bad key");

            let conn = Connection::open(bad_env_db.to_str().unwrap())
                .await
                .expect("open locked bad-env db");
            let err = conn
                .query("PRAGMA key = 'tamper-guard-key';")
                .await
                .expect_err("tampered -key envelope must fail closed");
            assert!(
                matches!(err, FrankenError::DatabaseCorrupt { .. }),
                "tampered -key expected DatabaseCorrupt, got {err:?}"
            );
            conn.close().await.expect("close");
        }

        // Negative control D: deleted `<db>-key` sidecar must fail closed on open.
        {
            let no_key_db = dir.path().join("missing_key.db");
            fs::write(&no_key_db, &base_bytes).expect("write db without -key");
            let open_res = Connection::open(no_key_db.to_str().unwrap()).await;
            assert!(
                open_res.is_err(),
                "opening ciphertext DB with deleted -key sidecar must fail closed"
            );
        }
    });
}

#[test]
fn test_o1_rekey_preserves_db_bytes_and_database_id_and_invalidates_old_key() {
    run_async(async {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("o1_rekey.db");
        let db_str = db_path.to_str().expect("utf8 path").to_owned();
        let key_path = key_envelope_path_for_db(&db_path);

        // Populate a multi-page encrypted DB (> 20 pages).
        {
            let conn = Connection::open(&db_str).await.expect("open");
            configure_fast_test_argon2(&conn).await;
            conn.query("PRAGMA key = 'initial-passphrase-1';")
                .await
                .expect("key");
            conn.execute("CREATE TABLE blobs(id INTEGER PRIMARY KEY, body TEXT NOT NULL);")
                .await
                .expect("create");
            conn.execute("BEGIN;").await.expect("begin");
            let payload = "Z".repeat(512);
            for i in 1..=200_i64 {
                conn.execute(&format!(
                    "INSERT INTO blobs(id, body) VALUES ({i}, 'row_{i}_{payload}');"
                ))
                .await
                .expect("insert");
            }
            conn.execute("COMMIT;").await.expect("commit");
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("checkpoint");

            let db_bytes_before = fs::read(&db_path).expect("read db before rekey");
            assert!(
                db_bytes_before.len() >= 20 * 4096,
                "expected multi-page DB (>= 80 KiB), got {} bytes",
                db_bytes_before.len()
            );
            let env_before =
                KeyEnvelope::from_bytes(&fs::read(&key_path).expect("read key before"))
                    .expect("parse key before");

            // Execute O(1) KEK re-wrap via PRAGMA rekey.
            let res = conn
                .query("PRAGMA rekey = 'rotated-passphrase-2';")
                .await
                .expect("PRAGMA rekey");
            assert_eq!(res[0].values(), &[SqliteValue::Text("ok".into())][..]);

            let db_bytes_after = fs::read(&db_path).expect("read db after rekey");
            let env_after = KeyEnvelope::from_bytes(&fs::read(&key_path).expect("read key after"))
                .expect("parse key after");

            // Core O(1) proof: ZERO data pages touched on disk!
            assert_eq!(
                db_bytes_before, db_bytes_after,
                "O(1) PRAGMA rekey must NOT rewrite any main-DB page bytes"
            );
            assert_eq!(
                env_before.database_id(),
                env_after.database_id(),
                "O(1) rekey must preserve DatabaseId so existing page AAD stays valid"
            );
            assert_ne!(
                env_before.salt(),
                env_after.salt(),
                "rekey must generate a fresh Argon2id salt"
            );
            assert_ne!(
                env_before.wrapped_dek(),
                env_after.wrapped_dek(),
                "rekey must produce a freshly wrapped DEK ciphertext"
            );
            conn.close().await.expect("close");
        }

        // Plant a simulated crash-leftover `<db>-key.tmp` file alongside `<db>-key`.
        let mut tmp_os = key_path.as_os_str().to_os_string();
        tmp_os.push(".tmp");
        fs::write(PathBuf::from(tmp_os), b"partial crash garbage").expect("write leftover tmp");

        // Reopen: old key MUST fail closed; new key MUST unlock and read all 200 rows.
        {
            let conn = Connection::open(&db_str).await.expect("reopen after rekey");
            let old_err = conn
                .query("PRAGMA key = 'initial-passphrase-1';")
                .await
                .expect_err("superseded key must fail closed");
            assert!(matches!(old_err, FrankenError::NotADatabase { .. }));

            conn.query("PRAGMA key = 'rotated-passphrase-2';")
                .await
                .expect("unlock with rotated key");
            let count = conn
                .query("SELECT count(*) FROM blobs;")
                .await
                .expect("count");
            assert_eq!(count[0].values(), &[SqliteValue::Integer(200)][..]);
            assert_integrity_ok(&conn).await;
            conn.close().await.expect("close");
        }
    });
}

#[test]
fn test_explicit_enable_and_disable_conversion_and_rusqlite_interoperability() {
    run_async(async {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("convert_roundtrip.db");
        let db_str = db_path.to_str().expect("utf8 path").to_owned();
        let key_path = key_envelope_path_for_db(&db_path);
        let sentinel = "FSQLITE_CONVERSION_SENTINEL_OMEGA_771122";

        // 1. Start with a populated PLAINTEXT database.
        {
            let conn = Connection::open(&db_str).await.expect("open plaintext db");
            configure_fast_test_argon2(&conn).await;
            conn.execute("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT NOT NULL);")
                .await
                .expect("create");
            conn.execute("CREATE INDEX idx_notes_body ON notes(body);")
                .await
                .expect("index");
            conn.execute(&format!(
                "INSERT INTO notes VALUES (1, '{sentinel}'), (2, 'second_note');"
            ))
            .await
            .expect("insert");
            assert!(!key_path.exists(), "plaintext DB must not have -key");

            // Plaintext -> Encrypted conversion via PRAGMA rekey = 'secret'.
            conn.query("PRAGMA rekey = 'convert-secret-key';")
                .await
                .expect("enable encryption via PRAGMA rekey");
            assert!(key_path.exists(), "converted DB must have -key sidecar");

            let rows = conn
                .query("SELECT id, body FROM notes ORDER BY id;")
                .await
                .expect("query after enable conversion");
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values()[1], SqliteValue::Text(sentinel.into()));
            assert_integrity_ok(&conn).await;
            conn.close().await.expect("close encrypted");
        }

        // Verify zero plaintext remains anywhere on disk after enable conversion.
        assert_no_plaintext_on_disk(&db_path, &[sentinel.as_bytes(), b"second_note"]);
        let raw_encrypted = fs::read(&db_path).expect("read encrypted db");
        assert_ne!(&raw_encrypted[..16], b"SQLite format 3\0");

        // 2. Reopen encrypted DB, unlock, then disable encryption via PRAGMA rekey = ''.
        {
            let conn = Connection::open(&db_str).await.expect("reopen locked");
            let err = conn
                .query("SELECT count(*) FROM notes;")
                .await
                .expect_err("must be locked after enable conversion");
            assert!(matches!(err, FrankenError::NotADatabase { .. }));

            conn.query("PRAGMA key = 'convert-secret-key';")
                .await
                .expect("unlock converted db");
            conn.query("PRAGMA rekey = '';")
                .await
                .expect("disable encryption via PRAGMA rekey = ''");
            assert!(
                !key_path.exists(),
                "disabling encryption must remove -key sidecar"
            );
            assert_integrity_ok(&conn).await;
            conn.close().await.expect("close plaintext");
        }

        // 3. Verify plaintext header (`reserved_per_page == 0`), FrankenSQLite keyless open,
        //    AND stock C SQLite (`rusqlite`) full interoperability!
        let raw_plain = fs::read(&db_path).expect("read decrypted db");
        assert_eq!(&raw_plain[..16], b"SQLite format 3\0");
        assert_eq!(raw_plain[20], 0, "disabled DB must have reserved_per_page = 0");

        {
            let conn = Connection::open(&db_str)
                .await
                .expect("open plaintext db without PRAGMA key");
            let rows = conn
                .query("SELECT id, body FROM notes ORDER BY id;")
                .await
                .expect("query without key");
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values()[1], SqliteValue::Text(sentinel.into()));
            assert_integrity_ok(&conn).await;
            conn.close().await.expect("close");
        }

        {
            let stock = rusqlite::Connection::open(&db_path).expect("rusqlite open decrypted db");
            let integrity: String = stock
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .expect("rusqlite integrity_check");
            assert_eq!(integrity, "ok");
            let mut stmt = stock
                .prepare("SELECT id, body FROM notes ORDER BY id;")
                .expect("rusqlite prepare");
            let rows: Vec<(i64, String)> = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .expect("query_map")
                .map(|r| r.expect("row"))
                .collect();
            assert_eq!(
                rows,
                vec![
                    (1, sentinel.to_owned()),
                    (2, "second_note".to_owned()),
                ]
            );
        }
    });
}

#[test]
fn test_encrypted_wal_concurrent_mvcc_writers_and_cancellation() {
    run_async(async {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("mvcc_encrypted.db");
        let db_str = db_path.to_str().expect("utf8 path").to_owned();

        // Setup two disjoint tables in an encrypted WAL DB.
        {
            let conn = Connection::open(&db_str).await.expect("open setup");
            assert!(
                conn.is_concurrent_mode_default(),
                "concurrent_mode_default MUST remain true on encrypted DBs"
            );
            configure_fast_test_argon2(&conn).await;
            conn.query("PRAGMA key = 'mvcc-enc-key';")
                .await
                .expect("key");
            conn.execute("CREATE TABLE left_t(id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .expect("create left_t");
            conn.execute("CREATE TABLE right_t(id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .expect("create right_t");
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("checkpoint schema");
            conn.close().await.expect("close setup");
        }

        // Open two concurrent connections, unlock both, run overlapping BEGIN transactions
        // on disjoint tables, and commit both concurrently without single-writer blocking.
        {
            let c1 = Connection::open(&db_str).await.expect("open c1");
            let c2 = Connection::open(&db_str).await.expect("open c2");
            assert!(c1.is_concurrent_mode_default());
            assert!(c2.is_concurrent_mode_default());
            c1.query("PRAGMA key = 'mvcc-enc-key';")
                .await
                .expect("unlock c1");
            c2.query("PRAGMA key = 'mvcc-enc-key';")
                .await
                .expect("unlock c2");

            c1.execute("BEGIN;").await.expect("c1 begin");
            c2.execute("BEGIN;").await.expect("c2 begin");
            c1.execute("INSERT INTO left_t VALUES (1, 'from_c1_a'), (2, 'from_c1_b');")
                .await
                .expect("c1 insert");
            c2.execute("INSERT INTO right_t VALUES (1, 'from_c2_a'), (2, 'from_c2_b');")
                .await
                .expect("c2 insert");
            c1.execute("COMMIT;").await.expect("c1 commit");
            c2.execute("COMMIT;").await.expect("c2 commit");

            c1.close().await.expect("close c1");
            c2.close().await.expect("close c2");
        }

        assert_no_plaintext_on_disk(&db_path, &[b"from_c1_a", b"from_c2_b"]);

        // Reopen fresh connection and verify both concurrent writers' commits survived.
        {
            let verify = Connection::open(&db_str).await.expect("open verify");
            verify
                .query("PRAGMA key = 'mvcc-enc-key';")
                .await
                .expect("unlock verify");
            let left = verify
                .query("SELECT count(*) FROM left_t;")
                .await
                .expect("count left");
            let right = verify
                .query("SELECT count(*) FROM right_t;")
                .await
                .expect("count right");
            assert_eq!(left[0].values(), &[SqliteValue::Integer(2)][..]);
            assert_eq!(right[0].values(), &[SqliteValue::Integer(2)][..]);
            assert_integrity_ok(&verify).await;
            verify.close().await.expect("close verify");
        }

        // Verify Cx cancellation returns FrankenError::Abort cleanly on unlock.
        let key_bytes = fs::read(key_envelope_path_for_db(&db_path)).expect("read -key");
        let envelope = KeyEnvelope::from_bytes(&key_bytes).expect("parse -key");
        let cancelled = Cx::new();
        cancelled.cancel_with_reason(CancelReason::UserInterrupt);
        let err = envelope
            .unlock(&cancelled, b"mvcc-enc-key")
            .expect_err("cancelled Cx must abort unlock");
        assert!(
            matches!(err, FrankenError::Abort),
            "expected FrankenError::Abort on cancelled Cx, got {err:?}"
        );
    });
}
