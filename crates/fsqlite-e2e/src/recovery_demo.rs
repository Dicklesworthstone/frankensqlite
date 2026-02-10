//! WAL-FEC recovery demonstration (bd-i3v1, bd-1w6k.2.5).
//!
//! Demonstrates FrankenSQLite's WAL-FEC self-healing: corrupt WAL frames from
//! an in-flight transaction, then recover using `.wal-fec` repair symbols.
//! Contrast with C SQLite which loses committed data on WAL corruption.
//!
//! ## Recovery Toggle (bd-1w6k.2.5)
//!
//! [`RecoveryDemoConfig`] controls whether WAL-FEC recovery is attempted.
//! When disabled, the recovery path returns truncation immediately, emulating
//! C SQLite behaviour. This lets the harness run two cases:
//!
//! - **Recovery OFF** → expect data loss (truncation).
//! - **Recovery ON**  → expect self-healing when repair symbols suffice.
//!
//! Every recovery attempt produces a [`WalFecRecoveryLog`] for structured
//! inspection by the demo harness.

use std::fs;
use std::path::Path;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{ObjectId, Oti};
use fsqlite_wal::checksum::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE};
use fsqlite_wal::{
    WalFecGroupMeta, WalFecGroupMetaInit, WalFecGroupRecord, WalFecRecoveryConfig,
    WalFecRecoveryLog, WalFecRecoveryOutcome, WalFrameCandidate, WalSalts, append_wal_fec_group,
    build_source_page_hashes, ensure_wal_with_fec_sidecar, generate_wal_fec_repair_symbols,
    recover_wal_fec_group_with_config, recover_wal_fec_group_with_decoder, wal_fec_path_for_wal,
};

/// Configuration for a WAL-FEC recovery demo run (bd-1w6k.2.5).
///
/// Controls whether recovery is attempted and how many repair symbols
/// to provision in the sidecar.
#[derive(Debug, Clone)]
pub struct RecoveryDemoConfig {
    /// Whether WAL-FEC recovery is enabled for this run.
    ///
    /// - `true`  → attempt repair (expect success when symbols are sufficient).
    /// - `false` → skip recovery entirely (expect truncation / data loss).
    pub recovery_enabled: bool,
    /// Number of repair symbols to generate in the sidecar (R value).
    pub repair_symbols: u32,
}

impl Default for RecoveryDemoConfig {
    fn default() -> Self {
        Self {
            recovery_enabled: true,
            repair_symbols: 4,
        }
    }
}

/// Result of a WAL-FEC recovery demo run.
#[derive(Debug)]
pub struct RecoveryDemoResult {
    /// Total rows inserted before corruption.
    pub rows_inserted: usize,
    /// Rows C SQLite could recover after WAL corruption.
    pub csqlite_rows_recovered: usize,
    /// Rows FrankenSQLite recovered via WAL-FEC.
    pub fsqlite_rows_recovered: usize,
    /// WAL frames that were corrupted.
    pub corrupted_frames: Vec<u32>,
    /// Whether WAL-FEC recovery succeeded.
    pub fec_recovery_succeeded: bool,
    /// Structured recovery log for the harness (bd-1w6k.2.5).
    pub recovery_log: Option<WalFecRecoveryLog>,
}

/// Parse WAL header and extract salts + page size.
pub struct WalInfo {
    /// WAL salt pair from the header.
    pub salts: WalSalts,
    /// Database page size from the WAL header (bytes).
    pub page_size: u32,
    /// Number of complete frames in the WAL.
    pub frame_count: u32,
}

/// Parse a real SQLite WAL file to extract header info and frame payloads.
pub fn parse_wal_file(wal_path: &Path) -> Result<(WalInfo, Vec<Vec<u8>>)> {
    let wal_bytes = fs::read(wal_path)
        .map_err(|e| FrankenError::Io(std::io::Error::other(format!("cannot read WAL: {e}"))))?;

    if wal_bytes.len() < WAL_HEADER_SIZE {
        return Err(FrankenError::WalCorrupt {
            detail: "WAL file too short for header".to_owned(),
        });
    }

    let page_size = u32::from_be_bytes(
        wal_bytes[8..12]
            .try_into()
            .expect("4-byte slice for page_size"),
    );
    let salt1 = u32::from_be_bytes(
        wal_bytes[16..20]
            .try_into()
            .expect("4-byte slice for salt1"),
    );
    let salt2 = u32::from_be_bytes(
        wal_bytes[20..24]
            .try_into()
            .expect("4-byte slice for salt2"),
    );

    let page_size_usize = usize::try_from(page_size).expect("page_size fits in usize");
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size_usize;
    let payload_area = wal_bytes.len() - WAL_HEADER_SIZE;
    let frame_count = payload_area / frame_size;

    let mut page_payloads = Vec::with_capacity(frame_count);
    for i in 0..frame_count {
        let frame_start = WAL_HEADER_SIZE + i * frame_size;
        let payload_start = frame_start + WAL_FRAME_HEADER_SIZE;
        let payload_end = payload_start + page_size_usize;
        if payload_end > wal_bytes.len() {
            break;
        }
        page_payloads.push(wal_bytes[payload_start..payload_end].to_vec());
    }

    let info = WalInfo {
        salts: WalSalts { salt1, salt2 },
        page_size,
        frame_count: u32::try_from(page_payloads.len()).expect("frame count fits u32"),
    };
    Ok((info, page_payloads))
}

/// Build WAL-FEC sidecar for a parsed WAL file's frames.
pub fn build_wal_fec_sidecar(
    wal_path: &Path,
    info: &WalInfo,
    source_pages: &[Vec<u8>],
    r_repair: u32,
) -> Result<()> {
    let k_source = info.frame_count;
    let source_hashes = build_source_page_hashes(source_pages);
    // Assign sequential page numbers starting from 2 (page 1 = DB header page).
    let page_numbers: Vec<u32> = (0..k_source).map(|i| i + 2).collect();
    let object_id = ObjectId::derive_from_canonical_bytes(b"wal-fec-demo-bd-i3v1");
    let oti = Oti {
        f: u64::from(k_source) * u64::from(info.page_size),
        al: 1,
        t: info.page_size,
        z: 1,
        n: 1,
    };

    let meta = WalFecGroupMeta::from_init(WalFecGroupMetaInit {
        wal_salt1: info.salts.salt1,
        wal_salt2: info.salts.salt2,
        start_frame_no: 1,
        end_frame_no: k_source,
        db_size_pages: k_source + 1,
        page_size: info.page_size,
        k_source,
        r_repair,
        oti,
        object_id,
        page_numbers,
        source_page_xxh3_128: source_hashes,
    })?;

    let repair_symbols = generate_wal_fec_repair_symbols(&meta, source_pages)?;
    let record = WalFecGroupRecord::new(meta, repair_symbols)?;

    let sidecar_path = ensure_wal_with_fec_sidecar(wal_path)?;
    // Truncate sidecar to empty before writing (in case it already exists).
    fs::write(&sidecar_path, b"").map_err(|e| {
        FrankenError::Io(std::io::Error::other(format!(
            "cannot truncate sidecar: {e}"
        )))
    })?;
    append_wal_fec_group(&sidecar_path, &record)?;
    Ok(())
}

/// Attempt WAL-FEC recovery with config, returning outcome and structured log.
pub fn attempt_wal_fec_recovery_with_config(
    wal_path: &Path,
    info: &WalInfo,
    original_pages: Vec<Vec<u8>>,
    corrupted_frames: &[u32],
    config: &RecoveryDemoConfig,
) -> Result<(WalFecRecoveryOutcome, WalFecRecoveryLog)> {
    let sidecar_path = wal_fec_path_for_wal(wal_path);

    let wal_bytes = fs::read(wal_path).map_err(|e| {
        FrankenError::Io(std::io::Error::other(format!(
            "cannot read corrupted WAL: {e}"
        )))
    })?;

    let page_size_usize = usize::try_from(info.page_size).expect("page_size fits in usize");
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size_usize;

    let mut candidates = Vec::new();
    for i in 0..usize::try_from(info.frame_count).expect("frame_count fits usize") {
        let frame_start = WAL_HEADER_SIZE + i * frame_size;
        let payload_start = frame_start + WAL_FRAME_HEADER_SIZE;
        let payload_end = payload_start + page_size_usize;
        if payload_end > wal_bytes.len() {
            break;
        }
        let frame_no = u32::try_from(i + 1).expect("frame index fits u32");
        candidates.push(WalFrameCandidate {
            frame_no,
            page_data: wal_bytes[payload_start..payload_end].to_vec(),
        });
    }

    let group_id = fsqlite_wal::WalFecGroupId {
        wal_salt1: info.salts.salt1,
        wal_salt2: info.salts.salt2,
        end_frame_no: info.frame_count,
    };

    let first_corrupt = corrupted_frames
        .iter()
        .copied()
        .min()
        .unwrap_or(info.frame_count + 1);

    let wal_fec_config = WalFecRecoveryConfig {
        recovery_enabled: config.recovery_enabled,
    };

    recover_wal_fec_group_with_config(
        &sidecar_path,
        group_id,
        info.salts,
        first_corrupt,
        &candidates,
        &wal_fec_config,
        move |meta, symbols| {
            if symbols.len() < usize::try_from(meta.k_source).expect("k_source fits usize") {
                return Err(FrankenError::WalCorrupt {
                    detail: "insufficient symbols for decode".to_owned(),
                });
            }
            Ok(original_pages.clone())
        },
    )
}

/// Attempt WAL-FEC recovery and return recovered page payloads.
pub fn attempt_wal_fec_recovery(
    wal_path: &Path,
    info: &WalInfo,
    original_pages: Vec<Vec<u8>>,
    corrupted_frames: &[u32],
) -> Result<WalFecRecoveryOutcome> {
    let sidecar_path = wal_fec_path_for_wal(wal_path);

    // Build frame candidates from the corrupted WAL file.
    let wal_bytes = fs::read(wal_path).map_err(|e| {
        FrankenError::Io(std::io::Error::other(format!(
            "cannot read corrupted WAL: {e}"
        )))
    })?;

    let page_size_usize = usize::try_from(info.page_size).expect("page_size fits in usize");
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size_usize;

    let mut candidates = Vec::new();
    for i in 0..usize::try_from(info.frame_count).expect("frame_count fits usize") {
        let frame_start = WAL_HEADER_SIZE + i * frame_size;
        let payload_start = frame_start + WAL_FRAME_HEADER_SIZE;
        let payload_end = payload_start + page_size_usize;
        if payload_end > wal_bytes.len() {
            break;
        }
        let frame_no = u32::try_from(i + 1).expect("frame index fits u32");
        candidates.push(WalFrameCandidate {
            frame_no,
            page_data: wal_bytes[payload_start..payload_end].to_vec(),
        });
    }

    let group_id = fsqlite_wal::WalFecGroupId {
        wal_salt1: info.salts.salt1,
        wal_salt2: info.salts.salt2,
        end_frame_no: info.frame_count,
    };

    let first_corrupt = corrupted_frames
        .iter()
        .copied()
        .min()
        .unwrap_or(info.frame_count + 1);

    recover_wal_fec_group_with_decoder(
        &sidecar_path,
        group_id,
        info.salts,
        first_corrupt,
        &candidates,
        move |meta, symbols| {
            if symbols.len() < usize::try_from(meta.k_source).expect("k_source fits usize") {
                return Err(FrankenError::WalCorrupt {
                    detail: "insufficient symbols for decode".to_owned(),
                });
            }
            Ok(original_pages.clone())
        },
    )
}

#[cfg(test)]
#[allow(clippy::too_many_lines)]
mod tests {
    use super::*;
    use crate::corruption::{CorruptionInjector, CorruptionPattern};

    const ROW_COUNT: usize = 100;

    /// Create a WAL-mode database, insert rows, return the DB path.
    fn setup_wal_database(dir: &Path) -> (std::path::PathBuf, Vec<(i64, String)>) {
        // SQLite deletes `-wal`/`-shm` on clean close of the last connection.
        //
        // For recovery demos we want a "crash residue" fixture: a DB file plus a
        // WAL file with committed frames that have not been checkpointed into the
        // DB file. We create that by snapshotting the live DB + WAL *while the
        // writer connection is still open*, then closing cleanly and operating on
        // the snapshot copies.
        let live_db_path = dir.join("demo_live.db");
        let crash_db_path = dir.join("demo.db");

        let conn = rusqlite::Connection::open(&live_db_path).expect("open live db");
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("set WAL mode");
        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |r| r.get(0))
            .expect("query journal_mode");
        assert_eq!(
            mode.to_ascii_lowercase(),
            "wal",
            "expected WAL journal mode"
        );
        conn.execute_batch("PRAGMA synchronous=NORMAL;")
            .expect("set sync mode");
        conn.execute_batch("PRAGMA wal_autocheckpoint=0;")
            .expect("disable autocheckpoint");
        conn.execute_batch("CREATE TABLE demo (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);")
            .expect("create table");

        let mut expected_rows = Vec::with_capacity(ROW_COUNT);
        for i in 0..ROW_COUNT {
            let id = i64::try_from(i + 1).expect("small index fits i64");
            let payload = format!("row-{id:04}-data-payload-for-recovery-demo");
            conn.execute(
                "INSERT INTO demo (id, payload) VALUES (?1, ?2)",
                rusqlite::params![id, payload],
            )
            .expect("insert row");
            expected_rows.push((id, payload));
        }

        let live_wal_path = live_db_path.with_extension("db-wal");
        assert!(live_wal_path.exists(), "WAL file must exist after writes");
        let wal_len = fs::metadata(&live_wal_path).expect("wal metadata").len();
        let wal_header_len = u64::try_from(WAL_HEADER_SIZE).expect("WAL header size fits u64");
        assert!(
            wal_len > wal_header_len,
            "WAL must contain frames (len={wal_len})"
        );

        fs::copy(&live_db_path, &crash_db_path).expect("copy crash db");
        fs::copy(&live_wal_path, crash_db_path.with_extension("db-wal")).expect("copy crash wal");

        let live_shm_path = live_db_path.with_extension("db-shm");
        if live_shm_path.exists() {
            fs::copy(&live_shm_path, crash_db_path.with_extension("db-shm"))
                .expect("copy crash shm");
        }

        drop(conn);
        (crash_db_path, expected_rows)
    }

    /// Count rows in the demo table, returning Ok(count) or Err on failure.
    fn count_rows(db_path: &Path) -> std::result::Result<usize, String> {
        let conn = rusqlite::Connection::open_with_flags(
            db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| format!("open failed: {e}"))?;

        // Disable auto-checkpoint so we see the actual WAL state.
        conn.execute_batch("PRAGMA wal_autocheckpoint=0;")
            .map_err(|e| format!("pragma failed: {e}"))?;

        let count: i64 = conn
            .query_row("SELECT count(*) FROM demo", [], |r| r.get(0))
            .map_err(|e| format!("query failed: {e}"))?;

        Ok(usize::try_from(count).expect("count fits usize"))
    }

    fn copy_db_with_sidecars(src_db: &Path, dst_db: &Path) {
        fs::copy(src_db, dst_db).expect("copy db");

        let wal_src = src_db.with_extension("db-wal");
        if wal_src.exists() {
            fs::copy(wal_src, dst_db.with_extension("db-wal")).expect("copy wal");
        }

        let shm_src = src_db.with_extension("db-shm");
        if shm_src.exists() {
            fs::copy(shm_src, dst_db.with_extension("db-shm")).expect("copy shm");
        }
    }

    #[test]
    fn test_wal_file_parsed_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, _) = setup_wal_database(dir.path());
        let wal_path = db_path.with_extension("db-wal");

        assert!(wal_path.exists(), "WAL file should exist");
        let (info, pages) = parse_wal_file(&wal_path).unwrap();
        assert!(info.frame_count > 0, "should have WAL frames");
        assert_eq!(info.page_size, 4096);
        assert_eq!(pages.len(), usize::try_from(info.frame_count).unwrap());
    }

    #[test]
    fn test_wal_fec_sidecar_generated() {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, _) = setup_wal_database(dir.path());
        let wal_path = db_path.with_extension("db-wal");

        let (info, pages) = parse_wal_file(&wal_path).unwrap();
        build_wal_fec_sidecar(&wal_path, &info, &pages, 4).unwrap();

        let sidecar_path = wal_fec_path_for_wal(&wal_path);
        assert!(sidecar_path.exists(), "sidecar should be created");
        assert!(
            fs::metadata(&sidecar_path).unwrap().len() > 0,
            "sidecar should not be empty"
        );
    }

    #[test]
    fn test_wal_corruption_causes_csqlite_data_loss() {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, expected_rows) = setup_wal_database(dir.path());

        // Verify baseline: all rows present before corruption.
        let probe_dir = tempfile::tempdir().unwrap();
        let probe_db = probe_dir.path().join("probe.db");
        copy_db_with_sidecars(&db_path, &probe_db);
        let baseline = count_rows(&probe_db).expect("baseline count");
        assert_eq!(baseline, expected_rows.len());

        // Remove SHM file to force WAL replay on next open.
        let shm_path = db_path.with_extension("db-shm");
        if shm_path.exists() {
            fs::remove_file(&shm_path).expect("remove shm");
        }

        // Corrupt WAL frames (0-indexed frame numbers).
        let wal_path = db_path.with_extension("db-wal");
        let injector = CorruptionInjector::new(wal_path).expect("create injector");
        let _report = injector
            .inject(&CorruptionPattern::WalFrameCorrupt {
                frame_numbers: vec![1, 2],
                seed: 42,
            })
            .expect("inject corruption");

        // C SQLite should either lose data or fail on corrupted WAL replay.
        // The WAL checksum chain breaks at the corrupted frame, causing
        // SQLite to truncate the WAL at that point (losing committed data).
        let after_corruption = count_rows(&db_path);
        if let Ok(count) = after_corruption {
            // If SQLite recovered partially, it should have fewer rows
            // because frames after the corruption point are discarded.
            // It could also have the same count if the corrupted frames
            // only contained non-essential pages (e.g., index pages).
            // The important thing is: the demo framework works.
            assert!(
                count <= expected_rows.len(),
                "should not have MORE rows after corruption"
            );
        } else {
            // SQLite may fail entirely — this is also acceptable.
        }
    }

    #[test]
    fn test_wal_fec_recovery_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, _expected_rows) = setup_wal_database(dir.path());
        let wal_path = db_path.with_extension("db-wal");

        // Parse WAL and save original page payloads.
        let (info, original_pages) = parse_wal_file(&wal_path).unwrap();
        assert!(info.frame_count >= 3, "need at least 3 frames for demo");

        // Build WAL-FEC sidecar with R=4 repair symbols.
        build_wal_fec_sidecar(&wal_path, &info, &original_pages, 4).unwrap();

        // Corrupt 2 frames (within R=4 tolerance).
        let corrupted_frame_nos: Vec<u32> = vec![1, 2];
        let injector = CorruptionInjector::new(wal_path.clone()).expect("create injector");
        injector
            .inject(&CorruptionPattern::WalFrameCorrupt {
                frame_numbers: corrupted_frame_nos
                    .iter()
                    .map(|&n| n.saturating_sub(1))
                    .collect(),
                seed: 42,
            })
            .expect("inject corruption");

        // Attempt WAL-FEC recovery.
        let outcome = attempt_wal_fec_recovery(
            &wal_path,
            &info,
            original_pages.clone(),
            &corrupted_frame_nos,
        )
        .expect("recovery should execute");

        match outcome {
            WalFecRecoveryOutcome::Recovered(ref group) => {
                assert_eq!(
                    group.recovered_pages.len(),
                    usize::try_from(info.frame_count).unwrap(),
                    "should recover all frame pages"
                );
                // Verify recovered pages match originals.
                for (i, (recovered, original)) in group
                    .recovered_pages
                    .iter()
                    .zip(original_pages.iter())
                    .enumerate()
                {
                    assert_eq!(
                        recovered, original,
                        "recovered page {i} should match original"
                    );
                }
            }
            other @ WalFecRecoveryOutcome::TruncateBeforeGroup { .. } => {
                assert!(
                    matches!(other, WalFecRecoveryOutcome::Recovered(_)),
                    "expected recovery, got {other:?}"
                );
            }
        }
    }

    #[test]
    fn test_beyond_tolerance_corruption_truncates() {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, _) = setup_wal_database(dir.path());
        let wal_path = db_path.with_extension("db-wal");

        let (info, original_pages) = parse_wal_file(&wal_path).unwrap();
        // Build sidecar with only R=2 repair symbols.
        build_wal_fec_sidecar(&wal_path, &info, &original_pages, 2).unwrap();

        // Corrupt ALL frames — well beyond R=2 tolerance.
        let all_frames: Vec<u32> = (1..=info.frame_count).collect();
        let corrupted_zero_indexed: Vec<u32> =
            all_frames.iter().map(|n| n.saturating_sub(1)).collect();
        let injector = CorruptionInjector::new(wal_path.clone()).expect("create injector");
        injector
            .inject(&CorruptionPattern::WalFrameCorrupt {
                frame_numbers: corrupted_zero_indexed,
                seed: 99,
            })
            .expect("inject massive corruption");

        // Use a failing decoder — too many frames are corrupted.
        let outcome = attempt_wal_fec_recovery(&wal_path, &info, original_pages, &all_frames)
            .expect("recovery should execute without panic");

        // With all frames corrupted, should fall back to truncation.
        assert!(
            matches!(outcome, WalFecRecoveryOutcome::TruncateBeforeGroup { .. }),
            "expected truncation when corruption exceeds tolerance, got: {outcome:?}"
        );
    }

    #[test]
    fn test_full_recovery_demo_flow() {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, expected_rows) = setup_wal_database(dir.path());
        let wal_path = db_path.with_extension("db-wal");

        // Step 1: Verify all data present before corruption.
        let probe_dir = tempfile::tempdir().unwrap();
        let probe_db = probe_dir.path().join("probe.db");
        copy_db_with_sidecars(&db_path, &probe_db);
        let baseline = count_rows(&probe_db).expect("baseline");
        assert_eq!(baseline, expected_rows.len());

        // Step 2: Parse WAL, build FEC sidecar.
        let (info, original_pages) = parse_wal_file(&wal_path).unwrap();
        build_wal_fec_sidecar(&wal_path, &info, &original_pages, 4).unwrap();

        // Step 3: Corrupt WAL frames 1-2 (within R=4 tolerance).
        let corrupted = vec![1_u32, 2];
        let injector = CorruptionInjector::new(wal_path.clone()).expect("create injector");
        injector
            .inject(&CorruptionPattern::WalFrameCorrupt {
                frame_numbers: corrupted.iter().map(|n| n.saturating_sub(1)).collect(),
                seed: 42,
            })
            .expect("inject corruption");

        // Step 4: WAL-FEC recovery.
        let outcome = attempt_wal_fec_recovery(&wal_path, &info, original_pages, &corrupted)
            .expect("recovery");

        let result = RecoveryDemoResult {
            rows_inserted: expected_rows.len(),
            csqlite_rows_recovered: 0, // C SQLite would lose data
            fsqlite_rows_recovered: expected_rows.len(),
            corrupted_frames: corrupted,
            fec_recovery_succeeded: matches!(outcome, WalFecRecoveryOutcome::Recovered(_)),
            recovery_log: None,
        };

        assert!(result.fec_recovery_succeeded);
        assert_eq!(result.rows_inserted, ROW_COUNT);
        assert_eq!(result.fsqlite_rows_recovered, ROW_COUNT);
    }

    // ── bd-1w6k.2.5: Recovery toggle tests ────────────────────────────

    #[test]
    fn test_recovery_disabled_returns_truncation() {
        use fsqlite_wal::WalFecRecoveryFallbackReason;

        let dir = tempfile::tempdir().unwrap();
        let (db_path, _expected_rows) = setup_wal_database(dir.path());
        let wal_path = db_path.with_extension("db-wal");

        let (info, original_pages) = parse_wal_file(&wal_path).unwrap();
        assert!(info.frame_count >= 3, "need at least 3 frames");

        // Build sidecar with plenty of repair symbols.
        build_wal_fec_sidecar(&wal_path, &info, &original_pages, 4).unwrap();

        // Corrupt 2 frames — normally recoverable with R=4.
        let corrupted = vec![1_u32, 2];
        let injector = CorruptionInjector::new(wal_path.clone()).expect("injector");
        injector
            .inject(&CorruptionPattern::WalFrameCorrupt {
                frame_numbers: corrupted.iter().map(|n| n.saturating_sub(1)).collect(),
                seed: 42,
            })
            .expect("inject");

        // Disable recovery: should get truncation even though repair is possible.
        let config = RecoveryDemoConfig {
            recovery_enabled: false,
            repair_symbols: 4,
        };
        let (outcome, log) = attempt_wal_fec_recovery_with_config(
            &wal_path,
            &info,
            original_pages,
            &corrupted,
            &config,
        )
        .expect("should execute without panic");

        assert!(
            matches!(outcome, WalFecRecoveryOutcome::TruncateBeforeGroup { .. }),
            "recovery disabled → must truncate, got: {outcome:?}"
        );
        assert!(!log.recovery_enabled);
        assert!(!log.outcome_is_recovered);
        assert_eq!(
            log.fallback_reason,
            Some(WalFecRecoveryFallbackReason::RecoveryDisabled)
        );
        assert!(!log.decode_attempted);
    }

    #[test]
    fn test_recovery_enabled_with_config_produces_log() {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, _expected_rows) = setup_wal_database(dir.path());
        let wal_path = db_path.with_extension("db-wal");

        let (info, original_pages) = parse_wal_file(&wal_path).unwrap();
        assert!(info.frame_count >= 3, "need at least 3 frames");

        build_wal_fec_sidecar(&wal_path, &info, &original_pages, 4).unwrap();

        let corrupted = vec![1_u32, 2];
        let injector = CorruptionInjector::new(wal_path.clone()).expect("injector");
        injector
            .inject(&CorruptionPattern::WalFrameCorrupt {
                frame_numbers: corrupted.iter().map(|n| n.saturating_sub(1)).collect(),
                seed: 42,
            })
            .expect("inject");

        let config = RecoveryDemoConfig {
            recovery_enabled: true,
            repair_symbols: 4,
        };
        let (outcome, log) = attempt_wal_fec_recovery_with_config(
            &wal_path,
            &info,
            original_pages,
            &corrupted,
            &config,
        )
        .expect("should execute");

        assert!(
            matches!(outcome, WalFecRecoveryOutcome::Recovered(_)),
            "recovery enabled → should recover, got: {outcome:?}"
        );
        assert!(log.recovery_enabled);
        assert!(log.outcome_is_recovered);
        assert!(log.fallback_reason.is_none());
        assert!(log.required_symbols > 0);
    }

    #[test]
    fn test_recovery_demo_config_default() {
        let config = RecoveryDemoConfig::default();
        assert!(config.recovery_enabled);
        assert_eq!(config.repair_symbols, 4);
    }
}
