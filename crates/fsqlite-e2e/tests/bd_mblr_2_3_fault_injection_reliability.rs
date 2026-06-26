//! Fault-injection end-to-end reliability tests.
//!
//! Bead: bd-mblr.2.3

#![allow(clippy::too_many_lines)]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{
    FaultInjectingVfs, FaultKind, FaultMetricsSnapshot, FaultSpec, FaultTriggerRecord,
};
use fsqlite_pager::{MvccPager, SimplePager, TransactionHandle, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_types::{LockLevel, PageNumber, PageSize};
use fsqlite_vfs::traits::{Vfs, VfsFile};
use fsqlite_vfs::{MemoryVfs, ShmRegion};
use serde_json::{Value, json};

const BEAD_ID: &str = "bd-mblr.2.3";
const SUITE_SEED: u64 = 0xBD23_0000_5EED_u64;
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_mblr_2_3_fault_injection_reliability -- --nocapture --test-threads=1";

fn emit_fault_log(test_name: &str, phase: &str, extra: Value) {
    eprintln!(
        "FAULT_INJECTION_E2E:{}",
        json!({
            "bead_id": BEAD_ID,
            "seed": SUITE_SEED,
            "replay_command": REPLAY_COMMAND,
            "test_name": test_name,
            "phase": phase,
            "extra": extra,
        })
    );
}

fn sample_page(fill: u8) -> Vec<u8> {
    vec![fill; PageSize::DEFAULT.as_usize()]
}

fn seed_committed_page(backing: &MemoryVfs, path: &Path, fill: u8) -> (PageNumber, Vec<u8>) {
    let cx = Cx::new();
    let pager = SimplePager::open_with_cx(&cx, backing.clone(), path, PageSize::DEFAULT)
        .expect("open seed pager");
    let original = sample_page(fill);
    let page_no = {
        let mut txn = pager
            .begin(&cx, TransactionMode::Immediate)
            .expect("begin seed txn");
        let page_no = txn.allocate_page(&cx).expect("allocate seed page");
        txn.write_page(&cx, page_no, &original)
            .expect("write seed page");
        txn.commit(&cx).expect("commit seed txn");
        page_no
    };
    drop(pager);
    (page_no, original)
}

fn read_committed_page(backing: &MemoryVfs, path: &Path, page_no: PageNumber) -> Vec<u8> {
    let cx = Cx::new();
    let pager = SimplePager::open_with_cx(&cx, backing.clone(), path, PageSize::DEFAULT)
        .expect("open reader pager");
    let reader = pager
        .begin(&cx, TransactionMode::ReadOnly)
        .expect("begin readonly txn");
    let bytes = reader
        .get_page(&cx, page_no)
        .expect("read committed page")
        .as_ref()
        .to_vec();
    drop(reader);
    bytes
}

fn fault_metrics_json(metrics: &FaultMetricsSnapshot) -> Value {
    json!({
        "metric_name": metrics.metric_name,
        "by_fault_type": metrics.by_fault_type,
        "total": metrics.total,
    })
}

fn fault_triggers_json(triggers: &[FaultTriggerRecord]) -> Value {
    Value::Array(
        triggers
            .iter()
            .map(|trigger| {
                json!({
                    "spec_index": trigger.spec_index,
                    "path": trigger.path.display().to_string(),
                    "kind": fault_kind_json(&trigger.kind),
                    "detail": trigger.detail,
                })
            })
            .collect(),
    )
}

fn fault_kind_json(kind: &FaultKind) -> Value {
    match kind {
        FaultKind::TornWrite { valid_bytes } => {
            json!({ "kind": "torn_write", "valid_bytes": valid_bytes })
        }
        FaultKind::PartialWrite { valid_bytes } => {
            json!({ "kind": "partial_write", "valid_bytes": valid_bytes })
        }
        FaultKind::PowerCut => json!({ "kind": "power_cut" }),
        FaultKind::IoError => json!({ "kind": "io_error" }),
        FaultKind::ReadFailure => json!({ "kind": "read_failure" }),
        FaultKind::WriteFailure => json!({ "kind": "write_failure" }),
        FaultKind::Latency {
            base_millis,
            jitter_millis,
        } => json!({
            "kind": "latency",
            "base_millis": base_millis,
            "jitter_millis": jitter_millis,
        }),
        FaultKind::DiskFull => json!({ "kind": "disk_full" }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartialReadSpec {
    skip_matches: usize,
    valid_bytes: usize,
}

#[derive(Debug, Default)]
struct TargetedFaultState {
    next_partial_read: Option<PartialReadSpec>,
    sync_io_armed: bool,
    triggered_partial_reads: usize,
    triggered_sync_failures: usize,
    last_partial_read_detail: Option<String>,
    last_sync_failure_detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TargetedFaultSnapshot {
    triggered_partial_reads: usize,
    triggered_sync_failures: usize,
    last_partial_read_detail: Option<String>,
    last_sync_failure_detail: Option<String>,
}

#[derive(Debug)]
struct TargetedFaultVfs<V: Vfs> {
    inner: V,
    state: Arc<Mutex<TargetedFaultState>>,
}

impl<V: Vfs> TargetedFaultVfs<V> {
    fn new(inner: V) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(TargetedFaultState::default())),
        }
    }

    fn inject_partial_read_after(&self, skip_matches: usize, valid_bytes: usize) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_partial_read = Some(PartialReadSpec {
            skip_matches,
            valid_bytes,
        });
    }

    fn inject_sync_io(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sync_io_armed = true;
    }

    fn snapshot(&self) -> TargetedFaultSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        TargetedFaultSnapshot {
            triggered_partial_reads: state.triggered_partial_reads,
            triggered_sync_failures: state.triggered_sync_failures,
            last_partial_read_detail: state.last_partial_read_detail.clone(),
            last_sync_failure_detail: state.last_sync_failure_detail.clone(),
        }
    }
}

#[derive(Debug)]
struct TargetedFaultFile<F: VfsFile> {
    inner: F,
    state: Arc<Mutex<TargetedFaultState>>,
}

fn targeted_io_error(message: &'static str) -> FrankenError {
    FrankenError::Io(io::Error::other(message))
}

impl<V: Vfs> Vfs for TargetedFaultVfs<V> {
    type File = TargetedFaultFile<V::File>;

    fn name(&self) -> &'static str {
        "bd-mblr-targeted-fault-vfs"
    }

    fn open(
        &self,
        cx: &Cx,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (inner, out_flags) = self.inner.open(cx, path, flags)?;
        Ok((
            TargetedFaultFile {
                inner,
                state: Arc::clone(&self.state),
            },
            out_flags,
        ))
    }

    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()> {
        self.inner.delete(cx, path, sync_dir)
    }

    fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool> {
        self.inner.access(cx, path, flags)
    }

    fn full_pathname(&self, cx: &Cx, path: &Path) -> Result<PathBuf> {
        self.inner.full_pathname(cx, path)
    }

    fn randomness(&self, cx: &Cx, buf: &mut [u8]) {
        self.inner.randomness(cx, buf);
    }

    fn current_time(&self, cx: &Cx) -> f64 {
        self.inner.current_time(cx)
    }

    fn is_memory(&self) -> bool {
        self.inner.is_memory()
    }
}

impl<F: VfsFile> VfsFile for TargetedFaultFile<F> {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.inner.close(cx)
    }

    fn read(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        let fault = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(mut spec) = state.next_partial_read.take() {
                if spec.skip_matches == 0 {
                    state.triggered_partial_reads += 1;
                    state.last_partial_read_detail = Some(format!(
                        "offset={offset} requested={} valid_bytes={}",
                        buf.len(),
                        spec.valid_bytes
                    ));
                    Some(spec)
                } else {
                    spec.skip_matches -= 1;
                    state.next_partial_read = Some(spec);
                    None
                }
            } else {
                None
            }
        };

        if let Some(spec) = fault {
            let requested = spec.valid_bytes.min(buf.len());
            let mut scratch = vec![0_u8; requested];
            let actual = self.inner.read(cx, &mut scratch, offset)?;
            buf.fill(0);
            if actual > 0 {
                buf[..actual].copy_from_slice(&scratch[..actual]);
            }
            Ok(actual)
        } else {
            self.inner.read(cx, buf, offset)
        }
    }

    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        self.inner.write(cx, buf, offset)
    }

    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.inner.truncate(cx, size)
    }

    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        let should_fail = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.sync_io_armed {
                state.sync_io_armed = false;
                state.triggered_sync_failures += 1;
                state.last_sync_failure_detail = Some(format!("flags={flags:?}"));
                true
            } else {
                false
            }
        };

        if should_fail {
            Err(targeted_io_error("fault injection: sync failure"))
        } else {
            self.inner.sync(cx, flags)
        }
    }

    fn file_size(&self, cx: &Cx) -> Result<u64> {
        self.inner.file_size(cx)
    }

    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.lock(cx, level)
    }

    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.unlock(cx, level)
    }

    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
        self.inner.check_reserved_lock(cx)
    }

    fn sector_size(&self) -> u32 {
        self.inner.sector_size()
    }

    fn device_characteristics(&self) -> u32 {
        self.inner.device_characteristics()
    }

    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
        self.inner.shm_map(cx, region, size, extend)
    }

    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        self.inner.shm_lock(cx, offset, n, flags)
    }

    fn shm_barrier(&self) {
        self.inner.shm_barrier();
    }

    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
        self.inner.shm_unmap(cx, delete)
    }

    fn set_busy_timeout_ms(&mut self, ms: u64) {
        self.inner.set_busy_timeout_ms(ms);
    }
}

#[test]
fn fault_injection_disk_full_during_commit_preserves_preexisting_page() {
    let path = PathBuf::from("/bd_mblr_2_3_disk_full.db");
    let backing = MemoryVfs::new();
    let (page_no, original) = seed_committed_page(&backing, &path, 0x11);
    let cx = Cx::new();

    let fault_vfs = FaultInjectingVfs::with_seed(backing.clone(), SUITE_SEED ^ 0x01);
    fault_vfs.inject_fault(FaultSpec::disk_full("*.db-journal").build());
    let pager =
        SimplePager::open_with_cx(&cx, fault_vfs, &path, PageSize::DEFAULT).expect("open pager");

    let err = {
        let mut txn = pager
            .begin(&cx, TransactionMode::Immediate)
            .expect("begin txn");
        txn.write_page(&cx, page_no, &sample_page(0x7A))
            .expect("stage updated page");
        txn.commit(&cx)
            .err()
            .unwrap_or_else(|| panic!("commit should fail"))
    };
    assert!(
        matches!(err, FrankenError::DatabaseFull),
        "disk-full fault must surface DatabaseFull, got {err:?}"
    );

    let fault_handle = pager.vfs_handle();
    let metrics = fault_handle.metrics_snapshot();
    let triggers = fault_handle.triggered_faults();
    assert_eq!(metrics.by_fault_type.get("disk_full"), Some(&1));
    assert_eq!(triggers.len(), 1, "disk-full must trigger exactly once");
    drop(pager);

    let recovered = read_committed_page(&backing, &path, page_no);
    assert_eq!(
        recovered, original,
        "disk-full fault must preserve the last committed page image"
    );

    emit_fault_log(
        "fault_injection_disk_full_during_commit_preserves_preexisting_page",
        "result",
        json!({
            "error": err.to_string(),
            "page_no": page_no.get(),
            "metrics": fault_metrics_json(&metrics),
            "triggered_faults": fault_triggers_json(&triggers),
        }),
    );
}

#[test]
fn fault_injection_io_error_mid_write_recovers_original_page_on_reopen() {
    let path = PathBuf::from("/bd_mblr_2_3_partial_write.db");
    let backing = MemoryVfs::new();
    let (page_no, original) = seed_committed_page(&backing, &path, 0x21);
    let cx = Cx::new();

    let fault_vfs = FaultInjectingVfs::with_seed(backing.clone(), SUITE_SEED ^ 0x02);
    fault_vfs.inject_fault(FaultSpec::partial_write("*.db").bytes_written(128).build());
    let pager =
        SimplePager::open_with_cx(&cx, fault_vfs, &path, PageSize::DEFAULT).expect("open pager");

    let err = {
        let mut txn = pager
            .begin(&cx, TransactionMode::Immediate)
            .expect("begin txn");
        txn.write_page(&cx, page_no, &sample_page(0x8C))
            .expect("stage updated page");
        txn.commit(&cx)
            .err()
            .unwrap_or_else(|| panic!("commit should fail"))
    };
    assert!(
        matches!(err, FrankenError::Io(_)),
        "partial main-db write must surface Io error, got {err:?}"
    );
    assert!(
        err.to_string().contains("partial write"),
        "mid-write error should preserve the partial-write diagnostic, got {err}"
    );

    let fault_handle = pager.vfs_handle();
    let metrics = fault_handle.metrics_snapshot();
    let triggers = fault_handle.triggered_faults();
    assert_eq!(metrics.by_fault_type.get("partial_write"), Some(&1));
    assert_eq!(triggers.len(), 1, "partial-write must trigger exactly once");
    drop(pager);

    let recovered = read_committed_page(&backing, &path, page_no);
    assert_eq!(
        recovered, original,
        "journal recovery must restore the last committed page after a mid-write I/O fault"
    );

    emit_fault_log(
        "fault_injection_io_error_mid_write_recovers_original_page_on_reopen",
        "result",
        json!({
            "error": err.to_string(),
            "page_no": page_no.get(),
            "metrics": fault_metrics_json(&metrics),
            "triggered_faults": fault_triggers_json(&triggers),
        }),
    );
}

#[test]
fn fault_injection_partial_read_on_page_fetch_reports_short_read_diagnostic() {
    let path = PathBuf::from("/bd_mblr_2_3_partial_read.db");
    let backing = MemoryVfs::new();
    let (page_no, original) = seed_committed_page(&backing, &path, 0x33);
    let cx = Cx::new();

    let fault_vfs = TargetedFaultVfs::new(backing.clone());
    let pager =
        SimplePager::open_with_cx(&cx, fault_vfs, &path, PageSize::DEFAULT).expect("open pager");

    let reader = pager
        .begin(&cx, TransactionMode::ReadOnly)
        .expect("begin readonly txn");
    pager
        .vfs_handle()
        .inject_partial_read_after(0, PageSize::DEFAULT.as_usize() / 2);
    let err = reader
        .get_page(&cx, page_no)
        .expect_err("short read on page fetch should fail");
    let detail = match &err {
        FrankenError::DatabaseCorrupt { detail } => detail,
        other => panic!("expected DatabaseCorrupt for short read, got {other:?}"),
    };
    assert!(
        detail.contains("short read fetching page"),
        "partial-read failure must explain the short-read root cause: {detail}"
    );
    drop(reader);

    let snapshot = pager.vfs_handle().snapshot();
    assert_eq!(snapshot.triggered_partial_reads, 1);
    drop(pager);

    let recovered = read_committed_page(&backing, &path, page_no);
    assert_eq!(
        recovered, original,
        "transient partial read must not mutate the committed database image"
    );

    emit_fault_log(
        "fault_injection_partial_read_on_page_fetch_reports_short_read_diagnostic",
        "result",
        json!({
            "error": err.to_string(),
            "page_no": page_no.get(),
            "snapshot": {
                "triggered_partial_reads": snapshot.triggered_partial_reads,
                "triggered_sync_failures": snapshot.triggered_sync_failures,
                "last_partial_read_detail": snapshot.last_partial_read_detail,
                "last_sync_failure_detail": snapshot.last_sync_failure_detail,
            },
        }),
    );
}

#[test]
fn fault_injection_fsync_failure_during_commit_preserves_preexisting_page() {
    let path = PathBuf::from("/bd_mblr_2_3_sync_failure.db");
    let backing = MemoryVfs::new();
    let (page_no, original) = seed_committed_page(&backing, &path, 0x44);
    let cx = Cx::new();

    let fault_vfs = TargetedFaultVfs::new(backing.clone());
    fault_vfs.inject_sync_io();
    let pager =
        SimplePager::open_with_cx(&cx, fault_vfs, &path, PageSize::DEFAULT).expect("open pager");

    let err = {
        let mut txn = pager
            .begin(&cx, TransactionMode::Immediate)
            .expect("begin txn");
        txn.write_page(&cx, page_no, &sample_page(0xC1))
            .expect("stage updated page");
        txn.commit(&cx)
            .err()
            .unwrap_or_else(|| panic!("commit should fail"))
    };
    assert!(
        matches!(err, FrankenError::Io(_)),
        "fsync failure must surface Io error, got {err:?}"
    );
    assert!(
        err.to_string().contains("sync failure"),
        "sync-failure diagnostic should be preserved, got {err}"
    );

    let snapshot = pager.vfs_handle().snapshot();
    assert_eq!(snapshot.triggered_sync_failures, 1);
    drop(pager);

    let recovered = read_committed_page(&backing, &path, page_no);
    assert_eq!(
        recovered, original,
        "sync failure must leave the last committed page recoverable on reopen"
    );

    emit_fault_log(
        "fault_injection_fsync_failure_during_commit_preserves_preexisting_page",
        "result",
        json!({
            "error": err.to_string(),
            "page_no": page_no.get(),
            "snapshot": {
                "triggered_partial_reads": snapshot.triggered_partial_reads,
                "triggered_sync_failures": snapshot.triggered_sync_failures,
                "last_partial_read_detail": snapshot.last_partial_read_detail,
                "last_sync_failure_detail": snapshot.last_sync_failure_detail,
            },
        }),
    );
}
