//! Fault-injection end-to-end reliability tests.
//!
//! Bead: bd-mblr.2.3

#![allow(clippy::too_many_lines)]
#![recursion_limit = "512"]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::io::{Seek, SeekFrom, Write};

use fsqlite_e2e::block_on;
use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{
    FaultInjectingVfs, FaultKind, FaultMetricsSnapshot, FaultSpec, FaultTriggerRecord,
};
#[cfg(unix)]
use fsqlite_pager::pager::DatabaseImageReceipt;
use fsqlite_pager::{MvccPager, SimplePager, TransactionHandle, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_types::{LockLevel, PageNumber, PageSize};
#[cfg(unix)]
use fsqlite_vfs::UnixVfs;
use fsqlite_vfs::traits::{Vfs, VfsFile};
use fsqlite_vfs::{FileIdentity, MemoryVfs, ShmRegion, SyncKind};
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
    let pager = block_on(SimplePager::open_with_cx(
        &cx,
        backing.clone(),
        path,
        PageSize::DEFAULT,
    ))
    .expect("open seed pager");
    let original = sample_page(fill);
    let page_no = {
        let mut txn =
            block_on(pager.begin(&cx, TransactionMode::Immediate)).expect("begin seed txn");
        let page_no = block_on(txn.allocate_page(&cx)).expect("allocate seed page");
        block_on(txn.write_page(&cx, page_no, &original)).expect("write seed page");
        block_on(txn.commit(&cx)).expect("commit seed txn");
        page_no
    };
    drop(pager);
    (page_no, original)
}

fn read_committed_page(backing: &MemoryVfs, path: &Path, page_no: PageNumber) -> Vec<u8> {
    let cx = Cx::new();
    let pager = block_on(SimplePager::open_with_cx(
        &cx,
        backing.clone(),
        path,
        PageSize::DEFAULT,
    ))
    .expect("open reader pager");
    let reader = block_on(pager.begin(&cx, TransactionMode::ReadOnly)).expect("begin readonly txn");
    let bytes = block_on(reader.get_page(&cx, page_no))
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

    fn file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.file_identity()
    }

    fn read<'a>(
        &'a self,
        cx: &'a Cx,
        buf: &'a mut [u8],
        offset: u64,
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a {
        async move {
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
                let actual = self.inner.read(cx, &mut scratch, offset).await?;
                buf.fill(0);
                if actual > 0 {
                    buf[..actual].copy_from_slice(&scratch[..actual]);
                }
                Ok(actual)
            } else {
                self.inner.read(cx, buf, offset).await
            }
        }
    }

    fn write<'a>(
        &'a self,
        cx: &'a Cx,
        buf: &'a [u8],
        offset: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
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

    fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
        let should_fail = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.sync_io_armed {
                state.sync_io_armed = false;
                state.triggered_sync_failures += 1;
                state.last_sync_failure_detail = Some(format!("kind={kind:?}"));
                true
            } else {
                false
            }
        };

        if should_fail {
            Err(targeted_io_error("fault injection: sync failure"))
        } else {
            self.inner.durable_sync(cx, kind)
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

    fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_shared_snapshot(cx)
    }

    fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_shared_snapshot_attempt(cx)
    }

    fn lock_external_maintenance(&mut self, cx: &Cx, wal_mode: bool) -> Result<()> {
        self.inner.lock_external_maintenance(cx, wal_mode)
    }

    fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_maintenance_attempt(cx)
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
    let pager = block_on(SimplePager::open_with_cx(
        &cx,
        fault_vfs,
        &path,
        PageSize::DEFAULT,
    ))
    .expect("open pager");

    let err = {
        let mut txn = block_on(pager.begin(&cx, TransactionMode::Immediate)).expect("begin txn");
        block_on(txn.write_page(&cx, page_no, &sample_page(0x7A))).expect("stage updated page");
        block_on(txn.commit(&cx))
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
    let pager = block_on(SimplePager::open_with_cx(
        &cx,
        fault_vfs,
        &path,
        PageSize::DEFAULT,
    ))
    .expect("open pager");

    let err = {
        let mut txn = block_on(pager.begin(&cx, TransactionMode::Immediate)).expect("begin txn");
        block_on(txn.write_page(&cx, page_no, &sample_page(0x8C))).expect("stage updated page");
        block_on(txn.commit(&cx))
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
fn targeted_fault_wrapper_preserves_identity_and_durable_sync_semantics() {
    let path = PathBuf::from("/bd_mblr_2_3_wrapper_contract.db");
    let backing = MemoryVfs::new();
    let fault_vfs = TargetedFaultVfs::new(backing);
    let cx = Cx::new();
    let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
    let (mut file, _) = fault_vfs
        .open(&cx, Some(&path), flags)
        .expect("open targeted fault file");

    assert!(
        file.file_identity()
            .expect("read wrapped file identity")
            .is_some(),
        "the wrapper must preserve the open file's stable identity"
    );

    fault_vfs.inject_sync_io();
    let error = file
        .durable_sync(&cx, SyncKind::FullDurable)
        .expect_err("armed sync fault must apply to durable sync");
    assert!(matches!(error, FrankenError::Io(_)));
    let snapshot = fault_vfs.snapshot();
    assert_eq!(snapshot.triggered_sync_failures, 1);
    assert_eq!(
        snapshot.last_sync_failure_detail.as_deref(),
        Some("kind=FullDurable"),
        "the wrapper must observe durable intent rather than collapsing through sync"
    );

    file.durable_sync(&cx, SyncKind::FullDurable)
        .expect("one-shot fault must leave later durable syncs delegated");
    file.close(&cx).expect("close targeted fault file");
}

#[test]
fn fault_injection_partial_read_on_page_fetch_reports_short_read_diagnostic() {
    let path = PathBuf::from("/bd_mblr_2_3_partial_read.db");
    let backing = MemoryVfs::new();
    let (page_no, original) = seed_committed_page(&backing, &path, 0x33);
    let cx = Cx::new();

    let fault_vfs = TargetedFaultVfs::new(backing.clone());
    let pager = block_on(SimplePager::open_with_cx(
        &cx,
        fault_vfs,
        &path,
        PageSize::DEFAULT,
    ))
    .expect("open pager");

    let reader = block_on(pager.begin(&cx, TransactionMode::ReadOnly)).expect("begin readonly txn");
    pager
        .vfs_handle()
        .inject_partial_read_after(0, PageSize::DEFAULT.as_usize() / 2);
    let err =
        block_on(reader.get_page(&cx, page_no)).expect_err("short read on page fetch should fail");
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
    let pager = block_on(SimplePager::open_with_cx(
        &cx,
        fault_vfs,
        &path,
        PageSize::DEFAULT,
    ))
    .expect("open pager");

    let err = {
        let mut txn = block_on(pager.begin(&cx, TransactionMode::Immediate)).expect("begin txn");
        block_on(txn.write_page(&cx, page_no, &sample_page(0xC1))).expect("stage updated page");
        block_on(txn.commit(&cx))
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

// ---------------------------------------------------------------------------
// GH-138: in-place VACUUM publication fault matrix
// ---------------------------------------------------------------------------

#[cfg(unix)]
const VACUUM_SOURCE_ROW_COUNT: i64 = 16;
#[cfg(unix)]
const VACUUM_CANDIDATE_ROW_COUNT: i64 = 2;

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VacuumFileRole {
    Source,
    Candidate,
    Journal,
    Other,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VacuumFaultOperation {
    Write,
    Truncate,
    DurableSync,
    Close,
    Delete,
    SyncParentDirectory,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VacuumFaultEffect {
    ReturnError,
    PartialWrite { valid_bytes: usize },
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct VacuumFaultSpec {
    label: &'static str,
    role: VacuumFileRole,
    operation: VacuumFaultOperation,
    nth_match: usize,
    effect: VacuumFaultEffect,
}

#[cfg(unix)]
impl VacuumFaultSpec {
    const fn error(
        label: &'static str,
        role: VacuumFileRole,
        operation: VacuumFaultOperation,
        nth_match: usize,
    ) -> Self {
        Self {
            label,
            role,
            operation,
            nth_match,
            effect: VacuumFaultEffect::ReturnError,
        }
    }

    const fn partial_write(
        label: &'static str,
        role: VacuumFileRole,
        nth_match: usize,
        valid_bytes: usize,
    ) -> Self {
        Self {
            label,
            role,
            operation: VacuumFaultOperation::Write,
            nth_match,
            effect: VacuumFaultEffect::PartialWrite { valid_bytes },
        }
    }
}

#[cfg(unix)]
#[derive(Debug, Default)]
struct VacuumFaultState {
    armed: Option<VacuumFaultSpec>,
    matching_operations_seen: usize,
    fired_label: Option<&'static str>,
    cancel_parent_after_source_write: Option<Cx>,
    cancelled_after_source_write: bool,
    silent_replay_corruption_armed: bool,
    silent_replay_source_writes_seen: usize,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct VacuumFaultSnapshot {
    matching_operations_seen: usize,
    fired_label: Option<&'static str>,
    cancelled_after_source_write: bool,
}

#[cfg(unix)]
struct VacuumFaultVfs<V: Vfs> {
    inner: V,
    source_path: PathBuf,
    candidate_path: PathBuf,
    journal_path: PathBuf,
    state: Arc<Mutex<VacuumFaultState>>,
}

#[cfg(unix)]
impl<V: Vfs> VacuumFaultVfs<V> {
    fn new(inner: V, source_path: PathBuf, candidate_path: PathBuf) -> Self {
        let source_path =
            std::fs::canonicalize(source_path).expect("canonicalize VACUUM source path");
        let candidate_path =
            std::fs::canonicalize(candidate_path).expect("canonicalize VACUUM candidate path");
        let journal_path = journal_path_for(&source_path);
        Self {
            inner,
            source_path,
            candidate_path,
            journal_path,
            state: Arc::new(Mutex::new(VacuumFaultState::default())),
        }
    }

    fn arm(&self, spec: VacuumFaultSpec) {
        assert!(spec.nth_match > 0, "fault match indices are one-based");
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = VacuumFaultState {
            armed: Some(spec),
            matching_operations_seen: 0,
            fired_label: None,
            cancel_parent_after_source_write: None,
            cancelled_after_source_write: false,
            silent_replay_corruption_armed: false,
            silent_replay_source_writes_seen: 0,
        };
    }

    fn arm_cancel_after_first_source_write(&self, parent_cx: &Cx) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = VacuumFaultState {
            armed: None,
            matching_operations_seen: 0,
            fired_label: None,
            cancel_parent_after_source_write: Some(parent_cx.clone()),
            cancelled_after_source_write: false,
            silent_replay_corruption_armed: false,
            silent_replay_source_writes_seen: 0,
        };
    }

    fn arm_silent_replay_corruption(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = VacuumFaultState {
            silent_replay_corruption_armed: true,
            ..VacuumFaultState::default()
        };
    }

    fn disarm(&self) {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = VacuumFaultState::default();
    }

    fn snapshot(&self) -> VacuumFaultSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        VacuumFaultSnapshot {
            matching_operations_seen: state.matching_operations_seen,
            fired_label: state.fired_label,
            cancelled_after_source_write: state.cancelled_after_source_write,
        }
    }

    fn role_for(&self, path: Option<&Path>, flags: VfsOpenFlags) -> VacuumFileRole {
        if flags.contains(VfsOpenFlags::MAIN_JOURNAL)
            || path.is_some_and(|path| path == self.journal_path)
        {
            VacuumFileRole::Journal
        } else if path.is_some_and(|path| path == self.source_path) {
            VacuumFileRole::Source
        } else if path.is_some_and(|path| path == self.candidate_path) {
            VacuumFileRole::Candidate
        } else {
            VacuumFileRole::Other
        }
    }

    fn wrap_file(
        &self,
        inner: V::File,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> VacuumFaultFile<V::File> {
        VacuumFaultFile {
            inner,
            role: self.role_for(path, flags),
            state: Arc::clone(&self.state),
        }
    }

    fn take_fault(
        &self,
        role: VacuumFileRole,
        operation: VacuumFaultOperation,
    ) -> Option<(VacuumFaultEffect, &'static str)> {
        take_vacuum_fault(&self.state, role, operation)
    }
}

#[cfg(unix)]
struct VacuumFaultFile<F: VfsFile> {
    inner: F,
    role: VacuumFileRole,
    state: Arc<Mutex<VacuumFaultState>>,
}

#[cfg(unix)]
fn take_vacuum_fault(
    state: &Arc<Mutex<VacuumFaultState>>,
    role: VacuumFileRole,
    operation: VacuumFaultOperation,
) -> Option<(VacuumFaultEffect, &'static str)> {
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let spec = state.armed?;
    if state.fired_label.is_some() || spec.role != role || spec.operation != operation {
        return None;
    }
    state.matching_operations_seen = state.matching_operations_seen.saturating_add(1);
    if state.matching_operations_seen != spec.nth_match {
        return None;
    }
    state.fired_label = Some(spec.label);
    Some((spec.effect, spec.label))
}

#[cfg(unix)]
fn take_parent_to_cancel_after_source_write(
    state: &Arc<Mutex<VacuumFaultState>>,
    role: VacuumFileRole,
) -> Option<Cx> {
    if role != VacuumFileRole::Source {
        return None;
    }
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let parent = state.cancel_parent_after_source_write.take()?;
    state.cancelled_after_source_write = true;
    Some(parent)
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SilentReplayWriteAction {
    TriggerRollback,
    CorruptSilently,
}

#[cfg(unix)]
fn take_silent_replay_write_action(
    state: &Arc<Mutex<VacuumFaultState>>,
    role: VacuumFileRole,
) -> Option<SilentReplayWriteAction> {
    if role != VacuumFileRole::Source {
        return None;
    }
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !state.silent_replay_corruption_armed {
        return None;
    }
    state.silent_replay_source_writes_seen =
        state.silent_replay_source_writes_seen.saturating_add(1);
    match state.silent_replay_source_writes_seen {
        1 => Some(SilentReplayWriteAction::TriggerRollback),
        2 => {
            state.silent_replay_corruption_armed = false;
            state.fired_label = Some("silent-replay-page-corruption");
            Some(SilentReplayWriteAction::CorruptSilently)
        }
        _ => None,
    }
}

#[cfg(unix)]
fn vacuum_injected_error(label: &str) -> FrankenError {
    FrankenError::Io(io::Error::other(format!(
        "GH-138 VACUUM fault injection: {label}"
    )))
}

#[cfg(unix)]
impl<V: Vfs> Vfs for VacuumFaultVfs<V> {
    type File = VacuumFaultFile<V::File>;

    fn name(&self) -> &'static str {
        "gh-138-vacuum-fault-vfs"
    }

    fn open(
        &self,
        cx: &Cx,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (inner, out_flags) = self.inner.open(cx, path, flags)?;
        Ok((self.wrap_file(inner, path, flags), out_flags))
    }

    fn open_with_expected_identity(
        &self,
        cx: &Cx,
        path: &Path,
        flags: VfsOpenFlags,
        expected_identity: FileIdentity,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (inner, out_flags) =
            self.inner
                .open_with_expected_identity(cx, path, flags, expected_identity)?;
        Ok((self.wrap_file(inner, Some(path), flags), out_flags))
    }

    fn open_reserved_with_expected_identity(
        &self,
        cx: &Cx,
        path: &Path,
        flags: VfsOpenFlags,
        expected_identity: FileIdentity,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (inner, out_flags) =
            self.inner
                .open_reserved_with_expected_identity(cx, path, flags, expected_identity)?;
        Ok((self.wrap_file(inner, Some(path), flags), out_flags))
    }

    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()> {
        let role = self.role_for(Some(path), VfsOpenFlags::empty());
        if let Some((_, label)) = self.take_fault(role, VacuumFaultOperation::Delete) {
            return Err(vacuum_injected_error(label));
        }
        self.inner.delete(cx, path, sync_dir)
    }

    fn sync_parent_directory(&self, cx: &Cx, path: &Path) -> Result<()> {
        let role = self.role_for(Some(path), VfsOpenFlags::empty());
        if let Some((_, label)) = self.take_fault(role, VacuumFaultOperation::SyncParentDirectory) {
            return Err(vacuum_injected_error(label));
        }
        self.inner.sync_parent_directory(cx, path)
    }

    fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool> {
        self.inner.access(cx, path, flags)
    }

    fn path_entry_exists(&self, cx: &Cx, path: &Path) -> Result<bool> {
        self.inner.path_entry_exists(cx, path)
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

#[cfg(unix)]
impl<F: VfsFile> VfsFile for VacuumFaultFile<F> {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        let fault = take_vacuum_fault(&self.state, self.role, VacuumFaultOperation::Close);
        let close_result = self.inner.close(cx);
        if let Some((_, label)) = fault {
            close_result?;
            Err(vacuum_injected_error(label))
        } else {
            close_result
        }
    }

    fn file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.file_identity()
    }

    fn read<'a>(
        &'a self,
        cx: &'a Cx,
        buf: &'a mut [u8],
        offset: u64,
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a {
        self.inner.read(cx, buf, offset)
    }

    fn write<'a>(
        &'a self,
        cx: &'a Cx,
        buf: &'a [u8],
        offset: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        async move {
            match take_silent_replay_write_action(&self.state, self.role) {
                Some(SilentReplayWriteAction::TriggerRollback) => {
                    self.inner.write(cx, buf, offset).await?;
                    return Err(vacuum_injected_error(
                        "trigger-rollback-before-silent-replay-corruption",
                    ));
                }
                Some(SilentReplayWriteAction::CorruptSilently) => {
                    let mut corrupted = buf.to_vec();
                    let byte = corrupted.last_mut().ok_or_else(|| {
                        FrankenError::internal("silent replay corruption received an empty write")
                    })?;
                    // Keep the SQLite header parseable so a fresh pager can reach
                    // hot-journal recovery; corrupt only restored payload bytes.
                    *byte ^= 0x5a;
                    return self.inner.write(cx, &corrupted, offset).await;
                }
                None => {}
            }
            if let Some(parent_cx) =
                take_parent_to_cancel_after_source_write(&self.state, self.role)
            {
                // Apply one complete target-page write first, then cancel the
                // caller and force the publisher down its hot-journal replay path.
                // The replay must use its independently masked cleanup context;
                // returning before the old image is restored would make the test's
                // byte and live-peer assertions fail immediately.
                self.inner.write(cx, buf, offset).await?;
                parent_cx.cancel();
                return Err(vacuum_injected_error(
                    "source-first-write-parent-cancellation",
                ));
            }
            match take_vacuum_fault(&self.state, self.role, VacuumFaultOperation::Write) {
                Some((VacuumFaultEffect::ReturnError, label)) => Err(vacuum_injected_error(label)),
                Some((VacuumFaultEffect::PartialWrite { valid_bytes }, label)) => {
                    let applied = valid_bytes.min(buf.len());
                    if applied > 0 {
                        self.inner.write(cx, &buf[..applied], offset).await?;
                    }
                    Err(vacuum_injected_error(label))
                }
                None => self.inner.write(cx, buf, offset).await,
            }
        }
    }

    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        if let Some((_, label)) =
            take_vacuum_fault(&self.state, self.role, VacuumFaultOperation::Truncate)
        {
            return Err(vacuum_injected_error(label));
        }
        self.inner.truncate(cx, size)
    }

    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.inner.sync(cx, flags)
    }

    fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
        if let Some((_, label)) =
            take_vacuum_fault(&self.state, self.role, VacuumFaultOperation::DurableSync)
        {
            return Err(vacuum_injected_error(label));
        }
        self.inner.durable_sync(cx, kind)
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

    fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_shared_snapshot(cx)
    }

    fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_shared_snapshot_attempt(cx)
    }

    fn lock_external_maintenance(&mut self, cx: &Cx, wal_mode: bool) -> Result<()> {
        self.inner.lock_external_maintenance(cx, wal_mode)
    }

    fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_maintenance_attempt(cx)
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

#[cfg(unix)]
fn journal_path_for(database_path: &Path) -> PathBuf {
    let mut journal_path: OsString = database_path.as_os_str().to_owned();
    journal_path.push("-journal");
    PathBuf::from(journal_path)
}

#[cfg(unix)]
fn create_sqlite_vacuum_fixture(path: &Path, row_count: i64, blob_size: usize, seed: u8) {
    let sqlite = rusqlite::Connection::open(path).expect("create SQLite VACUUM fixture");
    sqlite
        .execute_batch(
            "PRAGMA page_size=4096; \
             PRAGMA journal_mode=DELETE; \
             PRAGMA synchronous=FULL; \
             CREATE TABLE payload(id INTEGER PRIMARY KEY, body BLOB NOT NULL);",
        )
        .expect("create fixture schema");
    let transaction = sqlite
        .unchecked_transaction()
        .expect("begin fixture transaction");
    for id in 0..row_count {
        let fill = seed.wrapping_add(u8::try_from(id).unwrap_or(u8::MAX));
        transaction
            .execute(
                "INSERT INTO payload(id, body) VALUES (?1, ?2);",
                rusqlite::params![id, vec![fill; blob_size]],
            )
            .expect("insert fixture row");
    }
    transaction.commit().expect("commit fixture rows");
    sqlite
        .execute_batch("VACUUM; PRAGMA journal_mode=DELETE;")
        .expect("compact fixture image");
    drop(sqlite);
}

#[cfg(unix)]
fn patch_database_change_counter(path: &Path, change_counter: u32) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open candidate header for provenance patch");
    file.seek(SeekFrom::Start(24))
        .expect("seek candidate change counter");
    file.write_all(&change_counter.to_be_bytes())
        .expect("write candidate change counter");
    file.seek(SeekFrom::Start(92))
        .expect("seek candidate version-valid-for");
    file.write_all(&change_counter.to_be_bytes())
        .expect("write candidate version-valid-for");
    file.sync_all().expect("durably patch candidate header");
}

#[cfg(unix)]
struct VacuumPublicationFixture {
    _directory: tempfile::TempDir,
    source_path: PathBuf,
    candidate_path: PathBuf,
    cx: Cx,
    pager: SimplePager<VacuumFaultVfs<UnixVfs>>,
    source_receipt: DatabaseImageReceipt,
    candidate_receipt: DatabaseImageReceipt,
    source_bytes: Vec<u8>,
    candidate_bytes: Vec<u8>,
}

#[cfg(unix)]
impl VacuumPublicationFixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("VACUUM publication tempdir");
        let source_path = directory.path().join("source.db");
        let candidate_path = directory.path().join("candidate.db");
        create_sqlite_vacuum_fixture(&source_path, VACUUM_SOURCE_ROW_COUNT, 1_900, 0x31);
        create_sqlite_vacuum_fixture(&candidate_path, VACUUM_CANDIDATE_ROW_COUNT, 320, 0xA1);

        let cx = Cx::new();
        let pager = block_on(SimplePager::open_with_cx(
            &cx,
            VacuumFaultVfs::new(UnixVfs::new(), source_path.clone(), candidate_path.clone()),
            &source_path,
            PageSize::DEFAULT,
        ))
        .expect("open real-VFS source pager");
        let source_receipt =
            block_on(pager.capture_vacuum_source_image(&cx)).expect("capture source image receipt");
        let next_change_counter = source_receipt
            .header()
            .change_counter
            .wrapping_add(1)
            .max(1);
        patch_database_change_counter(&candidate_path, next_change_counter);
        let candidate_receipt = block_on(pager.inspect_database_image(&cx, &candidate_path))
            .expect("capture candidate image receipt");
        let source_bytes = std::fs::read(&source_path).expect("read source fixture bytes");
        let candidate_bytes = std::fs::read(&candidate_path).expect("read candidate fixture bytes");
        assert!(
            source_bytes.len() > candidate_bytes.len(),
            "fault matrix requires a shrinking publication: source={} candidate={}",
            source_bytes.len(),
            candidate_bytes.len()
        );
        assert_ne!(
            source_receipt.identity(),
            candidate_receipt.identity(),
            "source and candidate must be distinct filesystem objects"
        );

        Self {
            _directory: directory,
            source_path,
            candidate_path,
            cx,
            pager,
            source_receipt,
            candidate_receipt,
            source_bytes,
            candidate_bytes,
        }
    }

    fn refresh_receipts_after_peer_open(&mut self) {
        self.source_receipt = block_on(self.pager.capture_vacuum_source_image(&self.cx))
            .expect("recapture source after opening live peer");
        let next_change_counter = self
            .source_receipt
            .header()
            .change_counter
            .wrapping_add(1)
            .max(1);
        patch_database_change_counter(&self.candidate_path, next_change_counter);
        self.candidate_receipt = block_on(
            self.pager
                .inspect_database_image(&self.cx, &self.candidate_path),
        )
        .expect("recapture candidate after opening live peer");
        self.source_bytes = std::fs::read(&self.source_path).expect("refresh source fixture bytes");
        self.candidate_bytes =
            std::fs::read(&self.candidate_path).expect("refresh candidate fixture bytes");
    }
}

#[cfg(unix)]
fn assert_image_matches_candidate(
    published: &DatabaseImageReceipt,
    candidate: &DatabaseImageReceipt,
    source_identity: FileIdentity,
) {
    assert_eq!(published.identity(), source_identity);
    assert_eq!(published.file_size(), candidate.file_size());
    assert_eq!(published.header(), candidate.header());
    assert_eq!(published.logical_hash(), candidate.logical_hash());
}

#[cfg(unix)]
fn assert_journal_is_absent_or_non_hot(database_path: &Path, label: &str) {
    let journal_path = journal_path_for(database_path);
    let Ok(bytes) = std::fs::read(&journal_path) else {
        return;
    };
    assert!(
        bytes.len() < 8 || bytes[..8].iter().all(|byte| *byte == 0),
        "{label}: failed publication left a hot rollback journal"
    );
}

#[cfg(unix)]
fn assert_both_engines_integrity(path: &Path, expected_rows: i64, label: &str) {
    let sqlite = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap_or_else(|error| panic!("{label}: stock SQLite open failed: {error}"));
    for pragma in ["quick_check", "integrity_check"] {
        let result: String = sqlite
            .query_row(&format!("PRAGMA {pragma};"), [], |row| row.get(0))
            .unwrap_or_else(|error| panic!("{label}: stock SQLite {pragma} failed: {error}"));
        assert_eq!(result, "ok", "{label}: stock SQLite {pragma}");
    }
    let sqlite_rows: i64 = sqlite
        .query_row("SELECT COUNT(*) FROM payload;", [], |row| row.get(0))
        .unwrap_or_else(|error| panic!("{label}: stock SQLite row count failed: {error}"));
    assert_eq!(sqlite_rows, expected_rows, "{label}: stock SQLite rows");
    drop(sqlite);

    let path_text = path.to_string_lossy();
    let frank = block_on(fsqlite::Connection::open(path_text.as_ref()))
        .unwrap_or_else(|error| panic!("{label}: FrankenSQLite open failed: {error}"));
    for pragma in ["quick_check", "integrity_check"] {
        let rows = block_on(frank.query(&format!("PRAGMA {pragma};")))
            .unwrap_or_else(|error| panic!("{label}: FrankenSQLite {pragma} failed: {error}"));
        assert_eq!(rows.len(), 1, "{label}: FrankenSQLite {pragma} rows");
        assert_eq!(
            rows[0].get(0),
            Some(&fsqlite_types::SqliteValue::Text("ok".into())),
            "{label}: FrankenSQLite {pragma}"
        );
    }
    let rows = block_on(frank.query("SELECT COUNT(*) FROM payload;"))
        .unwrap_or_else(|error| panic!("{label}: FrankenSQLite row count failed: {error}"));
    assert_eq!(rows.len(), 1, "{label}: FrankenSQLite count rows");
    assert_eq!(
        rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Integer(expected_rows)),
        "{label}: FrankenSQLite rows"
    );
}

#[cfg(unix)]
fn assert_bytes_reopen_in_both_engines(
    directory: &Path,
    bytes: &[u8],
    expected_rows: i64,
    label: &str,
) {
    let verification_path = directory.join(format!("{label}-fresh-reopen.db"));
    std::fs::write(&verification_path, bytes).expect("write fresh-open verification image");
    assert_both_engines_integrity(&verification_path, expected_rows, label);
}

#[cfg(unix)]
fn assert_live_franken_integrity(
    connection: &fsqlite::Connection,
    expected_rows: i64,
    label: &str,
) {
    for pragma in ["quick_check", "integrity_check"] {
        let rows = block_on(connection.query(&format!("PRAGMA {pragma};")))
            .unwrap_or_else(|error| panic!("{label}: live peer {pragma} failed: {error}"));
        assert_eq!(rows.len(), 1, "{label}: live peer {pragma} rows");
        assert_eq!(
            rows[0].get(0),
            Some(&fsqlite_types::SqliteValue::Text("ok".into())),
            "{label}: live peer {pragma}"
        );
    }
    let rows = block_on(connection.query("SELECT COUNT(*) FROM payload;"))
        .unwrap_or_else(|error| panic!("{label}: live peer row count failed: {error}"));
    assert_eq!(rows.len(), 1, "{label}: live peer count rows");
    assert_eq!(
        rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Integer(expected_rows)),
        "{label}: live peer observed a partially published image"
    );
}

#[cfg(unix)]
#[test]
fn vacuum_publication_precommit_fault_matrix_restores_exact_source_and_retries() {
    let cases = [
        VacuumFaultSpec::partial_write(
            "journal-partial-record-write",
            VacuumFileRole::Journal,
            2,
            128,
        ),
        VacuumFaultSpec::error(
            "journal-first-durable-sync",
            VacuumFileRole::Journal,
            VacuumFaultOperation::DurableSync,
            1,
        ),
        VacuumFaultSpec::error(
            "journal-parent-directory-sync",
            VacuumFileRole::Journal,
            VacuumFaultOperation::SyncParentDirectory,
            1,
        ),
        VacuumFaultSpec::partial_write("source-partial-page-write", VacuumFileRole::Source, 1, 128),
        VacuumFaultSpec::error(
            "source-truncate",
            VacuumFileRole::Source,
            VacuumFaultOperation::Truncate,
            1,
        ),
        VacuumFaultSpec::error(
            "source-durable-sync",
            VacuumFileRole::Source,
            VacuumFaultOperation::DurableSync,
            1,
        ),
        VacuumFaultSpec::error(
            "journal-invalidation-durable-sync",
            VacuumFileRole::Journal,
            VacuumFaultOperation::DurableSync,
            3,
        ),
    ];

    for spec in cases {
        let fixture = VacuumPublicationFixture::new();
        let vfs = fixture.pager.vfs_handle();
        let source_identity = fixture.source_receipt.identity();
        vfs.arm(spec);

        let publication = block_on(fixture.pager.publish_validated_database_image(
            &fixture.cx,
            &fixture.candidate_path,
            &fixture.source_receipt,
            &fixture.candidate_receipt,
        ));
        let error = match publication {
            Err(error) => error,
            Ok(()) => panic!(
                "pre-commit injected fault must fail publication: spec={spec:?}, snapshot={:?}",
                vfs.snapshot()
            ),
        };
        assert!(
            error.to_string().contains(spec.label),
            "{}: injected diagnostic was lost: {error}",
            spec.label
        );
        let snapshot = vfs.snapshot();
        assert_eq!(snapshot.fired_label, Some(spec.label));
        assert_eq!(snapshot.matching_operations_seen, spec.nth_match);

        let restored_bytes =
            std::fs::read(&fixture.source_path).expect("read source after failed publication");
        assert_eq!(
            restored_bytes, fixture.source_bytes,
            "{}: failed publication changed source bytes",
            spec.label
        );
        assert_eq!(
            block_on(fixture.pager.file_identity(&fixture.cx)).expect("source identity"),
            Some(source_identity),
            "{}: failed publication replaced the source inode",
            spec.label
        );
        let restored_receipt = block_on(fixture.pager.capture_vacuum_source_image(&fixture.cx))
            .expect("recapture restored source image");
        assert!(
            restored_receipt == fixture.source_receipt,
            "{}: restored logical source receipt changed",
            spec.label
        );
        assert_journal_is_absent_or_non_hot(&fixture.source_path, spec.label);
        assert_bytes_reopen_in_both_engines(
            fixture._directory.path(),
            &restored_bytes,
            VACUUM_SOURCE_ROW_COUNT,
            spec.label,
        );

        vfs.disarm();
        block_on(fixture.pager.publish_validated_database_image(
            &fixture.cx,
            &fixture.candidate_path,
            &fixture.source_receipt,
            &fixture.candidate_receipt,
        ))
        .unwrap_or_else(|retry_error| panic!("{}: clean retry failed: {retry_error}", spec.label));
        let published_bytes =
            std::fs::read(&fixture.source_path).expect("read retried publication");
        assert_eq!(
            published_bytes, fixture.candidate_bytes,
            "{}: retry did not publish the exact candidate bytes",
            spec.label
        );
        let published_receipt = block_on(fixture.pager.capture_vacuum_source_image(&fixture.cx))
            .expect("capture retried publication");
        assert_image_matches_candidate(
            &published_receipt,
            &fixture.candidate_receipt,
            source_identity,
        );
        assert_both_engines_integrity(&fixture.source_path, VACUUM_CANDIDATE_ROW_COUNT, spec.label);
    }
}

#[cfg(unix)]
#[test]
fn vacuum_publication_parent_cancellation_after_first_target_write_restores_before_return() {
    const LABEL: &str = "source-first-write-parent-cancellation";

    let mut fixture = VacuumPublicationFixture::new();
    let source_path_text = fixture.source_path.to_string_lossy().into_owned();
    let live_peer = block_on(fsqlite::Connection::open(&source_path_text)).expect("open live peer");
    block_on(live_peer.execute("PRAGMA fsqlite.concurrent_mode=OFF;"))
        .expect("keep cancellation fixture in rollback-journal format");
    assert_live_franken_integrity(&live_peer, VACUUM_SOURCE_ROW_COUNT, LABEL);
    fixture.refresh_receipts_after_peer_open();

    let vfs = fixture.pager.vfs_handle();
    let source_identity = fixture.source_receipt.identity();
    vfs.arm_cancel_after_first_source_write(&fixture.cx);
    let error = block_on(fixture.pager.publish_validated_database_image(
        &fixture.cx,
        &fixture.candidate_path,
        &fixture.source_receipt,
        &fixture.candidate_receipt,
    ))
    .expect_err("cancellation after the first target write must select rollback");
    assert!(
        error.to_string().contains(LABEL),
        "cancellation diagnostic was lost: {error}"
    );
    assert!(
        fixture.cx.is_cancel_requested(),
        "fault hook did not cancel the publication parent context"
    );
    let snapshot = vfs.snapshot();
    assert!(snapshot.cancelled_after_source_write);
    assert_eq!(snapshot.fired_label, None);

    // These assertions run immediately after the synchronous call returns.
    // They prove the publisher did not surface cancellation while replay was
    // still pending in a background task or deferred Drop path.
    assert_eq!(
        std::fs::read(&fixture.source_path).expect("read source after cancellation"),
        fixture.source_bytes,
        "publisher returned before restoring the exact old source image"
    );
    let verification_cx = Cx::new();
    assert_eq!(
        block_on(fixture.pager.file_identity(&verification_cx)).expect("source identity"),
        Some(source_identity),
        "cancelled publication replaced the source inode"
    );
    let restored_receipt = block_on(fixture.pager.capture_vacuum_source_image(&verification_cx))
        .expect("maintenance locks must be released despite parent cancellation");
    assert!(
        restored_receipt == fixture.source_receipt,
        "cancelled publication did not restore the source receipt"
    );
    assert_journal_is_absent_or_non_hot(&fixture.source_path, LABEL);
    assert_live_franken_integrity(&live_peer, VACUUM_SOURCE_ROW_COUNT, LABEL);
}

#[cfg(unix)]
#[test]
fn vacuum_silent_replay_corruption_stays_hot_and_recovers_on_fresh_open() {
    const LABEL: &str = "silent-replay-page-corruption";
    let fixture = VacuumPublicationFixture::new();
    let original_receipt = fixture.source_receipt.clone();
    let fault_vfs = fixture.pager.vfs_handle();
    fault_vfs.arm_silent_replay_corruption();

    let error = block_on(fixture.pager.publish_validated_database_image(
        &fixture.cx,
        &fixture.candidate_path,
        &fixture.source_receipt,
        &fixture.candidate_receipt,
    ))
    .expect_err("silent replay corruption must fail exact rollback verification");
    assert!(
        error
            .to_string()
            .contains("rollback recovery verification mismatch"),
        "exact replay-verification diagnostic was lost: {error}"
    );
    assert_eq!(fault_vfs.snapshot().fired_label, Some(LABEL));

    let journal_bytes = std::fs::read(journal_path_for(&fixture.source_path))
        .expect("failed replay verification must retain its hot journal");
    assert!(
        journal_bytes.len() >= 8 && journal_bytes[..8].iter().any(|byte| *byte != 0),
        "failed replay verification invalidated the only recovery record"
    );
    assert!(
        block_on(fixture.pager.capture_vacuum_source_image(&fixture.cx)).is_err(),
        "a pager with unverified recovery must remain fail-closed"
    );

    fault_vfs.disarm();
    let recovery_vfs = UnixVfs::new();
    let VacuumPublicationFixture {
        _directory,
        source_path,
        candidate_path: _,
        cx,
        pager,
        source_receipt: _,
        candidate_receipt: _,
        source_bytes,
        candidate_bytes: _,
    } = fixture;
    drop(fault_vfs);
    drop(pager);

    let recovered = block_on(SimplePager::open_with_cx(
        &cx,
        recovery_vfs,
        &source_path,
        PageSize::DEFAULT,
    ))
    .expect("fresh open must retry and verify the retained hot journal");
    assert_eq!(
        std::fs::read(&source_path).expect("read freshly recovered source"),
        source_bytes,
        "fresh recovery did not restore the exact pre-publication bytes"
    );
    let recovered_receipt = block_on(recovered.capture_vacuum_source_image(&cx))
        .expect("freshly recovered pager must leave recovery clean");
    assert!(
        recovered_receipt == original_receipt,
        "fresh recovery changed the source image receipt"
    );
    assert_journal_is_absent_or_non_hot(&source_path, LABEL);
    drop(recovered);
    assert_both_engines_integrity(&source_path, VACUUM_SOURCE_ROW_COUNT, LABEL);
    drop(_directory);
}

#[cfg(unix)]
#[test]
fn rollback_journal_failed_commit_recovers_before_return() {
    const LABEL: &str = "ordinary-commit-source-partial-write";

    let mut fixture = VacuumPublicationFixture::new();
    let source_path_text = fixture.source_path.to_string_lossy().into_owned();
    let live_peer = block_on(fsqlite::Connection::open(&source_path_text)).expect("open live peer");
    block_on(live_peer.execute("PRAGMA fsqlite.concurrent_mode=OFF;"))
        .expect("keep failed-commit fixture in rollback-journal format");
    assert_live_franken_integrity(&live_peer, VACUUM_SOURCE_ROW_COUNT, LABEL);
    fixture.refresh_receipts_after_peer_open();

    let page_size = PageSize::DEFAULT.as_usize();
    let original_database = fixture.source_bytes.clone();
    let original_page_one = original_database[..page_size].to_vec();
    let mut failed_page_one = original_page_one.clone();
    failed_page_one[100] ^= 0xFF;

    let vfs = fixture.pager.vfs_handle();
    let spec = VacuumFaultSpec::partial_write(LABEL, VacuumFileRole::Source, 1, 128);
    let mut transaction = block_on(fixture.pager.begin(&fixture.cx, TransactionMode::Immediate))
        .expect("begin ordinary rollback-journal transaction");
    block_on(transaction.write_page(&fixture.cx, PageNumber::ONE, &failed_page_one))
        .expect("stage page-one overwrite");
    vfs.arm(spec);
    let error = block_on(transaction.commit(&fixture.cx))
        .expect_err("partial target write must fail ordinary commit");
    assert!(
        error.to_string().contains(LABEL),
        "fault diagnostic was lost: {error}"
    );
    let snapshot = vfs.snapshot();
    assert_eq!(snapshot.fired_label, Some(LABEL));
    assert_eq!(snapshot.matching_operations_seen, 1);

    // The commit made its journal durable and partially touched the database,
    // but its async failure path must complete the recovery epoch before
    // resolving. No later Drop or reopen may be required for atomicity.
    assert_eq!(
        std::fs::read(&fixture.source_path).expect("read commit-recovered database"),
        original_database,
        "failed commit returned before restoring the exact pre-commit database"
    );
    assert_journal_is_absent_or_non_hot(&fixture.source_path, LABEL);

    drop(transaction);

    // Dropping the already-recovered failed transaction must release its
    // snapshot and writer baton without changing the restored image.
    let verification_cx = Cx::new();
    let mut reader = block_on(
        fixture
            .pager
            .begin(&verification_cx, TransactionMode::ReadOnly),
    )
    .expect("failed-commit recovery must clear pending state for the live pager");
    assert_eq!(
        block_on(reader.get_page(&verification_cx, PageNumber::ONE))
            .expect("read recovered page one")
            .as_ref(),
        original_page_one.as_slice(),
        "live pager retained a partially written page after Drop recovery"
    );
    block_on(reader.commit(&verification_cx)).expect("finish recovery verification reader");
    assert_live_franken_integrity(&live_peer, VACUUM_SOURCE_ROW_COUNT, LABEL);

    // A full clean journal commit on the same live pager proves recovery did
    // not merely make reads work while leaving writer state poisoned.
    vfs.disarm();
    let mut retry = block_on(
        fixture
            .pager
            .begin(&verification_cx, TransactionMode::Immediate),
    )
    .expect("begin clean commit after failed-commit recovery");
    block_on(retry.write_page(&verification_cx, PageNumber::ONE, &original_page_one))
        .expect("stage clean retry page");
    block_on(retry.commit(&verification_cx)).expect("clean commit after failed-commit recovery");
    assert_journal_is_absent_or_non_hot(&fixture.source_path, LABEL);
    assert_both_engines_integrity(&fixture.source_path, VACUUM_SOURCE_ROW_COUNT, LABEL);
    assert_live_franken_integrity(&live_peer, VACUUM_SOURCE_ROW_COUNT, LABEL);
}

#[cfg(unix)]
#[test]
fn vacuum_publication_postcommit_cleanup_faults_preserve_committed_candidate_and_retry() {
    let cases = [
        VacuumFaultSpec::error(
            "journal-postcommit-truncate",
            VacuumFileRole::Journal,
            VacuumFaultOperation::Truncate,
            3,
        ),
        VacuumFaultSpec::error(
            "journal-postcommit-durable-sync",
            VacuumFileRole::Journal,
            VacuumFaultOperation::DurableSync,
            4,
        ),
        VacuumFaultSpec::error(
            "journal-postcommit-delete",
            VacuumFileRole::Journal,
            VacuumFaultOperation::Delete,
            1,
        ),
        VacuumFaultSpec::error(
            "candidate-postcommit-close",
            VacuumFileRole::Candidate,
            VacuumFaultOperation::Close,
            1,
        ),
    ];

    for spec in cases {
        let fixture = VacuumPublicationFixture::new();
        let vfs = fixture.pager.vfs_handle();
        let source_identity = fixture.source_receipt.identity();
        vfs.arm(spec);

        block_on(fixture.pager.publish_validated_database_image(
            &fixture.cx,
            &fixture.candidate_path,
            &fixture.source_receipt,
            &fixture.candidate_receipt,
        ))
        .unwrap_or_else(|error| {
            panic!(
                "{}: post-commit cleanup fault changed success into error: {error}",
                spec.label
            )
        });
        let snapshot = vfs.snapshot();
        assert_eq!(snapshot.fired_label, Some(spec.label));
        assert_eq!(snapshot.matching_operations_seen, spec.nth_match);
        assert_eq!(
            std::fs::read(&fixture.source_path).expect("read committed target"),
            fixture.candidate_bytes,
            "{}: cleanup fault changed committed candidate bytes",
            spec.label
        );
        assert_eq!(
            block_on(fixture.pager.file_identity(&fixture.cx)).expect("source identity"),
            Some(source_identity),
            "{}: publication replaced the source inode",
            spec.label
        );
        assert_journal_is_absent_or_non_hot(&fixture.source_path, spec.label);
        let first_published_receipt =
            block_on(fixture.pager.capture_vacuum_source_image(&fixture.cx))
                .expect("capture committed candidate after cleanup fault");
        assert_image_matches_candidate(
            &first_published_receipt,
            &fixture.candidate_receipt,
            source_identity,
        );

        // A second in-place publication proves that any non-hot leftover was
        // accepted and cleaned, and that post-commit errors did not poison the
        // live pager's maintenance state.
        vfs.disarm();
        let second_source = block_on(fixture.pager.capture_vacuum_source_image(&fixture.cx))
            .expect("capture source for second publication");
        let next_change_counter = second_source.header().change_counter.wrapping_add(1).max(1);
        patch_database_change_counter(&fixture.candidate_path, next_change_counter);
        let second_candidate = block_on(
            fixture
                .pager
                .inspect_database_image(&fixture.cx, &fixture.candidate_path),
        )
        .expect("inspect second candidate");
        block_on(fixture.pager.publish_validated_database_image(
            &fixture.cx,
            &fixture.candidate_path,
            &second_source,
            &second_candidate,
        ))
        .unwrap_or_else(|error| panic!("{}: second publication failed: {error}", spec.label));
        let second_candidate_bytes =
            std::fs::read(&fixture.candidate_path).expect("read second candidate");
        assert_eq!(
            std::fs::read(&fixture.source_path).expect("read second publication"),
            second_candidate_bytes,
            "{}: second publication did not exactly match candidate",
            spec.label
        );
        let second_published_receipt =
            block_on(fixture.pager.capture_vacuum_source_image(&fixture.cx))
                .expect("capture second published image");
        assert_image_matches_candidate(
            &second_published_receipt,
            &second_candidate,
            source_identity,
        );
        assert_both_engines_integrity(&fixture.source_path, VACUUM_CANDIDATE_ROW_COUNT, spec.label);
    }
}
