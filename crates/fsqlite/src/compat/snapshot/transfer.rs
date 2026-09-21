//! Capture an open SQL database for authenticated snapshot transfer.
//!
//! The source is captured by `Connection::export_bytes`, not by reading a live
//! main file and WAL independently. Export owns its existing pager quiescence,
//! checkpoint and read-fence protocol. Once capture returns, packet generation
//! owns immutable encoded bytes and no source connection or database locks.
//! This is a whole-image bootstrap, not an incremental replication log.

use std::fmt;

use fsqlite_core::replication_sender::{PageEntry, ReplicationPacket};
pub use fsqlite_core::replication_sender::SenderConfig;
use fsqlite_core::snapshot_shipping::SnapshotSender;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_vfs::VfsFile;

use super::{
    BootstrapOptions, BootstrapProgress, Destination, SnapshotCheckpoint, SnapshotImageState,
    SnapshotManifest, SnapshotOpen, SnapshotPacketResult, SnapshotSpool, SnapshotSpoolState,
};
use crate::{Connection, ConnectionEnv};

/// Admission policy for the captured image and its packet coding schedule.
///
/// `max_image_bytes` is an image-admission limit, not an allocation/RSS cap:
/// the existing exporter materializes its image before this limit is checked.
/// Encoding subsequently holds page copies and encoded blocks; the codec has
/// its own workspace limit. Size the exporter/environment for the source too.
#[derive(Debug, Clone)]
pub struct CaptureOptions {
    pub max_image_bytes: usize,
    pub coding: SenderConfig,
}

impl Default for CaptureOptions {
    fn default() -> Self {
        Self {
            max_image_bytes: 64 * 1024 * 1024,
            coding: SenderConfig::default(),
        }
    }
}

impl CaptureOptions {
    fn preflight(&self) -> Result<()> {
        if self.max_image_bytes < 512 {
            return Err(FrankenError::TooBig);
        }
        ReplicationPacket::validate_symbol_size(usize::from(self.coding.symbol_size))?;
        if self.coding.max_isi_multiplier == 0 {
            return Err(FrankenError::OutOfRange {
                what: "max_isi_multiplier".to_owned(),
                value: "0".to_owned(),
            });
        }
        Ok(())
    }
}

/// Immutable source content and its fixed, authenticated packet schedule.
///
/// Supply `manifest_id()` to the receiver through a trusted control plane.
/// Sending a digest beside an untrusted manifest does not establish trust.
/// Packet authentication is mandatory here, but it does not encrypt database
/// contents: use a confidential transport where the data requires one.
#[must_use]
pub struct CapturedSnapshot {
    sender: SnapshotSender,
    manifest: SnapshotManifest,
    manifest_id: [u8; 32],
    image_bytes: u64,
    auth_key: [u8; 32],
}

impl fmt::Debug for CapturedSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapturedSnapshot")
            .field("manifest_id", &self.manifest_id)
            .field("image_bytes", &self.image_bytes)
            .field("blocks", &self.manifest.blocks().len())
            .finish_non_exhaustive()
    }
}

fn checkpoint(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)
}

fn corrupt(detail: &'static str) -> FrankenError {
    FrankenError::DatabaseCorrupt { detail: detail.to_owned() }
}

/// Check the self-contained image envelope before allocating page copies.
/// Like the image installer, respect SQLite's legacy page-count authority rule.
/// This does not certify the B-trees, application schema, or source provenance.
fn image_geometry(image: &[u8], limit: usize) -> Result<(u32, u32)> {
    if image.len() > limit {
        return Err(FrankenError::TooBig);
    }
    if image.len() < 100 || &image[..16] != b"SQLite format 3\0" {
        return Err(corrupt("snapshot source has no SQLite database header"));
    }
    let encoded = u16::from_be_bytes([image[16], image[17]]);
    let page_size = if encoded == 1 { 65_536 } else { u32::from(encoded) };
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two()
        || !matches!((image[18], image[19]), (1, 1) | (2, 2))
        || image[21..24] != [64, 32, 32]
    {
        return Err(corrupt("snapshot source has invalid geometry or journal format"));
    }
    let size = usize::try_from(page_size).map_err(|_| FrankenError::TooBig)?;
    if image.len() < size || !image.len().is_multiple_of(size) {
        return Err(corrupt("snapshot source contains an incomplete database page"));
    }
    let page_count = u32::try_from(image.len() / size).map_err(|_| FrankenError::TooBig)?;
    if page_count == u32::MAX {
        return Err(FrankenError::TooBig);
    }
    let declared = u32::from_be_bytes(image[28..32].try_into().expect("header width"));
    if declared != 0 && image[24..28] == image[92..96] && declared != page_count {
        return Err(corrupt("snapshot source header and physical page counts disagree"));
    }
    Ok((page_size, page_count))
}

impl CapturedSnapshot {
    /// Capture `main` through the existing public export boundary.
    ///
    /// The exclusive borrow prevents SQL re-entry through this connection
    /// while capture is pending. Export settles abandoned transaction cleanup,
    /// refuses an active transaction, and may flush/checkpoint committed data.
    /// Contention and export failures propagate; no retry or implicit COMMIT
    /// of a caller's explicit transaction is introduced. TEMP and attachments
    /// are not included, and no database is opened by pathname here.
    ///
    /// Export uses the connection's configured context. `cx` controls admission
    /// and packetization; configure related lineages when both must share
    /// cancellation. Cancellation after export cannot undo its checkpoint.
    /// The image limit is checked *after* export; see `CaptureOptions`.
    pub async fn capture(
        cx: &Cx,
        connection: &mut Connection,
        auth_key: [u8; 32],
        options: CaptureOptions,
    ) -> Result<Self> {
        checkpoint(cx)?;
        options.preflight()?;
        let image = connection.export_bytes().await?;
        Self::from_image(cx, image, auth_key, options)
    }

    /// Packetize an already coherent, exclusively owned SQLite image.
    ///
    /// The caller is responsible for atomic source capture. Never obtain this
    /// input by copying a live main file without its pager snapshot/fence.
    /// Ownership prevents later caller mutation from changing the manifest or
    /// packets. Structural admission is not an integrity-check certificate.
    pub fn from_image(
        cx: &Cx,
        image: Vec<u8>,
        auth_key: [u8; 32],
        options: CaptureOptions,
    ) -> Result<Self> {
        checkpoint(cx)?;
        options.preflight()?;
        let (page_size, page_count) = image_geometry(&image, options.max_image_bytes)?;
        let image_bytes = u64::try_from(image.len()).map_err(|_| FrankenError::TooBig)?;
        let mut pages = Vec::new();
        pages.try_reserve_exact(usize::try_from(page_count).map_err(|_| FrankenError::TooBig)?)
            .map_err(|_| FrankenError::OutOfMemory)?;
        for (index, data) in image.chunks_exact(page_size as usize).enumerate() {
            checkpoint(cx)?;
            let number = u32::try_from(index + 1).map_err(|_| FrankenError::TooBig)?;
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(data.len()).map_err(|_| FrankenError::OutOfMemory)?;
            bytes.extend_from_slice(data);
            pages.push(PageEntry::new(number, bytes));
        }
        // Do not retain a third database-sized copy during encoding.
        drop(image);
        let sender = SnapshotSender::prepare(page_size, &mut pages, options.coding)?;
        drop(pages);
        checkpoint(cx)?;
        let manifest = sender.manifest()?;
        let manifest_id = manifest.id();
        checkpoint(cx)?;
        Ok(Self { sender, manifest, manifest_id, image_bytes, auth_key })
    }

    #[must_use]
    pub const fn manifest_id(&self) -> [u8; 32] { self.manifest_id }

    #[must_use]
    pub const fn manifest(&self) -> &SnapshotManifest { &self.manifest }

    #[must_use]
    pub const fn image_bytes(&self) -> u64 { self.image_bytes }

    /// Produce one owned, keyed V2 packet. Retain it until its send attempt is
    /// resolved; retransmitting the same packet is safe for the receiver.
    /// `None` ends this pass, not a receiver/durability acknowledgement.
    pub fn next_packet(&mut self, cx: &Cx) -> Result<Option<ReplicationPacket>> {
        checkpoint(cx)?;
        let Some(mut packet) = self.sender.next_packet(cx)? else { return Ok(None); };
        packet.attach_auth_tag(&self.auth_key);
        Ok(Some(packet))
    }

    /// Start a fresh pass over the same captured content and identity, without
    /// reopening or recapturing a source that may since have changed.
    pub fn restart(&mut self) { self.sender.restart(); }
}

/// A journal checkpoint and the independently verified SQL installation.
///
/// The checkpoint covers authenticated packet bytes, not SQL-open success.
/// Inspect `installed.connection` separately; an open error does not undo
/// either durable artifact. Journal namespace/directory durability remains
/// the caller's responsibility, as with [`SnapshotSpool::checkpoint`].
#[derive(Debug)]
#[must_use = "inspect the journal checkpoint, image receipt and SQL-open result"]
pub struct JournaledSnapshotOpen {
    pub checkpoint: SnapshotCheckpoint,
    pub installed: SnapshotOpen,
}

/// Receive an authenticated snapshot into both a packet journal and a SQL image.
///
/// Unlike the volatile bootstrap, every newly accepted packet is journaled
/// before its decoded pages are written. Completed blocks are drained after
/// each packet, so the image is not retained in memory. Call `checkpoint` to
/// acknowledge a recoverable prefix, and retain that receipt in trusted state.
/// Neither a received packet nor a decoded block is a durability receipt.
///
/// The borrowed spool is exclusively owned for this attempt. Its file must
/// live OUTSIDE the private bootstrap directory and must not alias a database
/// or any of its sidecars. The caller owns journal namespace exclusion,
/// directory durability and quiescence of abandoned I/O before reopening.
/// Its preconfigured payload/journal limits apply; the bootstrap options'
/// file limit governs the output image, not the journal or decoder memory.
/// Failed staging files are preserved, never deleted or reused in place.
pub struct JournaledSnapshotBootstrap<'a, F: VfsFile> {
    destination: Destination,
    spool: &'a mut SnapshotSpool<F>,
    write_in_flight: bool,
}

impl<F: VfsFile> fmt::Debug for JournaledSnapshotBootstrap<'_, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournaledSnapshotBootstrap")
            .field("progress", &self.progress())
            .field("journal_records", &self.spool.record_count())
            .finish_non_exhaustive()
    }
}

impl<'a, F: VfsFile> JournaledSnapshotBootstrap<'a, F> {
    /// Bind a newly created, empty spool to a fresh guarded image destination.
    ///
    /// The spool already carries a trusted manifest and packet authentication
    /// key. A nonfresh spool is refused before creating any output artifacts;
    /// previously decoded pages must never disappear through a second owner.
    pub fn begin(
        cx: &Cx,
        options: &BootstrapOptions,
        spool: &'a mut SnapshotSpool<F>,
        expected_id: [u8; 32],
    ) -> Result<Self> {
        checkpoint(cx)?;
        Self::require_fresh(spool, SnapshotSpoolState::Ready)?;
        let destination = Destination::create(
            cx, options, spool.receiver().manifest().clone(), expected_id,
        )?;
        Ok(Self { destination, spool, write_in_flight: false })
    }

    fn require_fresh(spool: &SnapshotSpool<F>, state: SnapshotSpoolState) -> Result<()> {
        if spool.state() != state || spool.record_count() != 0
            || spool.receiver().blocks_decoded() != 0
            || spool.receiver().retained_payload_bytes() != 0
        {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }

    #[must_use]
    pub fn progress(&self) -> BootstrapProgress {
        BootstrapProgress {
            blocks_decoded: self.spool.receiver().blocks_decoded(),
            blocks_written: self.destination.image.blocks_applied(),
            blocks_total: self.spool.receiver().manifest().blocks().len(),
            retained_payload_bytes: self.spool.receiver().retained_payload_bytes(),
            poisoned: self.write_in_flight
                || self.spool.state() == SnapshotSpoolState::Poisoned
                || self.destination.image.state() == SnapshotImageState::Poisoned,
        }
    }

    /// Private staging pathname; never open it before successful finalization.
    #[must_use]
    pub fn database_path(&self) -> &std::path::Path { &self.destination.path }

    async fn drain_blocks(&mut self, cx: &Cx) -> Result<()> {
        // Arm BEFORE taking output: cancellation at apply_block's entry must
        // not discard a decoded block while leaving finalization admissible.
        self.write_in_flight = true;
        for block in self.spool.take_decoded_blocks() {
            self.destination.image.apply_block(cx, &block).await?;
        }
        self.write_in_flight = false;
        Ok(())
    }

    /// Append an admitted packet before applying any newly decoded pages.
    ///
    /// Rejected/duplicate packets do not grow the journal. Preflight admission
    /// errors leave the owner retryable; failed or abandoned journal/image
    /// writes poison it. Restart from the journal rather than retrying an
    /// in-doubt image write. No durability acknowledgement is implicit here.
    pub async fn receive_packet(
        &mut self, cx: &Cx, packet: &ReplicationPacket,
    ) -> Result<SnapshotPacketResult> {
        if self.progress().poisoned { return Err(FrankenError::BusyRecovery); }
        let result = self.spool.append(cx, packet).await?;
        self.drain_blocks(cx).await?;
        Ok(result)
    }

    /// Sync the saved packet prefix, without claiming the image is installed.
    pub fn checkpoint(&mut self, cx: &Cx) -> Result<SnapshotCheckpoint> {
        if self.progress().poisoned { return Err(FrankenError::BusyRecovery); }
        self.spool.checkpoint(cx)
    }

    pub async fn finish_and_open(self, cx: &Cx) -> Result<JournaledSnapshotOpen> {
        self.finish_and_open_with_env(cx, ConnectionEnv::default()).await
    }

    /// Sync the complete journal before image verification/publication/SQL open.
    ///
    /// Uses the existing guarded install path and preserves its separate
    /// image/open result. A failed or abandoned finalizer can leave durable
    /// bytes without a returned receipt; it never deletes either artifact.
    pub async fn finish_and_open_with_env(
        self, cx: &Cx, env: ConnectionEnv,
    ) -> Result<JournaledSnapshotOpen> {
        let progress = self.progress();
        if progress.poisoned { return Err(FrankenError::BusyRecovery); }
        if !progress.ready_to_finish() { return Err(FrankenError::Busy); }
        let saved = self.spool.checkpoint(cx)?;
        let installed = self.destination.finish_and_open(cx, env).await?;
        Ok(JournaledSnapshotOpen { checkpoint: saved, installed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{BootstrapOptions, SnapshotBootstrap};
    use crate::SqliteValue;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::unix::{UnixFile, UnixVfs};
    use fsqlite_vfs::{Vfs, host_fs};

    const KEY: [u8; 32] = [0x6B; 32];

    fn with_runtime<F: std::future::Future>(future: F) -> F::Output {
        asupersync::runtime::RuntimeBuilder::current_thread()
            .blocking_threads(1, 2).build().unwrap().block_on(future)
    }

    fn context() -> Cx {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        cx
    }

    fn image(page_size: u32) -> Vec<u8> {
        let root = tempfile::tempdir().unwrap().keep();
        let path = root.join("source.db");
        let stock = rusqlite::Connection::open(&path).unwrap();
        stock.execute_batch(&format!(
            "PRAGMA page_size={page_size}; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); \
             INSERT INTO t VALUES(1,'before');"
        )).unwrap();
        stock.close().unwrap();
        fsqlite_vfs::host_fs::read(&path).unwrap()
    }

    async fn install(cx: &Cx, source: &mut CapturedSnapshot) -> Connection {
        let root = tempfile::tempdir().unwrap().keep();
        let options = BootstrapOptions::new(root.join("replica"));
        let mut destination = SnapshotBootstrap::begin(
            cx, &options, source.manifest().clone(), source.manifest_id(), KEY,
        ).unwrap();
        while let Some(packet) = source.next_packet(cx).unwrap() {
            assert!(packet.verify_integrity(Some(&KEY)));
            destination.receive_packet(cx, &packet).await.unwrap();
        }
        destination.finish_and_open(cx).await.unwrap().connection.unwrap()
    }

    #[test]
    fn live_capture_releases_source_and_installs_only_captured_rows() {
        with_runtime(async {
            let cx = context();
            let root = tempfile::tempdir().unwrap().keep();
            let path = root.join("live.db");
            let mut source = Connection::open(path.to_str().unwrap()).await.unwrap();
            source.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE); \
                CREATE VIEW names AS SELECT v FROM t; \
                BEGIN; INSERT INTO t VALUES(1,'before'); COMMIT;").await.unwrap();
            let mut captured = CapturedSnapshot::capture(
                &cx, &mut source, KEY, CaptureOptions::default(),
            ).await.unwrap();
            let id = captured.manifest_id();
            // A peer can write before delivery; it cannot change captured bytes.
            let peer = Connection::open(path.to_str().unwrap()).await.unwrap();
            peer.execute("BEGIN; UPDATE t SET v='after'; INSERT INTO t VALUES(2,'later'); COMMIT;")
                .await.unwrap();
            peer.close().await.unwrap();
            source.close().await.unwrap();
            let replica = install(&cx, &mut captured).await;
            assert_eq!(replica.query_row("SELECT v FROM names").await.unwrap().get(0),
                Some(&SqliteValue::Text("before".into())));
            assert_eq!(captured.manifest_id(), id);
            replica.execute("BEGIN; INSERT INTO t VALUES(3,'replica-write'); COMMIT;").await.unwrap();
            assert_eq!(replica.query_row("PRAGMA integrity_check").await.unwrap().get(0),
                Some(&SqliteValue::Text("ok".into())));
            replica.close().await.unwrap();
        });
    }

    #[test]
    fn capture_refuses_active_transaction_without_committing_its_writes() {
        with_runtime(async {
            let cx = context();
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY); BEGIN; INSERT INTO t VALUES(1);")
                .await.unwrap();
            assert!(CapturedSnapshot::capture(&cx, &mut conn, KEY, CaptureOptions::default())
                .await.is_err());
            assert!(conn.in_transaction());
            conn.execute("ROLLBACK;").await.unwrap();
            let mut captured = CapturedSnapshot::capture(&cx, &mut conn, KEY, CaptureOptions::default())
                .await.unwrap();
            let replica = install(&cx, &mut captured).await;
            assert_eq!(replica.query_row("SELECT count(*) FROM t").await.unwrap().get(0),
                Some(&SqliteValue::Integer(0)));
            replica.close().await.unwrap();
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn source_restart_repeats_authenticated_packets_and_fixed_identity() {
        let cx = Cx::new();
        let mut captured = CapturedSnapshot::from_image(
            &cx, image(512), KEY, CaptureOptions {
                coding: SenderConfig { symbol_size: 256, max_isi_multiplier: 1 },
                ..CaptureOptions::default()
            },
        ).unwrap();
        let id = captured.manifest_id();
        let mut first = Vec::new();
        while let Some(packet) = captured.next_packet(&cx).unwrap() {
            assert!(packet.verify_integrity(Some(&KEY)));
            assert!(!packet.verify_integrity(Some(&[0x77; 32])));
            first.push(packet.to_bytes().unwrap());
        }
        captured.restart();
        for bytes in first {
            assert_eq!(captured.next_packet(&cx).unwrap().unwrap().to_bytes().unwrap(), bytes);
        }
        assert!(captured.next_packet(&cx).unwrap().is_none());
        assert_eq!(captured.manifest_id(), id);
        assert!(!format!("{captured:?}").contains("auth_key"));
    }

    #[test]
    fn image_admission_handles_page_sizes_and_legacy_page_count_authority() {
        for page_size in [512, 8192, 65_536] {
            let original = image(page_size);
            let expected = (page_size, u32::try_from(original.len() / page_size as usize).unwrap());
            assert_eq!(image_geometry(&original, original.len()).unwrap(), expected);
            let mut legacy = original.clone();
            legacy[28..32].copy_from_slice(&0_u32.to_be_bytes());
            assert_eq!(image_geometry(&legacy, legacy.len()).unwrap(), expected);
            legacy[28..32].copy_from_slice(&u32::MAX.to_be_bytes());
            legacy[92..96].copy_from_slice(&[0xFF; 4]);
            assert_eq!(image_geometry(&legacy, legacy.len()).unwrap(), expected);
            let changed_counter: [u8; 4] = legacy[24..28].try_into().unwrap();
            legacy[92..96].copy_from_slice(&changed_counter);
            assert!(image_geometry(&legacy, legacy.len()).is_err());
            assert!(matches!(image_geometry(&original, original.len() - 1), Err(FrankenError::TooBig)));
        }
    }

    #[test]
    fn source_rejects_truncated_malformed_and_cancelled_inputs() {
        let cx = Cx::new();
        let bytes = image(512);
        for length in [0, 15, 99, 100, 511, bytes.len() - 1] {
            assert!(CapturedSnapshot::from_image(
                &cx, bytes[..length].to_vec(), KEY, CaptureOptions::default(),
            ).is_err());
        }
        for (offset, value) in [(0, b'X'), (16, 3), (18, 0), (19, 2), (21, 0)] {
            let mut invalid = bytes.clone();
            invalid[offset] = value;
            assert!(CapturedSnapshot::from_image(&cx, invalid, KEY, CaptureOptions::default()).is_err());
        }
        cx.cancel();
        assert!(matches!(CapturedSnapshot::from_image(
            &cx, bytes, KEY, CaptureOptions::default(),
        ), Err(FrankenError::Interrupt)));
    }

    async fn create_journal(
        cx: &Cx,
        path: &std::path::Path,
        manifest: &SnapshotManifest,
        limit: u64,
    ) -> SnapshotSpool<UnixFile> {
        let vfs = UnixVfs::new();
        let (file, _) = vfs.open(cx, Some(path),
            VfsOpenFlags::CREATE | VfsOpenFlags::EXCLUSIVE | VfsOpenFlags::READWRITE,
        ).unwrap();
        let receiver = super::super::ManifestSnapshotReceiver::new(
            manifest.clone(), manifest.id(), KEY, 1 << 20,
        ).unwrap();
        let spool = SnapshotSpool::create(cx, file, receiver, limit).await.unwrap();
        vfs.sync_parent_directory(cx, path).unwrap();
        spool
    }

    async fn reopen_journal(
        cx: &Cx,
        path: &std::path::Path,
        manifest: &SnapshotManifest,
        saved: SnapshotCheckpoint,
        writable: bool,
    ) -> SnapshotSpool<UnixFile> {
        let flags = if writable { VfsOpenFlags::READWRITE } else { VfsOpenFlags::READONLY };
        let (file, _) = UnixVfs::new().open(cx, Some(path), flags).unwrap();
        let receiver = super::super::ManifestSnapshotReceiver::new(
            manifest.clone(), manifest.id(), KEY, 1 << 20,
        ).unwrap();
        SnapshotSpool::open(cx, file, receiver, 1 << 20, Some(saved)).await.unwrap()
    }

    fn captured_image(cx: &Cx) -> CapturedSnapshot {
        CapturedSnapshot::from_image(cx, image(512), KEY, CaptureOptions {
            coding: SenderConfig { symbol_size: 256, max_isi_multiplier: 1 },
            ..CaptureOptions::default()
        }).unwrap()
    }

    #[test]
    fn journaled_live_bootstrap_checkpoints_and_recovers_sql_image() {
        with_runtime(async {
            let cx = context();
            let root = tempfile::tempdir().unwrap().keep();
            let mut source = captured_image(&cx);
            let manifest = source.manifest().clone();
            let path = root.join("transfer.spool");
            let mut spool = create_journal(&cx, &path, &manifest, 1 << 20).await;
            let options = BootstrapOptions::new(root.join("replica"));
            let mut bootstrap = JournaledSnapshotBootstrap::begin(
                &cx, &options, &mut spool, manifest.id(),
            ).unwrap();
            assert_eq!(bootstrap.checkpoint(&cx).unwrap().record_count, 0);
            let first = source.next_packet(&cx).unwrap().unwrap();
            assert!(first.k_source > 1);
            bootstrap.receive_packet(&cx, &first).await.unwrap();
            let partial = bootstrap.checkpoint(&cx).unwrap();
            assert_eq!(partial.record_count, 1);
            assert!(!bootstrap.progress().ready_to_finish());
            while let Some(packet) = source.next_packet(&cx).unwrap() {
                bootstrap.receive_packet(&cx, &packet).await.unwrap();
            }
            assert!(bootstrap.progress().ready_to_finish());
            assert_eq!(bootstrap.progress().retained_payload_bytes, 0);
            let result = bootstrap.finish_and_open(&cx).await.unwrap();
            assert!(result.checkpoint.record_count > partial.record_count);
            assert_eq!(result.installed.image.byte_len, source.image_bytes());
            let conn = result.installed.connection.unwrap();
            assert_eq!(conn.query_row("SELECT v FROM t").await.unwrap().get(0),
                Some(&SqliteValue::Text("before".into())));
            conn.execute("BEGIN; INSERT INTO t VALUES(2,'replica-only'); COMMIT;").await.unwrap();
            conn.close().await.unwrap();
            spool.into_file().close(&cx).unwrap();

            // A reopened owner can use the acknowledged packet journal alone.
            // Recovery must not pick up writes made later on the first replica.
            let original = host_fs::read(&path).unwrap();
            let mut reopened = reopen_journal(&cx, &path, &manifest, result.checkpoint, false).await;
            let restored = super::super::restore_spool_and_open(
                &cx, &BootstrapOptions::new(root.join("restored")), &mut reopened, manifest.id(),
            ).await.unwrap();
            let conn = restored.connection.unwrap();
            assert_eq!(conn.query_row("SELECT count(*) FROM t").await.unwrap().get(0),
                Some(&SqliteValue::Integer(1)));
            assert_eq!(conn.query_row("PRAGMA integrity_check").await.unwrap().get(0),
                Some(&SqliteValue::Text("ok".into())));
            conn.close().await.unwrap();
            reopened.into_file().close(&cx).unwrap();
            assert_eq!(host_fs::read(&path).unwrap(), original);
        });
    }

    #[test]
    fn journaled_admission_is_retryable_and_never_acknowledges_rejected_packets() {
        with_runtime(async {
            let cx = context();
            let root = tempfile::tempdir().unwrap().keep();
            let mut source = captured_image(&cx);
            let manifest = source.manifest().clone();
            let first = source.next_packet(&cx).unwrap().unwrap();
            let second = source.next_packet(&cx).unwrap().unwrap();
            let wire = first.to_bytes().unwrap();
            // Header + exactly one record (sequence/length, packet, chain hash).
            let limit = 40 + 12 + u64::try_from(wire.len()).unwrap() + 32;
            let mut spool = create_journal(&cx, &root.join("bounded.spool"), &manifest, limit).await;
            let options = BootstrapOptions::new(root.join("bounded-image"));
            let mut bootstrap = JournaledSnapshotBootstrap::begin(
                &cx, &options, &mut spool, manifest.id(),
            ).unwrap();
            let mut bad = ReplicationPacket::from_bytes(&wire).unwrap();
            bad.symbol_data[0] ^= 1;
            assert_eq!(bootstrap.receive_packet(&cx, &bad).await.unwrap(), SnapshotPacketResult::Rejected);
            assert_eq!(bootstrap.checkpoint(&cx).unwrap().record_count, 0);
            assert_eq!(bootstrap.receive_packet(&cx, &first).await.unwrap(), SnapshotPacketResult::Accepted);
            let saved = bootstrap.checkpoint(&cx).unwrap();
            assert_eq!(bootstrap.receive_packet(&cx, &first).await.unwrap(), SnapshotPacketResult::Duplicate);
            assert!(matches!(bootstrap.receive_packet(&cx, &second).await, Err(FrankenError::TooBig)));
            assert!(!bootstrap.progress().poisoned);
            assert_eq!(bootstrap.checkpoint(&cx).unwrap(), saved);
            assert!(matches!(bootstrap.finish_and_open(&cx).await, Err(FrankenError::Busy)));
            assert_eq!(spool.state(), SnapshotSpoolState::Ready);
            assert_eq!(spool.record_count(), 1);
            spool.into_file().close(&cx).unwrap();
        });
    }

    #[test]
    fn journaled_begin_refuses_wrong_identity_and_used_spools_before_output_creation() {
        with_runtime(async {
            let cx = context();
            let root = tempfile::tempdir().unwrap().keep();
            let mut source = captured_image(&cx);
            let manifest = source.manifest().clone();
            let mut spool = create_journal(&cx, &root.join("used.spool"), &manifest, 1 << 20).await;
            let options = BootstrapOptions::new(root.join("refused-image"));
            let mut wrong_id = manifest.id();
            wrong_id[0] ^= 1;
            assert!(JournaledSnapshotBootstrap::begin(&cx, &options, &mut spool, wrong_id).is_err());
            assert!(host_fs::metadata(&options.directory).is_err());
            let first = source.next_packet(&cx).unwrap().unwrap();
            spool.append(&cx, &first).await.unwrap();
            assert!(matches!(
                JournaledSnapshotBootstrap::begin(&cx, &options, &mut spool, manifest.id()),
                Err(FrankenError::BusyRecovery)
            ));
            assert!(host_fs::metadata(&options.directory).is_err());
            assert_eq!(spool.record_count(), 1);
            spool.into_file().close(&cx).unwrap();
        });
    }
}
