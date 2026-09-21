//! rusqlite-compatible adapter layer for FrankenSQLite.
//!
//! Provides familiar macros, traits, and wrappers so that migrating from
//! `rusqlite` to `fsqlite` is mostly mechanical import swaps.

mod batch;
#[cfg(feature = "session")]
pub mod changeset;
#[cfg(all(feature = "session", feature = "native", not(target_arch = "wasm32")))]
pub mod changeset_stream;
mod connection;
mod flags;
mod optional;
mod params;
#[cfg(all(feature = "native", not(target_arch = "wasm32"), unix))]
pub mod recovery;
mod row;
mod transaction;

pub use batch::*;
pub use connection::*;
pub use flags::*;
pub use optional::*;
pub use params::*;
pub use row::*;
pub use transaction::*;

/// Authenticated snapshot bootstrap into a new, writable SQL database.
///
/// This is a FrankenSQLite replication extension, not a rusqlite API.
#[cfg(all(feature = "native", not(target_arch = "wasm32"), unix))]
pub mod snapshot {
    use std::io::Write as _;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use fsqlite_error::Result;
    use fsqlite_types::LockLevel;
    use fsqlite_types::cx::Cx;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::namespace::{
        DatabaseNamespaceBinding, NamespaceOpenIntent, PendingNamespaceOpen,
    };
    use fsqlite_vfs::unix::{UnixFile, UnixVfs};
    use fsqlite_vfs::{SyncKind, Vfs, VfsFile, host_fs};

    pub use fsqlite_core::replication_sender::ReplicationPacket;
    pub use fsqlite_core::snapshot_shipping::SnapshotPacketResult;
    pub use fsqlite_core::snapshot_shipping::manifest::{
        ManifestSnapshotReceiver, SnapshotImageReceipt, SnapshotImageState, SnapshotManifest,
    };
    use fsqlite_core::snapshot_shipping::manifest::SnapshotImageWriter;

    use crate::{Connection, ConnectionEnv, FrankenError};

    const DATABASE_NAME: &str = "database.db";
    const MANIFEST_NAME: &str = "manifest.fsqlite";

    /// Explicit limits for the received image and retained encoded payloads.
    ///
    /// The payload limit is not a total-RSS limit: decoder workspaces have the
    /// codec's separate admission limit. Completed blocks are written and
    /// released immediately rather than retaining a database-sized image.
    #[derive(Debug, Clone, Copy)]
    pub struct BootstrapLimits {
        pub max_file_bytes: u64,
        pub max_payload_bytes: usize,
    }

    impl Default for BootstrapLimits {
        fn default() -> Self {
            Self {
                max_file_bytes: 1 << 30,
                max_payload_bytes: 64 << 20,
            }
        }
    }

    /// A dedicated, empty output directory in a caller-controlled namespace.
    ///
    /// Missing directories are created. An existing nonempty directory is
    /// refused, including one left by a previous failed or completed import.
    /// `manifest.fsqlite` is reserved with create-new semantics before any
    /// database is created. Neither it nor a failed database is ever deleted.
    /// Treat the directory as private staging until the returned open receipt;
    /// do not open, rename, or modify its contents during transfer.
    #[derive(Debug, Clone)]
    pub struct BootstrapOptions {
        pub directory: PathBuf,
        pub limits: BootstrapLimits,
    }

    impl BootstrapOptions {
        #[must_use]
        pub fn new(directory: impl Into<PathBuf>) -> Self {
            Self {
                directory: directory.into(),
                limits: BootstrapLimits::default(),
            }
        }
    }

    /// Transfer progress is not a durability acknowledgement.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BootstrapProgress {
        pub blocks_decoded: usize,
        pub blocks_written: usize,
        pub blocks_total: usize,
        pub retained_payload_bytes: usize,
        pub poisoned: bool,
    }

    impl BootstrapProgress {
        /// All blocks are staged; final verification, sync and open are still due.
        #[must_use]
        pub const fn ready_to_finish(self) -> bool {
            !self.poisoned
                && self.blocks_decoded == self.blocks_total
                && self.blocks_written == self.blocks_total
        }
    }

    /// A verified, synced installation and the separate result of SQL open.
    ///
    /// The receipt describes the exact imported image *before* SQL open.
    /// Successful open may initialize ordinary WAL/metadata and subsequent
    /// writes may change the image. An SQL-open error does not undo installation
    /// or delete its evidence. The connection retains normal writer defaults.
    #[derive(Debug)]
    #[must_use = "inspect both the installed image receipt and SQL open result"]
    pub struct SnapshotOpen {
        pub image: SnapshotImageReceipt,
        pub database_path: PathBuf,
        pub connection: Result<Connection>,
    }

    // Field order matters: the managed database handle must be dropped before
    // its namespace lease. Never open/close an independent raw main-file fd:
    // that would release other connections' POSIX locks in this process.
    #[derive(Debug)]
    struct Destination {
        image: SnapshotImageWriter<UnixFile>,
        binding: Arc<DatabaseNamespaceBinding>,
        directory: PathBuf,
        path: PathBuf,
    }

    fn checkpoint(cx: &Cx) -> Result<()> {
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)
    }

    fn cannot_open(path: &Path) -> FrankenError {
        FrankenError::CannotOpen { path: path.to_owned() }
    }

    fn validate_image_budget(
        manifest: &SnapshotManifest,
        expected_id: [u8; 32],
        limits: BootstrapLimits,
    ) -> Result<()> {
        if manifest.id() != expected_id {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "snapshot does not match the trusted manifest identity".to_owned(),
            });
        }
        let page_size = manifest.page_size();
        if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "snapshot bootstrap requires a SQLite page size".to_owned(),
            });
        }
        let page_count = manifest.blocks().iter().try_fold(0_u64, |count, block| {
            count.checked_add(u64::from(block.page_count()))
        }).ok_or(FrankenError::TooBig)?;
        let bytes = page_count.checked_mul(u64::from(page_size)).ok_or(FrankenError::TooBig)?;
        if page_count == 0 || page_count >= u64::from(u32::MAX)
            || bytes > limits.max_file_bytes || limits.max_payload_bytes == 0
        {
            return Err(FrankenError::TooBig);
        }
        Ok(())
    }

    impl Destination {
        fn create(
            cx: &Cx,
            options: &BootstrapOptions,
            manifest: SnapshotManifest,
            expected_id: [u8; 32],
        ) -> Result<Self> {
            checkpoint(cx)?;
            // All content/dimension admission precedes directory/file creation.
            validate_image_budget(&manifest, expected_id, options.limits)?;
            if options.directory.to_str().is_none() {
                return Err(cannot_open(&options.directory));
            }
            let vfs = UnixVfs::new();
            let directory = vfs.full_pathname(cx, &options.directory)?;
            if directory.to_str().is_none() {
                return Err(cannot_open(&directory));
            }
            host_fs::create_dir_all(&directory)?;
            if !host_fs::read_dir_paths(&directory)?.is_empty() {
                return Err(cannot_open(&directory));
            }
            let manifest_path = directory.join(MANIFEST_NAME);
            // Atomic ownership claim also excludes another snapshot importer.
            // The artifact contains no authentication key. It is not itself a
            // trusted root; callers must retain the independently obtained ID.
            let mut claim = host_fs::reserve_new_file(&manifest_path)?;
            let entries = host_fs::read_dir_paths(&directory)?;
            if entries.len() != 1 || entries[0] != manifest_path {
                return Err(cannot_open(&directory));
            }
            claim.write_all(&manifest.to_bytes())?;
            claim.sync_all()?;
            drop(claim);
            checkpoint(cx)?;

            let path = directory.join(DATABASE_NAME);
            let admission = PendingNamespaceOpen::begin(&path, NamespaceOpenIntent::ReservedExclusive)?;
            let (mut file, _) = vfs.open(
                cx,
                Some(&path),
                VfsOpenFlags::CREATE | VfsOpenFlags::EXCLUSIVE
                    | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB,
            )?;
            // The namespace lease excludes FrankenSQLite peers. The main-file
            // lock also excludes cooperative stock SQLite processes while the
            // private image is incomplete. Neither lock is a filesystem ACL.
            file.lock(cx, LockLevel::Shared)?;
            file.lock(cx, LockLevel::Exclusive)?;
            let identity = file.file_identity()?.ok_or_else(|| cannot_open(&path))?;
            let binding = admission.bind(identity)?;
            binding.validate_path_identity()?;
            let image = SnapshotImageWriter::create(
                cx, file, manifest, expected_id, options.limits.max_file_bytes,
            )?;
            Ok(Self { image, binding, directory, path })
        }

        async fn finish_and_open(mut self, cx: &Cx, env: ConnectionEnv) -> Result<SnapshotOpen> {
            // The existing writer authenticates every block again, verifies
            // readback, syncs data BEFORE installing page one, then syncs it.
            let image = self.image.finish(cx).await?;
            let mut file = self.image.into_file();
            let identity = file.refresh_file_identity()?.ok_or_else(|| cannot_open(&self.path))?;
            self.binding.validate_path_identity_with_descriptor(Some(identity))?;

            // Refuse any sidecar introduced outside this exclusive owner.
            // An old WAL/certificate/FEC file must never overlay the new image.
            for entry in host_fs::read_dir_paths(&self.directory)? {
                let allowed = [MANIFEST_NAME, DATABASE_NAME, "database.db-fsqlite-ns-gate", "database.db-fsqlite-ns-use"]
                    .iter().any(|name| entry == self.directory.join(name));
                if !allowed { return Err(cannot_open(&entry)); }
            }
            file.durable_sync(cx, SyncKind::FullDurable)?;
            let vfs = UnixVfs::new();
            vfs.sync_parent_directory(cx, &self.path)?;
            vfs.sync_parent_directory(cx, &self.directory)?;
            self.binding.validate_path_identity_with_descriptor(file.refresh_file_identity()?)?;
            // Only the fully verified image becomes admissible. Release the
            // main lock before SQL ingress, but retain the managed descriptor
            // and the shared namespace binding across the identity-checked open.
            file.unlock(cx, LockLevel::None)?;
            self.binding.finish_bootstrap()?;
            let ingress = checkpoint(cx).and_then(|()| {
                self.path.to_str().map(str::to_owned).ok_or_else(|| cannot_open(&self.path))
            });
            let connection = match ingress {
                Err(error) => Err(error),
                Ok(path) => {
                    Connection::open_existing_with_expected_identity_and_env(
                        path, identity, env,
                    ).await
                }
            };
            drop(file);
            drop(self.binding);
            Ok(SnapshotOpen { image, database_path: self.path, connection })
        }
    }

    /// Stream authenticated source/repair packets directly into a SQL replica.
    ///
    /// Obtain the manifest ID and authentication key independently of the
    /// untrusted transport. The existing manifest receiver handles duplicate,
    /// out-of-order and erased symbols. This owner drains completed blocks to
    /// the existing verified image writer; it does not collect the full image.
    ///
    /// A dropped/failed write attempt is terminal, including cancellation
    /// between draining decoded output and entering the image writer. Preserve
    /// the failed directory, quiesce outstanding I/O and retry into another
    /// empty directory. No private executor or background SQL task is created.
    /// A dropped finalizer may leave a complete image without a returned receipt.
    ///
    /// This installs a snapshot, not an ordered incremental replication log or
    /// an atomic capture of a live source. The sender must supply a consistent
    /// source snapshot. It never replaces an existing database generation.
    #[derive(Debug)]
    pub struct SnapshotBootstrap {
        destination: Destination,
        receiver: ManifestSnapshotReceiver,
        write_in_flight: bool,
    }

    impl SnapshotBootstrap {
        pub fn begin(
            cx: &Cx,
            options: &BootstrapOptions,
            manifest: SnapshotManifest,
            expected_id: [u8; 32],
            authentication_key: [u8; 32],
        ) -> Result<Self> {
            checkpoint(cx)?;
            let receiver = ManifestSnapshotReceiver::new(
                manifest.clone(), expected_id, authentication_key, options.limits.max_payload_bytes,
            )?;
            let destination = Destination::create(cx, options, manifest, expected_id)?;
            Ok(Self { destination, receiver, write_in_flight: false })
        }

        #[must_use]
        pub fn progress(&self) -> BootstrapProgress {
            BootstrapProgress {
                blocks_decoded: self.receiver.blocks_decoded(),
                blocks_written: self.destination.image.blocks_applied(),
                blocks_total: self.receiver.manifest().blocks().len(),
                retained_payload_bytes: self.receiver.retained_payload_bytes(),
                poisoned: self.write_in_flight || self.destination.image.state() == SnapshotImageState::Poisoned,
            }
        }

        /// Private staging pathname; do not open it before successful finalization.
        #[must_use]
        pub fn database_path(&self) -> &Path { &self.destination.path }

        /// Receive one packet and write any newly completed block before returning.
        /// Accepted/staged packets have not yet crossed the final durability fence.
        pub async fn receive_packet(
            &mut self, cx: &Cx, packet: &ReplicationPacket,
        ) -> Result<SnapshotPacketResult> {
            if self.progress().poisoned { return Err(FrankenError::BusyRecovery); }
            let result = self.receiver.process_packet(cx, packet)?;
            // Set BEFORE draining: a cancellation at the writer's entry
            // checkpoint must not lose a decoded block and permit a later retry.
            self.write_in_flight = true;
            for block in self.receiver.take_decoded_blocks() {
                self.destination.image.apply_block(cx, &block).await?;
            }
            self.write_in_flight = false;
            Ok(result)
        }

        pub async fn finish_and_open(self, cx: &Cx) -> Result<SnapshotOpen> {
            self.finish_and_open_with_env(cx, ConnectionEnv::default()).await
        }

        /// Verify, sync and install, then open the same identity for ordinary SQL.
        ///
        /// Outer Err is not a rollback certificate: partial or even complete
        /// bytes may exist. Outer Ok preserves the image receipt even when SQL
        /// open fails. Import cancellation uses cx; SQL uses the supplied env's
        /// unchanged lineage. Use a related env when both should share cancellation.
        pub async fn finish_and_open_with_env(
            self, cx: &Cx, env: ConnectionEnv,
        ) -> Result<SnapshotOpen> {
            let progress = self.progress();
            if progress.poisoned { return Err(FrankenError::BusyRecovery); }
            if !progress.ready_to_finish() { return Err(FrankenError::Busy); }
            self.destination.finish_and_open(cx, env).await
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use fsqlite_core::replication_sender::{PageEntry, SenderConfig};
        use fsqlite_core::snapshot_shipping::SnapshotSender;
        use crate::SqliteValue;

        const KEY: [u8; 32] = [0x6B; 32];

        fn with_runtime<F: std::future::Future>(future: F) -> F::Output {
            asupersync::runtime::RuntimeBuilder::current_thread()
                .blocking_threads(1, 2)
                .build()
                .unwrap()
                .block_on(future)
        }

        fn context() -> Cx {
            let cx = Cx::new();
            cx.set_native_cx(asupersync::Cx::current().unwrap());
            cx
        }

        fn fixture() -> (PathBuf, SnapshotManifest, Vec<ReplicationPacket>, u64) {
            let root = tempfile::tempdir().unwrap().keep();
            let source = root.join("source.db");
            let stock = rusqlite::Connection::open(&source).unwrap();
            stock.execute_batch(
                "PRAGMA page_size=512; \
                 CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT UNIQUE, qty INTEGER); \
                 CREATE INDEX by_qty ON items(qty); \
                 CREATE VIEW available AS SELECT id,name FROM items WHERE qty>0; \
                 INSERT INTO items VALUES(1,'alpha',3),(2,'beta',0),(3,'gamma',7);",
            ).unwrap();
            // Never overlap two SQLite implementations on the same file in one
            // process. The transport is built from the closed oracle image.
            stock.close().unwrap();
            let bytes = host_fs::read(&source).unwrap();
            let length = u64::try_from(bytes.len()).unwrap();
            assert_eq!(bytes.len() % 512, 0);
            let mut pages: Vec<_> = bytes.chunks_exact(512).enumerate().map(|(index, page)| {
                PageEntry::new(u32::try_from(index + 1).unwrap(), page.to_vec())
            }).collect();
            let mut sender = SnapshotSender::prepare(512, &mut pages, SenderConfig {
                symbol_size: 256,
                max_isi_multiplier: 3,
            }).unwrap();
            let manifest = sender.manifest().unwrap();
            let cx = context();
            let mut packets = Vec::new();
            while let Some(mut packet) = sender.next_packet(&cx).unwrap() {
                packet.attach_auth_tag(&KEY);
                packets.push(packet);
            }
            (root, manifest, packets, length)
        }

        async fn receive_all(bootstrap: &mut SnapshotBootstrap, cx: &Cx, packets: &[ReplicationPacket]) {
            for packet in packets {
                bootstrap.receive_packet(cx, packet).await.unwrap();
            }
            assert!(bootstrap.progress().ready_to_finish());
            assert_eq!(bootstrap.progress().retained_payload_bytes, 0);
        }

        #[test]
        fn bootstrap_reordered_erasure_repair_opens_queries_writes_and_reopens() {
            with_runtime(async {
                let (root, manifest, packets, byte_len) = fixture();
                let expected_id = manifest.id();
                let options = BootstrapOptions::new(root.join("replica"));
                let cx = context();
                let mut bootstrap = SnapshotBootstrap::begin(
                    &cx, &options, manifest, expected_id, KEY,
                ).unwrap();
                // Pin the entire transfer behind namespace admission, not just
                // a flag in this object. The check opens no raw main-file fd.
                assert!(matches!(
                    PendingNamespaceOpen::begin(bootstrap.database_path(), NamespaceOpenIntent::Shared),
                    Err(FrankenError::Busy)
                ));
                assert!(packets.iter().any(|packet| packet.esi >= packet.k_source));
                // Drop source symbol zero and deliver repair packets first.
                // Deliver each remaining packet twice to cover authenticated
                // duplicates both before and after decode completion.
                for packet in packets.iter().rev().filter(|packet| packet.esi != 0) {
                    bootstrap.receive_packet(&cx, packet).await.unwrap();
                    bootstrap.receive_packet(&cx, packet).await.unwrap();
                }
                assert!(bootstrap.progress().ready_to_finish());
                assert_eq!(bootstrap.progress().retained_payload_bytes, 0);
                let opened = bootstrap.finish_and_open(&cx).await.unwrap();
                assert_eq!(opened.image.manifest_id, expected_id);
                assert_eq!(opened.image.byte_len, byte_len);
                assert_eq!(u64::from(opened.image.page_count) * 512, byte_len);
                let path = opened.database_path;
                let conn = opened.connection.unwrap();
                let rows = conn.query("SELECT name FROM available ORDER BY id").await.unwrap();
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].get(0), Some(&SqliteValue::Text("alpha".into())));
                assert_eq!(rows[1].get(0), Some(&SqliteValue::Text("gamma".into())));
                assert_eq!(
                    conn.query_row("PRAGMA integrity_check").await.unwrap().get(0),
                    Some(&SqliteValue::Text("ok".into()))
                );
                conn.execute("BEGIN; INSERT INTO items VALUES(4,'delta',9); COMMIT;").await.unwrap();
                conn.close().await.unwrap();
                let reopened = Connection::open(path.to_str().unwrap()).await.unwrap();
                assert_eq!(
                    reopened.query_row("SELECT count(*) FROM items").await.unwrap().get(0),
                    Some(&SqliteValue::Integer(4))
                );
                reopened.close().await.unwrap();
                // Source and durable manifest evidence were not modified.
                let stock = rusqlite::Connection::open(root.join("source.db")).unwrap();
                assert_eq!(stock.query_row("SELECT count(*) FROM items", [], |row| row.get::<_, i64>(0)).unwrap(), 3);
                stock.close().unwrap();
                assert_eq!(
                    SnapshotManifest::from_bytes(&host_fs::read(&options.directory.join(MANIFEST_NAME)).unwrap()).unwrap().id(),
                    expected_id
                );
            });
        }

        #[test]
        fn bootstrap_rejects_untrusted_manifest_and_excessive_size_before_creation() {
            with_runtime(async {
                let (root, manifest, _, byte_len) = fixture();
                let id = manifest.id();
                let cx = context();
                let options = BootstrapOptions::new(root.join("wrong-root"));
                let mut wrong = id;
                wrong[0] ^= 1;
                assert!(SnapshotBootstrap::begin(&cx, &options, manifest.clone(), wrong, KEY).is_err());
                assert!(!options.directory.exists());
                let mut options = BootstrapOptions::new(root.join("too-large"));
                options.limits.max_file_bytes = byte_len - 1;
                assert!(matches!(
                    SnapshotBootstrap::begin(&cx, &options, manifest.clone(), id, KEY),
                    Err(FrankenError::TooBig)
                ));
                assert!(!options.directory.exists());
                options.limits.max_file_bytes = byte_len;
                options.limits.max_payload_bytes = 0;
                assert!(matches!(
                    SnapshotBootstrap::begin(&cx, &options, manifest, id, KEY),
                    Err(FrankenError::TooBig)
                ));
                assert!(!options.directory.exists());
            });
        }

        #[test]
        fn bootstrap_refuses_existing_data_and_duplicate_destination_owner() {
            with_runtime(async {
                let (root, manifest, _, _) = fixture();
                let id = manifest.id();
                let cx = context();
                let options = BootstrapOptions::new(root.join("occupied"));
                let existing = options.directory.join(DATABASE_NAME);
                host_fs::write(&existing, b"unrelated database owner").unwrap();
                assert!(SnapshotBootstrap::begin(&cx, &options, manifest.clone(), id, KEY).is_err());
                assert_eq!(host_fs::read(&existing).unwrap(), b"unrelated database owner");
                assert_eq!(host_fs::read_dir_paths(&options.directory).unwrap(), vec![existing]);

                let options = BootstrapOptions::new(root.join("claimed"));
                let first = SnapshotBootstrap::begin(&cx, &options, manifest.clone(), id, KEY).unwrap();
                assert!(SnapshotBootstrap::begin(&cx, &options, manifest, id, KEY).is_err());
                drop(first);
                assert!(options.directory.join(MANIFEST_NAME).exists());
                assert!(options.directory.join(DATABASE_NAME).exists());
            });
        }

        #[test]
        fn bootstrap_rejects_bad_authentication_without_poisoning_valid_transfer() {
            with_runtime(async {
                let (root, manifest, packets, _) = fixture();
                let id = manifest.id();
                let options = BootstrapOptions::new(root.join("authenticated"));
                let cx = context();
                let mut bootstrap = SnapshotBootstrap::begin(&cx, &options, manifest, id, KEY).unwrap();
                let before = bootstrap.progress();
                let mut bad = packets[0].clone();
                bad.attach_auth_tag(&[0xCD; 32]);
                assert!(matches!(
                    bootstrap.receive_packet(&cx, &bad).await.unwrap(),
                    SnapshotPacketResult::Rejected
                ));
                assert_eq!(bootstrap.progress(), before);
                receive_all(&mut bootstrap, &cx, &packets).await;
                let result = bootstrap.finish_and_open(&cx).await.unwrap();
                result.connection.unwrap().close().await.unwrap();
            });
        }

        #[test]
        fn incomplete_or_poisoned_bootstrap_cannot_open_sql() {
            with_runtime(async {
                let (root, manifest, packets, _) = fixture();
                let id = manifest.id();
                let cx = context();
                let options = BootstrapOptions::new(root.join("incomplete"));
                let bootstrap = SnapshotBootstrap::begin(&cx, &options, manifest.clone(), id, KEY).unwrap();
                assert!(matches!(bootstrap.finish_and_open(&cx).await, Err(FrankenError::Busy)));
                assert!(options.directory.join(DATABASE_NAME).exists());

                let options = BootstrapOptions::new(root.join("abandoned-write"));
                let mut bootstrap = SnapshotBootstrap::begin(&cx, &options, manifest, id, KEY).unwrap();
                // Model the exact retained state after dropping a write future
                // once decoded output has been drained. Lower-level abandoned
                // I/O is separately covered by SnapshotImageWriter's tests.
                bootstrap.write_in_flight = true;
                assert!(bootstrap.progress().poisoned);
                assert!(matches!(bootstrap.receive_packet(&cx, &packets[0]).await, Err(FrankenError::BusyRecovery)));
                assert!(matches!(bootstrap.finish_and_open(&cx).await, Err(FrankenError::BusyRecovery)));
                assert!(options.directory.join(MANIFEST_NAME).exists());
            });
        }

        #[test]
        fn bootstrap_never_opens_a_replaced_main_path() {
            with_runtime(async {
                let (root, manifest, packets, _) = fixture();
                let id = manifest.id();
                let options = BootstrapOptions::new(root.join("replaced-path"));
                let cx = context();
                let mut bootstrap = SnapshotBootstrap::begin(&cx, &options, manifest, id, KEY).unwrap();
                receive_all(&mut bootstrap, &cx, &packets).await;
                let path = bootstrap.database_path().to_owned();
                let retired = root.join("retired-image.db");
                // Deliberately violate cooperative namespace ownership. The
                // original managed fd stays alive; the impostor is never opened.
                host_fs::rename(&path, &retired).unwrap();
                host_fs::write(&path, b"different owner").unwrap();
                assert!(bootstrap.finish_and_open(&cx).await.is_err());
                assert_eq!(host_fs::read(&path).unwrap(), b"different owner");
                assert!(retired.exists());
            });
        }

        #[test]
        fn bootstrap_refuses_an_unowned_sidecar_before_sql_open() {
            with_runtime(async {
                let (root, manifest, packets, _) = fixture();
                let id = manifest.id();
                let options = BootstrapOptions::new(root.join("sidecar-injection"));
                let cx = context();
                let mut bootstrap = SnapshotBootstrap::begin(&cx, &options, manifest, id, KEY).unwrap();
                receive_all(&mut bootstrap, &cx, &packets).await;
                let sidecar = options.directory.join("database.db-wal");
                host_fs::write(&sidecar, b"unrelated WAL").unwrap();
                assert!(bootstrap.finish_and_open(&cx).await.is_err());
                assert_eq!(host_fs::read(&sidecar).unwrap(), b"unrelated WAL");
            });
        }

        #[test]
        fn bootstrap_precancelled_admission_creates_nothing() {
            with_runtime(async {
                let (root, manifest, _, _) = fixture();
                let id = manifest.id();
                let options = BootstrapOptions::new(root.join("cancelled"));
                let cx = context();
                cx.cancel();
                assert!(matches!(
                    SnapshotBootstrap::begin(&cx, &options, manifest, id, KEY),
                    Err(FrankenError::Interrupt)
                ));
                assert!(!options.directory.exists());
            });
        }
    }
}
