#![cfg_attr(windows, feature(windows_by_handle))]

pub mod memory;
pub mod metrics;
#[cfg(all(feature = "native", any(unix, windows)))]
pub mod namespace;
pub mod process;
pub mod shm;
pub mod traits;
#[cfg(all(feature = "native", unix))]
pub mod unix;
#[cfg(all(feature = "native", unix))]
mod unix_shm;
#[cfg(all(feature = "native", target_os = "linux"))]
pub mod uring;
#[cfg(all(feature = "native", target_os = "windows"))]
pub mod windows;

/// Host filesystem helpers that are allowed to use `std::fs`.
///
/// The ambient-authority audit gate (`fsqlite-types::cx::tests::test_ambient_authority_audit_gate`)
/// forbids `std::fs::` usage outside the VFS boundary. Test harness crates should depend on these
/// helpers rather than calling `std::fs` directly.
#[cfg(feature = "native")]
pub mod host_fs {
    use std::fs::{File, OpenOptions};
    use std::io::Write as _;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[cfg(unix)]
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
    #[cfg(windows)]
    use std::os::windows::fs::OpenOptionsExt as _;

    #[cfg(windows)]
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

    use fsqlite_error::Result;

    pub fn create_dir_all(path: &Path) -> Result<()> {
        std::fs::create_dir_all(path)?;
        Ok(())
    }

    pub fn read(path: &Path) -> Result<Vec<u8>> {
        Ok(std::fs::read(path)?)
    }

    pub fn read_to_string(path: &Path) -> Result<String> {
        Ok(std::fs::read_to_string(path)?)
    }

    pub fn metadata(path: &Path) -> Result<std::fs::Metadata> {
        Ok(std::fs::metadata(path)?)
    }

    /// Atomically reserve a new, empty, read-write file at `path`.
    ///
    /// This never creates parent directories and never follows or replaces an
    /// existing final path component: regular files, directories, and both
    /// live and dangling symlinks all make `create_new` fail. On Unix the
    /// initial mode is owner-only; the process umask may narrow it further.
    ///
    /// On Windows this handle is retained open for the lifetime of a caller's
    /// reservation (see `fsqlite-core`'s `DatabaseBuilderReservation`) while a
    /// second, independent open against the same path is made through the
    /// `windows` VFS module. Rust's default Windows share mode is
    /// `FILE_SHARE_READ | FILE_SHARE_WRITE` (no `FILE_SHARE_DELETE`); this
    /// explicitly matches stock SQLite's `FILE_SHARE_READ | FILE_SHARE_WRITE
    /// | FILE_SHARE_DELETE` instead, for the same bd-h5oaj / GH#355 reason
    /// documented next to `windows_open_options` in `windows.rs`.
    pub fn reserve_new_file(path: &Path) -> Result<File> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        #[cfg(windows)]
        {
            const FILE_SHARE_READ: u32 = 0x0000_0001;
            const FILE_SHARE_WRITE: u32 = 0x0000_0002;
            const FILE_SHARE_DELETE: u32 = 0x0000_0004;
            options.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE);
        }
        Ok(options.open(path)?)
    }

    /// Revalidate a reserved file's pathname against its retained descriptor identity.
    ///
    /// On Unix this deliberately uses metadata only. Opening and closing an
    /// independent descriptor for a replacement could release another local
    /// connection's POSIX locks on that file. Final symlinks, non-regular files
    /// and hard-link aliases are refused. Windows compares the full native
    /// handle identity through the existing no-follow ingress.
    ///
    /// The caller must keep the descriptor that supplied `expected_identity`
    /// alive. This is an observation, not a namespace lock: the same trusted,
    /// cooperative parent-directory contract as native database admission
    /// applies. Nothing is created, rewritten, truncated or removed.
    #[cfg(any(unix, windows))]
    pub fn validate_reserved_file_identity(
        path: &Path,
        expected_identity: super::FileIdentity,
    ) -> Result<()> {
        #[cfg(unix)]
        {
            let metadata = std::fs::symlink_metadata(path)?;
            if !metadata.is_file()
                || metadata.nlink() != 1
                || super::FileIdentity::from_unix_parts(metadata.dev(), metadata.ino())
                    != expected_identity
            {
                return Err(fsqlite_error::FrankenError::BusyRecovery);
            }
        }
        #[cfg(windows)]
        {
            let file = open_existing_regular_file_no_follow(path)?;
            if super::FileIdentity::from_file(&file)? != Some(expected_identity) {
                return Err(fsqlite_error::FrankenError::BusyRecovery);
            }
        }
        Ok(())
    }

    pub fn open_file(path: &Path) -> Result<File> {
        Ok(File::open(path)?)
    }

    /// Open an existing regular file without following its final path entry.
    ///
    /// This is the identity-guard ingress for callers that will subsequently
    /// ask the pager to open the same cooperative database pathname. It rejects
    /// final symlinks/reparse points and non-regular objects before they can
    /// block or alias the identity probe. Unix additionally rejects files with
    /// multiple hard links, because such a pathname is not an isolated
    /// authority namespace.
    pub fn open_existing_regular_file_no_follow(path: &Path) -> Result<File> {
        open_existing_regular_file_with_access(path, false)
    }

    /// Open an existing, identity-checked WAL for synchronous guarded repair.
    ///
    /// Never creates, truncates, or replaces a file. The caller must retain the
    /// native main-file recovery fence and a stable cooperative namespace for
    /// the descriptor's entire lifetime. This is NOT a locking primitive.
    /// Do not use this to open a locked main database: closing an independent
    /// descriptor can release that process's POSIX main-file locks. Recovery
    /// must first prove through VFS that WAL and main have different identities.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_wal_for_guarded_repair(
        path: &Path,
        expected_identity: super::FileIdentity,
    ) -> Result<File> {
        let file = open_existing_regular_file_with_access(path, true)?;
        if super::FileIdentity::from_file(&file)? != Some(expected_identity) {
            return Err(fsqlite_error::FrankenError::BusyRecovery);
        }
        Ok(file)
    }

    fn open_existing_regular_file_with_access(path: &Path, writable: bool) -> Result<File> {
        let mut options = OpenOptions::new();
        options.read(true).write(writable);
        #[cfg(unix)]
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
        #[cfg(windows)]
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);

        let file = options.open(path)?;
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "identity-bound database path is not a regular file",
            )
            .into());
        }
        #[cfg(unix)]
        if metadata.nlink() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "identity-bound database path has multiple hard links",
            )
            .into());
        }
        Ok(file)
    }

    pub fn rename(from: &Path, to: &Path) -> Result<()> {
        std::fs::rename(from, to)?;
        Ok(())
    }

    pub fn read_link(path: &Path) -> Result<PathBuf> {
        Ok(std::fs::read_link(path)?)
    }

    pub fn read_dir_paths(dir: &Path) -> Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            out.push(entry?.path());
        }
        Ok(out)
    }

    pub fn write(path: &Path, bytes: impl AsRef<[u8]>) -> Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, bytes)?;
        Ok(())
    }

    pub fn copy_file(from: &Path, to: &Path) -> Result<u64> {
        if let Some(parent) = to.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        Ok(std::fs::copy(from, to)?)
    }

    /// Publish bytes through a newly reserved sibling, never a pre-existing
    /// temporary file.
    ///
    /// Unix staging and published files are owner-only from creation (the
    /// umask may narrow that further); Windows inherits the parent directory's
    /// ACL. Existing destination aliases are replaced by rename, not opened
    /// for writing, so their referents are not truncated.
    ///
    /// The staging descriptor is synced before rename. This is atomic file
    /// publication, NOT a multi-file snapshot or a directory durability fence.
    /// The parent directory must already exist and remain in the caller's
    /// trusted, cooperative namespace. Failed staging files are retained,
    /// never reused or removed by this helper.
    pub fn write_private_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
        publish_private_file(path, |file| {
            file.write_all(bytes)?;
            Ok(())
        })
    }

    /// Stream an existing regular source into a privately reserved sibling of
    /// `to`, then publish it atomically. Source permissions are not copied.
    ///
    /// `Ok(false)` means opening the SOURCE returned `NotFound`. Destination,
    /// read, write, sync, and publication failures always propagate, including
    /// a missing destination directory. Sources use the existing no-follow
    /// regular-file ingress; Unix additionally rejects multiple hard links.
    ///
    /// Like `copy_file`, this opens and closes an independent source descriptor;
    /// it must not be used to snapshot a live database without the caller's
    /// recovery/backup exclusion protocol. It neither acquires database locks
    /// nor makes a changing database/WAL family into a consistent snapshot.
    /// See `write_private_atomic` for publication and namespace requirements.
    pub fn copy_private_atomic_if_exists(from: &Path, to: &Path) -> Result<bool> {
        if from == to {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "private backup source and destination must differ",
            )
            .into());
        }
        let mut source = match open_existing_regular_file_no_follow(from) {
            Ok(file) => file,
            Err(fsqlite_error::FrankenError::Io(err))
                if err.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(false);
            }
            Err(err) => return Err(err),
        };
        publish_private_file(to, |file| {
            std::io::copy(&mut source, file)?;
            Ok(true)
        })
    }

    fn publish_private_file<T>(
        path: &Path,
        write: impl FnOnce(&mut File) -> Result<T>,
    ) -> Result<T> {
        let (staging_path, mut file) = reserve_private_sibling(path)?;
        let value = write(&mut file)?;
        file.sync_all()?;
        // Keep the reservation open through publication. reserve_new_file
        // explicitly permits delete sharing on Windows for this operation.
        std::fs::rename(&staging_path, path)?;
        Ok(value)
    }

    fn reserve_private_sibling(path: &Path) -> Result<(PathBuf, File)> {
        static NEXT_STAGING_ID: AtomicUsize = AtomicUsize::new(0);
        // create_new is the authority, not the predictability of the name.
        // Stale artifacts and concurrent callers never share a descriptor.
        for _ in 0..64 {
            let sequence = NEXT_STAGING_ID.fetch_add(1, Ordering::Relaxed);
            let mut name = path.as_os_str().to_owned();
            name.push(format!(".tmp.{}.{sequence}", std::process::id()));
            let staging_path = PathBuf::from(name);
            match reserve_new_file(&staging_path) {
                Ok(file) => return Ok((staging_path, file)),
                Err(fsqlite_error::FrankenError::Io(err))
                    if err.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(err) => return Err(err),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("could not reserve a private sibling of {}", path.display()),
        )
        .into())
    }

    #[cfg(test)]
    mod private_publication_tests {
        use super::*;

        #[test]
        fn write_error_preserves_previous_publication_and_partial_staging() {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("backup");
            std::fs::write(&path, b"previous complete image").unwrap();
            let result: Result<()> = publish_private_file(&path, |file| {
                file.write_all(b"partial new image")?;
                Err(std::io::Error::other("injected staging write failure").into())
            });
            assert!(result.is_err());
            assert_eq!(std::fs::read(&path).unwrap(), b"previous complete image");
            let staged: Vec<_> = read_dir_paths(&directory)
                .unwrap()
                .into_iter()
                .filter(|entry| entry != &path)
                .collect();
            assert_eq!(staged.len(), 1);
            assert_eq!(std::fs::read(&staged[0]).unwrap(), b"partial new image");
        }
    }

    pub fn remove_file(path: &Path) -> Result<()> {
        std::fs::remove_file(path).map_err(|e| {
            std::io::Error::new(e.kind(), format!("remove {}: {e}", path.display()))
        })?;
        Ok(())
    }

    pub fn create_empty_file(path: &Path) -> Result<()> {
        write(path, [])?;
        Ok(())
    }

    pub fn append_line(path: &Path, line: &str) -> Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        writeln!(file, "{line}")?;
        file.flush()?;
        Ok(())
    }
}

#[cfg(all(test, feature = "native", unix))]
mod host_fs_security_tests {
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::unix::fs::{PermissionsExt as _, symlink};

    use super::FileIdentity;
    use super::host_fs::{
        copy_private_atomic_if_exists, open_existing_regular_file_no_follow,
        open_wal_for_guarded_repair, reserve_new_file, write_private_atomic,
    };

    #[test]
    fn private_publication_replaces_broad_permissions_without_reusing_staging() {
        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("marker");
        let stale = directory.join("marker.tmp");
        std::fs::write(&path, b"old marker").unwrap();
        std::fs::write(&stale, b"retained interrupted write").unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o666);
        std::fs::set_permissions(&path, permissions).unwrap();

        write_private_atomic(&path, b"complete new marker").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"complete new marker");
        assert_eq!(
            std::fs::read(&stale).unwrap(),
            b"retained interrupted write"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o077,
            0
        );
    }

    #[test]
    fn private_publication_does_not_truncate_temporary_or_final_alias_targets() {
        for hard_link in [false, true] {
            let directory = tempfile::tempdir().unwrap().keep();
            let victim = directory.join("unrelated-data");
            let path = directory.join("marker");
            let stale = directory.join("marker.tmp");
            std::fs::write(&victim, b"must survive publication").unwrap();
            if hard_link {
                std::fs::hard_link(&victim, &path).unwrap();
                std::fs::hard_link(&victim, &stale).unwrap();
            } else {
                symlink(&victim, &path).unwrap();
                symlink(&victim, &stale).unwrap();
            }
            write_private_atomic(&path, b"new marker").unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), b"new marker");
            assert_eq!(std::fs::read(&victim).unwrap(), b"must survive publication");
            assert_eq!(std::fs::read(&stale).unwrap(), b"must survive publication");
            assert!(std::fs::symlink_metadata(&path).unwrap().is_file());
        }
    }

    #[test]
    fn private_copy_preserves_bytes_without_copying_source_permissions() {
        let directory = tempfile::tempdir().unwrap().keep();
        let source = directory.join("database");
        let backup = directory.join("backup");
        let payload: Vec<u8> = (0..=255).cycle().take(1_048_577).collect();
        std::fs::write(&source, &payload).unwrap();
        let mut permissions = std::fs::metadata(&source).unwrap().permissions();
        permissions.set_mode(0o644);
        std::fs::set_permissions(&source, permissions).unwrap();
        assert!(copy_private_atomic_if_exists(&source, &backup).unwrap());
        assert_eq!(std::fs::read(&backup).unwrap(), payload);
        assert_eq!(std::fs::read(&source).unwrap(), payload);
        assert_eq!(
            std::fs::metadata(&backup).unwrap().permissions().mode() & 0o077,
            0
        );
        assert_eq!(
            std::fs::metadata(&source).unwrap().permissions().mode() & 0o777,
            0o644
        );
    }

    #[test]
    fn private_copy_only_treats_source_not_found_as_optional() {
        let directory = tempfile::tempdir().unwrap().keep();
        let source = directory.join("source");
        let backup = directory.join("existing-backup");
        std::fs::write(&backup, b"previous backup").unwrap();
        assert!(!copy_private_atomic_if_exists(&source, &backup).unwrap());
        assert_eq!(std::fs::read(&backup).unwrap(), b"previous backup");

        std::fs::write(&source, b"new source").unwrap();
        let missing_destination_parent = directory.join("absent-parent").join("backup");
        assert!(matches!(
            copy_private_atomic_if_exists(&source, &missing_destination_parent),
            Err(fsqlite_error::FrankenError::Io(err))
                if err.kind() == std::io::ErrorKind::NotFound
        ));
        assert!(!directory.join("absent-parent").exists());
        assert!(copy_private_atomic_if_exists(&source, &source).is_err());
        assert_eq!(std::fs::read(&source).unwrap(), b"new source");
    }

    #[test]
    fn private_copy_rejects_dangling_and_live_source_aliases() {
        let directory = tempfile::tempdir().unwrap().keep();
        let source = directory.join("source");
        let link = directory.join("source-link");
        let backup = directory.join("backup");
        std::fs::write(&backup, b"preserve previous backup").unwrap();
        symlink(&source, &link).unwrap();
        assert!(copy_private_atomic_if_exists(&link, &backup).is_err());
        std::fs::write(&source, b"source bytes").unwrap();
        assert!(copy_private_atomic_if_exists(&link, &backup).is_err());
        let alias = directory.join("source-hard-link");
        std::fs::hard_link(&source, &alias).unwrap();
        assert!(copy_private_atomic_if_exists(&source, &backup).is_err());
        assert!(copy_private_atomic_if_exists(&alias, &backup).is_err());
        assert!(copy_private_atomic_if_exists(&directory, &backup).is_err());
        assert_eq!(std::fs::read(&backup).unwrap(), b"preserve previous backup");
        assert_eq!(std::fs::read(&source).unwrap(), b"source bytes");
    }

    #[test]
    fn failed_private_publication_retains_private_staging_and_destination() {
        let directory = tempfile::tempdir().unwrap().keep();
        let destination = directory.join("blocked");
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(destination.join("sentinel"), b"existing destination").unwrap();
        assert!(write_private_atomic(&destination, b"complete staging bytes").is_err());
        assert_eq!(
            std::fs::read(destination.join("sentinel")).unwrap(),
            b"existing destination"
        );
        let staged: Vec<_> = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path != &destination)
            .collect();
        assert_eq!(staged.len(), 1);
        assert_eq!(
            std::fs::read(&staged[0]).unwrap(),
            b"complete staging bytes"
        );
        assert_eq!(
            std::fs::metadata(&staged[0]).unwrap().permissions().mode() & 0o077,
            0
        );
    }

    #[test]
    fn concurrent_private_publication_never_shares_a_staging_file() {
        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("marker");
        write_private_atomic(&path, &[0; 8192]).unwrap();
        let barrier = std::sync::Barrier::new(9);
        std::thread::scope(|scope| {
            for tag in 1_u8..=8 {
                let path = &path;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    write_private_atomic(path, &[tag; 8192]).unwrap();
                });
            }
            barrier.wait();
            for _ in 0..64 {
                let image = std::fs::read(&path).unwrap();
                assert_eq!(image.len(), 8192);
                assert!(image[0] <= 8);
                assert!(image.iter().all(|&byte| byte == image[0]));
            }
        });
        let image = std::fs::read(&path).unwrap();
        assert!((1..=8).contains(&image[0]));
        assert!(image.iter().all(|&byte| byte == image[0]));
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
    }

    #[test]
    fn reserved_identity_revalidation_refuses_missing_and_replaced_paths() {
        use super::host_fs::validate_reserved_file_identity;

        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("reserved.db");
        let retained = directory.join("retained.db");
        let mut original = reserve_new_file(&path).unwrap();
        original
            .write_all(b"same bytes, different identity")
            .unwrap();
        let identity = FileIdentity::from_file(&original).unwrap().unwrap();
        validate_reserved_file_identity(&path, identity).unwrap();
        std::fs::rename(&path, &retained).unwrap();
        assert!(validate_reserved_file_identity(&path, identity).is_err());
        assert!(
            !path.exists(),
            "validation must not recreate a missing reservation"
        );
        std::fs::write(&path, b"same bytes, different identity").unwrap();
        assert!(matches!(
            validate_reserved_file_identity(&path, identity),
            Err(fsqlite_error::FrankenError::BusyRecovery)
        ));
        assert_eq!(FileIdentity::from_file(&original).unwrap(), Some(identity));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            std::fs::read(&retained).unwrap()
        );
    }

    #[test]
    fn reserved_identity_revalidation_refuses_symlinks_and_hard_link_aliases() {
        use super::host_fs::validate_reserved_file_identity;

        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("reserved.db");
        let original = reserve_new_file(&path).unwrap();
        let identity = FileIdentity::from_file(&original).unwrap().unwrap();
        let link = directory.join("symlink.db");
        symlink(&path, &link).unwrap();
        assert!(validate_reserved_file_identity(&link, identity).is_err());
        validate_reserved_file_identity(&path, identity).unwrap();
        let alias = directory.join("alias.db");
        std::fs::hard_link(&path, &alias).unwrap();
        assert!(validate_reserved_file_identity(&path, identity).is_err());
        assert!(validate_reserved_file_identity(&alias, identity).is_err());
    }

    #[test]
    fn guarded_wal_open_is_existing_only_and_preserves_bytes_and_identity() {
        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("source-wal");
        let mut original = reserve_new_file(&path).unwrap();
        original.write_all(b"original WAL bytes").unwrap();
        let identity = FileIdentity::from_file(&original).unwrap().unwrap();
        let mut writable = open_wal_for_guarded_repair(&path, identity).unwrap();
        let mut bytes = Vec::new();
        writable.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"original WAL bytes");
        writable.seek(SeekFrom::Start(0)).unwrap();
        writable.write_all(b"repaired").unwrap();
        assert_eq!(writable.metadata().unwrap().len(), 18);
        assert_eq!(FileIdentity::from_file(&writable).unwrap(), Some(identity));
        let missing = directory.join("missing-wal");
        assert!(open_wal_for_guarded_repair(&missing, identity).is_err());
        assert!(!missing.exists());
    }

    #[test]
    fn guarded_wal_open_refuses_wrong_identity_without_truncation() {
        let directory = tempfile::tempdir().unwrap().keep();
        let first = directory.join("first-wal");
        let second = directory.join("second-wal");
        let original = reserve_new_file(&first).unwrap();
        let identity = FileIdentity::from_file(&original).unwrap().unwrap();
        let mut unrelated = reserve_new_file(&second).unwrap();
        unrelated.write_all(b"other generation").unwrap();
        assert!(matches!(
            open_wal_for_guarded_repair(&second, identity),
            Err(fsqlite_error::FrankenError::BusyRecovery)
        ));
        assert_eq!(std::fs::read(&second).unwrap(), b"other generation");
    }

    #[test]
    fn guarded_wal_open_refuses_symlinks_and_hard_links() {
        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("source-wal");
        let original = reserve_new_file(&path).unwrap();
        let identity = FileIdentity::from_file(&original).unwrap().unwrap();
        let link = directory.join("symlink-wal");
        symlink(&path, &link).unwrap();
        assert!(open_wal_for_guarded_repair(&link, identity).is_err());
        let hard_link = directory.join("hardlink-wal");
        std::fs::hard_link(&path, &hard_link).unwrap();
        assert!(open_wal_for_guarded_repair(&path, identity).is_err());
        assert!(open_wal_for_guarded_repair(&hard_link, identity).is_err());
    }

    #[test]
    fn identity_guard_rejects_final_symlinks_and_hard_link_aliases() {
        let directory = tempfile::tempdir().expect("create isolated directory");
        let database = directory.path().join("authority.db");
        drop(reserve_new_file(&database).expect("reserve regular database"));
        open_existing_regular_file_no_follow(&database).expect("regular single-link file opens");

        let symlink_path = directory.path().join("authority-symlink.db");
        symlink(&database, &symlink_path).expect("create final symlink");
        assert!(
            open_existing_regular_file_no_follow(&symlink_path).is_err(),
            "identity guard must not follow a final symlink"
        );

        let hard_link_path = directory.path().join("authority-hardlink.db");
        std::fs::hard_link(&database, &hard_link_path).expect("create hard-link alias");
        assert!(
            open_existing_regular_file_no_follow(&database).is_err(),
            "identity guard must reject a multiply linked database"
        );
        assert!(
            open_existing_regular_file_no_follow(&hard_link_path).is_err(),
            "identity guard must reject the hard-link alias"
        );
    }
}

#[cfg(test)]
pub(crate) fn block_on_test_io<F: std::future::Future>(
    cx: &fsqlite_types::cx::Cx,
    future: F,
) -> F::Output {
    std::thread_local! {
        static TEST_IO_RUNTIME: asupersync::runtime::Runtime =
            asupersync::runtime::RuntimeBuilder::current_thread()
                .blocking_threads(1, 2)
                .build()
                .expect("VFS test runtime should build");
    }

    TEST_IO_RUNTIME.with(|runtime| {
        runtime.block_on(async {
            let native_cx = asupersync::Cx::current()
                .expect("VFS test runtime should install a capability context");
            cx.set_native_cx(native_cx);
            future.await
        })
    })
}

pub use memory::{MemoryFile, MemoryVfs, MemoryVfsConfig, MemoryVfsUsageSnapshot};
pub use metrics::{GLOBAL_VFS_METRICS, TracingFile, VfsMetrics};
#[cfg(all(feature = "native", any(unix, windows)))]
pub use namespace::{
    DatabaseNamespaceBinding, DatabaseNamespaceGenerationTransition,
    NamespaceGenerationTransitionOutcome, NamespaceOpenIntent, PendingNamespaceOpen,
    PreOpenLockSidecars, WindowsLockSidecarPolicy, begin_database_namespace_generation_transition,
    cleanup_abandoned_private_database, validate_reserved_database_artifacts,
};
pub use shm::ShmRegion;
pub use traits::{
    FileIdentity, ForeignWalParticipation, PrivateDatabaseCleanupDurability,
    PrivateDatabaseCleanupEntryReceipt, PrivateDatabaseCleanupEntryState,
    PrivateDatabaseCleanupFailure, PrivateDatabaseCleanupFailureStage,
    PrivateDatabaseCleanupOutcome, SyncKind, Vfs, VfsFile, VfsWriteCompletion,
    VfsWriteCompletionState, VfsWriteCompletionWait,
};
#[cfg(all(feature = "native", unix))]
pub use unix::{UnixFile, UnixVfs};
#[cfg(all(feature = "native", target_os = "linux"))]
pub use uring::{IoUringFile, IoUringVfs};
#[cfg(all(feature = "native", target_os = "windows"))]
pub use windows::{WindowsFile, WindowsVfs};
