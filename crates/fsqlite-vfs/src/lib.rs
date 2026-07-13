#![cfg_attr(windows, feature(windows_by_handle))]

pub mod memory;
pub mod metrics;
pub mod shm;
pub mod traits;
#[cfg(all(feature = "native", unix))]
pub mod unix;
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
    use std::io::Write as _;
    use std::path::{Path, PathBuf};

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

    pub fn read_dir_paths(dir: &Path) -> Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            out.push(entry?.path());
        }
        Ok(out)
    }

    pub fn write(path: &Path, bytes: impl AsRef<[u8]>) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(path, bytes)?;
        Ok(())
    }

    pub fn copy_file(from: &Path, to: &Path) -> Result<u64> {
        if let Some(parent) = to.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        Ok(std::fs::copy(from, to)?)
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
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
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

pub use memory::{MemoryFile, MemoryVfs, MemoryVfsConfig, MemoryVfsUsageSnapshot};
pub use metrics::{GLOBAL_VFS_METRICS, TracingFile, VfsMetrics};
pub use shm::ShmRegion;
pub use traits::{AsyncVfsDataPath, FileIdentity, SyncKind, Vfs, VfsFile};
#[cfg(all(feature = "native", unix))]
pub use unix::{UnixFile, UnixVfs};
#[cfg(all(feature = "native", target_os = "linux"))]
pub use uring::{IoUringFile, IoUringVfs};
#[cfg(all(feature = "native", target_os = "windows"))]
pub use windows::{WindowsFile, WindowsVfs};
