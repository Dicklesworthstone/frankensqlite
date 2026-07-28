//! Fixed-residency helpers for whole-image bounded validation.
//!
//! The semantic oracle keeps database-proportional state in anonymous private
//! files. It never mmaps those files and reads or writes them through fixed
//! windows, so resident memory does not grow with the database page count.

#![cfg(not(target_arch = "wasm32"))]

use std::fs::{File, Metadata};
use std::io;
use std::path::Path;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{PageNumber, PageSize};

pub(crate) const OWNERSHIP_SCAN_WINDOW_BYTES: usize = 64 * 1024;

#[cfg(unix)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    while !buf.is_empty() {
        let read = file.read_at(buf, offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short anonymous spool read",
            ));
        }
        offset = offset.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        buf = &mut buf[read..];
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    while !buf.is_empty() {
        let written = file.write_at(buf, offset)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short anonymous spool write",
            ));
        }
        offset = offset.saturating_add(u64::try_from(written).unwrap_or(u64::MAX));
        buf = &buf[written..];
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;

    while !buf.is_empty() {
        let read = file.seek_read(buf, offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short anonymous spool read",
            ));
        }
        offset = offset.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        buf = &mut buf[read..];
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;

    while !buf.is_empty() {
        let written = file.seek_write(buf, offset)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short anonymous spool write",
            ));
        }
        offset = offset.saturating_add(u64::try_from(written).unwrap_or(u64::MAX));
        buf = &buf[written..];
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
compile_error!("bounded whole-image validation requires positional file I/O");

fn validate_spool_parent(parent: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(parent)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(FrankenError::NotImplemented(format!(
            "bounded validation spool parent must be a real directory, not a symlink or non-directory: {}",
            parent.display()
        )));
    }
    Ok(())
}

fn validate_anonymous_regular_file(metadata: &Metadata) -> Result<()> {
    if !metadata.is_file() {
        return Err(FrankenError::NotImplemented(
            "bounded validation spool is not a regular file".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 0 {
            return Err(FrankenError::NotImplemented(
                "bounded validation spool retained a filesystem name or hard link".to_owned(),
            ));
        }
    }
    Ok(())
}

/// One byte per database page, stored in an anonymous private file.
///
/// Zero means unowned. Non-zero values are diagnostic owner classes. A second
/// mark is an exact duplicate/cycle/cross-tree ownership failure.
#[derive(Debug)]
pub(crate) struct PrivatePageOwnership {
    file: File,
    total_pages: u32,
    marked_pages: u64,
}

impl PrivatePageOwnership {
    pub(crate) fn create(parent: &Path, total_pages: u32) -> Result<Self> {
        validate_spool_parent(parent)?;
        let file = tempfile::tempfile_in(parent)?;
        validate_anonymous_regular_file(&file.metadata()?)?;
        file.set_len(u64::from(total_pages))?;
        Ok(Self {
            file,
            total_pages,
            marked_pages: 0,
        })
    }

    pub(crate) fn mark(
        &mut self,
        page_size: PageSize,
        page_no: PageNumber,
        owner_class: u8,
        owner: &str,
    ) -> Result<()> {
        debug_assert_ne!(owner_class, 0);
        if page_no.get() > self.total_pages {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "page {} referenced by {owner} lies past the end of the database (page_count={})",
                    page_no.get(),
                    self.total_pages
                ),
            });
        }
        if page_no.get() == fsqlite_pager::lock_byte_page(page_size) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "page {} referenced by {owner} is the reserved lock-byte page",
                    page_no.get()
                ),
            });
        }

        let offset = u64::from(page_no.get() - 1);
        let mut existing = [0_u8; 1];
        read_exact_at(&self.file, &mut existing, offset)?;
        if existing[0] != 0 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "page {} is referenced multiple times (prior owner class {}; {owner})",
                    page_no.get(),
                    existing[0]
                ),
            });
        }
        write_all_at(&self.file, &[owner_class], offset)?;
        self.marked_pages = self.marked_pages.saturating_add(1);
        Ok(())
    }

    pub(crate) fn first_unowned(&self, page_size: PageSize) -> Result<Option<PageNumber>> {
        let lock_byte_page = fsqlite_pager::lock_byte_page(page_size);
        let mut window = [0_u8; OWNERSHIP_SCAN_WINDOW_BYTES];
        let mut offset = 0_u64;
        let total = u64::from(self.total_pages);
        while offset < total {
            let remaining = usize::try_from(total - offset).unwrap_or(usize::MAX);
            let read_len = remaining.min(window.len());
            read_exact_at(&self.file, &mut window[..read_len], offset)?;
            for (index, marker) in window[..read_len].iter().copied().enumerate() {
                let raw_page = offset
                    .saturating_add(u64::try_from(index).unwrap_or(u64::MAX))
                    .saturating_add(1);
                let raw_page = u32::try_from(raw_page).map_err(|_| {
                    FrankenError::internal("bounded ownership page offset overflow")
                })?;
                if raw_page != lock_byte_page && marker == 0 {
                    return Ok(PageNumber::new(raw_page));
                }
            }
            offset = offset.saturating_add(u64::try_from(read_len).unwrap_or(u64::MAX));
        }
        Ok(None)
    }

    pub(crate) const fn spool_bytes(&self) -> u64 {
        self.total_pages as u64
    }

    pub(crate) const fn marked_pages(&self) -> u64 {
        self.marked_pages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hfdt_0117_ownership_spool_detects_duplicate_range_and_orphan_without_resident_map() {
        let dir = tempfile::tempdir().unwrap();
        let page_size = PageSize::new(4096).unwrap();
        let mut owners = PrivatePageOwnership::create(dir.path(), 4).unwrap();
        owners
            .mark(page_size, PageNumber::new(1).unwrap(), 1, "schema")
            .unwrap();
        owners
            .mark(page_size, PageNumber::new(3).unwrap(), 2, "table")
            .unwrap();
        let duplicate = owners
            .mark(page_size, PageNumber::new(3).unwrap(), 3, "index")
            .unwrap_err();
        assert!(matches!(duplicate, FrankenError::DatabaseCorrupt { .. }));
        let range = owners
            .mark(page_size, PageNumber::new(5).unwrap(), 2, "table")
            .unwrap_err();
        assert!(matches!(range, FrankenError::DatabaseCorrupt { .. }));
        assert_eq!(owners.first_unowned(page_size).unwrap().unwrap().get(), 2);
        assert_eq!(owners.spool_bytes(), 4);
        assert_eq!(owners.marked_pages(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn hfdt_0117_anonymous_spool_has_no_link_and_survives_parent_path_swap_with_clean_drop() {
        use std::os::unix::fs::MetadataExt;

        let outer = tempfile::tempdir().unwrap();
        let parent = outer.path().join("spool-parent");
        std::fs::create_dir(&parent).unwrap();
        let mut owners = PrivatePageOwnership::create(&parent, 2).unwrap();
        assert_eq!(owners.file.metadata().unwrap().nlink(), 0);
        assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 0);

        let moved = outer.path().join("spool-parent-moved");
        std::fs::rename(&parent, &moved).unwrap();
        std::fs::create_dir(&parent).unwrap();
        owners
            .mark(
                PageSize::new(4096).unwrap(),
                PageNumber::new(1).unwrap(),
                1,
                "schema",
            )
            .unwrap();
        drop(owners);
        assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 0);
        assert_eq!(std::fs::read_dir(&moved).unwrap().count(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn hfdt_0117_spool_refuses_symlink_and_nonregular_parent() {
        use std::os::unix::fs::symlink;

        let outer = tempfile::tempdir().unwrap();
        let real = outer.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let linked = outer.path().join("linked");
        symlink(&real, &linked).unwrap();
        let symlink_error = PrivatePageOwnership::create(&linked, 1).unwrap_err();
        assert!(matches!(symlink_error, FrankenError::NotImplemented(_)));

        let regular = outer.path().join("regular");
        std::fs::write(&regular, b"not a directory").unwrap();
        let nonregular_error = PrivatePageOwnership::create(&regular, 1).unwrap_err();
        assert!(matches!(nonregular_error, FrankenError::NotImplemented(_)));
    }
}
