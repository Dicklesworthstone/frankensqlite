//! bd-2bjaf: Corruption fingerprint extractor and normalizer.
//!
//! Parses SQLite `PRAGMA integrity_check` output into structured failures,
//! then normalizes into stable signatures for grouping identical corruption
//! modes across different databases and runs.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{self, Write as _};
use std::fs::{self, File};
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FailureKind {
    PageDoubleReference {
        ref_count: u32,
    },
    RowidOutOfOrder,
    FreeSpaceCorruption,
    ExtendsOffEnd,
    OffsetOutOfRange {
        offset: u32,
        range_lo: u32,
        range_hi: u32,
    },
    ChildPageDepthDiffers,
    MultipleUsesForByte,
    FreelistLeafCountTooBig,
    PtrmapReadFailed,
    PageReadFailed,
    PageNeverUsed,
    PagePointerMapReferenced,
    InvalidPageNumber,
    Other(String),
}

impl fmt::Display for FailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PageDoubleReference { ref_count } => {
                write!(f, "page_double_ref(count={ref_count})")
            }
            Self::RowidOutOfOrder => write!(f, "rowid_out_of_order"),
            Self::FreeSpaceCorruption => write!(f, "free_space_corruption"),
            Self::ExtendsOffEnd => write!(f, "extends_off_end"),
            Self::OffsetOutOfRange {
                offset,
                range_lo,
                range_hi,
            } => {
                write!(f, "offset_out_of_range({offset},{range_lo}..{range_hi})")
            }
            Self::ChildPageDepthDiffers => write!(f, "child_page_depth_differs"),
            Self::MultipleUsesForByte => write!(f, "multiple_uses_for_byte"),
            Self::FreelistLeafCountTooBig => write!(f, "freelist_leaf_count_too_big"),
            Self::PtrmapReadFailed => write!(f, "ptrmap_read_failed"),
            Self::PageReadFailed => write!(f, "page_read_failed"),
            Self::PageNeverUsed => write!(f, "page_never_used"),
            Self::PagePointerMapReferenced => write!(f, "page_pointer_map_referenced"),
            Self::InvalidPageNumber => write!(f, "invalid_page_number"),
            Self::Other(s) => write!(f, "other({s})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub tree_id: Option<u32>,
    pub page: Option<u32>,
    pub cell: Option<u32>,
    pub kind: FailureKind,
    pub referenced_page: Option<u32>,
    pub raw_line: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TreeIdClass {
    Small,
    Medium,
    Large,
}

impl TreeIdClass {
    #[must_use]
    pub fn from_id(id: u32) -> Self {
        match id {
            0..=10 => Self::Small,
            11..=100 => Self::Medium,
            _ => Self::Large,
        }
    }
}

impl fmt::Display for TreeIdClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Small => write!(f, "small"),
            Self::Medium => write!(f, "medium"),
            Self::Large => write!(f, "large"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RefCountClass {
    Single,
    Few,
    Many,
}

impl RefCountClass {
    #[must_use]
    pub fn from_count(n: u32) -> Self {
        match n {
            0..=1 => Self::Single,
            2..=5 => Self::Few,
            _ => Self::Many,
        }
    }
}

impl fmt::Display for RefCountClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single => write!(f, "single"),
            Self::Few => write!(f, "few"),
            Self::Many => write!(f, "many"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NormalizedSignature {
    pub tree_id_class: Option<TreeIdClass>,
    pub kind: FailureKind,
    pub ref_count_class: Option<RefCountClass>,
    pub has_cell: bool,
}

impl fmt::Display for NormalizedSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref tc) = self.tree_id_class {
            write!(f, "tree:{tc}/")?;
        }
        write!(f, "{}", self.kind)?;
        if self.has_cell {
            write!(f, "/cell")?;
        }
        if let Some(ref rc) = self.ref_count_class {
            write!(f, "/refs:{rc}")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ParseError {
    pub line_number: usize,
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line_number, self.message)
    }
}

impl std::error::Error for ParseError {}

#[must_use]
pub fn parse_failure(line: &str) -> Option<Failure> {
    let trimmed = line.trim();

    if trimmed.is_empty() || trimmed == "ok" || trimmed.starts_with("***") {
        return None;
    }

    if let Some(f) = try_parse_tree_page_cell(trimmed) {
        return Some(f);
    }
    if let Some(f) = try_parse_tree_page(trimmed) {
        return Some(f);
    }
    if let Some(f) = try_parse_freelist(trimmed) {
        return Some(f);
    }
    if let Some(f) = try_parse_page_level(trimmed) {
        return Some(f);
    }
    if let Some(f) = try_parse_bare(trimmed) {
        return Some(f);
    }

    Some(Failure {
        tree_id: None,
        page: None,
        cell: None,
        kind: FailureKind::Other(trimmed.to_owned()),
        referenced_page: None,
        raw_line: trimmed.to_owned(),
    })
}

fn try_parse_tree_page_cell(line: &str) -> Option<Failure> {
    // "Tree N page P cell C: <message>"
    let rest = line.strip_prefix("Tree ")?;
    let (tree_str, rest) = rest.split_once(' ')?;
    let tree_id: u32 = tree_str.parse().ok()?;
    let rest = rest.strip_prefix("page ")?;
    let (page_str, rest) = rest.split_once(' ')?;
    let page: u32 = page_str.parse().ok()?;
    let rest = rest.strip_prefix("cell ")?;
    let (cell_str, rest) = rest.split_once(':')?;
    let cell: u32 = cell_str.parse().ok()?;
    let msg = rest.trim();

    let (kind, ref_page) = parse_message(msg);

    Some(Failure {
        tree_id: Some(tree_id),
        page: Some(page),
        cell: Some(cell),
        kind,
        referenced_page: ref_page,
        raw_line: line.to_owned(),
    })
}

fn try_parse_tree_page(line: &str) -> Option<Failure> {
    // "Tree N page P: <message>" or "Tree N page P right child: <message>"
    let rest = line.strip_prefix("Tree ")?;
    let (tree_str, rest) = rest.split_once(' ')?;
    let tree_id: u32 = tree_str.parse().ok()?;
    let rest = rest.strip_prefix("page ")?;

    let (page_str, rest) = if let Some((p, r)) = rest.split_once(':') {
        let p = p.trim().trim_end_matches(" right child");
        (p, r)
    } else {
        return None;
    };

    let page: u32 = page_str.parse().ok()?;
    let msg = rest.trim();
    let (kind, ref_page) = parse_message(msg);

    Some(Failure {
        tree_id: Some(tree_id),
        page: Some(page),
        cell: None,
        kind,
        referenced_page: ref_page,
        raw_line: line.to_owned(),
    })
}

fn try_parse_freelist(line: &str) -> Option<Failure> {
    let rest = line.strip_prefix("Freelist: ")?;
    let (kind, ref_page) = parse_message(rest.trim());

    Some(Failure {
        tree_id: None,
        page: None,
        cell: None,
        kind,
        referenced_page: ref_page,
        raw_line: line.to_owned(),
    })
}

fn try_parse_page_level(line: &str) -> Option<Failure> {
    // "Page N: <message>"
    let rest = line.strip_prefix("Page ")?;
    let (page_str, rest) = rest.split_once(':')?;
    let page: u32 = page_str.trim().parse().ok()?;
    let msg = rest.trim();

    let kind = if msg.starts_with("never used") {
        FailureKind::PageNeverUsed
    } else if msg.starts_with("pointer map referenced") {
        FailureKind::PagePointerMapReferenced
    } else if msg.starts_with("Multiple uses for byte") {
        FailureKind::MultipleUsesForByte
    } else {
        FailureKind::Other(msg.to_owned())
    };

    Some(Failure {
        tree_id: None,
        page: Some(page),
        cell: None,
        kind,
        referenced_page: None,
        raw_line: line.to_owned(),
    })
}

fn try_parse_bare(line: &str) -> Option<Failure> {
    if line.starts_with("invalid page number") {
        let num = line
            .strip_prefix("invalid page number ")?
            .trim()
            .parse::<u32>()
            .ok();
        return Some(Failure {
            tree_id: None,
            page: num,
            cell: None,
            kind: FailureKind::InvalidPageNumber,
            referenced_page: None,
            raw_line: line.to_owned(),
        });
    }
    None
}

fn parse_message(msg: &str) -> (FailureKind, Option<u32>) {
    if let Some(rest) = msg.strip_prefix("2nd reference to page ") {
        let page: u32 = rest.trim().parse().unwrap_or(0);
        return (
            FailureKind::PageDoubleReference { ref_count: 2 },
            Some(page),
        );
    }

    if msg.contains("reference to page") {
        let ref_count = msg
            .split_whitespace()
            .next()
            .and_then(|s| {
                s.strip_suffix("th")
                    .or_else(|| s.strip_suffix("rd"))
                    .or_else(|| s.strip_suffix("nd"))
                    .or_else(|| s.strip_suffix("st"))
            })
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(2);
        let page = msg.rsplit(' ').next().and_then(|s| s.parse::<u32>().ok());
        return (FailureKind::PageDoubleReference { ref_count }, page);
    }

    if msg.starts_with("Rowid") && msg.contains("out of order") {
        return (FailureKind::RowidOutOfOrder, None);
    }
    if msg.starts_with("free space corruption") {
        return (FailureKind::FreeSpaceCorruption, None);
    }
    if msg == "Extends off end of page" {
        return (FailureKind::ExtendsOffEnd, None);
    }
    if msg.starts_with("Offset") && msg.contains("out of range") {
        let nums: Vec<u32> = msg
            .split(|c: char| !c.is_ascii_digit())
            .filter_map(|s| s.parse().ok())
            .collect();
        let (offset, lo, hi) = match nums.len() {
            3.. => (nums[0], nums[1], nums[2]),
            _ => (0, 0, 0),
        };
        return (
            FailureKind::OffsetOutOfRange {
                offset,
                range_lo: lo,
                range_hi: hi,
            },
            None,
        );
    }
    if msg == "Child page depth differs" {
        return (FailureKind::ChildPageDepthDiffers, None);
    }
    if msg.starts_with("freelist leaf count too big") {
        return (FailureKind::FreelistLeafCountTooBig, None);
    }
    if msg.starts_with("Failed to read ptrmap") {
        return (FailureKind::PtrmapReadFailed, None);
    }
    if msg.starts_with("failed to get page") {
        return (FailureKind::PageReadFailed, None);
    }

    (FailureKind::Other(msg.to_owned()), None)
}

#[must_use]
pub fn normalize(failure: &Failure) -> NormalizedSignature {
    let tree_id_class = failure.tree_id.map(TreeIdClass::from_id);

    let ref_count_class = match &failure.kind {
        FailureKind::PageDoubleReference { ref_count } => {
            Some(RefCountClass::from_count(ref_count.saturating_sub(1)))
        }
        _ => None,
    };

    let kind = match &failure.kind {
        FailureKind::OffsetOutOfRange { .. } => FailureKind::OffsetOutOfRange {
            offset: 0,
            range_lo: 0,
            range_hi: 0,
        },
        other => other.clone(),
    };

    NormalizedSignature {
        tree_id_class,
        kind,
        ref_count_class,
        has_cell: failure.cell.is_some(),
    }
}

pub fn fingerprint(failure_text: &str) -> Result<NormalizedSignature, ParseError> {
    match parse_failure(failure_text) {
        Some(f) => Ok(normalize(&f)),
        None => Err(ParseError {
            line_number: 0,
            message: "not a failure line".to_owned(),
        }),
    }
}

#[must_use]
pub fn fingerprint_collection(
    failure_lines: &[&str],
) -> HashMap<NormalizedSignature, Vec<Failure>> {
    let mut map: HashMap<NormalizedSignature, Vec<Failure>> = HashMap::new();
    for line in failure_lines {
        if let Some(failure) = parse_failure(line) {
            let sig = normalize(&failure);
            map.entry(sig).or_default().push(failure);
        }
    }
    map
}

#[must_use]
pub fn parse_integrity_check_output(output: &str) -> Vec<Failure> {
    output.lines().filter_map(parse_failure).collect()
}

#[must_use]
pub fn inventory_report(output: &str) -> String {
    let failures = parse_integrity_check_output(output);
    if failures.is_empty() {
        return "No integrity check failures found.\n".to_owned();
    }

    let mut grouped: HashMap<NormalizedSignature, Vec<&Failure>> = HashMap::new();
    for f in &failures {
        let sig = normalize(f);
        grouped.entry(sig).or_default().push(f);
    }

    let mut sigs: Vec<_> = grouped.into_iter().collect();
    sigs.sort_by_key(|(_, examples)| std::cmp::Reverse(examples.len()));

    let mut report = String::new();
    let _ = write!(
        report,
        "# Corruption Fingerprint Inventory\n\nTotal failures: {}\nDistinct signatures: {}\n\n",
        failures.len(),
        sigs.len()
    );

    for (i, (sig, examples)) in sigs.iter().enumerate() {
        let _ = write!(
            report,
            "## Signature {} — {} ({} occurrences)\n\n",
            i + 1,
            sig,
            examples.len()
        );

        let trees: Vec<_> = examples
            .iter()
            .filter_map(|f| f.tree_id)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        if !trees.is_empty() {
            let _ = writeln!(report, "Trees: {trees:?}");
        }

        let pages: Vec<_> = examples
            .iter()
            .filter_map(|f| f.page)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        if !pages.is_empty() {
            let _ = writeln!(report, "Pages: {pages:?}");
        }

        if examples.len() <= 3 {
            report.push_str("\nExamples:\n");
            for ex in examples {
                let _ = writeln!(report, "  {}", ex.raw_line);
            }
        } else {
            let _ = write!(report, "\nFirst 3 of {}:\n", examples.len());
            for ex in examples.iter().take(3) {
                let _ = writeln!(report, "  {}", ex.raw_line);
            }
        }
        report.push('\n');
    }

    report
}

/// A stable metadata snapshot used to prove that artifact classification did
/// not create, truncate, or rewrite the database or any standard sidecar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactFileSnapshot {
    pub path: PathBuf,
    pub exists: bool,
    pub length: Option<u64>,
    pub modified_unix_nanos: Option<u128>,
}

/// Parsed metadata for a SQLite sidecar observed next to an artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactSidecar {
    pub kind: String,
    pub snapshot: ArtifactFileSnapshot,
    pub header_hex: Option<String>,
    pub wal: Option<WalSidecarMetadata>,
}

/// Non-mutating WAL-header facts. A non-zero `trailing_bytes` value means the
/// file length is not an exact number of complete frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WalSidecarMetadata {
    pub magic: u32,
    pub format_version: u32,
    pub page_size: u32,
    pub checkpoint_sequence: u32,
    pub salt: [u32; 2],
    pub frame_count: u64,
    pub trailing_bytes: u64,
}

/// Pages attributed to one schema object by SQLite's read-only `dbstat`
/// virtual table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BtreeOwnership {
    pub name: String,
    pub object_type: String,
    pub root_page: u32,
    pub height: u32,
    pub pages: Vec<u32>,
    pub overflow_pages: Vec<u32>,
}

/// Logical comparison of a direct-column, non-partial rowid-table index with
/// the table rows that should populate it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexConsistency {
    pub index_name: String,
    pub table_name: String,
    pub comparable: bool,
    pub reason: Option<String>,
    pub query_plan: Option<String>,
    pub table_entry_count: usize,
    pub index_entry_count: usize,
    pub missing_from_index: Vec<String>,
    pub extra_in_index: Vec<String>,
}

/// Best-effort raw inspection of a page that belongs to neither a schema
/// b-tree nor the committed freelist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrphanPageProbe {
    pub page: u32,
    pub btree_page_type: Option<String>,
    pub header_hex: String,
    pub matching_target_key_fragments: Vec<String>,
}

/// Read-only structural report for a closed SQLite artifact.
///
/// The report deliberately separates raw header facts, SQLite's own `dbstat`
/// ownership view, and best-effort probes. This makes it useful on historical
/// corruption without turning an inference into a claimed root cause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SqliteArtifactClassification {
    pub database: PathBuf,
    pub page_size: u32,
    pub page_count: u32,
    pub header_page_count: u32,
    pub pragma_page_count: u32,
    pub first_freelist_trunk: u32,
    pub header_freelist_count: u32,
    pub pragma_freelist_count: u32,
    pub freelist_pages: Vec<u32>,
    pub pointer_map_pages: Vec<u32>,
    pub ownership: Vec<BtreeOwnership>,
    pub multiply_owned_pages: BTreeMap<u32, Vec<String>>,
    pub orphan_pages: Vec<u32>,
    pub orphan_probes: Vec<OrphanPageProbe>,
    pub target_index: Option<IndexConsistency>,
    pub integrity_check_lines: Vec<String>,
    pub integrity_check_error: Option<String>,
    pub sidecars: Vec<ArtifactSidecar>,
    pub warnings: Vec<String>,
    pub input_files_unchanged: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactClassificationError {
    #[error("artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("artifact SQLite inspection failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("invalid SQLite artifact: {0}")]
    Invalid(String),
}

#[derive(Debug, Default)]
struct OwnershipBuilder {
    object_type: String,
    root_page: u32,
    height: u32,
    pages: BTreeSet<u32>,
    overflow_pages: BTreeSet<u32>,
}

fn read_u32_be(bytes: &[u8], offset: usize) -> Option<u32> {
    let value = bytes.get(offset..offset + 4)?;
    Some(u32::from_be_bytes(value.try_into().ok()?))
}

fn snapshot_file(path: &Path) -> ArtifactFileSnapshot {
    let metadata = fs::metadata(path).ok();
    ArtifactFileSnapshot {
        path: path.to_path_buf(),
        exists: metadata.is_some(),
        length: metadata.as_ref().map(fs::Metadata::len),
        modified_unix_nanos: metadata
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos()),
    }
}

fn sidecar_path(database: &Path, suffix: &str) -> PathBuf {
    let mut path = database.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

fn read_prefix(path: &Path, limit: usize) -> Result<Vec<u8>, std::io::Error> {
    let mut file = File::open(path)?;
    let mut bytes = vec![0; limit];
    let count = file.read(&mut bytes)?;
    bytes.truncate(count);
    Ok(bytes)
}

fn classify_sidecar(kind: &str, path: &Path) -> ArtifactSidecar {
    let snapshot = snapshot_file(path);
    let header = snapshot
        .exists
        .then(|| read_prefix(path, 64).ok())
        .flatten();
    let header_hex = header.as_ref().map(crate::bytes_to_lower_hex);
    let wal = if kind == "wal" {
        header
            .as_deref()
            .and_then(|bytes| parse_wal_sidecar(bytes, snapshot.length.unwrap_or_default()))
    } else {
        None
    };
    ArtifactSidecar {
        kind: kind.to_owned(),
        snapshot,
        header_hex,
        wal,
    }
}

fn parse_wal_sidecar(header: &[u8], length: u64) -> Option<WalSidecarMetadata> {
    if header.len() < 32 {
        return None;
    }
    let page_size = read_u32_be(header, 8)?;
    let page_size = if page_size == 0 { 65_536 } else { page_size };
    let frame_size = u64::from(page_size).checked_add(24)?;
    let payload_length = length.saturating_sub(32);
    Some(WalSidecarMetadata {
        magic: read_u32_be(header, 0)?,
        format_version: read_u32_be(header, 4)?,
        page_size,
        checkpoint_sequence: read_u32_be(header, 12)?,
        salt: [read_u32_be(header, 16)?, read_u32_be(header, 20)?],
        frame_count: payload_length / frame_size,
        trailing_bytes: payload_length % frame_size,
    })
}

fn sqlite_immutable_uri(path: &Path) -> String {
    let mut uri = String::from("file:");
    for byte in path.to_string_lossy().bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-' | b'~' | b':')
        {
            uri.push(char::from(byte));
        } else {
            let _ = write!(uri, "%{byte:02X}");
        }
    }
    uri.push_str("?immutable=1");
    uri
}

fn read_database_page(
    file: &mut File,
    page_size: u32,
    page: u32,
) -> Result<Vec<u8>, std::io::Error> {
    let offset = u64::from(page.saturating_sub(1)) * u64::from(page_size);
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0; page_size as usize];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn walk_freelist(
    file: &mut File,
    page_size: u32,
    usable_size: u32,
    page_count: u32,
    first_trunk: u32,
    declared_count: u32,
) -> (BTreeSet<u32>, Vec<String>) {
    let mut pages = BTreeSet::new();
    let mut warnings = Vec::new();
    let mut trunk = first_trunk;
    let max_leaf_count = usable_size.saturating_div(4).saturating_sub(2);

    while trunk != 0 {
        if trunk > page_count {
            warnings.push(format!(
                "freelist trunk page {trunk} exceeds page_count {page_count}"
            ));
            break;
        }
        if !pages.insert(trunk) {
            warnings.push(format!("freelist trunk cycle or duplicate at page {trunk}"));
            break;
        }
        let bytes = match read_database_page(file, page_size, trunk) {
            Ok(bytes) => bytes,
            Err(error) => {
                warnings.push(format!("failed to read freelist trunk {trunk}: {error}"));
                break;
            }
        };
        let next = read_u32_be(&bytes, 0).unwrap_or_default();
        let leaf_count = read_u32_be(&bytes, 4).unwrap_or_default();
        if leaf_count > max_leaf_count {
            warnings.push(format!(
                "freelist trunk {trunk} claims {leaf_count} leaves; maximum is {max_leaf_count}"
            ));
            break;
        }
        for index in 0..leaf_count {
            let offset = 8 + index as usize * 4;
            let leaf = read_u32_be(&bytes, offset).unwrap_or_default();
            if leaf == 0 || leaf > page_count {
                warnings.push(format!(
                    "freelist trunk {trunk} contains out-of-range leaf page {leaf}"
                ));
            } else if !pages.insert(leaf) {
                warnings.push(format!("freelist page {leaf} is referenced more than once"));
            }
        }
        if pages.len() > page_count as usize {
            warnings.push("freelist walk exceeded database page count".to_owned());
            break;
        }
        trunk = next;
    }

    if pages.len() != declared_count as usize {
        warnings.push(format!(
            "freelist header count {declared_count} differs from walked count {}",
            pages.len()
        ));
    }
    (pages, warnings)
}

fn pointer_map_pages(page_size: u32, usable_size: u32, page_count: u32) -> BTreeSet<u32> {
    let mut pages = BTreeSet::new();
    let stride = usable_size.saturating_div(5).saturating_add(1);
    if stride <= 1 {
        return pages;
    }
    let pending_byte_page = 0x4000_0000_u64 / u64::from(page_size) + 1;
    let mut page = 2_u64;
    while page <= u64::from(page_count) {
        if page == pending_byte_page {
            page = page.saturating_add(1);
        }
        if page <= u64::from(page_count) {
            if let Ok(page) = u32::try_from(page) {
                pages.insert(page);
            }
        }
        page = page.saturating_add(u64::from(stride));
    }
    pages
}

fn schema_objects(
    connection: &Connection,
) -> Result<BTreeMap<String, (String, String, u32, String)>, rusqlite::Error> {
    let mut statement = connection.prepare(
        "SELECT name,type,tbl_name,rootpage,coalesce(sql,'') FROM sqlite_schema \
         WHERE rootpage > 0 ORDER BY name",
    )?;
    statement
        .query_map([], |row| {
            let name: String = row.get(0)?;
            Ok((
                name,
                (row.get(1)?, row.get(2)?, row.get::<_, u32>(3)?, row.get(4)?),
            ))
        })?
        .collect()
}

fn collect_ownership(
    connection: &Connection,
    schema: &BTreeMap<String, (String, String, u32, String)>,
) -> Result<(Vec<BtreeOwnership>, BTreeMap<u32, Vec<String>>), rusqlite::Error> {
    let mut builders = BTreeMap::<String, OwnershipBuilder>::new();
    builders.insert(
        "sqlite_schema".to_owned(),
        OwnershipBuilder {
            object_type: "table".to_owned(),
            root_page: 1,
            ..OwnershipBuilder::default()
        },
    );
    for (name, (object_type, _, root_page, _)) in schema {
        builders.insert(
            name.clone(),
            OwnershipBuilder {
                object_type: object_type.clone(),
                root_page: *root_page,
                ..OwnershipBuilder::default()
            },
        );
    }

    let mut statement = connection.prepare(
        "SELECT name,path,pageno,pagetype FROM dbstat ORDER BY name,pageno",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let path: String = row.get(1)?;
        let page: u32 = row.get(2)?;
        let page_type: String = row.get(3)?;
        let builder = builders.entry(name).or_default();
        builder.pages.insert(page);
        if page_type == "overflow" {
            builder.overflow_pages.insert(page);
        } else {
            let depth = u32::try_from(path.split('/').filter(|part| !part.is_empty()).count())
                .unwrap_or(u32::MAX);
            builder.height = builder.height.max(depth.saturating_add(1));
        }
    }

    let mut owners = BTreeMap::<u32, Vec<String>>::new();
    let ownership = builders
        .into_iter()
        .map(|(name, builder)| {
            for page in &builder.pages {
                owners.entry(*page).or_default().push(name.clone());
            }
            BtreeOwnership {
                name,
                object_type: builder.object_type,
                root_page: builder.root_page,
                height: builder.height,
                pages: builder.pages.into_iter().collect(),
                overflow_pages: builder.overflow_pages.into_iter().collect(),
            }
        })
        .collect();
    Ok((ownership, owners))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn encode_value(value: ValueRef<'_>, fragments: &mut BTreeSet<Vec<u8>>) -> String {
    match value {
        ValueRef::Null => "null".to_owned(),
        ValueRef::Integer(value) => format!("integer:{value}"),
        ValueRef::Real(value) => format!("real:{:016x}", value.to_bits()),
        ValueRef::Text(value) => {
            if (4..=64).contains(&value.len()) {
                fragments.insert(value.to_vec());
            }
            format!("text:{}:{}", value.len(), crate::bytes_to_lower_hex(value))
        }
        ValueRef::Blob(value) => {
            if (4..=64).contains(&value.len()) {
                fragments.insert(value.to_vec());
            }
            format!("blob:{}:{}", value.len(), crate::bytes_to_lower_hex(value))
        }
    }
}

fn collect_index_entries(
    connection: &Connection,
    sql: &str,
    column_count: usize,
) -> Result<(Vec<String>, BTreeSet<Vec<u8>>), rusqlite::Error> {
    let mut statement = connection.prepare(sql)?;
    let mut rows = statement.query([])?;
    let mut entries = Vec::new();
    let mut fragments = BTreeSet::new();
    while let Some(row) = rows.next()? {
        let mut encoded = Vec::with_capacity(column_count);
        for column in 0..column_count {
            encoded.push(encode_value(row.get_ref(column)?, &mut fragments));
        }
        entries.push(encoded.join("|"));
    }
    entries.sort();
    Ok((entries, fragments))
}

fn multiset_difference(left: &[String], right: &[String]) -> Vec<String> {
    let mut right_counts = BTreeMap::<&str, usize>::new();
    for entry in right {
        *right_counts.entry(entry).or_default() += 1;
    }
    let mut difference = Vec::new();
    for entry in left {
        match right_counts.get_mut(entry.as_str()) {
            Some(count) if *count > 0 => *count -= 1,
            _ => difference.push(entry.clone()),
        }
    }
    difference
}

fn incomparable_index(index_name: &str, table_name: &str, reason: String) -> IndexConsistency {
    IndexConsistency {
        index_name: index_name.to_owned(),
        table_name: table_name.to_owned(),
        comparable: false,
        reason: Some(reason),
        query_plan: None,
        table_entry_count: 0,
        index_entry_count: 0,
        missing_from_index: Vec::new(),
        extra_in_index: Vec::new(),
    }
}

fn compare_target_index(
    connection: &Connection,
    schema: &BTreeMap<String, (String, String, u32, String)>,
    index_name: &str,
) -> Result<(IndexConsistency, BTreeSet<Vec<u8>>), rusqlite::Error> {
    let Some((object_type, table_name, _, _)) = schema.get(index_name) else {
        return Ok((
            incomparable_index(index_name, "", "index is absent from sqlite_schema".to_owned()),
            BTreeSet::new(),
        ));
    };
    if object_type != "index" {
        return Ok((
            incomparable_index(
                index_name,
                table_name,
                format!("schema object has type {object_type}, not index"),
            ),
            BTreeSet::new(),
        ));
    }
    let table_sql = schema
        .get(table_name)
        .map(|(_, _, _, sql)| sql.as_str())
        .unwrap_or_default();
    if table_sql.to_ascii_uppercase().contains("WITHOUT ROWID") {
        return Ok((
            incomparable_index(
                index_name,
                table_name,
                "WITHOUT ROWID index comparison is not yet supported".to_owned(),
            ),
            BTreeSet::new(),
        ));
    }

    let partial: i64 = connection
        .query_row(
            "SELECT partial FROM pragma_index_list(?1) WHERE name=?2",
            [table_name, index_name],
            |row| row.get(0),
        )
        .unwrap_or(0);
    if partial != 0 {
        return Ok((
            incomparable_index(
                index_name,
                table_name,
                "partial index requires predicate-aware comparison".to_owned(),
            ),
            BTreeSet::new(),
        ));
    }

    let pragma = format!("PRAGMA index_xinfo({})", quote_identifier(index_name));
    let mut statement = connection.prepare(&pragma)?;
    let columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(5)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut key_columns = Vec::new();
    for (column_id, name, is_key) in columns {
        if is_key == 0 {
            continue;
        }
        let Some(name) = name else {
            return Ok((
                incomparable_index(
                    index_name,
                    table_name,
                    "expression index requires expression-aware comparison".to_owned(),
                ),
                BTreeSet::new(),
            ));
        };
        if column_id < 0 {
            return Ok((
                incomparable_index(
                    index_name,
                    table_name,
                    "index key contains a non-column term".to_owned(),
                ),
                BTreeSet::new(),
            ));
        }
        key_columns.push(name);
    }
    if key_columns.is_empty() {
        return Ok((
            incomparable_index(
                index_name,
                table_name,
                "index has no direct key columns".to_owned(),
            ),
            BTreeSet::new(),
        ));
    }

    let projection = key_columns
        .iter()
        .map(|column| quote_identifier(column))
        .chain(std::iter::once("rowid".to_owned()))
        .collect::<Vec<_>>()
        .join(",");
    let table = quote_identifier(table_name);
    let index = quote_identifier(index_name);
    let table_sql = format!("SELECT {projection} FROM {table} NOT INDEXED");
    let index_sql = format!("SELECT {projection} FROM {table} INDEXED BY {index}");
    let plan_sql = format!("EXPLAIN QUERY PLAN {index_sql}");
    let query_plan = connection
        .prepare(&plan_sql)?
        .query_map([], |row| row.get::<_, String>(3))?
        .collect::<Result<Vec<_>, _>>()?
        .join("; ");
    let column_count = key_columns.len() + 1;
    let (table_entries, table_fragments) =
        collect_index_entries(connection, &table_sql, column_count)?;
    let (index_entries, index_fragments) =
        collect_index_entries(connection, &index_sql, column_count)?;
    let mut fragments = table_fragments;
    fragments.extend(index_fragments);
    let missing_from_index = multiset_difference(&table_entries, &index_entries);
    let extra_in_index = multiset_difference(&index_entries, &table_entries);
    Ok((
        IndexConsistency {
            index_name: index_name.to_owned(),
            table_name: table_name.to_owned(),
            comparable: true,
            reason: None,
            query_plan: Some(query_plan),
            table_entry_count: table_entries.len(),
            index_entry_count: index_entries.len(),
            missing_from_index,
            extra_in_index,
        },
        fragments,
    ))
}

fn page_type_name(page: &[u8], page_number: u32) -> Option<String> {
    let offset = if page_number == 1 { 100 } else { 0 };
    match page.get(offset).copied()? {
        0x02 => Some("interior-index".to_owned()),
        0x05 => Some("interior-table".to_owned()),
        0x0a => Some("leaf-index".to_owned()),
        0x0d => Some("leaf-table".to_owned()),
        _ => None,
    }
}

fn probe_orphan_pages(
    file: &mut File,
    page_size: u32,
    orphan_pages: &[u32],
    target_fragments: &BTreeSet<Vec<u8>>,
    warnings: &mut Vec<String>,
) -> Vec<OrphanPageProbe> {
    let mut probes = Vec::new();
    for &page in orphan_pages {
        let bytes = match read_database_page(file, page_size, page) {
            Ok(bytes) => bytes,
            Err(error) => {
                warnings.push(format!("failed to probe orphan page {page}: {error}"));
                continue;
            }
        };
        let matching_target_key_fragments = target_fragments
            .iter()
            .filter(|fragment| {
                bytes
                    .windows(fragment.len())
                    .any(|window| window == fragment.as_slice())
            })
            .take(16)
            .map(|fragment| crate::bytes_to_lower_hex(fragment))
            .collect();
        probes.push(OrphanPageProbe {
            page,
            btree_page_type: page_type_name(&bytes, page),
            header_hex: crate::bytes_to_lower_hex(&bytes[..bytes.len().min(32)]),
            matching_target_key_fragments,
        });
    }
    probes
}

fn collect_integrity_check(connection: &Connection) -> (Vec<String>, Option<String>) {
    let mut lines = Vec::new();
    let mut statement = match connection.prepare("PRAGMA integrity_check") {
        Ok(statement) => statement,
        Err(error) => return (lines, Some(error.to_string())),
    };
    let mut rows = match statement.query([]) {
        Ok(rows) => rows,
        Err(error) => return (lines, Some(error.to_string())),
    };
    loop {
        match rows.next() {
            Ok(Some(row)) => match row.get(0) {
                Ok(line) => lines.push(line),
                Err(error) => return (lines, Some(error.to_string())),
            },
            Ok(None) => return (lines, None),
            Err(error) => return (lines, Some(error.to_string())),
        }
    }
}

/// Classify a closed SQLite database without mutating it.
///
/// SQLite is opened through an `immutable=1` URI, and the database plus
/// `-wal`, `-shm`, and `-journal` paths are snapshotted before and after the
/// inspection. `target_index` enables a direct-column table/index comparison
/// and best-effort raw orphan-page fragment matching.
pub fn classify_sqlite_artifact(
    database: &Path,
    target_index: Option<&str>,
) -> Result<SqliteArtifactClassification, ArtifactClassificationError> {
    let database = fs::canonicalize(database)?;
    let wal_path = sidecar_path(&database, "-wal");
    let shm_path = sidecar_path(&database, "-shm");
    let journal_path = sidecar_path(&database, "-journal");
    let observed_paths = [
        database.clone(),
        wal_path.clone(),
        shm_path.clone(),
        journal_path.clone(),
    ];
    let before = observed_paths
        .iter()
        .map(|path| snapshot_file(path))
        .collect::<Vec<_>>();
    let sidecars = vec![
        classify_sidecar("wal", &wal_path),
        classify_sidecar("shm", &shm_path),
        classify_sidecar("journal", &journal_path),
    ];

    let mut file = File::open(&database)?;
    let mut header = [0_u8; 100];
    file.read_exact(&mut header)?;
    if &header[..16] != b"SQLite format 3\0" {
        return Err(ArtifactClassificationError::Invalid(
            "missing SQLite format 3 header".to_owned(),
        ));
    }
    let encoded_page_size = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if encoded_page_size == 1 {
        65_536
    } else {
        u32::from(encoded_page_size)
    };
    if page_size < 512 || !page_size.is_power_of_two() {
        return Err(ArtifactClassificationError::Invalid(format!(
            "invalid page size {page_size}"
        )));
    }
    let reserved_bytes = u32::from(header[20]);
    let usable_size = page_size.saturating_sub(reserved_bytes);
    let header_page_count = read_u32_be(&header, 28).unwrap_or_default();
    let file_page_count = u32::try_from(file.metadata()?.len() / u64::from(page_size))
        .unwrap_or(u32::MAX);
    let page_count = if header_page_count == 0 {
        file_page_count
    } else {
        header_page_count
    };
    let first_freelist_trunk = read_u32_be(&header, 32).unwrap_or_default();
    let header_freelist_count = read_u32_be(&header, 36).unwrap_or_default();
    let largest_root_page = read_u32_be(&header, 52).unwrap_or_default();
    let (freelist_pages, mut warnings) = walk_freelist(
        &mut file,
        page_size,
        usable_size,
        page_count,
        first_freelist_trunk,
        header_freelist_count,
    );

    let uri = sqlite_immutable_uri(&database);
    let connection = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let pragma_page_count = connection.pragma_query_value(None, "page_count", |row| row.get(0))?;
    let pragma_freelist_count =
        connection.pragma_query_value(None, "freelist_count", |row| row.get(0))?;
    if header_page_count != 0 && header_page_count != pragma_page_count {
        warnings.push(format!(
            "header page_count {header_page_count} differs from PRAGMA page_count {pragma_page_count}"
        ));
    }
    if header_freelist_count != pragma_freelist_count {
        warnings.push(format!(
            "header freelist_count {header_freelist_count} differs from PRAGMA freelist_count {pragma_freelist_count}"
        ));
    }

    let schema = schema_objects(&connection)?;
    let (ownership, owners) = collect_ownership(&connection, &schema)?;
    let multiply_owned_pages = owners
        .iter()
        .filter(|(_, names)| names.len() > 1)
        .map(|(page, names)| (*page, names.clone()))
        .collect();
    let owned_pages = owners.keys().copied().collect::<BTreeSet<_>>();
    let pointer_map_pages = if largest_root_page == 0 {
        BTreeSet::new()
    } else {
        pointer_map_pages(page_size, usable_size, page_count)
    };
    let pending_byte_page = u32::try_from(0x4000_0000_u64 / u64::from(page_size) + 1).ok();
    let orphan_pages = (1..=page_count)
        .filter(|page| {
            !owned_pages.contains(page)
                && !freelist_pages.contains(page)
                && !pointer_map_pages.contains(page)
                && Some(*page) != pending_byte_page
        })
        .collect::<Vec<_>>();

    let (target_index, target_fragments) = match target_index {
        Some(index_name) => {
            let (comparison, fragments) =
                compare_target_index(&connection, &schema, index_name)?;
            (Some(comparison), fragments)
        }
        None => (None, BTreeSet::new()),
    };
    let (integrity_check_lines, integrity_check_error) = collect_integrity_check(&connection);
    let orphan_probes = probe_orphan_pages(
        &mut file,
        page_size,
        &orphan_pages,
        &target_fragments,
        &mut warnings,
    );
    drop(connection);
    drop(file);

    let after = observed_paths
        .iter()
        .map(|path| snapshot_file(path))
        .collect::<Vec<_>>();
    let input_files_unchanged = before == after;
    if !input_files_unchanged {
        warnings.push("artifact or sidecar metadata changed during classification".to_owned());
    }

    Ok(SqliteArtifactClassification {
        database,
        page_size,
        page_count,
        header_page_count,
        pragma_page_count,
        first_freelist_trunk,
        header_freelist_count,
        pragma_freelist_count,
        freelist_pages: freelist_pages.into_iter().collect(),
        pointer_map_pages: pointer_map_pages.into_iter().collect(),
        ownership,
        multiply_owned_pages,
        orphan_pages,
        orphan_probes,
        target_index,
        integrity_check_lines,
        integrity_check_error,
        sidecars,
        warnings,
        input_files_unchanged,
    })
}
