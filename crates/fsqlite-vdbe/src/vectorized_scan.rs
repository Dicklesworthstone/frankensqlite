//! Vectorized table-scan source operator.
//!
//! This module implements the `bd-14vp7.2` scan source that reads rows from a
//! B-tree cursor, converts them into columnar [`Batch`](crate::vectorized::Batch)
//! values, supports page-range morsels, and applies early filter pushdown via
//! selection vectors.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use fsqlite_btree::{BtCursor, BtreeCursorOps, PageWriter};
use fsqlite_error::FrankenError;
use fsqlite_types::record::parse_record;
use fsqlite_types::value::SqliteValue;
use fsqlite_types::{Cx, PageNumber};

use crate::vectorized::{Batch, BatchFormatError, Column, ColumnData, ColumnSpec, SelectionVector};

/// Row predicate for scan-time filter pushdown.
pub type RowPredicate = Arc<dyn Fn(i64, &[SqliteValue]) -> bool + Send + Sync + 'static>;

/// Errors returned by [`VectorizedTableScan`].
#[derive(Debug)]
pub enum VectorizedScanError {
    Cursor(FrankenError),
    Batch(BatchFormatError),
    InvalidMorsel {
        start: PageNumber,
        end: PageNumber,
    },
    InvalidBatchCapacity(usize),
    RecordDecode {
        rowid: i64,
        payload_len: usize,
    },
    SelectionIndexOverflow(usize),
    OffsetOutOfBounds {
        column: String,
        row_idx: usize,
        start: usize,
        end: usize,
        data_len: usize,
    },
    InvalidUtf8 {
        column: String,
        row_idx: usize,
    },
}

impl fmt::Display for VectorizedScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cursor(err) => write!(f, "cursor error: {err}"),
            Self::Batch(err) => write!(f, "batch format error: {err}"),
            Self::InvalidMorsel { start, end } => {
                write!(
                    f,
                    "invalid morsel range: start page {start} > end page {end}"
                )
            }
            Self::InvalidBatchCapacity(capacity) => {
                write!(f, "batch capacity must be positive, got {capacity}")
            }
            Self::RecordDecode { rowid, payload_len } => write!(
                f,
                "failed to decode record payload for rowid {rowid} (payload_len={payload_len})"
            ),
            Self::SelectionIndexOverflow(idx) => write!(
                f,
                "selection index {idx} does not fit into u16 selection vector entry"
            ),
            Self::OffsetOutOfBounds {
                column,
                row_idx,
                start,
                end,
                data_len,
            } => write!(
                f,
                "column {column} has invalid offset range [{start}, {end}) for row {row_idx} \
                 (data_len={data_len})"
            ),
            Self::InvalidUtf8 { column, row_idx } => {
                write!(f, "column {column} row {row_idx} contains invalid UTF-8")
            }
        }
    }
}

impl std::error::Error for VectorizedScanError {}

impl From<FrankenError> for VectorizedScanError {
    fn from(value: FrankenError) -> Self {
        Self::Cursor(value)
    }
}

impl From<BatchFormatError> for VectorizedScanError {
    fn from(value: BatchFormatError) -> Self {
        Self::Batch(value)
    }
}

/// Result alias for vectorized scan operations.
pub type ScanResult<T> = std::result::Result<T, VectorizedScanError>;

/// Contiguous page range assigned to a scan worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageMorsel {
    pub start_page: PageNumber,
    pub end_page: PageNumber,
}

impl PageMorsel {
    /// Create a page-range morsel `[start_page, end_page]`.
    ///
    /// # Errors
    ///
    /// Returns an error when `start_page > end_page`.
    pub fn new(start_page: PageNumber, end_page: PageNumber) -> ScanResult<Self> {
        if start_page > end_page {
            return Err(VectorizedScanError::InvalidMorsel {
                start: start_page,
                end: end_page,
            });
        }
        Ok(Self {
            start_page,
            end_page,
        })
    }

    #[must_use]
    pub fn contains(self, page_no: PageNumber) -> bool {
        page_no >= self.start_page && page_no <= self.end_page
    }
}

/// Metadata emitted alongside each scan batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanBatchStats {
    /// Number of rows decoded from row-oriented payloads.
    pub rows_scanned: usize,
    /// Number of rows selected after applying the predicate.
    pub rows_selected: usize,
    /// Distinct leaf pages touched while producing this batch.
    pub pages_touched: Vec<PageNumber>,
    /// Number of best-effort prefetch hints issued while producing this batch.
    pub prefetch_hints_issued: usize,
}

/// A vectorized scan output chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanBatch {
    pub batch: Batch,
    pub stats: ScanBatchStats,
}

/// Vectorized B-tree table scan source.
pub struct VectorizedTableScan<P>
where
    P: PageWriter,
{
    cursor: BtCursor<P>,
    cx: Cx,
    specs: Vec<ColumnSpec>,
    batch_capacity: usize,
    predicate: Option<RowPredicate>,
    morsel: Option<PageMorsel>,
    started: bool,
    finished: bool,
    last_prefetched_page: Option<PageNumber>,
}

impl<P> VectorizedTableScan<P>
where
    P: PageWriter,
{
    /// Create a new vectorized table scan.
    ///
    /// # Errors
    ///
    /// Returns an error when `batch_capacity` is zero.
    pub fn try_new(
        cursor: BtCursor<P>,
        specs: Vec<ColumnSpec>,
        batch_capacity: usize,
    ) -> ScanResult<Self> {
        if batch_capacity == 0 {
            return Err(VectorizedScanError::InvalidBatchCapacity(batch_capacity));
        }
        Ok(Self {
            cursor,
            cx: Cx::new(),
            specs,
            batch_capacity,
            predicate: None,
            morsel: None,
            started: false,
            finished: false,
            last_prefetched_page: None,
        })
    }

    /// Attach a page-range morsel boundary.
    #[must_use]
    pub fn with_morsel(mut self, morsel: PageMorsel) -> Self {
        self.morsel = Some(morsel);
        self
    }

    /// Attach a scan-time predicate for selection-vector pushdown.
    #[must_use]
    pub fn with_predicate(mut self, predicate: RowPredicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Produce the next columnar batch.
    ///
    /// Returns `Ok(None)` when the scan is exhausted.
    ///
    /// # Errors
    ///
    /// Returns an error when cursor I/O fails, row payloads cannot be decoded,
    /// or batch construction fails.
    #[allow(clippy::too_many_lines)]
    pub fn next_batch(&mut self) -> ScanResult<Option<ScanBatch>> {
        if self.finished {
            return Ok(None);
        }
        if !self.started {
            self.started = true;
            self.cursor.clear_witness_keys();
            if !self.cursor.first(&self.cx)? {
                self.finished = true;
                return Ok(None);
            }
        }

        let mut rows = Vec::with_capacity(self.batch_capacity);
        let mut selection_indices = Vec::with_capacity(self.batch_capacity);
        let mut page_set = BTreeSet::new();
        let mut pages_touched = Vec::new();
        let mut prefetch_hints_issued = 0usize;

        while rows.len() < self.batch_capacity {
            if self.cursor.eof() {
                self.finished = true;
                break;
            }

            let current_page = self.current_page_or_internal_error()?;
            if let Some(morsel) = self.morsel {
                if current_page < morsel.start_page {
                    if !self.cursor.next(&self.cx)? {
                        self.finished = true;
                        break;
                    }
                    continue;
                }
                if current_page > morsel.end_page {
                    self.finished = true;
                    break;
                }
            }

            if page_set.insert(current_page) {
                pages_touched.push(current_page);
                if let Some(prefetch_page) = self.next_prefetch_page(current_page)
                    && self.last_prefetched_page != Some(prefetch_page)
                {
                    self.cursor.prefetch_page_hint(&self.cx, prefetch_page);
                    self.last_prefetched_page = Some(prefetch_page);
                    prefetch_hints_issued = prefetch_hints_issued.saturating_add(1);
                }
            }

            let rowid = self.cursor.rowid(&self.cx)?;
            let payload = self.cursor.payload(&self.cx)?;
            let row = parse_record(&payload).ok_or(VectorizedScanError::RecordDecode {
                rowid,
                payload_len: payload.len(),
            })?;

            let row_index = rows.len();
            if self.predicate_matches(rowid, &row) {
                let selected = u16::try_from(row_index)
                    .map_err(|_| VectorizedScanError::SelectionIndexOverflow(row_index))?;
                selection_indices.push(selected);
            }
            rows.push(row);

            if !self.cursor.next(&self.cx)? {
                self.finished = true;
                break;
            }
        }

        if rows.is_empty() {
            return Ok(None);
        }

        let mut batch = Batch::from_rows(&rows, &self.specs, self.batch_capacity)?;
        if selection_indices.len() != rows.len() {
            batch.apply_selection(SelectionVector::from_indices(selection_indices))?;
        }

        let stats = ScanBatchStats {
            rows_scanned: rows.len(),
            rows_selected: batch.selection().len(),
            pages_touched,
            prefetch_hints_issued,
        };

        Ok(Some(ScanBatch { batch, stats }))
    }

    fn predicate_matches(&self, rowid: i64, row: &[SqliteValue]) -> bool {
        self.predicate
            .as_ref()
            .is_none_or(|predicate| predicate(rowid, row))
    }

    fn current_page_or_internal_error(&self) -> ScanResult<PageNumber> {
        self.cursor.current_leaf_page().ok_or_else(|| {
            VectorizedScanError::Cursor(FrankenError::internal(
                "cursor positioned on row without a current leaf page",
            ))
        })
    }

    fn next_prefetch_page(&self, current_page: PageNumber) -> Option<PageNumber> {
        let candidate = current_page
            .get()
            .checked_add(1)
            .and_then(PageNumber::new)?;
        if let Some(morsel) = self.morsel {
            if candidate > morsel.end_page {
                return None;
            }
            if candidate < morsel.start_page {
                return None;
            }
        }
        Some(candidate)
    }
}

/// Materialize selected rows from a batch into row-oriented values.
///
/// Useful for correctness checks against row-at-a-time execution.
///
/// # Errors
///
/// Returns an error when selection indices or varlen offsets are invalid.
pub fn materialize_selected_rows(batch: &Batch) -> ScanResult<Vec<Vec<SqliteValue>>> {
    let mut rows = Vec::with_capacity(batch.selection().len());
    for &selected in batch.selection().as_slice() {
        let row_idx = usize::from(selected);
        let mut row = Vec::with_capacity(batch.columns().len());
        for column in batch.columns() {
            row.push(column_value_at(column, row_idx)?);
        }
        rows.push(row);
    }
    Ok(rows)
}

fn column_value_at(column: &Column, row_idx: usize) -> ScanResult<SqliteValue> {
    if !column.validity.is_valid(row_idx) {
        return Ok(SqliteValue::Null);
    }

    match &column.data {
        ColumnData::Int8(values) => Ok(SqliteValue::Integer(i64::from(values.as_slice()[row_idx]))),
        ColumnData::Int16(values) => {
            Ok(SqliteValue::Integer(i64::from(values.as_slice()[row_idx])))
        }
        ColumnData::Int32(values) => {
            Ok(SqliteValue::Integer(i64::from(values.as_slice()[row_idx])))
        }
        ColumnData::Int64(values) => Ok(SqliteValue::Integer(values.as_slice()[row_idx])),
        ColumnData::Float32(values) => {
            Ok(SqliteValue::Float(f64::from(values.as_slice()[row_idx])))
        }
        ColumnData::Float64(values) => Ok(SqliteValue::Float(values.as_slice()[row_idx])),
        ColumnData::Binary { offsets, data } => {
            let (start, end) =
                checked_offset_span(offsets, data.len(), row_idx, &column.spec.name)?;
            Ok(SqliteValue::Blob(data[start..end].to_vec()))
        }
        ColumnData::Text { offsets, data } => {
            let (start, end) =
                checked_offset_span(offsets, data.len(), row_idx, &column.spec.name)?;
            let text = std::str::from_utf8(&data[start..end]).map_err(|_| {
                VectorizedScanError::InvalidUtf8 {
                    column: column.spec.name.clone(),
                    row_idx,
                }
            })?;
            Ok(SqliteValue::Text(text.to_owned()))
        }
    }
}

fn checked_offset_span(
    offsets: &[u32],
    data_len: usize,
    row_idx: usize,
    column: &str,
) -> ScanResult<(usize, usize)> {
    if row_idx + 1 >= offsets.len() {
        return Err(VectorizedScanError::OffsetOutOfBounds {
            column: column.to_owned(),
            row_idx,
            start: 0,
            end: 0,
            data_len,
        });
    }

    let start =
        usize::try_from(offsets[row_idx]).map_err(|_| VectorizedScanError::OffsetOutOfBounds {
            column: column.to_owned(),
            row_idx,
            start: 0,
            end: 0,
            data_len,
        })?;
    let end = usize::try_from(offsets[row_idx + 1]).map_err(|_| {
        VectorizedScanError::OffsetOutOfBounds {
            column: column.to_owned(),
            row_idx,
            start: 0,
            end: 0,
            data_len,
        }
    })?;

    if start > end || end > data_len {
        return Err(VectorizedScanError::OffsetOutOfBounds {
            column: column.to_owned(),
            row_idx,
            start,
            end,
            data_len,
        });
    }
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::rc::Rc;

    use fsqlite_btree::{MemPageStore, PageReader};
    use fsqlite_types::record::serialize_record;

    use super::*;
    use crate::vectorized::{ColumnSpec, ColumnVectorType, DEFAULT_BATCH_ROW_CAPACITY};

    const PAGE_SIZE: u32 = 512;
    const ROOT_PAGE: u32 = 2;
    const BEAD_ID: &str = "bd-14vp7.2";

    #[derive(Clone, Debug)]
    struct SharedTrackingPageIo {
        store: Rc<RefCell<MemPageStore>>,
        hinted_pages: Rc<RefCell<Vec<PageNumber>>>,
    }

    impl SharedTrackingPageIo {
        fn new(page_size: u32, root_page: PageNumber) -> Self {
            Self {
                store: Rc::new(RefCell::new(MemPageStore::with_empty_table(
                    root_page, page_size,
                ))),
                hinted_pages: Rc::new(RefCell::new(Vec::new())),
            }
        }

        fn hinted_pages(&self) -> Vec<PageNumber> {
            self.hinted_pages.borrow().clone()
        }
    }

    impl PageReader for SharedTrackingPageIo {
        fn read_page(&self, cx: &Cx, page_no: PageNumber) -> fsqlite_error::Result<Vec<u8>> {
            self.store.borrow().read_page(cx, page_no)
        }

        fn prefetch_page_hint(&self, _cx: &Cx, page_no: PageNumber) {
            self.hinted_pages.borrow_mut().push(page_no);
        }
    }

    impl fsqlite_btree::PageWriter for SharedTrackingPageIo {
        fn write_page(
            &mut self,
            cx: &Cx,
            page_no: PageNumber,
            data: &[u8],
        ) -> fsqlite_error::Result<()> {
            self.store.borrow_mut().write_page(cx, page_no, data)
        }

        fn allocate_page(&mut self, cx: &Cx) -> fsqlite_error::Result<PageNumber> {
            self.store.borrow_mut().allocate_page(cx)
        }

        fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> fsqlite_error::Result<()> {
            self.store.borrow_mut().free_page(cx, page_no)
        }
    }

    fn specs() -> Vec<ColumnSpec> {
        vec![
            ColumnSpec::new("c0", ColumnVectorType::Int64),
            ColumnSpec::new("c1", ColumnVectorType::Float64),
            ColumnSpec::new("c2", ColumnVectorType::Text),
            ColumnSpec::new("c3", ColumnVectorType::Binary),
        ]
    }

    fn row_for_rowid(rowid: i64) -> Vec<SqliteValue> {
        vec![
            SqliteValue::Integer(rowid * 7),
            SqliteValue::Float(rowid as f64 * 0.5),
            SqliteValue::Text(format!("row-{rowid:05}")),
            SqliteValue::Blob(vec![
                u8::try_from(rowid.rem_euclid(251)).expect("mod value should fit into u8"),
                u8::try_from((rowid * 3).rem_euclid(251)).expect("mod value should fit into u8"),
                u8::try_from((rowid * 7).rem_euclid(251)).expect("mod value should fit into u8"),
            ]),
        ]
    }

    fn build_fixture(row_count: usize) -> (SharedTrackingPageIo, PageNumber) {
        let root_page = PageNumber::new(ROOT_PAGE).expect("root page should be non-zero");
        let io = SharedTrackingPageIo::new(PAGE_SIZE, root_page);
        let mut writer = BtCursor::new(io.clone(), root_page, PAGE_SIZE, true);
        let cx = Cx::new();

        for idx in 0..row_count {
            let rowid = i64::try_from(idx + 1).expect("rowid should fit into i64");
            let row = row_for_rowid(rowid);
            let payload = serialize_record(&row);
            writer
                .table_insert(&cx, rowid, &payload)
                .expect("table_insert should succeed");
        }

        (io, root_page)
    }

    fn collect_rows_row_at_a_time<F>(
        io: SharedTrackingPageIo,
        root_page: PageNumber,
        morsel: Option<PageMorsel>,
        predicate: F,
    ) -> (Vec<Vec<SqliteValue>>, Vec<PageNumber>)
    where
        F: Fn(i64, &[SqliteValue]) -> bool,
    {
        let mut cursor = BtCursor::new(io, root_page, PAGE_SIZE, true);
        let cx = Cx::new();
        let mut rows = Vec::new();
        let mut pages = Vec::new();
        let mut seen_pages = BTreeSet::new();

        if !cursor.first(&cx).expect("first should succeed") {
            return (rows, pages);
        }

        loop {
            if cursor.eof() {
                break;
            }

            let current_page = cursor
                .current_leaf_page()
                .expect("cursor at row should have current leaf page");
            if let Some(m) = morsel {
                if current_page < m.start_page {
                    if !cursor.next(&cx).expect("next should succeed") {
                        break;
                    }
                    continue;
                }
                if current_page > m.end_page {
                    break;
                }
            }

            if seen_pages.insert(current_page) {
                pages.push(current_page);
            }

            let rowid = cursor.rowid(&cx).expect("rowid should succeed");
            let payload = cursor.payload(&cx).expect("payload should succeed");
            let row = parse_record(&payload).expect("payload should decode");
            if predicate(rowid, &row) {
                rows.push(row);
            }

            if !cursor.next(&cx).expect("next should succeed") {
                break;
            }
        }

        (rows, pages)
    }

    #[test]
    fn scan_output_matches_row_at_a_time_output() {
        let (io, root_page) = build_fixture(2_000);
        let scan_cursor = BtCursor::new(io.clone(), root_page, PAGE_SIZE, true);
        let mut scan =
            VectorizedTableScan::try_new(scan_cursor, specs(), DEFAULT_BATCH_ROW_CAPACITY)
                .expect("scan should initialize");

        let mut actual_rows = Vec::new();
        let mut scanned_pages = BTreeSet::new();
        while let Some(output) = scan.next_batch().expect("batch should scan successfully") {
            for page in output.stats.pages_touched {
                scanned_pages.insert(page);
            }
            let selected =
                materialize_selected_rows(&output.batch).expect("selected rows should materialize");
            actual_rows.extend(selected);
        }

        let (expected_rows, _) =
            collect_rows_row_at_a_time(io, root_page, None, |_rowid, _row| true);
        assert_eq!(
            actual_rows, expected_rows,
            "bead_id={BEAD_ID} full scan mismatch"
        );
        assert!(
            scanned_pages.len() > 1,
            "bead_id={BEAD_ID} expected multi-page scan to validate leaf traversal"
        );
    }

    #[test]
    fn filter_pushdown_updates_selection_vector() {
        let (io, root_page) = build_fixture(1_500);
        let predicate: RowPredicate = Arc::new(|rowid, _row| rowid % 3 == 0);
        let scan_cursor = BtCursor::new(io.clone(), root_page, PAGE_SIZE, true);
        let mut scan = VectorizedTableScan::try_new(scan_cursor, specs(), 256)
            .expect("scan should initialize")
            .with_predicate(predicate.clone());

        let mut actual_rows = Vec::new();
        let mut saw_pushdown = false;
        while let Some(output) = scan.next_batch().expect("batch should scan successfully") {
            if output.stats.rows_selected < output.stats.rows_scanned {
                saw_pushdown = true;
            }
            actual_rows.extend(
                materialize_selected_rows(&output.batch).expect("selected rows should materialize"),
            );
        }

        let (expected_rows, _) =
            collect_rows_row_at_a_time(io, root_page, None, |rowid, _row| rowid % 3 == 0);
        assert_eq!(
            actual_rows, expected_rows,
            "bead_id={BEAD_ID} predicate pushdown mismatch"
        );
        assert!(
            saw_pushdown,
            "bead_id={BEAD_ID} expected at least one filtered batch"
        );
    }

    #[test]
    fn scan_respects_page_morsel_boundaries() {
        let (io, root_page) = build_fixture(3_000);
        let (_all_rows, all_pages) =
            collect_rows_row_at_a_time(io.clone(), root_page, None, |_rowid, _row| true);
        assert!(
            all_pages.len() >= 3,
            "bead_id={BEAD_ID} expected at least 3 pages for morsel boundary test"
        );

        let morsel = PageMorsel::new(all_pages[1], all_pages[2]).expect("morsel should be valid");
        let scan_cursor = BtCursor::new(io.clone(), root_page, PAGE_SIZE, true);
        let mut scan = VectorizedTableScan::try_new(scan_cursor, specs(), 192)
            .expect("scan should initialize")
            .with_morsel(morsel);

        let mut actual_rows = Vec::new();
        let mut touched_pages = BTreeSet::new();
        while let Some(output) = scan.next_batch().expect("batch should scan successfully") {
            for page in output.stats.pages_touched {
                assert!(
                    morsel.contains(page),
                    "bead_id={BEAD_ID} page {page} escaped morsel {:?}",
                    morsel
                );
                touched_pages.insert(page);
            }
            actual_rows.extend(
                materialize_selected_rows(&output.batch).expect("selected rows should materialize"),
            );
        }

        let (expected_rows, expected_pages) =
            collect_rows_row_at_a_time(io, root_page, Some(morsel), |_rowid, _row| true);
        assert_eq!(
            actual_rows, expected_rows,
            "bead_id={BEAD_ID} morsel output mismatch"
        );
        let expected_page_set: BTreeSet<PageNumber> = expected_pages.into_iter().collect();
        assert_eq!(
            touched_pages, expected_page_set,
            "bead_id={BEAD_ID} touched page set mismatch"
        );
    }

    #[test]
    fn prefetch_hints_are_emitted_during_scan() {
        let (io, root_page) = build_fixture(2_500);
        let (all_rows, pages) =
            collect_rows_row_at_a_time(io.clone(), root_page, None, |_rowid, _row| true);
        assert!(
            pages.len() >= 2,
            "bead_id={BEAD_ID} expected at least two pages for prefetch test"
        );
        assert!(!all_rows.is_empty(), "fixture should contain rows");

        let morsel = PageMorsel::new(pages[0], pages[1]).expect("morsel should be valid");
        let scan_cursor = BtCursor::new(io.clone(), root_page, PAGE_SIZE, true);
        let mut scan = VectorizedTableScan::try_new(scan_cursor, specs(), 128)
            .expect("scan should initialize")
            .with_morsel(morsel);

        let mut total_hints = 0usize;
        while let Some(output) = scan.next_batch().expect("batch should scan successfully") {
            total_hints = total_hints.saturating_add(output.stats.prefetch_hints_issued);
        }

        let hinted_pages = io.hinted_pages();
        assert!(
            total_hints > 0,
            "bead_id={BEAD_ID} expected scan to issue explicit prefetch hints"
        );
        assert!(
            !hinted_pages.is_empty(),
            "bead_id={BEAD_ID} expected page-reader prefetch hints"
        );
    }
}
