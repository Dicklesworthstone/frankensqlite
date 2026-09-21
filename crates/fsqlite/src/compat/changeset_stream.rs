//! Bounded, asynchronous decoding of SQLite Session changesets and patchsets.
//!
//! The reader retains one table header, one row, and an 8 KiB input buffer,
//! rather than the entire input or an entire table. Limits are checked before
//! reserving attacker-controlled value/column storage. This is a wire reader,
//! not a live SQL mutation recorder; use the SQL applier to validate row and
//! target-schema semantics.
//!
//! A stream ends at EOF; it has no outer length or authentication envelope.
//! The transport must provide a bounded, authenticated message where needed.
//! Dropping `next` after polling it, or any decoding error, poisons the reader:
//! consumed bytes cannot safely be replayed as a new record. Discard it rather
//! than retrying. Ready input and Interrupted retries share a cooperative work
//! budget across rows, yielding with a wakeup before decoding more input.
//! Cancellation is checked on resume. A pending transport must still support
//! its own cancellation or the caller must drop the operation.

pub mod verified;

use std::future::poll_fn;
use std::io;
use std::sync::Arc;
use std::task::Poll;

use asupersync::io::{AsyncRead, AsyncReadExt};
use fsqlite_ext_session::{
    ChangeOp, ChangesetKind, ChangesetRow, ChangesetValue, TableInfo,
};
use fsqlite_types::cx::Cx;

const BUFFER_SIZE: usize = 8192;
// One unit is a buffered input access or an underlying Interrupted retry.
// This is a scheduling bound on decoder steps, not a wall-time guarantee for
// an arbitrary transport or allocation. Keep the budget across next() calls:
// a caller may consume many immediately-ready rows within one executor poll.
const DECODE_WORK_BUDGET: usize = 256;

/// Resource policy for one EOF-delimited input. Row bytes count value tags,
/// fixed payloads, and variable payloads (not the input buffer or Rust enum
/// overhead). Column limits separately bound the two row-value vectors.
#[derive(Debug, Clone, Copy)]
pub struct ChangesetStreamLimits {
    pub max_columns: usize,
    pub max_table_name_bytes: usize,
    pub max_value_bytes: usize,
    pub max_row_bytes: usize,
    pub max_input_bytes: u64,
    pub max_rows: u64,
    pub max_table_sections: u64,
}

impl Default for ChangesetStreamLimits {
    fn default() -> Self {
        Self {
            max_columns: 2000,
            max_table_name_bytes: 1024,
            max_value_bytes: 16 * 1024 * 1024,
            max_row_bytes: 64 * 1024 * 1024,
            max_input_bytes: u64::MAX,
            max_rows: u64::MAX,
            max_table_sections: 1_000_000,
        }
    }
}

/// Input failures remain distinct from SQL conflicts and storage failures.
#[derive(Debug)]
pub enum ChangesetStreamError {
    Io(io::Error),
    Cancelled,
    Poisoned,
    Truncated { offset: u64 },
    Malformed { offset: u64, detail: &'static str },
    Limit { offset: u64, resource: &'static str },
    Allocation { offset: u64 },
}

impl std::fmt::Display for ChangesetStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "changeset input: {error}"),
            Self::Cancelled => f.write_str("changeset input cancelled"),
            Self::Poisoned => f.write_str("changeset reader was interrupted or failed"),
            Self::Truncated { offset } => write!(f, "truncated changeset at byte {offset}"),
            Self::Malformed { offset, detail } => write!(f, "malformed changeset at byte {offset}: {detail}"),
            Self::Limit { offset, resource } => write!(f, "changeset {resource} limit at byte {offset}"),
            Self::Allocation { offset } => write!(f, "changeset allocation failed at byte {offset}"),
        }
    }
}

impl std::error::Error for ChangesetStreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

/// An owned row with shared metadata. Section numbers are one-based and change
/// even when two consecutive sections name the same table. Row indices are
/// zero-based within that section. No payload aliases the input buffer.
#[derive(Debug)]
pub struct StreamedChange {
    pub kind: ChangesetKind,
    pub table: Arc<TableInfo>,
    pub section: u64,
    pub row_index: usize,
    pub change: ChangesetRow,
}

/// Single-pass SQLite Session wire reader, using the project's async I/O.
#[derive(Debug)]
pub struct ChangesetStreamReader<R> {
    input: R,
    limits: ChangesetStreamLimits,
    buffer: [u8; BUFFER_SIZE],
    start: usize,
    end: usize,
    offset: u64,
    rows: u64,
    section: u64,
    row_index: usize,
    table: Option<Arc<TableInfo>>,
    kind: Option<ChangesetKind>,
    poisoned: bool,
    finished: bool,
    work_remaining: usize,
}

impl<R> ChangesetStreamReader<R> {
    #[must_use]
    pub const fn new(input: R, limits: ChangesetStreamLimits) -> Self {
        Self {
            input, limits, buffer: [0; BUFFER_SIZE], start: 0, end: 0,
            offset: 0, rows: 0, section: 0, row_index: 0, table: None,
            kind: None, poisoned: false, finished: false,
            work_remaining: DECODE_WORK_BUDGET,
        }
    }

    /// Bytes consumed by the decoder, excluding buffered read-ahead.
    #[must_use]
    pub const fn bytes_consumed(&self) -> u64 { self.offset }

    #[must_use]
    pub const fn rows_decoded(&self) -> u64 { self.rows }

    fn malformed(&self, detail: &'static str) -> ChangesetStreamError {
        ChangesetStreamError::Malformed { offset: self.offset, detail }
    }

    fn limit(&self, resource: &'static str) -> ChangesetStreamError {
        ChangesetStreamError::Limit { offset: self.offset, resource }
    }

    fn reserve<T>(&self, values: &mut Vec<T>, count: usize) -> Result<(), ChangesetStreamError> {
        values.try_reserve_exact(count)
            .map_err(|_| ChangesetStreamError::Allocation { offset: self.offset })
    }

    fn advance(&mut self, count: usize) -> Result<(), ChangesetStreamError> {
        let count_u64 = u64::try_from(count).map_err(|_| self.limit("input bytes"))?;
        let next = self.offset.checked_add(count_u64).ok_or_else(|| self.limit("input bytes"))?;
        if next > self.limits.max_input_bytes { return Err(self.limit("input bytes")); }
        self.offset = next;
        self.start += count;
        Ok(())
    }

    fn charge(&self, used: &mut usize, count: usize) -> Result<(), ChangesetStreamError> {
        let next = used.checked_add(count).ok_or_else(|| self.limit("row bytes"))?;
        if next > self.limits.max_row_bytes { return Err(self.limit("row bytes")); }
        *used = next;
        Ok(())
    }

    async fn cooperate(&mut self, cx: &Cx) -> Result<(), ChangesetStreamError> {
        cx.checkpoint().map_err(|_| ChangesetStreamError::Cancelled)?;
        if self.work_remaining == 0 {
            // Checkpoints alone cannot schedule another task on this executor.
            // Self-wake once, then return Pending without a timer or a task.
            let mut yielded = false;
            poll_fn(|task_cx| {
                if yielded {
                    Poll::Ready(())
                } else {
                    yielded = true;
                    task_cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
            self.work_remaining = DECODE_WORK_BUDGET;
            // A cancellation task may have run while we yielded. Do not touch
            // the input or publish another row before observing its request.
            cx.checkpoint().map_err(|_| ChangesetStreamError::Cancelled)?;
        }
        self.work_remaining -= 1;
        Ok(())
    }
}

impl<R: AsyncRead + Unpin> ChangesetStreamReader<R> {
    /// Decode one row, or return `None` only at a complete record boundary.
    /// Empty table sections are consumed without emitting a row. Mixed wire
    /// kinds, malformed UTF-8, truncated fields and unknown tags are errors.
    /// After any error or a dropped in-flight call the reader is unusable.
    pub async fn next(&mut self, cx: &Cx) -> Result<Option<StreamedChange>, ChangesetStreamError> {
        if self.poisoned { return Err(ChangesetStreamError::Poisoned); }
        if self.finished { return Ok(None); }
        self.poisoned = true;
        let result = self.next_inner(cx).await;
        if result.is_ok() { self.poisoned = false; }
        result
    }

    async fn refill(&mut self, cx: &Cx) -> Result<bool, ChangesetStreamError> {
        self.cooperate(cx).await?;
        if self.start < self.end { return Ok(true); }
        loop {
            match self.input.read(&mut self.buffer).await {
                Ok(count) => {
                    self.start = 0;
                    self.end = count;
                    return Ok(count != 0);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    self.cooperate(cx).await?;
                }
                Err(error) => return Err(ChangesetStreamError::Io(error)),
            }
        }
    }

    async fn optional_byte(&mut self, cx: &Cx) -> Result<Option<u8>, ChangesetStreamError> {
        if !self.refill(cx).await? { return Ok(None); }
        let value = self.buffer[self.start];
        self.advance(1)?;
        Ok(Some(value))
    }

    async fn byte(&mut self, cx: &Cx) -> Result<u8, ChangesetStreamError> {
        self.optional_byte(cx).await?.ok_or(ChangesetStreamError::Truncated { offset: self.offset })
    }

    async fn exact(&mut self, cx: &Cx, destination: &mut [u8]) -> Result<(), ChangesetStreamError> {
        let mut written = 0;
        while written < destination.len() {
            if !self.refill(cx).await? {
                return Err(ChangesetStreamError::Truncated { offset: self.offset });
            }
            let count = (self.end - self.start).min(destination.len() - written);
            destination[written..written + count].copy_from_slice(&self.buffer[self.start..self.start + count]);
            self.advance(count)?;
            written += count;
        }
        Ok(())
    }

    /// SQLite varints use eight seven-bit groups and a final eight-bit byte.
    async fn varint(&mut self, cx: &Cx) -> Result<u64, ChangesetStreamError> {
        let mut value = 0_u64;
        for _ in 0..8 {
            let byte = self.byte(cx).await?;
            value = (value << 7) | u64::from(byte & 0x7f);
            if byte & 0x80 == 0 { return Ok(value); }
        }
        Ok((value << 8) | u64::from(self.byte(cx).await?))
    }

    async fn header(&mut self, cx: &Cx, marker: u8) -> Result<(), ChangesetStreamError> {
        let kind = if marker == b'T' { ChangesetKind::Changeset } else { ChangesetKind::Patchset };
        if self.kind.is_some_and(|previous| previous != kind) {
            return Err(self.malformed("mixed changeset and patchset headers"));
        }
        if self.section >= self.limits.max_table_sections { return Err(self.limit("table sections")); }
        let columns = usize::try_from(self.varint(cx).await?).map_err(|_| self.limit("columns"))?;
        if columns == 0 { return Err(self.malformed("table has no columns")); }
        if columns > self.limits.max_columns { return Err(self.limit("columns")); }
        let mut pk_flags = Vec::new();
        self.reserve(&mut pk_flags, columns)?;
        for _ in 0..columns {
            // SQLite may encode composite-key ordinals, not only 0/1.
            pk_flags.push(self.byte(cx).await? != 0);
        }
        if !pk_flags.iter().any(|flag| *flag) { return Err(self.malformed("table has no primary key")); }
        let mut name = Vec::new();
        loop {
            let byte = self.byte(cx).await?;
            if byte == 0 { break; }
            if name.len() >= self.limits.max_table_name_bytes { return Err(self.limit("table name bytes")); }
            self.reserve(&mut name, 1)?;
            name.push(byte);
        }
        if name.is_empty() { return Err(self.malformed("empty table name")); }
        let name = String::from_utf8(name).map_err(|_| self.malformed("table name is not UTF-8"))?;
        self.table = Some(Arc::new(TableInfo { name, column_count: columns, pk_flags }));
        self.kind = Some(kind);
        self.section += 1;
        self.row_index = 0;
        Ok(())
    }

    async fn value(&mut self, cx: &Cx, used: &mut usize) -> Result<ChangesetValue, ChangesetStreamError> {
        self.charge(used, 1)?;
        match self.byte(cx).await? {
            0 => Ok(ChangesetValue::Undefined),
            5 => Ok(ChangesetValue::Null),
            tag @ (1 | 2) => {
                self.charge(used, 8)?;
                let mut bytes = [0; 8];
                self.exact(cx, &mut bytes).await?;
                if tag == 1 { Ok(ChangesetValue::Integer(i64::from_be_bytes(bytes))) }
                else { Ok(ChangesetValue::Real(f64::from_be_bytes(bytes))) }
            }
            tag @ (3 | 4) => {
                let len = usize::try_from(self.varint(cx).await?).map_err(|_| self.limit("value bytes"))?;
                if len > self.limits.max_value_bytes { return Err(self.limit("value bytes")); }
                self.charge(used, len)?;
                // Check the remaining wire budget before allocating the payload.
                let len_u64 = u64::try_from(len).map_err(|_| self.limit("input bytes"))?;
                if len_u64 > self.limits.max_input_bytes.saturating_sub(self.offset) {
                    return Err(self.limit("input bytes"));
                }
                let mut bytes = Vec::new();
                self.reserve(&mut bytes, len)?;
                bytes.resize(len, 0);
                self.exact(cx, &mut bytes).await?;
                if tag == 3 {
                    String::from_utf8(bytes).map(ChangesetValue::Text)
                        .map_err(|_| self.malformed("text value is not UTF-8"))
                } else { Ok(ChangesetValue::Blob(bytes)) }
            }
            _ => Err(self.malformed("unknown value tag")),
        }
    }

    async fn values(&mut self, cx: &Cx, count: usize, used: &mut usize) -> Result<Vec<ChangesetValue>, ChangesetStreamError> {
        let mut values = Vec::new();
        self.reserve(&mut values, count)?;
        for _ in 0..count { values.push(self.value(cx, used).await?); }
        Ok(values)
    }

    async fn next_inner(&mut self, cx: &Cx) -> Result<Option<StreamedChange>, ChangesetStreamError> {
        loop {
            let Some(marker) = self.optional_byte(cx).await? else {
                self.finished = true;
                return Ok(None);
            };
            if marker == b'T' || marker == b'P' {
                self.header(cx, marker).await?;
                continue;
            }
            let table = self.table.clone().ok_or_else(|| self.malformed("row before table header"))?;
            let kind = self.kind.ok_or_else(|| self.malformed("missing wire kind"))?;
            let op = ChangeOp::from_byte(marker).ok_or_else(|| self.malformed("unknown operation"))?;
            let indirect = match self.byte(cx).await? {
                0 => false,
                1 => true,
                _ => return Err(self.malformed("indirect flag must be 0 or 1")),
            };
            if self.rows >= self.limits.max_rows { return Err(self.limit("rows")); }
            let next_index = self.row_index.checked_add(1).ok_or_else(|| self.limit("rows per table"))?;
            let mut used = 0;
            let (old_values, new_values) = match (kind, op) {
                (_, ChangeOp::Insert) => (Vec::new(), self.values(cx, table.column_count, &mut used).await?),
                (ChangesetKind::Changeset, ChangeOp::Delete) => (self.values(cx, table.column_count, &mut used).await?, Vec::new()),
                (ChangesetKind::Changeset, ChangeOp::Update) => (
                    self.values(cx, table.column_count, &mut used).await?,
                    self.values(cx, table.column_count, &mut used).await?,
                ),
                (ChangesetKind::Patchset, ChangeOp::Delete | ChangeOp::Update) => {
                    let mut old = Vec::new();
                    let mut new = Vec::new();
                    self.reserve(&mut old, table.column_count)?;
                    if op == ChangeOp::Update { self.reserve(&mut new, table.column_count)?; }
                    for pk in &table.pk_flags {
                        if *pk {
                            old.push(self.value(cx, &mut used).await?);
                            if op == ChangeOp::Update { new.push(ChangesetValue::Undefined); }
                        } else {
                            old.push(ChangesetValue::Undefined);
                            if op == ChangeOp::Update { new.push(self.value(cx, &mut used).await?); }
                        }
                    }
                    (old, new)
                }
            };
            let row_index = self.row_index;
            self.row_index = next_index;
            self.rows += 1;
            return Ok(Some(StreamedChange {
                kind, table, section: self.section, row_index,
                change: ChangesetRow { op, indirect, old_values, new_values },
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use asupersync::io::ReadBuf;
    use fsqlite_ext_session::{Changeset, TableChangeset};

    struct Fragments { bytes: Vec<u8>, offset: usize, chunk: usize, pending_at: Option<usize> }

    impl AsyncRead for Fragments {
        fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.pending_at.is_some_and(|at| this.offset >= at) { return Poll::Pending; }
            let n = this.chunk.min(buf.remaining()).min(this.bytes.len() - this.offset);
            buf.put_slice(&this.bytes[this.offset..this.offset + n]);
            this.offset += n;
            Poll::Ready(Ok(()))
        }
    }

    fn input(bytes: Vec<u8>, chunk: usize) -> Fragments {
        Fragments { bytes, offset: 0, chunk, pending_at: None }
    }

    // Always ready, including its bounded Interrupted prefix. The old decoder
    // finishes these inputs in one poll, so red tests fail promptly, not hang.
    struct ImmediateSource {
        inner: Fragments,
        interruptions: usize,
        calls: Arc<AtomicUsize>,
    }

    impl AsyncRead for ImmediateSource {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            out: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.calls.fetch_add(1, Ordering::Relaxed);
            if this.interruptions > 0 {
                this.interruptions -= 1;
                return Poll::Ready(Err(io::ErrorKind::Interrupted.into()));
            }
            Pin::new(&mut this.inner).poll_read(cx, out)
        }
    }

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn fixture() -> Changeset {
        Changeset { kind: ChangesetKind::Changeset, tables: vec![TableChangeset {
            info: TableInfo { name: "quoted\"table".to_owned(), column_count: 3, pk_flags: vec![false, true, true] },
            rows: vec![
                ChangesetRow { op: ChangeOp::Insert, indirect: true, old_values: vec![], new_values: vec![
                    ChangesetValue::Text("nul\0unicode: λ".to_owned()), ChangesetValue::Integer(-7), ChangesetValue::Blob(vec![0, 255]),
                ] },
                ChangesetRow { op: ChangeOp::Update, indirect: false, old_values: vec![
                    ChangesetValue::Text("before".to_owned()), ChangesetValue::Integer(-7), ChangesetValue::Blob(vec![0, 255]),
                ], new_values: vec![ChangesetValue::Real(1.25), ChangesetValue::Undefined, ChangesetValue::Undefined] },
                ChangesetRow { op: ChangeOp::Delete, indirect: true, old_values: vec![
                    ChangesetValue::Null, ChangesetValue::Integer(-7), ChangesetValue::Blob(vec![0, 255]),
                ], new_values: vec![] },
            ],
        }] }
    }

    #[test]
    fn interrupted_ready_input_yields_and_observes_cancellation_before_retrying() {
        let calls = Arc::new(AtomicUsize::new(0));
        let source = ImmediateSource {
            inner: input(fixture().encode(), BUFFER_SIZE),
            interruptions: DECODE_WORK_BUDGET * 2,
            calls: Arc::clone(&calls),
        };
        let mut reader = ChangesetStreamReader::new(source, ChangesetStreamLimits::default());
        let cx = Cx::new();
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let mut task = Context::from_waker(&waker);
        let mut next = Box::pin(reader.next(&cx));
        assert!(next.as_mut().poll(&mut task).is_pending());
        assert_eq!(calls.load(Ordering::Relaxed), DECODE_WORK_BUDGET);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        cx.cancel();
        assert!(matches!(
            next.as_mut().poll(&mut task),
            Poll::Ready(Err(ChangesetStreamError::Cancelled))
        ));
        assert_eq!(calls.load(Ordering::Relaxed), DECODE_WORK_BUDGET);
        drop(next);
        assert_eq!(reader.bytes_consumed(), 0);
        assert!(matches!(
            Box::pin(reader.next(&cx)).as_mut().poll(&mut task),
            Poll::Ready(Err(ChangesetStreamError::Poisoned))
        ));
    }

    #[test]
    fn interrupted_ready_input_resumes_without_losing_or_repeating_rows() {
        asupersync::test_utils::run_test(|| async {
            let expected = fixture();
            let wire = expected.encode();
            let wire_len = u64::try_from(wire.len()).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let source = ImmediateSource {
                inner: input(wire, 3),
                interruptions: DECODE_WORK_BUDGET * 2 + 7,
                calls: Arc::clone(&calls),
            };
            let mut reader = ChangesetStreamReader::new(source, ChangesetStreamLimits::default());
            let cx = Cx::new();
            for (index, row) in expected.tables[0].rows.iter().enumerate() {
                let actual = reader.next(&cx).await.unwrap().unwrap();
                assert_eq!(&actual.change, row);
                assert_eq!(actual.row_index, index);
                assert_eq!(actual.section, 1);
            }
            assert!(reader.next(&cx).await.unwrap().is_none());
            assert_eq!(reader.bytes_consumed(), wire_len);
            assert_eq!(reader.rows_decoded(), 3);
            assert!(calls.load(Ordering::Relaxed) > DECODE_WORK_BUDGET * 2);
        });
    }

    #[test]
    fn one_byte_ready_input_yields_within_a_row_and_resumes_exactly() {
        let mut expected = fixture();
        expected.tables[0].rows.truncate(1);
        expected.tables[0].rows[0].new_values[0] =
            ChangesetValue::Text("x".repeat(DECODE_WORK_BUDGET * 2));
        let wire = expected.encode();
        let wire_len = wire.len();
        let calls = Arc::new(AtomicUsize::new(0));
        let source = ImmediateSource {
            inner: input(wire, 1),
            interruptions: 0,
            calls: Arc::clone(&calls),
        };
        let mut reader = ChangesetStreamReader::new(source, ChangesetStreamLimits::default());
        let cx = Cx::new();
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let mut task = Context::from_waker(&waker);
        let mut next = Box::pin(reader.next(&cx));
        assert!(next.as_mut().poll(&mut task).is_pending());
        assert_eq!(calls.load(Ordering::Relaxed), DECODE_WORK_BUDGET);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        let mut completed = None;
        for _ in 0..wire_len {
            if let Poll::Ready(result) = next.as_mut().poll(&mut task) {
                completed = Some(result);
                break;
            }
        }
        let row = completed.expect("bounded ready source must complete").unwrap().unwrap();
        assert_eq!(row.change, expected.tables[0].rows[0]);
        drop(next);
        assert_eq!(reader.bytes_consumed(), u64::try_from(wire_len).unwrap());
        assert_eq!(reader.rows_decoded(), 1);
    }

    #[test]
    fn buffered_rows_share_the_budget_between_next_calls() {
        let mut expected = fixture();
        expected.tables[0].rows = vec![expected.tables[0].rows[0].clone(); DECODE_WORK_BUDGET];
        let calls = Arc::new(AtomicUsize::new(0));
        let source = ImmediateSource {
            inner: input(expected.encode(), BUFFER_SIZE),
            interruptions: 0,
            calls: Arc::clone(&calls),
        };
        let mut reader = ChangesetStreamReader::new(source, ChangesetStreamLimits::default());
        let cx = Cx::new();
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let mut task = Context::from_waker(&waker);
        for index in 0..DECODE_WORK_BUDGET {
            let mut next = Box::pin(reader.next(&cx));
            match next.as_mut().poll(&mut task) {
                Poll::Ready(Ok(Some(row))) => {
                    assert_eq!(row.row_index, index);
                    assert_eq!(row.change, expected.tables[0].rows[index]);
                }
                Poll::Pending => {
                    assert!(index > 0, "small rows should not unconditionally yield");
                    assert_eq!(calls.load(Ordering::Relaxed), 1, "work was already buffered");
                    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
                    cx.cancel();
                    assert!(matches!(
                        next.as_mut().poll(&mut task),
                        Poll::Ready(Err(ChangesetStreamError::Cancelled))
                    ));
                    drop(next);
                    assert_eq!(reader.rows_decoded(), u64::try_from(index).unwrap());
                    return;
                }
                other => panic!("unexpected decode outcome: {other:?}"),
            }
        }
        panic!("ready rows must not reset the shared work budget");
    }

    #[test]
    fn buffered_empty_sections_yield_and_dropped_decode_stays_poisoned() {
        let wire = [b'T', 1, 1, b't', 0].repeat(DECODE_WORK_BUDGET * 2);
        assert!(wire.len() < BUFFER_SIZE);
        let calls = Arc::new(AtomicUsize::new(0));
        let source = ImmediateSource {
            inner: input(wire, BUFFER_SIZE),
            interruptions: 0,
            calls: Arc::clone(&calls),
        };
        let mut reader = ChangesetStreamReader::new(source, ChangesetStreamLimits::default());
        let cx = Cx::new();
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let mut task = Context::from_waker(&waker);
        let mut next = Box::pin(reader.next(&cx));
        assert!(next.as_mut().poll(&mut task).is_pending());
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        drop(next);
        assert_eq!(reader.rows_decoded(), 0);
        assert!(matches!(
            Box::pin(reader.next(&cx)).as_mut().poll(&mut task),
            Poll::Ready(Err(ChangesetStreamError::Poisoned))
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn stock_sqlite_composite_key_changeset_and_patchset_goldens() {
        // Generated and iterated with stock SQLite 3.46.1. The schema is
        // t(v, a INTEGER, b BLOB, PRIMARY KEY(b,a)) WITHOUT ROWID; primary-key
        // ordinal bytes are 0,2,1, not a boolean mask. Includes all three
        // operations, an indirect INSERT, embedded NUL and UTF-8 text.
        const FULL: &str = "540300020174000900050100000000000000020401111201030f6e756c00756e69636f64653a20cebb01fffffffffffffff9040200ff170003036f6c64010000000000000001040200ff023ff40000000000000000";
        const PATCH: &str = "5003000201740009000100000000000000020401111201030f6e756c00756e69636f64653a20cebb01fffffffffffffff9040200ff1700023ff4000000000000010000000000000001040200ff";
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            for (hex, patchset) in [(FULL, false), (PATCH, true)] {
                let wire: Vec<u8> = (0..hex.len()).step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect();
                for chunk in 1..=wire.len() {
                    let mut reader = ChangesetStreamReader::new(input(wire.clone(), chunk), ChangesetStreamLimits::default());
                    let deleted = reader.next(&cx).await.unwrap().unwrap();
                    assert_eq!(deleted.table.pk_flags, vec![false, true, true]);
                    assert_eq!(deleted.change.op, ChangeOp::Delete);
                    assert_eq!(deleted.change.old_values, vec![
                        if patchset { ChangesetValue::Undefined } else { ChangesetValue::Null },
                        ChangesetValue::Integer(2), ChangesetValue::Blob(vec![17]),
                    ]);
                    let inserted = reader.next(&cx).await.unwrap().unwrap();
                    assert!(inserted.change.indirect);
                    assert_eq!(inserted.change.new_values, vec![
                        ChangesetValue::Text("nul\0unicode: λ".to_owned()),
                        ChangesetValue::Integer(-7), ChangesetValue::Blob(vec![0, 255]),
                    ]);
                    let updated = reader.next(&cx).await.unwrap().unwrap();
                    assert_eq!(updated.change.op, ChangeOp::Update);
                    assert_eq!(updated.change.old_values, vec![
                        if patchset { ChangesetValue::Undefined } else { ChangesetValue::Text("old".to_owned()) },
                        ChangesetValue::Integer(1), ChangesetValue::Blob(vec![0, 255]),
                    ]);
                    assert_eq!(updated.change.new_values, vec![
                        ChangesetValue::Real(1.25), ChangesetValue::Undefined, ChangesetValue::Undefined,
                    ]);
                    assert!(reader.next(&cx).await.unwrap().is_none());
                }
            }
        });
    }

    #[test]
    fn fragmented_changesets_and_patchsets_match_existing_codec() {
        asupersync::test_utils::run_test(|| async {
            let source = fixture();
            for patchset in [false, true] {
                let wire = if patchset { source.encode_patchset() } else { source.encode() };
                let expected = if patchset { Changeset::decode_patchset(&wire).unwrap() } else { Changeset::decode(&wire).unwrap() };
                for chunk in [1, 2, 7, BUFFER_SIZE] {
                    let mut reader = ChangesetStreamReader::new(input(wire.clone(), chunk), ChangesetStreamLimits::default());
                    let cx = Cx::new();
                    for (index, expected_row) in expected.tables[0].rows.iter().enumerate() {
                        let actual = reader.next(&cx).await.unwrap().unwrap();
                        assert_eq!(actual.kind, expected.kind);
                        assert_eq!(actual.table.as_ref(), &expected.tables[0].info);
                        assert_eq!(&actual.change, expected_row);
                        assert_eq!(actual.row_index, index);
                        assert_eq!(actual.section, 1);
                    }
                    assert!(reader.next(&cx).await.unwrap().is_none());
                    assert!(reader.next(&cx).await.unwrap().is_none());
                    assert_eq!(reader.bytes_consumed(), u64::try_from(wire.len()).unwrap());
                    assert_eq!(reader.rows_decoded(), 3);
                }
            }
        });
    }

    #[test]
    fn partial_fields_fail_and_poison_the_reader() {
        asupersync::test_utils::run_test(|| async {
            let mut source = fixture();
            source.tables[0].rows.truncate(1);
            let wire = source.encode();
            let mut header = Vec::new();
            source.tables[0].info.encode(&mut header);
            for cut in 1..wire.len() {
                // EOF immediately after a complete table header is valid.
                if cut == header.len() { continue; }
                let mut reader = ChangesetStreamReader::new(input(wire[..cut].to_vec(), 1), ChangesetStreamLimits::default());
                let cx = Cx::new();
                assert!(reader.next(&cx).await.is_err(), "cut={cut}");
                assert!(matches!(reader.next(&cx).await, Err(ChangesetStreamError::Poisoned)));
            }
        });
    }

    #[test]
    fn untrusted_lengths_and_row_payloads_are_bounded_before_allocation() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            // Header claims u64::MAX columns via SQLite's nine-byte varint.
            let mut huge = vec![b'T']; huge.extend_from_slice(&[255; 9]);
            let mut reader = ChangesetStreamReader::new(input(huge, 1), ChangesetStreamLimits::default());
            assert!(matches!(reader.next(&cx).await, Err(ChangesetStreamError::Limit { resource: "columns", .. })));
            let wire = fixture().encode();
            for limits in [
                ChangesetStreamLimits { max_value_bytes: 1, ..ChangesetStreamLimits::default() },
                ChangesetStreamLimits { max_row_bytes: 1, ..ChangesetStreamLimits::default() },
                ChangesetStreamLimits { max_table_name_bytes: 1, ..ChangesetStreamLimits::default() },
                ChangesetStreamLimits { max_input_bytes: 1, ..ChangesetStreamLimits::default() },
                ChangesetStreamLimits { max_rows: 0, ..ChangesetStreamLimits::default() },
                ChangesetStreamLimits { max_table_sections: 0, ..ChangesetStreamLimits::default() },
            ] {
                let mut reader = ChangesetStreamReader::new(input(wire.clone(), 1), limits);
                assert!(matches!(reader.next(&cx).await, Err(ChangesetStreamError::Limit { .. })));
            }
        });
    }

    #[test]
    fn mixed_kinds_invalid_tags_and_flags_are_rejected() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            for wire in [
                vec![b'T', 1, 1, b't', 0, b'P', 1, 1, b't', 0],
                vec![b'T', 1, 1, b't', 0, 18, 2, 5],
                vec![b'T', 1, 1, b't', 0, 18, 0, 6],
                vec![b'T', 1, 1, b't', 0, 255],
                vec![b'T', 1, 0, b't', 0],
                vec![b'T', 1, 1, 255, 0],
                vec![b'T', 1, 1, b't', 0, 18, 0, 3, 1, 255],
            ] {
                let mut reader = ChangesetStreamReader::new(input(wire, 1), ChangesetStreamLimits::default());
                assert!(matches!(reader.next(&cx).await, Err(ChangesetStreamError::Malformed { .. })));
            }
        });
    }

    #[test]
    fn dropped_partial_read_cannot_resume_from_a_field_interior() {
        asupersync::test_utils::run_test(|| async {
            let mut source = input(fixture().encode(), 1);
            source.pending_at = Some(3);
            let mut reader = ChangesetStreamReader::new(source, ChangesetStreamLimits::default());
            let cx = Cx::new();
            {
                let mut future = std::pin::pin!(reader.next(&cx));
                std::future::poll_fn(|task_cx| {
                    assert!(std::future::Future::poll(future.as_mut(), task_cx).is_pending());
                    Poll::Ready(())
                }).await;
            }
            assert!(matches!(reader.next(&cx).await, Err(ChangesetStreamError::Poisoned)));
        });
    }

    #[test]
    fn cancelled_context_stops_even_buffered_input() {
        asupersync::test_utils::run_test(|| async {
            let mut reader = ChangesetStreamReader::new(input(fixture().encode(), BUFFER_SIZE), ChangesetStreamLimits::default());
            let cx = Cx::new();
            reader.next(&cx).await.unwrap().unwrap();
            cx.cancel();
            assert!(matches!(reader.next(&cx).await, Err(ChangesetStreamError::Cancelled)));
        });
    }

    #[test]
    fn metadata_is_shared_within_sections_and_empty_sections_are_valid() {
        asupersync::test_utils::run_test(|| async {
            let mut wire = fixture().encode();
            wire.extend_from_slice(&[b'T', 1, 1, b'e', 0]);
            let mut reader = ChangesetStreamReader::new(input(wire, BUFFER_SIZE), ChangesetStreamLimits::default());
            let cx = Cx::new();
            let first = reader.next(&cx).await.unwrap().unwrap();
            let second = reader.next(&cx).await.unwrap().unwrap();
            assert!(Arc::ptr_eq(&first.table, &second.table));
            reader.next(&cx).await.unwrap().unwrap();
            assert!(reader.next(&cx).await.unwrap().is_none());
        });
    }
}
