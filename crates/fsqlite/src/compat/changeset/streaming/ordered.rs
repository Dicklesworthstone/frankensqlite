//! Ordered, content-bound Session replication with an atomic SQL cursor.
//!
//! A stream is explicitly seeded from a trusted baseline. Each message binds
//! that stream, its next sequence, its predecessor, and its verified frame.
//! Rows and the new cursor commit in ONE ordinary concurrent transaction.
//! This is logical Session replication, not native ECS marker replay, source
//! change capture, leader election, encryption, or schema migration.
//!
//! Trust the envelope identity independently of its transport. The destination
//! schema and local SQL remain trusted: this is not a sandbox for hostile
//! triggers or functions. SQL/trigger effects are provisional until COMMIT;
//! external effects of user functions cannot be undone. Retained input is one
//! verified chunk plus one decoded row, not the entire changeset. Transaction
//! storage can still grow with the write set.

use std::fmt;

use asupersync::io::AsyncRead;
use fsqlite_ast::Statement;
use fsqlite_parser::Parser;
use fsqlite_types::{PayloadHash, cx::Cx};

use super::{ApplyTransaction, ConflictAction, StreamApplyError, apply_rows};
use crate::compat::changeset::SqlChangesetApplyReport;
use crate::compat::changeset_stream::verified::{
    ChangesetFrame, MAX_CHANGESET_FRAME_CHUNKS, VerifiedChangesetReader,
};
use crate::compat::changeset_stream::{ChangesetStreamLimits, ChangesetStreamReader};
use crate::{Connection, FrankenError, Row, SqliteValue};

const MAGIC: &[u8; 8] = b"FSCSOR01";
const HEADER_BYTES: usize = 80;
const MAX_ENVELOPE_BYTES: usize = HEADER_BYTES + 20 + MAX_CHANGESET_FRAME_CHUNKS * 32;
const ID_DOMAIN: &str = "fsqlite:ordered-session-envelope:v1";
const CURSOR_TABLE: &str = "_fsqlite_replica_cursor_v1";
const CREATE_CURSOR: &str = "CREATE TABLE IF NOT EXISTS main._fsqlite_replica_cursor_v1(\
    stream_id BLOB PRIMARY KEY NOT NULL, sequence INTEGER NOT NULL, tip BLOB NOT NULL)";

/// Domain-separated BLAKE3 through the shared types crate, not a new facade
/// dependency. The canonical hash input is domain || NUL || envelope bytes.
fn envelope_id(bytes: &[u8]) -> PayloadHash {
    let mut input = Vec::with_capacity(ID_DOMAIN.len() + 1 + bytes.len());
    input.extend_from_slice(ID_DOMAIN.as_bytes());
    input.push(0);
    input.extend_from_slice(bytes);
    PayloadHash::blake3(&input)
}

/// Immutable ordering and content commitment for a single Session message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationEnvelope {
    stream_id: PayloadHash,
    sequence: u64,
    previous: PayloadHash,
    frame: ChangesetFrame,
}

impl ReplicationEnvelope {
    /// Construct at the sender. Sequence zero belongs to the baseline, not a
    /// message. The SQL cursor deliberately uses the signed INTEGER range.
    pub fn new(
        stream_id: PayloadHash,
        sequence: u64,
        previous: PayloadHash,
        frame: ChangesetFrame,
    ) -> Result<Self, ReplicaApplyError> {
        if sequence == 0 || i64::try_from(sequence).is_err() {
            return Err(protocol("replication sequence is outside 1..=i64::MAX"));
        }
        Ok(Self { stream_id, sequence, previous, frame })
    }

    #[must_use]
    pub const fn stream_id(&self) -> PayloadHash { self.stream_id }

    #[must_use]
    pub const fn sequence(&self) -> u64 { self.sequence }

    #[must_use]
    pub const fn previous(&self) -> PayloadHash { self.previous }

    #[must_use]
    pub const fn frame(&self) -> &ChangesetFrame { &self.frame }

    /// Magic, stream(32), sequence(u64 LE), predecessor(32), frame descriptor.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let frame = self.frame.encode();
        let mut bytes = Vec::with_capacity(HEADER_BYTES + frame.len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(self.stream_id.as_bytes());
        bytes.extend_from_slice(&self.sequence.to_le_bytes());
        bytes.extend_from_slice(self.previous.as_bytes());
        bytes.extend_from_slice(&frame);
        bytes
    }

    #[must_use]
    pub fn id(&self) -> PayloadHash {
        envelope_id(&self.encode())
    }

    /// Verify the independently trusted envelope root before allocating its
    /// frame. Frame identity is then trusted transitively through that root.
    pub fn decode(
        bytes: &[u8],
        expected_id: PayloadHash,
        max_message_bytes: u64,
    ) -> Result<Self, ReplicaApplyError> {
        if !(HEADER_BYTES..=MAX_ENVELOPE_BYTES).contains(&bytes.len())
            || &bytes[..8] != MAGIC
        {
            return Err(protocol("invalid ordered changeset envelope"));
        }
        if envelope_id(bytes) != expected_id {
            return Err(protocol("ordered changeset envelope identity mismatch"));
        }
        let stream_id = PayloadHash::from_bytes(bytes[8..40].try_into().expect("header width"));
        let sequence = u64::from_le_bytes(bytes[40..48].try_into().expect("header width"));
        let previous = PayloadHash::from_bytes(bytes[48..80].try_into().expect("header width"));
        let descriptor = &bytes[HEADER_BYTES..];
        let frame = ChangesetFrame::decode(
            descriptor, PayloadHash::blake3(descriptor), max_message_bytes,
        )?;
        Self::new(stream_id, sequence, previous, frame)
    }
}

/// Last accepted position in one stream; stored with that stream's SQL data.
/// A baseline tip is supplied by the owner of an already installed snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaCheckpoint {
    pub stream_id: PayloadHash,
    pub sequence: u64,
    pub tip: PayloadHash,
}

/// Retries of the CURRENT committed message do not execute SQL rows again.
/// Older messages are rejected as Stale, not certified as historical matches:
/// only the current cursor is retained, so metadata stays bounded per stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaDisposition {
    Applied(SqlChangesetApplyReport),
    AlreadyApplied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "a replication receipt exists only after successful COMMIT"]
pub struct ReplicaApplyReceipt {
    pub checkpoint: ReplicaCheckpoint,
    pub disposition: ReplicaDisposition,
}

/// SQL commit errors preserve outcome uncertainty. After an error or dropped
/// future, settle/reopen the connection and inspect current_checkpoint before
/// deciding whether to resend. No automatic transaction replay occurs here.
#[derive(Debug)]
pub enum ReplicaApplyError {
    Protocol { detail: &'static str },
    Uninitialized,
    Gap { expected: u64, received: u64 },
    Stale { current: u64, received: u64 },
    Diverged { sequence: u64 },
    Stream(StreamApplyError),
    Database(FrankenError),
    Rollback { cause: Box<Self>, rollback: FrankenError },
}

impl fmt::Display for ReplicaApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol { detail } => write!(f, "replication protocol: {detail}"),
            Self::Uninitialized => f.write_str("replication stream has no trusted baseline"),
            Self::Gap { expected, received } => write!(f, "replication gap: expected {expected}, received {received}"),
            Self::Stale { current, received } => write!(f, "stale replication message {received}; current is {current}"),
            Self::Diverged { sequence } => write!(f, "replication history diverges at sequence {sequence}"),
            Self::Stream(error) => write!(f, "{error}"),
            Self::Database(error) => write!(f, "{error}"),
            Self::Rollback { cause, rollback } => write!(f, "{cause}; replication rollback also failed: {rollback}"),
        }
    }
}

impl std::error::Error for ReplicaApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Stream(error) => Some(error),
            Self::Database(error) => Some(error),
            Self::Rollback { cause, .. } => Some(cause.as_ref()),
            _ => None,
        }
    }
}

impl From<FrankenError> for ReplicaApplyError {
    fn from(error: FrankenError) -> Self { Self::Database(error) }
}

impl From<StreamApplyError> for ReplicaApplyError {
    fn from(error: StreamApplyError) -> Self { Self::Stream(error) }
}

const fn protocol(detail: &'static str) -> ReplicaApplyError {
    ReplicaApplyError::Protocol { detail }
}

fn checkpoint(cx: &Cx) -> Result<(), ReplicaApplyError> {
    cx.checkpoint().map_err(|_| FrankenError::Interrupt.into())
}

fn blob(hash: PayloadHash) -> SqliteValue {
    SqliteValue::Blob(hash.as_bytes().to_vec().into())
}

fn row_text(row: &Row, column: usize) -> Option<&str> {
    match row.get(column) {
        Some(SqliteValue::Text(value)) => Some(value),
        _ => None,
    }
}

async fn idle(conn: &Connection) -> Result<(), ReplicaApplyError> {
    // Discharge an abandoned owner, but never take over a live transaction.
    conn.execute_batch("").await?;
    if conn.in_transaction() { return Err(FrankenError::NestedTransaction.into()); }
    Ok(())
}

async fn rollback(
    transaction: &mut ApplyTransaction<'_>,
    cause: ReplicaApplyError,
) -> ReplicaApplyError {
    if !transaction.conn.in_transaction() {
        transaction.armed = false;
        return cause;
    }
    match transaction.conn.rollback_transaction().await {
        Ok(()) => { transaction.armed = false; cause }
        Err(rollback) => ReplicaApplyError::Rollback { cause: Box::new(cause), rollback },
    }
}

async fn begin(conn: &Connection) -> Result<ApplyTransaction<'_>, ReplicaApplyError> {
    idle(conn).await?;
    let mut owner = ApplyTransaction { conn, armed: true };
    if let Err(error) = conn.begin_transaction().await {
        return Err(rollback(&mut owner, error.into()).await);
    }
    Ok(owner)
}

/// Refuse a colliding user object, generated columns, or metadata triggers.
/// All callers run this inside their owning transaction. Normal concurrent
/// DDL remains subject to the engine's schema/snapshot validation at COMMIT.
async fn validate_cursor_schema(conn: &Connection) -> Result<(), ReplicaApplyError> {
    let name = SqliteValue::Text(CURSOR_TABLE.into());
    let objects = conn.query_with_params(
        "SELECT type, sql FROM main.sqlite_schema WHERE name=?1 COLLATE NOCASE", std::slice::from_ref(&name),
    ).await?;
    let [object] = objects.as_slice() else { return Err(protocol("replication cursor table is missing or ambiguous")); };
    if row_text(object, 0) != Some("table") { return Err(protocol("replication cursor is not an ordinary table")); }
    let sql = row_text(object, 1).ok_or_else(|| protocol("replication cursor has no schema"))?;
    let (statements, errors) = Parser::from_sql(sql).parse_all();
    if !errors.is_empty() || !matches!(statements.as_slice(), [Statement::CreateTable(_)]) {
        return Err(protocol("replication cursor is not an ordinary table"));
    }
    let columns = conn.query("PRAGMA main.table_xinfo('_fsqlite_replica_cursor_v1')").await?;
    let expected = [("stream_id", "BLOB", 1_i64), ("sequence", "INTEGER", 0), ("tip", "BLOB", 0)];
    if columns.len() != expected.len() { return Err(protocol("incompatible replication cursor columns")); }
    for (index, (column, (name, kind, pk))) in columns.iter().zip(expected).enumerate() {
        if column.get(0) != Some(&SqliteValue::Integer(i64::try_from(index).expect("three columns")))
            || row_text(column, 1) != Some(name)
            || !row_text(column, 2).is_some_and(|value| value.eq_ignore_ascii_case(kind))
            || column.get(3) != Some(&SqliteValue::Integer(1))
            || column.get(4) != Some(&SqliteValue::Null)
            || column.get(5) != Some(&SqliteValue::Integer(pk))
            || column.get(6) != Some(&SqliteValue::Integer(0))
        {
            return Err(protocol("incompatible replication cursor columns"));
        }
    }
    for catalog in ["main", "temp"] {
        let triggers = conn.query_with_params(
            &format!("SELECT name FROM {catalog}.sqlite_schema WHERE type='trigger' AND tbl_name=?1 COLLATE NOCASE"),
            std::slice::from_ref(&name),
        ).await?;
        if !triggers.is_empty() { return Err(protocol("replication cursor must not have triggers")); }
    }
    Ok(())
}

async fn load_cursor(
    conn: &Connection,
    stream_id: PayloadHash,
) -> Result<Option<ReplicaCheckpoint>, ReplicaApplyError> {
    let rows = conn.query_with_params(
        "SELECT sequence, tip FROM main._fsqlite_replica_cursor_v1 WHERE stream_id=?1 LIMIT 2", &[blob(stream_id)],
    ).await?;
    let row = match rows.as_slice() {
        [] => return Ok(None),
        [row] => row,
        _ => return Err(protocol("replication cursor is not unique")),
    };
    let Some(SqliteValue::Integer(sequence)) = row.get(0) else { return Err(protocol("invalid replication cursor sequence")); };
    let sequence = u64::try_from(*sequence).map_err(|_| protocol("negative replication cursor sequence"))?;
    let Some(SqliteValue::Blob(bytes)) = row.get(1) else { return Err(protocol("invalid replication cursor tip")); };
    let tip = PayloadHash::from_bytes(bytes.as_ref().try_into().map_err(|_| protocol("invalid replication cursor tip width"))?);
    Ok(Some(ReplicaCheckpoint { stream_id, sequence, tip }))
}

/// Seed an already installed, coherent baseline. The caller supplies its
/// trusted stream/position/tip; this does not capture or verify a snapshot.
/// Initialization is insert-only and idempotent for the SAME baseline. It can
/// never reset a progressed stream or claim that existing unrelated data is a
/// replica. A failed/dropped initialization owns the usual deferred rollback.
pub async fn initialize(
    conn: &mut Connection,
    cx: &Cx,
    baseline: ReplicaCheckpoint,
) -> Result<ReplicaCheckpoint, ReplicaApplyError> {
    checkpoint(cx)?;
    let sequence = i64::try_from(baseline.sequence).map_err(|_| protocol("baseline sequence exceeds i64::MAX"))?;
    let mut owner = begin(conn).await?;
    let result = async {
        conn.execute(CREATE_CURSOR).await?;
        validate_cursor_schema(conn).await?;
        match load_cursor(conn, baseline.stream_id).await? {
            Some(current) if current == baseline => {}
            Some(_) => return Err(protocol("replication baseline cannot replace an existing cursor")),
            None => {
                let changed = conn.execute_with_params(
                    "INSERT OR ABORT INTO main._fsqlite_replica_cursor_v1(stream_id,sequence,tip) VALUES(?1,?2,?3)",
                    &[blob(baseline.stream_id), SqliteValue::Integer(sequence), blob(baseline.tip)],
                ).await?;
                if changed != 1 || load_cursor(conn, baseline.stream_id).await? != Some(baseline) {
                    return Err(protocol("replication baseline was not installed exactly"));
                }
            }
        }
        checkpoint(cx)?;
        conn.commit_transaction().await?;
        Ok(baseline)
    }.await;
    match result {
        Ok(baseline) => { owner.armed = false; Ok(baseline) }
        Err(error) => Err(rollback(&mut owner, error).await),
    }
}

/// Read a committed cursor for restart or uncertain-outcome reconciliation.
/// A caller-owned transaction is refused, so provisional progress is never
/// returned as a receipt. A newer cursor does not certify any particular old
/// message: keep trusted source history when that distinction is needed.
pub async fn current_checkpoint(
    conn: &mut Connection,
    cx: &Cx,
    stream_id: PayloadHash,
) -> Result<Option<ReplicaCheckpoint>, ReplicaApplyError> {
    checkpoint(cx)?;
    let mut owner = begin(conn).await?;
    let result = async {
        validate_cursor_schema(conn).await?;
        let current = load_cursor(conn, stream_id).await?;
        checkpoint(cx)?;
        conn.commit_transaction().await?;
        Ok(current)
    }.await;
    match result {
        Ok(current) => { owner.armed = false; Ok(current) }
        Err(error) => Err(rollback(&mut owner, error).await),
    }
}

fn classify(
    current: ReplicaCheckpoint,
    message: &ReplicationEnvelope,
    identity: PayloadHash,
) -> Result<bool, ReplicaApplyError> {
    if message.sequence < current.sequence {
        return Err(ReplicaApplyError::Stale { current: current.sequence, received: message.sequence });
    }
    if message.sequence == current.sequence {
        if identity != current.tip { return Err(ReplicaApplyError::Diverged { sequence: message.sequence }); }
        return Ok(false);
    }
    let expected = current.sequence.checked_add(1).ok_or_else(|| protocol("replication sequence exhausted"))?;
    if message.sequence != expected { return Err(ReplicaApplyError::Gap { expected, received: message.sequence }); }
    if message.previous != current.tip { return Err(ReplicaApplyError::Diverged { sequence: message.sequence }); }
    Ok(true)
}

async fn advance_cursor(
    conn: &Connection,
    previous: ReplicaCheckpoint,
    next: ReplicaCheckpoint,
) -> Result<(), ReplicaApplyError> {
    // Revalidate after user DML as well. Direct payload writes to this table
    // are excluded by apply_rows; indirect trigger tampering must fail too.
    validate_cursor_schema(conn).await?;
    let changed = conn.execute_with_params(
        "UPDATE OR ABORT main._fsqlite_replica_cursor_v1 SET sequence=?1, tip=?2 \
         WHERE stream_id=?3 AND sequence=?4 AND tip=?5",
        &[
            SqliteValue::Integer(i64::try_from(next.sequence).expect("admitted sequence")), blob(next.tip),
            blob(next.stream_id), SqliteValue::Integer(i64::try_from(previous.sequence).expect("SQL cursor")),
            blob(previous.tip),
        ],
    ).await?;
    if changed != 1 || load_cursor(conn, next.stream_id).await? != Some(next) {
        return Err(protocol("replication cursor changed during apply"));
    }
    Ok(())
}

/// Apply exactly the next trusted message, with strict existing SQL conflicts.
///
/// Admission, cursor lookup, streamed DML and cursor advancement share one
/// transaction. Only a successfully committed result is acknowledged. Empty
/// messages also advance the cursor. No replacement/omit policy can silently
/// diverge a replica, and no concurrency mode or foreign-key policy is changed.
///
/// The CURRENT exact duplicate returns AlreadyApplied without reading input
/// or running rows/triggers. Other sequence/predecessor failures also leave
/// input unread. On a shared transport the caller must discard/reframe that
/// body explicitly. A newly applied frame consumes only its declared bytes,
/// never the next message. On failure the input can be partially consumed.
///
/// cx controls input checkpoints; SQL uses the connection's own environment.
/// Configure related lineages when both must cancel together. A stalled input
/// must arrange its own wakeup or its caller must drop this future. Drop owns
/// deferred rollback even while awaiting BEGIN, a later chunk, or COMMIT.
pub async fn apply<R: AsyncRead + Unpin>(
    conn: &mut Connection,
    cx: &Cx,
    input: &mut R,
    message: &ReplicationEnvelope,
    expected_id: PayloadHash,
    mut limits: ChangesetStreamLimits,
) -> Result<ReplicaApplyReceipt, ReplicaApplyError> {
    checkpoint(cx)?;
    if message.id() != expected_id { return Err(protocol("ordered changeset envelope identity mismatch")); }
    if message.frame.byte_len() > limits.max_input_bytes { return Err(FrankenError::TooBig.into()); }
    limits.max_input_bytes = message.frame.byte_len();
    let mut owner = begin(conn).await?;
    let result = async {
        validate_cursor_schema(conn).await?;
        let current = load_cursor(conn, message.stream_id).await?.ok_or(ReplicaApplyError::Uninitialized)?;
        let receipt = if classify(current, message, expected_id)? {
            let mut verified = VerifiedChangesetReader::new(
                input, message.frame.clone(), message.frame.id(), limits.max_input_bytes,
            )?;
            let mut reader = ChangesetStreamReader::new(&mut verified, limits);
            let first = reader.next(cx).await.map_err(StreamApplyError::from)?;
            let report = match first {
                Some(first) => apply_rows(
                    conn, cx, &mut reader, first, &mut |_| ConflictAction::Abort, Some(CURSOR_TABLE),
                ).await?,
                None => SqlChangesetApplyReport::default(),
            };
            let consumed = reader.bytes_consumed();
            drop(reader);
            if consumed != message.frame.byte_len() || !verified.is_complete() {
                return Err(protocol("ordered changeset frame was not completely consumed"));
            }
            checkpoint(cx)?;
            let next = ReplicaCheckpoint { stream_id: message.stream_id, sequence: message.sequence, tip: expected_id };
            advance_cursor(conn, current, next).await?;
            ReplicaApplyReceipt { checkpoint: next, disposition: ReplicaDisposition::Applied(report) }
        } else {
            ReplicaApplyReceipt { checkpoint: current, disposition: ReplicaDisposition::AlreadyApplied }
        };
        checkpoint(cx)?;
        conn.commit_transaction().await?;
        Ok(receipt)
    }.await;
    match result {
        Ok(receipt) => { owner.armed = false; Ok(receipt) }
        Err(error) => Err(rollback(&mut owner, error).await),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::io::{AsyncRead, ReadBuf};
    use fsqlite_ext_session::{ChangeOp, Changeset, ChangesetKind, ChangesetRow, ChangesetValue, TableChangeset, TableInfo};
    use crate::compat::changeset_stream::verified::CHANGESET_FRAME_CHUNK_BYTES;
    use std::cell::Cell;
    use std::future::{Future, poll_fn};
    use std::io;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll};

    struct Input {
        bytes: Vec<u8>,
        offset: usize,
        chunk: usize,
        reads: usize,
    }

    impl Input {
        fn new(bytes: Vec<u8>) -> Self { Self { bytes, offset: 0, chunk: 3, reads: 0 } }
    }

    impl AsyncRead for Input {
        fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.reads += 1;
            let count = this.chunk.min(buf.remaining()).min(this.bytes.len() - this.offset);
            buf.put_slice(&this.bytes[this.offset..this.offset + count]);
            this.offset += count;
            Poll::Ready(Ok(()))
        }
    }

    const fn hash(byte: u8) -> PayloadHash { PayloadHash::from_bytes([byte; 32]) }

    const fn baseline() -> ReplicaCheckpoint {
        ReplicaCheckpoint { stream_id: hash(1), sequence: 0, tip: hash(2) }
    }

    fn insert(id: i64, text: &str) -> ChangesetRow {
        ChangesetRow {
            op: ChangeOp::Insert, indirect: false, old_values: Vec::new(),
            new_values: vec![ChangesetValue::Integer(id), ChangesetValue::Text(text.to_owned())],
        }
    }

    fn table(name: &str, pk: Vec<bool>, rows: Vec<ChangesetRow>) -> TableChangeset {
        TableChangeset { info: TableInfo { name: name.to_owned(), column_count: pk.len(), pk_flags: pk }, rows }
    }

    fn wire(rows: Vec<ChangesetRow>) -> Vec<u8> {
        Changeset { kind: ChangesetKind::Changeset, tables: vec![table("t", vec![true, false], rows)] }.encode()
    }

    fn envelope(previous: ReplicaCheckpoint, bytes: &[u8]) -> ReplicationEnvelope {
        ReplicationEnvelope::new(
            previous.stream_id, previous.sequence + 1, previous.tip,
            ChangesetFrame::for_message(&Cx::new(), bytes, 1 << 20).unwrap(),
        ).unwrap()
    }

    async fn setup() -> Connection {
        let mut conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE); \
            CREATE TABLE audit(id INTEGER PRIMARY KEY); \
            CREATE TRIGGER log_t AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END;")
            .await.unwrap();
        initialize(&mut conn, &Cx::new(), baseline()).await.unwrap();
        conn
    }

    async fn count(conn: &Connection, table: &str) -> i64 {
        let row = conn.query_row(&format!("SELECT count(*) FROM {table}")).await.unwrap();
        let Some(SqliteValue::Integer(value)) = row.get(0) else { panic!("integer count"); };
        *value
    }

    async fn assert_unapplied(conn: &mut Connection) {
        assert_eq!(count(conn, "t").await, 0);
        assert_eq!(count(conn, "audit").await, 0);
        assert_eq!(current_checkpoint(conn, &Cx::new(), baseline().stream_id).await.unwrap(), Some(baseline()));
        assert!(!conn.in_transaction());
    }

    #[test]
    fn ordered_envelope_binds_stream_position_predecessor_and_content() {
        let bytes = wire(vec![insert(1, "one")]);
        let message = envelope(baseline(), &bytes);
        let encoded = message.encode();
        assert_eq!(ReplicationEnvelope::decode(&encoded, message.id(), 1 << 20).unwrap(), message);
        assert_ne!(message.id(), PayloadHash::blake3(&encoded), "envelope hashes are domain separated");
        for offset in [0, 8, 40, 48, encoded.len() - 1] {
            let mut altered = encoded.clone();
            altered[offset] ^= 1;
            assert!(ReplicationEnvelope::decode(&altered, message.id(), 1 << 20).is_err());
        }
        assert!(ReplicationEnvelope::decode(&encoded, message.id(), 1).is_err());
        for length in [0, 7, 79, encoded.len() - 1] {
            assert!(ReplicationEnvelope::decode(&encoded[..length], message.id(), 1 << 20).is_err());
        }
        for sequence in [0, u64::MAX] {
            assert!(ReplicationEnvelope::new(hash(1), sequence, hash(2), message.frame.clone()).is_err());
        }
    }

    #[test]
    fn ordered_rows_and_cursor_commit_together_and_current_retry_is_idempotent() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let cx = Cx::new();
            let first = wire(vec![insert(1, "one")]);
            let message = envelope(baseline(), &first);
            let mut transport = Input::new([first.clone(), b"next-frame".to_vec()].concat());
            let receipt = apply(&mut conn, &cx, &mut transport, &message, message.id(), ChangesetStreamLimits::default()).await.unwrap();
            assert_eq!(transport.offset, first.len(), "must not consume the next frame");
            assert_eq!(receipt.disposition, ReplicaDisposition::Applied(SqlChangesetApplyReport { applied: 1, skipped: 0, replaced: 0 }));
            assert_eq!(receipt.checkpoint.tip, message.id());
            assert_eq!(current_checkpoint(&mut conn, &cx, message.stream_id()).await.unwrap(), Some(receipt.checkpoint));
            let mut duplicate = Input::new(Vec::new());
            let retried = apply(&mut conn, &cx, &mut duplicate, &message, message.id(), ChangesetStreamLimits::default()).await.unwrap();
            assert_eq!(retried.disposition, ReplicaDisposition::AlreadyApplied);
            assert_eq!(duplicate.reads, 0);
            assert_eq!(count(&conn, "audit").await, 1, "retry must not fire the trigger twice");

            let update = ChangesetRow {
                op: ChangeOp::Update, indirect: false,
                old_values: vec![ChangesetValue::Integer(1), ChangesetValue::Text("one".to_owned())],
                new_values: vec![ChangesetValue::Undefined, ChangesetValue::Text("two".to_owned())],
            };
            let second = wire(vec![update]);
            let message2 = envelope(receipt.checkpoint, &second);
            let applied2 = apply(&mut conn, &cx, &mut Input::new(second), &message2, message2.id(), ChangesetStreamLimits::default()).await.unwrap();
            assert_eq!(applied2.checkpoint.sequence, 2);
            assert_eq!(conn.query_row("SELECT v FROM t WHERE id=1").await.unwrap().get(0), Some(&SqliteValue::Text("two".into())));
            let mut old = Input::new(first);
            assert!(matches!(apply(&mut conn, &cx, &mut old, &message, message.id(), ChangesetStreamLimits::default()).await,
                Err(ReplicaApplyError::Stale { current: 2, received: 1 })));
            assert_eq!(old.reads, 0);
        });
    }

    #[test]
    fn ordered_admission_rejects_gaps_divergence_and_wrong_trust_before_input() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let cx = Cx::new();
            let bytes = wire(vec![insert(1, "one")]);
            let frame = ChangesetFrame::for_message(&cx, &bytes, 1 << 20).unwrap();
            let gap = ReplicationEnvelope::new(hash(1), 2, hash(2), frame.clone()).unwrap();
            let mut input = Input::new(bytes.clone());
            assert!(matches!(apply(&mut conn, &cx, &mut input, &gap, gap.id(), ChangesetStreamLimits::default()).await,
                Err(ReplicaApplyError::Gap { expected: 1, received: 2 })));
            let divergent = ReplicationEnvelope::new(hash(1), 1, hash(3), frame).unwrap();
            assert!(matches!(apply(&mut conn, &cx, &mut input, &divergent, divergent.id(), ChangesetStreamLimits::default()).await,
                Err(ReplicaApplyError::Diverged { sequence: 1 })));
            let valid = envelope(baseline(), &bytes);
            assert!(matches!(apply(&mut conn, &cx, &mut input, &valid, hash(9), ChangesetStreamLimits::default()).await,
                Err(ReplicaApplyError::Protocol { .. })));
            assert_eq!(input.reads, 0);
            assert_unapplied(&mut conn).await;
        });
    }

    #[test]
    fn ordered_late_conflict_rolls_back_rows_triggers_and_cursor() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let bytes = wire(vec![insert(1, "same"), insert(2, "same")]);
            let message = envelope(baseline(), &bytes);
            assert!(matches!(apply(&mut conn, &Cx::new(), &mut Input::new(bytes), &message, message.id(), ChangesetStreamLimits::default()).await,
                Err(ReplicaApplyError::Stream(_))));
            assert_unapplied(&mut conn).await;
            let valid = wire(vec![insert(1, "valid retry")]);
            let replacement = envelope(baseline(), &valid);
            assert_eq!(apply(&mut conn, &Cx::new(), &mut Input::new(valid), &replacement, replacement.id(), ChangesetStreamLimits::default()).await.unwrap().checkpoint.sequence, 1);
        });
    }

    #[test]
    fn ordered_cursor_cannot_be_targeted_directly_or_changed_by_payload_triggers() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let mutation = ChangesetRow {
                op: ChangeOp::Delete, indirect: false, new_values: Vec::new(),
                old_values: vec![ChangesetValue::Blob(vec![1; 32]), ChangesetValue::Integer(0), ChangesetValue::Blob(vec![2; 32])],
            };
            let bytes = Changeset { kind: ChangesetKind::Changeset, tables: vec![
                table("t", vec![true, false], vec![insert(1, "before")]),
                table("_FSQLITE_REPLICA_CURSOR_V1", vec![true, false, false], vec![mutation]),
            ] }.encode();
            let message = envelope(baseline(), &bytes);
            assert!(apply(&mut conn, &Cx::new(), &mut Input::new(bytes), &message, message.id(), ChangesetStreamLimits::default()).await.is_err());
            assert_unapplied(&mut conn).await;

            conn.execute("CREATE TRIGGER tamper AFTER INSERT ON t BEGIN UPDATE _fsqlite_replica_cursor_v1 SET sequence=99; END;").await.unwrap();
            let bytes = wire(vec![insert(1, "tamper")]);
            let message = envelope(baseline(), &bytes);
            assert!(matches!(apply(&mut conn, &Cx::new(), &mut Input::new(bytes), &message, message.id(), ChangesetStreamLimits::default()).await,
                Err(ReplicaApplyError::Protocol { .. })));
            assert_unapplied(&mut conn).await;
        });
    }

    #[test]
    fn ordered_baseline_is_insert_only_and_caller_transactions_are_preserved() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let cx = Cx::new();
            assert_eq!(initialize(&mut conn, &cx, baseline()).await.unwrap(), baseline());
            let other = ReplicaCheckpoint { tip: hash(8), ..baseline() };
            assert!(matches!(initialize(&mut conn, &cx, other).await, Err(ReplicaApplyError::Protocol { .. })));
            conn.execute("BEGIN; INSERT INTO t VALUES(7,'caller');").await.unwrap();
            assert!(matches!(initialize(&mut conn, &cx, baseline()).await, Err(ReplicaApplyError::Database(FrankenError::NestedTransaction))));
            let bytes = wire(vec![insert(1, "remote")]);
            let message = envelope(baseline(), &bytes);
            let mut input = Input::new(bytes);
            assert!(matches!(apply(&mut conn, &cx, &mut input, &message, message.id(), ChangesetStreamLimits::default()).await,
                Err(ReplicaApplyError::Database(FrankenError::NestedTransaction))));
            assert_eq!(input.reads, 0);
            assert!(conn.in_transaction());
            assert_eq!(count(&conn, "t").await, 1);
            conn.execute("ROLLBACK").await.unwrap();
            assert_unapplied(&mut conn).await;
        });
    }

    #[test]
    fn ordered_file_reopen_reconciles_cursor_and_deduplicates_trigger_effects() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("ordered.db");
            let cx = Cx::new();
            let mut conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE); \
                CREATE TABLE audit(id INTEGER PRIMARY KEY); \
                CREATE TRIGGER log_t AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END;")
                .await.unwrap();
            initialize(&mut conn, &cx, baseline()).await.unwrap();
            let bytes = wire(vec![insert(1, "first")]);
            let message = envelope(baseline(), &bytes);
            let receipt = apply(&mut conn, &cx, &mut Input::new(bytes), &message,
                message.id(), ChangesetStreamLimits::default()).await.unwrap();
            conn.close().await.unwrap();

            let mut reopened = Connection::open(path.to_str().unwrap()).await.unwrap();
            assert_eq!(current_checkpoint(&mut reopened, &cx, baseline().stream_id).await.unwrap(),
                Some(receipt.checkpoint));
            assert!(initialize(&mut reopened, &cx, baseline()).await.is_err(), "never reset progress");
            let mut duplicate = Input::new(Vec::new());
            assert_eq!(apply(&mut reopened, &cx, &mut duplicate, &message, message.id(),
                ChangesetStreamLimits::default()).await.unwrap().disposition, ReplicaDisposition::AlreadyApplied);
            assert_eq!(duplicate.reads, 0);
            assert_eq!(count(&reopened, "audit").await, 1);
            let fork = envelope(baseline(), &wire(vec![insert(1, "fork")]));
            assert!(matches!(apply(&mut reopened, &cx, &mut duplicate, &fork, fork.id(),
                ChangesetStreamLimits::default()).await, Err(ReplicaApplyError::Diverged { sequence: 1 })));
            assert_eq!(duplicate.reads, 0);

            let next = wire(vec![ChangesetRow {
                op: ChangeOp::Delete, indirect: false,
                old_values: vec![ChangesetValue::Integer(1), ChangesetValue::Text("first".to_owned())],
                new_values: Vec::new(),
            }, insert(2, "second")]);
            let next_message = envelope(receipt.checkpoint, &next);
            let next_receipt = apply(&mut reopened, &cx, &mut Input::new(next), &next_message,
                next_message.id(), ChangesetStreamLimits::default()).await.unwrap();
            reopened.close().await.unwrap();

            let mut final_open = Connection::open(path.to_str().unwrap()).await.unwrap();
            assert_eq!(current_checkpoint(&mut final_open, &cx, baseline().stream_id).await.unwrap(),
                Some(next_receipt.checkpoint));
            assert_eq!(count(&final_open, "t").await, 1);
            assert_eq!(count(&final_open, "audit").await, 2);
            assert_eq!(final_open.query_row("SELECT v FROM t WHERE id=2").await.unwrap().get(0),
                Some(&SqliteValue::Text("second".into())));
            assert_eq!(final_open.query_row("PRAGMA integrity_check").await.unwrap().get(0),
                Some(&SqliteValue::Text("ok".into())));
            final_open.close().await.unwrap();
        });
    }

    fn multichunk_message() -> Vec<u8> {
        // The first row fits in verified chunk zero and is applied before the
        // second row needs another chunk. Failures there must undo that prefix.
        wire(vec![insert(1, "verified prefix"), insert(2, &"x".repeat(CHANGESET_FRAME_CHUNK_BYTES + 17))])
    }

    #[test]
    fn ordered_late_chunk_corruption_truncation_and_limits_undo_the_prefix() {
        asupersync::test_utils::run_test(|| async {
            let bytes = multichunk_message();
            let message = envelope(baseline(), &bytes);
            assert!(bytes.len() > CHANGESET_FRAME_CHUNK_BYTES);
            let mut altered = bytes.clone();
            let tail = altered.len() - 1;
            altered[tail] ^= 1; // Still valid text, but not the trusted content.
            for input_bytes in [altered, bytes[..CHANGESET_FRAME_CHUNK_BYTES].to_vec()] {
                let mut conn = setup().await;
                let mut input = Input::new(input_bytes);
                input.chunk = 8192;
                let error = apply(&mut conn, &Cx::new(), &mut input, &message, message.id(),
                    ChangesetStreamLimits::default()).await.unwrap_err();
                assert!(matches!(error, ReplicaApplyError::Stream(StreamApplyError::Input(_))));
                assert!(input.offset >= CHANGESET_FRAME_CHUNK_BYTES);
                assert_unapplied(&mut conn).await;
            }
            let mut conn = setup().await;
            let mut input = Input::new(bytes);
            input.chunk = 8192;
            let limits = ChangesetStreamLimits { max_rows: 1, ..ChangesetStreamLimits::default() };
            assert!(matches!(apply(&mut conn, &Cx::new(), &mut input, &message, message.id(), limits).await,
                Err(ReplicaApplyError::Stream(StreamApplyError::Input(
                    crate::compat::changeset_stream::ChangesetStreamError::Limit { resource: "rows", .. }
                )))));
            assert_unapplied(&mut conn).await;
        });
    }

    struct PausedInput {
        input: Input,
        stop: usize,
        reached: Rc<Cell<bool>>,
    }

    impl AsyncRead for PausedInput {
        fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, out: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if out.remaining() == 0 { return Poll::Ready(Ok(())); }
            if this.input.offset == this.stop {
                this.reached.set(true);
                return Poll::Pending;
            }
            let n = out.remaining().min(this.input.chunk)
                .min(this.input.bytes.len() - this.input.offset)
                .min(this.stop - this.input.offset);
            out.put_slice(&this.input.bytes[this.input.offset..this.input.offset + n]);
            this.input.offset += n;
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn ordered_dropped_future_waiting_for_later_chunk_rolls_back_before_reconciliation() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let cx = Cx::new();
            let bytes = multichunk_message();
            let message = envelope(baseline(), &bytes);
            let reached = Rc::new(Cell::new(false));
            let mut input = PausedInput {
                input: Input { bytes: bytes.clone(), offset: 0, chunk: 8192, reads: 0 },
                stop: CHANGESET_FRAME_CHUNK_BYTES,
                reached: Rc::clone(&reached),
            };
            let mut operation = Box::pin(apply(&mut conn, &cx, &mut input, &message,
                message.id(), ChangesetStreamLimits::default()));
            poll_fn(|task_cx| {
                assert!(operation.as_mut().poll(task_cx).is_pending(), "the second chunk must suspend");
                if reached.get() { Poll::Ready(()) } else { Poll::Pending }
            }).await;
            drop(operation);
            // Reconciliation itself must settle deferred cleanup. Do not run
            // a separate SQL statement first that could conceal a missing guard.
            assert_eq!(current_checkpoint(&mut conn, &cx, baseline().stream_id).await.unwrap(), Some(baseline()));
            assert_unapplied(&mut conn).await;
            let mut retry = Input::new(bytes);
            retry.chunk = 8192;
            assert_eq!(apply(&mut conn, &cx, &mut retry, &message, message.id(),
                ChangesetStreamLimits::default()).await.unwrap().checkpoint.sequence, 1);
            assert_eq!(count(&conn, "t").await, 2);
            assert_eq!(count(&conn, "audit").await, 2);
        });
    }

    #[test]
    fn ordered_commit_time_foreign_key_failure_rolls_back_advanced_cursor() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            let cx = Cx::new();
            conn.execute("PRAGMA foreign_keys=ON; CREATE TABLE parent(id INTEGER PRIMARY KEY); \
                CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED); \
                CREATE TABLE audit(id INTEGER PRIMARY KEY); \
                CREATE TRIGGER log_t AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END;")
                .await.unwrap();
            initialize(&mut conn, &cx, baseline()).await.unwrap();
            let bytes = wire(vec![insert(1, "99")]);
            let message = envelope(baseline(), &bytes);
            assert!(matches!(apply(&mut conn, &cx, &mut Input::new(bytes.clone()), &message,
                message.id(), ChangesetStreamLimits::default()).await, Err(ReplicaApplyError::Database(_))));
            assert_unapplied(&mut conn).await;
            conn.execute("INSERT INTO parent VALUES(99)").await.unwrap();
            assert_eq!(apply(&mut conn, &cx, &mut Input::new(bytes), &message,
                message.id(), ChangesetStreamLimits::default()).await.unwrap().checkpoint.sequence, 1);
            assert_eq!(count(&conn, "t").await, 1);
            assert_eq!(count(&conn, "audit").await, 1);
        });
    }

    #[test]
    fn ordered_empty_messages_checkpoint_without_input_and_stop_at_signed_limit() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            let cx = Cx::new();
            let max_sequence = u64::try_from(i64::MAX).unwrap();
            let near_limit = ReplicaCheckpoint { sequence: max_sequence - 1, ..baseline() };
            initialize(&mut conn, &cx, near_limit).await.unwrap();
            let message = envelope(near_limit, &[]);
            let mut input = Input::new(b"next message must stay unread".to_vec());
            let receipt = apply(&mut conn, &cx, &mut input, &message, message.id(),
                ChangesetStreamLimits::default()).await.unwrap();
            assert_eq!(receipt.checkpoint.sequence, max_sequence);
            assert_eq!(receipt.disposition, ReplicaDisposition::Applied(SqlChangesetApplyReport::default()));
            assert_eq!(input.reads, 0);
            assert_eq!(current_checkpoint(&mut conn, &cx, baseline().stream_id).await.unwrap(), Some(receipt.checkpoint));
            assert!(ReplicationEnvelope::new(hash(1), max_sequence + 1, message.id(), message.frame.clone()).is_err());
            let absent = ReplicationEnvelope::new(hash(3), 1, hash(2), message.frame.clone()).unwrap();
            assert!(matches!(apply(&mut conn, &cx, &mut input, &absent, absent.id(),
                ChangesetStreamLimits::default()).await, Err(ReplicaApplyError::Uninitialized)));
            let cancelled = Cx::new();
            cancelled.cancel();
            assert!(matches!(apply(&mut conn, &cancelled, &mut input, &message, message.id(),
                ChangesetStreamLimits::default()).await, Err(ReplicaApplyError::Database(FrankenError::Interrupt))));
            assert_eq!(input.reads, 0);
        });
    }

    #[test]
    fn ordered_cursor_schema_collisions_and_metadata_triggers_are_not_adopted() {
        asupersync::test_utils::run_test(|| async {
            for definition in [
                "CREATE TABLE _fsqlite_replica_cursor_v1(stream_id TEXT PRIMARY KEY NOT NULL, sequence INTEGER NOT NULL, tip BLOB NOT NULL)",
                "CREATE VIEW _fsqlite_replica_cursor_v1 AS SELECT 1 AS stream_id, 0 AS sequence, NULL AS tip",
            ] {
                let mut conn = Connection::open(":memory:").await.unwrap();
                conn.execute(definition).await.unwrap();
                let original = conn.query_row("SELECT sql FROM main.sqlite_schema WHERE name='_fsqlite_replica_cursor_v1'")
                    .await.unwrap().get(0).unwrap().clone();
                assert!(initialize(&mut conn, &Cx::new(), baseline()).await.is_err());
                assert_eq!(conn.query_row("SELECT sql FROM main.sqlite_schema WHERE name='_fsqlite_replica_cursor_v1'")
                    .await.unwrap().get(0), Some(&original));
                assert!(!conn.in_transaction());
            }
            for temporary in [false, true] {
                let mut conn = Connection::open(":memory:").await.unwrap();
                conn.execute(CREATE_CURSOR).await.unwrap();
                let prefix = if temporary { "TEMP " } else { "" };
                conn.execute(&format!("CREATE {prefix}TRIGGER reject_cursor BEFORE INSERT ON main._fsqlite_replica_cursor_v1 \
                    BEGIN SELECT RAISE(ABORT,'metadata trigger must not run'); END")).await.unwrap();
                assert!(matches!(initialize(&mut conn, &Cx::new(), baseline()).await,
                    Err(ReplicaApplyError::Protocol { detail: "replication cursor must not have triggers" })));
                assert_eq!(count(&conn, CURSOR_TABLE).await, 0);
                assert!(!conn.in_transaction());
            }
        });
    }
}
