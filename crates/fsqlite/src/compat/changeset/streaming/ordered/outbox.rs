//! Transactional producer queue for ordered Session replication.
//!
//! `record` applies a caller-supplied Session message and saves its exact bytes
//! and next source position in the SAME SQL transaction. It does not capture
//! arbitrary SQL, synthesize trigger changes, or provide a transport. Source
//! and replica schemas, trigger behavior and the initial snapshot must agree.
//! Writes outside this API are not captured. Reserve both metadata tables for
//! this owner; local SQL/functions/triggers are trusted, not sandboxed here.
//!
//! Reopen and read the queue after an uncertain commit rather than inventing a
//! new sequence. Current-message retries are idempotent. Limits bound retained
//! wire bytes/messages per stream, not database pages, transaction memory or
//! total RSS. SQL uses the caller's unchanged connection environment; `Cx`
//! governs ingestion and publication checkpoints. No runtime or writer mutex
//! is created, and the ordinary concurrent-writer mode is never disabled.
//!
//! Acknowledgements refer to ONE configured downstream delivery obligation
//! per stream. Authenticate the replica and its committed checkpoint before
//! calling `acknowledge`; a constructed checkpoint is not a signature. For
//! fan-out, configure [`fanout`] before recording work: its persisted roster
//! and per-replica cursors retain each message until every member confirms it.
//! The single-recipient acknowledgement API refuses roster-managed streams.
//! Neither mode makes quorum durability promises. Reclamation changes SQL
//! queue rows, never files.

pub mod fanout;

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use asupersync::io::{AsyncRead, ReadBuf};
use fsqlite_ast::{CreateTableStatement, Statement};
use fsqlite_parser::Parser;
use fsqlite_types::{PayloadHash, cx::Cx};

use super::{
    ApplyTransaction, ReplicaApplyError, ReplicaCheckpoint, ReplicaDisposition,
    ReplicationEnvelope, SqlChangesetApplyReport, StreamApplyError, begin, blob, checkpoint,
    protocol, rollback, row_text,
};
use crate::compat::changeset_stream::verified::ChangesetFrame;
use crate::compat::changeset_stream::{ChangesetStreamLimits, ChangesetStreamReader};
use crate::{Connection, FrankenError, Row, SqliteValue};

const STREAM_TABLE: &str = "_fsqlite_source_stream_v1";
const QUEUE_TABLE: &str = "_fsqlite_source_outbox_v1";
const CREATE_STREAM: &str = "CREATE TABLE IF NOT EXISTS main._fsqlite_source_stream_v1(\
    stream_id BLOB PRIMARY KEY NOT NULL, sequence INTEGER NOT NULL, tip BLOB NOT NULL, \
    ack_sequence INTEGER NOT NULL, ack_tip BLOB NOT NULL, pending_bytes INTEGER NOT NULL)";
const CREATE_QUEUE: &str = "CREATE TABLE IF NOT EXISTS main._fsqlite_source_outbox_v1(\
    stream_id BLOB NOT NULL, sequence INTEGER NOT NULL, previous BLOB NOT NULL, \
    tip BLOB NOT NULL, body BLOB NOT NULL, PRIMARY KEY(stream_id,sequence))";

type Result<T> = std::result::Result<T, ReplicaApplyError>;

/// Per-stream backpressure, including the message currently being recorded.
/// An empty message still occupies a message slot. Raising a limit is explicit.
#[derive(Debug, Clone, Copy)]
pub struct OutboxLimits {
    pub max_pending_messages: u64,
    pub max_pending_bytes: u64,
    pub input: ChangesetStreamLimits,
}

impl Default for OutboxLimits {
    fn default() -> Self {
        Self {
            max_pending_messages: 1024,
            max_pending_bytes: 64 * 1024 * 1024,
            input: ChangesetStreamLimits {
                max_input_bytes: 16 * 1024 * 1024,
                ..ChangesetStreamLimits::default()
            },
        }
    }
}

/// Produced and confirmed positions belong to the same logical stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxState {
    pub produced: ReplicaCheckpoint,
    pub acknowledged: ReplicaCheckpoint,
    pub pending_bytes: u64,
}

impl OutboxState {
    #[must_use]
    pub const fn pending_messages(self) -> u64 {
        self.produced
            .sequence
            .saturating_sub(self.acknowledged.sequence)
    }
}

/// The envelope becomes deliverable only after successful source COMMIT.
/// A source receipt is NOT an acknowledgement from a remote replica.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "retain the envelope identity to reconcile an uncertain source commit"]
pub struct OutboxCommit {
    pub envelope: ReplicationEnvelope,
    pub disposition: ReplicaDisposition,
}

/// A single committed, reverified message. Transport the ID independently
/// through the authenticated control plane required by the ordered receiver.
#[derive(Clone)]
pub struct OutboxMessage {
    envelope: ReplicationEnvelope,
    body: Vec<u8>,
}

impl std::fmt::Debug for OutboxMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutboxMessage")
            .field("sequence", &self.envelope.sequence())
            .field("body_bytes", &self.body.len())
            .finish_non_exhaustive()
    }
}

impl OutboxMessage {
    #[must_use]
    pub const fn envelope(&self) -> &ReplicationEnvelope {
        &self.envelope
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

// Immutable caller-owned bytes: preflight and SQL application see exactly
// the same message despite yielding, without a message-sized decoder copy.
struct Bytes<'a>(&'a [u8]);

impl AsyncRead for Bytes<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let count = output.remaining().min(this.0.len());
        output.put_slice(&this.0[..count]);
        this.0 = &this.0[count..];
        Poll::Ready(Ok(()))
    }
}

fn integer(value: u64) -> Result<SqliteValue> {
    Ok(SqliteValue::Integer(
        i64::try_from(value).map_err(|_| FrankenError::TooBig)?,
    ))
}

fn unsigned(row: &Row, index: usize) -> Result<u64> {
    match row.get(index) {
        Some(SqliteValue::Integer(value)) => {
            u64::try_from(*value).map_err(|_| protocol("negative outbox counter"))
        }
        _ => Err(protocol("invalid outbox counter")),
    }
}

fn hash_column(row: &Row, index: usize) -> Result<PayloadHash> {
    let Some(SqliteValue::Blob(bytes)) = row.get(index) else {
        return Err(protocol("invalid outbox identity storage class"));
    };
    Ok(PayloadHash::from_bytes(
        bytes
            .as_ref()
            .try_into()
            .map_err(|_| protocol("invalid outbox identity width"))?,
    ))
}

fn definition(sql: &str) -> Result<CreateTableStatement> {
    let (mut statements, errors) = Parser::from_sql(sql).parse_all();
    if !errors.is_empty() || statements.len() != 1 {
        return Err(protocol("invalid outbox metadata schema"));
    }
    let Statement::CreateTable(mut table) = statements.remove(0) else {
        return Err(protocol("outbox metadata is not an ordinary table"));
    };
    table.if_not_exists = false;
    table.name.schema = None;
    Ok(table)
}

async fn validate_schema(conn: &Connection) -> Result<()> {
    validate_tables(
        conn,
        &[(STREAM_TABLE, CREATE_STREAM), (QUEUE_TABLE, CREATE_QUEUE)],
    )
    .await
}

async fn validate_tables(conn: &Connection, tables: &[(&str, &str)]) -> Result<()> {
    for &(name, expected) in tables {
        let parameter = SqliteValue::Text(name.into());
        let rows = conn
            .query_with_params(
                "SELECT type,sql FROM main.sqlite_schema WHERE name=?1 COLLATE NOCASE",
                std::slice::from_ref(&parameter),
            )
            .await?;
        let [row] = rows.as_slice() else {
            return Err(protocol("missing or ambiguous outbox metadata"));
        };
        if row_text(row, 0) != Some("table")
            || definition(row_text(row, 1).ok_or_else(|| protocol("missing outbox schema"))?)?
                != definition(expected)?
        {
            return Err(protocol("incompatible outbox metadata schema"));
        }
        for catalog in ["main", "temp"] {
            let triggers = conn.query_with_params(
                &format!("SELECT name FROM {catalog}.sqlite_schema WHERE type='trigger' AND tbl_name=?1 COLLATE NOCASE LIMIT 1"),
                std::slice::from_ref(&parameter),
            ).await?;
            if !triggers.is_empty() {
                return Err(protocol("outbox metadata must not have triggers"));
            }
        }
    }
    Ok(())
}

async fn load_state(conn: &Connection, stream_id: PayloadHash) -> Result<OutboxState> {
    let rows = conn.query_with_params(
        "SELECT sequence,tip,ack_sequence,ack_tip,pending_bytes FROM main._fsqlite_source_stream_v1 WHERE stream_id=?1 LIMIT 2",
        &[blob(stream_id)],
    ).await?;
    let row = match rows.as_slice() {
        [] => return Err(ReplicaApplyError::Uninitialized),
        [row] => row,
        _ => return Err(protocol("ambiguous source stream")),
    };
    let state = OutboxState {
        produced: ReplicaCheckpoint {
            stream_id,
            sequence: unsigned(row, 0)?,
            tip: hash_column(row, 1)?,
        },
        acknowledged: ReplicaCheckpoint {
            stream_id,
            sequence: unsigned(row, 2)?,
            tip: hash_column(row, 3)?,
        },
        pending_bytes: unsigned(row, 4)?,
    };
    if state.acknowledged.sequence > state.produced.sequence
        || (state.acknowledged.sequence == state.produced.sequence
            && state.acknowledged.tip != state.produced.tip)
    {
        return Err(protocol("inconsistent outbox positions"));
    }
    // Return scalar accounting, not an owned collection of queued bodies.
    // Engine execution/storage allocations remain outside the wire budget.
    // With the validated composite PK, count/min/max prove sequence coverage.
    let rows = conn.query_with_params(
        "SELECT count(*),coalesce(sum(length(body)),0),min(sequence),max(sequence) FROM main._fsqlite_source_outbox_v1 WHERE stream_id=?1",
        &[blob(stream_id)],
    ).await?;
    let [row] = rows.as_slice() else {
        return Err(protocol("missing outbox accounting"));
    };
    if unsigned(row, 0)? != state.pending_messages() || unsigned(row, 1)? != state.pending_bytes {
        return Err(protocol("outbox payload accounting mismatch"));
    }
    if state.pending_messages() == 0 {
        if row.get(2) != Some(&SqliteValue::Null) || row.get(3) != Some(&SqliteValue::Null) {
            return Err(protocol("nonempty outbox at acknowledged tip"));
        }
    } else if unsigned(row, 2)? != state.acknowledged.sequence + 1
        || unsigned(row, 3)? != state.produced.sequence
    {
        return Err(protocol("outbox contains a sequence gap"));
    }
    Ok(state)
}

async fn save_state(conn: &Connection, old: OutboxState, next: OutboxState) -> Result<()> {
    let changed = conn.execute_with_params(
        "UPDATE OR ABORT main._fsqlite_source_stream_v1 SET sequence=?1,tip=?2,ack_sequence=?3,ack_tip=?4,pending_bytes=?5 \
         WHERE stream_id=?6 AND sequence=?7 AND tip=?8 AND ack_sequence=?9 AND ack_tip=?10 AND pending_bytes=?11",
        &[
            integer(next.produced.sequence)?, blob(next.produced.tip),
            integer(next.acknowledged.sequence)?, blob(next.acknowledged.tip), integer(next.pending_bytes)?,
            blob(old.produced.stream_id), integer(old.produced.sequence)?, blob(old.produced.tip),
            integer(old.acknowledged.sequence)?, blob(old.acknowledged.tip), integer(old.pending_bytes)?,
        ],
    ).await?;
    validate_schema(conn).await?;
    if changed != 1 || load_state(conn, old.produced.stream_id).await? != next {
        return Err(protocol("source stream changed during outbox transaction"));
    }
    Ok(())
}

async fn complete<T>(mut owner: ApplyTransaction<'_>, cx: &Cx, result: Result<T>) -> Result<T> {
    let value = match result {
        Ok(value) => value,
        Err(error) => return Err(rollback(&mut owner, error).await),
    };
    if let Err(error) = checkpoint(cx) {
        return Err(rollback(&mut owner, error).await);
    }
    if let Err(error) = owner.conn.commit_transaction().await {
        return Err(rollback(&mut owner, error.into()).await);
    }
    owner.armed = false;
    Ok(value)
}

/// Establish the source position of an already coherent baseline. Provision
/// the receiver from that SAME baseline separately. Never infer it from input.
/// Existing streams are accepted only at the same already-empty position;
/// initialization never moves either cursor or resets progressed history.
pub async fn initialize(
    conn: &mut Connection,
    cx: &Cx,
    baseline: ReplicaCheckpoint,
) -> Result<OutboxState> {
    checkpoint(cx)?;
    let sequence = integer(baseline.sequence)?;
    let owner = begin(conn).await?;
    let result = async {
        conn.execute(CREATE_STREAM).await?;
        conn.execute(CREATE_QUEUE).await?;
        validate_schema(conn).await?;
        let expected = OutboxState { produced: baseline, acknowledged: baseline, pending_bytes: 0 };
        match load_state(conn, baseline.stream_id).await {
            Ok(current) if current == expected => {}
            Ok(_) => return Err(protocol("source baseline cannot replace existing progress")),
            Err(ReplicaApplyError::Uninitialized) => {
                let queued = conn.query_with_params(
                    "SELECT sequence FROM main._fsqlite_source_outbox_v1 WHERE stream_id=?1 LIMIT 1",
                    &[blob(baseline.stream_id)],
                ).await?;
                if !queued.is_empty() { return Err(protocol("orphaned source outbox rows")); }
                let changed = conn.execute_with_params(
                    "INSERT OR ABORT INTO main._fsqlite_source_stream_v1(stream_id,sequence,tip,ack_sequence,ack_tip,pending_bytes) VALUES(?1,?2,?3,?2,?3,0)",
                    &[blob(baseline.stream_id), sequence, blob(baseline.tip)],
                ).await?;
                if changed != 1 { return Err(protocol("source baseline insertion was not exact")); }
            }
            Err(error) => return Err(error),
        }
        if load_state(conn, baseline.stream_id).await? != expected {
            return Err(protocol("source baseline verification failed"));
        }
        Ok(expected)
    }.await;
    complete(owner, cx, result).await
}

/// Inspect committed producer and delivery progress, settling deferred cleanup
/// first. A caller's still-active transaction is never adopted or committed.
pub async fn state(conn: &mut Connection, cx: &Cx, stream_id: PayloadHash) -> Result<OutboxState> {
    checkpoint(cx)?;
    let owner = begin(conn).await?;
    let result = async {
        validate_schema(conn).await?;
        let current = load_state(conn, stream_id).await?;
        let _ = fanout::snapshot(conn, current).await?;
        Ok(current)
    }
    .await;
    complete(owner, cx, result).await
}

async fn preflight(cx: &Cx, body: &[u8], limits: ChangesetStreamLimits) -> Result<()> {
    let mut reader = ChangesetStreamReader::new(Bytes(body), limits);
    while let Some(row) = reader.next(cx).await.map_err(StreamApplyError::from)? {
        if [
            STREAM_TABLE,
            QUEUE_TABLE,
            super::CURSOR_TABLE,
            fanout::GROUP_TABLE,
            fanout::REPLICA_TABLE,
        ]
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&row.table.name))
        {
            return Err(protocol("source message targets replication metadata"));
        }
    }
    Ok(())
}

/// Apply supplied wire changes and persist their delivery record atomically.
///
/// `previous` is an optimistic source position, not a request to reset it.
/// Retry the SAME position and bytes after a lost source acknowledgement.
/// Only the current exact retry is certified; older positions are refused.
/// Full changesets and patchsets use the existing decoder and strict applier.
///
/// Input is already caller-owned. Two bounded passes enforce metadata/decoder
/// limits before SQL, then share the existing row applier. The saved bytes are
/// exactly those applied, not a subsequently re-encoded or recaptured message.
/// Trigger effects must be reproducible on the replica; arbitrary source SQL
/// and trigger-generated changes are not automatically recorded by this API.
pub async fn record(
    conn: &mut Connection,
    cx: &Cx,
    previous: ReplicaCheckpoint,
    body: &[u8],
    limits: OutboxLimits,
) -> Result<OutboxCommit> {
    checkpoint(cx)?;
    let byte_len = u64::try_from(body.len()).map_err(|_| FrankenError::TooBig)?;
    if byte_len > limits.input.max_input_bytes {
        return Err(FrankenError::TooBig.into());
    }
    let sequence = previous
        .sequence
        .checked_add(1)
        .ok_or(FrankenError::TooBig)?;
    let envelope = ReplicationEnvelope::new(
        previous.stream_id,
        sequence,
        previous.tip,
        ChangesetFrame::for_message(cx, body, limits.input.max_input_bytes)?,
    )?;
    let identity = envelope.id();
    let owner = begin(conn).await?;
    let result = async {
        validate_schema(conn).await?;
        let current = load_state(conn, previous.stream_id).await?;
        let audience = fanout::snapshot(conn, current).await?;
        if !super::classify(current.produced, &envelope, identity)? {
            return Ok(ReplicaDisposition::AlreadyApplied);
        }
        let pending_bytes = current.pending_bytes.checked_add(byte_len).ok_or(FrankenError::TooBig)?;
        if current.pending_messages() >= limits.max_pending_messages
            || pending_bytes > limits.max_pending_bytes || i64::try_from(pending_bytes).is_err()
        {
            return Err(FrankenError::TooBig.into());
        }
        preflight(cx, body, limits.input).await?;
        let mut reader = ChangesetStreamReader::new(Bytes(body), limits.input);
        let first = reader.next(cx).await.map_err(StreamApplyError::from)?;
        let report = match first {
            Some(first) => super::apply_rows(conn, cx, &mut reader, first,
                &mut |_| super::ConflictAction::Abort, Some(STREAM_TABLE)).await?,
            None => SqlChangesetApplyReport::default(),
        };
        drop(reader);
        // A trigger may have changed current-source metadata indirectly.
        validate_schema(conn).await?;
        if load_state(conn, previous.stream_id).await? != current
            || fanout::snapshot(conn, current).await? != audience
        {
            return Err(protocol("source metadata changed during row application"));
        }
        let mut payload = Vec::new();
        payload.try_reserve_exact(body.len()).map_err(|_| FrankenError::OutOfMemory)?;
        payload.extend_from_slice(body);
        let changed = conn.execute_with_params(
            "INSERT OR ABORT INTO main._fsqlite_source_outbox_v1(stream_id,sequence,previous,tip,body) VALUES(?1,?2,?3,?4,?5)",
            &[blob(previous.stream_id), integer(sequence)?, blob(previous.tip), blob(identity), SqliteValue::Blob(payload.into())],
        ).await?;
        if changed != 1 { return Err(protocol("outbox insertion was not exact")); }
        let next = OutboxState {
            produced: ReplicaCheckpoint { stream_id: previous.stream_id, sequence, tip: identity },
            pending_bytes, ..current
        };
        save_state(conn, current, next).await?;
        Ok(ReplicaDisposition::Applied(report))
    }.await;
    let disposition = complete(owner, cx, result).await?;
    Ok(OutboxCommit {
        envelope,
        disposition,
    })
}

async fn read_after(
    conn: &Connection,
    cx: &Cx,
    current: OutboxState,
    after: ReplicaCheckpoint,
    max_message_bytes: u64,
) -> Result<Option<OutboxMessage>> {
    if after.stream_id != current.produced.stream_id
        || after.sequence < current.acknowledged.sequence
        || after.sequence > current.produced.sequence
    {
        return Err(protocol("outbox read position is outside retained history"));
    }
    if after.sequence == current.produced.sequence {
        if after != current.produced {
            return Err(protocol("outbox read position has a conflicting tip"));
        }
        return Ok(None);
    }
    let sequence = after.sequence + 1;
    let params = [blob(current.produced.stream_id), integer(sequence)?];
    // Check length before requesting an owned payload result. The engine may
    // still read/decode storage pages to evaluate this scalar length query.
    let rows = conn.query_with_params(
        "SELECT previous,tip,length(body),typeof(body) FROM main._fsqlite_source_outbox_v1 WHERE stream_id=?1 AND sequence=?2",
        &params,
    ).await?;
    let [row] = rows.as_slice() else {
        return Err(protocol("missing outbox head"));
    };
    let previous = hash_column(row, 0)?;
    let tip = hash_column(row, 1)?;
    let length = unsigned(row, 2)?;
    if previous != after.tip || row_text(row, 3) != Some("blob") {
        return Err(protocol("outbox head predecessor or payload type mismatch"));
    }
    if length > max_message_bytes {
        return Err(FrankenError::TooBig.into());
    }
    let rows = conn
        .query_with_params(
            "SELECT body FROM main._fsqlite_source_outbox_v1 WHERE stream_id=?1 AND sequence=?2",
            &params,
        )
        .await?;
    let [row] = rows.as_slice() else {
        return Err(protocol("missing outbox payload"));
    };
    let Some(SqliteValue::Blob(bytes)) = row.get(0) else {
        return Err(protocol("outbox body is not a blob"));
    };
    if u64::try_from(bytes.len()).ok() != Some(length) {
        return Err(protocol("outbox body length changed"));
    }
    let envelope = ReplicationEnvelope::new(
        current.produced.stream_id,
        sequence,
        previous,
        ChangesetFrame::for_message(cx, bytes.as_ref(), max_message_bytes)?,
    )?;
    if envelope.id() != tip
        || (sequence == current.produced.sequence && tip != current.produced.tip)
    {
        return Err(protocol("outbox content identity mismatch"));
    }
    let mut body = Vec::new();
    body.try_reserve_exact(bytes.len())
        .map_err(|_| FrankenError::OutOfMemory)?;
    body.extend_from_slice(bytes.as_ref());
    Ok(Some(OutboxMessage { envelope, body }))
}

/// Fetch only the oldest unacknowledged message, rechecking its content hash.
/// No progress is changed by reading; repeated reads support lost delivery ACKs.
pub async fn next_pending(
    conn: &mut Connection,
    cx: &Cx,
    stream_id: PayloadHash,
    max_message_bytes: u64,
) -> Result<Option<OutboxMessage>> {
    checkpoint(cx)?;
    let owner = begin(conn).await?;
    let result = async {
        validate_schema(conn).await?;
        let current = load_state(conn, stream_id).await?;
        let _ = fanout::snapshot(conn, current).await?;
        read_after(conn, cx, current, current.acknowledged, max_message_bytes).await
    }
    .await;
    complete(owner, cx, result).await
}

/// Confirm exactly the oldest pending message and reclaim its saved body.
///
/// Supply a checkpoint obtained from the configured replica over a trusted
/// channel AFTER its successful commit (or committed-state reconciliation).
/// A successful send or local source commit is not sufficient. The identity,
/// stream and immediate next sequence must match the reverified queue head.
/// Skipping messages or replacing history is never implicit.
///
/// Body deletion and acknowledgement advancement commit together. The current
/// exact acknowledgement is idempotent after a lost response, even though its
/// body has been reclaimed. Older confirmations are Stale, not certified from
/// missing history. Failure/drop uses the same deferred rollback ownership as
/// recording; inspect `state` after uncertain commit outcomes.
pub async fn acknowledge(
    conn: &mut Connection,
    cx: &Cx,
    confirmed: ReplicaCheckpoint,
    max_message_bytes: u64,
) -> Result<OutboxState> {
    checkpoint(cx)?;
    let _ = integer(confirmed.sequence)?;
    let owner = begin(conn).await?;
    let result = async {
        validate_schema(conn).await?;
        let current = load_state(conn, confirmed.stream_id).await?;
        if fanout::snapshot(conn, current).await?.is_some() {
            return Err(protocol("fanout acknowledgement requires a replica identity"));
        }
        if confirmed.sequence < current.acknowledged.sequence {
            return Err(ReplicaApplyError::Stale {
                current: current.acknowledged.sequence,
                received: confirmed.sequence,
            });
        }
        if confirmed.sequence == current.acknowledged.sequence {
            if confirmed != current.acknowledged {
                return Err(ReplicaApplyError::Diverged { sequence: confirmed.sequence });
            }
            return Ok(current);
        }
        let expected = current.acknowledged.sequence + 1;
        if confirmed.sequence != expected {
            return Err(ReplicaApplyError::Gap { expected, received: confirmed.sequence });
        }
        let message = read_after(conn, cx, current, current.acknowledged, max_message_bytes).await?
            .ok_or_else(|| protocol("acknowledgement has no queued message"))?;
        if message.envelope.id() != confirmed.tip {
            return Err(ReplicaApplyError::Diverged { sequence: confirmed.sequence });
        }
        let length = u64::try_from(message.body.len()).map_err(|_| FrankenError::TooBig)?;
        let pending_bytes = current.pending_bytes.checked_sub(length)
            .ok_or_else(|| protocol("outbox byte accounting underflow"))?;
        checkpoint(cx)?;
        let changed = conn.execute_with_params(
            "DELETE FROM main._fsqlite_source_outbox_v1 WHERE stream_id=?1 AND sequence=?2 AND tip=?3",
            &[blob(confirmed.stream_id), integer(confirmed.sequence)?, blob(confirmed.tip)],
        ).await?;
        if changed != 1 { return Err(protocol("outbox acknowledgement did not remove exactly one message")); }
        let next = OutboxState { acknowledged: confirmed, pending_bytes, ..current };
        save_state(conn, current, next).await?;
        Ok(next)
    }.await;
    complete(owner, cx, result).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_ext_session::{
        ChangeOp, Changeset, ChangesetKind, ChangesetRow, ChangesetValue, TableChangeset, TableInfo,
    };

    fn baseline() -> ReplicaCheckpoint {
        ReplicaCheckpoint {
            stream_id: PayloadHash::from_bytes([1; 32]),
            sequence: 0,
            tip: PayloadHash::from_bytes([2; 32]),
        }
    }

    fn wire(values: &[(i64, &str)]) -> Vec<u8> {
        Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: values
                    .iter()
                    .map(|(id, value)| ChangesetRow {
                        op: ChangeOp::Insert,
                        indirect: false,
                        old_values: Vec::new(),
                        new_values: vec![
                            ChangesetValue::Integer(*id),
                            ChangesetValue::Text((*value).to_owned()),
                        ],
                    })
                    .collect(),
            }],
        }
        .encode()
    }

    async fn setup(path: &str) -> Connection {
        let conn = Connection::open(path).await.unwrap();
        conn.execute(
            "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT UNIQUE); \
            CREATE TABLE audit(id INTEGER PRIMARY KEY); \
            CREATE TRIGGER audit_t AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END;",
        )
        .await
        .unwrap();
        conn
    }

    async fn count(conn: &Connection, name: &str) -> i64 {
        let row = conn
            .query_row(&format!("SELECT count(*) FROM {name}"))
            .await
            .unwrap();
        let Some(SqliteValue::Integer(value)) = row.get(0) else {
            panic!("integer count");
        };
        *value
    }

    fn committed_position(commit: &OutboxCommit) -> ReplicaCheckpoint {
        ReplicaCheckpoint {
            stream_id: commit.envelope.stream_id(),
            sequence: commit.envelope.sequence(),
            tip: commit.envelope.id(),
        }
    }

    #[test]
    fn outbox_source_commit_reaches_ordered_replica_and_retry_does_not_repeat_triggers() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut source = setup(":memory:").await;
            let mut target = setup(":memory:").await;
            initialize(&mut source, &cx, baseline()).await.unwrap();
            super::super::initialize(&mut target, &cx, baseline())
                .await
                .unwrap();
            let body = wire(&[(1, "one"), (2, "two")]);
            let committed = record(&mut source, &cx, baseline(), &body, OutboxLimits::default())
                .await
                .unwrap();
            assert!(matches!(
                committed.disposition,
                ReplicaDisposition::Applied(SqlChangesetApplyReport { applied: 2, .. })
            ));
            let retry = record(&mut source, &cx, baseline(), &body, OutboxLimits::default())
                .await
                .unwrap();
            assert_eq!(retry.disposition, ReplicaDisposition::AlreadyApplied);
            assert_eq!(retry.envelope, committed.envelope);
            assert_eq!(count(&source, "audit").await, 2);
            let message = next_pending(&mut source, &cx, baseline().stream_id, 1 << 20)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(message.body(), body);
            assert_eq!(message.envelope(), &committed.envelope);
            let receipt = super::super::apply(
                &mut target,
                &cx,
                &mut Bytes(message.body()),
                message.envelope(),
                committed.envelope.id(),
                ChangesetStreamLimits::default(),
            )
            .await
            .unwrap();
            assert_eq!(receipt.checkpoint.sequence, 1);
            assert_eq!(count(&target, "t").await, 2);
            assert_eq!(count(&target, "audit").await, 2);
            assert_eq!(
                state(&mut source, &cx, baseline().stream_id)
                    .await
                    .unwrap()
                    .pending_messages(),
                1
            );
            source.close().await.unwrap();
            target.close().await.unwrap();
        });
    }

    #[test]
    fn outbox_late_constraint_rolls_back_source_trigger_queue_and_sequence() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut conn = setup(":memory:").await;
            let initial = initialize(&mut conn, &cx, baseline()).await.unwrap();
            assert!(
                record(
                    &mut conn,
                    &cx,
                    baseline(),
                    &wire(&[(1, "same"), (2, "same")]),
                    OutboxLimits::default()
                )
                .await
                .is_err()
            );
            assert_eq!(
                state(&mut conn, &cx, baseline().stream_id).await.unwrap(),
                initial
            );
            assert_eq!(count(&conn, "t").await, 0);
            assert_eq!(count(&conn, "audit").await, 0);
            assert_eq!(count(&conn, QUEUE_TABLE).await, 0);
            let committed = record(
                &mut conn,
                &cx,
                baseline(),
                &wire(&[(1, "retry")]),
                OutboxLimits::default(),
            )
            .await
            .unwrap();
            assert_eq!(committed.envelope.sequence(), 1);
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn outbox_backpressure_and_caller_transactions_do_not_commit_a_source_prefix() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut conn = setup(":memory:").await;
            let initial = initialize(&mut conn, &cx, baseline()).await.unwrap();
            let bytes = wire(&[(1, "one")]);
            for limits in [
                OutboxLimits {
                    max_pending_messages: 0,
                    ..OutboxLimits::default()
                },
                OutboxLimits {
                    max_pending_bytes: bytes.len() as u64 - 1,
                    ..OutboxLimits::default()
                },
                OutboxLimits {
                    input: ChangesetStreamLimits {
                        max_rows: 0,
                        ..ChangesetStreamLimits::default()
                    },
                    ..OutboxLimits::default()
                },
            ] {
                assert!(
                    record(&mut conn, &cx, baseline(), &bytes, limits)
                        .await
                        .is_err()
                );
                assert_eq!(
                    state(&mut conn, &cx, baseline().stream_id).await.unwrap(),
                    initial
                );
                assert_eq!(count(&conn, "t").await, 0);
            }
            conn.execute("BEGIN; INSERT INTO t VALUES(9,'caller');")
                .await
                .unwrap();
            assert!(matches!(
                record(&mut conn, &cx, baseline(), &bytes, OutboxLimits::default()).await,
                Err(ReplicaApplyError::Database(FrankenError::NestedTransaction))
            ));
            assert!(conn.in_transaction());
            assert_eq!(count(&conn, "t").await, 1);
            conn.execute("ROLLBACK").await.unwrap();
            assert_eq!(
                state(&mut conn, &cx, baseline().stream_id).await.unwrap(),
                initial
            );
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn outbox_current_metadata_tampering_aborts_all_source_effects() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut conn = setup(":memory:").await;
            let initial = initialize(&mut conn, &cx, baseline()).await.unwrap();
            conn.execute(
                "CREATE TRIGGER tamper AFTER INSERT ON t BEGIN \
                UPDATE _fsqlite_source_stream_v1 SET tip=zeroblob(32); END;",
            )
            .await
            .unwrap();
            assert!(
                record(
                    &mut conn,
                    &cx,
                    baseline(),
                    &wire(&[(1, "one")]),
                    OutboxLimits::default()
                )
                .await
                .is_err()
            );
            assert_eq!(
                state(&mut conn, &cx, baseline().stream_id).await.unwrap(),
                initial
            );
            assert_eq!(count(&conn, "audit").await, 0);
            assert_eq!(count(&conn, QUEUE_TABLE).await, 0);
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn outbox_restart_and_lost_remote_ack_keep_source_and_replica_exactly_once() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let directory = tempfile::tempdir().unwrap().keep();
            let source_path = directory.join("source.db");
            let replica_path = directory.join("replica.db");
            let mut source = setup(source_path.to_str().unwrap()).await;
            let mut replica = setup(replica_path.to_str().unwrap()).await;
            initialize(&mut source, &cx, baseline()).await.unwrap();
            super::super::initialize(&mut replica, &cx, baseline())
                .await
                .unwrap();
            let body = wire(&[(1, "one")]);
            let first = record(&mut source, &cx, baseline(), &body, OutboxLimits::default())
                .await
                .unwrap();
            let second = record(
                &mut source,
                &cx,
                committed_position(&first),
                &wire(&[(2, "two")]),
                OutboxLimits::default(),
            )
            .await
            .unwrap();
            source.close().await.unwrap();
            let mut source = Connection::open(source_path.to_str().unwrap())
                .await
                .unwrap();
            let pending = next_pending(&mut source, &cx, baseline().stream_id, 1 << 20)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(pending.envelope(), &first.envelope);
            let receipt = super::super::apply(
                &mut replica,
                &cx,
                &mut Bytes(pending.body()),
                pending.envelope(),
                first.envelope.id(),
                ChangesetStreamLimits::default(),
            )
            .await
            .unwrap();
            // Lose the response before persisting source acknowledgement.
            replica.close().await.unwrap();
            source.close().await.unwrap();
            let mut replica = Connection::open(replica_path.to_str().unwrap())
                .await
                .unwrap();
            let mut source = Connection::open(source_path.to_str().unwrap())
                .await
                .unwrap();
            let pending = next_pending(&mut source, &cx, baseline().stream_id, 1 << 20)
                .await
                .unwrap()
                .unwrap();
            let duplicate = super::super::apply(
                &mut replica,
                &cx,
                &mut Bytes(pending.body()),
                pending.envelope(),
                first.envelope.id(),
                ChangesetStreamLimits::default(),
            )
            .await
            .unwrap();
            assert_eq!(duplicate.disposition, ReplicaDisposition::AlreadyApplied);
            assert_eq!(duplicate.checkpoint, receipt.checkpoint);
            let acknowledged = acknowledge(&mut source, &cx, duplicate.checkpoint, 1 << 20)
                .await
                .unwrap();
            assert_eq!(acknowledged.pending_messages(), 1);
            source.close().await.unwrap();
            let mut source = Connection::open(source_path.to_str().unwrap())
                .await
                .unwrap();
            assert_eq!(
                acknowledge(&mut source, &cx, duplicate.checkpoint, 0)
                    .await
                    .unwrap(),
                acknowledged
            );
            let next = next_pending(&mut source, &cx, baseline().stream_id, 1 << 20)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(next.envelope(), &second.envelope);
            let receipt = super::super::apply(
                &mut replica,
                &cx,
                &mut Bytes(next.body()),
                next.envelope(),
                second.envelope.id(),
                ChangesetStreamLimits::default(),
            )
            .await
            .unwrap();
            let empty = acknowledge(&mut source, &cx, receipt.checkpoint, 1 << 20)
                .await
                .unwrap();
            assert_eq!(empty.pending_messages(), 0);
            assert_eq!(empty.pending_bytes, 0);
            assert!(
                next_pending(&mut source, &cx, baseline().stream_id, 0)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(count(&source, QUEUE_TABLE).await, 0);
            for conn in [&source, &replica] {
                assert_eq!(count(conn, "t").await, 2);
                assert_eq!(count(conn, "audit").await, 2);
                assert_eq!(
                    conn.query_row("PRAGMA integrity_check")
                        .await
                        .unwrap()
                        .get(0),
                    Some(&SqliteValue::Text("ok".into()))
                );
            }
            source.close().await.unwrap();
            replica.close().await.unwrap();
        });
    }

    #[test]
    fn outbox_ack_cannot_skip_or_forge_history_and_releases_only_confirmed_capacity() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut conn = setup(":memory:").await;
            initialize(&mut conn, &cx, baseline()).await.unwrap();
            let limits = OutboxLimits {
                max_pending_messages: 2,
                ..OutboxLimits::default()
            };
            let first = record(&mut conn, &cx, baseline(), &wire(&[(1, "one")]), limits)
                .await
                .unwrap();
            let second = record(
                &mut conn,
                &cx,
                committed_position(&first),
                &wire(&[(2, "two")]),
                limits,
            )
            .await
            .unwrap();
            let before = state(&mut conn, &cx, baseline().stream_id).await.unwrap();
            let third = wire(&[(3, "three")]);
            assert!(matches!(
                record(&mut conn, &cx, committed_position(&second), &third, limits).await,
                Err(ReplicaApplyError::Database(FrankenError::TooBig))
            ));
            assert!(matches!(
                acknowledge(&mut conn, &cx, committed_position(&second), 1 << 20).await,
                Err(ReplicaApplyError::Gap {
                    expected: 1,
                    received: 2
                })
            ));
            let forged = ReplicaCheckpoint {
                tip: PayloadHash::from_bytes([9; 32]),
                ..committed_position(&first)
            };
            assert!(matches!(
                acknowledge(&mut conn, &cx, forged, 1 << 20).await,
                Err(ReplicaApplyError::Diverged { sequence: 1 })
            ));
            assert_eq!(
                state(&mut conn, &cx, baseline().stream_id).await.unwrap(),
                before
            );
            let after = acknowledge(&mut conn, &cx, committed_position(&first), 1 << 20)
                .await
                .unwrap();
            assert_eq!(after.pending_messages(), 1);
            assert!(after.pending_bytes < before.pending_bytes);
            assert!(matches!(
                acknowledge(&mut conn, &cx, baseline(), 1 << 20).await,
                Err(ReplicaApplyError::Stale { .. })
            ));
            let third = record(&mut conn, &cx, committed_position(&second), &third, limits)
                .await
                .unwrap();
            assert_eq!(third.envelope.sequence(), 3);
            assert_eq!(count(&conn, "t").await, 3);
            assert_eq!(
                next_pending(&mut conn, &cx, baseline().stream_id, 1 << 20)
                    .await
                    .unwrap()
                    .unwrap()
                    .envelope(),
                &second.envelope
            );
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn outbox_read_and_ack_refuse_corrupt_payload_without_losing_the_record() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut conn = setup(":memory:").await;
            initialize(&mut conn, &cx, baseline()).await.unwrap();
            let body = wire(&[(1, "one")]);
            let commit = record(&mut conn, &cx, baseline(), &body, OutboxLimits::default())
                .await
                .unwrap();
            let before = state(&mut conn, &cx, baseline().stream_id).await.unwrap();
            assert!(matches!(
                next_pending(&mut conn, &cx, baseline().stream_id, body.len() as u64 - 1).await,
                Err(ReplicaApplyError::Database(FrankenError::TooBig))
            ));
            // Preserve length/accounting but change authenticated content.
            conn.execute("UPDATE _fsqlite_source_outbox_v1 SET body=zeroblob(length(body))")
                .await
                .unwrap();
            assert!(matches!(
                next_pending(&mut conn, &cx, baseline().stream_id, 1 << 20).await,
                Err(ReplicaApplyError::Protocol { .. })
            ));
            assert!(matches!(
                acknowledge(&mut conn, &cx, committed_position(&commit), 1 << 20).await,
                Err(ReplicaApplyError::Protocol { .. })
            ));
            assert_eq!(
                state(&mut conn, &cx, baseline().stream_id).await.unwrap(),
                before
            );
            assert_eq!(count(&conn, QUEUE_TABLE).await, 1);
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn outbox_deferred_commit_failure_removes_provisional_source_and_delivery() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys=ON; CREATE TABLE parent(id INTEGER PRIMARY KEY); \
                CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);")
                .await.unwrap();
            let initial = initialize(&mut conn, &cx, baseline()).await.unwrap();
            let body = wire(&[(1, "99")]);
            assert!(matches!(
                record(&mut conn, &cx, baseline(), &body, OutboxLimits::default()).await,
                Err(ReplicaApplyError::Database(_))
            ));
            assert_eq!(
                state(&mut conn, &cx, baseline().stream_id).await.unwrap(),
                initial
            );
            assert_eq!(count(&conn, "t").await, 0);
            assert_eq!(count(&conn, QUEUE_TABLE).await, 0);
            conn.execute("INSERT INTO parent VALUES(99)").await.unwrap();
            assert_eq!(
                record(&mut conn, &cx, baseline(), &body, OutboxLimits::default())
                    .await
                    .unwrap()
                    .envelope
                    .sequence(),
                1
            );
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn outbox_empty_message_capacity_cancel_and_metadata_admission() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut conn = setup(":memory:").await;
            initialize(&mut conn, &cx, baseline()).await.unwrap();
            let limits = OutboxLimits {
                max_pending_messages: 1,
                max_pending_bytes: 0,
                ..OutboxLimits::default()
            };
            let first = record(&mut conn, &cx, baseline(), &[], limits)
                .await
                .unwrap();
            assert!(matches!(
                record(&mut conn, &cx, committed_position(&first), &[], limits).await,
                Err(ReplicaApplyError::Database(FrankenError::TooBig))
            ));
            let cancelled = Cx::new();
            cancelled.cancel();
            assert!(matches!(
                acknowledge(&mut conn, &cancelled, committed_position(&first), 0).await,
                Err(ReplicaApplyError::Database(FrankenError::Interrupt))
            ));
            assert_eq!(
                state(&mut conn, &cx, baseline().stream_id)
                    .await
                    .unwrap()
                    .pending_messages(),
                1
            );
            let cleared = acknowledge(&mut conn, &cx, committed_position(&first), 0)
                .await
                .unwrap();
            assert_eq!(cleared.pending_messages(), 0);
            assert!(initialize(&mut conn, &cx, baseline()).await.is_err());
            // Direct mutation is rejected even with a valid Session row shape.
            for name in [STREAM_TABLE, QUEUE_TABLE, super::super::CURSOR_TABLE] {
                let body = Changeset {
                    kind: ChangesetKind::Changeset,
                    tables: vec![TableChangeset {
                        info: TableInfo {
                            name: name.to_ascii_uppercase(),
                            column_count: 1,
                            pk_flags: vec![true],
                        },
                        rows: vec![ChangesetRow {
                            op: ChangeOp::Insert,
                            indirect: false,
                            old_values: Vec::new(),
                            new_values: vec![ChangesetValue::Integer(1)],
                        }],
                    }],
                }
                .encode();
                assert!(matches!(
                    record(
                        &mut conn,
                        &cx,
                        committed_position(&first),
                        &body,
                        OutboxLimits::default()
                    )
                    .await,
                    Err(ReplicaApplyError::Protocol {
                        detail: "source message targets replication metadata"
                    })
                ));
            }
            assert_eq!(
                state(&mut conn, &cx, baseline().stream_id).await.unwrap(),
                cleared
            );
            conn.execute(
                "CREATE TEMP TRIGGER metadata BEFORE INSERT ON main._fsqlite_source_outbox_v1 \
                BEGIN SELECT RAISE(ABORT,'must not run'); END;",
            )
            .await
            .unwrap();
            assert!(matches!(
                record(&mut conn, &cx, committed_position(&first), &[], limits).await,
                Err(ReplicaApplyError::Protocol {
                    detail: "outbox metadata must not have triggers"
                })
            ));
            conn.close().await.unwrap();
        });
    }
}
