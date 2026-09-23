//! Persistent source outbox composed with native SQL change capture.
//!
//! Application rows, sequence allocation, payload and outbox accounting commit
//! in ONE ordinary transaction. After an unacknowledged COMMIT, lookup by the
//! stable request ID before replaying application work. Transport retries read
//! the SAME stored bytes; this module never retries application SQL or creates
//! an executor. Concurrent producers retain the normal MVCC conflict policy.
//!
//! One outbox incarnation identifies one ordered delivery log/receiver route.
//! The caller supplies a nonzero, globally distinct incarnation (for example a
//! UUID) and must change it when intentionally forking a source. Acknowledging
//! clears payload storage but retains the request ID, sequence and digest, so
//! a delayed duplicate request cannot become a new mutation. Receipt storage
//! is explicitly bounded; no history is silently pruned or reused.
//!
//! ACK is an explicit trusted application action, NOT a network authenticator.
//! Call it only after validating the intended receiver's committed receipt.
//! This module provides durable at-least-once source delivery, not receiver
//! deduplication, transport security, fanout, or end-to-end exactly-once apply.
//! Durability follows the Connection's configured journal/synchronous policy.
//! Never directly edit the two reserved main-schema tables.
//!
//! ```ignore
//! use fsqlite::compat::capture::{CaptureOptions, ChangesetCapture};
//! use fsqlite::compat::capture::outbox::{ChangesetOutbox, OutboxLimits};
//! let outbox = ChangesetOutbox::new(source_incarnation, OutboxLimits::default())?;
//! outbox.initialize(&mut connection, &cx).await?;
//! let mut capture = ChangesetCapture::begin(
//!     &mut connection, &cx, CaptureOptions::new(["items"]),
//! ).await?;
//! capture.execute("INSERT INTO items VALUES(?1,?2)", &params).await?;
//! let receipt = capture.commit_to_outbox(&outbox, stable_request_id).await?;
//! // Deliver pending() bytes with receipt metadata through an authenticated
//! // transport. Only acknowledge() after validating the receiver's COMMIT.
//! ```

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
#[path = "changeset_delivery.rs"]
pub mod delivery;

use fsqlite_types::ecs::PayloadHash;

use super::{
    CaptureError, CaptureResult, CapturedChangeset, ChangesetCapture, Connection,
    Cx, FrankenError, Parser, Row, SqliteValue, Transaction, TransactionExt,
    checkpoint, count, integer, literal, quote, text,
};

const QUEUE: &str = "__fsqlite_changeset_outbox";
const STATE: &str = "__fsqlite_changeset_outbox_state";
// Keep the bypass guard active even in builds without the native delivery API.
const DELIVERY_ROUTE: &str = "__fsqlite_changeset_delivery_route";
const MAX_PAYLOAD: usize = 64 * 1024 * 1024;
const STATE_DDL: &str = "CREATE TABLE \"__fsqlite_changeset_outbox_state\" (slot INTEGER PRIMARY KEY CHECK(slot=1), incarnation BLOB NOT NULL, last_sequence INTEGER NOT NULL, pending_messages INTEGER NOT NULL, pending_bytes INTEGER NOT NULL, receipts INTEGER NOT NULL)";
const QUEUE_DDL: &str = "CREATE TABLE \"__fsqlite_changeset_outbox\" (sequence INTEGER PRIMARY KEY, message_id BLOB NOT NULL UNIQUE, payload_hash BLOB NOT NULL, payload_bytes INTEGER NOT NULL, change_count INTEGER NOT NULL, touched_rows INTEGER NOT NULL, payload BLOB)";
const META_COLUMNS: &str = "sequence,message_id,payload_hash,payload_bytes,change_count,touched_rows,payload IS NULL,CASE WHEN payload IS NULL THEN -1 ELSE length(payload) END";

#[derive(Debug, Clone, Copy)]
pub struct OutboxLimits {
    /// Includes acknowledged tombstones. Maximum 1,000,000; no automatic prune.
    pub max_receipts: usize,
    pub max_pending_messages: usize,
    /// Sum of retained payload lengths, not process RSS. Maximum 1 GiB.
    pub max_pending_bytes: usize,
}

impl Default for OutboxLimits {
    fn default() -> Self {
        Self { max_receipts: 100_000, max_pending_messages: 4096, max_pending_bytes: 64 * 1024 * 1024 }
    }
}

#[derive(Debug, Clone)]
pub struct ChangesetOutbox {
    incarnation: [u8; 16],
    limits: OutboxLimits,
}

/// Immutable identity of one committed source request. Digest verification
/// detects changed payloads but does not authenticate a remote party.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxReceipt {
    pub incarnation: [u8; 16],
    pub sequence: i64,
    pub message_id: [u8; 16],
    pub payload_hash: [u8; 32],
    pub payload_bytes: usize,
    pub changes: usize,
    pub touched_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxStatus {
    pub receipt: OutboxReceipt,
    pub acknowledged: bool,
}

#[derive(Debug)]
pub struct OutboxMessage {
    pub receipt: OutboxReceipt,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcknowledgeOutcome {
    Acknowledged,
    AlreadyAcknowledged,
}

struct State {
    last_sequence: i64,
    pending_messages: usize,
    pending_bytes: usize,
    receipts: usize,
}

fn blob(bytes: &[u8]) -> SqliteValue { SqliteValue::Blob(bytes.to_vec().into()) }
fn number(value: usize) -> CaptureResult<SqliteValue> {
    i64::try_from(value).map(SqliteValue::Integer).map_err(|_| CaptureError::Limit("outbox integer"))
}
fn fixed<const N: usize>(row: &Row, column: usize) -> CaptureResult<[u8; N]> {
    match row.get(column) {
        Some(SqliteValue::Blob(value)) => value.as_ref().try_into()
            .map_err(|_| CaptureError::Schema("outbox identity/digest has an invalid length")),
        _ => Err(CaptureError::Schema("outbox identity/digest is not a BLOB")),
    }
}
fn canonical(sql: &str) -> CaptureResult<String> {
    if sql.len() > 8192 { return Err(CaptureError::Schema("outbox DDL is oversized")); }
    let (statements, errors) = Parser::from_sql(sql).parse_all();
    if !errors.is_empty() || statements.len() != 1 {
        return Err(CaptureError::Schema("outbox DDL is malformed"));
    }
    Ok(statements[0].to_string())
}

impl ChangesetOutbox {
    pub fn new(incarnation: [u8; 16], limits: OutboxLimits) -> CaptureResult<Self> {
        if incarnation == [0; 16]
            || limits.max_receipts == 0 || limits.max_receipts > 1_000_000
            || limits.max_pending_messages == 0 || limits.max_pending_messages > limits.max_receipts
            || limits.max_pending_bytes == 0 || limits.max_pending_bytes > 1024 * 1024 * 1024
        {
            return Err(CaptureError::Input("invalid outbox incarnation or capacity limits"));
        }
        Ok(Self { incarnation, limits })
    }

    #[must_use]
    pub const fn incarnation(&self) -> [u8; 16] { self.incarnation }

    /// Create the outbox atomically, or verify the exact existing schema and
    /// incarnation. Never adopt a partial, foreign, or altered installation.
    pub async fn initialize(&self, connection: &mut Connection, cx: &Cx) -> CaptureResult<()> {
        checkpoint(cx)?;
        let transaction = connection.transaction().await?;
        let result = self.initialize_in(&transaction, cx).await;
        settle(transaction, cx, result).await
    }

    async fn initialize_in(&self, transaction: &Transaction<'_>, cx: &Cx) -> CaptureResult<()> {
        let existing = transaction.query_with_params(
            "SELECT name FROM main.sqlite_schema WHERE CAST(name AS BLOB)=?1 OR CAST(name AS BLOB)=?2 LIMIT 3",
            &[blob(STATE.as_bytes()), blob(QUEUE.as_bytes())],
        ).await?;
        match existing.len() {
            0 => {
                transaction.execute(STATE_DDL).await?;
                checkpoint(cx)?;
                transaction.execute(QUEUE_DDL).await?;
                transaction.execute_with_params(&format!(
                    "INSERT INTO main.{} VALUES(1,?1,0,0,0,0)", quote(STATE),
                ), &[blob(&self.incarnation)]).await?;
            }
            2 => {}
            _ => return Err(CaptureError::Schema("partial or ambiguous outbox installation")),
        }
        Self::validate_schema(transaction, cx).await?;
        self.read_state(transaction).await?;
        Ok(())
    }

    async fn validate_schema(transaction: &Transaction<'_>, cx: &Cx) -> CaptureResult<()> {
        for (table, expected, indexes) in [(STATE, STATE_DDL, 0), (QUEUE, QUEUE_DDL, 1)] {
            checkpoint(cx)?;
            let rows = transaction.query_with_params(
                "SELECT sql FROM main.sqlite_schema WHERE CAST(name AS BLOB)=?1 AND length(CAST(sql AS BLOB))<=8192 LIMIT 2",
                &[blob(table.as_bytes())],
            ).await?;
            let [row] = rows.as_slice() else { return Err(CaptureError::Schema("outbox table is missing or ambiguous")); };
            if canonical(text(row, 0)?)? != canonical(expected)? {
                return Err(CaptureError::Schema("outbox schema does not match this wire/storage version"));
            }
            let actual = transaction.query(&format!("PRAGMA main.index_list({})", literal(table))).await?;
            if actual.len() != indexes || actual.iter().any(|row| {
                integer(row, 2).ok() != Some(1) || text(row, 3).ok() != Some("u") || integer(row, 4).ok() != Some(0)
            }) {
                return Err(CaptureError::Schema("outbox has unexpected indexes"));
            }
        }
        for schema in ["main", "temp"] {
            let triggers = transaction.query(&format!(
                "SELECT tbl_name FROM {schema}.sqlite_schema WHERE type='trigger' LIMIT 1025",
            )).await?;
            if triggers.len() > 1024 { return Err(CaptureError::Schema("outbox trigger admission limit exceeded")); }
            for row in triggers {
                let table = text(&row, 0)?;
                if table.eq_ignore_ascii_case(STATE) || table.eq_ignore_ascii_case(QUEUE) {
                    return Err(CaptureError::Schema("outbox tables must not have application triggers"));
                }
            }
        }
        Ok(())
    }

    async fn read_state(&self, transaction: &Transaction<'_>) -> CaptureResult<State> {
        let rows = transaction.query(&format!(
            "SELECT slot,incarnation,last_sequence,pending_messages,pending_bytes,receipts FROM main.{} LIMIT 2", quote(STATE),
        )).await?;
        let [row] = rows.as_slice() else { return Err(CaptureError::Schema("outbox requires exactly one state row")); };
        if integer(row, 0)? != 1 || fixed::<16>(row, 1)? != self.incarnation {
            return Err(CaptureError::Schema("outbox incarnation does not match the configured source log"));
        }
        let state = State {
            last_sequence: integer(row, 2)?, pending_messages: count(row, 3)?,
            pending_bytes: count(row, 4)?, receipts: count(row, 5)?,
        };
        if state.last_sequence < 0
            || usize::try_from(state.last_sequence).ok() != Some(state.receipts)
            || state.pending_messages > state.receipts
            || (state.pending_messages == 0 && state.pending_bytes != 0)
        {
            return Err(CaptureError::Schema("outbox accounting invariant violated"));
        }
        Ok(state)
    }

    fn status(&self, row: &Row) -> CaptureResult<OutboxStatus> {
        let receipt = OutboxReceipt {
            incarnation: self.incarnation, sequence: integer(row, 0)?,
            message_id: fixed(row, 1)?, payload_hash: fixed(row, 2)?,
            payload_bytes: count(row, 3)?, changes: count(row, 4)?, touched_rows: count(row, 5)?,
        };
        let acknowledged = integer(row, 6)?;
        let actual_len = integer(row, 7)?;
        if receipt.sequence <= 0 || receipt.payload_bytes > MAX_PAYLOAD
            || receipt.changes > receipt.touched_rows || receipt.touched_rows > 100_000
            || !matches!(acknowledged, 0 | 1)
            || (acknowledged == 1 && actual_len != -1)
            || (acknowledged == 0 && usize::try_from(actual_len).ok() != Some(receipt.payload_bytes))
        {
            return Err(CaptureError::Schema("outbox record metadata is invalid"));
        }
        Ok(OutboxStatus { receipt, acknowledged: acknowledged == 1 })
    }

    async fn lookup_in(&self, transaction: &Transaction<'_>, message_id: [u8; 16]) -> CaptureResult<Option<OutboxStatus>> {
        let rows = transaction.query_with_params(&format!(
            "SELECT {META_COLUMNS} FROM main.{} WHERE message_id=?1 LIMIT 2", quote(QUEUE),
        ), &[blob(&message_id)]).await?;
        match rows.as_slice() {
            [] => Ok(None),
            [row] => self.status(row).map(Some),
            _ => Err(CaptureError::Schema("outbox request identity is not unique")),
        }
    }

    /// Resolve a potentially committed request without replaying its SQL.
    /// Acknowledged metadata is retained after payload removal. This reads a
    /// transaction snapshot; it is not a new remote-delivery acknowledgement.
    pub async fn lookup(
        &self, connection: &mut Connection, cx: &Cx, message_id: [u8; 16],
    ) -> CaptureResult<Option<OutboxStatus>> {
        checkpoint(cx)?;
        let transaction = connection.transaction().await?;
        let result = async {
            Self::validate_schema(&transaction, cx).await?;
            self.read_state(&transaction).await?;
            self.lookup_in(&transaction, message_id).await
        }.await;
        settle(transaction, cx, result).await
    }

    /// Read one bounded batch in sequence order. Each payload is length-checked
    /// BEFORE transfer and then checked against its retained BLAKE3 digest.
    /// An oversized first message is an explicit limit failure, not an empty
    /// batch that could make a sender falsely declare the outbox drained.
    pub async fn pending(
        &self, connection: &mut Connection, cx: &Cx, max_messages: usize, max_bytes: usize,
    ) -> CaptureResult<Vec<OutboxMessage>> {
        if max_messages == 0 || max_messages > 256 || max_bytes == 0 || max_bytes > MAX_PAYLOAD {
            return Err(CaptureError::Input("outbox batch requires 1..256 messages and 1..64 MiB"));
        }
        checkpoint(cx)?;
        let transaction = connection.transaction().await?;
        let result = async {
            Self::validate_schema(&transaction, cx).await?;
            let state = self.read_state(&transaction).await?;
            let rows = transaction.query_with_params(&format!(
                "SELECT {META_COLUMNS} FROM main.{} WHERE payload IS NOT NULL ORDER BY sequence LIMIT ?1", quote(QUEUE),
            ), &[number(max_messages)?]).await?;
            if rows.len() != state.pending_messages.min(max_messages) {
                return Err(CaptureError::Schema("outbox pending count disagrees with its retained records"));
            }
            let mut result = Vec::new();
            let mut used = 0_usize;
            let mut previous = 0;
            for row in rows {
                checkpoint(cx)?;
                let status = self.status(&row)?;
                let receipt = status.receipt;
                if status.acknowledged || receipt.sequence <= previous || receipt.sequence > state.last_sequence {
                    return Err(CaptureError::Schema("outbox pending sequence is invalid"));
                }
                let next = used.checked_add(receipt.payload_bytes).ok_or(CaptureError::Limit("outbox batch bytes"))?;
                if next > max_bytes {
                    if result.is_empty() { return Err(CaptureError::Limit("outbox first message bytes")); }
                    break;
                }
                let payload = transaction.query_row_with_params(&format!(
                    "SELECT payload FROM main.{} WHERE sequence=?1", quote(QUEUE),
                ), &[SqliteValue::Integer(receipt.sequence)]).await?;
                let Some(SqliteValue::Blob(bytes)) = payload.get(0) else {
                    return Err(CaptureError::Schema("outbox payload disappeared inside its snapshot"));
                };
                if bytes.len() != receipt.payload_bytes || *PayloadHash::blake3(bytes.as_ref()).as_bytes() != receipt.payload_hash {
                    return Err(CaptureError::Schema("outbox payload failed length/digest verification"));
                }
                used = next;
                previous = receipt.sequence;
                result.push(OutboxMessage { receipt, bytes: bytes.to_vec() });
            }
            if used > state.pending_bytes {
                return Err(CaptureError::Schema("outbox pending byte count under-reports its payloads"));
            }
            Ok(result)
        }.await;
        settle(transaction, cx, result).await
    }

    /// Persist an externally verified receiver acknowledgement. The complete
    /// source receipt must match, including incarnation, request ID, sequence,
    /// digest and counts. Duplicate acknowledgements are idempotent. Retained
    /// identity tombstones prevent request-ID reuse even after payload removal.
    /// Once an ordered delivery route exists, use that route's acknowledgement
    /// API instead: reclaiming bytes without advancing its predecessor would
    /// make the remaining messages undeliverable.
    pub async fn acknowledge(
        &self, connection: &mut Connection, cx: &Cx, receipt: &OutboxReceipt,
    ) -> CaptureResult<AcknowledgeOutcome> {
        if receipt.incarnation != self.incarnation {
            return Err(CaptureError::Input("acknowledgement belongs to a different outbox incarnation"));
        }
        checkpoint(cx)?;
        let transaction = connection.transaction().await?;
        let result = async {
            let routes = transaction.query_with_params(
                "SELECT name FROM main.sqlite_schema WHERE CAST(name AS BLOB)=?1 LIMIT 1",
                &[blob(DELIVERY_ROUTE.as_bytes())],
            ).await?;
            if !routes.is_empty() {
                return Err(CaptureError::Input("ordered delivery requires a route-bound acknowledgement"));
            }
            self.acknowledge_in(&transaction, cx, receipt).await
        }.await;
        settle(transaction, cx, result).await
    }

    // The route owner reuses this mutation inside its OWN transaction so
    // payload reclamation, accounting and predecessor publication are atomic.
    async fn acknowledge_in(
        &self, transaction: &Transaction<'_>, cx: &Cx, receipt: &OutboxReceipt,
    ) -> CaptureResult<AcknowledgeOutcome> {
        if receipt.incarnation != self.incarnation {
            return Err(CaptureError::Input("acknowledgement belongs to a different outbox incarnation"));
        }
        Self::validate_schema(transaction, cx).await?;
        let state = self.read_state(transaction).await?;
        let status = self.lookup_in(transaction, receipt.message_id).await?
            .ok_or(CaptureError::Input("acknowledgement names an unknown request"))?;
        if &status.receipt != receipt {
            return Err(CaptureError::Input("acknowledgement does not match the committed source receipt"));
        }
        if status.acknowledged { return Ok(AcknowledgeOutcome::AlreadyAcknowledged); }
        let messages = state.pending_messages.checked_sub(1)
            .ok_or(CaptureError::Schema("outbox pending message count underflow"))?;
        let bytes = state.pending_bytes.checked_sub(receipt.payload_bytes)
            .ok_or(CaptureError::Schema("outbox pending byte count underflow"))?;
        if messages == 0 && bytes != 0 { return Err(CaptureError::Schema("outbox final acknowledgement leaves inconsistent byte accounting")); }
        checkpoint(cx)?;
        let changed = transaction.execute_with_params(&format!(
            "UPDATE main.{} SET payload=NULL WHERE sequence=?1 AND payload IS NOT NULL", quote(QUEUE),
        ), &[SqliteValue::Integer(receipt.sequence)]).await?;
        if changed != 1 { return Err(CaptureError::Schema("outbox acknowledgement lost its record")); }
        let changed = transaction.execute_with_params(&format!(
            "UPDATE main.{} SET pending_messages=?1,pending_bytes=?2 WHERE slot=1", quote(STATE),
        ), &[number(messages)?, number(bytes)?]).await?;
        if changed != 1 { return Err(CaptureError::Schema("outbox acknowledgement lost its accounting row")); }
        Ok(AcknowledgeOutcome::Acknowledged)
    }

    async fn enqueue(
        &self, transaction: &Transaction<'_>, cx: &Cx, message_id: [u8; 16], captured: CapturedChangeset,
    ) -> CaptureResult<OutboxReceipt> {
        Self::validate_schema(transaction, cx).await?;
        let state = self.read_state(transaction).await?;
        if let Some(previous) = self.lookup_in(transaction, message_id).await? {
            return Err(CaptureError::DuplicateMessage { sequence: previous.receipt.sequence });
        }
        let pending_bytes = state.pending_bytes.checked_add(captured.bytes.len())
            .ok_or(CaptureError::Limit("outbox pending bytes"))?;
        if state.receipts >= self.limits.max_receipts { return Err(CaptureError::Limit("outbox retained receipts")); }
        if state.pending_messages >= self.limits.max_pending_messages { return Err(CaptureError::Limit("outbox pending messages")); }
        if pending_bytes > self.limits.max_pending_bytes { return Err(CaptureError::Limit("outbox pending bytes")); }
        let sequence = state.last_sequence.checked_add(1).ok_or(CaptureError::Limit("outbox sequence"))?;
        let receipt = OutboxReceipt {
            incarnation: self.incarnation, sequence, message_id,
            payload_hash: *PayloadHash::blake3(&captured.bytes).as_bytes(),
            payload_bytes: captured.bytes.len(), changes: captured.changes, touched_rows: captured.touched_rows,
        };
        checkpoint(cx)?;
        let changed = transaction.execute_with_params(&format!(
            "INSERT INTO main.{} VALUES(?1,?2,?3,?4,?5,?6,?7)", quote(QUEUE),
        ), &[
            SqliteValue::Integer(sequence), blob(&message_id), blob(&receipt.payload_hash),
            number(receipt.payload_bytes)?, number(receipt.changes)?, number(receipt.touched_rows)?,
            SqliteValue::Blob(captured.bytes.into()),
        ]).await?;
        if changed != 1 { return Err(CaptureError::Schema("outbox insertion did not create exactly one message")); }
        let changed = transaction.execute_with_params(&format!(
            "UPDATE main.{} SET last_sequence=?1,pending_messages=?2,pending_bytes=?3,receipts=?4 WHERE slot=1", quote(STATE),
        ), &[
            SqliteValue::Integer(sequence), number(state.pending_messages + 1)?,
            number(pending_bytes)?, number(state.receipts + 1)?,
        ]).await?;
        if changed != 1 { return Err(CaptureError::Schema("outbox insertion lost its accounting row")); }
        Ok(receipt)
    }
}

impl ChangesetCapture<'_> {
    /// Commit application DML and its encoded changeset to the persistent outbox
    /// atomically. The outbox must already be initialized for this incarnation.
    /// A duplicate request ID rolls back THIS scope, including application DML;
    /// lookup retrieves the earlier committed result, even after it was ACKed.
    /// After Commit/Rollback errors, reconcile the stable ID before replaying.
    pub async fn commit_to_outbox(
        mut self, outbox: &ChangesetOutbox, message_id: [u8; 16],
    ) -> CaptureResult<OutboxReceipt> {
        let result = async {
            let captured = self.collect().await?;
            outbox.enqueue(&self.transaction, self.cx, message_id, captured).await
        }.await;
        settle(self.transaction, self.cx, result).await
    }
}

async fn rollback_failure(transaction: &mut Transaction<'_>, cause: CaptureError) -> CaptureError {
    match transaction.rollback().await {
        Ok(()) | Err(FrankenError::NoActiveTransaction) => cause,
        Err(error) => CaptureError::Rollback { cause: Box::new(cause), error },
    }
}

async fn settle<T>(mut transaction: Transaction<'_>, cx: &Cx, result: CaptureResult<T>) -> CaptureResult<T> {
    let result = result.and_then(|value| { checkpoint(cx)?; Ok(value) });
    let value = match result {
        Ok(value) => value,
        Err(error) => return Err(rollback_failure(&mut transaction, error).await),
    };
    if let Err(error) = transaction.commit().await {
        return Err(rollback_failure(&mut transaction, CaptureError::Commit(error)).await);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::capture::CaptureOptions;
    use crate::compat::changeset::apply_changeset;
    use fsqlite_ext_session::Changeset;

    async fn source() -> Connection {
        let connection = Connection::open(":memory:").await.unwrap();
        connection.execute_batch("PRAGMA recursive_triggers=ON; CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT);").await.unwrap();
        connection
    }

    fn outbox() -> ChangesetOutbox {
        ChangesetOutbox::new([7;16], OutboxLimits::default()).unwrap()
    }

    async fn insert(connection: &mut Connection, cx: &Cx, outbox: &ChangesetOutbox, id: u8) -> CaptureResult<OutboxReceipt> {
        let mut capture = ChangesetCapture::begin(connection, cx, CaptureOptions::new(["items"])).await?;
        capture.execute("INSERT INTO items VALUES(?1,'captured')", &[SqliteValue::Integer(i64::from(id))]).await?;
        capture.commit_to_outbox(outbox, [id;16]).await
    }

    #[test]
    fn outbox_capture_delivery_and_acknowledgement_roundtrip() {
        asupersync::test_utils::run_test(|| async {
            let mut source = source().await;
            let mut replica = self::source().await;
            let cx = Cx::new();
            let outbox = outbox();
            outbox.initialize(&mut source, &cx).await.unwrap();
            outbox.initialize(&mut source, &cx).await.unwrap();
            let receipt = insert(&mut source, &cx, &outbox, 1).await.unwrap();
            assert_eq!(receipt.sequence, 1);
            let messages = outbox.pending(&mut source, &cx, 16, 1024).await.unwrap();
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].receipt, receipt);
            apply_changeset(&mut replica, &Changeset::decode(&messages[0].bytes).unwrap()).await.unwrap();
            let replicated = replica.query_row("SELECT id,value FROM items").await.unwrap();
            assert_eq!(integer(&replicated, 0).unwrap(), 1);
            assert_eq!(text(&replicated, 1).unwrap(), "captured");
            assert_eq!(outbox.acknowledge(&mut source, &cx, &receipt).await.unwrap(), AcknowledgeOutcome::Acknowledged);
            assert_eq!(outbox.acknowledge(&mut source, &cx, &receipt).await.unwrap(), AcknowledgeOutcome::AlreadyAcknowledged);
            assert!(outbox.pending(&mut source, &cx, 16, 1024).await.unwrap().is_empty());
            let status = outbox.lookup(&mut source, &cx, [1;16]).await.unwrap().unwrap();
            assert!(status.acknowledged);
            assert_eq!(status.receipt, receipt);
            let next = insert(&mut source, &cx, &outbox, 2).await.unwrap();
            assert_eq!(next.sequence, 2);
        });
    }

    #[test]
    fn duplicate_request_after_ack_rolls_back_repeated_application_work() {
        asupersync::test_utils::run_test(|| async {
            let mut source = source().await;
            let cx = Cx::new();
            let outbox = outbox();
            outbox.initialize(&mut source, &cx).await.unwrap();
            let receipt = insert(&mut source, &cx, &outbox, 1).await.unwrap();
            outbox.acknowledge(&mut source, &cx, &receipt).await.unwrap();
            let mut capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["items"])).await.unwrap();
            capture.execute("UPDATE items SET value='must roll back'", &[]).await.unwrap();
            assert!(matches!(capture.commit_to_outbox(&outbox, [1;16]).await,
                Err(CaptureError::DuplicateMessage { sequence: 1 })));
            let row = source.query_row("SELECT value FROM items WHERE id=1").await.unwrap();
            assert_eq!(text(&row, 0).unwrap(), "captured");
            assert_eq!(outbox.lookup(&mut source, &cx, [1;16]).await.unwrap().unwrap().receipt, receipt);
        });
    }

    #[test]
    fn capacity_failure_never_commits_application_rows_and_ack_releases_payload_capacity() {
        asupersync::test_utils::run_test(|| async {
            let mut source = source().await;
            let cx = Cx::new();
            let outbox = ChangesetOutbox::new([7;16], OutboxLimits { max_receipts: 2, max_pending_messages: 1, max_pending_bytes: 1024 }).unwrap();
            outbox.initialize(&mut source, &cx).await.unwrap();
            let receipt = insert(&mut source, &cx, &outbox, 1).await.unwrap();
            assert!(matches!(insert(&mut source, &cx, &outbox, 2).await, Err(CaptureError::Limit(_))));
            assert!(source.query("SELECT * FROM items WHERE id=2").await.unwrap().is_empty());
            outbox.acknowledge(&mut source, &cx, &receipt).await.unwrap();
            let receipt = insert(&mut source, &cx, &outbox, 2).await.unwrap();
            outbox.acknowledge(&mut source, &cx, &receipt).await.unwrap();
            assert!(matches!(insert(&mut source, &cx, &outbox, 3).await, Err(CaptureError::Limit("outbox retained receipts"))));
            assert!(source.query("SELECT * FROM items WHERE id=3").await.unwrap().is_empty());
        });
    }

    #[test]
    fn wrong_receipts_and_incarnations_do_not_acknowledge_messages() {
        asupersync::test_utils::run_test(|| async {
            let mut source = source().await;
            let cx = Cx::new();
            let outbox = outbox();
            outbox.initialize(&mut source, &cx).await.unwrap();
            let receipt = insert(&mut source, &cx, &outbox, 1).await.unwrap();
            for field in 0..7 {
                let mut forged = receipt.clone();
                match field {
                    0 => forged.incarnation[0] ^= 1,
                    1 => forged.message_id[0] ^= 1,
                    2 => forged.sequence += 1,
                    3 => forged.payload_hash[0] ^= 1,
                    4 => forged.payload_bytes += 1,
                    5 => forged.changes += 1,
                    _ => forged.touched_rows += 1,
                }
                assert!(outbox.acknowledge(&mut source, &cx, &forged).await.is_err());
            }
            let foreign = ChangesetOutbox::new([8;16], OutboxLimits::default()).unwrap();
            assert!(foreign.initialize(&mut source, &cx).await.is_err());
            assert!(foreign.lookup(&mut source, &cx, [1;16]).await.is_err());
            assert_eq!(outbox.pending(&mut source, &cx, 1, 1024).await.unwrap()[0].receipt, receipt);
        });
    }

    #[test]
    fn pending_batches_keep_sequence_order_and_byte_limits_after_partial_ack() {
        asupersync::test_utils::run_test(|| async {
            let mut source = source().await;
            let cx = Cx::new();
            let outbox = outbox();
            outbox.initialize(&mut source, &cx).await.unwrap();
            let first = insert(&mut source, &cx, &outbox, 1).await.unwrap();
            let second = insert(&mut source, &cx, &outbox, 2).await.unwrap();
            let third = insert(&mut source, &cx, &outbox, 3).await.unwrap();
            let batch = outbox.pending(&mut source, &cx, 2, 1024).await.unwrap();
            assert_eq!(batch.iter().map(|message| message.receipt.sequence).collect::<Vec<_>>(), vec![1, 2]);
            let batch = outbox.pending(&mut source, &cx, 3, first.payload_bytes).await.unwrap();
            assert_eq!(batch.len(), 1);
            assert_eq!(batch[0].receipt, first);
            outbox.acknowledge(&mut source, &cx, &second).await.unwrap();
            let batch = outbox.pending(&mut source, &cx, 3, 1024).await.unwrap();
            assert_eq!(batch.iter().map(|message| message.receipt.sequence).collect::<Vec<_>>(), vec![1, 3]);
            assert_eq!(batch[1].receipt, third);
        });
    }

    #[test]
    fn payload_corruption_and_oversized_first_message_are_not_reported_as_drained() {
        asupersync::test_utils::run_test(|| async {
            let mut source = source().await;
            let cx = Cx::new();
            let outbox = outbox();
            outbox.initialize(&mut source, &cx).await.unwrap();
            let receipt = insert(&mut source, &cx, &outbox, 1).await.unwrap();
            assert!(matches!(outbox.pending(&mut source, &cx, 1, 1).await, Err(CaptureError::Limit(_))));
            source.execute_with_params(&format!("UPDATE main.{} SET payload=zeroblob(?1) WHERE sequence=1", quote(QUEUE)), &[number(receipt.payload_bytes).unwrap()]).await.unwrap();
            assert!(matches!(outbox.pending(&mut source, &cx, 1, 1024).await, Err(CaptureError::Schema(_))));
            assert!(!outbox.lookup(&mut source, &cx, [1;16]).await.unwrap().unwrap().acknowledged);
        });
    }

    #[test]
    fn uninitialized_or_foreign_schema_cannot_commit_a_captured_scope() {
        asupersync::test_utils::run_test(|| async {
            let mut source = source().await;
            let cx = Cx::new();
            let outbox = outbox();
            assert!(insert(&mut source, &cx, &outbox, 1).await.is_err());
            assert!(source.query("SELECT * FROM items").await.unwrap().is_empty());
            source.execute(&format!("CREATE TABLE {}(unowned TEXT)", quote(QUEUE))).await.unwrap();
            source.execute(&format!("INSERT INTO {} VALUES('preserve')", quote(QUEUE))).await.unwrap();
            assert!(outbox.initialize(&mut source, &cx).await.is_err());
            let row = source.query_row(&format!("SELECT unowned FROM {}", quote(QUEUE))).await.unwrap();
            assert_eq!(text(&row, 0).unwrap(), "preserve");
        });
    }

    #[test]
    fn persisted_request_can_be_reconciled_and_delivered_after_reopen() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("source.db");
            let cx = Cx::new();
            let outbox = outbox();
            let mut source = Connection::open(path.to_str().unwrap()).await.unwrap();
            source.execute_batch("PRAGMA recursive_triggers=ON; CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT);").await.unwrap();
            outbox.initialize(&mut source, &cx).await.unwrap();
            let original = insert(&mut source, &cx, &outbox, 1).await.unwrap();
            let expected = outbox.pending(&mut source, &cx, 1, 1024).await.unwrap().remove(0).bytes;
            drop(source); // No in-flight source transaction remains.
            let mut reopened = Connection::open(path.to_str().unwrap()).await.unwrap();
            outbox.initialize(&mut reopened, &cx).await.unwrap();
            let status = outbox.lookup(&mut reopened, &cx, [1;16]).await.unwrap().unwrap();
            assert_eq!(status.receipt, original);
            assert!(!status.acknowledged);
            let pending = outbox.pending(&mut reopened, &cx, 1, 1024).await.unwrap();
            assert_eq!(pending[0].bytes, expected);
            outbox.acknowledge(&mut reopened, &cx, &original).await.unwrap();
            drop(reopened);
            let mut reopened = Connection::open(path.to_str().unwrap()).await.unwrap();
            assert!(outbox.lookup(&mut reopened, &cx, [1;16]).await.unwrap().unwrap().acknowledged);
            assert!(outbox.pending(&mut reopened, &cx, 1, 1024).await.unwrap().is_empty());
        });
    }

    #[test]
    fn empty_changeset_is_a_pending_message_not_an_acknowledged_tombstone() {
        asupersync::test_utils::run_test(|| async {
            let mut source = source().await;
            let cx = Cx::new();
            let outbox = outbox();
            outbox.initialize(&mut source, &cx).await.unwrap();
            let capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["items"])).await.unwrap();
            let receipt = capture.commit_to_outbox(&outbox, [1;16]).await.unwrap();
            assert_eq!(receipt.payload_bytes, 0);
            let messages = outbox.pending(&mut source, &cx, 1, 1).await.unwrap();
            assert_eq!(messages.len(), 1);
            assert!(messages[0].bytes.is_empty());
            outbox.acknowledge(&mut source, &cx, &receipt).await.unwrap();
            assert!(outbox.lookup(&mut source, &cx, [1;16]).await.unwrap().unwrap().acknowledged);
        });
    }

    #[test]
    fn source_outbox_process_exit_is_atomic() {
        const DATABASE: &str = "FSQLITE_OUTBOX_EXIT_DATABASE";
        const PHASE: &str = "FSQLITE_OUTBOX_EXIT_PHASE";
        const TEST: &str = "compat::connection::capture::outbox::tests::source_outbox_process_exit_is_atomic";
        if let Some(path) = std::env::var_os(DATABASE) {
            asupersync::test_utils::run_test(|| async {
                let phase: u8 = std::env::var(PHASE).unwrap().parse().unwrap();
                let mut connection = Connection::open(path.to_str().unwrap()).await.unwrap();
                connection.execute_batch("PRAGMA recursive_triggers=ON; CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT);").await.unwrap();
                let cx = Cx::new();
                let outbox = outbox();
                outbox.initialize(&mut connection, &cx).await.unwrap();
                let mut capture = ChangesetCapture::begin(&mut connection, &cx, CaptureOptions::new(["items"])).await.unwrap();
                capture.execute("INSERT INTO items VALUES(1,'captured')", &[]).await.unwrap();
                if phase == 0 { std::process::exit(0); }
                if phase == 1 {
                    let captured = capture.collect().await.unwrap();
                    outbox.enqueue(&capture.transaction, &cx, [1;16], captured).await.unwrap();
                    // Both persistent outbox writes exist, but COMMIT does not.
                    std::process::exit(0);
                }
                capture.commit_to_outbox(&outbox, [1;16]).await.unwrap();
                // Lose the application-level response without running Drop.
                std::process::exit(0);
            });
            return;
        }
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap().keep();
            for phase in 0..3 {
                let path = directory.join(format!("phase-{phase}.db"));
                let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([TEST, "--exact", "--nocapture", "--test-threads=1"])
                    .env(DATABASE, &path).env(PHASE, phase.to_string()).spawn().unwrap();
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                loop {
                    if let Some(status) = child.try_wait().unwrap() {
                        assert!(status.success(), "capture child failed in phase {phase}");
                        break;
                    }
                    if std::time::Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("capture child did not exit in phase {phase}");
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                let mut connection = Connection::open(path.to_str().unwrap()).await.unwrap();
                let cx = Cx::new();
                let outbox = outbox();
                let rows = connection.query("SELECT * FROM items").await.unwrap();
                let receipt = outbox.lookup(&mut connection, &cx, [1;16]).await.unwrap();
                let pending = outbox.pending(&mut connection, &cx, 1, 1024).await.unwrap();
                assert_eq!(rows.len(), usize::from(phase == 2));
                assert_eq!(receipt.is_some(), phase == 2);
                assert_eq!(pending.len(), usize::from(phase == 2));
                if let Some(message) = pending.first() {
                    assert_eq!(message.receipt.sequence, 1);
                    assert!(!message.bytes.is_empty());
                }
            }
        });
    }
}
