//! Join native capture's request-ID outbox to the existing ordered receiver.
//!
//! No application SQL is replayed at the source and no second payload queue is
//! created. A single durable route binds the source incarnation to a trusted
//! sequence-zero replica baseline. Its acknowledged envelope tip supplies the
//! predecessor for the next retained raw Session message. The frame and envelope
//! are reconstructed deterministically from those exact, hash-checked bytes.
//!
//! Reclaiming a payload, updating outbox accounting and advancing the route tip
//! share ONE source transaction. A lost receiver response leaves that message
//! pending; the ordered receiver can recognize the same current envelope on
//! retry. Request-ID tombstones remain available through the original outbox.
//!
//! Provision the receiver from the SAME coherent baseline separately. This API
//! does not verify a snapshot, authenticate a peer, provide encryption or change
//! the engine's concurrency/durability settings. One route is one downstream
//! delivery obligation, not a quorum or fanout. Once routed, raw acknowledge()
//! is refused; bypassing this owner with direct SQL is unsupported.

use std::fmt;

use fsqlite_types::{PayloadHash, cx::Cx};

use super::{
    CaptureError, ChangesetOutbox, Connection, DELIVERY_ROUTE, FrankenError, META_COLUMNS,
    OutboxReceipt, QUEUE, SqliteValue, Transaction, TransactionExt, blob, canonical, checkpoint,
    count, fixed, integer, quote, text,
};
use crate::compat::changeset::streaming::ordered::{
    ReplicaApplyError, ReplicaCheckpoint, ReplicationEnvelope,
};
use crate::compat::changeset_stream::verified::ChangesetFrame;

const ROUTE_DDL: &str = r#"CREATE TABLE "__fsqlite_changeset_delivery_route" (
    slot INTEGER PRIMARY KEY CHECK(slot=1),
    incarnation BLOB NOT NULL,
    stream_id BLOB NOT NULL,
    baseline_tip BLOB NOT NULL,
    ack_sequence INTEGER NOT NULL,
    ack_tip BLOB NOT NULL
)"#;
const READ_ROUTE: &str = r#"SELECT slot,incarnation,stream_id,baseline_tip,ack_sequence,ack_tip
    FROM main.__fsqlite_changeset_delivery_route LIMIT 2"#;
const INSERT_ROUTE: &str = r#"INSERT INTO main.__fsqlite_changeset_delivery_route
    VALUES(1,?1,?2,?3,0,?3)"#;
const ADVANCE_ROUTE: &str = r#"UPDATE OR ABORT main.__fsqlite_changeset_delivery_route
    SET ack_sequence=?1,ack_tip=?2
    WHERE slot=1 AND incarnation=?3 AND stream_id=?4 AND baseline_tip=?5
      AND ack_sequence=?6 AND ack_tip=?7"#;
// With the INTEGER PRIMARY KEY, count/min/max establish a contiguous retained
// request history. The remaining aggregates prove that NULL tombstones are
// exactly the acknowledged prefix, not arbitrary holes in the delivery log.
// Only scalar metadata crosses this query boundary; no queued body is loaded.
const QUEUE_COVERAGE: &str = r#"SELECT count(*),min(sequence),max(sequence),
    coalesce(sum(CASE WHEN payload IS NOT NULL THEN 1 ELSE 0 END),0),
    coalesce(sum(CASE WHEN payload IS NOT NULL THEN length(payload) ELSE 0 END),0),
    coalesce(sum(CASE WHEN sequence<=?1 THEN payload IS NOT NULL ELSE payload IS NULL END),0),
    coalesce(sum(CASE WHEN payload IS NOT NULL AND
        (CAST(typeof(payload) AS BLOB)<>x'626c6f62' OR length(payload)<>payload_bytes)
        THEN 1 ELSE 0 END),0)
    FROM main.__fsqlite_changeset_outbox"#;

/// Errors preserve source commit uncertainty and failed rollback obligations.
#[derive(Debug)]
pub enum OrderedDeliveryError {
    Source(CaptureError),
    Replication(ReplicaApplyError),
    ReceiptMismatch,
    Rollback {
        cause: Box<Self>,
        error: FrankenError,
    },
}

pub type DeliveryResult<T> = Result<T, OrderedDeliveryError>;

impl fmt::Display for OrderedDeliveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(error) => write!(f, "ordered source: {error}"),
            Self::Replication(error) => write!(f, "{error}"),
            Self::ReceiptMismatch => {
                f.write_str("receiver checkpoint does not match the pending source message")
            }
            Self::Rollback { cause, error } => {
                write!(f, "{cause}; route rollback also failed: {error}")
            }
        }
    }
}

impl std::error::Error for OrderedDeliveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Replication(error) => Some(error),
            Self::Rollback { cause, .. } => Some(cause.as_ref()),
            Self::ReceiptMismatch => None,
        }
    }
}

impl From<CaptureError> for OrderedDeliveryError {
    fn from(error: CaptureError) -> Self {
        Self::Source(error)
    }
}
impl From<FrankenError> for OrderedDeliveryError {
    fn from(error: FrankenError) -> Self {
        Self::Source(CaptureError::Engine(error))
    }
}
impl From<ReplicaApplyError> for OrderedDeliveryError {
    fn from(error: ReplicaApplyError) -> Self {
        Self::Replication(error)
    }
}

fn invalid(detail: &'static str) -> OrderedDeliveryError {
    CaptureError::Schema(detail).into()
}

/// Delivery progress from one committed source snapshot. The produced sequence
/// is not itself a receiver acknowledgement or a content-bound checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryProgress {
    pub acknowledged: ReplicaCheckpoint,
    pub produced_sequence: u64,
    pub pending_messages: usize,
    pub pending_bytes: usize,
}

/// Private fields keep the source receipt, envelope and bytes bound together.
/// Transport the envelope ID through an authenticated channel independently of
/// untrusted body bytes. Merely sending a packet does not authorize source ACK.
#[derive(Debug, Clone)]
pub struct OrderedMessage {
    source: OutboxReceipt,
    envelope: ReplicationEnvelope,
    body: Vec<u8>,
}

impl OrderedMessage {
    #[must_use]
    pub const fn source_receipt(&self) -> &OutboxReceipt {
        &self.source
    }

    #[must_use]
    pub const fn envelope(&self) -> &ReplicationEnvelope {
        &self.envelope
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// The checkpoint a receiver must commit before this message can be ACKed.
    #[must_use]
    pub fn expected_checkpoint(&self) -> ReplicaCheckpoint {
        ReplicaCheckpoint {
            stream_id: self.envelope.stream_id(),
            sequence: self.envelope.sequence(),
            tip: self.envelope.id(),
        }
    }
}

/// A permanent single-recipient route for one request-ID source outbox.
/// Initialization can attach to an already populated log only while its full
/// unacknowledged history remains. An ACKed legacy log lacks predecessor
/// evidence and is refused, never silently rebased or reset.
#[derive(Debug, Clone)]
pub struct OrderedDelivery {
    source: ChangesetOutbox,
    baseline: ReplicaCheckpoint,
}

impl OrderedDelivery {
    pub fn new(source: ChangesetOutbox, baseline: ReplicaCheckpoint) -> DeliveryResult<Self> {
        if baseline.sequence != 0 {
            return Err(CaptureError::Input(
                "capture outboxes require a sequence-zero delivery baseline",
            )
            .into());
        }
        Ok(Self { source, baseline })
    }

    #[must_use]
    pub const fn outbox(&self) -> &ChangesetOutbox {
        &self.source
    }

    #[must_use]
    pub const fn baseline(&self) -> ReplicaCheckpoint {
        self.baseline
    }

    /// Create/bind the route and source outbox atomically. Reinitializing with
    /// the exact same configuration preserves progress, including after ACKs.
    /// A different stream, incarnation or baseline can never adopt its data.
    pub async fn initialize(
        &self,
        conn: &mut Connection,
        cx: &Cx,
    ) -> DeliveryResult<DeliveryProgress> {
        checkpoint(cx)?;
        let transaction = conn.transaction().await?;
        let result = async {
            self.source.initialize_in(&transaction, cx).await?;
            let objects = transaction
                .query_with_params(
                    "SELECT name FROM main.sqlite_schema WHERE CAST(name AS BLOB)=?1 LIMIT 2",
                    &[blob(DELIVERY_ROUTE.as_bytes())],
                )
                .await?;
            if objects.is_empty() {
                transaction.execute(ROUTE_DDL).await?;
                transaction
                    .execute_with_params(
                        INSERT_ROUTE,
                        &[
                            blob(&self.source.incarnation),
                            blob(self.baseline.stream_id.as_bytes()),
                            blob(self.baseline.tip.as_bytes()),
                        ],
                    )
                    .await?;
            }
            Self::validate_route(&transaction, cx).await?;
            self.load_progress(&transaction).await
        }
        .await;
        settle(transaction, cx, result).await
    }

    async fn validate_route(transaction: &Transaction<'_>, cx: &Cx) -> DeliveryResult<()> {
        ChangesetOutbox::validate_schema(transaction, cx).await?;
        let rows = transaction.query_with_params(
            "SELECT sql FROM main.sqlite_schema WHERE CAST(name AS BLOB)=?1 AND length(CAST(sql AS BLOB))<=8192 LIMIT 2",
            &[blob(DELIVERY_ROUTE.as_bytes())],
        ).await?;
        let [row] = rows.as_slice() else {
            return Err(invalid("delivery route is missing or ambiguous"));
        };
        if canonical(text(row, 0)?)? != canonical(ROUTE_DDL)? {
            return Err(invalid("incompatible delivery route schema"));
        }
        let indexes = transaction
            .query("PRAGMA main.index_list('__fsqlite_changeset_delivery_route')")
            .await?;
        if !indexes.is_empty() {
            return Err(invalid("delivery route has unexpected indexes"));
        }
        for catalog in ["main", "temp"] {
            checkpoint(cx)?;
            let rows = transaction
                .query(&format!(
                    "SELECT tbl_name FROM {catalog}.sqlite_schema WHERE type='trigger' LIMIT 1025",
                ))
                .await?;
            if rows.len() > 1024 {
                return Err(invalid("delivery route trigger admission limit exceeded"));
            }
            for row in rows {
                if text(&row, 0)?.eq_ignore_ascii_case(DELIVERY_ROUTE) {
                    return Err(invalid("delivery route must not have application triggers"));
                }
            }
        }
        Ok(())
    }

    async fn load_progress(
        &self,
        transaction: &Transaction<'_>,
    ) -> DeliveryResult<DeliveryProgress> {
        let state = self.source.read_state(transaction).await?;
        let rows = transaction.query(READ_ROUTE).await?;
        let [row] = rows.as_slice() else {
            return Err(invalid("delivery route requires exactly one row"));
        };
        if integer(row, 0)? != 1
            || fixed::<16>(row, 1)? != self.source.incarnation
            || fixed::<32>(row, 2)? != *self.baseline.stream_id.as_bytes()
            || fixed::<32>(row, 3)? != *self.baseline.tip.as_bytes()
        {
            return Err(invalid(
                "delivery route does not match the configured source/baseline",
            ));
        }
        let ack = integer(row, 4)?;
        let tip = PayloadHash::from_bytes(fixed(row, 5)?);
        if ack < 0 || ack > state.last_sequence || (ack == 0 && tip != self.baseline.tip) {
            return Err(invalid("invalid delivery acknowledgement position"));
        }
        let rows = transaction
            .query_with_params(QUEUE_COVERAGE, &[SqliteValue::Integer(ack)])
            .await?;
        let [row] = rows.as_slice() else {
            return Err(invalid("missing delivery queue accounting"));
        };
        let acknowledged =
            usize::try_from(ack).map_err(|_| invalid("delivery sequence overflow"))?;
        if count(row, 0)? != state.receipts
            || count(row, 3)? != state.pending_messages
            || count(row, 4)? != state.pending_bytes
            || count(row, 5)? != 0
            || count(row, 6)? != 0
            || state.receipts.checked_sub(acknowledged) != Some(state.pending_messages)
        {
            return Err(invalid(
                "delivery queue is not the exact acknowledged-prefix/pending-suffix partition",
            ));
        }
        if state.receipts == 0 {
            if row.get(1) != Some(&SqliteValue::Null) || row.get(2) != Some(&SqliteValue::Null) {
                return Err(invalid("empty delivery log has nonempty sequence bounds"));
            }
        } else if integer(row, 1)? != 1 || integer(row, 2)? != state.last_sequence {
            return Err(invalid("delivery request history has a sequence gap"));
        }
        Ok(DeliveryProgress {
            acknowledged: ReplicaCheckpoint {
                stream_id: self.baseline.stream_id,
                sequence: u64::try_from(ack).map_err(|_| invalid("negative delivery sequence"))?,
                tip,
            },
            produced_sequence: u64::try_from(state.last_sequence)
                .map_err(|_| invalid("negative produced sequence"))?,
            pending_messages: state.pending_messages,
            pending_bytes: state.pending_bytes,
        })
    }

    pub async fn progress(
        &self,
        conn: &mut Connection,
        cx: &Cx,
    ) -> DeliveryResult<DeliveryProgress> {
        checkpoint(cx)?;
        let transaction = conn.transaction().await?;
        let result = async {
            Self::validate_route(&transaction, cx).await?;
            self.load_progress(&transaction).await
        }
        .await;
        settle(transaction, cx, result).await
    }

    /// Prepare only the oldest pending message. The source transaction ends
    /// before the packet is returned, so no source lock/snapshot spans network
    /// I/O. Reopen or call again after a lost response to obtain the SAME ID.
    pub async fn next_pending(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        max_message_bytes: usize,
    ) -> DeliveryResult<Option<OrderedMessage>> {
        if max_message_bytes == 0 || max_message_bytes > super::MAX_PAYLOAD {
            return Err(CaptureError::Input("delivery message bound must be in 1..64 MiB").into());
        }
        checkpoint(cx)?;
        let transaction = conn.transaction().await?;
        let result = async {
            Self::validate_route(&transaction, cx).await?;
            let progress = self.load_progress(&transaction).await?;
            if progress.pending_messages == 0 {
                return Ok(None);
            }
            let sequence = progress
                .acknowledged
                .sequence
                .checked_add(1)
                .ok_or(FrankenError::TooBig)?;
            let parameter =
                SqliteValue::Integer(i64::try_from(sequence).map_err(|_| FrankenError::TooBig)?);
            let rows = transaction
                .query_with_params(
                    &format!(
                        "SELECT {META_COLUMNS} FROM main.{} WHERE sequence=?1 LIMIT 2",
                        quote(QUEUE),
                    ),
                    std::slice::from_ref(&parameter),
                )
                .await?;
            let [row] = rows.as_slice() else {
                return Err(invalid("oldest delivery message is missing"));
            };
            let status = self.source.status(row)?;
            if status.acknowledged
                || status.receipt.sequence
                    != i64::try_from(sequence).map_err(|_| FrankenError::TooBig)?
            {
                return Err(invalid("invalid oldest delivery message"));
            }
            if status.receipt.payload_bytes > max_message_bytes {
                return Err(CaptureError::Limit("delivery first message bytes").into());
            }
            checkpoint(cx)?;
            let row = transaction
                .query_row_with_params(
                    &format!(
                        "SELECT payload FROM main.{} WHERE sequence=?1",
                        quote(QUEUE),
                    ),
                    &[parameter],
                )
                .await?;
            let Some(SqliteValue::Blob(bytes)) = row.get(0) else {
                return Err(invalid("delivery payload is not a BLOB"));
            };
            if bytes.len() != status.receipt.payload_bytes
                || *PayloadHash::blake3(bytes.as_ref()).as_bytes() != status.receipt.payload_hash
            {
                return Err(invalid(
                    "delivery payload failed length/digest verification",
                ));
            }
            let frame = ChangesetFrame::for_message(
                cx,
                bytes.as_ref(),
                u64::try_from(max_message_bytes).map_err(|_| FrankenError::TooBig)?,
            )?;
            let envelope = ReplicationEnvelope::new(
                self.baseline.stream_id,
                sequence,
                progress.acknowledged.tip,
                frame,
            )?;
            let mut body = Vec::new();
            body.try_reserve_exact(bytes.len())
                .map_err(|_| FrankenError::OutOfMemory)?;
            body.extend_from_slice(bytes.as_ref());
            Ok(Some(OrderedMessage {
                source: status.receipt,
                envelope,
                body,
            }))
        }
        .await;
        settle(transaction, cx, result).await
    }

    /// Accept an authenticated receiver's committed checkpoint for this exact
    /// packet, then atomically reclaim its source payload and advance the tip.
    /// The current exact ACK is idempotent. Older ACKs are explicitly Stale.
    /// A locally constructed checkpoint is not evidence of remote durability.
    pub async fn acknowledge(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        message: &OrderedMessage,
        confirmed: ReplicaCheckpoint,
    ) -> DeliveryResult<DeliveryProgress> {
        if message.source.incarnation != self.source.incarnation
            || message.envelope.stream_id() != self.baseline.stream_id
            || confirmed != message.expected_checkpoint()
            || u64::try_from(message.source.sequence).ok() != Some(confirmed.sequence)
        {
            return Err(OrderedDeliveryError::ReceiptMismatch);
        }
        checkpoint(cx)?;
        let transaction = conn.transaction().await?;
        let result = async {
            Self::validate_route(&transaction, cx).await?;
            let before = self.load_progress(&transaction).await?;
            if confirmed.sequence < before.acknowledged.sequence {
                return Err(ReplicaApplyError::Stale {
                    current: before.acknowledged.sequence,
                    received: confirmed.sequence,
                }
                .into());
            }
            let status = self
                .source
                .lookup_in(&transaction, message.source.message_id)
                .await?
                .ok_or_else(|| invalid("delivery source request identity is missing"))?;
            if status.receipt != message.source {
                return Err(OrderedDeliveryError::ReceiptMismatch);
            }
            if confirmed.sequence == before.acknowledged.sequence {
                if confirmed != before.acknowledged || !status.acknowledged {
                    return Err(OrderedDeliveryError::ReceiptMismatch);
                }
                return Ok(before);
            }
            let expected = before
                .acknowledged
                .sequence
                .checked_add(1)
                .ok_or(FrankenError::TooBig)?;
            if confirmed.sequence != expected {
                return Err(ReplicaApplyError::Gap {
                    expected,
                    received: confirmed.sequence,
                }
                .into());
            }
            if message.envelope.previous() != before.acknowledged.tip || status.acknowledged {
                return Err(OrderedDeliveryError::ReceiptMismatch);
            }
            self.source
                .acknowledge_in(&transaction, cx, &message.source)
                .await?;
            checkpoint(cx)?;
            let changed = transaction
                .execute_with_params(
                    ADVANCE_ROUTE,
                    &[
                        SqliteValue::Integer(
                            i64::try_from(confirmed.sequence).map_err(|_| FrankenError::TooBig)?,
                        ),
                        blob(confirmed.tip.as_bytes()),
                        blob(&self.source.incarnation),
                        blob(self.baseline.stream_id.as_bytes()),
                        blob(self.baseline.tip.as_bytes()),
                        SqliteValue::Integer(
                            i64::try_from(before.acknowledged.sequence)
                                .map_err(|_| FrankenError::TooBig)?,
                        ),
                        blob(before.acknowledged.tip.as_bytes()),
                    ],
                )
                .await?;
            if changed != 1 {
                return Err(invalid("delivery ACK lost its predecessor"));
            }
            let after = self.load_progress(&transaction).await?;
            if after.acknowledged != confirmed
                || after.produced_sequence != before.produced_sequence
                || before.pending_messages.checked_sub(1) != Some(after.pending_messages)
                || before
                    .pending_bytes
                    .checked_sub(message.source.payload_bytes)
                    != Some(after.pending_bytes)
            {
                return Err(invalid("delivery ACK failed atomic accounting readback"));
            }
            Ok(after)
        }
        .await;
        settle(transaction, cx, result).await
    }
}

async fn settle<T>(
    mut transaction: Transaction<'_>,
    cx: &Cx,
    result: DeliveryResult<T>,
) -> DeliveryResult<T> {
    let result = result.and_then(|value| {
        checkpoint(cx)?;
        Ok(value)
    });
    let failure = match result {
        Ok(value) => match transaction.commit().await {
            Ok(()) => return Ok(value),
            Err(error) => CaptureError::Commit(error).into(),
        },
        Err(error) => error,
    };
    match transaction.rollback().await {
        Ok(()) | Err(FrankenError::NoActiveTransaction) => Err(failure),
        Err(error) => Err(OrderedDeliveryError::Rollback {
            cause: Box::new(failure),
            error,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use asupersync::io::{AsyncRead, ReadBuf};

    use super::*;
    use crate::compat::capture::outbox::OutboxLimits;
    use crate::compat::capture::{CaptureOptions, ChangesetCapture};
    use crate::compat::changeset::streaming::ordered::{self, ReplicaDisposition};
    use crate::compat::changeset_stream::ChangesetStreamLimits;

    fn route() -> OrderedDelivery {
        OrderedDelivery::new(
            ChangesetOutbox::new([7; 16], OutboxLimits::default()).unwrap(),
            ReplicaCheckpoint {
                stream_id: PayloadHash::from_bytes([11; 32]),
                sequence: 0,
                tip: PayloadHash::from_bytes([13; 32]),
            },
        )
        .unwrap()
    }

    async fn database() -> Connection {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch(
            "PRAGMA recursive_triggers=ON; CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT);",
        )
        .await
        .unwrap();
        conn
    }

    async fn insert(
        conn: &mut Connection,
        cx: &Cx,
        route: &OrderedDelivery,
        id: u8,
    ) -> OutboxReceipt {
        let mut capture = ChangesetCapture::begin(conn, cx, CaptureOptions::new(["items"]))
            .await
            .unwrap();
        capture
            .execute(
                "INSERT INTO items VALUES(?1,'captured')",
                &[SqliteValue::Integer(i64::from(id))],
            )
            .await
            .unwrap();
        capture
            .commit_to_outbox(route.outbox(), [id; 16])
            .await
            .unwrap()
    }

    struct Fragmented<'a>(&'a [u8]);
    impl AsyncRead for Fragmented<'_> {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            output: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            let n = this.0.len().min(output.remaining()).min(7);
            output.put_slice(&this.0[..n]);
            this.0 = &this.0[n..];
            Poll::Ready(Ok(()))
        }
    }

    async fn receive(
        conn: &mut Connection,
        cx: &Cx,
        message: &OrderedMessage,
    ) -> ordered::ReplicaApplyReceipt {
        ordered::apply(
            conn,
            cx,
            &mut Fragmented(message.body()),
            message.envelope(),
            message.envelope().id(),
            ChangesetStreamLimits::default(),
        )
        .await
        .unwrap()
    }

    #[test]
    fn captured_source_reaches_ordered_receiver_and_keeps_request_tombstone() {
        asupersync::test_utils::run_test(|| async {
            let route = route();
            let cx = Cx::new();
            let mut source = database().await;
            let mut target = database().await;
            route.initialize(&mut source, &cx).await.unwrap();
            ordered::initialize(&mut target, &cx, route.baseline())
                .await
                .unwrap();
            let source_receipt = insert(&mut source, &cx, &route, 1).await;
            let message = route
                .next_pending(&mut source, &cx, 1024)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(message.source_receipt(), &source_receipt);
            assert_eq!(message.envelope().previous(), route.baseline().tip);
            let raw = route
                .outbox()
                .pending(&mut source, &cx, 1, 1024)
                .await
                .unwrap();
            assert_eq!(message.body(), raw[0].bytes);
            let receipt = receive(&mut target, &cx, &message).await;
            assert!(
                matches!(receipt.disposition, ReplicaDisposition::Applied(report) if report.applied == 1)
            );
            let progress = route
                .acknowledge(&mut source, &cx, &message, receipt.checkpoint)
                .await
                .unwrap();
            assert_eq!(progress.pending_messages, 0);
            assert_eq!(progress.pending_bytes, 0);
            assert_eq!(
                route
                    .acknowledge(&mut source, &cx, &message, receipt.checkpoint)
                    .await
                    .unwrap(),
                progress
            );
            assert!(
                route
                    .outbox()
                    .lookup(&mut source, &cx, [1; 16])
                    .await
                    .unwrap()
                    .unwrap()
                    .acknowledged
            );
            assert!(
                route
                    .next_pending(&mut source, &cx, 1024)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                integer(
                    &source
                        .query_row("SELECT count(*) FROM items")
                        .await
                        .unwrap(),
                    0
                )
                .unwrap(),
                1,
                "delivery must not replay captured source DML"
            );
            assert_eq!(
                text(
                    &target
                        .query_row("SELECT value FROM items WHERE id=1")
                        .await
                        .unwrap(),
                    0
                )
                .unwrap(),
                "captured"
            );
        });
    }

    #[test]
    fn lost_receiver_response_retries_same_envelope_without_repeating_triggers() {
        asupersync::test_utils::run_test(|| async {
            let route = route();
            let cx = Cx::new();
            let mut source = database().await;
            let mut target = database().await;
            target.execute_batch("CREATE TABLE audit(id INTEGER); CREATE TRIGGER audit_items AFTER INSERT ON items BEGIN INSERT INTO audit VALUES(new.id); END;").await.unwrap();
            route.initialize(&mut source, &cx).await.unwrap();
            ordered::initialize(&mut target, &cx, route.baseline())
                .await
                .unwrap();
            insert(&mut source, &cx, &route, 1).await;
            insert(&mut source, &cx, &route, 2).await;
            let first = route
                .next_pending(&mut source, &cx, 1024)
                .await
                .unwrap()
                .unwrap();
            receive(&mut target, &cx, &first).await;
            let retry = route
                .next_pending(&mut source, &cx, 1024)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(retry.envelope(), first.envelope());
            assert_eq!(retry.body(), first.body());
            let confirmed = receive(&mut target, &cx, &retry).await;
            assert_eq!(confirmed.disposition, ReplicaDisposition::AlreadyApplied);
            assert_eq!(
                integer(
                    &target
                        .query_row("SELECT count(*) FROM audit")
                        .await
                        .unwrap(),
                    0
                )
                .unwrap(),
                1
            );
            route
                .acknowledge(&mut source, &cx, &retry, confirmed.checkpoint)
                .await
                .unwrap();
            let second = route
                .next_pending(&mut source, &cx, 1024)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(second.envelope().sequence(), 2);
            assert_eq!(second.envelope().previous(), first.envelope().id());
            let confirmed = receive(&mut target, &cx, &second).await;
            route
                .acknowledge(&mut source, &cx, &second, confirmed.checkpoint)
                .await
                .unwrap();
            assert!(matches!(
                route
                    .acknowledge(&mut source, &cx, &first, first.expected_checkpoint())
                    .await,
                Err(OrderedDeliveryError::Replication(
                    ReplicaApplyError::Stale { .. }
                ))
            ));
        });
    }

    #[test]
    fn raw_ack_and_wrong_receiver_checkpoints_cannot_reclaim_routed_payloads() {
        asupersync::test_utils::run_test(|| async {
            let route = route();
            let cx = Cx::new();
            let mut source = database().await;
            route.initialize(&mut source, &cx).await.unwrap();
            let receipt = insert(&mut source, &cx, &route, 1).await;
            let message = route
                .next_pending(&mut source, &cx, 1024)
                .await
                .unwrap()
                .unwrap();
            assert!(
                route
                    .outbox()
                    .acknowledge(&mut source, &cx, &receipt)
                    .await
                    .is_err()
            );
            for field in 0..3 {
                let mut wrong = message.expected_checkpoint();
                match field {
                    0 => wrong.sequence += 1,
                    1 => wrong.stream_id = PayloadHash::from_bytes([99; 32]),
                    _ => wrong.tip = PayloadHash::from_bytes([98; 32]),
                }
                assert!(matches!(
                    route.acknowledge(&mut source, &cx, &message, wrong).await,
                    Err(OrderedDeliveryError::ReceiptMismatch)
                ));
            }
            assert_eq!(
                route
                    .progress(&mut source, &cx)
                    .await
                    .unwrap()
                    .pending_messages,
                1
            );
            assert_eq!(
                route
                    .next_pending(&mut source, &cx, 1024)
                    .await
                    .unwrap()
                    .unwrap()
                    .body(),
                message.body()
            );
        });
    }

    #[test]
    fn route_reopen_and_reinitialization_preserve_the_exact_acknowledged_predecessor() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("routed.db");
            let route = route();
            let cx = Cx::new();
            let mut source = Connection::open(path.to_str().unwrap()).await.unwrap();
            source.execute_batch("PRAGMA recursive_triggers=ON; CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT);").await.unwrap();
            route.initialize(&mut source, &cx).await.unwrap();
            insert(&mut source, &cx, &route, 1).await;
            insert(&mut source, &cx, &route, 2).await;
            let first = route
                .next_pending(&mut source, &cx, 1024)
                .await
                .unwrap()
                .unwrap();
            let before = route
                .acknowledge(&mut source, &cx, &first, first.expected_checkpoint())
                .await
                .unwrap();
            let next = route
                .next_pending(&mut source, &cx, 1024)
                .await
                .unwrap()
                .unwrap();
            source.close().await.unwrap();
            let mut source = Connection::open(path.to_str().unwrap()).await.unwrap();
            assert_eq!(route.initialize(&mut source, &cx).await.unwrap(), before);
            let reopened = route
                .next_pending(&mut source, &cx, 1024)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reopened.envelope(), next.envelope());
            assert_eq!(reopened.body(), next.body());
            let foreign = OrderedDelivery::new(
                route.outbox().clone(),
                ReplicaCheckpoint {
                    tip: PayloadHash::from_bytes([55; 32]),
                    ..route.baseline()
                },
            )
            .unwrap();
            assert!(foreign.initialize(&mut source, &cx).await.is_err());
            assert_eq!(route.progress(&mut source, &cx).await.unwrap(), before);
        });
    }

    #[test]
    fn legacy_acknowledged_log_is_not_silently_rebased() {
        asupersync::test_utils::run_test(|| async {
            let route = route();
            let cx = Cx::new();
            let mut source = database().await;
            route.outbox().initialize(&mut source, &cx).await.unwrap();
            let first = insert(&mut source, &cx, &route, 1).await;
            insert(&mut source, &cx, &route, 2).await;
            route
                .outbox()
                .acknowledge(&mut source, &cx, &first)
                .await
                .unwrap();
            assert!(route.initialize(&mut source, &cx).await.is_err());
            assert!(source.query("SELECT name FROM main.sqlite_schema WHERE name='__fsqlite_changeset_delivery_route'").await.unwrap().is_empty());
            assert_eq!(
                route
                    .outbox()
                    .pending(&mut source, &cx, 2, 1024)
                    .await
                    .unwrap()
                    .len(),
                1
            );
        });
    }

    #[test]
    fn queue_holes_and_out_of_band_acks_fail_closed_instead_of_skipping_messages() {
        asupersync::test_utils::run_test(|| async {
            for mutation in [
                "DELETE FROM __fsqlite_changeset_outbox WHERE sequence=1",
                "UPDATE __fsqlite_changeset_outbox SET payload=NULL WHERE sequence=2; UPDATE __fsqlite_changeset_outbox_state SET pending_messages=1,pending_bytes=(SELECT payload_bytes FROM __fsqlite_changeset_outbox WHERE sequence=1)",
                "UPDATE __fsqlite_changeset_outbox SET sequence=3 WHERE sequence=2",
                "UPDATE __fsqlite_changeset_outbox_state SET pending_bytes=0",
            ] {
                let route = route();
                let cx = Cx::new();
                let mut source = database().await;
                route.initialize(&mut source, &cx).await.unwrap();
                insert(&mut source, &cx, &route, 1).await;
                insert(&mut source, &cx, &route, 2).await;
                source.execute_batch(mutation).await.unwrap();
                assert!(
                    route.next_pending(&mut source, &cx, 1024).await.is_err(),
                    "{mutation}"
                );
            }
        });
    }

    #[test]
    fn body_bounds_and_hash_failures_never_look_like_a_drained_queue() {
        asupersync::test_utils::run_test(|| async {
            let route = route();
            let cx = Cx::new();
            let mut source = database().await;
            route.initialize(&mut source, &cx).await.unwrap();
            let receipt = insert(&mut source, &cx, &route, 1).await;
            assert!(matches!(
                route.next_pending(&mut source, &cx, 1).await,
                Err(OrderedDeliveryError::Source(CaptureError::Limit(_)))
            ));
            source
                .execute_with_params(
                    "UPDATE __fsqlite_changeset_outbox SET payload=zeroblob(?1) WHERE sequence=1",
                    &[super::super::number(receipt.payload_bytes).unwrap()],
                )
                .await
                .unwrap();
            assert!(route.next_pending(&mut source, &cx, 1024).await.is_err());
            assert!(
                !route
                    .outbox()
                    .lookup(&mut source, &cx, [1; 16])
                    .await
                    .unwrap()
                    .unwrap()
                    .acknowledged
            );
        });
    }

    #[test]
    fn zero_byte_message_still_advances_the_receiver_and_route() {
        asupersync::test_utils::run_test(|| async {
            let route = route();
            let cx = Cx::new();
            let mut source = database().await;
            let mut target = database().await;
            route.initialize(&mut source, &cx).await.unwrap();
            ordered::initialize(&mut target, &cx, route.baseline())
                .await
                .unwrap();
            let capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["items"]))
                .await
                .unwrap();
            capture
                .commit_to_outbox(route.outbox(), [1; 16])
                .await
                .unwrap();
            let message = route
                .next_pending(&mut source, &cx, 1)
                .await
                .unwrap()
                .unwrap();
            assert!(message.body().is_empty());
            let confirmed = receive(&mut target, &cx, &message).await;
            assert_eq!(confirmed.checkpoint.sequence, 1);
            let progress = route
                .acknowledge(&mut source, &cx, &message, confirmed.checkpoint)
                .await
                .unwrap();
            assert_eq!(progress.pending_messages, 0);
            assert_eq!(progress.pending_bytes, 0);
        });
    }

    #[test]
    fn abandoned_ack_mutations_restore_both_payload_and_route_before_next_sql() {
        asupersync::test_utils::run_test(|| async {
            let route = route();
            let cx = Cx::new();
            let mut source = database().await;
            route.initialize(&mut source, &cx).await.unwrap();
            insert(&mut source, &cx, &route, 1).await;
            let message = route
                .next_pending(&mut source, &cx, 1024)
                .await
                .unwrap()
                .unwrap();
            let before = route.progress(&mut source, &cx).await.unwrap();
            let transaction = source.transaction().await.unwrap();
            route
                .source
                .acknowledge_in(&transaction, &cx, message.source_receipt())
                .await
                .unwrap();
            drop(transaction); // The actual raw ACK mutation ran, but not COMMIT.
            assert_eq!(route.progress(&mut source, &cx).await.unwrap(), before);
            assert_eq!(
                route
                    .next_pending(&mut source, &cx, 1024)
                    .await
                    .unwrap()
                    .unwrap()
                    .body(),
                message.body()
            );
            cx.cancel();
            assert!(
                route
                    .acknowledge(&mut source, &cx, &message, message.expected_checkpoint())
                    .await
                    .is_err()
            );
            assert_eq!(
                route.progress(&mut source, &Cx::new()).await.unwrap(),
                before
            );
        });
    }
}
