//! All-recipient delivery of native SQL capture without copying its outbox.
//!
//! A fixed roster of 1..64 receivers shares the existing ordered envelope
//! chain. Each receiver advances independently. The ordinary route cursor is
//! the minimum acknowledged position: a payload becomes a request tombstone
//! only when EVERY member has committed it. Receiver advancement, reclamation,
//! source accounting and the route tip share one source SQL transaction.
//!
//! Provision all receivers from the SAME trusted sequence-zero baseline before
//! delivery. IDs identify obligations, not credentials. Authenticate the named
//! receiver and its committed checkpoint before acknowledging it. This API is
//! not consensus, quorum durability, dynamic membership or snapshot bootstrap.
//! There is no eviction/timeout of a slow member and no automatic SQL replay.
//! A full outbox retains its existing backpressure, including source rollback.
//!
//! One bounded roster is stored in addition to the existing payload queue.
//! Cursor checksums bind the roster, incarnation, baseline and receiver; they
//! detect accidental corruption, not malicious direct edits to trusted SQL.
//! Reads and acknowledgements use the normal concurrent transaction policy.
//! No source transaction spans transport, and no executor or writer mutex is
//! created. COMMIT follows the caller's Connection durability configuration.

use fsqlite_types::{PayloadHash, cx::Cx};

use super::{
    ADVANCE_ROUTE, CaptureError, ChangesetOutbox, Connection, DELIVERY_ROUTE,
    DeliveryProgress, DeliveryResult, FrankenError, INSERT_ROUTE, OrderedDelivery,
    OrderedDeliveryError, OrderedMessage, ROUTE_DDL, ReplicaApplyError,
    ReplicaCheckpoint, SqliteValue, Transaction, TransactionExt, blob, canonical,
    checkpoint, fixed, integer, invalid, settle, text,
};

const TABLE: &str = "__fsqlite_changeset_delivery_fanout";
const DDL: &str = r#"CREATE TABLE "__fsqlite_changeset_delivery_fanout" (
    receiver_id BLOB PRIMARY KEY NOT NULL,
    ack_sequence INTEGER NOT NULL,
    ack_tip BLOB NOT NULL,
    seal BLOB NOT NULL
)"#;
const PRESENT: &str = r#"SELECT name FROM main.sqlite_schema
    WHERE name='__fsqlite_changeset_delivery_fanout' COLLATE NOCASE LIMIT 1"#;
const READ: &str = r#"SELECT receiver_id,ack_sequence,ack_tip,seal
    FROM main.__fsqlite_changeset_delivery_fanout ORDER BY receiver_id LIMIT 65"#;
const INSERT: &str = r#"INSERT OR ABORT INTO main.__fsqlite_changeset_delivery_fanout
    VALUES(?1,0,?2,?3)"#;
const ADVANCE: &str = r#"UPDATE OR ABORT main.__fsqlite_changeset_delivery_fanout
    SET ack_sequence=?1,ack_tip=?2,seal=?3
    WHERE receiver_id=?4 AND ack_sequence=?5 AND ack_tip=?6 AND seal=?7"#;
const MAX_RECEIVERS: usize = 64;

/// A stable receiver obligation, e.g. a nonzero UUID supplied by the caller.
pub type ReceiverId = [u8; 16];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaProgress {
    pub receiver_id: ReceiverId,
    pub acknowledged: ReplicaCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutProgress {
    /// Unique retained payloads/bytes, NOT the sum of every receiver's backlog.
    /// The acknowledged checkpoint is the all-recipient reclamation floor.
    pub source: DeliveryProgress,
    /// Canonical receiver-ID order; at most 64 entries.
    pub replicas: Vec<ReplicaProgress>,
}

/// An immutable delivery roster bound to one captured source incarnation.
/// Changing, dropping or adding members on an existing installation is refused.
#[derive(Debug, Clone)]
pub struct FanoutDelivery {
    route: OrderedDelivery,
    receivers: Vec<ReceiverId>,
    roster_id: PayloadHash,
}

pub(super) async fn require_single_recipient(transaction: &Transaction<'_>) -> DeliveryResult<()> {
    if !transaction.query(PRESENT).await?.is_empty() {
        return Err(CaptureError::Input(
            "fanout delivery requires a named receiver acknowledgement",
        ).into());
    }
    Ok(())
}

impl FanoutDelivery {
    pub fn new(
        source: ChangesetOutbox,
        baseline: ReplicaCheckpoint,
        receivers: impl IntoIterator<Item = ReceiverId>,
    ) -> DeliveryResult<Self> {
        let route = OrderedDelivery::new(source, baseline)?;
        let mut members = Vec::new();
        for receiver in receivers {
            if members.len() == MAX_RECEIVERS || receiver == [0; 16] {
                return Err(CaptureError::Input("fanout requires 1..64 distinct nonzero receiver IDs").into());
            }
            members.push(receiver);
        }
        members.sort_unstable();
        if members.is_empty() || members.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CaptureError::Input("fanout requires 1..64 distinct nonzero receiver IDs").into());
        }
        let mut identity = b"fsqlite:captured-fanout-roster:v1\0".to_vec();
        identity.extend_from_slice(&route.source.incarnation);
        identity.extend_from_slice(baseline.stream_id.as_bytes());
        identity.extend_from_slice(baseline.tip.as_bytes());
        for member in &members {
            identity.extend_from_slice(member);
        }
        let roster_id = PayloadHash::blake3(&identity);
        Ok(Self { route, receivers: members, roster_id })
    }

    #[must_use]
    pub const fn outbox(&self) -> &ChangesetOutbox { self.route.outbox() }

    #[must_use]
    pub const fn baseline(&self) -> ReplicaCheckpoint { self.route.baseline() }

    #[must_use]
    pub fn receivers(&self) -> &[ReceiverId] { &self.receivers }

    fn receiver_slot(&self, receiver: ReceiverId) -> DeliveryResult<usize> {
        self.receivers.binary_search(&receiver).map_err(|_| {
            CaptureError::Input("receiver is not a member of this fanout roster").into()
        })
    }

    fn seal(&self, receiver: ReceiverId, cursor: ReplicaCheckpoint) -> [u8; 32] {
        let mut bytes = b"fsqlite:captured-fanout-cursor:v1\0".to_vec();
        bytes.extend_from_slice(self.roster_id.as_bytes());
        bytes.extend_from_slice(&receiver);
        bytes.extend_from_slice(&cursor.sequence.to_le_bytes());
        bytes.extend_from_slice(cursor.tip.as_bytes());
        *PayloadHash::blake3(&bytes).as_bytes()
    }

    /// Atomically initialize source, route and complete roster, or verify the
    /// same existing installation without changing progress. A populated source
    /// can enroll only while its entire sequence-zero history remains pending.
    /// There is no automatic adoption of partially initialized or pruned logs.
    pub async fn initialize(&self, conn: &mut Connection, cx: &Cx) -> DeliveryResult<FanoutProgress> {
        checkpoint(cx)?;
        let transaction = conn.transaction().await?;
        let result = async {
            let enrolled = !transaction.query(PRESENT).await?.is_empty();
            self.route.source.initialize_in(&transaction, cx).await?;
            let routes = transaction.query_with_params(
                "SELECT name FROM main.sqlite_schema WHERE CAST(name AS BLOB)=?1 LIMIT 2",
                &[blob(DELIVERY_ROUTE.as_bytes())],
            ).await?;
            if routes.is_empty() {
                if enrolled { return Err(invalid("fanout roster has no source route")); }
                transaction.execute(ROUTE_DDL).await?;
                transaction.execute_with_params(INSERT_ROUTE, &[
                    blob(&self.route.source.incarnation),
                    blob(self.baseline().stream_id.as_bytes()),
                    blob(self.baseline().tip.as_bytes()),
                ]).await?;
            }
            OrderedDelivery::validate_route(&transaction, cx).await?;
            let progress = self.route.load_progress(&transaction).await?;
            if !enrolled {
                if progress.acknowledged != self.baseline() {
                    return Err(invalid("fanout enrollment requires the complete unacknowledged baseline history"));
                }
                transaction.execute(DDL).await?;
                for &receiver in &self.receivers {
                    checkpoint(cx)?;
                    let changed = transaction.execute_with_params(INSERT, &[
                        blob(&receiver), blob(self.baseline().tip.as_bytes()),
                        blob(&self.seal(receiver, self.baseline())),
                    ]).await?;
                    if changed != 1 { return Err(invalid("fanout enrollment did not insert one member")); }
                }
            }
            self.load(&transaction, cx).await
        }.await;
        settle(transaction, cx, result).await
    }

    async fn validate_schema(transaction: &Transaction<'_>, cx: &Cx) -> DeliveryResult<()> {
        OrderedDelivery::validate_route(transaction, cx).await?;
        let rows = transaction.query_with_params(
            "SELECT sql FROM main.sqlite_schema WHERE CAST(name AS BLOB)=?1 AND length(CAST(sql AS BLOB))<=8192 LIMIT 2",
            &[blob(TABLE.as_bytes())],
        ).await?;
        let [row] = rows.as_slice() else { return Err(invalid("fanout roster is missing or ambiguous")); };
        if canonical(text(row, 0)?)? != canonical(DDL)? {
            return Err(invalid("incompatible fanout roster schema"));
        }
        let indexes = transaction.query("PRAGMA main.index_list('__fsqlite_changeset_delivery_fanout')").await?;
        if indexes.len() != 1 || integer(&indexes[0], 2)? != 1
            || text(&indexes[0], 3)? != "pk" || integer(&indexes[0], 4)? != 0
        {
            return Err(invalid("fanout roster has unexpected indexes"));
        }
        for catalog in ["main", "temp"] {
            checkpoint(cx)?;
            let triggers = transaction.query_with_params(
                &format!("SELECT name FROM {catalog}.sqlite_schema WHERE type='trigger' AND tbl_name=?1 COLLATE NOCASE LIMIT 1"),
                &[SqliteValue::Text(TABLE.into())],
            ).await?;
            if !triggers.is_empty() { return Err(invalid("fanout roster must not have application triggers")); }
        }
        Ok(())
    }

    async fn load(&self, transaction: &Transaction<'_>, cx: &Cx) -> DeliveryResult<FanoutProgress> {
        Self::validate_schema(transaction, cx).await?;
        let source = self.route.load_progress(transaction).await?;
        let rows = transaction.query(READ).await?;
        if rows.len() != self.receivers.len() {
            return Err(invalid("fanout roster does not match the configured members"));
        }
        let mut replicas: Vec<ReplicaProgress> = Vec::with_capacity(rows.len());
        for (row, &expected) in rows.iter().zip(&self.receivers) {
            checkpoint(cx)?;
            let receiver_id = fixed::<16>(row, 0)?;
            let cursor = ReplicaCheckpoint {
                stream_id: self.baseline().stream_id,
                sequence: u64::try_from(integer(row, 1)?).map_err(|_| invalid("negative fanout cursor"))?,
                tip: PayloadHash::from_bytes(fixed(row, 2)?),
            };
            if receiver_id != expected || fixed::<32>(row, 3)? != self.seal(receiver_id, cursor)
                || cursor.sequence < source.acknowledged.sequence
                || cursor.sequence > source.produced_sequence
                || (cursor.sequence == source.acknowledged.sequence && cursor != source.acknowledged)
                || replicas.iter().any(|other| other.acknowledged.sequence == cursor.sequence
                    && other.acknowledged.tip != cursor.tip)
            {
                return Err(invalid("fanout cursor identity, checksum or predecessor is inconsistent"));
            }
            replicas.push(ReplicaProgress { receiver_id, acknowledged: cursor });
        }
        let floor = replicas.iter().map(|member| member.acknowledged.sequence).min();
        if floor != Some(source.acknowledged.sequence) {
            return Err(invalid("fanout minimum and source reclamation floor disagree"));
        }
        Ok(FanoutProgress { source, replicas })
    }

    pub async fn progress(&self, conn: &mut Connection, cx: &Cx) -> DeliveryResult<FanoutProgress> {
        checkpoint(cx)?;
        let transaction = conn.transaction().await?;
        let result = self.load(&transaction, cx).await;
        settle(transaction, cx, result).await
    }

    /// Read one receiver's oldest pending message; another receiver may lag
    /// arbitrarily within the source's retained capacity. None means this
    /// receiver was caught up at the read, NOT that the whole group is drained.
    /// Only this message's owned payload is returned; transport runs afterward.
    pub async fn next_pending(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        receiver: ReceiverId,
        max_message_bytes: usize,
    ) -> DeliveryResult<Option<OrderedMessage>> {
        let slot = self.receiver_slot(receiver)?;
        if max_message_bytes == 0 || max_message_bytes > super::super::MAX_PAYLOAD {
            return Err(CaptureError::Input("delivery message bound must be in 1..64 MiB").into());
        }
        checkpoint(cx)?;
        let transaction = conn.transaction().await?;
        let result = async {
            let progress = self.load(&transaction, cx).await?;
            let cursor = progress.replicas[slot].acknowledged;
            if cursor.sequence == progress.source.produced_sequence { return Ok(None); }
            self.route.message_after(&transaction, cx, cursor, max_message_bytes).await.map(Some)
        }.await;
        settle(transaction, cx, result).await
    }

    /// Accept ONLY the named receiver's authenticated committed checkpoint.
    /// Exact current ACK replay is idempotent, including after payload release;
    /// older ACKs are Stale. A loss/error after COMMIT remains uncertain: inspect
    /// progress or retry the same identity rather than repeating source SQL.
    /// There is no automatic member removal or quorum-based reclamation.
    pub async fn acknowledge(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        receiver: ReceiverId,
        message: &OrderedMessage,
        confirmed: ReplicaCheckpoint,
    ) -> DeliveryResult<FanoutProgress> {
        let slot = self.receiver_slot(receiver)?;
        if message.source.incarnation != self.route.source.incarnation
            || message.envelope.stream_id() != self.baseline().stream_id
            || confirmed != message.expected_checkpoint()
            || u64::try_from(message.source.sequence).ok() != Some(confirmed.sequence)
        {
            return Err(OrderedDeliveryError::ReceiptMismatch);
        }
        checkpoint(cx)?;
        let transaction = conn.transaction().await?;
        let result = self.acknowledge_in(&transaction, cx, slot, message, confirmed).await;
        settle(transaction, cx, result).await
    }

    async fn acknowledge_in(
        &self,
        transaction: &Transaction<'_>,
        cx: &Cx,
        slot: usize,
        message: &OrderedMessage,
        confirmed: ReplicaCheckpoint,
    ) -> DeliveryResult<FanoutProgress> {
        let before = self.load(transaction, cx).await?;
        let member = before.replicas[slot];
        let cursor = member.acknowledged;
        if confirmed.sequence < cursor.sequence {
            return Err(ReplicaApplyError::Stale { current: cursor.sequence, received: confirmed.sequence }.into());
        }
        let status = self.route.source.lookup_in(transaction, message.source.message_id).await?
            .ok_or_else(|| invalid("fanout source request identity is missing"))?;
        if status.receipt != message.source { return Err(OrderedDeliveryError::ReceiptMismatch); }
        if confirmed.sequence == cursor.sequence {
            if confirmed != cursor { return Err(OrderedDeliveryError::ReceiptMismatch); }
            return Ok(before);
        }
        let expected = cursor.sequence.checked_add(1).ok_or(FrankenError::TooBig)?;
        if confirmed.sequence != expected {
            return Err(ReplicaApplyError::Gap { expected, received: confirmed.sequence }.into());
        }
        if message.envelope.previous() != cursor.tip || status.acknowledged {
            return Err(OrderedDeliveryError::ReceiptMismatch);
        }
        let mut replicas = before.replicas.clone();
        replicas[slot].acknowledged = confirmed;
        let floor = replicas.iter().min_by_key(|member| member.acknowledged.sequence)
            .ok_or_else(|| invalid("fanout roster is empty"))?.acknowledged;
        let releases = floor.sequence != before.source.acknowledged.sequence;
        // Advancing ONE receiver by ONE message can move the shared floor by
        // at most one. The acknowledged message itself is the one being freed.
        if (releases && (floor != confirmed
            || floor.sequence != before.source.acknowledged.sequence.checked_add(1).ok_or(FrankenError::TooBig)?))
            || (!releases && floor != before.source.acknowledged)
        {
            return Err(invalid("fanout acknowledgement attempted to skip retained history"));
        }
        checkpoint(cx)?;
        let changed = transaction.execute_with_params(ADVANCE, &[
            SqliteValue::Integer(i64::try_from(confirmed.sequence).map_err(|_| FrankenError::TooBig)?),
            blob(confirmed.tip.as_bytes()), blob(&self.seal(member.receiver_id, confirmed)),
            blob(&member.receiver_id), SqliteValue::Integer(i64::try_from(cursor.sequence).map_err(|_| FrankenError::TooBig)?),
            blob(cursor.tip.as_bytes()), blob(&self.seal(member.receiver_id, cursor)),
        ]).await?;
        if changed != 1 { return Err(invalid("fanout ACK lost its receiver predecessor")); }
        if releases {
            self.route.source.acknowledge_in(transaction, cx, &message.source).await?;
            checkpoint(cx)?;
            let changed = transaction.execute_with_params(ADVANCE_ROUTE, &[
                SqliteValue::Integer(i64::try_from(floor.sequence).map_err(|_| FrankenError::TooBig)?),
                blob(floor.tip.as_bytes()), blob(&self.route.source.incarnation),
                blob(self.baseline().stream_id.as_bytes()), blob(self.baseline().tip.as_bytes()),
                SqliteValue::Integer(i64::try_from(before.source.acknowledged.sequence).map_err(|_| FrankenError::TooBig)?),
                blob(before.source.acknowledged.tip.as_bytes()),
            ]).await?;
            if changed != 1 { return Err(invalid("fanout ACK lost its source floor")); }
        }
        let after = self.load(transaction, cx).await?;
        let released_messages = usize::from(releases);
        let released_bytes = if releases { message.source.payload_bytes } else { 0 };
        if after.replicas != replicas || after.source.acknowledged != floor
            || after.source.produced_sequence != before.source.produced_sequence
            || before.source.pending_messages.checked_sub(released_messages) != Some(after.source.pending_messages)
            || before.source.pending_bytes.checked_sub(released_bytes) != Some(after.source.pending_bytes)
        {
            return Err(invalid("fanout ACK failed atomic accounting readback"));
        }
        Ok(after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::tests::{database, insert, receive, route};
    use crate::compat::capture::{CaptureOptions, ChangesetCapture};
    use crate::compat::capture::outbox::OutboxLimits;
    use crate::compat::changeset::streaming::ordered::{self, ReplicaDisposition};

    const FAST: ReceiverId = [1; 16];
    const SLOW: ReceiverId = [2; 16];

    fn group() -> FanoutDelivery {
        let route = route();
        FanoutDelivery::new(route.source, route.baseline, [SLOW, FAST]).unwrap()
    }

    async fn next(group: &FanoutDelivery, conn: &mut Connection, cx: &Cx, receiver: ReceiverId) -> OrderedMessage {
        group.next_pending(conn, cx, receiver, 4096).await.unwrap().unwrap()
    }

    #[test]
    fn fast_receiver_advances_while_slow_receiver_retains_one_shared_payload_queue() {
        asupersync::test_utils::run_test(|| async {
            let group = group();
            let cx = Cx::new();
            let mut source = database().await;
            let mut fast = database().await;
            let mut slow = database().await;
            group.initialize(&mut source, &cx).await.unwrap();
            for target in [&mut fast, &mut slow] {
                ordered::initialize(target, &cx, group.baseline()).await.unwrap();
            }
            for id in 1..=3 { insert(&mut source, &cx, &group.route, id).await; }
            let initial = group.progress(&mut source, &cx).await.unwrap();
            let mut sent = Vec::new();
            for sequence in 1..=3 {
                let message = next(&group, &mut source, &cx, FAST).await;
                assert_eq!(message.envelope().sequence(), sequence);
                let receipt = receive(&mut fast, &cx, &message).await;
                let progress = group.acknowledge(&mut source, &cx, FAST, &message, receipt.checkpoint).await.unwrap();
                assert_eq!(progress.source, initial.source, "a slow member must retain every payload");
                assert_eq!(progress.replicas[0].acknowledged.sequence, sequence);
                assert_eq!(progress.replicas[1].acknowledged.sequence, 0);
                sent.push(message);
            }
            assert!(group.next_pending(&mut source, &cx, FAST, 4096).await.unwrap().is_none());
            let mut remaining_bytes = initial.source.pending_bytes;
            for (index, fast_message) in sent.iter().enumerate() {
                let message = next(&group, &mut source, &cx, SLOW).await;
                assert_eq!(message.envelope(), fast_message.envelope());
                assert_eq!(message.body(), fast_message.body());
                let receipt = receive(&mut slow, &cx, &message).await;
                remaining_bytes -= message.source_receipt().payload_bytes;
                let progress = group.acknowledge(&mut source, &cx, SLOW, &message, receipt.checkpoint).await.unwrap();
                assert_eq!(progress.source.acknowledged, receipt.checkpoint);
                assert_eq!(progress.source.pending_messages, sent.len() - index - 1);
                assert_eq!(progress.source.pending_bytes, remaining_bytes);
                let tombstone = group.outbox().lookup(&mut source, &cx, message.source_receipt().message_id).await.unwrap().unwrap();
                assert!(tombstone.acknowledged);
                assert_eq!(&tombstone.receipt, message.source_receipt());
                assert_eq!(group.acknowledge(&mut source, &cx, SLOW, &message, receipt.checkpoint).await.unwrap(), progress);
            }
            let last = sent.last().unwrap();
            let final_progress = group.progress(&mut source, &cx).await.unwrap();
            assert_eq!(group.acknowledge(&mut source, &cx, FAST, last, last.expected_checkpoint()).await.unwrap(), final_progress);
            for conn in [&source, &fast, &slow] {
                let rows = conn.query("SELECT id,value FROM items ORDER BY id").await.unwrap();
                assert_eq!(rows.len(), 3);
                for (row, id) in rows.iter().zip(1..=3) {
                    assert_eq!(integer(row, 0).unwrap(), id);
                    assert_eq!(text(row, 1).unwrap(), "captured");
                }
            }
            assert!(group.next_pending(&mut source, &cx, SLOW, 4096).await.unwrap().is_none());
            source.close().await.unwrap(); fast.close().await.unwrap(); slow.close().await.unwrap();
        });
    }

    #[test]
    fn lost_receiver_reply_replays_current_envelope_without_repeating_trigger_effects() {
        asupersync::test_utils::run_test(|| async {
            let group = group(); let cx = Cx::new();
            let mut source = database().await; let mut target = database().await;
            target.execute_batch("CREATE TABLE audit(id INTEGER); CREATE TRIGGER audit_items AFTER INSERT ON items BEGIN INSERT INTO audit VALUES(new.id); END;").await.unwrap();
            group.initialize(&mut source, &cx).await.unwrap();
            ordered::initialize(&mut target, &cx, group.baseline()).await.unwrap();
            insert(&mut source, &cx, &group.route, 1).await;
            let original = next(&group, &mut source, &cx, FAST).await;
            receive(&mut target, &cx, &original).await;
            // The receiver committed, but no source ACK was accepted.
            let retry = next(&group, &mut source, &cx, FAST).await;
            assert_eq!(retry.envelope(), original.envelope());
            assert_eq!(retry.body(), original.body());
            let receipt = receive(&mut target, &cx, &retry).await;
            assert_eq!(receipt.disposition, ReplicaDisposition::AlreadyApplied);
            let progress = group.acknowledge(&mut source, &cx, FAST, &retry, receipt.checkpoint).await.unwrap();
            assert_eq!(progress.source.pending_messages, 1);
            assert_eq!(integer(&target.query_row("SELECT count(*) FROM audit").await.unwrap(), 0).unwrap(), 1);
            assert_eq!(integer(&source.query_row("SELECT count(*) FROM items").await.unwrap(), 0).unwrap(), 1);
            source.close().await.unwrap(); target.close().await.unwrap();
        });
    }

    #[test]
    fn single_recipient_and_raw_ack_handles_cannot_bypass_the_roster() {
        asupersync::test_utils::run_test(|| async {
            let group = group(); let cx = Cx::new(); let mut source = database().await;
            group.route.initialize(&mut source, &cx).await.unwrap();
            let receipt = insert(&mut source, &cx, &group.route, 1).await;
            let old_message = group.route.next_pending(&mut source, &cx, 4096).await.unwrap().unwrap();
            // Enrollment after production is safe only while all bytes remain.
            let before = group.initialize(&mut source, &cx).await.unwrap();
            assert!(group.route.acknowledge(&mut source, &cx, &old_message, old_message.expected_checkpoint()).await.is_err());
            assert!(group.outbox().acknowledge(&mut source, &cx, &receipt).await.is_err());
            assert_eq!(group.progress(&mut source, &cx).await.unwrap(), before);
            assert_eq!(next(&group, &mut source, &cx, SLOW).await.body(), old_message.body());
            source.close().await.unwrap();
        });
    }

    #[test]
    fn file_reopen_preserves_different_receiver_positions_and_exact_roster() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("fanout.db");
            let group = group(); let cx = Cx::new();
            let mut source = Connection::open(path.to_str().unwrap()).await.unwrap();
            source.execute_batch("PRAGMA recursive_triggers=ON; CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT);").await.unwrap();
            let mut target = database().await;
            group.initialize(&mut source, &cx).await.unwrap();
            ordered::initialize(&mut target, &cx, group.baseline()).await.unwrap();
            for id in 1..=2 { insert(&mut source, &cx, &group.route, id).await; }
            let first = next(&group, &mut source, &cx, FAST).await;
            let receipt = receive(&mut target, &cx, &first).await;
            let before = group.acknowledge(&mut source, &cx, FAST, &first, receipt.checkpoint).await.unwrap();
            let fast_next = next(&group, &mut source, &cx, FAST).await;
            source.close().await.unwrap();
            let mut reopened = Connection::open(path.to_str().unwrap()).await.unwrap();
            let same = FanoutDelivery::new(group.outbox().clone(), group.baseline(), [FAST, SLOW]).unwrap();
            assert_eq!(same.initialize(&mut reopened, &cx).await.unwrap(), before);
            assert_eq!(next(&same, &mut reopened, &cx, FAST).await.envelope(), fast_next.envelope());
            assert_eq!(next(&same, &mut reopened, &cx, SLOW).await.envelope(), first.envelope());
            for members in [vec![FAST], vec![FAST, [3; 16]], vec![FAST, SLOW, [3; 16]]] {
                let changed = FanoutDelivery::new(group.outbox().clone(), group.baseline(), members).unwrap();
                assert!(changed.initialize(&mut reopened, &cx).await.is_err());
                assert_eq!(same.progress(&mut reopened, &cx).await.unwrap(), before);
            }
            target.close().await.unwrap(); reopened.close().await.unwrap();
        });
    }

    #[test]
    fn stale_gap_wrong_and_unknown_receiver_acknowledgements_do_not_change_state() {
        asupersync::test_utils::run_test(|| async {
            let group = group(); let cx = Cx::new();
            let mut source = database().await; let mut target = database().await;
            group.initialize(&mut source, &cx).await.unwrap();
            ordered::initialize(&mut target, &cx, group.baseline()).await.unwrap();
            for id in 1..=2 { insert(&mut source, &cx, &group.route, id).await; }
            let mut sent = Vec::new();
            for _ in 0..2 {
                let message = next(&group, &mut source, &cx, FAST).await;
                let receipt = receive(&mut target, &cx, &message).await;
                group.acknowledge(&mut source, &cx, FAST, &message, receipt.checkpoint).await.unwrap();
                sent.push(message);
            }
            let before = group.progress(&mut source, &cx).await.unwrap();
            assert!(matches!(group.acknowledge(&mut source, &cx, FAST, &sent[0], sent[0].expected_checkpoint()).await,
                Err(OrderedDeliveryError::Replication(ReplicaApplyError::Stale { .. }))));
            assert!(matches!(group.acknowledge(&mut source, &cx, SLOW, &sent[1], sent[1].expected_checkpoint()).await,
                Err(OrderedDeliveryError::Replication(ReplicaApplyError::Gap { .. }))));
            assert!(group.acknowledge(&mut source, &cx, [9; 16], &sent[0], sent[0].expected_checkpoint()).await.is_err());
            for field in 0..3 {
                let mut wrong = sent[0].expected_checkpoint();
                match field {
                    0 => wrong.sequence += 1,
                    1 => wrong.stream_id = PayloadHash::from_bytes([8; 32]),
                    _ => wrong.tip = PayloadHash::from_bytes([9; 32]),
                }
                assert!(matches!(group.acknowledge(&mut source, &cx, SLOW, &sent[0], wrong).await,
                    Err(OrderedDeliveryError::ReceiptMismatch)));
            }
            assert_eq!(group.progress(&mut source, &cx).await.unwrap(), before);
            source.close().await.unwrap(); target.close().await.unwrap();
        });
    }

    #[test]
    fn dropped_final_ack_rolls_back_recipient_payload_accounting_and_route_together() {
        asupersync::test_utils::run_test(|| async {
            let group = group(); let cx = Cx::new();
            let mut source = database().await; let mut fast = database().await; let mut slow = database().await;
            group.initialize(&mut source, &cx).await.unwrap();
            for target in [&mut fast, &mut slow] { ordered::initialize(target, &cx, group.baseline()).await.unwrap(); }
            insert(&mut source, &cx, &group.route, 1).await;
            let message = next(&group, &mut source, &cx, FAST).await;
            let fast_receipt = receive(&mut fast, &cx, &message).await;
            group.acknowledge(&mut source, &cx, FAST, &message, fast_receipt.checkpoint).await.unwrap();
            let slow_receipt = receive(&mut slow, &cx, &message).await;
            let before = group.progress(&mut source, &cx).await.unwrap();
            let transaction = source.transaction().await.unwrap();
            let provisional = group.acknowledge_in(&transaction, &cx, group.receiver_slot(SLOW).unwrap(), &message, slow_receipt.checkpoint).await.unwrap();
            assert_eq!(provisional.source.pending_messages, 0, "real payload mutation must have run");
            drop(transaction); // No COMMIT: deferred rollback belongs to the connection.
            assert_eq!(group.progress(&mut source, &cx).await.unwrap(), before);
            assert_eq!(next(&group, &mut source, &cx, SLOW).await.body(), message.body());
            let cancelled = Cx::new(); cancelled.cancel();
            assert!(group.acknowledge(&mut source, &cancelled, SLOW, &message, slow_receipt.checkpoint).await.is_err());
            assert_eq!(group.progress(&mut source, &cx).await.unwrap(), before);
            assert_eq!(group.acknowledge(&mut source, &cx, SLOW, &message, slow_receipt.checkpoint).await.unwrap().source.pending_messages, 0);
            source.close().await.unwrap(); fast.close().await.unwrap(); slow.close().await.unwrap();
        });
    }

    #[test]
    fn slow_receiver_preserves_source_backpressure_and_failed_capture_rolls_back_dml() {
        asupersync::test_utils::run_test(|| async {
            let template = group();
            let outbox = ChangesetOutbox::new([7; 16], OutboxLimits { max_pending_messages: 1, ..OutboxLimits::default() }).unwrap();
            let group = FanoutDelivery::new(outbox, template.baseline(), [FAST, SLOW]).unwrap();
            let cx = Cx::new(); let mut source = database().await;
            let mut fast = database().await; let mut slow = database().await;
            group.initialize(&mut source, &cx).await.unwrap();
            for target in [&mut fast, &mut slow] { ordered::initialize(target, &cx, group.baseline()).await.unwrap(); }
            insert(&mut source, &cx, &group.route, 1).await;
            let message = next(&group, &mut source, &cx, FAST).await;
            let receipt = receive(&mut fast, &cx, &message).await;
            group.acknowledge(&mut source, &cx, FAST, &message, receipt.checkpoint).await.unwrap();
            let mut capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["items"])).await.unwrap();
            capture.execute("INSERT INTO items VALUES(2,'must roll back')", &[]).await.unwrap();
            assert!(matches!(capture.commit_to_outbox(group.outbox(), [2; 16]).await, Err(CaptureError::Limit(_))));
            assert_eq!(integer(&source.query_row("SELECT count(*) FROM items").await.unwrap(), 0).unwrap(), 1);
            assert!(group.outbox().lookup(&mut source, &cx, [2; 16]).await.unwrap().is_none());
            let receipt = receive(&mut slow, &cx, &message).await;
            group.acknowledge(&mut source, &cx, SLOW, &message, receipt.checkpoint).await.unwrap();
            let second = insert(&mut source, &cx, &group.route, 2).await;
            assert_eq!(second.sequence, 2, "failed capture must not consume a sequence or request ID");
            source.close().await.unwrap(); fast.close().await.unwrap(); slow.close().await.unwrap();
        });
    }

    #[test]
    fn zero_byte_capture_still_requires_every_receiver_acknowledgement() {
        asupersync::test_utils::run_test(|| async {
            let group = group(); let cx = Cx::new(); let mut source = database().await;
            let mut fast = database().await; let mut slow = database().await;
            group.initialize(&mut source, &cx).await.unwrap();
            for target in [&mut fast, &mut slow] { ordered::initialize(target, &cx, group.baseline()).await.unwrap(); }
            let capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["items"])).await.unwrap();
            capture.commit_to_outbox(group.outbox(), [1; 16]).await.unwrap();
            for (receiver, target, expected) in [(FAST, &mut fast, 1), (SLOW, &mut slow, 0)] {
                let message = group.next_pending(&mut source, &cx, receiver, 1).await.unwrap().unwrap();
                assert!(message.body().is_empty());
                let receipt = receive(target, &cx, &message).await;
                let progress = group.acknowledge(&mut source, &cx, receiver, &message, receipt.checkpoint).await.unwrap();
                assert_eq!(progress.source.pending_messages, expected);
                assert_eq!(progress.source.pending_bytes, 0);
            }
            source.close().await.unwrap(); fast.close().await.unwrap(); slow.close().await.unwrap();
        });
    }

    #[test]
    fn malformed_roster_cursors_and_source_gaps_fail_closed() {
        asupersync::test_utils::run_test(|| async {
            for mutation in [
                "DELETE FROM __fsqlite_changeset_delivery_fanout WHERE receiver_id=x'02020202020202020202020202020202'",
                "UPDATE __fsqlite_changeset_delivery_fanout SET ack_tip=zeroblob(32)",
                "UPDATE __fsqlite_changeset_delivery_fanout SET seal=zeroblob(32)",
                "UPDATE __fsqlite_changeset_delivery_fanout SET ack_sequence=1",
                "CREATE TRIGGER bad_roster AFTER UPDATE ON __fsqlite_changeset_delivery_fanout BEGIN SELECT 1; END",
                "DELETE FROM __fsqlite_changeset_outbox WHERE sequence=1",
            ] {
                let group = group(); let cx = Cx::new(); let mut source = database().await;
                group.initialize(&mut source, &cx).await.unwrap();
                insert(&mut source, &cx, &group.route, 1).await;
                source.execute_batch(mutation).await.unwrap();
                assert!(group.progress(&mut source, &cx).await.is_err(), "{mutation}");
                assert!(group.next_pending(&mut source, &cx, FAST, 4096).await.is_err(), "{mutation}");
                source.close().await.unwrap();
            }
        });
    }

    #[test]
    fn enrollment_never_resets_acknowledged_history_and_bounds_are_explicit() {
        let group = group();
        for members in [vec![], vec![[0; 16]], vec![FAST, FAST], vec![FAST; 65]] {
            assert!(FanoutDelivery::new(group.outbox().clone(), group.baseline(), members).is_err());
        }
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new(); let mut source = database().await; let mut target = database().await;
            group.route.initialize(&mut source, &cx).await.unwrap();
            ordered::initialize(&mut target, &cx, group.baseline()).await.unwrap();
            insert(&mut source, &cx, &group.route, 1).await;
            let first = group.route.next_pending(&mut source, &cx, 4096).await.unwrap().unwrap();
            let receipt = receive(&mut target, &cx, &first).await;
            let before = group.route.acknowledge(&mut source, &cx, &first, receipt.checkpoint).await.unwrap();
            assert!(group.initialize(&mut source, &cx).await.is_err());
            assert!(source.query(PRESENT).await.unwrap().is_empty());
            assert_eq!(group.route.progress(&mut source, &cx).await.unwrap(), before);
            source.close().await.unwrap(); target.close().await.unwrap();
        });
    }
}
