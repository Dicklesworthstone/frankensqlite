//! Durable, all-required-replica delivery for the ordered source outbox.
//!
//! Configure a fixed roster at an empty, trusted source position. Every member
//! advances independently, but a body is reclaimed only after ALL members have
//! confirmed it. Roster bytes are stored separately from progress rows: a lost
//! progress row is corruption, never permission to forget that member.
//!
//! This is not consensus, membership discovery, or a quorum durability claim.
//! Authenticate BOTH the replica identity and its committed checkpoint before
//! acknowledging. A fabricated checkpoint is not evidence. Membership cannot
//! be removed or changed through this API; replacing a failed member requires
//! an explicit new stream/bootstrap policy, not silent queue reclamation.
//! SQL/schema/functions remain trusted, and the existing outbox memory,
//! transaction-outcome, cancellation and source-capture boundaries still apply.

use fsqlite_types::{PayloadHash, cx::Cx};

use super::{
    Connection, FrankenError, OutboxMessage, OutboxState, ReplicaApplyError,
    ReplicaCheckpoint, Result, SqliteValue, begin, blob, checkpoint, complete,
    hash_column, integer, load_state, protocol, read_after, row_text, save_state,
    unsigned, validate_schema, validate_tables,
};

/// Maximum fixed recipient roster. This bounds metadata, not SQL engine RSS.
pub const MAX_REPLICAS: usize = 256;
pub(super) const GROUP_TABLE: &str = "_fsqlite_source_fanout_v1";
pub(super) const REPLICA_TABLE: &str = "_fsqlite_source_replica_v1";
const CREATE_GROUP: &str = "CREATE TABLE IF NOT EXISTS main._fsqlite_source_fanout_v1(\
    stream_id BLOB PRIMARY KEY NOT NULL, start_sequence INTEGER NOT NULL, \
    start_tip BLOB NOT NULL, members BLOB NOT NULL)";
const CREATE_REPLICA: &str = "CREATE TABLE IF NOT EXISTS main._fsqlite_source_replica_v1(\
    stream_id BLOB NOT NULL, replica_id BLOB NOT NULL, sequence INTEGER NOT NULL, \
    tip BLOB NOT NULL, PRIMARY KEY(stream_id,replica_id))";

/// The last independently confirmed commit for one required replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaProgress {
    pub replica_id: PayloadHash,
    pub checkpoint: ReplicaCheckpoint,
}

/// Source retention and per-replica progress from the same SQL transaction.
/// The outbox's acknowledged position is the minimum of the member positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutState {
    pub outbox: OutboxState,
    pub baseline: ReplicaCheckpoint,
    /// Canonical bytewise replica-ID order, independent of configuration order.
    pub replicas: Vec<ReplicaProgress>,
}

fn roster(members: &[PayloadHash]) -> Result<Vec<PayloadHash>> {
    if members.is_empty() || members.len() > MAX_REPLICAS {
        return Err(protocol("fanout requires 1..=256 replicas"));
    }
    let mut ordered = members.to_vec();
    ordered.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(protocol("fanout replica identities must be unique"));
    }
    Ok(ordered)
}

// Old single-recipient databases have neither table. Partial/colliding schema
// is refused rather than accidentally downgrading a stream to that old mode.
async fn tables_present(conn: &Connection) -> Result<bool> {
    let objects = conn.query_with_params(
        "SELECT name FROM main.sqlite_schema WHERE name=?1 COLLATE NOCASE OR name=?2 COLLATE NOCASE LIMIT 3",
        &[SqliteValue::Text(GROUP_TABLE.into()), SqliteValue::Text(REPLICA_TABLE.into())],
    ).await?;
    if objects.is_empty() { return Ok(false); }
    if objects.len() != 2 { return Err(protocol("incomplete fanout metadata schema")); }
    validate_tables(conn, &[(GROUP_TABLE, CREATE_GROUP), (REPLICA_TABLE, CREATE_REPLICA)]).await?;
    Ok(true)
}

/// Validate fixed membership, every progress row, and the retention frontier.
/// Body payloads are not requested by these metadata queries.
pub(super) async fn snapshot(conn: &Connection, current: OutboxState) -> Result<Option<FanoutState>> {
    if !tables_present(conn).await? { return Ok(None); }
    let stream_id = current.produced.stream_id;
    let parameter = [blob(stream_id)];
    let rows = conn.query_with_params(
        "SELECT CASE WHEN typeof(start_sequence)='integer' THEN start_sequence END,\
         length(start_tip),typeof(start_tip),length(members),typeof(members) \
         FROM main._fsqlite_source_fanout_v1 WHERE stream_id=?1 LIMIT 2",
        &parameter,
    ).await?;
    let row = match rows.as_slice() {
        [] => {
            let orphan = conn.query_with_params(
                "SELECT 1 FROM main._fsqlite_source_replica_v1 WHERE stream_id=?1 LIMIT 1",
                &parameter,
            ).await?;
            if !orphan.is_empty() { return Err(protocol("orphaned fanout replica progress")); }
            return Ok(None);
        }
        [row] => row,
        _ => return Err(protocol("ambiguous fanout roster")),
    };
    let start_sequence = unsigned(row, 0)?;
    let length = usize::try_from(unsigned(row, 3)?).map_err(|_| FrankenError::TooBig)?;
    if unsigned(row, 1)? != 32 || row_text(row, 2) != Some("blob")
        || row_text(row, 4) != Some("blob") || length == 0 || length > MAX_REPLICAS * 32
        || !length.is_multiple_of(32)
    {
        return Err(protocol("invalid fanout roster dimensions"));
    }
    // Only request the bounded BLOB after its scalar length is admitted.
    let rows = conn.query_with_params(
        "SELECT start_tip,members FROM main._fsqlite_source_fanout_v1 WHERE stream_id=?1 LIMIT 2", &parameter,
    ).await?;
    let [row] = rows.as_slice() else { return Err(protocol("missing fanout roster")); };
    let baseline = ReplicaCheckpoint { stream_id, sequence: start_sequence, tip: hash_column(row, 0)? };
    if baseline.sequence > current.acknowledged.sequence
        || (baseline.sequence == current.acknowledged.sequence && baseline.tip != current.acknowledged.tip)
    {
        return Err(protocol("fanout baseline disagrees with source retention"));
    }
    let Some(SqliteValue::Blob(bytes)) = row.get(1) else { return Err(protocol("invalid fanout roster storage")); };
    if bytes.len() != length { return Err(protocol("fanout roster length changed")); }
    let members: Vec<_> = bytes.as_ref().as_chunks::<32>().0.iter()
        .map(|bytes| PayloadHash::from_bytes(*bytes)).collect();
    if members.windows(2).any(|pair| pair[0].as_bytes() >= pair[1].as_bytes()) {
        return Err(protocol("noncanonical fanout roster"));
    }
    let counts = conn.query_with_params(
        "SELECT count(*) FROM main._fsqlite_source_replica_v1 WHERE stream_id=?1", &parameter,
    ).await?;
    let [count] = counts.as_slice() else { return Err(protocol("missing fanout member count")); };
    if unsigned(count, 0)? != (length / 32) as u64 {
        return Err(protocol("fanout progress does not cover its roster"));
    }
    let rows = conn.query_with_params(
        "SELECT replica_id,sequence,tip FROM main._fsqlite_source_replica_v1 WHERE stream_id=?1 \
         AND typeof(replica_id)='blob' AND length(replica_id)=32 AND typeof(tip)='blob' AND length(tip)=32 \
         AND typeof(sequence)='integer' \
         ORDER BY replica_id LIMIT 257",
        &parameter,
    ).await?;
    if rows.len() != members.len() { return Err(protocol("fanout progress does not cover its roster")); }
    let mut replicas = Vec::with_capacity(members.len());
    let mut minimum = current.produced.sequence;
    for (row, expected_id) in rows.iter().zip(members) {
        let replica_id = hash_column(row, 0)?;
        let position = ReplicaCheckpoint { stream_id, sequence: unsigned(row, 1)?, tip: hash_column(row, 2)? };
        if replica_id != expected_id || position.sequence < current.acknowledged.sequence
            || position.sequence > current.produced.sequence
        {
            return Err(protocol("fanout progress identity or position mismatch"));
        }
        let expected_tip = if position.sequence == current.acknowledged.sequence {
            current.acknowledged.tip
        } else {
            let queued = conn.query_with_params(
                "SELECT tip FROM main._fsqlite_source_outbox_v1 WHERE stream_id=?1 AND sequence=?2 \
                 AND typeof(tip)='blob' AND length(tip)=32 LIMIT 2",
                &[blob(stream_id), integer(position.sequence)?],
            ).await?;
            let [queued] = queued.as_slice() else { return Err(protocol("fanout progress references missing history")); };
            hash_column(queued, 0)?
        };
        if position.tip != expected_tip { return Err(protocol("fanout progress has a conflicting tip")); }
        minimum = minimum.min(position.sequence);
        replicas.push(ReplicaProgress { replica_id, checkpoint: position });
    }
    if minimum != current.acknowledged.sequence {
        return Err(protocol("fanout retention frontier is inconsistent"));
    }
    Ok(Some(FanoutState { outbox: current, baseline, replicas }))
}

fn member_index(state: &FanoutState, replica_id: PayloadHash) -> Result<usize> {
    state.replicas.binary_search_by(|entry| entry.replica_id.as_bytes().cmp(replica_id.as_bytes()))
        .map_err(|_| protocol("replica is not in the required fanout roster"))
}

async fn load(conn: &Connection, stream_id: PayloadHash) -> Result<FanoutState> {
    validate_schema(conn).await?;
    let current = load_state(conn, stream_id).await?;
    snapshot(conn, current).await?.ok_or_else(|| protocol("source stream has no fanout roster"))
}

/// Seal the required roster at an already initialized, empty source position.
///
/// The caller must provision EVERY recipient from the same coherent baseline.
/// No old queued work may exist when membership is established. Repeating the
/// same roster and original baseline is idempotent even after progress; it
/// never rewinds confirmations. A different roster/baseline is always refused.
/// This transaction does not initialize a replica or verify its remote state.
pub async fn configure(
    conn: &mut Connection, cx: &Cx, baseline: ReplicaCheckpoint, members: &[PayloadHash],
) -> Result<FanoutState> {
    checkpoint(cx)?;
    let _ = integer(baseline.sequence)?;
    let members = roster(members)?;
    let owner = begin(conn).await?;
    let result = async {
        validate_schema(conn).await?;
        let current = load_state(conn, baseline.stream_id).await?;
        // Detect an existing partial schema before CREATE IF NOT EXISTS can
        // mask evidence of a missing membership table.
        let present = tables_present(conn).await?;
        if let Some(existing) = snapshot(conn, current).await? {
            if existing.baseline != baseline
                || !existing.replicas.iter().map(|entry| entry.replica_id).eq(members.iter().copied())
            {
                return Err(protocol("fanout roster or baseline cannot be replaced"));
            }
            return Ok(existing);
        }
        if current.produced != baseline || current.acknowledged != baseline || current.pending_bytes != 0 {
            return Err(protocol("fanout configuration requires an empty matching baseline"));
        }
        if !present {
            conn.execute(CREATE_GROUP).await?;
            conn.execute(CREATE_REPLICA).await?;
        }
        let mut encoded = Vec::with_capacity(members.len() * 32);
        for member in &members { encoded.extend_from_slice(member.as_bytes()); }
        let changed = conn.execute_with_params(
            "INSERT OR ABORT INTO main._fsqlite_source_fanout_v1(stream_id,start_sequence,start_tip,members) VALUES(?1,?2,?3,?4)",
            &[blob(baseline.stream_id), integer(baseline.sequence)?, blob(baseline.tip), SqliteValue::Blob(encoded.into())],
        ).await?;
        if changed != 1 { return Err(protocol("fanout roster insertion was not exact")); }
        for member in &members {
            checkpoint(cx)?;
            let changed = conn.execute_with_params(
                "INSERT OR ABORT INTO main._fsqlite_source_replica_v1(stream_id,replica_id,sequence,tip) VALUES(?1,?2,?3,?4)",
                &[blob(baseline.stream_id), blob(*member), integer(baseline.sequence)?, blob(baseline.tip)],
            ).await?;
            if changed != 1 { return Err(protocol("fanout progress insertion was not exact")); }
        }
        // Join the existing source-row write/validation surface. A competing
        // record/configuration remains subject to ordinary FCW/SSI validation;
        // no global or per-connection writer gate is introduced.
        save_state(conn, current, current).await?;
        load(conn, baseline.stream_id).await
    }.await;
    complete(owner, cx, result).await
}

/// Inspect committed source and recipient progress, including after lost ACKs.
pub async fn state(conn: &mut Connection, cx: &Cx, stream_id: PayloadHash) -> Result<FanoutState> {
    checkpoint(cx)?;
    let owner = begin(conn).await?;
    let result = load(conn, stream_id).await;
    complete(owner, cx, result).await
}

/// Fetch one recipient's next message, without changing any delivery progress.
/// Fast replicas may continue while slower replicas retain earlier bodies.
/// A caller-owned transaction is refused; no SQL transaction spans transport.
pub async fn next_pending(
    conn: &mut Connection, cx: &Cx, stream_id: PayloadHash,
    replica_id: PayloadHash, max_message_bytes: u64,
) -> Result<Option<OutboxMessage>> {
    checkpoint(cx)?;
    let owner = begin(conn).await?;
    let result = async {
        let group = load(conn, stream_id).await?;
        let position = group.replicas[member_index(&group, replica_id)?].checkpoint;
        read_after(conn, cx, group.outbox, position, max_message_bytes).await
    }.await;
    complete(owner, cx, result).await
}

/// Confirm one member's exact next commit and reclaim only the all-member head.
///
/// Authenticate the member and committed checkpoint outside this API. No send
/// result is treated as an ACK. Current exact retries are idempotent; stale,
/// skipped and conflicting checkpoints never advance any member. Each call
/// advances one member by at most one message, so the minimum frontier also
/// advances by at most one and reclamation never needs an unbounded loop.
/// Member progress, body deletion, source accounting and frontier share ONE
/// transaction. Failed/dropped/uncertain commits use the existing cleanup and
/// reconciliation protocol; do not turn an I/O error into a success receipt.
pub async fn acknowledge(
    conn: &mut Connection, cx: &Cx, replica_id: PayloadHash,
    confirmed: ReplicaCheckpoint, max_message_bytes: u64,
) -> Result<FanoutState> {
    checkpoint(cx)?;
    let _ = integer(confirmed.sequence)?;
    let owner = begin(conn).await?;
    let result = async {
        let mut group = load(conn, confirmed.stream_id).await?;
        let index = member_index(&group, replica_id)?;
        let previous = group.replicas[index].checkpoint;
        if confirmed.sequence < previous.sequence {
            return Err(ReplicaApplyError::Stale { current: previous.sequence, received: confirmed.sequence });
        }
        if confirmed.sequence == previous.sequence {
            if confirmed != previous { return Err(ReplicaApplyError::Diverged { sequence: confirmed.sequence }); }
            return Ok(group);
        }
        let expected = previous.sequence + 1;
        if confirmed.sequence != expected {
            return Err(ReplicaApplyError::Gap { expected, received: confirmed.sequence });
        }
        let message = read_after(conn, cx, group.outbox, previous, max_message_bytes).await?
            .ok_or_else(|| protocol("fanout acknowledgement has no queued message"))?;
        if message.envelope().id() != confirmed.tip {
            return Err(ReplicaApplyError::Diverged { sequence: confirmed.sequence });
        }
        checkpoint(cx)?;
        let changed = conn.execute_with_params(
            "UPDATE OR ABORT main._fsqlite_source_replica_v1 SET sequence=?1,tip=?2 \
             WHERE stream_id=?3 AND replica_id=?4 AND sequence=?5 AND tip=?6",
            &[integer(confirmed.sequence)?, blob(confirmed.tip), blob(confirmed.stream_id),
                blob(replica_id), integer(previous.sequence)?, blob(previous.tip)],
        ).await?;
        if changed != 1 { return Err(protocol("fanout confirmation did not update exactly one member")); }
        group.replicas[index].checkpoint = confirmed;
        let minimum = group.replicas.iter().map(|entry| entry.checkpoint.sequence)
            .min().ok_or_else(|| protocol("fanout roster became empty"))?;
        let current = group.outbox;
        if minimum != current.acknowledged.sequence {
            if minimum != current.acknowledged.sequence + 1 || confirmed.sequence != minimum {
                return Err(protocol("fanout frontier did not advance by one"));
            }
            let length = u64::try_from(message.body().len()).map_err(|_| FrankenError::TooBig)?;
            let changed = conn.execute_with_params(
                "DELETE FROM main._fsqlite_source_outbox_v1 WHERE stream_id=?1 AND sequence=?2 AND tip=?3",
                &[blob(confirmed.stream_id), integer(confirmed.sequence)?, blob(confirmed.tip)],
            ).await?;
            if changed != 1 { return Err(protocol("fanout reclamation did not remove exactly one body")); }
            group.outbox.acknowledged = confirmed;
            group.outbox.pending_bytes = current.pending_bytes.checked_sub(length)
                .ok_or_else(|| protocol("fanout byte accounting underflow"))?;
        }
        save_state(conn, current, group.outbox).await?;
        if load(conn, confirmed.stream_id).await? != group {
            return Err(protocol("fanout state changed during acknowledgement"));
        }
        Ok(group)
    }.await;
    complete(owner, cx, result).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{Bytes, OutboxLimits, record};
    use crate::compat::changeset::streaming::ordered;
    use crate::compat::changeset_stream::ChangesetStreamLimits;
    use fsqlite_ext_session::{
        ChangeOp, Changeset, ChangesetKind, ChangesetRow, ChangesetValue, TableChangeset, TableInfo,
    };

    const fn hash(byte: u8) -> PayloadHash { PayloadHash::from_bytes([byte; 32]) }
    const fn baseline() -> ReplicaCheckpoint {
        ReplicaCheckpoint { stream_id: hash(1), sequence: 0, tip: hash(2) }
    }

    fn wire(id: i64) -> Vec<u8> {
        Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo { name: "t".to_owned(), column_count: 2, pk_flags: vec![true, false] },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Insert, indirect: false, old_values: Vec::new(),
                    new_values: vec![ChangesetValue::Integer(id), ChangesetValue::Text(format!("row-{id}"))],
                }],
            }],
        }.encode()
    }

    async fn database(path: &str) -> Connection {
        let conn = Connection::open(path).await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); \
            CREATE TABLE audit(id INTEGER PRIMARY KEY); \
            CREATE TRIGGER audit_t AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END;")
            .await.unwrap();
        conn
    }

    async fn source(path: &str) -> Connection {
        let mut conn = database(path).await;
        super::super::initialize(&mut conn, &Cx::new(), baseline()).await.unwrap();
        conn
    }

    async fn count(conn: &Connection, name: &str) -> i64 {
        let row = conn.query_row(&format!("SELECT count(*) FROM {name}")).await.unwrap();
        let Some(SqliteValue::Integer(value)) = row.get(0) else { panic!("integer count"); };
        *value
    }

    fn position(commit: &super::super::OutboxCommit) -> ReplicaCheckpoint {
        ReplicaCheckpoint {
            stream_id: commit.envelope.stream_id(), sequence: commit.envelope.sequence(), tip: commit.envelope.id(),
        }
    }

    async fn produce(conn: &mut Connection, previous: ReplicaCheckpoint, id: i64) -> ReplicaCheckpoint {
        position(&record(conn, &Cx::new(), previous, &wire(id), OutboxLimits::default()).await.unwrap())
    }

    #[test]
    fn fanout_roster_accepts_the_exact_limit_and_sorts_all_byte_identities() {
        let members: Vec<_> = (0_u8..=u8::MAX).rev().map(hash).collect();
        let sorted = roster(&members).unwrap();
        assert_eq!(sorted.len(), MAX_REPLICAS);
        assert_eq!(sorted.first(), Some(&hash(0)));
        assert_eq!(sorted.last(), Some(&hash(255)));
        assert_eq!(roster(&[hash(1)]).unwrap(), vec![hash(1)]);
        let mut duplicate = members;
        duplicate[0] = duplicate[1];
        assert!(roster(&duplicate).is_err());
    }

    #[test]
    fn fanout_fast_replicas_progress_without_reclaiming_slow_replica_history() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut source = source(":memory:").await;
            configure(&mut source, &cx, baseline(), &[hash(12), hash(10), hash(11)]).await.unwrap();
            let mut tip = baseline();
            for id in 1..=3 { tip = produce(&mut source, tip, id).await; }
            let bytes = state(&mut source, &cx, hash(1)).await.unwrap().outbox.pending_bytes;
            for (member_index, member) in [hash(10), hash(11), hash(12)].into_iter().enumerate() {
                let mut replica = database(":memory:").await;
                ordered::initialize(&mut replica, &cx, baseline()).await.unwrap();
                for sequence in 1..=3_u64 {
                    let message = next_pending(&mut source, &cx, hash(1), member, 1 << 20).await.unwrap().unwrap();
                    assert_eq!(message.envelope().sequence(), sequence);
                    let receipt = ordered::apply(
                        &mut replica, &cx, &mut Bytes(message.body()), message.envelope(),
                        message.envelope().id(), ChangesetStreamLimits::default(),
                    ).await.unwrap();
                    let progress = acknowledge(&mut source, &cx, member, receipt.checkpoint, 1 << 20).await.unwrap();
                    assert_eq!(progress.outbox.acknowledged.sequence, if member_index == 2 { sequence } else { 0 });
                    assert_eq!(progress.outbox.pending_messages(), if member_index == 2 { 3 - sequence } else { 3 });
                    if member_index < 2 { assert_eq!(progress.outbox.pending_bytes, bytes); }
                }
                assert!(next_pending(&mut source, &cx, hash(1), member, 1 << 20).await.unwrap().is_none());
                assert_eq!(count(&replica, "t").await, 3);
                assert_eq!(count(&replica, "audit").await, 3);
                replica.close().await.unwrap();
            }
            let final_state = state(&mut source, &cx, hash(1)).await.unwrap();
            assert_eq!(final_state.outbox.produced, tip);
            assert_eq!(final_state.outbox.acknowledged, tip);
            assert_eq!(final_state.outbox.pending_bytes, 0);
            assert_eq!(count(&source, super::super::QUEUE_TABLE).await, 0);
            assert_eq!(count(&source, "audit").await, 3);
            source.close().await.unwrap();
        });
    }

    #[test]
    fn fanout_configuration_is_canonical_fixed_and_never_resets_progress() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut source = source(":memory:").await;
            for invalid in [vec![], vec![hash(10), hash(10)], vec![hash(10); MAX_REPLICAS + 1]] {
                assert!(configure(&mut source, &cx, baseline(), &invalid).await.is_err());
            }
            assert!(!tables_present(&source).await.unwrap());
            let first = configure(&mut source, &cx, baseline(), &[hash(11), hash(10)]).await.unwrap();
            assert_eq!(first.replicas[0].replica_id, hash(10));
            assert_eq!(configure(&mut source, &cx, baseline(), &[hash(10), hash(11)]).await.unwrap(), first);
            let committed = produce(&mut source, baseline(), 1).await;
            let advanced = acknowledge(&mut source, &cx, hash(10), committed, 1 << 20).await.unwrap();
            assert_eq!(configure(&mut source, &cx, baseline(), &[hash(11), hash(10)]).await.unwrap(), advanced);
            for changed in [vec![hash(10)], vec![hash(10), hash(12)]] {
                assert!(configure(&mut source, &cx, baseline(), &changed).await.is_err());
            }
            assert!(configure(&mut source, &cx, committed, &[hash(10), hash(11)]).await.is_err());
            assert_eq!(state(&mut source, &cx, hash(1)).await.unwrap(), advanced);
            assert!(next_pending(&mut source, &cx, hash(1), hash(99), 1 << 20).await.is_err());
            source.close().await.unwrap();
        });
    }

    #[test]
    fn fanout_confirmation_checks_identity_order_and_cannot_bypass_retention() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut source = source(":memory:").await;
            configure(&mut source, &cx, baseline(), &[hash(10), hash(11)]).await.unwrap();
            let first = produce(&mut source, baseline(), 1).await;
            let second = produce(&mut source, first, 2).await;
            let before = state(&mut source, &cx, hash(1)).await.unwrap();
            assert!(super::super::acknowledge(&mut source, &cx, first, 1 << 20).await.is_err());
            assert!(acknowledge(&mut source, &cx, hash(99), first, 1 << 20).await.is_err());
            assert!(matches!(acknowledge(&mut source, &cx, hash(10), second, 1 << 20).await,
                Err(ReplicaApplyError::Gap { expected: 1, received: 2 })));
            let forged = ReplicaCheckpoint { tip: hash(99), ..first };
            assert!(matches!(acknowledge(&mut source, &cx, hash(10), forged, 1 << 20).await,
                Err(ReplicaApplyError::Diverged { sequence: 1 })));
            let cancelled = Cx::new();
            cancelled.cancel();
            assert!(acknowledge(&mut source, &cancelled, hash(10), first, 1 << 20).await.is_err());
            assert_eq!(state(&mut source, &cx, hash(1)).await.unwrap(), before);
            let fast = acknowledge(&mut source, &cx, hash(10), first, 1 << 20).await.unwrap();
            assert_eq!(fast.outbox, before.outbox);
            assert_eq!(acknowledge(&mut source, &cx, hash(10), first, 0).await.unwrap(), fast);
            let released = acknowledge(&mut source, &cx, hash(11), first, 1 << 20).await.unwrap();
            assert_eq!(released.outbox.pending_messages(), 1);
            assert_eq!(released.outbox.acknowledged, first);
            assert_eq!(acknowledge(&mut source, &cx, hash(11), first, 0).await.unwrap(), released);
            assert!(matches!(acknowledge(&mut source, &cx, hash(11), baseline(), 0).await,
                Err(ReplicaApplyError::Stale { current: 1, received: 0 })));
            source.close().await.unwrap();
        });
    }

    #[test]
    fn fanout_missing_member_is_corruption_not_a_smaller_required_roster() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut source = source(":memory:").await;
            configure(&mut source, &cx, baseline(), &[hash(10), hash(11)]).await.unwrap();
            let committed = produce(&mut source, baseline(), 1).await;
            acknowledge(&mut source, &cx, hash(10), committed, 1 << 20).await.unwrap();
            source.execute_with_params(
                "DELETE FROM main._fsqlite_source_replica_v1 WHERE stream_id=?1 AND replica_id=?2",
                &[blob(hash(1)), blob(hash(11))],
            ).await.unwrap();
            assert!(state(&mut source, &cx, hash(1)).await.is_err());
            assert!(super::super::state(&mut source, &cx, hash(1)).await.is_err());
            assert!(acknowledge(&mut source, &cx, hash(10), committed, 1 << 20).await.is_err());
            assert!(super::super::acknowledge(&mut source, &cx, committed, 1 << 20).await.is_err());
            assert!(record(&mut source, &cx, committed, &wire(2), OutboxLimits::default()).await.is_err());
            assert_eq!(count(&source, super::super::QUEUE_TABLE).await, 1);
            assert_eq!(count(&source, "t").await, 1);
            assert!(!source.in_transaction());
            source.close().await.unwrap();
        });
    }

    #[test]
    fn fanout_reopen_recovers_independent_positions_and_lost_remote_ack() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let directory = tempfile::tempdir().unwrap().keep();
            let source_path = directory.join("source.db");
            let replica_path = directory.join("slow.db");
            let mut source = source(source_path.to_str().unwrap()).await;
            let mut slow = database(replica_path.to_str().unwrap()).await;
            let mut fast = database(":memory:").await;
            for target in [&mut fast, &mut slow] {
                ordered::initialize(target, &cx, baseline()).await.unwrap();
            }
            configure(&mut source, &cx, baseline(), &[hash(10), hash(11)]).await.unwrap();
            let first = produce(&mut source, baseline(), 1).await;
            let second = produce(&mut source, first, 2).await;
            for _ in 0..2 {
                let message = next_pending(&mut source, &cx, hash(1), hash(10), 1 << 20).await.unwrap().unwrap();
                let receipt = ordered::apply(&mut fast, &cx, &mut Bytes(message.body()), message.envelope(),
                    message.envelope().id(), ChangesetStreamLimits::default()).await.unwrap();
                acknowledge(&mut source, &cx, hash(10), receipt.checkpoint, 1 << 20).await.unwrap();
            }
            fast.close().await.unwrap();
            let message = next_pending(&mut source, &cx, hash(1), hash(11), 1 << 20).await.unwrap().unwrap();
            let lost = ordered::apply(&mut slow, &cx, &mut Bytes(message.body()), message.envelope(),
                message.envelope().id(), ChangesetStreamLimits::default()).await.unwrap();
            assert_eq!(lost.checkpoint, first);
            // Lose the remote response before source acknowledgement. Both
            // sides reopen; the fast member must not rewind to the slow one.
            source.close().await.unwrap();
            slow.close().await.unwrap();
            let mut source = Connection::open(source_path.to_str().unwrap()).await.unwrap();
            let mut slow = Connection::open(replica_path.to_str().unwrap()).await.unwrap();
            assert!(next_pending(&mut source, &cx, hash(1), hash(10), 0).await.unwrap().is_none());
            let pending = next_pending(&mut source, &cx, hash(1), hash(11), 1 << 20).await.unwrap().unwrap();
            assert_eq!(pending.envelope().sequence(), 1);
            let duplicate = ordered::apply(&mut slow, &cx, &mut Bytes(pending.body()), pending.envelope(),
                pending.envelope().id(), ChangesetStreamLimits::default()).await.unwrap();
            assert_eq!(duplicate.disposition, ordered::ReplicaDisposition::AlreadyApplied);
            let saved = acknowledge(&mut source, &cx, hash(11), duplicate.checkpoint, 1 << 20).await.unwrap();
            assert_eq!(saved.outbox.acknowledged, first);
            source.close().await.unwrap();
            let mut source = Connection::open(source_path.to_str().unwrap()).await.unwrap();
            assert_eq!(acknowledge(&mut source, &cx, hash(11), first, 0).await.unwrap(), saved);
            let pending = next_pending(&mut source, &cx, hash(1), hash(11), 1 << 20).await.unwrap().unwrap();
            assert_eq!(pending.envelope().sequence(), 2);
            let receipt = ordered::apply(&mut slow, &cx, &mut Bytes(pending.body()), pending.envelope(),
                pending.envelope().id(), ChangesetStreamLimits::default()).await.unwrap();
            let empty = acknowledge(&mut source, &cx, hash(11), receipt.checkpoint, 1 << 20).await.unwrap();
            assert_eq!(empty.outbox.acknowledged, second);
            assert_eq!(empty.outbox.pending_bytes, 0);
            assert_eq!(count(&source, super::super::QUEUE_TABLE).await, 0);
            assert_eq!(count(&slow, "audit").await, 2);
            assert_eq!(slow.query_row("PRAGMA integrity_check").await.unwrap().get(0),
                Some(&SqliteValue::Text("ok".into())));
            source.close().await.unwrap();
            slow.close().await.unwrap();
        });
    }

    #[test]
    fn fanout_capacity_empty_messages_and_caller_transactions_preserve_work() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut source = source(":memory:").await;
            configure(&mut source, &cx, baseline(), &[hash(10), hash(11)]).await.unwrap();
            let limits = OutboxLimits { max_pending_messages: 1, max_pending_bytes: 0, ..OutboxLimits::default() };
            let first = position(&record(&mut source, &cx, baseline(), &[], limits).await.unwrap());
            acknowledge(&mut source, &cx, hash(10), first, 0).await.unwrap();
            assert!(matches!(record(&mut source, &cx, first, &[], limits).await,
                Err(ReplicaApplyError::Database(FrankenError::TooBig))));
            let cleared = acknowledge(&mut source, &cx, hash(11), first, 0).await.unwrap();
            assert_eq!(cleared.outbox.pending_messages(), 0);
            let second = position(&record(&mut source, &cx, first, &[], limits).await.unwrap());
            let saved = state(&mut source, &cx, hash(1)).await.unwrap();
            assert_eq!(saved.outbox.pending_messages(), 1);
            source.execute("BEGIN; INSERT INTO t VALUES(7,'caller');").await.unwrap();
            assert!(matches!(acknowledge(&mut source, &cx, hash(10), second, 0).await,
                Err(ReplicaApplyError::Database(FrankenError::NestedTransaction))));
            assert!(matches!(state(&mut source, &cx, hash(1)).await,
                Err(ReplicaApplyError::Database(FrankenError::NestedTransaction))));
            assert!(matches!(configure(&mut source, &cx, baseline(), &[hash(10),hash(11)]).await,
                Err(ReplicaApplyError::Database(FrankenError::NestedTransaction))));
            assert!(source.in_transaction());
            assert_eq!(count(&source, "t").await, 1);
            source.execute("ROLLBACK").await.unwrap();
            assert_eq!(state(&mut source, &cx, hash(1)).await.unwrap(), saved);
            source.close().await.unwrap();
        });
    }

    #[test]
    fn fanout_source_cannot_replace_even_a_structurally_valid_roster() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut source = source(":memory:").await;
            let saved = configure(&mut source, &cx, baseline(), &[hash(10),hash(11)]).await.unwrap();
            for name in [GROUP_TABLE, REPLICA_TABLE] {
                let body = Changeset {
                    kind: ChangesetKind::Changeset,
                    tables: vec![TableChangeset {
                        info: TableInfo { name: name.to_ascii_uppercase(), column_count: 1, pk_flags: vec![true] },
                        rows: vec![ChangesetRow { op: ChangeOp::Insert, indirect: false, old_values: Vec::new(),
                            new_values: vec![ChangesetValue::Integer(1)] }],
                    }],
                }.encode();
                assert!(matches!(record(&mut source, &cx, baseline(), &body, OutboxLimits::default()).await,
                    Err(ReplicaApplyError::Protocol { detail: "source message targets replication metadata" })));
            }
            // Rewrite BOTH roster and matching progress rows. The altered
            // metadata would pass structural validation; equality with the
            // pre-DML snapshot must nevertheless reject this implicit change.
            source.execute(&format!("CREATE TRIGGER replace_member AFTER INSERT ON t BEGIN \
                UPDATE _fsqlite_source_replica_v1 SET replica_id=x'{}' WHERE replica_id=x'{}'; \
                UPDATE _fsqlite_source_fanout_v1 SET members=x'{}{}'; END;",
                "0c".repeat(32), "0b".repeat(32), "0a".repeat(32), "0c".repeat(32)))
                .await.unwrap();
            assert!(matches!(record(&mut source, &cx, baseline(), &wire(1), OutboxLimits::default()).await,
                Err(ReplicaApplyError::Protocol { detail: "source metadata changed during row application" })));
            assert_eq!(state(&mut source, &cx, hash(1)).await.unwrap(), saved);
            assert_eq!(count(&source, "t").await, 0);
            assert_eq!(count(&source, "audit").await, 0);
            assert_eq!(count(&source, super::super::QUEUE_TABLE).await, 0);
            source.close().await.unwrap();
        });
    }

    #[test]
    fn fanout_corrupt_payload_roster_and_metadata_triggers_never_discard_work() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            for tamper in [
                "UPDATE _fsqlite_source_fanout_v1 SET members=zeroblob(8193)",
                "UPDATE _fsqlite_source_fanout_v1 SET start_sequence=zeroblob(4096)",
                "UPDATE _fsqlite_source_replica_v1 SET tip=zeroblob(4096)",
                "UPDATE _fsqlite_source_replica_v1 SET sequence=zeroblob(4096)",
                "UPDATE _fsqlite_source_outbox_v1 SET body=zeroblob(length(body))",
            ] {
                let mut source = source(":memory:").await;
                configure(&mut source, &cx, baseline(), &[hash(10),hash(11)]).await.unwrap();
                let first = produce(&mut source, baseline(), 1).await;
                assert!(matches!(next_pending(&mut source, &cx, hash(1), hash(10), 0).await,
                    Err(ReplicaApplyError::Database(FrankenError::TooBig))));
                source.execute(tamper).await.unwrap();
                assert!(next_pending(&mut source, &cx, hash(1), hash(10), 1 << 20).await.is_err());
                assert!(acknowledge(&mut source, &cx, hash(10), first, 1 << 20).await.is_err());
                assert_eq!(count(&source, super::super::QUEUE_TABLE).await, 1);
                assert_eq!(source.query_row("SELECT ack_sequence FROM _fsqlite_source_stream_v1")
                    .await.unwrap().get(0), Some(&SqliteValue::Integer(0)));
                assert!(!source.in_transaction());
                source.close().await.unwrap();
            }
            for temporary in [false, true] {
                let mut source = source(":memory:").await;
                configure(&mut source, &cx, baseline(), &[hash(10),hash(11)]).await.unwrap();
                let first = produce(&mut source, baseline(), 1).await;
                let prefix = if temporary { "TEMP " } else { "" };
                source.execute(&format!("CREATE {prefix}TRIGGER ignore_ack BEFORE UPDATE ON main._fsqlite_source_replica_v1 \
                    BEGIN SELECT RAISE(IGNORE); END;")).await.unwrap();
                assert!(matches!(acknowledge(&mut source, &cx, hash(10), first, 1 << 20).await,
                    Err(ReplicaApplyError::Protocol { detail: "outbox metadata must not have triggers" })));
                assert_eq!(count(&source, super::super::QUEUE_TABLE).await, 1);
                source.close().await.unwrap();
            }
        });
    }

    #[test]
    fn fanout_other_streams_remain_independent_and_backlog_cannot_be_adopted() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut source = source(":memory:").await;
            configure(&mut source, &cx, baseline(), &[hash(10),hash(11)]).await.unwrap();
            let first = produce(&mut source, baseline(), 1).await;
            let saved = state(&mut source, &cx, hash(1)).await.unwrap();
            let other = ReplicaCheckpoint { stream_id: hash(20), sequence: 0, tip: hash(21) };
            super::super::initialize(&mut source, &cx, other).await.unwrap();
            let unrelated = produce(&mut source, other, 9).await;
            assert!(configure(&mut source, &cx, other, &[hash(10)]).await.is_err());
            let released = super::super::acknowledge(&mut source, &cx, unrelated, 1 << 20).await.unwrap();
            assert_eq!(released.pending_messages(), 0);
            assert_eq!(state(&mut source, &cx, hash(1)).await.unwrap(), saved);
            assert!(super::super::acknowledge(&mut source, &cx, first, 1 << 20).await.is_err());
            assert_eq!(count(&source, super::super::QUEUE_TABLE).await, 1);
            assert!(next_pending(&mut source, &cx, hash(20), hash(10), 1 << 20).await.is_err());
            source.close().await.unwrap();
        });
    }
}
