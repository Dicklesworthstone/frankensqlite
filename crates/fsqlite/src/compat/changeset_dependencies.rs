//! Bounded dependency retries for buffered Session application.
//!
//! A changeset records final row effects, not the SQL statement order that
//! produced them. An UPDATE/INSERT may therefore need a UNIQUE key released
//! by a later row in the same table section. Retry those rejected attempts
//! only after their row savepoints have been fully rolled back. No constraint
//! is disabled and no unrelated row is deleted to make a dependency pass.
//!
//! This module uses the ordinary SQL applier, not a second DML implementation.
//! Input rows are borrowed; the retry queue stores only original row indices.
//! Streaming application retains its separate one-row memory contract and
//! does not use this buffered retry policy.

use super::{
    ApplyResult, ApplyTransaction, Changeset, ChangesetRow, ConflictAction, ConflictType,
    Connection, FrankenError, RowOutcome, SqlChangesetApplyReport, SqlChangesetConflict,
    TableChangeset, TablePlan, apply_row, validate,
};

/// Resource admission for automatic retries, independent of the caller-owned
/// changeset payload. A zero limit permits unblocked work, but refuses work
/// requiring that resource. Exhaustion fails the whole owning transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryLimits {
    /// Maximum pending row indices within one table section.
    pub max_deferred_rows: usize,
    /// Maximum replayed row attempts across the entire apply, including the
    /// final conflict-resolution pass. Initial attempts do not consume this.
    pub max_retry_attempts: usize,
}

impl Default for RetryLimits {
    fn default() -> Self {
        Self {
            max_deferred_rows: 4096,
            max_retry_attempts: 65_536,
        }
    }
}

fn account(report: &mut SqlChangesetApplyReport, outcome: RowOutcome) -> ApplyResult<()> {
    let increment = |counter: &mut usize, n: usize| -> ApplyResult<()> {
        *counter = counter.checked_add(n).ok_or_else(|| {
            FrankenError::internal("changeset application count overflow")
        })?;
        Ok(())
    };
    match outcome {
        RowOutcome::Applied { replaced } => {
            increment(&mut report.applied, 1)?;
            increment(&mut report.replaced, usize::from(replaced))
        }
        RowOutcome::Skipped => increment(&mut report.skipped, 1),
    }
}

/// Only typed uniqueness failures are dependency candidates. NOT NULL, CHECK,
/// FK, trigger RAISE, resource, I/O and cancellation failures keep their
/// existing semantics. In particular, a failed rollback never reaches here.
fn is_unique_dependency(conflict: &SqlChangesetConflict<'_>) -> bool {
    conflict.kind == ConflictType::Constraint
        && matches!(
            conflict.error,
            Some(FrankenError::UniqueViolation { .. } | FrankenError::PrimaryKeyViolation)
        )
}

async fn try_row<F>(
    conn: &Connection,
    plan: &TablePlan,
    index: usize,
    change: &ChangesetRow,
    handler: &mut F,
) -> ApplyResult<Option<RowOutcome>>
where
    F: FnMut(SqlChangesetConflict<'_>) -> ConflictAction,
{
    let mut deferred = false;
    let mut handler_called = false;
    let outcome = apply_row(conn, plan, index, change, &mut |conflict| {
        // apply_row has restored the row savepoint before this CONSTRAINT
        // callback. Internally omitting this attempt is NOT a final omission:
        // its original index is retained and no report count is advanced.
        // Once an application handler has resolved DATA/PK conflict, preserve
        // its existing follow-up semantics rather than replaying its decision.
        if !handler_called && is_unique_dependency(&conflict) {
            deferred = true;
            ConflictAction::OmitChange
        } else {
            handler_called = true;
            handler(conflict)
        }
    })
    .await?;
    match (deferred, outcome) {
        (true, RowOutcome::Skipped) => Ok(None),
        (false, outcome) => Ok(Some(outcome)),
        (true, RowOutcome::Applied { .. }) => Err(FrankenError::internal(
            "deferred changeset attempt unexpectedly applied a row",
        )
        .into()),
    }
}

async fn apply_table<F>(
    conn: &Connection,
    table: &TableChangeset,
    plan: &TablePlan,
    handler: &mut F,
    report: &mut SqlChangesetApplyReport,
    limits: RetryLimits,
    attempts_left: &mut usize,
) -> ApplyResult<()>
where
    F: FnMut(SqlChangesetConflict<'_>) -> ConflictAction,
{
    let mut pending = Vec::new();
    for (index, change) in table.rows.iter().enumerate() {
        if let Some(outcome) = try_row(conn, plan, index, change, handler).await? {
            account(report, outcome)?;
        } else {
            if pending.len() >= limits.max_deferred_rows {
                return Err(FrankenError::TooBig.into());
            }
            pending
                .try_reserve(1)
                .map_err(|_| FrankenError::OutOfMemory)?;
            pending.push(index);
        }
    }

    let mut resolve = false;
    while !pending.is_empty() {
        let before = pending.len();
        let mut retained = 0;
        for position in 0..before {
            *attempts_left = attempts_left
                .checked_sub(1)
                .ok_or(FrankenError::TooBig)?;
            let index = pending[position];
            let change = &table.rows[index];
            let outcome = if resolve {
                Some(apply_row(conn, plan, index, change, handler).await?)
            } else {
                try_row(conn, plan, index, change, handler).await?
            };
            if let Some(outcome) = outcome {
                account(report, outcome)?;
            } else {
                // Compact indices in place; never clone BLOB/TEXT retry rows.
                pending[retained] = index;
                retained += 1;
            }
        }
        pending.truncate(retained);
        // A productive pass strictly shrinks the queue. A stalled pass is
        // followed by exactly one ordinary conflict-resolution pass, not an
        // unbounded retry loop or implicit REPLACE policy.
        resolve = retained == before;
    }
    Ok(())
}

/// Apply buffered changes with explicit dependency-retry admission.
///
/// DATA, NOTFOUND and primary-key CONFLICT callbacks retain their ordinary
/// ordering. Unresolved secondary uniqueness failures reach the handler after
/// the table section has made all available progress, with their ORIGINAL row
/// indices and a fresh view of the target. Handler-resolved replacements are
/// not automatically replayed. SQL trigger effects of rejected attempts are
/// rolled back; external effects of SQL functions cannot be undone.
///
/// All table sections, retries and counts share one transaction. Exhaustion,
/// conflict abort, cancellation or a failed row cleanup aborts that transaction
/// using the existing owner. Only successful COMMIT returns a report. An I/O
/// failure at COMMIT retains the engine's uncertain-outcome semantics.
pub async fn apply_with_limits<F>(
    conn: &mut Connection,
    changeset: &Changeset,
    limits: RetryLimits,
    mut handler: F,
) -> ApplyResult<SqlChangesetApplyReport>
where
    F: FnMut(SqlChangesetConflict<'_>) -> ConflictAction,
{
    validate(changeset)?;
    if conn.in_transaction() {
        return Err(FrankenError::NestedTransaction.into());
    }
    if changeset.tables.iter().all(|table| table.rows.is_empty()) {
        return Ok(SqlChangesetApplyReport::default());
    }
    let mut transaction = ApplyTransaction { conn, armed: true };
    if let Err(error) = conn.begin_transaction().await {
        return Err(transaction.rollback(error.into()).await);
    }
    let result = async {
        let mut plans = Vec::new();
        for table in changeset.tables.iter().filter(|table| !table.rows.is_empty()) {
            plans.push((table, TablePlan::load(conn, table).await?));
        }
        let mut report = SqlChangesetApplyReport::default();
        let mut attempts_left = limits.max_retry_attempts;
        for (table, plan) in plans {
            apply_table(
                conn,
                table,
                &plan,
                &mut handler,
                &mut report,
                limits,
                &mut attempts_left,
            )
            .await?;
        }
        Ok(report)
    }
    .await;
    let report = match result {
        Ok(report) => report,
        Err(error) => return Err(transaction.rollback(error).await),
    };
    if let Err(error) = conn.commit_transaction().await {
        return Err(transaction.rollback(error.into()).await);
    }
    transaction.armed = false;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqliteValue;
    use crate::compat::changeset::{SqlChangesetApplyError, apply_changeset, apply_changeset_with_handler};
    use fsqlite_ext_session::{ChangeOp, ChangesetKind, ChangesetValue, TableInfo};

    fn update(id: i64, old: i64, new: i64) -> ChangesetRow {
        ChangesetRow {
            op: ChangeOp::Update,
            indirect: false,
            old_values: vec![ChangesetValue::Integer(id), ChangesetValue::Integer(old)],
            new_values: vec![ChangesetValue::Undefined, ChangesetValue::Integer(new)],
        }
    }

    fn insert(id: i64, value: i64) -> ChangesetRow {
        ChangesetRow {
            op: ChangeOp::Insert,
            indirect: false,
            old_values: Vec::new(),
            new_values: vec![ChangesetValue::Integer(id), ChangesetValue::Integer(value)],
        }
    }

    fn changes(rows: Vec<ChangesetRow>) -> Changeset {
        Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows,
            }],
        }
    }

    async fn values(conn: &Connection) -> Vec<(i64, i64)> {
        conn.query("SELECT id,v FROM t ORDER BY id")
            .await
            .unwrap()
            .iter()
            .map(|row| match (row.get(0), row.get(1)) {
                (Some(SqliteValue::Integer(id)), Some(SqliteValue::Integer(v))) => (*id, *v),
                _ => panic!("expected integer row"),
            })
            .collect()
    }

    async fn setup(count: i64) -> Connection {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER UNIQUE)")
            .await.unwrap();
        for i in 1..=count {
            conn.execute_with_params("INSERT INTO t VALUES(?1,?2)", &[SqliteValue::Integer(i),SqliteValue::Integer(i)])
                .await.unwrap();
        }
        conn
    }

    #[test]
    fn reverse_unique_dependencies_match_stock_final_rows_without_callbacks() {
        asupersync::test_utils::run_test(|| async {
            for count in [2_i64, 3, 8, 32] {
                for patchset in [false, true] {
                    let mut conn = setup(count).await;
                    let incoming = changes((1..=count).map(|i| update(i, i, i + 1)).collect());
                    let incoming = if patchset {
                        Changeset::decode_patchset(&incoming.encode_patchset()).unwrap()
                    } else { incoming };
                    let mut calls = 0;
                    let report = apply_changeset_with_handler(&mut conn, &incoming, |_| {
                        calls += 1;
                        ConflictAction::Abort
                    }).await.unwrap();
                    assert_eq!(calls, 0, "transient UNIQUE failures are not application conflicts");
                    assert_eq!(report.applied, usize::try_from(count).unwrap());
                    assert_eq!((report.skipped, report.replaced), (0, 0));
                    let stock = rusqlite::Connection::open_in_memory().unwrap();
                    stock.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER UNIQUE)").unwrap();
                    for i in 1..=count { stock.execute("INSERT INTO t VALUES(?1,?2)", [i,i]).unwrap(); }
                    for i in (1..=count).rev() { stock.execute("UPDATE t SET v=?1 WHERE id=?2", [i+1,i]).unwrap(); }
                    let expected: Vec<(i64,i64)> = stock.prepare("SELECT id,v FROM t ORDER BY id").unwrap()
                        .query_map([], |row| Ok((row.get(0)?,row.get(1)?))).unwrap().collect::<Result<_,_>>().unwrap();
                    assert_eq!(values(&conn).await, expected);
                    assert_eq!(conn.query_row("PRAGMA integrity_check").await.unwrap().get(0), Some(&SqliteValue::Text("ok".into())));
                    assert!(!conn.in_transaction());
                }
            }
        });
    }

    #[test]
    fn insert_waits_for_later_delete_without_leaking_failed_trigger_effects() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup(1).await;
            conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY)").await.unwrap();
            conn.execute("CREATE TRIGGER audit_insert BEFORE INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END").await.unwrap();
            let incoming = changes(vec![insert(2,1), ChangesetRow {
                op: ChangeOp::Delete, indirect: false,
                old_values: vec![ChangesetValue::Integer(1),ChangesetValue::Integer(1)],
                new_values: Vec::new(),
            }]);
            assert_eq!(apply_changeset(&mut conn,&incoming).await.unwrap().applied,2);
            assert_eq!(values(&conn).await,vec![(2,1)]);
            let audit = conn.query("SELECT id FROM audit").await.unwrap();
            assert_eq!(audit.len(),1);
            assert_eq!(audit[0].get(0),Some(&SqliteValue::Integer(2)));
        });
    }

    #[test]
    fn terminal_unique_conflict_is_reported_once_with_original_index() {
        asupersync::test_utils::run_test(|| async {
            for omit in [false,true] {
                let mut conn=setup(1).await;
                let incoming=changes(vec![insert(2,2),insert(3,1),insert(4,4)]);
                let mut seen=Vec::new();
                let result=apply_changeset_with_handler(&mut conn,&incoming,|conflict| {
                    seen.push((conflict.kind,conflict.row));
                    if omit { ConflictAction::OmitChange } else { ConflictAction::Abort }
                }).await;
                assert_eq!(seen,vec![(ConflictType::Constraint,1)]);
                if omit {
                    assert_eq!(result.unwrap(),SqlChangesetApplyReport{applied:2,skipped:1,replaced:0});
                    assert_eq!(values(&conn).await,vec![(1,1),(2,2),(4,4)]);
                } else {
                    assert!(matches!(result,Err(SqlChangesetApplyError::Constraint{row:1,..})));
                    assert_eq!(values(&conn).await,vec![(1,1)]);
                }
                assert!(!conn.in_transaction());
            }
        });
    }

    #[test]
    fn retry_resource_exhaustion_rolls_back_all_provisional_progress() {
        asupersync::test_utils::run_test(|| async {
            for limits in [
                RetryLimits{max_deferred_rows:1,max_retry_attempts:100},
                RetryLimits{max_deferred_rows:4,max_retry_attempts:0},
            ] {
                let mut conn=setup(3).await;
                let incoming=changes(vec![update(1,1,2),update(2,2,3),update(3,3,4)]);
                let mut calls=0;
                let result=apply_with_limits(&mut conn,&incoming,limits,|_| {
                    calls+=1;
                    ConflictAction::OmitChange
                }).await;
                assert!(matches!(result,Err(SqlChangesetApplyError::Database(FrankenError::TooBig))));
                assert_eq!(calls,0,"resource admission cannot become an omittable constraint");
                assert_eq!(values(&conn).await,vec![(1,1),(2,2),(3,3)]);
                assert!(!conn.in_transaction());
                assert_eq!(apply_changeset(&mut conn,&incoming).await.unwrap().applied,3);
                assert_eq!(values(&conn).await,vec![(1,2),(2,3),(3,4)]);
            }
        });
    }

    #[test]
    fn primary_key_conflicts_are_not_hidden_by_a_later_delete() {
        asupersync::test_utils::run_test(|| async {
            let mut conn=setup(1).await;
            let incoming=changes(vec![insert(1,2),ChangesetRow {
                op:ChangeOp::Delete,indirect:false,
                old_values:vec![ChangesetValue::Integer(1),ChangesetValue::Integer(1)],new_values:Vec::new(),
            }]);
            let result=apply_changeset(&mut conn,&incoming).await;
            assert!(matches!(result,Err(SqlChangesetApplyError::Conflict{kind:ConflictType::Conflict,row:0,..})));
            assert_eq!(values(&conn).await,vec![(1,1)]);
        });
    }

    #[cfg(feature="native")]
    #[test]
    fn dependency_resolved_rows_and_unique_index_survive_reopen() {
        asupersync::test_utils::run_test(|| async {
            let directory=tempfile::tempdir().unwrap();
            let path=directory.path().join("dependencies.db");
            let mut conn=Connection::open(path.to_str().unwrap()).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER UNIQUE)").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1,1),(2,2),(3,3)").await.unwrap();
            let incoming=changes(vec![update(1,1,2),update(2,2,3),update(3,3,4)]);
            assert_eq!(apply_changeset(&mut conn,&incoming).await.unwrap().applied,3);
            conn.close().await.unwrap();
            let reopened=Connection::open(path.to_str().unwrap()).await.unwrap();
            assert_eq!(values(&reopened).await,vec![(1,2),(2,3),(3,4)]);
            assert_eq!(reopened.query_row("SELECT id FROM t WHERE v=3").await.unwrap().get(0),Some(&SqliteValue::Integer(2)));
            assert_eq!(reopened.query_row("PRAGMA integrity_check").await.unwrap().get(0),Some(&SqliteValue::Text("ok".into())));
            reopened.close().await.unwrap();
            let stock=rusqlite::Connection::open(&path).unwrap();
            assert_eq!(stock.query_row("SELECT id FROM t WHERE v=3",[],|row|row.get::<_,i64>(0)).unwrap(),2);
            assert_eq!(stock.query_row("PRAGMA integrity_check",[],|row|row.get::<_,String>(0)).unwrap(),"ok");
        });
    }
}
