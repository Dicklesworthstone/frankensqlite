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
    TableChangeset, TablePlan, apply_row, quote_identifier, schema, validate,
};
use crate::SqliteValue;

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

/// Foreign-key admission for a complete, buffered logical import.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ForeignKeyPolicy {
    /// Keep the connection's ordinary immediate/deferred constraint policy.
    #[default]
    Preserve,
    /// Require enforcement, reject pre-existing violations, and temporarily
    /// defer eligible checks inside the apply's own transaction. Validate all
    /// attached schemas again before COMMIT. Unresolved references cannot be
    /// omitted by a row handler. RESTRICT and cascade semantics are unchanged.
    DeferChecked,
}

/// Options for the buffered applier. Defaults retain ordinary FK policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyOptions {
    pub retry_limits: RetryLimits,
    pub foreign_keys: ForeignKeyPolicy,
}

fn account(report: &mut SqlChangesetApplyReport, outcome: RowOutcome) -> ApplyResult<()> {
    let increment = |counter: &mut usize, n: usize| -> ApplyResult<()> {
        *counter = counter
            .checked_add(n)
            .ok_or_else(|| FrankenError::internal("changeset application count overflow"))?;
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
            *attempts_left = attempts_left.checked_sub(1).ok_or(FrankenError::TooBig)?;
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

async fn pragma_flag(conn: &Connection, name: &'static str) -> ApplyResult<bool> {
    // Callers pass only the two fixed pragma names below, never wire input.
    let rows = conn.query(&format!("PRAGMA {name}")).await?;
    if let [row] = rows.as_slice() {
        match row.get(0) {
            Some(SqliteValue::Integer(0)) => return Ok(false),
            Some(SqliteValue::Integer(1)) => return Ok(true),
            _ => {}
        }
    }
    Err(FrankenError::internal(format!("unsupported or malformed {name} readback")).into())
}

/// Re-enumerate at each boundary, including TEMP and attached databases.
/// Checking only the named changeset tables misses indirect trigger/FK work.
/// This opt-in validation may scan whole schemas; it is not a constant-cost
/// or constant-memory check on a database with many violations.
async fn require_clean_foreign_keys(conn: &Connection) -> ApplyResult<()> {
    if !pragma_flag(conn, "foreign_keys").await? {
        return Err(schema(
            "main",
            "deferred apply requires PRAGMA foreign_keys=ON",
        ));
    }
    let databases = conn.query("PRAGMA database_list").await?;
    let mut names: Vec<String> = Vec::new();
    for database in &databases {
        let Some(SqliteValue::Text(name)) = database.get(1) else {
            return Err(FrankenError::internal("invalid foreign-key schema inventory").into());
        };
        if name.is_empty()
            || name.contains('\0')
            || names.iter().any(|seen| seen.eq_ignore_ascii_case(name))
        {
            return Err(FrankenError::internal("ambiguous foreign-key schema inventory").into());
        }
        names.push(name.to_string());
    }
    if !names.iter().any(|name| name.eq_ignore_ascii_case("main")) {
        return Err(FrankenError::internal("foreign-key schema inventory omits main").into());
    }
    for name in names {
        let violations = conn
            .query(&format!(
                "PRAGMA {}.foreign_key_check",
                quote_identifier(&name)
            ))
            .await?;
        if !violations.is_empty() {
            return Err(FrankenError::ForeignKeyViolation.into());
        }
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
    handler: F,
) -> ApplyResult<SqlChangesetApplyReport>
where
    F: FnMut(SqlChangesetConflict<'_>) -> ConflictAction,
{
    apply_with_options(
        conn,
        changeset,
        ApplyOptions {
            retry_limits: limits,
            foreign_keys: ForeignKeyPolicy::Preserve,
        },
        handler,
    )
    .await
}

/// Apply a complete buffered import with explicit retry and FK policies.
///
/// [`ForeignKeyPolicy::DeferChecked`] enables eligible foreign-key deferral
/// only AFTER entering the owned transaction and proving enforcement is on
/// and every schema is initially clean. It never sets `foreign_keys=OFF`.
/// Children may precede their parents, including cyclic NO ACTION references.
/// All schemas are checked again after the final row/retry and before COMMIT;
/// unresolved references or disabled enforcement abort the complete import,
/// even when a row handler omitted a needed parent. This check also runs for
/// empty imports so an empty payload cannot bypass the requested policy.
///
/// The transaction owns the deferral setting. Successful COMMIT or ROLLBACK
/// auto-resets it just as an ordinary transaction does, including when it was
/// already enabled before BEGIN. There is no post-COMMIT await that can turn
/// an acknowledged apply into a retry. Dropping or panicking retains the same
/// deferred rollback owner; the next SQL entry settles that transaction and
/// resets its deferral. Failed rollback remains an explicit cleanup obligation.
///
/// This policy deliberately rejects pre-existing FK violations rather than
/// silently adopting them. It does not add SQLite's global FOREIGN_KEY omit
/// callback, change cascade/RESTRICT actions, or certify cross-database atomic
/// commits beyond the underlying connection's existing transaction contract.
pub async fn apply_with_options<F>(
    conn: &mut Connection,
    changeset: &Changeset,
    options: ApplyOptions,
    mut handler: F,
) -> ApplyResult<SqlChangesetApplyReport>
where
    F: FnMut(SqlChangesetConflict<'_>) -> ConflictAction,
{
    validate(changeset)?;
    if conn.in_transaction() {
        return Err(FrankenError::NestedTransaction.into());
    }
    let defer = options.foreign_keys == ForeignKeyPolicy::DeferChecked;
    if !defer && changeset.tables.iter().all(|table| table.rows.is_empty()) {
        return Ok(SqlChangesetApplyReport::default());
    }
    let mut transaction = ApplyTransaction { conn, armed: true };
    if let Err(error) = conn.begin_transaction().await {
        return Err(transaction.rollback(error.into()).await);
    }
    let result = async {
        if defer {
            require_clean_foreign_keys(conn).await?;
            conn.execute("PRAGMA defer_foreign_keys=ON").await?;
            if !pragma_flag(conn, "defer_foreign_keys").await? {
                return Err(schema("main", "foreign-key deferral was not enabled"));
            }
        }
        let mut plans = Vec::new();
        for table in changeset
            .tables
            .iter()
            .filter(|table| !table.rows.is_empty())
        {
            plans.push((table, TablePlan::load(conn, table).await?));
        }
        let mut report = SqlChangesetApplyReport::default();
        let mut attempts_left = options.retry_limits.max_retry_attempts;
        for (table, plan) in plans {
            apply_table(
                conn,
                table,
                &plan,
                &mut handler,
                &mut report,
                options.retry_limits,
                &mut attempts_left,
            )
            .await?;
        }
        if defer {
            if !pragma_flag(conn, "defer_foreign_keys").await? {
                return Err(schema("main", "foreign-key deferral changed during apply"));
            }
            require_clean_foreign_keys(conn).await?;
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
    use crate::compat::changeset::{
        SqlChangesetApplyError, apply_changeset, apply_changeset_with_handler,
    };
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

    fn table(name: &str, columns: usize, rows: Vec<ChangesetRow>) -> TableChangeset {
        let mut pk_flags = vec![false; columns];
        pk_flags[0] = true;
        TableChangeset {
            info: TableInfo {
                name: name.to_owned(),
                column_count: columns,
                pk_flags,
            },
            rows,
        }
    }

    fn changes(rows: Vec<ChangesetRow>) -> Changeset {
        Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![table("t", 2, rows)],
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
            .await
            .unwrap();
        for i in 1..=count {
            conn.execute_with_params(
                "INSERT INTO t VALUES(?1,?2)",
                &[SqliteValue::Integer(i), SqliteValue::Integer(i)],
            )
            .await
            .unwrap();
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
                    } else {
                        incoming
                    };
                    let mut calls = 0;
                    let report = apply_changeset_with_handler(&mut conn, &incoming, |_| {
                        calls += 1;
                        ConflictAction::Abort
                    })
                    .await
                    .unwrap();
                    assert_eq!(
                        calls, 0,
                        "transient UNIQUE failures are not application conflicts"
                    );
                    assert_eq!(report.applied, usize::try_from(count).unwrap());
                    assert_eq!((report.skipped, report.replaced), (0, 0));
                    let stock = rusqlite::Connection::open_in_memory().unwrap();
                    stock
                        .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER UNIQUE)")
                        .unwrap();
                    for i in 1..=count {
                        stock
                            .execute("INSERT INTO t VALUES(?1,?2)", [i, i])
                            .unwrap();
                    }
                    for i in (1..=count).rev() {
                        stock
                            .execute("UPDATE t SET v=?1 WHERE id=?2", [i + 1, i])
                            .unwrap();
                    }
                    let expected: Vec<(i64, i64)> = stock
                        .prepare("SELECT id,v FROM t ORDER BY id")
                        .unwrap()
                        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                        .unwrap()
                        .collect::<Result<_, _>>()
                        .unwrap();
                    assert_eq!(values(&conn).await, expected);
                    assert_eq!(
                        conn.query_row("PRAGMA integrity_check")
                            .await
                            .unwrap()
                            .get(0),
                        Some(&SqliteValue::Text("ok".into()))
                    );
                    assert!(!conn.in_transaction());
                }
            }
        });
    }

    #[test]
    fn insert_waits_for_later_delete_without_leaking_failed_trigger_effects() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup(1).await;
            conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            conn.execute("CREATE TRIGGER audit_insert BEFORE INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END").await.unwrap();
            let incoming = changes(vec![
                insert(2, 1),
                ChangesetRow {
                    op: ChangeOp::Delete,
                    indirect: false,
                    old_values: vec![ChangesetValue::Integer(1), ChangesetValue::Integer(1)],
                    new_values: Vec::new(),
                },
            ]);
            assert_eq!(
                apply_changeset(&mut conn, &incoming).await.unwrap().applied,
                2
            );
            assert_eq!(values(&conn).await, vec![(2, 1)]);
            let audit = conn.query("SELECT id FROM audit").await.unwrap();
            assert_eq!(audit.len(), 1);
            assert_eq!(audit[0].get(0), Some(&SqliteValue::Integer(2)));
        });
    }

    #[test]
    fn terminal_unique_conflict_is_reported_once_with_original_index() {
        asupersync::test_utils::run_test(|| async {
            for omit in [false, true] {
                let mut conn = setup(1).await;
                let incoming = changes(vec![insert(2, 2), insert(3, 1), insert(4, 4)]);
                let mut seen = Vec::new();
                let result = apply_changeset_with_handler(&mut conn, &incoming, |conflict| {
                    seen.push((conflict.kind, conflict.row));
                    if omit {
                        ConflictAction::OmitChange
                    } else {
                        ConflictAction::Abort
                    }
                })
                .await;
                assert_eq!(seen, vec![(ConflictType::Constraint, 1)]);
                if omit {
                    assert_eq!(
                        result.unwrap(),
                        SqlChangesetApplyReport {
                            applied: 2,
                            skipped: 1,
                            replaced: 0
                        }
                    );
                    assert_eq!(values(&conn).await, vec![(1, 1), (2, 2), (4, 4)]);
                } else {
                    assert!(matches!(
                        result,
                        Err(SqlChangesetApplyError::Constraint { row: 1, .. })
                    ));
                    assert_eq!(values(&conn).await, vec![(1, 1)]);
                }
                assert!(!conn.in_transaction());
            }
        });
    }

    #[test]
    fn retry_resource_exhaustion_rolls_back_all_provisional_progress() {
        asupersync::test_utils::run_test(|| async {
            for limits in [
                RetryLimits {
                    max_deferred_rows: 1,
                    max_retry_attempts: 100,
                },
                RetryLimits {
                    max_deferred_rows: 4,
                    max_retry_attempts: 0,
                },
            ] {
                let mut conn = setup(3).await;
                let incoming = changes(vec![update(1, 1, 2), update(2, 2, 3), update(3, 3, 4)]);
                let mut calls = 0;
                let result = apply_with_limits(&mut conn, &incoming, limits, |_| {
                    calls += 1;
                    ConflictAction::OmitChange
                })
                .await;
                assert!(matches!(
                    result,
                    Err(SqlChangesetApplyError::Database(FrankenError::TooBig))
                ));
                assert_eq!(
                    calls, 0,
                    "resource admission cannot become an omittable constraint"
                );
                assert_eq!(values(&conn).await, vec![(1, 1), (2, 2), (3, 3)]);
                assert!(!conn.in_transaction());
                assert_eq!(
                    apply_changeset(&mut conn, &incoming).await.unwrap().applied,
                    3
                );
                assert_eq!(values(&conn).await, vec![(1, 2), (2, 3), (3, 4)]);
            }
        });
    }

    #[test]
    fn primary_key_conflicts_are_not_hidden_by_a_later_delete() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup(1).await;
            let incoming = changes(vec![
                insert(1, 2),
                ChangesetRow {
                    op: ChangeOp::Delete,
                    indirect: false,
                    old_values: vec![ChangesetValue::Integer(1), ChangesetValue::Integer(1)],
                    new_values: Vec::new(),
                },
            ]);
            let result = apply_changeset(&mut conn, &incoming).await;
            assert!(matches!(
                result,
                Err(SqlChangesetApplyError::Conflict {
                    kind: ConflictType::Conflict,
                    row: 0,
                    ..
                })
            ));
            assert_eq!(values(&conn).await, vec![(1, 1)]);
        });
    }

    #[cfg(feature = "native")]
    #[test]
    fn dependency_resolved_rows_and_unique_index_survive_reopen() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("dependencies.db");
            let mut conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER UNIQUE)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1,1),(2,2),(3,3)")
                .await
                .unwrap();
            let incoming = changes(vec![update(1, 1, 2), update(2, 2, 3), update(3, 3, 4)]);
            assert_eq!(
                apply_changeset(&mut conn, &incoming).await.unwrap().applied,
                3
            );
            conn.close().await.unwrap();
            let reopened = Connection::open(path.to_str().unwrap()).await.unwrap();
            assert_eq!(values(&reopened).await, vec![(1, 2), (2, 3), (3, 4)]);
            assert_eq!(
                reopened
                    .query_row("SELECT id FROM t WHERE v=3")
                    .await
                    .unwrap()
                    .get(0),
                Some(&SqliteValue::Integer(2))
            );
            assert_eq!(
                reopened
                    .query_row("PRAGMA integrity_check")
                    .await
                    .unwrap()
                    .get(0),
                Some(&SqliteValue::Text("ok".into()))
            );
            reopened.close().await.unwrap();
            let stock = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(
                stock
                    .query_row("SELECT id FROM t WHERE v=3", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                2
            );
            assert_eq!(
                stock
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
        });
    }

    fn deferred() -> ApplyOptions {
        ApplyOptions {
            foreign_keys: ForeignKeyPolicy::DeferChecked,
            ..ApplyOptions::default()
        }
    }

    fn child_and_parent() -> Changeset {
        Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![
                table("t", 2, vec![insert(1, 9)]),
                table(
                    "parent",
                    1,
                    vec![ChangesetRow {
                        op: ChangeOp::Insert,
                        indirect: false,
                        old_values: Vec::new(),
                        new_values: vec![ChangesetValue::Integer(9)],
                    }],
                ),
            ],
        }
    }

    async fn fk_setup(conn: &Connection) {
        conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER REFERENCES parent(id))")
            .await
            .unwrap();
    }

    async fn assert_policy_reset(conn: &Connection) {
        // Query first: it also discharges an abandoned apply's rollback owner.
        assert!(!pragma_flag(conn, "defer_foreign_keys").await.unwrap());
        assert!(pragma_flag(conn, "foreign_keys").await.unwrap());
        assert!(!conn.in_transaction());
    }

    #[test]
    fn deferred_mode_resolves_child_before_parent_but_default_remains_immediate() {
        asupersync::test_utils::run_test(|| async {
            for patchset in [false, true] {
                let mut conn = Connection::open(":memory:").await.unwrap();
                fk_setup(&conn).await;
                let incoming = child_and_parent();
                let incoming = if patchset {
                    Changeset::decode_patchset(&incoming.encode_patchset()).unwrap()
                } else {
                    incoming
                };
                assert!(matches!(
                    apply_changeset(&mut conn, &incoming).await,
                    Err(SqlChangesetApplyError::Constraint {
                        error: FrankenError::ForeignKeyViolation,
                        ..
                    })
                ));
                assert!(values(&conn).await.is_empty());
                let mut calls = 0;
                let report = apply_with_options(&mut conn, &incoming, deferred(), |_| {
                    calls += 1;
                    ConflictAction::Abort
                })
                .await
                .unwrap();
                assert_eq!(
                    report,
                    SqlChangesetApplyReport {
                        applied: 2,
                        skipped: 0,
                        replaced: 0
                    }
                );
                assert_eq!(calls, 0);
                assert_eq!(values(&conn).await, vec![(1, 9)]);
                assert!(
                    conn.query("PRAGMA foreign_key_check")
                        .await
                        .unwrap()
                        .is_empty()
                );
                assert_policy_reset(&conn).await;
                assert!(conn.execute("INSERT INTO t VALUES(2,99)").await.is_err());
            }
        });
    }

    #[test]
    fn deferred_mode_handles_cycles_together_with_unique_dependency_retries() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup(3).await;
            conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
            conn.execute("CREATE TABLE a(id INTEGER PRIMARY KEY,bid REFERENCES b(id))")
                .await
                .unwrap();
            conn.execute("CREATE TABLE b(id INTEGER PRIMARY KEY,aid REFERENCES a(id))")
                .await
                .unwrap();
            let mut incoming = changes(vec![update(1, 1, 2), update(2, 2, 3), update(3, 3, 4)]);
            incoming.tables.insert(0, table("b", 2, vec![insert(2, 1)]));
            incoming.tables.insert(0, table("a", 2, vec![insert(1, 2)]));
            let report =
                apply_with_options(&mut conn, &incoming, deferred(), |_| ConflictAction::Abort)
                    .await
                    .unwrap();
            assert_eq!(report.applied, 5);
            assert_eq!(values(&conn).await, vec![(1, 2), (2, 3), (3, 4)]);
            assert_eq!(conn.query("SELECT * FROM a").await.unwrap().len(), 1);
            assert_eq!(conn.query("SELECT * FROM b").await.unwrap().len(), 1);
            assert!(
                conn.query("PRAGMA foreign_key_check")
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_policy_reset(&conn).await;
        });
    }

    #[test]
    fn omitted_parent_cannot_make_unresolved_fk_or_trigger_writes_commit() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
            conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY,v TEXT NOT NULL)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v REFERENCES parent(id))")
                .await
                .unwrap();
            conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            conn.execute("CREATE TRIGGER log_t AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END").await.unwrap();
            let mut incoming = child_and_parent();
            incoming.tables[1] = table(
                "parent",
                2,
                vec![ChangesetRow {
                    op: ChangeOp::Insert,
                    indirect: false,
                    old_values: Vec::new(),
                    new_values: vec![ChangesetValue::Integer(9), ChangesetValue::Null],
                }],
            );
            let mut calls = 0;
            let result = apply_with_options(&mut conn, &incoming, deferred(), |conflict| {
                calls += 1;
                assert!(matches!(
                    conflict.error,
                    Some(FrankenError::NotNullViolation { .. })
                ));
                ConflictAction::OmitChange
            })
            .await;
            assert!(matches!(
                result,
                Err(SqlChangesetApplyError::Database(
                    FrankenError::ForeignKeyViolation
                ))
            ));
            assert_eq!(
                calls, 1,
                "final FK failure is not an omittable row conflict"
            );
            for name in ["t", "parent", "audit"] {
                assert!(
                    conn.query(&format!("SELECT * FROM {name}"))
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
            assert_policy_reset(&conn).await;
            incoming.tables[1].rows[0].new_values[1] = ChangesetValue::Text("parent".to_owned());
            assert_eq!(
                apply_with_options(&mut conn, &incoming, deferred(), |_| ConflictAction::Abort)
                    .await
                    .unwrap()
                    .applied,
                2
            );
            assert_eq!(conn.query("SELECT * FROM audit").await.unwrap().len(), 1);
        });
    }

    #[test]
    fn deferred_mode_rejects_preexisting_main_and_temp_orphans_even_for_empty_input() {
        asupersync::test_utils::run_test(|| async {
            for name in ["main", "temp"] {
                let mut conn = setup(0).await;
                conn.execute("PRAGMA foreign_keys=OFF").await.unwrap();
                conn.execute(&format!(
                    "CREATE TABLE {name}.bad_parent(id INTEGER PRIMARY KEY)"
                ))
                .await
                .unwrap();
                conn.execute(&format!("CREATE TABLE {name}.bad_child(id INTEGER PRIMARY KEY,pid REFERENCES bad_parent(id))")).await.unwrap();
                conn.execute(&format!("INSERT INTO {name}.bad_child VALUES(1,99)"))
                    .await
                    .unwrap();
                conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
                for incoming in [changes(Vec::new()), changes(vec![insert(1, 1)])] {
                    let mut calls = 0;
                    let result = apply_with_options(&mut conn, &incoming, deferred(), |_| {
                        calls += 1;
                        ConflictAction::OmitChange
                    })
                    .await;
                    assert!(matches!(
                        result,
                        Err(SqlChangesetApplyError::Database(
                            FrankenError::ForeignKeyViolation
                        ))
                    ));
                    assert_eq!(calls, 0);
                    assert!(values(&conn).await.is_empty());
                    assert_eq!(
                        conn.query(&format!("SELECT * FROM {name}.bad_child"))
                            .await
                            .unwrap()
                            .len(),
                        1
                    );
                    assert_policy_reset(&conn).await;
                }
            }
        });
    }

    #[cfg(feature = "native")]
    #[test]
    fn attached_orphans_are_checked_without_importing_into_the_attachment() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("preexisting.db");
            let stock = rusqlite::Connection::open(&path).unwrap();
            stock.execute_batch("PRAGMA foreign_keys=OFF; CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(id INTEGER PRIMARY KEY,pid REFERENCES p(id)); INSERT INTO c VALUES(1,99)").unwrap();
            stock.close().unwrap();
            let mut conn = setup(0).await;
            conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
            conn.execute_with_params(
                "ATTACH DATABASE ?1 AS \"odd\"\"attached\"",
                &[SqliteValue::Text(path.to_str().unwrap().into())],
            )
            .await
            .unwrap();
            let result =
                apply_with_options(&mut conn, &changes(vec![insert(1, 1)]), deferred(), |_| {
                    ConflictAction::Abort
                })
                .await;
            assert!(matches!(
                result,
                Err(SqlChangesetApplyError::Database(
                    FrankenError::ForeignKeyViolation
                ))
            ));
            assert!(values(&conn).await.is_empty());
            assert_policy_reset(&conn).await;
            conn.close().await.unwrap();
            let stock = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(
                stock
                    .query_row("SELECT pid FROM c", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                99
            );
        });
    }

    #[test]
    fn deferred_mode_checks_indirect_temp_trigger_work_before_commit() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup(0).await;
            conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
            conn.execute("CREATE TEMP TABLE p(id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            conn.execute("CREATE TEMP TABLE c(id INTEGER PRIMARY KEY,pid REFERENCES p(id))")
                .await
                .unwrap();
            conn.execute("CREATE TEMP TRIGGER log_t AFTER INSERT ON t BEGIN INSERT INTO c VALUES(new.id,new.v); END").await.unwrap();
            let result =
                apply_with_options(&mut conn, &changes(vec![insert(1, 99)]), deferred(), |_| {
                    ConflictAction::Abort
                })
                .await;
            assert!(matches!(
                result,
                Err(SqlChangesetApplyError::Database(
                    FrankenError::ForeignKeyViolation
                ))
            ));
            assert!(values(&conn).await.is_empty());
            assert!(conn.query("SELECT * FROM temp.c").await.unwrap().is_empty());
            assert_policy_reset(&conn).await;
        });
    }

    #[test]
    fn deferred_mode_requires_enforcement_and_never_takes_over_a_caller_transaction() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup(0).await;
            conn.execute("PRAGMA foreign_keys=OFF").await.unwrap();
            for incoming in [changes(Vec::new()), changes(vec![insert(1, 1)])] {
                assert!(matches!(
                    apply_with_options(&mut conn, &incoming, deferred(), |_| ConflictAction::Abort)
                        .await,
                    Err(SqlChangesetApplyError::Schema { .. })
                ));
                assert!(values(&conn).await.is_empty());
                assert!(!pragma_flag(&conn, "foreign_keys").await.unwrap());
                assert!(!pragma_flag(&conn, "defer_foreign_keys").await.unwrap());
            }
            conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
            conn.begin_transaction().await.unwrap();
            conn.execute("PRAGMA defer_foreign_keys=ON").await.unwrap();
            conn.execute("INSERT INTO t VALUES(9,9)").await.unwrap();
            assert!(matches!(
                apply_with_options(&mut conn, &changes(vec![insert(1, 1)]), deferred(), |_| {
                    ConflictAction::Abort
                })
                .await,
                Err(SqlChangesetApplyError::Database(
                    FrankenError::NestedTransaction
                ))
            ));
            assert!(conn.in_transaction());
            assert!(pragma_flag(&conn, "defer_foreign_keys").await.unwrap());
            assert_eq!(values(&conn).await, vec![(9, 9)]);
            conn.rollback_transaction().await.unwrap();
            assert_policy_reset(&conn).await;
        });
    }

    #[test]
    fn panic_during_deferred_apply_retains_rollback_and_restores_the_next_transaction() {
        use std::future::{Future, poll_fn};
        use std::panic::{AssertUnwindSafe, catch_unwind};
        use std::task::Poll;

        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            fk_setup(&conn).await;
            let mut incoming = child_and_parent();
            incoming.tables[0].rows.push(insert(1, 9));
            let mut operation =
                Box::pin(apply_with_options(&mut conn, &incoming, deferred(), |_| {
                    panic!("deferred apply conflict handler panic");
                }));
            let panicked = poll_fn(|cx| {
                match catch_unwind(AssertUnwindSafe(|| operation.as_mut().poll(cx))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(_)) => Poll::Ready(false),
                    Err(_) => Poll::Ready(true),
                }
            })
            .await;
            assert!(panicked);
            drop(operation);
            assert!(values(&conn).await.is_empty());
            assert_policy_reset(&conn).await;
            assert_eq!(
                apply_with_options(&mut conn, &child_and_parent(), deferred(), |_| {
                    ConflictAction::Abort
                })
                .await
                .unwrap()
                .applied,
                2
            );
        });
    }

    #[cfg(feature = "native")]
    #[test]
    fn deferred_parent_child_commit_and_index_survive_reopen() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("deferred.db");
            let mut conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            fk_setup(&conn).await;
            conn.execute("CREATE INDEX by_parent ON t(v)")
                .await
                .unwrap();
            conn.execute("PRAGMA defer_foreign_keys=ON").await.unwrap();
            assert_eq!(
                apply_with_options(&mut conn, &child_and_parent(), deferred(), |_| {
                    ConflictAction::Abort
                })
                .await
                .unwrap()
                .applied,
                2
            );
            assert_policy_reset(&conn).await;
            conn.close().await.unwrap();
            let reopened = Connection::open(path.to_str().unwrap()).await.unwrap();
            assert_eq!(values(&reopened).await, vec![(1, 9)]);
            assert_eq!(
                reopened
                    .query_row("SELECT id FROM t INDEXED BY by_parent WHERE v=9")
                    .await
                    .unwrap()
                    .get(0),
                Some(&SqliteValue::Integer(1))
            );
            assert!(
                reopened
                    .query("PRAGMA foreign_key_check")
                    .await
                    .unwrap()
                    .is_empty()
            );
            reopened.close().await.unwrap();
            let stock = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(
                stock
                    .query_row(
                        "SELECT t.id FROM t JOIN parent ON t.v=parent.id",
                        [],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                1
            );
            assert_eq!(
                stock
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            assert!(
                stock
                    .prepare("PRAGMA foreign_key_check")
                    .unwrap()
                    .query([])
                    .unwrap()
                    .next()
                    .unwrap()
                    .is_none()
            );
        });
    }
}
