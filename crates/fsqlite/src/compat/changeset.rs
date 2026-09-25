//! Apply SQLite session changes to an actual SQL connection.
//!
//! Unlike `SimpleTarget`, this path uses the connection's ordinary DML,
//! indexes, constraints, triggers and durable transaction machinery. It owns
//! one transaction and requires an idle, exclusively borrowed connection.
//! Dropping an unfinished apply records the same deferred rollback obligation
//! used by `compat::Transaction`; no private runtime or detached SQL is used.
//!
//! Targets are ordinary tables in `main`, with an explicit non-NULL primary
//! key. Extra trailing target columns may use defaults. Missing/incompatible
//! schemas are errors, not SQLite's silent table-skipping policy. Generated,
//! virtual and internal tables are not accepted. This is not a preupdate-hook
//! recorder, a rebaser, or an implementation of the C session API.
//!
//! Enable the `session` Cargo feature. Decode using the existing
//! `fsqlite::session::Changeset` API, then apply the typed changeset:
//!
//! ```ignore
//! use fsqlite::compat::changeset::apply_changeset_with_handler;
//! use fsqlite::session::{Changeset, ConflictAction, ConflictType};
//!
//! let changes = Changeset::decode(&bytes)?;
//! let receipt = apply_changeset_with_handler(&mut connection, &changes, |conflict| {
//!     match conflict.kind {
//!         ConflictType::NotFound => ConflictAction::OmitChange,
//!         _ => ConflictAction::Abort,
//!     }
//! }).await?;
//! // Only now does receipt describe committed effects.
//! ```

use std::fmt;

use fsqlite_ast::Statement;
use fsqlite_ext_session::{
    ChangeOp, Changeset, ChangesetKind, ChangesetRow, ChangesetValue, ConflictAction, ConflictType,
    TableChangeset,
};
use fsqlite_parser::Parser;

use crate::{Connection, FrankenError, Row, SqliteValue};

/// Counts become a receipt only after the enclosing transaction commits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqlChangesetApplyReport {
    /// Successfully applied rows, including resolved replacements.
    pub applied: usize,
    /// Rows omitted by the conflict handler; none of their effects survive.
    pub skipped: usize,
    /// Applied rows whose DATA or primary-key CONFLICT was overridden.
    pub replaced: usize,
}

/// A conflict observed inside the apply transaction. The callback is
/// synchronous and cannot re-enter the exclusively borrowed connection.
/// `current` contains the recorded columns of the primary-key-matching target
/// row, not any unrelated row occupying a secondary UNIQUE key. A failed DML
/// attempt has already been rolled back, including its trigger effects, before
/// a constraint callback is invoked.
#[derive(Debug)]
pub struct SqlChangesetConflict<'a> {
    pub table: &'a str,
    /// Zero-based row index within this changeset table section.
    pub row: usize,
    pub kind: ConflictType,
    pub change: &'a ChangesetRow,
    pub current: Option<&'a [SqliteValue]>,
    pub error: Option<&'a FrankenError>,
}

/// A rejected apply, including errors from the storage transaction itself.
/// Database/commit errors retain the engine's outcome information: an I/O
/// error is not, by itself, proof that no durable effects occurred.
#[derive(Debug)]
pub enum SqlChangesetApplyError {
    InvalidChange {
        table: String,
        row: usize,
        detail: &'static str,
    },
    Schema {
        table: String,
        detail: &'static str,
    },
    Conflict {
        table: String,
        row: usize,
        kind: ConflictType,
    },
    Constraint {
        table: String,
        row: usize,
        error: FrankenError,
    },
    InvalidResolution {
        table: String,
        row: usize,
        kind: ConflictType,
    },
    Database(FrankenError),
    Rollback {
        cause: Box<Self>,
        rollback: FrankenError,
    },
}

impl fmt::Display for SqlChangesetApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidChange { table, row, detail } => {
                write!(f, "invalid changeset for {table:?}, row {row}: {detail}")
            }
            Self::Schema { table, detail } => {
                write!(f, "incompatible changeset target {table:?}: {detail}")
            }
            Self::Conflict { table, row, kind } => {
                write!(f, "changeset conflict in {table:?}, row {row}: {kind:?}")
            }
            Self::Constraint { table, row, error } => {
                write!(f, "changeset constraint in {table:?}, row {row}: {error}")
            }
            Self::InvalidResolution { table, row, kind } => write!(
                f,
                "REPLACE is not permitted for {kind:?} in {table:?}, row {row}"
            ),
            Self::Database(error) => write!(f, "{error}"),
            Self::Rollback { cause, rollback } => {
                write!(f, "{cause}; rollback also failed: {rollback}")
            }
        }
    }
}

impl std::error::Error for SqlChangesetApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(error) | Self::Constraint { error, .. } => Some(error),
            Self::Rollback { cause, .. } => Some(cause.as_ref()),
            _ => None,
        }
    }
}

impl From<FrankenError> for SqlChangesetApplyError {
    fn from(error: FrankenError) -> Self {
        Self::Database(error)
    }
}

type ApplyResult<T> = Result<T, SqlChangesetApplyError>;

fn invalid(table: &TableChangeset, row: usize, detail: &'static str) -> SqlChangesetApplyError {
    SqlChangesetApplyError::InvalidChange {
        table: table.info.name.clone(),
        row,
        detail,
    }
}

fn schema(name: &str, detail: &'static str) -> SqlChangesetApplyError {
    SqlChangesetApplyError::Schema {
        table: name.to_owned(),
        detail,
    }
}

const fn undefined(value: &ChangesetValue) -> bool {
    matches!(value, ChangesetValue::Undefined)
}

/// Public changeset structs are constructible without passing through a wire
/// decoder. Validate every row before the first SQL statement, including the
/// slots which would otherwise silently turn Undefined into SQL NULL.
fn validate(changeset: &Changeset) -> ApplyResult<()> {
    for table in &changeset.tables {
        let info = &table.info;
        if info.name.is_empty()
            || info.name.contains('\0')
            || info
                .name
                .get(..7)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("sqlite_"))
            || info.column_count == 0
            || info.pk_flags.len() != info.column_count
            || !info.pk_flags.iter().any(|pk| *pk)
        {
            return Err(invalid(
                table,
                0,
                "invalid name, column count or primary-key layout",
            ));
        }
        for (index, row) in table.rows.iter().enumerate() {
            let n = info.column_count;
            let width_ok = match row.op {
                ChangeOp::Insert => row.old_values.is_empty() && row.new_values.len() == n,
                ChangeOp::Delete => row.old_values.len() == n && row.new_values.is_empty(),
                ChangeOp::Update => row.old_values.len() == n && row.new_values.len() == n,
            };
            if !width_ok {
                return Err(invalid(table, index, "invalid row width"));
            }
            if row
                .old_values
                .iter()
                .chain(&row.new_values)
                .any(|value| matches!(value, ChangesetValue::Real(number) if number.is_nan()))
            {
                return Err(invalid(
                    table,
                    index,
                    "NaN must be normalized before recording a change",
                ));
            }
            let key = if row.op == ChangeOp::Insert {
                &row.new_values
            } else {
                &row.old_values
            };
            if info.pk_flags.iter().zip(key).any(|(pk, value)| {
                *pk && matches!(value, ChangesetValue::Undefined | ChangesetValue::Null)
            }) {
                return Err(invalid(
                    table,
                    index,
                    "primary-key values must be defined and non-NULL",
                ));
            }
            match row.op {
                ChangeOp::Insert if row.new_values.iter().any(undefined) => {
                    return Err(invalid(table, index, "INSERT contains an undefined value"));
                }
                ChangeOp::Delete => {
                    for (pk, old) in info.pk_flags.iter().zip(&row.old_values) {
                        let valid = if changeset.kind == ChangesetKind::Changeset || *pk {
                            !undefined(old)
                        } else {
                            undefined(old)
                        };
                        if !valid {
                            return Err(invalid(table, index, "invalid DELETE old-value layout"));
                        }
                    }
                }
                ChangeOp::Update => {
                    let mut modified = false;
                    for ((pk, old), new) in info
                        .pk_flags
                        .iter()
                        .zip(&row.old_values)
                        .zip(&row.new_values)
                    {
                        if *pk {
                            if !undefined(new) {
                                return Err(invalid(
                                    table,
                                    index,
                                    "primary-key changes must be DELETE plus INSERT",
                                ));
                            }
                        } else {
                            let valid = match changeset.kind {
                                ChangesetKind::Changeset => undefined(old) == undefined(new),
                                ChangesetKind::Patchset => undefined(old),
                            };
                            if !valid {
                                return Err(invalid(
                                    table,
                                    index,
                                    "invalid UPDATE old/new value layout",
                                ));
                            }
                            modified |= !undefined(new);
                        }
                    }
                    if !modified {
                        return Err(invalid(
                            table,
                            index,
                            "UPDATE has no changed non-key column",
                        ));
                    }
                }
                ChangeOp::Insert => {}
            }
        }
    }
    Ok(())
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn text(row: &Row, index: usize) -> Option<&str> {
    match row.get(index) {
        Some(SqliteValue::Text(value)) => Some(value),
        _ => None,
    }
}

struct TablePlan {
    name: String,
    qualified: String,
    columns: Vec<String>,
    keys: Vec<usize>,
    insert: String,
}

impl TablePlan {
    async fn load(conn: &Connection, table: &TableChangeset) -> ApplyResult<Self> {
        let name = &table.info.name;
        let entries = conn.query_with_params(
            "SELECT name, sql FROM main.sqlite_schema WHERE type='table' AND name = ?1 COLLATE NOCASE",
            &[SqliteValue::Text(name.as_str().into())],
        ).await?;
        if entries.len() != 1 {
            return Err(schema(name, "ordinary main table does not exist"));
        }
        let canonical = text(&entries[0], 0).ok_or_else(|| schema(name, "missing catalog name"))?;
        let sql =
            text(&entries[0], 1).ok_or_else(|| schema(name, "missing CREATE TABLE statement"))?;
        let (statements, errors) = Parser::from_sql(sql).parse_all();
        if !errors.is_empty() || !matches!(statements.as_slice(), [Statement::CreateTable(_)]) {
            return Err(schema(name, "only ordinary tables are supported"));
        }
        let columns = conn
            .query(&format!(
                "PRAGMA main.table_xinfo({})",
                quote_identifier(canonical)
            ))
            .await?;
        if columns.len() < table.info.column_count {
            return Err(schema(name, "target has fewer columns than the changeset"));
        }
        let mut names = Vec::with_capacity(table.info.column_count);
        for (index, column) in columns.iter().enumerate() {
            let Some(SqliteValue::Integer(cid)) = column.get(0) else {
                return Err(schema(name, "invalid column ordinal"));
            };
            let Some(SqliteValue::Integer(pk)) = column.get(5) else {
                return Err(schema(name, "missing primary-key metadata"));
            };
            if usize::try_from(*cid).ok() != Some(index)
                || !matches!(column.get(6), Some(SqliteValue::Integer(0)))
            {
                return Err(schema(
                    name,
                    "hidden or generated columns are not supported",
                ));
            }
            if (*pk > 0) != table.info.pk_flags.get(index).copied().unwrap_or(false) {
                return Err(schema(name, "primary-key columns differ"));
            }
            if index < table.info.column_count {
                let column_name =
                    text(column, 1).ok_or_else(|| schema(name, "missing column name"))?;
                if column_name.contains('\0') {
                    return Err(schema(name, "invalid column name"));
                }
                names.push(quote_identifier(column_name));
            }
        }
        let qualified = format!("main.{}", quote_identifier(canonical));
        let placeholders = (1..=names.len())
            .map(|n| format!("?{n}"))
            .collect::<Vec<_>>()
            .join(",");
        // ABORT deliberately overrides a target's IGNORE/REPLACE default. An
        // import must not silently discard data or delete an unrelated row.
        let insert = format!(
            "INSERT OR ABORT INTO {qualified} ({}) VALUES ({placeholders})",
            names.join(",")
        );
        let keys = table
            .info
            .pk_flags
            .iter()
            .enumerate()
            .filter(|(_, pk)| **pk)
            .map(|(i, _)| i)
            .collect();
        Ok(Self {
            name: canonical.to_owned(),
            qualified,
            columns: names,
            keys,
            insert,
        })
    }

    fn key_predicate(&self, row: &ChangesetRow, params: &mut Vec<SqliteValue>) -> String {
        let values = if row.op == ChangeOp::Insert {
            &row.new_values
        } else {
            &row.old_values
        };
        self.keys
            .iter()
            .map(|index| {
                params.push(values[*index].to_sqlite());
                format!("{} IS ?{}", self.columns[*index], params.len())
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    async fn lookup(
        &self,
        conn: &Connection,
        change: &ChangesetRow,
    ) -> ApplyResult<Option<(Vec<SqliteValue>, bool)>> {
        let mut params = Vec::new();
        let key = self.key_predicate(change, &mut params);
        let mut checks = Vec::new();
        for (index, old) in change.old_values.iter().enumerate() {
            if !self.keys.contains(&index) && !undefined(old) {
                params.push(old.to_sqlite());
                // Let SQL retain the target column's affinity and collation.
                // Comparing converted SqliteValues in Rust would lose both.
                checks.push(format!("{} IS ?{}", self.columns[index], params.len()));
            }
        }
        let check = if checks.is_empty() {
            "1".to_owned()
        } else {
            checks.join(" AND ")
        };
        let sql = format!(
            "SELECT {}, ({check}) FROM {} WHERE {key} LIMIT 2",
            self.columns.join(","),
            self.qualified
        );
        let rows = conn.query_with_params(&sql, &params).await?;
        if rows.len() > 1 {
            return Err(schema(
                &self.name,
                "primary-key lookup matched multiple rows",
            ));
        }
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let current = (0..self.columns.len())
            .map(|index| {
                row.get(index)
                    .cloned()
                    .ok_or_else(|| schema(&self.name, "short target row"))
            })
            .collect::<ApplyResult<Vec<_>>>()?;
        let matches = match row.get(self.columns.len()) {
            Some(SqliteValue::Integer(value)) => *value != 0,
            _ => return Err(schema(&self.name, "invalid old-value comparison result")),
        };
        Ok(Some((current, matches)))
    }

    async fn delete_key(
        &self,
        conn: &Connection,
        change: &ChangesetRow,
    ) -> Result<(), FrankenError> {
        let mut params = Vec::new();
        let key = self.key_predicate(change, &mut params);
        let changed = conn
            .execute_with_params(
                &format!("DELETE FROM {} WHERE {key}", self.qualified),
                &params,
            )
            .await?;
        if changed != 1 {
            return Err(FrankenError::internal(
                "changeset replacement lost its primary-key target",
            ));
        }
        Ok(())
    }

    async fn write(&self, conn: &Connection, change: &ChangesetRow) -> Result<(), FrankenError> {
        let mut params = Vec::new();
        let sql = match change.op {
            ChangeOp::Insert => {
                params.extend(change.new_values.iter().map(ChangesetValue::to_sqlite));
                self.insert.clone()
            }
            ChangeOp::Delete => {
                let key = self.key_predicate(change, &mut params);
                format!("DELETE FROM {} WHERE {key}", self.qualified)
            }
            ChangeOp::Update => {
                let mut assignments = Vec::new();
                for (index, value) in change.new_values.iter().enumerate() {
                    if !undefined(value) {
                        params.push(value.to_sqlite());
                        assignments.push(format!("{} = ?{}", self.columns[index], params.len()));
                    }
                }
                let key = self.key_predicate(change, &mut params);
                format!(
                    "UPDATE OR ABORT {} SET {} WHERE {key}",
                    self.qualified,
                    assignments.join(",")
                )
            }
        };
        let changed = conn.execute_with_params(&sql, &params).await?;
        if changed != 1 {
            return Err(FrankenError::internal(format!(
                "changeset DML expected one target row, changed {changed}"
            )));
        }
        Ok(())
    }
}

enum WriteAttempt {
    Applied,
    /// The row savepoint has been rolled back and released successfully.
    Rejected(FrankenError),
}

/// Never use INSERT OR REPLACE: it can delete unrelated rows that happen to
/// occupy secondary UNIQUE keys. Only delete the primary-key conflict, inside
/// the same savepoint as the subsequent ABORT-mode insert. Rejection restores
/// that row and all trigger side effects before asking the handler what to do.
async fn attempt_write(
    conn: &Connection,
    plan: &TablePlan,
    change: &ChangesetRow,
    replace_insert: bool,
) -> ApplyResult<WriteAttempt> {
    conn.execute("SAVEPOINT _fsqlite_changeset_row").await?;
    let result = async {
        if replace_insert {
            plan.delete_key(conn, change).await?;
        }
        plan.write(conn, change).await
    }
    .await;
    match result {
        Ok(()) => {
            conn.execute("RELEASE _fsqlite_changeset_row").await?;
            Ok(WriteAttempt::Applied)
        }
        Err(error) => {
            // RAISE(ROLLBACK), a failed cleanup, or a lost transaction is not
            // an omittable row conflict. The whole apply must stop.
            if !conn.in_transaction() {
                return Err(error.into());
            }
            let restored = async {
                conn.execute("ROLLBACK TO _fsqlite_changeset_row").await?;
                conn.execute("RELEASE _fsqlite_changeset_row").await?;
                Ok::<_, FrankenError>(())
            }
            .await;
            match restored {
                Ok(()) => Ok(WriteAttempt::Rejected(error)),
                Err(rollback) => Err(SqlChangesetApplyError::Rollback {
                    cause: Box::new(error.into()),
                    rollback,
                }),
            }
        }
    }
}

const fn is_row_constraint(error: &FrankenError) -> bool {
    // RaiseFail is a typed SQL trigger rejection even though the core's error
    // mapping currently calls it SQLITE_ERROR. Do not guess from text or turn
    // generic function/I/O/cancellation failures into skippable constraints.
    matches!(
        error,
        FrankenError::UniqueViolation { .. }
            | FrankenError::PrimaryKeyViolation
            | FrankenError::NotNullViolation { .. }
            | FrankenError::CheckViolation { .. }
            | FrankenError::ForeignKeyViolation
            | FrankenError::DatatypeViolation { .. }
            | FrankenError::RtreeConstraint { .. }
            | FrankenError::RaiseFail(_)
    )
}

enum RowOutcome {
    Applied { replaced: bool },
    Skipped,
}

async fn apply_row<F>(
    conn: &Connection,
    plan: &TablePlan,
    index: usize,
    change: &ChangesetRow,
    handler: &mut F,
) -> ApplyResult<RowOutcome>
where
    F: FnMut(SqlChangesetConflict<'_>) -> ConflictAction,
{
    let current = plan.lookup(conn, change).await?;
    let conflict = match (change.op, &current) {
        (ChangeOp::Insert, Some(_)) => Some(ConflictType::Conflict),
        (ChangeOp::Delete | ChangeOp::Update, None) => Some(ConflictType::NotFound),
        (ChangeOp::Delete | ChangeOp::Update, Some((_, false))) => Some(ConflictType::Data),
        _ => None,
    };
    let mut replaced = false;
    if let Some(kind) = conflict {
        match handler(SqlChangesetConflict {
            table: &plan.name,
            row: index,
            kind,
            change,
            current: current.as_ref().map(|(row, _)| row.as_slice()),
            error: None,
        }) {
            ConflictAction::Abort => {
                return Err(SqlChangesetApplyError::Conflict {
                    table: plan.name.clone(),
                    row: index,
                    kind,
                });
            }
            ConflictAction::OmitChange => return Ok(RowOutcome::Skipped),
            ConflictAction::Replace
                if matches!(kind, ConflictType::Data | ConflictType::Conflict) =>
            {
                replaced = true;
            }
            ConflictAction::Replace => {
                return Err(SqlChangesetApplyError::InvalidResolution {
                    table: plan.name.clone(),
                    row: index,
                    kind,
                });
            }
        }
    }
    match attempt_write(
        conn,
        plan,
        change,
        replaced && change.op == ChangeOp::Insert,
    )
    .await?
    {
        WriteAttempt::Applied => Ok(RowOutcome::Applied { replaced }),
        WriteAttempt::Rejected(error) => {
            if !is_row_constraint(&error) {
                return Err(error.into());
            }
            let kind = ConflictType::Constraint;
            match handler(SqlChangesetConflict {
                table: &plan.name,
                row: index,
                kind,
                change,
                current: current.as_ref().map(|(row, _)| row.as_slice()),
                error: Some(&error),
            }) {
                ConflictAction::OmitChange => Ok(RowOutcome::Skipped),
                ConflictAction::Abort => Err(SqlChangesetApplyError::Constraint {
                    table: plan.name.clone(),
                    row: index,
                    error,
                }),
                ConflictAction::Replace => Err(SqlChangesetApplyError::InvalidResolution {
                    table: plan.name.clone(),
                    row: index,
                    kind,
                }),
            }
        }
    }
}

struct ApplyTransaction<'a> {
    conn: &'a Connection,
    armed: bool,
}

impl ApplyTransaction<'_> {
    async fn rollback(&mut self, cause: SqlChangesetApplyError) -> SqlChangesetApplyError {
        if !self.conn.in_transaction() {
            self.armed = false;
            return cause;
        }
        match self.conn.rollback_transaction().await {
            Ok(()) => {
                self.armed = false;
                cause
            }
            Err(rollback) => SqlChangesetApplyError::Rollback {
                cause: Box::new(cause),
                rollback,
            },
        }
    }
}

impl Drop for ApplyTransaction<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.conn.mark_transaction_cleanup_required();
        }
    }
}

/// Apply a full changeset or decoded patchset to ordinary `main` tables.
///
/// Aborts on every data/key conflict. All table layouts are checked before
/// the first change. INSERT supplies recorded columns only; trailing target
/// columns receive their defaults. Sparse UPDATE and patchset slots preserve
/// unmodified target values. The `indirect` flag is provenance, not a request
/// to ignore a change or suppress ordinary target triggers.
///
/// An existing transaction is refused before modification. On failure an
/// awaited rollback is attempted; cancellation/drop records deferred cleanup
/// for the next SQL entry. Commit/rollback I/O errors remain explicit and are
/// not claims that a durable transaction was undone. This API does not weaken
/// foreign-key enforcement or change writer-concurrency defaults.
pub async fn apply_changeset(
    conn: &mut Connection,
    changeset: &Changeset,
) -> ApplyResult<SqlChangesetApplyReport> {
    apply_changeset_with_handler(conn, changeset, |_| ConflictAction::Abort).await
}

/// Apply with synchronous conflict resolution and a per-row rollback boundary.
///
/// `OmitChange` leaves no effects from the rejected row, including trigger
/// writes. `Replace` is permitted only for DATA mismatches or INSERT primary-
/// key conflicts. A replacement that violates another constraint is undone
/// before a second CONSTRAINT callback; unrelated UNIQUE-key rows are never
/// deleted. A failed savepoint rollback, I/O, cancellation or lost transaction
/// cannot be omitted. Counts describe only the successfully committed result.
///
/// Rows run in supplied order. Unlike the C session applier, this adapter does
/// not retry deferred uniqueness dependencies or temporarily defer foreign
/// keys: immediate FK errors are row CONSTRAINTs and a deferred FK failure at
/// COMMIT aborts the transaction. No global FOREIGN_KEY omit callback, rebasing
/// or operation reordering is provided. Normal target triggers remain enabled.
pub async fn apply_changeset_with_handler<F>(
    conn: &mut Connection,
    changeset: &Changeset,
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
    // Arm before BEGIN can suspend, not after it returns successfully.
    if let Err(error) = conn.begin_transaction().await {
        return Err(transaction.rollback(error.into()).await);
    }
    let result = async {
        let mut plans = Vec::new();
        for table in changeset
            .tables
            .iter()
            .filter(|table| !table.rows.is_empty())
        {
            plans.push((table, TablePlan::load(conn, table).await?));
        }
        let mut report = SqlChangesetApplyReport::default();
        for (table, plan) in plans {
            for (index, change) in table.rows.iter().enumerate() {
                match apply_row(conn, &plan, index, change, &mut handler).await? {
                    RowOutcome::Applied { replaced } => {
                        report.applied += 1;
                        report.replaced += usize::from(replaced);
                    }
                    RowOutcome::Skipped => report.skipped += 1,
                }
            }
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

/// Bounded changeset ingestion into one real SQL transaction.
///
/// Unlike table-by-table calls to `apply_changeset`, this path never commits
/// an input prefix. The wire reader retains one row and a fixed input buffer;
/// ordinary SQL transaction storage may still grow with the write set.
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub mod streaming {
    pub mod ordered;

    use asupersync::io::AsyncRead;
    use fsqlite_types::cx::Cx;

    use super::{
        ApplyTransaction, Changeset, ConflictAction, Connection, FrankenError, RowOutcome,
        SqlChangesetApplyError, SqlChangesetApplyReport, SqlChangesetConflict, TableChangeset,
        TablePlan, apply_row, validate,
    };
    use crate::compat::changeset_stream::{
        ChangesetStreamError, ChangesetStreamLimits, ChangesetStreamReader, StreamedChange,
    };

    /// Input failures are not row conflicts and cannot be omitted by a handler.
    /// A rollback error preserves both the original failure and cleanup failure.
    #[derive(Debug)]
    pub enum StreamApplyError {
        Input(ChangesetStreamError),
        Sql(SqlChangesetApplyError),
        LengthMismatch {
            expected: u64,
            consumed: u64,
        },
        Verification {
            detail: String,
        },
        Rollback {
            cause: Box<Self>,
            rollback: FrankenError,
        },
    }

    impl std::fmt::Display for StreamApplyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Input(error) => write!(f, "{error}"),
                Self::Sql(error) => write!(f, "{error}"),
                Self::LengthMismatch { expected, consumed } => {
                    write!(
                        f,
                        "changeset message length mismatch: expected {expected}, consumed {consumed}"
                    )
                }
                Self::Verification { detail } => {
                    write!(f, "changeset message verification failed: {detail}")
                }
                Self::Rollback { cause, rollback } => {
                    write!(f, "{cause}; stream rollback also failed: {rollback}")
                }
            }
        }
    }

    impl std::error::Error for StreamApplyError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Input(error) => Some(error),
                Self::Sql(error) => Some(error),
                Self::Rollback { cause, .. } => Some(cause.as_ref()),
                Self::LengthMismatch { .. } | Self::Verification { .. } => None,
            }
        }
    }

    impl From<ChangesetStreamError> for StreamApplyError {
        fn from(error: ChangesetStreamError) -> Self {
            Self::Input(error)
        }
    }

    impl From<SqlChangesetApplyError> for StreamApplyError {
        fn from(error: SqlChangesetApplyError) -> Self {
            Self::Sql(error)
        }
    }

    impl From<FrankenError> for StreamApplyError {
        fn from(error: FrankenError) -> Self {
            Self::Sql(error.into())
        }
    }

    /// Apply one complete EOF-delimited changeset or patchset atomically.
    ///
    /// The caller must supply one complete message: the SQLite Session wire
    /// format has no outer length, authentication, or commit-sequence field.
    /// In particular, EOF at a row boundary cannot establish that a transport
    /// delivered every row intended by its sender. Authenticate and frame the
    /// input outside this API when that guarantee is required, or use
    /// [`apply_verified`] to gate COMMIT on its length and completion verifier.
    ///
    /// Existing transactions are refused before input is read. A late schema,
    /// row, decoding, resource-limit, or input-I/O failure rolls back all prior
    /// rows, including trigger effects. Dropping this future records deferred
    /// rollback before the next SQL entry, using the existing apply guard.
    /// An error from COMMIT remains an error, not proof of non-durability.
    ///
    /// `cx` controls ingestion checkpoints. SQL uses the connection's unchanged
    /// environment; supply a shared context lineage there when required. A
    /// stalled input must wake on cancellation, or its caller must drop this
    /// future. This function builds no runtime and changes no concurrency mode.
    pub async fn apply<R: AsyncRead + Unpin>(
        conn: &mut Connection,
        cx: &Cx,
        input: R,
        limits: ChangesetStreamLimits,
    ) -> Result<SqlChangesetApplyReport, StreamApplyError> {
        apply_with_handler(conn, cx, input, limits, |_| ConflictAction::Abort).await
    }

    /// Streaming counterpart of `apply_changeset_with_handler`.
    ///
    /// Conflict semantics, parameter binding, target affinity/collation,
    /// primary-key-only replacement and per-row savepoints are shared with
    /// the ordinary SQL applier. Table plans are rebuilt at section boundaries,
    /// including repeated sections naming the same table. Row indices in
    /// callbacks and invalid-change errors refer to the original section.
    /// Unlike an already buffered changeset, later schemas are checked as they
    /// arrive; no effects become committed until the entire input is accepted.
    pub async fn apply_with_handler<R, F>(
        conn: &mut Connection,
        cx: &Cx,
        input: R,
        limits: ChangesetStreamLimits,
        handler: F,
    ) -> Result<SqlChangesetApplyReport, StreamApplyError>
    where
        R: AsyncRead + Unpin,
        F: FnMut(SqlChangesetConflict<'_>) -> ConflictAction,
    {
        apply_checked(conn, cx, input, limits, None, |_| Ok(()), handler).await
    }

    /// Apply an exact-length message only after its completion verifier accepts.
    ///
    /// `verify` receives the SAME owned input after clean EOF, before COMMIT,
    /// and is called exactly once on an otherwise successful input, including
    /// an empty message. It can check an incremental digest or authenticated
    /// transport receipt against metadata trusted by the caller. A rejection
    /// rolls back all SQL effects and never produces a success receipt.
    ///
    /// The verifier must inspect state derived from the bytes this input
    /// actually returned, not re-read a replaceable pathname. This API supplies
    /// no hashing algorithm, trusted key, signature protocol or replay ordering.
    /// Its length check also rejects truncation at a valid row boundary. Extra
    /// input is an error, not an implicitly accepted second message; the source
    /// must terminate at EOF rather than leave a persistent socket open.
    ///
    /// Verification gates DATABASE COMMIT, not execution: SQL triggers and
    /// conflict callbacks can run before verification. Their external effects
    /// cannot be rolled back. Authenticate chunks before ingestion when these
    /// callbacks or SQL functions can perform external side effects.
    pub async fn apply_verified<R, V>(
        conn: &mut Connection,
        cx: &Cx,
        input: R,
        limits: ChangesetStreamLimits,
        expected_bytes: u64,
        verify: V,
    ) -> Result<SqlChangesetApplyReport, StreamApplyError>
    where
        R: AsyncRead + Unpin,
        V: FnOnce(&R) -> Result<(), String>,
    {
        apply_verified_with_handler(conn, cx, input, limits, expected_bytes, verify, |_| {
            ConflictAction::Abort
        })
        .await
    }

    /// Exact-message verification with the ordinary strict conflict handler.
    /// See [`apply_verified`] for trust, EOF and callback-side-effect semantics.
    pub async fn apply_verified_with_handler<R, V, F>(
        conn: &mut Connection,
        cx: &Cx,
        input: R,
        limits: ChangesetStreamLimits,
        expected_bytes: u64,
        verify: V,
        handler: F,
    ) -> Result<SqlChangesetApplyReport, StreamApplyError>
    where
        R: AsyncRead + Unpin,
        V: FnOnce(&R) -> Result<(), String>,
        F: FnMut(SqlChangesetConflict<'_>) -> ConflictAction,
    {
        apply_checked(
            conn,
            cx,
            input,
            limits,
            Some(expected_bytes),
            verify,
            handler,
        )
        .await
    }

    async fn apply_checked<R, V, F>(
        conn: &mut Connection,
        cx: &Cx,
        mut input: R,
        mut limits: ChangesetStreamLimits,
        expected_bytes: Option<u64>,
        verify: V,
        mut handler: F,
    ) -> Result<SqlChangesetApplyReport, StreamApplyError>
    where
        R: AsyncRead + Unpin,
        V: FnOnce(&R) -> Result<(), String>,
        F: FnMut(SqlChangesetConflict<'_>) -> ConflictAction,
    {
        if conn.in_transaction() {
            return Err(FrankenError::NestedTransaction.into());
        }
        checkpoint(cx)?;
        if let Some(expected) = expected_bytes {
            if expected > limits.max_input_bytes {
                return Err(ChangesetStreamError::Limit {
                    offset: 0,
                    resource: "declared input bytes",
                }
                .into());
            }
            // Bound work by the declared message too, not just the caller's
            // general policy. Never apply an attacker-supplied extra stream.
            limits.max_input_bytes = expected;
        }
        let mut reader = ChangesetStreamReader::new(&mut input, limits);
        let Some(first) = reader.next(cx).await? else {
            let consumed = reader.bytes_consumed();
            drop(reader);
            verify_input(&input, expected_bytes, consumed, verify)?;
            checkpoint(cx)?;
            return Ok(SqlChangesetApplyReport::default());
        };
        // Arm before BEGIN can suspend. A cancelled future must not leave an
        // unowned transaction even if admission completed before cancellation.
        let mut transaction = ApplyTransaction { conn, armed: true };
        if let Err(error) = conn.begin_transaction().await {
            return Err(rollback(&mut transaction, error.into()).await);
        }
        let result = apply_rows(conn, cx, &mut reader, first, &mut handler, None).await;
        let report = match result {
            Ok(report) => report,
            Err(error) => return Err(rollback(&mut transaction, error).await),
        };
        let consumed = reader.bytes_consumed();
        drop(reader);
        if let Err(error) = verify_input(&input, expected_bytes, consumed, verify) {
            return Err(rollback(&mut transaction, error).await);
        }
        if let Err(error) = checkpoint(cx) {
            return Err(rollback(&mut transaction, error).await);
        }
        if let Err(error) = conn.commit_transaction().await {
            return Err(rollback(&mut transaction, error.into()).await);
        }
        transaction.armed = false;
        Ok(report)
    }

    fn verify_input<R, V>(
        input: &R,
        expected_bytes: Option<u64>,
        consumed: u64,
        verify: V,
    ) -> Result<(), StreamApplyError>
    where
        V: FnOnce(&R) -> Result<(), String>,
    {
        if let Some(expected) = expected_bytes
            && consumed != expected
        {
            return Err(StreamApplyError::LengthMismatch { expected, consumed });
        }
        verify(input).map_err(|detail| StreamApplyError::Verification { detail })
    }

    fn checkpoint(cx: &Cx) -> Result<(), StreamApplyError> {
        cx.checkpoint()
            .map_err(|_| ChangesetStreamError::Cancelled.into())
    }

    async fn rollback(
        transaction: &mut ApplyTransaction<'_>,
        cause: StreamApplyError,
    ) -> StreamApplyError {
        if !transaction.conn.in_transaction() {
            transaction.armed = false;
            return cause;
        }
        match transaction.conn.rollback_transaction().await {
            Ok(()) => {
                transaction.armed = false;
                cause
            }
            Err(rollback) => StreamApplyError::Rollback {
                cause: Box::new(cause),
                rollback,
            },
        }
    }

    async fn apply_rows<R, F>(
        conn: &Connection,
        cx: &Cx,
        reader: &mut ChangesetStreamReader<R>,
        first: StreamedChange,
        handler: &mut F,
        excluded_table: Option<&str>,
    ) -> Result<SqlChangesetApplyReport, StreamApplyError>
    where
        R: AsyncRead + Unpin,
        F: FnMut(SqlChangesetConflict<'_>) -> ConflictAction,
    {
        let mut section = first.section;
        let mut single = Changeset {
            kind: first.kind,
            tables: vec![TableChangeset {
                info: first.table.as_ref().clone(),
                rows: Vec::with_capacity(1),
            }],
        };
        let mut plan = None;
        let mut next = Some(first);
        let mut report = SqlChangesetApplyReport::default();
        while let Some(event) = next {
            checkpoint(cx)?;
            if excluded_table.is_some_and(|name| name.eq_ignore_ascii_case(&event.table.name)) {
                return Err(SqlChangesetApplyError::Schema {
                    table: event.table.name.clone(),
                    detail: "changeset targets replication-owned metadata",
                }
                .into());
            }
            if event.section != section {
                section = event.section;
                single.kind = event.kind;
                single.tables[0].info = event.table.as_ref().clone();
                plan = None;
            }
            single.tables[0].rows.push(event.change);
            // Use the same semantic validator, with a one-row reusable slot,
            // instead of a second interpretation of Undefined/NULL/PK rules.
            validate(&single).map_err(|error| match error {
                SqlChangesetApplyError::InvalidChange { table, detail, .. } => {
                    SqlChangesetApplyError::InvalidChange {
                        table,
                        row: event.row_index,
                        detail,
                    }
                }
                other => other,
            })?;
            if plan.is_none() {
                plan = Some(TablePlan::load(conn, &single.tables[0]).await?);
            }
            let table_plan = plan.as_ref().ok_or_else(|| {
                FrankenError::internal("streamed changeset lost its validated table plan")
            })?;
            match apply_row(
                conn,
                table_plan,
                event.row_index,
                &single.tables[0].rows[0],
                handler,
            )
            .await?
            {
                RowOutcome::Applied { replaced } => {
                    report.applied = report.applied.checked_add(1).ok_or_else(|| {
                        FrankenError::internal("streamed changeset applied count overflow")
                    })?;
                    report.replaced = report
                        .replaced
                        .checked_add(usize::from(replaced))
                        .ok_or_else(|| {
                            FrankenError::internal("streamed changeset replacement count overflow")
                        })?;
                }
                RowOutcome::Skipped => {
                    report.skipped = report.skipped.checked_add(1).ok_or_else(|| {
                        FrankenError::internal("streamed changeset skipped count overflow")
                    })?;
                }
            }
            // Release the old payload BEFORE awaiting/allocating the next row.
            single.tables[0].rows.clear();
            next = reader.next(cx).await?;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_ext_session::{Session, TableInfo};

    const fn int(value: i64) -> ChangesetValue {
        ChangesetValue::Integer(value)
    }
    fn string(value: &str) -> ChangesetValue {
        ChangesetValue::Text(value.to_owned())
    }
    fn insert(values: Vec<ChangesetValue>) -> ChangesetRow {
        ChangesetRow {
            op: ChangeOp::Insert,
            indirect: false,
            old_values: Vec::new(),
            new_values: values,
        }
    }
    fn delete(values: Vec<ChangesetValue>) -> ChangesetRow {
        ChangesetRow {
            op: ChangeOp::Delete,
            indirect: false,
            old_values: values,
            new_values: Vec::new(),
        }
    }
    fn update(old: Vec<ChangesetValue>, new: Vec<ChangesetValue>) -> ChangesetRow {
        ChangesetRow {
            op: ChangeOp::Update,
            indirect: false,
            old_values: old,
            new_values: new,
        }
    }
    fn table(name: &str, pk: &[bool], rows: Vec<ChangesetRow>) -> TableChangeset {
        TableChangeset {
            info: TableInfo {
                name: name.to_owned(),
                column_count: pk.len(),
                pk_flags: pk.to_vec(),
            },
            rows,
        }
    }
    fn changes(rows: Vec<ChangesetRow>) -> Changeset {
        Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![table("t", &[true, false], rows)],
        }
    }
    async fn rows(conn: &Connection, sql: &str, width: usize) -> Vec<Vec<SqliteValue>> {
        conn.query(sql)
            .await
            .unwrap()
            .iter()
            .map(|row| {
                (0..width)
                    .map(|index| row.get(index).unwrap().clone())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn recorded_changes_reach_sql_and_an_inverse_restores_the_base() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1,'old'),(2,'gone')")
                .await
                .unwrap();
            let mut session = Session::new();
            session.attach_table("t", 2, vec![true, false]);
            session.record_update(
                "t",
                vec![int(1), string("old")],
                vec![int(1), string("new")],
            );
            session.record_delete("t", vec![int(2), string("gone")]);
            session.record_insert("t", vec![int(3), ChangesetValue::Blob(vec![0, 0xff, 0])]);
            let changeset = session.changeset();
            assert_eq!(
                apply_changeset(&mut conn, &changeset)
                    .await
                    .unwrap()
                    .applied,
                3
            );
            assert_eq!(
                rows(&conn, "SELECT * FROM t ORDER BY id", 2).await,
                vec![
                    vec![SqliteValue::Integer(1), SqliteValue::Text("new".into())],
                    vec![
                        SqliteValue::Integer(3),
                        SqliteValue::Blob(vec![0, 0xff, 0].into())
                    ],
                ]
            );
            assert!(!conn.in_transaction());
            apply_changeset(&mut conn, &changeset.invert().unwrap())
                .await
                .unwrap();
            assert_eq!(
                rows(&conn, "SELECT * FROM t ORDER BY id", 2).await,
                vec![
                    vec![SqliteValue::Integer(1), SqliteValue::Text("old".into())],
                    vec![SqliteValue::Integer(2), SqliteValue::Text("gone".into())],
                ]
            );
        });
    }

    #[test]
    fn sparse_updates_preserve_local_edits_and_extra_default_columns() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v, local, extra TEXT DEFAULT 'default')",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t VALUES(1,'old','local edit','retained')")
                .await
                .unwrap();
            let changeset = Changeset {
                kind: ChangesetKind::Changeset,
                tables: vec![table(
                    "t",
                    &[true, false, false],
                    vec![
                        update(
                            vec![int(1), string("old"), ChangesetValue::Undefined],
                            vec![
                                ChangesetValue::Undefined,
                                string("new"),
                                ChangesetValue::Undefined,
                            ],
                        ),
                        insert(vec![int(2), ChangesetValue::Null, string("remote")]),
                    ],
                )],
            };
            apply_changeset(&mut conn, &changeset).await.unwrap();
            assert_eq!(
                rows(&conn, "SELECT v,local,extra FROM t ORDER BY id", 3).await,
                vec![
                    vec![
                        SqliteValue::Text("new".into()),
                        SqliteValue::Text("local edit".into()),
                        SqliteValue::Text("retained".into())
                    ],
                    vec![
                        SqliteValue::Null,
                        SqliteValue::Text("remote".into()),
                        SqliteValue::Text("default".into())
                    ],
                ]
            );
        });
    }

    #[test]
    fn data_conflict_rolls_back_earlier_changes_across_tables() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE other(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1,'local')")
                .await
                .unwrap();
            let changeset = Changeset {
                kind: ChangesetKind::Changeset,
                tables: vec![
                    table(
                        "other",
                        &[true, false],
                        vec![insert(vec![int(9), string("must roll back")])],
                    ),
                    table(
                        "t",
                        &[true, false],
                        vec![delete(vec![int(1), string("stale")])],
                    ),
                ],
            };
            assert!(matches!(
                apply_changeset(&mut conn, &changeset).await,
                Err(SqlChangesetApplyError::Conflict {
                    kind: ConflictType::Data,
                    ..
                })
            ));
            assert!(rows(&conn, "SELECT * FROM other", 2).await.is_empty());
            assert_eq!(
                rows(&conn, "SELECT v FROM t", 1).await,
                vec![vec![SqliteValue::Text("local".into())]]
            );
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn missing_rows_and_duplicate_keys_are_explicit_conflicts() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1,'keep')")
                .await
                .unwrap();
            for (row, expected) in [
                (
                    insert(vec![int(1), string("overwrite")]),
                    ConflictType::Conflict,
                ),
                (
                    delete(vec![int(2), string("absent")]),
                    ConflictType::NotFound,
                ),
                (
                    update(
                        vec![int(2), string("absent")],
                        vec![ChangesetValue::Undefined, string("new")],
                    ),
                    ConflictType::NotFound,
                ),
            ] {
                assert!(
                    matches!(apply_changeset(&mut conn, &changes(vec![row])).await,
                    Err(SqlChangesetApplyError::Conflict { kind, .. }) if kind == expected)
                );
                assert!(!conn.in_transaction());
            }
            assert_eq!(
                rows(&conn, "SELECT * FROM t", 2).await,
                vec![vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("keep".into())
                ]]
            );
        });
    }

    #[test]
    fn target_ignore_policy_cannot_silently_discard_an_import() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE ON CONFLICT IGNORE)",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t VALUES(1,'occupied')")
                .await
                .unwrap();
            let changeset = changes(vec![
                insert(vec![int(2), string("temporary")]),
                insert(vec![int(3), string("occupied")]),
            ]);
            assert!(apply_changeset(&mut conn, &changeset).await.is_err());
            assert_eq!(
                rows(&conn, "SELECT * FROM t", 2).await,
                vec![vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("occupied".into())
                ]]
            );
        });
    }

    #[test]
    fn decoded_patchsets_skip_absent_old_non_key_values() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1,'local'),(2,'different')")
                .await
                .unwrap();
            let full = changes(vec![
                update(
                    vec![int(1), string("remote base")],
                    vec![ChangesetValue::Undefined, string("remote end")],
                ),
                delete(vec![int(2), string("other base")]),
            ]);
            let patch = Changeset::decode_patchset(&full.encode_patchset()).unwrap();
            assert_eq!(apply_changeset(&mut conn, &patch).await.unwrap().applied, 2);
            assert_eq!(
                rows(&conn, "SELECT * FROM t", 2).await,
                vec![vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("remote end".into())
                ]]
            );
        });
    }

    #[test]
    fn composite_keys_without_rowid_and_quoted_names_use_bound_values() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE \"odd\"\" table\"(\"key\"\"one\" TEXT, k2 INTEGER, v, PRIMARY KEY(\"key\"\"one\",k2)) WITHOUT ROWID").await.unwrap();
            let value = "'); DROP TABLE t; --";
            let initial = Changeset {
                kind: ChangesetKind::Changeset,
                tables: vec![table(
                    "odd\" table",
                    &[true, true, false],
                    vec![insert(vec![string("k'\""), int(7), string(value)])],
                )],
            };
            apply_changeset(&mut conn, &initial).await.unwrap();
            assert_eq!(
                rows(&conn, "SELECT v FROM \"odd\"\" table\"", 1).await,
                vec![vec![SqliteValue::Text(value.into())]]
            );
            apply_changeset(&mut conn, &initial.invert().unwrap())
                .await
                .unwrap();
            assert!(
                rows(&conn, "SELECT v FROM \"odd\"\" table\"", 1)
                    .await
                    .is_empty()
            );
        });
    }

    #[test]
    fn old_value_checks_use_sql_affinity_and_column_collation() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id TEXT PRIMARY KEY COLLATE NOCASE, v TEXT COLLATE NOCASE, n INTEGER)").await.unwrap();
            conn.execute("INSERT INTO t VALUES('Key','LOCAL',12)")
                .await
                .unwrap();
            let changeset = Changeset {
                kind: ChangesetKind::Changeset,
                tables: vec![table(
                    "t",
                    &[true, false, false],
                    vec![update(
                        vec![string("key"), string("local"), string("12")],
                        vec![ChangesetValue::Undefined, string("new"), int(13)],
                    )],
                )],
            };
            apply_changeset(&mut conn, &changeset).await.unwrap();
            assert_eq!(
                rows(&conn, "SELECT id,v,n FROM t", 3).await,
                vec![vec![
                    SqliteValue::Text("Key".into()),
                    SqliteValue::Text("new".into()),
                    SqliteValue::Integer(13),
                ]]
            );
        });
    }

    #[test]
    fn schema_mismatch_rejects_the_whole_apply_and_leaves_source_tables() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE incompatible(id, v PRIMARY KEY)")
                .await
                .unwrap();
            let changeset = Changeset {
                kind: ChangesetKind::Changeset,
                tables: vec![
                    table(
                        "t",
                        &[true, false],
                        vec![insert(vec![int(1), string("not committed")])],
                    ),
                    table(
                        "incompatible",
                        &[true, false],
                        vec![insert(vec![int(2), string("no")])],
                    ),
                ],
            };
            assert!(matches!(
                apply_changeset(&mut conn, &changeset).await,
                Err(SqlChangesetApplyError::Schema { .. })
            ));
            assert!(rows(&conn, "SELECT * FROM t", 2).await.is_empty());
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn a_caller_transaction_is_neither_committed_nor_rolled_back() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            conn.begin_transaction().await.unwrap();
            conn.execute("INSERT INTO t VALUES(1,'caller')")
                .await
                .unwrap();
            assert!(matches!(
                apply_changeset(
                    &mut conn,
                    &changes(vec![insert(vec![int(2), string("no")])])
                )
                .await,
                Err(SqlChangesetApplyError::Database(
                    FrankenError::NestedTransaction
                ))
            ));
            assert!(conn.in_transaction());
            assert_eq!(
                rows(&conn, "SELECT count(*) FROM t", 1).await,
                vec![vec![SqliteValue::Integer(1)]]
            );
            conn.commit_transaction().await.unwrap();
        });
    }

    #[test]
    fn invalid_public_rows_are_rejected_without_mutation_or_panics() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            for row in [
                insert(vec![int(1)]),
                insert(vec![int(1), ChangesetValue::Undefined]),
                insert(vec![ChangesetValue::Null, int(1)]),
                insert(vec![int(1), ChangesetValue::Real(f64::NAN)]),
                delete(vec![int(1), ChangesetValue::Undefined]),
                update(vec![int(1), int(2)], vec![int(1), int(3)]),
                update(
                    vec![int(1), ChangesetValue::Undefined],
                    vec![ChangesetValue::Undefined, int(3)],
                ),
            ] {
                assert!(matches!(
                    apply_changeset(&mut conn, &changes(vec![row])).await,
                    Err(SqlChangesetApplyError::InvalidChange { .. })
                ));
            }
            assert!(rows(&conn, "SELECT * FROM t", 2).await.is_empty());
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn dropped_apply_guard_records_rollback_before_next_sql() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            let guard = ApplyTransaction {
                conn: &conn,
                armed: true,
            };
            conn.begin_transaction().await.unwrap();
            conn.execute("INSERT INTO t VALUES(1,'abandoned')")
                .await
                .unwrap();
            drop(guard);
            assert!(rows(&conn, "SELECT * FROM t", 2).await.is_empty());
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn handler_resolves_data_keys_and_missing_rows_with_committed_counts() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1,'local'),(2,'occupied')")
                .await
                .unwrap();
            let mut incoming = changes(vec![
                update(
                    vec![int(1), string("stale")],
                    vec![ChangesetValue::Undefined, string("merged")],
                ),
                insert(vec![int(2), string("replacement")]),
                delete(vec![int(9), string("missing")]),
                insert(vec![int(3), string("fresh")]),
            ]);
            incoming.tables[0].rows[1].indirect = true;
            let mut observed = Vec::new();
            let report = apply_changeset_with_handler(&mut conn, &incoming, |conflict| {
                observed.push((conflict.kind, conflict.row, conflict.change.indirect));
                assert_eq!(conflict.table, "t");
                assert!(conflict.error.is_none());
                if conflict.kind == ConflictType::NotFound {
                    assert!(conflict.current.is_none());
                    ConflictAction::OmitChange
                } else {
                    assert!(conflict.current.is_some());
                    ConflictAction::Replace
                }
            })
            .await
            .unwrap();
            assert_eq!(
                observed,
                vec![
                    (ConflictType::Data, 0, false),
                    (ConflictType::Conflict, 1, true),
                    (ConflictType::NotFound, 2, false),
                ]
            );
            assert_eq!(
                report,
                SqlChangesetApplyReport {
                    applied: 3,
                    skipped: 1,
                    replaced: 2
                }
            );
            assert_eq!(
                rows(&conn, "SELECT v FROM t ORDER BY id", 1).await,
                vec![
                    vec![SqliteValue::Text("merged".into())],
                    vec![SqliteValue::Text("replacement".into())],
                    vec![SqliteValue::Text("fresh".into())],
                ]
            );
        });
    }

    #[test]
    fn failed_replacement_restores_the_original_and_never_deletes_an_unrelated_unique_row() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1,'original'),(2,'occupied')")
                .await
                .unwrap();
            conn.execute("CREATE TRIGGER log_delete AFTER DELETE ON t BEGIN INSERT INTO audit VALUES(old.id,old.v); END").await.unwrap();
            let incoming = changes(vec![
                insert(vec![int(1), string("occupied")]),
                insert(vec![int(3), string("later")]),
            ]);
            let mut observed = Vec::new();
            let report = apply_changeset_with_handler(&mut conn, &incoming, |conflict| {
                observed.push(conflict.kind);
                if conflict.kind == ConflictType::Conflict {
                    return ConflictAction::Replace;
                }
                assert_eq!(conflict.kind, ConflictType::Constraint);
                assert!(matches!(
                    conflict.error,
                    Some(FrankenError::UniqueViolation { .. })
                ));
                assert_eq!(
                    conflict.current.unwrap()[1],
                    SqliteValue::Text("original".into())
                );
                ConflictAction::OmitChange
            })
            .await
            .unwrap();
            assert_eq!(
                observed,
                vec![ConflictType::Conflict, ConflictType::Constraint]
            );
            assert_eq!(
                report,
                SqlChangesetApplyReport {
                    applied: 1,
                    skipped: 1,
                    replaced: 0
                }
            );
            assert!(rows(&conn, "SELECT * FROM audit", 2).await.is_empty());
            assert_eq!(
                rows(&conn, "SELECT * FROM t ORDER BY id", 2).await,
                vec![
                    vec![
                        SqliteValue::Integer(1),
                        SqliteValue::Text("original".into())
                    ],
                    vec![
                        SqliteValue::Integer(2),
                        SqliteValue::Text("occupied".into())
                    ],
                    vec![SqliteValue::Integer(3), SqliteValue::Text("later".into())],
                ]
            );
        });
    }

    #[test]
    fn omitted_constraint_preserves_earlier_and_later_accepted_rows() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL CHECK(v <> 'bad'))",
            )
            .await
            .unwrap();
            let incoming = changes(vec![
                insert(vec![int(1), string("before")]),
                insert(vec![int(2), ChangesetValue::Null]),
                insert(vec![int(3), string("bad")]),
                insert(vec![int(4), string("after")]),
            ]);
            let mut observed = 0;
            let report = apply_changeset_with_handler(&mut conn, &incoming, |conflict| {
                assert_eq!(conflict.kind, ConflictType::Constraint);
                assert!(conflict.error.is_some());
                observed += 1;
                ConflictAction::OmitChange
            })
            .await
            .unwrap();
            assert_eq!(observed, 2);
            assert_eq!(
                report,
                SqlChangesetApplyReport {
                    applied: 2,
                    skipped: 2,
                    replaced: 0
                }
            );
            assert_eq!(
                rows(&conn, "SELECT id FROM t ORDER BY id", 1).await,
                vec![vec![SqliteValue::Integer(1)], vec![SqliteValue::Integer(4)]]
            );
        });
    }

    #[test]
    fn replace_is_illegal_for_missing_rows_and_secondary_constraints() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1,'occupied')")
                .await
                .unwrap();
            for (row, kind) in [
                (
                    delete(vec![int(9), string("absent")]),
                    ConflictType::NotFound,
                ),
                (
                    insert(vec![int(8), string("occupied")]),
                    ConflictType::Constraint,
                ),
            ] {
                let incoming = changes(vec![insert(vec![int(2), string("must roll back")]), row]);
                assert!(
                    matches!(apply_changeset_with_handler(&mut conn, &incoming, |_| ConflictAction::Replace).await,
                    Err(SqlChangesetApplyError::InvalidResolution { kind: actual, .. }) if actual == kind)
                );
                assert_eq!(
                    rows(&conn, "SELECT * FROM t", 2).await,
                    vec![vec![
                        SqliteValue::Integer(1),
                        SqliteValue::Text("occupied".into())
                    ]]
                );
                assert!(!conn.in_transaction());
            }
        });
    }

    #[test]
    fn failed_data_replacement_can_be_omitted_without_changing_the_row() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1,'local'),(2,'occupied')")
                .await
                .unwrap();
            let incoming = changes(vec![update(
                vec![int(1), string("stale")],
                vec![ChangesetValue::Undefined, string("occupied")],
            )]);
            let mut kinds = Vec::new();
            let report = apply_changeset_with_handler(&mut conn, &incoming, |conflict| {
                kinds.push(conflict.kind);
                if conflict.kind == ConflictType::Data {
                    ConflictAction::Replace
                } else {
                    ConflictAction::OmitChange
                }
            })
            .await
            .unwrap();
            assert_eq!(kinds, vec![ConflictType::Data, ConflictType::Constraint]);
            assert_eq!(
                report,
                SqlChangesetApplyReport {
                    applied: 0,
                    skipped: 1,
                    replaced: 0
                }
            );
            assert_eq!(
                rows(&conn, "SELECT v FROM t ORDER BY id", 1).await,
                vec![
                    vec![SqliteValue::Text("local".into())],
                    vec![SqliteValue::Text("occupied".into())]
                ]
            );
        });
    }

    #[test]
    fn raising_fail_trigger_effects_are_restored_before_omitting_the_row() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            conn.execute("CREATE TRIGGER reject_row AFTER INSERT ON t WHEN new.v='reject' BEGIN INSERT INTO audit VALUES(new.id); SELECT RAISE(FAIL,'row rejected'); END").await.unwrap();
            let incoming = changes(vec![
                insert(vec![int(1), string("reject")]),
                insert(vec![int(2), string("accepted")]),
            ]);
            let mut calls = 0;
            let report = apply_changeset_with_handler(&mut conn, &incoming, |conflict| {
                calls += 1;
                assert_eq!(conflict.kind, ConflictType::Constraint);
                assert!(matches!(conflict.error, Some(FrankenError::RaiseFail(_))));
                ConflictAction::OmitChange
            })
            .await
            .unwrap();
            assert_eq!(calls, 1);
            assert_eq!(
                report,
                SqlChangesetApplyReport {
                    applied: 1,
                    skipped: 1,
                    replaced: 0
                }
            );
            assert!(rows(&conn, "SELECT * FROM audit", 1).await.is_empty());
            assert_eq!(
                rows(&conn, "SELECT id FROM t", 1).await,
                vec![vec![SqliteValue::Integer(2)]]
            );
        });
    }

    #[test]
    fn raising_rollback_is_fatal_and_cannot_be_omitted() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            conn.execute("CREATE TRIGGER reject_all BEFORE INSERT ON t WHEN new.v='reject' BEGIN SELECT RAISE(ROLLBACK,'transaction rejected'); END").await.unwrap();
            let incoming = changes(vec![
                insert(vec![int(1), string("earlier")]),
                insert(vec![int(2), string("reject")]),
            ]);
            let mut calls = 0;
            assert!(
                apply_changeset_with_handler(&mut conn, &incoming, |_| {
                    calls += 1;
                    ConflictAction::OmitChange
                })
                .await
                .is_err()
            );
            assert_eq!(calls, 0);
            assert!(rows(&conn, "SELECT * FROM t", 2).await.is_empty());
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn deferred_foreign_key_failure_does_not_return_a_success_receipt() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
            conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)").await.unwrap();
            let incoming = changes(vec![insert(vec![int(1), int(9)])]);
            let mut calls = 0;
            assert!(
                apply_changeset_with_handler(&mut conn, &incoming, |_| {
                    calls += 1;
                    ConflictAction::OmitChange
                })
                .await
                .is_err()
            );
            assert_eq!(
                calls, 0,
                "commit failure must not become an omittable row conflict"
            );
            assert!(rows(&conn, "SELECT * FROM t", 2).await.is_empty());
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn immediate_foreign_key_constraints_can_be_omitted_without_disabling_enforcement() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
            conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            conn.execute("INSERT INTO parent VALUES(1)").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER REFERENCES parent(id))")
                .await
                .unwrap();
            let incoming = changes(vec![
                insert(vec![int(1), int(9)]),
                insert(vec![int(2), int(1)]),
            ]);
            let report = apply_changeset_with_handler(&mut conn, &incoming, |conflict| {
                assert_eq!(conflict.kind, ConflictType::Constraint);
                assert!(matches!(
                    conflict.error,
                    Some(FrankenError::ForeignKeyViolation)
                ));
                ConflictAction::OmitChange
            })
            .await
            .unwrap();
            assert_eq!(
                report,
                SqlChangesetApplyReport {
                    applied: 1,
                    skipped: 1,
                    replaced: 0
                }
            );
            assert!(conn.execute("INSERT INTO t VALUES(3,99)").await.is_err());
            assert_eq!(
                rows(&conn, "SELECT id FROM t", 1).await,
                vec![vec![SqliteValue::Integer(2)]]
            );
        });
    }

    #[test]
    fn handler_panic_leaves_a_deferred_rollback_obligation() {
        use std::future::{Future, poll_fn};
        use std::panic::{AssertUnwindSafe, catch_unwind};
        use std::task::Poll;

        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1,'original')")
                .await
                .unwrap();
            let incoming = changes(vec![
                insert(vec![int(2), string("abandoned")]),
                insert(vec![int(1), string("conflict")]),
            ]);
            let mut future = Box::pin(apply_changeset_with_handler(&mut conn, &incoming, |_| {
                std::panic::panic_any("changeset callback panic sentinel")
            }));
            let result =
                poll_fn(
                    |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                        Ok(Poll::Pending) => Poll::Pending,
                        Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
                        Err(panic) => Poll::Ready(Err(panic)),
                    },
                )
                .await;
            assert!(result.is_err());
            drop(future);
            assert_eq!(
                rows(&conn, "SELECT * FROM t", 2).await,
                vec![vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("original".into())
                ]]
            );
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn resource_io_and_generic_sql_errors_are_never_omittable_row_constraints() {
        for error in [
            FrankenError::Interrupt,
            FrankenError::OutOfMemory,
            FrankenError::ReadOnly,
            FrankenError::Io(std::io::Error::other(
                "CHECK constraint failed: not actually a constraint",
            )),
            FrankenError::FunctionError("FOREIGN KEY constraint failed".to_owned()),
            FrankenError::Busy,
            FrankenError::DatabaseFull,
        ] {
            assert!(!is_row_constraint(&error));
        }
        assert!(is_row_constraint(&FrankenError::RaiseFail(
            "trigger rejection".to_owned()
        )));
        assert!(is_row_constraint(&FrankenError::ForeignKeyViolation));
    }

    #[cfg(feature = "native")]
    #[test]
    fn applied_rows_and_index_entries_survive_reopen() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("applied.db");
            let mut conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX by_v ON t(v)").await.unwrap();
            apply_changeset(
                &mut conn,
                &changes(vec![insert(vec![int(42), string("durable")])]),
            )
            .await
            .unwrap();
            conn.close().await.unwrap();
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            assert_eq!(
                rows(
                    &conn,
                    "SELECT id FROM t INDEXED BY by_v WHERE v='durable'",
                    1
                )
                .await,
                vec![vec![SqliteValue::Integer(42)]]
            );
            conn.close().await.unwrap();
        });
    }
}

#[cfg(all(test, feature = "native", not(target_arch = "wasm32")))]
mod streaming_apply_tests {
    use super::streaming::{StreamApplyError, apply, apply_verified, apply_with_handler};
    use super::*;
    use crate::compat::changeset_stream::{ChangesetStreamError, ChangesetStreamLimits};
    use asupersync::io::{AsyncRead, ReadBuf};
    use fsqlite_ext_session::TableInfo;
    use fsqlite_types::cx::Cx;
    use sha2::{Digest, Sha256};
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
        reads: Rc<Cell<usize>>,
        at_end: Rc<Cell<bool>>,
        fail_at_end: bool,
        stall_at_end: bool,
    }

    impl Input {
        fn new(bytes: Vec<u8>, chunk: usize) -> Self {
            Self {
                bytes,
                offset: 0,
                chunk,
                reads: Rc::new(Cell::new(0)),
                at_end: Rc::new(Cell::new(false)),
                fail_at_end: false,
                stall_at_end: false,
            }
        }
    }

    impl AsyncRead for Input {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.reads.set(this.reads.get() + 1);
            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            if this.offset == this.bytes.len() {
                this.at_end.set(true);
                if this.fail_at_end {
                    return Poll::Ready(Err(io::Error::other("input failed after a complete row")));
                }
                if this.stall_at_end {
                    return Poll::Pending;
                }
            }
            let n = this
                .chunk
                .min(buf.remaining())
                .min(this.bytes.len() - this.offset);
            buf.put_slice(&this.bytes[this.offset..this.offset + n]);
            this.offset += n;
            Poll::Ready(Ok(()))
        }
    }

    // A transport-owned digest: production does not depend on a second hash
    // implementation or read the source again after applying it. sha2 is an
    // existing dev dependency, used here to exercise a real completion check.
    struct HashedInput {
        input: Input,
        digest: Sha256,
    }

    impl HashedInput {
        fn new(bytes: Vec<u8>, chunk: usize) -> Self {
            Self {
                input: Input::new(bytes, chunk),
                digest: Sha256::new(),
            }
        }

        fn check_digest(&self, expected: [u8; 32]) -> Result<(), String> {
            let actual: [u8; 32] = self.digest.clone().finalize().into();
            if actual == expected {
                Ok(())
            } else {
                Err("SHA-256 mismatch".to_owned())
            }
        }
    }

    impl AsyncRead for HashedInput {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            let before = buf.filled().len();
            match Pin::new(&mut this.input).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    this.digest.update(&buf.filled()[before..]);
                    Poll::Ready(Ok(()))
                }
                other => other,
            }
        }
    }

    fn insert(id: i64, value: &str) -> ChangesetRow {
        ChangesetRow {
            op: ChangeOp::Insert,
            indirect: false,
            old_values: Vec::new(),
            new_values: vec![
                ChangesetValue::Integer(id),
                ChangesetValue::Text(value.to_owned()),
            ],
        }
    }

    fn table(name: &str, rows: Vec<ChangesetRow>) -> TableChangeset {
        TableChangeset {
            info: TableInfo {
                name: name.to_owned(),
                column_count: 2,
                pk_flags: vec![true, false],
            },
            rows,
        }
    }

    fn wire(tables: Vec<TableChangeset>) -> Vec<u8> {
        Changeset {
            kind: ChangesetKind::Changeset,
            tables,
        }
        .encode()
    }

    async fn setup() -> Connection {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .unwrap();
        conn.execute("CREATE TRIGGER log_t AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(NEW.id, NEW.v); END").await.unwrap();
        conn
    }

    async fn count(conn: &Connection, table: &str) -> i64 {
        let rows = conn
            .query(&format!("SELECT count(*) FROM {table}"))
            .await
            .unwrap();
        let Some(SqliteValue::Integer(n)) = rows[0].get(0) else {
            panic!("integer count");
        };
        *n
    }

    async fn assert_empty(conn: &Connection) {
        assert_eq!(count(conn, "t").await, 0);
        assert_eq!(count(conn, "audit").await, 0);
        assert!(!conn.in_transaction());
    }

    #[test]
    fn fragmented_streams_commit_rows_and_trigger_effects_once() {
        asupersync::test_utils::run_test(|| async {
            for chunk in [1, 2, 7, 8192] {
                let mut conn = setup().await;
                let bytes = wire(vec![table(
                    "t",
                    vec![insert(1, "alpha"), insert(2, "nul\0λ")],
                )]);
                let report = apply(
                    &mut conn,
                    &Cx::new(),
                    Input::new(bytes, chunk),
                    ChangesetStreamLimits::default(),
                )
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
                assert_eq!(count(&conn, "t").await, 2);
                assert_eq!(count(&conn, "audit").await, 2);
                assert_eq!(
                    conn.query_row("SELECT v FROM t WHERE id=2")
                        .await
                        .unwrap()
                        .get(0),
                    Some(&SqliteValue::Text("nul\0λ".into()))
                );
                assert!(!conn.in_transaction());
            }
        });
    }

    #[test]
    fn changeset_and_patchset_update_delete_use_the_same_sql_executor() {
        asupersync::test_utils::run_test(|| async {
            let incoming = Changeset {
                kind: ChangesetKind::Changeset,
                tables: vec![table(
                    "t",
                    vec![
                        ChangesetRow {
                            op: ChangeOp::Update,
                            indirect: false,
                            old_values: vec![
                                ChangesetValue::Integer(1),
                                ChangesetValue::Text("old".to_owned()),
                            ],
                            new_values: vec![
                                ChangesetValue::Undefined,
                                ChangesetValue::Text("new".to_owned()),
                            ],
                        },
                        ChangesetRow {
                            op: ChangeOp::Delete,
                            indirect: true,
                            old_values: vec![
                                ChangesetValue::Integer(2),
                                ChangesetValue::Text("gone".to_owned()),
                            ],
                            new_values: Vec::new(),
                        },
                    ],
                )],
            };
            for patchset in [false, true] {
                let mut conn = setup().await;
                conn.execute("INSERT INTO t VALUES(1,'old'),(2,'gone')")
                    .await
                    .unwrap();
                let bytes = if patchset {
                    incoming.encode_patchset()
                } else {
                    incoming.encode()
                };
                let report = apply(
                    &mut conn,
                    &Cx::new(),
                    Input::new(bytes, 1),
                    ChangesetStreamLimits::default(),
                )
                .await
                .unwrap();
                assert_eq!(report.applied, 2);
                assert_eq!(count(&conn, "t").await, 1);
                assert_eq!(
                    conn.query_row("SELECT v FROM t WHERE id=1")
                        .await
                        .unwrap()
                        .get(0),
                    Some(&SqliteValue::Text("new".into()))
                );
            }
        });
    }

    #[test]
    fn late_malformed_truncated_and_mixed_wire_input_undo_the_prefix() {
        asupersync::test_utils::run_test(|| async {
            let prefix = wire(vec![table("t", vec![insert(1, "first")])]);
            for tail in [vec![255], vec![18, 0, 1, 0], vec![b'P', 2, 1, 0, b't', 0]] {
                let mut bytes = prefix.clone();
                bytes.extend_from_slice(&tail);
                let mut conn = setup().await;
                assert!(matches!(
                    apply(
                        &mut conn,
                        &Cx::new(),
                        Input::new(bytes, 1),
                        ChangesetStreamLimits::default()
                    )
                    .await,
                    Err(StreamApplyError::Input(_))
                ));
                assert_empty(&conn).await;
            }
        });
    }

    #[test]
    fn input_error_after_a_complete_row_rolls_back_instead_of_committing_eof() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let mut input = Input::new(wire(vec![table("t", vec![insert(1, "first")])]), 1);
            input.fail_at_end = true;
            assert!(matches!(
                apply(
                    &mut conn,
                    &Cx::new(),
                    input,
                    ChangesetStreamLimits::default()
                )
                .await,
                Err(StreamApplyError::Input(ChangesetStreamError::Io(_)))
            ));
            assert_empty(&conn).await;
        });
    }

    #[test]
    fn late_resource_limit_failure_undoes_all_sql_and_trigger_writes() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let bytes = wire(vec![table(
                "t",
                vec![insert(1, "first"), insert(2, "second")],
            )]);
            let limits = ChangesetStreamLimits {
                max_rows: 1,
                ..ChangesetStreamLimits::default()
            };
            assert!(matches!(
                apply(&mut conn, &Cx::new(), Input::new(bytes, 1), limits).await,
                Err(StreamApplyError::Input(ChangesetStreamError::Limit {
                    resource: "rows",
                    ..
                }))
            ));
            assert_empty(&conn).await;
        });
    }

    #[test]
    fn late_schema_mismatch_including_repeated_table_layout_rolls_back() {
        asupersync::test_utils::run_test(|| async {
            for repeated in [false, true] {
                let mut conn = setup().await;
                let mut second = table(
                    if repeated { "t" } else { "absent" },
                    vec![insert(2, "second")],
                );
                if repeated {
                    second.info.pk_flags = vec![false, true];
                }
                let bytes = wire(vec![table("t", vec![insert(1, "first")]), second]);
                assert!(matches!(
                    apply(
                        &mut conn,
                        &Cx::new(),
                        Input::new(bytes, 2),
                        ChangesetStreamLimits::default()
                    )
                    .await,
                    Err(StreamApplyError::Sql(SqlChangesetApplyError::Schema { .. }))
                ));
                assert_empty(&conn).await;
            }
        });
    }

    #[test]
    fn semantic_validation_retains_the_original_row_index_and_cannot_be_omitted() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let mut invalid = insert(2, "invalid");
            invalid.new_values[1] = ChangesetValue::Undefined;
            let bytes = wire(vec![table("t", vec![insert(1, "first"), invalid])]);
            let mut callbacks = 0;
            let error = apply_with_handler(
                &mut conn,
                &Cx::new(),
                Input::new(bytes, 1),
                ChangesetStreamLimits::default(),
                |_| {
                    callbacks += 1;
                    ConflictAction::OmitChange
                },
            )
            .await
            .unwrap_err();
            assert!(matches!(
                error,
                StreamApplyError::Sql(SqlChangesetApplyError::InvalidChange { row: 1, .. })
            ));
            assert_eq!(callbacks, 0);
            assert_empty(&conn).await;
        });
    }

    #[test]
    fn row_conflicts_omit_only_rejected_effects_and_preserve_section_indices() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let bytes = wire(vec![
                table(
                    "t",
                    vec![insert(1, "first"), insert(2, "first"), insert(3, "third")],
                ),
                table("t", vec![insert(1, "duplicate"), insert(4, "fourth")]),
            ]);
            let mut seen = Vec::new();
            let report = apply_with_handler(
                &mut conn,
                &Cx::new(),
                Input::new(bytes, 1),
                ChangesetStreamLimits::default(),
                |conflict| {
                    seen.push((conflict.kind, conflict.row));
                    ConflictAction::OmitChange
                },
            )
            .await
            .unwrap();
            assert_eq!(
                seen,
                vec![(ConflictType::Constraint, 1), (ConflictType::Conflict, 0)]
            );
            assert_eq!(
                report,
                SqlChangesetApplyReport {
                    applied: 3,
                    skipped: 2,
                    replaced: 0
                }
            );
            assert_eq!(count(&conn, "t").await, 3);
            assert_eq!(count(&conn, "audit").await, 3);
        });
    }

    #[test]
    fn caller_transaction_is_refused_without_consuming_input() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            conn.begin_transaction().await.unwrap();
            conn.execute("INSERT INTO t VALUES(9,'caller')")
                .await
                .unwrap();
            let input = Input::new(wire(vec![table("t", vec![insert(1, "remote")])]), 1);
            let reads = Rc::clone(&input.reads);
            assert!(matches!(
                apply(
                    &mut conn,
                    &Cx::new(),
                    input,
                    ChangesetStreamLimits::default()
                )
                .await,
                Err(StreamApplyError::Sql(SqlChangesetApplyError::Database(
                    FrankenError::NestedTransaction
                )))
            ));
            assert_eq!(reads.get(), 0);
            assert!(conn.in_transaction());
            conn.commit_transaction().await.unwrap();
            assert_eq!(count(&conn, "t").await, 1);
        });
    }

    #[test]
    fn dropped_stream_waiting_for_more_input_cannot_publish_its_prefix() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let mut input = Input::new(wire(vec![table("t", vec![insert(1, "abandoned")])]), 1);
            input.stall_at_end = true;
            let at_end = Rc::clone(&input.at_end);
            let cx = Cx::new();
            let mut operation = Box::pin(apply(
                &mut conn,
                &cx,
                input,
                ChangesetStreamLimits::default(),
            ));
            poll_fn(|task_cx| {
                assert!(
                    operation.as_mut().poll(task_cx).is_pending(),
                    "input must suspend after the first row"
                );
                if at_end.get() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            drop(operation);
            // This is actual cancellation of the public future after its first
            // row was applied, not a direct construction of a cleanup guard.
            assert_empty(&conn).await;
            let report = apply(
                &mut conn,
                &Cx::new(),
                Input::new(wire(vec![table("t", vec![insert(2, "next")])]), 1),
                ChangesetStreamLimits::default(),
            )
            .await
            .unwrap();
            assert_eq!(report.applied, 1);
            assert_eq!(count(&conn, "t").await, 1);
        });
    }

    #[test]
    fn cancelled_context_and_empty_stream_do_not_modify_sql() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            assert_eq!(
                apply(
                    &mut conn,
                    &Cx::new(),
                    Input::new(Vec::new(), 1),
                    ChangesetStreamLimits::default()
                )
                .await
                .unwrap(),
                SqlChangesetApplyReport::default()
            );
            let cx = Cx::new();
            cx.cancel();
            let input = Input::new(wire(vec![table("t", vec![insert(1, "cancelled")])]), 1);
            let reads = Rc::clone(&input.reads);
            assert!(matches!(
                apply(&mut conn, &cx, input, ChangesetStreamLimits::default()).await,
                Err(StreamApplyError::Input(ChangesetStreamError::Cancelled))
            ));
            assert_eq!(reads.get(), 0);
            assert_empty(&conn).await;
        });
    }

    #[test]
    fn streamed_file_backed_changes_survive_close_and_reopen() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("streamed.db");
            let mut conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
                .await
                .unwrap();
            let bytes = wire(vec![table(
                "t",
                vec![insert(1, "alpha"), insert(2, "beta")],
            )]);
            assert_eq!(
                apply(
                    &mut conn,
                    &Cx::new(),
                    Input::new(bytes, 1),
                    ChangesetStreamLimits::default()
                )
                .await
                .unwrap()
                .applied,
                2
            );
            conn.close().await.unwrap();
            let reopened = Connection::open(path.to_str().unwrap()).await.unwrap();
            assert_eq!(count(&reopened, "t").await, 2);
            assert_eq!(
                reopened
                    .query_row("PRAGMA integrity_check")
                    .await
                    .unwrap()
                    .get(0),
                Some(&SqliteValue::Text("ok".into()))
            );
            reopened.close().await.unwrap();
        });
    }

    #[test]
    fn exact_message_is_verified_once_from_the_actual_fragmented_input() {
        asupersync::test_utils::run_test(|| async {
            for chunk in [1, 3, 8192] {
                let mut conn = setup().await;
                let bytes = wire(vec![table(
                    "t",
                    vec![insert(1, "alpha"), insert(2, "beta")],
                )]);
                let expected: [u8; 32] = Sha256::digest(&bytes).into();
                let length = u64::try_from(bytes.len()).unwrap();
                let mut checks = 0;
                let report = apply_verified(
                    &mut conn,
                    &Cx::new(),
                    HashedInput::new(bytes, chunk),
                    ChangesetStreamLimits::default(),
                    length,
                    |input| {
                        checks += 1;
                        assert!(input.input.at_end.get(), "verify only after actual EOF");
                        input.check_digest(expected)
                    },
                )
                .await
                .unwrap();
                assert_eq!(checks, 1);
                assert_eq!(report.applied, 2);
                assert_eq!(count(&conn, "t").await, 2);
                assert_eq!(count(&conn, "audit").await, 2);
            }
        });
    }

    #[test]
    fn digest_rejection_of_well_formed_sql_rolls_back_the_whole_message() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let intended = wire(vec![table(
                "t",
                vec![insert(1, "alpha"), insert(2, "beta")],
            )]);
            let altered = wire(vec![table(
                "t",
                vec![insert(1, "ALPHA"), insert(2, "beta")],
            )]);
            assert_eq!(intended.len(), altered.len());
            let expected: [u8; 32] = Sha256::digest(&intended).into();
            let length = u64::try_from(intended.len()).unwrap();
            let error = apply_verified(
                &mut conn,
                &Cx::new(),
                HashedInput::new(altered, 1),
                ChangesetStreamLimits::default(),
                length,
                |input| input.check_digest(expected),
            )
            .await
            .unwrap_err();
            assert!(matches!(error, StreamApplyError::Verification { .. }));
            assert_empty(&conn).await;
        });
    }

    #[test]
    fn a_missing_final_row_is_not_accepted_as_a_complete_verified_message() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let complete = wire(vec![table(
                "t",
                vec![insert(1, "first"), insert(2, "second")],
            )]);
            let prefix = wire(vec![table("t", vec![insert(1, "first")])]);
            let length = u64::try_from(complete.len()).unwrap();
            let prefix_length = u64::try_from(prefix.len()).unwrap();
            let mut checks = 0;
            let result = apply_verified(
                &mut conn,
                &Cx::new(),
                HashedInput::new(prefix, 1),
                ChangesetStreamLimits::default(),
                length,
                |_| {
                    checks += 1;
                    Ok(())
                },
            )
            .await;
            assert!(
                matches!(result, Err(StreamApplyError::LengthMismatch { expected, consumed })
                if expected == length && consumed == prefix_length)
            );
            assert_eq!(
                checks, 0,
                "a completion callback cannot waive missing bytes"
            );
            assert_empty(&conn).await;
        });
    }

    #[test]
    fn extra_valid_rows_and_oversized_declarations_cannot_extend_the_boundary() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let complete = wire(vec![table(
                "t",
                vec![insert(1, "first"), insert(2, "extra")],
            )]);
            let first = wire(vec![table("t", vec![insert(1, "first")])]);
            let mut checks = 0;
            let result = apply_verified(
                &mut conn,
                &Cx::new(),
                Input::new(complete, 8192),
                ChangesetStreamLimits::default(),
                u64::try_from(first.len()).unwrap(),
                |_| {
                    checks += 1;
                    Ok(())
                },
            )
            .await;
            assert!(matches!(
                result,
                Err(StreamApplyError::Input(ChangesetStreamError::Limit {
                    resource: "input bytes",
                    ..
                }))
            ));
            assert_eq!(checks, 0);
            assert_empty(&conn).await;

            let input = Input::new(first, 1);
            let reads = Rc::clone(&input.reads);
            let limits = ChangesetStreamLimits {
                max_input_bytes: 1,
                ..ChangesetStreamLimits::default()
            };
            assert!(matches!(
                apply_verified(&mut conn, &Cx::new(), input, limits, 2, |_| Ok(())).await,
                Err(StreamApplyError::Input(ChangesetStreamError::Limit {
                    offset: 0,
                    resource: "declared input bytes"
                }))
            ));
            assert_eq!(reads.get(), 0);
            assert_empty(&conn).await;
        });
    }

    #[test]
    fn empty_and_header_only_messages_still_require_verification() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            for bytes in [Vec::new(), wire(vec![table("t", Vec::new())])] {
                let length = u64::try_from(bytes.len()).unwrap();
                let expected: [u8; 32] = Sha256::digest(&bytes).into();
                let mut checks = 0;
                let report = apply_verified(
                    &mut conn,
                    &Cx::new(),
                    HashedInput::new(bytes.clone(), 1),
                    ChangesetStreamLimits::default(),
                    length,
                    |input| {
                        checks += 1;
                        input.check_digest(expected)
                    },
                )
                .await
                .unwrap();
                assert_eq!(checks, 1);
                assert_eq!(report, SqlChangesetApplyReport::default());
                assert!(matches!(
                    apply_verified(
                        &mut conn,
                        &Cx::new(),
                        HashedInput::new(bytes, 1),
                        ChangesetStreamLimits::default(),
                        length,
                        |_| Err("untrusted envelope".to_owned())
                    )
                    .await,
                    Err(StreamApplyError::Verification { .. })
                ));
                assert_empty(&conn).await;
            }
        });
    }

    #[test]
    fn completion_callback_panic_and_cancellation_preserve_rollback_ownership() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        asupersync::test_utils::run_test(|| async {
            let mut conn = setup().await;
            let bytes = wire(vec![table("t", vec![insert(1, "abandoned")])]);
            let length = u64::try_from(bytes.len()).unwrap();
            let cx = Cx::new();
            let mut operation = Box::pin(apply_verified(
                &mut conn,
                &cx,
                Input::new(bytes.clone(), 1),
                ChangesetStreamLimits::default(),
                length,
                |_| panic!("completion verifier panic"),
            ));
            let panicked = poll_fn(|task_cx| {
                match catch_unwind(AssertUnwindSafe(|| operation.as_mut().poll(task_cx))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(_)) => Poll::Ready(false),
                    Err(_) => Poll::Ready(true),
                }
            })
            .await;
            assert!(panicked);
            drop(operation);
            assert_empty(&conn).await;

            let result = apply_verified(
                &mut conn,
                &cx,
                Input::new(bytes, 1),
                ChangesetStreamLimits::default(),
                length,
                |_| {
                    cx.cancel();
                    Ok(())
                },
            )
            .await;
            assert!(matches!(
                result,
                Err(StreamApplyError::Input(ChangesetStreamError::Cancelled))
            ));
            assert_empty(&conn).await;
        });
    }

    #[test]
    fn a_verified_message_still_has_to_pass_the_database_commit() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
            conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)")
                .await.unwrap();
            let bytes = wire(vec![table("t", vec![insert(1, "99")])]);
            let length = u64::try_from(bytes.len()).unwrap();
            let expected: [u8; 32] = Sha256::digest(&bytes).into();
            let mut checks = 0;
            let result = apply_verified(
                &mut conn,
                &Cx::new(),
                HashedInput::new(bytes, 1),
                ChangesetStreamLimits::default(),
                length,
                |input| {
                    checks += 1;
                    input.check_digest(expected)
                },
            )
            .await;
            assert_eq!(
                checks, 1,
                "the completed message was verified before COMMIT"
            );
            assert!(matches!(result, Err(StreamApplyError::Sql(_))));
            assert_eq!(count(&conn, "t").await, 0);
            assert!(!conn.in_transaction());
        });
    }
}
