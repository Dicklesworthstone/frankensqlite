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

use std::fmt;

use fsqlite_ast::Statement;
use fsqlite_ext_session::{ChangeOp, Changeset, ChangesetKind, ChangesetRow, ChangesetValue, ConflictType, TableChangeset};
use fsqlite_parser::Parser;

use crate::{Connection, FrankenError, Row, SqliteValue};

/// Counts become a receipt only after the enclosing transaction commits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqlChangesetApplyReport {
    pub applied: usize,
}

/// A rejected apply, including errors from the storage transaction itself.
/// Database/commit errors retain the engine's outcome information: an I/O
/// error is not, by itself, proof that no durable effects occurred.
#[derive(Debug)]
pub enum SqlChangesetApplyError {
    InvalidChange { table: String, row: usize, detail: &'static str },
    Schema { table: String, detail: &'static str },
    Conflict { table: String, row: usize, kind: ConflictType },
    Database(FrankenError),
    Rollback { cause: Box<Self>, rollback: FrankenError },
}

impl fmt::Display for SqlChangesetApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidChange { table, row, detail } => write!(f, "invalid changeset for {table:?}, row {row}: {detail}"),
            Self::Schema { table, detail } => write!(f, "incompatible changeset target {table:?}: {detail}"),
            Self::Conflict { table, row, kind } => write!(f, "changeset conflict in {table:?}, row {row}: {kind:?}"),
            Self::Database(error) => write!(f, "{error}"),
            Self::Rollback { cause, rollback } => write!(f, "{cause}; rollback also failed: {rollback}"),
        }
    }
}

impl std::error::Error for SqlChangesetApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::Rollback { cause, .. } => Some(cause.as_ref()),
            _ => None,
        }
    }
}

impl From<FrankenError> for SqlChangesetApplyError {
    fn from(error: FrankenError) -> Self { Self::Database(error) }
}

type ApplyResult<T> = Result<T, SqlChangesetApplyError>;

fn invalid(table: &TableChangeset, row: usize, detail: &'static str) -> SqlChangesetApplyError {
    SqlChangesetApplyError::InvalidChange { table: table.info.name.clone(), row, detail }
}

fn schema(name: &str, detail: &'static str) -> SqlChangesetApplyError {
    SqlChangesetApplyError::Schema { table: name.to_owned(), detail }
}

const fn undefined(value: &ChangesetValue) -> bool { matches!(value, ChangesetValue::Undefined) }

/// Public changeset structs are constructible without passing through a wire
/// decoder. Validate every row before the first SQL statement, including the
/// slots which would otherwise silently turn Undefined into SQL NULL.
fn validate(changeset: &Changeset) -> ApplyResult<()> {
    for table in &changeset.tables {
        let info = &table.info;
        if info.name.is_empty() || info.name.contains('\0')
            || info.name.get(..7).is_some_and(|prefix| prefix.eq_ignore_ascii_case("sqlite_"))
            || info.column_count == 0 || info.pk_flags.len() != info.column_count
            || !info.pk_flags.iter().any(|pk| *pk)
        {
            return Err(invalid(table, 0, "invalid name, column count or primary-key layout"));
        }
        for (index, row) in table.rows.iter().enumerate() {
            let n = info.column_count;
            let width_ok = match row.op {
                ChangeOp::Insert => row.old_values.is_empty() && row.new_values.len() == n,
                ChangeOp::Delete => row.old_values.len() == n && row.new_values.is_empty(),
                ChangeOp::Update => row.old_values.len() == n && row.new_values.len() == n,
            };
            if !width_ok { return Err(invalid(table, index, "invalid row width")); }
            if row.old_values.iter().chain(&row.new_values)
                .any(|value| matches!(value, ChangesetValue::Real(number) if number.is_nan()))
            {
                return Err(invalid(table, index, "NaN must be normalized before recording a change"));
            }
            let key = if row.op == ChangeOp::Insert { &row.new_values } else { &row.old_values };
            if info.pk_flags.iter().zip(key).any(|(pk, value)| {
                *pk && matches!(value, ChangesetValue::Undefined | ChangesetValue::Null)
            }) {
                return Err(invalid(table, index, "primary-key values must be defined and non-NULL"));
            }
            match row.op {
                ChangeOp::Insert if row.new_values.iter().any(undefined) => {
                    return Err(invalid(table, index, "INSERT contains an undefined value"));
                }
                ChangeOp::Delete => {
                    for (pk, old) in info.pk_flags.iter().zip(&row.old_values) {
                        let valid = if changeset.kind == ChangesetKind::Changeset || *pk {
                            !undefined(old)
                        } else { undefined(old) };
                        if !valid { return Err(invalid(table, index, "invalid DELETE old-value layout")); }
                    }
                }
                ChangeOp::Update => {
                    let mut modified = false;
                    for ((pk, old), new) in info.pk_flags.iter().zip(&row.old_values).zip(&row.new_values) {
                        if *pk {
                            if !undefined(new) {
                                return Err(invalid(table, index, "primary-key changes must be DELETE plus INSERT"));
                            }
                        } else {
                            let valid = match changeset.kind {
                                ChangesetKind::Changeset => undefined(old) == undefined(new),
                                ChangesetKind::Patchset => undefined(old),
                            };
                            if !valid { return Err(invalid(table, index, "invalid UPDATE old/new value layout")); }
                            modified |= !undefined(new);
                        }
                    }
                    if !modified { return Err(invalid(table, index, "UPDATE has no changed non-key column")); }
                }
                ChangeOp::Insert => {}
            }
        }
    }
    Ok(())
}

fn quote_identifier(name: &str) -> String { format!("\"{}\"", name.replace('"', "\"\"")) }

fn text(row: &Row, index: usize) -> Option<&str> {
    match row.get(index) { Some(SqliteValue::Text(value)) => Some(value), _ => None }
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
        if entries.len() != 1 { return Err(schema(name, "ordinary main table does not exist")); }
        let canonical = text(&entries[0], 0).ok_or_else(|| schema(name, "missing catalog name"))?;
        let sql = text(&entries[0], 1).ok_or_else(|| schema(name, "missing CREATE TABLE statement"))?;
        let (statements, errors) = Parser::from_sql(sql).parse_all();
        if !errors.is_empty() || !matches!(statements.as_slice(), [Statement::CreateTable(_)]) {
            return Err(schema(name, "only ordinary tables are supported"));
        }
        let columns = conn.query(&format!("PRAGMA main.table_xinfo({})", quote_identifier(canonical))).await?;
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
                return Err(schema(name, "hidden or generated columns are not supported"));
            }
            if (*pk > 0) != table.info.pk_flags.get(index).copied().unwrap_or(false) {
                return Err(schema(name, "primary-key columns differ"));
            }
            if index < table.info.column_count {
                let column_name = text(column, 1).ok_or_else(|| schema(name, "missing column name"))?;
                if column_name.contains('\0') { return Err(schema(name, "invalid column name")); }
                names.push(quote_identifier(column_name));
            }
        }
        let qualified = format!("main.{}", quote_identifier(canonical));
        let placeholders = (1..=names.len()).map(|n| format!("?{n}")).collect::<Vec<_>>().join(",");
        // ABORT deliberately overrides a target's IGNORE/REPLACE default. An
        // import must not silently discard data or delete an unrelated row.
        let insert = format!("INSERT OR ABORT INTO {qualified} ({}) VALUES ({placeholders})", names.join(","));
        let keys = table.info.pk_flags.iter().enumerate().filter(|(_, pk)| **pk).map(|(i, _)| i).collect();
        Ok(Self { name: canonical.to_owned(), qualified, columns: names, keys, insert })
    }

    fn key_predicate(&self, row: &ChangesetRow, params: &mut Vec<SqliteValue>) -> String {
        let values = if row.op == ChangeOp::Insert { &row.new_values } else { &row.old_values };
        self.keys.iter().map(|index| {
            params.push(values[*index].to_sqlite());
            format!("{} IS ?{}", self.columns[*index], params.len())
        }).collect::<Vec<_>>().join(" AND ")
    }

    async fn lookup(&self, conn: &Connection, change: &ChangesetRow) -> ApplyResult<Option<(Vec<SqliteValue>, bool)>> {
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
        let check = if checks.is_empty() { "1".to_owned() } else { checks.join(" AND ") };
        let sql = format!("SELECT {}, ({check}) FROM {} WHERE {key} LIMIT 2", self.columns.join(","), self.qualified);
        let rows = conn.query_with_params(&sql, &params).await?;
        if rows.len() > 1 { return Err(schema(&self.name, "primary-key lookup matched multiple rows")); }
        let Some(row) = rows.first() else { return Ok(None); };
        let current = (0..self.columns.len()).map(|index| row.get(index).cloned()
            .ok_or_else(|| schema(&self.name, "short target row"))).collect::<ApplyResult<Vec<_>>>()?;
        let matches = match row.get(self.columns.len()) {
            Some(SqliteValue::Integer(value)) => *value != 0,
            _ => return Err(schema(&self.name, "invalid old-value comparison result")),
        };
        Ok(Some((current, matches)))
    }

    async fn write(&self, conn: &Connection, change: &ChangesetRow) -> ApplyResult<()> {
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
                format!("UPDATE OR ABORT {} SET {} WHERE {key}", self.qualified, assignments.join(","))
            }
        };
        let changed = conn.execute_with_params(&sql, &params).await?;
        if changed != 1 {
            return Err(FrankenError::internal(format!("changeset DML expected one target row, changed {changed}")).into());
        }
        Ok(())
    }
}

struct ApplyTransaction<'a> {
    conn: &'a Connection,
    armed: bool,
}

impl ApplyTransaction<'_> {
    async fn rollback(&mut self, cause: SqlChangesetApplyError) -> SqlChangesetApplyError {
        if !self.conn.in_transaction() { self.armed = false; return cause; }
        match self.conn.rollback_transaction().await {
            Ok(()) => { self.armed = false; cause }
            Err(rollback) => SqlChangesetApplyError::Rollback { cause: Box::new(cause), rollback },
        }
    }
}

impl Drop for ApplyTransaction<'_> {
    fn drop(&mut self) {
        if self.armed { self.conn.mark_transaction_cleanup_required(); }
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
pub async fn apply_changeset(conn: &mut Connection, changeset: &Changeset) -> ApplyResult<SqlChangesetApplyReport> {
    validate(changeset)?;
    if conn.in_transaction() { return Err(FrankenError::NestedTransaction.into()); }
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
        for table in changeset.tables.iter().filter(|table| !table.rows.is_empty()) {
            plans.push((table, TablePlan::load(conn, table).await?));
        }
        let mut report = SqlChangesetApplyReport::default();
        for (table, plan) in plans {
            for (index, change) in table.rows.iter().enumerate() {
                let current = plan.lookup(conn, change).await?;
                let conflict = match (change.op, &current) {
                    (ChangeOp::Insert, Some(_)) => Some(ConflictType::Conflict),
                    (ChangeOp::Delete | ChangeOp::Update, None) => Some(ConflictType::NotFound),
                    (ChangeOp::Delete | ChangeOp::Update, Some((_, false))) => Some(ConflictType::Data),
                    _ => None,
                };
                if let Some(kind) = conflict {
                    return Err(SqlChangesetApplyError::Conflict { table: plan.name.clone(), row: index, kind });
                }
                plan.write(conn, change).await?;
                report.applied += 1;
            }
        }
        Ok(report)
    }.await;
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
    use fsqlite_ext_session::{Session, TableInfo};

    const fn int(value: i64) -> ChangesetValue { ChangesetValue::Integer(value) }
    fn string(value: &str) -> ChangesetValue { ChangesetValue::Text(value.to_owned()) }
    fn insert(values: Vec<ChangesetValue>) -> ChangesetRow {
        ChangesetRow { op: ChangeOp::Insert, indirect: false, old_values: Vec::new(), new_values: values }
    }
    fn delete(values: Vec<ChangesetValue>) -> ChangesetRow {
        ChangesetRow { op: ChangeOp::Delete, indirect: false, old_values: values, new_values: Vec::new() }
    }
    fn update(old: Vec<ChangesetValue>, new: Vec<ChangesetValue>) -> ChangesetRow {
        ChangesetRow { op: ChangeOp::Update, indirect: false, old_values: old, new_values: new }
    }
    fn table(name: &str, pk: &[bool], rows: Vec<ChangesetRow>) -> TableChangeset {
        TableChangeset { info: TableInfo { name: name.to_owned(), column_count: pk.len(), pk_flags: pk.to_vec() }, rows }
    }
    fn changes(rows: Vec<ChangesetRow>) -> Changeset {
        Changeset { kind: ChangesetKind::Changeset, tables: vec![table("t", &[true, false], rows)] }
    }
    async fn rows(conn: &Connection, sql: &str, width: usize) -> Vec<Vec<SqliteValue>> {
        conn.query(sql).await.unwrap().iter().map(|row| {
            (0..width).map(|index| row.get(index).unwrap().clone()).collect()
        }).collect()
    }

    #[test]
    fn recorded_changes_reach_sql_and_an_inverse_restores_the_base() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value)").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1,'old'),(2,'gone')").await.unwrap();
            let mut session = Session::new();
            session.attach_table("t", 2, vec![true, false]);
            session.record_update("t", vec![int(1), string("old")], vec![int(1), string("new")]);
            session.record_delete("t", vec![int(2), string("gone")]);
            session.record_insert("t", vec![int(3), ChangesetValue::Blob(vec![0, 0xff, 0])]);
            let changeset = session.changeset();
            assert_eq!(apply_changeset(&mut conn, &changeset).await.unwrap().applied, 3);
            assert_eq!(rows(&conn, "SELECT * FROM t ORDER BY id", 2).await, vec![
                vec![SqliteValue::Integer(1), SqliteValue::Text("new".into())],
                vec![SqliteValue::Integer(3), SqliteValue::Blob(vec![0, 0xff, 0].into())],
            ]);
            assert!(!conn.in_transaction());
            apply_changeset(&mut conn, &changeset.invert().unwrap()).await.unwrap();
            assert_eq!(rows(&conn, "SELECT * FROM t ORDER BY id", 2).await, vec![
                vec![SqliteValue::Integer(1), SqliteValue::Text("old".into())],
                vec![SqliteValue::Integer(2), SqliteValue::Text("gone".into())],
            ]);
        });
    }

    #[test]
    fn sparse_updates_preserve_local_edits_and_extra_default_columns() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v, local, extra TEXT DEFAULT 'default')").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1,'old','local edit','retained')").await.unwrap();
            let changeset = Changeset { kind: ChangesetKind::Changeset, tables: vec![table("t", &[true, false, false], vec![
                update(vec![int(1), string("old"), ChangesetValue::Undefined],
                    vec![ChangesetValue::Undefined, string("new"), ChangesetValue::Undefined]),
                insert(vec![int(2), ChangesetValue::Null, string("remote")]),
            ])] };
            apply_changeset(&mut conn, &changeset).await.unwrap();
            assert_eq!(rows(&conn, "SELECT v,local,extra FROM t ORDER BY id", 3).await, vec![
                vec![SqliteValue::Text("new".into()), SqliteValue::Text("local edit".into()), SqliteValue::Text("retained".into())],
                vec![SqliteValue::Null, SqliteValue::Text("remote".into()), SqliteValue::Text("default".into())],
            ]);
        });
    }

    #[test]
    fn data_conflict_rolls_back_earlier_changes_across_tables() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)").await.unwrap();
            conn.execute("CREATE TABLE other(id INTEGER PRIMARY KEY, v)").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1,'local')").await.unwrap();
            let changeset = Changeset { kind: ChangesetKind::Changeset, tables: vec![
                table("other", &[true, false], vec![insert(vec![int(9), string("must roll back")])]),
                table("t", &[true, false], vec![delete(vec![int(1), string("stale")])]),
            ] };
            assert!(matches!(apply_changeset(&mut conn, &changeset).await,
                Err(SqlChangesetApplyError::Conflict { kind: ConflictType::Data, .. })));
            assert!(rows(&conn, "SELECT * FROM other", 2).await.is_empty());
            assert_eq!(rows(&conn, "SELECT v FROM t", 1).await, vec![vec![SqliteValue::Text("local".into())]]);
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn missing_rows_and_duplicate_keys_are_explicit_conflicts() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1,'keep')").await.unwrap();
            for (row, expected) in [
                (insert(vec![int(1), string("overwrite")]), ConflictType::Conflict),
                (delete(vec![int(2), string("absent")]), ConflictType::NotFound),
                (update(vec![int(2), string("absent")], vec![ChangesetValue::Undefined, string("new")]), ConflictType::NotFound),
            ] {
                assert!(matches!(apply_changeset(&mut conn, &changes(vec![row])).await,
                    Err(SqlChangesetApplyError::Conflict { kind, .. }) if kind == expected));
                assert!(!conn.in_transaction());
            }
            assert_eq!(rows(&conn, "SELECT * FROM t", 2).await,
                vec![vec![SqliteValue::Integer(1), SqliteValue::Text("keep".into())]]);
        });
    }

    #[test]
    fn target_ignore_policy_cannot_silently_discard_an_import() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE ON CONFLICT IGNORE)").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1,'occupied')").await.unwrap();
            let changeset = changes(vec![insert(vec![int(2), string("temporary")]), insert(vec![int(3), string("occupied")])]);
            assert!(apply_changeset(&mut conn, &changeset).await.is_err());
            assert_eq!(rows(&conn, "SELECT * FROM t", 2).await,
                vec![vec![SqliteValue::Integer(1), SqliteValue::Text("occupied".into())]]);
        });
    }

    #[test]
    fn decoded_patchsets_skip_absent_old_non_key_values() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1,'local'),(2,'different')").await.unwrap();
            let full = changes(vec![
                update(vec![int(1), string("remote base")], vec![ChangesetValue::Undefined, string("remote end")]),
                delete(vec![int(2), string("other base")]),
            ]);
            let patch = Changeset::decode_patchset(&full.encode_patchset()).unwrap();
            assert_eq!(apply_changeset(&mut conn, &patch).await.unwrap().applied, 2);
            assert_eq!(rows(&conn, "SELECT * FROM t", 2).await,
                vec![vec![SqliteValue::Integer(1), SqliteValue::Text("remote end".into())]]);
        });
    }

    #[test]
    fn composite_keys_without_rowid_and_quoted_names_use_bound_values() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE \"odd\"\" table\"(\"key\"\"one\" TEXT, k2 INTEGER, v, PRIMARY KEY(\"key\"\"one\",k2)) WITHOUT ROWID").await.unwrap();
            let value = "'); DROP TABLE t; --";
            let initial = Changeset { kind: ChangesetKind::Changeset, tables: vec![table("odd\" table", &[true, true, false],
                vec![insert(vec![string("k'\""), int(7), string(value)])])] };
            apply_changeset(&mut conn, &initial).await.unwrap();
            assert_eq!(rows(&conn, "SELECT v FROM \"odd\"\" table\"", 1).await,
                vec![vec![SqliteValue::Text(value.into())]]);
            apply_changeset(&mut conn, &initial.invert().unwrap()).await.unwrap();
            assert!(rows(&conn, "SELECT v FROM \"odd\"\" table\"", 1).await.is_empty());
        });
    }

    #[test]
    fn old_value_checks_use_sql_affinity_and_column_collation() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id TEXT PRIMARY KEY COLLATE NOCASE, v TEXT COLLATE NOCASE, n INTEGER)").await.unwrap();
            conn.execute("INSERT INTO t VALUES('Key','LOCAL',12)").await.unwrap();
            let changeset = Changeset { kind: ChangesetKind::Changeset, tables: vec![table("t", &[true, false, false], vec![
                update(vec![string("key"), string("local"), string("12")],
                    vec![ChangesetValue::Undefined, string("new"), int(13)]),
            ])] };
            apply_changeset(&mut conn, &changeset).await.unwrap();
            assert_eq!(rows(&conn, "SELECT id,v,n FROM t", 3).await, vec![vec![
                SqliteValue::Text("Key".into()), SqliteValue::Text("new".into()), SqliteValue::Integer(13),
            ]]);
        });
    }

    #[test]
    fn schema_mismatch_rejects_the_whole_apply_and_leaves_source_tables() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)").await.unwrap();
            conn.execute("CREATE TABLE incompatible(id, v PRIMARY KEY)").await.unwrap();
            let changeset = Changeset { kind: ChangesetKind::Changeset, tables: vec![
                table("t", &[true, false], vec![insert(vec![int(1), string("not committed")])]),
                table("incompatible", &[true, false], vec![insert(vec![int(2), string("no")])]),
            ] };
            assert!(matches!(apply_changeset(&mut conn, &changeset).await, Err(SqlChangesetApplyError::Schema { .. })));
            assert!(rows(&conn, "SELECT * FROM t", 2).await.is_empty());
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn a_caller_transaction_is_neither_committed_nor_rolled_back() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)").await.unwrap();
            conn.begin_transaction().await.unwrap();
            conn.execute("INSERT INTO t VALUES(1,'caller')").await.unwrap();
            assert!(matches!(apply_changeset(&mut conn, &changes(vec![insert(vec![int(2), string("no")])])).await,
                Err(SqlChangesetApplyError::Database(FrankenError::NestedTransaction))));
            assert!(conn.in_transaction());
            assert_eq!(rows(&conn, "SELECT count(*) FROM t", 1).await, vec![vec![SqliteValue::Integer(1)]]);
            conn.commit_transaction().await.unwrap();
        });
    }

    #[test]
    fn invalid_public_rows_are_rejected_without_mutation_or_panics() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)").await.unwrap();
            for row in [
                insert(vec![int(1)]), insert(vec![int(1), ChangesetValue::Undefined]),
                insert(vec![ChangesetValue::Null, int(1)]), insert(vec![int(1), ChangesetValue::Real(f64::NAN)]),
                delete(vec![int(1), ChangesetValue::Undefined]),
                update(vec![int(1), int(2)], vec![int(1), int(3)]),
                update(vec![int(1), ChangesetValue::Undefined], vec![ChangesetValue::Undefined, int(3)]),
            ] {
                assert!(matches!(apply_changeset(&mut conn, &changes(vec![row])).await,
                    Err(SqlChangesetApplyError::InvalidChange { .. })));
            }
            assert!(rows(&conn, "SELECT * FROM t", 2).await.is_empty());
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn dropped_apply_guard_records_rollback_before_next_sql() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)").await.unwrap();
            let guard = ApplyTransaction { conn: &conn, armed: true };
            conn.begin_transaction().await.unwrap();
            conn.execute("INSERT INTO t VALUES(1,'abandoned')").await.unwrap();
            drop(guard);
            assert!(rows(&conn, "SELECT * FROM t", 2).await.is_empty());
            assert!(!conn.in_transaction());
        });
    }

    #[cfg(feature = "native")]
    #[test]
    fn applied_rows_and_index_entries_survive_reopen() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("applied.db");
            let mut conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").await.unwrap();
            conn.execute("CREATE INDEX by_v ON t(v)").await.unwrap();
            apply_changeset(&mut conn, &changes(vec![insert(vec![int(42), string("durable")])])).await.unwrap();
            conn.close().await.unwrap();
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            assert_eq!(rows(&conn, "SELECT id FROM t INDEXED BY by_v WHERE v='durable'", 1).await,
                vec![vec![SqliteValue::Integer(42)]]);
            conn.close().await.unwrap();
        });
    }
}
