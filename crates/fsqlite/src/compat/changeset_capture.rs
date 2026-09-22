//! Transaction-scoped capture of real SQL mutations into SQLite Session bytes.
//!
//! TEMP triggers retain only the first image of each touched primary key. The
//! final image is read through the table's primary-key lookup, so repeated
//! writes coalesce without copying the whole database. Journal writes obey the
//! same savepoints as application writes. This is an explicit capture scope,
//! not a preupdate hook installed on every Connection.
//!
//! Requires ordinary main tables with declared primary keys, no generated
//! columns, no application triggers, and recursive_triggers=ON (including
//! REPLACE's implicit deletes). NULL primary keys are not recorded, matching
//! SQLite Session. The indirect flag applies to the entire scope; trigger depth
//! is not inferred. Selected tables define the replication subset, including
//! any foreign-key-cascade destinations the caller needs to replicate.
//! Key changes use canonical DELETE/INSERT records, including case-only changes
//! under NOCASE. This intentionally avoids the unapplyable UPDATE/INSERT pair
//! emitted by SQLite Session 3.46.1 for that edge; wire compatibility is not a
//! claim of identical record selection on every historical SQLite build.
//!
//! The scope exclusively borrows the Connection. Its SQL API admits only reads
//! and DML on selected tables, plus explicit savepoint methods. No transaction
//! control, DDL or PRAGMA can bypass capture finalization. Application functions
//! must not reenter the connection or mutate it through external aliases.
//! Dropping any in-flight operation poisons capture. Dropping the scope uses
//! the existing Transaction rollback obligation; it never builds an executor.
//!
//! ```ignore
//! use fsqlite::compat::capture::{CaptureOptions, ChangesetCapture};
//! let mut capture = ChangesetCapture::begin(
//!     &mut connection, &cx, CaptureOptions::new(["items"]),
//! ).await?;
//! capture.execute("UPDATE items SET value=?1 WHERE id=?2", &params).await?;
//! let committed = capture.commit().await?;
//! // committed.bytes is a full SQLite Session changeset, not a patchset.
//! ```

#[path = "changeset_outbox.rs"]
pub mod outbox;

use std::fmt;

use fsqlite_ast::{QualifiedName, Statement};
use fsqlite_ext_session::{ChangeOp, ChangesetRow, ChangesetValue, TableInfo};
use fsqlite_parser::Parser;
use fsqlite_types::cx::Cx;
use fsqlite_types::serial_type::{varint_len, write_varint};
use fsqlite_types::value::SqliteValue;

use crate::compat::{Transaction, TransactionExt};
use crate::{Connection, FrankenError, Row};

const PREFIX: &str = "__fsqlite_capture_";
const BUDGET: &str = "__fsqlite_capture_budget";
const MAX_SQL_BYTES: usize = 1024 * 1024;
const TRIGGERS: [(&str, &str, &str, bool); 4] = [
    ("bu", "BEFORE UPDATE", "OLD", false),
    ("bd", "BEFORE DELETE", "OLD", false),
    ("ai", "AFTER INSERT", "NEW", true),
    ("au", "AFTER UPDATE", "NEW", true),
];

/// Limits account for journal/final row images and encoded output separately.
/// They are not a process-RSS budget or a bound on the caller's query results.
#[derive(Debug, Clone)]
pub struct CaptureOptions {
    pub tables: Vec<String>,
    pub max_touched_rows: usize,
    pub max_image_bytes: usize,
    pub max_cells: usize,
    pub max_changeset_bytes: usize,
    pub indirect: bool,
}

impl CaptureOptions {
    #[must_use]
    pub fn new(tables: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            tables: tables.into_iter().map(Into::into).collect(),
            max_touched_rows: 10_000,
            max_image_bytes: 8 * 1024 * 1024,
            max_cells: 100_000,
            max_changeset_bytes: 16 * 1024 * 1024,
            indirect: false,
        }
    }

    fn validate(&self) -> CaptureResult<()> {
        if self.tables.is_empty() || self.tables.len() > 64 {
            return Err(CaptureError::Input("select 1..64 application tables"));
        }
        for (index, table) in self.tables.iter().enumerate() {
            validate_name(table)?;
            if reserved(table)
                || self.tables[..index].iter().any(|other| other.eq_ignore_ascii_case(table))
            {
                return Err(CaptureError::Input("select distinct non-reserved application tables"));
            }
        }
        for (limit, ceiling) in [
            (self.max_touched_rows, 100_000),
            (self.max_image_bytes, 64 * 1024 * 1024),
            (self.max_cells, 1_000_000),
            (self.max_changeset_bytes, 64 * 1024 * 1024),
        ] {
            if limit == 0 || limit > ceiling {
                return Err(CaptureError::Input("capture limits must be positive and within their ceilings"));
            }
        }
        Ok(())
    }
}

/// COMMIT errors remain distinguishable from a failure before COMMIT. A
/// rollback error preserves both causes; neither certifies absence of a write.
#[derive(Debug)]
pub enum CaptureError {
    Input(&'static str),
    Schema(&'static str),
    Limit(&'static str),
    Poisoned,
    /// A prior committed request owns this identity. Look it up rather than
    /// committing the newly repeated application work.
    DuplicateMessage { sequence: i64 },
    Engine(FrankenError),
    Commit(FrankenError),
    Rollback { cause: Box<Self>, error: FrankenError },
}

pub type CaptureResult<T> = Result<T, CaptureError>;

impl fmt::Display for CaptureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input(detail) => write!(f, "changeset capture input: {detail}"),
            Self::Schema(detail) => write!(f, "changeset capture schema: {detail}"),
            Self::Limit(resource) => write!(f, "changeset capture {resource} limit exceeded"),
            Self::Poisoned => f.write_str("changeset capture failed or was interrupted; rollback required"),
            Self::DuplicateMessage { sequence } => write!(f, "outbox message already committed at sequence {sequence}; new capture commit refused"),
            Self::Engine(error) => write!(f, "changeset capture: {error}"),
            Self::Commit(error) => write!(f, "changeset capture COMMIT not acknowledged: {error}"),
            Self::Rollback { cause, error } => write!(f, "{cause}; rollback also failed: {error}"),
        }
    }
}

impl std::error::Error for CaptureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Engine(error) | Self::Commit(error) => Some(error),
            Self::Rollback { cause, .. } => Some(cause.as_ref()),
            _ => None,
        }
    }
}

impl From<FrankenError> for CaptureError {
    fn from(error: FrankenError) -> Self { Self::Engine(error) }
}

/// Returned only after capture encoding and SQL COMMIT both succeed.
/// SQL commit follows the connection's durability settings. These bytes have
/// not been stored in an outbox or delivered to another database by this API.
#[derive(Debug)]
#[must_use]
pub struct CapturedChangeset {
    pub bytes: Vec<u8>,
    pub changes: usize,
    pub touched_rows: usize,
}

#[derive(Debug)]
struct TablePlan {
    info: TableInfo,
    // SQLite compares the raw PK ordinal bytes when admitting WITHOUT ROWID
    // changesets. TableInfo's boolean view is sufficient for row semantics,
    // but encoding it would turn PK(b,a) [2,1] into [1,1] and skip the table.
    pk_ordinals: Vec<u8>,
    columns: Vec<String>,
    keys: Vec<usize>,
    journal: String,
}

/// Owned SQL transaction plus its TEMP first-touch journal. Ordinary BEGIN
/// retains the engine's default concurrent-writer policy. No global lock is
/// introduced. A live caller transaction is refused, not adopted or committed.
pub struct ChangesetCapture<'a> {
    transaction: Transaction<'a>,
    cx: &'a Cx,
    options: CaptureOptions,
    plans: Vec<TablePlan>,
    poisoned: bool,
    main_version: i64,
    temp_version: i64,
}

fn checkpoint(cx: &Cx) -> CaptureResult<()> {
    cx.checkpoint().map_err(|_| CaptureError::Engine(FrankenError::Interrupt))
}

fn quote(name: &str) -> String { format!("\"{}\"", name.replace('"', "\"\"")) }
fn literal(name: &str) -> String { format!("'{}'", name.replace('\'', "''")) }
fn reserved(name: &str) -> bool {
    let folded = name.to_ascii_lowercase();
    folded.starts_with("sqlite_") || folded.starts_with("__fsqlite_")
}
fn validate_name(name: &str) -> CaptureResult<()> {
    if name.is_empty() || name.len() > 1024 || name.contains('\0') {
        return Err(CaptureError::Input("names require 1..1024 UTF-8 bytes without NUL"));
    }
    Ok(())
}
fn integer(row: &Row, column: usize) -> CaptureResult<i64> {
    match row.get(column) {
        Some(SqliteValue::Integer(value)) => Ok(*value),
        _ => Err(CaptureError::Schema("expected integer catalog/journal metadata")),
    }
}
fn text(row: &Row, column: usize) -> CaptureResult<&str> {
    match row.get(column) {
        Some(SqliteValue::Text(value)) => Ok(value.as_ref()),
        _ => Err(CaptureError::Schema("expected text catalog metadata")),
    }
}
fn count(row: &Row, column: usize) -> CaptureResult<usize> {
    usize::try_from(integer(row, column)?)
        .map_err(|_| CaptureError::Schema("invalid catalog/journal count"))
}
async fn scalar(transaction: &Transaction<'_>, sql: &str) -> CaptureResult<i64> {
    integer(&transaction.query_row(sql).await?, 0)
}

impl<'a> ChangesetCapture<'a> {
    pub async fn begin(
        connection: &'a mut Connection,
        cx: &'a Cx,
        options: CaptureOptions,
    ) -> CaptureResult<Self> {
        options.validate()?;
        checkpoint(cx)?;
        let transaction = connection.transaction().await?;
        let mut capture = Self {
            transaction, cx, options, plans: Vec::new(), poisoned: true,
            main_version: 0, temp_version: 0,
        };
        if let Err(error) = capture.install().await {
            return Err(capture.rollback_after(error).await);
        }
        capture.poisoned = false;
        Ok(capture)
    }

    async fn install(&mut self) -> CaptureResult<()> {
        if scalar(&self.transaction, "PRAGMA recursive_triggers").await? != 1 {
            return Err(CaptureError::Schema("enable recursive_triggers before capture to observe REPLACE deletes"));
        }
        for schema in ["main", "temp"] {
            // Application trigger ordering cannot substitute for a preupdate
            // hook. Refuse before installing any capture object or running DML.
            let sql = format!("SELECT name FROM {schema}.sqlite_schema WHERE type = 'trigger' LIMIT 1");
            if !self.transaction.query(&sql).await?.is_empty() {
                return Err(CaptureError::Schema("application triggers are not supported in a capture scope"));
            }
        }
        if !self.transaction.query_with_params(
            "SELECT name FROM temp.sqlite_schema WHERE name GLOB ?1 LIMIT 1",
            &[SqliteValue::Text(format!("{PREFIX}*").into())],
        ).await?.is_empty() {
            return Err(CaptureError::Schema("reserved capture TEMP objects already exist"));
        }
        for table in &self.options.tables {
            checkpoint(self.cx)?;
            let plan = read_plan(&self.transaction, table, self.plans.len()).await?;
            self.plans.push(plan);
        }
        self.transaction.execute(&format!(
            "CREATE TEMP TABLE {} (n INTEGER NOT NULL, bytes INTEGER NOT NULL, cells INTEGER NOT NULL)", quote(BUDGET),
        )).await?;
        self.transaction.execute(&format!("INSERT INTO temp.{} VALUES (0,0,0)", quote(BUDGET))).await?;
        for plan in &self.plans {
            for sql in journal_statements(plan, &self.options) {
                checkpoint(self.cx)?;
                self.transaction.execute(&sql).await?;
            }
        }
        self.main_version = scalar(&self.transaction, "PRAGMA main.schema_version").await?;
        self.temp_version = scalar(&self.transaction, "PRAGMA temp.schema_version").await?;
        checkpoint(self.cx)
    }

    fn ready(&mut self) -> CaptureResult<()> {
        if self.poisoned { return Err(CaptureError::Poisoned); }
        let result = checkpoint(self.cx);
        if result.is_err() { self.poisoned = true; }
        result
    }

    fn admit_sql(&self, sql: &str) -> CaptureResult<()> {
        if sql.len() > MAX_SQL_BYTES { return Err(CaptureError::Limit("SQL bytes")); }
        let (statements, errors) = Parser::from_sql(sql).parse_all();
        if let Some(error) = errors.first() {
            return Err(FrankenError::ParseError {
                offset: error.span.start as usize, detail: error.to_string(),
            }.into());
        }
        for statement in &statements {
            let table = match statement {
                Statement::Select(_) => continue,
                Statement::Insert(insert) => &insert.table,
                Statement::Update(update) => &update.table.name,
                Statement::Delete(delete) => &delete.table.name,
                _ => return Err(CaptureError::Input("capture SQL admits SELECT and selected-table DML only")),
            };
            self.admit_table(table)?;
        }
        Ok(())
    }

    fn admit_table(&self, table: &QualifiedName) -> CaptureResult<()> {
        if table.schema.as_ref().is_some_and(|schema| !schema.eq_ignore_ascii_case("main"))
            || !self.plans.iter().any(|plan| plan.info.name.eq_ignore_ascii_case(&table.name))
        {
            return Err(CaptureError::Input("DML target is not a selected main table"));
        }
        Ok(())
    }

    /// Any error, even one the caller catches, makes this scope uncommittable.
    /// Dropping this future while it is pending has the same effect.
    pub async fn execute(&mut self, sql: &str, params: &[SqliteValue]) -> CaptureResult<usize> {
        self.ready()?;
        self.poisoned = true;
        self.admit_sql(sql)?;
        let changed = self.transaction.execute_with_params(sql, params).await?;
        checkpoint(self.cx)?;
        self.poisoned = false;
        Ok(changed)
    }

    /// The entire batch is parsed and admitted before its first statement runs.
    pub async fn execute_batch(&mut self, sql: &str) -> CaptureResult<()> {
        self.ready()?;
        self.poisoned = true;
        self.admit_sql(sql)?;
        self.transaction.execute_batch(sql).await?;
        checkpoint(self.cx)?;
        self.poisoned = false;
        Ok(())
    }

    pub async fn query(&mut self, sql: &str, params: &[SqliteValue]) -> CaptureResult<Vec<Row>> {
        self.ready()?;
        self.poisoned = true;
        self.admit_sql(sql)?;
        let rows = self.transaction.query_with_params(sql, params).await?;
        checkpoint(self.cx)?;
        self.poisoned = false;
        Ok(rows)
    }

    pub async fn savepoint(&mut self, name: &str) -> CaptureResult<()> {
        self.savepoint_sql("SAVEPOINT", name).await
    }
    pub async fn rollback_to(&mut self, name: &str) -> CaptureResult<()> {
        self.savepoint_sql("ROLLBACK TO", name).await
    }
    pub async fn release(&mut self, name: &str) -> CaptureResult<()> {
        self.savepoint_sql("RELEASE", name).await
    }
    async fn savepoint_sql(&mut self, operation: &str, name: &str) -> CaptureResult<()> {
        self.ready()?;
        self.poisoned = true;
        validate_name(name)?;
        if reserved(name) { return Err(CaptureError::Input("reserved savepoint name")); }
        self.transaction.execute(&format!("{operation} {}", quote(name))).await?;
        checkpoint(self.cx)?;
        self.poisoned = false;
        Ok(())
    }

    /// Encode net changes before COMMIT. No bytes escape on a failed or
    /// unacknowledged COMMIT. Retrying application work is the caller's choice.
    pub async fn commit(mut self) -> CaptureResult<CapturedChangeset> {
        let result = self.collect().await;
        let captured = match result {
            Ok(captured) => captured,
            Err(error) => return Err(self.rollback_after(error).await),
        };
        if let Err(error) = self.transaction.commit().await {
            return Err(self.rollback_after(CaptureError::Commit(error)).await);
        }
        Ok(captured)
    }

    pub async fn rollback(mut self) -> CaptureResult<()> {
        self.transaction.rollback().await.map_err(CaptureError::Engine)
    }

    async fn rollback_after(&mut self, cause: CaptureError) -> CaptureError {
        match self.transaction.rollback().await {
            Ok(()) | Err(FrankenError::NoActiveTransaction) => cause,
            Err(error) => CaptureError::Rollback { cause: Box::new(cause), error },
        }
    }

    async fn collect(&mut self) -> CaptureResult<CapturedChangeset> {
        self.ready()?;
        self.poisoned = true;
        if scalar(&self.transaction, "PRAGMA main.schema_version").await? != self.main_version
            || scalar(&self.transaction, "PRAGMA temp.schema_version").await? != self.temp_version
            || scalar(&self.transaction, "PRAGMA recursive_triggers").await? != 1
        {
            return Err(CaptureError::Schema("schema or recursive_triggers changed during capture"));
        }
        let budget = self.transaction.query_row(&format!("SELECT n,bytes,cells FROM temp.{}", quote(BUDGET))).await?;
        let touched_rows = count(&budget, 0)?;
        let mut retained = ImageBudget { bytes: count(&budget, 1)?, cells: count(&budget, 2)? };
        if touched_rows > self.options.max_touched_rows {
            return Err(CaptureError::Limit("touched rows"));
        }
        retained.check(&self.options)?;
        let mut bytes = Vec::new();
        let mut changes = 0;
        for plan in &self.plans {
            let rows = collect_table(&self.transaction, self.cx, &self.options, plan, &mut retained).await?;
            if !rows.is_empty() {
                let required = table_wire_size(&plan.info, &rows)?;
                let total = bytes.len().checked_add(required).ok_or(CaptureError::Limit("wire bytes"))?;
                if total > self.options.max_changeset_bytes { return Err(CaptureError::Limit("wire bytes")); }
                bytes.try_reserve_exact(required).map_err(|_| FrankenError::OutOfMemory)?;
                encode_capture_header(plan, &mut bytes);
                for row in &rows { row.encode_changeset(&mut bytes); }
                changes += rows.len();
            }
        }
        for plan in &self.plans {
            for (suffix, _, _, _) in TRIGGERS {
                checkpoint(self.cx)?;
                self.transaction.execute(&format!("DROP TRIGGER temp.{}", quote(&format!("{}_{suffix}", plan.journal)))).await?;
            }
            self.transaction.execute(&format!("DROP TABLE temp.{}", quote(&plan.journal))).await?;
        }
        self.transaction.execute(&format!("DROP TABLE temp.{}", quote(BUDGET))).await?;
        checkpoint(self.cx)?;
        Ok(CapturedChangeset { bytes, changes, touched_rows })
    }
}

async fn read_plan(transaction: &Transaction<'_>, requested: &str, ordinal: usize) -> CaptureResult<TablePlan> {
    for row in transaction.query(&format!("PRAGMA temp.table_list({})", literal(requested))).await? {
        if text(&row, 1)?.eq_ignore_ascii_case(requested) {
            return Err(CaptureError::Schema("a TEMP object shadows a selected main table"));
        }
    }
    let listed = transaction.query(&format!("PRAGMA main.table_list({})", literal(requested))).await?;
    let matching: Vec<_> = listed.iter().filter(|row| {
        text(row, 0).is_ok_and(|schema| schema == "main")
            && text(row, 1).is_ok_and(|name| name.eq_ignore_ascii_case(requested))
    }).collect();
    let [table] = matching.as_slice() else { return Err(CaptureError::Schema("selected main table does not exist")); };
    if text(table, 2)? != "table" { return Err(CaptureError::Schema("capture requires an ordinary main table")); }
    let name = text(table, 1)?.to_owned();
    let columns = transaction.query(&format!("PRAGMA main.table_xinfo({})", literal(&name))).await?;
    if columns.is_empty() || columns.len() > 256 || columns.len() != count(table, 3)? {
        return Err(CaptureError::Schema("capture supports 1..256 ordinary columns"));
    }
    let mut names = Vec::new();
    let mut keys = Vec::new();
    let mut ordinals = Vec::new();
    let mut pk_ordinals = Vec::new();
    let mut flags = Vec::new();
    for (index, row) in columns.iter().enumerate() {
        if count(row, 0)? != index || count(row, 6)? != 0 {
            return Err(CaptureError::Schema("hidden/generated columns are not supported"));
        }
        let name = text(row, 1)?;
        validate_name(name)?;
        names.push(name.to_owned());
        let pk = count(row, 5)?;
        pk_ordinals.push(u8::try_from(pk).map_err(|_| CaptureError::Schema("primary-key ordinal exceeds the Session wire domain"))?);
        flags.push(pk != 0);
        if pk != 0 { keys.push(index); ordinals.push(pk); }
    }
    ordinals.sort_unstable();
    if keys.is_empty() || keys.len() > 16 || ordinals.iter().copied().ne(1..=keys.len()) {
        return Err(CaptureError::Schema("capture requires a declared primary key of 1..16 columns"));
    }
    Ok(TablePlan {
        info: TableInfo { name, column_count: names.len(), pk_flags: flags },
        pk_ordinals,
        columns: names, keys, journal: format!("{PREFIX}{ordinal}"),
    })
}

fn encode_capture_header(plan: &TablePlan, output: &mut Vec<u8>) {
    output.push(b'T');
    let mut encoded = [0_u8; 9];
    let len = write_varint(&mut encoded, u64::try_from(plan.info.column_count).expect("at most 256 columns"));
    output.extend_from_slice(&encoded[..len]);
    output.extend_from_slice(&plan.pk_ordinals);
    output.extend_from_slice(plan.info.name.as_bytes());
    output.push(0);
}

// SQL comparisons over explicit type tags and BLOB text keys cannot be
// changed by an application overriding BINARY/NOCASE. Plain-column indexes
// also avoid depending on TEMP expression-index support for capture journals.
fn key_value(expression: &str) -> String {
    format!("CASE WHEN CAST(typeof({expression}) AS BLOB)=x'74657874' THEN CAST({expression} AS BLOB) ELSE {expression} END")
}
fn value_cost(expression: &str) -> String {
    // Twice the encoded length also covers UTF-16 -> UTF-8 expansion.
    format!("(CASE CAST(typeof({expression}) AS BLOB) WHEN x'74657874' THEN 2*length(CAST({expression} AS BLOB)) WHEN x'626c6f62' THEN length({expression}) ELSE 8 END)")
}
fn image_cost(expressions: &[String]) -> String {
    format!("{}+{}", 64 + expressions.len() * 32, expressions.iter().map(|value| value_cost(value)).collect::<Vec<_>>().join("+"))
}
fn journal_statements(plan: &TablePlan, options: &CaptureOptions) -> Vec<String> {
    let fields: Vec<_> = (0..plan.columns.len()).map(|index| format!("v{index}")).collect();
    let keys: Vec<_> = (0..plan.keys.len()).flat_map(|index| [format!("t{index}"), format!("k{index}")]).collect();
    let definitions = fields.iter().chain(&keys).map(|field| format!("{field} BLOB")).collect::<Vec<_>>().join(",");
    let mut sql = vec![
        format!("CREATE TEMP TABLE {} (seq INTEGER PRIMARY KEY,inserted INTEGER NOT NULL,{definitions})", quote(&plan.journal)),
        format!("CREATE UNIQUE INDEX temp.{} ON {} ({})", quote(&format!("{}_pk", plan.journal)), quote(&plan.journal), keys.join(",")),
    ];
    for (suffix, event, image, inserted) in TRIGGERS {
        let values: Vec<_> = plan.columns.iter().enumerate().map(|(index, column)| {
            if inserted && !plan.info.pk_flags[index] { "NULL".to_owned() }
            else { format!("{image}.{}", quote(column)) }
        }).collect();
        let typed: Vec<_> = plan.keys.iter().flat_map(|&index| {
            let value = &values[index];
            [format!("CAST(typeof({value}) AS BLOB)"), key_value(value)]
        }).collect();
        let present = plan.keys.iter().map(|&index| format!("{} IS NOT NULL", values[index])).collect::<Vec<_>>().join(" AND ");
        let same = keys.iter().zip(&typed).map(|(key, value)| format!("{key} IS ({value})")).collect::<Vec<_>>().join(" AND ");
        let stored: Vec<_> = values.into_iter().chain(typed).collect();
        let cost = image_cost(&stored);
        sql.push(format!(
            "CREATE TEMP TRIGGER {} {event} ON main.{} WHEN {present} AND NOT EXISTS (SELECT 1 FROM {} WHERE {same}) BEGIN SELECT CASE WHEN n>={} OR cells+{}>{} OR bytes+({cost})>{} THEN RAISE(ABORT,'ERR_FSQLITE_CAPTURE_LIMIT') END FROM {}; UPDATE {} SET n=n+1,cells=cells+{},bytes=bytes+({cost}); INSERT INTO {} VALUES(NULL,{},{}) ; END",
            quote(&format!("{}_{suffix}", plan.journal)), quote(&plan.info.name), quote(&plan.journal),
            options.max_touched_rows, stored.len(), options.max_cells, options.max_image_bytes,
            quote(BUDGET), quote(BUDGET), stored.len(), quote(&plan.journal), u8::from(inserted), stored.join(","),
        ));
    }
    sql
}

struct ImageBudget { bytes: usize, cells: usize }
impl ImageBudget {
    fn check(&self, options: &CaptureOptions) -> CaptureResult<()> {
        if self.bytes > options.max_image_bytes { return Err(CaptureError::Limit("row-image bytes")); }
        if self.cells > options.max_cells { return Err(CaptureError::Limit("row-image cells")); }
        Ok(())
    }
    fn add(&mut self, bytes: usize, cells: usize, options: &CaptureOptions) -> CaptureResult<()> {
        self.bytes = self.bytes.checked_add(bytes).ok_or(CaptureError::Limit("row-image bytes"))?;
        self.cells = self.cells.checked_add(cells).ok_or(CaptureError::Limit("row-image cells"))?;
        self.check(options)
    }
}
fn row_values(row: &Row, offset: usize, columns: usize) -> CaptureResult<Vec<ChangesetValue>> {
    (offset..offset + columns).map(|index| {
        row.get(index).map(ChangesetValue::from_sqlite)
            .ok_or(CaptureError::Schema("incomplete captured row"))
    }).collect()
}

async fn collect_table(
    transaction: &Transaction<'_>, cx: &Cx, options: &CaptureOptions,
    plan: &TablePlan, retained: &mut ImageBudget,
) -> CaptureResult<Vec<ChangesetRow>> {
    let columns: Vec<_> = plan.columns.iter().map(|column| quote(column)).collect();
    let fields = (0..columns.len()).map(|index| format!("v{index}")).collect::<Vec<_>>().join(",");
    let lookup = plan.keys.iter().enumerate().map(|(index, &column)| format!("{}=?{}", columns[column], index + 1)).collect::<Vec<_>>().join(" AND ");
    let source = format!("FROM main.{} WHERE {lookup} LIMIT 2", quote(&plan.info.name));
    let mut rows = Vec::new();
    let mut after = 0_i64;
    loop {
        checkpoint(cx)?;
        let batch = transaction.query_with_params(&format!(
            "SELECT seq,inserted,{fields} FROM temp.{} WHERE seq>?1 ORDER BY seq LIMIT 32", quote(&plan.journal),
        ), &[SqliteValue::Integer(after)]).await?;
        if batch.is_empty() { break; }
        for entry in batch {
            checkpoint(cx)?;
            let seq = integer(&entry, 0)?;
            let inserted = integer(&entry, 1)?;
            if seq <= after || !matches!(inserted, 0 | 1) {
                return Err(CaptureError::Schema("invalid first-touch journal sequence"));
            }
            after = seq;
            let old = row_values(&entry, 2, columns.len())?;
            let params: Vec<_> = plan.keys.iter().map(|&index| old[index].to_sqlite()).collect();
            if params.iter().any(SqliteValue::is_null) { return Err(CaptureError::Schema("NULL journal key")); }
            let costs = transaction.query_with_params(&format!("SELECT {} {source}", image_cost(&columns)), &params).await?;
            if costs.len() > 1 { return Err(CaptureError::Schema("primary key lookup returned multiple rows")); }
            if let Some(cost) = costs.first() { retained.add(count(cost, 0)?, columns.len(), options)?; }
            let current = transaction.query_with_params(&format!("SELECT {} {source}", columns.join(",")), &params).await?;
            if current.len() != costs.len() { return Err(CaptureError::Schema("primary-key snapshot changed during capture")); }
            let current = current.first().map(|row| row_values(row, 0, columns.len())).transpose()?;
            // A declared NOCASE or numeric-affinity index finds candidate rows;
            // Session identity additionally retains storage class and key bytes.
            let current = current.filter(|row| plan.keys.iter().all(|&index| old[index] == row[index]));
            if let Some(change) = net_change(old, current, inserted == 1, &plan.info.pk_flags, options.indirect) {
                rows.push(change);
            }
        }
    }
    // A key move must vacate the old key before the new key is inserted.
    rows.sort_by_key(|row| u8::from(row.op != ChangeOp::Delete));
    Ok(rows)
}

fn net_change(
    old: Vec<ChangesetValue>, current: Option<Vec<ChangesetValue>>, inserted: bool,
    keys: &[bool], indirect: bool,
) -> Option<ChangesetRow> {
    match (inserted, current) {
        (true, None) => None,
        (true, Some(new_values)) => Some(ChangesetRow { op: ChangeOp::Insert, indirect, old_values: Vec::new(), new_values }),
        (false, None) => Some(ChangesetRow { op: ChangeOp::Delete, indirect, old_values: old, new_values: Vec::new() }),
        (false, Some(current)) => {
            let mut changed = false;
            let mut old_values = Vec::new();
            let mut new_values = Vec::new();
            for ((before, after), &key) in old.into_iter().zip(current).zip(keys) {
                let modified = !key && before != after;
                changed |= modified;
                old_values.push(if key || modified { before } else { ChangesetValue::Undefined });
                new_values.push(if modified { after } else { ChangesetValue::Undefined });
            }
            changed.then_some(ChangesetRow { op: ChangeOp::Update, indirect, old_values, new_values })
        }
    }
}

fn table_wire_size(info: &TableInfo, rows: &[ChangesetRow]) -> CaptureResult<usize> {
    let mut size = 2 + varint_len(u64::try_from(info.column_count).map_err(|_| CaptureError::Limit("columns"))?)
        + info.column_count + info.name.len();
    for row in rows {
        size = size.checked_add(2).ok_or(CaptureError::Limit("wire bytes"))?;
        for value in row.old_values.iter().chain(&row.new_values) {
            let len = match value {
                ChangesetValue::Undefined | ChangesetValue::Null => 1,
                ChangesetValue::Integer(_) | ChangesetValue::Real(_) => 9,
                ChangesetValue::Text(value) => value_wire_size(value.len())?,
                ChangesetValue::Blob(value) => value_wire_size(value.len())?,
            };
            size = size.checked_add(len).ok_or(CaptureError::Limit("wire bytes"))?;
        }
    }
    Ok(size)
}
fn value_wire_size(bytes: usize) -> CaptureResult<usize> {
    let encoded = u64::try_from(bytes).map_err(|_| CaptureError::Limit("wire bytes"))?;
    bytes.checked_add(1 + varint_len(encoded)).ok_or(CaptureError::Limit("wire bytes"))
}

#[cfg(test)]
#[path = "changeset_capture_tests.rs"]
mod tests;
