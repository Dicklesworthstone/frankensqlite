//! Independent typed SQL generator for differential testing.
//!
//! This module deliberately owns its AST and printer. It does not use
//! `fsqlite-ast` or any production SQL formatting path, so parser and printer
//! defects cannot validate one another. Campaign integration, capability
//! profiles, and execution against both engines are layered on top by the
//! dependent Turso-adaptation beads.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info};
use xxhash_rust::xxh3::xxh3_64;

use crate::canonical_parity_contract::{
    CONTRACT_AUTHORITY_REGISTRY_SCHEMA_VERSION, CanonicalParityContractBundle, LifecycleState,
    ParityTaxonomyStatus, SupportState, canonical_contract_authority_report,
};
use crate::test_inventory::ExecutionLane;

/// Stable artifact schema for generated cases.
pub const GENERATOR_SCHEMA_VERSION: u32 = 2;
/// Generator implementation version. Seed compatibility is scoped to this value.
pub const GENERATOR_VERSION: &str = "2.0.0";
/// The test-local profile retained for generator-core tests.
pub const BOOTSTRAP_PROFILE_NAME: &str = "supported_core_bootstrap";
/// Version of the test-local bootstrap profile.
pub const BOOTSTRAP_PROFILE_VERSION: &str = "1.0.0";
/// Canonical named-profile schema.
pub const CANONICAL_PROFILE_SCHEMA_VERSION: &str = "fsqlite.generator_profile.v1";
/// Exact normalized weight total for every canonical profile.
pub const CANONICAL_PROFILE_WEIGHT_TOTAL: u32 = 10_000;

const SEED_DOMAIN: &[u8] = b"fsqlite.typed-sql-generator.v1";

/// A validated SQL identifier. Printing always uses SQLite double-quote escaping.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Identifier(String);

impl Identifier {
    /// Validate and construct an identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, GenerationError> {
        let value = value.into();
        if value.is_empty() {
            return Err(GenerationError::invalid_input(
                "identifier",
                "identifier must not be empty",
            ));
        }
        if value.contains('\0') {
            return Err(GenerationError::invalid_input(
                "identifier",
                "identifier must not contain NUL",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn write_sql(&self, output: &mut String) {
        output.push('"');
        for ch in self.0.chars() {
            if ch == '"' {
                output.push('"');
            }
            output.push(ch);
        }
        output.push('"');
    }
}

/// A validated finite real literal stored in canonical text form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RealLiteral(String);

impl RealLiteral {
    pub fn new(value: impl Into<String>) -> Result<Self, GenerationError> {
        let value = value.into();
        let parsed = value.parse::<f64>().map_err(|_| {
            GenerationError::invalid_input("real_literal", "real literal must parse as f64")
        })?;
        if !parsed.is_finite() {
            return Err(GenerationError::invalid_input(
                "real_literal",
                "real literal must be finite",
            ));
        }
        let canonical = if parsed == 0.0 {
            "0.0".to_owned()
        } else {
            parsed.to_string()
        };
        Ok(Self(canonical))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// SQLite literal values supported by the bootstrap generator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(RealLiteral),
    Text(String),
    Blob(Vec<u8>),
}

impl SqlValue {
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Integer(value) => value.to_string().len(),
            Self::Real(value) => value.as_str().len(),
            Self::Text(value) => value.len(),
            Self::Blob(value) => value.len(),
        }
    }

    fn write_sql(&self, output: &mut String) {
        match self {
            Self::Null => output.push_str("NULL"),
            Self::Integer(value) => write!(output, "{value}").expect("write to String"),
            Self::Real(value) => output.push_str(value.as_str()),
            Self::Text(value) => {
                output.push('\'');
                for ch in value.chars() {
                    if ch == '\'' {
                        output.push('\'');
                    }
                    output.push(ch);
                }
                output.push('\'');
            }
            Self::Blob(value) => {
                output.push_str("X'");
                for byte in value {
                    write!(output, "{byte:02X}").expect("write to String");
                }
                output.push('\'');
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnAffinity {
    Integer,
    Real,
    Text,
    Blob,
    Numeric,
}

impl ColumnAffinity {
    const fn sql(self) -> &'static str {
        match self {
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
            Self::Numeric => "NUMERIC",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSpec {
    pub name: Identifier,
    pub affinity: ColumnAffinity,
    pub primary_key: bool,
    pub not_null: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnaryOp {
    Negate,
    Not,
    BitNot,
}

impl UnaryOp {
    const fn sql(self) -> &'static str {
        match self {
            Self::Negate => "-",
            Self::Not => "NOT ",
            Self::BitNot => "~",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    And,
    Or,
    Concat,
}

impl BinaryOp {
    const fn sql(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Equal => "=",
            Self::NotEqual => "<>",
            Self::Less => "<",
            Self::LessEqual => "<=",
            Self::Greater => ">",
            Self::GreaterEqual => ">=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Concat => "||",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl AggregateFunction {
    const fn sql(self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Avg => "AVG",
        }
    }
}

/// Independent expression tree used only by the test generator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Expr {
    Value {
        value: SqlValue,
    },
    Column {
        table: Option<Identifier>,
        column: Identifier,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Binary {
        left: Box<Self>,
        op: BinaryOp,
        right: Box<Self>,
    },
    IsNull {
        expr: Box<Self>,
        negated: bool,
    },
    Aggregate {
        function: AggregateFunction,
        expr: Option<Box<Self>>,
        distinct: bool,
    },
    InSubquery {
        expr: Box<Self>,
        subquery: Box<Select>,
        negated: bool,
    },
    ScalarSubquery {
        subquery: Box<Select>,
    },
}

impl Expr {
    #[must_use]
    pub fn literal(value: SqlValue) -> Self {
        Self::Value { value }
    }

    #[must_use]
    pub fn column(table: Option<Identifier>, column: Identifier) -> Self {
        Self::Column { table, column }
    }

    #[must_use]
    pub fn depth(&self) -> u32 {
        match self {
            Self::Value { .. } | Self::Column { .. } => 1,
            Self::Unary { expr, .. } | Self::IsNull { expr, .. } => 1 + expr.depth(),
            Self::Binary { left, right, .. } => 1 + left.depth().max(right.depth()),
            Self::Aggregate { expr, .. } => 1 + expr.as_deref().map_or(0, Self::depth),
            Self::InSubquery { expr, subquery, .. } => 1 + expr.depth().max(subquery.depth()),
            Self::ScalarSubquery { subquery } => 1 + subquery.depth(),
        }
    }

    fn write_sql(&self, output: &mut String) {
        match self {
            Self::Value { value } => value.write_sql(output),
            Self::Column { table, column } => {
                if let Some(table) = table {
                    table.write_sql(output);
                    output.push('.');
                }
                column.write_sql(output);
            }
            Self::Unary { op, expr } => {
                output.push('(');
                output.push_str(op.sql());
                expr.write_sql(output);
                output.push(')');
            }
            Self::Binary { left, op, right } => {
                output.push('(');
                left.write_sql(output);
                write!(output, " {} ", op.sql()).expect("write to String");
                right.write_sql(output);
                output.push(')');
            }
            Self::IsNull { expr, negated } => {
                output.push('(');
                expr.write_sql(output);
                output.push_str(if *negated { " IS NOT NULL" } else { " IS NULL" });
                output.push(')');
            }
            Self::Aggregate {
                function,
                expr,
                distinct,
            } => {
                output.push_str(function.sql());
                output.push('(');
                if *distinct {
                    output.push_str("DISTINCT ");
                }
                if let Some(expr) = expr {
                    expr.write_sql(output);
                } else {
                    output.push('*');
                }
                output.push(')');
            }
            Self::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                output.push('(');
                expr.write_sql(output);
                output.push_str(if *negated { " NOT IN (" } else { " IN (" });
                subquery.write_sql(output);
                output.push_str("))");
            }
            Self::ScalarSubquery { subquery } => {
                output.push('(');
                subquery.write_sql(output);
                output.push(')');
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<Identifier>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FromItem {
    pub table: Identifier,
    pub alias: Option<Identifier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    Inner,
    Left,
}

impl JoinKind {
    const fn sql(self) -> &'static str {
        match self {
            Self::Inner => "INNER JOIN",
            Self::Left => "LEFT JOIN",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Join {
    pub kind: JoinKind,
    pub table: Identifier,
    pub alias: Option<Identifier>,
    pub on: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderTerm {
    pub expr: Expr,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompoundOperator {
    Union,
    UnionAll,
    Intersect,
    Except,
}

impl CompoundOperator {
    const fn sql(self) -> &'static str {
        match self {
            Self::Union => "UNION",
            Self::UnionAll => "UNION ALL",
            Self::Intersect => "INTERSECT",
            Self::Except => "EXCEPT",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompoundSelect {
    pub operator: CompoundOperator,
    pub right: Box<Select>,
}

/// Independent SELECT representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Select {
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
    pub from: Option<FromItem>,
    pub joins: Vec<Join>,
    pub predicate: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub compound: Option<CompoundSelect>,
    pub order_by: Vec<OrderTerm>,
    pub limit: Option<u32>,
}

impl Select {
    #[must_use]
    pub fn depth(&self) -> u32 {
        let mut depth = 1;
        for item in &self.projection {
            depth = depth.max(1 + item.expr.depth());
        }
        for join in &self.joins {
            depth = depth.max(1 + join.on.depth());
        }
        if let Some(predicate) = &self.predicate {
            depth = depth.max(1 + predicate.depth());
        }
        for expr in &self.group_by {
            depth = depth.max(1 + expr.depth());
        }
        if let Some(having) = &self.having {
            depth = depth.max(1 + having.depth());
        }
        if let Some(compound) = &self.compound {
            depth = depth.max(1 + compound.right.depth());
        }
        depth
    }

    fn write_sql(&self, output: &mut String) {
        output.push_str("SELECT ");
        if self.distinct {
            output.push_str("DISTINCT ");
        }
        for (index, item) in self.projection.iter().enumerate() {
            if index > 0 {
                output.push_str(", ");
            }
            item.expr.write_sql(output);
            if let Some(alias) = &item.alias {
                output.push_str(" AS ");
                alias.write_sql(output);
            }
        }
        if let Some(from) = &self.from {
            output.push_str(" FROM ");
            from.table.write_sql(output);
            if let Some(alias) = &from.alias {
                output.push_str(" AS ");
                alias.write_sql(output);
            }
        }
        for join in &self.joins {
            write!(output, " {} ", join.kind.sql()).expect("write to String");
            join.table.write_sql(output);
            if let Some(alias) = &join.alias {
                output.push_str(" AS ");
                alias.write_sql(output);
            }
            output.push_str(" ON ");
            join.on.write_sql(output);
        }
        if let Some(predicate) = &self.predicate {
            output.push_str(" WHERE ");
            predicate.write_sql(output);
        }
        if !self.group_by.is_empty() {
            output.push_str(" GROUP BY ");
            write_expr_list(output, &self.group_by);
        }
        if let Some(having) = &self.having {
            output.push_str(" HAVING ");
            having.write_sql(output);
        }
        if let Some(compound) = &self.compound {
            write!(output, " {} ", compound.operator.sql()).expect("write to String");
            compound.right.write_sql(output);
        }
        if !self.order_by.is_empty() {
            output.push_str(" ORDER BY ");
            for (index, term) in self.order_by.iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                term.expr.write_sql(output);
                output.push_str(match term.direction {
                    OrderDirection::Asc => " ASC",
                    OrderDirection::Desc => " DESC",
                });
            }
        }
        if let Some(limit) = self.limit {
            write!(output, " LIMIT {limit}").expect("write to String");
        }
    }
}

fn write_expr_list(output: &mut String, expressions: &[Expr]) {
    for (index, expr) in expressions.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        expr.write_sql(output);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStatement {
    Begin,
    Commit,
    Rollback,
}

/// Independent statement tree for the supported-core bootstrap profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Statement {
    CreateTable {
        table: Identifier,
        columns: Vec<ColumnSpec>,
    },
    CreateIndex {
        index: Identifier,
        table: Identifier,
        columns: Vec<Identifier>,
        unique: bool,
    },
    Insert {
        table: Identifier,
        columns: Vec<Identifier>,
        rows: Vec<Vec<SqlValue>>,
    },
    Update {
        table: Identifier,
        assignments: Vec<(Identifier, Expr)>,
        predicate: Option<Expr>,
    },
    Delete {
        table: Identifier,
        predicate: Option<Expr>,
    },
    Select {
        select: Select,
    },
    Transaction {
        statement: TransactionStatement,
    },
}

impl Statement {
    #[must_use]
    pub fn depth(&self) -> u32 {
        match self {
            Self::CreateTable { .. }
            | Self::CreateIndex { .. }
            | Self::Insert { .. }
            | Self::Transaction { .. } => 1,
            Self::Update {
                assignments,
                predicate,
                ..
            } => {
                let assignment_depth = assignments
                    .iter()
                    .map(|(_, expr)| expr.depth())
                    .max()
                    .unwrap_or(0);
                1 + assignment_depth.max(predicate.as_ref().map_or(0, Expr::depth))
            }
            Self::Delete { predicate, .. } => 1 + predicate.as_ref().map_or(0, Expr::depth),
            Self::Select { select } => select.depth(),
        }
    }

    #[must_use]
    pub fn to_sql(&self) -> String {
        let mut output = String::new();
        match self {
            Self::CreateTable { table, columns } => {
                output.push_str("CREATE TABLE ");
                table.write_sql(&mut output);
                output.push_str(" (");
                for (index, column) in columns.iter().enumerate() {
                    if index > 0 {
                        output.push_str(", ");
                    }
                    column.name.write_sql(&mut output);
                    write!(output, " {}", column.affinity.sql()).expect("write to String");
                    if column.primary_key {
                        output.push_str(" PRIMARY KEY");
                    }
                    if column.not_null {
                        output.push_str(" NOT NULL");
                    }
                }
                output.push(')');
            }
            Self::CreateIndex {
                index,
                table,
                columns,
                unique,
            } => {
                output.push_str(if *unique {
                    "CREATE UNIQUE INDEX "
                } else {
                    "CREATE INDEX "
                });
                index.write_sql(&mut output);
                output.push_str(" ON ");
                table.write_sql(&mut output);
                output.push_str(" (");
                for (position, column) in columns.iter().enumerate() {
                    if position > 0 {
                        output.push_str(", ");
                    }
                    column.write_sql(&mut output);
                }
                output.push(')');
            }
            Self::Insert {
                table,
                columns,
                rows,
            } => {
                output.push_str("INSERT INTO ");
                table.write_sql(&mut output);
                output.push_str(" (");
                for (index, column) in columns.iter().enumerate() {
                    if index > 0 {
                        output.push_str(", ");
                    }
                    column.write_sql(&mut output);
                }
                output.push_str(") VALUES ");
                for (row_index, row) in rows.iter().enumerate() {
                    if row_index > 0 {
                        output.push_str(", ");
                    }
                    output.push('(');
                    for (value_index, value) in row.iter().enumerate() {
                        if value_index > 0 {
                            output.push_str(", ");
                        }
                        value.write_sql(&mut output);
                    }
                    output.push(')');
                }
            }
            Self::Update {
                table,
                assignments,
                predicate,
            } => {
                output.push_str("UPDATE ");
                table.write_sql(&mut output);
                output.push_str(" SET ");
                for (index, (column, expr)) in assignments.iter().enumerate() {
                    if index > 0 {
                        output.push_str(", ");
                    }
                    column.write_sql(&mut output);
                    output.push_str(" = ");
                    expr.write_sql(&mut output);
                }
                if let Some(predicate) = predicate {
                    output.push_str(" WHERE ");
                    predicate.write_sql(&mut output);
                }
            }
            Self::Delete { table, predicate } => {
                output.push_str("DELETE FROM ");
                table.write_sql(&mut output);
                if let Some(predicate) = predicate {
                    output.push_str(" WHERE ");
                    predicate.write_sql(&mut output);
                }
            }
            Self::Select { select } => select.write_sql(&mut output),
            Self::Transaction { statement } => output.push_str(match statement {
                TransactionStatement::Begin => "BEGIN",
                TransactionStatement::Commit => "COMMIT",
                TransactionStatement::Rollback => "ROLLBACK",
            }),
        }
        output.push(';');
        output
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableState {
    pub name: Identifier,
    pub columns: Vec<ColumnSpec>,
    pub estimated_rows: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexState {
    pub name: Identifier,
    pub table: Identifier,
    pub columns: Vec<Identifier>,
    pub unique: bool,
}

/// Generator-owned schema model. It changes only through accepted proposals.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaState {
    pub tables: Vec<TableState>,
    pub indexes: Vec<IndexState>,
    pub transaction_open: bool,
}

impl SchemaState {
    #[must_use]
    pub fn table(&self, name: &Identifier) -> Option<&TableState> {
        self.tables.iter().find(|table| table.name == *name)
    }

    #[must_use]
    pub fn total_rows(&self) -> u32 {
        self.tables.iter().fold(0_u32, |total, table| {
            total.saturating_add(table.estimated_rows)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Construct {
    CreateTable,
    CreateIndex,
    Insert,
    Update,
    Delete,
    Select,
    Join,
    Aggregate,
    Subquery,
    CompoundSelect,
    Transaction,
}

/// Whether a statement prepares a case or exercises the requested subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementRole {
    Setup,
    Subject,
}

impl StatementRole {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Setup => "setup",
            Self::Subject => "subject",
        }
    }
}

/// Admission policy for canonical contract bindings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileMode {
    Required,
    Partial,
    FeatureDevelopment,
}

/// Stable built-in profile names derived from the canonical contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamedGeneratorProfile {
    SupportedCore,
    ReadOnly,
    Dml,
    Planner,
    Vdbe,
    Transaction,
    Mvcc,
    PlannerPartial,
    VdbePartial,
}

impl NamedGeneratorProfile {
    const fn label(self) -> &'static str {
        match self {
            Self::SupportedCore => "supported_core",
            Self::ReadOnly => "read_only",
            Self::Dml => "dml",
            Self::Planner => "planner",
            Self::Vdbe => "vdbe",
            Self::Transaction => "transaction",
            Self::Mvcc => "mvcc",
            Self::PlannerPartial => "planner_partial",
            Self::VdbePartial => "vdbe_partial",
        }
    }
}

/// One requested binding with explicit expected contract state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileFeatureRequest {
    pub role: StatementRole,
    pub construct: Construct,
    pub feature_id: String,
    pub surface_id: String,
    pub ledger_feature_id: String,
    pub component: String,
    pub expected_taxonomy_status: ParityTaxonomyStatus,
    pub expected_surface_state: SupportState,
    pub expected_lifecycle_state: LifecycleState,
}

/// Full fail-closed request used to derive a generator profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalProfileRequest {
    pub name: String,
    pub version: String,
    pub mode: ProfileMode,
    pub expected_gap_policy: Option<String>,
    pub authorization_bead: Option<String>,
    pub setup: Vec<Construct>,
    pub features: Vec<ProfileFeatureRequest>,
    pub required_lanes: Vec<ExecutionLane>,
}

/// Actual canonical binding retained in generated-case evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileBinding {
    pub role: StatementRole,
    pub construct: Construct,
    pub feature_id: String,
    pub taxonomy_status: ParityTaxonomyStatus,
    pub taxonomy_weight: u64,
    pub surface_id: String,
    pub surface_state: SupportState,
    pub ledger_feature_id: String,
    pub component: String,
    pub lifecycle_state: LifecycleState,
}

/// Normalized subject weight for one exact canonical feature binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileFeatureWeight {
    pub feature_id: String,
    pub construct: Construct,
    pub weight: u32,
}

/// Canonical hashes and verified bindings that make profile derivation replayable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalProfileEvidence {
    pub schema_version: String,
    pub registry_schema_version: String,
    pub profile_name: String,
    pub profile_version: String,
    pub setup: Vec<Construct>,
    pub sqlite_target: String,
    pub taxonomy_version: String,
    pub version_contract_sha256: String,
    pub surface_matrix_sha256: String,
    pub feature_ledger_sha256: String,
    pub parity_taxonomy_sha256: String,
    pub mode: ProfileMode,
    pub expected_gap_policy: Option<String>,
    pub authorization_bead: Option<String>,
    pub required_lanes: Vec<ExecutionLane>,
    pub bindings: Vec<ProfileBinding>,
    pub feature_weights: Vec<ProfileFeatureWeight>,
    pub profile_sha256: String,
}

/// Verify the schema identifiers and every digest retained in canonical
/// profile evidence.
///
/// # Errors
///
/// Returns an invalid-input error when an authority digest is malformed or
/// when the profile digest does not cover the exact retained evidence.
pub fn validate_canonical_profile_evidence(
    evidence: &CanonicalProfileEvidence,
) -> Result<(), GenerationError> {
    if evidence.schema_version != CANONICAL_PROFILE_SCHEMA_VERSION
        || evidence.registry_schema_version != CONTRACT_AUTHORITY_REGISTRY_SCHEMA_VERSION
    {
        return Err(GenerationError::invalid_input(
            "profile.canonical_evidence.schema",
            "canonical profile evidence schema identifiers do not match this generator",
        ));
    }
    for (field, digest) in [
        (
            "version_contract_sha256",
            evidence.version_contract_sha256.as_str(),
        ),
        (
            "surface_matrix_sha256",
            evidence.surface_matrix_sha256.as_str(),
        ),
        (
            "feature_ledger_sha256",
            evidence.feature_ledger_sha256.as_str(),
        ),
        (
            "parity_taxonomy_sha256",
            evidence.parity_taxonomy_sha256.as_str(),
        ),
        ("profile_sha256", evidence.profile_sha256.as_str()),
    ] {
        if !is_lower_sha256(digest) {
            return Err(GenerationError::invalid_input(
                format!("profile.canonical_evidence.{field}"),
                "digest must be 64 lowercase hexadecimal characters",
            ));
        }
    }

    let mut unhashed = evidence.clone();
    unhashed.profile_sha256.clear();
    let expected = sha256_hex(canonical_json(&unhashed)?.as_bytes());
    if evidence.profile_sha256 != expected {
        return Err(GenerationError::invalid_input(
            "profile.canonical_evidence.profile_sha256",
            "profile digest does not match the retained canonical evidence",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstructWeight {
    pub construct: Construct,
    pub weight: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratorProfile {
    pub name: String,
    pub version: String,
    pub setup: Vec<Construct>,
    pub require_subject: bool,
    pub weights: Vec<ConstructWeight>,
    pub feature_weights: Vec<ProfileFeatureWeight>,
    pub canonical_evidence: Option<CanonicalProfileEvidence>,
}

impl GeneratorProfile {
    #[must_use]
    pub fn bootstrap() -> Self {
        Self {
            name: BOOTSTRAP_PROFILE_NAME.to_owned(),
            version: BOOTSTRAP_PROFILE_VERSION.to_owned(),
            setup: vec![
                Construct::CreateTable,
                Construct::CreateTable,
                Construct::CreateIndex,
                Construct::Insert,
                Construct::Insert,
                Construct::Transaction,
                Construct::Update,
                Construct::Transaction,
            ],
            require_subject: false,
            weights: vec![
                ConstructWeight {
                    construct: Construct::Select,
                    weight: 18,
                },
                ConstructWeight {
                    construct: Construct::Join,
                    weight: 12,
                },
                ConstructWeight {
                    construct: Construct::Aggregate,
                    weight: 12,
                },
                ConstructWeight {
                    construct: Construct::Subquery,
                    weight: 10,
                },
                ConstructWeight {
                    construct: Construct::CompoundSelect,
                    weight: 10,
                },
                ConstructWeight {
                    construct: Construct::Insert,
                    weight: 12,
                },
                ConstructWeight {
                    construct: Construct::Update,
                    weight: 10,
                },
                ConstructWeight {
                    construct: Construct::Delete,
                    weight: 8,
                },
                ConstructWeight {
                    construct: Construct::Transaction,
                    weight: 8,
                },
            ],
            feature_weights: Vec::new(),
            canonical_evidence: None,
        }
    }

    fn validate(&self) -> Result<(), GenerationError> {
        if self.name.trim().is_empty() {
            return Err(GenerationError::invalid_input(
                "profile.name",
                "profile name must not be empty",
            ));
        }
        if self.version.trim().is_empty() {
            return Err(GenerationError::invalid_input(
                "profile.version",
                "profile version must not be empty",
            ));
        }
        if self.weights.is_empty() || self.weights.iter().all(|entry| entry.weight == 0) {
            return Err(GenerationError::exhausted_choices(
                "profile.weights",
                "profile must contain at least one positive weight",
            ));
        }
        if self.require_subject && self.canonical_evidence.is_none() {
            return Err(GenerationError::invalid_input(
                "profile.canonical_evidence",
                "subject-required profiles must retain canonical contract evidence",
            ));
        }
        let mut constructs = BTreeSet::new();
        for entry in &self.weights {
            if !constructs.insert(entry.construct) {
                return Err(GenerationError::invalid_input(
                    "profile.weights",
                    "profile contains a duplicate construct weight",
                ));
            }
        }
        if let Some(evidence) = &self.canonical_evidence {
            validate_canonical_profile_evidence(evidence)?;
            if self.feature_weights.is_empty()
                || self.feature_weights.iter().any(|entry| entry.weight == 0)
            {
                return Err(GenerationError::invalid_input(
                    "profile.feature_weights",
                    "canonical profiles require positive per-feature weights",
                ));
            }
            let mut feature_ids = BTreeSet::new();
            if self
                .feature_weights
                .iter()
                .any(|entry| !feature_ids.insert(entry.feature_id.as_str()))
            {
                return Err(GenerationError::invalid_input(
                    "profile.feature_weights",
                    "canonical profile contains a duplicate weighted feature",
                ));
            }
            let total = self
                .feature_weights
                .iter()
                .fold(0_u32, |sum, entry| sum.saturating_add(entry.weight));
            if total != CANONICAL_PROFILE_WEIGHT_TOTAL
                || aggregate_construct_weights(&self.feature_weights) != self.weights
                || evidence.feature_weights != self.feature_weights
                || evidence.setup != self.setup
                || evidence.profile_name != self.name
                || evidence.profile_version != self.version
            {
                return Err(GenerationError::invalid_input(
                    "profile.canonical_evidence",
                    "canonical profile weights or identity drifted from retained evidence",
                ));
            }
        } else if !self.feature_weights.is_empty() {
            return Err(GenerationError::invalid_input(
                "profile.feature_weights",
                "per-feature weights require canonical profile evidence",
            ));
        }
        Ok(())
    }
}

/// Build the reviewed request behind one stable named profile.
#[must_use]
pub fn named_profile_request(kind: NamedGeneratorProfile) -> CanonicalProfileRequest {
    let standard_setup = vec![
        Construct::CreateTable,
        Construct::CreateTable,
        Construct::CreateIndex,
        Construct::Insert,
        Construct::Insert,
    ];
    let transaction_setup = vec![Construct::CreateTable, Construct::Insert];
    let setup_features = || {
        vec![
            profile_feature(
                StatementRole::Setup,
                Construct::CreateTable,
                "F-SQL.16",
                "SURF-SQL-CORE-001",
                "LEDGER-0001",
                "parser",
                PASS_TESTED,
            ),
            profile_feature(
                StatementRole::Setup,
                Construct::CreateIndex,
                "F-SQL.22",
                "SURF-SQL-CORE-001",
                "LEDGER-0001",
                "parser",
                PASS_TESTED,
            ),
            profile_feature(
                StatementRole::Setup,
                Construct::Insert,
                "F-SQL.10",
                "SURF-SQL-CORE-001",
                "LEDGER-0002",
                "parser",
                PASS_TESTED,
            ),
        ]
    };
    let transaction_setup_features = || {
        vec![
            profile_feature(
                StatementRole::Setup,
                Construct::CreateTable,
                "F-SQL.16",
                "SURF-SQL-CORE-001",
                "LEDGER-0001",
                "parser",
                PASS_TESTED,
            ),
            profile_feature(
                StatementRole::Setup,
                Construct::Insert,
                "F-SQL.10",
                "SURF-SQL-CORE-001",
                "LEDGER-0002",
                "parser",
                PASS_TESTED,
            ),
        ]
    };
    let read_only_subjects = || {
        vec![
            profile_feature(
                StatementRole::Subject,
                Construct::Select,
                "F-SQL.01",
                "SURF-SQL-CORE-001",
                "LEDGER-0001",
                "parser",
                PASS_TESTED,
            ),
            profile_feature(
                StatementRole::Subject,
                Construct::Join,
                "F-SQL.02",
                "SURF-SQL-CORE-001",
                "LEDGER-0001",
                "parser",
                PASS_TESTED,
            ),
            profile_feature(
                StatementRole::Subject,
                Construct::CompoundSelect,
                "F-SQL.03",
                "SURF-SQL-COMPOUND-002",
                "LEDGER-0003",
                "parser",
                PASS_TESTED,
            ),
            profile_feature(
                StatementRole::Subject,
                Construct::Subquery,
                "F-SQL.06",
                "SURF-SQL-COMPOUND-002",
                "LEDGER-0003",
                "parser",
                PASS_TESTED,
            ),
            profile_feature(
                StatementRole::Subject,
                Construct::Aggregate,
                "F-SQL.08",
                "SURF-SQL-CORE-001",
                "LEDGER-0001",
                "parser",
                PASS_TESTED,
            ),
        ]
    };
    let dml_subjects = || {
        vec![
            profile_feature(
                StatementRole::Subject,
                Construct::Insert,
                "F-SQL.10",
                "SURF-SQL-CORE-001",
                "LEDGER-0002",
                "parser",
                PASS_TESTED,
            ),
            profile_feature(
                StatementRole::Subject,
                Construct::Update,
                "F-SQL.14",
                "SURF-SQL-CORE-001",
                "LEDGER-0002",
                "parser",
                PASS_TESTED,
            ),
            profile_feature(
                StatementRole::Subject,
                Construct::Delete,
                "F-SQL.15",
                "SURF-SQL-CORE-001",
                "LEDGER-0002",
                "parser",
                PASS_TESTED,
            ),
        ]
    };

    let (mode, expected_gap_policy, setup, mut features, required_lanes) = match kind {
        NamedGeneratorProfile::SupportedCore => {
            let mut features = setup_features();
            features.extend(read_only_subjects());
            features.extend(dml_subjects());
            features.push(profile_feature(
                StatementRole::Subject,
                Construct::Transaction,
                "F-TXN.02",
                "SURF-TXN-MVCC-CONCURRENT-006",
                "LEDGER-0013",
                "core",
                PASS_DIFFERENTIALLY_VERIFIED,
            ));
            (
                ProfileMode::Required,
                None,
                standard_setup.clone(),
                features,
                vec![ExecutionLane::SqlResultOnly],
            )
        }
        NamedGeneratorProfile::ReadOnly => {
            let mut features = setup_features();
            features.extend(read_only_subjects());
            (
                ProfileMode::Required,
                None,
                standard_setup.clone(),
                features,
                vec![ExecutionLane::SqlResultOnly],
            )
        }
        NamedGeneratorProfile::Dml => {
            let mut features = setup_features();
            features.extend(dml_subjects());
            (
                ProfileMode::Required,
                None,
                standard_setup.clone(),
                features,
                vec![ExecutionLane::SqlResultOnly],
            )
        }
        NamedGeneratorProfile::Planner => {
            let mut features = setup_features();
            features.extend([
                profile_feature(
                    StatementRole::Subject,
                    Construct::Select,
                    "F-PLN.01",
                    "SURF-SQL-CORE-001",
                    "LEDGER-0005",
                    "planner",
                    PASS_DIFFERENTIALLY_VERIFIED,
                ),
                profile_feature(
                    StatementRole::Subject,
                    Construct::Select,
                    "F-PLN.02",
                    "SURF-SQL-CORE-001",
                    "LEDGER-0006",
                    "planner",
                    PASS_TESTED,
                ),
                profile_feature(
                    StatementRole::Subject,
                    Construct::Join,
                    "F-PLN.11",
                    "SURF-SQL-CORE-001",
                    "LEDGER-0006",
                    "planner",
                    PASS_TESTED,
                ),
                profile_feature(
                    StatementRole::Subject,
                    Construct::Select,
                    "F-PLN.12",
                    "SURF-SQL-CORE-001",
                    "LEDGER-0006",
                    "planner",
                    PASS_TESTED,
                ),
            ]);
            (
                ProfileMode::Required,
                None,
                standard_setup.clone(),
                features,
                vec![ExecutionLane::PlannerRequired],
            )
        }
        NamedGeneratorProfile::Vdbe => {
            let mut features = setup_features();
            features.extend([
                profile_feature(
                    StatementRole::Subject,
                    Construct::Select,
                    "F-VDB.01",
                    "SURF-SQL-CORE-001",
                    "LEDGER-0009",
                    "vdbe",
                    PASS_TESTED,
                ),
                profile_feature(
                    StatementRole::Subject,
                    Construct::Select,
                    "F-VDB.02",
                    "SURF-SQL-CORE-001",
                    "LEDGER-0010",
                    "vdbe",
                    PASS_TESTED,
                ),
                profile_feature(
                    StatementRole::Subject,
                    Construct::Select,
                    "F-VDB.03",
                    "SURF-SQL-CORE-001",
                    "LEDGER-0010",
                    "vdbe",
                    PASS_TESTED,
                ),
                profile_feature(
                    StatementRole::Subject,
                    Construct::Transaction,
                    "F-VDB.08",
                    "SURF-SQL-CORE-001",
                    "LEDGER-0011",
                    "vdbe",
                    PASS_IMPLEMENTED,
                ),
                profile_feature(
                    StatementRole::Subject,
                    Construct::CompoundSelect,
                    "F-VDB.14",
                    "SURF-SQL-COMPOUND-002",
                    "LEDGER-0012",
                    "vdbe",
                    PASS_IMPLEMENTED,
                ),
            ]);
            (
                ProfileMode::Required,
                None,
                standard_setup.clone(),
                features,
                vec![ExecutionLane::VdbeRequired],
            )
        }
        NamedGeneratorProfile::Transaction | NamedGeneratorProfile::Mvcc => {
            let mut features = transaction_setup_features();
            features.extend([
                profile_feature(
                    StatementRole::Subject,
                    Construct::Transaction,
                    "F-TXN.01",
                    "SURF-TXN-MVCC-CONCURRENT-006",
                    "LEDGER-0014",
                    "core",
                    PASS_TESTED,
                ),
                profile_feature(
                    StatementRole::Subject,
                    Construct::Transaction,
                    "F-TXN.02",
                    "SURF-TXN-MVCC-CONCURRENT-006",
                    "LEDGER-0013",
                    "core",
                    PASS_DIFFERENTIALLY_VERIFIED,
                ),
                profile_feature(
                    StatementRole::Subject,
                    Construct::Transaction,
                    "F-TXN.03",
                    "SURF-TXN-MVCC-CONCURRENT-006",
                    "LEDGER-0014",
                    "core",
                    PASS_TESTED,
                ),
            ]);
            let lane = if kind == NamedGeneratorProfile::Mvcc {
                ExecutionLane::MvccRequired
            } else {
                ExecutionLane::PagerBackedRequired
            };
            (
                ProfileMode::Required,
                None,
                transaction_setup.clone(),
                features,
                vec![lane],
            )
        }
        NamedGeneratorProfile::PlannerPartial => {
            let mut features = setup_features();
            for (construct, feature_id, ledger_id, lifecycle) in [
                (
                    Construct::Select,
                    "F-PLN.03",
                    "LEDGER-0008",
                    LifecycleState::Implemented,
                ),
                (
                    Construct::Join,
                    "F-PLN.04",
                    "LEDGER-0008",
                    LifecycleState::Implemented,
                ),
                (
                    Construct::Select,
                    "F-PLN.05",
                    "LEDGER-0008",
                    LifecycleState::Implemented,
                ),
                (
                    Construct::Subquery,
                    "F-PLN.07",
                    "LEDGER-0007",
                    LifecycleState::Tested,
                ),
                (
                    Construct::Select,
                    "F-PLN.09",
                    "LEDGER-0008",
                    LifecycleState::Implemented,
                ),
            ] {
                let surface_id = if feature_id == "F-PLN.07" {
                    "SURF-SQL-COMPOUND-002"
                } else {
                    "SURF-SQL-CORE-001"
                };
                features.push(profile_feature(
                    StatementRole::Subject,
                    construct,
                    feature_id,
                    surface_id,
                    ledger_id,
                    "planner",
                    ExpectedProfileStates {
                        taxonomy: ParityTaxonomyStatus::Partial,
                        lifecycle,
                    },
                ));
            }
            (
                ProfileMode::Partial,
                Some("exercise documented planner gaps without promotion claims".to_owned()),
                standard_setup.clone(),
                features,
                vec![ExecutionLane::PlannerRequired],
            )
        }
        NamedGeneratorProfile::VdbePartial => {
            let mut features = setup_features();
            features.push(profile_feature(
                StatementRole::Subject,
                Construct::Select,
                "F-VDB.15",
                "SURF-SQL-COMPOUND-002",
                "LEDGER-0012",
                "vdbe",
                PARTIAL_IMPLEMENTED,
            ));
            (
                ProfileMode::Partial,
                Some(
                    "exercise documented remaining-opcode gaps without promotion claims".to_owned(),
                ),
                standard_setup,
                features,
                vec![ExecutionLane::VdbeRequired],
            )
        }
    };

    features.sort_by(|left, right| {
        (left.role, left.construct, left.feature_id.as_str()).cmp(&(
            right.role,
            right.construct,
            right.feature_id.as_str(),
        ))
    });
    CanonicalProfileRequest {
        name: kind.label().to_owned(),
        version: "1.0.0".to_owned(),
        mode,
        expected_gap_policy,
        authorization_bead: None,
        setup,
        features,
        required_lanes,
    }
}

#[derive(Clone, Copy)]
struct ExpectedProfileStates {
    taxonomy: ParityTaxonomyStatus,
    lifecycle: LifecycleState,
}

const PASS_TESTED: ExpectedProfileStates = ExpectedProfileStates {
    taxonomy: ParityTaxonomyStatus::Pass,
    lifecycle: LifecycleState::Tested,
};
const PASS_IMPLEMENTED: ExpectedProfileStates = ExpectedProfileStates {
    taxonomy: ParityTaxonomyStatus::Pass,
    lifecycle: LifecycleState::Implemented,
};
const PASS_DIFFERENTIALLY_VERIFIED: ExpectedProfileStates = ExpectedProfileStates {
    taxonomy: ParityTaxonomyStatus::Pass,
    lifecycle: LifecycleState::DifferentiallyVerified,
};
const PARTIAL_IMPLEMENTED: ExpectedProfileStates = ExpectedProfileStates {
    taxonomy: ParityTaxonomyStatus::Partial,
    lifecycle: LifecycleState::Implemented,
};

fn profile_feature(
    role: StatementRole,
    construct: Construct,
    feature_id: &str,
    surface_id: &str,
    ledger_feature_id: &str,
    component: &str,
    expected: ExpectedProfileStates,
) -> ProfileFeatureRequest {
    ProfileFeatureRequest {
        role,
        construct,
        feature_id: feature_id.to_owned(),
        surface_id: surface_id.to_owned(),
        ledger_feature_id: ledger_feature_id.to_owned(),
        component: component.to_owned(),
        expected_taxonomy_status: expected.taxonomy,
        expected_surface_state: SupportState::Supported,
        expected_lifecycle_state: expected.lifecycle,
    }
}

/// Derive a built-in profile from the canonical contracts and their content hashes.
pub fn derive_named_profile(
    workspace_root: &Path,
    kind: NamedGeneratorProfile,
) -> Result<GeneratorProfile, GenerationError> {
    derive_canonical_profile(workspace_root, &named_profile_request(kind))
}

/// Derive a custom profile while enforcing the same fail-closed contract policy.
pub fn derive_canonical_profile(
    workspace_root: &Path,
    request: &CanonicalProfileRequest,
) -> Result<GeneratorProfile, GenerationError> {
    validate_profile_request(request)?;
    let bundle = CanonicalParityContractBundle::load(workspace_root).map_err(|error| {
        GenerationError::invalid_input(
            "canonical_contract_bundle",
            format!("failed to load canonical profile contracts: {error}"),
        )
    })?;
    let validation = bundle.validate(workspace_root);
    if let Some(diagnostic) = validation.diagnostics.first() {
        return Err(GenerationError::invalid_input(
            "canonical_contract_bundle",
            format!(
                "canonical contract validation failed code={} diagnostic={}",
                diagnostic.code, diagnostic.message
            ),
        ));
    }

    let authority_report = canonical_contract_authority_report(workspace_root);
    if let Some(diagnostic) = authority_report.diagnostics.first() {
        return Err(GenerationError::invalid_input(
            "canonical_contract_authority",
            format!(
                "canonical authority validation failed code={} diagnostic={}",
                diagnostic.code, diagnostic.message
            ),
        ));
    }
    let authority_hash = |logical_name: &str| -> Result<String, GenerationError> {
        authority_report
            .authorities
            .iter()
            .find(|authority| authority.logical_name == logical_name)
            .map(|authority| authority.canonical_sha256.clone())
            .filter(|hash| hash.len() == 64)
            .ok_or_else(|| {
                GenerationError::invalid_input(
                    "canonical_contract_authority",
                    format!("missing canonical SHA-256 for {logical_name}"),
                )
            })
    };

    let mut seen_features = BTreeSet::new();
    let mut bindings = Vec::with_capacity(request.features.len());
    for requested in &request.features {
        if !seen_features.insert((requested.role, requested.feature_id.as_str())) {
            return Err(GenerationError::invalid_input(
                "profile.features",
                format!(
                    "duplicate {:?} feature request for {}",
                    requested.role, requested.feature_id
                ),
            ));
        }
        let taxonomy = bundle
            .parity_taxonomy
            .features
            .iter()
            .find(|feature| feature.id == requested.feature_id)
            .ok_or_else(|| {
                GenerationError::invalid_input(
                    "profile.feature_id",
                    format!(
                        "unknown taxonomy feature '{}' in canonical profile '{}'",
                        requested.feature_id, request.name
                    ),
                )
            })?;
        let surface = bundle
            .surface_matrix
            .surface
            .iter()
            .find(|entry| entry.feature_id == requested.surface_id)
            .ok_or_else(|| {
                GenerationError::invalid_input(
                    "profile.surface_id",
                    format!(
                        "unknown surface '{}' for taxonomy feature '{}'",
                        requested.surface_id, requested.feature_id
                    ),
                )
            })?;
        let ledger = bundle
            .feature_ledger
            .features
            .iter()
            .find(|feature| feature.feature_id == requested.ledger_feature_id)
            .ok_or_else(|| {
                GenerationError::invalid_input(
                    "profile.ledger_feature_id",
                    format!(
                        "unknown ledger feature '{}' for taxonomy feature '{}'",
                        requested.ledger_feature_id, requested.feature_id
                    ),
                )
            })?;

        if taxonomy.status != requested.expected_taxonomy_status {
            return Err(stale_profile_state(
                &requested.feature_id,
                "taxonomy_status",
                requested.expected_taxonomy_status,
                taxonomy.status,
            ));
        }
        if surface.support_state != requested.expected_surface_state {
            return Err(stale_profile_state(
                &requested.feature_id,
                "surface_state",
                requested.expected_surface_state,
                surface.support_state,
            ));
        }
        if ledger.lifecycle_state != requested.expected_lifecycle_state {
            return Err(stale_profile_state(
                &requested.feature_id,
                "lifecycle_state",
                requested.expected_lifecycle_state,
                ledger.lifecycle_state,
            ));
        }
        if ledger.surface_id != requested.surface_id {
            return Err(GenerationError::invalid_input(
                "profile.contract_contradiction",
                format!(
                    "taxonomy feature '{}' expects surface '{}' but ledger '{}' binds '{}'",
                    requested.feature_id,
                    requested.surface_id,
                    requested.ledger_feature_id,
                    ledger.surface_id
                ),
            ));
        }
        if ledger.component != requested.component {
            return Err(GenerationError::invalid_input(
                "profile.contract_contradiction",
                format!(
                    "taxonomy feature '{}' expects component '{}' but ledger '{}' binds '{}'",
                    requested.feature_id,
                    requested.component,
                    requested.ledger_feature_id,
                    ledger.component
                ),
            ));
        }
        let family = taxonomy_family(&requested.feature_id).ok_or_else(|| {
            GenerationError::invalid_input(
                "profile.feature_id",
                format!("invalid taxonomy feature id '{}'", requested.feature_id),
            )
        })?;
        if expected_component_for_family(family) != Some(requested.component.as_str()) {
            return Err(GenerationError::invalid_input(
                "profile.component_family",
                format!(
                    "taxonomy feature '{}' family '{}' cannot bind component '{}'",
                    requested.feature_id, family, requested.component
                ),
            ));
        }

        enforce_profile_admission(
            request,
            requested.role,
            taxonomy.status,
            surface.support_state,
            ledger.lifecycle_state,
        )?;
        bindings.push(ProfileBinding {
            role: requested.role,
            construct: requested.construct,
            feature_id: requested.feature_id.clone(),
            taxonomy_status: taxonomy.status,
            taxonomy_weight: taxonomy.weight,
            surface_id: requested.surface_id.clone(),
            surface_state: surface.support_state,
            ledger_feature_id: requested.ledger_feature_id.clone(),
            component: requested.component.clone(),
            lifecycle_state: ledger.lifecycle_state,
        });
    }

    bindings.sort_by(|left, right| {
        (
            left.role,
            left.construct,
            left.feature_id.as_str(),
            left.ledger_feature_id.as_str(),
        )
            .cmp(&(
                right.role,
                right.construct,
                right.feature_id.as_str(),
                right.ledger_feature_id.as_str(),
            ))
    });
    validate_binding_roles(request, &bindings)?;
    let feature_weights = normalize_subject_feature_weights(&bindings)?;
    let weights = aggregate_construct_weights(&feature_weights);
    let mut evidence = CanonicalProfileEvidence {
        schema_version: CANONICAL_PROFILE_SCHEMA_VERSION.to_owned(),
        registry_schema_version: CONTRACT_AUTHORITY_REGISTRY_SCHEMA_VERSION.to_owned(),
        profile_name: request.name.clone(),
        profile_version: request.version.clone(),
        setup: request.setup.clone(),
        sqlite_target: bundle.version_contract.contract.sqlite_target,
        taxonomy_version: bundle.parity_taxonomy.meta.version,
        version_contract_sha256: authority_hash("sqlite_version_contract")?,
        surface_matrix_sha256: authority_hash("supported_surface_matrix")?,
        feature_ledger_sha256: authority_hash("feature_universe_ledger")?,
        parity_taxonomy_sha256: authority_hash("parity_taxonomy")?,
        mode: request.mode,
        expected_gap_policy: request.expected_gap_policy.clone(),
        authorization_bead: request.authorization_bead.clone(),
        required_lanes: request.required_lanes.clone(),
        bindings,
        feature_weights: feature_weights.clone(),
        profile_sha256: String::new(),
    };
    evidence.profile_sha256 = sha256_hex(canonical_json(&evidence)?.as_bytes());

    Ok(GeneratorProfile {
        name: request.name.clone(),
        version: request.version.clone(),
        setup: request.setup.clone(),
        require_subject: true,
        weights,
        feature_weights,
        canonical_evidence: Some(evidence),
    })
}

fn validate_profile_request(request: &CanonicalProfileRequest) -> Result<(), GenerationError> {
    for (field, value) in [
        ("profile.name", request.name.as_str()),
        ("profile.version", request.version.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(GenerationError::invalid_input(field, "must not be empty"));
        }
    }
    if request.setup.is_empty() {
        return Err(GenerationError::invalid_input(
            "profile.setup",
            "canonical profiles require explicit setup constructs",
        ));
    }
    if request.features.is_empty() {
        return Err(GenerationError::invalid_input(
            "profile.features",
            "canonical profiles require feature bindings",
        ));
    }
    if request.required_lanes.is_empty() {
        return Err(GenerationError::invalid_input(
            "profile.required_lanes",
            "canonical profiles require at least one execution lane",
        ));
    }
    let mut lanes = BTreeSet::new();
    if request
        .required_lanes
        .iter()
        .any(|lane| !lanes.insert(*lane))
    {
        return Err(GenerationError::invalid_input(
            "profile.required_lanes",
            "duplicate execution lane",
        ));
    }
    match request.mode {
        ProfileMode::Required => {
            if request
                .expected_gap_policy
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            {
                return Err(GenerationError::invalid_input(
                    "profile.expected_gap_policy",
                    "required profiles cannot carry a gap policy",
                ));
            }
        }
        ProfileMode::Partial => {
            if request
                .expected_gap_policy
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(GenerationError::invalid_input(
                    "profile.expected_gap_policy",
                    "partial profiles require a non-empty expected-gap policy",
                ));
            }
        }
        ProfileMode::FeatureDevelopment => {
            if request
                .authorization_bead
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(GenerationError::invalid_input(
                    "profile.authorization_bead",
                    "feature-development profiles require an authorization bead",
                ));
            }
        }
    }
    Ok(())
}

fn enforce_profile_admission(
    request: &CanonicalProfileRequest,
    role: StatementRole,
    taxonomy_status: ParityTaxonomyStatus,
    surface_state: SupportState,
    lifecycle_state: LifecycleState,
) -> Result<(), GenerationError> {
    if lifecycle_state == LifecycleState::Declared
        && request.mode != ProfileMode::FeatureDevelopment
    {
        return Err(GenerationError::invalid_input(
            "profile.lifecycle_state",
            "declared-only ledger features require feature-development authorization",
        ));
    }
    match request.mode {
        ProfileMode::Required => {
            if taxonomy_status != ParityTaxonomyStatus::Pass
                || surface_state != SupportState::Supported
            {
                return Err(GenerationError::invalid_input(
                    "profile.required_admission",
                    "required profiles admit only pass taxonomy features on supported surfaces",
                ));
            }
        }
        ProfileMode::Partial => {
            if role == StatementRole::Setup
                && (taxonomy_status != ParityTaxonomyStatus::Pass
                    || surface_state != SupportState::Supported)
            {
                return Err(GenerationError::invalid_input(
                    "profile.partial_setup",
                    "partial profiles still require pass/supported setup features",
                ));
            }
            if matches!(
                taxonomy_status,
                ParityTaxonomyStatus::Fail | ParityTaxonomyStatus::Excluded
            ) || surface_state == SupportState::Excluded
            {
                return Err(GenerationError::invalid_input(
                    "profile.partial_admission",
                    "fail or excluded features require feature-development authorization",
                ));
            }
        }
        ProfileMode::FeatureDevelopment => {}
    }
    Ok(())
}

fn validate_binding_roles(
    request: &CanonicalProfileRequest,
    bindings: &[ProfileBinding],
) -> Result<(), GenerationError> {
    let setup_constructs = bindings
        .iter()
        .filter(|binding| binding.role == StatementRole::Setup)
        .map(|binding| binding.construct)
        .collect::<BTreeSet<_>>();
    let setup_binding_count = bindings
        .iter()
        .filter(|binding| binding.role == StatementRole::Setup)
        .count();
    if setup_binding_count != setup_constructs.len() {
        return Err(GenerationError::invalid_input(
            "profile.setup",
            "each setup construct must map to exactly one canonical feature",
        ));
    }
    for construct in &request.setup {
        if !setup_constructs.contains(construct) {
            return Err(GenerationError::invalid_input(
                "profile.setup",
                format!("setup construct {construct:?} has no setup feature binding"),
            ));
        }
    }
    let subject_bindings = bindings
        .iter()
        .filter(|binding| binding.role == StatementRole::Subject)
        .collect::<Vec<_>>();
    if subject_bindings.is_empty() {
        return Err(GenerationError::invalid_input(
            "profile.subject",
            "canonical profiles require at least one subject feature binding",
        ));
    }
    if request.mode == ProfileMode::Partial
        && !subject_bindings
            .iter()
            .any(|binding| binding.taxonomy_status == ParityTaxonomyStatus::Partial)
    {
        return Err(GenerationError::invalid_input(
            "profile.expected_gap_policy",
            "partial profiles must bind at least one partial subject feature",
        ));
    }
    Ok(())
}

fn normalize_subject_feature_weights(
    bindings: &[ProfileBinding],
) -> Result<Vec<ProfileFeatureWeight>, GenerationError> {
    let mut raw_by_feature = BTreeMap::<(String, Construct), u64>::new();
    for binding in bindings
        .iter()
        .filter(|binding| binding.role == StatementRole::Subject)
    {
        let weight = raw_by_feature
            .entry((binding.feature_id.clone(), binding.construct))
            .or_default();
        *weight = weight.saturating_add(binding.taxonomy_weight);
    }
    let total = raw_by_feature
        .values()
        .copied()
        .fold(0_u64, u64::saturating_add);
    if total == 0 {
        return Err(GenerationError::exhausted_choices(
            "profile.weights",
            "subject taxonomy weights sum to zero",
        ));
    }

    let target = u64::from(CANONICAL_PROFILE_WEIGHT_TOTAL);
    let mut normalized = BTreeMap::<(String, Construct), u32>::new();
    let mut remainders = Vec::with_capacity(raw_by_feature.len());
    let mut assigned = 0_u64;
    for ((feature_id, construct), raw) in raw_by_feature {
        let scaled = raw.saturating_mul(target);
        let base = scaled / total;
        let remainder = scaled % total;
        assigned = assigned.saturating_add(base);
        normalized.insert(
            (feature_id.clone(), construct),
            u32::try_from(base).unwrap_or(u32::MAX),
        );
        remainders.push((feature_id, construct, remainder));
    }
    remainders.sort_by(|left, right| {
        right
            .2
            .cmp(&left.2)
            .then_with(|| left.0.cmp(&right.0))
            .then_with(|| left.1.cmp(&right.1))
    });
    for (feature_id, construct, _) in remainders
        .into_iter()
        .take(usize::try_from(target.saturating_sub(assigned)).unwrap_or(usize::MAX))
    {
        let Some(value) = normalized.get_mut(&(feature_id, construct)) else {
            return Err(GenerationError::invalid_input(
                "profile.weights",
                "normalized feature remainder lost its source binding",
            ));
        };
        *value = value.saturating_add(1);
    }
    let weights = normalized
        .into_iter()
        .map(|((feature_id, construct), weight)| ProfileFeatureWeight {
            feature_id,
            construct,
            weight,
        })
        .collect::<Vec<_>>();
    let normalized_total = weights
        .iter()
        .fold(0_u32, |sum, entry| sum.saturating_add(entry.weight));
    if normalized_total != CANONICAL_PROFILE_WEIGHT_TOTAL {
        return Err(GenerationError::invalid_input(
            "profile.weights",
            format!(
                "normalized subject weights total {normalized_total}, expected {CANONICAL_PROFILE_WEIGHT_TOTAL}"
            ),
        ));
    }
    Ok(weights)
}

fn aggregate_construct_weights(feature_weights: &[ProfileFeatureWeight]) -> Vec<ConstructWeight> {
    let mut by_construct = BTreeMap::<Construct, u32>::new();
    for feature in feature_weights {
        let weight = by_construct.entry(feature.construct).or_default();
        *weight = weight.saturating_add(feature.weight);
    }
    by_construct
        .into_iter()
        .map(|(construct, weight)| ConstructWeight { construct, weight })
        .collect()
}

fn stale_profile_state(
    feature_id: &str,
    field: &str,
    expected: impl fmt::Debug,
    observed: impl fmt::Debug,
) -> GenerationError {
    GenerationError::invalid_input(
        "profile.stale_contract_state",
        format!("feature '{feature_id}' expected {field}={expected:?}, observed {observed:?}"),
    )
}

fn taxonomy_family(feature_id: &str) -> Option<&str> {
    let (family, ordinal) = feature_id.strip_prefix("F-")?.split_once('.')?;
    if family.is_empty()
        || !family.chars().all(|ch| ch.is_ascii_uppercase())
        || ordinal.len() < 2
        || !ordinal.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }
    Some(family)
}

fn expected_component_for_family(family: &str) -> Option<&'static str> {
    match family {
        "SQL" => Some("parser"),
        "PLN" => Some("planner"),
        "VDB" => Some("vdbe"),
        "TXN" | "PGM" => Some("core"),
        "EXT" => Some("extension"),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationBudget {
    pub max_ast_depth: u32,
    pub max_statements: u32,
    pub max_rows: u32,
    pub max_value_bytes: u32,
    pub max_execution_steps: u64,
    pub max_rejections: u32,
}

impl Default for GenerationBudget {
    fn default() -> Self {
        Self {
            max_ast_depth: 8,
            max_statements: 32,
            max_rows: 64,
            max_value_bytes: 128,
            max_execution_steps: 4_096,
            max_rejections: 32,
        }
    }
}

impl GenerationBudget {
    fn validate(&self) -> Result<(), GenerationError> {
        let values = [
            ("max_ast_depth", u64::from(self.max_ast_depth)),
            ("max_statements", u64::from(self.max_statements)),
            ("max_rows", u64::from(self.max_rows)),
            ("max_value_bytes", u64::from(self.max_value_bytes)),
            ("max_execution_steps", self.max_execution_steps),
            ("max_rejections", u64::from(self.max_rejections)),
        ];
        if let Some((name, _)) = values.into_iter().find(|(_, value)| *value == 0) {
            return Err(GenerationError::invalid_input(
                name,
                "budget values must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratorConfig {
    pub root_seed: u64,
    pub requested_statements: u32,
    pub profile: GeneratorProfile,
    pub budget: GenerationBudget,
}

impl GeneratorConfig {
    #[must_use]
    pub fn bootstrap(root_seed: u64, requested_statements: u32) -> Self {
        Self {
            root_seed,
            requested_statements,
            profile: GeneratorProfile::bootstrap(),
            budget: GenerationBudget::default(),
        }
    }

    pub fn from_json(json: &str) -> Result<Self, GenerationError> {
        let config: Self = serde_json::from_str(json).map_err(|error| {
            GenerationError::invalid_input(
                "config_json",
                format!("malformed generator config: {error}"),
            )
        })?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), GenerationError> {
        self.profile.validate()?;
        self.budget.validate()?;
        if self.requested_statements == 0 {
            return Err(GenerationError::invalid_input(
                "requested_statements",
                "requested statement count must be greater than zero",
            ));
        }
        if self.requested_statements > self.budget.max_statements {
            return Err(GenerationError::budget_exhausted(
                "max_statements",
                format!(
                    "requested {} statements but budget permits {}",
                    self.requested_statements, self.budget.max_statements
                ),
            ));
        }
        let setup_count = u32::try_from(self.profile.setup.len()).unwrap_or(u32::MAX);
        if self.profile.require_subject && self.requested_statements <= setup_count {
            return Err(GenerationError::invalid_input(
                "requested_statements",
                format!(
                    "canonical profile '{}' requires more than {setup_count} statements so at least one subject is generated",
                    self.profile.name
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceCounters {
    pub proposals: u32,
    pub accepted: u32,
    pub rejected: u32,
    pub rows: u32,
    pub value_bytes: u64,
    pub execution_steps: u64,
    pub maximum_ast_depth: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceOutcome {
    Proposed,
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationTraceEvent {
    pub ordinal: u32,
    pub seed_path: String,
    pub derived_seed: u64,
    pub role: StatementRole,
    pub construct: Construct,
    pub profile_feature_id: Option<String>,
    pub outcome: TraceOutcome,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedStatement {
    pub ordinal: u32,
    pub seed_path: String,
    pub derived_seed: u64,
    pub role: StatementRole,
    pub construct: Construct,
    pub profile_feature_id: Option<String>,
    pub ast: Statement,
    pub sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalClassification {
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedCase {
    pub schema_version: u32,
    pub generator_version: String,
    pub profile_name: String,
    pub profile_version: String,
    pub canonical_profile_evidence: Option<CanonicalProfileEvidence>,
    pub root_seed: u64,
    pub statements: Vec<GeneratedStatement>,
    pub final_schema: SchemaState,
    pub trace: Vec<GenerationTraceEvent>,
    pub counters: ResourceCounters,
    pub sql_hash: String,
    pub trace_hash: String,
    pub schema_hash: String,
    pub terminal_classification: TerminalClassification,
}

impl GeneratedCase {
    pub fn to_canonical_json(&self) -> Result<String, GenerationError> {
        serde_json::to_string_pretty(self).map_err(|error| {
            GenerationError::invalid_input(
                "artifact_serialization",
                format!("generated artifact serialization failed: {error}"),
            )
        })
    }

    #[must_use]
    pub fn sql_script(&self) -> String {
        let mut script = String::new();
        for statement in &self.statements {
            script.push_str(&statement.sql);
            script.push('\n');
        }
        script
    }

    /// Rebuild a generated case after a test-only structured reduction.
    ///
    /// The original generation trace is retained as immutable seed provenance;
    /// reduction decisions are recorded by the canonical mismatch minimizer.
    /// Derived SQL, schema, resource counters, and content hashes are recomputed
    /// from the reduced statement trees so the result can re-enter the public
    /// typed differential adapter.
    pub fn rebuild_with_statements(
        &self,
        mut statements: Vec<GeneratedStatement>,
    ) -> Result<Self, GenerationError> {
        if statements.is_empty() {
            return Err(GenerationError::invalid_input(
                "reduction.statements",
                "a reduced case must retain at least one statement",
            ));
        }

        let mut schema = SchemaState::default();
        let mut transaction_snapshot = None;
        let mut counters = ResourceCounters::default();
        let mut subject_seen = false;
        for statement in &mut statements {
            match statement.role {
                StatementRole::Setup if subject_seen => {
                    return Err(GenerationError::invalid_input(
                        "reduction.statement_order",
                        "setup statement appeared after the first subject statement",
                    ));
                }
                StatementRole::Setup => {}
                StatementRole::Subject => subject_seen = true,
            }
            statement.sql = statement.ast.to_sql();
            apply_statement_to_schema(&mut schema, &mut transaction_snapshot, &statement.ast)?;
            let (_, value_bytes, _) = statement_resources(&statement.ast);
            counters.proposals = counters.proposals.saturating_add(1);
            counters.accepted = counters.accepted.saturating_add(1);
            counters.rows = schema.total_rows();
            counters.value_bytes = counters.value_bytes.saturating_add(value_bytes);
            counters.execution_steps = counters
                .execution_steps
                .saturating_add(statement_cost(&statement.ast));
            counters.maximum_ast_depth = counters.maximum_ast_depth.max(statement.ast.depth());
        }
        if transaction_snapshot.is_some() || schema.transaction_open {
            return Err(GenerationError::invalid_input(
                "reduction.transaction",
                "a reduced case must not end with an open transaction",
            ));
        }
        if !subject_seen {
            return Err(GenerationError::invalid_input(
                "reduction.workload",
                "a reduced case must retain at least one subject statement",
            ));
        }

        let sql_script = statements
            .iter()
            .map(|statement| statement.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let trace_json = canonical_json(&self.trace)?;
        let schema_json = canonical_json(&schema)?;
        Ok(Self {
            schema_version: self.schema_version,
            generator_version: self.generator_version.clone(),
            profile_name: self.profile_name.clone(),
            profile_version: self.profile_version.clone(),
            canonical_profile_evidence: self.canonical_profile_evidence.clone(),
            root_seed: self.root_seed,
            statements,
            final_schema: schema,
            trace: self.trace.clone(),
            counters,
            sql_hash: sha256_hex(sql_script.as_bytes()),
            trace_hash: sha256_hex(trace_json.as_bytes()),
            schema_hash: sha256_hex(schema_json.as_bytes()),
            terminal_classification: self.terminal_classification,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationErrorKind {
    InvalidInput,
    BudgetExhausted,
    ImpossibleSchema,
    ExhaustedChoices,
    PendingProposal,
    ProposalMismatch,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationError {
    pub kind: GenerationErrorKind,
    pub constraint: String,
    pub message: String,
}

impl GenerationError {
    fn new(
        kind: GenerationErrorKind,
        constraint: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            constraint: constraint.into(),
            message: message.into(),
        }
    }

    fn invalid_input(constraint: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(GenerationErrorKind::InvalidInput, constraint, message)
    }

    fn budget_exhausted(constraint: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(GenerationErrorKind::BudgetExhausted, constraint, message)
    }

    fn impossible_schema(constraint: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(GenerationErrorKind::ImpossibleSchema, constraint, message)
    }

    fn exhausted_choices(constraint: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(GenerationErrorKind::ExhaustedChoices, constraint, message)
    }
}

impl fmt::Display for GenerationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.constraint, self.message)
    }
}

impl std::error::Error for GenerationError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementProposal {
    pub proposal_id: u64,
    pub statement: GeneratedStatement,
}

/// Stateful proposal interface. Callers accept only after both engines accept.
pub struct GenerationSession {
    config: GeneratorConfig,
    schema: SchemaState,
    transaction_snapshot: Option<SchemaState>,
    statements: Vec<GeneratedStatement>,
    trace: Vec<GenerationTraceEvent>,
    counters: ResourceCounters,
    pending: Option<StatementProposal>,
}

impl GenerationSession {
    pub fn new(config: GeneratorConfig) -> Result<Self, GenerationError> {
        config.validate()?;
        Ok(Self {
            config,
            schema: SchemaState::default(),
            transaction_snapshot: None,
            statements: Vec::new(),
            trace: Vec::new(),
            counters: ResourceCounters::default(),
            pending: None,
        })
    }

    #[must_use]
    pub const fn schema(&self) -> &SchemaState {
        &self.schema
    }

    #[must_use]
    pub const fn counters(&self) -> ResourceCounters {
        self.counters
    }

    pub fn propose_next(&mut self) -> Result<StatementProposal, GenerationError> {
        let (role, construct, profile_feature_id) =
            self.next_profile_construct(self.counters.accepted, self.counters.proposals)?;
        self.propose_construct_with_role(role, construct, profile_feature_id)
    }

    pub fn propose_construct(
        &mut self,
        construct: Construct,
    ) -> Result<StatementProposal, GenerationError> {
        let profile_feature_id = if let Some(evidence) = &self.config.profile.canonical_evidence {
            let matches = evidence
                .bindings
                .iter()
                .filter(|binding| {
                    binding.role == StatementRole::Subject && binding.construct == construct
                })
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [binding] => Some(binding.feature_id.clone()),
                [] => {
                    return Err(GenerationError::invalid_input(
                        "profile.subject",
                        format!(
                            "construct {construct:?} is not admitted by canonical profile '{}'",
                            self.config.profile.name
                        ),
                    ));
                }
                _ => {
                    return Err(GenerationError::invalid_input(
                        "profile.subject",
                        format!(
                            "construct {construct:?} maps to multiple canonical features; use propose_next for weighted feature lineage"
                        ),
                    ));
                }
            }
        } else {
            None
        };
        self.propose_construct_with_role(StatementRole::Subject, construct, profile_feature_id)
    }

    fn propose_construct_with_role(
        &mut self,
        role: StatementRole,
        construct: Construct,
        profile_feature_id: Option<String>,
    ) -> Result<StatementProposal, GenerationError> {
        if self.pending.is_some() {
            return Err(GenerationError::new(
                GenerationErrorKind::PendingProposal,
                "pending_proposal",
                "accept or reject the current proposal before requesting another",
            ));
        }
        if self.counters.accepted >= self.config.budget.max_statements {
            return Err(GenerationError::budget_exhausted(
                "max_statements",
                "statement budget is exhausted",
            ));
        }

        let ordinal = self.counters.proposals;
        let feature_seed_path = profile_feature_id.as_deref().unwrap_or("unbound");
        let seed_path = format!(
            "statement/{ordinal}/{}/{feature_seed_path}/{construct:?}",
            role.label()
        )
        .to_ascii_lowercase();
        let derived_seed = derive_seed(self.config.root_seed, &seed_path);
        let mut rng = StableRng::new(derived_seed);
        let ast = self.generate_statement(construct, &mut rng)?;
        self.validate_statement_budget(&ast)?;
        let sql = ast.to_sql();
        let proposal_id = derive_seed(derived_seed, &sql);
        let statement = GeneratedStatement {
            ordinal,
            seed_path: seed_path.clone(),
            derived_seed,
            role,
            construct,
            profile_feature_id: profile_feature_id.clone(),
            ast,
            sql,
        };
        let proposal = StatementProposal {
            proposal_id,
            statement,
        };

        self.counters.proposals = self.counters.proposals.saturating_add(1);
        self.trace.push(GenerationTraceEvent {
            ordinal,
            seed_path,
            derived_seed,
            role,
            construct,
            profile_feature_id: profile_feature_id.clone(),
            outcome: TraceOutcome::Proposed,
            detail: "candidate generated without mutating schema".to_owned(),
        });
        debug!(
            root_seed = self.config.root_seed,
            derived_seed,
            ordinal,
            role = %role.label(),
            construct = ?construct,
            profile_feature_id = ?profile_feature_id,
            "typed SQL statement proposed"
        );
        self.pending = Some(proposal.clone());
        Ok(proposal)
    }

    pub fn accept(&mut self, proposal_id: u64) -> Result<(), GenerationError> {
        let proposal = self.take_matching_proposal(proposal_id)?;
        self.apply_accepted_statement(&proposal.statement.ast)?;
        self.update_resource_counters(&proposal.statement.ast);
        self.counters.accepted = self.counters.accepted.saturating_add(1);
        self.trace.push(GenerationTraceEvent {
            ordinal: proposal.statement.ordinal,
            seed_path: proposal.statement.seed_path.clone(),
            derived_seed: proposal.statement.derived_seed,
            role: proposal.statement.role,
            construct: proposal.statement.construct,
            profile_feature_id: proposal.statement.profile_feature_id.clone(),
            outcome: TraceOutcome::Accepted,
            detail: "caller accepted proposal after engine validation".to_owned(),
        });
        self.statements.push(proposal.statement);
        Ok(())
    }

    pub fn reject(
        &mut self,
        proposal_id: u64,
        reason: impl Into<String>,
    ) -> Result<(), GenerationError> {
        let proposal = self.take_matching_proposal(proposal_id)?;
        self.counters.rejected = self.counters.rejected.saturating_add(1);
        if self.counters.rejected > self.config.budget.max_rejections {
            return Err(GenerationError::budget_exhausted(
                "max_rejections",
                "proposal rejection budget is exhausted",
            ));
        }
        self.trace.push(GenerationTraceEvent {
            ordinal: proposal.statement.ordinal,
            seed_path: proposal.statement.seed_path,
            derived_seed: proposal.statement.derived_seed,
            role: proposal.statement.role,
            construct: proposal.statement.construct,
            profile_feature_id: proposal.statement.profile_feature_id,
            outcome: TraceOutcome::Rejected,
            detail: reason.into(),
        });
        Ok(())
    }

    pub fn finish(self) -> Result<GeneratedCase, GenerationError> {
        if self.pending.is_some() {
            return Err(GenerationError::new(
                GenerationErrorKind::PendingProposal,
                "pending_proposal",
                "cannot finish with an unresolved proposal",
            ));
        }
        if self.counters.accepted != self.config.requested_statements {
            return Err(GenerationError::budget_exhausted(
                "requested_statements",
                format!(
                    "accepted {} of {} requested statements",
                    self.counters.accepted, self.config.requested_statements
                ),
            ));
        }
        if let Some(evidence) = &self.config.profile.canonical_evidence {
            for statement in &self.statements {
                let Some(feature_id) = statement.profile_feature_id.as_deref() else {
                    return Err(GenerationError::invalid_input(
                        "profile.feature_lineage",
                        format!(
                            "canonical statement ordinal {} is missing its feature id",
                            statement.ordinal
                        ),
                    ));
                };
                if !evidence.bindings.iter().any(|binding| {
                    binding.role == statement.role
                        && binding.construct == statement.construct
                        && binding.feature_id == feature_id
                }) {
                    return Err(GenerationError::invalid_input(
                        "profile.feature_lineage",
                        format!(
                            "canonical statement ordinal {} has unknown binding role={} construct={:?} feature_id={feature_id}",
                            statement.ordinal,
                            statement.role.label(),
                            statement.construct
                        ),
                    ));
                }
            }
        }

        let sql_script = self
            .statements
            .iter()
            .map(|statement| statement.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let trace_json = canonical_json(&self.trace)?;
        let schema_json = canonical_json(&self.schema)?;
        let generated = GeneratedCase {
            schema_version: GENERATOR_SCHEMA_VERSION,
            generator_version: GENERATOR_VERSION.to_owned(),
            profile_name: self.config.profile.name,
            profile_version: self.config.profile.version,
            canonical_profile_evidence: self.config.profile.canonical_evidence,
            root_seed: self.config.root_seed,
            statements: self.statements,
            final_schema: self.schema,
            trace: self.trace,
            counters: self.counters,
            sql_hash: sha256_hex(sql_script.as_bytes()),
            trace_hash: sha256_hex(trace_json.as_bytes()),
            schema_hash: sha256_hex(schema_json.as_bytes()),
            terminal_classification: TerminalClassification::Complete,
        };
        info!(
            root_seed = generated.root_seed,
            profile = %generated.profile_name,
            accepted = generated.counters.accepted,
            rejected = generated.counters.rejected,
            rows = generated.counters.rows,
            maximum_ast_depth = generated.counters.maximum_ast_depth,
            sql_hash = %generated.sql_hash,
            "typed SQL generation complete"
        );
        Ok(generated)
    }

    fn next_profile_construct(
        &self,
        accepted_ordinal: u32,
        proposal_ordinal: u32,
    ) -> Result<(StatementRole, Construct, Option<String>), GenerationError> {
        if let Some(construct) = self
            .config
            .profile
            .setup
            .get(usize::try_from(accepted_ordinal).unwrap_or(usize::MAX))
        {
            let feature_id = self
                .config
                .profile
                .canonical_evidence
                .as_ref()
                .and_then(|evidence| {
                    evidence.bindings.iter().find(|binding| {
                        binding.role == StatementRole::Setup && binding.construct == *construct
                    })
                })
                .map(|binding| binding.feature_id.clone());
            if self.config.profile.canonical_evidence.is_some() && feature_id.is_none() {
                return Err(GenerationError::invalid_input(
                    "profile.setup",
                    format!("setup construct {construct:?} lost its canonical feature binding"),
                ));
            }
            return Ok((StatementRole::Setup, *construct, feature_id));
        }
        let seed_path = format!("statement/{proposal_ordinal}/weighted_construct");
        let seed = derive_seed(self.config.root_seed, &seed_path);
        if self.config.profile.feature_weights.is_empty() {
            return choose_weighted(&self.config.profile.weights, seed)
                .map(|construct| (StatementRole::Subject, construct, None));
        }
        choose_weighted_feature(&self.config.profile.feature_weights, seed).map(|feature| {
            (
                StatementRole::Subject,
                feature.construct,
                Some(feature.feature_id.clone()),
            )
        })
    }

    fn take_matching_proposal(
        &mut self,
        proposal_id: u64,
    ) -> Result<StatementProposal, GenerationError> {
        let proposal = self.pending.take().ok_or_else(|| {
            GenerationError::new(
                GenerationErrorKind::ProposalMismatch,
                "proposal_id",
                "no proposal is pending",
            )
        })?;
        if proposal.proposal_id != proposal_id {
            self.pending = Some(proposal);
            return Err(GenerationError::new(
                GenerationErrorKind::ProposalMismatch,
                "proposal_id",
                "proposal identifier does not match the pending proposal",
            ));
        }
        Ok(proposal)
    }

    fn generate_statement(
        &self,
        construct: Construct,
        rng: &mut StableRng,
    ) -> Result<Statement, GenerationError> {
        match construct {
            Construct::CreateTable => self.generate_create_table(),
            Construct::CreateIndex => self.generate_create_index(),
            Construct::Insert => self.generate_insert(rng),
            Construct::Update => self.generate_update(rng),
            Construct::Delete => self.generate_delete(rng),
            Construct::Select => self.generate_select(rng),
            Construct::Join => self.generate_join_select(),
            Construct::Aggregate => self.generate_aggregate_select(),
            Construct::Subquery => self.generate_subquery_select(),
            Construct::CompoundSelect => self.generate_compound_select(),
            Construct::Transaction => Ok(self.generate_transaction()),
        }
    }

    fn generate_create_table(&self) -> Result<Statement, GenerationError> {
        let schema_ordinal = self.schema.tables.len();
        let proposal_ordinal = self.counters.proposals;
        let table = ident(format!("t{schema_ordinal}_p{proposal_ordinal}"))?;
        Ok(Statement::CreateTable {
            table,
            columns: vec![
                ColumnSpec {
                    name: ident("id")?,
                    affinity: ColumnAffinity::Integer,
                    primary_key: true,
                    not_null: false,
                },
                ColumnSpec {
                    name: ident("label")?,
                    affinity: ColumnAffinity::Text,
                    primary_key: false,
                    not_null: true,
                },
                ColumnSpec {
                    name: ident("score")?,
                    affinity: ColumnAffinity::Integer,
                    primary_key: false,
                    not_null: false,
                },
            ],
        })
    }

    fn generate_create_index(&self) -> Result<Statement, GenerationError> {
        let table = self.first_table()?;
        Ok(Statement::CreateIndex {
            index: ident(format!(
                "idx_{}_score_p{}",
                table.name.as_str(),
                self.counters.proposals
            ))?,
            table: table.name.clone(),
            columns: vec![ident("score")?],
            unique: false,
        })
    }

    fn generate_insert(&self, rng: &mut StableRng) -> Result<Statement, GenerationError> {
        let table = self
            .schema
            .tables
            .iter()
            .min_by_key(|table| table.estimated_rows)
            .ok_or_else(|| {
                GenerationError::impossible_schema(
                    "insert.table",
                    "INSERT requires at least one accepted table",
                )
            })?;
        let next_id = i64::from(table.estimated_rows) + 1;
        let suffix = rng.next_u64() % 10_000;
        Ok(Statement::Insert {
            table: table.name.clone(),
            columns: table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            rows: vec![vec![
                SqlValue::Integer(next_id),
                SqlValue::Text(format!("row_{suffix}")),
                SqlValue::Integer(rng.next_i64(-32, 32)),
            ]],
        })
    }

    fn generate_update(&self, rng: &mut StableRng) -> Result<Statement, GenerationError> {
        let table = self.first_table()?;
        Ok(Statement::Update {
            table: table.name.clone(),
            assignments: vec![(
                ident("score")?,
                Expr::literal(SqlValue::Integer(rng.next_i64(-8, 8))),
            )],
            predicate: Some(equal_expr(
                Expr::column(None, ident("id")?),
                Expr::literal(SqlValue::Integer(1)),
            )),
        })
    }

    fn generate_delete(&self, rng: &mut StableRng) -> Result<Statement, GenerationError> {
        let table = self.first_table()?;
        Ok(Statement::Delete {
            table: table.name.clone(),
            predicate: Some(Expr::Binary {
                left: Box::new(Expr::column(None, ident("id")?)),
                op: BinaryOp::Greater,
                right: Box::new(Expr::literal(SqlValue::Integer(rng.next_i64(8, 32)))),
            }),
        })
    }

    fn generate_select(&self, rng: &mut StableRng) -> Result<Statement, GenerationError> {
        let table = self.first_table()?;
        Ok(Statement::Select {
            select: Select {
                distinct: rng.next_bool(),
                projection: vec![SelectItem {
                    expr: Expr::column(None, ident("id")?),
                    alias: None,
                }],
                from: Some(FromItem {
                    table: table.name.clone(),
                    alias: None,
                }),
                joins: Vec::new(),
                predicate: Some(Expr::Binary {
                    left: Box::new(Expr::column(None, ident("score")?)),
                    op: BinaryOp::GreaterEqual,
                    right: Box::new(Expr::literal(SqlValue::Integer(rng.next_i64(-16, 16)))),
                }),
                group_by: Vec::new(),
                having: None,
                compound: None,
                order_by: vec![OrderTerm {
                    expr: Expr::column(None, ident("id")?),
                    direction: if rng.next_bool() {
                        OrderDirection::Asc
                    } else {
                        OrderDirection::Desc
                    },
                }],
                limit: Some(rng.next_u32(1, 8)),
            },
        })
    }

    fn generate_join_select(&self) -> Result<Statement, GenerationError> {
        let (left, right) = self.two_tables("join")?;
        let left_alias = ident("lhs")?;
        let right_alias = ident("rhs")?;
        Ok(Statement::Select {
            select: Select {
                distinct: false,
                projection: vec![
                    SelectItem {
                        expr: Expr::column(Some(left_alias.clone()), ident("id")?),
                        alias: Some(ident("left_id")?),
                    },
                    SelectItem {
                        expr: Expr::column(Some(right_alias.clone()), ident("label")?),
                        alias: Some(ident("right_label")?),
                    },
                ],
                from: Some(FromItem {
                    table: left.name.clone(),
                    alias: Some(left_alias.clone()),
                }),
                joins: vec![Join {
                    kind: JoinKind::Inner,
                    table: right.name.clone(),
                    alias: Some(right_alias.clone()),
                    on: equal_expr(
                        Expr::column(Some(left_alias.clone()), ident("id")?),
                        Expr::column(Some(right_alias), ident("id")?),
                    ),
                }],
                predicate: None,
                group_by: Vec::new(),
                having: None,
                compound: None,
                order_by: vec![OrderTerm {
                    expr: Expr::column(Some(left_alias), ident("id")?),
                    direction: OrderDirection::Asc,
                }],
                limit: Some(8),
            },
        })
    }

    fn generate_aggregate_select(&self) -> Result<Statement, GenerationError> {
        let table = self.first_table()?;
        let aggregate = Expr::Aggregate {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
        };
        Ok(Statement::Select {
            select: Select {
                distinct: false,
                projection: vec![SelectItem {
                    expr: aggregate.clone(),
                    alias: Some(ident("row_count")?),
                }],
                from: Some(FromItem {
                    table: table.name.clone(),
                    alias: None,
                }),
                joins: Vec::new(),
                predicate: None,
                group_by: Vec::new(),
                having: Some(Expr::Binary {
                    left: Box::new(aggregate),
                    op: BinaryOp::GreaterEqual,
                    right: Box::new(Expr::literal(SqlValue::Integer(0))),
                }),
                compound: None,
                order_by: Vec::new(),
                limit: None,
            },
        })
    }

    fn generate_subquery_select(&self) -> Result<Statement, GenerationError> {
        let (outer, inner) = self.two_tables("subquery")?;
        let subquery = simple_id_select(inner.name.clone())?;
        Ok(Statement::Select {
            select: Select {
                distinct: false,
                projection: vec![SelectItem {
                    expr: Expr::column(None, ident("id")?),
                    alias: None,
                }],
                from: Some(FromItem {
                    table: outer.name.clone(),
                    alias: None,
                }),
                joins: Vec::new(),
                predicate: Some(Expr::InSubquery {
                    expr: Box::new(Expr::column(None, ident("id")?)),
                    subquery: Box::new(subquery),
                    negated: false,
                }),
                group_by: Vec::new(),
                having: None,
                compound: None,
                order_by: vec![OrderTerm {
                    expr: Expr::column(None, ident("id")?),
                    direction: OrderDirection::Asc,
                }],
                limit: None,
            },
        })
    }

    fn generate_compound_select(&self) -> Result<Statement, GenerationError> {
        let (left, right) = self.two_tables("compound_select")?;
        let mut select = simple_id_select(left.name.clone())?;
        select.compound = Some(CompoundSelect {
            operator: CompoundOperator::UnionAll,
            right: Box::new(simple_id_select(right.name.clone())?),
        });
        select.order_by.push(OrderTerm {
            expr: Expr::column(None, ident("id")?),
            direction: OrderDirection::Asc,
        });
        Ok(Statement::Select { select })
    }

    fn generate_transaction(&self) -> Statement {
        Statement::Transaction {
            statement: if self.schema.transaction_open {
                TransactionStatement::Commit
            } else {
                TransactionStatement::Begin
            },
        }
    }

    fn first_table(&self) -> Result<&TableState, GenerationError> {
        self.schema.tables.first().ok_or_else(|| {
            GenerationError::impossible_schema(
                "schema.table",
                "construct requires at least one accepted table",
            )
        })
    }

    fn two_tables(&self, construct: &str) -> Result<(&TableState, &TableState), GenerationError> {
        if self.schema.tables.len() < 2 {
            return Err(GenerationError::impossible_schema(
                format!("{construct}.tables"),
                format!("{construct} requires two accepted tables"),
            ));
        }
        Ok((&self.schema.tables[0], &self.schema.tables[1]))
    }

    fn validate_statement_budget(&self, statement: &Statement) -> Result<(), GenerationError> {
        let depth = statement.depth();
        if depth > self.config.budget.max_ast_depth {
            return Err(GenerationError::budget_exhausted(
                "max_ast_depth",
                format!(
                    "statement depth {depth} exceeds {}",
                    self.config.budget.max_ast_depth
                ),
            ));
        }
        let (rows, _, maximum_value_bytes) = statement_resources(statement);
        if self.schema.total_rows().saturating_add(rows) > self.config.budget.max_rows {
            return Err(GenerationError::budget_exhausted(
                "max_rows",
                "statement would exceed the row budget",
            ));
        }
        if maximum_value_bytes > u64::from(self.config.budget.max_value_bytes) {
            return Err(GenerationError::budget_exhausted(
                "max_value_bytes",
                format!(
                    "statement value size {maximum_value_bytes} exceeds {}",
                    self.config.budget.max_value_bytes
                ),
            ));
        }
        let steps = statement_cost(statement);
        if self.counters.execution_steps.saturating_add(steps)
            > self.config.budget.max_execution_steps
        {
            return Err(GenerationError::budget_exhausted(
                "max_execution_steps",
                "statement would exceed the execution-step budget",
            ));
        }
        Ok(())
    }

    fn update_resource_counters(&mut self, statement: &Statement) {
        let (_, value_bytes, _) = statement_resources(statement);
        self.counters.rows = self.schema.total_rows();
        self.counters.value_bytes = self.counters.value_bytes.saturating_add(value_bytes);
        self.counters.execution_steps = self
            .counters
            .execution_steps
            .saturating_add(statement_cost(statement));
        self.counters.maximum_ast_depth = self.counters.maximum_ast_depth.max(statement.depth());
    }

    fn apply_accepted_statement(&mut self, statement: &Statement) -> Result<(), GenerationError> {
        apply_statement_to_schema(&mut self.schema, &mut self.transaction_snapshot, statement)
    }
}

fn apply_statement_to_schema(
    schema: &mut SchemaState,
    transaction_snapshot: &mut Option<SchemaState>,
    statement: &Statement,
) -> Result<(), GenerationError> {
    match statement {
        Statement::CreateTable { table, columns } => {
            if schema.table(table).is_some() {
                return Err(GenerationError::impossible_schema(
                    "create_table.name",
                    "accepted table name already exists",
                ));
            }
            if columns.is_empty() {
                return Err(GenerationError::impossible_schema(
                    "create_table.columns",
                    "accepted table requires at least one column",
                ));
            }
            schema.tables.push(TableState {
                name: table.clone(),
                columns: columns.clone(),
                estimated_rows: 0,
            });
        }
        Statement::CreateIndex {
            index,
            table,
            columns,
            unique,
        } => {
            if schema.indexes.iter().any(|entry| entry.name == *index) {
                return Err(GenerationError::impossible_schema(
                    "create_index.name",
                    "accepted index name already exists",
                ));
            }
            let table_state = schema.table(table).ok_or_else(|| {
                GenerationError::impossible_schema(
                    "create_index.table",
                    "accepted index references an unknown table",
                )
            })?;
            if columns.is_empty()
                || columns.iter().any(|name| {
                    !table_state
                        .columns
                        .iter()
                        .any(|column| column.name == *name)
                })
            {
                return Err(GenerationError::impossible_schema(
                    "create_index.columns",
                    "accepted index references an unknown or empty column list",
                ));
            }
            schema.indexes.push(IndexState {
                name: index.clone(),
                table: table.clone(),
                columns: columns.clone(),
                unique: *unique,
            });
        }
        Statement::Insert {
            table,
            columns,
            rows,
        } => {
            let table_state = schema
                .tables
                .iter_mut()
                .find(|entry| entry.name == *table)
                .ok_or_else(|| {
                    GenerationError::impossible_schema(
                        "insert.table",
                        "accepted INSERT references an unknown table",
                    )
                })?;
            if columns.is_empty()
                || rows.is_empty()
                || rows.iter().any(|row| row.len() != columns.len())
            {
                return Err(GenerationError::impossible_schema(
                    "insert.rows",
                    "accepted INSERT has an empty or mismatched row shape",
                ));
            }
            let mut unique_columns = BTreeSet::new();
            if columns.iter().any(|name| {
                !unique_columns.insert(name)
                    || !table_state
                        .columns
                        .iter()
                        .any(|column| column.name == *name)
            }) {
                return Err(GenerationError::impossible_schema(
                    "insert.columns",
                    "accepted INSERT references an unknown or duplicate column",
                ));
            }
            table_state.estimated_rows = table_state
                .estimated_rows
                .saturating_add(u32::try_from(rows.len()).unwrap_or(u32::MAX));
        }
        Statement::Delete { table, .. } => {
            if schema.table(table).is_none() {
                return Err(GenerationError::impossible_schema(
                    "delete.table",
                    "accepted DELETE references an unknown table",
                ));
            }
            // Predicate cardinality is unknown until the execution adapter
            // reports it. Retaining the conservative row estimate avoids
            // manufacturing primary-key reuse after a no-op DELETE.
        }
        Statement::Update {
            table, assignments, ..
        } => {
            let table_state = schema.table(table).ok_or_else(|| {
                GenerationError::impossible_schema(
                    "update.table",
                    "accepted UPDATE references an unknown table",
                )
            })?;
            if assignments.is_empty()
                || assignments.iter().any(|(name, _)| {
                    !table_state
                        .columns
                        .iter()
                        .any(|column| column.name == *name)
                })
            {
                return Err(GenerationError::impossible_schema(
                    "update.assignments",
                    "accepted UPDATE references an unknown or empty assignment list",
                ));
            }
        }
        Statement::Select { .. } => {}
        Statement::Transaction { statement } => match statement {
            TransactionStatement::Begin => {
                if schema.transaction_open {
                    return Err(GenerationError::impossible_schema(
                        "transaction.begin",
                        "a transaction is already open",
                    ));
                }
                *transaction_snapshot = Some(schema.clone());
                schema.transaction_open = true;
            }
            TransactionStatement::Commit => {
                if !schema.transaction_open {
                    return Err(GenerationError::impossible_schema(
                        "transaction.commit",
                        "no transaction is open",
                    ));
                }
                *transaction_snapshot = None;
                schema.transaction_open = false;
            }
            TransactionStatement::Rollback => {
                let mut snapshot = transaction_snapshot.take().ok_or_else(|| {
                    GenerationError::impossible_schema(
                        "transaction.rollback",
                        "no transaction is open",
                    )
                })?;
                snapshot.transaction_open = false;
                *schema = snapshot;
            }
        },
    }
    Ok(())
}

/// Generate a deterministic test-local case. Production campaign adapters must
/// use [`GenerationSession`] and accept only after both engines accept.
pub fn generate_case(config: GeneratorConfig) -> Result<GeneratedCase, GenerationError> {
    generate_case_with_cancel(config, || false)
}

/// Generate a case while checking cancellation before every proposal.
pub fn generate_case_with_cancel(
    config: GeneratorConfig,
    mut is_cancelled: impl FnMut() -> bool,
) -> Result<GeneratedCase, GenerationError> {
    let requested = config.requested_statements;
    let mut session = GenerationSession::new(config)?;
    while session.counters.accepted < requested {
        if is_cancelled() {
            return Err(GenerationError::new(
                GenerationErrorKind::Cancelled,
                "cancellation",
                format!(
                    "generation cancelled after {} accepted statements",
                    session.counters.accepted
                ),
            ));
        }
        let proposal = session.propose_next()?;
        session.accept(proposal.proposal_id)?;
    }
    session.finish()
}

/// Stable domain-separated seed derivation.
#[must_use]
pub fn derive_seed(root_seed: u64, path: &str) -> u64 {
    let mut bytes = Vec::with_capacity(SEED_DOMAIN.len() + 8 + path.len());
    bytes.extend_from_slice(SEED_DOMAIN);
    bytes.extend_from_slice(&root_seed.to_le_bytes());
    bytes.extend_from_slice(path.as_bytes());
    xxh3_64(&bytes)
}

fn choose_weighted(
    choices: &[ConstructWeight],
    random_value: u64,
) -> Result<Construct, GenerationError> {
    let total = choices.iter().fold(0_u64, |sum, entry| {
        sum.saturating_add(u64::from(entry.weight))
    });
    if total == 0 {
        return Err(GenerationError::exhausted_choices(
            "profile.weights",
            "weighted choice has no positive entries",
        ));
    }
    let mut target = random_value % total;
    for entry in choices {
        let weight = u64::from(entry.weight);
        if target < weight {
            return Ok(entry.construct);
        }
        target -= weight;
    }
    Err(GenerationError::exhausted_choices(
        "profile.weights",
        "weighted choice traversal exhausted unexpectedly",
    ))
}

fn choose_weighted_feature(
    choices: &[ProfileFeatureWeight],
    random_value: u64,
) -> Result<&ProfileFeatureWeight, GenerationError> {
    let total = choices.iter().fold(0_u64, |sum, entry| {
        sum.saturating_add(u64::from(entry.weight))
    });
    if total == 0 {
        return Err(GenerationError::exhausted_choices(
            "profile.feature_weights",
            "weighted feature choice has no positive entries",
        ));
    }
    let mut target = random_value % total;
    for entry in choices {
        let weight = u64::from(entry.weight);
        if target < weight {
            return Ok(entry);
        }
        target -= weight;
    }
    Err(GenerationError::exhausted_choices(
        "profile.feature_weights",
        "weighted feature choice traversal exhausted unexpectedly",
    ))
}

fn ident(value: impl Into<String>) -> Result<Identifier, GenerationError> {
    Identifier::new(value)
}

fn equal_expr(left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::Equal,
        right: Box::new(right),
    }
}

fn simple_id_select(table: Identifier) -> Result<Select, GenerationError> {
    Ok(Select {
        distinct: false,
        projection: vec![SelectItem {
            expr: Expr::column(None, ident("id")?),
            alias: None,
        }],
        from: Some(FromItem { table, alias: None }),
        joins: Vec::new(),
        predicate: None,
        group_by: Vec::new(),
        having: None,
        compound: None,
        order_by: Vec::new(),
        limit: None,
    })
}

fn statement_resources(statement: &Statement) -> (u32, u64, u64) {
    match statement {
        Statement::Insert { rows, .. } => {
            let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
            let bytes = rows.iter().flatten().fold(0_u64, |total, value| {
                total.saturating_add(u64::try_from(value.encoded_len()).unwrap_or(u64::MAX))
            });
            let maximum = rows
                .iter()
                .flatten()
                .map(SqlValue::encoded_len)
                .max()
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or(0);
            (row_count, bytes, maximum)
        }
        _ => (0, 0, 0),
    }
}

fn statement_cost(statement: &Statement) -> u64 {
    let (rows, _, _) = statement_resources(statement);
    1_u64
        .saturating_add(u64::from(statement.depth()))
        .saturating_add(u64::from(rows))
}

fn canonical_json(value: &impl Serialize) -> Result<String, GenerationError> {
    serde_json::to_string(value).map_err(|error| {
        GenerationError::invalid_input(
            "canonical_serialization",
            format!("canonical serialization failed: {error}"),
        )
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(output, "{byte:02x}").expect("write to String");
    }
    output
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

/// SplitMix64 is specified here rather than delegated to a dependency whose
/// stream may change between releases.
struct StableRng {
    state: u64,
}

impl StableRng {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn next_bool(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    fn next_u32(&mut self, min: u32, max: u32) -> u32 {
        let width = max.saturating_sub(min).saturating_add(1);
        min.saturating_add(u32::try_from(self.next_u64() % u64::from(width)).unwrap_or(0))
    }

    fn next_i64(&mut self, min: i64, max: i64) -> i64 {
        let width = u64::try_from(max.saturating_sub(min)).unwrap_or(0) + 1;
        min.saturating_add(i64::try_from(self.next_u64() % width).unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::fs;
    use std::path::PathBuf;
    use std::process::Command;

    use fsqlite_parser::Parser;
    use proptest::prelude::*;

    use crate::differential_v2::{CsqliteExecutor, FsqliteExecutor, SqlExecutor, StmtOutcome};

    use super::*;

    const TEST_SEED: u64 = 0xD1FF_E2E0_2026_0804;
    const CHILD_ENV: &str = "FSQLITE_TYPED_SQL_GENERATOR_CHILD";
    const CHILD_ARTIFACT_ENV: &str = "FSQLITE_TYPED_SQL_GENERATOR_ARTIFACT";

    fn assert_fsqlite_parses(sql: &str) {
        Parser::from_sql(sql)
            .parse_statement()
            .unwrap_or_else(|error| panic!("FrankenSQLite parser rejected {sql:?}: {error}"));
    }

    fn bootstrap_case() -> GeneratedCase {
        generate_case(GeneratorConfig::bootstrap(TEST_SEED, 20)).expect("generate bootstrap case")
    }

    #[test]
    fn reduced_case_recomputes_derived_state_and_retains_seed_trace() {
        let original = bootstrap_case();
        let mut statements = original.statements.clone();
        let removable = statements
            .iter()
            .position(|statement| {
                statement.role == StatementRole::Subject
                    && matches!(statement.ast, Statement::Select { .. })
            })
            .expect("bootstrap profile must contain a subject SELECT");
        statements.remove(removable);

        let reduced = original
            .rebuild_with_statements(statements)
            .expect("rebuild a valid reduced case");
        assert_eq!(reduced.trace, original.trace);
        assert_eq!(reduced.trace_hash, original.trace_hash);
        assert_ne!(reduced.sql_hash, original.sql_hash);
        assert_eq!(reduced.schema_hash, original.schema_hash);
        assert_eq!(
            reduced.counters.accepted,
            u32::try_from(reduced.statements.len()).unwrap()
        );
        assert!(
            reduced
                .statements
                .iter()
                .all(|statement| statement.sql == statement.ast.to_sql())
        );
    }

    #[test]
    fn reduced_case_rejects_missing_setup_and_unclosed_transactions() {
        let original = bootstrap_case();
        let insert = original
            .statements
            .iter()
            .find(|statement| matches!(statement.ast, Statement::Insert { .. }))
            .expect("bootstrap profile must contain an INSERT")
            .clone();
        assert_eq!(
            original
                .rebuild_with_statements(vec![insert])
                .unwrap_err()
                .constraint,
            "insert.table"
        );

        let mut transaction = original
            .statements
            .iter()
            .find(|statement| statement.role == StatementRole::Subject)
            .expect("bootstrap profile must contain a subject statement")
            .clone();
        transaction.ast = Statement::Transaction {
            statement: TransactionStatement::Begin,
        };
        transaction.sql = transaction.ast.to_sql();
        assert_eq!(
            original
                .rebuild_with_statements(vec![transaction])
                .unwrap_err()
                .constraint,
            "reduction.transaction"
        );
    }

    fn compare_paired_statement_outcome(
        fsqlite: &FsqliteExecutor,
        sqlite: &CsqliteExecutor,
        sql: &str,
    ) -> Result<(), String> {
        match (fsqlite.run_stmt(sql), sqlite.run_stmt(sql)) {
            (StmtOutcome::Rows(fsqlite_rows), StmtOutcome::Rows(sqlite_rows))
                if fsqlite_rows == sqlite_rows =>
            {
                Ok(())
            }
            (StmtOutcome::Execute(fsqlite_count), StmtOutcome::Execute(sqlite_count))
                if fsqlite_count == sqlite_count =>
            {
                Ok(())
            }
            (fsqlite_outcome, sqlite_outcome) => Err(format!(
                "SQL {sql:?}: FrankenSQLite={fsqlite_outcome:?}, C SQLite={sqlite_outcome:?}"
            )),
        }
    }

    fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    #[cfg(unix)]
    fn copy_profile_contract_fixture(source_root: &Path, fixture_root: &Path) {
        use std::os::unix::fs::symlink;

        use crate::canonical_parity_contract::{
            CANONICAL_CONTRACT_AUTHORITIES, PARITY_SCORE_CONTRACT_PATH,
        };

        fs::create_dir_all(fixture_root.join("docs/contracts"))
            .expect("create fixture contract directory");
        for authority in CANONICAL_CONTRACT_AUTHORITIES {
            fs::copy(
                source_root.join(authority.canonical_path),
                fixture_root.join(authority.canonical_path),
            )
            .expect("copy canonical contract");
            fs::copy(
                source_root.join(authority.inert_root_path),
                fixture_root.join(authority.inert_root_path),
            )
            .expect("copy inert root pointer");
        }
        fs::copy(
            source_root.join(PARITY_SCORE_CONTRACT_PATH),
            fixture_root.join(PARITY_SCORE_CONTRACT_PATH),
        )
        .expect("copy parity score contract");
        for path in ["AGENTS.md", "README.md"] {
            fs::copy(source_root.join(path), fixture_root.join(path))
                .expect("copy root evidence file");
        }
        symlink(source_root.join("crates"), fixture_root.join("crates"))
            .expect("link contract evidence tree");
    }

    #[cfg(unix)]
    #[test]
    fn temporary_registry_fixture_derives_and_contract_drift_fails_closed() {
        use crate::canonical_parity_contract::PARITY_TAXONOMY_PATH;

        let root = workspace_root();
        let fixture = tempfile::tempdir().expect("temporary canonical fixture");
        copy_profile_contract_fixture(&root, fixture.path());

        let profile = derive_named_profile(fixture.path(), NamedGeneratorProfile::ReadOnly)
            .expect("temporary canonical registry should derive profile");
        validate_canonical_profile_evidence(
            profile
                .canonical_evidence
                .as_ref()
                .expect("canonical profile evidence"),
        )
        .unwrap();

        let taxonomy_path = fixture.path().join(PARITY_TAXONOMY_PATH);
        let taxonomy = fs::read_to_string(&taxonomy_path).expect("read fixture taxonomy");
        let drifted = taxonomy.replacen("id = \"F-SQL.02\"", "id = \"F-SQL.01\"", 1);
        assert_ne!(
            taxonomy, drifted,
            "fixture must contain the pinned feature ID"
        );
        fs::write(&taxonomy_path, drifted).expect("write duplicate feature fixture");
        let error = derive_named_profile(fixture.path(), NamedGeneratorProfile::ReadOnly)
            .expect_err("duplicate canonical feature ID must fail closed");
        assert_eq!(error.constraint, "canonical_contract_bundle");
        assert!(error.message.contains("duplicate_taxonomy_feature_id"));
        assert!(error.message.contains(PARITY_TAXONOMY_PATH));
    }

    #[test]
    fn every_named_profile_derives_with_exact_contract_and_weight_evidence() {
        let root = workspace_root();
        for kind in [
            NamedGeneratorProfile::SupportedCore,
            NamedGeneratorProfile::ReadOnly,
            NamedGeneratorProfile::Dml,
            NamedGeneratorProfile::Planner,
            NamedGeneratorProfile::Vdbe,
            NamedGeneratorProfile::Transaction,
            NamedGeneratorProfile::Mvcc,
            NamedGeneratorProfile::PlannerPartial,
            NamedGeneratorProfile::VdbePartial,
        ] {
            let profile = derive_named_profile(&root, kind)
                .unwrap_or_else(|error| panic!("profile {kind:?} failed: {error}"));
            assert_eq!(
                profile
                    .weights
                    .iter()
                    .map(|entry| entry.weight)
                    .sum::<u32>(),
                CANONICAL_PROFILE_WEIGHT_TOTAL
            );
            assert!(profile.require_subject);
            let evidence = profile
                .canonical_evidence
                .as_ref()
                .expect("named profile evidence");
            assert_eq!(evidence.profile_name, kind.label());
            assert_eq!(evidence.sqlite_target, "3.52.0");
            assert_eq!(evidence.taxonomy_version, "1.0.0");
            assert_eq!(evidence.profile_sha256.len(), 64);
            assert_eq!(evidence.version_contract_sha256.len(), 64);
            assert_eq!(evidence.surface_matrix_sha256.len(), 64);
            assert_eq!(evidence.feature_ledger_sha256.len(), 64);
            assert_eq!(evidence.parity_taxonomy_sha256.len(), 64);
            validate_canonical_profile_evidence(evidence).unwrap();
            assert!(!evidence.required_lanes.is_empty());
            assert!(evidence.bindings.iter().any(|binding| {
                binding.role == StatementRole::Setup
                    && binding.taxonomy_status == ParityTaxonomyStatus::Pass
                    && binding.surface_state == SupportState::Supported
            }));
            assert!(evidence.bindings.iter().any(|binding| {
                binding.role == StatementRole::Subject
                    && (evidence.mode == ProfileMode::Partial
                        || binding.taxonomy_status == ParityTaxonomyStatus::Pass)
            }));
        }
    }

    #[test]
    fn profile_derivation_is_deterministic_and_hash_covers_evidence() {
        let root = workspace_root();
        let first = derive_named_profile(&root, NamedGeneratorProfile::SupportedCore).unwrap();
        let second = derive_named_profile(&root, NamedGeneratorProfile::SupportedCore).unwrap();
        assert_eq!(first, second);
        let mut evidence = first.canonical_evidence.expect("canonical evidence");
        let expected_hash = evidence.profile_sha256.clone();
        evidence.profile_sha256.clear();
        assert_eq!(
            sha256_hex(canonical_json(&evidence).unwrap().as_bytes()),
            expected_hash
        );
        evidence.profile_sha256 = expected_hash;
        evidence.taxonomy_version.push_str("-altered");
        assert_eq!(
            validate_canonical_profile_evidence(&evidence)
                .unwrap_err()
                .constraint,
            "profile.canonical_evidence.profile_sha256"
        );
    }

    #[test]
    fn canonical_setup_and_subject_roles_do_not_leak() {
        let root = workspace_root();
        let profile = derive_named_profile(&root, NamedGeneratorProfile::ReadOnly).unwrap();
        let setup_len = profile.setup.len();
        let mut too_short = GeneratorConfig::bootstrap(
            TEST_SEED,
            u32::try_from(setup_len).expect("bounded setup length"),
        );
        too_short.profile = profile.clone();
        assert_eq!(
            generate_case(too_short).unwrap_err().constraint,
            "requested_statements"
        );

        let requested = u32::try_from(setup_len + 8).unwrap();
        let generated = generate_case(GeneratorConfig {
            root_seed: TEST_SEED,
            requested_statements: requested,
            profile: profile.clone(),
            budget: GenerationBudget::default(),
        })
        .unwrap();
        assert!(
            generated.statements[..setup_len]
                .iter()
                .all(|statement| statement.role == StatementRole::Setup)
        );
        assert!(
            generated.statements[setup_len..]
                .iter()
                .all(|statement| statement.role == StatementRole::Subject)
        );
        let subject_constructs = profile
            .weights
            .iter()
            .map(|weight| weight.construct)
            .collect::<BTreeSet<_>>();
        assert!(
            generated.statements[setup_len..]
                .iter()
                .all(|statement| subject_constructs.contains(&statement.construct))
        );
        assert!(generated.trace.iter().all(|event| {
            generated.statements.iter().any(|statement| {
                event.ordinal == statement.ordinal
                    && event.role == statement.role
                    && event.construct == statement.construct
            })
        }));
    }

    #[test]
    fn stale_unknown_duplicate_and_partial_profile_requests_fail_closed() {
        let root = workspace_root();

        let mut unknown = named_profile_request(NamedGeneratorProfile::ReadOnly);
        unknown.features[0].feature_id = "F-SQL.999".to_owned();
        assert_eq!(
            derive_canonical_profile(&root, &unknown)
                .unwrap_err()
                .constraint,
            "profile.feature_id"
        );

        let mut duplicate = named_profile_request(NamedGeneratorProfile::ReadOnly);
        duplicate.features.push(duplicate.features[0].clone());
        assert_eq!(
            derive_canonical_profile(&root, &duplicate)
                .unwrap_err()
                .constraint,
            "profile.features"
        );

        let mut stale = named_profile_request(NamedGeneratorProfile::ReadOnly);
        stale.features[0].expected_taxonomy_status = ParityTaxonomyStatus::Partial;
        assert_eq!(
            derive_canonical_profile(&root, &stale)
                .unwrap_err()
                .constraint,
            "profile.stale_contract_state"
        );

        let mut contradiction = named_profile_request(NamedGeneratorProfile::ReadOnly);
        contradiction.features[0].component = "planner".to_owned();
        assert_eq!(
            derive_canonical_profile(&root, &contradiction)
                .unwrap_err()
                .constraint,
            "profile.contract_contradiction"
        );

        let mut partial = named_profile_request(NamedGeneratorProfile::PlannerPartial);
        partial.expected_gap_policy = None;
        assert_eq!(
            derive_canonical_profile(&root, &partial)
                .unwrap_err()
                .constraint,
            "profile.expected_gap_policy"
        );
    }

    #[test]
    fn excluded_declared_features_require_explicit_development_authorization() {
        let root = workspace_root();
        let mut request = named_profile_request(NamedGeneratorProfile::Dml);
        request.mode = ProfileMode::FeatureDevelopment;
        request.name = "fts3_feature_development".to_owned();
        request
            .features
            .retain(|feature| feature.role == StatementRole::Setup);
        request.features.push(ProfileFeatureRequest {
            role: StatementRole::Subject,
            construct: Construct::Select,
            feature_id: "F-EXT.01".to_owned(),
            surface_id: "SURF-EXT-FTS3-014".to_owned(),
            ledger_feature_id: "LEDGER-0019".to_owned(),
            component: "extension".to_owned(),
            expected_taxonomy_status: ParityTaxonomyStatus::Partial,
            expected_surface_state: SupportState::Excluded,
            expected_lifecycle_state: LifecycleState::Declared,
        });
        request.authorization_bead = None;
        assert_eq!(
            derive_canonical_profile(&root, &request)
                .unwrap_err()
                .constraint,
            "profile.authorization_bead"
        );
        request.authorization_bead = Some("bd-feature-development-review".to_owned());
        let profile = derive_canonical_profile(&root, &request).unwrap();
        assert_eq!(
            profile
                .canonical_evidence
                .expect("development evidence")
                .authorization_bead
                .as_deref(),
            Some("bd-feature-development-review")
        );
    }

    #[test]
    fn fixed_seed_canonical_profile_executes_on_paired_engines() {
        let root = workspace_root();
        let profile = derive_named_profile(&root, NamedGeneratorProfile::ReadOnly).unwrap();
        let generated = generate_case(GeneratorConfig {
            root_seed: TEST_SEED,
            requested_statements: u32::try_from(profile.setup.len() + 8).unwrap(),
            profile,
            budget: GenerationBudget::default(),
        })
        .unwrap();
        let fsqlite = FsqliteExecutor::open_in_memory().unwrap();
        let sqlite = CsqliteExecutor::open_in_memory().unwrap();
        for statement in &generated.statements {
            compare_paired_statement_outcome(&fsqlite, &sqlite, &statement.sql)
                .unwrap_or_else(|error| panic!("paired engine mismatch: {error}"));
        }
        assert!(generated.canonical_profile_evidence.is_some());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(18))]

        #[test]
        fn canonical_profile_generation_preserves_exact_binding_lineage(
            root_seed in any::<u64>(),
            profile_index in 0_usize..9,
            subject_count in 1_usize..=8,
        ) {
            let kinds = [
                NamedGeneratorProfile::SupportedCore,
                NamedGeneratorProfile::ReadOnly,
                NamedGeneratorProfile::Dml,
                NamedGeneratorProfile::Planner,
                NamedGeneratorProfile::Vdbe,
                NamedGeneratorProfile::Transaction,
                NamedGeneratorProfile::Mvcc,
                NamedGeneratorProfile::PlannerPartial,
                NamedGeneratorProfile::VdbePartial,
            ];
            let kind = kinds
                .get(profile_index)
                .copied()
                .expect("generated profile index is bounded");
            let profile = derive_named_profile(&workspace_root(), kind)
                .expect("named profile must derive");
            let requested_statements = u32::try_from(profile.setup.len() + subject_count)
                .expect("bounded statement count");
            let generated = generate_case(GeneratorConfig {
                root_seed,
                requested_statements,
                profile,
                budget: GenerationBudget::default(),
            })
            .expect("canonical profile must generate");
            let evidence = generated
                .canonical_profile_evidence
                .as_ref()
                .expect("canonical evidence");
            prop_assert_eq!(
                evidence
                    .feature_weights
                    .iter()
                    .map(|weight| weight.weight)
                    .sum::<u32>(),
                CANONICAL_PROFILE_WEIGHT_TOTAL
            );
            let lineage_is_exact = generated.statements.iter().all(|statement| {
                statement.profile_feature_id.as_deref().is_some_and(|feature_id| {
                    evidence.bindings.iter().any(|binding| {
                        binding.role == statement.role
                            && binding.construct == statement.construct
                            && binding.feature_id == feature_id
                    })
                })
            });
            prop_assert!(lineage_is_exact);
        }
    }

    #[test]
    fn identifier_and_value_printer_escape_boundaries() {
        let table = Identifier::new("select\"table").unwrap();
        let statement = Statement::Insert {
            table,
            columns: vec![Identifier::new("value").unwrap()],
            rows: vec![
                vec![SqlValue::Text("a'b".to_owned())],
                vec![SqlValue::Blob(vec![0, 255])],
            ],
        };
        assert_eq!(
            statement.to_sql(),
            "INSERT INTO \"select\"\"table\" (\"value\") VALUES ('a''b'), (X'00FF');"
        );
        assert!(Identifier::new("").is_err());
        assert!(Identifier::new("nul\0name").is_err());
        assert!(RealLiteral::new("NaN").is_err());
        assert_eq!(RealLiteral::new("-0.0").unwrap().as_str(), "0.0");
    }

    #[test]
    fn every_statement_and_expression_branch_prints_valid_sql() {
        let mut session = GenerationSession::new(GeneratorConfig::bootstrap(TEST_SEED, 1)).unwrap();
        let create = session.propose_construct(Construct::CreateTable).unwrap();
        assert_fsqlite_parses(&create.statement.sql);
        session.accept(create.proposal_id).unwrap();

        let mut statements = Vec::new();
        for construct in [
            Construct::Insert,
            Construct::CreateIndex,
            Construct::Select,
            Construct::Update,
            Construct::Delete,
            Construct::Transaction,
        ] {
            let proposal = session.propose_construct(construct).unwrap();
            assert_fsqlite_parses(&proposal.statement.sql);
            statements.push(proposal.statement.sql.clone());
            session.accept(proposal.proposal_id).unwrap();
        }

        let second = session.propose_construct(Construct::CreateTable).unwrap();
        session.accept(second.proposal_id).unwrap();
        for construct in [
            Construct::Join,
            Construct::Aggregate,
            Construct::Subquery,
            Construct::CompoundSelect,
        ] {
            let proposal = session.propose_construct(construct).unwrap();
            assert_fsqlite_parses(&proposal.statement.sql);
            statements.push(proposal.statement.sql.clone());
            session.accept(proposal.proposal_id).unwrap();
        }

        let table = ident("t0").unwrap();
        let expression_variants = [
            Expr::Unary {
                op: UnaryOp::Negate,
                expr: Box::new(Expr::literal(SqlValue::Integer(1))),
            },
            Expr::IsNull {
                expr: Box::new(Expr::column(None, ident("score").unwrap())),
                negated: true,
            },
            Expr::ScalarSubquery {
                subquery: Box::new(simple_id_select(table.clone()).unwrap()),
            },
            Expr::Aggregate {
                function: AggregateFunction::Sum,
                expr: Some(Box::new(Expr::column(None, ident("score").unwrap()))),
                distinct: true,
            },
        ];
        for expr in expression_variants {
            let sql = Statement::Select {
                select: Select {
                    distinct: false,
                    projection: vec![SelectItem { expr, alias: None }],
                    from: Some(FromItem {
                        table: table.clone(),
                        alias: None,
                    }),
                    joins: Vec::new(),
                    predicate: None,
                    group_by: Vec::new(),
                    having: None,
                    compound: None,
                    order_by: Vec::new(),
                    limit: None,
                },
            }
            .to_sql();
            assert_fsqlite_parses(&sql);
        }
        assert!(!statements.is_empty());
    }

    #[test]
    fn every_operator_affinity_join_compound_and_value_variant_prints() {
        let table = ident("all_variants").unwrap();
        let affinities = [
            ColumnAffinity::Integer,
            ColumnAffinity::Real,
            ColumnAffinity::Text,
            ColumnAffinity::Blob,
            ColumnAffinity::Numeric,
        ];
        let create = Statement::CreateTable {
            table: table.clone(),
            columns: affinities
                .into_iter()
                .enumerate()
                .map(|(index, affinity)| ColumnSpec {
                    name: ident(format!("c{index}")).unwrap(),
                    affinity,
                    primary_key: index == 0,
                    not_null: index == 1,
                })
                .collect(),
        };
        assert_fsqlite_parses(&create.to_sql());

        let values = [
            SqlValue::Null,
            SqlValue::Integer(i64::MIN),
            SqlValue::Real(RealLiteral::new("1.25").unwrap()),
            SqlValue::Text("quote'boundary".to_owned()),
            SqlValue::Blob(vec![0, 127, 255]),
        ];
        assert_fsqlite_parses(
            &Statement::Select {
                select: Select {
                    distinct: true,
                    projection: values
                        .into_iter()
                        .map(|value| SelectItem {
                            expr: Expr::literal(value),
                            alias: None,
                        })
                        .collect(),
                    from: None,
                    joins: Vec::new(),
                    predicate: None,
                    group_by: Vec::new(),
                    having: None,
                    compound: None,
                    order_by: Vec::new(),
                    limit: None,
                },
            }
            .to_sql(),
        );

        for op in [UnaryOp::Negate, UnaryOp::Not, UnaryOp::BitNot] {
            let sql = projection_sql(Expr::Unary {
                op,
                expr: Box::new(Expr::literal(SqlValue::Integer(1))),
            });
            assert_fsqlite_parses(&sql);
        }
        for op in [
            BinaryOp::Add,
            BinaryOp::Subtract,
            BinaryOp::Multiply,
            BinaryOp::Divide,
            BinaryOp::Equal,
            BinaryOp::NotEqual,
            BinaryOp::Less,
            BinaryOp::LessEqual,
            BinaryOp::Greater,
            BinaryOp::GreaterEqual,
            BinaryOp::And,
            BinaryOp::Or,
            BinaryOp::Concat,
        ] {
            let sql = projection_sql(Expr::Binary {
                left: Box::new(Expr::literal(SqlValue::Integer(4))),
                op,
                right: Box::new(Expr::literal(SqlValue::Integer(2))),
            });
            assert_fsqlite_parses(&sql);
        }
        for function in [
            AggregateFunction::Count,
            AggregateFunction::Sum,
            AggregateFunction::Min,
            AggregateFunction::Max,
            AggregateFunction::Avg,
        ] {
            let sql = projection_sql(Expr::Aggregate {
                function,
                expr: Some(Box::new(Expr::literal(SqlValue::Integer(1)))),
                distinct: true,
            });
            assert_fsqlite_parses(&sql);
        }
        for negated in [false, true] {
            assert_fsqlite_parses(&projection_sql(Expr::IsNull {
                expr: Box::new(Expr::literal(SqlValue::Null)),
                negated,
            }));
            assert_fsqlite_parses(&projection_sql(Expr::InSubquery {
                expr: Box::new(Expr::literal(SqlValue::Integer(1))),
                subquery: Box::new(simple_id_select(table.clone()).unwrap()),
                negated,
            }));
        }

        for kind in [JoinKind::Inner, JoinKind::Left] {
            let select = Select {
                distinct: false,
                projection: vec![SelectItem {
                    expr: Expr::column(Some(ident("lhs").unwrap()), ident("c0").unwrap()),
                    alias: None,
                }],
                from: Some(FromItem {
                    table: table.clone(),
                    alias: Some(ident("lhs").unwrap()),
                }),
                joins: vec![Join {
                    kind,
                    table: table.clone(),
                    alias: Some(ident("rhs").unwrap()),
                    on: equal_expr(
                        Expr::column(Some(ident("lhs").unwrap()), ident("c0").unwrap()),
                        Expr::column(Some(ident("rhs").unwrap()), ident("c0").unwrap()),
                    ),
                }],
                predicate: None,
                group_by: Vec::new(),
                having: None,
                compound: None,
                order_by: Vec::new(),
                limit: None,
            };
            assert_fsqlite_parses(&Statement::Select { select }.to_sql());
        }

        for operator in [
            CompoundOperator::Union,
            CompoundOperator::UnionAll,
            CompoundOperator::Intersect,
            CompoundOperator::Except,
        ] {
            let mut select = simple_id_select(table.clone()).unwrap();
            select.compound = Some(CompoundSelect {
                operator,
                right: Box::new(simple_id_select(table.clone()).unwrap()),
            });
            assert_fsqlite_parses(&Statement::Select { select }.to_sql());
        }
        for direction in [OrderDirection::Asc, OrderDirection::Desc] {
            let mut select = simple_id_select(table.clone()).unwrap();
            select.order_by.push(OrderTerm {
                expr: Expr::column(None, ident("c0").unwrap()),
                direction,
            });
            assert_fsqlite_parses(&Statement::Select { select }.to_sql());
        }
        for statement in [
            TransactionStatement::Begin,
            TransactionStatement::Commit,
            TransactionStatement::Rollback,
        ] {
            assert_fsqlite_parses(&Statement::Transaction { statement }.to_sql());
        }
    }

    fn projection_sql(expr: Expr) -> String {
        Statement::Select {
            select: Select {
                distinct: false,
                projection: vec![SelectItem { expr, alias: None }],
                from: None,
                joins: Vec::new(),
                predicate: None,
                group_by: Vec::new(),
                having: None,
                compound: None,
                order_by: Vec::new(),
                limit: None,
            },
        }
        .to_sql()
    }

    #[test]
    fn rejected_proposal_advances_seed_lineage_without_mutating_schema() {
        let mut session = GenerationSession::new(GeneratorConfig::bootstrap(TEST_SEED, 1)).unwrap();
        let before = session.schema().clone();
        let rejected = session.propose_next().unwrap();
        session
            .reject(rejected.proposal_id, "oracle rejected")
            .unwrap();
        assert_eq!(session.schema(), &before);
        assert_eq!(session.counters().rejected, 1);

        let retry = session.propose_next().unwrap();
        assert_eq!(rejected.statement.construct, Construct::CreateTable);
        assert_eq!(retry.statement.construct, Construct::CreateTable);
        assert_eq!(rejected.statement.ordinal, 0);
        assert_eq!(retry.statement.ordinal, 1);
        assert_ne!(rejected.statement.seed_path, retry.statement.seed_path);
        assert_ne!(
            rejected.statement.derived_seed,
            retry.statement.derived_seed
        );
        assert_ne!(rejected.proposal_id, retry.proposal_id);
        assert_ne!(rejected.statement.sql, retry.statement.sql);
        session.accept(retry.proposal_id).unwrap();
        assert_eq!(session.schema().tables.len(), 1);
    }

    #[test]
    fn accepted_no_op_delete_does_not_reuse_generated_primary_key() {
        let mut session = GenerationSession::new(GeneratorConfig::bootstrap(TEST_SEED, 4)).unwrap();
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();

        for construct in [Construct::CreateTable, Construct::Insert, Construct::Delete] {
            let proposal = session.propose_construct(construct).unwrap();
            sqlite.execute_batch(&proposal.statement.sql).unwrap();
            session.accept(proposal.proposal_id).unwrap();
        }

        assert_eq!(session.schema().tables[0].estimated_rows, 1);
        let second_insert = session.propose_construct(Construct::Insert).unwrap();
        sqlite.execute_batch(&second_insert.statement.sql).unwrap();
        assert!(second_insert.statement.sql.contains("VALUES (2,"));
    }

    #[test]
    fn transaction_rollback_restores_schema_model() {
        let mut session = GenerationSession::new(GeneratorConfig::bootstrap(TEST_SEED, 4)).unwrap();
        let create = session.propose_construct(Construct::CreateTable).unwrap();
        session.accept(create.proposal_id).unwrap();
        let begin = session.propose_construct(Construct::Transaction).unwrap();
        session.accept(begin.proposal_id).unwrap();
        let insert = session.propose_construct(Construct::Insert).unwrap();
        session.accept(insert.proposal_id).unwrap();
        assert_eq!(session.schema().total_rows(), 1);
        let rollback = Statement::Transaction {
            statement: TransactionStatement::Rollback,
        };
        session.apply_accepted_statement(&rollback).unwrap();
        assert_eq!(session.schema().total_rows(), 0);
        assert!(!session.schema().transaction_open);
    }

    #[test]
    fn stable_seed_vectors_and_weight_boundaries() {
        assert_eq!(derive_seed(0, "statement/0"), 16_214_625_229_517_900_363);
        assert_eq!(
            derive_seed(TEST_SEED, "statement/7"),
            15_012_522_081_494_179_931
        );
        let choices = [
            ConstructWeight {
                construct: Construct::Select,
                weight: 2,
            },
            ConstructWeight {
                construct: Construct::Insert,
                weight: 3,
            },
        ];
        assert_eq!(choose_weighted(&choices, 0).unwrap(), Construct::Select);
        assert_eq!(choose_weighted(&choices, 1).unwrap(), Construct::Select);
        assert_eq!(choose_weighted(&choices, 2).unwrap(), Construct::Insert);
        assert_eq!(choose_weighted(&choices, 4).unwrap(), Construct::Insert);
        assert_eq!(choose_weighted(&choices, 5).unwrap(), Construct::Select);
    }

    #[test]
    fn same_seed_profile_and_schema_are_byte_identical() {
        let first = bootstrap_case().to_canonical_json().unwrap();
        let second = bootstrap_case().to_canonical_json().unwrap();
        assert_eq!(first.as_bytes(), second.as_bytes());
    }

    #[test]
    fn generated_bootstrap_case_executes_on_both_engines() {
        let generated = bootstrap_case();
        let fsqlite = FsqliteExecutor::open_in_memory().unwrap();
        let sqlite = CsqliteExecutor::open_in_memory().unwrap();
        for statement in &generated.statements {
            assert_fsqlite_parses(&statement.sql);
            compare_paired_statement_outcome(&fsqlite, &sqlite, &statement.sql)
                .unwrap_or_else(|error| panic!("paired engine mismatch: {error}"));
        }
        assert_eq!(generated.counters.accepted, 20);
        assert_eq!(generated.statements.len(), 20);
        assert_eq!(
            generated.terminal_classification,
            TerminalClassification::Complete
        );
        assert!(!generated.sql_hash.is_empty());
        assert!(!generated.trace_hash.is_empty());
        assert!(!generated.schema_hash.is_empty());
    }

    #[test]
    fn impossible_schema_pending_and_mismatched_proposals_fail_closed() {
        let mut session = GenerationSession::new(GeneratorConfig::bootstrap(TEST_SEED, 1)).unwrap();
        let impossible = session.propose_construct(Construct::Join).unwrap_err();
        assert_eq!(impossible.kind, GenerationErrorKind::ImpossibleSchema);
        let proposal = session.propose_construct(Construct::CreateTable).unwrap();
        assert_eq!(
            session
                .propose_construct(Construct::CreateTable)
                .unwrap_err()
                .kind,
            GenerationErrorKind::PendingProposal
        );
        assert_eq!(
            session
                .accept(proposal.proposal_id.wrapping_add(1))
                .unwrap_err()
                .kind,
            GenerationErrorKind::ProposalMismatch
        );
        session.accept(proposal.proposal_id).unwrap();
    }

    #[test]
    fn malformed_config_and_every_budget_fail_with_named_constraint() {
        assert_eq!(
            GeneratorConfig::from_json("{").unwrap_err().constraint,
            "config_json"
        );
        let mut config = GeneratorConfig::bootstrap(TEST_SEED, 2);
        config.budget.max_statements = 1;
        assert_eq!(
            generate_case(config).unwrap_err().constraint,
            "max_statements"
        );

        let mut depth = GeneratorConfig::bootstrap(TEST_SEED, 12);
        depth.budget.max_ast_depth = 1;
        assert_eq!(
            generate_case(depth).unwrap_err().constraint,
            "max_ast_depth"
        );

        let mut rows = GeneratorConfig::bootstrap(TEST_SEED, 5);
        rows.budget.max_rows = 1;
        assert_eq!(generate_case(rows).unwrap_err().constraint, "max_rows");

        let mut values = GeneratorConfig::bootstrap(TEST_SEED, 4);
        values.budget.max_value_bytes = 1;
        assert_eq!(
            generate_case(values).unwrap_err().constraint,
            "max_value_bytes"
        );

        let mut steps = GeneratorConfig::bootstrap(TEST_SEED, 2);
        steps.budget.max_execution_steps = 2;
        assert_eq!(
            generate_case(steps).unwrap_err().constraint,
            "max_execution_steps"
        );

        let mut weights = GeneratorConfig::bootstrap(TEST_SEED, 1);
        weights.profile.weights.clear();
        assert_eq!(
            generate_case(weights).unwrap_err().kind,
            GenerationErrorKind::ExhaustedChoices
        );
    }

    #[test]
    fn cancellation_is_counted_and_actionable() {
        let calls = Cell::new(0_u32);
        let error = generate_case_with_cancel(GeneratorConfig::bootstrap(TEST_SEED, 12), || {
            let next = calls.get().saturating_add(1);
            calls.set(next);
            next > 4
        })
        .unwrap_err();
        assert_eq!(error.kind, GenerationErrorKind::Cancelled);
        assert!(error.message.contains("4 accepted statements"));
    }

    #[test]
    fn clean_process_generator_child() {
        if std::env::var_os(CHILD_ENV).is_none() {
            return;
        }
        let path =
            PathBuf::from(std::env::var_os(CHILD_ARTIFACT_ENV).expect("child artifact path"));
        let bytes = bootstrap_case().to_canonical_json().unwrap();
        std::fs::write(path, bytes).expect("write child artifact");
    }

    #[test]
    fn clean_process_fixed_seed_artifact_is_byte_identical() {
        let temp = tempfile::tempdir().unwrap();
        let child_artifact = temp.path().join("generated-case.json");
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("typed_sql_generator::tests::clean_process_generator_child")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD_ENV, "1")
            .env(CHILD_ARTIFACT_ENV, &child_artifact)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let child = std::fs::read(&child_artifact).unwrap();
        let parent = bootstrap_case().to_canonical_json().unwrap();
        assert_eq!(child, parent.as_bytes());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        #[test]
        fn generated_cases_execute_and_compare_on_fresh_paired_engines(
            root_seed in any::<u64>(),
            requested_statements in 8_u32..=16,
        ) {
            let generated = generate_case(GeneratorConfig::bootstrap(
                root_seed,
                requested_statements,
            ))
            .expect("bounded typed case must generate");
            let fsqlite = FsqliteExecutor::open_in_memory()
                .expect("open fresh FrankenSQLite executor");
            let sqlite = CsqliteExecutor::open_in_memory()
                .expect("open fresh C SQLite executor");

            for statement in &generated.statements {
                if let Err(error) =
                    compare_paired_statement_outcome(&fsqlite, &sqlite, &statement.sql)
                {
                    prop_assert!(
                        false,
                        "paired statement mismatch for seed {}: {}",
                        root_seed,
                        error
                    );
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn seed_derivation_is_deterministic_and_domain_separated(root in any::<u64>(), path in "[a-z0-9_/]{1,48}") {
            prop_assert_eq!(derive_seed(root, &path), derive_seed(root, &path));
            prop_assert_ne!(derive_seed(root, &path), derive_seed(root, &format!("{path}/other")));
        }

        #[test]
        fn text_and_identifier_printers_always_parse(
            text in "[ -~]{0,64}",
            identifier in "[A-Za-z_][A-Za-z0-9_\"]{0,24}",
        ) {
            let statement = Statement::Select {
                select: Select {
                    distinct: false,
                    projection: vec![SelectItem {
                        expr: Expr::literal(SqlValue::Text(text)),
                        alias: Some(Identifier::new(identifier).unwrap()),
                    }],
                    from: None,
                    joins: Vec::new(),
                    predicate: None,
                    group_by: Vec::new(),
                    having: None,
                    compound: None,
                    order_by: Vec::new(),
                    limit: None,
                },
            };
            prop_assert!(Parser::from_sql(&statement.to_sql()).parse_statement().is_ok());
        }

        #[test]
        fn fixed_seed_generation_round_trips_stably(root in any::<u64>(), statements in 1_u32..=24) {
            let first = generate_case(GeneratorConfig::bootstrap(root, statements)).unwrap();
            let encoded = first.to_canonical_json().unwrap();
            let decoded: GeneratedCase = serde_json::from_str(&encoded).unwrap();
            prop_assert_eq!(first, decoded);
        }
    }
}
