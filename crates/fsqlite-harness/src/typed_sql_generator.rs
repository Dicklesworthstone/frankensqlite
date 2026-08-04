//! Independent typed SQL generator for differential testing.
//!
//! This module deliberately owns its AST and printer. It does not use
//! `fsqlite-ast` or any production SQL formatting path, so parser and printer
//! defects cannot validate one another. Campaign integration, capability
//! profiles, and execution against both engines are layered on top by the
//! dependent Turso-adaptation beads.

use std::collections::BTreeSet;
use std::fmt::{self, Write as _};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info};
use xxhash_rust::xxh3::xxh3_64;

/// Stable artifact schema for generated cases.
pub const GENERATOR_SCHEMA_VERSION: u32 = 1;
/// Generator implementation version. Seed compatibility is scoped to this value.
pub const GENERATOR_VERSION: &str = "1.0.0";
/// The test-local profile used before canonical capability mapping is added by `.4`.
pub const BOOTSTRAP_PROFILE_NAME: &str = "supported_core_bootstrap";
/// Version of the test-local bootstrap profile.
pub const BOOTSTRAP_PROFILE_VERSION: &str = "1.0.0";

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstructWeight {
    pub construct: Construct,
    pub weight: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratorProfile {
    pub name: String,
    pub version: String,
    pub weights: Vec<ConstructWeight>,
}

impl GeneratorProfile {
    #[must_use]
    pub fn bootstrap() -> Self {
        Self {
            name: BOOTSTRAP_PROFILE_NAME.to_owned(),
            version: BOOTSTRAP_PROFILE_VERSION.to_owned(),
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
        let mut constructs = BTreeSet::new();
        for entry in &self.weights {
            if !constructs.insert(entry.construct) {
                return Err(GenerationError::invalid_input(
                    "profile.weights",
                    "profile contains a duplicate construct weight",
                ));
            }
        }
        Ok(())
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
    pub construct: Construct,
    pub outcome: TraceOutcome,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedStatement {
    pub ordinal: u32,
    pub seed_path: String,
    pub derived_seed: u64,
    pub construct: Construct,
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
        let construct =
            self.bootstrap_construct(self.counters.accepted, self.counters.proposals)?;
        self.propose_construct(construct)
    }

    pub fn propose_construct(
        &mut self,
        construct: Construct,
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
        let seed_path = format!("statement/{ordinal}/{construct:?}").to_ascii_lowercase();
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
            construct,
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
            construct,
            outcome: TraceOutcome::Proposed,
            detail: "candidate generated without mutating schema".to_owned(),
        });
        debug!(
            root_seed = self.config.root_seed,
            derived_seed,
            ordinal,
            construct = ?construct,
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
            construct: proposal.statement.construct,
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
            construct: proposal.statement.construct,
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

    fn bootstrap_construct(
        &self,
        accepted_ordinal: u32,
        proposal_ordinal: u32,
    ) -> Result<Construct, GenerationError> {
        let mandatory = [
            Construct::CreateTable,
            Construct::CreateTable,
            Construct::CreateIndex,
            Construct::Insert,
            Construct::Insert,
            Construct::Transaction,
            Construct::Update,
            Construct::Transaction,
        ];
        if let Some(construct) =
            mandatory.get(usize::try_from(accepted_ordinal).unwrap_or(usize::MAX))
        {
            return Ok(*construct);
        }
        let seed_path = format!("statement/{proposal_ordinal}/weighted_construct");
        let seed = derive_seed(self.config.root_seed, &seed_path);
        choose_weighted(&self.config.profile.weights, seed)
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
        match statement {
            Statement::CreateTable { table, columns } => {
                if self.schema.table(table).is_some() {
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
                self.schema.tables.push(TableState {
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
                if self.schema.indexes.iter().any(|entry| entry.name == *index) {
                    return Err(GenerationError::impossible_schema(
                        "create_index.name",
                        "accepted index name already exists",
                    ));
                }
                let table_state = self.schema.table(table).ok_or_else(|| {
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
                self.schema.indexes.push(IndexState {
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
                let table_state = self
                    .schema
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
                if self.schema.table(table).is_none() {
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
                let table_state = self.schema.table(table).ok_or_else(|| {
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
                    if self.schema.transaction_open {
                        return Err(GenerationError::impossible_schema(
                            "transaction.begin",
                            "a transaction is already open",
                        ));
                    }
                    self.transaction_snapshot = Some(self.schema.clone());
                    self.schema.transaction_open = true;
                }
                TransactionStatement::Commit => {
                    if !self.schema.transaction_open {
                        return Err(GenerationError::impossible_schema(
                            "transaction.commit",
                            "no transaction is open",
                        ));
                    }
                    self.transaction_snapshot = None;
                    self.schema.transaction_open = false;
                }
                TransactionStatement::Rollback => {
                    let mut snapshot = self.transaction_snapshot.take().ok_or_else(|| {
                        GenerationError::impossible_schema(
                            "transaction.rollback",
                            "no transaction is open",
                        )
                    })?;
                    snapshot.transaction_open = false;
                    self.schema = snapshot;
                }
            },
        }
        Ok(())
    }
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
