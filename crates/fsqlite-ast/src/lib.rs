//! SQL Abstract Syntax Tree node types for FrankenSQLite.
//!
//! This module defines the complete AST type hierarchy for the SQLite SQL
//! dialect. Every SQL statement parsed by `fsqlite-parser` produces a tree of
//! these nodes. All expression nodes carry a [`Span`] for error reporting.
//!
//! Reference: §10.3–10.4 of the FrankenSQLite specification.

use std::fmt;

// ---------------------------------------------------------------------------
// Span — source location tracking
// ---------------------------------------------------------------------------

/// A byte-offset range into the original SQL source text.
///
/// Every AST node that represents user-written syntax carries a `Span` so that
/// error messages, EXPLAIN output, and debugging tools can point back to the
/// exact source location.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Span {
    /// Byte offset of the first character (inclusive).
    pub start: u32,
    /// Byte offset one past the last character (exclusive).
    pub end: u32,
}

impl Span {
    /// Create a new span from start (inclusive) to end (exclusive) byte offsets.
    #[must_use]
    pub const fn new(start: u32, end: u32) -> Self {
        Self { start, end }
    }

    /// A zero-length span at position 0, used as a placeholder.
    pub const ZERO: Self = Self { start: 0, end: 0 };

    /// Merge two spans into one that covers both.
    #[must_use]
    pub const fn merge(self, other: Self) -> Self {
        let start = if self.start < other.start {
            self.start
        } else {
            other.start
        };
        let end = if self.end > other.end {
            self.end
        } else {
            other.end
        };
        Self { start, end }
    }

    /// Length in bytes.
    #[must_use]
    pub const fn len(self) -> u32 {
        self.end - self.start
    }

    /// Whether the span is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.start == self.end
    }
}

impl fmt::Debug for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

// ---------------------------------------------------------------------------
// Top-level statement
// ---------------------------------------------------------------------------

/// A single parsed SQL statement.
///
/// This is the top-level AST node. The parser produces one `Statement` per
/// semicolon-delimited SQL command.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    // DML
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),

    // DDL
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    CreateView(CreateViewStatement),
    CreateTrigger(CreateTriggerStatement),
    CreateVirtualTable(CreateVirtualTableStatement),
    Drop(DropStatement),
    AlterTable(AlterTableStatement),

    // Transaction control
    Begin(BeginStatement),
    Commit,
    Rollback(RollbackStatement),
    Savepoint(String),
    Release(String),

    // Database operations
    Attach(AttachStatement),
    Detach(String),
    Pragma(PragmaStatement),
    Vacuum(VacuumStatement),

    // Meta / utility
    Reindex(Option<QualifiedName>),
    Analyze(Option<QualifiedName>),
    Explain { query_plan: bool, stmt: Box<Self> },
}

// ---------------------------------------------------------------------------
// Qualified names
// ---------------------------------------------------------------------------

/// A possibly-schema-qualified name like `main.users` or just `users`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedName {
    /// Optional schema name (e.g. `main`, `temp`).
    pub schema: Option<String>,
    /// The object name.
    pub name: String,
}

impl QualifiedName {
    /// Create an unqualified name.
    #[must_use]
    pub fn bare(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            name: name.into(),
        }
    }

    /// Create a schema-qualified name.
    #[must_use]
    pub fn qualified(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            name: name.into(),
        }
    }
}

impl fmt::Display for QualifiedName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref s) = self.schema {
            write!(f, "{s}.{}", self.name)
        } else {
            f.write_str(&self.name)
        }
    }
}

// ---------------------------------------------------------------------------
// Type name
// ---------------------------------------------------------------------------

/// A column type name as written in DDL (e.g. `VARCHAR(255)`, `INTEGER`).
///
/// SQLite does not enforce column types strictly; they only determine affinity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeName {
    /// The type name tokens joined (e.g. `"VARCHAR"`).
    pub name: String,
    /// Optional first size parameter (e.g. `255` in `VARCHAR(255)`).
    pub arg1: Option<String>,
    /// Optional second size parameter (e.g. `10` in `DECIMAL(10,2)`).
    pub arg2: Option<String>,
}

// ---------------------------------------------------------------------------
// Literals
// ---------------------------------------------------------------------------

/// A literal value in SQL source.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// Numeric integer literal.
    Integer(i64),
    /// Numeric float literal.
    Float(f64),
    /// String literal (single-quoted).
    String(String),
    /// Blob literal (`X'...'`).
    Blob(Vec<u8>),
    /// The keyword `NULL`.
    Null,
    /// The keyword `TRUE` (integer 1).
    True,
    /// The keyword `FALSE` (integer 0).
    False,
    /// The keyword `CURRENT_TIME`.
    CurrentTime,
    /// The keyword `CURRENT_DATE`.
    CurrentDate,
    /// The keyword `CURRENT_TIMESTAMP`.
    CurrentTimestamp,
}

// ---------------------------------------------------------------------------
// Column references
// ---------------------------------------------------------------------------

/// A reference to a column, possibly qualified with a table name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Optional table (or alias) qualifier.
    pub table: Option<String>,
    /// Column name.
    pub column: String,
}

impl ColumnRef {
    /// Create an unqualified column reference.
    #[must_use]
    pub fn bare(column: impl Into<String>) -> Self {
        Self {
            table: None,
            column: column.into(),
        }
    }

    /// Create a table-qualified column reference.
    #[must_use]
    pub fn qualified(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            column: column.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    // String
    Concat,

    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Is,
    IsNot,

    // Logical
    And,
    Or,

    // Bitwise
    BitAnd,
    BitOr,
    ShiftLeft,
    ShiftRight,
}

impl fmt::Display for BinaryOp {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
            Self::Concat => "||",
            Self::Eq => "=",
            Self::Ne => "!=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::Is => "IS",
            Self::IsNot => "IS NOT",
            Self::And => "AND",
            Self::Or => "OR",
            Self::BitAnd => "&",
            Self::BitOr => "|",
            Self::ShiftLeft => "<<",
            Self::ShiftRight => ">>",
        })
    }
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    /// Unary minus (`-expr`).
    Negate,
    /// Unary plus (`+expr`).
    Plus,
    /// Bitwise NOT (`~expr`).
    BitNot,
    /// Logical NOT (`NOT expr`).
    Not,
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Negate => "-",
            Self::Plus => "+",
            Self::BitNot => "~",
            Self::Not => "NOT",
        })
    }
}

/// LIKE operator variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LikeOp {
    Like,
    Glob,
    Match,
    Regexp,
}

/// JSON access arrow types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JsonArrow {
    /// `->` extracts as JSON.
    Arrow,
    /// `->>` extracts as text.
    DoubleArrow,
}

// ---------------------------------------------------------------------------
// Expressions (§10.3 Expr enum)
// ---------------------------------------------------------------------------

/// An expression node in the AST.
///
/// Every variant carries a [`Span`] for source-location tracking. The Expr
/// enum covers all expression forms in the SQLite SQL dialect.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A literal constant.
    Literal(Literal, Span),

    /// A column reference (possibly table-qualified).
    Column(ColumnRef, Span),

    /// A binary operation: `left op right`.
    BinaryOp {
        left: Box<Self>,
        op: BinaryOp,
        right: Box<Self>,
        span: Span,
    },

    /// A unary operation: `op expr`.
    UnaryOp {
        op: UnaryOp,
        expr: Box<Self>,
        span: Span,
    },

    /// `expr [NOT] BETWEEN low AND high`.
    Between {
        expr: Box<Self>,
        low: Box<Self>,
        high: Box<Self>,
        not: bool,
        span: Span,
    },

    /// `expr [NOT] IN (...)`.
    In {
        expr: Box<Self>,
        set: InSet,
        not: bool,
        span: Span,
    },

    /// `expr [NOT] LIKE/GLOB/MATCH/REGEXP pattern [ESCAPE escape]`.
    Like {
        expr: Box<Self>,
        pattern: Box<Self>,
        escape: Option<Box<Self>>,
        op: LikeOp,
        not: bool,
        span: Span,
    },

    /// `CASE [operand] WHEN ... THEN ... [ELSE ...] END`.
    Case {
        operand: Option<Box<Self>>,
        whens: Vec<(Self, Self)>,
        else_expr: Option<Box<Self>>,
        span: Span,
    },

    /// `CAST(expr AS type_name)`.
    Cast {
        expr: Box<Self>,
        type_name: TypeName,
        span: Span,
    },

    /// `[NOT] EXISTS (subquery)`.
    Exists {
        subquery: Box<SelectStatement>,
        not: bool,
        span: Span,
    },

    /// A scalar subquery: `(SELECT ...)`.
    Subquery(Box<SelectStatement>, Span),

    /// A function call, optionally with DISTINCT, FILTER, and window spec.
    FunctionCall {
        name: String,
        args: FunctionArgs,
        distinct: bool,
        filter: Option<Box<Self>>,
        over: Option<WindowSpec>,
        span: Span,
    },

    /// `expr COLLATE collation_name`.
    Collate {
        expr: Box<Self>,
        collation: String,
        span: Span,
    },

    /// `expr IS [NOT] NULL` / `expr ISNULL` / `expr NOTNULL`.
    IsNull {
        expr: Box<Self>,
        not: bool,
        span: Span,
    },

    /// `RAISE(action, message)` — used inside trigger bodies.
    Raise {
        action: RaiseAction,
        message: Option<String>,
        span: Span,
    },

    /// `expr -> path` or `expr ->> path` (JSON access).
    JsonAccess {
        expr: Box<Self>,
        path: Box<Self>,
        arrow: JsonArrow,
        span: Span,
    },

    /// A row value `(a, b, c)` for multi-column comparisons (SQLite 3.15+).
    RowValue(Vec<Self>, Span),

    /// A bind parameter (`?`, `?NNN`, `:name`, `@name`, `$name`).
    Placeholder(PlaceholderType, Span),
}

impl Expr {
    /// Return the span of this expression node.
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Literal(_, s)
            | Self::Column(_, s)
            | Self::Subquery(_, s)
            | Self::RowValue(_, s)
            | Self::Placeholder(_, s) => *s,
            Self::BinaryOp { span, .. }
            | Self::UnaryOp { span, .. }
            | Self::Between { span, .. }
            | Self::In { span, .. }
            | Self::Like { span, .. }
            | Self::Case { span, .. }
            | Self::Cast { span, .. }
            | Self::Exists { span, .. }
            | Self::FunctionCall { span, .. }
            | Self::Collate { span, .. }
            | Self::IsNull { span, .. }
            | Self::Raise { span, .. }
            | Self::JsonAccess { span, .. } => *span,
        }
    }
}

/// The set of values for an IN expression.
#[derive(Debug, Clone, PartialEq)]
pub enum InSet {
    /// `IN (expr, expr, ...)`
    List(Vec<Expr>),
    /// `IN (SELECT ...)`
    Subquery(Box<SelectStatement>),
    /// `IN table_name` — shorthand for `IN (SELECT * FROM table_name)`.
    Table(QualifiedName),
}

/// Function argument list.
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArgs {
    /// `func(*)` — used for `COUNT(*)`.
    Star,
    /// `func(arg1, arg2, ...)` or `func()`.
    List(Vec<Expr>),
}

/// Bind parameter types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PlaceholderType {
    /// `?` — anonymous positional.
    Anonymous,
    /// `?NNN` — numbered positional.
    Numbered(u32),
    /// `:name` — colon-prefixed named parameter.
    ColonNamed(String),
    /// `@name` — at-prefixed named parameter.
    AtNamed(String),
    /// `$name` — dollar-prefixed named parameter.
    DollarNamed(String),
}

/// RAISE action for trigger bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RaiseAction {
    Ignore,
    Rollback,
    Abort,
    Fail,
}

// ---------------------------------------------------------------------------
// Window specifications
// ---------------------------------------------------------------------------

/// Window specification for window functions.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    /// Optional base window name.
    pub base_window: Option<String>,
    /// PARTITION BY expressions.
    pub partition_by: Vec<Expr>,
    /// ORDER BY terms within the window.
    pub order_by: Vec<OrderingTerm>,
    /// Frame specification.
    pub frame: Option<FrameSpec>,
}

/// Window frame specification.
#[derive(Debug, Clone, PartialEq)]
pub struct FrameSpec {
    /// Frame type: ROWS, RANGE, or GROUPS.
    pub frame_type: FrameType,
    /// Frame start bound.
    pub start: FrameBound,
    /// Frame end bound (None means current row for BETWEEN-less syntax).
    pub end: Option<FrameBound>,
    /// EXCLUDE clause.
    pub exclude: Option<FrameExclude>,
}

/// Window frame type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameType {
    Rows,
    Range,
    Groups,
}

/// Window frame boundary.
#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound {
    /// `UNBOUNDED PRECEDING`.
    UnboundedPreceding,
    /// `expr PRECEDING`.
    Preceding(Box<Expr>),
    /// `CURRENT ROW`.
    CurrentRow,
    /// `expr FOLLOWING`.
    Following(Box<Expr>),
    /// `UNBOUNDED FOLLOWING`.
    UnboundedFollowing,
}

/// Window frame EXCLUDE clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameExclude {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

// ---------------------------------------------------------------------------
// SELECT statement
// ---------------------------------------------------------------------------

/// A full SELECT statement, including WITH, ORDER BY, and LIMIT.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// Optional common table expressions.
    pub with: Option<WithClause>,
    /// The SELECT body (core + compound operators).
    pub body: SelectBody,
    /// ORDER BY clause.
    pub order_by: Vec<OrderingTerm>,
    /// LIMIT clause.
    pub limit: Option<LimitClause>,
}

/// WITH clause for common table expressions.
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    /// Whether this is `WITH RECURSIVE`.
    pub recursive: bool,
    /// The CTE definitions.
    pub ctes: Vec<Cte>,
}

/// A single Common Table Expression definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    /// CTE name.
    pub name: String,
    /// Optional column name list.
    pub columns: Vec<String>,
    /// Materialization hint.
    pub materialized: Option<CteMaterialized>,
    /// The CTE body query.
    pub query: SelectStatement,
}

/// CTE materialization hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CteMaterialized {
    Materialized,
    NotMaterialized,
}

/// The body of a SELECT: one or more SELECT cores connected by compound ops.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectBody {
    /// The first SELECT core.
    pub select: SelectCore,
    /// Zero or more compound operations (UNION, INTERSECT, EXCEPT).
    pub compounds: Vec<(CompoundOp, SelectCore)>,
}

/// Compound SELECT operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompoundOp {
    Union,
    UnionAll,
    Intersect,
    Except,
}

/// A single SELECT core or VALUES clause.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectCore {
    /// `SELECT [DISTINCT|ALL] columns FROM ... WHERE ... GROUP BY ... HAVING ... WINDOW ...`
    Select {
        distinct: Distinctness,
        columns: Vec<ResultColumn>,
        from: Option<FromClause>,
        where_clause: Option<Box<Expr>>,
        group_by: Vec<Expr>,
        having: Option<Box<Expr>>,
        windows: Vec<WindowDef>,
    },
    /// `VALUES (row), (row), ...` — first-class in SQLite.
    Values(Vec<Vec<Expr>>),
}

/// DISTINCT / ALL modifier on SELECT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Distinctness {
    #[default]
    All,
    Distinct,
}

/// A single column in the SELECT result list.
#[derive(Debug, Clone, PartialEq)]
pub enum ResultColumn {
    /// `*` — all columns from all tables.
    Star,
    /// `table.*` — all columns from a specific table.
    TableStar(String),
    /// `expr [AS alias]`.
    Expr { expr: Expr, alias: Option<String> },
}

/// The FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    /// The table sources joined together.
    pub source: TableOrSubquery,
    /// JOIN clauses.
    pub joins: Vec<JoinClause>,
}

/// A table source in a FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum TableOrSubquery {
    /// A named table: `[schema.]table [AS alias] [INDEXED BY idx | NOT INDEXED]`.
    Table {
        name: QualifiedName,
        alias: Option<String>,
        index_hint: Option<IndexHint>,
    },
    /// A subquery: `(SELECT ...) [AS alias]`.
    Subquery {
        query: Box<SelectStatement>,
        alias: Option<String>,
    },
    /// A table-valued function call: `func(args) [AS alias]`.
    TableFunction {
        name: String,
        args: Vec<Expr>,
        alias: Option<String>,
    },
    /// Parenthesized join: `(table JOIN table ...)`.
    ParenJoin(Box<FromClause>),
}

/// Index hint on a FROM table reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexHint {
    /// `INDEXED BY index_name`.
    IndexedBy(String),
    /// `NOT INDEXED`.
    NotIndexed,
}

/// A JOIN clause.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    /// Join type (INNER, LEFT, CROSS, NATURAL, etc.).
    pub join_type: JoinType,
    /// The right-hand table source.
    pub table: TableOrSubquery,
    /// Join constraint (ON or USING).
    pub constraint: Option<JoinConstraint>,
}

/// Join type modifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JoinType {
    /// Whether this is a NATURAL join.
    pub natural: bool,
    /// The join kind.
    pub kind: JoinKind,
}

/// The kind of join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinKind {
    /// `,` or `CROSS JOIN`.
    Cross,
    /// `[INNER] JOIN`.
    Inner,
    /// `LEFT [OUTER] JOIN`.
    Left,
    /// `RIGHT [OUTER] JOIN` (SQLite 3.39+).
    Right,
    /// `FULL [OUTER] JOIN` (SQLite 3.39+).
    Full,
}

/// Join constraint: ON expression or USING column list.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<String>),
}

/// Named window definition in the WINDOW clause.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowDef {
    /// Window name.
    pub name: String,
    /// Window specification.
    pub spec: WindowSpec,
}

/// ORDER BY term.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderingTerm {
    /// The expression to order by.
    pub expr: Expr,
    /// Sort direction.
    pub direction: Option<SortDirection>,
    /// NULLS FIRST or NULLS LAST.
    pub nulls: Option<NullsOrder>,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// NULLS ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullsOrder {
    First,
    Last,
}

/// LIMIT clause: `LIMIT expr [OFFSET expr]` or `LIMIT expr, expr`.
#[derive(Debug, Clone, PartialEq)]
pub struct LimitClause {
    pub limit: Expr,
    pub offset: Option<Expr>,
}

// ---------------------------------------------------------------------------
// INSERT statement
// ---------------------------------------------------------------------------

/// An INSERT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    /// Optional WITH clause.
    pub with: Option<WithClause>,
    /// INSERT or REPLACE or INSERT OR conflict_action.
    pub or_conflict: Option<ConflictAction>,
    /// Target table name.
    pub table: QualifiedName,
    /// Optional alias for the target table.
    pub alias: Option<String>,
    /// Optional column name list.
    pub columns: Vec<String>,
    /// The source of values.
    pub source: InsertSource,
    /// ON CONFLICT (upsert) clauses.
    pub upsert: Vec<UpsertClause>,
    /// RETURNING clause.
    pub returning: Vec<ResultColumn>,
}

/// Source of values for INSERT.
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    /// `VALUES (row), (row), ...`
    Values(Vec<Vec<Expr>>),
    /// `SELECT ...`
    Select(Box<SelectStatement>),
    /// `DEFAULT VALUES`
    DefaultValues,
}

/// Conflict resolution action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConflictAction {
    Rollback,
    Abort,
    Fail,
    Ignore,
    Replace,
}

/// An ON CONFLICT (upsert) clause.
#[derive(Debug, Clone, PartialEq)]
pub struct UpsertClause {
    /// Conflict target columns.
    pub target: Option<UpsertTarget>,
    /// The DO action.
    pub action: UpsertAction,
}

/// Upsert conflict target.
#[derive(Debug, Clone, PartialEq)]
pub struct UpsertTarget {
    /// Indexed columns.
    pub columns: Vec<IndexedColumn>,
    /// Optional WHERE clause for partial index matching.
    pub where_clause: Option<Expr>,
}

/// Upsert action: DO NOTHING or DO UPDATE SET ...
#[derive(Debug, Clone, PartialEq)]
pub enum UpsertAction {
    Nothing,
    Update {
        assignments: Vec<Assignment>,
        where_clause: Option<Box<Expr>>,
    },
}

// ---------------------------------------------------------------------------
// UPDATE statement
// ---------------------------------------------------------------------------

/// An UPDATE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    /// Optional WITH clause.
    pub with: Option<WithClause>,
    /// UPDATE OR conflict_action.
    pub or_conflict: Option<ConflictAction>,
    /// Target table.
    pub table: QualifiedTableRef,
    /// SET assignments.
    pub assignments: Vec<Assignment>,
    /// Optional FROM clause (SQLite 3.33+).
    pub from: Option<FromClause>,
    /// WHERE clause.
    pub where_clause: Option<Expr>,
    /// RETURNING clause.
    pub returning: Vec<ResultColumn>,
    /// ORDER BY (only with LIMIT).
    pub order_by: Vec<OrderingTerm>,
    /// LIMIT clause.
    pub limit: Option<LimitClause>,
}

/// A SET assignment: `column = expr` or `(col1, col2) = expr`.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// Target column(s).
    pub target: AssignmentTarget,
    /// Value expression.
    pub value: Expr,
}

/// Left-hand side of an assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssignmentTarget {
    /// Single column name.
    Column(String),
    /// Column name list: `(col1, col2, ...)`.
    ColumnList(Vec<String>),
}

/// A table reference with optional alias and index hint (for UPDATE/DELETE).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedTableRef {
    pub name: QualifiedName,
    pub alias: Option<String>,
    pub index_hint: Option<IndexHint>,
}

// ---------------------------------------------------------------------------
// DELETE statement
// ---------------------------------------------------------------------------

/// A DELETE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    /// Optional WITH clause.
    pub with: Option<WithClause>,
    /// Target table.
    pub table: QualifiedTableRef,
    /// WHERE clause.
    pub where_clause: Option<Expr>,
    /// RETURNING clause.
    pub returning: Vec<ResultColumn>,
    /// ORDER BY (only with LIMIT).
    pub order_by: Vec<OrderingTerm>,
    /// LIMIT clause.
    pub limit: Option<LimitClause>,
}

// ---------------------------------------------------------------------------
// DDL: CREATE TABLE
// ---------------------------------------------------------------------------

/// A CREATE TABLE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// Table name.
    pub name: QualifiedName,
    /// Table definition body.
    pub body: CreateTableBody,
    /// `WITHOUT ROWID` flag.
    pub without_rowid: bool,
    /// `STRICT` flag (SQLite 3.37+).
    pub strict: bool,
}

/// The body of a CREATE TABLE.
#[derive(Debug, Clone, PartialEq)]
pub enum CreateTableBody {
    /// Column and constraint definitions.
    Columns {
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    },
    /// `AS SELECT ...`
    AsSelect(Box<SelectStatement>),
}

/// A column definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Optional type name.
    pub type_name: Option<TypeName>,
    /// Column constraints.
    pub constraints: Vec<ColumnConstraint>,
}

/// A constraint on a single column.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnConstraint {
    /// Optional constraint name.
    pub name: Option<String>,
    /// The constraint kind.
    pub kind: ColumnConstraintKind,
}

/// Column constraint variants.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraintKind {
    PrimaryKey {
        direction: Option<SortDirection>,
        conflict: Option<ConflictAction>,
        autoincrement: bool,
    },
    NotNull {
        conflict: Option<ConflictAction>,
    },
    Unique {
        conflict: Option<ConflictAction>,
    },
    Check(Expr),
    Default(DefaultValue),
    Collate(String),
    ForeignKey(ForeignKeyClause),
    Generated {
        expr: Expr,
        storage: Option<GeneratedStorage>,
    },
}

/// Default value for a column.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultValue {
    Expr(Expr),
    /// Parenthesized expression: `DEFAULT (expr)`.
    ParenExpr(Expr),
}

/// Generated column storage type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GeneratedStorage {
    Stored,
    Virtual,
}

/// A table-level constraint.
#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint {
    /// Optional constraint name.
    pub name: Option<String>,
    /// The constraint kind.
    pub kind: TableConstraintKind,
}

/// Table constraint variants.
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintKind {
    PrimaryKey {
        columns: Vec<IndexedColumn>,
        conflict: Option<ConflictAction>,
    },
    Unique {
        columns: Vec<IndexedColumn>,
        conflict: Option<ConflictAction>,
    },
    Check(Expr),
    ForeignKey {
        columns: Vec<String>,
        clause: ForeignKeyClause,
    },
}

/// An indexed column specification (for PRIMARY KEY, UNIQUE, CREATE INDEX).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedColumn {
    /// The column expression (usually just a column name).
    pub expr: Expr,
    /// Optional collation.
    pub collation: Option<String>,
    /// Optional sort direction.
    pub direction: Option<SortDirection>,
}

/// A REFERENCES clause for foreign keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyClause {
    /// Referenced table.
    pub table: String,
    /// Referenced columns (empty = implicit rowid).
    pub columns: Vec<String>,
    /// ON DELETE / ON UPDATE actions.
    pub actions: Vec<ForeignKeyAction>,
    /// DEFERRABLE clause.
    pub deferrable: Option<Deferrable>,
}

/// Foreign key ON DELETE/UPDATE action.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ForeignKeyAction {
    pub trigger: ForeignKeyTrigger,
    pub action: ForeignKeyActionType,
}

/// When the foreign key action fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForeignKeyTrigger {
    OnDelete,
    OnUpdate,
}

/// Foreign key action type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForeignKeyActionType {
    SetNull,
    SetDefault,
    Cascade,
    Restrict,
    NoAction,
}

/// Deferrable constraint specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Deferrable {
    pub not: bool,
    pub initially: Option<DeferrableInitially>,
}

/// INITIALLY DEFERRED or INITIALLY IMMEDIATE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeferrableInitially {
    Deferred,
    Immediate,
}

// ---------------------------------------------------------------------------
// DDL: CREATE INDEX
// ---------------------------------------------------------------------------

/// A CREATE INDEX statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    /// `CREATE UNIQUE INDEX`.
    pub unique: bool,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// Index name.
    pub name: QualifiedName,
    /// Table the index is on.
    pub table: String,
    /// Indexed columns.
    pub columns: Vec<IndexedColumn>,
    /// Optional partial index WHERE clause.
    pub where_clause: Option<Expr>,
}

// ---------------------------------------------------------------------------
// DDL: CREATE VIEW
// ---------------------------------------------------------------------------

/// A CREATE VIEW statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStatement {
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// `CREATE TEMP VIEW`.
    pub temporary: bool,
    /// View name.
    pub name: QualifiedName,
    /// Optional column name list.
    pub columns: Vec<String>,
    /// The view's SELECT query.
    pub query: SelectStatement,
}

// ---------------------------------------------------------------------------
// DDL: CREATE TRIGGER
// ---------------------------------------------------------------------------

/// A CREATE TRIGGER statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTriggerStatement {
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// `CREATE TEMP TRIGGER`.
    pub temporary: bool,
    /// Trigger name.
    pub name: QualifiedName,
    /// When the trigger fires.
    pub timing: TriggerTiming,
    /// What event triggers it.
    pub event: TriggerEvent,
    /// Table the trigger is on.
    pub table: String,
    /// `FOR EACH ROW` (SQLite only supports row-level triggers).
    pub for_each_row: bool,
    /// Optional WHEN condition.
    pub when: Option<Expr>,
    /// Trigger body statements.
    pub body: Vec<Statement>,
}

/// Trigger timing: BEFORE, AFTER, or INSTEAD OF.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// Trigger event: INSERT, DELETE, or UPDATE [OF columns].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Delete,
    Update(Vec<String>),
}

// ---------------------------------------------------------------------------
// DDL: CREATE VIRTUAL TABLE
// ---------------------------------------------------------------------------

/// A CREATE VIRTUAL TABLE statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateVirtualTableStatement {
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// Table name.
    pub name: QualifiedName,
    /// Module name (e.g. `fts5`, `rtree`).
    pub module: String,
    /// Module arguments (opaque strings).
    pub args: Vec<String>,
}

// ---------------------------------------------------------------------------
// DDL: DROP
// ---------------------------------------------------------------------------

/// A DROP statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropStatement {
    /// What kind of object to drop.
    pub object_type: DropObjectType,
    /// `IF EXISTS` flag.
    pub if_exists: bool,
    /// Object name.
    pub name: QualifiedName,
}

/// DROP target type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DropObjectType {
    Table,
    View,
    Index,
    Trigger,
}

// ---------------------------------------------------------------------------
// DDL: ALTER TABLE
// ---------------------------------------------------------------------------

/// An ALTER TABLE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStatement {
    /// Table name.
    pub table: QualifiedName,
    /// The alteration to perform.
    pub action: AlterTableAction,
}

/// ALTER TABLE action variants.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
    /// `RENAME TO new_name`.
    RenameTo(String),
    /// `RENAME COLUMN old TO new`.
    RenameColumn { old: String, new: String },
    /// `ADD COLUMN column_def`.
    AddColumn(ColumnDef),
    /// `DROP COLUMN column_name`.
    DropColumn(String),
}

// ---------------------------------------------------------------------------
// Transaction control
// ---------------------------------------------------------------------------

/// A BEGIN statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BeginStatement {
    /// Transaction mode.
    pub mode: Option<TransactionMode>,
}

/// Transaction mode for BEGIN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionMode {
    Deferred,
    Immediate,
    Exclusive,
}

/// A ROLLBACK statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackStatement {
    /// Optional savepoint name to roll back to.
    pub to_savepoint: Option<String>,
}

// ---------------------------------------------------------------------------
// ATTACH / DETACH
// ---------------------------------------------------------------------------

/// An ATTACH statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AttachStatement {
    /// The database file expression.
    pub expr: Expr,
    /// The schema name.
    pub schema: String,
}

// ---------------------------------------------------------------------------
// PRAGMA
// ---------------------------------------------------------------------------

/// A PRAGMA statement.
#[derive(Debug, Clone, PartialEq)]
pub struct PragmaStatement {
    /// Pragma name (possibly schema-qualified).
    pub name: QualifiedName,
    /// Pragma value or call argument.
    pub value: Option<PragmaValue>,
}

/// PRAGMA value form.
#[derive(Debug, Clone, PartialEq)]
pub enum PragmaValue {
    /// `PRAGMA name = value`.
    Assign(Expr),
    /// `PRAGMA name(value)`.
    Call(Expr),
}

// ---------------------------------------------------------------------------
// VACUUM
// ---------------------------------------------------------------------------

/// A VACUUM statement.
#[derive(Debug, Clone, PartialEq)]
pub struct VacuumStatement {
    /// Optional schema name.
    pub schema: Option<String>,
    /// Optional INTO filename.
    pub into: Option<Expr>,
}

// ---------------------------------------------------------------------------
// Name resolution types (§10.4)
// ---------------------------------------------------------------------------

/// A resolved column reference after name resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedColumn {
    /// Index of the table in the FROM clause (0-based).
    pub table_idx: usize,
    /// Column index within that table's schema (0-based).
    pub column_idx: usize,
    /// The table name or alias this resolved to.
    pub table_name: String,
    /// The column name.
    pub column_name: String,
}

/// A table schema entry used during name resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    /// The table name as it appears in the schema.
    pub name: String,
    /// The alias bound in the FROM clause (if any).
    pub alias: Option<String>,
    /// Column names in order.
    pub columns: Vec<String>,
}

impl TableSchema {
    /// The effective name for lookup (alias if present, else table name).
    #[must_use]
    pub fn effective_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// Errors during name resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// No such table in the FROM clause.
    NoSuchTable { name: String, span: Span },
    /// No such column in the referenced table.
    NoSuchColumn {
        table: String,
        column: String,
        span: Span,
    },
    /// Ambiguous unqualified column reference matches multiple tables.
    AmbiguousColumn {
        column: String,
        candidates: Vec<String>,
        span: Span,
    },
    /// Unqualified column name not found in any table in scope.
    ColumnNotFound { column: String, span: Span },
    /// Correlated subquery references an outer table that doesn't exist.
    NoOuterTable { name: String, span: Span },
}

impl fmt::Display for ResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoSuchTable { name, span } => {
                write!(f, "no such table: {name} at {span}")
            }
            Self::NoSuchColumn {
                table,
                column,
                span,
            } => {
                write!(f, "no such column: {table}.{column} at {span}")
            }
            Self::AmbiguousColumn {
                column,
                candidates,
                span,
            } => {
                write!(
                    f,
                    "ambiguous column name: {column} (candidates: {}) at {span}",
                    candidates.join(", ")
                )
            }
            Self::ColumnNotFound { column, span } => {
                write!(f, "no such column: {column} at {span}")
            }
            Self::NoOuterTable { name, span } => {
                write!(f, "no such table in outer scope: {name} at {span}")
            }
        }
    }
}

impl std::error::Error for ResolveError {}

/// A scope for name resolution, supporting nested subquery scopes.
#[derive(Debug, Clone)]
pub struct ResolverScope {
    /// Tables available in this scope.
    pub tables: Vec<TableSchema>,
    /// Parent scope (for correlated subquery resolution).
    pub parent: Option<Box<Self>>,
}

impl ResolverScope {
    /// Create a new root scope with the given table schemas.
    #[must_use]
    pub fn new(tables: Vec<TableSchema>) -> Self {
        Self {
            tables,
            parent: None,
        }
    }

    /// Create a child scope for a subquery, with this scope as the parent.
    #[must_use]
    pub fn child(self, tables: Vec<TableSchema>) -> Self {
        Self {
            tables,
            parent: Some(Box::new(self)),
        }
    }

    /// Resolve a possibly-qualified column reference.
    ///
    /// For qualified refs (`t.col`): find table `t` in scope, then verify `col`.
    /// For unqualified refs (`col`): search all tables; error if ambiguous.
    /// If not found in this scope, search parent scopes (correlated subquery).
    pub fn resolve(&self, col: &ColumnRef, span: Span) -> Result<ResolvedColumn, ResolveError> {
        match &col.table {
            Some(table_name) => self.resolve_qualified(table_name, &col.column, span),
            None => self.resolve_unqualified(&col.column, span),
        }
    }

    fn resolve_qualified(
        &self,
        table_name: &str,
        column: &str,
        span: Span,
    ) -> Result<ResolvedColumn, ResolveError> {
        for (idx, table) in self.tables.iter().enumerate() {
            if table.effective_name().eq_ignore_ascii_case(table_name) {
                return match table
                    .columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(column))
                {
                    Some(col_idx) => Ok(ResolvedColumn {
                        table_idx: idx,
                        column_idx: col_idx,
                        table_name: table.effective_name().to_owned(),
                        column_name: table.columns[col_idx].clone(),
                    }),
                    None => Err(ResolveError::NoSuchColumn {
                        table: table_name.to_owned(),
                        column: column.to_owned(),
                        span,
                    }),
                };
            }
        }

        // Try parent scope (correlated subquery).
        if let Some(ref parent) = self.parent {
            return parent.resolve_qualified(table_name, column, span);
        }

        Err(ResolveError::NoSuchTable {
            name: table_name.to_owned(),
            span,
        })
    }

    fn resolve_unqualified(
        &self,
        column: &str,
        span: Span,
    ) -> Result<ResolvedColumn, ResolveError> {
        let mut found: Option<ResolvedColumn> = None;
        let mut candidates = Vec::new();

        for (idx, table) in self.tables.iter().enumerate() {
            if let Some(col_idx) = table
                .columns
                .iter()
                .position(|c| c.eq_ignore_ascii_case(column))
            {
                candidates.push(table.effective_name().to_owned());
                found = Some(ResolvedColumn {
                    table_idx: idx,
                    column_idx: col_idx,
                    table_name: table.effective_name().to_owned(),
                    column_name: table.columns[col_idx].clone(),
                });
            }
        }

        match candidates.len() {
            0 => {
                // Try parent scope (correlated subquery).
                if let Some(ref parent) = self.parent {
                    return parent.resolve_unqualified(column, span);
                }
                Err(ResolveError::ColumnNotFound {
                    column: column.to_owned(),
                    span,
                })
            }
            1 => Ok(found.expect("candidates len is 1")),
            _ => Err(ResolveError::AmbiguousColumn {
                column: column.to_owned(),
                candidates,
                span,
            }),
        }
    }

    /// Expand `SELECT *` to explicit column references.
    ///
    /// Returns a list of `(table_name, column_name)` pairs in order.
    #[must_use]
    pub fn expand_star(&self) -> Vec<(String, String)> {
        let mut result = Vec::new();
        for table in &self.tables {
            for col in &table.columns {
                result.push((table.effective_name().to_owned(), col.clone()));
            }
        }
        result
    }

    /// Expand `table.*` to explicit column references.
    pub fn expand_table_star(
        &self,
        table_name: &str,
        span: Span,
    ) -> Result<Vec<(String, String)>, ResolveError> {
        for table in &self.tables {
            if table.effective_name().eq_ignore_ascii_case(table_name) {
                return Ok(table
                    .columns
                    .iter()
                    .map(|c| (table.effective_name().to_owned(), c.clone()))
                    .collect());
            }
        }
        Err(ResolveError::NoSuchTable {
            name: table_name.to_owned(),
            span,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- AST construction tests (§10.3) ---

    #[test]
    fn test_ast_statement_variants_dml() {
        let _ = Statement::Select(SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Values(vec![vec![Expr::Literal(
                    Literal::Integer(1),
                    Span::ZERO,
                )]]),
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        });

        let _ = Statement::Insert(InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::DefaultValues,
            upsert: vec![],
            returning: vec![],
        });

        let table_ref = QualifiedTableRef {
            name: QualifiedName::bare("t"),
            alias: None,
            index_hint: None,
        };
        let _ = Statement::Update(UpdateStatement {
            with: None,
            or_conflict: None,
            table: table_ref.clone(),
            assignments: vec![],
            from: None,
            where_clause: None,
            returning: vec![],
            order_by: vec![],
            limit: None,
        });
        let _ = Statement::Delete(DeleteStatement {
            with: None,
            table: table_ref,
            where_clause: None,
            returning: vec![],
            order_by: vec![],
            limit: None,
        });
    }

    #[test]
    fn test_ast_statement_variants_ddl() {
        let _ = Statement::CreateTable(CreateTableStatement {
            if_not_exists: false,
            name: QualifiedName::bare("t"),
            body: CreateTableBody::Columns {
                columns: vec![],
                constraints: vec![],
            },
            without_rowid: false,
            strict: false,
        });

        let _ = Statement::CreateIndex(CreateIndexStatement {
            unique: false,
            if_not_exists: false,
            name: QualifiedName::bare("idx"),
            table: "t".to_owned(),
            columns: vec![],
            where_clause: None,
        });

        let _ = Statement::CreateView(CreateViewStatement {
            if_not_exists: false,
            temporary: false,
            name: QualifiedName::bare("v"),
            columns: vec![],
            query: SelectStatement {
                with: None,
                body: SelectBody {
                    select: SelectCore::Values(vec![]),
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            },
        });

        let _ = Statement::CreateTrigger(CreateTriggerStatement {
            if_not_exists: false,
            temporary: false,
            name: QualifiedName::bare("tr"),
            timing: TriggerTiming::Before,
            event: TriggerEvent::Insert,
            table: "t".to_owned(),
            for_each_row: true,
            when: None,
            body: vec![],
        });

        let _ = Statement::CreateVirtualTable(CreateVirtualTableStatement {
            if_not_exists: false,
            name: QualifiedName::bare("vt"),
            module: "fts5".to_owned(),
            args: vec!["content".to_owned()],
        });

        let _ = Statement::Drop(DropStatement {
            object_type: DropObjectType::Table,
            if_exists: false,
            name: QualifiedName::bare("t"),
        });

        let _ = Statement::AlterTable(AlterTableStatement {
            table: QualifiedName::bare("t"),
            action: AlterTableAction::RenameTo("t2".to_owned()),
        });
    }

    #[test]
    fn test_ast_statement_variants_txn_and_misc() {
        let _ = Statement::Begin(BeginStatement { mode: None });
        let _ = Statement::Commit;
        let _ = Statement::Rollback(RollbackStatement { to_savepoint: None });

        let _ = Statement::Savepoint("sp1".to_owned());
        let _ = Statement::Release("sp1".to_owned());

        let _ = Statement::Attach(AttachStatement {
            expr: Expr::Literal(Literal::String("file.db".to_owned()), Span::ZERO),
            schema: "aux".to_owned(),
        });
        let _ = Statement::Detach("aux".to_owned());

        let _ = Statement::Pragma(PragmaStatement {
            name: QualifiedName::bare("cache_size"),
            value: None,
        });
        let _ = Statement::Vacuum(VacuumStatement {
            schema: None,
            into: None,
        });

        let _ = Statement::Reindex(None);
        let _ = Statement::Analyze(None);

        let _ = Statement::Explain {
            query_plan: true,
            stmt: Box::new(Statement::Commit),
        };
    }

    #[test]
    fn test_ast_select_body_with_compounds() {
        let core1 = SelectCore::Values(vec![vec![Expr::Literal(Literal::Integer(1), Span::ZERO)]]);
        let core2 = SelectCore::Values(vec![vec![Expr::Literal(Literal::Integer(2), Span::ZERO)]]);
        let core3 = SelectCore::Values(vec![vec![Expr::Literal(Literal::Integer(3), Span::ZERO)]]);

        let body = SelectBody {
            select: core1,
            compounds: vec![(CompoundOp::Union, core2), (CompoundOp::Intersect, core3)],
        };

        assert_eq!(body.compounds.len(), 2);
        assert_eq!(body.compounds[0].0, CompoundOp::Union);
        assert_eq!(body.compounds[1].0, CompoundOp::Intersect);
    }

    #[test]
    fn test_ast_values_as_first_class() {
        let values = SelectCore::Values(vec![
            vec![
                Expr::Literal(Literal::Integer(1), Span::ZERO),
                Expr::Literal(Literal::Integer(2), Span::ZERO),
            ],
            vec![
                Expr::Literal(Literal::Integer(3), Span::ZERO),
                Expr::Literal(Literal::Integer(4), Span::ZERO),
            ],
        ]);

        assert!(matches!(values, SelectCore::Values(ref rows) if rows.len() == 2));

        // Values is distinct from Select.
        let select = SelectCore::Select {
            distinct: Distinctness::All,
            columns: vec![],
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            windows: vec![],
        };
        assert!(!matches!(select, SelectCore::Values(_)));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_ast_expr_variants_core() {
        let span = Span::new(0, 10);
        let dummy = || Box::new(Expr::Literal(Literal::Null, span));

        let exprs: Vec<Expr> = vec![
            Expr::Literal(Literal::Integer(42), span),
            Expr::Column(ColumnRef::bare("x"), span),
            Expr::BinaryOp {
                left: dummy(),
                op: BinaryOp::Add,
                right: dummy(),
                span,
            },
            Expr::UnaryOp {
                op: UnaryOp::Negate,
                expr: dummy(),
                span,
            },
            Expr::Between {
                expr: dummy(),
                low: dummy(),
                high: dummy(),
                not: false,
                span,
            },
            Expr::In {
                expr: dummy(),
                set: InSet::List(vec![]),
                not: false,
                span,
            },
            Expr::Like {
                expr: dummy(),
                pattern: dummy(),
                escape: None,
                op: LikeOp::Like,
                not: false,
                span,
            },
            Expr::Case {
                operand: None,
                whens: vec![],
                else_expr: None,
                span,
            },
            Expr::Cast {
                expr: dummy(),
                type_name: TypeName {
                    name: "INTEGER".to_owned(),
                    arg1: None,
                    arg2: None,
                },
                span,
            },
            Expr::Collate {
                expr: dummy(),
                collation: "NOCASE".to_owned(),
                span,
            },
            Expr::IsNull {
                expr: dummy(),
                not: false,
                span,
            },
            Expr::JsonAccess {
                expr: dummy(),
                path: dummy(),
                arrow: JsonArrow::Arrow,
                span,
            },
            Expr::RowValue(vec![], span),
            Expr::Placeholder(PlaceholderType::Anonymous, span),
        ];

        for expr in &exprs {
            assert_eq!(expr.span(), span);
        }
    }

    #[test]
    fn test_ast_expr_variants_subqueries_and_calls() {
        let span = Span::new(0, 10);
        let dummy = || Box::new(Expr::Literal(Literal::Null, span));

        let empty_select = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Values(vec![]),
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };

        let exprs: Vec<Expr> = vec![
            Expr::Exists {
                subquery: Box::new(empty_select.clone()),
                not: false,
                span,
            },
            Expr::Subquery(Box::new(empty_select), span),
            Expr::FunctionCall {
                name: "count".to_owned(),
                args: FunctionArgs::Star,
                distinct: false,
                filter: None,
                over: None,
                span,
            },
            Expr::Raise {
                action: RaiseAction::Abort,
                message: Some("error".to_owned()),
                span,
            },
            // Ensure we still cover at least one boxed expression path for spans.
            Expr::UnaryOp {
                op: UnaryOp::Negate,
                expr: dummy(),
                span,
            },
        ];

        for expr in &exprs {
            assert_eq!(expr.span(), span);
        }
    }

    #[test]
    fn test_ast_function_call_with_window() {
        let span = Span::new(0, 30);
        let expr = Expr::FunctionCall {
            name: "row_number".to_owned(),
            args: FunctionArgs::List(vec![]),
            distinct: false,
            filter: None,
            over: Some(WindowSpec {
                base_window: None,
                partition_by: vec![Expr::Column(ColumnRef::bare("dept"), span)],
                order_by: vec![OrderingTerm {
                    expr: Expr::Column(ColumnRef::bare("salary"), span),
                    direction: Some(SortDirection::Desc),
                    nulls: None,
                }],
                frame: Some(FrameSpec {
                    frame_type: FrameType::Rows,
                    start: FrameBound::UnboundedPreceding,
                    end: Some(FrameBound::CurrentRow),
                    exclude: None,
                }),
            }),
            span,
        };

        assert!(matches!(expr, Expr::FunctionCall { over: Some(_), .. }));
        if let Expr::FunctionCall {
            over: Some(ref win),
            ..
        } = expr
        {
            assert_eq!(win.partition_by.len(), 1);
            assert_eq!(win.order_by.len(), 1);
            assert!(win.frame.is_some());
        }
    }

    #[test]
    fn test_ast_like_with_escape() {
        let span = Span::ZERO;
        let expr = Expr::Like {
            expr: Box::new(Expr::Column(ColumnRef::bare("name"), span)),
            pattern: Box::new(Expr::Literal(Literal::String("foo%".to_owned()), span)),
            escape: Some(Box::new(Expr::Literal(
                Literal::String("\\".to_owned()),
                span,
            ))),
            op: LikeOp::Like,
            not: false,
            span,
        };

        assert!(matches!(
            expr,
            Expr::Like {
                escape: Some(_),
                ..
            }
        ));
        if let Expr::Like {
            escape: Some(ref esc),
            ..
        } = expr
        {
            assert!(matches!(esc.as_ref(), Expr::Literal(Literal::String(_), _)));
        }
    }

    #[test]
    fn test_ast_json_access_arrow_types() {
        let span = Span::ZERO;
        let arrow = Expr::JsonAccess {
            expr: Box::new(Expr::Column(ColumnRef::bare("data"), span)),
            path: Box::new(Expr::Literal(Literal::String("$.name".to_owned()), span)),
            arrow: JsonArrow::Arrow,
            span,
        };
        let double_arrow = Expr::JsonAccess {
            expr: Box::new(Expr::Column(ColumnRef::bare("data"), span)),
            path: Box::new(Expr::Literal(Literal::String("$.name".to_owned()), span)),
            arrow: JsonArrow::DoubleArrow,
            span,
        };

        assert!(matches!(
            (&arrow, &double_arrow),
            (
                Expr::JsonAccess {
                    arrow: JsonArrow::Arrow,
                    ..
                },
                Expr::JsonAccess {
                    arrow: JsonArrow::DoubleArrow,
                    ..
                }
            )
        ));
    }

    #[test]
    fn test_ast_row_value() {
        let span = Span::ZERO;
        let rv = Expr::RowValue(
            vec![
                Expr::Column(ColumnRef::bare("a"), span),
                Expr::Column(ColumnRef::bare("b"), span),
                Expr::Column(ColumnRef::bare("c"), span),
            ],
            span,
        );

        assert!(matches!(rv, Expr::RowValue(_, _)));
        if let Expr::RowValue(ref elems, _) = rv {
            assert_eq!(elems.len(), 3);
        }
    }

    // --- Name resolution tests (§10.4) ---

    fn make_scope_t1_t2() -> ResolverScope {
        ResolverScope::new(vec![
            TableSchema {
                name: "t1".to_owned(),
                alias: None,
                columns: vec!["a".to_owned(), "b".to_owned()],
            },
            TableSchema {
                name: "t2".to_owned(),
                alias: None,
                columns: vec!["c".to_owned(), "d".to_owned()],
            },
        ])
    }

    #[test]
    fn test_resolve_unambiguous_column() {
        let scope = make_scope_t1_t2();
        let result = scope
            .resolve(&ColumnRef::bare("a"), Span::ZERO)
            .expect("should resolve");
        assert_eq!(result.table_name, "t1");
        assert_eq!(result.column_name, "a");
        assert_eq!(result.table_idx, 0);
        assert_eq!(result.column_idx, 0);
    }

    #[test]
    fn test_resolve_ambiguous_column_error() {
        let scope = ResolverScope::new(vec![
            TableSchema {
                name: "t1".to_owned(),
                alias: None,
                columns: vec!["x".to_owned(), "y".to_owned()],
            },
            TableSchema {
                name: "t2".to_owned(),
                alias: None,
                columns: vec!["x".to_owned(), "z".to_owned()],
            },
        ]);

        let err = scope
            .resolve(&ColumnRef::bare("x"), Span::ZERO)
            .unwrap_err();
        assert!(matches!(err, ResolveError::AmbiguousColumn { .. }));
        if let ResolveError::AmbiguousColumn {
            column, candidates, ..
        } = err
        {
            assert_eq!(column, "x");
            assert_eq!(candidates, vec!["t1", "t2"]);
        }
    }

    #[test]
    fn test_resolve_qualified_column() {
        let scope = make_scope_t1_t2();

        let result = scope
            .resolve(&ColumnRef::qualified("t1", "a"), Span::ZERO)
            .expect("should resolve");
        assert_eq!(result.table_name, "t1");
        assert_eq!(result.column_name, "a");

        let err = scope
            .resolve(&ColumnRef::qualified("t1", "nonexistent"), Span::ZERO)
            .unwrap_err();
        assert!(matches!(err, ResolveError::NoSuchColumn { .. }));
    }

    #[test]
    fn test_resolve_alias_binding() {
        let scope = ResolverScope::new(vec![TableSchema {
            name: "users".to_owned(),
            alias: Some("u".to_owned()),
            columns: vec!["id".to_owned(), "name".to_owned()],
        }]);

        let result = scope
            .resolve(&ColumnRef::qualified("u", "name"), Span::ZERO)
            .expect("should resolve via alias");
        assert_eq!(result.table_name, "u");
        assert_eq!(result.column_name, "name");
    }

    #[test]
    fn test_resolve_star_expansion() {
        let scope = make_scope_t1_t2();
        let expanded = scope.expand_star();
        assert_eq!(
            expanded,
            vec![
                ("t1".to_owned(), "a".to_owned()),
                ("t1".to_owned(), "b".to_owned()),
                ("t2".to_owned(), "c".to_owned()),
                ("t2".to_owned(), "d".to_owned()),
            ]
        );
    }

    #[test]
    fn test_resolve_qualified_star() {
        let scope = make_scope_t1_t2();
        let expanded = scope.expand_table_star("t1", Span::ZERO).unwrap();
        assert_eq!(
            expanded,
            vec![
                ("t1".to_owned(), "a".to_owned()),
                ("t1".to_owned(), "b".to_owned()),
            ]
        );
    }

    #[test]
    fn test_resolve_subquery_scope() {
        // Outer scope has t1(a, b), inner scope has t2(c, d).
        // Inner should be able to resolve t1.a from outer.
        let outer = ResolverScope::new(vec![TableSchema {
            name: "t1".to_owned(),
            alias: None,
            columns: vec!["a".to_owned(), "b".to_owned()],
        }]);

        let inner = outer.child(vec![TableSchema {
            name: "t2".to_owned(),
            alias: None,
            columns: vec!["c".to_owned(), "d".to_owned()],
        }]);

        // Inner can resolve t2.c directly.
        let result = inner
            .resolve(&ColumnRef::qualified("t2", "c"), Span::ZERO)
            .expect("inner table");
        assert_eq!(result.table_name, "t2");

        // Inner can resolve t1.a from outer scope (correlated).
        let result = inner
            .resolve(&ColumnRef::qualified("t1", "a"), Span::ZERO)
            .expect("correlated outer reference");
        assert_eq!(result.table_name, "t1");
        assert_eq!(result.column_name, "a");
    }

    #[test]
    fn test_resolve_scope_shadowing() {
        // Inner scope has t1 that shadows outer t1.
        let outer = ResolverScope::new(vec![TableSchema {
            name: "t1".to_owned(),
            alias: None,
            columns: vec!["outer_col".to_owned()],
        }]);

        let inner = outer.child(vec![TableSchema {
            name: "t1".to_owned(),
            alias: None,
            columns: vec!["inner_col".to_owned()],
        }]);

        // Resolving t1.inner_col should find the inner scope's t1.
        let result = inner
            .resolve(&ColumnRef::qualified("t1", "inner_col"), Span::ZERO)
            .expect("inner shadows outer");
        assert_eq!(result.column_name, "inner_col");

        // Resolving t1.outer_col should fail because inner t1 shadows.
        let err = inner
            .resolve(&ColumnRef::qualified("t1", "outer_col"), Span::ZERO)
            .unwrap_err();
        assert!(matches!(err, ResolveError::NoSuchColumn { .. }));
    }

    #[test]
    fn test_resolve_nonexistent_table_error() {
        let scope = make_scope_t1_t2();
        let err = scope
            .resolve(&ColumnRef::qualified("nonexistent", "a"), Span::ZERO)
            .unwrap_err();
        assert!(matches!(err, ResolveError::NoSuchTable { .. }));
    }

    #[test]
    fn test_resolve_unqualified_column_not_found() {
        let scope = make_scope_t1_t2();
        let err = scope
            .resolve(&ColumnRef::bare("nonexistent"), Span::ZERO)
            .unwrap_err();
        assert!(matches!(err, ResolveError::ColumnNotFound { .. }));
        if let ResolveError::ColumnNotFound { column, .. } = err {
            assert_eq!(column, "nonexistent");
        }
    }

    #[test]
    fn test_resolve_column_in_order_by() {
        // Test that an alias resolves when used as a virtual column.
        let scope = ResolverScope::new(vec![TableSchema {
            name: "result".to_owned(),
            alias: None,
            columns: vec!["total".to_owned()],
        }]);

        let result = scope
            .resolve(&ColumnRef::bare("total"), Span::ZERO)
            .expect("order by alias");
        assert_eq!(result.column_name, "total");
    }

    // --- Span tests ---

    #[test]
    fn test_span_merge() {
        let a = Span::new(5, 10);
        let b = Span::new(15, 20);
        let merged = a.merge(b);
        assert_eq!(merged.start, 5);
        assert_eq!(merged.end, 20);
    }

    #[test]
    fn test_span_len_is_empty() {
        let s = Span::new(10, 20);
        assert_eq!(s.len(), 10);
        assert!(!s.is_empty());

        assert!(Span::ZERO.is_empty());
    }

    // --- QualifiedName tests ---

    #[test]
    fn test_qualified_name_display() {
        let bare = QualifiedName::bare("users");
        assert_eq!(bare.to_string(), "users");

        let qual = QualifiedName::qualified("main", "users");
        assert_eq!(qual.to_string(), "main.users");
    }

    // --- Operator display tests ---

    #[test]
    fn test_binary_op_display() {
        assert_eq!(BinaryOp::Add.to_string(), "+");
        assert_eq!(BinaryOp::Concat.to_string(), "||");
        assert_eq!(BinaryOp::And.to_string(), "AND");
        assert_eq!(BinaryOp::IsNot.to_string(), "IS NOT");
    }

    #[test]
    fn test_unary_op_display() {
        assert_eq!(UnaryOp::Negate.to_string(), "-");
        assert_eq!(UnaryOp::Not.to_string(), "NOT");
    }
}
