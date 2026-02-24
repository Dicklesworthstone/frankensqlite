//! Semantic analysis: name resolution, type checking, and scope validation.
//!
//! Validates AST nodes against a schema to ensure:
//! - Column references resolve to known tables/columns
//! - Table aliases are unique within a query scope
//! - Function arity matches known functions
//! - CTE names are visible in the correct scope
//! - Type affinity is tracked for expression results
//!
//! # Usage
//!
//! ```ignore
//! let schema = Schema::new();
//! schema.add_table(TableDef { name: "users", columns: vec![...] });
//! let mut resolver = Resolver::new(&schema);
//! let errors = resolver.resolve_statement(&stmt);
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use fsqlite_ast::{
    ColumnRef, Expr, FromClause, FunctionArgs, InSet, JoinClause, JoinConstraint, ResultColumn,
    SelectCore, SelectStatement, Statement, TableOrSubquery,
};
use fsqlite_types::TypeAffinity;

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Monotonic counter of semantic errors encountered.
static FSQLITE_SEMANTIC_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Point-in-time snapshot of semantic analysis metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SemanticMetricsSnapshot {
    pub fsqlite_semantic_errors_total: u64,
}

/// Take a point-in-time snapshot of semantic metrics.
#[must_use]
pub fn semantic_metrics_snapshot() -> SemanticMetricsSnapshot {
    SemanticMetricsSnapshot {
        fsqlite_semantic_errors_total: FSQLITE_SEMANTIC_ERRORS_TOTAL.load(Ordering::Relaxed),
    }
}

/// Reset semantic metrics.
pub fn reset_semantic_metrics() {
    FSQLITE_SEMANTIC_ERRORS_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Schema types
// ---------------------------------------------------------------------------

/// A column definition in the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// Column name (stored in original case).
    pub name: String,
    /// Type affinity determined from the DDL type name.
    pub affinity: TypeAffinity,
    /// Whether this column is an INTEGER PRIMARY KEY (rowid alias).
    pub is_ipk: bool,
    /// Whether this column has a NOT NULL constraint.
    pub not_null: bool,
}

/// A table definition in the schema.
#[derive(Debug, Clone)]
pub struct TableDef {
    /// Table name.
    pub name: String,
    /// Column definitions in declaration order.
    pub columns: Vec<ColumnDef>,
    /// Whether this is a WITHOUT ROWID table.
    pub without_rowid: bool,
    /// Whether this is a STRICT table.
    pub strict: bool,
}

impl TableDef {
    /// Find a column by name (case-insensitive).
    #[must_use]
    pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Check if this table has a column with the given name (case-insensitive).
    #[must_use]
    pub fn has_column(&self, name: &str) -> bool {
        self.find_column(name).is_some()
    }

    /// Check if a name is a rowid alias for this table.
    #[must_use]
    pub fn is_rowid_alias(&self, name: &str) -> bool {
        if self.without_rowid {
            return false;
        }
        let lower = name.to_ascii_lowercase();
        matches!(lower.as_str(), "rowid" | "_rowid_" | "oid")
            || self
                .columns
                .iter()
                .any(|c| c.is_ipk && c.name.eq_ignore_ascii_case(name))
    }
}

/// The database schema: a collection of table definitions.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    /// Tables by lowercase name.
    tables: HashMap<String, TableDef>,
}

impl Schema {
    /// Create an empty schema.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a table definition.
    pub fn add_table(&mut self, table: TableDef) {
        self.tables.insert(table.name.to_ascii_lowercase(), table);
    }

    /// Look up a table by name (case-insensitive).
    #[must_use]
    pub fn find_table(&self, name: &str) -> Option<&TableDef> {
        self.tables.get(&name.to_ascii_lowercase())
    }

    /// Number of tables in the schema.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

// ---------------------------------------------------------------------------
// Scope tracking
// ---------------------------------------------------------------------------

/// A name scope for query resolution. Scopes nest for subqueries and CTEs.
#[derive(Debug, Clone)]
pub struct Scope {
    /// Table aliases visible in this scope: alias → table name.
    aliases: HashMap<String, String>,
    /// Columns visible from each alias: alias → set of column names.
    /// None means the columns are unknown (CTE or subquery), so any column reference is optimistically accepted.
    columns: HashMap<String, Option<HashSet<String>>>,
    /// CTE names visible in this scope.
    ctes: HashSet<String>,
    /// Parent scope (for subquery nesting).
    parent: Option<Box<Self>>,
}

impl Scope {
    /// Create a root scope.
    #[must_use]
    pub fn root() -> Self {
        Self {
            aliases: HashMap::new(),
            columns: HashMap::new(),
            ctes: HashSet::new(),
            parent: None,
        }
    }

    /// Create a child scope (for subqueries).
    #[must_use]
    pub fn child(parent: Self) -> Self {
        Self {
            aliases: HashMap::new(),
            columns: HashMap::new(),
            ctes: HashSet::new(),
            parent: Some(Box::new(parent)),
        }
    }

    /// Register a table alias with its columns.
    pub fn add_alias(&mut self, alias: &str, table_name: &str, columns: Option<HashSet<String>>) {
        let key = alias.to_ascii_lowercase();
        self.aliases.insert(key.clone(), table_name.to_owned());
        self.columns.insert(key, columns);
    }

    /// Register a CTE name.
    pub fn add_cte(&mut self, name: &str) {
        self.ctes.insert(name.to_ascii_lowercase());
    }

    /// Check if an alias is visible in this scope (or parent scopes).
    #[must_use]
    pub fn has_alias(&self, alias: &str) -> bool {
        let key = alias.to_ascii_lowercase();
        if self.aliases.contains_key(&key) || self.ctes.contains(&key) {
            return true;
        }
        self.parent.as_ref().is_some_and(|p| p.has_alias(alias))
    }

    /// Check if an alias is defined locally in this scope.
    #[must_use]
    pub fn has_alias_local(&self, alias: &str) -> bool {
        let key = alias.to_ascii_lowercase();
        self.aliases.contains_key(&key) || self.ctes.contains(&key)
    }

    /// Resolve a column reference: find which alias provides it.
    ///
    /// If `table_qualifier` is Some, checks only that alias.
    /// If None, searches all visible aliases for the column name.
    /// Returns the resolved (alias, column_name) or None.
    #[must_use]
    pub fn resolve_column(
        &self,
        table_qualifier: Option<&str>,
        column_name: &str,
    ) -> ResolveResult {
        let col_lower = column_name.to_ascii_lowercase();

        if let Some(qualifier) = table_qualifier {
            let key = qualifier.to_ascii_lowercase();
            if let Some(cols) = self.columns.get(&key) {
                if cols.as_ref().is_none_or(|c| c.contains(&col_lower)) {
                    return ResolveResult::Resolved(key);
                }
                return ResolveResult::ColumnNotFound;
            }
            // Check parent scope.
            if let Some(ref parent) = self.parent {
                return parent.resolve_column(table_qualifier, column_name);
            }
            return ResolveResult::TableNotFound;
        }

        // Unqualified: search all aliases in this scope.
        let mut matches = Vec::new();
        for (alias, cols) in &self.columns {
            if cols.as_ref().is_none_or(|c| c.contains(&col_lower)) {
                matches.push(alias.clone());
            }
        }

        match matches.len() {
            0 => {
                // Check parent scope.
                if let Some(ref parent) = self.parent {
                    return parent.resolve_column(None, column_name);
                }
                ResolveResult::ColumnNotFound
            }
            1 => ResolveResult::Resolved(matches.into_iter().next().unwrap()),
            _ => ResolveResult::Ambiguous(matches),
        }
    }

    /// Number of aliases registered in this scope (not counting parents).
    #[must_use]
    pub fn alias_count(&self) -> usize {
        self.aliases.len()
    }
}

/// Result of resolving a column reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveResult {
    /// Column resolved to the given alias.
    Resolved(String),
    /// The table qualifier was not found.
    TableNotFound,
    /// The column was not found in the specified table.
    ColumnNotFound,
    /// The column was found in multiple tables (ambiguous).
    Ambiguous(Vec<String>),
}

// ---------------------------------------------------------------------------
// Semantic errors
// ---------------------------------------------------------------------------

/// A semantic analysis error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticError {
    /// Error kind.
    pub kind: SemanticErrorKind,
    /// Human-readable message.
    pub message: String,
}

/// Kinds of semantic errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemanticErrorKind {
    /// Column reference could not be resolved.
    UnresolvedColumn {
        table: Option<String>,
        column: String,
    },
    /// Column reference is ambiguous (exists in multiple tables).
    AmbiguousColumn {
        column: String,
        candidates: Vec<String>,
    },
    /// Table or alias not found.
    UnresolvedTable { name: String },
    /// Duplicate alias in the same scope.
    DuplicateAlias { alias: String },
    /// Function called with wrong number of arguments.
    FunctionArityMismatch {
        function: String,
        expected: FunctionArity,
        actual: usize,
    },
    /// Type coercion warning (not fatal).
    ImplicitTypeCoercion {
        from: TypeAffinity,
        to: TypeAffinity,
        context: String,
    },
}

/// Expected function arity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionArity {
    /// Exact number of arguments.
    Exact(usize),
    /// Range of acceptable argument counts.
    Range(usize, usize),
    /// Any number of arguments.
    Variadic,
}

impl std::fmt::Display for SemanticError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

/// The semantic analyzer / name resolver.
///
/// Given a `Schema` and an AST, validates all name references and collects
/// errors. Uses scope tracking for nested queries and CTEs.
pub struct Resolver<'a> {
    schema: &'a Schema,
    errors: Vec<SemanticError>,
    tables_resolved: u64,
    columns_bound: u64,
}

impl<'a> Resolver<'a> {
    /// Create a new resolver for the given schema.
    #[must_use]
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            errors: Vec::new(),
            tables_resolved: 0,
            columns_bound: 0,
        }
    }

    /// Resolve all name references in a statement.
    ///
    /// Returns the list of semantic errors found.
    pub fn resolve_statement(&mut self, stmt: &Statement) -> Vec<SemanticError> {
        let span = tracing::debug_span!(
            target: "fsqlite.parse",
            "semantic_analysis",
            tables_resolved = tracing::field::Empty,
            columns_bound = tracing::field::Empty,
            errors = tracing::field::Empty,
        );
        let _guard = span.enter();

        self.errors.clear();
        self.tables_resolved = 0;
        self.columns_bound = 0;

        let mut scope = Scope::root();
        self.resolve_stmt_inner(stmt, &mut scope);

        span.record("tables_resolved", self.tables_resolved);
        span.record("columns_bound", self.columns_bound);
        span.record("errors", self.errors.len() as u64);

        // Record error metrics.
        if !self.errors.is_empty() {
            FSQLITE_SEMANTIC_ERRORS_TOTAL.fetch_add(self.errors.len() as u64, Ordering::Relaxed);
        }

        self.errors.clone()
    }

    fn resolve_stmt_inner(&mut self, stmt: &Statement, scope: &mut Scope) {
        match stmt {
            Statement::Select(select) => self.resolve_select(select, scope),
            Statement::Insert(insert) => {
                self.resolve_table_name(&insert.table.name, scope);
                match &insert.source {
                    fsqlite_ast::InsertSource::Select(select) => self.resolve_select(select, scope),
                    fsqlite_ast::InsertSource::Values(rows) => {
                        for row in rows {
                            for expr in row {
                                self.resolve_expr(expr, scope);
                            }
                        }
                    }
                    fsqlite_ast::InsertSource::DefaultValues => {}
                }
                for upsert in &insert.upsert {
                    if let Some(target) = &upsert.target {
                        for col in &target.columns {
                            self.resolve_expr(&col.expr, scope);
                        }
                        if let Some(where_clause) = &target.where_clause {
                            self.resolve_expr(where_clause, scope);
                        }
                    }
                    match &upsert.action {
                        fsqlite_ast::UpsertAction::Update { assignments, where_clause } => {
                            for assignment in assignments {
                                self.resolve_expr(&assignment.value, scope);
                            }
                            if let Some(w) = where_clause {
                                self.resolve_expr(w, scope);
                            }
                        }
                        fsqlite_ast::UpsertAction::Nothing => {}
                    }
                }
                for ret in &insert.returning {
                    self.resolve_result_column(ret, scope);
                }
            }
            Statement::Update(update) => {
                self.resolve_table_name(&update.table.name.name, scope);
                for assignment in &update.assignments {
                    self.resolve_expr(&assignment.value, scope);
                }
                if let Some(from) = &update.from {
                    self.resolve_from(from, scope);
                }
                if let Some(where_clause) = &update.where_clause {
                    self.resolve_expr(where_clause, scope);
                }
                for ret in &update.returning {
                    self.resolve_result_column(ret, scope);
                }
                for term in &update.order_by {
                    self.resolve_expr(&term.expr, scope);
                }
                if let Some(limit) = &update.limit {
                    self.resolve_expr(&limit.limit, scope);
                    if let Some(offset) = &limit.offset {
                        self.resolve_expr(offset, scope);
                    }
                }
            }
            Statement::Delete(delete) => {
                self.resolve_table_name(&delete.table.name.name, scope);
                if let Some(where_clause) = &delete.where_clause {
                    self.resolve_expr(where_clause, scope);
                }
                for ret in &delete.returning {
                    self.resolve_result_column(ret, scope);
                }
                for term in &delete.order_by {
                    self.resolve_expr(&term.expr, scope);
                }
                if let Some(limit) = &delete.limit {
                    self.resolve_expr(&limit.limit, scope);
                    if let Some(offset) = &limit.offset {
                        self.resolve_expr(offset, scope);
                    }
                }
            }
            // DDL and control statements don't need name resolution.
            _ => {}
        }
    }

    fn resolve_select(&mut self, select: &SelectStatement, scope: &mut Scope) {
        // Register CTEs first (they are visible in the entire WITH scope).
        if let Some(ref with) = select.with {
            for cte in &with.ctes {
                scope.add_cte(&cte.name);
            }
        }

        // Resolve the primary select core.
        self.resolve_select_core(&select.body.select, scope);

        // Resolve any compound queries (UNION, INTERSECT, EXCEPT).
        for (_op, core) in &select.body.compounds {
            self.resolve_select_core(core, scope);
        }

        // Resolve ORDER BY.
        for term in &select.order_by {
            self.resolve_expr(&term.expr, scope);
        }

        // Resolve LIMIT.
        if let Some(limit) = &select.limit {
            self.resolve_expr(&limit.limit, scope);
            if let Some(offset) = &limit.offset {
                self.resolve_expr(offset, scope);
            }
        }
    }

    fn resolve_select_core(&mut self, core: &SelectCore, scope: &mut Scope) {
        match core {
            SelectCore::Select {
                columns,
                from,
                where_clause,
                group_by,
                having,
                windows,
                ..
            } => {
                // Resolve FROM clause first (registers table aliases).
                if let Some(from) = from {
                    self.resolve_from(from, scope);
                }

                // Resolve column references in SELECT list.
                for col in columns {
                    self.resolve_result_column(col, scope);
                }

                // Resolve WHERE clause.
                if let Some(where_expr) = where_clause {
                    self.resolve_expr(where_expr, scope);
                }

                // Resolve GROUP BY.
                for expr in group_by {
                    self.resolve_expr(expr, scope);
                }

                // Resolve HAVING.
                if let Some(having_expr) = having {
                    self.resolve_expr(having_expr, scope);
                }

                // Resolve WINDOW definitions.
                for window_def in windows {
                    for expr in &window_def.spec.partition_by {
                        self.resolve_expr(expr, scope);
                    }
                    for term in &window_def.spec.order_by {
                        self.resolve_expr(&term.expr, scope);
                    }
                    if let Some(frame) = &window_def.spec.frame {
                        match &frame.start {
                            fsqlite_ast::FrameBound::Preceding(expr)
                            | fsqlite_ast::FrameBound::Following(expr) => {
                                self.resolve_expr(expr, scope);
                            }
                            _ => {}
                        }
                        if let Some(end) = &frame.end {
                            match end {
                                fsqlite_ast::FrameBound::Preceding(expr)
                                | fsqlite_ast::FrameBound::Following(expr) => {
                                    self.resolve_expr(expr, scope);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            SelectCore::Values(_) => {
                // VALUES doesn't reference columns.
            }
        }
    }

    fn resolve_from(&mut self, from: &FromClause, scope: &mut Scope) {
        self.resolve_table_or_subquery(&from.source, scope);

        for join in &from.joins {
            self.resolve_join(join, scope);
        }
    }

    fn resolve_table_or_subquery(&mut self, tos: &TableOrSubquery, scope: &mut Scope) {
        match tos {
            TableOrSubquery::Table { name, alias, .. } => {
                let table_name = &name.name;
                let alias_name = alias.as_deref().unwrap_or(table_name);

                // Check for duplicate alias in the CURRENT scope only.
                if scope.has_alias_local(alias_name) {
                    self.push_error(SemanticErrorKind::DuplicateAlias {
                        alias: alias_name.to_owned(),
                    });
                }

                // Resolve table name against schema or CTEs.
                if scope.ctes.contains(&table_name.to_ascii_lowercase()) {
                    // CTE reference — columns are unknown at this stage.
                    scope.add_alias(alias_name, table_name, None);
                    self.tables_resolved += 1;
                } else if let Some(table_def) = self.schema.find_table(table_name) {
                    let col_set: HashSet<String> = table_def
                        .columns
                        .iter()
                        .map(|c| c.name.to_ascii_lowercase())
                        .collect();
                    scope.add_alias(alias_name, table_name, Some(col_set));
                    self.tables_resolved += 1;
                } else {
                    self.push_error(SemanticErrorKind::UnresolvedTable {
                        name: table_name.clone(),
                    });
                }
            }
            TableOrSubquery::Subquery { query, alias, .. } => {
                // Resolve subquery in a child scope.
                let mut child = Scope::child(scope.clone());
                self.resolve_select(query, &mut child);

                // Register the subquery alias with empty columns (we don't
                // track subquery output columns at this stage).
                let alias_name = alias.as_deref().unwrap_or("");

                if !alias_name.is_empty() && scope.has_alias_local(alias_name) {
                    self.push_error(SemanticErrorKind::DuplicateAlias {
                        alias: alias_name.to_owned(),
                    });
                }

                scope.add_alias(alias_name, "<subquery>", None);
            }
            TableOrSubquery::TableFunction { name, args, alias, .. } => {
                for arg in args {
                    self.resolve_expr(arg, scope);
                }

                let alias_name = alias.as_deref().unwrap_or(name);

                if scope.has_alias_local(alias_name) {
                    self.push_error(SemanticErrorKind::DuplicateAlias {
                        alias: alias_name.to_owned(),
                    });
                }

                scope.add_alias(alias_name, name, None);
                self.tables_resolved += 1;
            }
            TableOrSubquery::ParenJoin(inner_from) => {
                self.resolve_from(inner_from, scope);
            }
        }
    }

    fn resolve_join(&mut self, join: &JoinClause, scope: &mut Scope) {
        self.resolve_table_or_subquery(&join.table, scope);
        if let Some(ref constraint) = join.constraint {
            match constraint {
                JoinConstraint::On(expr) => self.resolve_expr(expr, scope),
                JoinConstraint::Using(cols) => {
                    for col in cols {
                        self.resolve_unqualified_column(col, scope);
                    }
                }
            }
        }
    }

    fn resolve_result_column(&mut self, col: &ResultColumn, scope: &Scope) {
        match col {
            ResultColumn::Star => {
                // SELECT * is valid if there's at least one table in scope.
                if scope.alias_count() == 0 && scope.parent.is_none() {
                    tracing::warn!(
                        target: "fsqlite.parse",
                        "SELECT * with no tables in scope"
                    );
                }
            }
            ResultColumn::TableStar(table_name) => {
                if !scope.has_alias(table_name) {
                    self.push_error(SemanticErrorKind::UnresolvedTable {
                        name: table_name.clone(),
                    });
                }
            }
            ResultColumn::Expr { expr, .. } => {
                self.resolve_expr(expr, scope);
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn resolve_expr(&mut self, expr: &Expr, scope: &Scope) {
        match expr {
            Expr::Column(col_ref, _span) => {
                self.resolve_column_ref(col_ref, scope);
            }
            Expr::BinaryOp { left, right, .. } => {
                self.resolve_expr(left, scope);
                self.resolve_expr(right, scope);
            }
            Expr::UnaryOp { expr: inner, .. }
            | Expr::Cast { expr: inner, .. }
            | Expr::Collate { expr: inner, .. }
            | Expr::IsNull { expr: inner, .. } => {
                self.resolve_expr(inner, scope);
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                ..
            } => {
                self.resolve_expr(inner, scope);
                self.resolve_expr(low, scope);
                self.resolve_expr(high, scope);
            }
            Expr::In {
                expr: inner, set, ..
            } => {
                self.resolve_expr(inner, scope);
                match set {
                    InSet::List(items) => {
                        for item in items {
                            self.resolve_expr(item, scope);
                        }
                    }
                    InSet::Subquery(select) => {
                        let mut child = Scope::child(scope.clone());
                        self.resolve_select(select, &mut child);
                    }
                    InSet::Table(name) => {
                        self.resolve_table_name(&name.name, scope);
                    }
                }
            }
            Expr::Like {
                expr: inner,
                pattern,
                escape,
                ..
            } => {
                self.resolve_expr(inner, scope);
                self.resolve_expr(pattern, scope);
                if let Some(esc) = escape {
                    self.resolve_expr(esc, scope);
                }
            }
            Expr::Subquery(select, _)
            | Expr::Exists {
                subquery: select, ..
            } => {
                let mut child = Scope::child(scope.clone());
                self.resolve_select(select, &mut child);
            }
            Expr::FunctionCall {
                name, args, filter, over, ..
            } => {
                let arg_slice: &[Expr] = match args {
                    FunctionArgs::Star => &[],
                    FunctionArgs::List(list) => list,
                };
                self.resolve_function(name, arg_slice, scope);
                if let Some(filter) = filter {
                    self.resolve_expr(filter, scope);
                }
                if let Some(window_spec) = over {
                    for expr in &window_spec.partition_by {
                        self.resolve_expr(expr, scope);
                    }
                    for term in &window_spec.order_by {
                        self.resolve_expr(&term.expr, scope);
                    }
                    if let Some(frame) = &window_spec.frame {
                        match &frame.start {
                            fsqlite_ast::FrameBound::Preceding(expr)
                            | fsqlite_ast::FrameBound::Following(expr) => {
                                self.resolve_expr(expr, scope);
                            }
                            _ => {}
                        }
                        if let Some(end) = &frame.end {
                            match end {
                                fsqlite_ast::FrameBound::Preceding(expr)
                                | fsqlite_ast::FrameBound::Following(expr) => {
                                    self.resolve_expr(expr, scope);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Expr::Case {
                operand,
                whens,
                else_expr,
                ..
            } => {
                if let Some(op) = operand {
                    self.resolve_expr(op, scope);
                }
                for (when_expr, then_expr) in whens {
                    self.resolve_expr(when_expr, scope);
                    self.resolve_expr(then_expr, scope);
                }
                if let Some(else_e) = else_expr {
                    self.resolve_expr(else_e, scope);
                }
            }
            Expr::JsonAccess {
                expr: inner, path, ..
            } => {
                self.resolve_expr(inner, scope);
                self.resolve_expr(path, scope);
            }
            Expr::RowValue(exprs, _) => {
                for e in exprs {
                    self.resolve_expr(e, scope);
                }
            }
            // Literals, placeholders, and RAISE don't need resolution.
            Expr::Literal(_, _) | Expr::Placeholder(_, _) | Expr::Raise { .. } => {}
        }
    }

    fn resolve_column_ref(&mut self, col_ref: &ColumnRef, scope: &Scope) {
        let result = scope.resolve_column(col_ref.table.as_deref(), &col_ref.column);
        match result {
            ResolveResult::Resolved(_) => {
                self.columns_bound += 1;
            }
            ResolveResult::TableNotFound => {
                tracing::error!(
                    target: "fsqlite.parse",
                    table = ?col_ref.table,
                    column = %col_ref.column,
                    "unresolvable table reference"
                );
                self.push_error(SemanticErrorKind::UnresolvedColumn {
                    table: col_ref.table.clone(),
                    column: col_ref.column.clone(),
                });
            }
            ResolveResult::ColumnNotFound => {
                tracing::error!(
                    target: "fsqlite.parse",
                    table = ?col_ref.table,
                    column = %col_ref.column,
                    "unresolvable column reference"
                );
                self.push_error(SemanticErrorKind::UnresolvedColumn {
                    table: col_ref.table.clone(),
                    column: col_ref.column.clone(),
                });
            }
            ResolveResult::Ambiguous(candidates) => {
                tracing::error!(
                    target: "fsqlite.parse",
                    column = %col_ref.column,
                    candidates = ?candidates,
                    "ambiguous column reference"
                );
                self.push_error(SemanticErrorKind::AmbiguousColumn {
                    column: col_ref.column.clone(),
                    candidates,
                });
            }
        }
    }

    fn resolve_unqualified_column(&mut self, name: &str, scope: &Scope) {
        let result = scope.resolve_column(None, name);
        match result {
            ResolveResult::Resolved(_) => {
                self.columns_bound += 1;
            }
            ResolveResult::ColumnNotFound | ResolveResult::TableNotFound => {
                self.push_error(SemanticErrorKind::UnresolvedColumn {
                    table: None,
                    column: name.to_owned(),
                });
            }
            ResolveResult::Ambiguous(candidates) => {
                self.push_error(SemanticErrorKind::AmbiguousColumn {
                    column: name.to_owned(),
                    candidates,
                });
            }
        }
    }

    fn resolve_table_name(&mut self, name: &str, scope: &Scope) {
        if scope.ctes.contains(&name.to_ascii_lowercase()) || self.schema.find_table(name).is_some()
        {
            self.tables_resolved += 1;
        } else {
            self.push_error(SemanticErrorKind::UnresolvedTable {
                name: name.to_owned(),
            });
        }
    }

    fn resolve_function(&mut self, name: &str, args: &[Expr], scope: &Scope) {
        // Resolve argument expressions.
        for arg in args {
            self.resolve_expr(arg, scope);
        }

        // Validate known function arity.
        if let Some(expected) = known_function_arity(name) {
            let actual = args.len();
            let valid = match &expected {
                FunctionArity::Exact(n) => actual == *n,
                FunctionArity::Range(lo, hi) => actual >= *lo && actual <= *hi,
                FunctionArity::Variadic => true,
            };
            if !valid {
                self.push_error(SemanticErrorKind::FunctionArityMismatch {
                    function: name.to_owned(),
                    expected,
                    actual,
                });
            }
        }
    }

    fn push_error(&mut self, kind: SemanticErrorKind) {
        let message = match &kind {
            SemanticErrorKind::UnresolvedColumn { table, column } => {
                if let Some(t) = table {
                    format!("no such column: {t}.{column}")
                } else {
                    format!("no such column: {column}")
                }
            }
            SemanticErrorKind::AmbiguousColumn {
                column, candidates, ..
            } => {
                format!(
                    "ambiguous column name: {column} (candidates: {})",
                    candidates.join(", ")
                )
            }
            SemanticErrorKind::UnresolvedTable { name } => {
                format!("no such table: {name}")
            }
            SemanticErrorKind::DuplicateAlias { alias } => {
                format!("duplicate alias: {alias}")
            }
            SemanticErrorKind::FunctionArityMismatch {
                function,
                expected,
                actual,
            } => {
                format!(
                    "wrong number of arguments to function {function}: expected {expected:?}, got {actual}"
                )
            }
            SemanticErrorKind::ImplicitTypeCoercion {
                from, to, context, ..
            } => {
                format!("implicit type coercion from {from:?} to {to:?} in {context}")
            }
        };

        self.errors.push(SemanticError { kind, message });
    }
}

// ---------------------------------------------------------------------------
// Known function arity table
// ---------------------------------------------------------------------------

/// Returns the expected arity for a known SQLite function, if recognized.
#[must_use]
fn known_function_arity(name: &str) -> Option<FunctionArity> {
    match name.to_ascii_lowercase().as_str() {
        "random" | "changes" | "last_insert_rowid" | "total_changes" => {
            Some(FunctionArity::Exact(0))
        }
        // Aggregate (1-arg) and scalar (1-arg) functions
        "sum" | "total" | "avg" | "abs" | "hex" | "length" | "lower" | "upper" | "typeof"
        | "unicode" | "quote" | "zeroblob" | "soundex" | "likelihood" | "randomblob" => {
            Some(FunctionArity::Exact(1))
        }
        "ifnull" | "nullif" | "instr" | "glob" => Some(FunctionArity::Exact(2)),
        "iif" | "replace" => Some(FunctionArity::Exact(3)),
        "count" => Some(FunctionArity::Range(0, 1)),
        "group_concat" | "trim" | "ltrim" | "rtrim" => Some(FunctionArity::Range(1, 2)),
        "substr" | "substring" | "like" => Some(FunctionArity::Range(2, 3)),
        // Variadic: aggregates, scalars, date/time, and JSON functions
        "min" | "max" | "coalesce" | "printf" | "format" | "char" | "date" | "time"
        | "datetime" | "julianday" | "strftime" | "unixepoch" | "json" | "json_array"
        | "json_object" | "json_type" | "json_valid" | "json_extract" | "json_insert"
        | "json_replace" | "json_set" | "json_remove" => Some(FunctionArity::Variadic),

        _ => None, // Unknown function — skip arity check.
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;

    fn make_schema() -> Schema {
        let mut schema = Schema::new();
        schema.add_table(TableDef {
            name: "users".to_owned(),
            columns: vec![
                ColumnDef {
                    name: "id".to_owned(),
                    affinity: TypeAffinity::Integer,
                    is_ipk: true,
                    not_null: true,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    affinity: TypeAffinity::Text,
                    is_ipk: false,
                    not_null: true,
                },
                ColumnDef {
                    name: "email".to_owned(),
                    affinity: TypeAffinity::Text,
                    is_ipk: false,
                    not_null: false,
                },
            ],
            without_rowid: false,
            strict: false,
        });
        schema.add_table(TableDef {
            name: "orders".to_owned(),
            columns: vec![
                ColumnDef {
                    name: "id".to_owned(),
                    affinity: TypeAffinity::Integer,
                    is_ipk: true,
                    not_null: true,
                },
                ColumnDef {
                    name: "user_id".to_owned(),
                    affinity: TypeAffinity::Integer,
                    is_ipk: false,
                    not_null: true,
                },
                ColumnDef {
                    name: "amount".to_owned(),
                    affinity: TypeAffinity::Real,
                    is_ipk: false,
                    not_null: false,
                },
            ],
            without_rowid: false,
            strict: false,
        });
        schema
    }

    fn parse_one(sql: &str) -> Statement {
        let mut p = Parser::from_sql(sql);
        let (stmts, errs) = p.parse_all();
        assert!(errs.is_empty(), "parse errors: {errs:?}");
        assert_eq!(stmts.len(), 1);
        stmts.into_iter().next().unwrap()
    }

    // ── Schema tests ──

    #[test]
    fn test_schema_find_table_case_insensitive() {
        let schema = make_schema();
        assert!(schema.find_table("users").is_some());
        assert!(schema.find_table("USERS").is_some());
        assert!(schema.find_table("Users").is_some());
        assert!(schema.find_table("nonexistent").is_none());
    }

    #[test]
    fn test_table_find_column() {
        let schema = make_schema();
        let users = schema.find_table("users").unwrap();
        assert!(users.has_column("id"));
        assert!(users.has_column("ID"));
        assert!(!users.has_column("nonexistent"));
    }

    #[test]
    fn test_table_rowid_alias() {
        let schema = make_schema();
        let users = schema.find_table("users").unwrap();
        assert!(users.is_rowid_alias("rowid"));
        assert!(users.is_rowid_alias("_rowid_"));
        assert!(users.is_rowid_alias("oid"));
        assert!(users.is_rowid_alias("id")); // IPK
        assert!(!users.is_rowid_alias("name"));
    }

    // ── Scope tests ──

    #[test]
    fn test_scope_resolve_qualified_column() {
        let mut scope = Scope::root();
        let cols: HashSet<String> = ["id", "name", "email"]
            .iter()
            .map(ToString::to_string)
            .collect();
        scope.add_alias("u", "users", Some(cols));

        assert_eq!(
            scope.resolve_column(Some("u"), "id"),
            ResolveResult::Resolved("u".to_string())
        );
        assert_eq!(
            scope.resolve_column(Some("u"), "nonexistent"),
            ResolveResult::ColumnNotFound
        );
        assert_eq!(
            scope.resolve_column(Some("x"), "id"),
            ResolveResult::TableNotFound
        );
    }

    #[test]
    fn test_scope_resolve_unqualified_column() {
        let mut scope = Scope::root();
        scope.add_alias(
            "u",
            "users",
            Some(["id", "name"].iter().map(ToString::to_string).collect()),
        );
        scope.add_alias(
            "o",
            "orders",
            Some(["id", "user_id"].iter().map(ToString::to_string).collect()),
        );

        // "name" is unique → resolved to "u"
        assert_eq!(
            scope.resolve_column(None, "name"),
            ResolveResult::Resolved("u".to_string())
        );

        // "user_id" is unique → resolved to "o"
        assert_eq!(
            scope.resolve_column(None, "user_id"),
            ResolveResult::Resolved("o".to_string())
        );

        // "id" is ambiguous
        match scope.resolve_column(None, "id") {
            ResolveResult::Ambiguous(candidates) => {
                assert_eq!(candidates.len(), 2);
            }
            other => panic!("expected Ambiguous, got {other:?}"),
        }

        // "nonexistent" not found
        assert_eq!(
            scope.resolve_column(None, "nonexistent"),
            ResolveResult::ColumnNotFound
        );
    }

    #[test]
    fn test_scope_child_inherits_parent() {
        let mut parent = Scope::root();
        parent.add_alias(
            "u",
            "users",
            Some(["id", "name"].iter().map(ToString::to_string).collect()),
        );
        let child = Scope::child(parent);

        // Child can see parent's columns.
        assert_eq!(
            child.resolve_column(Some("u"), "id"),
            ResolveResult::Resolved("u".to_string())
        );
    }

    // ── Resolver tests ──

    #[test]
    fn test_resolve_simple_select() {
        let schema = make_schema();
        let stmt = parse_one("SELECT id, name FROM users");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(resolver.tables_resolved, 1);
        assert_eq!(resolver.columns_bound, 2);
    }

    #[test]
    fn test_resolve_qualified_column() {
        let schema = make_schema();
        let stmt = parse_one("SELECT u.id, u.name FROM users u");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(resolver.tables_resolved, 1);
        assert_eq!(resolver.columns_bound, 2);
    }

    #[test]
    fn test_resolve_join() {
        let schema = make_schema();
        let stmt =
            parse_one("SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(resolver.tables_resolved, 2);
        assert_eq!(resolver.columns_bound, 4); // u.name, o.amount, u.id, o.user_id
    }

    #[test]
    fn test_resolve_unresolved_table() {
        let schema = make_schema();
        let stmt = parse_one("SELECT * FROM nonexistent");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0].kind,
            SemanticErrorKind::UnresolvedTable { .. }
        ));
    }

    #[test]
    fn test_resolve_unresolved_column() {
        let schema = make_schema();
        let stmt = parse_one("SELECT nonexistent FROM users");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0].kind,
            SemanticErrorKind::UnresolvedColumn { .. }
        ));
    }

    #[test]
    fn test_resolve_ambiguous_column() {
        let schema = make_schema();
        let stmt = parse_one("SELECT id FROM users, orders");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0].kind,
            SemanticErrorKind::AmbiguousColumn { .. }
        ));
    }

    #[test]
    fn test_resolve_where_clause() {
        let schema = make_schema();
        let stmt = parse_one("SELECT name FROM users WHERE id > 10");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(resolver.columns_bound, 2); // name, id
    }

    #[test]
    fn test_resolve_star_select() {
        let schema = make_schema();
        let stmt = parse_one("SELECT * FROM users");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(resolver.tables_resolved, 1);
    }

    #[test]
    fn test_resolve_insert_checks_table() {
        let schema = make_schema();
        let stmt = parse_one("INSERT INTO nonexistent VALUES (1)");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0].kind,
            SemanticErrorKind::UnresolvedTable { .. }
        ));
    }

    // ── Metrics tests ──

    #[test]
    fn test_semantic_metrics() {
        reset_semantic_metrics();
        let schema = make_schema();

        // Trigger an error.
        let stmt = parse_one("SELECT nonexistent FROM users");
        let mut resolver = Resolver::new(&schema);
        let _ = resolver.resolve_statement(&stmt);

        let snap = semantic_metrics_snapshot();
        assert!(snap.fsqlite_semantic_errors_total >= 1);
    }
}
