//! Semantic analysis: name resolution, type checking, and scope validation.
//!
//! Validates AST nodes against a schema to ensure:
//! - Column references resolve to known tables/columns
//! - Every FROM source remains independently addressable during name resolution
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
    ColumnRef, Cte, Expr, FromClause, FunctionArgs, InSet, JoinClause, JoinConstraint, Literal,
    QualifiedName, ResultColumn, SelectCore, SelectStatement, Statement, TableOrSubquery,
    WithClause,
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
        if let Some(column) = self.find_column(name) {
            return column.is_ipk;
        }
        is_hidden_rowid_alias_name(name)
    }
}

fn is_hidden_rowid_alias_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "rowid" | "_rowid_" | "oid"
    )
}

/// The database schema: a collection of table definitions.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    /// Tables by lowercase name.
    tables: HashMap<String, TableDef>,
    /// Non-main schema tables by lowercase schema name then lowercase table name.
    namespaced_tables: HashMap<String, HashMap<String, TableDef>>,
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

    /// Add a table definition to a specific schema namespace.
    pub fn add_table_in_schema(&mut self, schema_name: &str, table: TableDef) {
        if schema_name.eq_ignore_ascii_case("main") {
            self.add_table(table);
            return;
        }

        self.namespaced_tables
            .entry(schema_name.to_ascii_lowercase())
            .or_default()
            .insert(table.name.to_ascii_lowercase(), table);
    }

    /// Look up a table by name (case-insensitive).
    #[must_use]
    pub fn find_table(&self, name: &str) -> Option<&TableDef> {
        self.tables.get(&name.to_ascii_lowercase())
    }

    /// Look up a table by optional schema-qualified name.
    #[must_use]
    pub fn find_table_in_schema(&self, schema: Option<&str>, name: &str) -> Option<&TableDef> {
        match schema {
            None => self.find_table(name),
            Some(schema_name) if schema_name.eq_ignore_ascii_case("main") => self.find_table(name),
            Some(schema_name) => self
                .namespaced_tables
                .get(&schema_name.to_ascii_lowercase())
                .and_then(|tables| tables.get(&name.to_ascii_lowercase())),
        }
    }

    /// Look up a table by a scope lookup key produced by `table_lookup_key`.
    #[must_use]
    pub fn find_table_by_lookup_key(&self, lookup_key: &str) -> Option<&TableDef> {
        if let Some((schema_name, table_name)) = lookup_key.split_once('\0') {
            self.find_table_in_schema(Some(schema_name), table_name)
        } else {
            self.find_table(lookup_key)
        }
    }

    /// Number of tables in the schema.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
            + self
                .namespaced_tables
                .values()
                .map(std::collections::HashMap::len)
                .sum::<usize>()
    }
}

fn table_lookup_key(name: &QualifiedName) -> String {
    match name.schema.as_deref() {
        None => name.name.to_ascii_lowercase(),
        Some(schema_name) if schema_name.eq_ignore_ascii_case("main") => {
            name.name.to_ascii_lowercase()
        }
        Some(schema_name) => format!(
            "{}\0{}",
            schema_name.to_ascii_lowercase(),
            name.name.to_ascii_lowercase()
        ),
    }
}

// ---------------------------------------------------------------------------
// Scope tracking
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct QualifiedColumnBinding {
    lookup_key: String,
    table_name: String,
    columns: Option<HashSet<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum StarNamespace {
    Database(String),
    Derived,
}

#[derive(Debug, Clone)]
struct OrderedColumns {
    ordered: Vec<String>,
    membership: HashSet<String>,
}

impl OrderedColumns {
    fn new(columns: impl IntoIterator<Item = String>) -> Self {
        let ordered = columns
            .into_iter()
            .map(|column| column.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let membership = ordered.iter().cloned().collect();
        Self {
            ordered,
            membership,
        }
    }
}

#[derive(Debug, Clone)]
enum OutputColumns {
    Known(OrderedColumns),
    Unknown,
}

impl OutputColumns {
    fn membership(&self) -> Option<&HashSet<String>> {
        match self {
            Self::Known(columns) => Some(&columns.membership),
            Self::Unknown => None,
        }
    }

    fn ordered(&self) -> Option<&[String]> {
        match self {
            Self::Known(columns) => Some(&columns.ordered),
            Self::Unknown => None,
        }
    }
}

#[derive(Debug, Clone)]
struct FromSourceBinding {
    effective_name: String,
    lookup_key: String,
    star_namespace: StarNamespace,
    output_columns: OutputColumns,
    qualified_only: bool,
}

/// A name scope for query resolution. Scopes nest for subqueries and CTEs.
#[derive(Debug, Clone)]
pub struct Scope {
    /// One binding per FROM source. Multiple sources can legitimately expose
    /// the same effective name (for example `main.t JOIN temp.t`), so name
    /// resolution must filter each source by the requested column before
    /// deciding whether the reference is ambiguous.
    bindings: Vec<FromSourceBinding>,
    /// Columns that were joined via `USING` and are therefore unambiguous.
    pub using_columns: HashSet<String>,
    /// CTE names visible in this scope.
    ctes: HashMap<String, Option<Vec<String>>>,
    /// Real table identities addressable as `schema.table.column`.
    qualified_bindings: Vec<QualifiedColumnBinding>,
    /// Parent scope (for subquery nesting).
    parent: Option<Box<Self>>,
}

impl Scope {
    /// Create a root scope.
    #[must_use]
    pub fn root() -> Self {
        Self {
            bindings: Vec::new(),
            using_columns: HashSet::new(),
            ctes: HashMap::new(),
            qualified_bindings: Vec::new(),
            parent: None,
        }
    }

    /// Create a child scope (for subqueries).
    #[must_use]
    pub fn child(parent: Self) -> Self {
        Self {
            bindings: Vec::new(),
            using_columns: HashSet::new(),
            ctes: HashMap::new(),
            qualified_bindings: Vec::new(),
            parent: Some(Box::new(parent)),
        }
    }

    /// Register a table alias with its columns.
    pub fn add_alias(&mut self, alias: &str, table_name: &str, columns: Option<HashSet<String>>) {
        let ordered_columns = columns.map(|columns| {
            let mut columns = columns.into_iter().collect::<Vec<_>>();
            columns.sort_unstable();
            columns
        });
        let star_namespace = if let Some((schema, _)) = table_name.split_once('\0') {
            StarNamespace::Database(schema.to_ascii_lowercase())
        } else if table_name.starts_with('<') {
            StarNamespace::Derived
        } else {
            StarNamespace::Database("main".to_owned())
        };
        self.add_alias_with_metadata(alias, table_name, star_namespace, ordered_columns, false);
    }

    fn add_alias_with_metadata(
        &mut self,
        alias: &str,
        lookup_key: &str,
        star_namespace: StarNamespace,
        ordered_columns: Option<Vec<String>>,
        qualified_only: bool,
    ) {
        self.bindings.push(FromSourceBinding {
            effective_name: alias.to_ascii_lowercase(),
            lookup_key: lookup_key.to_owned(),
            star_namespace,
            output_columns: ordered_columns.map_or(OutputColumns::Unknown, |columns| {
                OutputColumns::Known(OrderedColumns::new(columns))
            }),
            qualified_only,
        });
    }

    fn add_table_binding(
        &mut self,
        name: &QualifiedName,
        alias: Option<&str>,
        ordered_columns: Option<Vec<String>>,
    ) {
        let effective_name = alias.unwrap_or(&name.name);
        let membership = ordered_columns
            .as_ref()
            .map(|columns| columns.iter().cloned().collect());
        if effective_name.eq_ignore_ascii_case(&name.name) {
            self.qualified_bindings.push(QualifiedColumnBinding {
                lookup_key: table_lookup_key(name),
                table_name: name.name.to_ascii_lowercase(),
                columns: membership,
            });
        }
        let schema_name = name
            .schema
            .as_deref()
            .unwrap_or("main")
            .to_ascii_lowercase();
        self.add_alias_with_metadata(
            effective_name,
            &table_lookup_key(name),
            StarNamespace::Database(schema_name),
            ordered_columns,
            false,
        );
    }

    /// Register an alias that does not participate in unqualified column resolution.
    pub fn add_qualified_only_alias(
        &mut self,
        alias: &str,
        table_name: &str,
        columns: Option<HashSet<String>>,
    ) {
        let ordered_columns = columns.map(|columns| {
            let mut columns = columns.into_iter().collect::<Vec<_>>();
            columns.sort_unstable();
            columns
        });
        self.add_alias_with_metadata(
            alias,
            table_name,
            StarNamespace::Derived,
            ordered_columns,
            true,
        );
    }

    /// Register a CTE name.
    pub fn add_cte(&mut self, name: &str) {
        self.add_cte_with_columns(name, None);
    }

    fn add_cte_with_columns(&mut self, name: &str, columns: Option<Vec<String>>) {
        self.ctes.insert(name.to_ascii_lowercase(), columns);
    }

    /// Check if a CTE is visible in this scope (or parent scopes).
    #[must_use]
    pub fn has_cte(&self, name: &str) -> bool {
        let key = name.to_ascii_lowercase();
        if self.ctes.contains_key(&key) {
            return true;
        }
        self.parent.as_ref().is_some_and(|p| p.has_cte(name))
    }

    fn cte_columns(&self, name: &str) -> Option<Option<Vec<String>>> {
        let key = name.to_ascii_lowercase();
        if let Some(columns) = self.ctes.get(&key) {
            return Some(columns.clone());
        }
        self.parent
            .as_ref()
            .and_then(|parent| parent.cte_columns(name))
    }

    /// Check if an alias is visible in this scope (or parent scopes).
    #[must_use]
    pub fn has_alias(&self, alias: &str) -> bool {
        let key = alias.to_ascii_lowercase();
        if self
            .bindings
            .iter()
            .any(|binding| binding.effective_name == key)
        {
            return true;
        }
        self.parent.as_ref().is_some_and(|p| p.has_alias(alias))
    }

    /// Check if a table reference is visible in this scope.
    ///
    /// Bare `table.*` must name the visible alias exactly. An explicit alias
    /// hides the underlying table name. Three-part `schema.table.*` is not
    /// legal SQLite syntax and is rejected by the parser. `table.*` is local
    /// to the current SELECT's FROM scope and never correlates to a parent
    /// SELECT. Unlike an ordinary qualified column, it expands every
    /// same-named local source in FROM order.
    #[must_use]
    pub fn has_table_reference(&self, name: &QualifiedName) -> bool {
        self.table_star_source_columns(name).is_some()
    }

    /// Return the known column sets for every source expanded by `table.*`.
    ///
    /// The outer vector preserves FROM-source order. A `None` entry represents
    /// a source whose columns are not known during this semantic pass.
    fn table_star_source_columns(
        &self,
        name: &QualifiedName,
    ) -> Option<Vec<Option<&HashSet<String>>>> {
        if name.schema.is_some() {
            return None;
        }
        let target_name = name.name.to_ascii_lowercase();
        let local_matches: Vec<_> = self
            .bindings
            .iter()
            .filter(|binding| binding.effective_name == target_name)
            .map(|binding| binding.output_columns.membership())
            .collect();
        if !local_matches.is_empty() {
            return Some(local_matches);
        }

        None
    }

    /// Return the first column made ambiguous by duplicate aliases within one
    /// database schema.
    ///
    /// SQLite expands duplicate aliases across distinct schemas (for example
    /// `main.a AS q JOIN temp.b AS q`) even when their column names overlap.
    /// Within one schema, however, an overlapping column makes `q.*`
    /// ambiguous. Ordinary `q.column` resolution remains schema-independent
    /// and is handled separately by [`Scope::resolve_column`].
    fn table_star_ambiguity(&self, name: &QualifiedName) -> Option<(String, Vec<String>)> {
        if name.schema.is_some() {
            return None;
        }

        let target_name = name.name.to_ascii_lowercase();
        let matching_bindings = self
            .bindings
            .iter()
            .filter(|binding| binding.effective_name == target_name)
            .collect::<Vec<_>>();
        for (binding_index, binding) in matching_bindings.iter().enumerate() {
            let Some(columns) = binding.output_columns.ordered() else {
                continue;
            };
            for column in columns {
                let overlaps_later_source = matching_bindings
                    .iter()
                    .skip(binding_index.saturating_add(1))
                    .any(|candidate| {
                        candidate.star_namespace == binding.star_namespace
                            && candidate
                                .output_columns
                                .membership()
                                .is_some_and(|columns| columns.contains(column))
                    });
                if overlaps_later_source {
                    let candidates = matching_bindings
                        .iter()
                        .filter(|candidate| candidate.star_namespace == binding.star_namespace)
                        .filter(|candidate| {
                            candidate
                                .output_columns
                                .membership()
                                .is_some_and(|columns| columns.contains(column))
                        })
                        .map(|candidate| candidate.effective_name.clone())
                        .collect();
                    return Some((column.clone(), candidates));
                }
            }
        }
        None
    }

    /// Check if an alias is defined locally in this scope.
    #[must_use]
    pub fn has_alias_local(&self, alias: &str) -> bool {
        let key = alias.to_ascii_lowercase();
        self.bindings
            .iter()
            .any(|binding| binding.effective_name == key)
    }

    /// Resolve a column reference: find which alias provides it.
    ///
    /// If `table_qualifier` is Some, checks only that alias.
    /// If None, searches all visible aliases for the column name.
    /// Returns the resolved (alias, column_name) or None.
    #[must_use]
    pub fn resolve_column(
        &self,
        schema: &Schema,
        table_qualifier: Option<&str>,
        column_name: &str,
    ) -> ResolveResult {
        let col_lower = column_name.to_ascii_lowercase();

        if let Some(qualifier) = table_qualifier {
            let key = qualifier.to_ascii_lowercase();
            let mut table_matches = 0_usize;
            let mut column_matches = Vec::new();
            for binding in self
                .bindings
                .iter()
                .filter(|binding| binding.effective_name == key)
            {
                table_matches = table_matches.saturating_add(1);
                let column_exists = binding
                    .output_columns
                    .membership()
                    .is_none_or(|columns| columns.contains(&col_lower))
                    || schema
                        .find_table_by_lookup_key(&binding.lookup_key)
                        .is_some_and(|table| table.is_rowid_alias(&col_lower));
                if column_exists {
                    column_matches.push(binding.effective_name.clone());
                }
            }

            return match column_matches.len() {
                1 => ResolveResult::Resolved(column_matches.remove(0)),
                count if count > 1 => ResolveResult::Ambiguous(column_matches),
                _ if table_matches > 0 => ResolveResult::ColumnNotFound,
                _ => {
                    if let Some(parent) = &self.parent {
                        parent.resolve_column(schema, table_qualifier, column_name)
                    } else {
                        ResolveResult::TableNotFound
                    }
                }
            };
        }

        // Unqualified: search every local FROM source independently.
        let mut matches = Vec::new();
        for binding in &self.bindings {
            if binding.qualified_only {
                continue;
            }
            let is_match = match binding.output_columns.membership() {
                Some(c) => {
                    c.contains(&col_lower) || {
                        schema
                            .find_table_by_lookup_key(&binding.lookup_key)
                            .is_some_and(|td| td.is_rowid_alias(&col_lower))
                    }
                }
                None => true,
            };
            if is_match {
                matches.push(binding.effective_name.clone());
            }
        }

        match matches.len() {
            0 => {
                // Check parent scope.
                if let Some(ref parent) = self.parent {
                    return parent.resolve_column(schema, None, column_name);
                }
                ResolveResult::ColumnNotFound
            }
            1 => ResolveResult::Resolved(matches.remove(0)),
            _ => {
                matches.sort();
                if self.using_columns.contains(&col_lower) {
                    // For USING columns, just pick the first one (they are equivalent).
                    ResolveResult::Resolved(matches.into_iter().next().unwrap_or_default())
                } else if matches.iter().any(|alias| alias == "<output>") {
                    ResolveResult::Resolved("<output>".to_owned())
                } else {
                    ResolveResult::Ambiguous(matches)
                }
            }
        }
    }

    #[must_use]
    fn resolve_schema_column(
        &self,
        schema: &Schema,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> ResolveResult {
        let qualified_name =
            QualifiedName::qualified(schema_name.to_owned(), table_name.to_owned());
        let lookup_key = table_lookup_key(&qualified_name);
        let table_lower = table_name.to_ascii_lowercase();
        let column_lower = column_name.to_ascii_lowercase();
        let mut table_matches = 0_usize;
        let mut column_matches = Vec::new();

        for binding in &self.qualified_bindings {
            if binding.lookup_key != lookup_key || binding.table_name != table_lower {
                continue;
            }
            table_matches = table_matches.saturating_add(1);
            let column_exists = binding
                .columns
                .as_ref()
                .is_none_or(|columns| columns.contains(&column_lower))
                || schema
                    .find_table_by_lookup_key(&binding.lookup_key)
                    .is_some_and(|table| table.is_rowid_alias(&column_lower));
            if column_exists {
                column_matches.push(format!("{schema_name}.{table_name}"));
            }
        }

        match column_matches.len() {
            1 => ResolveResult::Resolved(column_matches.remove(0)),
            count if count > 1 => ResolveResult::Ambiguous(column_matches),
            _ if table_matches > 0 => ResolveResult::ColumnNotFound,
            _ => {
                if let Some(parent) = &self.parent {
                    parent.resolve_schema_column(schema, schema_name, table_name, column_name)
                } else {
                    ResolveResult::TableNotFound
                }
            }
        }
    }

    /// Number of aliases registered in this scope (not counting parents).
    #[must_use]
    pub fn alias_count(&self) -> usize {
        self.bindings.len()
    }

    /// Return known column sets from all local sources (for NATURAL JOIN).
    /// Aliases with unknown columns (`None`) are omitted.
    #[must_use]
    pub fn known_local_column_sets(&self) -> Vec<&HashSet<String>> {
        self.bindings
            .iter()
            .filter_map(|binding| binding.output_columns.membership())
            .collect()
    }

    /// Return the column set for a specific alias (lowercased lookup).
    #[must_use]
    pub fn columns_for_alias(&self, alias: &str) -> Option<&HashSet<String>> {
        let alias = alias.to_ascii_lowercase();
        let mut matches = self
            .bindings
            .iter()
            .filter(|binding| binding.effective_name == alias);
        let binding = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        binding.output_columns.membership()
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
    /// SELECT * used without any tables in scope.
    NoTablesSpecifiedForStar,
    /// Type coercion warning (not fatal).
    ImplicitTypeCoercion {
        from: TypeAffinity,
        to: TypeAffinity,
        context: String,
    },
    /// A function argument fails a compile-time constraint (e.g. the
    /// probability argument to `likelihood()` must be a constant float literal
    /// in `[0.0, 1.0]`). Carries the fully-formed diagnostic message.
    InvalidFunctionArgument { message: String },
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
    /// Minimum number of arguments.
    VariadicMin(usize),
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
            Statement::Select(select) => {
                let _ = self.resolve_select(select, scope);
            }
            Statement::Insert(insert) => {
                // Process WITH clause CTEs if present.
                if let Some(ref with) = insert.with {
                    self.resolve_with_clause(with, scope);
                }

                // Resolve the data source (VALUES or SELECT).
                // The target table is NOT visible to the body.
                match &insert.source {
                    fsqlite_ast::InsertSource::Values(rows) => {
                        for row in rows {
                            for expr in row {
                                self.resolve_expr(expr, scope);
                            }
                        }
                    }
                    fsqlite_ast::InsertSource::Select(select) => {
                        let mut source_scope = scope.clone();
                        let _ = self.resolve_select(select, &mut source_scope);
                    }
                    fsqlite_ast::InsertSource::DefaultValues => {}
                }

                // Bind the target table so RETURNING or UPSERT can reference it.
                self.bind_table_to_scope(&insert.table, insert.alias.as_deref(), scope);

                // Scope strictly for target column checks
                let mut target_scope = Scope::root();
                if insert.table.schema.is_none() && scope.has_cte(&insert.table.name) {
                    target_scope.add_alias(&insert.table.name, &insert.table.name, None);
                } else if let Some(table_def) = self
                    .schema
                    .find_table_in_schema(insert.table.schema.as_deref(), &insert.table.name)
                {
                    let ordered_columns: Vec<String> = table_def
                        .columns
                        .iter()
                        .map(|c| c.name.to_ascii_lowercase())
                        .collect();
                    target_scope.add_table_binding(&insert.table, None, Some(ordered_columns));
                }

                for col in &insert.columns {
                    self.resolve_unqualified_column(col, &target_scope, false);
                }

                // Resolve UPSERT.
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
                        fsqlite_ast::UpsertAction::Update {
                            assignments,
                            where_clause,
                        } => {
                            let mut upsert_scope = Scope::child(scope.clone());
                            let alias_name = insert.alias.as_deref().unwrap_or(&insert.table.name);
                            let target_lookup_key = table_lookup_key(&insert.table);
                            if let Some(table_def) = self.schema.find_table_in_schema(
                                insert.table.schema.as_deref(),
                                &insert.table.name,
                            ) {
                                let ordered_columns: Vec<String> = table_def
                                    .columns
                                    .iter()
                                    .map(|c| c.name.to_ascii_lowercase())
                                    .collect();
                                let col_set: HashSet<String> =
                                    ordered_columns.iter().cloned().collect();
                                upsert_scope.add_qualified_only_alias(
                                    "excluded",
                                    &target_lookup_key,
                                    Some(col_set.clone()),
                                );
                                upsert_scope.add_table_binding(
                                    &insert.table,
                                    insert.alias.as_deref(),
                                    Some(ordered_columns),
                                );
                            } else {
                                upsert_scope.add_qualified_only_alias("excluded", "<pseudo>", None);
                                upsert_scope.add_alias(alias_name, "<pseudo>", None);
                            }

                            for assignment in assignments {
                                match &assignment.target {
                                    fsqlite_ast::AssignmentTarget::Column(col) => {
                                        self.resolve_unqualified_column(col, &target_scope, false);
                                    }
                                    fsqlite_ast::AssignmentTarget::ColumnList(cols) => {
                                        for col in cols {
                                            self.resolve_unqualified_column(
                                                col,
                                                &target_scope,
                                                false,
                                            );
                                        }
                                    }
                                }
                                self.resolve_expr(&assignment.value, &upsert_scope);
                            }
                            if let Some(w) = where_clause {
                                self.resolve_expr(w, &upsert_scope);
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
                // Process WITH clause CTEs if present.
                if let Some(ref with) = update.with {
                    self.resolve_with_clause(with, scope);
                }

                // LIMIT and OFFSET cannot reference target or FROM tables.
                let limit_scope = scope.clone();

                self.bind_table_to_scope(&update.table.name, update.table.alias.as_deref(), scope);

                // Scope strictly for target column checks
                let mut target_scope = Scope::root();
                self.bind_table_to_scope(
                    &update.table.name,
                    update.table.alias.as_deref(),
                    &mut target_scope,
                );

                // The RETURNING clause can ONLY see the target table (and outer scopes/CTEs).
                // It CANNOT see tables from the FROM clause.
                let returning_scope = scope.clone();

                for assignment in &update.assignments {
                    match &assignment.target {
                        fsqlite_ast::AssignmentTarget::Column(col) => {
                            self.resolve_unqualified_column(col, &target_scope, false);
                        }
                        fsqlite_ast::AssignmentTarget::ColumnList(cols) => {
                            for col in cols {
                                self.resolve_unqualified_column(col, &target_scope, false);
                            }
                        }
                    }
                }
                if let Some(from) = &update.from {
                    self.resolve_from(from, scope);
                }
                for assignment in &update.assignments {
                    self.resolve_expr(&assignment.value, scope);
                }
                if let Some(where_clause) = &update.where_clause {
                    self.resolve_expr(where_clause, scope);
                }
                for ret in &update.returning {
                    self.resolve_result_column(ret, &returning_scope);
                }
                for term in &update.order_by {
                    self.resolve_expr(&term.expr, scope);
                }
                if let Some(limit) = &update.limit {
                    self.resolve_expr(&limit.limit, &limit_scope);
                    if let Some(offset) = &limit.offset {
                        self.resolve_expr(offset, &limit_scope);
                    }
                }
            }
            Statement::Delete(delete) => {
                // Process WITH clause CTEs if present.
                if let Some(ref with) = delete.with {
                    self.resolve_with_clause(with, scope);
                }

                // LIMIT and OFFSET cannot reference the target table.
                let limit_scope = scope.clone();

                self.bind_table_to_scope(&delete.table.name, delete.table.alias.as_deref(), scope);
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
                    self.resolve_expr(&limit.limit, &limit_scope);
                    if let Some(offset) = &limit.offset {
                        self.resolve_expr(offset, &limit_scope);
                    }
                }
            }
            // DDL and control statements don't need name resolution.
            _ => {}
        }
    }

    fn select_core_output_columns(core: &SelectCore, scope: &Scope) -> Option<Vec<String>> {
        match core {
            SelectCore::Select { columns, .. } => {
                let mut output = Vec::new();
                for column in columns {
                    match column {
                        ResultColumn::Expr {
                            alias: Some(alias), ..
                        } => output.push(alias.to_ascii_lowercase()),
                        ResultColumn::Expr {
                            expr: Expr::Column(column, _),
                            ..
                        } => output.push(column.column.to_ascii_lowercase()),
                        // SQLite derives a result name from the original SQL
                        // text for an unaliased expression. The semantic layer
                        // does not retain that source spelling, so do not turn
                        // the expression into an "unknown columns" wildcard:
                        // that would make unrelated outer names spuriously
                        // resolve (or become ambiguous). Explicit aliases and
                        // direct column references above remain addressable.
                        ResultColumn::Expr { .. } => {}
                        ResultColumn::Star => {
                            for binding in &scope.bindings {
                                output.extend(binding.output_columns.ordered()?.iter().cloned());
                            }
                        }
                        ResultColumn::TableStar(name) => {
                            if scope.table_star_ambiguity(name).is_some() {
                                return None;
                            }
                            let target = name.name.to_ascii_lowercase();
                            let mut found = false;
                            for binding in scope
                                .bindings
                                .iter()
                                .filter(|binding| binding.effective_name == target)
                            {
                                found = true;
                                output.extend(binding.output_columns.ordered()?.iter().cloned());
                            }
                            if !found {
                                return None;
                            }
                        }
                    }
                }
                Some(output)
            }
            SelectCore::Values(rows) => rows.first().map(|row| {
                (1..=row.len())
                    .map(|index| format!("column{index}"))
                    .collect()
            }),
        }
    }

    fn cte_output_columns(cte: &Cte, inferred: Option<Vec<String>>) -> Option<Vec<String>> {
        let columns = if cte.columns.is_empty() {
            inferred?
        } else {
            cte.columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect()
        };
        Some(Self::canonicalize_derived_output_columns(columns))
    }

    fn canonicalize_derived_output_columns(columns: Vec<String>) -> Vec<String> {
        let mut used = HashSet::with_capacity(columns.len());
        let mut canonical = Vec::with_capacity(columns.len());

        for column in columns {
            if used.insert(column.to_ascii_lowercase()) {
                canonical.push(column);
                continue;
            }

            let base = column
                .rsplit_once(':')
                .map_or(column.as_str(), |(base, suffix)| {
                    if !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit()) {
                        base
                    } else {
                        column.as_str()
                    }
                });
            let candidate_limit = used.len().saturating_add(1);
            let renamed = (1..=candidate_limit).find_map(|suffix| {
                let candidate = format!("{base}:{suffix}");
                used.insert(candidate.to_ascii_lowercase())
                    .then_some(candidate)
            });

            if let Some(renamed) = renamed {
                canonical.push(renamed);
            } else {
                debug_assert!(false, "a free derived-column suffix must exist");
            }
        }

        canonical
    }

    fn table_function_output_columns(name: &str) -> Option<Vec<String>> {
        if name.eq_ignore_ascii_case("json_each") || name.eq_ignore_ascii_case("json_tree") {
            Some(
                [
                    "key", "value", "type", "atom", "id", "parent", "fullkey", "path",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            )
        } else {
            None
        }
    }

    fn resolve_with_clause(&mut self, with: &WithClause, scope: &mut Scope) {
        if with.recursive {
            // In WITH RECURSIVE, all CTE names are visible to all CTE bodies.
            for cte in &with.ctes {
                let declared = Self::cte_output_columns(cte, None);
                scope.add_cte_with_columns(&cte.name, declared);
            }
            for cte in &with.ctes {
                let mut cte_scope = scope.clone();
                let inferred = self.resolve_select(&cte.query, &mut cte_scope);
                let columns = Self::cte_output_columns(cte, inferred);
                scope.add_cte_with_columns(&cte.name, columns);
            }
        } else {
            // In plain WITH, a CTE body can only see previously defined CTEs.
            for cte in &with.ctes {
                let mut cte_scope = scope.clone();
                let inferred = self.resolve_select(&cte.query, &mut cte_scope);
                // Add *after* resolving the query so it can't see itself or subsequent CTEs.
                let columns = Self::cte_output_columns(cte, inferred);
                scope.add_cte_with_columns(&cte.name, columns);
            }
        }
    }

    // SQLite compound SELECTs allow ORDER BY terms to reuse a projected
    // expression verbatim, even though underlying table aliases are no longer
    // in scope at the compound boundary.
    fn compound_order_by_matches_output_expr(select: &SelectStatement, order_expr: &Expr) -> bool {
        if select.body.compounds.is_empty() {
            return false;
        }

        std::iter::once(&select.body.select)
            .chain(select.body.compounds.iter().map(|(_, core)| core))
            .filter_map(|core| match core {
                SelectCore::Select { columns, .. } => Some(columns.iter()),
                _ => None,
            })
            .flatten()
            .any(|column| match column {
                ResultColumn::Expr { expr, .. } => expr == order_expr,
                _ => false,
            })
    }

    fn resolve_select(
        &mut self,
        select: &SelectStatement,
        scope: &mut Scope,
    ) -> Option<Vec<String>> {
        // Register CTEs if present.
        if let Some(ref with) = select.with {
            self.resolve_with_clause(with, scope);
        }

        // Resolve the primary select core in an isolated scope.
        let mut first_core_scope = scope.clone();
        self.resolve_select_core(&select.body.select, &mut first_core_scope);
        let output_columns =
            Self::select_core_output_columns(&select.body.select, &first_core_scope)
                .map(Self::canonicalize_derived_output_columns);

        // Resolve any compound queries (UNION, INTERSECT, EXCEPT) in isolated scopes.
        for (_op, core) in &select.body.compounds {
            let mut comp_scope = scope.clone();
            self.resolve_select_core(core, &mut comp_scope);
        }

        // Resolve ORDER BY against the appropriate scope.
        let mut order_by_scope = if select.body.compounds.is_empty() {
            first_core_scope.clone()
        } else {
            scope.clone() // Compounds can only see outer scope + result columns
        };

        let mut output_cols = HashSet::new();
        for core in std::iter::once(&select.body.select)
            .chain(select.body.compounds.iter().map(|(_, core)| core))
        {
            if let SelectCore::Select { columns, .. } = core {
                for col in columns {
                    match col {
                        ResultColumn::Expr {
                            alias: Some(alias_id),
                            ..
                        } => {
                            output_cols.insert(alias_id.to_ascii_lowercase());
                        }
                        ResultColumn::Expr {
                            expr: Expr::Column(col_ref, _),
                            ..
                        } => {
                            output_cols.insert(col_ref.column.to_ascii_lowercase());
                        }
                        _ => {}
                    }
                }
            }
        }
        if !output_cols.is_empty() {
            // Add the output columns as a pseudo-table so ORDER BY can reference them.
            order_by_scope.add_alias("<output>", "<output>", Some(output_cols));
        }

        for term in &select.order_by {
            if Self::compound_order_by_matches_output_expr(select, &term.expr) {
                continue;
            }
            self.resolve_expr(&term.expr, &order_by_scope);
        }

        // Resolve LIMIT against the base scope (no FROM aliases).
        if let Some(limit) = &select.limit {
            self.resolve_expr(&limit.limit, scope);
            if let Some(offset) = &limit.offset {
                self.resolve_expr(offset, scope);
            }
        }

        output_columns
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

                // Create a scope for GROUP BY, HAVING, and WINDOW that includes output columns.
                let mut post_select_scope = scope.clone();
                let mut output_cols = HashSet::new();
                for col in columns {
                    if let ResultColumn::Expr {
                        alias: Some(alias_id),
                        ..
                    } = col
                    {
                        output_cols.insert(alias_id.to_ascii_lowercase());
                    } else if let ResultColumn::Expr {
                        expr: Expr::Column(col_ref, _),
                        ..
                    } = col
                    {
                        output_cols.insert(col_ref.column.to_ascii_lowercase());
                    }
                }
                if !output_cols.is_empty() {
                    post_select_scope.add_alias("<output>", "<output>", Some(output_cols));
                } else {
                    post_select_scope.add_alias("<output>", "<output>", None);
                }

                for expr in group_by {
                    self.resolve_expr(expr, &post_select_scope);
                }
                if let Some(having) = having {
                    self.resolve_expr(having, &post_select_scope);
                }
                for window in windows {
                    for part in &window.spec.partition_by {
                        self.resolve_expr(part, &post_select_scope);
                    }
                    for order in &window.spec.order_by {
                        self.resolve_expr(&order.expr, &post_select_scope);
                    }
                }
            }
            SelectCore::Values(rows) => {
                for row in rows {
                    for expr in row {
                        self.resolve_expr(expr, scope);
                    }
                }
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

                // Resolve table name against schema or CTEs.
                if name.schema.is_none() && scope.has_cte(table_name) {
                    let columns = scope.cte_columns(table_name).flatten();
                    scope.add_alias_with_metadata(
                        alias_name,
                        &format!("*\0{table_name}"),
                        StarNamespace::Derived,
                        columns,
                        false,
                    );
                    self.tables_resolved += 1;
                } else if let Some(table_def) = self
                    .schema
                    .find_table_in_schema(name.schema.as_deref(), table_name)
                {
                    let ordered_columns: Vec<String> = table_def
                        .columns
                        .iter()
                        .map(|c| c.name.to_ascii_lowercase())
                        .collect();
                    scope.add_table_binding(name, alias.as_deref(), Some(ordered_columns));
                    self.tables_resolved += 1;
                } else {
                    self.push_error(SemanticErrorKind::UnresolvedTable {
                        name: name.to_string(),
                    });
                }
            }
            TableOrSubquery::Subquery { query, alias, .. } => {
                // Resolve subquery in a child scope.
                let mut child = Scope::child(scope.clone());
                let output_columns = self.resolve_select(query, &mut child);

                let alias_name = if let Some(a) = alias {
                    a.clone()
                } else {
                    format!("<subquery_{}>", self.tables_resolved)
                };

                scope.add_alias_with_metadata(
                    &alias_name,
                    "*\0<subquery>",
                    StarNamespace::Derived,
                    output_columns,
                    false,
                );

                self.tables_resolved += 1;
            }
            TableOrSubquery::TableFunction {
                name, args, alias, ..
            } => {
                for arg in args {
                    self.resolve_expr(arg, scope);
                }

                let alias_name = alias.as_deref().unwrap_or(name);
                scope.add_alias_with_metadata(
                    alias_name,
                    name,
                    StarNamespace::Database("main".to_owned()),
                    Self::table_function_output_columns(name),
                    false,
                );
                self.tables_resolved += 1;
            }
            TableOrSubquery::ParenJoin(inner_from) => {
                self.resolve_from(inner_from, scope);
            }
        }
    }

    fn resolve_join(&mut self, join: &JoinClause, scope: &mut Scope) {
        // Snapshot column names from existing aliases BEFORE adding the new
        // table, so we can compute shared columns for NATURAL JOIN and USING.
        let pre_join_binding_count = scope.bindings.len();
        let pre_join_columns: Vec<HashSet<String>> = scope
            .known_local_column_sets()
            .into_iter()
            .cloned()
            .collect();

        self.resolve_table_or_subquery(&join.table, scope);

        if join.join_type.natural && join.constraint.is_none() {
            // NATURAL JOIN: implicitly equate all columns with matching names
            // between the pre-existing tables and the newly joined table(s).
            let mut to_insert = Vec::new();
            for binding in &scope.bindings[pre_join_binding_count..] {
                if let Some(new_cols) = binding.output_columns.membership() {
                    for col_name in new_cols {
                        if pre_join_columns.iter().any(|cs| cs.contains(col_name)) {
                            to_insert.push(col_name.clone());
                        }
                    }
                }
            }
            for col_name in to_insert {
                scope.using_columns.insert(col_name);
            }
        }

        if let Some(ref constraint) = join.constraint {
            match constraint {
                JoinConstraint::On(expr) => self.resolve_expr(expr, scope),
                JoinConstraint::Using(cols) => {
                    for col in cols {
                        let col_lower = col.to_ascii_lowercase();
                        scope.using_columns.insert(col_lower.clone());

                        // Validate that column exists on the left side
                        let in_left = pre_join_columns.iter().any(|cs| cs.contains(&col_lower));
                        // Validate that column exists on the right side
                        let mut in_right = false;
                        for binding in &scope.bindings[pre_join_binding_count..] {
                            if let Some(new_cols) = binding.output_columns.membership() {
                                if new_cols.contains(&col_lower) {
                                    in_right = true;
                                    break;
                                }
                            } else {
                                // If right side columns are unknown (e.g. subquery), assume it exists
                                in_right = true;
                                break;
                            }
                        }

                        // If left side has unknown columns, we might not find it in `pre_join_columns`
                        let left_has_unknown = scope.bindings[..pre_join_binding_count]
                            .iter()
                            .any(|binding| binding.output_columns.membership().is_none());

                        if (!in_left && !left_has_unknown) || !in_right {
                            self.push_error(SemanticErrorKind::UnresolvedColumn {
                                table: None,
                                column: col.clone(),
                            });
                        }

                        self.resolve_unqualified_column(col, scope, true);
                    }
                }
            }
        }
    }

    fn resolve_result_column(&mut self, col: &ResultColumn, scope: &Scope) {
        match col {
            ResultColumn::Star => {
                // SELECT * is valid if there's at least one table in scope.
                // Suppress this error if we already reported an UnresolvedTable
                // error — the missing star target is a cascading consequence.
                if scope.alias_count() == 0
                    && !self
                        .errors
                        .iter()
                        .any(|e| matches!(e.kind, SemanticErrorKind::UnresolvedTable { .. }))
                {
                    self.push_error(SemanticErrorKind::NoTablesSpecifiedForStar);
                }
            }
            ResultColumn::TableStar(table_name) => {
                if let Some((column, candidates)) = scope.table_star_ambiguity(table_name) {
                    self.push_error(SemanticErrorKind::AmbiguousColumn { column, candidates });
                } else if let Some(source_columns) = scope.table_star_source_columns(table_name) {
                    // SQLite expands every matching unaliased source in FROM
                    // order. Count each known source's columns independently;
                    // do not collapse equal effective names.
                    for columns in source_columns.into_iter().flatten() {
                        self.columns_bound = self
                            .columns_bound
                            .saturating_add(u64::try_from(columns.len()).unwrap_or(u64::MAX));
                    }
                } else {
                    self.push_error(SemanticErrorKind::UnresolvedTable {
                        name: table_name.to_string(),
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
                        let _ = self.resolve_select(select, &mut child);
                    }
                    InSet::Table(name) => self.resolve_table_name(name, scope),
                }
            }
            Expr::Like {
                expr: inner,
                pattern,
                escape,
                op,
                ..
            } => {
                self.resolve_expr(inner, scope);
                self.resolve_expr(pattern, scope);
                if let Some(esc) = escape {
                    if *op != fsqlite_ast::LikeOp::Like {
                        // SQLite only supports ESCAPE with LIKE. For GLOB, MATCH, REGEXP it throws "wrong number of arguments to function X()"
                        self.push_error(SemanticErrorKind::FunctionArityMismatch {
                            function: match op {
                                fsqlite_ast::LikeOp::Like => "LIKE",
                                fsqlite_ast::LikeOp::Glob => "GLOB",
                                fsqlite_ast::LikeOp::Match => "MATCH",
                                fsqlite_ast::LikeOp::Regexp => "REGEXP",
                            }
                            .to_owned(),
                            expected: FunctionArity::Exact(2),
                            actual: 3,
                        });
                    }
                    self.resolve_expr(esc, scope);
                }
            }
            Expr::Subquery(select, _)
            | Expr::Exists {
                subquery: select, ..
            } => {
                let mut child = Scope::child(scope.clone());
                let _ = self.resolve_select(select, &mut child);
            }
            Expr::FunctionCall {
                name,
                args,
                filter,
                over,
                ..
            } => {
                self.resolve_function(name, args, scope);
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
                        if let Some(
                            fsqlite_ast::FrameBound::Preceding(expr)
                            | fsqlite_ast::FrameBound::Following(expr),
                        ) = &frame.end
                        {
                            self.resolve_expr(expr, scope);
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
        let result = match (&col_ref.schema, &col_ref.table) {
            (Some(schema_name), Some(table_name)) => {
                scope.resolve_schema_column(self.schema, schema_name, table_name, &col_ref.column)
            }
            (None, table_name) => {
                scope.resolve_column(self.schema, table_name.as_deref(), &col_ref.column)
            }
            (Some(_), None) => ResolveResult::TableNotFound,
        };
        let display_qualifier = match (&col_ref.schema, &col_ref.table) {
            (Some(schema_name), Some(table_name)) => Some(format!("{schema_name}.{table_name}")),
            (_, table_name) => table_name.as_ref().map(ToString::to_string),
        };
        match result {
            ResolveResult::Resolved(_) => {
                self.columns_bound += 1;
            }
            ResolveResult::TableNotFound => {
                tracing::error!(
                    target: "fsqlite.parse",
                    schema = ?col_ref.schema,
                    table = ?col_ref.table,
                    column = %col_ref.column,
                    "unresolvable table reference"
                );
                self.push_error(SemanticErrorKind::UnresolvedColumn {
                    table: display_qualifier.clone(),
                    column: col_ref.column.to_string(),
                });
            }
            ResolveResult::ColumnNotFound => {
                tracing::error!(
                    target: "fsqlite.parse",
                    schema = ?col_ref.schema,
                    table = ?col_ref.table,
                    column = %col_ref.column,
                    "unresolvable column reference"
                );
                self.push_error(SemanticErrorKind::UnresolvedColumn {
                    table: display_qualifier,
                    column: col_ref.column.to_string(),
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
                    column: col_ref.column.to_string(),
                    candidates,
                });
            }
        }
    }

    fn resolve_unqualified_column(&mut self, name: &str, scope: &Scope, is_using_clause: bool) {
        let result = scope.resolve_column(self.schema, None, name);
        match result {
            ResolveResult::Resolved(_) => {
                self.columns_bound += 1;
            }
            ResolveResult::Ambiguous(candidates) => {
                if is_using_clause {
                    self.columns_bound += 1;
                } else {
                    self.push_error(SemanticErrorKind::AmbiguousColumn {
                        column: name.to_owned(),
                        candidates,
                    });
                }
            }
            ResolveResult::ColumnNotFound | ResolveResult::TableNotFound => {
                self.push_error(SemanticErrorKind::UnresolvedColumn {
                    table: None,
                    column: name.to_owned(),
                });
            }
        }
    }

    fn bind_table_to_scope(
        &mut self,
        name: &QualifiedName,
        alias: Option<&str>,
        scope: &mut Scope,
    ) {
        let alias_name = alias.unwrap_or(&name.name);
        if name.schema.is_none() && scope.has_cte(&name.name) {
            let columns = scope.cte_columns(&name.name).flatten();
            scope.add_alias_with_metadata(
                alias_name,
                &format!("*\0{}", name.name),
                StarNamespace::Derived,
                columns,
                false,
            );
            self.tables_resolved += 1;
        } else if let Some(table_def) = self
            .schema
            .find_table_in_schema(name.schema.as_deref(), &name.name)
        {
            let ordered_columns: Vec<String> = table_def
                .columns
                .iter()
                .map(|c| c.name.to_ascii_lowercase())
                .collect();
            scope.add_table_binding(name, alias, Some(ordered_columns));
            self.tables_resolved += 1;
        } else {
            self.push_error(SemanticErrorKind::UnresolvedTable {
                name: name.to_string(),
            });
        }
    }

    fn resolve_table_name(&mut self, name: &QualifiedName, _scope: &Scope) {
        if self
            .schema
            .find_table_in_schema(name.schema.as_deref(), &name.name)
            .is_some()
        {
            self.tables_resolved += 1;
        } else {
            self.push_error(SemanticErrorKind::UnresolvedTable {
                name: name.to_string(),
            });
        }
    }

    fn resolve_function(&mut self, name: &str, args: &FunctionArgs, scope: &Scope) {
        // Resolve argument expressions.
        let actual = match args {
            FunctionArgs::Star => {
                if !name.eq_ignore_ascii_case("count") {
                    let expected = known_function_arity(name).unwrap_or(FunctionArity::Range(0, 1));
                    self.push_error(SemanticErrorKind::FunctionArityMismatch {
                        function: name.to_owned(),
                        expected,
                        actual: 1,
                    });
                }
                1 // `*` counts as 1 argument for arity purposes (e.g. count(*))
            }
            FunctionArgs::List(list) => {
                for arg in list {
                    self.resolve_expr(arg, scope);
                }
                list.len()
            }
        };

        // Validate known function arity.
        if let Some(expected) = known_function_arity(name) {
            let valid = match &expected {
                FunctionArity::Exact(n) => actual == *n,
                FunctionArity::Range(lo, hi) => actual >= *lo && actual <= *hi,
                FunctionArity::Variadic => true,
                FunctionArity::VariadicMin(min) => actual >= *min,
            };
            if !valid {
                self.push_error(SemanticErrorKind::FunctionArityMismatch {
                    function: name.to_owned(),
                    expected,
                    actual,
                });
            }
        }

        // likelihood(X, prob): the probability must be a constant floating-point
        // literal in [0.0, 1.0], matching C SQLite's exprProbability() contract.
        // Integer literals, out-of-range values, and non-literal expressions are
        // all rejected at prepare time.
        if name.eq_ignore_ascii_case("likelihood") {
            if let FunctionArgs::List(list) = args {
                if list.len() == 2 {
                    let is_valid_probability = matches!(
                        &list[1],
                        Expr::Literal(Literal::Float(p), _) if (0.0..=1.0).contains(p)
                    );
                    if !is_valid_probability {
                        self.push_error(SemanticErrorKind::InvalidFunctionArgument {
                            message:
                                "second argument to likelihood() must be a constant between 0.0 and 1.0"
                                    .to_owned(),
                        });
                    }
                }
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
            SemanticErrorKind::NoTablesSpecifiedForStar => "no tables specified".to_string(),
            SemanticErrorKind::ImplicitTypeCoercion {
                from, to, context, ..
            } => {
                format!("implicit type coercion from {from:?} to {to:?} in {context}")
            }
            SemanticErrorKind::InvalidFunctionArgument { message } => message.clone(),
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
        | "unicode" | "quote" | "zeroblob" | "soundex" | "likely" | "unlikely" | "randomblob" => {
            Some(FunctionArity::Exact(1))
        }
        "ifnull" | "nullif" | "instr" | "glob" | "likelihood" => Some(FunctionArity::Exact(2)),
        "replace" => Some(FunctionArity::Exact(3)),
        "count" => Some(FunctionArity::Range(0, 1)),
        "group_concat" | "trim" | "ltrim" | "rtrim" | "round" => Some(FunctionArity::Range(1, 2)),
        // iif/if accept the 2-argument shorthand iif(X,Y) as of SQLite 3.48;
        // `if` is iif's registered alias.
        "substr" | "substring" | "like" | "iif" | "if" => Some(FunctionArity::Range(2, 3)),
        "coalesce" | "json_extract" => Some(FunctionArity::VariadicMin(2)),
        "json_remove" => Some(FunctionArity::VariadicMin(1)),
        "json_insert" | "json_replace" | "json_set" => Some(FunctionArity::VariadicMin(3)),
        // Variadic: aggregates, scalars, date/time, and JSON functions
        "min" | "max" | "printf" | "format" | "strftime" | "json" | "json_type" | "json_valid" => {
            Some(FunctionArity::VariadicMin(1))
        }
        "date" | "time" | "datetime" | "julianday" | "unixepoch" => {
            Some(FunctionArity::VariadicMin(0))
        }
        "char" | "json_array" | "json_object" => Some(FunctionArity::Variadic),

        _ => None, // Unknown function — skip arity check.
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "semantic_test.rs"]
mod semantic_test;

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
    fn test_schema_find_table_in_named_namespace() {
        let mut schema = make_schema();
        schema.add_table_in_schema(
            "aux",
            TableDef {
                name: "users".to_owned(),
                columns: vec![ColumnDef {
                    name: "nickname".to_owned(),
                    affinity: TypeAffinity::Text,
                    is_ipk: false,
                    not_null: false,
                }],
                without_rowid: false,
                strict: false,
            },
        );

        assert!(schema.find_table_in_schema(Some("main"), "users").is_some());
        assert!(schema.find_table_in_schema(Some("aux"), "users").is_some());
        assert!(schema.find_table_in_schema(Some("AUX"), "USERS").is_some());
        assert!(
            schema
                .find_table_in_schema(Some("missing"), "users")
                .is_none()
        );
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

    #[test]
    fn test_table_rowid_alias_respects_shadowing() {
        let mut schema = Schema::new();
        schema.add_table(TableDef {
            name: "shadowed".to_owned(),
            columns: vec![
                ColumnDef {
                    name: "rowid".to_owned(),
                    affinity: TypeAffinity::Text,
                    is_ipk: false,
                    not_null: false,
                },
                ColumnDef {
                    name: "_rowid_".to_owned(),
                    affinity: TypeAffinity::Text,
                    is_ipk: false,
                    not_null: false,
                },
                ColumnDef {
                    name: "id".to_owned(),
                    affinity: TypeAffinity::Integer,
                    is_ipk: true,
                    not_null: false,
                },
            ],
            without_rowid: false,
            strict: false,
        });

        let shadowed = schema.find_table("shadowed").unwrap();
        assert!(!shadowed.is_rowid_alias("rowid"));
        assert!(!shadowed.is_rowid_alias("_rowid_"));
        assert!(shadowed.is_rowid_alias("oid"));
        assert!(shadowed.is_rowid_alias("id"));
    }

    #[test]
    fn test_table_rowid_alias_disabled_for_without_rowid_tables() {
        let mut schema = Schema::new();
        schema.add_table(TableDef {
            name: "wr".to_owned(),
            columns: vec![
                ColumnDef {
                    name: "id".to_owned(),
                    affinity: TypeAffinity::Integer,
                    is_ipk: true,
                    not_null: true,
                },
                ColumnDef {
                    name: "payload".to_owned(),
                    affinity: TypeAffinity::Text,
                    is_ipk: false,
                    not_null: false,
                },
            ],
            without_rowid: true,
            strict: false,
        });

        let wr = schema.find_table("wr").unwrap();
        assert!(!wr.is_rowid_alias("rowid"));
        assert!(!wr.is_rowid_alias("_rowid_"));
        assert!(!wr.is_rowid_alias("oid"));
        assert!(!wr.is_rowid_alias("id"));
        assert!(wr.has_column("id"));
    }

    // ── Scope tests ──

    #[test]
    fn test_scope_resolve_qualified_column() {
        let mut scope = Scope::root();
        let schema = make_schema();
        let cols: HashSet<String> = ["id", "name", "email"]
            .iter()
            .map(ToString::to_string)
            .collect();
        scope.add_alias("u", "users", Some(cols));

        assert_eq!(
            scope.resolve_column(&schema, Some("u"), "id"),
            ResolveResult::Resolved("u".to_string())
        );
        assert_eq!(
            scope.resolve_column(&schema, Some("u"), "nonexistent"),
            ResolveResult::ColumnNotFound
        );
        assert_eq!(
            scope.resolve_column(&schema, Some("x"), "id"),
            ResolveResult::TableNotFound
        );
    }

    #[test]
    fn test_scope_resolve_unqualified_column() {
        let mut scope = Scope::root();
        let schema = make_schema();
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
            scope.resolve_column(&schema, None, "name"),
            ResolveResult::Resolved("u".to_string())
        );

        // "user_id" is unique → resolved to "o"
        assert_eq!(
            scope.resolve_column(&schema, None, "user_id"),
            ResolveResult::Resolved("o".to_string())
        );

        // "id" is ambiguous
        match scope.resolve_column(&schema, None, "id") {
            ResolveResult::Ambiguous(candidates) => {
                assert_eq!(candidates.len(), 2);
            }
            other => panic!("expected Ambiguous, got {other:?}"),
        }

        // "nonexistent" not found
        assert_eq!(
            scope.resolve_column(&schema, None, "nonexistent"),
            ResolveResult::ColumnNotFound
        );
    }

    #[test]
    fn test_scope_child_inherits_parent() {
        let mut parent = Scope::root();
        let schema = make_schema();
        parent.add_alias(
            "u",
            "users",
            Some(["id", "name"].iter().map(ToString::to_string).collect()),
        );
        let child = Scope::child(parent);

        // Child can see parent's columns.
        assert_eq!(
            child.resolve_column(&schema, Some("u"), "id"),
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
    fn test_resolve_select_from_named_namespace() {
        let mut schema = make_schema();
        schema.add_table_in_schema(
            "aux",
            TableDef {
                name: "users".to_owned(),
                columns: vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        affinity: TypeAffinity::Integer,
                        is_ipk: true,
                        not_null: true,
                    },
                    ColumnDef {
                        name: "nickname".to_owned(),
                        affinity: TypeAffinity::Text,
                        is_ipk: false,
                        not_null: false,
                    },
                ],
                without_rowid: false,
                strict: false,
            },
        );

        let stmt = parse_one("SELECT nickname FROM aux.users");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(resolver.tables_resolved, 1);
        assert_eq!(resolver.columns_bound, 1);
    }

    #[test]
    fn test_resolve_schema_qualified_column_and_alias_hiding() {
        let schema = make_schema();

        let stmt = parse_one("SELECT main.users.name FROM users");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(resolver.columns_bound, 1);

        let stmt = parse_one("SELECT main.users.name FROM users AS u");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(
            errors.iter().any(|error| matches!(
                &error.kind,
                SemanticErrorKind::UnresolvedColumn {
                    table: Some(table),
                    column,
                } if table == "main.users" && column == "name"
            )),
            "an alias must hide the original schema-qualified identity: {errors:?}"
        );
    }

    #[test]
    fn test_resolve_same_table_name_across_main_and_temp_namespaces() {
        let mut schema = make_schema();
        schema.add_table_in_schema(
            "temp",
            TableDef {
                name: "users".to_owned(),
                columns: vec![ColumnDef {
                    name: "nickname".to_owned(),
                    affinity: TypeAffinity::Text,
                    is_ipk: false,
                    not_null: false,
                }],
                without_rowid: false,
                strict: false,
            },
        );

        let stmt = parse_one(
            "SELECT main.users.name, temp.users.nickname, \
                    users.name, users.nickname, name, nickname \
             FROM main.users JOIN temp.users",
        );
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(resolver.columns_bound, 6);
    }

    #[test]
    fn test_table_star_expands_every_same_effective_name_source_in_from_order() {
        let mut schema = make_schema();
        schema.add_table_in_schema(
            "temp",
            TableDef {
                name: "users".to_owned(),
                columns: vec![ColumnDef {
                    name: "nickname".to_owned(),
                    affinity: TypeAffinity::Text,
                    is_ipk: false,
                    not_null: false,
                }],
                without_rowid: false,
                strict: false,
            },
        );

        let stmt = parse_one("SELECT users.* FROM main.users JOIN temp.users");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);

        assert!(
            errors.is_empty(),
            "unexpected table-star errors: {errors:?}"
        );
        assert_eq!(
            resolver.columns_bound, 4,
            "main.users' three columns and temp.users' one column must both expand"
        );
    }

    #[test]
    fn test_same_effective_table_name_is_ambiguous_only_for_overlapping_column() {
        let mut schema = make_schema();
        schema.add_table_in_schema(
            "temp",
            TableDef {
                name: "users".to_owned(),
                columns: vec![ColumnDef {
                    name: "name".to_owned(),
                    affinity: TypeAffinity::Text,
                    is_ipk: false,
                    not_null: false,
                }],
                without_rowid: false,
                strict: false,
            },
        );

        let stmt = parse_one(
            "SELECT users.name, name \
             FROM main.users JOIN temp.users",
        );
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert_eq!(
            errors.len(),
            2,
            "expected one qualified and one bare ambiguity: {errors:?}"
        );
        assert!(
            errors
                .iter()
                .all(|error| matches!(&error.kind, SemanticErrorKind::AmbiguousColumn { .. })),
            "both qualified and bare overlapping columns must be ambiguous: {errors:?}"
        );
    }

    #[test]
    fn test_duplicate_explicit_aliases_resolve_each_source_by_matching_column() {
        let mut schema = Schema::new();
        for (name, distinct_column) in [("left_source", "x"), ("right_source", "y")] {
            schema.add_table(TableDef {
                name: name.to_owned(),
                columns: [distinct_column, "z"]
                    .into_iter()
                    .map(|column| ColumnDef {
                        name: column.to_owned(),
                        affinity: TypeAffinity::Integer,
                        is_ipk: false,
                        not_null: false,
                    })
                    .collect(),
                without_rowid: false,
                strict: false,
            });
        }

        let disjoint = parse_one(
            "SELECT q.x, q.y \
             FROM left_source AS q JOIN right_source AS q",
        );
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&disjoint);
        assert!(
            errors.is_empty(),
            "duplicate aliases with disjoint columns must resolve: {errors:?}"
        );
        assert_eq!(resolver.columns_bound, 2);

        let star = parse_one(
            "SELECT q.* \
             FROM left_source AS q JOIN right_source AS q",
        );
        let errors = resolver.resolve_statement(&star);
        assert!(
            matches!(
                errors.as_slice(),
                [SemanticError {
                    kind: SemanticErrorKind::AmbiguousColumn {
                        column,
                        candidates,
                    },
                    ..
                }] if column == "z"
                    && candidates.iter().map(String::as_str).eq(["q", "q"])
            ),
            "same-schema q.* must reject the overlapping column: {errors:?}"
        );

        let Statement::Select(select) = &star else {
            panic!("expected SELECT statement");
        };
        let SelectCore::Select {
            from: Some(from), ..
        } = &select.body.select
        else {
            panic!("expected SELECT core with FROM");
        };
        let mut star_scope = Scope::root();
        let mut star_resolver = Resolver::new(&schema);
        star_resolver.resolve_from(from, &mut star_scope);
        let source_columns = star_scope
            .table_star_source_columns(&QualifiedName::bare("q"))
            .expect("q must match both local FROM sources");
        assert_eq!(source_columns.len(), 2);
        assert!(
            source_columns[0]
                .is_some_and(|columns| columns.contains("x") && !columns.contains("y")),
            "left_source must be the first q.* expansion"
        );
        assert!(
            source_columns[1]
                .is_some_and(|columns| columns.contains("y") && !columns.contains("x")),
            "right_source must be the second q.* expansion"
        );

        let overlapping = parse_one(
            "SELECT q.z \
             FROM left_source AS q JOIN right_source AS q",
        );
        let errors = resolver.resolve_statement(&overlapping);
        assert_eq!(
            errors.len(),
            1,
            "only the overlapping column must be ambiguous: {errors:?}"
        );
        assert!(
            matches!(
                &errors[0].kind,
                SemanticErrorKind::AmbiguousColumn {
                    column,
                    candidates,
                } if column == "z"
                    && candidates.iter().map(String::as_str).eq(["q", "q"])
            ),
            "duplicate aliases must retain one ambiguity candidate per source: {errors:?}"
        );
    }

    #[test]
    fn test_duplicate_alias_table_star_expands_overlaps_across_schemas() {
        let mut schema = Schema::new();
        schema.add_table_in_schema(
            "main",
            TableDef {
                name: "left_source".to_owned(),
                columns: ["x", "z"]
                    .into_iter()
                    .map(|column| ColumnDef {
                        name: column.to_owned(),
                        affinity: TypeAffinity::Integer,
                        is_ipk: false,
                        not_null: false,
                    })
                    .collect(),
                without_rowid: false,
                strict: false,
            },
        );
        schema.add_table_in_schema(
            "temp",
            TableDef {
                name: "right_source".to_owned(),
                columns: ["y", "z"]
                    .into_iter()
                    .map(|column| ColumnDef {
                        name: column.to_owned(),
                        affinity: TypeAffinity::Integer,
                        is_ipk: false,
                        not_null: false,
                    })
                    .collect(),
                without_rowid: false,
                strict: false,
            },
        );

        let star = parse_one(
            "SELECT q.* \
             FROM main.left_source AS q JOIN temp.right_source AS q",
        );
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&star);
        assert!(
            errors.is_empty(),
            "cross-schema duplicate aliases must both expand: {errors:?}"
        );
        assert_eq!(resolver.columns_bound, 4);

        let overlapping = parse_one(
            "SELECT q.z \
             FROM main.left_source AS q JOIN temp.right_source AS q",
        );
        let errors = resolver.resolve_statement(&overlapping);
        assert!(
            matches!(
                errors.as_slice(),
                [SemanticError {
                    kind: SemanticErrorKind::AmbiguousColumn { column, .. },
                    ..
                }] if column == "z"
            ),
            "ordinary q.z must remain ambiguous across schemas: {errors:?}"
        );
    }

    #[test]
    fn test_duplicate_alias_table_star_uses_sqlite_source_namespaces() {
        let column = |name: &str| ColumnDef {
            name: name.to_owned(),
            affinity: TypeAffinity::Integer,
            is_ipk: false,
            not_null: false,
        };
        let mut schema = Schema::new();
        schema.add_table(TableDef {
            name: "main_source".to_owned(),
            columns: ["x", "z"].into_iter().map(column).collect(),
            without_rowid: false,
            strict: false,
        });
        schema.add_table(TableDef {
            name: "json_source".to_owned(),
            columns: ["key", "other"].into_iter().map(column).collect(),
            without_rowid: false,
            strict: false,
        });

        for sql in [
            "SELECT q.* FROM main_source AS q \
             JOIN (SELECT 1 AS y, 2 AS z) AS q",
            "WITH c(y, z) AS (VALUES (1, 2)) \
             SELECT q.* FROM main_source AS q JOIN c AS q",
            "SELECT q.* FROM json_each('[]') AS q \
             JOIN (SELECT 1 AS key) AS q",
        ] {
            let mut resolver = Resolver::new(&schema);
            let errors = resolver.resolve_statement(&parse_one(sql));
            assert!(
                errors.is_empty(),
                "different SQLite source namespaces must both expand for `{sql}`: {errors:?}"
            );
        }

        for (sql, expected_column) in [
            (
                "SELECT q.* FROM (SELECT 1 AS x, 2 AS z) AS q \
                 JOIN (SELECT 3 AS y, 4 AS z) AS q",
                "z",
            ),
            (
                "WITH a(x, z) AS (VALUES (1, 2)), \
                      b(y, z) AS (VALUES (3, 4)) \
                 SELECT q.* FROM a AS q JOIN b AS q",
                "z",
            ),
            (
                "WITH a(x, z) AS (VALUES (1, 2)) \
                 SELECT q.* FROM a AS q \
                 JOIN (SELECT 3 AS y, 4 AS z) AS q",
                "z",
            ),
            (
                "SELECT q.* FROM json_source AS q JOIN json_each('[]') AS q",
                "key",
            ),
            (
                "SELECT q.* FROM json_each('[1]') AS q \
                 JOIN json_each('[2]') AS q",
                "key",
            ),
        ] {
            let mut resolver = Resolver::new(&schema);
            let errors = resolver.resolve_statement(&parse_one(sql));
            assert!(
                matches!(
                    errors.as_slice(),
                    [SemanticError {
                        kind: SemanticErrorKind::AmbiguousColumn { column, .. },
                        ..
                    }] if column == expected_column
                ),
                "same SQLite source namespace must reject overlap for `{sql}`: {errors:?}"
            );
        }
    }

    #[test]
    fn test_duplicate_alias_table_star_preserves_declared_column_order() {
        let mut schema = Schema::new();
        for (table_name, columns) in [
            ("left_za", ["z", "a"]),
            ("right_az", ["a", "z"]),
            ("left_az", ["a", "z"]),
            ("right_za", ["z", "a"]),
        ] {
            schema.add_table(TableDef {
                name: table_name.to_owned(),
                columns: columns
                    .into_iter()
                    .map(|name| ColumnDef {
                        name: name.to_owned(),
                        affinity: TypeAffinity::Integer,
                        is_ipk: false,
                        not_null: false,
                    })
                    .collect(),
                without_rowid: false,
                strict: false,
            });
        }

        for (sql, expected_column) in [
            ("SELECT q.* FROM left_za AS q JOIN right_az AS q", "z"),
            ("SELECT q.* FROM left_az AS q JOIN right_za AS q", "a"),
        ] {
            let mut resolver = Resolver::new(&schema);
            let errors = resolver.resolve_statement(&parse_one(sql));
            assert!(
                matches!(
                    errors.as_slice(),
                    [SemanticError {
                        kind: SemanticErrorKind::AmbiguousColumn { column, .. },
                        ..
                    }] if column == expected_column
                ),
                "SQLite reports the first overlap in the earlier source's declared order for \
                 `{sql}`: {errors:?}"
            );
        }
    }

    #[test]
    fn test_duplicate_alias_derived_stars_propagate_ordered_columns() {
        let mut schema = Schema::new();
        for (table_name, columns) in [("left_source", ["x", "z"]), ("right_source", ["y", "z"])] {
            schema.add_table(TableDef {
                name: table_name.to_owned(),
                columns: columns
                    .into_iter()
                    .map(|name| ColumnDef {
                        name: name.to_owned(),
                        affinity: TypeAffinity::Integer,
                        is_ipk: false,
                        not_null: false,
                    })
                    .collect(),
                without_rowid: false,
                strict: false,
            });
        }

        let statement = parse_one(
            "SELECT q.* \
             FROM (SELECT * FROM left_source) AS q \
             JOIN (SELECT * FROM right_source) AS q",
        );
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&statement);
        assert!(
            matches!(
                errors.as_slice(),
                [SemanticError {
                    kind: SemanticErrorKind::AmbiguousColumn { column, .. },
                    ..
                }] if column == "z"
            ),
            "derived SELECT * metadata must expose the shared column: {errors:?}"
        );
    }

    #[test]
    fn test_derived_output_names_use_sqlite_lowest_free_suffix() {
        for (columns, expected) in [
            (vec!["x", "x", "x:1"], vec!["x", "x:1", "x:2"]),
            (vec!["x", "x:2", "x", "x"], vec!["x", "x:2", "x:1", "x:3"]),
            (vec!["x:01", "x:01", "x"], vec!["x:01", "x:1", "x"]),
        ] {
            assert_eq!(
                Resolver::canonicalize_derived_output_columns(
                    columns.into_iter().map(str::to_owned).collect()
                ),
                expected.into_iter().map(str::to_owned).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn test_derived_and_cte_duplicate_outputs_publish_canonical_names() {
        let schema = Schema::new();
        for sql in [
            "SELECT q.\"x:1\" FROM (SELECT 1 AS x, 2 AS x) AS q",
            "WITH q(x, x) AS (VALUES (1, 2)) SELECT q.\"x:1\" FROM q",
        ] {
            let mut resolver = Resolver::new(&schema);
            let errors = resolver.resolve_statement(&parse_one(sql));
            assert!(
                errors.is_empty(),
                "derived output names must be addressable after SQLite-style renaming for \
                 `{sql}`: {errors:?}"
            );
        }
    }

    #[test]
    fn test_cross_schema_star_duplicates_are_renamed_at_derived_boundary() {
        let column = |name: &str| ColumnDef {
            name: name.to_owned(),
            affinity: TypeAffinity::Integer,
            is_ipk: false,
            not_null: false,
        };
        let mut schema = Schema::new();
        schema.add_table_in_schema(
            "main",
            TableDef {
                name: "left_source".to_owned(),
                columns: ["x", "z"].into_iter().map(column).collect(),
                without_rowid: false,
                strict: false,
            },
        );
        schema.add_table_in_schema(
            "temp",
            TableDef {
                name: "right_source".to_owned(),
                columns: ["y", "z"].into_iter().map(column).collect(),
                without_rowid: false,
                strict: false,
            },
        );

        let statement = parse_one(
            "SELECT outer_q.\"z:1\" \
             FROM (SELECT q.* \
                   FROM main.left_source AS q JOIN temp.right_source AS q) AS outer_q",
        );
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&statement);
        assert!(
            errors.is_empty(),
            "a derived boundary must expose the second inherited z as z:1: {errors:?}"
        );
    }

    #[test]
    fn test_table_star_does_not_correlate_to_parent_scope() {
        let mut schema = Schema::new();
        schema.add_table(TableDef {
            name: "outer_t".to_owned(),
            columns: vec![ColumnDef {
                name: "x".to_owned(),
                affinity: TypeAffinity::Integer,
                is_ipk: false,
                not_null: false,
            }],
            without_rowid: false,
            strict: false,
        });

        let correlated_column = parse_one("SELECT (SELECT outer_t.x) FROM outer_t");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&correlated_column);
        assert!(
            errors.is_empty(),
            "ordinary qualified columns must remain correlated: {errors:?}"
        );
        assert_eq!(resolver.columns_bound, 1);

        let correlated_star = parse_one("SELECT (SELECT outer_t.*) FROM outer_t");
        let errors = resolver.resolve_statement(&correlated_star);
        assert_eq!(
            errors.len(),
            1,
            "parent-scope table-star must be rejected: {errors:?}"
        );
        assert!(
            matches!(
                &errors[0].kind,
                SemanticErrorKind::UnresolvedTable { name } if name == "outer_t"
            ),
            "table-star must resolve only against the local FROM scope: {errors:?}"
        );
    }

    #[test]
    fn test_quoted_dotted_table_identifier_is_not_schema_qualified() {
        let schema = make_schema();
        let stmt = parse_one("SELECT \"main.users\".name FROM users");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(
            errors.iter().any(|error| matches!(
                &error.kind,
                SemanticErrorKind::UnresolvedColumn {
                    table: Some(table),
                    column,
                } if table == "main.users" && column == "name"
            )),
            "quoted dotted identifier must remain one unresolved table alias: {errors:?}"
        );
    }

    #[test]
    fn test_resolve_named_namespace_does_not_fall_back_to_main_schema() {
        let mut schema = make_schema();
        schema.add_table_in_schema(
            "aux",
            TableDef {
                name: "users".to_owned(),
                columns: vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        affinity: TypeAffinity::Integer,
                        is_ipk: true,
                        not_null: true,
                    },
                    ColumnDef {
                        name: "nickname".to_owned(),
                        affinity: TypeAffinity::Text,
                        is_ipk: false,
                        not_null: false,
                    },
                ],
                without_rowid: false,
                strict: false,
            },
        );

        let stmt = parse_one("SELECT name FROM aux.users");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert_eq!(errors.len(), 1, "expected unresolved aux.users.name");
        assert!(matches!(
            errors[0].kind,
            SemanticErrorKind::UnresolvedColumn { .. }
        ));
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
    fn test_resolve_join_using() {
        let schema = make_schema();
        let stmt = parse_one("SELECT u.name, o.amount FROM users u JOIN orders o USING (id)");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(resolver.tables_resolved, 2);
        assert_eq!(resolver.columns_bound, 3); // u.name, o.amount, id (resolved redundantly but bounded once)
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
    fn test_unaliased_subqueries() {
        let schema = make_schema();
        // Since there are two unknown subqueries and a is not known, "a" should be reported as unresolved
        let stmt = parse_one("SELECT a FROM (SELECT 1), (SELECT 2)");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert_eq!(errors.len(), 1, "Expected unresolved column error!");
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
    fn test_resolve_table_star_honors_alias_hiding() {
        let schema = make_schema();

        for sql in ["SELECT users.* FROM users", "SELECT u.* FROM users AS u"] {
            let stmt = parse_one(sql);
            let mut resolver = Resolver::new(&schema);
            let errors = resolver.resolve_statement(&stmt);
            assert!(errors.is_empty(), "unexpected errors for {sql}: {errors:?}");
        }

        let stmt = parse_one("SELECT users.* FROM users AS u");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(
            errors.iter().any(|error| matches!(
                &error.kind,
                SemanticErrorKind::UnresolvedTable { name } if name == "users"
            )),
            "the alias must hide the underlying table-star qualifier: {errors:?}"
        );
    }

    #[test]
    fn test_resolve_star_in_subquery_without_tables() {
        let schema = make_schema();
        let stmt = parse_one("SELECT (SELECT *) FROM users");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0].kind,
            SemanticErrorKind::NoTablesSpecifiedForStar
        ));
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

    #[test]
    fn test_resolve_rowid_column() {
        let schema = make_schema();
        let stmt = parse_one("SELECT rowid, _rowid_, oid FROM users");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }

    #[test]
    fn test_order_by_select_alias_shadowing() {
        let mut schema = Schema::new();
        schema.add_table(TableDef {
            name: "tbl".to_owned(),
            columns: vec![ColumnDef {
                name: "a".to_owned(),
                affinity: TypeAffinity::Integer,
                is_ipk: false,
                not_null: false,
            }],
            without_rowid: false,
            strict: false,
        });

        // "a" is both an alias and a column in the table.
        let stmt = parse_one("SELECT 1 AS a FROM tbl ORDER BY a");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);

        // SQLite permits ORDER BY to resolve the SELECT-list alias here rather
        // than treating the alias/column name overlap as ambiguous.
        if !errors.is_empty() {
            panic!("Expected no errors, but got: {:?}", errors);
        }
    }

    #[test]
    fn test_compound_order_by_can_resolve_alias_from_later_arm() {
        let schema = make_schema();
        let stmt = parse_one("SELECT 1 AS a UNION SELECT 2 AS b ORDER BY b");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }

    #[test]
    fn test_compound_order_by_can_match_output_expression_from_later_arm() {
        let mut schema = Schema::new();
        schema.add_table(TableDef {
            name: "tbl".to_owned(),
            columns: vec![
                ColumnDef {
                    name: "a".to_owned(),
                    affinity: TypeAffinity::Integer,
                    is_ipk: false,
                    not_null: false,
                },
                ColumnDef {
                    name: "b".to_owned(),
                    affinity: TypeAffinity::Integer,
                    is_ipk: false,
                    not_null: false,
                },
            ],
            without_rowid: false,
            strict: false,
        });

        let stmt = parse_one("SELECT a + 1 FROM tbl UNION SELECT b + 1 FROM tbl ORDER BY b + 1");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }

    // ── Metrics tests ──

    #[test]
    fn test_semantic_metrics() {
        // Delta-based assertion: never call reset_semantic_metrics() in tests
        // as it races with parallel tests.
        let before = semantic_metrics_snapshot();
        let schema = make_schema();

        // Trigger an error.
        let stmt = parse_one("SELECT nonexistent FROM users");
        let mut resolver = Resolver::new(&schema);
        let _ = resolver.resolve_statement(&stmt);

        let after = semantic_metrics_snapshot();
        assert!(
            after.fsqlite_semantic_errors_total > before.fsqlite_semantic_errors_total,
            "expected at least 1 new semantic error, before={}, after={}",
            before.fsqlite_semantic_errors_total,
            after.fsqlite_semantic_errors_total,
        );
    }

    #[test]
    fn test_resolve_function_arity() {
        let schema = make_schema();
        let stmt = parse_one("SELECT sum(1, 2)");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0].kind,
            SemanticErrorKind::FunctionArityMismatch { .. }
        ));
    }

    #[test]
    fn test_resolve_group_by_alias() {
        let schema = make_schema();
        let stmt = parse_one("SELECT id AS x FROM users GROUP BY x");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }

    #[test]
    fn test_resolve_escape_on_non_like() {
        let schema = make_schema();
        // LIKE with ESCAPE is valid.
        let stmt_like = parse_one("SELECT 1 LIKE 2 ESCAPE 3");
        let mut resolver_like = Resolver::new(&schema);
        let errors_like = resolver_like.resolve_statement(&stmt_like);
        assert!(errors_like.is_empty(), "LIKE ESCAPE should be valid");

        // GLOB with ESCAPE is invalid.
        let stmt_glob = parse_one("SELECT 1 GLOB 2 ESCAPE 3");
        let mut resolver_glob = Resolver::new(&schema);
        let errors_glob = resolver_glob.resolve_statement(&stmt_glob);
        assert_eq!(errors_glob.len(), 1);
        assert!(matches!(
            errors_glob[0].kind,
            SemanticErrorKind::FunctionArityMismatch { .. }
        ));
    }

    #[test]
    fn test_update_assignment_target_strict() {
        let schema = make_schema();
        // The outer query has a table `orders` with `amount`.
        // The inner query updates `users`.
        // `users` does not have `amount`.
        // If the assignment target incorrectly resolves against the outer scope, no error is emitted.
        // It SHOULD emit an error because `amount` is not in `users`.
        let stmt = parse_one("WITH cte(amount) AS (SELECT 1) UPDATE users SET amount = 1 FROM cte");
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert_eq!(
            errors.len(),
            1,
            "Should report amount as unresolved for users table, instead got: {:?}",
            errors
        );
    }

    #[test]
    fn test_rowid_resolution() {
        let schema = make_schema();
        let mut p = Parser::from_sql("SELECT rowid FROM users");
        let (stmts, _) = p.parse_all();
        let stmt = stmts.into_iter().next().unwrap();
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "errors: {:?}", errors);
    }
}
