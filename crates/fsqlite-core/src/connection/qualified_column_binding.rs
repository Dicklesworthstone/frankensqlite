//! bd-at0bx: bind `schema.table.column` references the way SQLite's
//! `lookupName` (resolve.c) does when the reference carries a database name.
//!
//! SQLite matches such a reference only against a FROM item (or DML target)
//! whose relation lives in that database and whose alias, or table name when
//! it has no alias, is `table`; it searches the innermost query first and
//! moves outward. A reference nothing matches is "no such column:
//! schema.table.column", and one that two items of a single query match is
//! "ambiguous column name". The resolvers downstream of this pass match a
//! qualifier by table name or alias alone, so same-named tables in `main`,
//! `temp` and attached databases could not be told apart: a
//! `temp.users.name` read `main.users`, and a qualifier naming the wrong
//! database was accepted.
//!
//! [`Connection::bind_schema_qualified_columns`] does SQLite's lookup before
//! any of them run. It rewrites each three-part reference to the two-part
//! form naming the item it matched; when another item visible at the
//! reference answers to the same name, the matched item gets a synthetic
//! alias (and every reference to it is rewritten to that alias) so the
//! two-part form cannot bind elsewhere. It also reports the unmatched and
//! ambiguous cases with stock's messages, including a two-part
//! `table.column` that matches same-named items from different databases.

#[allow(clippy::wildcard_imports)]
use super::*;

mod schema_only_cte;

/// What a FROM item's relation is, as far as column binding needs it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct QualifiedColumnRelation {
    /// Owning database, lowercased: `main`, `temp`, or an attached name.
    /// `None` for a subquery or a CTE, which no database qualifier names.
    database: Option<String>,
    /// Column names, or `None` when they are not known here (a virtual
    /// table's hidden columns, a view whose projection is not plain); every
    /// column is then taken to exist.
    columns: Option<Vec<String>>,
    /// Which of `rowid`, `_rowid_` and `oid` name the hidden rowid.
    rowid_names: Vec<&'static str>,
    /// Whether this relation exposes an explicit `true` or `false` column (bd-pud7p).
    ///
    /// Derived relations (subqueries, CTEs, views) never expose `true`/`false`
    /// because SQLite's `sqlite3ColumnsFromExprList` renames them to `columnN`.
    exposes_true: bool,
    exposes_false: bool,
}

impl QualifiedColumnRelation {
    fn table(database: &str, table: &TableSchema) -> Self {
        Self {
            database: Some(database.to_owned()),
            columns: Some(table.columns.iter().map(|c| c.name.clone()).collect()),
            rowid_names: ["rowid", "_rowid_", "oid"]
                .into_iter()
                .filter(|name| !table.without_rowid && table.column_index(name).is_none())
                .collect(),
            exposes_true: table.column_index("true").is_some(),
            exposes_false: table.column_index("false").is_some(),
        }
    }

    fn vtab(database: &str, table: Option<&TableSchema>) -> Self {
        Self {
            database: Some(database.to_owned()),
            columns: None,
            rowid_names: Vec::new(),
            exposes_true: table.is_some_and(|t| t.column_index("true").is_some()),
            exposes_false: table.is_some_and(|t| t.column_index("false").is_some()),
        }
    }

    fn opaque(database: &str, columns: Option<Vec<String>>) -> Self {
        Self {
            database: Some(database.to_owned()),
            columns,
            rowid_names: Vec::new(),
            exposes_true: false,
            exposes_false: false,
        }
    }

    /// `Some(found)` when the columns are known, `None` otherwise.
    fn has_column(&self, column: &str) -> Option<bool> {
        self.columns
            .as_ref()
            .map(|columns| columns.iter().any(|c| c.eq_ignore_ascii_case(column)))
    }

    fn has_rowid_name(&self, column: &str) -> bool {
        self.rowid_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(column))
    }

    fn exposes_true_or_false(&self, is_true: bool) -> bool {
        if is_true {
            self.exposes_true
        } else {
            self.exposes_false
        }
    }
}

/// Catalog facts the binder looks up.
trait QualifiedColumnCatalog {
    /// Lowercased name of the open database a qualifier names, if any.
    fn database_named(&self, schema: &str) -> Option<String>;

    /// The relation `[schema.]name` names. An unqualified name is searched
    /// in TEMP, then MAIN, then each attached database in attach order, as
    /// SQLite does. `None` when there is no such relation.
    fn relation(&self, schema: Option<&str>, name: &str) -> Option<QualifiedColumnRelation>;

    /// Column metadata for a table-valued function (eponymous virtual table in
    /// MAIN). Defaults to opaque when the catalog has no TVF registry.
    fn table_function_relation(&self, _name: &str) -> QualifiedColumnRelation {
        QualifiedColumnRelation::opaque("main", None)
    }

    /// Validate a function call's existence / arity so walk-order error
    /// precedence reports an earlier `no such function` ahead of a later
    /// three-part column error (bd-av6zk).
    fn validate_function_call(
        &self,
        _name: &str,
        _args: &FunctionArgs,
        _over: Option<&WindowSpec>,
    ) -> Result<()> {
        Ok(())
    }
}

fn builtin_schema_column_relation(database: &str) -> QualifiedColumnRelation {
    QualifiedColumnRelation {
        database: Some(database.to_owned()),
        columns: Some(
            sqlite_master_column_infos()
                .into_iter()
                .map(|column| column.name)
                .collect(),
        ),
        rowid_names: vec!["rowid", "_rowid_", "oid"],
        exposes_true: false,
        exposes_false: false,
    }
}

impl QualifiedColumnCatalog for Connection {
    fn database_named(&self, schema: &str) -> Option<String> {
        self.attached_schemas
            .borrow()
            .is_valid_schema(schema)
            .then(|| schema.to_ascii_lowercase())
    }

    fn relation(&self, schema: Option<&str>, name: &str) -> Option<QualifiedColumnRelation> {
        let Some(schema) = schema else {
            if canonical_sqlite_temp_schema_name(name).is_some() {
                return Some(builtin_schema_column_relation("temp"));
            }
            if canonical_sqlite_schema_name(name).is_some() {
                return Some(builtin_schema_column_relation("main"));
            }
            return self
                .local_column_relation(name, PragmaSchemaScope::Temp)
                .or_else(|| self.local_column_relation(name, PragmaSchemaScope::Main))
                .or_else(|| {
                    let attached: Vec<String> = self
                        .attached_schemas
                        .borrow()
                        .all_schemas()
                        .into_iter()
                        .filter(|schema| !is_builtin_schema(schema))
                        .map(str::to_owned)
                        .collect();
                    attached
                        .iter()
                        .find_map(|schema| self.attached_column_relation(schema, name))
                });
        };
        if schema.eq_ignore_ascii_case("main") {
            if canonical_sqlite_schema_name(name).is_some() {
                return Some(builtin_schema_column_relation("main"));
            }
            self.local_column_relation(name, PragmaSchemaScope::Main)
        } else if schema.eq_ignore_ascii_case("temp") {
            if canonical_sqlite_schema_name(name).is_some()
                || canonical_sqlite_temp_schema_name(name).is_some()
            {
                return Some(builtin_schema_column_relation("temp"));
            }
            self.local_column_relation(name, PragmaSchemaScope::Temp)
        } else {
            if self.database_named(schema).is_some()
                && canonical_sqlite_schema_name(name).is_some()
            {
                return Some(builtin_schema_column_relation(&schema.to_ascii_lowercase()));
            }
            self.attached_column_relation(schema, name)
        }
    }

    fn table_function_relation(&self, name: &str) -> QualifiedColumnRelation {
        let columns = self.table_function_result_column_names(name).map(|mut cols| {
            for hidden in table_function_hidden_columns(name) {
                if !cols.iter().any(|col| col.eq_ignore_ascii_case(hidden)) {
                    cols.push((*hidden).to_owned());
                }
            }
            if name.eq_ignore_ascii_case("generate_series") {
                for hidden in ["start", "stop", "step"] {
                    if !cols.iter().any(|col| col.eq_ignore_ascii_case(hidden)) {
                        cols.push(hidden.to_owned());
                    }
                }
            }
            cols
        });
        QualifiedColumnRelation {
            database: Some("main".to_owned()),
            columns,
            rowid_names: vec!["rowid", "_rowid_", "oid"],
            exposes_true: false,
            exposes_false: false,
        }
    }

    fn validate_function_call(
        &self,
        name: &str,
        args: &FunctionArgs,
        over: Option<&WindowSpec>,
    ) -> Result<()> {
        self.with_fallback_function_registry(|| {
            validate_function_call_resolution(name, args, over, Span::ZERO)
        })
    }
}

impl Connection {
    /// bd-at0bx: bind the statement's `schema.table.column` references as
    /// SQLite does (see the module docs). Returns the rewritten statement,
    /// `None` when there is nothing to rewrite, or stock's "no such column" /
    /// "ambiguous column name" error.
    ///
    /// This is the shared pre-execution entry point for direct and prepared
    /// statements. After binding, schema-only SELECTs may also lower a
    /// transparent, single-use CTE to an ordinary derived-table read.
    pub(super) fn bind_schema_qualified_columns(
        &self,
        statement: &Statement,
    ) -> Result<Option<Statement>> {
        let bound = if statement_needs_binding(statement)
            || statement_shares_a_name_across_queries(statement)
        {
            bind_schema_qualified_columns_with(self, statement)?
        } else {
            None
        };
        // Bind the original WITH scope first: a CTE has no owning database,
        // and lowering must not make an invalid main.cte.column reference
        // resolve to a real table with that name.
        let lowered = self.lower_schema_only_projection_cte(bound.as_ref().unwrap_or(statement));
        Ok(lowered.or(bound))
    }

    /// A table or view of this connection's own MAIN or TEMP database.
    fn local_column_relation(
        &self,
        name: &str,
        scope: PragmaSchemaScope,
    ) -> Option<QualifiedColumnRelation> {
        let database = if matches!(scope, PragmaSchemaScope::Temp) {
            "temp"
        } else {
            "main"
        };
        if self.table_exists_for_scope(name, scope) {
            if self
                .vtab_instances
                .borrow()
                .keys()
                .any(|key| key.eq_ignore_ascii_case(name))
            {
                if matches!(scope, PragmaSchemaScope::Main)
                    && let Some(table) = self
                        .shadowed_main_tables
                        .borrow()
                        .get(&name.to_ascii_lowercase())
                {
                    return Some(QualifiedColumnRelation::vtab(database, Some(table)));
                }
                let schema = self.schema.borrow();
                let table = self.schema_index_of(name).and_then(|idx| schema.get(idx));
                return Some(QualifiedColumnRelation::vtab(database, table));
            }
            if matches!(scope, PragmaSchemaScope::Main)
                && let Some(table) = self
                    .shadowed_main_tables
                    .borrow()
                    .get(&name.to_ascii_lowercase())
            {
                return Some(QualifiedColumnRelation::table(database, table));
            }
            let index = self.schema_index_of(name)?;
            return self
                .schema
                .borrow()
                .get(index)
                .map(|table| QualifiedColumnRelation::table(database, table));
        }
        let index = self.view_index_for_scope(name, scope)?;
        let view = self.views.borrow().get(index)?.clone();
        let columns = view_output_column_names(&view).or_else(|| {
            let resolved = Self::deduplicate_relation_column_names(
                self.select_result_column_names(&view.query, &[], &mut Vec::new()),
            );
            (!resolved.is_empty()).then_some(resolved)
        });
        Some(QualifiedColumnRelation::opaque(database, columns))
    }

    /// A table or view of the attached database `schema`.
    fn attached_column_relation(
        &self,
        schema: &str,
        name: &str,
    ) -> Option<QualifiedColumnRelation> {
        let relation = self
            .with_attached_connection(schema, |conn| {
                Ok(conn.local_column_relation(name, PragmaSchemaScope::Main))
            })
            .ok()??;
        Some(QualifiedColumnRelation {
            database: Some(schema.to_ascii_lowercase()),
            ..relation
        })
    }
}

/// Bind against `catalog`; see [`Connection::bind_schema_qualified_columns`].
fn bind_schema_qualified_columns_with(
    catalog: &dyn QualifiedColumnCatalog,
    statement: &Statement,
) -> Result<Option<Statement>> {
    let mut bound = statement.clone();
    let mut binder = QualifiedColumnBinder::new(catalog);
    binder.statement(&mut bound);
    // SQLite resolves every table before any column, so a missing relation
    // is reported ("no such table") ahead of any column error: leave the
    // statement to the ordinary path, which reports it.
    if binder.unresolved_relation {
        return Ok(None);
    }
    if let Some(error) = binder.error.take() {
        return Err(error);
    }
    if binder.saw_later_three_part_error
        && let Some(error) = binder.earlier_non_three_part_error.take()
    {
        return Err(error);
    }
    // bd-x4g7x: a `name.*` over several same-named items is rewritten one
    // star per item even when no three-part reference forces the aliases.
    let splits_table_star = binder
        .table_star_groups
        .iter()
        .any(|group| group.len() > 1);
    if binder.three_part_refs == 0
        && !splits_table_star
        && binder.true_false_rewrites == 0
        && binder.needs_alias.is_empty()
    {
        return Ok(None);
    }
    binder.assign_aliases();
    binder.begin_pass(BindPass::Rewrite);
    binder.statement(&mut bound);
    Ok(Some(bound))
}

/// One FROM item (or DML target) as `lookupName` sees it.
#[derive(Debug)]
struct BindingSource {
    /// Position in the statement's walk order; stable across passes.
    id: usize,
    /// The relation's own name (`pTab->zName`); `None` for a subquery.
    table: Option<String>,
    /// The alias written on the item.
    alias: Option<String>,
    relation: QualifiedColumnRelation,
    /// Columns the join that brought this item in coalesces with the items
    /// before it (`USING`, or a NATURAL join's common columns).
    coalesced: Vec<String>,
    /// The INSERT/UPDATE/DELETE target. The DML executors key the target by
    /// its table name, so it is never given a synthetic alias; the items that
    /// share its name are aliased instead.
    is_target: bool,
}

impl BindingSource {
    /// The name a qualifier must spell to address this item.
    fn addressed_as(&self) -> Option<&str> {
        self.alias.as_deref().or(self.table.as_deref())
    }

    /// `(name, database)` that seeds this item's synthetic alias.
    fn alias_seed(&self) -> (String, String) {
        (
            self.addressed_as().unwrap_or_default().to_owned(),
            self.relation.database.clone().unwrap_or_default(),
        )
    }

    fn coalesces(&self, column: &str) -> bool {
        self.coalesced
            .iter()
            .any(|name| name.eq_ignore_ascii_case(column))
    }
}

enum Lookup {
    Bound { scope: usize, id: usize },
    Ambiguous,
    Missing,
}

enum TrueFalseLookup {
    /// Resolved to an unambiguous FROM/DML table column named `true` or `false`.
    Column,
    /// Resolved to an explicit `AS "true"` / `AS "false"` result-set alias.
    Alias {
        alias_expr: Box<Expr>,
        from_outer: bool,
    },
    /// Two or more uncoalesced sources in the same scope expose `true` / `false`.
    Ambiguous,
    /// No visible column or alias matched — keep as boolean literal `TRUE`/`FALSE`.
    Literal,
}

/// One lexical query scope: its FROM/DML sources plus optional `SELECT`
/// result-set aliases (SQLite's `NC_UEList`).
#[derive(Debug, Default)]
struct BindingScope {
    sources: Vec<BindingSource>,
    result_aliases: Vec<String>,
    true_alias: Option<Expr>,
    false_alias: Option<Expr>,
    aliases_visible: bool,
}

impl BindingScope {
    fn from_sources(sources: Vec<BindingSource>) -> Self {
        Self {
            sources,
            result_aliases: Vec::new(),
            true_alias: None,
            false_alias: None,
            aliases_visible: false,
        }
    }

    fn has_result_alias(&self, name: &str) -> bool {
        self.result_aliases
            .iter()
            .any(|alias| alias.eq_ignore_ascii_case(name))
    }

    fn alias_for(&self, is_true: bool) -> Option<&Expr> {
        if is_true {
            self.true_alias.as_ref()
        } else {
            self.false_alias.as_ref()
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BindPass {
    /// Resolve every qualified reference and unquoted `TRUE`/`FALSE`, record
    /// errors, and find the items that need a synthetic alias. Does not modify
    /// the statement.
    Bind,
    /// Apply the synthetic aliases and rewrite the references.
    Rewrite,
}

struct QualifiedColumnBinder<'a> {
    catalog: &'a dyn QualifiedColumnCatalog,
    pass: BindPass,
    /// Enclosing query scopes, outermost first.
    scopes: Vec<BindingScope>,
    /// The CTEs in scope, per WITH clause: lowercased name and the columns
    /// it exposes when they are known (see [`cte_known_columns`]).
    ctes: Vec<Vec<(String, Option<Vec<String>>)>>,
    next_id: usize,
    three_part_refs: usize,
    true_false_rewrites: usize,
    unresolved_relation: bool,
    error: Option<FrankenError>,
    /// First bare-column, two-part-missing, or function error seen before any
    /// binder error (bd-av6zk). Surfaced only when a later three-part error
    /// would otherwise preempt it out of walk order.
    earlier_non_three_part_error: Option<FrankenError>,
    saw_later_three_part_error: bool,
    /// Items that get a synthetic alias: id -> (name, database).
    needs_alias: BTreeMap<usize, (String, String)>,
    /// For each `name.*`, the non-target items of its query addressed as
    /// `name`: id -> (name, database).
    table_star_groups: Vec<Vec<(usize, (String, String))>>,
    aliases: HashMap<usize, String>,
    /// Lowercased names the statement already uses as a table name, alias,
    /// CTE name or column qualifier.
    taken_names: HashSet<String>,
    /// The DML scope while its RETURNING clause is walked.
    returning_scope: Option<usize>,
    /// True while walking a `WHERE` clause (so aliased aggregate/window
    /// expressions stay spelled as bare aliases for exact error reporting).
    in_where_clause: bool,
    /// True while walking an `ON CONFLICT DO UPDATE` clause where `excluded.*`
    /// is visible.
    in_upsert_update: bool,
}

impl<'a> QualifiedColumnBinder<'a> {
    fn new(catalog: &'a dyn QualifiedColumnCatalog) -> Self {
        Self {
            catalog,
            pass: BindPass::Bind,
            scopes: Vec::new(),
            ctes: Vec::new(),
            next_id: 0,
            three_part_refs: 0,
            true_false_rewrites: 0,
            unresolved_relation: false,
            error: None,
            earlier_non_three_part_error: None,
            saw_later_three_part_error: false,
            needs_alias: BTreeMap::new(),
            table_star_groups: Vec::new(),
            aliases: HashMap::new(),
            taken_names: HashSet::new(),
            returning_scope: None,
            in_where_clause: false,
            in_upsert_update: false,
        }
    }

    fn begin_pass(&mut self, pass: BindPass) {
        self.pass = pass;
        self.scopes.clear();
        self.ctes.clear();
        self.next_id = 0;
        self.returning_scope = None;
        self.in_where_clause = false;
        self.in_upsert_update = false;
    }

    fn fail(&mut self, error: FrankenError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    fn fail_three_part(&mut self, error: FrankenError) {
        if self.earlier_non_three_part_error.is_some() {
            self.saw_later_three_part_error = true;
            return;
        }
        self.fail(error);
    }

    fn note_non_three_part_error(&mut self, error: FrankenError) {
        if self.error.is_none() && self.earlier_non_three_part_error.is_none() {
            self.earlier_non_three_part_error = Some(error);
        }
    }

    fn take_name(&mut self, name: &str) {
        if self.pass == BindPass::Bind {
            self.taken_names.insert(name.to_ascii_lowercase());
        }
    }

    /// Give each item in `needs_alias` a name nothing in the statement uses.
    /// A `name.*` covering an aliased item, or more than one item (bd-x4g7x:
    /// `users.*` over `main.users JOIN temp.users` expands both, as SQLite's
    /// `selectExpander` does), is spelled one star per item, so every item it
    /// covers is aliased and each star addresses its own.
    fn assign_aliases(&mut self) {
        for group in std::mem::take(&mut self.table_star_groups) {
            if group.len() > 1
                || group
                    .iter()
                    .any(|(id, _)| self.needs_alias.contains_key(id))
            {
                for (id, seed) in group {
                    self.needs_alias.entry(id).or_insert(seed);
                }
            }
        }
        for (id, (name, database)) in std::mem::take(&mut self.needs_alias) {
            let base = format!("{name}@{database}");
            let mut alias = base.clone();
            let mut suffix = 1_usize;
            while !self.taken_names.insert(alias.to_ascii_lowercase()) {
                suffix += 1;
                alias = format!("{base}#{suffix}");
            }
            self.aliases.insert(id, alias);
        }
    }

    fn statement(&mut self, statement: &mut Statement) {
        match statement {
            Statement::Select(select) => self.select(select),
            Statement::Insert(insert) => self.insert(insert),
            Statement::Update(update) => self.update(update),
            Statement::Delete(delete) => self.delete(delete),
            Statement::CreateTable(create) => {
                if let CreateTableBody::AsSelect(select) = &mut create.body {
                    self.select(select);
                }
            }
            Statement::Explain { stmt, .. } => self.statement(stmt),
            _ => {}
        }
    }

    fn with_clause(&mut self, with: Option<&mut fsqlite_ast::WithClause>) -> bool {
        let Some(with) = with else {
            return false;
        };
        let ctes: Vec<(String, Option<Vec<String>>)> = with
            .ctes
            .iter()
            .map(|cte| (cte.name.to_ascii_lowercase(), cte_known_columns(cte)))
            .collect();
        for (name, _) in &ctes {
            self.take_name(name);
        }
        self.ctes.push(ctes);
        for cte in &mut with.ctes {
            self.select(&mut cte.query);
        }
        true
    }

    /// The relation a CTE in scope named `name` binds as, the innermost WITH
    /// first; `None` when no CTE has the name. A CTE belongs to no database.
    fn cte_relation(&self, name: &str) -> Option<QualifiedColumnRelation> {
        self.ctes
            .iter()
            .rev()
            .flatten()
            .find(|(cte, _)| cte.eq_ignore_ascii_case(name))
            .map(|(_, columns)| QualifiedColumnRelation {
                columns: columns.clone(),
                ..QualifiedColumnRelation::default()
            })
    }

    fn select(&mut self, select: &mut SelectStatement) {
        let pushed_ctes = self.with_clause(select.with.as_mut());
        if select.body.compounds.is_empty() {
            self.core(
                &mut select.body.select,
                Some(&mut select.order_by),
                select.limit.as_mut(),
                None,
            );
        } else {
            // A compound's ORDER BY terms are matched against result columns
            // (see `compound_order_terms`); `settled` marks the terms that
            // need no binding or are already matched.
            let mut settled: Vec<bool> = select
                .order_by
                .iter_mut()
                .map(|term| {
                    compound_order_column(&mut term.expr).is_none()
                        && bare_true_false_under_collate(&mut term.expr).is_none()
                })
                .collect();
            // SQLite's `resolveCompoundOrderBy` matches bare ORDER BY names
            // against the leftmost SELECT's explicit AS aliases before probing
            // individual arms (`resolveAsName`).
            if let SelectCore::Select { columns, .. } = &select.body.select {
                for (term, is_settled) in select.order_by.iter_mut().zip(settled.iter_mut()) {
                    if *is_settled {
                        continue;
                    }
                    let Some((leaf, is_true, span)) =
                        bare_true_false_under_collate(&mut term.expr)
                    else {
                        continue;
                    };
                    let col_name = if is_true { "true" } else { "false" };
                    let matches_leftmost_alias = columns.iter().any(|col| {
                        matches!(
                            col,
                            ResultColumn::Expr { alias: Some(alias), .. }
                                if alias.eq_ignore_ascii_case(col_name)
                        )
                    });
                    if matches_leftmost_alias {
                        *is_settled = true;
                        match self.pass {
                            BindPass::Bind => self.true_false_rewrites += 1,
                            BindPass::Rewrite => {
                                *leaf = Expr::Column(ColumnRef::bare(col_name), span);
                            }
                        }
                    }
                }
            }
            self.core(
                &mut select.body.select,
                None,
                None,
                Some((select.order_by.as_mut_slice(), settled.as_mut_slice())),
            );
            for (_, core) in &mut select.body.compounds {
                self.core(
                    core,
                    None,
                    None,
                    Some((select.order_by.as_mut_slice(), settled.as_mut_slice())),
                );
            }
            if let Some(limit) = select.limit.as_mut() {
                self.limit(limit);
            }
        }
        if pushed_ctes {
            self.ctes.pop();
        }
    }

    fn core(
        &mut self,
        core: &mut SelectCore,
        order_by: Option<&mut Vec<OrderingTerm>>,
        limit: Option<&mut LimitClause>,
        compound_order_by: Option<(&mut [OrderingTerm], &mut [bool])>,
    ) {
        let mut scope = BindingScope::default();
        match core {
            SelectCore::Values(rows) => {
                self.scopes.push(scope);
                for row in rows.iter_mut() {
                    for expr in row {
                        self.expr(expr);
                    }
                }
            }
            SelectCore::Select {
                columns,
                from,
                where_clause,
                group_by,
                having,
                windows,
                ..
            } => {
                // FROM subqueries see only the enclosing queries, so they are
                // walked while this query's items are still being collected.
                if let Some(from) = from.as_mut() {
                    self.register_from(from, &mut scope.sources);
                }
                self.scopes.push(scope);
                // Before the result columns are rewritten, so both passes
                // compare the terms with the columns as written.
                if let Some((terms, settled)) = compound_order_by {
                    self.compound_order_terms(terms, settled, columns);
                }
                // Walk the SELECT projection list FIRST with `aliases_visible = false`
                // (mirroring SQLite's `resolveSelectStep`: result columns cannot
                // reference sibling `AS` aliases, and their resolved expressions
                // then seed `NC_UEList` for WHERE / GROUP BY / HAVING / ORDER BY).
                self.result_columns(columns);
                let result_aliases = collect_result_aliases(columns);
                let (true_alias, false_alias) = collect_true_false_result_aliases(columns);
                if let Some(current) = self.scopes.last_mut() {
                    current.result_aliases = result_aliases;
                    current.true_alias = true_alias;
                    current.false_alias = false_alias;
                }
                if let Some(from) = from.as_mut() {
                    self.join_exprs(from);
                }
                for window in windows {
                    self.window_spec(&mut window.spec);
                }
                if let Some(current) = self.scopes.last_mut() {
                    current.aliases_visible = true;
                }
                if let Some(expr) = where_clause {
                    let prev_where = self.in_where_clause;
                    self.in_where_clause = true;
                    self.expr(expr);
                    self.in_where_clause = prev_where;
                }
                for expr in group_by {
                    self.expr(expr);
                }
                if let Some(expr) = having {
                    self.expr(expr);
                }
            }
        }
        for term in order_by.into_iter().flatten() {
            // SQLite's `resolveOrderGroupBy` (`zType[0] != 'G'`) resolves a
            // bare (or COLLATE-wrapped) ORDER BY identifier against `AS`
            // aliases before FROM columns (`resolveAsName`).
            if let Some((leaf, is_true, span)) = bare_true_false_under_collate(&mut term.expr)
                && self
                    .scopes
                    .last()
                    .and_then(|current| current.alias_for(is_true))
                    .is_some()
            {
                let col_name = if is_true { "true" } else { "false" };
                match self.pass {
                    BindPass::Bind => self.true_false_rewrites += 1,
                    BindPass::Rewrite => {
                        *leaf = Expr::Column(ColumnRef::bare(col_name), span);
                    }
                }
                continue;
            }
            self.expr(&mut term.expr);
        }
        if let Some(limit) = limit {
            self.limit(limit);
        }
        self.scopes.pop();
    }

    fn insert(&mut self, insert: &mut InsertStatement) {
        let pushed_ctes = self.with_clause(insert.with.as_mut());
        match &mut insert.source {
            InsertSource::Select(select) => self.select(select),
            InsertSource::Values(rows) => {
                self.scopes.push(BindingScope::default());
                for expr in rows.iter_mut().flatten() {
                    self.expr(expr);
                }
                self.scopes.pop();
            }
            InsertSource::DefaultValues => {}
        }
        let mut sources = Vec::new();
        self.register_named(&insert.table, &mut insert.alias, true, &mut sources);
        self.scopes.push(BindingScope::from_sources(sources));
        for upsert in &mut insert.upsert {
            if let Some(expr) = upsert
                .target
                .as_mut()
                .and_then(|target| target.where_clause.as_mut())
            {
                self.expr(expr);
            }
            if let UpsertAction::Update {
                assignments,
                where_clause,
            } = &mut upsert.action
            {
                let prev_upsert = self.in_upsert_update;
                self.in_upsert_update = true;
                for assignment in assignments {
                    self.expr(&mut assignment.value);
                }
                if let Some(expr) = where_clause {
                    self.expr(expr);
                }
                self.in_upsert_update = prev_upsert;
            }
        }
        self.returning(&mut insert.returning);
        self.scopes.pop();
        if pushed_ctes {
            self.ctes.pop();
        }
    }

    fn update(&mut self, update: &mut UpdateStatement) {
        let pushed_ctes = self.with_clause(update.with.as_mut());
        let mut sources = Vec::new();
        self.register_named(
            &update.table.name,
            &mut update.table.alias,
            true,
            &mut sources,
        );
        if let Some(from) = update.from.as_mut() {
            self.register_from(from, &mut sources);
        }
        self.scopes.push(BindingScope::from_sources(sources));
        if let Some(from) = update.from.as_mut() {
            self.join_exprs(from);
        }
        for assignment in &mut update.assignments {
            self.expr(&mut assignment.value);
        }
        if let Some(expr) = &mut update.where_clause {
            self.expr(expr);
        }
        self.returning(&mut update.returning);
        for term in &mut update.order_by {
            self.expr(&mut term.expr);
        }
        if let Some(limit) = &mut update.limit {
            self.limit(limit);
        }
        self.scopes.pop();
        if pushed_ctes {
            self.ctes.pop();
        }
    }

    fn delete(&mut self, delete: &mut DeleteStatement) {
        let pushed_ctes = self.with_clause(delete.with.as_mut());
        let mut sources = Vec::new();
        self.register_named(
            &delete.table.name,
            &mut delete.table.alias,
            true,
            &mut sources,
        );
        self.scopes.push(BindingScope::from_sources(sources));
        if let Some(expr) = &mut delete.where_clause {
            self.expr(expr);
        }
        self.returning(&mut delete.returning);
        for term in &mut delete.order_by {
            self.expr(&mut term.expr);
        }
        if let Some(limit) = &mut delete.limit {
            self.limit(limit);
        }
        self.scopes.pop();
        if pushed_ctes {
            self.ctes.pop();
        }
    }

    /// SQLite resolves a RETURNING clause through the trigger-table path of
    /// `lookupName`, which a database-qualified reference never takes: a
    /// three-part reference to the DML target (or its FROM items) is "no such
    /// column", even from a subquery of the clause.
    fn returning(&mut self, columns: &mut Vec<ResultColumn>) {
        self.returning_scope = self.scopes.len().checked_sub(1);
        self.result_columns(columns);
        self.returning_scope = None;
    }

    fn register_from(&mut self, from: &mut FromClause, scope: &mut Vec<BindingSource>) {
        self.register_source(&mut from.source, None, false, scope);
        for join in &mut from.joins {
            let JoinClause {
                join_type,
                table,
                constraint,
            } = join;
            self.register_source(table, constraint.as_ref(), join_type.natural, scope);
        }
    }

    fn register_source(
        &mut self,
        source: &mut TableOrSubquery,
        constraint: Option<&JoinConstraint>,
        natural: bool,
        scope: &mut Vec<BindingSource>,
    ) {
        let first_new = scope.len();
        match source {
            TableOrSubquery::Table { name, alias, .. } => {
                self.register_named(name, alias, false, scope);
            }
            TableOrSubquery::Subquery { query, alias } => {
                let id = self.next_id;
                self.next_id += 1;
                self.select(query);
                let written_alias = self.apply_alias(id, alias);
                scope.push(BindingSource {
                    id,
                    table: None,
                    alias: written_alias,
                    relation: QualifiedColumnRelation::default(),
                    coalesced: Vec::new(),
                    is_target: false,
                });
            }
            TableOrSubquery::TableFunction { name, alias, .. } => {
                let id = self.next_id;
                self.next_id += 1;
                self.take_name(name);
                let written_alias = self.apply_alias(id, alias);
                // Table-valued functions are eponymous virtual tables of MAIN.
                let relation = self.catalog.table_function_relation(name);
                scope.push(BindingSource {
                    id,
                    table: Some(name.clone()),
                    alias: written_alias,
                    relation,
                    coalesced: Vec::new(),
                    is_target: false,
                });
            }
            TableOrSubquery::ParenJoin(inner) => self.register_from(inner, scope),
        }
        let coalesced: Vec<String> = match constraint {
            Some(JoinConstraint::Using(columns)) => columns.clone(),
            _ if natural => {
                let (before, new) = scope.split_at(first_new);
                new.iter()
                    .filter_map(|source| source.relation.columns.as_ref())
                    .flatten()
                    .filter(|column| {
                        before
                            .iter()
                            .any(|source| source.relation.has_column(column) == Some(true))
                    })
                    .cloned()
                    .collect()
            }
            _ => Vec::new(),
        };
        if !coalesced.is_empty() {
            for source in &mut scope[first_new..] {
                source.coalesced.clone_from(&coalesced);
            }
        }
    }

    /// Register a named relation: a FROM table, or the DML target.
    fn register_named(
        &mut self,
        name: &QualifiedName,
        alias: &mut Option<String>,
        is_target: bool,
        scope: &mut Vec<BindingSource>,
    ) {
        let id = self.next_id;
        self.next_id += 1;
        let cte = (!is_target && name.schema.is_none())
            .then(|| self.cte_relation(&name.name))
            .flatten();
        let relation = if let Some(cte) = cte {
            cte
        } else if let Some(relation) = self.catalog.relation(name.schema.as_deref(), &name.name) {
            relation
        } else {
            self.unresolved_relation = true;
            QualifiedColumnRelation::default()
        };
        self.take_name(&name.name);
        let written_alias = self.apply_alias(id, alias);
        scope.push(BindingSource {
            id,
            table: Some(name.name.clone()),
            alias: written_alias,
            relation,
            coalesced: Vec::new(),
            is_target,
        });
    }

    /// Note the alias written on item `id` and, in the rewrite pass, replace
    /// it with the item's synthetic alias. Returns the written alias, which
    /// lookups keep using so both passes bind identically.
    fn apply_alias(&mut self, id: usize, alias: &mut Option<String>) -> Option<String> {
        let written = alias.clone();
        if let Some(name) = written.as_deref() {
            self.take_name(name);
        }
        if self.pass == BindPass::Rewrite
            && let Some(synthetic) = self.aliases.get(&id)
        {
            *alias = Some(synthetic.clone());
        }
        written
    }

    /// Walk a FROM clause's ON constraints and table-function arguments.
    ///
    /// In SQLite, non-outer (INNER/CROSS) JOIN ON predicates and table-function
    /// arguments see `NC_UEList` result aliases, whereas outer (LEFT/RIGHT/FULL)
    /// JOIN ON predicates do not.
    fn join_exprs(&mut self, from: &mut FromClause) {
        self.source_exprs(&mut from.source);
        for join in &mut from.joins {
            self.source_exprs(&mut join.table);
            if let Some(JoinConstraint::On(expr)) = &mut join.constraint {
                let is_outer = matches!(
                    join.join_type.kind,
                    JoinKind::Left | JoinKind::Right | JoinKind::Full
                );
                let prev = self.scopes.last().is_some_and(|s| s.aliases_visible);
                if let Some(current) = self.scopes.last_mut() {
                    current.aliases_visible = !is_outer;
                }
                self.expr(expr);
                if let Some(current) = self.scopes.last_mut() {
                    current.aliases_visible = prev;
                }
            }
        }
    }

    fn source_exprs(&mut self, source: &mut TableOrSubquery) {
        match source {
            TableOrSubquery::TableFunction { args, .. } => {
                let prev = self.scopes.last().is_some_and(|s| s.aliases_visible);
                if let Some(current) = self.scopes.last_mut() {
                    current.aliases_visible = true;
                }
                for expr in args {
                    self.expr(expr);
                }
                if let Some(current) = self.scopes.last_mut() {
                    current.aliases_visible = prev;
                }
            }
            TableOrSubquery::ParenJoin(inner) => self.join_exprs(inner),
            TableOrSubquery::Table { .. } | TableOrSubquery::Subquery { .. } => {}
        }
    }

    fn result_columns(&mut self, columns: &mut Vec<ResultColumn>) {
        for column in columns.iter_mut() {
            match column {
                ResultColumn::Expr { expr, .. } => self.expr(expr),
                ResultColumn::TableStar(name) => {
                    if self.pass == BindPass::Bind && name.schema.is_none() {
                        self.take_name(&name.name);
                        let group = self
                            .scopes
                            .last()
                            .into_iter()
                            .flat_map(|scope| scope.sources.iter())
                            .filter(|source| {
                                !source.is_target
                                    && source.addressed_as().is_some_and(|addressed| {
                                        addressed.eq_ignore_ascii_case(&name.name)
                                    })
                            })
                            .map(|source| (source.id, source.alias_seed()))
                            .collect();
                        self.table_star_groups.push(group);
                    }
                }
                ResultColumn::Star => {}
            }
        }
        if self.pass == BindPass::Rewrite && !self.aliases.is_empty() {
            self.retarget_table_stars(columns);
        }
    }

    /// `name.*` expands every item of the query addressed as `name`; when
    /// one of them now carries a synthetic alias, spell one star per item.
    fn retarget_table_stars(&self, columns: &mut Vec<ResultColumn>) {
        let Some(scope) = self.scopes.last() else {
            return;
        };
        let matching = |name: &str| -> Vec<&BindingSource> {
            scope
                .sources
                .iter()
                .filter(|source| {
                    source
                        .addressed_as()
                        .is_some_and(|addressed| addressed.eq_ignore_ascii_case(name))
                })
                .collect()
        };
        let retargets = |column: &ResultColumn| {
            matches!(column, ResultColumn::TableStar(name)
                if name.schema.is_none()
                    && matching(&name.name)
                        .iter()
                        .any(|source| self.aliases.contains_key(&source.id)))
        };
        if !columns.iter().any(retargets) {
            return;
        }
        let mut rewritten = Vec::with_capacity(columns.len() + 1);
        for column in std::mem::take(columns) {
            if retargets(&column)
                && let ResultColumn::TableStar(name) = &column
            {
                rewritten.extend(matching(&name.name).into_iter().map(|source| {
                    ResultColumn::TableStar(QualifiedName::bare(self.final_name(source)))
                }));
            } else {
                rewritten.push(column);
            }
        }
        *columns = rewritten;
    }

    /// SQLite's `resolveCompoundOrderBy` for three-part terms: each arm of a
    /// compound, left to right, resolves a term against its own FROM items
    /// alone (no enclosing query) and compares it with its result columns.
    /// The first arm where the term binds to the item and column of one of
    /// its result columns (or that has a `*` / `name.*` column) settles it,
    /// and the term is rewritten as a reference in that arm would be, so it
    /// still matches the result column after that column's rewrite. A term
    /// no arm settles keeps its spelling and fails or matches downstream as
    /// before. Called with this arm's scope innermost and `columns` not yet
    /// rewritten.
    fn compound_order_terms(
        &mut self,
        terms: &mut [OrderingTerm],
        settled: &mut [bool],
        columns: &[ResultColumn],
    ) {
        let arm = self.scopes.len() - 1;
        for (term, settled) in terms.iter_mut().zip(settled.iter_mut()) {
            if *settled {
                continue;
            }
            if let Some((leaf, is_true, span)) = bare_true_false_under_collate(&mut term.expr) {
                let col_name = if is_true { "true" } else { "false" };
                let has_col_in_arm = matches!(
                    self.lookup_unqualified_true_false_in_scope(arm, is_true),
                    TrueFalseLookup::Column
                );
                let matches_result = has_col_in_arm
                    && columns.iter().any(|result| match result {
                        ResultColumn::Star | ResultColumn::TableStar(_) => true,
                        ResultColumn::Expr { expr, .. } => match strip_collate_ref(expr) {
                            Expr::Column(col, _) => col.column.eq_ignore_ascii_case(col_name),
                            Expr::Literal(Literal::True, _) if is_true => true,
                            Expr::Literal(Literal::False, _) if !is_true => true,
                            _ => false,
                        },
                    });
                if matches_result {
                    *settled = true;
                    match self.pass {
                        BindPass::Bind => self.true_false_rewrites += 1,
                        BindPass::Rewrite => {
                            *leaf = Expr::Column(ColumnRef::bare(col_name), span);
                        }
                    }
                }
                continue;
            }
            let Some(column) = compound_order_column(&mut term.expr) else {
                continue;
            };
            let (Some(database), Some(table)) = (column.schema.clone(), column.table.clone())
            else {
                continue;
            };
            let Lookup::Bound { id, .. } =
                self.lookup_from(arm, Some(&database), &table, &column.column)
            else {
                continue;
            };
            let matches_a_result_column = columns.iter().any(|result| match result {
                ResultColumn::Star | ResultColumn::TableStar(_) => true,
                ResultColumn::Expr { expr, .. } => {
                    let expr = match expr {
                        Expr::Collate { expr, .. } => expr.as_ref(),
                        other => other,
                    };
                    let Expr::Column(result, _) = expr else {
                        return false;
                    };
                    let Some(result_table) = result.table.as_deref() else {
                        return false;
                    };
                    if !result.column.eq_ignore_ascii_case(&column.column) {
                        return false;
                    }
                    let lookup = self.lookup_from(
                        arm,
                        result.schema.as_deref(),
                        result_table,
                        &result.column,
                    );
                    matches!(lookup, Lookup::Bound { id: result_id, .. } if result_id == id)
                }
            });
            if !matches_a_result_column {
                continue;
            }
            *settled = true;
            if self.pass == BindPass::Rewrite {
                let qualifier = self.final_name(self.source(arm, id));
                column.schema = None;
                column.table = Some(Arc::from(qualifier));
            }
        }
    }

    fn limit(&mut self, limit: &mut LimitClause) {
        // SQLite evaluates LIMIT/OFFSET with no table/outer columns in scope,
        // while subqueries nested inside LIMIT push their own fresh scopes.
        let saved_scopes = std::mem::take(&mut self.scopes);
        let saved_returning = self.returning_scope.take();
        self.expr(&mut limit.limit);
        if let Some(offset) = &mut limit.offset {
            self.expr(offset);
        }
        self.scopes = saved_scopes;
        self.returning_scope = saved_returning;
    }

    fn window_spec(&mut self, spec: &mut WindowSpec) {
        for expr in &mut spec.partition_by {
            self.expr(expr);
        }
        for term in &mut spec.order_by {
            self.expr(&mut term.expr);
        }
        if let Some(frame) = &mut spec.frame {
            for bound in std::iter::once(&mut frame.start).chain(frame.end.as_mut()) {
                if let FrameBound::Preceding(expr) | FrameBound::Following(expr) = bound {
                    self.expr(expr);
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn expr(&mut self, expr: &mut Expr) {
        match expr {
            Expr::Column(column, _) => self.column(column),
            Expr::BinaryOp { left, right, .. } => {
                self.expr(left);
                self.expr(right);
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::IsNull { expr, .. } => self.expr(expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr(expr);
                self.expr(low);
                self.expr(high);
            }
            Expr::In { expr, set, .. } => {
                self.expr(expr);
                match set {
                    InSet::List(items) => {
                        for item in items {
                            self.expr(item);
                        }
                    }
                    InSet::Subquery(select) => self.select(select),
                    InSet::Table(_) => {}
                }
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.expr(expr);
                self.expr(pattern);
                if let Some(escape) = escape {
                    self.expr(escape);
                }
            }
            Expr::Case {
                operand,
                whens,
                else_expr,
                ..
            } => {
                if let Some(operand) = operand {
                    self.expr(operand);
                }
                for (when, then) in whens {
                    self.expr(when);
                    self.expr(then);
                }
                if let Some(else_expr) = else_expr {
                    self.expr(else_expr);
                }
            }
            Expr::Exists { subquery, .. } | Expr::Subquery(subquery, _) => self.select(subquery),
            Expr::FunctionCall {
                name,
                args,
                order_by,
                filter,
                over,
                ..
            } => {
                if let FunctionArgs::List(args) = args {
                    for arg in args {
                        self.expr(arg);
                    }
                }
                for term in order_by {
                    self.expr(&mut term.expr);
                }
                if let Some(filter) = filter {
                    self.expr(filter);
                }
                if let Some(over) = over {
                    self.window_spec(over);
                }
                if self.pass == BindPass::Bind
                    && let Err(error) =
                        self.catalog.validate_function_call(name, args, over.as_ref())
                {
                    self.note_non_three_part_error(error);
                }
            }
            Expr::JsonAccess { expr, path, .. } => {
                self.expr(expr);
                self.expr(path);
            }
            Expr::RowValue(items, _) => {
                for item in items {
                    self.expr(item);
                }
            }
            Expr::Literal(lit @ (Literal::True | Literal::False), span) => {
                let is_true = matches!(lit, Literal::True);
                let col_name = if is_true { "true" } else { "false" };
                let span = *span;
                match self.lookup_unqualified_true_false(is_true) {
                    TrueFalseLookup::Column => match self.pass {
                        BindPass::Bind => self.true_false_rewrites += 1,
                        BindPass::Rewrite => {
                            *expr = Expr::Column(ColumnRef::bare(col_name), span);
                        }
                    },
                    TrueFalseLookup::Alias {
                        alias_expr,
                        from_outer,
                    } => {
                        let replacement = if !from_outer
                            && self.in_where_clause
                            && expr_contains_function_call(&alias_expr)
                        {
                            // Preserve alias spelling in WHERE when the aliased
                            // expression could be an aggregate/window function so
                            // downstream diagnostics report "misuse of aliased ...".
                            Expr::Column(ColumnRef::bare(col_name), span)
                        } else {
                            *alias_expr
                        };
                        match self.pass {
                            BindPass::Bind => {
                                if replacement != *expr {
                                    self.true_false_rewrites += 1;
                                }
                            }
                            BindPass::Rewrite => {
                                *expr = replacement;
                            }
                        }
                    }
                    TrueFalseLookup::Ambiguous => {
                        if self.pass == BindPass::Bind {
                            self.fail(FrankenError::AmbiguousColumn {
                                name: col_name.to_owned(),
                            });
                        }
                    }
                    TrueFalseLookup::Literal => {}
                }
            }
            Expr::Literal(..)
            | Expr::BoundOuterValue { .. }
            | Expr::Raise { .. }
            | Expr::Placeholder(..) => {}
        }
    }

    fn column(&mut self, column: &mut ColumnRef) {
        let Some(table) = column.table.clone() else {
            if self.pass == BindPass::Bind && !self.bare_column_is_known(&column.column) {
                self.note_non_three_part_error(FrankenError::NoSuchColumn {
                    name: column.column.to_string(),
                });
            }
            return;
        };
        let Some(database) = column.schema.clone() else {
            self.two_part(column, &table);
            return;
        };
        let lookup = self.lookup(Some(&*database), &table, &column.column);
        match self.pass {
            BindPass::Bind => {
                self.three_part_refs += 1;
                let name = || format!("{database}.{table}.{}", column.column);
                match lookup {
                    Lookup::Bound { scope, id } => self.note_shared_name(scope, id),
                    Lookup::Ambiguous => {
                        self.fail_three_part(FrankenError::AmbiguousColumn { name: name() });
                    }
                    Lookup::Missing => {
                        self.fail_three_part(FrankenError::NoSuchColumn { name: name() });
                    }
                }
            }
            BindPass::Rewrite => {
                if let Lookup::Bound { scope, id } = lookup {
                    let qualifier = self.final_name(self.source(scope, id));
                    column.schema = None;
                    column.table = Some(Arc::from(qualifier));
                }
            }
        }
    }

    fn bare_column_is_known(&self, column: &str) -> bool {
        for scope in self.scopes.iter().rev() {
            for source in &scope.sources {
                match source.relation.has_column(column) {
                    Some(true) | None => return true,
                    Some(false) => {}
                }
                if source.relation.has_rowid_name(column) {
                    return true;
                }
            }
            if scope.aliases_visible && scope.has_result_alias(column) {
                return true;
            }
        }
        false
    }

    fn two_part(&mut self, column: &mut ColumnRef, table: &str) {
        let lookup = self.lookup(None, table, &column.column);
        match self.pass {
            BindPass::Bind => {
                self.take_name(table);
                match lookup {
                    Lookup::Ambiguous => self.fail(FrankenError::AmbiguousColumn {
                        name: format!("{table}.{}", column.column),
                    }),
                    // bd-cxe6r: the item lookupName chose may share its name
                    // with another item the reference sees: an inner item
                    // without the column (the reference moved outward), or a
                    // DML target beside a FROM item that has it. The other
                    // resolvers take the nearest item of the name, so the
                    // chosen one (or the capturing target's sharers) gets a
                    // synthetic alias and the reference follows it.
                    Lookup::Bound { scope, id } if self.nearest_item_misleads(scope, id, table) => {
                        self.note_shared_name(scope, id);
                    }
                    Lookup::Bound { .. } => {}
                    Lookup::Missing => {
                        if !(self.in_upsert_update && table.eq_ignore_ascii_case("excluded")) {
                            self.note_non_three_part_error(FrankenError::NoSuchColumn {
                                name: format!("{table}.{}", column.column),
                            });
                        }
                    }
                }
            }
            BindPass::Rewrite => {
                if let Lookup::Bound { id, .. } = lookup
                    && let Some(alias) = self.aliases.get(&id)
                {
                    column.table = Some(Arc::from(alias.as_str()));
                }
            }
        }
    }

    /// SQLite's `lookupName` for a qualified column: search each query from
    /// the innermost outward; in a query, count the items in `database` (when
    /// given) addressed as `table` that have `column`. An item joined with
    /// USING / NATURAL on `column` does not count again, and a lone item
    /// addressed as `table` supplies its hidden rowid.
    fn lookup(&self, database: Option<&str>, table: &str, column: &str) -> Lookup {
        self.lookup_from(0, database, table, column)
    }

    /// [`Self::lookup`] over the queries from `outermost` inward only.
    fn lookup_from(
        &self,
        outermost: usize,
        database: Option<&str>,
        table: &str,
        column: &str,
    ) -> Lookup {
        let database = match database {
            Some(qualifier) => match self.catalog.database_named(qualifier) {
                Some(database) => Some(database),
                None => return Lookup::Missing,
            },
            None => None,
        };
        for (scope_index, scope) in self.scopes.iter().enumerate().skip(outermost).rev() {
            if database.is_some() && self.returning_scope == Some(scope_index) {
                continue;
            }
            let mut definite = 0_usize;
            let mut found = None;
            let mut unknown = None;
            let mut addressed = Vec::new();
            for source in &scope.sources {
                if let Some(database) = database.as_deref()
                    && source.relation.database.as_deref() != Some(database)
                {
                    continue;
                }
                if !source
                    .addressed_as()
                    .is_some_and(|name| name.eq_ignore_ascii_case(table))
                {
                    continue;
                }
                addressed.push(source);
                match source.relation.has_column(column) {
                    Some(true) => {
                        if definite > 0 && source.coalesces(column) {
                            continue;
                        }
                        definite += 1;
                        found.get_or_insert(source.id);
                    }
                    Some(false) => {}
                    None => {
                        unknown.get_or_insert(source.id);
                    }
                }
            }
            if definite > 1 {
                return Lookup::Ambiguous;
            }
            if let Some(id) = found.or(unknown) {
                return Lookup::Bound {
                    scope: scope_index,
                    id,
                };
            }
            if let [only] = addressed.as_slice()
                && only.relation.has_rowid_name(column)
            {
                return Lookup::Bound {
                    scope: scope_index,
                    id: only.id,
                };
            }
        }
        Lookup::Missing
    }

    fn lookup_unqualified_true_false_in_scope(
        &self,
        scope_index: usize,
        is_true: bool,
    ) -> TrueFalseLookup {
        let Some(scope) = self.scopes.get(scope_index) else {
            return TrueFalseLookup::Literal;
        };
        let col_name = if is_true { "true" } else { "false" };
        let mut definite = 0_usize;
        for source in &scope.sources {
            if source.relation.exposes_true_or_false(is_true) {
                if definite > 0 && source.coalesces(col_name) {
                    continue;
                }
                definite += 1;
            }
        }
        if definite > 1 {
            return TrueFalseLookup::Ambiguous;
        }
        if definite == 1 {
            return TrueFalseLookup::Column;
        }
        if scope.aliases_visible
            && let Some(alias_expr) = scope.alias_for(is_true)
        {
            let from_outer = scope_index + 1 < self.scopes.len();
            return TrueFalseLookup::Alias {
                alias_expr: Box::new(alias_expr.clone()),
                from_outer,
            };
        }
        TrueFalseLookup::Literal
    }

    fn lookup_unqualified_true_false(&self, is_true: bool) -> TrueFalseLookup {
        for scope_index in (0..self.scopes.len()).rev() {
            match self.lookup_unqualified_true_false_in_scope(scope_index, is_true) {
                TrueFalseLookup::Literal => {}
                found => return found,
            }
        }
        TrueFalseLookup::Literal
    }

    fn source(&self, scope: usize, id: usize) -> &BindingSource {
        self.scopes[scope]
            .sources
            .iter()
            .find(|source| source.id == id)
            .expect("a bound source belongs to the scope it was found in")
    }

    /// The qualifier that addresses `source` after the rewrite.
    fn final_name(&self, source: &BindingSource) -> String {
        self.aliases
            .get(&source.id)
            .cloned()
            .or_else(|| source.addressed_as().map(str::to_owned))
            .unwrap_or_default()
    }

    /// A three-part reference bound to `id` in scope `scope`. Its two-part
    /// form is only unambiguous when no other item visible at the reference
    /// (in that scope or any query nested between) answers to the same name;
    /// otherwise the item needs a synthetic alias.
    /// bd-cxe6r: whether a resolver that takes the nearest item addressed as
    /// `table` would miss the item `table.column` bound to (`scope`, `id`):
    /// an inner query has an item of the name (without the column), or the
    /// bound item is a FROM item beside a DML target of the name.
    fn nearest_item_misleads(&self, scope: usize, id: usize, table: &str) -> bool {
        let addressed = |source: &BindingSource| {
            source
                .addressed_as()
                .is_some_and(|name| name.eq_ignore_ascii_case(table))
        };
        let nearest = self
            .scopes
            .iter()
            .rposition(|s| s.sources.iter().any(addressed));
        if nearest != Some(scope) {
            return true;
        }
        let bound_is_target = self.source(scope, id).is_target;
        !bound_is_target
            && self.scopes[scope]
                .sources
                .iter()
                .any(|source| source.is_target && addressed(source))
    }

    fn note_shared_name(&mut self, scope: usize, id: usize) {
        let source = self.source(scope, id);
        let Some(name) = source.addressed_as() else {
            return;
        };
        let sharers: Vec<(usize, (String, String))> = self.scopes[scope..]
            .iter()
            .flat_map(|s| s.sources.iter())
            .filter(|other| {
                other.id != id
                    && other
                        .addressed_as()
                        .is_some_and(|other_name| other_name.eq_ignore_ascii_case(name))
            })
            .map(|other| (other.id, other.alias_seed()))
            .collect();
        if sharers.is_empty() {
            return;
        }
        if source.is_target {
            // Rename the items that would capture the reference instead.
            for (other, seed) in sharers {
                self.needs_alias.entry(other).or_insert(seed);
            }
        } else {
            let seed = source.alias_seed();
            self.needs_alias.entry(id).or_insert(seed);
        }
    }
}

/// The three-part column reference a compound's ORDER BY term is, under any
/// COLLATE (SQLite skips those before matching a term to a result column).
fn compound_order_column(expr: &mut Expr) -> Option<&mut ColumnRef> {
    match expr {
        Expr::Column(column, _) if column.schema.is_some() => Some(column),
        Expr::Collate { expr, .. } => compound_order_column(expr),
        _ => None,
    }
}

fn strip_collate_ref(mut expr: &Expr) -> &Expr {
    while let Expr::Collate { expr: inner, .. } = expr {
        expr = inner.as_ref();
    }
    expr
}

fn bare_true_false_under_collate(expr: &mut Expr) -> Option<(&mut Expr, bool, Span)> {
    match expr {
        Expr::Collate { expr: inner, .. } => bare_true_false_under_collate(inner),
        Expr::Literal(Literal::True, span) => {
            let span = *span;
            Some((expr, true, span))
        }
        Expr::Literal(Literal::False, span) => {
            let span = *span;
            Some((expr, false, span))
        }
        _ => None,
    }
}

fn collect_result_aliases(columns: &[ResultColumn]) -> Vec<String> {
    columns
        .iter()
        .filter_map(|column| match column {
            ResultColumn::Expr {
                alias: Some(alias), ..
            } => Some(alias.clone()),
            _ => None,
        })
        .collect()
}

fn collect_true_false_result_aliases(columns: &[ResultColumn]) -> (Option<Expr>, Option<Expr>) {
    let mut true_alias = None;
    let mut false_alias = None;
    for column in columns {
        let ResultColumn::Expr {
            expr,
            alias: Some(alias),
        } = column
        else {
            continue;
        };
        if true_alias.is_none() && alias.eq_ignore_ascii_case("true") {
            true_alias = Some(expr.clone());
        } else if false_alias.is_none() && alias.eq_ignore_ascii_case("false") {
            false_alias = Some(expr.clone());
        }
    }
    (true_alias, false_alias)
}

// ---------------------------------------------------------------------------
// Cheap syntactic gate. Binding can change a statement only when it has a
// three-part column reference (to rewrite or reject), or when one query's
// FROM items share a name and at least one of them is database-qualified (a
// two-part reference to that name is then ambiguous across databases). Every
// other statement skips the clone and the catalog lookups.
// ---------------------------------------------------------------------------

/// Whether `statement` has a three-part column reference, or a query whose
/// FROM items (with an UPDATE's target) share a name one of them qualifies.
/// bd-x5eqm: the columns a CTE exposes, when the binder can name them: its
/// column list, or else its first core's result names where each result is a
/// plain column or carries an alias. `None` (every column taken to exist)
/// otherwise. With the columns known, `c.x` over a CTE `c` and a table also
/// addressed as `c` that both have `x` is ambiguous, as in stock's
/// `lookupName`; a recursive arm's `FROM c, main.c` reached no other check.
fn cte_known_columns(cte: &fsqlite_ast::Cte) -> Option<Vec<String>> {
    if !cte.columns.is_empty() {
        return Some(cte.columns.clone());
    }
    let SelectCore::Select { columns, .. } = &cte.query.body.select else {
        return None;
    };
    columns
        .iter()
        .map(|column| match column {
            ResultColumn::Expr {
                alias: Some(alias), ..
            } => Some(alias.clone()),
            ResultColumn::Expr {
                expr: Expr::Column(column, _),
                alias: None,
            } => Some(column.column.to_string()),
            ResultColumn::Expr { .. } | ResultColumn::Star | ResultColumn::TableStar(_) => None,
        })
        .collect()
}

/// bd-cxe6r: whether a query nested in `statement` has a FROM item addressed
/// by a name an item of an enclosing query also has. A qualified reference
/// there can bind to the enclosing item (lookupName moves outward when the
/// inner item lacks the column), which only the binder tells apart.
fn statement_shares_a_name_across_queries(statement: &Statement) -> bool {
    let mut enclosing = Vec::new();
    match statement {
        Statement::Select(select) => select_shares_a_name(select, &mut enclosing),
        Statement::Update(update) => {
            enclosing.push(
                update
                    .table
                    .alias
                    .as_deref()
                    .unwrap_or(&update.table.name.name)
                    .to_ascii_lowercase(),
            );
            if let Some(from) = &update.from {
                let mut names = Vec::new();
                collect_from_names(from, &mut names);
                enclosing.extend(names.iter().map(|(name, _)| name.to_ascii_lowercase()));
            }
            update
                .assignments
                .iter()
                .map(|assignment| &assignment.value)
                .chain(update.where_clause.iter())
                .any(|expr| expr_shares_a_name(expr, &mut enclosing))
        }
        Statement::Delete(delete) => {
            enclosing.push(
                delete
                    .table
                    .alias
                    .as_deref()
                    .unwrap_or(&delete.table.name.name)
                    .to_ascii_lowercase(),
            );
            delete
                .where_clause
                .as_ref()
                .is_some_and(|expr| expr_shares_a_name(expr, &mut enclosing))
        }
        _ => false,
    }
}

fn select_shares_a_name(select: &SelectStatement, enclosing: &mut Vec<String>) -> bool {
    // A CTE body sees no enclosing query's items.
    if select.with.as_ref().is_some_and(|with| {
        with.ctes
            .iter()
            .any(|cte| select_shares_a_name(&cte.query, &mut Vec::new()))
    }) {
        return true;
    }
    std::iter::once(&select.body.select)
        .chain(select.body.compounds.iter().map(|(_, core)| core))
        .any(|core| core_shares_a_name(core, enclosing))
}

fn core_shares_a_name(core: &SelectCore, enclosing: &mut Vec<String>) -> bool {
    let SelectCore::Select {
        columns,
        from,
        where_clause,
        having,
        ..
    } = core
    else {
        return false;
    };
    let mut names = Vec::new();
    if let Some(from) = from {
        collect_from_names(from, &mut names);
        if names
            .iter()
            .any(|(name, _)| enclosing.iter().any(|outer| outer.eq_ignore_ascii_case(name)))
        {
            return true;
        }
        // A FROM subquery sees the enclosing queries, not its siblings.
        if from_subqueries_share_a_name(from, enclosing) {
            return true;
        }
    }
    let depth = enclosing.len();
    enclosing.extend(names.iter().map(|(name, _)| name.to_ascii_lowercase()));
    let shares = columns.iter().any(|column| match column {
        ResultColumn::Expr { expr, .. } => expr_shares_a_name(expr, enclosing),
        ResultColumn::Star | ResultColumn::TableStar(_) => false,
    }) || where_clause
        .as_deref()
        .into_iter()
        .chain(having.as_deref())
        .any(|expr| expr_shares_a_name(expr, enclosing));
    enclosing.truncate(depth);
    shares
}

fn from_subqueries_share_a_name(from: &FromClause, enclosing: &mut Vec<String>) -> bool {
    std::iter::once(&from.source)
        .chain(from.joins.iter().map(|join| &join.table))
        .any(|source| match source {
            TableOrSubquery::Subquery { query, .. } => select_shares_a_name(query, enclosing),
            TableOrSubquery::ParenJoin(inner) => from_subqueries_share_a_name(inner, enclosing),
            TableOrSubquery::Table { .. } | TableOrSubquery::TableFunction { .. } => false,
        })
}

/// Whether a subquery of `expr` shares a name with `enclosing` (the queries
/// around it, innermost last).
fn expr_shares_a_name(expr: &Expr, enclosing: &mut Vec<String>) -> bool {
    expr_contains_subquery_match(expr, &mut |subquery| {
        let select = match subquery {
            SubqueryExprRef::Scalar(select)
            | SubqueryExprRef::Exists(select)
            | SubqueryExprRef::In(select) => select,
        };
        select_shares_a_name(select, enclosing)
    })
}

fn statement_needs_binding(statement: &Statement) -> bool {
    match statement {
        Statement::Select(select) => select_needs_binding(select),
        Statement::Insert(insert) => {
            with_needs_binding(insert.with.as_ref())
                || match &insert.source {
                    InsertSource::Select(select) => select_needs_binding(select),
                    InsertSource::Values(rows) => rows.iter().flatten().any(expr_needs_binding),
                    InsertSource::DefaultValues => false,
                }
                || insert.upsert.iter().any(|upsert| {
                    upsert
                        .target
                        .as_ref()
                        .and_then(|target| target.where_clause.as_ref())
                        .is_some_and(expr_needs_binding)
                        || matches!(&upsert.action, UpsertAction::Update {
                            assignments,
                            where_clause,
                        } if assignments.iter().any(|a| expr_needs_binding(&a.value))
                            || where_clause.as_deref().is_some_and(expr_needs_binding))
                })
                || result_columns_need_binding(&insert.returning)
        }
        Statement::Update(update) => {
            let target = (
                update
                    .table
                    .alias
                    .as_deref()
                    .unwrap_or(&update.table.name.name),
                update.table.name.schema.is_some(),
            );
            with_needs_binding(update.with.as_ref())
                || update
                    .from
                    .as_ref()
                    .is_some_and(|from| from_needs_binding(from, Some(target)))
                || update
                    .assignments
                    .iter()
                    .any(|assignment| expr_needs_binding(&assignment.value))
                || update.where_clause.as_ref().is_some_and(expr_needs_binding)
                || result_columns_need_binding(&update.returning)
                || update
                    .order_by
                    .iter()
                    .any(|term| expr_needs_binding(&term.expr))
                || update.limit.as_ref().is_some_and(limit_needs_binding)
        }
        Statement::Delete(delete) => {
            with_needs_binding(delete.with.as_ref())
                || delete.where_clause.as_ref().is_some_and(expr_needs_binding)
                || result_columns_need_binding(&delete.returning)
                || delete
                    .order_by
                    .iter()
                    .any(|term| expr_needs_binding(&term.expr))
                || delete.limit.as_ref().is_some_and(limit_needs_binding)
        }
        Statement::CreateTable(create) => {
            matches!(&create.body, CreateTableBody::AsSelect(select) if select_needs_binding(select))
        }
        Statement::Explain { stmt, .. } => statement_needs_binding(stmt),
        _ => false,
    }
}

fn with_needs_binding(with: Option<&fsqlite_ast::WithClause>) -> bool {
    with.is_some_and(|with| with.ctes.iter().any(|cte| select_needs_binding(&cte.query)))
}

fn select_needs_binding(select: &SelectStatement) -> bool {
    with_needs_binding(select.with.as_ref())
        || core_needs_binding(&select.body.select)
        || select
            .body
            .compounds
            .iter()
            .any(|(_, core)| core_needs_binding(core))
        || select
            .order_by
            .iter()
            .any(|term| expr_needs_binding(&term.expr))
        || select.limit.as_ref().is_some_and(limit_needs_binding)
}

fn limit_needs_binding(limit: &LimitClause) -> bool {
    expr_needs_binding(&limit.limit) || limit.offset.as_ref().is_some_and(expr_needs_binding)
}

fn core_needs_binding(core: &SelectCore) -> bool {
    match core {
        SelectCore::Values(rows) => rows.iter().flatten().any(expr_needs_binding),
        SelectCore::Select {
            columns,
            from,
            where_clause,
            group_by,
            having,
            windows,
            ..
        } => {
            result_columns_need_binding(columns)
                || from
                    .as_ref()
                    .is_some_and(|from| from_needs_binding(from, None))
                || where_clause.as_deref().is_some_and(expr_needs_binding)
                || group_by.iter().any(expr_needs_binding)
                || having.as_deref().is_some_and(expr_needs_binding)
                || windows
                    .iter()
                    .any(|window| window_needs_binding(&window.spec))
        }
    }
}

fn result_columns_need_binding(columns: &[ResultColumn]) -> bool {
    columns.iter().any(|column| match column {
        ResultColumn::Expr { expr, .. } => expr_needs_binding(expr),
        ResultColumn::Star | ResultColumn::TableStar(_) => false,
    })
}

/// One query's FROM clause, plus an UPDATE's target when it shares the
/// query: `(addressed-as name, database-qualified)`.
fn from_needs_binding(from: &FromClause, target: Option<(&str, bool)>) -> bool {
    // A shared name matters only when one of its items is qualified; without
    // any qualified item, skip collecting the names (no allocation).
    let any_qualified =
        target.is_some_and(|(_, qualified)| qualified) || from_has_qualified_item(from);
    if any_qualified {
        let mut names: Vec<(&str, bool)> = target.into_iter().collect();
        collect_from_names(from, &mut names);
        let shares_a_qualified_name = names.iter().enumerate().any(|(index, (name, qualified))| {
            names[index + 1..].iter().any(|(other, other_qualified)| {
                (*qualified || *other_qualified) && name.eq_ignore_ascii_case(other)
            })
        });
        if shares_a_qualified_name {
            return true;
        }
    }
    from_parts_need_binding(from)
}

/// Whether a FROM clause names a table with a database qualifier.
fn from_has_qualified_item(from: &FromClause) -> bool {
    std::iter::once(&from.source)
        .chain(from.joins.iter().map(|join| &join.table))
        .any(|source| match source {
            TableOrSubquery::Table { name, .. } => name.schema.is_some(),
            TableOrSubquery::ParenJoin(inner) => from_has_qualified_item(inner),
            TableOrSubquery::Subquery { .. } | TableOrSubquery::TableFunction { .. } => false,
        })
}

fn collect_from_names<'a>(from: &'a FromClause, names: &mut Vec<(&'a str, bool)>) {
    for source in std::iter::once(&from.source).chain(from.joins.iter().map(|join| &join.table)) {
        match source {
            TableOrSubquery::Table { name, alias, .. } => {
                names.push((
                    alias.as_deref().unwrap_or(&name.name),
                    name.schema.is_some(),
                ));
            }
            TableOrSubquery::Subquery { alias, .. } => {
                if let Some(alias) = alias.as_deref() {
                    names.push((alias, false));
                }
            }
            TableOrSubquery::TableFunction { name, alias, .. } => {
                names.push((alias.as_deref().unwrap_or(name), false));
            }
            TableOrSubquery::ParenJoin(inner) => collect_from_names(inner, names),
        }
    }
}

/// Subqueries, ON constraints and table-function arguments of a FROM clause.
fn from_parts_need_binding(from: &FromClause) -> bool {
    source_parts_need_binding(&from.source)
        || from.joins.iter().any(|join| {
            source_parts_need_binding(&join.table)
                || matches!(&join.constraint, Some(JoinConstraint::On(expr)) if expr_needs_binding(expr))
        })
}

fn source_parts_need_binding(source: &TableOrSubquery) -> bool {
    match source {
        TableOrSubquery::Table { .. } => false,
        TableOrSubquery::Subquery { query, .. } => select_needs_binding(query),
        TableOrSubquery::TableFunction { args, .. } => args.iter().any(expr_needs_binding),
        TableOrSubquery::ParenJoin(inner) => from_parts_need_binding(inner),
    }
}

fn window_needs_binding(spec: &WindowSpec) -> bool {
    spec.partition_by.iter().any(expr_needs_binding)
        || spec
            .order_by
            .iter()
            .any(|term| expr_needs_binding(&term.expr))
        || spec.frame.as_ref().is_some_and(|frame| {
            std::iter::once(&frame.start)
                .chain(frame.end.as_ref())
                .any(|bound| match bound {
                    FrameBound::Preceding(expr) | FrameBound::Following(expr) => {
                        expr_needs_binding(expr)
                    }
                    FrameBound::UnboundedPreceding
                    | FrameBound::CurrentRow
                    | FrameBound::UnboundedFollowing => false,
                })
        })
}

fn expr_needs_binding(expr: &Expr) -> bool {
    match expr {
        Expr::Column(column, _) => column.schema.is_some(),
        Expr::Literal(Literal::True | Literal::False, _) => true,
        Expr::BinaryOp { left, right, .. } => expr_needs_binding(left) || expr_needs_binding(right),
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::IsNull { expr, .. } => expr_needs_binding(expr),
        Expr::Between {
            expr, low, high, ..
        } => expr_needs_binding(expr) || expr_needs_binding(low) || expr_needs_binding(high),
        Expr::In { expr, set, .. } => {
            expr_needs_binding(expr)
                || match set {
                    InSet::List(items) => items.iter().any(expr_needs_binding),
                    InSet::Subquery(select) => select_needs_binding(select),
                    InSet::Table(_) => false,
                }
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_needs_binding(expr)
                || expr_needs_binding(pattern)
                || escape.as_deref().is_some_and(expr_needs_binding)
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            operand.as_deref().is_some_and(expr_needs_binding)
                || whens
                    .iter()
                    .any(|(when, then)| expr_needs_binding(when) || expr_needs_binding(then))
                || else_expr.as_deref().is_some_and(expr_needs_binding)
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery, _) => {
            select_needs_binding(subquery)
        }
        Expr::FunctionCall {
            args,
            order_by,
            filter,
            over,
            ..
        } => {
            matches!(args, FunctionArgs::List(args) if args.iter().any(expr_needs_binding))
                || order_by.iter().any(|term| expr_needs_binding(&term.expr))
                || filter.as_deref().is_some_and(expr_needs_binding)
                || over.as_ref().is_some_and(window_needs_binding)
        }
        Expr::JsonAccess { expr, path, .. } => expr_needs_binding(expr) || expr_needs_binding(path),
        Expr::RowValue(items, _) => items.iter().any(expr_needs_binding),
        Expr::Literal(..)
        | Expr::BoundOuterValue { .. }
        | Expr::Raise { .. }
        | Expr::Placeholder(..) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MAIN `users(name)` and `t(x)`, TEMP `users(name)` and `tonly(x)`,
    /// attached `aux.users(name)`.
    struct Catalog;

    impl QualifiedColumnCatalog for Catalog {
        fn database_named(&self, schema: &str) -> Option<String> {
            let lower = schema.to_ascii_lowercase();
            ["main", "temp", "aux"]
                .contains(&lower.as_str())
                .then_some(lower)
        }

        fn relation(&self, schema: Option<&str>, name: &str) -> Option<QualifiedColumnRelation> {
            let tables: &[(&str, &str, &str)] = &[
                ("temp", "users", "name"),
                ("temp", "tonly", "x"),
                ("main", "users", "name"),
                ("main", "t", "x"),
                ("aux", "users", "name"),
            ];
            tables
                .iter()
                .find(|(database, table, _)| {
                    table.eq_ignore_ascii_case(name)
                        && schema.is_none_or(|schema| schema.eq_ignore_ascii_case(database))
                })
                .map(|(database, _, column)| QualifiedColumnRelation {
                    database: Some((*database).to_owned()),
                    columns: Some(vec![(*column).to_owned()]),
                    rowid_names: vec!["rowid", "_rowid_", "oid"],
                    exposes_true: column.eq_ignore_ascii_case("true"),
                    exposes_false: column.eq_ignore_ascii_case("false"),
                })
        }
    }

    fn bind(sql: &str) -> Result<Option<String>> {
        let statement = parse_single_statement(sql)?;
        if !statement_needs_binding(&statement)
            && !statement_shares_a_name_across_queries(&statement)
        {
            return Ok(None);
        }
        bind_schema_qualified_columns_with(&Catalog, &statement)
            .map(|bound| bound.map(|statement| statement.to_string()))
    }

    fn bind_error(sql: &str) -> String {
        bind(sql).expect_err("binding should fail").to_string()
    }

    #[test]
    fn same_name_items_of_two_databases_get_distinct_aliases() {
        let bound = bind("SELECT main.users.name, temp.users.name FROM main.users JOIN temp.users")
            .expect("binds")
            .expect("rewritten");
        let reparsed = parse_single_statement(&bound).expect("rewritten SQL reparses");
        assert_eq!(reparsed.to_string(), bound);
        // Display quotes the keyword `temp`.
        assert_eq!(
            bound,
            r#"SELECT "users@main".name, "users@temp".name FROM main.users AS "users@main" INNER JOIN "temp".users AS "users@temp""#
        );
    }

    #[test]
    fn unshared_name_only_drops_the_database() {
        assert_eq!(
            bind("SELECT main.t.x FROM t").expect("binds").as_deref(),
            Some("SELECT t.x FROM t")
        );
        assert_eq!(
            bind("SELECT main.m.name FROM main.users AS m")
                .expect("binds")
                .as_deref(),
            Some("SELECT m.name FROM main.users AS m")
        );
    }

    #[test]
    fn correlated_reference_aliases_the_outer_item_it_binds() {
        let bound = bind("SELECT (SELECT main.users.name FROM temp.users) FROM main.users")
            .expect("binds")
            .expect("rewritten");
        assert_eq!(
            bound,
            r#"SELECT (SELECT "users@main".name FROM "temp".users) FROM main.users AS "users@main""#
        );
    }

    #[test]
    fn a_two_part_reference_to_an_aliased_item_follows_the_alias() {
        let bound = bind(
            "SELECT users.name FROM temp.users WHERE EXISTS \
             (SELECT 1 FROM main.users WHERE temp.users.name = 't')",
        )
        .expect("binds")
        .expect("rewritten");
        assert_eq!(
            bound,
            r#"SELECT "users@temp".name FROM "temp".users AS "users@temp" WHERE EXISTS (SELECT 1 FROM main.users WHERE "users@temp".name = 't')"#
        );
    }

    #[test]
    fn a_dml_target_keeps_its_name_and_the_capturing_item_is_aliased() {
        let bound = bind(
            "UPDATE main.users SET name = \
             (SELECT main.users.name || temp.users.name FROM temp.users)",
        )
        .expect("binds")
        .expect("rewritten");
        assert_eq!(
            bound,
            r#"UPDATE main.users SET name = (SELECT users.name || "users@temp".name FROM "temp".users AS "users@temp")"#
        );
    }

    #[test]
    fn a_table_star_over_an_aliased_item_is_spelled_per_item() {
        let bound = bind("SELECT users.*, main.users.name FROM main.users JOIN temp.users")
            .expect("binds")
            .expect("rewritten");
        assert_eq!(
            bound,
            r#"SELECT "users@main".*, "users@temp".*, "users@main".name FROM main.users AS "users@main" INNER JOIN "temp".users AS "users@temp""#
        );
    }

    /// bd-x4g7x: `name.*` over several same-named items expands each of
    /// them, with no three-part reference needed to force the aliases.
    #[test]
    fn a_table_star_over_same_named_items_is_spelled_per_item() {
        let bound = bind("SELECT users.* FROM main.users JOIN temp.users")
            .expect("binds")
            .expect("rewritten");
        assert_eq!(
            bound,
            r#"SELECT "users@main".*, "users@temp".* FROM main.users AS "users@main" INNER JOIN "temp".users AS "users@temp""#
        );
    }

    #[test]
    fn a_compound_order_by_term_follows_the_result_column_it_matches() {
        // The first arm's result column is rewritten to the synthetic alias;
        // the term naming the same item and column follows it.
        let bound = bind(
            "SELECT main.users.name FROM main.users JOIN temp.users \
             UNION ALL SELECT 'z' ORDER BY main.users.name",
        )
        .expect("binds")
        .expect("rewritten");
        assert!(bound.ends_with(r#"ORDER BY "users@main".name"#), "{bound}");
        // A term the first arm binds but none of its result columns names is
        // matched against the next arm, as resolveCompoundOrderBy does.
        let bound = bind(
            "SELECT main.users.name FROM main.users JOIN temp.users \
             UNION SELECT temp.users.name FROM temp.users ORDER BY temp.users.name",
        )
        .expect("binds")
        .expect("rewritten");
        assert!(bound.ends_with("ORDER BY users.name"), "{bound}");
        // A term no arm binds keeps its spelling.
        let bound = bind(
            "SELECT main.users.name FROM main.users JOIN temp.users \
             UNION SELECT 'z' ORDER BY aux.users.name",
        )
        .expect("binds")
        .expect("rewritten");
        assert!(bound.ends_with("ORDER BY aux.users.name"), "{bound}");
    }

    #[test]
    fn returning_never_matches_a_database_qualified_reference() {
        assert_eq!(
            bind_error("UPDATE main.users SET name = 'x' RETURNING main.users.name"),
            "no such column: main.users.name"
        );
        assert_eq!(
            bind_error("DELETE FROM temp.users RETURNING temp.users.name"),
            "no such column: temp.users.name"
        );
        assert_eq!(
            bind_error("INSERT INTO main.t VALUES (1) RETURNING main.t.x"),
            "no such column: main.t.x"
        );
        // Table-qualified references are not affected.
        assert_eq!(
            bind("UPDATE main.users SET name = 'x' RETURNING users.name").expect("binds"),
            None
        );
    }

    #[test]
    fn database_mismatch_and_unknown_database_are_no_such_column() {
        assert_eq!(
            bind_error("SELECT temp.t.x FROM t"),
            "no such column: temp.t.x"
        );
        assert_eq!(
            bind_error("SELECT main.users.name FROM users"),
            "no such column: main.users.name"
        );
        assert_eq!(
            bind_error("SELECT main.tonly.x FROM tonly"),
            "no such column: main.tonly.x"
        );
        assert_eq!(
            bind_error("SELECT nosuch.users.name FROM users"),
            "no such column: nosuch.users.name"
        );
        assert_eq!(
            bind_error("SELECT main.users.name FROM main.users AS m"),
            "no such column: main.users.name"
        );
        assert_eq!(
            bind_error("UPDATE temp.users SET name = main.users.name"),
            "no such column: main.users.name"
        );
    }

    #[test]
    fn same_name_items_make_a_table_qualifier_ambiguous() {
        assert_eq!(
            bind_error("SELECT users.name FROM main.users JOIN temp.users"),
            "ambiguous column name: users.name"
        );
        assert_eq!(
            bind_error("SELECT users.name FROM main.users, aux.users"),
            "ambiguous column name: users.name"
        );
        assert_eq!(
            bind("SELECT users.name FROM main.users JOIN temp.users USING (name)")
                .expect("USING coalesces the column"),
            None
        );
    }

    #[test]
    fn a_cte_with_known_columns_shares_a_table_qualifier() {
        // bd-x5eqm: a recursive arm's `FROM t, main.t`, a column list or an
        // alias naming the CTE's columns.
        assert_eq!(
            bind_error(
                "WITH RECURSIVE t(x) AS (SELECT 1 UNION ALL \
                 SELECT t.x + 1 FROM t, main.t WHERE t.x < 3) SELECT x FROM t"
            ),
            "ambiguous column name: t.x"
        );
        assert_eq!(
            bind_error("WITH t AS (SELECT 1 AS x) SELECT t.x FROM main.t, t"),
            "ambiguous column name: t.x"
        );
        // A CTE without the column leaves the qualifier to the table; one
        // whose column names are not known here is taken to have every
        // column, so the table's still binds.
        assert_eq!(
            bind("WITH t(y) AS (SELECT 1) SELECT t.x FROM t, main.t").expect("binds"),
            None
        );
        assert_eq!(
            bind("WITH t AS (SELECT 1 + 1) SELECT t.x FROM t, main.t").expect("binds"),
            None
        );
    }

    #[test]
    fn a_qualifier_whose_inner_item_lacks_the_column_binds_the_enclosing_item() {
        // bd-cxe6r: the inner `users AS t` has no x, so lookupName moves out
        // to the enclosing `t`, which gets a synthetic alias.
        assert_eq!(
            bind("SELECT (SELECT t.x FROM users AS t) FROM t")
                .expect("binds")
                .as_deref(),
            Some(r#"SELECT (SELECT "t@main".x FROM users AS t) FROM t AS "t@main""#)
        );
        // The inner item has the column: it binds there, and nothing moves.
        assert_eq!(
            bind("SELECT (SELECT t.x FROM t) FROM t").expect("binds"),
            None
        );
        assert_eq!(
            bind("SELECT (SELECT u.name FROM users AS u) FROM t AS u").expect("binds"),
            None
        );
    }

    #[test]
    fn statements_without_a_database_name_or_with_a_missing_table_are_left_alone() {
        assert_eq!(bind("SELECT users.name FROM users").expect("binds"), None);
        assert_eq!(
            bind("SELECT temp.t.x FROM t JOIN nosuch").expect("binds"),
            None
        );
    }
}
