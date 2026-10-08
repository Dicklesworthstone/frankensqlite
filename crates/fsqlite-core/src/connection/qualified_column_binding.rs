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
        }
    }

    fn opaque(database: &str, columns: Option<Vec<String>>) -> Self {
        Self {
            database: Some(database.to_owned()),
            columns,
            rowid_names: Vec::new(),
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
}

/// Catalog facts the binder looks up.
trait QualifiedColumnCatalog {
    /// Lowercased name of the open database a qualifier names, if any.
    fn database_named(&self, schema: &str) -> Option<String>;

    /// The relation `[schema.]name` names. An unqualified name is searched
    /// in TEMP, then MAIN, then each attached database in attach order, as
    /// SQLite does. `None` when there is no such relation.
    fn relation(&self, schema: Option<&str>, name: &str) -> Option<QualifiedColumnRelation>;
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
            self.local_column_relation(name, PragmaSchemaScope::Main)
        } else if schema.eq_ignore_ascii_case("temp") {
            self.local_column_relation(name, PragmaSchemaScope::Temp)
        } else {
            self.attached_column_relation(schema, name)
        }
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
        let bound = if statement_needs_binding(statement) {
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
                return Some(QualifiedColumnRelation::opaque(database, None));
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
        self.views
            .borrow()
            .get(index)
            .map(|view| QualifiedColumnRelation::opaque(database, view_output_column_names(view)))
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
    // bd-x4g7x: a `name.*` over several same-named items is rewritten one
    // star per item even when no three-part reference forces the aliases.
    let splits_table_star = binder
        .table_star_groups
        .iter()
        .any(|group| group.len() > 1);
    if binder.three_part_refs == 0 && !splits_table_star {
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum BindPass {
    /// Resolve every qualified reference, record errors, and find the items
    /// that need a synthetic alias. Does not modify the statement.
    Bind,
    /// Apply the synthetic aliases and rewrite the references.
    Rewrite,
}

struct QualifiedColumnBinder<'a> {
    catalog: &'a dyn QualifiedColumnCatalog,
    pass: BindPass,
    /// FROM items of each enclosing query, outermost first.
    scopes: Vec<Vec<BindingSource>>,
    /// Lowercased CTE names in scope, per WITH clause.
    ctes: Vec<Vec<String>>,
    next_id: usize,
    three_part_refs: usize,
    unresolved_relation: bool,
    error: Option<FrankenError>,
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
            unresolved_relation: false,
            error: None,
            needs_alias: BTreeMap::new(),
            table_star_groups: Vec::new(),
            aliases: HashMap::new(),
            taken_names: HashSet::new(),
            returning_scope: None,
        }
    }

    fn begin_pass(&mut self, pass: BindPass) {
        self.pass = pass;
        self.scopes.clear();
        self.ctes.clear();
        self.next_id = 0;
    }

    fn fail(&mut self, error: FrankenError) {
        if self.error.is_none() {
            self.error = Some(error);
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
        let names: Vec<String> = with
            .ctes
            .iter()
            .map(|cte| cte.name.to_ascii_lowercase())
            .collect();
        for name in &names {
            self.take_name(name);
        }
        self.ctes.push(names);
        for cte in &mut with.ctes {
            self.select(&mut cte.query);
        }
        true
    }

    fn is_cte(&self, name: &str) -> bool {
        self.ctes
            .iter()
            .flatten()
            .any(|cte| cte.eq_ignore_ascii_case(name))
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
                .map(|term| compound_order_column(&mut term.expr).is_none())
                .collect();
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
        let mut scope = Vec::new();
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
                    self.register_from(from, &mut scope);
                }
                self.scopes.push(scope);
                // Before the result columns are rewritten, so both passes
                // compare the terms with the columns as written.
                if let Some((terms, settled)) = compound_order_by {
                    self.compound_order_terms(terms, settled, columns);
                }
                if let Some(from) = from.as_mut() {
                    self.join_exprs(from);
                }
                self.result_columns(columns);
                if let Some(expr) = where_clause {
                    self.expr(expr);
                }
                for expr in group_by {
                    self.expr(expr);
                }
                if let Some(expr) = having {
                    self.expr(expr);
                }
                for window in windows {
                    self.window_spec(&mut window.spec);
                }
            }
        }
        for term in order_by.into_iter().flatten() {
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
                self.scopes.push(Vec::new());
                for expr in rows.iter_mut().flatten() {
                    self.expr(expr);
                }
                self.scopes.pop();
            }
            InsertSource::DefaultValues => {}
        }
        let mut scope = Vec::new();
        self.register_named(&insert.table, &mut insert.alias, true, &mut scope);
        self.scopes.push(scope);
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
                for assignment in assignments {
                    self.expr(&mut assignment.value);
                }
                if let Some(expr) = where_clause {
                    self.expr(expr);
                }
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
        let mut scope = Vec::new();
        self.register_named(
            &update.table.name,
            &mut update.table.alias,
            true,
            &mut scope,
        );
        if let Some(from) = update.from.as_mut() {
            self.register_from(from, &mut scope);
        }
        self.scopes.push(scope);
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
        let mut scope = Vec::new();
        self.register_named(
            &delete.table.name,
            &mut delete.table.alias,
            true,
            &mut scope,
        );
        self.scopes.push(scope);
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
                scope.push(BindingSource {
                    id,
                    table: Some(name.clone()),
                    alias: written_alias,
                    relation: QualifiedColumnRelation::opaque("main", None),
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
        let relation = if name.schema.is_none() && self.is_cte(&name.name) {
            QualifiedColumnRelation::default()
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

    /// Walk a FROM clause's ON constraints and table-function arguments,
    /// which see the whole query's items.
    fn join_exprs(&mut self, from: &mut FromClause) {
        self.source_exprs(&mut from.source);
        for join in &mut from.joins {
            self.source_exprs(&mut join.table);
            if let Some(JoinConstraint::On(expr)) = &mut join.constraint {
                self.expr(expr);
            }
        }
    }

    fn source_exprs(&mut self, source: &mut TableOrSubquery) {
        match source {
            TableOrSubquery::TableFunction { args, .. } => {
                for expr in args {
                    self.expr(expr);
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
                            .flatten()
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
        &self,
        terms: &mut [OrderingTerm],
        settled: &mut [bool],
        columns: &[ResultColumn],
    ) {
        let arm = self.scopes.len() - 1;
        for (term, settled) in terms.iter_mut().zip(settled.iter_mut()) {
            if *settled {
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
        self.expr(&mut limit.limit);
        if let Some(offset) = &mut limit.offset {
            self.expr(offset);
        }
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
            Expr::Literal(..)
            | Expr::BoundOuterValue { .. }
            | Expr::Raise { .. }
            | Expr::Placeholder(..) => {}
        }
    }

    fn column(&mut self, column: &mut ColumnRef) {
        let Some(table) = column.table.clone() else {
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
                    Lookup::Ambiguous => self.fail(FrankenError::AmbiguousColumn { name: name() }),
                    Lookup::Missing => self.fail(FrankenError::NoSuchColumn { name: name() }),
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

    fn two_part(&mut self, column: &mut ColumnRef, table: &str) {
        let lookup = self.lookup(None, table, &column.column);
        match self.pass {
            BindPass::Bind => {
                self.take_name(table);
                if matches!(lookup, Lookup::Ambiguous) {
                    self.fail(FrankenError::AmbiguousColumn {
                        name: format!("{table}.{}", column.column),
                    });
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
            for source in scope {
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

    fn source(&self, scope: usize, id: usize) -> &BindingSource {
        self.scopes[scope]
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
    fn note_shared_name(&mut self, scope: usize, id: usize) {
        let source = self.source(scope, id);
        let Some(name) = source.addressed_as() else {
            return;
        };
        let sharers: Vec<(usize, (String, String))> = self.scopes[scope..]
            .iter()
            .flatten()
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

// ---------------------------------------------------------------------------
// Cheap syntactic gate. Binding can change a statement only when it has a
// three-part column reference (to rewrite or reject), or when one query's
// FROM items share a name and at least one of them is database-qualified (a
// two-part reference to that name is then ambiguous across databases). Every
// other statement skips the clone and the catalog lookups.
// ---------------------------------------------------------------------------

/// Whether `statement` has a three-part column reference, or a query whose
/// FROM items (with an UPDATE's target) share a name one of them qualifies.
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
        }
        Statement::Delete(delete) => {
            with_needs_binding(delete.with.as_ref())
                || delete.where_clause.as_ref().is_some_and(expr_needs_binding)
                || result_columns_need_binding(&delete.returning)
                || delete
                    .order_by
                    .iter()
                    .any(|term| expr_needs_binding(&term.expr))
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
}

fn expr_needs_binding(expr: &Expr) -> bool {
    match expr {
        Expr::Column(column, _) => column.schema.is_some(),
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
                })
        }
    }

    fn bind(sql: &str) -> Result<Option<String>> {
        let statement = parse_single_statement(sql)?;
        if !statement_needs_binding(&statement) {
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
    fn statements_without_a_database_name_or_with_a_missing_table_are_left_alone() {
        assert_eq!(bind("SELECT users.name FROM users").expect("binds"), None);
        assert_eq!(
            bind("SELECT temp.t.x FROM t JOIN nosuch").expect("binds"),
            None
        );
    }
}
