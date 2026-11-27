//! bd-u01ku (item 1): stock SQLite's cursor budget at a CTE expansion.
//!
//! Stock (select.c `resolveFromTermToCte`, 3.54.1) checks, each time it
//! expands a reference to a CTE, whether the statement has already assigned
//! `SQLITE_LIMIT_EXPR_DEPTH` (1000) cursors. If so its nesting count jumps to
//! the cursor count and the expansion fails with "VIEWs and/or subqueries
//! nested too deep" (the guard against CTEs that multiply cursors
//! exponentially). Cursors are assigned as the expander walks the statement:
//!
//! - `selectExpander` gives one cursor to every FROM item of a SELECT,
//!   recursing into the FROM list of a subquery item's last arm
//!   (`sqlite3SrcListAssignCursors`), then expands the items in order: a
//!   subquery is walked at once, a CTE reference is checked against the
//!   budget and its body is walked as a fresh copy, so each reference to a
//!   CTE assigns the body's cursors again;
//! - then the SELECT's expressions are walked (their subqueries), and a
//!   compound's arms from the last to the first.
//!
//! So `WITH c0 AS (SELECT 1 AS x), ..., c998 AS (SELECT x FROM c997)
//! SELECT x FROM (SELECT x FROM c998)` (999 CTEs) is refused, as is a chain
//! of 500 CTEs whose bodies read through a derived table, or nine levels of
//! a CTE joined to itself; fsqlite answered all of them. This pass counts
//! cursors in that order, stops at the budget, and runs before the
//! statement's relations are resolved. A view is counted as one item and not
//! expanded (stock expands it too, so this can only under-count).
//!
//! bd-pn6az: the query flattener's limit ("too many FROM clause terms, max:
//! 200") and the join planner's ("at most 64 tables in a join") are checked
//! after the relations resolve (`Connection::validate_join_term_limits`).

#[allow(clippy::wildcard_imports)]
use super::*;

/// `SQLITE_LIMIT_EXPR_DEPTH`'s default, the budget stock checks.
const CTE_EXPANSION_CURSOR_BUDGET: usize = 1000;

impl Connection {
    /// bd-u01ku: refuse `select` as stock does once its CTE expansions reach
    /// the cursor budget. A statement without a WITH clause expands no CTE.
    pub(super) fn validate_cte_cursor_budget(select: &SelectStatement) -> Result<()> {
        if select.with.is_none() {
            return Ok(());
        }
        CursorBudget::default().select(select)
    }
}

#[derive(Default)]
struct CursorBudget {
    /// Cursors assigned so far (stock's `pParse->nTab`).
    cursors: usize,
    /// The WITH clauses in scope, outermost first.
    scopes: Vec<Vec<fsqlite_ast::Cte>>,
    /// The CTEs being expanded, as (scope, index in it): a reference to one
    /// of them is a recursive self-reference, which stock does not expand.
    active: Vec<(usize, usize)>,
    /// FROM lists whose cursors an enclosing SELECT already assigned (a
    /// subquery item's), within the current expansion.
    preassigned: HashSet<*const FromClause>,
}

impl CursorBudget {
    fn select(&mut self, select: &SelectStatement) -> Result<()> {
        let pushed = if let Some(with) = &select.with {
            self.scopes.push(with.ctes.clone());
            true
        } else {
            false
        };
        let result = self.select_arms(select);
        if pushed {
            self.scopes.pop();
        }
        result
    }

    /// The arms from the last to the first; the statement's ORDER BY and
    /// LIMIT belong to the last.
    fn select_arms(&mut self, select: &SelectStatement) -> Result<()> {
        let arms: Vec<&SelectCore> = std::iter::once(&select.body.select)
            .chain(select.body.compounds.iter().map(|(_, core)| core))
            .collect();
        for (index, core) in arms.iter().enumerate().rev() {
            self.core(core)?;
            if index + 1 == arms.len() {
                for term in &select.order_by {
                    self.expr(&term.expr)?;
                }
                if let Some(limit) = &select.limit {
                    self.expr(&limit.limit)?;
                    if let Some(offset) = &limit.offset {
                        self.expr(offset)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn core(&mut self, core: &SelectCore) -> Result<()> {
        match core {
            SelectCore::Values(rows) => {
                for expr in rows.iter().flatten() {
                    self.expr(expr)?;
                }
            }
            SelectCore::Select {
                columns,
                from,
                where_clause,
                group_by,
                having,
                ..
            } => {
                if let Some(from) = from {
                    self.assign_cursors(from);
                    self.expand_from(from)?;
                }
                for column in columns {
                    if let ResultColumn::Expr { expr, .. } = column {
                        self.expr(expr)?;
                    }
                }
                for expr in where_clause
                    .as_deref()
                    .into_iter()
                    .chain(group_by)
                    .chain(having.as_deref())
                {
                    self.expr(expr)?;
                }
            }
        }
        Ok(())
    }

    /// `sqlite3SrcListAssignCursors`: one cursor per FROM item, recursing
    /// into a subquery item's FROM list (its last arm's, the one stock links
    /// the subquery to). A leading parenthesized join or redundant parens
    /// around a single item splice into the enclosing FROM list (0 extra
    /// cursors); a non-leading multi-item parenthesized join becomes an
    /// `SF_NestedFrom` subquery and takes one cursor of its own before its
    /// inner items (`sqlite3.c`: case 115 + `sqlite3SrcListAssignCursors`).
    fn assign_cursors(&mut self, from: &FromClause) {
        if !self.preassigned.insert(std::ptr::from_ref(from)) {
            return;
        }
        self.assign_source_cursors(&from.source, true);
        for join in &from.joins {
            self.assign_source_cursors(&join.table, false);
        }
    }

    fn assign_source_cursors(&mut self, source: &TableOrSubquery, is_leading: bool) {
        match source {
            TableOrSubquery::ParenJoin(inner) => {
                let unwrapped = super::unwrap_single_item_paren_join(inner);
                if !is_leading && !unwrapped.joins.is_empty() {
                    self.cursors += 1;
                }
                self.assign_cursors(unwrapped);
            }
            TableOrSubquery::Subquery { query, .. } => {
                self.cursors += 1;
                let last_arm = query
                    .body
                    .compounds
                    .last()
                    .map_or(&query.body.select, |(_, core)| core);
                if let SelectCore::Select {
                    from: Some(inner), ..
                } = last_arm
                {
                    self.assign_cursors(inner);
                }
            }
            TableOrSubquery::Table { .. } | TableOrSubquery::TableFunction { .. } => {
                self.cursors += 1;
            }
        }
    }

    fn expand_from(&mut self, from: &FromClause) -> Result<()> {
        for source in std::iter::once(&from.source).chain(from.joins.iter().map(|join| &join.table))
        {
            match source {
                TableOrSubquery::ParenJoin(inner) => self.expand_from(inner)?,
                TableOrSubquery::Subquery { query, .. } => self.select(query)?,
                TableOrSubquery::Table { name, .. } if name.schema.is_none() => {
                    if let Some(found) = self.visible_cte(&name.name) {
                        self.expand_cte(found)?;
                    }
                }
                TableOrSubquery::TableFunction { args, .. } => {
                    for arg in args {
                        self.expr(arg)?;
                    }
                }
                TableOrSubquery::Table { .. } => {}
            }
        }
        Ok(())
    }

    /// The innermost CTE of `name` in scope, as (scope, index in it).
    fn visible_cte(&self, name: &str) -> Option<(usize, usize)> {
        self.scopes
            .iter()
            .enumerate()
            .rev()
            .find_map(|(scope, ctes)| {
                ctes.iter()
                    .position(|cte| cte.name.eq_ignore_ascii_case(name))
                    .map(|index| (scope, index))
            })
    }

    fn expand_cte(&mut self, found: (usize, usize)) -> Result<()> {
        if self.active.contains(&found) {
            return Ok(());
        }
        if self.cursors >= CTE_EXPANSION_CURSOR_BUDGET {
            return Err(FrankenError::ViewNestingTooDeep {
                max: MAX_VIEW_NESTING_DEPTH,
            });
        }
        let (scope, index) = found;
        let query = self.scopes[scope][index].query.clone();
        // The body resolves in its own WITH and the ones around it, and its
        // FROM lists are a fresh copy's.
        let hidden = self.scopes.split_off(scope + 1);
        let preassigned = std::mem::take(&mut self.preassigned);
        self.active.push(found);
        let result = with_view_expansion_stack(|| self.select(&query));
        self.active.pop();
        self.preassigned = preassigned;
        self.scopes.extend(hidden);
        result
    }

    /// The subqueries of `expr`, each walked as a SELECT.
    fn expr(&mut self, expr: &Expr) -> Result<()> {
        let mut failure = None;
        expr_contains_subquery_match(expr, &mut |subquery| {
            let select = match subquery {
                SubqueryExprRef::Scalar(select)
                | SubqueryExprRef::Exists(select)
                | SubqueryExprRef::In(select) => select,
            };
            match self.select(select) {
                Ok(()) => false,
                Err(error) => {
                    failure = Some(error);
                    true
                }
            }
        });
        failure.map_or(Ok(()), Err)
    }
}

/// bd-pn6az: `SQLITE_MAX_SRCLIST`, the most terms the flattener lets one
/// FROM list grow to.
const MAX_FLATTENED_FROM_TERMS: usize = 200;
/// bd-pn6az: `BMS`, the most tables the where-planner joins in one SELECT.
const MAX_JOIN_TABLES: usize = 64;

impl Connection {
    /// bd-pn6az / bd-b8cfq: stock's two limits on the tables one SELECT joins.
    /// Its flattener splices each subquery, view, and non-materialized CTE
    /// reference it can flatten into the enclosing FROM list (a copy per
    /// reference), and refuses a list that grows past 200 terms ("too many
    /// FROM clause terms, max: 200"); the where-planner then refuses a SELECT
    /// of more than 64 terms ("at most 64 tables in a join").
    ///
    /// Runs after [`Self::validate_cte_cursor_budget`] and
    /// `SelectRelationResolver`, so relation names are already valid and
    /// circular views have already been rejected.
    pub(super) fn validate_join_term_limits(&self, select: &SelectStatement) -> Result<()> {
        // The common statement, with no views in scope, no WITH, no subquery,
        // and at most 64 named tables or functions in each FROM, flattens
        // nothing into them.
        if !self.has_any_views_in_catalog()
            && select.with.is_none()
            && std::iter::once(&select.body.select)
                .chain(select.body.compounds.iter().map(|(_, core)| core))
                .all(core_joins_named_sources_within_limit)
            && select
                .order_by
                .iter()
                .all(|term| !expr_contains_subquery_match(&term.expr, &mut |_subquery| true))
        {
            return Ok(());
        }
        let most = self.with_fallback_function_registry(|| {
            let mut limits = JoinTermLimits::new(self);
            limits.select(select);
            limits.most
        });
        if most > MAX_FLATTENED_FROM_TERMS {
            return Err(FrankenError::FunctionError(format!(
                "too many FROM clause terms, max: {MAX_FLATTENED_FROM_TERMS}"
            )));
        }
        if most > MAX_JOIN_TABLES {
            return Err(FrankenError::FunctionError(format!(
                "at most {MAX_JOIN_TABLES} tables in a join"
            )));
        }
        Ok(())
    }

    fn has_any_views_in_catalog(&self) -> bool {
        if !self.views.borrow().is_empty() {
            return true;
        }
        self.attached_connections
            .borrow()
            .values()
            .any(|attached| !attached.views.borrow().is_empty())
    }
}

/// A subquery, view, or CTE body stock's flattener certainly splices into its
/// parent: one SELECT with a FROM and no outer join, with no DISTINCT,
/// GROUP BY, HAVING, window, LIMIT, or aggregate/window function in its result
/// columns or ORDER BY. Scalar function calls (such as `abs(a.x)`) and an
/// inner `WITH` clause do not prevent flattening in SQLite 3.54.1. An ORDER BY
/// does not stop it either: stock drops a subquery's ORDER BY first when
/// flattening into a join.
fn flattenable(query: &SelectStatement) -> bool {
    let SelectCore::Select {
        distinct: fsqlite_ast::Distinctness::All,
        columns,
        from: Some(from),
        group_by,
        having: None,
        windows,
        ..
    } = &query.body.select
    else {
        return false;
    };
    query.body.compounds.is_empty()
        && query.limit.is_none()
        && group_by.is_empty()
        && windows.is_empty()
        && !from_has_outer_join(from)
        && columns.iter().all(|column| match column {
            ResultColumn::Expr { expr, .. } => !expr_blocks_flattening(expr),
            ResultColumn::Star | ResultColumn::TableStar(_) => true,
        })
        && query
            .order_by
            .iter()
            .all(|term| !expr_blocks_flattening(&term.expr))
}

/// Whether `expr` contains an aggregate or window function at the current
/// SELECT level (without crossing subquery boundaries), which forces
/// `SF_Aggregate` or `pWin != NULL` and stops stock's flattener.
fn expr_blocks_flattening(expr: &Expr) -> bool {
    expr_has_aggregate(expr)
        || expr_contains_registered_agg(expr)
        || expr_has_direct_window_call(expr)
}

fn expr_has_direct_window_call(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { over: Some(_), .. } => true,
        Expr::FunctionCall {
            args,
            order_by,
            filter,
            ..
        } => {
            let in_args = match args {
                FunctionArgs::List(exprs) => exprs.iter().any(expr_has_direct_window_call),
                FunctionArgs::Star => false,
            };
            in_args
                || order_by
                    .iter()
                    .any(|term| expr_has_direct_window_call(&term.expr))
                || filter.as_deref().is_some_and(expr_has_direct_window_call)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_has_direct_window_call(left) || expr_has_direct_window_call(right)
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. }
        | Expr::Cast { expr: inner, .. }
        | Expr::Collate { expr: inner, .. } => expr_has_direct_window_call(inner),
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            expr_has_direct_window_call(inner)
                || expr_has_direct_window_call(low)
                || expr_has_direct_window_call(high)
        }
        Expr::In { expr: inner, set, .. } => {
            expr_has_direct_window_call(inner)
                || match set {
                    fsqlite_ast::InSet::List(items) => {
                        items.iter().any(expr_has_direct_window_call)
                    }
                    fsqlite_ast::InSet::Subquery(_) | fsqlite_ast::InSet::Table(_) => false,
                }
        }
        Expr::Like {
            expr: inner,
            pattern,
            escape,
            ..
        } => {
            expr_has_direct_window_call(inner)
                || expr_has_direct_window_call(pattern)
                || escape.as_deref().is_some_and(expr_has_direct_window_call)
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            operand.as_deref().is_some_and(expr_has_direct_window_call)
                || whens.iter().any(|(when_expr, then_expr)| {
                    expr_has_direct_window_call(when_expr)
                        || expr_has_direct_window_call(then_expr)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(expr_has_direct_window_call)
        }
        Expr::JsonAccess { expr, path, .. } => {
            expr_has_direct_window_call(expr) || expr_has_direct_window_call(path)
        }
        Expr::RowValue(values, _) => values.iter().any(expr_has_direct_window_call),
        Expr::BoundOuterValue { .. }
        | Expr::Raise { .. }
        | Expr::Literal(_, _)
        | Expr::Column(_, _)
        | Expr::Placeholder(_, _)
        | Expr::Subquery(_, _)
        | Expr::Exists { .. } => false,
    }
}

/// Whether `core` joins at most 64 named tables or table-valued functions and
/// reads no subquery, so nothing can flatten into it or plan a larger join.
fn core_joins_named_sources_within_limit(core: &SelectCore) -> bool {
    let no_subquery = |expr: &Expr| !expr_contains_subquery_match(expr, &mut |_subquery| true);
    match core {
        SelectCore::Values(rows) => rows.iter().flatten().all(no_subquery),
        SelectCore::Select {
            columns,
            from,
            where_clause,
            group_by,
            having,
            ..
        } => {
            from.as_ref().is_none_or(|from| {
                from.joins.len() < MAX_JOIN_TABLES
                    && std::iter::once(&from.source)
                        .chain(from.joins.iter().map(|join| &join.table))
                        .all(|item| match item {
                            TableOrSubquery::Table { .. } => true,
                            TableOrSubquery::TableFunction { args, .. } => {
                                args.iter().all(no_subquery)
                            }
                            TableOrSubquery::Subquery { .. } | TableOrSubquery::ParenJoin(_) => {
                                false
                            }
                        })
                    && from.joins.iter().all(|join| match &join.constraint {
                        Some(fsqlite_ast::JoinConstraint::On(expr)) => no_subquery(expr),
                        _ => true,
                    })
            }) && columns.iter().all(|column| match column {
                ResultColumn::Expr { expr, .. } => no_subquery(expr),
                ResultColumn::Star | ResultColumn::TableStar(_) => true,
            }) && where_clause
                .as_deref()
                .into_iter()
                .chain(group_by)
                .chain(having.as_deref())
                .all(no_subquery)
        }
    }
}

fn from_has_outer_join(from: &FromClause) -> bool {
    from.joins.iter().any(|join| {
        matches!(
            join.join_type.kind,
            JoinKind::Left | JoinKind::Right | JoinKind::Full
        )
    })
}

struct JoinTermLimits<'conn> {
    /// Connection whose catalog is currently searched for views/tables.
    conn: &'conn Connection,
    /// Persistent view owner schema label (`main` or an attached alias).
    /// `None` outside persistent views (and inside TEMP views), where the
    /// normal TEMP -> MAIN -> attached search order applies.
    persistent_owner_label: Option<String>,
    /// The most terms one SELECT joins, saturated just past the larger limit.
    most: usize,
    /// The WITH clauses in scope, outermost first.
    scopes: Vec<Vec<fsqlite_ast::Cte>>,
    /// The CTEs being expanded: a reference to one is a recursive
    /// self-reference, which is not expanded.
    active: Vec<(usize, usize)>,
    /// Memoized CTE term counts keyed by `(scope_index, cte_index)` within the
    /// active `WITH` stack: `Some(n)` if the CTE is flattenable with `n`
    /// flattened terms, `None` if non-flattenable (its own SELECT was already
    /// folded into `most`). Cleared when leaving a scope.
    cte_cache: HashMap<(usize, usize), Option<usize>>,
    /// Views currently on the expansion stack (cycle guard).
    active_views: HashSet<String>,
    /// Memoized view term counts keyed by view identity: `Some(n)` if the view
    /// is flattenable with `n` flattened terms, `None` if non-flattenable (its
    /// own SELECT was already folded into `most`).
    view_cache: HashMap<String, Option<usize>>,
}

impl<'conn> JoinTermLimits<'conn> {
    fn new(conn: &'conn Connection) -> Self {
        Self {
            conn,
            persistent_owner_label: None,
            most: 0,
            scopes: Vec::new(),
            active: Vec::new(),
            cte_cache: HashMap::new(),
            active_views: HashSet::new(),
            view_cache: HashMap::new(),
        }
    }

    fn select(&mut self, select: &SelectStatement) {
        if self.most > MAX_FLATTENED_FROM_TERMS {
            return;
        }
        let pushed_scope = if let Some(with) = &select.with {
            let scope_idx = self.scopes.len();
            self.scopes.push(with.ctes.clone());
            Some(scope_idx)
        } else {
            None
        };
        for core in std::iter::once(&select.body.select)
            .chain(select.body.compounds.iter().map(|(_, core)| core))
        {
            let terms = self.core(core);
            self.most = self.most.max(terms);
            if self.most > MAX_FLATTENED_FROM_TERMS {
                break;
            }
        }
        if self.most <= MAX_FLATTENED_FROM_TERMS {
            for term in &select.order_by {
                self.expr(&term.expr);
            }
            if let Some(limit) = &select.limit {
                self.expr(&limit.limit);
                if let Some(offset) = &limit.offset {
                    self.expr(offset);
                }
            }
        }
        if let Some(scope_idx) = pushed_scope {
            self.scopes.pop();
            self.cte_cache.retain(|&(s, _), _| s < scope_idx);
        }
    }

    /// Count the flattened terms of a flattenable single-arm SELECT (which may
    /// carry its own `WITH` clause and `ORDER BY` subqueries).
    fn flattened_select(&mut self, select: &SelectStatement) -> usize {
        if self.most > MAX_FLATTENED_FROM_TERMS {
            return MAX_FLATTENED_FROM_TERMS + 1;
        }
        let pushed_scope = if let Some(with) = &select.with {
            let scope_idx = self.scopes.len();
            self.scopes.push(with.ctes.clone());
            Some(scope_idx)
        } else {
            None
        };
        let terms = self.core(&select.body.select);
        if self.most <= MAX_FLATTENED_FROM_TERMS {
            for term in &select.order_by {
                self.expr(&term.expr);
            }
        }
        if let Some(scope_idx) = pushed_scope {
            self.scopes.pop();
            self.cte_cache.retain(|&(s, _), _| s < scope_idx);
        }
        terms
    }

    /// The terms `core` joins once the flattened subqueries are spliced in;
    /// the subqueries its expressions read are SELECTs of their own.
    fn core(&mut self, core: &SelectCore) -> usize {
        if self.most > MAX_FLATTENED_FROM_TERMS {
            return MAX_FLATTENED_FROM_TERMS + 1;
        }
        match core {
            SelectCore::Values(rows) => {
                for expr in rows.iter().flatten() {
                    self.expr(expr);
                }
                0
            }
            SelectCore::Select {
                columns,
                from,
                where_clause,
                group_by,
                having,
                ..
            } => {
                let terms = from.as_ref().map_or(0, |from| self.list_terms(from));
                if self.most <= MAX_FLATTENED_FROM_TERMS {
                    for column in columns {
                        if let ResultColumn::Expr { expr, .. } = column {
                            self.expr(expr);
                        }
                    }
                    for expr in where_clause
                        .as_deref()
                        .into_iter()
                        .chain(group_by)
                        .chain(having.as_deref())
                    {
                        self.expr(expr);
                    }
                }
                terms
            }
        }
    }

    /// The terms a FROM list joins, each item's flattened terms summed.
    fn list_terms(&mut self, from: &FromClause) -> usize {
        let mut terms = self.item_terms(&from.source, false);
        for join in &from.joins {
            let outer = matches!(
                join.join_type.kind,
                JoinKind::Left | JoinKind::Right | JoinKind::Full
            );
            terms = terms
                .saturating_add(self.item_terms(&join.table, outer))
                .min(MAX_FLATTENED_FROM_TERMS + 1);
            if let Some(fsqlite_ast::JoinConstraint::On(expr)) = &join.constraint {
                self.expr(expr);
            }
            if terms > MAX_FLATTENED_FROM_TERMS {
                self.most = self.most.max(terms);
                return terms;
            }
        }
        terms.min(MAX_FLATTENED_FROM_TERMS + 1)
    }

    /// The terms one FROM item adds; `outer` when an outer join holds it.
    fn item_terms(&mut self, item: &TableOrSubquery, outer: bool) -> usize {
        if self.most > MAX_FLATTENED_FROM_TERMS {
            return MAX_FLATTENED_FROM_TERMS + 1;
        }
        match item {
            TableOrSubquery::Table { name, .. } => {
                if name.schema.is_none()
                    && let Some(found) = self.visible_cte(&name.name)
                {
                    return self.cte_terms(found, outer);
                }
                self.catalog_relation_terms(name, outer)
            }
            TableOrSubquery::Subquery { query, .. } => {
                if !outer && flattenable(query) {
                    self.flattened_select(query)
                } else {
                    self.select(query);
                    1
                }
            }
            TableOrSubquery::ParenJoin(inner) if !outer && !from_has_outer_join(inner) => {
                self.list_terms(inner)
            }
            TableOrSubquery::ParenJoin(inner) => {
                let terms = self.list_terms(inner);
                self.most = self.most.max(terms);
                1
            }
            TableOrSubquery::TableFunction { args, .. } => {
                for arg in args {
                    self.expr(arg);
                }
                1
            }
        }
    }

    /// Resolve a catalog relation (table or view) following SQLite's exact
    /// scope precedence (TEMP -> MAIN -> attached, or pinned to the owning
    /// persistent schema inside a persistent view). Ordinary tables and virtual
    /// tables count 1 term; views expand under their own fresh CTE scope.
    fn catalog_relation_terms(&mut self, name: &QualifiedName, outer: bool) -> usize {
        if let Some(owner_label) = self.persistent_owner_label.clone() {
            if let Some(schema) = name.schema.as_deref()
                && !schema.eq_ignore_ascii_case(&owner_label)
            {
                return 1;
            }
            return self.local_catalog_relation_terms(
                self.conn,
                &name.name,
                PragmaSchemaScope::Main,
                &owner_label,
                outer,
            );
        }

        let Some(schema_name) = name.schema.as_deref() else {
            if let Some(terms) = self.try_local_catalog_relation_terms(
                self.conn,
                &name.name,
                PragmaSchemaScope::Unqualified,
                "main",
                outer,
            ) {
                return terms;
            }
            let attached_names: Vec<String> = self
                .conn
                .attached_schemas
                .borrow()
                .all_schemas()
                .into_iter()
                .skip(2)
                .map(str::to_owned)
                .collect();
            for attached_schema in attached_names {
                let mut resolved = None;
                let _ = self
                    .conn
                    .with_attached_connection(&attached_schema, |attached| {
                        resolved = self.try_local_catalog_relation_terms(
                            attached,
                            &name.name,
                            PragmaSchemaScope::Main,
                            &attached_schema,
                            outer,
                        );
                        Ok(())
                    });
                if let Some(terms) = resolved {
                    return terms;
                }
            }
            return 1;
        };

        if schema_name.eq_ignore_ascii_case("main") {
            return self.local_catalog_relation_terms(
                self.conn,
                &name.name,
                PragmaSchemaScope::Main,
                "main",
                outer,
            );
        }
        if schema_name.eq_ignore_ascii_case("temp") {
            return self.local_catalog_relation_terms(
                self.conn,
                &name.name,
                PragmaSchemaScope::Temp,
                "main",
                outer,
            );
        }
        if self
            .conn
            .attached_schemas
            .borrow()
            .find(schema_name)
            .is_some()
        {
            let mut terms = 1;
            let _ = self.conn.with_attached_connection(schema_name, |attached| {
                terms = self.local_catalog_relation_terms(
                    attached,
                    &name.name,
                    PragmaSchemaScope::Main,
                    schema_name,
                    outer,
                );
                Ok(())
            });
            return terms;
        }
        1
    }

    fn local_catalog_relation_terms(
        &mut self,
        target_conn: &Connection,
        relation_name: &str,
        scope: PragmaSchemaScope,
        main_label: &str,
        outer: bool,
    ) -> usize {
        self.try_local_catalog_relation_terms(target_conn, relation_name, scope, main_label, outer)
            .unwrap_or(1)
    }

    /// Returns `Some(terms)` if `relation_name` matches a table, vtab, or view
    /// in `target_conn`/`scope`, or `None` if not present there (so unqualified
    /// lookup can continue to attached schemas).
    fn try_local_catalog_relation_terms(
        &mut self,
        target_conn: &Connection,
        relation_name: &str,
        scope: PragmaSchemaScope,
        main_label: &str,
        outer: bool,
    ) -> Option<usize> {
        if is_schema_relation_in_scope(relation_name, scope) {
            return Some(1);
        }
        let resolved_scope = if scope == PragmaSchemaScope::Unqualified
            && (target_conn.table_exists_for_scope(relation_name, PragmaSchemaScope::Temp)
                || target_conn
                    .view_index_for_scope(relation_name, PragmaSchemaScope::Temp)
                    .is_some())
        {
            PragmaSchemaScope::Temp
        } else if scope == PragmaSchemaScope::Unqualified {
            PragmaSchemaScope::Main
        } else {
            scope
        };
        if target_conn.table_exists_for_scope(relation_name, resolved_scope)
            || (resolved_scope == PragmaSchemaScope::Main
                && target_conn.has_live_vtab_instance(relation_name))
        {
            return Some(1);
        }
        let view = target_conn
            .view_index_for_scope(relation_name, resolved_scope)
            .and_then(|index| target_conn.views.borrow().get(index).cloned())?;
        Some(self.expand_view_terms(target_conn, &view, resolved_scope, main_label, outer))
    }

    fn expand_view_terms(
        &mut self,
        target_conn: &Connection,
        view: &ViewDef,
        resolved_scope: PragmaSchemaScope,
        main_label: &str,
        outer: bool,
    ) -> usize {
        let view_key = format!(
            "{:p}:{resolved_scope:?}:{}",
            target_conn,
            view.name.to_ascii_lowercase()
        );
        if let Some(&cached) = self.view_cache.get(&view_key) {
            return match cached {
                Some(terms) if !outer => terms,
                Some(terms) => {
                    self.most = self.most.max(terms);
                    1
                }
                None => 1,
            };
        }
        if !self.active_views.insert(view_key.clone()) {
            return 1;
        }

        let view_flattenable = flattenable(&view.query);
        let next_owner = if view.temporary {
            None
        } else {
            Some(
                self.persistent_owner_label
                    .clone()
                    .unwrap_or_else(|| main_label.to_owned()),
            )
        };
        // A view owns a fresh CTE scope: outer CTEs never leak into its body.
        let mut child = JoinTermLimits {
            conn: target_conn,
            persistent_owner_label: next_owner,
            most: self.most,
            scopes: Vec::new(),
            active: Vec::new(),
            cte_cache: HashMap::new(),
            active_views: std::mem::take(&mut self.active_views),
            view_cache: std::mem::take(&mut self.view_cache),
        };
        let cached = with_view_expansion_stack(|| {
            if view_flattenable {
                let terms = child.flattened_select(&view.query);
                Some(terms)
            } else {
                child.select(&view.query);
                None
            }
        });
        self.most = self.most.max(child.most);
        self.active_views = child.active_views;
        self.view_cache = child.view_cache;
        self.active_views.remove(&view_key);
        self.view_cache.insert(view_key, cached);

        match cached {
            Some(terms) if !outer => terms,
            Some(terms) => {
                self.most = self.most.max(terms);
                1
            }
            None => 1,
        }
    }

    /// The innermost CTE of `name` in scope, as (scope, index in it).
    fn visible_cte(&self, name: &str) -> Option<(usize, usize)> {
        self.scopes
            .iter()
            .enumerate()
            .rev()
            .find_map(|(scope, ctes)| {
                ctes.iter()
                    .position(|cte| cte.name.eq_ignore_ascii_case(name))
                    .map(|index| (scope, index))
            })
    }

    /// A CTE reference: its body's terms when it is flattened (a copy per
    /// reference, as stock's), else one term, the body a SELECT of its own.
    fn cte_terms(&mut self, found: (usize, usize), outer: bool) -> usize {
        if self.active.contains(&found) {
            return 1;
        }
        if let Some(&cached) = self.cte_cache.get(&found) {
            return match cached {
                Some(terms) if !outer => terms,
                Some(terms) => {
                    self.most = self.most.max(terms);
                    1
                }
                None => 1,
            };
        }
        let (scope, index) = found;
        let cte = self.scopes[scope][index].clone();
        let can_flatten = cte.materialized != Some(fsqlite_ast::CteMaterialized::Materialized)
            && flattenable(&cte.query);
        // The body resolves in its own WITH and the ones around it.
        let hidden = self.scopes.split_off(scope + 1);
        let saved_cte_cache = std::mem::take(&mut self.cte_cache);
        let mut inner_cache = HashMap::new();
        let mut hidden_cache = HashMap::new();
        for (key, value) in saved_cte_cache {
            if key.0 <= scope {
                inner_cache.insert(key, value);
            } else {
                hidden_cache.insert(key, value);
            }
        }
        self.cte_cache = inner_cache;
        self.active.push(found);
        let cached = with_view_expansion_stack(|| {
            if can_flatten {
                let terms = self.flattened_select(&cte.query);
                Some(terms)
            } else {
                self.select(&cte.query);
                None
            }
        });
        self.active.pop();
        self.scopes.extend(hidden);
        self.cte_cache.extend(hidden_cache);
        self.cte_cache.insert(found, cached);
        match cached {
            Some(terms) if !outer => terms,
            Some(terms) => {
                self.most = self.most.max(terms);
                1
            }
            None => 1,
        }
    }

    /// The subqueries of `expr`, each a SELECT of its own.
    fn expr(&mut self, expr: &Expr) {
        if self.most > MAX_FLATTENED_FROM_TERMS {
            return;
        }
        expr_contains_subquery_match(expr, &mut |subquery| {
            let (SubqueryExprRef::Scalar(select)
            | SubqueryExprRef::Exists(select)
            | SubqueryExprRef::In(select)) = subquery;
            self.select(select);
            self.most > MAX_FLATTENED_FROM_TERMS
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(sql: &str) -> Result<()> {
        let Statement::Select(select) = parse_single_statement(sql).expect("parses") else {
            panic!("not a SELECT: {sql}");
        };
        Connection::validate_cte_cursor_budget(&select)
    }

    fn chain(n: usize, derived: bool) -> String {
        let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_owned()];
        for i in 1..n {
            let source = if derived {
                format!("(SELECT x FROM c{})", i - 1)
            } else {
                format!("c{}", i - 1)
            };
            ctes.push(format!("c{i} AS (SELECT x FROM {source})"));
        }
        format!(
            "WITH {} SELECT x FROM (SELECT x FROM c{})",
            ctes.join(", "),
            n - 1
        )
    }

    fn doubling(k: usize) -> String {
        let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_owned()];
        for i in 1..=k {
            ctes.push(format!(
                "c{i} AS (SELECT a.x FROM c{0} AS a, c{0} AS b)",
                i - 1
            ));
        }
        format!("WITH {} SELECT x FROM c{k}", ctes.join(", "))
    }

    #[test]
    fn stock_boundaries_of_the_cursor_budget() {
        // Stock 3.54.1: the plain chain answers at 998 CTEs and is refused at
        // 999; the derived-table chain at 499 and 500; the doubling chain is
        // refused at c9.
        assert!(budget(&chain(998, false)).is_ok());
        assert!(matches!(
            budget(&chain(999, false)),
            Err(FrankenError::ViewNestingTooDeep { .. })
        ));
        assert!(budget(&chain(499, true)).is_ok());
        assert!(matches!(
            budget(&chain(500, true)),
            Err(FrankenError::ViewNestingTooDeep { .. })
        ));
        assert!(budget(&doubling(8)).is_ok());
        assert!(matches!(
            budget(&doubling(9)),
            Err(FrankenError::ViewNestingTooDeep { .. })
        ));
    }

    fn limits(sql: &str) -> Result<()> {
        let Statement::Select(select) = parse_single_statement(sql).expect("parses") else {
            panic!("not a SELECT: {sql}");
        };
        // `run_test` takes a body whose future yields `()`: carry the result
        // out (bd-7wn08).
        let mut result = None;
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.expect("open memory db");
            result = Some(conn.validate_join_term_limits(&select));
        });
        result.expect("the test body ran")
    }

    fn refused_with(result: &Result<()>, message: &str) -> bool {
        matches!(result, Err(FrankenError::FunctionError(text)) if text == message)
    }

    #[test]
    fn stock_join_term_limits_of_a_doubling_chain_and_a_plain_join() {
        // Stock 3.54.1: the doubling chain joins 2^k flattened terms, so 6
        // levels answer, 7 exceed the 64-table join and 8 the 200 terms.
        assert!(limits(&doubling(6)).is_ok());
        assert!(refused_with(
            &limits(&doubling(7)),
            "at most 64 tables in a join"
        ));
        assert!(refused_with(
            &limits(&doubling(8)),
            "too many FROM clause terms, max: 200"
        ));
        let join = |n: usize| {
            let items: Vec<String> = (1..=n).map(|i| format!("o AS o{i}")).collect();
            format!("SELECT count(*) FROM {}", items.join(", "))
        };
        assert!(limits(&join(64)).is_ok());
        assert!(refused_with(
            &limits(&join(65)),
            "at most 64 tables in a join"
        ));
        assert!(refused_with(
            &limits(&format!("SELECT ({})", join(65))),
            "at most 64 tables in a join"
        ));
        // Bodies stock does not flatten (DISTINCT, an outer join, LIMIT) are
        // one term each, so the chain stays small.
        let chain = |body: &dyn Fn(usize) -> String| {
            let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_owned()];
            ctes.extend((1..=8).map(|i| format!("c{i} AS ({})", body(i - 1))));
            format!("WITH {} SELECT x FROM c8", ctes.join(", "))
        };
        for sql in [
            chain(&|p| format!("SELECT DISTINCT a.x FROM c{p} AS a, c{p} AS b")),
            chain(&|p| format!("SELECT a.x FROM c{p} AS a LEFT JOIN c{p} AS b ON 1")),
            chain(&|p| format!("SELECT a.x FROM c{p} AS a, c{p} AS b LIMIT 5")),
        ] {
            assert!(limits(&sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn a_recursive_self_reference_and_a_qualified_name_are_not_expanded() {
        // A recursive CTE's own reference is the recursive table, and
        // `main.c` never names a CTE, so neither multiplies cursors.
        assert!(
            budget(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5) \
             SELECT x FROM c, c AS d"
            )
            .is_ok()
        );
        assert!(budget("WITH c(x) AS (SELECT 1) SELECT x FROM main.c").is_ok());
        assert!(budget("SELECT 1").is_ok());
    }

    #[test]
    fn non_first_paren_join_cursor_budget_boundaries() {
        let with_outer = |n: usize, outer: &str| {
            let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_owned()];
            for i in 1..n {
                ctes.push(format!("c{i} AS (SELECT x FROM c{})", i - 1));
            }
            format!(
                "WITH {} {}",
                ctes.join(", "),
                outer.replace("{last}", &format!("c{}", n - 1))
            )
        };
        let non_first = "SELECT x FROM base JOIN ({last} JOIN base AS b2)";
        let double_wrapped = "SELECT x FROM base JOIN (({last} JOIN base AS b2))";
        let two_nested = "SELECT x FROM base JOIN (base AS b2 JOIN ({last} JOIN base AS b3))";
        let leading = "SELECT x FROM ({last} JOIN base AS b2) JOIN base";
        let single_item = "SELECT x FROM base JOIN ({last})";

        assert!(budget(&with_outer(996, non_first)).is_ok());
        assert!(matches!(
            budget(&with_outer(997, non_first)),
            Err(FrankenError::ViewNestingTooDeep { .. })
        ));
        assert!(budget(&with_outer(996, double_wrapped)).is_ok());
        assert!(matches!(
            budget(&with_outer(997, double_wrapped)),
            Err(FrankenError::ViewNestingTooDeep { .. })
        ));
        assert!(budget(&with_outer(994, two_nested)).is_ok());
        assert!(matches!(
            budget(&with_outer(995, two_nested)),
            Err(FrankenError::ViewNestingTooDeep { .. })
        ));
        assert!(budget(&with_outer(997, leading)).is_ok());
        assert!(matches!(
            budget(&with_outer(998, leading)),
            Err(FrankenError::ViewNestingTooDeep { .. })
        ));
        assert!(budget(&with_outer(998, single_item)).is_ok());
        assert!(matches!(
            budget(&with_outer(999, single_item)),
            Err(FrankenError::ViewNestingTooDeep { .. })
        ));
    }
}
