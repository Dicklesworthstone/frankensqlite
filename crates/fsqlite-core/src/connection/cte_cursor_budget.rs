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
    /// the subquery to). A parenthesized join is a list of its own items.
    fn assign_cursors(&mut self, from: &FromClause) {
        if !self.preassigned.insert(std::ptr::from_ref(from)) {
            return;
        }
        for source in std::iter::once(&from.source).chain(from.joins.iter().map(|join| &join.table))
        {
            match source {
                TableOrSubquery::ParenJoin(inner) => self.assign_cursors(inner),
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
    /// bd-pn6az: stock's two limits on the tables one SELECT joins. Its
    /// flattener splices each subquery and CTE reference it can flatten into
    /// the enclosing FROM list (a copy per reference), and refuses a list that
    /// grows past 200 terms ("too many FROM clause terms, max: 200"); the
    /// where-planner then refuses a SELECT of more than 64 terms ("at most 64
    /// tables in a join"). So a CTE joined to itself refuses at 7 levels (128
    /// terms) and 8 (256), and a plain 65-table join refuses too.
    ///
    /// A reference counts as its body's terms only for a body stock certainly
    /// flattens (see [`flattenable`]) that no outer join holds; anything else
    /// (a view, a table-valued function, a materialized CTE) counts one term.
    /// So this can miss a refusal stock makes, never invent one. Runs after
    /// [`Self::validate_cte_cursor_budget`], whose refusal comes first in stock.
    pub(super) fn validate_join_term_limits(select: &SelectStatement) -> Result<()> {
        // The common statement, with no WITH, no subquery and at most 64 named
        // tables or functions in each FROM, flattens nothing into them.
        if select.with.is_none()
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
        let mut limits = JoinTermLimits::default();
        limits.select(select);
        if limits.most > MAX_FLATTENED_FROM_TERMS {
            return Err(FrankenError::FunctionError(format!(
                "too many FROM clause terms, max: {MAX_FLATTENED_FROM_TERMS}"
            )));
        }
        if limits.most > MAX_JOIN_TABLES {
            return Err(FrankenError::FunctionError(format!(
                "at most {MAX_JOIN_TABLES} tables in a join"
            )));
        }
        Ok(())
    }
}

/// A subquery or CTE body stock's flattener certainly splices into its
/// parent: one SELECT with a FROM and no outer join, with no WITH, DISTINCT,
/// GROUP BY, HAVING, window, LIMIT or function call in its result columns
/// (an aggregate cannot be told from a scalar function here). An ORDER BY
/// does not stop it: stock drops a subquery's ORDER BY first.
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
    query.with.is_none()
        && query.body.compounds.is_empty()
        && query.limit.is_none()
        && group_by.is_empty()
        && windows.is_empty()
        && !from_has_outer_join(from)
        && columns.iter().all(|column| match column {
            ResultColumn::Expr { expr, .. } => !expr_contains_function_call(expr),
            ResultColumn::Star | ResultColumn::TableStar(_) => true,
        })
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

#[derive(Default)]
struct JoinTermLimits {
    /// The most terms one SELECT joins, saturated just past the larger limit.
    most: usize,
    /// The WITH clauses in scope, outermost first.
    scopes: Vec<Vec<fsqlite_ast::Cte>>,
    /// The CTEs being expanded: a reference to one is a recursive
    /// self-reference, which is not expanded.
    active: Vec<(usize, usize)>,
}

impl JoinTermLimits {
    fn select(&mut self, select: &SelectStatement) {
        let pushed = if let Some(with) = &select.with {
            self.scopes.push(with.ctes.clone());
            true
        } else {
            false
        };
        for core in std::iter::once(&select.body.select)
            .chain(select.body.compounds.iter().map(|(_, core)| core))
        {
            let terms = self.core(core);
            self.most = self.most.max(terms);
        }
        for term in &select.order_by {
            self.expr(&term.expr);
        }
        if pushed {
            self.scopes.pop();
        }
    }

    /// The terms `core` joins once the flattened subqueries are spliced in;
    /// the subqueries its expressions read are SELECTs of their own.
    fn core(&mut self, core: &SelectCore) -> usize {
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
            terms = terms.saturating_add(self.item_terms(&join.table, outer));
            if let Some(fsqlite_ast::JoinConstraint::On(expr)) = &join.constraint {
                self.expr(expr);
            }
        }
        terms.min(MAX_FLATTENED_FROM_TERMS + 1)
    }

    /// The terms one FROM item adds; `outer` when an outer join holds it.
    fn item_terms(&mut self, item: &TableOrSubquery, outer: bool) -> usize {
        match item {
            TableOrSubquery::Table { name, .. } if name.schema.is_none() => {
                match self.visible_cte(&name.name) {
                    Some(found) => self.cte_terms(found, outer),
                    None => 1,
                }
            }
            TableOrSubquery::Subquery { query, .. } => {
                if !outer && flattenable(query) {
                    self.core(&query.body.select)
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
            TableOrSubquery::Table { .. } => 1,
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
        let (scope, index) = found;
        let cte = self.scopes[scope][index].clone();
        let flattened = !outer
            && cte.materialized != Some(fsqlite_ast::CteMaterialized::Materialized)
            && flattenable(&cte.query);
        // The body resolves in its own WITH and the ones around it.
        let hidden = self.scopes.split_off(scope + 1);
        self.active.push(found);
        let terms = with_view_expansion_stack(|| {
            if flattened {
                self.core(&cte.query.body.select)
            } else {
                self.select(&cte.query);
                1
            }
        });
        self.active.pop();
        self.scopes.extend(hidden);
        terms
    }

    /// The subqueries of `expr`, each a SELECT of its own.
    fn expr(&mut self, expr: &Expr) {
        expr_contains_subquery_match(expr, &mut |subquery| {
            let (SubqueryExprRef::Scalar(select)
            | SubqueryExprRef::Exists(select)
            | SubqueryExprRef::In(select)) = subquery;
            self.select(select);
            false
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
        Connection::validate_join_term_limits(&select)
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
}
