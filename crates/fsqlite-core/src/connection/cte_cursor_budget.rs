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
//! expanded (stock expands it too, so this can only under-count), and the
//! query flattener's separate limit ("too many FROM clause terms, max:
//! 200") is not modelled.

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
