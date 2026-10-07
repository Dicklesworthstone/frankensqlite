//! GH493: lower transparent, single-use CTE reads before materialization.
//!
//! A plain table projection/filter used once can remain an ordinary derived
//! table. It needs neither a persistent MemDatabase image nor a statement-local
//! root. Keep the body (including its filter and column metadata) behind the
//! subquery boundary; the existing SELECT planner/dispatcher owns execution.
//!
//! This is not the general materialized/recursive CTE storage cutover. Those
//! shapes, writes, nested scopes, volatile expressions and explicit MATERIALIZED
//! fences deliberately retain the existing execution path.

#[allow(clippy::wildcard_imports)]
use super::super::*;

impl Connection {
    /// Pure AST lowering: no transactions, hydration-policy changes, SQL
    /// execution, cache invalidation or temporary storage installation.
    pub(super) fn lower_schema_only_projection_cte(
        &self,
        statement: &Statement,
    ) -> Option<Statement> {
        if !self.defer_memdb_row_hydration()
            || *self.reject_mem_fallback_strict.borrow()
            || self.bypass_compiled_cache.get()
            || self.time_travel_active.get()
        {
            return None;
        }
        let Statement::Select(select) = statement else {
            return None;
        };
        lower_projection_cte(select, |name| self.projection_cte_plain_table(name))
            .map(Statement::Select)
    }

    /// Admit only ordinary, unshadowed MAIN relations. In particular a view,
    /// a virtual table, or a live outer CTE must not be mistaken for persistent
    /// table storage. The caller's bypass-cache guard excludes materialized
    /// statement scopes; TEMP names and generated expressions decline here.
    fn projection_cte_plain_table(&self, name: &QualifiedName) -> Option<TableSchema> {
        if name
            .schema
            .as_deref()
            .is_some_and(|schema| !schema.eq_ignore_ascii_case("main"))
            || self
                .temp_table_names
                .borrow()
                .contains(&name.name.to_ascii_lowercase())
            || is_sqlite_schema_name(&name.name)
        {
            return None;
        }
        self.schema
            .borrow()
            .iter()
            .find(|table| table.name.eq_ignore_ascii_case(&name.name))
            .filter(|table| {
                table.root_page > 0
                    && table.columns.iter().all(|column| column.generated_expr.is_none())
            })
            .cloned()
    }
}

/// Return None rather than partially lowering an unsupported scope. The only
/// CTE reference must be a top-level FROM item, and no nested relation can be
/// hidden inside an expression. That makes removing the WITH scope sound
/// without changing any sibling, recursive, correlated or attached binding.
fn lower_projection_cte(
    select: &SelectStatement,
    mut table_named: impl FnMut(&QualifiedName) -> Option<TableSchema>,
) -> Option<SelectStatement> {
    let with = select.with.as_ref()?;
    if with.recursive || with.ctes.len() != 1 || !select.body.compounds.is_empty() {
        return None;
    }
    let cte = &with.ctes[0];
    if cte.materialized == Some(CteMaterialized::Materialized)
        || cte.query.with.is_some()
        || !cte.query.body.compounds.is_empty()
        || !cte.query.order_by.is_empty()
        || cte.query.limit.is_some()
    {
        return None;
    }
    let SelectCore::Select {
        distinct: Distinctness::All,
        columns: projected,
        from: Some(body_from),
        where_clause: body_filter,
        group_by,
        having: None,
        windows,
    } = &cte.query.body.select
    else {
        return None;
    };
    if !body_from.joins.is_empty() || !group_by.is_empty() || !windows.is_empty() {
        return None;
    }
    let TableOrSubquery::Table {
        name: base,
        alias: base_alias,
        time_travel: None,
        ..
    } = &body_from.source
    else {
        return None;
    };
    // Also decline qualified same-name sources: mixed MAIN/CTE shadowing is
    // intentionally outside this lowering, not inferred from a bare name.
    if base.name.eq_ignore_ascii_case(&cte.name) {
        return None;
    }
    let table = table_named(base)?;
    let base_label = base_alias.as_deref().unwrap_or(&base.name);
    let body_column = |column: &ColumnRef| {
        column.schema.is_none()
            && column
                .table
                .as_deref()
                .is_none_or(|qualifier| qualifier.eq_ignore_ascii_case(base_label))
            && table.column_index(&column.column).is_some()
            && !is_rowid_alias(&column.column)
    };
    if projected.is_empty()
        || (!cte.columns.is_empty() && cte.columns.len() != projected.len())
        || body_filter
            .as_deref()
            .is_some_and(|expr| !transparent_expr(expr, &body_column, 0))
    {
        return None;
    }
    let mut output_names = HashSet::new();
    for (index, column) in projected.iter().enumerate() {
        let ResultColumn::Expr {
            expr: Expr::Column(column, _),
            alias,
        } = column
        else {
            // No stars, computed expressions or function calls. Besides
            // volatility, this preserves unused-expression/error behavior.
            return None;
        };
        let output_name = cte
            .columns
            .get(index)
            .map(String::as_str)
            .or(alias.as_deref())
            .unwrap_or(&column.column);
        if !body_column(column)
            || is_rowid_alias(output_name)
            || !output_names.insert(output_name.to_ascii_lowercase())
        {
            return None;
        }
    }

    let SelectCore::Select {
        columns,
        from: Some(from),
        where_clause,
        group_by,
        having: None,
        windows,
        ..
    } = &select.body.select
    else {
        return None;
    };
    if !group_by.is_empty() || !windows.is_empty() {
        return None;
    }
    let outer_column = |column: &ColumnRef| {
        column.schema.is_none() && !is_rowid_alias(&column.column)
    };
    let outer_expr = |expr: &Expr| transparent_expr(expr, &outer_column, 0);
    if !columns.iter().all(|column| match column {
        ResultColumn::Expr { expr, .. } => outer_expr(expr),
        ResultColumn::Star => true,
        ResultColumn::TableStar(name) => name.schema.is_none(),
    }) || where_clause.as_deref().is_some_and(|expr| !outer_expr(expr))
        || select.order_by.iter().any(|term| !outer_expr(&term.expr))
        || select.limit.as_ref().is_some_and(|limit| {
            !outer_expr(&limit.limit)
                || limit.offset.as_ref().is_some_and(|expr| !outer_expr(expr))
        })
        || from.joins.iter().any(|join| {
            matches!(&join.constraint, Some(JoinConstraint::On(expr)) if !outer_expr(expr))
        })
    {
        return None;
    }

    let mut references = 0_usize;
    for source in std::iter::once(&from.source).chain(from.joins.iter().map(|join| &join.table)) {
        let TableOrSubquery::Table {
            name,
            index_hint,
            time_travel: None,
            ..
        } = source
        else {
            return None;
        };
        if name.name.eq_ignore_ascii_case(&cte.name) {
            if name.schema.is_some() || index_hint.is_some() {
                return None;
            }
            references += 1;
        } else {
            table_named(name)?;
        }
    }
    if references != 1 {
        return None;
    }

    let mut query = cte.query.clone();
    // WITH c(x,y) overrides the body's output names. A derived table has no
    // such column-list syntax, so carry the names as projection aliases.
    if !cte.columns.is_empty()
        && let SelectCore::Select { columns, .. } = &mut query.body.select
    {
        for (column, name) in columns.iter_mut().zip(&cte.columns) {
            if let ResultColumn::Expr { alias, .. } = column {
                *alias = Some(name.clone());
            }
        }
    }
    let mut lowered = select.clone();
    lowered.with = None;
    let SelectCore::Select { from: Some(from), .. } = &mut lowered.body.select else {
        return None;
    };
    for source in std::iter::once(&mut from.source)
        .chain(from.joins.iter_mut().map(|join| &mut join.table))
    {
        if let TableOrSubquery::Table { name, alias, .. } = source
            && name.schema.is_none()
            && name.name.eq_ignore_ascii_case(&cte.name)
        {
            let alias = alias.clone().unwrap_or_else(|| name.name.clone());
            *source = TableOrSubquery::Subquery {
                query: Box::new(query),
                alias: Some(alias),
            };
            break;
        }
    }
    Some(lowered)
}

/// A conservative, total expression gate. No hidden subqueries, IN-table
/// shorthand, callbacks, volatile values or unresolved parameter numbering.
/// The fixed recursion budget is a refusal boundary, not an execution limit.
fn transparent_expr(
    expr: &Expr,
    column_allowed: &impl Fn(&ColumnRef) -> bool,
    depth: u8,
) -> bool {
    if depth >= 64 {
        return false;
    }
    let nested = |expr: &Expr| transparent_expr(expr, column_allowed, depth + 1);
    match expr {
        Expr::Column(column, _) => column_allowed(column),
        Expr::Literal(literal, _) => !matches!(
            literal,
            Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp
        ),
        // Do not reassign anonymous/named slots by moving their AST walk
        // order from WITH to FROM. Numbered slots keep their original index.
        Expr::Placeholder(PlaceholderType::Numbered(_), _) => true,
        Expr::BinaryOp { left, right, .. } => nested(left) && nested(right),
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::IsNull { expr, .. } => nested(expr),
        Expr::Between { expr, low, high, .. } => nested(expr) && nested(low) && nested(high),
        Expr::In { expr, set: InSet::List(items), .. } => {
            nested(expr) && items.iter().all(nested)
        }
        Expr::RowValue(items, _) => items.iter().all(nested),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_named(name: &QualifiedName) -> Option<TableSchema> {
        if name.schema.as_deref().is_some_and(|schema| !schema.eq_ignore_ascii_case("main"))
            || !["a", "b"].iter().any(|table| table.eq_ignore_ascii_case(&name.name))
        {
            return None;
        }
        Some(TableSchema {
            name: name.name.clone(),
            root_page: 2,
            columns: ["id", "v"].into_iter().map(|name| ColumnInfo {
                name: name.to_owned(),
                affinity: if name == "id" { 'B' } else { 'D' },
                is_ipk: false,
                type_name: None,
                notnull: false,
                unique: false,
                default_value: None,
                strict_type: None,
                generated_expr: None,
                generated_stored: None,
                collation: None,
                conflict_action: None,
            }).collect(),
            indexes: Vec::new(),
            strict: false,
            without_rowid: false,
            primary_key_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
        })
    }

    fn lower(sql: &str) -> Option<SelectStatement> {
        let Statement::Select(select) = parse_single_statement(sql).unwrap() else {
            panic!("expected SELECT");
        };
        lower_projection_cte(&select, table_named)
    }

    #[test]
    fn projection_cte_preserves_body_aliases_and_numbered_parameters() {
        let original = "WITH c(key, val) AS (SELECT id,v FROM a WHERE v >= ?2) \
            SELECT ?1, p.key, p.val+b.v FROM c AS p JOIN b ON b.id=p.key ORDER BY p.key";
        let lowered = lower(original).expect("transparent single-use projection");
        assert!(lowered.with.is_none());
        let SelectCore::Select { from: Some(from), .. } = &lowered.body.select else {
            panic!("expected FROM");
        };
        let TableOrSubquery::Subquery { query, alias } = &from.source else {
            panic!("CTE use must become a derived table, not a bare base table");
        };
        assert_eq!(alias.as_deref(), Some("p"));
        let expected = parse_single_statement("SELECT id AS key,v AS val FROM a WHERE v >= ?2").unwrap();
        assert_eq!(Statement::Select((**query).clone()).to_string(), expected.to_string());
        let sql = lowered.to_string();
        assert!(sql.contains("?1"));
        assert!(sql.contains("?2"));
        assert_eq!(parse_single_statement(&sql).unwrap().to_string(), sql);
    }

    #[test]
    fn projection_cte_refuses_materialization_fences_and_scope_changes() {
        for sql in [
            "WITH c AS MATERIALIZED (SELECT id FROM a) SELECT id FROM c",
            "WITH RECURSIVE c(id) AS (SELECT id FROM a UNION ALL SELECT id FROM c) SELECT id FROM c",
            "WITH c AS (SELECT id FROM a) SELECT x.id FROM c x JOIN c y ON x.id=y.id",
            "WITH c AS (SELECT id FROM a) SELECT (SELECT id FROM c) FROM b",
            "WITH c AS (SELECT id FROM a) SELECT id FROM b WHERE id IN c",
            "WITH c AS (SELECT id FROM a) SELECT id FROM (SELECT id FROM c)",
            "WITH c AS (SELECT id FROM a), d AS (SELECT id FROM c) SELECT id FROM d",
            "WITH c AS (SELECT id FROM a) SELECT id FROM c UNION ALL SELECT id FROM b",
            "WITH c AS (SELECT random() AS id FROM a) SELECT id FROM c",
            "WITH c AS (SELECT id FROM a WHERE random()>0) SELECT id FROM c",
            "WITH c AS (SELECT id FROM a) SELECT random(),id FROM c",
            "WITH c AS (SELECT id FROM a LIMIT 1) SELECT id FROM c",
            "WITH c AS (SELECT DISTINCT id FROM a) SELECT id FROM c",
            "WITH c AS (SELECT id FROM a ORDER BY id) SELECT id FROM c",
            "WITH c AS (SELECT count(*) AS id FROM a) SELECT id FROM c",
            "WITH c AS (SELECT id FROM a) SELECT count(*) FROM c",
            "WITH c AS (SELECT id FROM a) SELECT rowid FROM c",
            "WITH c(x,y) AS (SELECT id FROM a) SELECT x FROM c",
            "WITH c AS (SELECT id AS x,v AS X FROM a) SELECT x FROM c",
            "WITH c AS (SELECT id FROM a) SELECT id FROM c INDEXED BY missing",
            "WITH c AS (SELECT id FROM aux.a) SELECT id FROM c",
            "WITH c AS (SELECT id FROM a) SELECT c.id FROM c JOIN aux.b ON c.id=b.id",
            "WITH c AS (SELECT id FROM missing) SELECT id FROM c",
            "WITH c AS (SELECT missing FROM a) SELECT missing FROM c",
            "WITH a AS (SELECT id FROM main.a) SELECT id FROM a",
        ] {
            assert!(lower(sql).is_none(), "must retain original execution: {sql}");
        }
    }

    #[test]
    fn projection_cte_preserves_numbering_and_effect_boundaries() {
        let column = |_: &ColumnRef| true;
        for kind in [
            PlaceholderType::Anonymous,
            PlaceholderType::ColonNamed("x".to_owned()),
            PlaceholderType::AtNamed("x".to_owned()),
            PlaceholderType::DollarNamed("x".to_owned()),
        ] {
            assert!(!transparent_expr(&Expr::Placeholder(kind, Span::ZERO), &column, 0));
        }
        assert!(transparent_expr(
            &Expr::Placeholder(PlaceholderType::Numbered(7), Span::ZERO), &column, 0
        ));
        assert!(!transparent_expr(&Expr::Literal(Literal::Integer(1), Span::ZERO), &column, 64));
        assert!(!transparent_expr(&Expr::Literal(Literal::CurrentTimestamp, Span::ZERO), &column, 0));
        for sql in [
            "WITH c AS (SELECT id,v FROM a) SELECT c.id,c.v+b.v FROM c JOIN b USING(id)",
            "WITH c AS NOT MATERIALIZED (SELECT id FROM a) SELECT id FROM c",
            "WITH c AS (SELECT id FROM a WHERE v IN (1,2,3)) SELECT id FROM c",
            "WITH c AS (SELECT id FROM a WHERE v IS NOT NULL) SELECT b.id,c.id FROM b LEFT JOIN c ON b.id=c.id",
        ] {
            assert!(lower(sql).is_some(), "must admit transparent read: {sql}");
        }
    }
}
