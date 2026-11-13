//! GH493: lower transparent, single-use CTE reads before materialization.
//!
//! A forest of plain projections/filters, each consumed exactly once, can
//! remain ordinary derived tables. It needs neither a persistent MemDatabase
//! image nor statement-local roots. Keep every body (including its filter and
//! column metadata) behind its subquery boundary; the existing SELECT
//! planner/dispatcher owns execution.
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

/// Refuse an excessively deep derived-table rewrite, not the SQL statement.
/// The original execution path still owns scopes beyond this lowering budget.
const MAX_PROJECTION_CTES: usize = 64;

struct ProjectionBody<'a> {
    columns: &'a [ResultColumn],
    from: &'a FromClause,
    filter: Option<&'a Expr>,
}

/// A CTE contributes exactly one transparent projection/filter layer. Joins
/// stay in the consumer; nested WITH scopes and materialization fences decline.
fn projection_body(cte: &fsqlite_ast::Cte) -> Option<ProjectionBody<'_>> {
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
    Some(ProjectionBody {
        columns: projected,
        from: body_from,
        filter: body_filter.as_deref(),
    })
}

/// Resolve an edge by CTE name, including forward references. A qualified
/// same-name source is deliberately refused, not mistaken for a CTE or its
/// persistent namesake. Index hints on CTEs also retain ordinary validation.
fn projection_source<'a>(
    source: &'a TableOrSubquery,
    names: &HashMap<String, usize>,
) -> Option<(&'a QualifiedName, Option<usize>)> {
    let TableOrSubquery::Table {
        name,
        index_hint,
        time_travel: None,
        ..
    } = source
    else {
        return None;
    };
    let dependency = names.get(&name.name.to_ascii_lowercase()).copied();
    if dependency.is_some() && (name.schema.is_some() || index_hint.is_some()) {
        return None;
    }
    Some((name, dependency))
}

/// Keep the original single-CTE consumer gate: no hidden dependency, callback,
/// aggregate or window may change the reference count or evaluation boundary.
fn projection_consumer_from(select: &SelectStatement) -> Option<&FromClause> {
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
    Some(from)
}

/// Lower a complete single-use dependency forest or leave the input untouched.
/// Every CTE must have exactly one reference across all sibling bodies and the
/// consumer. No expression may hide another relation. Shared, unused, cyclic,
/// recursive or unsupported scopes retain the original execution path.
fn lower_projection_cte(
    select: &SelectStatement,
    mut table_named: impl FnMut(&QualifiedName) -> Option<TableSchema>,
) -> Option<SelectStatement> {
    let with = select.with.as_ref()?;
    if with.recursive
        || with.ctes.is_empty()
        || with.ctes.len() > MAX_PROJECTION_CTES
        || !select.body.compounds.is_empty()
    {
        return None;
    }
    let mut names = HashMap::with_capacity(with.ctes.len());
    for (index, cte) in with.ctes.iter().enumerate() {
        if names.insert(cte.name.to_ascii_lowercase(), index).is_some() {
            return None;
        }
    }
    let from = projection_consumer_from(select)?;

    let mut referenced = vec![false; with.ctes.len()];
    for cte in &with.ctes {
        let body = projection_body(cte)?;
        if let (_, Some(dependency)) = projection_source(&body.from.source, &names)?
            && std::mem::replace(&mut referenced[dependency], true)
        {
            return None;
        }
    }
    for source in std::iter::once(&from.source).chain(from.joins.iter().map(|join| &join.table)) {
        let (name, dependency) = projection_source(source, &names)?;
        if let Some(dependency) = dependency {
            if std::mem::replace(&mut referenced[dependency], true) {
                return None;
            }
        } else {
            table_named(name)?;
        }
    }
    if referenced.iter().any(|used| !used) {
        return None;
    }

    let mut expanded = vec![false; with.ctes.len()];
    let mut lowered = select.clone();
    lowered.with = None;
    let SelectCore::Select { from: Some(from), .. } = &mut lowered.body.select else {
        return None;
    };
    for source in std::iter::once(&mut from.source)
        .chain(from.joins.iter_mut().map(|join| &mut join.table))
    {
        let (_, dependency) = projection_source(source, &names)?;
        if let Some(dependency) = dependency {
            let projection = expand_projection_cte(
                dependency, &with.ctes, &names, &mut table_named, &mut expanded,
            )?;
            let TableOrSubquery::Table { name, alias, .. } = source else {
                return None;
            };
            let label = alias.clone().unwrap_or_else(|| name.name.clone());
            *source = TableOrSubquery::Subquery {
                query: Box::new(projection.query),
                alias: Some(label),
            };
        }
    }
    // A disconnected cycle can have one reference per CTE without any path
    // to the consumer. It is not ours to prune or validate differently.
    expanded.iter().all(|visited| *visited).then_some(lowered)
}

struct LoweredProjection {
    query: SelectStatement,
    names: Vec<String>,
}

/// Expand each dependency once and move its derived query into its consumer.
/// The single-use check prevents exponential cloning or duplicated evaluation;
/// the scope-size budget bounds recursion. Output names flow forward through
/// each original projection, while affinity/collation remain in the AST.
fn expand_projection_cte(
    index: usize,
    ctes: &[fsqlite_ast::Cte],
    names: &HashMap<String, usize>,
    table_named: &mut impl FnMut(&QualifiedName) -> Option<TableSchema>,
    expanded: &mut [bool],
) -> Option<LoweredProjection> {
    if std::mem::replace(&mut expanded[index], true) {
        return None;
    }
    let cte = &ctes[index];
    let body = projection_body(cte)?;
    let (base, dependency) = projection_source(&body.from.source, names)?;
    let TableOrSubquery::Table { alias, .. } = &body.from.source else {
        return None;
    };
    let label = alias.as_deref().unwrap_or(&base.name);
    let (source_names, child_query) = if let Some(dependency) = dependency {
        let child = expand_projection_cte(dependency, ctes, names, table_named, expanded)?;
        (child.names, Some(child.query))
    } else {
        let table = table_named(base)?;
        (table.columns.iter().map(|column| column.name.clone()).collect(), None)
    };
    let body_column = |column: &ColumnRef| {
        column.schema.is_none()
            && column.table.as_deref().is_none_or(|qualifier| qualifier.eq_ignore_ascii_case(label))
            && source_names.iter().any(|name| name.eq_ignore_ascii_case(&column.column))
            && !is_rowid_alias(&column.column)
    };
    if body.columns.is_empty()
        || (!cte.columns.is_empty() && cte.columns.len() != body.columns.len())
        || body.filter.is_some_and(|expr| !transparent_expr(expr, &body_column, 0))
    {
        return None;
    }
    let mut output_names = Vec::with_capacity(body.columns.len());
    let mut unique_names = HashSet::new();
    for (index, column) in body.columns.iter().enumerate() {
        let ResultColumn::Expr { expr: Expr::Column(column, _), alias } = column else {
            // Keep computed/volatile projection and unused-expression errors
            // on the original path, exactly as for a single projection CTE.
            return None;
        };
        let output_name = cte.columns.get(index).map(String::as_str)
            .or(alias.as_deref()).unwrap_or(&column.column);
        if !body_column(column)
            || is_rowid_alias(output_name)
            || !unique_names.insert(output_name.to_ascii_lowercase())
        {
            return None;
        }
        output_names.push(output_name.to_owned());
    }

    let mut query = cte.query.clone();
    let SelectCore::Select { columns, from: Some(from), .. } = &mut query.body.select else {
        return None;
    };
    if let Some(child_query) = child_query {
        from.source = TableOrSubquery::Subquery {
            query: Box::new(child_query),
            alias: Some(label.to_owned()),
        };
    }
    // A derived table has no WITH column-list syntax: retain each override as
    // a projection alias, including overrides on intermediate dependencies.
    if !cte.columns.is_empty() {
        for (column, name) in columns.iter_mut().zip(&cte.columns) {
            if let ResultColumn::Expr { alias, .. } = column {
                *alias = Some(name.clone());
            }
        }
    }
    Some(LoweredProjection { query, names: output_names })
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

    #[test]
    fn projection_cte_forests_preserve_dependency_scopes() {
        for (original, expected) in [
            (
                "WITH c AS (SELECT id FROM a), d AS (SELECT id FROM c) SELECT id FROM d",
                "SELECT id FROM (SELECT id FROM (SELECT id FROM a) AS c) AS d",
            ),
            (
                "WITH c AS (SELECT id,v FROM a), d AS (SELECT id,v FROM b) \
                 SELECT c.id,c.v+d.v FROM c JOIN d USING(id) ORDER BY c.id",
                "SELECT c.id,c.v+d.v FROM (SELECT id,v FROM a) AS c \
                 JOIN (SELECT id,v FROM b) AS d USING(id) ORDER BY c.id",
            ),
            (
                "WITH Finish(k,val) AS (SELECT q.key,q.value FROM Start AS q WHERE q.value>?2), \
                 Start(key,value) AS (SELECT id,v FROM a WHERE v<?3) \
                 SELECT ?1,f.k,f.val FROM Finish AS f ORDER BY f.k",
                "SELECT ?1,f.k,f.val FROM \
                 (SELECT q.key AS k,q.value AS val FROM \
                 (SELECT id AS key,v AS value FROM a WHERE v<?3) AS q WHERE q.value>?2) AS f \
                 ORDER BY f.k",
            ),
            (
                "WITH StageOne AS (SELECT id,v FROM a), StageTwo AS (SELECT f.id,f.v FROM sTaGeOnE f) \
                 SELECT l.id,l.v FROM sTaGeTwO l WHERE l.v<3",
                "SELECT l.id,l.v FROM (SELECT f.id,f.v FROM (SELECT id,v FROM a) AS f) AS l \
                 WHERE l.v<3",
            ),
        ] {
            let Statement::Select(select) = parse_single_statement(original).unwrap() else {
                panic!("expected SELECT");
            };
            let unchanged = select.clone();
            let lowered = lower_projection_cte(&select, table_named).expect(original);
            assert!(lowered.with.is_none());
            assert_eq!(
                Statement::Select(lowered).to_string(),
                parse_single_statement(expected).unwrap().to_string(),
                "{original}",
            );
            assert_eq!(select, unchanged, "lowering must not edit the caller's AST");
        }
    }

    #[test]
    fn projection_cte_forests_refuse_shared_unused_or_opaque_dependencies() {
        for sql in [
            "WITH c AS (SELECT id FROM a), d AS (SELECT id FROM c), e AS (SELECT id FROM c) SELECT d.id FROM d JOIN e USING(id)",
            "WITH c AS (SELECT id FROM a), d AS (SELECT id FROM c) SELECT c.id FROM c JOIN d USING(id)",
            "WITH c AS (SELECT id FROM d), d AS (SELECT id FROM c) SELECT id FROM c",
            "WITH c AS (SELECT id FROM d), d AS (SELECT id FROM c) SELECT id FROM a",
            "WITH c AS (SELECT id FROM c) SELECT id FROM a",
            "WITH c AS (SELECT id FROM a), unused AS (SELECT broken FROM missing) SELECT id FROM c",
            "WITH c AS (SELECT id FROM a), C AS (SELECT id FROM b) SELECT id FROM c",
            "WITH c AS MATERIALIZED (SELECT id FROM a), d AS (SELECT id FROM c) SELECT id FROM d",
            "WITH c AS (SELECT id FROM a), d AS MATERIALIZED (SELECT id FROM c) SELECT id FROM d",
            "WITH c AS (SELECT id FROM a WHERE random()>0), d AS (SELECT id FROM c) SELECT id FROM d",
            "WITH c AS (SELECT id FROM a), d AS (SELECT random() AS id FROM c) SELECT id FROM d",
            "WITH c AS (SELECT id FROM a), d AS (SELECT id FROM c WHERE id IN (SELECT id FROM b)) SELECT id FROM d",
            "WITH c AS (SELECT id FROM a), d AS (SELECT id FROM c INDEXED BY missing) SELECT id FROM d",
            "WITH c AS (SELECT id FROM a), d AS (SELECT id FROM main.c) SELECT c.id,d.id FROM c JOIN d ON c.id=d.id",
            "WITH c AS (SELECT id FROM aux.a), d AS (SELECT id FROM c) SELECT id FROM d",
            "WITH c AS (SELECT id FROM a), d AS (SELECT v FROM c) SELECT v FROM d",
            "WITH c(x,x) AS (SELECT id,v FROM a), d AS (SELECT x FROM c) SELECT x FROM d",
            "WITH c AS (SELECT id FROM a), d AS (SELECT id FROM (WITH x AS (SELECT id FROM c) SELECT id FROM x)) SELECT id FROM d",
        ] {
            assert!(lower(sql).is_none(), "must retain the whole original scope: {sql}");
        }
    }

    #[test]
    fn projection_cte_forest_expansion_budget_is_conservative() {
        for count in [1, MAX_PROJECTION_CTES, MAX_PROJECTION_CTES + 1] {
            let bodies: Vec<_> = (0..count).map(|index| {
                let source = if index == 0 { "a".to_owned() } else { format!("c{}", index - 1) };
                format!("c{index} AS (SELECT id,v FROM {source})")
            }).collect();
            let sql = format!("WITH {} SELECT id,v FROM c{}", bodies.join(","), count - 1);
            assert_eq!(lower(&sql).is_some(), count <= MAX_PROJECTION_CTES, "{count} CTEs");
        }
    }

    #[cfg(feature = "native")]
    #[test]
    fn projection_cte_forests_lowered_sql_matches_stock() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE a(id TEXT COLLATE NOCASE,v); CREATE TABLE b(id TEXT,v); \
             INSERT INTO a VALUES ('A',1),('b','2'),('c',NULL),('d',X'31'),('e',2.5); \
             INSERT INTO b VALUES ('a',10),('b',20),('c',30),('d',40),('e',50);",
        ).unwrap();
        let rows = |sql: &str| -> Vec<Vec<rusqlite::types::Value>> {
            let mut statement = db.prepare(sql).unwrap();
            let width = statement.column_count();
            statement.query_map([], |row| {
                (0..width).map(|column| row.get(column)).collect::<rusqlite::Result<Vec<_>>>()
            }).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap()
        };
        for sql in [
            "WITH c AS (SELECT id,v FROM a), d AS (SELECT id,v FROM c) SELECT id,v FROM d ORDER BY id",
            "WITH c AS (SELECT id,v FROM a), d AS (SELECT id,v FROM b) SELECT c.id,c.v,d.v FROM c JOIN d ON c.id=d.id ORDER BY c.id",
            "WITH c AS (SELECT id,v FROM a), d AS (SELECT id,v FROM c WHERE v IS NOT NULL) SELECT id,v FROM d WHERE id='a'",
            "WITH c(k,n) AS (SELECT id,v FROM a), d(x,y) AS (SELECT k,n FROM c) SELECT x,y FROM d ORDER BY x DESC LIMIT 3 OFFSET 1",
            "WITH c AS (SELECT id,v FROM b WHERE v<20), d AS (SELECT id,v FROM a) SELECT d.id,c.v FROM d LEFT JOIN c ON d.id=c.id ORDER BY d.id",
            "WITH d AS (SELECT id,v FROM c), c AS (SELECT id,v FROM a) SELECT id,v FROM d WHERE v IS NULL",
        ] {
            let lowered = lower(sql).expect(sql).to_string();
            assert_eq!(rows(sql), rows(&lowered), "{sql}\n{lowered}");
        }
    }
}
