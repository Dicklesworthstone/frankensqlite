use fsqlite_ast::{SelectCore, SelectStatement, Statement};
use fsqlite_parser::parse_first_statement_with_tail;
use fsqlite_planner::{
    AccessPath, IndexInfo, PlannerFeatureFlags, QueryPlan, StatsSource, TableStats, WhereTerm,
    classify_where_term, decompose_where, order_joins_with_hints_and_features,
};

fn parse_select(sql: &str) -> Result<SelectStatement, String> {
    let Some((stmt, consumed)) =
        parse_first_statement_with_tail(sql).map_err(|error| error.to_string())?
    else {
        return Err("expected a parsed SELECT statement".to_owned());
    };
    if consumed != sql.len() {
        return Err(format!(
            "parser consumed {consumed} bytes from a {} byte statement",
            sql.len()
        ));
    }

    match stmt {
        Statement::Select(select) => Ok(select),
        other => Err(format!("expected SELECT statement, got {other:?}")),
    }
}

fn table(name: &str, n_pages: u64, n_rows: u64) -> TableStats {
    TableStats {
        name: name.to_owned(),
        n_pages,
        n_rows,
        source: StatsSource::Analyze,
    }
}

fn index(name: &str, table: &str, columns: &[&str], unique: bool, n_pages: u64) -> IndexInfo {
    IndexInfo {
        name: name.to_owned(),
        table: table.to_owned(),
        columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        unique,
        n_pages,
        source: StatsSource::Analyze,
        partial_where: None,
        expression_columns: Vec::new(),
        rows_per_key: Vec::new(),
    }
}

fn contains_name(names: &[&str], name: &str) -> bool {
    names
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(name))
}

fn snapshot_tables(names: &[&str]) -> Vec<TableStats> {
    vec![
        table("users", 320, 12_000),
        table("orders", 1_800, 220_000),
        table("products", 180, 8_000),
        table("events", 4_000, 1_500_000),
    ]
    .into_iter()
    .filter(|table| contains_name(names, &table.name))
    .collect()
}

fn snapshot_indexes(table_names: &[&str]) -> Vec<IndexInfo> {
    vec![
        index("idx_users_email", "users", &["email"], true, 48),
        index("idx_users_org", "users", &["org_id"], false, 80),
        index("idx_orders_user", "orders", &["user_id"], false, 360),
        index("idx_orders_product", "orders", &["product_id"], false, 280),
        index(
            "idx_products_category",
            "products",
            &["category_id"],
            false,
            36,
        ),
        index(
            "idx_events_tenant_ts",
            "events",
            &["tenant_id", "event_ts"],
            false,
            900,
        ),
    ]
    .into_iter()
    .filter(|index| contains_name(table_names, &index.table))
    .collect()
}

fn where_terms(select: &SelectStatement) -> Result<Vec<WhereTerm<'_>>, String> {
    match &select.body.select {
        SelectCore::Select { where_clause, .. } => {
            Ok(where_clause.as_deref().map_or_else(Vec::new, |expr| {
                decompose_where(expr)
                    .into_iter()
                    .map(classify_where_term)
                    .collect()
            }))
        }
        SelectCore::Values(_) => Err("VALUES cores do not produce table plans".to_owned()),
    }
}

fn render_access_path(path: &AccessPath) -> String {
    format!(
        "  table={} kind={:?} index={} rows={:.3} cost={:.3} probe={:?}",
        path.table,
        path.kind,
        path.index.as_deref().unwrap_or("(none)"),
        path.estimated_rows,
        path.estimated_cost,
        path.probe
    )
}

fn render_plan_case(
    name: &str,
    sql: &str,
    table_names: &[&str],
    features: PlannerFeatureFlags,
) -> Result<String, String> {
    let select = parse_select(sql)?;
    let terms = where_terms(&select)?;
    let tables = snapshot_tables(table_names);
    let indexes = snapshot_indexes(table_names);
    let plan = order_joins_with_hints_and_features(
        &tables,
        &indexes,
        &terms,
        None,
        &[],
        None,
        None,
        features,
    );

    Ok(render_plan(name, sql, features, &plan))
}

fn render_plan(name: &str, sql: &str, features: PlannerFeatureFlags, plan: &QueryPlan) -> String {
    let mut output = String::new();

    output.push_str(&format!("case: {name}\n"));
    output.push_str(&format!("sql: {sql}\n"));
    output.push_str(&format!(
        "features: leapfrog_join={} dpccp_join={}\n",
        features.leapfrog_join, features.dpccp_join
    ));
    output.push_str(&format!("join_order: {}\n", plan.join_order.join(" -> ")));
    output.push_str(&format!("total_cost: {:.3}\n", plan.total_cost));
    output.push_str("access_paths:\n");
    for path in &plan.access_paths {
        output.push_str(&render_access_path(path));
        output.push('\n');
    }
    output.push_str("display:\n");
    output.push_str(&plan.to_string());
    output
}

#[test]
fn golden_planner_query_plan_family() -> Result<(), String> {
    let indexed_three_way_join = render_plan_case(
        "planner_indexed_three_way_join",
        "SELECT users.id, orders.id, products.id \
             FROM users, orders, products \
             WHERE users.email = ?1 \
               AND users.id = orders.user_id \
               AND orders.product_id = products.id",
        &["users", "orders", "products"],
        PlannerFeatureFlags {
            dpccp_join: true,
            ..PlannerFeatureFlags::default()
        },
    )?;
    insta::assert_snapshot!("planner_indexed_three_way_join", indexed_three_way_join);

    let event_range_scan = render_plan_case(
        "planner_event_range_scan",
        "SELECT tenant_id, count(*) \
             FROM events \
             WHERE tenant_id = ?1 AND event_ts >= ?2 AND event_ts < ?3 \
             GROUP BY tenant_id",
        &["events"],
        PlannerFeatureFlags::default(),
    )?;
    insta::assert_snapshot!("planner_event_range_scan", event_range_scan);

    Ok(())
}
