use fsqlite_ast::{SelectCore, SelectStatement, Statement};
use fsqlite_parser::parse_first_statement_with_tail;
use fsqlite_planner::decision_contract::{GENESIS_HASH, build_contract};
use fsqlite_planner::{
    IndexInfo, StatsSource, TableStats, WhereTerm, classify_where_term, decompose_where,
    order_joins,
};
use serde_json::json;

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

fn table(name: &str, n_pages: u64, n_rows: u64, source: StatsSource) -> TableStats {
    TableStats {
        name: name.to_owned(),
        n_pages,
        n_rows,
        source,
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
    }
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

/// Rewrite a JSON tree so every object's keys are in lexicographic order.
///
/// `serde_json`'s object type is `IndexMap` when the `preserve_order` feature
/// is on and `BTreeMap` when it is off, so the same contract serialises to
/// insertion order or to lexicographic order depending on which crates share
/// the build. `fsqlite-ext-json` enables `preserve_order` for SQLite JSON1
/// key-order parity, and Cargo unifies that feature into every build that
/// contains both crates: these goldens therefore passed under
/// `cargo test --workspace` and failed under `cargo test -p fsqlite-planner`
/// (or vice versa) with no planner change at all. Canonicalising here pins the
/// golden bytes to one order under either resolution.
fn canonicalize(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, nested)| (key, canonicalize(nested)))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(items.into_iter().map(canonicalize).collect()),
        other => other,
    }
}

fn render_contract_case(
    name: &str,
    sql: &str,
    tables: &[TableStats],
    indexes: &[IndexInfo],
    beam_width: usize,
    star_query_detected: bool,
) -> Result<String, String> {
    let select = parse_select(sql)?;
    let terms = where_terms(&select)?;
    let plan = order_joins(tables, indexes, &terms, None, &[]);
    let contract = build_contract(
        sql,
        tables,
        indexes,
        terms.len(),
        None,
        0,
        &plan,
        beam_width,
        star_query_detected,
        GENESIS_HASH,
    );

    let stable_contract = json!({
        "case": name,
        "query_text": contract.query_text,
        "state": contract.state,
        "action": contract.action,
        "loss": contract.loss,
        "calibration": contract.calibration,
    });
    serde_json::to_string_pretty(&canonicalize(stable_contract)).map_err(|error| error.to_string())
}

#[test]
fn golden_planner_decision_contract_family() -> Result<(), String> {
    let single_table = render_contract_case(
        "decision_single_table_full_scan",
        "SELECT * FROM t",
        &[table("t", 10, 100, StatsSource::Heuristic)],
        &[],
        1,
        false,
    )?;
    insta::assert_snapshot!("decision_single_table_full_scan", single_table);

    let indexed_join = render_contract_case(
        "decision_indexed_join",
        "SELECT users.id, orders.id \
             FROM users, orders \
             WHERE users.email = ?1 AND orders.product_id = ?2",
        &[
            table("users", 200, 50_000, StatsSource::Analyze),
            table("orders", 1_000, 500_000, StatsSource::Analyze),
        ],
        &[
            index("idx_users_email", "users", &["email"], true, 30),
            index(
                "idx_orders_product_id",
                "orders",
                &["product_id"],
                false,
                80,
            ),
        ],
        5,
        false,
    )?;
    insta::assert_snapshot!("decision_indexed_join", indexed_join);

    Ok(())
}
