use fsqlite_ast::{SelectStatement, Statement};
use fsqlite_parser::parse_first_statement_with_tail;
use fsqlite_vdbe::{
    ProgramBuilder,
    codegen::{
        CodegenContext, ColumnInfo, IndexSchema, TableSchema, codegen_delete, codegen_insert,
        codegen_select, codegen_update,
    },
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

fn parse_statement(sql: &str) -> Result<Statement, String> {
    let Some((stmt, consumed)) =
        parse_first_statement_with_tail(sql).map_err(|error| error.to_string())?
    else {
        return Err("expected a parsed statement".to_owned());
    };
    if consumed != sql.len() {
        return Err(format!(
            "parser consumed {consumed} bytes from a {} byte statement",
            sql.len()
        ));
    }

    Ok(stmt)
}

fn column(name: &str, affinity: char, is_ipk: bool) -> ColumnInfo {
    ColumnInfo::basic(name, affinity, is_ipk)
}

fn index(name: &str, root_page: i32, columns: &[&str], is_unique: bool) -> IndexSchema {
    IndexSchema {
        name: name.to_owned(),
        root_page,
        columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        key_expressions: Vec::new(),
        key_sort_directions: Vec::new(),
        where_clause: None,
        is_unique,
        key_collations: Vec::new(),
        conflict_action: None,
    }
}

fn table(
    name: &str,
    root_page: i32,
    columns: Vec<ColumnInfo>,
    indexes: Vec<IndexSchema>,
) -> TableSchema {
    TableSchema {
        name: name.to_owned(),
        root_page,
        columns,
        indexes,
        strict: false,
        without_rowid: false,
        primary_key_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        check_constraints: Vec::new(),
    }
}

fn snapshot_schema() -> Vec<TableSchema> {
    vec![
        table(
            "docs",
            2,
            vec![
                column("id", 'D', true),
                column("category_id", 'D', false),
                column("title", 'B', false),
                column("body", 'B', false),
                column("score", 'D', false),
            ],
            vec![
                index("idx_docs_category", 10, &["category_id"], false),
                index("idx_docs_title", 11, &["title"], false),
            ],
        ),
        table(
            "categories",
            3,
            vec![column("id", 'D', true), column("name", 'B', false)],
            vec![index("idx_categories_name", 12, &["name"], false)],
        ),
        table(
            "events",
            4,
            vec![
                column("id", 'D', true),
                column("category_id", 'D', false),
                column("score", 'D', false),
            ],
            vec![index("idx_events_category", 13, &["category_id"], false)],
        ),
    ]
}

fn render_bytecode_case(name: &str, sql: &str) -> Result<String, String> {
    let select = parse_select(sql)?;
    let schema = snapshot_schema();
    let mut builder = ProgramBuilder::new();
    let mut output = String::new();

    output.push_str(&format!("case: {name}\n"));
    output.push_str(&format!("sql: {sql}\n"));

    match codegen_select(&mut builder, &select, &schema, &CodegenContext::default()) {
        Ok(()) => match builder.finish() {
            Ok(program) => {
                output.push_str("status: ok\n");
                output.push_str(&format!("registers: {}\n", program.register_count()));
                match program.max_bind_parameter_index() {
                    Ok(max_index) => {
                        output.push_str(&format!("bind_parameters: {max_index}\n"));
                    }
                    Err(bad_index) => {
                        output.push_str(&format!("bind_parameters_error: {bad_index}\n"));
                    }
                }
                output.push_str(&format!(
                    "requires_attached_memdb: {}\n",
                    program.requires_attached_memdb()
                ));
                output.push_str(&format!(
                    "requires_version_store: {}\n",
                    program.requires_version_store()
                ));
                output.push('\n');
                output.push_str(&program.disassemble());
            }
            Err(error) => {
                output.push_str("status: builder_error\n");
                output.push_str(&format!("error: {error}\n"));
            }
        },
        Err(error) => {
            output.push_str("status: codegen_error\n");
            output.push_str(&format!("error: {error}\n"));
        }
    }

    Ok(output)
}

fn render_statement_bytecode_case(name: &str, sql: &str) -> Result<String, String> {
    let statement = parse_statement(sql)?;
    let schema = snapshot_schema();
    let mut builder = ProgramBuilder::new();
    let mut output = String::new();
    let dml_ctx = CodegenContext {
        concurrent_mode: true,
        rowid_alias_col_idx: Some(0),
        ..CodegenContext::default()
    };

    output.push_str(&format!("case: {name}\n"));
    output.push_str(&format!("sql: {sql}\n"));

    let codegen_result = match &statement {
        Statement::Select(select) => {
            codegen_select(&mut builder, select, &schema, &CodegenContext::default())
        }
        Statement::Insert(insert) => codegen_insert(&mut builder, insert, &schema, &dml_ctx),
        Statement::Update(update) => codegen_update(&mut builder, update, &schema, &dml_ctx),
        Statement::Delete(delete) => codegen_delete(&mut builder, delete, &schema, &dml_ctx),
        other => Err(fsqlite_vdbe::codegen::CodegenError::Unsupported(format!(
            "golden renderer does not compile {other:?}"
        ))),
    };

    match codegen_result {
        Ok(()) => match builder.finish() {
            Ok(program) => {
                output.push_str("status: ok\n");
                output.push_str(&format!("registers: {}\n", program.register_count()));
                match program.max_bind_parameter_index() {
                    Ok(max_index) => {
                        output.push_str(&format!("bind_parameters: {max_index}\n"));
                    }
                    Err(bad_index) => {
                        output.push_str(&format!("bind_parameters_error: {bad_index}\n"));
                    }
                }
                output.push_str(&format!(
                    "requires_attached_memdb: {}\n",
                    program.requires_attached_memdb()
                ));
                output.push_str(&format!(
                    "requires_version_store: {}\n",
                    program.requires_version_store()
                ));
                output.push('\n');
                output.push_str(&program.disassemble());
            }
            Err(error) => {
                output.push_str("status: builder_error\n");
                output.push_str(&format!("error: {error}\n"));
            }
        },
        Err(error) => {
            output.push_str("status: codegen_error\n");
            output.push_str(&format!("error: {error}\n"));
        }
    }

    Ok(output)
}

#[test]
fn golden_vdbe_select_bytecode_recent_stub_shapes() -> Result<(), String> {
    let join_lookup = render_bytecode_case(
        "join_lookup",
        "SELECT docs.id, categories.name \
             FROM docs JOIN categories ON docs.category_id = categories.id \
             WHERE docs.category_id = 7",
    )?;
    insta::assert_snapshot!("select_join_lookup", join_lookup);

    let grouped_aggregate = render_bytecode_case(
        "grouped_aggregate",
        "SELECT category_id, count(*), sum(score) \
             FROM docs GROUP BY category_id HAVING sum(score) > 10",
    )?;
    insta::assert_snapshot!("select_grouped_aggregate", grouped_aggregate);

    let match_predicate = render_bytecode_case(
        "match_predicate",
        "SELECT id, title FROM docs WHERE title MATCH ?1",
    )?;
    insta::assert_snapshot!("select_match_predicate", match_predicate);

    let window_boundary = render_bytecode_case(
        "window_boundary",
        "SELECT category_id, \
                    sum(score) OVER (PARTITION BY category_id ORDER BY score) \
             FROM events",
    )?;
    insta::assert_snapshot!("select_window_boundary", window_boundary);

    Ok(())
}

#[test]
fn golden_vdbe_fts5_match_bytecode_family() -> Result<(), String> {
    let body_match_with_filter = render_bytecode_case(
        "fts5_body_match_with_filter",
        "SELECT id, title, score \
             FROM docs \
             WHERE body MATCH ?1 AND category_id = 3 \
             ORDER BY score DESC",
    )?;
    insta::assert_snapshot!(
        "fts5_body_match_with_filter_ordered",
        body_match_with_filter
    );

    let title_match_with_limit = render_bytecode_case(
        "fts5_title_match_with_limit",
        "SELECT id, title \
             FROM docs \
             WHERE title MATCH 'franken sqlite' \
             ORDER BY id \
             LIMIT 5",
    )?;
    insta::assert_snapshot!("fts5_title_match_with_limit", title_match_with_limit);

    Ok(())
}

#[test]
fn golden_vdbe_window_rank_bytecode_family() -> Result<(), String> {
    let row_number_partition = render_bytecode_case(
        "window_row_number_partition",
        "SELECT category_id, \
                ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY score) \
             FROM events \
             WHERE score > 10 \
             ORDER BY category_id",
    )?;
    insta::assert_snapshot!("window_row_number_partition", row_number_partition);

    let rank_partition = render_bytecode_case(
        "window_rank_partition",
        "SELECT category_id, \
                RANK() OVER (PARTITION BY category_id ORDER BY score DESC) \
             FROM events \
             ORDER BY category_id, score DESC",
    )?;
    insta::assert_snapshot!("window_rank_partition", rank_partition);

    Ok(())
}

#[test]
fn golden_vdbe_multi_table_join_bytecode_family() -> Result<(), String> {
    let three_way_ordered = render_bytecode_case(
        "multi_join_three_way_ordered",
        "SELECT docs.id, categories.name, events.score \
             FROM docs \
             JOIN categories ON docs.category_id = categories.id \
             JOIN events ON events.category_id = categories.id \
             WHERE docs.category_id = 7 \
             ORDER BY events.score DESC",
    )?;
    insta::assert_snapshot!("multi_join_three_way_ordered", three_way_ordered);

    let filtered_join = render_bytecode_case(
        "multi_join_filtered_predicates",
        "SELECT categories.name, docs.title \
             FROM categories \
             JOIN docs ON docs.category_id = categories.id \
             JOIN events ON events.category_id = docs.category_id \
             WHERE events.score > 10 AND docs.category_id = 3",
    )?;
    insta::assert_snapshot!("multi_join_filtered_predicates", filtered_join);

    Ok(())
}

#[test]
fn golden_vdbe_aggregate_group_by_bytecode_family() -> Result<(), String> {
    let filtered_group = render_bytecode_case(
        "aggregate_group_by_filtered",
        "SELECT category_id, count(*), sum(score) \
             FROM docs \
             WHERE score > 0 \
             GROUP BY category_id \
             HAVING count(*) > 1 \
             ORDER BY category_id",
    )?;
    insta::assert_snapshot!("aggregate_group_by_filtered", filtered_group);

    let events_group = render_bytecode_case(
        "aggregate_group_by_events_limit",
        "SELECT category_id, count(*), sum(score) \
             FROM events \
             GROUP BY category_id \
             ORDER BY category_id \
             LIMIT 10",
    )?;
    insta::assert_snapshot!("aggregate_group_by_joined", events_group);

    Ok(())
}

#[test]
fn golden_vdbe_subquery_cte_bytecode_family() -> Result<(), String> {
    let in_subquery = render_statement_bytecode_case(
        "subquery_in_predicate",
        "SELECT id, title \
             FROM docs \
             WHERE category_id IN (SELECT id FROM categories WHERE name = 'docs') \
             ORDER BY id",
    )?;
    insta::assert_snapshot!("subquery_in_predicate", in_subquery);

    let cte_reference = render_statement_bytecode_case(
        "cte_reference_filter",
        "WITH picked AS (SELECT id FROM categories WHERE name = 'docs') \
             SELECT id, title FROM docs \
             WHERE category_id IN (SELECT id FROM picked)",
    )?;
    insta::assert_snapshot!("cte_reference_filter", cte_reference);

    Ok(())
}

#[test]
fn golden_vdbe_upsert_on_conflict_bytecode_family() -> Result<(), String> {
    let do_nothing = render_statement_bytecode_case(
        "upsert_on_conflict_do_nothing",
        "INSERT INTO docs (id, category_id, title, body, score) \
             VALUES (1, 2, 'title', 'body', 10) \
             ON CONFLICT (id) DO NOTHING",
    )?;
    insta::assert_snapshot!("upsert_on_conflict_do_nothing", do_nothing);

    let do_update = render_statement_bytecode_case(
        "upsert_on_conflict_do_update",
        "INSERT INTO docs (id, category_id, title, body, score) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT (id) DO UPDATE SET \
                 title = excluded.title, \
                 score = excluded.score \
             WHERE excluded.score > docs.score",
    )?;
    insta::assert_snapshot!("upsert_on_conflict_do_update", do_update);

    Ok(())
}

#[test]
fn golden_vdbe_update_delete_where_bytecode_family() -> Result<(), String> {
    let update_where = render_statement_bytecode_case(
        "update_with_where_predicate",
        "UPDATE docs \
             SET score = score + 1, title = 'updated' \
             WHERE category_id = ?1 AND score < ?2",
    )?;
    insta::assert_snapshot!("update_with_where_predicate", update_where);

    let delete_where = render_statement_bytecode_case(
        "delete_with_where_predicate",
        "DELETE FROM docs \
             WHERE category_id = ?1 AND score < ?2",
    )?;
    insta::assert_snapshot!("delete_with_where_predicate", delete_where);

    Ok(())
}
