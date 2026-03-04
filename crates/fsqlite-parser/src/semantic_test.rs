use super::*;
use crate::parser::Parser;

fn make_schema() -> Schema {
    let mut schema = Schema::new();
    schema.add_table(TableDef {
        name: "users".to_owned(),
        columns: vec![
            ColumnDef {
                name: "id".to_owned(),
                affinity: TypeAffinity::Integer,
                is_ipk: true,
                not_null: true,
            },
            ColumnDef {
                name: "name".to_owned(),
                affinity: TypeAffinity::Text,
                is_ipk: false,
                not_null: true,
            },
            ColumnDef {
                name: "email".to_owned(),
                affinity: TypeAffinity::Text,
                is_ipk: false,
                not_null: false,
            },
        ],
        without_rowid: false,
        strict: false,
    });
    schema.add_table(TableDef {
        name: "orders".to_owned(),
        columns: vec![
            ColumnDef {
                name: "id".to_owned(),
                affinity: TypeAffinity::Integer,
                is_ipk: true,
                not_null: true,
            },
            ColumnDef {
                name: "user_id".to_owned(),
                affinity: TypeAffinity::Integer,
                is_ipk: false,
                not_null: true,
            },
            ColumnDef {
                name: "amount".to_owned(),
                affinity: TypeAffinity::Real,
                is_ipk: false,
                not_null: false,
            },
        ],
        without_rowid: false,
        strict: false,
    });
    schema
}

fn parse_one(sql: &str) -> Statement {
    let mut p = Parser::from_sql(sql);
    let (stmts, errs) = p.parse_all();
    assert!(errs.is_empty(), "parse errors: {errs:?}");
    assert_eq!(stmts.len(), 1);
    stmts.into_iter().next().unwrap()
}

#[test]
fn test_count_zero_args() {
    let sql = "SELECT count();";
    let (stmts, parse_errors) = crate::parser::Parser::from_sql(sql).parse_all();
    assert!(
        parse_errors.is_empty(),
        "expected no parse errors, got {parse_errors:?}"
    );
    let schema = Schema::new();
    let mut resolver = Resolver::new(&schema);
    let errors = resolver.resolve_statement(&stmts.into_iter().next().unwrap());
    assert!(errors.is_empty(), "expected no errors, got {errors:?}");
}

#[test]
fn test_update_returning_from_clause() {
    let schema = make_schema();
    let stmt = parse_one("UPDATE users SET id = 1 FROM orders WHERE users.id = orders.id RETURNING orders.id");
    let mut resolver = Resolver::new(&schema);
    let errors = resolver.resolve_statement(&stmt);
    assert!(errors.is_empty(), "SQLite allows RETURNING from FROM clause tables, got errors: {errors:?}");
}


#[test]
fn test_order_by_select_alias() {
    let schema = make_schema();
    let stmt = parse_one("SELECT id AS alias_id FROM users ORDER BY alias_id");
    let mut resolver = Resolver::new(&schema);
    let errors = resolver.resolve_statement(&stmt);
    assert!(errors.is_empty(), "Expected no errors, got {:?}", errors);
}
