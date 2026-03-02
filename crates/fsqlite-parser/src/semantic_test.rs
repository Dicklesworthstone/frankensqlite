use super::*;

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
