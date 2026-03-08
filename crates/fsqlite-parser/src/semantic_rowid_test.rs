#[cfg(test)]
mod tests {
    use crate::parser::Parser;
    use crate::semantic::*;
    use fsqlite_types::TypeAffinity;

    fn make_schema() -> Schema {
        let mut schema = Schema::new();
        schema.add_table(TableDef {
            name: "users".to_owned(),
            columns: vec![ColumnDef {
                name: "id".to_owned(),
                affinity: TypeAffinity::Integer,
                is_ipk: true,
                not_null: true,
            }],
            without_rowid: false,
            strict: false,
        });
        schema
    }

    #[test]
    fn test_rowid_resolution() {
        let schema = make_schema();
        let mut p = Parser::from_sql("SELECT rowid FROM users");
        let (stmts, _) = p.parse_all();
        let stmt = stmts.into_iter().next().unwrap();
        let mut resolver = Resolver::new(&schema);
        let errors = resolver.resolve_statement(&stmt);
        assert!(errors.is_empty(), "errors: {:?}", errors);
    }
}
