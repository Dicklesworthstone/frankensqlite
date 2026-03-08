//! WebAssembly bindings for FrankenSQLite.
//!
//! This crate provides the WASM-compatible subset of FrankenSQLite:
//! SQL parsing, AST construction, query planning, and built-in functions.
//!
//! All OS-specific functionality (VFS, pager, WAL, MVCC, io_uring) is
//! excluded — those require the `native` feature on `fsqlite-types` and
//! OS-level primitives not available in `wasm32-unknown-unknown`.

pub use fsqlite_ast as ast;
pub use fsqlite_error as error;
pub use fsqlite_func as func;
pub use fsqlite_parser as parser;
pub use fsqlite_planner as planner;
pub use fsqlite_types as types;

/// Parse a SQL string into a list of AST statements.
///
/// Returns the parsed statements and any parse errors encountered.
pub fn parse_sql(input: &str) -> (Vec<ast::Statement>, Vec<parser::ParseError>) {
    let tokens = parser::Lexer::tokenize(input);
    let mut p = parser::Parser::new(tokens);
    p.parse_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_select() {
        let (stmts, errors) = parse_sql("SELECT 1 + 2");
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_create_table() {
        let (stmts, errors) = parse_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_error_reported() {
        let (_stmts, errors) = parse_sql("NOT VALID SQL {{{{");
        assert!(!errors.is_empty());
    }
}
