// bd-2tu6: §10.1–10.2 SQL Lexer and Parser
//
// Hand-written recursive descent SQL parser with Pratt precedence-climbing
// for expressions. Produces an AST from `fsqlite-ast`.

pub mod expr;
pub mod lexer;
pub mod parser;
pub mod semantic;
pub mod token;

pub use lexer::{
    Lexer, TokenizeDurationSecondsHistogram, TokenizeMetricsSnapshot, reset_tokenize_metrics,
    tokenize_metrics_snapshot,
};
pub use parser::{
    ParseError, ParseMetricsSnapshot, Parser, parse_first_statement_with_tail,
    parse_metrics_enabled, parse_metrics_snapshot, reset_parse_metrics, set_parse_metrics_enabled,
};
pub use semantic::{
    ColumnDef as SemanticColumnDef, FunctionArity, ResolveResult, Resolver, Schema, Scope,
    SemanticError, SemanticErrorKind, SemanticMetricsSnapshot, TableDef as SemanticTableDef,
    reset_semantic_metrics, semantic_metrics_snapshot,
};
#[cfg(test)]
mod semantic_test_hack;

pub use token::{Token, TokenKind};
