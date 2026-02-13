// bd-2tu6: §10.1–10.2 SQL Lexer and Parser
//
// Hand-written recursive descent SQL parser with Pratt precedence-climbing
// for expressions. Produces an AST from `fsqlite-ast`.

pub mod expr;
pub mod lexer;
pub mod parser;
pub mod token;

pub use lexer::{
    reset_tokenize_metrics, tokenize_metrics_snapshot, Lexer, TokenizeDurationSecondsHistogram,
    TokenizeMetricsSnapshot,
};
pub use parser::{ParseError, Parser};
pub use token::{Token, TokenKind};
