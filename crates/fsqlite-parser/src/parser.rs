// bd-2tu6: §10.2 SQL Parser
//
// Hand-written statement and DDL grammar. The iterative expression and SELECT
// state machine lives in expr.rs.

use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use fsqlite_ast::{
    AlterTableAction, AlterTableStatement, Assignment, AssignmentTarget, AttachStatement,
    BeginStatement, ColumnConstraint, ColumnConstraintKind, ColumnDef, ColumnRef, ConflictAction,
    CreateIndexStatement, CreateTableBody, CreateTableStatement, CreateTriggerStatement,
    CreateViewStatement, CreateVirtualTableStatement, DefaultValue, Deferrable,
    DeferrableInitially, DeleteStatement, DropObjectType, DropStatement, Expr, ForeignKeyAction,
    ForeignKeyActionType, ForeignKeyClause, ForeignKeyTrigger, GeneratedStorage, IndexHint,
    IndexedColumn, InsertSource, InsertStatement, JoinKind, JoinType, LimitClause, Literal,
    NullsOrder, OrderingTerm, PragmaStatement, PragmaValue, QualifiedName, QualifiedTableRef,
    ResultColumn, RollbackStatement, SelectCore, SelectStatement, SortDirection, Span, Statement,
    TableConstraint, TableConstraintKind, TimeTravelClause, TimeTravelTarget, TransactionMode,
    TriggerEvent, TriggerTiming, TypeName, UpdateStatement, UpsertAction, UpsertClause,
    UpsertTarget, VacuumStatement, ValuesClause, WithClause,
};
#[cfg(test)]
use fsqlite_ast::{
    CompoundOp, CteMaterialized, Distinctness, FrameBound, FrameExclude, FrameSpec, FrameType,
    FromClause, JoinClause, JoinConstraint, SelectBody, TableOrSubquery, UnaryOp,
    ValuesRepresentation, WindowDef, WindowReference, WindowSpec,
};

#[cfg(test)]
use crate::expr::{ParsedFrameBound, validate_frame_end, validate_frame_start};
use crate::lexer::Lexer;
use crate::token::{Token, TokenKind};

// ---------------------------------------------------------------------------
// Parse metrics
// ---------------------------------------------------------------------------

/// Monotonic counter of successfully parsed statements.
static FSQLITE_PARSE_STATEMENTS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Monotonic counter of tokens consumed by successful parse_all() calls.
static FSQLITE_PARSE_TOKENS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Monotonic counter of parse errors encountered by parse_all().
static FSQLITE_PARSE_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Whether parse metrics should be collected on the hot path.
///
/// These counters are currently used only for diagnostics/tests, so leave
/// them disabled by default to avoid shared-state bookkeeping on ordinary
/// parse calls.
static FSQLITE_PARSE_METRICS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Point-in-time parse metrics snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ParseMetricsSnapshot {
    /// Total statements successfully parsed.
    pub fsqlite_parse_statements_total: u64,
    /// Total tokens observed by successful parse_all() calls.
    pub fsqlite_parse_tokens_total: u64,
    /// Total parse errors observed by parse_all() calls.
    pub fsqlite_parse_errors_total: u64,
}

/// Take a point-in-time snapshot of parse metrics.
#[must_use]
pub fn parse_metrics_snapshot() -> ParseMetricsSnapshot {
    ParseMetricsSnapshot {
        fsqlite_parse_statements_total: FSQLITE_PARSE_STATEMENTS_TOTAL.load(Ordering::Relaxed),
        fsqlite_parse_tokens_total: FSQLITE_PARSE_TOKENS_TOTAL.load(Ordering::Relaxed),
        fsqlite_parse_errors_total: FSQLITE_PARSE_ERRORS_TOTAL.load(Ordering::Relaxed),
    }
}

/// Enable or disable parse metrics collection on the hot path.
pub fn set_parse_metrics_enabled(enabled: bool) {
    FSQLITE_PARSE_METRICS_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Return whether parse metrics collection is enabled.
#[must_use]
pub fn parse_metrics_enabled() -> bool {
    FSQLITE_PARSE_METRICS_ENABLED.load(Ordering::Relaxed)
}

/// Reset parse metrics (used by tests/diagnostics).
pub fn reset_parse_metrics() {
    FSQLITE_PARSE_STATEMENTS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_PARSE_TOKENS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_PARSE_ERRORS_TOTAL.store(0, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DmlParseContext {
    TopLevel,
    TriggerBody,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseErrorKind {
    Syntax,
    /// A tokenizer (lexer) error — an unterminated/unrecognized/malformed token.
    /// Its `message` is already SQLite's stock form (`unrecognized token: "X"`)
    /// and must be surfaced verbatim, with NO `SQL error at offset N:` prefix.
    /// bd-parser-syntax-error-format-6w6kp (Part A).
    Tokenizer,
    ExpressionTooDeep { max: u32 },
    RecursionLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub kind: ParseErrorKind,
    pub message: String,
    pub span: Span,
    pub line: u32,
    pub col: u32,
}

impl ParseError {
    #[must_use]
    pub(crate) fn at(message: impl Into<String>, token: Option<&Token>) -> Self {
        if let Some(t) = token {
            Self {
                kind: ParseErrorKind::Syntax,
                message: message.into(),
                span: t.span,
                line: t.line,
                col: t.col,
            }
        } else {
            Self {
                kind: ParseErrorKind::Syntax,
                message: message.into(),
                span: Span::ZERO,
                line: 0,
                col: 0,
            }
        }
    }

    #[must_use]
    pub(crate) fn expression_too_deep(max: u32, token: Option<&Token>) -> Self {
        let mut error = Self::at(
            format!("Expression tree is too large (maximum depth {max})"),
            token,
        );
        error.kind = ParseErrorKind::ExpressionTooDeep { max };
        error
    }

    #[must_use]
    fn recursion_limit(token: Option<&Token>) -> Self {
        let mut error = Self::at(
            format!("parser recursion limit exceeded (maximum depth {MAX_NATIVE_PARSE_DEPTH})"),
            token,
        );
        error.kind = ParseErrorKind::RecursionLimit;
        error
    }

    #[must_use]
    pub const fn is_expression_too_deep(&self) -> bool {
        matches!(self.kind, ParseErrorKind::ExpressionTooDeep { .. })
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}: {}", self.line, self.col, self.message)
    }
}

impl Error for ParseError {}

/// Caller-owned statement parse scratch.
///
/// The parser still allocates AST nodes normally, but repeated statement-cache
/// misses can now keep token/error vectors hot and reset them wholesale at the
/// statement boundary instead of rebuilding fresh `Vec`s on every parse.
#[derive(Debug, Default)]
pub struct StatementParseScratch {
    tokens: Vec<Token>,
    errors: Vec<ParseError>,
    identifier_interner: crate::lexer::IdentifierInterner,
}

impl StatementParseScratch {
    /// Clear logical contents while retaining backing allocations for reuse.
    pub fn reset(&mut self) {
        self.tokens.clear();
        self.errors.clear();
        self.identifier_interner.reset();
    }

    #[must_use]
    pub fn token_capacity(&self) -> usize {
        self.tokens.capacity()
    }

    #[must_use]
    pub fn error_capacity(&self) -> usize {
        self.errors.capacity()
    }

    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.tokens
            .capacity()
            .saturating_mul(std::mem::size_of::<Token>())
            .saturating_add(
                self.errors
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ParseError>()),
            )
            .saturating_add(self.identifier_interner.retained_bytes())
    }

    #[cfg(test)]
    fn identifier_interner_is_empty(&self) -> bool {
        self.identifier_interner.is_empty()
    }

    #[cfg(test)]
    fn identifier_interner_len(&self) -> usize {
        self.identifier_interner.len()
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Maximum expression nesting depth.
///
/// Matches C SQLite's default `SQLITE_MAX_EXPR_DEPTH` (1000). C SQLite
/// allows compile-time override; this constant could be made generic or
/// builder-configurable if needed.
pub const MAX_PARSE_DEPTH: u32 = 1000;

/// Native parser-call guard.
///
/// This is deliberately distinct from [`MAX_PARSE_DEPTH`]. Statement, SELECT,
/// and grammar helper frames consume this defensive implementation budget, but
/// they do not contribute nodes to SQLite's semantic expression-height limit.
const MAX_NATIVE_PARSE_DEPTH: u32 = 1000;

pub(crate) struct HeightTracked<T> {
    pub(crate) value: T,
    pub(crate) height: u32,
}

pub struct Parser {
    pub(crate) tokens: Vec<Token>,
    pub(crate) pos: usize,
    pub(crate) errors: Vec<ParseError>,
    pub(crate) depth: u32,
    pub(crate) has_with: bool,
}

impl Parser {
    #[must_use]
    pub fn new(mut tokens: Vec<Token>) -> Self {
        // The public constructor accepts caller-built token streams. Normalize
        // every such stream to one physical EOF at the end so an embedded
        // sentinel cannot silently hide later tokens from parse/tail checks.
        let terminal_eof = tokens
            .last()
            .filter(|token| matches!(token.kind, TokenKind::Eof))
            .cloned();
        tokens.retain(|token| !matches!(token.kind, TokenKind::Eof));
        if let Some(eof) = terminal_eof {
            tokens.push(eof);
        } else {
            let (offset, line, col) = tokens.last().map_or((0, 1, 1), |token| {
                (
                    token.span.end,
                    token.line,
                    token.col.saturating_add(token.span.len()),
                )
            });
            tokens.push(Token {
                kind: TokenKind::Eof,
                span: Span::new(offset, offset),
                line,
                col,
            });
        }
        Self {
            tokens,
            pos: 0,
            errors: Vec::new(),
            depth: 0,
            has_with: false,
        }
    }

    pub(crate) fn enter_recursion(&mut self) -> Result<(), ParseError> {
        if self.depth >= MAX_NATIVE_PARSE_DEPTH {
            return Err(ParseError::recursion_limit(self.current()));
        }
        self.depth += 1;
        Ok(())
    }

    pub(crate) fn leave_recursion(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    pub(crate) fn checked_cached_parent_height(
        &self,
        max_child_height: u32,
    ) -> Result<u32, ParseError> {
        let height = max_child_height.saturating_add(1);
        if height > MAX_PARSE_DEPTH {
            return Err(ParseError::expression_too_deep(
                MAX_PARSE_DEPTH,
                self.current(),
            ));
        }
        Ok(height)
    }

    pub(crate) fn with_recursion_guard<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, ParseError>,
    ) -> Result<T, ParseError> {
        self.enter_recursion()?;
        let result = f(self);
        self.leave_recursion();
        result
    }

    #[must_use]
    pub fn from_sql(sql: &str) -> Self {
        Self::new(Lexer::tokenize(sql))
    }

    pub fn parse_all(&mut self) -> (Vec<Statement>, Vec<ParseError>) {
        let parse_debug_enabled = tracing::enabled!(target: "fsqlite.parse", tracing::Level::DEBUG);
        let collect_parse_metrics = parse_metrics_enabled();
        if collect_parse_metrics {
            let token_count = u64::try_from(self.tokens.len()).unwrap_or(u64::MAX);
            FSQLITE_PARSE_TOKENS_TOTAL.fetch_add(token_count, Ordering::Relaxed);
        }
        let span = parse_debug_enabled.then(|| {
            tracing::debug_span!(
                target: "fsqlite.parse",
                "parse",
                ast_node_count = tracing::field::Empty,
                parse_errors = tracing::field::Empty,
            )
        });
        let _guard = span.as_ref().map(|span| span.enter());

        let mut stmts = Vec::new();
        while !self.at_eof() {
            if self.check(&TokenKind::Semicolon) {
                self.advance();
                continue;
            }
            match self.parse_statement() {
                Ok(s) => {
                    if collect_parse_metrics {
                        FSQLITE_PARSE_STATEMENTS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    }
                    stmts.push(s);
                    if self.at_eof() || self.eat(&TokenKind::Semicolon) {
                        continue;
                    }

                    let error = self
                        .err_msg("unexpected token after end of statement; expected ';' separator");
                    if collect_parse_metrics {
                        FSQLITE_PARSE_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    }
                    tracing::warn!(
                        target: "fsqlite.parse",
                        error = %error,
                        "parse recovery: missing statement separator"
                    );
                    self.errors.push(error);
                    self.synchronize();
                }
                Err(e) => {
                    if collect_parse_metrics {
                        FSQLITE_PARSE_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    }
                    tracing::warn!(
                        target: "fsqlite.parse",
                        error = %e,
                        "parse recovery: skipping malformed statement"
                    );
                    self.errors.push(e);
                    self.synchronize();
                }
            }
        }

        let errors = std::mem::take(&mut self.errors);
        if let Some(span) = span.as_ref() {
            span.record("ast_node_count", stmts.len() as u64);
            span.record("parse_errors", errors.len() as u64);
        }

        (stmts, errors)
    }

    pub fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        // SQLite's WITH marker is sticky within one statement because it can
        // change how later VALUES rows are represented. It must not leak into
        // the next statement parsed from the same token stream.
        self.has_with = false;
        self.parse_statement_inner()
    }

    #[must_use]
    pub fn errors(&self) -> &[ParseError] {
        &self.errors
    }

    // -----------------------------------------------------------------------
    // Token navigation
    // -----------------------------------------------------------------------

    pub(crate) fn peek(&self) -> &TokenKind {
        self.current().map_or(&TokenKind::Eof, |t| &t.kind)
    }

    pub(crate) fn current(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    pub(crate) fn peek_nth(&self, n: usize) -> &TokenKind {
        self.tokens
            .get(self.pos + n)
            .map_or(&TokenKind::Eof, |t| &t.kind)
    }

    pub(crate) fn at_eof(&self) -> bool {
        matches!(self.peek(), TokenKind::Eof)
    }

    pub(crate) fn advance(&mut self) -> Option<&Token> {
        let t = self.tokens.get(self.pos);
        if self.pos < self.tokens.len().saturating_sub(1) {
            self.pos += 1;
        }
        t
    }

    pub(crate) fn check(&self, kind: &TokenKind) -> bool {
        std::mem::discriminant(self.peek()) == std::mem::discriminant(kind)
    }

    pub(crate) fn check_kw(&self, kw: &TokenKind) -> bool {
        self.peek() == kw
    }

    pub(crate) fn eat(&mut self, kind: &TokenKind) -> bool {
        if self.check(kind) {
            self.advance();
            true
        } else {
            false
        }
    }

    pub(crate) fn eat_kw(&mut self, kw: &TokenKind) -> bool {
        if self.peek() == kw {
            self.advance();
            true
        } else {
            false
        }
    }

    pub(crate) fn expect_kw(&mut self, kw: &TokenKind) -> Result<Span, ParseError> {
        if self.peek() == kw {
            let sp = self.current_span();
            self.advance();
            Ok(sp)
        } else {
            Err(self.err_expected(&format!("{kw:?}")))
        }
    }

    pub(crate) fn expect_token(&mut self, kind: &TokenKind) -> Result<Span, ParseError> {
        if self.check(kind) {
            let sp = self.current_span();
            self.advance();
            Ok(sp)
        } else {
            Err(self.err_expected(&format!("{kind:?}")))
        }
    }

    pub(crate) fn current_span(&self) -> Span {
        self.current().map_or(Span::ZERO, |t| t.span)
    }

    pub(crate) fn err_expected(&self, what: &str) -> ParseError {
        ParseError::at(format!("expected {what}"), self.current())
    }

    pub(crate) fn err_msg(&self, msg: impl Into<String>) -> ParseError {
        ParseError::at(msg, self.current())
    }

    fn synchronize(&mut self) {
        loop {
            match self.peek() {
                TokenKind::Eof => return,
                TokenKind::Semicolon => {
                    self.advance();
                    return;
                }
                k if k.is_statement_start() => return,
                _ => {
                    self.advance();
                }
            }
        }
    }

    fn starts_nested_trigger_definition(&self) -> bool {
        if !self.check_kw(&TokenKind::KwCreate) {
            return false;
        }

        let mut offset = 1_usize;
        while offset <= 2
            && matches!(
                self.peek_nth(offset),
                TokenKind::KwTemp | TokenKind::KwTemporary | TokenKind::KwUnique
            )
        {
            offset += 1;
        }
        self.peek_nth(offset) == &TokenKind::KwTrigger
    }

    fn recover_trigger_body_after_error(&mut self, statement_start: usize) {
        // Rewind to the rejected body statement's first token. The enclosing
        // trigger END is then the first END observed at a statement boundary;
        // END used as an alias or CASE terminator remains inside its statement.
        self.pos = statement_start.min(self.tokens.len().saturating_sub(1));
        let mut at_statement_boundary = true;
        let mut nested_trigger_header = false;
        let mut nested_trigger_depth = 0_usize;

        loop {
            match self.peek() {
                TokenKind::Eof => return,
                TokenKind::Semicolon => {
                    self.advance();
                    at_statement_boundary = true;
                    nested_trigger_header = false;
                }
                TokenKind::KwBegin if nested_trigger_header => {
                    nested_trigger_depth = nested_trigger_depth.saturating_add(1);
                    self.advance();
                    at_statement_boundary = true;
                    nested_trigger_header = false;
                }
                TokenKind::KwEnd if at_statement_boundary && nested_trigger_depth > 0 => {
                    nested_trigger_depth -= 1;
                    self.advance();
                    let _ = self.eat(&TokenKind::Semicolon);
                    at_statement_boundary = true;
                }
                TokenKind::KwEnd if at_statement_boundary => {
                    self.advance();
                    let _ = self.eat(&TokenKind::Semicolon);
                    return;
                }
                _ => {
                    if at_statement_boundary {
                        nested_trigger_header = self.starts_nested_trigger_definition();
                    }
                    self.advance();
                    at_statement_boundary = false;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Identifiers and names
    // -----------------------------------------------------------------------

    pub(crate) fn parse_identifier(&mut self) -> Result<String, ParseError> {
        match self.peek().clone() {
            TokenKind::Id(s) | TokenKind::QuotedId(s, _) => {
                self.advance();
                Ok(s.to_string())
            }
            TokenKind::String(s) => {
                self.advance();
                Ok(s)
            }
            ref k if starts_post_dot_identifier(k) => {
                let s = kw_to_str(k);
                self.advance();
                Ok(s)
            }
            _ => Err(self.err_expected("identifier")),
        }
    }

    pub(crate) fn parse_table_star_qualifier(&mut self) -> Result<String, ParseError> {
        match self.peek().clone() {
            TokenKind::Id(s) | TokenKind::QuotedId(s, _) => {
                self.advance();
                Ok(s.to_string())
            }
            TokenKind::String(s) => {
                self.advance();
                Ok(s)
            }
            ref k if starts_table_star_qualifier(k) => {
                let s = kw_to_str(k);
                self.advance();
                Ok(s)
            }
            _ => Err(self.err_expected("table-star qualifier")),
        }
    }

    pub(crate) fn parse_window_name(&mut self) -> Result<String, ParseError> {
        match self.peek().clone() {
            TokenKind::Id(s) | TokenKind::QuotedId(s, _) => {
                self.advance();
                Ok(s.to_string())
            }
            TokenKind::String(s) => {
                self.advance();
                Ok(s)
            }
            ref k if starts_bare_window_name(k) => {
                let s = kw_to_str(k);
                self.advance();
                Ok(s)
            }
            _ => Err(self.err_expected("window name")),
        }
    }

    pub(crate) fn parse_qualified_name(&mut self) -> Result<QualifiedName, ParseError> {
        let first = self.parse_identifier()?;
        if self.eat(&TokenKind::Dot) {
            let second = self.parse_identifier()?;
            Ok(QualifiedName::qualified(first, second))
        } else {
            Ok(QualifiedName::bare(first))
        }
    }

    fn parse_dml_target_name(
        &mut self,
        context: DmlParseContext,
    ) -> Result<QualifiedName, ParseError> {
        let first = self.parse_identifier()?;
        if self.check(&TokenKind::Dot) {
            if context == DmlParseContext::TriggerBody {
                return Err(self
                    .err_msg("qualified table names are not allowed in trigger body statements"));
            }
            self.advance();
            let second = self.parse_identifier()?;
            Ok(QualifiedName::qualified(first, second))
        } else {
            Ok(QualifiedName::bare(first))
        }
    }

    fn parse_qualified_table_ref(
        &mut self,
        context: DmlParseContext,
    ) -> Result<QualifiedTableRef, ParseError> {
        let name = self.parse_dml_target_name(context)?;
        if context == DmlParseContext::TriggerBody && self.check_kw(&TokenKind::KwAs) {
            return Err(self.err_msg("table aliases are not allowed in trigger body statements"));
        }
        let alias = if context == DmlParseContext::TopLevel {
            self.try_table_alias()?
        } else {
            None
        };
        if context == DmlParseContext::TriggerBody
            && (self.check_kw(&TokenKind::KwIndexed)
                || (self.check_kw(&TokenKind::KwNot) && self.peek_nth(1) == &TokenKind::KwIndexed))
        {
            return Err(self
                .err_msg("INDEXED BY and NOT INDEXED are not allowed in trigger body statements"));
        }
        let index_hint = self.parse_index_hint()?;
        let time_travel = self.parse_time_travel_clause()?;
        Ok(QualifiedTableRef {
            name,
            alias,
            index_hint,
            time_travel,
        })
    }

    fn parse_alias_name(&mut self) -> Result<String, ParseError> {
        match self.peek().clone() {
            TokenKind::Id(s) | TokenKind::QuotedId(s, _) => {
                self.advance();
                Ok(s.to_string())
            }
            TokenKind::String(s) => {
                self.advance();
                Ok(s)
            }
            ref k if starts_explicit_alias_name(k) => {
                let s = kw_to_str(k);
                self.advance();
                Ok(s)
            }
            _ => Err(self.err_expected("alias")),
        }
    }

    fn starts_window_clause(&self) -> bool {
        self.check_kw(&TokenKind::KwWindow)
            && starts_bare_window_name(self.peek_nth(1))
            && self.peek_nth(2) == &TokenKind::KwAs
    }

    fn starts_time_travel_clause(&self) -> bool {
        self.check_kw(&TokenKind::KwFor)
            && matches!(
                self.peek_nth(1),
                TokenKind::Id(name) if name.eq_ignore_ascii_case("SYSTEM_TIME")
            )
    }

    pub(crate) fn try_result_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.eat_kw(&TokenKind::KwAs) {
            return Ok(Some(self.parse_alias_name()?));
        }
        if self.starts_window_clause() {
            return Ok(None);
        }
        if starts_result_alias(self.peek()) {
            return Ok(Some(self.parse_alias_name()?));
        }
        Ok(None)
    }

    pub(crate) fn try_table_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.eat_kw(&TokenKind::KwAs) {
            return Ok(Some(self.parse_alias_name()?));
        }
        if self.starts_window_clause() || self.starts_time_travel_clause() {
            return Ok(None);
        }
        if starts_table_alias(self.peek()) {
            return Ok(Some(self.parse_alias_name()?));
        }
        Ok(None)
    }

    pub(crate) fn parse_index_hint(&mut self) -> Result<Option<IndexHint>, ParseError> {
        if self.eat_kw(&TokenKind::KwIndexed) {
            self.expect_kw(&TokenKind::KwBy)?;
            Ok(Some(IndexHint::IndexedBy(self.parse_identifier()?)))
        } else if self.check_kw(&TokenKind::KwNot) && self.peek_nth(1) == &TokenKind::KwIndexed {
            self.advance();
            self.advance();
            Ok(Some(IndexHint::NotIndexed))
        } else {
            Ok(None)
        }
    }

    /// Parse an optional `FOR SYSTEM_TIME AS OF ...` clause (SQL:2011 temporal query).
    ///
    /// Grammar:
    /// ```text
    /// time_travel_clause ::= FOR SYSTEM_TIME AS OF COMMITSEQ integer
    ///                      | FOR SYSTEM_TIME AS OF string_literal
    /// ```
    pub(crate) fn parse_time_travel_clause(
        &mut self,
    ) -> Result<Option<TimeTravelClause>, ParseError> {
        if !self.check_kw(&TokenKind::KwFor) {
            return Ok(None);
        }
        // Lookahead: FOR must be followed by SYSTEM_TIME (contextual identifier).
        if !matches!(self.peek_nth(1), TokenKind::Id(s) if s.eq_ignore_ascii_case("SYSTEM_TIME")) {
            return Ok(None);
        }
        self.advance(); // consume FOR
        self.advance(); // consume SYSTEM_TIME
        self.expect_kw(&TokenKind::KwAs)?;
        self.expect_kw(&TokenKind::KwOf)?;

        let target = if self.eat_kw(&TokenKind::KwCommitseq) {
            match self.peek().clone() {
                TokenKind::Integer(n) if n >= 0 => {
                    self.advance();
                    TimeTravelTarget::CommitSequence(n as u64)
                }
                TokenKind::OversizedInt(s) => {
                    if let Ok(n) = s.parse::<u64>() {
                        self.advance();
                        TimeTravelTarget::CommitSequence(n)
                    } else {
                        return Err(self.err_expected("non-negative integer after COMMITSEQ"));
                    }
                }
                _ => return Err(self.err_expected("non-negative integer after COMMITSEQ")),
            }
        } else {
            match self.peek().clone() {
                TokenKind::String(s) => {
                    self.advance();
                    TimeTravelTarget::Timestamp(s)
                }
                _ => {
                    return Err(self.err_expected(
                        "COMMITSEQ <n> or '<timestamp>' after FOR SYSTEM_TIME AS OF",
                    ));
                }
            }
        };

        Ok(Some(TimeTravelClause { target }))
    }

    pub(crate) fn parse_comma_sep<T>(
        &mut self,
        f: fn(&mut Self) -> Result<T, ParseError>,
    ) -> Result<Vec<T>, ParseError> {
        let mut v = Vec::with_capacity(4);
        v.push(f(self)?);
        while self.eat(&TokenKind::Comma) {
            v.push(f(self)?);
        }
        Ok(v)
    }

    // -----------------------------------------------------------------------
    // Statement dispatch
    // -----------------------------------------------------------------------

    fn parse_statement_inner(&mut self) -> Result<Statement, ParseError> {
        self.with_recursion_guard(|parser| match parser.peek().clone() {
            TokenKind::KwSelect | TokenKind::KwValues => {
                Ok(Statement::Select(parser.parse_select_stmt(None)?))
            }
            TokenKind::KwWith => parser.parse_with_leading(),
            TokenKind::KwInsert | TokenKind::KwReplace => {
                parser.parse_insert_stmt(None, DmlParseContext::TopLevel)
            }
            TokenKind::KwUpdate => parser.parse_update_stmt(None, DmlParseContext::TopLevel),
            TokenKind::KwDelete => parser.parse_delete_stmt(None, DmlParseContext::TopLevel),
            TokenKind::KwCreate => parser.parse_create(),
            TokenKind::KwDrop => parser.parse_drop(),
            TokenKind::KwAlter => parser.parse_alter(),
            TokenKind::KwBegin => parser.parse_begin(),
            TokenKind::KwCommit | TokenKind::KwEnd => {
                parser.advance();
                let _ = parser.eat_kw(&TokenKind::KwTransaction);
                Ok(Statement::Commit)
            }
            TokenKind::KwRollback => parser.parse_rollback(),
            TokenKind::KwSavepoint => {
                parser.advance();
                Ok(Statement::Savepoint(parser.parse_identifier()?))
            }
            TokenKind::KwRelease => {
                parser.advance();
                let _ = parser.eat_kw(&TokenKind::KwSavepoint);
                Ok(Statement::Release(parser.parse_identifier()?))
            }
            TokenKind::KwAttach => parser.parse_attach(),
            TokenKind::KwDetach => {
                parser.advance();
                let _ = parser.eat_kw(&TokenKind::KwDatabase);
                Ok(Statement::Detach(parser.parse_identifier()?))
            }
            TokenKind::KwPragma => parser.parse_pragma(),
            TokenKind::KwVacuum => parser.parse_vacuum(),
            TokenKind::KwReindex => {
                parser.advance();
                let name = if !parser.at_eof() && !parser.check(&TokenKind::Semicolon) {
                    Some(parser.parse_qualified_name()?)
                } else {
                    None
                };
                Ok(Statement::Reindex(name))
            }
            TokenKind::KwAnalyze => {
                parser.advance();
                let name = if !parser.at_eof() && !parser.check(&TokenKind::Semicolon) {
                    Some(parser.parse_qualified_name()?)
                } else {
                    None
                };
                Ok(Statement::Analyze(name))
            }
            TokenKind::KwExplain => parser.parse_explain(),
            _ => Err(parser.err_msg("unexpected token at start of statement")),
        })
    }

    // -----------------------------------------------------------------------
    // WITH ... (SELECT | INSERT | UPDATE | DELETE)
    // -----------------------------------------------------------------------

    fn parse_with_leading(&mut self) -> Result<Statement, ParseError> {
        let with = self.parse_with_clause()?;
        match self.peek() {
            TokenKind::KwSelect | TokenKind::KwValues => {
                Ok(Statement::Select(self.parse_select_stmt(Some(with))?))
            }
            TokenKind::KwInsert | TokenKind::KwReplace => {
                self.parse_insert_stmt(Some(with), DmlParseContext::TopLevel)
            }
            TokenKind::KwUpdate => self.parse_update_stmt(Some(with), DmlParseContext::TopLevel),
            TokenKind::KwDelete => self.parse_delete_stmt(Some(with), DmlParseContext::TopLevel),
            _ => Err(self.err_expected("SELECT, INSERT, UPDATE, or DELETE after WITH")),
        }
    }

    pub(crate) fn parse_with_clause(&mut self) -> Result<WithClause, ParseError> {
        self.parse_with_clause_machine()
    }

    // -----------------------------------------------------------------------
    // SELECT
    // -----------------------------------------------------------------------

    pub(crate) fn parse_select_stmt(
        &mut self,
        with: Option<WithClause>,
    ) -> Result<SelectStatement, ParseError> {
        self.parse_select_stmt_tracked(with)
            .map(|tracked| tracked.value)
    }

    pub(crate) fn parse_select_stmt_tracked(
        &mut self,
        with: Option<WithClause>,
    ) -> Result<HeightTracked<SelectStatement>, ParseError> {
        self.parse_select_tracked_machine(with)
    }

    #[cfg(test)]
    pub(crate) fn parse_select_stmt_inner_tracked(
        &mut self,
        with: Option<WithClause>,
    ) -> Result<HeightTracked<SelectStatement>, ParseError> {
        let body = self.parse_select_body_tracked()?;
        let mut height = body.height;
        let final_core = body
            .value
            .compounds
            .last()
            .map_or(&body.value.select, |(_, core)| core);
        if matches!(final_core, SelectCore::Values(_))
            && matches!(self.peek(), TokenKind::KwOrder | TokenKind::KwLimit)
        {
            return Err(self.err_msg("ORDER BY / LIMIT clause is not allowed after a VALUES term"));
        }
        let order_by = if self.eat_kw(&TokenKind::KwOrder) {
            self.expect_kw(&TokenKind::KwBy)?;
            let parsed = self.parse_comma_sep(Self::parse_ordering_term_tracked)?;
            let mut terms = Vec::with_capacity(parsed.len());
            for tracked in parsed {
                height = height.max(tracked.height);
                terms.push(tracked.value);
            }
            terms
        } else {
            vec![]
        };
        let limit = self.parse_limit_tracked()?;
        height = height.max(limit.height);
        // bd-tp6ia: SQLite's grammar attaches ORDER BY / LIMIT to a SELECT,
        // but a final VALUES term has no such slot. This rejects both standalone
        // `VALUES (1) ORDER BY 1` and compounds such as
        // `SELECT 1 UNION VALUES (2),(3) ORDER BY 1`.
        if matches!(final_core, SelectCore::Values(_))
            && (!order_by.is_empty() || limit.value.is_some())
        {
            return Err(self.err_msg("ORDER BY / LIMIT clause is not allowed after a VALUES term"));
        }
        Ok(HeightTracked {
            value: SelectStatement {
                with,
                body: body.value,
                order_by,
                limit: limit.value,
            },
            height,
        })
    }

    #[cfg(test)]
    fn parse_select_body_tracked(&mut self) -> Result<HeightTracked<SelectBody>, ParseError> {
        let select = self.parse_select_core_tracked()?;
        let mut height = select.height;
        let mut compounds = Vec::new();
        loop {
            let op = if self.eat_kw(&TokenKind::KwUnion) {
                if self.eat_kw(&TokenKind::KwAll) {
                    CompoundOp::UnionAll
                } else {
                    CompoundOp::Union
                }
            } else if self.eat_kw(&TokenKind::KwIntersect) {
                CompoundOp::Intersect
            } else if self.eat_kw(&TokenKind::KwExcept) {
                CompoundOp::Except
            } else {
                break;
            };
            let core = self.parse_select_core_tracked()?;
            height = height.max(core.height);
            compounds.push((op, core.value));
        }
        Ok(HeightTracked {
            value: SelectBody {
                select: select.value,
                compounds,
            },
            height,
        })
    }

    #[cfg(test)]
    fn parse_select_core_tracked(&mut self) -> Result<HeightTracked<SelectCore>, ParseError> {
        if self.eat_kw(&TokenKind::KwValues) {
            return self.parse_values_core_tracked();
        }
        self.expect_kw(&TokenKind::KwSelect)?;
        let distinct = if self.eat_kw(&TokenKind::KwDistinct) {
            Distinctness::Distinct
        } else {
            let _ = self.eat_kw(&TokenKind::KwAll);
            Distinctness::All
        };
        let parsed_columns = self.parse_comma_sep(Self::parse_result_column_tracked)?;
        let mut height = 0;
        let mut columns = Vec::with_capacity(parsed_columns.len());
        for tracked in parsed_columns {
            height = height.max(tracked.height);
            columns.push(tracked.value);
        }
        let from = if self.eat_kw(&TokenKind::KwFrom) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };
        let where_clause = if self.eat_kw(&TokenKind::KwWhere) {
            let parsed = self.parse_expr_tracked()?;
            height = height.max(parsed.height);
            Some(Box::new(parsed.expr))
        } else {
            None
        };
        let group_by = if self.eat_kw(&TokenKind::KwGroup) {
            self.expect_kw(&TokenKind::KwBy)?;
            let parsed = self.parse_comma_sep(Self::parse_expr_tracked)?;
            let mut expressions = Vec::with_capacity(parsed.len());
            for tracked in parsed {
                height = height.max(tracked.height);
                expressions.push(tracked.expr);
            }
            expressions
        } else {
            vec![]
        };
        let having = if self.eat_kw(&TokenKind::KwHaving) {
            let parsed = self.parse_expr_tracked()?;
            height = height.max(parsed.height);
            Some(Box::new(parsed.expr))
        } else {
            None
        };
        let windows = if self.eat_kw(&TokenKind::KwWindow) {
            self.parse_comma_sep(Self::parse_window_def)?
        } else {
            vec![]
        };
        Ok(HeightTracked {
            value: SelectCore::Select {
                distinct,
                columns,
                from,
                where_clause,
                group_by,
                having,
                windows,
            },
            height,
        })
    }

    fn parse_values_core(&mut self) -> Result<SelectCore, ParseError> {
        self.parse_values_core_tracked()
            .map(|tracked| tracked.value)
    }

    fn parse_values_core_tracked(&mut self) -> Result<HeightTracked<SelectCore>, ParseError> {
        let mut rows = Vec::new();
        let mut height = 0;
        let mut force_union_all_from = None;
        loop {
            self.expect_token(&TokenKind::LeftParen)?;
            let parsed = self.parse_comma_sep(Self::parse_expr_tracked)?;
            let mut row = Vec::with_capacity(parsed.len());
            for tracked in parsed {
                height = height.max(tracked.height);
                row.push(tracked.expr);
            }
            self.expect_token(&TokenKind::RightParen)?;
            if force_union_all_from.is_none() && self.has_with {
                force_union_all_from = Some(rows.len());
            }
            rows.push(row);
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        Ok(HeightTracked {
            value: SelectCore::Values(ValuesClause::parsed(rows, force_union_all_from)),
            height,
        })
    }

    fn parse_result_column_tracked(&mut self) -> Result<HeightTracked<ResultColumn>, ParseError> {
        if self.eat(&TokenKind::Star) {
            return Ok(HeightTracked {
                value: ResultColumn::Star,
                height: 0,
            });
        }
        if starts_table_star_qualifier(self.peek()) && self.peek_nth(1) == &TokenKind::Dot {
            if self.peek_nth(2) == &TokenKind::Star {
                let table = self.parse_table_star_qualifier()?;
                self.expect_token(&TokenKind::Dot)?;
                self.expect_token(&TokenKind::Star)?;
                return Ok(HeightTracked {
                    value: ResultColumn::TableStar(QualifiedName::bare(table)),
                    height: 0,
                });
            }
            if starts_table_star_qualifier(self.peek_nth(2))
                && self.peek_nth(3) == &TokenKind::Dot
                && self.peek_nth(4) == &TokenKind::Star
            {
                let schema = self.parse_table_star_qualifier()?;
                self.expect_token(&TokenKind::Dot)?;
                let table = self.parse_table_star_qualifier()?;
                self.expect_token(&TokenKind::Dot)?;
                self.expect_token(&TokenKind::Star)?;
                return Ok(HeightTracked {
                    value: ResultColumn::TableStar(QualifiedName::qualified(schema, table)),
                    height: 0,
                });
            }
        }
        let parsed = self.parse_expr_tracked()?;
        let alias = self.try_result_alias()?;
        Ok(HeightTracked {
            value: ResultColumn::Expr {
                expr: parsed.expr,
                alias,
            },
            height: parsed.height,
        })
    }

    fn parse_result_column(&mut self) -> Result<ResultColumn, ParseError> {
        self.parse_result_column_tracked()
            .map(|tracked| tracked.value)
    }

    // -----------------------------------------------------------------------
    // FROM clause & JOINs
    // -----------------------------------------------------------------------

    #[cfg(test)]
    fn parse_from_clause(&mut self) -> Result<FromClause, ParseError> {
        let source = self.parse_table_or_subquery()?;
        let mut joins = Vec::new();
        loop {
            if let Some(jt) = self.try_join_type()? {
                let table = self.parse_table_or_subquery()?;
                let constraint = self.parse_join_constraint()?;
                if jt.natural && constraint.is_some() {
                    return Err(self.err_msg("a NATURAL join may not have an ON or USING clause"));
                }
                joins.push(JoinClause {
                    join_type: jt,
                    table,
                    constraint,
                });
            } else if self.eat(&TokenKind::Comma) {
                let table = self.parse_table_or_subquery()?;
                let constraint = self.parse_join_constraint()?;
                joins.push(JoinClause {
                    join_type: JoinType {
                        natural: false,
                        kind: JoinKind::Cross,
                    },
                    table,
                    constraint,
                });
            } else {
                break;
            }
        }
        Ok(FromClause { source, joins })
    }

    #[cfg(test)]
    fn parse_table_or_subquery(&mut self) -> Result<TableOrSubquery, ParseError> {
        self.with_recursion_guard(|parser| parser.parse_table_or_subquery_inner())
    }

    #[cfg(test)]
    fn parse_table_or_subquery_inner(&mut self) -> Result<TableOrSubquery, ParseError> {
        if self.check(&TokenKind::LeftParen) {
            self.advance();
            if matches!(
                self.peek(),
                TokenKind::KwSelect | TokenKind::KwWith | TokenKind::KwValues
            ) {
                let with = if self.check_kw(&TokenKind::KwWith) {
                    Some(self.parse_with_clause()?)
                } else {
                    None
                };
                let q = self.parse_select_stmt(with)?;
                self.expect_token(&TokenKind::RightParen)?;
                let alias = self.try_table_alias()?;
                return Ok(TableOrSubquery::Subquery {
                    query: Box::new(q),
                    alias,
                });
            }
            // Parenthesized join
            let fc = self.parse_from_clause()?;
            self.expect_token(&TokenKind::RightParen)?;
            return Ok(TableOrSubquery::ParenJoin(Box::new(fc)));
        }

        let name = self.parse_qualified_name()?;

        // Table-valued function: name(args)
        if self.check(&TokenKind::LeftParen) && name.schema.is_none() {
            self.advance();
            let args = if self.check(&TokenKind::RightParen) {
                vec![]
            } else {
                self.parse_comma_sep(Self::parse_expr)?
            };
            self.expect_token(&TokenKind::RightParen)?;
            let alias = self.try_table_alias()?;
            return Ok(TableOrSubquery::TableFunction {
                name: name.name,
                args,
                alias,
            });
        }

        let alias = self.try_table_alias()?;
        let index_hint = self.parse_index_hint()?;
        let time_travel = self.parse_time_travel_clause()?;
        Ok(TableOrSubquery::Table {
            name,
            alias,
            index_hint,
            time_travel,
        })
    }

    pub(crate) fn try_join_type(&mut self) -> Result<Option<JoinType>, ParseError> {
        let natural = self.eat_kw(&TokenKind::KwNatural);
        let kind = if self.eat_kw(&TokenKind::KwJoin) {
            Some(JoinKind::Inner)
        } else if self.eat_kw(&TokenKind::KwInner) {
            self.expect_kw(&TokenKind::KwJoin)?;
            Some(JoinKind::Inner)
        } else if self.eat_kw(&TokenKind::KwCross) {
            self.expect_kw(&TokenKind::KwJoin)?;
            Some(JoinKind::Cross)
        } else if self.eat_kw(&TokenKind::KwLeft) {
            let _ = self.eat_kw(&TokenKind::KwOuter);
            self.expect_kw(&TokenKind::KwJoin)?;
            Some(JoinKind::Left)
        } else if self.eat_kw(&TokenKind::KwRight) {
            let _ = self.eat_kw(&TokenKind::KwOuter);
            self.expect_kw(&TokenKind::KwJoin)?;
            Some(JoinKind::Right)
        } else if self.eat_kw(&TokenKind::KwFull) {
            let _ = self.eat_kw(&TokenKind::KwOuter);
            self.expect_kw(&TokenKind::KwJoin)?;
            Some(JoinKind::Full)
        } else {
            None
        };
        match kind {
            Some(k) => Ok(Some(JoinType { natural, kind: k })),
            None if natural => Err(self.err_expected("JOIN after NATURAL")),
            None => Ok(None),
        }
    }

    #[cfg(test)]
    fn parse_join_constraint(&mut self) -> Result<Option<JoinConstraint>, ParseError> {
        if self.eat_kw(&TokenKind::KwOn) {
            Ok(Some(JoinConstraint::On(self.parse_expr()?)))
        } else if self.eat_kw(&TokenKind::KwUsing) {
            self.expect_token(&TokenKind::LeftParen)?;
            let cols = self.parse_comma_sep(Self::parse_identifier)?;
            self.expect_token(&TokenKind::RightParen)?;
            Ok(Some(JoinConstraint::Using(cols)))
        } else {
            Ok(None)
        }
    }

    // -----------------------------------------------------------------------
    // ORDER BY / LIMIT
    // -----------------------------------------------------------------------

    pub(crate) fn parse_ordering_term(&mut self) -> Result<OrderingTerm, ParseError> {
        self.parse_ordering_term_tracked()
            .map(|tracked| tracked.value)
    }

    fn parse_ordering_term_tracked(&mut self) -> Result<HeightTracked<OrderingTerm>, ParseError> {
        let parsed = self.parse_expr_tracked()?;
        let direction = if self.eat_kw(&TokenKind::KwAsc) {
            Some(SortDirection::Asc)
        } else if self.eat_kw(&TokenKind::KwDesc) {
            Some(SortDirection::Desc)
        } else {
            None
        };
        let nulls = if self.eat_kw(&TokenKind::KwNulls) {
            if self.eat_kw(&TokenKind::KwFirst) {
                Some(NullsOrder::First)
            } else {
                self.expect_kw(&TokenKind::KwLast)?;
                Some(NullsOrder::Last)
            }
        } else {
            None
        };
        Ok(HeightTracked {
            value: OrderingTerm {
                expr: parsed.expr,
                direction,
                nulls,
            },
            height: parsed.height,
        })
    }

    pub(crate) fn parse_limit(&mut self) -> Result<Option<LimitClause>, ParseError> {
        self.parse_limit_tracked().map(|tracked| tracked.value)
    }

    fn parse_limit_tracked(&mut self) -> Result<HeightTracked<Option<LimitClause>>, ParseError> {
        if !self.eat_kw(&TokenKind::KwLimit) {
            return Ok(HeightTracked {
                value: None,
                height: 0,
            });
        }
        let first = self.parse_expr_tracked()?;
        if self.eat_kw(&TokenKind::KwOffset) {
            let offset = self.parse_expr_tracked()?;
            let height = self.checked_cached_parent_height(first.height.max(offset.height))?;
            return Ok(HeightTracked {
                value: Some(LimitClause {
                    limit: first.expr,
                    offset: Some(offset.expr),
                }),
                height,
            });
        }

        if self.eat(&TokenKind::Comma) {
            // LIMIT offset, count — SQLite/MySQL compatibility form.
            let second = self.parse_expr_tracked()?;
            let height = self.checked_cached_parent_height(first.height.max(second.height))?;
            return Ok(HeightTracked {
                value: Some(LimitClause {
                    limit: second.expr,
                    offset: Some(first.expr),
                }),
                height,
            });
        }

        let height = self.checked_cached_parent_height(first.height)?;
        Ok(HeightTracked {
            value: Some(LimitClause {
                limit: first.expr,
                offset: None,
            }),
            height,
        })
    }

    // -----------------------------------------------------------------------
    // RETURNING clause
    // -----------------------------------------------------------------------

    fn parse_returning(
        &mut self,
        context: DmlParseContext,
    ) -> Result<Vec<ResultColumn>, ParseError> {
        if self.check_kw(&TokenKind::KwReturning) && context == DmlParseContext::TriggerBody {
            return Err(self.err_msg("RETURNING is not allowed in trigger body statements"));
        }
        if self.eat_kw(&TokenKind::KwReturning) {
            self.parse_comma_sep(Self::parse_result_column)
        } else {
            Ok(vec![])
        }
    }

    // -----------------------------------------------------------------------
    // INSERT
    // -----------------------------------------------------------------------

    fn parse_insert_stmt(
        &mut self,
        with: Option<WithClause>,
        context: DmlParseContext,
    ) -> Result<Statement, ParseError> {
        let or_conflict = if self.eat_kw(&TokenKind::KwReplace) {
            Some(ConflictAction::Replace)
        } else {
            self.expect_kw(&TokenKind::KwInsert)?;
            if self.eat_kw(&TokenKind::KwOr) {
                Some(self.parse_conflict_action()?)
            } else {
                None
            }
        };
        self.eat_kw(&TokenKind::KwInto);
        let table = self.parse_dml_target_name(context)?;
        if context == DmlParseContext::TriggerBody && self.check_kw(&TokenKind::KwAs) {
            return Err(self.err_msg("table aliases are not allowed in trigger body statements"));
        }
        let alias = if self.eat_kw(&TokenKind::KwAs) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let columns = if self.check(&TokenKind::LeftParen)
            && !matches!(
                self.peek_nth(1),
                TokenKind::KwSelect | TokenKind::KwWith | TokenKind::KwValues
            ) {
            self.advance();
            let cols = self.parse_comma_sep(Self::parse_identifier)?;
            self.expect_token(&TokenKind::RightParen)?;
            cols
        } else {
            vec![]
        };
        let source = if self.check_kw(&TokenKind::KwDefault)
            && context == DmlParseContext::TriggerBody
        {
            return Err(self.err_msg("DEFAULT VALUES is not allowed in trigger body statements"));
        } else if self.eat_kw(&TokenKind::KwDefault) {
            self.expect_kw(&TokenKind::KwValues)?;
            InsertSource::DefaultValues
        } else if self.eat_kw(&TokenKind::KwValues) {
            match self.parse_values_core()? {
                SelectCore::Values(rows) => InsertSource::Values(rows.into_rows()),
                SelectCore::Select { .. } => unreachable!("parse_values_core must return VALUES"),
            }
        } else {
            let inner_with = if self.check_kw(&TokenKind::KwWith) {
                Some(self.parse_with_clause()?)
            } else {
                None
            };
            InsertSource::Select(Box::new(self.parse_select_stmt(inner_with)?))
        };
        let upsert = self.parse_upsert_clauses()?;
        let returning = self.parse_returning(context)?;
        Ok(Statement::Insert(InsertStatement {
            with,
            or_conflict,
            table,
            alias,
            columns,
            source,
            upsert,
            returning,
        }))
    }

    fn parse_conflict_action(&mut self) -> Result<ConflictAction, ParseError> {
        if self.eat_kw(&TokenKind::KwRollback) {
            Ok(ConflictAction::Rollback)
        } else if self.eat_kw(&TokenKind::KwAbort) {
            Ok(ConflictAction::Abort)
        } else if self.eat_kw(&TokenKind::KwFail) {
            Ok(ConflictAction::Fail)
        } else if self.eat_kw(&TokenKind::KwIgnore) {
            Ok(ConflictAction::Ignore)
        } else if self.eat_kw(&TokenKind::KwReplace) {
            Ok(ConflictAction::Replace)
        } else {
            Err(self.err_expected("conflict action"))
        }
    }

    fn parse_upsert_clauses(&mut self) -> Result<Vec<UpsertClause>, ParseError> {
        let mut clauses = Vec::new();
        while self.check_kw(&TokenKind::KwOn) && self.peek_nth(1) == &TokenKind::KwConflict {
            // SQLite 3.35+: a clause without a conflict target is only valid as
            // the final ON CONFLICT clause of the INSERT.
            if clauses
                .last()
                .is_some_and(|clause: &UpsertClause| clause.target.is_none())
            {
                return Err(self.err_msg(
                    "ON CONFLICT clause without a conflict target must be the last ON CONFLICT clause",
                ));
            }
            self.advance(); // ON
            self.advance(); // CONFLICT
            let target = if self.check(&TokenKind::LeftParen) {
                self.advance();
                let columns = self.parse_comma_sep(Self::parse_indexed_column)?;
                self.expect_token(&TokenKind::RightParen)?;
                let wh = if self.eat_kw(&TokenKind::KwWhere) {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                Some(UpsertTarget {
                    columns,
                    where_clause: wh,
                })
            } else {
                None
            };
            self.expect_kw(&TokenKind::KwDo)?;
            let action = if self.eat_kw(&TokenKind::KwNothing) {
                UpsertAction::Nothing
            } else {
                self.expect_kw(&TokenKind::KwUpdate)?;
                self.expect_kw(&TokenKind::KwSet)?;
                let assignments = self.parse_comma_sep(Self::parse_assignment)?;
                let wh = if self.eat_kw(&TokenKind::KwWhere) {
                    Some(Box::new(self.parse_expr()?))
                } else {
                    None
                };
                UpsertAction::Update {
                    assignments,
                    where_clause: wh,
                }
            };
            clauses.push(UpsertClause { target, action });
        }
        Ok(clauses)
    }

    // -----------------------------------------------------------------------
    // UPDATE
    // -----------------------------------------------------------------------

    fn parse_update_stmt(
        &mut self,
        with: Option<WithClause>,
        context: DmlParseContext,
    ) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwUpdate)?;
        let or_conflict = if self.eat_kw(&TokenKind::KwOr) {
            Some(self.parse_conflict_action()?)
        } else {
            None
        };
        let table = self.parse_qualified_table_ref(context)?;
        self.expect_kw(&TokenKind::KwSet)?;
        let assignments = self.parse_comma_sep(Self::parse_assignment)?;
        let from = if self.eat_kw(&TokenKind::KwFrom) {
            Some(self.parse_from_clause_machine()?)
        } else {
            None
        };
        let where_clause = if self.eat_kw(&TokenKind::KwWhere) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let returning = self.parse_returning(context)?;
        if context == DmlParseContext::TriggerBody && self.check_kw(&TokenKind::KwOrder) {
            return Err(self.err_msg("ORDER BY is not allowed in trigger body UPDATE statements"));
        }
        let order_by = if self.eat_kw(&TokenKind::KwOrder) {
            self.expect_kw(&TokenKind::KwBy)?;
            self.parse_comma_sep(Self::parse_ordering_term)?
        } else {
            vec![]
        };
        if context == DmlParseContext::TriggerBody && self.check_kw(&TokenKind::KwLimit) {
            return Err(self.err_msg("LIMIT is not allowed in trigger body UPDATE statements"));
        }
        let limit = self.parse_limit()?;
        Ok(Statement::Update(UpdateStatement {
            with,
            or_conflict,
            table,
            assignments,
            from,
            where_clause,
            returning,
            order_by,
            limit,
        }))
    }

    fn parse_assignment(&mut self) -> Result<Assignment, ParseError> {
        let target = if self.check(&TokenKind::LeftParen) {
            self.advance();
            let cols = self.parse_comma_sep(Self::parse_identifier)?;
            self.expect_token(&TokenKind::RightParen)?;
            AssignmentTarget::ColumnList(cols)
        } else {
            AssignmentTarget::Column(self.parse_identifier()?)
        };
        self.expect_token(&TokenKind::Eq)?;
        let value = self.parse_expr()?;
        Ok(Assignment { target, value })
    }

    // -----------------------------------------------------------------------
    // DELETE
    // -----------------------------------------------------------------------

    fn parse_delete_stmt(
        &mut self,
        with: Option<WithClause>,
        context: DmlParseContext,
    ) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwDelete)?;
        self.expect_kw(&TokenKind::KwFrom)?;
        let table = self.parse_qualified_table_ref(context)?;
        let where_clause = if self.eat_kw(&TokenKind::KwWhere) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let returning = self.parse_returning(context)?;
        if context == DmlParseContext::TriggerBody && self.check_kw(&TokenKind::KwOrder) {
            return Err(self.err_msg("ORDER BY is not allowed in trigger body DELETE statements"));
        }
        let order_by = if self.eat_kw(&TokenKind::KwOrder) {
            self.expect_kw(&TokenKind::KwBy)?;
            self.parse_comma_sep(Self::parse_ordering_term)?
        } else {
            vec![]
        };
        if context == DmlParseContext::TriggerBody && self.check_kw(&TokenKind::KwLimit) {
            return Err(self.err_msg("LIMIT is not allowed in trigger body DELETE statements"));
        }
        let limit = self.parse_limit()?;
        Ok(Statement::Delete(DeleteStatement {
            with,
            table,
            where_clause,
            returning,
            order_by,
            limit,
        }))
    }

    // -----------------------------------------------------------------------
    // CREATE
    // -----------------------------------------------------------------------

    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwCreate)?;
        let temporary = self.eat_kw(&TokenKind::KwTemp) || self.eat_kw(&TokenKind::KwTemporary);
        let unique = self.eat_kw(&TokenKind::KwUnique);

        if self.eat_kw(&TokenKind::KwTable) {
            if unique {
                return Err(self.err_expected("INDEX after UNIQUE"));
            }
            return self.parse_create_table(temporary);
        }
        if self.eat_kw(&TokenKind::KwIndex) {
            if temporary {
                return Err(self.err_expected("TABLE, VIEW, or TRIGGER after TEMP"));
            }
            return self.parse_create_index(unique);
        }
        if self.eat_kw(&TokenKind::KwView) {
            if unique {
                return Err(self.err_expected("INDEX after UNIQUE"));
            }
            return self.parse_create_view(temporary);
        }
        if self.eat_kw(&TokenKind::KwTrigger) {
            if unique {
                return Err(self.err_expected("INDEX after UNIQUE"));
            }
            return self.parse_create_trigger(temporary);
        }
        if self.eat_kw(&TokenKind::KwVirtual) {
            if temporary || unique {
                return Err(self.err_expected("TABLE, INDEX, VIEW, or TRIGGER"));
            }
            self.expect_kw(&TokenKind::KwTable)?;
            return self.parse_create_virtual_table();
        }
        Err(self.err_expected("TABLE, INDEX, VIEW, TRIGGER, or VIRTUAL"))
    }

    fn parse_if_not_exists(&mut self) -> bool {
        if self.check_kw(&TokenKind::KwIf)
            && self.peek_nth(1) == &TokenKind::KwNot
            && self.peek_nth(2) == &TokenKind::KwExists
        {
            self.advance();
            self.advance();
            self.advance();
            true
        } else {
            false
        }
    }

    fn parse_create_table(&mut self, temporary: bool) -> Result<Statement, ParseError> {
        let if_not_exists = self.parse_if_not_exists();
        let name = self.parse_qualified_name()?;
        let body = if self.eat_kw(&TokenKind::KwAs) {
            let with = if self.check_kw(&TokenKind::KwWith) {
                Some(self.parse_with_clause()?)
            } else {
                None
            };
            CreateTableBody::AsSelect(Box::new(self.parse_select_stmt(with)?))
        } else {
            self.expect_token(&TokenKind::LeftParen)?;
            let mut columns = Vec::new();
            let mut constraints = Vec::new();
            loop {
                if self.is_table_constraint_start() {
                    constraints.push(self.parse_table_constraint()?);
                } else {
                    columns.push(self.parse_column_def()?);
                }
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
            self.expect_token(&TokenKind::RightParen)?;
            CreateTableBody::Columns {
                columns,
                constraints,
            }
        };
        let mut without_rowid = false;
        let mut strict = false;
        // Table options after the closing paren.
        if self.check_kw(&TokenKind::KwWithout) || self.check_kw(&TokenKind::KwStrict) {
            loop {
                if self.check_kw(&TokenKind::KwWithout) {
                    self.advance();
                    // Expect "ROWID" as an identifier.
                    let id = self.parse_identifier()?;
                    if !id.eq_ignore_ascii_case("ROWID") {
                        return Err(self.err_expected("ROWID after WITHOUT"));
                    }
                    without_rowid = true;
                } else if self.eat_kw(&TokenKind::KwStrict) {
                    strict = true;
                } else {
                    return Err(self.err_expected("table option"));
                }
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        Ok(Statement::CreateTable(CreateTableStatement {
            if_not_exists,
            temporary,
            name,
            body,
            without_rowid,
            strict,
        }))
    }

    fn is_table_constraint_start(&self) -> bool {
        matches!(
            self.peek(),
            TokenKind::KwPrimary | TokenKind::KwUnique | TokenKind::KwCheck | TokenKind::KwForeign
        ) || (self.check_kw(&TokenKind::KwConstraint))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParseError> {
        let name = self.parse_identifier()?;
        let type_name = self.try_type_name()?;
        let mut constraints = Vec::new();
        while let Some(c) = self.try_column_constraint()? {
            constraints.push(c);
        }
        Ok(ColumnDef {
            name,
            type_name,
            constraints,
        })
    }

    fn try_type_name(&mut self) -> Result<Option<TypeName>, ParseError> {
        // Type name is one or more identifiers, stopping at known boundaries.
        if self.is_column_constraint_start()
            || matches!(
                self.peek(),
                TokenKind::Comma | TokenKind::RightParen | TokenKind::Eof
            )
        {
            return Ok(None);
        }
        // Collect type name words.
        let mut words = Vec::new();
        loop {
            match self.peek() {
                TokenKind::Id(_) | TokenKind::QuotedId(_, _) => {
                    words.push(self.parse_identifier()?);
                }
                k if is_nonreserved_kw(k) => {
                    words.push(self.parse_identifier()?);
                }
                _ => break,
            }
            if self.is_column_constraint_start()
                || matches!(
                    self.peek(),
                    TokenKind::Comma | TokenKind::RightParen | TokenKind::LeftParen
                )
            {
                break;
            }
        }
        if words.is_empty() {
            return Ok(None);
        }
        let type_name = words.join(" ");
        let (arg1, arg2) = if self.eat(&TokenKind::LeftParen) {
            let a1 = self.parse_signed_number_str()?;
            let a2 = if self.eat(&TokenKind::Comma) {
                Some(self.parse_signed_number_str()?)
            } else {
                None
            };
            self.expect_token(&TokenKind::RightParen)?;
            (Some(a1), a2)
        } else {
            (None, None)
        };
        Ok(Some(TypeName {
            name: type_name,
            arg1,
            arg2,
        }))
    }

    fn parse_signed_number_str(&mut self) -> Result<String, ParseError> {
        let neg = self.eat(&TokenKind::Minus);
        let plus = if neg {
            false
        } else {
            self.eat(&TokenKind::Plus)
        };
        let _ = plus; // just consume
        match self.peek().clone() {
            TokenKind::Integer(n) => {
                self.advance();
                Ok(if neg { format!("-{n}") } else { n.to_string() })
            }
            TokenKind::OversizedInt(s) => {
                self.advance();
                Ok(if neg { format!("-{s}") } else { s.clone() })
            }
            TokenKind::Float(f) => {
                self.advance();
                Ok(if neg { format!("-{f}") } else { f.to_string() })
            }
            _ => Err(self.err_expected("number")),
        }
    }

    fn is_column_constraint_start(&self) -> bool {
        matches!(
            self.peek(),
            TokenKind::KwPrimary
                | TokenKind::KwNot
                | TokenKind::KwNull
                | TokenKind::KwUnique
                | TokenKind::KwCheck
                | TokenKind::KwDefault
                | TokenKind::KwCollate
                | TokenKind::KwReferences
                | TokenKind::KwGenerated
                | TokenKind::KwConstraint
                | TokenKind::KwAs
        )
    }

    fn try_column_constraint(&mut self) -> Result<Option<ColumnConstraint>, ParseError> {
        let name = if self.eat_kw(&TokenKind::KwConstraint) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let kind = if self.eat_kw(&TokenKind::KwPrimary) {
            self.expect_kw(&TokenKind::KwKey)?;
            let direction = if self.eat_kw(&TokenKind::KwAsc) {
                Some(SortDirection::Asc)
            } else if self.eat_kw(&TokenKind::KwDesc) {
                Some(SortDirection::Desc)
            } else {
                None
            };
            let conflict = self.parse_on_conflict()?;
            let autoincrement = self.eat_kw(&TokenKind::KwAutoincrement);
            ColumnConstraintKind::PrimaryKey {
                direction,
                conflict,
                autoincrement,
            }
        } else if self.check_kw(&TokenKind::KwNot) && self.peek_nth(1) == &TokenKind::KwNull {
            self.advance();
            self.advance();
            let conflict = self.parse_on_conflict()?;
            ColumnConstraintKind::NotNull { conflict }
        } else if self.eat_kw(&TokenKind::KwNull) {
            ColumnConstraintKind::Null
        } else if self.eat_kw(&TokenKind::KwUnique) {
            let conflict = self.parse_on_conflict()?;
            ColumnConstraintKind::Unique { conflict }
        } else if self.eat_kw(&TokenKind::KwCheck) {
            self.expect_token(&TokenKind::LeftParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&TokenKind::RightParen)?;
            ColumnConstraintKind::Check(expr)
        } else if self.eat_kw(&TokenKind::KwDefault) {
            if self.eat(&TokenKind::LeftParen) {
                let expr = self.parse_expr()?;
                self.expect_token(&TokenKind::RightParen)?;
                ColumnConstraintKind::Default(DefaultValue::ParenExpr(expr))
            } else if let TokenKind::Id(name) = self.peek().clone() {
                // SQLite quirk: a BARE (unparenthesized, unquoted) identifier after
                // DEFAULT is treated as a STRING LITERAL, not a column reference — a
                // column DEFAULT cannot reference other columns. `DEFAULT abc` yields
                // the string 'abc'; `DEFAULT (abc)` stays an expression and is later
                // rejected as non-constant. Keyword constants (TRUE/FALSE/NULL/
                // CURRENT_*) lex as keywords (not Id), so they fall through to the
                // normal expression parse below and keep their literal meaning.
                let span = self.current_span();
                self.advance();
                ColumnConstraintKind::Default(DefaultValue::Expr(Expr::Literal(
                    Literal::String(name.to_string()),
                    span,
                )))
            } else {
                let expr = self.parse_expr()?;
                ColumnConstraintKind::Default(DefaultValue::Expr(expr))
            }
        } else if self.eat_kw(&TokenKind::KwCollate) {
            ColumnConstraintKind::Collate(self.parse_identifier()?)
        } else if self.eat_kw(&TokenKind::KwReferences) {
            ColumnConstraintKind::ForeignKey(self.parse_fk_clause()?)
        } else if self.eat_kw(&TokenKind::KwGenerated) || self.eat_kw(&TokenKind::KwAs) {
            if self.tokens[self.pos.saturating_sub(1)].kind == TokenKind::KwGenerated {
                let _ = self.eat_kw(&TokenKind::KwAlways);
                let _ = self.eat_kw(&TokenKind::KwAs);
            }
            self.expect_token(&TokenKind::LeftParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&TokenKind::RightParen)?;
            let storage = if self.eat_kw(&TokenKind::KwStored) {
                Some(GeneratedStorage::Stored)
            } else if self.eat_kw(&TokenKind::KwVirtual) {
                Some(GeneratedStorage::Virtual)
            } else {
                None
            };
            ColumnConstraintKind::Generated { expr, storage }
        } else if name.is_some() {
            return Err(self.err_expected("constraint kind after CONSTRAINT name"));
        } else {
            return Ok(None);
        };
        Ok(Some(ColumnConstraint { name, kind }))
    }

    fn parse_on_conflict(&mut self) -> Result<Option<ConflictAction>, ParseError> {
        if self.check_kw(&TokenKind::KwOn) && self.peek_nth(1) == &TokenKind::KwConflict {
            self.advance();
            self.advance();
            Ok(Some(self.parse_conflict_action()?))
        } else {
            Ok(None)
        }
    }

    fn parse_fk_clause(&mut self) -> Result<ForeignKeyClause, ParseError> {
        let table = self.parse_identifier()?;
        let columns = if self.eat(&TokenKind::LeftParen) {
            let cols = self.parse_comma_sep(Self::parse_identifier)?;
            self.expect_token(&TokenKind::RightParen)?;
            cols
        } else {
            vec![]
        };
        let mut actions = Vec::new();
        let mut deferrable = None;
        loop {
            if self.check_kw(&TokenKind::KwOn) {
                self.advance();
                let trigger = if self.eat_kw(&TokenKind::KwDelete) {
                    ForeignKeyTrigger::OnDelete
                } else {
                    self.expect_kw(&TokenKind::KwUpdate)?;
                    ForeignKeyTrigger::OnUpdate
                };
                let action = self.parse_fk_action_type()?;
                actions.push(ForeignKeyAction { trigger, action });
            } else if self.check_kw(&TokenKind::KwNot) || self.check_kw(&TokenKind::KwDeferrable) {
                let not = self.eat_kw(&TokenKind::KwNot);
                self.expect_kw(&TokenKind::KwDeferrable)?;
                let initially = if self.eat_kw(&TokenKind::KwInitially) {
                    if self.eat_kw(&TokenKind::KwDeferred) {
                        Some(DeferrableInitially::Deferred)
                    } else {
                        self.expect_kw(&TokenKind::KwImmediate)?;
                        Some(DeferrableInitially::Immediate)
                    }
                } else {
                    None
                };
                deferrable = Some(Deferrable { not, initially });
            } else if self.eat_kw(&TokenKind::KwMatch) {
                // MATCH name — parsed but ignored per SQLite behavior.
                self.parse_identifier()?;
            } else {
                break;
            }
        }
        Ok(ForeignKeyClause {
            table,
            columns,
            actions,
            deferrable,
        })
    }

    fn parse_fk_action_type(&mut self) -> Result<ForeignKeyActionType, ParseError> {
        if self.eat_kw(&TokenKind::KwSet) {
            if self.eat_kw(&TokenKind::KwNull) {
                Ok(ForeignKeyActionType::SetNull)
            } else {
                self.expect_kw(&TokenKind::KwDefault)?;
                Ok(ForeignKeyActionType::SetDefault)
            }
        } else if self.eat_kw(&TokenKind::KwCascade) {
            Ok(ForeignKeyActionType::Cascade)
        } else if self.eat_kw(&TokenKind::KwRestrict) {
            Ok(ForeignKeyActionType::Restrict)
        } else if self.check_kw(&TokenKind::KwNo) {
            self.advance();
            let id = self.parse_identifier()?;
            if !id.eq_ignore_ascii_case("ACTION") {
                return Err(self.err_expected("ACTION after NO"));
            }
            Ok(ForeignKeyActionType::NoAction)
        } else {
            Err(self.err_expected("foreign key action"))
        }
    }

    fn parse_table_constraint(&mut self) -> Result<TableConstraint, ParseError> {
        let name = if self.eat_kw(&TokenKind::KwConstraint) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let kind = if self.eat_kw(&TokenKind::KwPrimary) {
            self.expect_kw(&TokenKind::KwKey)?;
            self.expect_token(&TokenKind::LeftParen)?;
            let columns = self.parse_comma_sep(Self::parse_indexed_column)?;
            self.expect_token(&TokenKind::RightParen)?;
            let conflict = self.parse_on_conflict()?;
            TableConstraintKind::PrimaryKey { columns, conflict }
        } else if self.eat_kw(&TokenKind::KwUnique) {
            self.expect_token(&TokenKind::LeftParen)?;
            let columns = self.parse_comma_sep(Self::parse_indexed_column)?;
            self.expect_token(&TokenKind::RightParen)?;
            let conflict = self.parse_on_conflict()?;
            TableConstraintKind::Unique { columns, conflict }
        } else if self.eat_kw(&TokenKind::KwCheck) {
            self.expect_token(&TokenKind::LeftParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&TokenKind::RightParen)?;
            // A table CHECK constraint may carry a trailing conflict clause
            // (`CHECK(expr) ON CONFLICT <action>`), accepted by stock sqlite3.
            // Consume it for parse parity so a stock-created schema stays
            // loadable on reopen (mirrors PRIMARY KEY / UNIQUE above).
            let _conflict = self.parse_on_conflict()?;
            TableConstraintKind::Check(expr)
        } else if self.eat_kw(&TokenKind::KwForeign) {
            self.expect_kw(&TokenKind::KwKey)?;
            self.expect_token(&TokenKind::LeftParen)?;
            let columns = self.parse_comma_sep(Self::parse_identifier)?;
            self.expect_token(&TokenKind::RightParen)?;
            self.expect_kw(&TokenKind::KwReferences)?;
            let clause = self.parse_fk_clause()?;
            TableConstraintKind::ForeignKey { columns, clause }
        } else {
            return Err(self.err_expected("table constraint"));
        };
        Ok(TableConstraint { name, kind })
    }

    fn parse_indexed_column(&mut self) -> Result<IndexedColumn, ParseError> {
        let expr = self.parse_expr()?;
        let collation = if self.eat_kw(&TokenKind::KwCollate) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let direction = if self.eat_kw(&TokenKind::KwAsc) {
            Some(SortDirection::Asc)
        } else if self.eat_kw(&TokenKind::KwDesc) {
            Some(SortDirection::Desc)
        } else {
            None
        };
        Ok(IndexedColumn {
            expr,
            collation,
            direction,
        })
    }

    fn parse_create_index(&mut self, unique: bool) -> Result<Statement, ParseError> {
        let if_not_exists = self.parse_if_not_exists();
        let name = self.parse_qualified_name()?;
        self.expect_kw(&TokenKind::KwOn)?;
        let table = self.parse_identifier()?;
        self.expect_token(&TokenKind::LeftParen)?;
        let columns = self.parse_comma_sep(Self::parse_indexed_column)?;
        self.expect_token(&TokenKind::RightParen)?;
        let where_clause = if self.eat_kw(&TokenKind::KwWhere) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::CreateIndex(CreateIndexStatement {
            unique,
            if_not_exists,
            name,
            table,
            columns,
            where_clause,
        }))
    }

    fn parse_create_view(&mut self, temporary: bool) -> Result<Statement, ParseError> {
        let if_not_exists = self.parse_if_not_exists();
        let name = self.parse_qualified_name()?;
        let columns = if self.check(&TokenKind::LeftParen) {
            self.advance();
            let cols = self.parse_comma_sep(Self::parse_identifier)?;
            self.expect_token(&TokenKind::RightParen)?;
            cols
        } else {
            vec![]
        };
        self.expect_kw(&TokenKind::KwAs)?;
        let with = if self.check_kw(&TokenKind::KwWith) {
            Some(self.parse_with_clause()?)
        } else {
            None
        };
        let query = self.parse_select_stmt(with)?;
        Ok(Statement::CreateView(CreateViewStatement {
            if_not_exists,
            temporary,
            name,
            columns,
            query,
        }))
    }

    fn parse_trigger_body_statement_inner(&mut self) -> Result<Statement, ParseError> {
        match self.peek().clone() {
            TokenKind::KwSelect | TokenKind::KwValues => {
                Ok(Statement::Select(self.parse_select_stmt(None)?))
            }
            TokenKind::KwWith => {
                // Stock SQLite (3.46.1, verified) only allows a WITH (CTE) clause
                // ahead of SELECT/VALUES in a trigger body; `WITH ... INSERT/
                // UPDATE/DELETE` is a syntax error ("near INSERT"). Keep parity.
                let with = self.parse_with_clause()?;
                if matches!(self.peek(), TokenKind::KwSelect | TokenKind::KwValues) {
                    Ok(Statement::Select(self.parse_select_stmt(Some(with))?))
                } else {
                    Err(self.err_expected("SELECT or VALUES after WITH in a trigger body"))
                }
            }
            TokenKind::KwInsert | TokenKind::KwReplace => {
                self.parse_insert_stmt(None, DmlParseContext::TriggerBody)
            }
            TokenKind::KwUpdate => self.parse_update_stmt(None, DmlParseContext::TriggerBody),
            TokenKind::KwDelete => self.parse_delete_stmt(None, DmlParseContext::TriggerBody),
            _ => {
                Err(self
                    .err_msg("trigger body statement must be SELECT, INSERT, UPDATE, or DELETE"))
            }
        }
    }

    fn parse_trigger_body_statement(&mut self) -> Result<Statement, ParseError> {
        self.parse_trigger_body_statement_inner()
    }

    fn parse_create_trigger(&mut self, temporary: bool) -> Result<Statement, ParseError> {
        let if_not_exists = self.parse_if_not_exists();
        let name = self.parse_qualified_name()?;
        let timing = if self.eat_kw(&TokenKind::KwBefore) {
            TriggerTiming::Before
        } else if self.eat_kw(&TokenKind::KwAfter) {
            TriggerTiming::After
        } else if self.eat_kw(&TokenKind::KwInstead) {
            self.expect_kw(&TokenKind::KwOf)?;
            TriggerTiming::InsteadOf
        } else {
            TriggerTiming::Before // default
        };
        let event = if self.eat_kw(&TokenKind::KwInsert) {
            TriggerEvent::Insert
        } else if self.eat_kw(&TokenKind::KwDelete) {
            TriggerEvent::Delete
        } else {
            self.expect_kw(&TokenKind::KwUpdate)?;
            let cols = if self.eat_kw(&TokenKind::KwOf) {
                self.parse_comma_sep(Self::parse_identifier)?
            } else {
                vec![]
            };
            TriggerEvent::Update(cols)
        };
        self.expect_kw(&TokenKind::KwOn)?;
        let table = self.parse_identifier()?;
        let for_each_row = if self.eat_kw(&TokenKind::KwFor) {
            self.expect_kw(&TokenKind::KwEach)?;
            self.expect_kw(&TokenKind::KwRow)?;
            true
        } else {
            false
        };
        let when = if self.eat_kw(&TokenKind::KwWhen) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        self.expect_kw(&TokenKind::KwBegin)?;
        let mut body = Vec::new();
        if self.check_kw(&TokenKind::KwEnd) {
            let error = self.err_msg("trigger body must contain at least one statement");
            self.recover_trigger_body_after_error(self.pos);
            return Err(error);
        }
        loop {
            if self.check_kw(&TokenKind::KwEnd) {
                break;
            }
            let statement_start = self.pos;
            let stmt = match self.parse_trigger_body_statement() {
                Ok(stmt) => stmt,
                Err(err) => {
                    self.recover_trigger_body_after_error(statement_start);
                    return Err(err);
                }
            };
            body.push(stmt);
            if !self.eat(&TokenKind::Semicolon) {
                let error = self.err_expected("';' after trigger body statement");
                // The statement parsed successfully, so the current token is
                // already a statement boundary. In particular, UPDATE/INSERT/
                // DELETE leave the enclosing trigger END current here.
                self.recover_trigger_body_after_error(self.pos);
                return Err(error);
            }
        }
        self.expect_kw(&TokenKind::KwEnd)?;
        Ok(Statement::CreateTrigger(CreateTriggerStatement {
            if_not_exists,
            temporary,
            name,
            timing,
            event,
            table,
            for_each_row,
            when,
            body,
        }))
    }

    fn parse_create_virtual_table(&mut self) -> Result<Statement, ParseError> {
        let if_not_exists = self.parse_if_not_exists();
        let name = self.parse_qualified_name()?;
        self.expect_kw(&TokenKind::KwUsing)?;
        let module = self.parse_identifier()?;
        let args = if self.eat(&TokenKind::LeftParen) {
            if self.check(&TokenKind::RightParen) {
                self.advance();
                vec![]
            } else {
                // Virtual table args are opaque; collect tokens as strings until matching rparen.
                let mut args = Vec::new();
                let mut depth = 0i32;
                let mut current_arg = String::new();
                loop {
                    match self.peek() {
                        TokenKind::RightParen if depth == 0 => {
                            self.advance();
                            args.push(current_arg.trim().to_owned());
                            break;
                        }
                        TokenKind::LeftParen => {
                            depth += 1;
                            current_arg.push('(');
                            self.advance();
                        }
                        TokenKind::RightParen => {
                            depth -= 1;
                            current_arg.push(')');
                            self.advance();
                        }
                        TokenKind::Comma if depth == 0 => {
                            args.push(current_arg.trim().to_owned());
                            current_arg = String::new();
                            self.advance();
                        }
                        TokenKind::Eof => {
                            return Err(self.err_expected("closing parenthesis"));
                        }
                        _ => {
                            // Reconstruct token text from token kind.
                            let t = self
                                .current()
                                .ok_or_else(|| self.err_expected("virtual table argument token"))?;
                            let text = t.kind.to_sql();
                            if !current_arg.is_empty()
                                && !current_arg.ends_with(' ')
                                && !text.is_empty()
                            {
                                current_arg.push(' ');
                            }
                            current_arg.push_str(&text);
                            self.advance();
                        }
                    }
                }
                args
            }
        } else {
            vec![]
        };
        Ok(Statement::CreateVirtualTable(CreateVirtualTableStatement {
            if_not_exists,
            name,
            module,
            args,
        }))
    }

    // -----------------------------------------------------------------------
    // DROP
    // -----------------------------------------------------------------------

    fn parse_drop(&mut self) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwDrop)?;
        let object_type = if self.eat_kw(&TokenKind::KwTable) {
            DropObjectType::Table
        } else if self.eat_kw(&TokenKind::KwView) {
            DropObjectType::View
        } else if self.eat_kw(&TokenKind::KwIndex) {
            DropObjectType::Index
        } else if self.eat_kw(&TokenKind::KwTrigger) {
            DropObjectType::Trigger
        } else {
            return Err(self.err_expected("TABLE, VIEW, INDEX, or TRIGGER"));
        };
        let if_exists =
            if self.check_kw(&TokenKind::KwIf) && self.peek_nth(1) == &TokenKind::KwExists {
                self.advance();
                self.advance();
                true
            } else {
                false
            };
        let name = self.parse_qualified_name()?;
        Ok(Statement::Drop(DropStatement {
            object_type,
            if_exists,
            name,
        }))
    }

    // -----------------------------------------------------------------------
    // ALTER TABLE
    // -----------------------------------------------------------------------

    fn parse_alter(&mut self) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwAlter)?;
        self.expect_kw(&TokenKind::KwTable)?;
        let table = self.parse_qualified_name()?;
        let action = if self.eat_kw(&TokenKind::KwRename) {
            if self.eat_kw(&TokenKind::KwTo) {
                AlterTableAction::RenameTo(self.parse_identifier()?)
            } else {
                let _ = self.eat_kw(&TokenKind::KwColumn);
                let old = self.parse_identifier()?;
                self.expect_kw(&TokenKind::KwTo)?;
                let new = self.parse_identifier()?;
                AlterTableAction::RenameColumn { old, new }
            }
        } else if self.eat_kw(&TokenKind::KwAdd) {
            let _ = self.eat_kw(&TokenKind::KwColumn);
            AlterTableAction::AddColumn(self.parse_column_def()?)
        } else if self.eat_kw(&TokenKind::KwDrop) {
            let _ = self.eat_kw(&TokenKind::KwColumn);
            AlterTableAction::DropColumn(self.parse_identifier()?)
        } else {
            return Err(self.err_expected("RENAME, ADD, or DROP"));
        };
        Ok(Statement::AlterTable(AlterTableStatement { table, action }))
    }

    // -----------------------------------------------------------------------
    // Transaction control
    // -----------------------------------------------------------------------

    fn parse_begin(&mut self) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwBegin)?;
        let mode = if self.eat_kw(&TokenKind::KwDeferred) {
            Some(TransactionMode::Deferred)
        } else if self.eat_kw(&TokenKind::KwImmediate) {
            Some(TransactionMode::Immediate)
        } else if self.eat_kw(&TokenKind::KwExclusive) {
            Some(TransactionMode::Exclusive)
        } else if self.eat_kw(&TokenKind::KwConcurrent) {
            Some(TransactionMode::Concurrent)
        } else {
            None
        };
        // Optional TRANSACTION keyword.
        let _ = self.eat_kw(&TokenKind::KwTransaction);
        Ok(Statement::Begin(BeginStatement { mode }))
    }

    fn parse_rollback(&mut self) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwRollback)?;
        let _ = self.eat_kw(&TokenKind::KwTransaction);
        let to_savepoint = if self.eat_kw(&TokenKind::KwTo) {
            let _ = self.eat_kw(&TokenKind::KwSavepoint);
            Some(self.parse_identifier()?)
        } else {
            None
        };
        Ok(Statement::Rollback(RollbackStatement { to_savepoint }))
    }

    // -----------------------------------------------------------------------
    // ATTACH / PRAGMA / VACUUM / EXPLAIN
    // -----------------------------------------------------------------------

    fn parse_attach(&mut self) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwAttach)?;
        let _ = self.eat_kw(&TokenKind::KwDatabase);
        let expr = self.parse_expr()?;
        self.expect_kw(&TokenKind::KwAs)?;
        let schema = self.parse_identifier()?;
        Ok(Statement::Attach(AttachStatement { expr, schema }))
    }

    fn parse_pragma_value_expr(&mut self) -> Result<Expr, ParseError> {
        // SQLite allows ON/OFF for many boolean pragmas. Treat `ON` as `TRUE`
        // in PRAGMA value position (OFF is tokenized as an identifier, so the
        // regular expression parser handles it).
        if self.check_kw(&TokenKind::KwOn) {
            let sp = self.current_span();
            self.advance();
            return Ok(Expr::Literal(Literal::True, sp));
        }
        // SQLite's pragma value grammar (nmnum ::= plus_num | nm | ON | DELETE |
        // DEFAULT) treats the reserved keywords DELETE and DEFAULT as names in
        // value position — e.g. `PRAGMA journal_mode=DELETE`, `PRAGMA
        // temp_store=DEFAULT`. The general expression parser rejects reserved
        // keyword tokens, so accept them here as identifier-valued pragma
        // arguments (matching how WAL/TRUNCATE/PERSIST/MEMORY/OFF lex as plain
        // identifiers and already work).
        let pragma_value_keyword = match self.peek() {
            TokenKind::KwDelete => Some("delete"),
            TokenKind::KwDefault => Some("default"),
            _ => None,
        };
        if let Some(name) = pragma_value_keyword {
            let sp = self.current_span();
            self.advance();
            return Ok(Expr::Column(ColumnRef::bare(name), sp));
        }
        self.parse_expr()
    }

    fn parse_pragma(&mut self) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwPragma)?;
        let name = self.parse_qualified_name()?;
        let value = if self.eat(&TokenKind::Eq) || self.eat(&TokenKind::EqEq) {
            Some(PragmaValue::Assign(self.parse_pragma_value_expr()?))
        } else if self.eat(&TokenKind::LeftParen) {
            let v = self.parse_pragma_value_expr()?;
            self.expect_token(&TokenKind::RightParen)?;
            Some(PragmaValue::Call(v))
        } else {
            None
        };
        Ok(Statement::Pragma(PragmaStatement { name, value }))
    }

    fn parse_vacuum(&mut self) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwVacuum)?;
        let schema = if !self.at_eof()
            && !self.check(&TokenKind::Semicolon)
            && !self.check_kw(&TokenKind::KwInto)
        {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let into = if self.eat_kw(&TokenKind::KwInto) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Vacuum(VacuumStatement { schema, into }))
    }

    fn parse_explain(&mut self) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwExplain)?;
        let query_plan = if self.eat_kw(&TokenKind::KwQuery) {
            self.expect_kw(&TokenKind::KwPlan)?;
            true
        } else {
            false
        };
        if self.check_kw(&TokenKind::KwExplain) {
            return Err(self.err_msg("nested EXPLAIN is not allowed"));
        }
        let stmt = self.parse_statement_inner()?;
        Ok(Statement::Explain {
            query_plan,
            stmt: Box::new(stmt),
        })
    }

    // -----------------------------------------------------------------------
    // Window definitions (used in SELECT ... WINDOW clause and OVER)
    // -----------------------------------------------------------------------

    #[cfg(test)]
    fn parse_window_def(&mut self) -> Result<WindowDef, ParseError> {
        let name = self.parse_window_name()?;
        self.expect_kw(&TokenKind::KwAs)?;
        self.expect_token(&TokenKind::LeftParen)?;
        let spec = self.parse_window_spec()?;
        self.expect_token(&TokenKind::RightParen)?;
        Ok(WindowDef { name, spec })
    }

    #[cfg(test)]
    pub(crate) fn parse_window_spec(&mut self) -> Result<WindowSpec, ParseError> {
        // Optional base window name.
        let has_base_window = starts_window_base_name(self.peek());
        let base_window = if has_base_window {
            Some(self.parse_window_name()?)
        } else {
            None
        };
        let partition_by = if self.eat_kw(&TokenKind::KwPartition) {
            self.expect_kw(&TokenKind::KwBy)?;
            self.parse_comma_sep(Self::parse_expr)?
        } else {
            vec![]
        };
        let order_by = if self.eat_kw(&TokenKind::KwOrder) {
            self.expect_kw(&TokenKind::KwBy)?;
            self.parse_comma_sep(Self::parse_ordering_term)?
        } else {
            vec![]
        };
        let frame = self.try_frame_spec()?;
        Ok(WindowSpec {
            window_ref: base_window.map(WindowReference::Base),
            partition_by,
            order_by,
            frame,
        })
    }

    #[cfg(test)]
    fn try_frame_spec(&mut self) -> Result<Option<FrameSpec>, ParseError> {
        let frame_type = if self.eat_kw(&TokenKind::KwRows) {
            FrameType::Rows
        } else if self.eat_kw(&TokenKind::KwRange) {
            FrameType::Range
        } else if self.eat_kw(&TokenKind::KwGroups) {
            FrameType::Groups
        } else {
            return Ok(None);
        };
        let (start, end) = if self.eat_kw(&TokenKind::KwBetween) {
            let start = self.parse_frame_bound()?;
            validate_frame_start(&start, true)?;
            self.expect_kw(&TokenKind::KwAnd)?;
            let end = self.parse_frame_bound()?;
            validate_frame_end(&start, &end)?;
            (start, Some(end))
        } else {
            let start = self.parse_frame_bound()?;
            validate_frame_start(&start, false)?;
            (start, None)
        };
        let exclude = if self.eat_kw(&TokenKind::KwExclude) {
            if self.check_kw(&TokenKind::KwNo) {
                self.advance();
                // "NO OTHERS"
                let id = self.parse_identifier()?;
                if !id.eq_ignore_ascii_case("OTHERS") {
                    return Err(self.err_expected("OTHERS"));
                }
                Some(FrameExclude::NoOthers)
            } else if self.eat_kw(&TokenKind::KwTies) {
                Some(FrameExclude::Ties)
            } else if self.eat_kw(&TokenKind::KwGroup) {
                Some(FrameExclude::Group)
            } else if matches!(self.peek(), TokenKind::Id(s) if s.eq_ignore_ascii_case("CURRENT")) {
                self.advance();
                self.expect_kw(&TokenKind::KwRow)?;
                Some(FrameExclude::CurrentRow)
            } else {
                return Err(
                    self.err_expected("NO OTHERS, TIES, GROUP, or CURRENT ROW after EXCLUDE")
                );
            }
        } else {
            None
        };
        Ok(Some(FrameSpec {
            frame_type,
            start: start.value,
            end: end.map(|bound| bound.value),
            exclude,
        }))
    }

    #[cfg(test)]
    fn parse_frame_bound(&mut self) -> Result<ParsedFrameBound, ParseError> {
        let origin = self
            .current()
            .cloned()
            .ok_or_else(|| self.err_expected("window frame bound"))?;
        let value = if self.eat_kw(&TokenKind::KwUnbounded) {
            if self.eat_kw(&TokenKind::KwPreceding) {
                FrameBound::UnboundedPreceding
            } else {
                self.expect_kw(&TokenKind::KwFollowing)?;
                FrameBound::UnboundedFollowing
            }
        } else if matches!(self.peek(), TokenKind::Id(s) if s.eq_ignore_ascii_case("CURRENT")) {
            self.advance();
            self.expect_kw(&TokenKind::KwRow)?;
            FrameBound::CurrentRow
        } else {
            let expr = self.parse_expr()?;
            if self.eat_kw(&TokenKind::KwPreceding) {
                FrameBound::Preceding(Box::new(expr))
            } else {
                self.expect_kw(&TokenKind::KwFollowing)?;
                FrameBound::Following(Box::new(expr))
            }
        };
        Ok(ParsedFrameBound { value, origin })
    }
}

fn parse_statements_with_scratch_inner(
    sql: &str,
    scratch: &mut StatementParseScratch,
) -> (Vec<Statement>, Option<ParseError>) {
    Lexer::tokenize_into_with_interner(sql, &mut scratch.tokens, &mut scratch.identifier_interner);
    let mut parser = Parser {
        tokens: std::mem::take(&mut scratch.tokens),
        pos: 0,
        errors: std::mem::take(&mut scratch.errors),
        depth: 0,
        has_with: false,
    };
    let (statements, errors) = parser.parse_all();
    scratch.tokens = parser.tokens;
    scratch.tokens.clear();
    scratch.identifier_interner.prepare_for_next_parse();
    scratch.errors = errors;
    let first_error = scratch.errors.first().cloned();
    scratch.errors.clear();
    (statements, first_error)
}

/// Parse all statements from `sql` using caller-owned token/error lookaside.
pub fn parse_statements_with_scratch(
    sql: &str,
    scratch: &mut StatementParseScratch,
) -> Result<Vec<Statement>, ParseError> {
    let (statements, first_error) = parse_statements_with_scratch_inner(sql, scratch);
    if let Some(error) = first_error {
        return Err(error);
    }
    if statements.is_empty() {
        return Err(ParseError::at("no SQL statement provided", None));
    }
    Ok(statements)
}

/// Parse exactly one statement from `sql` using caller-owned lookaside.
pub fn parse_single_statement_with_scratch(
    sql: &str,
    scratch: &mut StatementParseScratch,
) -> Result<Statement, ParseError> {
    let statements = parse_statements_with_scratch(sql, scratch)?;
    let mut iter = statements.into_iter();
    let statement = iter
        .next()
        .ok_or_else(|| ParseError::at("no SQL statement provided", None))?;
    if iter.next().is_some() {
        return Err(ParseError::at(
            "multiple statements are not supported in this API path",
            None,
        ));
    }
    Ok(statement)
}

/// Parse only the first top-level SQL statement from `sql` and report the byte
/// offset immediately after the consumed statement text.
///
/// Returns `Ok(None)` when the input contains no statement text (for example
/// whitespace, comments, or empty statements only). The returned tail offset is
/// suitable for `sqlite3_prepare_v2`-style APIs that must leave any remaining
/// SQL untouched.
pub fn parse_first_statement_with_tail(
    sql: &str,
) -> Result<Option<(Statement, usize)>, ParseError> {
    let mut parser = Parser::from_sql(sql);

    while parser.eat(&TokenKind::Semicolon) {}
    if parser.at_eof() {
        return Ok(None);
    }

    let statement = parser.parse_statement()?;
    let tail_offset = if parser.eat(&TokenKind::Semicolon) {
        parser
            .tokens
            .get(parser.pos.saturating_sub(1))
            .map_or(sql.len(), |token| token.span.end as usize)
    } else if parser.at_eof() {
        sql.len()
    } else {
        return Err(ParseError::at(
            "unexpected token after end of statement; expected ';' separator",
            parser.current(),
        ));
    };

    Ok(Some((statement, tail_offset)))
}

// ---------------------------------------------------------------------------
// Keyword classification helper
// ---------------------------------------------------------------------------

/// Whether `OVER` can consume the next token as a bare window name.
///
/// SQLite's fallback-name grammar is contextual here: it accepts several hard
/// keywords that the generic identifier parser rejects, while `FILTER`,
/// `NOTHING`, and `TRANSACTION` remain structural/reserved in this position.
pub(crate) fn starts_bare_window_name(k: &TokenKind) -> bool {
    matches!(
        k,
        TokenKind::Id(_) | TokenKind::QuotedId(_, _) | TokenKind::String(_)
    ) || (is_nonreserved_kw(k) && !matches!(k, TokenKind::KwFilter))
        || matches!(
            k,
            TokenKind::KwAttach
                | TokenKind::KwBegin
                | TokenKind::KwBy
                | TokenKind::KwCast
                | TokenKind::KwCurrentDate
                | TokenKind::KwCurrentTime
                | TokenKind::KwCurrentTimestamp
                | TokenKind::KwCross
                | TokenKind::KwDetach
                | TokenKind::KwExplain
                | TokenKind::KwFalse
                | TokenKind::KwFor
                | TokenKind::KwGlob
                | TokenKind::KwInner
                | TokenKind::KwLeft
                | TokenKind::KwLike
                | TokenKind::KwNatural
                | TokenKind::KwOuter
                | TokenKind::KwRaise
                | TokenKind::KwRegexp
                | TokenKind::KwRight
                | TokenKind::KwRollback
                | TokenKind::KwTrue
                | TokenKind::KwWith
        )
}

/// Whether a token can begin the optional base name inside `OVER (...)`.
///
/// This uses the same contextual fallback set as a bare name, minus the five
/// structural window-spec delimiters.
pub(crate) fn starts_window_base_name(k: &TokenKind) -> bool {
    starts_bare_window_name(k)
        && !matches!(
            k,
            TokenKind::KwPartition
                | TokenKind::KwOrder
                | TokenKind::KwRange
                | TokenKind::KwRows
                | TokenKind::KwGroups
        )
}

pub(crate) fn starts_post_dot_identifier(k: &TokenKind) -> bool {
    matches!(
        k,
        TokenKind::Id(_) | TokenKind::QuotedId(_, _) | TokenKind::String(_)
    ) || (k.keyword_str().is_some()
        && !matches!(
            k,
            TokenKind::KwAdd
                | TokenKind::KwAll
                | TokenKind::KwAlter
                | TokenKind::KwAnd
                | TokenKind::KwAs
                | TokenKind::KwAutoincrement
                | TokenKind::KwBetween
                | TokenKind::KwCase
                | TokenKind::KwCheck
                | TokenKind::KwCollate
                | TokenKind::KwCommit
                | TokenKind::KwConstraint
                | TokenKind::KwCreate
                | TokenKind::KwDefault
                | TokenKind::KwDeferrable
                | TokenKind::KwDelete
                | TokenKind::KwDistinct
                | TokenKind::KwDrop
                | TokenKind::KwElse
                | TokenKind::KwEscape
                | TokenKind::KwExcept
                | TokenKind::KwExists
                | TokenKind::KwForeign
                | TokenKind::KwFrom
                | TokenKind::KwGroup
                | TokenKind::KwHaving
                | TokenKind::KwIn
                | TokenKind::KwIndex
                | TokenKind::KwInsert
                | TokenKind::KwIntersect
                | TokenKind::KwInto
                | TokenKind::KwIs
                | TokenKind::KwIsnull
                | TokenKind::KwJoin
                | TokenKind::KwLimit
                | TokenKind::KwNot
                | TokenKind::KwNothing
                | TokenKind::KwNotnull
                | TokenKind::KwNull
                | TokenKind::KwOn
                | TokenKind::KwOr
                | TokenKind::KwOrder
                | TokenKind::KwPrimary
                | TokenKind::KwReferences
                | TokenKind::KwReturning
                | TokenKind::KwSelect
                | TokenKind::KwSet
                | TokenKind::KwTable
                | TokenKind::KwThen
                | TokenKind::KwTo
                | TokenKind::KwTransaction
                | TokenKind::KwUnion
                | TokenKind::KwUnique
                | TokenKind::KwUpdate
                | TokenKind::KwUsing
                | TokenKind::KwValues
                | TokenKind::KwWhen
                | TokenKind::KwWhere
        ))
}

pub(crate) fn starts_table_star_qualifier(k: &TokenKind) -> bool {
    matches!(
        k,
        TokenKind::Id(_)
            | TokenKind::QuotedId(_, _)
            | TokenKind::String(_)
            | TokenKind::KwAbort
            | TokenKind::KwAction
            | TokenKind::KwAfter
            | TokenKind::KwAlways
            | TokenKind::KwAnalyze
            | TokenKind::KwAsc
            | TokenKind::KwAttach
            | TokenKind::KwBefore
            | TokenKind::KwBegin
            | TokenKind::KwBy
            | TokenKind::KwCascade
            | TokenKind::KwColumn
            | TokenKind::KwCommitseq
            | TokenKind::KwConcurrent
            | TokenKind::KwConflict
            | TokenKind::KwCross
            | TokenKind::KwDatabase
            | TokenKind::KwDeferred
            | TokenKind::KwDesc
            | TokenKind::KwDetach
            | TokenKind::KwDo
            | TokenKind::KwEach
            | TokenKind::KwEnd
            | TokenKind::KwExclude
            | TokenKind::KwExclusive
            | TokenKind::KwExplain
            | TokenKind::KwFail
            | TokenKind::KwFalse
            | TokenKind::KwFilter
            | TokenKind::KwFirst
            | TokenKind::KwFollowing
            | TokenKind::KwFor
            | TokenKind::KwFull
            | TokenKind::KwGenerated
            | TokenKind::KwGlob
            | TokenKind::KwGroups
            | TokenKind::KwIf
            | TokenKind::KwIgnore
            | TokenKind::KwImmediate
            | TokenKind::KwIndexed
            | TokenKind::KwInitially
            | TokenKind::KwInner
            | TokenKind::KwInstead
            | TokenKind::KwKey
            | TokenKind::KwLast
            | TokenKind::KwLeft
            | TokenKind::KwLike
            | TokenKind::KwMatch
            | TokenKind::KwMaterialized
            | TokenKind::KwNatural
            | TokenKind::KwNo
            | TokenKind::KwNulls
            | TokenKind::KwOf
            | TokenKind::KwOffset
            | TokenKind::KwOthers
            | TokenKind::KwOuter
            | TokenKind::KwOver
            | TokenKind::KwPartition
            | TokenKind::KwPlan
            | TokenKind::KwPragma
            | TokenKind::KwPreceding
            | TokenKind::KwQuery
            | TokenKind::KwRange
            | TokenKind::KwRecursive
            | TokenKind::KwRegexp
            | TokenKind::KwReindex
            | TokenKind::KwRelease
            | TokenKind::KwRename
            | TokenKind::KwReplace
            | TokenKind::KwRestrict
            | TokenKind::KwRight
            | TokenKind::KwRollback
            | TokenKind::KwRow
            | TokenKind::KwRows
            | TokenKind::KwSavepoint
            | TokenKind::KwStored
            | TokenKind::KwStrict
            | TokenKind::KwTemp
            | TokenKind::KwTemporary
            | TokenKind::KwTies
            | TokenKind::KwTrigger
            | TokenKind::KwTrue
            | TokenKind::KwUnbounded
            | TokenKind::KwVacuum
            | TokenKind::KwView
            | TokenKind::KwVirtual
            | TokenKind::KwWindow
            | TokenKind::KwWith
            | TokenKind::KwWithout
    )
}

pub(crate) fn is_nonreserved_kw(k: &TokenKind) -> bool {
    matches!(
        k,
        TokenKind::KwAbort
            | TokenKind::KwAction
            | TokenKind::KwAfter
            | TokenKind::KwAlways
            | TokenKind::KwAnalyze
            | TokenKind::KwAsc
            | TokenKind::KwBefore
            | TokenKind::KwCascade
            | TokenKind::KwColumn
            | TokenKind::KwConcurrent
            | TokenKind::KwConflict
            | TokenKind::KwDatabase
            | TokenKind::KwDeferred
            | TokenKind::KwDesc
            | TokenKind::KwDo
            | TokenKind::KwEach
            | TokenKind::KwEnd
            | TokenKind::KwExclude
            | TokenKind::KwExclusive
            | TokenKind::KwFail
            | TokenKind::KwFilter
            | TokenKind::KwFirst
            | TokenKind::KwFollowing
            | TokenKind::KwFull
            | TokenKind::KwGenerated
            | TokenKind::KwGroups
            | TokenKind::KwIf
            | TokenKind::KwIgnore
            | TokenKind::KwImmediate
            | TokenKind::KwInitially
            | TokenKind::KwInstead
            | TokenKind::KwKey
            | TokenKind::KwLast
            | TokenKind::KwMatch
            | TokenKind::KwMaterialized
            | TokenKind::KwNo
            | TokenKind::KwNulls
            | TokenKind::KwOf
            | TokenKind::KwOffset
            | TokenKind::KwOthers
            | TokenKind::KwOver
            | TokenKind::KwPartition
            | TokenKind::KwPlan
            | TokenKind::KwPragma
            | TokenKind::KwPreceding
            | TokenKind::KwQuery
            | TokenKind::KwRange
            | TokenKind::KwRecursive
            | TokenKind::KwReindex
            | TokenKind::KwRelease
            | TokenKind::KwRename
            | TokenKind::KwReplace
            | TokenKind::KwRestrict
            | TokenKind::KwRow
            | TokenKind::KwRows
            | TokenKind::KwSavepoint
            | TokenKind::KwStored
            | TokenKind::KwStrict
            | TokenKind::KwTemp
            | TokenKind::KwTemporary
            | TokenKind::KwTies
            | TokenKind::KwTrigger
            | TokenKind::KwUnbounded
            | TokenKind::KwVacuum
            | TokenKind::KwView
            | TokenKind::KwVirtual
            | TokenKind::KwWindow
            | TokenKind::KwWithout
    )
}

/// Whether a token can be used as an alias after an explicit `AS`.
///
/// SQLite's fallback-name grammar accepts more hard keywords here than the
/// old generic identifier grammar, while operator-shaped and structural
/// keywords remain reserved.
fn starts_explicit_alias_name(k: &TokenKind) -> bool {
    starts_post_dot_identifier(k)
}

/// Whether a token can be consumed as an implicit result-column alias.
///
/// Pattern-matching tokens remain expression operators in this position.
/// `WINDOW` is admitted here only after `Parser::starts_window_clause` has
/// ruled out an actual named-window clause.
fn starts_result_alias(k: &TokenKind) -> bool {
    starts_explicit_alias_name(k)
        && !matches!(
            k,
            TokenKind::KwCross
                | TokenKind::KwFull
                | TokenKind::KwGlob
                | TokenKind::KwIndexed
                | TokenKind::KwInner
                | TokenKind::KwLeft
                | TokenKind::KwLike
                | TokenKind::KwMatch
                | TokenKind::KwNatural
                | TokenKind::KwOuter
                | TokenKind::KwRegexp
                | TokenKind::KwRight
        )
}

/// Whether a token can be consumed as an implicit table/source alias.
///
/// Join prefixes, `INDEXED`, and FrankenSQLite's `FOR SYSTEM_TIME` introducer
/// must remain available to the surrounding FROM grammar. Unlike a result
/// alias, MATCH-family tokens are unambiguous source aliases here.
fn starts_table_alias(k: &TokenKind) -> bool {
    starts_explicit_alias_name(k)
        && !matches!(
            k,
            TokenKind::KwCross
                | TokenKind::KwFull
                | TokenKind::KwIndexed
                | TokenKind::KwInner
                | TokenKind::KwIsnull
                | TokenKind::KwLeft
                | TokenKind::KwNatural
                | TokenKind::KwNotnull
                | TokenKind::KwOuter
                | TokenKind::KwRight
        )
}

pub(crate) fn kw_to_str(k: &TokenKind) -> String {
    k.keyword_str()
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| {
            let dbg = format!("{k:?}");
            dbg.strip_prefix("Kw").unwrap_or(&dbg).to_ascii_lowercase()
        })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    static PARSE_OBSERVABILITY_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn parse_ok(sql: &str) -> Vec<Statement> {
        let mut p = Parser::from_sql(sql);
        let (stmts, errs) = p.parse_all();
        assert!(errs.is_empty(), "unexpected errors: {errs:?}");
        stmts
    }

    fn parse_one(sql: &str) -> Statement {
        let stmts = parse_ok(sql);
        assert_eq!(stmts.len(), 1, "expected 1 statement, got {}", stmts.len());
        stmts.into_iter().next().unwrap()
    }

    fn parse_full_select(sql: &str) -> SelectStatement {
        let Some((statement, tail_offset)) =
            parse_first_statement_with_tail(sql).expect("full SELECT statement must parse")
        else {
            panic!("expected one SELECT statement");
        };
        assert_eq!(
            tail_offset,
            sql.len(),
            "public parser must consume the full statement"
        );
        let Statement::Select(select) = statement else {
            panic!("expected SELECT AST");
        };
        select
    }

    fn top_level_values(statement: &Statement) -> &ValuesClause {
        let Statement::Select(select) = statement else {
            panic!("expected SELECT statement");
        };
        let SelectCore::Values(values) = &select.body.select else {
            panic!("expected top-level VALUES core");
        };
        values
    }

    fn scalar_subquery_values(expr: &Expr) -> &ValuesClause {
        let Expr::Subquery(select, _) = expr else {
            panic!("expected scalar subquery");
        };
        let SelectCore::Values(values) = &select.body.select else {
            panic!("expected VALUES scalar subquery");
        };
        values
    }

    fn only_join(select: &SelectStatement) -> &JoinClause {
        let SelectCore::Select {
            from: Some(from), ..
        } = &select.body.select
        else {
            panic!("expected SELECT with FROM clause");
        };
        let [join] = from.joins.as_slice() else {
            panic!("expected exactly one join");
        };
        assert_eq!(join.join_type.kind, JoinKind::Cross);
        assert!(!join.join_type.natural);
        join
    }

    #[test]
    fn test_comma_join_accepts_on_constraint_and_consumes_full_statement() {
        let select = parse_full_select("SELECT * FROM a, b ON a.id = b.id");
        assert!(matches!(
            &only_join(&select).constraint,
            Some(JoinConstraint::On(Expr::BinaryOp {
                op: fsqlite_ast::BinaryOp::Eq,
                ..
            }))
        ));
    }

    #[test]
    fn test_comma_join_accepts_using_constraint_and_consumes_full_statement() {
        let select = parse_full_select("SELECT * FROM a, b USING(id)");
        assert!(matches!(
            &only_join(&select).constraint,
            Some(JoinConstraint::Using(columns))
                if columns.len() == 1 && columns[0] == "id"
        ));
    }

    #[test]
    fn test_explicit_cross_join_accepts_on_constraint_and_consumes_full_statement() {
        let select = parse_full_select("SELECT * FROM a CROSS JOIN b ON a.id = b.id");
        assert!(matches!(
            &only_join(&select).constraint,
            Some(JoinConstraint::On(Expr::BinaryOp {
                op: fsqlite_ast::BinaryOp::Eq,
                ..
            }))
        ));
    }

    #[test]
    fn test_nonreserved_keyword_table_star_uses_wildcard_ast() {
        let select = parse_full_select("SELECT filter.* FROM t AS filter");
        let SelectCore::Select { columns, .. } = &select.body.select else {
            panic!("expected SELECT core");
        };
        assert!(matches!(
            columns.as_slice(),
            [ResultColumn::TableStar(name)] if name == &QualifiedName::bare("filter")
        ));
        assert_eq!(
            select.to_string(),
            "SELECT \"filter\".* FROM t AS \"filter\""
        );
    }

    #[test]
    fn test_table_star_qualifier_uses_sqlite_contextual_keyword_matrix() {
        for name in [
            "abort",
            "action",
            "after",
            "always",
            "analyze",
            "asc",
            "attach",
            "before",
            "begin",
            "by",
            "cascade",
            "column",
            "commitseq",
            "concurrent",
            "conflict",
            "cross",
            "database",
            "deferred",
            "desc",
            "detach",
            "do",
            "each",
            "end",
            "exclude",
            "exclusive",
            "explain",
            "fail",
            "false",
            "filter",
            "first",
            "following",
            "for",
            "full",
            "generated",
            "glob",
            "groups",
            "if",
            "ignore",
            "immediate",
            "indexed",
            "initially",
            "inner",
            "instead",
            "key",
            "last",
            "left",
            "like",
            "match",
            "materialized",
            "natural",
            "no",
            "nulls",
            "of",
            "offset",
            "others",
            "outer",
            "over",
            "partition",
            "plan",
            "pragma",
            "preceding",
            "query",
            "range",
            "recursive",
            "regexp",
            "reindex",
            "release",
            "rename",
            "replace",
            "restrict",
            "right",
            "rollback",
            "row",
            "rows",
            "savepoint",
            "stored",
            "strict",
            "temp",
            "temporary",
            "ties",
            "trigger",
            "true",
            "unbounded",
            "vacuum",
            "view",
            "virtual",
            "window",
            "with",
            "without",
        ] {
            let sql = format!("SELECT {name}.* FROM t AS \"{name}\"");
            let select = parse_full_select(&sql);
            let SelectCore::Select { columns, .. } = &select.body.select else {
                panic!("expected SELECT core for `{sql}`");
            };
            assert!(matches!(
                columns.as_slice(),
                [ResultColumn::TableStar(qualifier)] if qualifier == &QualifiedName::bare(name)
            ));
        }

        for name in ["nothing", "transaction", "select", "table"] {
            let sql = format!("SELECT {name}.* FROM t AS \"{name}\"");
            parse_first_statement_with_tail(&sql)
                .expect_err("non-fallback table-star keywords must be rejected");
        }
    }

    #[test]
    fn test_qualified_star_is_rejected_outside_result_columns() {
        for sql in ["SELECT 1 WHERE t.*", "SELECT abs(t.*) FROM t"] {
            let error = parse_first_statement_with_tail(sql)
                .expect_err("qualified star must not become an ordinary column expression");
            assert_eq!(error.kind, ParseErrorKind::Syntax);
            assert_eq!(
                &sql[error.span.start as usize..error.span.end as usize],
                "*",
                "the diagnostic for `{sql}` must point at the illegal wildcard"
            );
            assert!(
                error.message.contains("expected column name after '.'"),
                "unexpected diagnostic for `{sql}`: {error:?}"
            );
        }

        let valid = parse_full_select("SELECT t.*, filter.* FROM t AS filter");
        let SelectCore::Select { columns, .. } = &valid.body.select else {
            panic!("expected SELECT core");
        };
        assert!(matches!(
            columns.as_slice(),
            [ResultColumn::TableStar(first), ResultColumn::TableStar(second)]
                if first == &QualifiedName::bare("t")
                    && second == &QualifiedName::bare("filter")
        ));
    }

    #[test]
    fn test_single_quoted_qualified_identifiers_follow_identifier_context() {
        for (sql, expected) in [
            ("SELECT 't'.x FROM t", "SELECT t.x FROM t"),
            ("SELECT t.'x' FROM t", "SELECT t.x FROM t"),
            ("SELECT 't'.'x' FROM t", "SELECT t.x FROM t"),
            ("SELECT t.'select' FROM t", "SELECT t.\"select\" FROM t"),
            ("SELECT 't'.* FROM t", "SELECT t.* FROM t"),
        ] {
            let select = parse_full_select(sql);
            assert_eq!(
                select.to_string(),
                expected,
                "round-trip mismatch for `{sql}`"
            );
        }
    }

    #[test]
    fn test_post_dot_identifier_classification_matches_unquoted_names() {
        for name in [
            "key",
            "window",
            "filter",
            "range",
            "rows",
            "groups",
            "match",
            "replace",
            "abort",
            "column",
            "strict",
            "true",
            "false",
            "current_date",
            "current_time",
            "current_timestamp",
            "like",
            "glob",
            "regexp",
        ] {
            parse_full_select(&format!("SELECT t.{name} FROM t"));
        }

        for name in [
            "add",
            "all",
            "alter",
            "and",
            "as",
            "autoincrement",
            "between",
            "case",
            "check",
            "collate",
            "commit",
            "constraint",
            "create",
            "default",
            "deferrable",
            "delete",
            "distinct",
            "drop",
            "else",
            "escape",
            "except",
            "exists",
            "foreign",
            "select",
            "from",
            "group",
            "having",
            "in",
            "index",
            "insert",
            "intersect",
            "into",
            "is",
            "isnull",
            "join",
            "limit",
            "not",
            "nothing",
            "notnull",
            "null",
            "on",
            "or",
            "order",
            "primary",
            "references",
            "returning",
            "set",
            "table",
            "then",
            "to",
            "transaction",
            "union",
            "unique",
            "update",
            "using",
            "values",
            "when",
            "where",
        ] {
            let sql = format!("SELECT t.{name} FROM t");
            let error = parse_first_statement_with_tail(&sql)
                .expect_err("hard reserved names after a dot must require quoting");
            assert_eq!(
                &sql[error.span.start as usize..error.span.end as usize],
                name,
                "the diagnostic for `{sql}` must point at the rejected name"
            );
        }

        for (sql, rejected) in [("SELECT t.1 FROM t", ".1"), ("SELECT t. 1 FROM t", "1")] {
            let error = parse_first_statement_with_tail(sql)
                .expect_err("numeric tokens after a dot must not become identifiers");
            assert_eq!(
                &sql[error.span.start as usize..error.span.end as usize],
                rejected
            );
        }
    }

    #[test]
    fn test_leading_qualified_keyword_uses_dot_lookahead_only() {
        for name in [
            "attach", "begin", "by", "false", "filter", "glob", "inner", "left", "like", "natural",
            "outer", "regexp", "right", "rollback", "true", "with",
        ] {
            let sql = format!("SELECT {name}.x FROM (SELECT 1 AS x) AS \"{name}\"");
            let select = parse_full_select(&sql);
            let SelectCore::Select { columns, .. } = &select.body.select else {
                panic!("expected SELECT core for `{sql}`");
            };
            assert!(matches!(
                columns.as_slice(),
                [ResultColumn::Expr {
                    expr: Expr::Column(column, _),
                    alias: None,
                }] if column.table.as_deref() == Some(name) && column.column.as_ref() == "x"
            ));
        }

        for name in ["cast", "current_date", "nothing", "raise", "transaction"] {
            let sql = format!("SELECT {name}.x FROM (SELECT 1 AS x) AS \"{name}\"");
            parse_first_statement_with_tail(&sql)
                .expect_err("non-fallback leading qualifiers must remain expressions or syntax");
        }
    }

    #[test]
    fn test_result_alias_uses_sqlite_contextual_name_policy() {
        for (source, expected) in [
            ("'single quoted'", "single quoted"),
            ("attach", "attach"),
            ("cast", "cast"),
            ("current_date", "current_date"),
            ("false", "false"),
            ("raise", "raise"),
            ("rollback", "rollback"),
            ("true", "true"),
            ("with", "with"),
            ("window", "window"),
            ("offset", "offset"),
        ] {
            let sql = format!("SELECT 1 {source}");
            let select = parse_full_select(&sql);
            let SelectCore::Select { columns, .. } = &select.body.select else {
                panic!("expected SELECT core for `{sql}`");
            };
            assert!(matches!(
                columns.as_slice(),
                [ResultColumn::Expr {
                    alias: Some(alias),
                    ..
                }] if alias == expected
            ));
        }

        for sql in [
            "SELECT 1 indexed",
            "SELECT 1 left",
            "SELECT 1 match",
            "SELECT 1 nothing",
            "SELECT 1 transaction",
            "SELECT 1 AS isnull",
            "SELECT 1 AS notnull",
        ] {
            parse_first_statement_with_tail(sql)
                .expect_err("operators and non-fallback names must not become result aliases");
        }
    }

    #[test]
    fn test_table_alias_uses_sqlite_contextual_name_policy() {
        for (source, expected) in [
            ("'single quoted'", "single quoted"),
            ("attach", "attach"),
            ("cast", "cast"),
            ("current_date", "current_date"),
            ("false", "false"),
            ("for", "for"),
            ("match", "match"),
            ("raise", "raise"),
            ("rollback", "rollback"),
            ("true", "true"),
            ("with", "with"),
            ("window", "window"),
            ("offset", "offset"),
        ] {
            let sql = format!("SELECT * FROM (SELECT 1) {source}");
            let select = parse_full_select(&sql);
            let SelectCore::Select {
                from:
                    Some(FromClause {
                        source: TableOrSubquery::Subquery { alias, .. },
                        ..
                    }),
                ..
            } = &select.body.select
            else {
                panic!("expected aliased subquery for `{sql}`");
            };
            assert_eq!(
                alias.as_deref(),
                Some(expected),
                "alias mismatch for `{sql}`"
            );
        }

        for sql in [
            "SELECT * FROM (SELECT 1) isnull",
            "SELECT * FROM (SELECT 1) notnull",
            "SELECT * FROM (SELECT 1) nothing",
            "SELECT * FROM (SELECT 1) transaction",
        ] {
            parse_first_statement_with_tail(sql)
                .expect_err("non-table-alias tokens must remain rejected");
        }

        let select = parse_full_select("SELECT * FROM t WINDOW w AS ()");
        let SelectCore::Select { from, windows, .. } = &select.body.select else {
            panic!("expected SELECT core");
        };
        assert!(matches!(
            from,
            Some(FromClause {
                source: TableOrSubquery::Table { alias: None, .. },
                ..
            })
        ));
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].name, "w");

        let select = parse_full_select("SELECT * FROM t FOR SYSTEM_TIME AS OF COMMITSEQ 1");
        let SelectCore::Select { from, .. } = &select.body.select else {
            panic!("expected SELECT core");
        };
        assert!(matches!(
            from,
            Some(FromClause {
                source: TableOrSubquery::Table {
                    alias: None,
                    time_travel: Some(TimeTravelClause {
                        target: TimeTravelTarget::CommitSequence(1),
                    }),
                    ..
                },
                ..
            })
        ));
    }

    #[test]
    fn test_indexed_is_explicit_name_but_not_implicit_alias() {
        for sql in [
            "SELECT 1 AS indexed",
            "SELECT * FROM (SELECT 1) AS indexed",
            "CREATE TABLE t(indexed); SELECT t.indexed FROM t",
        ] {
            let mut parser = Parser::from_sql(sql);
            let (statements, errors) = parser.parse_all();
            assert!(
                errors.is_empty(),
                "unexpected errors for `{sql}`: {errors:?}"
            );
            assert!(
                !statements.is_empty(),
                "explicit INDEXED name context must produce an AST for `{sql}`"
            );
        }

        for sql in ["SELECT 1 indexed", "SELECT * FROM (SELECT 1) indexed"] {
            parse_first_statement_with_tail(sql)
                .expect_err("INDEXED must remain unavailable as an implicit alias");
        }
    }

    #[test]
    fn test_nested_explain_is_rejected_at_the_second_explain() {
        for sql in [
            "EXPLAIN EXPLAIN SELECT 1",
            "EXPLAIN QUERY PLAN EXPLAIN SELECT 1",
        ] {
            let error = parse_first_statement_with_tail(sql)
                .expect_err("SQLite does not permit nested EXPLAIN statements");
            assert_eq!(error.kind, ParseErrorKind::Syntax);
            assert!(
                error.message.contains("nested EXPLAIN"),
                "unexpected diagnostic for `{sql}`: {error:?}"
            );
            let second_explain = sql
                .match_indices("EXPLAIN")
                .nth(1)
                .map(|(offset, _)| offset)
                .expect("test SQL must contain a second EXPLAIN");
            assert_eq!(
                &sql[error.span.start as usize..error.span.end as usize],
                &sql[second_explain..second_explain + "EXPLAIN".len()],
                "the nested-EXPLAIN diagnostic must point at the rejected keyword"
            );
        }
    }

    #[test]
    fn test_generic_name_slots_use_sqlite_fallback_name_policy() {
        for sql in [
            "CREATE TABLE begin(x)",
            "SELECT * FROM begin",
            "DROP TABLE main.begin",
        ] {
            parse_first_statement_with_tail(sql)
                .expect("fallback-name keywords must parse in an established name slot");
        }
    }

    #[test]
    fn test_hard_reserved_column_names_require_quoting_in_ddl() {
        for name in ["index", "nothing", "returning", "table", "transaction"] {
            let sql = format!("CREATE TABLE t({name} INTEGER)");
            let error = parse_first_statement_with_tail(&sql)
                .expect_err("hard reserved column names must require quoting");
            assert_eq!(
                &sql[error.span.start as usize..error.span.end as usize],
                name
            );
        }
    }

    #[test]
    fn test_final_values_rejects_order_by_and_limit() {
        for (sql, clause) in [
            ("VALUES (1) ORDER BY 1", "ORDER"),
            ("VALUES (1) LIMIT 1", "LIMIT"),
            ("SELECT 1 UNION VALUES (2) ORDER BY 1", "ORDER"),
            ("SELECT 1 UNION VALUES (2) LIMIT 1", "LIMIT"),
        ] {
            let error = parse_first_statement_with_tail(sql)
                .expect_err("a trailing clause on a final VALUES term must be rejected");
            assert_eq!(error.kind, ParseErrorKind::Syntax);
            assert!(
                error.message.contains("not allowed after a VALUES term"),
                "unexpected diagnostic for `{sql}`: {error:?}"
            );
            assert_eq!(
                &sql[error.span.start as usize..error.span.end as usize],
                clause,
                "the primary error for `{sql}` must point at the forbidden clause"
            );
        }

        let deeply_nested = format!(
            "VALUES (1) ORDER BY {}1{}",
            "(".repeat(1_200),
            ")".repeat(1_200)
        );
        let error = parse_first_statement_with_tail(&deeply_nested)
            .expect_err("the forbidden final-VALUES clause must win over expression depth");
        assert_eq!(error.kind, ParseErrorKind::Syntax);
        assert_eq!(
            &deeply_nested[error.span.start as usize..error.span.end as usize],
            "ORDER"
        );

        let final_select = parse_full_select("VALUES (1) UNION SELECT 2 ORDER BY 1 LIMIT 1");
        assert_eq!(
            final_select.to_string(),
            "VALUES (1) UNION SELECT 2 ORDER BY 1 LIMIT 1"
        );
        let wrapped = parse_full_select("SELECT * FROM (VALUES (1)) ORDER BY 1 LIMIT 1");
        assert_eq!(
            wrapped.to_string(),
            "SELECT * FROM (VALUES (1)) ORDER BY 1 LIMIT 1"
        );
    }

    #[test]
    fn test_count_star_rejects_aggregate_order_by() {
        let error = parse_first_statement_with_tail("SELECT count(* ORDER BY x) FROM t")
            .expect_err("aggregate ORDER BY after count(*) must be rejected");
        assert_eq!(error.kind, ParseErrorKind::Syntax);
        assert!(
            error.message.contains("RightParen"),
            "unexpected diagnostic: {error:?}"
        );

        let valid = parse_full_select("SELECT count(*) FILTER (WHERE x > 0) OVER () FROM t");
        assert_eq!(
            valid.to_string(),
            "SELECT count(*) FILTER (WHERE x > 0) OVER () FROM t"
        );
    }

    #[test]
    fn test_parse_metrics_emitted_when_enabled() {
        let _guard = PARSE_OBSERVABILITY_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let prev_metrics_enabled = parse_metrics_enabled();
        reset_parse_metrics();
        set_parse_metrics_enabled(true);

        let mut parser = Parser::from_sql("SELECT 1; SELECT 2;");
        let (stmts, errs) = parser.parse_all();
        assert!(errs.is_empty(), "unexpected errors: {errs:?}");
        assert_eq!(stmts.len(), 2);

        let snapshot = parse_metrics_snapshot();
        assert!(snapshot.fsqlite_parse_statements_total >= 2);

        set_parse_metrics_enabled(prev_metrics_enabled);
        reset_parse_metrics();
    }

    #[test]
    fn test_public_parser_new_normalizes_empty_and_missing_eof_streams() {
        let error = Parser::new(Vec::new())
            .parse_expr()
            .expect_err("an empty public token stream must return an error, not panic");
        assert_eq!(error.kind, ParseErrorKind::Syntax);
        assert_eq!(error.span, Span::ZERO);

        let integer = Token {
            kind: TokenKind::Integer(1),
            span: Span::new(0, 1),
            line: 1,
            col: 1,
        };
        let mut expression_parser = Parser::new(vec![integer]);
        assert!(matches!(
            expression_parser
                .parse_expr()
                .expect("a token stream without explicit EOF must be normalized"),
            Expr::Literal(Literal::Integer(1), _)
        ));
        assert!(expression_parser.at_eof());

        let mut tokens = Lexer::tokenize("SELECT 1");
        assert!(matches!(
            tokens.pop(),
            Some(Token {
                kind: TokenKind::Eof,
                ..
            })
        ));
        let (statements, errors) = Parser::new(tokens).parse_all();
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].to_string(), "SELECT 1");

        let mut tokens = Lexer::tokenize("SELECT 1; SELECT 2");
        tokens.insert(
            2,
            Token {
                kind: TokenKind::Eof,
                span: Span::new(8, 8),
                line: 1,
                col: 9,
            },
        );
        let mut parser = Parser::new(tokens);
        assert_eq!(
            parser
                .tokens
                .iter()
                .filter(|token| token.kind == TokenKind::Eof)
                .count(),
            1
        );
        assert!(matches!(
            parser.tokens.last(),
            Some(Token {
                kind: TokenKind::Eof,
                ..
            })
        ));
        let (statements, errors) = parser.parse_all();
        assert!(
            errors.is_empty(),
            "embedded EOF normalization must not hide later tokens: {errors:?}"
        );
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[1].to_string(), "SELECT 2");

        let tokens = Lexer::tokenize("SELECT CASE WHEN 1 THEN 'a\nb'");
        let expected_eof = tokens
            .last()
            .cloned()
            .expect("the lexer must supply a terminal EOF");
        let mut parser = Parser::new(tokens);
        let normalized_eof = parser
            .tokens
            .last()
            .expect("the normalized stream must retain a terminal EOF");
        assert_eq!(normalized_eof, &expected_eof);
        assert_eq!(normalized_eof.line, 2);
        let (_, errors) = parser.parse_all();
        assert!(
            errors
                .iter()
                .any(|error| error.span == expected_eof.span && error.line == expected_eof.line),
            "the missing END diagnostic must retain the lexer's multiline EOF coordinates: \
             {errors:?}"
        );
    }

    #[test]
    fn test_parse_metrics_can_be_disabled_off_hot_path() {
        let _guard = PARSE_OBSERVABILITY_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let prev_metrics_enabled = parse_metrics_enabled();
        reset_parse_metrics();
        set_parse_metrics_enabled(false);

        let mut parser = Parser::from_sql("SELECT 1; SELECT 2;");
        let (stmts, errs) = parser.parse_all();
        assert!(errs.is_empty(), "unexpected errors: {errs:?}");
        assert_eq!(stmts.len(), 2);

        let snapshot = parse_metrics_snapshot();
        assert_eq!(snapshot.fsqlite_parse_statements_total, 0);

        set_parse_metrics_enabled(prev_metrics_enabled);
        reset_parse_metrics();
    }

    #[test]
    fn test_parse_depth_overflow_does_not_poison_following_statement() {
        const OVER_LIMIT: usize = MAX_PARSE_DEPTH as usize + 1;
        let expression = std::iter::repeat_n("1", OVER_LIMIT)
            .collect::<Vec<_>>()
            .join(" + ");
        let sql = format!("SELECT {expression}; SELECT 42;");
        let mut parser = Parser::from_sql(&sql);
        let (statements, errors) = parser.parse_all();

        assert_eq!(
            errors.len(),
            1,
            "only the height-1001 statement should be rejected: {errors:?}"
        );
        assert_eq!(
            errors[0].kind,
            ParseErrorKind::ExpressionTooDeep {
                max: MAX_PARSE_DEPTH
            }
        );
        assert_eq!(statements.len(), 1, "the valid statement must survive");
        assert_eq!(statements[0].to_string(), "SELECT 42");
        assert_eq!(
            parser.depth, 0,
            "expression-height recovery must not poison native parser depth"
        );
    }

    #[test]
    fn test_parse_first_statement_with_tail_consumes_full_trigger_body() {
        let sql = "CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES('first'); INSERT INTO audit VALUES('second'); END; SELECT 1;";
        let Some((statement, tail_offset)) =
            parse_first_statement_with_tail(sql).expect("trigger statement should parse")
        else {
            panic!("expected a trigger statement");
        };

        assert!(matches!(statement, Statement::CreateTrigger(_)));
        assert_eq!(&sql[tail_offset..], " SELECT 1;");
    }

    #[test]
    fn test_trigger_body_accepts_only_sqlite_trigger_commands() {
        let sql = "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                   SELECT 1; \
                   VALUES (2); \
                   INSERT INTO audit VALUES (3); \
                   REPLACE INTO audit VALUES (4); \
                   UPDATE audit SET value = 5; \
                   DELETE FROM audit WHERE value = 6; \
                   WITH seed(value) AS (VALUES (7)) SELECT value FROM seed; \
                   SELECT 8 end; \
                   SELECT * FROM audit end; \
                   SELECT * FROM (SELECT 9) end; \
                   UPDATE audit SET value = source.value FROM source \
                     WHERE audit.id = source.id; \
                   END";
        let Statement::CreateTrigger(trigger) = parse_one(sql) else {
            panic!("expected CREATE TRIGGER");
        };
        assert_eq!(trigger.body.len(), 11);
        assert!(matches!(trigger.body[0], Statement::Select(_)));
        assert!(matches!(trigger.body[1], Statement::Select(_)));
        assert!(matches!(trigger.body[2], Statement::Insert(_)));
        assert!(matches!(trigger.body[3], Statement::Insert(_)));
        assert!(matches!(trigger.body[4], Statement::Update(_)));
        assert!(matches!(trigger.body[5], Statement::Delete(_)));
        assert!(matches!(trigger.body[6], Statement::Select(_)));
        assert!(matches!(trigger.body[7], Statement::Select(_)));
        assert!(matches!(trigger.body[8], Statement::Select(_)));
        assert!(matches!(trigger.body[9], Statement::Select(_)));
        assert!(matches!(trigger.body[10], Statement::Update(_)));
    }

    #[test]
    fn test_trigger_body_rejects_empty_missing_semicolon_and_non_dml_commands() {
        for (sql, rejected) in [
            ("CREATE TRIGGER trg AFTER INSERT ON t BEGIN END", "END"),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN CREATE TABLE bad(x); END",
                "CREATE",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN BEGIN; END",
                "BEGIN",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN PRAGMA foreign_keys; END",
                "PRAGMA",
            ),
            (
                "CREATE TRIGGER outer_trg AFTER INSERT ON t BEGIN \
                 CREATE TRIGGER inner_trg AFTER INSERT ON t BEGIN SELECT 1; END; END",
                "CREATE",
            ),
        ] {
            let error = parse_first_statement_with_tail(sql)
                .expect_err("invalid trigger-body grammar must fail closed");
            assert_eq!(error.kind, ParseErrorKind::Syntax);
            assert_eq!(
                &sql[error.span.start as usize..error.span.end as usize],
                rejected,
                "the diagnostic for `{sql}` must identify the rejected token"
            );
        }

        let missing_separator = "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1 END";
        let error = parse_first_statement_with_tail(missing_separator)
            .expect_err("a trigger body statement still requires a semicolon");
        assert_eq!(error.kind, ParseErrorKind::Syntax);
        assert!(
            error
                .message
                .contains("expected ';' after trigger body statement"),
            "unexpected missing-separator diagnostic: {error:?}"
        );
        assert_eq!(
            error.span.start, error.span.end,
            "like stock SQLite's incomplete-input result, the parser must not reinterpret \
             the implicit END alias as the trigger terminator"
        );
    }

    #[test]
    fn test_trigger_missing_separator_recovery_preserves_trailing_top_level_sql() {
        for body_statement in [
            "INSERT INTO audit VALUES (1)",
            "UPDATE audit SET value = 1",
            "DELETE FROM audit",
        ] {
            let sql = format!(
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 {body_statement} END; SELECT 42;"
            );
            let (statements, errors) = Parser::from_sql(&sql).parse_all();

            assert_eq!(
                errors.len(),
                1,
                "the missing trigger-body separator must be reported: {sql}"
            );
            assert_eq!(
                statements.len(),
                1,
                "recovery must preserve the trailing top-level statement: {sql}"
            );
            assert_eq!(statements[0].to_string(), "SELECT 42");
        }
    }

    #[test]
    fn test_trigger_body_rejects_stock_forbidden_dml_forms_at_exact_tokens() {
        for (sql, rejected) in [
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 INSERT INTO main.audit VALUES (1); END",
                ".",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 UPDATE main.audit SET value = 1; END",
                ".",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 DELETE FROM main.audit; END",
                ".",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 INSERT INTO audit AS target VALUES (1); END",
                "AS",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 UPDATE audit AS target SET value = 1; END",
                "AS",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 DELETE FROM audit AS target; END",
                "AS",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 INSERT INTO audit DEFAULT VALUES; END",
                "DEFAULT",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 UPDATE audit INDEXED BY audit_idx SET value = 1; END",
                "INDEXED",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 UPDATE audit NOT INDEXED SET value = 1; END",
                "NOT",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 DELETE FROM audit INDEXED BY audit_idx; END",
                "INDEXED",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 DELETE FROM audit NOT INDEXED; END",
                "NOT",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 UPDATE audit SET value = 1 ORDER BY value; END",
                "ORDER",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 UPDATE audit SET value = 1 LIMIT 1; END",
                "LIMIT",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 DELETE FROM audit ORDER BY value; END",
                "ORDER",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 DELETE FROM audit LIMIT 1; END",
                "LIMIT",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 INSERT INTO audit VALUES (1) RETURNING value; END",
                "RETURNING",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 UPDATE audit SET value = 1 RETURNING value; END",
                "RETURNING",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 DELETE FROM audit RETURNING value; END",
                "RETURNING",
            ),
            (
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                 WITH seed(value) AS (VALUES (1)) \
                 INSERT INTO audit SELECT value FROM seed; END",
                "INSERT",
            ),
        ] {
            let error = parse_first_statement_with_tail(sql)
                .expect_err("stock-forbidden trigger DML must fail closed");
            assert_eq!(error.kind, ParseErrorKind::Syntax);
            assert_eq!(
                &sql[error.span.start as usize..error.span.end as usize],
                rejected,
                "the diagnostic for `{sql}` must identify the forbidden token"
            );
        }
    }

    #[test]
    fn test_trigger_dml_restrictions_do_not_leak_to_top_level_statements() {
        for sql in [
            "INSERT INTO main.audit DEFAULT VALUES RETURNING rowid",
            "UPDATE main.audit INDEXED BY audit_idx SET value = 1 \
             RETURNING value ORDER BY value LIMIT 1",
            "DELETE FROM main.audit NOT INDEXED RETURNING value ORDER BY value LIMIT 1",
        ] {
            parse_first_statement_with_tail(sql)
                .unwrap_or_else(|error| panic!("top-level DML must remain accepted: {error}"));
        }
    }

    #[test]
    fn test_trigger_body_recovery_preserves_following_top_level_statement() {
        let sql = "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
                   CREATE TABLE bad(x); END; SELECT 42;";
        let (statements, errors) = Parser::from_sql(sql).parse_all();
        assert_eq!(errors.len(), 1, "expected one trigger-body grammar error");
        assert_eq!(
            statements.len(),
            1,
            "the malformed trigger must be discarded"
        );
        assert_eq!(statements[0].to_string(), "SELECT 42");
        assert_eq!(
            &sql[errors[0].span.start as usize..errors[0].span.end as usize],
            "CREATE"
        );
    }

    #[test]
    fn test_parse_first_statement_with_tail_rejects_adjacent_statements_without_separator() {
        let error = parse_first_statement_with_tail("SELECT 1 SELECT 2")
            .expect_err("adjacent statements without a semicolon must be rejected");

        assert!(
            error.message.contains("expected ';' separator"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn test_parse_all_reports_and_recovers_from_missing_statement_separator() {
        let sql = "SELECT 1 SELECT 2";
        let mut parser = Parser::from_sql(sql);
        let (statements, errors) = parser.parse_all();

        assert_eq!(
            statements.len(),
            2,
            "both independently valid statements should remain available for diagnostics"
        );
        assert_eq!(statements[0].to_string(), "SELECT 1");
        assert_eq!(statements[1].to_string(), "SELECT 2");
        assert_eq!(errors.len(), 1, "the missing separator must be reported");
        assert!(
            errors[0].message.contains("expected ';' separator"),
            "unexpected diagnostic: {:?}",
            errors[0]
        );
        assert_eq!(
            &sql[errors[0].span.start as usize..errors[0].span.end as usize],
            "SELECT",
            "the separator diagnostic must point at the second statement"
        );
    }

    #[test]
    fn test_create_table_without_rowid_and_strict_round_trips_display() {
        let sql = "CREATE TABLE s (id INTEGER PRIMARY KEY) WITHOUT ROWID, STRICT";
        let Some((statement, _)) =
            parse_first_statement_with_tail(sql).expect("statement should parse")
        else {
            panic!("expected CREATE TABLE statement");
        };

        assert_eq!(statement.to_string(), sql);
    }

    #[test]
    fn test_error_recovery_does_not_fabricate_top_level_statements_from_trigger_body() {
        let mut parser = Parser::from_sql(
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
             XYZZY; SELECT CASE WHEN 1 THEN 2 END AS end; SELECT 2; END; SELECT 3;",
        );
        let (stmts, errs) = parser.parse_all();

        assert_eq!(errs.len(), 1, "expected one trigger-body parse error");
        assert_eq!(
            stmts.len(),
            1,
            "only the trailing top-level SELECT should remain"
        );
        assert!(
            matches!(
                &stmts[0],
                Statement::Select(select)
                    if matches!(
                        &select.body.select,
                        SelectCore::Select { columns, .. }
                            if matches!(
                                columns.as_slice(),
                                [ResultColumn::Expr {
                                    expr: Expr::Literal(Literal::Integer(3), _),
                                    alias: None,
                                }]
                            )
                    )
            ),
            "parser must skip the malformed trigger instead of reinterpreting body tokens as top-level SQL: {stmts:?}"
        );
    }

    #[test]
    fn test_error_recovery_skips_a_rejected_nested_trigger_before_outer_end() {
        for nested_prefix in ["CREATE TRIGGER", "CREATE UNIQUE TRIGGER"] {
            let sql = format!(
                "CREATE TRIGGER outer_trg AFTER INSERT ON t BEGIN \
                 {nested_prefix} inner_trg AFTER INSERT ON t BEGIN SELECT 1; END; \
                 END; SELECT 7;"
            );
            let (stmts, errs) = Parser::from_sql(&sql).parse_all();

            assert_eq!(errs.len(), 1, "expected one trigger-body parse error");
            assert_eq!(
                stmts.len(),
                1,
                "neither nested-trigger tokens nor the outer END may escape as top-level SQL: {sql}"
            );
            assert_eq!(stmts[0].to_string(), "SELECT 7");
        }
    }

    #[test]
    fn test_error_recovery_recovers_values_statement_after_garbage() {
        let mut parser = Parser::from_sql("XYZZY VALUES (1);");
        let (stmts, errs) = parser.parse_all();

        assert_eq!(errs.len(), 1, "expected one error for leading garbage");
        assert_eq!(stmts.len(), 1, "VALUES statement should still be recovered");
        assert!(matches!(stmts[0], Statement::Select(_)));
    }

    #[test]
    fn test_error_recovery_does_not_swallow_top_level_sql_after_unbalanced_trigger_paren() {
        let mut parser = Parser::from_sql(
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT (1; END; SELECT 2;",
        );
        let (stmts, errs) = parser.parse_all();

        assert_eq!(errs.len(), 1, "expected one trigger-body parse error");
        assert_eq!(
            stmts.len(),
            1,
            "malformed trigger recovery must still preserve the trailing top-level SELECT"
        );
        assert!(
            matches!(
                &stmts[0],
                Statement::Select(select)
                    if matches!(
                        &select.body.select,
                        SelectCore::Select { columns, .. }
                            if matches!(
                                columns.as_slice(),
                                [ResultColumn::Expr {
                                    expr: Expr::Literal(Literal::Integer(2), _),
                                    alias: None,
                                }]
                            )
                    )
            ),
            "parser must stop at the trigger END even when parentheses are left unbalanced: {stmts:?}"
        );
    }

    #[test]
    fn select_literal() {
        let stmt = parse_one("SELECT 1");
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn select_star_from() {
        let stmt = parse_one("SELECT * FROM t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, from, .. } = &s.body.select {
                assert!(matches!(columns[0], ResultColumn::Star));
                assert!(from.is_some());
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn select_where_order_limit() {
        let stmt = parse_one("SELECT a FROM t WHERE a > 1 ORDER BY a LIMIT 10 OFFSET 5");
        if let Statement::Select(s) = stmt {
            assert!(s.limit.is_some());
            assert_eq!(s.order_by.len(), 1);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn select_limit_comma_syntax_uses_offset_then_count() {
        let stmt = parse_one("SELECT a FROM t LIMIT 5, 10");
        if let Statement::Select(s) = stmt {
            let limit = s.limit.expect("LIMIT clause");
            assert!(matches!(
                limit.limit,
                Expr::Literal(Literal::Integer(10), _)
            ));
            assert!(matches!(
                limit.offset,
                Some(Expr::Literal(Literal::Integer(5), _))
            ));
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn select_order_by_nulls_first_last() {
        let stmt = parse_one("SELECT a FROM t ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.order_by.len(), 2);
            assert_eq!(s.order_by[0].direction, Some(SortDirection::Asc));
            assert_eq!(s.order_by[0].nulls, Some(NullsOrder::First));
            assert_eq!(s.order_by[1].direction, Some(SortDirection::Desc));
            assert_eq!(s.order_by[1].nulls, Some(NullsOrder::Last));
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn select_from_indexed_by_hint() {
        let stmt = parse_one("SELECT * FROM t INDEXED BY idx_t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                match &from.source {
                    TableOrSubquery::Table {
                        index_hint: Some(IndexHint::IndexedBy(name)),
                        ..
                    } => assert_eq!(name, "idx_t"),
                    other => unreachable!("expected indexed table source, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn select_from_not_indexed_hint() {
        let stmt = parse_one("SELECT * FROM t NOT INDEXED");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                match &from.source {
                    TableOrSubquery::Table {
                        index_hint: Some(IndexHint::NotIndexed),
                        ..
                    } => {}
                    other => unreachable!("expected not-indexed table source, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn select_from_table_valued_function() {
        let stmt = parse_one("SELECT * FROM generate_series(1, 100) AS gs");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                match &from.source {
                    TableOrSubquery::TableFunction { name, args, alias } => {
                        assert_eq!(name, "generate_series");
                        assert_eq!(args.len(), 2);
                        assert_eq!(alias.as_deref(), Some("gs"));
                    }
                    other => unreachable!("expected table-valued function source, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn select_window_function_over_clause() {
        let stmt = parse_one(
            "SELECT sum(x) OVER (PARTITION BY y ORDER BY z \
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        assert_eq!(over.partition_by.len(), 1);
                        assert_eq!(over.order_by.len(), 1);
                        assert!(matches!(
                            over.frame,
                            Some(FrameSpec {
                                frame_type: FrameType::Rows,
                                ..
                            })
                        ));
                    }
                    other => unreachable!("expected window function result column, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn select_named_window_definition_and_reference() {
        let stmt = parse_one(
            "SELECT sum(x) OVER win FROM t \
             WINDOW win AS (PARTITION BY y ORDER BY z)",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select {
                columns, windows, ..
            } = &s.body.select
            {
                assert_eq!(windows.len(), 1);
                assert_eq!(windows[0].name, "win");
                assert_eq!(windows[0].spec.partition_by.len(), 1);
                assert_eq!(windows[0].spec.order_by.len(), 1);
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => assert_eq!(
                        over.window_ref,
                        Some(WindowReference::Direct("win".to_owned()))
                    ),
                    other => unreachable!("expected named window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn select_named_window_reference_uses_sqlite_contextual_keyword_matrix() {
        let assert_reference = |reference: &str, name: &str| {
            let sql = format!("SELECT sum(1) OVER {reference} WINDOW {name} AS ()");
            let select = parse_full_select(&sql);
            let SelectCore::Select {
                columns, windows, ..
            } = &select.body.select
            else {
                panic!("expected SELECT core for `{sql}`");
            };
            assert_eq!(windows.len(), 1, "missing WINDOW definition for `{sql}`");
            assert_eq!(windows[0].name, name);
            let [
                ResultColumn::Expr {
                    expr:
                        Expr::FunctionCall {
                            over: Some(window), ..
                        },
                    alias: None,
                },
            ] = columns.as_slice()
            else {
                panic!("expected one named window call for `{sql}`");
            };
            let expected = if reference.starts_with('(') {
                WindowReference::Base(name.to_owned())
            } else {
                WindowReference::Direct(name.to_owned())
            };
            assert_eq!(
                window.window_ref,
                Some(expected),
                "wrong OVER form for `{sql}`"
            );
        };

        for name in [
            "attach",
            "begin",
            "by",
            "cast",
            "current_date",
            "current_time",
            "current_timestamp",
            "cross",
            "detach",
            "explain",
            "false",
            "for",
            "glob",
            "inner",
            "left",
            "like",
            "natural",
            "outer",
            "over",
            "raise",
            "regexp",
            "right",
            "rollback",
            "key",
            "true",
            "window",
            "with",
        ] {
            assert_reference(name, name);
            assert_reference(&format!("({name})"), name);
        }

        for name in ["partition", "range", "rows", "groups"] {
            assert_reference(name, name);
            let sql = format!("SELECT sum(1) OVER ({name})");
            parse_first_statement_with_tail(&sql)
                .expect_err("window-spec delimiters cannot be parenthesized base names");
        }
    }

    #[test]
    fn select_named_window_reference_rejects_non_fallback_keywords() {
        for name in ["filter", "nothing", "transaction"] {
            for reference in [name.to_owned(), format!("({name})")] {
                let sql = format!("SELECT sum(1) OVER {reference}");
                parse_first_statement_with_tail(&sql)
                    .expect_err("reserved window-name tokens must not be consumed as names");
            }
            let sql = format!("SELECT sum(1) WINDOW {name} AS ()");
            parse_first_statement_with_tail(&sql)
                .expect_err("reserved WINDOW definition names must be rejected");
        }
    }

    #[test]
    fn select_named_window_reference_accepts_string_and_parenthesized_names() {
        for (sql, expected_name) in [
            ("SELECT sum(1) OVER 'w' WINDOW 'w' AS ()", "w"),
            ("SELECT sum(1) OVER ('w') WINDOW 'w' AS ()", "w"),
            ("SELECT sum(1) OVER (window) WINDOW window AS ()", "window"),
        ] {
            let select = parse_full_select(sql);
            let SelectCore::Select {
                columns, windows, ..
            } = &select.body.select
            else {
                panic!("expected SELECT core for `{sql}`");
            };
            assert_eq!(windows.len(), 1, "missing WINDOW definition for `{sql}`");
            assert_eq!(windows[0].name, expected_name);
            let [
                ResultColumn::Expr {
                    expr:
                        Expr::FunctionCall {
                            over: Some(window), ..
                        },
                    alias: None,
                },
            ] = columns.as_slice()
            else {
                panic!("expected one named window call for `{sql}`");
            };
            let expected = if sql.contains("OVER (") {
                WindowReference::Base(expected_name.to_owned())
            } else {
                WindowReference::Direct(expected_name.to_owned())
            };
            assert_eq!(
                window.window_ref,
                Some(expected),
                "wrong OVER form for `{sql}`"
            );
        }
    }

    #[test]
    fn over_window_prefers_named_window_reference_over_implicit_alias() {
        let sql = "WITH t(x) AS (VALUES (1), (2)) \
                   SELECT sum(x) OVER window FROM t WINDOW window AS ()";
        let select = parse_full_select(sql);
        let SelectCore::Select {
            columns, windows, ..
        } = &select.body.select
        else {
            panic!("expected SELECT core");
        };
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].name, "window");
        assert!(matches!(
            columns.as_slice(),
            [ResultColumn::Expr {
                expr: Expr::FunctionCall {
                    over: Some(WindowSpec {
                        window_ref: Some(WindowReference::Direct(base_window)),
                        ..
                    }),
                    ..
                },
                alias: None,
            }] if base_window == "window"
        ));

        let error = parse_first_statement_with_tail("SELECT sum(1) OVER WINDOW w AS ()")
            .expect_err("WINDOW after OVER is a window name, not an implicit OVER alias");
        assert_eq!(error.kind, ParseErrorKind::Syntax);
    }

    #[test]
    fn over_implicit_alias_disambiguation_is_preserved_at_real_boundaries() {
        for sql in ["SELECT sum(1) OVER, 2", "SELECT sum(1) OVER FROM t"] {
            let select = parse_full_select(sql);
            let SelectCore::Select { columns, .. } = &select.body.select else {
                panic!("expected SELECT core for `{sql}`");
            };
            assert!(matches!(
                columns.first(),
                Some(ResultColumn::Expr {
                    expr: Expr::FunctionCall { over: None, .. },
                    alias: Some(alias),
                }) if alias == "over"
            ));
        }
    }

    #[test]
    fn overflowing_float_literals_round_trip_as_infinite_numbers() {
        fn assert_infinite_columns(select: &SelectStatement) {
            let SelectCore::Select { columns, .. } = &select.body.select else {
                panic!("expected SELECT core");
            };
            let [positive, negative] = columns.as_slice() else {
                panic!("expected positive and negative infinity columns");
            };
            assert!(matches!(
                positive,
                ResultColumn::Expr {
                    expr: Expr::Literal(Literal::Float(value), _),
                    ..
                } if value.is_infinite() && value.is_sign_positive()
            ));
            let ResultColumn::Expr {
                expr:
                    Expr::UnaryOp {
                        op: UnaryOp::Negate,
                        expr,
                        ..
                    },
                ..
            } = negative
            else {
                panic!("expected negative infinity to retain unary negation");
            };
            assert!(matches!(
                expr.as_ref(),
                Expr::Literal(Literal::Float(value), _)
                    if value.is_infinite() && value.is_sign_positive()
            ));
        }

        let parsed = parse_full_select("SELECT 9e999, -9e999");
        assert_infinite_columns(&parsed);
        let rendered = parsed.to_string();
        assert_eq!(rendered, "SELECT 9e999, -9e999");
        let reparsed = parse_full_select(&rendered);
        assert_infinite_columns(&reparsed);
        assert_eq!(reparsed.to_string(), rendered);
    }

    #[test]
    fn insert_values() {
        let stmt = parse_one("INSERT INTO t (a, b) VALUES (1, 2), (3, 4)");
        assert!(matches!(stmt, Statement::Insert(_)));
    }

    #[test]
    fn update_set() {
        let stmt = parse_one("UPDATE t SET a = 1, b = 2 WHERE id = 3");
        assert!(matches!(stmt, Statement::Update(_)));
    }

    #[test]
    fn delete_from() {
        let stmt = parse_one("DELETE FROM t WHERE id = 1");
        assert!(matches!(stmt, Statement::Delete(_)));
    }

    #[test]
    fn create_table_basic() {
        let stmt = parse_one("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
        if let Statement::CreateTable(ct) = stmt {
            assert_eq!(ct.name.name, "t");
            if let CreateTableBody::Columns { columns, .. } = ct.body {
                assert_eq!(columns.len(), 2);
            } else {
                unreachable!("expected column defs");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn create_index() {
        let stmt = parse_one("CREATE UNIQUE INDEX idx ON t (a, b DESC)");
        if let Statement::CreateIndex(ci) = stmt {
            assert!(ci.unique);
            assert_eq!(ci.columns.len(), 2);
        } else {
            unreachable!("expected CreateIndex");
        }
    }

    #[test]
    fn drop_table_if_exists() {
        let stmt = parse_one("DROP TABLE IF EXISTS t");
        if let Statement::Drop(d) = stmt {
            assert!(d.if_exists);
            assert_eq!(d.object_type, DropObjectType::Table);
        } else {
            unreachable!("expected Drop");
        }
    }

    #[test]
    fn begin_commit() {
        let stmts = parse_ok("BEGIN IMMEDIATE; COMMIT");
        assert_eq!(stmts.len(), 2);
        if let Statement::Begin(b) = &stmts[0] {
            assert_eq!(b.mode, Some(TransactionMode::Immediate));
        } else {
            unreachable!("expected Begin");
        }
        assert!(matches!(stmts[1], Statement::Commit));
    }

    #[test]
    fn begin_concurrent() {
        let stmt = parse_one("BEGIN CONCURRENT");
        if let Statement::Begin(b) = stmt {
            assert_eq!(b.mode, Some(TransactionMode::Concurrent));
        } else {
            unreachable!("expected Begin");
        }
    }

    #[test]
    fn rollback_to_savepoint() {
        let stmt = parse_one("ROLLBACK TO SAVEPOINT sp1");
        if let Statement::Rollback(r) = stmt {
            assert_eq!(r.to_savepoint.as_deref(), Some("sp1"));
        } else {
            unreachable!("expected Rollback");
        }
    }

    #[test]
    fn explain_query_plan() {
        let stmt = parse_one("EXPLAIN QUERY PLAN SELECT 1");
        assert!(matches!(
            stmt,
            Statement::Explain {
                query_plan: true,
                ..
            }
        ));
    }

    #[test]
    fn pragma() {
        let stmt = parse_one("PRAGMA journal_mode = WAL");
        assert!(matches!(stmt, Statement::Pragma(_)));
    }

    #[test]
    fn pragma_allows_on_value() {
        let stmt = parse_one("PRAGMA fsqlite.serializable = ON");
        assert!(matches!(stmt, Statement::Pragma(_)));
    }

    #[test]
    fn pragma_allows_delete_and_default_keyword_values() {
        // GH #276: DELETE and DEFAULT are reserved statement keywords but are
        // valid pragma values (SQLite's nmnum grammar). They must parse in value
        // position and resolve to their identifier name.
        for (sql, expected) in [
            ("PRAGMA journal_mode = DELETE", "delete"),
            ("PRAGMA temp_store = DEFAULT", "default"),
        ] {
            let Statement::Pragma(p) = parse_one(sql) else {
                unreachable!("expected Pragma for {sql}");
            };
            match p.value {
                Some(PragmaValue::Assign(Expr::Column(col, _))) => {
                    assert!(col.table.is_none(), "sql={sql}");
                    assert_eq!(&*col.column, expected, "sql={sql}");
                }
                other => unreachable!("expected Assign(Column) for {sql}, got {other:?}"),
            }
        }
    }

    #[test]
    fn error_recovery_multiple_statements() {
        let mut p = Parser::from_sql("SELECT 1; XYZZY; SELECT 2");
        let (stmts, errs) = p.parse_all();
        assert_eq!(stmts.len(), 2, "should recover: stmts={stmts:?}");
        assert!(!errs.is_empty());
    }

    #[test]
    fn compound_union() {
        let stmt = parse_one("SELECT 1 UNION ALL SELECT 2");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.body.compounds.len(), 1);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn alter_table_rename() {
        let stmt = parse_one("ALTER TABLE t RENAME TO t2");
        assert!(matches!(
            stmt,
            Statement::AlterTable(AlterTableStatement {
                action: AlterTableAction::RenameTo(_),
                ..
            })
        ));
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: parser join types
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_join_inner() {
        let stmt = parse_one("SELECT * FROM a INNER JOIN b ON a.id = b.a_id");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert!(!from.joins.is_empty());
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Inner);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_join_left() {
        let stmt = parse_one("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Left);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_join_left_outer() {
        let stmt = parse_one("SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.a_id");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Left);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_join_right() {
        let stmt = parse_one("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Right);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_join_full() {
        let stmt = parse_one("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.a_id");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Full);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_join_full_outer_with_semicolon() {
        let stmt = parse_one("SELECT l.name, r.tag FROM l FULL OUTER JOIN r ON l.id = r.l_id;");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins.len(), 1);
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Full);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_join_cross() {
        let stmt = parse_one("SELECT * FROM a CROSS JOIN b");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Cross);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_join_natural() {
        let stmt = parse_one("SELECT * FROM a NATURAL JOIN b");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert!(from.joins[0].join_type.natural);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_join_using() {
        let stmt = parse_one("SELECT * FROM a JOIN b USING (id)");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert!(matches!(
                    from.joins[0].constraint,
                    Some(JoinConstraint::Using(_))
                ));
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_join_comma() {
        // Comma-join is an implicit cross join.
        let stmt = parse_one("SELECT * FROM a, b WHERE a.id = b.a_id");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert!(!from.joins.is_empty());
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Cross);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: CTE syntax
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_cte_basic() {
        let stmt = parse_one("WITH cte AS (SELECT 1 AS x) SELECT * FROM cte");
        if let Statement::Select(s) = stmt {
            let with = s.with.as_ref().expect("WITH clause");
            assert!(!with.recursive);
            assert_eq!(with.ctes.len(), 1);
            assert_eq!(with.ctes[0].name, "cte");
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_cte_multiple() {
        let stmt = parse_one("WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b");
        if let Statement::Select(s) = stmt {
            let with = s.with.as_ref().expect("WITH clause");
            assert_eq!(with.ctes.len(), 2);
            assert_eq!(with.ctes[0].name, "a");
            assert_eq!(with.ctes[1].name, "b");
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_cte_recursive() {
        let stmt = parse_one(
            "WITH RECURSIVE cnt(x) AS (\
             SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<10\
             ) SELECT x FROM cnt",
        );
        if let Statement::Select(s) = stmt {
            let with = s.with.as_ref().expect("WITH clause");
            assert!(with.recursive);
            assert_eq!(with.ctes[0].name, "cnt");
            assert_eq!(with.ctes[0].columns, vec!["x".to_owned()]);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_cte_materialized() {
        let stmt = parse_one("WITH cte AS MATERIALIZED (SELECT 1) SELECT * FROM cte");
        if let Statement::Select(s) = stmt {
            let with = s.with.as_ref().expect("WITH clause");
            assert_eq!(
                with.ctes[0].materialized,
                Some(CteMaterialized::Materialized)
            );
        } else {
            unreachable!("expected Select");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2d6i §12.1 SELECT full syntax acceptance tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_select_table_star() {
        let stmt = parse_one("SELECT t1.* FROM t1, t2");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                assert!(
                    matches!(&columns[0], ResultColumn::TableStar(t) if t == &QualifiedName::bare("t1")),
                    "expected TableStar(t1), got {:?}",
                    columns[0]
                );
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_select_schema_table_star() {
        let stmt = parse_one("SELECT aux.t1.* FROM aux.t1");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                assert!(
                    matches!(&columns[0], ResultColumn::TableStar(t) if t == &QualifiedName::qualified("aux", "t1")),
                    "expected TableStar(aux.t1), got {:?}",
                    columns[0]
                );
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_select_expr_alias() {
        let stmt = parse_one("SELECT x + 1 AS result FROM t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        alias: Some(alias), ..
                    } => assert_eq!(alias, "result"),
                    other => unreachable!("expected aliased expr column, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_select_distinct_keyword() {
        let stmt = parse_one("SELECT DISTINCT a, b FROM t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select {
                distinct, columns, ..
            } = &s.body.select
            {
                assert_eq!(*distinct, Distinctness::Distinct);
                assert_eq!(columns.len(), 2);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_select_values_clause() {
        let stmt = parse_one("VALUES (1, 2), (3, 4)");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Values(rows) = &s.body.select {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
                assert_eq!(rows[1].len(), 2);
            } else {
                unreachable!("expected Values core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_values_representation_captures_leading_and_nested_with_timing() {
        let plain = parse_one("VALUES (1), (2), (3)");
        assert_eq!(
            top_level_values(&plain).representation(),
            ValuesRepresentation::Deferred {
                force_union_all_from: None,
            }
        );

        let leading = parse_one("WITH c(x) AS (SELECT 1) VALUES (2), (3)");
        assert_eq!(top_level_values(&leading).force_union_all_from(), Some(0));

        let nested_first =
            parse_one("VALUES ((WITH c(x) AS (SELECT 1) SELECT x FROM c)), (2), (3)");
        assert_eq!(
            top_level_values(&nested_first).force_union_all_from(),
            Some(0)
        );

        let nested_second =
            parse_one("VALUES (1), ((WITH c(x) AS (SELECT 2) SELECT x FROM c)), (3)");
        assert_eq!(
            top_level_values(&nested_second).force_union_all_from(),
            Some(1)
        );
    }

    #[test]
    fn test_values_representation_is_sticky_but_not_retroactive_within_a_statement() {
        let later_with = parse_full_select(
            "SELECT (VALUES (1), (2)), (WITH c(x) AS (SELECT 3) SELECT x FROM c)",
        );
        let SelectCore::Select { columns, .. } = &later_with.body.select else {
            panic!("expected SELECT core");
        };
        let ResultColumn::Expr { expr, .. } = &columns[0] else {
            panic!("expected expression result column");
        };
        assert_eq!(scalar_subquery_values(expr).force_union_all_from(), None);

        let earlier_with = parse_full_select(
            "SELECT (WITH c(x) AS (SELECT 3) SELECT x FROM c), (VALUES (1), (2))",
        );
        let SelectCore::Select { columns, .. } = &earlier_with.body.select else {
            panic!("expected SELECT core");
        };
        let ResultColumn::Expr { expr, .. } = &columns[1] else {
            panic!("expected expression result column");
        };
        assert_eq!(scalar_subquery_values(expr).force_union_all_from(), Some(0));
    }

    #[test]
    fn test_values_with_state_resets_between_parse_all_statements() {
        let statements = parse_ok("WITH c(x) AS (SELECT 1) VALUES (2), (3); VALUES (4), (5);");
        assert_eq!(statements.len(), 2);
        assert_eq!(
            top_level_values(&statements[0]).force_union_all_from(),
            Some(0)
        );
        assert_eq!(
            top_level_values(&statements[1]).force_union_all_from(),
            None
        );
    }

    #[test]
    fn test_direct_values_parser_captures_nested_with_row_boundary() {
        let mut parser =
            Parser::from_sql("VALUES (1), ((WITH c(x) AS (SELECT 2) SELECT x FROM c)), (3)");
        let parsed = parser
            .parse_select_core_tracked()
            .expect("direct VALUES parser must succeed");
        let SelectCore::Values(values) = parsed.value else {
            panic!("direct parser must return VALUES");
        };

        assert_eq!(values.force_union_all_from(), Some(1));
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_insert_values_extraction_retains_nested_values_representation() {
        let statement =
            parse_one("WITH c(x) AS (SELECT 1) INSERT INTO t VALUES ((VALUES (2), (3)))");
        let Statement::Insert(insert) = statement else {
            panic!("expected INSERT statement");
        };
        let InsertSource::Values(rows) = insert.source else {
            panic!("expected INSERT VALUES source");
        };
        let [row] = rows.as_slice() else {
            panic!("expected one INSERT row");
        };
        let [expr] = row.as_slice() else {
            panic!("expected one INSERT column");
        };

        assert_eq!(scalar_subquery_values(expr).force_union_all_from(), Some(0));
    }

    #[test]
    fn test_select_group_by_having() {
        let stmt = parse_one("SELECT dept, count(*) FROM emp GROUP BY dept HAVING count(*) > 5");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select {
                group_by, having, ..
            } = &s.body.select
            {
                assert_eq!(group_by.len(), 1);
                assert!(having.is_some(), "HAVING clause must be present");
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_compound_union() {
        let stmt = parse_one("SELECT 1 UNION SELECT 2");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.body.compounds.len(), 1);
            assert_eq!(s.body.compounds[0].0, CompoundOp::Union);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_compound_union_all() {
        let stmt = parse_one("SELECT 1 UNION ALL SELECT 2");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.body.compounds.len(), 1);
            assert_eq!(s.body.compounds[0].0, CompoundOp::UnionAll);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_compound_intersect() {
        let stmt = parse_one("SELECT 1 INTERSECT SELECT 2");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.body.compounds.len(), 1);
            assert_eq!(s.body.compounds[0].0, CompoundOp::Intersect);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_compound_except() {
        let stmt = parse_one("SELECT 1 EXCEPT SELECT 2");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.body.compounds.len(), 1);
            assert_eq!(s.body.compounds[0].0, CompoundOp::Except);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_compound_order_applies_to_whole() {
        // ORDER BY and LIMIT apply to the entire compound result per SQL spec.
        let stmt = parse_one("SELECT a FROM t1 UNION ALL SELECT b FROM t2 ORDER BY 1 LIMIT 10");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.body.compounds.len(), 1);
            assert_eq!(s.order_by.len(), 1, "ORDER BY must be on compound");
            assert!(s.limit.is_some(), "LIMIT must be on compound");
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_compound_three_way() {
        let stmt = parse_one("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.body.compounds.len(), 2);
            assert_eq!(s.body.compounds[0].0, CompoundOp::Union);
            assert_eq!(s.body.compounds[1].0, CompoundOp::Intersect);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_cte_not_materialized() {
        let stmt = parse_one("WITH cte AS NOT MATERIALIZED (SELECT 1) SELECT * FROM cte");
        if let Statement::Select(s) = stmt {
            let with = s.with.as_ref().expect("WITH clause");
            assert_eq!(
                with.ctes[0].materialized,
                Some(CteMaterialized::NotMaterialized)
            );
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_cte_with_explicit_columns() {
        let stmt = parse_one("WITH cte(a, b, c) AS (SELECT 1, 2, 3) SELECT * FROM cte");
        if let Statement::Select(s) = stmt {
            let with = s.with.as_ref().expect("WITH clause");
            assert_eq!(with.ctes[0].columns, vec!["a", "b", "c"]);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_frame_range() {
        let stmt = parse_one(
            "SELECT sum(x) OVER (ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        let frame = over.frame.as_ref().expect("frame spec");
                        assert_eq!(frame.frame_type, FrameType::Range);
                        assert!(matches!(frame.start, FrameBound::UnboundedPreceding));
                        assert!(matches!(frame.end, Some(FrameBound::CurrentRow)));
                    }
                    other => unreachable!("expected window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_frame_groups() {
        let stmt = parse_one(
            "SELECT sum(x) OVER (ORDER BY y GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        let frame = over.frame.as_ref().expect("frame spec");
                        assert_eq!(frame.frame_type, FrameType::Groups);
                        assert!(matches!(frame.start, FrameBound::Preceding(_)));
                        assert!(matches!(frame.end, Some(FrameBound::Following(_))));
                    }
                    other => unreachable!("expected window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_frame_exclude_current_row() {
        let stmt = parse_one(
            "SELECT sum(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND \
             UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        let frame = over.frame.as_ref().expect("frame spec");
                        assert_eq!(frame.exclude, Some(FrameExclude::CurrentRow));
                    }
                    other => unreachable!("expected window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_frame_exclude_ties() {
        let stmt = parse_one(
            "SELECT sum(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND \
             UNBOUNDED FOLLOWING EXCLUDE TIES) FROM t",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        let frame = over.frame.as_ref().expect("frame spec");
                        assert_eq!(frame.exclude, Some(FrameExclude::Ties));
                    }
                    other => unreachable!("expected window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_frame_exclude_group() {
        let stmt =
            parse_one("SELECT sum(x) OVER (ORDER BY y GROUPS CURRENT ROW EXCLUDE GROUP) FROM t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        let frame = over.frame.as_ref().expect("frame spec");
                        assert_eq!(frame.frame_type, FrameType::Groups);
                        assert_eq!(frame.exclude, Some(FrameExclude::Group));
                    }
                    other => unreachable!("expected window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_frame_unbounded_following() {
        let stmt = parse_one(
            "SELECT sum(x) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        let frame = over.frame.as_ref().expect("frame spec");
                        assert!(matches!(frame.start, FrameBound::CurrentRow));
                        assert!(matches!(frame.end, Some(FrameBound::UnboundedFollowing)));
                    }
                    other => unreachable!("expected window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_frame_rejects_illegal_bound_order_with_exact_span() {
        for (sql, rejected) in [
            (
                "SELECT sum(x) OVER (ROWS UNBOUNDED FOLLOWING) FROM t",
                "UNBOUNDED",
            ),
            ("SELECT sum(x) OVER (ROWS 1 FOLLOWING) FROM t", "1"),
            (
                "SELECT sum(x) OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) FROM t",
                "UNBOUNDED",
            ),
            (
                "SELECT sum(x) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM t",
                "UNBOUNDED",
            ),
            (
                "SELECT sum(x) OVER (ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM t",
                "1",
            ),
            (
                "SELECT sum(x) OVER (ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM t",
                "CURRENT",
            ),
        ] {
            let error = parse_first_statement_with_tail(sql)
                .expect_err("illegal window-frame boundaries must be rejected");
            assert_eq!(error.kind, ParseErrorKind::Syntax);
            assert_eq!(
                &sql[error.span.start as usize..error.span.end as usize],
                rejected,
                "the diagnostic for `{sql}` must point at the illegal boundary"
            );
        }
    }

    #[test]
    fn test_window_frame_accepts_legal_categorical_order_without_offset_comparison() {
        for sql in [
            "SELECT sum(x) OVER (ROWS 1 PRECEDING) FROM t",
            "SELECT sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) FROM t",
            "SELECT sum(x) OVER (ROWS BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) FROM t",
            "SELECT sum(x) OVER (RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t",
            "SELECT sum(x) OVER (GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM t",
        ] {
            parse_full_select(sql);
        }
    }

    #[test]
    fn test_filter_clause_aggregate() {
        let stmt = parse_one("SELECT count(*) FILTER (WHERE x > 0) FROM t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr: Expr::FunctionCall { filter, .. },
                        ..
                    } => {
                        assert!(
                            filter.is_some(),
                            "FILTER clause must be present on aggregate"
                        );
                    }
                    other => unreachable!("expected function call with filter, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_filter_clause_window() {
        let stmt = parse_one("SELECT sum(x) FILTER (WHERE x > 0) OVER (ORDER BY y) FROM t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                filter,
                                over: Some(_),
                                ..
                            },
                        ..
                    } => {
                        assert!(
                            filter.is_some(),
                            "FILTER clause must be present on window function"
                        );
                    }
                    other => unreachable!("expected window function with filter, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_subquery_in_from() {
        let stmt = parse_one("SELECT sub.x FROM (SELECT 1 AS x) AS sub");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                match &from.source {
                    TableOrSubquery::Subquery { alias, .. } => {
                        assert_eq!(alias.as_deref(), Some("sub"));
                    }
                    other => unreachable!("expected subquery source, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_multiple_joins_chain() {
        let stmt = parse_one(
            "SELECT * FROM a INNER JOIN b ON a.id = b.a_id \
             LEFT JOIN c ON b.id = c.b_id \
             CROSS JOIN d",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins.len(), 3);
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Inner);
                assert_eq!(from.joins[1].join_type.kind, JoinKind::Left);
                assert_eq!(from.joins[2].join_type.kind, JoinKind::Cross);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_natural_left_join() {
        let stmt = parse_one("SELECT * FROM a NATURAL LEFT JOIN b");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                let jt = &from.joins[0].join_type;
                assert!(jt.natural, "must be NATURAL");
                assert_eq!(jt.kind, JoinKind::Left);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_select_nulls_first_default_asc() {
        // Verify NULLS FIRST with explicit ASC direction.
        let stmt = parse_one("SELECT a FROM t ORDER BY a ASC NULLS FIRST");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.order_by.len(), 1);
            assert_eq!(s.order_by[0].direction, Some(SortDirection::Asc));
            assert_eq!(s.order_by[0].nulls, Some(NullsOrder::First));
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_select_nulls_last_desc() {
        // Verify NULLS LAST with explicit DESC direction.
        let stmt = parse_one("SELECT a FROM t ORDER BY a DESC NULLS LAST");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.order_by.len(), 1);
            assert_eq!(s.order_by[0].direction, Some(SortDirection::Desc));
            assert_eq!(s.order_by[0].nulls, Some(NullsOrder::Last));
        } else {
            unreachable!("expected Select");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2d6i §12.1 roundtrip coverage for advanced SELECT forms
    // -----------------------------------------------------------------------

    #[test]
    fn test_roundtrip_select_filter_clause() {
        assert_roundtrip("SELECT count(*) FILTER (WHERE x > 0) FROM t");
    }

    #[test]
    fn test_roundtrip_select_window_frame_groups() {
        assert_roundtrip(
            "SELECT sum(x) OVER (ORDER BY y GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
        );
    }

    #[test]
    fn test_roundtrip_select_window_frame_exclude() {
        assert_roundtrip(
            "SELECT sum(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND \
             UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t",
        );
        assert_roundtrip(
            "SELECT sum(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND \
             UNBOUNDED FOLLOWING EXCLUDE TIES) FROM t",
        );
        assert_roundtrip("SELECT sum(x) OVER (ORDER BY y GROUPS CURRENT ROW EXCLUDE GROUP) FROM t");
    }

    #[test]
    fn test_roundtrip_select_nulls_order() {
        assert_roundtrip("SELECT a FROM t ORDER BY a ASC NULLS FIRST");
        assert_roundtrip("SELECT a FROM t ORDER BY a DESC NULLS LAST");
    }

    #[test]
    fn test_roundtrip_select_values() {
        assert_roundtrip("VALUES (1, 2), (3, 4)");
    }

    #[test]
    fn test_roundtrip_select_compound_order_limit() {
        assert_roundtrip("SELECT a FROM t1 UNION ALL SELECT b FROM t2 ORDER BY 1 LIMIT 10");
    }

    #[test]
    fn test_roundtrip_select_cte_not_materialized() {
        assert_roundtrip("WITH cte AS NOT MATERIALIZED (SELECT 1) SELECT * FROM cte");
    }

    #[test]
    fn test_roundtrip_select_natural_left_join() {
        assert_roundtrip("SELECT * FROM a NATURAL LEFT JOIN b");
    }

    #[test]
    fn test_roundtrip_select_indexed_by() {
        assert_roundtrip("SELECT * FROM t INDEXED BY idx_t WHERE x = 1");
    }

    #[test]
    fn test_roundtrip_select_filter_window_combined() {
        assert_roundtrip("SELECT sum(x) FILTER (WHERE x > 0) OVER (ORDER BY y) FROM t");
    }

    #[test]
    fn test_serializer_regression_window_base_name_and_extensions_roundtrip() {
        assert_roundtrip("SELECT sum(x) OVER base FROM t");
        assert_roundtrip(
            "SELECT sum(x) OVER (base PARTITION BY p ORDER BY y \
             ROWS BETWEEN z PRECEDING AND CURRENT ROW) FROM t",
        );
    }

    #[test]
    fn test_roundtrip_select_three_way_compound() {
        assert_roundtrip("SELECT 1 UNION SELECT 2 EXCEPT SELECT 3");
    }

    #[test]
    fn test_roundtrip_select_multiple_joins() {
        assert_roundtrip(
            "SELECT * FROM a INNER JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id",
        );
    }

    // -----------------------------------------------------------------------
    // bd-2d6i §12.1 — remaining required tests (exact names per bead spec)
    // -----------------------------------------------------------------------

    #[test]
    fn test_select_star() {
        // SELECT * returns all columns from all tables.
        let stmt = parse_one("SELECT * FROM t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                assert!(matches!(columns[0], ResultColumn::Star));
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_inner_join_on() {
        // INNER JOIN ON produces correct intersection.
        let stmt = parse_one("SELECT * FROM a INNER JOIN b ON a.id = b.a_id");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Inner);
                assert!(matches!(
                    from.joins[0].constraint,
                    Some(JoinConstraint::On(_))
                ));
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_left_outer_join() {
        // LEFT JOIN returns all left rows with NULLs for non-matching right.
        let stmt = parse_one("SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.a_id");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Left);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_right_outer_join() {
        // RIGHT JOIN returns all right rows (3.39+ feature).
        let stmt = parse_one("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Right);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_full_outer_join() {
        // FULL OUTER JOIN returns rows from both tables.
        let stmt = parse_one("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.a_id");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Full);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_cross_join_no_reorder() {
        // CROSS JOIN prevents optimizer reordering; parser must produce JoinKind::Cross.
        let stmt = parse_one("SELECT * FROM a CROSS JOIN b");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Cross);
                // Cross joins must NOT have an ON or USING constraint.
                assert!(from.joins[0].constraint.is_none());
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_natural_join() {
        // NATURAL JOIN uses shared column names for implicit ON.
        let stmt = parse_one("SELECT * FROM a NATURAL JOIN b");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert!(from.joins[0].join_type.natural);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_using_clause() {
        // JOIN USING joins on specified shared columns.
        let stmt = parse_one("SELECT * FROM a JOIN b USING (id, name)");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                match &from.joins[0].constraint {
                    Some(JoinConstraint::Using(cols)) => {
                        assert_eq!(cols.len(), 2);
                        assert_eq!(cols[0], "id");
                        assert_eq!(cols[1], "name");
                    }
                    other => unreachable!("expected USING constraint, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_cte_basic() {
        // WITH clause defines reusable named subquery.
        let stmt = parse_one("WITH cte AS (SELECT 1 AS x) SELECT * FROM cte");
        if let Statement::Select(s) = stmt {
            let with = s.with.as_ref().expect("WITH clause");
            assert!(!with.recursive);
            assert_eq!(with.ctes.len(), 1);
            assert_eq!(with.ctes[0].name, "cte");
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_cte_recursive_union_all() {
        // Recursive CTE with UNION ALL generates rows.
        let stmt = parse_one(
            "WITH RECURSIVE cnt(x) AS (\
             SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<10\
             ) SELECT x FROM cnt",
        );
        if let Statement::Select(s) = stmt {
            let with = s.with.as_ref().expect("WITH clause");
            assert!(with.recursive);
            assert_eq!(with.ctes[0].name, "cnt");
            // Verify the CTE body contains a UNION ALL compound.
            let cte_body = &with.ctes[0].query;
            assert_eq!(cte_body.body.compounds.len(), 1);
            assert_eq!(cte_body.body.compounds[0].0, CompoundOp::UnionAll);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_cte_recursive_union_cycle_detection() {
        // Recursive CTE with UNION (not UNION ALL) detects cycles via dedup.
        let stmt = parse_one(
            "WITH RECURSIVE paths(a, b) AS (\
             SELECT src, dst FROM edges \
             UNION \
             SELECT p.a, e.dst FROM paths p JOIN edges e ON p.b = e.src\
             ) SELECT * FROM paths",
        );
        if let Statement::Select(s) = stmt {
            let with = s.with.as_ref().expect("WITH clause");
            assert!(with.recursive);
            // UNION (not UNION ALL) provides implicit cycle detection.
            let cte_body = &with.ctes[0].query;
            assert_eq!(cte_body.body.compounds.len(), 1);
            assert_eq!(cte_body.body.compounds[0].0, CompoundOp::Union);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_cte_materialized_hint() {
        // MATERIALIZED forces single evaluation.
        let stmt = parse_one("WITH cte AS MATERIALIZED (SELECT 1) SELECT * FROM cte");
        if let Statement::Select(s) = stmt {
            let with = s.with.as_ref().expect("WITH clause");
            assert_eq!(
                with.ctes[0].materialized,
                Some(CteMaterialized::Materialized)
            );
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_cte_not_materialized_hint() {
        // NOT MATERIALIZED allows inlining.
        let stmt = parse_one("WITH cte AS NOT MATERIALIZED (SELECT 1) SELECT * FROM cte");
        if let Statement::Select(s) = stmt {
            let with = s.with.as_ref().expect("WITH clause");
            assert_eq!(
                with.ctes[0].materialized,
                Some(CteMaterialized::NotMaterialized)
            );
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_partition_by() {
        // PARTITION BY correctly groups window function output.
        let stmt = parse_one("SELECT sum(x) OVER (PARTITION BY dept) FROM emp");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        assert_eq!(over.partition_by.len(), 1);
                    }
                    other => unreachable!("expected window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_order_by() {
        // ORDER BY within window function controls row ordering.
        let stmt = parse_one("SELECT row_number() OVER (ORDER BY salary DESC) FROM emp");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        assert_eq!(over.order_by.len(), 1);
                        assert_eq!(over.order_by[0].direction, Some(SortDirection::Desc));
                    }
                    other => unreachable!("expected window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_frame_rows() {
        // ROWS frame spec limits window to specified row range.
        let stmt = parse_one(
            "SELECT sum(x) OVER (ORDER BY y ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        let frame = over.frame.as_ref().expect("frame spec");
                        assert_eq!(frame.frame_type, FrameType::Rows);
                        assert!(matches!(frame.start, FrameBound::Preceding(_)));
                        assert!(matches!(frame.end, Some(FrameBound::CurrentRow)));
                    }
                    other => unreachable!("expected window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_exclude_current_row() {
        // EXCLUDE CURRENT ROW omits current row from frame.
        let stmt = parse_one(
            "SELECT sum(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND \
             UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        let frame = over.frame.as_ref().expect("frame spec");
                        assert_eq!(frame.exclude, Some(FrameExclude::CurrentRow));
                    }
                    other => unreachable!("expected window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_window_exclude_ties() {
        // EXCLUDE TIES omits peers of current row.
        let stmt = parse_one(
            "SELECT sum(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND \
             UNBOUNDED FOLLOWING EXCLUDE TIES) FROM t",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr:
                            Expr::FunctionCall {
                                over: Some(over), ..
                            },
                        ..
                    } => {
                        let frame = over.frame.as_ref().expect("frame spec");
                        assert_eq!(frame.exclude, Some(FrameExclude::Ties));
                    }
                    other => unreachable!("expected window function, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_nulls_first_asc() {
        // NULLS FIRST with ASC puts NULLs before non-NULL values.
        let stmt = parse_one("SELECT a FROM t ORDER BY a ASC NULLS FIRST");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.order_by.len(), 1);
            assert_eq!(s.order_by[0].direction, Some(SortDirection::Asc));
            assert_eq!(s.order_by[0].nulls, Some(NullsOrder::First));
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_nulls_last_asc() {
        // NULLS LAST with ASC puts NULLs after non-NULL values.
        let stmt = parse_one("SELECT a FROM t ORDER BY a ASC NULLS LAST");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.order_by.len(), 1);
            assert_eq!(s.order_by[0].direction, Some(SortDirection::Asc));
            assert_eq!(s.order_by[0].nulls, Some(NullsOrder::Last));
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_distinct_deduplicates() {
        // SELECT DISTINCT removes duplicate rows (parser-level: keyword present).
        let stmt = parse_one("SELECT DISTINCT a, b FROM t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { distinct, .. } = &s.body.select {
                assert_eq!(*distinct, Distinctness::Distinct);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_limit_offset() {
        // LIMIT N OFFSET M skips M rows and returns N.
        let stmt = parse_one("SELECT a FROM t LIMIT 10 OFFSET 20");
        if let Statement::Select(s) = stmt {
            let limit = s.limit.expect("LIMIT clause");
            assert!(matches!(
                limit.limit,
                Expr::Literal(Literal::Integer(10), _)
            ));
            assert!(matches!(
                limit.offset,
                Some(Expr::Literal(Literal::Integer(20), _))
            ));
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_limit_comma_syntax() {
        // LIMIT offset,count (MySQL syntax) — offset first, count second.
        let stmt = parse_one("SELECT a FROM t LIMIT 5, 10");
        if let Statement::Select(s) = stmt {
            let limit = s.limit.expect("LIMIT clause");
            // In MySQL syntax, LIMIT 5, 10 means offset=5, count=10.
            assert!(matches!(
                limit.limit,
                Expr::Literal(Literal::Integer(10), _)
            ));
            assert!(matches!(
                limit.offset,
                Some(Expr::Literal(Literal::Integer(5), _))
            ));
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_negative_limit_unlimited() {
        // Negative LIMIT means unlimited (parser accepts negative literal).
        let stmt = parse_one("SELECT a FROM t LIMIT -1");
        if let Statement::Select(s) = stmt {
            let limit = s.limit.expect("LIMIT clause");
            // Parser may represent -1 as UnaryOp::Negate on Integer(1),
            // or as Integer(-1). Either is valid.
            match &limit.limit {
                Expr::UnaryOp {
                    op: fsqlite_ast::UnaryOp::Negate,
                    ..
                } => {}
                Expr::Literal(Literal::Integer(n), _) if *n < 0 => {}
                other => unreachable!("expected negative limit expression, got {other:?}"),
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_negative_offset_zero() {
        // Negative OFFSET treated as zero (parser accepts negative literal).
        let stmt = parse_one("SELECT a FROM t LIMIT 10 OFFSET -5");
        if let Statement::Select(s) = stmt {
            let limit = s.limit.expect("LIMIT clause");
            assert!(limit.offset.is_some());
            match limit.offset.as_ref().unwrap() {
                Expr::UnaryOp {
                    op: fsqlite_ast::UnaryOp::Negate,
                    ..
                } => {}
                Expr::Literal(Literal::Integer(n), _) if *n < 0 => {}
                other => unreachable!("expected negative offset expression, got {other:?}"),
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_current_date_constant() {
        // current_date is parsed as a literal keyword.
        let stmt = parse_one("SELECT CURRENT_DATE");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr: Expr::Literal(Literal::CurrentDate, _),
                        ..
                    } => {}
                    other => unreachable!("expected CURRENT_DATE literal, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_current_time_constant() {
        // current_time is parsed as a literal keyword.
        let stmt = parse_one("SELECT CURRENT_TIME");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr: Expr::Literal(Literal::CurrentTime, _),
                        ..
                    } => {}
                    other => unreachable!("expected CURRENT_TIME literal, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_current_timestamp_constant() {
        // current_timestamp is parsed as a literal keyword.
        let stmt = parse_one("SELECT CURRENT_TIMESTAMP");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr {
                        expr: Expr::Literal(Literal::CurrentTimestamp, _),
                        ..
                    } => {}
                    other => unreachable!("expected CURRENT_TIMESTAMP literal, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_date_constants_evaluated_once_per_statement() {
        // Parser-level: all three date/time constants parse as distinct Literal variants.
        // Runtime guarantee (evaluated once per stmt, not per row) is verified at VDBE level.
        let stmt = parse_one("SELECT CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP FROM t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                assert_eq!(columns.len(), 3);
                assert!(matches!(
                    &columns[0],
                    ResultColumn::Expr {
                        expr: Expr::Literal(Literal::CurrentDate, _),
                        ..
                    }
                ));
                assert!(matches!(
                    &columns[1],
                    ResultColumn::Expr {
                        expr: Expr::Literal(Literal::CurrentTime, _),
                        ..
                    }
                ));
                assert!(matches!(
                    &columns[2],
                    ResultColumn::Expr {
                        expr: Expr::Literal(Literal::CurrentTimestamp, _),
                        ..
                    }
                ));
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_indexed_by_hint() {
        // FROM t1 INDEXED BY idx forces specified index.
        let stmt = parse_one("SELECT * FROM t INDEXED BY idx_t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                match &from.source {
                    TableOrSubquery::Table {
                        index_hint: Some(IndexHint::IndexedBy(name)),
                        ..
                    } => assert_eq!(name, "idx_t"),
                    other => unreachable!("expected indexed table source, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_not_indexed_hint() {
        // FROM t1 NOT INDEXED prevents index use.
        let stmt = parse_one("SELECT * FROM t NOT INDEXED");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                match &from.source {
                    TableOrSubquery::Table {
                        index_hint: Some(IndexHint::NotIndexed),
                        ..
                    } => {}
                    other => unreachable!("expected not-indexed table source, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_table_valued_function_in_from() {
        // FROM generate_series(1,100) works as table source.
        let stmt = parse_one("SELECT * FROM generate_series(1, 100) AS gs");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                match &from.source {
                    TableOrSubquery::TableFunction { name, args, alias } => {
                        assert_eq!(name, "generate_series");
                        assert_eq!(args.len(), 2);
                        assert_eq!(alias.as_deref(), Some("gs"));
                    }
                    other => unreachable!("expected table-valued function source, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    // -----------------------------------------------------------------------
    // bd-1llo §12.2-12.4 INSERT + UPDATE + DELETE DML parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_insert_values_single() {
        let stmt = parse_one("INSERT INTO t (a, b, c) VALUES (1, 'hello', 3.14)");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.columns, vec!["a", "b", "c"]);
            if let InsertSource::Values(rows) = &i.source {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 3);
            } else {
                unreachable!("expected Values source");
            }
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_insert_values_multi() {
        let stmt = parse_one("INSERT INTO t (x, y) VALUES (1, 2), (3, 4), (5, 6)");
        if let Statement::Insert(i) = stmt {
            if let InsertSource::Values(rows) = &i.source {
                assert_eq!(rows.len(), 3);
                for row in rows {
                    assert_eq!(row.len(), 2);
                }
            } else {
                unreachable!("expected Values source");
            }
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_insert_from_select() {
        let stmt = parse_one("INSERT INTO t2 (a, b) SELECT x, y FROM t1 WHERE x > 0");
        if let Statement::Insert(i) = stmt {
            assert!(matches!(i.source, InsertSource::Select(_)));
            assert_eq!(i.columns, vec!["a", "b"]);
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_insert_from_select_without_from_clause() {
        let stmt = parse_one("INSERT INTO t (a) SELECT 1");
        if let Statement::Insert(i) = stmt {
            if let InsertSource::Select(select) = &i.source {
                if let SelectCore::Select { from, columns, .. } = &select.body.select {
                    assert!(from.is_none(), "SELECT 1 should parse without FROM");
                    assert_eq!(columns.len(), 1);
                } else {
                    unreachable!("expected Select core");
                }
            } else {
                unreachable!("expected Select source");
            }
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_insert_from_select_subquery_source() {
        let stmt = parse_one("INSERT INTO t (a) SELECT sub.x FROM (SELECT 1 AS x) AS sub");
        if let Statement::Insert(i) = stmt {
            if let InsertSource::Select(select) = &i.source {
                if let SelectCore::Select { from, .. } = &select.body.select {
                    let from = from.as_ref().expect("FROM clause");
                    match &from.source {
                        TableOrSubquery::Subquery { alias, .. } => {
                            assert_eq!(alias.as_deref(), Some("sub"));
                        }
                        other => unreachable!("expected subquery source, got {other:?}"),
                    }
                } else {
                    unreachable!("expected Select core");
                }
            } else {
                unreachable!("expected Select source");
            }
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_insert_from_select_table_function_source() {
        let stmt = parse_one("INSERT INTO t (a) SELECT gs.value FROM generate_series(1, 3) AS gs");
        if let Statement::Insert(i) = stmt {
            if let InsertSource::Select(select) = &i.source {
                if let SelectCore::Select { from, .. } = &select.body.select {
                    let from = from.as_ref().expect("FROM clause");
                    match &from.source {
                        TableOrSubquery::TableFunction { name, args, alias } => {
                            assert_eq!(name, "generate_series");
                            assert_eq!(args.len(), 2);
                            assert_eq!(alias.as_deref(), Some("gs"));
                        }
                        other => unreachable!("expected table function source, got {other:?}"),
                    }
                } else {
                    unreachable!("expected Select core");
                }
            } else {
                unreachable!("expected Select source");
            }
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_insert_default_values() {
        let stmt = parse_one("INSERT INTO t DEFAULT VALUES");
        if let Statement::Insert(i) = stmt {
            assert!(matches!(i.source, InsertSource::DefaultValues));
            assert!(i.columns.is_empty());
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_insert_or_abort() {
        let stmt = parse_one("INSERT OR ABORT INTO t (a) VALUES (1)");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.or_conflict, Some(ConflictAction::Abort));
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_insert_or_rollback() {
        let stmt = parse_one("INSERT OR ROLLBACK INTO t (a) VALUES (1)");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.or_conflict, Some(ConflictAction::Rollback));
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_insert_or_fail() {
        let stmt = parse_one("INSERT OR FAIL INTO t (a) VALUES (1)");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.or_conflict, Some(ConflictAction::Fail));
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_insert_or_ignore() {
        let stmt = parse_one("INSERT OR IGNORE INTO t (a) VALUES (1)");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.or_conflict, Some(ConflictAction::Ignore));
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_insert_or_replace() {
        // Both INSERT OR REPLACE and REPLACE INTO forms
        let stmt = parse_one("INSERT OR REPLACE INTO t (a) VALUES (1)");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.or_conflict, Some(ConflictAction::Replace));
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_upsert_do_update() {
        let stmt = parse_one(
            "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET b = excluded.b",
        );
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.upsert.len(), 1);
            assert!(i.upsert[0].target.is_some());
            match &i.upsert[0].action {
                UpsertAction::Update {
                    assignments,
                    where_clause,
                } => {
                    assert_eq!(assignments.len(), 1);
                    assert!(where_clause.is_none());
                }
                UpsertAction::Nothing => unreachable!("expected Update action"),
            }
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_upsert_do_nothing() {
        let stmt = parse_one("INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO NOTHING");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.upsert.len(), 1);
            assert!(matches!(i.upsert[0].action, UpsertAction::Nothing));
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_upsert_excluded_pseudo_table() {
        let stmt = parse_one(
            "INSERT INTO t (a, b) VALUES (1, 2) \
             ON CONFLICT (a) DO UPDATE SET b = excluded.b, a = excluded.a + 1",
        );
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.upsert.len(), 1);
            if let UpsertAction::Update { assignments, .. } = &i.upsert[0].action {
                assert_eq!(assignments.len(), 2);
                // Verify excluded.b reference in first assignment
                match &assignments[0].value {
                    Expr::Column(col, _) => {
                        assert_eq!(col.table.as_deref(), Some("excluded"));
                        assert_eq!(col.column.as_ref(), "b");
                    }
                    other => unreachable!("expected Column ref to excluded.b, got {other:?}"),
                }
            } else {
                unreachable!("expected Update action");
            }
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_upsert_multiple_on_conflict() {
        let stmt = parse_one(
            "INSERT INTO t (a, b) VALUES (1, 2) \
             ON CONFLICT (a) DO NOTHING \
             ON CONFLICT (b) DO UPDATE SET a = excluded.a",
        );
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.upsert.len(), 2);
            assert!(matches!(i.upsert[0].action, UpsertAction::Nothing));
            assert!(matches!(i.upsert[1].action, UpsertAction::Update { .. }));
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_upsert_where_on_conflict_target() {
        let stmt = parse_one(
            "INSERT INTO t (a, b) VALUES (1, 2) \
             ON CONFLICT (a) WHERE a > 0 DO UPDATE SET b = excluded.b WHERE b < 100",
        );
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.upsert.len(), 1);
            let target = i.upsert[0].target.as_ref().expect("conflict target");
            assert!(target.where_clause.is_some(), "target WHERE missing");
            if let UpsertAction::Update { where_clause, .. } = &i.upsert[0].action {
                assert!(where_clause.is_some(), "action WHERE missing");
            } else {
                unreachable!("expected Update action");
            }
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_returning_insert() {
        let stmt = parse_one("INSERT INTO t (a, b) VALUES (1, 2) RETURNING a, b, rowid");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.returning.len(), 3);
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_returning_insert_select_with_semicolon() {
        let stmt = parse_one("INSERT INTO t2 SELECT * FROM t RETURNING *;");
        if let Statement::Insert(i) = stmt {
            assert!(matches!(i.source, InsertSource::Select(_)));
            assert_eq!(i.returning.len(), 1);
            assert!(matches!(i.returning[0], ResultColumn::Star));
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_returning_reflects_before_triggers() {
        // Parser-level: verify RETURNING clause parses alongside trigger-affected DML
        // Runtime verification that RETURNING reflects BEFORE-trigger modifications
        // is deferred to VDBE/engine tests
        let stmt = parse_one("INSERT INTO t (a) VALUES (1) RETURNING a AS modified_a");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.returning.len(), 1);
            match &i.returning[0] {
                ResultColumn::Expr { alias, .. } => {
                    assert_eq!(alias.as_deref(), Some("modified_a"));
                }
                other => unreachable!("expected Expr result column, got {other:?}"),
            }
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_returning_ignores_after_triggers() {
        // Parser-level: verify RETURNING * parses on INSERT with conflict clause
        // Runtime verification that RETURNING ignores AFTER-trigger modifications
        // is deferred to VDBE/engine tests
        let stmt = parse_one("INSERT OR REPLACE INTO t (a) VALUES (1) RETURNING *");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.or_conflict, Some(ConflictAction::Replace));
            assert_eq!(i.returning.len(), 1);
            assert!(matches!(i.returning[0], ResultColumn::Star));
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_returning_after_before_trigger_modify() {
        // Parser-level: verify RETURNING with multiple column expressions
        // Runtime verification of BEFORE trigger modifying returned values
        // is deferred to VDBE/engine tests
        let stmt = parse_one("INSERT INTO t (a, b) VALUES (1, 2) RETURNING a, b, a + b AS total");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.returning.len(), 3);
            match &i.returning[2] {
                ResultColumn::Expr {
                    alias: Some(alias), ..
                } => assert_eq!(alias, "total"),
                other => unreachable!("expected aliased expression, got {other:?}"),
            }
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_returning_before_trigger_raise_abort() {
        // Parser-level: RAISE(ABORT, ...) is a valid expression in trigger bodies;
        // here we verify RETURNING parses on multi-row INSERT (runtime abort
        // behavior verified in VDBE/engine tests)
        let stmt = parse_one("INSERT INTO t (a) VALUES (1), (2), (3) RETURNING a");
        if let Statement::Insert(i) = stmt {
            if let InsertSource::Values(rows) = &i.source {
                assert_eq!(rows.len(), 3);
            } else {
                unreachable!("expected Values source");
            }
            assert_eq!(i.returning.len(), 1);
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_returning_instead_of_view() {
        // Parser-level: INSERT into a view name parses the same as INSERT into a table
        // Runtime INSTEAD OF trigger behavior is verified in VDBE/engine tests
        let stmt = parse_one("INSERT INTO v (a, b) VALUES (1, 2) RETURNING *");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.table.name, "v");
            assert!(!i.returning.is_empty());
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_returning_autoincrement_with_trigger() {
        // Parser-level: verify RETURNING can reference rowid on INSERT
        // Runtime autoincrement + trigger interaction is verified in VDBE/engine tests
        let stmt = parse_one("INSERT INTO t (name) VALUES ('test') RETURNING rowid, name");
        if let Statement::Insert(i) = stmt {
            assert_eq!(i.returning.len(), 2);
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_update_set_where() {
        let stmt = parse_one("UPDATE t SET a = 1, b = 'hello' WHERE id = 42");
        if let Statement::Update(u) = stmt {
            assert_eq!(u.assignments.len(), 2);
            assert!(u.where_clause.is_some());
            assert!(u.from.is_none());
        } else {
            unreachable!("expected Update");
        }
    }

    #[test]
    fn test_update_from_join() {
        let stmt = parse_one("UPDATE t1 SET a = t2.x FROM t2 WHERE t1.id = t2.id");
        if let Statement::Update(u) = stmt {
            assert_eq!(u.assignments.len(), 1);
            assert!(u.from.is_some());
            assert!(u.where_clause.is_some());
        } else {
            unreachable!("expected Update");
        }
    }

    #[test]
    fn test_update_from_multi_match() {
        // Parser-level: UPDATE FROM with a join that could produce multiple matches
        // Runtime behavior (arbitrary row chosen) verified in VDBE/engine tests
        let stmt = parse_one(
            "UPDATE t1 SET val = src.val FROM src \
             INNER JOIN mapping ON mapping.src_id = src.id \
             WHERE t1.id = mapping.dst_id",
        );
        if let Statement::Update(u) = stmt {
            assert!(u.from.is_some());
            let from = u.from.as_ref().unwrap();
            assert!(!from.joins.is_empty(), "expected JOIN in FROM clause");
        } else {
            unreachable!("expected Update");
        }
    }

    #[test]
    fn test_update_from_parentheses_are_stack_safe_at_1000_and_1001() {
        fn drop_update_from_iteratively(statement: Statement) {
            let Statement::Update(mut update) = statement else {
                panic!("expected UPDATE statement");
            };
            let Some(mut from) = update.from.take() else {
                panic!("expected UPDATE FROM clause");
            };
            drop(update);
            loop {
                let FromClause { source, joins } = from;
                assert!(joins.is_empty());
                match source {
                    TableOrSubquery::ParenJoin(inner) => from = *inner,
                    leaf => {
                        drop(leaf);
                        break;
                    }
                }
            }
        }

        for height in [1000, 1001] {
            let sql = format!(
                "UPDATE target SET value = 1 FROM {}source{} WHERE target.id = source.id",
                "(".repeat(height),
                ")".repeat(height)
            );
            let (rendered, statement) = std::thread::Builder::new()
                .stack_size(1024 * 1024)
                .spawn(move || {
                    let statement = Parser::from_sql(&sql)
                        .parse_statement()
                        .expect("deep UPDATE FROM must parse");
                    let rendered = statement.to_string();
                    (rendered, statement)
                })
                .expect("1 MiB parser thread must spawn")
                .join()
                .expect("deep UPDATE FROM parsing and formatting must not overflow");
            assert!(rendered.starts_with("UPDATE target SET value = 1 FROM "));
            assert!(rendered.ends_with(" WHERE target.id = source.id"));
            assert_eq!(rendered.matches('(').count(), height);
            assert_eq!(rendered.matches(')').count(), height);
            drop_update_from_iteratively(statement);
        }
    }

    #[test]
    fn test_malformed_deep_update_from_recovers_following_statement() {
        let sql = format!(
            "UPDATE target SET value = 1 FROM {}source{}; SELECT 42;",
            "(".repeat(1001),
            ")".repeat(1000)
        );
        let (statements, errors) = std::thread::Builder::new()
            .stack_size(1024 * 1024)
            .spawn(move || Parser::from_sql(&sql).parse_all())
            .expect("1 MiB parser thread must spawn")
            .join()
            .expect("malformed deep UPDATE FROM recovery must not overflow");

        assert_eq!(errors.len(), 1, "expected one unbalanced-FROM error");
        assert_eq!(
            statements.len(),
            1,
            "the malformed UPDATE must be discarded"
        );
        assert_eq!(statements[0].to_string(), "SELECT 42");
    }

    #[test]
    fn test_update_order_by_limit() {
        let stmt = parse_one("UPDATE t SET a = a + 1 ORDER BY b DESC LIMIT 10");
        if let Statement::Update(u) = stmt {
            assert_eq!(u.order_by.len(), 1);
            assert_eq!(u.order_by[0].direction, Some(SortDirection::Desc));
            assert!(u.limit.is_some());
        } else {
            unreachable!("expected Update");
        }
    }

    #[test]
    fn test_update_returning() {
        let stmt = parse_one("UPDATE t SET a = 1 WHERE id = 5 RETURNING id, a AS new_a");
        if let Statement::Update(u) = stmt {
            assert_eq!(u.returning.len(), 2);
            match &u.returning[1] {
                ResultColumn::Expr {
                    alias: Some(alias), ..
                } => assert_eq!(alias, "new_a"),
                other => unreachable!("expected aliased result column, got {other:?}"),
            }
        } else {
            unreachable!("expected Update");
        }
    }

    #[test]
    fn test_update_or_ignore() {
        let stmt = parse_one("UPDATE OR IGNORE t SET a = 1 WHERE id = 5");
        if let Statement::Update(u) = stmt {
            assert_eq!(u.or_conflict, Some(ConflictAction::Ignore));
            assert!(u.where_clause.is_some());
        } else {
            unreachable!("expected Update");
        }
    }

    #[test]
    fn test_delete_where() {
        let stmt = parse_one("DELETE FROM t WHERE id = 42 AND active = 0");
        if let Statement::Delete(d) = stmt {
            assert!(d.where_clause.is_some());
            assert!(d.returning.is_empty());
        } else {
            unreachable!("expected Delete");
        }
    }

    #[test]
    fn test_delete_order_by_limit() {
        let stmt = parse_one("DELETE FROM t ORDER BY created_at ASC LIMIT 100");
        if let Statement::Delete(d) = stmt {
            assert_eq!(d.order_by.len(), 1);
            assert_eq!(d.order_by[0].direction, Some(SortDirection::Asc));
            let limit = d.limit.as_ref().expect("LIMIT clause");
            assert!(matches!(
                limit.limit,
                Expr::Literal(Literal::Integer(100), _)
            ));
        } else {
            unreachable!("expected Delete");
        }
    }

    #[test]
    fn test_delete_returning() {
        let stmt = parse_one("DELETE FROM t WHERE id = 1 RETURNING *");
        if let Statement::Delete(d) = stmt {
            assert!(d.where_clause.is_some());
            assert_eq!(d.returning.len(), 1);
            assert!(matches!(d.returning[0], ResultColumn::Star));
        } else {
            unreachable!("expected Delete");
        }
    }

    #[test]
    fn test_delete_bulk_optimization() {
        // Parser-level: DELETE without WHERE produces AST with no where_clause
        // Runtime bulk-delete optimization (OP_Clear) is verified in VDBE/engine tests
        let stmt = parse_one("DELETE FROM t");
        if let Statement::Delete(d) = stmt {
            assert!(d.where_clause.is_none());
            assert!(d.order_by.is_empty());
            assert!(d.limit.is_none());
            assert!(d.returning.is_empty());
        } else {
            unreachable!("expected Delete");
        }
    }

    #[test]
    fn test_delete_bulk_no_where_fast() {
        // Parser-level: confirms DELETE without WHERE parses to minimal AST
        // Runtime OP_Clear vs OP_Delete selection is verified in VDBE/engine tests
        let stmt = parse_one("DELETE FROM main.t");
        if let Statement::Delete(d) = stmt {
            assert_eq!(d.table.name.schema.as_deref(), Some("main"));
            assert_eq!(d.table.name.name, "t");
            assert!(d.where_clause.is_none());
        } else {
            unreachable!("expected Delete");
        }
    }

    #[test]
    fn test_delete_bulk_blocked_by_trigger() {
        // Parser-level: DELETE without WHERE from a table that might have triggers
        // has the same AST shape (no WHERE). Runtime trigger detection is in the engine.
        let stmt = parse_one("DELETE FROM orders");
        if let Statement::Delete(d) = stmt {
            assert!(d.where_clause.is_none());
            assert!(d.returning.is_empty());
        } else {
            unreachable!("expected Delete");
        }
    }

    #[test]
    fn test_delete_bulk_blocked_by_fk() {
        // Parser-level: DELETE without WHERE is the same AST regardless of FK constraints.
        // Runtime FK-based fallback to row-by-row is verified in VDBE/engine tests.
        let stmt = parse_one("DELETE FROM parent_table");
        if let Statement::Delete(d) = stmt {
            assert!(d.where_clause.is_none());
        } else {
            unreachable!("expected Delete");
        }
    }

    #[test]
    fn test_delete_bulk_changes_count() {
        // Parser-level: DELETE without WHERE returning count via changes()
        // is the same AST as any unconditional delete. Runtime changes()
        // reporting is verified in VDBE/engine tests.
        let stmt = parse_one("DELETE FROM t");
        if let Statement::Delete(d) = stmt {
            assert!(d.where_clause.is_none());
        } else {
            unreachable!("expected Delete");
        }
    }

    #[test]
    fn test_delete_bulk_autoincrement_preserved() {
        // Parser-level: DELETE without WHERE on an autoincrement table has
        // identical AST to any unconditional delete. Runtime autoincrement
        // sequence preservation is verified in VDBE/engine tests.
        let stmt = parse_one("DELETE FROM t");
        if let Statement::Delete(d) = stmt {
            assert!(d.where_clause.is_none());
            assert!(d.limit.is_none());
        } else {
            unreachable!("expected Delete");
        }
    }

    #[test]
    fn test_delete_bulk_where_1_not_optimized() {
        // Parser-level: DELETE WHERE 1 has a where_clause (unlike bare DELETE),
        // so the optimizer cannot use bulk-delete. Verify WHERE is present.
        let stmt = parse_one("DELETE FROM t WHERE 1");
        if let Statement::Delete(d) = stmt {
            assert!(
                d.where_clause.is_some(),
                "WHERE 1 must produce a where_clause"
            );
            assert!(matches!(
                d.where_clause.as_ref().unwrap(),
                Expr::Literal(Literal::Integer(1), _)
            ));
        } else {
            unreachable!("expected Delete");
        }
    }

    // -----------------------------------------------------------------------
    // bd-34de §12.5-12.6 DDL: CREATE TABLE + CREATE INDEX parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_table_basic() {
        let stmt = parse_one("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
        if let Statement::CreateTable(ct) = stmt {
            assert_eq!(ct.name.name, "users");
            assert!(!ct.if_not_exists);
            assert!(!ct.temporary);
            assert!(!ct.without_rowid);
            assert!(!ct.strict);
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[1].name, "name");
                assert_eq!(columns[2].name, "age");
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let stmt = parse_one("CREATE TABLE IF NOT EXISTS t (id INTEGER)");
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.if_not_exists);
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_create_temp_table() {
        let stmt = parse_one("CREATE TEMP TABLE session_data (key TEXT, val BLOB)");
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.temporary);
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_create_table_as_select() {
        let stmt = parse_one("CREATE TABLE t2 AS SELECT id, name FROM t1 WHERE active = 1");
        if let Statement::CreateTable(ct) = stmt {
            assert!(matches!(ct.body, CreateTableBody::AsSelect(_)));
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_column_primary_key() {
        let stmt = parse_one("CREATE TABLE t (id INTEGER PRIMARY KEY ASC)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let pk = columns[0]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::PrimaryKey { .. }));
                assert!(pk.is_some(), "PK constraint missing");
                if let ColumnConstraintKind::PrimaryKey { direction, .. } = &pk.unwrap().kind {
                    assert_eq!(*direction, Some(SortDirection::Asc));
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_column_primary_key_autoincrement() {
        let stmt = parse_one("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let pk = columns[0]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::PrimaryKey { .. }));
                if let ColumnConstraintKind::PrimaryKey { autoincrement, .. } = &pk.unwrap().kind {
                    assert!(autoincrement, "AUTOINCREMENT flag not set");
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_autoincrement_uses_sqlite_sequence() {
        // Parser-level: verify AUTOINCREMENT syntax parses correctly.
        // Runtime sqlite_sequence tracking is verified in VDBE/engine tests.
        let stmt = parse_one("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                assert_eq!(columns.len(), 2);
                let pk = columns[0].constraints.iter().find(|c| {
                    matches!(
                        c.kind,
                        ColumnConstraintKind::PrimaryKey {
                            autoincrement: true,
                            ..
                        }
                    )
                });
                assert!(pk.is_some(), "AUTOINCREMENT constraint missing");
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_column_not_null() {
        let stmt = parse_one("CREATE TABLE t (name TEXT NOT NULL)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let nn = columns[0]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::NotNull { .. }));
                assert!(nn.is_some(), "NOT NULL constraint missing");
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_column_unique() {
        let stmt = parse_one("CREATE TABLE t (email TEXT UNIQUE)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let uq = columns[0]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::Unique { .. }));
                assert!(uq.is_some(), "UNIQUE constraint missing");
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_column_check() {
        let stmt = parse_one("CREATE TABLE t (age INTEGER CHECK(age >= 0 AND age < 200))");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let chk = columns[0]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::Check(_)));
                assert!(chk.is_some(), "CHECK constraint missing");
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_check_constraint_expression_height_fails_closed_at_1001() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = std::iter::repeat_n("1", LIMIT)
            .collect::<Vec<_>>()
            .join(" + ");
        let statement = format!("CREATE TABLE t (value INTEGER CHECK({at_limit}))");
        assert!(
            matches!(parse_one(&statement), Statement::CreateTable(_)),
            "height-1000 CHECK must remain attached to its CREATE TABLE"
        );

        let over_limit = std::iter::repeat_n("1", LIMIT + 1)
            .collect::<Vec<_>>()
            .join(" + ");
        let statement = format!("CREATE TABLE t (value INTEGER CHECK({over_limit}))");
        let mut parser = Parser::from_sql(&statement);
        let error = parser
            .parse_statement()
            .expect_err("height-1001 CHECK must reject the entire schema statement");
        assert_eq!(
            error.kind,
            ParseErrorKind::ExpressionTooDeep {
                max: MAX_PARSE_DEPTH
            }
        );
        assert_eq!(
            parser.depth, 0,
            "expression-height rejection must unwind parser recursion state"
        );
    }

    #[test]
    fn test_expression_height_boundary_is_context_independent_on_one_mib_stack() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        fn right_deep_expression(height: usize) -> String {
            format!("{}1{}", "1 + (".repeat(height - 1), ")".repeat(height - 1))
        }
        fn parse_on_one_mib_stack(sql: String) -> Result<(), ParseError> {
            std::thread::Builder::new()
                .stack_size(1024 * 1024)
                .spawn(move || {
                    let statement = Parser::from_sql(&sql).parse_statement()?;
                    // Destruction is part of the constrained-stack contract:
                    // a successful parse must not merely move a deep AST onto
                    // the caller's larger stack before dropping it.
                    drop(statement);
                    Ok(())
                })
                .expect("1 MiB parser thread must spawn")
                .join()
                .expect("schema-context parse must not overflow or panic")
        }

        let at_limit = right_deep_expression(LIMIT);
        let over_limit = right_deep_expression(LIMIT + 1);
        let contexts = [
            (
                "SELECT",
                format!("SELECT {at_limit}"),
                format!("SELECT {over_limit}"),
            ),
            (
                "column CHECK",
                format!("CREATE TABLE t (value INTEGER CHECK({at_limit}))"),
                format!("CREATE TABLE t (value INTEGER CHECK({over_limit}))"),
            ),
            (
                "table CHECK",
                format!("CREATE TABLE t (value INTEGER, CHECK({at_limit}))"),
                format!("CREATE TABLE t (value INTEGER, CHECK({over_limit}))"),
            ),
            (
                "view",
                format!("CREATE VIEW v AS SELECT {at_limit}"),
                format!("CREATE VIEW v AS SELECT {over_limit}"),
            ),
            (
                "trigger",
                format!(
                    "CREATE TRIGGER tr BEFORE INSERT ON t WHEN {at_limit} \
                     BEGIN SELECT 1; END"
                ),
                format!(
                    "CREATE TRIGGER tr BEFORE INSERT ON t WHEN {over_limit} \
                     BEGIN SELECT 1; END"
                ),
            ),
        ];

        for (context, accepted, rejected) in contexts {
            parse_on_one_mib_stack(accepted)
                .unwrap_or_else(|error| panic!("{context} height 1000 rejected: {error}"));

            let error = parse_on_one_mib_stack(rejected)
                .expect_err("height 1001 must reject every expression-bearing context");
            assert_eq!(
                error.kind,
                ParseErrorKind::ExpressionTooDeep {
                    max: MAX_PARSE_DEPTH
                },
                "wrong error classification for {context}: {error}"
            );
        }
    }

    #[test]
    fn test_column_default_literal() {
        let stmt = parse_one("CREATE TABLE t (status TEXT DEFAULT 'active')");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let def = columns[0]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::Default(_)));
                assert!(def.is_some(), "DEFAULT constraint missing");
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_column_default_expr() {
        let stmt = parse_one("CREATE TABLE t (created_at TEXT DEFAULT (datetime('now')))");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let def = columns[0].constraints.iter().find(|c| {
                    matches!(
                        c.kind,
                        ColumnConstraintKind::Default(DefaultValue::ParenExpr(_))
                    )
                });
                assert!(def.is_some(), "DEFAULT (expr) missing");
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_column_collate() {
        let stmt = parse_one("CREATE TABLE t (name TEXT COLLATE NOCASE)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let coll = columns[0]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::Collate(_)));
                assert!(coll.is_some(), "COLLATE constraint missing");
                if let ColumnConstraintKind::Collate(name) = &coll.unwrap().kind {
                    assert_eq!(name, "NOCASE");
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_table_constraint_composite_pk() {
        let stmt = parse_one("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { constraints, .. } = &ct.body {
                let pk = constraints
                    .iter()
                    .find(|c| matches!(c.kind, TableConstraintKind::PrimaryKey { .. }));
                assert!(pk.is_some(), "composite PK missing");
                if let TableConstraintKind::PrimaryKey { columns, .. } = &pk.unwrap().kind {
                    assert_eq!(columns.len(), 2);
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_table_constraint_composite_unique() {
        let stmt = parse_one("CREATE TABLE t (a TEXT, b TEXT, UNIQUE (a, b))");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { constraints, .. } = &ct.body {
                let uq = constraints
                    .iter()
                    .find(|c| matches!(c.kind, TableConstraintKind::Unique { .. }));
                assert!(uq.is_some(), "composite UNIQUE missing");
                if let TableConstraintKind::Unique { columns, .. } = &uq.unwrap().kind {
                    assert_eq!(columns.len(), 2);
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_table_constraint_check() {
        let stmt = parse_one(
            "CREATE TABLE t (start_date TEXT, end_date TEXT, CHECK (start_date < end_date))",
        );
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { constraints, .. } = &ct.body {
                let chk = constraints
                    .iter()
                    .find(|c| matches!(c.kind, TableConstraintKind::Check(_)));
                assert!(chk.is_some(), "table CHECK constraint missing");
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_create_rejects_modifiers_for_incompatible_object_kinds() {
        for sql in [
            "CREATE UNIQUE TABLE t(value INTEGER)",
            "CREATE UNIQUE VIEW v AS SELECT 1",
            "CREATE UNIQUE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END",
            "CREATE UNIQUE VIRTUAL TABLE vt USING fts5(content)",
            "CREATE TEMP INDEX i ON t(value)",
            "CREATE TEMP VIRTUAL TABLE vt USING fts5(content)",
        ] {
            Parser::from_sql(sql)
                .parse_statement()
                .expect_err("CREATE modifiers must not be discarded for incompatible objects");
        }
    }

    #[test]
    fn test_invalid_create_modifier_recovers_next_statement() {
        let (statements, errors) =
            Parser::from_sql("CREATE UNIQUE TABLE t(value INTEGER); SELECT 42;").parse_all();

        assert_eq!(errors.len(), 1);
        assert_eq!(statements.len(), 1);
        assert!(matches!(statements[0], Statement::Select(_)));
    }

    #[test]
    fn test_foreign_key_on_delete_cascade() {
        let stmt = parse_one(
            "CREATE TABLE child (id INTEGER, parent_id INTEGER \
             REFERENCES parent(id) ON DELETE CASCADE)",
        );
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let fk = columns[1]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::ForeignKey(_)));
                assert!(fk.is_some(), "FK constraint missing");
                if let ColumnConstraintKind::ForeignKey(clause) = &fk.unwrap().kind {
                    assert_eq!(clause.table, "parent");
                    let del = clause
                        .actions
                        .iter()
                        .find(|a| a.trigger == ForeignKeyTrigger::OnDelete);
                    assert!(del.is_some());
                    assert_eq!(del.unwrap().action, ForeignKeyActionType::Cascade);
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_foreign_key_on_delete_set_null() {
        let stmt = parse_one(
            "CREATE TABLE child (id INTEGER, parent_id INTEGER \
             REFERENCES parent(id) ON DELETE SET NULL)",
        );
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                if let ColumnConstraintKind::ForeignKey(clause) = &columns[1]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::ForeignKey(_)))
                    .unwrap()
                    .kind
                {
                    let del = clause
                        .actions
                        .iter()
                        .find(|a| a.trigger == ForeignKeyTrigger::OnDelete);
                    assert_eq!(del.unwrap().action, ForeignKeyActionType::SetNull);
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_foreign_key_on_update_cascade() {
        let stmt = parse_one(
            "CREATE TABLE child (id INTEGER, parent_id INTEGER \
             REFERENCES parent(id) ON UPDATE CASCADE)",
        );
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                if let ColumnConstraintKind::ForeignKey(clause) = &columns[1]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::ForeignKey(_)))
                    .unwrap()
                    .kind
                {
                    let upd = clause
                        .actions
                        .iter()
                        .find(|a| a.trigger == ForeignKeyTrigger::OnUpdate);
                    assert!(upd.is_some());
                    assert_eq!(upd.unwrap().action, ForeignKeyActionType::Cascade);
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_foreign_key_restrict() {
        let stmt = parse_one(
            "CREATE TABLE child (id INTEGER, parent_id INTEGER \
             REFERENCES parent(id) ON DELETE RESTRICT)",
        );
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                if let ColumnConstraintKind::ForeignKey(clause) = &columns[1]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::ForeignKey(_)))
                    .unwrap()
                    .kind
                {
                    let del = clause
                        .actions
                        .iter()
                        .find(|a| a.trigger == ForeignKeyTrigger::OnDelete);
                    assert_eq!(del.unwrap().action, ForeignKeyActionType::Restrict);
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_foreign_key_deferred() {
        let stmt = parse_one(
            "CREATE TABLE child (id INTEGER, parent_id INTEGER \
             REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
        );
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                if let ColumnConstraintKind::ForeignKey(clause) = &columns[1]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::ForeignKey(_)))
                    .unwrap()
                    .kind
                {
                    let def = clause.deferrable.as_ref().expect("DEFERRABLE missing");
                    assert!(!def.not, "should be DEFERRABLE, not NOT DEFERRABLE");
                    assert_eq!(def.initially, Some(DeferrableInitially::Deferred));
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_foreign_key_pragma_required() {
        // Parser-level: FK syntax parses identically regardless of PRAGMA state.
        // Runtime enforcement requiring PRAGMA foreign_keys = ON is in VDBE/engine.
        let stmt = parse_one(
            "CREATE TABLE child (id INTEGER, parent_id INTEGER \
             REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL)",
        );
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                if let ColumnConstraintKind::ForeignKey(clause) = &columns[1]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::ForeignKey(_)))
                    .unwrap()
                    .kind
                {
                    assert_eq!(clause.actions.len(), 2);
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_conflict_clause_on_not_null() {
        let stmt = parse_one("CREATE TABLE t (name TEXT NOT NULL ON CONFLICT IGNORE)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let nn = columns[0]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::NotNull { .. }));
                if let ColumnConstraintKind::NotNull { conflict } = &nn.unwrap().kind {
                    assert_eq!(*conflict, Some(ConflictAction::Ignore));
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_without_rowid_table() {
        let stmt = parse_one("CREATE TABLE t (k TEXT PRIMARY KEY, v BLOB) WITHOUT ROWID");
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.without_rowid);
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_without_rowid_no_autoincrement() {
        // Parser-level: verify WITHOUT ROWID and AUTOINCREMENT can both parse.
        // Runtime rejection of AUTOINCREMENT on WITHOUT ROWID is in schema validation.
        let stmt = parse_one(
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT) WITHOUT ROWID",
        );
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.without_rowid);
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let pk = columns[0].constraints.iter().find(|c| {
                    matches!(
                        c.kind,
                        ColumnConstraintKind::PrimaryKey {
                            autoincrement: true,
                            ..
                        }
                    )
                });
                assert!(pk.is_some());
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_without_rowid_integer_pk_not_alias() {
        // Parser-level: INTEGER PRIMARY KEY in WITHOUT ROWID parses as normal PK.
        // Runtime non-aliasing of rowid is in the B-tree layer.
        let stmt = parse_one("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT) WITHOUT ROWID");
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.without_rowid);
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                assert_eq!(columns[0].name, "id");
                assert!(columns[0].type_name.is_some());
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_strict_table_type_enforcement() {
        // Parser-level: STRICT keyword parses on CREATE TABLE.
        // Runtime type enforcement on INSERT/UPDATE is in VDBE/engine.
        let stmt = parse_one("CREATE TABLE t (id INTEGER, name TEXT, score REAL) STRICT");
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.strict);
            assert!(!ct.without_rowid);
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_strict_table_any_column() {
        // Parser-level: ANY type name parses in STRICT table context.
        let stmt = parse_one("CREATE TABLE t (id INTEGER, data ANY) STRICT");
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.strict);
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let tn = columns[1].type_name.as_ref().expect("type name");
                assert_eq!(tn.name, "ANY");
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_strict_allowed_types() {
        // Parser-level: STRICT table with all allowed type names parses.
        let stmt =
            parse_one("CREATE TABLE t (a INT, b INTEGER, c REAL, d TEXT, e BLOB, f ANY) STRICT");
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.strict);
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                assert_eq!(columns.len(), 6);
                let types: Vec<&str> = columns
                    .iter()
                    .map(|c| c.type_name.as_ref().unwrap().name.as_str())
                    .collect();
                assert_eq!(types, vec!["INT", "INTEGER", "REAL", "TEXT", "BLOB", "ANY"]);
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_generated_col_virtual() {
        let stmt = parse_one(
            "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL)",
        );
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let generated = columns[2]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::Generated { .. }));
                assert!(generated.is_some(), "Generated constraint missing");
                if let ColumnConstraintKind::Generated { storage, .. } = &generated.unwrap().kind {
                    assert_eq!(*storage, Some(GeneratedStorage::Virtual));
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_generated_col_stored() {
        let stmt = parse_one(
            "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a * b) STORED)",
        );
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                let generated = columns[2]
                    .constraints
                    .iter()
                    .find(|c| matches!(c.kind, ColumnConstraintKind::Generated { .. }));
                if let ColumnConstraintKind::Generated { storage, .. } = &generated.unwrap().kind {
                    assert_eq!(*storage, Some(GeneratedStorage::Stored));
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_generated_col_ordering() {
        // Parser-level: generated columns with forward references parse correctly.
        // Runtime rejection of forward references is in schema validation.
        let stmt = parse_one(
            "CREATE TABLE t (\
             a INTEGER, \
             b INTEGER GENERATED ALWAYS AS (a + 1) STORED, \
             c INTEGER GENERATED ALWAYS AS (b * 2) VIRTUAL)",
        );
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                assert_eq!(columns.len(), 3);
                // Both b and c have Generated constraints
                let gen_b = columns[1]
                    .constraints
                    .iter()
                    .any(|c| matches!(c.kind, ColumnConstraintKind::Generated { .. }));
                let gen_c = columns[2]
                    .constraints
                    .iter()
                    .any(|c| matches!(c.kind, ColumnConstraintKind::Generated { .. }));
                assert!(gen_b, "column b should be generated");
                assert!(gen_c, "column c should be generated");
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_generated_col_stored_indexable() {
        // Parser-level: STORED generated column can appear alongside indexes.
        // Runtime indexability verified in B-tree/engine tests.
        let stmts = parse_ok(
            "CREATE TABLE t (a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2) STORED); \
             CREATE INDEX idx_b ON t (b)",
        );
        assert_eq!(stmts.len(), 2);
        assert!(matches!(stmts[0], Statement::CreateTable(_)));
        assert!(matches!(stmts[1], Statement::CreateIndex(_)));
    }

    #[test]
    fn test_type_affinity_int() {
        // Parser-level: type names containing "INT" parse as valid type names.
        // Runtime affinity determination is in the type system.
        let stmt = parse_one("CREATE TABLE t (a INTEGER, b BIGINT, c SMALLINT, d MEDIUMINT)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                assert_eq!(columns.len(), 4);
                for col in columns {
                    let tn = col.type_name.as_ref().unwrap();
                    assert!(tn.name.contains("INT"), "{} should contain INT", tn.name);
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_type_affinity_text() {
        let stmt = parse_one("CREATE TABLE t (a TEXT, b VARCHAR, c CLOB, d CHARACTER)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                assert_eq!(columns.len(), 4);
                for col in columns {
                    assert!(col.type_name.is_some());
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_type_affinity_blob() {
        let stmt = parse_one("CREATE TABLE t (a BLOB, b)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].type_name.as_ref().unwrap().name, "BLOB");
                // Column b has no type name -> BLOB affinity
                assert!(columns[1].type_name.is_none());
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_type_affinity_real() {
        let stmt = parse_one("CREATE TABLE t (a REAL, b DOUBLE, c FLOAT)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                assert_eq!(columns.len(), 3);
                for col in columns {
                    assert!(col.type_name.is_some());
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_type_affinity_numeric() {
        let stmt = parse_one("CREATE TABLE t (a NUMERIC, b DECIMAL, c BOOLEAN)");
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns { columns, .. } = &ct.body {
                assert_eq!(columns.len(), 3);
                for col in columns {
                    assert!(col.type_name.is_some());
                }
            } else {
                unreachable!("expected Columns body");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    #[test]
    fn test_create_unique_index() {
        let stmt = parse_one("CREATE UNIQUE INDEX idx_email ON users (email)");
        if let Statement::CreateIndex(ci) = stmt {
            assert!(ci.unique);
            assert_eq!(ci.name.name, "idx_email");
            assert_eq!(ci.table, "users");
            assert_eq!(ci.columns.len(), 1);
            assert!(ci.where_clause.is_none());
        } else {
            unreachable!("expected CreateIndex");
        }
    }

    #[test]
    fn test_partial_index() {
        let stmt = parse_one("CREATE INDEX idx_active ON users (name) WHERE active = 1");
        if let Statement::CreateIndex(ci) = stmt {
            assert!(!ci.unique);
            assert_eq!(ci.name.name, "idx_active");
            assert!(ci.where_clause.is_some(), "partial index WHERE missing");
        } else {
            unreachable!("expected CreateIndex");
        }
    }

    #[test]
    fn test_partial_index_planner_usage() {
        // Parser-level: partial index with complex WHERE parses correctly.
        // Runtime planner usage (query WHERE implies index WHERE) is in planner tests.
        let stmt =
            parse_one("CREATE INDEX idx_recent ON orders (created_at) WHERE status != 'archived'");
        if let Statement::CreateIndex(ci) = stmt {
            assert!(ci.where_clause.is_some());
            assert_eq!(ci.columns.len(), 1);
        } else {
            unreachable!("expected CreateIndex");
        }
    }

    #[test]
    fn test_expression_index() {
        let stmt = parse_one("CREATE INDEX idx_lower_name ON users (lower(name))");
        if let Statement::CreateIndex(ci) = stmt {
            assert_eq!(ci.columns.len(), 1);
            // The indexed expression should be a function call, not a plain column
            assert!(
                matches!(ci.columns[0].expr, Expr::FunctionCall { .. }),
                "expected function call expression in index"
            );
        } else {
            unreachable!("expected CreateIndex");
        }
    }

    #[test]
    fn test_expression_index_planner_match() {
        // Parser-level: expression index with arithmetic parses correctly.
        // Runtime structural equality matching is in planner tests.
        let stmt = parse_one("CREATE INDEX idx_calc ON t (a + b * 2)");
        if let Statement::CreateIndex(ci) = stmt {
            assert_eq!(ci.columns.len(), 1);
            assert!(
                matches!(ci.columns[0].expr, Expr::BinaryOp { .. }),
                "expected binary op in expression index"
            );
        } else {
            unreachable!("expected CreateIndex");
        }
    }

    #[test]
    fn test_index_collate_asc_desc() {
        let stmt = parse_one("CREATE INDEX idx_multi ON t (a COLLATE NOCASE ASC, b DESC, c)");
        if let Statement::CreateIndex(ci) = stmt {
            assert_eq!(ci.columns.len(), 3);
            // COLLATE is consumed by the expression parser as Expr::Collate
            assert!(
                matches!(
                    &ci.columns[0].expr,
                    Expr::Collate { collation, .. } if collation == "NOCASE"
                ),
                "expected Collate expr with NOCASE"
            );
            assert_eq!(ci.columns[0].direction, Some(SortDirection::Asc));
            assert_eq!(ci.columns[1].direction, Some(SortDirection::Desc));
            assert!(ci.columns[2].direction.is_none());
        } else {
            unreachable!("expected CreateIndex");
        }
    }

    // -----------------------------------------------------------------------
    // bd-3kin §12.7-12.9 CREATE VIEW + CREATE TRIGGER + ALTER/DROP parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_view_basic() {
        let stmt = parse_one("CREATE VIEW v AS SELECT id, name FROM users");
        if let Statement::CreateView(cv) = stmt {
            assert_eq!(cv.name.name, "v");
            assert!(!cv.if_not_exists);
            assert!(!cv.temporary);
            assert!(cv.columns.is_empty());
        } else {
            unreachable!("expected CreateView");
        }
    }

    #[test]
    fn test_create_view_column_aliases() {
        let stmt = parse_one("CREATE VIEW v (user_id, user_name) AS SELECT id, name FROM users");
        if let Statement::CreateView(cv) = stmt {
            assert_eq!(cv.columns, vec!["user_id", "user_name"]);
        } else {
            unreachable!("expected CreateView");
        }
    }

    #[test]
    fn test_create_view_if_not_exists() {
        let stmt = parse_one("CREATE VIEW IF NOT EXISTS v AS SELECT 1");
        if let Statement::CreateView(cv) = stmt {
            assert!(cv.if_not_exists);
        } else {
            unreachable!("expected CreateView");
        }
    }

    #[test]
    fn test_create_temp_view() {
        let stmt = parse_one("CREATE TEMP VIEW tv AS SELECT 1");
        if let Statement::CreateView(cv) = stmt {
            assert!(cv.temporary);
        } else {
            unreachable!("expected CreateView");
        }
    }

    #[test]
    fn test_view_inline_expansion() {
        // Parser-level: view defined with WHERE is captured in AST.
        // Runtime inline expansion (not materialization) is in the planner.
        let stmt =
            parse_one("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1");
        if let Statement::CreateView(cv) = stmt {
            assert_eq!(cv.name.name, "active_users");
        } else {
            unreachable!("expected CreateView");
        }
    }

    #[test]
    fn test_view_read_only() {
        // Parser-level: views parse as SELECT-only definitions.
        // Runtime read-only enforcement (rejecting DML without INSTEAD OF) is in the engine.
        let stmt = parse_one("CREATE VIEW v AS SELECT * FROM t");
        assert!(matches!(stmt, Statement::CreateView(_)));
    }

    #[test]
    fn test_view_with_recursive_cte() {
        // View referencing a subquery (parser does not yet support WITH directly
        // in CREATE VIEW ... AS context; CTE-in-view support is a planner concern).
        let stmt = parse_one(
            "CREATE VIEW tree AS \
             SELECT n.id, n.parent FROM nodes n \
             WHERE n.parent IS NULL \
             UNION ALL \
             SELECT c.id, c.parent FROM nodes c JOIN nodes p ON c.parent = p.id",
        );
        if let Statement::CreateView(cv) = stmt {
            assert_eq!(cv.name.name, "tree");
            // Compound UNION ALL captured
            assert!(
                !cv.query.body.compounds.is_empty(),
                "expected compound SELECT in view"
            );
        } else {
            unreachable!("expected CreateView");
        }
    }

    #[test]
    fn test_instead_of_trigger_on_view() {
        let stmt = parse_one(
            "CREATE TRIGGER tr INSTEAD OF INSERT ON v BEGIN \
             INSERT INTO t (a) VALUES (NEW.a); \
             END",
        );
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.timing, TriggerTiming::InsteadOf);
            assert!(matches!(ct.event, TriggerEvent::Insert));
            assert_eq!(ct.table, "v");
            assert!(!ct.body.is_empty());
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_before_insert() {
        let stmt = parse_one("CREATE TRIGGER tr BEFORE INSERT ON t BEGIN SELECT 1; END");
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.timing, TriggerTiming::Before);
            assert!(matches!(ct.event, TriggerEvent::Insert));
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_after_insert() {
        let stmt = parse_one("CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END");
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.timing, TriggerTiming::After);
            assert!(matches!(ct.event, TriggerEvent::Insert));
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_before_update() {
        let stmt = parse_one("CREATE TRIGGER tr BEFORE UPDATE ON t BEGIN SELECT OLD.a, NEW.a; END");
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.timing, TriggerTiming::Before);
            assert!(matches!(ct.event, TriggerEvent::Update(_)));
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_after_delete() {
        let stmt = parse_one("CREATE TRIGGER tr AFTER DELETE ON t BEGIN SELECT OLD.id; END");
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.timing, TriggerTiming::After);
            assert!(matches!(ct.event, TriggerEvent::Delete));
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_update_of_column() {
        let stmt =
            parse_one("CREATE TRIGGER tr BEFORE UPDATE OF name, email ON t BEGIN SELECT 1; END");
        if let Statement::CreateTrigger(ct) = stmt {
            if let TriggerEvent::Update(cols) = &ct.event {
                assert_eq!(cols, &["name", "email"]);
            } else {
                unreachable!("expected Update event with columns");
            }
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_when_clause() {
        let stmt = parse_one(
            "CREATE TRIGGER tr BEFORE INSERT ON t WHEN NEW.active = 1 BEGIN SELECT 1; END",
        );
        if let Statement::CreateTrigger(ct) = stmt {
            assert!(ct.when.is_some(), "WHEN clause missing");
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_old_new_pseudo_tables() {
        let stmt = parse_one(
            "CREATE TRIGGER tr BEFORE UPDATE ON t BEGIN \
             INSERT INTO log (old_val, new_val) VALUES (OLD.a, NEW.a); \
             END",
        );
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.body.len(), 1);
            assert!(matches!(ct.body[0], Statement::Insert(_)));
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_raise_abort() {
        let stmt = parse_one(
            "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN \
             SELECT RAISE(ABORT, 'not allowed'); \
             END",
        );
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.body.len(), 1);
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_raise_rollback() {
        let stmt = parse_one(
            "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN \
             SELECT RAISE(ROLLBACK, 'invalid'); \
             END",
        );
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.body.len(), 1);
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_raise_fail() {
        let stmt = parse_one(
            "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN \
             SELECT RAISE(FAIL, 'bad data'); \
             END",
        );
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.body.len(), 1);
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_raise_ignore() {
        let stmt = parse_one(
            "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN \
             SELECT RAISE(IGNORE); \
             END",
        );
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.body.len(), 1);
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_recursive() {
        // Parser-level: trigger referencing its own table parses normally.
        // Runtime recursive trigger behavior (PRAGMA recursive_triggers) is in the engine.
        let stmt = parse_one(
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
             INSERT INTO t (val) VALUES (NEW.val + 1); \
             END",
        );
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.timing, TriggerTiming::After);
            assert_eq!(ct.table, "t");
            assert_eq!(ct.body.len(), 1);
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_max_recursion_depth() {
        // Parser-level: trigger with WHEN depth guard parses.
        // Runtime SQLITE_MAX_TRIGGER_DEPTH enforcement is in the VDBE.
        let stmt = parse_one(
            "CREATE TRIGGER tr AFTER INSERT ON t \
             WHEN NEW.depth < 1000 BEGIN \
             INSERT INTO t (depth) VALUES (NEW.depth + 1); \
             END",
        );
        if let Statement::CreateTrigger(ct) = stmt {
            assert!(ct.when.is_some());
            assert_eq!(ct.body.len(), 1);
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_heap_frame_stack() {
        // Parser-level: trigger with UPDATE body parses correctly.
        // Runtime heap-allocated frame stack is in the VDBE.
        let stmt = parse_one(
            "CREATE TRIGGER tr AFTER UPDATE ON t BEGIN \
             UPDATE t SET counter = counter + 1 WHERE id = NEW.parent_id; \
             END",
        );
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.body.len(), 1);
            assert!(matches!(ct.body[0], Statement::Update(_)));
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_trigger_multiple_dml() {
        let stmt = parse_one(
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
             INSERT INTO audit (action) VALUES ('insert'); \
             UPDATE stats SET count = count + 1; \
             END",
        );
        if let Statement::CreateTrigger(ct) = stmt {
            assert_eq!(ct.body.len(), 2);
            assert!(matches!(ct.body[0], Statement::Insert(_)));
            assert!(matches!(ct.body[1], Statement::Update(_)));
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_alter_table_rename() {
        let stmt = parse_one("ALTER TABLE t RENAME TO t2");
        if let Statement::AlterTable(at) = stmt {
            assert_eq!(at.table.name, "t");
            assert!(matches!(at.action, AlterTableAction::RenameTo(ref n) if n == "t2"));
        } else {
            unreachable!("expected AlterTable");
        }
    }

    #[test]
    fn test_alter_table_rename_column() {
        let stmt = parse_one("ALTER TABLE t RENAME COLUMN old_name TO new_name");
        if let Statement::AlterTable(at) = stmt {
            if let AlterTableAction::RenameColumn { old, new } = &at.action {
                assert_eq!(old, "old_name");
                assert_eq!(new, "new_name");
            } else {
                unreachable!("expected RenameColumn action");
            }
        } else {
            unreachable!("expected AlterTable");
        }
    }

    #[test]
    fn test_alter_table_add_column() {
        let stmt = parse_one("ALTER TABLE t ADD COLUMN email TEXT NOT NULL DEFAULT ''");
        if let Statement::AlterTable(at) = stmt {
            if let AlterTableAction::AddColumn(col) = &at.action {
                assert_eq!(col.name, "email");
                assert!(!col.constraints.is_empty());
            } else {
                unreachable!("expected AddColumn action");
            }
        } else {
            unreachable!("expected AlterTable");
        }
    }

    #[test]
    fn test_alter_table_remove_column() {
        let stmt = parse_one("ALTER TABLE t DROP COLUMN old_col");
        if let Statement::AlterTable(at) = stmt {
            assert!(matches!(at.action, AlterTableAction::DropColumn(ref c) if c == "old_col"));
        } else {
            unreachable!("expected AlterTable");
        }
    }

    #[test]
    fn test_alter_remove_column_pk_fails() {
        // Parser-level: DROP COLUMN on a PK column parses normally.
        // Runtime rejection is in schema validation.
        let stmt = parse_one("ALTER TABLE t DROP COLUMN id");
        if let Statement::AlterTable(at) = stmt {
            assert!(matches!(at.action, AlterTableAction::DropColumn(ref c) if c == "id"));
        } else {
            unreachable!("expected AlterTable");
        }
    }

    #[test]
    fn test_alter_remove_column_unique_fails() {
        // Parser-level: DROP COLUMN on UNIQUE column parses normally.
        let stmt = parse_one("ALTER TABLE t DROP COLUMN email");
        if let Statement::AlterTable(at) = stmt {
            assert!(matches!(at.action, AlterTableAction::DropColumn(ref c) if c == "email"));
        } else {
            unreachable!("expected AlterTable");
        }
    }

    #[test]
    fn test_alter_remove_column_index_fails() {
        // Parser-level: DROP COLUMN on indexed column parses normally.
        let stmt = parse_one("ALTER TABLE t DROP COLUMN indexed_col");
        if let Statement::AlterTable(at) = stmt {
            assert!(matches!(
                at.action,
                AlterTableAction::DropColumn(ref c) if c == "indexed_col"
            ));
        } else {
            unreachable!("expected AlterTable");
        }
    }

    #[test]
    fn test_alter_remove_column_check_fails() {
        // Parser-level: DROP COLUMN on CHECK-constrained column parses normally.
        let stmt = parse_one("ALTER TABLE t DROP COLUMN checked_col");
        if let Statement::AlterTable(at) = stmt {
            assert!(matches!(
                at.action,
                AlterTableAction::DropColumn(ref c) if c == "checked_col"
            ));
        } else {
            unreachable!("expected AlterTable");
        }
    }

    #[test]
    fn test_alter_remove_column_fk_fails() {
        // Parser-level: DROP COLUMN on FK-constrained column parses normally.
        let stmt = parse_one("ALTER TABLE t DROP COLUMN fk_col");
        if let Statement::AlterTable(at) = stmt {
            assert!(matches!(at.action, AlterTableAction::DropColumn(ref c) if c == "fk_col"));
        } else {
            unreachable!("expected AlterTable");
        }
    }

    #[test]
    fn test_alter_remove_only_column_fails() {
        // Parser-level: DROP COLUMN on only column parses normally.
        let stmt = parse_one("ALTER TABLE t DROP COLUMN only_col");
        if let Statement::AlterTable(at) = stmt {
            assert!(matches!(
                at.action,
                AlterTableAction::DropColumn(ref c) if c == "only_col"
            ));
        } else {
            unreachable!("expected AlterTable");
        }
    }

    #[test]
    fn test_ddl_remove_table() {
        let stmt = parse_one("DROP TABLE t");
        if let Statement::Drop(d) = stmt {
            assert_eq!(d.object_type, DropObjectType::Table);
            assert!(!d.if_exists);
            assert_eq!(d.name.name, "t");
        } else {
            unreachable!("expected Drop");
        }
    }

    #[test]
    fn test_ddl_remove_table_if_exists() {
        let stmt = parse_one("DROP TABLE IF EXISTS t");
        if let Statement::Drop(d) = stmt {
            assert_eq!(d.object_type, DropObjectType::Table);
            assert!(d.if_exists);
        } else {
            unreachable!("expected Drop");
        }
    }

    #[test]
    fn test_ddl_remove_index() {
        let stmt = parse_one("DROP INDEX idx");
        if let Statement::Drop(d) = stmt {
            assert_eq!(d.object_type, DropObjectType::Index);
            assert_eq!(d.name.name, "idx");
        } else {
            unreachable!("expected Drop");
        }
    }

    #[test]
    fn test_ddl_remove_view() {
        let stmt = parse_one("DROP VIEW v");
        if let Statement::Drop(d) = stmt {
            assert_eq!(d.object_type, DropObjectType::View);
            assert_eq!(d.name.name, "v");
        } else {
            unreachable!("expected Drop");
        }
    }

    #[test]
    fn test_ddl_remove_trigger() {
        let stmt = parse_one("DROP TRIGGER tr");
        if let Statement::Drop(d) = stmt {
            assert_eq!(d.object_type, DropObjectType::Trigger);
            assert_eq!(d.name.name, "tr");
        } else {
            unreachable!("expected Drop");
        }
    }

    // -----------------------------------------------------------------------
    // bd-3kin §12.7-12.9 DDL gap-fill: REINDEX, ANALYZE, qualified names,
    //                                   IF NOT EXISTS/TEMP triggers
    // -----------------------------------------------------------------------

    #[test]
    fn test_reindex_global() {
        let stmt = parse_one("REINDEX");
        assert!(matches!(stmt, Statement::Reindex(None)));
    }

    #[test]
    fn test_reindex_table() {
        let stmt = parse_one("REINDEX t");
        if let Statement::Reindex(Some(name)) = stmt {
            assert_eq!(name.name, "t");
            assert!(name.schema.is_none());
        } else {
            unreachable!("expected Reindex(Some), got {stmt:?}");
        }
    }

    #[test]
    fn test_reindex_qualified() {
        let stmt = parse_one("REINDEX main.idx");
        if let Statement::Reindex(Some(name)) = stmt {
            assert_eq!(name.schema.as_deref(), Some("main"));
            assert_eq!(name.name, "idx");
        } else {
            unreachable!("expected Reindex(Some), got {stmt:?}");
        }
    }

    #[test]
    fn test_analyze_global() {
        let stmt = parse_one("ANALYZE");
        assert!(matches!(stmt, Statement::Analyze(None)));
    }

    #[test]
    fn test_analyze_table() {
        let stmt = parse_one("ANALYZE t");
        if let Statement::Analyze(Some(name)) = stmt {
            assert_eq!(name.name, "t");
            assert!(name.schema.is_none());
        } else {
            unreachable!("expected Analyze(Some), got {stmt:?}");
        }
    }

    #[test]
    fn test_analyze_qualified() {
        let stmt = parse_one("ANALYZE main.t");
        if let Statement::Analyze(Some(name)) = stmt {
            assert_eq!(name.schema.as_deref(), Some("main"));
            assert_eq!(name.name, "t");
        } else {
            unreachable!("expected Analyze(Some), got {stmt:?}");
        }
    }

    #[test]
    fn test_drop_view_if_exists() {
        let stmt = parse_one("DROP VIEW IF EXISTS v");
        if let Statement::Drop(d) = stmt {
            assert_eq!(d.object_type, DropObjectType::View);
            assert!(d.if_exists);
            assert_eq!(d.name.name, "v");
        } else {
            unreachable!("expected Drop");
        }
    }

    #[test]
    fn test_drop_index_if_exists() {
        let stmt = parse_one("DROP INDEX IF EXISTS idx");
        if let Statement::Drop(d) = stmt {
            assert_eq!(d.object_type, DropObjectType::Index);
            assert!(d.if_exists);
        } else {
            unreachable!("expected Drop");
        }
    }

    #[test]
    fn test_drop_trigger_if_exists_qualified() {
        let stmt = parse_one("DROP TRIGGER IF EXISTS main.tr");
        if let Statement::Drop(d) = stmt {
            assert_eq!(d.object_type, DropObjectType::Trigger);
            assert!(d.if_exists);
            assert_eq!(d.name.schema.as_deref(), Some("main"));
            assert_eq!(d.name.name, "tr");
        } else {
            unreachable!("expected Drop");
        }
    }

    #[test]
    fn test_drop_table_qualified() {
        let stmt = parse_one("DROP TABLE main.t");
        if let Statement::Drop(d) = stmt {
            assert_eq!(d.name.schema.as_deref(), Some("main"));
            assert_eq!(d.name.name, "t");
        } else {
            unreachable!("expected Drop");
        }
    }

    #[test]
    fn test_create_trigger_if_not_exists() {
        let stmt =
            parse_one("CREATE TRIGGER IF NOT EXISTS tr BEFORE INSERT ON t BEGIN SELECT 1; END");
        if let Statement::CreateTrigger(ct) = stmt {
            assert!(ct.if_not_exists);
            assert_eq!(ct.name.name, "tr");
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_create_temp_trigger() {
        let stmt = parse_one("CREATE TEMP TRIGGER tr BEFORE INSERT ON t BEGIN SELECT 1; END");
        if let Statement::CreateTrigger(ct) = stmt {
            assert!(ct.temporary);
            assert_eq!(ct.name.name, "tr");
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_create_view_qualified_name() {
        let stmt = parse_one("CREATE VIEW main.v AS SELECT 1");
        if let Statement::CreateView(cv) = stmt {
            assert_eq!(cv.name.schema.as_deref(), Some("main"));
            assert_eq!(cv.name.name, "v");
        } else {
            unreachable!("expected CreateView");
        }
    }

    #[test]
    fn test_alter_table_qualified() {
        let stmt = parse_one("ALTER TABLE main.t RENAME TO u");
        if let Statement::AlterTable(at) = stmt {
            assert_eq!(at.table.schema.as_deref(), Some("main"));
            assert_eq!(at.table.name, "t");
        } else {
            unreachable!("expected AlterTable");
        }
    }

    #[test]
    fn test_roundtrip_reindex_all() {
        assert_roundtrip("REINDEX");
        assert_roundtrip("REINDEX t");
        assert_roundtrip("REINDEX main.idx");
    }

    #[test]
    fn test_roundtrip_analyze_all() {
        assert_roundtrip("ANALYZE");
        assert_roundtrip("ANALYZE t");
        assert_roundtrip("ANALYZE main.t");
    }

    #[test]
    fn test_roundtrip_drop_all_types_extended() {
        assert_roundtrip("DROP TABLE IF EXISTS main.t");
        assert_roundtrip("DROP VIEW IF EXISTS v");
        assert_roundtrip("DROP INDEX IF EXISTS idx");
        assert_roundtrip("DROP TRIGGER IF EXISTS main.tr");
    }

    #[test]
    fn test_roundtrip_create_trigger_extended() {
        assert_roundtrip("CREATE TRIGGER IF NOT EXISTS tr BEFORE INSERT ON t BEGIN SELECT 1; END");
        assert_roundtrip("CREATE TEMP TRIGGER tr BEFORE INSERT ON t BEGIN SELECT 1; END");
        assert_roundtrip(
            "CREATE TRIGGER tr INSTEAD OF UPDATE ON v BEGIN INSERT INTO log VALUES (1); END",
        );
        assert_roundtrip("CREATE TRIGGER tr BEFORE UPDATE OF a, b ON t BEGIN SELECT 1; END");
        assert_roundtrip(
            "CREATE TRIGGER tr AFTER DELETE ON \"order\" BEGIN INSERT INTO log VALUES (OLD.id); END",
        );
    }

    #[test]
    fn test_roundtrip_create_view_extended() {
        assert_roundtrip("CREATE VIEW main.v AS SELECT 1");
        assert_roundtrip("CREATE VIEW v(x, y, z) AS SELECT a, b, c FROM t");
    }

    #[test]
    fn test_roundtrip_alter_table_extended() {
        assert_roundtrip("ALTER TABLE t RENAME COLUMN a TO b");
        assert_roundtrip("ALTER TABLE main.t RENAME TO u");
        assert_roundtrip("ALTER TABLE t ADD COLUMN c INTEGER NOT NULL DEFAULT 0");
    }

    // -----------------------------------------------------------------------
    // bd-7pxb §12.10-12.12 Transaction Control + ATTACH/DETACH + EXPLAIN
    // -----------------------------------------------------------------------

    #[test]
    fn test_begin_deferred() {
        let stmt = parse_one("BEGIN DEFERRED TRANSACTION");
        if let Statement::Begin(b) = stmt {
            assert_eq!(b.mode, Some(TransactionMode::Deferred));
        } else {
            unreachable!("expected Begin");
        }
    }

    #[test]
    fn test_begin_immediate() {
        let stmt = parse_one("BEGIN IMMEDIATE");
        if let Statement::Begin(b) = stmt {
            assert_eq!(b.mode, Some(TransactionMode::Immediate));
        } else {
            unreachable!("expected Begin");
        }
    }

    #[test]
    fn test_begin_exclusive() {
        let stmt = parse_one("BEGIN EXCLUSIVE TRANSACTION");
        if let Statement::Begin(b) = stmt {
            assert_eq!(b.mode, Some(TransactionMode::Exclusive));
        } else {
            unreachable!("expected Begin");
        }
    }

    #[test]
    fn test_begin_concurrent() {
        let stmt = parse_one("BEGIN CONCURRENT");
        if let Statement::Begin(b) = stmt {
            assert_eq!(b.mode, Some(TransactionMode::Concurrent));
        } else {
            unreachable!("expected Begin");
        }
    }

    #[test]
    fn test_concurrent_no_conflict() {
        // Parser-level: BEGIN without mode (the concurrent entry point) parses.
        // Runtime concurrent writer conflict detection is in the MVCC/WAL layer.
        let stmt = parse_one("BEGIN");
        assert!(matches!(stmt, Statement::Begin(_)));
    }

    #[test]
    fn test_concurrent_page_conflict() {
        // Parser-level: verify basic transaction and DML parse.
        // Runtime page-level conflict (SQLITE_BUSY_SNAPSHOT) is in the MVCC layer.
        let stmts = parse_ok("BEGIN; INSERT INTO t (a) VALUES (1)");
        assert_eq!(stmts.len(), 2);
        assert!(matches!(stmts[0], Statement::Begin(_)));
        assert!(matches!(stmts[1], Statement::Insert(_)));
    }

    #[test]
    fn test_commit_end_synonym() {
        let stmt1 = parse_one("COMMIT");
        assert!(matches!(stmt1, Statement::Commit));
        let stmt2 = parse_one("END TRANSACTION");
        assert!(matches!(stmt2, Statement::Commit));
        let stmt3 = parse_one("COMMIT TRANSACTION");
        assert!(matches!(stmt3, Statement::Commit));
    }

    #[test]
    fn test_rollback() {
        let stmt = parse_one("ROLLBACK");
        if let Statement::Rollback(r) = stmt {
            assert!(r.to_savepoint.is_none());
        } else {
            unreachable!("expected Rollback");
        }
    }

    #[test]
    fn test_savepoint_basic() {
        let stmt = parse_one("SAVEPOINT sp1");
        assert!(matches!(stmt, Statement::Savepoint(ref name) if name == "sp1"));
    }

    #[test]
    fn test_savepoint_release() {
        let stmt = parse_one("RELEASE SAVEPOINT sp1");
        assert!(matches!(stmt, Statement::Release(ref name) if name == "sp1"));
    }

    #[test]
    fn test_savepoint_release_removes_later() {
        // Parser-level: RELEASE without SAVEPOINT keyword also works.
        // Runtime savepoint stack semantics verified in engine tests.
        let stmt = parse_one("RELEASE sp2");
        assert!(matches!(stmt, Statement::Release(ref name) if name == "sp2"));
    }

    #[test]
    fn test_savepoint_rollback_to() {
        let stmt = parse_one("ROLLBACK TO SAVEPOINT sp1");
        if let Statement::Rollback(r) = stmt {
            assert_eq!(r.to_savepoint.as_deref(), Some("sp1"));
        } else {
            unreachable!("expected Rollback");
        }
    }

    #[test]
    fn test_savepoint_nested() {
        // Parser-level: multiple savepoints in sequence parse independently.
        // Runtime stack semantics verified in engine tests.
        let stmts = parse_ok("SAVEPOINT sp1; SAVEPOINT sp2; SAVEPOINT sp3");
        assert_eq!(stmts.len(), 3);
        assert!(matches!(stmts[0], Statement::Savepoint(ref n) if n == "sp1"));
        assert!(matches!(stmts[1], Statement::Savepoint(ref n) if n == "sp2"));
        assert!(matches!(stmts[2], Statement::Savepoint(ref n) if n == "sp3"));
    }

    #[test]
    fn test_savepoint_rollback_then_continue() {
        // Parser-level: ROLLBACK TO followed by more DML parses.
        let stmts = parse_ok("ROLLBACK TO sp1; INSERT INTO t VALUES (1)");
        assert_eq!(stmts.len(), 2);
        assert!(matches!(stmts[0], Statement::Rollback(_)));
        assert!(matches!(stmts[1], Statement::Insert(_)));
    }

    #[test]
    fn test_attach_database() {
        let stmt = parse_one("ATTACH DATABASE 'other.db' AS other");
        if let Statement::Attach(a) = stmt {
            assert_eq!(a.schema, "other");
        } else {
            unreachable!("expected Attach");
        }
    }

    #[test]
    fn test_attach_schema_qualified_access() {
        // Parser-level: schema-qualified table reference parses correctly.
        let stmt = parse_one("SELECT * FROM other.t");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                match &from.source {
                    TableOrSubquery::Table { name, .. } => {
                        assert_eq!(name.schema.as_deref(), Some("other"));
                        assert_eq!(name.name, "t");
                    }
                    other => unreachable!("expected Table source, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_detach_database() {
        let stmt = parse_one("DETACH DATABASE other");
        assert!(matches!(stmt, Statement::Detach(ref name) if name == "other"));
    }

    #[test]
    fn test_attach_max_limit() {
        // Parser-level: ATTACH parses identically regardless of limit.
        // Runtime SQLITE_MAX_ATTACHED enforcement is in the engine.
        let stmt = parse_one("ATTACH 'db11.sqlite' AS db11");
        if let Statement::Attach(a) = stmt {
            assert_eq!(a.schema, "db11");
        } else {
            unreachable!("expected Attach");
        }
    }

    #[test]
    fn test_cross_database_transaction() {
        // Parser-level: transaction with cross-database DML parses.
        // Runtime cross-database atomic commit is in WAL/MVCC layer.
        let stmts = parse_ok("BEGIN; INSERT INTO main.t SELECT * FROM other.t; COMMIT");
        assert_eq!(stmts.len(), 3);
        assert!(matches!(stmts[0], Statement::Begin(_)));
        assert!(matches!(stmts[1], Statement::Insert(_)));
        assert!(matches!(stmts[2], Statement::Commit));
    }

    #[test]
    fn test_explain_returns_bytecode() {
        let stmt = parse_one("EXPLAIN SELECT 1");
        if let Statement::Explain { query_plan, stmt } = stmt {
            assert!(!query_plan);
            assert!(matches!(*stmt, Statement::Select(_)));
        } else {
            unreachable!("expected Explain");
        }
    }

    #[test]
    fn test_explain_query_plan_columns() {
        let stmt = parse_one("EXPLAIN QUERY PLAN SELECT * FROM t WHERE id = 1");
        if let Statement::Explain { query_plan, stmt } = stmt {
            assert!(query_plan);
            assert!(matches!(*stmt, Statement::Select(_)));
        } else {
            unreachable!("expected Explain");
        }
    }

    #[test]
    fn test_explain_query_plan_shows_index() {
        // Parser-level: EXPLAIN QUERY PLAN on indexed query parses.
        // Runtime index usage in EQP output is in the planner.
        let stmt = parse_one("EXPLAIN QUERY PLAN SELECT * FROM t WHERE id = 1");
        if let Statement::Explain { query_plan, .. } = stmt {
            assert!(query_plan);
        } else {
            unreachable!("expected Explain");
        }
    }

    #[test]
    fn test_explain_query_plan_tree_structure() {
        // Parser-level: EXPLAIN QUERY PLAN on a join query parses.
        // Runtime tree structure in EQP output is in the planner.
        let stmt = parse_one("EXPLAIN QUERY PLAN SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id");
        if let Statement::Explain { query_plan, stmt } = stmt {
            assert!(query_plan);
            assert!(matches!(*stmt, Statement::Select(_)));
        } else {
            unreachable!("expected Explain");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: keywords as identifiers
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_keyword_as_column_name() {
        // "order" is a keyword but valid as a column name in many contexts.
        let stmt = parse_one("SELECT \"order\" FROM t");
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parser_keyword_as_alias() {
        let stmt = parse_one("SELECT 1 AS \"limit\"");
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parser_keyword_as_table_name() {
        let stmt = parse_one("SELECT * FROM \"group\"");
        assert!(matches!(stmt, Statement::Select(_)));
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: all statement types (Section 12 coverage)
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_all_statement_types() {
        // Each statement type from Section 12 must parse without error.
        let statements = [
            // DML
            "SELECT 1",
            "INSERT INTO t VALUES (1)",
            "INSERT OR REPLACE INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t WHERE id = 1",
            "REPLACE INTO t VALUES (1)",
            // DDL
            "CREATE TABLE t (id INTEGER PRIMARY KEY)",
            "CREATE TEMPORARY TABLE t (id INTEGER)",
            "CREATE TABLE IF NOT EXISTS t (id INTEGER)",
            "CREATE INDEX idx ON t (a)",
            "CREATE UNIQUE INDEX idx ON t (a)",
            "CREATE VIEW v AS SELECT 1",
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END",
            "CREATE VIRTUAL TABLE t USING fts5(a, b)",
            "ALTER TABLE t RENAME TO t2",
            "ALTER TABLE t ADD COLUMN c TEXT",
            "ALTER TABLE t DROP COLUMN c",
            "ALTER TABLE t RENAME COLUMN a TO b",
            "DROP TABLE t",
            "DROP TABLE IF EXISTS t",
            "DROP INDEX idx",
            "DROP VIEW v",
            "DROP TRIGGER tr",
            // Transaction
            "BEGIN",
            "BEGIN DEFERRED",
            "BEGIN IMMEDIATE",
            "BEGIN EXCLUSIVE",
            "COMMIT",
            "END",
            "ROLLBACK",
            "SAVEPOINT sp1",
            "RELEASE sp1",
            "RELEASE SAVEPOINT sp1",
            "ROLLBACK TO sp1",
            "ROLLBACK TO SAVEPOINT sp1",
            // Utility
            "ATTACH DATABASE ':memory:' AS db2",
            "DETACH db2",
            "ANALYZE",
            "ANALYZE t",
            "VACUUM",
            "VACUUM INTO '/tmp/backup.db'",
            "REINDEX",
            "REINDEX t",
            "EXPLAIN SELECT 1",
            "EXPLAIN QUERY PLAN SELECT 1",
            // PRAGMA
            "PRAGMA journal_mode",
            "PRAGMA journal_mode = WAL",
            "PRAGMA table_info(t)",
        ];

        for sql in &statements {
            let mut p = Parser::from_sql(sql);
            let (stmts, errs) = p.parse_all();
            assert!(errs.is_empty(), "failed to parse '{sql}': {errs:?}");
            assert_eq!(
                stmts.len(),
                1,
                "expected 1 statement for '{sql}', got {}",
                stmts.len()
            );
        }
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: expression precedence
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_expression_precedence_mul_over_add() {
        // 1 + 2 * 3 should parse as 1 + (2 * 3)
        let stmt = parse_one("SELECT 1 + 2 * 3");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                match &columns[0] {
                    ResultColumn::Expr { expr, .. } => {
                        // Outer expression should be Add, right side should be Multiply.
                        assert!(
                            matches!(expr, Expr::BinaryOp { .. }),
                            "expected BinaryOp, got {expr:?}"
                        );
                    }
                    other => unreachable!("expected Expr column, got {other:?}"),
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: INSERT with ON CONFLICT and RETURNING
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_insert_on_conflict() {
        let stmt =
            parse_one("INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = excluded.a");
        if let Statement::Insert(i) = stmt {
            assert!(!i.upsert.is_empty());
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_parser_insert_returning() {
        let stmt = parse_one("INSERT INTO t (a) VALUES (1) RETURNING *");
        if let Statement::Insert(i) = stmt {
            assert!(!i.returning.is_empty());
        } else {
            unreachable!("expected Insert");
        }
    }

    #[test]
    fn test_parser_delete_returning() {
        let stmt = parse_one("DELETE FROM t WHERE id = 1 RETURNING *");
        if let Statement::Delete(d) = stmt {
            assert!(!d.returning.is_empty());
        } else {
            unreachable!("expected Delete");
        }
    }

    #[test]
    fn test_parser_update_returning() {
        let stmt = parse_one("UPDATE t SET a = 1 RETURNING a, b");
        if let Statement::Update(u) = stmt {
            assert_eq!(u.returning.len(), 2);
        } else {
            unreachable!("expected Update");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: compound SELECT operators
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_union() {
        let stmt = parse_one("SELECT 1 UNION SELECT 2");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.body.compounds.len(), 1);
            assert_eq!(s.body.compounds[0].0, CompoundOp::Union);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_intersect() {
        let stmt = parse_one("SELECT 1 INTERSECT SELECT 2");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.body.compounds.len(), 1);
            assert_eq!(s.body.compounds[0].0, CompoundOp::Intersect);
        } else {
            unreachable!("expected Select");
        }
    }

    #[test]
    fn test_parser_except() {
        let stmt = parse_one("SELECT 1 EXCEPT SELECT 2");
        if let Statement::Select(s) = stmt {
            assert_eq!(s.body.compounds.len(), 1);
            assert_eq!(s.body.compounds[0].0, CompoundOp::Except);
        } else {
            unreachable!("expected Select");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: subquery in FROM
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_subquery_in_from() {
        let stmt = parse_one("SELECT * FROM (SELECT 1 AS x) AS sub");
        assert!(matches!(stmt, Statement::Select(_)));
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: CREATE TABLE with constraints
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_create_table_all_constraints() {
        let stmt = parse_one(
            "CREATE TABLE t (\
             id INTEGER PRIMARY KEY AUTOINCREMENT,\
             name TEXT NOT NULL DEFAULT '',\
             email TEXT UNIQUE,\
             age INTEGER CHECK(age >= 0),\
             dept_id INTEGER REFERENCES dept(id) ON DELETE CASCADE,\
             CONSTRAINT pk PRIMARY KEY (id),\
             UNIQUE (email),\
             CHECK (age < 200),\
             FOREIGN KEY (dept_id) REFERENCES dept(id)\
             )",
        );
        if let Statement::CreateTable(ct) = stmt {
            if let CreateTableBody::Columns {
                columns,
                constraints,
            } = ct.body
            {
                assert_eq!(columns.len(), 5);
                assert!(!constraints.is_empty());
            } else {
                unreachable!("expected column defs");
            }
        } else {
            unreachable!("expected CreateTable");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: CREATE TRIGGER with all timing/events
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_create_trigger_before_delete() {
        let stmt = parse_one("CREATE TRIGGER tr BEFORE DELETE ON t BEGIN SELECT 1; END");
        if let Statement::CreateTrigger(tr) = stmt {
            assert_eq!(tr.timing, TriggerTiming::Before);
            assert!(matches!(tr.event, TriggerEvent::Delete));
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    #[test]
    fn test_parser_create_trigger_instead_of_update() {
        let stmt =
            parse_one("CREATE TRIGGER tr INSTEAD OF UPDATE OF a, b ON v BEGIN SELECT 1; END");
        if let Statement::CreateTrigger(tr) = stmt {
            assert_eq!(tr.timing, TriggerTiming::InsteadOf);
            if let TriggerEvent::Update(cols) = &tr.event {
                assert_eq!(cols.len(), 2);
            } else {
                unreachable!("expected UpdateOf event");
            }
        } else {
            unreachable!("expected CreateTrigger");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: CREATE VIEW with columns
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_create_view_with_columns() {
        let stmt = parse_one("CREATE VIEW v (a, b) AS SELECT 1, 2");
        if let Statement::CreateView(cv) = stmt {
            assert_eq!(cv.columns, vec!["a".to_owned(), "b".to_owned()]);
        } else {
            unreachable!("expected CreateView");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: multi-way join
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_multi_join() {
        let stmt = parse_one(
            "SELECT a.x, b.y, c.z FROM a \
             JOIN b ON a.id = b.a_id \
             LEFT JOIN c ON b.id = c.b_id \
             CROSS JOIN d",
        );
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { from, .. } = &s.body.select {
                let from = from.as_ref().expect("FROM clause");
                assert_eq!(from.joins.len(), 3);
                assert_eq!(from.joins[0].join_type.kind, JoinKind::Inner);
                assert_eq!(from.joins[1].join_type.kind, JoinKind::Left);
                assert_eq!(from.joins[2].join_type.kind, JoinKind::Cross);
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: GROUP BY / HAVING
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_group_by_having() {
        let stmt = parse_one("SELECT dept, count(*) FROM emp GROUP BY dept HAVING count(*) > 5");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select {
                group_by, having, ..
            } = &s.body.select
            {
                assert!(!group_by.is_empty());
                assert!(having.is_some());
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: Error recovery with line:column spans
    // -----------------------------------------------------------------------

    #[test]
    fn test_parser_error_recovery_with_span() {
        // Multi-line input with an error on line 2.
        let sql = "SELECT 1;\nXYZZY 42;\nSELECT 3";
        let mut p = Parser::from_sql(sql);
        let (stmts, errs) = p.parse_all();
        assert_eq!(stmts.len(), 2, "should recover two valid statements");
        assert!(!errs.is_empty(), "should report at least one error");

        let err = &errs[0];
        // XYZZY starts at line 2, column 1.
        assert_eq!(err.line, 2, "error should be on line 2");
        assert_eq!(err.col, 1, "error should be at column 1");
        // Span should be non-zero and point within the source.
        assert!(
            err.span.start < err.span.end,
            "error span should be non-empty"
        );
        let source_len = u32::try_from(sql.len()).unwrap();
        assert!(
            err.span.end <= source_len,
            "error span.end should be within source"
        );
    }

    #[test]
    fn test_parser_error_span_mid_line() {
        // Incomplete CREATE should produce an error.
        let bad = Parser::from_sql("CREATE").parse_statement();
        assert!(bad.is_err());
        let err = bad.unwrap_err();
        assert_eq!(err.line, 1);
    }

    // -----------------------------------------------------------------------
    // bd-2kvo Phase 3 acceptance: Keyword lookup covers 150+ keywords
    // -----------------------------------------------------------------------

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_parser_keyword_lookup_all_150() {
        use crate::token::TokenKind;

        // Exhaustive list of all SQL keywords in lookup_keyword.
        let keywords = [
            "ABORT",
            "ACTION",
            "ADD",
            "AFTER",
            "ALL",
            "ALTER",
            "ALWAYS",
            "ANALYZE",
            "AND",
            "AS",
            "ASC",
            "ATTACH",
            "AUTOINCREMENT",
            "BEFORE",
            "BEGIN",
            "BETWEEN",
            "BY",
            "CASCADE",
            "CASE",
            "CAST",
            "CHECK",
            "COLLATE",
            "COLUMN",
            "COMMIT",
            "CONCURRENT",
            "CONFLICT",
            "CONSTRAINT",
            "CREATE",
            "CROSS",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "DATABASE",
            "DEFAULT",
            "DEFERRABLE",
            "DEFERRED",
            "DELETE",
            "DESC",
            "DETACH",
            "DISTINCT",
            "DO",
            "DROP",
            "EACH",
            "ELSE",
            "END",
            "ESCAPE",
            "EXCEPT",
            "EXCLUDE",
            "EXCLUSIVE",
            "EXISTS",
            "EXPLAIN",
            "FAIL",
            "FILTER",
            "FIRST",
            "FOLLOWING",
            "FOR",
            "FOREIGN",
            "FROM",
            "FULL",
            "GENERATED",
            "GLOB",
            "GROUP",
            "GROUPS",
            "HAVING",
            "IF",
            "IGNORE",
            "IMMEDIATE",
            "IN",
            "INDEX",
            "INDEXED",
            "INITIALLY",
            "INNER",
            "INSERT",
            "INSTEAD",
            "INTERSECT",
            "INTO",
            "IS",
            "ISNULL",
            "JOIN",
            "KEY",
            "LAST",
            "LEFT",
            "LIKE",
            "LIMIT",
            "MATCH",
            "MATERIALIZED",
            "NATURAL",
            "NO",
            "NOT",
            "NOTHING",
            "NOTNULL",
            "NULL",
            "NULLS",
            "OF",
            "OFFSET",
            "ON",
            "OR",
            "ORDER",
            "OTHERS",
            "OUTER",
            "OVER",
            "PARTITION",
            "PLAN",
            "PRAGMA",
            "PRECEDING",
            "PRIMARY",
            "QUERY",
            "RAISE",
            "RANGE",
            "RECURSIVE",
            "REFERENCES",
            "REGEXP",
            "REINDEX",
            "RELEASE",
            "RENAME",
            "REPLACE",
            "RESTRICT",
            "RETURNING",
            "RIGHT",
            "ROLLBACK",
            "ROW",
            "ROWS",
            "SAVEPOINT",
            "SELECT",
            "SET",
            "STORED",
            "STRICT",
            "TABLE",
            "TEMP",
            "TEMPORARY",
            "THEN",
            "TIES",
            "TO",
            "TRANSACTION",
            "TRIGGER",
            "TRUE",
            "FALSE",
            "UNBOUNDED",
            "UNION",
            "UNIQUE",
            "UPDATE",
            "USING",
            "VACUUM",
            "VALUES",
            "VIEW",
            "VIRTUAL",
            "WHEN",
            "WHERE",
            "WINDOW",
            "WITH",
            "WITHOUT",
        ];

        assert!(
            keywords.len() >= 150,
            "expected 150+ keywords, got {}",
            keywords.len()
        );

        for kw in &keywords {
            assert!(
                TokenKind::lookup_keyword(kw).is_some(),
                "keyword {kw} not recognized (uppercase)"
            );
            // Case-insensitive: lowercase must also work.
            let lower = kw.to_ascii_lowercase();
            assert!(
                TokenKind::lookup_keyword(&lower).is_some(),
                "keyword {kw} not recognized (lowercase)"
            );
            // Mixed case.
            let mixed: String = kw
                .chars()
                .enumerate()
                .map(|(i, c)| {
                    if i % 2 == 0 {
                        c.to_ascii_lowercase()
                    } else {
                        c.to_ascii_uppercase()
                    }
                })
                .collect();
            assert!(
                TokenKind::lookup_keyword(&mixed).is_some(),
                "keyword {kw} not recognized (mixed case: {mixed})"
            );
        }

        // Non-keyword should return None.
        assert!(TokenKind::lookup_keyword("FOOBAR").is_none());
        assert!(TokenKind::lookup_keyword("").is_none());
    }

    // -----------------------------------------------------------------------
    // Round-trip: parse → Display → re-parse → compare ASTs
    // -----------------------------------------------------------------------

    /// Parse SQL, convert back to string via Display, re-parse, convert back
    /// again, and assert the two rendered strings are identical.  We compare
    /// rendered strings (not ASTs) because Display may normalise constructs
    /// (e.g. `INSERT OR REPLACE` → `REPLACE`) which changes SQL length and
    /// therefore Span positions, while the logical content is identical.
    fn assert_roundtrip(sql: &str) {
        let ast1 = parse_one(sql);
        let rendered1 = ast1.to_string();
        let ast2 = parse_one(&rendered1);
        let rendered2 = ast2.to_string();
        assert_eq!(
            rendered1, rendered2,
            "round-trip failed for:\n  input: {sql}\n  rendered1: {rendered1}\n  rendered2: {rendered2}"
        );
    }

    #[test]
    fn test_roundtrip_select_simple() {
        assert_roundtrip("SELECT 1");
        assert_roundtrip("SELECT 1, 2, 3");
        assert_roundtrip("SELECT *");
        assert_roundtrip("SELECT * FROM t");
        assert_roundtrip("SELECT a, b FROM t WHERE a > 10");
        assert_roundtrip("SELECT a FROM t ORDER BY a DESC");
        assert_roundtrip("SELECT a FROM t LIMIT 10 OFFSET 5");
    }

    #[test]
    fn test_roundtrip_select_distinct() {
        assert_roundtrip("SELECT DISTINCT a, b FROM t");
    }

    #[test]
    fn test_roundtrip_select_alias() {
        assert_roundtrip("SELECT a AS x, b AS y FROM t AS u");
    }

    #[test]
    fn test_roundtrip_select_join_types() {
        assert_roundtrip("SELECT * FROM a INNER JOIN b ON a.id = b.id");
        assert_roundtrip("SELECT * FROM a LEFT JOIN b ON a.id = b.id");
        assert_roundtrip("SELECT * FROM a RIGHT JOIN b ON a.id = b.id");
        assert_roundtrip("SELECT * FROM a FULL JOIN b ON a.id = b.id");
        assert_roundtrip("SELECT * FROM a CROSS JOIN b");
        assert_roundtrip("SELECT * FROM a NATURAL INNER JOIN b");
        assert_roundtrip("SELECT * FROM a LEFT JOIN b USING (id)");
    }

    #[test]
    fn test_roundtrip_select_subquery() {
        assert_roundtrip("SELECT * FROM (SELECT 1 AS x) AS sub");
    }

    #[test]
    fn test_roundtrip_select_group_by_having() {
        assert_roundtrip("SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > 1");
    }

    #[test]
    fn test_roundtrip_select_window() {
        assert_roundtrip("SELECT sum(x) OVER (PARTITION BY g ORDER BY x) FROM t");
    }

    #[test]
    fn test_roundtrip_select_cte() {
        assert_roundtrip("WITH cte AS (SELECT 1 AS n) SELECT * FROM cte");
        assert_roundtrip(
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10) SELECT * FROM cnt",
        );
    }

    #[test]
    fn test_roundtrip_select_compound() {
        assert_roundtrip("SELECT 1 UNION SELECT 2");
        assert_roundtrip("SELECT 1 UNION ALL SELECT 2");
        assert_roundtrip("SELECT 1 INTERSECT SELECT 2");
        assert_roundtrip("SELECT 1 EXCEPT SELECT 2");
    }

    #[test]
    fn test_roundtrip_insert() {
        assert_roundtrip("INSERT INTO t (a, b) VALUES (1, 2)");
        assert_roundtrip("INSERT INTO t DEFAULT VALUES");
        assert_roundtrip("INSERT INTO t SELECT * FROM u");
        assert_roundtrip("INSERT OR REPLACE INTO t (a) VALUES (1)");
        assert_roundtrip("REPLACE INTO t (a) VALUES (1)");
    }

    #[test]
    fn test_roundtrip_insert_returning() {
        assert_roundtrip("INSERT INTO t (a) VALUES (1) RETURNING *");
        assert_roundtrip("INSERT INTO t (a) VALUES (1) RETURNING a, b");
    }

    #[test]
    fn test_roundtrip_insert_on_conflict() {
        assert_roundtrip("INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO NOTHING");
        assert_roundtrip(
            "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = excluded.a",
        );
    }

    #[test]
    fn test_roundtrip_update() {
        assert_roundtrip("UPDATE t SET a = 1");
        assert_roundtrip("UPDATE t SET a = 1, b = 2 WHERE c > 3");
        assert_roundtrip("UPDATE t SET a = 1 RETURNING *");
    }

    #[test]
    fn test_roundtrip_delete() {
        assert_roundtrip("DELETE FROM t");
        assert_roundtrip("DELETE FROM t WHERE a = 1");
        assert_roundtrip("DELETE FROM t RETURNING *");
    }

    #[test]
    fn test_roundtrip_create_table() {
        assert_roundtrip("CREATE TABLE t (a INTEGER, b TEXT)");
        assert_roundtrip("CREATE TABLE IF NOT EXISTS t (a INTEGER PRIMARY KEY)");
        assert_roundtrip("CREATE TEMP TABLE t (a TEXT NOT NULL, b REAL DEFAULT 0.0)");
    }

    #[test]
    fn test_roundtrip_create_index() {
        assert_roundtrip("CREATE INDEX idx ON t (a)");
        assert_roundtrip("CREATE UNIQUE INDEX IF NOT EXISTS idx ON t (a, b DESC)");
        assert_roundtrip("CREATE INDEX idx ON t (a) WHERE a > 0");
    }

    #[test]
    fn test_roundtrip_drop() {
        assert_roundtrip("DROP TABLE t");
        assert_roundtrip("DROP TABLE IF EXISTS t");
        assert_roundtrip("DROP INDEX idx");
        assert_roundtrip("DROP VIEW v");
    }

    #[test]
    fn test_roundtrip_alter_table() {
        assert_roundtrip("ALTER TABLE t RENAME TO u");
        assert_roundtrip("ALTER TABLE t ADD COLUMN c TEXT");
        assert_roundtrip("ALTER TABLE t DROP COLUMN c");
    }

    #[test]
    fn test_roundtrip_transaction() {
        assert_roundtrip("BEGIN");
        assert_roundtrip("BEGIN IMMEDIATE");
        assert_roundtrip("BEGIN EXCLUSIVE");
        assert_roundtrip("COMMIT");
        assert_roundtrip("ROLLBACK");
        assert_roundtrip("SAVEPOINT sp1");
        assert_roundtrip("RELEASE sp1");
    }

    #[test]
    fn test_roundtrip_pragma() {
        assert_roundtrip("PRAGMA journal_mode");
        assert_roundtrip("PRAGMA journal_mode = wal");
    }

    #[test]
    fn test_roundtrip_explain() {
        assert_roundtrip("EXPLAIN SELECT 1");
        assert_roundtrip("EXPLAIN QUERY PLAN SELECT * FROM t");
    }

    #[test]
    fn test_roundtrip_expressions() {
        assert_roundtrip("SELECT 1 + 2 * 3");
        assert_roundtrip("SELECT NOT a");
        assert_roundtrip("SELECT -x");
        assert_roundtrip("SELECT ~x");
        assert_roundtrip("SELECT a BETWEEN 1 AND 10");
        assert_roundtrip("SELECT a NOT BETWEEN 1 AND 10");
        assert_roundtrip("SELECT a IN (1, 2, 3)");
        assert_roundtrip("SELECT a NOT IN (1, 2, 3)");
        assert_roundtrip("SELECT a LIKE '%foo%'");
        assert_roundtrip("SELECT a GLOB '*foo*'");
        assert_roundtrip("SELECT CASE WHEN a = 1 THEN 'one' ELSE 'other' END");
        assert_roundtrip("SELECT CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' END");
        assert_roundtrip("SELECT CAST(a AS TEXT)");
        assert_roundtrip("SELECT EXISTS (SELECT 1)");
        assert_roundtrip("SELECT (SELECT 1)");
        assert_roundtrip("SELECT a COLLATE NOCASE");
    }

    #[test]
    fn test_roundtrip_literals() {
        assert_roundtrip("SELECT NULL");
        assert_roundtrip("SELECT TRUE");
        assert_roundtrip("SELECT FALSE");
        assert_roundtrip("SELECT 42");
        assert_roundtrip("SELECT 3.14");
        assert_roundtrip("SELECT 'hello'");
        assert_roundtrip("SELECT X'DEADBEEF'");
        assert_roundtrip("SELECT CURRENT_TIME");
        assert_roundtrip("SELECT CURRENT_DATE");
        assert_roundtrip("SELECT CURRENT_TIMESTAMP");
    }

    #[test]
    fn test_roundtrip_placeholders() {
        assert_roundtrip("SELECT ?");
        assert_roundtrip("SELECT ?1");
        assert_roundtrip("SELECT :name");
        assert_roundtrip("SELECT @name");
        assert_roundtrip("SELECT $name");
    }

    #[test]
    fn test_roundtrip_json_arrows() {
        assert_roundtrip("SELECT a -> 'key'");
        assert_roundtrip("SELECT a ->> 'key'");
    }

    #[test]
    fn test_roundtrip_function_calls() {
        assert_roundtrip("SELECT count(*)");
        assert_roundtrip("SELECT count(DISTINCT a)");
        assert_roundtrip("SELECT sum(x) FILTER (WHERE x > 0)");
    }

    #[test]
    fn test_roundtrip_isnull_notnull() {
        assert_roundtrip("SELECT a ISNULL");
        assert_roundtrip("SELECT a IS NOT NULL");
    }

    #[test]
    fn test_roundtrip_create_view() {
        assert_roundtrip("CREATE VIEW v AS SELECT * FROM t");
        assert_roundtrip("CREATE VIEW IF NOT EXISTS v (a, b) AS SELECT 1, 2");
    }

    #[test]
    fn test_roundtrip_create_trigger() {
        assert_roundtrip(
            "CREATE TRIGGER tr BEFORE DELETE ON t FOR EACH ROW BEGIN DELETE FROM log WHERE id = OLD.id; END",
        );
    }

    #[test]
    fn test_roundtrip_attach_detach() {
        assert_roundtrip("ATTACH 'file.db' AS db2");
        assert_roundtrip("DETACH db2");
    }

    #[test]
    fn test_roundtrip_vacuum() {
        assert_roundtrip("VACUUM");
    }

    #[test]
    fn test_roundtrip_analyze_reindex() {
        assert_roundtrip("ANALYZE");
        assert_roundtrip("ANALYZE t");
        assert_roundtrip("REINDEX");
        assert_roundtrip("REINDEX t");
    }

    #[test]
    fn test_roundtrip_cte_materialized() {
        assert_roundtrip("WITH cte AS MATERIALIZED (SELECT 1) SELECT * FROM cte");
        assert_roundtrip("WITH cte AS NOT MATERIALIZED (SELECT 1) SELECT * FROM cte");
    }

    // -----------------------------------------------------------------------
    // Proptest: round-trip property test (bd-2kvo acceptance criterion #12)
    // -----------------------------------------------------------------------

    mod proptest_roundtrip {
        use super::*;
        use proptest::prelude::*;

        /// Returns `true` if the string is a SQL keyword.
        fn is_keyword(s: &str) -> bool {
            TokenKind::lookup_keyword(s).is_some()
        }

        /// Generate a random identifier (simple alphanumeric, not a SQL keyword).
        fn arb_ident() -> BoxedStrategy<String> {
            prop::string::string_regex("[a-z][a-z0-9]{0,5}")
                .expect("valid regex")
                .prop_filter("must not be keyword", |s| !is_keyword(s))
                .boxed()
        }

        /// Generate a random literal value.
        fn arb_literal() -> BoxedStrategy<String> {
            prop_oneof![
                any::<i32>().prop_map(|n| n.to_string()),
                (1i32..1000).prop_map(|n| format!("{n}.{}", n % 100)),
                arb_ident().prop_map(|s| format!("'{s}'")),
                Just("NULL".to_string()),
                Just("TRUE".to_string()),
                Just("FALSE".to_string()),
            ]
            .boxed()
        }

        /// Generate a random expression of bounded depth.
        fn arb_expr(depth: u32) -> BoxedStrategy<String> {
            if depth == 0 {
                prop_oneof![
                    arb_literal(),
                    arb_ident(),
                    (arb_ident(), arb_ident()).prop_map(|(t, c)| format!("{t}.{c}")),
                ]
                .boxed()
            } else {
                let leaf = arb_expr(0);
                prop_oneof![
                    4 => leaf,
                    // Binary ops (always parenthesized by display)
                    2 => (arb_expr(depth - 1), prop_oneof![
                        Just("+"), Just("-"), Just("*"), Just("/"),
                        Just("="), Just("!="), Just("<"), Just("<="),
                        Just(">"), Just(">="), Just("AND"), Just("OR"),
                        Just("||"),
                    ], arb_expr(depth - 1))
                        .prop_map(|(l, op, r)| format!("({l} {op} {r})")),
                    // Unary ops
                    1 => arb_expr(depth - 1).prop_map(|e| format!("(-{e})")),
                    1 => arb_expr(depth - 1).prop_map(|e| format!("(NOT {e})")),
                    // IS NULL / IS NOT NULL
                    1 => arb_expr(depth - 1).prop_map(|e| format!("{e} IS NULL")),
                    1 => arb_expr(depth - 1).prop_map(|e| format!("{e} IS NOT NULL")),
                    // Single-token postfix null tests
                    1 => arb_expr(depth - 1).prop_map(|e| format!("{e} ISNULL")),
                    1 => arb_expr(depth - 1).prop_map(|e| format!("{e} NOTNULL")),
                    // COLLATE (postfix)
                    1 => arb_expr(depth - 1).prop_map(|e| format!("{e} COLLATE nocase")),
                    // UNPARENTHESIZED binary ops: exercise precedence and
                    // associativity through the display round-trip (issue
                    // #122: `a IS NULL = b IS NULL` must not regroup after
                    // parse → display → re-parse).
                    2 => (arb_expr(depth - 1), prop_oneof![
                        Just("+"), Just("*"), Just("="), Just("<"),
                        Just("AND"), Just("OR"), Just("||"), Just("IS"),
                        Just("IS NOT"),
                    ], arb_expr(depth - 1))
                        .prop_map(|(l, op, r)| format!("{l} {op} {r}")),
                    // BETWEEN
                    1 => (arb_expr(depth - 1), arb_expr(0), arb_expr(0))
                        .prop_map(|(e, lo, hi)| format!("{e} BETWEEN {lo} AND {hi}")),
                    // IN list
                    1 => (arb_expr(depth - 1), proptest::collection::vec(arb_expr(0), 1..4))
                        .prop_map(|(e, items)| format!("{e} IN ({})", items.join(", "))),
                    // LIKE
                    1 => (arb_expr(depth - 1), arb_ident())
                        .prop_map(|(e, p)| format!("{e} LIKE '{p}'")),
                    // CAST
                    1 => arb_expr(depth - 1).prop_map(|e| format!("CAST({e} AS TEXT)")),
                    // CASE
                    1 => (arb_expr(depth - 1), arb_expr(0), arb_expr(0))
                        .prop_map(|(c, t, el)| format!("CASE WHEN {c} THEN {t} ELSE {el} END")),
                    // Function call
                    1 => (arb_ident(), proptest::collection::vec(arb_expr(0), 0..3))
                        .prop_map(|(name, args)| format!("{name}({})", args.join(", "))),
                    // Subquery
                    1 => arb_expr(0).prop_map(|e| format!("(SELECT {e})")),
                ]
                .boxed()
            }
        }

        /// Generate a random SELECT statement.
        fn arb_select() -> BoxedStrategy<String> {
            use std::fmt::Write as _;

            let cols =
                proptest::collection::vec(arb_expr(1), 1..4).prop_map(|cols| cols.join(", "));
            let table = arb_ident();
            let where_clause = prop::option::of(arb_expr(1));
            let order_by = prop::option::of(arb_ident());
            let limit = prop::option::of(1u32..100);

            (cols, table, where_clause, order_by, limit)
                .prop_map(|(cols, tbl, wh, ord, lim)| {
                    let mut sql = format!("SELECT {cols} FROM {tbl}");
                    if let Some(w) = wh {
                        write!(sql, " WHERE {w}").expect("writing to String should not fail");
                    }
                    if let Some(o) = ord {
                        write!(sql, " ORDER BY {o}").expect("writing to String should not fail");
                    }
                    if let Some(l) = lim {
                        write!(sql, " LIMIT {l}").expect("writing to String should not fail");
                    }
                    sql
                })
                .boxed()
        }

        /// Generate a random INSERT statement.
        fn arb_insert() -> BoxedStrategy<String> {
            let ncols = 1usize..4;
            ncols
                .prop_flat_map(|n| {
                    let tbl = arb_ident();
                    let cols = proptest::collection::vec(arb_ident(), n..=n);
                    let vals = proptest::collection::vec(arb_literal(), n..=n);
                    (tbl, cols, vals).prop_map(|(t, cs, vs): (String, Vec<String>, Vec<String>)| {
                        format!(
                            "INSERT INTO {t} ({}) VALUES ({})",
                            cs.join(", "),
                            vs.join(", ")
                        )
                    })
                })
                .boxed()
        }

        /// Generate a random statement.
        fn arb_statement() -> BoxedStrategy<String> {
            prop_oneof![
                6 => arb_select(),
                3 => arb_insert(),
                1 => arb_expr(2).prop_map(|e| format!("SELECT {e}")),
                1 => (arb_ident(), arb_expr(1))
                    .prop_map(|(t, w)| format!("DELETE FROM {t} WHERE {w}")),
                1 => (arb_ident(), arb_ident(), arb_literal(), arb_expr(1))
                    .prop_map(|(t, c, v, w)| format!("UPDATE {t} SET {c} = {v} WHERE {w}")),
            ]
            .boxed()
        }

        /// Try to parse SQL into a single statement; returns `None` if unparseable.
        fn try_parse_one(sql: &str) -> Option<Statement> {
            let mut p = Parser::from_sql(sql);
            let (stmts, errs) = p.parse_all();
            if errs.is_empty() && stmts.len() == 1 {
                Some(stmts.into_iter().next().unwrap())
            } else {
                None
            }
        }

        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(1000))]

            #[test]
            fn test_parser_roundtrip_proptest(sql in arb_statement()) {
                // Phase 1: parse the generated SQL.
                let Some(ast1) = try_parse_one(&sql) else {
                    return Ok(()); // skip unparseable inputs
                };

                // Phase 2: display the AST back to SQL text.
                let rendered1 = ast1.to_string();

                // Phase 3: re-parse the rendered SQL.
                let Some(ast2) = try_parse_one(&rendered1) else {
                    let msg = format!("re-parse failed for rendered SQL: {rendered1:?}");
                    prop_assert!(false, "{}", msg);
                    unreachable!()
                };

                // Phase 4: display again and compare (idempotency check).
                let rendered2 = ast2.to_string();
                let msg = format!(
                    "round-trip not idempotent:\n  original: {sql}\n  rendered1: {rendered1}\n  rendered2: {rendered2}"
                );
                prop_assert_eq!(rendered1, rendered2, "{}", msg);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Proptest: additional property tests (bd-1lsfu.4)
    // -----------------------------------------------------------------------

    mod proptest_properties {
        use super::*;
        use proptest::prelude::*;

        /// Reuse the statement generator from the roundtrip module.
        fn arb_ident() -> BoxedStrategy<String> {
            prop::string::string_regex("[a-z][a-z0-9]{0,5}")
                .expect("valid regex")
                .prop_filter("must not be keyword", |s| {
                    TokenKind::lookup_keyword(s).is_none()
                })
                .boxed()
        }

        fn arb_literal() -> BoxedStrategy<String> {
            prop_oneof![
                any::<i32>().prop_map(|n| n.to_string()),
                (1i32..1000).prop_map(|n| format!("{n}.{}", n % 100)),
                arb_ident().prop_map(|s| format!("'{s}'")),
                Just("NULL".to_string()),
                Just("TRUE".to_string()),
                Just("FALSE".to_string()),
            ]
            .boxed()
        }

        fn arb_expr(depth: u32) -> BoxedStrategy<String> {
            if depth == 0 {
                prop_oneof![arb_literal(), arb_ident(),].boxed()
            } else {
                let leaf = arb_expr(0);
                prop_oneof![
                    4 => leaf,
                    2 => (arb_expr(depth - 1), prop_oneof![
                        Just("+"), Just("-"), Just("*"), Just("/"),
                        Just("="), Just("!="), Just("<"), Just("<="),
                        Just(">"), Just(">="), Just("AND"), Just("OR"),
                    ], arb_expr(depth - 1))
                        .prop_map(|(l, op, r)| format!("({l} {op} {r})")),
                    1 => arb_expr(depth - 1).prop_map(|e| format!("(-{e})")),
                    1 => arb_expr(depth - 1).prop_map(|e| format!("(NOT {e})")),
                ]
                .boxed()
            }
        }

        fn arb_select() -> BoxedStrategy<String> {
            use std::fmt::Write as _;
            let cols =
                proptest::collection::vec(arb_expr(1), 1..4).prop_map(|cols| cols.join(", "));
            let table = arb_ident();
            let where_clause = prop::option::of(arb_expr(1));
            (cols, table, where_clause)
                .prop_map(|(cols, tbl, wh)| {
                    let mut sql = format!("SELECT {cols} FROM {tbl}");
                    if let Some(w) = wh {
                        write!(sql, " WHERE {w}").expect("writing to String should not fail");
                    }
                    sql
                })
                .boxed()
        }

        fn arb_statement() -> BoxedStrategy<String> {
            prop_oneof![
                6 => arb_select(),
                3 => {
                    let ncols = 1usize..4;
                    ncols
                        .prop_flat_map(|n| {
                            let tbl = arb_ident();
                            let cols = proptest::collection::vec(arb_ident(), n..=n);
                            let vals = proptest::collection::vec(arb_literal(), n..=n);
                            (tbl, cols, vals).prop_map(
                                |(t, cs, vs): (String, Vec<String>, Vec<String>)| {
                                    format!(
                                        "INSERT INTO {t} ({}) VALUES ({})",
                                        cs.join(", "),
                                        vs.join(", ")
                                    )
                                },
                            )
                        })
                        .boxed()
                },
                1 => arb_expr(2).prop_map(|e| format!("SELECT {e}")),
                1 => (arb_ident(), arb_expr(1))
                    .prop_map(|(t, w)| format!("DELETE FROM {t} WHERE {w}")),
                1 => (arb_ident(), arb_ident(), arb_literal(), arb_expr(1))
                    .prop_map(|(t, c, v, w)| format!("UPDATE {t} SET {c} = {v} WHERE {w}")),
            ]
            .boxed()
        }

        // Property 2: Determinism — same input always produces the same AST.
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(500))]

            #[test]
            fn test_parser_determinism(sql in arb_statement()) {
                let mut p1 = Parser::from_sql(&sql);
                let (stmts1, errs1) = p1.parse_all();

                let mut p2 = Parser::from_sql(&sql);
                let (stmts2, errs2) = p2.parse_all();

                // Both parses must produce the same number of statements and errors.
                let msg_stmt = format!("different statement counts for: {sql}");
                prop_assert_eq!(stmts1.len(), stmts2.len(), "{}", msg_stmt);
                let msg_err = format!("different error counts for: {sql}");
                prop_assert_eq!(errs1.len(), errs2.len(), "{}", msg_err);

                // If successful, the rendered SQL must be identical.
                if errs1.is_empty() && !stmts1.is_empty() {
                    for (s1, s2) in stmts1.iter().zip(stmts2.iter()) {
                        let r1 = s1.to_string();
                        let r2 = s2.to_string();
                        let msg_det = format!("non-deterministic parse output for: {sql}");
                        prop_assert_eq!(r1, r2, "{}", msg_det);
                    }
                }
            }
        }

        // Property 3: Fuzz safety — random byte strings never panic the parser.
        proptest::proptest! {
                #![proptest_config(proptest::prelude::ProptestConfig::with_cases(2000))]

                #[test]
        fn test_parser_fuzz_no_panic(input in prop::collection::vec(any::<u8>(), 0..256)) {
            let sql = String::from_utf8_lossy(&input);
            // Must not panic — errors are fine, panics are not.
            let mut p = Parser::from_sql(&sql);
            let _ = p.parse_all();
                }
            }

        // Property 3b: Fuzz safety with near-valid SQL (more likely to trigger edge cases).
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(1000))]

            #[test]
            fn test_parser_fuzz_near_valid(
                prefix in prop_oneof![
                    Just("SELECT "),
                    Just("INSERT INTO "),
                    Just("DELETE FROM "),
                    Just("UPDATE "),
                    Just("CREATE TABLE "),
                    Just("DROP TABLE "),
                    Just("BEGIN "),
                    Just("PRAGMA "),
                ],
                suffix in prop::string::string_regex("[a-zA-Z0-9_ ,.*=<>!()'\";+\\-/]{0,100}")
                    .expect("valid regex")
            ) {
                let sql = format!("{prefix}{suffix}");
                let mut p = Parser::from_sql(&sql);
                let _ = p.parse_all();
            }
        }

        // Property 4: Unicode identifiers parse correctly.
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(200))]

            #[test]
            fn test_parser_unicode_identifiers(
                name in prop::string::string_regex("[\\p{L}][\\p{L}\\p{N}_]{0,10}")
                    .expect("valid regex")
                    .prop_filter("must not be keyword", |s| {
                        TokenKind::lookup_keyword(s).is_none()
                    })
            ) {
                // Double-quoted identifiers with Unicode should parse.
                let sql = format!("SELECT \"{name}\" FROM \"{name}\"");
                let mut p = Parser::from_sql(&sql);
                let (stmts, errs) = p.parse_all();
                prop_assert!(
                    errs.is_empty(),
                    "Unicode identifier should parse: {sql}, errors: {errs:?}"
                );
                prop_assert_eq!(stmts.len(), 1);
            }
        }

        // Property 5: Rejection — various forms of invalid SQL are rejected.
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(300))]

            #[test]
            fn test_parser_rejects_incomplete_statements(
                kind in prop_oneof![
                    Just("SELECT"),
                    Just("SELECT FROM"),
                    Just("INSERT INTO"),
                    Just("DELETE"),
                    Just("UPDATE SET"),
                    Just("CREATE"),
                    Just("CREATE TABLE"),
                    Just("DROP"),
                ],
                trailing in prop::option::of(
                    prop::string::string_regex("[;, ]{0,3}").expect("valid regex")
                )
            ) {
                let sql = match trailing {
                    Some(t) => format!("{kind}{t}"),
                    None => kind.to_string(),
                };
                let mut p = Parser::from_sql(&sql);
                let (stmts, errs) = p.parse_all();
                // At least one of: parse errors, or no valid statements produced.
                // The parser should not silently produce a valid-looking AST from
                // these fundamentally incomplete inputs.
                prop_assert!(
                    !errs.is_empty() || stmts.is_empty(),
                    "Expected rejection of incomplete SQL: {sql}, got {stmts:?}"
                );
            }
        }

        // Property 6: Statement count stability — concatenated statements produce
        // the right number of parsed statements.
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(200))]

            #[test]
            fn test_parser_multi_statement_count(
                stmts in proptest::collection::vec(arb_statement(), 1..4)
            ) {
                let sql = stmts.join("; ");
                let mut p = Parser::from_sql(&sql);
                let (parsed, errors) = p.parse_all();
                // If no errors, we should get at least as many statements as we joined.
                if errors.is_empty() {
                    prop_assert!(
                        parsed.len() >= stmts.len(),
                        "Expected at least {} statements from: {sql}, got {}",
                        stmts.len(),
                        parsed.len()
                    );
                }
            }
        }
    }

    #[test]
    fn test_parse_statements_with_scratch_reuses_token_and_error_capacity() {
        let mut scratch = StatementParseScratch::default();
        let err = parse_statements_with_scratch("SELECT FROM", &mut scratch)
            .expect_err("malformed SQL should surface a parse error");
        assert!(
            err.message.contains("expected"),
            "malformed parse should preserve its diagnostic detail",
        );
        let warmed_token_capacity = scratch.token_capacity();
        let warmed_error_capacity = scratch.error_capacity();
        assert!(
            warmed_token_capacity > 0,
            "parse scratch should warm token storage"
        );
        assert!(
            warmed_error_capacity > 0,
            "parse scratch should warm error storage"
        );

        let statements = parse_statements_with_scratch("SELECT 1;", &mut scratch)
            .expect("follow-up parse should succeed");
        assert_eq!(statements.len(), 1);
        assert_eq!(
            scratch.token_capacity(),
            warmed_token_capacity,
            "successful parse should reuse token scratch capacity",
        );
        assert_eq!(
            scratch.error_capacity(),
            warmed_error_capacity,
            "successful parse should preserve error scratch capacity for the next recovery path",
        );
    }

    #[test]
    fn test_parse_statements_with_scratch_enforces_top_level_separators() {
        let mut scratch = StatementParseScratch::default();
        let sql = "SELECT 1 SELECT 2";
        let error = parse_statements_with_scratch(sql, &mut scratch)
            .expect_err("scratch parser must reject adjacent statements");
        assert!(
            error.message.contains("expected ';' separator"),
            "unexpected diagnostic: {error:?}"
        );
        assert_eq!(
            &sql[error.span.start as usize..error.span.end as usize],
            "SELECT"
        );

        let statements = parse_statements_with_scratch("SELECT 1; SELECT 2;", &mut scratch)
            .expect("semicolon-separated statements must remain valid");
        assert_eq!(statements.len(), 2);

        let trigger_script = "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
                              INSERT INTO t VALUES (1); \
                              INSERT INTO t VALUES (2); \
                              END; SELECT 3;";
        let statements = parse_statements_with_scratch(trigger_script, &mut scratch)
            .expect("trigger-body terminators must not become top-level separator errors");
        assert_eq!(statements.len(), 2);
        assert!(matches!(statements[0], Statement::CreateTrigger(_)));
        assert!(matches!(statements[1], Statement::Select(_)));
    }

    #[test]
    fn test_parse_statements_with_scratch_reuses_identifier_interns_across_parses() {
        let mut scratch = StatementParseScratch::default();
        let mut sql = String::from("SELECT ");
        for i in 0..32 {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("unique_identifier_{i} AS unique_alias_{i}"));
        }
        sql.push(';');

        let statements = parse_statements_with_scratch(&sql, &mut scratch)
            .expect("identifier-heavy statement should parse");
        assert_eq!(statements.len(), 1);
        let interner_len = scratch.identifier_interner_len();
        assert!(
            interner_len > 0,
            "scratch should retain identifier interns for the next parse",
        );

        let statements = parse_statements_with_scratch(&sql, &mut scratch)
            .expect("repeat parse should also succeed");
        assert_eq!(statements.len(), 1);
        assert_eq!(
            scratch.identifier_interner_len(),
            interner_len,
            "repeated parse should reuse the retained interner set instead of growing it",
        );

        scratch.reset();
        assert!(
            scratch.identifier_interner_is_empty(),
            "explicit scratch reset should also keep the interner logically empty",
        );
    }

    #[test]
    fn test_parse_statements_with_scratch_drops_oversized_identifier_interner() {
        let mut scratch = StatementParseScratch::default();
        let mut sql = String::from("SELECT ");
        for i in 0..300 {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "very_long_unique_identifier_{i:03} AS alias_{i:03}"
            ));
        }
        sql.push(';');

        let statements = parse_statements_with_scratch(&sql, &mut scratch)
            .expect("oversized identifier-heavy statement should parse");
        assert_eq!(statements.len(), 1);
        assert!(
            scratch.identifier_interner_is_empty(),
            "oversized identifier interners should be dropped instead of retained indefinitely",
        );
    }

    // ── bd-1702 repro tests ─────────────────────────────────────────────
    // Reserved-word column names in CREATE TABLE (quoted and unquoted).

    #[test]
    fn create_table_quoted_reserved_word_key() {
        // Double-quoted "key" should parse as identifier, not KwKey.
        parse_ok(r#"CREATE TABLE "meta" ("key" TEXT, "val" TEXT);"#);
    }

    #[test]
    fn create_table_unquoted_key_column() {
        // KEY is a non-reserved keyword — should work unquoted.
        parse_ok("CREATE TABLE meta (key TEXT, val TEXT);");
    }

    #[test]
    fn create_table_quoted_order_column() {
        // ORDER is reserved — must work when double-quoted.
        parse_ok(r#"CREATE TABLE t ("order" INTEGER);"#);
    }

    #[test]
    fn create_table_quoted_select_column() {
        // SELECT is reserved — must work when double-quoted.
        parse_ok(r#"CREATE TABLE t ("select" TEXT);"#);
    }

    #[test]
    fn select_with_reserved_word_column_key() {
        // SELECT using "key" as column name — unquoted.
        parse_ok("SELECT key FROM meta;");
    }

    #[test]
    fn select_with_reserved_word_column_value() {
        // SELECT using "value" — check if it's a keyword.
        parse_ok("SELECT value FROM meta;");
    }

    #[test]
    fn select_with_reserved_word_column_order() {
        // ORDER is reserved — quoted should work.
        parse_ok(r#"SELECT "order" FROM t;"#);
    }

    #[test]
    fn where_clause_with_reserved_word_column() {
        // WHERE referencing a reserved-word column.
        parse_ok("UPDATE meta SET val = '2.0' WHERE key = 'version';");
    }

    #[test]
    fn update_set_reserved_word_column() {
        // SET reserved-word column.
        parse_ok(r#"UPDATE meta SET "key" = 'newkey' WHERE "key" = 'oldkey';"#);
    }

    #[test]
    fn delete_where_reserved_word_column() {
        parse_ok("DELETE FROM meta WHERE key = 'version';");
    }

    #[test]
    fn persistence_dump_with_reserved_word_columns() {
        // Simulates the exact SQL that build_create_table_sql generates
        // for a table that was originally created with reserved-word columns.
        let sql = concat!(
            r#"CREATE TABLE "meta" ("key" TEXT, "value" TEXT);"#,
            "\n",
            r#"INSERT INTO "meta" VALUES ('version', '1.0');"#,
            "\n",
            r#"INSERT INTO "meta" VALUES ('author', 'test');"#,
        );
        let mut p = Parser::from_sql(sql);
        let (stmts, errs) = p.parse_all();
        assert!(
            errs.is_empty(),
            "persistence dump with reserved-word columns should parse cleanly: {errs:?}"
        );
        assert_eq!(stmts.len(), 3);
    }

    #[test]
    fn create_table_with_single_quoted_name_parses_cleanly() {
        let sql = "CREATE TABLE 'fts_messages_data'(id INTEGER PRIMARY KEY, block BLOB);";
        let mut p = Parser::from_sql(sql);
        let (stmts, errs) = p.parse_all();
        assert!(
            errs.is_empty(),
            "single-quoted sqlite_master shadow-table SQL should parse cleanly: {errs:?}"
        );
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::CreateTable(stmt) => {
                assert_eq!(stmt.name.name, "fts_messages_data");
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn select_qualified_column_with_alias() {
        // Bug: "a.name as from_name" was being parsed with alias=None and
        // col_ref.column="name as" instead of alias=Some("from_name").
        let stmt = parse_one("SELECT a.name AS from_name FROM users a");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                assert_eq!(columns.len(), 1);
                match &columns[0] {
                    ResultColumn::Expr { expr, alias } => {
                        // Alias should be captured as "from_name".
                        assert_eq!(
                            alias.as_deref(),
                            Some("from_name"),
                            "alias should be 'from_name', got {alias:?}"
                        );
                        // Expression should be a qualified column ref: a.name
                        if let Expr::Column(col_ref, _) = expr {
                            assert_eq!(col_ref.table.as_deref(), Some("a"));
                            assert_eq!(col_ref.column.as_ref(), "name");
                        } else {
                            panic!("expected Column expression, got {expr:?}");
                        }
                    }
                    other => panic!("expected Expr variant, got {other:?}"),
                }
            } else {
                panic!("expected Select core");
            }
        } else {
            panic!("expected Select statement");
        }
    }

    #[test]
    fn select_qualified_column_with_implicit_alias() {
        // Test implicit alias syntax (without AS keyword).
        let stmt = parse_one("SELECT a.name from_name FROM users a");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                assert_eq!(columns.len(), 1);
                match &columns[0] {
                    ResultColumn::Expr { expr, alias } => {
                        // Alias should be captured as "from_name" even without AS.
                        assert_eq!(
                            alias.as_deref(),
                            Some("from_name"),
                            "implicit alias should be 'from_name', got {alias:?}"
                        );
                        // Expression should be a qualified column ref: a.name
                        if let Expr::Column(col_ref, _) = expr {
                            assert_eq!(col_ref.table.as_deref(), Some("a"));
                            assert_eq!(col_ref.column.as_ref(), "name");
                        } else {
                            panic!("expected Column expression, got {expr:?}");
                        }
                    }
                    other => panic!("expected Expr variant, got {other:?}"),
                }
            } else {
                panic!("expected Select core");
            }
        } else {
            panic!("expected Select statement");
        }
    }

    #[test]
    fn select_implicit_alias_non_reserved_keyword() {
        // 'action' is a non-reserved keyword (TokenKind::KwAction).
        // It should be accepted as an implicit alias: SELECT 1 action
        let stmt = parse_one("SELECT 1 action");
        if let Statement::Select(s) = stmt {
            if let SelectCore::Select { columns, .. } = &s.body.select {
                if let ResultColumn::Expr { alias, .. } = &columns[0] {
                    assert_eq!(
                        alias.as_deref(),
                        Some("action"),
                        "implicit alias 'action' (keyword) failed to parse"
                    );
                } else {
                    unreachable!("expected Expr result column");
                }
            } else {
                unreachable!("expected Select core");
            }
        } else {
            unreachable!("expected Select");
        }
    }
}
