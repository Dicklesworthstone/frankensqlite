// bd-2tu6: §10.2 SQL Parser
//
// Hand-written recursive descent parser. Expression parsing lives in expr.rs.

use std::error::Error;
use std::fmt;

use fsqlite_ast::{
    AlterTableAction, AlterTableStatement, Assignment, AssignmentTarget, AttachStatement,
    BeginStatement, ColumnConstraint, ColumnConstraintKind, ColumnDef, CompoundOp, ConflictAction,
    CreateIndexStatement, CreateTableBody, CreateTableStatement, CreateTriggerStatement,
    CreateViewStatement, CreateVirtualTableStatement, Cte, CteMaterialized, DefaultValue,
    Deferrable, DeferrableInitially, DeleteStatement, Distinctness, DropObjectType, DropStatement,
    ForeignKeyAction, ForeignKeyActionType, ForeignKeyClause, ForeignKeyTrigger, FrameBound,
    FrameExclude, FrameSpec, FrameType, FromClause, GeneratedStorage, IndexHint, IndexedColumn,
    InsertSource, InsertStatement, JoinClause, JoinConstraint, JoinKind, JoinType, LimitClause,
    NullsOrder, OrderingTerm, PragmaStatement, PragmaValue, QualifiedName, QualifiedTableRef,
    ResultColumn, RollbackStatement, SelectBody, SelectCore, SelectStatement, SortDirection, Span,
    Statement, TableConstraint, TableConstraintKind, TableOrSubquery, TransactionMode,
    TriggerEvent, TriggerTiming, TypeName, UpdateStatement, UpsertAction, UpsertClause,
    UpsertTarget, VacuumStatement, WindowDef, WindowSpec, WithClause,
};

use crate::lexer::Lexer;
use crate::token::{Token, TokenKind};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
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
                message: message.into(),
                span: t.span,
                line: t.line,
                col: t.col,
            }
        } else {
            Self {
                message: message.into(),
                span: Span::ZERO,
                line: 0,
                col: 0,
            }
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}: {}", self.line, self.col, self.message)
    }
}

impl Error for ParseError {}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

pub struct Parser {
    pub(crate) tokens: Vec<Token>,
    pub(crate) pos: usize,
    pub(crate) errors: Vec<ParseError>,
}

impl Parser {
    #[must_use]
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            pos: 0,
            errors: Vec::new(),
        }
    }

    #[must_use]
    pub fn from_sql(sql: &str) -> Self {
        Self::new(Lexer::tokenize(sql))
    }

    pub fn parse_all(&mut self) -> (Vec<Statement>, Vec<ParseError>) {
        let mut stmts = Vec::new();
        while !self.at_eof() {
            if self.check(&TokenKind::Semicolon) {
                self.advance();
                continue;
            }
            match self.parse_statement() {
                Ok(s) => {
                    stmts.push(s);
                    let _ = self.eat(&TokenKind::Semicolon);
                }
                Err(e) => {
                    self.errors.push(e);
                    self.synchronize();
                }
            }
        }
        (stmts, std::mem::take(&mut self.errors))
    }

    pub fn parse_statement(&mut self) -> Result<Statement, ParseError> {
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

    // -----------------------------------------------------------------------
    // Identifiers and names
    // -----------------------------------------------------------------------

    pub(crate) fn parse_identifier(&mut self) -> Result<String, ParseError> {
        match self.peek().clone() {
            TokenKind::Id(s) | TokenKind::QuotedId(s, _) => {
                self.advance();
                Ok(s)
            }
            ref k if is_nonreserved_kw(k) => {
                let s = kw_to_str(k);
                self.advance();
                Ok(s)
            }
            _ => Err(self.err_expected("identifier")),
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

    fn parse_qualified_table_ref(&mut self) -> Result<QualifiedTableRef, ParseError> {
        let name = self.parse_qualified_name()?;
        let alias = self.try_alias()?;
        let index_hint = self.parse_index_hint()?;
        Ok(QualifiedTableRef {
            name,
            alias,
            index_hint,
        })
    }

    fn try_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.eat_kw(&TokenKind::KwAs) {
            return Ok(Some(self.parse_identifier()?));
        }
        // Peek for an identifier that isn't a keyword starting the next clause.
        if matches!(self.peek(), TokenKind::Id(_) | TokenKind::QuotedId(_, _)) {
            return Ok(Some(self.parse_identifier()?));
        }
        Ok(None)
    }

    fn parse_index_hint(&mut self) -> Result<Option<IndexHint>, ParseError> {
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

    pub(crate) fn parse_comma_sep<T>(
        &mut self,
        f: fn(&mut Self) -> Result<T, ParseError>,
    ) -> Result<Vec<T>, ParseError> {
        let mut v = vec![f(self)?];
        while self.eat(&TokenKind::Comma) {
            v.push(f(self)?);
        }
        Ok(v)
    }

    // -----------------------------------------------------------------------
    // Statement dispatch
    // -----------------------------------------------------------------------

    fn parse_statement_inner(&mut self) -> Result<Statement, ParseError> {
        match self.peek().clone() {
            TokenKind::KwSelect | TokenKind::KwValues => {
                Ok(Statement::Select(self.parse_select_stmt(None)?))
            }
            TokenKind::KwWith => self.parse_with_leading(),
            TokenKind::KwInsert | TokenKind::KwReplace => self.parse_insert_stmt(None),
            TokenKind::KwUpdate => self.parse_update_stmt(None),
            TokenKind::KwDelete => self.parse_delete_stmt(None),
            TokenKind::KwCreate => self.parse_create(),
            TokenKind::KwDrop => self.parse_drop(),
            TokenKind::KwAlter => self.parse_alter(),
            TokenKind::KwBegin => self.parse_begin(),
            TokenKind::KwCommit | TokenKind::KwEnd => {
                self.advance();
                let _ = self.eat_kw(&TokenKind::KwTransaction);
                Ok(Statement::Commit)
            }
            TokenKind::KwRollback => self.parse_rollback(),
            TokenKind::KwSavepoint => {
                self.advance();
                Ok(Statement::Savepoint(self.parse_identifier()?))
            }
            TokenKind::KwRelease => {
                self.advance();
                let _ = self.eat_kw(&TokenKind::KwSavepoint);
                Ok(Statement::Release(self.parse_identifier()?))
            }
            TokenKind::KwAttach => self.parse_attach(),
            TokenKind::KwDetach => {
                self.advance();
                let _ = self.eat_kw(&TokenKind::KwDatabase);
                Ok(Statement::Detach(self.parse_identifier()?))
            }
            TokenKind::KwPragma => self.parse_pragma(),
            TokenKind::KwVacuum => self.parse_vacuum(),
            TokenKind::KwReindex => {
                self.advance();
                let name = if !self.at_eof() && !self.check(&TokenKind::Semicolon) {
                    Some(self.parse_qualified_name()?)
                } else {
                    None
                };
                Ok(Statement::Reindex(name))
            }
            TokenKind::KwAnalyze => {
                self.advance();
                let name = if !self.at_eof() && !self.check(&TokenKind::Semicolon) {
                    Some(self.parse_qualified_name()?)
                } else {
                    None
                };
                Ok(Statement::Analyze(name))
            }
            TokenKind::KwExplain => self.parse_explain(),
            _ => Err(self.err_msg("unexpected token at start of statement")),
        }
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
            TokenKind::KwInsert | TokenKind::KwReplace => self.parse_insert_stmt(Some(with)),
            TokenKind::KwUpdate => self.parse_update_stmt(Some(with)),
            TokenKind::KwDelete => self.parse_delete_stmt(Some(with)),
            _ => Err(self.err_expected("SELECT, INSERT, UPDATE, or DELETE after WITH")),
        }
    }

    fn parse_with_clause(&mut self) -> Result<WithClause, ParseError> {
        self.expect_kw(&TokenKind::KwWith)?;
        let recursive = self.eat_kw(&TokenKind::KwRecursive);
        let ctes = self.parse_comma_sep(Self::parse_cte)?;
        Ok(WithClause { recursive, ctes })
    }

    fn parse_cte(&mut self) -> Result<Cte, ParseError> {
        let name = self.parse_identifier()?;
        let columns = if self.eat(&TokenKind::LeftParen) {
            let cols = self.parse_comma_sep(Self::parse_identifier)?;
            self.expect_token(&TokenKind::RightParen)?;
            cols
        } else {
            vec![]
        };
        let materialized = if self.eat_kw(&TokenKind::KwAs) {
            None
        } else if self.check_kw(&TokenKind::KwNot) {
            self.advance();
            self.expect_kw(&TokenKind::KwMaterialized)?;
            self.expect_kw(&TokenKind::KwAs)?;
            Some(CteMaterialized::NotMaterialized)
        } else if self.eat_kw(&TokenKind::KwMaterialized) {
            self.expect_kw(&TokenKind::KwAs)?;
            Some(CteMaterialized::Materialized)
        } else {
            self.expect_kw(&TokenKind::KwAs)?;
            None
        };
        // Handle the case where AS was consumed above.
        // Now we need the parenthesized select.
        self.expect_token(&TokenKind::LeftParen)?;
        let query = self.parse_select_stmt(None)?;
        self.expect_token(&TokenKind::RightParen)?;
        Ok(Cte {
            name,
            columns,
            materialized,
            query,
        })
    }

    // -----------------------------------------------------------------------
    // SELECT
    // -----------------------------------------------------------------------

    pub(crate) fn parse_select_stmt(
        &mut self,
        with: Option<WithClause>,
    ) -> Result<SelectStatement, ParseError> {
        let body = self.parse_select_body()?;
        let order_by = if self.eat_kw(&TokenKind::KwOrder) {
            self.expect_kw(&TokenKind::KwBy)?;
            self.parse_comma_sep(Self::parse_ordering_term)?
        } else {
            vec![]
        };
        let limit = self.parse_limit()?;
        Ok(SelectStatement {
            with,
            body,
            order_by,
            limit,
        })
    }

    fn parse_select_body(&mut self) -> Result<SelectBody, ParseError> {
        let select = self.parse_select_core()?;
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
            compounds.push((op, self.parse_select_core()?));
        }
        Ok(SelectBody { select, compounds })
    }

    fn parse_select_core(&mut self) -> Result<SelectCore, ParseError> {
        if self.eat_kw(&TokenKind::KwValues) {
            return self.parse_values_core();
        }
        self.expect_kw(&TokenKind::KwSelect)?;
        let distinct = if self.eat_kw(&TokenKind::KwDistinct) {
            Distinctness::Distinct
        } else {
            let _ = self.eat_kw(&TokenKind::KwAll);
            Distinctness::All
        };
        let columns = self.parse_comma_sep(Self::parse_result_column)?;
        let from = if self.eat_kw(&TokenKind::KwFrom) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };
        let where_clause = if self.eat_kw(&TokenKind::KwWhere) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        let group_by = if self.eat_kw(&TokenKind::KwGroup) {
            self.expect_kw(&TokenKind::KwBy)?;
            self.parse_comma_sep(Self::parse_expr)?
        } else {
            vec![]
        };
        let having = if self.eat_kw(&TokenKind::KwHaving) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        let windows = if self.eat_kw(&TokenKind::KwWindow) {
            self.parse_comma_sep(Self::parse_window_def)?
        } else {
            vec![]
        };
        Ok(SelectCore::Select {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            windows,
        })
    }

    fn parse_values_core(&mut self) -> Result<SelectCore, ParseError> {
        let mut rows = Vec::new();
        loop {
            self.expect_token(&TokenKind::LeftParen)?;
            let row = self.parse_comma_sep(Self::parse_expr)?;
            self.expect_token(&TokenKind::RightParen)?;
            rows.push(row);
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        Ok(SelectCore::Values(rows))
    }

    fn parse_result_column(&mut self) -> Result<ResultColumn, ParseError> {
        if self.eat(&TokenKind::Star) {
            return Ok(ResultColumn::Star);
        }
        // table.* check: identifier followed by dot-star.
        if matches!(self.peek(), TokenKind::Id(_) | TokenKind::QuotedId(_, _))
            && self.peek_nth(1) == &TokenKind::Dot
            && self.peek_nth(2) == &TokenKind::Star
        {
            let tbl = self.parse_identifier()?;
            self.advance(); // dot
            self.advance(); // star
            return Ok(ResultColumn::TableStar(tbl));
        }
        let expr = self.parse_expr()?;
        let alias = self.try_alias()?;
        Ok(ResultColumn::Expr { expr, alias })
    }

    // -----------------------------------------------------------------------
    // FROM clause & JOINs
    // -----------------------------------------------------------------------

    fn parse_from_clause(&mut self) -> Result<FromClause, ParseError> {
        let source = self.parse_table_or_subquery()?;
        let mut joins = Vec::new();
        loop {
            if let Some(jt) = self.try_join_type()? {
                let table = self.parse_table_or_subquery()?;
                let constraint = self.parse_join_constraint()?;
                joins.push(JoinClause {
                    join_type: jt,
                    table,
                    constraint,
                });
            } else if self.eat(&TokenKind::Comma) {
                let table = self.parse_table_or_subquery()?;
                joins.push(JoinClause {
                    join_type: JoinType {
                        natural: false,
                        kind: JoinKind::Cross,
                    },
                    table,
                    constraint: None,
                });
            } else {
                break;
            }
        }
        Ok(FromClause { source, joins })
    }

    fn parse_table_or_subquery(&mut self) -> Result<TableOrSubquery, ParseError> {
        if self.check(&TokenKind::LeftParen) {
            self.advance();
            if matches!(
                self.peek(),
                TokenKind::KwSelect | TokenKind::KwWith | TokenKind::KwValues
            ) {
                let q = self.parse_select_stmt(None)?;
                self.expect_token(&TokenKind::RightParen)?;
                let alias = self.try_alias()?;
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
            let alias = self.try_alias()?;
            return Ok(TableOrSubquery::TableFunction {
                name: name.name,
                args,
                alias,
            });
        }

        let alias = self.try_alias()?;
        let index_hint = self.parse_index_hint()?;
        Ok(TableOrSubquery::Table {
            name,
            alias,
            index_hint,
        })
    }

    fn try_join_type(&mut self) -> Result<Option<JoinType>, ParseError> {
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

    fn parse_ordering_term(&mut self) -> Result<OrderingTerm, ParseError> {
        let expr = self.parse_expr()?;
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
        Ok(OrderingTerm {
            expr,
            direction,
            nulls,
        })
    }

    fn parse_limit(&mut self) -> Result<Option<LimitClause>, ParseError> {
        if !self.eat_kw(&TokenKind::KwLimit) {
            return Ok(None);
        }
        let limit = self.parse_expr()?;
        let offset = if self.eat_kw(&TokenKind::KwOffset) {
            Some(self.parse_expr()?)
        } else if self.eat(&TokenKind::Comma) {
            // LIMIT count, offset — SQLite compat (offset is second arg).
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Some(LimitClause { limit, offset }))
    }

    // -----------------------------------------------------------------------
    // RETURNING clause
    // -----------------------------------------------------------------------

    fn parse_returning(&mut self) -> Result<Vec<ResultColumn>, ParseError> {
        if self.eat_kw(&TokenKind::KwReturning) {
            self.parse_comma_sep(Self::parse_result_column)
        } else {
            Ok(vec![])
        }
    }

    // -----------------------------------------------------------------------
    // INSERT
    // -----------------------------------------------------------------------

    fn parse_insert_stmt(&mut self, with: Option<WithClause>) -> Result<Statement, ParseError> {
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
        self.expect_kw(&TokenKind::KwInto)?;
        let table = self.parse_qualified_name()?;
        let alias = if self.eat_kw(&TokenKind::KwAs) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let columns = if self.check(&TokenKind::LeftParen)
            && !matches!(self.peek_nth(1), TokenKind::KwSelect | TokenKind::KwWith)
        {
            self.advance();
            let cols = self.parse_comma_sep(Self::parse_identifier)?;
            self.expect_token(&TokenKind::RightParen)?;
            cols
        } else {
            vec![]
        };
        let source = if self.eat_kw(&TokenKind::KwDefault) {
            self.expect_kw(&TokenKind::KwValues)?;
            InsertSource::DefaultValues
        } else if self.check_kw(&TokenKind::KwValues) {
            self.advance();
            let mut rows = Vec::new();
            loop {
                self.expect_token(&TokenKind::LeftParen)?;
                let row = self.parse_comma_sep(Self::parse_expr)?;
                self.expect_token(&TokenKind::RightParen)?;
                rows.push(row);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
            InsertSource::Values(rows)
        } else {
            InsertSource::Select(Box::new(self.parse_select_stmt(None)?))
        };
        let upsert = self.parse_upsert_clauses()?;
        let returning = self.parse_returning()?;
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

    fn parse_update_stmt(&mut self, with: Option<WithClause>) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwUpdate)?;
        let or_conflict = if self.eat_kw(&TokenKind::KwOr) {
            Some(self.parse_conflict_action()?)
        } else {
            None
        };
        let table = self.parse_qualified_table_ref()?;
        self.expect_kw(&TokenKind::KwSet)?;
        let assignments = self.parse_comma_sep(Self::parse_assignment)?;
        let from = if self.eat_kw(&TokenKind::KwFrom) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };
        let where_clause = if self.eat_kw(&TokenKind::KwWhere) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let returning = self.parse_returning()?;
        let order_by = if self.eat_kw(&TokenKind::KwOrder) {
            self.expect_kw(&TokenKind::KwBy)?;
            self.parse_comma_sep(Self::parse_ordering_term)?
        } else {
            vec![]
        };
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

    fn parse_delete_stmt(&mut self, with: Option<WithClause>) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwDelete)?;
        self.expect_kw(&TokenKind::KwFrom)?;
        let table = self.parse_qualified_table_ref()?;
        let where_clause = if self.eat_kw(&TokenKind::KwWhere) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let returning = self.parse_returning()?;
        let order_by = if self.eat_kw(&TokenKind::KwOrder) {
            self.expect_kw(&TokenKind::KwBy)?;
            self.parse_comma_sep(Self::parse_ordering_term)?
        } else {
            vec![]
        };
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
            return self.parse_create_table(temporary);
        }
        if self.eat_kw(&TokenKind::KwIndex) {
            return self.parse_create_index(unique);
        }
        if self.eat_kw(&TokenKind::KwView) {
            return self.parse_create_view(temporary);
        }
        if self.eat_kw(&TokenKind::KwTrigger) {
            return self.parse_create_trigger(temporary);
        }
        if self.eat_kw(&TokenKind::KwVirtual) {
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
            CreateTableBody::AsSelect(Box::new(self.parse_select_stmt(None)?))
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
                break;
            }
            let _ = self.eat(&TokenKind::Comma);
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
                | TokenKind::KwUnique
                | TokenKind::KwCheck
                | TokenKind::KwDefault
                | TokenKind::KwCollate
                | TokenKind::KwReferences
                | TokenKind::KwGenerated
                | TokenKind::KwConstraint
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
            } else {
                let expr = self.parse_expr()?;
                ColumnConstraintKind::Default(DefaultValue::Expr(expr))
            }
        } else if self.eat_kw(&TokenKind::KwCollate) {
            ColumnConstraintKind::Collate(self.parse_identifier()?)
        } else if self.eat_kw(&TokenKind::KwReferences) {
            ColumnConstraintKind::ForeignKey(self.parse_fk_clause()?)
        } else if self.eat_kw(&TokenKind::KwGenerated) {
            let _ = self.eat_kw(&TokenKind::KwAlways);
            let _ = self.eat_kw(&TokenKind::KwAs);
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
        let query = self.parse_select_stmt(None)?;
        Ok(Statement::CreateView(CreateViewStatement {
            if_not_exists,
            temporary,
            name,
            columns,
            query,
        }))
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
        loop {
            if self.check_kw(&TokenKind::KwEnd) {
                break;
            }
            body.push(self.parse_statement_inner()?);
            let _ = self.eat(&TokenKind::Semicolon);
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
                            // Reconstruct token text from span.
                            let t = self.current().unwrap();
                            let text = format!("{:?}", t.kind);
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

    fn parse_pragma(&mut self) -> Result<Statement, ParseError> {
        self.expect_kw(&TokenKind::KwPragma)?;
        let name = self.parse_qualified_name()?;
        let value = if self.eat(&TokenKind::Eq) || self.eat(&TokenKind::EqEq) {
            Some(PragmaValue::Assign(self.parse_expr()?))
        } else if self.eat(&TokenKind::LeftParen) {
            let v = self.parse_expr()?;
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
        let stmt = self.parse_statement_inner()?;
        Ok(Statement::Explain {
            query_plan,
            stmt: Box::new(stmt),
        })
    }

    // -----------------------------------------------------------------------
    // Window definitions (used in SELECT ... WINDOW clause and OVER)
    // -----------------------------------------------------------------------

    fn parse_window_def(&mut self) -> Result<WindowDef, ParseError> {
        let name = self.parse_identifier()?;
        self.expect_kw(&TokenKind::KwAs)?;
        self.expect_token(&TokenKind::LeftParen)?;
        let spec = self.parse_window_spec()?;
        self.expect_token(&TokenKind::RightParen)?;
        Ok(WindowDef { name, spec })
    }

    pub(crate) fn parse_window_spec(&mut self) -> Result<WindowSpec, ParseError> {
        // Optional base window name.
        let base_window = if matches!(self.peek(), TokenKind::Id(_))
            && !self.check_kw(&TokenKind::KwPartition)
            && !self.check_kw(&TokenKind::KwOrder)
            && !self.check_kw(&TokenKind::KwRange)
            && !self.check_kw(&TokenKind::KwRows)
            && !self.check_kw(&TokenKind::KwGroups)
        {
            Some(self.parse_identifier()?)
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
            base_window,
            partition_by,
            order_by,
            frame,
        })
    }

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
            let s = self.parse_frame_bound()?;
            self.expect_kw(&TokenKind::KwAnd)?;
            let e = self.parse_frame_bound()?;
            (s, Some(e))
        } else {
            (self.parse_frame_bound()?, None)
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
            } else if self.check_kw(&TokenKind::KwOthers) {
                self.advance();
                Some(FrameExclude::NoOthers)
            } else if self.eat_kw(&TokenKind::KwTies) {
                Some(FrameExclude::Ties)
            } else {
                // "CURRENT ROW"
                self.expect_kw(&TokenKind::KwGroup)
                    .map(|_| Some(FrameExclude::Group))
                    .or(
                        // Reparse as "CURRENT ROW"
                        Ok(Some(FrameExclude::CurrentRow)),
                    )?
            }
        } else {
            None
        };
        Ok(Some(FrameSpec {
            frame_type,
            start,
            end,
            exclude,
        }))
    }

    fn parse_frame_bound(&mut self) -> Result<FrameBound, ParseError> {
        if self.eat_kw(&TokenKind::KwUnbounded) {
            if self.eat_kw(&TokenKind::KwPreceding) {
                Ok(FrameBound::UnboundedPreceding)
            } else {
                self.expect_kw(&TokenKind::KwFollowing)?;
                Ok(FrameBound::UnboundedFollowing)
            }
        } else if matches!(self.peek(), TokenKind::Id(s) if s.eq_ignore_ascii_case("CURRENT")) {
            self.advance();
            self.expect_kw(&TokenKind::KwRow)?;
            Ok(FrameBound::CurrentRow)
        } else {
            let expr = self.parse_expr()?;
            if self.eat_kw(&TokenKind::KwPreceding) {
                Ok(FrameBound::Preceding(Box::new(expr)))
            } else {
                self.expect_kw(&TokenKind::KwFollowing)?;
                Ok(FrameBound::Following(Box::new(expr)))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Keyword classification helper
// ---------------------------------------------------------------------------

fn is_nonreserved_kw(k: &TokenKind) -> bool {
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
            | TokenKind::KwIndex
            | TokenKind::KwInitially
            | TokenKind::KwInstead
            | TokenKind::KwKey
            | TokenKind::KwLast
            | TokenKind::KwMatch
            | TokenKind::KwMaterialized
            | TokenKind::KwNo
            | TokenKind::KwNothing
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
            | TokenKind::KwReturning
            | TokenKind::KwRow
            | TokenKind::KwRows
            | TokenKind::KwSavepoint
            | TokenKind::KwStored
            | TokenKind::KwStrict
            | TokenKind::KwTable
            | TokenKind::KwTemp
            | TokenKind::KwTemporary
            | TokenKind::KwTies
            | TokenKind::KwTransaction
            | TokenKind::KwTrigger
            | TokenKind::KwUnbounded
            | TokenKind::KwVacuum
            | TokenKind::KwView
            | TokenKind::KwVirtual
            | TokenKind::KwWindow
            | TokenKind::KwWithout
            | TokenKind::KwAdd
    )
}

pub(crate) fn kw_to_str(k: &TokenKind) -> String {
    let dbg = format!("{k:?}");
    dbg.strip_prefix("Kw").unwrap_or(&dbg).to_ascii_uppercase()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
}
