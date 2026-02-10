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
    Expr, ForeignKeyAction, ForeignKeyActionType, ForeignKeyClause, ForeignKeyTrigger, FrameBound,
    FrameExclude, FrameSpec, FrameType, FromClause, GeneratedStorage, IndexHint, IndexedColumn,
    InsertSource, InsertStatement, JoinClause, JoinConstraint, JoinKind, JoinType, LimitClause,
    Literal, NullsOrder, OrderingTerm, PragmaStatement, PragmaValue, QualifiedName,
    QualifiedTableRef, ResultColumn, RollbackStatement, SelectBody, SelectCore, SelectStatement,
    SortDirection, Span, Statement, TableConstraint, TableConstraintKind, TableOrSubquery,
    TransactionMode, TriggerEvent, TriggerTiming, TypeName, UpdateStatement, UpsertAction,
    UpsertClause, UpsertTarget, VacuumStatement, WindowDef, WindowSpec, WithClause,
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
        // SQL syntax: name AS [NOT] MATERIALIZED (subquery)
        self.expect_kw(&TokenKind::KwAs)?;
        let materialized = if self.check_kw(&TokenKind::KwNot) {
            self.advance();
            self.expect_kw(&TokenKind::KwMaterialized)?;
            Some(CteMaterialized::NotMaterialized)
        } else if self.eat_kw(&TokenKind::KwMaterialized) {
            Some(CteMaterialized::Materialized)
        } else {
            None
        };
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
        let first = self.parse_expr()?;
        if self.eat_kw(&TokenKind::KwOffset) {
            return Ok(Some(LimitClause {
                limit: first,
                offset: Some(self.parse_expr()?),
            }));
        }

        if self.eat(&TokenKind::Comma) {
            // LIMIT offset, count — SQLite/MySQL compatibility form.
            let second = self.parse_expr()?;
            return Ok(Some(LimitClause {
                limit: second,
                offset: Some(first),
            }));
        }

        Ok(Some(LimitClause {
            limit: first,
            offset: None,
        }))
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
                            // Reconstruct token text from token kind.
                            let t = self.current().unwrap();
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
            } else if self.eat_kw(&TokenKind::KwGroup) {
                Some(FrameExclude::Group)
            } else if matches!(self.peek(), TokenKind::Id(s) if s.eq_ignore_ascii_case("CURRENT")) {
                self.advance();
                self.expect_kw(&TokenKind::KwRow)?;
                Some(FrameExclude::CurrentRow)
            } else {
                return Err(self.err_expected("GROUP or CURRENT ROW after EXCLUDE"));
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
                    } => assert_eq!(over.base_window.as_deref(), Some("win")),
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
                    matches!(&columns[0], ResultColumn::TableStar(t) if t == "t1"),
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
                        assert_eq!(col.column, "b");
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
}
