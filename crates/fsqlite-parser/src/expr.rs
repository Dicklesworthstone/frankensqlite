// bd-16ov: §12.15 Expression Syntax
//
// Pratt expression parser with SQLite-correct operator precedence.
// Normative reference: §10.2 of the FrankenSQLite specification.
//
// Precedence table (from canonical upstream SQLite grammar, lowest to highest):
//   OR
//   AND
//   NOT (prefix)
//   = == != <> IS [NOT] MATCH LIKE GLOB BETWEEN IN ISNULL NOTNULL
//   < <= > >=
//   & | << >> (bitwise)
//   + - (binary)
//   * / %
//   || (concat)
//   COLLATE (postfix)
//   ~ - + (unary prefix)
//   -> ->> (JSON)

use fsqlite_ast::{
    BinaryOp, ColumnRef, Distinctness, Expr, FromClause, FunctionArgs, InSet, JsonArrow, LikeOp,
    Literal, PlaceholderType, QualifiedName, RaiseAction, ResultColumn, SelectBody, SelectCore,
    SelectStatement, Span, TableOrSubquery, TypeName, UnaryOp, WindowSpec,
};

use crate::parser::{ParseError, Parser};
use crate::token::{Token, TokenKind};

// Binding powers: higher = tighter binding.
// Left BP is checked against min_bp; right BP is passed to recursive call.
mod bp {
    // Infix: (left, right)
    pub const OR: (u8, u8) = (1, 2);
    pub const AND: (u8, u8) = (3, 4);
    // Prefix NOT right BP:
    pub const NOT_PREFIX: u8 = 5;
    // Equality / pattern / membership:
    pub const EQUALITY: (u8, u8) = (7, 8);
    // Relational comparison:
    pub const COMPARISON: (u8, u8) = (9, 10);
    // Bitwise operators (all share one level in SQLite):
    pub const BITWISE: (u8, u8) = (13, 14);
    // Addition / subtraction:
    pub const ADD: (u8, u8) = (15, 16);
    // Multiplication / division / modulo:
    pub const MUL: (u8, u8) = (17, 18);
    // String concatenation:
    pub const CONCAT: (u8, u8) = (19, 20);
    // COLLATE (postfix left BP):
    pub const COLLATE: u8 = 21;
    // Unary prefix (- + ~) right BP:
    pub const UNARY: u8 = 23;
    // JSON access (-> ->>):
    pub const JSON: (u8, u8) = (25, 26);
}

impl Parser {
    /// Parse a single SQL expression.
    pub fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_expr_bp(0)
    }

    // ── Pratt core ──────────────────────────────────────────────────────

    fn parse_expr_bp(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        let mut lhs = self.parse_prefix()?;

        loop {
            // Postfix: COLLATE, ISNULL, NOTNULL
            if let Some(l_bp) = self.postfix_bp() {
                if l_bp < min_bp {
                    break;
                }
                lhs = self.parse_postfix(lhs)?;
                continue;
            }

            // Infix: binary operators, IS, LIKE, BETWEEN, IN, etc.
            if let Some((l_bp, r_bp)) = self.infix_bp() {
                if l_bp < min_bp {
                    break;
                }
                lhs = self.parse_infix(lhs, r_bp)?;
                continue;
            }

            break;
        }

        Ok(lhs)
    }

    // ── Token helpers ───────────────────────────────────────────────────

    fn peek_kind(&self) -> &TokenKind {
        self.tokens
            .get(self.pos)
            .map_or(&TokenKind::Eof, |t| &t.kind)
    }

    #[allow(dead_code)]
    fn peek_span(&self) -> Span {
        self.tokens.get(self.pos).map_or(Span::ZERO, |t| t.span)
    }

    fn peek_token(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance_token(&mut self) -> Token {
        let tok = self.tokens[self.pos].clone();
        if tok.kind != TokenKind::Eof {
            self.pos += 1;
        }
        tok
    }

    fn at_kind(&self, kind: &TokenKind) -> bool {
        std::mem::discriminant(self.peek_kind()) == std::mem::discriminant(kind)
    }

    fn eat_kind(&mut self, kind: &TokenKind) -> bool {
        if self.at_kind(kind) {
            self.advance_token();
            true
        } else {
            false
        }
    }

    fn expect_kind(&mut self, expected: &TokenKind) -> Result<Span, ParseError> {
        if self.at_kind(expected) {
            Ok(self.advance_token().span)
        } else {
            Err(self.err_here(format!("expected {expected:?}, got {:?}", self.peek_kind())))
        }
    }

    fn err_here(&self, message: impl Into<String>) -> ParseError {
        ParseError::at(message, self.peek_token())
    }

    // ── Prefix (nud) ────────────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    fn parse_prefix(&mut self) -> Result<Expr, ParseError> {
        let tok = self.advance_token();
        match &tok.kind {
            // ── Literals ────────────────────────────────────────────────
            TokenKind::Integer(i) => Ok(Expr::Literal(Literal::Integer(*i), tok.span)),
            TokenKind::Float(f) => Ok(Expr::Literal(Literal::Float(*f), tok.span)),
            TokenKind::String(s) => Ok(Expr::Literal(Literal::String(s.clone()), tok.span)),
            TokenKind::Blob(b) => Ok(Expr::Literal(Literal::Blob(b.clone()), tok.span)),
            TokenKind::KwNull => Ok(Expr::Literal(Literal::Null, tok.span)),
            TokenKind::KwTrue => Ok(Expr::Literal(Literal::True, tok.span)),
            TokenKind::KwFalse => Ok(Expr::Literal(Literal::False, tok.span)),
            TokenKind::KwCurrentTime => Ok(Expr::Literal(Literal::CurrentTime, tok.span)),
            TokenKind::KwCurrentDate => Ok(Expr::Literal(Literal::CurrentDate, tok.span)),
            TokenKind::KwCurrentTimestamp => Ok(Expr::Literal(Literal::CurrentTimestamp, tok.span)),

            // ── Bind parameters ─────────────────────────────────────────
            TokenKind::Question => Ok(Expr::Placeholder(PlaceholderType::Anonymous, tok.span)),
            TokenKind::QuestionNum(n) => {
                Ok(Expr::Placeholder(PlaceholderType::Numbered(*n), tok.span))
            }
            TokenKind::ColonParam(s) => Ok(Expr::Placeholder(
                PlaceholderType::ColonNamed(s.clone()),
                tok.span,
            )),
            TokenKind::AtParam(s) => Ok(Expr::Placeholder(
                PlaceholderType::AtNamed(s.clone()),
                tok.span,
            )),
            TokenKind::DollarParam(s) => Ok(Expr::Placeholder(
                PlaceholderType::DollarNamed(s.clone()),
                tok.span,
            )),

            // ── Unary prefix: - + ~ ─────────────────────────────────────
            TokenKind::Minus => {
                let inner = self.parse_expr_bp(bp::UNARY)?;
                let span = tok.span.merge(inner.span());
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Negate,
                    expr: Box::new(inner),
                    span,
                })
            }
            TokenKind::Plus => {
                let inner = self.parse_expr_bp(bp::UNARY)?;
                let span = tok.span.merge(inner.span());
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Plus,
                    expr: Box::new(inner),
                    span,
                })
            }
            TokenKind::Tilde => {
                let inner = self.parse_expr_bp(bp::UNARY)?;
                let span = tok.span.merge(inner.span());
                Ok(Expr::UnaryOp {
                    op: UnaryOp::BitNot,
                    expr: Box::new(inner),
                    span,
                })
            }

            // ── Prefix NOT ──────────────────────────────────────────────
            TokenKind::KwNot => {
                // NOT EXISTS (subquery)
                if matches!(self.peek_kind(), TokenKind::KwExists) {
                    self.advance_token();
                    self.expect_kind(&TokenKind::LeftParen)?;
                    let subquery = self.parse_subquery_minimal()?;
                    let end = self.expect_kind(&TokenKind::RightParen)?;
                    let span = tok.span.merge(end);
                    return Ok(Expr::Exists {
                        subquery: Box::new(subquery),
                        not: true,
                        span,
                    });
                }
                let inner = self.parse_expr_bp(bp::NOT_PREFIX)?;
                let span = tok.span.merge(inner.span());
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(inner),
                    span,
                })
            }

            // ── EXISTS (subquery) ───────────────────────────────────────
            TokenKind::KwExists => {
                self.expect_kind(&TokenKind::LeftParen)?;
                let subquery = self.parse_subquery_minimal()?;
                let end = self.expect_kind(&TokenKind::RightParen)?;
                let span = tok.span.merge(end);
                Ok(Expr::Exists {
                    subquery: Box::new(subquery),
                    not: false,
                    span,
                })
            }

            // ── CAST(expr AS type_name) ─────────────────────────────────
            TokenKind::KwCast => {
                self.expect_kind(&TokenKind::LeftParen)?;
                let inner = self.parse_expr()?;
                self.expect_kind(&TokenKind::KwAs)?;
                let type_name = self.parse_type_name()?;
                let end = self.expect_kind(&TokenKind::RightParen)?;
                let span = tok.span.merge(end);
                Ok(Expr::Cast {
                    expr: Box::new(inner),
                    type_name,
                    span,
                })
            }

            // ── CASE [operand] WHEN ... THEN ... [ELSE ...] END ────────
            TokenKind::KwCase => self.parse_case_expr(tok.span),

            // ── RAISE(action, message) ──────────────────────────────────
            TokenKind::KwRaise => {
                self.expect_kind(&TokenKind::LeftParen)?;
                let (action, message) = self.parse_raise_args()?;
                let end = self.expect_kind(&TokenKind::RightParen)?;
                let span = tok.span.merge(end);
                Ok(Expr::Raise {
                    action,
                    message,
                    span,
                })
            }

            // ── Parenthesized expr / subquery / row-value ───────────────
            TokenKind::LeftParen => {
                if matches!(self.peek_kind(), TokenKind::KwSelect) {
                    let subquery = self.parse_subquery_minimal()?;
                    let end = self.expect_kind(&TokenKind::RightParen)?;
                    let span = tok.span.merge(end);
                    return Ok(Expr::Subquery(Box::new(subquery), span));
                }
                let first = self.parse_expr()?;
                if self.eat_kind(&TokenKind::Comma) {
                    let mut exprs = vec![first];
                    loop {
                        exprs.push(self.parse_expr()?);
                        if !self.eat_kind(&TokenKind::Comma) {
                            break;
                        }
                    }
                    let end = self.expect_kind(&TokenKind::RightParen)?;
                    let span = tok.span.merge(end);
                    Ok(Expr::RowValue(exprs, span))
                } else {
                    self.expect_kind(&TokenKind::RightParen)?;
                    Ok(first)
                }
            }

            // ── Identifier: column ref or function call ─────────────────
            TokenKind::Id(name) | TokenKind::QuotedId(name, _) => {
                let name = name.clone();
                self.parse_ident_expr(name, tok.span)
            }

            // ── Keywords usable as function names ───────────────────────
            TokenKind::KwReplace if matches!(self.peek_kind(), TokenKind::LeftParen) => {
                self.parse_function_call("replace".to_owned(), tok.span)
            }

            _ => Err(ParseError::at(
                format!("unexpected token in expression: {:?}", tok.kind),
                Some(&tok),
            )),
        }
    }

    /// Parse `name`, `name.column`, or `name(args)`.
    fn parse_ident_expr(&mut self, name: String, start: Span) -> Result<Expr, ParseError> {
        // Function call: name(...)
        if matches!(self.peek_kind(), TokenKind::LeftParen) {
            return self.parse_function_call(name, start);
        }
        // Table-qualified column: name.column
        if matches!(self.peek_kind(), TokenKind::Dot) {
            self.advance_token();
            let col_tok = self.advance_token();
            let col_name = match &col_tok.kind {
                TokenKind::Id(c) | TokenKind::QuotedId(c, _) => c.clone(),
                TokenKind::Star => "*".to_owned(),
                _ => {
                    return Err(ParseError::at(
                        format!("expected column name after '.', got {:?}", col_tok.kind),
                        Some(&col_tok),
                    ));
                }
            };
            let span = start.merge(col_tok.span);
            return Ok(Expr::Column(ColumnRef::qualified(name, col_name), span));
        }
        Ok(Expr::Column(ColumnRef::bare(name), start))
    }

    // ── Postfix ─────────────────────────────────────────────────────────

    fn postfix_bp(&self) -> Option<u8> {
        match self.peek_kind() {
            TokenKind::KwCollate => Some(bp::COLLATE),
            TokenKind::KwIsnull | TokenKind::KwNotnull => Some(bp::EQUALITY.0),
            _ => None,
        }
    }

    fn parse_postfix(&mut self, lhs: Expr) -> Result<Expr, ParseError> {
        let tok = self.advance_token();
        match &tok.kind {
            TokenKind::KwCollate => {
                let name_tok = self.advance_token();
                let collation = match &name_tok.kind {
                    TokenKind::Id(s) | TokenKind::QuotedId(s, _) => s.clone(),
                    _ => {
                        return Err(ParseError::at(
                            "expected collation name after COLLATE",
                            Some(&name_tok),
                        ));
                    }
                };
                let span = lhs.span().merge(name_tok.span);
                Ok(Expr::Collate {
                    expr: Box::new(lhs),
                    collation,
                    span,
                })
            }
            TokenKind::KwIsnull => {
                let span = lhs.span().merge(tok.span);
                Ok(Expr::IsNull {
                    expr: Box::new(lhs),
                    not: false,
                    span,
                })
            }
            TokenKind::KwNotnull => {
                let span = lhs.span().merge(tok.span);
                Ok(Expr::IsNull {
                    expr: Box::new(lhs),
                    not: true,
                    span,
                })
            }
            other => Err(ParseError::at(
                format!("unexpected postfix token: {other:?}"),
                Some(&tok),
            )),
        }
    }

    // ── Infix ───────────────────────────────────────────────────────────

    fn infix_bp(&self) -> Option<(u8, u8)> {
        match self.peek_kind() {
            TokenKind::KwOr => Some(bp::OR),
            TokenKind::KwAnd => Some(bp::AND),

            TokenKind::Eq
            | TokenKind::EqEq
            | TokenKind::Ne
            | TokenKind::LtGt
            | TokenKind::KwIs
            | TokenKind::KwLike
            | TokenKind::KwGlob
            | TokenKind::KwMatch
            | TokenKind::KwRegexp
            | TokenKind::KwBetween
            | TokenKind::KwIn => Some(bp::EQUALITY),

            // NOT LIKE / NOT IN / NOT BETWEEN / NOT GLOB / NOT MATCH / NOT REGEXP
            TokenKind::KwNot => {
                let next = self.tokens.get(self.pos + 1).map(|t| &t.kind);
                match next {
                    Some(
                        TokenKind::KwLike
                        | TokenKind::KwGlob
                        | TokenKind::KwMatch
                        | TokenKind::KwRegexp
                        | TokenKind::KwBetween
                        | TokenKind::KwIn,
                    ) => Some(bp::EQUALITY),
                    _ => None,
                }
            }

            TokenKind::Lt | TokenKind::Le | TokenKind::Gt | TokenKind::Ge => Some(bp::COMPARISON),

            TokenKind::Ampersand
            | TokenKind::Pipe
            | TokenKind::ShiftLeft
            | TokenKind::ShiftRight => Some(bp::BITWISE),

            TokenKind::Plus | TokenKind::Minus => Some(bp::ADD),
            TokenKind::Star | TokenKind::Slash | TokenKind::Percent => Some(bp::MUL),
            TokenKind::Concat => Some(bp::CONCAT),
            TokenKind::Arrow | TokenKind::DoubleArrow => Some(bp::JSON),

            _ => None,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn parse_infix(&mut self, lhs: Expr, r_bp: u8) -> Result<Expr, ParseError> {
        let tok = self.advance_token();
        match &tok.kind {
            // ── Simple binary operators ──────────────────────────────────
            TokenKind::Plus => self.make_binop(lhs, BinaryOp::Add, r_bp),
            TokenKind::Minus => self.make_binop(lhs, BinaryOp::Subtract, r_bp),
            TokenKind::Star => self.make_binop(lhs, BinaryOp::Multiply, r_bp),
            TokenKind::Slash => self.make_binop(lhs, BinaryOp::Divide, r_bp),
            TokenKind::Percent => self.make_binop(lhs, BinaryOp::Modulo, r_bp),
            TokenKind::Concat => self.make_binop(lhs, BinaryOp::Concat, r_bp),
            TokenKind::Eq | TokenKind::EqEq => self.make_binop(lhs, BinaryOp::Eq, r_bp),
            TokenKind::Ne | TokenKind::LtGt => self.make_binop(lhs, BinaryOp::Ne, r_bp),
            TokenKind::Lt => self.make_binop(lhs, BinaryOp::Lt, r_bp),
            TokenKind::Le => self.make_binop(lhs, BinaryOp::Le, r_bp),
            TokenKind::Gt => self.make_binop(lhs, BinaryOp::Gt, r_bp),
            TokenKind::Ge => self.make_binop(lhs, BinaryOp::Ge, r_bp),
            TokenKind::Ampersand => self.make_binop(lhs, BinaryOp::BitAnd, r_bp),
            TokenKind::Pipe => self.make_binop(lhs, BinaryOp::BitOr, r_bp),
            TokenKind::ShiftLeft => self.make_binop(lhs, BinaryOp::ShiftLeft, r_bp),
            TokenKind::ShiftRight => self.make_binop(lhs, BinaryOp::ShiftRight, r_bp),
            TokenKind::KwOr => self.make_binop(lhs, BinaryOp::Or, r_bp),
            TokenKind::KwAnd => self.make_binop(lhs, BinaryOp::And, r_bp),

            // ── IS [NOT] [NULL | expr] ──────────────────────────────────
            TokenKind::KwIs => {
                let not = self.eat_kind(&TokenKind::KwNot);
                if matches!(self.peek_kind(), TokenKind::KwNull) {
                    let end = self.advance_token().span;
                    let span = lhs.span().merge(end);
                    return Ok(Expr::IsNull {
                        expr: Box::new(lhs),
                        not,
                        span,
                    });
                }
                let rhs = self.parse_expr_bp(r_bp)?;
                let span = lhs.span().merge(rhs.span());
                let op = if not { BinaryOp::IsNot } else { BinaryOp::Is };
                Ok(Expr::BinaryOp {
                    left: Box::new(lhs),
                    op,
                    right: Box::new(rhs),
                    span,
                })
            }

            // ── LIKE / GLOB / MATCH / REGEXP ────────────────────────────
            TokenKind::KwLike => self.parse_like(lhs, LikeOp::Like, false),
            TokenKind::KwGlob => self.parse_like(lhs, LikeOp::Glob, false),
            TokenKind::KwMatch => self.parse_like(lhs, LikeOp::Match, false),
            TokenKind::KwRegexp => self.parse_like(lhs, LikeOp::Regexp, false),

            // ── BETWEEN ─────────────────────────────────────────────────
            TokenKind::KwBetween => self.parse_between(lhs, false),

            // ── IN ──────────────────────────────────────────────────────
            TokenKind::KwIn => self.parse_in(lhs, false),

            // ── JSON -> / ->> ───────────────────────────────────────────
            TokenKind::Arrow => {
                let rhs = self.parse_expr_bp(r_bp)?;
                let span = lhs.span().merge(rhs.span());
                Ok(Expr::JsonAccess {
                    expr: Box::new(lhs),
                    path: Box::new(rhs),
                    arrow: JsonArrow::Arrow,
                    span,
                })
            }
            TokenKind::DoubleArrow => {
                let rhs = self.parse_expr_bp(r_bp)?;
                let span = lhs.span().merge(rhs.span());
                Ok(Expr::JsonAccess {
                    expr: Box::new(lhs),
                    path: Box::new(rhs),
                    arrow: JsonArrow::DoubleArrow,
                    span,
                })
            }

            // ── NOT LIKE / GLOB / BETWEEN / IN ──────────────────────────
            TokenKind::KwNot => {
                let next = self.advance_token();
                match &next.kind {
                    TokenKind::KwLike => self.parse_like(lhs, LikeOp::Like, true),
                    TokenKind::KwGlob => self.parse_like(lhs, LikeOp::Glob, true),
                    TokenKind::KwMatch => self.parse_like(lhs, LikeOp::Match, true),
                    TokenKind::KwRegexp => self.parse_like(lhs, LikeOp::Regexp, true),
                    TokenKind::KwBetween => self.parse_between(lhs, true),
                    TokenKind::KwIn => self.parse_in(lhs, true),
                    _ => Err(ParseError::at(
                        format!(
                            "expected LIKE/GLOB/MATCH/REGEXP/BETWEEN/IN \
                             after NOT, got {:?}",
                            next.kind
                        ),
                        Some(&next),
                    )),
                }
            }

            other => Err(ParseError::at(
                format!("unexpected infix token: {other:?}"),
                Some(&tok),
            )),
        }
    }

    fn make_binop(&mut self, lhs: Expr, op: BinaryOp, r_bp: u8) -> Result<Expr, ParseError> {
        let rhs = self.parse_expr_bp(r_bp)?;
        let span = lhs.span().merge(rhs.span());
        Ok(Expr::BinaryOp {
            left: Box::new(lhs),
            op,
            right: Box::new(rhs),
            span,
        })
    }

    // ── Special expression forms ────────────────────────────────────────

    fn parse_like(&mut self, lhs: Expr, op: LikeOp, not: bool) -> Result<Expr, ParseError> {
        let pattern = self.parse_expr_bp(bp::EQUALITY.1)?;
        let escape = if self.eat_kind(&TokenKind::KwEscape) {
            Some(Box::new(self.parse_expr_bp(bp::EQUALITY.1)?))
        } else {
            None
        };
        let end = escape.as_ref().map_or_else(|| pattern.span(), |e| e.span());
        let span = lhs.span().merge(end);
        Ok(Expr::Like {
            expr: Box::new(lhs),
            pattern: Box::new(pattern),
            escape,
            op,
            not,
            span,
        })
    }

    fn parse_between(&mut self, lhs: Expr, not: bool) -> Result<Expr, ParseError> {
        // Parse low bound above AND level so AND keyword is not consumed.
        let low = self.parse_expr_bp(bp::NOT_PREFIX)?;
        if !self.eat_kind(&TokenKind::KwAnd) {
            return Err(self.err_here("expected AND in BETWEEN expression"));
        }
        let high = self.parse_expr_bp(bp::NOT_PREFIX)?;
        let span = lhs.span().merge(high.span());
        Ok(Expr::Between {
            expr: Box::new(lhs),
            low: Box::new(low),
            high: Box::new(high),
            not,
            span,
        })
    }

    fn parse_in(&mut self, lhs: Expr, not: bool) -> Result<Expr, ParseError> {
        let start = lhs.span();

        // SQLite supports both "x IN ( ... )" and "x IN table_name".
        if !self.at_kind(&TokenKind::LeftParen) {
            let table = self.parse_qualified_name()?;
            let end = self.tokens[self.pos.saturating_sub(1)].span;
            let span = start.merge(end);
            return Ok(Expr::In {
                expr: Box::new(lhs),
                set: InSet::Table(table),
                not,
                span,
            });
        }

        self.expect_kind(&TokenKind::LeftParen)?;

        if matches!(self.peek_kind(), TokenKind::KwSelect) {
            let subquery = self.parse_subquery_minimal()?;
            let end = self.expect_kind(&TokenKind::RightParen)?;
            let span = start.merge(end);
            return Ok(Expr::In {
                expr: Box::new(lhs),
                set: InSet::Subquery(Box::new(subquery)),
                not,
                span,
            });
        }

        let mut exprs = vec![self.parse_expr()?];
        while self.eat_kind(&TokenKind::Comma) {
            exprs.push(self.parse_expr()?);
        }
        let end = self.expect_kind(&TokenKind::RightParen)?;
        let span = start.merge(end);
        Ok(Expr::In {
            expr: Box::new(lhs),
            set: InSet::List(exprs),
            not,
            span,
        })
    }

    fn parse_case_expr(&mut self, start: Span) -> Result<Expr, ParseError> {
        let operand = if matches!(self.peek_kind(), TokenKind::KwWhen) {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };

        let mut whens = Vec::new();
        while self.eat_kind(&TokenKind::KwWhen) {
            let condition = self.parse_expr()?;
            if !self.eat_kind(&TokenKind::KwThen) {
                return Err(self.err_here("expected THEN in CASE expression"));
            }
            let result = self.parse_expr()?;
            whens.push((condition, result));
        }
        if whens.is_empty() {
            return Err(self.err_here("CASE requires at least one WHEN clause"));
        }

        let else_expr = if self.eat_kind(&TokenKind::KwElse) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        if !self.eat_kind(&TokenKind::KwEnd) {
            return Err(self.err_here("expected END for CASE expression"));
        }
        let end = self.tokens[self.pos.saturating_sub(1)].span;
        let span = start.merge(end);
        Ok(Expr::Case {
            operand,
            whens,
            else_expr,
            span,
        })
    }

    fn parse_function_call(&mut self, name: String, start: Span) -> Result<Expr, ParseError> {
        self.expect_kind(&TokenKind::LeftParen)?;

        let (args, distinct) = if matches!(self.peek_kind(), TokenKind::Star) {
            self.advance_token();
            (FunctionArgs::Star, false)
        } else {
            let distinct = self.eat_kind(&TokenKind::KwDistinct);
            let args = if matches!(self.peek_kind(), TokenKind::RightParen) {
                FunctionArgs::List(Vec::new())
            } else {
                let mut list = vec![self.parse_expr()?];
                while self.eat_kind(&TokenKind::Comma) {
                    list.push(self.parse_expr()?);
                }
                FunctionArgs::List(list)
            };
            (args, distinct)
        };

        let mut end = self.expect_kind(&TokenKind::RightParen)?;
        let filter = if self.eat_kind(&TokenKind::KwFilter) {
            self.expect_kind(&TokenKind::LeftParen)?;
            self.expect_kind(&TokenKind::KwWhere)?;
            let predicate = self.parse_expr()?;
            let filter_end = self.expect_kind(&TokenKind::RightParen)?;
            end = end.merge(filter_end);
            Some(Box::new(predicate))
        } else {
            None
        };
        let over = if self.eat_kind(&TokenKind::KwOver) {
            if self.eat_kind(&TokenKind::LeftParen) {
                let spec = self.parse_window_spec()?;
                let over_end = self.expect_kind(&TokenKind::RightParen)?;
                end = end.merge(over_end);
                Some(spec)
            } else {
                let base_window = self.parse_identifier()?;
                let base_span = self.tokens[self.pos.saturating_sub(1)].span;
                end = end.merge(base_span);
                Some(WindowSpec {
                    base_window: Some(base_window),
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    frame: None,
                })
            }
        } else {
            None
        };

        let span = start.merge(end);
        Ok(Expr::FunctionCall {
            name,
            args,
            distinct,
            filter,
            over,
            span,
        })
    }

    fn parse_raise_args(&mut self) -> Result<(RaiseAction, Option<String>), ParseError> {
        let action_tok = self.advance_token();
        let action = match &action_tok.kind {
            TokenKind::KwIgnore => RaiseAction::Ignore,
            TokenKind::KwRollback => RaiseAction::Rollback,
            TokenKind::KwAbort => RaiseAction::Abort,
            TokenKind::KwFail => RaiseAction::Fail,
            _ => {
                return Err(ParseError::at(
                    "expected IGNORE, ROLLBACK, ABORT, or FAIL in RAISE",
                    Some(&action_tok),
                ));
            }
        };
        if matches!(action, RaiseAction::Ignore) {
            return Ok((action, None));
        }
        self.expect_kind(&TokenKind::Comma)?;
        let msg_tok = self.advance_token();
        let message = match &msg_tok.kind {
            TokenKind::String(s) => s.clone(),
            _ => {
                return Err(ParseError::at(
                    "expected string message in RAISE",
                    Some(&msg_tok),
                ));
            }
        };
        Ok((action, Some(message)))
    }

    fn parse_type_name(&mut self) -> Result<TypeName, ParseError> {
        let mut parts = Vec::new();
        while matches!(
            self.peek_kind(),
            TokenKind::Id(_) | TokenKind::QuotedId(_, _)
        ) {
            let tok = self.advance_token();
            if let TokenKind::Id(s) | TokenKind::QuotedId(s, _) = &tok.kind {
                parts.push(s.clone());
            } else {
                return Err(ParseError::at(
                    "expected identifier in type name",
                    Some(&tok),
                ));
            }
        }
        if parts.is_empty() {
            return Err(self.err_here("expected type name"));
        }
        let name = parts.join(" ");

        let (arg1, arg2) = if self.eat_kind(&TokenKind::LeftParen) {
            let a1 = self.parse_type_arg()?;
            let a2 = if self.eat_kind(&TokenKind::Comma) {
                Some(self.parse_type_arg()?)
            } else {
                None
            };
            self.expect_kind(&TokenKind::RightParen)?;
            (Some(a1), a2)
        } else {
            (None, None)
        };

        Ok(TypeName { name, arg1, arg2 })
    }

    fn parse_type_arg(&mut self) -> Result<String, ParseError> {
        let tok = self.advance_token();
        match &tok.kind {
            TokenKind::Integer(i) => Ok(i.to_string()),
            TokenKind::Minus => {
                let next = self.advance_token();
                match &next.kind {
                    TokenKind::Integer(i) => Ok(format!("-{i}")),
                    _ => Err(ParseError::at(
                        "expected integer in type argument",
                        Some(&next),
                    )),
                }
            }
            TokenKind::Id(s) | TokenKind::QuotedId(s, _) => Ok(s.clone()),
            _ => Err(ParseError::at("expected type argument", Some(&tok))),
        }
    }

    /// Minimal subquery parser for EXISTS/IN expression support.
    ///
    /// Handles basic `SELECT [DISTINCT] columns [FROM table] [WHERE expr]`.
    /// Full SELECT parsing is a separate bead.
    fn parse_subquery_minimal(&mut self) -> Result<SelectStatement, ParseError> {
        if !self.eat_kind(&TokenKind::KwSelect) {
            return Err(self.err_here("expected SELECT in subquery"));
        }

        let distinct = if self.eat_kind(&TokenKind::KwDistinct) {
            Distinctness::Distinct
        } else {
            let _ = self.eat_kind(&TokenKind::KwAll);
            Distinctness::All
        };

        let mut columns = Vec::new();
        loop {
            if matches!(self.peek_kind(), TokenKind::Star) {
                self.advance_token();
                columns.push(ResultColumn::Star);
            } else {
                let expr = self.parse_expr()?;
                let alias = if self.eat_kind(&TokenKind::KwAs) {
                    let tok = self.advance_token();
                    match &tok.kind {
                        TokenKind::Id(s) | TokenKind::QuotedId(s, _) => Some(s.clone()),
                        _ => return Err(ParseError::at("expected alias", Some(&tok))),
                    }
                } else {
                    None
                };
                columns.push(ResultColumn::Expr { expr, alias });
            }
            if !self.eat_kind(&TokenKind::Comma) {
                break;
            }
        }

        let from = if self.eat_kind(&TokenKind::KwFrom) {
            let tok = self.advance_token();
            let name = match &tok.kind {
                TokenKind::Id(s) | TokenKind::QuotedId(s, _) => QualifiedName::bare(s.clone()),
                _ => return Err(ParseError::at("expected table name", Some(&tok))),
            };
            Some(FromClause {
                source: TableOrSubquery::Table {
                    name,
                    alias: None,
                    index_hint: None,
                },
                joins: Vec::new(),
            })
        } else {
            None
        };

        let where_clause = if self.eat_kind(&TokenKind::KwWhere) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        Ok(SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct,
                    columns,
                    from,
                    where_clause,
                    group_by: Vec::new(),
                    having: None,
                    windows: Vec::new(),
                },
                compounds: Vec::new(),
            },
            order_by: Vec::new(),
            limit: None,
        })
    }
}

/// Parse a single expression from raw SQL text.
pub fn parse_expr(sql: &str) -> Result<Expr, ParseError> {
    let mut parser = Parser::from_sql(sql);
    let expr = parser.parse_expr()?;
    if !matches!(parser.peek_kind(), TokenKind::Eof | TokenKind::Semicolon) {
        return Err(parser.err_here(format!(
            "unexpected token after expression: {:?}",
            parser.peek_kind()
        )));
    }
    Ok(expr)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> Expr {
        match parse_expr(sql) {
            Ok(expr) => expr,
            Err(err) => unreachable!("parse error for `{sql}`: {err}"),
        }
    }

    // ── Precedence tests (normative invariants) ─────────────────────────

    #[test]
    fn test_not_lower_precedence_than_comparison() {
        // NOT x = y → NOT (x = y)
        let expr = parse("NOT x = y");
        match &expr {
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: inner,
                ..
            } => match inner.as_ref() {
                Expr::BinaryOp {
                    op: BinaryOp::Eq, ..
                } => {}
                other => unreachable!("expected Eq inside NOT, got {other:?}"),
            },
            other => unreachable!("expected NOT(Eq), got {other:?}"),
        }
    }

    #[test]
    fn test_unary_binds_tighter_than_collate() {
        // -x COLLATE NOCASE → (-x) COLLATE NOCASE
        let expr = parse("-x COLLATE NOCASE");
        match &expr {
            Expr::Collate {
                expr: inner,
                collation,
                ..
            } => {
                assert_eq!(collation, "NOCASE");
                assert!(matches!(
                    inner.as_ref(),
                    Expr::UnaryOp {
                        op: UnaryOp::Negate,
                        ..
                    }
                ));
            }
            other => unreachable!("expected COLLATE(Negate), got {other:?}"),
        }
    }

    #[test]
    fn test_arithmetic_precedence() {
        // 1 + 2 * 3 → 1 + (2 * 3)
        let expr = parse("1 + 2 * 3");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Add,
                left,
                right,
                ..
            } => {
                assert!(matches!(
                    left.as_ref(),
                    Expr::Literal(Literal::Integer(1), _)
                ));
                assert!(matches!(
                    right.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Multiply,
                        ..
                    }
                ));
            }
            other => unreachable!("expected Add(1, Mul(2,3)), got {other:?}"),
        }
    }

    #[test]
    fn test_and_higher_than_or() {
        // a OR b AND c → a OR (b AND c)
        let expr = parse("a OR b AND c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Or,
                right,
                ..
            } => {
                assert!(matches!(
                    right.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::And,
                        ..
                    }
                ));
            }
            other => unreachable!("expected Or(a, And(b,c)), got {other:?}"),
        }
    }

    // ── CAST ────────────────────────────────────────────────────────────

    #[test]
    fn test_cast_expression() {
        let expr = parse("CAST(42 AS INTEGER)");
        match &expr {
            Expr::Cast {
                expr: inner,
                type_name,
                ..
            } => {
                assert!(matches!(
                    inner.as_ref(),
                    Expr::Literal(Literal::Integer(42), _)
                ));
                assert_eq!(type_name.name, "INTEGER");
            }
            other => unreachable!("expected Cast, got {other:?}"),
        }
    }

    // ── CASE ────────────────────────────────────────────────────────────

    #[test]
    fn test_case_when_simple() {
        let expr = parse(
            "CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' \
             ELSE 'other' END",
        );
        match &expr {
            Expr::Case {
                operand: Some(op),
                whens,
                else_expr: Some(_),
                ..
            } => {
                assert!(matches!(op.as_ref(), Expr::Column(..)));
                assert_eq!(whens.len(), 2);
            }
            other => unreachable!("expected simple CASE, got {other:?}"),
        }
    }

    #[test]
    fn test_case_when_searched() {
        let expr = parse(
            "CASE WHEN x > 0 THEN 'pos' WHEN x < 0 THEN 'neg' \
             ELSE 'zero' END",
        );
        match &expr {
            Expr::Case {
                operand: None,
                whens,
                else_expr: Some(_),
                ..
            } => {
                assert_eq!(whens.len(), 2);
                assert!(matches!(
                    &whens[0].0,
                    Expr::BinaryOp {
                        op: BinaryOp::Gt,
                        ..
                    }
                ));
            }
            other => unreachable!("expected searched CASE, got {other:?}"),
        }
    }

    // ── EXISTS ──────────────────────────────────────────────────────────

    #[test]
    fn test_exists_subquery() {
        let expr = parse("EXISTS (SELECT 1)");
        assert!(matches!(expr, Expr::Exists { not: false, .. }));
    }

    #[test]
    fn test_not_exists_subquery() {
        let expr = parse("NOT EXISTS (SELECT 1)");
        assert!(matches!(expr, Expr::Exists { not: true, .. }));
    }

    // ── IN ──────────────────────────────────────────────────────────────

    #[test]
    fn test_in_expr_list() {
        let expr = parse("x IN (1, 2, 3)");
        match &expr {
            Expr::In {
                not: false,
                set: InSet::List(items),
                ..
            } => assert_eq!(items.len(), 3),
            other => unreachable!("expected IN list, got {other:?}"),
        }
    }

    #[test]
    fn test_in_subquery() {
        let expr = parse("x IN (SELECT y FROM t)");
        assert!(matches!(
            expr,
            Expr::In {
                not: false,
                set: InSet::Subquery(_),
                ..
            }
        ));
    }

    #[test]
    fn test_not_in() {
        let expr = parse("x NOT IN (1, 2)");
        assert!(matches!(expr, Expr::In { not: true, .. }));
    }

    #[test]
    fn test_in_table_name() {
        let expr = parse("x IN t");
        assert!(matches!(
            expr,
            Expr::In {
                not: false,
                set: InSet::Table(_),
                ..
            }
        ));
    }

    #[test]
    fn test_not_in_table_name() {
        let expr = parse("x NOT IN t");
        assert!(matches!(
            expr,
            Expr::In {
                not: true,
                set: InSet::Table(_),
                ..
            }
        ));
    }

    #[test]
    fn test_in_schema_table_name() {
        let expr = parse("x IN main.t");
        match expr {
            Expr::In {
                set: InSet::Table(name),
                ..
            } => {
                assert_eq!(name.schema.as_deref(), Some("main"));
                assert_eq!(name.name, "t");
            }
            other => unreachable!("expected IN table form, got {other:?}"),
        }
    }

    // ── BETWEEN ─────────────────────────────────────────────────────────

    #[test]
    fn test_between_and() {
        let expr = parse("x BETWEEN 1 AND 10");
        assert!(matches!(expr, Expr::Between { not: false, .. }));
    }

    #[test]
    fn test_not_between() {
        let expr = parse("x NOT BETWEEN 1 AND 10");
        assert!(matches!(expr, Expr::Between { not: true, .. }));
    }

    #[test]
    fn test_between_does_not_consume_outer_and() {
        // x BETWEEN 1 AND 10 AND y = 1 → (BETWEEN) AND (y = 1)
        let expr = parse("x BETWEEN 1 AND 10 AND y = 1");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::And,
                left,
                ..
            } => assert!(matches!(left.as_ref(), Expr::Between { .. })),
            other => unreachable!("expected AND(BETWEEN, Eq), got {other:?}"),
        }
    }

    // ── LIKE / GLOB ─────────────────────────────────────────────────────

    #[test]
    fn test_like_pattern() {
        let expr = parse("name LIKE '%foo%'");
        assert!(matches!(
            expr,
            Expr::Like {
                op: LikeOp::Like,
                not: false,
                escape: None,
                ..
            }
        ));
    }

    #[test]
    fn test_like_escape() {
        let expr = parse("name LIKE '%\\%%' ESCAPE '\\'");
        assert!(matches!(
            expr,
            Expr::Like {
                op: LikeOp::Like,
                escape: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn test_glob_pattern() {
        let expr = parse("path GLOB '*.rs'");
        assert!(matches!(
            expr,
            Expr::Like {
                op: LikeOp::Glob,
                not: false,
                ..
            }
        ));
    }

    #[test]
    fn test_glob_character_class() {
        let expr = parse("name GLOB '[a-z]*'");
        match &expr {
            Expr::Like {
                op: LikeOp::Glob,
                pattern,
                ..
            } => assert!(matches!(
                pattern.as_ref(),
                Expr::Literal(Literal::String(s), _) if s == "[a-z]*"
            )),
            other => unreachable!("expected GLOB, got {other:?}"),
        }
    }

    // ── COLLATE ─────────────────────────────────────────────────────────

    #[test]
    fn test_collate_override() {
        let expr = parse("name COLLATE NOCASE");
        match &expr {
            Expr::Collate { collation, .. } => {
                assert_eq!(collation, "NOCASE");
            }
            other => unreachable!("expected COLLATE, got {other:?}"),
        }
    }

    // ── JSON operators ──────────────────────────────────────────────────

    #[test]
    fn test_json_arrow_operator() {
        let expr = parse("data -> 'key'");
        assert!(matches!(
            expr,
            Expr::JsonAccess {
                arrow: JsonArrow::Arrow,
                ..
            }
        ));
    }

    #[test]
    fn test_json_double_arrow_operator() {
        let expr = parse("data ->> 'key'");
        assert!(matches!(
            expr,
            Expr::JsonAccess {
                arrow: JsonArrow::DoubleArrow,
                ..
            }
        ));
    }

    // ── IS NULL / ISNULL / NOTNULL ──────────────────────────────────────

    #[test]
    fn test_is_null() {
        assert!(matches!(
            parse("x IS NULL"),
            Expr::IsNull { not: false, .. }
        ));
    }

    #[test]
    fn test_is_not_null() {
        assert!(matches!(
            parse("x IS NOT NULL"),
            Expr::IsNull { not: true, .. }
        ));
    }

    #[test]
    fn test_isnull_keyword() {
        assert!(matches!(parse("x ISNULL"), Expr::IsNull { not: false, .. }));
    }

    #[test]
    fn test_notnull_keyword() {
        assert!(matches!(parse("x NOTNULL"), Expr::IsNull { not: true, .. }));
    }

    // ── Function calls ──────────────────────────────────────────────────

    #[test]
    fn test_function_call() {
        let expr = parse("max(a, b)");
        match &expr {
            Expr::FunctionCall { name, args, .. } => {
                assert_eq!(name, "max");
                match args {
                    FunctionArgs::List(v) => assert_eq!(v.len(), 2),
                    FunctionArgs::Star => unreachable!("expected arg list"),
                }
            }
            other => unreachable!("expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn test_count_star() {
        let expr = parse("count(*)");
        assert!(matches!(
            expr,
            Expr::FunctionCall {
                args: FunctionArgs::Star,
                ..
            }
        ));
    }

    #[test]
    fn test_count_distinct() {
        let expr = parse("count(DISTINCT x)");
        assert!(matches!(expr, Expr::FunctionCall { distinct: true, .. }));
    }

    #[test]
    fn test_function_call_filter_clause() {
        let expr = parse("count(x) FILTER (WHERE x > 0)");
        match expr {
            Expr::FunctionCall {
                filter: Some(filter),
                over: None,
                ..
            } => {
                assert!(matches!(
                    filter.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Gt,
                        ..
                    }
                ));
            }
            other => unreachable!("expected function call with FILTER, got {other:?}"),
        }
    }

    #[test]
    fn test_function_call_over_named_window() {
        let expr = parse("sum(x) OVER win");
        match expr {
            Expr::FunctionCall {
                over: Some(over),
                filter: None,
                ..
            } => {
                assert_eq!(over.base_window.as_deref(), Some("win"));
                assert!(over.partition_by.is_empty());
                assert!(over.order_by.is_empty());
                assert!(over.frame.is_none());
            }
            other => unreachable!("expected function call with OVER win, got {other:?}"),
        }
    }

    #[test]
    fn test_function_call_over_window_spec() {
        let expr = parse(
            "sum(x) OVER (PARTITION BY y ORDER BY z \
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        );
        match expr {
            Expr::FunctionCall {
                over: Some(over), ..
            } => {
                assert!(over.base_window.is_none());
                assert_eq!(over.partition_by.len(), 1);
                assert_eq!(over.order_by.len(), 1);
                match over.frame {
                    Some(fsqlite_ast::FrameSpec {
                        frame_type: fsqlite_ast::FrameType::Rows,
                        start: fsqlite_ast::FrameBound::Preceding(expr),
                        end: Some(fsqlite_ast::FrameBound::CurrentRow),
                        ..
                    }) => {
                        assert!(matches!(
                            expr.as_ref(),
                            Expr::Literal(Literal::Integer(1), _)
                        ));
                    }
                    other => unreachable!("expected ROWS frame, got {other:?}"),
                }
            }
            other => unreachable!("expected function call with OVER spec, got {other:?}"),
        }
    }

    #[test]
    fn test_function_call_filter_then_over() {
        let expr = parse("sum(x) FILTER (WHERE x > 10) OVER win");
        match expr {
            Expr::FunctionCall {
                filter: Some(_),
                over: Some(over),
                ..
            } => assert_eq!(over.base_window.as_deref(), Some("win")),
            other => unreachable!("expected FILTER + OVER, got {other:?}"),
        }
    }

    // ── Literals & placeholders ─────────────────────────────────────────

    #[test]
    fn test_literals() {
        assert!(matches!(
            parse("42"),
            Expr::Literal(Literal::Integer(42), _)
        ));
        assert!(matches!(parse("3.14"), Expr::Literal(Literal::Float(_), _)));
        assert!(matches!(
            parse("'hello'"),
            Expr::Literal(Literal::String(_), _)
        ));
        assert!(matches!(parse("NULL"), Expr::Literal(Literal::Null, _)));
        assert!(matches!(parse("TRUE"), Expr::Literal(Literal::True, _)));
        assert!(matches!(parse("FALSE"), Expr::Literal(Literal::False, _)));
    }

    #[test]
    fn test_placeholders() {
        assert!(matches!(
            parse("?"),
            Expr::Placeholder(PlaceholderType::Anonymous, _)
        ));
        assert!(matches!(
            parse("?1"),
            Expr::Placeholder(PlaceholderType::Numbered(1), _)
        ));
        assert!(matches!(
            parse(":name"),
            Expr::Placeholder(PlaceholderType::ColonNamed(_), _)
        ));
    }

    // ── Column references ───────────────────────────────────────────────

    #[test]
    fn test_column_bare() {
        match &parse("x") {
            Expr::Column(
                ColumnRef {
                    table: None,
                    column,
                },
                _,
            ) => assert_eq!(column, "x"),
            other => unreachable!("expected bare column, got {other:?}"),
        }
    }

    #[test]
    fn test_column_qualified() {
        match &parse("t.x") {
            Expr::Column(
                ColumnRef {
                    table: Some(t),
                    column,
                },
                _,
            ) => {
                assert_eq!(t, "t");
                assert_eq!(column, "x");
            }
            other => unreachable!("expected qualified column, got {other:?}"),
        }
    }

    // ── Concat / precedence ─────────────────────────────────────────────

    #[test]
    fn test_concat_higher_than_add() {
        // a + b || c → a + (b || c) since || binds tighter
        let expr = parse("a + b || c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Add,
                right,
                ..
            } => assert!(matches!(
                right.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOp::Concat,
                    ..
                }
            )),
            other => unreachable!("expected Add(a, Concat(b,c)), got {other:?}"),
        }
    }

    // ── Parenthesized ───────────────────────────────────────────────────

    #[test]
    fn test_parenthesized() {
        // (1 + 2) * 3 → Mul(Add(1,2), 3)
        let expr = parse("(1 + 2) * 3");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Multiply,
                left,
                ..
            } => assert!(matches!(
                left.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOp::Add,
                    ..
                }
            )),
            other => unreachable!("expected Mul(Add, 3), got {other:?}"),
        }
    }

    // ── IS / IS NOT ─────────────────────────────────────────────────────

    #[test]
    fn test_is_operator() {
        assert!(matches!(
            parse("a IS b"),
            Expr::BinaryOp {
                op: BinaryOp::Is,
                ..
            }
        ));
    }

    #[test]
    fn test_is_not_operator() {
        assert!(matches!(
            parse("a IS NOT b"),
            Expr::BinaryOp {
                op: BinaryOp::IsNot,
                ..
            }
        ));
    }

    // ── Bitwise ─────────────────────────────────────────────────────────

    #[test]
    fn test_bitwise_ops() {
        // & and | share the same precedence (left-associative)
        let expr = parse("a & b | c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::BitOr,
                left,
                ..
            } => assert!(matches!(
                left.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOp::BitAnd,
                    ..
                }
            )),
            other => unreachable!("expected BitOr(BitAnd, c), got {other:?}"),
        }
    }

    #[test]
    fn test_bitnot() {
        assert!(matches!(
            parse("~x"),
            Expr::UnaryOp {
                op: UnaryOp::BitNot,
                ..
            }
        ));
    }

    // ── Complex expressions ─────────────────────────────────────────────

    #[test]
    fn test_complex_where_clause() {
        let expr = parse("a > 1 AND b LIKE '%test%' OR NOT c IS NULL");
        assert!(matches!(
            expr,
            Expr::BinaryOp {
                op: BinaryOp::Or,
                ..
            }
        ));
    }

    #[test]
    fn test_not_like_pattern() {
        assert!(matches!(
            parse("name NOT LIKE '%foo'"),
            Expr::Like {
                op: LikeOp::Like,
                not: true,
                ..
            }
        ));
    }

    #[test]
    fn test_subquery_expr() {
        assert!(matches!(parse("(SELECT 1)"), Expr::Subquery(..)));
    }

    // ── bd-kzat: §10.2 Pratt Precedence Validation ─────────────────────
    //
    // Systematic tests for ALL 11 operator precedence levels.
    // Each level gets a dedicated associativity test and a boundary test
    // against the adjacent level.

    // Level 1: OR — left-associative
    #[test]
    fn test_pratt_level1_or_left_assoc() {
        // a OR b OR c → (a OR b) OR c
        let expr = parse("a OR b OR c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Or,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Or,
                        ..
                    }
                ),
                "OR should be left-associative"
            ),
            other => unreachable!("expected Or(Or(a,b), c), got {other:?}"),
        }
    }

    // Level 2: AND — left-associative, tighter than OR
    #[test]
    fn test_pratt_level2_and_left_assoc() {
        // a AND b AND c → (a AND b) AND c
        let expr = parse("a AND b AND c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::And,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::And,
                        ..
                    }
                ),
                "AND should be left-associative"
            ),
            other => unreachable!("expected And(And(a,b), c), got {other:?}"),
        }
    }

    // Level 3: NOT — prefix, higher than AND, lower than equality
    #[test]
    fn test_pratt_level3_not_higher_than_and() {
        // NOT a AND b → (NOT a) AND b
        let expr = parse("NOT a AND b");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::And,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::UnaryOp {
                        op: UnaryOp::Not,
                        ..
                    }
                ),
                "NOT should bind tighter than AND"
            ),
            other => unreachable!("expected And(Not(a), b), got {other:?}"),
        }
    }

    // Level 4: Equality/membership — left-associative
    #[test]
    fn test_pratt_level4_equality_left_assoc() {
        // a = b != c → (a = b) != c
        let expr = parse("a = b != c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Ne,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Eq,
                        ..
                    }
                ),
                "equality operators should be left-associative at same level"
            ),
            other => unreachable!("expected Ne(Eq(a,b), c), got {other:?}"),
        }
    }

    // Level 4 vs Level 5: THE CRITICAL BOUNDARY
    // Equality (level 4) and relational (level 5) are SEPARATE levels
    // per canonical upstream SQLite grammar.
    #[test]
    fn test_pratt_level4_vs_level5_eq_lt_boundary() {
        // a = b < c MUST parse as a = (b < c), NOT (a = b) < c
        // This is the normative invariant from §10.2.
        let expr = parse("a = b < c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Eq,
                right,
                ..
            } => assert!(
                matches!(
                    right.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Lt,
                        ..
                    }
                ),
                "a = b < c MUST parse as a = (b < c): relational binds tighter"
            ),
            other => unreachable!("expected Eq(a, Lt(b,c)), got {other:?}"),
        }
    }

    // Reverse direction of the same boundary
    #[test]
    fn test_pratt_level4_vs_level5_ne_ge_boundary() {
        // a != b >= c → a != (b >= c)
        let expr = parse("a != b >= c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Ne,
                right,
                ..
            } => assert!(
                matches!(
                    right.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Ge,
                        ..
                    }
                ),
                "a != b >= c must parse as a != (b >= c)"
            ),
            other => unreachable!("expected Ne(a, Ge(b,c)), got {other:?}"),
        }
    }

    // Level 5: Relational — left-associative
    #[test]
    fn test_pratt_level5_relational_left_assoc() {
        // a < b >= c → (a < b) >= c
        let expr = parse("a < b >= c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Ge,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Lt,
                        ..
                    }
                ),
                "relational operators should be left-associative"
            ),
            other => unreachable!("expected Ge(Lt(a,b), c), got {other:?}"),
        }
    }

    // Level 6: Bitwise — tighter than relational
    #[test]
    fn test_pratt_level6_bitwise_tighter_than_comparison() {
        // a < b & c → a < (b & c)
        let expr = parse("a < b & c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Lt,
                right,
                ..
            } => assert!(
                matches!(
                    right.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::BitAnd,
                        ..
                    }
                ),
                "bitwise should bind tighter than relational"
            ),
            other => unreachable!("expected Lt(a, BitAnd(b,c)), got {other:?}"),
        }
    }

    // Level 6: Shift operators left-associative
    #[test]
    fn test_pratt_level6_shifts_left_assoc() {
        // a << b >> c → (a << b) >> c
        let expr = parse("a << b >> c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::ShiftRight,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::ShiftLeft,
                        ..
                    }
                ),
                "shift operators should be left-associative"
            ),
            other => unreachable!("expected ShiftRight(ShiftLeft(a,b), c), got {other:?}"),
        }
    }

    // Level 7: Addition/subtraction — left-associative, tighter than bitwise
    #[test]
    fn test_pratt_level7_add_sub_left_assoc() {
        // a + b - c → (a + b) - c
        let expr = parse("a + b - c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Subtract,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Add,
                        ..
                    }
                ),
                "add/sub should be left-associative"
            ),
            other => unreachable!("expected Sub(Add(a,b), c), got {other:?}"),
        }
    }

    #[test]
    fn test_pratt_level7_tighter_than_bitwise() {
        // a & b + c → a & (b + c)
        let expr = parse("a & b + c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::BitAnd,
                right,
                ..
            } => assert!(
                matches!(
                    right.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Add,
                        ..
                    }
                ),
                "addition should bind tighter than bitwise"
            ),
            other => unreachable!("expected BitAnd(a, Add(b,c)), got {other:?}"),
        }
    }

    // Level 8: Multiplication/division/modulo — left-associative
    #[test]
    fn test_pratt_level8_mul_div_left_assoc() {
        // a * b / c → (a * b) / c
        let expr = parse("a * b / c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Divide,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Multiply,
                        ..
                    }
                ),
                "mul/div should be left-associative"
            ),
            other => unreachable!("expected Div(Mul(a,b), c), got {other:?}"),
        }
    }

    #[test]
    fn test_pratt_level8_modulo() {
        // a * b % c → (a * b) % c
        let expr = parse("a * b % c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Modulo,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Multiply,
                        ..
                    }
                ),
                "modulo and multiply at same level, left-associative"
            ),
            other => unreachable!("expected Mod(Mul(a,b), c), got {other:?}"),
        }
    }

    // Level 9: Concatenation (||) — left-associative, tighter than mul
    #[test]
    fn test_pratt_level9_concat_left_assoc() {
        // a || b || c → (a || b) || c
        let expr = parse("a || b || c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Concat,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Concat,
                        ..
                    }
                ),
                "concatenation should be left-associative"
            ),
            other => unreachable!("expected Concat(Concat(a,b), c), got {other:?}"),
        }
    }

    #[test]
    fn test_pratt_level9_tighter_than_mul() {
        // a * b || c → a * (b || c)
        let expr = parse("a * b || c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Multiply,
                right,
                ..
            } => assert!(
                matches!(
                    right.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Concat,
                        ..
                    }
                ),
                "concat should bind tighter than multiply"
            ),
            other => unreachable!("expected Mul(a, Concat(b,c)), got {other:?}"),
        }
    }

    // Level 10: COLLATE — postfix, tighter than concat
    #[test]
    fn test_pratt_level10_collate_tighter_than_concat() {
        // a || b COLLATE NOCASE → a || (b COLLATE NOCASE)
        let expr = parse("a || b COLLATE NOCASE");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Concat,
                right,
                ..
            } => assert!(
                matches!(right.as_ref(), Expr::Collate { .. }),
                "COLLATE should bind tighter than concat"
            ),
            other => unreachable!("expected Concat(a, Collate(b)), got {other:?}"),
        }
    }

    // Level 11: Unary prefix (- + ~) — tightest of all
    #[test]
    fn test_pratt_level11_unary_negate_tightest() {
        // -a * b → (-a) * b
        let expr = parse("-a * b");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Multiply,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::UnaryOp {
                        op: UnaryOp::Negate,
                        ..
                    }
                ),
                "unary minus should bind tighter than multiply"
            ),
            other => unreachable!("expected Mul(Negate(a), b), got {other:?}"),
        }
    }

    #[test]
    fn test_pratt_level11_bitnot_tightest() {
        // ~a + b → (~a) + b
        let expr = parse("~a + b");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Add,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::UnaryOp {
                        op: UnaryOp::BitNot,
                        ..
                    }
                ),
                "bitwise NOT should bind tighter than addition"
            ),
            other => unreachable!("expected Add(BitNot(a), b), got {other:?}"),
        }
    }

    // ESCAPE is NOT a standalone infix operator — it's suffix of LIKE/GLOB
    #[test]
    fn test_pratt_escape_not_infix_operator() {
        // a LIKE b ESCAPE c → Like(a, b, escape=c)
        let expr = parse("a LIKE b ESCAPE c");
        match &expr {
            Expr::Like {
                escape: Some(esc), ..
            } => assert!(
                matches!(esc.as_ref(), Expr::Column(..)),
                "ESCAPE should be parsed as suffix of LIKE, not standalone infix"
            ),
            other => unreachable!("expected Like with escape, got {other:?}"),
        }
    }

    #[test]
    fn test_pratt_escape_glob_not_infix() {
        // a GLOB b ESCAPE c → Like(a, b, op=Glob, escape=c)
        let expr = parse("a GLOB b ESCAPE c");
        match &expr {
            Expr::Like {
                op: LikeOp::Glob,
                escape: Some(_),
                ..
            } => {}
            other => unreachable!("expected Glob with escape, got {other:?}"),
        }
    }

    // Error recovery: multiple errors collected in one pass
    #[test]
    fn test_pratt_error_recovery_multiple_errors() {
        use crate::parser::Parser;
        let mut p = Parser::from_sql("SELECT +; SELECT *; SELECT 1");
        let (stmts, errs) = p.parse_all();
        // SELECT + fails (missing operand), SELECT * fails (no FROM for bare *),
        // SELECT 1 should succeed.
        assert!(
            !stmts.is_empty(),
            "should recover and parse at least one valid statement"
        );
        assert!(
            !errs.is_empty(),
            "should collect at least one error from malformed statements"
        );
    }

    // Complex mixed expression: full 11-level test
    #[test]
    fn test_pratt_complex_mixed_all_levels() {
        // NOT a = b + c * -d OR e < f AND g LIKE h
        // → (NOT (a = (b + (c * (-d))))) OR ((e < f) AND (g LIKE h))
        let expr = parse("NOT a = b + c * -d OR e < f AND g LIKE h");
        // Top level: OR
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Or,
                left,
                right,
                ..
            } => {
                // left = NOT (a = (b + (c * (-d))))
                assert!(
                    matches!(
                        left.as_ref(),
                        Expr::UnaryOp {
                            op: UnaryOp::Not,
                            ..
                        }
                    ),
                    "left of OR should be NOT(...)"
                );
                // right = (e < f) AND (g LIKE h)
                match right.as_ref() {
                    Expr::BinaryOp {
                        op: BinaryOp::And,
                        left: and_left,
                        right: and_right,
                        ..
                    } => {
                        assert!(
                            matches!(
                                and_left.as_ref(),
                                Expr::BinaryOp {
                                    op: BinaryOp::Lt,
                                    ..
                                }
                            ),
                            "left of AND should be Lt(e,f)"
                        );
                        assert!(
                            matches!(and_right.as_ref(), Expr::Like { .. }),
                            "right of AND should be Like(g,h)"
                        );
                    }
                    other => unreachable!("expected And(Lt, Like), got {other:?}"),
                }

                // Drill into the NOT to verify deeper structure:
                // NOT → Eq → right = Add → right = Mul → right = Negate
                if let Expr::UnaryOp {
                    expr: not_inner, ..
                } = left.as_ref()
                {
                    if let Expr::BinaryOp {
                        op: BinaryOp::Eq,
                        right: eq_right,
                        ..
                    } = not_inner.as_ref()
                    {
                        if let Expr::BinaryOp {
                            op: BinaryOp::Add,
                            right: add_right,
                            ..
                        } = eq_right.as_ref()
                        {
                            if let Expr::BinaryOp {
                                op: BinaryOp::Multiply,
                                right: mul_right,
                                ..
                            } = add_right.as_ref()
                            {
                                assert!(
                                    matches!(
                                        mul_right.as_ref(),
                                        Expr::UnaryOp {
                                            op: UnaryOp::Negate,
                                            ..
                                        }
                                    ),
                                    "deepest: negate"
                                );
                            } else {
                                unreachable!("expected Mul in add_right");
                            }
                        } else {
                            unreachable!("expected Add in eq_right");
                        }
                    } else {
                        unreachable!("expected Eq inside NOT");
                    }
                }
            }
            other => unreachable!("expected Or(Not(...), And(...)), got {other:?}"),
        }
    }

    // JSON operators at highest infix precedence
    #[test]
    fn test_pratt_json_highest_infix() {
        // a || b -> c → a || (b -> c) since JSON binds tightest
        let expr = parse("a || b -> c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Concat,
                right,
                ..
            } => assert!(
                matches!(
                    right.as_ref(),
                    Expr::JsonAccess {
                        arrow: JsonArrow::Arrow,
                        ..
                    }
                ),
                "JSON -> should bind tighter than concat"
            ),
            other => unreachable!("expected Concat(a, JsonAccess(b,c)), got {other:?}"),
        }
    }
}
