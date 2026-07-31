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
//   || -> ->> (left-associative; same precedence level)
//   COLLATE (postfix)
//   ~ - + (unary prefix)

use fsqlite_ast::{
    BinaryOp, ColumnRef, Expr, FunctionArgs, InSet, JsonArrow, LikeOp, Literal, PlaceholderType,
    RaiseAction, ResultColumn, SelectCore, SelectStatement, Span, TypeName, UnaryOp, WindowSpec,
};
use fsqlite_types::limits::MAX_EXPR_DEPTH;
use std::sync::Arc;

use crate::parser::{ParseError, Parser, is_nonreserved_kw, kw_to_str};
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
    // JSON access (-> ->>): Same as CONCAT
    pub const JSON: (u8, u8) = (19, 20);
}

/// An expression paired with its logical AST height.
///
/// Height is tracked while the Pratt parser reduces nodes so a wide,
/// left-associative chain does not need an O(n²) sequence of AST walks.
/// Parentheses do not create an [`Expr`] node and therefore do not increase
/// this value.
struct ParsedExpr {
    node: Expr,
    height: u32,
}

impl ParsedExpr {
    fn leaf(node: Expr) -> Self {
        Self { node, height: 1 }
    }

    fn span(&self) -> Span {
        self.node.span()
    }

    fn into_node(self) -> Expr {
        self.node
    }
}

/// Work item for the SQLite-compatible expression-height calculation.
///
/// This is deliberately one non-recursive worklist for both expressions and
/// SELECTs. SQLite 3.52.0's `exprSetHeight()` delegates subqueries to
/// `heightOfSelect()`, which walks every compound term but only its result
/// expressions, WHERE, HAVING, LIMIT, GROUP BY, and ORDER BY. In particular it
/// does not descend into CTE bodies or FROM-clause subqueries.
enum HeightWork<'a> {
    Expr(&'a Expr, u32),
    Select(&'a SelectStatement, u32),
}

fn push_select_core_height_work<'a>(
    core: &'a SelectCore,
    expr_depth: u32,
    pending: &mut Vec<HeightWork<'a>>,
    max_height: &mut u32,
) {
    match core {
        SelectCore::Select {
            columns,
            where_clause,
            group_by,
            having,
            ..
        } => {
            for column in columns {
                match column {
                    ResultColumn::Star => *max_height = (*max_height).max(expr_depth),
                    // Canonical SQLite represents `table.*` as TK_DOT with
                    // TK_ID/TK_ASTERISK children, hence height 2.
                    ResultColumn::TableStar(_) => {
                        *max_height = (*max_height).max(expr_depth.saturating_add(1));
                    }
                    ResultColumn::Expr { expr, .. } => {
                        pending.push(HeightWork::Expr(expr, expr_depth));
                    }
                }
            }

            pending.extend(
                group_by
                    .iter()
                    .map(|expr| HeightWork::Expr(expr, expr_depth)),
            );
            if let Some(having) = having {
                pending.push(HeightWork::Expr(having, expr_depth));
            }

            if let Some(where_clause) = where_clause {
                pending.push(HeightWork::Expr(where_clause, expr_depth));
            }
        }
        SelectCore::Values(rows) => {
            for row in rows {
                pending.extend(row.iter().map(|expr| HeightWork::Expr(expr, expr_depth)));
            }
        }
    }
}

/// Return whether SQLite's parser-side `EP_HasFunc` flag is observable at the
/// root of `expr`.
///
/// This is intentionally not a generic "contains a function" walk. SQLite
/// attaches COLLATE and BETWEEN metadata after their root flags are computed,
/// and TK_VECTOR propagates flags only from its first element. SQLite's
/// parser-time boolean simplifier inspects this exact root flag.
fn sqlite_expr_has_function(expr: &Expr) -> bool {
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            Expr::FunctionCall { .. } | Expr::Like { .. } | Expr::JsonAccess { .. } => {
                return true;
            }
            Expr::Literal(
                Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp,
                _,
            ) => return true,
            Expr::BinaryOp { left, right, .. } => {
                pending.push(left);
                pending.push(right);
            }
            Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
                pending.push(expr);
            }
            Expr::Between { expr, .. } => pending.push(expr),
            Expr::In { expr, set, .. } => {
                pending.push(expr);
                if let InSet::List(items) = set {
                    pending.extend(items);
                }
            }
            Expr::Case {
                operand,
                whens,
                else_expr,
                ..
            } => {
                if let Some(operand) = operand {
                    pending.push(operand);
                }
                for (condition, result) in whens {
                    pending.push(condition);
                    pending.push(result);
                }
                if let Some(else_expr) = else_expr {
                    pending.push(else_expr);
                }
            }
            Expr::RowValue(items, _) => {
                if let Some(first) = items.first() {
                    pending.push(first);
                }
            }
            Expr::Collate { .. }
            | Expr::Exists { .. }
            | Expr::Subquery(..)
            | Expr::Literal(..)
            | Expr::Column(..)
            | Expr::Raise { .. }
            | Expr::Placeholder(..) => {}
        }
    }
    false
}

/// Mirror the constant-expression predicate used by SQLite's singleton-IN
/// rewrite without recursive Rust calls.
fn sqlite_expr_is_constant_for_in(expr: &Expr) -> bool {
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            Expr::Literal(
                Literal::Integer(_)
                | Literal::Float(_)
                | Literal::String(_)
                | Literal::Blob(_)
                | Literal::Null
                | Literal::True
                | Literal::False,
                _,
            ) => {}
            Expr::UnaryOp { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::IsNull { expr, .. } => pending.push(expr),
            Expr::BinaryOp { left, right, .. } => {
                pending.push(left);
                pending.push(right);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                pending.push(expr);
                pending.push(low);
                pending.push(high);
            }
            Expr::In { expr, set, .. } => {
                pending.push(expr);
                if let InSet::List(items) = set {
                    pending.extend(items);
                } else {
                    return false;
                }
            }
            Expr::Case {
                operand,
                whens,
                else_expr,
                ..
            } => {
                if let Some(operand) = operand {
                    pending.push(operand);
                }
                for (condition, result) in whens {
                    pending.push(condition);
                    pending.push(result);
                }
                if let Some(else_expr) = else_expr {
                    pending.push(else_expr);
                }
            }
            Expr::RowValue(items, _) => pending.extend(items),
            Expr::Literal(
                Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp,
                _,
            )
            | Expr::Column(..)
            | Expr::Exists { .. }
            | Expr::Subquery(..)
            | Expr::FunctionCall { .. }
            | Expr::Like { .. }
            | Expr::JsonAccess { .. }
            | Expr::Raise { .. }
            | Expr::Placeholder(..) => return false,
        }
    }
    true
}

fn sqlite_null_test_fold_value(expr: &Expr, is_not_null: bool) -> Option<i64> {
    let mut expr = expr;
    while let Expr::UnaryOp {
        op: UnaryOp::Plus | UnaryOp::Negate,
        expr: inner,
        ..
    } = expr
    {
        expr = inner;
    }

    matches!(
        expr,
        Expr::Literal(
            Literal::Integer(_) | Literal::Float(_) | Literal::String(_) | Literal::Blob(_),
            _
        )
    )
    .then_some(i64::from(is_not_null))
}

fn sqlite_direct_false(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(Literal::Integer(0), _))
}

/// Return whether a keyword token is accepted by SQLite 3.46.1's
/// `nm ::= idj | STRING` production for a `RAISE()` message.
///
/// `idj` accepts `INDEXED`, every `JOIN_KW`, and `ID`. Lemon's `%fallback ID`
/// declaration additionally turns only the keyword classes listed below into
/// `ID` in this parser state. Keep this separate from the parser's broader
/// `is_nonreserved_kw` convenience predicate: that predicate both rejects
/// valid `nm` tokens such as `BEGIN` and accepts reserved tokens such as
/// `TABLE`, `RETURNING`, and `TRANSACTION`.
fn is_sqlite_346_raise_nm_keyword(kind: &TokenKind) -> bool {
    matches!(
        kind,
        // idj ::= INDEXED | JOIN_KW (plain and quoted IDs are handled by the
        // caller because they carry the message text directly).
        TokenKind::KwIndexed
            | TokenKind::KwCross
            | TokenKind::KwFull
            | TokenKind::KwInner
            | TokenKind::KwLeft
            | TokenKind::KwNatural
            | TokenKind::KwOuter
            | TokenKind::KwRight
            // SQLite 3.46.1's canonical `%fallback ID` list.
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
            | TokenKind::KwCast
            | TokenKind::KwColumn
            | TokenKind::KwConflict
            | TokenKind::KwCurrentDate
            | TokenKind::KwCurrentTime
            | TokenKind::KwCurrentTimestamp
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
            | TokenKind::KwFirst
            | TokenKind::KwFilter
            | TokenKind::KwFollowing
            | TokenKind::KwFor
            | TokenKind::KwGenerated
            | TokenKind::KwGlob
            | TokenKind::KwGroups
            | TokenKind::KwIf
            | TokenKind::KwIgnore
            | TokenKind::KwImmediate
            | TokenKind::KwInitially
            | TokenKind::KwInstead
            | TokenKind::KwKey
            | TokenKind::KwLast
            | TokenKind::KwLike
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
            | TokenKind::KwRaise
            | TokenKind::KwRange
            | TokenKind::KwRecursive
            | TokenKind::KwRegexp
            | TokenKind::KwReindex
            | TokenKind::KwRelease
            | TokenKind::KwRename
            | TokenKind::KwReplace
            | TokenKind::KwRestrict
            | TokenKind::KwRollback
            | TokenKind::KwRow
            | TokenKind::KwRows
            | TokenKind::KwSavepoint
            | TokenKind::KwTemp
            | TokenKind::KwTemporary
            | TokenKind::KwTies
            | TokenKind::KwTrigger
            | TokenKind::KwUnbounded
            | TokenKind::KwVacuum
            | TokenKind::KwView
            | TokenKind::KwVirtual
            | TokenKind::KwWindow
            | TokenKind::KwWith
            | TokenKind::KwWithout
            // FrankenSQLite tokenizes these extension/literal names as
            // keywords, but SQLite 3.46.1 tokenizes each lexeme as ID.
            | TokenKind::KwCommitseq
            | TokenKind::KwConcurrent
            | TokenKind::KwFalse
            | TokenKind::KwStored
            | TokenKind::KwStrict
            | TokenKind::KwTrue
    )
}

/// Compute observable expression height for the SQLite 3.46.1 compatibility
/// target without recursive Rust calls.
///
/// The non-structural cases below are intentional parity requirements:
///
/// * COLLATE and row vectors attach their children after allocating a
///   height-one node and do not recompute it.
/// * BETWEEN recomputes from its left operand before attaching its bounds.
/// * aggregate ORDER BY, FILTER, and OVER metadata is attached after the
///   function node's height is set and does not recompute it.
/// * NOT LIKE/BETWEEN/IN/EXISTS has an additional hidden `TK_NOT` node.
/// * RAISE messages use the 3.46.1 `nm` grammar (`idj` identifier or string
///   token), not an expression child, and therefore do not increase height.
///   SQLite 3.47 and later expanded this grammar; that newer behavior is
///   intentionally not modeled.
///
/// These rules follow `src/expr.c` and `src/parse.y` for the project's SQLite
/// 3.46.1 target; changing this to ordinary Rust AST height breaks the public
/// `SQLITE_LIMIT_EXPR_DEPTH` boundary.
#[allow(clippy::too_many_lines)]
fn sqlite_height(initial: HeightWork<'_>) -> u32 {
    let mut max_height = 0;
    let mut pending = vec![initial];

    while let Some(work) = pending.pop() {
        match work {
            HeightWork::Expr(expr, depth) => {
                max_height = max_height.max(depth);
                let child_depth = depth.saturating_add(1);
                match expr {
                    Expr::BinaryOp { left, right, .. } => {
                        pending.push(HeightWork::Expr(left, child_depth));
                        pending.push(HeightWork::Expr(right, child_depth));
                    }
                    Expr::UnaryOp { expr, .. }
                    | Expr::Cast { expr, .. }
                    | Expr::IsNull { expr, .. } => {
                        pending.push(HeightWork::Expr(expr, child_depth));
                    }
                    Expr::Collate { .. } | Expr::RowValue(..) => {}
                    Expr::Between { expr, not, .. } => {
                        let node_depth = depth.saturating_add(u32::from(*not));
                        max_height = max_height.max(node_depth);
                        pending.push(HeightWork::Expr(expr, node_depth.saturating_add(1)));
                    }
                    Expr::In { expr, set, not, .. } => {
                        let node_depth = depth.saturating_add(u32::from(*not));
                        max_height = max_height.max(node_depth);
                        let child_depth = node_depth.saturating_add(1);
                        pending.push(HeightWork::Expr(expr, child_depth));
                        match set {
                            InSet::List(items) => {
                                if matches!(expr.as_ref(), Expr::RowValue(..)) {
                                    // `sqlite3ExprListToValues()` lowers a
                                    // vector RHS to VALUES rows. TK_VECTOR
                                    // wrappers disappear, so their elements
                                    // contribute directly to SELECT height.
                                    for item in items {
                                        if let Expr::RowValue(elements, _) = item {
                                            pending.extend(elements.iter().map(|element| {
                                                HeightWork::Expr(element, child_depth)
                                            }));
                                        } else {
                                            pending.push(HeightWork::Expr(item, child_depth));
                                        }
                                    }
                                } else {
                                    pending.extend(
                                        items
                                            .iter()
                                            .map(|item| HeightWork::Expr(item, child_depth)),
                                    );
                                }
                            }
                            InSet::Subquery(select) => {
                                pending.push(HeightWork::Select(select, node_depth));
                            }
                            InSet::Table(_) => {}
                        }
                    }
                    Expr::Like {
                        expr,
                        pattern,
                        escape,
                        not,
                        ..
                    } => {
                        let node_depth = depth.saturating_add(u32::from(*not));
                        max_height = max_height.max(node_depth);
                        let child_depth = node_depth.saturating_add(1);
                        pending.push(HeightWork::Expr(expr, child_depth));
                        pending.push(HeightWork::Expr(pattern, child_depth));
                        if let Some(escape) = escape {
                            pending.push(HeightWork::Expr(escape, child_depth));
                        }
                    }
                    Expr::Case {
                        operand,
                        whens,
                        else_expr,
                        ..
                    } => {
                        if let Some(operand) = operand {
                            pending.push(HeightWork::Expr(operand, child_depth));
                        }
                        for (condition, result) in whens {
                            pending.push(HeightWork::Expr(condition, child_depth));
                            pending.push(HeightWork::Expr(result, child_depth));
                        }
                        if let Some(else_expr) = else_expr {
                            pending.push(HeightWork::Expr(else_expr, child_depth));
                        }
                    }
                    Expr::Exists { subquery, not, .. } => {
                        let node_depth = depth.saturating_add(u32::from(*not));
                        max_height = max_height.max(node_depth);
                        pending.push(HeightWork::Select(subquery, node_depth));
                    }
                    Expr::Subquery(select, _) => {
                        pending.push(HeightWork::Select(select, depth));
                    }
                    Expr::FunctionCall { args, .. } => {
                        if let FunctionArgs::List(args) = args {
                            pending
                                .extend(args.iter().map(|arg| HeightWork::Expr(arg, child_depth)));
                        }
                    }
                    Expr::JsonAccess { expr, path, .. } => {
                        pending.push(HeightWork::Expr(expr, child_depth));
                        pending.push(HeightWork::Expr(path, child_depth));
                    }
                    Expr::Column(column, _) => {
                        let qualifier_nodes =
                            u32::from(column.table.is_some()) + u32::from(column.schema.is_some());
                        max_height = max_height.max(depth.saturating_add(qualifier_nodes));
                    }
                    Expr::Literal(..) | Expr::Raise { .. } | Expr::Placeholder(..) => {}
                }
            }
            HeightWork::Select(select, parent_depth) => {
                let expr_depth = parent_depth.saturating_add(1);
                push_select_core_height_work(
                    &select.body.select,
                    expr_depth,
                    &mut pending,
                    &mut max_height,
                );
                for (_, core) in &select.body.compounds {
                    push_select_core_height_work(core, expr_depth, &mut pending, &mut max_height);
                }
                pending.extend(
                    select
                        .order_by
                        .iter()
                        .map(|ordering| HeightWork::Expr(&ordering.expr, expr_depth)),
                );
                if let Some(limit) = &select.limit {
                    // SQLite stores LIMIT/OFFSET beneath a synthetic TK_LIMIT
                    // node, so both expressions are one level below pLimit.
                    max_height = max_height.max(expr_depth);
                    let limit_child_depth = expr_depth.saturating_add(1);
                    pending.push(HeightWork::Expr(&limit.limit, limit_child_depth));
                    if let Some(offset) = &limit.offset {
                        pending.push(HeightWork::Expr(offset, limit_child_depth));
                    }
                }
            }
        }
    }

    max_height
}

fn sqlite_expr_height(root: &Expr) -> u32 {
    sqlite_height(HeightWork::Expr(root, 1))
}

fn sqlite_select_height(select: &SelectStatement) -> u32 {
    sqlite_height(HeightWork::Select(select, 0))
}

impl Parser {
    /// Parse a single SQL expression.
    pub fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_expr_tracked().map(ParsedExpr::into_node)
    }

    fn parse_expr_tracked(&mut self) -> Result<ParsedExpr, ParseError> {
        self.parse_expr_bp(0)
    }

    // ── Pratt core ──────────────────────────────────────────────────────

    fn parse_expr_bp(&mut self, min_bp: u8) -> Result<ParsedExpr, ParseError> {
        self.with_recursion_guard(|p| p.parse_expr_bp_inner(min_bp))
    }

    fn parse_expr_bp_inner(&mut self, min_bp: u8) -> Result<ParsedExpr, ParseError> {
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

    fn checked_parent(
        &self,
        max_child_height: u32,
        build: impl FnOnce() -> Expr,
    ) -> Result<ParsedExpr, ParseError> {
        self.checked_height(max_child_height.saturating_add(1), build)
    }

    fn checked_parent_with_hidden_not(
        &self,
        max_child_height: u32,
        not: bool,
        build: impl FnOnce() -> Expr,
    ) -> Result<ParsedExpr, ParseError> {
        let height = max_child_height
            .saturating_add(1)
            .saturating_add(u32::from(not));
        self.checked_height(height, build)
    }

    fn checked_height(
        &self,
        height: u32,
        build: impl FnOnce() -> Expr,
    ) -> Result<ParsedExpr, ParseError> {
        if height > MAX_EXPR_DEPTH {
            return Err(self.err_here(format!(
                "expression AST is too deep (maximum height {MAX_EXPR_DEPTH})"
            )));
        }
        Ok(ParsedExpr {
            node: build(),
            height,
        })
    }

    fn make_null_test(
        &self,
        lhs: ParsedExpr,
        is_not_null: bool,
        span: Span,
    ) -> Result<ParsedExpr, ParseError> {
        if let Some(value) = sqlite_null_test_fold_value(&lhs.node, is_not_null) {
            return Ok(ParsedExpr::leaf(Expr::Literal(
                Literal::Integer(value),
                span,
            )));
        }
        self.checked_parent(lhs.height, || Expr::IsNull {
            expr: Box::new(lhs.node),
            not: is_not_null,
            span,
        })
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
    fn parse_prefix(&mut self) -> Result<ParsedExpr, ParseError> {
        let Token {
            kind,
            span: token_span,
            line,
            col,
        } = self.advance_token();
        match kind {
            // ── Literals ────────────────────────────────────────────────
            TokenKind::Integer(i) => Ok(ParsedExpr::leaf(Expr::Literal(
                Literal::Integer(i),
                token_span,
            ))),
            // An integer literal too large for i64 becomes a REAL, and a
            // magnitude beyond f64 range becomes ±Infinity — matching C
            // SQLite's text-to-real conversion (no f64::MAX clamp).
            TokenKind::OversizedInt(s) => match s.parse::<f64>() {
                Ok(v) => Ok(ParsedExpr::leaf(Expr::Literal(
                    Literal::Float(v),
                    token_span,
                ))),
                Err(_) => Err(ParseError {
                    message: "integer out of range".to_owned(),
                    span: token_span,
                    line,
                    col,
                }),
            },
            TokenKind::Float(f) => Ok(ParsedExpr::leaf(Expr::Literal(
                Literal::Float(f),
                token_span,
            ))),
            TokenKind::String(s) => Ok(ParsedExpr::leaf(Expr::Literal(
                Literal::String(s),
                token_span,
            ))),
            TokenKind::Blob(b) => Ok(ParsedExpr::leaf(Expr::Literal(
                Literal::Blob(b),
                token_span,
            ))),
            TokenKind::KwNull => Ok(ParsedExpr::leaf(Expr::Literal(Literal::Null, token_span))),
            TokenKind::KwTrue => Ok(ParsedExpr::leaf(Expr::Literal(Literal::True, token_span))),
            TokenKind::KwFalse => Ok(ParsedExpr::leaf(Expr::Literal(Literal::False, token_span))),
            TokenKind::KwCurrentTime => Ok(ParsedExpr::leaf(Expr::Literal(
                Literal::CurrentTime,
                token_span,
            ))),
            TokenKind::KwCurrentDate => Ok(ParsedExpr::leaf(Expr::Literal(
                Literal::CurrentDate,
                token_span,
            ))),
            TokenKind::KwCurrentTimestamp => Ok(ParsedExpr::leaf(Expr::Literal(
                Literal::CurrentTimestamp,
                token_span,
            ))),

            // ── Bind parameters ─────────────────────────────────────────
            TokenKind::Question => Ok(ParsedExpr::leaf(Expr::Placeholder(
                PlaceholderType::Anonymous,
                token_span,
            ))),
            TokenKind::QuestionNum(n) => Ok(ParsedExpr::leaf(Expr::Placeholder(
                PlaceholderType::Numbered(n),
                token_span,
            ))),
            TokenKind::ColonParam(s) => Ok(ParsedExpr::leaf(Expr::Placeholder(
                PlaceholderType::ColonNamed(s),
                token_span,
            ))),
            TokenKind::AtParam(s) => Ok(ParsedExpr::leaf(Expr::Placeholder(
                PlaceholderType::AtNamed(s),
                token_span,
            ))),
            TokenKind::DollarParam(s) => Ok(ParsedExpr::leaf(Expr::Placeholder(
                PlaceholderType::DollarNamed(s),
                token_span,
            ))),

            // ── Unary prefix: - + ~ ─────────────────────────────────────
            TokenKind::Minus => {
                // Peek ahead to handle exactly `-9223372036854775808`
                if let TokenKind::OversizedInt(s) = self.peek_kind() {
                    if s == "9223372036854775808" {
                        let num_span = self.advance_token().span;
                        let span = token_span.merge(num_span);
                        return Ok(ParsedExpr::leaf(Expr::Literal(
                            Literal::Integer(i64::MIN),
                            span,
                        )));
                    }
                }
                let inner = self.parse_expr_bp(bp::UNARY)?;
                let span = token_span.merge(inner.span());
                let height = inner.height;
                match inner.node {
                    Expr::UnaryOp {
                        op: UnaryOp::Plus,
                        expr,
                        ..
                    } => self.checked_height(height, || Expr::UnaryOp {
                        op: UnaryOp::Negate,
                        expr,
                        span,
                    }),
                    node => self.checked_parent(height, || Expr::UnaryOp {
                        op: UnaryOp::Negate,
                        expr: Box::new(node),
                        span,
                    }),
                }
            }
            TokenKind::Plus => {
                let inner = self.parse_expr_bp(bp::UNARY)?;
                let span = token_span.merge(inner.span());
                let height = inner.height;
                match inner.node {
                    Expr::UnaryOp {
                        op: UnaryOp::Plus,
                        expr,
                        ..
                    } => self.checked_height(height, || Expr::UnaryOp {
                        op: UnaryOp::Plus,
                        expr,
                        span,
                    }),
                    node => self.checked_parent(height, || Expr::UnaryOp {
                        op: UnaryOp::Plus,
                        expr: Box::new(node),
                        span,
                    }),
                }
            }
            TokenKind::Tilde => {
                let inner = self.parse_expr_bp(bp::UNARY)?;
                let span = token_span.merge(inner.span());
                self.checked_parent(inner.height, || Expr::UnaryOp {
                    op: UnaryOp::BitNot,
                    expr: Box::new(inner.node),
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
                    let span = token_span.merge(end);
                    let select_height = sqlite_select_height(&subquery);
                    return self.checked_parent_with_hidden_not(select_height, true, || {
                        Expr::Exists {
                            subquery: Box::new(subquery),
                            not: true,
                            span,
                        }
                    });
                }
                let inner = self.parse_expr_bp(bp::NOT_PREFIX)?;
                let span = token_span.merge(inner.span());
                self.checked_parent(inner.height, || Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(inner.node),
                    span,
                })
            }

            // ── EXISTS (subquery) ───────────────────────────────────────
            TokenKind::KwExists => {
                self.expect_kind(&TokenKind::LeftParen)?;
                let subquery = self.parse_subquery_minimal()?;
                let end = self.expect_kind(&TokenKind::RightParen)?;
                let span = token_span.merge(end);
                let select_height = sqlite_select_height(&subquery);
                self.checked_parent(select_height, || Expr::Exists {
                    subquery: Box::new(subquery),
                    not: false,
                    span,
                })
            }

            // ── CAST(expr AS type_name) ─────────────────────────────────
            TokenKind::KwCast => {
                self.expect_kind(&TokenKind::LeftParen)?;
                let inner = self.parse_expr_tracked()?;
                self.expect_kind(&TokenKind::KwAs)?;
                let type_name = self.parse_type_name()?;
                let end = self.expect_kind(&TokenKind::RightParen)?;
                let span = token_span.merge(end);
                self.checked_parent(inner.height, || Expr::Cast {
                    expr: Box::new(inner.node),
                    type_name,
                    span,
                })
            }

            // ── CASE [operand] WHEN ... THEN ... [ELSE ...] END ────────
            TokenKind::KwCase => self.parse_case_expr(token_span),

            // ── RAISE(action, message) ──────────────────────────────────
            TokenKind::KwRaise => {
                self.expect_kind(&TokenKind::LeftParen)?;
                let (action, message) = self.parse_raise_args()?;
                let end = self.expect_kind(&TokenKind::RightParen)?;
                let span = token_span.merge(end);
                self.checked_height(1, || Expr::Raise {
                    action,
                    message,
                    span,
                })
            }

            // ── Parenthesized expr / subquery / row-value ───────────────
            TokenKind::LeftParen => {
                if matches!(
                    self.peek_kind(),
                    TokenKind::KwSelect | TokenKind::KwWith | TokenKind::KwValues
                ) {
                    let subquery = self.parse_subquery_minimal()?;
                    let end = self.expect_kind(&TokenKind::RightParen)?;
                    let span = token_span.merge(end);
                    let select_height = sqlite_select_height(&subquery);
                    return self.checked_parent(select_height, || {
                        Expr::Subquery(Box::new(subquery), span)
                    });
                }
                let first = self.parse_expr_tracked()?;
                if self.eat_kind(&TokenKind::Comma) {
                    let mut exprs = vec![first];
                    loop {
                        exprs.push(self.parse_expr_tracked()?);
                        if !self.eat_kind(&TokenKind::Comma) {
                            break;
                        }
                    }
                    let end = self.expect_kind(&TokenKind::RightParen)?;
                    let span = token_span.merge(end);
                    // SQLite allocates TK_VECTOR at height one, then attaches
                    // its expression list without recomputing nHeight. Each
                    // element was already checked while it was parsed.
                    Ok(ParsedExpr::leaf(Expr::RowValue(
                        exprs.into_iter().map(ParsedExpr::into_node).collect(),
                        span,
                    )))
                } else {
                    self.expect_kind(&TokenKind::RightParen)?;
                    Ok(first)
                }
            }

            // ── Identifier: column ref or function call ─────────────────
            TokenKind::Id(name) | TokenKind::QuotedId(name, _) => {
                self.parse_ident_expr(name, token_span)
            }

            // ── Keywords usable as function names ───────────────────────
            TokenKind::KwReplace if matches!(self.peek_kind(), TokenKind::LeftParen) => {
                self.parse_function_call("replace".to_owned(), token_span)
            }
            // C SQLite exposes the pattern-matching operators as scalar
            // functions too: `like(P, X [, E])`, `glob(P, X)`,
            // `regexp(P, X)`, `match(P, X)`. The token doubles as an infix
            // operator, so only treat it as a function name when directly
            // followed by `(`.
            TokenKind::KwLike if matches!(self.peek_kind(), TokenKind::LeftParen) => {
                self.parse_function_call("like".to_owned(), token_span)
            }
            TokenKind::KwGlob if matches!(self.peek_kind(), TokenKind::LeftParen) => {
                self.parse_function_call("glob".to_owned(), token_span)
            }
            TokenKind::KwRegexp if matches!(self.peek_kind(), TokenKind::LeftParen) => {
                self.parse_function_call("regexp".to_owned(), token_span)
            }
            TokenKind::KwMatch if matches!(self.peek_kind(), TokenKind::LeftParen) => {
                self.parse_function_call("match".to_owned(), token_span)
            }

            // ── Non-reserved keywords usable as identifiers ─────────────
            // In SQL, non-reserved keywords (like KEY, MATCH, FIRST, etc.)
            // can be used as column names without quoting.
            k if is_nonreserved_kw(&k) => {
                let name = kw_to_str(&k);
                self.parse_ident_expr(name, token_span)
            }

            kind => Err(ParseError {
                message: format!("unexpected token in expression: {kind:?}"),
                span: token_span,
                line,
                col,
            }),
        }
    }

    /// Parse `name`, `table.column`, `schema.table.column`, or `name(args)`.
    fn parse_ident_expr<S>(&mut self, name: S, start: Span) -> Result<ParsedExpr, ParseError>
    where
        S: AsRef<str> + Into<Arc<str>>,
    {
        // Function call: name(...)
        if matches!(self.peek_kind(), TokenKind::LeftParen) {
            return self.parse_function_call(name.as_ref().to_owned(), start);
        }
        let name = name.into();
        if self.eat_kind(&TokenKind::Dot) {
            let second = self.advance_token();
            let second_name = Self::qualified_column_component(&second)?;

            if self.eat_kind(&TokenKind::Dot) {
                if matches!(second.kind, TokenKind::Star) {
                    return Err(ParseError::at(
                        "expected table name before second '.'",
                        Some(&second),
                    ));
                }
                let third = self.advance_token();
                if matches!(&third.kind, TokenKind::Star) {
                    return Err(ParseError::at(
                        "expected column name after second '.', got Star",
                        Some(&third),
                    ));
                }
                let third_name = Self::qualified_column_component(&third)?;
                let span = start.merge(third.span);
                return self.checked_height(3, || {
                    Expr::Column(
                        ColumnRef::schema_qualified(name, second_name, third_name),
                        span,
                    )
                });
            }

            let span = start.merge(second.span);
            return self.checked_height(2, || {
                Expr::Column(ColumnRef::qualified(name, second_name), span)
            });
        }
        Ok(ParsedExpr::leaf(Expr::Column(ColumnRef::bare(name), start)))
    }

    fn qualified_column_component(token: &Token) -> Result<Arc<str>, ParseError> {
        match &token.kind {
            TokenKind::Id(component) | TokenKind::QuotedId(component, _) => {
                Ok(Arc::clone(component))
            }
            TokenKind::Star => Ok(Arc::<str>::from("*")),
            // After a dot, any keyword is a valid identifier.
            kind if kind.keyword_str().is_some() => Ok(Arc::<str>::from(kw_to_str(kind))),
            kind => Err(ParseError::at(
                format!("expected identifier after '.', got {kind:?}"),
                Some(token),
            )),
        }
    }

    // ── Postfix ─────────────────────────────────────────────────────────

    fn postfix_bp(&self) -> Option<u8> {
        match self.peek_kind() {
            TokenKind::KwCollate => Some(bp::COLLATE),
            TokenKind::KwIsnull | TokenKind::KwNotnull => Some(bp::EQUALITY.0),
            TokenKind::KwNot => {
                if let Some(next) = self.tokens.get(self.pos + 1) {
                    if matches!(next.kind, TokenKind::KwNull) {
                        return Some(bp::EQUALITY.0);
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn parse_postfix(&mut self, lhs: ParsedExpr) -> Result<ParsedExpr, ParseError> {
        let tok = self.advance_token();
        match &tok.kind {
            TokenKind::KwCollate => {
                let collation = match self.parse_identifier() {
                    Ok(s) => s,
                    Err(_) => {
                        return Err(self.err_here("expected collation name after COLLATE"));
                    }
                };
                let name_span = self.tokens[self.pos.saturating_sub(1)].span;
                let span = lhs.span().merge(name_span);
                // `sqlite3ExprAddCollateToken()` attaches pLeft after
                // allocating a height-one TK_COLLATE node and intentionally
                // does not recompute nHeight. The child was already checked.
                Ok(ParsedExpr::leaf(Expr::Collate {
                    expr: Box::new(lhs.node),
                    collation,
                    span,
                }))
            }
            TokenKind::KwIsnull => {
                let span = lhs.span().merge(tok.span);
                self.make_null_test(lhs, false, span)
            }
            TokenKind::KwNotnull => {
                let span = lhs.span().merge(tok.span);
                self.make_null_test(lhs, true, span)
            }
            TokenKind::KwNot => {
                let null_tok = self.advance_token(); // we know from postfix_bp that this is KwNull
                let span = lhs.span().merge(null_tok.span);
                self.make_null_test(lhs, true, span)
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
    fn parse_infix(&mut self, lhs: ParsedExpr, r_bp: u8) -> Result<ParsedExpr, ParseError> {
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

            // ── IS [NOT] [DISTINCT FROM | NULL | expr] ──────────────────────────────────
            TokenKind::KwIs => {
                let not = self.eat_kind(&TokenKind::KwNot);
                if self.eat_kind(&TokenKind::KwDistinct) {
                    self.expect_kind(&TokenKind::KwFrom)?;
                    let rhs = self.parse_expr_bp(r_bp)?;
                    let span = lhs.span().merge(rhs.span());
                    // IS DISTINCT FROM is equivalent to IS NOT
                    // IS NOT DISTINCT FROM is equivalent to IS
                    let op = if not { BinaryOp::Is } else { BinaryOp::IsNot };
                    if matches!(&rhs.node, Expr::Literal(Literal::Null, _)) {
                        return self.make_null_test(lhs, !not, span);
                    }
                    let max_child_height = lhs.height.max(rhs.height);
                    return self.checked_parent(max_child_height, || Expr::BinaryOp {
                        left: Box::new(lhs.node),
                        op,
                        right: Box::new(rhs.node),
                        span,
                    });
                }
                let rhs = self.parse_expr_bp(r_bp)?;
                let span = lhs.span().merge(rhs.span());
                // SQLite folds `expr IS [NOT] expr` into a unary null-test
                // only when the right operand, parsed at normal precedence,
                // is the NULL literal. Parsing the RHS first — rather than
                // greedily consuming a NULL token — keeps tighter-binding operators
                // attached to NULL: `x IS NULL < 2` parses as
                // `x IS (NULL < 2)`, matching C SQLite (verified against the
                // sqlite3 CLI: `SELECT 1 IS NULL < 2` yields 0, not 1).
                if matches!(&rhs.node, Expr::Literal(Literal::Null, _)) {
                    return self.make_null_test(lhs, not, span);
                }
                let op = if not { BinaryOp::IsNot } else { BinaryOp::Is };
                let max_child_height = lhs.height.max(rhs.height);
                self.checked_parent(max_child_height, || Expr::BinaryOp {
                    left: Box::new(lhs.node),
                    op,
                    right: Box::new(rhs.node),
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
                let max_child_height = lhs.height.max(rhs.height);
                self.checked_parent(max_child_height, || Expr::JsonAccess {
                    expr: Box::new(lhs.node),
                    path: Box::new(rhs.node),
                    arrow: JsonArrow::Arrow,
                    span,
                })
            }
            TokenKind::DoubleArrow => {
                let rhs = self.parse_expr_bp(r_bp)?;
                let span = lhs.span().merge(rhs.span());
                let max_child_height = lhs.height.max(rhs.height);
                self.checked_parent(max_child_height, || Expr::JsonAccess {
                    expr: Box::new(lhs.node),
                    path: Box::new(rhs.node),
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

    fn make_binop(
        &mut self,
        lhs: ParsedExpr,
        op: BinaryOp,
        r_bp: u8,
    ) -> Result<ParsedExpr, ParseError> {
        let rhs = self.parse_expr_bp(r_bp)?;
        let span = lhs.span().merge(rhs.span());
        if op == BinaryOp::And
            && (sqlite_direct_false(&lhs.node) || sqlite_direct_false(&rhs.node))
            && !sqlite_expr_has_function(&lhs.node)
            && !sqlite_expr_has_function(&rhs.node)
        {
            return Ok(ParsedExpr::leaf(Expr::Literal(Literal::Integer(0), span)));
        }
        let max_child_height = lhs.height.max(rhs.height);
        self.checked_parent(max_child_height, || Expr::BinaryOp {
            left: Box::new(lhs.node),
            op,
            right: Box::new(rhs.node),
            span,
        })
    }

    // ── Special expression forms ────────────────────────────────────────

    fn parse_like(
        &mut self,
        lhs: ParsedExpr,
        op: LikeOp,
        not: bool,
    ) -> Result<ParsedExpr, ParseError> {
        let pattern = self.parse_expr_bp(bp::EQUALITY.1)?;
        let escape = if self.eat_kind(&TokenKind::KwEscape) {
            // SQLite's grammar accepts ESCAPE for all pattern-matching operators
            // (LIKE, GLOB, MATCH, REGEXP), not just LIKE.
            Some(self.parse_expr_bp(bp::EQUALITY.1)?)
        } else {
            None
        };
        let end = escape.as_ref().map_or_else(|| pattern.span(), |e| e.span());
        let span = lhs.span().merge(end);
        let max_child_height = lhs
            .height
            .max(pattern.height)
            .max(escape.as_ref().map_or(0, |expr| expr.height));
        // NOT LIKE/GLOB/MATCH/REGEXP is represented by an additional
        // canonical TK_NOT node that is not explicit in our AST.
        self.checked_parent_with_hidden_not(max_child_height, not, || Expr::Like {
            expr: Box::new(lhs.node),
            pattern: Box::new(pattern.node),
            escape: escape.map(|expr| Box::new(expr.node)),
            op,
            not,
            span,
        })
    }

    fn parse_between(&mut self, lhs: ParsedExpr, not: bool) -> Result<ParsedExpr, ParseError> {
        // Parse low bound above AND level so AND keyword is not consumed.
        let low = self.parse_expr_bp(bp::NOT_PREFIX)?;
        if !self.eat_kind(&TokenKind::KwAnd) {
            return Err(self.err_here("expected AND in BETWEEN expression"));
        }
        let high = self.parse_expr_bp(bp::EQUALITY.1)?;
        let span = lhs.span().merge(high.span());
        // SQLite computes TK_BETWEEN height from pLeft before attaching the
        // low/high ExprList, so the bounds do not increase this node's height.
        // They were independently checked by their own parse reductions.
        self.checked_parent_with_hidden_not(lhs.height, not, || Expr::Between {
            expr: Box::new(lhs.node),
            low: Box::new(low.node),
            high: Box::new(high.node),
            not,
            span,
        })
    }

    fn parse_in(&mut self, lhs: ParsedExpr, not: bool) -> Result<ParsedExpr, ParseError> {
        let start = lhs.span();

        // SQLite supports both "x IN ( ... )" and "x IN table_name".
        if !self.at_kind(&TokenKind::LeftParen) {
            let table = self.parse_qualified_name()?;
            let end = self.tokens[self.pos.saturating_sub(1)].span;
            let span = start.merge(end);
            return self.checked_parent_with_hidden_not(lhs.height, not, || Expr::In {
                expr: Box::new(lhs.node),
                set: InSet::Table(table),
                not,
                span,
            });
        }

        self.expect_kind(&TokenKind::LeftParen)?;

        if matches!(
            self.peek_kind(),
            TokenKind::KwSelect | TokenKind::KwWith | TokenKind::KwValues
        ) {
            let subquery = self.parse_subquery_minimal()?;
            let end = self.expect_kind(&TokenKind::RightParen)?;
            let span = start.merge(end);
            let select_height = sqlite_select_height(&subquery);
            let max_child_height = lhs.height.max(select_height);
            return self.checked_parent_with_hidden_not(max_child_height, not, || Expr::In {
                expr: Box::new(lhs.node),
                set: InSet::Subquery(Box::new(subquery)),
                not,
                span,
            });
        }

        let mut exprs = Vec::new();
        if !self.at_kind(&TokenKind::RightParen) {
            exprs.push(self.parse_expr_tracked()?);
            while self.eat_kind(&TokenKind::Comma) {
                exprs.push(self.parse_expr_tracked()?);
            }
        }
        let end = self.expect_kind(&TokenKind::RightParen)?;
        let span = start.merge(end);

        if exprs.is_empty() {
            return Ok(ParsedExpr::leaf(Expr::Literal(
                Literal::Integer(i64::from(not)),
                span,
            )));
        }

        if exprs.len() == 1
            && !matches!(&lhs.node, Expr::RowValue(..))
            && sqlite_expr_is_constant_for_in(&exprs[0].node)
        {
            let Some(rhs) = exprs.pop() else {
                unreachable!("singleton expression list must contain one item");
            };
            let rhs_span = rhs.span();
            let rhs_plus = self.checked_parent(rhs.height, || Expr::UnaryOp {
                op: UnaryOp::Plus,
                expr: Box::new(rhs.node),
                span: rhs_span,
            })?;
            let eq_height = lhs.height.max(rhs_plus.height);
            let eq = self.checked_parent(eq_height, || Expr::BinaryOp {
                left: Box::new(lhs.node),
                op: BinaryOp::Eq,
                right: Box::new(rhs_plus.node),
                span,
            })?;
            if not {
                return self.checked_parent(eq.height, || Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(eq.node),
                    span,
                });
            }
            return Ok(eq);
        }

        let rhs_height = if matches!(&lhs.node, Expr::RowValue(..)) {
            let mut height = 0;
            for expr in &exprs {
                if let Expr::RowValue(elements, _) = &expr.node {
                    for element in elements {
                        height = height.max(sqlite_expr_height(element));
                    }
                } else {
                    height = height.max(expr.height);
                }
            }
            height
        } else {
            exprs.iter().map(|expr| expr.height).max().unwrap_or(0)
        };
        let max_child_height = lhs.height.max(rhs_height);
        self.checked_parent_with_hidden_not(max_child_height, not, || Expr::In {
            expr: Box::new(lhs.node),
            set: InSet::List(exprs.into_iter().map(ParsedExpr::into_node).collect()),
            not,
            span,
        })
    }

    fn parse_case_expr(&mut self, start: Span) -> Result<ParsedExpr, ParseError> {
        let operand = if matches!(self.peek_kind(), TokenKind::KwWhen) {
            None
        } else {
            Some(self.parse_expr_tracked()?)
        };

        let mut whens = Vec::new();
        while self.eat_kind(&TokenKind::KwWhen) {
            let condition = self.parse_expr_tracked()?;
            if !self.eat_kind(&TokenKind::KwThen) {
                return Err(self.err_here("expected THEN in CASE expression"));
            }
            let result = self.parse_expr_tracked()?;
            whens.push((condition, result));
        }
        if whens.is_empty() {
            return Err(self.err_here("CASE requires at least one WHEN clause"));
        }

        let else_expr = if self.eat_kind(&TokenKind::KwElse) {
            Some(self.parse_expr_tracked()?)
        } else {
            None
        };

        if !self.eat_kind(&TokenKind::KwEnd) {
            return Err(self.err_here("expected END for CASE expression"));
        }
        let end = self.tokens[self.pos.saturating_sub(1)].span;
        let span = start.merge(end);
        let max_child_height = operand
            .as_ref()
            .map_or(0, |expr| expr.height)
            .max(
                whens
                    .iter()
                    .flat_map(|(condition, result)| [condition.height, result.height])
                    .max()
                    .unwrap_or(0),
            )
            .max(else_expr.as_ref().map_or(0, |expr| expr.height));
        self.checked_parent(max_child_height, || Expr::Case {
            operand: operand.map(|expr| Box::new(expr.node)),
            whens: whens
                .into_iter()
                .map(|(condition, result)| (condition.node, result.node))
                .collect(),
            else_expr: else_expr.map(|expr| Box::new(expr.node)),
            span,
        })
    }

    fn parse_function_call(&mut self, name: String, start: Span) -> Result<ParsedExpr, ParseError> {
        self.expect_kind(&TokenKind::LeftParen)?;

        let (args, distinct, args_height) = if matches!(self.peek_kind(), TokenKind::Star) {
            if !name.eq_ignore_ascii_case("count") {
                return Err(self.err_here("'*' can only be used with count() function"));
            }
            self.advance_token();
            (FunctionArgs::Star, false, 0)
        } else {
            let distinct = self.eat_kind(&TokenKind::KwDistinct);
            let (args, args_height) = if matches!(self.peek_kind(), TokenKind::RightParen) {
                if distinct {
                    return Err(self.err_here("DISTINCT requires at least one argument"));
                }
                (FunctionArgs::List(Vec::new()), 0)
            } else {
                let mut list = vec![self.parse_expr_tracked()?];
                while self.eat_kind(&TokenKind::Comma) {
                    list.push(self.parse_expr_tracked()?);
                }
                let height = list.iter().map(|expr| expr.height).max().unwrap_or(0);
                (
                    FunctionArgs::List(list.into_iter().map(ParsedExpr::into_node).collect()),
                    height,
                )
            };
            (args, distinct, args_height)
        };

        // In-aggregate ORDER BY (SQLite 3.44+): group_concat(x, ',' ORDER BY y DESC)
        let order_by = if self.eat_kind(&TokenKind::KwOrder) {
            self.expect_kind(&TokenKind::KwBy)?;
            self.parse_comma_sep(Self::parse_ordering_term)?
        } else {
            vec![]
        };

        let mut end = self.expect_kind(&TokenKind::RightParen)?;
        // Peek ahead: only consume FILTER if followed by '(' to avoid
        // swallowing FILTER when used as a column alias (it's non-reserved).
        let filter = if matches!(self.peek_kind(), TokenKind::KwFilter)
            && self
                .tokens
                .get(self.pos + 1)
                .is_some_and(|t| t.kind == TokenKind::LeftParen)
        {
            self.advance_token(); // consume FILTER
            self.expect_kind(&TokenKind::LeftParen)?;
            self.expect_kind(&TokenKind::KwWhere)?;
            let predicate = self.parse_expr_tracked()?;
            let filter_end = self.expect_kind(&TokenKind::RightParen)?;
            end = end.merge(filter_end);
            Some(predicate)
        } else {
            None
        };
        // Peek: only consume OVER if followed by '(' or an identifier
        // (window name), to avoid swallowing OVER as a column alias.
        let over = if matches!(self.peek_kind(), TokenKind::KwOver)
            && self.tokens.get(self.pos + 1).is_some_and(|t| {
                matches!(
                    t.kind,
                    TokenKind::LeftParen | TokenKind::Id(_) | TokenKind::QuotedId(_, _)
                )
            }) {
            self.advance_token(); // consume OVER
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
        // Canonical SQLite sets TK_FUNCTION height from its argument list.
        // Aggregate ORDER BY, FILTER, and OVER are attached later without
        // recomputing nHeight. Their expressions were checked independently
        // while being parsed, but they do not increase the function node.
        self.checked_parent(args_height, || Expr::FunctionCall {
            name,
            args,
            distinct,
            order_by,
            filter: filter.map(|predicate| Box::new(predicate.node)),
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
            TokenKind::Id(s) | TokenKind::QuotedId(s, _) => s.to_string(),
            kind if is_sqlite_346_raise_nm_keyword(kind) => kw_to_str(kind),
            _ => {
                return Err(ParseError::at(
                    "expected identifier or string message in RAISE",
                    Some(&msg_tok),
                ));
            }
        };
        Ok((action, Some(message)))
    }

    fn parse_type_name(&mut self) -> Result<TypeName, ParseError> {
        let mut parts = Vec::new();
        loop {
            match self.peek_kind() {
                TokenKind::Id(_) | TokenKind::QuotedId(_, _) => {
                    let tok = self.advance_token();
                    if let TokenKind::Id(s) | TokenKind::QuotedId(s, _) = &tok.kind {
                        parts.push(s.to_string());
                    } else {
                        unreachable!();
                    }
                }
                k if is_nonreserved_kw(k) => {
                    let tok = self.advance_token();
                    parts.push(kw_to_str(&tok.kind));
                }
                _ => break,
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
            TokenKind::Float(f) => Ok(f.to_string()),
            TokenKind::Minus => {
                let next = self.advance_token();
                match &next.kind {
                    TokenKind::Integer(i) => Ok(format!("-{i}")),
                    TokenKind::OversizedInt(s) => Ok(format!("-{s}")),
                    TokenKind::Float(f) => Ok(format!("-{f}")),
                    _ => Err(ParseError::at(
                        "expected number in type argument",
                        Some(&next),
                    )),
                }
            }
            TokenKind::Plus => {
                let next = self.advance_token();
                match &next.kind {
                    TokenKind::Integer(i) => Ok(format!("+{i}")),
                    TokenKind::OversizedInt(s) => Ok(format!("+{s}")),
                    TokenKind::Float(f) => Ok(format!("+{f}")),
                    _ => Err(ParseError::at(
                        "expected number in type argument",
                        Some(&next),
                    )),
                }
            }
            TokenKind::OversizedInt(s) => Ok(s.clone()),
            TokenKind::Id(s) | TokenKind::QuotedId(s, _) => Ok(s.to_string()),
            _ => Err(ParseError::at("expected type argument", Some(&tok))),
        }
    }

    /// Subquery parser for EXISTS/IN expression support.
    fn parse_subquery_minimal(&mut self) -> Result<SelectStatement, ParseError> {
        let with = if self.at_kind(&TokenKind::KwWith) {
            Some(self.parse_with_clause()?)
        } else {
            None
        };
        self.parse_select_stmt(with)
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
    use fsqlite_ast::{SelectCore, TableOrSubquery};

    fn parse(sql: &str) -> Expr {
        match parse_expr(sql) {
            Ok(expr) => expr,
            Err(err) => unreachable!("parse error for `{sql}`: {err}"),
        }
    }

    fn left_deep_or_sql(term_count: u32) -> String {
        (0..term_count)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    fn balanced_or_sql(first_value: u32, leaf_count: u32) -> String {
        if leaf_count == 1 {
            return first_value.to_string();
        }
        let left_count = leaf_count / 2;
        let right_count = leaf_count - left_count;
        format!(
            "({} OR {})",
            balanced_or_sql(first_value, left_count),
            balanced_or_sql(first_value + left_count, right_count)
        )
    }

    fn repeated_or_sql(term: &str, term_count: u32) -> String {
        (0..term_count)
            .map(|_| term)
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    fn assert_sqlite_height(sql: &str, expected: u32) {
        let expr = parse_expr(sql).unwrap_or_else(|error| {
            panic!("height-{expected} expression must parse: {error}");
        });
        assert_eq!(sqlite_expr_height(&expr), expected);
    }

    fn assert_sqlite_height_rejected(sql: &str) {
        let error = parse_expr(sql).expect_err("overheight expression must be rejected");
        assert!(
            error.message.contains("expression AST is too deep")
                && error.message.contains("maximum height 1000"),
            "unexpected logical-height diagnostic: {error:?}"
        );
    }

    #[test]
    fn test_left_deep_logical_height_exactly_maximum_passes() {
        let sql = left_deep_or_sql(MAX_EXPR_DEPTH);
        let expr = parse_expr(&sql).expect("height 1000 must remain valid");
        assert_eq!(sqlite_expr_height(&expr), MAX_EXPR_DEPTH);
    }

    #[test]
    fn test_left_deep_logical_height_above_maximum_does_not_publish_statement() {
        let sql = left_deep_or_sql(MAX_EXPR_DEPTH + 1);
        let mut parser = Parser::from_sql(&format!("SELECT {sql}"));
        let (statements, errors) = parser.parse_all();
        assert!(
            statements.is_empty(),
            "height 1001 must not publish a partial statement"
        );
        let [error] = errors.as_slice() else {
            panic!("expected one deterministic height error, got {errors:?}");
        };
        assert!(
            error.message.contains("expression AST is too deep")
                && error.message.contains("maximum height 1000"),
            "logical-height diagnostic must be distinct and deterministic: {error:?}"
        );
    }

    #[test]
    fn test_qualified_column_sqlite_height_boundaries() {
        assert_sqlite_height("t.c", 2);
        assert_sqlite_height("main.t.c", 3);

        let two_part_at_limit = repeated_or_sql("t.c", MAX_EXPR_DEPTH - 1);
        assert_sqlite_height(&two_part_at_limit, MAX_EXPR_DEPTH);
        let two_part_over_limit = repeated_or_sql("t.c", MAX_EXPR_DEPTH);
        assert_sqlite_height_rejected(&two_part_over_limit);

        let three_part_at_limit = repeated_or_sql("main.t.c", MAX_EXPR_DEPTH - 2);
        assert_sqlite_height(&three_part_at_limit, MAX_EXPR_DEPTH);
        let three_part_over_limit = repeated_or_sql("main.t.c", MAX_EXPR_DEPTH - 1);
        assert_sqlite_height_rejected(&three_part_over_limit);
    }

    #[test]
    fn test_balanced_expression_over_one_thousand_nodes_passes() {
        const LEAF_COUNT: u32 = 1_024;
        let sql = balanced_or_sql(0, LEAF_COUNT);
        let expr = parse_expr(&sql).expect("node count alone must not be limited");

        // 1,024 leaves plus 1,023 OR nodes exceed 1,000 total nodes, while
        // the balanced tree's logical height is only 11.
        let ast_node_count = LEAF_COUNT * 2 - 1;
        let ast_height = sqlite_expr_height(&expr);
        assert!(ast_node_count > MAX_EXPR_DEPTH);
        assert_eq!(ast_height, 11);
        assert!(ast_height <= MAX_EXPR_DEPTH);
    }

    #[test]
    fn test_scalar_subquery_sqlite_height_boundary() {
        let at_limit = format!("(SELECT {})", left_deep_or_sql(MAX_EXPR_DEPTH - 1));
        assert_sqlite_height(&at_limit, MAX_EXPR_DEPTH);

        let over_limit = format!("(SELECT {})", left_deep_or_sql(MAX_EXPR_DEPTH));
        assert_sqlite_height_rejected(&over_limit);
    }

    #[test]
    fn test_exists_subquery_sqlite_height_boundary() {
        let at_limit = format!("EXISTS (SELECT {})", left_deep_or_sql(MAX_EXPR_DEPTH - 1));
        assert_sqlite_height(&at_limit, MAX_EXPR_DEPTH);

        let over_limit = format!("EXISTS (SELECT {})", left_deep_or_sql(MAX_EXPR_DEPTH));
        assert_sqlite_height_rejected(&over_limit);
    }

    #[test]
    fn test_in_select_sqlite_height_boundary() {
        let at_limit = format!("0 IN (SELECT {})", left_deep_or_sql(MAX_EXPR_DEPTH - 1));
        assert_sqlite_height(&at_limit, MAX_EXPR_DEPTH);

        let over_limit = format!("0 IN (SELECT {})", left_deep_or_sql(MAX_EXPR_DEPTH));
        assert_sqlite_height_rejected(&over_limit);
    }

    #[test]
    fn test_not_exists_hidden_node_sqlite_height_boundary() {
        let at_limit = format!(
            "NOT EXISTS (SELECT {})",
            left_deep_or_sql(MAX_EXPR_DEPTH - 2)
        );
        assert_sqlite_height(&at_limit, MAX_EXPR_DEPTH);

        let over_limit = format!(
            "NOT EXISTS (SELECT {})",
            left_deep_or_sql(MAX_EXPR_DEPTH - 1)
        );
        assert_sqlite_height_rejected(&over_limit);
    }

    #[test]
    fn test_not_like_hidden_node_sqlite_height_boundary() {
        let at_limit = format!("({}) NOT LIKE 0", left_deep_or_sql(MAX_EXPR_DEPTH - 2));
        assert_sqlite_height(&at_limit, MAX_EXPR_DEPTH);

        let over_limit = format!("({}) NOT LIKE 0", left_deep_or_sql(MAX_EXPR_DEPTH - 1));
        assert_sqlite_height_rejected(&over_limit);
    }

    #[test]
    fn test_not_between_hidden_node_sqlite_height_boundary() {
        let at_limit = format!(
            "({}) NOT BETWEEN 0 AND 0",
            left_deep_or_sql(MAX_EXPR_DEPTH - 2)
        );
        assert_sqlite_height(&at_limit, MAX_EXPR_DEPTH);

        let over_limit = format!(
            "({}) NOT BETWEEN 0 AND 0",
            left_deep_or_sql(MAX_EXPR_DEPTH - 1)
        );
        assert_sqlite_height_rejected(&over_limit);
    }

    #[test]
    fn test_not_in_list_hidden_node_sqlite_height_boundary() {
        let at_limit = format!("0 NOT IN (({}), 0)", left_deep_or_sql(MAX_EXPR_DEPTH - 2));
        assert_sqlite_height(&at_limit, MAX_EXPR_DEPTH);

        let over_limit = format!("0 NOT IN (({}), 0)", left_deep_or_sql(MAX_EXPR_DEPTH - 1));
        assert_sqlite_height_rejected(&over_limit);
    }

    #[test]
    fn test_not_in_select_hidden_node_sqlite_height_boundary() {
        let at_limit = format!("0 NOT IN (SELECT {})", left_deep_or_sql(MAX_EXPR_DEPTH - 2));
        assert_sqlite_height(&at_limit, MAX_EXPR_DEPTH);

        let over_limit = format!("0 NOT IN (SELECT {})", left_deep_or_sql(MAX_EXPR_DEPTH - 1));
        assert_sqlite_height_rejected(&over_limit);
    }

    #[test]
    fn test_select_wrapper_scans_sqlite_352_clause_set_and_compounds() {
        let height_998 = left_deep_or_sql(MAX_EXPR_DEPTH - 2);
        let height_999 = left_deep_or_sql(MAX_EXPR_DEPTH - 1);
        let height_1000 = left_deep_or_sql(MAX_EXPR_DEPTH);

        let boundary_pairs = [
            (
                format!("(SELECT {height_999})"),
                format!("(SELECT {height_1000})"),
            ),
            (
                format!("(SELECT 1 WHERE {height_999})"),
                format!("(SELECT 1 WHERE {height_1000})"),
            ),
            (
                format!("(SELECT 1 GROUP BY {height_999})"),
                format!("(SELECT 1 GROUP BY {height_1000})"),
            ),
            (
                format!("(SELECT 1 HAVING {height_999})"),
                format!("(SELECT 1 HAVING {height_1000})"),
            ),
            (
                format!("(SELECT 1 ORDER BY {height_999})"),
                format!("(SELECT 1 ORDER BY {height_1000})"),
            ),
            (
                format!("(SELECT 1 UNION SELECT {height_999})"),
                format!("(SELECT 1 UNION SELECT {height_1000})"),
            ),
            (
                format!("(SELECT 1 LIMIT {height_998})"),
                format!("(SELECT 1 LIMIT {height_999})"),
            ),
            (
                format!("(SELECT 1 LIMIT 1 OFFSET {height_998})"),
                format!("(SELECT 1 LIMIT 1 OFFSET {height_999})"),
            ),
        ];

        for (at_limit, over_limit) in boundary_pairs {
            assert_sqlite_height(&at_limit, MAX_EXPR_DEPTH);
            assert_sqlite_height_rejected(&over_limit);
        }
    }

    #[test]
    fn test_select_wrapper_does_not_descend_excluded_sqlite_352_children() {
        let height_1000 = left_deep_or_sql(MAX_EXPR_DEPTH);
        let expressions = [
            format!("(SELECT 1 FROM (SELECT {height_1000}) AS nested)"),
            format!("(WITH c AS (SELECT {height_1000}) SELECT 1)"),
            format!("(SELECT 1 FROM f({height_1000}))"),
            format!("(SELECT 1 WINDOW w AS (PARTITION BY {height_1000}))"),
            format!("(SELECT 1 FROM t JOIN u ON {height_1000})"),
        ];

        for expression in expressions {
            assert_sqlite_height(&expression, 2);
        }
    }

    #[test]
    fn test_sqlite_352_non_recomputed_height_metadata() {
        let height_1000 = left_deep_or_sql(MAX_EXPR_DEPTH);

        assert_sqlite_height(&format!("({height_1000}) COLLATE binary"), 1);
        assert_sqlite_height(&format!("1 BETWEEN ({height_1000}) AND 2"), 2);
        assert_sqlite_height(&format!("(({height_1000}), 1)"), 1);
        assert_sqlite_height(&format!("group_concat(1 ORDER BY {height_1000})"), 2);
        assert_sqlite_height(&format!("count(*) FILTER (WHERE {height_1000})"), 1);
        assert_sqlite_height(&format!("sum(1) OVER (PARTITION BY {height_1000})"), 2);
    }

    #[test]
    fn test_sqlite_352_empty_in_rewrites_and_discards_lhs_height() {
        let height_1000 = left_deep_or_sql(MAX_EXPR_DEPTH);
        let in_empty = parse(&format!("({height_1000}) IN ()"));
        assert!(matches!(&in_empty, Expr::Literal(Literal::Integer(0), _)));
        assert_eq!(sqlite_expr_height(&in_empty), 1);

        let not_in_empty = parse(&format!("({height_1000}) NOT IN ()"));
        assert!(matches!(
            &not_in_empty,
            Expr::Literal(Literal::Integer(1), _)
        ));
        assert_eq!(sqlite_expr_height(&not_in_empty), 1);

        let function_lhs = parse("f() IN ()");
        assert!(matches!(
            &function_lhs,
            Expr::Literal(Literal::Integer(0), _)
        ));
        assert_eq!(sqlite_expr_height(&function_lhs), 1);
    }

    #[test]
    fn test_sqlite_352_singleton_constant_in_rewrites_to_eq_with_uplus() {
        let expr = parse("x IN (1)");
        assert_eq!(sqlite_expr_height(&expr), 3);
        assert!(matches!(
            expr,
            Expr::BinaryOp {
                op: BinaryOp::Eq,
                right,
                ..
            } if matches!(
                right.as_ref(),
                Expr::UnaryOp {
                    op: UnaryOp::Plus,
                    ..
                }
            )
        ));

        let not_expr = parse("x NOT IN (1)");
        assert_eq!(sqlite_expr_height(&not_expr), 4);
        assert!(matches!(
            not_expr,
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr,
                ..
            } if matches!(
                expr.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOp::Eq,
                    ..
                }
            )
        ));

        assert_sqlite_height("x IN (y)", 2);
    }

    #[test]
    fn test_sqlite_352_vector_in_list_uses_values_select_height() {
        let at_limit = format!(
            "(a, b) IN ((1, ({})))",
            left_deep_or_sql(MAX_EXPR_DEPTH - 1)
        );
        assert_sqlite_height(&at_limit, MAX_EXPR_DEPTH);

        let over_limit = format!("(a, b) IN ((1, ({})))", left_deep_or_sql(MAX_EXPR_DEPTH));
        assert_sqlite_height_rejected(&over_limit);
    }

    #[test]
    fn test_sqlite_352_literal_null_tests_fold_to_integer_leaves() {
        let cases = [
            ("1 IS NULL", 0),
            ("'x' IS NOT NULL", 1),
            ("1 ISNULL", 0),
            ("1 NOTNULL", 1),
            ("1 NOT NULL", 1),
            ("1 IS DISTINCT FROM NULL", 1),
            ("1 IS NOT DISTINCT FROM NULL", 0),
        ];
        for (sql, expected) in cases {
            let expr = parse(sql);
            assert!(
                matches!(&expr, Expr::Literal(Literal::Integer(value), _) if *value == expected),
                "unexpected folded form for {sql}: {expr:?}"
            );
            assert_eq!(sqlite_expr_height(&expr), 1, "{sql}");
        }

        assert_sqlite_height("NULL IS NULL", 2);
    }

    #[test]
    fn test_sqlite_352_false_and_function_free_expression_folds() {
        let height_1000 = left_deep_or_sql(MAX_EXPR_DEPTH);
        let left_false = parse(&format!("0 AND ({height_1000})"));
        let right_false = parse(&format!("({height_1000}) AND 0"));
        assert!(matches!(left_false, Expr::Literal(Literal::Integer(0), _)));
        assert!(matches!(right_false, Expr::Literal(Literal::Integer(0), _)));

        assert_sqlite_height_rejected(&format!("FALSE AND ({height_1000})"));
        let height_999 = left_deep_or_sql(MAX_EXPR_DEPTH - 1);
        assert_sqlite_height_rejected(&format!("0 AND f(({height_999}))"));
    }

    #[test]
    fn test_sqlite_352_repeated_unary_plus_reuses_one_node() {
        let plus = parse("++++1");
        assert_eq!(sqlite_expr_height(&plus), 2);
        assert!(matches!(
            plus,
            Expr::UnaryOp {
                op: UnaryOp::Plus,
                expr,
                ..
            } if matches!(expr.as_ref(), Expr::Literal(Literal::Integer(1), _))
        ));

        let negate = parse("-+++1");
        assert_eq!(sqlite_expr_height(&negate), 2);
        assert!(matches!(
            negate,
            Expr::UnaryOp {
                op: UnaryOp::Negate,
                expr,
                ..
            } if matches!(expr.as_ref(), Expr::Literal(Literal::Integer(1), _))
        ));
    }

    #[test]
    fn test_sqlite_346_raise_nm_message_is_height_one() {
        assert_sqlite_height("RAISE(IGNORE)", 1);
        assert_sqlite_height("RAISE(ABORT, 'message')", 1);
        assert_sqlite_height("RAISE(ABORT, message)", 1);

        for (sql, expected) in [
            ("RAISE(ABORT, message)", "message"),
            ("RAISE(FAIL, \"bad row\")", "bad row"),
            ("RAISE(FAIL, 'bad row')", "bad row"),
            ("RAISE(ABORT, indexed)", "indexed"),
            ("RAISE(ABORT, left)", "left"),
            ("RAISE(ABORT, begin)", "begin"),
            ("RAISE(ABORT, cast)", "cast"),
            ("RAISE(ABORT, filter)", "filter"),
            ("RAISE(ABORT, over)", "over"),
            ("RAISE(ABORT, window)", "window"),
            ("RAISE(ABORT, strict)", "strict"),
            ("RAISE(ABORT, concurrent)", "concurrent"),
            ("RAISE(ROLLBACK, end)", "end"),
        ] {
            let expr = parse(sql);
            assert!(
                matches!(
                    &expr,
                    Expr::Raise {
                        message: Some(message),
                        ..
                    } if message == expected
                ),
                "SQLite 3.46.1 nm message must remain a height-one token: {expr:?}"
            );
        }

        let at_limit = repeated_or_sql("RAISE(ABORT, message)", MAX_EXPR_DEPTH);
        assert_sqlite_height(&at_limit, MAX_EXPR_DEPTH);
        let over_limit = repeated_or_sql("RAISE(ABORT, message)", MAX_EXPR_DEPTH.saturating_add(1));
        assert_sqlite_height_rejected(&over_limit);
    }

    #[test]
    fn test_sqlite_346_raise_nm_rejects_non_fallback_reserved_keywords() {
        for sql in [
            "RAISE(ABORT, join)",
            "RAISE(ABORT, table)",
            "RAISE(ABORT, returning)",
            "RAISE(ABORT, transaction)",
        ] {
            let error = parse_expr(sql).expect_err("reserved keyword must not match 3.46.1 nm");
            assert!(
                error
                    .message
                    .contains("expected identifier or string message in RAISE"),
                "unexpected RAISE nm diagnostic for `{sql}`: {error:?}"
            );
        }
    }

    #[test]
    fn test_logical_height_boundary_parses_and_drops_on_two_mib_stack() {
        std::thread::Builder::new()
            .name("expr-height-2mib".to_owned())
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                let rejected = left_deep_or_sql(MAX_EXPR_DEPTH + 1);
                let mut rejected_parser = Parser::from_sql(&rejected);
                let error = rejected_parser
                    .parse_expr()
                    .expect_err("height 1001 must be rejected");
                assert!(
                    error.message.contains("expression AST is too deep"),
                    "unexpected logical-height diagnostic: {error:?}"
                );
                assert_eq!(
                    rejected_parser.depth, 0,
                    "height rejection must unwind the physical recursion guard"
                );

                let accepted = left_deep_or_sql(MAX_EXPR_DEPTH);
                let expr = parse_expr(&accepted).expect("height 1000 must parse");
                assert_eq!(sqlite_expr_height(&expr), MAX_EXPR_DEPTH);
                drop(expr);

                let scalar_subquery = format!("(SELECT {})", left_deep_or_sql(MAX_EXPR_DEPTH - 1));
                let expr =
                    parse_expr(&scalar_subquery).expect("height-1000 scalar subquery must parse");
                assert_eq!(sqlite_expr_height(&expr), MAX_EXPR_DEPTH);
                drop(expr);
            })
            .expect("2 MiB parser thread must spawn")
            .join()
            .expect("2 MiB parser thread must complete without stack overflow");
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

    #[test]
    fn test_cast_float_argument() {
        // CAST(x AS DECIMAL(10.5, -2.5))
        let expr = parse("CAST(x AS DECIMAL(10.5, -2.5))");
        match &expr {
            Expr::Cast { type_name, .. } => {
                assert_eq!(type_name.name, "DECIMAL");
                assert_eq!(type_name.arg1.as_deref(), Some("10.5"));
                assert_eq!(type_name.arg2.as_deref(), Some("-2.5"));
            }
            other => unreachable!("expected Cast with float args, got {other:?}"),
        }
    }

    #[test]
    fn test_cast_signed_args() {
        // CAST(x AS NUMERIC(+5, -5))
        let expr = parse("CAST(x AS NUMERIC(+5, -5))");
        match &expr {
            Expr::Cast { type_name, .. } => {
                assert_eq!(type_name.name, "NUMERIC");
                assert_eq!(type_name.arg1.as_deref(), Some("+5"));
                assert_eq!(type_name.arg2.as_deref(), Some("-5"));
            }
            other => unreachable!("expected Cast with signed args, got {other:?}"),
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

    #[test]
    fn test_exists_subquery_supports_qualified_table_with_alias() {
        let expr = parse("EXISTS (SELECT 1 FROM main.users AS u WHERE u.id = 1)");
        match expr {
            Expr::Exists { subquery, .. } => match subquery.body.select {
                SelectCore::Select {
                    from: Some(from), ..
                } => match from.source {
                    TableOrSubquery::Table { name, alias, .. } => {
                        assert_eq!(name.schema.as_deref(), Some("main"));
                        assert_eq!(name.name, "users");
                        assert_eq!(alias.as_deref(), Some("u"));
                    }
                    other => unreachable!("expected table source, got {other:?}"),
                },
                other => unreachable!("expected SELECT core with FROM, got {other:?}"),
            },
            other => unreachable!("expected EXISTS subquery, got {other:?}"),
        }
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
    fn test_in_subquery_with_order_by_and_limit() {
        // This is the pattern used in mcp-agent-mail-db prune queries
        let expr =
            parse("id NOT IN (SELECT id FROM search_recipes ORDER BY updated_ts DESC LIMIT 5)");
        match &expr {
            Expr::In {
                not: true,
                set: InSet::Subquery(stmt),
                ..
            } => {
                assert_eq!(stmt.order_by.len(), 1, "ORDER BY should be parsed");
                assert!(stmt.limit.is_some(), "LIMIT should be parsed");
            }
            other => unreachable!("expected NOT IN subquery, got {other:?}"),
        }
    }

    #[test]
    fn test_in_subquery_supports_group_by_and_having() {
        let expr = parse("x IN (SELECT y FROM t GROUP BY y HAVING COUNT(*) > 1)");
        match expr {
            Expr::In {
                set: InSet::Subquery(stmt),
                ..
            } => match stmt.body.select {
                SelectCore::Select {
                    group_by, having, ..
                } => {
                    assert_eq!(group_by.len(), 1, "GROUP BY should be parsed");
                    assert!(having.is_some(), "HAVING should be parsed");
                }
                SelectCore::Values(_) => unreachable!("expected SELECT core"),
            },
            other => unreachable!("expected IN subquery, got {other:?}"),
        }
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

    // ── IS NULL / IS NOT ─────────────────────────────────────────────────────

    #[test]
    fn test_is_null() {
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

    // ── Issue #122: postfix null-test vs `=` precedence and round-trip ──

    /// `a IS NULL = b IS NULL` (no parentheses) groups left-associatively:
    /// `((a IS NULL) = b) IS NULL`. Verified against the C SQLite CLI:
    /// `SELECT 200 IS NULL = 'ok' IS NULL` yields 0 (not 1), because the
    /// null-test and `=` share one left-associative precedence level.
    #[test]
    fn test_isnull_eq_isnull_unparenthesized_left_associative() {
        let expr = parse("a IS NULL = b IS NULL");
        match &expr {
            Expr::IsNull {
                expr: inner,
                not: false,
                ..
            } => match inner.as_ref() {
                Expr::BinaryOp {
                    op: BinaryOp::Eq,
                    left,
                    right,
                    ..
                } => {
                    assert!(
                        matches!(left.as_ref(), Expr::IsNull { not: false, .. }),
                        "expected (a IS NULL) on the left, got {left:?}"
                    );
                    assert!(
                        matches!(right.as_ref(), Expr::Column(..)),
                        "expected bare column b on the right, got {right:?}"
                    );
                }
                other => unreachable!("expected Eq inside IsNull, got {other:?}"),
            },
            other => unreachable!("expected IsNull(Eq(IsNull(a), b)), got {other:?}"),
        }
    }

    /// `(a IS NULL) = (b IS NULL)` must parse as Eq of two null-tests, and
    /// the display round-trip must preserve that grouping (issue #122: the
    /// serializer used to strip these parentheses, silently inverting CHECK
    /// constraints of the form `(a IS NULL) = (b IS NULL)`).
    #[test]
    fn test_isnull_eq_isnull_parenthesized_round_trip() {
        let assert_shape = |expr: &Expr| match expr {
            Expr::BinaryOp {
                op: BinaryOp::Eq,
                left,
                right,
                ..
            } => {
                assert!(
                    matches!(left.as_ref(), Expr::IsNull { not: false, .. }),
                    "expected IsNull on the left, got {left:?}"
                );
                assert!(
                    matches!(right.as_ref(), Expr::IsNull { not: false, .. }),
                    "expected IsNull on the right, got {right:?}"
                );
            }
            other => unreachable!("expected Eq(IsNull, IsNull), got {other:?}"),
        };
        let expr = parse("(a IS NULL) = (b IS NULL)");
        assert_shape(&expr);
        let rendered = expr.to_string();
        assert_eq!(rendered, "(a IS NULL) = (b IS NULL)");
        let reparsed = parse(&rendered);
        assert_shape(&reparsed);
        assert_eq!(reparsed.to_string(), rendered, "round-trip not idempotent");
    }

    /// An operator binding tighter than IS attaches to the NULL literal, so
    /// no null-test fold happens: `1 IS NULL < 2` is `1 IS (NULL < 2)`.
    /// Verified against the C SQLite CLI: `SELECT 1 IS NULL < 2` yields 0
    /// (`1 IS NULL` would give 0, then `0 < 2` would give 1).
    #[test]
    fn test_is_null_followed_by_tighter_operator_binds_to_null() {
        let expr = parse("1 IS NULL < 2");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Is,
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
                "expected Lt(NULL, 2) on the right of IS, got {right:?}"
            ),
            other => unreachable!("expected Is(1, Lt(NULL, 2)), got {other:?}"),
        }
    }

    /// `x IS (NULL)` folds to a null-test just like `x IS NULL`, matching
    /// SQLite's binaryToUnaryIfNull (the fold keys on the resolved RHS
    /// expression, not on the raw token).
    #[test]
    fn test_is_parenthesized_null_folds_to_isnull() {
        assert!(matches!(
            parse("x IS (NULL)"),
            Expr::IsNull { not: false, .. }
        ));
        assert!(matches!(
            parse("x IS NOT (NULL)"),
            Expr::IsNull { not: true, .. }
        ));
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
                    schema: None,
                    table: None,
                    column,
                },
                _,
            ) => assert_eq!(column.as_ref(), "x"),
            other => unreachable!("expected bare column, got {other:?}"),
        }
    }

    #[test]
    fn test_column_qualified() {
        match &parse("t.x") {
            Expr::Column(
                ColumnRef {
                    schema: None,
                    table: Some(t),
                    column,
                },
                _,
            ) => {
                assert_eq!(t.as_ref(), "t");
                assert_eq!(column.as_ref(), "x");
            }
            other => unreachable!("expected qualified column, got {other:?}"),
        }
    }

    #[test]
    fn test_column_schema_qualified_components_remain_distinct() {
        match &parse("main.t.x") {
            Expr::Column(
                ColumnRef {
                    schema: Some(schema),
                    table: Some(table),
                    column,
                },
                _,
            ) => {
                assert_eq!(schema.as_ref(), "main");
                assert_eq!(table.as_ref(), "t");
                assert_eq!(column.as_ref(), "x");
            }
            other => unreachable!("expected schema-qualified column, got {other:?}"),
        }

        match &parse("\"main.t\".x") {
            Expr::Column(
                ColumnRef {
                    schema: None,
                    table: Some(table),
                    column,
                },
                _,
            ) => {
                assert_eq!(table.as_ref(), "main.t");
                assert_eq!(column.as_ref(), "x");
            }
            other => unreachable!("quoted dotted identifier must remain one component: {other:?}"),
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
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::BitAnd,
                        ..
                    }
                ),
                "bitwise operators should be left-associative"
            ),
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
            other => unreachable!("expected Ne(Ge(b,c)), got {other:?}"),
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
    fn test_pratt_level7_add_sub_left_assoc_reverse() {
        // a - b + c → (a - b) + c
        let expr = parse("a - b + c");
        match &expr {
            Expr::BinaryOp {
                op: BinaryOp::Add,
                left,
                ..
            } => assert!(
                matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Subtract,
                        ..
                    }
                ),
                "add/sub should be left-associative"
            ),
            other => unreachable!("expected Add(Sub(a,b), c), got {other:?}"),
        }
    }

    #[test]
    fn test_pratt_level9_concat_tighter_than_mul() {
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
    fn test_pratt_level9_concat_left_assoc_reverse() {
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
                matches!(esc.as_ref(), Expr::Column(_, _)),
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

    // JSON operators share precedence with concat and associate left-to-right.
    #[test]
    fn test_pratt_json_same_precedence_as_concat() {
        // a || b -> c parses as (a || b) -> c.
        let expr = parse("a || b -> c");
        match &expr {
            Expr::JsonAccess {
                expr: left,
                path: right,
                arrow: JsonArrow::Arrow,
                ..
            } => {
                assert!(
                    matches!(
                        left.as_ref(),
                        Expr::BinaryOp {
                            op: BinaryOp::Concat,
                            ..
                        }
                    ),
                    "left side should be concat expression"
                );
                assert!(
                    matches!(right.as_ref(), Expr::Column(_, _)),
                    "path should remain the right-hand expression"
                );
            }
            other => unreachable!("expected JsonAccess(Concat(a,b), c), got {other:?}"),
        }
    }

    #[test]
    fn test_pratt_double_arrow_same_precedence_as_concat() {
        let expr = parse("a || b ->> c");
        assert!(
            matches!(
                expr,
                Expr::JsonAccess {
                    arrow: JsonArrow::DoubleArrow,
                    ..
                }
            ),
            "double-arrow should parse as JsonAccess at the same precedence level as concat"
        );
    }
}
