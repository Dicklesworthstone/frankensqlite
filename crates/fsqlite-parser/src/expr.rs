// bd-16ov: §12.15 Expression Syntax
//
// Explicit-state Pratt expression and SELECT parser with SQLite-correct
// operator precedence. Recursive implementations are retained only as test
// oracles where noted.
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
    BinaryOp, ColumnRef, CompoundOp, Cte, CteMaterialized, Distinctness, Expr, FrameBound,
    FrameExclude, FrameSpec, FrameType, FromClause, FunctionArgs, InSet, JoinClause,
    JoinConstraint, JoinKind, JoinType, JsonArrow, LikeOp, LimitClause, Literal, NullsOrder,
    OrderingTerm, PlaceholderType, QualifiedName, RaiseAction, ResultColumn, SelectBody,
    SelectCore, SelectStatement, SortDirection, Span, TableOrSubquery, TypeName, UnaryOp,
    ValuesClause, WindowDef, WindowReference, WindowSpec, WithClause,
};
#[cfg(test)]
use std::cell::Cell;
use std::sync::Arc;

use crate::parser::{
    HeightTracked, MAX_PARSE_DEPTH, ParseError, Parser, is_nonreserved_kw, kw_to_str,
    starts_bare_window_name, starts_post_dot_identifier, starts_table_star_qualifier,
    starts_window_base_name,
};
use crate::token::{Token, TokenKind};

pub(crate) struct ParsedExpr {
    pub(crate) expr: Expr,
    pub(crate) height: u32,
    is_constant: bool,
    has_function: bool,
    root: CachedRoot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CachedRoot {
    Other,
    UnaryPlus,
    Vector,
    ScalarSubquery,
}

fn vector_in_list_arity_error(lhs: &Expr, items: &[ParsedExpr]) -> Option<String> {
    let expected = match lhs {
        Expr::RowValue(lhs_terms, _) => lhs_terms.len(),
        // A subquery-expression LHS is not an explicit row-value literal. SQLite
        // resolves it semantically, where name/function errors, constant
        // short-circuiting, and context-sensitive row-value diagnostics can
        // take precedence over an IN-list arity error.
        _ => return None,
    };
    for item in items {
        let actual = match &item.expr {
            Expr::RowValue(element_terms, _) => element_terms.len(),
            // A parenthesized subquery element carries an unknown result width
            // until name resolution, so its IN-list arity is deferred to the
            // semantic resolver: `(a, b) IN ((SELECT 1), (SELECT 2))` becomes a
            // FunctionError there, not a parse error. A single bare subquery is
            // the ordinary set-valued RHS `(a, b) IN ((SELECT 1, 2))`, which the
            // resolver also validates for width.
            Expr::Subquery(..) => continue,
            _ => 1,
        };
        if actual == expected {
            continue;
        }
        let term_suffix = if actual == 1 { "" } else { "s" };
        return Some(format!(
            "IN(...) element has {actual} term{term_suffix} - expected {expected}"
        ));
    }
    None
}

#[cfg(test)]
enum DeepExprFrame {
    Unary {
        op: UnaryOp,
        span: Span,
        right_bp: u8,
    },
    Parenthesis {
        span: Span,
    },
}

struct InlineStack<T, const N: usize> {
    inline: [Option<T>; N],
    inline_len: usize,
    spill: Vec<T>,
}

impl<T, const N: usize> InlineStack<T, N> {
    fn new() -> Self {
        Self {
            inline: [const { None }; N],
            inline_len: 0,
            spill: Vec::new(),
        }
    }

    fn push(&mut self, value: T) {
        if self.inline_len < N && self.spill.is_empty() {
            self.inline[self.inline_len] = Some(value);
            self.inline_len += 1;
        } else {
            #[cfg(test)]
            if self.spill.is_empty() {
                PARSE_MACHINE_STACK_SPILLS.set(PARSE_MACHINE_STACK_SPILLS.get().saturating_add(1));
            }
            self.spill.push(value);
        }
    }

    fn pop(&mut self) -> Option<T> {
        if let Some(value) = self.spill.pop() {
            return Some(value);
        }
        if self.inline_len == 0 {
            return None;
        }
        self.inline_len -= 1;
        self.inline[self.inline_len].take()
    }
}

struct FunctionBuild {
    name: String,
    start: Span,
    args: FunctionArgs,
    distinct: bool,
    height: u32,
    order_by: Vec<OrderingTerm>,
    filter: Option<Box<Expr>>,
    over: Option<WindowSpec>,
    end: Span,
}

struct CaseBuild {
    start: Span,
    operand: Option<ParsedExpr>,
    whens: Vec<(ParsedExpr, ParsedExpr)>,
}

struct SelectBuild {
    with: Option<WithClause>,
    first: SelectCore,
    compounds: Vec<(CompoundOp, SelectCore)>,
    height: u32,
    order_by: Vec<OrderingTerm>,
}

struct CoreBuild {
    distinct: Distinctness,
    columns: Vec<ResultColumn>,
    height: u32,
    from: Option<FromClause>,
    where_clause: Option<Box<Expr>>,
    group_by: Vec<Expr>,
    having: Option<Box<Expr>>,
    windows: Vec<WindowDef>,
}

struct FromBuild {
    source: TableOrSubquery,
    joins: Vec<JoinClause>,
}

struct WindowBuild {
    base_window: Option<String>,
    partition_by: Vec<Expr>,
    order_by: Vec<OrderingTerm>,
}

pub(crate) struct ParsedFrameBound {
    pub(crate) value: FrameBound,
    pub(crate) origin: Token,
}

fn frame_bound_rank(bound: &FrameBound) -> u8 {
    match bound {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::Preceding(_) => 1,
        FrameBound::CurrentRow => 2,
        FrameBound::Following(_) => 3,
        FrameBound::UnboundedFollowing => 4,
    }
}

pub(crate) fn validate_frame_start(
    start: &ParsedFrameBound,
    has_explicit_end: bool,
) -> Result<(), ParseError> {
    if matches!(start.value, FrameBound::UnboundedFollowing) {
        return Err(ParseError::at(
            "window frame starting bound must not be UNBOUNDED FOLLOWING",
            Some(&start.origin),
        ));
    }
    if !has_explicit_end && frame_bound_rank(&start.value) > 2 {
        return Err(ParseError::at(
            "single-bound window frame must not start after CURRENT ROW",
            Some(&start.origin),
        ));
    }
    Ok(())
}

pub(crate) fn validate_frame_end(
    start: &ParsedFrameBound,
    end: &ParsedFrameBound,
) -> Result<(), ParseError> {
    if matches!(end.value, FrameBound::UnboundedPreceding) {
        return Err(ParseError::at(
            "window frame ending bound must not be UNBOUNDED PRECEDING",
            Some(&end.origin),
        ));
    }
    if frame_bound_rank(&end.value) < frame_bound_rank(&start.value) {
        return Err(ParseError::at(
            "window frame ending bound must not precede its starting bound",
            Some(&end.origin),
        ));
    }
    Ok(())
}

// Boxing the large variants would add heap traffic to the shallow parse path
// that this inline stack is specifically intended to keep allocation-free.
#[allow(clippy::large_enum_variant)]
enum MachineValue {
    Expr(ParsedExpr),
    Select(HeightTracked<SelectStatement>),
    Core(HeightTracked<SelectCore>),
    From(FromClause),
    Table(TableOrSubquery),
    Ordering(HeightTracked<OrderingTerm>),
    Window(WindowSpec),
    FrameBound(ParsedFrameBound),
    With(WithClause),
}

// The largest continuations own partially built AST nodes. Keep them inline so
// ordinary expressions do not allocate merely to suspend one parser phase.
#[allow(clippy::large_enum_variant)]
enum ParseControl {
    ExprStart {
        min_bp: u8,
    },
    ExprTail {
        min_bp: u8,
    },
    UnaryDone {
        outer_min_bp: u8,
        op: UnaryOp,
        span: Span,
    },
    CastDone {
        outer_min_bp: u8,
        start: Span,
    },
    GroupFirstDone {
        outer_min_bp: u8,
        start: Span,
    },
    RowItemDone {
        outer_min_bp: u8,
        start: Span,
        values: Vec<Expr>,
        is_constant: bool,
        has_function: bool,
    },
    CaseOperandDone {
        outer_min_bp: u8,
        start: Span,
    },
    CaseWhenStart {
        outer_min_bp: u8,
        build: CaseBuild,
    },
    CaseConditionDone {
        outer_min_bp: u8,
        build: CaseBuild,
    },
    CaseResultDone {
        outer_min_bp: u8,
        build: CaseBuild,
        condition: ParsedExpr,
    },
    CaseElseDone {
        outer_min_bp: u8,
        build: CaseBuild,
    },
    FunctionArgDone {
        outer_min_bp: u8,
        build: FunctionBuild,
    },
    FunctionOrderStart {
        outer_min_bp: u8,
        build: FunctionBuild,
    },
    FunctionOrderDone {
        outer_min_bp: u8,
        build: FunctionBuild,
    },
    FunctionClose {
        outer_min_bp: u8,
        build: FunctionBuild,
    },
    FunctionFilterDone {
        outer_min_bp: u8,
        build: FunctionBuild,
        has_filter: bool,
    },
    FunctionOverDone {
        outer_min_bp: u8,
        build: FunctionBuild,
    },
    BinaryDone {
        outer_min_bp: u8,
        lhs: ParsedExpr,
        op: BinaryOp,
    },
    JsonDone {
        outer_min_bp: u8,
        lhs: ParsedExpr,
        arrow: JsonArrow,
    },
    IsDone {
        outer_min_bp: u8,
        lhs: ParsedExpr,
        not: bool,
    },
    LikePatternDone {
        outer_min_bp: u8,
        lhs: ParsedExpr,
        op: LikeOp,
        not: bool,
    },
    LikeEscapeDone {
        outer_min_bp: u8,
        lhs: ParsedExpr,
        pattern: ParsedExpr,
        op: LikeOp,
        not: bool,
    },
    BetweenLowDone {
        outer_min_bp: u8,
        lhs: ParsedExpr,
        not: bool,
    },
    BetweenHighDone {
        outer_min_bp: u8,
        lhs: ParsedExpr,
        low: ParsedExpr,
        not: bool,
    },
    InItemDone {
        outer_min_bp: u8,
        lhs: ParsedExpr,
        not: bool,
        items: Vec<ParsedExpr>,
        start: Span,
    },
    InSelectDone {
        outer_min_bp: u8,
        lhs: ParsedExpr,
        not: bool,
        start: Span,
    },
    ExistsDone {
        outer_min_bp: u8,
        not: bool,
        start: Span,
    },
    ScalarSelectDone {
        outer_min_bp: u8,
        start: Span,
    },
    OrderingStart,
    OrderingDone,
    WindowStart,
    WindowPartitionDone {
        build: WindowBuild,
    },
    WindowOrderStart {
        build: WindowBuild,
    },
    WindowOrderDone {
        build: WindowBuild,
    },
    WindowFrameStart {
        build: WindowBuild,
    },
    WindowFirstBoundDone {
        build: WindowBuild,
        frame_type: FrameType,
        between: bool,
    },
    WindowSecondBoundDone {
        build: WindowBuild,
        frame_type: FrameType,
        start: ParsedFrameBound,
    },
    FrameBoundStart,
    FrameBoundExprDone {
        origin: Token,
    },
    SubqueryStart,
    SubqueryWithDone,
    SelectStart {
        with: Option<WithClause>,
    },
    SelectFirstCoreDone {
        with: Option<WithClause>,
    },
    SelectCompoundDone {
        build: SelectBuild,
        op: CompoundOp,
    },
    SelectOrderStart {
        build: SelectBuild,
    },
    SelectOrderDone {
        build: SelectBuild,
    },
    SelectLimitFirstDone {
        build: SelectBuild,
    },
    SelectLimitSecondDone {
        build: SelectBuild,
        first: ParsedExpr,
        comma_form: bool,
    },
    CoreStart,
    CoreColumnStart {
        build: CoreBuild,
    },
    CoreColumnDone {
        build: CoreBuild,
    },
    CoreAfterColumns {
        build: CoreBuild,
    },
    CoreFromDone {
        build: CoreBuild,
    },
    CoreWhereDone {
        build: CoreBuild,
    },
    CoreGroupDone {
        build: CoreBuild,
    },
    CoreHavingDone {
        build: CoreBuild,
    },
    CoreWindowStart {
        build: CoreBuild,
    },
    CoreWindowDone {
        build: CoreBuild,
        name: String,
    },
    ValuesRowStart {
        rows: Vec<Vec<Expr>>,
        height: u32,
        force_union_all_from: Option<usize>,
    },
    ValuesItemDone {
        rows: Vec<Vec<Expr>>,
        row: Vec<Expr>,
        height: u32,
        force_union_all_from: Option<usize>,
    },
    FromStart,
    FromSourceDone,
    FromTableDone {
        build: FromBuild,
        join_type: JoinType,
    },
    FromJoinConstraintDone {
        build: FromBuild,
        join_type: JoinType,
        table: TableOrSubquery,
    },
    TableStart,
    TableSubqueryDone,
    TableParenJoinDone,
    TableFunctionArgDone {
        name: String,
        args: Vec<Expr>,
    },
    WithStart,
    CteQueryDone {
        recursive: bool,
        ctes: Vec<Cte>,
        name: String,
        columns: Vec<String>,
        materialized: Option<CteMaterialized>,
    },
}

impl ParsedExpr {
    fn leaf(expr: Expr) -> Self {
        let (is_constant, has_function) = match &expr {
            Expr::Literal(
                Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp,
                _,
            ) => (false, true),
            Expr::Literal(..) | Expr::BoundOuterValue { .. } => (true, false),
            _ => (false, false),
        };
        Self {
            expr,
            height: 1,
            is_constant,
            has_function,
            root: CachedRoot::Other,
        }
    }
}

#[cfg(test)]
enum CachedHeightTask<'a> {
    Expr(&'a Expr),
    Select(&'a SelectStatement),
    SelectCore(&'a SelectCore),
    Limit(&'a fsqlite_ast::LimitClause),
    Finish(CachedFinish),
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum CachedFinish {
    Generic(usize),
    Unary(UnaryOp),
    Vector(usize),
    Like { children: usize, not: bool },
    Between { not: bool },
    InList { items: usize, not: bool },
    InSubquery { not: bool },
    InTable { not: bool },
    Exists { not: bool },
    Subquery,
    Function { args: usize },
    Select { expressions: usize },
    Limit { expressions: usize },
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct CachedFacts {
    height: u32,
    is_constant: bool,
    has_function: bool,
    root: CachedRoot,
}

#[cfg(test)]
impl CachedFacts {
    const fn leaf(is_constant: bool, has_function: bool) -> Self {
        Self {
            height: 1,
            is_constant,
            has_function,
            root: CachedRoot::Other,
        }
    }
}

#[cfg(test)]
thread_local! {
    static HEIGHT_WALK_VISITS: Cell<usize> = const { Cell::new(0) };
    static PARSE_MACHINE_STEPS: Cell<usize> = const { Cell::new(0) };
    static PARSE_MACHINE_STACK_SPILLS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn aggregate_cached_facts(values: &mut Vec<CachedFacts>, count: usize) -> CachedFacts {
    let start = values.len().saturating_sub(count);
    let mut facts = CachedFacts {
        height: 0,
        is_constant: true,
        has_function: false,
        root: CachedRoot::Other,
    };
    for child in &values[start..] {
        facts.height = facts.height.max(child.height);
        facts.is_constant &= child.is_constant;
        facts.has_function |= child.has_function;
    }
    if count == 1 {
        facts.root = values[start].root;
    }
    values.truncate(start);
    facts
}

#[cfg(test)]
fn cached_facts_from_tasks(mut pending: Vec<CachedHeightTask<'_>>) -> CachedFacts {
    let mut values = Vec::new();
    while let Some(task) = pending.pop() {
        #[cfg(test)]
        HEIGHT_WALK_VISITS.set(HEIGHT_WALK_VISITS.get() + 1);
        match task {
            CachedHeightTask::Expr(current) => match current {
                Expr::BinaryOp { left, right, .. } => {
                    pending.push(CachedHeightTask::Finish(CachedFinish::Generic(2)));
                    pending.push(CachedHeightTask::Expr(left));
                    pending.push(CachedHeightTask::Expr(right));
                }
                Expr::UnaryOp { op, expr, .. } => {
                    pending.push(CachedHeightTask::Finish(CachedFinish::Unary(*op)));
                    pending.push(CachedHeightTask::Expr(expr));
                }
                Expr::Cast { expr, .. }
                | Expr::Collate { expr, .. }
                | Expr::IsNull { expr, .. } => {
                    pending.push(CachedHeightTask::Finish(CachedFinish::Generic(1)));
                    pending.push(CachedHeightTask::Expr(expr));
                }
                Expr::Between {
                    expr,
                    low,
                    high,
                    not,
                    ..
                } => {
                    pending.push(CachedHeightTask::Finish(CachedFinish::Between {
                        not: *not,
                    }));
                    pending.push(CachedHeightTask::Expr(expr));
                    pending.push(CachedHeightTask::Expr(low));
                    pending.push(CachedHeightTask::Expr(high));
                }
                Expr::In { expr, set, not, .. } => match set {
                    InSet::List(values) => {
                        pending.push(CachedHeightTask::Finish(CachedFinish::InList {
                            items: values.len(),
                            not: *not,
                        }));
                        pending.push(CachedHeightTask::Expr(expr));
                        pending.extend(values.iter().map(CachedHeightTask::Expr));
                    }
                    InSet::Subquery(select) => {
                        pending.push(CachedHeightTask::Finish(CachedFinish::InSubquery {
                            not: *not,
                        }));
                        pending.push(CachedHeightTask::Expr(expr));
                        pending.push(CachedHeightTask::Select(select));
                    }
                    InSet::Table(_) => {
                        pending.push(CachedHeightTask::Finish(CachedFinish::InTable {
                            not: *not,
                        }));
                        pending.push(CachedHeightTask::Expr(expr));
                    }
                },
                Expr::Like {
                    expr,
                    pattern,
                    escape,
                    not,
                    ..
                } => {
                    pending.push(CachedHeightTask::Finish(CachedFinish::Like {
                        children: 2 + usize::from(escape.is_some()),
                        not: *not,
                    }));
                    pending.push(CachedHeightTask::Expr(expr));
                    pending.push(CachedHeightTask::Expr(pattern));
                    if let Some(escape) = escape {
                        pending.push(CachedHeightTask::Expr(escape));
                    }
                }
                Expr::Case {
                    operand,
                    whens,
                    else_expr,
                    ..
                } => {
                    let children = usize::from(operand.is_some())
                        + whens.len().saturating_mul(2)
                        + usize::from(else_expr.is_some());
                    pending.push(CachedHeightTask::Finish(CachedFinish::Generic(children)));
                    if let Some(operand) = operand {
                        pending.push(CachedHeightTask::Expr(operand));
                    }
                    for (condition, result) in whens {
                        pending.push(CachedHeightTask::Expr(condition));
                        pending.push(CachedHeightTask::Expr(result));
                    }
                    if let Some(else_expr) = else_expr {
                        pending.push(CachedHeightTask::Expr(else_expr));
                    }
                }
                Expr::Exists { subquery, not, .. } => {
                    pending.push(CachedHeightTask::Finish(CachedFinish::Exists { not: *not }));
                    pending.push(CachedHeightTask::Select(subquery));
                }
                Expr::Subquery(subquery, _) => {
                    pending.push(CachedHeightTask::Finish(CachedFinish::Subquery));
                    pending.push(CachedHeightTask::Select(subquery));
                }
                Expr::FunctionCall { args, .. } => {
                    let FunctionArgs::List(args) = args else {
                        values.push(CachedFacts {
                            height: 1,
                            is_constant: false,
                            has_function: true,
                            root: CachedRoot::Other,
                        });
                        continue;
                    };
                    pending.push(CachedHeightTask::Finish(CachedFinish::Function {
                        args: args.len(),
                    }));
                    pending.extend(args.iter().map(CachedHeightTask::Expr));
                }
                Expr::JsonAccess { expr, path, .. } => {
                    pending.push(CachedHeightTask::Finish(CachedFinish::Like {
                        children: 2,
                        not: false,
                    }));
                    pending.push(CachedHeightTask::Expr(expr));
                    pending.push(CachedHeightTask::Expr(path));
                }
                Expr::RowValue(items, _) => {
                    pending.push(CachedHeightTask::Finish(CachedFinish::Vector(items.len())));
                    pending.extend(items.iter().map(CachedHeightTask::Expr));
                }
                Expr::Literal(
                    Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp,
                    _,
                ) => values.push(CachedFacts::leaf(false, true)),
                Expr::Literal(..) | Expr::BoundOuterValue { .. } => {
                    values.push(CachedFacts::leaf(true, false));
                }
                Expr::Column(column, _) if column.table.is_some() => {
                    values.push(CachedFacts {
                        height: 2,
                        is_constant: false,
                        has_function: false,
                        root: CachedRoot::Other,
                    });
                }
                Expr::Column(..) | Expr::Raise { .. } | Expr::Placeholder(..) => {
                    values.push(CachedFacts::leaf(false, false));
                }
            },
            CachedHeightTask::Select(select) => {
                let expressions = 1
                    + select.body.compounds.len()
                    + select.order_by.len()
                    + usize::from(select.limit.is_some());
                pending.push(CachedHeightTask::Finish(CachedFinish::Select {
                    expressions,
                }));
                pending.push(CachedHeightTask::SelectCore(&select.body.select));
                pending.extend(
                    select
                        .body
                        .compounds
                        .iter()
                        .map(|(_, core)| CachedHeightTask::SelectCore(core)),
                );
                pending.extend(
                    select
                        .order_by
                        .iter()
                        .map(|term| CachedHeightTask::Expr(&term.expr)),
                );
                if let Some(limit) = &select.limit {
                    pending.push(CachedHeightTask::Limit(limit));
                }
            }
            CachedHeightTask::SelectCore(core) => match core {
                SelectCore::Select {
                    columns,
                    where_clause,
                    group_by,
                    having,
                    ..
                } => {
                    let expressions = columns
                        .iter()
                        .filter(|column| matches!(column, ResultColumn::Expr { .. }))
                        .count()
                        + usize::from(where_clause.is_some())
                        + group_by.len()
                        + usize::from(having.is_some());
                    pending.push(CachedHeightTask::Finish(CachedFinish::Select {
                        expressions,
                    }));
                    pending.extend(columns.iter().filter_map(|column| match column {
                        ResultColumn::Expr { expr, .. } => Some(CachedHeightTask::Expr(expr)),
                        ResultColumn::Star | ResultColumn::TableStar(_) => None,
                    }));
                    if let Some(where_clause) = where_clause {
                        pending.push(CachedHeightTask::Expr(where_clause));
                    }
                    pending.extend(group_by.iter().map(CachedHeightTask::Expr));
                    if let Some(having) = having {
                        pending.push(CachedHeightTask::Expr(having));
                    }
                }
                SelectCore::Values(rows) => {
                    let expressions = rows.iter().map(Vec::len).sum();
                    pending.push(CachedHeightTask::Finish(CachedFinish::Select {
                        expressions,
                    }));
                    pending.extend(rows.iter().flatten().map(CachedHeightTask::Expr));
                }
            },
            CachedHeightTask::Limit(limit) => {
                let expressions = 1 + usize::from(limit.offset.is_some());
                pending.push(CachedHeightTask::Finish(CachedFinish::Limit {
                    expressions,
                }));
                pending.push(CachedHeightTask::Expr(&limit.limit));
                if let Some(offset) = &limit.offset {
                    pending.push(CachedHeightTask::Expr(offset));
                }
            }
            CachedHeightTask::Finish(finish) => match finish {
                CachedFinish::Generic(children) => {
                    let mut facts = aggregate_cached_facts(&mut values, children);
                    facts.height = facts.height.saturating_add(1);
                    facts.root = CachedRoot::Other;
                    values.push(facts);
                }
                CachedFinish::Unary(op) => {
                    let mut child = values.pop().expect("unary cached-height child");
                    if !(matches!(op, UnaryOp::Plus | UnaryOp::Negate)
                        && child.root == CachedRoot::UnaryPlus)
                    {
                        child.height = child.height.saturating_add(1);
                    }
                    child.root = if op == UnaryOp::Plus {
                        CachedRoot::UnaryPlus
                    } else {
                        CachedRoot::Other
                    };
                    values.push(child);
                }
                CachedFinish::Vector(children) => {
                    let mut facts = aggregate_cached_facts(&mut values, children);
                    facts.height = 1;
                    facts.root = CachedRoot::Vector;
                    values.push(facts);
                }
                CachedFinish::Like { children, not } => {
                    let mut facts = aggregate_cached_facts(&mut values, children);
                    facts.height = facts
                        .height
                        .saturating_add(1)
                        .saturating_add(u32::from(not));
                    facts.is_constant = false;
                    facts.has_function = true;
                    facts.root = CachedRoot::Other;
                    values.push(facts);
                }
                CachedFinish::Between { not } => {
                    let mut facts = aggregate_cached_facts(&mut values, 3);
                    facts.height = facts
                        .height
                        .saturating_add(1)
                        .saturating_add(u32::from(not));
                    facts.root = CachedRoot::Other;
                    values.push(facts);
                }
                CachedFinish::InList { items, not } => {
                    let lhs = values.pop().expect("IN cached-height lhs");
                    let item_facts = aggregate_cached_facts(&mut values, items);
                    if items == 0 {
                        values.push(if lhs.has_function {
                            CachedFacts {
                                height: lhs.height.saturating_add(1),
                                is_constant: false,
                                has_function: true,
                                root: CachedRoot::Other,
                            }
                        } else {
                            CachedFacts::leaf(true, false)
                        });
                        continue;
                    }
                    let cached_child_height =
                        if items == 1 && item_facts.is_constant && lhs.root != CachedRoot::Vector {
                            lhs.height.max(item_facts.height.saturating_add(1))
                        } else if items == 1 && item_facts.root == CachedRoot::ScalarSubquery {
                            lhs.height.max(item_facts.height.saturating_sub(1))
                        } else {
                            lhs.height.max(item_facts.height)
                        };
                    values.push(CachedFacts {
                        height: cached_child_height
                            .saturating_add(1)
                            .saturating_add(u32::from(not)),
                        is_constant: lhs.is_constant && item_facts.is_constant,
                        has_function: lhs.has_function || item_facts.has_function,
                        root: CachedRoot::Other,
                    });
                }
                CachedFinish::InSubquery { not } => {
                    let lhs = values.pop().expect("IN-subquery cached-height lhs");
                    let select = values.pop().expect("IN-subquery cached-height SELECT");
                    values.push(CachedFacts {
                        height: lhs
                            .height
                            .max(select.height)
                            .saturating_add(1)
                            .saturating_add(u32::from(not)),
                        is_constant: false,
                        has_function: lhs.has_function,
                        root: CachedRoot::Other,
                    });
                }
                CachedFinish::InTable { not } => {
                    let lhs = values.pop().expect("IN-table cached-height lhs");
                    values.push(CachedFacts {
                        height: lhs.height.saturating_add(1).saturating_add(u32::from(not)),
                        is_constant: false,
                        has_function: lhs.has_function,
                        root: CachedRoot::Other,
                    });
                }
                CachedFinish::Exists { not } => {
                    let select = values.pop().expect("EXISTS cached-height SELECT");
                    values.push(CachedFacts {
                        height: select
                            .height
                            .saturating_add(1)
                            .saturating_add(u32::from(not)),
                        is_constant: false,
                        has_function: false,
                        root: CachedRoot::Other,
                    });
                }
                CachedFinish::Subquery => {
                    let select = values.pop().expect("scalar-subquery cached-height SELECT");
                    values.push(CachedFacts {
                        height: select.height.saturating_add(1),
                        is_constant: false,
                        has_function: false,
                        root: CachedRoot::ScalarSubquery,
                    });
                }
                CachedFinish::Function { args } => {
                    let facts = aggregate_cached_facts(&mut values, args);
                    values.push(CachedFacts {
                        height: facts.height.saturating_add(1),
                        is_constant: false,
                        has_function: true,
                        root: CachedRoot::Other,
                    });
                }
                CachedFinish::Select { expressions } => {
                    let facts = aggregate_cached_facts(&mut values, expressions);
                    values.push(CachedFacts {
                        height: facts.height,
                        is_constant: false,
                        has_function: false,
                        root: CachedRoot::Other,
                    });
                }
                CachedFinish::Limit { expressions } => {
                    let mut facts = aggregate_cached_facts(&mut values, expressions);
                    facts.height = facts.height.saturating_add(1);
                    facts.root = CachedRoot::Other;
                    values.push(facts);
                }
            },
        }
    }
    values.pop().unwrap_or(CachedFacts {
        height: 0,
        is_constant: false,
        has_function: false,
        root: CachedRoot::Other,
    })
}

/// Return a normalized structural height for an expression AST.
///
/// SQLite's signed-minimum special case is already normalized to one literal
/// node, so the test oracle measures the same retained tree as the parser.
#[cfg(test)]
#[must_use]
fn normalized_ast_expr_height(expr: &Expr) -> u32 {
    cached_facts_from_tasks(vec![CachedHeightTask::Expr(expr)]).height
}

/// Return the normalized maximum expression height retained by a SELECT AST.
///
/// This is a private test oracle, not an exact reconstruction of SQLite's
/// syntax-sensitive cached `Expr.nHeight` values.
#[cfg(test)]
#[must_use]
fn normalized_ast_select_height(select: &SelectStatement) -> u32 {
    cached_facts_from_tasks(vec![CachedHeightTask::Select(select)]).height
}

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

struct ParseMachine<'a> {
    parser: &'a mut Parser,
    controls: InlineStack<ParseControl, 8>,
    values: InlineStack<MachineValue, 8>,
}

impl<'a> ParseMachine<'a> {
    fn for_expr(parser: &'a mut Parser) -> Self {
        let mut controls = InlineStack::new();
        controls.push(ParseControl::ExprStart { min_bp: 0 });
        Self {
            parser,
            controls,
            values: InlineStack::new(),
        }
    }

    fn for_select(parser: &'a mut Parser, with: Option<WithClause>) -> Self {
        let mut controls = InlineStack::new();
        controls.push(ParseControl::SelectStart { with });
        Self {
            parser,
            controls,
            values: InlineStack::new(),
        }
    }

    fn for_with(parser: &'a mut Parser) -> Self {
        let mut controls = InlineStack::new();
        controls.push(ParseControl::WithStart);
        Self {
            parser,
            controls,
            values: InlineStack::new(),
        }
    }

    fn for_from(parser: &'a mut Parser) -> Self {
        let mut controls = InlineStack::new();
        controls.push(ParseControl::FromStart);
        Self {
            parser,
            controls,
            values: InlineStack::new(),
        }
    }

    fn run_expr(mut self) -> Result<ParsedExpr, ParseError> {
        self.run()?;
        self.pop_expr()
    }

    fn run_select(mut self) -> Result<HeightTracked<SelectStatement>, ParseError> {
        self.run()?;
        self.pop_select()
    }

    fn run_with(mut self) -> Result<WithClause, ParseError> {
        self.run()?;
        self.pop_with()
    }

    fn run_from(mut self) -> Result<FromClause, ParseError> {
        self.run()?;
        self.pop_from()
    }

    fn run(&mut self) -> Result<(), ParseError> {
        while let Some(control) = self.controls.pop() {
            #[cfg(test)]
            PARSE_MACHINE_STEPS.set(PARSE_MACHINE_STEPS.get().saturating_add(1));
            self.step(control)?;
        }
        Ok(())
    }

    fn pop_expr(&mut self) -> Result<ParsedExpr, ParseError> {
        match self.values.pop() {
            Some(MachineValue::Expr(expr)) => Ok(expr),
            _ => Err(self
                .parser
                .err_here("internal expression parser state mismatch")),
        }
    }

    fn pop_select(&mut self) -> Result<HeightTracked<SelectStatement>, ParseError> {
        match self.values.pop() {
            Some(MachineValue::Select(select)) => Ok(select),
            _ => Err(self
                .parser
                .err_here("internal SELECT parser state mismatch")),
        }
    }

    fn pop_core(&mut self) -> Result<HeightTracked<SelectCore>, ParseError> {
        match self.values.pop() {
            Some(MachineValue::Core(core)) => Ok(core),
            _ => Err(self
                .parser
                .err_here("internal SELECT-core parser state mismatch")),
        }
    }

    fn pop_from(&mut self) -> Result<FromClause, ParseError> {
        match self.values.pop() {
            Some(MachineValue::From(from)) => Ok(from),
            _ => Err(self.parser.err_here("internal FROM parser state mismatch")),
        }
    }

    fn pop_table(&mut self) -> Result<TableOrSubquery, ParseError> {
        match self.values.pop() {
            Some(MachineValue::Table(table)) => Ok(table),
            _ => Err(self.parser.err_here("internal table parser state mismatch")),
        }
    }

    fn pop_ordering(&mut self) -> Result<HeightTracked<OrderingTerm>, ParseError> {
        match self.values.pop() {
            Some(MachineValue::Ordering(term)) => Ok(term),
            _ => Err(self
                .parser
                .err_here("internal ORDER BY parser state mismatch")),
        }
    }

    fn pop_window(&mut self) -> Result<WindowSpec, ParseError> {
        match self.values.pop() {
            Some(MachineValue::Window(window)) => Ok(window),
            _ => Err(self
                .parser
                .err_here("internal WINDOW parser state mismatch")),
        }
    }

    fn pop_frame_bound(&mut self) -> Result<ParsedFrameBound, ParseError> {
        match self.values.pop() {
            Some(MachineValue::FrameBound(bound)) => Ok(bound),
            _ => Err(self
                .parser
                .err_here("internal frame-bound parser state mismatch")),
        }
    }

    fn pop_with(&mut self) -> Result<WithClause, ParseError> {
        match self.values.pop() {
            Some(MachineValue::With(with)) => Ok(with),
            _ => Err(self.parser.err_here("internal WITH parser state mismatch")),
        }
    }

    fn push_expr_tail(&mut self, expr: ParsedExpr, min_bp: u8) {
        self.values.push(MachineValue::Expr(expr));
        self.controls.push(ParseControl::ExprTail { min_bp });
    }

    fn finish_function(
        &mut self,
        outer_min_bp: u8,
        build: FunctionBuild,
    ) -> Result<(), ParseError> {
        let span = build.start.merge(build.end);
        let parsed = self.parser.checked_expr(
            Expr::FunctionCall {
                name: build.name,
                args: build.args,
                distinct: build.distinct,
                order_by: build.order_by,
                filter: build.filter,
                over: build.over,
                span,
            },
            build.height,
            false,
            true,
        )?;
        self.push_expr_tail(parsed, outer_min_bp);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn step(&mut self, control: ParseControl) -> Result<(), ParseError> {
        match control {
            ParseControl::ExprStart { min_bp } => self.expr_start(min_bp),
            ParseControl::ExprTail { min_bp } => self.expr_tail(min_bp),
            ParseControl::UnaryDone {
                outer_min_bp,
                op,
                span,
            } => {
                let inner = self.pop_expr()?;
                let span = span.merge(inner.expr.span());
                let parsed = self.parser.finish_unary(op, inner, span)?;
                self.push_expr_tail(parsed, outer_min_bp);
                Ok(())
            }
            ParseControl::CastDone {
                outer_min_bp,
                start,
            } => {
                let inner = self.pop_expr()?;
                self.parser.expect_kind(&TokenKind::KwAs)?;
                let type_name = self.parser.parse_type_name()?;
                let end = self.parser.expect_kind(&TokenKind::RightParen)?;
                let height = inner.height;
                let is_constant = inner.is_constant;
                let has_function = inner.has_function;
                let parsed = self.parser.checked_expr(
                    Expr::Cast {
                        expr: Box::new(inner.expr),
                        type_name,
                        span: start.merge(end),
                    },
                    height,
                    is_constant,
                    has_function,
                )?;
                self.push_expr_tail(parsed, outer_min_bp);
                Ok(())
            }
            ParseControl::GroupFirstDone {
                outer_min_bp,
                start,
            } => {
                let first = self.pop_expr()?;
                if self.parser.eat_kind(&TokenKind::Comma) {
                    let is_constant = first.is_constant;
                    let has_function = first.has_function;
                    self.controls.push(ParseControl::RowItemDone {
                        outer_min_bp,
                        start,
                        values: vec![first.expr],
                        is_constant,
                        has_function,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                } else {
                    self.parser.expect_kind(&TokenKind::RightParen)?;
                    self.push_expr_tail(first, outer_min_bp);
                }
                Ok(())
            }
            ParseControl::RowItemDone {
                outer_min_bp,
                start,
                mut values,
                mut is_constant,
                mut has_function,
            } => {
                let parsed = self.pop_expr()?;
                is_constant &= parsed.is_constant;
                has_function |= parsed.has_function;
                values.push(parsed.expr);
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::RowItemDone {
                        outer_min_bp,
                        start,
                        values,
                        is_constant,
                        has_function,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                } else {
                    let end = self.parser.expect_kind(&TokenKind::RightParen)?;
                    let parsed = self.parser.finish_expr(
                        Expr::RowValue(values, start.merge(end)),
                        1,
                        is_constant,
                        has_function,
                    )?;
                    self.push_expr_tail(parsed, outer_min_bp);
                }
                Ok(())
            }
            ParseControl::CaseOperandDone {
                outer_min_bp,
                start,
            } => {
                let operand = self.pop_expr()?;
                self.controls.push(ParseControl::CaseWhenStart {
                    outer_min_bp,
                    build: CaseBuild {
                        start,
                        operand: Some(operand),
                        whens: Vec::new(),
                    },
                });
                Ok(())
            }
            ParseControl::CaseWhenStart {
                outer_min_bp,
                build,
            } => {
                if self.parser.eat_kind(&TokenKind::KwWhen) {
                    self.controls.push(ParseControl::CaseConditionDone {
                        outer_min_bp,
                        build,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                    return Ok(());
                }
                if build.whens.is_empty() {
                    return Err(self
                        .parser
                        .err_here("CASE requires at least one WHEN clause"));
                }
                if self.parser.eat_kind(&TokenKind::KwElse) {
                    self.controls.push(ParseControl::CaseElseDone {
                        outer_min_bp,
                        build,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                    return Ok(());
                }
                self.finish_case(outer_min_bp, build, None)
            }
            ParseControl::CaseConditionDone {
                outer_min_bp,
                build,
            } => {
                let condition = self.pop_expr()?;
                if !self.parser.eat_kind(&TokenKind::KwThen) {
                    return Err(self.parser.err_here("expected THEN in CASE expression"));
                }
                self.controls.push(ParseControl::CaseResultDone {
                    outer_min_bp,
                    build,
                    condition,
                });
                self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                Ok(())
            }
            ParseControl::CaseResultDone {
                outer_min_bp,
                mut build,
                condition,
            } => {
                let result = self.pop_expr()?;
                build.whens.push((condition, result));
                self.controls.push(ParseControl::CaseWhenStart {
                    outer_min_bp,
                    build,
                });
                Ok(())
            }
            ParseControl::CaseElseDone {
                outer_min_bp,
                build,
            } => {
                let else_expr = self.pop_expr()?;
                self.finish_case(outer_min_bp, build, Some(else_expr))
            }
            ParseControl::FunctionArgDone {
                outer_min_bp,
                mut build,
            } => {
                let arg = self.pop_expr()?;
                build.height = build.height.max(arg.height);
                let FunctionArgs::List(args) = &mut build.args else {
                    return Err(self
                        .parser
                        .err_here("internal function argument state mismatch"));
                };
                args.push(arg.expr);
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::FunctionArgDone {
                        outer_min_bp,
                        build,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                } else {
                    self.controls.push(ParseControl::FunctionOrderStart {
                        outer_min_bp,
                        build,
                    });
                }
                Ok(())
            }
            ParseControl::FunctionOrderStart {
                outer_min_bp,
                build,
            } => {
                if self.parser.eat_kind(&TokenKind::KwOrder) {
                    self.parser.expect_kind(&TokenKind::KwBy)?;
                    self.controls.push(ParseControl::FunctionOrderDone {
                        outer_min_bp,
                        build,
                    });
                    self.controls.push(ParseControl::OrderingStart);
                } else {
                    self.controls.push(ParseControl::FunctionClose {
                        outer_min_bp,
                        build,
                    });
                }
                Ok(())
            }
            ParseControl::FunctionOrderDone {
                outer_min_bp,
                mut build,
            } => {
                let term = self.pop_ordering()?;
                build.order_by.push(term.value);
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::FunctionOrderDone {
                        outer_min_bp,
                        build,
                    });
                    self.controls.push(ParseControl::OrderingStart);
                } else {
                    self.controls.push(ParseControl::FunctionClose {
                        outer_min_bp,
                        build,
                    });
                }
                Ok(())
            }
            ParseControl::FunctionClose {
                outer_min_bp,
                mut build,
            } => {
                build.end = self.parser.expect_kind(&TokenKind::RightParen)?;
                if matches!(self.parser.peek_kind(), TokenKind::KwFilter)
                    && self
                        .parser
                        .tokens
                        .get(self.parser.pos + 1)
                        .is_some_and(|token| token.kind == TokenKind::LeftParen)
                {
                    self.parser.advance_token();
                    self.parser.expect_kind(&TokenKind::LeftParen)?;
                    self.parser.expect_kind(&TokenKind::KwWhere)?;
                    self.controls.push(ParseControl::FunctionFilterDone {
                        outer_min_bp,
                        build,
                        has_filter: true,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                } else {
                    self.controls.push(ParseControl::FunctionFilterDone {
                        outer_min_bp,
                        build,
                        has_filter: false,
                    });
                }
                Ok(())
            }
            ParseControl::FunctionFilterDone {
                outer_min_bp,
                mut build,
                has_filter,
            } => {
                if has_filter {
                    let filter = self.pop_expr()?;
                    let end = self.parser.expect_kind(&TokenKind::RightParen)?;
                    build.end = build.end.merge(end);
                    build.filter = Some(Box::new(filter.expr));
                }
                if matches!(self.parser.peek_kind(), TokenKind::KwOver)
                    && self
                        .parser
                        .tokens
                        .get(self.parser.pos + 1)
                        .is_some_and(|token| {
                            matches!(token.kind, TokenKind::LeftParen)
                                || starts_bare_window_name(&token.kind)
                        })
                {
                    self.parser.advance_token();
                    if self.parser.eat_kind(&TokenKind::LeftParen) {
                        self.controls.push(ParseControl::FunctionOverDone {
                            outer_min_bp,
                            build,
                        });
                        self.controls.push(ParseControl::WindowStart);
                    } else {
                        let base_window = self.parser.parse_window_name()?;
                        let base_span = self.parser.tokens[self.parser.pos.saturating_sub(1)].span;
                        build.end = build.end.merge(base_span);
                        build.over = Some(WindowSpec {
                            window_ref: Some(WindowReference::Direct(base_window)),
                            partition_by: Vec::new(),
                            order_by: Vec::new(),
                            frame: None,
                        });
                        self.finish_function(outer_min_bp, build)?;
                    }
                } else {
                    self.finish_function(outer_min_bp, build)?;
                }
                Ok(())
            }
            ParseControl::FunctionOverDone {
                outer_min_bp,
                mut build,
            } => {
                build.over = Some(self.pop_window()?);
                let end = self.parser.expect_kind(&TokenKind::RightParen)?;
                build.end = build.end.merge(end);
                self.finish_function(outer_min_bp, build)
            }
            ParseControl::BinaryDone {
                outer_min_bp,
                lhs,
                op,
            } => {
                let rhs = self.pop_expr()?;
                let span = lhs.expr.span().merge(rhs.expr.span());
                let height = lhs.height.max(rhs.height);
                let is_constant = lhs.is_constant && rhs.is_constant;
                let has_function = lhs.has_function || rhs.has_function;
                let parsed = self.parser.checked_expr(
                    Expr::BinaryOp {
                        left: Box::new(lhs.expr),
                        op,
                        right: Box::new(rhs.expr),
                        span,
                    },
                    height,
                    is_constant,
                    has_function,
                )?;
                self.push_expr_tail(parsed, outer_min_bp);
                Ok(())
            }
            ParseControl::JsonDone {
                outer_min_bp,
                lhs,
                arrow,
            } => {
                let rhs = self.pop_expr()?;
                let span = lhs.expr.span().merge(rhs.expr.span());
                let height = lhs.height.max(rhs.height);
                let parsed = self.parser.checked_expr(
                    Expr::JsonAccess {
                        expr: Box::new(lhs.expr),
                        path: Box::new(rhs.expr),
                        arrow,
                        span,
                    },
                    height,
                    false,
                    true,
                )?;
                self.push_expr_tail(parsed, outer_min_bp);
                Ok(())
            }
            ParseControl::IsDone {
                outer_min_bp,
                lhs,
                not,
            } => {
                let rhs = self.pop_expr()?;
                let span = lhs.expr.span().merge(rhs.expr.span());
                let parsed = if matches!(&rhs.expr, Expr::Literal(Literal::Null, _)) {
                    self.parser.checked_expr(
                        Expr::IsNull {
                            expr: Box::new(lhs.expr),
                            not,
                            span,
                        },
                        lhs.height,
                        lhs.is_constant,
                        lhs.has_function,
                    )?
                } else {
                    let height = lhs.height.max(rhs.height);
                    let is_constant = lhs.is_constant && rhs.is_constant;
                    let has_function = lhs.has_function || rhs.has_function;
                    self.parser.checked_expr(
                        Expr::BinaryOp {
                            left: Box::new(lhs.expr),
                            op: if not { BinaryOp::IsNot } else { BinaryOp::Is },
                            right: Box::new(rhs.expr),
                            span,
                        },
                        height,
                        is_constant,
                        has_function,
                    )?
                };
                self.push_expr_tail(parsed, outer_min_bp);
                Ok(())
            }
            ParseControl::LikePatternDone {
                outer_min_bp,
                lhs,
                op,
                not,
            } => {
                let pattern = self.pop_expr()?;
                if self.parser.eat_kind(&TokenKind::KwEscape) {
                    self.controls.push(ParseControl::LikeEscapeDone {
                        outer_min_bp,
                        lhs,
                        pattern,
                        op,
                        not,
                    });
                    self.controls.push(ParseControl::ExprStart {
                        min_bp: bp::EQUALITY.1,
                    });
                    return Ok(());
                }
                self.finish_like(outer_min_bp, lhs, pattern, None, op, not)
            }
            ParseControl::LikeEscapeDone {
                outer_min_bp,
                lhs,
                pattern,
                op,
                not,
            } => {
                let escape = self.pop_expr()?;
                self.finish_like(outer_min_bp, lhs, pattern, Some(escape), op, not)
            }
            ParseControl::BetweenLowDone {
                outer_min_bp,
                lhs,
                not,
            } => {
                let low = self.pop_expr()?;
                if !self.parser.eat_kind(&TokenKind::KwAnd) {
                    return Err(self.parser.err_here("expected AND in BETWEEN expression"));
                }
                self.controls.push(ParseControl::BetweenHighDone {
                    outer_min_bp,
                    lhs,
                    low,
                    not,
                });
                self.controls.push(ParseControl::ExprStart {
                    min_bp: bp::EQUALITY.1,
                });
                Ok(())
            }
            ParseControl::BetweenHighDone {
                outer_min_bp,
                lhs,
                low,
                not,
            } => {
                let high = self.pop_expr()?;
                let span = lhs.expr.span().merge(high.expr.span());
                let height = lhs.height.max(low.height).max(high.height);
                let is_constant = lhs.is_constant && low.is_constant && high.is_constant;
                let has_function = lhs.has_function || low.has_function || high.has_function;
                let parsed = self.parser.checked_expr(
                    Expr::Between {
                        expr: Box::new(lhs.expr),
                        low: Box::new(low.expr),
                        high: Box::new(high.expr),
                        not,
                        span,
                    },
                    height,
                    is_constant,
                    has_function,
                )?;
                let parsed = if not {
                    self.parser.add_cached_parent(parsed)?
                } else {
                    parsed
                };
                self.push_expr_tail(parsed, outer_min_bp);
                Ok(())
            }
            ParseControl::InItemDone {
                outer_min_bp,
                lhs,
                not,
                mut items,
                start,
            } => {
                let item = self.pop_expr()?;
                items.push(item);
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::InItemDone {
                        outer_min_bp,
                        lhs,
                        not,
                        items,
                        start,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                } else {
                    let end = self.parser.expect_kind(&TokenKind::RightParen)?;
                    self.finish_in_list(outer_min_bp, lhs, not, items, start.merge(end))?;
                }
                Ok(())
            }
            ParseControl::InSelectDone {
                outer_min_bp,
                lhs,
                not,
                start,
            } => {
                let select = self.pop_select()?;
                let end = self.parser.expect_kind(&TokenKind::RightParen)?;
                let height = lhs.height.max(select.height);
                let has_function = lhs.has_function;
                let parsed = self.parser.checked_expr(
                    Expr::In {
                        expr: Box::new(lhs.expr),
                        set: InSet::Subquery(Box::new(select.value)),
                        not,
                        span: start.merge(end),
                    },
                    height,
                    false,
                    has_function,
                )?;
                let parsed = if not {
                    self.parser.add_cached_parent(parsed)?
                } else {
                    parsed
                };
                self.push_expr_tail(parsed, outer_min_bp);
                Ok(())
            }
            ParseControl::ExistsDone {
                outer_min_bp,
                not,
                start,
            } => {
                let select = self.pop_select()?;
                let end = self.parser.expect_kind(&TokenKind::RightParen)?;
                let parsed = self.parser.checked_expr(
                    Expr::Exists {
                        subquery: Box::new(select.value),
                        not,
                        span: start.merge(end),
                    },
                    select.height,
                    false,
                    false,
                )?;
                let parsed = if not {
                    self.parser.add_cached_parent(parsed)?
                } else {
                    parsed
                };
                self.push_expr_tail(parsed, outer_min_bp);
                Ok(())
            }
            ParseControl::ScalarSelectDone {
                outer_min_bp,
                start,
            } => {
                let select = self.pop_select()?;
                let end = self.parser.expect_kind(&TokenKind::RightParen)?;
                let parsed = self.parser.checked_expr(
                    Expr::Subquery(Box::new(select.value), start.merge(end)),
                    select.height,
                    false,
                    false,
                )?;
                self.push_expr_tail(parsed, outer_min_bp);
                Ok(())
            }
            ParseControl::OrderingStart => {
                self.controls.push(ParseControl::OrderingDone);
                self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                Ok(())
            }
            ParseControl::OrderingDone => {
                let expr = self.pop_expr()?;
                let direction = if self.parser.eat_kind(&TokenKind::KwAsc) {
                    Some(SortDirection::Asc)
                } else if self.parser.eat_kind(&TokenKind::KwDesc) {
                    Some(SortDirection::Desc)
                } else {
                    None
                };
                let nulls = if self.parser.eat_kind(&TokenKind::KwNulls) {
                    if self.parser.eat_kind(&TokenKind::KwFirst) {
                        Some(NullsOrder::First)
                    } else {
                        self.parser.expect_kw(&TokenKind::KwLast)?;
                        Some(NullsOrder::Last)
                    }
                } else {
                    None
                };
                self.values.push(MachineValue::Ordering(HeightTracked {
                    height: expr.height,
                    value: OrderingTerm {
                        expr: expr.expr,
                        direction,
                        nulls,
                    },
                }));
                Ok(())
            }
            ParseControl::WindowStart => self.window_start(),
            ParseControl::WindowPartitionDone { mut build } => {
                build.partition_by.push(self.pop_expr()?.expr);
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls
                        .push(ParseControl::WindowPartitionDone { build });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                } else {
                    self.controls.push(ParseControl::WindowOrderStart { build });
                }
                Ok(())
            }
            ParseControl::WindowOrderStart { build } => {
                if self.parser.eat_kind(&TokenKind::KwOrder) {
                    self.parser.expect_kw(&TokenKind::KwBy)?;
                    self.controls.push(ParseControl::WindowOrderDone { build });
                    self.controls.push(ParseControl::OrderingStart);
                } else {
                    self.controls.push(ParseControl::WindowFrameStart { build });
                }
                Ok(())
            }
            ParseControl::WindowOrderDone { mut build } => {
                build.order_by.push(self.pop_ordering()?.value);
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::WindowOrderDone { build });
                    self.controls.push(ParseControl::OrderingStart);
                } else {
                    self.controls.push(ParseControl::WindowFrameStart { build });
                }
                Ok(())
            }
            ParseControl::WindowFrameStart { build } => {
                self.window_frame_start(build);
                Ok(())
            }
            ParseControl::WindowFirstBoundDone {
                build,
                frame_type,
                between,
            } => {
                let start = self.pop_frame_bound()?;
                validate_frame_start(&start, between)?;
                if between {
                    self.parser.expect_kw(&TokenKind::KwAnd)?;
                    self.controls.push(ParseControl::WindowSecondBoundDone {
                        build,
                        frame_type,
                        start,
                    });
                    self.controls.push(ParseControl::FrameBoundStart);
                } else {
                    let frame = self.finish_frame(frame_type, start.value, None)?;
                    self.values.push(MachineValue::Window(WindowSpec {
                        window_ref: build.base_window.map(WindowReference::Base),
                        partition_by: build.partition_by,
                        order_by: build.order_by,
                        frame: Some(frame),
                    }));
                }
                Ok(())
            }
            ParseControl::WindowSecondBoundDone {
                build,
                frame_type,
                start,
            } => {
                let end = self.pop_frame_bound()?;
                validate_frame_end(&start, &end)?;
                let frame = self.finish_frame(frame_type, start.value, Some(end.value))?;
                self.values.push(MachineValue::Window(WindowSpec {
                    window_ref: build.base_window.map(WindowReference::Base),
                    partition_by: build.partition_by,
                    order_by: build.order_by,
                    frame: Some(frame),
                }));
                Ok(())
            }
            ParseControl::FrameBoundStart => self.frame_bound_start(),
            ParseControl::FrameBoundExprDone { origin } => {
                let expr = self.pop_expr()?.expr;
                let bound = if self.parser.eat_kind(&TokenKind::KwPreceding) {
                    FrameBound::Preceding(Box::new(expr))
                } else {
                    self.parser.expect_kw(&TokenKind::KwFollowing)?;
                    FrameBound::Following(Box::new(expr))
                };
                self.values.push(MachineValue::FrameBound(ParsedFrameBound {
                    value: bound,
                    origin,
                }));
                Ok(())
            }
            ParseControl::SubqueryStart => {
                if self.parser.at_kind(&TokenKind::KwWith) {
                    self.controls.push(ParseControl::SubqueryWithDone);
                    self.controls.push(ParseControl::WithStart);
                } else {
                    self.controls.push(ParseControl::SelectStart { with: None });
                }
                Ok(())
            }
            ParseControl::SubqueryWithDone => {
                let with = self.pop_with()?;
                self.controls
                    .push(ParseControl::SelectStart { with: Some(with) });
                Ok(())
            }
            other => self.step_select(other),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn expr_start(&mut self, min_bp: u8) -> Result<(), ParseError> {
        let Token {
            kind,
            span: token_span,
            line,
            col,
        } = self.parser.advance_token();
        if self.parser.at_kind(&TokenKind::Dot) && starts_table_star_qualifier(&kind) {
            let name = match &kind {
                TokenKind::Id(name) | TokenKind::QuotedId(name, _) => Arc::clone(name),
                TokenKind::String(name) => Arc::<str>::from(name.as_str()),
                keyword => Arc::<str>::from(kw_to_str(keyword)),
            };
            return self.identifier_or_function(name, token_span, min_bp);
        }
        let parsed = match kind {
            TokenKind::Integer(value) => {
                ParsedExpr::leaf(Expr::Literal(Literal::Integer(value), token_span))
            }
            TokenKind::OversizedInt(value) => match value.parse::<f64>() {
                Ok(value) => ParsedExpr::leaf(Expr::Literal(Literal::Float(value), token_span)),
                Err(_) => {
                    return Err(ParseError {
                        kind: crate::parser::ParseErrorKind::Syntax,
                        message: "integer out of range".to_owned(),
                        span: token_span,
                        line,
                        col,
                    });
                }
            },
            TokenKind::Float(value) => {
                ParsedExpr::leaf(Expr::Literal(Literal::Float(value), token_span))
            }
            TokenKind::String(value) if self.parser.at_kind(&TokenKind::Dot) => {
                return self.identifier_or_function(Arc::<str>::from(value), token_span, min_bp);
            }
            TokenKind::String(value) => {
                ParsedExpr::leaf(Expr::Literal(Literal::String(value), token_span))
            }
            TokenKind::Blob(value) => {
                ParsedExpr::leaf(Expr::Literal(Literal::Blob(value), token_span))
            }
            TokenKind::KwNull => ParsedExpr::leaf(Expr::Literal(Literal::Null, token_span)),
            TokenKind::KwTrue => ParsedExpr::leaf(Expr::Literal(Literal::True, token_span)),
            TokenKind::KwFalse => ParsedExpr::leaf(Expr::Literal(Literal::False, token_span)),
            TokenKind::KwCurrentTime => {
                ParsedExpr::leaf(Expr::Literal(Literal::CurrentTime, token_span))
            }
            TokenKind::KwCurrentDate => {
                ParsedExpr::leaf(Expr::Literal(Literal::CurrentDate, token_span))
            }
            TokenKind::KwCurrentTimestamp => {
                ParsedExpr::leaf(Expr::Literal(Literal::CurrentTimestamp, token_span))
            }
            TokenKind::Question => {
                ParsedExpr::leaf(Expr::Placeholder(PlaceholderType::Anonymous, token_span))
            }
            TokenKind::QuestionNum(value) => ParsedExpr::leaf(Expr::Placeholder(
                PlaceholderType::Numbered(value),
                token_span,
            )),
            TokenKind::ColonParam(value) => ParsedExpr::leaf(Expr::Placeholder(
                PlaceholderType::ColonNamed(value),
                token_span,
            )),
            TokenKind::AtParam(value) => ParsedExpr::leaf(Expr::Placeholder(
                PlaceholderType::AtNamed(value),
                token_span,
            )),
            TokenKind::DollarParam(value) => ParsedExpr::leaf(Expr::Placeholder(
                PlaceholderType::DollarNamed(value),
                token_span,
            )),
            TokenKind::Minus => {
                if let TokenKind::OversizedInt(value) = self.parser.peek_kind()
                    && value == "9223372036854775808"
                {
                    let number_span = self.parser.advance_token().span;
                    let parsed = self.parser.finish_expr(
                        Expr::Literal(Literal::Integer(i64::MIN), token_span.merge(number_span)),
                        1,
                        true,
                        false,
                    )?;
                    self.push_expr_tail(parsed, min_bp);
                    return Ok(());
                }
                self.controls.push(ParseControl::UnaryDone {
                    outer_min_bp: min_bp,
                    op: UnaryOp::Negate,
                    span: token_span,
                });
                self.controls
                    .push(ParseControl::ExprStart { min_bp: bp::UNARY });
                return Ok(());
            }
            TokenKind::Plus => {
                self.controls.push(ParseControl::UnaryDone {
                    outer_min_bp: min_bp,
                    op: UnaryOp::Plus,
                    span: token_span,
                });
                self.controls
                    .push(ParseControl::ExprStart { min_bp: bp::UNARY });
                return Ok(());
            }
            TokenKind::Tilde => {
                self.controls.push(ParseControl::UnaryDone {
                    outer_min_bp: min_bp,
                    op: UnaryOp::BitNot,
                    span: token_span,
                });
                self.controls
                    .push(ParseControl::ExprStart { min_bp: bp::UNARY });
                return Ok(());
            }
            TokenKind::KwNot => {
                if self.parser.eat_kind(&TokenKind::KwExists) {
                    self.parser.expect_kind(&TokenKind::LeftParen)?;
                    self.controls.push(ParseControl::ExistsDone {
                        outer_min_bp: min_bp,
                        not: true,
                        start: token_span,
                    });
                    self.controls.push(ParseControl::SubqueryStart);
                } else {
                    self.controls.push(ParseControl::UnaryDone {
                        outer_min_bp: min_bp,
                        op: UnaryOp::Not,
                        span: token_span,
                    });
                    self.controls.push(ParseControl::ExprStart {
                        min_bp: bp::NOT_PREFIX,
                    });
                }
                return Ok(());
            }
            TokenKind::KwExists => {
                self.parser.expect_kind(&TokenKind::LeftParen)?;
                self.controls.push(ParseControl::ExistsDone {
                    outer_min_bp: min_bp,
                    not: false,
                    start: token_span,
                });
                self.controls.push(ParseControl::SubqueryStart);
                return Ok(());
            }
            TokenKind::KwCast => {
                self.parser.expect_kind(&TokenKind::LeftParen)?;
                self.controls.push(ParseControl::CastDone {
                    outer_min_bp: min_bp,
                    start: token_span,
                });
                self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                return Ok(());
            }
            TokenKind::KwCase => {
                if self.parser.at_kind(&TokenKind::KwWhen) {
                    self.controls.push(ParseControl::CaseWhenStart {
                        outer_min_bp: min_bp,
                        build: CaseBuild {
                            start: token_span,
                            operand: None,
                            whens: Vec::new(),
                        },
                    });
                } else {
                    self.controls.push(ParseControl::CaseOperandDone {
                        outer_min_bp: min_bp,
                        start: token_span,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                }
                return Ok(());
            }
            TokenKind::KwRaise => {
                self.parser.expect_kind(&TokenKind::LeftParen)?;
                let (action, message) = self.parser.parse_raise_args()?;
                let end = self.parser.expect_kind(&TokenKind::RightParen)?;
                ParsedExpr::leaf(Expr::Raise {
                    action,
                    message,
                    span: token_span.merge(end),
                })
            }
            TokenKind::LeftParen => {
                if matches!(
                    self.parser.peek_kind(),
                    TokenKind::KwSelect | TokenKind::KwWith | TokenKind::KwValues
                ) {
                    self.controls.push(ParseControl::ScalarSelectDone {
                        outer_min_bp: min_bp,
                        start: token_span,
                    });
                    self.controls.push(ParseControl::SubqueryStart);
                } else {
                    self.controls.push(ParseControl::GroupFirstDone {
                        outer_min_bp: min_bp,
                        start: token_span,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                }
                return Ok(());
            }
            TokenKind::Id(name) | TokenKind::QuotedId(name, _) => {
                return self.identifier_or_function(name, token_span, min_bp);
            }
            TokenKind::KwReplace if self.parser.at_kind(&TokenKind::LeftParen) => {
                return self.start_function("replace".to_owned(), token_span, min_bp);
            }
            TokenKind::KwLike if self.parser.at_kind(&TokenKind::LeftParen) => {
                return self.start_function("like".to_owned(), token_span, min_bp);
            }
            TokenKind::KwGlob if self.parser.at_kind(&TokenKind::LeftParen) => {
                return self.start_function("glob".to_owned(), token_span, min_bp);
            }
            TokenKind::KwRegexp if self.parser.at_kind(&TokenKind::LeftParen) => {
                return self.start_function("regexp".to_owned(), token_span, min_bp);
            }
            TokenKind::KwMatch if self.parser.at_kind(&TokenKind::LeftParen) => {
                return self.start_function("match".to_owned(), token_span, min_bp);
            }
            kind if is_nonreserved_kw(&kind) => {
                let name = Arc::<str>::from(kw_to_str(&kind));
                return self.identifier_or_function(name, token_span, min_bp);
            }
            TokenKind::Error(msg) => {
                // A tokenizer error already carries SQLite's stock-form message
                // (`unrecognized token: "X"`); surface it verbatim under the
                // Tokenizer kind so no `SQL error at offset N:` prefix is added.
                // bd-parser-syntax-error-format-6w6kp (Part A).
                return Err(ParseError {
                    kind: crate::parser::ParseErrorKind::Tokenizer,
                    message: msg,
                    span: token_span,
                    line,
                    col,
                });
            }
            kind => {
                return Err(ParseError {
                    kind: crate::parser::ParseErrorKind::UnexpectedToken,
                    message: format!("unexpected token in expression: {kind:?}"),
                    span: token_span,
                    line,
                    col,
                });
            }
        };
        self.push_expr_tail(parsed, min_bp);
        Ok(())
    }

    fn identifier_or_function(
        &mut self,
        name: Arc<str>,
        start: Span,
        min_bp: u8,
    ) -> Result<(), ParseError> {
        if self.parser.at_kind(&TokenKind::LeftParen) {
            return self.start_function(name.to_string(), start, min_bp);
        }
        let parsed = if self.parser.at_kind(&TokenKind::Dot) {
            let Some(mid_token) = self.parser.tokens.get(self.parser.pos + 1).cloned() else {
                return Err(self.parser.err_here("expected column name after '.'"));
            };
            let mid = match &mid_token.kind {
                TokenKind::Id(mid) | TokenKind::QuotedId(mid, _) => Arc::clone(mid),
                TokenKind::String(mid) => Arc::<str>::from(mid.as_str()),
                kind if starts_post_dot_identifier(kind) => Arc::<str>::from(kw_to_str(kind)),
                _ => {
                    return Err(ParseError::at(
                        format!("expected column name after '.', got {:?}", mid_token.kind),
                        Some(&mid_token),
                    ));
                }
            };
            // Three-part reference `schema.table.column` (e.g. `main.t.id`):
            // a second dot after the middle segment promotes `name` to the
            // schema/database qualifier and `mid` to the table qualifier.
            if matches!(
                self.parser.tokens.get(self.parser.pos + 2).map(|t| &t.kind),
                Some(TokenKind::Dot)
            ) {
                let Some(column_token) = self.parser.tokens.get(self.parser.pos + 3).cloned()
                else {
                    return Err(ParseError::at(
                        "expected column name after '.'".to_owned(),
                        self.parser.tokens.get(self.parser.pos + 2),
                    ));
                };
                let column = match &column_token.kind {
                    TokenKind::Id(column) | TokenKind::QuotedId(column, _) => Arc::clone(column),
                    TokenKind::String(column) => Arc::<str>::from(column.as_str()),
                    kind if starts_post_dot_identifier(kind) => Arc::<str>::from(kw_to_str(kind)),
                    _ => {
                        return Err(ParseError::at(
                            format!(
                                "expected column name after '.', got {:?}",
                                column_token.kind
                            ),
                            Some(&column_token),
                        ));
                    }
                };
                self.parser.pos = self.parser.pos.saturating_add(4);
                self.parser.finish_expr(
                    Expr::Column(
                        ColumnRef::schema_qualified(name, mid, column),
                        start.merge(column_token.span),
                    ),
                    2,
                    false,
                    false,
                )?
            } else {
                self.parser.pos = self.parser.pos.saturating_add(2);
                self.parser.finish_expr(
                    Expr::Column(
                        ColumnRef::qualified(name, mid),
                        start.merge(mid_token.span),
                    ),
                    2,
                    false,
                    false,
                )?
            }
        } else {
            ParsedExpr::leaf(Expr::Column(ColumnRef::bare(name), start))
        };
        self.push_expr_tail(parsed, min_bp);
        Ok(())
    }

    fn start_function(
        &mut self,
        name: String,
        start: Span,
        outer_min_bp: u8,
    ) -> Result<(), ParseError> {
        self.parser.expect_kind(&TokenKind::LeftParen)?;
        let mut build = FunctionBuild {
            name,
            start,
            args: FunctionArgs::List(Vec::new()),
            distinct: false,
            height: 0,
            order_by: Vec::new(),
            filter: None,
            over: None,
            end: start,
        };
        if self.parser.eat_kind(&TokenKind::Star) {
            // bd-2fong parity: stock SQLite parses `f(*)` for ANY function.
            // Only count keeps star semantics; every other function treats
            // the star as a zero-argument call, so arity validation later
            // yields stock's exact behavior — `random(*)` evaluates,
            // `abs(*)` / `max(*)` fail with "wrong number of arguments".
            build.args = if build.name.eq_ignore_ascii_case("count") {
                FunctionArgs::Star
            } else {
                FunctionArgs::List(Vec::new())
            };
            self.controls.push(ParseControl::FunctionClose {
                outer_min_bp,
                build,
            });
        } else {
            build.distinct = self.parser.eat_kind(&TokenKind::KwDistinct);
            if self.parser.at_kind(&TokenKind::RightParen) {
                if build.distinct {
                    return Err(self
                        .parser
                        .err_here("DISTINCT requires at least one argument"));
                }
                self.controls.push(ParseControl::FunctionOrderStart {
                    outer_min_bp,
                    build,
                });
            } else {
                self.controls.push(ParseControl::FunctionArgDone {
                    outer_min_bp,
                    build,
                });
                self.controls.push(ParseControl::ExprStart { min_bp: 0 });
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn expr_tail(&mut self, min_bp: u8) -> Result<(), ParseError> {
        let lhs = self.pop_expr()?;
        if let Some(left_bp) = self.parser.postfix_bp()
            && left_bp >= min_bp
        {
            let parsed = self.parser.parse_postfix(lhs)?;
            self.push_expr_tail(parsed, min_bp);
            return Ok(());
        }
        let Some((left_bp, right_bp)) = self.parser.infix_bp() else {
            self.values.push(MachineValue::Expr(lhs));
            return Ok(());
        };
        if left_bp < min_bp {
            self.values.push(MachineValue::Expr(lhs));
            return Ok(());
        }

        let token = self.parser.advance_token();
        let simple = match &token.kind {
            TokenKind::Plus => Some(BinaryOp::Add),
            TokenKind::Minus => Some(BinaryOp::Subtract),
            TokenKind::Star => Some(BinaryOp::Multiply),
            TokenKind::Slash => Some(BinaryOp::Divide),
            TokenKind::Percent => Some(BinaryOp::Modulo),
            TokenKind::Concat => Some(BinaryOp::Concat),
            TokenKind::Eq | TokenKind::EqEq => Some(BinaryOp::Eq),
            TokenKind::Ne | TokenKind::LtGt => Some(BinaryOp::Ne),
            TokenKind::Lt => Some(BinaryOp::Lt),
            TokenKind::Le => Some(BinaryOp::Le),
            TokenKind::Gt => Some(BinaryOp::Gt),
            TokenKind::Ge => Some(BinaryOp::Ge),
            TokenKind::Ampersand => Some(BinaryOp::BitAnd),
            TokenKind::Pipe => Some(BinaryOp::BitOr),
            TokenKind::ShiftLeft => Some(BinaryOp::ShiftLeft),
            TokenKind::ShiftRight => Some(BinaryOp::ShiftRight),
            TokenKind::KwOr => Some(BinaryOp::Or),
            TokenKind::KwAnd => Some(BinaryOp::And),
            _ => None,
        };
        if let Some(op) = simple {
            self.controls.push(ParseControl::BinaryDone {
                outer_min_bp: min_bp,
                lhs,
                op,
            });
            self.controls
                .push(ParseControl::ExprStart { min_bp: right_bp });
            return Ok(());
        }
        match &token.kind {
            TokenKind::KwIs => {
                let not = self.parser.eat_kind(&TokenKind::KwNot);
                if self.parser.eat_kind(&TokenKind::KwDistinct) {
                    self.parser.expect_kind(&TokenKind::KwFrom)?;
                    self.controls.push(ParseControl::BinaryDone {
                        outer_min_bp: min_bp,
                        lhs,
                        op: if not { BinaryOp::Is } else { BinaryOp::IsNot },
                    });
                } else {
                    self.controls.push(ParseControl::IsDone {
                        outer_min_bp: min_bp,
                        lhs,
                        not,
                    });
                }
                self.controls
                    .push(ParseControl::ExprStart { min_bp: right_bp });
            }
            TokenKind::KwLike | TokenKind::KwGlob | TokenKind::KwMatch | TokenKind::KwRegexp => {
                let op = match &token.kind {
                    TokenKind::KwLike => LikeOp::Like,
                    TokenKind::KwGlob => LikeOp::Glob,
                    TokenKind::KwMatch => LikeOp::Match,
                    TokenKind::KwRegexp => LikeOp::Regexp,
                    _ => unreachable!(),
                };
                self.controls.push(ParseControl::LikePatternDone {
                    outer_min_bp: min_bp,
                    lhs,
                    op,
                    not: false,
                });
                self.controls.push(ParseControl::ExprStart {
                    min_bp: bp::EQUALITY.1,
                });
            }
            TokenKind::KwBetween => {
                self.controls.push(ParseControl::BetweenLowDone {
                    outer_min_bp: min_bp,
                    lhs,
                    not: false,
                });
                self.controls.push(ParseControl::ExprStart {
                    min_bp: bp::NOT_PREFIX,
                });
            }
            TokenKind::KwIn => self.start_in(lhs, false, min_bp)?,
            TokenKind::Arrow => {
                self.controls.push(ParseControl::JsonDone {
                    outer_min_bp: min_bp,
                    lhs,
                    arrow: JsonArrow::Arrow,
                });
                self.controls
                    .push(ParseControl::ExprStart { min_bp: right_bp });
            }
            TokenKind::DoubleArrow => {
                self.controls.push(ParseControl::JsonDone {
                    outer_min_bp: min_bp,
                    lhs,
                    arrow: JsonArrow::DoubleArrow,
                });
                self.controls
                    .push(ParseControl::ExprStart { min_bp: right_bp });
            }
            TokenKind::KwNot => {
                let next = self.parser.advance_token();
                match &next.kind {
                    TokenKind::KwLike
                    | TokenKind::KwGlob
                    | TokenKind::KwMatch
                    | TokenKind::KwRegexp => {
                        let op = match &next.kind {
                            TokenKind::KwLike => LikeOp::Like,
                            TokenKind::KwGlob => LikeOp::Glob,
                            TokenKind::KwMatch => LikeOp::Match,
                            TokenKind::KwRegexp => LikeOp::Regexp,
                            _ => unreachable!(),
                        };
                        self.controls.push(ParseControl::LikePatternDone {
                            outer_min_bp: min_bp,
                            lhs,
                            op,
                            not: true,
                        });
                        self.controls.push(ParseControl::ExprStart {
                            min_bp: bp::EQUALITY.1,
                        });
                    }
                    TokenKind::KwBetween => {
                        self.controls.push(ParseControl::BetweenLowDone {
                            outer_min_bp: min_bp,
                            lhs,
                            not: true,
                        });
                        self.controls.push(ParseControl::ExprStart {
                            min_bp: bp::NOT_PREFIX,
                        });
                    }
                    TokenKind::KwIn => self.start_in(lhs, true, min_bp)?,
                    _ => {
                        return Err(ParseError::at(
                            format!(
                                "expected LIKE/GLOB/MATCH/REGEXP/BETWEEN/IN after NOT, got {:?}",
                                next.kind
                            ),
                            Some(&next),
                        ));
                    }
                }
            }
            other => {
                return Err(ParseError::at(
                    format!("unexpected infix token: {other:?}"),
                    Some(&token),
                ));
            }
        }
        Ok(())
    }

    fn start_in(&mut self, lhs: ParsedExpr, not: bool, outer_min_bp: u8) -> Result<(), ParseError> {
        let start = lhs.expr.span();
        if !self.parser.eat_kind(&TokenKind::LeftParen) {
            let table = self.parser.parse_qualified_name()?;
            let end = self.parser.tokens[self.parser.pos.saturating_sub(1)].span;
            let height = lhs.height;
            let has_function = lhs.has_function;
            let parsed = self.parser.checked_expr(
                Expr::In {
                    expr: Box::new(lhs.expr),
                    set: InSet::Table(table),
                    not,
                    span: start.merge(end),
                },
                height,
                false,
                has_function,
            )?;
            let parsed = if not {
                self.parser.add_cached_parent(parsed)?
            } else {
                parsed
            };
            self.push_expr_tail(parsed, outer_min_bp);
        } else if matches!(
            self.parser.peek_kind(),
            TokenKind::KwSelect | TokenKind::KwWith | TokenKind::KwValues
        ) {
            self.controls.push(ParseControl::InSelectDone {
                outer_min_bp,
                lhs,
                not,
                start,
            });
            self.controls.push(ParseControl::SubqueryStart);
        } else if self.parser.at_kind(&TokenKind::RightParen) {
            let end = self.parser.expect_kind(&TokenKind::RightParen)?;
            self.finish_in_list(outer_min_bp, lhs, not, Vec::new(), start.merge(end))?;
        } else {
            self.controls.push(ParseControl::InItemDone {
                outer_min_bp,
                lhs,
                not,
                items: Vec::new(),
                start,
            });
            self.controls.push(ParseControl::ExprStart { min_bp: 0 });
        }
        Ok(())
    }

    fn finish_case(
        &mut self,
        outer_min_bp: u8,
        build: CaseBuild,
        else_expr: Option<ParsedExpr>,
    ) -> Result<(), ParseError> {
        if !self.parser.eat_kind(&TokenKind::KwEnd) {
            return Err(self.parser.err_here("expected END for CASE expression"));
        }
        let end = self.parser.tokens[self.parser.pos.saturating_sub(1)].span;
        let mut height = build.operand.as_ref().map_or(0, |expr| expr.height);
        let mut is_constant = build.operand.as_ref().is_none_or(|expr| expr.is_constant);
        let mut has_function = build.operand.as_ref().is_some_and(|expr| expr.has_function);
        for (condition, result) in &build.whens {
            height = height.max(condition.height).max(result.height);
            is_constant &= condition.is_constant && result.is_constant;
            has_function |= condition.has_function || result.has_function;
        }
        if let Some(expr) = &else_expr {
            height = height.max(expr.height);
            is_constant &= expr.is_constant;
            has_function |= expr.has_function;
        }
        let parsed = self.parser.checked_expr(
            Expr::Case {
                operand: build.operand.map(|expr| Box::new(expr.expr)),
                whens: build
                    .whens
                    .into_iter()
                    .map(|(condition, result)| (condition.expr, result.expr))
                    .collect(),
                else_expr: else_expr.map(|expr| Box::new(expr.expr)),
                span: build.start.merge(end),
            },
            height,
            is_constant,
            has_function,
        )?;
        self.push_expr_tail(parsed, outer_min_bp);
        Ok(())
    }

    fn finish_like(
        &mut self,
        outer_min_bp: u8,
        lhs: ParsedExpr,
        pattern: ParsedExpr,
        escape: Option<ParsedExpr>,
        op: LikeOp,
        not: bool,
    ) -> Result<(), ParseError> {
        let end = escape
            .as_ref()
            .map_or_else(|| pattern.expr.span(), |expr| expr.expr.span());
        let height = escape.as_ref().map_or_else(
            || lhs.height.max(pattern.height),
            |expr| lhs.height.max(pattern.height).max(expr.height),
        );
        let span = lhs.expr.span().merge(end);
        let parsed = self.parser.checked_expr(
            Expr::Like {
                expr: Box::new(lhs.expr),
                pattern: Box::new(pattern.expr),
                escape: escape.map(|expr| Box::new(expr.expr)),
                op,
                not,
                span,
            },
            height,
            false,
            true,
        )?;
        let parsed = if not {
            self.parser.add_cached_parent(parsed)?
        } else {
            parsed
        };
        self.push_expr_tail(parsed, outer_min_bp);
        Ok(())
    }

    fn finish_in_list(
        &mut self,
        outer_min_bp: u8,
        lhs: ParsedExpr,
        not: bool,
        items: Vec<ParsedExpr>,
        span: Span,
    ) -> Result<(), ParseError> {
        if let Some(message) = vector_in_list_arity_error(&lhs.expr, &items) {
            // Stock emits this row-value arity message VERBATIM (no offset
            // prefix, not a near-X form). bd-parser-syntax-error-format-6w6kp.
            return Err(self.parser.err_semantic(message));
        }
        let item_height = items.iter().map(|item| item.height).max().unwrap_or(0);
        let items_are_constant = items.iter().all(|item| item.is_constant);
        let item_has_function = items.iter().any(|item| item.has_function);
        let singleton_constant = matches!(items.as_slice(), [item] if item.is_constant)
            && lhs.root != CachedRoot::Vector;
        let singleton_subquery =
            matches!(items.as_slice(), [item] if item.root == CachedRoot::ScalarSubquery);
        let lhs_height = lhs.height;
        let lhs_is_constant = lhs.is_constant;
        let lhs_has_function = lhs.has_function;
        let expr = Expr::In {
            expr: Box::new(lhs.expr),
            set: InSet::List(items.into_iter().map(|item| item.expr).collect()),
            not,
            span,
        };
        let parsed = if item_height == 0 {
            if lhs_has_function {
                self.parser
                    .finish_expr(expr, lhs_height.saturating_add(1), false, true)?
            } else {
                self.parser.finish_expr(expr, 1, true, false)?
            }
        } else {
            let cached_child_height = if singleton_constant {
                lhs_height.max(item_height.saturating_add(1))
            } else if singleton_subquery {
                lhs_height.max(item_height.saturating_sub(1))
            } else {
                lhs_height.max(item_height)
            };
            let parsed = self.parser.checked_expr(
                expr,
                cached_child_height,
                lhs_is_constant && items_are_constant,
                lhs_has_function || item_has_function,
            )?;
            if not {
                self.parser.add_cached_parent(parsed)?
            } else {
                parsed
            }
        };
        self.push_expr_tail(parsed, outer_min_bp);
        Ok(())
    }

    fn window_start(&mut self) -> Result<(), ParseError> {
        let has_base_window = starts_window_base_name(self.parser.peek_kind());
        let build = WindowBuild {
            base_window: if has_base_window {
                Some(self.parser.parse_window_name()?)
            } else {
                None
            },
            partition_by: Vec::new(),
            order_by: Vec::new(),
        };
        if self.parser.eat_kind(&TokenKind::KwPartition) {
            self.parser.expect_kw(&TokenKind::KwBy)?;
            self.controls
                .push(ParseControl::WindowPartitionDone { build });
            self.controls.push(ParseControl::ExprStart { min_bp: 0 });
        } else {
            self.controls.push(ParseControl::WindowOrderStart { build });
        }
        Ok(())
    }

    fn window_frame_start(&mut self, build: WindowBuild) {
        let frame_type = if self.parser.eat_kind(&TokenKind::KwRows) {
            Some(FrameType::Rows)
        } else if self.parser.eat_kind(&TokenKind::KwRange) {
            Some(FrameType::Range)
        } else if self.parser.eat_kind(&TokenKind::KwGroups) {
            Some(FrameType::Groups)
        } else {
            None
        };
        let Some(frame_type) = frame_type else {
            self.values.push(MachineValue::Window(WindowSpec {
                window_ref: build.base_window.map(WindowReference::Base),
                partition_by: build.partition_by,
                order_by: build.order_by,
                frame: None,
            }));
            return;
        };
        let between = self.parser.eat_kind(&TokenKind::KwBetween);
        self.controls.push(ParseControl::WindowFirstBoundDone {
            build,
            frame_type,
            between,
        });
        self.controls.push(ParseControl::FrameBoundStart);
    }

    fn frame_bound_start(&mut self) -> Result<(), ParseError> {
        let origin = self
            .parser
            .peek_token()
            .cloned()
            .ok_or_else(|| self.parser.err_here("expected window frame bound"))?;
        if self.parser.eat_kind(&TokenKind::KwUnbounded) {
            let bound = if self.parser.eat_kind(&TokenKind::KwPreceding) {
                FrameBound::UnboundedPreceding
            } else {
                self.parser.expect_kw(&TokenKind::KwFollowing)?;
                FrameBound::UnboundedFollowing
            };
            self.values.push(MachineValue::FrameBound(ParsedFrameBound {
                value: bound,
                origin,
            }));
        } else if matches!(
            self.parser.peek_kind(),
            TokenKind::Id(value) if value.eq_ignore_ascii_case("CURRENT")
        ) {
            self.parser.advance_token();
            self.parser.expect_kw(&TokenKind::KwRow)?;
            self.values.push(MachineValue::FrameBound(ParsedFrameBound {
                value: FrameBound::CurrentRow,
                origin,
            }));
        } else {
            self.controls
                .push(ParseControl::FrameBoundExprDone { origin });
            self.controls.push(ParseControl::ExprStart { min_bp: 0 });
        }
        Ok(())
    }

    fn finish_frame(
        &mut self,
        frame_type: FrameType,
        start: FrameBound,
        end: Option<FrameBound>,
    ) -> Result<FrameSpec, ParseError> {
        let exclude = if self.parser.eat_kind(&TokenKind::KwExclude) {
            if self.parser.eat_kind(&TokenKind::KwNo) {
                let others = self.parser.parse_identifier()?;
                if !others.eq_ignore_ascii_case("OTHERS") {
                    return Err(self.parser.err_here("expected OTHERS"));
                }
                Some(FrameExclude::NoOthers)
            } else if self.parser.eat_kind(&TokenKind::KwTies) {
                Some(FrameExclude::Ties)
            } else if self.parser.eat_kind(&TokenKind::KwGroup) {
                Some(FrameExclude::Group)
            } else if matches!(
                self.parser.peek_kind(),
                TokenKind::Id(value) if value.eq_ignore_ascii_case("CURRENT")
            ) {
                self.parser.advance_token();
                self.parser.expect_kw(&TokenKind::KwRow)?;
                Some(FrameExclude::CurrentRow)
            } else {
                return Err(self
                    .parser
                    .err_here("expected NO OTHERS, TIES, GROUP, or CURRENT ROW after EXCLUDE"));
            }
        } else {
            None
        };
        Ok(FrameSpec {
            frame_type,
            start,
            end,
            exclude,
        })
    }

    fn step_select(&mut self, control: ParseControl) -> Result<(), ParseError> {
        match control {
            ParseControl::SelectStart { with } => {
                self.controls
                    .push(ParseControl::SelectFirstCoreDone { with });
                self.controls.push(ParseControl::CoreStart);
                Ok(())
            }
            ParseControl::SelectFirstCoreDone { with } => {
                let core = self.pop_core()?;
                self.continue_select_body(SelectBuild {
                    with,
                    height: core.height,
                    first: core.value,
                    compounds: Vec::new(),
                    order_by: Vec::new(),
                });
                Ok(())
            }
            ParseControl::SelectCompoundDone { mut build, op } => {
                let core = self.pop_core()?;
                build.height = build.height.max(core.height);
                build.compounds.push((op, core.value));
                self.continue_select_body(build);
                Ok(())
            }
            ParseControl::SelectOrderStart { build } => {
                let final_core = build
                    .compounds
                    .last()
                    .map_or(&build.first, |(_, core)| core);
                if matches!(final_core, SelectCore::Values(_))
                    && matches!(
                        self.parser.peek_kind(),
                        TokenKind::KwOrder | TokenKind::KwLimit
                    )
                {
                    return Err(self
                        .parser
                        .err_here("ORDER BY / LIMIT clause is not allowed after a VALUES term"));
                }
                if self.parser.eat_kind(&TokenKind::KwOrder) {
                    self.parser.expect_kw(&TokenKind::KwBy)?;
                    self.controls.push(ParseControl::SelectOrderDone { build });
                    self.controls.push(ParseControl::OrderingStart);
                } else {
                    self.start_select_limit(build)?;
                }
                Ok(())
            }
            ParseControl::SelectOrderDone { mut build } => {
                let term = self.pop_ordering()?;
                build.height = build.height.max(term.height);
                build.order_by.push(term.value);
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::SelectOrderDone { build });
                    self.controls.push(ParseControl::OrderingStart);
                } else {
                    self.start_select_limit(build)?;
                }
                Ok(())
            }
            ParseControl::SelectLimitFirstDone { build } => {
                let first = self.pop_expr()?;
                if self.parser.eat_kind(&TokenKind::KwOffset) {
                    self.controls.push(ParseControl::SelectLimitSecondDone {
                        build,
                        first,
                        comma_form: false,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                } else if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::SelectLimitSecondDone {
                        build,
                        first,
                        comma_form: true,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                } else {
                    let height = self.parser.checked_cached_parent_height(first.height)?;
                    self.finish_select(
                        build,
                        HeightTracked {
                            value: Some(LimitClause {
                                limit: first.expr,
                                offset: None,
                            }),
                            height,
                        },
                    )?;
                }
                Ok(())
            }
            ParseControl::SelectLimitSecondDone {
                build,
                first,
                comma_form,
            } => {
                let second = self.pop_expr()?;
                let height = self
                    .parser
                    .checked_cached_parent_height(first.height.max(second.height))?;
                let limit = if comma_form {
                    LimitClause {
                        limit: second.expr,
                        offset: Some(first.expr),
                    }
                } else {
                    LimitClause {
                        limit: first.expr,
                        offset: Some(second.expr),
                    }
                };
                self.finish_select(
                    build,
                    HeightTracked {
                        value: Some(limit),
                        height,
                    },
                )
            }
            ParseControl::CoreStart => self.core_start(),
            ParseControl::CoreColumnStart { build } => self.core_column_start(build),
            ParseControl::CoreColumnDone { mut build } => {
                let expr = self.pop_expr()?;
                build.height = build.height.max(expr.height);
                build.columns.push(ResultColumn::Expr {
                    expr: expr.expr,
                    alias: self.parser.try_result_alias()?,
                });
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::CoreColumnStart { build });
                } else {
                    self.controls.push(ParseControl::CoreAfterColumns { build });
                }
                Ok(())
            }
            ParseControl::CoreAfterColumns { build } => {
                if self.parser.eat_kind(&TokenKind::KwFrom) {
                    self.controls.push(ParseControl::CoreFromDone { build });
                    self.controls.push(ParseControl::FromStart);
                } else {
                    self.continue_core_where(build)?;
                }
                Ok(())
            }
            ParseControl::CoreFromDone { mut build } => {
                build.from = Some(self.pop_from()?);
                self.continue_core_where(build)
            }
            ParseControl::CoreWhereDone { mut build } => {
                let expr = self.pop_expr()?;
                build.height = build.height.max(expr.height);
                build.where_clause = Some(Box::new(expr.expr));
                self.continue_core_group(build)
            }
            ParseControl::CoreGroupDone { mut build } => {
                let expr = self.pop_expr()?;
                build.height = build.height.max(expr.height);
                build.group_by.push(expr.expr);
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::CoreGroupDone { build });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                } else {
                    self.continue_core_having(build);
                }
                Ok(())
            }
            ParseControl::CoreHavingDone { mut build } => {
                let expr = self.pop_expr()?;
                build.height = build.height.max(expr.height);
                build.having = Some(Box::new(expr.expr));
                self.continue_core_windows(build);
                Ok(())
            }
            ParseControl::CoreWindowStart { build } => {
                let name = self.parser.parse_window_name()?;
                self.parser.expect_kw(&TokenKind::KwAs)?;
                self.parser.expect_token(&TokenKind::LeftParen)?;
                self.controls
                    .push(ParseControl::CoreWindowDone { build, name });
                self.controls.push(ParseControl::WindowStart);
                Ok(())
            }
            ParseControl::CoreWindowDone { mut build, name } => {
                let spec = self.pop_window()?;
                self.parser.expect_token(&TokenKind::RightParen)?;
                build.windows.push(WindowDef { name, spec });
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::CoreWindowStart { build });
                } else {
                    self.finish_core(build);
                }
                Ok(())
            }
            ParseControl::ValuesRowStart {
                rows,
                height,
                force_union_all_from,
            } => {
                self.parser.expect_token(&TokenKind::LeftParen)?;
                self.controls.push(ParseControl::ValuesItemDone {
                    rows,
                    row: Vec::new(),
                    height,
                    force_union_all_from,
                });
                self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                Ok(())
            }
            ParseControl::ValuesItemDone {
                mut rows,
                mut row,
                mut height,
                mut force_union_all_from,
            } => {
                let expr = self.pop_expr()?;
                height = height.max(expr.height);
                row.push(expr.expr);
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::ValuesItemDone {
                        rows,
                        row,
                        height,
                        force_union_all_from,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                } else {
                    self.parser.expect_token(&TokenKind::RightParen)?;
                    if force_union_all_from.is_none() && self.parser.has_with {
                        force_union_all_from = Some(rows.len());
                    }
                    rows.push(row);
                    if self.parser.eat_kind(&TokenKind::Comma) {
                        self.controls.push(ParseControl::ValuesRowStart {
                            rows,
                            height,
                            force_union_all_from,
                        });
                    } else {
                        self.values.push(MachineValue::Core(HeightTracked {
                            value: SelectCore::Values(ValuesClause::parsed(
                                rows,
                                force_union_all_from,
                            )),
                            height,
                        }));
                    }
                }
                Ok(())
            }
            ParseControl::FromStart => {
                self.controls.push(ParseControl::FromSourceDone);
                self.controls.push(ParseControl::TableStart);
                Ok(())
            }
            ParseControl::FromSourceDone => {
                let source = self.pop_table()?;
                self.continue_from(FromBuild {
                    source,
                    joins: Vec::new(),
                })
            }
            ParseControl::FromTableDone { build, join_type } => {
                let table = self.pop_table()?;
                if self.parser.eat_kind(&TokenKind::KwOn) {
                    self.controls.push(ParseControl::FromJoinConstraintDone {
                        build,
                        join_type,
                        table,
                    });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                    return Ok(());
                }
                let constraint = if self.parser.eat_kind(&TokenKind::KwUsing) {
                    self.parser.expect_token(&TokenKind::LeftParen)?;
                    let mut columns = vec![self.parser.parse_identifier()?];
                    while self.parser.eat_kind(&TokenKind::Comma) {
                        columns.push(self.parser.parse_identifier()?);
                    }
                    self.parser.expect_token(&TokenKind::RightParen)?;
                    Some(JoinConstraint::Using(columns))
                } else {
                    None
                };
                self.append_join(build, join_type, table, constraint)
            }
            ParseControl::FromJoinConstraintDone {
                build,
                join_type,
                table,
            } => {
                let expr = self.pop_expr()?;
                self.append_join(build, join_type, table, Some(JoinConstraint::On(expr.expr)))
            }
            ParseControl::TableStart => self.table_start(),
            ParseControl::TableSubqueryDone => {
                let select = self.pop_select()?;
                self.parser.expect_token(&TokenKind::RightParen)?;
                self.values
                    .push(MachineValue::Table(TableOrSubquery::Subquery {
                        query: Box::new(select.value),
                        alias: self.parser.try_table_alias()?,
                    }));
                Ok(())
            }
            ParseControl::TableParenJoinDone => {
                let from = self.pop_from()?;
                self.parser.expect_token(&TokenKind::RightParen)?;
                self.values
                    .push(MachineValue::Table(TableOrSubquery::ParenJoin(Box::new(
                        from,
                    ))));
                Ok(())
            }
            ParseControl::TableFunctionArgDone { name, mut args } => {
                args.push(self.pop_expr()?.expr);
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls
                        .push(ParseControl::TableFunctionArgDone { name, args });
                    self.controls.push(ParseControl::ExprStart { min_bp: 0 });
                } else {
                    self.parser.expect_token(&TokenKind::RightParen)?;
                    self.values
                        .push(MachineValue::Table(TableOrSubquery::TableFunction {
                            name,
                            args,
                            alias: self.parser.try_table_alias()?,
                        }));
                }
                Ok(())
            }
            ParseControl::WithStart => self.with_start(),
            ParseControl::CteQueryDone {
                recursive,
                mut ctes,
                name,
                columns,
                materialized,
            } => {
                let query = self.pop_select()?;
                self.parser.expect_token(&TokenKind::RightParen)?;
                ctes.push(Cte {
                    name,
                    columns,
                    materialized,
                    query: query.value,
                });
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.start_cte(recursive, ctes)?;
                } else {
                    // SQLite treats `RECURSIVE` as optional; a self-referencing
                    // CTE is recursive regardless of the keyword.
                    let mut with = WithClause { recursive, ctes };
                    with.normalize_recursive();
                    self.values.push(MachineValue::With(with));
                }
                Ok(())
            }
            _ => Err(self
                .parser
                .err_here("internal expression parser control reached SELECT dispatcher")),
        }
    }

    fn continue_select_body(&mut self, build: SelectBuild) {
        let op = if self.parser.eat_kind(&TokenKind::KwUnion) {
            Some(if self.parser.eat_kind(&TokenKind::KwAll) {
                CompoundOp::UnionAll
            } else {
                CompoundOp::Union
            })
        } else if self.parser.eat_kind(&TokenKind::KwIntersect) {
            Some(CompoundOp::Intersect)
        } else if self.parser.eat_kind(&TokenKind::KwExcept) {
            Some(CompoundOp::Except)
        } else {
            None
        };
        if let Some(op) = op {
            self.controls
                .push(ParseControl::SelectCompoundDone { build, op });
            self.controls.push(ParseControl::CoreStart);
        } else {
            self.controls.push(ParseControl::SelectOrderStart { build });
        }
    }

    fn start_select_limit(&mut self, build: SelectBuild) -> Result<(), ParseError> {
        if self.parser.eat_kind(&TokenKind::KwLimit) {
            self.controls
                .push(ParseControl::SelectLimitFirstDone { build });
            self.controls.push(ParseControl::ExprStart { min_bp: 0 });
        } else {
            self.finish_select(
                build,
                HeightTracked {
                    value: None,
                    height: 0,
                },
            )?;
        }
        Ok(())
    }

    fn finish_select(
        &mut self,
        mut build: SelectBuild,
        limit: HeightTracked<Option<LimitClause>>,
    ) -> Result<(), ParseError> {
        build.height = build.height.max(limit.height);
        let final_core = build
            .compounds
            .last()
            .map_or(&build.first, |(_, core)| core);
        if matches!(final_core, SelectCore::Values(_))
            && (!build.order_by.is_empty() || limit.value.is_some())
        {
            return Err(self
                .parser
                .err_here("ORDER BY / LIMIT clause is not allowed after a VALUES term"));
        }
        self.values.push(MachineValue::Select(HeightTracked {
            value: SelectStatement {
                with: build.with,
                body: SelectBody {
                    select: build.first,
                    compounds: build.compounds,
                },
                order_by: build.order_by,
                limit: limit.value,
            },
            height: build.height,
        }));
        Ok(())
    }

    fn core_start(&mut self) -> Result<(), ParseError> {
        if self.parser.eat_kind(&TokenKind::KwValues) {
            self.controls.push(ParseControl::ValuesRowStart {
                rows: Vec::new(),
                height: 0,
                force_union_all_from: None,
            });
            return Ok(());
        }
        self.parser.expect_kw(&TokenKind::KwSelect)?;
        let distinct = if self.parser.eat_kind(&TokenKind::KwDistinct) {
            Distinctness::Distinct
        } else {
            let _ = self.parser.eat_kind(&TokenKind::KwAll);
            Distinctness::All
        };
        self.controls.push(ParseControl::CoreColumnStart {
            build: CoreBuild {
                distinct,
                columns: Vec::new(),
                height: 0,
                from: None,
                where_clause: None,
                group_by: Vec::new(),
                having: None,
                windows: Vec::new(),
            },
        });
        Ok(())
    }

    fn core_column_start(&mut self, mut build: CoreBuild) -> Result<(), ParseError> {
        if self.parser.eat_kind(&TokenKind::Star) {
            build.columns.push(ResultColumn::Star);
            if self.parser.eat_kind(&TokenKind::Comma) {
                self.controls.push(ParseControl::CoreColumnStart { build });
            } else {
                self.controls.push(ParseControl::CoreAfterColumns { build });
            }
            return Ok(());
        }
        if starts_table_star_qualifier(self.parser.peek_kind())
            && self
                .parser
                .tokens
                .get(self.parser.pos + 1)
                .is_some_and(|token| token.kind == TokenKind::Dot)
        {
            let table_star = self
                .parser
                .tokens
                .get(self.parser.pos + 2)
                .is_some_and(|token| token.kind == TokenKind::Star);
            let schema_table_star = self
                .parser
                .tokens
                .get(self.parser.pos + 2)
                .is_some_and(|token| starts_table_star_qualifier(&token.kind))
                && self
                    .parser
                    .tokens
                    .get(self.parser.pos + 3)
                    .is_some_and(|token| token.kind == TokenKind::Dot)
                && self
                    .parser
                    .tokens
                    .get(self.parser.pos + 4)
                    .is_some_and(|token| token.kind == TokenKind::Star);
            if table_star || schema_table_star {
                let first = self.parser.parse_table_star_qualifier()?;
                self.parser.expect_token(&TokenKind::Dot)?;
                let name = if schema_table_star {
                    let second = self.parser.parse_table_star_qualifier()?;
                    self.parser.expect_token(&TokenKind::Dot)?;
                    QualifiedName::qualified(first, second)
                } else {
                    QualifiedName::bare(first)
                };
                self.parser.expect_token(&TokenKind::Star)?;
                build.columns.push(ResultColumn::TableStar(name));
                if self.parser.eat_kind(&TokenKind::Comma) {
                    self.controls.push(ParseControl::CoreColumnStart { build });
                } else {
                    self.controls.push(ParseControl::CoreAfterColumns { build });
                }
                return Ok(());
            }
        }
        self.controls.push(ParseControl::CoreColumnDone { build });
        self.controls.push(ParseControl::ExprStart { min_bp: 0 });
        Ok(())
    }

    fn continue_core_where(&mut self, build: CoreBuild) -> Result<(), ParseError> {
        if self.parser.eat_kind(&TokenKind::KwWhere) {
            self.controls.push(ParseControl::CoreWhereDone { build });
            self.controls.push(ParseControl::ExprStart { min_bp: 0 });
        } else {
            self.continue_core_group(build)?;
        }
        Ok(())
    }

    fn continue_core_group(&mut self, build: CoreBuild) -> Result<(), ParseError> {
        if self.parser.eat_kind(&TokenKind::KwGroup) {
            self.parser.expect_kw(&TokenKind::KwBy)?;
            self.controls.push(ParseControl::CoreGroupDone { build });
            self.controls.push(ParseControl::ExprStart { min_bp: 0 });
        } else {
            self.continue_core_having(build);
        }
        Ok(())
    }

    fn continue_core_having(&mut self, build: CoreBuild) {
        if self.parser.eat_kind(&TokenKind::KwHaving) {
            self.controls.push(ParseControl::CoreHavingDone { build });
            self.controls.push(ParseControl::ExprStart { min_bp: 0 });
        } else {
            self.continue_core_windows(build);
        }
    }

    fn continue_core_windows(&mut self, build: CoreBuild) {
        if self.parser.eat_kind(&TokenKind::KwWindow) {
            self.controls.push(ParseControl::CoreWindowStart { build });
        } else {
            self.finish_core(build);
        }
    }

    fn finish_core(&mut self, build: CoreBuild) {
        self.values.push(MachineValue::Core(HeightTracked {
            value: SelectCore::Select {
                distinct: build.distinct,
                columns: build.columns,
                from: build.from,
                where_clause: build.where_clause,
                group_by: build.group_by,
                having: build.having,
                windows: build.windows,
            },
            height: build.height,
        }));
    }

    fn continue_from(&mut self, build: FromBuild) -> Result<(), ParseError> {
        let join_type = if let Some(join_type) = self.parser.try_join_type()? {
            Some(join_type)
        } else if self.parser.eat_kind(&TokenKind::Comma) {
            Some(JoinType {
                natural: false,
                kind: JoinKind::Cross,
            })
        } else {
            None
        };
        if let Some(join_type) = join_type {
            self.controls
                .push(ParseControl::FromTableDone { build, join_type });
            self.controls.push(ParseControl::TableStart);
        } else {
            self.values.push(MachineValue::From(FromClause {
                source: build.source,
                joins: build.joins,
            }));
        }
        Ok(())
    }

    fn append_join(
        &mut self,
        mut build: FromBuild,
        join_type: JoinType,
        table: TableOrSubquery,
        constraint: Option<JoinConstraint>,
    ) -> Result<(), ParseError> {
        if join_type.natural && constraint.is_some() {
            // Stock SQLite emits this fixed message verbatim (Part C), not the
            // generic near-X form. bd-parser-syntax-error-format-6w6kp.
            return Err(self
                .parser
                .err_semantic("a NATURAL join may not have an ON or USING clause"));
        }
        build.joins.push(JoinClause {
            join_type,
            table,
            constraint,
        });
        self.continue_from(build)
    }

    fn table_start(&mut self) -> Result<(), ParseError> {
        if self.parser.eat_kind(&TokenKind::LeftParen) {
            if matches!(
                self.parser.peek_kind(),
                TokenKind::KwSelect | TokenKind::KwWith | TokenKind::KwValues
            ) {
                self.controls.push(ParseControl::TableSubqueryDone);
                self.controls.push(ParseControl::SubqueryStart);
            } else {
                self.controls.push(ParseControl::TableParenJoinDone);
                self.controls.push(ParseControl::FromStart);
            }
            return Ok(());
        }
        let name = self.parser.parse_qualified_name()?;
        if name.schema.is_none() && self.parser.eat_kind(&TokenKind::LeftParen) {
            if self.parser.eat_kind(&TokenKind::RightParen) {
                self.values
                    .push(MachineValue::Table(TableOrSubquery::TableFunction {
                        name: name.name,
                        args: Vec::new(),
                        alias: self.parser.try_table_alias()?,
                    }));
            } else {
                self.controls.push(ParseControl::TableFunctionArgDone {
                    name: name.name,
                    args: Vec::new(),
                });
                self.controls.push(ParseControl::ExprStart { min_bp: 0 });
            }
            return Ok(());
        }
        self.values
            .push(MachineValue::Table(TableOrSubquery::Table {
                name,
                alias: self.parser.try_table_alias()?,
                index_hint: self.parser.parse_index_hint()?,
                time_travel: self.parser.parse_time_travel_clause()?,
            }));
        Ok(())
    }

    fn with_start(&mut self) -> Result<(), ParseError> {
        self.parser.expect_kw(&TokenKind::KwWith)?;
        self.parser.has_with = true;
        let recursive = self.parser.eat_kind(&TokenKind::KwRecursive);
        self.start_cte(recursive, Vec::new())
    }

    fn start_cte(&mut self, recursive: bool, ctes: Vec<Cte>) -> Result<(), ParseError> {
        let name = self.parser.parse_identifier()?;
        let columns = if self.parser.eat_kind(&TokenKind::LeftParen) {
            let mut columns = vec![self.parser.parse_identifier()?];
            while self.parser.eat_kind(&TokenKind::Comma) {
                columns.push(self.parser.parse_identifier()?);
            }
            self.parser.expect_token(&TokenKind::RightParen)?;
            columns
        } else {
            Vec::new()
        };
        self.parser.expect_kw(&TokenKind::KwAs)?;
        let materialized = if self.parser.eat_kind(&TokenKind::KwNot) {
            self.parser.expect_kw(&TokenKind::KwMaterialized)?;
            Some(CteMaterialized::NotMaterialized)
        } else if self.parser.eat_kind(&TokenKind::KwMaterialized) {
            Some(CteMaterialized::Materialized)
        } else {
            None
        };
        self.parser.expect_token(&TokenKind::LeftParen)?;
        self.controls.push(ParseControl::CteQueryDone {
            recursive,
            ctes,
            name,
            columns,
            materialized,
        });
        self.controls.push(ParseControl::SubqueryStart);
        Ok(())
    }
}

impl Parser {
    /// Parse a single SQL expression.
    pub fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_expr_tracked().map(|parsed| parsed.expr)
    }

    pub(crate) fn parse_expr_tracked(&mut self) -> Result<ParsedExpr, ParseError> {
        ParseMachine::for_expr(self).run_expr()
    }

    pub(crate) fn parse_select_tracked_machine(
        &mut self,
        with: Option<WithClause>,
    ) -> Result<HeightTracked<SelectStatement>, ParseError> {
        ParseMachine::for_select(self, with).run_select()
    }

    pub(crate) fn parse_with_clause_machine(&mut self) -> Result<WithClause, ParseError> {
        ParseMachine::for_with(self).run_with()
    }

    pub(crate) fn parse_from_clause_machine(&mut self) -> Result<FromClause, ParseError> {
        ParseMachine::for_from(self).run_from()
    }

    fn finish_expr(
        &self,
        expr: Expr,
        height: u32,
        is_constant: bool,
        has_function: bool,
    ) -> Result<ParsedExpr, ParseError> {
        if height > MAX_PARSE_DEPTH {
            return Err(ParseError::expression_too_deep(
                MAX_PARSE_DEPTH,
                self.peek_token(),
            ));
        }
        let root = match &expr {
            Expr::UnaryOp {
                op: UnaryOp::Plus, ..
            } => CachedRoot::UnaryPlus,
            Expr::RowValue(..) => CachedRoot::Vector,
            Expr::Subquery(..) => CachedRoot::ScalarSubquery,
            _ => CachedRoot::Other,
        };
        Ok(ParsedExpr {
            expr,
            height,
            is_constant,
            has_function,
            root,
        })
    }

    fn checked_expr(
        &self,
        expr: Expr,
        max_child_height: u32,
        is_constant: bool,
        has_function: bool,
    ) -> Result<ParsedExpr, ParseError> {
        let height = max_child_height.saturating_add(1);
        self.finish_expr(expr, height, is_constant, has_function)
    }

    fn add_cached_parent(&self, mut parsed: ParsedExpr) -> Result<ParsedExpr, ParseError> {
        parsed.height = parsed.height.saturating_add(1);
        if parsed.height > MAX_PARSE_DEPTH {
            return Err(ParseError::expression_too_deep(
                MAX_PARSE_DEPTH,
                self.peek_token(),
            ));
        }
        parsed.root = CachedRoot::Other;
        Ok(parsed)
    }

    fn finish_unary(
        &self,
        op: UnaryOp,
        mut inner: ParsedExpr,
        span: Span,
    ) -> Result<ParsedExpr, ParseError> {
        if matches!(op, UnaryOp::Plus | UnaryOp::Negate)
            && inner.root == CachedRoot::UnaryPlus
            && let Expr::UnaryOp {
                op: inner_op,
                span: inner_span,
                ..
            } = &mut inner.expr
        {
            *inner_op = op;
            *inner_span = span;
            inner.root = if op == UnaryOp::Plus {
                CachedRoot::UnaryPlus
            } else {
                CachedRoot::Other
            };
            return Ok(inner);
        }

        let height = inner.height;
        let is_constant = inner.is_constant;
        let has_function = inner.has_function;
        self.checked_expr(
            Expr::UnaryOp {
                op,
                expr: Box::new(inner.expr),
                span,
            },
            height,
            is_constant,
            has_function,
        )
    }

    // ── Pratt core ──────────────────────────────────────────────────────

    #[cfg(test)]
    fn parse_expr_bp(&mut self, min_bp: u8) -> Result<ParsedExpr, ParseError> {
        self.with_recursion_guard(|p| p.parse_expr_bp_inner(min_bp))
    }

    #[cfg(test)]
    fn parse_expr_bp_inner(&mut self, min_bp: u8) -> Result<ParsedExpr, ParseError> {
        let prefixes = self.collect_prefix_frames();
        let mut lhs = self.parse_prefix()?;

        for prefix in prefixes.into_iter().rev() {
            match prefix {
                DeepExprFrame::Unary { op, span, right_bp } => {
                    lhs = self.parse_expr_tail(lhs, right_bp)?;
                    let span = span.merge(lhs.expr.span());
                    lhs = self.finish_unary(op, lhs, span)?;
                }
                DeepExprFrame::Parenthesis { span } => {
                    lhs = self.finish_parenthesized_frame(lhs, span)?;
                }
            }
        }

        self.parse_expr_tail(lhs, min_bp)
    }

    #[cfg(test)]
    fn parse_expr_tail(
        &mut self,
        mut lhs: ParsedExpr,
        min_bp: u8,
    ) -> Result<ParsedExpr, ParseError> {
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

    #[cfg(test)]
    fn collect_prefix_frames(&mut self) -> Vec<DeepExprFrame> {
        let mut prefixes = Vec::new();
        loop {
            let unary = match self.peek_kind() {
                TokenKind::Minus => {
                    let folds_i64_min = matches!(
                        self.tokens.get(self.pos + 1).map(|token| &token.kind),
                        Some(TokenKind::OversizedInt(value)) if value == "9223372036854775808"
                    );
                    if folds_i64_min {
                        break;
                    }
                    Some((UnaryOp::Negate, bp::UNARY))
                }
                TokenKind::Plus => Some((UnaryOp::Plus, bp::UNARY)),
                TokenKind::Tilde => Some((UnaryOp::BitNot, bp::UNARY)),
                TokenKind::KwNot => {
                    if matches!(
                        self.tokens.get(self.pos + 1).map(|token| &token.kind),
                        Some(TokenKind::KwExists)
                    ) {
                        break;
                    }
                    Some((UnaryOp::Not, bp::NOT_PREFIX))
                }
                TokenKind::LeftParen => {
                    let starts_subquery = matches!(
                        self.tokens.get(self.pos + 1).map(|token| &token.kind),
                        Some(TokenKind::KwSelect | TokenKind::KwWith | TokenKind::KwValues)
                    );
                    if starts_subquery {
                        break;
                    }
                    let token = self.advance_token();
                    prefixes.push(DeepExprFrame::Parenthesis { span: token.span });
                    continue;
                }
                _ => break,
            };
            let Some((op, right_bp)) = unary else {
                break;
            };
            let token = self.advance_token();
            prefixes.push(DeepExprFrame::Unary {
                op,
                span: token.span,
                right_bp,
            });
        }
        prefixes
    }

    #[cfg(test)]
    fn finish_parenthesized_frame(
        &mut self,
        mut first: ParsedExpr,
        start: Span,
    ) -> Result<ParsedExpr, ParseError> {
        first = self.parse_expr_tail(first, 0)?;
        if self.eat_kind(&TokenKind::Comma) {
            let mut is_constant = first.is_constant;
            let mut has_function = first.has_function;
            let mut exprs = vec![first.expr];
            loop {
                let parsed = self.parse_expr_bp(0)?;
                is_constant &= parsed.is_constant;
                has_function |= parsed.has_function;
                exprs.push(parsed.expr);
                if !self.eat_kind(&TokenKind::Comma) {
                    break;
                }
            }
            let end = self.expect_kind(&TokenKind::RightParen)?;
            return self.finish_expr(
                Expr::RowValue(exprs, start.merge(end)),
                1,
                is_constant,
                has_function,
            );
        }
        self.expect_kind(&TokenKind::RightParen)?;
        Ok(first)
    }

    // ── Token helpers ───────────────────────────────────────────────────

    fn peek_kind(&self) -> &TokenKind {
        self.tokens
            .get(self.pos)
            .map_or(&TokenKind::Eof, |t| &t.kind)
    }

    fn peek_token(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    #[cfg(test)]
    fn peek_nth_token(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset)
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
        // Every expression-grammar "expected/unexpected" failure is reported by
        // stock SQLite as `near "<offending-token>": syntax error` (or
        // `incomplete input` at EOF). Tag UnexpectedToken so the connection
        // boundary renders it that way, keyed on the offending token's span.
        // bd-parser-syntax-error-format-6w6kp (Part B).
        let mut error = ParseError::at(message, self.peek_token());
        error.kind = crate::parser::ParseErrorKind::UnexpectedToken;
        error
    }

    // ── Prefix (nud) ────────────────────────────────────────────────────

    #[cfg(test)]
    #[allow(clippy::too_many_lines)]
    fn parse_prefix(&mut self) -> Result<ParsedExpr, ParseError> {
        let Token {
            kind,
            span: token_span,
            line,
            col,
        } = self.advance_token();
        if self.at_kind(&TokenKind::Dot) && starts_table_star_qualifier(&kind) {
            let name = match &kind {
                TokenKind::Id(name) | TokenKind::QuotedId(name, _) => Arc::clone(name),
                TokenKind::String(name) => Arc::<str>::from(name.as_str()),
                keyword => Arc::<str>::from(kw_to_str(keyword)),
            };
            return self.parse_ident_expr(name, token_span);
        }
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
                    kind: crate::parser::ParseErrorKind::Syntax,
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
            TokenKind::String(s) if matches!(self.peek_kind(), TokenKind::Dot) => {
                self.parse_ident_expr(s, token_span)
            }
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
                // SQLite accepts this one magnitude as the signed minimum.
                // Preserve the normalized one-node literal AST and its actual
                // retained height.
                if let TokenKind::OversizedInt(s) = self.peek_kind()
                    && s == "9223372036854775808"
                {
                    let num_span = self.advance_token().span;
                    let span = token_span.merge(num_span);
                    return self.finish_expr(
                        Expr::Literal(Literal::Integer(i64::MIN), span),
                        1,
                        true,
                        false,
                    );
                }
                let inner = self.parse_expr_bp(bp::UNARY)?;
                let span = token_span.merge(inner.expr.span());
                self.finish_unary(UnaryOp::Negate, inner, span)
            }
            TokenKind::Plus => {
                let inner = self.parse_expr_bp(bp::UNARY)?;
                let span = token_span.merge(inner.expr.span());
                self.finish_unary(UnaryOp::Plus, inner, span)
            }
            TokenKind::Tilde => {
                let inner = self.parse_expr_bp(bp::UNARY)?;
                let span = token_span.merge(inner.expr.span());
                self.finish_unary(UnaryOp::BitNot, inner, span)
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
                    let height = subquery.height;
                    let exists = self.checked_expr(
                        Expr::Exists {
                            subquery: Box::new(subquery.value),
                            not: true,
                            span,
                        },
                        height,
                        false,
                        false,
                    )?;
                    return self.add_cached_parent(exists);
                }
                let inner = self.parse_expr_bp(bp::NOT_PREFIX)?;
                let span = token_span.merge(inner.expr.span());
                self.finish_unary(UnaryOp::Not, inner, span)
            }

            // ── EXISTS (subquery) ───────────────────────────────────────
            TokenKind::KwExists => {
                self.expect_kind(&TokenKind::LeftParen)?;
                let subquery = self.parse_subquery_minimal()?;
                let end = self.expect_kind(&TokenKind::RightParen)?;
                let span = token_span.merge(end);
                let height = subquery.height;
                self.checked_expr(
                    Expr::Exists {
                        subquery: Box::new(subquery.value),
                        not: false,
                        span,
                    },
                    height,
                    false,
                    false,
                )
            }

            // ── CAST(expr AS type_name) ─────────────────────────────────
            TokenKind::KwCast => {
                self.expect_kind(&TokenKind::LeftParen)?;
                let inner = self.parse_expr_bp(0)?;
                self.expect_kind(&TokenKind::KwAs)?;
                let type_name = self.parse_type_name()?;
                let end = self.expect_kind(&TokenKind::RightParen)?;
                let span = token_span.merge(end);
                let height = inner.height;
                let is_constant = inner.is_constant;
                let has_function = inner.has_function;
                self.checked_expr(
                    Expr::Cast {
                        expr: Box::new(inner.expr),
                        type_name,
                        span,
                    },
                    height,
                    is_constant,
                    has_function,
                )
            }

            // ── CASE [operand] WHEN ... THEN ... [ELSE ...] END ────────
            TokenKind::KwCase => self.parse_case_expr(token_span),

            // ── RAISE(action, message) ──────────────────────────────────
            TokenKind::KwRaise => {
                self.expect_kind(&TokenKind::LeftParen)?;
                let (action, message) = self.parse_raise_args()?;
                let end = self.expect_kind(&TokenKind::RightParen)?;
                let span = token_span.merge(end);
                Ok(ParsedExpr::leaf(Expr::Raise {
                    action,
                    message,
                    span,
                }))
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
                    return self.checked_expr(
                        Expr::Subquery(Box::new(subquery.value), span),
                        subquery.height,
                        false,
                        false,
                    );
                }
                let first = self.parse_expr_bp(0)?;
                if self.eat_kind(&TokenKind::Comma) {
                    let mut is_constant = first.is_constant;
                    let mut has_function = first.has_function;
                    let mut exprs = vec![first.expr];
                    loop {
                        let parsed = self.parse_expr_bp(0)?;
                        is_constant &= parsed.is_constant;
                        has_function |= parsed.has_function;
                        exprs.push(parsed.expr);
                        if !self.eat_kind(&TokenKind::Comma) {
                            break;
                        }
                    }
                    let end = self.expect_kind(&TokenKind::RightParen)?;
                    let span = token_span.merge(end);
                    self.finish_expr(Expr::RowValue(exprs, span), 1, is_constant, has_function)
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

            TokenKind::Error(msg) => Err(ParseError {
                // Surface the tokenizer error verbatim (see the other arm).
                // bd-parser-syntax-error-format-6w6kp (Part A).
                kind: crate::parser::ParseErrorKind::Tokenizer,
                message: msg,
                span: token_span,
                line,
                col,
            }),
            kind => Err(ParseError {
                kind: crate::parser::ParseErrorKind::UnexpectedToken,
                message: format!("unexpected token in expression: {kind:?}"),
                span: token_span,
                line,
                col,
            }),
        }
    }

    /// Parse `name`, `name.column`, or `name(args)`.
    #[cfg(test)]
    fn parse_ident_expr<S>(&mut self, name: S, start: Span) -> Result<ParsedExpr, ParseError>
    where
        S: AsRef<str> + Into<Arc<str>>,
    {
        // Function call: name(...)
        if matches!(self.peek_kind(), TokenKind::LeftParen) {
            return self.parse_function_call(name.as_ref().to_owned(), start);
        }
        let name = name.into();
        // Table-qualified column (`name.column`) or three-part
        // schema-qualified column (`name.mid.column`).
        if matches!(self.peek_kind(), TokenKind::Dot) {
            let Some(mid_tok) = self.peek_nth_token(1).cloned() else {
                return Err(self.err_here("expected column name after '.'"));
            };
            let mid_name = match &mid_tok.kind {
                TokenKind::Id(c) | TokenKind::QuotedId(c, _) => Arc::clone(c),
                TokenKind::String(c) => Arc::<str>::from(c.as_str()),
                k if starts_post_dot_identifier(k) => Arc::<str>::from(kw_to_str(k)),
                _ => {
                    return Err(ParseError::at(
                        format!("expected column name after '.', got {:?}", mid_tok.kind),
                        Some(&mid_tok),
                    ));
                }
            };
            // Three-part reference `schema.table.column` (e.g. `main.t.id`).
            if matches!(self.peek_nth_token(2).map(|t| &t.kind), Some(TokenKind::Dot)) {
                let Some(col_tok) = self.peek_nth_token(3).cloned() else {
                    return Err(self.err_here("expected column name after '.'"));
                };
                let col_name = match &col_tok.kind {
                    TokenKind::Id(c) | TokenKind::QuotedId(c, _) => Arc::clone(c),
                    TokenKind::String(c) => Arc::<str>::from(c.as_str()),
                    k if starts_post_dot_identifier(k) => Arc::<str>::from(kw_to_str(k)),
                    _ => {
                        return Err(ParseError::at(
                            format!("expected column name after '.', got {:?}", col_tok.kind),
                            Some(&col_tok),
                        ));
                    }
                };
                let span = start.merge(col_tok.span);
                self.pos = self.pos.saturating_add(4);
                return self.finish_expr(
                    Expr::Column(ColumnRef::schema_qualified(name, mid_name, col_name), span),
                    2,
                    false,
                    false,
                );
            }
            let span = start.merge(mid_tok.span);
            self.pos = self.pos.saturating_add(2);
            return self.finish_expr(
                Expr::Column(ColumnRef::qualified(name, mid_name), span),
                2,
                false,
                false,
            );
        }
        Ok(ParsedExpr::leaf(Expr::Column(ColumnRef::bare(name), start)))
    }

    // ── Postfix ─────────────────────────────────────────────────────────

    fn postfix_bp(&self) -> Option<u8> {
        match self.peek_kind() {
            TokenKind::KwCollate => Some(bp::COLLATE),
            TokenKind::KwIsnull | TokenKind::KwNotnull => Some(bp::EQUALITY.0),
            TokenKind::KwNot => {
                if let Some(next) = self.tokens.get(self.pos + 1)
                    && matches!(next.kind, TokenKind::KwNull)
                {
                    return Some(bp::EQUALITY.0);
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
                let span = lhs.expr.span().merge(name_span);
                let height = lhs.height;
                let is_constant = lhs.is_constant;
                let has_function = lhs.has_function;
                self.checked_expr(
                    Expr::Collate {
                        expr: Box::new(lhs.expr),
                        collation,
                        span,
                    },
                    height,
                    is_constant,
                    has_function,
                )
            }
            TokenKind::KwIsnull => {
                let span = lhs.expr.span().merge(tok.span);
                let height = lhs.height;
                let is_constant = lhs.is_constant;
                let has_function = lhs.has_function;
                self.checked_expr(
                    Expr::IsNull {
                        expr: Box::new(lhs.expr),
                        not: false,
                        span,
                    },
                    height,
                    is_constant,
                    has_function,
                )
            }
            TokenKind::KwNotnull => {
                let span = lhs.expr.span().merge(tok.span);
                let height = lhs.height;
                let is_constant = lhs.is_constant;
                let has_function = lhs.has_function;
                self.checked_expr(
                    Expr::IsNull {
                        expr: Box::new(lhs.expr),
                        not: true,
                        span,
                    },
                    height,
                    is_constant,
                    has_function,
                )
            }
            TokenKind::KwNot => {
                let null_tok = self.advance_token(); // we know from postfix_bp that this is KwNull
                let span = lhs.expr.span().merge(null_tok.span);
                let height = lhs.height;
                let is_constant = lhs.is_constant;
                let has_function = lhs.has_function;
                self.checked_expr(
                    Expr::IsNull {
                        expr: Box::new(lhs.expr),
                        not: true,
                        span,
                    },
                    height,
                    is_constant,
                    has_function,
                )
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

    #[cfg(test)]
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
                    let span = lhs.expr.span().merge(rhs.expr.span());
                    let height = lhs.height.max(rhs.height);
                    let is_constant = lhs.is_constant && rhs.is_constant;
                    let has_function = lhs.has_function || rhs.has_function;
                    // IS DISTINCT FROM is equivalent to IS NOT
                    // IS NOT DISTINCT FROM is equivalent to IS
                    let op = if not { BinaryOp::Is } else { BinaryOp::IsNot };
                    return self.checked_expr(
                        Expr::BinaryOp {
                            left: Box::new(lhs.expr),
                            op,
                            right: Box::new(rhs.expr),
                            span,
                        },
                        height,
                        is_constant,
                        has_function,
                    );
                }
                let rhs = self.parse_expr_bp(r_bp)?;
                let span = lhs.expr.span().merge(rhs.expr.span());
                // SQLite folds `expr IS [NOT] expr` into a unary null-test
                // only when the right operand, parsed at normal precedence,
                // is the NULL literal. Parsing the RHS first — rather than
                // greedily consuming a NULL token — keeps tighter-binding operators
                // attached to NULL: `x IS NULL < 2` parses as
                // `x IS (NULL < 2)`, matching C SQLite (verified against the
                // sqlite3 CLI: `SELECT 1 IS NULL < 2` yields 0, not 1).
                if matches!(&rhs.expr, Expr::Literal(Literal::Null, _)) {
                    let height = lhs.height;
                    let is_constant = lhs.is_constant;
                    let has_function = lhs.has_function;
                    return self.checked_expr(
                        Expr::IsNull {
                            expr: Box::new(lhs.expr),
                            not,
                            span,
                        },
                        height,
                        is_constant,
                        has_function,
                    );
                }
                let op = if not { BinaryOp::IsNot } else { BinaryOp::Is };
                let height = lhs.height.max(rhs.height);
                let is_constant = lhs.is_constant && rhs.is_constant;
                let has_function = lhs.has_function || rhs.has_function;
                self.checked_expr(
                    Expr::BinaryOp {
                        left: Box::new(lhs.expr),
                        op,
                        right: Box::new(rhs.expr),
                        span,
                    },
                    height,
                    is_constant,
                    has_function,
                )
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
                let span = lhs.expr.span().merge(rhs.expr.span());
                let height = lhs.height.max(rhs.height);
                let is_constant = false;
                let has_function = true;
                self.checked_expr(
                    Expr::JsonAccess {
                        expr: Box::new(lhs.expr),
                        path: Box::new(rhs.expr),
                        arrow: JsonArrow::Arrow,
                        span,
                    },
                    height,
                    is_constant,
                    has_function,
                )
            }
            TokenKind::DoubleArrow => {
                let rhs = self.parse_expr_bp(r_bp)?;
                let span = lhs.expr.span().merge(rhs.expr.span());
                let height = lhs.height.max(rhs.height);
                let is_constant = false;
                let has_function = true;
                self.checked_expr(
                    Expr::JsonAccess {
                        expr: Box::new(lhs.expr),
                        path: Box::new(rhs.expr),
                        arrow: JsonArrow::DoubleArrow,
                        span,
                    },
                    height,
                    is_constant,
                    has_function,
                )
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

    #[cfg(test)]
    fn make_binop(
        &mut self,
        lhs: ParsedExpr,
        op: BinaryOp,
        r_bp: u8,
    ) -> Result<ParsedExpr, ParseError> {
        let rhs = self.parse_expr_bp(r_bp)?;
        let span = lhs.expr.span().merge(rhs.expr.span());
        let height = lhs.height.max(rhs.height);
        let is_constant = lhs.is_constant && rhs.is_constant;
        let has_function = lhs.has_function || rhs.has_function;
        self.checked_expr(
            Expr::BinaryOp {
                left: Box::new(lhs.expr),
                op,
                right: Box::new(rhs.expr),
                span,
            },
            height,
            is_constant,
            has_function,
        )
    }

    // ── Special expression forms ────────────────────────────────────────

    #[cfg(test)]
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
        let end = escape
            .as_ref()
            .map_or_else(|| pattern.expr.span(), |e| e.expr.span());
        let span = lhs.expr.span().merge(end);
        let height = escape.as_ref().map_or_else(
            || lhs.height.max(pattern.height),
            |parsed| lhs.height.max(pattern.height).max(parsed.height),
        );
        let parsed = self.checked_expr(
            Expr::Like {
                expr: Box::new(lhs.expr),
                pattern: Box::new(pattern.expr),
                escape: escape.map(|parsed| Box::new(parsed.expr)),
                op,
                not,
                span,
            },
            height,
            false,
            true,
        )?;
        if not {
            self.add_cached_parent(parsed)
        } else {
            Ok(parsed)
        }
    }

    #[cfg(test)]
    fn parse_between(&mut self, lhs: ParsedExpr, not: bool) -> Result<ParsedExpr, ParseError> {
        // Parse low bound above AND level so AND keyword is not consumed.
        let low = self.parse_expr_bp(bp::NOT_PREFIX)?;
        if !self.eat_kind(&TokenKind::KwAnd) {
            return Err(self.err_here("expected AND in BETWEEN expression"));
        }
        let high = self.parse_expr_bp(bp::EQUALITY.1)?;
        let span = lhs.expr.span().merge(high.expr.span());
        let height = lhs.height.max(low.height).max(high.height);
        let is_constant = lhs.is_constant && low.is_constant && high.is_constant;
        let has_function = lhs.has_function || low.has_function || high.has_function;
        let parsed = self.checked_expr(
            Expr::Between {
                expr: Box::new(lhs.expr),
                low: Box::new(low.expr),
                high: Box::new(high.expr),
                not,
                span,
            },
            height,
            is_constant,
            has_function,
        )?;
        if not {
            self.add_cached_parent(parsed)
        } else {
            Ok(parsed)
        }
    }

    #[cfg(test)]
    fn parse_in(&mut self, lhs: ParsedExpr, not: bool) -> Result<ParsedExpr, ParseError> {
        let start = lhs.expr.span();

        // SQLite supports both "x IN ( ... )" and "x IN table_name".
        if !self.at_kind(&TokenKind::LeftParen) {
            let table = self.parse_qualified_name()?;
            let end = self.tokens[self.pos.saturating_sub(1)].span;
            let span = start.merge(end);
            let height = lhs.height;
            let has_function = lhs.has_function;
            let parsed = self.checked_expr(
                Expr::In {
                    expr: Box::new(lhs.expr),
                    set: InSet::Table(table),
                    not,
                    span,
                },
                height,
                false,
                has_function,
            )?;
            return if not {
                self.add_cached_parent(parsed)
            } else {
                Ok(parsed)
            };
        }

        self.expect_kind(&TokenKind::LeftParen)?;

        if matches!(
            self.peek_kind(),
            TokenKind::KwSelect | TokenKind::KwWith | TokenKind::KwValues
        ) {
            let subquery = self.parse_subquery_minimal()?;
            let end = self.expect_kind(&TokenKind::RightParen)?;
            let span = start.merge(end);
            let height = lhs.height.max(subquery.height);
            let has_function = lhs.has_function;
            let parsed = self.checked_expr(
                Expr::In {
                    expr: Box::new(lhs.expr),
                    set: InSet::Subquery(Box::new(subquery.value)),
                    not,
                    span,
                },
                height,
                false,
                has_function,
            )?;
            return if not {
                self.add_cached_parent(parsed)
            } else {
                Ok(parsed)
            };
        }

        let mut parsed_items = Vec::new();
        if !self.at_kind(&TokenKind::RightParen) {
            let item = self.parse_expr_bp(0)?;
            parsed_items.push(item);
            while self.eat_kind(&TokenKind::Comma) {
                let item = self.parse_expr_bp(0)?;
                parsed_items.push(item);
            }
        }
        let end = self.expect_kind(&TokenKind::RightParen)?;
        if let Some(message) = vector_in_list_arity_error(&lhs.expr, &parsed_items) {
            // Stock emits this row-value arity message VERBATIM (no offset
            // prefix, not a near-X form). bd-parser-syntax-error-format-6w6kp.
            return Err(self.err_semantic(message));
        }
        let span = start.merge(end);
        let item_height = parsed_items
            .iter()
            .map(|item| item.height)
            .max()
            .unwrap_or(0);
        let items_are_constant = parsed_items.iter().all(|item| item.is_constant);
        let item_has_function = parsed_items.iter().any(|item| item.has_function);
        let singleton_constant = matches!(parsed_items.as_slice(), [item] if item.is_constant)
            && lhs.root != CachedRoot::Vector;
        let singleton_subquery =
            matches!(parsed_items.as_slice(), [item] if item.root == CachedRoot::ScalarSubquery);
        let exprs = parsed_items.into_iter().map(|parsed| parsed.expr).collect();
        let lhs_height = lhs.height;
        let lhs_is_constant = lhs.is_constant;
        let lhs_has_function = lhs.has_function;
        let expr = Expr::In {
            expr: Box::new(lhs.expr),
            set: InSet::List(exprs),
            not,
            span,
        };

        if item_height == 0 {
            if lhs_has_function {
                return self.finish_expr(expr, lhs_height.saturating_add(1), false, true);
            }
            return self.finish_expr(expr, 1, true, false);
        }

        let cached_child_height = if singleton_constant {
            lhs_height.max(item_height.saturating_add(1))
        } else if singleton_subquery {
            lhs_height.max(item_height.saturating_sub(1))
        } else {
            lhs_height.max(item_height)
        };
        let parsed = self.checked_expr(
            expr,
            cached_child_height,
            lhs_is_constant && items_are_constant,
            lhs_has_function || item_has_function,
        )?;
        if not {
            self.add_cached_parent(parsed)
        } else {
            Ok(parsed)
        }
    }

    #[cfg(test)]
    fn parse_case_expr(&mut self, start: Span) -> Result<ParsedExpr, ParseError> {
        let operand = if matches!(self.peek_kind(), TokenKind::KwWhen) {
            None
        } else {
            Some(self.parse_expr_bp(0)?)
        };

        let mut whens = Vec::new();
        while self.eat_kind(&TokenKind::KwWhen) {
            let condition = self.parse_expr_bp(0)?;
            if !self.eat_kind(&TokenKind::KwThen) {
                return Err(self.err_here("expected THEN in CASE expression"));
            }
            let result = self.parse_expr_bp(0)?;
            whens.push((condition, result));
        }
        if whens.is_empty() {
            return Err(self.err_here("CASE requires at least one WHEN clause"));
        }

        let else_expr = if self.eat_kind(&TokenKind::KwElse) {
            Some(self.parse_expr_bp(0)?)
        } else {
            None
        };

        if !self.eat_kind(&TokenKind::KwEnd) {
            return Err(self.err_here("expected END for CASE expression"));
        }
        let end = self.tokens[self.pos.saturating_sub(1)].span;
        let span = start.merge(end);
        let mut height = operand.as_ref().map_or(0, |parsed| parsed.height);
        let mut is_constant = operand.as_ref().is_none_or(|parsed| parsed.is_constant);
        let mut has_function = operand.as_ref().is_some_and(|parsed| parsed.has_function);
        for (condition, result) in &whens {
            height = height.max(condition.height).max(result.height);
            is_constant &= condition.is_constant && result.is_constant;
            has_function |= condition.has_function || result.has_function;
        }
        if let Some(parsed) = &else_expr {
            height = height.max(parsed.height);
            is_constant &= parsed.is_constant;
            has_function |= parsed.has_function;
        }
        self.checked_expr(
            Expr::Case {
                operand: operand.map(|parsed| Box::new(parsed.expr)),
                whens: whens
                    .into_iter()
                    .map(|(condition, result)| (condition.expr, result.expr))
                    .collect(),
                else_expr: else_expr.map(|parsed| Box::new(parsed.expr)),
                span,
            },
            height,
            is_constant,
            has_function,
        )
    }

    #[cfg(test)]
    fn parse_function_call(&mut self, name: String, start: Span) -> Result<ParsedExpr, ParseError> {
        self.expect_kind(&TokenKind::LeftParen)?;

        let (args, distinct, height) = if matches!(self.peek_kind(), TokenKind::Star) {
            self.advance_token();
            // Mirrors start_function: `f(*)` parses for any function; only
            // count keeps star semantics, others become zero-arg calls.
            if name.eq_ignore_ascii_case("count") {
                (FunctionArgs::Star, false, 0)
            } else {
                (FunctionArgs::List(Vec::new()), false, 0)
            }
        } else {
            let distinct = self.eat_kind(&TokenKind::KwDistinct);
            let (args, height) = if matches!(self.peek_kind(), TokenKind::RightParen) {
                if distinct {
                    return Err(self.err_here("DISTINCT requires at least one argument"));
                }
                (FunctionArgs::List(Vec::new()), 0)
            } else {
                let first = self.parse_expr_bp(0)?;
                let mut height = first.height;
                let mut list = vec![first.expr];
                while self.eat_kind(&TokenKind::Comma) {
                    let parsed = self.parse_expr_bp(0)?;
                    height = height.max(parsed.height);
                    list.push(parsed.expr);
                }
                (FunctionArgs::List(list), height)
            };
            (args, distinct, height)
        };

        // In-aggregate ORDER BY (SQLite 3.44+): group_concat(x, ',' ORDER BY y DESC)
        let order_by =
            if matches!(&args, FunctionArgs::List(_)) && self.eat_kind(&TokenKind::KwOrder) {
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
            let predicate = self.parse_expr()?;
            let filter_end = self.expect_kind(&TokenKind::RightParen)?;
            end = end.merge(filter_end);
            Some(Box::new(predicate))
        } else {
            None
        };
        // Peek: only consume OVER if followed by '(' or an identifier
        // (window name), to avoid swallowing OVER as a column alias.
        let over = if matches!(self.peek_kind(), TokenKind::KwOver)
            && self.tokens.get(self.pos + 1).is_some_and(|t| {
                matches!(t.kind, TokenKind::LeftParen) || starts_bare_window_name(&t.kind)
            }) {
            self.advance_token(); // consume OVER
            if self.eat_kind(&TokenKind::LeftParen) {
                let spec = self.parse_window_spec()?;
                let over_end = self.expect_kind(&TokenKind::RightParen)?;
                end = end.merge(over_end);
                Some(spec)
            } else {
                let base_window = self.parse_window_name()?;
                let base_span = self.tokens[self.pos.saturating_sub(1)].span;
                end = end.merge(base_span);
                Some(WindowSpec {
                    window_ref: Some(WindowReference::Direct(base_window)),
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    frame: None,
                })
            }
        } else {
            None
        };

        let span = start.merge(end);
        self.checked_expr(
            Expr::FunctionCall {
                name,
                args,
                distinct,
                order_by,
                filter,
                over,
                span,
            },
            height,
            false,
            true,
        )
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
    #[cfg(test)]
    fn parse_subquery_minimal(&mut self) -> Result<HeightTracked<SelectStatement>, ParseError> {
        let with = if self.at_kind(&TokenKind::KwWith) {
            Some(ParseMachine::for_with(self).run_with()?)
        } else {
            None
        };
        ParseMachine::for_select(self, with).run_select()
    }
}

/// Parse a single expression from raw SQL text.
pub fn parse_expr(sql: &str) -> Result<Expr, ParseError> {
    let mut parser = Parser::from_sql(sql);
    let expr = parser.parse_expr()?;
    let _ = parser.eat(&TokenKind::Semicolon);
    if !parser.at_eof() {
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
    use crate::parser::ParseErrorKind;
    use fsqlite_ast::{BoundCollation, SelectCore, TableOrSubquery};
    use fsqlite_types::{SqliteValue, TypeAffinity};

    fn parse(sql: &str) -> Expr {
        match parse_expr(sql) {
            Ok(expr) => expr,
            Err(err) => unreachable!("parse error for `{sql}`: {err}"),
        }
    }

    fn repeated_infix_expression(term_count: usize, operator: &str) -> String {
        std::iter::repeat_n("1", term_count)
            .collect::<Vec<_>>()
            .join(operator)
    }

    fn assert_expression_depth_error(sql: &str) {
        let error = parse_expr(sql).expect_err("expression height 1001 must fail closed");
        assert_eq!(
            error.kind,
            ParseErrorKind::ExpressionTooDeep {
                max: MAX_PARSE_DEPTH
            }
        );
        assert_eq!(
            error.message,
            format!(
                "Expression tree is too large (maximum depth {})",
                MAX_PARSE_DEPTH
            )
        );
        assert!(error.is_expression_too_deep());
    }

    fn right_deep_binary_expression(height: usize) -> String {
        format!("{}1{}", "1 + (".repeat(height - 1), ")".repeat(height - 1))
    }

    fn single_arg_function_expression(height: usize) -> String {
        format!("{}1{}", "abs(".repeat(height - 1), ")".repeat(height - 1))
    }

    fn scalar_subquery_expression(height: usize) -> String {
        format!(
            "{}1{}",
            "(SELECT ".repeat(height - 1),
            ")".repeat(height - 1)
        )
    }

    fn on_one_mib_stack<T: Send + 'static>(task: impl FnOnce() -> T + Send + 'static) -> T {
        std::thread::Builder::new()
            .stack_size(1024 * 1024)
            .spawn(task)
            .expect("1 MiB parser thread must spawn")
            .join()
            .expect("parser task must not overflow or panic")
    }

    fn parsed_select_height(sql: &str) -> u32 {
        let mut parser = Parser::from_sql(sql);
        let select = parser
            .parse_subquery_minimal()
            .expect("SELECT fixture must parse");
        assert_eq!(
            select.height,
            normalized_ast_select_height(&select.value),
            "tracked SELECT height diverged from the normalized AST test oracle"
        );
        select.height
    }

    fn parsed_expr_height(sql: &str) -> u32 {
        let mut parser = Parser::from_sql(sql);
        let parsed = parser
            .parse_expr_tracked()
            .expect("expression fixture must parse");
        assert!(
            matches!(parser.peek_kind(), TokenKind::Eof | TokenKind::Semicolon),
            "expression fixture left an unparsed token: {sql}"
        );
        assert_eq!(
            parsed.height,
            normalized_ast_expr_height(&parsed.expr),
            "tracked expression height diverged from the normalized AST test oracle: {sql}"
        );
        parsed.height
    }

    #[test]
    fn bound_outer_value_has_constant_leaf_facts() {
        let expr = Expr::BoundOuterValue {
            value: SqliteValue::Integer(42),
            collation: BoundCollation::Named("NOCASE".to_owned()),
            affinity: Some(TypeAffinity::Integer),
            span: Span::ZERO,
        };

        let parsed = ParsedExpr::leaf(expr.clone());
        assert_eq!(parsed.height, 1);
        assert!(parsed.is_constant);
        assert!(!parsed.has_function);

        let facts = cached_facts_from_tasks(vec![CachedHeightTask::Expr(&expr)]);
        assert_eq!(facts.height, 1);
        assert!(facts.is_constant);
        assert!(!facts.has_function);
    }

    fn mixed_deep_expression(height: usize) -> String {
        let mut prefix = String::new();
        let mut closing_count = 0;
        for index in 1..height {
            if index % 7 == 0 {
                prefix.push('(');
                closing_count += 1;
            }
            match index % 4 {
                0 => prefix.push('~'),
                1 => {
                    prefix.push_str("abs(");
                    closing_count += 1;
                }
                2 => {
                    prefix.push_str("(SELECT ");
                    closing_count += 1;
                }
                _ => {
                    prefix.push_str("1 + (");
                    closing_count += 1;
                }
            }
        }
        prefix.push('1');
        prefix.push_str(&")".repeat(closing_count));
        prefix
    }

    fn assert_machine_matches_recursive_oracle(sql: &str) {
        let mut machine = Parser::from_sql(sql);
        let machine_result = machine.parse_expr_tracked();
        let machine_pos = machine.pos;
        let machine_tail = machine.peek_kind().clone();

        let mut oracle = Parser::from_sql(sql);
        let oracle_result = oracle.parse_expr_bp(0);
        let oracle_pos = oracle.pos;
        let oracle_tail = oracle.peek_kind().clone();

        assert_eq!(
            machine_pos, oracle_pos,
            "parser tail position differs: {sql}"
        );
        assert_eq!(
            machine_tail, oracle_tail,
            "parser tail token differs: {sql}"
        );
        match (machine_result, oracle_result) {
            (Ok(machine), Ok(oracle)) => {
                assert_eq!(machine.expr, oracle.expr, "AST or spans differ: {sql}");
                assert_eq!(machine.height, oracle.height, "height differs: {sql}");
                assert_eq!(
                    machine.is_constant, oracle.is_constant,
                    "constant fact differs: {sql}"
                );
                assert_eq!(
                    machine.has_function, oracle.has_function,
                    "function fact differs: {sql}"
                );
                assert_eq!(machine.root, oracle.root, "root fact differs: {sql}");
            }
            (Err(machine), Err(oracle)) => {
                assert_eq!(machine, oracle, "diagnostic differs: {sql}");
            }
            (Ok(_), Err(_)) | (Err(_), Ok(_)) => {
                panic!("machine/oracle result class differs for `{sql}`");
            }
        }
    }

    fn assert_select_machine_matches_recursive_oracle(sql: &str) {
        let mut machine = Parser::from_sql(sql);
        let machine_result = machine.parse_select_stmt_tracked(None);
        let machine_pos = machine.pos;
        let machine_tail = machine.peek_kind().clone();

        let mut oracle = Parser::from_sql(sql);
        let oracle_result = oracle.parse_select_stmt_inner_tracked(None);
        let oracle_pos = oracle.pos;
        let oracle_tail = oracle.peek_kind().clone();

        assert_eq!(
            machine_pos, oracle_pos,
            "SELECT parser tail position differs: {sql}"
        );
        assert_eq!(
            machine_tail, oracle_tail,
            "SELECT parser tail token differs: {sql}"
        );
        match (machine_result, oracle_result) {
            (Ok(machine), Ok(oracle)) => {
                assert_eq!(machine.value, oracle.value, "SELECT AST differs: {sql}");
                assert_eq!(
                    machine.height, oracle.height,
                    "SELECT height differs: {sql}"
                );
            }
            (Err(machine), Err(oracle)) => {
                assert_eq!(machine, oracle, "SELECT diagnostic differs: {sql}");
            }
            (Ok(_), Err(_)) | (Err(_), Ok(_)) => {
                panic!("SELECT machine/oracle result class differs for `{sql}`");
            }
        }
    }

    fn bitnot_depth(mut expr: &Expr) -> usize {
        let mut depth = 0;
        while let Expr::UnaryOp {
            op: UnaryOp::BitNot,
            expr: inner,
            ..
        } = expr
        {
            depth += 1;
            expr = inner;
        }
        depth
    }

    fn wrap_function_to_height(base: &str, target_height: usize) -> String {
        let base_height = parsed_expr_height(base) as usize;
        assert!(base_height <= target_height);
        let wrappers = target_height - base_height;
        format!("{}{base}{}", "abs(".repeat(wrappers), ")".repeat(wrappers))
    }

    #[test]
    fn test_explicit_machine_matches_recursive_oracle_for_shallow_valid_expressions() {
        for sql in [
            "1",
            "-9223372036854775808",
            "a.b + c * 2",
            "'a'.b + a.'b'",
            "attach.x",
            "filter.x",
            "true.x",
            "with.x",
            "a.current_date",
            "NOT a = b",
            "x IS NULL < 2",
            "x IS NOT DISTINCT FROM y",
            "CAST(x + 1 AS DECIMAL(10, 2))",
            "CASE x WHEN 1 THEN y ELSE z END",
            "x NOT BETWEEN 1 AND 2",
            "x IN (1, 2 + 3)",
            "(SELECT 1, 2) IN ((1, 2))",
            "(a, b) IN ((SELECT 1))",
            "(a, b) IN ((SELECT 1, 2, 3))",
            "(a, b) IN ((SELECT 1), (SELECT 2))",
            "(SELECT * FROM t) IN (1)",
            "x IN (SELECT y FROM t WHERE z > 0 ORDER BY y LIMIT 1)",
            "EXISTS (SELECT 1 FROM t WHERE x = y)",
            "(1, 2 + 3)",
            "value COLLATE \"my col\"",
            "doc -> '$.x' || suffix",
            "sum(DISTINCT x ORDER BY y DESC) FILTER (WHERE z > 0) OVER (PARTITION BY p ORDER BY q ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        ] {
            assert_machine_matches_recursive_oracle(sql);
        }
    }

    #[test]
    fn test_explicit_machine_matches_recursive_oracle_for_shallow_malformed_expressions() {
        for sql in [
            "+",
            "CASE END",
            "CASE WHEN 1 2 END",
            "CASE WHEN 1 THEN 2",
            "CAST(1 INTEGER)",
            "f(DISTINCT)",
            "(1,)",
            "a.",
            "a.select",
            "a.nothing",
            "cast.x",
            "current_date.x",
            "raise.x",
            "transaction.x",
            "a BETWEEN 1 2",
            "a IN (1,)",
            "(a, b) IN (1)",
            "(a, b) IN (+(SELECT 1, 2))",
            "(a, b) IN ((1, 2), 3)",
            "(a, b) NOT IN ((1, 2, 3))",
            "(SELECT 1, 2) IN (1)",
            "(SELECT 1, 2) IN ((1, 2), 3)",
            "(SELECT 1, 2) NOT IN ((1, 2, 3))",
            "a LIKE",
            "EXISTS (SELECT)",
            "count(* ORDER BY x)",
            "t.*",
        ] {
            assert_machine_matches_recursive_oracle(sql);
        }
    }

    #[test]
    fn public_parse_expr_requires_eof_after_one_optional_terminator() {
        assert_eq!(
            parse_expr("1;")
                .expect("one trailing expression terminator must remain valid")
                .to_string(),
            "1"
        );

        for (sql, unexpected) in [("1; 2", "2"), ("1; SELECT 2", "SELECT"), ("1;;", ";")] {
            let error = parse_expr(sql)
                .expect_err("tokens after the optional expression terminator must be rejected");
            // Stock renders a stray token after an expression as a near-X syntax
            // error. bd-parser-syntax-error-format-6w6kp (Part B).
            assert_eq!(error.kind, ParseErrorKind::UnexpectedToken);
            assert!(
                error.message.contains("unexpected token after expression"),
                "unexpected diagnostic for `{sql}`: {error:?}"
            );
            assert_eq!(
                &sql[error.span.start as usize..error.span.end as usize],
                unexpected,
                "the diagnostic for `{sql}` must point at the first forbidden token"
            );
        }
    }

    #[test]
    fn test_explicit_select_machine_matches_recursive_shallow_oracle() {
        for sql in [
            "SELECT 1",
            "SELECT DISTINCT t.x AS y, count(*) FROM t INNER JOIN u ON t.id = u.id WHERE t.x > 0 GROUP BY t.x HAVING count(*) > 1 WINDOW w AS (PARTITION BY t.p ORDER BY t.q ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY t.x DESC NULLS LAST LIMIT 5 OFFSET 1",
            "SELECT * FROM a CROSS JOIN b ON a.id = b.id",
            "SELECT * FROM a, b",
            "SELECT * FROM a, b ON a.id = b.id",
            "SELECT * FROM a, b USING(id)",
            "SELECT filter.* FROM t AS filter",
            "SELECT attach.* FROM t AS \"attach\"",
            "SELECT attach.x FROM (SELECT 1 AS x) AS \"attach\"",
            "SELECT filter.x FROM (SELECT 1 AS x) AS \"filter\"",
            "SELECT 't'.*, t.'select' FROM t",
            "SELECT 1 'single quoted'",
            "SELECT 1 attach",
            "SELECT 1 window",
            "SELECT * FROM (SELECT 1) 'single quoted'",
            "SELECT * FROM (SELECT 1) match",
            "SELECT sum(1) OVER attach WINDOW attach AS ()",
            "SELECT sum(1) OVER (attach) WINDOW attach AS ()",
            "SELECT sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) FROM t",
            "SELECT sum(x) OVER (RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t",
            "VALUES (1, 2), (3, 4)",
            "VALUES (1) ORDER BY 1",
            "VALUES (1) LIMIT 1",
            "VALUES (1) UNION SELECT 2 ORDER BY 1 LIMIT 1",
            "SELECT 1 UNION ALL SELECT 2 INTERSECT SELECT 3",
            "SELECT 1 UNION VALUES (2), (3) ORDER BY 1",
            "SELECT FROM t",
            "SELECT nothing.* FROM t AS \"nothing\"",
            "SELECT sum(1) OVER filter",
            "SELECT sum(x) OVER (ROWS 1 FOLLOWING) FROM t",
            "SELECT sum(x) OVER (ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM t",
            "VALUES 1",
        ] {
            assert_select_machine_matches_recursive_oracle(sql);
        }
    }

    #[test]
    fn test_threshold_unary_precedence_associativity_and_spans_are_stable() {
        for unary_count in [63_u32, 64] {
            let sql = format!("{}1 + (1)", "~".repeat(unary_count as usize));
            let parsed = parse_expr(&sql).expect("threshold unary-plus expression must parse");
            assert_eq!(
                parsed.span(),
                Span::new(0, unary_count + 6),
                "grouping delimiters must not change the established root span"
            );
            let Expr::BinaryOp {
                left,
                op: BinaryOp::Add,
                right,
                ..
            } = &parsed
            else {
                panic!("unary prefix must not capture the lower-precedence addition");
            };
            assert_eq!(bitnot_depth(left), unary_count as usize);
            assert!(matches!(
                right.as_ref(),
                Expr::Literal(Literal::Integer(1), _)
            ));

            let subtraction = format!("{}10 - 3 - 2", "~".repeat(unary_count as usize));
            let subtraction =
                parse_expr(&subtraction).expect("threshold subtraction expression must parse");
            assert!(matches!(
                subtraction,
                Expr::BinaryOp {
                    op: BinaryOp::Subtract,
                    left,
                    ..
                } if matches!(
                    left.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Subtract,
                        ..
                    }
                )
            ));

            let precedence = format!("{}1 + 2 * 3", "~".repeat(unary_count as usize));
            let precedence =
                parse_expr(&precedence).expect("threshold precedence expression must parse");
            assert!(matches!(
                precedence,
                Expr::BinaryOp {
                    op: BinaryOp::Add,
                    right,
                    ..
                } if matches!(
                    right.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOp::Multiply,
                        ..
                    }
                )
            ));

            let parenthesized = format!(
                "{}1{}",
                "(".repeat(unary_count as usize),
                ")".repeat(unary_count as usize)
            );
            let parenthesized =
                parse_expr(&parenthesized).expect("threshold parentheses must parse");
            assert_eq!(
                parenthesized.span(),
                Span::new(unary_count, unary_count + 1)
            );
        }
    }

    #[test]
    fn test_shallow_machine_uses_only_inline_control_and_value_storage() {
        PARSE_MACHINE_STACK_SPILLS.set(0);
        let expr = parse_expr("a + b * 2").expect("shallow expression must parse");
        assert_eq!(expr.to_string(), "a + b * 2");
        assert_eq!(
            PARSE_MACHINE_STACK_SPILLS.get(),
            0,
            "representative shallow parsing must not allocate parser stack spill storage"
        );
    }

    #[test]
    fn test_formatter_precedence_associativity_and_migration_scale_stability() {
        for (sql, expected) in [
            ("a + b * 2", "a + b * 2"),
            ("a * (b + c)", "a * (b + c)"),
            ("(a - b) - c", "a - b - c"),
            ("a - (b - c)", "a - (b - c)"),
            ("a / (b / c)", "a / (b / c)"),
        ] {
            let expr = parse_expr(sql).expect("precedence fixture must parse");
            let rendered = expr.to_string();
            assert_eq!(rendered, expected);
            let reparsed = parse_expr(&rendered).expect("formatted fixture must reparse");
            assert_eq!(reparsed.to_string(), rendered);
        }

        const TERM_COUNT: usize = MAX_PARSE_DEPTH as usize;
        for operator in ["AND", "OR"] {
            let mut sql = String::new();
            for _ in 1..TERM_COUNT {
                sql.push_str("flag ");
                sql.push_str(operator);
                sql.push_str(" (");
            }
            sql.push_str("flag");
            sql.push_str(&")".repeat(TERM_COUNT - 1));

            let expr = parse_expr(&sql).expect("migration-scale associative chain must parse");
            let rendered = expr.to_string();
            assert_eq!(rendered.matches(operator).count(), TERM_COUNT - 1);
            assert!(
                !rendered.contains('('),
                "associative {operator} chain must have a flat canonical form"
            );
            let reparsed = parse_expr(&rendered).expect("flat migration-scale chain must reparse");
            assert_eq!(
                reparsed.to_string(),
                rendered,
                "format-parse-format must be byte-stable for {operator}"
            );
        }
    }

    #[test]
    fn test_expression_height_exact_1000_1001_flat_infix_boundary() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = repeated_infix_expression(LIMIT, " + ");
        let expr = parse_expr(&at_limit).expect("1000-term left-associated tree has height 1000");
        let rendered = expr.to_string();
        parse_expr(&rendered).expect("formatted height-1000 expression must remain parseable");

        let over_limit = repeated_infix_expression(LIMIT + 1, " + ");
        assert_expression_depth_error(&over_limit);
    }

    #[test]
    fn test_expression_height_exact_1000_1001_unary_boundary() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = format!("{}1", "~".repeat(LIMIT - 1));
        parse_expr(&at_limit).expect("999 unary nodes plus one leaf have height 1000");

        let over_limit = format!("{}1", "~".repeat(LIMIT));
        assert_expression_depth_error(&over_limit);
    }

    #[test]
    fn test_expression_height_exact_1000_1001_not_boundary() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = format!("{}1", "NOT ".repeat(LIMIT - 1));
        parse_expr(&at_limit).expect("999 NOT nodes plus one leaf have height 1000");

        let over_limit = format!("{}1", "NOT ".repeat(LIMIT));
        assert_expression_depth_error(&over_limit);
    }

    #[test]
    fn test_expression_parenthesis_chain_is_stack_safe() {
        const PAREN_PAIRS: usize = MAX_PARSE_DEPTH as usize * 2;
        let deeply_parenthesized =
            format!("{}1{}", "(".repeat(PAREN_PAIRS), ")".repeat(PAREN_PAIRS));
        parse_expr(&deeply_parenthesized)
            .expect("parentheses do not add AST height or consume the native call stack");
    }

    #[test]
    fn test_expression_prefix_frames_preserve_grouping_and_row_values() {
        let grouped = parse_expr("-((1 + 2)) * 3").expect("grouped unary expression must parse");
        assert!(matches!(
            grouped,
            Expr::BinaryOp {
                left,
                op: BinaryOp::Multiply,
                ..
            } if matches!(
                left.as_ref(),
                Expr::UnaryOp {
                    op: UnaryOp::Negate,
                    ..
                }
            )
        ));

        let row = parse_expr("((1), 2)").expect("nested row value must parse");
        assert!(matches!(row, Expr::RowValue(values, _) if values.len() == 2));
    }

    #[test]
    fn test_expression_iterative_prefix_frames_preserve_minimum_integer_literal() {
        let mut parser = Parser::from_sql("-9223372036854775808");
        let parsed = parser
            .parse_expr_tracked()
            .expect("minimum signed integer literal must parse");
        assert!(matches!(
            &parsed.expr,
            Expr::Literal(Literal::Integer(i64::MIN), _)
        ));
        assert_eq!(parsed.height, 1);
        assert_eq!(parsed.expr.to_string(), "-9223372036854775808");
    }

    #[test]
    fn test_serializer_regression_double_negated_minimum_integer_round_trips() {
        let expr = parse_expr("- -9223372036854775808")
            .expect("double-negated minimum integer must parse");
        let rendered = expr.to_string();
        assert_eq!(rendered, "-(-9223372036854775808)");
        let reparsed = parse_expr(&rendered).expect("rendered double negation must remain SQL");
        assert_eq!(expr, reparsed);
    }

    #[test]
    fn test_serializer_regression_quoted_collation_name_round_trips() {
        let expr =
            parse_expr("value COLLATE \"my col\"").expect("quoted collation name must parse");
        let rendered = expr.to_string();
        assert_eq!(rendered, "value COLLATE \"my col\"");
        let reparsed = parse_expr(&rendered).expect("rendered collation must remain SQL");
        assert_eq!(expr, reparsed);
    }

    #[test]
    fn test_expression_height_mixed_prefix_and_flat_reductions() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        const PREFIX_COUNT: usize = 499;
        let at_limit = format!(
            "{}{}",
            "~".repeat(PREFIX_COUNT),
            repeated_infix_expression(LIMIT - PREFIX_COUNT, " - ")
        );
        parse_expr(&at_limit)
            .expect("mixed unary and left-associated reductions have exact height 1000");

        let over_limit = format!(
            "{}{}",
            "~".repeat(PREFIX_COUNT),
            repeated_infix_expression(LIMIT - PREFIX_COUNT + 1, " - ")
        );
        assert_expression_depth_error(&over_limit);
    }

    #[test]
    fn test_expression_height_container_adds_one_level() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = format!("abs({})", repeated_infix_expression(LIMIT - 1, " + "));
        parse_expr(&at_limit).expect("function node over height-999 argument has height 1000");

        let over_limit = format!("abs({})", repeated_infix_expression(LIMIT, " + "));
        assert_expression_depth_error(&over_limit);
    }

    #[test]
    fn test_right_deep_binary_exact_1000_1001_boundary_on_one_mib_stack() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = right_deep_binary_expression(LIMIT);
        let expr = on_one_mib_stack(move || {
            parse_expr(&at_limit).expect("right-deep height-1000 expression must parse")
        });
        assert_expression_depth_error(&right_deep_binary_expression(LIMIT + 1));
        drop(expr);
    }

    #[test]
    fn test_single_arg_function_exact_1000_1001_boundary_on_one_mib_stack() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = single_arg_function_expression(LIMIT);
        let expr = on_one_mib_stack(move || {
            parse_expr(&at_limit).expect("nested function height-1000 expression must parse")
        });
        assert_expression_depth_error(&single_arg_function_expression(LIMIT + 1));
        drop(expr);
    }

    #[test]
    fn test_scalar_subquery_exact_1000_1001_boundary_on_one_mib_stack() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = scalar_subquery_expression(LIMIT);
        let rendered_selects = on_one_mib_stack(move || {
            let expr =
                parse_expr(&at_limit).expect("scalar subquery height-1000 expression must parse");
            let rendered = expr.to_string();
            let select_count = rendered.matches("(SELECT ").count();
            drop(rendered);
            drop(expr);
            select_count
        });
        assert_eq!(rendered_selects, LIMIT - 1);
        assert_expression_depth_error(&scalar_subquery_expression(LIMIT + 1));
    }

    #[test]
    fn test_signed_minimum_and_qualified_bases_round_trip_on_one_mib_stack() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        for (base, canonical_base) in [
            ("-9223372036854775808", "-9223372036854775808"),
            ("schema.column", "schema.\"column\""),
        ] {
            let at_limit = wrap_function_to_height(base, LIMIT);
            let (height, rendered) = on_one_mib_stack(move || {
                let expr = parse_expr(&at_limit).expect("height-1000 base expression must parse");
                let height = normalized_ast_expr_height(&expr);
                let rendered = expr.to_string();
                let reparsed =
                    parse_expr(&rendered).expect("formatted height-1000 base must reparse");
                assert_eq!(normalized_ast_expr_height(&reparsed), MAX_PARSE_DEPTH);
                assert_eq!(rendered, reparsed.to_string());
                drop(reparsed);
                drop(expr);
                (height, rendered)
            });
            assert_eq!(height, MAX_PARSE_DEPTH);
            assert!(rendered.contains(canonical_base));

            let over_limit = wrap_function_to_height(base, LIMIT + 1);
            let error = on_one_mib_stack(move || {
                parse_expr(&over_limit).expect_err("height-1001 base must fail closed")
            });
            assert_eq!(
                error.kind,
                ParseErrorKind::ExpressionTooDeep {
                    max: MAX_PARSE_DEPTH
                }
            );
        }
    }

    #[test]
    fn test_mixed_deep_height_1000_parse_walk_format_and_drop_on_one_mib_stack() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = mixed_deep_expression(LIMIT);
        let (height, rendered_len, walk_visits) = on_one_mib_stack(move || {
            let expr = parse_expr(&at_limit)
                .expect("mixed unary/function/subquery/binary height-1000 expression must parse");
            HEIGHT_WALK_VISITS.set(0);
            let height = normalized_ast_expr_height(&expr);
            let walk_visits = HEIGHT_WALK_VISITS.get();
            let rendered = expr.to_string();
            let rendered_len = rendered.len();
            drop(rendered);
            drop(expr);
            (height, rendered_len, walk_visits)
        });
        assert_eq!(height, MAX_PARSE_DEPTH);
        assert!(rendered_len > LIMIT);
        assert!(
            walk_visits <= at_limit_token_bound(LIMIT),
            "heap-backed cached-height walk must remain linear: {walk_visits} visits"
        );

        let over_limit = mixed_deep_expression(LIMIT + 1);
        let error = on_one_mib_stack(move || {
            parse_expr(&over_limit).expect_err("mixed height-1001 expression must fail closed")
        });
        assert_eq!(
            error.kind,
            ParseErrorKind::ExpressionTooDeep {
                max: MAX_PARSE_DEPTH
            }
        );
    }

    #[test]
    fn test_mixed_select_shape_round_trips_and_drops_on_one_mib_stack_at_exact_limit() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let base = "CASE WHEN 5 BETWEEN 1 AND 9 THEN 3 IN (WITH c AS (SELECT x FROM t) SELECT sum(c.x) OVER (PARTITION BY u.p ORDER BY u.q ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM c INNER JOIN u ON c.x = u.x WHERE u.x > 0 GROUP BY c.x HAVING count(*) > 0 ORDER BY c.x LIMIT 1) ELSE 0 END";
        let at_limit = wrap_function_to_height(base, LIMIT);
        let (height, rendered_len) = on_one_mib_stack(move || {
            let expr = parse_expr(&at_limit)
                .expect("mixed CASE/BETWEEN/IN/CTE/JOIN/window height-1000 expression must parse");
            assert_eq!(normalized_ast_expr_height(&expr), MAX_PARSE_DEPTH);
            let rendered = expr.to_string();
            let reparsed =
                parse_expr(&rendered).expect("formatted mixed height-1000 expression must reparse");
            assert_eq!(normalized_ast_expr_height(&reparsed), MAX_PARSE_DEPTH);
            let rerendered = reparsed.to_string();
            assert_eq!(rendered, rerendered);
            let rendered_len = rendered.len();
            drop(rerendered);
            drop(reparsed);
            drop(rendered);
            drop(expr);
            (MAX_PARSE_DEPTH, rendered_len)
        });
        assert_eq!(height, MAX_PARSE_DEPTH);
        assert!(rendered_len > LIMIT);

        let over_limit = wrap_function_to_height(base, LIMIT + 1);
        let error = on_one_mib_stack(move || {
            parse_expr(&over_limit).expect_err("mixed height-1001 expression must fail closed")
        });
        assert_eq!(
            error.kind,
            ParseErrorKind::ExpressionTooDeep {
                max: MAX_PARSE_DEPTH
            }
        );
        assert_eq!(
            error.message,
            format!(
                "Expression tree is too large (maximum depth {})",
                MAX_PARSE_DEPTH
            )
        );
    }

    const fn at_limit_token_bound(height: usize) -> usize {
        height * 12
    }

    #[test]
    fn test_998_wrapper_aggregate_filter_machine_steps_are_linear() {
        const WRAPPERS: usize = 998;
        let sql = format!(
            "{}1{} FILTER (WHERE 1)",
            "abs(".repeat(WRAPPERS),
            ")".repeat(WRAPPERS)
        );
        let token_count = Parser::from_sql(&sql).tokens.len();
        PARSE_MACHINE_STEPS.set(0);
        let expr = parse_expr(&sql).expect("near-match aggregate FILTER expression must parse");
        let visits = PARSE_MACHINE_STEPS.get();
        assert!(
            visits <= token_count.saturating_mul(8),
            "explicit parser machine revisited tokens superlinearly: {visits} steps for {token_count} tokens"
        );
        drop(expr);
    }

    #[test]
    fn test_aggregate_order_by_auxiliary_roots_do_not_rescan_descendants() {
        let mut sql = "1".to_owned();
        for _ in 0..64 {
            sql = format!("f(0 ORDER BY {sql})");
        }

        HEIGHT_WALK_VISITS.set(0);
        parse_expr(&sql).expect("nested aggregate ORDER BY expression must parse");
        assert_eq!(
            HEIGHT_WALK_VISITS.get(),
            0,
            "independent aggregate ORDER BY roots must not walk completed ASTs"
        );
    }

    #[test]
    fn test_subquery_height_contract_matches_sqlite_height_of_select_fields() {
        for (sql, expected_height) in [
            ("SELECT 1 + 2 + 3", 3),
            ("SELECT 0 WHERE 1 + 2 + 3", 3),
            ("SELECT 0 GROUP BY 1 + 2 + 3 HAVING 1 + 2 + 3", 3),
            (
                "SELECT 0 ORDER BY 1 + 2 + 3 LIMIT 1 + 2 + 3 OFFSET 1 + 2 + 3",
                4,
            ),
            ("VALUES (1 + 2 + 3)", 3),
            ("SELECT 0 UNION ALL SELECT 1 + 2 + 3", 3),
        ] {
            assert_eq!(
                parsed_select_height(sql),
                expected_height,
                "official SELECT expression-height field was omitted: {sql}"
            );
        }

        for sql in [
            "WITH c AS (SELECT 1 + 2 + 3) SELECT 0",
            "SELECT 0 FROM (SELECT 1 + 2 + 3)",
            "SELECT 0 FROM json_each(1 + 2 + 3)",
            "SELECT 0 FROM a JOIN b ON 1 + 2 + 3",
            "SELECT 0 WINDOW w AS (PARTITION BY 1 + 2 + 3 ORDER BY 1 + 2 + 3)",
        ] {
            assert_eq!(
                parsed_select_height(sql),
                1,
                "independent SQL root was incorrectly charged to its enclosing SELECT: {sql}"
            );
        }
    }

    #[test]
    fn test_cached_height_matches_sqlite_grammar_rewrites() {
        for (sql, expected_height) in [
            ("NOT 1", 2),
            ("NOT EXISTS (SELECT 1)", 3),
            ("1 BETWEEN 2 AND 3", 2),
            ("1 NOT BETWEEN 2 AND 3", 3),
            ("1 LIKE 2", 2),
            ("1 NOT LIKE 2", 3),
            ("1 IN ()", 1),
            ("1 NOT IN ()", 1),
            ("abs(1) IN ()", 3),
            ("abs(1) NOT IN ()", 3),
            ("1 IN (2)", 3),
            ("1 IN (+2)", 4),
            ("1 NOT IN (2)", 4),
            ("1 IN (2, 3)", 2),
            ("1 NOT IN (2, 3)", 3),
            ("1 IN (SELECT 1 + 2)", 3),
            ("1 IN ((SELECT 1 + 2))", 3),
            ("(1, 2) IN ((3, 4))", 2),
            ("+1", 2),
            ("++1", 2),
            ("-+1", 2),
            ("(1 + 2 + 3, 4)", 1),
            ("(1 + 2 + 3, 4) + 5", 2),
        ] {
            assert_eq!(
                parsed_expr_height(sql),
                expected_height,
                "cached grammar height mismatch: {sql}"
            );
        }
    }

    #[test]
    fn test_function_cached_height_excludes_auxiliary_expression_roots() {
        for (sql, expected_height) in [
            ("sum(1 + 2 + 3)", 4),
            ("sum(1 ORDER BY 1 + 2 + 3)", 2),
            ("sum(1) FILTER (WHERE 1 + 2 + 3)", 2),
            ("sum(1) OVER (ORDER BY 1 + 2 + 3)", 2),
            (
                "sum(1 ORDER BY 1 + 2 + 3) FILTER (WHERE 1 + 2 + 3) \
                 OVER (ORDER BY 1 + 2 + 3)",
                2,
            ),
        ] {
            assert_eq!(
                parsed_expr_height(sql),
                expected_height,
                "auxiliary root leaked into the aggregate argument height: {sql}"
            );
        }
    }

    #[test]
    fn test_function_auxiliary_roots_have_independent_1000_1001_boundaries() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = repeated_infix_expression(LIMIT, " + ");
        let over_limit = repeated_infix_expression(LIMIT + 1, " + ");
        for sql in [
            format!("sum(1) FILTER (WHERE {at_limit})"),
            format!("row_number() OVER (ORDER BY {at_limit})"),
            format!("group_concat(1 ORDER BY {at_limit})"),
        ] {
            parse_expr(&sql).expect("height-1000 auxiliary root must remain independently valid");
        }
        for sql in [
            format!("sum(1) FILTER (WHERE {over_limit})"),
            format!("row_number() OVER (ORDER BY {over_limit})"),
            format!("group_concat(1 ORDER BY {over_limit})"),
        ] {
            assert_expression_depth_error(&sql);
        }
    }

    #[test]
    fn test_select_auxiliary_roots_have_independent_1000_1001_boundaries() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = repeated_infix_expression(LIMIT, " + ");
        let over_limit = repeated_infix_expression(LIMIT + 1, " + ");
        for sql in [
            format!("(WITH c AS (SELECT {at_limit}) SELECT 0)"),
            format!("(SELECT 0 FROM (SELECT {at_limit}))"),
            format!("(SELECT 0 FROM json_each({at_limit}))"),
            format!("(SELECT 0 FROM a JOIN b ON {at_limit})"),
            format!("(SELECT 0 WINDOW w AS (ORDER BY {at_limit}))"),
        ] {
            parse_expr(&sql).expect("height-1000 independent SELECT root must remain valid");
        }
        for sql in [
            format!("(WITH c AS (SELECT {over_limit}) SELECT 0)"),
            format!("(SELECT 0 FROM (SELECT {over_limit}))"),
            format!("(SELECT 0 FROM json_each({over_limit}))"),
            format!("(SELECT 0 FROM a JOIN b ON {over_limit})"),
            format!("(SELECT 0 WINDOW w AS (ORDER BY {over_limit}))"),
        ] {
            assert_expression_depth_error(&sql);
        }
    }

    #[test]
    fn test_nested_subquery_height_is_threaded_without_ast_rescans() {
        let sql = scalar_subquery_expression(63);
        HEIGHT_WALK_VISITS.set(0);
        parse_expr(&sql).expect("tracked nested scalar-subquery height must parse");
        assert_eq!(
            HEIGHT_WALK_VISITS.get(),
            0,
            "nested subqueries must return tracked SELECT height in O(1) per parent"
        );
    }

    #[test]
    fn test_subquery_height_contract_exact_1000_1001_boundary() {
        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let inner_at_limit = repeated_infix_expression(LIMIT - 1, " + ");
        for sql in [
            format!("(SELECT {inner_at_limit})"),
            format!("EXISTS (SELECT {inner_at_limit})"),
            format!("1 IN (SELECT {inner_at_limit})"),
        ] {
            parse_expr(&sql).expect("height-999 SELECT under one expression node must be accepted");
        }

        let inner_over_limit = repeated_infix_expression(LIMIT, " + ");
        for sql in [
            format!("(SELECT {inner_over_limit})"),
            format!("EXISTS (SELECT {inner_over_limit})"),
            format!("1 IN (SELECT {inner_over_limit})"),
        ] {
            assert_expression_depth_error(&sql);
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
    fn test_explicit_row_value_in_list_rejects_mismatched_element_arities() {
        for (sql, expected_message) in [
            (
                "(a, b, c) IN ((1, 2))",
                "IN(...) element has 2 terms - expected 3",
            ),
            (
                "(a, b) IN ((1, 2, 3))",
                "IN(...) element has 3 terms - expected 2",
            ),
            ("(a, b) IN (1)", "IN(...) element has 1 term - expected 2"),
            (
                "(a, b) IN ((1, 2), 3)",
                "IN(...) element has 1 term - expected 2",
            ),
            (
                "(a, b) NOT IN ((1, 2), (3, 4, 5))",
                "IN(...) element has 3 terms - expected 2",
            ),
            (
                "0 AND (a, b) IN (1)",
                "IN(...) element has 1 term - expected 2",
            ),
            (
                "(a, b) IN (+(SELECT 1, 2))",
                "IN(...) element has 1 term - expected 2",
            ),
        ] {
            let error = parse_expr(sql).expect_err("mismatched vector IN arity must fail parsing");
            assert_eq!(
                error.kind,
                ParseErrorKind::Semantic,
                "unexpected kind for `{sql}`"
            );
            assert_eq!(
                error.message, expected_message,
                "unexpected error for `{sql}`"
            );
        }
    }

    #[test]
    fn test_subquery_lhs_defers_in_list_arity_to_semantic_resolution() {
        for sql in [
            "(SELECT 1, 2) IN (1)",
            "(SELECT 1, 2) IN ((1, 2, 3))",
            "(VALUES (1, 2, 3)) IN ((1, 2))",
            "(SELECT 1, 2 UNION ALL SELECT 3, 4) NOT IN (5)",
            "0 AND (SELECT 1, 2) IN (1)",
            "(SELECT 1, 2) IN (nosuch_vector_function())",
        ] {
            let expr = parse_expr(sql).unwrap_or_else(|error| {
                panic!("subquery-expression IN semantics must be deferred for `{sql}`: {error}")
            });
            assert!(
                matches!(expr, Expr::In { .. } | Expr::BinaryOp { .. }),
                "unexpected AST for `{sql}`"
            );
        }
    }

    #[test]
    fn test_vector_in_list_accepts_matching_empty_and_singleton_subquery_forms() {
        for sql in [
            "(a, b) IN ((1, 2), (3, 4))",
            "(a, b) NOT IN ((1, 2), (3, 4))",
            "(a, b) IN ()",
            "(a, b) IN ((SELECT 1, 2))",
            "(a, b) IN ((SELECT 1))",
            "(a, b) IN ((SELECT 1, 2, 3))",
            // Multi-element subquery lists defer their arity to the semantic
            // resolver (subquery widths need name resolution), so the parser
            // accepts them; `(a, b) IN ((SELECT 1), (SELECT 2))` then fails as a
            // FunctionError, not a parse error.
            "(a, b) IN ((SELECT 1), (SELECT 2))",
            "(SELECT 1, 2) IN ((1, 2), (3, 4))",
            "(VALUES (1, 2), (3, 4)) NOT IN ((1, 2))",
            "(SELECT 1, 2 UNION ALL SELECT 3, 4) IN ((1, 2))",
            // Star widths require schema lookup and must not be guessed here.
            "(SELECT * FROM t) IN (1)",
            "(SELECT t.* FROM t) IN ((1, 2, 3))",
            // Preserve subquery column-count diagnostics for both RHS forms.
            "(SELECT 1, 2) IN ((SELECT 1))",
            "(SELECT 1, 2) IN (SELECT 1)",
        ] {
            let expr = parse_expr(sql).unwrap_or_else(|error| {
                panic!("matching vector IN list must parse for `{sql}`: {error}")
            });
            assert!(
                matches!(expr, Expr::In { .. }),
                "unexpected AST for `{sql}`"
            );
        }
    }

    #[test]
    fn test_vector_in_list_trailing_comma_syntax_error_precedes_arity() {
        let error = parse_expr("(a, b) IN (1,)")
            .expect_err("a trailing comma in an IN list must fail parsing");
        assert_eq!(error.kind, ParseErrorKind::UnexpectedToken);
        assert_eq!(error.message, "unexpected token in expression: RightParen");
    }

    #[test]
    fn test_statement_parsers_reject_vector_in_list_arity_mismatches() {
        for (sql, expected_message) in [
            (
                "SELECT (a, b) IN (1) FROM t",
                "IN(...) element has 1 term - expected 2",
            ),
            (
                "UPDATE t SET flag = (a, b) IN ((1, 2, 3))",
                "IN(...) element has 3 terms - expected 2",
            ),
            (
                "DELETE FROM t WHERE (a, b) NOT IN ((1, 2), 3)",
                "IN(...) element has 1 term - expected 2",
            ),
        ] {
            let error = Parser::from_sql(sql)
                .parse_statement()
                .expect_err("statement parser must reject mismatched vector IN arity");
            assert_eq!(
                error.kind,
                ParseErrorKind::Semantic,
                "unexpected kind for `{sql}`"
            );
            assert_eq!(
                error.message, expected_message,
                "unexpected error for `{sql}`"
            );
        }
    }

    #[test]
    fn test_statement_parsers_defer_subquery_in_list_arity() {
        for sql in [
            "SELECT (SELECT 1, 2) IN (1)",
            "UPDATE t SET flag = (SELECT 1, 2) IN ((1, 2, 3))",
            "DELETE FROM t WHERE 0 AND (SELECT 1, 2) NOT IN ((1, 2), 3)",
        ] {
            Parser::from_sql(sql)
                .parse_statement()
                .unwrap_or_else(|error| {
                    panic!("statement semantics must be deferred for `{sql}`: {error}")
                });
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
        assert_eq!(rendered, "a IS NULL = (b IS NULL)");
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
    fn test_column_schema_qualified() {
        // Three-part `schema.table.column` reference (e.g. `main.t.id`).
        match &parse("main.t.id") {
            Expr::Column(
                ColumnRef {
                    schema: Some(s),
                    table: Some(t),
                    column,
                },
                _,
            ) => {
                assert_eq!(s.as_ref(), "main");
                assert_eq!(t.as_ref(), "t");
                assert_eq!(column.as_ref(), "id");
            }
            other => unreachable!("expected schema-qualified column, got {other:?}"),
        }
        // Round-trips through Display as `main.t.id`.
        assert_eq!(parse("main.t.id").to_string(), "main.t.id");
    }

    #[test]
    fn test_qualified_column_retains_dot_height_and_exact_boundary() {
        assert_eq!(parsed_expr_height("x"), 1);
        assert_eq!(parsed_expr_height("t.x"), 2);

        const LIMIT: usize = MAX_PARSE_DEPTH as usize;
        let at_limit = format!("{}t.x", "~".repeat(LIMIT - 2));
        parse_expr(&at_limit).expect("998 unary nodes plus qualified column have height 1000");

        let over_limit = format!("{}t.x", "~".repeat(LIMIT - 1));
        assert_expression_depth_error(&over_limit);
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
