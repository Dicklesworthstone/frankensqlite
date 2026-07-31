//! SQL pretty-printing via `fmt::Display` for AST nodes.
//!
//! Every major AST type implements `Display` to reconstruct valid SQL text.
//! This enables the round-trip property: `parse(sql).to_string()` should
//! parse identically to the original.

#[allow(clippy::wildcard_imports)]
use crate::*;
use smallvec::SmallVec;
use std::fmt;

// ---------------------------------------------------------------------------
// Helper: write a comma-separated list
// ---------------------------------------------------------------------------

fn comma_list<T: fmt::Display>(f: &mut fmt::Formatter<'_>, items: &[T]) -> fmt::Result {
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{item}")?;
    }
    Ok(())
}

fn comma_list_fn<T>(
    f: &mut fmt::Formatter<'_>,
    items: &[T],
    fmt_item: impl Fn(&T, &mut fmt::Formatter<'_>) -> fmt::Result,
) -> fmt::Result {
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        fmt_item(item, f)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: quote an identifier if needed
// ---------------------------------------------------------------------------

// Keep this list in sync with `fsqlite-parser`'s `TokenKind::keyword_str`.
const SQL_KEYWORDS: &[&str] = &[
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
    "COMMITSEQ",
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
    "FALSE",
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

fn is_sql_keyword(name: &str) -> bool {
    name.is_ascii()
        && SQL_KEYWORDS
            .iter()
            .any(|keyword| keyword.eq_ignore_ascii_case(name))
}

/// Returns true if the name needs quoting (contains special chars or is a keyword).
fn needs_quoting(name: &str) -> bool {
    if name.is_empty() {
        return true;
    }
    let first = name.as_bytes()[0];
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return true;
    }
    name.bytes()
        .any(|b| !(b.is_ascii_alphanumeric() || b == b'_'))
        || is_sql_keyword(name)
}

pub fn write_ident(f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
    if needs_quoting(name) {
        write!(f, "\"{}\"", name.replace('"', "\"\""))
    } else {
        f.write_str(name)
    }
}

pub fn write_qualified_name(
    f: &mut fmt::Formatter<'_>,
    name: &crate::QualifiedName,
) -> fmt::Result {
    if let Some(ref schema) = name.schema {
        write_ident(f, schema)?;
        f.write_str(".")?;
    }
    write_ident(f, &name.name)
}

const PREC_OR: u8 = 1;
const PREC_AND: u8 = 3;
const PREC_NOT: u8 = 5;
const PREC_EQUALITY: u8 = 7;
const PREC_COMPARISON: u8 = 9;
const PREC_ESCAPE: u8 = 11;
const PREC_BITWISE: u8 = 13;
const PREC_ADD: u8 = 15;
const PREC_MULTIPLY: u8 = 17;
const PREC_CONCAT: u8 = 19;
const PREC_COLLATE: u8 = 21;
const PREC_UNARY: u8 = 23;
const PREC_ATOM: u8 = u8::MAX;

#[derive(Clone, Copy)]
enum ExprParent {
    Binary(BinaryOp),
    Unary(UnaryOp),
    Between,
    In,
    Like,
    Escape,
    Collate,
    IsNull,
    Json,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OperandSide {
    Left,
    Right,
    Prefix,
}

fn binary_precedence(op: BinaryOp) -> u8 {
    match op {
        BinaryOp::Or => PREC_OR,
        BinaryOp::And => PREC_AND,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Is | BinaryOp::IsNot => PREC_EQUALITY,
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => PREC_COMPARISON,
        BinaryOp::BitAnd | BinaryOp::BitOr | BinaryOp::ShiftLeft | BinaryOp::ShiftRight => {
            PREC_BITWISE
        }
        BinaryOp::Add | BinaryOp::Subtract => PREC_ADD,
        BinaryOp::Multiply | BinaryOp::Divide | BinaryOp::Modulo => PREC_MULTIPLY,
        BinaryOp::Concat => PREC_CONCAT,
    }
}

fn parent_precedence(parent: ExprParent) -> u8 {
    match parent {
        ExprParent::Binary(op) => binary_precedence(op),
        ExprParent::Unary(UnaryOp::Not) => PREC_NOT,
        ExprParent::Unary(_) => PREC_UNARY,
        ExprParent::Between | ExprParent::In | ExprParent::Like | ExprParent::IsNull => {
            PREC_EQUALITY
        }
        ExprParent::Escape => PREC_ESCAPE,
        ExprParent::Collate => PREC_COLLATE,
        ExprParent::Json => PREC_CONCAT,
    }
}

fn expr_precedence(expr: &Expr) -> u8 {
    match expr {
        Expr::BinaryOp { op, .. } => binary_precedence(*op),
        Expr::UnaryOp {
            op: UnaryOp::Not, ..
        }
        | Expr::Exists { not: true, .. } => PREC_NOT,
        Expr::UnaryOp { .. } => PREC_UNARY,
        Expr::Literal(Literal::Integer(value), _) if *value < 0 => PREC_UNARY,
        Expr::Literal(Literal::Float(value), _) if !value.is_nan() && value.is_sign_negative() => {
            PREC_UNARY
        }
        Expr::Between { .. } | Expr::In { .. } | Expr::Like { .. } | Expr::IsNull { .. } => {
            PREC_EQUALITY
        }
        Expr::JsonAccess { .. } => PREC_CONCAT,
        Expr::Collate { .. } => PREC_COLLATE,
        Expr::Literal(..)
        | Expr::Column(..)
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::Exists { .. }
        | Expr::Subquery(..)
        | Expr::FunctionCall { .. }
        | Expr::Raise { .. }
        | Expr::RowValue(..)
        | Expr::Placeholder(..) => PREC_ATOM,
    }
}

/// Decide whether an operator child needs grouping in its exact parent
/// context. SQLite's infix operators are left-associative: an equal-precedence
/// left child is already grouped correctly, while an equal-precedence right
/// child must normally stay parenthesized. AND and OR are the only right-side
/// exceptions because their associative chains have a deliberately flat
/// canonical form.
fn operand_needs_parentheses(expr: &Expr, parent: ExprParent, side: OperandSide) -> bool {
    let child_precedence = expr_precedence(expr);
    let parent_precedence = parent_precedence(parent);
    if child_precedence != parent_precedence {
        return child_precedence < parent_precedence;
    }
    if side == OperandSide::Left {
        return false;
    }
    if side == OperandSide::Right {
        return !matches!(
            (parent, expr),
            (
                ExprParent::Binary(BinaryOp::And),
                Expr::BinaryOp {
                    op: BinaryOp::And,
                    ..
                }
            ) | (
                ExprParent::Binary(BinaryOp::Or),
                Expr::BinaryOp {
                    op: BinaryOp::Or,
                    ..
                }
            )
        );
    }
    true
}

enum ExprWriteTask<'a> {
    Expr(&'a Expr),
    Operand {
        expr: &'a Expr,
        parent: ExprParent,
        side: OperandSide,
    },
    Statement(&'a Statement),
    Select(&'a SelectStatement),
    SelectBody(&'a SelectBody),
    With(&'a WithClause),
    Cte(&'a Cte),
    SelectCore(&'a SelectCore),
    ResultColumn(&'a ResultColumn),
    From(&'a FromClause),
    Table(&'a TableOrSubquery),
    Join(&'a JoinClause),
    JoinConstraint(&'a JoinConstraint),
    WindowDef(&'a WindowDef),
    Limit(&'a LimitClause),
    Update(&'a UpdateStatement),
    CreateTrigger(&'a CreateTriggerStatement),
    Text(&'static str),
    Ident(&'a str),
    Literal(&'a Literal),
    Column(&'a ColumnRef),
    BinaryOp(&'a BinaryOp),
    CompoundOp(&'a CompoundOp),
    JoinType(&'a JoinType),
    UnaryOp(&'a UnaryOp),
    LikeOp(&'a LikeOp),
    TypeName(&'a TypeName),
    Placeholder(&'a PlaceholderType),
    QualifiedName(&'a QualifiedName),
    ParenthesizedSelect(&'a SelectStatement),
    OrderingTerm(&'a OrderingTerm),
    Window(&'a WindowSpec),
    Frame(&'a FrameSpec),
    FrameBound(&'a FrameBound),
}

const INLINE_EXPR_WRITE_TASKS: usize = 32;

struct ExprWriteTaskStack<'a> {
    tasks: SmallVec<[ExprWriteTask<'a>; INLINE_EXPR_WRITE_TASKS]>,
    #[cfg(test)]
    peak_len: usize,
    #[cfg(test)]
    ever_spilled: bool,
}

impl<'a> ExprWriteTaskStack<'a> {
    fn new(task: ExprWriteTask<'a>) -> Self {
        let mut tasks = SmallVec::new();
        tasks.push(task);
        Self {
            tasks,
            #[cfg(test)]
            peak_len: 1,
            #[cfg(test)]
            ever_spilled: false,
        }
    }

    fn push(&mut self, task: ExprWriteTask<'a>) {
        self.tasks.push(task);
        #[cfg(test)]
        {
            self.peak_len = self.peak_len.max(self.tasks.len());
            self.ever_spilled |= self.tasks.spilled();
        }
    }

    fn pop(&mut self) -> Option<ExprWriteTask<'a>> {
        self.tasks.pop()
    }

    #[cfg(test)]
    fn stats(&self) -> ExprWriteTaskStackStats {
        ExprWriteTaskStackStats {
            peak_len: self.peak_len,
            spilled: self.ever_spilled,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ExprWriteTaskStackStats {
    peak_len: usize,
    spilled: bool,
}

#[cfg(test)]
std::thread_local! {
    static LAST_EXPR_WRITE_TASK_STACK_STATS: std::cell::Cell<ExprWriteTaskStackStats> =
        const { std::cell::Cell::new(ExprWriteTaskStackStats { peak_len: 0, spilled: false }) };
}

fn push_comma_separated_exprs<'a>(tasks: &mut ExprWriteTaskStack<'a>, exprs: &'a [Expr]) {
    for (index, expr) in exprs.iter().enumerate().rev() {
        tasks.push(ExprWriteTask::Expr(expr));
        if index > 0 {
            tasks.push(ExprWriteTask::Text(", "));
        }
    }
}

fn push_comma_separated_ordering_terms<'a>(
    tasks: &mut ExprWriteTaskStack<'a>,
    terms: &'a [OrderingTerm],
) {
    for (index, term) in terms.iter().enumerate().rev() {
        tasks.push(ExprWriteTask::OrderingTerm(term));
        if index > 0 {
            tasks.push(ExprWriteTask::Text(", "));
        }
    }
}

fn push_comma_separated_result_columns<'a>(
    tasks: &mut ExprWriteTaskStack<'a>,
    columns: &'a [ResultColumn],
) {
    for (index, column) in columns.iter().enumerate().rev() {
        tasks.push(ExprWriteTask::ResultColumn(column));
        if index > 0 {
            tasks.push(ExprWriteTask::Text(", "));
        }
    }
}

fn push_comma_separated_ctes<'a>(tasks: &mut ExprWriteTaskStack<'a>, ctes: &'a [Cte]) {
    for (index, cte) in ctes.iter().enumerate().rev() {
        tasks.push(ExprWriteTask::Cte(cte));
        if index > 0 {
            tasks.push(ExprWriteTask::Text(", "));
        }
    }
}

fn push_comma_separated_window_defs<'a>(
    tasks: &mut ExprWriteTaskStack<'a>,
    windows: &'a [WindowDef],
) {
    for (index, window) in windows.iter().enumerate().rev() {
        tasks.push(ExprWriteTask::WindowDef(window));
        if index > 0 {
            tasks.push(ExprWriteTask::Text(", "));
        }
    }
}

#[allow(clippy::too_many_lines)]
fn write_expr(f: &mut fmt::Formatter<'_>, root: &Expr) -> fmt::Result {
    write_expr_tasks(f, ExprWriteTask::Expr(root))
}

fn write_select(f: &mut fmt::Formatter<'_>, root: &SelectStatement) -> fmt::Result {
    write_expr_tasks(f, ExprWriteTask::Select(root))
}

fn write_select_body(f: &mut fmt::Formatter<'_>, root: &SelectBody) -> fmt::Result {
    write_expr_tasks(f, ExprWriteTask::SelectBody(root))
}

fn write_select_core(f: &mut fmt::Formatter<'_>, root: &SelectCore) -> fmt::Result {
    write_expr_tasks(f, ExprWriteTask::SelectCore(root))
}

fn write_from(f: &mut fmt::Formatter<'_>, root: &FromClause) -> fmt::Result {
    write_expr_tasks(f, ExprWriteTask::From(root))
}

fn write_table(f: &mut fmt::Formatter<'_>, root: &TableOrSubquery) -> fmt::Result {
    write_expr_tasks(f, ExprWriteTask::Table(root))
}

fn write_join(f: &mut fmt::Formatter<'_>, root: &JoinClause) -> fmt::Result {
    write_expr_tasks(f, ExprWriteTask::Join(root))
}

fn write_update(f: &mut fmt::Formatter<'_>, root: &UpdateStatement) -> fmt::Result {
    write_expr_tasks(f, ExprWriteTask::Update(root))
}

fn write_create_trigger(f: &mut fmt::Formatter<'_>, root: &CreateTriggerStatement) -> fmt::Result {
    write_expr_tasks(f, ExprWriteTask::CreateTrigger(root))
}

fn write_statement(f: &mut fmt::Formatter<'_>, root: &Statement) -> fmt::Result {
    write_expr_tasks(f, ExprWriteTask::Statement(root))
}

#[allow(clippy::too_many_lines)]
fn write_expr_tasks(f: &mut fmt::Formatter<'_>, root: ExprWriteTask<'_>) -> fmt::Result {
    #[cfg(test)]
    LAST_EXPR_WRITE_TASK_STACK_STATS.set(ExprWriteTaskStackStats::default());
    let mut tasks = ExprWriteTaskStack::new(root);
    while let Some(task) = tasks.pop() {
        match task {
            ExprWriteTask::Text(text) => f.write_str(text)?,
            ExprWriteTask::Ident(name) => write_ident(f, name)?,
            ExprWriteTask::Literal(literal) => write!(f, "{literal}")?,
            ExprWriteTask::Column(column) => write!(f, "{column}")?,
            ExprWriteTask::BinaryOp(op) => write!(f, "{op}")?,
            ExprWriteTask::CompoundOp(op) => write!(f, "{op}")?,
            ExprWriteTask::JoinType(join_type) => write!(f, "{join_type}")?,
            ExprWriteTask::UnaryOp(op) => write!(f, "{op}")?,
            ExprWriteTask::LikeOp(op) => write!(f, "{op}")?,
            ExprWriteTask::TypeName(type_name) => write!(f, "{type_name}")?,
            ExprWriteTask::Placeholder(placeholder) => write!(f, "{placeholder}")?,
            ExprWriteTask::QualifiedName(name) => write!(f, "{name}")?,
            ExprWriteTask::ParenthesizedSelect(select) => {
                tasks.push(ExprWriteTask::Text(")"));
                tasks.push(ExprWriteTask::Select(select));
                tasks.push(ExprWriteTask::Text("("));
            }
            ExprWriteTask::Operand { expr, parent, side } => {
                if operand_needs_parentheses(expr, parent, side) {
                    tasks.push(ExprWriteTask::Text(")"));
                    tasks.push(ExprWriteTask::Expr(expr));
                    tasks.push(ExprWriteTask::Text("("));
                } else {
                    tasks.push(ExprWriteTask::Expr(expr));
                }
            }
            ExprWriteTask::Statement(statement) => match statement {
                Statement::Select(select) => tasks.push(ExprWriteTask::Select(select)),
                Statement::Update(update) => tasks.push(ExprWriteTask::Update(update)),
                Statement::CreateTrigger(trigger) => {
                    tasks.push(ExprWriteTask::CreateTrigger(trigger));
                }
                Statement::Explain { query_plan, stmt } => {
                    tasks.push(ExprWriteTask::Statement(stmt));
                    if *query_plan {
                        tasks.push(ExprWriteTask::Text("EXPLAIN QUERY PLAN "));
                    } else {
                        tasks.push(ExprWriteTask::Text("EXPLAIN "));
                    }
                }
                Statement::Insert(insert) => write!(f, "{insert}")?,
                Statement::Delete(delete) => write!(f, "{delete}")?,
                Statement::CreateTable(create) => write!(f, "{create}")?,
                Statement::CreateIndex(create) => write!(f, "{create}")?,
                Statement::CreateView(create) => write!(f, "{create}")?,
                Statement::CreateVirtualTable(create) => write!(f, "{create}")?,
                Statement::Drop(drop) => write!(f, "{drop}")?,
                Statement::AlterTable(alter) => write!(f, "{alter}")?,
                Statement::Begin(begin) => write!(f, "{begin}")?,
                Statement::Commit => f.write_str("COMMIT")?,
                Statement::Rollback(rollback) => write!(f, "{rollback}")?,
                Statement::Savepoint(name) => {
                    f.write_str("SAVEPOINT ")?;
                    write_ident(f, name)?;
                }
                Statement::Release(name) => {
                    f.write_str("RELEASE ")?;
                    write_ident(f, name)?;
                }
                Statement::Attach(attach) => write!(f, "{attach}")?,
                Statement::Detach(schema) => {
                    f.write_str("DETACH ")?;
                    write_ident(f, schema)?;
                }
                Statement::Pragma(pragma) => write!(f, "{pragma}")?,
                Statement::Vacuum(vacuum) => write!(f, "{vacuum}")?,
                Statement::Reindex(None) => f.write_str("REINDEX")?,
                Statement::Reindex(Some(name)) => write!(f, "REINDEX {name}")?,
                Statement::Analyze(None) => f.write_str("ANALYZE")?,
                Statement::Analyze(Some(name)) => write!(f, "ANALYZE {name}")?,
            },
            ExprWriteTask::Select(select) => {
                if let Some(limit) = &select.limit {
                    tasks.push(ExprWriteTask::Limit(limit));
                    tasks.push(ExprWriteTask::Text(" "));
                }
                if !select.order_by.is_empty() {
                    push_comma_separated_ordering_terms(&mut tasks, &select.order_by);
                    tasks.push(ExprWriteTask::Text(" ORDER BY "));
                }
                for (op, core) in select.body.compounds.iter().rev() {
                    tasks.push(ExprWriteTask::SelectCore(core));
                    tasks.push(ExprWriteTask::Text(" "));
                    tasks.push(ExprWriteTask::CompoundOp(op));
                    tasks.push(ExprWriteTask::Text(" "));
                }
                tasks.push(ExprWriteTask::SelectCore(&select.body.select));
                if let Some(with) = &select.with {
                    tasks.push(ExprWriteTask::Text(" "));
                    tasks.push(ExprWriteTask::With(with));
                }
            }
            ExprWriteTask::SelectBody(body) => {
                for (op, core) in body.compounds.iter().rev() {
                    tasks.push(ExprWriteTask::SelectCore(core));
                    tasks.push(ExprWriteTask::Text(" "));
                    tasks.push(ExprWriteTask::CompoundOp(op));
                    tasks.push(ExprWriteTask::Text(" "));
                }
                tasks.push(ExprWriteTask::SelectCore(&body.select));
            }
            ExprWriteTask::With(with) => {
                push_comma_separated_ctes(&mut tasks, &with.ctes);
                if with.recursive {
                    tasks.push(ExprWriteTask::Text("WITH RECURSIVE "));
                } else {
                    tasks.push(ExprWriteTask::Text("WITH "));
                }
            }
            ExprWriteTask::Cte(cte) => {
                tasks.push(ExprWriteTask::Text(")"));
                tasks.push(ExprWriteTask::Select(&cte.query));
                tasks.push(ExprWriteTask::Text("("));
                match cte.materialized {
                    Some(CteMaterialized::Materialized) => {
                        tasks.push(ExprWriteTask::Text("MATERIALIZED "));
                    }
                    Some(CteMaterialized::NotMaterialized) => {
                        tasks.push(ExprWriteTask::Text("NOT MATERIALIZED "));
                    }
                    None => {}
                }
                tasks.push(ExprWriteTask::Text(" AS "));
                if !cte.columns.is_empty() {
                    tasks.push(ExprWriteTask::Text(")"));
                    for (index, column) in cte.columns.iter().enumerate().rev() {
                        tasks.push(ExprWriteTask::Ident(column));
                        if index > 0 {
                            tasks.push(ExprWriteTask::Text(", "));
                        }
                    }
                    tasks.push(ExprWriteTask::Text("("));
                }
                tasks.push(ExprWriteTask::Ident(&cte.name));
            }
            ExprWriteTask::SelectCore(core) => match core {
                SelectCore::Select {
                    distinct,
                    columns,
                    from,
                    where_clause,
                    group_by,
                    having,
                    windows,
                } => {
                    if !windows.is_empty() {
                        push_comma_separated_window_defs(&mut tasks, windows);
                        tasks.push(ExprWriteTask::Text(" WINDOW "));
                    }
                    if let Some(having) = having {
                        tasks.push(ExprWriteTask::Expr(having));
                        tasks.push(ExprWriteTask::Text(" HAVING "));
                    }
                    if !group_by.is_empty() {
                        push_comma_separated_exprs(&mut tasks, group_by);
                        tasks.push(ExprWriteTask::Text(" GROUP BY "));
                    }
                    if let Some(where_clause) = where_clause {
                        tasks.push(ExprWriteTask::Expr(where_clause));
                        tasks.push(ExprWriteTask::Text(" WHERE "));
                    }
                    if let Some(from) = from {
                        tasks.push(ExprWriteTask::From(from));
                        tasks.push(ExprWriteTask::Text(" FROM "));
                    }
                    push_comma_separated_result_columns(&mut tasks, columns);
                    if *distinct == Distinctness::Distinct {
                        tasks.push(ExprWriteTask::Text("SELECT DISTINCT "));
                    } else {
                        tasks.push(ExprWriteTask::Text("SELECT "));
                    }
                }
                SelectCore::Values(rows) => {
                    for (row_index, row) in rows.iter().enumerate().rev() {
                        tasks.push(ExprWriteTask::Text(")"));
                        push_comma_separated_exprs(&mut tasks, row);
                        tasks.push(ExprWriteTask::Text("("));
                        if row_index > 0 {
                            tasks.push(ExprWriteTask::Text(", "));
                        }
                    }
                    tasks.push(ExprWriteTask::Text("VALUES "));
                }
            },
            ExprWriteTask::ResultColumn(column) => match column {
                ResultColumn::Star => tasks.push(ExprWriteTask::Text("*")),
                ResultColumn::TableStar(name) => {
                    tasks.push(ExprWriteTask::Text(".*"));
                    tasks.push(ExprWriteTask::QualifiedName(name));
                }
                ResultColumn::Expr { expr, alias } => {
                    if let Some(alias) = alias {
                        tasks.push(ExprWriteTask::Ident(alias));
                        tasks.push(ExprWriteTask::Text(" AS "));
                    }
                    tasks.push(ExprWriteTask::Expr(expr));
                }
            },
            ExprWriteTask::From(from) => {
                for join in from.joins.iter().rev() {
                    tasks.push(ExprWriteTask::Join(join));
                    tasks.push(ExprWriteTask::Text(" "));
                }
                tasks.push(ExprWriteTask::Table(&from.source));
            }
            ExprWriteTask::Table(table) => match table {
                TableOrSubquery::Table {
                    name,
                    alias,
                    index_hint,
                    time_travel,
                } => {
                    if let Some(time_travel) = time_travel {
                        write!(f, "{name}")?;
                        if let Some(alias) = alias {
                            f.write_str(" AS ")?;
                            write_ident(f, alias)?;
                        }
                        if let Some(index_hint) = index_hint {
                            write!(f, " {index_hint}")?;
                        }
                        write!(f, " {time_travel}")?;
                    } else {
                        if let Some(index_hint) = index_hint {
                            match index_hint {
                                IndexHint::IndexedBy(name) => {
                                    tasks.push(ExprWriteTask::Ident(name));
                                    tasks.push(ExprWriteTask::Text(" INDEXED BY "));
                                }
                                IndexHint::NotIndexed => {
                                    tasks.push(ExprWriteTask::Text(" NOT INDEXED"));
                                }
                            }
                        }
                        if let Some(alias) = alias {
                            tasks.push(ExprWriteTask::Ident(alias));
                            tasks.push(ExprWriteTask::Text(" AS "));
                        }
                        tasks.push(ExprWriteTask::QualifiedName(name));
                    }
                }
                TableOrSubquery::Subquery { query, alias } => {
                    if let Some(alias) = alias {
                        tasks.push(ExprWriteTask::Ident(alias));
                        tasks.push(ExprWriteTask::Text(" AS "));
                    }
                    tasks.push(ExprWriteTask::Text(")"));
                    tasks.push(ExprWriteTask::Select(query));
                    tasks.push(ExprWriteTask::Text("("));
                }
                TableOrSubquery::TableFunction { name, args, alias } => {
                    if let Some(alias) = alias {
                        tasks.push(ExprWriteTask::Ident(alias));
                        tasks.push(ExprWriteTask::Text(" AS "));
                    }
                    tasks.push(ExprWriteTask::Text(")"));
                    push_comma_separated_exprs(&mut tasks, args);
                    tasks.push(ExprWriteTask::Text("("));
                    tasks.push(ExprWriteTask::Ident(name));
                }
                TableOrSubquery::ParenJoin(inner) => {
                    tasks.push(ExprWriteTask::Text(")"));
                    tasks.push(ExprWriteTask::From(inner));
                    tasks.push(ExprWriteTask::Text("("));
                }
            },
            ExprWriteTask::Join(join) => {
                if let Some(constraint) = &join.constraint {
                    tasks.push(ExprWriteTask::JoinConstraint(constraint));
                    tasks.push(ExprWriteTask::Text(" "));
                }
                tasks.push(ExprWriteTask::Table(&join.table));
                tasks.push(ExprWriteTask::Text(" "));
                tasks.push(ExprWriteTask::JoinType(&join.join_type));
            }
            ExprWriteTask::JoinConstraint(constraint) => match constraint {
                JoinConstraint::On(expr) => {
                    tasks.push(ExprWriteTask::Expr(expr));
                    tasks.push(ExprWriteTask::Text("ON "));
                }
                JoinConstraint::Using(columns) => {
                    tasks.push(ExprWriteTask::Text(")"));
                    for (index, column) in columns.iter().enumerate().rev() {
                        tasks.push(ExprWriteTask::Ident(column));
                        if index > 0 {
                            tasks.push(ExprWriteTask::Text(", "));
                        }
                    }
                    tasks.push(ExprWriteTask::Text("USING ("));
                }
            },
            ExprWriteTask::WindowDef(window) => {
                tasks.push(ExprWriteTask::Window(&window.spec));
                tasks.push(ExprWriteTask::Text(" AS "));
                tasks.push(ExprWriteTask::Ident(&window.name));
            }
            ExprWriteTask::Limit(limit) => {
                if let Some(offset) = &limit.offset {
                    tasks.push(ExprWriteTask::Expr(offset));
                    tasks.push(ExprWriteTask::Text(" OFFSET "));
                }
                tasks.push(ExprWriteTask::Expr(&limit.limit));
                tasks.push(ExprWriteTask::Text("LIMIT "));
            }
            ExprWriteTask::Update(update) => {
                if let Some(limit) = &update.limit {
                    tasks.push(ExprWriteTask::Limit(limit));
                    tasks.push(ExprWriteTask::Text(" "));
                }
                if !update.order_by.is_empty() {
                    push_comma_separated_ordering_terms(&mut tasks, &update.order_by);
                    tasks.push(ExprWriteTask::Text(" ORDER BY "));
                }
                if !update.returning.is_empty() {
                    push_comma_separated_result_columns(&mut tasks, &update.returning);
                    tasks.push(ExprWriteTask::Text(" RETURNING "));
                }
                if let Some(where_clause) = &update.where_clause {
                    tasks.push(ExprWriteTask::Expr(where_clause));
                    tasks.push(ExprWriteTask::Text(" WHERE "));
                }
                if let Some(from) = &update.from {
                    tasks.push(ExprWriteTask::From(from));
                    tasks.push(ExprWriteTask::Text(" FROM "));
                }

                if let Some(with) = &update.with {
                    write!(f, "{with} ")?;
                }
                f.write_str("UPDATE")?;
                if let Some(action) = &update.or_conflict {
                    write!(f, " OR {action}")?;
                }
                write!(f, " {} SET ", update.table)?;
                comma_list(f, &update.assignments)?;
            }
            ExprWriteTask::CreateTrigger(trigger) => {
                tasks.push(ExprWriteTask::Text("END"));
                for statement in trigger.body.iter().rev() {
                    tasks.push(ExprWriteTask::Text("; "));
                    tasks.push(ExprWriteTask::Statement(statement));
                }

                f.write_str("CREATE ")?;
                if trigger.temporary {
                    f.write_str("TEMP ")?;
                }
                f.write_str("TRIGGER ")?;
                if trigger.if_not_exists {
                    f.write_str("IF NOT EXISTS ")?;
                }
                write!(
                    f,
                    "{} {} {} ON ",
                    trigger.name, trigger.timing, trigger.event
                )?;
                write_ident(f, &trigger.table)?;
                if trigger.for_each_row {
                    f.write_str(" FOR EACH ROW")?;
                }
                if let Some(when) = &trigger.when {
                    write!(f, " WHEN {when}")?;
                }
                f.write_str(" BEGIN ")?;
            }
            ExprWriteTask::OrderingTerm(term) => {
                if let Some(nulls) = term.nulls {
                    match nulls {
                        NullsOrder::First => tasks.push(ExprWriteTask::Text(" NULLS FIRST")),
                        NullsOrder::Last => tasks.push(ExprWriteTask::Text(" NULLS LAST")),
                    }
                }
                if let Some(direction) = term.direction {
                    match direction {
                        SortDirection::Asc => tasks.push(ExprWriteTask::Text(" ASC")),
                        SortDirection::Desc => tasks.push(ExprWriteTask::Text(" DESC")),
                    }
                }
                tasks.push(ExprWriteTask::Expr(&term.expr));
            }
            ExprWriteTask::Window(window) => {
                let has_base = window.window_ref.is_some();
                let has_partition = !window.partition_by.is_empty();
                let has_order = !window.order_by.is_empty();
                tasks.push(ExprWriteTask::Text(")"));
                if let Some(frame) = &window.frame {
                    tasks.push(ExprWriteTask::Frame(frame));
                    if has_base || has_partition || has_order {
                        tasks.push(ExprWriteTask::Text(" "));
                    }
                }
                if has_order {
                    push_comma_separated_ordering_terms(&mut tasks, &window.order_by);
                    tasks.push(ExprWriteTask::Text("ORDER BY "));
                    if has_base || has_partition {
                        tasks.push(ExprWriteTask::Text(" "));
                    }
                }
                if has_partition {
                    push_comma_separated_exprs(&mut tasks, &window.partition_by);
                    tasks.push(ExprWriteTask::Text("PARTITION BY "));
                    if has_base {
                        tasks.push(ExprWriteTask::Text(" "));
                    }
                }
                if let Some(window_ref) = &window.window_ref {
                    tasks.push(ExprWriteTask::Ident(window_ref.name()));
                }
                tasks.push(ExprWriteTask::Text("("));
            }
            ExprWriteTask::Frame(frame) => {
                if let Some(exclude) = frame.exclude {
                    match exclude {
                        FrameExclude::NoOthers => {
                            tasks.push(ExprWriteTask::Text(" EXCLUDE NO OTHERS"));
                        }
                        FrameExclude::CurrentRow => {
                            tasks.push(ExprWriteTask::Text(" EXCLUDE CURRENT ROW"));
                        }
                        FrameExclude::Group => {
                            tasks.push(ExprWriteTask::Text(" EXCLUDE GROUP"));
                        }
                        FrameExclude::Ties => {
                            tasks.push(ExprWriteTask::Text(" EXCLUDE TIES"));
                        }
                    }
                }
                if let Some(end) = &frame.end {
                    tasks.push(ExprWriteTask::FrameBound(end));
                    tasks.push(ExprWriteTask::Text(" AND "));
                    tasks.push(ExprWriteTask::FrameBound(&frame.start));
                    tasks.push(ExprWriteTask::Text(" BETWEEN "));
                } else {
                    tasks.push(ExprWriteTask::FrameBound(&frame.start));
                    tasks.push(ExprWriteTask::Text(" "));
                }
                match frame.frame_type {
                    FrameType::Rows => tasks.push(ExprWriteTask::Text("ROWS")),
                    FrameType::Range => tasks.push(ExprWriteTask::Text("RANGE")),
                    FrameType::Groups => tasks.push(ExprWriteTask::Text("GROUPS")),
                }
            }
            ExprWriteTask::FrameBound(bound) => match bound {
                FrameBound::UnboundedPreceding => {
                    tasks.push(ExprWriteTask::Text("UNBOUNDED PRECEDING"));
                }
                FrameBound::Preceding(expr) => {
                    tasks.push(ExprWriteTask::Text(" PRECEDING"));
                    tasks.push(ExprWriteTask::Expr(expr));
                }
                FrameBound::CurrentRow => tasks.push(ExprWriteTask::Text("CURRENT ROW")),
                FrameBound::Following(expr) => {
                    tasks.push(ExprWriteTask::Text(" FOLLOWING"));
                    tasks.push(ExprWriteTask::Expr(expr));
                }
                FrameBound::UnboundedFollowing => {
                    tasks.push(ExprWriteTask::Text("UNBOUNDED FOLLOWING"));
                }
            },
            ExprWriteTask::Expr(expr) => match expr {
                Expr::Literal(literal, _) => tasks.push(ExprWriteTask::Literal(literal)),
                Expr::Column(column, _) => tasks.push(ExprWriteTask::Column(column)),
                Expr::BinaryOp {
                    left, op, right, ..
                } => {
                    tasks.push(ExprWriteTask::Operand {
                        expr: right,
                        parent: ExprParent::Binary(*op),
                        side: OperandSide::Right,
                    });
                    tasks.push(ExprWriteTask::Text(" "));
                    tasks.push(ExprWriteTask::BinaryOp(op));
                    tasks.push(ExprWriteTask::Text(" "));
                    tasks.push(ExprWriteTask::Operand {
                        expr: left,
                        parent: ExprParent::Binary(*op),
                        side: OperandSide::Left,
                    });
                }
                Expr::UnaryOp { op, expr, .. } => {
                    tasks.push(ExprWriteTask::Operand {
                        expr,
                        parent: ExprParent::Unary(*op),
                        side: OperandSide::Prefix,
                    });
                    if matches!(op, UnaryOp::Not) {
                        tasks.push(ExprWriteTask::Text("NOT "));
                    } else {
                        tasks.push(ExprWriteTask::UnaryOp(op));
                    }
                }
                Expr::Between {
                    expr,
                    low,
                    high,
                    not,
                    ..
                } => {
                    tasks.push(ExprWriteTask::Operand {
                        expr: high,
                        parent: ExprParent::Between,
                        side: OperandSide::Right,
                    });
                    tasks.push(ExprWriteTask::Text(" AND "));
                    tasks.push(ExprWriteTask::Operand {
                        expr: low,
                        parent: ExprParent::Between,
                        side: OperandSide::Right,
                    });
                    tasks.push(ExprWriteTask::Text(" BETWEEN "));
                    if *not {
                        tasks.push(ExprWriteTask::Text(" NOT"));
                    }
                    tasks.push(ExprWriteTask::Operand {
                        expr,
                        parent: ExprParent::Between,
                        side: OperandSide::Left,
                    });
                }
                Expr::In { expr, set, not, .. } => {
                    match set {
                        InSet::List(items) => {
                            tasks.push(ExprWriteTask::Text(")"));
                            push_comma_separated_exprs(&mut tasks, items);
                            tasks.push(ExprWriteTask::Text("("));
                        }
                        InSet::Subquery(select) => {
                            tasks.push(ExprWriteTask::ParenthesizedSelect(select));
                        }
                        InSet::Table(name) => tasks.push(ExprWriteTask::QualifiedName(name)),
                    }
                    tasks.push(ExprWriteTask::Text(" IN "));
                    if *not {
                        tasks.push(ExprWriteTask::Text(" NOT"));
                    }
                    tasks.push(ExprWriteTask::Operand {
                        expr,
                        parent: ExprParent::In,
                        side: OperandSide::Left,
                    });
                }
                Expr::Like {
                    expr,
                    pattern,
                    escape,
                    op,
                    not,
                    ..
                } => {
                    if let Some(escape) = escape {
                        tasks.push(ExprWriteTask::Operand {
                            expr: escape,
                            parent: ExprParent::Escape,
                            side: OperandSide::Right,
                        });
                        tasks.push(ExprWriteTask::Text(" ESCAPE "));
                    }
                    tasks.push(ExprWriteTask::Operand {
                        expr: pattern,
                        parent: ExprParent::Like,
                        side: OperandSide::Right,
                    });
                    tasks.push(ExprWriteTask::Text(" "));
                    tasks.push(ExprWriteTask::LikeOp(op));
                    tasks.push(ExprWriteTask::Text(" "));
                    if *not {
                        tasks.push(ExprWriteTask::Text(" NOT"));
                    }
                    tasks.push(ExprWriteTask::Operand {
                        expr,
                        parent: ExprParent::Like,
                        side: OperandSide::Left,
                    });
                }
                Expr::Case {
                    operand,
                    whens,
                    else_expr,
                    ..
                } => {
                    tasks.push(ExprWriteTask::Text(" END"));
                    if let Some(else_expr) = else_expr {
                        tasks.push(ExprWriteTask::Expr(else_expr));
                        tasks.push(ExprWriteTask::Text(" ELSE "));
                    }
                    for (condition, result) in whens.iter().rev() {
                        tasks.push(ExprWriteTask::Expr(result));
                        tasks.push(ExprWriteTask::Text(" THEN "));
                        tasks.push(ExprWriteTask::Expr(condition));
                        tasks.push(ExprWriteTask::Text(" WHEN "));
                    }
                    if let Some(operand) = operand {
                        tasks.push(ExprWriteTask::Expr(operand));
                        tasks.push(ExprWriteTask::Text(" "));
                    }
                    tasks.push(ExprWriteTask::Text("CASE"));
                }
                Expr::Cast {
                    expr, type_name, ..
                } => {
                    tasks.push(ExprWriteTask::Text(")"));
                    tasks.push(ExprWriteTask::TypeName(type_name));
                    tasks.push(ExprWriteTask::Text(" AS "));
                    tasks.push(ExprWriteTask::Expr(expr));
                    tasks.push(ExprWriteTask::Text("CAST("));
                }
                Expr::Exists { subquery, not, .. } => {
                    tasks.push(ExprWriteTask::Text(")"));
                    tasks.push(ExprWriteTask::Select(subquery));
                    if *not {
                        tasks.push(ExprWriteTask::Text("NOT EXISTS ("));
                    } else {
                        tasks.push(ExprWriteTask::Text("EXISTS ("));
                    }
                }
                Expr::Subquery(select, _) => {
                    tasks.push(ExprWriteTask::Text(")"));
                    tasks.push(ExprWriteTask::Select(select));
                    tasks.push(ExprWriteTask::Text("("));
                }
                Expr::FunctionCall {
                    name,
                    args,
                    distinct,
                    order_by,
                    filter,
                    over,
                    ..
                } => {
                    if let Some(window) = over {
                        match &window.window_ref {
                            Some(WindowReference::Direct(name))
                                if window.partition_by.is_empty()
                                    && window.order_by.is_empty()
                                    && window.frame.is_none() =>
                            {
                                tasks.push(ExprWriteTask::Ident(name));
                            }
                            _ => tasks.push(ExprWriteTask::Window(window)),
                        }
                        tasks.push(ExprWriteTask::Text(" OVER "));
                    }
                    if let Some(filter) = filter {
                        tasks.push(ExprWriteTask::Text(")"));
                        tasks.push(ExprWriteTask::Expr(filter));
                        tasks.push(ExprWriteTask::Text(" FILTER (WHERE "));
                    }
                    tasks.push(ExprWriteTask::Text(")"));
                    if !order_by.is_empty() {
                        push_comma_separated_ordering_terms(&mut tasks, order_by);
                        tasks.push(ExprWriteTask::Text(" ORDER BY "));
                    }
                    match args {
                        FunctionArgs::Star => tasks.push(ExprWriteTask::Text("*")),
                        FunctionArgs::List(items) => {
                            push_comma_separated_exprs(&mut tasks, items);
                        }
                    }
                    if *distinct {
                        tasks.push(ExprWriteTask::Text("DISTINCT "));
                    }
                    tasks.push(ExprWriteTask::Text("("));
                    tasks.push(ExprWriteTask::Ident(name));
                }
                Expr::Collate {
                    expr, collation, ..
                } => {
                    tasks.push(ExprWriteTask::Ident(collation));
                    tasks.push(ExprWriteTask::Text(" COLLATE "));
                    tasks.push(ExprWriteTask::Operand {
                        expr,
                        parent: ExprParent::Collate,
                        side: OperandSide::Left,
                    });
                }
                Expr::IsNull { expr, not, .. } => {
                    if *not {
                        tasks.push(ExprWriteTask::Text(" IS NOT NULL"));
                    } else {
                        tasks.push(ExprWriteTask::Text(" IS NULL"));
                    }
                    tasks.push(ExprWriteTask::Operand {
                        expr,
                        parent: ExprParent::IsNull,
                        side: OperandSide::Left,
                    });
                }
                Expr::Raise {
                    action, message, ..
                } => {
                    write!(f, "RAISE({action}")?;
                    if let Some(message) = message {
                        write!(f, ", '{}'", message.replace('\'', "''"))?;
                    }
                    f.write_str(")")?;
                }
                Expr::JsonAccess {
                    expr, path, arrow, ..
                } => {
                    tasks.push(ExprWriteTask::Operand {
                        expr: path,
                        parent: ExprParent::Json,
                        side: OperandSide::Right,
                    });
                    match arrow {
                        JsonArrow::Arrow => tasks.push(ExprWriteTask::Text(" -> ")),
                        JsonArrow::DoubleArrow => tasks.push(ExprWriteTask::Text(" ->> ")),
                    }
                    tasks.push(ExprWriteTask::Operand {
                        expr,
                        parent: ExprParent::Json,
                        side: OperandSide::Left,
                    });
                }
                Expr::RowValue(exprs, _) => {
                    tasks.push(ExprWriteTask::Text(")"));
                    push_comma_separated_exprs(&mut tasks, exprs);
                    tasks.push(ExprWriteTask::Text("("));
                }
                Expr::Placeholder(placeholder, _) => {
                    tasks.push(ExprWriteTask::Placeholder(placeholder));
                }
            },
        }
    }
    #[cfg(test)]
    LAST_EXPR_WRITE_TASK_STACK_STATS.set(tasks.stats());
    Ok(())
}

// ---------------------------------------------------------------------------
// Literal
// ---------------------------------------------------------------------------

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(n) => write!(f, "{n}"),
            Self::Float(v) => {
                if v.is_nan() {
                    // SQLite never surfaces NaN as a REAL value. Arithmetic
                    // and register writes normalize it to SQL NULL, so the
                    // executable SQL rendering must preserve that policy
                    // instead of emitting the identifier `NaN`.
                    f.write_str("NULL")
                } else if v.is_infinite() {
                    if v.is_sign_negative() {
                        f.write_str("-9e999")
                    } else {
                        f.write_str("9e999")
                    }
                // Ensure a finite integral float always has a decimal point.
                } else if v.fract() == 0.0 && !v.is_nan() {
                    write!(f, "{v:.1}")
                } else {
                    write!(f, "{v}")
                }
            }
            Self::String(s) => {
                write!(f, "'{}'", s.replace('\'', "''"))
            }
            Self::Blob(bytes) => {
                f.write_str("X'")?;
                for b in bytes {
                    write!(f, "{b:02X}")?;
                }
                f.write_str("'")
            }
            Self::Null => f.write_str("NULL"),
            Self::True => f.write_str("TRUE"),
            Self::False => f.write_str("FALSE"),
            Self::CurrentTime => f.write_str("CURRENT_TIME"),
            Self::CurrentDate => f.write_str("CURRENT_DATE"),
            Self::CurrentTimestamp => f.write_str("CURRENT_TIMESTAMP"),
        }
    }
}

// ---------------------------------------------------------------------------
// ColumnRef
// ---------------------------------------------------------------------------

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref t) = self.table {
            write_ident(f, t)?;
            f.write_str(".")?;
        }
        write_ident(f, &self.column)
    }
}

// ---------------------------------------------------------------------------
// TypeName
// ---------------------------------------------------------------------------

impl fmt::Display for TypeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)?;
        match (&self.arg1, &self.arg2) {
            (Some(a1), Some(a2)) => write!(f, "({a1}, {a2})"),
            (Some(a1), None) => write!(f, "({a1})"),
            _ => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------------
// PlaceholderType
// ---------------------------------------------------------------------------

impl fmt::Display for PlaceholderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Anonymous => f.write_str("?"),
            Self::Numbered(n) => write!(f, "?{n}"),
            Self::ColonNamed(s) => write!(f, ":{s}"),
            Self::AtNamed(s) => write!(f, "@{s}"),
            Self::DollarNamed(s) => write!(f, "${s}"),
        }
    }
}

// ---------------------------------------------------------------------------
// LikeOp
// ---------------------------------------------------------------------------

impl fmt::Display for LikeOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Like => "LIKE",
            Self::Glob => "GLOB",
            Self::Match => "MATCH",
            Self::Regexp => "REGEXP",
        })
    }
}

// ---------------------------------------------------------------------------
// RaiseAction
// ---------------------------------------------------------------------------

impl fmt::Display for RaiseAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ignore => "IGNORE",
            Self::Rollback => "ROLLBACK",
            Self::Abort => "ABORT",
            Self::Fail => "FAIL",
        })
    }
}

// ---------------------------------------------------------------------------
// Expr
// ---------------------------------------------------------------------------

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_expr(f, self)
    }
}

// ---------------------------------------------------------------------------
// WindowSpec
// ---------------------------------------------------------------------------

impl fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(WindowReference::Direct(name)) = &self.window_ref
            && self.partition_by.is_empty()
            && self.order_by.is_empty()
            && self.frame.is_none()
        {
            return write_ident(f, name);
        }
        f.write_str("(")?;
        let mut need_space = if let Some(window_ref) = &self.window_ref {
            write_ident(f, window_ref.name())?;
            true
        } else {
            false
        };
        if !self.partition_by.is_empty() {
            if need_space {
                f.write_str(" ")?;
            }
            f.write_str("PARTITION BY ")?;
            comma_list(f, &self.partition_by)?;
            need_space = true;
        }
        if !self.order_by.is_empty() {
            if need_space {
                f.write_str(" ")?;
            }
            f.write_str("ORDER BY ")?;
            comma_list(f, &self.order_by)?;
            need_space = true;
        }
        if let Some(frame) = &self.frame {
            if need_space {
                f.write_str(" ")?;
            }
            write!(f, "{frame}")?;
        }
        f.write_str(")")
    }
}

// ---------------------------------------------------------------------------
// FrameSpec
// ---------------------------------------------------------------------------

impl fmt::Display for FrameSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.frame_type)?;
        if let Some(ref end) = self.end {
            write!(f, " BETWEEN {} AND {end}", self.start)?;
        } else {
            write!(f, " {}", self.start)?;
        }
        if let Some(ref excl) = self.exclude {
            write!(f, " EXCLUDE {excl}")?;
        }
        Ok(())
    }
}

impl fmt::Display for FrameType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Rows => "ROWS",
            Self::Range => "RANGE",
            Self::Groups => "GROUPS",
        })
    }
}

impl fmt::Display for FrameBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnboundedPreceding => f.write_str("UNBOUNDED PRECEDING"),
            Self::Preceding(e) => write!(f, "{e} PRECEDING"),
            Self::CurrentRow => f.write_str("CURRENT ROW"),
            Self::Following(e) => write!(f, "{e} FOLLOWING"),
            Self::UnboundedFollowing => f.write_str("UNBOUNDED FOLLOWING"),
        }
    }
}

impl fmt::Display for FrameExclude {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoOthers => "NO OTHERS",
            Self::CurrentRow => "CURRENT ROW",
            Self::Group => "GROUP",
            Self::Ties => "TIES",
        })
    }
}

// ---------------------------------------------------------------------------
// OrderingTerm
// ---------------------------------------------------------------------------

impl fmt::Display for OrderingTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if let Some(dir) = self.direction {
            write!(f, " {dir}")?;
        }
        if let Some(nulls) = self.nulls {
            write!(f, " {nulls}")?;
        }
        Ok(())
    }
}

impl fmt::Display for SortDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        })
    }
}

impl fmt::Display for NullsOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::First => "NULLS FIRST",
            Self::Last => "NULLS LAST",
        })
    }
}

// ---------------------------------------------------------------------------
// ResultColumn
// ---------------------------------------------------------------------------

impl fmt::Display for ResultColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Star => f.write_str("*"),
            Self::TableStar(t) => {
                write_qualified_name(f, t)?;
                f.write_str(".*")
            }
            Self::Expr { expr, alias } => {
                write!(f, "{expr}")?;
                if let Some(a) = alias {
                    f.write_str(" AS ")?;
                    write_ident(f, a)?;
                }
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SelectStatement
// ---------------------------------------------------------------------------

impl fmt::Display for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_select(f, self)
    }
}

// ---------------------------------------------------------------------------
// WithClause / CTE
// ---------------------------------------------------------------------------

impl fmt::Display for WithClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WITH ")?;
        if self.recursive {
            f.write_str("RECURSIVE ")?;
        }
        comma_list(f, &self.ctes)
    }
}

impl fmt::Display for Cte {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_ident(f, &self.name)?;
        if !self.columns.is_empty() {
            f.write_str("(")?;
            comma_list_fn(f, &self.columns, |col, f| write_ident(f, col))?;
            f.write_str(")")?;
        }
        f.write_str(" AS ")?;
        if let Some(mat) = self.materialized {
            write!(f, "{mat} ")?;
        }
        write!(f, "({})", self.query)
    }
}

impl fmt::Display for CteMaterialized {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Materialized => "MATERIALIZED",
            Self::NotMaterialized => "NOT MATERIALIZED",
        })
    }
}

// ---------------------------------------------------------------------------
// SelectBody / SelectCore
// ---------------------------------------------------------------------------

impl fmt::Display for SelectBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_select_body(f, self)
    }
}

impl fmt::Display for CompoundOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Union => "UNION",
            Self::UnionAll => "UNION ALL",
            Self::Intersect => "INTERSECT",
            Self::Except => "EXCEPT",
        })
    }
}

impl fmt::Display for SelectCore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_select_core(f, self)
    }
}

// ---------------------------------------------------------------------------
// FromClause / TableOrSubquery
// ---------------------------------------------------------------------------

impl fmt::Display for FromClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_from(f, self)
    }
}

impl fmt::Display for TableOrSubquery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_table(f, self)
    }
}

impl fmt::Display for IndexHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IndexedBy(name) => {
                f.write_str("INDEXED BY ")?;
                write_ident(f, name)
            }
            Self::NotIndexed => f.write_str("NOT INDEXED"),
        }
    }
}

// ---------------------------------------------------------------------------
// Time-travel clause
// ---------------------------------------------------------------------------

impl fmt::Display for TimeTravelClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FOR SYSTEM_TIME AS OF {}", self.target)
    }
}

impl fmt::Display for TimeTravelTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommitSequence(seq) => write!(f, "COMMITSEQ {seq}"),
            Self::Timestamp(ts) => write!(f, "'{}'", ts.replace('\'', "''")),
        }
    }
}

// ---------------------------------------------------------------------------
// JoinClause / JoinType
// ---------------------------------------------------------------------------

impl fmt::Display for JoinClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_join(f, self)
    }
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.natural {
            f.write_str("NATURAL ")?;
        }
        write!(f, "{}", self.kind)?;
        f.write_str(" JOIN")
    }
}

impl fmt::Display for JoinKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Cross => "CROSS",
            Self::Inner => "INNER",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::Full => "FULL",
        })
    }
}

impl fmt::Display for JoinConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::On(expr) => write!(f, "ON {expr}"),
            Self::Using(cols) => {
                f.write_str("USING (")?;
                comma_list_fn(f, cols, |col, f| write_ident(f, col))?;
                f.write_str(")")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WindowDef
// ---------------------------------------------------------------------------

impl fmt::Display for WindowDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_ident(f, &self.name)?;
        write!(f, " AS {}", self.spec)
    }
}

// ---------------------------------------------------------------------------
// LimitClause
// ---------------------------------------------------------------------------

impl fmt::Display for LimitClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LIMIT {}", self.limit)?;
        if let Some(ref off) = self.offset {
            write!(f, " OFFSET {off}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ConflictAction
// ---------------------------------------------------------------------------

impl fmt::Display for ConflictAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Rollback => "ROLLBACK",
            Self::Abort => "ABORT",
            Self::Fail => "FAIL",
            Self::Ignore => "IGNORE",
            Self::Replace => "REPLACE",
        })
    }
}

// ---------------------------------------------------------------------------
// InsertStatement
// ---------------------------------------------------------------------------

impl fmt::Display for InsertStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref with) = self.with {
            write!(f, "{with} ")?;
        }
        if self.or_conflict == Some(ConflictAction::Replace) {
            f.write_str("REPLACE")?;
        } else {
            f.write_str("INSERT")?;
            if let Some(ref action) = self.or_conflict {
                write!(f, " OR {action}")?;
            }
        }
        write!(f, " INTO {}", self.table)?;
        if let Some(ref a) = self.alias {
            f.write_str(" AS ")?;
            write_ident(f, a)?;
        }
        if !self.columns.is_empty() {
            f.write_str(" (")?;
            comma_list_fn(f, &self.columns, |col, f| write_ident(f, col))?;
            f.write_str(")")?;
        }
        write!(f, " {}", self.source)?;
        for upsert in &self.upsert {
            write!(f, " {upsert}")?;
        }
        if !self.returning.is_empty() {
            f.write_str(" RETURNING ")?;
            comma_list(f, &self.returning)?;
        }
        Ok(())
    }
}

impl fmt::Display for InsertSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Values(rows) => {
                f.write_str("VALUES ")?;
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    f.write_str("(")?;
                    comma_list(f, row)?;
                    f.write_str(")")?;
                }
                Ok(())
            }
            Self::Select(q) => write!(f, "{q}"),
            Self::DefaultValues => f.write_str("DEFAULT VALUES"),
        }
    }
}

impl fmt::Display for UpsertClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ON CONFLICT")?;
        if let Some(ref target) = self.target {
            f.write_str(" (")?;
            comma_list(f, &target.columns)?;
            f.write_str(")")?;
            if let Some(ref w) = target.where_clause {
                write!(f, " WHERE {w}")?;
            }
        }
        write!(f, " {}", self.action)
    }
}

impl fmt::Display for IndexedColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if let Some(ref col) = self.collation {
            write!(f, " COLLATE {col}")?;
        }
        if let Some(dir) = self.direction {
            write!(f, " {dir}")?;
        }
        Ok(())
    }
}

impl fmt::Display for UpsertAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nothing => f.write_str("DO NOTHING"),
            Self::Update {
                assignments,
                where_clause,
            } => {
                f.write_str("DO UPDATE SET ")?;
                comma_list(f, assignments)?;
                if let Some(w) = where_clause {
                    write!(f, " WHERE {w}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.target, self.value)
    }
}

impl fmt::Display for AssignmentTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(name) => write_ident(f, name),
            Self::ColumnList(names) => {
                f.write_str("(")?;
                comma_list_fn(f, names, |col, f| write_ident(f, col))?;
                f.write_str(")")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// UpdateStatement
// ---------------------------------------------------------------------------

impl fmt::Display for UpdateStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_update(f, self)
    }
}

impl fmt::Display for QualifiedTableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(ref a) = self.alias {
            f.write_str(" AS ")?;
            write_ident(f, a)?;
        }
        if let Some(ref hint) = self.index_hint {
            write!(f, " {hint}")?;
        }
        if let Some(ref tt) = self.time_travel {
            write!(f, " {tt}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DeleteStatement
// ---------------------------------------------------------------------------

impl fmt::Display for DeleteStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref with) = self.with {
            write!(f, "{with} ")?;
        }
        write!(f, "DELETE FROM {}", self.table)?;
        if let Some(ref w) = self.where_clause {
            write!(f, " WHERE {w}")?;
        }
        if !self.returning.is_empty() {
            f.write_str(" RETURNING ")?;
            comma_list(f, &self.returning)?;
        }
        if !self.order_by.is_empty() {
            f.write_str(" ORDER BY ")?;
            comma_list(f, &self.order_by)?;
        }
        if let Some(ref lim) = self.limit {
            write!(f, " {lim}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CreateTableStatement
// ---------------------------------------------------------------------------

impl fmt::Display for CreateTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.temporary {
            f.write_str("TEMP ")?;
        }
        f.write_str("TABLE ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        match &self.body {
            CreateTableBody::Columns {
                columns,
                constraints,
            } => {
                f.write_str(" (")?;
                for (i, col) in columns.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{col}")?;
                }
                for constraint in constraints {
                    f.write_str(", ")?;
                    write!(f, "{constraint}")?;
                }
                f.write_str(")")?;
            }
            CreateTableBody::AsSelect(q) => {
                write!(f, " AS {q}")?;
            }
        }
        let mut table_options = Vec::new();
        if self.without_rowid {
            table_options.push("WITHOUT ROWID");
        }
        if self.strict {
            table_options.push("STRICT");
        }
        if !table_options.is_empty() {
            write!(f, " {}", table_options.join(", "))?;
        }
        Ok(())
    }
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_ident(f, &self.name)?;
        if let Some(ref tn) = self.type_name {
            write!(f, " {tn}")?;
        }
        for c in &self.constraints {
            write!(f, " {c}")?;
        }
        Ok(())
    }
}

impl fmt::Display for ColumnConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref name) = self.name {
            f.write_str("CONSTRAINT ")?;
            write_ident(f, name)?;
            f.write_str(" ")?;
        }
        write!(f, "{}", self.kind)
    }
}

impl fmt::Display for ColumnConstraintKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrimaryKey {
                direction,
                conflict,
                autoincrement,
            } => {
                f.write_str("PRIMARY KEY")?;
                if let Some(dir) = direction {
                    write!(f, " {dir}")?;
                }
                if let Some(action) = conflict {
                    write!(f, " ON CONFLICT {action}")?;
                }
                if *autoincrement {
                    f.write_str(" AUTOINCREMENT")?;
                }
                Ok(())
            }
            Self::NotNull { conflict } => {
                f.write_str("NOT NULL")?;
                if let Some(action) = conflict {
                    write!(f, " ON CONFLICT {action}")?;
                }
                Ok(())
            }
            Self::Null => f.write_str("NULL"),
            Self::Unique { conflict } => {
                f.write_str("UNIQUE")?;
                if let Some(action) = conflict {
                    write!(f, " ON CONFLICT {action}")?;
                }
                Ok(())
            }
            Self::Check(expr) => write!(f, "CHECK ({expr})"),
            Self::Default(val) => {
                f.write_str("DEFAULT ")?;
                match val {
                    DefaultValue::Expr(e) => write!(f, "{e}"),
                    DefaultValue::ParenExpr(e) => write!(f, "({e})"),
                }
            }
            Self::Collate(name) => write!(f, "COLLATE {name}"),
            Self::ForeignKey(fk) => write!(f, "{fk}"),
            Self::Generated { expr, storage } => {
                write!(f, "GENERATED ALWAYS AS ({expr})")?;
                if let Some(s) = storage {
                    write!(f, " {s}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for GeneratedStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stored => "STORED",
            Self::Virtual => "VIRTUAL",
        })
    }
}

impl fmt::Display for TableConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref name) = self.name {
            f.write_str("CONSTRAINT ")?;
            write_ident(f, name)?;
            f.write_str(" ")?;
        }
        write!(f, "{}", self.kind)
    }
}

impl fmt::Display for TableConstraintKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrimaryKey { columns, conflict } => {
                f.write_str("PRIMARY KEY (")?;
                comma_list(f, columns)?;
                f.write_str(")")?;
                if let Some(action) = conflict {
                    write!(f, " ON CONFLICT {action}")?;
                }
                Ok(())
            }
            Self::Unique { columns, conflict } => {
                f.write_str("UNIQUE (")?;
                comma_list(f, columns)?;
                f.write_str(")")?;
                if let Some(action) = conflict {
                    write!(f, " ON CONFLICT {action}")?;
                }
                Ok(())
            }
            Self::Check(expr) => write!(f, "CHECK ({expr})"),
            Self::ForeignKey { columns, clause } => {
                f.write_str("FOREIGN KEY (")?;
                comma_list_fn(f, columns, |col, f| write_ident(f, col))?;
                write!(f, ") {clause}")
            }
        }
    }
}

impl fmt::Display for ForeignKeyClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("REFERENCES ")?;
        write_ident(f, &self.table)?;
        if !self.columns.is_empty() {
            f.write_str(" (")?;
            comma_list_fn(f, &self.columns, |col, f| write_ident(f, col))?;
            f.write_str(")")?;
        }
        for action in &self.actions {
            write!(f, " {action}")?;
        }
        if let Some(ref def) = self.deferrable {
            write!(f, " {def}")?;
        }
        Ok(())
    }
}

impl fmt::Display for ForeignKeyAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.trigger, self.action)
    }
}

impl fmt::Display for ForeignKeyTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::OnDelete => "ON DELETE",
            Self::OnUpdate => "ON UPDATE",
        })
    }
}

impl fmt::Display for ForeignKeyActionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::SetNull => "SET NULL",
            Self::SetDefault => "SET DEFAULT",
            Self::Cascade => "CASCADE",
            Self::Restrict => "RESTRICT",
            Self::NoAction => "NO ACTION",
        })
    }
}

impl fmt::Display for Deferrable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.not {
            f.write_str("NOT ")?;
        }
        f.write_str("DEFERRABLE")?;
        if let Some(init) = self.initially {
            write!(f, " {init}")?;
        }
        Ok(())
    }
}

impl fmt::Display for DeferrableInitially {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Deferred => "INITIALLY DEFERRED",
            Self::Immediate => "INITIALLY IMMEDIATE",
        })
    }
}

// ---------------------------------------------------------------------------
// CreateIndexStatement
// ---------------------------------------------------------------------------

impl fmt::Display for CreateIndexStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.unique {
            f.write_str("UNIQUE ")?;
        }
        f.write_str("INDEX ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        write!(f, "{} ON ", self.name)?;
        write_ident(f, &self.table)?;
        f.write_str("(")?;
        comma_list(f, &self.columns)?;
        f.write_str(")")?;
        if let Some(ref w) = self.where_clause {
            write!(f, " WHERE {w}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CreateViewStatement
// ---------------------------------------------------------------------------

impl fmt::Display for CreateViewStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.temporary {
            f.write_str("TEMP ")?;
        }
        f.write_str("VIEW ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        if !self.columns.is_empty() {
            f.write_str(" (")?;
            comma_list_fn(f, &self.columns, |col, f| write_ident(f, col))?;
            f.write_str(")")?;
        }
        write!(f, " AS {}", self.query)
    }
}

// ---------------------------------------------------------------------------
// CreateTriggerStatement
// ---------------------------------------------------------------------------

impl fmt::Display for CreateTriggerStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_create_trigger(f, self)
    }
}

impl fmt::Display for TriggerTiming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Before => "BEFORE",
            Self::After => "AFTER",
            Self::InsteadOf => "INSTEAD OF",
        })
    }
}

impl fmt::Display for TriggerEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert => f.write_str("INSERT"),
            Self::Delete => f.write_str("DELETE"),
            Self::Update(cols) => {
                f.write_str("UPDATE")?;
                if !cols.is_empty() {
                    f.write_str(" OF ")?;
                    comma_list_fn(f, cols, |col, f| write_ident(f, col))?;
                }
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CreateVirtualTableStatement
// ---------------------------------------------------------------------------

impl fmt::Display for CreateVirtualTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE VIRTUAL TABLE ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        write!(f, "{} USING ", self.name)?;
        write_ident(f, &self.module)?;
        if !self.args.is_empty() {
            f.write_str("(")?;
            for (i, arg) in self.args.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                f.write_str(arg)?;
            }
            f.write_str(")")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DropStatement
// ---------------------------------------------------------------------------

impl fmt::Display for DropStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP {}", self.object_type)?;
        if self.if_exists {
            f.write_str(" IF EXISTS")?;
        }
        write!(f, " {}", self.name)
    }
}

impl fmt::Display for DropObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Table => "TABLE",
            Self::View => "VIEW",
            Self::Index => "INDEX",
            Self::Trigger => "TRIGGER",
        })
    }
}

// ---------------------------------------------------------------------------
// AlterTableStatement
// ---------------------------------------------------------------------------

impl fmt::Display for AlterTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER TABLE {} {}", self.table, self.action)
    }
}

impl fmt::Display for AlterTableAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RenameTo(name) => {
                f.write_str("RENAME TO ")?;
                write_ident(f, name)
            }
            Self::RenameColumn { old, new } => {
                f.write_str("RENAME COLUMN ")?;
                write_ident(f, old)?;
                f.write_str(" TO ")?;
                write_ident(f, new)
            }
            Self::AddColumn(col) => write!(f, "ADD COLUMN {col}"),
            Self::DropColumn(name) => {
                f.write_str("DROP COLUMN ")?;
                write_ident(f, name)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Transaction control
// ---------------------------------------------------------------------------

impl fmt::Display for BeginStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BEGIN")?;
        if let Some(mode) = self.mode {
            write!(f, " {mode}")?;
        }
        Ok(())
    }
}

impl fmt::Display for TransactionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Deferred => "DEFERRED",
            Self::Immediate => "IMMEDIATE",
            Self::Exclusive => "EXCLUSIVE",
            Self::Concurrent => "CONCURRENT",
        })
    }
}

impl fmt::Display for RollbackStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ROLLBACK")?;
        if let Some(ref sp) = self.to_savepoint {
            f.write_str(" TO SAVEPOINT ")?;
            write_ident(f, sp)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ATTACH / DETACH / PRAGMA / VACUUM
// ---------------------------------------------------------------------------

impl fmt::Display for AttachStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ATTACH {} AS ", self.expr)?;
        write_ident(f, &self.schema)
    }
}

impl fmt::Display for PragmaStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PRAGMA {}", self.name)?;
        if let Some(ref val) = self.value {
            match val {
                PragmaValue::Assign(e) => write!(f, " = {e}")?,
                PragmaValue::Call(e) => write!(f, "({e})")?,
            }
        }
        Ok(())
    }
}

impl fmt::Display for VacuumStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VACUUM")?;
        if let Some(ref s) = self.schema {
            f.write_str(" ")?;
            write_ident(f, s)?;
        }
        if let Some(ref expr) = self.into {
            write!(f, " INTO {expr}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Top-level Statement
// ---------------------------------------------------------------------------

impl fmt::Display for Statement {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_statement(f, self)
    }
}

#[cfg(test)]
mod expr_display_tests {
    use super::*;
    use std::fmt::Write as _;

    fn column(name: &str) -> Expr {
        Expr::Column(ColumnRef::bare(name), Span::ZERO)
    }

    fn integer(value: i64) -> Expr {
        Expr::Literal(Literal::Integer(value), Span::ZERO)
    }

    fn binary(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
            span: Span::ZERO,
        }
    }

    fn table_source(name: &str) -> TableOrSubquery {
        TableOrSubquery::Table {
            name: QualifiedName::bare(name),
            alias: None,
            index_hint: None,
            time_travel: None,
        }
    }

    fn nested_from(height: usize) -> FromClause {
        let mut from = FromClause {
            source: table_source("leaf"),
            joins: Vec::new(),
        };
        for _ in 0..height {
            from = FromClause {
                source: TableOrSubquery::ParenJoin(Box::new(from)),
                joins: Vec::new(),
            };
        }
        from
    }

    fn update_with_from(from: FromClause) -> UpdateStatement {
        UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("target"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("x".to_owned()),
                value: integer(1),
            }],
            from: Some(from),
            where_clause: None,
            returning: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        }
    }

    fn drop_table(name: &str) -> Statement {
        Statement::Drop(DropStatement {
            object_type: DropObjectType::Table,
            if_exists: false,
            name: QualifiedName::bare(name),
        })
    }

    fn drop_from_iteratively(root: FromClause) {
        let mut from_clauses = vec![root];
        let mut tables = Vec::new();
        while let Some(FromClause { source, joins }) = from_clauses.pop() {
            tables.push(source);
            for join in joins {
                tables.push(join.table);
                drop(join.constraint);
            }
            while let Some(table) = tables.pop() {
                match table {
                    TableOrSubquery::ParenJoin(inner) => from_clauses.push(*inner),
                    leaf => drop(leaf),
                }
            }
        }
    }

    fn drop_table_iteratively(table: TableOrSubquery) {
        match table {
            TableOrSubquery::ParenJoin(inner) => drop_from_iteratively(*inner),
            leaf => drop(leaf),
        }
    }

    fn drop_select_core_iteratively(core: SelectCore) {
        match core {
            SelectCore::Select { from, .. } => {
                if let Some(from) = from {
                    drop_from_iteratively(from);
                }
            }
            SelectCore::Values(_) => {}
        }
    }

    fn drop_select_body_iteratively(body: SelectBody) {
        drop_select_core_iteratively(body.select);
        for (_, core) in body.compounds {
            drop_select_core_iteratively(core);
        }
    }

    fn drop_statement_iteratively(mut statement: Statement) {
        loop {
            match statement {
                Statement::Explain { stmt, .. } => statement = *stmt,
                Statement::CreateTrigger(mut trigger) if trigger.body.len() == 1 => {
                    statement = trigger
                        .body
                        .pop()
                        .expect("single trigger body statement must exist");
                }
                Statement::Update(mut update) => {
                    if let Some(from) = update.from.take() {
                        drop_from_iteratively(from);
                    }
                    break;
                }
                leaf => {
                    drop(leaf);
                    break;
                }
            }
        }
    }

    fn drop_scalar_subquery_chain_iteratively(mut expr: Expr) {
        loop {
            match expr {
                Expr::Subquery(select, _) => {
                    let SelectStatement {
                        with,
                        body,
                        order_by,
                        limit,
                    } = *select;
                    assert!(with.is_none());
                    assert!(order_by.is_empty());
                    assert!(limit.is_none());
                    let SelectBody { select, compounds } = body;
                    assert!(compounds.is_empty());
                    let SelectCore::Select {
                        mut columns,
                        from,
                        where_clause,
                        group_by,
                        having,
                        windows,
                        ..
                    } = select
                    else {
                        panic!("scalar-subquery chain must contain SELECT cores");
                    };
                    assert!(from.is_none());
                    assert!(where_clause.is_none());
                    assert!(group_by.is_empty());
                    assert!(having.is_none());
                    assert!(windows.is_empty());
                    assert_eq!(columns.len(), 1);
                    let ResultColumn::Expr { expr: child, alias } = columns
                        .pop()
                        .expect("scalar-subquery SELECT must contain one column")
                    else {
                        panic!("scalar-subquery SELECT column must be an expression");
                    };
                    assert!(alias.is_none());
                    expr = child;
                }
                leaf => {
                    drop(leaf);
                    break;
                }
            }
        }
    }

    fn format_statement_on_one_mib_stack(statement: Statement) -> String {
        let (rendered, statement) = std::thread::Builder::new()
            .stack_size(1024 * 1024)
            .spawn(move || (statement.to_string(), statement))
            .expect("1 MiB formatter thread must spawn")
            .join()
            .expect("formatting on a 1 MiB stack must not overflow");
        drop_statement_iteratively(statement);
        rendered
    }

    #[test]
    fn expression_task_stack_uses_inline_boundary_and_preserves_lifo() {
        let mut stack = ExprWriteTaskStack::new(ExprWriteTask::Text("first"));
        stack.push(ExprWriteTask::Text("second"));
        stack.push(ExprWriteTask::Text("third"));
        assert!(matches!(stack.pop(), Some(ExprWriteTask::Text("third"))));
        assert!(matches!(stack.pop(), Some(ExprWriteTask::Text("second"))));
        assert!(matches!(stack.pop(), Some(ExprWriteTask::Text("first"))));

        let mut boundary = ExprWriteTaskStack::new(ExprWriteTask::Text("inline"));
        for _ in 1..INLINE_EXPR_WRITE_TASKS {
            boundary.push(ExprWriteTask::Text("inline"));
        }
        assert!(!boundary.tasks.spilled());
        boundary.push(ExprWriteTask::Text("spill"));
        assert!(boundary.tasks.spilled());
        while boundary.pop().is_some() {}
        assert!(
            boundary.stats().spilled,
            "spill history must survive draining the task stack"
        );
    }

    #[test]
    fn formatter_error_resets_task_stack_stats() {
        struct FailingWriter;

        impl fmt::Write for FailingWriter {
            fn write_str(&mut self, _: &str) -> fmt::Result {
                Err(fmt::Error)
            }
        }

        let mut deep = integer(1);
        for _ in 1..100 {
            deep = binary(deep, BinaryOp::Add, integer(1));
        }
        let _ = deep.to_string();
        assert!(
            LAST_EXPR_WRITE_TASK_STACK_STATS
                .with(std::cell::Cell::get)
                .spilled
        );

        let mut writer = FailingWriter;
        write!(&mut writer, "{}", column("value"))
            .expect_err("the test writer must reject formatter output");
        assert_eq!(
            LAST_EXPR_WRITE_TASK_STACK_STATS.with(std::cell::Cell::get),
            ExprWriteTaskStackStats::default(),
            "an early formatter error must not expose the previous call's task stats"
        );
    }

    #[test]
    fn public_from_roots_format_height_1000_and_1001_on_one_mib_stack() {
        for height in [1000, 1001] {
            let from_root = nested_from(height);
            let table_root = TableOrSubquery::ParenJoin(Box::new(nested_from(height)));
            let join_root = JoinClause {
                join_type: JoinType {
                    natural: false,
                    kind: JoinKind::Inner,
                },
                table: TableOrSubquery::ParenJoin(Box::new(nested_from(height))),
                constraint: None,
            };
            let select_core_root = SelectCore::Select {
                distinct: Distinctness::All,
                columns: vec![ResultColumn::Star],
                from: Some(nested_from(height)),
                where_clause: None,
                group_by: Vec::new(),
                having: None,
                windows: Vec::new(),
            };
            let select_body_root = SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Star],
                    from: Some(nested_from(height)),
                    where_clause: None,
                    group_by: Vec::new(),
                    having: None,
                    windows: Vec::new(),
                },
                compounds: Vec::new(),
            };
            let update_root = Statement::Update(update_with_from(nested_from(height)));

            let (rendered, roots) = std::thread::Builder::new()
                .stack_size(1024 * 1024)
                .spawn(move || {
                    let rendered = [
                        from_root.to_string(),
                        table_root.to_string(),
                        join_root.to_string(),
                        select_core_root.to_string(),
                        select_body_root.to_string(),
                        update_root.to_string(),
                    ];
                    (
                        rendered,
                        (
                            from_root,
                            table_root,
                            join_root,
                            select_core_root,
                            select_body_root,
                            update_root,
                        ),
                    )
                })
                .expect("1 MiB formatter thread must spawn")
                .join()
                .expect("all public FROM roots must format without stack overflow");
            let (
                from_root,
                table_root,
                JoinClause {
                    table: join_table, ..
                },
                select_core_root,
                select_body_root,
                update_root,
            ) = roots;
            drop_from_iteratively(from_root);
            drop_table_iteratively(table_root);
            drop_table_iteratively(join_table);
            drop_select_core_iteratively(select_core_root);
            drop_select_body_iteratively(select_body_root);
            drop_statement_iteratively(update_root);

            let expected_parentheses = [height, height + 1, height + 1, height, height, height];
            for (sql, expected) in rendered.iter().zip(expected_parentheses) {
                assert_eq!(sql.matches('(').count(), expected);
                assert_eq!(sql.matches(')').count(), expected);
                assert!(sql.contains("leaf"));
            }
            assert!(rendered[2].starts_with("INNER JOIN "));
            assert!(rendered[3].starts_with("SELECT * FROM "));
            assert!(rendered[4].starts_with("SELECT * FROM "));
            assert!(rendered[5].starts_with("UPDATE target SET x = 1 FROM "));
        }
    }

    #[test]
    fn nested_explain_height_1000_and_1001_formats_on_one_mib_stack() {
        for height in [1000, 1001] {
            let mut statement = drop_table("leaf");
            for level in 0..height {
                statement = Statement::Explain {
                    query_plan: level % 2 == 0,
                    stmt: Box::new(statement),
                };
            }

            let rendered = format_statement_on_one_mib_stack(statement);
            let mut tail = rendered.as_str();
            for level in (0..height).rev() {
                let prefix = if level % 2 == 0 {
                    "EXPLAIN QUERY PLAN "
                } else {
                    "EXPLAIN "
                };
                tail = tail
                    .strip_prefix(prefix)
                    .expect("EXPLAIN wrappers must retain their exact order");
            }
            assert_eq!(tail, "DROP TABLE leaf");
        }
    }

    #[test]
    fn nested_trigger_body_height_1000_and_1001_formats_on_one_mib_stack() {
        for height in [1000, 1001] {
            let mut statement = drop_table("leaf");
            for level in 0..height {
                statement = Statement::CreateTrigger(CreateTriggerStatement {
                    if_not_exists: false,
                    temporary: false,
                    name: QualifiedName::bare(format!("trigger_{level}")),
                    timing: TriggerTiming::After,
                    event: TriggerEvent::Insert,
                    table: "target".to_owned(),
                    for_each_row: false,
                    when: None,
                    body: vec![statement],
                });
            }

            let rendered = format_statement_on_one_mib_stack(statement);
            assert_eq!(rendered.matches("CREATE TRIGGER ").count(), height);
            assert_eq!(rendered.matches("; END").count(), height);
            assert!(rendered.contains("DROP TABLE leaf"));
            assert!(rendered.ends_with("END"));
        }
    }

    #[test]
    fn iterative_public_roots_preserve_shallow_sql() {
        let joined = FromClause {
            source: table_source("a"),
            joins: vec![JoinClause {
                join_type: JoinType {
                    natural: false,
                    kind: JoinKind::Inner,
                },
                table: table_source("b"),
                constraint: Some(JoinConstraint::On(binary(
                    column("a_id"),
                    BinaryOp::Eq,
                    column("b_id"),
                ))),
            }],
        };
        assert_eq!(joined.to_string(), "a INNER JOIN b ON a_id = b_id");

        let body = SelectBody {
            select: SelectCore::Select {
                distinct: Distinctness::All,
                columns: vec![ResultColumn::Star],
                from: Some(joined),
                where_clause: None,
                group_by: Vec::new(),
                having: None,
                windows: Vec::new(),
            },
            compounds: Vec::new(),
        };
        assert_eq!(
            body.to_string(),
            "SELECT * FROM a INNER JOIN b ON a_id = b_id"
        );

        let compounds = SelectBody {
            select: SelectCore::Values(vec![vec![integer(1)]]),
            compounds: vec![
                (
                    CompoundOp::UnionAll,
                    SelectCore::Values(vec![vec![integer(2)]]),
                ),
                (
                    CompoundOp::Except,
                    SelectCore::Values(vec![vec![integer(3)]]),
                ),
            ],
        };
        assert_eq!(
            compounds.to_string(),
            "VALUES (1) UNION ALL VALUES (2) EXCEPT VALUES (3)"
        );

        let update = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("target"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("x".to_owned()),
                value: integer(1),
            }],
            from: Some(FromClause {
                source: table_source("source"),
                joins: Vec::new(),
            }),
            where_clause: Some(binary(column("id"), BinaryOp::Eq, integer(7))),
            returning: vec![ResultColumn::Expr {
                expr: column("x"),
                alias: Some("updated".to_owned()),
            }],
            order_by: vec![OrderingTerm {
                expr: column("id"),
                direction: Some(SortDirection::Desc),
                nulls: Some(NullsOrder::Last),
            }],
            limit: Some(LimitClause {
                limit: integer(10),
                offset: Some(integer(2)),
            }),
        };
        assert_eq!(
            update.to_string(),
            "UPDATE target SET x = 1 FROM source WHERE id = 7 RETURNING x AS updated \
             ORDER BY id DESC NULLS LAST LIMIT 10 OFFSET 2"
        );

        let explained = Statement::Explain {
            query_plan: true,
            stmt: Box::new(drop_table("old")),
        };
        assert_eq!(explained.to_string(), "EXPLAIN QUERY PLAN DROP TABLE old");

        let trigger = CreateTriggerStatement {
            if_not_exists: false,
            temporary: false,
            name: QualifiedName::bare("tr"),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            table: "target".to_owned(),
            for_each_row: false,
            when: None,
            body: vec![drop_table("old"), drop_table("older")],
        };
        assert_eq!(
            trigger.to_string(),
            "CREATE TRIGGER tr AFTER INSERT ON target BEGIN \
             DROP TABLE old; DROP TABLE older; END"
        );
    }

    #[test]
    fn representative_rich_select_stays_in_inline_task_stack() {
        let cte_query = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Values(vec![vec![integer(1)]]),
                compounds: Vec::new(),
            },
            order_by: Vec::new(),
            limit: None,
        };
        let select = SelectStatement {
            with: Some(WithClause {
                recursive: false,
                ctes: vec![Cte {
                    name: "seed".to_owned(),
                    columns: vec!["id".to_owned()],
                    materialized: Some(CteMaterialized::NotMaterialized),
                    query: cte_query,
                }],
            }),
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::Distinct,
                    columns: vec![
                        ResultColumn::Expr {
                            expr: column("a_id"),
                            alias: Some("id".to_owned()),
                        },
                        ResultColumn::Expr {
                            expr: column("b_value"),
                            alias: None,
                        },
                    ],
                    from: Some(FromClause {
                        source: table_source("a"),
                        joins: vec![JoinClause {
                            join_type: JoinType {
                                natural: false,
                                kind: JoinKind::Left,
                            },
                            table: table_source("b"),
                            constraint: Some(JoinConstraint::On(binary(
                                column("a_id"),
                                BinaryOp::Eq,
                                column("b_id"),
                            ))),
                        }],
                    }),
                    where_clause: Some(Box::new(binary(column("a_id"), BinaryOp::Gt, integer(0)))),
                    group_by: vec![column("a_id")],
                    having: Some(Box::new(binary(
                        column("b_value"),
                        BinaryOp::IsNot,
                        Expr::Literal(Literal::Null, Span::ZERO),
                    ))),
                    windows: vec![WindowDef {
                        name: "w".to_owned(),
                        spec: WindowSpec {
                            window_ref: None,
                            partition_by: vec![column("a_id")],
                            order_by: vec![OrderingTerm {
                                expr: column("b_value"),
                                direction: Some(SortDirection::Desc),
                                nulls: None,
                            }],
                            frame: None,
                        },
                    }],
                },
                compounds: Vec::new(),
            },
            order_by: vec![OrderingTerm {
                expr: column("a_id"),
                direction: Some(SortDirection::Asc),
                nulls: Some(NullsOrder::First),
            }],
            limit: Some(LimitClause {
                limit: integer(25),
                offset: Some(integer(5)),
            }),
        };

        assert_eq!(
            select.to_string(),
            "WITH seed(id) AS NOT MATERIALIZED (VALUES (1)) \
             SELECT DISTINCT a_id AS id, b_value FROM a \
             LEFT JOIN b ON a_id = b_id WHERE a_id > 0 GROUP BY a_id \
             HAVING b_value IS NOT NULL WINDOW w AS \
             (PARTITION BY a_id ORDER BY b_value DESC) \
             ORDER BY a_id ASC NULLS FIRST LIMIT 25 OFFSET 5"
        );
        let task_stats = LAST_EXPR_WRITE_TASK_STACK_STATS.with(std::cell::Cell::get);
        assert!(
            !task_stats.spilled,
            "representative rich SELECT should remain in the inline task stack"
        );
        assert!(task_stats.peak_len <= INLINE_EXPR_WRITE_TASKS);
    }

    #[test]
    fn binary_operands_use_minimal_semantics_preserving_parentheses() {
        let tighter_right = binary(
            column("a"),
            BinaryOp::Add,
            binary(column("b"), BinaryOp::Multiply, integer(2)),
        );
        assert_eq!(tighter_right.to_string(), "a + b * 2");

        let looser_right = binary(
            column("a"),
            BinaryOp::Multiply,
            binary(column("b"), BinaryOp::Add, column("c")),
        );
        assert_eq!(looser_right.to_string(), "a * (b + c)");

        let left_associative = binary(
            binary(column("a"), BinaryOp::Subtract, column("b")),
            BinaryOp::Subtract,
            column("c"),
        );
        assert_eq!(left_associative.to_string(), "a - b - c");

        let right_subtract = binary(
            column("a"),
            BinaryOp::Subtract,
            binary(column("b"), BinaryOp::Subtract, column("c")),
        );
        assert_eq!(right_subtract.to_string(), "a - (b - c)");

        let right_divide = binary(
            column("a"),
            BinaryOp::Divide,
            binary(column("b"), BinaryOp::Divide, column("c")),
        );
        assert_eq!(right_divide.to_string(), "a / (b / c)");

        let and_chain = binary(
            column("a"),
            BinaryOp::And,
            binary(column("b"), BinaryOp::And, column("c")),
        );
        assert_eq!(and_chain.to_string(), "a AND b AND c");

        let or_chain = binary(
            column("a"),
            BinaryOp::Or,
            binary(column("b"), BinaryOp::Or, column("c")),
        );
        assert_eq!(or_chain.to_string(), "a OR b OR c");
    }

    #[test]
    fn expression_display_height_1000_uses_bounded_work_stack() {
        let mut expr = Expr::Literal(Literal::Integer(1), Span::ZERO);
        for _ in 1..1000 {
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                span: Span::ZERO,
            };
        }

        let rendered = expr.to_string();
        let task_stats = LAST_EXPR_WRITE_TASK_STACK_STATS.with(std::cell::Cell::get);
        assert!(
            task_stats.spilled,
            "height-1000 expression should exercise the heap spill path"
        );
        assert!(task_stats.peak_len > INLINE_EXPR_WRITE_TASKS);
        assert_eq!(rendered.matches('+').count(), 999);
        assert!(rendered.ends_with(" + 1"));
    }

    #[test]
    fn scalar_subquery_display_height_1000_uses_one_mib_stack() {
        let mut expr = Expr::Literal(Literal::Integer(1), Span::ZERO);
        for _ in 1..1000 {
            expr = Expr::Subquery(
                Box::new(SelectStatement {
                    with: None,
                    body: SelectBody {
                        select: SelectCore::Select {
                            distinct: Distinctness::All,
                            columns: vec![ResultColumn::Expr { expr, alias: None }],
                            from: None,
                            where_clause: None,
                            group_by: Vec::new(),
                            having: None,
                            windows: Vec::new(),
                        },
                        compounds: Vec::new(),
                    },
                    order_by: Vec::new(),
                    limit: None,
                }),
                Span::ZERO,
            );
        }

        let (rendered, expr) = std::thread::Builder::new()
            .stack_size(1024 * 1024)
            .spawn(move || {
                let rendered = expr.to_string();
                (rendered, expr)
            })
            .expect("1 MiB formatter thread must spawn")
            .join()
            .expect("height-1000 scalar subquery formatting must not overflow");
        drop_scalar_subquery_chain_iteratively(expr);
        assert_eq!(rendered.matches("(SELECT ").count(), 999);
        assert!(rendered.ends_with(&")".repeat(999)));
    }

    #[test]
    fn negative_literal_operands_cannot_merge_into_sql_comments() {
        let integer = Expr::UnaryOp {
            op: UnaryOp::Negate,
            expr: Box::new(Expr::Literal(Literal::Integer(i64::MIN), Span::ZERO)),
            span: Span::ZERO,
        };
        assert_eq!(integer.to_string(), "-(-9223372036854775808)");

        let negative_zero = Expr::UnaryOp {
            op: UnaryOp::Negate,
            expr: Box::new(Expr::Literal(Literal::Float(-0.0), Span::ZERO)),
            span: Span::ZERO,
        };
        assert_eq!(negative_zero.to_string(), "-(-0.0)");
    }

    #[test]
    fn like_escape_operand_preserves_comparison_grouping() {
        let expr = Expr::Like {
            expr: Box::new(column("value")),
            pattern: Box::new(column("pattern")),
            escape: Some(Box::new(binary(
                column("lower"),
                BinaryOp::Lt,
                column("upper"),
            ))),
            op: LikeOp::Like,
            not: false,
            span: Span::ZERO,
        };
        assert_eq!(
            expr.to_string(),
            "value LIKE pattern ESCAPE (lower < upper)"
        );
    }

    #[test]
    fn infinite_float_literals_render_as_numeric_sql() {
        assert_eq!(Literal::Float(f64::INFINITY).to_string(), "9e999");
        assert_eq!(Literal::Float(f64::NEG_INFINITY).to_string(), "-9e999");
        assert_eq!(Literal::Float(f64::NAN).to_string(), "NULL");
        assert_eq!(Literal::Float(-f64::NAN).to_string(), "NULL");
    }

    #[test]
    fn collation_names_use_identifier_quoting() {
        let expr = Expr::Collate {
            expr: Box::new(column("value")),
            collation: "my col".to_owned(),
            span: Span::ZERO,
        };
        assert_eq!(expr.to_string(), "value COLLATE \"my col\"");
    }

    #[test]
    fn window_reference_form_is_preserved_exactly() {
        let extended = Expr::FunctionCall {
            name: "sum".to_owned(),
            args: FunctionArgs::List(vec![column("x")]),
            distinct: false,
            order_by: Vec::new(),
            filter: None,
            over: Some(WindowSpec {
                window_ref: Some(WindowReference::Base("base".to_owned())),
                partition_by: vec![column("p")],
                order_by: vec![OrderingTerm {
                    expr: column("y"),
                    direction: None,
                    nulls: None,
                }],
                frame: Some(FrameSpec {
                    frame_type: FrameType::Rows,
                    start: FrameBound::Preceding(Box::new(column("z"))),
                    end: Some(FrameBound::CurrentRow),
                    exclude: None,
                }),
            }),
            span: Span::ZERO,
        };
        assert_eq!(
            extended.to_string(),
            "sum(x) OVER (base PARTITION BY p ORDER BY y ROWS BETWEEN z PRECEDING AND CURRENT ROW)"
        );
        let task_stats = LAST_EXPR_WRITE_TASK_STACK_STATS.with(std::cell::Cell::get);
        assert!(
            !task_stats.spilled,
            "representative window expression should remain in the inline task stack"
        );
        assert!(task_stats.peak_len <= INLINE_EXPR_WRITE_TASKS);

        let bare = Expr::FunctionCall {
            name: "sum".to_owned(),
            args: FunctionArgs::List(vec![column("x")]),
            distinct: false,
            order_by: Vec::new(),
            filter: None,
            over: Some(WindowSpec {
                window_ref: Some(WindowReference::Direct("base".to_owned())),
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: None,
            }),
            span: Span::ZERO,
        };
        assert_eq!(bare.to_string(), "sum(x) OVER base");
        let Expr::FunctionCall {
            over: Some(bare_window),
            ..
        } = &bare
        else {
            panic!("bare window function should carry a window");
        };
        assert_eq!(bare_window.to_string(), "base");

        let parenthesized = Expr::FunctionCall {
            name: "sum".to_owned(),
            args: FunctionArgs::List(vec![column("x")]),
            distinct: false,
            order_by: Vec::new(),
            filter: None,
            over: Some(WindowSpec {
                window_ref: Some(WindowReference::Base("base".to_owned())),
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: None,
            }),
            span: Span::ZERO,
        };
        assert_eq!(parenthesized.to_string(), "sum(x) OVER (base)");
        let Expr::FunctionCall {
            over: Some(parenthesized_window),
            ..
        } = &parenthesized
        else {
            panic!("parenthesized window function should carry a window");
        };
        assert_eq!(parenthesized_window.to_string(), "(base)");
    }
}
