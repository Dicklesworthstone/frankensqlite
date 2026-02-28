//! SQL pretty-printing via `fmt::Display` for AST nodes.
//!
//! Every major AST type implements `Display` to reconstruct valid SQL text.
//! This enables the round-trip property: `parse(sql).to_string()` should
//! parse identically to the original.

#[allow(clippy::wildcard_imports)]
use crate::*;
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
}

fn write_ident(f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
    if needs_quoting(name) {
        write!(f, "\"{}\"", name.replace('"', "\"\""))
    } else {
        f.write_str(name)
    }
}

/// Write an expression, wrapping in parentheses if it is a binary or unary op.
/// This ensures correct precedence round-trips through parse → display → re-parse
/// and prevents operator merging (e.g. `--x` which becomes a line comment).
fn write_paren_if_compound(f: &mut fmt::Formatter<'_>, expr: &crate::Expr) -> fmt::Result {
    if matches!(
        expr,
        crate::Expr::BinaryOp { .. } | crate::Expr::UnaryOp { .. }
    ) {
        write!(f, "({expr})")
    } else {
        write!(f, "{expr}")
    }
}

// ---------------------------------------------------------------------------
// Literal
// ---------------------------------------------------------------------------

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(n) => write!(f, "{n}"),
            Self::Float(v) => {
                // Ensure the float always has a decimal point.
                if v.fract() == 0.0 && !v.is_infinite() && !v.is_nan() {
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
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(lit, _) => write!(f, "{lit}"),
            Self::Column(col, _) => write!(f, "{col}"),
            Self::BinaryOp {
                left, op, right, ..
            } => {
                write_paren_if_compound(f, left)?;
                write!(f, " {op} ")?;
                write_paren_if_compound(f, right)
            }
            Self::UnaryOp { op, expr, .. } => {
                if matches!(op, UnaryOp::Not) {
                    write!(f, "NOT ")?;
                } else {
                    write!(f, "{op}")?;
                }
                write_paren_if_compound(f, expr)
            }
            Self::Between {
                expr,
                low,
                high,
                not,
                ..
            } => {
                write_paren_if_compound(f, expr)?;
                if *not {
                    f.write_str(" NOT")?;
                }
                f.write_str(" BETWEEN ")?;
                write_paren_if_compound(f, low)?;
                f.write_str(" AND ")?;
                write_paren_if_compound(f, high)
            }
            Self::In { expr, set, not, .. } => {
                write_paren_if_compound(f, expr)?;
                if *not {
                    f.write_str(" NOT")?;
                }
                f.write_str(" IN ")?;
                match set {
                    InSet::List(items) => {
                        f.write_str("(")?;
                        comma_list(f, items)?;
                        f.write_str(")")
                    }
                    InSet::Subquery(q) => write!(f, "({q})"),
                    InSet::Table(name) => write!(f, "{name}"),
                }
            }
            Self::Like {
                expr,
                pattern,
                escape,
                op,
                not,
                ..
            } => {
                write_paren_if_compound(f, expr)?;
                if *not {
                    f.write_str(" NOT")?;
                }
                write!(f, " {op} ")?;
                write_paren_if_compound(f, pattern)?;
                if let Some(esc) = escape {
                    f.write_str(" ESCAPE ")?;
                    write_paren_if_compound(f, esc)?;
                }
                Ok(())
            }
            Self::Case {
                operand,
                whens,
                else_expr,
                ..
            } => {
                f.write_str("CASE")?;
                if let Some(op) = operand {
                    write!(f, " {op}")?;
                }
                for (cond, then) in whens {
                    write!(f, " WHEN {cond} THEN {then}")?;
                }
                if let Some(el) = else_expr {
                    write!(f, " ELSE {el}")?;
                }
                f.write_str(" END")
            }
            Self::Cast {
                expr, type_name, ..
            } => write!(f, "CAST({expr} AS {type_name})"),
            Self::Exists { subquery, not, .. } => {
                if *not {
                    f.write_str("NOT ")?;
                }
                write!(f, "EXISTS ({subquery})")
            }
            Self::Subquery(q, _) => write!(f, "({q})"),
            Self::FunctionCall {
                name,
                args,
                distinct,
                filter,
                over,
                ..
            } => {
                write_ident(f, name)?;
                f.write_str("(")?;
                if *distinct {
                    f.write_str("DISTINCT ")?;
                }
                match args {
                    FunctionArgs::Star => f.write_str("*")?,
                    FunctionArgs::List(items) => comma_list(f, items)?,
                }
                f.write_str(")")?;
                if let Some(filter_expr) = filter {
                    write!(f, " FILTER (WHERE {filter_expr})")?;
                }
                if let Some(win) = over {
                    f.write_str(" OVER ")?;
                    if let Some(ref base) = win.base_window {
                        write_ident(f, base)?;
                    } else {
                        write!(f, "{win}")?;
                    }
                }
                Ok(())
            }
            Self::Collate {
                expr, collation, ..
            } => {
                write_paren_if_compound(f, expr)?;
                write!(f, " COLLATE {collation}")
            }
            Self::IsNull { expr, not, .. } => {
                write_paren_if_compound(f, expr)?;
                if *not {
                    f.write_str(" IS NOT NULL")
                } else {
                    f.write_str(" IS NULL")
                }
            }
            Self::Raise {
                action, message, ..
            } => {
                f.write_str("RAISE(")?;
                write!(f, "{action}")?;
                if let Some(msg) = message {
                    write!(f, ", '{}'", msg.replace('\'', "''"))?;
                }
                f.write_str(")")
            }
            Self::JsonAccess {
                expr, path, arrow, ..
            } => {
                write_paren_if_compound(f, expr)?;
                match arrow {
                    JsonArrow::Arrow => f.write_str(" -> ")?,
                    JsonArrow::DoubleArrow => f.write_str(" ->> ")?,
                }
                write_paren_if_compound(f, path)
            }
            Self::RowValue(exprs, _) => {
                f.write_str("(")?;
                comma_list(f, exprs)?;
                f.write_str(")")
            }
            Self::Placeholder(ph, _) => write!(f, "{ph}"),
        }
    }
}

// ---------------------------------------------------------------------------
// WindowSpec
// ---------------------------------------------------------------------------

impl fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("(")?;
        let mut need_space = if let Some(base) = &self.base_window {
            write_ident(f, base)?;
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
                write_ident(f, t)?;
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
        if let Some(ref with) = self.with {
            write!(f, "{with} ")?;
        }
        write!(f, "{}", self.body)?;
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
        write!(f, "{}", self.select)?;
        for (op, core) in &self.compounds {
            write!(f, " {op} {core}")?;
        }
        Ok(())
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
        match self {
            Self::Select {
                distinct,
                columns,
                from,
                where_clause,
                group_by,
                having,
                windows,
            } => {
                f.write_str("SELECT ")?;
                if *distinct == Distinctness::Distinct {
                    f.write_str("DISTINCT ")?;
                }
                comma_list(f, columns)?;
                if let Some(from_clause) = from {
                    write!(f, " FROM {from_clause}")?;
                }
                if let Some(w) = where_clause {
                    write!(f, " WHERE {w}")?;
                }
                if !group_by.is_empty() {
                    f.write_str(" GROUP BY ")?;
                    comma_list(f, group_by)?;
                }
                if let Some(h) = having {
                    write!(f, " HAVING {h}")?;
                }
                if !windows.is_empty() {
                    f.write_str(" WINDOW ")?;
                    comma_list(f, windows)?;
                }
                Ok(())
            }
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
        }
    }
}

// ---------------------------------------------------------------------------
// FromClause / TableOrSubquery
// ---------------------------------------------------------------------------

impl fmt::Display for FromClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.source)?;
        for join in &self.joins {
            write!(f, " {join}")?;
        }
        Ok(())
    }
}

impl fmt::Display for TableOrSubquery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table {
                name,
                alias,
                index_hint,
            } => {
                write!(f, "{name}")?;
                if let Some(a) = alias {
                    f.write_str(" AS ")?;
                    write_ident(f, a)?;
                }
                if let Some(hint) = index_hint {
                    write!(f, " {hint}")?;
                }
                Ok(())
            }
            Self::Subquery { query, alias } => {
                write!(f, "({query})")?;
                if let Some(a) = alias {
                    f.write_str(" AS ")?;
                    write_ident(f, a)?;
                }
                Ok(())
            }
            Self::TableFunction { name, args, alias } => {
                write_ident(f, name)?;
                f.write_str("(")?;
                comma_list(f, args)?;
                f.write_str(")")?;
                if let Some(a) = alias {
                    f.write_str(" AS ")?;
                    write_ident(f, a)?;
                }
                Ok(())
            }
            Self::ParenJoin(inner) => write!(f, "({inner})"),
        }
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
// JoinClause / JoinType
// ---------------------------------------------------------------------------

impl fmt::Display for JoinClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.join_type, self.table)?;
        if let Some(ref constraint) = self.constraint {
            write!(f, " {constraint}")?;
        }
        Ok(())
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
        if let Some(ref with) = self.with {
            write!(f, "{with} ")?;
        }
        f.write_str("UPDATE")?;
        if let Some(ref action) = self.or_conflict {
            write!(f, " OR {action}")?;
        }
        write!(f, " {}", self.table)?;
        f.write_str(" SET ")?;
        comma_list(f, &self.assignments)?;
        if let Some(ref from_clause) = self.from {
            write!(f, " FROM {from_clause}")?;
        }
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
        if self.without_rowid {
            f.write_str(" WITHOUT ROWID")?;
        }
        if self.strict {
            f.write_str(" STRICT")?;
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
        f.write_str(" (")?;
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
        f.write_str("CREATE ")?;
        if self.temporary {
            f.write_str("TEMP ")?;
        }
        f.write_str("TRIGGER ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        write!(f, "{} {} {} ON ", self.name, self.timing, self.event)?;
        write_ident(f, &self.table)?;
        if self.for_each_row {
            f.write_str(" FOR EACH ROW")?;
        }
        if let Some(ref w) = self.when {
            write!(f, " WHEN {w}")?;
        }
        f.write_str(" BEGIN ")?;
        for stmt in &self.body {
            write!(f, "{stmt}; ")?;
        }
        f.write_str("END")
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
        match self {
            Self::Select(s) => write!(f, "{s}"),
            Self::Insert(s) => write!(f, "{s}"),
            Self::Update(s) => write!(f, "{s}"),
            Self::Delete(s) => write!(f, "{s}"),
            Self::CreateTable(s) => write!(f, "{s}"),
            Self::CreateIndex(s) => write!(f, "{s}"),
            Self::CreateView(s) => write!(f, "{s}"),
            Self::CreateTrigger(s) => write!(f, "{s}"),
            Self::CreateVirtualTable(s) => write!(f, "{s}"),
            Self::Drop(s) => write!(f, "{s}"),
            Self::AlterTable(s) => write!(f, "{s}"),
            Self::Begin(s) => write!(f, "{s}"),
            Self::Commit => f.write_str("COMMIT"),
            Self::Rollback(s) => write!(f, "{s}"),
            Self::Savepoint(name) => {
                f.write_str("SAVEPOINT ")?;
                write_ident(f, name)
            }
            Self::Release(name) => {
                f.write_str("RELEASE ")?;
                write_ident(f, name)
            }
            Self::Attach(s) => write!(f, "{s}"),
            Self::Detach(schema) => {
                f.write_str("DETACH ")?;
                write_ident(f, schema)
            }
            Self::Pragma(s) => write!(f, "{s}"),
            Self::Vacuum(s) => write!(f, "{s}"),
            Self::Reindex(None) => f.write_str("REINDEX"),
            Self::Reindex(Some(name)) => write!(f, "REINDEX {name}"),
            Self::Analyze(None) => f.write_str("ANALYZE"),
            Self::Analyze(Some(name)) => write!(f, "ANALYZE {name}"),
            Self::Explain { query_plan, stmt } => {
                if *query_plan {
                    write!(f, "EXPLAIN QUERY PLAN {stmt}")
                } else {
                    write!(f, "EXPLAIN {stmt}")
                }
            }
        }
    }
}
