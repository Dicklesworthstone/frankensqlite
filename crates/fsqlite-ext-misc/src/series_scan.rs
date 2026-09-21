//! Constraint-driven scans for generate_series. Plan arguments are mapped in
//! hidden-column order followed by value bounds, not WHERE-clause order.
//! Range intersection takes place in sequence-index space: changing a bound
//! must never change the residue class of start + n * step.

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::vtab::{ConstraintOp, IndexInfo};
use fsqlite_types::{SqliteValue, cx::Cx};

use super::{GENERATE_SERIES_DEFAULT_STOP, GenerateSeriesCursor};

const START: i32 = 1;
const STOP: i32 = 2;
const STEP: i32 = 4;
const EQUAL: i32 = 8;
const GREATER: i32 = 16;
const AT_LEAST: i32 = 32;
const LESS: i32 = 64;
const AT_MOST: i32 = 128;
const ARGUMENTS: i32 = 255;
const PLANNED: i32 = 256;
const ASCENDING: i32 = 512;
const DESCENDING: i32 = 1024;
const PLAN_NAME: &str = "series-range-v1";

fn invalid_plan() -> FrankenError {
    FrankenError::FunctionError("generate_series: invalid scan plan or argument count".to_owned())
}

fn missing_start() -> FrankenError {
    FrankenError::FunctionError("generate_series: start argument is required".to_owned())
}

fn is_sql_null(value: &SqliteValue) -> bool {
    value.is_null() || matches!(value, SqliteValue::Float(number) if number.is_nan())
}

const fn bound_code(op: ConstraintOp) -> Option<u8> {
    match op {
        ConstraintOp::Eq => Some(b'='),
        ConstraintOp::Gt => Some(b'>'),
        ConstraintOp::Ge => Some(b'G'),
        ConstraintOp::Lt => Some(b'<'),
        ConstraintOp::Le => Some(b'L'),
        _ => None,
    }
}

const fn bound_flag(code: u8) -> Option<i32> {
    match code {
        b'=' => Some(EQUAL),
        b'>' => Some(GREATER),
        b'G' => Some(AT_LEAST),
        b'<' => Some(LESS),
        b'L' => Some(AT_MOST),
        _ => None,
    }
}

pub fn best_index(info: &mut IndexInfo) -> Result<()> {
    if info.constraint_usage.len() != info.constraints.len() {
        return Err(invalid_plan());
    }
    info.constraint_usage.fill(Default::default());
    info.idx_num = 0;
    info.idx_str = None;
    info.order_by_consumed = false;
    info.estimated_cost = 1.0e12;
    info.estimated_rows = i64::MAX;

    // Keep the unplanned positional protocol for table-function callers that
    // pass their 1..=3 arguments directly to filter(0, None, args). Without a
    // usable start equality, do not consume unrelated WHERE arguments as START.
    let hidden = [1, 2, 3].map(|column| {
        info.constraints.iter().any(|constraint| {
            constraint.usable && constraint.column == column && constraint.op == ConstraintOp::Eq
        })
    });
    let unavailable_parameter = info.constraints.iter().any(|constraint| {
        let slot = match constraint.column { 1 => 0, 2 => 1, 3 => 2, _ => return false };
        constraint.op == ConstraintOp::Eq && !constraint.usable && !hidden[slot]
    });
    if !hidden[0] || unavailable_parameter {
        // An unavailable correlated STOP/STEP is not an omitted argument.
        // Preserve positional routing instead of inventing a default and
        // claiming a partially bound plan or ordering guarantee.
        return Ok(());
    }

    let slots = [
        (START, 1, ConstraintOp::Eq),
        (STOP, 2, ConstraintOp::Eq),
        (STEP, 3, ConstraintOp::Eq),
        (EQUAL, 0, ConstraintOp::Eq),
        (GREATER, 0, ConstraintOp::Gt),
        (AT_LEAST, 0, ConstraintOp::Ge),
        (LESS, 0, ConstraintOp::Lt),
        (AT_MOST, 0, ConstraintOp::Le),
    ];
    let mut argument = 0_i32;
    info.idx_num = PLANNED;
    for (flag, column, op) in slots {
        let selected = info.constraints.iter().position(|constraint| {
            constraint.usable && constraint.op == op
                && (constraint.column == column || (column == 0 && constraint.column == -1))
        });
        if let Some(index) = selected {
            argument += 1;
            info.constraint_usage[index].argv_index = argument;
            // Hidden arguments define the series, including integer coercion
            // and zero-step normalization. Keep visible comparisons residual:
            // their SQL affinity/collation semantics remain owned by the core.
            info.constraint_usage[index].omit = column != 0;
            info.idx_num |= flag;
        }
    }
    // The bitmask describes one argument per operator. Append any additional
    // VALUE/rowid comparisons after that canonical prefix and describe their
    // operators in the plan string. A loose first bound must not hide a much
    // tighter duplicate or a contradiction. Hidden duplicates stay residual.
    let mut name = PLAN_NAME.to_owned();
    for (index, constraint) in info.constraints.iter().enumerate() {
        if !constraint.usable || !matches!(constraint.column, -1 | 0)
            || info.constraint_usage[index].argv_index != 0
        {
            continue;
        }
        if let Some(code) = bound_code(constraint.op) {
            argument = argument.checked_add(1).ok_or(FrankenError::TooBig)?;
            info.constraint_usage[index].argv_index = argument;
            if name.len() == PLAN_NAME.len() { name.push(':'); }
            name.push(char::from(code));
        }
    }
    info.idx_str = Some(name);
    if let [order] = info.order_by.as_slice()
        && matches!(order.column, -1 | 0)
    {
        info.idx_num |= if order.desc { DESCENDING } else { ASCENDING };
        info.order_by_consumed = true;
    }
    if info.idx_num & EQUAL != 0 {
        info.estimated_cost = 1.0;
        info.estimated_rows = 1;
    } else if info.idx_num & STOP != 0 {
        info.estimated_cost = 10.0;
        info.estimated_rows = 1000;
    } else {
        info.estimated_cost = 1.0e9;
        info.estimated_rows = GENERATE_SERIES_DEFAULT_STOP;
    }
    Ok(())
}

pub fn filter(
    cursor: &mut GenerateSeriesCursor,
    cx: &Cx,
    index: i32,
    name: Option<&str>,
    args: &[SqliteValue],
) -> Result<()> {
    // An unsuccessful re-filter must not leave the preceding row visible.
    cursor.done = true;
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
    if index == 0 && name.is_none() {
        if args.is_empty() {
            return Err(missing_start());
        }
        if args.len() > 3 {
            return Err(invalid_plan());
        }
        if args.iter().any(is_sql_null) {
            return Ok(());
        }
        return cursor.init(
            args[0].to_integer(),
            args.get(1).map_or(GENERATE_SERIES_DEFAULT_STOP, SqliteValue::to_integer),
            args.get(2).map_or(1, SqliteValue::to_integer),
        );
    }
    let extra_bounds = if name == Some(PLAN_NAME) {
        &[][..]
    } else {
        name.and_then(|name| name.strip_prefix(PLAN_NAME))
            .and_then(|suffix| suffix.strip_prefix(':'))
            .filter(|suffix| !suffix.is_empty())
            .ok_or_else(invalid_plan)?.as_bytes()
    };
    let base_arguments = usize::try_from((index & ARGUMENTS).count_ones())
        .map_err(|_| invalid_plan())?;
    if index & (PLANNED | START) != (PLANNED | START)
        || index & !(ARGUMENTS | PLANNED | ASCENDING | DESCENDING) != 0
        || index & (ASCENDING | DESCENDING) == (ASCENDING | DESCENDING)
        || base_arguments.checked_add(extra_bounds.len()) != Some(args.len())
        || extra_bounds.iter().any(|code| {
            bound_flag(*code).is_none_or(|flag| index & flag == 0)
        })
    {
        return Err(invalid_plan());
    }
    // All selected predicates are ordinary comparisons, so NULL cannot match.
    if args.iter().any(is_sql_null) {
        return Ok(());
    }
    let mut position = 0;
    let mut argument = |flag: i32| {
        if index & flag == 0 {
            None
        } else {
            let value = &args[position];
            position += 1;
            Some(value)
        }
    };
    let start = argument(START).ok_or_else(missing_start)?.to_integer();
    let stop = argument(STOP).map_or(GENERATE_SERIES_DEFAULT_STOP, SqliteValue::to_integer);
    let step = argument(STEP).map_or(1, SqliteValue::to_integer);
    let mut lower = i128::from(i64::MIN);
    let mut upper = i128::from(i64::MAX);
    for flag in [EQUAL, GREATER, AT_LEAST, LESS, AT_MOST] {
        if let Some(value) = argument(flag) {
            intersect_value_bound(&mut lower, &mut upper, flag, value);
        }
    }
    for (code, value) in extra_bounds.iter().zip(&args[base_arguments..]) {
        let flag = bound_flag(*code).ok_or_else(invalid_plan)?;
        intersect_value_bound(&mut lower, &mut upper, flag, value);
    }
    cursor.init(start, stop, step)?;
    intersect_sequence(cursor, lower, upper, index)
}

fn intersect_value_bound(lower: &mut i128, upper: &mut i128, flag: i32, value: &SqliteValue) {
    match value {
        SqliteValue::Integer(value) => intersect_integer_bound(lower, upper, flag, i128::from(*value)),
        SqliteValue::Float(value) => intersect_real_bound(lower, upper, flag, *value),
        // The core rechecks every visible comparison. Never guess TEXT/BLOB
        // affinity, including when an opaque operand precedes a numeric bound.
        _ => {}
    }
}

fn intersect_integer_bound(lower: &mut i128, upper: &mut i128, flag: i32, value: i128) {
    match flag {
        EQUAL => { *lower = (*lower).max(value); *upper = (*upper).min(value); }
        GREATER => *lower = (*lower).max(value + 1),
        AT_LEAST => *lower = (*lower).max(value),
        LESS => *upper = (*upper).min(value - 1),
        AT_MOST => *upper = (*upper).min(value),
        _ => unreachable!("only value-bound plan bits reach interval intersection"),
    }
}

/// Convert a REAL comparison to exact integer endpoints. Never round an i64
/// sequence member to f64: above 2^53 adjacent members can compare differently
/// even though converting them to f64 would produce the same number.
#[allow(clippy::cast_possible_truncation)] // Only rounded finite values in the checked i64 domain are cast.
fn intersect_real_bound(lower: &mut i128, upper: &mut i128, flag: i32, value: f64) {
    const MINIMUM: f64 = -9_223_372_036_854_775_808.0;
    const END_EXCLUSIVE: f64 = 9_223_372_036_854_775_808.0;
    if value.is_nan() {
        *lower = 1;
        *upper = 0;
    } else if value >= END_EXCLUSIVE {
        // Includes +infinity. A one-past-domain sentinel has the same ordering
        // as every larger REAL against every possible generated integer.
        intersect_integer_bound(lower, upper, flag, i128::from(i64::MAX) + 1);
    } else if value < MINIMUM {
        // Includes -infinity; never let a saturating cast invent MIN as a match.
        intersect_integer_bound(lower, upper, flag, i128::from(i64::MIN) - 1);
    } else {
        let floor = value.floor() as i128;
        let ceil = value.ceil() as i128;
        match flag {
            EQUAL if floor != ceil => { *lower = 1; *upper = 0; }
            EQUAL => intersect_integer_bound(lower, upper, EQUAL, floor),
            GREATER => intersect_integer_bound(lower, upper, AT_LEAST, floor + 1),
            AT_LEAST => intersect_integer_bound(lower, upper, AT_LEAST, ceil),
            LESS => intersect_integer_bound(lower, upper, AT_MOST, ceil - 1),
            AT_MOST => intersect_integer_bound(lower, upper, AT_MOST, floor),
            _ => unreachable!("only value-bound plan bits reach REAL intersection"),
        }
    }
}

fn intersect_sequence(cursor: &mut GenerateSeriesCursor, lower: i128, upper: i128, index: i32) -> Result<()> {
    if cursor.done || lower > upper {
        cursor.done = true;
        return Ok(());
    }
    let start = i128::from(cursor.start);
    let stop = i128::from(cursor.stop);
    let step = i128::from(cursor.step);
    let stride = step.abs();
    let maximum = (stop - start).abs() / stride;
    // div_euclid rounds negative quotients DOWN. Rust's truncating division
    // would include an out-of-range row when a bound falls before the origin.
    let ceil = |value: i128| -(-value).div_euclid(stride);
    let (first, last) = if step > 0 {
        (ceil(lower - start).max(0), (upper - start).div_euclid(stride).min(maximum))
    } else {
        (ceil(start - upper).max(0), (start - lower).div_euclid(stride).min(maximum))
    };
    if first > last {
        cursor.done = true;
        return Ok(());
    }
    let reverse = (index & ASCENDING != 0 && step < 0)
        || (index & DESCENDING != 0 && step > 0);
    let (first, last) = if reverse { (last, first) } else { (first, last) };
    // Indices are intersected with both the original sequence and the i64
    // domain. The conversions are checked, including full-domain spans.
    cursor.current = i64::try_from(start + first * step).map_err(|_| invalid_plan())?;
    cursor.scan_end = i64::try_from(start + last * step).map_err(|_| invalid_plan())?;
    cursor.scan_step = if reverse { -step } else { step };
    Ok(())
}

pub fn next(cursor: &mut GenerateSeriesCursor, cx: &Cx) -> Result<()> {
    if cursor.done {
        return Ok(());
    }
    if cx.checkpoint().is_err() {
        cursor.done = true;
        return Err(FrankenError::Interrupt);
    }
    let next = i128::from(cursor.current) + cursor.scan_step;
    match i64::try_from(next) {
        Ok(value) => {
            cursor.current = value;
            cursor.done = if cursor.scan_step > 0 {
                value > cursor.scan_end
            } else {
                value < cursor.scan_end
            };
        }
        Err(_) => cursor.done = true,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GenerateSeriesTable;
    use fsqlite_func::vtab::{ColumnContext, IndexConstraint, IndexOrderBy, VirtualTable, VirtualTableCursor};

    fn constraint(column: i32, op: ConstraintOp) -> IndexConstraint {
        IndexConstraint { column, op, usable: true }
    }

    fn planned(
        constraints: Vec<IndexConstraint>, values: &[SqliteValue], order: Vec<IndexOrderBy>,
    ) -> (GenerateSeriesCursor, IndexInfo) {
        assert_eq!(constraints.len(), values.len());
        let mut info = IndexInfo::new(constraints, order);
        GenerateSeriesTable.best_index(&mut info).unwrap();
        let mut indexed = info.constraint_usage.iter().enumerate()
            .filter(|(_, usage)| usage.argv_index > 0)
            .map(|(index, usage)| (usage.argv_index, values[index].clone()))
            .collect::<Vec<_>>();
        indexed.sort_by_key(|(index, _)| *index);
        let args = indexed.into_iter().map(|(_, value)| value).collect::<Vec<_>>();
        let mut cursor = GenerateSeriesTable.open().unwrap();
        cursor.filter(&Cx::new(), info.idx_num, info.idx_str.as_deref(), &args).unwrap();
        (cursor, info)
    }

    fn values(mut cursor: GenerateSeriesCursor) -> Vec<i64> {
        let mut result = Vec::new();
        while !cursor.eof() {
            assert!(result.len() < 100, "range pushdown failed to bound the scan");
            result.push(cursor.rowid().unwrap());
            cursor.next(&Cx::new()).unwrap();
        }
        let mut column = ColumnContext::new();
        cursor.column(&mut column, 0).unwrap();
        assert_eq!(column.take_value(), Some(SqliteValue::Null));
        assert_eq!(cursor.rowid().unwrap(), 0);
        result
    }

    fn range(start: i64, stop: i64, step: i64, lower: i64, upper: i64, desc: Option<bool>) -> GenerateSeriesCursor {
        planned(
            vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq),
                constraint(3, ConstraintOp::Eq), constraint(0, ConstraintOp::Ge), constraint(0, ConstraintOp::Le)],
            &[start, stop, step, lower, upper].map(SqliteValue::Integer),
            desc.map(|desc| IndexOrderBy { column: 0, desc }).into_iter().collect(),
        ).0
    }

    #[test]
    fn hidden_arguments_are_bound_by_column_not_predicate_order() {
        let (cursor, info) = planned(
            vec![constraint(2, ConstraintOp::Eq), constraint(0, ConstraintOp::Ge),
                constraint(3, ConstraintOp::Eq), constraint(1, ConstraintOp::Eq)],
            &[11, 4, 2, 1].map(SqliteValue::Integer), Vec::new(),
        );
        assert_eq!(info.constraint_usage.iter().map(|usage| usage.argv_index).collect::<Vec<_>>(), vec![2, 4, 3, 1]);
        assert!(!info.constraint_usage[1].omit);
        assert!(info.constraint_usage[0].omit);
        assert_eq!(values(cursor), vec![5, 7, 9, 11]);
    }

    #[test]
    fn duplicate_unusable_and_unsupported_constraints_remain_residual() {
        let constraints = vec![constraint(1, ConstraintOp::Eq), constraint(1, ConstraintOp::Eq),
            IndexConstraint { column: 2, op: ConstraintOp::Eq, usable: false },
            constraint(0, ConstraintOp::Ne), constraint(0, ConstraintOp::Gt)];
        let original = constraints.clone();
        let mut info = IndexInfo::new(constraints, Vec::new());
        best_index(&mut info).unwrap();
        assert_eq!(info.constraints, original);
        assert_eq!(info.constraint_usage.iter().map(|usage| usage.argv_index).collect::<Vec<_>>(), vec![0, 0, 0, 0, 0]);
        assert!(info.constraint_usage[1..].iter().all(|usage| !usage.omit));
        // Repeated planning must not retain old mappings or ordering promises.
        info.constraints[0].usable = false;
        info.constraints[1].usable = false;
        best_index(&mut info).unwrap();
        assert_eq!(info.idx_num, 0);
        assert!(info.constraint_usage.iter().all(|usage| usage.argv_index == 0 && !usage.omit));
    }

    #[test]
    fn bounds_without_start_do_not_masquerade_as_positional_arguments() {
        let mut info = IndexInfo::new(vec![constraint(0, ConstraintOp::Ge)],
            vec![IndexOrderBy { column: 0, desc: true }]);
        best_index(&mut info).unwrap();
        assert_eq!(info.idx_num, 0);
        assert!(!info.order_by_consumed);
        assert_eq!(info.constraint_usage[0].argv_index, 0);
        assert!(info.estimated_cost >= 1.0e9);
    }

    #[test]
    fn unavailable_hidden_parameters_are_not_replaced_with_defaults() {
        for column in [2, 3] {
            let mut info = IndexInfo::new(
                vec![constraint(1, ConstraintOp::Eq),
                    IndexConstraint { column, op: ConstraintOp::Eq, usable: false }],
                vec![IndexOrderBy { column: 0, desc: true }],
            );
            best_index(&mut info).unwrap();
            assert_eq!(info.idx_num, 0);
            assert!(info.idx_str.is_none());
            assert!(!info.order_by_consumed);
            assert!(info.constraint_usage.iter().all(|usage| usage.argv_index == 0 && !usage.omit));
            // A usable equality for the same hidden column can bind it; the
            // unavailable duplicate is still evaluated by the core.
            info.constraints.push(constraint(column, ConstraintOp::Eq));
            info.constraint_usage.push(Default::default());
            best_index(&mut info).unwrap();
            assert_ne!(info.idx_num & PLANNED, 0);
            assert_eq!(info.constraint_usage[1].argv_index, 0);
            assert!(!info.constraint_usage[1].omit);
            assert_eq!(info.constraint_usage[2].argv_index, 2);
        }
    }

    #[test]
    fn duplicate_visible_bounds_intersect_but_hidden_duplicates_stay_residual() {
        let (cursor, info) = planned(
            vec![constraint(1, ConstraintOp::Eq), constraint(1, ConstraintOp::Eq),
                constraint(2, ConstraintOp::Eq), constraint(0, ConstraintOp::Ge),
                constraint(0, ConstraintOp::Ge), constraint(0, ConstraintOp::Le)],
            &[0, 2, 10, 4, 6, 5].map(SqliteValue::Integer), Vec::new(),
        );
        assert_eq!(info.constraint_usage.iter().map(|usage| usage.argv_index).collect::<Vec<_>>(),
            vec![1, 0, 2, 3, 5, 4]);
        assert!(!info.constraint_usage[1].omit);
        assert!(info.constraint_usage[3..].iter().all(|usage| !usage.omit));
        assert_eq!(info.idx_str.as_deref(), Some("series-range-v1:G"));
        assert!(cursor.eof(), "the second lower bound contradicts the upper bound");
    }

    #[test]
    fn repeated_value_and_rowid_bounds_jump_directly_in_either_order() {
        for reverse in [false, true] {
            let mut constraints = vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq),
                constraint(0, ConstraintOp::Ge), constraint(-1, ConstraintOp::Ge),
                constraint(0, ConstraintOp::Le), constraint(-1, ConstraintOp::Le)];
            let mut operands = [i64::MIN, i64::MAX, i64::MIN, 41, i64::MAX, 43]
                .map(SqliteValue::Integer).to_vec();
            if reverse { constraints.reverse(); operands.reverse(); }
            let (cursor, info) = planned(constraints, &operands,
                vec![IndexOrderBy { column: -1, desc: true }]);
            assert!(info.order_by_consumed);
            assert_eq!(cursor.current, 43, "filter must not walk the discarded prefix");
            assert_eq!(values(cursor), vec![43, 42, 41]);
        }
        // Every repeat is represented, not just the second of each operator.
        let mut constraints = vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq)];
        let mut operands = vec![SqliteValue::Integer(0), SqliteValue::Integer(100)];
        for lower in 0..=99 {
            constraints.push(constraint(0, ConstraintOp::Ge));
            operands.push(SqliteValue::Integer(lower));
        }
        let (cursor, _) = planned(constraints, &operands, Vec::new());
        assert_eq!(values(cursor), vec![99, 100]);
    }

    #[test]
    fn opaque_and_null_duplicate_operands_do_not_mask_numeric_bounds() {
        for opaque in [SqliteValue::Text("0".into()), SqliteValue::Blob(vec![b'0'].into())] {
            let (cursor, info) = planned(
                vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq),
                    constraint(0, ConstraintOp::Ge), constraint(0, ConstraintOp::Ge)],
                &[SqliteValue::Integer(0), SqliteValue::Integer(5), opaque, SqliteValue::Float(3.5)],
                Vec::new(),
            );
            assert!(info.constraint_usage[2..].iter().all(|usage| !usage.omit));
            assert_eq!(values(cursor), vec![4, 5]);
        }
        for null in [SqliteValue::Null, SqliteValue::Float(f64::NAN)] {
            let (cursor, _) = planned(
                vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq),
                    constraint(0, ConstraintOp::Eq), constraint(-1, ConstraintOp::Eq)],
                &[SqliteValue::Integer(0), SqliteValue::Integer(5), SqliteValue::Integer(3), null],
                Vec::new(),
            );
            assert!(cursor.eof());
        }
    }

    #[test]
    fn extra_bound_shape_is_validated_before_the_null_shortcut() {
        for (index, name, count) in [
            (PLANNED | START | EQUAL, "series-range-v1:", 2),
            (PLANNED | START | EQUAL, "series-range-v1:?", 3),
            (PLANNED | START | EQUAL, "series-range-v1:G", 3),
            (PLANNED | START | EQUAL, "series-range-v1:=", 2),
            (PLANNED | START | EQUAL, "series-range-v1:=", 4),
            (PLANNED | START | EQUAL, "series-range-v1:=G", 4),
        ] {
            let mut cursor = GenerateSeriesTable.open().unwrap();
            cursor.init(1, 3, 1).unwrap();
            assert!(cursor.filter(&Cx::new(), index, Some(name), &vec![SqliteValue::Null; count]).is_err());
            assert!(cursor.eof());
            assert_eq!(cursor.rowid().unwrap(), 0);
        }
    }

    #[test]
    fn bounded_default_stop_jumps_over_billions_of_excluded_values() {
        let (cursor, _) = planned(
            vec![constraint(1, ConstraintOp::Eq), constraint(0, ConstraintOp::Ge)],
            &[SqliteValue::Integer(0), SqliteValue::Integer(4_294_967_292)], Vec::new(),
        );
        assert_eq!(cursor.current, 4_294_967_292);
        assert_eq!(values(cursor), vec![4_294_967_292, 4_294_967_293, 4_294_967_294, 4_294_967_295]);
    }

    #[test]
    fn clipping_and_ordering_preserve_the_original_residue_and_hidden_values() {
        let cursor = range(1, 11, 2, 4, 8, Some(true));
        assert_eq!((cursor.start, cursor.stop, cursor.step), (1, 11, 2));
        assert_eq!(values(cursor), vec![7, 5]);
        assert_eq!(values(range(10, -1, -3, 1, 8, None)), vec![7, 4, 1]);
        assert_eq!(values(range(10, -1, -3, 1, 8, Some(false))), vec![1, 4, 7]);
    }

    #[test]
    fn only_supported_single_column_ordering_is_consumed() {
        for order in [vec![], vec![IndexOrderBy { column: 1, desc: false }],
            vec![IndexOrderBy { column: 0, desc: false }, IndexOrderBy { column: 2, desc: true }]] {
            let mut info = IndexInfo::new(vec![constraint(1, ConstraintOp::Eq)], order.clone());
            best_index(&mut info).unwrap();
            assert!(!info.order_by_consumed);
            assert_eq!(info.order_by, order);
        }
        let mut info = IndexInfo::new(vec![constraint(1, ConstraintOp::Eq)],
            vec![IndexOrderBy { column: -1, desc: true }]);
        best_index(&mut info).unwrap();
        assert!(info.order_by_consumed);
    }

    #[test]
    fn equality_point_probes_respect_step_alignment() {
        for (point, expected) in [(6, vec![]), (7, vec![7])] {
            let (cursor, info) = planned(
                vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq),
                    constraint(3, ConstraintOp::Eq), constraint(0, ConstraintOp::Eq)],
                &[1, i64::MAX, 3, point].map(SqliteValue::Integer), Vec::new(),
            );
            assert_eq!(info.estimated_rows, 1);
            assert_eq!(values(cursor), expected);
        }
    }

    #[test]
    fn strict_bounds_do_not_saturate_into_false_matches_at_i64_extremes() {
        for (op, value) in [(ConstraintOp::Gt, i64::MAX), (ConstraintOp::Lt, i64::MIN)] {
            let (cursor, _) = planned(
                vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq), constraint(0, op)],
                &[i64::MIN, i64::MAX, value].map(SqliteValue::Integer), Vec::new(),
            );
            assert!(cursor.eof());
        }
        assert_eq!(values(range(i64::MIN, i64::MAX, 1, i64::MAX - 1, i64::MAX, Some(true))),
            vec![i64::MAX, i64::MAX - 1]);
    }

    #[test]
    fn negative_minimum_step_can_be_reversed_without_overflow() {
        assert_eq!(values(range(i64::MAX, i64::MIN, i64::MIN, i64::MIN, i64::MAX, Some(false))),
            vec![-1, i64::MAX]);
        assert_eq!(values(range(0, i64::MIN, i64::MIN, i64::MIN, i64::MAX, Some(false))),
            vec![i64::MIN, 0]);
    }

    #[test]
    fn null_arguments_empty_both_positional_and_planned_scans() {
        for null_at in 0..3 {
            let mut args = [SqliteValue::Integer(1), SqliteValue::Integer(5), SqliteValue::Integer(1)];
            args[null_at] = SqliteValue::Null;
            let mut cursor = GenerateSeriesTable.open().unwrap();
            cursor.init(99, 100, 1).unwrap();
            cursor.filter(&Cx::new(), 0, None, &args).unwrap();
            assert!(cursor.eof());
            let (cursor, _) = planned(
                vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq), constraint(3, ConstraintOp::Eq)],
                &args, Vec::new(),
            );
            assert!(cursor.eof());
        }
        let (cursor, _) = planned(vec![constraint(1, ConstraintOp::Eq), constraint(0, ConstraintOp::Gt)],
            &[SqliteValue::Integer(0), SqliteValue::Null], Vec::new());
        assert!(cursor.eof());
    }

    #[test]
    fn invalid_plan_and_cancellation_never_expose_a_stale_row() {
        for (index, name, args) in [
            (PLANNED | START, Some("unknown"), vec![SqliteValue::Integer(1)]),
            (PLANNED | START | STOP, Some(PLAN_NAME), vec![SqliteValue::Integer(1)]),
            (PLANNED | START | ASCENDING | DESCENDING, Some(PLAN_NAME), vec![SqliteValue::Integer(1)]),
            (0, None, vec![SqliteValue::Integer(1); 4]),
        ] {
            let mut cursor = GenerateSeriesTable.open().unwrap();
            cursor.init(1, 9, 1).unwrap();
            assert!(cursor.filter(&Cx::new(), index, name, &args).is_err());
            assert!(cursor.eof());
        }
        let cx = Cx::new();
        cx.cancel();
        let mut cursor = range(1, 10, 1, 1, 10, None);
        assert!(matches!(cursor.next(&cx), Err(FrankenError::Interrupt)));
        assert!(cursor.eof());
        assert!(matches!(cursor.filter(&cx, 0, None, &[SqliteValue::Integer(1)]), Err(FrankenError::Interrupt)));
    }

    #[test]
    fn rowid_bounds_and_zero_step_use_the_value_sequence() {
        let (cursor, _) = planned(
            vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq),
                constraint(3, ConstraintOp::Eq), constraint(-1, ConstraintOp::Gt), constraint(-1, ConstraintOp::Lt)],
            &[0, 10, 0, 4, 8].map(SqliteValue::Integer), Vec::new(),
        );
        assert_eq!(cursor.step, 1);
        assert_eq!(values(cursor), vec![5, 6, 7]);
    }

    fn real_bound(start: i64, stop: i64, op: ConstraintOp, bound: f64) -> GenerateSeriesCursor {
        planned(
            vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq), constraint(0, op)],
            &[SqliteValue::Integer(start), SqliteValue::Integer(stop), SqliteValue::Float(bound)],
            Vec::new(),
        ).0
    }

    #[test]
    #[allow(clippy::float_cmp)] // Exact integer/quarter comparisons are the predicate being tested.
    fn fractional_real_bounds_use_mathematical_floor_and_ceiling() {
        for quarter in -28..=28_i32 {
            let bound = f64::from(quarter) / 4.0;
            for op in [ConstraintOp::Eq, ConstraintOp::Gt, ConstraintOp::Ge, ConstraintOp::Lt, ConstraintOp::Le] {
                let expected = (-6..=6_i32).filter(|value| {
                    let value = f64::from(*value);
                    match op {
                        ConstraintOp::Eq => value == bound,
                        ConstraintOp::Gt => value > bound,
                        ConstraintOp::Ge => value >= bound,
                        ConstraintOp::Lt => value < bound,
                        ConstraintOp::Le => value <= bound,
                        _ => unreachable!(),
                    }
                }).map(i64::from).collect::<Vec<_>>();
                assert_eq!(values(real_bound(-6, 6, op, bound)), expected,
                    "quarter={quarter}, op={op:?}");
            }
        }
    }

    #[test]
    fn real_bound_does_not_round_adjacent_large_integer_rows() {
        const ORIGIN: i64 = 9_007_199_254_740_992;
        assert_eq!(values(real_bound(ORIGIN - 1, ORIGIN + 3, ConstraintOp::Eq, 9_007_199_254_740_992.0)),
            vec![ORIGIN]);
        assert_eq!(values(real_bound(ORIGIN - 1, ORIGIN + 3, ConstraintOp::Gt, 9_007_199_254_740_992.0)),
            vec![ORIGIN + 1, ORIGIN + 2, ORIGIN + 3]);
        assert_eq!(values(real_bound(-ORIGIN - 3, -ORIGIN + 1, ConstraintOp::Lt, -9_007_199_254_740_992.0)),
            vec![-ORIGIN - 3, -ORIGIN - 2, -ORIGIN - 1]);
    }

    #[test]
    fn real_domain_edges_and_infinities_do_not_invent_endpoint_matches() {
        for bound in [9_223_372_036_854_775_808.0, f64::INFINITY] {
            for op in [ConstraintOp::Eq, ConstraintOp::Gt, ConstraintOp::Ge] {
                assert!(real_bound(i64::MAX - 2, i64::MAX, op, bound).eof());
            }
            for op in [ConstraintOp::Lt, ConstraintOp::Le] {
                assert_eq!(values(real_bound(i64::MAX - 2, i64::MAX, op, bound)),
                    vec![i64::MAX - 2, i64::MAX - 1, i64::MAX]);
            }
        }
        for bound in [-9_223_372_036_854_777_856.0, f64::NEG_INFINITY] {
            for op in [ConstraintOp::Eq, ConstraintOp::Lt, ConstraintOp::Le] {
                assert!(real_bound(i64::MIN, i64::MIN + 2, op, bound).eof());
            }
            for op in [ConstraintOp::Gt, ConstraintOp::Ge] {
                assert_eq!(values(real_bound(i64::MIN, i64::MIN + 2, op, bound)),
                    vec![i64::MIN, i64::MIN + 1, i64::MIN + 2]);
            }
        }
        assert_eq!(values(real_bound(i64::MIN, i64::MIN + 2, ConstraintOp::Eq, -9_223_372_036_854_775_808.0)),
            vec![i64::MIN]);
        assert!(real_bound(i64::MIN, i64::MIN + 2, ConstraintOp::Lt, -9_223_372_036_854_775_808.0).eof());
    }

    #[test]
    fn numeric_looking_text_and_blobs_remain_core_comparisons() {
        for value in [SqliteValue::Text("4".into()), SqliteValue::Blob(vec![b'4'].into())] {
            let (cursor, info) = planned(
                vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq), constraint(0, ConstraintOp::Ge)],
                &[SqliteValue::Integer(1), SqliteValue::Integer(5), value], Vec::new(),
            );
            assert!(!info.constraint_usage[2].omit);
            assert_eq!(values(cursor), vec![1, 2, 3, 4, 5], "do not prune on guessed text affinity");
        }
    }

    #[test]
    fn nan_inputs_empty_scans_instead_of_becoming_zero() {
        for nan_at in 0..3 {
            let mut args = [SqliteValue::Integer(1), SqliteValue::Integer(5), SqliteValue::Integer(1)];
            args[nan_at] = SqliteValue::Float(f64::NAN);
            let mut cursor = GenerateSeriesTable.open().unwrap();
            cursor.filter(&Cx::new(), 0, None, &args).unwrap();
            assert!(cursor.eof());
            let (cursor, _) = planned(
                vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq), constraint(3, ConstraintOp::Eq)],
                &args, Vec::new(),
            );
            assert!(cursor.eof());
        }
        for op in [ConstraintOp::Eq, ConstraintOp::Gt, ConstraintOp::Ge, ConstraintOp::Lt, ConstraintOp::Le] {
            assert!(real_bound(0, 5, op, f64::NAN).eof());
        }
    }

    #[test]
    fn intersected_real_bounds_keep_negative_step_alignment_when_reversed() {
        let (cursor, _) = planned(
            vec![constraint(1, ConstraintOp::Eq), constraint(2, ConstraintOp::Eq), constraint(3, ConstraintOp::Eq),
                constraint(0, ConstraintOp::Gt), constraint(0, ConstraintOp::Le)],
            &[SqliteValue::Integer(10), SqliteValue::Integer(-10), SqliteValue::Integer(-3),
                SqliteValue::Float(-4.5), SqliteValue::Float(6.25)],
            vec![IndexOrderBy { column: 0, desc: false }],
        );
        assert_eq!((cursor.start, cursor.stop, cursor.step), (10, -10, -3));
        assert_eq!(values(cursor), vec![-2, 1, 4]);
    }

    #[test]
    fn exhaustive_small_ranges_match_enumeration_in_every_order() {
        for start in -5..=5_i64 {
            for stop in -5..=5_i64 {
                for step in -4..=4_i64 {
                    for lower in -6..=6_i64 {
                        for upper in -6..=6_i64 {
                            let stride = if step == 0 { 1 } else { step };
                            let mut expected = Vec::new();
                            let mut value = start;
                            while if stride > 0 { value <= stop } else { value >= stop } {
                                if value >= lower && value <= upper { expected.push(value); }
                                value += stride;
                            }
                            for desc in [None, Some(false), Some(true)] {
                                let mut ordered = expected.clone();
                                if let Some(desc) = desc {
                                    ordered.sort_unstable();
                                    if desc { ordered.reverse(); }
                                }
                                assert_eq!(values(range(start, stop, step, lower, upper, desc)), ordered,
                                    "start={start} stop={stop} step={step} lower={lower} upper={upper} desc={desc:?}");
                            }
                        }
                    }
                }
            }
        }
    }
}
