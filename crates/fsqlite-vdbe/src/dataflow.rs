//! Weighted-row dataflow substrate for Bloodstream VDBE automata.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use fsqlite_types::SqliteValue;

/// A SQLite row paired with its algebraic multiplicity in a differential stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeightedRow {
    /// Row payload.
    pub values: Vec<SqliteValue>,
    /// Algebraic multiplicity. Inserts are positive, deletes are negative, and
    /// consolidated deltas can carry larger magnitudes.
    pub weight: i64,
}

impl WeightedRow {
    /// Construct a row with an explicit algebraic weight.
    pub fn new(values: Vec<SqliteValue>, weight: i64) -> Self {
        Self { values, weight }
    }

    /// Construct an inserted row (`+1`).
    pub fn insert(values: Vec<SqliteValue>) -> Self {
        Self::new(values, 1)
    }

    /// Construct a deleted row (`-1`).
    pub fn delete(values: Vec<SqliteValue>) -> Self {
        Self::new(values, -1)
    }

    /// Number of values in the row payload.
    pub fn width(&self) -> usize {
        self.values.len()
    }

    /// Whether this delta has no effect and can be elided.
    pub fn is_zero(&self) -> bool {
        self.weight == 0
    }

    fn project(&self, columns: &[usize]) -> DataflowResult<Vec<SqliteValue>> {
        columns
            .iter()
            .map(|&column| {
                self.values
                    .get(column)
                    .cloned()
                    .ok_or(DataflowError::ColumnOutOfBounds {
                        column,
                        width: self.values.len(),
                    })
            })
            .collect()
    }
}

/// Errors surfaced by the weighted-row dataflow automaton.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataflowError {
    /// A requested column index exceeded the row width.
    ColumnOutOfBounds { column: usize, width: usize },
    /// Integer predicate operators only accept integer value inputs.
    PredicateValueNotInteger { column: usize },
    /// Floating-point predicate operators only accept float value inputs.
    PredicateValueNotFloat { column: usize },
    /// Text predicate operators only accept text value inputs.
    PredicateValueNotText { column: usize },
    /// Blob predicate operators only accept blob value inputs.
    PredicateValueNotBlob { column: usize },
    /// Integer aggregate operators only accept integer value inputs.
    AggregateValueNotInteger { column: usize },
    /// Join key mappings must specify the same number of left and right columns.
    JoinKeyArityMismatch { left: usize, right: usize },
    /// Input no longer matches the schema width captured when the automaton was built.
    SchemaChanged {
        expected_width: usize,
        actual_width: usize,
    },
}

impl fmt::Display for DataflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnOutOfBounds { column, width } => {
                write!(f, "column {column} out of bounds for row width {width}")
            }
            Self::PredicateValueNotInteger { column } => {
                write!(f, "predicate value column {column} is not an integer")
            }
            Self::PredicateValueNotFloat { column } => {
                write!(f, "predicate value column {column} is not a float")
            }
            Self::PredicateValueNotText { column } => {
                write!(f, "predicate value column {column} is not text")
            }
            Self::PredicateValueNotBlob { column } => {
                write!(f, "predicate value column {column} is not a blob")
            }
            Self::AggregateValueNotInteger { column } => {
                write!(f, "aggregate value column {column} is not an integer")
            }
            Self::JoinKeyArityMismatch { left, right } => {
                write!(f, "join key arity mismatch: left={left}, right={right}")
            }
            Self::SchemaChanged {
                expected_width,
                actual_width,
            } => write!(
                f,
                "schema changed: expected row width {expected_width}, got {actual_width}"
            ),
        }
    }
}

impl Error for DataflowError {}

type DataflowResult<T> = std::result::Result<T, DataflowError>;

/// Ordered operator sequence for a bounded differential VDBE automaton.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataflowAutomaton {
    expected_input_width: Option<usize>,
    operators: Vec<DataflowOperator>,
}

impl DataflowAutomaton {
    /// Create an automaton with no captured input schema.
    pub fn new(operators: Vec<DataflowOperator>) -> Self {
        Self {
            expected_input_width: None,
            operators,
        }
    }

    /// Create an automaton that fail-closes when input row width changes.
    pub fn with_input_width(expected_input_width: usize, operators: Vec<DataflowOperator>) -> Self {
        Self {
            expected_input_width: Some(expected_input_width),
            operators,
        }
    }

    /// Execute the operator sequence over a weighted-row batch.
    pub fn execute(&self, rows: &[WeightedRow]) -> DataflowResult<Vec<WeightedRow>> {
        tracing::debug!(
            target: "fsqlite_vdbe::dataflow",
            event = "execute",
            operators = self.operators.len(),
            input_rows = rows.len()
        );

        let mut current = self.validate_and_elide_zero_rows(rows)?;
        for operator in &self.operators {
            current = operator.apply(&current)?;
        }

        tracing::debug!(
            target: "fsqlite_vdbe::dataflow",
            event = "execute_complete",
            operators = self.operators.len(),
            output_rows = current.len()
        );
        Ok(current)
    }

    /// Access the operator list.
    pub fn operators(&self) -> &[DataflowOperator] {
        &self.operators
    }

    fn validate_and_elide_zero_rows(
        &self,
        rows: &[WeightedRow],
    ) -> DataflowResult<Vec<WeightedRow>> {
        let mut current = Vec::with_capacity(rows.len());
        for row in rows {
            if row.is_zero() {
                continue;
            }
            if let Some(expected_width) = self.expected_input_width
                && row.width() != expected_width
            {
                return Err(DataflowError::SchemaChanged {
                    expected_width,
                    actual_width: row.width(),
                });
            }
            current.push(row.clone());
        }
        Ok(current)
    }
}

/// Primitive weighted-row operators that preserve algebraic multiplicity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataflowOperator {
    /// Keep only rows where `column == value`.
    FilterEq { column: usize, value: SqliteValue },
    /// Keep rows whose column value appears in the configured set.
    FilterInSet {
        column: usize,
        values: Vec<SqliteValue>,
    },
    /// Keep rows whose column value does not appear in the configured set.
    FilterNotInSet {
        column: usize,
        values: Vec<SqliteValue>,
    },
    /// Keep rows whose blob column starts with the configured byte prefix.
    FilterBlobPrefix { column: usize, prefix: Vec<u8> },
    /// Keep rows whose blob column ends with the configured byte suffix.
    FilterBlobSuffix { column: usize, suffix: Vec<u8> },
    /// Keep rows whose blob column contains the configured byte subsequence.
    FilterBlobContains { column: usize, needle: Vec<u8> },
    /// Keep rows whose blob column does not contain the configured byte subsequence.
    FilterBlobNotContains { column: usize, needle: Vec<u8> },
    /// Keep rows whose text column starts with the configured prefix.
    FilterTextPrefix { column: usize, prefix: String },
    /// Keep rows whose text column ends with the configured suffix.
    FilterTextSuffix { column: usize, suffix: String },
    /// Keep rows whose text column contains the configured substring.
    FilterTextContains { column: usize, needle: String },
    /// Keep rows whose text column does not contain the configured substring.
    FilterTextNotContains { column: usize, needle: String },
    /// Keep rows whose text column length lies in an inclusive character range.
    FilterTextLengthBetween {
        column: usize,
        lower: usize,
        upper: usize,
    },
    /// Keep rows whose text column character length equals the configured length.
    FilterTextLengthExact { column: usize, length: usize },
    /// Keep rows whose text column character length differs from the configured length.
    FilterTextLengthNotExact { column: usize, length: usize },
    /// Keep rows whose blob column byte length lies in an inclusive range.
    FilterBlobLengthBetween {
        column: usize,
        lower: usize,
        upper: usize,
    },
    /// Keep rows whose blob column byte length equals the configured length.
    FilterBlobLengthExact { column: usize, length: usize },
    /// Keep rows whose blob column byte length differs from the configured length.
    FilterBlobLengthNotExact { column: usize, length: usize },
    /// Keep rows whose blob column byte length lies outside an inclusive range.
    FilterBlobLengthNotBetween {
        column: usize,
        lower: usize,
        upper: usize,
    },
    /// Keep rows whose text column length lies outside an inclusive character range.
    FilterTextLengthNotBetween {
        column: usize,
        lower: usize,
        upper: usize,
    },
    /// Keep rows where an integer column lies in an inclusive range.
    FilterIntegerBetween {
        column: usize,
        lower: i64,
        upper: i64,
    },
    /// Keep rows where an integer column lies outside an inclusive range.
    FilterIntegerNotBetween {
        column: usize,
        lower: i64,
        upper: i64,
    },
    /// Keep rows whose floating-point column is finite.
    FilterFloatFinite { column: usize },
    /// Keep rows whose floating-point column is NaN or infinite.
    FilterFloatNonFinite { column: usize },
    /// Keep rows whose floating-point column matches the requested sign.
    FilterFloatSign { column: usize, sign: FloatSign },
    /// Keep rows where a floating-point column satisfies the comparison.
    FilterFloatCompare {
        column: usize,
        op: FloatComparison,
        value: SqliteValue,
    },
    /// Keep rows where a floating-point column lies in an inclusive range.
    FilterFloatBetween {
        column: usize,
        lower: SqliteValue,
        upper: SqliteValue,
    },
    /// Keep rows where a floating-point column lies outside an inclusive range.
    FilterFloatNotBetween {
        column: usize,
        lower: SqliteValue,
        upper: SqliteValue,
    },
    /// Keep rows where an integer column satisfies the comparison.
    FilterIntegerCompare {
        column: usize,
        op: IntegerComparison,
        value: i64,
    },
    /// Keep rows whose column nullness matches the requested predicate.
    FilterNull {
        column: usize,
        predicate: NullPredicate,
    },
    /// Keep only the requested columns, preserving row weight.
    Project { columns: Vec<usize> },
    /// Keep rows whose algebraic weight matches the requested sign.
    FilterWeightSign { sign: WeightSign },
    /// Append a constant payload value to each row, preserving row weight.
    AppendLiteral { value: SqliteValue },
    /// Consolidate algebraic weights by key and elide zero-weight results.
    ConsolidateByKey { key_columns: Vec<usize> },
    /// Emit one materialized `COUNT(*)` row per key.
    CountByKey { key_columns: Vec<usize> },
    /// Emit one materialized integer `SUM(value_column)` row per key.
    SumIntegerByKey {
        key_columns: Vec<usize>,
        value_column: usize,
    },
    /// Emit one materialized integer `MIN(value_column)` row per key.
    MinIntegerByKey {
        key_columns: Vec<usize>,
        value_column: usize,
    },
    /// Emit one materialized integer `MAX(value_column)` row per key.
    MaxIntegerByKey {
        key_columns: Vec<usize>,
        value_column: usize,
    },
    /// Emit one materialized floating-point `AVG(value_column)` row per key.
    AverageIntegerByKey {
        key_columns: Vec<usize>,
        value_column: usize,
    },
    /// Consolidate algebraic weights by complete row value.
    ConsolidateRows,
    /// Consolidate identical rows, then collapse surviving weights to their sign.
    ConsolidateWeightSigns,
    /// Multiply every row weight by `factor`, eliding zero-weight output rows.
    ScaleWeight { factor: i64 },
    /// Reverse every row's algebraic delta polarity.
    NegateWeight,
    /// Collapse every non-zero row weight to its algebraic sign.
    NormalizeWeightSign,
    /// Add a static weighted-row batch to the current stream.
    AddRows { rows: Vec<WeightedRow> },
    /// Subtract a static weighted-row batch from the current stream.
    SubtractRows { rows: Vec<WeightedRow> },
    /// Keep current stream rows whose values are present in a static batch.
    RetainRowsIn { rows: Vec<WeightedRow> },
    /// Drop current stream rows whose values are present in a static batch.
    RejectRowsIn { rows: Vec<WeightedRow> },
    /// Rewrite every non-zero input row to a fixed output weight.
    SetWeight { weight: i64 },
    /// Keep rows with positive accumulated weight as set membership.
    ThresholdPositive,
    /// Append each row's algebraic weight as an integer payload column for emission.
    AppendWeightColumn,
    /// Append each row's algebraic weight sign as an integer payload column.
    AppendWeightSignColumn,
    /// Append each row's saturated algebraic weight magnitude as an integer payload column.
    AppendWeightMagnitudeColumn,
    /// Compute `DeltaLeft JOIN Right`, with the current stream as the delta-left input.
    DeltaJoinLeft {
        stable_right: Vec<WeightedRow>,
        key_spec: JoinKeySpec,
    },
    /// Compute `Left JOIN DeltaRight`, with the current stream as the delta-right input.
    DeltaJoinRight {
        stable_left: Vec<WeightedRow>,
        key_spec: JoinKeySpec,
    },
    /// Compute the full join delta for simultaneous left and right relation changes.
    DeltaJoinUpdate {
        stable_left: Vec<WeightedRow>,
        stable_right: Vec<WeightedRow>,
        delta_right: Vec<WeightedRow>,
        key_spec: JoinKeySpec,
    },
}

impl DataflowOperator {
    fn apply(&self, rows: &[WeightedRow]) -> DataflowResult<Vec<WeightedRow>> {
        match self {
            Self::FilterEq { column, value } => rows
                .iter()
                .filter_map(|row| match row.values.get(*column) {
                    Some(candidate) if candidate == value => Some(Ok(row.clone())),
                    Some(_) => None,
                    None => Some(Err(DataflowError::ColumnOutOfBounds {
                        column: *column,
                        width: row.width(),
                    })),
                })
                .collect(),
            Self::FilterInSet { column, values } => filter_in_set(rows, *column, values),
            Self::FilterNotInSet { column, values } => filter_not_in_set(rows, *column, values),
            Self::FilterBlobPrefix { column, prefix } => filter_blob_prefix(rows, *column, prefix),
            Self::FilterBlobSuffix { column, suffix } => filter_blob_suffix(rows, *column, suffix),
            Self::FilterBlobContains { column, needle } => {
                filter_blob_contains(rows, *column, needle)
            }
            Self::FilterBlobNotContains { column, needle } => {
                filter_blob_not_contains(rows, *column, needle)
            }
            Self::FilterTextPrefix { column, prefix } => filter_text_prefix(rows, *column, prefix),
            Self::FilterTextSuffix { column, suffix } => filter_text_suffix(rows, *column, suffix),
            Self::FilterTextContains { column, needle } => {
                filter_text_contains(rows, *column, needle)
            }
            Self::FilterTextNotContains { column, needle } => {
                filter_text_not_contains(rows, *column, needle)
            }
            Self::FilterTextLengthBetween {
                column,
                lower,
                upper,
            } => filter_text_length_between(rows, *column, *lower, *upper),
            Self::FilterTextLengthExact { column, length } => {
                filter_text_length_exact(rows, *column, *length)
            }
            Self::FilterTextLengthNotExact { column, length } => {
                filter_text_length_not_exact(rows, *column, *length)
            }
            Self::FilterBlobLengthBetween {
                column,
                lower,
                upper,
            } => filter_blob_length_between(rows, *column, *lower, *upper),
            Self::FilterBlobLengthExact { column, length } => {
                filter_blob_length_exact(rows, *column, *length)
            }
            Self::FilterBlobLengthNotExact { column, length } => {
                filter_blob_length_not_exact(rows, *column, *length)
            }
            Self::FilterBlobLengthNotBetween {
                column,
                lower,
                upper,
            } => filter_blob_length_not_between(rows, *column, *lower, *upper),
            Self::FilterTextLengthNotBetween {
                column,
                lower,
                upper,
            } => filter_text_length_not_between(rows, *column, *lower, *upper),
            Self::FilterIntegerBetween {
                column,
                lower,
                upper,
            } => filter_integer_between(rows, *column, *lower, *upper),
            Self::FilterIntegerNotBetween {
                column,
                lower,
                upper,
            } => filter_integer_not_between(rows, *column, *lower, *upper),
            Self::FilterFloatFinite { column } => filter_float_finite(rows, *column),
            Self::FilterFloatNonFinite { column } => filter_float_non_finite(rows, *column),
            Self::FilterFloatSign { column, sign } => filter_float_sign(rows, *column, *sign),
            Self::FilterFloatCompare { column, op, value } => {
                filter_float_compare(rows, *column, *op, value)
            }
            Self::FilterFloatBetween {
                column,
                lower,
                upper,
            } => filter_float_between(rows, *column, lower, upper),
            Self::FilterFloatNotBetween {
                column,
                lower,
                upper,
            } => filter_float_not_between(rows, *column, lower, upper),
            Self::FilterIntegerCompare { column, op, value } => {
                filter_integer_compare(rows, *column, *op, *value)
            }
            Self::FilterNull { column, predicate } => filter_null(rows, *column, *predicate),
            Self::Project { columns } => rows
                .iter()
                .map(|row| Ok(WeightedRow::new(row.project(columns)?, row.weight)))
                .collect(),
            Self::FilterWeightSign { sign } => Ok(filter_weight_sign(rows, *sign)),
            Self::AppendLiteral { value } => Ok(append_literal_column(rows, value)),
            Self::ConsolidateByKey { key_columns } => consolidate_by_key(rows, key_columns),
            Self::CountByKey { key_columns } => count_by_key(rows, key_columns),
            Self::SumIntegerByKey {
                key_columns,
                value_column,
            } => sum_integer_by_key(rows, key_columns, *value_column),
            Self::MinIntegerByKey {
                key_columns,
                value_column,
            } => min_integer_by_key(rows, key_columns, *value_column),
            Self::MaxIntegerByKey {
                key_columns,
                value_column,
            } => max_integer_by_key(rows, key_columns, *value_column),
            Self::AverageIntegerByKey {
                key_columns,
                value_column,
            } => average_integer_by_key(rows, key_columns, *value_column),
            Self::ConsolidateRows => Ok(consolidate_rows(rows.to_vec())),
            Self::ConsolidateWeightSigns => Ok(consolidate_weight_signs(rows)),
            Self::ScaleWeight { factor } => Ok(scale_weights(rows, *factor)),
            Self::NegateWeight => Ok(negate_weights(rows)),
            Self::NormalizeWeightSign => Ok(normalize_weight_sign(rows)),
            Self::AddRows { rows: right } => Ok(add_rows(rows, right)),
            Self::SubtractRows { rows: right } => Ok(subtract_rows(rows, right)),
            Self::RetainRowsIn { rows: candidates } => Ok(retain_rows_in(rows, candidates)),
            Self::RejectRowsIn { rows: candidates } => Ok(reject_rows_in(rows, candidates)),
            Self::SetWeight { weight } => Ok(set_weights(rows, *weight)),
            Self::ThresholdPositive => Ok(threshold_positive(rows)),
            Self::AppendWeightColumn => Ok(append_weight_column(rows)),
            Self::AppendWeightSignColumn => Ok(append_weight_sign_column(rows)),
            Self::AppendWeightMagnitudeColumn => Ok(append_weight_magnitude_column(rows)),
            Self::DeltaJoinLeft {
                stable_right,
                key_spec,
            } => delta_join_left(rows, stable_right, key_spec),
            Self::DeltaJoinRight {
                stable_left,
                key_spec,
            } => delta_join_right(stable_left, rows, key_spec),
            Self::DeltaJoinUpdate {
                stable_left,
                stable_right,
                delta_right,
                key_spec,
            } => delta_join_update(stable_left, rows, stable_right, delta_right, key_spec),
        }
    }
}

/// Integer predicate comparison used by dataflow filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegerComparison {
    /// Match values strictly below the threshold.
    LessThan,
    /// Match values at or below the threshold.
    LessOrEqual,
    /// Match values strictly above the threshold.
    GreaterThan,
    /// Match values at or above the threshold.
    GreaterOrEqual,
}

impl IntegerComparison {
    fn matches(self, candidate: i64, threshold: i64) -> bool {
        match self {
            Self::LessThan => candidate < threshold,
            Self::LessOrEqual => candidate <= threshold,
            Self::GreaterThan => candidate > threshold,
            Self::GreaterOrEqual => candidate >= threshold,
        }
    }
}

/// Floating-point predicate comparison used by dataflow filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloatComparison {
    /// Match values strictly below the threshold.
    LessThan,
    /// Match values at or below the threshold.
    LessOrEqual,
    /// Match values strictly above the threshold.
    GreaterThan,
    /// Match values at or above the threshold.
    GreaterOrEqual,
}

impl FloatComparison {
    fn matches(self, candidate: f64, threshold: f64) -> bool {
        match self {
            Self::LessThan => candidate < threshold,
            Self::LessOrEqual => candidate <= threshold,
            Self::GreaterThan => candidate > threshold,
            Self::GreaterOrEqual => candidate >= threshold,
        }
    }
}

/// Floating-point sign predicate used by dataflow filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloatSign {
    /// Positive finite values and positive infinity.
    Positive,
    /// Negative finite values and negative infinity.
    Negative,
    /// Both `+0.0` and `-0.0`.
    Zero,
}

impl FloatSign {
    fn matches(self, candidate: f64) -> bool {
        match self {
            Self::Positive => candidate > 0.0,
            Self::Negative => candidate < 0.0,
            Self::Zero => candidate == 0.0,
        }
    }
}

/// Null predicate used by dataflow filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullPredicate {
    /// Match `NULL` values.
    IsNull,
    /// Match non-`NULL` values.
    IsNotNull,
}

impl NullPredicate {
    fn matches(self, value: &SqliteValue) -> bool {
        match self {
            Self::IsNull => matches!(value, SqliteValue::Null),
            Self::IsNotNull => !matches!(value, SqliteValue::Null),
        }
    }
}

/// Algebraic weight sign used to split insert and delete delta streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightSign {
    /// Positive-weight rows, commonly insertion deltas.
    Positive,
    /// Negative-weight rows, commonly deletion deltas.
    Negative,
}

/// Column mapping for delta-aware inner joins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinKeySpec {
    /// Key columns read from the left relation.
    pub left_columns: Vec<usize>,
    /// Key columns read from the right relation.
    pub right_columns: Vec<usize>,
}

impl JoinKeySpec {
    /// Construct a join-key spec.
    pub fn new(left_columns: Vec<usize>, right_columns: Vec<usize>) -> Self {
        Self {
            left_columns,
            right_columns,
        }
    }

    fn validate(&self) -> DataflowResult<()> {
        if self.left_columns.len() != self.right_columns.len() {
            return Err(DataflowError::JoinKeyArityMismatch {
                left: self.left_columns.len(),
                right: self.right_columns.len(),
            });
        }
        Ok(())
    }
}

fn consolidate_by_key(
    rows: &[WeightedRow],
    key_columns: &[usize],
) -> DataflowResult<Vec<WeightedRow>> {
    let mut groups: Vec<(Vec<SqliteValue>, i64)> = Vec::new();
    for row in rows {
        if row.is_zero() {
            continue;
        }
        let key = row.project(key_columns)?;
        if let Some((_, weight)) = groups.iter_mut().find(|(candidate, _)| *candidate == key) {
            *weight = weight.saturating_add(row.weight);
        } else {
            groups.push((key, row.weight));
        }
    }
    Ok(groups
        .into_iter()
        .filter_map(|(values, weight)| (weight != 0).then(|| WeightedRow::new(values, weight)))
        .collect())
}

fn filter_weight_sign(rows: &[WeightedRow], sign: WeightSign) -> Vec<WeightedRow> {
    rows.iter()
        .filter_map(|row| match (sign, row.weight) {
            (WeightSign::Positive, weight) if weight > 0 => Some(row.clone()),
            (WeightSign::Negative, weight) if weight < 0 => Some(row.clone()),
            _ => None,
        })
        .collect()
}

fn append_literal_column(rows: &[WeightedRow], value: &SqliteValue) -> Vec<WeightedRow> {
    rows.iter()
        .filter_map(|row| {
            if row.is_zero() {
                return None;
            }
            let mut values = row.values.clone();
            values.push(value.clone());
            Some(WeightedRow::new(values, row.weight))
        })
        .collect()
}

fn filter_in_set(
    rows: &[WeightedRow],
    column: usize,
    values: &[SqliteValue],
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(candidate) if values.iter().any(|value| value == candidate) => {
                Some(Ok(row.clone()))
            }
            Some(_) => None,
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_not_in_set(
    rows: &[WeightedRow],
    column: usize,
    values: &[SqliteValue],
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(candidate) if values.iter().all(|value| value != candidate) => {
                Some(Ok(row.clone()))
            }
            Some(_) => None,
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_blob_prefix(
    rows: &[WeightedRow],
    column: usize,
    prefix: &[u8],
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Blob(candidate)) if candidate.starts_with(prefix) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Blob(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotBlob { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_blob_suffix(
    rows: &[WeightedRow],
    column: usize,
    suffix: &[u8],
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Blob(candidate)) if candidate.ends_with(suffix) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Blob(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotBlob { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_blob_contains(
    rows: &[WeightedRow],
    column: usize,
    needle: &[u8],
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Blob(candidate)) if blob_contains(candidate, needle) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Blob(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotBlob { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_blob_not_contains(
    rows: &[WeightedRow],
    column: usize,
    needle: &[u8],
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Blob(candidate)) if !blob_contains(candidate, needle) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Blob(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotBlob { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn blob_contains(candidate: &[u8], needle: &[u8]) -> bool {
    needle.is_empty()
        || candidate
            .windows(needle.len())
            .any(|window| window == needle)
}

fn filter_text_prefix(
    rows: &[WeightedRow],
    column: usize,
    prefix: &str,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Text(candidate)) if candidate.as_str().starts_with(prefix) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Text(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotText { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_text_suffix(
    rows: &[WeightedRow],
    column: usize,
    suffix: &str,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Text(candidate)) if candidate.as_str().ends_with(suffix) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Text(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotText { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_text_contains(
    rows: &[WeightedRow],
    column: usize,
    needle: &str,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Text(candidate)) if candidate.as_str().contains(needle) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Text(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotText { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_text_not_contains(
    rows: &[WeightedRow],
    column: usize,
    needle: &str,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Text(candidate)) if !candidate.as_str().contains(needle) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Text(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotText { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_text_length_between(
    rows: &[WeightedRow],
    column: usize,
    lower: usize,
    upper: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Text(candidate))
                if (lower..=upper).contains(&candidate.chars().count()) =>
            {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Text(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotText { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_text_length_exact(
    rows: &[WeightedRow],
    column: usize,
    length: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Text(candidate)) if candidate.chars().count() == length => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Text(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotText { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_text_length_not_exact(
    rows: &[WeightedRow],
    column: usize,
    length: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Text(candidate)) if candidate.chars().count() != length => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Text(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotText { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_blob_length_between(
    rows: &[WeightedRow],
    column: usize,
    lower: usize,
    upper: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Blob(candidate)) if (lower..=upper).contains(&candidate.len()) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Blob(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotBlob { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_blob_length_exact(
    rows: &[WeightedRow],
    column: usize,
    length: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Blob(candidate)) if candidate.len() == length => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Blob(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotBlob { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_blob_length_not_exact(
    rows: &[WeightedRow],
    column: usize,
    length: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Blob(candidate)) if candidate.len() != length => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Blob(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotBlob { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_blob_length_not_between(
    rows: &[WeightedRow],
    column: usize,
    lower: usize,
    upper: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Blob(candidate))
                if candidate.len() < lower || upper < candidate.len() =>
            {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Blob(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotBlob { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_text_length_not_between(
    rows: &[WeightedRow],
    column: usize,
    lower: usize,
    upper: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Text(candidate)) => {
                let length = candidate.chars().count();
                (length < lower || upper < length).then(|| Ok(row.clone()))
            }
            Some(_) => Some(Err(DataflowError::PredicateValueNotText { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_integer_between(
    rows: &[WeightedRow],
    column: usize,
    lower: i64,
    upper: i64,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Integer(candidate)) if lower <= *candidate && *candidate <= upper => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Integer(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotInteger { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_integer_not_between(
    rows: &[WeightedRow],
    column: usize,
    lower: i64,
    upper: i64,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Integer(candidate)) if *candidate < lower || upper < *candidate => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Integer(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotInteger { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_float_finite(rows: &[WeightedRow], column: usize) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Float(candidate)) if candidate.is_finite() => Some(Ok(row.clone())),
            Some(SqliteValue::Float(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotFloat { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_float_non_finite(
    rows: &[WeightedRow],
    column: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Float(candidate)) if !candidate.is_finite() => Some(Ok(row.clone())),
            Some(SqliteValue::Float(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotFloat { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_float_sign(
    rows: &[WeightedRow],
    column: usize,
    sign: FloatSign,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Float(candidate)) if sign.matches(*candidate) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Float(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotFloat { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_float_compare(
    rows: &[WeightedRow],
    column: usize,
    op: FloatComparison,
    value: &SqliteValue,
) -> DataflowResult<Vec<WeightedRow>> {
    let SqliteValue::Float(threshold) = value else {
        return Err(DataflowError::PredicateValueNotFloat { column });
    };

    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Float(candidate)) if op.matches(*candidate, *threshold) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Float(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotFloat { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_float_between(
    rows: &[WeightedRow],
    column: usize,
    lower: &SqliteValue,
    upper: &SqliteValue,
) -> DataflowResult<Vec<WeightedRow>> {
    let SqliteValue::Float(lower) = lower else {
        return Err(DataflowError::PredicateValueNotFloat { column });
    };
    let SqliteValue::Float(upper) = upper else {
        return Err(DataflowError::PredicateValueNotFloat { column });
    };
    if lower.is_nan() || upper.is_nan() {
        return Ok(Vec::new());
    }

    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Float(candidate)) if lower <= candidate && candidate <= upper => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Float(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotFloat { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_float_not_between(
    rows: &[WeightedRow],
    column: usize,
    lower: &SqliteValue,
    upper: &SqliteValue,
) -> DataflowResult<Vec<WeightedRow>> {
    let SqliteValue::Float(lower) = lower else {
        return Err(DataflowError::PredicateValueNotFloat { column });
    };
    let SqliteValue::Float(upper) = upper else {
        return Err(DataflowError::PredicateValueNotFloat { column });
    };
    if lower.is_nan() || upper.is_nan() {
        return Ok(Vec::new());
    }

    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Float(candidate)) if candidate < lower || upper < candidate => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Float(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotFloat { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_integer_compare(
    rows: &[WeightedRow],
    column: usize,
    op: IntegerComparison,
    value: i64,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(SqliteValue::Integer(candidate)) if op.matches(*candidate, value) => {
                Some(Ok(row.clone()))
            }
            Some(SqliteValue::Integer(_)) => None,
            Some(_) => Some(Err(DataflowError::PredicateValueNotInteger { column })),
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn filter_null(
    rows: &[WeightedRow],
    column: usize,
    predicate: NullPredicate,
) -> DataflowResult<Vec<WeightedRow>> {
    rows.iter()
        .filter_map(|row| match row.values.get(column) {
            Some(value) if predicate.matches(value) => Some(Ok(row.clone())),
            Some(_) => None,
            None => Some(Err(DataflowError::ColumnOutOfBounds {
                column,
                width: row.width(),
            })),
        })
        .collect()
}

fn count_by_key(rows: &[WeightedRow], key_columns: &[usize]) -> DataflowResult<Vec<WeightedRow>> {
    let mut groups: Vec<(Vec<SqliteValue>, i64)> = Vec::new();
    for row in rows {
        if row.is_zero() {
            continue;
        }
        let key = row.project(key_columns)?;
        if let Some((_, count)) = groups.iter_mut().find(|(candidate, _)| *candidate == key) {
            *count = count.saturating_add(row.weight);
        } else {
            groups.push((key, row.weight));
        }
    }

    Ok(groups
        .into_iter()
        .filter_map(|(mut values, count)| {
            if count == 0 {
                return None;
            }
            values.push(SqliteValue::Integer(count));
            Some(WeightedRow::insert(values))
        })
        .collect())
}

fn sum_integer_by_key(
    rows: &[WeightedRow],
    key_columns: &[usize],
    value_column: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    let mut groups: Vec<(Vec<SqliteValue>, i64)> = Vec::new();
    for row in rows {
        if row.is_zero() {
            continue;
        }
        let key = row.project(key_columns)?;
        let value = integer_value_at(row, value_column)?;
        let weighted_value = value.saturating_mul(row.weight);
        if let Some((_, sum)) = groups.iter_mut().find(|(candidate, _)| *candidate == key) {
            *sum = sum.saturating_add(weighted_value);
        } else {
            groups.push((key, weighted_value));
        }
    }

    Ok(groups
        .into_iter()
        .filter_map(|(mut values, sum)| {
            if sum == 0 {
                return None;
            }
            values.push(SqliteValue::Integer(sum));
            Some(WeightedRow::insert(values))
        })
        .collect())
}

fn min_integer_by_key(
    rows: &[WeightedRow],
    key_columns: &[usize],
    value_column: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    let mut groups: Vec<(Vec<SqliteValue>, i64)> = Vec::new();
    for row in rows {
        if row.weight <= 0 {
            continue;
        }
        let key = row.project(key_columns)?;
        let value = integer_value_at(row, value_column)?;
        if let Some((_, min_value)) = groups.iter_mut().find(|(candidate, _)| *candidate == key) {
            *min_value = (*min_value).min(value);
        } else {
            groups.push((key, value));
        }
    }

    Ok(groups
        .into_iter()
        .map(|(mut values, min_value)| {
            values.push(SqliteValue::Integer(min_value));
            WeightedRow::insert(values)
        })
        .collect())
}

fn max_integer_by_key(
    rows: &[WeightedRow],
    key_columns: &[usize],
    value_column: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    let mut groups: Vec<(Vec<SqliteValue>, i64)> = Vec::new();
    for row in rows {
        if row.weight <= 0 {
            continue;
        }
        let key = row.project(key_columns)?;
        let value = integer_value_at(row, value_column)?;
        if let Some((_, max_value)) = groups.iter_mut().find(|(candidate, _)| *candidate == key) {
            *max_value = (*max_value).max(value);
        } else {
            groups.push((key, value));
        }
    }

    Ok(groups
        .into_iter()
        .map(|(mut values, max_value)| {
            values.push(SqliteValue::Integer(max_value));
            WeightedRow::insert(values)
        })
        .collect())
}

fn average_integer_by_key(
    rows: &[WeightedRow],
    key_columns: &[usize],
    value_column: usize,
) -> DataflowResult<Vec<WeightedRow>> {
    let mut groups: Vec<(Vec<SqliteValue>, i64, i64)> = Vec::new();
    for row in rows {
        if row.weight <= 0 {
            continue;
        }
        let key = row.project(key_columns)?;
        let value = integer_value_at(row, value_column)?;
        let weighted_value = value.saturating_mul(row.weight);
        if let Some((_, sum, count)) = groups
            .iter_mut()
            .find(|(candidate, _, _)| *candidate == key)
        {
            *sum = sum.saturating_add(weighted_value);
            *count = count.saturating_add(row.weight);
        } else {
            groups.push((key, weighted_value, row.weight));
        }
    }

    Ok(groups
        .into_iter()
        .filter_map(|(mut values, sum, count)| {
            if count <= 0 {
                return None;
            }
            values.push(SqliteValue::Float(sum as f64 / count as f64));
            Some(WeightedRow::insert(values))
        })
        .collect())
}

fn integer_value_at(row: &WeightedRow, value_column: usize) -> DataflowResult<i64> {
    match row.values.get(value_column) {
        Some(SqliteValue::Integer(value)) => Ok(*value),
        Some(_) => Err(DataflowError::AggregateValueNotInteger {
            column: value_column,
        }),
        None => Err(DataflowError::ColumnOutOfBounds {
            column: value_column,
            width: row.width(),
        }),
    }
}

// `SqliteValue::Text` may carry a lazy sharing cache, but row-key ordering is
// derived from stable SQLite value content.
#[allow(clippy::mutable_key_type)]
fn index_weighted_rows<'a>(
    rows: &'a [WeightedRow],
    key_columns: &[usize],
) -> DataflowResult<BTreeMap<Vec<SqliteValue>, Vec<&'a WeightedRow>>> {
    let mut index: BTreeMap<Vec<SqliteValue>, Vec<&WeightedRow>> = BTreeMap::new();
    for row in rows {
        if row.is_zero() {
            continue;
        }
        let key = row.project(key_columns)?;
        index.entry(key).or_default().push(row);
    }
    Ok(index)
}

/// Consolidate identical output rows by algebraic weight, preserving first-seen order.
pub fn consolidate_rows(rows: Vec<WeightedRow>) -> Vec<WeightedRow> {
    let mut groups: Vec<(Vec<SqliteValue>, i64)> = Vec::new();
    for row in rows {
        if row.is_zero() {
            continue;
        }
        let values = row.values;
        if let Some((_, weight)) = groups
            .iter_mut()
            .find(|(candidate, _)| candidate == &values)
        {
            *weight = weight.saturating_add(row.weight);
        } else {
            groups.push((values, row.weight));
        }
    }
    groups
        .into_iter()
        .filter_map(|(values, weight)| (weight != 0).then(|| WeightedRow::new(values, weight)))
        .collect()
}

/// Consolidate identical rows, then collapse surviving weights to their sign.
pub fn consolidate_weight_signs(rows: &[WeightedRow]) -> Vec<WeightedRow> {
    normalize_weight_sign(&consolidate_rows(rows.to_vec()))
}

/// Multiply row weights by an algebraic factor, preserving row order and values.
pub fn scale_weights(rows: &[WeightedRow], factor: i64) -> Vec<WeightedRow> {
    if factor == 0 {
        return Vec::new();
    }

    rows.iter()
        .filter_map(|row| {
            let weight = row.weight.saturating_mul(factor);
            (weight != 0).then(|| WeightedRow::new(row.values.clone(), weight))
        })
        .collect()
}

/// Reverse row weights while preserving row order and values.
pub fn negate_weights(rows: &[WeightedRow]) -> Vec<WeightedRow> {
    scale_weights(rows, -1)
}

/// Collapse non-zero row weights to `+1` or `-1`, preserving polarity and values.
pub fn normalize_weight_sign(rows: &[WeightedRow]) -> Vec<WeightedRow> {
    rows.iter()
        .filter_map(|row| {
            let weight = match row.weight.cmp(&0) {
                Ordering::Greater => 1,
                Ordering::Less => -1,
                Ordering::Equal => return None,
            };
            Some(WeightedRow::new(row.values.clone(), weight))
        })
        .collect()
}

/// Add a static weighted-row batch and consolidate matching rows.
pub fn add_rows(left: &[WeightedRow], right: &[WeightedRow]) -> Vec<WeightedRow> {
    let mut rows = Vec::with_capacity(left.len() + right.len());
    rows.extend(left.iter().filter(|row| !row.is_zero()).cloned());
    rows.extend(right.iter().filter(|row| !row.is_zero()).cloned());
    consolidate_rows(rows)
}

/// Subtract a static weighted-row batch and consolidate matching rows.
pub fn subtract_rows(left: &[WeightedRow], right: &[WeightedRow]) -> Vec<WeightedRow> {
    let mut rows = Vec::with_capacity(left.len() + right.len());
    rows.extend(left.iter().filter(|row| !row.is_zero()).cloned());
    rows.extend(negate_weights(right));
    consolidate_rows(rows)
}

/// Retain current stream rows whose values are present in a static batch.
pub fn retain_rows_in(rows: &[WeightedRow], candidates: &[WeightedRow]) -> Vec<WeightedRow> {
    let retained_values: Vec<Vec<SqliteValue>> = consolidate_rows(candidates.to_vec())
        .into_iter()
        .map(|row| row.values)
        .collect();

    rows.iter()
        .filter(|row| {
            !row.is_zero()
                && retained_values
                    .iter()
                    .any(|candidate| candidate == &row.values)
        })
        .cloned()
        .collect()
}

/// Drop current stream rows whose values are present in a static batch.
pub fn reject_rows_in(rows: &[WeightedRow], candidates: &[WeightedRow]) -> Vec<WeightedRow> {
    let rejected_values: Vec<Vec<SqliteValue>> = consolidate_rows(candidates.to_vec())
        .into_iter()
        .map(|row| row.values)
        .collect();

    rows.iter()
        .filter(|row| {
            !row.is_zero()
                && rejected_values
                    .iter()
                    .all(|candidate| candidate != &row.values)
        })
        .cloned()
        .collect()
}

/// Rewrite non-zero input rows to a fixed algebraic weight.
pub fn set_weights(rows: &[WeightedRow], weight: i64) -> Vec<WeightedRow> {
    if weight == 0 {
        return Vec::new();
    }

    rows.iter()
        .filter(|row| !row.is_zero())
        .map(|row| WeightedRow::new(row.values.clone(), weight))
        .collect()
}

/// Convert accumulated differential counts into positive set membership.
pub fn threshold_positive(rows: &[WeightedRow]) -> Vec<WeightedRow> {
    consolidate_rows(rows.to_vec())
        .into_iter()
        .filter_map(|row| (row.weight > 0).then(|| WeightedRow::insert(row.values)))
        .collect()
}

/// Materialize algebraic weights into payload rows for downstream delta emission.
pub fn append_weight_column(rows: &[WeightedRow]) -> Vec<WeightedRow> {
    rows.iter()
        .filter_map(|row| {
            if row.is_zero() {
                return None;
            }
            let mut values = row.values.clone();
            values.push(SqliteValue::Integer(row.weight));
            Some(WeightedRow::insert(values))
        })
        .collect()
}

/// Materialize algebraic weight polarity into payload rows for delta emission.
pub fn append_weight_sign_column(rows: &[WeightedRow]) -> Vec<WeightedRow> {
    normalize_weight_sign(rows)
        .into_iter()
        .map(|row| {
            let mut values = row.values;
            values.push(SqliteValue::Integer(row.weight));
            WeightedRow::insert(values)
        })
        .collect()
}

/// Materialize saturated algebraic weight magnitude into payload rows.
pub fn append_weight_magnitude_column(rows: &[WeightedRow]) -> Vec<WeightedRow> {
    rows.iter()
        .filter_map(|row| {
            if row.is_zero() {
                return None;
            }
            let mut values = row.values.clone();
            values.push(SqliteValue::Integer(weight_magnitude(row.weight)));
            Some(WeightedRow::insert(values))
        })
        .collect()
}

fn weight_magnitude(weight: i64) -> i64 {
    weight.checked_abs().unwrap_or(i64::MAX)
}

/// Compute `DeltaLeft JOIN Right`, preserving algebraic weights.
#[allow(clippy::mutable_key_type)]
pub fn delta_join_left(
    delta_left: &[WeightedRow],
    stable_right: &[WeightedRow],
    key_spec: &JoinKeySpec,
) -> DataflowResult<Vec<WeightedRow>> {
    key_spec.validate()?;
    let right_index = index_weighted_rows(stable_right, &key_spec.right_columns)?;
    let mut joined = Vec::new();

    for left in delta_left {
        if left.is_zero() {
            continue;
        }
        let left_key = left.project(&key_spec.left_columns)?;
        if let Some(matches) = right_index.get(&left_key) {
            for right in matches {
                let mut values = left.values.clone();
                values.extend(right.values.clone());
                joined.push(WeightedRow::new(
                    values,
                    left.weight.saturating_mul(right.weight),
                ));
            }
        }
    }

    tracing::debug!(
        target: "fsqlite_vdbe::dataflow",
        event = "delta_join_left",
        delta_rows = delta_left.len(),
        stable_rows = stable_right.len(),
        output_rows = joined.len()
    );
    Ok(joined)
}

/// Compute `Left JOIN DeltaRight`, preserving algebraic weights.
#[allow(clippy::mutable_key_type)]
pub fn delta_join_right(
    stable_left: &[WeightedRow],
    delta_right: &[WeightedRow],
    key_spec: &JoinKeySpec,
) -> DataflowResult<Vec<WeightedRow>> {
    key_spec.validate()?;
    let left_index = index_weighted_rows(stable_left, &key_spec.left_columns)?;
    let mut joined = Vec::new();

    for right in delta_right {
        if right.is_zero() {
            continue;
        }
        let right_key = right.project(&key_spec.right_columns)?;
        if let Some(matches) = left_index.get(&right_key) {
            for left in matches {
                let mut values = left.values.clone();
                values.extend(right.values.clone());
                joined.push(WeightedRow::new(
                    values,
                    left.weight.saturating_mul(right.weight),
                ));
            }
        }
    }

    tracing::debug!(
        target: "fsqlite_vdbe::dataflow",
        event = "delta_join_right",
        stable_rows = stable_left.len(),
        delta_rows = delta_right.len(),
        output_rows = joined.len()
    );
    Ok(joined)
}

/// Compute the full join delta for simultaneous left and right relation changes.
///
/// Given old stable relations `L` and `R`, plus input deltas `dL` and `dR`,
/// this emits `(dL JOIN R) UNION (L JOIN dR) UNION (dL JOIN dR)` and
/// consolidates duplicate output rows by algebraic weight.
pub fn delta_join_update(
    stable_left: &[WeightedRow],
    delta_left: &[WeightedRow],
    stable_right: &[WeightedRow],
    delta_right: &[WeightedRow],
    key_spec: &JoinKeySpec,
) -> DataflowResult<Vec<WeightedRow>> {
    key_spec.validate()?;
    let mut joined = delta_join_left(delta_left, stable_right, key_spec)?;
    joined.extend(delta_join_right(stable_left, delta_right, key_spec)?);
    joined.extend(delta_join_left(delta_left, delta_right, key_spec)?);
    let joined = consolidate_rows(joined);

    tracing::debug!(
        target: "fsqlite_vdbe::dataflow",
        event = "delta_join_update",
        stable_left_rows = stable_left.len(),
        delta_left_rows = delta_left.len(),
        stable_right_rows = stable_right.len(),
        delta_right_rows = delta_right.len(),
        output_rows = joined.len()
    );
    Ok(joined)
}

#[cfg(test)]
mod tests {
    use super::{
        DataflowAutomaton, DataflowError, DataflowOperator, FloatComparison, FloatSign,
        IntegerComparison, JoinKeySpec, NullPredicate, WeightSign, WeightedRow, delta_join_left,
        delta_join_right, delta_join_update, scale_weights, set_weights, threshold_positive,
    };
    use fsqlite_types::SqliteValue;

    fn int(value: i64) -> SqliteValue {
        SqliteValue::Integer(value)
    }

    fn text(value: &str) -> SqliteValue {
        SqliteValue::Text(value.into())
    }

    fn blob(bytes: &[u8]) -> SqliteValue {
        SqliteValue::Blob(bytes.to_vec().into())
    }

    #[test]
    fn automaton_filters_projects_and_preserves_weight() {
        let automaton = DataflowAutomaton::with_input_width(
            3,
            vec![
                DataflowOperator::FilterEq {
                    column: 1,
                    value: int(7),
                },
                DataflowOperator::Project {
                    columns: vec![2, 0],
                },
            ],
        );
        let rows = vec![
            WeightedRow::new(vec![int(1), int(7), int(10)], 3),
            WeightedRow::new(vec![int(2), int(8), int(20)], 5),
            WeightedRow::new(vec![int(3), int(7), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(actual, vec![WeightedRow::new(vec![int(10), int(1)], 3)]);
    }

    #[test]
    fn automaton_fail_closes_on_schema_width_change() {
        let automaton = DataflowAutomaton::with_input_width(2, Vec::new());

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2), int(3)])])
            .expect_err("width mismatch should halt the automaton");

        assert_eq!(
            err,
            DataflowError::SchemaChanged {
                expected_width: 2,
                actual_width: 3
            }
        );
    }

    #[test]
    fn filter_in_set_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterInSet {
            column: 1,
            values: vec![int(3), int(5)],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(2)], 7),
            WeightedRow::new(vec![int(2), int(3)], -2),
            WeightedRow::new(vec![int(3), int(5)], 4),
            WeightedRow::new(vec![int(4), int(5)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), int(3)], -2),
                WeightedRow::new(vec![int(3), int(5)], 4),
            ]
        );
    }

    #[test]
    fn filter_in_set_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterInSet {
            column: 2,
            values: vec![int(3)],
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("invalid in-set predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_not_in_set_keeps_non_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterNotInSet {
            column: 1,
            values: vec![int(3), int(5)],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(2)], 7),
            WeightedRow::new(vec![int(2), int(3)], -2),
            WeightedRow::new(vec![int(3), int(5)], 4),
            WeightedRow::new(vec![int(4), int(8)], -6),
            WeightedRow::new(vec![int(5), int(8)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), int(2)], 7),
                WeightedRow::new(vec![int(4), int(8)], -6),
            ]
        );
    }

    #[test]
    fn filter_not_in_set_empty_values_keeps_all_non_zero_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterNotInSet {
            column: 1,
            values: Vec::new(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(2)], 7),
            WeightedRow::new(vec![int(2), int(3)], -2),
            WeightedRow::new(vec![int(3), int(5)], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), int(2)], 7),
                WeightedRow::new(vec![int(2), int(3)], -2),
            ]
        );
    }

    #[test]
    fn filter_not_in_set_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterNotInSet {
            column: 2,
            values: vec![int(3)],
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("invalid not-in-set predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_blob_prefix_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobPrefix {
            column: 1,
            prefix: vec![0xCA, 0xFE],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[0xCA])], 7),
            WeightedRow::new(vec![int(2), blob(&[0xCA, 0xFE])], -2),
            WeightedRow::new(vec![int(3), blob(&[0xCA, 0xFE, 0xBA])], 4),
            WeightedRow::new(vec![int(4), blob(&[0xBA, 0xBE])], 5),
            WeightedRow::new(vec![int(5), blob(&[0xCA, 0xFE, 0x01])], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), blob(&[0xCA, 0xFE])], -2),
                WeightedRow::new(vec![int(3), blob(&[0xCA, 0xFE, 0xBA])], 4),
            ]
        );
    }

    #[test]
    fn filter_blob_prefix_empty_prefix_keeps_all_non_zero_blob_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobPrefix {
            column: 1,
            prefix: Vec::new(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[1, 2])], 7),
            WeightedRow::new(vec![int(2), blob(&[])], -2),
            WeightedRow::new(vec![int(3), blob(&[3])], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), blob(&[1, 2])], 7),
                WeightedRow::new(vec![int(2), blob(&[])], -2),
            ]
        );
    }

    #[test]
    fn filter_blob_prefix_rejects_non_blob_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobPrefix {
            column: 1,
            prefix: vec![0xCA],
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("cafe")])])
            .expect_err("non-blob predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotBlob { column: 1 });
    }

    #[test]
    fn filter_blob_prefix_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobPrefix {
            column: 2,
            prefix: vec![0xCA],
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), blob(&[0xCA])])])
            .expect_err("invalid blob-prefix predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_blob_suffix_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobSuffix {
            column: 1,
            suffix: vec![0xFE, 0xED],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[0xFE])], 7),
            WeightedRow::new(vec![int(2), blob(&[0xCA, 0xFE, 0xED])], -2),
            WeightedRow::new(vec![int(3), blob(&[0x00, 0xFE, 0xED])], 4),
            WeightedRow::new(vec![int(4), blob(&[0xFE, 0xED, 0x00])], 5),
            WeightedRow::new(vec![int(5), blob(&[0x01, 0xFE, 0xED])], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), blob(&[0xCA, 0xFE, 0xED])], -2),
                WeightedRow::new(vec![int(3), blob(&[0x00, 0xFE, 0xED])], 4),
            ]
        );
    }

    #[test]
    fn filter_blob_suffix_empty_suffix_keeps_all_non_zero_blob_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobSuffix {
            column: 1,
            suffix: Vec::new(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[1, 2])], 7),
            WeightedRow::new(vec![int(2), blob(&[])], -2),
            WeightedRow::new(vec![int(3), blob(&[3])], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), blob(&[1, 2])], 7),
                WeightedRow::new(vec![int(2), blob(&[])], -2),
            ]
        );
    }

    #[test]
    fn filter_blob_suffix_rejects_non_blob_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobSuffix {
            column: 1,
            suffix: vec![0xFE],
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("feed")])])
            .expect_err("non-blob predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotBlob { column: 1 });
    }

    #[test]
    fn filter_blob_suffix_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobSuffix {
            column: 2,
            suffix: vec![0xFE],
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), blob(&[0xFE])])])
            .expect_err("invalid blob-suffix predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_blob_contains_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobContains {
            column: 1,
            needle: vec![0xAA, 0xBB],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[0xAA])], 7),
            WeightedRow::new(vec![int(2), blob(&[0xAA, 0xBB])], -2),
            WeightedRow::new(vec![int(3), blob(&[0x00, 0xAA, 0xBB, 0xCC])], 4),
            WeightedRow::new(vec![int(4), blob(&[0xAA, 0x00, 0xBB])], 5),
            WeightedRow::new(vec![int(5), blob(&[0xCC, 0xAA, 0xBB])], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), blob(&[0xAA, 0xBB])], -2),
                WeightedRow::new(vec![int(3), blob(&[0x00, 0xAA, 0xBB, 0xCC])], 4),
            ]
        );
    }

    #[test]
    fn filter_blob_contains_empty_needle_keeps_all_non_zero_blob_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobContains {
            column: 1,
            needle: Vec::new(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[1, 2])], 7),
            WeightedRow::new(vec![int(2), blob(&[])], -2),
            WeightedRow::new(vec![int(3), blob(&[3])], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), blob(&[1, 2])], 7),
                WeightedRow::new(vec![int(2), blob(&[])], -2),
            ]
        );
    }

    #[test]
    fn filter_blob_contains_rejects_non_blob_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobContains {
            column: 1,
            needle: vec![0xAA],
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("aabb")])])
            .expect_err("non-blob predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotBlob { column: 1 });
    }

    #[test]
    fn filter_blob_contains_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobContains {
            column: 2,
            needle: vec![0xAA],
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), blob(&[0xAA])])])
            .expect_err("invalid blob-contains predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_blob_not_contains_keeps_non_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobNotContains {
            column: 1,
            needle: vec![0xAA, 0xBB],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[0xAA])], 7),
            WeightedRow::new(vec![int(2), blob(&[0xAA, 0xBB])], -2),
            WeightedRow::new(vec![int(3), blob(&[0x00, 0xAA, 0xBB, 0xCC])], 4),
            WeightedRow::new(vec![int(4), blob(&[0xAA, 0x00, 0xBB])], 5),
            WeightedRow::new(vec![int(5), blob(&[0xCC])], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), blob(&[0xAA])], 7),
                WeightedRow::new(vec![int(4), blob(&[0xAA, 0x00, 0xBB])], 5),
            ]
        );
    }

    #[test]
    fn filter_blob_not_contains_empty_needle_drops_all_blob_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobNotContains {
            column: 1,
            needle: Vec::new(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[1, 2])], 7),
            WeightedRow::new(vec![int(2), blob(&[])], -2),
            WeightedRow::new(vec![int(3), blob(&[3])], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert!(actual.is_empty());
    }

    #[test]
    fn filter_blob_not_contains_rejects_non_blob_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobNotContains {
            column: 1,
            needle: vec![0xAA],
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("aabb")])])
            .expect_err("non-blob predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotBlob { column: 1 });
    }

    #[test]
    fn filter_blob_not_contains_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobNotContains {
            column: 2,
            needle: vec![0xAA],
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), blob(&[0xAA])])])
            .expect_err("invalid blob-not-contains predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_text_prefix_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextPrefix {
            column: 1,
            prefix: "al".to_owned(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("alpha")], 7),
            WeightedRow::new(vec![int(2), text("alpine")], -2),
            WeightedRow::new(vec![int(3), text("beta")], 4),
            WeightedRow::new(vec![int(4), text("altar")], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), text("alpha")], 7),
                WeightedRow::new(vec![int(2), text("alpine")], -2),
            ]
        );
    }

    #[test]
    fn filter_text_prefix_empty_prefix_keeps_all_non_zero_text_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextPrefix {
            column: 1,
            prefix: String::new(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("alpha")], 7),
            WeightedRow::new(vec![int(2), text("beta")], -2),
            WeightedRow::new(vec![int(3), text("gamma")], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), text("alpha")], 7),
                WeightedRow::new(vec![int(2), text("beta")], -2),
            ]
        );
    }

    #[test]
    fn filter_text_prefix_rejects_non_text_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextPrefix {
            column: 1,
            prefix: "al".to_owned(),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("non-text predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotText { column: 1 });
    }

    #[test]
    fn filter_text_prefix_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextPrefix {
            column: 2,
            prefix: "al".to_owned(),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("alpha")])])
            .expect_err("invalid text-prefix predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_text_suffix_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextSuffix {
            column: 1,
            suffix: "ta".to_owned(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("alpha")], 7),
            WeightedRow::new(vec![int(2), text("beta")], -2),
            WeightedRow::new(vec![int(3), text("delta")], 4),
            WeightedRow::new(vec![int(4), text("zeta")], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), text("beta")], -2),
                WeightedRow::new(vec![int(3), text("delta")], 4),
            ]
        );
    }

    #[test]
    fn filter_text_suffix_empty_suffix_keeps_all_non_zero_text_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextSuffix {
            column: 1,
            suffix: String::new(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("alpha")], 7),
            WeightedRow::new(vec![int(2), text("beta")], -2),
            WeightedRow::new(vec![int(3), text("gamma")], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), text("alpha")], 7),
                WeightedRow::new(vec![int(2), text("beta")], -2),
            ]
        );
    }

    #[test]
    fn filter_text_suffix_rejects_non_text_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextSuffix {
            column: 1,
            suffix: "ta".to_owned(),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("non-text predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotText { column: 1 });
    }

    #[test]
    fn filter_text_suffix_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextSuffix {
            column: 2,
            suffix: "ta".to_owned(),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("beta")])])
            .expect_err("invalid text-suffix predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_text_contains_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextContains {
            column: 1,
            needle: "et".to_owned(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("alpha")], 7),
            WeightedRow::new(vec![int(2), text("beta")], -2),
            WeightedRow::new(vec![int(3), text("theta")], 4),
            WeightedRow::new(vec![int(4), text("meteor")], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), text("beta")], -2),
                WeightedRow::new(vec![int(3), text("theta")], 4),
            ]
        );
    }

    #[test]
    fn filter_text_contains_empty_needle_keeps_all_non_zero_text_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextContains {
            column: 1,
            needle: String::new(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("alpha")], 7),
            WeightedRow::new(vec![int(2), text("beta")], -2),
            WeightedRow::new(vec![int(3), text("gamma")], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), text("alpha")], 7),
                WeightedRow::new(vec![int(2), text("beta")], -2),
            ]
        );
    }

    #[test]
    fn filter_text_contains_rejects_non_text_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextContains {
            column: 1,
            needle: "et".to_owned(),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("non-text predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotText { column: 1 });
    }

    #[test]
    fn filter_text_contains_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextContains {
            column: 2,
            needle: "et".to_owned(),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("beta")])])
            .expect_err("invalid text-contains predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_text_not_contains_keeps_non_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextNotContains {
            column: 1,
            needle: "et".to_owned(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("alpha")], 7),
            WeightedRow::new(vec![int(2), text("beta")], -2),
            WeightedRow::new(vec![int(3), text("theta")], 4),
            WeightedRow::new(vec![int(4), text("gamma")], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![WeightedRow::new(vec![int(1), text("alpha")], 7)]
        );
    }

    #[test]
    fn filter_text_not_contains_empty_needle_matches_no_text_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextNotContains {
            column: 1,
            needle: String::new(),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("alpha")], 7),
            WeightedRow::new(vec![int(2), text("beta")], -2),
            WeightedRow::new(vec![int(3), text("gamma")], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            Vec::new()
        );
    }

    #[test]
    fn filter_text_not_contains_rejects_non_text_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextNotContains {
            column: 1,
            needle: "et".to_owned(),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("non-text predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotText { column: 1 });
    }

    #[test]
    fn filter_text_not_contains_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextNotContains {
            column: 2,
            needle: "et".to_owned(),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("beta")])])
            .expect_err("invalid text-not-contains predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_text_length_between_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthBetween {
            column: 1,
            lower: 3,
            upper: 4,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("at")], 7),
            WeightedRow::new(vec![int(2), text("cat")], -2),
            WeightedRow::new(vec![int(3), text("echo")], 4),
            WeightedRow::new(vec![int(4), text("alpha")], 5),
            WeightedRow::new(vec![int(5), text("sun")], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), text("cat")], -2),
                WeightedRow::new(vec![int(3), text("echo")], 4),
            ]
        );
    }

    #[test]
    fn filter_text_length_between_counts_unicode_scalar_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthBetween {
            column: 1,
            lower: 4,
            upper: 4,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("echo")], 3),
            WeightedRow::new(vec![int(2), text("\u{e9}cho")], -2),
            WeightedRow::new(vec![int(3), text("\u{e9}chos")], 5),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), text("echo")], 3),
                WeightedRow::new(vec![int(2), text("\u{e9}cho")], -2),
            ]
        );
    }

    #[test]
    fn filter_text_length_between_inverted_range_matches_no_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthBetween {
            column: 1,
            lower: 5,
            upper: 3,
        }]);
        let rows = vec![WeightedRow::new(vec![int(1), text("echo")], 3)];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            Vec::<WeightedRow>::new()
        );
    }

    #[test]
    fn filter_text_length_between_rejects_non_text_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthBetween {
            column: 1,
            lower: 3,
            upper: 4,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(4)])])
            .expect_err("non-text predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotText { column: 1 });
    }

    #[test]
    fn filter_text_length_between_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthBetween {
            column: 2,
            lower: 3,
            upper: 4,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("echo")])])
            .expect_err("invalid text-length predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_text_length_exact_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthExact {
            column: 1,
            length: 4,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("at")], 7),
            WeightedRow::new(vec![int(2), text("echo")], -2),
            WeightedRow::new(vec![int(3), text("beta")], 4),
            WeightedRow::new(vec![int(4), text("alpha")], 5),
            WeightedRow::new(vec![int(5), text("zeta")], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), text("echo")], -2),
                WeightedRow::new(vec![int(3), text("beta")], 4),
            ]
        );
    }

    #[test]
    fn filter_text_length_exact_counts_unicode_scalar_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthExact {
            column: 1,
            length: 4,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("echo")], 3),
            WeightedRow::new(vec![int(2), text("\u{e9}cho")], -2),
            WeightedRow::new(vec![int(3), text("\u{e9}chos")], 5),
            WeightedRow::new(vec![int(4), text("e\u{301}cho")], 6),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), text("echo")], 3),
                WeightedRow::new(vec![int(2), text("\u{e9}cho")], -2),
            ]
        );
    }

    #[test]
    fn filter_text_length_exact_matches_empty_text_length() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthExact {
            column: 1,
            length: 0,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("")], 7),
            WeightedRow::new(vec![int(2), text("a")], -2),
            WeightedRow::new(vec![int(3), text("")], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![WeightedRow::new(vec![int(1), text("")], 7)]
        );
    }

    #[test]
    fn filter_text_length_exact_rejects_non_text_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthExact {
            column: 1,
            length: 4,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(4)])])
            .expect_err("non-text predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotText { column: 1 });
    }

    #[test]
    fn filter_text_length_exact_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthExact {
            column: 2,
            length: 4,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("echo")])])
            .expect_err("invalid text-length-exact predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_text_length_not_exact_keeps_non_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthNotExact {
            column: 1,
            length: 4,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("at")], 7),
            WeightedRow::new(vec![int(2), text("echo")], -2),
            WeightedRow::new(vec![int(3), text("beta")], 4),
            WeightedRow::new(vec![int(4), text("alpha")], 5),
            WeightedRow::new(vec![int(5), text("sun")], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), text("at")], 7),
                WeightedRow::new(vec![int(4), text("alpha")], 5),
            ]
        );
    }

    #[test]
    fn filter_text_length_not_exact_counts_unicode_scalar_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthNotExact {
            column: 1,
            length: 4,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("echo")], 3),
            WeightedRow::new(vec![int(2), text("\u{e9}cho")], -2),
            WeightedRow::new(vec![int(3), text("\u{e9}chos")], 5),
            WeightedRow::new(vec![int(4), text("e\u{301}cho")], 6),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(3), text("\u{e9}chos")], 5),
                WeightedRow::new(vec![int(4), text("e\u{301}cho")], 6),
            ]
        );
    }

    #[test]
    fn filter_text_length_not_exact_excludes_empty_text_length() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthNotExact {
            column: 1,
            length: 0,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("")], 7),
            WeightedRow::new(vec![int(2), text("a")], -2),
            WeightedRow::new(vec![int(3), text("")], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![WeightedRow::new(vec![int(2), text("a")], -2)]
        );
    }

    #[test]
    fn filter_text_length_not_exact_rejects_non_text_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthNotExact {
            column: 1,
            length: 4,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(4)])])
            .expect_err("non-text predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotText { column: 1 });
    }

    #[test]
    fn filter_text_length_not_exact_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthNotExact {
            column: 2,
            length: 4,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("echo")])])
            .expect_err("invalid text-length-not-exact predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_blob_length_between_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthBetween {
            column: 1,
            lower: 2,
            upper: 4,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[1])], 7),
            WeightedRow::new(vec![int(2), blob(&[1, 2])], -2),
            WeightedRow::new(vec![int(3), blob(&[1, 2, 3, 4])], 4),
            WeightedRow::new(vec![int(4), blob(&[1, 2, 3, 4, 5])], 5),
            WeightedRow::new(vec![int(5), blob(&[9, 9])], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), blob(&[1, 2])], -2),
                WeightedRow::new(vec![int(3), blob(&[1, 2, 3, 4])], 4),
            ]
        );
    }

    #[test]
    fn filter_blob_length_between_inverted_range_matches_no_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthBetween {
            column: 1,
            lower: 5,
            upper: 3,
        }]);
        let rows = vec![WeightedRow::new(vec![int(1), blob(&[1, 2, 3, 4])], 3)];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            Vec::<WeightedRow>::new()
        );
    }

    #[test]
    fn filter_blob_length_between_rejects_non_blob_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthBetween {
            column: 1,
            lower: 2,
            upper: 4,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("blob")])])
            .expect_err("non-blob predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotBlob { column: 1 });
    }

    #[test]
    fn filter_blob_length_between_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthBetween {
            column: 2,
            lower: 2,
            upper: 4,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), blob(&[1, 2])])])
            .expect_err("invalid blob-length predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_blob_length_exact_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthExact {
            column: 1,
            length: 2,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[0xAA])], 7),
            WeightedRow::new(vec![int(2), blob(&[0xAA, 0xBB])], -2),
            WeightedRow::new(vec![int(3), blob(&[])], 4),
            WeightedRow::new(vec![int(4), blob(&[0x00, 0x01])], 5),
            WeightedRow::new(vec![int(5), blob(&[0xCC, 0xDD])], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), blob(&[0xAA, 0xBB])], -2),
                WeightedRow::new(vec![int(4), blob(&[0x00, 0x01])], 5),
            ]
        );
    }

    #[test]
    fn filter_blob_length_exact_matches_empty_blob_length() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthExact {
            column: 1,
            length: 0,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[1, 2])], 7),
            WeightedRow::new(vec![int(2), blob(&[])], -2),
            WeightedRow::new(vec![int(3), blob(&[])], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![WeightedRow::new(vec![int(2), blob(&[])], -2)]
        );
    }

    #[test]
    fn filter_blob_length_exact_rejects_non_blob_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthExact {
            column: 1,
            length: 2,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("aa")])])
            .expect_err("non-blob predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotBlob { column: 1 });
    }

    #[test]
    fn filter_blob_length_exact_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthExact {
            column: 2,
            length: 1,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), blob(&[0xAA])])])
            .expect_err("invalid blob-length-exact predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_blob_length_not_exact_keeps_non_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthNotExact {
            column: 1,
            length: 2,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[0xAA])], 7),
            WeightedRow::new(vec![int(2), blob(&[0xAA, 0xBB])], -2),
            WeightedRow::new(vec![int(3), blob(&[])], 4),
            WeightedRow::new(vec![int(4), blob(&[0x00, 0x01])], 5),
            WeightedRow::new(vec![int(5), blob(&[0xCC])], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), blob(&[0xAA])], 7),
                WeightedRow::new(vec![int(3), blob(&[])], 4),
            ]
        );
    }

    #[test]
    fn filter_blob_length_not_exact_excludes_empty_blob_length() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthNotExact {
            column: 1,
            length: 0,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[1, 2])], 7),
            WeightedRow::new(vec![int(2), blob(&[])], -2),
            WeightedRow::new(vec![int(3), blob(&[3])], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![WeightedRow::new(vec![int(1), blob(&[1, 2])], 7)]
        );
    }

    #[test]
    fn filter_blob_length_not_exact_rejects_non_blob_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthNotExact {
            column: 1,
            length: 2,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("aa")])])
            .expect_err("non-blob predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotBlob { column: 1 });
    }

    #[test]
    fn filter_blob_length_not_exact_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthNotExact {
            column: 2,
            length: 1,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), blob(&[0xAA])])])
            .expect_err("invalid blob-length-not-exact predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_blob_length_not_between_keeps_outside_weighted_rows() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthNotBetween {
                column: 1,
                lower: 2,
                upper: 4,
            }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[1])], 7),
            WeightedRow::new(vec![int(2), blob(&[1, 2])], -2),
            WeightedRow::new(vec![int(3), blob(&[1, 2, 3, 4])], 4),
            WeightedRow::new(vec![int(4), blob(&[1, 2, 3, 4, 5])], 5),
            WeightedRow::new(vec![int(5), blob(&[9])], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), blob(&[1])], 7),
                WeightedRow::new(vec![int(4), blob(&[1, 2, 3, 4, 5])], 5),
            ]
        );
    }

    #[test]
    fn filter_blob_length_not_between_inverted_range_keeps_all_non_zero_blob_rows() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthNotBetween {
                column: 1,
                lower: 5,
                upper: 3,
            }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), blob(&[1, 2])], 3),
            WeightedRow::new(vec![int(2), blob(&[1, 2, 3, 4])], -4),
            WeightedRow::new(vec![int(3), blob(&[])], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), blob(&[1, 2])], 3),
                WeightedRow::new(vec![int(2), blob(&[1, 2, 3, 4])], -4),
            ]
        );
    }

    #[test]
    fn filter_blob_length_not_between_rejects_non_blob_values() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthNotBetween {
                column: 1,
                lower: 2,
                upper: 4,
            }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("blob")])])
            .expect_err("non-blob predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotBlob { column: 1 });
    }

    #[test]
    fn filter_blob_length_not_between_rejects_out_of_bounds_columns() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterBlobLengthNotBetween {
                column: 2,
                lower: 2,
                upper: 4,
            }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), blob(&[1, 2])])])
            .expect_err("invalid blob-length anti-range predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_text_length_not_between_keeps_outside_weighted_rows() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthNotBetween {
                column: 1,
                lower: 3,
                upper: 4,
            }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("at")], 7),
            WeightedRow::new(vec![int(2), text("cat")], -2),
            WeightedRow::new(vec![int(3), text("echo")], 4),
            WeightedRow::new(vec![int(4), text("alpha")], 5),
            WeightedRow::new(vec![int(5), text("sun")], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), text("at")], 7),
                WeightedRow::new(vec![int(4), text("alpha")], 5),
            ]
        );
    }

    #[test]
    fn filter_text_length_not_between_counts_unicode_scalar_values() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthNotBetween {
                column: 1,
                lower: 4,
                upper: 4,
            }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("echo")], 3),
            WeightedRow::new(vec![int(2), text("\u{e9}cho")], -2),
            WeightedRow::new(vec![int(3), text("\u{e9}chos")], 5),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![WeightedRow::new(vec![int(3), text("\u{e9}chos")], 5)]
        );
    }

    #[test]
    fn filter_text_length_not_between_inverted_range_keeps_all_non_zero_text_rows() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthNotBetween {
                column: 1,
                lower: 5,
                upper: 3,
            }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), text("at")], 7),
            WeightedRow::new(vec![int(2), text("echo")], -2),
            WeightedRow::new(vec![int(3), text("alpha")], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), text("at")], 7),
                WeightedRow::new(vec![int(2), text("echo")], -2),
            ]
        );
    }

    #[test]
    fn filter_text_length_not_between_rejects_non_text_values() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthNotBetween {
                column: 1,
                lower: 3,
                upper: 4,
            }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(4)])])
            .expect_err("non-text predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotText { column: 1 });
    }

    #[test]
    fn filter_text_length_not_between_rejects_out_of_bounds_columns() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterTextLengthNotBetween {
                column: 2,
                lower: 3,
                upper: 4,
            }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), text("echo")])])
            .expect_err("invalid text-length-not-between predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_integer_between_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterIntegerBetween {
            column: 1,
            lower: 10,
            upper: 20,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(9)], 3),
            WeightedRow::new(vec![int(2), int(10)], -2),
            WeightedRow::new(vec![int(3), int(15)], 4),
            WeightedRow::new(vec![int(4), int(20)], 5),
            WeightedRow::new(vec![int(5), int(21)], 6),
            WeightedRow::new(vec![int(6), int(15)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), int(10)], -2),
                WeightedRow::new(vec![int(3), int(15)], 4),
                WeightedRow::new(vec![int(4), int(20)], 5),
            ]
        );
    }

    #[test]
    fn filter_integer_between_inverted_range_matches_no_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterIntegerBetween {
            column: 1,
            lower: 20,
            upper: 10,
        }]);
        let rows = vec![WeightedRow::new(vec![int(1), int(15)], 3)];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            Vec::<WeightedRow>::new()
        );
    }

    #[test]
    fn filter_integer_between_rejects_non_integer_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterIntegerBetween {
            column: 1,
            lower: 10,
            upper: 20,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![
                int(1),
                SqliteValue::Text("15".into()),
            ])])
            .expect_err("non-integer predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotInteger { column: 1 });
    }

    #[test]
    fn filter_integer_between_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterIntegerBetween {
            column: 2,
            lower: 10,
            upper: 20,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("invalid between predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_integer_not_between_keeps_outside_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterIntegerNotBetween {
            column: 1,
            lower: 10,
            upper: 20,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(9)], 3),
            WeightedRow::new(vec![int(2), int(10)], -2),
            WeightedRow::new(vec![int(3), int(15)], 4),
            WeightedRow::new(vec![int(4), int(20)], 5),
            WeightedRow::new(vec![int(5), int(21)], 6),
            WeightedRow::new(vec![int(6), int(9)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), int(9)], 3),
                WeightedRow::new(vec![int(5), int(21)], 6),
            ]
        );
    }

    #[test]
    fn filter_integer_not_between_inverted_range_keeps_all_non_zero_integer_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterIntegerNotBetween {
            column: 1,
            lower: 20,
            upper: 10,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(15)], 3),
            WeightedRow::new(vec![int(2), int(20)], -2),
            WeightedRow::new(vec![int(3), int(10)], 0),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), int(15)], 3),
                WeightedRow::new(vec![int(2), int(20)], -2),
            ]
        );
    }

    #[test]
    fn filter_integer_not_between_rejects_non_integer_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterIntegerNotBetween {
            column: 1,
            lower: 10,
            upper: 20,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![
                int(1),
                SqliteValue::Text("15".into()),
            ])])
            .expect_err("non-integer predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotInteger { column: 1 });
    }

    #[test]
    fn filter_integer_not_between_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterIntegerNotBetween {
            column: 2,
            lower: 10,
            upper: 20,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("invalid not-between predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_float_finite_keeps_only_finite_weighted_rows() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterFloatFinite { column: 1 }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), SqliteValue::Float(1.5)], 7),
            WeightedRow::new(vec![int(2), SqliteValue::Float(f64::INFINITY)], -2),
            WeightedRow::new(vec![int(3), SqliteValue::Float(f64::NEG_INFINITY)], 4),
            WeightedRow::new(vec![int(4), SqliteValue::Float(f64::NAN)], 5),
            WeightedRow::new(vec![int(5), SqliteValue::Float(0.25)], -6),
            WeightedRow::new(vec![int(6), SqliteValue::Float(2.0)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), SqliteValue::Float(1.5)], 7),
                WeightedRow::new(vec![int(5), SqliteValue::Float(0.25)], -6),
            ]
        );
    }

    #[test]
    fn filter_float_finite_rejects_non_float_values() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterFloatFinite { column: 1 }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("non-float predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotFloat { column: 1 });
    }

    #[test]
    fn filter_float_finite_rejects_out_of_bounds_columns() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterFloatFinite { column: 2 }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("invalid finite-float predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_float_non_finite_keeps_only_nan_and_infinity_weighted_rows() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterFloatNonFinite { column: 1 }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), SqliteValue::Float(1.5)], 7),
            WeightedRow::new(vec![int(2), SqliteValue::Float(f64::INFINITY)], -2),
            WeightedRow::new(vec![int(3), SqliteValue::Float(f64::NEG_INFINITY)], 4),
            WeightedRow::new(vec![int(4), SqliteValue::Float(f64::NAN)], 5),
            WeightedRow::new(vec![int(5), SqliteValue::Float(0.25)], -6),
            WeightedRow::new(vec![int(6), SqliteValue::Float(f64::INFINITY)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(actual.len(), 3);
        assert_eq!(
            actual[0],
            WeightedRow::new(vec![int(2), SqliteValue::Float(f64::INFINITY)], -2)
        );
        assert_eq!(
            actual[1],
            WeightedRow::new(vec![int(3), SqliteValue::Float(f64::NEG_INFINITY)], 4)
        );
        assert_eq!(actual[2].weight, 5);
        assert_eq!(actual[2].values[0], int(4));
        let SqliteValue::Float(value) = actual[2].values[1] else {
            panic!("expected NaN payload");
        };
        assert!(value.is_nan());
    }

    #[test]
    fn filter_float_non_finite_rejects_non_float_values() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterFloatNonFinite { column: 1 }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("non-float predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotFloat { column: 1 });
    }

    #[test]
    fn filter_float_non_finite_rejects_out_of_bounds_columns() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::FilterFloatNonFinite { column: 2 }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("invalid non-finite-float predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_float_sign_splits_positive_negative_and_zero_real_rows() {
        let rows = vec![
            WeightedRow::new(vec![int(1), SqliteValue::Float(3.5)], 7),
            WeightedRow::new(vec![int(2), SqliteValue::Float(f64::INFINITY)], -2),
            WeightedRow::new(vec![int(3), SqliteValue::Float(-2.25)], 4),
            WeightedRow::new(vec![int(4), SqliteValue::Float(f64::NEG_INFINITY)], -5),
            WeightedRow::new(vec![int(5), SqliteValue::Float(0.0)], 6),
            WeightedRow::new(vec![int(6), SqliteValue::Float(-0.0)], -7),
            WeightedRow::new(vec![int(7), SqliteValue::Float(f64::NAN)], 8),
            WeightedRow::new(vec![int(8), SqliteValue::Float(1.0)], 0),
        ];

        let positives = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatSign {
            column: 1,
            sign: FloatSign::Positive,
        }])
        .execute(&rows)
        .expect("positive sign filter should execute");
        let negatives = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatSign {
            column: 1,
            sign: FloatSign::Negative,
        }])
        .execute(&rows)
        .expect("negative sign filter should execute");
        let zeroes = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatSign {
            column: 1,
            sign: FloatSign::Zero,
        }])
        .execute(&rows)
        .expect("zero sign filter should execute");

        assert_eq!(
            positives,
            vec![
                WeightedRow::new(vec![int(1), SqliteValue::Float(3.5)], 7),
                WeightedRow::new(vec![int(2), SqliteValue::Float(f64::INFINITY)], -2),
            ]
        );
        assert_eq!(
            negatives,
            vec![
                WeightedRow::new(vec![int(3), SqliteValue::Float(-2.25)], 4),
                WeightedRow::new(vec![int(4), SqliteValue::Float(f64::NEG_INFINITY)], -5),
            ]
        );
        assert_eq!(
            zeroes,
            vec![
                WeightedRow::new(vec![int(5), SqliteValue::Float(0.0)], 6),
                WeightedRow::new(vec![int(6), SqliteValue::Float(-0.0)], -7),
            ]
        );
    }

    #[test]
    fn filter_float_sign_rejects_non_float_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatSign {
            column: 1,
            sign: FloatSign::Positive,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("non-float predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotFloat { column: 1 });
    }

    #[test]
    fn filter_float_sign_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatSign {
            column: 2,
            sign: FloatSign::Zero,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("invalid real-sign predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_float_compare_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatCompare {
            column: 1,
            op: FloatComparison::GreaterOrEqual,
            value: SqliteValue::Float(1.5),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), SqliteValue::Float(1.25)], 3),
            WeightedRow::new(vec![int(2), SqliteValue::Float(1.5)], -2),
            WeightedRow::new(vec![int(3), SqliteValue::Float(2.0)], 4),
            WeightedRow::new(vec![int(4), SqliteValue::Float(f64::INFINITY)], 5),
            WeightedRow::new(vec![int(5), SqliteValue::Float(f64::NAN)], 6),
            WeightedRow::new(vec![int(6), SqliteValue::Float(3.0)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), SqliteValue::Float(1.5)], -2),
                WeightedRow::new(vec![int(3), SqliteValue::Float(2.0)], 4),
                WeightedRow::new(vec![int(4), SqliteValue::Float(f64::INFINITY)], 5),
            ]
        );
    }

    #[test]
    fn filter_float_compare_supports_lower_bound_operators() {
        let rows = vec![
            WeightedRow::new(vec![int(1), SqliteValue::Float(-2.0)], 3),
            WeightedRow::new(vec![int(2), SqliteValue::Float(-1.0)], -2),
            WeightedRow::new(vec![int(3), SqliteValue::Float(0.0)], 4),
            WeightedRow::new(vec![int(4), SqliteValue::Float(1.0)], 5),
        ];

        let strict = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatCompare {
            column: 1,
            op: FloatComparison::LessThan,
            value: SqliteValue::Float(0.0),
        }])
        .execute(&rows)
        .expect("strict comparison should execute");
        let inclusive = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatCompare {
            column: 1,
            op: FloatComparison::LessOrEqual,
            value: SqliteValue::Float(0.0),
        }])
        .execute(&rows)
        .expect("inclusive comparison should execute");

        assert_eq!(
            strict,
            vec![
                WeightedRow::new(vec![int(1), SqliteValue::Float(-2.0)], 3),
                WeightedRow::new(vec![int(2), SqliteValue::Float(-1.0)], -2),
            ]
        );
        assert_eq!(
            inclusive,
            vec![
                WeightedRow::new(vec![int(1), SqliteValue::Float(-2.0)], 3),
                WeightedRow::new(vec![int(2), SqliteValue::Float(-1.0)], -2),
                WeightedRow::new(vec![int(3), SqliteValue::Float(0.0)], 4),
            ]
        );
    }

    #[test]
    fn filter_float_compare_rejects_non_float_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatCompare {
            column: 1,
            op: FloatComparison::GreaterThan,
            value: SqliteValue::Float(1.0),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("non-float predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotFloat { column: 1 });
    }

    #[test]
    fn filter_float_compare_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatCompare {
            column: 2,
            op: FloatComparison::GreaterThan,
            value: SqliteValue::Float(1.0),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(2.0)])])
            .expect_err("invalid real comparison predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_float_between_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatBetween {
            column: 1,
            lower: SqliteValue::Float(-1.0),
            upper: SqliteValue::Float(2.0),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), SqliteValue::Float(-2.0)], 3),
            WeightedRow::new(vec![int(2), SqliteValue::Float(-1.0)], -2),
            WeightedRow::new(vec![int(3), SqliteValue::Float(0.0)], 4),
            WeightedRow::new(vec![int(4), SqliteValue::Float(2.0)], 5),
            WeightedRow::new(vec![int(5), SqliteValue::Float(f64::INFINITY)], 6),
            WeightedRow::new(vec![int(6), SqliteValue::Float(f64::NAN)], 7),
            WeightedRow::new(vec![int(7), SqliteValue::Float(1.5)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), SqliteValue::Float(-1.0)], -2),
                WeightedRow::new(vec![int(3), SqliteValue::Float(0.0)], 4),
                WeightedRow::new(vec![int(4), SqliteValue::Float(2.0)], 5),
            ]
        );
    }

    #[test]
    fn filter_float_between_inverted_range_matches_no_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatBetween {
            column: 1,
            lower: SqliteValue::Float(3.0),
            upper: SqliteValue::Float(1.0),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), SqliteValue::Float(1.5)], 3),
            WeightedRow::new(vec![int(2), SqliteValue::Float(2.0)], -2),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            Vec::<WeightedRow>::new()
        );
    }

    #[test]
    fn filter_float_between_nan_bound_matches_no_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatBetween {
            column: 1,
            lower: SqliteValue::Float(f64::NAN),
            upper: SqliteValue::Float(2.0),
        }]);
        let rows = vec![WeightedRow::new(vec![int(1), SqliteValue::Float(1.5)], 3)];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            Vec::<WeightedRow>::new()
        );
    }

    #[test]
    fn filter_float_between_rejects_non_float_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatBetween {
            column: 1,
            lower: SqliteValue::Float(1.0),
            upper: SqliteValue::Float(2.0),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("non-float predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotFloat { column: 1 });
    }

    #[test]
    fn filter_float_between_rejects_non_float_bounds() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatBetween {
            column: 1,
            lower: int(1),
            upper: SqliteValue::Float(2.0),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("non-float range bound should fail");

        assert_eq!(err, DataflowError::PredicateValueNotFloat { column: 1 });
    }

    #[test]
    fn filter_float_between_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatBetween {
            column: 2,
            lower: SqliteValue::Float(1.0),
            upper: SqliteValue::Float(2.0),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("invalid real-range predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_float_not_between_keeps_outside_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatNotBetween {
            column: 1,
            lower: SqliteValue::Float(-1.0),
            upper: SqliteValue::Float(2.0),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), SqliteValue::Float(-2.0)], 3),
            WeightedRow::new(vec![int(2), SqliteValue::Float(-1.0)], -2),
            WeightedRow::new(vec![int(3), SqliteValue::Float(0.0)], 4),
            WeightedRow::new(vec![int(4), SqliteValue::Float(2.0)], 5),
            WeightedRow::new(vec![int(5), SqliteValue::Float(f64::INFINITY)], 6),
            WeightedRow::new(vec![int(6), SqliteValue::Float(f64::NEG_INFINITY)], -7),
            WeightedRow::new(vec![int(7), SqliteValue::Float(f64::NAN)], 8),
            WeightedRow::new(vec![int(8), SqliteValue::Float(3.0)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), SqliteValue::Float(-2.0)], 3),
                WeightedRow::new(vec![int(5), SqliteValue::Float(f64::INFINITY)], 6),
                WeightedRow::new(vec![int(6), SqliteValue::Float(f64::NEG_INFINITY)], -7),
            ]
        );
    }

    #[test]
    fn filter_float_not_between_inverted_range_keeps_all_finite_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatNotBetween {
            column: 1,
            lower: SqliteValue::Float(3.0),
            upper: SqliteValue::Float(1.0),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), SqliteValue::Float(1.5)], 3),
            WeightedRow::new(vec![int(2), SqliteValue::Float(2.0)], -2),
            WeightedRow::new(vec![int(3), SqliteValue::Float(f64::NAN)], 4),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            vec![
                WeightedRow::new(vec![int(1), SqliteValue::Float(1.5)], 3),
                WeightedRow::new(vec![int(2), SqliteValue::Float(2.0)], -2),
            ]
        );
    }

    #[test]
    fn filter_float_not_between_nan_bound_matches_no_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatNotBetween {
            column: 1,
            lower: SqliteValue::Float(f64::NAN),
            upper: SqliteValue::Float(2.0),
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), SqliteValue::Float(1.5)], 3),
            WeightedRow::new(vec![int(2), SqliteValue::Float(3.0)], -2),
        ];

        assert_eq!(
            automaton.execute(&rows).expect("dataflow should execute"),
            Vec::<WeightedRow>::new()
        );
    }

    #[test]
    fn filter_float_not_between_rejects_non_float_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatNotBetween {
            column: 1,
            lower: SqliteValue::Float(1.0),
            upper: SqliteValue::Float(2.0),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("non-float predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotFloat { column: 1 });
    }

    #[test]
    fn filter_float_not_between_rejects_non_float_bounds() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatNotBetween {
            column: 1,
            lower: SqliteValue::Float(1.0),
            upper: text("high"),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("non-float range bound should fail");

        assert_eq!(err, DataflowError::PredicateValueNotFloat { column: 1 });
    }

    #[test]
    fn filter_float_not_between_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterFloatNotBetween {
            column: 2,
            lower: SqliteValue::Float(1.0),
            upper: SqliteValue::Float(2.0),
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("invalid real-outside-range predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_integer_compare_keeps_matching_weighted_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterIntegerCompare {
            column: 1,
            op: IntegerComparison::GreaterOrEqual,
            value: 10,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(9)], 3),
            WeightedRow::new(vec![int(2), int(10)], -2),
            WeightedRow::new(vec![int(3), int(11)], 4),
            WeightedRow::new(vec![int(4), int(12)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), int(10)], -2),
                WeightedRow::new(vec![int(3), int(11)], 4),
            ]
        );
    }

    #[test]
    fn filter_integer_compare_rejects_non_integer_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterIntegerCompare {
            column: 1,
            op: IntegerComparison::LessThan,
            value: 10,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("non-integer predicate input should fail");

        assert_eq!(err, DataflowError::PredicateValueNotInteger { column: 1 });
    }

    #[test]
    fn filter_integer_compare_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterIntegerCompare {
            column: 2,
            op: IntegerComparison::LessOrEqual,
            value: 10,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("invalid predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_null_keeps_matching_weighted_rows() {
        let is_null = DataflowAutomaton::new(vec![DataflowOperator::FilterNull {
            column: 1,
            predicate: NullPredicate::IsNull,
        }]);
        let is_not_null = DataflowAutomaton::new(vec![DataflowOperator::FilterNull {
            column: 1,
            predicate: NullPredicate::IsNotNull,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), SqliteValue::Null], 3),
            WeightedRow::new(vec![int(2), int(20)], -2),
            WeightedRow::new(vec![int(3), SqliteValue::Null], 0),
        ];

        assert_eq!(
            is_null.execute(&rows).expect("is-null stream"),
            vec![WeightedRow::new(vec![int(1), SqliteValue::Null], 3)]
        );
        assert_eq!(
            is_not_null.execute(&rows).expect("is-not-null stream"),
            vec![WeightedRow::new(vec![int(2), int(20)], -2)]
        );
    }

    #[test]
    fn filter_null_rejects_out_of_bounds_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::FilterNull {
            column: 2,
            predicate: NullPredicate::IsNull,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("invalid null predicate column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn filter_weight_sign_keeps_requested_delta_side_and_elides_zeroes() {
        let positive = DataflowAutomaton::new(vec![DataflowOperator::FilterWeightSign {
            sign: WeightSign::Positive,
        }]);
        let negative = DataflowAutomaton::new(vec![DataflowOperator::FilterWeightSign {
            sign: WeightSign::Negative,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1)], 3),
            WeightedRow::new(vec![int(2)], -2),
            WeightedRow::new(vec![int(3)], 0),
        ];

        assert_eq!(
            positive.execute(&rows).expect("positive stream"),
            vec![WeightedRow::new(vec![int(1)], 3)]
        );
        assert_eq!(
            negative.execute(&rows).expect("negative stream"),
            vec![WeightedRow::new(vec![int(2)], -2)]
        );
    }

    #[test]
    fn append_literal_column_preserves_weights_and_elides_zeroes() {
        let automaton =
            DataflowAutomaton::new(vec![DataflowOperator::AppendLiteral { value: int(99) }]);
        let rows = vec![
            WeightedRow::new(vec![int(1)], 3),
            WeightedRow::new(vec![int(2)], -2),
            WeightedRow::new(vec![int(3)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), int(99)], 3),
                WeightedRow::new(vec![int(2), int(99)], -2),
            ]
        );
    }

    #[test]
    fn consolidate_by_key_preserves_first_key_order_and_elides_zeroes() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::ConsolidateByKey {
            key_columns: vec![0],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(2), int(20)], 1),
            WeightedRow::new(vec![int(1), int(10)], 4),
            WeightedRow::new(vec![int(2), int(21)], -1),
            WeightedRow::new(vec![int(1), int(11)], 3),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![WeightedRow::new(vec![int(1)], 7)],
            "key 2 should cancel out and key 1 should retain first-seen ordering"
        );
    }

    #[test]
    fn consolidate_rows_operator_preserves_first_row_order_and_elides_zeroes() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::ConsolidateRows]);
        let rows = vec![
            WeightedRow::new(vec![int(2), int(20)], 1),
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::new(vec![int(2), int(20)], -1),
            WeightedRow::new(vec![int(1), int(10)], 4),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(actual, vec![WeightedRow::new(vec![int(1), int(10)], 7)]);
    }

    #[test]
    fn consolidate_weight_signs_consolidates_and_normalizes_survivors() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::ConsolidateWeightSigns]);
        let rows = vec![
            WeightedRow::new(vec![int(2), int(20)], 1),
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::new(vec![int(2), int(20)], -1),
            WeightedRow::new(vec![int(1), int(10)], -7),
            WeightedRow::new(vec![int(3), int(30)], 4),
            WeightedRow::new(vec![int(4), int(40)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::delete(vec![int(1), int(10)]),
                WeightedRow::insert(vec![int(3), int(30)]),
            ]
        );
    }

    #[test]
    fn consolidate_weight_signs_empty_after_cancellation() {
        let rows = vec![
            WeightedRow::new(vec![int(1)], 9),
            WeightedRow::new(vec![int(1)], -9),
            WeightedRow::new(vec![int(2)], 0),
        ];

        let actual = super::consolidate_weight_signs(&rows);

        assert_eq!(actual, Vec::<WeightedRow>::new());
    }

    #[test]
    fn count_by_key_appends_counts_and_elides_zero_groups() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::CountByKey {
            key_columns: vec![0],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(2), int(20)], 1),
            WeightedRow::new(vec![int(1), int(10)], 4),
            WeightedRow::new(vec![int(2), int(21)], -1),
            WeightedRow::new(vec![int(1), int(11)], 3),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(actual, vec![WeightedRow::insert(vec![int(1), int(7)])]);
    }

    #[test]
    fn count_by_key_rejects_out_of_bounds_key_columns() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::CountByKey {
            key_columns: vec![2],
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), int(2)])])
            .expect_err("invalid key column should fail");

        assert_eq!(
            err,
            DataflowError::ColumnOutOfBounds {
                column: 2,
                width: 2
            }
        );
    }

    #[test]
    fn sum_integer_by_key_appends_weighted_sums_and_elides_zero_groups() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::SumIntegerByKey {
            key_columns: vec![0],
            value_column: 1,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(2), int(20)], 1),
            WeightedRow::new(vec![int(1), int(10)], 4),
            WeightedRow::new(vec![int(2), int(20)], -1),
            WeightedRow::new(vec![int(1), int(11)], 3),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(actual, vec![WeightedRow::insert(vec![int(1), int(73)])]);
    }

    #[test]
    fn sum_integer_by_key_rejects_non_integer_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::SumIntegerByKey {
            key_columns: vec![0],
            value_column: 1,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("non-integer aggregate input should fail");

        assert_eq!(err, DataflowError::AggregateValueNotInteger { column: 1 });
    }

    #[test]
    fn min_integer_by_key_appends_minimum_for_positive_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::MinIntegerByKey {
            key_columns: vec![0],
            value_column: 1,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(2), int(20)], 1),
            WeightedRow::new(vec![int(1), int(10)], 1),
            WeightedRow::new(vec![int(2), int(7)], 1),
            WeightedRow::new(vec![int(1), int(3)], -1),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::insert(vec![int(2), int(7)]),
                WeightedRow::insert(vec![int(1), int(10)]),
            ]
        );
    }

    #[test]
    fn min_integer_by_key_rejects_non_integer_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::MinIntegerByKey {
            key_columns: vec![0],
            value_column: 1,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("non-integer aggregate input should fail");

        assert_eq!(err, DataflowError::AggregateValueNotInteger { column: 1 });
    }

    #[test]
    fn max_integer_by_key_appends_maximum_for_positive_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::MaxIntegerByKey {
            key_columns: vec![0],
            value_column: 1,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(2), int(20)], 1),
            WeightedRow::new(vec![int(1), int(10)], 1),
            WeightedRow::new(vec![int(2), int(7)], 1),
            WeightedRow::new(vec![int(1), int(30)], -1),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::insert(vec![int(2), int(20)]),
                WeightedRow::insert(vec![int(1), int(10)]),
            ]
        );
    }

    #[test]
    fn max_integer_by_key_rejects_non_integer_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::MaxIntegerByKey {
            key_columns: vec![0],
            value_column: 1,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("non-integer aggregate input should fail");

        assert_eq!(err, DataflowError::AggregateValueNotInteger { column: 1 });
    }

    #[test]
    fn average_integer_by_key_appends_average_for_positive_rows() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::AverageIntegerByKey {
            key_columns: vec![0],
            value_column: 1,
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(2), int(20)], 2),
            WeightedRow::new(vec![int(1), int(10)], 1),
            WeightedRow::new(vec![int(2), int(8)], 1),
            WeightedRow::new(vec![int(1), int(30)], -1),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::insert(vec![int(2), SqliteValue::Float(16.0)]),
                WeightedRow::insert(vec![int(1), SqliteValue::Float(10.0)]),
            ]
        );
    }

    #[test]
    fn average_integer_by_key_rejects_non_integer_values() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::AverageIntegerByKey {
            key_columns: vec![0],
            value_column: 1,
        }]);

        let err = automaton
            .execute(&[WeightedRow::insert(vec![int(1), SqliteValue::Float(1.5)])])
            .expect_err("non-integer aggregate input should fail");

        assert_eq!(err, DataflowError::AggregateValueNotInteger { column: 1 });
    }

    #[test]
    fn scale_weight_operator_inverts_weights_and_elides_zeroes() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::ScaleWeight { factor: -1 }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::new(vec![int(2), int(20)], -2),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), int(10)], -3),
                WeightedRow::new(vec![int(2), int(20)], 2),
            ]
        );
    }

    #[test]
    fn scale_weights_zero_factor_elides_all_rows() {
        let rows = vec![
            WeightedRow::new(vec![int(1)], 3),
            WeightedRow::new(vec![int(2)], -2),
        ];

        let actual = scale_weights(&rows, 0);

        assert_eq!(actual, Vec::<WeightedRow>::new());
    }

    #[test]
    fn scale_weights_saturates_on_overflow() {
        let rows = vec![WeightedRow::new(vec![int(1)], i64::MAX)];

        let actual = scale_weights(&rows, 2);

        assert_eq!(actual, vec![WeightedRow::new(vec![int(1)], i64::MAX)]);
    }

    #[test]
    fn negate_weight_operator_reverses_delta_polarity() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::NegateWeight]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::new(vec![int(2), int(20)], -2),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), int(10)], -3),
                WeightedRow::new(vec![int(2), int(20)], 2),
            ]
        );
    }

    #[test]
    fn negate_weights_saturates_minimum_weight() {
        let rows = vec![WeightedRow::new(vec![int(1)], i64::MIN)];

        let actual = super::negate_weights(&rows);

        assert_eq!(actual, vec![WeightedRow::new(vec![int(1)], i64::MAX)]);
    }

    #[test]
    fn normalize_weight_sign_operator_collapses_magnitude_and_preserves_polarity() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::NormalizeWeightSign]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], i64::MAX),
            WeightedRow::new(vec![int(2), int(20)], -7),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::insert(vec![int(1), int(10)]),
                WeightedRow::delete(vec![int(2), int(20)]),
            ]
        );
    }

    #[test]
    fn normalize_weight_sign_handles_minimum_weight_without_overflow() {
        let rows = vec![WeightedRow::new(vec![int(1)], i64::MIN)];

        let actual = super::normalize_weight_sign(&rows);

        assert_eq!(actual, vec![WeightedRow::delete(vec![int(1)])]);
    }

    #[test]
    fn add_rows_operator_adds_static_batch_and_consolidates() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::AddRows {
            rows: vec![
                WeightedRow::new(vec![int(1), int(10)], -1),
                WeightedRow::new(vec![int(3), int(30)], 2),
                WeightedRow::new(vec![int(4), int(40)], 0),
            ],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::insert(vec![int(2), int(20)]),
            WeightedRow::new(vec![int(5), int(50)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), int(10)], 2),
                WeightedRow::insert(vec![int(2), int(20)]),
                WeightedRow::new(vec![int(3), int(30)], 2),
            ]
        );
    }

    #[test]
    fn add_rows_elides_exact_cancellations() {
        let left = vec![WeightedRow::insert(vec![int(1)])];
        let right = vec![WeightedRow::delete(vec![int(1)])];

        let actual = super::add_rows(&left, &right);

        assert_eq!(actual, Vec::<WeightedRow>::new());
    }

    #[test]
    fn subtract_rows_operator_subtracts_static_batch_and_consolidates() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::SubtractRows {
            rows: vec![
                WeightedRow::new(vec![int(1), int(10)], 1),
                WeightedRow::new(vec![int(3), int(30)], 2),
                WeightedRow::new(vec![int(4), int(40)], 0),
            ],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::insert(vec![int(2), int(20)]),
            WeightedRow::new(vec![int(5), int(50)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), int(10)], 2),
                WeightedRow::insert(vec![int(2), int(20)]),
                WeightedRow::new(vec![int(3), int(30)], -2),
            ]
        );
    }

    #[test]
    fn subtract_rows_elides_exact_cancellations() {
        let left = vec![
            WeightedRow::insert(vec![int(1)]),
            WeightedRow::new(vec![int(2)], 2),
        ];
        let right = vec![
            WeightedRow::insert(vec![int(1)]),
            WeightedRow::new(vec![int(2)], 2),
        ];

        let actual = super::subtract_rows(&left, &right);

        assert_eq!(actual, Vec::<WeightedRow>::new());
    }

    #[test]
    fn retain_rows_in_operator_keeps_current_rows_with_static_membership() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::RetainRowsIn {
            rows: vec![
                WeightedRow::new(vec![int(1), int(10)], 2),
                WeightedRow::new(vec![int(3), int(30)], -1),
                WeightedRow::new(vec![int(4), int(40)], 1),
                WeightedRow::new(vec![int(4), int(40)], -1),
                WeightedRow::new(vec![int(5), int(50)], 0),
            ],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::new(vec![int(2), int(20)], -2),
            WeightedRow::insert(vec![int(3), int(30)]),
            WeightedRow::new(vec![int(4), int(40)], 5),
            WeightedRow::new(vec![int(5), int(50)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), int(10)], 3),
                WeightedRow::insert(vec![int(3), int(30)]),
            ]
        );
    }

    #[test]
    fn retain_rows_in_empty_static_batch_elides_all_rows() {
        let rows = vec![
            WeightedRow::insert(vec![int(1)]),
            WeightedRow::delete(vec![int(2)]),
        ];

        let actual = super::retain_rows_in(&rows, &[]);

        assert_eq!(actual, Vec::<WeightedRow>::new());
    }

    #[test]
    fn reject_rows_in_operator_drops_current_rows_with_static_membership() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::RejectRowsIn {
            rows: vec![
                WeightedRow::new(vec![int(1), int(10)], 2),
                WeightedRow::new(vec![int(3), int(30)], -1),
                WeightedRow::new(vec![int(4), int(40)], 1),
                WeightedRow::new(vec![int(4), int(40)], -1),
                WeightedRow::new(vec![int(5), int(50)], 0),
            ],
        }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::new(vec![int(2), int(20)], -2),
            WeightedRow::insert(vec![int(3), int(30)]),
            WeightedRow::new(vec![int(4), int(40)], 5),
            WeightedRow::new(vec![int(5), int(50)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(2), int(20)], -2),
                WeightedRow::new(vec![int(4), int(40)], 5),
            ]
        );
    }

    #[test]
    fn reject_rows_in_empty_static_batch_preserves_nonzero_rows() {
        let rows = vec![
            WeightedRow::insert(vec![int(1)]),
            WeightedRow::delete(vec![int(2)]),
            WeightedRow::new(vec![int(3)], 0),
        ];

        let actual = super::reject_rows_in(&rows, &[]);

        assert_eq!(
            actual,
            vec![
                WeightedRow::insert(vec![int(1)]),
                WeightedRow::delete(vec![int(2)]),
            ]
        );
    }

    #[test]
    fn set_weight_operator_normalizes_surviving_row_weights() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::SetWeight { weight: 1 }]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::new(vec![int(2), int(20)], -2),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::insert(vec![int(1), int(10)]),
                WeightedRow::insert(vec![int(2), int(20)]),
            ]
        );
    }

    #[test]
    fn set_weights_zero_output_weight_elides_all_rows() {
        let rows = vec![
            WeightedRow::new(vec![int(1)], 3),
            WeightedRow::new(vec![int(2)], -2),
        ];

        let actual = set_weights(&rows, 0);

        assert_eq!(actual, Vec::<WeightedRow>::new());
    }

    #[test]
    fn threshold_positive_consolidates_and_normalizes_membership() {
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], 2),
            WeightedRow::new(vec![int(2), int(20)], 1),
            WeightedRow::new(vec![int(1), int(10)], -1),
            WeightedRow::new(vec![int(2), int(20)], -1),
            WeightedRow::new(vec![int(3), int(30)], -4),
            WeightedRow::new(vec![int(4), int(40)], 0),
        ];

        let actual = threshold_positive(&rows);

        assert_eq!(actual, vec![WeightedRow::insert(vec![int(1), int(10)])]);
    }

    #[test]
    fn threshold_positive_operator_can_follow_weight_scaling() {
        let automaton = DataflowAutomaton::new(vec![
            DataflowOperator::ScaleWeight { factor: -1 },
            DataflowOperator::ThresholdPositive,
        ]);
        let rows = vec![
            WeightedRow::new(vec![int(1)], -3),
            WeightedRow::new(vec![int(2)], 2),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(actual, vec![WeightedRow::insert(vec![int(1)])]);
    }

    #[test]
    fn append_weight_column_materializes_delta_weights_for_emission() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::AppendWeightColumn]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::new(vec![int(2), int(20)], -2),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::insert(vec![int(1), int(10), int(3)]),
                WeightedRow::insert(vec![int(2), int(20), int(-2)]),
            ]
        );
    }

    #[test]
    fn append_weight_sign_column_materializes_delta_polarity_for_emission() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::AppendWeightSignColumn]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::new(vec![int(2), int(20)], -2),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::insert(vec![int(1), int(10), int(1)]),
                WeightedRow::insert(vec![int(2), int(20), int(-1)]),
            ]
        );
    }

    #[test]
    fn append_weight_sign_column_handles_minimum_weight_without_overflow() {
        let rows = vec![WeightedRow::new(vec![int(1)], i64::MIN)];

        let actual = super::append_weight_sign_column(&rows);

        assert_eq!(actual, vec![WeightedRow::insert(vec![int(1), int(-1)])]);
    }

    #[test]
    fn append_weight_magnitude_column_materializes_absolute_delta_weight() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::AppendWeightMagnitudeColumn]);
        let rows = vec![
            WeightedRow::new(vec![int(1), int(10)], 3),
            WeightedRow::new(vec![int(2), int(20)], -2),
            WeightedRow::new(vec![int(3), int(30)], 0),
        ];

        let actual = automaton.execute(&rows).expect("dataflow should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::insert(vec![int(1), int(10), int(3)]),
                WeightedRow::insert(vec![int(2), int(20), int(2)]),
            ]
        );
    }

    #[test]
    fn append_weight_magnitude_column_saturates_minimum_weight() {
        let rows = vec![WeightedRow::new(vec![int(1)], i64::MIN)];

        let actual = super::append_weight_magnitude_column(&rows);

        assert_eq!(
            actual,
            vec![WeightedRow::insert(vec![int(1), int(i64::MAX)])]
        );
    }

    #[test]
    fn delta_join_left_multiplies_weights() {
        let spec = JoinKeySpec::new(vec![0], vec![0]);
        let delta_left = vec![
            WeightedRow::new(vec![int(1), int(10)], 2),
            WeightedRow::new(vec![int(2), int(20)], 5),
        ];
        let stable_right = vec![
            WeightedRow::new(vec![int(1), int(100)], -3),
            WeightedRow::new(vec![int(3), int(300)], 7),
        ];

        let actual =
            delta_join_left(&delta_left, &stable_right, &spec).expect("join should execute");

        assert_eq!(
            actual,
            vec![WeightedRow::new(
                vec![int(1), int(10), int(1), int(100)],
                -6
            )]
        );
    }

    #[test]
    fn delta_join_right_multiplies_weights() {
        let spec = JoinKeySpec::new(vec![0], vec![0]);
        let stable_left = vec![WeightedRow::new(vec![int(9), int(90)], 4)];
        let delta_right = vec![
            WeightedRow::new(vec![int(9), int(900)], -2),
            WeightedRow::new(vec![int(8), int(800)], -2),
        ];

        let actual =
            delta_join_right(&stable_left, &delta_right, &spec).expect("join should execute");

        assert_eq!(
            actual,
            vec![WeightedRow::new(
                vec![int(9), int(90), int(9), int(900)],
                -8
            )]
        );
    }

    #[test]
    fn join_key_arity_mismatch_is_rejected() {
        let spec = JoinKeySpec::new(vec![0, 1], vec![0]);

        let err = delta_join_left(&[], &[], &spec).expect_err("arity mismatch must fail");

        assert_eq!(
            err,
            DataflowError::JoinKeyArityMismatch { left: 2, right: 1 }
        );
    }

    #[test]
    fn delta_join_update_includes_both_sides_and_delta_delta_term() {
        let spec = JoinKeySpec::new(vec![0], vec![0]);
        let stable_left = vec![WeightedRow::new(vec![int(1), int(10)], 1)];
        let stable_right = vec![WeightedRow::new(vec![int(1), int(100)], 1)];
        let delta_left = vec![WeightedRow::new(vec![int(1), int(11)], 1)];
        let delta_right = vec![WeightedRow::new(vec![int(1), int(101)], 1)];

        let actual = delta_join_update(
            &stable_left,
            &delta_left,
            &stable_right,
            &delta_right,
            &spec,
        )
        .expect("join update should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), int(11), int(1), int(100)], 1),
                WeightedRow::new(vec![int(1), int(10), int(1), int(101)], 1),
                WeightedRow::new(vec![int(1), int(11), int(1), int(101)], 1),
            ]
        );
    }

    #[test]
    fn delta_join_update_consolidates_duplicate_output_rows() {
        let spec = JoinKeySpec::new(vec![0], vec![0]);
        let stable_left = vec![WeightedRow::new(vec![int(1), int(10)], 1)];
        let stable_right = vec![WeightedRow::new(vec![int(1), int(100)], 1)];
        let delta_left = vec![WeightedRow::new(vec![int(1), int(10)], 1)];
        let delta_right = vec![WeightedRow::new(vec![int(1), int(100)], -1)];

        let actual = delta_join_update(
            &stable_left,
            &delta_left,
            &stable_right,
            &delta_right,
            &spec,
        )
        .expect("join update should execute");

        assert_eq!(
            actual,
            vec![WeightedRow::new(
                vec![int(1), int(10), int(1), int(100)],
                -1
            )]
        );
    }

    #[test]
    fn automaton_delta_join_update_uses_current_rows_as_left_delta() {
        let automaton = DataflowAutomaton::new(vec![DataflowOperator::DeltaJoinUpdate {
            stable_left: vec![WeightedRow::new(vec![int(1), int(10)], 1)],
            stable_right: vec![WeightedRow::new(vec![int(1), int(100)], 1)],
            delta_right: vec![WeightedRow::new(vec![int(1), int(101)], 1)],
            key_spec: JoinKeySpec::new(vec![0], vec![0]),
        }]);

        let actual = automaton
            .execute(&[WeightedRow::new(vec![int(1), int(11)], 1)])
            .expect("join update operator should execute");

        assert_eq!(
            actual,
            vec![
                WeightedRow::new(vec![int(1), int(11), int(1), int(100)], 1),
                WeightedRow::new(vec![int(1), int(10), int(1), int(101)], 1),
                WeightedRow::new(vec![int(1), int(11), int(1), int(101)], 1),
            ]
        );
    }
}
