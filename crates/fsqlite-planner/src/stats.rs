//! Statistics for cost-based query planning.
//!
//! Implements histograms, NDV (Number of Distinct Values), and column statistics
//! used by the optimizer to estimate cardinality and selectivity.
//!
//! # Equi-Depth Histograms
//! We use equi-depth histograms where each bucket contains approximately the same
//! number of rows. This provides better resolution for skewed data distributions
//! compared to equi-width histograms.
//!
//! # Estimation
//! - Equality (`=`, `IS`): `1 / NDV` (or `1 / row_count` if unique).
//! - Range (`<`, `>`, `BETWEEN`): Interpolation within histogram buckets.
//! - NULL: `null_count / row_count`.

use fsqlite_types::value::SqliteValue;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

/// A single bucket in an equi-depth histogram.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistogramBucket {
    /// Inclusive lower bound of the bucket.
    pub lower: SqliteValue,
    /// Inclusive upper bound of the bucket.
    pub upper: SqliteValue,
    /// Number of rows in this bucket.
    pub count: u64,
    /// Number of distinct values in this bucket (if known).
    pub ndv: u64,
}

impl HistogramBucket {
    /// Check if a value falls within this bucket [lower, upper].
    pub fn contains(&self, value: &SqliteValue) -> bool {
        value >= &self.lower && value <= &self.upper
    }
}

/// A histogram approximating the distribution of values in a column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Histogram {
    /// Ordered list of buckets.
    /// Buckets should cover the full range of non-NULL values.
    pub buckets: Vec<HistogramBucket>,
}

impl Histogram {
    /// Estimate the number of rows satisfying `col = value`.
    pub fn estimate_equality_rows(&self, value: &SqliteValue) -> f64 {
        for bucket in &self.buckets {
            if bucket.contains(value) {
                // Uniform assumption within bucket: count / ndv
                // If NDV is unknown (0), assume 1.
                let ndv = bucket.ndv.max(1) as f64;
                return bucket.count as f64 / ndv;
            }
        }
        // Value not covered by histogram (out of bounds) -> assume minimal selectivity
        1.0
    }

    /// Estimate the number of rows satisfying `col < value` (strictly less).
    pub fn estimate_less_than_rows(&self, value: &SqliteValue) -> f64 {
        let mut count = 0.0;
        for bucket in &self.buckets {
            if value > &bucket.upper {
                // Bucket is entirely below value
                count += bucket.count as f64;
            } else if value <= &bucket.lower {
                // Bucket is entirely above value
                break;
            } else {
                // Value falls inside this bucket. Interpolate.
                // Fraction = (value - lower) / (upper - lower)
                // Note: SqliteValue subtraction is not directly defined for all types.
                // We use a heuristic interpolation for numeric types.
                let fraction = interpolate_position(&bucket.lower, &bucket.upper, value);
                count = (bucket.count as f64).mul_add(fraction, count);
                break;
            }
        }
        count
    }

    /// Estimate the number of rows satisfying `col > value` (strictly greater).
    pub fn estimate_greater_than_rows(&self, value: &SqliteValue) -> f64 {
        let mut count = 0.0;
        for bucket in self.buckets.iter().rev() {
            if value < &bucket.lower {
                // Bucket is entirely above value
                count += bucket.count as f64;
            } else if value >= &bucket.upper {
                // Bucket is entirely below value
                break;
            } else {
                // Value falls inside this bucket. Interpolate.
                // Fraction = (upper - value) / (upper - lower)
                let fraction = 1.0 - interpolate_position(&bucket.lower, &bucket.upper, value);
                count = (bucket.count as f64).mul_add(fraction, count);
                break;
            }
        }
        count
    }
}

fn bytes_to_fraction(bytes: &[u8]) -> f64 {
    const PREFIX_BYTES: usize = size_of::<u64>();
    const FRACTION_SCALE: f64 = (1_u64 << f64::MANTISSA_DIGITS) as f64;

    // Interpret the first eight bytes as a big-endian base-256 fraction. A
    // binary64 cannot represent all 64 fraction bits: values sufficiently
    // close to one (including eight 0xFF bytes) round to 1.0. Truncating the
    // integer prefix to binary64's 53 significant bits before conversion
    // gives an exact dyadic fraction, preserves lexicographic monotonicity,
    // and guarantees the result is in the required half-open interval [0, 1).
    let mut prefix_bytes = [0_u8; PREFIX_BYTES];
    for (destination, source) in prefix_bytes.iter_mut().zip(bytes) {
        *destination = *source;
    }
    let prefix = u64::from_be_bytes(prefix_bytes);
    let numerator = prefix >> (u64::BITS - f64::MANTISSA_DIGITS);
    numerator as f64 / FRACTION_SCALE
}

/// Heuristic linear interpolation of `val` between `min` and `max`.
/// Returns a value in [0.0, 1.0].
fn interpolate_position(min: &SqliteValue, max: &SqliteValue, val: &SqliteValue) -> f64 {
    match (min, max, val) {
        (SqliteValue::Integer(min_i), SqliteValue::Integer(max_i), SqliteValue::Integer(val_i)) => {
            if max_i <= min_i {
                return 0.5;
            }
            let range = *max_i as f64 - *min_i as f64;
            let offset = *val_i as f64 - *min_i as f64;
            let fraction = offset / range;
            if fraction.is_nan() {
                0.5
            } else {
                fraction.clamp(0.0, 1.0)
            }
        }
        (SqliteValue::Float(min_f), SqliteValue::Float(max_f), SqliteValue::Float(val_f)) => {
            if max_f <= min_f || min_f.is_nan() || max_f.is_nan() || val_f.is_nan() {
                return 0.5;
            }
            let range = max_f - min_f;
            let offset = val_f - min_f;
            let fraction = offset / range;
            if fraction.is_nan() {
                0.5
            } else {
                fraction.clamp(0.0, 1.0)
            }
        }
        (SqliteValue::Text(min_s), SqliteValue::Text(max_s), SqliteValue::Text(val_s)) => {
            if max_s <= min_s {
                return 0.5;
            }
            let min_frac = bytes_to_fraction(min_s.as_bytes());
            let max_frac = bytes_to_fraction(max_s.as_bytes());
            let val_frac = bytes_to_fraction(val_s.as_bytes());
            let range = max_frac - min_frac;
            if range <= 0.0 {
                return 0.5;
            }
            let offset = val_frac - min_frac;
            (offset / range).clamp(0.0, 1.0)
        }
        (SqliteValue::Blob(min_b), SqliteValue::Blob(max_b), SqliteValue::Blob(val_b)) => {
            if max_b <= min_b {
                return 0.5;
            }
            let min_frac = bytes_to_fraction(min_b);
            let max_frac = bytes_to_fraction(max_b);
            let val_frac = bytes_to_fraction(val_b);
            let range = max_frac - min_frac;
            if range <= 0.0 {
                return 0.5;
            }
            let offset = val_frac - min_frac;
            (offset / range).clamp(0.0, 1.0)
        }
        _ => 0.5,
    }
}

/// Statistics for a single column.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ColumnStats {
    /// Total number of rows in the table.
    pub table_row_count: u64,
    /// Number of NULL values.
    pub null_count: u64,
    /// Number of Distinct Values (NDV).
    pub ndv: u64,
    /// Minimum non-NULL value.
    pub min_value: Option<SqliteValue>,
    /// Maximum non-NULL value.
    pub max_value: Option<SqliteValue>,
    /// Average size of the column value in bytes (for I/O estimation).
    pub avg_width: f64,
    /// Histogram for range estimation.
    pub histogram: Option<Histogram>,
}

impl ColumnStats {
    /// Estimate selectivity of a predicate.
    /// Selectivity is P(predicate is true), range [0.0, 1.0].
    pub fn estimate_selectivity(&self, op: &Operator, value: &SqliteValue) -> f64 {
        if self.table_row_count == 0 {
            return 0.0;
        }

        // Base probability space is non-NULL rows (SQL tristate logic)
        let non_null_count = self.table_row_count.saturating_sub(self.null_count) as f64;
        if non_null_count <= 0.0 {
            return 0.0;
        }

        let estimated_matches = match op {
            Operator::Eq => {
                if let Some(hist) = &self.histogram {
                    hist.estimate_equality_rows(value)
                } else {
                    // Uniform assumption: 1 / NDV
                    let ndv = self.ndv.max(1) as f64;
                    non_null_count / ndv
                }
            }
            Operator::Lt => {
                if let Some(hist) = &self.histogram {
                    hist.estimate_less_than_rows(value)
                } else {
                    // Default 1/3 for range open-ended
                    non_null_count / 3.0
                }
            }
            Operator::Gt => {
                if let Some(hist) = &self.histogram {
                    hist.estimate_greater_than_rows(value)
                } else {
                    // Default 1/3
                    non_null_count / 3.0
                }
            }
            Operator::Le => {
                // Less than + Equality
                let lt = if let Some(hist) = &self.histogram {
                    hist.estimate_less_than_rows(value)
                } else {
                    non_null_count / 3.0
                };
                let eq = if let Some(hist) = &self.histogram {
                    hist.estimate_equality_rows(value)
                } else {
                    let ndv = self.ndv.max(1) as f64;
                    non_null_count / ndv
                };
                lt + eq
            }
            Operator::Ge => {
                let gt = if let Some(hist) = &self.histogram {
                    hist.estimate_greater_than_rows(value)
                } else {
                    non_null_count / 3.0
                };
                let eq = if let Some(hist) = &self.histogram {
                    hist.estimate_equality_rows(value)
                } else {
                    let ndv = self.ndv.max(1) as f64;
                    non_null_count / ndv
                };
                gt + eq
            }
            // For other operators (LIKE, GLOB, NE), use heuristics
            Operator::Ne => {
                // Compute the absolute Eq row estimate inline (not via
                // estimate_selectivity, which returns a fraction relative to
                // table_row_count, not non_null_count).
                let eq_matches = if let Some(hist) = &self.histogram {
                    hist.estimate_equality_rows(value)
                } else {
                    let ndv = self.ndv.max(1) as f64;
                    non_null_count / ndv
                };
                (non_null_count - eq_matches).max(0.0)
            }
            _ => non_null_count * 0.1, // Fallback heuristic
        };

        if self.table_row_count == 0 {
            return 0.0;
        }

        let fraction = estimated_matches / self.table_row_count as f64;
        if fraction.is_nan() {
            0.0
        } else {
            fraction.clamp(0.0, 1.0)
        }
    }
}

/// Abstract operator for selectivity estimation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    Glob,
    Is,
    IsNot,
}

/// Collection of statistics for a table.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableStatistics {
    pub row_count: u64,
    pub columns: HashMap<String, ColumnStats>,
}

// ---------------------------------------------------------------------------
// sqlite_stat1 parsing (PLANNER-1)
// ---------------------------------------------------------------------------

/// Parsed representation of a single row of the `sqlite_stat1` `stat` column.
///
/// SQLite's ANALYZE records one row per (table, index) pair in `sqlite_stat1`
/// with a whitespace-separated `stat` string. The first integer is the
/// approximate total row count for the table. Subsequent integers are the
/// approximate number of rows per distinct value of each indexed column,
/// computed cumulatively from the leading column.
///
/// For rows where `idx` is NULL (table-level stats), only the row count is
/// recorded and `per_column_distinct` is empty.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Stat1Summary {
    /// Total row count for the table.
    pub n_rows: u64,
    /// Approximate number of distinct values per indexed column, expressed as
    /// "rows per distinct value". Empty for table-only stats rows.
    pub per_column_distinct: Vec<u64>,
}

/// Parse the `stat` column text from a `sqlite_stat1` row.
///
/// The format is whitespace-separated integers:
/// - First integer: total table row count.
/// - Subsequent integers: approximate rows-per-distinct-value for each indexed
///   column (cumulative from left).
///
/// Returns `None` if the string is empty, contains no parseable integer in the
/// first position, or the leading row count overflows `u64`. Unparseable
/// trailing tokens are silently dropped (they are advisory) and non-numeric
/// garbage after the first token is ignored.
#[must_use]
pub fn parse_stat1(stat: &str) -> Option<Stat1Summary> {
    let mut parts = stat.split_ascii_whitespace();
    let first = parts.next()?;
    let n_rows: u64 = first.parse().ok()?;
    let per_column_distinct: Vec<u64> = parts.filter_map(|t| t.parse::<u64>().ok()).collect();
    Some(Stat1Summary {
        n_rows,
        per_column_distinct,
    })
}

// ---------------------------------------------------------------------------
// Sampling-based cardinality estimation (bd-1as.1)
// ---------------------------------------------------------------------------

/// Method used to produce a cardinality estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstimationMethod {
    /// Used histogram-based interpolation.
    Histogram,
    /// Used NDV-based uniform assumption.
    Ndv,
    /// Used sampling from provided rows.
    Sampling,
    /// Used default heuristic (no stats available).
    Heuristic,
}

/// A cardinality estimate with provenance.
#[derive(Debug, Clone)]
pub struct CardinalityEstimate {
    /// Estimated number of matching rows.
    pub estimated_rows: f64,
    /// Selectivity in [0.0, 1.0].
    pub selectivity: f64,
    /// Method used to produce the estimate.
    pub method: EstimationMethod,
}

impl ColumnStats {
    /// Estimate cardinality with sampling fallback.
    ///
    /// If a histogram is available, uses histogram-based interpolation.
    /// If a sample of values is provided and no histogram exists, estimates
    /// selectivity from the sample using the proportion of matching values.
    /// Otherwise falls back to NDV-based or default heuristic estimates.
    pub fn estimate_cardinality(
        &self,
        op: &Operator,
        value: &SqliteValue,
        sample: Option<&[SqliteValue]>,
    ) -> CardinalityEstimate {
        let rows = self.table_row_count as f64;
        if rows <= 0.0 {
            return CardinalityEstimate {
                estimated_rows: 0.0,
                selectivity: 0.0,
                method: EstimationMethod::Heuristic,
            };
        }

        // Try histogram first.
        if self.histogram.is_some() {
            let sel = self.estimate_selectivity(op, value);
            let span = tracing::debug_span!(
                target: "fsqlite.planner",
                "cost_estimate",
                table = tracing::field::Empty,
                estimated_rows = (sel * rows),
                actual_method = "histogram",
            );
            let _g = span.enter();
            return CardinalityEstimate {
                estimated_rows: sel * rows,
                selectivity: sel,
                method: EstimationMethod::Histogram,
            };
        }

        // Try sampling fallback.
        if let Some(sample) = sample {
            if !sample.is_empty() {
                let matching = sample
                    .iter()
                    .filter(|sv| cmp_matches(sv, *op, value))
                    .count();
                let sel = (matching as f64 / sample.len() as f64).clamp(0.0, 1.0);
                let span = tracing::debug_span!(
                    target: "fsqlite.planner",
                    "cost_estimate",
                    table = tracing::field::Empty,
                    estimated_rows = (sel * rows),
                    actual_method = "sampling",
                );
                let _g = span.enter();
                return CardinalityEstimate {
                    estimated_rows: sel * rows,
                    selectivity: sel,
                    method: EstimationMethod::Sampling,
                };
            }
        }

        // NDV-based fallback for equality.
        if matches!(op, Operator::Eq | Operator::Is) && self.ndv > 0 {
            let sel = 1.0 / self.ndv as f64;
            let span = tracing::debug_span!(
                target: "fsqlite.planner",
                "cost_estimate",
                table = tracing::field::Empty,
                estimated_rows = (sel * rows),
                actual_method = "ndv",
            );
            let _g = span.enter();
            return CardinalityEstimate {
                estimated_rows: sel * rows,
                selectivity: sel,
                method: EstimationMethod::Ndv,
            };
        }

        // Default heuristic.
        let sel = default_selectivity(*op);
        let span = tracing::debug_span!(
            target: "fsqlite.planner",
            "cost_estimate",
            table = tracing::field::Empty,
            estimated_rows = (sel * rows),
            actual_method = "heuristic",
        );
        let _g = span.enter();
        CardinalityEstimate {
            estimated_rows: sel * rows,
            selectivity: sel,
            method: EstimationMethod::Heuristic,
        }
    }
}

/// Default selectivity heuristic when no statistics are available.
fn default_selectivity(op: Operator) -> f64 {
    match op {
        Operator::Eq | Operator::Is => 0.01, // ~1/100 rows match
        Operator::Ne | Operator::IsNot => 0.99,
        Operator::Lt | Operator::Le | Operator::Gt | Operator::Ge => 1.0 / 3.0,
        Operator::Like | Operator::Glob => 0.1,
    }
}

/// Check if a sample value satisfies the comparison operator against the probe.
fn cmp_matches(sample_val: &SqliteValue, op: Operator, probe: &SqliteValue) -> bool {
    let ord = sample_val.partial_cmp(probe);
    match op {
        Operator::Eq | Operator::Is => ord == Some(Ordering::Equal),
        Operator::Ne | Operator::IsNot => ord != Some(Ordering::Equal),
        Operator::Lt => ord == Some(Ordering::Less),
        Operator::Le => matches!(ord, Some(Ordering::Less | Ordering::Equal)),
        Operator::Gt => ord == Some(Ordering::Greater),
        Operator::Ge => matches!(ord, Some(Ordering::Greater | Ordering::Equal)),
        Operator::Like | Operator::Glob => false, // Pattern matching not supported in samples
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_interpolation_integer() {
        let bucket = HistogramBucket {
            lower: SqliteValue::Integer(0),
            upper: SqliteValue::Integer(100),
            count: 100,
            ndv: 100,
        };
        let hist = Histogram {
            buckets: vec![bucket],
        };

        // Value 50 should be ~50% through the bucket
        let est = hist.estimate_less_than_rows(&SqliteValue::Integer(50));
        assert!((est - 50.0).abs() < 1.0);
    }

    #[test]
    fn test_histogram_bucket_contains_is_inclusive_on_both_ends() {
        let int_bucket = HistogramBucket {
            lower: SqliteValue::Integer(10),
            upper: SqliteValue::Integer(20),
            count: 5,
            ndv: 3,
        };
        assert!(
            int_bucket.contains(&SqliteValue::Integer(10)),
            "lower bound is inclusive"
        );
        assert!(
            int_bucket.contains(&SqliteValue::Integer(20)),
            "upper bound is inclusive"
        );
        assert!(int_bucket.contains(&SqliteValue::Integer(15)));
        assert!(!int_bucket.contains(&SqliteValue::Integer(9)));
        assert!(!int_bucket.contains(&SqliteValue::Integer(21)));

        // A single-value bucket [5, 5] contains only 5.
        let point = HistogramBucket {
            lower: SqliteValue::Integer(5),
            upper: SqliteValue::Integer(5),
            count: 1,
            ndv: 1,
        };
        assert!(point.contains(&SqliteValue::Integer(5)));
        assert!(!point.contains(&SqliteValue::Integer(4)));
        assert!(!point.contains(&SqliteValue::Integer(6)));

        // Float buckets behave the same: inclusive endpoints, exclusive outside.
        let float_bucket = HistogramBucket {
            lower: SqliteValue::Float(1.0),
            upper: SqliteValue::Float(2.0),
            count: 4,
            ndv: 4,
        };
        assert!(float_bucket.contains(&SqliteValue::Float(1.0)));
        assert!(float_bucket.contains(&SqliteValue::Float(2.0)));
        assert!(float_bucket.contains(&SqliteValue::Float(1.5)));
        assert!(!float_bucket.contains(&SqliteValue::Float(0.99)));
        assert!(!float_bucket.contains(&SqliteValue::Float(2.01)));
    }

    #[test]
    fn test_bytes_to_fraction_base256_encoding() {
        let bf = bytes_to_fraction;
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-12;

        // Empty -> 0; a single byte contributes value / 256.
        assert!(approx(bf(b""), 0.0));
        assert!(approx(bf(&[0x80]), 0.5));
        assert!(approx(bf(&[0x40]), 0.25));
        assert!(approx(bf(&[0xFF]), 255.0 / 256.0));

        // The second byte contributes value / 256^2.
        assert!(approx(bf(&[0x00, 0x80]), 128.0 / 65536.0)); // 2^-9 = 0.001953125
        assert!(approx(bf(&[0x80, 0x80]), 0.5 + 128.0 / 65536.0));

        // Only the first 8 bytes matter; trailing bytes are ignored.
        assert_eq!(bf(&[0xFF; 9]), bf(&[0xFF; 8]));

        // Strictly increasing with the most-significant byte; result stays in [0, 1).
        assert!(bf(&[0x02]) > bf(&[0x01]));
        assert!(bf(&[0x01]) > bf(&[0x00, 0xFF, 0xFF]));
        let max8 = bf(&[0xFF; 8]);
        assert!(
            (0.0..1.0).contains(&max8),
            "eight 0xFF bytes must stay below 1.0, got {max8}"
        );
        assert_eq!(
            max8,
            1.0 - f64::EPSILON / 2.0,
            "the maximum prefix maps to the largest binary64 below 1.0"
        );

        // The conversion rounds downward to a 53-bit dyadic grid. Prefixes
        // in one grid cell compare equal, while crossing the next 53-bit
        // boundary remains strictly ordered even at the upper endpoint.
        let same_top_bin = (u64::MAX - ((1_u64 << 11) - 1)).to_be_bytes();
        let previous_bin = (u64::MAX - (1_u64 << 11)).to_be_bytes();
        assert_eq!(bf(&same_top_bin), max8);
        assert!(bf(&previous_bin) < max8);
        assert_eq!(max8 - bf(&previous_bin), f64::EPSILON / 2.0);
    }

    #[test]
    fn test_interpolate_position_clamps_handles_degenerate_and_mixed_types() {
        use SqliteValue::{Float, Integer};
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;

        // Integer: linear position, clamped to [0,1] for values outside [min,max].
        assert!(approx(
            interpolate_position(&Integer(0), &Integer(100), &Integer(50)),
            0.5
        ));
        assert!(approx(
            interpolate_position(&Integer(0), &Integer(100), &Integer(0)),
            0.0
        ));
        assert!(approx(
            interpolate_position(&Integer(0), &Integer(100), &Integer(100)),
            1.0
        ));
        assert!(
            approx(
                interpolate_position(&Integer(0), &Integer(100), &Integer(-50)),
                0.0
            ),
            "below min clamps to 0"
        );
        assert!(
            approx(
                interpolate_position(&Integer(0), &Integer(100), &Integer(200)),
                1.0
            ),
            "above max clamps to 1"
        );
        // Degenerate range (max <= min) carries no information -> 0.5.
        assert!(approx(
            interpolate_position(&Integer(50), &Integer(50), &Integer(50)),
            0.5
        ));
        assert!(approx(
            interpolate_position(&Integer(100), &Integer(0), &Integer(50)),
            0.5
        ));

        // Float: same linear behavior; any NaN endpoint/value returns 0.5.
        assert!(approx(
            interpolate_position(&Float(0.0), &Float(10.0), &Float(2.5)),
            0.25
        ));
        assert!(
            approx(
                interpolate_position(&Float(0.0), &Float(10.0), &Float(f64::NAN)),
                0.5
            ),
            "NaN value -> 0.5"
        );
        assert!(
            approx(
                interpolate_position(&Float(f64::NAN), &Float(10.0), &Float(5.0)),
                0.5
            ),
            "NaN min -> 0.5"
        );

        // Mixed / uncomparable types fall through to the 0.5 default.
        assert!(approx(
            interpolate_position(&Integer(0), &Float(10.0), &Integer(5)),
            0.5
        ));
    }

    #[test]
    fn test_interpolate_position_text_and_blob_branches() {
        // The existing interpolate_position test covers Integer/Float/mixed; the
        // Text and Blob branches (which interpolate via the base-256 byte
        // fraction and have their own degenerate-range guards) were untested.
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;

        // Text: linear position via the base-256 fraction of the bytes. With
        // ASCII 'A'(65), 'B'(66), 'C'(67), B sits halfway between A and C.
        let t = |s: &str| SqliteValue::from(s);
        assert!(approx(interpolate_position(&t("A"), &t("C"), &t("B")), 0.5));
        assert!(approx(interpolate_position(&t("A"), &t("C"), &t("A")), 0.0));
        assert!(approx(interpolate_position(&t("A"), &t("C"), &t("C")), 1.0));
        // Out-of-range values clamp to [0, 1].
        assert!(approx(interpolate_position(&t("A"), &t("C"), &t("@")), 0.0)); // '@'=64 < min
        assert!(approx(interpolate_position(&t("A"), &t("C"), &t("D")), 1.0)); // 'D'=68 > max
        // A reversed or empty text range carries no information -> 0.5.
        assert!(approx(interpolate_position(&t("C"), &t("A"), &t("B")), 0.5));
        assert!(approx(interpolate_position(&t("B"), &t("B"), &t("B")), 0.5));

        // Blob: same base-256 interpolation, but the bytes can be arbitrary.
        let b = |bytes: &[u8]| SqliteValue::from(bytes);
        assert!(approx(
            interpolate_position(&b(&[0x00]), &b(&[0x80]), &b(&[0x40])),
            0.5
        ));
        // A reversed blob range -> 0.5.
        assert!(approx(
            interpolate_position(&b(&[0x80]), &b(&[0x00]), &b(&[0x40])),
            0.5
        ));
        // Distinct blobs (max > min lexically) whose 8-byte base-256 encodings
        // collide because trailing zero bytes are ignored hit the inner
        // zero-range guard -> 0.5.
        assert!(approx(
            interpolate_position(&b(&[0x01]), &b(&[0x01, 0x00]), &b(&[0x01])),
            0.5
        ));
    }

    #[test]
    fn test_histogram_equality_and_range_estimates_multi_bucket() {
        // Two equi-depth buckets with distinct densities so equality estimates
        // (count / ndv) differ per bucket and range estimates span both.
        let hist = Histogram {
            buckets: vec![
                HistogramBucket {
                    lower: SqliteValue::Integer(0),
                    upper: SqliteValue::Integer(99),
                    count: 100,
                    ndv: 10,
                },
                HistogramBucket {
                    lower: SqliteValue::Integer(100),
                    upper: SqliteValue::Integer(199),
                    count: 200,
                    ndv: 50,
                },
            ],
        };
        let total = 300.0;
        let iv = SqliteValue::Integer;

        // Equality: uniform within bucket = count / ndv (boundaries included).
        assert!((hist.estimate_equality_rows(&iv(50)) - 10.0).abs() < f64::EPSILON); // A: 100/10
        assert!((hist.estimate_equality_rows(&iv(0)) - 10.0).abs() < f64::EPSILON); // A lower
        assert!((hist.estimate_equality_rows(&iv(99)) - 10.0).abs() < f64::EPSILON); // A upper
        assert!((hist.estimate_equality_rows(&iv(100)) - 4.0).abs() < f64::EPSILON); // B: 200/50
        assert!((hist.estimate_equality_rows(&iv(150)) - 4.0).abs() < f64::EPSILON);
        // Out of histogram range -> minimal-selectivity fallback of 1.0 row.
        assert!((hist.estimate_equality_rows(&iv(10_000)) - 1.0).abs() < f64::EPSILON);

        // Range endpoints: below all rows -> 0; at/above all rows -> total.
        assert!((hist.estimate_less_than_rows(&iv(-100)) - 0.0).abs() < f64::EPSILON);
        assert!((hist.estimate_less_than_rows(&iv(10_000)) - total).abs() < f64::EPSILON);
        assert!((hist.estimate_greater_than_rows(&iv(10_000)) - 0.0).abs() < f64::EPSILON);
        assert!((hist.estimate_greater_than_rows(&iv(-100)) - total).abs() < f64::EPSILON);

        // less_than is monotonic non-decreasing across the domain.
        let lt_lo = hist.estimate_less_than_rows(&iv(-100));
        let lt_a = hist.estimate_less_than_rows(&iv(50));
        let lt_b = hist.estimate_less_than_rows(&iv(150));
        let lt_hi = hist.estimate_less_than_rows(&iv(10_000));
        assert!(
            lt_lo <= lt_a && lt_a <= lt_b && lt_b <= lt_hi,
            "less_than must be monotonic: {lt_lo} <= {lt_a} <= {lt_b} <= {lt_hi}"
        );

        // Every estimate stays within [0, total].
        for n in [-100_i64, 0, 50, 99, 100, 150, 199, 10_000] {
            let lt = hist.estimate_less_than_rows(&iv(n));
            let gt = hist.estimate_greater_than_rows(&iv(n));
            assert!(
                (0.0..=total).contains(&lt),
                "less_than({n})={lt} out of [0,{total}]"
            );
            assert!(
                (0.0..=total).contains(&gt),
                "greater_than({n})={gt} out of [0,{total}]"
            );
        }
    }

    #[test]
    fn test_selectivity_defaults() {
        let stats = ColumnStats {
            table_row_count: 1000,
            null_count: 0,
            ndv: 100,
            min_value: Some(SqliteValue::Integer(0)),
            max_value: Some(SqliteValue::Integer(1000)),
            avg_width: 8.0,
            histogram: None,
        };

        // Eq: 1/NDV = 1/100 = 0.01
        let sel = stats.estimate_selectivity(&Operator::Eq, &SqliteValue::Integer(50));
        assert!((sel - 0.01).abs() < 0.001);

        // Gt: 1/3 heuristic
        let sel = stats.estimate_selectivity(&Operator::Gt, &SqliteValue::Integer(50));
        assert!((sel - 0.333).abs() < 0.001);
    }

    #[test]
    fn test_estimate_selectivity_operator_dispatch_and_null_handling() {
        // test_selectivity_defaults only covers Eq + Gt; this covers the rest of
        // the no-histogram dispatch (Ne/Lt/Le/Ge), NULL handling, empty/all-NULL
        // edges, and the algebraic relationships between operators.
        let base = ColumnStats {
            table_row_count: 1000,
            null_count: 0,
            ndv: 100,
            min_value: Some(SqliteValue::Integer(0)),
            max_value: Some(SqliteValue::Integer(1000)),
            avg_width: 8.0,
            histogram: None,
        };
        let v = SqliteValue::Integer(50);
        let sel = |stats: &ColumnStats, op: Operator| stats.estimate_selectivity(&op, &v);

        let eq = sel(&base, Operator::Eq);
        let ne = sel(&base, Operator::Ne);
        let lt = sel(&base, Operator::Lt);
        let gt = sel(&base, Operator::Gt);
        let le = sel(&base, Operator::Le);
        let ge = sel(&base, Operator::Ge);
        assert!((eq - 0.01).abs() < 1e-9, "Eq = 1/ndv = 0.01, got {eq}");
        assert!((lt - gt).abs() < 1e-12, "Lt and Gt share the 1/3 default");
        assert!((lt - 1.0 / 3.0).abs() < 1e-9, "Lt = 1/3 default, got {lt}");
        // Eq and Ne partition the non-NULL space: with no NULLs they sum to 1.0.
        assert!(
            (eq + ne - 1.0).abs() < 1e-9,
            "Eq + Ne should be 1.0 with no NULLs, got {}",
            eq + ne
        );
        // Closed endpoints add the equality mass: Le = Lt + Eq, Ge = Gt + Eq.
        assert!(((le - lt) - eq).abs() < 1e-9, "Le - Lt should equal Eq");
        assert!(((ge - gt) - eq).abs() < 1e-9, "Ge - Gt should equal Eq");

        // NULLs shrink the non-NULL base: equality selectivity drops and Eq+Ne
        // sums to the non-NULL fraction rather than 1.0.
        let with_nulls = ColumnStats {
            null_count: 200,
            ..base.clone()
        };
        let eq_n = sel(&with_nulls, Operator::Eq);
        let ne_n = sel(&with_nulls, Operator::Ne);
        assert!(
            (eq_n - 0.008).abs() < 1e-9,
            "Eq with 200/1000 NULLs = (800/100)/1000 = 0.008, got {eq_n}"
        );
        assert!(
            (eq_n + ne_n - 0.8).abs() < 1e-9,
            "Eq+Ne should equal the non-NULL fraction 0.8, got {}",
            eq_n + ne_n
        );

        // Edge cases: empty table and an all-NULL column yield zero selectivity
        // for every operator. Every estimate stays a valid probability in [0,1].
        let empty = ColumnStats {
            table_row_count: 0,
            ..base.clone()
        };
        let all_null = ColumnStats {
            table_row_count: 500,
            null_count: 500,
            ..base.clone()
        };
        for op in [
            Operator::Eq,
            Operator::Ne,
            Operator::Lt,
            Operator::Le,
            Operator::Gt,
            Operator::Ge,
        ] {
            assert!(
                sel(&empty, op).abs() < f64::EPSILON,
                "empty table -> 0 selectivity"
            );
            assert!(
                sel(&all_null, op).abs() < f64::EPSILON,
                "all-NULL -> 0 selectivity"
            );
            for stats in [&base, &with_nulls] {
                let s = sel(stats, op);
                assert!((0.0..=1.0).contains(&s), "selectivity {s} out of [0,1]");
            }
        }
    }

    // ── Cardinality estimation with sampling fallback (bd-1as.1) ──

    #[test]
    fn test_cardinality_estimate_histogram_preferred() {
        let hist = Histogram {
            buckets: vec![HistogramBucket {
                lower: SqliteValue::Integer(0),
                upper: SqliteValue::Integer(100),
                count: 1000,
                ndv: 100,
            }],
        };
        let stats = ColumnStats {
            table_row_count: 1000,
            null_count: 0,
            ndv: 100,
            min_value: Some(SqliteValue::Integer(0)),
            max_value: Some(SqliteValue::Integer(100)),
            avg_width: 8.0,
            histogram: Some(hist),
        };

        let est = stats.estimate_cardinality(
            &Operator::Eq,
            &SqliteValue::Integer(50),
            Some(&[SqliteValue::Integer(50)]), // Sample should be ignored
        );
        assert_eq!(est.method, EstimationMethod::Histogram);
        assert!(est.estimated_rows > 0.0);
    }

    #[test]
    fn test_cardinality_estimate_sampling_fallback() {
        let stats = ColumnStats {
            table_row_count: 1000,
            null_count: 0,
            ndv: 0,
            min_value: None,
            max_value: None,
            avg_width: 0.0,
            histogram: None,
        };

        // Sample: 3 out of 10 match value 42
        let sample: Vec<SqliteValue> = (0..10)
            .map(|i| SqliteValue::Integer(if i < 3 { 42 } else { i + 100 }))
            .collect();

        let est =
            stats.estimate_cardinality(&Operator::Eq, &SqliteValue::Integer(42), Some(&sample));
        assert_eq!(est.method, EstimationMethod::Sampling);
        assert!((est.selectivity - 0.3).abs() < 0.01);
        assert!((est.estimated_rows - 300.0).abs() < 1.0);
    }

    #[test]
    fn test_cardinality_estimate_ndv_fallback() {
        let stats = ColumnStats {
            table_row_count: 1000,
            null_count: 0,
            ndv: 50,
            min_value: None,
            max_value: None,
            avg_width: 0.0,
            histogram: None,
        };

        let est = stats.estimate_cardinality(&Operator::Eq, &SqliteValue::Integer(42), None);
        assert_eq!(est.method, EstimationMethod::Ndv);
        assert!((est.selectivity - 0.02).abs() < 0.001);
        assert!((est.estimated_rows - 20.0).abs() < 0.1);
    }

    #[test]
    fn test_estimate_cardinality_empty_table_and_ndv_only_for_equality() {
        // Empty table -> zero estimate with heuristic provenance.
        let empty = ColumnStats {
            table_row_count: 0,
            ndv: 100,
            ..ColumnStats::default()
        };
        let est = empty.estimate_cardinality(&Operator::Eq, &SqliteValue::Integer(1), None);
        assert!(est.estimated_rows.abs() < f64::EPSILON);
        assert!(est.selectivity.abs() < f64::EPSILON);
        assert_eq!(est.method, EstimationMethod::Heuristic);

        // With NDV but no histogram/sample, the NDV fallback is wired ONLY for
        // equality. A range operator with the same stats falls through to the
        // default heuristic instead of using NDV.
        let stats = ColumnStats {
            table_row_count: 1000,
            ndv: 50,
            ..ColumnStats::default()
        };
        let eq = stats.estimate_cardinality(&Operator::Eq, &SqliteValue::Integer(1), None);
        assert_eq!(eq.method, EstimationMethod::Ndv, "equality uses NDV");
        let lt = stats.estimate_cardinality(&Operator::Lt, &SqliteValue::Integer(1), None);
        assert_eq!(
            lt.method,
            EstimationMethod::Heuristic,
            "a range op falls to the heuristic, not NDV"
        );

        // Across methods, estimated_rows is exactly selectivity * row_count.
        for est in [&eq, &lt] {
            assert!(
                est.selectivity.mul_add(-1000.0, est.estimated_rows).abs() < 1e-6,
                "estimated_rows must equal selectivity * row_count"
            );
        }
    }

    #[test]
    fn test_cardinality_estimate_heuristic_fallback() {
        let stats = ColumnStats {
            table_row_count: 1000,
            null_count: 0,
            ndv: 0,
            min_value: None,
            max_value: None,
            avg_width: 0.0,
            histogram: None,
        };

        let est = stats.estimate_cardinality(&Operator::Gt, &SqliteValue::Integer(42), None);
        assert_eq!(est.method, EstimationMethod::Heuristic);
        assert!((est.selectivity - 1.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn test_default_selectivity_values() {
        assert!((default_selectivity(Operator::Eq) - 0.01).abs() < 0.001);
        assert!((default_selectivity(Operator::Ne) - 0.99).abs() < 0.001);
        assert!((default_selectivity(Operator::Lt) - 0.333).abs() < 0.001);
        assert!((default_selectivity(Operator::Like) - 0.1).abs() < 0.001);

        // Complete the operator coverage: the aliases collapse to their primary
        // operator's selectivity, and all four range operators agree.
        let eps = 1e-9;
        assert!(
            (default_selectivity(Operator::Is) - default_selectivity(Operator::Eq)).abs() < eps
        );
        assert!(
            (default_selectivity(Operator::IsNot) - default_selectivity(Operator::Ne)).abs() < eps
        );
        assert!(
            (default_selectivity(Operator::Glob) - default_selectivity(Operator::Like)).abs() < eps
        );
        for op in [Operator::Le, Operator::Gt, Operator::Ge] {
            assert!((default_selectivity(op) - default_selectivity(Operator::Lt)).abs() < eps);
        }
    }

    #[test]
    fn test_cmp_matches() {
        let v50 = SqliteValue::Integer(50);
        let v100 = SqliteValue::Integer(100);

        assert!(cmp_matches(&v50, Operator::Eq, &v50));
        assert!(!cmp_matches(&v50, Operator::Eq, &v100));
        assert!(cmp_matches(&v50, Operator::Lt, &v100));
        assert!(!cmp_matches(&v100, Operator::Lt, &v50));
        assert!(cmp_matches(&v50, Operator::Le, &v50));
        assert!(cmp_matches(&v100, Operator::Gt, &v50));
        assert!(cmp_matches(&v100, Operator::Ge, &v100));
        assert!(cmp_matches(&v50, Operator::Ne, &v100));

        // IS / IS NOT mirror Eq / Ne on samples.
        assert!(cmp_matches(&v50, Operator::Is, &v50));
        assert!(!cmp_matches(&v50, Operator::Is, &v100));
        assert!(cmp_matches(&v50, Operator::IsNot, &v100));
        assert!(!cmp_matches(&v50, Operator::IsNot, &v50));

        // Le also matches strictly-less; Ge also matches strictly-greater.
        assert!(cmp_matches(&v50, Operator::Le, &v100));
        assert!(cmp_matches(&v100, Operator::Ge, &v50));

        // LIKE / GLOB are not evaluated against samples: they never match, even
        // when the two values are equal.
        assert!(!cmp_matches(&v50, Operator::Like, &v50));
        assert!(!cmp_matches(&v50, Operator::Glob, &v50));
    }

    #[test]
    fn test_estimation_method_hierarchy() {
        // Sampling should take precedence over NDV when no histogram
        let stats = ColumnStats {
            table_row_count: 1000,
            null_count: 0,
            ndv: 50,
            min_value: None,
            max_value: None,
            avg_width: 0.0,
            histogram: None,
        };

        let sample = vec![SqliteValue::Integer(42); 10];
        let est =
            stats.estimate_cardinality(&Operator::Eq, &SqliteValue::Integer(42), Some(&sample));
        // With sample, should prefer sampling over NDV
        assert_eq!(est.method, EstimationMethod::Sampling);
        assert!((est.selectivity - 1.0).abs() < 0.01);
    }

    // ── sqlite_stat1 parsing (PLANNER-1) ──

    #[test]
    fn parse_stat1_table_only_row() {
        // Table-only row (idx IS NULL in sqlite_stat1): just the row count.
        let parsed = parse_stat1("12345").unwrap();
        assert_eq!(parsed.n_rows, 12345);
        assert!(parsed.per_column_distinct.is_empty());
    }

    #[test]
    fn parse_stat1_index_row_with_distinct_counts() {
        // Typical index-row: "N k1 k2 ..." where N is row count and
        // k_i is rows-per-distinct-value for the i-th leading column.
        let parsed = parse_stat1("1000 10 1").unwrap();
        assert_eq!(parsed.n_rows, 1000);
        assert_eq!(parsed.per_column_distinct, vec![10, 1]);
    }

    #[test]
    fn parse_stat1_tolerates_extra_whitespace() {
        let parsed = parse_stat1("  500\t 20   5 ").unwrap();
        assert_eq!(parsed.n_rows, 500);
        assert_eq!(parsed.per_column_distinct, vec![20, 5]);
    }

    #[test]
    fn parse_stat1_rejects_empty_and_non_numeric() {
        assert!(parse_stat1("").is_none());
        assert!(parse_stat1("   ").is_none());
        assert!(parse_stat1("not-a-number 1 2").is_none());
    }

    #[test]
    fn parse_stat1_drops_trailing_garbage() {
        // Unparseable trailing tokens are advisory; drop them silently.
        let parsed = parse_stat1("42 7 foo 3").unwrap();
        assert_eq!(parsed.n_rows, 42);
        assert_eq!(parsed.per_column_distinct, vec![7, 3]);
    }
}
