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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
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
                count += bucket.count as f64 * fraction;
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
                count += bucket.count as f64 * fraction;
                break;
            }
        }
        count
    }
}

fn bytes_to_fraction(bytes: &[u8]) -> f64 {
    let mut fraction = 0.0;
    let mut weight = 1.0 / 256.0;
    for &b in bytes.iter().take(8) {
        fraction += f64::from(b) * weight;
        weight /= 256.0;
    }
    fraction
}

/// Heuristic linear interpolation of `val` between `min` and `max`.
/// Returns a value in [0.0, 1.0].
fn interpolate_position(min: &SqliteValue, max: &SqliteValue, val: &SqliteValue) -> f64 {
    match (min, max, val) {
        (SqliteValue::Integer(min_i), SqliteValue::Integer(max_i), SqliteValue::Integer(val_i)) => {
            if max_i <= min_i {
                return 0.5;
            }
            let range = (*max_i - *min_i) as f64;
            let offset = (*val_i - *min_i) as f64;
            (offset / range).clamp(0.0, 1.0)
        }
        (SqliteValue::Float(min_f), SqliteValue::Float(max_f), SqliteValue::Float(val_f)) => {
            if max_f <= min_f {
                return 0.5;
            }
            let range = max_f - min_f;
            let offset = val_f - min_f;
            (offset / range).clamp(0.0, 1.0)
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
                let eq_sel = self.estimate_selectivity(&Operator::Eq, value);
                non_null_count * (1.0 - eq_sel)
            }
            _ => non_null_count * 0.1, // Fallback heuristic
        };

        (estimated_matches / self.table_row_count as f64).clamp(0.0, 1.0)
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
}
