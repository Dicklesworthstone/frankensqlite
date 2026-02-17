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
            if value > &self.upper {
                // Bucket is entirely below value
                count += bucket.count as f64;
            } else if value <= &self.lower {
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
            if value < &self.lower {
                // Bucket is entirely above value
                count += bucket.count as f64;
            } else if value >= &self.upper {
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

/// Heuristic linear interpolation of `val` between `min` and `max`.
/// Returns a value in [0.0, 1.0].
fn interpolate_position(min: &SqliteValue, max: &SqliteValue, val: &SqliteValue) -> f64 {
    // Only interpolate numeric types. For others, assume 0.5.
    match (min, max, val) {
        (SqliteValue::Integer(min_i), SqliteValue::Integer(max_i), SqliteValue::Integer(val_i)) => {
            if max_i <= min_i {
                return 0.5;
            }
            let range = (max_i - min_i) as f64;
            let offset = (val_i - min_i) as f64;
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
        // TODO: Could implement lexicographical interpolation for strings/blobs
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
        let hist = Histogram { buckets: vec![bucket] };

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
}
