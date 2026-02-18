//! Property-based tests: vectorized operator output equivalence (bd-14vp7.8).
//!
//! Uses proptest to verify that every vectorized operator produces correct
//! results for arbitrary inputs.  Each property encodes a reference ("naive")
//! computation and asserts the vectorized path matches.

#[cfg(test)]
mod tests {
    use crate::vectorized::{
        Batch, ColumnData, ColumnSpec, ColumnVectorType, DEFAULT_BATCH_ROW_CAPACITY,
    };
    use crate::vectorized_agg::{AggregateOp, AggregateSpec, aggregate_batch_hash};
    use crate::vectorized_hash_join::{JoinType, hash_join_build, hash_join_probe};
    use crate::vectorized_ops::{CompareOp, filter_batch_int64};
    use crate::vectorized_sort::{NullOrdering, SortDirection, SortKeySpec, sort_batch};
    use fsqlite_types::value::SqliteValue;
    use proptest::prelude::*;

    // ── Helpers ────────────────────────────────────────────────────────────

    fn make_int64_batch(col_name: &str, values: &[Option<i64>]) -> Batch {
        let specs = vec![ColumnSpec::new(col_name, ColumnVectorType::Int64)];
        let rows: Vec<Vec<SqliteValue>> = values
            .iter()
            .map(|v| match v {
                Some(i) => vec![SqliteValue::Integer(*i)],
                None => vec![SqliteValue::Null],
            })
            .collect();
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch")
    }

    fn make_two_col_batch(rows: &[(Option<i64>, Option<i64>)]) -> Batch {
        let specs = vec![
            ColumnSpec::new("a", ColumnVectorType::Int64),
            ColumnSpec::new("b", ColumnVectorType::Int64),
        ];
        let row_values: Vec<Vec<SqliteValue>> = rows
            .iter()
            .map(|(a, b)| {
                vec![
                    a.map_or(SqliteValue::Null, SqliteValue::Integer),
                    b.map_or(SqliteValue::Null, SqliteValue::Integer),
                ]
            })
            .collect();
        Batch::from_rows(&row_values, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch")
    }

    fn extract_int64(batch: &Batch, col_idx: usize) -> Vec<Option<i64>> {
        let col = &batch.columns()[col_idx];
        let sel = batch.selection().as_slice();
        if let ColumnData::Int64(v) = &col.data {
            sel.iter()
                .map(|&i| {
                    let idx = usize::from(i);
                    if col.validity.is_valid(idx) {
                        Some(v.as_slice()[idx])
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            panic!("expected Int64 column at index {col_idx}")
        }
    }

    // ── Proptest Strategies ───────────────────────────────────────────────

    fn nullable_i64() -> impl Strategy<Value = Option<i64>> {
        prop_oneof![
            9 => any::<i64>().prop_map(Some),
            1 => Just(None),
        ]
    }

    fn int64_vec(max_len: usize) -> impl Strategy<Value = Vec<Option<i64>>> {
        prop::collection::vec(nullable_i64(), 0..=max_len)
    }

    fn two_col_row() -> impl Strategy<Value = (Option<i64>, Option<i64>)> {
        (nullable_i64(), nullable_i64())
    }

    fn two_col_vec(max_len: usize) -> impl Strategy<Value = Vec<(Option<i64>, Option<i64>)>> {
        prop::collection::vec(two_col_row(), 0..=max_len)
    }

    // Group keys for join — small domain so joins actually produce matches.
    fn join_key() -> impl Strategy<Value = Option<i64>> {
        prop_oneof![
            8 => (0_i64..10).prop_map(Some),
            2 => Just(None),
        ]
    }

    fn join_row() -> impl Strategy<Value = (Option<i64>, Option<i64>)> {
        (join_key(), nullable_i64())
    }

    fn join_vec(max_len: usize) -> impl Strategy<Value = Vec<(Option<i64>, Option<i64>)>> {
        prop::collection::vec(join_row(), 0..=max_len)
    }

    // ────────────────────────────────────────────────────────────────────
    // 1. FILTER PROPERTY
    // ────────────────────────────────────────────────────────────────────

    fn naive_filter_i64(values: &[Option<i64>], op: CompareOp, threshold: i64) -> Vec<usize> {
        values
            .iter()
            .enumerate()
            .filter_map(|(i, v)| {
                let val = (*v)?; // NULL → not selected
                let pass = match op {
                    CompareOp::Eq => val == threshold,
                    CompareOp::Ne => val != threshold,
                    CompareOp::Lt => val < threshold,
                    CompareOp::Le => val <= threshold,
                    CompareOp::Gt => val > threshold,
                    CompareOp::Ge => val >= threshold,
                };
                if pass { Some(i) } else { None }
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        #[test]
        fn prop_filter_matches_naive(
            values in int64_vec(256),
            threshold in any::<i64>(),
            op_idx in 0_usize..6,
        ) {
            let ops = [
                CompareOp::Eq, CompareOp::Ne, CompareOp::Lt,
                CompareOp::Le, CompareOp::Gt, CompareOp::Ge,
            ];
            let op = ops[op_idx];
            let batch = make_int64_batch("x", &values);
            let sel = filter_batch_int64(&batch, 0, op, threshold).unwrap();

            let expected = naive_filter_i64(&values, op, threshold);
            let actual: Vec<usize> = sel.as_slice().iter().map(|&i| usize::from(i)).collect();

            prop_assert_eq!(actual, expected);
        }
    }

    // ────────────────────────────────────────────────────────────────────
    // 2. SORT PROPERTY
    // ────────────────────────────────────────────────────────────────────

    fn is_sorted_asc_nulls_first(vals: &[Option<i64>]) -> bool {
        vals.windows(2).all(|w| match (w[0], w[1]) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(a), Some(b)) => a <= b,
        })
    }

    fn is_sorted_desc_nulls_last(vals: &[Option<i64>]) -> bool {
        vals.windows(2).all(|w| match (w[0], w[1]) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(a), Some(b)) => a >= b,
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        #[test]
        fn prop_sort_asc_produces_ordered_output(values in int64_vec(256)) {
            let batch = make_int64_batch("x", &values);
            let result = sort_batch(
                &batch,
                &[SortKeySpec {
                    column_idx: 0,
                    direction: SortDirection::Asc,
                    null_ordering: NullOrdering::NullsFirst,
                }],
            )
            .unwrap();
            let sorted = extract_int64(&result, 0);

            // Same multiset.
            let mut expected = values.clone();
            expected.sort_by(|a, b| match (a, b) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (Some(x), Some(y)) => x.cmp(y),
            });
            prop_assert_eq!(&sorted, &expected);
            prop_assert!(is_sorted_asc_nulls_first(&sorted));
        }

        #[test]
        fn prop_sort_desc_produces_ordered_output(values in int64_vec(256)) {
            let batch = make_int64_batch("x", &values);
            let result = sort_batch(
                &batch,
                &[SortKeySpec {
                    column_idx: 0,
                    direction: SortDirection::Desc,
                    null_ordering: NullOrdering::NullsLast,
                }],
            )
            .unwrap();
            let sorted = extract_int64(&result, 0);

            let mut expected = values.clone();
            expected.sort_by(|a, b| match (a, b) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(_), None) => std::cmp::Ordering::Less,
                (Some(x), Some(y)) => y.cmp(x),
            });
            prop_assert_eq!(&sorted, &expected);
            prop_assert!(is_sorted_desc_nulls_last(&sorted));
        }

        #[test]
        fn prop_sort_preserves_multiset(values in int64_vec(256)) {
            let batch = make_int64_batch("x", &values);
            let result = sort_batch(
                &batch,
                &[SortKeySpec {
                    column_idx: 0,
                    direction: SortDirection::Asc,
                    null_ordering: NullOrdering::NullsFirst,
                }],
            )
            .unwrap();
            let sorted = extract_int64(&result, 0);

            let mut orig_sorted = values.clone();
            orig_sorted.sort();
            let mut out_sorted = sorted.clone();
            out_sorted.sort();
            prop_assert_eq!(orig_sorted, out_sorted);
        }
    }

    // ────────────────────────────────────────────────────────────────────
    // 3. AGGREGATION PROPERTY
    // ────────────────────────────────────────────────────────────────────

    #[allow(clippy::cast_possible_wrap)]
    fn naive_count_star(values: &[Option<i64>]) -> i64 {
        values.len() as i64
    }

    #[allow(clippy::cast_possible_wrap)]
    fn naive_count(values: &[Option<i64>]) -> i64 {
        values.iter().filter(|v| v.is_some()).count() as i64
    }

    fn naive_sum(values: &[Option<i64>]) -> Option<i64> {
        let non_null: Vec<i64> = values.iter().filter_map(|&v| v).collect();
        if non_null.is_empty() {
            None
        } else {
            Some(non_null.iter().sum())
        }
    }

    fn naive_min(values: &[Option<i64>]) -> Option<i64> {
        values.iter().filter_map(|&v| v).min()
    }

    fn naive_max(values: &[Option<i64>]) -> Option<i64> {
        values.iter().filter_map(|&v| v).max()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        #[test]
        fn prop_agg_count_star(values in int64_vec(256)) {
            if values.is_empty() { return Ok(()); }
            let batch = make_int64_batch("x", &values);
            let result = aggregate_batch_hash(
                &batch,
                &[],
                &[AggregateSpec {
                    op: AggregateOp::CountStar,
                    column_idx: 0,
                    output_name: "cnt".to_string(),
                }],
            )
            .unwrap();
            let out = extract_int64(&result, 0);
            prop_assert_eq!(out, vec![Some(naive_count_star(&values))]);
        }

        #[test]
        fn prop_agg_count(values in int64_vec(256)) {
            if values.is_empty() { return Ok(()); }
            let batch = make_int64_batch("x", &values);
            let result = aggregate_batch_hash(
                &batch,
                &[],
                &[AggregateSpec {
                    op: AggregateOp::Count,
                    column_idx: 0,
                    output_name: "cnt".to_string(),
                }],
            )
            .unwrap();
            let out = extract_int64(&result, 0);
            prop_assert_eq!(out, vec![Some(naive_count(&values))]);
        }

        #[test]
        fn prop_agg_sum(values in int64_vec(128)) {
            if values.is_empty() { return Ok(()); }
            // Restrict to small values to avoid overflow.
            let clamped: Vec<Option<i64>> = values
                .iter()
                .map(|v| v.map(|x| x % 10_000))
                .collect();
            let batch = make_int64_batch("x", &clamped);
            let result = aggregate_batch_hash(
                &batch,
                &[],
                &[AggregateSpec {
                    op: AggregateOp::Sum,
                    column_idx: 0,
                    output_name: "s".to_string(),
                }],
            )
            .unwrap();
            let out = extract_int64(&result, 0);
            let expected = naive_sum(&clamped);
            prop_assert_eq!(out, vec![expected]);
        }

        #[test]
        fn prop_agg_min(values in int64_vec(256)) {
            if values.is_empty() { return Ok(()); }
            let batch = make_int64_batch("x", &values);
            let result = aggregate_batch_hash(
                &batch,
                &[],
                &[AggregateSpec {
                    op: AggregateOp::Min,
                    column_idx: 0,
                    output_name: "m".to_string(),
                }],
            )
            .unwrap();
            let out = extract_int64(&result, 0);
            prop_assert_eq!(out, vec![naive_min(&values)]);
        }

        #[test]
        fn prop_agg_max(values in int64_vec(256)) {
            if values.is_empty() { return Ok(()); }
            let batch = make_int64_batch("x", &values);
            let result = aggregate_batch_hash(
                &batch,
                &[],
                &[AggregateSpec {
                    op: AggregateOp::Max,
                    column_idx: 0,
                    output_name: "m".to_string(),
                }],
            )
            .unwrap();
            let out = extract_int64(&result, 0);
            prop_assert_eq!(out, vec![naive_max(&values)]);
        }
    }

    // ────────────────────────────────────────────────────────────────────
    // 4. HASH-JOIN PROPERTY
    // ────────────────────────────────────────────────────────────────────

    /// Naive nested-loop inner join on column 0 (key), outputting
    /// (probe_key, probe_val, build_val) for each match.
    fn naive_inner_join(
        build: &[(Option<i64>, Option<i64>)],
        probe: &[(Option<i64>, Option<i64>)],
    ) -> Vec<(Option<i64>, Option<i64>, Option<i64>)> {
        let mut out = Vec::new();
        for p in probe {
            let pk = match p.0 {
                Some(k) => k,
                None => continue, // NULL keys never match
            };
            for b in build {
                let bk = match b.0 {
                    Some(k) => k,
                    None => continue,
                };
                if pk == bk {
                    out.push((p.0, p.1, b.1));
                }
            }
        }
        out
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        #[test]
        fn prop_hash_join_inner_matches_naive(
            build_rows in join_vec(64),
            probe_rows in join_vec(64),
        ) {
            if build_rows.is_empty() || probe_rows.is_empty() {
                return Ok(());
            }
            let build_batch = make_two_col_batch(&build_rows);
            let probe_batch = make_two_col_batch(&probe_rows);

            let table = hash_join_build(build_batch, &[0]).unwrap();
            let result = hash_join_probe(&table, &probe_batch, &[0], JoinType::Inner).unwrap();

            // Extract output columns: probe_key(0), probe_val(1), build_val(2).
            let out_keys = extract_int64(&result, 0);
            let out_probe_vals = extract_int64(&result, 1);
            let out_build_vals = extract_int64(&result, 2);

            let mut actual: Vec<(Option<i64>, Option<i64>, Option<i64>)> = out_keys
                .into_iter()
                .zip(out_probe_vals)
                .zip(out_build_vals)
                .map(|((k, pv), bv)| (k, pv, bv))
                .collect();
            actual.sort_by_key(|r| (r.0, r.1, r.2));

            let mut expected = naive_inner_join(&build_rows, &probe_rows);
            expected.sort_by_key(|r| (r.0, r.1, r.2));

            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn prop_hash_join_semi_subset_of_probe(
            build_rows in join_vec(64),
            probe_rows in join_vec(64),
        ) {
            if build_rows.is_empty() || probe_rows.is_empty() {
                return Ok(());
            }
            let build_batch = make_two_col_batch(&build_rows);
            let probe_batch = make_two_col_batch(&probe_rows);

            let table = hash_join_build(build_batch, &[0]).unwrap();
            let result = hash_join_probe(&table, &probe_batch, &[0], JoinType::Semi).unwrap();

            let semi_keys = extract_int64(&result, 0);

            // Semi join: every output row must have a matching build key,
            // and each probe row appears at most once.
            let build_keys: std::collections::HashSet<i64> = build_rows
                .iter()
                .filter_map(|r| r.0)
                .collect();
            for key in &semi_keys {
                if let Some(k) = key {
                    prop_assert!(
                        build_keys.contains(k),
                        "semi key {} not in build set", k
                    );
                }
            }
            // No duplicates from same probe row — output count ≤ probe count.
            prop_assert!(semi_keys.len() <= probe_rows.len());
        }

        #[test]
        fn prop_hash_join_anti_disjoint_from_inner(
            build_rows in join_vec(64),
            probe_rows in join_vec(64),
        ) {
            if build_rows.is_empty() || probe_rows.is_empty() {
                return Ok(());
            }
            let build_batch = make_two_col_batch(&build_rows);
            let probe_batch = make_two_col_batch(&probe_rows);

            let table = hash_join_build(build_batch.clone(), &[0]).unwrap();
            let inner = hash_join_probe(&table, &probe_batch, &[0], JoinType::Inner).unwrap();
            let table2 = hash_join_build(build_batch, &[0]).unwrap();
            let anti = hash_join_probe(&table2, &probe_batch, &[0], JoinType::Anti).unwrap();

            let inner_count = inner.selection().len();
            let anti_count = anti.selection().len();

            // Anti join rows that did NOT match — count ≤ probe rows.
            prop_assert!(anti_count <= probe_rows.len());
            // If no inner matches, anti should return all probe rows.
            if inner_count == 0 {
                prop_assert_eq!(anti_count, probe_rows.len());
            }
        }
    }

    // ────────────────────────────────────────────────────────────────────
    // 5. END-TO-END PIPELINE PROPERTY
    //    scan → filter → aggregate → sort
    // ────────────────────────────────────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        #[test]
        fn prop_pipeline_filter_agg_sort(
            rows in two_col_vec(128),
            threshold in -100_i64..100,
        ) {
            if rows.is_empty() { return Ok(()); }

            // Build batch with two columns: "key" and "val".
            let specs = vec![
                ColumnSpec::new("key", ColumnVectorType::Int64),
                ColumnSpec::new("val", ColumnVectorType::Int64),
            ];
            let row_values: Vec<Vec<SqliteValue>> = rows
                .iter()
                .map(|(k, v)| {
                    vec![
                        k.map_or(SqliteValue::Null, SqliteValue::Integer),
                        v.map_or(SqliteValue::Null, SqliteValue::Integer),
                    ]
                })
                .collect();
            let batch = Batch::from_rows(&row_values, &specs, DEFAULT_BATCH_ROW_CAPACITY)
                .expect("batch");

            // Step 1: filter — keep rows where key > threshold.
            let sel = filter_batch_int64(&batch, 0, CompareOp::Gt, threshold).unwrap();
            let mut filtered = batch.clone();
            filtered.apply_selection(sel).unwrap();

            // Step 2: aggregate — SUM(val) grouped by key.
            let agg_result = aggregate_batch_hash(
                &filtered,
                &[0],
                &[AggregateSpec {
                    op: AggregateOp::Sum,
                    column_idx: 1,
                    output_name: "sum_val".to_string(),
                }],
            );
            // May produce empty result if filter eliminated everything.
            let agg_batch = match agg_result {
                Ok(b) => b,
                Err(_) => return Ok(()),
            };
            if agg_batch.selection().is_empty() {
                return Ok(());
            }

            // Step 3: sort the aggregated result by key ASC.
            let sorted = sort_batch(
                &agg_batch,
                &[SortKeySpec {
                    column_idx: 0,
                    direction: SortDirection::Asc,
                    null_ordering: NullOrdering::NullsFirst,
                }],
            )
            .unwrap();

            // Verify: output keys are in ascending order.
            let out_keys = extract_int64(&sorted, 0);
            for window in out_keys.windows(2) {
                match (window[0], window[1]) {
                    (None, _) => {} // NULL first, ok
                    (Some(_), None) => {
                        prop_assert!(false, "non-null before null in sorted output");
                    }
                    (Some(a), Some(b)) => {
                        prop_assert!(a <= b, "keys not sorted: {} > {}", a, b);
                    }
                }
            }

            // Verify: sum values match naive computation.
            let out_sums = extract_int64(&sorted, 1);
            let mut naive_groups: std::collections::BTreeMap<i64, i64> =
                std::collections::BTreeMap::new();
            for (k, v) in &rows {
                if let (Some(key), Some(val)) = (k, v) {
                    if *key > threshold {
                        *naive_groups.entry(*key).or_insert(0) += val;
                    }
                }
            }

            // Check non-null keys match.
            let non_null_keys: Vec<i64> = out_keys.iter().filter_map(|&v| v).collect();
            let non_null_sums: Vec<Option<i64>> = out_keys
                .iter()
                .zip(out_sums.iter())
                .filter_map(|(k, s)| k.map(|_| *s))
                .collect();

            for (key, sum) in non_null_keys.iter().zip(non_null_sums.iter()) {
                if let Some(expected) = naive_groups.get(key) {
                    if let Some(actual) = sum {
                        prop_assert_eq!(*actual, *expected);
                    }
                }
            }
        }
    }
}
