-- Correctness-only replay; expected result follows each query.
.mode list
.headers off
-- cte_count; expected 1000000
WITH t AS (SELECT value FROM generate_series(1,1000000)) SELECT count(*) FROM t;
-- single_count; expected 1000000
SELECT count(*) FROM generate_series(1,1000000);
-- cte_correlated_exists; expected 10000
WITH t AS (SELECT value FROM generate_series(1,10000)) SELECT count(*) FROM t WHERE EXISTS(SELECT 1 FROM t t1 WHERE t.value=t1.value);
-- direct_correlated_exists; expected 10000
SELECT count(*) FROM generate_series(1,10000)t WHERE EXISTS(SELECT 1 FROM generate_series(1,10000)t1 WHERE t.value=t1.value);
-- cte_count_materialized; expected 1000000
WITH t AS MATERIALIZED (SELECT value FROM generate_series(1,1000000)) SELECT count(*) FROM t;
-- cte_count_not_materialized; expected 1000000
WITH t AS NOT MATERIALIZED (SELECT value FROM generate_series(1,1000000)) SELECT count(*) FROM t;
-- cte_body_filter_count; expected 500000
WITH t AS (SELECT value FROM generate_series(1,1000000) WHERE value<=500000) SELECT count(*) FROM t;
-- cte_outer_filter_count; expected 500000
WITH t AS (SELECT value FROM generate_series(1,1000000)) SELECT count(*) FROM t WHERE value<=500000;
-- cte_projected_sum; expected 1000002000000
WITH t AS (SELECT value*2+1 AS n FROM generate_series(1,1000000)) SELECT sum(n) FROM t;
-- cte_named_column_sum; expected 500000500000
WITH t(n) AS (SELECT value FROM generate_series(1,1000000)) SELECT sum(n) FROM t;
-- cte_hidden_column_sum; expected 1500001
WITH t AS (SELECT value,start,stop,step FROM generate_series(1,1000000,2)) SELECT sum(start+step)+max(stop)-max(value) FROM t;
-- cte_hidden_constraint_miss; expected 0
WITH t AS (SELECT value,step FROM generate_series(1,1000000,2)) SELECT count(*) FROM t WHERE step=3;
-- cte_limited_sum; expected 6050
WITH t AS (SELECT value FROM generate_series(1,1000000) LIMIT 100 OFFSET 10) SELECT sum(value) FROM t;
-- cte_ordered_limit_sum; expected 2999997
WITH t AS (SELECT value FROM generate_series(1,1000000) ORDER BY value DESC LIMIT 3) SELECT sum(value) FROM t;
-- cte_empty_count; expected 0
WITH t AS (SELECT value FROM generate_series(2,1)) SELECT count(*) FROM t;
-- cte_multiple_reference_sum; expected 5000150000
WITH t AS (SELECT value FROM generate_series(1,100000)) SELECT (SELECT sum(value) FROM t)+(SELECT count(*) FROM t);
-- cte_correlated_exists_materialized; expected 1000
WITH t AS MATERIALIZED (SELECT value FROM generate_series(1,1000)) SELECT count(*) FROM t WHERE EXISTS(SELECT 1 FROM t t1 WHERE t.value=t1.value);
-- cte_correlated_exists_not_materialized; expected 1000
WITH t AS NOT MATERIALIZED (SELECT value FROM generate_series(1,1000)) SELECT count(*) FROM t WHERE EXISTS(SELECT 1 FROM t t1 WHERE t.value=t1.value);
-- cte_correlated_miss; expected 0
WITH t AS (SELECT value FROM generate_series(1,1000)) SELECT count(*) FROM t WHERE EXISTS(SELECT 1 FROM t t1 WHERE t.value=t1.value+1000);
-- direct_correlated_miss; expected 0
SELECT count(*) FROM generate_series(1,1000)t WHERE EXISTS(SELECT 1 FROM generate_series(1,1000)t1 WHERE t.value=t1.value+1000);
-- cte_correlated_offset; expected 999
WITH t AS (SELECT value FROM generate_series(1,1000)) SELECT count(*) FROM t WHERE EXISTS(SELECT 1 FROM t t1 WHERE t.value=t1.value+1);
