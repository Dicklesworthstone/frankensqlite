use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[derive(Clone, Copy)]
struct QueryCase {
    name: &'static str,
    sql: &'static str,
}

#[derive(Clone, Copy)]
struct StatementCase {
    name: &'static str,
    sql: &'static str,
}

struct CoreSqlConformanceHarness {
    franken: Connection,
    sqlite: rusqlite::Connection,
}

impl CoreSqlConformanceHarness {
    fn new(setup_sql: &str) -> Self {
        let franken = Connection::open(":memory:").expect("open FrankenSQLite in-memory database");
        let sqlite =
            rusqlite::Connection::open_in_memory().expect("open rusqlite in-memory database");
        franken
            .execute_batch(setup_sql)
            .expect("execute FrankenSQLite setup SQL");
        sqlite
            .execute_batch(setup_sql)
            .expect("execute rusqlite setup SQL");
        Self { franken, sqlite }
    }

    fn assert_queries_match(&self, family: &str, cases: &[QueryCase]) {
        for case in cases {
            assert_eq!(
                self.franken_query_rows(case.sql),
                self.sqlite_query_rows(case.sql),
                "{family} conformance case failed: {} ({})",
                case.name,
                case.sql
            );
        }
    }

    fn assert_statement_errors_match(&self, family: &str, cases: &[StatementCase]) {
        for case in cases {
            let franken_error = self.franken.execute_batch(case.sql).is_err();
            let sqlite_error = self.sqlite.execute_batch(case.sql).is_err();
            assert!(
                sqlite_error,
                "{family} conformance case expected rusqlite error: {} ({})",
                case.name, case.sql
            );
            assert_eq!(
                franken_error, sqlite_error,
                "{family} conformance case failed: {} ({})",
                case.name, case.sql
            );
        }
    }

    fn execute_script(&self, sql: &str) {
        self.franken
            .execute_batch(sql)
            .expect("execute FrankenSQLite SQL script");
        self.sqlite
            .execute_batch(sql)
            .expect("execute rusqlite SQL script");
    }

    fn franken_query_rows(&self, sql: &str) -> Vec<Vec<String>> {
        self.franken
            .query(sql)
            .unwrap_or_else(|error| panic!("query FrankenSQLite `{sql}`: {error}"))
            .iter()
            .map(|row| row.values().iter().map(format_franken_value).collect())
            .collect()
    }

    fn sqlite_query_rows(&self, sql: &str) -> Vec<Vec<String>> {
        let mut stmt = self.sqlite.prepare(sql).expect("prepare rusqlite query");
        let column_count = stmt.column_count();
        stmt.query_map([], |row| {
            let mut values = Vec::with_capacity(column_count);
            for index in 0..column_count {
                let value: rusqlite::types::Value = row.get(index)?;
                values.push(format_rusqlite_value(&value));
            }
            Ok(values)
        })
        .expect("query rusqlite rows")
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("read rusqlite rows")
    }
}

fn format_franken_value(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(number) => number.to_string(),
        SqliteValue::Float(number) => format!("{number}"),
        SqliteValue::Text(text) => format!("'{text}'"),
        SqliteValue::Blob(bytes) => format!(
            "X'{}'",
            bytes
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        ),
    }
}

fn format_rusqlite_value(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(number) => number.to_string(),
        rusqlite::types::Value::Real(number) => format!("{number}"),
        rusqlite::types::Value::Text(text) => format!("'{text}'"),
        rusqlite::types::Value::Blob(bytes) => format!(
            "X'{}'",
            bytes
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        ),
    }
}

const SALES_SETUP: &str = "
    CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
    CREATE TABLE stores (id INTEGER PRIMARY KEY, region_id INTEGER, name TEXT NOT NULL);
    CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, base_price REAL NOT NULL);
    CREATE TABLE sales (
        id INTEGER PRIMARY KEY,
        store_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        qty INTEGER NOT NULL,
        sale_date TEXT NOT NULL
    );
    CREATE TABLE returns (id INTEGER PRIMARY KEY, sale_id INTEGER NOT NULL, qty INTEGER NOT NULL);

    INSERT INTO regions VALUES (1, 'North'), (2, 'South'), (3, 'East');
    INSERT INTO stores VALUES
        (10, 1, 'Store-A'),
        (20, 1, 'Store-B'),
        (30, 2, 'Store-C'),
        (40, 3, 'Store-D');
    INSERT INTO products VALUES
        (100, 'Widget', 9.99),
        (200, 'Gadget', 24.99),
        (300, 'Bolt', 1.50);
    INSERT INTO sales VALUES
        (1, 10, 100, 5, '2025-01-15'),
        (2, 10, 200, 2, '2025-01-16'),
        (3, 20, 100, 10, '2025-02-01'),
        (4, 30, 300, 100, '2025-02-10'),
        (5, 40, 200, 3, '2025-03-01'),
        (6, 30, 100, 7, '2025-03-05'),
        (7, 10, 300, 50, '2025-03-10');
    INSERT INTO returns VALUES (1, 1, 1), (2, 4, 5), (3, 6, 2);
";

const SELECT_JOIN_GROUP_AGGREGATE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "four table inner join",
        sql: "SELECT r.name, st.name, p.name, s.qty FROM regions r JOIN stores st ON st.region_id = r.id JOIN sales s ON s.store_id = st.id JOIN products p ON p.id = s.product_id ORDER BY r.name, st.name, s.id",
    },
    QueryCase {
        name: "left join with null fill",
        sql: "SELECT s.id, p.name, s.qty, COALESCE(ret.qty, 0) FROM sales s JOIN products p ON p.id = s.product_id LEFT JOIN returns ret ON ret.sale_id = s.id ORDER BY s.id",
    },
    QueryCase {
        name: "group by aggregate by region",
        sql: "SELECT r.name, COUNT(*) AS sale_count, SUM(s.qty) AS total_qty, MIN(s.qty), MAX(s.qty) FROM regions r JOIN stores st ON st.region_id = r.id JOIN sales s ON s.store_id = st.id GROUP BY r.name ORDER BY r.name",
    },
    QueryCase {
        name: "having aggregate expression",
        sql: "SELECT st.name, SUM(s.qty * p.base_price) AS revenue FROM stores st JOIN sales s ON s.store_id = st.id JOIN products p ON p.id = s.product_id GROUP BY st.name HAVING revenue > 50 ORDER BY revenue DESC, st.name",
    },
    QueryCase {
        name: "aggregate over expression group",
        sql: "SELECT substr(s.sale_date, 1, 7) AS month, COUNT(*), SUM(s.qty), AVG(s.qty) FROM sales s GROUP BY substr(s.sale_date, 1, 7) ORDER BY month",
    },
];

const LEFT_JOIN_PREDICATE_EDGE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "on predicate preserves null extended left rows",
        sql: "SELECT s.id, COALESCE(ret.qty, 0) FROM sales s LEFT JOIN returns ret ON ret.sale_id = s.id AND ret.qty >= 2 ORDER BY s.id",
    },
    QueryCase {
        name: "where predicate filters null extended rows",
        sql: "SELECT s.id, ret.qty FROM sales s LEFT JOIN returns ret ON ret.sale_id = s.id WHERE ret.qty >= 2 ORDER BY s.id",
    },
    QueryCase {
        name: "left join is null anti join",
        sql: "SELECT s.id FROM sales s LEFT JOIN returns ret ON ret.sale_id = s.id WHERE ret.id IS NULL ORDER BY s.id",
    },
    QueryCase {
        name: "chained left join aggregate null extension",
        sql: "SELECT st.name, COUNT(s.id), COUNT(ret.id) FROM stores st LEFT JOIN sales s ON s.store_id = st.id LEFT JOIN returns ret ON ret.sale_id = s.id GROUP BY st.name ORDER BY st.name",
    },
    QueryCase {
        name: "mixed inner and left join ordering",
        sql: "SELECT r.name, st.name, s.id, ret.qty FROM regions r JOIN stores st ON st.region_id = r.id JOIN sales s ON s.store_id = st.id LEFT JOIN returns ret ON ret.sale_id = s.id ORDER BY r.name, st.name, s.id",
    },
];

const JOIN_ALIAS_SELF_EDGE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "self join same store pairs",
        sql: "SELECT first.id, second.id FROM sales AS first JOIN sales AS second ON first.store_id = second.store_id AND first.id < second.id ORDER BY first.id, second.id",
    },
    QueryCase {
        name: "left self join finds last sale per store",
        sql: "SELECT s.id, s.store_id FROM sales AS s LEFT JOIN sales AS later ON later.store_id = s.store_id AND later.id > s.id WHERE later.id IS NULL ORDER BY s.store_id, s.id",
    },
    QueryCase {
        name: "alias qualified join predicate",
        sql: "SELECT st.name, r.name FROM stores AS st JOIN regions AS r ON r.id = st.region_id WHERE st.id IN (10, 30) ORDER BY st.name",
    },
    QueryCase {
        name: "computed predicate inside join on",
        sql: "SELECT s.id, p.name FROM sales AS s JOIN products AS p ON p.id = s.product_id AND s.qty * p.base_price >= 50 ORDER BY s.id",
    },
    QueryCase {
        name: "filtered join product with constant on",
        sql: "SELECT r.name, st.name FROM regions AS r JOIN stores AS st ON 1 = 1 WHERE st.region_id = r.id ORDER BY r.name, st.name",
    },
];

const JOIN_USING_NATURAL_SETUP: &str = "
    CREATE TABLE employees (
        id INTEGER PRIMARY KEY,
        dept_id INTEGER,
        employee TEXT NOT NULL,
        grade INTEGER NOT NULL
    );
    CREATE TABLE departments (
        dept_id INTEGER PRIMARY KEY,
        dept_name TEXT NOT NULL,
        region TEXT NOT NULL
    );

    INSERT INTO employees VALUES
        (1, 10, 'Ada', 5),
        (2, 10, 'Bert', 3),
        (3, 20, 'Cara', 4),
        (4, 99, 'Drew', 2),
        (5, NULL, 'Eli', 1);
    INSERT INTO departments VALUES
        (10, 'Engineering', 'North'),
        (20, 'Support', 'South'),
        (30, 'Sales', 'East');
";

const JOIN_USING_NATURAL_CASES: &[QueryCase] = &[
    QueryCase {
        name: "inner join using shared column",
        sql: "SELECT employee, dept_name, region FROM employees JOIN departments USING (dept_id) ORDER BY employee",
    },
    QueryCase {
        name: "left join using preserves unmatched rows",
        sql: "SELECT employee, dept_name, region FROM employees LEFT JOIN departments USING (dept_id) ORDER BY employee",
    },
    QueryCase {
        name: "natural join uses shared dept id",
        sql: "SELECT employee, dept_name, region FROM employees NATURAL JOIN departments ORDER BY employee",
    },
    QueryCase {
        name: "natural left join preserves unmatched rows",
        sql: "SELECT employee, dept_name, region FROM employees NATURAL LEFT JOIN departments ORDER BY employee",
    },
    QueryCase {
        name: "using column is projected once in star expansion",
        sql: "SELECT * FROM employees JOIN departments USING (dept_id) ORDER BY id LIMIT 2",
    },
    QueryCase {
        name: "unqualified using column after aliases",
        sql: "SELECT e.employee, d.dept_name FROM employees AS e JOIN departments AS d USING (dept_id) WHERE dept_id = 10 ORDER BY e.employee",
    },
    QueryCase {
        name: "group by using column after join",
        sql: "SELECT dept_id, COUNT(*), MIN(employee), MAX(employee) FROM employees JOIN departments USING (dept_id) GROUP BY dept_id ORDER BY dept_id",
    },
];

const AGGREGATE_EDGE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "empty input aggregate identities",
        sql: "SELECT COUNT(*), COUNT(qty), SUM(qty), AVG(qty), MIN(qty), MAX(qty) FROM sales WHERE qty > 1000",
    },
    QueryCase {
        name: "left join aggregate null skipping",
        sql: "SELECT COUNT(*), COUNT(ret.qty), SUM(ret.qty), COALESCE(SUM(ret.qty), 0) FROM sales s LEFT JOIN returns ret ON ret.sale_id = s.id",
    },
    QueryCase {
        name: "distinct aggregate per group",
        sql: "SELECT product_id, COUNT(*), COUNT(DISTINCT store_id), SUM(DISTINCT qty) FROM sales GROUP BY product_id ORDER BY product_id",
    },
    QueryCase {
        name: "aggregate filter on whole table",
        sql: "SELECT COUNT(*) FILTER (WHERE qty >= 10), SUM(qty) FILTER (WHERE sale_date >= '2025-03-01') FROM sales",
    },
    QueryCase {
        name: "aggregate filter per joined group",
        sql: "SELECT st.region_id, COUNT(*) FILTER (WHERE s.qty >= 10), SUM(s.qty) FILTER (WHERE p.name = 'Bolt') FROM stores st JOIN sales s ON s.store_id = st.id JOIN products p ON p.id = s.product_id GROUP BY st.region_id ORDER BY st.region_id",
    },
    QueryCase {
        name: "having with distinct aggregate alias",
        sql: "SELECT st.name, COUNT(DISTINCT s.product_id) AS product_count, SUM(s.qty) AS total_qty FROM stores st LEFT JOIN sales s ON s.store_id = st.id GROUP BY st.name HAVING product_count >= 2 OR total_qty IS NULL ORDER BY st.name",
    },
    QueryCase {
        name: "case expression inside aggregate",
        sql: "SELECT SUM(CASE WHEN qty >= 10 THEN qty END), COUNT(CASE WHEN qty < 10 THEN 1 END) FROM sales",
    },
];

const GROUP_CONCAT_SETUP: &str = "
    CREATE TABLE concat_groups (
        grp TEXT PRIMARY KEY
    );
    CREATE TABLE concat_items (
        id INTEGER PRIMARY KEY,
        grp TEXT,
        label TEXT,
        amount INTEGER,
        ord INTEGER
    );
    INSERT INTO concat_groups (grp) VALUES ('A'), ('B'), ('C');
    INSERT INTO concat_items (id, grp, label, amount, ord) VALUES
        (1, 'A', 'red', 10, 2),
        (2, 'A', 'blue', 20, 1),
        (3, 'A', NULL, 30, 3),
        (4, 'B', 'green', NULL, 1),
        (5, 'B', 'yellow', 40, 2);
";

const GROUP_CONCAT_SEPARATOR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "default separator skips null inputs",
        sql: "SELECT grp, group_concat(label) FROM concat_items GROUP BY grp ORDER BY grp",
    },
    QueryCase {
        name: "custom separator skips null inputs",
        sql: "SELECT grp, group_concat(label, '|') FROM concat_items GROUP BY grp ORDER BY grp",
    },
    QueryCase {
        name: "left join empty group returns null",
        sql: "SELECT g.grp, group_concat(i.label, ':') FROM concat_groups AS g LEFT JOIN concat_items AS i USING (grp) GROUP BY g.grp ORDER BY g.grp",
    },
    QueryCase {
        name: "numeric values are coerced to text",
        sql: "SELECT grp, group_concat(amount, '/') FROM concat_items GROUP BY grp ORDER BY grp",
    },
    QueryCase {
        name: "empty input aggregate result",
        sql: "SELECT group_concat(label), group_concat(label, '|') FROM concat_items WHERE 0",
    },
    QueryCase {
        name: "ordered subquery input",
        sql: "SELECT grp, group_concat(label, ',') FROM (SELECT grp, label FROM concat_items WHERE label IS NOT NULL ORDER BY grp, ord DESC) GROUP BY grp ORDER BY grp",
    },
];

const HAVING_AGGREGATE_ORDER_CASES: &[QueryCase] = &[
    QueryCase {
        name: "having without group by over single aggregate group",
        sql: "SELECT COUNT(*) AS sale_count, SUM(qty) AS total_qty FROM sales HAVING total_qty > 100",
    },
    QueryCase {
        name: "aggregate alias in having and order by",
        sql: "SELECT product_id, SUM(qty) AS total_qty FROM sales GROUP BY product_id HAVING total_qty >= 10 ORDER BY total_qty DESC, product_id",
    },
    QueryCase {
        name: "aggregate aliases in range having",
        sql: "SELECT product_id, MIN(qty) AS min_qty, MAX(qty) AS max_qty FROM sales GROUP BY product_id HAVING max_qty > min_qty ORDER BY product_id",
    },
    QueryCase {
        name: "aggregate expressions in order by",
        sql: "SELECT store_id, COUNT(*) AS sale_count, SUM(qty) AS total_qty FROM sales GROUP BY store_id ORDER BY SUM(qty) DESC, COUNT(*) DESC, store_id",
    },
    QueryCase {
        name: "filtered aggregate aliases in having",
        sql: "SELECT substr(sale_date, 1, 7) AS month, COUNT(*) FILTER (WHERE qty >= 10) AS large_sales, SUM(qty) FILTER (WHERE product_id = 300) AS bolt_qty FROM sales GROUP BY month HAVING large_sales > 0 OR bolt_qty IS NOT NULL ORDER BY month",
    },
    QueryCase {
        name: "aggregate ordering with limit offset",
        sql: "SELECT store_id, SUM(qty) AS total_qty FROM sales GROUP BY store_id HAVING SUM(qty) > 5 ORDER BY total_qty DESC, store_id LIMIT 2 OFFSET 1",
    },
    QueryCase {
        name: "left join aggregate having count",
        sql: "SELECT r.name, COUNT(ret.id) AS return_rows, COALESCE(SUM(ret.qty), 0) AS returned_qty FROM regions r JOIN stores st ON st.region_id = r.id LEFT JOIN sales s ON s.store_id = st.id LEFT JOIN returns ret ON ret.sale_id = s.id GROUP BY r.name HAVING return_rows >= 1 ORDER BY returned_qty DESC, r.name",
    },
];

const UPSERT_SETUP: &str = "
    CREATE TABLE kv (
        id INTEGER PRIMARY KEY,
        key TEXT NOT NULL UNIQUE,
        value INTEGER NOT NULL,
        updated INTEGER NOT NULL DEFAULT 0
    );
    INSERT INTO kv(id, key, value, updated) VALUES
        (1, 'alpha', 10, 0),
        (2, 'beta', 20, 0),
        (3, 'gamma', 30, 0);
";

const UPSERT_SCRIPT: &str = "
    INSERT INTO kv(id, key, value)
        VALUES (4, 'delta', 40)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated = updated + 1;
    INSERT INTO kv(id, key, value)
        VALUES (5, 'alpha', 15)
        ON CONFLICT(key) DO UPDATE SET value = value + excluded.value, updated = updated + 1;
    INSERT INTO kv(id, key, value)
        VALUES (2, 'beta-replaced', 99)
        ON CONFLICT(id) DO UPDATE SET key = excluded.key, value = excluded.value, updated = updated + 1;
    INSERT INTO kv(id, key, value)
        VALUES (6, 'gamma', 999)
        ON CONFLICT(key) DO NOTHING;
";

const UPSERT_CASES: &[QueryCase] = &[
    QueryCase {
        name: "final row order",
        sql: "SELECT id, key, value, updated FROM kv ORDER BY id",
    },
    QueryCase {
        name: "updated row count",
        sql: "SELECT COUNT(*) FROM kv WHERE updated > 0",
    },
    QueryCase {
        name: "value aggregate",
        sql: "SELECT SUM(value), MIN(value), MAX(value) FROM kv",
    },
];

const WITH_UPSERT_RETURNING_SETUP: &str = UPSERT_SETUP;

const WITH_UPSERT_RETURNING_CASES: &[QueryCase] = &[
    QueryCase {
        name: "with insert-select upsert returning conflict and insert rows",
        sql: "WITH incoming(key, value) AS (VALUES ('alpha', 5), ('epsilon', 50)) INSERT INTO kv(key, value) SELECT key, value FROM incoming WHERE 1 ON CONFLICT(key) DO UPDATE SET value = value + excluded.value, updated = updated + 1 RETURNING key, value, updated",
    },
    QueryCase {
        name: "with insert-select do nothing returning only inserted rows",
        sql: "WITH incoming(key, value) AS (VALUES ('beta', 70), ('zeta', 60)) INSERT INTO kv(key, value) SELECT key, value FROM incoming WHERE 1 ON CONFLICT(key) DO NOTHING RETURNING key, value, updated",
    },
    QueryCase {
        name: "final rows after with upsert returning",
        sql: "SELECT key, value, updated FROM kv ORDER BY key",
    },
];

const CONFLICT_RESOLUTION_SETUP: &str = "
    CREATE TABLE conflict_items (
        id INTEGER PRIMARY KEY,
        sku TEXT NOT NULL UNIQUE,
        qty INTEGER NOT NULL,
        note TEXT DEFAULT 'seed'
    );

    INSERT INTO conflict_items(id, sku, qty, note) VALUES
        (1, 'alpha', 10, 'first'),
        (2, 'beta', 20, 'second'),
        (3, 'gamma', 30, 'third');
";

const CONFLICT_RESOLUTION_SCRIPT: &str = "
    INSERT OR IGNORE INTO conflict_items(id, sku, qty, note)
        VALUES (4, 'alpha', 99, 'ignored-by-sku');
    INSERT OR IGNORE INTO conflict_items(id, sku, qty, note)
        VALUES (4, 'delta', 40, 'inserted');
    INSERT OR REPLACE INTO conflict_items(id, sku, qty, note)
        VALUES (2, 'beta', 25, 'replaced-by-id');
    INSERT OR REPLACE INTO conflict_items(id, sku, qty, note)
        VALUES (5, 'gamma', 35, 'replaced-by-sku');
    UPDATE OR IGNORE conflict_items
        SET sku = 'delta', note = 'ignored-update'
        WHERE id = 2;
    UPDATE OR REPLACE conflict_items
        SET sku = 'delta', qty = qty + 5, note = 'replace-update'
        WHERE id = 1;
";

const CONFLICT_RESOLUTION_CASES: &[QueryCase] = &[
    QueryCase {
        name: "final conflict resolution rows",
        sql: "SELECT id, sku, qty, note FROM conflict_items ORDER BY id",
    },
    QueryCase {
        name: "ignored and replaced rows are absent",
        sql: "SELECT COUNT(*) FROM conflict_items WHERE note LIKE 'ignored%' OR id IN (3, 4)",
    },
    QueryCase {
        name: "unique sku groups remain singular",
        sql: "SELECT sku, COUNT(*), SUM(qty) FROM conflict_items GROUP BY sku ORDER BY sku",
    },
    QueryCase {
        name: "post conflict aggregate state",
        sql: "SELECT COUNT(*), SUM(qty), MIN(id), MAX(id) FROM conflict_items",
    },
];

const CTE_SETUP: &str = "
    CREATE TABLE employees (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        manager_id INTEGER,
        salary INTEGER NOT NULL
    );
    INSERT INTO employees VALUES
        (1, 'Ada', NULL, 100),
        (2, 'Bert', 1, 80),
        (3, 'Cara', 1, 90),
        (4, 'Drew', 2, 60),
        (5, 'Eli', 2, 55),
        (6, 'Fay', 3, 70);

    CREATE TABLE facts (category TEXT NOT NULL, value INTEGER NOT NULL);
    INSERT INTO facts VALUES
        ('a', 10),
        ('a', 20),
        ('b', 7),
        ('b', 13),
        ('c', 5);
";

const CTE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "ordinary aggregate cte",
        sql: "WITH totals AS (SELECT category, SUM(value) AS total FROM facts GROUP BY category) SELECT category, total FROM totals WHERE total >= 20 ORDER BY category",
    },
    QueryCase {
        name: "multi cte join",
        sql: "WITH totals AS (SELECT category, SUM(value) AS total FROM facts GROUP BY category), counts AS (SELECT category, COUNT(*) AS cnt FROM facts GROUP BY category) SELECT totals.category, totals.total, counts.cnt FROM totals JOIN counts ON counts.category = totals.category ORDER BY totals.category",
    },
    QueryCase {
        name: "recursive hierarchy",
        sql: "WITH RECURSIVE org(id, name, depth) AS (SELECT id, name, 0 FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, org.depth + 1 FROM employees e JOIN org ON e.manager_id = org.id) SELECT name, depth FROM org ORDER BY depth, name",
    },
    QueryCase {
        name: "recursive numeric aggregate",
        sql: "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 8) SELECT SUM(x), COUNT(*), MAX(x) FROM cnt",
    },
    QueryCase {
        name: "cte self join materialization",
        sql: "WITH vals AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT a.n, b.n FROM vals a, vals b WHERE a.n < b.n ORDER BY a.n, b.n",
    },
];

const VALUES_CLAUSE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "single literal values row",
        sql: "VALUES (1, 'alpha', NULL)",
    },
    QueryCase {
        name: "multi row mixed storage values",
        sql: "VALUES (1, 'one'), (2, 'two'), (3, NULL)",
    },
    QueryCase {
        name: "expressions inside values rows",
        sql: "VALUES (1 + 2, upper('ab'), typeof(NULL)), (5 / 2, lower('CD'), typeof(3.5))",
    },
    QueryCase {
        name: "values backed cte explicit columns",
        sql: "WITH incoming(id, label) AS (VALUES (2, 'beta'), (1, 'alpha'), (3, NULL)) SELECT id, label FROM incoming ORDER BY id",
    },
    QueryCase {
        name: "aggregate over values rows",
        sql: "WITH nums(n) AS (VALUES (1), (2), (2), (NULL)) SELECT COUNT(*), COUNT(n), SUM(n), COUNT(DISTINCT n) FROM nums",
    },
    QueryCase {
        name: "join values output to table",
        sql: "WITH ids(id) AS (VALUES (1), (3), (99)) SELECT c.name FROM ids JOIN customers c ON c.id = ids.id ORDER BY c.name",
    },
];

const WINDOW_SETUP: &str = "
    CREATE TABLE compensation (
        id INTEGER PRIMARY KEY,
        dept TEXT NOT NULL,
        employee TEXT NOT NULL,
        salary INTEGER NOT NULL
    );
    INSERT INTO compensation VALUES
        (1, 'eng', 'Ada', 120),
        (2, 'eng', 'Bert', 95),
        (3, 'eng', 'Cara', 120),
        (4, 'ops', 'Drew', 85),
        (5, 'ops', 'Eli', 90),
        (6, 'ops', 'Fay', 85);
";

const WINDOW_CASES: &[QueryCase] = &[
    QueryCase {
        name: "row number partition",
        sql: "SELECT dept, employee, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC, employee) FROM compensation ORDER BY dept, salary DESC, employee",
    },
    QueryCase {
        name: "rank and dense rank ties",
        sql: "SELECT employee, salary, RANK() OVER (ORDER BY salary DESC), DENSE_RANK() OVER (ORDER BY salary DESC) FROM compensation ORDER BY salary DESC, employee",
    },
    QueryCase {
        name: "running partition aggregate",
        sql: "SELECT dept, employee, salary, SUM(salary) OVER (PARTITION BY dept ORDER BY employee ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM compensation ORDER BY dept, employee",
    },
    QueryCase {
        name: "sliding frame aggregate",
        sql: "SELECT id, salary, SUM(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM compensation ORDER BY id",
    },
    QueryCase {
        name: "lag lead defaults",
        sql: "SELECT id, salary, LAG(salary, 1, -1) OVER (ORDER BY id), LEAD(salary, 1, -1) OVER (ORDER BY id) FROM compensation ORDER BY id",
    },
];

const CAST_COLLATION_SETUP: &str = "
    CREATE TABLE typed (
        id INTEGER PRIMARY KEY,
        raw TEXT,
        amount TEXT
    );
    INSERT INTO typed VALUES
        (1, '456', '3.14'),
        (2, 'abc', '123.45abc'),
        (3, '', ''),
        (4, NULL, NULL);

    CREATE TABLE tags (tag TEXT COLLATE NOCASE);
    INSERT INTO tags VALUES
        ('Rust'),
        ('rust'),
        ('RUST'),
        ('Python'),
        ('python');

    CREATE TABLE words (w TEXT COLLATE NOCASE);
    INSERT INTO words VALUES
        ('banana'),
        ('Apple'),
        ('cherry'),
        ('APRICOT'),
        ('Blueberry');

    CREATE TABLE padded (id INTEGER PRIMARY KEY, label TEXT COLLATE RTRIM);
    INSERT INTO padded VALUES
        (1, 'abc'),
        (2, 'abc  '),
        (3, 'ABC'),
        (4, 'abd');
";

const CAST_COLLATION_CASES: &[QueryCase] = &[
    QueryCase {
        name: "cast text numeric prefixes",
        sql: "SELECT id, CAST(raw AS INTEGER), typeof(CAST(raw AS INTEGER)), CAST(amount AS REAL), typeof(CAST(amount AS REAL)) FROM typed ORDER BY id",
    },
    QueryCase {
        name: "cast scalar storage classes",
        sql: "SELECT CAST(123 AS TEXT), typeof(CAST(123 AS TEXT)), CAST(3.99 AS INTEGER), CAST(NULL AS TEXT), typeof(CAST(NULL AS TEXT))",
    },
    QueryCase {
        name: "nocase distinct aggregate",
        sql: "SELECT COUNT(DISTINCT tag), MIN(tag), MAX(tag) FROM tags",
    },
    QueryCase {
        name: "nocase grouping",
        sql: "SELECT tag, COUNT(*) FROM tags GROUP BY tag ORDER BY tag",
    },
    QueryCase {
        name: "nocase ordering",
        sql: "SELECT w FROM words ORDER BY w",
    },
    QueryCase {
        name: "rtrim equality",
        sql: "SELECT id, label FROM padded WHERE label = 'abc' ORDER BY id",
    },
];

const COLLATION_EXPRESSION_CASES: &[QueryCase] = &[
    QueryCase {
        name: "explicit nocase comparison on expression",
        sql: "SELECT w FROM words WHERE w COLLATE NOCASE = 'apple' ORDER BY w COLLATE BINARY",
    },
    QueryCase {
        name: "explicit binary comparison overrides column nocase",
        sql: "SELECT w FROM words WHERE w COLLATE BINARY = 'Apple' ORDER BY w COLLATE BINARY",
    },
    QueryCase {
        name: "explicit binary grouping overrides column nocase",
        sql: "SELECT tag COLLATE BINARY, COUNT(*) FROM tags GROUP BY tag COLLATE BINARY ORDER BY tag COLLATE BINARY",
    },
    QueryCase {
        name: "distinct aggregate respects explicit collation",
        sql: "SELECT COUNT(DISTINCT tag COLLATE BINARY), COUNT(DISTINCT tag COLLATE NOCASE) FROM tags",
    },
    QueryCase {
        name: "explicit binary comparison overrides column rtrim",
        sql: "SELECT id, label FROM padded WHERE label COLLATE BINARY = 'abc' ORDER BY id",
    },
    QueryCase {
        name: "explicit rtrim grouping on expression",
        sql: "SELECT label COLLATE RTRIM, COUNT(*) FROM padded GROUP BY label COLLATE RTRIM ORDER BY label COLLATE BINARY",
    },
];

const DML_SETUP: &str = "
    CREATE TABLE inventory (
        id INTEGER PRIMARY KEY,
        sku TEXT NOT NULL UNIQUE,
        category TEXT NOT NULL,
        qty INTEGER NOT NULL,
        price_cents INTEGER NOT NULL,
        active INTEGER NOT NULL DEFAULT 1
    );

    INSERT INTO inventory(id, sku, category, qty, price_cents, active) VALUES
        (1, 'bolt', 'hardware', 100, 50, 1),
        (2, 'screw', 'hardware', 80, 25, 1),
        (3, 'old', 'clearance', 4, 300, 1),
        (4, 'gizmo', 'gadget', 12, 500, 1);
";

const DML_SCRIPT: &str = "
    INSERT INTO inventory(id, sku, category, qty, price_cents)
        VALUES (5, 'nut', 'hardware', 40, 75);
    INSERT INTO inventory(id, sku, category, qty, price_cents, active)
        VALUES (6, 'kit', 'bundle', 37, 500, 1);

    UPDATE inventory
        SET qty = qty + 5
        WHERE category = 'hardware' AND active = 1;
    UPDATE inventory
        SET active = 0, price_cents = price_cents - 25
        WHERE sku = 'old';
    UPDATE inventory
        SET qty = qty - 15
        WHERE sku = 'bolt';
    UPDATE inventory
        SET qty = qty + 20
        WHERE sku = 'screw';

    DELETE FROM inventory
        WHERE active = 0 AND qty < 10;
    DELETE FROM inventory
        WHERE category = 'gadget' AND qty <= 12;
";

const DML_CASES: &[QueryCase] = &[
    QueryCase {
        name: "final mutated rows",
        sql: "SELECT id, sku, category, qty, price_cents, active FROM inventory ORDER BY id",
    },
    QueryCase {
        name: "category summaries after dml",
        sql: "SELECT category, COUNT(*), SUM(qty), MIN(price_cents), MAX(price_cents) FROM inventory GROUP BY category ORDER BY category",
    },
    QueryCase {
        name: "active inventory value",
        sql: "SELECT COUNT(*), SUM(qty), SUM(price_cents * qty) FROM inventory WHERE active = 1",
    },
    QueryCase {
        name: "deleted row absence",
        sql: "SELECT COUNT(*) FROM inventory WHERE sku IN ('old', 'gizmo')",
    },
];

const CHANGE_TRACKING_SETUP: &str = "
    CREATE TABLE change_tracking (
        id INTEGER PRIMARY KEY,
        label TEXT UNIQUE
    );
";

const CHANGE_TRACKING_STATE_QUERY: &str = "SELECT last_insert_rowid(), changes(), total_changes()";

const DML_RETURNING_SETUP: &str = "
    CREATE TABLE returning_items (
        id INTEGER PRIMARY KEY,
        sku TEXT NOT NULL UNIQUE,
        qty INTEGER NOT NULL,
        note TEXT DEFAULT 'seed'
    );

    INSERT INTO returning_items(id, sku, qty, note) VALUES
        (1, 'alpha', 10, 'first'),
        (2, 'beta', 20, 'second'),
        (3, 'gamma', 30, 'third');
";

const DML_RETURNING_CASES: &[QueryCase] = &[
    QueryCase {
        name: "insert returning expressions",
        sql: "INSERT INTO returning_items(sku, qty, note) VALUES ('delta', 40, 'inserted') RETURNING id, sku, qty, note, qty * 2",
    },
    QueryCase {
        name: "update returning mutated row",
        sql: "UPDATE returning_items SET qty = qty + 5, note = note || '-updated' WHERE sku = 'alpha' RETURNING id, sku, qty, note",
    },
    QueryCase {
        name: "delete returning deleted row",
        sql: "DELETE FROM returning_items WHERE sku = 'gamma' RETURNING id, sku, qty, note",
    },
    QueryCase {
        name: "insert returning defaulted column",
        sql: "INSERT INTO returning_items(id, sku, qty) VALUES (10, 'epsilon', 50) RETURNING id, sku, qty, note",
    },
    QueryCase {
        name: "final returning table state",
        sql: "SELECT id, sku, qty, note FROM returning_items ORDER BY id",
    },
];

const ATTACHED_UPDATE_SETUP: &str = "
    ATTACH DATABASE ':memory:' AS aux;

    CREATE TABLE main_filter (id INTEGER PRIMARY KEY);
    INSERT INTO main_filter VALUES (1), (3);

    CREATE TABLE aux.t (
        id INTEGER PRIMARY KEY,
        value TEXT NOT NULL UNIQUE,
        qty INTEGER NOT NULL
    );
    INSERT INTO aux.t VALUES
        (1, 'alpha', 10),
        (2, 'beta', 20),
        (3, 'gamma', 30);
";

const ATTACHED_UPDATE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "attached update returning single row",
        sql: "UPDATE aux.t SET qty = qty + 1 WHERE id = 1 RETURNING id, value, qty",
    },
    QueryCase {
        name: "attached update returning with main subquery",
        sql: "UPDATE aux.t SET value = value || '-hit' WHERE id IN (SELECT id FROM main_filter) RETURNING id, value, qty",
    },
    QueryCase {
        name: "attached update with cte materialization",
        sql: "WITH picked(id) AS (SELECT id FROM main_filter WHERE id <> 1) UPDATE aux.t SET qty = qty + 10 WHERE id IN (SELECT id FROM picked) RETURNING id, qty",
    },
    QueryCase {
        name: "attached update persisted rows",
        sql: "SELECT id, value, qty FROM aux.t ORDER BY id",
    },
    QueryCase {
        name: "attached update aggregate",
        sql: "SELECT COUNT(*), SUM(qty), MIN(value), MAX(value) FROM aux.t",
    },
];

const ATTACHED_INSERT_SELECT_SETUP: &str = "
    ATTACH DATABASE ':memory:' AS aux;

    CREATE TABLE source_items (
        id INTEGER PRIMARY KEY,
        value TEXT NOT NULL,
        qty INTEGER NOT NULL
    );
    INSERT INTO source_items VALUES
        (1, 'alpha', 10),
        (2, 'beta', 20),
        (3, 'gamma', 30),
        (4, 'delta', 40);

    CREATE TABLE main_filter (id INTEGER PRIMARY KEY);
    INSERT INTO main_filter VALUES (1), (3);

    CREATE TABLE aux.t (
        id INTEGER PRIMARY KEY,
        value TEXT NOT NULL UNIQUE,
        qty INTEGER NOT NULL
    );

    INSERT INTO aux.t(id, value, qty)
        SELECT id, value, qty
        FROM source_items
        WHERE qty >= 20
        ORDER BY id;

    INSERT INTO aux.t(id, value, qty)
        SELECT id + 10, value || '-copy', qty + 1
        FROM source_items
        WHERE id IN (SELECT id FROM main_filter)
        ORDER BY id;
";

const ATTACHED_INSERT_SELECT_CASES: &[QueryCase] = &[
    QueryCase {
        name: "attached insert select persisted rows",
        sql: "SELECT id, value, qty FROM aux.t ORDER BY id",
    },
    QueryCase {
        name: "attached insert select aggregate",
        sql: "SELECT COUNT(*), SUM(qty), MIN(value), MAX(value) FROM aux.t",
    },
    QueryCase {
        name: "attached insert select copied filtered rows",
        sql: "SELECT id, value, qty FROM aux.t WHERE id >= 10 ORDER BY id",
    },
];

const ATTACHED_DROP_SETUP: &str = "
    ATTACH DATABASE ':memory:' AS aux;

    CREATE TABLE aux.keep (
        id INTEGER PRIMARY KEY,
        value TEXT NOT NULL
    );
    INSERT INTO aux.keep VALUES (1, 'stay');

    CREATE TABLE aux.drop_me (
        id INTEGER PRIMARY KEY,
        value TEXT NOT NULL
    );
    INSERT INTO aux.drop_me VALUES (1, 'gone');

    CREATE TABLE aux.drop_index_only (
        id INTEGER PRIMARY KEY,
        value TEXT NOT NULL
    );
    INSERT INTO aux.drop_index_only VALUES (1, 'indexed'), (2, 'also-indexed');

    CREATE INDEX aux.idx_drop_me_value ON drop_me(value);
    CREATE INDEX aux.idx_drop_index_only_value ON drop_index_only(value);
";

const ATTACHED_DROP_SCRIPT: &str = "
    DROP INDEX aux.idx_drop_index_only_value;
    DROP TABLE aux.drop_me;
    DROP TABLE IF EXISTS aux.missing_table;
";

const ATTACHED_DROP_CASES: &[QueryCase] = &[
    QueryCase {
        name: "attached drop removes table and indexes",
        sql: "SELECT type, name FROM aux.sqlite_master WHERE name IN ('drop_me', 'idx_drop_me_value', 'drop_index_only', 'idx_drop_index_only_value', 'keep') ORDER BY type, name",
    },
    QueryCase {
        name: "attached drop preserves unrelated table rows",
        sql: "SELECT id, value FROM aux.keep ORDER BY id",
    },
    QueryCase {
        name: "attached drop index preserves table rows",
        sql: "SELECT id, value FROM aux.drop_index_only ORDER BY id",
    },
];

const ATTACHED_CREATE_VIEW_SETUP: &str = "
    ATTACH DATABASE ':memory:' AS aux;

    CREATE TABLE aux.base (
        id INTEGER PRIMARY KEY,
        value TEXT NOT NULL,
        qty INTEGER NOT NULL
    );
    INSERT INTO aux.base VALUES
        (1, 'alpha', 10),
        (2, 'beta', 20),
        (3, 'gamma', 30);

    CREATE VIEW aux.filtered AS
        SELECT id, value, qty * 2 AS doubled
        FROM base
        WHERE qty >= 20;
";

const ATTACHED_CREATE_VIEW_CASES: &[QueryCase] = &[
    QueryCase {
        name: "attached create view projected rows",
        sql: "SELECT id, value, doubled FROM aux.filtered ORDER BY id",
    },
    QueryCase {
        name: "attached create view sqlite_master row",
        sql: "SELECT type, name FROM aux.sqlite_master WHERE name = 'filtered'",
    },
    QueryCase {
        name: "attached create view aggregate",
        sql: "SELECT COUNT(*), SUM(doubled), MIN(value), MAX(value) FROM aux.filtered",
    },
];

const ATTACHED_VACUUM_SETUP: &str = "
    ATTACH DATABASE ':memory:' AS aux;

    CREATE TABLE aux.items (
        id INTEGER PRIMARY KEY,
        value TEXT NOT NULL,
        qty INTEGER NOT NULL
    );
    INSERT INTO aux.items VALUES
        (1, 'alpha', 10),
        (2, 'beta', 20),
        (3, 'gamma', 30);
    DELETE FROM aux.items WHERE id = 2;

    VACUUM aux;
";

const ATTACHED_VACUUM_CASES: &[QueryCase] = &[
    QueryCase {
        name: "attached vacuum preserves live rows",
        sql: "SELECT id, value, qty FROM aux.items ORDER BY id",
    },
    QueryCase {
        name: "attached vacuum preserves schema row",
        sql: "SELECT type, name FROM aux.sqlite_master WHERE name = 'items'",
    },
    QueryCase {
        name: "attached vacuum aggregate",
        sql: "SELECT COUNT(*), SUM(qty), MIN(value), MAX(value) FROM aux.items",
    },
];

const ERROR_PATH_SETUP: &str = "
    CREATE TABLE constrained (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        sku TEXT NOT NULL,
        qty INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX idx_constrained_sku ON constrained(sku);
    INSERT INTO constrained(id, name, sku, qty) VALUES
        (1, 'alpha', 'sku-a', 10),
        (2, 'beta', 'sku-b', 20);
";

const ERROR_PATH_CASES: &[StatementCase] = &[
    StatementCase {
        name: "duplicate integer primary key",
        sql: "INSERT INTO constrained(id, name, sku, qty) VALUES (1, 'dupe', 'sku-c', 1)",
    },
    StatementCase {
        name: "duplicate unique index value",
        sql: "INSERT INTO constrained(id, name, sku, qty) VALUES (3, 'gamma', 'sku-a', 1)",
    },
    StatementCase {
        name: "insert null into not null",
        sql: "INSERT INTO constrained(id, name, sku, qty) VALUES (4, NULL, 'sku-d', 1)",
    },
    StatementCase {
        name: "insert missing not null column",
        sql: "INSERT INTO constrained(id, name, sku) VALUES (5, 'missing-qty', 'sku-e')",
    },
    StatementCase {
        name: "update not null column to null",
        sql: "UPDATE constrained SET name = NULL WHERE id = 1",
    },
    StatementCase {
        name: "unknown table lookup",
        sql: "SELECT * FROM missing_table",
    },
    StatementCase {
        name: "unknown column lookup",
        sql: "SELECT missing_column FROM constrained",
    },
];

const CHECK_CONSTRAINT_SETUP: &str = "
    CREATE TABLE checked_items (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL CHECK(name <> ''),
        qty INTEGER CHECK(qty >= 0),
        price INTEGER NOT NULL,
        discount INTEGER NOT NULL DEFAULT 0,
        CHECK(discount >= 0 AND price >= discount),
        CHECK(qty IS NULL OR qty <= 100)
    );

    INSERT INTO checked_items(id, name, qty, price, discount) VALUES
        (1, 'alpha', 10, 100, 0),
        (2, 'beta', NULL, 50, 5);
";

const CHECK_CONSTRAINT_SCRIPT: &str = "
    INSERT INTO checked_items(id, name, qty, price, discount)
        VALUES (3, 'gamma', 0, 25, 25);
    INSERT OR IGNORE INTO checked_items(id, name, qty, price, discount)
        VALUES (4, 'bad-qty', -1, 10, 0);
    INSERT OR IGNORE INTO checked_items(id, name, qty, price, discount)
        VALUES (5, 'bad-upper-bound', 101, 10, 0);
    INSERT OR IGNORE INTO checked_items(id, name, qty, price, discount)
        VALUES (6, 'bad-price', 1, 5, 10);
    UPDATE checked_items
        SET qty = 15, discount = 10
        WHERE id = 1;
    UPDATE OR IGNORE checked_items
        SET price = 1, discount = 9
        WHERE id = 3;
";

const CHECK_CONSTRAINT_CASES: &[QueryCase] = &[
    QueryCase {
        name: "final checked rows",
        sql: "SELECT id, name, qty, price, discount FROM checked_items ORDER BY id",
    },
    QueryCase {
        name: "ignored check violations did not insert",
        sql: "SELECT COUNT(*) FROM checked_items WHERE id IN (4, 5, 6)",
    },
    QueryCase {
        name: "null passes check and aggregates remain stable",
        sql: "SELECT COUNT(*), COUNT(qty), SUM(qty), SUM(price - discount) FROM checked_items",
    },
    QueryCase {
        name: "table checks hold after ignored update",
        sql: "SELECT id, qty IS NULL, price >= discount FROM checked_items ORDER BY id",
    },
];

const CHECK_CONSTRAINT_ERROR_CASES: &[StatementCase] = &[
    StatementCase {
        name: "insert violates column check",
        sql: "INSERT INTO checked_items(id, name, qty, price, discount) VALUES (7, 'negative', -1, 10, 0)",
    },
    StatementCase {
        name: "insert violates text check",
        sql: "INSERT INTO checked_items(id, name, qty, price, discount) VALUES (8, '', 1, 10, 0)",
    },
    StatementCase {
        name: "insert violates table check",
        sql: "INSERT INTO checked_items(id, name, qty, price, discount) VALUES (9, 'over-discount', 1, 10, 20)",
    },
    StatementCase {
        name: "update violates upper bound check",
        sql: "UPDATE checked_items SET qty = 101 WHERE id = 1",
    },
    StatementCase {
        name: "update violates multi-column check",
        sql: "UPDATE checked_items SET discount = price + 1 WHERE id = 2",
    },
];

const FOREIGN_KEY_ACTION_SETUP: &str = "
    PRAGMA foreign_keys = ON;

    CREATE TABLE fk_parent (
        id INTEGER PRIMARY KEY,
        code TEXT NOT NULL UNIQUE
    );
    CREATE TABLE fk_child_cascade (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER REFERENCES fk_parent(id) ON DELETE CASCADE,
        label TEXT NOT NULL
    );
    CREATE TABLE fk_child_setnull (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER REFERENCES fk_parent(id) ON DELETE SET NULL,
        label TEXT NOT NULL
    );
    CREATE TABLE fk_child_restrict (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER REFERENCES fk_parent(id) ON DELETE RESTRICT,
        label TEXT NOT NULL
    );

    INSERT INTO fk_parent VALUES
        (1, 'cascade'),
        (2, 'set-null'),
        (3, 'restrict'),
        (4, 'survive');
    INSERT INTO fk_child_cascade VALUES
        (10, 1, 'cascade-a'),
        (11, 1, 'cascade-b'),
        (12, 4, 'survivor');
    INSERT INTO fk_child_setnull VALUES
        (20, 2, 'set-null-a'),
        (21, 4, 'survivor');
    INSERT INTO fk_child_restrict VALUES
        (30, 3, 'restrict-a');
";

const FOREIGN_KEY_ACTION_SCRIPT: &str = "
    DELETE FROM fk_parent WHERE id = 1;
    DELETE FROM fk_parent WHERE id = 2;
";

const FOREIGN_KEY_ACTION_CASES: &[QueryCase] = &[
    QueryCase {
        name: "parent rows after cascade and set null deletes",
        sql: "SELECT id, code FROM fk_parent ORDER BY id",
    },
    QueryCase {
        name: "cascade child rows removed",
        sql: "SELECT id, parent_id, label FROM fk_child_cascade ORDER BY id",
    },
    QueryCase {
        name: "set null child rows retained",
        sql: "SELECT id, parent_id, label FROM fk_child_setnull ORDER BY id",
    },
    QueryCase {
        name: "restrict child rows retained",
        sql: "SELECT id, parent_id, label FROM fk_child_restrict ORDER BY id",
    },
    QueryCase {
        name: "foreign key check remains clean",
        sql: "PRAGMA foreign_key_check",
    },
];

const FOREIGN_KEY_ACTION_ERROR_CASES: &[StatementCase] = &[
    StatementCase {
        name: "insert missing parent is rejected",
        sql: "INSERT INTO fk_child_cascade(id, parent_id, label) VALUES (13, 99, 'missing-parent')",
    },
    StatementCase {
        name: "restrict delete is rejected",
        sql: "DELETE FROM fk_parent WHERE id = 3",
    },
];

const DDL_SETUP: &str = "
    CREATE TABLE accounts (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        tier TEXT DEFAULT 'free',
        score INTEGER DEFAULT 0
    );
    CREATE INDEX idx_accounts_tier ON accounts(tier);
    INSERT INTO accounts(id, name) VALUES
        (1, 'Ada'),
        (2, 'Bert');
    INSERT INTO accounts(id, name, tier, score) VALUES
        (3, 'Cara', 'pro', 20),
        (4, 'Drew', 'free', 5),
        (5, 'Eli', 'enterprise', 50);

    ALTER TABLE accounts ADD COLUMN active INTEGER DEFAULT 1;
    UPDATE accounts SET active = 0 WHERE id = 2;
    CREATE VIEW active_accounts AS
        SELECT id, name, tier, score FROM accounts WHERE active = 1;
";

const DDL_CASES: &[QueryCase] = &[
    QueryCase {
        name: "defaults and altered column values",
        sql: "SELECT id, name, tier, score, active FROM accounts ORDER BY id",
    },
    QueryCase {
        name: "grouping after ddl defaults",
        sql: "SELECT tier, COUNT(*), SUM(score) FROM accounts GROUP BY tier ORDER BY tier",
    },
    QueryCase {
        name: "view projection and filter",
        sql: "SELECT name, tier, score FROM active_accounts ORDER BY score DESC, name",
    },
    QueryCase {
        name: "index schema registration",
        sql: "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'index' AND name = 'idx_accounts_tier'",
    },
    QueryCase {
        name: "view schema registration",
        sql: "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'view' AND name = 'active_accounts'",
    },
];

const CTAS_SETUP: &str = "
    CREATE TABLE ctas_source (
        id INTEGER PRIMARY KEY,
        category TEXT NOT NULL,
        name TEXT NOT NULL,
        qty INTEGER NOT NULL,
        price INTEGER NOT NULL,
        active INTEGER NOT NULL
    );

    INSERT INTO ctas_source(id, category, name, qty, price, active) VALUES
        (1, 'hardware', 'bolt', 10, 2, 1),
        (2, 'hardware', 'nut', 5, 3, 0),
        (3, 'tool', 'driver', 2, 25, 1),
        (4, 'tool', 'wrench', 1, 40, 1);

    CREATE TABLE ctas_filtered AS
        SELECT id, name, qty, price, qty * price AS inventory_value
        FROM ctas_source
        WHERE active = 1;

    CREATE TABLE ctas_expression AS
        SELECT category, upper(name) AS upper_name, qty + price AS qty_plus_price
        FROM ctas_source
        WHERE id IN (1, 3, 4);

    CREATE TABLE ctas_empty AS
        SELECT id, name, qty
        FROM ctas_source
        WHERE qty < 0;
";

const CTAS_SCRIPT: &str = "
    INSERT INTO ctas_empty VALUES (99, 'manual', 7);
";

const CTAS_CASES: &[QueryCase] = &[
    QueryCase {
        name: "filtered projection rows",
        sql: "SELECT id, name, qty, price, inventory_value FROM ctas_filtered ORDER BY id",
    },
    QueryCase {
        name: "expression projection rows",
        sql: "SELECT category, upper_name, qty_plus_price FROM ctas_expression ORDER BY category, upper_name",
    },
    QueryCase {
        name: "empty ctas accepts later insert",
        sql: "SELECT id, name, qty FROM ctas_empty ORDER BY id",
    },
    QueryCase {
        name: "ctas projected value storage classes",
        sql: "SELECT id, typeof(inventory_value), inventory_value FROM ctas_filtered ORDER BY id",
    },
    QueryCase {
        name: "ctas schema rows registered",
        sql: "SELECT name FROM sqlite_schema WHERE type = 'table' AND name IN ('ctas_filtered', 'ctas_expression', 'ctas_empty') ORDER BY name",
    },
];

const DEFAULT_VALUE_SETUP: &str = "
    CREATE TABLE default_items (
        id INTEGER PRIMARY KEY,
        name TEXT DEFAULT 'unnamed',
        qty INTEGER DEFAULT 7,
        active INTEGER NOT NULL DEFAULT 1,
        note TEXT
    );
";

const DEFAULT_VALUE_SCRIPT: &str = "
    INSERT INTO default_items DEFAULT VALUES;
    INSERT INTO default_items(id, note) VALUES (10, 'explicit id');
    INSERT INTO default_items(name, qty, active, note) VALUES ('custom', 3, 0, 'all explicit');
    INSERT INTO default_items(id, name, note) VALUES (20, NULL, 'explicit null name');
    INSERT INTO default_items(name) VALUES ('name-only');
";

const DEFAULT_VALUE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "default values and omitted columns",
        sql: "SELECT id, name, qty, active, note FROM default_items ORDER BY id",
    },
    QueryCase {
        name: "defaulted column aggregates",
        sql: "SELECT COUNT(*), SUM(qty), SUM(active), COUNT(note), COUNT(name) FROM default_items",
    },
    QueryCase {
        name: "explicit null does not use default",
        sql: "SELECT id, name IS NULL, note FROM default_items WHERE id = 20",
    },
    QueryCase {
        name: "defaulted values participate in grouping",
        sql: "SELECT active, COUNT(*), SUM(qty) FROM default_items GROUP BY active ORDER BY active",
    },
];

const ROWID_IDENTIFIER_SETUP: &str = r#"
    CREATE TABLE rowid_items (
        name TEXT NOT NULL,
        qty INTEGER NOT NULL
    );
    INSERT INTO rowid_items(rowid, name, qty) VALUES
        (5, 'five', 50),
        (2, 'two', 20),
        (9, 'nine', 90);

    CREATE TABLE rowid_alias_items (
        id INTEGER PRIMARY KEY,
        label TEXT NOT NULL
    );
    INSERT INTO rowid_alias_items(id, label) VALUES
        (3, 'three'),
        (1, 'one');

    CREATE TABLE quoted_names (
        "select" TEXT NOT NULL,
        "from" INTEGER NOT NULL,
        "Mixed Name" TEXT NOT NULL
    );
    INSERT INTO quoted_names("select", "from", "Mixed Name") VALUES
        ('alpha', 2, 'A'),
        ('beta', 1, 'B');
"#;

const ROWID_IDENTIFIER_CASES: &[QueryCase] = &[
    QueryCase {
        name: "implicit rowid aliases project identically",
        sql: "SELECT rowid, _rowid_, oid, name FROM rowid_items ORDER BY rowid",
    },
    QueryCase {
        name: "integer primary key aliases rowid",
        sql: "SELECT rowid, id, label FROM rowid_alias_items ORDER BY rowid",
    },
    QueryCase {
        name: "rowid predicates and descending order",
        sql: "SELECT rowid, name FROM rowid_items WHERE rowid IN (2, 9) ORDER BY rowid DESC",
    },
    QueryCase {
        name: "quoted reserved word identifiers",
        sql: "SELECT \"select\", \"from\", \"Mixed Name\" FROM quoted_names ORDER BY \"from\"",
    },
    QueryCase {
        name: "qualified quoted identifier expressions",
        sql: "SELECT q.\"select\" || ':' || q.\"Mixed Name\" FROM quoted_names AS q ORDER BY q.\"select\"",
    },
];

const TRANSACTION_SETUP: &str = "
    CREATE TABLE ledger (
        id INTEGER PRIMARY KEY,
        label TEXT NOT NULL,
        amount INTEGER NOT NULL
    );
";

const TRANSACTION_SCRIPT: &str = "
    BEGIN;
    INSERT INTO ledger(id, label, amount) VALUES
        (1, 'opening', 100),
        (2, 'fee', -10);
    SAVEPOINT adjust;
    UPDATE ledger SET amount = amount + 50 WHERE id = 1;
    INSERT INTO ledger(id, label, amount) VALUES (3, 'transient', 999);
    ROLLBACK TO adjust;
    RELEASE adjust;
    INSERT INTO ledger(id, label, amount) VALUES (4, 'settled', 25);
    COMMIT;

    BEGIN;
    INSERT INTO ledger(id, label, amount) VALUES (5, 'rolled-back', 500);
    UPDATE ledger SET amount = amount - 100 WHERE id = 4;
    ROLLBACK;
";

const TRANSACTION_CASES: &[QueryCase] = &[
    QueryCase {
        name: "committed rows survive savepoint rollback",
        sql: "SELECT id, label, amount FROM ledger ORDER BY id",
    },
    QueryCase {
        name: "rolled back rows absent",
        sql: "SELECT COUNT(*) FROM ledger WHERE id IN (3, 5)",
    },
    QueryCase {
        name: "aggregate after transaction boundaries",
        sql: "SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM ledger",
    },
];

const COMPOUND_SETUP: &str = "
    CREATE TABLE left_values (group_name TEXT NOT NULL, value INTEGER NOT NULL);
    CREATE TABLE right_values (group_name TEXT NOT NULL, value INTEGER NOT NULL);

    INSERT INTO left_values VALUES
        ('a', 1),
        ('a', 2),
        ('a', 3),
        ('b', 3),
        ('b', 4);
    INSERT INTO right_values VALUES
        ('a', 3),
        ('a', 4),
        ('a', 5),
        ('b', 4),
        ('b', 6);
";

const COMPOUND_CASES: &[QueryCase] = &[
    QueryCase {
        name: "union distinct",
        sql: "SELECT value FROM left_values WHERE group_name = 'a' UNION SELECT value FROM right_values WHERE group_name = 'a' ORDER BY value",
    },
    QueryCase {
        name: "union all preserves duplicates",
        sql: "SELECT value FROM left_values WHERE value >= 3 UNION ALL SELECT value FROM right_values WHERE value <= 4 ORDER BY value",
    },
    QueryCase {
        name: "intersect",
        sql: "SELECT value FROM left_values INTERSECT SELECT value FROM right_values ORDER BY value",
    },
    QueryCase {
        name: "except",
        sql: "SELECT value FROM left_values EXCEPT SELECT value FROM right_values ORDER BY value",
    },
    QueryCase {
        name: "compound order limit offset",
        sql: "SELECT value FROM left_values UNION ALL SELECT value FROM right_values ORDER BY value LIMIT 4 OFFSET 2",
    },
];

const COMPOUND_EDGE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "union distinct deduplicates null rows",
        sql: "SELECT NULL AS value UNION SELECT NULL UNION SELECT 1 ORDER BY value",
    },
    QueryCase {
        name: "multi column union distinct",
        sql: "SELECT group_name, value FROM left_values WHERE value >= 3 UNION SELECT group_name, value FROM right_values WHERE value <= 4 ORDER BY group_name, value",
    },
    QueryCase {
        name: "union all ordinal order limit offset",
        sql: "SELECT group_name, value FROM left_values UNION ALL SELECT group_name, value FROM right_values ORDER BY 2 DESC, 1 LIMIT 5 OFFSET 1",
    },
    QueryCase {
        name: "intersect after projection expression",
        sql: "SELECT value % 2 AS parity FROM left_values INTERSECT SELECT value % 2 FROM right_values ORDER BY parity",
    },
    QueryCase {
        name: "except with filtered right arm",
        sql: "SELECT value FROM left_values EXCEPT SELECT value FROM right_values WHERE group_name = 'b' ORDER BY value",
    },
    QueryCase {
        name: "compound subquery filtering",
        sql: "SELECT value FROM (SELECT value FROM left_values UNION ALL SELECT value FROM right_values) WHERE value % 2 = 0 ORDER BY value",
    },
];

const CASE_NULL_SETUP: &str = "
    CREATE TABLE expr_rows (
        id INTEGER PRIMARY KEY,
        a INTEGER,
        b INTEGER,
        label TEXT
    );

    INSERT INTO expr_rows VALUES
        (1, 10, 5, 'high'),
        (2, 5, 5, 'same'),
        (3, 2, 9, NULL),
        (4, NULL, 7, 'missing-a'),
        (5, 4, NULL, NULL);
";

const CASE_NULL_CASES: &[QueryCase] = &[
    QueryCase {
        name: "searched case null aware comparisons",
        sql: "SELECT id, CASE WHEN a IS NULL THEN 'missing-a' WHEN b IS NULL THEN 'missing-b' WHEN a > b THEN 'gt' WHEN a = b THEN 'eq' ELSE 'lt' END FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "simple case expression",
        sql: "SELECT id, CASE label WHEN 'high' THEN 1 WHEN 'same' THEN 2 ELSE 0 END FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "case aggregate over nulls",
        sql: "SELECT SUM(CASE WHEN label IS NULL THEN 1 ELSE 0 END), SUM(CASE WHEN a IS NULL OR b IS NULL THEN 1 ELSE 0 END) FROM expr_rows",
    },
    QueryCase {
        name: "coalesce projection",
        sql: "SELECT id, COALESCE(label, 'none'), COALESCE(a, b, 0) FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "boolean precedence with null predicates",
        sql: "SELECT id FROM expr_rows WHERE NOT (a > 3 AND b > 3) OR label IS NULL ORDER BY id",
    },
];

const SCALAR_NULL_COMPARISON_CASES: &[QueryCase] = &[
    QueryCase {
        name: "is and is not null predicates",
        sql: "SELECT id, a IS b, a IS NOT b, a IS NULL, b IS NOT NULL FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "between three valued logic",
        sql: "SELECT id, a BETWEEN 3 AND 10, a NOT BETWEEN 3 AND 10, a BETWEEN b AND 10 FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "in list null semantics",
        sql: "SELECT id, a IN (5, 10, NULL), a NOT IN (5, 10, NULL), label IN ('high', NULL) FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "in subquery and not in subquery",
        sql: "SELECT id, a IN (SELECT b FROM expr_rows WHERE b IS NOT NULL), a NOT IN (SELECT b FROM expr_rows WHERE b IS NOT NULL) FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "ifnull and nullif projections",
        sql: "SELECT id, IFNULL(label, 'fallback'), NULLIF(a, b), NULLIF(label, 'high') FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "null scalar constants",
        sql: "SELECT NULL = NULL, NULL != NULL, NULL IS NULL, NULL IS NOT NULL, 1 IS NOT NULL, '' IS NOT NULL",
    },
];

const IS_DISTINCT_FROM_CASES: &[QueryCase] = &[
    QueryCase {
        name: "literal null distinctness",
        sql: "SELECT NULL IS DISTINCT FROM NULL, NULL IS DISTINCT FROM 1, 1 IS DISTINCT FROM NULL, 1 IS NOT DISTINCT FROM 1, 1 IS NOT DISTINCT FROM NULL",
    },
    QueryCase {
        name: "literal mixed storage distinctness",
        sql: "SELECT 1 IS DISTINCT FROM 1.0, 1 IS DISTINCT FROM '1', 'a' IS DISTINCT FROM 'A', X'41' IS DISTINCT FROM 'A'",
    },
    QueryCase {
        name: "column driven null aware distinctness",
        sql: "SELECT id, a IS DISTINCT FROM b, a IS NOT DISTINCT FROM b, label IS DISTINCT FROM NULL, label IS NOT DISTINCT FROM 'high' FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "expression operands",
        sql: "SELECT id, (a + 0) IS DISTINCT FROM b, COALESCE(label, 'none') IS NOT DISTINCT FROM label, (a > b) IS DISTINCT FROM NULL FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "scalar subquery operands",
        sql: "SELECT id, a IS DISTINCT FROM (SELECT max(b) FROM expr_rows), label IS NOT DISTINCT FROM (SELECT label FROM expr_rows WHERE id = 1) FROM expr_rows ORDER BY id",
    },
];

const TYPEOF_SCALAR_SETUP: &str = "
    CREATE TABLE typeof_values (
        id INTEGER PRIMARY KEY,
        value
    );
    INSERT INTO typeof_values (id, value) VALUES
        (1, NULL),
        (2, 42),
        (3, 3.5),
        (4, 'text'),
        (5, X'00FF'),
        (6, '12');
";

const TYPEOF_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "literal storage classes",
        sql: "SELECT typeof(NULL), typeof(0), typeof(-7), typeof(3.5), typeof('text'), typeof(X'00FF')",
    },
    QueryCase {
        name: "expression result storage classes",
        sql: "SELECT typeof(1 + 2), typeof(1 + 2.5), typeof('1' + 2), typeof('a' || 'b'), typeof(NULL || 'x')",
    },
    QueryCase {
        name: "cast result storage classes",
        sql: "SELECT typeof(CAST(42 AS TEXT)), typeof(CAST('42' AS INTEGER)), typeof(CAST('3.5' AS REAL)), typeof(CAST('abc' AS BLOB)), typeof(CAST(NULL AS BLOB))",
    },
    QueryCase {
        name: "column driven storage classes",
        sql: "SELECT id, typeof(value), quote(value) FROM typeof_values ORDER BY id",
    },
    QueryCase {
        name: "case and coalesce storage classes",
        sql: "SELECT id, typeof(CASE WHEN id % 2 = 0 THEN value ELSE NULL END), typeof(coalesce(value, 'fallback')) FROM typeof_values ORDER BY id",
    },
    QueryCase {
        name: "scalar subquery storage classes",
        sql: "SELECT typeof((SELECT value FROM typeof_values WHERE id = 5)), typeof((SELECT value FROM typeof_values WHERE id = 1)), typeof((SELECT max(value) FROM typeof_values WHERE id IN (2, 3)))",
    },
];

const CONDITIONAL_SCALAR_SETUP: &str = "
    CREATE TABLE conditional_values (
        id INTEGER PRIMARY KEY,
        a INTEGER,
        b REAL,
        label TEXT,
        fallback TEXT
    );
    INSERT INTO conditional_values (id, a, b, label, fallback) VALUES
        (1, NULL, NULL, NULL, 'fallback'),
        (2, 0, 0.0, '', 'empty'),
        (3, 5, 2.5, 'five', NULL),
        (4, -3, -1.5, 'same', 'same'),
        (5, NULL, 7.0, '7', 'alt');
";

const CONDITIONAL_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "coalesce preserves first non null storage class",
        sql: "SELECT coalesce(NULL, NULL, 'x'), typeof(coalesce(NULL, 42, 'x')), coalesce(NULL, 3.5), typeof(coalesce(NULL, 3.5))",
    },
    QueryCase {
        name: "ifnull preserves selected storage class",
        sql: "SELECT ifnull(NULL, 'fallback'), ifnull('value', 'fallback'), typeof(ifnull(NULL, 7)), typeof(ifnull(7, 'fallback'))",
    },
    QueryCase {
        name: "nullif equality null and storage behavior",
        sql: "SELECT nullif(1, 1), nullif(1, 2), nullif('a', 'a'), nullif('a', 'A'), typeof(nullif(1, 2)), typeof(nullif(NULL, NULL)), nullif(NULL, 1), nullif(1, NULL)",
    },
    QueryCase {
        name: "iif numeric truthiness",
        sql: "SELECT iif(1, 'T', 'F'), iif(0, 'T', 'F'), iif(NULL, 'T', 'F'), iif(0.5, 'T', 'F'), iif(-2, 'T', 'F')",
    },
    QueryCase {
        name: "iif text numeric truthiness",
        sql: "SELECT iif('1', 'T', 'F'), iif('0', 'T', 'F'), iif('abc', 'T', 'F'), iif('  5  ', 'T', 'F'), iif('-0.25', 'T', 'F')",
    },
    QueryCase {
        name: "column driven conditional scalars",
        sql: "SELECT id, coalesce(label, fallback, 'none'), ifnull(a, -99), nullif(label, fallback), iif(a, 'nonzero', 'zeroish'), iif(b, 'nonzero', 'zeroish') FROM conditional_values ORDER BY id",
    },
];

const BETWEEN_IN_PREDICATE_SETUP: &str = "
    CREATE TABLE predicate_rows (
        id INTEGER PRIMARY KEY,
        n INTEGER,
        lo INTEGER,
        hi INTEGER,
        txt TEXT
    );
    CREATE TABLE predicate_filter (
        value INTEGER
    );

    INSERT INTO predicate_rows VALUES
        (1, 5, 1, 10, 'alpha'),
        (2, 10, 5, 8, 'Beta'),
        (3, 7, NULL, 9, 'delta'),
        (4, NULL, 0, 99, 'omega'),
        (5, 12, 20, 30, NULL);
    INSERT INTO predicate_filter VALUES
        (5),
        (7),
        (NULL);
";

const BETWEEN_IN_PREDICATE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "between column bounds and reversed bounds",
        sql: "SELECT id, n BETWEEN lo AND hi, n NOT BETWEEN lo AND hi, n BETWEEN hi AND lo FROM predicate_rows ORDER BY id",
    },
    QueryCase {
        name: "between expression bounds in where predicate",
        sql: "SELECT id FROM predicate_rows WHERE n BETWEEN COALESCE(lo, n) AND COALESCE(hi, n) ORDER BY id",
    },
    QueryCase {
        name: "empty in list truth table",
        sql: "SELECT id, n IN (), n NOT IN (), NULL IN (), NULL NOT IN () FROM predicate_rows ORDER BY id",
    },
    QueryCase {
        name: "in subquery containing null",
        sql: "SELECT id, n IN (SELECT value FROM predicate_filter), n NOT IN (SELECT value FROM predicate_filter) FROM predicate_rows ORDER BY id",
    },
    QueryCase {
        name: "in values subquery",
        sql: "SELECT id FROM predicate_rows WHERE n IN (VALUES (5), (7), (NULL)) ORDER BY id",
    },
    QueryCase {
        name: "in expression list",
        sql: "SELECT id FROM predicate_rows WHERE n IN (lo, hi, lo + hi) ORDER BY id",
    },
    QueryCase {
        name: "text between binary and nocase collation",
        sql: "SELECT id, txt BETWEEN 'alpha' AND 'delta', txt COLLATE NOCASE BETWEEN 'alpha' AND 'delta' FROM predicate_rows ORDER BY id",
    },
];

const BOOLEAN_LOGIC_PRECEDENCE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "literal three valued truth table",
        sql: "SELECT NULL AND 0, NULL AND 1, NULL OR 0, NULL OR 1, NOT NULL, NOT 0, NOT 5",
    },
    QueryCase {
        name: "null predicate conjunctions",
        sql: "SELECT id, a IS NOT NULL AND b IS NOT NULL, a IS NULL OR b IS NULL, NOT (a IS NULL OR b IS NULL) FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "not comparison precedence",
        sql: "SELECT id, NOT a = b, NOT (a = b), (NOT a) = b FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "grouped where predicate",
        sql: "SELECT id FROM expr_rows WHERE NOT (a > 3 AND b > 3) OR (label IS NULL AND a < b) ORDER BY id",
    },
    QueryCase {
        name: "mixed comparison boolean projections",
        sql: "SELECT id, (a > 3) AND (label = 'high'), (a < b) OR (label IS NULL), (a = b) OR NULL FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "truthy columns in case and iif",
        sql: "SELECT id, CASE WHEN a AND b THEN 'both' WHEN a OR b THEN 'either' ELSE 'neither' END, IIF(a > b, 'gt', 'not-gt') FROM expr_rows ORDER BY id",
    },
];

const NUMERIC_COERCION_CASES: &[QueryCase] = &[
    QueryCase {
        name: "integer arithmetic with null propagation",
        sql: "SELECT id, a + b, a - b, a * b, a / NULLIF(b, 0), a % NULLIF(b, 0) FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "text numeric coercion literals",
        sql: "SELECT '12' + 3, '12.5' * 2, 'abc' + 4, typeof('12' + 3), typeof('12.5' * 2)",
    },
    QueryCase {
        name: "unary plus minus expressions",
        sql: "SELECT id, +a, -a, -(a + COALESCE(b, 0)), typeof(+a) FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "concatenation coerces numbers and propagates null",
        sql: "SELECT id, a || ':' || IFNULL(label, 'none'), label || a, a || NULL FROM expr_rows ORDER BY id",
    },
    QueryCase {
        name: "arithmetic expression in where predicate",
        sql: "SELECT id FROM expr_rows WHERE (a + COALESCE(b, 0)) >= 10 ORDER BY id",
    },
    QueryCase {
        name: "mixed integer real division storage classes",
        sql: "SELECT 5 / 2, 5 / 2.0, 5.0 / 2, 7 % 3, typeof(5 / 2), typeof(5 / 2.0)",
    },
];

const GROUP_BY_EXPRESSION_ALIAS_CASES: &[QueryCase] = &[
    QueryCase {
        name: "group by select list alias",
        sql: "SELECT COALESCE(label, 'none') AS bucket, COUNT(*) AS n, SUM(COALESCE(a, 0)) FROM expr_rows GROUP BY bucket ORDER BY bucket",
    },
    QueryCase {
        name: "group by ordinal expression",
        sql: "SELECT CASE WHEN a IS NULL OR b IS NULL THEN 'partial' WHEN a = b THEN 'same' ELSE 'diff' END AS kind, COUNT(*), MIN(id), MAX(id) FROM expr_rows GROUP BY 1 ORDER BY kind",
    },
    QueryCase {
        name: "group by boolean null flags",
        sql: "SELECT a IS NULL AS missing_a, b IS NULL AS missing_b, COUNT(*) FROM expr_rows GROUP BY missing_a, missing_b ORDER BY missing_a, missing_b",
    },
    QueryCase {
        name: "having aggregate alias over grouped expression",
        sql: "SELECT COALESCE(label, 'none') AS bucket, COUNT(*) AS n FROM expr_rows GROUP BY bucket HAVING n >= 1 ORDER BY n DESC, bucket",
    },
    QueryCase {
        name: "null values share one group",
        sql: "SELECT label, COUNT(*), MIN(id), MAX(id) FROM expr_rows GROUP BY label ORDER BY label IS NOT NULL, label",
    },
];

const ORDER_BY_EXPRESSION_CASES: &[QueryCase] = &[
    QueryCase {
        name: "case expression custom text rank",
        sql: "SELECT id, label FROM expr_rows ORDER BY CASE WHEN label = 'high' THEN 0 WHEN label = 'same' THEN 1 WHEN label IS NULL THEN 3 ELSE 2 END, id",
    },
    QueryCase {
        name: "computed nullable arithmetic sort",
        sql: "SELECT id, a, b FROM expr_rows ORDER BY COALESCE(a, -1000) + COALESCE(b, -1000) DESC, id",
    },
    QueryCase {
        name: "predicate expression ordering",
        sql: "SELECT id, a, b FROM expr_rows ORDER BY a IS NULL, b IS NULL, a DESC, id",
    },
    QueryCase {
        name: "repeated projected expression ordering",
        sql: "SELECT id, COALESCE(label, 'none') AS resolved FROM expr_rows ORDER BY COALESCE(label, 'none') = 'none', COALESCE(label, 'none') DESC, id",
    },
    QueryCase {
        name: "case expression with limit offset",
        sql: "SELECT id, label FROM expr_rows ORDER BY CASE WHEN a IS NULL THEN 2 WHEN a >= b THEN 0 ELSE 1 END, id LIMIT 3 OFFSET 1",
    },
];

const DISTINCT_ORDER_LIMIT_CASES: &[QueryCase] = &[
    QueryCase {
        name: "distinct nullable text ordering",
        sql: "SELECT DISTINCT label FROM expr_rows ORDER BY label",
    },
    QueryCase {
        name: "distinct boolean null flags",
        sql: "SELECT DISTINCT a IS NULL, b IS NULL FROM expr_rows ORDER BY 1, 2",
    },
    QueryCase {
        name: "order by alias and ordinal with limit offset",
        sql: "SELECT id, COALESCE(label, 'none') AS resolved FROM expr_rows ORDER BY 2 DESC, 1 LIMIT 3 OFFSET 1",
    },
    QueryCase {
        name: "order by computed expression",
        sql: "SELECT id, a + COALESCE(b, 0) AS total FROM expr_rows ORDER BY total DESC, id",
    },
    QueryCase {
        name: "null aware order by expression",
        sql: "SELECT id, label FROM expr_rows ORDER BY label IS NULL, label DESC, id LIMIT 4",
    },
    QueryCase {
        name: "distinct case expression with limit",
        sql: "SELECT DISTINCT CASE WHEN a IS NULL THEN 'missing' WHEN a >= b THEN 'ge' ELSE 'lt' END AS bucket FROM expr_rows ORDER BY bucket LIMIT 3",
    },
];

const LIMIT_OFFSET_EDGE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "zero limit returns no rows",
        sql: "SELECT id FROM expr_rows ORDER BY id LIMIT 0",
    },
    QueryCase {
        name: "large offset returns no rows",
        sql: "SELECT id FROM expr_rows ORDER BY id LIMIT 3 OFFSET 99",
    },
    QueryCase {
        name: "negative limit keeps all rows after offset",
        sql: "SELECT id FROM expr_rows ORDER BY id LIMIT -1 OFFSET 2",
    },
    QueryCase {
        name: "computed limit and offset expressions",
        sql: "SELECT id FROM expr_rows ORDER BY id LIMIT 1 + 2 OFFSET COALESCE(NULL, 1)",
    },
    QueryCase {
        name: "comma syntax uses offset then count",
        sql: "SELECT id FROM expr_rows ORDER BY id LIMIT 2, 2",
    },
];

const ORDER_BY_NULLS_CASES: &[QueryCase] = &[
    QueryCase {
        name: "text ascending nulls last",
        sql: "SELECT id, label FROM expr_rows ORDER BY label ASC NULLS LAST, id",
    },
    QueryCase {
        name: "text descending nulls first",
        sql: "SELECT id, label FROM expr_rows ORDER BY label DESC NULLS FIRST, id",
    },
    QueryCase {
        name: "integer ascending nulls first",
        sql: "SELECT id, a FROM expr_rows ORDER BY a ASC NULLS FIRST, id",
    },
    QueryCase {
        name: "integer descending nulls last",
        sql: "SELECT id, b FROM expr_rows ORDER BY b DESC NULLS LAST, id",
    },
    QueryCase {
        name: "alias ordering nulls last",
        sql: "SELECT id, label AS resolved FROM expr_rows ORDER BY resolved ASC NULLS LAST, id",
    },
    QueryCase {
        name: "computed expression nulls first",
        sql: "SELECT id, a + b AS total FROM expr_rows ORDER BY total DESC NULLS FIRST, id",
    },
    QueryCase {
        name: "null placement with limit offset",
        sql: "SELECT id, a FROM expr_rows ORDER BY a ASC NULLS LAST, id LIMIT 3 OFFSET 1",
    },
];

const SUBQUERY_SETUP: &str = "
    CREATE TABLE customers (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        tier TEXT NOT NULL
    );
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        amount INTEGER NOT NULL,
        status TEXT NOT NULL
    );
    CREATE TABLE flags (
        customer_id INTEGER PRIMARY KEY,
        blocked INTEGER NOT NULL
    );

    INSERT INTO customers VALUES
        (1, 'Ada', 'gold'),
        (2, 'Bert', 'silver'),
        (3, 'Cara', 'gold'),
        (4, 'Drew', 'bronze'),
        (5, 'Eli', 'silver');
    INSERT INTO orders VALUES
        (10, 1, 120, 'shipped'),
        (11, 1, 40, 'pending'),
        (12, 2, 75, 'shipped'),
        (13, 3, 200, 'cancelled'),
        (14, 3, 30, 'shipped'),
        (15, 5, 15, 'pending');
    INSERT INTO flags VALUES
        (2, 1),
        (4, 1);
";

const SUBQUERY_CASES: &[QueryCase] = &[
    QueryCase {
        name: "scalar correlated aggregate",
        sql: "SELECT c.name, (SELECT SUM(o.amount) FROM orders o WHERE o.customer_id = c.id) AS total_amount FROM customers c ORDER BY c.id",
    },
    QueryCase {
        name: "in grouped subquery",
        sql: "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders GROUP BY customer_id HAVING SUM(amount) >= 100) ORDER BY name",
    },
    QueryCase {
        name: "correlated exists",
        sql: "SELECT c.name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.status = 'shipped') ORDER BY c.name",
    },
    QueryCase {
        name: "correlated not exists",
        sql: "SELECT c.name FROM customers c WHERE NOT EXISTS (SELECT 1 FROM flags f WHERE f.customer_id = c.id AND f.blocked = 1) ORDER BY c.name",
    },
    QueryCase {
        name: "derived table aggregate",
        sql: "SELECT tier, COUNT(*), SUM(total_amount) FROM (SELECT c.id, c.tier, SUM(o.amount) AS total_amount FROM customers c JOIN orders o ON o.customer_id = c.id GROUP BY c.id, c.tier) totals GROUP BY tier ORDER BY tier",
    },
    QueryCase {
        name: "scalar threshold subquery",
        sql: "SELECT id, amount FROM orders WHERE amount > (SELECT AVG(amount) FROM orders) ORDER BY id",
    },
];

const SCALAR_SUBQUERY_EDGE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "scalar subquery row and empty result",
        sql: "SELECT (SELECT amount FROM orders ORDER BY amount DESC LIMIT 1), (SELECT amount FROM orders WHERE amount > 999)",
    },
    QueryCase {
        name: "correlated scalar count",
        sql: "SELECT c.name, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS order_count FROM customers c ORDER BY c.id",
    },
    QueryCase {
        name: "exists ignores null projection",
        sql: "SELECT name FROM customers c WHERE EXISTS (SELECT NULL FROM orders o WHERE o.customer_id = c.id AND o.status = 'pending') ORDER BY name",
    },
    QueryCase {
        name: "not in subquery without nulls",
        sql: "SELECT name FROM customers WHERE id NOT IN (SELECT customer_id FROM orders WHERE status = 'cancelled') ORDER BY name",
    },
    QueryCase {
        name: "scalar subquery in predicate",
        sql: "SELECT name FROM customers WHERE tier = (SELECT tier FROM customers WHERE name = 'Ada') ORDER BY name",
    },
];

const PRAGMA_SETUP: &str = "
    PRAGMA foreign_keys = ON;
    PRAGMA user_version = 123;
    PRAGMA application_id = 456789;

    CREATE TABLE pragma_parent (
        id INTEGER PRIMARY KEY,
        code TEXT NOT NULL UNIQUE
    );
    CREATE TABLE pragma_child (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER NOT NULL REFERENCES pragma_parent(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        score INTEGER DEFAULT 0,
        note TEXT DEFAULT 'new'
    );
    CREATE UNIQUE INDEX idx_pragma_child_parent_score_name
        ON pragma_child(parent_id, score, name);
";

const PRAGMA_CASES: &[QueryCase] = &[
    QueryCase {
        name: "table info includes constraints and defaults",
        sql: "PRAGMA table_info(pragma_child)",
    },
    QueryCase {
        name: "index list includes uniqueness and origin",
        sql: "PRAGMA index_list(pragma_child)",
    },
    QueryCase {
        name: "index info preserves indexed column order",
        sql: "PRAGMA index_info(idx_pragma_child_parent_score_name)",
    },
    QueryCase {
        name: "foreign key list exposes referenced table and action",
        sql: "PRAGMA foreign_key_list(pragma_child)",
    },
    QueryCase {
        name: "user version round trip",
        sql: "PRAGMA user_version",
    },
    QueryCase {
        name: "application id round trip",
        sql: "PRAGMA application_id",
    },
    QueryCase {
        name: "foreign keys setting round trip",
        sql: "PRAGMA foreign_keys",
    },
];

const TRIGGER_SETUP: &str = "
    CREATE TABLE trigger_items (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        qty INTEGER NOT NULL,
        active INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE trigger_audit (
        seq INTEGER PRIMARY KEY,
        action TEXT NOT NULL,
        item_id INTEGER NOT NULL,
        old_name TEXT,
        new_name TEXT,
        old_qty INTEGER,
        new_qty INTEGER
    );

    CREATE TRIGGER trigger_items_ai
        AFTER INSERT ON trigger_items
        BEGIN
            INSERT INTO trigger_audit(action, item_id, old_name, new_name, old_qty, new_qty)
            VALUES ('insert', NEW.id, NULL, NEW.name, NULL, NEW.qty);
        END;
    CREATE TRIGGER trigger_items_au
        AFTER UPDATE ON trigger_items
        BEGIN
            INSERT INTO trigger_audit(action, item_id, old_name, new_name, old_qty, new_qty)
            VALUES ('update', NEW.id, OLD.name, NEW.name, OLD.qty, NEW.qty);
        END;
    CREATE TRIGGER trigger_items_ad
        AFTER DELETE ON trigger_items
        BEGIN
            INSERT INTO trigger_audit(action, item_id, old_name, new_name, old_qty, new_qty)
            VALUES ('delete', OLD.id, OLD.name, NULL, OLD.qty, NULL);
        END;
";

const TRIGGER_SCRIPT: &str = "
    INSERT INTO trigger_items(id, name, qty) VALUES (1, 'bolt', 10);
    INSERT INTO trigger_items(id, name, qty) VALUES (2, 'nut', 20);
    UPDATE trigger_items
        SET name = 'bolt-plus', qty = qty + 5
        WHERE id = 1;
    UPDATE trigger_items
        SET active = 0
        WHERE id = 2;
    DELETE FROM trigger_items
        WHERE active = 0;
";

const TRIGGER_CASES: &[QueryCase] = &[
    QueryCase {
        name: "final trigger table rows",
        sql: "SELECT id, name, qty, active FROM trigger_items ORDER BY id",
    },
    QueryCase {
        name: "trigger audit old new values",
        sql: "SELECT action, item_id, old_name, new_name, old_qty, new_qty FROM trigger_audit ORDER BY seq",
    },
    QueryCase {
        name: "trigger action aggregate",
        sql: "SELECT action, COUNT(*) FROM trigger_audit GROUP BY action ORDER BY action",
    },
    QueryCase {
        name: "trigger schema registration",
        sql: "SELECT type, name, tbl_name FROM sqlite_schema WHERE type = 'trigger' ORDER BY name",
    },
];

const DATE_TIME_CASES: &[QueryCase] = &[
    QueryCase {
        name: "basic date time datetime projections",
        sql: "SELECT date('2024-01-15'), time('2024-01-15 13:45:30'), datetime('2024-01-15 13:45:30')",
    },
    QueryCase {
        name: "calendar boundary modifiers",
        sql: "SELECT date('2024-01-31', '+1 month'), date('2024-02-29', '+1 year'), date('2024-03-01', '-1 day')",
    },
    QueryCase {
        name: "time arithmetic modifiers",
        sql: "SELECT datetime('2024-01-15 12:00:00', '+90 minutes'), time('23:30:00', '+2 hours')",
    },
    QueryCase {
        name: "strftime calendar fields",
        sql: "SELECT strftime('%Y-%m-%d %H:%M:%S', '2024-02-29 23:59:01'), strftime('%j', '2024-12-31')",
    },
    QueryCase {
        name: "unixepoch and julianday storage class",
        sql: "SELECT unixepoch('1970-01-02 00:00:00'), typeof(julianday('2024-01-15'))",
    },
    QueryCase {
        name: "invalid and null date inputs",
        sql: "SELECT date(NULL), time('not-a-date'), datetime('2024-01-15', 'bogus')",
    },
];

const DATE_TIME_EDGE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "start modifiers preserve SQLite modifier order",
        sql: "SELECT date('2024-03-15', 'start of month', '+1 day'), date('2024-03-15', '+1 day', 'start of month')",
    },
    QueryCase {
        name: "weekday modifier advances or noops",
        sql: "SELECT date('2024-03-15', 'weekday 0'), date('2024-03-17', 'weekday 0')",
    },
    QueryCase {
        name: "numeric input modifiers",
        sql: "SELECT datetime(0, 'unixepoch'), datetime(1710531045, 'auto'), date(2460384.5, 'auto')",
    },
    QueryCase {
        name: "iso separator and bare time inputs",
        sql: "SELECT datetime('2024-03-15T14:30:00'), date('12:30:00'), time('2024-03-15T14:30:45')",
    },
    QueryCase {
        name: "timezone offset normalization",
        sql: "SELECT datetime('2026-04-07T16:00:00Z'), datetime('2026-04-07T16:00:00+01:00'), datetime('2026-04-07T16:00:00-05:30')",
    },
    QueryCase {
        name: "strftime epoch and weekday fields",
        sql: "SELECT strftime('%s', '1970-01-02 00:00:00'), strftime('%w', '2024-03-17'), strftime('%W', '2024-03-18')",
    },
];

const JSON1_CASES: &[QueryCase] = &[
    QueryCase {
        name: "json validation and canonicalization",
        sql: r#"SELECT json('[1,2,3]'), json_valid('{"a":1}'), json_valid('[1,2,]')"#,
    },
    QueryCase {
        name: "json extract scalar and multi path",
        sql: r#"SELECT json_extract('{"a":[10,20,30],"b":{"c":"see"}}', '$.a[1]'), json_extract('{"a":1,"b":2}', '$.a', '$.b', '$.missing')"#,
    },
    QueryCase {
        name: "json type and array length",
        sql: r#"SELECT json_type('{"a":[1,null,"x"]}', '$.a'), json_type('{"a":[1,null,"x"]}', '$.a[1]'), json_array_length('{"a":[1,2,3]}', '$.a')"#,
    },
    QueryCase {
        name: "json constructors and quote",
        sql: r#"SELECT json_array(1, 'two', NULL), json_object('a', 1, 'b', json('[2,3]')), json_quote('needs "quotes"')"#,
    },
    QueryCase {
        name: "json mutators",
        sql: r#"SELECT json_set('{"a":[1,2]}', '$.a[#]', 3), json_insert('{"a":1}', '$.a', 99, '$.b', 2), json_replace('{"a":1,"b":2}', '$.a', 7, '$.missing', 8)"#,
    },
    QueryCase {
        name: "json remove and patch",
        sql: r#"SELECT json_remove('{"a":[1,2,3],"b":4}', '$.a[#-2]', '$.b'), json_patch('{"a":{"b":1,"c":2},"d":3}', '{"a":{"b":9,"c":null},"e":4}')"#,
    },
    QueryCase {
        name: "json_each table valued rows",
        sql: r#"SELECT key, value, type FROM json_each('["a",2,null,true]') ORDER BY key"#,
    },
];

const STRING_FUNCTION_CASES: &[QueryCase] = &[
    QueryCase {
        name: "substr positive and omitted length",
        sql: "SELECT substr('hello world', 7), substr('hello world', 7, 5), substr('abcdef', 2, 3)",
    },
    QueryCase {
        name: "substr negative and zero positions",
        sql: "SELECT substr('hello world', -5), substr('hello', 0, 3), substr('hello', -2, 1)",
    },
    QueryCase {
        name: "substr zero and negative lengths",
        sql: "SELECT substr('hello', 1, 0), substr('hello', 1, -1), substr('', 1, 1), substr(NULL, 1, 3)",
    },
    QueryCase {
        name: "replace normal overlapping and empty needle",
        sql: "SELECT replace('hello world', 'world', 'there'), replace('aaaa', 'aa', 'b'), replace('hello', '', 'x')",
    },
    QueryCase {
        name: "replace deletion and null propagation",
        sql: "SELECT replace('banana', 'na', ''), replace(NULL, 'a', 'b'), replace('abc', NULL, 'x')",
    },
    QueryCase {
        name: "instr hits misses and empty needle",
        sql: "SELECT instr('hello world', 'world'), instr('hello world', 'xyz'), instr('hello', ''), instr('', '')",
    },
    QueryCase {
        name: "instr null propagation and case sensitivity",
        sql: "SELECT instr(NULL, 'x'), instr('abc', NULL), instr('Hello', 'he'), instr('Hello', 'He')",
    },
];

const REPLACE_SCALAR_SETUP: &str = "
    CREATE TABLE replace_values (
        id INTEGER PRIMARY KEY,
        raw,
        needle,
        replacement
    );
    INSERT INTO replace_values (id, raw, needle, replacement) VALUES
        (1, 'banana', 'na', ''),
        (2, 'aaaa', 'aa', 'b'),
        (3, 'Case', 'C', 'x'),
        (4, 12121, '12', 'x'),
        (5, '', '', 'z'),
        (6, NULL, 'a', 'b');
";

const REPLACE_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "non recursive overlapping replacements",
        sql: "SELECT replace('aaaa', 'aa', 'b'), replace('aaa', 'a', 'bb'), replace('aaaa', 'a', 'aa')",
    },
    QueryCase {
        name: "empty needle leaves input unchanged",
        sql: "SELECT replace('abc', '', 'x'), replace('', '', 'x'), typeof(replace('abc', '', 'x'))",
    },
    QueryCase {
        name: "deletion and case sensitive matching",
        sql: "SELECT replace('banana', 'na', ''), replace('Case', 'c', 'x'), replace('Case', 'C', 'x')",
    },
    QueryCase {
        name: "numeric coercion and storage class",
        sql: "SELECT replace(12121, '12', 'x'), replace(-12012, '12', 'x'), typeof(replace(12121, '12', 'x'))",
    },
    QueryCase {
        name: "null argument propagation",
        sql: "SELECT replace(NULL, 'a', 'b'), replace('abc', NULL, 'x'), replace('abc', 'a', NULL), typeof(replace(NULL, 'a', 'b'))",
    },
    QueryCase {
        name: "column driven replace",
        sql: "SELECT id, replace(raw, needle, replacement), typeof(replace(raw, needle, replacement)) FROM replace_values ORDER BY id",
    },
];

const SUBSTR_SCALAR_SETUP: &str = "
    CREATE TABLE substr_values (
        id INTEGER PRIMARY KEY,
        txt TEXT,
        payload BLOB,
        start_at INTEGER,
        take_len INTEGER
    );
    INSERT INTO substr_values (id, txt, payload, start_at, take_len) VALUES
        (1, 'abcdef', X'0102030405', 2, 3),
        (2, 'abcdef', X'0102030405', -2, 2),
        (3, 'abcdef', X'0102030405', 3, -2),
        (4, 'eclair', X'CAFEBA', 1, 2),
        (5, NULL, NULL, 1, 2);
";

const SUBSTR_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "negative length and zero position quirks",
        sql: "SELECT substr('abcdef', 3, -2), substr('abcdef', -2, -2), substr('abcdef', 0, 3), substr('abcdef', 2, 0)",
    },
    QueryCase {
        name: "substring alias mirrors substr",
        sql: "SELECT substring('abcdef', 2, 3), substring('abcdef', -2), substring(NULL, 1, 2)",
    },
    QueryCase {
        name: "unicode text slicing",
        sql: "SELECT substr(char(233) || 'clair', 1, 2), substr(char(233) || 'clair', -3, 2), length(substr(char(233) || 'clair', 1, 2))",
    },
    QueryCase {
        name: "blob slicing preserves blob storage",
        sql: "SELECT hex(substr(X'0102030405', 2, 3)), hex(substr(X'0102030405', -2, -2)), typeof(substr(X'0102030405', 2, 3))",
    },
    QueryCase {
        name: "null arguments",
        sql: "SELECT substr(NULL, 1, 2), substr('abcdef', NULL, 2), typeof(substr(NULL, 1, 2)), typeof(substr('abcdef', NULL, 2))",
    },
    QueryCase {
        name: "column driven substr",
        sql: "SELECT id, substr(txt, start_at, take_len), hex(substr(payload, start_at, take_len)), typeof(substr(payload, start_at, take_len)) FROM substr_values ORDER BY id",
    },
];

const STRING_SCALAR_EDGE_CASES: &[QueryCase] = &[
    QueryCase {
        name: "ascii case conversion and null propagation",
        sql: "SELECT upper('MiXeD'), lower('MiXeD'), upper(NULL), lower(NULL)",
    },
    QueryCase {
        name: "length storage class and null behavior",
        sql: "SELECT length('hello'), length(''), length(NULL), typeof(length('abc'))",
    },
    QueryCase {
        name: "default trim family",
        sql: "SELECT trim('  padded  '), ltrim('  padded  '), rtrim('  padded  ')",
    },
    QueryCase {
        name: "custom trim character sets",
        sql: "SELECT trim('xyxhelloxy', 'xy'), ltrim('xyxhello', 'xy'), rtrim('helloxyx', 'xy')",
    },
    QueryCase {
        name: "hex conversion storage class",
        sql: "SELECT hex('Az'), hex(42), typeof(hex(42))",
    },
    QueryCase {
        name: "string functions over table columns",
        sql: "SELECT id, upper(IFNULL(label, 'none')), length(IFNULL(label, '')), trim(label || '  ') FROM expr_rows ORDER BY id",
    },
];

const TRIM_SCALAR_SETUP: &str = "
    CREATE TABLE trim_values (
        id INTEGER PRIMARY KEY,
        raw,
        cut
    );
    INSERT INTO trim_values (id, raw, cut) VALUES
        (1, '  alpha  ', ' '),
        (2, char(9) || 'tab' || char(9), char(9)),
        (3, 'xyxmiddleyxx', 'xy'),
        (4, 12321, '12'),
        (5, '', 'x'),
        (6, NULL, 'x');
";

const TRIM_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "default trim removes spaces only",
        sql: "SELECT trim('  padded  '), ltrim('  padded'), rtrim('padded  '), length(trim(char(9) || 'x' || char(9)))",
    },
    QueryCase {
        name: "custom tab trim character set",
        sql: "SELECT length(trim(char(9) || 'x' || char(9), char(9))), length(ltrim(char(9) || 'x', char(9))), length(rtrim('x' || char(9), char(9)))",
    },
    QueryCase {
        name: "empty and multi-character trim sets",
        sql: "SELECT trim('abc', ''), ltrim('abc', ''), rtrim('abc', ''), trim('xyxabcxy', 'xy')",
    },
    QueryCase {
        name: "numeric coercion and storage class",
        sql: "SELECT trim(12321, '12'), ltrim(12321, '12'), rtrim(12321, '12'), typeof(trim(12321, '12'))",
    },
    QueryCase {
        name: "null argument propagation",
        sql: "SELECT trim(NULL), ltrim(NULL, 'x'), rtrim(NULL, 'x'), typeof(trim(NULL, 'x'))",
    },
    QueryCase {
        name: "column driven trim functions",
        sql: "SELECT id, length(trim(raw)), length(trim(raw, cut)), length(ltrim(raw, cut)), length(rtrim(raw, cut)), typeof(trim(raw, cut)) FROM trim_values ORDER BY id",
    },
];

const LENGTH_INSTR_NUL_SETUP: &str = "
    CREATE TABLE length_instr_values (
        id INTEGER PRIMARY KEY,
        txt TEXT,
        payload BLOB,
        needle_text TEXT,
        needle_blob BLOB
    );
    INSERT INTO length_instr_values (id, txt, payload, needle_text, needle_blob) VALUES
        (1, 'abc', X'616263', 'b', X'62'),
        (2, 'a' || char(0) || 'bc', X'61006263', char(0), X'0062'),
        (3, char(0) || 'abc', X'00616263', 'a', X'00'),
        (4, '', X'', '', X''),
        (5, NULL, NULL, NULL, NULL);
";

const LENGTH_INSTR_NUL_CASES: &[QueryCase] = &[
    QueryCase {
        name: "embedded nul text length and instr",
        sql: "SELECT length('a' || char(0) || 'bc'), octet_length('a' || char(0) || 'bc'), instr('a' || char(0) || 'bc', 'bc'), instr('a' || char(0) || 'bc', char(0))",
    },
    QueryCase {
        name: "leading nul text length and instr",
        sql: "SELECT length(char(0) || 'abc'), octet_length(char(0) || 'abc'), instr(char(0) || 'abc', 'a'), instr(char(0) || 'abc', char(0))",
    },
    QueryCase {
        name: "blob length and byte instr",
        sql: "SELECT length(X'61006263'), octet_length(X'61006263'), instr(X'61006263', X'0062'), instr(X'61006263', X'63'), instr(X'61006263', X'7A'), instr(X'61006263', X''), instr(X'', X'')",
    },
    QueryCase {
        name: "numeric and null length instr",
        sql: "SELECT length(12345), octet_length(12345), length(NULL), octet_length(NULL), instr(NULL, 'a'), instr('abc', NULL)",
    },
    QueryCase {
        name: "column driven nul and blob length instr",
        sql: "SELECT id, length(txt), octet_length(txt), instr(txt, needle_text), length(payload), octet_length(payload), instr(payload, needle_blob) FROM length_instr_values ORDER BY id",
    },
];

const CONCAT_SCALAR_SETUP: &str = "
    CREATE TABLE concat_scalar_values (
        id INTEGER PRIMARY KEY,
        prefix TEXT,
        suffix TEXT,
        n INTEGER,
        amount REAL
    );
    INSERT INTO concat_scalar_values (id, prefix, suffix, n, amount) VALUES
        (1, 'alpha', 'one', 10, 1.5),
        (2, '', 'empty-prefix', -3, -2.25),
        (3, NULL, 'missing-prefix', NULL, NULL),
        (4, 'space ', NULL, 0, 0.0);
";

const CONCAT_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "concat treats null as empty text",
        sql: "SELECT concat('a', NULL, 'b'), concat(NULL, NULL), typeof(concat(NULL, NULL)), concat('', NULL, '')",
    },
    QueryCase {
        name: "concat coerces numeric arguments",
        sql: "SELECT concat('n=', 42, ',r=', 3.5), concat(-7, ':', 0), typeof(concat(1, 2))",
    },
    QueryCase {
        name: "concat ws skips null values",
        sql: "SELECT concat_ws('-', 'a', NULL, 'b'), concat_ws('-', NULL, NULL), concat_ws('', 'a', NULL, 'b')",
    },
    QueryCase {
        name: "concat ws null separator returns null",
        sql: "SELECT concat_ws(NULL, 'a', 'b'), concat_ws(NULL, NULL), typeof(concat_ws(NULL, 'a'))",
    },
    QueryCase {
        name: "column driven concat",
        sql: "SELECT id, concat(prefix, ':', suffix), concat('n=', n, ', amount=', amount) FROM concat_scalar_values ORDER BY id",
    },
    QueryCase {
        name: "column driven concat ws",
        sql: "SELECT id, concat_ws('|', prefix, suffix, n, amount), concat_ws('', prefix, suffix) FROM concat_scalar_values ORDER BY id",
    },
];

const SOUNDEX_SCALAR_SETUP: &str = "
    CREATE TABLE soundex_values (
        id INTEGER PRIMARY KEY,
        name TEXT,
        code INTEGER
    );
    INSERT INTO soundex_values (id, name, code) VALUES
        (1, 'Robert', 123),
        (2, 'Rupert', 456),
        (3, 'Ashcraft', 789),
        (4, '', 0),
        (5, NULL, NULL),
        (6, '1234', 321);
";

const SOUNDEX_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "canonical soundex examples",
        sql: "SELECT soundex('Robert'), soundex('Rupert'), soundex('Ashcraft'), soundex('Tymczak')",
    },
    QueryCase {
        name: "empty null and non alpha inputs",
        sql: "SELECT soundex(''), soundex(NULL), soundex('1234'), soundex('---')",
    },
    QueryCase {
        name: "duplicate codes and separators",
        sql: "SELECT soundex('Pfister'), soundex('Honeyman'), soundex('Van-Deusen'), soundex('van deusen')",
    },
    QueryCase {
        name: "numeric coercion",
        sql: "SELECT soundex(123), soundex(3.14), soundex(-7)",
    },
    QueryCase {
        name: "column driven soundex",
        sql: "SELECT id, soundex(name), soundex(code) FROM soundex_values ORDER BY id",
    },
];

const CHAR_UNICODE_SETUP: &str = "
    CREATE TABLE char_unicode_values (
        id INTEGER PRIMARY KEY,
        label TEXT,
        code INTEGER
    );
    INSERT INTO char_unicode_values (id, label, code) VALUES
        (1, 'Alpha', 65),
        (2, ' space', 32),
        (3, '', 90),
        (4, NULL, 48);
";

const CHAR_UNICODE_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "unicode ascii empty and null inputs",
        sql: "SELECT unicode('A'), unicode(' '), unicode('Alpha'), unicode(''), unicode(NULL)",
    },
    QueryCase {
        name: "char single and multi codepoints",
        sql: "SELECT char(65), char(65, 66, 67), char(32), length(char(65, 66, 67)), hex(char(65, 66, 67))",
    },
    QueryCase {
        name: "unicode char round trips",
        sql: "SELECT unicode(char(90)), char(unicode('Q')), hex(char(unicode('A'), unicode('B')))",
    },
    QueryCase {
        name: "column driven unicode and char",
        sql: "SELECT id, unicode(label), char(code), hex(char(code)) FROM char_unicode_values ORDER BY id",
    },
    QueryCase {
        name: "unicode expression feeds char",
        sql: "SELECT id, char(unicode(substr(label, 1, 1)) + 1) FROM char_unicode_values WHERE label IS NOT NULL AND label <> '' ORDER BY id",
    },
];

const SCALAR_MIN_MAX_SETUP: &str = "
    CREATE TABLE scalar_extrema (
        id INTEGER PRIMARY KEY,
        a INTEGER,
        b REAL,
        label TEXT,
        payload BLOB
    );
    INSERT INTO scalar_extrema (id, a, b, label, payload) VALUES
        (1, 10, 5.5, 'alpha', X'01'),
        (2, -3, 7.25, 'Beta', X'02FF'),
        (3, NULL, 4.0, NULL, NULL),
        (4, 8, NULL, 'gamma', X'');
";

const SCALAR_MIN_MAX_CASES: &[QueryCase] = &[
    QueryCase {
        name: "literal scalar extrema",
        sql: "SELECT min(3, 1, 4, 1, 5), max(3, 1, 4, 1, 5), typeof(min(3, 1)), typeof(max(3, 1))",
    },
    QueryCase {
        name: "null argument propagation",
        sql: "SELECT min(NULL, 1, 2), max(1, NULL, 2), min(NULL, NULL), max(NULL, NULL)",
    },
    QueryCase {
        name: "mixed integer real storage class",
        sql: "SELECT min(10, 2.5), max(10, 2.5), typeof(min(10, 2.5)), typeof(max(10, 2.5))",
    },
    QueryCase {
        name: "mixed numeric text storage class",
        sql: "SELECT min(10, '2'), max(10, '2'), typeof(min(10, '2')), typeof(max(10, '2'))",
    },
    QueryCase {
        name: "binary text ordering",
        sql: "SELECT min('Beta', 'alpha', 'gamma'), max('Beta', 'alpha', 'gamma')",
    },
    QueryCase {
        name: "blob storage ordering",
        sql: "SELECT min(X'01', X'0100'), max(X'01', X'0100'), typeof(min(X'01', X'0100')), typeof(max(X'01', X'0100'))",
    },
    QueryCase {
        name: "column driven numeric extrema",
        sql: "SELECT id, min(a, b), max(a, b), min(COALESCE(a, 0), COALESCE(b, 0)) FROM scalar_extrema ORDER BY id",
    },
    QueryCase {
        name: "column driven text and blob extrema",
        sql: "SELECT id, min(label, 'm'), max(label, 'm'), min(payload, X'10'), max(payload, X'10') FROM scalar_extrema ORDER BY id",
    },
];

const MISC_SCALAR_SETUP: &str = "
    CREATE TABLE misc_scalar_values (
        id INTEGER PRIMARY KEY,
        label TEXT,
        n INTEGER,
        payload BLOB
    );
    INSERT INTO misc_scalar_values (id, label, n, payload) VALUES
        (1, 'alpha', 10, X'00FF'),
        (2, '', -2, X''),
        (3, NULL, NULL, NULL),
        (4, 'space ', 0, X'4142');
";

const MISC_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "octet length text and null",
        sql: "SELECT octet_length(''), octet_length('abc'), octet_length(char(233)), length(char(233)), typeof(octet_length('abc')), octet_length(NULL)",
    },
    QueryCase {
        name: "octet length blob and numeric",
        sql: "SELECT octet_length(X''), octet_length(X'00FF'), length(X'00FF'), octet_length(12345), octet_length(-7.5)",
    },
    QueryCase {
        name: "randomblob length and type",
        sql: "SELECT length(randomblob(0)), length(randomblob(-5)), length(randomblob(NULL)), length(randomblob(3)), typeof(randomblob(3))",
    },
    QueryCase {
        name: "likelihood family returns first argument",
        sql: "SELECT likelihood(42, 0.25), likelihood('text', 0.75), likely('yes'), unlikely(NULL), typeof(likely(3.5))",
    },
    QueryCase {
        name: "likelihood family in predicates",
        sql: "SELECT id FROM misc_scalar_values WHERE likely(n >= 0) OR unlikely(label IS NULL) ORDER BY id",
    },
    QueryCase {
        name: "misc scalar functions over table columns",
        sql: "SELECT id, octet_length(label), octet_length(payload), likelihood(label, 0.5), likely(n IS NULL) FROM misc_scalar_values ORDER BY id",
    },
];

const FORMAT_QUOTE_SETUP: &str = "
    CREATE TABLE format_values (
        id INTEGER PRIMARY KEY,
        label TEXT,
        amount INTEGER,
        payload BLOB
    );
    INSERT INTO format_values (id, label, amount, payload) VALUES
        (1, 'alpha', 42, X'CAFE'),
        (2, 'needs ''quote''', -7, X''),
        (3, NULL, NULL, NULL);
";

const FORMAT_QUOTE_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "printf integer padding and bases",
        sql: "SELECT printf('%d', 42), printf('%05d', 42), printf('%x', 255), printf('%o', 8)",
    },
    QueryCase {
        name: "printf text float and escaped percent",
        sql: "SELECT printf('%s:%.2f', 'pi', 3.14159), printf('%10s', 'hi'), printf('%-10s|', 'hi'), printf('%%')",
    },
    QueryCase {
        name: "printf null coercion",
        sql: "SELECT printf('%s', NULL), printf('%d', NULL)",
    },
    QueryCase {
        name: "quote storage classes from table",
        sql: "SELECT id, quote(label), quote(amount), quote(payload), quote(NULL) FROM format_values ORDER BY id",
    },
    QueryCase {
        name: "zeroblob storage and hex",
        sql: "SELECT typeof(zeroblob(0)), length(zeroblob(0)), hex(zeroblob(0)), typeof(zeroblob(4)), length(zeroblob(4)), hex(zeroblob(4))",
    },
    QueryCase {
        name: "format and quote composition",
        sql: "SELECT id, printf('%s=%s', IFNULL(label, 'none'), quote(amount)) FROM format_values ORDER BY id",
    },
];

const UNHEX_SCALAR_SETUP: &str = "
    CREATE TABLE unhex_values (
        id INTEGER PRIMARY KEY,
        raw,
        ignore_chars
    );
    INSERT INTO unhex_values (id, raw, ignore_chars) VALUES
        (1, '4869', NULL),
        (2, '48-69', '-'),
        (3, '4 869', ' '),
        (4, '', '-'),
        (5, NULL, '-'),
        (6, '4142', '4');
";

const UNHEX_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "valid hex to blob storage",
        sql: "SELECT hex(unhex('48656C6C6F')), typeof(unhex('48656C6C6F')), length(unhex('48656C6C6F'))",
    },
    QueryCase {
        name: "mixed case and numeric coercion",
        sql: "SELECT hex(unhex('abCD')), hex(unhex(4142)), typeof(unhex(4142))",
    },
    QueryCase {
        name: "empty and null inputs",
        sql: "SELECT hex(unhex('')), typeof(unhex('')), length(unhex('')), unhex(NULL) IS NULL, typeof(unhex(NULL)), unhex('41', NULL) IS NULL",
    },
    QueryCase {
        name: "invalid and odd length inputs",
        sql: "SELECT unhex('Z1') IS NULL, unhex('A') IS NULL, unhex('123') IS NULL, typeof(unhex('GG'))",
    },
    QueryCase {
        name: "ignored separators between byte pairs",
        sql: "SELECT hex(unhex('48-65-6C', '-')), hex(unhex('48 65 6C', ' ')), hex(unhex('48:65:6C', ':'))",
    },
    QueryCase {
        name: "ignored chars inside byte pair and hex digit ignore argument",
        sql: "SELECT unhex('4 865', ' ') IS NULL, hex(unhex('41', '4'))",
    },
    QueryCase {
        name: "column driven unhex",
        sql: "SELECT id, hex(unhex(raw)), typeof(unhex(raw)), hex(unhex(raw, ignore_chars)), unhex(raw, ignore_chars) IS NULL FROM unhex_values ORDER BY id",
    },
];

const MATH_FUNCTION_CASES: &[QueryCase] = &[
    QueryCase {
        name: "abs nulls and storage class",
        sql: "SELECT abs(-42), abs(42), abs(NULL), typeof(abs(-42))",
    },
    QueryCase {
        name: "sqrt pow and power",
        sql: "SELECT sqrt(144), pow(2, 10), power(3, 4)",
    },
    QueryCase {
        name: "logarithm exact powers",
        sql: "SELECT log(10), log(2, 8), log2(8), log10(1000), ln(1), exp(0)",
    },
    QueryCase {
        name: "rounding family positive and negative",
        sql: "SELECT ceil(1.2), ceiling(-1.2), floor(1.8), floor(-1.2), trunc(1.8), trunc(-1.8)",
    },
    QueryCase {
        name: "domain errors become null",
        sql: "SELECT sqrt(-1), log(-1), ln(0), acos(2), asin(2)",
    },
    QueryCase {
        name: "mod and angle conversion",
        sql: "SELECT mod(10, 3), mod(10, 0), degrees(0), radians(0)",
    },
];

const ROUND_SCALAR_SETUP: &str = "
    CREATE TABLE round_values (
        id INTEGER PRIMARY KEY,
        val REAL,
        places INTEGER
    );
    INSERT INTO round_values (id, val, places) VALUES
        (1, 3.14159, 2),
        (2, 2.5, 0),
        (3, -2.5, 0),
        (4, 12.75, 1),
        (5, NULL, 2);
";

const ROUND_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "default precision and half away from zero ties",
        sql: "SELECT round(3.14159), round(2.5), round(3.5), round(-2.5), round(-3.5)",
    },
    QueryCase {
        name: "explicit precision",
        sql: "SELECT round(3.14159, 2), round(3.14159, 4), round(12.75, 1)",
    },
    QueryCase {
        name: "precision clamping and null precision",
        sql: "SELECT round(1.2345, -2), round(1.2345, 40), round(123.4, NULL)",
    },
    QueryCase {
        name: "text coercion and null input",
        sql: "SELECT round('12.75', 1), round('abc', 1), round(NULL), typeof(round(NULL))",
    },
    QueryCase {
        name: "column driven round",
        sql: "SELECT id, round(val), round(val, places), typeof(round(val, places)) FROM round_values ORDER BY id",
    },
];

const SIGN_SCALAR_SETUP: &str = "
    CREATE TABLE sign_values (
        id INTEGER PRIMARY KEY,
        val,
        txt TEXT
    );
    INSERT INTO sign_values (id, val, txt) VALUES
        (1, -2.5, '-7.2'),
        (2, 0, '0'),
        (3, 3.5, '  5  '),
        (4, NULL, 'abc'),
        (5, -0.0, NULL);
";

const SIGN_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "numeric signs and storage class",
        sql: "SELECT sign(-2.5), sign(0), sign(3.5), sign(-0.0), sign(NULL), typeof(sign(3.5))",
    },
    QueryCase {
        name: "numeric text coercion",
        sql: "SELECT sign('  5  '), sign('-7.2'), sign('0'), sign('1e500'), sign('-1e500')",
    },
    QueryCase {
        name: "non numeric text returns null",
        sql: "SELECT sign('abc'), sign(''), sign('   '), sign('NaN'), sign('inf'), sign('-inf')",
    },
    QueryCase {
        name: "blob and null inputs",
        sql: "SELECT sign(X'0102'), sign(NULL), typeof(sign(X'0102')), typeof(sign(NULL))",
    },
    QueryCase {
        name: "column driven sign",
        sql: "SELECT id, sign(val), sign(txt), typeof(sign(val)), typeof(sign(txt)) FROM sign_values ORDER BY id",
    },
];

const PATTERN_SETUP: &str = "
    CREATE TABLE patterns (
        id INTEGER PRIMARY KEY,
        name TEXT
    );
    INSERT INTO patterns VALUES
        (1, 'hello'),
        (2, 'HELLO'),
        (3, 'he_lo'),
        (4, 'he%llo'),
        (5, 'help'),
        (6, 'shell'),
        (7, '');
";

const PATTERN_CASES: &[QueryCase] = &[
    QueryCase {
        name: "like ascii case insensitive prefix",
        sql: "SELECT id FROM patterns WHERE name LIKE 'he%' ORDER BY id",
    },
    QueryCase {
        name: "like escape literal underscore",
        sql: "SELECT id FROM patterns WHERE name LIKE 'he!_%' ESCAPE '!' ORDER BY id",
    },
    QueryCase {
        name: "like escape literal percent",
        sql: "SELECT id FROM patterns WHERE name LIKE 'he!%%' ESCAPE '!' ORDER BY id",
    },
    QueryCase {
        name: "not like and null predicate",
        sql: "SELECT id FROM patterns WHERE name NOT LIKE 'he%' OR name LIKE NULL ORDER BY id",
    },
    QueryCase {
        name: "glob case sensitive prefix",
        sql: "SELECT id FROM patterns WHERE name GLOB 'he*' ORDER BY id",
    },
    QueryCase {
        name: "glob case sensitive uppercase prefix",
        sql: "SELECT id FROM patterns WHERE name GLOB 'HE*' ORDER BY id",
    },
    QueryCase {
        name: "glob question wildcard",
        sql: "SELECT id FROM patterns WHERE name GLOB 'he?lo' ORDER BY id",
    },
    QueryCase {
        name: "not glob case sensitive",
        sql: "SELECT id FROM patterns WHERE name NOT GLOB 'he*' ORDER BY id",
    },
    QueryCase {
        name: "scalar like glob null behavior",
        sql: "SELECT 'abc' LIKE 'ABC', 'abc' GLOB 'ABC', NULL LIKE 'a%', 'abc' GLOB NULL",
    },
];

const PATTERN_SCALAR_CASES: &[QueryCase] = &[
    QueryCase {
        name: "like scalar ascii case folding",
        sql: "SELECT like('he%', 'hello'), like('HE%', 'hello'), like('he%', 'shell'), like('he_', 'he_')",
    },
    QueryCase {
        name: "like scalar escape handling",
        sql: "SELECT like('he!_%', 'he_lo', '!'), like('he!%%', 'he%llo', '!'), like('he!!%', 'he!lp', '!')",
    },
    QueryCase {
        name: "like scalar null propagation",
        sql: "SELECT like(NULL, 'abc'), like('a%', NULL), like('a%', 'abc', NULL), typeof(like(NULL, 'abc'))",
    },
    QueryCase {
        name: "glob scalar wildcards and ranges",
        sql: "SELECT glob('he*', 'hello'), glob('HE*', 'hello'), glob('he?lo', 'hello'), glob('h[ae]llo', 'hello'), glob('*[0-9]', 'abc5')",
    },
    QueryCase {
        name: "glob scalar null propagation",
        sql: "SELECT glob(NULL, 'abc'), glob('a*', NULL), typeof(glob(NULL, 'abc'))",
    },
    QueryCase {
        name: "column driven scalar pattern functions",
        sql: "SELECT id, like('he%', name), like('he!_%', name, '!'), glob('he*', name), glob('HE*', name) FROM patterns ORDER BY id",
    },
];

const REGEXP_ERROR_CASES: &[StatementCase] = &[
    StatementCase {
        name: "regexp without registered function",
        sql: "SELECT 'abc' REGEXP 'a.*'",
    },
    StatementCase {
        name: "not regexp without registered function",
        sql: "SELECT 'abc' NOT REGEXP 'z.*'",
    },
    StatementCase {
        name: "table regexp without registered function",
        sql: "SELECT id FROM patterns WHERE name REGEXP '^he'",
    },
];

#[test]
fn select_join_group_by_aggregates_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SALES_SETUP);
    harness.assert_queries_match(
        "SELECT/JOIN/GROUP BY/aggregate",
        SELECT_JOIN_GROUP_AGGREGATE_CASES,
    );
}

#[test]
fn left_join_predicate_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SALES_SETUP);
    harness.assert_queries_match("LEFT JOIN predicate edge", LEFT_JOIN_PREDICATE_EDGE_CASES);
}

#[test]
fn join_alias_and_self_join_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SALES_SETUP);
    harness.assert_queries_match("JOIN alias/self edge", JOIN_ALIAS_SELF_EDGE_CASES);
}

#[test]
fn join_using_and_natural_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(JOIN_USING_NATURAL_SETUP);
    harness.assert_queries_match("JOIN USING/NATURAL edge", JOIN_USING_NATURAL_CASES);
}

#[test]
fn aggregate_edge_cases_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SALES_SETUP);
    harness.assert_queries_match("aggregate edge", AGGREGATE_EDGE_CASES);
}

#[test]
fn group_concat_separator_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(GROUP_CONCAT_SETUP);
    harness.assert_queries_match("group_concat separator edge", GROUP_CONCAT_SEPARATOR_CASES);
}

#[test]
fn having_and_aggregate_ordering_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SALES_SETUP);
    harness.assert_queries_match(
        "HAVING/aggregate ordering edge",
        HAVING_AGGREGATE_ORDER_CASES,
    );
}

#[test]
fn upsert_conflict_handling_matches_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(UPSERT_SETUP);
    harness.execute_script(UPSERT_SCRIPT);
    harness.assert_queries_match("UPSERT", UPSERT_CASES);
}

#[test]
fn with_upsert_returning_matches_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(WITH_UPSERT_RETURNING_SETUP);
    harness.assert_queries_match("WITH/UPSERT/RETURNING", WITH_UPSERT_RETURNING_CASES);
}

#[test]
fn conflict_resolution_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CONFLICT_RESOLUTION_SETUP);
    harness.execute_script(CONFLICT_RESOLUTION_SCRIPT);
    harness.assert_queries_match("conflict resolution edge", CONFLICT_RESOLUTION_CASES);
}

#[test]
fn cte_queries_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CTE_SETUP);
    harness.assert_queries_match("CTE", CTE_CASES);
}

#[test]
fn values_clause_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SUBQUERY_SETUP);
    harness.assert_queries_match("VALUES clause edge", VALUES_CLAUSE_CASES);
}

#[test]
fn window_functions_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(WINDOW_SETUP);
    harness.assert_queries_match("window", WINDOW_CASES);
}

#[test]
fn cast_and_collation_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CAST_COLLATION_SETUP);
    harness.assert_queries_match("CAST/collation", CAST_COLLATION_CASES);
}

#[test]
fn collation_expression_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CAST_COLLATION_SETUP);
    harness.assert_queries_match("collation expression", COLLATION_EXPRESSION_CASES);
}

#[test]
fn dml_insert_update_delete_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(DML_SETUP);
    harness.execute_script(DML_SCRIPT);
    harness.assert_queries_match("DML", DML_CASES);
}

#[test]
fn change_tracking_function_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CHANGE_TRACKING_SETUP);
    harness.assert_queries_match(
        "change tracking initial state",
        &[QueryCase {
            name: "initial state",
            sql: CHANGE_TRACKING_STATE_QUERY,
        }],
    );

    for (name, script) in [
        (
            "auto rowid insert",
            "INSERT INTO change_tracking(label) VALUES ('alpha')",
        ),
        (
            "explicit rowid insert",
            "INSERT INTO change_tracking(id, label) VALUES (10, 'beta')",
        ),
        (
            "multi row update",
            "UPDATE change_tracking SET label = label || '-x' WHERE id IN (1, 10)",
        ),
        (
            "no-op update",
            "UPDATE change_tracking SET label = 'none' WHERE id = 99",
        ),
        ("matched delete", "DELETE FROM change_tracking WHERE id = 1"),
        ("no-op delete", "DELETE FROM change_tracking WHERE id = 99"),
    ] {
        harness.execute_script(script);
        harness.assert_queries_match(
            "change tracking statement state",
            &[QueryCase {
                name,
                sql: CHANGE_TRACKING_STATE_QUERY,
            }],
        );
    }

    let duplicate_insert = "INSERT INTO change_tracking(id, label) VALUES (12, 'beta-x')";
    let franken_error = harness.franken.execute_batch(duplicate_insert).is_err();
    let sqlite_error = harness.sqlite.execute_batch(duplicate_insert).is_err();
    assert!(
        sqlite_error,
        "change tracking conformance expected rusqlite duplicate insert error"
    );
    assert_eq!(
        franken_error, sqlite_error,
        "change tracking conformance failed after duplicate insert ({duplicate_insert})"
    );
    harness.assert_queries_match(
        "change tracking failed statement state",
        &[QueryCase {
            name: "failed duplicate insert preserves counters",
            sql: CHANGE_TRACKING_STATE_QUERY,
        }],
    );
}

#[test]
fn dml_returning_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(DML_RETURNING_SETUP);
    harness.assert_queries_match("DML RETURNING edge", DML_RETURNING_CASES);
}

#[test]
fn attached_update_delegation_matches_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(ATTACHED_UPDATE_SETUP);
    harness.assert_queries_match("attached UPDATE", ATTACHED_UPDATE_CASES);
}

#[test]
fn attached_insert_select_delegation_matches_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(ATTACHED_INSERT_SELECT_SETUP);
    harness.assert_queries_match("attached INSERT SELECT", ATTACHED_INSERT_SELECT_CASES);
}

#[test]
fn attached_drop_delegation_matches_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(ATTACHED_DROP_SETUP);
    harness.execute_script(ATTACHED_DROP_SCRIPT);
    harness.assert_queries_match("attached DROP", ATTACHED_DROP_CASES);
}

#[test]
fn attached_create_view_delegation_matches_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(ATTACHED_CREATE_VIEW_SETUP);
    harness.assert_queries_match("attached CREATE VIEW", ATTACHED_CREATE_VIEW_CASES);
}

#[test]
fn attached_vacuum_delegation_matches_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(ATTACHED_VACUUM_SETUP);
    harness.assert_queries_match("attached VACUUM", ATTACHED_VACUUM_CASES);
}

#[test]
fn error_paths_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(ERROR_PATH_SETUP);
    harness.assert_statement_errors_match("error path", ERROR_PATH_CASES);
}

#[test]
fn check_constraint_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CHECK_CONSTRAINT_SETUP);
    harness.execute_script(CHECK_CONSTRAINT_SCRIPT);
    harness.assert_queries_match("CHECK constraint edge", CHECK_CONSTRAINT_CASES);
    harness.assert_statement_errors_match("CHECK constraint edge", CHECK_CONSTRAINT_ERROR_CASES);
}

#[test]
fn foreign_key_action_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(FOREIGN_KEY_ACTION_SETUP);
    harness.execute_script(FOREIGN_KEY_ACTION_SCRIPT);
    harness.assert_queries_match("FOREIGN KEY action edge", FOREIGN_KEY_ACTION_CASES);
    harness
        .assert_statement_errors_match("FOREIGN KEY action edge", FOREIGN_KEY_ACTION_ERROR_CASES);
}

#[test]
fn ddl_defaults_and_views_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(DDL_SETUP);
    harness.assert_queries_match("DDL/default/view", DDL_CASES);
}

#[test]
fn create_table_as_select_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CTAS_SETUP);
    harness.execute_script(CTAS_SCRIPT);
    harness.assert_queries_match("CREATE TABLE AS SELECT edge", CTAS_CASES);
}

#[test]
fn default_values_and_column_defaults_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(DEFAULT_VALUE_SETUP);
    harness.execute_script(DEFAULT_VALUE_SCRIPT);
    harness.assert_queries_match("DEFAULT VALUES/default column", DEFAULT_VALUE_CASES);
}

#[test]
fn rowid_and_quoted_identifier_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(ROWID_IDENTIFIER_SETUP);
    harness.assert_queries_match("rowid/quoted identifier edge", ROWID_IDENTIFIER_CASES);
}

#[test]
fn transactions_and_savepoints_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(TRANSACTION_SETUP);
    harness.execute_script(TRANSACTION_SCRIPT);
    harness.assert_queries_match("transaction/savepoint", TRANSACTION_CASES);
}

#[test]
fn compound_selects_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(COMPOUND_SETUP);
    harness.assert_queries_match("compound SELECT", COMPOUND_CASES);
}

#[test]
fn compound_select_edge_cases_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(COMPOUND_SETUP);
    harness.assert_queries_match("compound SELECT edge", COMPOUND_EDGE_CASES);
}

#[test]
fn case_and_null_logic_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CASE_NULL_SETUP);
    harness.assert_queries_match("CASE/null logic", CASE_NULL_CASES);
}

#[test]
fn scalar_null_comparison_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CASE_NULL_SETUP);
    harness.assert_queries_match("scalar NULL/comparison edge", SCALAR_NULL_COMPARISON_CASES);
}

#[test]
fn is_distinct_from_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CASE_NULL_SETUP);
    harness.assert_queries_match("IS DISTINCT FROM edge", IS_DISTINCT_FROM_CASES);
}

#[test]
fn typeof_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(TYPEOF_SCALAR_SETUP);
    harness.assert_queries_match("typeof scalar edge", TYPEOF_SCALAR_CASES);
}

#[test]
fn conditional_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CONDITIONAL_SCALAR_SETUP);
    harness.assert_queries_match("conditional scalar edge", CONDITIONAL_SCALAR_CASES);
}

#[test]
fn between_and_in_predicate_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(BETWEEN_IN_PREDICATE_SETUP);
    harness.assert_queries_match("BETWEEN/IN predicate edge", BETWEEN_IN_PREDICATE_CASES);
}

#[test]
fn boolean_logic_precedence_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CASE_NULL_SETUP);
    harness.assert_queries_match(
        "boolean logic precedence edge",
        BOOLEAN_LOGIC_PRECEDENCE_CASES,
    );
}

#[test]
fn numeric_coercion_expression_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CASE_NULL_SETUP);
    harness.assert_queries_match("numeric coercion edge", NUMERIC_COERCION_CASES);
}

#[test]
fn group_by_expression_and_alias_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CASE_NULL_SETUP);
    harness.assert_queries_match(
        "GROUP BY expression/alias edge",
        GROUP_BY_EXPRESSION_ALIAS_CASES,
    );
}

#[test]
fn order_by_expression_and_case_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CASE_NULL_SETUP);
    harness.assert_queries_match("ORDER BY expression/CASE edge", ORDER_BY_EXPRESSION_CASES);
}

#[test]
fn distinct_order_limit_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CASE_NULL_SETUP);
    harness.assert_queries_match("DISTINCT/ORDER/LIMIT edge", DISTINCT_ORDER_LIMIT_CASES);
}

#[test]
fn limit_offset_expression_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CASE_NULL_SETUP);
    harness.assert_queries_match("LIMIT/OFFSET expression edge", LIMIT_OFFSET_EDGE_CASES);
}

#[test]
fn order_by_nulls_placement_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CASE_NULL_SETUP);
    harness.assert_queries_match("ORDER BY NULLS placement edge", ORDER_BY_NULLS_CASES);
}

#[test]
fn subqueries_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SUBQUERY_SETUP);
    harness.assert_queries_match("subquery", SUBQUERY_CASES);
}

#[test]
fn scalar_subquery_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SUBQUERY_SETUP);
    harness.assert_queries_match("scalar subquery edge", SCALAR_SUBQUERY_EDGE_CASES);
}

#[test]
fn pragmas_and_schema_introspection_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(PRAGMA_SETUP);
    harness.assert_queries_match("PRAGMA/schema introspection", PRAGMA_CASES);
}

#[test]
fn triggers_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(TRIGGER_SETUP);
    harness.execute_script(TRIGGER_SCRIPT);
    harness.assert_queries_match("trigger", TRIGGER_CASES);
}

#[test]
fn date_time_functions_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new("");
    harness.assert_queries_match("date/time", DATE_TIME_CASES);
}

#[test]
fn date_time_modifier_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new("");
    harness.assert_queries_match("date/time modifier edge", DATE_TIME_EDGE_CASES);
}

#[test]
fn json1_functions_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new("");
    harness.assert_queries_match("JSON1", JSON1_CASES);
}

#[test]
fn string_functions_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new("");
    harness.assert_queries_match("string functions", STRING_FUNCTION_CASES);
}

#[test]
fn replace_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(REPLACE_SCALAR_SETUP);
    harness.assert_queries_match("replace scalar edge", REPLACE_SCALAR_CASES);
}

#[test]
fn substr_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SUBSTR_SCALAR_SETUP);
    harness.assert_queries_match("substr scalar edge", SUBSTR_SCALAR_CASES);
}

#[test]
fn string_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CASE_NULL_SETUP);
    harness.assert_queries_match("string scalar edge", STRING_SCALAR_EDGE_CASES);
}

#[test]
fn trim_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(TRIM_SCALAR_SETUP);
    harness.assert_queries_match("trim scalar edge", TRIM_SCALAR_CASES);
}

#[test]
fn length_instr_nul_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(LENGTH_INSTR_NUL_SETUP);
    harness.assert_queries_match("length/instr NUL edge", LENGTH_INSTR_NUL_CASES);
}

#[test]
fn concat_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CONCAT_SCALAR_SETUP);
    harness.assert_queries_match("concat scalar edge", CONCAT_SCALAR_CASES);
}

#[test]
fn soundex_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SOUNDEX_SCALAR_SETUP);
    harness.assert_queries_match("soundex scalar edge", SOUNDEX_SCALAR_CASES);
}

#[test]
fn char_unicode_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(CHAR_UNICODE_SETUP);
    harness.assert_queries_match("char unicode scalar edge", CHAR_UNICODE_SCALAR_CASES);
}

#[test]
fn scalar_min_max_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SCALAR_MIN_MAX_SETUP);
    harness.assert_queries_match("scalar min max edge", SCALAR_MIN_MAX_CASES);
}

#[test]
fn misc_scalar_function_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(MISC_SCALAR_SETUP);
    harness.assert_queries_match("misc scalar function edge", MISC_SCALAR_CASES);
}

#[test]
fn format_quote_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(FORMAT_QUOTE_SETUP);
    harness.assert_queries_match("format quote scalar edge", FORMAT_QUOTE_SCALAR_CASES);
}

#[test]
fn unhex_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(UNHEX_SCALAR_SETUP);
    harness.assert_queries_match("unhex scalar edge", UNHEX_SCALAR_CASES);
}

#[test]
fn math_functions_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new("");
    harness.assert_queries_match("math functions", MATH_FUNCTION_CASES);
}

#[test]
fn round_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(ROUND_SCALAR_SETUP);
    harness.assert_queries_match("round scalar edge", ROUND_SCALAR_CASES);
}

#[test]
fn sign_scalar_edges_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(SIGN_SCALAR_SETUP);
    harness.assert_queries_match("sign scalar edge", SIGN_SCALAR_CASES);
}

#[test]
fn pattern_matching_functions_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(PATTERN_SETUP);
    harness.assert_queries_match("LIKE/GLOB", PATTERN_CASES);
    harness.assert_statement_errors_match("REGEXP", REGEXP_ERROR_CASES);
}

#[test]
fn pattern_scalar_functions_match_rusqlite() {
    let harness = CoreSqlConformanceHarness::new(PATTERN_SETUP);
    harness.assert_queries_match("LIKE/GLOB scalar function", PATTERN_SCALAR_CASES);
}
