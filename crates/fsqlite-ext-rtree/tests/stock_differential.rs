//! Differential test against stock SQLite's R-tree (rusqlite's bundled SQLite
//! is built with `SQLITE_ENABLE_RTREE`). Both sides run the same seeded
//! inserts, updates and deletes. At every checkpoint the `_node`, `_rowid`
//! and `_parent` shadow tables must hold the same rows, node blobs byte for
//! byte, and queries must return the same rows in the same order. Halfway
//! through, our table is reloaded from stock's shadow rows and carries on.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use fsqlite_ext_rtree::{
    RtreeShadowWrites, RtreeVirtualTable, rtree_i32_module_factory, rtree_module_factory,
};
use fsqlite_func::vtab::{
    ColumnContext, ErasedVtabInstance, VirtualTable, VirtualTableCursor, VtabModuleFactory,
};
use fsqlite_types::SqliteValue;
use fsqlite_types::cx::Cx;
use rusqlite::types::Value;

/// `xBestIndex` strategies, as stock numbers them.
const LOOKUP_ROWID: i32 = 1;
const SCAN_TREE: i32 = 2;

/// One shadow table: rowid to the record after its INTEGER PRIMARY KEY.
type Rows = BTreeMap<i64, Vec<SqliteValue>>;

/// xorshift64*: deterministic and dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn one_in(&mut self, n: u64) -> bool {
        self.below(n) == 0
    }
}

#[derive(Clone, Copy, Debug)]
struct Config {
    int32: bool,
    dimensions: usize,
    aux: usize,
    page_size: usize,
    operations: usize,
    seed: u64,
}

impl Config {
    const fn module(&self) -> &'static str {
        if self.int32 { "rtree_i32" } else { "rtree" }
    }

    /// Column names as `SET` and `SELECT` see them.
    fn column_names(&self) -> Vec<String> {
        let mut names = vec!["id".to_owned()];
        names.extend((0..self.dimensions * 2).map(|coord| format!("c{coord}")));
        names.extend((0..self.aux).map(|aux| format!("tag{aux}")));
        names
    }

    /// Module arguments, with auxiliary columns marked `+`.
    fn module_args(&self) -> Vec<String> {
        let coords = 1 + self.dimensions * 2;
        self.column_names()
            .into_iter()
            .enumerate()
            .map(|(index, name)| {
                if index >= coords {
                    format!("+{name}")
                } else {
                    name
                }
            })
            .collect()
    }
}

fn to_ours(value: &Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Integer(integer) => SqliteValue::Integer(*integer),
        Value::Real(real) => SqliteValue::Float(*real),
        Value::Text(text) => SqliteValue::Text(text.as_str().into()),
        Value::Blob(blob) => SqliteValue::Blob(Arc::from(blob.as_slice())),
    }
}

fn rtree(ours: &dyn ErasedVtabInstance) -> &RtreeVirtualTable {
    ours.as_any()
        .downcast_ref()
        .expect("instance is an RtreeVirtualTable")
}

fn rtree_mut(ours: &mut dyn ErasedVtabInstance) -> &mut RtreeVirtualTable {
    ours.as_any_mut()
        .downcast_mut()
        .expect("instance is an RtreeVirtualTable")
}

struct Pair {
    config: Config,
    cx: Cx,
    stock: rusqlite::Connection,
    ours: Box<dyn ErasedVtabInstance>,
    /// Our `_node`, `_rowid` and `_parent` rows as the drained writes left them.
    shadow: [Rows; 3],
    ids: Vec<i64>,
    live: HashSet<i64>,
    rng: Rng,
}

impl Pair {
    fn new(config: Config) -> Self {
        let stock = rusqlite::Connection::open_in_memory().expect("open stock");
        stock
            .execute_batch(&format!(
                "PRAGMA page_size={}; CREATE VIRTUAL TABLE rt USING {}({});",
                config.page_size,
                config.module(),
                config.module_args().join(", ")
            ))
            .expect("stock CREATE VIRTUAL TABLE");
        let page_size: i64 = stock
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .expect("stock page_size");
        assert_eq!(page_size, config.page_size as i64);

        let cx = Cx::new();
        let ours = Self::create_ours(&cx, config);
        let mut pair = Self {
            config,
            cx,
            stock,
            ours,
            shadow: Default::default(),
            ids: Vec::new(),
            live: HashSet::new(),
            rng: Rng(config.seed),
        };
        rtree_mut(pair.ours.as_mut()).init_shadow_storage(config.page_size);
        pair.drain();

        // xCreate's shadow tables carry stock's exact schema text.
        let mut stock_schema: Vec<(String, String)> = pair
            .stock
            .prepare("SELECT name, sql FROM sqlite_schema WHERE name LIKE 'rt\\_%' ESCAPE '\\'")
            .expect("prepare schema query")
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("query schema")
            .collect::<rusqlite::Result<_>>()
            .expect("read schema");
        let mut our_schema = rtree(pair.ours.as_ref()).shadow_table_definitions();
        stock_schema.sort();
        our_schema.sort();
        assert_eq!(our_schema, stock_schema, "{config:?}: shadow table schema");
        pair
    }

    fn create_ours(cx: &Cx, config: Config) -> Box<dyn ErasedVtabInstance> {
        let mut argv = vec![
            config.module().to_owned(),
            "main".to_owned(),
            "rt".to_owned(),
        ];
        argv.extend(config.module_args());
        let argv: Vec<&str> = argv.iter().map(String::as_str).collect();
        if config.int32 {
            rtree_i32_module_factory().create(cx, &argv)
        } else {
            rtree_module_factory().create(cx, &argv)
        }
        .expect("our xCreate")
    }

    /// Applies the writes our last operation drained, as the engine stores them.
    fn drain(&mut self) {
        let RtreeShadowWrites {
            node,
            rowid,
            parent,
        } = rtree_mut(self.ours.as_mut()).take_shadow_writes();
        for (rows, changes) in self.shadow.iter_mut().zip([node, rowid, parent]) {
            for (key, record) in changes {
                if let Some(record) = record {
                    assert_eq!(
                        record.first(),
                        Some(&SqliteValue::Null),
                        "the INTEGER PRIMARY KEY slot is stored as NULL"
                    );
                    rows.insert(key, record[1..].to_vec());
                } else {
                    rows.remove(&key);
                }
            }
        }
    }

    fn stock_rows(&self, sql: &str) -> Rows {
        let mut statement = self.stock.prepare(sql).expect("prepare shadow read");
        let width = statement.column_count();
        statement
            .query_map([], |row| {
                let key: i64 = row.get(0)?;
                let values = (1..width)
                    .map(|index| row.get::<_, Value>(index).map(|value| to_ours(&value)))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok((key, values))
            })
            .expect("query shadow table")
            .collect::<rusqlite::Result<_>>()
            .expect("read shadow table")
    }

    fn stock_shadow(&self) -> [Rows; 3] {
        let aux: String = (0..self.config.aux)
            .map(|aux| format!(", a{aux}"))
            .collect();
        [
            self.stock_rows("SELECT nodeno, data FROM rt_node ORDER BY nodeno"),
            self.stock_rows(&format!(
                "SELECT rowid, nodeno{aux} FROM rt_rowid ORDER BY rowid"
            )),
            self.stock_rows("SELECT nodeno, parentnode FROM rt_parent ORDER BY nodeno"),
        ]
    }

    fn coordinates(&mut self) -> Vec<Value> {
        let mut values = Vec::with_capacity(self.config.dimensions * 2);
        for _ in 0..self.config.dimensions {
            if self.config.int32 {
                let lo = self.rng.below(200_000) as i64 - 100_000;
                let hi = lo + self.rng.below(4_000) as i64;
                values.extend([Value::Integer(lo), Value::Integer(hi)]);
            } else if self.rng.one_in(20) {
                // Integer coordinates on a float table are converted too.
                let lo = self.rng.below(2_000) as i64 - 1_000;
                values.extend([Value::Integer(lo), Value::Integer(lo + 3)]);
            } else {
                let lo = (self.rng.below(2_000_000) as f64 - 1_000_000.0) / 977.0;
                let hi = lo + self.rng.below(40_000) as f64 / 313.0;
                values.extend([Value::Real(lo), Value::Real(hi)]);
            }
        }
        values
    }

    fn aux_values(&mut self) -> Vec<Value> {
        (0..self.config.aux)
            .map(|_| match self.rng.below(4) {
                0 => Value::Null,
                1 => Value::Integer(self.rng.below(1_000) as i64),
                2 => Value::Text(format!("tag-{}", self.rng.below(1_000))),
                _ => Value::Real(self.rng.below(1_000) as f64 / 8.0),
            })
            .collect()
    }

    fn fresh_id(&mut self) -> i64 {
        loop {
            let id = 1 + self.rng.below(1_000_000) as i64;
            if !self.live.contains(&id) {
                return id;
            }
        }
    }

    fn remember(&mut self, id: i64) {
        assert!(self.live.insert(id), "id {id} inserted twice");
        self.ids.push(id);
    }

    fn insert(&mut self) {
        let id = if self.rng.one_in(10) {
            Value::Null
        } else {
            Value::Integer(self.fresh_id())
        };
        let mut row = vec![id];
        row.extend(self.coordinates());
        row.extend(self.aux_values());
        let placeholders: Vec<String> = (1..=row.len()).map(|index| format!("?{index}")).collect();
        self.stock
            .execute(
                &format!("INSERT INTO rt VALUES({})", placeholders.join(", ")),
                rusqlite::params_from_iter(&row),
            )
            .expect("stock INSERT");
        let stock_id = self.stock.last_insert_rowid();

        let mut args = vec![SqliteValue::Null, to_ours(&row[0]), to_ours(&row[0])];
        args.extend(row[1..].iter().map(to_ours));
        let our_id = self.ours.update(&self.cx, &args).expect("our INSERT");
        assert_eq!(our_id, Some(stock_id), "{:?}: inserted rowid", self.config);
        self.remember(stock_id);
        self.drain();
    }

    fn delete(&mut self) {
        let index = self.rng.below(self.ids.len() as u64) as usize;
        let id = self.ids.swap_remove(index);
        self.live.remove(&id);
        let deleted = self
            .stock
            .execute("DELETE FROM rt WHERE id = ?1", [id])
            .expect("stock DELETE");
        assert_eq!(deleted, 1);
        self.ours
            .update(&self.cx, &[SqliteValue::Integer(id)])
            .expect("our DELETE");
        self.drain();
    }

    fn update(&mut self) {
        let index = self.rng.below(self.ids.len() as u64) as usize;
        let old = self.ids[index];
        let new = if self.rng.one_in(3) {
            let new = self.fresh_id();
            self.live.remove(&old);
            self.live.insert(new);
            self.ids[index] = new;
            new
        } else {
            old
        };
        let mut row = vec![Value::Integer(new)];
        row.extend(self.coordinates());
        row.extend(self.aux_values());
        let assignments: Vec<String> = self
            .config
            .column_names()
            .iter()
            .enumerate()
            .map(|(index, name)| format!("{name} = ?{}", index + 1))
            .collect();
        let mut params = row.clone();
        params.push(Value::Integer(old));
        let updated = self
            .stock
            .execute(
                &format!(
                    "UPDATE rt SET {} WHERE id = ?{}",
                    assignments.join(", "),
                    params.len()
                ),
                rusqlite::params_from_iter(&params),
            )
            .expect("stock UPDATE");
        assert_eq!(updated, 1);

        let mut args = vec![SqliteValue::Integer(old)];
        args.extend([to_ours(&row[0]), to_ours(&row[0])]);
        args.extend(row[1..].iter().map(to_ours));
        assert_eq!(self.ours.update(&self.cx, &args).expect("our UPDATE"), None);
        self.drain();
    }

    fn stock_query(&self, sql: &str, params: &[Value]) -> Vec<Vec<SqliteValue>> {
        let mut statement = self.stock.prepare(sql).expect("prepare query");
        let width = statement.column_count();
        statement
            .query_map(rusqlite::params_from_iter(params), |row| {
                (0..width)
                    .map(|index| row.get::<_, Value>(index).map(|value| to_ours(&value)))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .expect("run query")
            .collect::<rusqlite::Result<_>>()
            .expect("read query")
    }

    fn our_query(&self, idx_num: i32, idx_str: &str, args: &[Value]) -> Vec<Vec<SqliteValue>> {
        let table = rtree(self.ours.as_ref());
        let width = self.config.column_names().len();
        let args: Vec<SqliteValue> = args.iter().map(to_ours).collect();
        let mut cursor = VirtualTable::open(table).expect("open cursor");
        cursor
            .filter(
                &self.cx,
                idx_num,
                (!idx_str.is_empty()).then_some(idx_str),
                &args,
            )
            .expect("filter");
        let mut rows = Vec::new();
        while !cursor.eof() {
            let mut row = Vec::with_capacity(width);
            for column in 0..width {
                let mut ctx = ColumnContext::new();
                cursor
                    .column(&mut ctx, i32::try_from(column).expect("column index"))
                    .expect("column");
                row.push(ctx.take_value().unwrap_or(SqliteValue::Null));
            }
            assert_eq!(cursor.rowid().expect("rowid"), row[0].to_integer());
            rows.push(row);
            cursor.next(&self.cx).expect("next");
        }
        rows
    }

    fn bound(&mut self) -> Value {
        if self.config.int32 {
            if self.rng.one_in(4) {
                Value::Real(self.rng.below(200_000) as f64 - 100_000.5)
            } else {
                Value::Integer(self.rng.below(200_000) as i64 - 100_000)
            }
        } else {
            Value::Real((self.rng.below(2_000_000) as f64 - 1_000_000.0) / 977.0)
        }
    }

    /// A window query over random dimensions, mostly `<=`/`>=` overlap tests
    /// with the odd strict or equality term, in both engines.
    fn compare_window_query(&mut self, label: &str) {
        let mut terms = Vec::new();
        let mut idx_str = String::new();
        let mut params = Vec::new();
        for dim in 0..self.config.dimensions {
            if !self.rng.one_in(2) && dim > 0 {
                continue;
            }
            let lo = self.bound();
            let span = if self.config.int32 {
                Value::Integer(self.rng.below(30_000) as i64)
            } else {
                Value::Real(self.rng.below(300_000) as f64 / 977.0)
            };
            let hi = match (&lo, &span) {
                (Value::Integer(lo), Value::Integer(span)) => Value::Integer(lo + span),
                (Value::Real(lo), Value::Integer(span)) => Value::Real(lo + *span as f64),
                (Value::Real(lo), Value::Real(span)) => Value::Real(lo + span),
                _ => unreachable!("bounds are numbers"),
            };
            let (min_col, max_col) = (dim * 2, dim * 2 + 1);
            let (low_op, low_code) = if self.rng.one_in(5) {
                ("<", 'C')
            } else {
                ("<=", 'B')
            };
            let (high_op, high_code) = if self.rng.one_in(5) {
                (">", 'E')
            } else {
                (">=", 'D')
            };
            terms.push(format!("c{min_col} {low_op} ?{}", params.len() + 1));
            idx_str.push(low_code);
            idx_str.push(char::from(b'0' + min_col as u8));
            params.push(hi);
            terms.push(format!("c{max_col} {high_op} ?{}", params.len() + 1));
            idx_str.push(high_code);
            idx_str.push(char::from(b'0' + max_col as u8));
            params.push(lo);
        }
        let sql = format!("SELECT * FROM rt WHERE {}", terms.join(" AND "));
        let stock = self.stock_query(&sql, &params);
        let ours = self.our_query(SCAN_TREE, &idx_str, &params);
        assert_eq!(ours, stock, "{label}: {sql} {params:?}");
    }

    fn check(&mut self, label: &str) {
        let stock = self.stock_shadow();
        for ((ours, stock), table) in self
            .shadow
            .iter()
            .zip(&stock)
            .zip(["node", "rowid", "parent"])
        {
            if ours != stock {
                let differing: Vec<_> = ours
                    .keys()
                    .chain(stock.keys())
                    .filter(|key| ours.get(key) != stock.get(key))
                    .take(5)
                    .collect();
                panic!(
                    "{label} {:?}: rt_{table} differs ({} vs {} rows) at keys {differing:?}",
                    self.config,
                    ours.len(),
                    stock.len()
                );
            }
        }
        assert_eq!(rtree(self.ours.as_ref()).len(), self.ids.len());

        let scan = self.stock_query("SELECT * FROM rt", &[]);
        assert_eq!(
            self.our_query(SCAN_TREE, "", &[]),
            scan,
            "{label}: full scan"
        );
        for _ in 0..6 {
            self.compare_window_query(label);
        }
        if !self.ids.is_empty() {
            let id = self.ids[self.rng.below(self.ids.len() as u64) as usize];
            let stock = self.stock_query("SELECT * FROM rt WHERE id = ?1", &[Value::Integer(id)]);
            assert_eq!(stock.len(), 1);
            assert_eq!(
                self.our_query(LOOKUP_ROWID, "", &[Value::Integer(id)]),
                stock,
                "{label}: lookup {id}"
            );
        }
        assert!(
            self.our_query(LOOKUP_ROWID, "", &[Value::Integer(0)])
                .is_empty()
        );
    }

    /// Rebuilds our table from stock's shadow rows, as opening a database
    /// file stock wrote does.
    fn reload_from_stock(&mut self) {
        let [nodes, rowids, parents] = self.stock_shadow();
        let rows = |rows: Rows| -> Vec<(i64, Vec<SqliteValue>)> { rows.into_iter().collect() };
        let mut ours = Self::create_ours(&self.cx, self.config);
        let table = rtree_mut(ours.as_mut());
        table
            .load_shadow_storage(
                &rows(nodes.clone()),
                &rows(rowids.clone()),
                &rows(parents.clone()),
            )
            .expect("load stock shadow rows");
        assert!(table.uses_shadow_storage());
        assert!(
            table.take_shadow_writes().is_empty(),
            "loading writes nothing"
        );
        self.ours = ours;
        self.shadow = [nodes, rowids, parents];
    }

    fn run(mut self) {
        let operations = self.config.operations;
        for step in 1..=operations {
            match self.rng.below(20) {
                _ if self.ids.len() < 8 => self.insert(),
                0..=12 => self.insert(),
                13..=16 => self.delete(),
                _ => self.update(),
            }
            if step % 150 == 0 {
                self.check(&format!("step {step}"));
            }
            if step == operations / 2 {
                self.reload_from_stock();
                self.check(&format!("reload at step {step}"));
            }
        }
        let depth = self.shadow[0][&1][0]
            .as_blob()
            .map(|blob| blob[..2].to_vec());
        assert_ne!(
            depth,
            Some(vec![0, 0]),
            "{:?}: the tree must grow past one level",
            self.config
        );
        eprintln!(
            "{:?}: {} entries in {} nodes, root depth bytes {depth:?}, matched stock",
            self.config,
            self.ids.len(),
            self.shadow[0].len()
        );

        // Delete everything: condensing shrinks the tree back to a bare root.
        let mut step = 0;
        while !self.ids.is_empty() {
            self.delete();
            step += 1;
            if step % 100 == 0 {
                self.check(&format!("drain step {step}"));
            }
        }
        self.check("empty");
        assert_eq!(self.shadow[0].len(), 1, "only the root node remains");
        assert!(self.shadow[1].is_empty() && self.shadow[2].is_empty());
    }
}

#[test]
fn rtree_2d_matches_stock_shadow_tables_and_queries() {
    Pair::new(Config {
        int32: false,
        dimensions: 2,
        aux: 0,
        page_size: 4096,
        operations: 3_000,
        seed: 0x9E37_79B9_7F4A_7C15,
    })
    .run();
}

#[test]
fn rtree_1d_small_pages_matches_stock() {
    Pair::new(Config {
        int32: false,
        dimensions: 1,
        aux: 0,
        page_size: 1024,
        operations: 2_000,
        seed: 0xD1B5_4A32_D192_ED03,
    })
    .run();
}

#[test]
fn rtree_3d_with_auxiliary_columns_matches_stock() {
    Pair::new(Config {
        int32: false,
        dimensions: 3,
        aux: 2,
        page_size: 1024,
        operations: 2_000,
        seed: 0xA076_1D64_78BD_642F,
    })
    .run();
}

#[test]
fn rtree_5d_matches_stock() {
    Pair::new(Config {
        int32: false,
        dimensions: 5,
        aux: 0,
        page_size: 4096,
        operations: 1_500,
        seed: 0xE703_7ED1_A0B4_28DB,
    })
    .run();
}

#[test]
fn rtree_i32_2d_with_auxiliary_column_matches_stock() {
    Pair::new(Config {
        int32: true,
        dimensions: 2,
        aux: 1,
        page_size: 1024,
        operations: 2_000,
        seed: 0x8EBC_6AF0_9C88_C6E3,
    })
    .run();
}

#[test]
fn rtree_i32_4d_matches_stock() {
    Pair::new(Config {
        int32: true,
        dimensions: 4,
        aux: 0,
        page_size: 4096,
        operations: 1_500,
        seed: 0x5899_65CC_7537_4CC3,
    })
    .run();
}
