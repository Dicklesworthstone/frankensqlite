//! Lossless, bounded ILP1 payloads for historical intent logs.
//!
//! This codec preserves evidence; it does not authorize replay or merging. In
//! particular it never sorts operations, drops footprints, normalizes function
//! names, converts SQL storage classes, or replaces invalid UTF-8 TEXT bytes.
//! The old unversioned digest stream is not accepted as a persisted intent log.
//! Authentication still depends on the containing ECS object's trusted identity.
//!
//! All integers are little-endian. Nonempty payloads start with `ILP\x01` and a
//! u32 operation count. Each operation stores its schema epoch, read and write
//! key vectors, structural flags, and tagged operation. Keys contain kind u8,
//! B-tree namespace u8, id u32, and digest [u8; 16]. Operation/expression/value
//! tags are explicit below; vectors and byte strings have u32 lengths. Floats
//! retain their IEEE-754 bits. Empty logs retain the historical four zero bytes.
//! Payloads are limited to 16 MiB, collections to 65536 entries, expression
//! trees to 64 levels, and total expression nodes across the log to 65536.

use super::HistoryCompressionError;
use fsqlite_types::value::SmallText;
use fsqlite_types::{
    BtreeRef, ColumnIdx, IndexId, IntentFootprint, IntentOp, IntentOpKind, RebaseBinaryOp,
    RebaseExpr, RebaseUnaryOp, RowId, SemanticKeyKind, SemanticKeyRef, SqliteValue,
    StructuralEffects, TableId,
};

const MAGIC: &[u8; 4] = b"ILP\x01";
const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_ITEMS: usize = 65_536;
const MAX_DEPTH: usize = 64;
const MAX_NODES: usize = 65_536;
const KEY_BYTES: usize = 22;
const MIN_OP_BYTES: usize = 33;

type Result<T> = std::result::Result<T, HistoryCompressionError>;

fn invalid(message: &str) -> HistoryCompressionError {
    HistoryCompressionError::DecodeError(format!(
        "intent log patch payload decode/encode: {message}"
    ))
}

fn enter_expression(remaining: &mut usize, depth: usize) -> Result<()> {
    if depth >= MAX_DEPTH || *remaining == 0 {
        return Err(invalid("expression depth or node budget exceeded"));
    }
    *remaining -= 1;
    Ok(())
}

struct Writer {
    bytes: Vec<u8>,
    remaining_nodes: usize,
}

impl Writer {
    fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MAX_BYTES - self.bytes.len() {
            return Err(invalid("payload exceeds 16 MiB"));
        }
        self.bytes
            .try_reserve(bytes.len())
            .map_err(|_| invalid("allocation failed"))?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn tag(&mut self, tag: u8) -> Result<()> {
        self.bytes(&[tag])
    }

    fn u32(&mut self, value: u32) -> Result<()> {
        self.bytes(&value.to_le_bytes())
    }

    fn count(&mut self, count: usize) -> Result<()> {
        if count > MAX_ITEMS {
            return Err(invalid("collection exceeds 65536 entries"));
        }
        self.u32(u32::try_from(count).map_err(|_| invalid("count overflow"))?)
    }

    fn data(&mut self, bytes: &[u8]) -> Result<()> {
        self.u32(u32::try_from(bytes.len()).map_err(|_| invalid("length overflow"))?)?;
        self.bytes(bytes)
    }

    fn keys(&mut self, keys: &[SemanticKeyRef]) -> Result<()> {
        self.count(keys.len())?;
        for key in keys {
            self.tag(match key.kind {
                SemanticKeyKind::TableRow => 0,
                SemanticKeyKind::IndexEntry => 1,
            })?;
            let (namespace, id) = match key.btree {
                BtreeRef::Table(table) => (0, table.get()),
                BtreeRef::Index(index) => (1, index.get()),
            };
            self.tag(namespace)?;
            self.u32(id)?;
            self.bytes(&key.key_digest)?;
        }
        Ok(())
    }

    fn operation(&mut self, intent: &IntentOp) -> Result<()> {
        self.bytes(&intent.schema_epoch.to_le_bytes())?;
        self.keys(&intent.footprint.reads)?;
        self.keys(&intent.footprint.writes)?;
        let structural = intent.footprint.structural.bits();
        if StructuralEffects::from_bits(structural).is_none() {
            return Err(invalid("unknown structural flags"));
        }
        self.u32(structural)?;
        match &intent.op {
            IntentOpKind::Insert { table, key, record } => {
                self.tag(0)?;
                self.table_row(*table, *key)?;
                self.data(record)
            }
            IntentOpKind::Delete { table, key } => {
                self.tag(1)?;
                self.table_row(*table, *key)
            }
            IntentOpKind::Update { table, key, new_record } => {
                self.tag(2)?;
                self.table_row(*table, *key)?;
                self.data(new_record)
            }
            IntentOpKind::IndexInsert { index, key, rowid } => {
                self.tag(3)?;
                self.index_row(*index, key, *rowid)
            }
            IntentOpKind::IndexDelete { index, key, rowid } => {
                self.tag(4)?;
                self.index_row(*index, key, *rowid)
            }
            IntentOpKind::UpdateExpression { table, key, column_updates } => {
                self.tag(5)?;
                self.table_row(*table, *key)?;
                self.count(column_updates.len())?;
                for (column, expr) in column_updates {
                    self.u32(column.get())?;
                    self.expression(expr, 0)?;
                }
                Ok(())
            }
        }
    }

    fn table_row(&mut self, table: TableId, row: RowId) -> Result<()> {
        self.u32(table.get())?;
        self.bytes(&row.get().to_le_bytes())
    }

    fn index_row(&mut self, index: IndexId, key: &[u8], row: RowId) -> Result<()> {
        self.u32(index.get())?;
        self.data(key)?;
        self.bytes(&row.get().to_le_bytes())
    }

    fn value(&mut self, value: &SqliteValue) -> Result<()> {
        match value {
            SqliteValue::Null => self.tag(0),
            SqliteValue::Integer(integer) => {
                self.tag(1)?;
                self.bytes(&integer.to_le_bytes())
            }
            SqliteValue::Float(real) => {
                self.tag(2)?;
                self.bytes(&real.to_bits().to_le_bytes())
            }
            SqliteValue::Text(text) => {
                self.tag(3)?;
                self.data(text.as_bytes_direct())
            }
            SqliteValue::Blob(blob) => {
                self.tag(4)?;
                self.data(blob)
            }
        }
    }

    fn expressions(&mut self, expressions: &[RebaseExpr], depth: usize) -> Result<()> {
        self.count(expressions.len())?;
        for expression in expressions {
            self.expression(expression, depth)?;
        }
        Ok(())
    }

    fn optional_expression(&mut self, expr: Option<&RebaseExpr>, depth: usize) -> Result<()> {
        self.tag(u8::from(expr.is_some()))?;
        if let Some(expr) = expr {
            self.expression(expr, depth)?;
        }
        Ok(())
    }

    fn expression(&mut self, expr: &RebaseExpr, depth: usize) -> Result<()> {
        enter_expression(&mut self.remaining_nodes, depth)?;
        match expr {
            RebaseExpr::ColumnRef(column) => {
                self.tag(0)?;
                self.u32(column.get())
            }
            RebaseExpr::Literal(value) => {
                self.tag(1)?;
                self.value(value)
            }
            RebaseExpr::UnaryOp { op, operand } => {
                self.tag(2)?;
                self.tag(match op {
                    RebaseUnaryOp::Negate => 0,
                    RebaseUnaryOp::BitwiseNot => 1,
                    RebaseUnaryOp::Not => 2,
                })?;
                self.expression(operand, depth + 1)
            }
            RebaseExpr::BinaryOp { op, left, right } => {
                self.tag(3)?;
                self.tag(match op {
                    RebaseBinaryOp::Add => 0,
                    RebaseBinaryOp::Subtract => 1,
                    RebaseBinaryOp::Multiply => 2,
                    RebaseBinaryOp::Divide => 3,
                    RebaseBinaryOp::Remainder => 4,
                    RebaseBinaryOp::BitwiseAnd => 5,
                    RebaseBinaryOp::BitwiseOr => 6,
                    RebaseBinaryOp::ShiftLeft => 7,
                    RebaseBinaryOp::ShiftRight => 8,
                })?;
                self.expression(left, depth + 1)?;
                self.expression(right, depth + 1)
            }
            RebaseExpr::FunctionCall { name, args } => {
                self.tag(4)?;
                self.data(name.as_bytes())?;
                self.expressions(args, depth + 1)
            }
            RebaseExpr::Cast { expr, type_name } => {
                self.tag(5)?;
                self.expression(expr, depth + 1)?;
                self.data(type_name.as_bytes())
            }
            RebaseExpr::Case { operand, when_clauses, else_clause } => {
                self.tag(6)?;
                self.optional_expression(operand.as_deref(), depth + 1)?;
                self.count(when_clauses.len())?;
                for (when, then) in when_clauses {
                    self.expression(when, depth + 1)?;
                    self.expression(then, depth + 1)?;
                }
                self.optional_expression(else_clause.as_deref(), depth + 1)
            }
            RebaseExpr::Coalesce(args) => {
                self.tag(7)?;
                self.expressions(args, depth + 1)
            }
            RebaseExpr::NullIf { left, right } => {
                self.tag(8)?;
                self.expression(left, depth + 1)?;
                self.expression(right, depth + 1)
            }
            RebaseExpr::Concat { left, right } => {
                self.tag(9)?;
                self.expression(left, depth + 1)?;
                self.expression(right, depth + 1)
            }
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    remaining_nodes: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        if length > self.bytes.len() {
            return Err(invalid("truncated field"));
        }
        let (head, rest) = self.bytes.split_at(length);
        self.bytes = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?.try_into().map_err(|_| invalid("truncated fixed field"))
    }

    fn tag(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn length(&mut self) -> Result<usize> {
        usize::try_from(self.u32()?).map_err(|_| invalid("length exceeds address space"))
    }

    fn data(&mut self) -> Result<&'a [u8]> {
        let length = self.length()?;
        self.take(length)
    }

    fn string(&mut self) -> Result<String> {
        let text = std::str::from_utf8(self.data()?).map_err(|_| invalid("invalid UTF-8 name"))?;
        let mut result = String::new();
        result.try_reserve(text.len()).map_err(|_| invalid("allocation failed"))?;
        result.push_str(text);
        Ok(result)
    }

    fn many<T>(
        &mut self,
        minimum: usize,
        mut read: impl FnMut(&mut Self) -> Result<T>,
    ) -> Result<Vec<T>> {
        let count = self.length()?;
        if count > MAX_ITEMS || count > self.bytes.len() / minimum {
            return Err(invalid("impossible collection count"));
        }
        // Never preallocate count entries from untrusted nested lengths. Only
        // materialized children allocate, with one shared expression budget.
        let mut values = Vec::new();
        for _ in 0..count {
            let value = read(self)?;
            values.try_reserve(1).map_err(|_| invalid("allocation failed"))?;
            values.push(value);
        }
        Ok(values)
    }

    fn key(&mut self) -> Result<SemanticKeyRef> {
        let kind = match self.tag()? {
            0 => SemanticKeyKind::TableRow,
            1 => SemanticKeyKind::IndexEntry,
            _ => return Err(invalid("unknown semantic key kind")),
        };
        let namespace = self.tag()?;
        let id = self.u32()?;
        let btree = match namespace {
            0 => BtreeRef::Table(TableId::new(id)),
            1 => BtreeRef::Index(IndexId::new(id)),
            _ => return Err(invalid("unknown B-tree namespace")),
        };
        Ok(SemanticKeyRef { btree, kind, key_digest: self.array()? })
    }

    fn operation(&mut self) -> Result<IntentOp> {
        let schema_epoch = u64::from_le_bytes(self.array()?);
        let reads = self.many(KEY_BYTES, Self::key)?;
        let writes = self.many(KEY_BYTES, Self::key)?;
        let structural = StructuralEffects::from_bits(self.u32()?)
            .ok_or_else(|| invalid("unknown structural flags"))?;
        let tag = self.tag()?;
        let id = self.u32()?;
        let op = match tag {
            0 => IntentOpKind::Insert {
                table: TableId::new(id),
                key: RowId::new(i64::from_le_bytes(self.array()?)),
                record: self.data()?.to_vec(),
            },
            1 => IntentOpKind::Delete {
                table: TableId::new(id),
                key: RowId::new(i64::from_le_bytes(self.array()?)),
            },
            2 => IntentOpKind::Update {
                table: TableId::new(id),
                key: RowId::new(i64::from_le_bytes(self.array()?)),
                new_record: self.data()?.to_vec(),
            },
            3 => IntentOpKind::IndexInsert {
                index: IndexId::new(id),
                key: self.data()?.to_vec(),
                rowid: RowId::new(i64::from_le_bytes(self.array()?)),
            },
            4 => IntentOpKind::IndexDelete {
                index: IndexId::new(id),
                key: self.data()?.to_vec(),
                rowid: RowId::new(i64::from_le_bytes(self.array()?)),
            },
            5 => IntentOpKind::UpdateExpression {
                table: TableId::new(id),
                key: RowId::new(i64::from_le_bytes(self.array()?)),
                column_updates: self.many(6, |reader| {
                    Ok((ColumnIdx::new(reader.u32()?), reader.expression(0)?))
                })?,
            },
            _ => return Err(invalid("unknown intent operation")),
        };
        Ok(IntentOp {
            schema_epoch,
            footprint: IntentFootprint { reads, writes, structural },
            op,
        })
    }

    fn value(&mut self) -> Result<SqliteValue> {
        match self.tag()? {
            0 => Ok(SqliteValue::Null),
            1 => Ok(SqliteValue::Integer(i64::from_le_bytes(self.array()?))),
            // Do not use From<f64>, which would replace NaN bits with NULL.
            2 => Ok(SqliteValue::Float(f64::from_bits(u64::from_le_bytes(self.array()?)))),
            3 => Ok(SqliteValue::Text(SmallText::from_bytes(self.data()?))),
            4 => Ok(SqliteValue::Blob(self.data()?.into())),
            _ => Err(invalid("unknown SQL value tag")),
        }
    }

    fn optional_expression(&mut self, depth: usize) -> Result<Option<Box<RebaseExpr>>> {
        match self.tag()? {
            0 => Ok(None),
            1 => Ok(Some(Box::new(self.expression(depth)?))),
            _ => Err(invalid("invalid optional expression flag")),
        }
    }

    fn expression(&mut self, depth: usize) -> Result<RebaseExpr> {
        enter_expression(&mut self.remaining_nodes, depth)?;
        match self.tag()? {
            0 => Ok(RebaseExpr::ColumnRef(ColumnIdx::new(self.u32()?))),
            1 => Ok(RebaseExpr::Literal(self.value()?)),
            2 => {
                let op = match self.tag()? {
                    0 => RebaseUnaryOp::Negate,
                    1 => RebaseUnaryOp::BitwiseNot,
                    2 => RebaseUnaryOp::Not,
                    _ => return Err(invalid("unknown unary operator")),
                };
                Ok(RebaseExpr::UnaryOp { op, operand: Box::new(self.expression(depth + 1)?) })
            }
            3 => {
                let op = match self.tag()? {
                    0 => RebaseBinaryOp::Add,
                    1 => RebaseBinaryOp::Subtract,
                    2 => RebaseBinaryOp::Multiply,
                    3 => RebaseBinaryOp::Divide,
                    4 => RebaseBinaryOp::Remainder,
                    5 => RebaseBinaryOp::BitwiseAnd,
                    6 => RebaseBinaryOp::BitwiseOr,
                    7 => RebaseBinaryOp::ShiftLeft,
                    8 => RebaseBinaryOp::ShiftRight,
                    _ => return Err(invalid("unknown binary operator")),
                };
                Ok(RebaseExpr::BinaryOp {
                    op,
                    left: Box::new(self.expression(depth + 1)?),
                    right: Box::new(self.expression(depth + 1)?),
                })
            }
            4 => Ok(RebaseExpr::FunctionCall {
                name: self.string()?,
                args: self.many(2, |reader| reader.expression(depth + 1))?,
            }),
            5 => Ok(RebaseExpr::Cast {
                expr: Box::new(self.expression(depth + 1)?),
                type_name: self.string()?,
            }),
            6 => Ok(RebaseExpr::Case {
                operand: self.optional_expression(depth + 1)?,
                when_clauses: self.many(4, |reader| {
                    Ok((reader.expression(depth + 1)?, reader.expression(depth + 1)?))
                })?,
                else_clause: self.optional_expression(depth + 1)?,
            }),
            7 => Ok(RebaseExpr::Coalesce(self.many(2, |reader| reader.expression(depth + 1))?)),
            8 => Ok(RebaseExpr::NullIf {
                left: Box::new(self.expression(depth + 1)?),
                right: Box::new(self.expression(depth + 1)?),
            }),
            9 => Ok(RebaseExpr::Concat {
                left: Box::new(self.expression(depth + 1)?),
                right: Box::new(self.expression(depth + 1)?),
            }),
            _ => Err(invalid("unknown expression tag")),
        }
    }
}

pub(super) fn encode(ops: &[IntentOp]) -> Result<Vec<u8>> {
    if ops.is_empty() {
        return Ok(0_u32.to_le_bytes().to_vec());
    }
    let mut writer = Writer { bytes: Vec::new(), remaining_nodes: MAX_NODES };
    writer.bytes(MAGIC)?;
    writer.count(ops.len())?;
    for op in ops {
        writer.operation(op)?;
    }
    Ok(writer.bytes)
}

pub(super) fn decode(payload: &[u8]) -> Result<Vec<IntentOp>> {
    if payload.is_empty() || payload == 0_u32.to_le_bytes().as_slice() {
        return Ok(Vec::new());
    }
    if payload.len() > MAX_BYTES {
        return Err(invalid("payload exceeds 16 MiB"));
    }
    let mut reader = Reader { bytes: payload, remaining_nodes: MAX_NODES };
    if reader.take(MAGIC.len())? != MAGIC {
        return Err(invalid("unknown format/version"));
    }
    let ops = reader.many(MIN_OP_BYTES, Reader::operation)?;
    if ops.is_empty() {
        return Err(invalid("noncanonical empty log"));
    }
    if !reader.bytes.is_empty() {
        return Err(invalid("trailing bytes"));
    }
    Ok(ops)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history_compression::{
        CompressedPageHistory, CompressedPageVersion, CompressedVersionData,
        are_intent_ops_independent, is_mergeable_intent,
    };
    use fsqlite_types::{CommitSeq, PageNumber, TypeAffinity};

    fn intent(op: IntentOpKind) -> IntentOp {
        IntentOp { schema_epoch: 7, footprint: IntentFootprint::empty(), op }
    }

    fn expression_intent(expr: RebaseExpr) -> IntentOp {
        intent(IntentOpKind::UpdateExpression {
            table: TableId::new(3),
            key: RowId::new(-7),
            column_updates: vec![(ColumnIdx::new(0), expr)],
        })
    }

    fn literal() -> RebaseExpr {
        RebaseExpr::Literal(SqliteValue::Integer(2))
    }

    fn expressions() -> Vec<RebaseExpr> {
        let mut result = vec![
            RebaseExpr::ColumnRef(ColumnIdx::new(u32::MAX)),
            literal(),
            RebaseExpr::FunctionCall { name: "mAx".to_owned(), args: vec![literal(), literal()] },
            RebaseExpr::Cast { expr: Box::new(literal()), type_name: "tExT".to_owned() },
            RebaseExpr::Case {
                operand: Some(Box::new(literal())),
                when_clauses: vec![(literal(), literal()), (literal(), literal())],
                else_clause: Some(Box::new(literal())),
            },
            RebaseExpr::Case { operand: None, when_clauses: Vec::new(), else_clause: None },
            RebaseExpr::Coalesce(vec![literal(), literal()]),
            RebaseExpr::NullIf { left: Box::new(literal()), right: Box::new(literal()) },
            RebaseExpr::Concat { left: Box::new(literal()), right: Box::new(literal()) },
        ];
        for op in [RebaseUnaryOp::Negate, RebaseUnaryOp::BitwiseNot, RebaseUnaryOp::Not] {
            result.push(RebaseExpr::UnaryOp { op, operand: Box::new(literal()) });
        }
        for op in [
            RebaseBinaryOp::Add, RebaseBinaryOp::Subtract, RebaseBinaryOp::Multiply,
            RebaseBinaryOp::Divide, RebaseBinaryOp::Remainder, RebaseBinaryOp::BitwiseAnd,
            RebaseBinaryOp::BitwiseOr, RebaseBinaryOp::ShiftLeft, RebaseBinaryOp::ShiftRight,
        ] {
            result.push(RebaseExpr::BinaryOp {
                op, left: Box::new(literal()), right: Box::new(literal()),
            });
        }
        result
    }

    fn all_operations() -> Vec<IntentOp> {
        let table = TableId::new(u32::MAX);
        let key = RowId::new(i64::MIN);
        let index = IndexId::new(u32::MAX);
        let mut ops = vec![
            intent(IntentOpKind::Insert { table, key, record: vec![0, 255, 3] }),
            intent(IntentOpKind::Delete { table, key: RowId::MAX }),
            intent(IntentOpKind::Update { table, key, new_record: Vec::new() }),
            intent(IntentOpKind::IndexInsert { index, key: vec![0, 255], rowid: key }),
            intent(IntentOpKind::IndexDelete { index, key: Vec::new(), rowid: RowId::MAX }),
            intent(IntentOpKind::UpdateExpression {
                table, key,
                // Repeated target columns and expression order must survive encoding.
                column_updates: expressions()
                    .into_iter()
                    .map(|expr| (ColumnIdx::new(1), expr))
                    .collect(),
            }),
        ];
        let table_key =
            SemanticKeyRef::new(BtreeRef::Table(table), SemanticKeyKind::TableRow, &[0]);
        let index_key =
            SemanticKeyRef::new(BtreeRef::Index(index), SemanticKeyKind::IndexEntry, &[1]);
        for op in &mut ops {
            op.schema_epoch = u64::MAX;
            op.footprint.reads = vec![index_key.clone(), table_key.clone(), index_key.clone()];
            op.footprint.writes = vec![table_key.clone(), index_key.clone()];
            op.footprint.structural = StructuralEffects::all();
        }
        ops
    }

    #[test]
    fn intent_wire_preserves_all_operations_footprints_and_expression_forms() {
        let ops = all_operations();
        let bytes = encode(&ops).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, ops);
        assert_eq!(encode(&decoded).unwrap(), bytes);
        assert!(decoded.iter().all(|op| !is_mergeable_intent(op)));
        assert!(!are_intent_ops_independent(&decoded[0], &decoded[1]));
    }

    #[test]
    fn intent_wire_has_an_independent_golden_delete_vector() {
        let ops = vec![IntentOp {
            schema_epoch: 0x0102_0304_0506_0708,
            footprint: IntentFootprint::empty(),
            op: IntentOpKind::Delete {
                table: TableId::new(0x090A_0B0C),
                key: RowId::new(-2),
            },
        }];
        let expected = [
            0x49, 0x4C, 0x50, 1, 1, 0, 0, 0,
            8, 7, 6, 5, 4, 3, 2, 1,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 0x0C, 0x0B, 0x0A, 9,
            0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        ];
        assert_eq!(encode(&ops).unwrap(), expected);
        assert_eq!(decode(&expected).unwrap(), ops);
    }

    #[test]
    fn intent_wire_preserves_literal_storage_classes_and_exact_payload_bits() {
        let values = vec![
            SqliteValue::Null,
            SqliteValue::Integer(i64::MIN),
            SqliteValue::Integer(i64::MAX),
            SqliteValue::Integer(0),
            SqliteValue::Float(0.0),
            SqliteValue::Float(-0.0),
            SqliteValue::Float(f64::from_bits(1)),
            SqliteValue::Float(f64::INFINITY),
            SqliteValue::Float(f64::NEG_INFINITY),
            SqliteValue::Float(f64::from_bits(0x7FF8_0000_0000_0042)),
            SqliteValue::Text(SmallText::from_bytes(b"\x80\0\x81")),
            SqliteValue::Text(SmallText::from_bytes(b"\x81\0\x80")),
            SqliteValue::Text("".into()),
            SqliteValue::Blob(b"\x80\0\x81".as_slice().into()),
            SqliteValue::Blob(Vec::<u8>::new().into()),
        ];
        let ops: Vec<_> = values
            .iter()
            .cloned()
            .map(RebaseExpr::Literal)
            .map(expression_intent)
            .collect();
        let bytes = encode(&ops).unwrap();
        let decoded = decode(&bytes).unwrap();
        for (original, restored) in values.iter().zip(&decoded) {
            let IntentOpKind::UpdateExpression { column_updates, .. } = &restored.op else {
                panic!("expected expression update");
            };
            let RebaseExpr::Literal(value) = &column_updates[0].1 else {
                panic!("expected literal");
            };
            match (original, value) {
                (SqliteValue::Null, SqliteValue::Null) => {}
                (SqliteValue::Integer(a), SqliteValue::Integer(b)) => assert_eq!(a, b),
                (SqliteValue::Float(a), SqliteValue::Float(b)) => {
                    assert_eq!(a.to_bits(), b.to_bits());
                }
                (SqliteValue::Text(a), SqliteValue::Text(b)) => {
                    assert_eq!(a.as_bytes_direct(), b.as_bytes_direct());
                }
                (SqliteValue::Blob(a), SqliteValue::Blob(b)) => assert_eq!(a, b),
                _ => panic!("storage class changed: {original:?} -> {value:?}"),
            }
        }
        assert_eq!(encode(&decoded).unwrap(), bytes);
    }

    #[test]
    fn nonempty_history_roundtrip_retains_executable_column_updates() {
        let op = expression_intent(RebaseExpr::BinaryOp {
            op: RebaseBinaryOp::Add,
            left: Box::new(RebaseExpr::ColumnRef(ColumnIdx::new(0))),
            right: Box::new(literal()),
        });
        let history = CompressedPageHistory {
            pgno: PageNumber::ONE,
            versions: vec![
                CompressedPageVersion {
                    commit_seq: CommitSeq::new(9),
                    data: CompressedVersionData::FullImage(vec![0; 512]),
                },
                CompressedPageVersion {
                    commit_seq: CommitSeq::new(8),
                    data: CompressedVersionData::IntentLogPatch(vec![op]),
                },
            ],
        };
        let bytes = history.try_to_bytes().unwrap();
        drop(history);
        let restored = CompressedPageHistory::from_bytes(&bytes).unwrap();
        let CompressedVersionData::IntentLogPatch(ops) = &restored.versions[1].data else {
            panic!("missing persisted intent log");
        };
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].schema_epoch, 7);
        let IntentOpKind::UpdateExpression { column_updates, .. } = &ops[0].op else {
            panic!("missing persisted column expressions");
        };
        let updated = crate::index_regen::apply_column_updates(
            &[SqliteValue::Integer(40)], column_updates, &[TypeAffinity::Integer],
        ).unwrap();
        assert_eq!(updated[0].as_integer(), Some(42));
    }

    #[test]
    fn intent_wire_keeps_empty_encoding_but_rejects_legacy_nonempty_digests() {
        assert_eq!(encode(&[]).unwrap(), [0; 4]);
        assert!(decode(&[]).unwrap().is_empty());
        assert!(decode(&[0; 4]).unwrap().is_empty());
        assert!(decode(b"ILP\x01\0\0\0\0").is_err());
        let legacy = super::super::canonical_intent_ops_bytes(&all_operations());
        assert!(decode(&legacy).is_err());
    }

    #[test]
    fn intent_wire_rejects_every_truncation_and_trailing_bytes() {
        let bytes = encode(&all_operations()).unwrap();
        for length in 1..bytes.len() {
            assert!(decode(&bytes[..length]).is_err(), "accepted truncation {length}");
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode(&trailing).is_err());
    }

    #[test]
    fn intent_wire_rejects_unknown_tags_flags_lengths_and_invalid_names() {
        let op = intent(IntentOpKind::Delete { table: TableId::new(1), key: RowId::new(1) });
        let bytes = encode(&[op]).unwrap();
        for (offset, value) in [(0, b'X'), (3, 2), (28, 6), (25, 1)] {
            let mut invalid_bytes = bytes.clone();
            invalid_bytes[offset] = value;
            assert!(decode(&invalid_bytes).is_err(), "accepted invalid field at {offset}");
        }
        for offset in [4, 16, 20] {
            let mut invalid_bytes = bytes.clone();
            invalid_bytes[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(decode(&invalid_bytes).is_err());
        }
        let mut op = expression_intent(literal());
        op.footprint.structural = StructuralEffects::from_bits_retain(1 << 31);
        assert!(encode(&[op]).is_err());

        // Decode expression fields directly with the same bounded parser.
        for expression in [
            vec![10],                         // unknown expression
            vec![1, 5],                       // unknown SQL value
            vec![2, 3, 1, 0],                 // unknown unary operator
            vec![3, 9, 1, 0, 1, 0],           // unknown binary operator
            vec![6, 2, 0, 0, 0, 0, 0],        // optional flag is not 0/1
            vec![4, 1, 0, 0, 0, 255, 0, 0, 0, 0], // invalid function-name UTF-8
            vec![1, 3, 255, 255, 255, 255],    // impossible TEXT length
        ] {
            let mut reader = Reader { bytes: &expression, remaining_nodes: MAX_NODES };
            assert!(reader.expression(0).is_err());
        }
        for (offset, tag) in [(0, 2), (1, 2)] {
            let mut bytes = [0; KEY_BYTES];
            bytes[offset] = tag;
            let mut reader = Reader { bytes: &bytes, remaining_nodes: MAX_NODES };
            assert!(reader.key().is_err());
        }
    }

    #[test]
    fn intent_wire_enforces_depth_and_log_wide_node_limits() {
        let mut expr = literal();
        for _ in 1..MAX_DEPTH {
            expr = RebaseExpr::UnaryOp { op: RebaseUnaryOp::Not, operand: Box::new(expr) };
        }
        let bytes = encode(&[expression_intent(expr.clone())]).unwrap();
        assert!(decode(&bytes).is_ok());
        let too_deep = RebaseExpr::UnaryOp { op: RebaseUnaryOp::Not, operand: Box::new(expr) };
        assert!(encode(&[expression_intent(too_deep)]).is_err());
        // The first expression follows the fixed header, op, and column fields.
        let mut too_deep_bytes = bytes;
        too_deep_bytes.splice(49..49, [2, 2]);
        assert!(decode(&too_deep_bytes).is_err());

        let wide =
            RebaseExpr::Coalesce(vec![RebaseExpr::Literal(SqliteValue::Null); MAX_NODES - 1]);
        let one = expression_intent(wide);
        let mut bytes = encode(std::slice::from_ref(&one)).unwrap();
        assert!(decode(&bytes).is_ok());
        assert!(encode(&[one, expression_intent(literal())]).is_err());
        // Add another fully encoded operation without resetting the node budget.
        let tail = encode(&[expression_intent(literal())]).unwrap();
        bytes[4..8].copy_from_slice(&2_u32.to_le_bytes());
        bytes.extend_from_slice(&tail[8..]);
        assert!(decode(&bytes).is_err());
    }

    #[test]
    fn intent_wire_rejects_oversized_payloads_and_collections() {
        assert!(decode(&vec![0; MAX_BYTES + 1]).is_err());
        let op = intent(IntentOpKind::Insert {
            table: TableId::new(1), key: RowId::new(1), record: vec![0; MAX_BYTES],
        });
        assert!(encode(&[op]).is_err());
        let op = intent(IntentOpKind::Delete { table: TableId::new(1), key: RowId::new(1) });
        assert!(encode(&vec![op; MAX_ITEMS + 1]).is_err());
    }

    #[test]
    fn accepted_wire_mutations_reencode_without_dropping_evidence() {
        let mut bytes = encode(&all_operations()).unwrap();
        for offset in 0..bytes.len() {
            for bit in 0..8 {
                bytes[offset] ^= 1 << bit;
                if let Ok(decoded) = decode(&bytes) {
                    assert_eq!(encode(&decoded).unwrap(), bytes);
                }
                bytes[offset] ^= 1 << bit;
            }
        }
    }
}
