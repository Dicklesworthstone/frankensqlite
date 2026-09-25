//! Lossless SPP1 payloads for historical structured patches.
//!
//! This is serialization, not merge authorization or B-tree validation. Operation
//! order, repeated keys, empty cell data and opaque-page XOR ranges are preserved.
//! Existing physical-merge validation still decides whether a patch may apply.

use super::HistoryCompressionError;
use crate::physical_merge::{
    CellOp, CellOpKind, FreeSpaceOp, HeaderOp, RangeXorPatch, StructuredPagePatch,
};
use fsqlite_types::PageNumber;

const MAGIC: &[u8; 4] = b"SPP\x01";
const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_OPS: usize = 65_536;

type Result<T> = std::result::Result<T, HistoryCompressionError>;

fn invalid(message: &str) -> HistoryCompressionError {
    HistoryCompressionError::DecodeError(format!(
        "structured patch payload decode/encode: {message}"
    ))
}

struct Writer(Vec<u8>);

impl Writer {
    fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MAX_BYTES - self.0.len() {
            return Err(invalid("payload exceeds 16 MiB"));
        }
        self.0
            .try_reserve(bytes.len())
            .map_err(|_| invalid("allocation failed"))?;
        self.0.extend_from_slice(bytes);
        Ok(())
    }

    fn count(&mut self, length: usize) -> Result<()> {
        if length > MAX_OPS {
            return Err(invalid("too many operations"));
        }
        self.bytes(
            &u32::try_from(length)
                .map_err(|_| invalid("count overflow"))?
                .to_le_bytes(),
        )
    }

    fn data(&mut self, bytes: &[u8]) -> Result<()> {
        self.bytes(
            &u32::try_from(bytes.len())
                .map_err(|_| invalid("length overflow"))?
                .to_le_bytes(),
        )?;
        self.bytes(bytes)
    }
}

pub(super) fn encode(patch: &StructuredPagePatch) -> Result<Vec<u8>> {
    // Keep the previous empty representation exactly, not a second empty encoding.
    if patch.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Writer(Vec::new());
    out.bytes(MAGIC)?;
    out.count(patch.header_ops.len())?;
    for op in &patch.header_ops {
        match op {
            HeaderOp::SetCellCount(count) => {
                out.bytes(&[0])?;
                out.bytes(&count.to_le_bytes())?;
            }
            HeaderOp::SetRightMostChild(page) => {
                out.bytes(&[1])?;
                out.bytes(&page.get().to_le_bytes())?;
            }
        }
    }
    out.count(patch.cell_ops.len())?;
    for op in &patch.cell_ops {
        out.bytes(&op.cell_key_digest)?;
        match &op.kind {
            CellOpKind::Insert { cell_bytes } => {
                out.bytes(&[0])?;
                out.data(cell_bytes)?;
            }
            CellOpKind::Delete => out.bytes(&[1])?,
            CellOpKind::Replace { new_cell_bytes } => {
                out.bytes(&[2])?;
                out.data(new_cell_bytes)?;
            }
        }
    }
    out.count(patch.free_ops.len())?;
    for FreeSpaceOp::AddFreeblock { offset, size } in &patch.free_ops {
        out.bytes(&[0])?;
        out.bytes(&offset.to_le_bytes())?;
        out.bytes(&size.to_le_bytes())?;
    }
    out.count(patch.raw_xor_ranges.len())?;
    for range in &patch.raw_xor_ranges {
        out.bytes(&range.offset.to_le_bytes())?;
        out.data(&range.data)?;
    }
    Ok(out.0)
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        if length > self.0.len() {
            return Err(invalid("truncated field"));
        }
        let (head, rest) = self.0.split_at(length);
        self.0 = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| invalid("truncated fixed field"))
    }

    fn tag(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn length(&mut self) -> Result<usize> {
        usize::try_from(self.u32()?).map_err(|_| invalid("length exceeds address space"))
    }

    fn count(&mut self, minimum: usize) -> Result<usize> {
        let count = self.length()?;
        if count > MAX_OPS || count > self.0.len() / minimum {
            return Err(invalid("impossible operation count"));
        }
        Ok(count)
    }

    fn data(&mut self) -> Result<Vec<u8>> {
        let length = self.length()?;
        Ok(self.take(length)?.to_vec())
    }
}

pub(super) fn decode(payload: &[u8]) -> Result<StructuredPagePatch> {
    if payload.is_empty() {
        return Ok(StructuredPagePatch::default());
    }
    if payload.len() > MAX_BYTES {
        return Err(invalid("payload exceeds 16 MiB"));
    }
    let mut input = Reader(payload);
    if input.take(MAGIC.len())? != MAGIC {
        return Err(invalid("unknown format/version"));
    }
    let mut patch = StructuredPagePatch::default();
    for _ in 0..input.count(3)? {
        patch.header_ops.push(match input.tag()? {
            0 => HeaderOp::SetCellCount(input.u16()?),
            1 => HeaderOp::SetRightMostChild(
                PageNumber::new(input.u32()?).ok_or_else(|| invalid("invalid child page"))?,
            ),
            _ => return Err(invalid("unknown header operation")),
        });
    }
    for _ in 0..input.count(17)? {
        let cell_key_digest = input.array()?;
        let kind = match input.tag()? {
            0 => CellOpKind::Insert {
                cell_bytes: input.data()?,
            },
            1 => CellOpKind::Delete,
            2 => CellOpKind::Replace {
                new_cell_bytes: input.data()?,
            },
            _ => return Err(invalid("unknown cell operation")),
        };
        patch.cell_ops.push(CellOp {
            cell_key_digest,
            kind,
        });
    }
    for _ in 0..input.count(5)? {
        if input.tag()? != 0 {
            return Err(invalid("unknown free-space operation"));
        }
        patch.free_ops.push(FreeSpaceOp::AddFreeblock {
            offset: input.u16()?,
            size: input.u16()?,
        });
    }
    for _ in 0..input.count(8)? {
        patch.raw_xor_ranges.push(RangeXorPatch {
            offset: input.u32()?,
            data: input.data()?,
        });
    }
    if !input.0.is_empty() {
        return Err(invalid("trailing bytes"));
    }
    if patch.is_empty() {
        return Err(invalid("noncanonical empty patch"));
    }
    Ok(patch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history_compression::{
        CompressedPageHistory, CompressedPageVersion, CompressedVersionData,
    };
    use fsqlite_types::{CommitSeq, IntentFootprint, IntentOp, IntentOpKind, RowId, TableId};

    fn sample() -> StructuredPagePatch {
        StructuredPagePatch {
            header_ops: vec![
                HeaderOp::SetCellCount(u16::MAX),
                HeaderOp::SetRightMostChild(PageNumber::new(u32::MAX - 1).unwrap()),
            ],
            cell_ops: vec![
                CellOp {
                    cell_key_digest: [7; 16],
                    kind: CellOpKind::Insert {
                        cell_bytes: vec![0, 255, 1],
                    },
                },
                CellOp {
                    cell_key_digest: [7; 16],
                    kind: CellOpKind::Delete,
                },
                CellOp {
                    cell_key_digest: [8; 16],
                    kind: CellOpKind::Replace {
                        new_cell_bytes: Vec::new(),
                    },
                },
            ],
            free_ops: vec![FreeSpaceOp::AddFreeblock {
                offset: u16::MAX,
                size: 0,
            }],
            raw_xor_ranges: vec![RangeXorPatch {
                offset: u32::MAX,
                data: vec![0, 42],
            }],
        }
    }

    fn history(data: CompressedVersionData) -> CompressedPageHistory {
        CompressedPageHistory {
            pgno: PageNumber::new(1).unwrap(),
            versions: vec![
                CompressedPageVersion {
                    commit_seq: CommitSeq::new(2),
                    data: CompressedVersionData::FullImage(vec![0; 512]),
                },
                CompressedPageVersion {
                    commit_seq: CommitSeq::new(1),
                    data,
                },
            ],
        }
    }

    #[test]
    fn structured_history_preserves_every_operation_and_order() {
        let original = history(CompressedVersionData::StructuredPatch(sample()));
        let bytes = original.try_to_bytes().unwrap();
        assert_eq!(CompressedPageHistory::from_bytes(&bytes).unwrap(), original);
        assert_eq!(original.to_bytes(), bytes);
    }

    #[test]
    fn empty_patch_keeps_legacy_encoding() {
        assert!(encode(&StructuredPagePatch::default()).unwrap().is_empty());
        assert_eq!(decode(&[]).unwrap(), StructuredPagePatch::default());
        let original = history(CompressedVersionData::StructuredPatch(
            StructuredPagePatch::default(),
        ));
        assert_eq!(
            CompressedPageHistory::from_bytes(&original.to_bytes()).unwrap(),
            original
        );
    }

    #[test]
    fn truncated_payloads_never_decode_as_empty_patches() {
        let encoded = encode(&sample()).unwrap();
        for length in 1..encoded.len() {
            assert!(
                decode(&encoded[..length]).is_err(),
                "accepted truncation at {length}"
            );
        }
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode(&trailing).is_err());
    }

    #[test]
    fn rejects_unknown_versions_operations_and_impossible_counts() {
        let original = encode(&sample()).unwrap();
        for (offset, value) in [(0, b'X'), (3, 2), (8, 9)] {
            let mut changed = original.clone();
            changed[offset] = value;
            assert!(decode(&changed).is_err());
        }
        let mut count = original;
        count[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&count).is_err());
        let mut empty = MAGIC.to_vec();
        empty.extend_from_slice(&[0; 16]);
        assert!(decode(&empty).is_err());
    }

    #[test]
    fn rejects_zero_child_page_and_oversized_input() {
        let mut payload = MAGIC.to_vec();
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(1);
        payload.extend_from_slice(&[0; 16]);
        assert!(decode(&payload).is_err());
        assert!(decode(&vec![0; MAX_BYTES + 1]).is_err());
        let patch = StructuredPagePatch {
            raw_xor_ranges: vec![RangeXorPatch {
                offset: 0,
                data: vec![0; MAX_BYTES],
            }],
            ..StructuredPagePatch::default()
        };
        assert!(encode(&patch).is_err());
    }

    #[test]
    fn checked_history_encoder_roundtrips_nonempty_intents() {
        let original = history(CompressedVersionData::IntentLogPatch(vec![IntentOp {
            schema_epoch: 1,
            footprint: IntentFootprint::empty(),
            op: IntentOpKind::Delete {
                table: TableId::new(1),
                key: RowId::new(1),
            },
        }]));
        let bytes = original.try_to_bytes().unwrap();
        assert_eq!(CompressedPageHistory::from_bytes(&bytes).unwrap(), original);
        let empty = history(CompressedVersionData::IntentLogPatch(Vec::new()));
        assert_eq!(
            CompressedPageHistory::from_bytes(&empty.try_to_bytes().unwrap()).unwrap(),
            empty
        );
    }

    #[test]
    fn roundtrip_does_not_authorize_raw_xor_on_structured_pages() {
        let patch = decode(&encode(&sample()).unwrap()).unwrap();
        assert!(
            patch
                .validate_no_raw_xor_for_structured(fsqlite_types::MergePageKind::BtreeLeafTable)
                .is_err()
        );
    }

    #[test]
    fn structured_patch_wire_has_a_stable_golden_vector() {
        let patch = StructuredPagePatch {
            header_ops: vec![HeaderOp::SetCellCount(2)],
            cell_ops: vec![CellOp {
                cell_key_digest: [0xAA; 16],
                kind: CellOpKind::Delete,
            }],
            free_ops: vec![FreeSpaceOp::AddFreeblock {
                offset: 10,
                size: 20,
            }],
            raw_xor_ranges: Vec::new(),
        };
        let mut expected = b"SPP\x01\x01\0\0\0\0\x02\0\x01\0\0\0".to_vec();
        expected.extend_from_slice(&[0xAA; 16]);
        expected.extend_from_slice(b"\x01\x01\0\0\0\0\x0A\0\x14\0\0\0\0\0");
        assert_eq!(encode(&patch).unwrap(), expected);
        assert_eq!(decode(&expected).unwrap(), patch);
    }

    #[test]
    fn structured_patch_rejects_sqlite_reserved_maximum_child_page() {
        let patch = StructuredPagePatch {
            header_ops: vec![HeaderOp::SetRightMostChild(PageNumber::ONE)],
            ..StructuredPagePatch::default()
        };
        let mut bytes = encode(&patch).unwrap();
        bytes[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&bytes).is_err());
    }
}
