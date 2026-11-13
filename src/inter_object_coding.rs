//! Inter-object coding groups for replication catch-up (§3.5.6, `bd-1hi.26`).
//!
//! This module provides deterministic group encoding across multiple ECS
//! objects and reconstruction from any sufficiently informative symbol subset.

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{ObjectId, gf256_inverse_byte, gf256_mul_byte};
use tracing::{debug, error, info, warn};

const INTER_OBJECT_BEAD_ID: &str = "bd-1hi.26";
const INTER_OBJECT_LOGGING_STANDARD: &str = "bd-1fpm";
const ECS_OBJECT_ID_DOMAIN: &[u8] = b"fsqlite:ecs:v1";
const CODING_GROUP_ID_DOMAIN: &[u8] = b"fsqlite:coding-group:v1";
const DEFAULT_REPAIR_OVERHEAD_BPS: u16 = 2_000; // 20%

/// Canonical ECS object payload for coding-group construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcsObject {
    /// Content-addressed identity of `canonical_bytes`.
    pub object_id: ObjectId,
    /// Canonical bytes (wire format) for the object.
    pub canonical_bytes: Vec<u8>,
}

impl EcsObject {
    /// Construct an object from canonical bytes and derive its object id.
    #[must_use]
    pub fn from_canonical(canonical_bytes: Vec<u8>) -> Self {
        let object_id = derive_ecs_object_id(&canonical_bytes);
        Self {
            object_id,
            canonical_bytes,
        }
    }

    /// Construct with explicit object id.
    #[must_use]
    pub const fn with_object_id(object_id: ObjectId, canonical_bytes: Vec<u8>) -> Self {
        Self {
            object_id,
            canonical_bytes,
        }
    }
}

/// Coding-group metadata used by sender and receiver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodingGroup {
    /// Deterministic content-addressed group id.
    pub group_id: ObjectId,
    /// Member object ids in canonical concatenation order.
    pub member_ids: Vec<ObjectId>,
    /// Total concatenated canonical bytes before padding.
    pub total_len: u64,
    /// Individual member byte lengths (for demultiplexing).
    pub member_lens: Vec<u64>,
    /// Number of source symbols in the grouped stream.
    pub k_source: u32,
    /// Symbol size used for the group.
    pub symbol_size: u32,
    /// Number of repair symbols generated for this group.
    pub repair_symbol_count: u32,
}

/// One symbol in a coded group stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSymbol {
    /// Coding group this symbol belongs to.
    pub group_id: ObjectId,
    /// Encoding symbol identifier.
    pub esi: u32,
    /// Symbol payload bytes.
    pub data: Vec<u8>,
    /// Coefficient row over source symbols.
    pub coefficients: Vec<u8>,
}

/// Encoded catch-up batch for replication transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodedCatchupBatch {
    /// Group metadata.
    pub group: CodingGroup,
    /// Streamable symbols.
    pub symbols: Vec<GroupSymbol>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LinearRow {
    coefficients: Vec<u8>,
    payload: Vec<u8>,
}

/// Encode multiple ECS objects into one inter-object coding group with default
/// repair overhead.
///
/// # Errors
///
/// Returns an error when object inputs or symbol size are invalid.
pub fn encode_coding_group(objects: &[EcsObject], symbol_size: u32) -> Result<CodedCatchupBatch> {
    encode_coding_group_with_repair(objects, symbol_size, None)
}

/// Encode multiple ECS objects into one inter-object coding group with an
/// optional explicit repair-symbol count.
///
/// # Errors
///
/// Returns an error when inputs are invalid.
#[allow(clippy::too_many_lines)]
pub fn encode_coding_group_with_repair(
    objects: &[EcsObject],
    symbol_size: u32,
    repair_symbol_count: Option<u32>,
) -> Result<CodedCatchupBatch> {
    #[allow(clippy::too_many_lines)]
    fn inner(
        objects: &[EcsObject],
        symbol_size: u32,
        repair_symbol_count: Option<u32>,
    ) -> Result<CodedCatchupBatch> {
        if objects.is_empty() {
            return Err(FrankenError::OutOfRange {
                what: "coding_group.member_count".to_owned(),
                value: "0".to_owned(),
            });
        }
        if symbol_size == 0 {
            return Err(FrankenError::OutOfRange {
                what: "coding_group.symbol_size".to_owned(),
                value: "0".to_owned(),
            });
        }

        debug!(
            bead_id = INTER_OBJECT_BEAD_ID,
            logging_standard = INTER_OBJECT_LOGGING_STANDARD,
            member_count = objects.len(),
            symbol_size = symbol_size,
            "encoding inter-object coding group"
        );

        let member_ids: Vec<ObjectId> = objects.iter().map(|object| object.object_id).collect();
        let member_lens: Vec<u64> = objects
            .iter()
            .map(|object| u64::try_from(object.canonical_bytes.len()).unwrap_or(u64::MAX))
            .collect();

        let mut concatenated = Vec::new();
        for object in objects {
            concatenated.extend_from_slice(&object.canonical_bytes);
        }
        let total_len =
            u64::try_from(concatenated.len()).map_err(|_| FrankenError::OutOfRange {
                what: "coding_group.total_len".to_owned(),
                value: concatenated.len().to_string(),
            })?;

        let symbol_size_usize =
            usize::try_from(symbol_size).map_err(|_| FrankenError::OutOfRange {
                what: "coding_group.symbol_size".to_owned(),
                value: symbol_size.to_string(),
            })?;
        let k_source_usize = ceil_div_usize(concatenated.len(), symbol_size_usize);
        let k_source = u32::try_from(k_source_usize).map_err(|_| FrankenError::OutOfRange {
            what: "coding_group.k_source".to_owned(),
            value: k_source_usize.to_string(),
        })?;

        if k_source == 0 {
            return Err(FrankenError::OutOfRange {
                what: "coding_group.k_source".to_owned(),
                value: "0".to_owned(),
            });
        }

        let group_id = derive_group_id(&member_ids, &member_lens, total_len, k_source, symbol_size);
        let default_repairs = default_repair_count(k_source);
        let repair_count = repair_symbol_count.unwrap_or(default_repairs);

        let padded_len = k_source_usize
            .checked_mul(symbol_size_usize)
            .ok_or_else(|| FrankenError::OutOfRange {
                what: "coding_group.padded_len".to_owned(),
                value: format!("{k_source_usize}*{symbol_size_usize}"),
            })?;
        concatenated.resize(padded_len, 0);

        let mut source_payloads = Vec::with_capacity(k_source_usize);
        let mut symbols = Vec::with_capacity(
            usize::try_from(k_source.saturating_add(repair_count)).unwrap_or(k_source_usize),
        );

        for source_idx in 0..k_source_usize {
            let start = source_idx * symbol_size_usize;
            let end = start + symbol_size_usize;
            let data = concatenated[start..end].to_vec();
            source_payloads.push(data.clone());
            symbols.push(GroupSymbol {
                group_id,
                esi: u32::try_from(source_idx).unwrap_or(u32::MAX),
                data,
                coefficients: unit_vector(k_source_usize, source_idx),
            });
        }

        for repair_idx in 0..repair_count {
            let esi = k_source.saturating_add(repair_idx);
            let coefficients = deterministic_coefficients(group_id, esi, k_source_usize);
            let data = linear_combine(&source_payloads, &coefficients, symbol_size_usize);
            symbols.push(GroupSymbol {
                group_id,
                esi,
                data,
                coefficients,
            });
        }

        info!(
            bead_id = INTER_OBJECT_BEAD_ID,
            logging_standard = INTER_OBJECT_LOGGING_STANDARD,
            group_id = %group_id,
            member_count = member_ids.len(),
            total_len = total_len,
            k_source = k_source,
            repair_symbol_count = repair_count,
            symbol_count = symbols.len(),
            "inter-object coding group encoded"
        );

        Ok(CodedCatchupBatch {
            group: CodingGroup {
                group_id,
                member_ids,
                total_len,
                member_lens,
                k_source,
                symbol_size,
                repair_symbol_count: repair_count,
            },
            symbols,
        })
    }
    inner(objects, symbol_size, repair_symbol_count)
}

/// Decode a coding group from received symbols.
///
/// # Errors
///
/// Returns an error when symbols are insufficient or inconsistent.
#[allow(clippy::too_many_lines)]
pub fn decode_coding_group(group: &CodingGroup, symbols: &[GroupSymbol]) -> Result<Vec<EcsObject>> {
    #[allow(clippy::too_many_lines)]
    fn inner(group: &CodingGroup, symbols: &[GroupSymbol]) -> Result<Vec<EcsObject>> {
        validate_group(group)?;
        let symbol_size_usize =
            usize::try_from(group.symbol_size).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!("invalid group symbol_size {}", group.symbol_size),
            })?;
        let k_source_usize =
            usize::try_from(group.k_source).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!("invalid group k_source {}", group.k_source),
            })?;

        let mut relevant: Vec<&GroupSymbol> = symbols
            .iter()
            .filter(|symbol| symbol.group_id == group.group_id)
            .collect();
        relevant.sort_by_key(|symbol| symbol.esi);

        if relevant.len() < k_source_usize {
            warn!(
                bead_id = INTER_OBJECT_BEAD_ID,
                logging_standard = INTER_OBJECT_LOGGING_STANDARD,
                group_id = %group.group_id,
                required_symbols = k_source_usize,
                received_symbols = relevant.len(),
                "insufficient symbols for coding-group decode"
            );
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "reason_code=inter_object_decode_insufficient_symbols group_id={} required={} received={}",
                    group.group_id,
                    k_source_usize,
                    relevant.len()
                ),
            });
        }

        debug!(
            bead_id = INTER_OBJECT_BEAD_ID,
            logging_standard = INTER_OBJECT_LOGGING_STANDARD,
            group_id = %group.group_id,
            required_symbols = k_source_usize,
            candidate_symbols = relevant.len(),
            "decoding inter-object coding group"
        );

        let mut rows = Vec::with_capacity(relevant.len());
        for symbol in relevant {
            if symbol.data.len() != symbol_size_usize {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "symbol size mismatch for group {}: expected {}, got {}",
                        group.group_id,
                        symbol_size_usize,
                        symbol.data.len()
                    ),
                });
            }
            let coefficients = if symbol.coefficients.len() == k_source_usize {
                symbol.coefficients.clone()
            } else if symbol.esi < group.k_source {
                unit_vector(
                    k_source_usize,
                    usize::try_from(symbol.esi).unwrap_or(usize::MAX),
                )
            } else {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "missing coefficient row for repair symbol esi={} group_id={}",
                        symbol.esi, group.group_id
                    ),
                });
            };
            rows.push(LinearRow {
                coefficients,
                payload: symbol.data.clone(),
            });
        }

        let source_payloads = solve_source_symbols(rows, k_source_usize, symbol_size_usize)
            .map_err(|decode_error| {
                error!(
                    bead_id = INTER_OBJECT_BEAD_ID,
                    logging_standard = INTER_OBJECT_LOGGING_STANDARD,
                    group_id = %group.group_id,
                    error = %decode_error,
                    "coding-group decode failed"
                );
                decode_error
            })?;

        let mut concatenated =
            Vec::with_capacity(k_source_usize.checked_mul(symbol_size_usize).unwrap_or(0));
        for payload in &source_payloads {
            concatenated.extend_from_slice(payload);
        }
        let total_len_usize =
            usize::try_from(group.total_len).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!("invalid group total_len {}", group.total_len),
            })?;
        if total_len_usize > concatenated.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "decoded payload shorter than total_len: decoded={} total_len={}",
                    concatenated.len(),
                    total_len_usize
                ),
            });
        }
        concatenated.truncate(total_len_usize);

        let mut offset = 0_usize;
        let mut recovered = Vec::with_capacity(group.member_lens.len());
        for (member_idx, member_len) in group.member_lens.iter().enumerate() {
            let member_len =
                usize::try_from(*member_len).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!("invalid member length {}", member_len),
                })?;
            let end = offset.saturating_add(member_len);
            if end > concatenated.len() {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "demultiplex overflow at member {}: offset={} len={} decoded_len={}",
                        member_idx,
                        offset,
                        member_len,
                        concatenated.len()
                    ),
                });
            }
            let object = EcsObject::from_canonical(concatenated[offset..end].to_vec());
            let expected = group.member_ids[member_idx];
            if object.object_id != expected {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "object id mismatch at member {}: expected {}, got {}",
                        member_idx, expected, object.object_id
                    ),
                });
            }
            recovered.push(object);
            offset = end;
        }

        info!(
            bead_id = INTER_OBJECT_BEAD_ID,
            logging_standard = INTER_OBJECT_LOGGING_STANDARD,
            group_id = %group.group_id,
            recovered_objects = recovered.len(),
            "inter-object coding group decoded"
        );

        Ok(recovered)
    }
    inner(group, symbols)
}

/// Build a catch-up batch for replication anti-entropy transfer.
///
/// # Errors
///
/// Returns an error when group encoding fails.
pub fn build_replication_catchup_batch(
    missing_objects: &[EcsObject],
    symbol_size: u32,
) -> Result<CodedCatchupBatch> {
    encode_coding_group(missing_objects, symbol_size)
}

fn validate_group(group: &CodingGroup) -> Result<()> {
    if group.member_ids.is_empty() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "coding group has no members".to_owned(),
        });
    }
    if group.member_ids.len() != group.member_lens.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "member id/length mismatch: ids={} lens={}",
                group.member_ids.len(),
                group.member_lens.len()
            ),
        });
    }
    let total: u64 = group.member_lens.iter().copied().sum();
    if total != group.total_len {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "total_len mismatch: declared={} computed={}",
                group.total_len, total
            ),
        });
    }
    if group.k_source == 0 || group.symbol_size == 0 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "invalid group dimensions: k_source={} symbol_size={}",
                group.k_source, group.symbol_size
            ),
        });
    }
    Ok(())
}

fn solve_source_symbols(
    mut rows: Vec<LinearRow>,
    k_source: usize,
    symbol_size: usize,
) -> Result<Vec<Vec<u8>>> {
    let mut pivot_row = 0_usize;
    for col in 0..k_source {
        let Some(found) = rows
            .iter()
            .enumerate()
            .skip(pivot_row)
            .find(|(_, row)| row.coefficients[col] != 0)
            .map(|(idx, _)| idx)
        else {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "reason_code=inter_object_decode_rank_deficient missing_pivot_col={col}"
                ),
            });
        };

        if found != pivot_row {
            rows.swap(found, pivot_row);
        }

        let pivot = rows[pivot_row].coefficients[col];
        let Some(inv_pivot) = gf256_inverse_byte(pivot) else {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("non-invertible pivot in column {col}: value={pivot}"),
            });
        };

        scale_row(&mut rows[pivot_row], inv_pivot);
        let pivot_snapshot = rows[pivot_row].clone();

        let mut row_idx = 0_usize;
        while row_idx < rows.len() {
            if row_idx == pivot_row {
                row_idx += 1;
                continue;
            }
            let factor = rows[row_idx].coefficients[col];
            if factor == 0 {
                row_idx += 1;
                continue;
            }
            eliminate_row(
                &mut rows[row_idx],
                &pivot_snapshot,
                factor,
                col,
                symbol_size,
            );
            row_idx += 1;
        }

        pivot_row += 1;
        if pivot_row == k_source {
            break;
        }
    }

    if pivot_row < k_source {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "reason_code=inter_object_decode_not_enough_independent_symbols rank={pivot_row} required={k_source}"
            ),
        });
    }

    Ok(rows
        .into_iter()
        .take(k_source)
        .map(|row| row.payload)
        .collect())
}

fn scale_row(row: &mut LinearRow, scalar: u8) {
    for coeff in &mut row.coefficients {
        *coeff = gf256_mul_byte(*coeff, scalar);
    }
    for byte in &mut row.payload {
        *byte = gf256_mul_byte(*byte, scalar);
    }
}

fn eliminate_row(
    target: &mut LinearRow,
    pivot: &LinearRow,
    factor: u8,
    col_start: usize,
    symbol_size: usize,
) {
    for idx in col_start..target.coefficients.len() {
        let scaled = gf256_mul_byte(pivot.coefficients[idx], factor);
        target.coefficients[idx] ^= scaled;
    }
    for idx in 0..symbol_size {
        let scaled = gf256_mul_byte(pivot.payload[idx], factor);
        target.payload[idx] ^= scaled;
    }
}

fn linear_combine(source_payloads: &[Vec<u8>], coefficients: &[u8], symbol_size: usize) -> Vec<u8> {
    let mut out = vec![0_u8; symbol_size];
    for (source, coefficient) in source_payloads.iter().zip(coefficients.iter()) {
        if *coefficient == 0 {
            continue;
        }
        for (dst, src) in out.iter_mut().zip(source.iter()) {
            *dst ^= gf256_mul_byte(*coefficient, *src);
        }
    }
    out
}

fn unit_vector(len: usize, hot_index: usize) -> Vec<u8> {
    let mut out = vec![0_u8; len];
    if hot_index < len {
        out[hot_index] = 1;
    }
    out
}

fn ceil_div_usize(numerator: usize, denominator: usize) -> usize {
    let q = numerator / denominator;
    let r = numerator % denominator;
    if r == 0 { q } else { q + 1 }
}

fn default_repair_count(k_source: u32) -> u32 {
    let k_source_u64 = u64::from(k_source);
    let numerator = k_source_u64.saturating_mul(u64::from(DEFAULT_REPAIR_OVERHEAD_BPS));
    let repair = numerator.div_ceil(10_000);
    u32::try_from(repair.max(1)).unwrap_or(u32::MAX)
}

fn derive_ecs_object_id(canonical_bytes: &[u8]) -> ObjectId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ECS_OBJECT_ID_DOMAIN);
    hasher.update(canonical_bytes);
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    ObjectId::from_bytes(bytes)
}

fn derive_group_id(
    member_ids: &[ObjectId],
    member_lens: &[u64],
    total_len: u64,
    k_source: u32,
    symbol_size: u32,
) -> ObjectId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CODING_GROUP_ID_DOMAIN);
    hasher.update(&total_len.to_le_bytes());
    hasher.update(&k_source.to_le_bytes());
    hasher.update(&symbol_size.to_le_bytes());
    for (member_id, member_len) in member_ids.iter().zip(member_lens.iter()) {
        hasher.update(member_id.as_bytes());
        hasher.update(&member_len.to_le_bytes());
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    ObjectId::from_bytes(bytes)
}

fn deterministic_coefficients(group_id: ObjectId, esi: u32, k_source: usize) -> Vec<u8> {
    let mut seed_buf = [0_u8; 20];
    seed_buf[..16].copy_from_slice(group_id.as_bytes());
    seed_buf[16..].copy_from_slice(&esi.to_le_bytes());
    let mut seed = xxhash_rust::xxh3::xxh3_64(&seed_buf);

    let mut coefficients = Vec::with_capacity(k_source);
    for _ in 0..k_source {
        seed = xorshift64(seed);
        let mut coefficient = seed.to_le_bytes()[0];
        if coefficient == 0 {
            coefficient = 1;
        }
        coefficients.push(coefficient);
    }
    coefficients
}

fn xorshift64(mut value: u64) -> u64 {
    value ^= value << 13;
    value ^= value >> 7;
    value ^ (value << 17)
}

#[cfg(test)]
#[allow(clippy::too_many_lines)]
mod tests {
    use super::*;

    fn make_object(seed: u8, len: usize) -> EcsObject {
        let mut bytes = Vec::with_capacity(len);
        for idx in 0..len {
            let idx_u8 = u8::try_from(idx % 251).unwrap_or(0);
            bytes.push(seed.wrapping_mul(31).wrapping_add(idx_u8));
        }
        EcsObject::from_canonical(bytes)
    }

    fn drop_indices(symbols: &[GroupSymbol], drop: &[usize]) -> Vec<GroupSymbol> {
        symbols
            .iter()
            .enumerate()
            .filter(|(idx, _)| !drop.contains(idx))
            .map(|(_, symbol)| symbol.clone())
            .collect()
    }

    #[test]
    fn test_coding_group_encode_decode() {
        let objects = vec![
            make_object(1, 96),
            make_object(2, 64),
            make_object(3, 48),
            make_object(4, 80),
            make_object(5, 120),
        ];
        let encoded =
            encode_coding_group_with_repair(&objects, 64, Some(6)).expect("encode coding group");
        let decoded = decode_coding_group(&encoded.group, &encoded.symbols).expect("decode");
        assert_eq!(decoded, objects);
    }

    #[test]
    fn test_coding_group_with_loss() {
        let objects = vec![
            make_object(11, 72),
            make_object(12, 53),
            make_object(13, 88),
            make_object(14, 41),
            make_object(15, 97),
        ];
        let encoded =
            encode_coding_group_with_repair(&objects, 48, Some(8)).expect("encode coding group");
        let total = encoded.symbols.len();
        let drop_count = total / 5; // 20%
        let mut drop = Vec::new();
        for idx in 0..drop_count {
            drop.push((idx * 3) % total);
        }
        let received = drop_indices(&encoded.symbols, &drop);
        let decoded = decode_coding_group(&encoded.group, &received).expect("decode with loss");
        assert_eq!(decoded, objects);
    }

    #[test]
    fn test_coding_group_member_verification() {
        let objects = vec![
            make_object(21, 50),
            make_object(22, 70),
            make_object(23, 90),
        ];
        let encoded = encode_coding_group_with_repair(&objects, 32, Some(4)).expect("encode");
        let mut tampered_group = encoded.group.clone();
        tampered_group.member_ids[1] = ObjectId::from_bytes([0xFF; 16]);
        let err = decode_coding_group(&tampered_group, &encoded.symbols).expect_err("must fail");
        assert!(
            err.to_string().contains("object id mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_coding_group_demultiplexing() {
        let objects = vec![
            make_object(31, 3),
            make_object(32, 129),
            make_object(33, 7),
            make_object(34, 48),
        ];
        let encoded = encode_coding_group_with_repair(&objects, 64, Some(6)).expect("encode");
        let decoded = decode_coding_group(&encoded.group, &encoded.symbols).expect("decode");
        let decoded_lens: Vec<usize> = decoded
            .iter()
            .map(|object| object.canonical_bytes.len())
            .collect();
        assert_eq!(decoded_lens, vec![3, 129, 7, 48]);
        assert_eq!(decoded, objects);
    }

    #[test]
    fn test_coding_group_deterministic() {
        let objects = vec![
            make_object(41, 77),
            make_object(42, 88),
            make_object(43, 99),
        ];
        let encoded_a = encode_coding_group_with_repair(&objects, 32, Some(5)).expect("encode a");
        let encoded_b = encode_coding_group_with_repair(&objects, 32, Some(5)).expect("encode b");
        assert_eq!(encoded_a.group, encoded_b.group);
        assert_eq!(encoded_a.symbols, encoded_b.symbols);
    }

    #[test]
    fn test_coding_group_single_object() {
        let objects = vec![make_object(51, 200)];
        let encoded = encode_coding_group_with_repair(&objects, 64, Some(4)).expect("encode");
        let decoded = decode_coding_group(&encoded.group, &encoded.symbols).expect("decode");
        assert_eq!(decoded, objects);
        assert_eq!(encoded.group.member_ids.len(), 1);
    }

    #[test]
    fn prop_coding_group_roundtrip() {
        for object_count in 1_u8..=8 {
            let mut objects = Vec::new();
            for idx in 0..object_count {
                let len = usize::from(idx).saturating_mul(37).saturating_add(11);
                objects.push(make_object(idx.wrapping_add(70), len));
            }
            let encoded =
                encode_coding_group_with_repair(&objects, 48, Some(8)).expect("encode property");
            let decoded = decode_coding_group(&encoded.group, &encoded.symbols).expect("decode");
            assert_eq!(decoded, objects);
        }
    }

    fn run_e2e_replication_catchup() {
        let missing_objects = vec![
            make_object(81, 64),
            make_object(82, 32),
            make_object(83, 96),
            make_object(84, 55),
            make_object(85, 78),
        ];
        let batch = build_replication_catchup_batch(&missing_objects, 40).expect("encode catchup");

        // Simulate lossy replication transport for lagging replica.
        let received = drop_indices(&batch.symbols, &[1, 7]);
        let recovered = decode_coding_group(&batch.group, &received).expect("decode catchup");
        assert_eq!(recovered, missing_objects);
    }

    #[test]
    fn test_e2e_replication_catchup_with_coding_group() {
        run_e2e_replication_catchup();
    }

    #[test]
    fn test_e2e_multicast_coding_group() {
        let objects = vec![
            make_object(91, 64),
            make_object(92, 128),
            make_object(93, 36),
            make_object(94, 72),
            make_object(95, 28),
            make_object(96, 40),
        ];
        let batch = encode_coding_group_with_repair(&objects, 48, Some(9)).expect("encode");

        let replica_a = drop_indices(&batch.symbols, &[1, 5, 9]);
        let replica_b = drop_indices(&batch.symbols, &[0, 3, 8, 11]);
        let replica_c = drop_indices(&batch.symbols, &[2, 4, 6, 10]);

        let decoded_a = decode_coding_group(&batch.group, &replica_a).expect("decode a");
        let decoded_b = decode_coding_group(&batch.group, &replica_b).expect("decode b");
        let decoded_c = decode_coding_group(&batch.group, &replica_c).expect("decode c");

        assert_eq!(decoded_a, objects);
        assert_eq!(decoded_b, objects);
        assert_eq!(decoded_c, objects);
    }

    #[test]
    fn test_bd_1hi_26_unit_compliance_gate() {
        assert_eq!(INTER_OBJECT_BEAD_ID, "bd-1hi.26");
        assert_eq!(INTER_OBJECT_LOGGING_STANDARD, "bd-1fpm");
        let objects = vec![make_object(101, 48), make_object(102, 72)];
        let batch = encode_coding_group(&objects, 32).expect("encode");
        assert!(batch.group.k_source >= 1);
        assert!(!batch.symbols.is_empty());
    }

    #[test]
    fn prop_bd_1hi_26_structure_compliance() {
        for symbol_size in [16_u32, 32, 48, 64, 96, 128] {
            let objects = vec![
                make_object(111, 25),
                make_object(112, 63),
                make_object(113, 91),
            ];
            let batch =
                encode_coding_group_with_repair(&objects, symbol_size, Some(6)).expect("encode");
            assert_eq!(batch.group.member_ids.len(), batch.group.member_lens.len());
            assert_eq!(batch.group.group_id, batch.symbols[0].group_id);
            let recovered = decode_coding_group(&batch.group, &batch.symbols).expect("decode");
            assert_eq!(recovered, objects);
        }
    }

    #[test]
    fn test_e2e_bd_1hi_26_compliance() {
        run_e2e_replication_catchup();
        test_e2e_multicast_coding_group();
    }
}
