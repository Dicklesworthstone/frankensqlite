//! SQLite record format serialization and deserialization.
//!
//! A SQLite record consists of a header followed by data. The header contains
//! the size of the header itself (as a varint) followed by serial type codes
//! (each as a varint) for every column. The data section contains the column
//! values packed sequentially according to their serial types.
//!
//! See: <https://www.sqlite.org/fileformat.html#record_format>

use crate::serial_type::{
    SerialTypeClass, classify_serial_type, read_varint, serial_type_for_blob,
    serial_type_for_integer, serial_type_for_text, serial_type_len, varint_len, write_varint,
};
use crate::value::SqliteValue;

/// Parse a serialized record into a list of `SqliteValue`s.
///
/// The input `data` should be the complete record (header + body).
/// Returns `None` if the record is malformed.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_record(data: &[u8]) -> Option<Vec<SqliteValue>> {
    if data.is_empty() {
        return Some(Vec::new());
    }

    // Read the header size.
    let (header_size_u64, hdr_varint_len) = read_varint(data)?;
    let header_size = header_size_u64 as usize;

    if header_size > data.len() || header_size < hdr_varint_len {
        return None;
    }

    // Parse serial types from the header.
    let mut serial_types = Vec::new();
    let mut offset = hdr_varint_len;
    while offset < header_size {
        let (serial_type, consumed) = read_varint(&data[offset..])?;
        serial_types.push(serial_type);
        offset += consumed;
    }

    // Parse values from the body.
    let mut body_offset = header_size;
    let mut values = Vec::with_capacity(serial_types.len());

    for &st in &serial_types {
        let value_len = serial_type_len(st)? as usize;
        if body_offset + value_len > data.len() {
            return None;
        }

        let value_bytes = &data[body_offset..body_offset + value_len];
        let value = decode_value(st, value_bytes)?;
        values.push(value);
        body_offset += value_len;
    }

    Some(values)
}

/// Serialize a list of `SqliteValue`s into the SQLite record format.
///
/// Returns the complete record bytes (header + body).
pub fn serialize_record(values: &[SqliteValue]) -> Vec<u8> {
    // First pass: compute serial types and header size.
    let serial_types: Vec<u64> = values.iter().map(serial_type_for_value).collect();

    let mut header_content_size: usize = 0;
    for &st in &serial_types {
        header_content_size += varint_len(st);
    }

    // The header size includes the varint encoding of the header size itself.
    // This is a bit circular: the header size varint is part of the header.
    let header_size = compute_header_size(header_content_size);
    let header_size_varint_len = varint_len(header_size as u64);

    // Second pass: compute body size.
    #[allow(clippy::cast_possible_truncation)]
    let body_size: usize = serial_types
        .iter()
        .map(|&st| serial_type_len(st).unwrap_or(0) as usize)
        .sum();

    let total_size = header_size + body_size;
    let mut buf = vec![0u8; total_size];

    // Write header.
    let mut offset = write_varint(&mut buf, header_size as u64);
    debug_assert_eq!(offset, header_size_varint_len);

    for &st in &serial_types {
        offset += write_varint(&mut buf[offset..], st);
    }
    debug_assert_eq!(offset, header_size);

    // Write body.
    #[allow(clippy::cast_possible_truncation)]
    for (value, &st) in values.iter().zip(serial_types.iter()) {
        let value_len = serial_type_len(st).unwrap_or(0) as usize;
        encode_value(value, st, &mut buf[offset..offset + value_len]);
        offset += value_len;
    }

    buf
}

/// Compute the total header size (including the header-size varint itself).
#[allow(clippy::cast_possible_truncation)]
fn compute_header_size(content_size: usize) -> usize {
    // Start with a guess and iterate.
    let mut header_size = content_size + 1; // +1 for the minimum varint
    loop {
        let needed = varint_len(header_size as u64) + content_size;
        if needed <= header_size {
            return header_size;
        }
        header_size = needed;
    }
}

/// Determine the serial type for a `SqliteValue`.
#[allow(clippy::cast_possible_truncation)]
fn serial_type_for_value(value: &SqliteValue) -> u64 {
    match value {
        SqliteValue::Null => 0,
        SqliteValue::Integer(i) => serial_type_for_integer(*i),
        // SQLite normalizes NaN to NULL for deterministic storage.
        SqliteValue::Float(f) => {
            if f.is_nan() {
                0
            } else {
                7
            }
        }
        SqliteValue::Text(s) => serial_type_for_text(s.len() as u64),
        SqliteValue::Blob(b) => serial_type_for_blob(b.len() as u64),
    }
}

/// Decode a value from its serial type and raw bytes.
#[allow(clippy::cast_possible_truncation)]
fn decode_value(serial_type: u64, bytes: &[u8]) -> Option<SqliteValue> {
    match classify_serial_type(serial_type) {
        SerialTypeClass::Null => Some(SqliteValue::Null),
        SerialTypeClass::Zero => Some(SqliteValue::Integer(0)),
        SerialTypeClass::One => Some(SqliteValue::Integer(1)),
        SerialTypeClass::Integer => {
            let value = decode_big_endian_signed(bytes);
            Some(SqliteValue::Integer(value))
        }
        SerialTypeClass::Float => {
            if bytes.len() != 8 {
                return None;
            }
            let bits = u64::from_be_bytes(bytes.try_into().ok()?);
            let value = f64::from_bits(bits);
            if value.is_nan() {
                Some(SqliteValue::Null)
            } else {
                Some(SqliteValue::Float(value))
            }
        }
        SerialTypeClass::Text => {
            let s = std::str::from_utf8(bytes).ok()?;
            Some(SqliteValue::Text(s.to_owned()))
        }
        SerialTypeClass::Blob => Some(SqliteValue::Blob(bytes.to_vec())),
        SerialTypeClass::Reserved => None,
    }
}

/// Decode a big-endian signed integer of 1-8 bytes.
#[allow(clippy::cast_possible_wrap)]
fn decode_big_endian_signed(bytes: &[u8]) -> i64 {
    if bytes.is_empty() {
        return 0;
    }

    // Sign-extend: if the high bit is set, fill with 0xFF.
    let negative = bytes[0] & 0x80 != 0;
    let mut value: u64 = if negative { u64::MAX } else { 0 };

    for &b in bytes {
        value = (value << 8) | u64::from(b);
    }

    value as i64
}

/// Encode a `SqliteValue` into its serial type byte representation.
fn encode_value(value: &SqliteValue, serial_type: u64, buf: &mut [u8]) {
    match value {
        SqliteValue::Null => {} // serial type 0: no data
        SqliteValue::Integer(i) => {
            if classify_serial_type(serial_type) == SerialTypeClass::Integer {
                let len = buf.len();
                let bytes = i.to_be_bytes();
                // Take the least significant `len` bytes.
                buf.copy_from_slice(&bytes[8 - len..]);
            }
            // Zero and One serial types have no data bytes.
        }
        SqliteValue::Float(f) => {
            if f.is_nan() {
                return;
            }
            let bits = f.to_bits();
            buf.copy_from_slice(&bits.to_be_bytes());
        }
        SqliteValue::Text(s) => {
            buf.copy_from_slice(s.as_bytes());
        }
        SqliteValue::Blob(b) => {
            buf.copy_from_slice(b);
        }
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp, clippy::approx_constant)]
mod tests {
    use super::*;

    #[test]
    fn empty_record() {
        let data = serialize_record(&[]);
        assert!(!data.is_empty());
        let values = parse_record(&data).unwrap();
        assert!(values.is_empty());
    }

    #[test]
    fn null_record() {
        let values = vec![SqliteValue::Null];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].is_null());
    }

    #[test]
    fn integer_zero() {
        let values = vec![SqliteValue::Integer(0)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(0));
    }

    #[test]
    fn integer_one() {
        let values = vec![SqliteValue::Integer(1)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(1));
    }

    #[test]
    fn integer_small() {
        let values = vec![SqliteValue::Integer(42)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(42));
    }

    #[test]
    fn integer_negative() {
        let values = vec![SqliteValue::Integer(-1)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(-1));
    }

    #[test]
    fn integer_large() {
        for &val in &[i64::MIN, i64::MAX, 0x7FFF_FFFF_FFFF, -0x7FFF_FFFF_FFFF] {
            let values = vec![SqliteValue::Integer(val)];
            let data = serialize_record(&values);
            let parsed = parse_record(&data).unwrap();
            assert_eq!(
                parsed[0].as_integer(),
                Some(val),
                "roundtrip failed for {val}"
            );
        }
    }

    #[test]
    fn float_value() {
        let values = vec![SqliteValue::Float(3.14)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_float(), Some(3.14));
    }

    #[test]
    fn float_special_values() {
        for &val in &[0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY] {
            let values = vec![SqliteValue::Float(val)];
            let data = serialize_record(&values);
            let parsed = parse_record(&data).unwrap();
            assert_eq!(parsed[0].as_float().unwrap().to_bits(), val.to_bits());
        }
    }

    #[test]
    fn float_nan_normalized_to_null_for_storage() {
        let values = vec![SqliteValue::Float(f64::NAN)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].is_null());
    }

    #[test]
    fn text_value() {
        let values = vec![SqliteValue::Text("hello world".to_owned())];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_text(), Some("hello world"));
    }

    #[test]
    fn text_empty() {
        let values = vec![SqliteValue::Text(String::new())];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_text(), Some(""));
    }

    #[test]
    fn blob_value() {
        let values = vec![SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_blob(), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
    }

    #[test]
    fn blob_empty() {
        let values = vec![SqliteValue::Blob(vec![])];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_blob(), Some(&[][..]));
    }

    #[test]
    fn mixed_record() {
        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::Text("hello".to_owned()),
            SqliteValue::Null,
            SqliteValue::Float(2.718),
            SqliteValue::Blob(vec![1, 2, 3]),
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
        ];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();

        assert_eq!(parsed.len(), 7);
        assert_eq!(parsed[0].as_integer(), Some(42));
        assert_eq!(parsed[1].as_text(), Some("hello"));
        assert!(parsed[2].is_null());
        assert_eq!(parsed[3].as_float(), Some(2.718));
        assert_eq!(parsed[4].as_blob(), Some(&[1, 2, 3][..]));
        assert_eq!(parsed[5].as_integer(), Some(0));
        assert_eq!(parsed[6].as_integer(), Some(1));
    }

    #[test]
    fn test_serial_type_roundtrip_all_categories() {
        let values = vec![
            SqliteValue::Null,
            SqliteValue::Integer(i64::from(i8::MIN)),
            SqliteValue::Integer(i64::from(i16::MIN)),
            SqliteValue::Integer(-8_388_608), // 24-bit boundary
            SqliteValue::Integer(i64::from(i32::MIN)),
            SqliteValue::Integer(-140_737_488_355_328), // 48-bit boundary
            SqliteValue::Integer(i64::MIN),
            SqliteValue::Float(-1234.5),
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
            SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            SqliteValue::Text("serial-type-text".to_owned()),
        ];

        let encoded = serialize_record(&values);
        let parsed = parse_record(&encoded).expect("record must decode");
        assert_eq!(parsed.len(), values.len());

        assert!(parsed[0].is_null());
        assert_eq!(parsed[1].as_integer(), Some(i64::from(i8::MIN)));
        assert_eq!(parsed[2].as_integer(), Some(i64::from(i16::MIN)));
        assert_eq!(parsed[3].as_integer(), Some(-8_388_608));
        assert_eq!(parsed[4].as_integer(), Some(i64::from(i32::MIN)));
        assert_eq!(parsed[5].as_integer(), Some(-140_737_488_355_328));
        assert_eq!(parsed[6].as_integer(), Some(i64::MIN));
        assert_eq!(
            parsed[7].as_float().map(f64::to_bits),
            Some((-1234.5f64).to_bits())
        );
        assert_eq!(parsed[8].as_integer(), Some(0));
        assert_eq!(parsed[9].as_integer(), Some(1));
        assert_eq!(parsed[10].as_blob(), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
        assert_eq!(parsed[11].as_text(), Some("serial-type-text"));
    }

    #[test]
    fn integer_size_boundaries() {
        // Verify that integers at size boundaries roundtrip correctly.
        let test_values: &[i64] = &[
            0,
            1,
            -1,
            127,
            -128,
            128,
            -129,
            32767,
            -32768,
            32768,
            8_388_607,
            -8_388_608,
            8_388_608,
            2_147_483_647,
            -2_147_483_648,
            2_147_483_648,
            0x0000_7FFF_FFFF_FFFF,
            -0x0000_8000_0000_0000,
            0x0000_8000_0000_0000,
            i64::MAX,
            i64::MIN,
        ];

        for &val in test_values {
            let values = vec![SqliteValue::Integer(val)];
            let data = serialize_record(&values);
            let parsed = parse_record(&data).unwrap();
            assert_eq!(
                parsed[0].as_integer(),
                Some(val),
                "roundtrip failed for {val}"
            );
        }
    }

    #[test]
    fn decode_big_endian_signed_cases() {
        assert_eq!(decode_big_endian_signed(&[]), 0);
        assert_eq!(decode_big_endian_signed(&[42]), 42);
        assert_eq!(decode_big_endian_signed(&[0xFF]), -1);
        assert_eq!(decode_big_endian_signed(&[0x00, 0x80]), 128);
        assert_eq!(decode_big_endian_signed(&[0xFF, 0x7F]), -129);
    }

    #[test]
    fn malformed_record_too_short() {
        // Header says it's 10 bytes but we only have 2.
        let data = [10, 0];
        assert!(parse_record(&data).is_none());
    }

    #[test]
    fn malformed_record_body_truncated() {
        // Header has serial type 6 (8-byte int) but body is empty.
        let data = [2, 6]; // header_size=2, serial_type=6, body missing
        assert!(parse_record(&data).is_none());
    }

    #[test]
    fn large_text_roundtrip() {
        let big_text = "x".repeat(10_000);
        let values = vec![SqliteValue::Text(big_text.clone())];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed[0].as_text(), Some(big_text.as_str()));
    }

    #[test]
    fn header_size_computation() {
        // For small records, header size varint is 1 byte.
        assert_eq!(compute_header_size(0), 1);
        assert_eq!(compute_header_size(1), 2);
        assert_eq!(compute_header_size(126), 127);
        // At 127+, the header size varint becomes 2 bytes.
        assert_eq!(compute_header_size(127), 129);
    }

    #[test]
    fn parse_empty_data() {
        let values = parse_record(&[]).unwrap();
        assert!(values.is_empty());
    }

    #[test]
    fn test_record_format_null_vector() {
        let data = serialize_record(&[SqliteValue::Null]);
        assert_eq!(data, vec![0x02, 0x00]);
    }

    #[test]
    fn test_record_format_int8_vector() {
        let data = serialize_record(&[SqliteValue::Integer(42)]);
        assert_eq!(data, vec![0x02, 0x01, 0x2A]);
    }

    #[test]
    fn test_record_format_int16_vector() {
        let data = serialize_record(&[SqliteValue::Integer(256)]);
        assert_eq!(data, vec![0x02, 0x02, 0x01, 0x00]);
    }

    #[test]
    fn test_record_format_int64_vector() {
        let value = 0x0102_0304_0506_0708_i64;
        let data = serialize_record(&[SqliteValue::Integer(value)]);
        assert_eq!(
            data,
            vec![0x02, 0x06, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
    }

    #[test]
    fn test_record_format_float_vector() {
        let data = serialize_record(&[SqliteValue::Float(3.14)]);
        assert_eq!(
            data,
            vec![0x02, 0x07, 0x40, 0x09, 0x1E, 0xB8, 0x51, 0xEB, 0x85, 0x1F]
        );
    }

    #[test]
    fn test_record_format_zero_one_constants_vectors() {
        let zero = serialize_record(&[SqliteValue::Integer(0)]);
        assert_eq!(zero, vec![0x02, 0x08]);

        let one = serialize_record(&[SqliteValue::Integer(1)]);
        assert_eq!(one, vec![0x02, 0x09]);
    }

    #[test]
    fn test_record_format_text_blob_vectors() {
        let text = serialize_record(&[SqliteValue::Text("hello".to_owned())]);
        assert_eq!(text, vec![0x02, 0x17, 0x68, 0x65, 0x6C, 0x6C, 0x6F]);

        let blob = serialize_record(&[SqliteValue::Blob(vec![0xCA, 0xFE])]);
        assert_eq!(blob, vec![0x02, 0x10, 0xCA, 0xFE]);
    }

    #[test]
    fn test_record_format_worked_example_exact_bytes() {
        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::Text("hello".to_owned()),
            SqliteValue::Float(3.14),
            SqliteValue::Null,
            SqliteValue::Blob(vec![0xCA, 0xFE]),
        ];
        let data = serialize_record(&values);
        let expected = vec![
            0x06, 0x01, 0x17, 0x07, 0x00, 0x10, 0x2A, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x40, 0x09,
            0x1E, 0xB8, 0x51, 0xEB, 0x85, 0x1F, 0xCA, 0xFE,
        ];
        assert_eq!(data, expected);
        assert_eq!(data.len(), 22);
    }

    #[test]
    fn test_record_header_size_includes_self_for_one_and_ten_columns() {
        let one_col = serialize_record(&[SqliteValue::Integer(42)]);
        let (one_header_size, one_consumed) = read_varint(&one_col).expect("valid one-col header");
        assert_eq!(one_consumed, 1);
        assert_eq!(one_header_size, 2);

        let ten_values: Vec<SqliteValue> = (2_i64..=11).map(SqliteValue::Integer).collect();
        let ten_col = serialize_record(&ten_values);
        let (ten_header_size, ten_consumed) = read_varint(&ten_col).expect("valid ten-col header");
        assert_eq!(ten_consumed, 1);
        assert_eq!(ten_header_size, 11);
        assert!(ten_col[1..11].iter().all(|&serial| serial == 0x01));
    }

    // -----------------------------------------------------------------------
    // bd-2sm1 §17.2 proptest: record format round-trip
    // -----------------------------------------------------------------------

    use proptest::prelude::*;

    /// Strategy to generate an arbitrary `SqliteValue`.
    fn arb_sqlite_value() -> BoxedStrategy<SqliteValue> {
        prop_oneof![
            5 => Just(SqliteValue::Null),
            10 => any::<i64>().prop_map(SqliteValue::Integer),
            5 => prop_oneof![
                Just(0_i64),
                Just(1_i64),
                Just(-1_i64),
                Just(127_i64),
                Just(-128_i64),
                Just(128_i64),
                Just(32767_i64),
                Just(-32768_i64),
                Just(8_388_607_i64),
                Just(-8_388_608_i64),
                Just(2_147_483_647_i64),
                Just(-2_147_483_648_i64),
                Just(i64::MAX),
                Just(i64::MIN),
            ].prop_map(SqliteValue::Integer),
            // Exclude NaN/Inf since they roundtrip but NaN != NaN in PartialEq.
            5 => (-1e15_f64..1e15_f64).prop_map(SqliteValue::Float),
            5 => prop_oneof![
                Just(0.0_f64),
                Just(-0.0_f64),
                Just(1.0_f64),
                Just(-1.0_f64),
                Just(3.14_f64),
                Just(f64::MAX),
                Just(f64::MIN),
                Just(f64::MIN_POSITIVE),
            ].prop_map(SqliteValue::Float),
            10 => "[a-zA-Z0-9 _]{0,200}".prop_map(SqliteValue::Text),
            5 => proptest::collection::vec(any::<u8>(), 0..200)
                .prop_map(SqliteValue::Blob),
        ]
        .boxed()
    }

    /// Bitwise equality for SqliteValue (handles NaN and -0.0 correctly).
    fn values_bitwise_eq(a: &SqliteValue, b: &SqliteValue) -> bool {
        match (a, b) {
            (SqliteValue::Null, SqliteValue::Null) => true,
            (SqliteValue::Integer(x), SqliteValue::Integer(y)) => x == y,
            (SqliteValue::Float(x), SqliteValue::Float(y)) => x.to_bits() == y.to_bits(),
            (SqliteValue::Text(x), SqliteValue::Text(y)) => x == y,
            (SqliteValue::Blob(x), SqliteValue::Blob(y)) => x == y,
            _ => false,
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(2000))]

        /// INV-PBT-3: Record encode/decode round-trip for arbitrary value vectors.
        #[test]
        fn prop_record_roundtrip_arbitrary(
            values in proptest::collection::vec(arb_sqlite_value(), 0..100)
        ) {
            let encoded = serialize_record(&values);
            let decoded = parse_record(&encoded)
                .expect("serialize_record output must always parse");
            prop_assert_eq!(
                values.len(),
                decoded.len(),
                "bead_id=bd-2sm1 case=record_roundtrip_len_mismatch expected={} got={}",
                values.len(),
                decoded.len()
            );
            for (i, (orig, parsed)) in values.iter().zip(decoded.iter()).enumerate() {
                prop_assert!(
                    values_bitwise_eq(orig, parsed),
                    "bead_id=bd-2sm1 case=record_roundtrip_value_mismatch col={} orig={:?} parsed={:?}",
                    i,
                    orig,
                    parsed
                );
            }
        }

        /// INV-PBT-3: Edge cases at varint encoding boundaries.
        #[test]
        fn prop_record_roundtrip_varint_boundaries(
            n_cols in 1_usize..50
        ) {
            // Use values at varint boundaries to stress header encoding.
            let boundary_values: Vec<i64> = vec![
                0, 1, -1, 127, -128, 128, -129, 32767, -32768, 32768,
                8_388_607, -8_388_608, 2_147_483_647, -2_147_483_648,
                i64::MAX, i64::MIN,
            ];
            let values: Vec<SqliteValue> = (0..n_cols)
                .map(|i| SqliteValue::Integer(boundary_values[i % boundary_values.len()]))
                .collect();
            let encoded = serialize_record(&values);
            let decoded = parse_record(&encoded).expect("valid boundary record");
            prop_assert_eq!(
                values.len(),
                decoded.len(),
                "bead_id=bd-2sm1 case=varint_boundary_len"
            );
            for (i, (orig, parsed)) in values.iter().zip(decoded.iter()).enumerate() {
                prop_assert!(
                    values_bitwise_eq(orig, parsed),
                    "bead_id=bd-2sm1 case=varint_boundary_mismatch col={}",
                    i
                );
            }
        }

        /// INV-PBT-3: Empty and single-value records.
        #[test]
        fn prop_record_roundtrip_single_value(value in arb_sqlite_value()) {
            let values = vec![value];
            let encoded = serialize_record(&values);
            let decoded = parse_record(&encoded).expect("valid single-value record");
            prop_assert_eq!(decoded.len(), 1, "bead_id=bd-2sm1 case=single_value_len");
            prop_assert!(
                values_bitwise_eq(&values[0], &decoded[0]),
                "bead_id=bd-2sm1 case=single_value_mismatch orig={:?} parsed={:?}",
                values[0],
                decoded[0]
            );
        }
    }
}
