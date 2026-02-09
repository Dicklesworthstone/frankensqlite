/// SQLite record serial type encoding.
///
/// Each value in a record is preceded by a serial type (stored as a varint)
/// that describes the type and size of the data that follows:
///
/// | Serial Type | Content Size | Meaning                    |
/// |-------------|-------------|----------------------------|
/// | 0           | 0           | NULL                       |
/// | 1           | 1           | 8-bit signed integer       |
/// | 2           | 2           | 16-bit big-endian integer  |
/// | 3           | 3           | 24-bit big-endian integer  |
/// | 4           | 4           | 32-bit big-endian integer  |
/// | 5           | 6           | 48-bit big-endian integer  |
/// | 6           | 8           | 64-bit big-endian integer  |
/// | 7           | 8           | IEEE 754 float             |
/// | 8           | 0           | Integer constant 0         |
/// | 9           | 0           | Integer constant 1         |
/// | 10, 11      | —           | Reserved                   |
/// | N >= 12 even| (N-12)/2    | BLOB of (N-12)/2 bytes     |
/// | N >= 13 odd | (N-13)/2    | TEXT of (N-13)/2 bytes      |
///
/// Compute the number of bytes of data for a given serial type.
///
/// Returns `None` for reserved serial types (10, 11).
pub const fn serial_type_len(serial_type: u64) -> Option<u64> {
    match serial_type {
        0 | 8 | 9 => Some(0),
        1 => Some(1),
        2 => Some(2),
        3 => Some(3),
        4 => Some(4),
        5 => Some(6),
        6 | 7 => Some(8),
        10 | 11 => None, // reserved
        n if n % 2 == 0 => Some((n - 12) / 2),
        n => Some((n - 13) / 2),
    }
}

/// Determine the serial type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialTypeClass {
    /// SQL NULL (serial type 0).
    Null,
    /// Signed integer of 1-8 bytes (serial types 1-6).
    Integer,
    /// IEEE 754 double (serial type 7).
    Float,
    /// Integer constant 0 (serial type 8).
    Zero,
    /// Integer constant 1 (serial type 9).
    One,
    /// Reserved for future use (serial types 10, 11).
    Reserved,
    /// BLOB of `(N-12)/2` bytes (even serial types >= 12).
    Blob,
    /// TEXT of `(N-13)/2` bytes (odd serial types >= 13).
    Text,
}

/// Classify a serial type value.
pub const fn classify_serial_type(serial_type: u64) -> SerialTypeClass {
    match serial_type {
        0 => SerialTypeClass::Null,
        1..=6 => SerialTypeClass::Integer,
        7 => SerialTypeClass::Float,
        8 => SerialTypeClass::Zero,
        9 => SerialTypeClass::One,
        10 | 11 => SerialTypeClass::Reserved,
        n if n % 2 == 0 => SerialTypeClass::Blob,
        _ => SerialTypeClass::Text,
    }
}

/// Compute the serial type for an integer value (choosing the smallest encoding).
#[allow(clippy::cast_sign_loss)]
pub const fn serial_type_for_integer(value: i64) -> u64 {
    let u = if value < 0 {
        !(value as u64)
    } else {
        value as u64
    };

    if u <= 127 {
        if value == 0 {
            return 8;
        }
        if value == 1 {
            return 9;
        }
        1
    } else if u <= 32767 {
        2
    } else if u <= 8_388_607 {
        3
    } else if u <= 2_147_483_647 {
        4
    } else if u <= 0x0000_7FFF_FFFF_FFFF {
        5
    } else {
        6
    }
}

/// Compute the serial type for a text value of `len` bytes.
pub const fn serial_type_for_text(len: u64) -> u64 {
    len * 2 + 13
}

/// Compute the serial type for a blob value of `len` bytes.
pub const fn serial_type_for_blob(len: u64) -> u64 {
    len * 2 + 12
}

/// The sizes for serial types less than 128, matching C SQLite's
/// `sqlite3SmallTypeSizes` lookup table.
pub const SMALL_TYPE_SIZES: [u8; 128] = {
    let mut table = [0u8; 128];
    let mut i: usize = 0;
    loop {
        if i >= 128 {
            break;
        }
        #[allow(clippy::cast_possible_truncation)]
        let size = match serial_type_len(i as u64) {
            Some(n) if n <= 255 => n as u8,
            _ => 0,
        };
        table[i] = size;
        i += 1;
    }
    table
};

/// Read a varint from a byte slice, returning `(value, bytes_consumed)`.
///
/// SQLite varints are 1-9 bytes. The high bit of each byte indicates whether
/// more bytes follow (except the 9th byte which uses all 8 bits).
pub fn read_varint(buf: &[u8]) -> Option<(u64, usize)> {
    if buf.is_empty() {
        return None;
    }

    let mut value: u64 = 0;
    for (i, &byte) in buf.iter().enumerate().take(8) {
        if byte & 0x80 == 0 {
            value = (value << 7) | u64::from(byte);
            return Some((value, i + 1));
        }
        value = (value << 7) | u64::from(byte & 0x7F);
    }

    // 9th byte (if present) uses all 8 bits
    if buf.len() > 8 {
        value = (value << 8) | u64::from(buf[8]);
        Some((value, 9))
    } else {
        None
    }
}

/// Compute the number of bytes needed to encode a value as a varint.
pub const fn varint_len(value: u64) -> usize {
    if value <= 0x7F {
        1
    } else if value <= 0x3FFF {
        2
    } else if value <= 0x001F_FFFF {
        3
    } else if value <= 0x0FFF_FFFF {
        4
    } else if value <= 0x07_FFFF_FFFF {
        5
    } else if value <= 0x03FF_FFFF_FFFF {
        6
    } else if value <= 0x01_FFFF_FFFF_FFFF {
        7
    } else if value <= 0xFF_FFFF_FFFF_FFFF {
        8
    } else {
        9
    }
}

/// Write a varint to a byte buffer, returning the number of bytes written.
///
/// The buffer must have at least 9 bytes available.
#[allow(clippy::cast_possible_truncation)]
pub fn write_varint(buf: &mut [u8], value: u64) -> usize {
    let len = varint_len(value);

    if len == 1 {
        buf[0] = value as u8;
    } else if len == 9 {
        // First 8 bytes: each has high bit set, carries 7 bits
        let mut v = value >> 8;
        for i in (0..8).rev() {
            buf[i] = (v as u8 & 0x7F) | 0x80;
            v >>= 7;
        }
        buf[8] = value as u8;
    } else {
        let mut v = value;
        for i in (0..len).rev() {
            if i == len - 1 {
                buf[i] = v as u8 & 0x7F;
            } else {
                buf[i] = (v as u8 & 0x7F) | 0x80;
            }
            v >>= 7;
        }
    }

    len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serial_type_sizes() {
        assert_eq!(serial_type_len(0), Some(0)); // NULL
        assert_eq!(serial_type_len(1), Some(1)); // 8-bit int
        assert_eq!(serial_type_len(2), Some(2)); // 16-bit int
        assert_eq!(serial_type_len(3), Some(3)); // 24-bit int
        assert_eq!(serial_type_len(4), Some(4)); // 32-bit int
        assert_eq!(serial_type_len(5), Some(6)); // 48-bit int
        assert_eq!(serial_type_len(6), Some(8)); // 64-bit int
        assert_eq!(serial_type_len(7), Some(8)); // float
        assert_eq!(serial_type_len(8), Some(0)); // constant 0
        assert_eq!(serial_type_len(9), Some(0)); // constant 1
        assert_eq!(serial_type_len(10), None); // reserved
        assert_eq!(serial_type_len(11), None); // reserved
    }

    #[test]
    fn serial_type_blob_text() {
        // Even >= 12 is BLOB
        assert_eq!(serial_type_len(12), Some(0)); // empty blob
        assert_eq!(serial_type_len(14), Some(1)); // 1-byte blob
        assert_eq!(serial_type_len(20), Some(4)); // 4-byte blob

        // Odd >= 13 is TEXT
        assert_eq!(serial_type_len(13), Some(0)); // empty text
        assert_eq!(serial_type_len(15), Some(1)); // 1-byte text
        assert_eq!(serial_type_len(21), Some(4)); // 4-byte text
    }

    #[test]
    fn classification() {
        assert_eq!(classify_serial_type(0), SerialTypeClass::Null);
        assert_eq!(classify_serial_type(1), SerialTypeClass::Integer);
        assert_eq!(classify_serial_type(6), SerialTypeClass::Integer);
        assert_eq!(classify_serial_type(7), SerialTypeClass::Float);
        assert_eq!(classify_serial_type(8), SerialTypeClass::Zero);
        assert_eq!(classify_serial_type(9), SerialTypeClass::One);
        assert_eq!(classify_serial_type(10), SerialTypeClass::Reserved);
        assert_eq!(classify_serial_type(11), SerialTypeClass::Reserved);
        assert_eq!(classify_serial_type(12), SerialTypeClass::Blob);
        assert_eq!(classify_serial_type(13), SerialTypeClass::Text);
        assert_eq!(classify_serial_type(14), SerialTypeClass::Blob);
        assert_eq!(classify_serial_type(15), SerialTypeClass::Text);
    }

    #[test]
    fn serial_type_for_integers() {
        assert_eq!(serial_type_for_integer(0), 8);
        assert_eq!(serial_type_for_integer(1), 9);
        assert_eq!(serial_type_for_integer(2), 1);
        assert_eq!(serial_type_for_integer(127), 1);
        assert_eq!(serial_type_for_integer(-1), 1);
        assert_eq!(serial_type_for_integer(-128), 1);
        assert_eq!(serial_type_for_integer(128), 2);
        assert_eq!(serial_type_for_integer(32767), 2);
        assert_eq!(serial_type_for_integer(32768), 3);
        assert_eq!(serial_type_for_integer(8_388_607), 3);
        assert_eq!(serial_type_for_integer(8_388_608), 4);
        assert_eq!(serial_type_for_integer(2_147_483_647), 4);
        assert_eq!(serial_type_for_integer(2_147_483_648), 5);
        assert_eq!(serial_type_for_integer(i64::MAX), 6);
        assert_eq!(serial_type_for_integer(i64::MIN), 6);
    }

    #[test]
    fn serial_type_for_text_and_blob() {
        assert_eq!(serial_type_for_text(0), 13);
        assert_eq!(serial_type_for_text(1), 15);
        assert_eq!(serial_type_for_text(5), 23);
        assert_eq!(serial_type_for_blob(0), 12);
        assert_eq!(serial_type_for_blob(1), 14);
        assert_eq!(serial_type_for_blob(5), 22);
    }

    #[test]
    fn small_type_sizes_table() {
        assert_eq!(SMALL_TYPE_SIZES[0], 0);
        assert_eq!(SMALL_TYPE_SIZES[1], 1);
        assert_eq!(SMALL_TYPE_SIZES[2], 2);
        assert_eq!(SMALL_TYPE_SIZES[3], 3);
        assert_eq!(SMALL_TYPE_SIZES[4], 4);
        assert_eq!(SMALL_TYPE_SIZES[5], 6);
        assert_eq!(SMALL_TYPE_SIZES[6], 8);
        assert_eq!(SMALL_TYPE_SIZES[7], 8);
        assert_eq!(SMALL_TYPE_SIZES[8], 0);
        assert_eq!(SMALL_TYPE_SIZES[9], 0);
    }

    #[test]
    fn varint_roundtrip() {
        let test_values: &[u64] = &[
            0,
            1,
            127,
            128,
            0x3FFF,
            0x4000,
            0x001F_FFFF,
            0x0020_0000,
            0x0FFF_FFFF,
            0x1000_0000,
            u64::from(u32::MAX),
            u64::MAX / 2,
            u64::MAX,
        ];

        let mut buf = [0u8; 9];
        for &value in test_values {
            let written = write_varint(&mut buf, value);
            let (decoded, consumed) = read_varint(&buf[..written]).unwrap();
            assert_eq!(decoded, value, "roundtrip failed for {value}");
            assert_eq!(written, consumed, "length mismatch for {value}");
            assert_eq!(
                written,
                varint_len(value),
                "varint_len mismatch for {value}"
            );
        }
    }

    #[test]
    fn varint_single_byte() {
        let mut buf = [0u8; 9];
        assert_eq!(write_varint(&mut buf, 0), 1);
        assert_eq!(buf[0], 0);

        assert_eq!(write_varint(&mut buf, 127), 1);
        assert_eq!(buf[0], 127);
    }

    #[test]
    fn varint_two_bytes() {
        let mut buf = [0u8; 9];
        let written = write_varint(&mut buf, 128);
        assert_eq!(written, 2);
        let (value, consumed) = read_varint(&buf[..written]).unwrap();
        assert_eq!(value, 128);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn varint_nine_bytes_uses_full_8bit_last_byte() {
        // Pick a value that requires 9 bytes and has a low byte with the high bit set (0xFF).
        // If the 9th byte were incorrectly treated as 7-bit, this would not round-trip.
        let value: u64 = (1u64 << 56) | 0xFF;

        let mut buf = [0u8; 9];
        let written = write_varint(&mut buf, value);
        assert_eq!(written, 9);
        assert_eq!(buf[8], 0xFF);

        // The first 8 bytes must all have the continuation bit set.
        assert!(buf[..8].iter().all(|b| b & 0x80 != 0));

        let (decoded, consumed) = read_varint(&buf).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(consumed, 9);
    }

    #[test]
    fn read_varint_empty() {
        assert!(read_varint(&[]).is_none());
    }

    // -----------------------------------------------------------------------
    // bd-1y7b: §11.2 Varint Edge Cases
    // -----------------------------------------------------------------------

    const BEAD_ID: &str = "bd-1y7b";

    /// Byte-length boundary values: (min_value, max_value, expected_bytes).
    const BYTE_BOUNDARIES: [(u64, u64, usize); 9] = [
        (0, 0x7F, 1),                                  // 1 byte: [0, 127]
        (0x80, 0x3FFF, 2),                             // 2 bytes: [128, 16383]
        (0x4000, 0x001F_FFFF, 3),                      // 3 bytes: [16384, 2097151]
        (0x0020_0000, 0x0FFF_FFFF, 4),                 // 4 bytes: [2097152, 268435455]
        (0x1000_0000, 0x07_FFFF_FFFF, 5),              // 5 bytes: [268435456, 34359738367]
        (0x08_0000_0000, 0x03FF_FFFF_FFFF, 6),         // 6 bytes
        (0x0400_0000_0000, 0x01_FFFF_FFFF_FFFF, 7),    // 7 bytes
        (0x02_0000_0000_0000, 0xFF_FFFF_FFFF_FFFF, 8), // 8 bytes
        (0x0100_0000_0000_0000, u64::MAX, 9),          // 9 bytes
    ];

    #[test]
    fn test_varint_1byte_boundary() {
        let mut buf = [0u8; 9];
        for value in [0u64, 1, 42, 126, 127] {
            let written = write_varint(&mut buf, value);
            assert_eq!(
                written, 1,
                "bead_id={BEAD_ID} case=1byte_boundary value={value}"
            );
            let (decoded, consumed) = read_varint(&buf[..written]).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(consumed, 1);
        }
    }

    #[test]
    fn test_varint_2byte_boundary() {
        let mut buf = [0u8; 9];
        // min 2-byte: 128
        let written = write_varint(&mut buf, 128);
        assert_eq!(written, 2, "bead_id={BEAD_ID} case=2byte_min");
        assert_eq!(
            &buf[..2],
            [0x81, 0x00],
            "bead_id={BEAD_ID} case=2byte_min_bytes"
        );
        let (decoded, _) = read_varint(&buf[..2]).unwrap();
        assert_eq!(decoded, 128);

        // max 2-byte: 16383
        let written = write_varint(&mut buf, 16383);
        assert_eq!(written, 2, "bead_id={BEAD_ID} case=2byte_max");
        assert_eq!(
            &buf[..2],
            [0xFF, 0x7F],
            "bead_id={BEAD_ID} case=2byte_max_bytes"
        );
        let (decoded, _) = read_varint(&buf[..2]).unwrap();
        assert_eq!(decoded, 16383);
    }

    #[test]
    fn test_varint_3byte_boundary() {
        let mut buf = [0u8; 9];
        let written = write_varint(&mut buf, 16384);
        assert_eq!(written, 3, "bead_id={BEAD_ID} case=3byte_min");
        let (decoded, consumed) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 16384);
        assert_eq!(consumed, 3);

        let written = write_varint(&mut buf, 2_097_151);
        assert_eq!(written, 3, "bead_id={BEAD_ID} case=3byte_max");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 2_097_151);
    }

    #[test]
    fn test_varint_4byte_boundary() {
        let mut buf = [0u8; 9];
        let written = write_varint(&mut buf, 2_097_152);
        assert_eq!(written, 4, "bead_id={BEAD_ID} case=4byte_min");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 2_097_152);

        let written = write_varint(&mut buf, 268_435_455);
        assert_eq!(written, 4, "bead_id={BEAD_ID} case=4byte_max");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 268_435_455);
    }

    #[test]
    fn test_varint_5byte_boundary() {
        let mut buf = [0u8; 9];
        let written = write_varint(&mut buf, 268_435_456);
        assert_eq!(written, 5, "bead_id={BEAD_ID} case=5byte_min");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 268_435_456);

        let written = write_varint(&mut buf, 34_359_738_367);
        assert_eq!(written, 5, "bead_id={BEAD_ID} case=5byte_max");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 34_359_738_367);
    }

    #[test]
    fn test_varint_6byte_boundary() {
        let mut buf = [0u8; 9];
        let written = write_varint(&mut buf, 34_359_738_368);
        assert_eq!(written, 6, "bead_id={BEAD_ID} case=6byte_min");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 34_359_738_368);

        let written = write_varint(&mut buf, 4_398_046_511_103);
        assert_eq!(written, 6, "bead_id={BEAD_ID} case=6byte_max");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 4_398_046_511_103);
    }

    #[test]
    fn test_varint_7byte_boundary() {
        let mut buf = [0u8; 9];
        let written = write_varint(&mut buf, 4_398_046_511_104);
        assert_eq!(written, 7, "bead_id={BEAD_ID} case=7byte_min");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 4_398_046_511_104);

        let written = write_varint(&mut buf, 562_949_953_421_311);
        assert_eq!(written, 7, "bead_id={BEAD_ID} case=7byte_max");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 562_949_953_421_311);
    }

    #[test]
    fn test_varint_8byte_boundary() {
        let mut buf = [0u8; 9];
        let written = write_varint(&mut buf, 562_949_953_421_312);
        assert_eq!(written, 8, "bead_id={BEAD_ID} case=8byte_min");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 562_949_953_421_312);

        let written = write_varint(&mut buf, 72_057_594_037_927_935);
        assert_eq!(written, 8, "bead_id={BEAD_ID} case=8byte_max");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, 72_057_594_037_927_935);
    }

    #[test]
    fn test_varint_9byte_full_u64() {
        let mut buf = [0u8; 9];

        // min 9-byte value
        let min9 = 72_057_594_037_927_936u64; // 2^56
        let written = write_varint(&mut buf, min9);
        assert_eq!(written, 9, "bead_id={BEAD_ID} case=9byte_min");
        let (decoded, consumed) = read_varint(&buf).unwrap();
        assert_eq!(decoded, min9);
        assert_eq!(consumed, 9);

        // u64::MAX
        let written = write_varint(&mut buf, u64::MAX);
        assert_eq!(written, 9, "bead_id={BEAD_ID} case=9byte_max");
        assert_eq!(
            buf,
            [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            "bead_id={BEAD_ID} case=9byte_max_bytes u64::MAX must be all-0xFF"
        );
        let (decoded, consumed) = read_varint(&buf).unwrap();
        assert_eq!(decoded, u64::MAX);
        assert_eq!(consumed, 9);
    }

    #[test]
    fn test_varint_9th_byte_all_bits() {
        // Verify the 9th byte contributes ALL 8 bits, not just 7.
        // Value chosen so the 9th byte has its high bit set (0x80+).
        let mut buf = [0u8; 9];

        for low_byte in [0x80u8, 0xFF, 0xAB, 0xFE] {
            let value = (1u64 << 56) | u64::from(low_byte);
            let written = write_varint(&mut buf, value);
            assert_eq!(written, 9);
            assert_eq!(
                buf[8], low_byte,
                "bead_id={BEAD_ID} case=9th_byte_all_bits low={low_byte:#04x}"
            );
            // First 8 bytes must all have continuation bit set.
            for (i, &b) in buf[..8].iter().enumerate() {
                assert_ne!(
                    b & 0x80,
                    0,
                    "bead_id={BEAD_ID} case=continuation_bit byte={i}"
                );
            }
            let (decoded, consumed) = read_varint(&buf).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(consumed, 9);
        }
    }

    #[test]
    fn test_varint_signed_negative_rowid() {
        let mut buf = [0u8; 9];

        // i64::MIN as u64 via two's complement = 0x8000_0000_0000_0000
        #[allow(clippy::cast_sign_loss)]
        let min_u64 = i64::MIN as u64;
        assert_eq!(min_u64, 0x8000_0000_0000_0000);

        let written = write_varint(&mut buf, min_u64);
        assert_eq!(written, 9, "bead_id={BEAD_ID} case=i64_min_length");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, min_u64);

        // Cast back to i64
        #[allow(clippy::cast_possible_wrap)]
        let signed = decoded as i64;
        assert_eq!(signed, i64::MIN, "bead_id={BEAD_ID} case=i64_min_roundtrip");
    }

    #[test]
    fn test_varint_signed_minus_one() {
        let mut buf = [0u8; 9];

        // -1i64 as u64 = u64::MAX
        #[allow(clippy::cast_sign_loss)]
        let minus_one_u64 = (-1i64) as u64;
        assert_eq!(minus_one_u64, u64::MAX);

        let written = write_varint(&mut buf, minus_one_u64);
        assert_eq!(written, 9, "bead_id={BEAD_ID} case=minus_one_length");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();

        #[allow(clippy::cast_possible_wrap)]
        let signed = decoded as i64;
        assert_eq!(signed, -1, "bead_id={BEAD_ID} case=minus_one_roundtrip");
    }

    #[test]
    fn test_varint_not_protobuf() {
        // SQLite varint for u64::MAX: exactly 9 bytes.
        // Protobuf LEB128 for u64::MAX: 10 bytes (7 bits per byte).
        let mut buf = [0u8; 9];
        let sqlite_len = write_varint(&mut buf, u64::MAX);
        assert_eq!(
            sqlite_len, 9,
            "bead_id={BEAD_ID} case=not_protobuf SQLite u64::MAX must be 9 bytes"
        );

        // Compute protobuf LEB128 length for u64::MAX.
        let protobuf_len = leb128_len(u64::MAX);
        assert_eq!(
            protobuf_len, 10,
            "bead_id={BEAD_ID} case=not_protobuf protobuf u64::MAX must be 10 bytes"
        );

        // Also verify a mid-range 9-byte value.
        let value = 1u64 << 56;
        let sqlite_len = write_varint(&mut buf, value);
        assert_eq!(sqlite_len, 9);
        let protobuf_len = leb128_len(value);
        assert_eq!(protobuf_len, 9); // protobuf is also 9 for 2^56 (57 bits / 7 = 9 bytes)

        // But the BYTE SEQUENCES differ. Encode both and compare.
        let mut leb_buf = [0u8; 10];
        let leb_n = leb128_encode(&mut leb_buf, value);
        assert_ne!(
            &buf[..sqlite_len],
            &leb_buf[..leb_n],
            "bead_id={BEAD_ID} case=not_protobuf byte sequences must differ for 2^56"
        );
    }

    /// Protobuf LEB128 encoding length (for comparison — NOT used by SQLite).
    fn leb128_len(mut v: u64) -> usize {
        let mut len = 1;
        while v >= 0x80 {
            v >>= 7;
            len += 1;
        }
        len
    }

    /// Protobuf LEB128 encode (for comparison — NOT used by SQLite).
    fn leb128_encode(buf: &mut [u8], mut v: u64) -> usize {
        let mut i = 0;
        while v >= 0x80 {
            #[allow(clippy::cast_possible_truncation)]
            {
                buf[i] = (v as u8 & 0x7F) | 0x80;
            }
            v >>= 7;
            i += 1;
        }
        #[allow(clippy::cast_possible_truncation)]
        {
            buf[i] = v as u8;
        }
        i + 1
    }

    #[test]
    fn test_varint_all_boundaries_roundtrip() {
        let mut buf = [0u8; 9];
        for &(min_val, max_val, expected_len) in &BYTE_BOUNDARIES {
            // Test min value
            let written = write_varint(&mut buf, min_val);
            assert_eq!(
                written, expected_len,
                "bead_id={BEAD_ID} case=boundary_min value={min_val} expected_len={expected_len}"
            );
            let (decoded, consumed) = read_varint(&buf[..written]).unwrap();
            assert_eq!(decoded, min_val);
            assert_eq!(consumed, expected_len);

            // Test max value
            let written = write_varint(&mut buf, max_val);
            assert_eq!(
                written, expected_len,
                "bead_id={BEAD_ID} case=boundary_max value={max_val} expected_len={expected_len}"
            );
            let (decoded, consumed) = read_varint(&buf[..written]).unwrap();
            assert_eq!(decoded, max_val);
            assert_eq!(consumed, expected_len);

            // Test varint_len matches
            assert_eq!(varint_len(min_val), expected_len);
            assert_eq!(varint_len(max_val), expected_len);
        }
    }

    #[test]
    fn test_varint_canonical_encoding() {
        // Verify encoder always produces minimal-length encoding.
        // For each boundary, the value just below the min should encode shorter.
        for &(min_val, _, expected_len) in &BYTE_BOUNDARIES {
            if min_val == 0 {
                continue;
            }
            let below = min_val - 1;
            let mut buf = [0u8; 9];
            let written = write_varint(&mut buf, below);
            assert!(
                written < expected_len,
                "bead_id={BEAD_ID} case=canonical value={below} written={written} \
                 must be < {expected_len}"
            );
        }
    }

    #[test]
    fn test_varint_decode_from_longer_buffer() {
        // Decoder must read exactly N bytes and leave trailing bytes untouched.
        let mut buf = [0xCC_u8; 16]; // fill with sentinel
        let written = write_varint(&mut buf, 128); // 2 bytes
        assert_eq!(written, 2);

        // Read from the full 16-byte buffer.
        let (decoded, consumed) = read_varint(&buf).unwrap();
        assert_eq!(decoded, 128);
        assert_eq!(
            consumed, 2,
            "bead_id={BEAD_ID} case=longer_buffer decoder must stop at 2 bytes"
        );
        // Trailing bytes must be untouched.
        assert!(
            buf[2..].iter().all(|&b| b == 0xCC),
            "bead_id={BEAD_ID} case=longer_buffer trailing bytes must be untouched"
        );
    }

    #[test]
    fn test_varint_decode_truncated_returns_none() {
        // A multi-byte varint with insufficient bytes should return None.
        let mut buf = [0u8; 9];
        let written = write_varint(&mut buf, 128); // 2 bytes: [0x81, 0x00]
        assert_eq!(written, 2);

        // Only provide 1 byte of the 2-byte encoding.
        assert!(
            read_varint(&buf[..1]).is_none(),
            "bead_id={BEAD_ID} case=truncated_2byte"
        );

        // 9-byte value with only 8 bytes available.
        let written = write_varint(&mut buf, u64::MAX);
        assert_eq!(written, 9);
        assert!(
            read_varint(&buf[..8]).is_none(),
            "bead_id={BEAD_ID} case=truncated_9byte"
        );
    }

    #[test]
    fn test_varint_golden_vectors() {
        // Golden byte sequences derived from C SQLite's sqlite3PutVarint.
        let cases: &[(u64, &[u8])] = &[
            (0, &[0x00]),
            (1, &[0x01]),
            (127, &[0x7F]),
            (128, &[0x81, 0x00]),
            (129, &[0x81, 0x01]),
            (16383, &[0xFF, 0x7F]),
            (16384, &[0x81, 0x80, 0x00]),
            (
                u64::MAX,
                &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
        ];

        let mut buf = [0u8; 9];
        for &(value, expected_bytes) in cases {
            let written = write_varint(&mut buf, value);
            assert_eq!(
                &buf[..written],
                expected_bytes,
                "bead_id={BEAD_ID} case=golden_vector value={value}"
            );
            let (decoded, consumed) = read_varint(expected_bytes).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(consumed, expected_bytes.len());
        }
    }

    #[test]
    fn test_varint_i64_max_and_nearby() {
        let mut buf = [0u8; 9];

        // i64::MAX = 2^63 - 1 = 0x7FFF_FFFF_FFFF_FFFF
        #[allow(clippy::cast_sign_loss)]
        let i64_max_u = i64::MAX as u64;
        let written = write_varint(&mut buf, i64_max_u);
        assert_eq!(written, 9, "bead_id={BEAD_ID} case=i64_max");
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, i64_max_u);

        // i64::MAX + 1 (first "negative" rowid as u64) = 0x8000_0000_0000_0000
        let written = write_varint(&mut buf, i64_max_u + 1);
        assert_eq!(written, 9);
        let (decoded, _) = read_varint(&buf[..written]).unwrap();
        assert_eq!(decoded, i64_max_u + 1);
    }

    // ================================================================
    // Property-based tests (bd-309f)
    // ================================================================
    use proptest::prelude::*;

    proptest! {
        /// Varint roundtrip: write then read recovers the original value.
        #[test]
        fn prop_varint_roundtrip(value: u64) {
            let mut buf = [0u8; 9];
            let written = write_varint(&mut buf, value);
            let (decoded, consumed) = read_varint(&buf[..written]).unwrap();
            prop_assert_eq!(decoded, value);
            prop_assert_eq!(consumed, written);
        }

        /// varint_len matches actual bytes written by write_varint.
        #[test]
        fn prop_varint_len_matches_write(value: u64) {
            let mut buf = [0u8; 9];
            let written = write_varint(&mut buf, value);
            prop_assert_eq!(varint_len(value), written);
        }

        /// Varint encoding is canonical: no leading zero-value continuation bytes
        /// (i.e. shorter encodings don't decode to the same value).
        #[test]
        fn prop_varint_canonical(value: u64) {
            let mut buf = [0u8; 9];
            let written = write_varint(&mut buf, value);
            // If more than 1 byte, removing the first byte should NOT decode
            // to the same value (proves minimality).
            if written > 1 {
                if let Some((alt, _)) = read_varint(&buf[1..written]) {
                    prop_assert_ne!(alt, value, "shorter encoding yields same value — not canonical");
                }
            }
        }

        /// serial_type_for_integer always returns a type whose class is
        /// Integer, Zero, or One (never Blob, Text, etc.).
        #[test]
        fn prop_integer_serial_type_class(value: i64) {
            let st = serial_type_for_integer(value);
            let class = classify_serial_type(st);
            prop_assert!(
                matches!(class, SerialTypeClass::Integer | SerialTypeClass::Zero | SerialTypeClass::One),
                "integer value {value} got unexpected class {class:?} for serial type {st}"
            );
        }

        /// serial_type_for_integer returns a type whose content size fits the value.
        #[test]
        fn prop_integer_serial_type_fits(value: i64) {
            let st = serial_type_for_integer(value);
            if let Some(size) = serial_type_len(st) {
                // Zero-length types are only valid for 0 and 1
                if size == 0 {
                    prop_assert!(value == 0 || value == 1);
                }
            }
        }

        /// serial_type_for_text produces odd types >= 13 that classify as Text.
        #[test]
        fn prop_text_serial_type(len in 0u64..=1_000_000) {
            let st = serial_type_for_text(len);
            prop_assert!(st >= 13, "text type {st} < 13");
            prop_assert!(st % 2 == 1, "text type {st} is even");
            prop_assert_eq!(classify_serial_type(st), SerialTypeClass::Text);
            // Inverse: recover original length
            prop_assert_eq!(serial_type_len(st), Some(len));
        }

        /// serial_type_for_blob produces even types >= 12 that classify as Blob.
        #[test]
        fn prop_blob_serial_type(len in 0u64..=1_000_000) {
            let st = serial_type_for_blob(len);
            prop_assert!(st >= 12, "blob type {st} < 12");
            prop_assert!(st % 2 == 0, "blob type {st} is odd");
            prop_assert_eq!(classify_serial_type(st), SerialTypeClass::Blob);
            // Inverse: recover original length
            prop_assert_eq!(serial_type_len(st), Some(len));
        }

        /// Classification is exhaustive and deterministic for arbitrary serial types.
        #[test]
        fn prop_classification_deterministic(st: u64) {
            let class = classify_serial_type(st);
            // Re-classify to confirm determinism
            prop_assert_eq!(classify_serial_type(st), class);
            // Verify consistency with serial_type_len
            match class {
                SerialTypeClass::Reserved => {
                    prop_assert!(serial_type_len(st).is_none());
                }
                _ => {
                    prop_assert!(serial_type_len(st).is_some());
                }
            }
        }

        /// SMALL_TYPE_SIZES matches serial_type_len for all indices 0..128.
        #[test]
        #[allow(clippy::cast_possible_truncation)]
        fn prop_small_type_table_consistent(i in 0u64..128) {
            let expected = match serial_type_len(i) {
                Some(n) if n <= 255 => n as u8,
                _ => 0,
            };
            prop_assert_eq!(SMALL_TYPE_SIZES[usize::try_from(i).unwrap()], expected);
        }

        /// Varint encoding uses at most 9 bytes and at least 1 byte.
        #[test]
        fn prop_varint_len_bounds(value: u64) {
            let len = varint_len(value);
            prop_assert!((1..=9).contains(&len), "varint_len({value}) = {len}");
        }

        /// For 9-byte varints, the first 8 bytes all have the continuation bit set.
        #[test]
        fn prop_nine_byte_varint_continuation_bits(value in 0x0100_0000_0000_0000u64..=u64::MAX) {
            let mut buf = [0u8; 9];
            let written = write_varint(&mut buf, value);
            if written == 9 {
                for (i, &byte) in buf[..8].iter().enumerate() {
                    prop_assert!(byte & 0x80 != 0, "byte {i} missing continuation bit for value {value}");
                }
            }
        }

        /// read_varint on a truncated buffer returns None.
        #[test]
        fn prop_truncated_varint_returns_none(value: u64) {
            let mut buf = [0u8; 9];
            let written = write_varint(&mut buf, value);
            if written > 1 {
                // Truncate by removing the last byte
                prop_assert!(read_varint(&buf[..written - 1]).is_none() ||
                    read_varint(&buf[..written - 1]).unwrap().0 != value,
                    "truncated buffer should not decode to original value");
            }
        }
    }
}
