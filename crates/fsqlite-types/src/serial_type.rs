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
}
