//! Canonical endian helpers for on-disk/wire encodings (§1.5, bd-22n.7).
//!
//! SQLite-compatible structures use big-endian integers.
//! FrankenSQLite-native ECS structures use little-endian integers.

#[inline]
#[must_use]
pub fn read_u16_be(src: &[u8]) -> Option<u16> {
    Some(u16::from_be_bytes(src.get(..2)?.try_into().ok()?))
}

#[inline]
#[must_use]
pub fn read_u32_be(src: &[u8]) -> Option<u32> {
    Some(u32::from_be_bytes(src.get(..4)?.try_into().ok()?))
}

#[inline]
#[must_use]
pub fn read_i32_be(src: &[u8]) -> Option<i32> {
    Some(i32::from_be_bytes(src.get(..4)?.try_into().ok()?))
}

#[inline]
#[must_use]
pub fn read_u32_le(src: &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(src.get(..4)?.try_into().ok()?))
}

#[inline]
#[must_use]
pub fn read_u16_le(src: &[u8]) -> Option<u16> {
    Some(u16::from_le_bytes(src.get(..2)?.try_into().ok()?))
}

#[inline]
#[must_use]
pub fn read_u64_le(src: &[u8]) -> Option<u64> {
    Some(u64::from_le_bytes(src.get(..8)?.try_into().ok()?))
}

#[inline]
pub fn write_u16_be(dst: &mut [u8], value: u16) -> Option<()> {
    dst.get_mut(..2)?.copy_from_slice(&value.to_be_bytes());
    Some(())
}

#[inline]
pub fn write_u32_be(dst: &mut [u8], value: u32) -> Option<()> {
    dst.get_mut(..4)?.copy_from_slice(&value.to_be_bytes());
    Some(())
}

#[inline]
pub fn write_i32_be(dst: &mut [u8], value: i32) -> Option<()> {
    dst.get_mut(..4)?.copy_from_slice(&value.to_be_bytes());
    Some(())
}

#[inline]
pub fn write_u32_le(dst: &mut [u8], value: u32) -> Option<()> {
    dst.get_mut(..4)?.copy_from_slice(&value.to_le_bytes());
    Some(())
}

#[inline]
pub fn write_u16_le(dst: &mut [u8], value: u16) -> Option<()> {
    dst.get_mut(..2)?.copy_from_slice(&value.to_le_bytes());
    Some(())
}

#[inline]
pub fn write_u64_le(dst: &mut [u8], value: u64) -> Option<()> {
    dst.get_mut(..8)?.copy_from_slice(&value.to_le_bytes());
    Some(())
}

#[inline]
#[must_use]
pub fn read_u64_be(src: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(src.get(..8)?.try_into().ok()?))
}

#[inline]
pub fn write_u64_be(dst: &mut [u8], value: u64) -> Option<()> {
    dst.get_mut(..8)?.copy_from_slice(&value.to_be_bytes());
    Some(())
}

#[inline]
pub fn append_u16_be(buf: &mut Vec<u8>, value: u16) {
    buf.extend_from_slice(&value.to_be_bytes());
}

#[inline]
pub fn append_u32_be(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

#[inline]
pub fn append_u64_be(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_be_bytes());
}

#[inline]
pub fn append_u32_le(buf: &mut Vec<u8>, value: u32) {
    let mut scratch = [0u8; 4];
    write_u32_le(&mut scratch, value).expect("fixed scratch width");
    buf.extend_from_slice(&scratch);
}

#[inline]
pub fn append_u16_le(buf: &mut Vec<u8>, value: u16) {
    let mut scratch = [0u8; 2];
    write_u16_le(&mut scratch, value).expect("fixed scratch width");
    buf.extend_from_slice(&scratch);
}

#[inline]
pub fn append_u64_le(buf: &mut Vec<u8>, value: u64) {
    let mut scratch = [0u8; 8];
    write_u64_le(&mut scratch, value).expect("fixed scratch width");
    buf.extend_from_slice(&scratch);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ecs::{PatchKind, VersionPointer};
    use crate::{DatabaseHeader, ObjectId};

    #[test]
    fn test_sqlite_structures_big_endian() {
        let header = DatabaseHeader {
            change_counter: 0x0102_0304,
            page_count: 0x1112_1314,
            page_size: crate::PageSize::new(4096).expect("valid page size"),
            ..DatabaseHeader::default()
        };
        let bytes = header.to_bytes().expect("header encodes");

        assert_eq!(read_u16_be(&bytes[16..18]), Some(4096));
        assert_eq!(read_u32_be(&bytes[24..28]), Some(0x0102_0304));
        assert_eq!(read_u32_be(&bytes[28..32]), Some(0x1112_1314));
    }

    #[test]
    fn test_native_ecs_structures_little_endian() {
        let pointer = VersionPointer {
            commit_seq: 0x0102_0304_0506_0708,
            patch_object: ObjectId::from_bytes([7u8; 16]),
            patch_kind: PatchKind::FullImage,
            base_hint: None,
        };
        let bytes = pointer.to_bytes();
        assert_eq!(
            read_u64_le(&bytes[..8]),
            Some(0x0102_0304_0506_0708),
            "version pointer commit_seq must be little-endian"
        );
    }

    #[test]
    fn test_canonical_encoding_unique() {
        let header = DatabaseHeader {
            change_counter: 42,
            page_count: 7,
            ..DatabaseHeader::default()
        };
        let a = header.to_bytes().expect("encodes");
        let b = header.to_bytes().expect("encodes");
        assert_eq!(a, b, "same sqlite header must encode identically");

        let pointer = VersionPointer {
            commit_seq: 99,
            patch_object: ObjectId::from_bytes([3u8; 16]),
            patch_kind: PatchKind::SparseXor,
            base_hint: Some(ObjectId::from_bytes([4u8; 16])),
        };
        assert_eq!(pointer.to_bytes(), pointer.to_bytes());
    }

    #[test]
    fn test_roundtrip_encode_decode() {
        let header = DatabaseHeader {
            change_counter: 123,
            page_count: 456,
            ..DatabaseHeader::default()
        };
        let header_bytes = header.to_bytes().expect("encodes");
        let parsed = DatabaseHeader::from_bytes(&header_bytes).expect("decodes");
        assert_eq!(parsed, header);

        let pointer = VersionPointer {
            commit_seq: 777,
            patch_object: ObjectId::from_bytes([9u8; 16]),
            patch_kind: PatchKind::FullImage,
            base_hint: Some(ObjectId::from_bytes([10u8; 16])),
        };
        let bytes = pointer.to_bytes();
        let decoded = VersionPointer::from_bytes(&bytes).expect("pointer decodes");
        assert_eq!(decoded, pointer);
    }

    #[test]
    fn test_mixed_endian_udp_documented() {
        // Header fields in network-byte-order (big-endian), payload metadata
        // in little-endian is intentional and explicitly encoded via helpers.
        let mut packet = [0u8; 10];
        write_u16_be(&mut packet[0..2], 9443).expect("u16 be");
        write_u16_be(&mut packet[2..4], 9444).expect("u16 be");
        write_u32_le(&mut packet[4..8], 128).expect("u32 le");
        write_u16_be(&mut packet[8..10], 1).expect("u16 be");

        assert_eq!(read_u16_be(&packet[0..2]), Some(9443));
        assert_eq!(read_u16_be(&packet[2..4]), Some(9444));
        assert_eq!(read_u32_le(&packet[4..8]), Some(128));
        assert_eq!(read_u16_be(&packet[8..10]), Some(1));
    }
}
