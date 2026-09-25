//! Opt-in, byte-exact page-history archives with bounded reverse-delta chains.
//!
//! This does not change the legacy history format or enable a storage backend.
//! XOR here encodes two known committed images; it never merges concurrent writes.
//! A full image every 32 versions bounds random snapshot reconstruction to at
//! most 31 deltas. Every reconstructed image is checked against its BLAKE3 hash.
//!
//! FPH1 wire layout (integers little-endian): `FPH\x01`, page number u32,
//! page size u32, version count u32; then each entry's sequence u64, tag u8
//! (0=full, 1=reverse-XOR), payload length u32, image hash [u8; 32], payload.
//! A domain-separated BLAKE3 checksum covers the entire preceding archive.
//! Image hashes bind page number, commit sequence and the exact original bytes.
//! The archive is limited to 4096 versions and 64 MiB; pages are 512..65536
//! bytes with power-of-two size. Decoding validates every retained version.

use super::{
    CompressedPageHistory, CompressedPageVersion, CompressedVersionData, HistoryCompressionError,
};
use crate::xor_delta::{DeltaThresholdConfig, decode_sparse_xor_delta, encode_sparse_xor_delta};
use fsqlite_types::{CommitSeq, PageNumber};

const MAGIC: &[u8; 4] = b"FPH\x01";
const HEADER_BYTES: usize = 16;
const ENTRY_BYTES: usize = 45;
const HASH_BYTES: usize = 32;
const MAX_ARCHIVE_BYTES: usize = 64 * 1024 * 1024;
const MAX_VERSIONS: usize = 4096;
const MAX_DELTA_DEPTH: usize = 31;
const IMAGE_DOMAIN: &[u8] = b"fsqlite:page-history:image:v1\0";
const ARCHIVE_DOMAIN: &[u8] = b"fsqlite:page-history:archive:v1\0";

type Result<T> = std::result::Result<T, HistoryCompressionError>;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Payload {
    Full(Vec<u8>),
    Xor(Vec<u8>),
}

impl Payload {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Full(bytes) | Self::Xor(bytes) => bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Version {
    seq: CommitSeq,
    hash: [u8; HASH_BYTES],
    payload: Payload,
    anchor: usize,
}

/// An immutable validated archive of one page's retained committed versions.
///
/// Callers must opt in to this format and keep its ECS object identity outside
/// the archive. Embedded hashes detect corruption, not malicious replacement.
/// Decoding uses bounded per-page working memory, not a materialized version chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactPageHistory {
    pgno: PageNumber,
    page_len: usize,
    versions: Vec<Version>,
    encoded_len: usize,
}

fn invalid(message: &str) -> HistoryCompressionError {
    HistoryCompressionError::DecodeError(format!("compact history: {message}"))
}

fn valid_page_len(length: usize) -> bool {
    (512..=65_536).contains(&length) && length.is_power_of_two()
}

fn wire_length(length: usize) -> Result<u32> {
    u32::try_from(length).map_err(|_| invalid("length exceeds u32"))
}

fn image_hash(pgno: PageNumber, seq: CommitSeq, bytes: &[u8]) -> [u8; HASH_BYTES] {
    let mut hash = blake3::Hasher::new();
    hash.update(IMAGE_DOMAIN);
    hash.update(&pgno.get().to_le_bytes());
    hash.update(&seq.get().to_le_bytes());
    hash.update(bytes);
    *hash.finalize().as_bytes()
}

fn archive_hash(bytes: &[u8]) -> [u8; HASH_BYTES] {
    let mut hash = blake3::Hasher::new();
    hash.update(ARCHIVE_DOMAIN);
    hash.update(bytes);
    *hash.finalize().as_bytes()
}

impl CompactPageHistory {
    /// Compress newest-first images using the default 25% minimum delta saving.
    ///
    /// # Errors
    ///
    /// Rejects empty/unordered histories, inconsistent or invalid `SQLite` page
    /// sizes, more than 4096 versions, or archives exceeding 64 MiB.
    pub fn compress(pgno: PageNumber, images: &[(CommitSeq, Vec<u8>)]) -> Result<Self> {
        Self::compress_with_threshold(pgno, images, DeltaThresholdConfig::default())
    }

    /// Compress with a validated minimum saving on the complete encoded entry.
    ///
    /// The decision uses actual sparse-run bytes plus entry metadata, not the
    /// nonzero-byte estimator. Fragmented or dense changes fall back to full
    /// images. Full anchors reset dependency depth even when a delta is smaller.
    ///
    /// # Errors
    ///
    /// Returns the same validation failures as [`Self::compress`].
    pub fn compress_with_threshold(
        pgno: PageNumber,
        images: &[(CommitSeq, Vec<u8>)],
        config: DeltaThresholdConfig,
    ) -> Result<Self> {
        let Some((_, newest)) = images.first() else {
            return Err(HistoryCompressionError::EmptyHistory);
        };
        if images.len() > MAX_VERSIONS || !valid_page_len(newest.len()) {
            return Err(invalid("invalid version count or page size"));
        }
        if images.iter().any(|(_, image)| image.len() != newest.len())
            || images
                .windows(2)
                .any(|pair| pair[0].0.get() <= pair[1].0.get())
        {
            return Err(invalid(
                "images must have equal sizes and strictly descending sequences",
            ));
        }
        let mut versions = Vec::with_capacity(images.len());
        let mut encoded_len = HEADER_BYTES + HASH_BYTES;
        let mut anchor = 0;
        for (index, (seq, image)) in images.iter().enumerate() {
            let delta = if index > 0 && index - anchor <= MAX_DELTA_DEPTH {
                Some(
                    encode_sparse_xor_delta(&images[index - 1].1, image)
                        .map_err(|error| invalid(&error.to_string()))?,
                )
            } else {
                None
            };
            let payload = match delta {
                Some(bytes)
                    if (bytes.len() + ENTRY_BYTES) * 100
                        <= (image.len() + ENTRY_BYTES)
                            * usize::from(100 - config.threshold_pct()) =>
                {
                    Payload::Xor(bytes)
                }
                _ => {
                    anchor = index;
                    Payload::Full(image.clone())
                }
            };
            encoded_len += ENTRY_BYTES + payload.bytes().len();
            if encoded_len > MAX_ARCHIVE_BYTES {
                return Err(invalid("archive exceeds 64 MiB"));
            }
            versions.push(Version {
                seq: *seq,
                hash: image_hash(pgno, *seq, image),
                payload,
                anchor,
            });
        }
        Ok(Self {
            pgno,
            page_len: newest.len(),
            versions,
            encoded_len,
        })
    }

    /// The page number bound into every image hash.
    #[must_use]
    pub const fn page_number(&self) -> PageNumber {
        self.pgno
    }

    /// Number of retained versions, including full anchors.
    #[must_use]
    pub const fn version_count(&self) -> usize {
        self.versions.len()
    }

    /// Complete archive size including headers, per-image hashes and checksum.
    /// This is encoded storage size, not allocator or process memory usage.
    #[must_use]
    pub const fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    /// Number of versions whose payload is a reverse sparse-XOR delta.
    #[must_use]
    pub fn delta_count(&self) -> usize {
        self.versions
            .iter()
            .filter(|entry| matches!(entry.payload, Payload::Xor(_)))
            .count()
    }

    /// Reconstruct the newest retained version at or before `snapshot`.
    ///
    /// A snapshot older than the retained floor returns `None`, not the newest
    /// image. Only the nearest full anchor and at most 31 deltas are decoded;
    /// the complete history is never materialized by this operation.
    ///
    /// # Errors
    ///
    /// Returns an error if a required delta or reconstructed image is invalid.
    pub fn reconstruct(&self, snapshot: CommitSeq) -> Result<Option<(CommitSeq, Vec<u8>)>> {
        let index = self
            .versions
            .partition_point(|version| version.seq.get() > snapshot.get());
        let Some(version) = self.versions.get(index) else {
            return Ok(None);
        };
        let mut image = Vec::new();
        for entry in &self.versions[version.anchor..=index] {
            image = self.materialize(entry, &image)?;
        }
        Ok(Some((version.seq, image)))
    }

    /// Explicitly materialize every retained image into the legacy representation.
    /// Unlike [`Self::reconstruct`], this allocates all returned page images.
    ///
    /// # Errors
    ///
    /// Returns an error on invalid deltas or an image-hash mismatch.
    pub fn decompress(&self) -> Result<CompressedPageHistory> {
        let mut versions = Vec::with_capacity(self.versions.len());
        let mut image = Vec::new();
        for entry in &self.versions {
            image = self.materialize(entry, &image)?;
            versions.push(CompressedPageVersion {
                commit_seq: entry.seq,
                data: CompressedVersionData::FullImage(image.clone()),
            });
        }
        Ok(CompressedPageHistory {
            pgno: self.pgno,
            versions,
        })
    }

    fn materialize(&self, entry: &Version, newer: &[u8]) -> Result<Vec<u8>> {
        let image = match &entry.payload {
            Payload::Full(bytes) => bytes.clone(),
            Payload::Xor(bytes) => decode_sparse_xor_delta(newer, bytes)
                .map_err(|error| invalid(&error.to_string()))?,
        };
        if image.len() != self.page_len || image_hash(self.pgno, entry.seq, &image) != entry.hash {
            return Err(invalid("reconstructed image hash mismatch"));
        }
        Ok(image)
    }

    /// Encode FPH1: header, length-delimited entries, then a BLAKE3 checksum.
    /// Old legacy readers do not understand this explicitly selected format.
    ///
    /// # Errors
    ///
    /// Returns an error if allocation fails or internal lengths are inconsistent.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        if self.encoded_len > MAX_ARCHIVE_BYTES {
            return Err(invalid("archive exceeds 64 MiB"));
        }
        let mut out = Vec::new();
        out.try_reserve_exact(self.encoded_len)
            .map_err(|_| invalid("allocation failed"))?;
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.pgno.get().to_le_bytes());
        out.extend_from_slice(&wire_length(self.page_len)?.to_le_bytes());
        out.extend_from_slice(&wire_length(self.versions.len())?.to_le_bytes());
        for entry in &self.versions {
            out.extend_from_slice(&entry.seq.get().to_le_bytes());
            out.push(u8::from(matches!(entry.payload, Payload::Xor(_))));
            out.extend_from_slice(&wire_length(entry.payload.bytes().len())?.to_le_bytes());
            out.extend_from_slice(&entry.hash);
            out.extend_from_slice(entry.payload.bytes());
        }
        if out.len() + HASH_BYTES != self.encoded_len {
            return Err(invalid("inconsistent encoded length"));
        }
        let checksum = archive_hash(&out);
        out.extend_from_slice(&checksum);
        Ok(out)
    }

    /// Decode and verify the complete archive before returning a usable history.
    ///
    /// Only bounded per-page working memory is used during validation. Version
    /// counts, page sizes, wire bytes and delta dependency depth are bounded.
    /// Every delta must have the production encoder's canonical representation.
    ///
    /// # Errors
    ///
    /// Rejects truncation, trailing bytes, unsupported formats, bad checksums,
    /// invalid sequences/lengths/tags, noncanonical deltas and wrong image hashes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_BYTES + HASH_BYTES || bytes.len() > MAX_ARCHIVE_BYTES {
            return Err(invalid("invalid archive length"));
        }
        let (body, checksum) = bytes.split_at(bytes.len() - HASH_BYTES);
        if archive_hash(body).as_slice() != checksum {
            return Err(invalid("archive checksum mismatch"));
        }
        let mut input = Reader(body);
        if input.take(4)? != MAGIC {
            return Err(invalid("unsupported format/version"));
        }
        let pgno = PageNumber::new(input.u32()?).ok_or_else(|| invalid("invalid page number"))?;
        let page_len = input.length()?;
        let count = input.length()?;
        if !valid_page_len(page_len)
            || count == 0
            || count > MAX_VERSIONS
            || count > input.0.len() / (ENTRY_BYTES + 8)
        {
            return Err(invalid("invalid page size or impossible version count"));
        }
        let mut versions: Vec<Version> = Vec::with_capacity(count);
        let mut image = Vec::new();
        let mut anchor = 0;
        for index in 0..count {
            let seq = CommitSeq::new(u64::from_le_bytes(input.array()?));
            if versions
                .last()
                .is_some_and(|previous| previous.seq.get() <= seq.get())
            {
                return Err(invalid("sequences are not strictly descending"));
            }
            let tag = input.array::<1>()?[0];
            let length = input.length()?;
            let hash = input.array()?;
            let payload_bytes = input.take(length)?;
            let payload = match tag {
                0 if length == page_len => {
                    anchor = index;
                    image = payload_bytes.to_vec();
                    Payload::Full(payload_bytes.to_vec())
                }
                1 if index > 0 && index - anchor <= MAX_DELTA_DEPTH && length < page_len => {
                    let reconstructed = decode_sparse_xor_delta(&image, payload_bytes)
                        .map_err(|error| invalid(&error.to_string()))?;
                    let canonical = encode_sparse_xor_delta(&image, &reconstructed)
                        .map_err(|error| invalid(&error.to_string()))?;
                    if canonical != payload_bytes {
                        return Err(invalid("noncanonical sparse delta"));
                    }
                    image = reconstructed;
                    Payload::Xor(payload_bytes.to_vec())
                }
                _ => return Err(invalid("invalid entry tag, payload length or delta chain")),
            };
            if image_hash(pgno, seq, &image) != hash {
                return Err(invalid("reconstructed image hash mismatch"));
            }
            versions.push(Version {
                seq,
                hash,
                payload,
                anchor,
            });
        }
        if !input.0.is_empty() {
            return Err(invalid("trailing bytes"));
        }
        Ok(Self {
            pgno,
            page_len,
            versions,
            encoded_len: bytes.len(),
        })
    }
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
            .map_err(|_| invalid("truncated field"))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn length(&mut self) -> Result<usize> {
        usize::try_from(self.u32()?).map_err(|_| invalid("length exceeds address space"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hot_images(count: u64, size: usize) -> Vec<(CommitSeq, Vec<u8>)> {
        (1..=count)
            .rev()
            .map(|seq| {
                let mut bytes = vec![0; size];
                if size >= 8 {
                    bytes[..8].copy_from_slice(&seq.to_le_bytes());
                }
                (CommitSeq::new(seq * 10), bytes)
            })
            .collect()
    }

    fn resign(bytes: &mut [u8]) {
        let end = bytes.len() - HASH_BYTES;
        let hash = archive_hash(&bytes[..end]);
        bytes[end..].copy_from_slice(&hash);
    }

    #[test]
    fn hot_history_compresses_and_reconstructs_every_retained_snapshot() {
        let images = hot_images(100, 4096);
        let compressed = CompactPageHistory::compress(PageNumber::ONE, &images).unwrap();
        assert_eq!(compressed.version_count(), 100);
        assert_eq!(compressed.delta_count(), 96);
        assert!(compressed.encoded_len() < 100 * 4096 / 10);
        let decoded = CompactPageHistory::from_bytes(&compressed.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, compressed);
        for snapshot in 0..=1010 {
            let expected = images
                .iter()
                .find(|(seq, _)| seq.get() <= snapshot)
                .cloned();
            assert_eq!(
                decoded.reconstruct(CommitSeq::new(snapshot)).unwrap(),
                expected
            );
        }
        assert_eq!(decoded.page_number(), PageNumber::ONE);
        assert_eq!(
            decoded.decompress().unwrap(),
            super::super::compress_page_history(PageNumber::ONE, &images).unwrap()
        );
    }

    #[test]
    fn every_sqlite_page_size_including_last_byte_of_64k_is_lossless() {
        for shift in 9..=16 {
            let size = 1_usize << shift;
            let newest = vec![0xA5; size];
            let mut older = newest.clone();
            older[size - 1] ^= 0xFF;
            let images = vec![(CommitSeq::new(2), newest), (CommitSeq::new(1), older)];
            let history = CompactPageHistory::compress(PageNumber::ONE, &images).unwrap();
            assert_eq!(history.delta_count(), 1);
            let decoded = CompactPageHistory::from_bytes(&history.to_bytes().unwrap()).unwrap();
            assert_eq!(
                decoded.reconstruct(CommitSeq::new(1)).unwrap(),
                Some(images[1].clone())
            );
        }
    }

    #[test]
    fn fragmented_and_dense_deltas_do_not_expand_storage() {
        let newest = vec![0; 4096];
        let mut fragmented = newest.clone();
        for index in (0..fragmented.len()).step_by(2) {
            fragmented[index] = 1;
        }
        // An estimate based only on nonzero bytes misses the 2048 run headers.
        assert!(encode_sparse_xor_delta(&newest, &fragmented).unwrap().len() > newest.len());
        for older in [fragmented, vec![0xFF; 4096]] {
            let history = CompactPageHistory::compress(
                PageNumber::ONE,
                &[
                    (CommitSeq::new(2), newest.clone()),
                    (CommitSeq::new(1), older),
                ],
            )
            .unwrap();
            assert_eq!(history.delta_count(), 0);
            assert_eq!(
                history.encoded_len(),
                HEADER_BYTES + HASH_BYTES + 2 * (ENTRY_BYTES + 4096)
            );
        }
    }

    #[test]
    fn saving_threshold_counts_metadata_and_keeps_default_deterministic() {
        let images = hot_images(2, 512);
        let tight = CompactPageHistory::compress_with_threshold(
            PageNumber::ONE,
            &images,
            DeltaThresholdConfig::new(99).unwrap(),
        )
        .unwrap();
        let loose = CompactPageHistory::compress_with_threshold(
            PageNumber::ONE,
            &images,
            DeltaThresholdConfig::new(1).unwrap(),
        )
        .unwrap();
        assert_eq!(tight.delta_count(), 0);
        assert_eq!(loose.delta_count(), 1);
        let first = CompactPageHistory::compress(PageNumber::ONE, &images).unwrap();
        let second = CompactPageHistory::compress(PageNumber::ONE, &images).unwrap();
        assert_eq!(first.to_bytes().unwrap(), second.to_bytes().unwrap());
        assert_eq!(first.encoded_len(), first.to_bytes().unwrap().len());
    }

    #[test]
    fn newest_and_each_dependency_boundary_are_full_anchors() {
        let history = CompactPageHistory::compress(PageNumber::ONE, &hot_images(100, 512)).unwrap();
        for (index, version) in history.versions.iter().enumerate() {
            assert!(index - version.anchor <= MAX_DELTA_DEPTH);
            assert!(matches!(
                history.versions[version.anchor].payload,
                Payload::Full(_)
            ));
            if index % 32 == 0 {
                assert!(matches!(version.payload, Payload::Full(_)));
            }
        }
    }

    #[test]
    fn equal_images_are_encoded_as_empty_sparse_runs_not_empty_placeholders() {
        let images = vec![
            (CommitSeq::new(u64::MAX), vec![1; 512]),
            (CommitSeq::new(0), vec![1; 512]),
        ];
        let history = CompactPageHistory::compress(PageNumber::ONE, &images).unwrap();
        let Payload::Xor(bytes) = &history.versions[1].payload else {
            panic!("expected delta");
        };
        assert_eq!(bytes, b"XD\x01\0\0\0\0\0");
        assert_ne!(history.versions[0].hash, history.versions[1].hash);
        assert_eq!(
            history.reconstruct(CommitSeq::new(0)).unwrap(),
            Some(images[1].clone())
        );
    }

    #[test]
    fn invalid_input_is_refused_without_modifying_images() {
        assert!(CompactPageHistory::compress(PageNumber::ONE, &[]).is_err());
        for size in [0, 256, 513, 65_537] {
            assert!(CompactPageHistory::compress(PageNumber::ONE, &hot_images(1, size)).is_err());
        }
        for sequences in [[1, 1], [1, 2]] {
            let images = vec![
                (CommitSeq::new(sequences[0]), vec![0; 512]),
                (CommitSeq::new(sequences[1]), vec![0; 512]),
            ];
            let original = images.clone();
            assert!(CompactPageHistory::compress(PageNumber::ONE, &images).is_err());
            assert_eq!(images, original);
        }
        assert!(
            CompactPageHistory::compress(
                PageNumber::ONE,
                &[
                    (CommitSeq::new(2), vec![0; 512]),
                    (CommitSeq::new(1), vec![0; 1024]),
                ],
            )
            .is_err()
        );
        assert!(CompactPageHistory::compress(PageNumber::ONE, &hot_images(4097, 512)).is_err());
    }

    #[test]
    fn all_truncations_and_bit_corruption_fail_before_publication() {
        let history = CompactPageHistory::compress(PageNumber::ONE, &hot_images(3, 512)).unwrap();
        let bytes = history.to_bytes().unwrap();
        for length in 0..bytes.len() {
            assert!(CompactPageHistory::from_bytes(&bytes[..length]).is_err());
        }
        for index in 0..bytes.len() {
            let mut changed = bytes.clone();
            changed[index] ^= 1;
            assert!(CompactPageHistory::from_bytes(&changed).is_err());
        }
        assert_eq!(CompactPageHistory::from_bytes(&bytes).unwrap(), history);
    }

    #[test]
    fn valid_outer_checksum_does_not_bypass_structural_validation() {
        let history = CompactPageHistory::compress(PageNumber::ONE, &hot_images(3, 512)).unwrap();
        let bytes = history.to_bytes().unwrap();
        for (offset, replacement) in [
            (0, b"NOPE".as_slice()),
            (4, &[0; 4]),
            (4, &[0xFF; 4]),
            (8, &[3, 0, 0, 0]),
            (12, &[0; 4]),
            (12, &[0xFF; 4]),
            (24, &[1]),
            (24, &[9]),
            (25, &[0xFF; 4]),
        ] {
            let mut changed = bytes.clone();
            changed[offset..offset + replacement.len()].copy_from_slice(replacement);
            resign(&mut changed);
            assert!(CompactPageHistory::from_bytes(&changed).is_err());
        }
        let mut trailing = bytes[..bytes.len() - HASH_BYTES].to_vec();
        trailing.push(0);
        let hash = archive_hash(&trailing);
        trailing.extend_from_slice(&hash);
        assert!(CompactPageHistory::from_bytes(&trailing).is_err());
    }

    #[test]
    fn per_image_hashes_bind_content_page_and_sequence() {
        let history = CompactPageHistory::compress(PageNumber::ONE, &hot_images(3, 512)).unwrap();
        let bytes = history.to_bytes().unwrap();
        for offset in [4, 16, 29, HEADER_BYTES + ENTRY_BYTES] {
            let mut changed = bytes.clone();
            changed[offset] ^= 2;
            resign(&mut changed);
            assert!(CompactPageHistory::from_bytes(&changed).is_err());
        }
        let mut corrupted = history;
        let Payload::Xor(delta) = &mut corrupted.versions[1].payload else {
            panic!("expected delta");
        };
        *delta.last_mut().unwrap() ^= 0x80;
        assert!(CompactPageHistory::from_bytes(&corrupted.to_bytes().unwrap()).is_err());
        assert!(corrupted.reconstruct(CommitSeq::new(20)).is_err());
    }

    #[test]
    fn noncanonical_flags_and_unbounded_chains_are_refused() {
        let history = CompactPageHistory::compress(PageNumber::ONE, &hot_images(40, 512)).unwrap();
        let mut flags = history.clone();
        let Payload::Xor(delta) = &mut flags.versions[1].payload else {
            panic!("expected delta");
        };
        delta[3] = 1;
        assert!(CompactPageHistory::from_bytes(&flags.to_bytes().unwrap()).is_err());
        let mut chain = history;
        let newer = chain
            .reconstruct(chain.versions[31].seq)
            .unwrap()
            .unwrap()
            .1;
        let older = chain
            .reconstruct(chain.versions[32].seq)
            .unwrap()
            .unwrap()
            .1;
        let bytes = encode_sparse_xor_delta(&newer, &older).unwrap();
        chain.encoded_len =
            chain.encoded_len - chain.versions[32].payload.bytes().len() + bytes.len();
        chain.versions[32].payload = Payload::Xor(bytes);
        assert!(CompactPageHistory::from_bytes(&chain.to_bytes().unwrap()).is_err());
    }

    #[test]
    fn decoded_history_refuses_ascending_sequence_even_with_correct_image_hash() {
        let mut history =
            CompactPageHistory::compress(PageNumber::ONE, &hot_images(2, 512)).unwrap();
        let image = history
            .reconstruct(history.versions[1].seq)
            .unwrap()
            .unwrap()
            .1;
        history.versions[1].seq = history.versions[0].seq;
        history.versions[1].hash = image_hash(history.pgno, history.versions[1].seq, &image);
        assert!(CompactPageHistory::from_bytes(&history.to_bytes().unwrap()).is_err());
    }

    #[test]
    fn random_access_starts_at_its_anchor_not_at_the_tip() {
        let images = hot_images(100, 512);
        let mut history = CompactPageHistory::compress(PageNumber::ONE, &images).unwrap();
        let Payload::Full(tip) = &mut history.versions[0].payload else {
            panic!("expected full tip");
        };
        tip[0] ^= 1;
        assert!(history.reconstruct(images[0].0).is_err());
        assert_eq!(
            history.reconstruct(images[40].0).unwrap(),
            Some(images[40].clone())
        );
    }
}
