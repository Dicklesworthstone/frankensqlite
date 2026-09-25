//! Content-bound framing for incremental Session changeset ingestion.
//!
//! A trusted descriptor fixes the message length and the ordered BLAKE3-256
//! digest of each 64 KiB chunk (the last chunk may be shorter). Its identity
//! is BLAKE3-256 of the canonical descriptor, NOT of the flat message. Receive
//! that identity over an authenticated control plane; a digest accompanying
//! an untrusted descriptor does not establish source identity or freshness.
//!
//! The reader verifies a complete chunk before exposing any of its bytes and
//! never reads beyond the declared message. Early EOF, including at a valid
//! Session row boundary, is an error. A caller must consume the entire frame
//! before committing provisional SQL effects. This does not provide source
//! mutation capture, transaction sequencing, deduplication, or encryption.

use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use asupersync::io::{AsyncRead, ReadBuf};
use fsqlite_types::{PayloadHash, cx::Cx};

use crate::FrankenError;

/// Fixed boundaries make verification independent of transport fragmentation.
pub const CHANGESET_FRAME_CHUNK_BYTES: usize = 65_536;
/// At most 2 MiB of chunk digests and a 4 GiB message per descriptor.
pub const MAX_CHANGESET_FRAME_CHUNKS: usize = 65_536;
const MAGIC: &[u8; 8] = b"FSCSFR01";
const HEADER_BYTES: usize = 20;
const DIGEST_BYTES: usize = 32;

fn malformed(detail: &'static str) -> FrankenError {
    FrankenError::DatabaseCorrupt {
        detail: detail.to_owned(),
    }
}

/// Immutable, canonical length and ordered-chunk commitment for one message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetFrame {
    byte_len: u64,
    chunks: Vec<PayloadHash>,
}

impl ChangesetFrame {
    /// Construct at the sender from already-owned wire bytes. Large producers
    /// may hash chunks incrementally and use `from_chunk_hashes` instead.
    pub fn for_message(cx: &Cx, bytes: &[u8], max_bytes: u64) -> Result<Self, FrankenError> {
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
        let byte_len = u64::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
        let count = Self::admit(byte_len, max_bytes)?;
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(count)
            .map_err(|_| FrankenError::OutOfMemory)?;
        for chunk in bytes.chunks(CHANGESET_FRAME_CHUNK_BYTES) {
            cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
            chunks.push(PayloadHash::blake3(chunk));
        }
        Ok(Self { byte_len, chunks })
    }

    /// Sender-side construction using digests of fixed-size chunks in order.
    /// The caller must hash the exact final short chunk, without zero padding.
    pub fn from_chunk_hashes(
        byte_len: u64,
        chunks: Vec<PayloadHash>,
        max_bytes: u64,
    ) -> Result<Self, FrankenError> {
        if chunks.len() != Self::admit(byte_len, max_bytes)? {
            return Err(malformed(
                "changeset frame length and digest count disagree",
            ));
        }
        Ok(Self { byte_len, chunks })
    }

    fn admit(byte_len: u64, max_bytes: u64) -> Result<usize, FrankenError> {
        let count = byte_len.div_ceil(CHANGESET_FRAME_CHUNK_BYTES as u64);
        if byte_len > max_bytes || count > MAX_CHANGESET_FRAME_CHUNKS as u64 {
            return Err(FrankenError::TooBig);
        }
        usize::try_from(count).map_err(|_| FrankenError::TooBig)
    }

    #[must_use]
    pub const fn byte_len(&self) -> u64 {
        self.byte_len
    }

    /// Canonical descriptor: magic/version, length(u64 LE), count(u32 LE),
    /// then the exact ordered 32-byte BLAKE3 chunk digests; no trailing bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_BYTES + self.chunks.len() * DIGEST_BYTES);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.byte_len.to_le_bytes());
        let count = u32::try_from(self.chunks.len()).expect("validated descriptor count");
        bytes.extend_from_slice(&count.to_le_bytes());
        for chunk in &self.chunks {
            bytes.extend_from_slice(chunk.as_bytes());
        }
        bytes
    }

    #[must_use]
    pub fn id(&self) -> PayloadHash {
        PayloadHash::blake3(&self.encode())
    }

    /// Check dimensions and the independently trusted root BEFORE allocating
    /// the digest list. The hard descriptor bound also limits hashing work.
    pub fn decode(
        bytes: &[u8],
        expected_id: PayloadHash,
        max_bytes: u64,
    ) -> Result<Self, FrankenError> {
        if bytes.len() < HEADER_BYTES || &bytes[..8] != MAGIC {
            return Err(malformed("invalid changeset frame descriptor"));
        }
        let byte_len = u64::from_le_bytes(bytes[8..16].try_into().expect("header width"));
        let count = Self::admit(byte_len, max_bytes)?;
        let recorded = u32::from_le_bytes(bytes[16..20].try_into().expect("header width"));
        if usize::try_from(recorded).ok() != Some(count)
            || bytes.len() != HEADER_BYTES + count * DIGEST_BYTES
        {
            return Err(malformed("noncanonical changeset frame dimensions"));
        }
        if PayloadHash::blake3(bytes) != expected_id {
            return Err(malformed(
                "changeset frame does not match its trusted identity",
            ));
        }
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(count)
            .map_err(|_| FrankenError::OutOfMemory)?;
        for hash in bytes[HEADER_BYTES..].chunks_exact(DIGEST_BYTES) {
            chunks.push(PayloadHash::from_bytes(
                hash.try_into().expect("digest width"),
            ));
        }
        Ok(Self { byte_len, chunks })
    }
}

/// Typed cause retained inside the `io::Error` returned by the reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangesetFrameReadError {
    Truncated { expected: u64, received: u64 },
    CorruptChunk { index: usize },
    Poisoned,
}

impl fmt::Display for ChangesetFrameReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated { expected, received } => {
                write!(
                    f,
                    "changeset frame truncated: expected {expected} bytes, received {received}"
                )
            }
            Self::CorruptChunk { index } => {
                write!(f, "changeset frame chunk {index} failed verification")
            }
            Self::Poisoned => f.write_str("changeset frame reader previously failed"),
        }
    }
}

impl std::error::Error for ChangesetFrameReadError {}

/// One verified chunk of look-ahead; no message-sized payload allocation.
///
/// Pending reads retain their partial chunk inside this owner. Discard the
/// reader after any error. A higher-level SQL apply must separately roll back
/// provisional effects when it is dropped. The underlying transport must wake
/// for its own cancellation/deadline; this adapter never spawns a task.
pub struct VerifiedChangesetReader<R> {
    input: R,
    frame: ChangesetFrame,
    buffer: Vec<u8>,
    chunk: usize,
    filled: usize,
    released: usize,
    delivered: u64,
    ready: bool,
    poisoned: bool,
}

impl<R> fmt::Debug for VerifiedChangesetReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedChangesetReader")
            .field("byte_len", &self.frame.byte_len)
            .field("delivered", &self.delivered)
            .field("chunk", &self.chunk)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl<R> VerifiedChangesetReader<R> {
    /// `expected_id` must come from trusted control-plane state. Recheck the
    /// frame's policy and identity even if it was constructed locally.
    pub fn new(
        input: R,
        frame: ChangesetFrame,
        expected_id: PayloadHash,
        max_bytes: u64,
    ) -> Result<Self, FrankenError> {
        ChangesetFrame::admit(frame.byte_len, max_bytes)?;
        if frame.id() != expected_id {
            return Err(malformed("changeset frame reader identity mismatch"));
        }
        let first = frame.byte_len.min(CHANGESET_FRAME_CHUNK_BYTES as u64);
        let first = usize::try_from(first).map_err(|_| FrankenError::TooBig)?;
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(first)
            .map_err(|_| FrankenError::OutOfMemory)?;
        buffer.resize(first, 0);
        Ok(Self {
            input,
            frame,
            buffer,
            chunk: 0,
            filled: 0,
            released: 0,
            delivered: 0,
            ready: false,
            poisoned: false,
        })
    }

    #[must_use]
    pub const fn bytes_delivered(&self) -> u64 {
        self.delivered
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        !self.poisoned && self.delivered == self.frame.byte_len
    }

    /// Recover the transport without consuming any following frame. On error
    /// its position is partial; do not treat it as a fresh message boundary.
    #[must_use]
    pub fn into_inner(self) -> R {
        self.input
    }

    fn fail(&mut self, kind: io::ErrorKind, cause: ChangesetFrameReadError) -> io::Error {
        self.poisoned = true;
        io::Error::new(kind, cause)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for VerifiedChangesetReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.poisoned {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                ChangesetFrameReadError::Poisoned,
            )));
        }
        if output.remaining() == 0 || this.is_complete() {
            return Poll::Ready(Ok(()));
        }
        if !this.ready {
            // Bound immediate underlying polls so even repeated Interrupted
            // errors or one-byte sources cannot monopolize one executor turn.
            for _ in 0..8 {
                if this.filled == this.buffer.len() {
                    break;
                }
                let mut target = ReadBuf::new(&mut this.buffer[this.filled..]);
                let result = Pin::new(&mut this.input).poll_read(cx, &mut target);
                let count = target.filled().len();
                this.filled += count;
                match result {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
                    Poll::Ready(Err(error)) => {
                        this.poisoned = true;
                        return Poll::Ready(Err(error));
                    }
                    Poll::Ready(Ok(())) if count == 0 => {
                        let received = this.chunk as u64 * CHANGESET_FRAME_CHUNK_BYTES as u64
                            + this.filled as u64;
                        let cause = ChangesetFrameReadError::Truncated {
                            expected: this.frame.byte_len,
                            received,
                        };
                        return Poll::Ready(Err(this.fail(io::ErrorKind::UnexpectedEof, cause)));
                    }
                    Poll::Ready(Ok(())) => {}
                }
                if this.filled == this.buffer.len() {
                    break;
                }
            }
            if this.filled != this.buffer.len() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if PayloadHash::blake3(&this.buffer) != this.frame.chunks[this.chunk] {
                let cause = ChangesetFrameReadError::CorruptChunk { index: this.chunk };
                return Poll::Ready(Err(this.fail(io::ErrorKind::InvalidData, cause)));
            }
            this.ready = true;
        }
        let count = output.remaining().min(this.buffer.len() - this.released);
        output.put_slice(&this.buffer[this.released..this.released + count]);
        this.released += count;
        this.delivered += count as u64;
        if this.released == this.buffer.len() {
            this.chunk += 1;
            this.filled = 0;
            this.released = 0;
            this.ready = false;
            let next =
                (this.frame.byte_len - this.delivered).min(CHANGESET_FRAME_CHUNK_BYTES as u64);
            // Never grow beyond the first chunk's already-reserved capacity.
            this.buffer
                .resize(usize::try_from(next).expect("one chunk fits usize"), 0);
        }
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::io::AsyncReadExt;

    struct Input {
        bytes: Vec<u8>,
        offset: usize,
        fragment: usize,
        pending: bool,
    }

    impl AsyncRead for Input {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            out: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.pending {
                this.pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let n = this
                .fragment
                .min(out.remaining())
                .min(this.bytes.len() - this.offset);
            out.put_slice(&this.bytes[this.offset..this.offset + n]);
            this.offset += n;
            this.pending = true;
            Poll::Ready(Ok(()))
        }
    }

    fn input(bytes: Vec<u8>, fragment: usize) -> Input {
        Input {
            bytes,
            offset: 0,
            fragment,
            pending: false,
        }
    }

    fn frame(bytes: &[u8]) -> ChangesetFrame {
        ChangesetFrame::for_message(&Cx::new(), bytes, u64::MAX).unwrap()
    }

    #[test]
    fn descriptor_roundtrip_requires_canonical_bytes_and_trusted_identity() {
        let descriptor = frame(&vec![42; CHANGESET_FRAME_CHUNK_BYTES + 3]);
        let bytes = descriptor.encode();
        assert_eq!(
            ChangesetFrame::decode(&bytes, descriptor.id(), u64::MAX).unwrap(),
            descriptor
        );
        assert_eq!(bytes.len(), HEADER_BYTES + 2 * DIGEST_BYTES);
        for cut in 0..bytes.len() {
            assert!(ChangesetFrame::decode(&bytes[..cut], descriptor.id(), u64::MAX).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(
            ChangesetFrame::decode(&trailing, PayloadHash::blake3(&trailing), u64::MAX).is_err()
        );
        assert!(
            ChangesetFrame::decode(
                &descriptor.encode(),
                PayloadHash::from_bytes([0; 32]),
                u64::MAX
            )
            .is_err()
        );
    }

    #[test]
    fn dimensions_and_limits_are_checked_before_payload_allocation() {
        assert!(matches!(
            ChangesetFrame::for_message(&Cx::new(), &[1, 2], 1),
            Err(FrankenError::TooBig)
        ));
        assert!(ChangesetFrame::from_chunk_hashes(1, Vec::new(), u64::MAX).is_err());
        assert!(matches!(
            ChangesetFrame::from_chunk_hashes(u64::MAX, Vec::new(), u64::MAX),
            Err(FrankenError::TooBig)
        ));
        let mut bytes = frame(&[]).encode();
        bytes[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(ChangesetFrame::decode(&bytes, PayloadHash::blake3(&bytes), u64::MAX).is_err());
        let cx = Cx::new();
        cx.cancel();
        assert!(matches!(
            ChangesetFrame::for_message(&cx, &[1], 1),
            Err(FrankenError::Interrupt)
        ));
    }

    #[test]
    fn fragmented_pending_reads_verify_chunks_and_do_not_read_the_next_frame() {
        asupersync::test_utils::run_test(|| async {
            let bytes = vec![0x5a; CHANGESET_FRAME_CHUNK_BYTES * 2 + 17];
            for fragment in [1, 8191, CHANGESET_FRAME_CHUNK_BYTES * 3] {
                let descriptor = frame(&bytes);
                let id = descriptor.id();
                let mut wire = bytes.clone();
                wire.extend_from_slice(b"next message");
                let mut reader =
                    VerifiedChangesetReader::new(input(wire, fragment), descriptor, id, u64::MAX)
                        .unwrap();
                let mut result = Vec::new();
                let mut buffer = [0; 701];
                loop {
                    let n = reader.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    result.extend_from_slice(&buffer[..n]);
                    assert!(reader.buffer.capacity() <= CHANGESET_FRAME_CHUNK_BYTES);
                }
                assert_eq!(result, bytes);
                assert!(reader.is_complete());
                assert_eq!(reader.bytes_delivered(), bytes.len() as u64);
                assert_eq!(reader.read(&mut buffer).await.unwrap(), 0);
                assert_eq!(reader.into_inner().offset, bytes.len());
            }
        });
    }

    #[test]
    fn corrupt_chunk_exposes_none_of_its_bytes_and_failure_is_terminal() {
        asupersync::test_utils::run_test(|| async {
            let good = vec![23; CHANGESET_FRAME_CHUNK_BYTES + 9];
            for at in [0, CHANGESET_FRAME_CHUNK_BYTES + 8] {
                let descriptor = frame(&good);
                let id = descriptor.id();
                let mut bad = good.clone();
                bad[at] ^= 1;
                let mut reader =
                    VerifiedChangesetReader::new(input(bad, 8192), descriptor, id, u64::MAX)
                        .unwrap();
                let mut delivered = 0;
                let mut buffer = [0; 8192];
                loop {
                    match reader.read(&mut buffer).await {
                        Ok(0) => panic!("corrupt input reported EOF"),
                        Ok(n) => delivered += n,
                        Err(error) => {
                            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                            assert!(matches!(
                                error
                                    .get_ref()
                                    .unwrap()
                                    .downcast_ref::<ChangesetFrameReadError>(),
                                Some(ChangesetFrameReadError::CorruptChunk { .. })
                            ));
                            break;
                        }
                    }
                }
                assert_eq!(
                    delivered,
                    if at == 0 {
                        0
                    } else {
                        CHANGESET_FRAME_CHUNK_BYTES
                    }
                );
                assert!(!reader.is_complete());
                assert!(reader.read(&mut buffer).await.is_err());
            }
        });
    }

    #[test]
    fn exact_chunk_boundary_truncation_is_not_a_successful_eof() {
        asupersync::test_utils::run_test(|| async {
            let bytes = vec![9; CHANGESET_FRAME_CHUNK_BYTES + 1];
            let descriptor = frame(&bytes);
            let id = descriptor.id();
            let mut reader = VerifiedChangesetReader::new(
                input(bytes[..CHANGESET_FRAME_CHUNK_BYTES].to_vec(), 8192),
                descriptor,
                id,
                u64::MAX,
            )
            .unwrap();
            let mut buffer = [0; 8192];
            let mut delivered = 0;
            let error = loop {
                match reader.read(&mut buffer).await {
                    Ok(0) => panic!("truncated frame reported EOF"),
                    Ok(n) => delivered += n,
                    Err(error) => break error,
                }
            };
            assert_eq!(delivered, CHANGESET_FRAME_CHUNK_BYTES);
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
            assert!(!reader.is_complete());
        });
    }

    #[test]
    fn empty_frame_does_not_consume_a_following_message_or_expose_input_in_debug() {
        asupersync::test_utils::run_test(|| async {
            let descriptor = frame(&[]);
            let id = descriptor.id();
            let mut reader = VerifiedChangesetReader::new(
                input(b"private next message".to_vec(), 1024),
                descriptor,
                id,
                0,
            )
            .unwrap();
            assert_eq!(reader.read(&mut [0; 1]).await.unwrap(), 0);
            assert!(reader.is_complete());
            assert!(!format!("{reader:?}").contains("private"));
            assert_eq!(reader.into_inner().offset, 0);
        });
    }
}
