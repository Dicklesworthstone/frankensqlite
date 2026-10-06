//! Bounded native-object codec using the existing asupersync RaptorQ pipeline.
//!
//! The V1 profile fixes 1 KiB symbols and 64 KiB source blocks. Its identity
//! binds that profile, the exact OTI, and the hash of the unpadded payload.
//! Source/repair kind and source-block number are carried in the packed ESI,
//! matching the existing core codec's layout. No systematic-only substitute or
//! fabricated repair path is used. Authenticated mode refuses zero tags.

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};

use asupersync::error::ErrorKind;
use asupersync::raptorq::{RaptorQReceiverBuilder, RaptorQSenderBuilder};
use asupersync::security::AuthenticationTag;
use asupersync::security::authenticated::AuthenticatedSymbol;
use asupersync::transport::error::{SinkError, StreamError};
use asupersync::transport::sink::SymbolSink;
use asupersync::transport::stream::SymbolStream;
use asupersync::types::{ObjectId as CodecObjectId, ObjectParams, Symbol, SymbolId, SymbolKind};
use asupersync::{Cx as NativeCx, RaptorQConfig};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::ecs::PayloadHash;
use fsqlite_types::{ObjectId, Oti, SymbolRecord, SymbolRecordFlags};

use super::NativeObjectCodec;

/// Largest payload supported by this profile. The 2x symbol redundancy plus
/// envelopes fits the default native log's 16 MiB per-object read limit.
pub const MAX_NATIVE_OBJECT_BYTES: usize = 4 * 1024 * 1024;
const SYMBOL_BYTES: u16 = 1024;
const BLOCK_BYTES: usize = 64 * 1024;
const MAX_RECORDS: usize = 16 * 1024;
const REPAIR_BIT: u32 = 1 << 31;
const BLOCK_SHIFT: u32 = 23;
const ESI_MASK: u32 = (1 << BLOCK_SHIFT) - 1;
const PIPELINE_OBJECT: u64 = 0x4653_4E43_0001;

/// Stateless per-call RaptorQ encoding/decoding for native admission objects.
///
/// An optional epoch key authenticates every stored envelope. It is never
/// included in Debug output (this type deliberately does not implement Debug).
/// Key distribution/rotation and epoch admission belong to the database owner.
/// Decoding with a key requires a nonzero valid tag; decoding without a key
/// refuses tagged input rather than silently skipping its authentication.
pub struct RaptorQNativeCodec {
    epoch_key: Option<[u8; 32]>,
}

impl RaptorQNativeCodec {
    #[must_use]
    pub const fn new(epoch_key: Option<[u8; 32]>) -> Self {
        Self { epoch_key }
    }

    /// Derive the exact content identity used by `encode`, before submission.
    ///
    /// # Errors
    /// Returns `TooBig` for empty or oversized objects.
    pub fn object_id(payload: &[u8]) -> Result<ObjectId> {
        let oti = layout(payload.len())?;
        let mut header = Vec::with_capacity(34);
        header.extend_from_slice(b"FNCO\x01\0\0\0");
        header.extend_from_slice(&65_536_u32.to_le_bytes());
        header.extend_from_slice(&oti.to_bytes());
        Ok(ObjectId::derive(&header, PayloadHash::blake3(payload)))
    }
}

impl Default for RaptorQNativeCodec {
    fn default() -> Self {
        Self::new(None)
    }
}

impl NativeObjectCodec for RaptorQNativeCodec {
    fn encode(&self, cx: &Cx, payload: &[u8]) -> Result<Vec<SymbolRecord>> {
        let oti = layout(payload.len())?;
        let object_id = Self::object_id(payload)?;
        let native = native_context(cx)?;
        let mut sender = RaptorQSenderBuilder::new()
            .config(configuration())
            .transport(CollectSymbols::default())
            .build()
            .map_err(|error| corrupt(&format!("native RaptorQ sender: {error}")))?;
        sender
            .send_object(
                &native,
                CodecObjectId::new_for_test(PIPELINE_OBJECT),
                payload,
            )
            .map_err(|error| codec_error(error.kind(), error.to_string()))?;
        let symbols = std::mem::take(&mut sender.transport_mut().0);
        if symbols.len() > MAX_RECORDS {
            return Err(FrankenError::TooBig);
        }
        let mut records = Vec::new();
        records
            .try_reserve_exact(symbols.len())
            .map_err(|_| FrankenError::OutOfMemory)?;
        for symbol in symbols {
            checkpoint(cx)?;
            if symbol.esi() > ESI_MASK || symbol.data().len() != usize::from(SYMBOL_BYTES) {
                return Err(corrupt("native RaptorQ encoder produced an invalid symbol"));
            }
            let key = (if symbol.kind().is_repair() {
                REPAIR_BIT
            } else {
                0
            }) | (u32::from(symbol.sbn()) << BLOCK_SHIFT)
                | symbol.esi();
            validate_key(key, oti)?;
            let mut record = SymbolRecord::new(
                object_id,
                oti,
                key,
                symbol.data().to_vec(),
                SymbolRecordFlags::empty(),
            );
            if let Some(key) = &self.epoch_key {
                record = record.with_auth_tag(key);
            }
            records.push(record);
        }
        Ok(records)
    }

    fn decode(&self, cx: &Cx, object_id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        checkpoint(cx)?;
        if records.is_empty() || records.len() > MAX_RECORDS {
            return Err(corrupt("native RaptorQ record count is out of bounds"));
        }
        let oti = records[0].oti;
        let len = usize::try_from(oti.f).map_err(|_| FrankenError::TooBig)?;
        if layout(len)? != oti {
            return Err(corrupt(
                "native RaptorQ object uses a different layout/profile",
            ));
        }
        let native = native_context(cx)?;
        let pipeline_id = CodecObjectId::new_for_test(PIPELINE_OBJECT);
        let mut seen = BTreeMap::<u32, &[u8]>::new();
        let mut symbols = VecDeque::new();
        symbols
            .try_reserve(records.len())
            .map_err(|_| FrankenError::OutOfMemory)?;
        for record in records {
            checkpoint(cx)?;
            // Check sizes before SymbolRecord's hashing helpers, which assume
            // symbol_data.len() == OTI.T. Never let malformed input panic there.
            if record.object_id != object_id
                || record.oti != oti
                || record.symbol_data.len() != usize::from(SYMBOL_BYTES)
                || !record.verify_integrity()
            {
                return Err(corrupt(
                    "native RaptorQ symbol identity/layout/integrity mismatch",
                ));
            }
            let auth_ok = match &self.epoch_key {
                Some(key) => record.auth_tag != [0; 16] && record.verify_auth(key),
                None => record.auth_tag == [0; 16],
            };
            if !auth_ok {
                return Err(corrupt("native RaptorQ symbol authentication failed"));
            }
            let (kind, block, esi) = validate_key(record.esi, oti)?;
            if let Some(previous) = seen.insert(record.esi, &record.symbol_data) {
                if previous != record.symbol_data.as_slice() {
                    return Err(corrupt("conflicting duplicate native RaptorQ symbol"));
                }
                continue;
            }
            let symbol = Symbol::new(
                SymbolId::new(pipeline_id, block, esi),
                record.symbol_data.clone(),
                kind,
            );
            // Storage authentication above is separate from the pipeline's
            // transport MAC domain. The in-memory stream carries no network MAC.
            symbols.push_back(AuthenticatedSymbol::from_parts(
                symbol,
                AuthenticationTag::zero(),
            ));
        }
        let block_symbols = len.min(BLOCK_BYTES).div_ceil(usize::from(SYMBOL_BYTES));
        let params = ObjectParams::new(
            pipeline_id,
            oti.f,
            SYMBOL_BYTES,
            u16::try_from(oti.z).map_err(|_| FrankenError::TooBig)?,
            u16::try_from(block_symbols).map_err(|_| FrankenError::TooBig)?,
        );
        let mut receiver = RaptorQReceiverBuilder::new()
            .config(configuration())
            .source(StoredSymbols(symbols))
            .build()
            .map_err(|error| corrupt(&format!("native RaptorQ receiver: {error}")))?;
        let outcome = receiver
            .receive_object(&native, &params)
            .map_err(|error| codec_error(error.kind(), error.to_string()))?;
        checkpoint(cx)?;
        if outcome.data.len() != len || Self::object_id(&outcome.data)? != object_id {
            return Err(corrupt(
                "native RaptorQ decoded payload identity/length mismatch",
            ));
        }
        Ok(outcome.data)
    }
}

fn layout(len: usize) -> Result<Oti> {
    if len == 0 || len > MAX_NATIVE_OBJECT_BYTES {
        return Err(FrankenError::TooBig);
    }
    Ok(Oti {
        f: u64::try_from(len).map_err(|_| FrankenError::TooBig)?,
        al: 1,
        t: u32::from(SYMBOL_BYTES),
        z: u32::try_from(len.div_ceil(BLOCK_BYTES)).map_err(|_| FrankenError::TooBig)?,
        n: 1,
    })
}

fn validate_key(key: u32, oti: Oti) -> Result<(SymbolKind, u8, u32)> {
    let block = u8::try_from((key >> BLOCK_SHIFT) & 0xFF).map_err(|_| FrankenError::TooBig)?;
    if u32::from(block) >= oti.z {
        return Err(corrupt("native RaptorQ symbol block is outside its object"));
    }
    let offset = u64::from(block) * 65_536;
    let bytes = oti
        .f
        .checked_sub(offset)
        .ok_or_else(|| corrupt("native RaptorQ block offset overflow"))?
        .min(65_536);
    let k = bytes.div_ceil(u64::from(SYMBOL_BYTES));
    let esi = key & ESI_MASK;
    let repair = key & REPAIR_BIT != 0;
    if (repair && u64::from(esi) < k) || (!repair && u64::from(esi) >= k) {
        return Err(corrupt("native RaptorQ symbol kind/ESI mismatch"));
    }
    Ok((
        if repair {
            SymbolKind::Repair
        } else {
            SymbolKind::Source
        },
        block,
        esi,
    ))
}

fn configuration() -> RaptorQConfig {
    let mut config = RaptorQConfig::default();
    config.encoding.symbol_size = SYMBOL_BYTES;
    config.encoding.max_block_size = BLOCK_BYTES;
    config.encoding.repair_overhead = 2.0;
    config
}

fn native_context(cx: &Cx) -> Result<NativeCx> {
    checkpoint(cx)?;
    cx.attached_native_cx()
        .or_else(NativeCx::current)
        .ok_or_else(|| {
            FrankenError::BackgroundWorkerFailed(
                "native RaptorQ requires the caller's runtime context".to_owned(),
            )
        })
}

fn checkpoint(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)
}
fn corrupt(detail: &str) -> FrankenError {
    FrankenError::WalCorrupt {
        detail: detail.to_owned(),
    }
}
fn codec_error(kind: ErrorKind, detail: String) -> FrankenError {
    match kind {
        ErrorKind::Cancelled
        | ErrorKind::CancelTimeout
        | ErrorKind::DeadlineExceeded
        | ErrorKind::PollQuotaExhausted
        | ErrorKind::CostQuotaExhausted => FrankenError::Interrupt,
        _ => corrupt(&format!("native RaptorQ failed: {detail}")),
    }
}

#[derive(Default)]
struct CollectSymbols(Vec<Symbol>);
impl SymbolSink for CollectSymbols {
    fn poll_send(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        symbol: AuthenticatedSymbol,
    ) -> Poll<std::result::Result<(), SinkError>> {
        self.0.push(symbol.into_symbol());
        Poll::Ready(Ok(()))
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), SinkError>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), SinkError>> {
        Poll::Ready(Ok(()))
    }
    fn poll_ready(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), SinkError>> {
        Poll::Ready(Ok(()))
    }
}

struct StoredSymbols(VecDeque<AuthenticatedSymbol>);
impl SymbolStream for StoredSymbols {
    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<AuthenticatedSymbol, StreamError>>> {
        Poll::Ready(self.0.pop_front().map(Ok))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.0.len(), Some(self.0.len()))
    }
    fn is_exhausted(&self) -> bool {
        self.0.is_empty()
    }
}
