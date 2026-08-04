use std::future::Future;

use fsqlite_btree::{BtreeCursorOps, SeekResult};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;

struct ExternalCursor;

impl BtreeCursorOps for ExternalCursor {
    fn index_move_to<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _key: &'a [u8],
    ) -> impl Future<Output = Result<SeekResult>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn table_move_to<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _rowid: i64,
    ) -> impl Future<Output = Result<SeekResult>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn first<'a>(&'a mut self, _cx: &'a Cx) -> impl Future<Output = Result<bool>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn last<'a>(&'a mut self, _cx: &'a Cx) -> impl Future<Output = Result<bool>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn next<'a>(&'a mut self, _cx: &'a Cx) -> impl Future<Output = Result<bool>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn prev<'a>(&'a mut self, _cx: &'a Cx) -> impl Future<Output = Result<bool>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn index_insert<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _key: &'a [u8],
    ) -> impl Future<Output = Result<()>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn table_insert<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _rowid: i64,
        _data: &'a [u8],
    ) -> impl Future<Output = Result<()>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn delete<'a>(&'a mut self, _cx: &'a Cx) -> impl Future<Output = Result<()>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn payload<'a>(&'a self, _cx: &'a Cx) -> impl Future<Output = Result<Vec<u8>>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn payload_into<'a>(
        &'a self,
        _cx: &'a Cx,
        _buf: &'a mut Vec<u8>,
    ) -> impl Future<Output = Result<()>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn payload_prefix_into<'a>(
        &'a self,
        _cx: &'a Cx,
        _max_prefix_bytes: usize,
        _buf: &'a mut Vec<u8>,
    ) -> impl Future<Output = Result<()>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn rowid<'a>(&'a self, _cx: &'a Cx) -> impl Future<Output = Result<i64>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn eof(&self) -> bool {
        true
    }
}

fn main() {}
