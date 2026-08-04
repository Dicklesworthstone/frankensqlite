use fsqlite_error::FrankenError;
use fsqlite_pager::{CheckpointPageWriter, traits::WalFuture};
use fsqlite_types::PageNumber;
use fsqlite_types::cx::Cx;

struct ExternalCheckpointWriter;

impl CheckpointPageWriter for ExternalCheckpointWriter {
    fn write_page<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _page_no: PageNumber,
        _data: &'a [u8],
    ) -> WalFuture<'a, ()> {
        Box::pin(std::future::ready(Err(FrankenError::Unsupported)))
    }

    fn truncate<'a>(&'a mut self, _cx: &'a Cx, _n_pages: u32) -> WalFuture<'a, ()> {
        Box::pin(std::future::ready(Err(FrankenError::Unsupported)))
    }

    fn sync<'a>(&'a mut self, _cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(std::future::ready(Err(FrankenError::Unsupported)))
    }
}

fn main() {}
