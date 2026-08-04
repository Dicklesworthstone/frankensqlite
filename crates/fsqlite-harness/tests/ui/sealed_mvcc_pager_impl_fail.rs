use std::future::Future;

use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::{JournalMode, MockTransaction, MvccPager, TransactionMode, WalBackend};
use fsqlite_types::cx::Cx;

struct ExternalPager;

impl MvccPager for ExternalPager {
    type Txn = MockTransaction;

    fn begin<'a>(
        &'a self,
        _cx: &'a Cx,
        _mode: TransactionMode,
    ) -> impl Future<Output = Result<Self::Txn>> + 'a {
        std::future::ready(Err(FrankenError::Unsupported))
    }

    fn journal_mode(&self) -> JournalMode {
        JournalMode::Delete
    }

    fn is_readonly(&self) -> bool {
        true
    }

    fn set_journal_mode<'a>(
        &'a self,
        _cx: &'a Cx,
        mode: JournalMode,
    ) -> impl Future<Output = Result<JournalMode>> + 'a {
        std::future::ready(Ok(mode))
    }

    fn set_wal_backend(&self, _backend: Box<dyn WalBackend>) -> Result<()> {
        Ok(())
    }
}

fn main() {}
