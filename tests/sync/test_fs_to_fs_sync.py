from sync.core import Syncer

from tests.sync.test_sync import SyncTestBase


class FsToFsSyncTest(SyncTestBase):
    __test__ = True

    def get_syncer(self) -> Syncer:
        raise NotImplementedError
