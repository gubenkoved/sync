import shutil
import tempfile

from sync.core import Syncer
from sync.providers.fs import FSProvider
from tests.sync.test_sync import SyncTestBase


class FsToFsSyncTest(SyncTestBase):
    __test__ = True

    def get_syncer(self) -> Syncer:
        src_dir = tempfile.mkdtemp()
        dst_dir = tempfile.mkdtemp()

        src_provider = FSProvider(root_dir=src_dir)
        dst_provider = FSProvider(root_dir=dst_dir)

        self.addCleanup(lambda: shutil.rmtree(src_dir))
        self.addCleanup(lambda: shutil.rmtree(dst_dir))

        return Syncer(
            src_provider,
            dst_provider,
        )
