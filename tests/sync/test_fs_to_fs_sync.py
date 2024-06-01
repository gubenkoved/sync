import os
import shutil
import tempfile

from sync.core import Syncer
from sync.providers.fs import FSProvider
from tests.sync.test_sync import SyncTestBase


class FsToFsSyncTest(SyncTestBase):
    __test__ = True

    def setUp(self):
        super().setUp()

        src_dir = tempfile.mkdtemp()
        dst_dir = tempfile.mkdtemp()

        src_provider = FSProvider(root_dir=src_dir)
        dst_provider = FSProvider(root_dir=dst_dir)

        self.addCleanup(lambda: shutil.rmtree(src_dir))
        self.addCleanup(lambda: shutil.rmtree(dst_dir))

        self._syncer = Syncer(
            src_provider,
            dst_provider,
        )

        state_file_path = self._syncer.get_state_file_path()
        self.addCleanup(lambda: os.remove(state_file_path))

    @property
    def syncer(self):
        return self._syncer
