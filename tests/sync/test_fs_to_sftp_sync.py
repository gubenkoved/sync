import os
import shutil
import tempfile
import uuid

from sync.core import Syncer
from sync.providers.sftp import STFPProvider
from sync.providers.fs import FSProvider
from tests.sync.test_sync import SyncTestBase


class FsToSftpSyncTest(SyncTestBase):
    __test__ = True

    def setUp(self):
        super().setUp()

        src_dir = tempfile.mkdtemp()
        src_provider = FSProvider(root_dir=src_dir)
        self.addCleanup(lambda: shutil.rmtree(src_dir))

        root_dir = '/tmp/sync-tests/%s' % uuid.uuid4()
        dst_provider = STFPProvider(
            root_dir=root_dir,
            host=os.environ['SFTP_HOST'],
            port=int(os.environ['SFTP_PORT']),
            username=os.environ['SFTP_USERNAME'],
            key_path=os.environ['SFTP_KEY_PATH'],
        )

        self._syncer = Syncer(
            src_provider,
            dst_provider,
        )

    @property
    def syncer(self):
        return self._syncer
