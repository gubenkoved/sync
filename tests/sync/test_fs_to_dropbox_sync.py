import os
import shutil
import tempfile
import uuid

from sync.core import Syncer
from sync.providers.dropbox import DropboxProvider
from sync.providers.fs import FSProvider
from tests.sync.test_sync import SyncTestBase


class FsToDropboxSyncTest(SyncTestBase):
    __test__ = True

    def setUp(self):
        super().setUp()

        src_dir = tempfile.mkdtemp()
        src_provider = FSProvider(root_dir=src_dir)
        self.addCleanup(lambda: shutil.rmtree(src_dir))

        root_dir = '/temp/sync-tests/%s' % uuid.uuid4()
        dst_provider = DropboxProvider(
            root_dir=root_dir,
            account_id='test',
            token=os.environ['DROPBOX_TOKEN'],
            app_key=os.environ['DROPBOX_APP_KEY'],
            app_secret=os.environ['DROPBOX_APP_SECRET'],
            is_refresh_token=True,
        )

        self._syncer = Syncer(
            src_provider,
            dst_provider,
        )

        state_file_path = self._syncer.get_state_file_path()
        self.addCleanup(lambda: os.remove(state_file_path))

    @property
    def syncer(self):
        return self._syncer
