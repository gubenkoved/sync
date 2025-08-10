import os
import uuid

import pytest

from sync.core import Syncer
from sync.providers.dropbox import DropboxProvider
from sync.providers.sftp import STFPProvider
from tests.sync.test_sync import SyncTestBase


@pytest.mark.dropbox
@pytest.mark.sftp
class DropboxToSftpSyncTest(SyncTestBase):
    __test__ = True

    def setUp(self):
        super().setUp()

        src_provider = DropboxProvider(
            root_dir="/temp/sync-tests/%s" % uuid.uuid4(),
            account_id="test",
            token=os.environ["DROPBOX_TOKEN"],
            app_key=os.environ["DROPBOX_APP_KEY"],
            app_secret=os.environ["DROPBOX_APP_SECRET"],
            is_refresh_token=True,
        )
        dst_provider = STFPProvider(
            root_dir="/tmp/sync-tests/%s" % uuid.uuid4(),
            host=os.environ["SFTP_HOST"],
            port=int(os.environ["SFTP_PORT"]),
            username=os.environ["SFTP_USERNAME"],
            key_path=os.environ["SFTP_KEY_PATH"],
        )

        self._syncer = Syncer(
            src_provider,
            dst_provider,
        )

    @property
    def syncer(self):
        return self._syncer
