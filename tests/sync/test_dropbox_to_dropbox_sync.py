import os
import uuid

import pytest

from sync.core import Syncer
from sync.providers.dropbox import DropboxProvider
from tests.sync.test_sync import SyncTestBase


@pytest.mark.dropbox
class DropboxToDropboxSyncTest(SyncTestBase):
    __test__ = True

    def setUp(self):
        super().setUp()

        src_provider = DropboxProvider(
            root_dir="/temp/sync-tests/%s" % uuid.uuid4(),
            account_id="test_src",
            token=os.environ["DROPBOX_TOKEN"],
            app_key=os.environ["DROPBOX_APP_KEY"],
            app_secret=os.environ["DROPBOX_APP_SECRET"],
            is_refresh_token=True,
        )
        dst_provider = DropboxProvider(
            root_dir="/temp/sync-tests/%s" % uuid.uuid4(),
            account_id="test_dst",
            token=os.environ["DROPBOX_TOKEN"],
            app_key=os.environ["DROPBOX_APP_KEY"],
            app_secret=os.environ["DROPBOX_APP_SECRET"],
            is_refresh_token=True,
        )

        self._syncer = Syncer(
            src_provider,
            dst_provider,
        )

    @property
    def syncer(self):
        return self._syncer
