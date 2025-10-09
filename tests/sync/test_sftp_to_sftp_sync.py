import os
import uuid

import pytest

from sync.core import Syncer
from sync.providers.sftp import STFPProvider
from tests.sync.test_sync import SyncTestBase


@pytest.mark.sftp
class SftpToSftpSyncTest(SyncTestBase):
    __test__ = True

    def setUp(self):
        super().setUp()

        src_dir = "/tmp/sync-tests/%s" % uuid.uuid4()
        src_provider = STFPProvider(
            root_dir=src_dir,
            host=os.environ["SFTP_HOST"],
            port=int(os.environ["SFTP_PORT"]),
            username=os.environ["SFTP_USERNAME"],
            key_path=os.environ["SFTP_KEY_PATH"],
        )

        dst_dir = "/tmp/sync-tests/%s" % uuid.uuid4()
        dst_provider = STFPProvider(
            root_dir=dst_dir,
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
