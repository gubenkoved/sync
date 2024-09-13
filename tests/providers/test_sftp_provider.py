import os
import os.path
import unittest
import uuid

import pytest

from sync.core import ProviderBase
from tests.common import create_sftp_provider
from tests.providers.test_provider_base import ProviderTestBase


@pytest.mark.sftp
class SFTPProviderTest(ProviderTestBase):
    __test__ = True

    def setUp(self):
        super().setUp()
        root_dir = '/tmp/sync-tests/%s' % uuid.uuid4()
        self.provider = create_sftp_provider(
            root_dir=root_dir,
            host=os.environ['SFTP_HOST'],
            port=int(os.environ.get('SFTP_PORT', '22')),
            username=os.environ['SFTP_USERNAME'],
            key_path=os.environ['SFTP_KEY_PATH'],
        )

    def get_provider(self) -> ProviderBase:
        return self.provider


if __name__ == '__main__':
    unittest.main()
