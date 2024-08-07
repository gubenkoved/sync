import os
import os.path
import unittest
import uuid

import pytest

from sync.core import ProviderBase
from sync.providers.dropbox import DropboxProvider
from tests.providers.test_provider_base import ProviderTestBase


@pytest.mark.dropbox
class DropboxProviderTest(ProviderTestBase):
    __test__ = True

    def setUp(self):
        super().setUp()
        root_dir = '/temp/sync-tests/%s' % uuid.uuid4()
        self.provider = DropboxProvider(
            root_dir=root_dir,
            account_id='test',
            token=os.environ['DROPBOX_TOKEN'],
            app_key=os.environ['DROPBOX_APP_KEY'],
            app_secret=os.environ['DROPBOX_APP_SECRET'],
            is_refresh_token=True,
        )
        # TODO: how to delete whole dir?

    def get_provider(self) -> ProviderBase:
        return self.provider


if __name__ == '__main__':
    unittest.main()
