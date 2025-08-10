import logging
import os
import os.path
import unittest
import uuid

import pytest

from sync.core import ProviderBase
from sync.providers.dropbox import DropboxProvider
from tests.common import cleanup_provider
from tests.providers.test_provider_base import ProviderTestBase

LOGGER = logging.getLogger(__name__)


@pytest.mark.dropbox
class DropboxProviderTest(ProviderTestBase):
    __test__ = True

    def __create_provider(self, root_dir: str) -> DropboxProvider:
        return DropboxProvider(
            root_dir=root_dir,
            account_id="test",
            token=os.environ["DROPBOX_TOKEN"],
            app_key=os.environ["DROPBOX_APP_KEY"],
            app_secret=os.environ["DROPBOX_APP_SECRET"],
            is_refresh_token=True,
        )

    def setUp(self):
        super().setUp()
        self.root_dir = "/temp/sync-tests/%s" % str(uuid.uuid4())
        self.provider = self.__create_provider(self.root_dir)
        self.addCleanup(lambda: cleanup_provider(self.provider))

    def get_provider(self) -> ProviderBase:
        return self.provider


if __name__ == "__main__":
    unittest.main()
