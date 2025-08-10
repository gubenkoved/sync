import logging
import os
import os.path
import unittest
import uuid

import pytest

from sync.core import ProviderBase
from sync.provider import FolderNotFoundProviderError
from sync.providers.dropbox import DropboxProvider
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
        self.subdir_name = str(uuid.uuid4())
        self.root_dir = "/temp/sync-tests/%s" % self.subdir_name
        self.provider = self.__create_provider(self.root_dir)

        def cleanup():
            cleanup_provider = self.__create_provider(
                root_dir="/temp/sync-tests",
            )
            try:
                cleanup_provider.remove_folder(self.subdir_name)
            except FolderNotFoundProviderError:
                LOGGER.debug(f"Folder {self.root_dir} not found")

        self.addCleanup(cleanup)

    def get_provider(self) -> ProviderBase:
        return self.provider


if __name__ == "__main__":
    unittest.main()
