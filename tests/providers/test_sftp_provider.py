import logging
import os
import os.path
import unittest
import uuid

import pytest

from sync.core import ProviderBase
from sync.provider import FolderNotFoundProviderError
from sync.providers.sftp import STFPProvider
from tests.providers.test_provider_base import ProviderTestBase

LOGGER = logging.getLogger(__name__)


@pytest.mark.sftp
class SFTPProviderTest(ProviderTestBase):
    __test__ = True

    def __create_provider(self, root_dir: str):
        return STFPProvider(
            root_dir=root_dir,
            host=os.environ["SFTP_HOST"],
            port=int(os.environ.get("SFTP_PORT", "22")),
            username=os.environ["SFTP_USERNAME"],
            key_path=os.environ["SFTP_KEY_PATH"],
        )

    def setUp(self):
        super().setUp()
        self.subdir_name = str(uuid.uuid4())
        self.root_dir = "/tmp/sync-tests/%s" % self.subdir_name
        self.provider = self.__create_provider(self.root_dir)

        def cleanup():
            cleanup_provider = self.__create_provider(root_dir="/tmp/sync-tests")

            try:
                cleanup_provider.remove_folder(self.subdir_name)
            except FolderNotFoundProviderError:
                LOGGER.debug(f"Folder {self.root_dir} not found")

        self.addCleanup(cleanup)

    def get_provider(self) -> ProviderBase:
        return self.provider


if __name__ == "__main__":
    unittest.main()
