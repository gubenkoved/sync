import shutil
import tempfile
import unittest

from sync.core import ProviderBase
from sync.providers.fs import FSProvider
from tests.provider_tests import ProviderTestBase


class FSProviderTest(ProviderTestBase):
    __test__ = True

    def setUp(self):
        super().setUp()
        self.root_dir = tempfile.mkdtemp()
        self.provider = FSProvider(root_dir=self.root_dir)
        self.addCleanup(lambda: shutil.rmtree(self.root_dir))

    def get_provider(self) -> ProviderBase:
        return self.provider


if __name__ == '__main__':
    unittest.main()
