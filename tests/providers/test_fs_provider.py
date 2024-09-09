import os
import shutil
import tempfile
import unittest
import unittest.mock as mock

from sync.cache import InMemoryCache
from sync.core import ProviderBase
from sync.providers.fs import FSProvider
from tests.common import bytes_as_stream
from tests.providers.test_provider_base import ProviderTestBase


class FSProviderTest(ProviderTestBase):
    __test__ = True

    def setUp(self):
        super().setUp()
        self.root_dir = tempfile.mkdtemp()
        self.cache = InMemoryCache()
        self.provider = FSProvider(root_dir=self.root_dir, cache=self.cache)
        self.addCleanup(lambda: shutil.rmtree(self.root_dir))

    def get_provider(self) -> ProviderBase:
        return self.provider

    def test_file_hash_is_taken_from_cache_until_file_modified(self):
        with mock.patch('sync.providers.fs.sha256_stream') as patcher:
            patcher.return_value = 'test_hash'

            with bytes_as_stream(b'foo') as stream:
                self.provider.write('foo', stream)

            with bytes_as_stream(b'bar') as stream:
                self.provider.write('bar', stream)

            _ = self.provider.get_state()
            self.assertEqual(2, patcher.call_count)
            patcher.reset_mock()

            # cache hot on both
            _ = self.provider.get_state()
            self.assertEqual(0, patcher.call_count)
            patcher.reset_mock()

            # change modification date for one
            foo_path = os.path.join(self.root_dir, 'foo')
            bar_path = os.path.join(self.root_dir, 'bar')

            os.utime(foo_path)

            _ = self.provider.get_state()
            self.assertEqual(1, patcher.call_count)
            patcher.reset_mock()

            os.utime(bar_path)

            _ = self.provider.get_state()
            self.assertEqual(1, patcher.call_count)
            patcher.reset_mock()

            # clear cache
            self.cache.clear()

            _ = self.provider.get_state()
            self.assertEqual(2, patcher.call_count)
            patcher.reset_mock()


if __name__ == '__main__':
    unittest.main()
