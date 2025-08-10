import logging
import os
import tempfile
import unittest
import unittest.mock as mock

from sync.cache import InMemoryCache
from sync.core import ProviderBase
from sync.providers.dropbox import DropboxProvider
from sync.providers.fs import FSProvider
from tests.common import bytes_as_stream, cleanup_provider
from tests.providers.test_provider_base import ProviderTestBase

LOGGER = logging.getLogger(__name__)


class FSProviderTest(ProviderTestBase):
    __test__ = True

    def __create_provider(self, root_dir: str) -> DropboxProvider:
        return FSProvider(
            root_dir=root_dir,
            cache=self.cache,
        )

    def setUp(self):
        super().setUp()
        self.root_dir = tempfile.mkdtemp()
        self.cache = InMemoryCache()
        self.provider = self.__create_provider(self.root_dir)
        self.addCleanup(lambda: cleanup_provider(self.provider))

    def get_provider(self) -> ProviderBase:
        return self.provider

    @staticmethod
    def ensure_modification_time_changed(path, previous_mtime):
        while True:
            os.utime(path)
            current_time = os.path.getmtime(path)
            if current_time != previous_mtime:
                break
            LOGGER.debug(
                "repeat changing modification time as it did" "not seem to change..."
            )

    def test_file_hash_is_taken_from_cache_until_file_modified(self):
        with mock.patch("sync.providers.fs.sha256_stream") as patcher:
            patcher.return_value = "test_hash"

            with bytes_as_stream(b"foo") as stream:
                self.provider.write("foo", stream)

            foo_path = os.path.join(self.root_dir, "foo")

            mtime = os.path.getmtime(foo_path)
            LOGGER.info("mtime: %s", mtime)

            _ = self.provider.get_state()
            self.assertEqual(1, patcher.call_count)
            patcher.reset_mock()

            # cache hot
            _ = self.provider.get_state()
            self.assertEqual(0, patcher.call_count)
            patcher.reset_mock()

            # cache hit again
            _ = self.provider.get_state()
            self.assertEqual(0, patcher.call_count)
            patcher.reset_mock()

            # change modification date
            self.ensure_modification_time_changed(foo_path, mtime)

            # cache miss
            _ = self.provider.get_state()
            self.assertEqual(1, patcher.call_count)
            patcher.reset_mock()

            # clear cache
            self.cache.clear()

            _ = self.provider.get_state()
            self.assertEqual(1, patcher.call_count)
            patcher.reset_mock()

    def test_file_hash_is_taken_from_cache_until_file_modified_multiple_files(self):
        with mock.patch("sync.providers.fs.sha256_stream") as patcher:
            patcher.return_value = "test_hash"

            with bytes_as_stream(b"foo") as stream:
                self.provider.write("foo", stream)

            with bytes_as_stream(b"bar") as stream:
                self.provider.write("bar", stream)

            # cache miss
            _ = self.provider.get_state()
            self.assertEqual(2, patcher.call_count)
            patcher.reset_mock()

            # cache hit
            _ = self.provider.get_state()
            self.assertEqual(0, patcher.call_count)
            patcher.reset_mock()

            foo_path = os.path.join(self.root_dir, "foo")
            bar_path = os.path.join(self.root_dir, "bar")

            # modify one
            foo_mtime = os.path.getmtime(foo_path)
            self.ensure_modification_time_changed(foo_path, foo_mtime)

            # cache miss for one
            _ = self.provider.get_state()
            self.assertEqual(1, patcher.call_count)
            patcher.reset_mock()

            # modify both
            foo_mtime = os.path.getmtime(foo_path)
            bar_mtime = os.path.getmtime(foo_path)
            self.ensure_modification_time_changed(foo_path, foo_mtime)
            self.ensure_modification_time_changed(bar_path, bar_mtime)

            # cache miss for both
            _ = self.provider.get_state()
            self.assertEqual(2, patcher.call_count)
            patcher.reset_mock()

            # clear cache
            self.cache.clear()

            # cache miss for both
            _ = self.provider.get_state()
            self.assertEqual(2, patcher.call_count)
            patcher.reset_mock()


if __name__ == "__main__":
    unittest.main()
