import abc
import logging
import time
import unicodedata
import unittest
import uuid

import pytest

from sync.hashing import HashType
from sync.provider import (
    FileAlreadyExistsError,
    FileNotFoundProviderError,
    ProviderBase,
    ProviderError,
    SafeUpdateSupportMixin,
)
from sync.state import StorageState
from tests.common import bytes_as_stream, random_bytes_stream, stream_to_bytes

LOGGER = logging.getLogger(__name__)


class ProviderTestBase(unittest.TestCase):
    __test__ = False

    @abc.abstractmethod
    def get_provider(self) -> ProviderBase:
        raise NotImplementedError

    def assert_storage_state_equal(self, expected: StorageState, actual: StorageState):
        self.assertEqual(len(expected.files), len(actual.files))
        self.assertEqual(set(expected.files), set(actual.files))

        for path in expected.files:
            expected_file_state = expected.files[path]
            actual_file_state = actual.files[path]
            self.assertEqual(
                expected_file_state.content_hash, actual_file_state.content_hash
            )

    def test_write_read(self):
        provider = self.get_provider()
        with bytes_as_stream(b"test") as stream:
            provider.write("foo", stream)
            with provider.read("foo") as read_stream:
                self.assertEqual(b"test", stream_to_bytes(read_stream))
        state = provider.get_state()
        self.assertEqual(1, len(state.files))
        self.assertIn("foo", state.files)

    def test_rewrite(self):
        provider = self.get_provider()

        with bytes_as_stream(b"test1") as stream:
            provider.write("foo", stream)

        with bytes_as_stream(b"test2") as stream:
            provider.write("foo", stream)

        with provider.read("foo") as read_stream:
            self.assertEqual(b"test2", stream_to_bytes(read_stream))
        state = provider.get_state()
        self.assertEqual(1, len(state.files))
        self.assertIn("foo", state.files)

    def test_write_nested(self):
        provider = self.get_provider()
        state = provider.get_state()
        self.assertEqual(0, len(state.files))
        with bytes_as_stream(b"test") as stream1:
            with bytes_as_stream(b"test2") as stream2:
                provider.write("foo/bar/baz.file", stream1)
                provider.write("foo/bar.file", stream2)
        state = provider.get_state()
        self.assertEqual(2, len(state.files))
        self.assertIn("foo/bar.file", state.files)
        self.assertIn("foo/bar/baz.file", state.files)

    @pytest.mark.slow
    def test_many_sub_directories(self):
        provider = self.get_provider()
        n = 16
        for dir_idx in range(n):
            with random_bytes_stream(128) as stream:
                provider.write("dir_%s/data" % dir_idx, stream)
        state = provider.get_state()
        self.assertEqual(n, len(state.files))

    def test_remove(self):
        provider = self.get_provider()
        with bytes_as_stream(b"test") as stream1:
            with bytes_as_stream(b"test2") as stream2:
                provider.write("foo/bar/baz.file", stream1)
                provider.write("foo/bar.file", stream2)
                provider.remove("foo/bar/baz.file")
                provider.remove("foo/bar.file")

        state = provider.get_state()
        self.assertEqual(0, len(state.files))

        # check file does not exist
        self.assertRaises(
            FileNotFoundProviderError, lambda: provider.get_file_state("foo/bar.file")
        )
        self.assertRaises(
            FileNotFoundProviderError,
            lambda: provider.get_file_state("foo/bar/baz.file"),
        )

    def test_read_missing_file(self):
        provider = self.get_provider()
        self.assertRaises(FileNotFoundProviderError, lambda: provider.read("foo.file"))
        self.assertRaises(
            FileNotFoundProviderError, lambda: provider.get_file_state("foo.file")
        )

    def test_remove_missing_file(self):
        provider = self.get_provider()
        self.assertRaises(
            FileNotFoundProviderError, lambda: provider.remove("foo.file")
        )

    def test_compute_native_hash(self):
        provider = self.get_provider()
        with bytes_as_stream(b"test1") as stream1:
            with bytes_as_stream(b"test2") as stream2:
                provider.write("file1", stream1)
                provider.write("file2", stream2)

        state1 = provider.get_file_state("file1")
        state2 = provider.get_file_state("file2")

        self.assertIsNotNone(state1.content_hash)
        self.assertIsNotNone(state2.content_hash)
        self.assertNotEqual(state1.content_hash, state2.content_hash)

    def test_compute_sha256_hash_if_supported(self):
        provider = self.get_provider()

        if HashType.SHA256 not in provider.supported_hash_types():
            self.skipTest("not supported")

        with bytes_as_stream(b"test") as stream:
            provider.write("file", stream)

        hash_result = provider.compute_hash("file", HashType.SHA256)
        self.assertEqual(
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
            hash_result,
        )

    def test_compute_dropbox_sha256_hash_if_supported(self):
        provider = self.get_provider()

        if HashType.DROPBOX_SHA256 not in provider.supported_hash_types():
            self.skipTest("not supported")

        with bytes_as_stream(b"test") as stream:
            provider.write("file", stream)

        hash_result = provider.compute_hash("file", HashType.DROPBOX_SHA256)
        self.assertEqual(
            "954d5a49fd70d9b8bcdb35d252267829957f7ef7fa6c74f88419bdc5e82209f4",
            hash_result,
        )

    def test_move(self):
        provider = self.get_provider()

        with bytes_as_stream(b"test") as stream:
            provider.write("foo", stream)

        provider.move("foo", "bar")

        state = provider.get_state()

        self.assertEqual(1, len(state.files))
        self.assertIn("bar", state.files)

    def test_move_non_existing(self):
        provider = self.get_provider()

        self.assertRaises(
            FileNotFoundProviderError, lambda: provider.move("foo", "bar")
        )

    def test_move_would_overwrite(self):
        provider = self.get_provider()

        with bytes_as_stream(b"foo") as stream:
            provider.write("foo", stream)

        with bytes_as_stream(b"bar") as stream:
            provider.write("bar", stream)

        state_before = provider.get_state()

        self.assertRaises(FileAlreadyExistsError, lambda: provider.move("foo", "bar"))

        state_after = provider.get_state()

        self.assert_storage_state_equal(state_before, state_after)

    def test_case_only_change_movement(self):
        provider = self.get_provider()

        with bytes_as_stream(b"foo") as stream:
            provider.write("foo", stream)

        state_before = provider.get_state()
        self.assertCountEqual(["foo"], state_before.files.keys())

        provider.move("foo", "FOO")

        state_before = provider.get_state()
        self.assertCountEqual(["FOO"], state_before.files.keys())

    def test_safe_update_if_supported(self):
        provider = self.get_provider()

        if not isinstance(provider, SafeUpdateSupportMixin):
            self.skipTest("not supported")

        # to make static analyzers happy
        assert isinstance(provider, ProviderBase) and isinstance(
            provider, SafeUpdateSupportMixin
        )

        with bytes_as_stream(b"test") as stream:
            provider.write("foo.file", stream)

        file_state = provider.get_file_state("foo.file")

        # avoid timing error -- if file is modified very fast its modification
        # time can be unchanged
        attempt = 1
        while attempt < 3:
            with bytes_as_stream(b"test2") as stream:
                provider.update("foo.file", stream, file_state.revision)
            file_state_after = provider.get_file_state("foo.file")

            if file_state.revision != file_state_after.revision:
                break

            LOGGER.warning("same revision after the update!")
            time.sleep(0.1)
            attempt += 1

        # make sure update happened
        with provider.read("foo.file") as stream:
            self.assertEqual(b"test2", stream_to_bytes(stream))

        # try update with outdated revision tag
        def try_update():
            with bytes_as_stream(b"test3") as stream:
                provider.update("foo.file", stream, file_state.revision)

        self.assertRaisesRegex(
            ProviderError,
            "conflict",
            try_update,
        )

    @pytest.mark.slow
    def test_create_and_delete_many_files(self):
        provider = self.get_provider()

        count = 128
        for file_idx in range(count):
            with random_bytes_stream(1024 * 1024) as data_stream:
                provider.write("file_%s" % file_idx, data_stream)

        state = provider.get_state()
        self.assertEqual(count, len(state.files))

        for file_idx in range(count):
            provider.remove("file_%s" % file_idx)

        state = provider.get_state()
        self.assertEqual(0, len(state.files))

    def test_move_when_only_case_is_different(self):
        provider = self.get_provider()

        with bytes_as_stream(b"data") as stream:
            provider.write("foo/data", stream)

        provider.move("foo/data", "foo/Data")

        state = provider.get_state()
        self.assertEqual({"foo/Data"}, set(state.files))

    def test_case_sensitivity_if_supported(self):
        provider = self.get_provider()

        if not provider.is_case_sensitive():
            self.skipTest("not supported")

        with bytes_as_stream(b"data1") as stream:
            provider.write("foo/data", stream)

        with bytes_as_stream(b"data2") as stream:
            provider.write("foo/Data", stream)

        with bytes_as_stream(b"data3") as stream:
            provider.write("Foo/Data", stream)

        state = provider.get_state()
        self.assertEqual({"foo/data", "foo/Data", "Foo/Data"}, set(state.files))

        self.assertEqual(b"data1", stream_to_bytes(provider.read("foo/data")))
        self.assertEqual(b"data2", stream_to_bytes(provider.read("foo/Data")))
        self.assertEqual(b"data3", stream_to_bytes(provider.read("Foo/Data")))

    def test_two_files_use_different_parent_dir_casing(self):
        provider = self.get_provider()

        with bytes_as_stream(b"data1") as stream:
            provider.write("foo/data1", stream)

        with bytes_as_stream(b"data2") as stream:
            provider.write("Foo/data2", stream)

        state = provider.get_state()

        if provider.is_case_sensitive():
            self.assertEqual({"foo/data1", "Foo/data2"}, set(state.files))
        else:
            self.assertEqual({"foo/data1", "foo/data2"}, set(state.files))

    def test_possible_to_download_by_different_cased_name_if_provider_case_insensitive(
        self,
    ):
        provider = self.get_provider()

        if provider.is_case_sensitive():
            self.skipTest("not supported")

        with bytes_as_stream(b"data1") as stream:
            provider.write("foo/data1", stream)

        self.assertEqual(b"data1", stream_to_bytes(provider.read("foo/data1")))
        self.assertEqual(b"data1", stream_to_bytes(provider.read("foo/Data1")))
        self.assertEqual(b"data1", stream_to_bytes(provider.read("Foo/Data1")))
        self.assertEqual(b"data1", stream_to_bytes(provider.read("FOO/DATA1")))

    def test_move_to_non_existing_directory(self):
        provider = self.get_provider()

        with bytes_as_stream(b"data1") as stream:
            provider.write("foo/data", stream)

        provider.move("foo/data", "bar/data")

        self.assertEqual({"bar/data"}, set(provider.get_state().files))

    def test_unicode_normalization(self):
        provider = self.get_provider()

        paths = [
            "йогурт",
            "ёшкин-кот"
            # hello in chinese
            "你好",
            # Dropbox actually does not support emoji in the path and answer with
            # malformed_path error
            # '😊',
        ]

        normal_forms = [
            "NFD",
            "NFC",
        ]

        for path in paths:
            for write_form in normal_forms:
                for read_form in normal_forms:
                    uniq = uuid.uuid4().hex

                    path_in_write_form = unicodedata.normalize(write_form, path)
                    path_in_read_form = unicodedata.normalize(read_form, path)

                    write_path = f"{uniq}/{path_in_write_form}"
                    read_path = f"{uniq}/{path_in_read_form}"

                    with bytes_as_stream(b"whatever") as stream:
                        provider.write(write_path, stream)

                    read_data = provider.read(read_path).read()

                    self.assertEqual(b"whatever", read_data)


if __name__ == "__main__":
    unittest.main()
