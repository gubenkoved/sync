import abc
import io
import os
import unittest
from typing import BinaryIO

from sync.provider import (
    ProviderBase,
    FileNotFoundProviderError,
    FileAlreadyExistsError,
)
from sync.hashing import HashType


def bytes_as_stream(data: bytes) -> BinaryIO:
    return io.BytesIO(data)


def random_bytes_stream(count: int) -> BinaryIO:
    data = os.urandom(count)
    return bytes_as_stream(data)


def stream_to_bytes(stream: BinaryIO) -> bytes:
    return stream.read()


class ProviderTestBase(unittest.TestCase):
    __test__ = False

    @abc.abstractmethod
    def get_provider(self) -> ProviderBase:
        raise NotImplementedError

    def test_write_read(self):
        provider = self.get_provider()
        with bytes_as_stream(b'test') as stream:
            provider.write('foo', stream)
            with provider.read('foo') as read_stream:
                self.assertEqual(
                    b'test',
                    stream_to_bytes(read_stream)
                )
        state = provider.get_state()
        self.assertEqual(1, len(state.files))
        self.assertIn('foo', state.files)

    def test_write_nested(self):
        provider = self.get_provider()
        with bytes_as_stream(b'test') as stream1:
            with bytes_as_stream(b'test2') as stream2:
                provider.write('foo/bar/baz.file', stream1)
                provider.write('foo/bar.file', stream2)
        state = provider.get_state()
        self.assertEqual(2, len(state.files))
        self.assertIn('foo/bar.file', state.files)
        self.assertIn('foo/bar/baz.file', state.files)

    def test_remove(self):
        provider = self.get_provider()
        with bytes_as_stream(b'test') as stream1:
            with bytes_as_stream(b'test2') as stream2:
                provider.write('foo/bar/baz.file', stream1)
                provider.write('foo/bar.file', stream2)
                provider.remove('foo/bar/baz.file')
                provider.remove('foo/bar.file')

        state = provider.get_state()
        self.assertEqual(0, len(state.files))

        # check file does not exist
        self.assertRaises(
            FileNotFoundProviderError,
            lambda: provider.get_file_state('foo/bar.file')
        )
        self.assertRaises(
            FileNotFoundProviderError,
            lambda: provider.get_file_state('foo/bar/baz.file')
        )

    def test_read_missing_file(self):
        provider = self.get_provider()
        self.assertRaises(
            FileNotFoundProviderError,
            lambda: provider.read('foo.file')
        )
        self.assertRaises(
            FileNotFoundProviderError,
            lambda: provider.get_file_state('foo.file')
        )

    def test_remove_missing_file(self):
        provider = self.get_provider()
        self.assertRaises(
            FileNotFoundProviderError,
            lambda: provider.remove('foo.file')
        )

    def test_compute_native_hash(self):
        provider = self.get_provider()
        with bytes_as_stream(b'test1') as stream1:
            with bytes_as_stream(b'test2') as stream2:
                provider.write('file1', stream1)
                provider.write('file2', stream2)

        state1 = provider.get_file_state('file1')
        state2 = provider.get_file_state('file2')

        self.assertIsNotNone(state1.content_hash)
        self.assertIsNotNone(state2.content_hash)
        self.assertNotEqual(state1.content_hash, state2.content_hash)

    def test_compute_sha256_hash_if_supported(self):
        provider = self.get_provider()

        if HashType.SHA256 not in provider.supported_hash_types():
            self.skipTest('not supported')

        with bytes_as_stream(b'test') as stream:
            provider.write('file', stream)

        hash_result = provider.compute_hash('file', HashType.SHA256)
        self.assertEqual(
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',
            hash_result
        )

    def test_compute_dropbox_sha256_hash_if_supported(self):
        provider = self.get_provider()

        if HashType.DROPBOX_SHA256 not in provider.supported_hash_types():
            self.skipTest('not supported')

        with bytes_as_stream(b'test') as stream:
            provider.write('file', stream)

        hash_result = provider.compute_hash('file', HashType.DROPBOX_SHA256)
        self.assertEqual(
            '954d5a49fd70d9b8bcdb35d252267829957f7ef7fa6c74f88419bdc5e82209f4',
            hash_result
        )

    def test_move(self):
        provider = self.get_provider()

        with bytes_as_stream(b'test') as stream:
            provider.write('foo', stream)

        provider.move('foo', 'bar')

        state = provider.get_state()

        self.assertEqual(1, len(state.files))
        self.assertIn('bar', state.files)

    def test_move_non_existing(self):
        provider = self.get_provider()

        self.assertRaises(
            FileNotFoundProviderError,
            lambda: provider.move('foo', 'bar')
        )

    def test_move_would_overwrite(self):
        provider = self.get_provider()

        with bytes_as_stream(b'foo') as stream:
            provider.write('foo', stream)

        with bytes_as_stream(b'bar') as stream:
            provider.write('bar', stream)

        state_before = provider.get_state()

        self.assertRaises(
            FileAlreadyExistsError,
            lambda: provider.move('foo', 'bar')
        )

        state_after = provider.get_state()

        self.assertEqual(state_before, state_after)


if __name__ == '__main__':
    unittest.main()
