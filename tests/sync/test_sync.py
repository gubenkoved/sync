from abc import abstractmethod
from unittest import TestCase

import pytest
from sync.core import Syncer
from tests.common import bytes_as_stream, stream_to_bytes


# TODO: update tests to check for sync actions
class SyncTestBase(TestCase):
    __test__ = False

    @abstractmethod
    def get_syncer(self) -> Syncer:
        raise NotImplementedError

    def test_sync_new_files(self):
        syncer = self.get_syncer()
        src_provider = syncer.src_provider
        dst_provider = syncer.dst_provider

        syncer.sync()

        self.assertEqual(0, len(dst_provider.get_state().files))

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        syncer.sync()

        self.assertEqual(1, len(dst_provider.get_state().files))

        with bytes_as_stream(b'data') as stream:
            src_provider.write('bar', stream)

        syncer.sync()

        self.assertEqual(2, len(dst_provider.get_state().files))

    def test_sync_updated_files(self):
        syncer = self.get_syncer()
        src_provider = syncer.src_provider
        dst_provider = syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        syncer.sync()

        self.assertEqual(
            b'data',
            stream_to_bytes(dst_provider.read('foo'))
        )

        with bytes_as_stream(b'updated') as stream:
            src_provider.write('foo', stream)

        syncer.sync()

        self.assertEqual(
            b'updated',
            stream_to_bytes(dst_provider.read('foo'))
        )

    def test_sync_deleted_files(self):
        syncer = self.get_syncer()
        src_provider = syncer.src_provider
        dst_provider = syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        with bytes_as_stream(b'data') as stream:
            src_provider.write('bar', stream)

        syncer.sync()

        self.assertEqual(2, len(dst_provider.get_state().files))

        src_provider.remove('foo')

        syncer.sync()

        self.assertEqual(1, len(dst_provider.get_state().files))

        src_provider.remove('bar')

        syncer.sync()

        self.assertEqual(0, len(dst_provider.get_state().files))

    def test_sync_moved_files(self):
        syncer = self.get_syncer()
        src_provider = syncer.src_provider
        dst_provider = syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        syncer.sync()

        self.assertEqual(1, len(dst_provider.get_state().files))

        # move the file
        src_provider.move('foo', 'bar')

        syncer.sync()

        self.assertEqual(1, len(dst_provider.get_state().files))

    def test_conflicting_updates(self):
        syncer = self.get_syncer()
        src_provider = syncer.src_provider
        dst_provider = syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        with bytes_as_stream(b'data') as stream:
            dst_provider.write('foo', stream)

        # no errors
        syncer.sync()


if __name__ == '__main__':
    pytest.main()
