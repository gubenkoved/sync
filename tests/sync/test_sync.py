import abc
from abc import abstractmethod
from unittest import TestCase

import pytest
from sync.core import Syncer
from tests.common import bytes_as_stream, stream_to_bytes
from sync.core import (
    UploadSyncAction, DownloadSyncAction,
    RemoveOnSourceSyncAction, RemoveOnDestinationSyncAction,
    MoveOnSourceSyncAction, MoveOnDestinationSyncAction,
    ResolveConflictSyncAction,
)


class SyncTestBase(TestCase):
    __test__ = False

    @property
    @abc.abstractmethod
    def syncer(self):
        raise NotImplementedError

    def ensure_same_state(self):
        src_state = self.syncer.src_provider.get_state()
        dst_state = self.syncer.dst_provider.get_state()

        self.assertEqual(src_state, dst_state)

    def do_sync(self, expected_sync_actions=None):
        sync_actions = self.syncer.sync()
        if expected_sync_actions is not None:
            self.assertCountEqual(expected_sync_actions, sync_actions)
        self.ensure_same_state()

    def test_sync_new_files(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        self.do_sync(expected_sync_actions=[])

        self.assertEqual(0, len(dst_provider.get_state().files))

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        self.do_sync(expected_sync_actions=[
            UploadSyncAction('foo')
        ])

        self.assertEqual(1, len(dst_provider.get_state().files))

        with bytes_as_stream(b'data') as stream:
            src_provider.write('bar', stream)

        self.do_sync([
            UploadSyncAction('bar')
        ])

        self.assertEqual(2, len(dst_provider.get_state().files))

        # add file on destination
        with bytes_as_stream(b'data') as stream:
            dst_provider.write('baz', stream)

        self.do_sync(expected_sync_actions=[
            DownloadSyncAction('baz')
        ])

        self.assertEqual(3, len(src_provider.get_state().files))

    def test_sync_updated_files(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        self.do_sync([
            UploadSyncAction('foo')
        ])

        self.assertEqual(
            b'data',
            stream_to_bytes(dst_provider.read('foo'))
        )

        with bytes_as_stream(b'updated') as stream:
            src_provider.write('foo', stream)

        self.do_sync([
            UploadSyncAction('foo')
        ])

        self.assertEqual(
            b'updated',
            stream_to_bytes(dst_provider.read('foo'))
        )

    def test_sync_deleted_files(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        with bytes_as_stream(b'data') as stream:
            src_provider.write('bar', stream)

        self.do_sync([
            UploadSyncAction('foo'),
            UploadSyncAction('bar'),
        ])

        self.assertEqual(2, len(dst_provider.get_state().files))

        src_provider.remove('foo')

        self.do_sync([
            RemoveOnDestinationSyncAction('foo'),
        ])

        self.assertEqual(1, len(dst_provider.get_state().files))

        src_provider.remove('bar')

        self.do_sync([
            RemoveOnDestinationSyncAction('bar'),
        ])

        self.assertEqual(0, len(dst_provider.get_state().files))

    def test_sync_moved_files(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        self.do_sync([
            UploadSyncAction('foo'),
        ])

        self.assertEqual(1, len(dst_provider.get_state().files))

        # move the file on source
        src_provider.move('foo', 'bar')

        self.do_sync([
            MoveOnDestinationSyncAction('foo', 'bar')
        ])

        self.assertEqual(1, len(dst_provider.get_state().files))

        # move the file on destination
        dst_provider.move('bar', 'baz')

        self.do_sync([
            MoveOnSourceSyncAction('bar', 'baz')
        ])

        self.assertEqual(1, len(dst_provider.get_state().files))

    def test_conflicting_updates(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        with bytes_as_stream(b'data') as stream:
            dst_provider.write('foo', stream)

        self.do_sync([
            ResolveConflictSyncAction('foo'),
        ])


if __name__ == '__main__':
    pytest.main()
