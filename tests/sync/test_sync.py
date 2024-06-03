import abc
import os.path
from unittest import TestCase

import pytest

from sync.core import (
    UploadSyncAction, DownloadSyncAction,
    RemoveOnDestinationSyncAction,
    MoveOnSourceSyncAction, MoveOnDestinationSyncAction,
    ResolveConflictSyncAction,
)
from tests.common import bytes_as_stream, stream_to_bytes, random_bytes_stream


class SyncTestBase(TestCase):
    __test__ = False

    @property
    @abc.abstractmethod
    def syncer(self):
        raise NotImplementedError

    def tearDown(self):
        super().tearDown()

        state_file_path = self.syncer.get_state_file_path()

        if os.path.exists(state_file_path):
            os.remove(state_file_path)

    def ensure_same_state(self):
        src_state = self.syncer.src_provider.get_state()
        dst_state = self.syncer.dst_provider.get_state()

        # ensure same file lists
        # note that we can not directly compare different providers StorageState
        # as the content hash is abstract and can mean different things
        self.assertEqual(set(src_state.files), set(dst_state.files))

        for path in src_state.files:
            self.assertTrue(
                self.syncer.compare(path), 'files are different by path %s' % path)

    def do_sync(self, expected_sync_actions=None, ensure_same_state=True):
        sync_actions = self.syncer.sync()
        if expected_sync_actions is not None:
            self.assertCountEqual(expected_sync_actions, sync_actions)
        if ensure_same_state:
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

    def test_limited_depth(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with random_bytes_stream() as stream:
            src_provider.write('file1', stream)
            src_provider.write('foo/file1', stream)
            src_provider.write('foo/file2', stream)
            src_provider.write('foo/bar/file1', stream)
            src_provider.write('foo/bar/file2', stream)

        # normally depth is set in the constructor, but it is okay currently
        # to modify it for simplicity
        self.syncer.depth = 1

        self.do_sync([
            UploadSyncAction('file1')
        ], ensure_same_state=False)

        self.syncer.depth = 2

        self.do_sync([
            ResolveConflictSyncAction('file1'),
            UploadSyncAction('foo/file1'),
            UploadSyncAction('foo/file2'),
        ], ensure_same_state=False)


if __name__ == '__main__':
    pytest.main()
