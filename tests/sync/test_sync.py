from abc import abstractmethod
from unittest import TestCase

import pytest
from sync.core import Syncer


class SyncTestBase(TestCase):
    __test__ = False

    @abstractmethod
    def get_syncer(self) -> Syncer:
        raise NotImplementedError

    def test_sync_new_files(self):
        syncer = self.get_syncer()
        src_provider = syncer.src_provider
        dst_provider = syncer.dst_provider


        src_provider.write('foo')
        raise NotImplementedError

    def test_sync_updated_files(self):
        raise NotImplementedError

    def test_sync_deleted_files(self):
        raise NotImplementedError

    def test_sync_moved_files(self):
        raise NotImplementedError

    def test_conflicting_updates(self):
        raise NotImplementedError


if __name__ == '__main__':
    pytest.main()
