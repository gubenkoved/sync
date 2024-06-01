import pickle
from typing import BinaryIO, Dict


# TODO: add more metadata like creation and last update time, also file size
class FileState:
    def __init__(self, content_hash: str, revision: str = None):
        self.content_hash = content_hash
        self.revision = revision

    def __repr__(self):
        return 'FileState(hash="%s", revision="%s")>' % (
            self.content_hash, self.revision)

    def __eq__(self, other):
        if not isinstance(other, FileState):
            return False
        return (self.content_hash == other.content_hash and
                self.revision == other.revision)


class StorageState:
    def __init__(self, files: Dict[str, FileState] = None):
        self.files: Dict[str, FileState] = files or {}

    def __repr__(self):
        return '<StorageState %s files>' % len(self.files)

    def __eq__(self, other):
        if not isinstance(other, StorageState):
            return False
        return self.files == other.files


class SyncPairState:
    def __init__(self, source_state: StorageState, dest_state: StorageState):
        self.source_state: StorageState = source_state
        self.dest_state: StorageState = dest_state

    def save(self, f: BinaryIO):
        pickle.dump(self, f)

    @staticmethod
    def load(f: BinaryIO) -> 'SyncPairState':
        obj = pickle.load(f)
        assert isinstance(obj, SyncPairState)
        return obj
