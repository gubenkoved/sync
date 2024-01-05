import pickle
from typing import BinaryIO, Dict


class FileState:
    def __init__(self, content_hash: str):
        self.content_hash = content_hash

    def __repr__(self):
        return '<FileState hash="%s">' % self.content_hash


class StorageState:
    def __init__(self, files: Dict[str, FileState] = None):
        self.files: Dict[str, FileState] = files or {}


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
