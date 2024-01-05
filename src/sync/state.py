from typing import BinaryIO, Dict
import pickle


class FileState:
    def __init__(self, content_hash: str):
        self.content_hash = content_hash

    def __repr__(self):
        return '<FileState hash="%s">' % self.content_hash


class StorageState:
    def __init__(self):
        self.files: Dict[str, FileState] = {}

    def save(self, f: BinaryIO):
        pickle.dump(self, f)

    @staticmethod
    def load(f: BinaryIO) -> 'StorageState':
        obj = pickle.load(f)
        assert isinstance(obj, StorageState)
        return obj
