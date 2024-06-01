import abc
from typing import Dict
from sync.state import StorageState


class DiffType(abc.ABC):
    TYPE = None

    def __init__(self, path):
        self.path = path


class AddedDiffType(DiffType):
    TYPE = 'ADDED'


class RemovedDiffType(DiffType):
    TYPE = 'REMOVED'


class ChangedDiffType(DiffType):
    TYPE = 'CHANGED'


class MovedDiffType(DiffType):
    TYPE = 'MOVED'

    def __init__(self, path, new_path):
        super().__init__(path)
        self.new_path = new_path


class StorageStateDiff:
    def __init__(self, changes: Dict[str, DiffType]):
        self.changes: Dict[str, DiffType] = changes

    # TODO: detect "MOVED" diff as a change where file with the same hash is
    #  removed in one location and appeared in another
    @staticmethod
    def compute(current: StorageState, baseline: StorageState) -> 'StorageStateDiff':
        changes = {}

        for path, state in current.files.items():
            if path not in baseline.files:
                changes[path] = AddedDiffType(path)
            else:
                if state.content_hash != baseline.files[path].content_hash:
                    changes[path] = ChangedDiffType(path)

        for path in baseline.files:
            if path not in current.files:
                changes[path] = RemovedDiffType(path)

        return StorageStateDiff(changes)
