import abc
import collections
import logging
from typing import Dict, List

from sync.providers.common import path_split
from sync.state import StorageState

LOGGER = logging.getLogger(__name__)


class DiffType(abc.ABC):
    TYPE = None

    def __init__(self, path):
        self.path = path

    def __repr__(self):
        return '%s("%s")' % (self.__class__.__name__, self.path)


class AddedDiffType(DiffType):
    TYPE = "ADDED"


class RemovedDiffType(DiffType):
    TYPE = "REMOVED"


class ChangedDiffType(DiffType):
    TYPE = "CHANGED"


class MovedDiffType(DiffType):
    TYPE = "MOVED"

    def __init__(self, path, new_path):
        super().__init__(path)
        self.new_path = new_path

    def __repr__(self):
        return '%s("%s", "%s")' % (self.__class__.__name__, self.path, self.new_path)


# https://stackoverflow.com/questions/2460177/edit-distance-in-python
def levenshtein_distance(s1, s2):
    if len(s1) > len(s2):
        s1, s2 = s2, s1

    distances = range(len(s1) + 1)
    for i2, c2 in enumerate(s2):
        distances_ = [i2 + 1]
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1])
            else:
                distances_.append(
                    1 + min((distances[i1], distances[i1 + 1], distances_[-1]))
                )
        distances = distances_
    return distances[-1]


class StorageStateDiff:
    def __init__(self, changes: Dict[str, DiffType]):
        self.changes: Dict[str, DiffType] = changes

    @staticmethod
    def compute(current: StorageState, baseline: StorageState) -> "StorageStateDiff":
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

        LOGGER.debug("raw changes: %s", changes)

        # construct hash to paths for added and removed diff types
        added_removed_by_hash: Dict[str, List[DiffType]] = collections.defaultdict(list)
        for path, diff in changes.items():
            if isinstance(diff, AddedDiffType):
                added_removed_by_hash[current.files[diff.path].content_hash].append(
                    diff
                )
            elif isinstance(diff, RemovedDiffType):
                added_removed_by_hash[baseline.files[diff.path].content_hash].append(
                    diff
                )

        # detect file movement
        for content_hash, diffs in added_removed_by_hash.items():
            added_diffs = list(filter(lambda x: isinstance(x, AddedDiffType), diffs))
            removed_diffs = list(
                filter(lambda x: isinstance(x, RemovedDiffType), diffs)
            )

            if len(added_diffs) == len(removed_diffs):
                # so we have same amount of removed and added items for the same hash
                # we need to allocate movement using some best match heuristic

                def score(added_diff: AddedDiffType, removed_diff: RemovedDiffType):
                    _, added_filename = path_split(added_diff.path)
                    _, removed_filename = path_split(removed_diff.path)
                    return levenshtein_distance(added_filename, removed_filename)

                for removed_diff in removed_diffs:
                    # pick added diff with best score
                    best_match_added_diff = min(
                        added_diffs,
                        key=lambda added_diff: score(added_diff, removed_diff),
                    )
                    added_diffs.remove(best_match_added_diff)

                    LOGGER.info(
                        'detected file movement "%s" --> "%s" (hash %s)',
                        removed_diff.path,
                        best_match_added_diff.path,
                        content_hash,
                    )

                    del changes[best_match_added_diff.path]
                    changes[removed_diff.path] = MovedDiffType(
                        removed_diff.path, best_match_added_diff.path
                    )

            if len(diffs) > 2:
                LOGGER.warning(
                    'multiple diff types detected for the same content hash "%s": %s',
                    content_hash,
                    diffs,
                )

        return StorageStateDiff(changes)
