import logging
import os.path
import shutil
import tempfile
from typing import BinaryIO, List

from sync.core import (
    SyncError,
)
from sync.hashing import (
    hash_stream, hash_dict, HashType,
    sha256_stream, dropbox_hash_stream,
)
from sync.provider import (
    ProviderBase,
    ProviderError,
    FileNotFoundProviderError,
    FileAlreadyExistsError,
)
from sync.providers.common import (
    unixify_path,
)
from sync.state import (
    StorageState,
    FileState,
)

LOGGER = logging.getLogger(__name__)


# TODO: provider should be able to have cache store
#  (e.g. to avoid recomputing hashes when there seem to be no reason to do so, like
#  both size and modification time is unchanged), this probably can be united with
#  the other provider state
class FSProvider(ProviderBase):
    BUFFER_SIZE = 4096
    SUPPORTED_HASH_TYPES = [HashType.SHA256, HashType.DROPBOX_SHA256]

    def __init__(self, root_dir: str):
        LOGGER.debug('init FS provider with root at "%s"', root_dir)
        self.root_dir = os.path.abspath(os.path.expanduser(root_dir))

        if not os.path.exists(self.root_dir):
            raise SyncError('root directory "%s" does not exist' % self.root_dir)

    def get_label(self) -> str:
        return 'FS(%s)' % self.root_dir

    def get_handle(self) -> str:
        return 'fs-' + hash_dict({
            'root_dir': self.root_dir,
            # change version when making a change that makes provider state
            # from previous version no longer compatible
            'version': 2,
        })

    def _file_state(self, abs_path: str) -> FileState:
        return FileState(
            content_hash=self._file_hash(abs_path),
            revision=str(os.path.getmtime(abs_path)),
        )

    def get_state(self, depth: int | None = None) -> StorageState:
        files = {}

        def walk(dir_path: str, level: int):
            LOGGER.debug('walking "%s"...', dir_path)

            if depth is not None and level > depth:
                return

            for entry in os.scandir(dir_path):
                if entry.is_file():
                    abs_path = entry.path
                    rel_path = os.path.relpath(abs_path, self.root_dir)
                    rel_path = unixify_path(rel_path)
                    files[rel_path] = self._file_state(abs_path)
                elif entry.is_dir():
                    walk(entry.path, level + 1)

        walk(self.root_dir, level=1)

        LOGGER.debug('discovered %d files', len(files))
        return StorageState(files)

    def _abs_path(self, path: str):
        abs_path = os.path.join(self.root_dir, path)
        abs_path = os.path.abspath(abs_path)
        if not abs_path.startswith(self.root_dir):
            raise ProviderError('path outside of root dir')
        return abs_path

    def get_file_state(self, path: str):
        abs_path = self._abs_path(path)
        try:
            return self._file_state(abs_path)
        except FileNotFoundError:
            raise FileNotFoundProviderError(f'File not found: {path}')

    def _file_hash(self, path):
        LOGGER.debug('compute hash for "%s"', path)
        abs_path = self._abs_path(path)
        with open(abs_path, 'rb') as f:
            return hash_stream(f)

    def read(self, path: str) -> BinaryIO:
        abs_path = self._abs_path(path)
        try:
            return open(abs_path, 'rb')
        except FileNotFoundError:
            raise FileNotFoundProviderError(f'File not found: {path}')

    @staticmethod
    def _ensure_dir(dir_path: str):
        if not os.path.exists(dir_path):
            LOGGER.debug(f'creating directory {dir_path}...')
            os.makedirs(dir_path)

    def write(self, path: str, stream: BinaryIO):
        abs_path = self._abs_path(path)
        dir_path = os.path.dirname(abs_path)

        self._ensure_dir(dir_path)

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            while True:
                buffer = stream.read(self.BUFFER_SIZE)
                if not buffer:
                    break
                temp_file.write(buffer)

        # now atomically move temp file
        shutil.move(temp_file.name, abs_path)

    def remove(self, path: str):
        abs_path = self._abs_path(path)
        try:
            os.unlink(abs_path)
        except FileNotFoundError:
            raise FileNotFoundProviderError(f'File not found: {path}')

    def move(self, source_path: str, destination_path: str):
        source_abs_path = self._abs_path(source_path)
        destination_abs_path = self._abs_path(destination_path)

        if os.path.exists(destination_abs_path):
            raise FileAlreadyExistsError(
                f'File already exists: {destination_path}')

        # ensure destination directory if missing
        destination_dir, _ = os.path.split(destination_abs_path)
        self._ensure_dir(destination_dir)

        try:
            shutil.move(source_abs_path, destination_abs_path)
        except FileNotFoundError as err:
            raise FileNotFoundProviderError(
                f'File not found: {source_path}') from err

    def supported_hash_types(self) -> List[HashType]:
        return self.SUPPORTED_HASH_TYPES

    def compute_hash(self, path: str, hash_type: HashType) -> str:
        assert hash_type in self.SUPPORTED_HASH_TYPES

        abs_path = self._abs_path(path)

        try:
            with open(abs_path, 'rb') as f:
                if hash_type == HashType.SHA256:
                    return sha256_stream(f)
                elif hash_type == HashType.DROPBOX_SHA256:
                    return dropbox_hash_stream(f)
                else:
                    raise NotImplementedError
        except FileNotFoundError:
            raise FileNotFoundProviderError(f'File not found: {path}')
