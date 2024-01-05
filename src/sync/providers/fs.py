import logging
import os.path
from typing import BinaryIO

from sync.core import ProviderBase, StorageState, FileState
from sync.hashing import Hasher

LOGGER = logging.getLogger(__name__)


class FSProvider(ProviderBase):
    BUFFER_SIZE = 4096

    def __init__(self, root_dir: str):
        LOGGER.info('init FS provider with root at "%s"', root_dir)
        self.root_dir = os.path.abspath(os.path.expanduser(root_dir))
        self.hasher = Hasher()

    def construct_state(self) -> StorageState:
        state = StorageState()
        for parent_dir_name, dir_names, file_names in os.walk(self.root_dir):
            for file_name in file_names:
                abs_path = os.path.join(parent_dir_name, file_name)
                rel_path = os.path.relpath(abs_path, self.root_dir)
                state.files[rel_path] = FileState(
                    content_hash=self._file_hash(abs_path)
                )
        return state

    def get_file_state(self, path: str):
        abs_path = os.path.join(self.root_dir, path)
        return FileState(
            content_hash=self._file_hash(abs_path)
        )

    def _file_hash(self, path):
        LOGGER.debug('compute hash for "%s"', path)
        abs_path = os.path.join(self.root_dir, path)
        with open(abs_path, 'rb') as f:
            return self.hasher.compute(f)

    def read(self, path: str) -> BinaryIO:
        abs_path = os.path.join(self.root_dir, path)
        return open(abs_path, 'rb')

    # TODO: write to temp file, then swap
    def write(self, path: str, stream: BinaryIO):
        abs_path = os.path.join(self.root_dir, path)
        with open(abs_path, 'wb') as f:
            while True:
                buffer = stream.read(self.BUFFER_SIZE)
                if not buffer:
                    break
                f.write(buffer)

    def remove(self, path: str):
        abs_path = os.path.join(self.root_dir, path)
        os.unlink(abs_path)

    def compute_content_hash(self, content: BinaryIO) -> str:
        return self.hasher.compute(content)
