import os.path
from typing import BinaryIO
import io

from sync.core import ProviderBase
from sync.state import FileState, StorageState
from sync.hashing import hash_dict

import dropbox
from dropbox.files import FileMetadata, WriteMode


# TODO: add support for refresh tokens
class DropboxProvider(ProviderBase):
    def __init__(self, token: str, root_dir: str):
        self.token = token
        self.root_dir = root_dir

    def get_handle(self) -> str:
        return 'd-' + hash_dict({
            'token': self.token,
            'root_dir': self.root_dir,
        })

    def _get_dropbox(self) -> dropbox.Dropbox:
        return dropbox.Dropbox(self.token)

    def get_state(self) -> StorageState:
        dbx = self._get_dropbox()

        files = {}

        def process(list_result):
            for entry in list_result.entries:
                if not isinstance(entry, FileMetadata):
                    continue
                full_path = entry.path_display
                assert full_path.startswith(self.root_dir)
                rel_path = os.path.relpath(full_path, self.root_dir)
                files[rel_path] = FileState(
                    entry.content_hash,
                )

        list_result = dbx.files_list_folder(self.root_dir, recursive=True)
        process(list_result)

        while list_result.has_more:
            list_result = dbx.files_list_folder_continue(list_result.cursor)
            process(list_result)

        return StorageState(files)

    def get_file_state(self, path: str) -> FileState:
        dbx = self._get_dropbox()

        full_path = os.path.join(self.root_dir, path)
        result = dbx.files_get_metadata(full_path)
        assert isinstance(result, FileMetadata)

        return FileState(
            result.content_hash,
        )

    def read(self, path: str) -> BinaryIO:
        dbx = self._get_dropbox()

        full_path = os.path.join(self.root_dir, path)
        metadata, response = dbx.files_download(full_path)

        return io.BytesIO(response.content)

    # TODO: use "update" method that checks file revision to avoid lost update
    def write(self, path: str, content: BinaryIO) -> None:
        dbx = self._get_dropbox()
        full_path = os.path.join(self.root_dir, path)
        file_bytes = content.read()
        dbx.files_upload(file_bytes, full_path, mode=WriteMode.overwrite)

    def remove(self, path: str) -> None:
        dbx = self._get_dropbox()
        full_path = os.path.join(self.root_dir, path)
        dbx.files_delete_v2(full_path)

    def compute_content_hash(self, content: BinaryIO) -> str:
        raise NotImplementedError
