import os.path
from typing import BinaryIO, Optional, List
import io

from sync.core import ProviderBase
from sync.state import FileState, StorageState
from sync.hashing import hash_dict, HashType

import dropbox
from dropbox.files import FileMetadata, FolderMetadata, WriteMode


# TODO: support mode where we manually walk directories so that we can control
#  depth
class DropboxProvider(ProviderBase):
    SUPPORTED_HASH_TYPES = [HashType.DROPBOX_SHA256]

    def __init__(self, account_id: str, token: str, root_dir: str,
                 is_refresh_token=False, app_key: Optional[str] = None,
                 app_secret: Optional[str] = None, depth: Optional[int] = None):
        self.account_id = account_id
        self.root_dir = root_dir
        self.token = token
        self.is_refresh_token = is_refresh_token
        self.app_key = app_key
        self.app_secret = app_secret
        self.depth = depth
        self._dropbox = None

    def get_handle(self) -> str:
        return 'd-' + hash_dict({
            'account_id': self.account_id,
            'root_dir': self.root_dir,
            'depth': self.depth,
        })

    def _get_dropbox(self) -> dropbox.Dropbox:
        if self._dropbox is None:
            if not self.is_refresh_token:
                self._dropbox = dropbox.Dropbox(
                    oauth2_access_token=self.token
                )
            else:
                self._dropbox = dropbox.Dropbox(
                    oauth2_refresh_token=self.token,
                    app_key=self.app_key,
                    app_secret=self.app_secret,
                )
        assert self._dropbox is not None
        return self._dropbox

    def _get_full_path(self, path: str):
        return os.path.join(self.root_dir, path)

    def _list_folder(self, dbx: dropbox.Dropbox, path: str):
        entries = []
        list_result = dbx.files_list_folder(path)
        entries.extend(list_result.entries)
        while list_result.has_more:
            list_result = dbx.files_list_folder_continue(list_result.cursor)
            entries.extend(list_result.entries)
        return entries

    def get_state(self) -> StorageState:
        dbx = self._get_dropbox()
        files = {}

        def walk(path: str, depth: int):
            if self.depth is not None and depth > self.depth:
                return
            for entry in self._list_folder(dbx, path):
                if isinstance(entry, FileMetadata):
                    full_path = entry.path_display
                    assert full_path.startswith(self.root_dir)
                    rel_path = os.path.relpath(full_path, self.root_dir)
                    files[rel_path] = FileState(
                        entry.content_hash,
                    )
                elif isinstance(entry, FolderMetadata):
                    walk(entry.path_display, depth+1)
        walk(self.root_dir, depth=1)
        return StorageState(files)

    def get_file_state(self, path: str) -> FileState:
        dbx = self._get_dropbox()

        full_path = self._get_full_path(path)
        result = dbx.files_get_metadata(full_path)
        assert isinstance(result, FileMetadata)

        return FileState(
            result.content_hash,
        )

    def read(self, path: str) -> BinaryIO:
        dbx = self._get_dropbox()

        full_path = self._get_full_path(path)
        metadata, response = dbx.files_download(full_path)

        return io.BytesIO(response.content)

    # TODO: use "update" method that checks file revision to avoid lost update
    def write(self, path: str, content: BinaryIO) -> None:
        dbx = self._get_dropbox()
        full_path = self._get_full_path(path)
        file_bytes = content.read()
        dbx.files_upload(file_bytes, full_path, mode=WriteMode.overwrite)

    def remove(self, path: str) -> None:
        dbx = self._get_dropbox()
        full_path = self._get_full_path(path)
        dbx.files_delete_v2(full_path)

    def supported_hash_types(self) -> List[HashType]:
        return self.SUPPORTED_HASH_TYPES

    def compute_hash(self, path: str, hash_type: HashType) -> str:
        dbx = self._get_dropbox()
        full_path = self._get_full_path(path)
        result = dbx.files_get_metadata(full_path)
        return result.content_hash
