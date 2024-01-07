import io
import os.path
from typing import BinaryIO, Optional, Tuple, List
from stat import S_ISDIR, S_ISREG

import paramiko

from sync.core import ProviderBase
from sync.hashing import hash_dict, HashType
from sync.state import FileState, StorageState


# TODO: add certificate based auth
class STFPProvider(ProviderBase):
    def __init__(self, host: str, username: str, root_dir: str,
                 password: Optional[str] = None, key_path: Optional[str] = None,
                 port: int = 22):
        self.host = host
        self.username = username
        self.root_dir = root_dir
        self.password = password
        self.key_path = key_path
        self.port = port

        if self.key_path:
            self.key_path = os.path.expanduser(self.key_path)

    def get_handle(self) -> str:
        return 'sftp-' + hash_dict({
            'host': self.host,
            'root_dir': self.root_dir,
        })

    def _connect(self) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient]:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(
            self.host,
            username=self.username,
            password=self.password,
            key_filename=self.key_path,
            port=self.port)

        sftp = ssh.open_sftp()
        return ssh, sftp

    @staticmethod
    def _file_state(entry):
        return FileState(
            # TODO: how to properly detect changes if size and
            #  modification time did not change?
            content_hash=hash_dict({
                'modified': str(entry.st_mtime),
                'size': str(entry.st_size),
            })
        )

    def get_state(self) -> StorageState:
        ssh, sftp = self._connect()

        files = {}

        def walk(dir_path):
            sftp.chdir(dir_path)
            dirs = []

            for entry in sftp.listdir_attr():
                is_dir = S_ISDIR(entry.st_mode)
                is_file = S_ISREG(entry.st_mode)

                filename = entry.filename
                full_path = os.path.join(dir_path, filename)
                rel_path = os.path.relpath(full_path, self.root_dir)

                if is_file:

                    files[rel_path] = self._file_state(entry)

                if is_dir:
                    dirs.append(filename)

            for dir_name in dirs:
                walk(os.path.join(dir_path, dir_name))

        with ssh, sftp:
            walk(self.root_dir)

        return StorageState(files)

    def get_file_state(self, path: str) -> FileState:
        ssh, sftp = self._connect()
        full_path = os.path.join(self.root_dir, path)

        with ssh, sftp:
            dir_path, filename = os.path.split(full_path)
            sftp.chdir(dir_path)
            entry = sftp.lstat(filename)
            assert S_ISREG(entry.st_mode)
            return self._file_state(entry)

    def read(self, path: str) -> BinaryIO:
        ssh, sftp = self._connect()
        full_path = os.path.join(self.root_dir, path)

        with ssh, sftp:
            buffer = io.BytesIO()
            sftp.getfo(full_path, buffer)
            buffer.seek(0)
            return buffer

    def write(self, path: str, content: BinaryIO) -> None:
        ssh, sftp = self._connect()
        full_path = os.path.join(self.root_dir, path)
        with ssh, sftp:
            sftp.putfo(content, full_path)

    def remove(self, path: str) -> None:
        ssh, sftp = self._connect()
        full_path = os.path.join(self.root_dir, path)
        with ssh, sftp:
            sftp.remove(full_path)

    def supported_hash_types(self) -> List[HashType]:
        return []

    def compute_hash(self, path: str, hash_type: HashType) -> str:
        raise Exception('not supported')
