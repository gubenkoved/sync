from typing import BinaryIO

from sync.core import ProviderBase
from sync.state import FileState, StorageState
from sync.hashing import hash_dict

import paramiko


# TODO: add certificate based auth
class STFPProvider(ProviderBase):
    def __init__(self, host: str, username: str, password: str, root_dir: str, port: int = 22):
        self.host = host
        self.username = username
        self.password = password
        self.root_dir = root_dir
        self.port = port

    def get_handle(self) -> str:
        return 'sftp-' + hash_dict({
            'host': self.host,
            'root_dir': self.root_dir,
        })

    def _connect(self) -> paramiko.SFTPClient:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(
            self.host,
            username=self.username,
            port=self.port)

        sftp = ssh.open_sftp()
        return sftp

    # TODO: how to gather content hash over SFTP
    def get_state(self) -> StorageState:
        with self._connect() as sftp:
            sftp.chdir(self.root_dir)
            sftp.listdir()

        raise NotImplementedError

    def get_file_state(self, path: str) -> FileState:
        pass

    def read(self, path: str) -> BinaryIO:
        pass

    def write(self, path: str, content: BinaryIO) -> None:
        pass

    def remove(self, path: str) -> None:
        pass

    def compute_content_hash(self, content: BinaryIO) -> str:
        pass

