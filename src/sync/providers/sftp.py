import io
import logging
import os.path
import shlex
from stat import S_ISDIR, S_ISREG
from typing import BinaryIO, Optional, Tuple, List

import paramiko

from sync.hashing import hash_dict, HashType
from sync.provider import (
    ProviderBase,
    ProviderError,
    FileNotFoundProviderError,
    FileAlreadyExistsError,
)
from sync.providers.common import (
    path_join, relative_path, path_split,
)
from sync.state import (
    StorageState,
    FileState,
)

LOGGER = logging.getLogger(__name__)


class STFPProvider(ProviderBase):
    SUPPORTED_HASH_TYPES = [HashType.SHA256]

    def __init__(self, host: str, username: str, root_dir: str,
                 password: Optional[str] = None, key_path: Optional[str] = None,
                 port: int = 22):
        """
        Implements SFTP provider with Unix as a target platform only.
        """
        self.host = host
        self.username = username
        self.root_dir = root_dir
        self.password = password
        self.key_path = key_path
        self.port = port

        if self.key_path:
            self.key_path = os.path.expanduser(self.key_path)

        self.__ssh_client = None
        self.__sftp_client = None

    def get_label(self) -> str:
        return 'SFTP(%s@%s:%s)' % (self.username, self.host, self.root_dir)

    def get_handle(self) -> str:
        return 'sftp-' + hash_dict({
            'host': self.host,
            'root_dir': self.root_dir,
        })

    def _need_reconnect(self) -> bool:
        if self.__ssh_client is None:
            return True

        if self.__ssh_client.get_transport() is None:
            return True

        return False

    def _connect(self) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient]:
        if self._need_reconnect():
            LOGGER.info('connecting to SSH server...')
            self.__ssh_client = paramiko.SSHClient()
            self.__ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            self.__ssh_client.connect(
                self.host,
                username=self.username,
                password=self.password,
                key_filename=self.key_path,
                port=self.port)

            self.__sftp_client = self.__ssh_client.open_sftp()

        return self.__ssh_client, self.__sftp_client

    @staticmethod
    def _sha256_file(ssh: paramiko.SSHClient, full_path: str):
        _, stdout, stderr = ssh.exec_command('shasum -a 256 %s' % shlex.quote(full_path))
        stdout_str = stdout.read().decode('utf-8')
        stderr_str = stderr.read().decode('utf-8')
        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            if stdout_str:
                LOGGER.error('STDOUT: %s', stdout_str)
            if stderr_str:
                LOGGER.error('STDERR: %s', stderr_str)
            raise ProviderError('unable to calculate file hash')
        return stdout_str.split(' ')[0]

    @staticmethod
    def _file_state(ssh: paramiko.SSHClient, full_path: str):
        return FileState(
            content_hash=STFPProvider._sha256_file(ssh, full_path),
            hash_type=HashType.SHA256,
        )

    def get_state(self, depth: int | None = None) -> StorageState:
        ssh, sftp = self._connect()
        files = {}

        def walk(dir_path, cur_depth):
            if depth is not None and cur_depth >= depth:
                return

            sftp.chdir(dir_path)
            dirs = []

            for entry in sftp.listdir_attr():
                is_dir = S_ISDIR(entry.st_mode)
                is_file = S_ISREG(entry.st_mode)

                filename = entry.filename
                full_path = path_join(dir_path, filename)
                rel_path = relative_path(full_path, self.root_dir)

                if is_file:
                    files[rel_path] = self._file_state(ssh, full_path)

                if is_dir:
                    dirs.append(filename)

            for dir_name in dirs:
                walk(path_join(dir_path, dir_name), cur_depth=cur_depth + 1)

        self._ensure_dir(ssh, self.root_dir)
        walk(self.root_dir, cur_depth=1)

        return StorageState(files)

    def _full_path(self, path):
        full_path = path_join(self.root_dir, path)
        if not full_path.startswith(self.root_dir):
            raise ProviderError('Path outside of the root dir!')
        return full_path

    def get_file_state(self, path: str) -> FileState:
        ssh, sftp = self._connect()
        full_path = self._full_path(path)

        dir_path, filename = path_split(full_path)
        try:
            sftp.chdir(dir_path)
            entry = sftp.lstat(filename)
            assert S_ISREG(entry.st_mode)
            return self._file_state(ssh, full_path)
        except FileNotFoundError:
            raise FileNotFoundProviderError(f'File not found: {full_path}')

    def read(self, path: str) -> BinaryIO:
        ssh, sftp = self._connect()
        full_path = self._full_path(path)

        buffer = io.BytesIO()
        try:
            sftp.getfo(full_path, buffer)
            buffer.seek(0)
            return buffer
        except FileNotFoundError:
            raise FileNotFoundProviderError(f'File not found: {full_path}')

    def _ensure_dir(self, ssh: paramiko.SSHClient, dir_path: str) -> None:
        stdin, stdout, stderr = ssh.exec_command(
            'mkdir -p %s' % shlex.quote(dir_path))

        exit_code = stdout.channel.recv_exit_status()

        if exit_code != 0:
            stdout_data = stdout.read()
            stderr_data = stderr.read()
            if stdout_data:
                LOGGER.error('STDOUT: %s', stdout_data)
            if stderr_data:
                LOGGER.error('STDERR: %s', stderr_data)
            raise ProviderError('unable to ensure directory exists')

    def write(self, path: str, content: BinaryIO) -> None:
        ssh, sftp = self._connect()
        full_path = self._full_path(path)
        dir_path, _ = path_split(full_path)
        self._ensure_dir(ssh, dir_path)
        sftp.putfo(content, full_path)

    def remove(self, path: str) -> None:
        ssh, sftp = self._connect()
        full_path = self._full_path(path)
        try:
            sftp.remove(full_path)
        except FileNotFoundError:
            raise FileNotFoundProviderError(f'File not found: {full_path}')

    def move(self, source_path: str, destination_path: str) -> None:
        ssh, sftp = self._connect()
        source_full_path = self._full_path(source_path)
        destination_full_path = self._full_path(destination_path)

        try:
            sftp.lstat(destination_full_path)
            destination_exists = True
        except FileNotFoundError:
            destination_exists = False

        if destination_exists:
            raise FileAlreadyExistsError(
                f'File already exists: {destination_full_path}')

        try:
            sftp.rename(source_full_path, destination_full_path)
        except FileNotFoundError:
            raise FileNotFoundProviderError(
                f'File not found: {source_full_path}')

    def supported_hash_types(self) -> List[HashType]:
        return self.SUPPORTED_HASH_TYPES

    def compute_hash(self, path: str, hash_type: HashType) -> str:
        if hash_type == HashType.SHA256:
            ssh, sftp = self._connect()
            full_path = path_join(self.root_dir, path)
            return self._sha256_file(ssh, full_path)
        raise Exception('not supported')
