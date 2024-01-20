import io
import logging
import os.path
import shlex
from stat import S_ISDIR, S_ISREG
from typing import BinaryIO, Optional, Tuple, List

import paramiko

from sync.core import ProviderBase, ProviderError, FileNotFoundProviderError
from sync.hashing import hash_dict, HashType
from sync.state import FileState, StorageState

LOGGER = logging.getLogger(__name__)


class STFPProvider(ProviderBase):
    def __init__(self, host: str, username: str, root_dir: str,
                 password: Optional[str] = None, key_path: Optional[str] = None,
                 port: int = 22, depth: Optional[int] = None):
        self.host = host
        self.username = username
        self.root_dir = root_dir
        self.password = password
        self.key_path = key_path
        self.port = port
        self.depth = depth

        if depth is not None:
            if depth < 1:
                raise ValueError(
                    'depth should either be None or bigger or equal to 1')

        if self.key_path:
            self.key_path = os.path.expanduser(self.key_path)

    def get_handle(self) -> str:
        return 'sftp-' + hash_dict({
            'host': self.host,
            'root_dir': self.root_dir,
            'depth': self.depth,
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
    def _file_state(ssh: paramiko.SSHClient, full_path: str):
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
        sha256 = stdout_str.split(' ')[0]
        return FileState(
            content_hash=sha256,
        )

    def get_state(self) -> StorageState:
        ssh, sftp = self._connect()
        files = {}

        def walk(dir_path, depth):
            if depth >= self.depth:
                return

            sftp.chdir(dir_path)
            dirs = []

            for entry in sftp.listdir_attr():
                is_dir = S_ISDIR(entry.st_mode)
                is_file = S_ISREG(entry.st_mode)

                filename = entry.filename
                full_path = os.path.join(dir_path, filename)
                rel_path = os.path.relpath(full_path, self.root_dir)

                if is_file:
                    files[rel_path] = self._file_state(ssh, full_path)

                if is_dir:
                    dirs.append(filename)

            for dir_name in dirs:
                walk(os.path.join(dir_path, dir_name), depth=depth + 1)

        with ssh, sftp:
            walk(self.root_dir, depth=1)

        return StorageState(files)

    def get_file_state(self, path: str) -> FileState:
        ssh, sftp = self._connect()
        full_path = os.path.join(self.root_dir, path)

        with ssh, sftp:
            dir_path, filename = os.path.split(full_path)
            try:
                sftp.chdir(dir_path)
                entry = sftp.lstat(filename)
                assert S_ISREG(entry.st_mode)
                return self._file_state(ssh, full_path)
            except FileNotFoundError:
                raise FileNotFoundProviderError(f'File not found: {full_path}')

    def read(self, path: str) -> BinaryIO:
        ssh, sftp = self._connect()
        full_path = os.path.join(self.root_dir, path)

        with ssh, sftp:
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
        full_path = os.path.join(self.root_dir, path)
        dir_path, _ = os.path.split(full_path)
        self._ensure_dir(ssh, dir_path)
        with ssh, sftp:
            sftp.putfo(content, full_path)

    def remove(self, path: str) -> None:
        ssh, sftp = self._connect()
        full_path = os.path.join(self.root_dir, path)
        with ssh, sftp:
            try:
                sftp.remove(full_path)
            except FileNotFoundError:
                raise FileNotFoundProviderError(f'File not found: {full_path}')

    def supported_hash_types(self) -> List[HashType]:
        return []

    def compute_hash(self, path: str, hash_type: HashType) -> str:
        raise Exception('not supported')
