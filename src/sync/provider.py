from abc import ABC, abstractmethod
from typing import BinaryIO, List

from sync.hashing import HashType
from sync.state import FileState, StorageState


class ProviderError(Exception):
    pass


class FileNotFoundProviderError(ProviderError):
    pass


class FileAlreadyExistsError(ProviderError):
    pass


class ConflictError(ProviderError):
    pass


class ProviderBase(ABC):
    def get_label(self) -> str:
        """Returns short string describing provider and vital parameters"""
        return self.__class__.__name__

    @abstractmethod
    def get_handle(self) -> str:
        """
        Returns string that identifies provider along with critical parameters like
        directory for FS provider or other important arguments that identify the
        storage itself.
        """
        raise NotImplementedError

    @abstractmethod
    def is_case_sensitive(self) -> bool:
        raise NotImplementedError

    # TODO: validate depth parameters on some generic level
    @abstractmethod
    def get_state(self, depth: int | None = None) -> StorageState:
        raise NotImplementedError

    @abstractmethod
    def get_file_state(self, path: str) -> FileState:
        raise NotImplementedError

    @abstractmethod
    def read(self, path: str) -> BinaryIO:
        raise NotImplementedError

    @abstractmethod
    def write(self, path: str, content: BinaryIO) -> None:
        raise NotImplementedError

    @abstractmethod
    def remove(self, path: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def move(self, source_path: str, destination_path: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def supported_hash_types(self) -> List[HashType]:
        raise NotImplementedError

    @abstractmethod
    def compute_hash(self, path: str, hash_type: HashType) -> str:
        raise NotImplementedError

    @abstractmethod
    def clone(self) -> 'ProviderBase':
        """Returns a new instance of the provider with the same settings"""
        raise NotImplementedError


class SafeUpdateSupportMixin:
    @abstractmethod
    def update(self, path: str, content: BinaryIO, revision: str) -> None:
        """
        Updates file by the given path checking for revision match before update.
        """
        raise NotImplementedError
