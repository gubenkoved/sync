import abc
import pickle
import logging


LOGGER = logging.getLogger(__name__)


class CacheBase:
    @abc.abstractmethod
    def get(self, key: str) -> str | None:
        pass

    @abc.abstractmethod
    def set(self, key: str, value: str) -> None:
        pass

    @abc.abstractmethod
    def delete(self, key: str) -> None:
        pass

    @abc.abstractmethod
    def clear(self) -> None:
        pass


class InMemoryCache(CacheBase):
    def __init__(self):
        self.data = {}

    def get(self, key: str) -> str | None:
        return self.data.get(key)

    def set(self, key: str, value: str) -> None:
        self.data[key] = value

    def delete(self, key: str) -> None:
        self.data.pop(key, None)

    def clear(self) -> None:
        self.data.clear()


class InMemoryCacheWithStorage(InMemoryCache):
    def __init__(self, storage_path: str):
        super().__init__()
        self.storage_path = storage_path

    def save(self):
        LOGGER.debug('saving cache into "%s"', self.storage_path)
        with open(self.storage_path, 'wb') as f:
            pickle.dump(self.data, f)

    def try_save(self):
        try:
            self.save()
        except Exception as exc:
            LOGGER.warning(
                'unable to save cache to "%s" due to "%s"', self.storage_path, exc)

    def load(self):
        LOGGER.debug('loading cache from "%s"', self.storage_path)
        with open(self.storage_path, 'rb') as f:
            pickled_data = pickle.load(f)
            assert isinstance(pickled_data, dict)
            self.data = pickled_data

    def try_load(self):
        try:
            self.load()
        except Exception as exc:
            LOGGER.warning(
                'unable to load cache from "%s" due to "%s"', self.storage_path, exc)
