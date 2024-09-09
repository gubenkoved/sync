import abc
import logging
import pickle

LOGGER = logging.getLogger(__name__)

class CacheMissSentinel:
    def __repr__(self):
        return '<CACHE MISS>'

CACHE_MISS = CacheMissSentinel()

PrimitiveType = (
        str | int | float |
        dict[str, 'PrimitiveType'] |
        list['PrimitiveType'] |
        tuple['PrimitiveType', ...] |
        None
)


class CacheBase:
    @abc.abstractmethod
    def get(self, key: str) -> PrimitiveType | CacheMissSentinel:
        pass

    @abc.abstractmethod
    def set(self, key: str, value: PrimitiveType) -> None:
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

    def get(self, key: str) -> PrimitiveType | CacheMissSentinel:
        return self.data.get(key, CACHE_MISS)

    def set(self, key: str, value: PrimitiveType) -> None:
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
        LOGGER.debug(
            'saving cache into "%s" (entry count: %d)',
            self.storage_path, len(self.data))
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
            LOGGER.debug('loaded %d entries', len(self.data))

    def try_load(self):
        try:
            self.load()
        except Exception as exc:
            LOGGER.warning(
                'unable to load cache from "%s" due to "%s"', self.storage_path, exc)
