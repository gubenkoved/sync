import unittest
import tempfile

from sync.cache import InMemoryCacheWithStorage, CACHE_MISS


class CacheTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cache_path = tempfile.mktemp()
        self.cache = InMemoryCacheWithStorage(self.cache_path)

    def test_get_set_delete(self):
        values = ['string', 42, 42.42, None, ['foo'], {'nested': 'dict'}]

        for value in values:
            self.assertIs(CACHE_MISS, self.cache.get('foo'))

            self.cache.set('foo', value)
            self.assertEqual(value, self.cache.get('foo'))

            self.cache.delete('foo')

    def test_get_set_delete_with_persistence(self):
        values = ['string', 42, 42.42, None, ['foo'], {'nested': 'dict'}]

        for value in values:
            self.assertIs(CACHE_MISS, self.cache.get('foo'))

            self.cache.set('foo', value)

            # save to disk and reload
            self.cache.save()
            self.cache.load()

            self.assertEqual(value, self.cache.get('foo'))

            self.cache.delete('foo')

            self.cache.save()
            self.cache.load()

            self.assertIs(CACHE_MISS, self.cache.get('foo'))
