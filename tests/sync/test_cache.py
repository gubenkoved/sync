import unittest
import tempfile

from sync.cache import InMemoryCacheWithStorage, CACHE_MISS, CacheCorruptedError


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

    def test_clear(self):
        self.cache.set('foo', 'bar')

        self.cache.clear()

        self.assertIs(CACHE_MISS, self.cache.get('foo'))

    def test_rewrite(self):
        self.cache.set('foo', 'bar')
        self.cache.set('foo', 'baz')

        self.assertEqual('baz', self.cache.get('foo'))

    def test_load_corrupted(self):
        with open(self.cache_path, 'w') as f:
            f.write('corrupted')

        self.assertRaises(
            CacheCorruptedError,
            self.cache.load
        )

        self.assertIs(CACHE_MISS, self.cache.get('foo'))

        self.cache.set('foo', 'bar')
        self.cache.save()
        self.cache.load()

        self.assertEqual('bar', self.cache.get('foo'))

    def test_load_restores_cache_state(self):
        self.cache.set('foo', 'bar')
        self.cache.set('spam', 'eggs')
        self.cache.save()

        self.cache.set('foo', 'baz')
        self.cache.delete('spam')

        self.assertEqual('baz', self.cache.get('foo'))
        self.assertIs(CACHE_MISS, self.cache.get('spam'))

        self.cache.load()

        self.assertEqual('bar', self.cache.get('foo'))
        self.assertEqual('eggs', self.cache.get('spam'))
