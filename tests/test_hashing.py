import io
from unittest import TestCase, main

import requests

from sync.hashing import dropbox_hash_stream


class DropboxHashTest(TestCase):
    def test_sample_file(self):
        response = requests.get('https://www.dropbox.com/static/images/developers/milky-way-nasa.jpg')
        data_bytes = response.content
        with io.BytesIO(data_bytes) as data_stream:
            self.assertEqual(
                '485291fa0ee50c016982abbfa943957bcd231aae0492ccbaa22c58e3997b35e0',
                dropbox_hash_stream(data_stream)
            )

    def test_empty(self):
        with io.BytesIO() as data_stream:
            self.assertEqual(
                'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
                dropbox_hash_stream(data_stream)
            )


if __name__ == '__main__':
    main()
