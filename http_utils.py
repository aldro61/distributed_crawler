"""
This file contains useful methods for downloading web content
"""

__author__ = 'Alexandre'

import gzip
import StringIO
import urllib2
import zlib

def open(url):
    """
    Opens a URL and return's its content.
    Note that in our requests, we prefer gzipped content, since
    the time required to download the content is greatly reduced.
    This allows our crawler to be significantly faster than if it
    used urllib2 to download the uncompressed content at an URL.
    The uncompression requires more CPU activity, but the true
    bottleneck in this situation is the time required to download
    the files.

    url -- The url to open
    """

    def decode(page):
        """
        Decodes the content of a gzipped web response.

        page -- The web response
        """
        encoding = page.info().get("Content-Encoding")
        if encoding in ('gzip', 'x-gzip', 'deflate'):
            content = page.read()
            if encoding == 'deflate':
                data = StringIO.StringIO(zlib.decompress(content))
            else:
                data = gzip.GzipFile('', 'rb', 9, StringIO.StringIO(content))
            content = data.read()
        else:
            content = page.read()

        return content, page.info().get("Content-Type")

    opener = urllib2.build_opener()
    opener.addheaders = [('User-Agent', 'graal-crawl beta'),
                         ('Accept-Encoding', 'gzip,deflate')]

    usock = opener.open(url, timeout=4)
    url = usock.geturl()
    data = decode(usock)
    usock.close()

    return data