"""
This file contains useful methods for downloading web content
"""

__author__ = 'Alexandre'

import requests

def open(url):
    """
    Opens a URL and return's its content.
    Note that in our requests, we prefer gzipped content, since
    the time required to download the content is greatly reduced.
    This allows our crawler to be significantly faster than if it
    were downloading the uncompressed content at an URL.
    The uncompression requires more CPU activity, but the true
    bottleneck in this situation is the time required to download
    the files.

    url -- The url to open
    """
    headers = {'User-Agent': 'distributed crawler v1.0',
               'Accept-Encoding': 'gzip,deflate'}

    try:
        request = requests.get(url, headers=headers, timeout=10)
        data = request.content
        content_type = request.headers['content-type']
    except Exception as e:
        print e
        data = ''
        content_type = None

    return data, content_type