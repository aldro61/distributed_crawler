__author__ = 'Alexandre'
from time import time

class VisitedURLCache:
    """
    Hashtable that keeps track of visited URLs
    """

    def __init__(self):
        self.storage = {}

    def put(self, url):
        """
        Flag a URL as being visited

        url -- The URL to add to mark as visited
        """
        self.storage[url] = time()

    def already_visited(self, url):
        """
        Check if a URL was visited

        url -- The URL for which to check if it has been visited
        """
        return self.storage.has_key(url)