__author__ = 'Alexandre'
from multiprocessing import Manager

class Frontier:
    """
    The frontier is a queue that contains a set of URLs to crawl
    """

    def __init__(self):
        self.queue = Manager().Queue()

    def put(self, url):
        """
        Add a URL to the frontier.

        url -- The URL to add to the frontier
        """
        self.queue.put(url)

    def get(self):
        """
        Get the next URL to crawl from the queue.
        """
        job = SpiderJob()
        job.add_url(self.queue.get())
        return job

    def empty(self):
        """
        Boolean value representing if there are still URLs in the frontier
        """
        return self.queue.empty()


class SpiderJob:
    """
    A sequence of URLs to crawl.

    This is used to package a set of URLs to send to a spider.
    """

    def __init__(self):
        self.urls = []

    def url_count(self):
        """
        Number of URLs in the job
        """
        return len(self.urls)

    def add_url(self, url):
        """
        Add a URL to the job
        """
        self.urls.append(url)