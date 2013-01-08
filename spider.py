__author__ = 'Alexandre'

from http_utils import open
from time import sleep

class Spider:
    """
    The spider fetches URLs to download from the crawler's frontier,
    download the documents and adds them to a temporary document store.
    """

    def __init__(self, spider_id, frontier, document_store):
        """
        spider_id -- The spider's identifier
        frontier -- The crawler's frontier
        document_store -- The crawler's document store
        """
        self.spider_id = spider_id
        self.frontier = frontier
        self.document_store = document_store

    def __call__(self):
        """
        Main routine of a spider.
        """
        while True:
            job = self.frontier.get()
            for url in job.urls:
                try:
                    #print "ATTEMPTING TO DOWNLOAD ", url
                    document, content_type = self.download_document(url)
                    self.document_store.put(url, document, content_type)
                    print "Added ", url, " to the document store."
                    sleep(2)
                except Exception as e:
                    #print "FAILED TO DOWNLOAD ", url, " reason: ", e
                    pass

    def download_document(self, url):
        """
        Download the document located at a URL
        """
        return open(url)
