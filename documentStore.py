__author__ = 'Alexandre'

from multiprocessing import Queue

class DocumentStore:
    """
    Document store to store documents downloaded by the spiders while awaiting indexing.

    Note: Currently implemented with a Queue, but should be implemented with something like sqlite3.
    """

    def __init__(self):
        self.storage = Queue()

    def put(self, url, document, content_type):
        """
        Store the content of a document, its corresponding URL and its content-type

        url -- The document's URL
        document -- The document's content
        content_type -- The document's MIME content-type
        """
        self.storage.put((url, document, content_type))

    def get(self):
        """
        Get a document, its corresponding URL and its content-type from the document store.
        """
        return self.storage.get()

