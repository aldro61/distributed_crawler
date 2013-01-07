__author__ = 'Alexandre'

class Index:
    """
    An interface to the document index
    """

    def __init__(self):
        self.fake_index_storage = {}

    def put(self, url, document, content_type):
        """
        Add a document to the index

        url -- The document's URL
        document -- The document's content
        content_type -- The document's content-type
        """
        self.fake_index_storage[url] = (document, content_type)

    def get(self, url):
        """
        Get a document from the index

        url -- The document's URL
        """
        if self.fake_index_storage.has_key(url):
            return self.fake_index_storage[url]
        else:
            return None

    def document_count(self):
        """
        Number of documents in the index
        """
        return len(self.fake_index_storage.keys())
