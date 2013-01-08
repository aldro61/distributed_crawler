__author__ = 'Alexandre'

from os import makedirs
from os.path import join, exists

class Index:
    """
    An interface to the document index
    """

    def __init__(self):
        #self.fake_index_storage = {}
        pass

    def put(self, url, document, content_type):
        """
        Add a document to the index

        url -- The document's URL
        document -- The document's content
        content_type -- The document's content-type
        """
        #self.fake_index_storage[url] = (document, content_type)
        index_path = 'indexed_files'
        if not exists(index_path):
            makedirs(index_path)

        filename = url.split('/')[-1]
        file = open(join(index_path, filename),"wb")
        file.write(document)
        file.close()

    def get(self, url):
        """
        Get a document from the index

        url -- The document's URL
        """
        raise NotImplementedError()
        #if self.fake_index_storage.has_key(url):
        #    return self.fake_index_storage[url]
        #else:
        #    return None

    def document_count(self):
        """
        Number of documents in the index
        """
        #return len(self.fake_index_storage.keys())
        raise NotImplementedError()
