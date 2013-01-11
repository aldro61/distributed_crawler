__author__ = 'Alexandre'

#import sqlite3
from multiprocessing import Queue

class DocumentStore:
    """
    Document store to store documents downloaded by the spiders while awaiting indexing.

    Note: Currently implemented with a Queue, but should be implemented with something like sqlite3.
    """

    def __init__(self):
        self.storage = Queue()
        #self.path = path

        #con = sqlite3.connect(path)
        #with con:
        #    cur = con.cursor()
        #    if not _has_table(path, 'Documents'):
        #        cur.execute("CREATE TABLE Documents(url TEXT, content BLOB, content_type TEXT)")
        #    con.commit()

    def put(self, url, document, content_type):
        """
        Store the content of a document, its corresponding URL and its content-type

        url -- The document's URL
        document -- The document's content
        content_type -- The document's MIME content-type
        """
        self.storage.put((url, document, content_type))
        #con = sqlite3.connect(self.path)
        #with con:
        #    cur = con.cursor()
        #    cur.execute('INSERT INTO Documents VALUES(?, ?, ?)', (url, buffer(document), content_type))
        #    con.commit()

    def get(self):
        """
        Get a document, its corresponding URL and its content-type from the document store.
        """
        return self.storage.get()
        #con = sqlite3.connect(self.path)
        #with con:
        #    cur = con.cursor()
        #    cur.execute('SELECT * from Documents limit 1')
        #    url, document, content_type = cur.fetchone()
        #    document = str(document)


#def _has_table(path, table):
    #con = sqlite3.connect(path)
    #with con:
    #    cur = con.cursor()
    #    cur.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="' + table + '";')
    #    data = cur.fetchone()

    #if data is None:
    #    return False
    #else:
    #    return True