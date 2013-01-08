__author__ = 'Alexandre'

from bs4 import BeautifulSoup
from urlparse import urljoin, urlparse
from urlnorm import url_normalize as normalize_url

class DocumentProcessor:
    """
    A document processor has 3 main tasks:
    1) Parse documents contained in the document store for URLs
    2) Find which URLs are appropriate and add them to the frontier
    3) Contact the index for the submission of files if they have a
       desired content-type.
    """

    def __init__(self, document_processor_id, frontier, document_store, visited_cache, index, indexable_content_types):
        """
        document_processor_id -- The document processor's indentifier
        frontier -- The crawler's frontier
        document_store -- The crawler's document store
        visited_cache -- The crawler's visited URL cache
        index -- The crawler's index
        indexable_content_types -- Content-types for which files should be indexed
        """
        self.document_processor_id = document_processor_id
        self.frontier = frontier
        self.document_store = document_store
        self.visited_cache = visited_cache
        self.index = index
        self.indexable_content_types = indexable_content_types

        self.authorized_extensions = ['dtd',
                                      'rna',
                                      'xml',
                                      'html',
                                      'htm',
                                      'xhtml',
                                      'xht',
                                      'mht',
                                      'mhtml',
                                      'maff',
                                      'asp',
                                      'aspx',
                                      'bml',
                                      'cfm',
                                      'cgi',
                                      'ihtml',
                                      'jsp',
                                      'las',
                                      'lasso',
                                      'lassoapp',
                                      'pl',
                                      'php',
                                      'phtml',
                                      'shtml',
                                      'stm']

    def __call__(self, visited_cache_lock):
        """
        Main routine of a document processor.
        """
        while True:
            url, document, content_type = self.document_store.get()
            #print "PROCESSING DOCUMENT: ", url
            if content_type is not None:
                if 'text/html' in content_type:
                    for u in self.get_urls(url, document):
                        visited_cache_lock.acquire()
                        if self.frontier_extension_allowed(u) and not self.visited_cache.has_key(u) and\
                           not self.in_domain_do_not_crawl_list(url):
                            self.visited_cache[u] = 1
                            self.frontier.put(u)
                            #print "Added ", u, " to the frontier"
                        visited_cache_lock.release()

                #Check if the document should be indexed
                if self.index_content_type_allowed(content_type):
                    print "Indexing ", url
                    self.index.put(url, document, content_type)
            #print "DOCUMENT PROCESSED: ", url

    def index_content_type_allowed(self, content_type):
        """
        Verifies if a content type should be allowed for indexing

        content_type -- The content-type
        """
        for c in self.indexable_content_types:
            if c[1] in content_type:
                return True
        return False

    def frontier_extension_allowed(self, url):
        """
        Verifies if a URL has a valid extension to be added into the frontier

        Note: Must be implemented. Currently allow's anything
        """
        filename = urlparse(url).path.split('/')

        if filename[-1] == "":
            return True
        else:
            extension = filename[-1].split('.')[-1]
            for c in self.indexable_content_types:
                if c[0] == extension:
                    return True

            for ext in self.authorized_extensions:
                if ext == extension:
                    return True
        return False

    def in_domain_do_not_crawl_list(self, url):
        """
        Verifies if the domain for a URL is in the do not crawl list
        """
        if 'arxiv' in url or 'wordpress' in url:
            return True
        else:
            return False

    #TODO: handle a robots.txt cache and download the robot.txt on cache miss. Use robotparse to parse.

    def get_urls(self, url, document):
        """
        Gets all the URLs in a document and returns them as absolute URLs.

        url -- The url of the document
        document -- The content of the document
        """
        urls = []
        soup = BeautifulSoup(document)

        for link in soup.find_all('a'):
            href = link.get('href')
            if href is not None:
                try:
                    #Convert relative urls to absolute urls
                    if not href.startswith('http'):
                        href = urljoin(url, href)
                    href = normalize_url(href)

                    urls.append(href)
                except:
                    pass

        return urls