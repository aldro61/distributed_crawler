__author__ = 'Alexandre'
import multiprocessing

from frontier import Frontier
from spider import Spider
from documentProcessor import DocumentProcessor
from documentStore import DocumentStore
from visitedURLCache import VisitedURLCache
from index import Index


class Crawler:
    """
    A distributed crawler based on the offline crawler model.

    See the following link for an introduction:
    http://blog.mischel.com/2011/12/16/writing-a-web-crawler-crawling-models/

    The crawler is composed of two main types of components:
    * The spiders: Fetch URLs to download from a queue called the frontier,
                   download the documents and add them to a temporary document store.
    * The document processors: Fetch downloaded documents from a document store,
                               if they are html pages, parses them for URLs, adds
                               the found URLs to the frontier and finally, submits
                               appropriate files for indexation based on their content-
                               type.
    """

    def __init__(self, n_spiders=1, n_document_processors=1, seeds=None, indexable_content_types=None):
        """
        n_spiders -- The number of spider processes to use
        n_document_processors -- The number of document processors to use
        seeds -- A list of initial URLs to crawl
        indexable_content_types -- A list of MIME content-types for which files should be indexed
        """
        if not indexable_content_types: indexable_content_types = ['text/html']
        if not seeds: seeds = []
        self.frontier = Frontier()
        self.document_store = DocumentStore()
        self.visited_cache = VisitedURLCache()
        self.index = Index()
        self.n_spiders = n_spiders
        self.n_document_processors = n_document_processors
        self.indexable_content_types = indexable_content_types
        self.spiders = []
        self.document_processors = []

        for seed_url in seeds:
            self.frontier.put(seed_url)

    def run(self):
        #Start the spider processes
        for i in xrange(self.n_spiders):
            spider_instance = Spider(i, self.frontier, self.document_store)
            spider_process = multiprocessing.Process(target=spider_instance)
            spider_process.daemon = True
            self.spiders.append(spider_process)
            spider_process.start()

        #Start the document processor processes
        for i in xrange(self.n_document_processors):
            doc_processor_instance = DocumentProcessor(i,
                self.frontier,
                self.document_store,
                self.visited_cache,
                self.index,
                self.indexable_content_types)
            doc_processor_process = multiprocessing.Process(target=doc_processor_instance)
            doc_processor_process.daemon = True
            self.document_processors.append(doc_processor_process)
            doc_processor_process.start()

    def stop(self):
        #Stop all spiders
        print 'Stopping spiders.'
        for spider in self.spiders:
            spider.terminate()

        #Stop all document processors
        print 'Stopping document processors.'
        for doc_processor in self.document_processors:
            doc_processor.terminate()
        print 'Done.'

    def request_crawl(self, url):
        """
        Request for a url to be crawled
        """
        # should verify format
        self.frontier.put(url)



