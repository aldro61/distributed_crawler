__author__ = 'Alexandre'

import shelve
from os.path import isfile
from multiprocessing import Process, Manager
from spider import Spider
from documentProcessor import DocumentProcessor
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


    WARNING: The current implementation does not have a politeness mechanism. Please be careful and use
    this code at your own risks. To slow down the rate of web requests sent to servers, I have temporarely
    placed a sleep(2) instruction in the spider's code.
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
        self.frontier = Manager().Queue()
        self.document_store = Manager().Queue()
        self.visited_cache_path = 'visited_cache'
        self.visited_cache = None
        self.index = Index()
        self.n_spiders = n_spiders
        self.n_document_processors = n_document_processors
        self.indexable_content_types = indexable_content_types
        self.spiders = []
        self.document_processors = []
        self.seed_urls = seeds
        for seed_url in seeds:
            self.frontier.put(seed_url)
        self.status = 'STOPPED'


    def start(self):
        """
        Starts the crawler
        """
        if self.status != "STOPPED":
            raise Exception("Attempted to start a crawler that was not stopped.")

        self.visited_cache = shelve.open(self.visited_cache_path, 'n')

        self._start_spiders()
        print 'Spider all started.'

        #Start the document processor processes
        self._start_document_processors()
        print 'Document processors all started.'

        self.status = "RUNNING"
        print "Started."


    def stop(self):
        """
        Stops the crawler and resets the current instance to
        it's initialisation parameter values.
        """
        if self.status != "RUNNING":
            raise Exception('Attempted to stop a crawler that was not running.')

        #Stop all spiders
        print 'Stopping spiders.'
        for spider in self.spiders:
            spider.terminate()

        #Stop all document processors
        print 'Stopping document processors.'
        for doc_processor in self.document_processors:
            doc_processor.terminate()

        self.status = "STOPPED"
        self.__init__(self.n_spiders, self.n_document_processors, self.seed_urls, self.indexable_content_types)
        self.visited_cache.close()
        print 'Stopped.'

    def restart(self):
        """
        Stops the crawler, resets the current instance to
        it's initialisation parameter values and starts the
        new instance.
        """
        if self.status != "RUNNING" and self.status != "PAUSED":
            raise Exception('Attempted to stop a crawler that was not running.')

        if self.status == "RUNNING":
            self.stop()

        self.__init__(self.n_spiders, self.n_document_processors, self.seed_urls, self.indexable_content_types)
        self.start()
        print 'Restarted.'

    def pause(self):
        """
        Stops the crawler and preserves the current
        instance's values (ex: caches, frontier)

        Use myCrawler.resume() to resume crawling

        Note: The files that were in the process of being processed by a
              spider or document processor instance will be lost. (Future works)
        """
        if self.status != "RUNNING":
            raise Exception('Attempted to pause a crawler that was not running.')

        self._stop_crawling()
        self.status = "PAUSED"
        print "Paused."

    def resume(self):
        """
        Resumes the activity of a paused crawler
        """
        if self.status != "PAUSED":
            raise Exception('Attempted to resume a crawler that was not paused.')

        self._start_spiders()
        self._start_document_processors()

        self.status = "RUNNING"
        print 'Resumed.'

    def request_crawl(self, url):
        """
        Request for a url to be crawled
        """
        # should verify format
        self.frontier.put(url)

    def _stop_crawling(self):
        """
        Stops all spider and document processor processes
        """
        #Stop all spiders
        print 'Stopping spiders.'
        for spider in self.spiders:
            spider.terminate()

        #Stop all document processors
        print 'Stopping document processors.'
        for doc_processor in self.document_processors:
            doc_processor.terminate()

    def _start_spiders(self):
        """
        Starts all spider processes
        """
        #Start the spider processes
        self.spiders = []
        for i in xrange(self.n_spiders):
            spider_instance = Spider(i, self.frontier, self.document_store)
            spider_process = Process(target=spider_instance)
            spider_process.daemon = True
            self.spiders.append(spider_process)
            spider_process.start()
        print 'Spider all started.'

    def _start_document_processors(self):
        """
        Starts all document processor processes
        """
        #Start the document processor processes
        self.document_processors = []
        visited_cache_lock = Manager().Lock()
        for i in xrange(self.n_document_processors):
            doc_processor_instance = DocumentProcessor(i,
                self.frontier,
                self.document_store,
                self.visited_cache,
                self.index,
                self.indexable_content_types)
            doc_processor_process = Process(target=doc_processor_instance, args=(visited_cache_lock,))
            doc_processor_process.daemon = True
            self.document_processors.append(doc_processor_process)
            doc_processor_process.start()
