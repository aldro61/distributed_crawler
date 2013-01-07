# -*- coding: utf-8 -*-
"""
This is a basic implementation of a crawler written in Python. It is incomplete, but attempts
to validate if Python is suitable to be used for this task.

Notes:
    Does not yet handle:
        * Crawling for files using REs
        * Validating that a file is not already in the index before downloading it
        * Crawling in parallel using multiple processes to consume the urls from to_visit.

    * The Queue data structure is fully thread safe
    * The multiprocessing package bypasses the GIL and allows the use of all CPUs on a machine
"""
__author__ = 'Alexandre Drouin'

import gzip
import Queue
import StringIO
import sys
import time
import urllib2
import zlib

from bs4 import BeautifulSoup
from multiprocessing import Pool
from bs4 import SoupStrainer
from urlparse import urljoin
from urlnorm import norms as normalize_url

simulated_index = Queue.Queue()


class Document():
    """
    Basic document. Contains the document URL and content.
    More fields could be added to this class if required.
    """

    def __init__(self, url, content):
        self.url = url
        self.content = content


def decode(page):
    """
    Decodes the content of a gzipped web response.

    page -- The web response
    """
    encoding = page.info().get("Content-Encoding")
    if encoding in ('gzip', 'x-gzip', 'deflate'):
        content = page.read()
        if encoding == 'deflate':
            data = StringIO.StringIO(zlib.decompress(content))
        else:
            data = gzip.GzipFile('', 'rb', 9, StringIO.StringIO(content))
        content = data.read()
    else:
        content = page.read()

    return content


def open(url, prohibited_types):
    """
    Opens a URL and return's its content.
    Note that in our requests, we prefer gzipped content, since
    the time required to download the content is greatly reduced.
    This allows our crawler to be significantly faster than if it
    used urllib2 to download the uncompressed content at an URL.
    The uncompression requires more CPU activity, but the true
    bottleneck in this situation is the time required to download
    the files.

    url -- The url to open
    """
    opener = urllib2.build_opener()
    opener.addheaders = [('User-Agent', 'graal-crawl beta'),
                         ('Accept-Encoding', 'gzip,deflate')]

    usock = opener.open(url, timeout=2)
    url = usock.geturl()

    dont_download = False
    for type in prohibited_types:
        if type in usock.headers.type:
            dont_download = True
            break

    if not dont_download:
        data = decode(usock)
    else:
        data = "<html></html>"
    usock.close()

    return data


def download_doc(url):
    """
    Downloads the file associated to an URL and extracts its content.
    The callback function download_doc_callback is called when the
    download and content extraction is completed.

    url -- The file's URL
    """
    html = open(url, [])
    soup = BeautifulSoup(html)
    return Document(url, soup.get_text())


def download_doc_callback(arg):
    """
    Callback function for when a download worker has successfully
    downloaded the content of a file.

    This function simulates the transmission of the file to an index.

    arg -- An instance of class Document representing the downloaded file.
    """
    print "Submitting new document for indexing:" + arg.url
    simulated_index.put(arg)
    print "Index now contains: " + str(simulated_index.qsize()) + " documents"


def crawl_url(url, crawl_frontier, found_files, search_extensions, visited):
    """
    Searches for URLs in the content of another URL. Files with given
    extensions are also flagged for downloading.

    url -- The source URL to look into
    crawl_frontier -- A reference to a queue of URLs to visit
    found_files -- A reference to a queue of files flagged for download
    search_extensions -- A list of extensions of files which must be flagged for download
    visited -- A dictionnary containing the visited URLs
    """
    html = open(url, prohibited_types=['image', 'application'])
    links = SoupStrainer('a')
    soup = BeautifulSoup(html, parse_only=links)
    for link in soup.find_all('a'):
        href = link.get('href')
        if href is not None:
            href = normalize_url(href)
            #Convert relative urls to absolute urls
            if not href.startswith('http'):
                href = urljoin(url, href)

            #Already visited check
            if not visited.has_key(href):
                crawl_frontier._put(href)
                visited[href] = int(time.time())

            #Check if the file has the extension we are looking for
            for ext in search_extensions:
                if href[-len(ext):] == ext:
                    found_files.put(href)


if __name__ == "__main__":
    visited = {}
    found_files = Queue.Queue()
    crawl_frontier = Queue.Queue()
    crawl_frontier.put(sys.argv[1])

    #Crawl from the source URL by doing a BFS.
    #The crawler searches for files with specific extensions.
    #If such files are found, they are added to a download queue called found_files
    #Note that future improvement include search for files using regular expressions
    #instead of just extensions.
    #All found URLs are also added to a visit queue called to_visit
    while not crawl_frontier.empty():
        url = crawl_frontier._get()
        print 'Crawling ' + url + "    crawl_frontier size: " + str(
            crawl_frontier.qsize()) + "   found_files_size: " + str(
            found_files.qsize())
        try:
            crawl_url(url, crawl_frontier, found_files, search_extensions=['.txt'], visited=visited)
        except Exception as e:
            print e

        #In a normal scenario, we would have multiple processes consuming the files
        #to download as they are added to the download queue. Here, for test purposes,
        #we do not use such processes, therefore we stop searching for files when we
        #have collected a given number.
        if found_files.qsize() >= 5:
            print "Stopped with frontier size of " + str(crawl_frontier.qsize()) + " and " + str(
                found_files.qsize()) + " found files"
            break

    #We define a pool of processes in charge of downloading the files and communicating
    #with the index to add the files once they are downloaded.
    pool = Pool()
    while not found_files.empty():
        pool.apply_async(download_doc, (found_files._get(),), callback=download_doc_callback)
    pool.close()
    pool.join()

    #To assess the performance of our crawler, we display the content of the files
    #contained in the index.
    print simulated_index.qsize()
    while not simulated_index.empty():
        document = simulated_index._get()
        print "*" * 20
        print document.url
        print document.content
        print

    print "Completed!"
