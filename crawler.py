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
import re
import StringIO
import sys
import urllib2
import zlib

from bs4 import BeautifulSoup
from multiprocessing import Pool
from bs4 import SoupStrainer
from urlparse import urljoin, urlparse, urlunparse
from urllib import unquote
from string import lower

simulated_index = Queue.Queue()


class Document():
    """
    Basic document. Contains the document URL and content.
    More fields could be added to this class if required.
    """

    def __init__(self, url, content):
        self.url = url
        self.content = content



_collapse = re.compile('([^/]+/\.\./?|/\./|//|/\.$|/\.\.$)')
_server_authority = re.compile('^(?:([^\@]+)\@)?([^\:]+)(?:\:(.+))?$')
_default_port = {   'http': '80',
                    'https': '443',
                    'gopher': '70',
                    'news': '119',
                    'snews': '563',
                    'nntp': '119',
                    'snntp': '563',
                    'ftp': '21',
                    'telnet': '23',
                    'prospero': '191',
                    }
_relative_schemes = [   'http',
                        'https',
                        'news',
                        'snews',
                        'nntp',
                        'snntp',
                        'ftp',
                        'file',
                        ''
]
_server_authority_schemes = [   'http',
                                'https',
                                'news',
                                'snews',
                                'ftp',
                                ]


def normalize_url(urlstring):
    """given a string URL, return its normalised form"""
    return urlunparse(norm(urlparse(urlstring)))


def norm(urltuple):
    """given a six-tuple URL, return its normalised form"""
    (scheme, authority, path, parameters, query, fragment) = urltuple
    scheme = lower(scheme)
    if authority:
        userinfo, host, port = _server_authority.match(authority).groups()
        if host[-1] == '.':
            host = host[:-1]
        authority = lower(host)
        if userinfo:
            authority = "%s@%s" % (userinfo, authority)
        if port and port != _default_port.get(scheme, None):
            authority = "%s:%s" % (authority, port)
    if scheme in _relative_schemes:
        last_path = path
        while 1:
            path = _collapse.sub('/', path, 1)
            if last_path == path:
                break
            last_path = path
    path = unquote(path)
    return scheme, authority, path, parameters, query, fragment




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


def open(url):
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
    opener.addheaders = [('User-Agent','graal-crawl beta'),
                        ('Accept-Encoding', 'gzip,deflate')]
    usock = opener.open(url)
    usock.geturl()
    data = decode(usock)
    usock.close()
    return data


def download_doc(url):
    """
    Downloads the file associated to an URL and extracts its content.
    The callback function download_doc_callback is called when the
    download and content extraction is completed.

    url -- The file's URL
    """
    html = open(url)
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
    """
    html = open(url)
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
                visited[href] = 1

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
        print 'Crawling ' + url + "    crawl_frontier size: " + str(crawl_frontier.qsize()) + "   found_files_size: " + str(
            found_files.qsize())
        try:
            crawl_url(url, crawl_frontier, found_files, search_extensions=['.txt'], visited=visited)
        except Exception as e:
            print e

        #In a normal scenario, we would have multiple processes consuming the files
        #to download as they are added to the download queue. Here, for test purposes,
        #we do not use such processes, therefore we stop searching for files when we
        #have collected a given number.
        if found_files.qsize() > 5:
            print "Stopped with queue size of " + str(crawl_frontier.qsize())
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
    while not simulated_index.empty():
        document = simulated_index._get()
        print "*"*20
        print document.url
        print document.content
        print

    print "Completed!"
