__author__ = 'Alexandre'

import urllib2
from bs4 import BeautifulSoup

if __name__ == "__main__":
    while(True):
        response = urllib2.urlopen('http://python.org/')
        html = response.read()
        print html