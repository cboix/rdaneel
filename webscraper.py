"""
 " webscraper.py
 " General-purpose web scraper
"""

from bs4 import BeautifulSoup

import mechanize

class Scraper(object):

    def __init__(self):
        self.browser = mechanize.Browser()
        self.MAGIC_URL = ""

    def getUrl(self, l):
        """ Fills in the base url with the passed parameters. """
        
        url = self.MAGIC_URL % tuple(l)
        return url

    def soupFromParams(self, urlParams):
        """ Returns a Beautiful Soup of the page with the given parameters. """

        url = self.getUrl(urlParams)
        soup = self.soupFromURL(url)

        return soup

    def soupFromURL(self, url):
        """ Returns a Beautiful Soup of the given url. """

        page = self.browser.open(url)
        html = page.read()
        soup = BeautifulSoup(html)

        return soup
