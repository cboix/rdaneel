"""
 " scraper.py
 " Scrapes le Reddit
"""

from bs4 import BeautifulSoup
from urllib2 import HTTPError
from webscraper import Scraper

import csv
import json

class RedditScraper(Scraper):
    
    def __init__(self):
        super(RedditScraper, self).__init__()
        
        # ID of last thing we scraped
        self.MAGIC_URL = "http://www.reddit.com/r/AskReddit/top/?sort=top&t=year&after=%s"
        self.lastID = ""

    def scrapePage(self, year, month):
        """ Given an id string, scrapes the page that begins with that id string,
        writes the scraped data to files, and then returns the id of the last
        element on the page. """

        # Find all urls/metadata
        # for commentUrl:
        #     scrapeComment(commentUrl)
        # lastID = getLastID()
        # return lastID
