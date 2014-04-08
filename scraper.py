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
        
        # Subreddit, ID of last thing we scraped
        self.MAGIC_URL = "http://www.reddit.com/r/%s/top/?sort=top&t=year&after=%s"
        self.lastID = ""
        
    def scrapeComments(self, url):
        return ""

    def scrapePage(self, subreddit, lastID):
        """ Given a subreddit and an id string, scrapes the page of the subreddit
        that begins with that id string, writes the scraped data to files, and
        then returns the id of the last element on the page. """

        soup = self.soupFromParams([subreddit, lastID])
        posts = soup.find(id="siteTable")
        titles = posts.find_all(class_="title")
        lastID = ""

        for title in titles:
            url = title.a.get('href')
            print url
            lastID = self.scrapeComments(url)
            
        return lastID

if __name__ == "__main__":
    rs = RedditScraper();
