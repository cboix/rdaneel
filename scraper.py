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
        self.database = ""
        
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
            lastID = self.scrapeComment(url)
            
        return lastID

        """ Scrapes the individual page """

        # Find all urls/metadata
        # for commentUrl:
        #     scrapeComment(commentUrl)
        # lastID = getLastID()
        # return lastID

    def scrapeComment(self,cUrl):
        d = {'hash' : cUrl.split('/')[6], 'forum' : cUrl.split('/')[4], 'postUrl' : cUrl}
        soup = soupFromUrl(cUrl)
        title = soup.find('div',attrs={'class' : ' thing id-t3_' + hash_str + ' odd link '})
        d = scrapeTitle(title,d)
        # get html, 
        # get soup

        # find titles etc.

        # go through top comments:
        # for comment in:

        # output to file

        return(d['hash'])

    def scrapeTitle(self,title,d):
        """ Get the important attributes from the title and return 
        the updated dictionary with them """
        d['data-ups'] = title['data-ups'].encode()
        d['data-downs'] = title['data-downs'].encode()
        head = title.find('a', {'class' : 'title may-blank '})
        d['title'] = head.getText().encode()
        d['outUrl'] = head['href'].encode()
        d['time'] = title.find('time')['title'].encode()
        return(d)


if __name__ == "__main__":
    rs = RedditScraper();
