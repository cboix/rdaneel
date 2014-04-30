"""
 " scraper.py
 " Scrapes le Reddit
"""

from bs4 import BeautifulSoup
from webscraper import Scraper
from sys import argv
from httplib import IncompleteRead
from urllib2 import HTTPError

import redis
import time

# Maximum number of times that we're willing to retry after a failed connection
MAX_RETRIES = 3

class RedditScraper(Scraper):

    def __init__(self):
        super(RedditScraper, self).__init__()

        # Unique user-agent to prevent Reddit from immediately blocking us
        self.browser.addheaders = [('User-agent', "R. Daneel Olivaw, Esq.")]

        # Subreddit, ID of last thing we scraped
        self.MAGIC_URL = "http://www.reddit.com/r/%s/top/?sort=top&t=day&after=t3_%s"
        self.lastID = ""
        self.db = redis.StrictRedis(host='localhost', port=6379, db=0)
        
    def scrapePage(self, subreddit, lastID):
        """ Given a subreddit and an id string, scrapes the page of the subreddit
        that begins with that id string, writes the scraped data to files, and
        then returns the id of the last element on the page. """

        soup = self.soupFromParams([subreddit, lastID])
        posts = soup.find(id="siteTable")
        titleLinks = posts.find_all('a', class_="comments")

        if titleLinks == []:
            raise EOFError()

        lastID = ""

        for link in titleLinks:
            retries = 0
            url = link.get('href')
            while retries < MAX_RETRIES:
                try:
                    lastID = self.scrapeComment(url)
                    retries = MAX_RETRIES
                except HTTPError:
                    retries += 1
                

        return lastID

    def scrapeTitle(self,title,d):
        """ Get the important attributes from the title and return 
        the updated dictionary with them """
        d['karma'] = int(title['data-ups'].encode('utf-8')) - \
                int(title['data-downs'].encode('utf-8'))
        head = title.find('a', {'class' : 'title may-blank '})
        d['date'] = title.find('time')['title'].encode('utf-8')
        return(d)

    # scrape a single comment, return id, content, karma.
    def scrapeOneComment(self,comment,soup):
        cDict = { 'id' : comment['id'].split('_')[1].encode('utf-8')}
        # FIX encoding AND Hyperlinking!
        try:
            cDict['content'] = comment.find('div',{'class' : 'md'}).getText().encode('utf-8')
            title = soup.find('div', {'data-fullname' : 't1_' + cDict['id'][:-3]})
            if (title is None):
                cDict = None
            else: 
                cDict['karma'] = int(title['data-ups'].encode('utf-8')) - \
                    int(title['data-downs'].encode('utf-8'))
        except AttributeError:
            cDict = None
        return(cDict)

    def scrapeComment(self,cUrl):
        # initialize dictionary, create soup
        post = {'id' : cUrl.split('/')[6], 'forum' : cUrl.split('/')[4], 'postUrl' : cUrl}
        soup = self.soupFromURL(cUrl)
        post['name'] = soup.find('title').getText().encode('utf-8')
        post['content'] = soup.find('meta',{ 'name' : 'description' })['content'].encode('utf-8')
        title = soup.find('div',attrs={'data-fullname' : 't3_' + post['id']})
        # update attributes from title:
        post = self.scrapeTitle(title,post)

        # get only the parent comments:
        comments = soup.findAll('div',attrs={'class' : 'entry unvoted'})
        cAll = [x.find('form',attrs={'class' : 'usertext'}) for x in comments]
        cAll = [x for x in cAll if x != None]

        childcomments = soup.findAll('div',attrs={'class' : 'child'})
        cChild = [x.findAll('form',attrs={'class' : 'usertext'}) for x in childcomments]
        cChild = [item for sublist in cChild for item in sublist] # flatten
        cChild = list(set(cChild)) # unique
        cChild = [x for x in cChild if x != None] # not needed
        cParents = [x for x in cAll if x not in cChild]

        # for all of the parent comments, get text + upvotes
        post['comments'] = [self.scrapeOneComment(x,soup) for x in cParents]
        # remove if none:
        post['comments'] = [x for x in post['comments'] if x != None]
        # output to file and return hash:
        self.writePost(post)
        return(post['id'])

    def writePost(self, post):
        """ Saves the post's data into our redis database. """
        """
        print "post id: ", post['id']
        print "post karma: ", post['karma']
        print "type of post karma: ", type(post['karma'])
        """
        self.db.zadd('posts', post['karma'], post['id'])
        self.db.hmset('post:' + post['id'], 
                      {
                          'name': post['name'],
                          'forum': post['forum'],
                          'date': post['date'],
                          'content': post['content'],
                          'karma': post['karma'],
                          'comments': 'comments:' + post['id'],
                      })
        
        for comment in post['comments']:
            self.db.zadd('comments:' + post['id'],
                         comment['karma'],
                         'comment:' + comment['id'])
            self.db.hmset('comment:' + comment['id'],
                          {
                              'content': comment['content'],
                              'karma': comment['karma'],
                          })

def test():
    rs = RedditScraper()
    rs.scrapePage('AskReddit', "1rgpdf")

    firstID = rs.db.zrange('posts', 0, 0, desc=True)[0]
    firstTitle = rs.db.hget('post:' + firstID, 'title')

if __name__ == "__main__":
    if len(argv) < 2:
        lastID = ""
    else:
        script, lastID = argv

    rs = RedditScraper()

    try:
        # Can only scan the first 40 pages of top
        for i in range(40):
            print "Page %d: %s" % (i, lastID)
            lastID = rs.scrapePage('AskReddit', lastID)
    except EOFError:
        pass
