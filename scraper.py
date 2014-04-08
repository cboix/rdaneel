"""
 " scraper.py
 " Scrapes le Reddit
"""

from bs4 import BeautifulSoup
from webscraper import Scraper

import redis

class RedditScraper(Scraper):

    def __init__(self):
        super(RedditScraper, self).__init__()

        # Subreddit, ID of last thing we scraped
        self.MAGIC_URL = "http://www.reddit.com/r/%s/top/?sort=top&t=year&after=%s"
        self.lastID = ""
        self.db = redis.StrictRedis(host='localhost', port=6379, db=0)
        
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

    def writePost(self, post):
        """ Saves the post's data into our redis database. """

        self.db.zadd('posts', post['id'], post['karma'])
        self.db.hmset('post:' + post['id'], 
                      {
                          'name': post['name'],
                          'date': post['date'],
                          'content': post['content'],
                          'karma': post['karma'],
                          'comments': 'comments:' + post['id'],
                      })
        
        for comment in post['comments']:
            self.db.zadd('comments:' + post['id'],
                         'comment:' + comment['id'],
                         comment['karma'])
            self.db.hmset('comment:' + comment['id'],
                          {
                              'content': comment['content'],
                              'karma': comment['karma'],
                          })

if __name__ == "__main__":
    rs = RedditScraper();
