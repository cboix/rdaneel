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

        """ Scrapes the individual page """

        # Find all urls/metadata
        # for commentUrl:
        #     scrapeComment(commentUrl)
        # lastID = getLastID()
        # return lastID

    def scrapeTitle(self,title,d):
        """ Get the important attributes from the title and return 
        the updated dictionary with them """
        d['karma'] = int(title['data-ups'].encode()) - \
                int(title['data-downs'].encode())
        head = title.find('a', {'class' : 'title may-blank '})
        d['name'] = head.getText().encode()
        d['content'] = head['href'].encode()
        d['date'] = title.find('time')['title'].encode()
        return(d)

    def scrapeComment(self,cUrl):
        # initialize dictionary, create soup
        post = {'id' : cUrl.split('/')[6], 'forum' : cUrl.split('/')[4], 'postUrl' : cUrl}
        soup = soupFromUrl(cUrl)
        title = soup.find('div',attrs={'class' : ' thing id-t3_' + post['id'] + ' odd link '})
        # update attributes from title:
        post = scrapeTitle(title,post)

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
        post['comments'] = [scrapeOneComment('',x,soup) for x in cParents]
        # Filter those with more or less karma!? No.

        # output to file
        writePost(post)
        return(post['id'])

    def scrapeOneComment(self,comment,soup):
        cDict = { 'id' : comment['id'].split('_')[1].encode()}
        # FIX encoding AND Hyperlinking!
        cDict['content'] = comment.find('div',{'class' : 'md'}).getText().encode()
        title = soup.find('div', {'data-fullname' : 't1_' + cDict['id'][:-3]})
        cDict['karma'] = int(title['data-ups'].encode()) - \
                int(title['data-downs'].encode())
        return(cDict)

        # If it has urls, make sure to note this: 

    def writePost(self, post):
        """ Saves the post's data into our redis database. """

        # Earth to Greg: take forum as well:
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

def test():
    rs = RedditScraper()
    rs.scrapePage('AskReddit', "1rgpdf")
    firstID = rs.db.zrange('posts', 0, 0, desc=True)[0]
    print "ID (should be '1qoyn2'):"
    print firstID
    firstTitle = rs.db.hget('post:' + firstID, 'title')
    print "Title (should be 'Assume all of world history is a movie. What are the biggest plotholes?'):"
    print firstTitle

if __name__ == "__main__":
    test()
