"""
 " parser.py
 " Parses our Redis database into a corpus of the form used by Gensim
"""

from collections import defaultdict
from gensim.corpora.dictionary import Dictionary
from gensim.corpora.bleicorpus import BleiCorpus
from os.path import isfile

import redis
import json
import time

# Our Redis database
db = redis.StrictRedis(host='localhost', port=6379, db=0)

# Name of dictionary file
dictName = 'redditdict.txt'

# Maps words to ids, keeps track of global frequency and per-document frequency
globalDict = Dictionary()

# Words to exclude from our analysis (selected somewhat arbitrarily from 
# https://en.wikipedia.org/wiki/Most_common_words_in_English)
ignoreList = ["the", "are", "is", "were", "was", "to", "of", "and", "a", "in",
              "that", "have", "had", "has", "i", "it", "for", "not", "on",
              "with", "without", "he", "she", "they", "as", "you", "do", "did",
              "does", "at", "this", "but", "his", "her", "hers", "their", "by",
              "from", "we", "or", "an", "will", "my", "your", "all", "there",
              "what", "so", "up", "down", "out", "if", "about", "no", "yes",
              "just", "him", "its", "also", "any", "me", "like", "be", "im",
              "them", "dont", ]
ignoreSet = set(ignoreList)

# Characters to remove from our strings
stripChars = ['\'', '"', '\\', '?', '!', '.', ',', '#', '$', '%', '^', '&', '*',
              '(', ')', '-', '+', '=', '[', ']', '{', '}', '/', '~', '`', ]

# Characters to remove without replacing with spaces
noSpaceChars = [';', ':']

# Differentiate words that appear in titles so that we can weight them more
# strongly
titleMarker = '#'

# We need to map ids to posts, but we also need to ignore them as words
idMarker = '$'

def cleanAndSplitString(s, isTitle=False):
    """ Sends s to lowercase, strips off all special characters, splits, adds a
    marker token for titles. """

    s = s.lower()
    for char in stripChars:
        s = s.replace(char, ' ')

    for char in noSpaceChars:
        s = s.replace(char, '')

    words = s.split()
    filtered = filter(lambda w: w not in ignoreSet, words)
    if isTitle:
        filtered = ['#' + w for w in filtered]

    return filtered

def contentFromId(contentid, isTitle=False):
    """ Cleans and splits the words in the given post title or comment, stores
    the cleaned content as a field in the post/comment. """

    keyword = 'name' if isTitle else 'content'
    content = db.hget(contentid, keyword)
    cleanList = cleanAndSplitString(content, isTitle)
    listStr = json.dumps(cleanList)
    db.hset(contentid, 'cleancontent', listStr)

    return cleanList

def addPostToDict(postid):
    """ Adds the given post to the dictionary. """

    postKey = 'post:%s' % postid
    title = contentFromId(postKey, isTitle=True)
    
    commentKey = 'comments:%s' % postid
    numComments = db.zcard(commentKey)
    commentids = db.zrevrange(commentKey, 0, numComments)

    # Remove duplicate comments
    commentids = list(set(commentids))

    comments = [contentFromId(commentid) for commentid in commentids]
    # Flatten comments
    postContent = [word for comment in comments for word in comment]
    postContent.extend(title)

    # Add the id to the contents
    postContent.append(idMarker + postid)

    contentStr = json.dumps(postContent)
    db.hset(postKey, 'document', contentStr)

    globalDict.doc2bow(postContent, allow_update=True)

def getPostids():
    """ Return a list of all unique post ids. """

    numPosts = db.zcard('posts')
    postids = db.zrevrange('posts', 0, numPosts)
    # Remove duplicate posts
    postids = list(set(postids))

    return postids

def buildDictionary(force=False):
    """ Build a dictionary in which each post corresponds to a document. """

    if force or not isfile(dictName):
        postids = getPostids()
        numPosts = len(postids)

        count = 0
        for postid in postids:
            if count % 100 == 0:
                print "Added %d out of %d to dictionary: %s" % (count, numPosts, time.strftime("%H:%M:%S"))
            addPostToDict(postid)
            count += 1

    else:
        globalDict = Dictionary.load(dictName)

def corpusOfPost(postid, force=False):
    """ Returns the contents of the given post in corpus vector form. """

    postKey = 'post:%s' % postid
    corpusStr = db.hget(postKey, 'corpus')

    if force or corpusStr == "":
        docStr = db.hget(postKey, 'document')
        doc = json.loads(docStr)
        corpus = globalDict.doc2bow(doc)
        corpusStr = json.dumps(corpus)
        db.hset(postKey, 'corpus', corpusStr)
    else:
        corpus = json.loads(corpusStr)

    return corpus

class RedisCorpus(object):
    def __init__(self, postids):
        self.postids = postids
        self.numPosts = len(self.postids)
        
    def __iter__(self):
        count = 0
        for postid in self.postids:
            if count % 100 == 0:
                print "Wrote %d out of %d to corpus: %s" % (count, self.numPosts, time.strftime("%H:%M:%S"))
            count += 1
            yield corpusOfPost(postid, force=True)

def buildCorpus():
    """ Returns a corpus object that contains sparse vectors from every post. """

    postids = getPostids()
    corpus = RedisCorpus(postids)
    return corpus

if __name__ == "__main__":
    buildDictionary(force=True)
    globalDict.save(dictName)

    corpus = buildCorpus()
    BleiCorpus.serialize('redditcorpus.lda-c', corpus)
