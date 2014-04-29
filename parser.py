"""
 " parser.py
 " Parses our Redis database into a corpus of the form used by Gensim
"""

from collections import defaultdict
from gensim.corpora.dictionary import Dictionary
from gensim.corpora.mmcorpus import MmCorpus

import redis


# Our Redis database
db = redis.StrictRedis(host='localhost', port=6379, db=0)

# Maps words to ids, keeps track of global frequency and per-document frequency
globalDict = Dictionary

# Words to exclude from our analysis (selected somewhat arbitrarily from 
# https://en.wikipedia.org/wiki/Most_common_words_in_English)
ignoreList = ["the", "are", "is", "were", "was", "to", "of", "and", "a", "in",
              "that", "have", "had", "has", "I", "it", "for", "not", "on",
              "with", "without", "he", "she", "they", "as", "you", "do", "did",
              "does", "at", "this", "but", "his", "her", "hers", "their", "by",
              "from", "we", "or", "an", "will", "my", "your", "all", "there",
              "what", "so", "up", "down", "out", "if", "about", "no", "yes",
              "just", "him", "its", "also", "any", ]
ignoreSet = set(ignoreList)

# Characters to remove from our strings
stripChars = ['\'', '"', '\\', '?', '!', '.', ',', '#', '$', '%', '^', '&', '*',
              '(', ')', '-', '+', '=', '[', ']', '{', '}', '/', '~', '`',]

# Differentiate words that appear in titles so that we can weight them more
# strongly
titleMarker = '#'

def cleanAndSplitString(s, isTitle=False):
    """ Sends s to lowercase, strips off all special characters, splits, adds a
    marker token for titles. """

    s = s.lower()
    for char in stripChars:
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
    cleanList = cleanAndSplitString(content)
    db.hset(contentid, 'cleancontent', cleanList)

    return cleanContent

def addPostToDict(postid):
    """ Adds the given post to the dictionary. """

    postKey = 'post:%s' % postid
    title = contentFromId(postKey, isTitle=True)
    
    commentKey = 'comments:%s' % postid
    numComments = db.zcard(commentKey)
    commentids = db.zrevrange(commentKey, 0, numComments)

    comments = [contentFromId(commentid) for commentid in commentids]
    # Flatten comments
    postContent = [word for comment in comments for word in comment]
    postContent.extend(title)

    db.hadd(postKey, 'document', postContent)
    globalDict.doc2bow(postContent, allow_update=True)

def buildDictionary(force=False):
    """ Build a dictionary in which each post corresponds to a document. """

    if force or len(globalDict.keys() == 0):
        numPosts = db.zcard('posts')
        postids = db.zrevrange('posts', 0, numPosts)
    
        for postid in postids:
            addPostToDict(postid)

def corpusOfPost(postid, force=False):
    """ Returns the contents of the given post in corpus vector form. """

    postKey = 'post:%s' % postid
    corpus = db.hget(postKey, 'corpus')

    if force or corpus is None:
        content = db.hget(postKey, 'document')
        corpus = globalDict.doc2bow(content)
        db.hadd(postKey, 'corpus', corpus)

    return corpus

def buildCorpus():
    """ Returns a corpus object that contains sparse vectors from every post. """

    numPosts = db.zcard('posts')
    postids = db.zrevrange('posts', 0, numPosts)
    
    # Generator for all corpuses
    corpusGen = (corpusOfPost(postid) for postid in postids)
    return corpusGen

if __name__ == "__main__":
    buildDictionary()
    corpus = buildCorpus()
    MmCorpus.serialize('redditcorpus.mm', corpus)
