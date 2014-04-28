"""
 " parser.py
 " Parses our Redis database into a corpus of the form used by Gensim
"""

from collections import defaultdict

import redis

# Our Redis database
db = redis.StrictRedis(host='localhost', port=6379, db-0)

# Every word seen in comments and the number of times we've seen them in comments
commentWordCounts = defaultdict(int)

# Every word seen in titles and the number of times we've seen them in titles
titleWordCounts = defaultdict(int)

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

def cleanAndSplitString(s):
    """ Sends s to lowercase, strips off all special characters, and splits. """

    s = s.lower()
    for char in stripChars:
        s = s.replace(char, '')

    words = s.split()
    filtered = filter(lambda w: w not in ignoreSet, words)

    return filtered

def addToCounts(words, isTitle=False):
    """ Adds each word in words to the appropriate wordCounts dictionary. """

    if isTitle:
        for word in words:
            titleWordCounts[word] += 1
    else:
        for word in words:
            commentWordCounts[word] += 1

def countContent(contentid, isTitle=False):
    """ Cleans and counts the words in the given post title or comment, stores
    the cleaned content as a field in the post/comment. """

    keyword = 'name' if isTitle else 'content'
    content = db.hget(contentid, keyword)
    cleanList = cleanAndSplitString(content)
    addToCounts(cleanList, isTitle)
    
    cleanContent = ' '.join(cleanList)
    db.hset(contentid, 'cleancontent', cleanContent)

def buildWordCounts():
    """ Count all words from titles and comments. """

    numPosts = db.zcard('posts')
    postids = db.zrevrange('posts', 0, numPosts)
    
    for postid in postids:
        postContentId = 'post:%s' % postid
        countContent(postContentId, isTitle=True)

        commentStr = 'comments:%s' % postid
        numComments = db.zcard(commentStr)
        commentids = db.zrevrange(commentStr, 0, numComments)

        for commentid in commentids:
            countContent(commentid)
