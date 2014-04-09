"""
 " analyzer.py
 " Analyzes le Reddit
"""

import redis

db = redis.StrictRedis(host='localhost', port=6379, db=0)

def readPost(self, postID):
    """ Given a post ID, construct an in-Python representation of that post from
    our database. """

    post = db.hgetall('post:' + postID)
    numComments = db.zcard(post['comments'])
    commentIDs = db.zrange(post['comments'], 0, numComments - 1)
    comments = []

    for commentID in commentIDs:
        comment = db.hgetall(commentID)
        comments.append(comment)

    post['comments'] = comments

    return post
