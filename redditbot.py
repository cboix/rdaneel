import praw
import threading 
import time
import sys


URL = sys.argv[1]
comment = sys.argv[2]

r = praw.Reddit("generic identifier, not used in our code")

#print("logging in")
r.login("Robot_Daneel_Olivaw", "zz4pepper")
#print("login worked")

submission = r.get_submission(URL)
submission.add_comment(comment)

#r.submit('reddit_api_test', 'I am Bot Daneel, and', text='I am making my second post')
#submission = r.get_submission("http://www.reddit.com/r/reddit_api_test/comments/22ejpb/i_am_bot_daneel/")
