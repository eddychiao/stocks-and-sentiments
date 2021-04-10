import requests
import numpy as np
import praw
import time
import datetime as dt
import pandas as pd
from psaw import PushshiftAPI
api = PushshiftAPI()

reddit = praw.Reddit(client_id = 'qS3X7m7aOySRIQ',
                     client_secret = 'yEyVgTYwRY7H67YtqgkYRfMZUwDK2A',
                     username = 'vinwaxie',
                     password = 'VincentOttawa123',
                     user_agent = 'scproject')


def get_date(created):
    return dt.datetime.fromtimestamp(created)

def get_time(year, month):
    date_list = []
    for day in range(1,31): #change the number of days for corresponding month, and for all second half collection
    #########################change the range value accordingly.
        start_time = int(dt.datetime(year, month, day).timestamp())
        date_list.append(start_time)
    return date_list

############ title collection, i break the month into two halves
############ just in case the api is unstable, we still obtain half of the data
def get_title_first_half():
    date_list = get_time1(2020, 11) #change the second parameter to corresponding month
    title_list = pd.DataFrame()
    for i, j in zip(range(0, 16), range(1, 17)):
        print("collecting title data for day {}".format(i))
        postlist1 = []
        for submission in api.search_submissions(after=date_list[i], before=date_list[j],
                                    subreddit='WallStreetBets',
                                    filter=['url','author', 'title', 'subreddit'],
                                    limit=10000):
            post = {}
            post['Title'] = str(submission.title)
            post["created"] = submission.created
            postlist1.append(post)
            Postdf1 = pd.DataFrame(postlist1)
            title_list = title_list.append(Postdf1)
    title_list['timestamp'] = title_list['created'].apply(lambda x: get_date(x))
    title_list = title_list.drop_duplicates(subset=['Title'], keep='first')
    del title_list['created']
    title_list.to_csv('./title_first_half.csv', header=True, index=False, columns=list(title_list.axes[1]))
# get_title_first_half()

def get_title_second_half():
    date_list = get_time1(2020, 11) #change the second parameter to corresponding month
    title_list = pd.DataFrame()
    for i, j in zip(range(0, 16), range(1, 17)):
        print("collecting title data for day {}".format(i))
        postlist1 = []
        for submission in api.search_submissions(after=date_list[i], before=date_list[j],
                                    subreddit='WallStreetBets',
                                    filter=['url','author', 'title', 'subreddit'],
                                    limit=10000):
            post = {}
            post['Title'] = str(submission.title)
            post["created"] = submission.created
            postlist1.append(post)
            Postdf1 = pd.DataFrame(postlist1)
            title_list = title_list.append(Postdf1)
    title_list['timestamp'] = title_list['created'].apply(lambda x: get_date(x))
    title_list = title_list.drop_duplicates(subset=['Title'], keep='first')
    del title_list['created']
    title_list.to_csv('./title_second_half.csv', header=True, index=False, columns=list(title_list.axes[1]))
# get_title_second_half()






############ comment collection, i break the month into two halves
############ just in case the api is unstable, we still obtain half of the data
def get_comment_first_half():
    date_list = get_time1(2020, 11) #change the second parameter to corresponding month
    comment_list = pd.DataFrame()
    for i, j in zip(range(0, 16), range(1, 17)):
        print("collecting comment data for day {}".format(i))
        postlist2 = []
        for comment in api.search_comments(after=date_list[i], before=date_list[j], subreddit="WallStreetBets", limit=3000):
            post = {}
            post['Comment'] = comment.body.encode('ascii', 'ignore').decode('ascii')
            post["created"] = comment.created
            postlist2.append(post)
            Postdf2 = pd.DataFrame(postlist2)
            comment_list = comment_list.append(Postdf2)
    comment_list['timestamp'] = comment_list['created'].apply(lambda x: get_date(x))
    comment_list = comment_list.drop_duplicates(subset=['Comment'], keep='first')
    del comment_list['created']
    comment_list.to_csv('./comment.csv', header=True, index=False, columns=list(comment_list.axes[1]))
# get_comment_first_half()

def get_comment_second_half():
    date_list = get_time1(2020, 11) #change the second parameter to corresponding month
    comment_list = pd.DataFrame()
    for i, j in zip(range(0, 16), range(1, 17)):
        print("collecting comment data for day {}".format(i))
        postlist2 = []
        for comment in api.search_comments(after=date_list[i], before=date_list[j], subreddit="WallStreetBets", limit=3000):
            post = {}
            post['Comment'] = comment.body.encode('ascii', 'ignore').decode('ascii')
            post["created"] = comment.created
            postlist2.append(post)
            Postdf2 = pd.DataFrame(postlist2)
            comment_list = comment_list.append(Postdf2)
    comment_list['timestamp'] = comment_list['created'].apply(lambda x: get_date(x))
    comment_list = comment_list.drop_duplicates(subset=['Comment'], keep='first')
    del comment_list['created']
    comment_list.to_csv('./comment.csv', header=True, index=False, columns=list(comment_list.axes[1]))
# get_comment_first_half()

