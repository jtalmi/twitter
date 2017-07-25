#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 12:48:12 2017

@author: jonathantalmi
"""

import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from pymongo import MongoClient
import json
import os

start_time = time.time() #grabs the system time
keyword_list = ['fed'] #track list

ckey = '9VqgPIbvqBox9ZBERG9GIAJt9'
consumer_secret = '4I7DOZ1ZduCt9sRiqlRh2A43K12vl3xeH5ZTzUV7dNVKcLUB8V'
access_token_key = '22732408-iyZRs0UCrk7CbYzkYKMvFZRLRO2DV4BdF8UbcAdyW'
access_token_secret = '5kcTpyC8Ko4Ex7RDwNZVuupEK6Ith98BoCo0CTCrl2kcg'
 

class listener(StreamListener):
 
    def __init__(self, start_time, time_limit=60): 
        self.time = start_time
        self.limit = time_limit
 
    def on_data(self, data):
        while (time.time() - self.time) and self.limit:
            try:
                client = MongoClient('localhost', 27017)
                db = client['twitter_db']
                collection = db['twitter_collection']
                tweet = json.loads(data)
                collection.insert_one(tweet)
                return True
            except BaseException, e:
                print 'failed ondata,', str(e)
                time.sleep(5)
                pass
 
        exit()
 
    def on_error(self, status):
        print statuses


auth = OAuthHandler(ckey, consumer_secret) #OAuth object
auth.set_access_token(access_token_key, access_token_secret)

twitterStream = Stream(auth, listener(start_time, time_limit=20)) #initialize Stream object with a time out limit
twitterStream.filter(track=keyword_list, languages=['en'])  #call the filter method to run the Stream Object

'''
tweets_iterator = collection.find()
for tweet in tweets_iterator:
  print tweet['text']
'''
