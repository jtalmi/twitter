import sys
import json
import re
import logging
from datetime import datetime
from collections import defaultdict
import time
import sendgrid
import os
from sendgrid.helpers.mail import *

from elasticsearch import Elasticsearch
from textwrap import TextWrapper
import tweepy

from twitter_streamer_utils import create_twitter_index, categorize_tweet, terms, banks
from credentials import credentials

# Fetch twitter credentials
consumer_key = credentials['consumer_key']
consumer_secret = credentials["consumer_secret"]

access_token = credentials["access_token"]
access_token_secret = credentials["access_token_secret"]

username = credentials['username']
password = credentials['password']

# ES
index_name = 'cb_twitter2'

# Authenticate
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

class StreamListener(tweepy.StreamListener):
	status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

	def on_data(self, data):
		if 'in_reply_to_status' in data:
			self.on_status(data)
		elif 'limit' in data:
			if self.on_limit(json.loads(data)['limit']['track']) is False:
				return False
		elif 'warning' in data:
			warning = json.loads(data)['warnings']
			print warning['message']
			logfile.write("%s - warning. %s\n" % (datetime.now(), warning['message']))
			return False

	def on_status(self, status):
		try:
			json_data = json.loads(status)
			if json_data['truncated']:
				json_data['text_lower'] = json_data['extended_tweet']['full_text'].lower()
			else:
				json_data['text_lower'] = json_data['text'].lower()
			json_data['timestamp'] = datetime.now()
			json_data['bank'] = categorize_tweet(json_data, banks)
			print '%s %s %s' % (json_data['user']['screen_name'], json_data['created_at'], json_data['bank'])

			es.index(index=index_name,
                      doc_type="tweet",
                      body=json_data
                     #ignore=400
			)
		except Exception, e:
			print e
			pass

	def on_limit(self, track):
	sys.stderr.write("\n" + str(datetime.now()) + ": We missed " + str(track) + " tweets" + "\n")
		logfile.write("\n" + str(datetime.now()) + ": We missed " + str(track) + " tweets" + "\n")
		return True

	def on_error(self, status_code):
		sys.stderr.write(str(datetime.now()) + ': Error: ' + str(status_code) + "\n")
		logfile(str(datetime.now()) + ': Error: ' + str(status_code) + "\n")
		return False

	def on_timeout(self):
		sys.stderr.write(str(datetime.now()) + ": Timeout, sleeping for 60 seconds...\n")
		logfile.write(str(datetime.now()) + ': Error: ' + str(status_code) + "\n")
		return TimeoutException

class TimeoutException(Exception):
	pass

def send_mail(sender="donotreply@cb_tweets.com", to="jtalmi@gmail.com", subject="Twitter streaming error", content=""):
	sg = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))
	mail = Mail(sender, subject, to, content)
	response = sg.client.mail.send.post(request_body=mail.get())

if __name__ == '__main__':
    # get trace logger and set level
    #tracer = logging.getLogger('elasticsearch.trace')
    #tracer.setLevel(logging.INFO)
    #tracer.addHandler(logging.FileHandler('/tmp/es_trace.log'))
    es = Elasticsearch(http_auth=(username, password))
    logfile = open('tweet_error_log.txt', 'w+')

    #create_twitter_index(es, index='cb_twitter')

    while True:
		try:
			streamer = tweepy.Stream(auth=auth, listener=StreamListener(), timeout=60)
			streamer.filter(None, terms, languages=['en'])
		except KeyboardInterrupt:
			#User pressed ctrl+c or cmd+c -- get ready to exit the program
			print("%s - KeyboardInterrupt caught. Closing stream and exiting."%datetime.now())
			logfile.write("%s - KeyboardInterrupt caught. Closing stream and exiting."%datetime.now())
			stream.disconnect()
			break
		except TimeoutException:
			#Timeout error, network problems? reconnect.
			print("%s - Timeout exception caught. Closing stream and reopening."%datetime.now())
			logfile.write("%s - Timeout exception caught. Closing stream and reopening."%datetime.now())
			try:
				stream.disconnect()
			except:
				pass
			continue
		except Exception as e:
			#Anything else
			try:
				info = str(e)
				sys.stderr.write("%s - Unexpected exception. %s\n" % (datetime.now(),info))
				logfile.write("%s - Unexpected exception. %s\n" % (datetime.now(),info))
				content = "Unexpected error in Twitter collector. Check server. %s" % info
				subject = "Unexpected error in Twitter collector"
				send_mail(subject=subject, content=content)	
			except:
				pass
			time.sleep(60) #Sleep sixty seconds and resume
