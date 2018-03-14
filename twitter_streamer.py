import sys
import json
import re
import logging
from datetime import datetime

from elasticsearch import Elasticsearch
from textwrap import TextWrapper
import tweepy

from twitter_index_setup import categorize_tweet, terms, banks

consumer_key="YAYQVfgLhNKhfCBQgOUmWXHMI"
consumer_secret="xR0VcKGaKSXlaA2EgAiItA6tY2XrxDJ503ucPXhNnRwfLa7bNW"

access_token="969664006247124992-rx6K00UGpg29O253ExsmftwJpys57yC"
access_token_secret="jbZx5lrZSF701L7lwPNqTw3OM5ggFsxDtOWq99k8GCdCI"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

class StreamListener(tweepy.StreamListener):
    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

    def on_data(self, data):
        if  'in_reply_to_status' in data:
            self.on_status(data)
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print warning['message']
            return False

    def on_status(self, status):
        try:
            json_data = json.loads(status)
            json_data['text_lower'] = json_data['text'].lower()
            json_data['timestamp'] = datetime.now()
            json_data['bank'] = categorize_tweet(json_data, banks)

            print '%s %s %s' % (json_data['user']['screen_name'], json_data['created_at'], json_data['bank'])

            es.index(index="twitter",
                      doc_type="tweet",
                      body=json_data
                     #ignore=400
			)
        except Exception, e:
            print e
            pass

    def on_limit(self, track):
        sys.stderr.write("\n" + str(datetime.datetime.now()) + ": We missed " + str(track) + " tweets" + "\n")
        return True

    def on_error(self, status_code):
        sys.stderr.write(str(datetime.datetime.now()) + ': Error: ' + str(status_code) + "\n")
        return False

    def on_timeout(self):
        sys.stderr.write(str(datetime.datetime.now()) + ": Timeout, sleeping for 60 seconds...\n")
        time.sleep( 60 )
        return False

def initialize_streamer(client, auth, terms):
    streamer = tweepy.Stream(auth=auth, listener=StreamListener(), timeout=3000000000 )
    streamer.filter(None, terms, languages=['en'])

if __name__ == '__main__':
    # get trace logger and set level
    tracer = logging.getLogger('elasticsearch.trace')
    tracer.setLevel(logging.INFO)
    tracer.addHandler(logging.FileHandler('/tmp/es_trace.log'))

    es = Elasticsearch()
    initialize_streamer(es, auth, terms)
