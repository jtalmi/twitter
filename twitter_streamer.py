import sys
import json
import re
import logging
from datetime import datetime

from elasticsearch import Elasticsearch
from textwrap import TextWrapper
import tweepy

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
            json_data['text_lower'] = json_data['extended_tweet']['full_text'].lower()
            json_data['created_at_dt'] = datetime.strptime(json_data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
            json_data['timestamp'] = datetime.now()

            try:
                json_data['bank'] = categorize_tweet(json_data['retweeted_status']['extended_tweet']['full_text'].lower() + '\s' + json_data['text_lower'])
            except:
                try:
                    json_data['bank'] = categorize_tweet(json_data['quoted_status']['text'].lower() + '\s' + json_data['text_lower'])
                except:
                    try:
                        json_data['extended_tweet']['full_text'])
                    except:
                        json_data['bank'] = categorize_tweet(json_data['text_lower'])

            print '%s %s %s' % (json_data['user']['screen_name'], json_data['created_at'], json_data['bank'])

            es.index(index="twitter",
                      doc_type="tweet",
                      body=json_data,
                      ignore=400
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

def categorize_tweet(tweet):
    categories = []
    for bank, keywords in banks.iteritems():
        for pattern in keywords:
            if bool(re.search(pattern, tweet)):
                categories.append(bank)
    return categories

def initialize_streamer(client, auth, terms):
    #create_twitter_index(client, 'twitter')
    streamer = tweepy.Stream(auth=auth, listener=StreamListener(), timeout=3000000000 )
    streamer.filter(None, terms, languages=['en'])

if __name__ == '__main__':
    # get trace logger and set level
    tracer = logging.getLogger('elasticsearch.trace')
    tracer.setLevel(logging.INFO)
    tracer.addHandler(logging.FileHandler('/tmp/es_trace.log'))

    consumer_key="YAYQVfgLhNKhfCBQgOUmWXHMI"
    consumer_secret="xR0VcKGaKSXlaA2EgAiItA6tY2XrxDJ503ucPXhNnRwfLa7bNW"

    access_token="969664006247124992-rx6K00UGpg29O253ExsmftwJpys57yC"
    access_token_secret="jbZx5lrZSF701L7lwPNqTw3OM5ggFsxDtOWq99k8GCdCI"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    terms = ['reserve bank', 'glenn stevens', 'graeme wheeler', 'philip lowe', 'phillip lowe',
                'bank of canada', 'poloz', 'boc rate', 'boc inflation', 'boc monetary', 'boc financial',
                'ecb', 'draghi', 'european central bank',
                'bank of england', 'mark carney', 'boe rate', 'boe inflation', 'boe monetary', 'boe financial',
                'fed', 'federal reserve', 'FOMC', 'yellen', 'powell',
                'bank of japan', 'kuroda', 'boj rate', 'boj inflation', 'boj monetary', 'boj financial']


    banks = {}
    banks['aus'] = [re.compile('^(?=.*reserve)(?=.*bank).*$'), re.compile('^(?=.*glenn)(?=.*stevens).*$'), re.compile('^(?=.*graeme)(?=.*wheeler).*$'), re.compile('^(?=.*philip)(?=.*lowe).*$'), re.compile('^(?=.*phillip)(?=.*lowe).*$')]
    banks['boc'] = [re.compile('^(?=.*bank)(?=.*of)(?=.*canada).*$'), 'poloz', re.compile('^(?=.*boc)(?=.*(inflation|rate|monetary|financial)).*$')]
    banks['ecb'] = ['ecb', 'draghi', re.compile('^(?=.*european)(?=.*central)(?=.*bank).*$')]
    banks['boe'] = [re.compile('^(?=.*bank)(?=.*of)(?=.*england).*$'), re.compile('^(?=.*mark)(?=.*carney).*$'), re.compile('^(?=.*boe)(?=.*(inflation|rate|monetary|financial)).*$')]
    banks['fed'] = ['fed', re.compile('^(?=.*federal)(?=.*reserve).*$'), 'fomc', 'yellen', 'powell']
    banks['boj'] = [re.compile('^(?=.*bank)(?=.*of)(?=.*japan).*$'), 'kuroda', re.compile('^(?=.*boj)(?=.*(inflation|rate|monetary|financial)).*$')]

    es = Elasticsearch()
    initialize_streamer(es, auth, terms)
