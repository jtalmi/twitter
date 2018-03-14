from datetime import datetime
from elasticsearch.exceptions import TransportError
import re

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

def create_twitter_index(client, index='twitter'):
    create_index_body = {
      'settings': {
        # just one shard, no replicas for testing
        'number_of_shards': 1,
        'number_of_replicas': 0,
        'mappings': {
            'tweet': {
                "dynamic_templates": [{
                    "created_at_as_datetime": {
                        "match_mapping_type": "*",
                        "match": "*created_at",
                        "mapping": {
                            'type': 'date',
                            'format': "EEE MMM dd HH:mm:ss Z yyyy"
                            }
                        }
                    }
                ]}
            }
        }
    }
    # create empty index
    try:
        client.indices.create(
            index=index,
            body=create_index_body,
        )
    except TransportError as e:
        # ignore already existing index
        if e.error == 'index_already_exists_exception':
            pass
        else:
            raise

def categorize_tweet(tweet, banks):
    """
    Twitter searches for keywords in the tweet text as well as the body of the tweet
    that is being retweeted or quoted. Tweets may or may not have those fields depending
    on their type. This function agglomerates text from all available places and then
    searches it.
    """
    categories = []
    text = ""

    if 'retweeted_status' in tweet.keys():
        try:
            text += tweet['retweeted_status']['extended_tweet']['full_text'].lower()
        except:
            text += tweet['retweeted_status']['text'].lower()
    if 'quoted_status' in tweet.keys():
        try:
            text += tweet['quoted_status']['extended_tweet']['full_text'].lower()
        except:
            text += tweet['quoted_status']['text'].lower()
    try:
        text += tweet['extended_tweet']['full_text'].lower()
    except:
        text += tweet['text_lower']

    for bank, keywords in banks.iteritems():
        for pattern in keywords:
            if bool(re.search(pattern, text)):
                categories.append(bank)
    return categories
