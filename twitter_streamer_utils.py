from datetime import datetime
from elasticsearch.exceptions import TransportError
import re

terms = ['reserve bank', 'glenn stevens', 'graeme wheeler', 'philip lowe', 'phillip lowe', 'adrian orr',
        'bank of canada', 'poloz', 'boc rate', 'boc inflation', 'boc monetary', 'boc financial',
        'ecb', 'draghi', 'european central bank',
        'bank of england', 'mark carney', 'boe rate', 'boe inflation', 'boe monetary', 'boe financial',
        'fed', 'federal reserve', 'FOMC', 'yellen', 'powell',
        'bank of japan', 'kuroda', 'boj rate', 'boj inflation', 'boj monetary', 'boj financial']

groups = {'aus', 'boc', 'ecb', 'boe', 'fed', 'boj'}
groups['aus'] = ['reserve bank', 'glenn stevens', 'graeme wheeler', 'philip lowe', 'phillip lowe', 'adrian orr']
groups['boc'] = ['bank of canada', 'poloz', 'boc rate', 'boc inflation', 'boc monetary', 'boc financial']
groups['ecb'] = ['ecb', 'draghi', 'european central bank']
groups['boe'] = ['bank of england', 'mark carney', 'boe rate', 'boe inflation', 'boe monetary', 'boe financial']
groups['fed'] = ['fed', 'FOMC', 'yellen', 'powell']
groups['boj'] = ['bank of japan', 'kuroda', 'boj rate', 'boj inflation', 'boj monetary', 'boj financial']

banks = {}
banks['aus'] = [re.compile('^(?=.*reserve)(?=.*bank).*$'), re.compile('^(?=.*glenn)(?=.*stevens).*$'), re.compile('^(?=.*graeme)(?=.*wheeler).*$'), re.compile('^(?=.*phill?ip)(?=.*lowe).*$')]
banks['boc'] = [re.compile('^(?=.*bank)(?=.*of)(?=.*canada).*$'), 'poloz', re.compile('^(?=.*boc)(?=.*(inflation|rate|monetary|financial)).*$')]
banks['ecb'] = ['ecb', 'draghi', re.compile('^(?=.*european)(?=.*central)(?=.*bank).*$')]
banks['boe'] = [re.compile('^(?=.*bank)(?=.*of)(?=.*england).*$'), re.compile('^(?=.*mark)(?=.*carney).*$'), re.compile('^(?=.*boe)(?=.*(inflation|rate|monetary|financial)).*$')]
banks['fed'] = ['fed', 'fomc', 'yellen', 'powell']
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
    if tweet['retweeted_status']:
        if tweet['retweeted_status']['extended_tweet']:
            text += tweet['retweeted_status']['extended_tweet']['full_text'] + '\s'
        else:
            text += tweet['retweeted_status']['text'] + '\s'
    if tweet['is_quote_status']:
        if tweet['quoted_status']['extended_tweet']['full_text']:
            text += tweet['quoted_status']['extended_tweet']['full_text'] + '\s'
        else:
            text += tweet['quoted_status']['text'] + '\s'
    if tweet['extended_tweet']:
        text += tweet['extended_tweet']['full_text'] + '\s'
    else:
        text += tweet['text'] + '\s'
    text = text.lower()

    for bank, keywords in banks.iteritems():
        for pattern in keywords:
            if bool(re.search(pattern, text)):
                categories.append(bank)
                continue

    return categories
