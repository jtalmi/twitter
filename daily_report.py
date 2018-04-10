import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
#import seaborn as sns

from datetime import datetime, timedelta
from dateutil import parser

import sendgrid
from sendgrid.helpers.mail import *

from elasticsearch import Elasticsearch
import elasticsearch_dsl as dsl

from credentials import credentials
from twitter_streamer_utils import banks

def tweets_per_minute_query(client, index='twitter', as_of=(datetime.now() - timedelta(hours=24)), up_to=datetime.now(), bank=None):
	as_of = as_of.strftime("%a %b %d %H:%M:%S +0000 %Y")
	up_to = up_to.strftime("%a %b %d %H:%M:%S +0000 %Y")
	s = dsl.Search(using=client, index=index)
	s = s.filter('range', created_at={'gte': as_of, 'lte': up_to}) 
	if bank:
		s = s.filter("term", bank=bank)
	s.aggs.bucket('date_histogram', 'date_histogram', field='created_at', interval='minute')
	response = s.execute().aggregations.date_histogram.buckets
	response = map(lambda x: {'timestamp': parser.parse(x.key_as_string), 
							  'count': x.doc_count}, response)
	return pd.DataFrame.from_records(response, index='timestamp')

if __name__ == '__main__':
	username = credentials['username']
	password = credentials['password']
	
	client = Elasticsearch(http_auth=(username, password))
	
	for bank in bank.iterkeys():
		df = tweets_per_minute_query(client, 'twitter', bank=bank)
		
