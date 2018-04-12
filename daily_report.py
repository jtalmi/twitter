import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("dark")

from datetime import datetime, timedelta
from dateutil import parser

import sendgrid
from sendgrid.helpers.mail import *

from elasticsearch import Elasticsearch
import elasticsearch_dsl as dsl
import base64

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
	return pd.DataFrame.from_records(response)

def send_email(api_key, sender, subject, recipients, message=str(datetime.now()), attachment_path=None):
	sg = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))
	content = Content('text/plain', message)
	mail = Mail(Email(sender), subject, Email(recipients), content)
	with open(attachment_path,'rb') as f:
		data = f.read()
		f.close()
	encoded = base64.b64encode(data).decode()
	attachment = Attachment()
	attachment.content = encoded
	attachment.type = "image/png"
	attachment.filename = "daily_report.png"
	attachment.disposition = "inline"
	attachment.content_id = "Banner"
	mail.add_attachment(attachment)
	try:
		response = sg.client.mail.send.post(request_body=mail.get())
		print response.status_code
		print response.body
		print response.headers
	except Exception as e:
		print (e.body)  #print (e.read())

if __name__ == '__main__':
	username = credentials['username']
	password = credentials['password']
	
	client = Elasticsearch(http_auth=(username, password))
	api_key = os.environ.get('SENDGRID_API_KEY')
	
	df = tweets_per_minute_query(client, 'twitter')

	for bank in banks.iterkeys():
		print bank
		df[bank] = tweets_per_minute_query(client, 'twitter', bank=bank)['count']
	
	df = df.set_index('timestamp')
	df = df.resample('3T').sum()
	ax = df.plot(subplots=True, sharex=True, figsize=(10,15))
	fig = plt.gcf()
	fig.savefig('/home/jtalmi/twitter/daily_report.png')
	send_email(api_key, 
			"donotreply@cbtweets.com", 
			"Daily twitter report - " + str(datetime.now()), 
			credentials['email'], 
			message="Tweets per minute", 
			attachment_path='/home/jtalmi/twitter/daily_report.png')
