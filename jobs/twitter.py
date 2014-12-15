import os
import json
import time
import sys

from datetime import datetime
from twython import Twython


APP_KEY = '5g8MCsu7a2e74JRORQ22G76uy'
APP_SECRET = 'qcwgzUGzgELMFRHQI1tZqsZtvkTTbQxp7KUnjQnR3WjKMin0Ff'

def get_tweets( latlong=None ):
	''' Fetches tweets with a given query at a given lat-long.'''
	twitter = Twython( APP_KEY, APP_SECRET )
	results = twitter.search( geocode=','.join([ str(x) for x in latlong ]) + ',15km', result_type='recent', count=100 )
	return results['statuses']


def get_lots_of_tweets( latlong ):
	""" Does pretty much what its long name suggests. """
	all_tweets = {}
	total_time = 300
	remaining_seconds = total_time
	interval = 30 
	while remaining_seconds > 0:
		added = 0
		new_tweets = get_tweets( latlong )
		for tweet in new_tweets:
			tid = tweet['id']
			if tid not in all_tweets and tweet['coordinates'] != None:
				properties = {}
				properties['lat'] = tweet['coordinates']['coordinates'][0]
				properties['lon'] = tweet['coordinates']['coordinates'][1]
				properties['tweet_id'] = tid
				properties['content'] = tweet['text']
				properties['user'] = tweet['user']['id']
				properties['user_location'] = tweet['user']['location']
				properties['raw_source'] = tweet
				properties['data_point'] = 'none'
				properties['time'] = tweet['created_at']
				all_tweets[ tid ] = properties
				added += 1
		print "At %d seconds, added %d new tweets, for a total of %d" % ( total_time - remaining_seconds, added, len( all_tweets ) )
		time.sleep(interval)
		remaining_seconds -= interval
	return all_tweets


def run():
	t = get_lots_of_tweets( [22.280893, 114.173035] )
	target_path = 'twitter/%stweets.json' %(str(datetime.now()))
	with open( './data/%stweets.json' %(datetime.now()), 'w' ) as f:
		f.write( json.dumps(t))

for i in range(500000000):
    run()

