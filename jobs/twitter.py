import os
import json
import time
import sys

from datetime import datetime
from twython import Twython

sys.path.insert(0, '../')
from s3 import upload_to_s3

APP_KEY = os.environ['TWITTER_API_KEY']
APP_SECRET = os.environ['TWITTER_API_SECRET']


def get_tweets( latlong=None ):
	''' Fetches tweets with a given query at a given lat-long.'''
	twitter = Twython( APP_KEY, APP_SECRET )
	results = twitter.search( geocode=','.join([ str(x) for x in latlong ]) + ',15km', result_type='recent', count=100 )
	return results['statuses']


def get_lots_of_tweets( latlong ):
	""" Does pretty much what its long name suggests. """
	all_tweets = {}
	total_time = 270
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
				all_tweets[ tid ] = properties
				added += 1
		print "At %d seconds, added %d new tweets, for a total of %d" % ( total_time - remaining_seconds, added, len( all_tweets ) )
		time.sleep(interval)
		remaining_seconds -= interval
	return all_tweets

def run():
	t = get_lots_of_tweets( [22.280893, 114.173035] )
	target_path = 'twitter/%stweets.json' %(datetime.now())
	#with open( './data/%stweets.json' %(datetime.now()), 'w' ) as f:
	#	f.write( json.dumps(t))
	upload = upload_to_s3( target_path, json.dumps(t))
