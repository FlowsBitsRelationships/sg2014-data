import os
import json
import time
from twython import Twython


APP_KEY = os.environ['TWITTER_APP_KEY']
APP_SECRET = os.environ['TWITTER_APP_SECRET']


def get_tweets( latlong=None ):
	""" Fetches tweets with a given query at a given lat-long."""
	twitter = Twython( APP_KEY, APP_SECRET )
	results = twitter.search( geocode=','.join([ str(x) for x in latlong ]) + ',8km', result_type='recent', count=25 )
	return results['statuses']


def get_lots_of_tweets( latlong ):
	""" Does pretty much what its long name suggests. """
	all_tweets = {}
	total_time = 1200
	remaining_seconds = total_time
	interval = 30
	max_id = 0
	while remaining_seconds > 0:
		added = 0
		new_tweets = get_tweets( latlong )
		for tweet in new_tweets:
			tid = tweet['id']
			if tid not in all_tweets:
				all_tweets[ tid ] = tweet
				if tid > max_id: max_id = tid
				added += 1
		print "At %d seconds, added %d new tweets, for a total of %d" % ( total_time - remaining_seconds, added, len( all_tweets ) )
		time.sleep(interval)
		remaining_seconds -= interval
	return all_tweets


if __name__=='__main__':
	t = get_lots_of_tweets( [22.280893, 114.173035] )
	with open( './data/tweets_3.json', 'w' ) as f:
		f.write( json.dumps([ v for k,v in t.iteritems() ]))