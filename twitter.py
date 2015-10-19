#!/usr/bin/env python

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import os
import json
import time
import math
from py2neo import neo4j
from py2neo import *		# only really needed for WriteBatch

#import esm
#import esmre 	# using esmre and not just esm to take advantage of regexes

########################################
#
# CONSTANTS

GRAPHENEDB_URL = os.environ['GRAPHENEDB_URL']
BUCKET_NAME = os.environ['BUCKET_NAME']

DB = neo4j.GraphDatabaseService( GRAPHENEDB_URL )


def push_tweet_to_db( tid, tweet, points_idx ):

	query_string = """ 
			MERGE (tweet:Social:Tweets { lat: {tp}.lat, lon: {tp}.lon, content: {tp}.content, user: {up}.username, origin:{origin}, raw_source:{tweet} })
			MERGE (user:Users:TwitterUsers {  
				username: {up}.username,
				followers_count: {up}.followers_count,
				id_str: {up}.id_str,
				location: {up}.location,
				lang: {up}.lang,
				name: {up}.name,
				desscription: {up}.description
			})
			MERGE (user)-[r:TWEETED{ time:{tp}.time }]->(tweet)
			RETURN tweet
		"""
	
	tweet_props = { k: v for k,v in tweet.iteritems() if k != 'raw_source' }
	tweet_props['lat'], tweet_props['lon'] = tweet_props['lon'], tweet_props['lat']    # twitter.py accidentally is swapping lattitude and longitude, swap it back here
	tweet_props['in_reply_to_user_id_str'] = tweet['raw_source']['in_reply_to_user_id_str']
	tweet_props['in_reply_to_status_id_str'] = tweet['raw_source']['in_reply_to_status_id_str']
	tweet_props['time'] = tweet['raw_source']['created_at']

	raw_user = tweet['raw_source']['user']
	user_props = {
		'username': raw_user['screen_name'],
		'followers_count': raw_user['followers_count'],
		'id_str': raw_user['id_str'],
		'location': raw_user['location'],
		'lang': raw_user['lang'],
		'name': raw_user['name'],
		'description': raw_user['description']
	}

	q = neo4j.CypherQuery( DB, query_string )
	results = q.execute( tp=tweet_props, up=user_props, origin='twitter', tweet=tid )
	tweet_node = results.data[0].values[0]
	points_idx.add('k', 'v', tweet_node )


def push_all_tweets_to_db( all_tweets, point_idx ):
	tweets_count = 0
	for tid, tweet in all_tweets.iteritems():
		push_tweet_to_db( tid, tweet, point_idx )
		tweets_count += 1
	print "Added %d tweets to the db." % tweets_count 


def all_tweets_s3_to_neo():
	points_3 = DB.get_or_create_index( neo4j.Node, 'points_hk', {
			'provider':'spatial',
			'geometry_type': 'point',
			'lat': 'lat',
			'lon': 'lon'
		})
	conn = S3Connection()
	bucket = conn.get_bucket(BUCKET_NAME)
	for key in bucket.list( 'data/twitter' ):
		raw_data = key.get_contents_as_string()
		if raw_data == '': continue
		print 'Processing tweets from %s...' % key.key
		data = json.loads( raw_data )
		push_all_tweets_to_db( data, points_3 )
		
		
if __name__=="__main__":
    all_tweets_s3_to_neo()
    print 'Finished processing tweets.'