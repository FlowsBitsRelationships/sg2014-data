#!/usr/bin/env python

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import os
import json
import time
from py2neo import neo4j

#import esm
#import esmre 	# using esmre and not just esm to take advantage of regexes


#GRAPHENEDB_URL = os.environ.get("GRAPHENEDB_URL", "http://localhost:7474/db/data/")
GRAPHENEDB_URL = os.environ['GRAPHENEDB_URL']
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
	
	
def push_to_db( tid, all_data, points_idx ):# tweet instead of all_data
	
	data_props = { k: v for k,v in all_data.iteritems() if k != 'raw_source' }
	
	if 'source' in data_props:  
		source = data_props['source']
	elif 'data_source' in data_props:
		source = data_props['data_source']
	elif 'tweet_id' in data_props:
		source = 'Twitter'
	#print data_props, source
	if source == 'Twitter':
		# twitter.py accidentally is swapping lattitude and longitude, swap it back here
		data_props['lat'], data_props['lon'] = data_props['lon'], data_props['lat']
		print data_props
		if 'raw_source' in data_props:
			data_props['in_reply_to_user_id_str'] = data_props['raw_source']['in_reply_to_user_id_str']
			data_props['in_reply_to_status_id_str'] = data_props['raw_source']['in_reply_to_status_id_str']
			data_props['time'] = data_props['raw_source']['created_at']
			raw_user = data_props['raw_source']['user']
			user_props = {
				'username': raw_user['screen_name'],
				'followers_count': raw_user['followers_count'],
				'id_str': raw_user['id_str'],
				'location': raw_user['location'],
				'lang': raw_user['lang'],
				'name': raw_user['name'],
				'description': raw_user['description']
			}
		else:
			user_props = {}

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
		q = neo4j.CypherQuery( DB, query_string )
		if not user_props:
			user_props = {}
		results = q.execute( tp=data_props, up=user_props, origin='twitter', tweet=tid )
			
		try:
			print results.data[0].values[0]
		except: pass
		#new_node = results.data[0].values[0]
		#points_idx.add('k', 'v', new_node )


def push_all_to_db( stuff, point_idx ):
	count = 0
	for tid, data in stuff.iteritems():
		push_to_db( tid, data, point_idx )
		count += 1
	print "Added %d data to the db." % count


def push_data_to_db():
	points_3 = DB.get_or_create_index( neo4j.Node, 'points_hk', {
			'provider':'spatial',
			'geometry_type': 'point',
			'lat': 'lat',
			'lon': 'lon'
		})
	all_data = crawl_s3()
	push_all_to_db( all_data, points_3 )
	#add_places()


def crawl_s3():
	# with fourquare it returns a dictionary of dictionaries
	# with twitter, it returns a dictionary of dictionaries
	# with traffic, it returns a list of dictionaries
	# with hk_gov, it returns a list of dictionaries
	
	data_dict = {}
	conn = S3Connection()
	bucket = conn.get_bucket('sg14fbr')
	test = [bucket.get_key('data/traffic/2014-07-13 06:58:42.942341traffic.json'),
		bucket.get_key('data/twitter/2014-07-10 18:22:12.990921tweets.json'),
		bucket.get_key('data/foursquare/2014-07-14 06:29:21.646841foursquare_trending.json')]
	for key in test:#list(bucket.list(prefix='data/twitter')):
		raw_data = key.get_contents_as_string()
		if raw_data == '': continue
		data = json.loads( raw_data )

		if 'hk_gov' in key.key:
			new_data = {}
			for i in data:
				new_data[i['content']] = i
			data = new_data
						
		elif 'traffic' in key.key:
			new_data = {}
			for i in data:
				new_data[i['date']] = i
			data = new_data
			
		data_dict = dict( data_dict, **data )
	return data_dict


def add_place_to_db( place, points_idx ):
	place_type = place['tags']['place'] if 'place' in place['tags'] else place['tags']['shop'] if 'shop' in place['tags'] else 'undefined'
	if place_type == 'undefined':
		print "undefined --> %s" % jsom.dumps( place['tags'] )
		return
	query_string = """ 
		MERGE ( place:Place:OSM:""" + place_type.replace(' ', '_').capitalize() + """ {
			lat: {p}.lat,
			lon: {p}.lon,
			name: {p}.name,
			osmid: {p}.osmid,
			raw_tags: {p}.raw_tags
		})
		RETURN place
	"""
	place_props = { k if k != 'tags' else 'raw_tags': v if k != 'tags' and v is not None and v != '' else json.dumps(v) for k,v in place.iteritems() }
	q = neo4j.CypherQuery( DB, query_string )
	results = q.execute( p=place_props )
	place_node = results.data[0].values[0]
	points_idx.add( 'k', 'v', place_node )


def add_places():
	with open('hk_places.json') as f:
		places = json.loads( f.read() )
	points_idx = DB.get_index( neo4j.Node, 'points_hk' )
	if points_idx is None:
		print "Could not find the points_hk index!"
		return
	for i, place in enumerate( places ):
		add_place_to_db( place, points_idx )
	


def get_name_index( places=None ):
	ignorables = ['on', 'ok', 'css', 'hong kong', 'hongkong', 'welcome', 'international']  # Maybe add 'central' to this list, but it's mostly used for the place
	regex_probs = ['|', '\\', '?', '*', '$', '^', '.', '+', '(', ')', '[', ']']
	name_set = set()
	index = esm.Index()
	if places is None:
		with open('hk_places.json') as f:
			places = [ place['name'] for place in json.loads( f.read() )]
	for i, place in enumerate( places ):
		name = place.encode('ascii', 'ignore').strip() #''.join([ c for c in place if ord(c) < 128 ]).strip()
		if name == '':
			continue
		name_set.add( name.lower() )	# convert everything to lowercase before matching
	for n in name_set:
		if n in ignorables: continue 	# Currently just a few problem cases. Some are leftovers from cutting out >128 code points.
		index.enter(n)
	index.fix()
	return index


def get_node_attr_by_label( label, attr ):
	all_nodes = []
	total = 0
	limit = 100
	while True:
		qs = "MATCH (n:%s) RETURN id(n), n.%s SKIP %d LIMIT %d" % ( label, attr, total, limit )
		q = neo4j.CypherQuery( DB, qs )
		results = q.execute()
		if len( results.data ) < 1:
			break
		else:
			all_nodes.extend([ d.values for d in results.data ])
			total += limit
	return all_nodes



def add_places_relationships():
	# Iterates over all the tweets and other text data and
	# uses Aho Corasick to check for matches against all 1100 or so places at once.
	# If a match is found it adds the appropriate relationship to the graph.
	all_matches = []
	place_nodes = get_node_attr_by_label( 'OSM', 'name' )
	place_ids = { p[1].lower(): p[0] for p in place_nodes  }
	name_index = get_name_index([ p[1] for p in place_nodes ])
	tweet_texts = get_node_attr_by_label( 'Tweets', 'content' )
	for _, tweet in enumerate(tweet_texts):
		tweet_id, text = tweet
		text = text.encode('ascii', 'ignore') 			# TODO Work out how to use esmre to match unicode code points > 128 
		matches = name_index.query( text.lower() )
		for m in matches:
			if m[1] in place_ids:
				all_matches.append(( tweet_id, place_ids[ m[1] ] ))
	for i, match in enumerate( all_matches ):
		tid, pid = match
		qs = """
			MATCH (n) WHERE id(n)={tid}
			MATCH (p) WHERE id(p)={pid}
			MERGE (n)-[m:MENTIONED]->(p)
			"""
		q = neo4j.CypherQuery( DB, qs )
		q.execute( tid=tid, pid=pid )



if __name__=='__main__':
	#DB.clear()
	push_data_to_db()
	#add_places_relationships()


####
####
#### MISC NOT CURRENTLY BEING USED ####
####
####

# TODO Write a blog post comparing Aho Corasick vs naive implementation, pointing out where the trade-off in setup time vs. operation time occurs, etc.

def test_ac( index ):
	text =[ """Here is some text that mentions this Wan Tuk place. I want to go there.  And I also want to go to Tai Wan. Hmm. Pok Tau Ha. Okay what if the
		text is really long?
	""", """ And there are lots of different text strings? Wan Tuk about then? """, """ Okay well, I think we have some idea.  But the thing is. I imagine
	that this function is getting dominated by the setup of the index.  But if we're looking at 3000 tweets that will get overwhelmed by the searching itself.
	 """ ]
	for x in xrange( 20000 ): 
		for t in text:
			matches = index.query( t )
	

def test_naive():
	name_set = set()
	with open('hk_places.json') as f:
		places = json.loads( f.read() )
	for i, place in enumerate( places ):
		name = ''.join([ c for c in place['name'] if ord(c) < 128 ]).strip()
		if name == '':
			continue
		name_set.add( name )
	text = [ """Here is some text that mentions this Wan Tuk place. I want to go there.  And I also want to go to Tai Wan. Hmm. Pok Tau Ha. Okay what if the
		text is really long?
	""", """ And there are lots of different text strings? Wan Tuk about then? """, """ Okay well, I think we have some idea.  But the thing is. I imagine
	that this function is getting dominated by the setup of the index.  But if we're looking at 3000 tweets that will get overwhelmed by the searching itself.
	 """ ]
	matches = 0
	for x in xrange( 20000 ):
		for n in name_set:
			for t in text:
				if n in t: matches += 1



#if __name__=='__main__':
	#push_data_to_db()
	#add_places()
	# ac_start = time.clock()
	# idx = get_name_index()
	# test_ac( idx )
	# ac_end = time.clock() - ac_start
	# print "AC/esmre --> %f" % ac_end
	# naive_start = time.clock()
	# test_naive()
	# naive_end = time.clock() - naive_start
	# print "Naive --> %f" % naive_end


# from neo4jrestclient.client import GraphDatabase

# def exp():
# 	gdb = GraphDatabase('http://localhost:7474/db/data')
# 	points = gdb.nodes.indexes.create('points_2', geometry_type='point', provider='spatial', lat='lat', lon='lon' )
# 	tweets = gdb.labels.create('tweets_nyc')
# 	with open( 'tweets.json' ) as f:
# 		all_tweets = json.loads( f.read() )
# 	for tid, t in all_tweets.iteritems():
# 		db_t = gdb.nodes.create(content=t['content'], lat=t['lat'], lon=t['lon'], user=t['user'], origin='twitter', raw_source='')
# 		tweets.add( db_t )
# 		points.add( 'k', 'v', db_t )

# # NOTE: START n=node:points_2('withinDistance:[-74.004767,40.737288,1000.0]') RETURN n  -- lon,lat not lat,lon!

# def foo():
# 	gdb = GraphDatabase('http://localhost:7474/db/data')
# 	r = gdb.query("START n=node:points_2('withinDistance:[-74.004767,40.737288,1000.0]') RETURN n")
# 	for x in r:
# 		print x[0]['data']['content']
# 		print " --- "





