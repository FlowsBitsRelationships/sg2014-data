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

DB = neo4j.GraphDatabaseService( GRAPHENEDB_URL )

#
#######################################
		

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
	
	
# Given some JSON data that represents traffic 
def push_traffic_json_to_db(json):
	batch = neo4j.WriteBatch(DB)
	i = 0
	while i < len( json ):
		s = json[i]
		e = json[i+1]
		q = """
			MERGE (start:Traffic:TrafficPoint {lat:{s_lat}, lon:{s_lon}})
			MERGE (end:Traffic:TrafficPoint {lat:{e_lat}, lon:{e_lon}})
			MERGE (start)-[:Traffic {time:{time}, saturation:{sat}, speed:{speed}, road_type:{rt}}]->(end)
		"""
		batch.append_cypher( q, {'s_lat': float( s['lat'] ), 's_lon': float( s['lon'] ), 'e_lat':float(  e['lat'] ), 'e_lon': float( e['lon'] ), 
		                         'time': e['date'], 'sat': e['saturation'], 'speed': e['speed'], 'rt': e['road_type'] })
		i += 2
	nodes = batch.submit()
	
	
def add_nodes_to_index( label='TrafficPoint' ):
	""" Adds nodes with the given label to the spatial points index. Developed in context of traffic nodes. """
	points_idx = DB.get_or_create_index( neo4j.Node, 'points_hk', {
		'provider':'spatial',
		'geometry_type': 'point',
		'lat': 'lat',
		'lon': 'lon'
	})
	qs = "MATCH (t:%s) RETURN t" % label
	q = neo4j.CypherQuery( DB, qs )
	results = q.execute( label=label )
	for i, r in enumerate( results.data ):
		n = r.values[0]
		points_idx.add( 'k', 'v', n )
	

# A function that only needs to be called once
# It reads data from the HK gov JSON file and places it into the DB
def push_hkgov_to_db(path):
	with open(path) as f:
		dictionaries = json.load(f)
			
		# create the neo spatial points index
		idx_name = DB.get_or_create_index( neo4j.Node, 'points_hk', {
			'provider':'spatial',
			'geometry_type': 'point',
			'lat': 'lat',
			'lon': 'lon'
		})
		
		# save the type nodes to the database
		batch = neo4j.WriteBatch(DB)
		hkGovTypes = {}	
		for dictionary in dictionaries:
			hkGovType = str(dictionary['type'])
			if hkGovType not in hkGovTypes:
				newNode = batch.create(node({'type':hkGovType}))
				batch.add_labels( newNode, 'HKGov', 'PlaceType' )		
				hkGovTypes[hkGovType] = newNode
				print "added ", hkGovType
		batch.submit()
		
	
		# Now save the places to the db and give them a relationship to their node type
		for dictionary in dictionaries:
			point = {'lat':float(dictionary['lat']), 'lon':float(dictionary['lon']), 'content':dictionary['content'], 'origin':'hk_gov' }
			placeNode = batch.create(node(point))
			batch.add_to_index( neo4j.Node, 'points_hk', 'k', 'v', placeNode ) 

			batch.add_labels( placeNode, 'Place', 'HKGov' )
			
			hkGovType = str(dictionary['type'])
			if hkGovType in hkGovTypes:
				typeNode = hkGovTypes[hkGovType]
				batch.create(rel(placeNode, "IsPlaceType", typeNode))
		results = batch.submit()

# Given a JSON file of four square explore data, push it into the DB
def push_4sqexplore_to_db():
	folder = 'sample_jsons/4sq'
	files = os.listdir(folder)
	for file in files:
		with open(folder+'/'+file) as f:
			dictionaries = json.load(f)
			if len(dictionaries) == 0: 
				continue
			else: 
				# create the neo spatial points index
				idx_name = DB.get_or_create_index( neo4j.Node, 'points_hk', {
					'provider':'spatial',
					'geometry_type': 'point',
					'lat': 'lat',
					'lon': 'lon'
				})
				
				batch = neo4j.WriteBatch(DB)
		
				# Now save the places to the db and give them a relationship to their node type
				for i, dictionary in dictionaries.iteritems():
					users = dictionary['user']
					place = {'lat':float(dictionary['latitude']), 'lon':float(dictionary['longitude']), 'name':dictionary['name']}
					placeNode = batch.create(node(place))
					batch.add_to_index( neo4j.Node, 'points_hk', 'k', 'v', placeNode )
					batch.add_labels( placeNode, '4SqrVenues' )
					for user in users:
						userNode = batch.create( node({'username':user}) )
						batch.add_labels( userNode, '4SqrUsers' )
						batch.create(rel( userNode, 'CheckedIn', placeNode))
				results = batch.submit()
				#print results


def crawl_s3():
	data_dict = {}
	conn = S3Connection()
	bucket = conn.get_bucket('sg14fbr')
	jsons = [bucket.get_key('data/traffic/2014-07-13 06:58:42.942341traffic.json'),
		bucket.get_key('data/twitter/2014-07-10 18:22:12.990921tweets.json'),
		bucket.get_key('data/foursquare/2014-07-14 06:29:21.646841foursquare_trending.json')]
	count = -1
	for key in bucket.list(prefix='data/traffic'):
		count += 1
		if count % 6 != 0: continue
		print "Processing file %d..." % count
		raw_data = key.get_contents_as_string()
		if raw_data == '': continue
		my_json = json.loads( raw_data )
		if 'hk_gov' in key.key:
			new_json = {}
			for i in my_json:
				new_json[i['content']] = i
			my_json = new_json
		elif 'traffic' in key.key:
			push_traffic_json_to_db(my_json)


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
	points_idx = DB.get_or_create_index( neo4j.Node, 'points_hk', {
			'provider':'spatial',
			'geometry_type': 'point',
			'lat': 'lat',
			'lon': 'lon'
		})
	if points_idx is None:
		print "Could not find or create the points_hk index!"
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
#


if __name__=='__main__':
	#DB.clear()
	#crawl_s3()
	# Afterwards, add the traffic endpoint nodes to the spatial index
	#add_nodes_to_index( 'TrafficPoint' )
	
	#Let's try and load data from individual, one-off JSON files
	push_hkgov_to_db('sample_jsons/hk_gov.json')
	push_4sqexplore_to_db()

