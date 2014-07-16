#!/usr/bin/env python

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import os
import json
import time
import math
import pickle
from py2neo import neo4j
from py2neo import *

GRAPHENEDB_URL = os.environ['GRAPHENEDB_URL']

DB = neo4j.GraphDatabaseService( GRAPHENEDB_URL )

# Given a JSON file of four square explore data, push it into the DB
def push_4sqexplore_to_db():
	folder = 'all_jsons/4sq'
	subfolders = list(os.walk(folder))[0][1]
	for subfolder in subfolders:
		files = os.listdir(folder+'/'+subfolder)
		#print files
	
		#folder = 'sample_jsons/4sq'
		#files = os.listdir(folder)
		for file in files:
			try:
				with open(folder+'/'+subfolder+'/'+file) as f:
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
						
						subfolder = 'coffee'
						add_batch( dictionaries, subfolder)
						add_nodes_to_index( 'FourSqrVenues_explore' )
			except: print 'no file'
				

	
def add_batch(dictionaries, subfolder):
	""" Does a batch insert of photo data into the db. """
	batch = neo4j.WriteBatch( DB )	
	count = 0
	for i, dictionary in dictionaries.iteritems():
		users = dictionary['user']
		# get rid of dumbly created dictionary keys, and add the right ones, plus a few new ones
		# we also cannot have non primitive datatypes as properties, so we need to serialize them
		new_dict = {}
		for prop in dictionary:
			if isinstance(dictionary[prop], list) or isinstance(dictionary[prop], dict):
				new_dict[prop] = json.dumps(dictionary[prop])
			elif prop != 'latitude' or prop != 'longitude':
				new_dict[prop] = dictionary[prop]
			new_dict['lat'] = dictionary['latitude']
			new_dict['lon'] = dictionary['longitude']
			new_dict['venue_type'] = subfolder
			count +=1

		
		qs = """
			MERGE (p:FourSqrVenues_explore { category:{category}, rating:{rating}, data_source:{data_source}, name:{name}, venue_type:{venue_type}, twitter:{twitter}, place_id:{place_id}, lon:{lon}, longitude:{longitude}, users_count:{users_count}, hours:{hours}, latitude:{latitude}, user:{user}, time:{time}, lat:{lat}, checkins:{checkins}})
		"""
		batch.append_cypher( qs, new_dict )
		
		for user in users:
			qu = """MERGE (u:FourSqrUsers { user_name:{user}})""" 
			batch.append_cypher(qu, {'user':user})
			re = """MATCH (a:FourSqrUsers {user_name: {user}}), (b:FourSqrVenues_explore {place_id: {place_id}}) MERGE (a)-[r:CHECKED_IN]->(b)""" 
			batch.append_cypher(re, {'user':user,'place_id':str(new_dict['place_id'])})
	r = batch.submit()
	print 'added', count, 'venues'
	
#CREATE (n {id:'something'})=[r:sameid]->(m)
def add_nodes_to_index( label='FourSqrVenues_explore' ):
	""" Adds nodes with the given label to the spatial points index. Developed in context of traffic nodes. """
	points_idx = DB.get_or_create_index( neo4j.Node, 'points_hk', {
		'provider':'spatial',
		'geometry_type': 'point',
		'lat': 'lat',
		'lon': 'lon'
	})
	if points_idx is None: 
		print "Could not find the points_hk index"
		return
	qs = "MATCH (t:%s) RETURN t" % label
	q = neo4j.CypherQuery( DB, qs )
	results = q.execute()
	for i, r in enumerate( results.data ):
		n = r.values[0]
		points_idx.add( 'k', 'v', n )
				
if __name__=='__main__':
	#DB.clear()
	push_4sqexplore_to_db()
	