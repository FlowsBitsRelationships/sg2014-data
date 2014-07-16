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
	'''folder = 'all_jsons/4sq'
	subfolders = list(os.walk(folder))[0][1]
	for subfolder in subfolders:
		files = os.listdir(folder+'/'+subfolder)
		print files'''
	
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
				
				new_batch = neo4j.WriteBatch(DB)
				
				# get rid of this... hard coded subfolder
				subfolder = 'coffee'
				# Now save the places to the db and give them a relationship to their node type
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
					
					# we create a new place_node
					place_node = new_batch.create(node(new_dict))
					new_batch.add_to_index( neo4j.Node, 'points_hk', 'k', 'v', place_node )
					new_batch.add_labels( place_node, 'FourSqrVenues_explore' )
					for user in users:
						user_node = new_batch.create(node({'username':user}))
						new_batch.add_labels( user_node, 'FourSqrUsers' )
						new_batch.create(rel( user_node, 'CheckedIn', place_node))
				results = new_batch.submit()
				print results
				
if __name__=='__main__':
	#DB.clear()
	push_4sqexplore_to_db()