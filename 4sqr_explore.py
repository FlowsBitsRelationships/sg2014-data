#!/usr/bin/env python

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import os
import json
import time
import math
from py2neo import neo4j
from py2neo import *

GRAPHENEDB_URL = os.environ['GRAPHENEDB_URL']

DB = neo4j.GraphDatabaseService( GRAPHENEDB_URL )

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
				
if __name__=='__main__':
	push_4sqexplore_to_db()