#!/usr/bin/env python

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import os
import json
import time
from py2neo import neo4j

#import esm
#import esmre 	# using esmre and not just esm to take advantage of regexes


#GRAPHENEDB_URL = os.environ.get("GRAPHENEDB_URL_3", "http://localhost:7474/db/data/")
GRAPHENEDB_URL = os.environ['GRAPHENEDB_URL']
DB = neo4j.GraphDatabaseService( GRAPHENEDB_URL )

def get_flickr_data():
	with open ('sample_json/2831flickr_search.json','r') as flickFile:
		return [ json.loads( flickFile.read() ) ]
		

def is_user_local( location ):
	""" Based on a Flickr user location string, determines whether or not
		they are local to Hong Kong. """
	if location == '':
		return 'Unknown'
	elif re.search('(hong\s*kong|hk)', location.lower() ) is not None:
		return 'True'
	else:
		return 'False'
		
		
def add_user_node( user_id, user_location ):
	""" Adds a user node to the db """
	is_local = is_user_local( user_location )
	query_string = """
		MERGE (user:FlickrUser { user_id: {user_id}, location: {user_location}, is_local:{is_local} })
	"""
	q = neo4j.CypherQuery( DB, query_string )
	q.execute( user_id=user_id, user_location=user_location, is_local=is_local )
	

def add_photo_to_neo( photo, points_idx ):
	""" Adds one individual flickr photo to the db, creating any and all other
		necessary nodes and relationships as needed. """
	query_string = """
		MERGE (photo:FlickrPhoto { 
			lat:{p}.latitude, 
			lon:{p}.longitude, 
			title: {p}.title,
			time: {p}.time,
			origin: "flickr" })
		RETURN photo
	"""
	q = neo4j.CypherQuery( DB, query_string )
	results = q.execute( p=photo )
	photo_node = results.data[0].values[0]
	points_idx.add( 'k', 'v', photo_node )
	

def flickr_to_neo():
	""" Puts flickr data into the neo4j db """
	users = set()
	points_idx = DB.get_or_create_index( neo4j.Node, 'points_hk', {
			'provider':'spatial',
			'geometry_type': 'point',
			'lat': 'lat',
			'lon': 'lon'
	})
	for photo_list in get_flickr_data():
		for photo_id, photo in photo_list.iteritems():
			users.add( ( photo['user'], photo['user_location'] ) )
			add_photo_to_neo( photo, points_idx )
	for user, location in users:
		add_user_to_db( user, location )
		

if __name__=='__main__':
	flickr_to_neo()
