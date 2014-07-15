from boto.s3.connection import S3Connection
from boto.s3.key import Key

import os
import re
import json
import time
import flickrapi
from py2neo import neo4j, node, rel

#import esm
#import esmre 	# using esmre and not just esm to take advantage of regexes

FLICKR_API_KEY = os.environ['FLICKR_API_KEY']
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
	

def add_photos_batch( photos, idx_name ):
	""" Does a batch insert of photo data into the db. """
	batch = neo4j.WriteBatch( DB )
	for i, photo in enumerate( photos ):
		db_photo = {
			'lat': photo['latitude'],
			'lon': photo['longitude'],
			'title': photo['title'],
			'time': photo['time'],
			'user': photo['user'],
			'photo_id': photo['photo_id'],
			'origin': 'flickr'
		}
		n = batch.create( node( db_photo ) )
		batch.add_labels( n, 'FlickrPhoto' )
		batch.add_to_index( neo4j.Node, idx_name, 'k', 'v', n )  
	batch.submit()
	
	
def add_users_batch( users ):
	""" Does a batch insert of user data into the db. """
	batch = neo4j.WriteBatch( DB )
	for user in users:
		u = {
			user_id: user[0],
			location: user[1],
			is_local: is_user_local( user[1] )
		}
		n = batch.create( node( u ) )
		batch.add_labels( n, 'FlickrUser' )
	batch.submit()
	
	
def add_photo_user_rels():
	""" Adds relationships between photos and the users who took them. """
	query_string = """
		MATCH (u:FlickrUser), (p:FlickrPhoto) WHERE p.user = u.user_id MERGE (u)-[a:Authored]->(p)
	"""
	q = neo4j.CypherQuery( DB, query_string )
	results = q.execute()
	

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
		add_photos_batch( [ p for i,p in photo_list.iteritems() ], 'points_hk'  )
		for photo_id, photo in photo_list.iteritems():
			users.add( ( photo['user'], photo['user_location'] ) )
	for user, location in users:
		add_user_node( user, location )
	add_photo_user_rels()
	

def photo_tags_to_neo( photo_id, tag_list ):
	"""For a given photo, add its tags (and relationships to them) to the database """
	for tag in tag_list:
		query_string = """
			MATCH (p:FlickrPhoto { photo_id:{photo_id}})
			MERGE (tag:FlickrTag { text:{tag_text}, tag_id:{tag_id}})
			MERGE (p)-[:Tagged]->(tag)
		"""
		q = neo4j.CypherQuery( DB, query_string )
		q.execute( photo_id=photo_id, tag_id=tag[0], tag_text=tag[1] )


def photo_tags_to_db():
	flickr = flickrapi.FlickrAPI( FLICKR_API_KEY )
	d = get_flickr_data()[0]
	for photo in [ data for i,data in d.iteritems() ]:
		print "Getting tags for %s" % photo['title']
		t = flickr.tags_getListPhoto( photo_id=photo['photo_id'] )
		tags= []
		for tag in t[0][0]:
			tags.append( ( tag.attrib['id'], tag.text ) )
		photo_tags_to_neo( photo['photo_id'], tags )
	
		
def photo_tags_to_db_batch():
	flickr = flickrapi.FlickrAPI( FLICKR_API_KEY )
	batch = neo4j.WriteBatch( DB )
	d = get_flickr_data()[0]
	count = 0
	for photo in [ data for i,data in d.iteritems() ]:
		print "Getting tags for %s" % photo['title']
		t = flickr.tags_getListPhoto( photo_id=photo['photo_id'] )
		for tag in t[0][0]:
			t_data = {
				'tag_id': tag.attrib['id'],
				'text': tag.text
			}
			t = node(t_data)
			p = node({'photo_id': photo['photo_id'] })
			r = rel( p, 'Tagged', t )
			batch.get_or_create_path( p, r, t )
		count += 1
		if count > 9: break
	batch.submit()


def add_tag_relateds():
	""" Adds relationships between Flickr tags indicating that Flickr considers them to be related. """
	flickr = flickrapi.FlickrAPI( FLICKR_API_KEY )
	limit = 100
	skip = 0
	query_string = """ MATCH (t:FlickrTag) RETURN t SKIP {skip} LIMIT {limit} """
	while True:
		q = neo4j.CypherQuery( DB, query_string )
		results = q.execute( skip=skip, limit=limit )
		results.data[0].values[0]
		
if __name__=='__main__':
	DB.clear()
	print "Cleared the database..."
	flickr_to_neo()
	print "Added users and photos..."
	photo_tags_to_db()
	print "Added photo tags."
	#add_tag_relateds()
	