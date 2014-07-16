#!/usr/bin/env python

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import os
import json
import time
from py2neo import neo4j

import re

GRAPHENEDB_URL = os.environ['GRAPHENEDB_URL']
DB = neo4j.GraphDatabaseService( GRAPHENEDB_URL )


def get_s3_data():
    conn = S3Connection()
    bucket = conn.get_bucket('sg14fbr')
    test_keys = [bucket.get_key('data/foursquare/2014-06-22 05:49:17.485494foursquare_trending.json'),
		        bucket.get_key('data/foursquare/2014-06-22 05:54:17.543465foursquare_trending.json'),
		        bucket.get_key('data/foursquare/2014-07-14 06:29:21.646841foursquare_trending.json')]
    for key in test_keys:
        data_str = key.get_contents_as_string()
        timestamp = re.search(r'(\d+)([^a-z]+)', key.key).group()
        if data_str != '':
            yield timestamp, json.loads( data_str )
    

def create_time_node(timestamp):
    """ Creates a node to represent a trending time snapshot. """
    query_string = """ MERGE (:Time { time:{time} }) """
    q = neo4j.CypherQuery( DB, query_string )
    results = q.execute( time=timestamp)
    print results
    
def create_restaurant_node(name_str, trending_at, location_str,):
    """ Creates a node to represent a trending time snapshot. """
    query_string = """ MERGE (:Restaurant { name:{name}, time:{time}, location:{location} }) """
    q = neo4j.CypherQuery( DB, query_string )
    results = q.execute(name=name_str, time=trending_at, location=location_str)   

def foursquare_to_neo():
    """ Get foursquare data and stick it in neo4j """
    for timestamp, data in get_s3_data():
        create_time_node(timestamp)
"""
        for k, place in data.iteritems():
            create_restaurant_node(place['raw_source']['name'], timestamp, (place['raw_source']['location']['lat'], place['raw_source']['location']['lng']))
"""
        # link nodes... abc = neo4j.Path(alice, "KNOWS", bob, "KNOWS", carol)
        
###111

if __name__=="__main__":
    DB.clear()
    foursquare_to_neo()