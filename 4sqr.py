#!/usr/bin/env python

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import os
import json
import time
from py2neo import neo4j


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
        if data_str != '':
            yield json.loads( data_str )
    

def create_trending_node( trending_data ):
    """ Creates a node to represent a trending time snapshot. """
    query_string = """ MERGE (:Trending { time:{time} }) """
    q = neo4j.CypherQuery( DB, query_string )
    results = q.execute( time=time )
    

def foursquare_to_neo():
    """ Get foursquare data and stick it in neo4j """
    for data in get_s3_data():
        create_trending_node( data )
        #for k, place in data.iteritems():
            
        


if __name__=="__main__":
    foursquare_to_neo()