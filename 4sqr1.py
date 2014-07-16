#!/usr/bin/env python

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import os
import json
import time
from py2neo import neo4j
from py2neo import *

import re

GRAPHENEDB_URL = os.environ['GRAPHENEDB_URL']
DB = neo4j.GraphDatabaseService( GRAPHENEDB_URL )


def get_s3_data():
    conn = S3Connection()
    bucket = conn.get_bucket('sg14fbr')
    for key in bucket.list( prefix='data/foursquare' ):
        data_str = key.get_contents_as_string()
        timestamp = re.search(r'(\d+)([^a-z]+)', key.key).group()
        if data_str != '':
            yield timestamp, json.loads( data_str )


def foursquare_to_neo():
    """ Get foursquare data and place it gently in neo4j """

    batch = neo4j.WriteBatch(DB)

    #time nodes
    for timestamp, data in get_s3_data():
        # time_node = create_time_node(timestamp)
        time_node = batch.create(node(time=timestamp))
        batch.add_labels( time_node, 'Time' )

        # place_nodes = [(k, info['raw_source']['name'], timestamp, (info['raw_source']['location']['lat'], info['raw_source']['location']['lng'])) for k, info in data.iteritems():
        place_nodes = []
        type_nodes = []
        street_nodes = []
        
        for k, info in data.iteritems():
            # place nodes
            # place_node = create_place_node(k, info['raw_source']['name'], timestamp, (info['raw_source']['location']['lat'], info['raw_source']['location']['lng']))
            place_nodes.append((k, info['raw_source']['name'], timestamp, info['raw_source']['location']['lat'], info['raw_source']['location']['lng']))
        
            # restaurant type nodes
            #type_node = create_type_node(info['raw_source']['categories'][0]['name'])
            type_nodes.append(info['raw_source']['categories'][0]['name'])
        
        
            if 'address' in info['raw_source']['location']:
                    st =  info['raw_source']['location']['address']
                    try:
                        st_name = re.search(r"([a-z]+\s)+(street|st|road|rd|avenue|ave|av|path|lane|boulevard|blvd)", st, re.IGNORECASE).group()
                    except AttributeError: 
                        pass
                    
                    street_nodes.append(st_name)
                    
        
        for p, t, s in zip(place_nodes, type_nodes, street_nodes):
            # new_place_node = batch.create(node(ident=p[0], name=p[1],timestamp=p[2], lat=p[3], lon=p[4]))
            places = DB.get_or_create_index(neo4j.Node, "places")
            new_place_node = places.create_if_none("place", p[1], {"ident":p[0], "name":p[1], "timestamp":p[2], "lat":p[3], "lon":p[4]})
            if new_place_node is None:
                new_place_node = places.get("place", p[1])[0]
            
            place_types = DB.get_or_create_index(neo4j.Node, "place_types")
            new_type_node = place_types.create_if_none("type", t, {"type": t})
            if new_type_node is None:
                new_type_node = place_types.get("type", t)[0]
            
            street_names = DB.get_or_create_index(neo4j.Node, "street_names")
            new_street_node = street_names.create_if_none("name", s, {"name": s})
            if new_street_node is None:
                new_street_node = place_types.get("type", t)[0]
            
            batch.add_to_index( neo4j.Node, 'points_hk', 'k', 'v', new_place_node )  
            
            batch.add_labels(new_place_node, 'Place')
            batch.add_labels(new_type_node, 'Type')
            batch.add_labels(new_street_node, 'Street')
            
            batch.create(rel(time_node, "TRENDING", new_place_node))
            batch.create(rel(time_node, "TRENDING", new_street_node))
            batch.create(rel(new_place_node, "TYPE", new_type_node))
            batch.create(rel(new_place_node, "LOCATION", new_street_node))
        
          
    nodes = batch.submit()
    # print nodes
	
if __name__=="__main__":
    #DB.clear()
    foursquare_to_neo()