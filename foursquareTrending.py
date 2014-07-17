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

# call S3 to fetch foursquare trending keys
def get_s3_data():
    conn = S3Connection()
    bucket = conn.get_bucket('sg14fbr')
    
    test_keys = [bucket.get_key('data/foursquare/2014-06-22 05:49:17.485494foursquare_trending.json'),
        		 bucket.get_key('data/foursquare/2014-06-22 05:54:17.543465foursquare_trending.json'),
        		 bucket.get_key('data/foursquare/2014-07-14 06:29:21.646841foursquare_trending.json')]
        		  
    # for key in bucket.list( prefix='data/foursquare' ):
    for key in bucket.list( prefix='data/foursquare'):
        data_str = key.get_contents_as_string()
        timestamp = re.search(r'(\d+)([^a-z]+)', key.key).group()
        if data_str != '':
            yield timestamp, json.loads( data_str )


def foursquare_trending_to_db():
    """ Writes data to neo4j """
    batch = neo4j.WriteBatch(DB)
    
    # add explorer node temp
    # batch.append_cypher( "MERGE (n:FourSqrVenues_explore {id:'4b0588d5f964a52041dc22e3'})" )
            
    #time nodes
    for timestamp, data in get_s3_data():

        # Now save the places to the db and give them a relationship to their node type
        for i, placeDict in data.iteritems():

			# Parse dictionary keys and replace any dicts or lists
            newDict = {}
            for placeDictKey in placeDict:
                
                if placeDictKey == 'raw_source':
                    for value in placeDict[placeDictKey]:
                        if value not in newDict:
                            if isinstance(placeDict[placeDictKey][value], list) or isinstance(placeDict[placeDictKey][value], dict):
                		        newDict[value] = json.dumps(placeDict[placeDictKey][value])
                            else:
                                newDict[value] = placeDict[placeDictKey][value]
                else:
                    if placeDictKey not in newDict:
                    	if isinstance(placeDict[placeDictKey], list) or isinstance(placeDict[placeDictKey], dict):
                    		newDict[placeDictKey] = json.dumps(placeDict[placeDictKey])
                    	else:
                    		newDict[placeDictKey] = placeDict[placeDictKey]
            
            print newDict
            print
            
            # Extract desired keys into new dict
            foursquareExploreDict = {}
            for key, value in newDict.items():
                if key in ['place_id', 'lat', 'lon', 'time', 'category', 'data_source', 'name', 'here_now', 'checkins']:
                    foursquareExploreDict[key] = value
            
            # Query string to create node
            qs = "MERGE (n:FourSqrVenues_trending {"
            count = 0
            for k,v in foursquareExploreDict.iteritems():
                if isinstance(v, str) or isinstance(v, unicode):
                    val = v.encode("ascii", "ignore")
                else: val = str(v)
                if count <= len(foursquareExploreDict)-2:
                    qs += k + ':' + '"' + val + '"' + ', '
                else: qs += k + ':' + '"' + val + '"' 
                count +=1
            qs += '})'

            batch.append_cypher(qs)

            # Query string to match trending nodes to explore nodes
            # String finds nodes and creates relationships
            createString = """
            MATCH a = (n:FourSqrVenues_trending {place_id:{nodeId}}), b = (q:FourSqrVenues_explore {place_id:{nodeId}}) 
            MERGE (n)-[:SAME_ID]->(q) RETURN a, b
			"""
			
            batch.append_cypher(createString, {'nodeId':str(foursquareExploreDict['place_id'])})

            # Match two nodes with the same place id
            # MATCH (n:FourSqrVenues_explore), (p:FourSqrVenues_trending) where n.place_id = p.place_id return n, p limit 100
            
            # create a test explore node with a specific place id
            # create (n:FourSqrVenues_trending {name:'Test Trend Node', checkins:'20176', place_id:'4c54fe6b5b839521789ada31'})
            
            
            try: results = batch.submit()
            except: pass
            # for i in results:
            #     print i#    u'{0}'.format(i)
            #     print
            
            
# Take all the users in the db that have checked into a 4sq explore venue,
# and link those exact same users with the trending venues
def foursquareTrendingLinkUsers():
    batch = neo4j.WriteBatch(DB)
    
    # try and link user data from user->explore to user->trending
    queryString = """
    MATCH (e:FourSqrVenues_explore), (t:FourSqrVenues_trending) where e.place_id = t.place_id 
    MATCH (e)<--(u:FourSqrUsers)
    MERGE (u)-[:CHECKD_IN]->(t) return u, t, e limit 100
	"""
	
    
    batch.append_cypher(queryString)
    results = batch.submit() 
            
if __name__=="__main__":
    #DB.clear()
    print DB
    try: foursquare_trending_to_db()
    except: pass
    #foursquareTrendingLinkUsers()
    # print DB
    
    '''MATCH (source:FlickrPhoto)(destination:Tweets) MATCH p = allShortestPaths(source-->destination) RETURN NODES(p);'''