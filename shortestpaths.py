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


def getShortestPaths():
    batch = neo4j.WriteBatch(DB)
    
    # try and link user data from user->explore to user->trending
    queryString = """
    MATCH (a:FourSqrVenues_explore)<-->(b:FourSqrUsers) 
    MATCH  p = allShortestPaths((a)<-->(b))
    RETURN p
    limit 100
    """
    """
    START n = node:points_hk('bbox:[ 22.3, 115.0, 22.271, 114.18 ]') 
    WHERE (n:FourSquareVenues_explore) 
    RETURN n
    """
    """START n=node:points_hk('withinDistance:[ 22.35, 114.2, 15.0 ]')
    WHERE (n:FourSqrVenues_explore) return n"""
    
    queryString = '''MATCH (u:FourSqrVenues_explore) RETURN u.lat, u.lon''' 
    
    batch.append_cypher(queryString)
    results = batch.submit() 
    for i in results[0]:
        lat, lon = i[0], i[1]
        batch.append_cypher("START n=node:points_hk('withinDistance:[ %s, %s, 15.0 ]') WHERE (n:FourSqrVenues_explore) return n" %(lat, lon))
    results = batch.submit() 
    print results
    '''
    for i in results:
        print i
        print'''
          
if __name__=="__main__":
    #DB.clear()
    # print DB
    getShortestPaths()