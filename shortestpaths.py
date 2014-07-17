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
    RETURN p, a, b
    limit 100
    """
    
    batch.append_cypher(queryString)
    results = batch.submit() 
    print results
    
    for i in results:
        print i
        print
          
if __name__=="__main__":
    #DB.clear()
    # print DB
    getShortestPaths()