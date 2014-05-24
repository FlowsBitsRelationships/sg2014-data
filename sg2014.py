
import os
import json
from py2neo import neo4j, node, rel
from py2neo.packages.urimagic import URI
from py2neo import cypher
from flask import Flask
#from flask import g
from flask import request

app = Flask(__name__)

GRAPHENEDB_URL = os.environ.get("GRAPHENEDB_URL", "http://localhost:7474/db/data/")
DB = neo4j.GraphDatabaseService( GRAPHENEDB_URL )


@app.route('/')
def hello_world():
	return 'Hello, World! Hello, Neo4j!'


@app.route('/node/<node_type>', methods=['POST', 'GET'])
def create_or_fetch_node( node_type ):
	""" A very general route for creating and fetching Person nodes. """
	if node_type not in ['movie', 'actor', 'location']:
		msg = '{"message": "failure! the only acceptable types are movie, actor, and location","data":[]}'
	elif request.method == 'GET':
		r = DB.find( node_type )
		results = []
		for row in r:
			results.append( row.get_properties() )
		msg = json.dumps({'message':'success', 'data': results })
	elif request.method == 'POST':
		new_things = json.loads( request.form['data'] )		
		q = neo4j.CypherQuery( DB, 'CREATE( :%s { props })' % node_type)
		for properties in new_things:
			q.execute( props=properties )
		msg = '{"message": "success", "data":[]}'
	return msg


@app.route('/rel/<actor_name>/acted_in', methods=['POST'])
def create_or_fetch_rel( actor_name ):
	""" A very general route for creating relationships between nodes. """
	new_rel_props = json.loads( request.form['data'] )
	q = neo4j.CypherQuery( DB, """MATCH( a:actor{name:{actor_name} 	})
								  MATCH( m:movie{name:{movie_name}})
								  CREATE (a)-[:acted_in{rel_props}]->(m)""")
	for all_rel_props in new_rel_props:
		rel_props = all_rel_props['properties']
		movie_name = all_rel_props['movie_name']
		q.execute( actor_name=actor_name, rel_props=rel_props, movie_name=movie_name )
	msg = '{"message": "success", "data": []}'
	return msg


@app.route('/version')
def get_version():
	return str( DB.neo4j_version )


if __name__=="__main__":
	app.debug = True
	
	#g.session = cypher.Session( graphenedb_url )
	#service_root = neo4j.ServiceRoot(URI(graphenedb_url).resolve("/"))
	#g.graph_db = service_root.graph_db
	app.run()
