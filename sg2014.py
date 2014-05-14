
import os
from py2neo import neo4j
from py2neo.packages.urimagic import URI
from py2neo import cypher
from flask import Flask
from flask import g
from flask import request

app = Flask(__name__)


@app.route('/')
def hello_world():
	return 'Hello, World!'


@app.route('/query', methods=['PUT'])
def cypher_query():
	return 'foo'
	q = request.form['query']
	print q
	tx = session.create_transaction()
	tx.append( q )
	results = tx.commit()
	return { 'results': results }


if __name__=="__main__":
	#app.debug = True
	graphenedb_url = os.environ.get("GRAPHENEDB_URL", "http://localhost:7474/")
	g.session = cypher.Session( graphenedb_url )
	#service_root = neo4j.ServiceRoot(URI(graphenedb_url).resolve("/"))
	#graph_db = service_root.graph_db
	app.run()