#!/usr/bin/env python


from boto.s3.connection import S3Connection
from boto.s3.key import Key


def upload_to_s3( target_path, data_string ):
	""" 
	Uploads a string to S3, located in the sg14fbr bucket at the given path. 
	All of the data is put inside the 'data' folder on S3.  For example:
	target_path='twitter/06-25-14-300.json', data_string='{"some":"big","json":"object"}'
	This would place the json string into a file on S3 located at data/twitter/06-25-14-300.json
	"""
	conn = S3Connection()
	bucket = conn.get_bucket('sg14fbr')
	k = Key( bucket )
	k.key = 'data/' + target_path
	k.set_contents_from_string( data_string )



