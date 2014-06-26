import urllib2
from xml.dom import minidom
import xml.etree.ElementTree as ET
import csv
import datetime
import json
import sys

sys.path.insert(0, '../')
from s3 import upload_to_s3

response = urllib2.urlopen('http://resource.data.one.gov.hk/td/speedmap.xml')
html = response.read()

csvfile = open('meta.csv', 'rU')
f = csvfile.readlines()
reader = csv.reader(f)

all_coords = {}
for i, row in enumerate(reader):
    if i >0:
        coords = {}
        coords['lat1'] = row[2]
        coords['lon1'] = row[3]
        coords['lat2'] = row[5]
        coords['lon2'] = row[6]
        all_coords[row[0]] = coords

def run():
    root = ET.fromstring(html)
    traffic = [ ]
    for child in root:
        c_coords = all_coords[child[0].text]
        road_t = child[2].text
        saturation = child[3].text
        speed = child[4].text
        date = child[5].text
        pt1 = {'road_type':road_t, 'saturation':saturation, 'speed':speed, 'date':str(datetime.datetime.now()), 'source':'hk_gov_speed',
               'lat':c_coords['lat1'], 'lon':c_coords['lon1']}
        traffic.append(pt1)
        pt2 = {'road_type':road_t, 'saturation':saturation, 'speed':speed, 'date':str(datetime.datetime.now()), 'source':'hk_gov_speed',
               'lat':c_coords['lat2'], 'lon':c_coords['lon2']}
        traffic.append(pt2)
        
    target_path = 'traffic/%straffic.json' %(str(datetime.datetime.now()))
    #with open( './data/%straffic.json' %(datetime.datetime.now()), 'w' ) as f:
    #	f.write( json.dumps(traffic))
    upload = upload_to_s3( target_path, json.dumps(traffic))
    
#run()
