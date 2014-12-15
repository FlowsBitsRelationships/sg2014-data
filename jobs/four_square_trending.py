import os
import json
import time
import sys
from datetime import datetime
import foursquare



# Set the API keys
client_id = 'W5UZGTZGO2TJELFWQF1IPYWJ2UXX1WEY2FFZBS14QLXOBKS1'#os.environ['FOURSQUARE_ID']
client_secret = 'C2RXMQINBN3ZAOBX3QIOBTYVGXDGWYTPRO5GKNWL0AOC4T12'#os.environ['FOURSQUARE_SECRET']
redirect = 'http://sg20141.sb02.stations.graphenedb.com:24789/browser/'#os.environ['FOURSQUARE_REDIRECT']

# Set some query parameters
ll = '22.2670,114.1880' 
#ll = '40.7127, -74.0059' #NY
total_time = 300 

def get_checkins(ll):
    # Construct the client object
    client = foursquare.Foursquare(client_id=client_id, client_secret=client_secret, redirect_uri=redirect)
    # get the response from the API
    response = client.venues.trending(params={'near': ll, 'limit':'1000', 'radius': '20000'})
    return response

def get_many_checkins(ll, total_time):
    all_checkins = {}
    remaining_seconds = total_time
    interval = 30

    while remaining_seconds > 0:
        added = 0
        t = get_checkins(ll)
        #print t
        new_checkins = t['venues']
        for checkin in new_checkins:
            #print checkin
            check_id = checkin['id']
            if check_id not in all_checkins:
                properties = {}
                
                properties['content'] = checkin['name']
                properties['here_now'] = checkin['hereNow']['count']
                properties['checkins'] = checkin['stats']['checkinsCount']
                properties['category'] = checkin['categories'][0]['name']
                properties['lat'] = checkin['location']['lat']
                properties['lon'] = checkin['location']['lng']
                properties['checkins'] = checkin['stats']['checkinsCount']
                properties['users_count'] = checkin['stats']['usersCount']
                properties['place_id'] = check_id
                properties['data_point'] = 'none'
                properties['raw_source'] = checkin
                properties['time'] = str(datetime.now())
                properties['data_source'] = 'foursquare_trending'
                
                all_checkins[check_id] = properties
                added += 1
        print "At %d seconds, added %d new checkins, for a total of %d" % ( total_time - remaining_seconds, added, len(all_checkins))
        time.sleep(interval)
        remaining_seconds -= interval
    return all_checkins
    
def run():
    checkins = get_many_checkins(ll, total_time)
    target_path = 'foursquare/%sfoursquare_trending.json' %(str(datetime.now()))

    with open( '/foursquare/%sfour_trending.json' %(str(datetime.now())), 'w' ) as f:
        f.write(json.dumps(checkins))

for i in range(500000000):
    run()
