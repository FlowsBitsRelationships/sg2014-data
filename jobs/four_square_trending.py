import os
import json
import time
import sys
from datetime import datetime
import foursquare

sys.path.insert(0, '../')
from s3 import upload_to_s3

# Set the API keys
client_id = os.environ['FOURSQUARE_ID']
client_secret = os.environ['FOURSQUARE_SECRET']
redirect = os.environ['FOURSQUARE_REDIRECT']

# Set some query parameters
ll = '22.2670,114.1880' 
#ll = '40.7127, -74.0059' #NY
total_time = 30 

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

    upload = upload_to_s3( target_path, json.dumps(checkins))
    #with open( '/Volumes/XP/Documents and Settings/Carlos Emilio/My Documents/sg2014/foursquare/%sfour_trending.json' %(str(datetime.now())), 'w' ) as f:
    #    f.write(json.dumps(checkins))

#run()
