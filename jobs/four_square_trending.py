import os
import json
import time
from datetime import datetime
import foursquare


client_id = os.environ['FOURSQUARE_ID']
client_secret = os.environ['FOURSQUARE_SECRET']
redirect = os.environ['FOURSQUARE_REDIRECT']

ll = '22.2670,114.1880'
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
            check_id = checkin['id']
            if check_id not in all_checkins:
                properties = {}
                properties['name'] = checkin['name']
                #properties['user'] = r['photo']['owner']['nsid']
                properties['here_now'] = checkin['hereNow']['count']
                properties['checkins'] = checkin['stats']['checkinsCount']
                properties['category'] = checkin['categories'][0]['name']
                properties['latitude'] = checkin['location']['lat']
                properties['longitude'] = checkin['location']['lng']
                properties['time'] = str(datetime.now())
                properties['data_source'] = 'foursquare_trending'
                
                all_checkins[check_id] = properties
                added += 1
        print "At %d seconds, added %d new checkins, for a total of %d" % ( total_time - remaining_seconds, added, len(all_checkins))
        time.sleep(interval)
        remaining_seconds -= interval
    return all_checkins
    
if __name__=='__main__':
    checkins = get_many_checkins(ll, total_time)
    print checkins
    with open( 'data/four_trending.json', 'w' ) as f:
        f.write(json.dumps(checkins))
