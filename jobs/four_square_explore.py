import os
import json
from datetime import datetime
import foursquare

client_id = os.environ['FOURSQUARE_ID']
client_secret = os.environ['FOURSQUARE_SECRET']
redirect = os.environ['FOURSQUARE_REDIRECT']

query = 'food'
queries = ['food', 'drinks', 'coffee', 'shops', 'arts', 'outdoors', 'sights', 'specials', 'topPicks']

# Construct the client object
client = foursquare.Foursquare(client_id=client_id, client_secret=client_secret, redirect_uri=redirect)

def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step

def get_checkins(ll, query):
    # Assign a search radius
    rad = '15000'
    # get the response from the API
    response = client.venues.explore(params={'ll': ll, 'radius': rad, 'section': query, 'limit': '1000000'})
    return response

def get_venue(venue_id):
    # Get client response
    r = client.venues(check_id)
    # Construct an empty dictionary for the properties
    properties = {}
    # Get the information of the venue
    venue = r['venue']
    # Add keys to the dictionary
    properties['name'] = venue['name']
    properties['user'] = [item['user']['id'] for item in venue['tips']['groups'][0]['items']]
    properties['checkins'] = venue['stats']['checkinsCount']
    properties['users_count'] = venue['stats']['usersCount']
    properties['category'] = venue['categories'][0]['name']
    properties['latitude'] = venue['location']['lat']
    properties['longitude'] = venue['location']['lng']
    try: properties['rating'] = venue['rating']
    except: properties['rating'] =  ''
    try: properties['hours'] = venue['hours']['timeframes']
    except: properties['hours'] = ''
    try: properties['twitter'] = venue['contact']['twitter']
    except: properties['twitter'] = ''
    try: properties['popular'] = venue['popular']['timeframes']
    except: properties['popular'] = ''
    properties['time'] = str(datetime.now())
    properties['data_source'] = 'foursquare_explore'
    
    # return the dictionary of properties
    return properties

sw = '22.1538, 113.8352'
ne = '22.5622, 114.4416'

if __name__=='__main__':
    # Construct a dictionary for all the checkins
    all_checkins = {}

    # since the api only returns up to 50 places at the time,
    # we construct a series of locations, and loop through them to scrape it
    sn_range = list(drange(22.1538, 22.5622, .001)) # 50.5 mts radius
    we_range = list(drange(113.8352, 114.4416, .001)) # 50.5 mts radius
    
    # for every latitude
    for n in sn_range[:40]:
        # for every longitude
        for e in we_range[:40]:
            # construct a location
            ll = str(n) + ', ' + str(e)
            print ll
            
            # Get all the checkins for a given location
            t = get_checkins(ll, query)
            new_checkins = t['groups'][0]['items']
            # for every checkin in the checkins
            for checkin in new_checkins:
                # Get the venue ID
                check_id = checkin['venue']['id']
                # if the id is not in the dictionary, add it
                if check_id not in all_checkins:
                    all_checkins[check_id] = get_venue(check_id)
    print len(all_checkins)
    # write the json to a file
    with open( 'data/four_explore.json', 'w' ) as f:
        f.write(json.dumps(all_checkins))

