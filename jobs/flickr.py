import time
import json
import flickr_api
from flickr_api.api import flickr

'https://www.flickr.com/services/api/flickr.photos.search.html'

api_key = os.environ['FLICKR_KEY']
api_secret = os.environ['FLICKR_SECRET']

lat = '22.2670'
lon = '114.1880'
radius = '32'
# loop through all the pages...

flickr_api.set_keys(api_key = api_key, api_secret = api_secret)

def get_location(photo_id):
    geo_r = flickr_api.method_call.call_api(method="flickr.photos.geo.getLocation", photo_id=photo_id)
    lat = geo_r['photo']['location']['latitude']
    lon = geo_r['photo']['location']['longitude']
    return lat, lon

def get_info(photo_id):
    r = flickr_api.method_call.call_api(method="flickr.photos.getInfo", photo_id=photo_id)
    properties = {}
    properties['title'] = r['photo']['title']
    properties['user'] = r['photo']['owner']['nsid']
    properties['user_location'] = r['photo']['owner']['location']
    properties['latitude'] = r['photo']['location']['latitude']
    properties['longitude'] = r['photo']['location']['longitude']
    properties['time'] = r['photo']['dates']['taken']
    properties['data_source'] = 'flickr'
    return properties

if __name__=='__main__':
    all_pics = {}
    for i in range(1):
        r = flickr_api.method_call.call_api(method="flickr.photos.search",
                                              lat=lat, lon=lon, radius=radius, per_page='500', page=str(i))
        photo_lst = r['photos']['photo']        
        
        for photo in photo_lst:
            photo_id = photo['id']
            if photo_id not in all_pics:
                properties = get_info(photo_id)
                all_pics[photo_id] = properties#get_info(photo_id)
                
    print all_pics
    print len(all_pics)
    with open( 'data/flickr_search.json', 'w' ) as f:
        f.write(json.dumps(all_pics))

