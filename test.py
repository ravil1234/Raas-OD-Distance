import googlemaps

import environment

gmaps = googlemaps.Client(key=environment.gcp_api_key) 
print(gmaps)
def calculateGoogleMapDistance(origin_latitude,origin_longitude,
                                destination_latitude,destination_longitude):
    distance = gmaps.distance_matrix([str(origin_latitude) + " " + str(origin_longitude)], 
                                    [str(destination_latitude) + " " + str(destination_longitude)], 
                                    mode='walking')['rows'][0]['elements'][0]
    print(distance)
    print(distance['distance']['text'])

origin_latitude = 12.9551779
origin_longitude = 77.6910334
destination_latitude = 28.505278
destination_longitude = 77.327774

calculateGoogleMapDistance(origin_latitude,origin_longitude,
                                destination_latitude,destination_longitude)     

