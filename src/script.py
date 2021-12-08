import json
import sys

sys.path.append('/home/ravil/Rivigo/PythonScript/Raas-OD-Distance/config')
from collections import defaultdict

import DbConnection as config
import environment
import googlemaps

import query

gmaps = googlemaps.Client(key=environment.gcp_api_key) 
 
def mergeTwoListOfDict(l1,l2):
    d = defaultdict(dict)
    for l in (l1, l2):
        for elem in l:
            d[elem['orderBookingId']].update(elem)
    l3 = d.values()
    return l3    

def calculateGoogleMapDistance(origin_latitude,origin_longitude,
                                destination_latitude,destination_longitude):
    distance = gmaps.distance_matrix([str(origin_latitude) + " " + str(origin_longitude)], 
                                    [str(destination_latitude) + " " + str(destination_longitude)], 
                                    mode='walking')['rows'][0]['elements'][0] 
    if distance['status']=='OK':                             
        return distance['distance']['text']
    else:
        print("----------Invalid Distance------")
        return '0 km'    

def calculateDistance(result):
    demandOrderDistance=[]
    for x in result :
        
        dist_source_origin_lane=calculateGoogleMapDistance(
            x['sourceLat'],
            x['sourceLng'],
            x['originNodeLat'],
            x['originNodeLng']
            )    
        dist_destination_destination_lane=calculateGoogleMapDistance(
            x['destinationLat'],
            x['destinationLng'],
            x['destinationNodeLat'],
            x['destinationNodeLng']
            )
        orderId=query.getOrderIdForVehicle(x['orderBookingId'])
        dist_prev_unload_load='0 km'
        if orderId != '':
            print("prevOrderId",orderId)
            previousOrderUnloadingLocation=query.getDestinationGeoLocation(orderId) # k 
            print("prev:",previousOrderUnloadingLocation,bool(previousOrderUnloadingLocation))
            if bool(previousOrderUnloadingLocation):
                print("prev:",previousOrderUnloadingLocation)
                dist_prev_unload_load=calculateGoogleMapDistance(
                previousOrderUnloadingLocation['unloadingLat'],
                previousOrderUnloadingLocation['unloadingLng'],
                x['sourceLat'],
                x['sourceLng']
                )   
        distanceObject=    {
                "orderBookingId":x['orderBookingId'],
                "KX":dist_prev_unload_load,
                "XO":dist_source_origin_lane,
                "OD":str(x['distance']) +' km',
                "DY":dist_destination_destination_lane
            }
        print(distanceObject)    
        demandOrderDistance.append(distanceObject)  
    return  demandOrderDistance

mysql_db=config.connect()
query=query.Query(mysql_db) 
print("------------DB Connected-------")
orderIds=query.getListOfOrderId('1637964045112' , '1638433904259')
print("------List of order Booking Ids---------\n")
print(orderIds)
print("---------Getting lat long of consignor Address source and destination------")
consignorGeoLocation=query.getConsignorGeoLocation(orderIds)
laneGeoLocation=query.getLaneGeoLocation(orderIds)
listOdLaneDistance=query.getODLaneDistance(orderIds)
result=mergeTwoListOfDict(consignorGeoLocation,laneGeoLocation)
result=mergeTwoListOfDict(result,listOdLaneDistance)
output=calculateDistance(result)
print("---------Calculating distance for orders list ------")
print(output)
print("Saving list to json file ....................................")
with open('/home/ravil/Rivigo/PythonScript/Raas-OD-Distance/output.json', 'w') as fout:
    json.dump(output , fout)
print("------------------------Saved----------------------")    




  





