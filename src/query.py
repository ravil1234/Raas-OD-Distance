class Query:
    def __init__(self, dbConnect):
      self.dbConnect = dbConnect
    
    def getCursor(self):
        return self.dbConnect.cursor()
    def showDatabases(self):
        cursor =self.getCursor()
        cursor.execute("SHOW DATABASES")
        for x in cursor:
            print(x)
    def getConsignorGeoLocation(self,orderIds):
        cursor = self.getCursor()
        listGeoLocation=[]
        t = tuple(orderIds)
        cursor.execute("""SELECT src.* , dst.dlat,dst.dlng from 
                        (SELECT 
                         o.order_booking_id, a.latitude slat, a.longitude slng
                        FROM
                        raas_order_metadata.order_detail o
                            LEFT JOIN
                        raas_metadata_backend.consignor_address ca ON ca.id = o.source_consignor_address_id
                            LEFT JOIN
                        raas_metadata_backend.address a ON ca.address_id = a.id where o.order_booking_id in {} )  src left join 
                        (SELECT 
                        o.order_booking_id, a.latitude dlat, a.longitude dlng
                        FROM
                        raas_order_metadata.order_detail o
                            LEFT JOIN
                        raas_metadata_backend.consignor_address ca ON ca.id = o.destination_consignor_address_id
                            LEFT JOIN
                        raas_metadata_backend.address a ON ca.address_id = a.id  where o.order_booking_id in {} ) dst on src.order_booking_id=dst.order_booking_id
                    """
                    .format(t,t)) 
        for x in cursor:
            listGeoLocation.append(
                {
                    "orderBookingId":x[0],
                    "sourceLat":x[1],
                    "sourceLng":x[2],
                    "destinationLat":x[3],
                    "destinationLng":x[4]
                })
        return listGeoLocation
    def getLaneGeoLocation(self,orderIds):
        cursor = self.getCursor()
        listGeoLocation=[]
        t = tuple(orderIds)
        cursor.execute("""SELECT src.* , dst.dlat,dst.dlng from 
            (SELECT 
            o.order_booking_id, n.latitude slat, n.longitude slng
            FROM
            raas_order_metadata.order_detail o
            LEFT JOIN
            raas_metadata_backend.node n ON n.code = o.source_consignor_lane_code
            where o.order_booking_id in {} )  src left join 
            (SELECT 
            o.order_booking_id, n.latitude dlat, n.longitude dlng
            FROM
            raas_order_metadata.order_detail o
            LEFT JOIN
            raas_metadata_backend.node n ON n.code = o.destination_consignor_lane_code
            where o.order_booking_id in {} ) dst on src.order_booking_id=dst.order_booking_id """
            .format(t,t)) 
        for x in cursor:
            listGeoLocation.append(
                {
                    "orderBookingId":x[0],
                    "originNodeLat":x[1],
                    "originNodeLng":x[2],
                    "destinationNodeLat":x[3],
                    "destinationNodeLng":x[4]
                })
        return listGeoLocation       
                
    def getListOfOrderId(self,fromDate, toDate):
        cursor = self.getCursor()
        orderIdList=[]
        cursor.execute("""select order_booking_id 
        from raas_order_metadata.order_detail 
        where created_timestamp between  
        %s and %s
        """,(fromDate,toDate))
        for x in cursor:
            orderIdList.append(x[0])
        return orderIdList    
        # for id in cursor:
    def getODdistance(self,originNodeId,destinationNodeId):
        cursor = self.getCursor()
        cursor.execute("")
    
    def getOrderIdForVehicle(self,orderId):
        cursor = self.getCursor()
        cursor.execute("""
                    select old.order_booking_id from
                    (SELECT vehicle_number vn FROM raas_matching_engine.vehicle_matching 
                    where order_booking_id= %s 
                    AND status = 'ACCEPTED') cur 
                    left join (
                    SELECT order_booking_id , vehicle_number FROM raas_matching_engine.vehicle_matching 
                    where 
                    status = 'ACCEPTED' AND order_booking_id =%s
                    order by created_timestamp desc) old on old.vehicle_number=cur.vn;
        """,(orderId,orderId))
        result = cursor.fetchone()
        if type(result) == type(None):
           return ''
        else:
            return result[0]
    def getDestinationGeoLocation(self,orderId):
        cursor = self.getCursor()
        cursor.execute("""
                SELECT 
                o.order_booking_id, a.latitude lat, a.longitude lng
                FROM
                raas_order_metadata.order_detail o
                LEFT JOIN
                raas_metadata_backend.consignor_address ca ON ca.id = o.destination_consignor_address_id
                LEFT JOIN
                raas_metadata_backend.address a ON ca.address_id = a.id  where o.order_booking_id= %s;
                """,(orderId,))
        result=cursor.fetchone()
        if type(result) == type(None):
           return {}
        else:
            return {"orderBookingId":result[0],
                    "unloadingLat":result[1],
                    "unloadingLng":result[2]}   

    def getODLaneDistance(self,orderIds):
        t = tuple(orderIds)
        listOdLaneDistance=[]
        cursor = self.getCursor()
        cursor.execute("""
                    SELECT 
                    order_node_map.order_booking_id, rd.distance
                    FROM
                    (SELECT 
                        org.*, dst.destination_id
                    FROM
                        (SELECT 
                        o.order_booking_id, node.id origin_id
                    FROM
                        raas_order_metadata.order_detail o
                    LEFT JOIN raas_metadata_backend.node node ON node.code = o.source_consignor_lane_code
                    WHERE
                        o.order_booking_id IN {} ) org
                    LEFT JOIN (SELECT 
                        o.order_booking_id, node.id destination_id
                    FROM
                        raas_order_metadata.order_detail o
                    LEFT JOIN raas_metadata_backend.node node ON node.code = o.destination_consignor_lane_code
                    WHERE
                        o.order_booking_id IN {} ) dst ON dst.order_booking_id = org.order_booking_id) order_node_map
                        LEFT JOIN
                    raas_metadata_backend.route_details rd ON rd.origin_node_id = order_node_map.origin_id
                        AND rd.destination_node_id = order_node_map.destination_id
                    """.format(t,t))
        for x in cursor:
            listOdLaneDistance.append(
                {
                    "orderBookingId":x[0],
                    "distance":x[1],
                })
        return listOdLaneDistance        
        



        







