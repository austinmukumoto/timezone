from requests.exceptions import Timeout
from datetime import datetime
import requests
import db
import time


class Timezone:

    def __init__(self):
        message = None
        timeout = 30
        key = 'PVR9JSSQ0M6A'
        base_url = 'http://api.timezonedb.com/v2.1'
       # API CALL FOR THE TIME ZONE LIST 
        try:
            resp_timezone_list = requests.post(
                url=f'{base_url}/list-time-zone',
                params={
                    'key':key,
                    'format':'json'
                },
                timeout=timeout
            )
            
            if resp_timezone_list.json().get('status') != 'OK':
                print('Error in pulling timezone list')
                self.error_logging(resp_timezone_list.json().get('message'))
                
                return

            print(f'Timezone http status code response: {resp_timezone_list.status_code}')
            print(f'Timezone list response: {resp_timezone_list.json()}')

            resp_data = resp_timezone_list.json().get('zones')
            
        
            # CONVERT TO TUPLE FOR MULTPLE INSERTING
            datas = []
            zones = []
            for index,sub_value in enumerate(resp_data, start = 0):
                if index == 0:
                    datas.append(list(sub_value.values()))
                    zones.append(list(sub_value.values()))
                else:
                    datas.append(list(sub_value.values())) 
                    zones.append(list(sub_value.values()))   
                
               
                # CONVERT TO DATETIME FROM TIMESTAMPS RESPONSE FROM THE API
                for data_index, data in enumerate(datas):
                    datas[data_index][4] = datetime.fromtimestamp(datas[data_index][4]) if isinstance(datas[data_index][4],int) else datas[data_index][4]
                
                # CONVERT TO DATETIME FROM TIMESTAMPS RESPONSE FROM THE API FOR ZONES SORTING  
                for zone_index, data in enumerate(zones):
                    zones[zone_index][4] = datetime.fromtimestamp(zones[zone_index][4]).strftime('%m/%d/%Y %I:%M %p') if isinstance(zones[zone_index][4],int) else zones[zone_index][4]

                # CONNECTION STARTED FOR SELECTING , DELETING AND INSERT TO THE DATABASE
                with db.get_database() as conn:

                    with conn.cursor() as cursor:
                        
                        # NO.3 TO BE DELETED FOR EVERY PULLING IN API
                        cursor.execute("SELECT * from TZDB_TIMEZONES")
                        timezones = cursor.fetchall()
                
                        if timezones:
                            cursor.execute('TRUNCATE TABLE TZDB_TIMEZONES')

                    # NO.1 PHASE - 1 POPULATE TO THE TZDB_TIMEZONES DATA
                    query = "INSERT INTO `TZDB_TIMEZONES` " \
                            "(`COUNTRYCODE`, `COUNTRYNAME`, `ZONENAME`, `GMTOFFSET`, `IMPORT_DATE`) " \
                            "VALUES (%s, %s, %s, %s, %s)"
                    with conn.cursor() as cursor:
        
                        cursor.executemany(query,datas)
                        conn.commit() 
    
            
            
            # API CALL FOR GET TIME ZONE
            zones = sorted(zones, key=lambda x: datetime.strptime(x[4], '%m/%d/%Y %I:%M %p'))
            
            
            # REMANING TASK THE ZONES MUST BE SORTED BY IMPORT DATE
            
            count = 1
            for zone in zones:
                print(zone)
                with db.get_database() as conn:

                    with conn.cursor() as cursor:
                            
                        # NO.4 CHECKING FOR EXISTING ROW
                        cursor.execute("SELECT * from TZDB_ZONES_DETAILS where ZONENAME = %(ZONENAME)s", {"ZONENAME":str(zone[2])})
                        timezones_stages = cursor.fetchone()
                        
                        if timezones_stages:
                            print('zone already exists!')
                            continue
                        else:
                            # IMPLEMENT DELAY BECAUSE OF THE RATE LIMIT OF THE API
                            time.sleep(3)
                            resp_timezone_get =  requests.post(
                                url=f'{base_url}/get-time-zone',
                                params={
                                    'key':key,
                                    'by':'zone',
                                    'format':'json',
                                    'zone':zone[2]
                                },
                                timeout=timeout
                            )
                            
                            if resp_timezone_get.json().get('status') != 'OK':
                                print('Error in pulling timezone get')
                                self.error_logging(resp_timezone_get.json().get('message'))
                                return
                            
                            print(f'Timezone http status code response: {resp_timezone_get.status_code}')
                            print(f'Timezone list response: {resp_timezone_get.json()}')
                            
                            
                            resp_data = resp_timezone_get.json()

                            # NO.1 PHASE - 2 POPULATE TO THE TZDB_ZONES_DETAILS DATA
                            query = "INSERT INTO `TZDB_ZONES_DETAILS` " \
                                        "(`COUNTRYCODE`, `COUNTRYNAME`, `ZONENAME`, `GMTOFFSET`, `DST`,`ZONESTART`,`ZONEEND`,`IMPORT_DATE`) " \
                                        "VALUES (%s, %s, %s, %s, %s, %s , %s, %s)"
                            
                            #ADDED COUNT FOR INCREMENT IDENTIFIER
                            query2 = "INSERT INTO `TZDB_ZONE_STAGES` " \
                                        "(`ZONENAME`, `COUNT`, `ACTION_DATE`) " \
                                        "VALUES (%s, %s, %s)"
                                        
                            cursor.execute(query,(resp_data.get('countryCode') or '',
                                                            resp_data.get('countryName') or '',
                                                            resp_data.get('zoneName') or '',
                                                            resp_data.get('gmtoffSet') or 0,
                                                            resp_data.get('dst') or 0,
                                                            resp_data.get('zoneStart') or 0,
                                                            resp_data.get('zoneEnd') or 0,
                                                            resp_data.get('formatted') or '0000-00-00 00:00:00'))
                            
                            cursor.execute(query2,(resp_data.get('zoneName'),count,datetime.now()))
                                    
                            conn.commit()  
                            
                            count+=1
                
        except Exception as e:
            # NO.2 FOR REPLICATION OF THIS EXERCISE I SET UP TIMEOUT SETTINGS ON THE API CALL , to 0.1 to Achieved the timeout scenario
            print(e)
            message = e
            if isinstance(e,Timeout):
                print('Timeout Request')
                message = 'Timeout Request'
                # ERROR LOG SAVING
            
                self.error_logging(message)
                    
                       
    def error_logging(self,message):
        with db.get_database() as conn:
            query = "INSERT INTO `TZDB_ERROR_LOG` " \
                            "(`ERROR_DATE`, `ERROR_MESSAGE`) " \
                            "VALUES (%s, %s)"
            with conn.cursor() as cursor:
                cursor.execute(query,
                            (datetime.now(),message))
                conn.commit()          
                        
# EXECUTION 
Timezone()