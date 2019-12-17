'''
Developed with Python 3.8.0
'''
import configparser
import sys
import logging
import time
import os
import json
from datetime import datetime
from azure.eventhub import EventHubClient, Receiver, Offset
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity

logger = logging.getLogger("azure")

CONSUMER_GROUP = "$default"
PARTITION_0, PARTITION_1 = "0", "1"
OFFSET = Offset("-1")

def get_resource_key(resource):
    '''
    Args:
        resource: str
    Returns:
        resource_key: str
    
    Returns the desired resource string from the config.ini file
    '''
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['weatherbit'][resource]

def receive():
    '''
    Connects to the EventHubClient and starts receiving data through the 
    different partitions.
    '''
    event_hub_connection_str = get_resource_key("event_hub_connection_str")

    # We create 2 clients, one for each partition
    client = EventHubClient.from_event_hub_connection_string
    (event_hub_connection_str)
    receiver_0 = client.add_receiver(
        CONSUMER_GROUP, PARTITION_0)
    receiver_1 = client.add_receiver(
        CONSUMER_GROUP, PARTITION_1)
    client.run()    

    total, index = 0, 0
    start_time = time.time()
    
    # These for loops iterate through each partition
    for event_data in receiver_0.receive(timeout=10):
        offset = event_data.offset
        message = str(event_data.message)
        message = message.replace("'", "\"")
        message = message.replace("None", "\"None\"")
        try:
            data = json.loads(message)
            index, date, temp, wind_spd, dni = retrieve_information(data, 
            index)
            print(str(index) + "," + date + "," +str(temp) + "," + 
            str(wind_spd) + "," + str(dni))
            store_data(temp, wind_spd, dni, date)
        except:
            raise
        total += 1

    for event_data in receiver_1.receive(timeout=10):
        offset = event_data.offset
        message = str(event_data.message)
        message = message.replace("'", "\"")
        message = message.replace("None", "\"None\"")
        try:
            data = json.loads(message)
            index, date, temp, wind_spd, dni = retrieve_information(data, 
            index)
            print(str(index) + "," + date + "," +str(temp) + "," + 
            str(wind_spd) + "," + str(dni))
            store_data(temp, wind_spd, dni, date)
        except:
            raise
        total += 1

    end_time = time.time()

    client.stop()
    run_time = end_time - start_time
    print(f"Received {total} messages in {run_time} seconds")

    client.stop()

def retrieve_information(data, index):
    '''
    Args:
        data: json
        index: int
    Returns:
        index: int
        date: str
        avg_temp: float
        avg_wind_spd: float
        avg_dni: float
    
    Processes all the data necessary to be printed out
    '''
    index += 1    
    avg_temp, avg_wind_spd, avg_dni = 0,0,0
    # Get date in format yyyy-mm-dd
    date = str(data["data"][0]["datetime"])[:-3]

    # len(data["data"]) should always be 24
    for i in range(len(data["data"])):
        avg_temp += data["data"][i]["temp"]
        avg_wind_spd += data["data"][i]["wind_spd"]
        avg_dni += data["data"][i]["dni"]

    avg_temp = avg_temp / len(data["data"])
    avg_wind_spd = avg_wind_spd / len(data["data"])
    avg_dni = avg_dni / len(data["data"])

    return index, date, avg_temp, avg_wind_spd, avg_dni

def store_data(temp, wind_spd, dni, date):
    '''
    Args:
        temp: float
        wind_spd: float
        dni: float
        date: str
    
    Connects to the Azure Table and stores:
        Month number (0yy1 for January, 12 for December) as the PartitionKey
        Date of record (yyyyMMdd) as the RowKey
        Timestamp as the timestamp the record was inserted
        Temp
        WindSpd
        Dni
        
    '''
    table_connection_str = get_resource_key("table_connection_str")
    table_service = TableService(table_connection_str)

    row = {'PartitionKey': date[5:7], 'RowKey': date.replace("-",""),
    'Timestamp': datetime.now(), 'Temp': temp, 'Wind_spd': wind_spd, 
    'Dni': dni}
    table_service.insert_entity('weatherstate', row)

if __name__ == "__main__":
    receive()