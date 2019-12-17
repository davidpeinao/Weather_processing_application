'''
Developed with Python 3.8.0
'''
import configparser
import requests
import sys
import logging
import time
import os
from dateutil import rrule
from datetime import datetime
from datetime import timedelta
from azure.eventhub import EventHubClient, Sender, EventData

def get_dates(year):
    '''
    Args:
        year: int
    Returns:
        dates: list of str

    Returns a list containing all the dates of input year in the format 
    yyy-mm-dd, if the input year is the current one the dates will be
    calculated until yesterday.
    '''
    start = '{}-01-01'.format(year)
    end = '{}-12-31'.format(year)

    # Now we check if the input year is the current one,
    # we will assign yesterday's date to end
    if datetime.now().strftime('%Y-%m-%d') < end:
        end = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

    dates = []
    # We iterate daily through the year and append each day to our dates list
    for dt in rrule.rrule(rrule.DAILY, 
                        dtstart=datetime.strptime(start, '%Y-%m-%d'),
                        until=datetime.strptime(end, '%Y-%m-%d')):
        dates.append(dt.strftime('%Y-%m-%d'))
    return dates

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

def get_weather_data(api_url, api_key, lat, lon, dates):
    '''
    Args:
        api_key: str
        lat: str
        lon: str
        dates: list of str
    
    This function calls the API for each day of the year that we have 
    requested.Retrieves the API data and calls calls the send_message 
    function.
    '''
    for i in range(len(dates)-1):
        url = "{}&lat={}&lon={}&\
start_date={}&end_date={}&tz=local&key={}".format(api_url, lat, lon, dates[i],
         dates[i+1], api_key)
        r = requests.get(url)
        # We make sure that we send messages to both partitions by iterating
        # with the module 2
        send_message(r.json(), i%2)

def send_message(payload, partition_number):
    '''
    Args:
        payload: json
        partition_number: int
        
    Connects to the EventHubClient and sends the data in json format retrieved
    from the API looping through the different partitions.
    '''

    logger = logging.getLogger("azure")
    event_hub_connection_str = get_resource_key("event_hub_connection_str")

    try:
        # Create Event Hubs client
        client = EventHubClient.from_event_hub_connection_string
        (event_hub_connection_str)
        sender = client.add_sender(partition=str(partition_number))
        client.run()
        try:
            start_time = time.time()
            print("Sending message: {}".format(payload))
            sender.send(EventData(payload))
        except:
            raise
        finally:
            end_time = time.time()
            client.stop()
            run_time = end_time - start_time
            logger.info("Runtime: {} seconds".format(run_time))

    except KeyboardInterrupt:
        exit()

def main():
    '''
    Checks if the client is called correctly, then calls the different 
    functions needed to retrieve API data and send azure eventhub messages.
    '''
    if len(sys.argv) != 2:
        exit("Usage: python {} YEAR".format(sys.argv[0]))
    if int(datetime.now().year) < int(sys.argv[1]):
        exit("Usage: python {} YEAR, please select a past year or the current\
        one".format(sys.argv[0]))
    
    # My current location
    lat, lon = get_resource_key("lat"), get_resource_key("lon")

    year = sys.argv[1]
    dates = get_dates(year)

    api_key = get_resource_key("api_key")
    api_url = get_resource_key("api_url")
    get_weather_data(api_url, api_key, lat, lon, dates)
 
 
if __name__ == '__main__':
    main()