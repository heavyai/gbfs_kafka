import pandas as pd
import requests
from multiprocessing import Pool
import datetime
import simplejson as json

from credentials import hosts

#round to minute, set the same for all records
#this is so that each load represents same time period
start_time = int(datetime.datetime.now().replace(second=0, microsecond=0).timestamp())

# read in endpoints
endpoints_df = pd.read_csv("data/gbfs_endpoints.csv")

# simple function to be p.map[ped]
def get_url(url):

    r = None
    try:
        r = (requests.get(url, timeout=2).json(), url)
    except:
        pass

    return r

def feedlist(df, feedtype):
    return df[df["feedtype"] == feedtype].feedurl

# no reason for 12 specifically, just "some" parallelism
p = Pool(12)

#### station_status
station_status = p.map(get_url, feedlist(endpoints_df, "station_status"))

#make df, make url column to know where data came from, append to list
station_status_df_list = []
for x in station_status:
    if x is not None:
        df = pd.DataFrame(x[0]["data"]["stations"])
        df["baseurl"] = x[1].replace("/station_status.json", "")
        station_status_df_list.append(df)

#make single table with specified columns so no surprise columns happen
station_status_df = pd.concat(station_status_df_list, axis=0, sort=False)[['baseurl',
                                                                          'eightd_has_available_keys',
                                                                          'is_installed',
                                                                          'is_renting',
                                                                          'is_returning',
                                                                          'last_reported',
                                                                          'num_bikes_available',
                                                                          'num_bikes_disabled',
                                                                          'num_docks_available',
                                                                          'num_docks_disabled',
                                                                          'station_id',
                                                                          'num_bikes_available_types',
                                                                          'num_ebikes_available',
                                                                          'eightd_active_station_services'
                                                                          ]]

#this will be the filter key in immerse, and match key if needed
station_status_df["accessed_on"] = start_time

station_status_normalized = station_status_df.to_dict(orient="records")

#write to kafka
from pykafka import KafkaClient

client = KafkaClient(hosts=hosts, broker_version="1.1.0")

topic = client.topics["gbfs_stationstatusall"]

with topic.get_producer(use_rdkafka=True) as producer:
    for message in station_status_normalized:
        producer.produce(json.dumps(message, ignore_nan=True).encode('utf-8'), timestamp=datetime.datetime.now())
