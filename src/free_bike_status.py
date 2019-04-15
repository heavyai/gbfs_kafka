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

#### free_bike_status
bike_status = p.map(get_url, feedlist(endpoints_df, "free_bike_status"))

#make df, make url column to know where data came from, append to list
bike_status_df_list = []
for x in bike_status:
    if x is not None:
        df = pd.DataFrame(x[0]["data"]["bikes"])
        df["baseurl"] = x[1].replace("/free_bike_status.json", "")
        bike_status_df_list.append(df)

#make single table with specified columns so no surprise columns happen
bike_status_df = pd.concat(bike_status_df_list, axis=0, sort=False)[['baseurl',
                                                                     'bike_id',
                                                                     'is_disabled',
                                                                     'is_reserved',
                                                                     'lat',
                                                                     'lon',
                                                                     'jump_ebike_battery_level',
                                                                     'name',
                                                                     'jump_vehicle_type',
                                                                     'vehicle_type'
                                                                    ]]

#this will be the filter key in immerse, and match key if needed
bike_status_df["accessed_on"] = start_time

bike_status_normalized = bike_status_df.to_dict(orient="records")

#write to kafka
from pykafka import KafkaClient

client = KafkaClient(hosts=hosts, broker_version="1.1.0")

topic = client.topics["gbfs_bikestatusall"]

with topic.get_producer(use_rdkafka=True) as producer:
    for message in bike_status_normalized:
        producer.produce(json.dumps(message, ignore_nan=True).encode('utf-8'), timestamp=datetime.datetime.now())
