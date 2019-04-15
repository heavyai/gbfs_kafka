import pandas as pd
import requests
from multiprocessing import Pool
from datetime import datetime

# read in endpoints
endpoints_df = pd.read_csv("data/gbfs_endpoints.csv")

# simple function to be p.map[ped]
def get_url(url):

    r = None
    try:
        r = requests.get(url, timeout=2).json()
    except:
        pass

    return r

def feedlist(df, feedtype):
    return df[df["feedtype"] == feedtype].feedurl



# no reason for 12 specifically, just "some" parallelism
p = Pool(12)

# these files should change infrequently enough to batch run and load
# probably only need to run when build_gbfs_endpoints.py run after new program added
#### system_information
system_information = p.map(get_url, feedlist(endpoints_df, "system_information"))
system_information_df = pd.DataFrame([x["data"] for x in system_information if x is not None])[["system_id", "language", "name", "short_name", "operator", "url", "purchase_url", "start_date", "phone_number", "email", "timezone", "license_url"]]
system_information_df["accessed_on"] = datetime.now()
system_information_df.to_csv("data/system_information.csv", index=False)

#### station_information
station_information = p.map(get_url, feedlist(endpoints_df, "station_information"))
station_information_df_list = [pd.DataFrame(x["data"]["stations"]) for x in station_information if x is not None]
station_information_df = pd.concat(station_information_df_list, axis=0, sort=False)[["capacity","eightd_has_key_dispenser","lat","lon","name","region_id","rental_methods","short_name","station_id","address","station_code","post_code","external_id","has_kiosk","rental_url","cross_street","eightd_station_services"]]
station_information_df["accessed_on"] = datetime.now()
station_information_df.to_csv("data/station_information.csv", index=False)

#### system_hours
system_hours = p.map(get_url, feedlist(endpoints_df, "system_hours"))
system_hours_df_list = [pd.DataFrame(x["data"]["rental_hours"]) for x in system_hours if x is not None]
system_hours_df = pd.concat(system_hours_df_list, axis=0, sort=False)[["user_types", "days", "start_time", "end_time"]]
system_hours_df["accessed_on"] = datetime.now()
system_hours_df.to_csv("data/system_hours.csv", index=False)

#### system_pricing_plans
system_pricing = p.map(get_url, feedlist(endpoints_df, "system_pricing_plans"))
system_pricing_df_list = [pd.DataFrame(x["data"]["plans"]) for x in system_pricing if x is not None]
system_pricing_df = pd.concat(system_pricing_df_list, axis=0, sort=False)[["currency","description","is_taxable","name","plan_id","price","url"]]
system_pricing_df["accessed_on"] = datetime.now()
system_pricing_df.to_csv("data/system_pricing_plans.csv", index=False)
