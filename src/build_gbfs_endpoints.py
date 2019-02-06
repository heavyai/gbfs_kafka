import requests
import pandas as pd

# list on github provides discovery urls, access to get all endpoints
feedlist = pd.read_csv("gbfs/systems.csv")

# Check for valid feeds, keep responses
validfeeds = []
for x in feedlist['Auto-Discovery URL']:
    try:
        t = requests.get(x, timeout=2)
        validfeeds.append(t.json())
    except:
        print("failed: " + x)
        pass

# De-duplicate feeds by choosing either 1) en if multiple languages provided or
# 2) using 'feeds' as provided
feedslist = []
for x in validfeeds:
    try:
        feedslist.append(x["data"]["en"]["feeds"])
    except:
        feedslist.append(x["data"]["feeds"])

#unnest url structure, make into tuples for later dataframe creation
urls = []
for f in feedslist:
    for g in f:
        urls.append((g["name"], g["url"]))

# create dataframe and export endpoints
gbfs_df = pd.DataFrame(urls, columns=["feedtype", "feedurl"])
gbfs_df.to_csv("data/gbfs_endpoints.csv", index=False)
