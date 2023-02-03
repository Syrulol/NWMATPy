#Lib Declarations
#TODO: Clean up scope maybe.
import numpy as np
import pandas as pd
import urllib3 as ul
import json
import time as tm
import shutil as stl
import datetime

# Establish Global Variables
#TODO: vars_dict will be used for pulling data frames into the working memory.
apiRoot = "https://nwmarketprices.com/api/"
vars_dict = {}


# Gets Server Information with Date/Time, writes retreival to serverdata.csv for comparison.
# Should only really be called on first time initialization or if serverdata.csv doesn't exist.
# Can probably deprecate this with logic in getupdatequeries()
# TODO: Failure to establish a connection may result in an infinite loop here. Add incrementer for retries.
def getserverstatus():
    http = ul.PoolManager()
    httpresponse = 0
    # Repeats query until it receives a 200 (OK) response.
    while httpresponse != 200:
        serverjson = http.request(
            'GET', apiRoot + 'servers_updated/', timeout=4.0, retries=2
        )
        httpresponse = serverjson.status
        print('HTTP Response ' + str(serverjson.status) + ' ' + apiRoot + 'servers_updated/')
        # If the response isn't a 200, it's likely a 403. Sleep for 2 seconds for rate limiting.
        if httpresponse != 200:
            print("getserverstatus Sleeping 2 seconds.")
            tm.sleep(2)
    # Parse and Flatten JSON
    serverdata = pd.json_normalize(json.loads(serverjson.data.decode('utf-8')), ['server_last_updated'])
    serverdata = serverdata.rename(columns={0: 'server_id', 1: 'server_name', 2: 'server_last_updated'})
    # Send DataFrame to DataFrame cache
    serverdata.to_csv('data/serverdata.csv')
    # Return DataFrame
    return serverdata

# Reads serverdata.csv as a data frame for manipulation.
def getservercache():
    return pd.read_csv('data/serverdata.csv')

# Compares existent serverdata.csv to API query to establish which servers are oudated.
# If serverdata.csv doesn't exist for comparison, instead writes file to serverdata.csv and returns the data requiring
# updates as a data frame for manipulation with server ID and server name columns.
def getupdatequeries():
    # Request new information from API
    http = ul.PoolManager()
    httpresponse = 0
    # Repeats query until it receives a 200 (OK) response.
    while httpresponse != 200:
        serverjson = http.request(
            'GET', apiRoot + 'servers_updated/', timeout=4.0, retries=2
        )
        httpresponse = serverjson.status
        print('HTTP Response ' + str(serverjson.status) + ' ' + apiRoot + 'servers_updated/')
        # If the response isn't a 200, it's likely a 403. Sleep for 2 seconds for rate limiting.
        if httpresponse != 200:
            print("getupdatequeries Sleeping 2 seconds.")
            tm.sleep(2)
    # Parse and Flatten JSON
    serverdata = pd.json_normalize(json.loads(serverjson.data.decode('utf-8')), ['server_last_updated'])
    serverdata = serverdata.rename(columns={0: 'server_id', 1: 'server_name', 2: 'server_last_updated'})
    # Try to fetch cached data to compare.
    try:
        comparedata = pd.read_csv('data/serverdata.csv')
    # If it doesn't exist, cache the data now.
    except FileNotFoundError:
        comparedata = getserverstatus()
    # Compare the cache against the API request to identify servers that are out of date.
    modvalue = np.where(serverdata['server_last_updated'] != comparedata['server_last_updated'])
    print("Update required for... " + serverdata.iloc[modvalue]['server_name'])
    # Some data frame structuring for the return object.
    returndataserverid = serverdata.iloc[modvalue]['server_id']
    returndataservername = serverdata.iloc[modvalue]['server_name']
    returndataframe = pd.DataFrame()
    returndataframe['server_id'] = returndataserverid
    returndataframe['server_name'] = returndataservername
    return returndataframe

# Only needs to be run on first initialization. Populates cache with a query-all type approach.
def populatemarketdata():
    http = ul.PoolManager()
    serverdf = getservercache()
    # Index helper for looping as a function of server ID's rather than index in the data frame.
    index = 0
    # Loops through every server in the serverID list.
    for x in serverdf['server_id']:
        httpresponse = 0
        print('Server ID Query: ' + str(x))
        # Repeats query until it receives a 200 (OK) response.
        while httpresponse != 200:
            marketjsonstream = http.request(
                'GET', apiRoot + 'latest-prices/' + str(x), timeout=4.0, retries=2
            )
            httpresponse = marketjsonstream.status
            print('HTTP Response ' + str(marketjsonstream.status))
            # If the response isn't a 200, it's likely a 403. Sleep for 2 seconds for rate limiting.
            if httpresponse != 200:
                print("populatemarketdata Sleeping 2 seconds.")
                tm.sleep(2)
        # Flatten and print the JSON. This is very messy in the console, but it's only necessary on first time run
        # and helps confirm that data was received in the anticipated format.
        marketjson = pd.json_normalize(json.loads(marketjsonstream.data.decode('utf-8')))
        print(marketjson)
        marketjson.to_csv('data/' + serverdf.iloc[index]['server_name'] + '.csv')
        print('data/' + serverdf.iloc[index]['server_name'] + '.csv')
        index += 1
        print(index)

# Helper function to query a specific server. This doesn't have rate limiting built in and no error handling.
# Probably a deprecated function.
def getservermarketbyname(marketarg):
    serverinfo = pd.read_csv('data/serverdata.csv')
    serverid = serverinfo.loc[serverinfo['server_name'] == str(marketarg), 'server_id'].iloc[0]
    http = ul.PoolManager()
    marketjsonstream = http.request(
        'GET', apiRoot + 'latest-prices/' + str(serverid), timeout=4.0, retries=2
    )
    marketjson = pd.json_normalize(json.loads(marketjsonstream.data.decode('utf-8')))
    marketjson.to_csv('data/' + marketarg + '.csv')

# Pulls JSON for servers listed from getupdatequeries() and archives the old data to data/archive with timestamping.
# Timestamp is when it was archived, not when the data was from. Not sure which is more useful right now.
def runupdatequeries():
    targetupdates = getupdatequeries()
    targetupdates.set_index(['server_id'])
    for x in targetupdates['server_id']:
        http = ul.PoolManager()
        httpresponse = 0
        # Repeats query until it receives a 200 (OK) response.
        while httpresponse != 200:
            marketjsonstream = http.request(
                'GET', apiRoot + 'latest-prices/' + str(x), timeout=4.0, retries=2
            )
            httpresponse = marketjsonstream.status
            print('HTTP Response ' + str(marketjsonstream.status))
            # If the response isn't a 200, it's likely a 403. Sleep for 2 seconds for rate limiting.
            if httpresponse != 200:
                print("runupdatequeries Sleeping 2 seconds.")
                tm.sleep(2)
        # Flatten and normalize JSON.
        marketjson = pd.json_normalize(json.loads(marketjsonstream.data.decode('utf-8')))
        # Gets the server name currently being manipulated for reference in filenames.
        servername = targetupdates.loc[targetupdates['server_id'] == x, 'server_name'].iloc[0]
        # Timestamp string for appending to archival.
        timestamp = datetime.datetime.now()
        timestampformat = timestamp.strftime('%d%m%Y-%H%M%S')
        # Moves the current server being queried into the archive with <servername><timestamp>.csv format.
        print('Archiving ' + servername + 'to data/archive/ as ' + servername + timestampformat)
        stl.move("data/" + servername + ".csv", "data/archive/" + servername + ' ' + timestampformat + '.csv')
        # Writes a new .csv with the <servername>.csv format at /data/
        print('Printing ' + servername + ' to .CSV')
        marketjson.to_csv('data/' + targetupdates.loc[targetupdates['server_id'] == x, 'server_name'].iloc[0] + '.csv')
    # Updates the serverdata.csv now that all information is up-to-date as of now.
    getserverstatus()

runupdatequeries()