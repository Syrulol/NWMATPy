# Lib Declarations
# TODO: Clean up scope maybe.
import json
import os
import shutil as stl
import string
import sys
import time as tm
from datetime import datetime

import numpy as np
import pandas as pd
import urllib3 as ul

# Establish Global Variables
apiRoot = "https://nwmarketprices.com/api/"
server_dict = {}


# This is messy, I need to handle exceptions when the resource wasn't fetched in a manner that gracefully terminates.
# Right now this will pass on an exception string as a JSON object if it 404's, or a null for other responses.
def queryapi(suburl):
    http = ul.PoolManager()
    httpresponse = 0
    retries = 0
    retrylim = 10  # This should be a global variable pulled from .cfg eventually.
    pausescaling = 5  # Controls how fast the pause variable grows.
    # Repeats query until it receives a 200 (OK) response or until retrylim is hit.
    while httpresponse != 200 and retries <= retrylim:
        jsonresponse = http.request(
            'GET', apiRoot + suburl, timeout=5.0, retries=5
        )
        httpresponse = jsonresponse.status
        timestamp = str(datetime.now())
        print(timestamp + ' | HTTP Response ' + str(httpresponse) + ' at ' + apiRoot + suburl)
        # If the response isn't a 200, it's likely a 403. Sleep for rate limiting.
        if httpresponse != 200:
            retries += 1
            print(timestamp + ' | Sleeping %f seconds.' % (retries * pausescaling))
            tm.sleep(retries * pausescaling)
    return jsonresponse


# Gets Server Information with Date/Time, writes retrieval to serverdata.csv for comparison.
# Should only really be called on first time initialization or if serverdata.csv doesn't exist.
# Can probably deprecate this with logic in getupdatequeries()
def getserverstatus():
    serverjson = queryapi('servers_updated/')
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
    serverjson = queryapi('servers_updated/')
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
    # Some data frame structuring for the return object.
    returndataframe = pd.DataFrame()
    returndataframe['server_id'] = serverdata.iloc[modvalue]['server_id']
    returndataframe['server_name'] = serverdata.iloc[modvalue]['server_name']
    returndataframe['last_timestamp'] = comparedata.iloc[modvalue]['server_last_updated']
    returndataframe['curr_timestamp'] = serverdata.iloc[modvalue]['server_last_updated']
    if np.any(modvalue):
        for server_name in returndataframe['server_name']:
            timestamp = str(datetime.now())
            print(timestamp + " | Update required for " + server_name)
    else:
        timestamp = str(datetime.now())
        print(timestamp + " | No update required.")
    return returndataframe


# Only needs to be run on first initialization. Populates cache with a query-all type approach.
def populatemarketdata():
    serverdf = getservercache()
    # Index helper for looping as a function of server ID's rather than index in the data frame.
    index = 0
    # Loops through every server in the serverID list.
    for x in serverdf['server_id']:
        # Query the API for latest prices by server ID
        marketjsonstream = queryapi('latest-prices/' + str(x))
        # Flatten and print the JSON. This is very messy in the console, but it's only necessary on first time run
        # and helps confirm that data was received in the anticipated format.
        marketjson = pd.json_normalize(json.loads(marketjsonstream.data.decode('utf-8')))
        print(marketjson)
        marketjson.to_csv('data/' + serverdf.iloc[index]['server_name'] + '.csv')
        print('data/' + serverdf.iloc[index]['server_name'] + '.csv')
        index += 1
        print(index)


# Helper function to query a specific server. This doesn't have rate limiting built in and no error handling.
# Probably a deprecated function. I also need to fix the output stream.
def getservermarketbyname(marketarg):
    serverinfo = pd.read_csv('data/serverdata.csv')
    serverid = serverinfo.loc[serverinfo['server_name'] == str(marketarg), 'server_id'].iloc[0]
    marketjsonstream = queryapi('latest-prices/' + str(serverid))
    marketjson = pd.json_normalize(json.loads(marketjsonstream.data.decode('utf-8')))
    marketjson.to_csv('data/' + marketarg + '.csv')


# Pulls JSON for servers listed from getupdatequeries() and archives the old data to data/archive with timestamping.
# Returns true if updates were written, and false if no updates were required.
def runupdatequeries():
    targetupdates = getupdatequeries()
    if targetupdates.empty:
        return False
    for x in targetupdates['server_id']:
        marketjsonstream = queryapi('latest-prices/' + str(x))
        # Flatten and normalize JSON.
        marketjson = pd.json_normalize(json.loads(marketjsonstream.data.decode('utf-8')))
        # Gets the server name currently being manipulated for reference in filenames.
        servername = targetupdates.loc[targetupdates['server_id'] == x, 'server_name'].iloc[0]
        # Gets the last queried timestamp of the server currently being manipulated for reference in filenames.
        timestamp = targetupdates.loc[targetupdates['server_id'] == x, 'last_timestamp'].iloc[0]
        timestampformat = timestamp[0:10] + 'T' + timestamp[11:19].translate(str.maketrans('', '', string.punctuation))
        # Moves the current server being queried into the archive with <servername><timestamp>.csv format.
        nowtimestamp = str(datetime.now())
        print(
            nowtimestamp + ' | Archiving ' + servername + ' to data/archive/ as ' + servername +
            ' ' + timestampformat + '.csv')
        stl.move("data/" + servername + ".csv", "data/archive/" + servername + ' ' + timestampformat + '.csv')
        # Writes a new .csv with the <servername>.csv format at /data/
        print(nowtimestamp + ' | Printing ' + servername + ' to .csv')
        marketjson.to_csv('data/' + targetupdates.loc[targetupdates['server_id'] == x, 'server_name'].iloc[0] + '.csv')
    # Updates the serverdata.csv now that all information is up-to-date as of now.
    getserverstatus()
    return True


# Pulls a list of CSV objects in the CWD data folder into the server_dict.
# Key: String Server Name, Value: Pandas.Dataframe
def fetchcsvs():
    for item in os.listdir('data/'):
        if item[-4:] == '.csv' and item[0:-4] != "serverdata":
            server_dict[item[0:-4]] = pd.read_csv('data/' + item)


# Fetches CSV's, populates a data frame with all server data, then filters that data by item and applies the
# pandas mean() function. Drops NaN.
def getmeanbyitem(itemname):
    fetchcsvs()
    returndf = pd.concat(server_dict.values())
    return returndf[returndf['ItemName'] == itemname]["Price"].mean()

# Calls runupdatequeries() with an output parameter. At the moment, only accepts log, which outputs to 
# logfile.txt. Any other strings besides 'log' will result in output to console. 
def runwithoutput(output):
    nowtimestamp = str(datetime.now())
    if output == 'log':
        sys.stdout = open('logfile.txt', 'a')
        print(nowtimestamp + ' | START')
        runupdatequeries()
        print(nowtimestamp + ' | STOP')
        sys.stdout.close()
    else:
        runupdatequeries()
