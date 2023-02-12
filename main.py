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
api_root = 'https://nwmarketprices.com/api/'
server_dict = {}


# This is messy, I need to handle exceptions when the resource wasn't fetched in a manner that gracefully terminates.
# Right now this will pass on an exception string as a JSON object if it 404's, or a null for other responses.
def query_api(suburl: str) -> json:
    http = ul.PoolManager()
    status_response_code = 0
    retries = 0
    retrylim = 10  # This should be a global variable pulled from .cfg eventually.
    pausescaling = 5  # Controls how fast the pause variable grows.
    # Repeats query until it receives a 200 (OK) response or until retrylim is hit.
    while status_response_code != 200 and retries <= retrylim:
        json_response = http.request(
            'GET', api_root + suburl, timeout=5.0, retries=5
        )
        status_response_code = json_response.status
        timestamp = str(datetime.now())
        print(timestamp + ' | HTTP Response ' + str(status_response_code) + ' at ' + api_root + suburl)
        # If the response isn't a 200, it's likely a 403. Sleep for rate limiting.
        if status_response_code != 200:
            retries += 1
            print(timestamp + ' | Sleeping %f seconds.' % (retries * pausescaling))
            tm.sleep(retries * pausescaling)
    return json_response


# Gets Server Information with Date/Time, writes retrieval to serverdata.csv for comparison.
# Should only really be called on first time initialization or if serverdata.csv doesn't exist.
# Can probably deprecate this with logic in getupdatequeries()
def get_server_status() -> pd.DataFrame:
    server_json = query_api('servers_updated/')
    # Parse and Flatten JSON
    server_data = pd.json_normalize(json.loads(server_json.data.decode('utf-8')), ['server_last_updated'])
    server_data = server_data.rename(columns={0: 'server_id', 1: 'server_name', 2: 'server_last_updated'})
    # Send DataFrame to DataFrame cache
    server_data.to_csv('data/serverdata.csv')
    # Return DataFrame
    return server_data


# Reads serverdata.csv as a data frame for manipulation.
def get_server_data_cache() -> pd.DataFrame:
    return pd.read_csv('data/serverdata.csv')


# Compares existent serverdata.csv to API query to establish which servers are oudated.
# If serverdata.csv doesn't exist for comparison, instead writes file to serverdata.csv and returns the data requiring
# updates as a data frame for manipulation with server ID and server name columns.
def get_update_queries() -> pd.DataFrame:
    # Request new information from API
    server_json = query_api('servers_updated/')
    # Parse and Flatten JSON
    server_data = pd.json_normalize(json.loads(server_json.data.decode('utf-8')), ['server_last_updated'])
    server_data = server_data.rename(columns={0: 'server_id', 1: 'server_name', 2: 'server_last_updated'})
    # Try to fetch cached data to compare.
    try:
        compare_data = pd.read_csv('data/serverdata.csv')
    # If it doesn't exist, cache the data now.
    except FileNotFoundError:
        compare_data = get_server_status()
    # Compare the cache against the API request to identify servers that are out of date.
    modvalue = np.where(server_data['server_last_updated'] != compare_data['server_last_updated'])
    # Some data frame structuring for the return object.
    return_dataframe = pd.DataFrame()
    return_dataframe['server_id'] = server_data.iloc[modvalue]['server_id']
    return_dataframe['server_name'] = server_data.iloc[modvalue]['server_name']
    return_dataframe['last_timestamp'] = compare_data.iloc[modvalue]['server_last_updated']
    return_dataframe['curr_timestamp'] = server_data.iloc[modvalue]['server_last_updated']
    # Workaround to suppress output in the console/logfile.
    if np.any(modvalue):
        for server_name in return_dataframe['server_name']:
            timestamp = str(datetime.now())
            print(timestamp + ' | Update required for ' + server_name)
    else:
        timestamp = str(datetime.now())
        print(timestamp + ' | No update required.')
    return return_dataframe


# Only needs to be run on first initialization. Populates cache with a query-all type approach.
def populate_market_data() -> None:
    server_df = get_server_data_cache()
    # Index helper for looping as a function of server ID's rather than index in the data frame.
    index = 0
    # Loops through every server in the serverID list.
    for server_id in server_df['server_id']:
        # Query the API for latest prices by server ID
        market_json = query_api('latest-prices/' + str(server_id))
        # Flatten and print the JSON. This is very messy in the console, but it's only necessary on first time run
        # and helps confirm that data was received in the anticipated format.
        market_json = pd.json_normalize(json.loads(market_json.data.decode('utf-8')))
        print(market_json)
        market_json.to_csv('data/' + server_df.iloc[index]['server_name'] + '.csv')
        print('data/' + server_df.iloc[index]['server_name'] + '.csv')
        index += 1
        print(index)
    return None


# Helper function to query a specific server. This doesn't have rate limiting built in and no error handling.
# Probably a deprecated function. I also need to fix the output stream.
def get_server_market(server_name: str) -> None:
    server_info = pd.read_csv('data/serverdata.csv')
    server_id = server_info.loc[server_info['server_name'] == str(server_name), 'server_id'].iloc[0]
    market_json = query_api('latest-prices/' + str(server_id))
    market_json = pd.json_normalize(json.loads(market_json.data.decode('utf-8')))
    market_json.to_csv('data/' + server_name + '.csv')
    return None


# Pulls JSON for servers listed from getupdatequeries() and archives the old data to data/archive with timestamping.
# Returns true if updates were written, and false if no updates were required.
def run_update_queries() -> bool:
    target_updates = get_update_queries()
    if target_updates.empty:
        return False
    for x in target_updates['server_id']:
        market_json = query_api('latest-prices/' + str(x))
        # Flatten and normalize JSON.
        market_json = pd.json_normalize(json.loads(market_json.data.decode('utf-8')))
        # Gets the server name currently being manipulated for reference in filenames.
        server_name = target_updates.loc[target_updates['server_id'] == x, 'server_name'].iloc[0]
        # Gets the last queried timestamp of the server currently being manipulated for reference in filenames.
        timestamp = target_updates.loc[target_updates['server_id'] == x, 'last_timestamp'].iloc[0]
        timestampformat = timestamp[0:10] + 'T' + timestamp[11:19].translate(str.maketrans('', '', string.punctuation))
        # Moves the current server being queried into the archive with <server_name><timestamp>.csv format.
        nowtimestamp = str(datetime.now())
        print(
            nowtimestamp + ' | Archiving ' + server_name + ' to data/archive/ as ' + server_name +
            ' ' + timestampformat + '.csv')
        stl.move("data/" + server_name + '.csv', 'data/archive/' + server_name + ' ' + timestampformat + '.csv')
        # Writes a new .csv with the <server_name>.csv format at /data/
        print(nowtimestamp + ' | Printing ' + server_name + ' to .csv')
        market_json.to_csv('data/' + target_updates.loc[target_updates['server_id'] == x, 'server_name'].iloc[0] + '.csv')
    # Updates the serverdata.csv now that all information is up-to-date as of now.
    get_server_status()
    return True


# Pulls a list of csv objects from the ./data folder into the server_dict as DataFrame objects, keyed with server name.
def fetch_server_cache() -> None:
    for item in os.listdir('data/'):
        if item[-4:] == '.csv' and item[0:-4] != "serverdata":
            server_dict[item[0:-4]] = pd.read_csv('data/' + item).set_index('Unnamed: 0')
            server_dict[item[0:-4]]['Server'] = item[0:-4]
    return None


# Fetches all instances of itemname from csv's, returns a dataframe.
def get_all_items(item_name: str) -> pd.DataFrame:
    fetch_server_cache()
    returndf = pd.concat(server_dict.values())
    return returndf[returndf['ItemName'] == item_name]


# Fetches pd.DataFrame of specific server from the dictionary.
def get_server_dataframe(server_name: str) -> pd.DataFrame:
    fetch_server_cache()
    return server_dict[server_name]


# Returns the data frame of an item by server and item name.
def get_item_dataframe(item_name: str, server_name: str) -> pd.DataFrame:
    return_frame = get_server_dataframe(server_name)
    return return_frame[return_frame['ItemName'] == item_name]


# Returns a data frame of an item's price history graph.
def item_history(item_name: str, server_name: str) -> pd.DataFrame:
    nwdb_id = lookup_nwdb_id(item_name)
    server_id = lookup_server_id(server_name)
    returnframe = pd.json_normalize(json.loads((query_api(server_id + '/?cn_id=' + nwdb_id).data.decode('utf-8'))),
                                    ['price_graph_data'])
    returnframe['server'] = server_name
    returnframe['item'] = item_name
    return returnframe[['avail', 'date_only', 'avg_price','server','item']]


# Calls run_update_queries() with an output parameter. At the moment, only accepts log, which outputs to
# logfile.txt. Any other strings besides 'log' will result in output to console.
# Open on finish will open the default text editor with the logfile Default False.
def update_with_output(output='', openonfinish=False) -> None:
    if output == 'log':
        sys.stdout = open('logfile.txt', 'a')
        sys.stderr = open('errorlog.txt', 'a')
        print(str(datetime.now()) + ' | START')
        run_update_queries()
        print(str(datetime.now()) + ' | STOP')
        sys.stdout.close()
        sys.stderr.close()
        if openonfinish:
            os.startfile('logfile.txt')

    else:
        run_update_queries()
    return None


# Returns a flat data frame with all cached server data.
def batch_merge() -> pd.DataFrame:
    fetch_server_cache()
    return pd.concat(server_dict.values())


# Quereies API for updated item ID database, caches into .data/static/itemdata.csv
def refresh_item_ids() -> None:
    item_json = query_api('confirmed_names/').data.decode('utf-8')
    outdataframe = pd.read_json(item_json, orient='index')
    outdataframe.to_csv('data/static/' + 'itemdata' + '.csv')
    return None


# Pulls item ID's from cache back into dataframe and drops unnecessary data.
def fetch_item_cache() -> pd.DataFrame:
    return pd.read_csv('data/static/itemdata.csv').drop('Unnamed: 0', axis=1)


# Helper function to return an item's ID from its item name.
def lookup_nwdb_id(item_name: str) -> str:
    itemdata = fetch_item_cache()
    return itemdata[itemdata['name'] == item_name]['nwdb_id'].values[0]


# Helper function to return a server's ID by its name.
def lookup_server_id(server_name: str) -> str:
    serverdata = get_server_data_cache()
    return str(serverdata[serverdata['server_name'] == server_name]['server_id'].values[0])
