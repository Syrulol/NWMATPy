# NWMATPy
New World Market Assessment Toolkit (Py) is a Python export of R-based New World Market Assessment Toolkit.

Data structure has been rearranged to prevent extensive API-queries and to facilitate automated archival of data. 

New World Market Assessment Toolkit is a set of scripts and functions designed to automate data scraping from https://www.nwmarketprices.com/.
NWMATPy automatically reformats JSON outputs from web API-queries into .csv format, and places them into the data folder for further manipulation.
NWMATPy also automatically detects and archives data requiring updates, to facilitate analysis over time. 

Further functionality will be imported from existent NWMAT to allow for various modeling functions as time permits. 

The express purpose of NWMAT is to pipe .csv formatted documents into Tableau and other data viz software for dashboard output.

# TODO/Bugs:
* No error handling for RetryLimit in HTTP GET request. Should only ever occur if a connection cannot be established to NWMarketPrices API. 
* Various analysis functions for economic standing metrics. (Mean Absolute Deviation, Average Deviation, etc.) 
* Very basic GUI. 
* Config file to adjust rate limiting intervals, file write locations, etc. 
