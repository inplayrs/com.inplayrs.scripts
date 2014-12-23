#!/usr/bin/env python3
'''
@author: chris

Description: Loads XML data feeds and stores to local disk

'''

import argparse
import logging
import os
import sys
import inspect
import time
import traceback
import urllib.request

# Add the parent directory to sys.path so we can import local modules
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

# Import local modules
import config.Settings
import util.IPUtils as IPUtils

# Get script name
scriptName = str(os.path.basename(__file__)).replace(".py", "")

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("type", choices=['preplay', 'inplay'], metavar="type", help="Type (preplay/inplay)")
parser.add_argument("-d", "--debug", help="Debug mode", action="store_true")
args = parser.parse_args()

# Get logger
loggingLevel = (logging.DEBUG if args.debug else logging.INFO)
logger = IPUtils.getLogger(scriptName, loggingLevel)


################################## FUNCTIONS ################################### 

def loadDataFeed(feed):
    logger.info("Loading feed: "+IPUtils.dictToString(feed))
    
    filePath = IPUtils.getDataFeedFilePath(feed)
    fileName = filePath +'/' +feed['fileName']
    tempFile = fileName+'.tmp'
    
    # Create storage directory if it doesn't already exist
    if not os.path.exists(filePath):
        os.makedirs(filePath)
    
    # Download the file from `url` and save it locally under `file_name`:
    with urllib.request.urlopen(feed['url']) as response, open(tempFile, 'wb') as outFile:
        data = response.read() # a `bytes` object
        outFile.write(data)
        os.rename(tempFile, fileName)
        logger.info("Loaded data to: "+fileName)
    
    

##################################### MAIN ##################################### 

logger.info("STARTING")

while True:
    try:
        for feed in config.Settings.dataFeeds:
            if feed['type'] == args.type:
                loadDataFeed(feed)
    
        logger.info("Sleeping for "+str(config.Settings.dataFeedInterval[args.type])+" seconds")
        time.sleep(config.Settings.dataFeedInterval[args.type])
    
    except (KeyboardInterrupt, SystemExit):
        logger.info("Received KeyboardInterrupt/SystemExit")
        exit()
    
    except Exception:
        # Catch any exceptions and retry on next run
        logger.error("Caught Exception: "+traceback.format_exc()+"\n"+str(sys.exc_info()[0]))
        time.sleep(config.Settings.dataFeedInterval[args.type]) 
        
    finally:
        logger.info("STOPPING")


