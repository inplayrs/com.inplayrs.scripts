#!/usr/bin/env python3
'''
Created on 22 Nov 2014

@author: chris
'''

import argparse
import logging
import os
import sys
import inspect
import pymysql            # MySQL DB connection
from lxml import etree    # xml parsing
from boto.s3.connection import S3Connection

# Add the parent directory to sys.path so we can import local modules
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

# Import local modules
import config.Config
import config.Amazon
import metadata.XPath
import util.IPUtils as IPUtils

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("data_source_id", type=int, help="ID of data_source from which we are importing team data")
parser.add_argument("env", choices=['local', 'dev', 'prod'], metavar="env", help="Environment (local/dev/prod)")
parser.add_argument("input_file", help="File containing team data")
parser.add_argument("-d", "--debug", help="Debug mode", action="store_true")
args = parser.parse_args()

# Get logger and set level
loggingLevel = (logging.DEBUG if args.debug else logging.INFO)
logging.basicConfig(format='%(levelname)s: %(message)s', level=loggingLevel)
logger = logging.getLogger()
logger.setLevel(loggingLevel)

# load DB config and connect to DB
dbConfig = config.Config.dbConfig.get(args.env)
db = pymysql.connect(host=dbConfig['host'], port=dbConfig['port'], user=dbConfig['user'], passwd=dbConfig['pass'], db=dbConfig['db'])
db.autocommit(1)

# Create Amazon S3 connection (used for storing images)
s3conn = S3Connection(config.Amazon.AWS_ACCESS_KEY_ID, config.Amazon.AWS_SECRET_ACCESS_KEY)
s3bucket = s3conn.get_bucket('storage.inplayrs.com', validate=False) # No need to validate as we know this bucket exists

############################### GLOBAL VARIABLES ############################### 

# external_id -> internal_id
playerDataSourceMappings = {}



################################## FUNCTIONS ################################### 

def processPlayer(player):
    externalPlayerID = int(player.attrib['id'])
    internalPlayerID = playerDataSourceMappings.get(externalPlayerID)
    
    # Only process players for which we have ID mappings after loading team data
    if (internalPlayerID != None):
        playerName = metadata.XPath.Player_Name(player)[0].text
        logger.info("Processing player external_id="+str(externalPlayerID)+", internal_id="+str(internalPlayerID)+", name="+playerName)
        
        # Save player image to file
        if (len(metadata.XPath.Player_Image(player)) > 0 ):
            logger.info("Saving player image")
            IPUtils.saveBase64EncodedStringToFile(metadata.XPath.Player_Image(player)[0].text, "/var/tmp/inplayrs/files/images/players/"+str(internalPlayerID)+".jpg")
        else:
            logger.info("No player image found")
                               

##################################### MAIN ##################################### 

# Log start of processing
logger.info("Importing player data.  data_source_id="+str(args.data_source_id)+", env="+args.env+", input_file="+args.input_file)


# Load existing data_source_mappings for team and player table
cursor = db.cursor()
getDataSourceMappingsSql = '''SELECT m.external_id, m.internal_id FROM data_source_mapping m
WHERE m.data_source = %s AND m.table = 'player' '''
cursor.execute(getDataSourceMappingsSql, args.data_source_id)

for row in cursor.fetchall():
    playerDataSourceMappings[row[0]] = row[1]

cursor.close()


# Load file and process each team
context = etree.iterparse(args.input_file, events=('end',), tag='player')

IPUtils.fast_iter(context, processPlayer)



















11