#!/usr/bin/env python3
'''
Created on 22 Nov 2014
@author: chris

Description: Used to import player data such as images.  Only processes players for which we have ID mappings after loading team data 

Steps:
1. Download the player data from http://www.goalserve.com/xml/players.zip
2. Unzip the player data into a folder (e.g. /var/tmp/inplayrs/files)
3. Process each file in turn (attackers.xml, defenders.xml, midfielders.xml, goalkeepers.xml)
   e.g: importPlayerData.py 1 local /var/tmp/inplayrs/files/players/midfielders.xml

Images will be stored to file, unless the -s3 option is specified, in which case the images will be stored to Amazon S3

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
parser.add_argument("-s3", "--storeToS3", help="Store images to Amazon S3, even if environment is not prod", action="store_true")
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
        
        # Save player image
        if (len(metadata.XPath.Player_Image(player)) > 0 ):
            fileName = None
            fileType = None
            
            if (args.storeToS3):
                logger.info("Saving player image to Amazon S3")
                fileName = 'images/players/'+str(internalPlayerID)+'.jpg'
                fileType = IPUtils.saveBase64EncodedImageToAmazonS3(metadata.XPath.Player_Image(player)[0].text, fileName, s3bucket)
            else:
                logger.info("Saving player image to file")
                fileName = '/var/tmp/inplayrs/files/images/players/'+str(internalPlayerID)+'.jpg'
                fileType = IPUtils.saveBase64EncodedImageToFile(metadata.XPath.Player_Image(player)[0].text, fileName)
                
            if (fileType == None):
                logger.warning("Invalid image found in tag, unable to save")
            else:
                logger.info("Successfully saved image. fileType="+fileType+", fileName="+fileName)
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


# Close DB & Amazon S3 Connections
db.close()
s3conn.close()
