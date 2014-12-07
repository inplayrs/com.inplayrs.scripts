#!/usr/bin/env python3
'''
@author: chris

Description: USE THIS FILE AS A TEMPLATE WHEN CREATING NEW PYTHON SCRIPTS 

'''

import argparse
import logging
import os
import sys
import inspect
import pymysql

# Add the parent directory to sys.path so we can import local modules
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

# Import local modules
import config.Config as Config
import util.IPUtils as IPUtils

# Get script name
scriptName = str(os.path.basename(__file__)).replace(".py", "")

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("env", choices=['local', 'dev', 'prod'], metavar="env", help="Environment (local/dev/prod)")
parser.add_argument("-d", "--debug", help="Debug mode", action="store_true")
args = parser.parse_args()

# Get logger
loggingLevel = (logging.DEBUG if args.debug else logging.INFO)
logger = IPUtils.getLogger(scriptName, loggingLevel)

# load DB config and connect to DB
dbConfig = Config.dbConfig.get(args.env)
db = pymysql.connect(host=dbConfig['host'], port=dbConfig['port'], user=dbConfig['user'], passwd=dbConfig['pass'], db=dbConfig['db'])
db.autocommit(1)


############################### GLOBAL VARIABLES ############################### 




################################## FUNCTIONS ################################### 




##################################### MAIN ##################################### 

logger.info("STARTING")




# Close DB Connection
db.close()

logger.info("FINISHED")