#!/usr/bin/env python3
'''
This script creates the next week's fantasy game based on the previous week's game
Pass in the game_id of the previous week's game, and the name for the next week's game, 
and the script will create a new game with a copy of the previous week's periods.
The mapping of old game_ids and period_ids is logged to disk
'''
import argparse
import logging
import os
import sys
import inspect

# Add the parent directory to sys.path so we can import local modules
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

#import metadata.State
import config.Config

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("game_id", type=int, help="ID of previous game")
parser.add_argument("name", help="Name of next game to create")
parser.add_argument("state", choices=["inactive", "preplay"], metavar="state", help="State that you want the new game to be in when created (inactive [-2] / preplay [-1])")
parser.add_argument("env", choices=['local', 'dev', 'prod'], metavar="env", help="Environment (local/dev/prod)")
parser.add_argument("-d", "--debug", help="Debug mode", action="store_true")
args = parser.parse_args()

# Get logger and set level
loggingLevel = (logging.DEBUG if args.debug else logging.INFO)
logging.basicConfig(format='%(levelname)s: %(message)s', level=loggingLevel)
logger = logging.getLogger()
logger.setLevel(loggingLevel)

# Validate command line args
#if args.state != metadata.State.INACTIVE and args.state != metadata.State.PREPLAY:
#    logger.error("state must be either "+str(metadata.State.INACTIVE)+" [INACTIVE] or "+str(metadata.State.PREPLAY)+" [PREPLAY]")
#    exit

logger.info("Creating next fantasy game from game_id "+str(args.game_id)+", with name '"+args.name+"'")

dbConfig = config.Config.dbConfig.get(args.env)

logger.debug("Environment: "+args.env+", DB Host: "+dbConfig["host"]+", DB Port: "+dbConfig["port"])















