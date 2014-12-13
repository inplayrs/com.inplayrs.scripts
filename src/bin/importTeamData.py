#!/usr/bin/env python3
'''
Created on 2 Nov 2014
@author: chris

Description: Loads team data from a file and populates into DB

Steps:
1. Load team data from http://www.goalserve.com/xml/teams.zip
2. Unzip to a file (e.g. /var/tmp/inplayrs/files/teams.xml
3. Process the file
   e.g. importTeamData.py  1 1204 1 local /var/tmp/inplayrs/files/teams.xml

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

# Get script name
scriptName = str(os.path.basename(__file__)).replace(".py", "")

# Add the parent directory to sys.path so we can import local modules
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

# Import local modules
import config.Connections
import metadata.XPath
import util.IPUtils as IPUtils

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("data_source_id", type=int, help="ID of data_source from which we are importing team data")
parser.add_argument("external_comp_id", type=int, help="ID of external competition to load team data for")
parser.add_argument("internal_comp_id", type=int, help="ID of internal competition to load team data for")
parser.add_argument("env", choices=['local', 'dev', 'prod'], metavar="env", help="Environment (local/dev/prod)")
parser.add_argument("input_file", help="File containing team data")
parser.add_argument("-d", "--debug", help="Debug mode", action="store_true")
parser.add_argument("-s3", "--storeToS3", help="Store images to Amazon S3, even if environment is not prod", action="store_true")
args = parser.parse_args()

# Get logger
loggingLevel = (logging.DEBUG if args.debug else logging.INFO)
logger = IPUtils.getLogger(scriptName, loggingLevel)

# load DB config and connect to DB
dbConfig = config.Connections.dbConfig.get(args.env)
db = pymysql.connect(host=dbConfig['host'], port=dbConfig['port'], user=dbConfig['user'], passwd=dbConfig['pass'], db=dbConfig['db'])
db.autocommit(1)

# Create Amazon S3 connection (used for storing images)
s3conn = S3Connection(config.Amazon.AWS_ACCESS_KEY_ID, config.Amazon.AWS_SECRET_ACCESS_KEY)
s3bucket = s3conn.get_bucket('storage.inplayrs.com', validate=False) # No need to validate as we know this bucket exists

############################### GLOBAL VARIABLES ############################### 

# name -> team_id
teams = {}

# internalTeamId:name -> player_id
players = {}

# external_id -> {internal_id, mapping_id}
teamDataSourceMappings = {}
playerDataSourceMappings = {}


################################## FUNCTIONS ################################### 
    
#
# Process team XML
#
def processTeam(team):
    teamIsInCompetiton = False
    
    # Only process the team if they are in the competition (external_comp_id)
    for league_id in metadata.XPath.Team_LeagueIDs(team):
        if (league_id.text == str(args.external_comp_id)):
            teamIsInCompetiton = True
    
    if (not teamIsInCompetiton):
        return
    
    teamName = metadata.XPath.Team_Name(team)[0].text
    externalTeamID = team.attrib['id']
    
    logger.info("Processing team: "+teamName+", external_id: "+externalTeamID)

    # Check if a team with that name exists for that competition (internal_comp_id) in team table, and if not, insert it and keep the ID
    if (teams.get(teamName) == None):
        
        # Check if there is an existing mapping first, and if so, throw an error and stop processing this team
        if(teamDataSourceMappings.get(externalTeamID) != None):
            logger.error("Team with name "+teamName+" does not exist however there is already a data_source_mapping for this team: mapping_id="+
                         teamDataSourceMappings[externalTeamID]['mapping_id']+", skipping processing of this team. Please update team name in DB if name has changed.")
            return
        
        # Insert new team
        logger.info("Inserting new team. name="+teamName+", competition="+str(args.internal_comp_id))
        newTeamId = insertTeam(teamName, args.internal_comp_id)
        if (newTeamId > 0):
            logger.info("New team_id for "+teamName+": "+str(newTeamId))
            teams[teamName] = newTeamId
            
            # Create new data source mapping for the team
            logger.info("Creating new data_source_mapping entry for team: internal_id="+newTeamId+", external_id="+externalTeamID)
            newDataSourceMappingID = insertDataSourceMapping(args.data_source_id, 'team', newTeamId, int(externalTeamID))
            if (newDataSourceMappingID > 0):
                teamDataSourceMappings[externalTeamID] = {"internal_id" : newTeamId, "mapping_id:" : newDataSourceMappingID}
            else:
                logger.error("Failed to create new data_source_mapping for team with internal_id="+newTeamId+", external_id="+externalTeamID+". skipping processing of this team")
                return
            
        else:
            logger.error("Failed to insert team "+teamName+", skipping processing of this team")
            return
    else:
        logger.info("Team already exists in DB with team_id "+str(teams.get(teamName)))

    # Save team image
    if (len(metadata.XPath.Team_Image(team)) > 0 ):
        fileName = None
        fileType = None
        
        if (args.storeToS3):
            logger.info("Saving team image to Amazon S3")
            fileName = 'images/teams/'+str(teams[teamName])+'.jpg'
            fileType = IPUtils.saveBase64EncodedImageToAmazonS3(metadata.XPath.Team_Image(team)[0].text, fileName, s3bucket)
        else:
            logger.info("Saving team image to file")
            fileName = '/var/tmp/inplayrs/files/images/teams/'+str(teams[teamName])+'.jpg'
            fileType = IPUtils.saveBase64EncodedImageToFile(metadata.XPath.Team_Image(team)[0].text, fileName)
            
        if (fileType == None):
            logger.warning("Invalid image found in tag, unable to save")
        else:
            logger.info("Successfully saved image fileType="+fileType+", fileName="+fileName)
    else:
        logger.info("No team image found")
            
    # Cycle through the squad and inert into the player table if they do not already exist
    for player in metadata.XPath.Team_Players(team):
        processPlayer(player, teams[teamName])


#
# Process player XML
#
def processPlayer(player, internalTeamId):
    playerName = player.attrib['name']
    externalPlayerId = player.attrib['id']
    teamAndExternalPlayerId = str(internalTeamId)+':'+playerName
    logger.info("Processing player: "+playerName+", external_id="+externalPlayerId)
    
    # Check if player already exists in the player table, and if not, insert them
    if (players.get(teamAndExternalPlayerId) == None):
        
        # Check if there is an existing mapping first, and if so, throw an error and stop processing this player
        if(playerDataSourceMappings.get(externalPlayerId) != None):
            logger.error("Player with name "+playerName+" does not exist however there is already a data_source_mapping for this player: mapping_id="+
                         str(playerDataSourceMappings[externalPlayerId]['mapping_id'])+", skipping processing of this player. Please update player name in DB if name has changed.")
            return
        
        # Insert new player
        logger.info("Inserting new player. name="+playerName+", team="+str(internalTeamId))
        newPlayerId = insertPlayer(playerName, internalTeamId)
        if (newPlayerId > 0):
            logger.info("New player_id for "+playerName+": "+str(newPlayerId))
            players[teamAndExternalPlayerId] = newPlayerId
            
            # Create new data source mapping for the player
            logger.info("Creating new data_source_mapping entry for player: internal_id="+str(newPlayerId)+", external_id="+externalPlayerId)
            newDataSourceMappingID = insertDataSourceMapping(args.data_source_id, 'player', newPlayerId, int(externalPlayerId))
            if (newDataSourceMappingID > 0):
                playerDataSourceMappings[externalPlayerId] = {"internal_id" : newPlayerId, "mapping_id:" : newDataSourceMappingID}
            else:
                logger.error("Failed to create new data_source_mapping for player with internal_id="+str(newPlayerId)+", external_id="+externalPlayerId+". skipping processing of this player")
                return
            
        else:
            logger.error("Failed to insert player "+playerName+", skipping processing of this player")
            return
        
    else:
        logger.info("Player already exists in DB with player_id "+str(players[teamAndExternalPlayerId]))
      
      

# Insert team into DB
def insertTeam(teamName, competition):
    insertTeamSql = "INSERT INTO team(name, competition) VALUES (%s, %s)"
    cursor = db.cursor()
    result = cursor.execute(insertTeamSql, (teamName, competition))
    
    if (result != 1):
        cursor.close()
        return -1
    
    cursor.close()
    return cursor.lastrowid


# Insert player into DB
def insertPlayer(playerName, teamId):
    insertPlayerSQL = "INSERT INTO player(name, team) VALUES (%s, %s)"
    cursor = db.cursor()
    result = cursor.execute(insertPlayerSQL, (playerName, teamId))
    
    if (result != 1):
        cursor.close()
        return -1
    
    cursor.close()
    return cursor.lastrowid


# Insert new data source mapping
def insertDataSourceMapping(data_source, table, internal_id, external_id):
    insertDataSourceMappingSql = "INSERT INTO data_source_mapping(data_source, `table`, internal_id, external_id) VALUES (%s, %s, %s, %s)"
    
    cursor = db.cursor()
    result = cursor.execute(insertDataSourceMappingSql, (data_source, table, internal_id, external_id))
    
    if (result != 1):
        cursor.close()
        return -1
    
    cursor.close()
    return cursor.lastrowid

##################################### MAIN ##################################### 

# Log start of processing
logger.info("Importing team data.  data_source_id="+str(args.data_source_id)+", external_comp_id="+str(args.external_comp_id)+
             ", internal_comp_id="+str(args.internal_comp_id)+", env="+args.env+", input_file="+args.input_file)

# Load existing team data for this competition
cursor = db.cursor()
getExistingTeamsForCompSql = "SELECT team_id, name FROM team WHERE competition = %s"
cursor.execute(getExistingTeamsForCompSql, args.internal_comp_id)

for row in cursor.fetchall():
    teams[row[1]] = row[0]
    
cursor.close()


# Load existing player data for this competition
cursor = db.cursor()
getExistingPlayersForCompSql = '''SELECT p.player_id, p.name, p.team 
FROM player p LEFT JOIN team t on p.team = t.team_id 
WHERE t.competition = %s'''
cursor.execute(getExistingPlayersForCompSql, args.internal_comp_id)

for row in cursor.fetchall():
    players[str(row[2])+':'+row[1]] = row[0]
    
cursor.close()


# Load existing data_source_mappings for team and player table
cursor = db.cursor()
getDataSourceMappingsSql = '''SELECT m.table, m.external_id, m.internal_id, m.mapping_id FROM data_source_mapping m
WHERE m.data_source = %s AND m.table IN ('team', 'player')'''
cursor.execute(getDataSourceMappingsSql, args.data_source_id)

for row in cursor.fetchall():
    if (row[0] == 'team'):
        teamDataSourceMappings[row[1]] = {"internal_id" : row[2], "mapping_id" : row[3]}
    elif (row[0] == 'player'):
        playerDataSourceMappings[row[1]] = {"internal_id" : row[2], "mapping_id" : row[3]}

cursor.close()


# Load file and process each team
context = etree.iterparse(args.input_file, events=('end',), tag='team')
IPUtils.fast_iter(context, processTeam)


# Close DB & Amazon S3 Connections
db.close()
s3conn.close()

