#!/usr/bin/env python3
'''
@author: chris

Description: Process user trophies 

'''

import argparse
import logging
import os
import sys
import inspect
import pymysql            # MySQL DB connection

# Add the parent directory to sys.path so we can import local modules
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

# Import local modules
import config.Connections
import util.IPUtils as IPUtils
import metadata.State
import metadata.Trophy
import metadata.GameType
import dao.MotdDao as MotdDao
import dao.GameDao as GameDao

# Get script name
scriptName = str(os.path.basename(__file__)).replace(".py", "")

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("env", choices=['local', 'dev', 'prod'], metavar="env", help="Environment (local/dev/prod)")
parser.add_argument("game_id", type=int, help="ID of game for which you want to process trophies")
parser.add_argument("-d", "--debug", help="Debug mode", action="store_true")
args = parser.parse_args()

# Get logger
loggingLevel = (logging.DEBUG if args.debug else logging.INFO)
logger = IPUtils.getLogger(scriptName, loggingLevel)

# load DB config and connect to DB
dbConfig = config.Connections.dbConfig.get(args.env)
db = pymysql.connect(host=dbConfig['host'], port=dbConfig['port'], user=dbConfig['user'], passwd=dbConfig['pass'], db=dbConfig['db'])
db.autocommit(1)


############################### GLOBAL VARIABLES ############################### 

# user_id -> [list of user trophies (None if user has no trophies)]
userTrophies = {}


################################## FUNCTIONS ################################### 

#
# getGameState: returns the state of the current game
#
def getGameState(game_id):
    cursor =db.cursor()
    cursor.execute("SELECT state FROM game WHERE game_id = %s", game_id)
    (state,) = cursor.fetchone()
    cursor.close()
    return state


#
# getGameCompId: Returns the comp_id for the given game
#
def getGameCompId(game_id):
    cursor =db.cursor()
    cursor.execute("SELECT competition FROM game WHERE game_id = %s", game_id)
    (comp_id,) = cursor.fetchone()
    cursor.close()
    return comp_id


#
# getCompState: returns the state of the current competition
#
def getCompState(comp_id):
    cursor =db.cursor()
    cursor.execute("SELECT state FROM competition WHERE comp_id = %s", comp_id)
    (state,) = cursor.fetchone()
    cursor.close()
    return state


#
# loadUserTrophies: populates a mapping of users to a list of their trophies
#
def loadUserTrophies(game_id):
    loadUserTrophySql = '''SELECT ge.user, ut.`trophy`
                            FROM game_entry ge LEFT JOIN user_trophy ut ON ge.user = ut.user
                            WHERE ge.game = %s'''
    cursor = db.cursor()
    cursor.execute(loadUserTrophySql, game_id)
    for row in cursor.fetchall():
        if (userTrophies.get(row[0]) != None):
            userTrophies[row[0]].append(row[1])
        else:
            if (row[1] != None):
                userTrophies[row[0]] = [row[1]]
            else:
                userTrophies[row[0]] = None
    cursor.close()
    

#
# loadSpecificUserTrophies: populates map of users to list of trophies for a specific user
#
def loadSpecificUserTrophies(user_id):
    loadSpecificUserTrophySql = "SELECT user, trophy From user_trophy WHERE user = %s"
    cursor = db.cursor()
    cursor.execute(loadSpecificUserTrophySql, user_id)
    for row in cursor.fetchall():
        if (userTrophies.get(row[0]) != None):
            userTrophies[row[0]].append(row[1])
        else:
            if (row[1] != None):
                userTrophies[row[0]] = [row[1]]
            else:
                userTrophies[row[0]] = None
    cursor.close()
    
    
#
# updateFirstGlobalWinTrophy: Finds the global winner(s) for this game and gives users 
#                             the trophy if they don't already have it
# 
def updateFirstGlobalWinTrophy(game_id):
    # Find the winning users in global pool for this game
    getGameGlobalWinnerSql = "SELECT `user` FROM global_game_leaderboard ggl WHERE ggl.game = %s AND rank = 1"
    cursor = db.cursor()
    cursor.execute(getGameGlobalWinnerSql, game_id)
    for row in cursor.fetchall():
        grantUserTrophyIfNew(row[0], metadata.Trophy.GLOBAL_WIN)
    cursor.close()
            
            
#
# updateFirstCompWinTrophy: If competition is complete, grants trophy to the winner
#
def updateFirstCompWinTrophy(game_id):
    # If the competition is complete, grant the competition win to the winner
    comp_id = getGameCompId(game_id)
    compState = getCompState(comp_id)
    if (compState == metadata.State.COMPLETE):
        # Process trophy as competition is complete
        getCompWinnersSql = "SELECT `user` FROM global_comp_leaderboard gcl WHERE gcl.competition = %s AND rank = 1"
        cursor = db.cursor()
        cursor.execute(getCompWinnersSql, comp_id)
        for row in cursor.fetchall():
            # The competition winner may not necessarily have entered this game, so load this user's trophies
            loadSpecificUserTrophies(row[0])
            grantUserTrophyIfNew(row[0], metadata.Trophy.COMPETITION_WIN)
        cursor.close()
    else:
        logger.info("Competition "+str(comp_id)+" is not complete, state="+str(compState)+". Not processing First Comp Win trophy")


#
# updateFirstFriendWinTrophy: Finds winners of all pools, and checks if this is their first friend pool win
#
def updateFirstFriendWinTrophy(game_id):
    # Get all winners of friend pools for this game
    getPoolGameWinnersSql = '''SELECT 
                                pgl.user,
                                p.num_players,
                                p.pool_id
                            FROM 
                                pool_game_leaderboard pgl 
                                LEFT JOIN pool p on pgl.pool = p.pool_id
                            WHERE pgl.game = %s AND pgl.rank = 1'''
    cursor = db.cursor()
    cursor.execute(getPoolGameWinnersSql, game_id)
    for row in cursor.fetchall():
        if (row[1] < config.Settings.MIN_USERS_IN_POOL_FOR_FRIEND_WIN_TROPHY):
            logger.info("Not processing "+metadata.Trophy.trophyNames[metadata.Trophy.FRIEND_WIN]+" Trophy for user "+str(row[0])+" in pool "+str(row[2])+" as it only has "+str(row[1])+
                        " members, which is less than the minimum of "+str(config.Settings.MIN_USERS_IN_POOL_FOR_FRIEND_WIN_TROPHY))
        else:
            logger.info("User "+str(row[0])+" is rank 1 in pool "+str(row[2])+" which has "+str(row[1])+
                        " members. Checking if user has already been awarded "+metadata.Trophy.trophyNames[metadata.Trophy.FRIEND_WIN]+" Trophy")
            grantUserTrophyIfNew(row[0], metadata.Trophy.FRIEND_WIN)
    cursor.close()


#
# updateFirstH2HWinTrophy: Grants H2H win trophy to all users who won their H2H in this game
#
def updateFirstH2HWinTrophy(game_id):
    # Find all users who won their H2H in this game
    cursor = db.cursor()
    cursor.execute("SELECT user FROM game_entry WHERE game = %s AND h2h_winnings > 0", (game_id))
    for row in cursor.fetchall():
        grantUserTrophyIfNew(row[0], metadata.Trophy.H2H_WIN)
    cursor.close()
    
    
#
# processBanked3TimesTrophy: Finds all users who entered this game and have banked 3 or more times
#
def processBanked3TimesTrophy(game_id):
    usersWhoHaveBanked3TimesSql = '''SELECT 
                                        numBanksForUsersInGame.user
                                    FROM
                                        (SELECT
                                               ge.user as 'user', 
                                               SUM(ps.`cashed_out`) as 'num_banks'
                                        FROM
                                            game_entry ge 
                                            LEFT JOIN period_selection ps ON ge.`game_entry_id` = ps.`game_entry`
                                        WHERE
                                            ge.`user` IN (SELECT user FROM game_entry ge WHERE ge.`game` = %s)
                                        GROUP BY ge.user) as numBanksForUsersInGame
                                    WHERE numBanksForUsersInGame.num_banks > 2'''
    cursor = db.cursor()
    cursor.execute(usersWhoHaveBanked3TimesSql, game_id)
    for row in cursor.fetchall():
        grantUserTrophyIfNew(row[0], metadata.Trophy.BANKED_3_TIMES)
    cursor.close()
    

#
# processPlayed10GamesTrophy: Grants trophy to all users in this game who have played 10 games or more
#
def processPlayed10GamesTrophy(game_id):
    played10GamesSql = '''SELECT
                               ge.user as 'user'
                        FROM
                            game_entry ge 
                            LEFT JOIN user_stats us ON ge.`user` = us.`user`
                        WHERE ge.`game` = %s AND us.total_games_played > 9'''
    cursor = db.cursor()
    cursor.execute(played10GamesSql, game_id)
    for row in cursor.fetchall():
        grantUserTrophyIfNew(row[0], metadata.Trophy.PLAYED_10_GAMES)
    cursor.close()


#
# processPlayed50GamesTrophy: Grants trophy to all users in this game who have played 50 games or more
#
def processPlayed50GamesTrophy(game_id):
    played50GamesSql = '''SELECT
                               ge.user as 'user'
                        FROM
                            game_entry ge 
                            LEFT JOIN user_stats us ON ge.`user` = us.`user`
                        WHERE ge.`game` = %s AND us.total_games_played > 49'''
    cursor = db.cursor()
    cursor.execute(played50GamesSql, game_id)
    for row in cursor.fetchall():
        grantUserTrophyIfNew(row[0], metadata.Trophy.PLAYED_50_GAMES)
    cursor.close()
    
    
#
# processPerfectGameTrophy: Finds any users in this game who got all selections correct and awards trophy
#
def processPerfectGameTrophy(game_id):
    # We do not process the Perfect Game trophy for Fantasy game type
    gameDao = GameDao.GameDao(db)
    gameType = gameDao.getGameType(game_id)
    
    if (gameType == metadata.GameType.FANTASY or gameType == metadata.GameType.QUIZ):
        logger.info("Not processing "+metadata.Trophy.trophyNames[metadata.Trophy.PERFECT_GAME]+ " Trophy as this is a Fantasy or Quiz game type")
        return
    
    perfectGameSql = '''SELECT
                            userCorrectAnswers.user
                        FROM
                            (SELECT
                                   ge.user as 'user',
                                   ge.game as 'game',
                                   SUM((IF (ps.selection = p.result, 1, 0))) AS 'correct_answers'
                            FROM
                                game_entry ge 
                                LEFT JOIN period_selection ps ON ge.`game_entry_id` = ps.`game_entry`
                                LEFT JOIN period p ON ps.`period` = p.`period_id`
                            WHERE ge.`game` = %s
                            GROUP BY ge.user, ge.game) as userCorrectAnswers
                            LEFT JOIN
                                (SELECT p.game as 'game', COUNT(1) as 'num_periods' FROM period p    WHERE p.`game` = %s) AS periods
                                ON userCorrectAnswers.game = periods.game
                        WHERE
                            userCorrectAnswers.correct_answers = periods.num_periods'''
    cursor = db.cursor()
    cursor.execute(perfectGameSql, (game_id, game_id))
    for row in cursor.fetchall():
        grantUserTrophyIfNew(row[0], metadata.Trophy.PERFECT_GAME)
    cursor.close()
    

#
# processInvited5UsersTrophy: Finds users in this game who have invited 5 users to join
#
def processInvited5UsersTrophy(game_id):
    userInviteSql = '''SELECT users_invites.user
                        FROM
                            (SELECT
                                   ge.user as 'user',
                                COUNT(ui.`user_invite_id`) as 'num_invites'
                            FROM
                                game_entry ge 
                                LEFT JOIN user_invite ui ON ge.`user` = ui.source_user
                            WHERE ge.`game` = %s
                            GROUP BY ge.user) AS users_invites
                        WHERE users_invites.num_invites > 4'''
    cursor = db.cursor()
    cursor.execute(userInviteSql, game_id)
    for row in cursor.fetchall():
        grantUserTrophyIfNew(row[0], metadata.Trophy.INVITED_5_PEOPLE)
    cursor.close()


#
# grantUserTrophyIfNew - Grants user this trophy if they do not already have it
#
def grantUserTrophyIfNew(user_id, trophy_id):
    # Grant those users the trophy if they have not yet achieved it
    if (userTrophies.get(user_id) != None):
        # User has existing trophies
        if (trophy_id in userTrophies.get(user_id)):
            logger.info("User "+str(user_id)+" already has trophy "+str(trophy_id))
        else:
            addUserTrophy(user_id, trophy_id)
            userTrophies[user_id].append(trophy_id)
    else:
        # User has no existing trophies
        addUserTrophy(user_id, trophy_id)
        userTrophies[user_id] = [trophy_id]


#
# addUserTrophy: Adds new trophy for user
#        
def addUserTrophy(user_id, trophy_id):
    logger.info("User "+str(user_id)+" has won a new trophy, trophy_id="+str(trophy_id)+". Inserting into DB")
    insertUserTrophySql = "INSERT INTO user_trophy(user, trophy) VALUES (%s, %s)"
    cursor = db.cursor()
    cursor.execute(insertUserTrophySql, (user_id, trophy_id))
    cursor.close()
    
    # Add MOTD for user
    motdDao = MotdDao.MotdDao(db)
    message = "Congratulations, you have achieved the "+metadata.Trophy.trophyNames[trophy_id]+" Trophy!"
    logger.info("Adding motd: user_id="+str(user_id)+", message="+message)
    try:
        motdDao.create(user_id, message)
    except pymysql.err.IntegrityError:
        logger.error("Duplicate motd present, cannot insert")
    except pymysql.err.MySQLError:
        logger.error("Error when inserting motd")

    
#
# closeConnections: Closes all connections - used before finishing script
#
def closeConnections():
    # Close DB Connection
    db.close()
    
#
# closeConnectionsAndExit: Closes all connections and exits script - used when aborting script
#
def closeConnectionsAndExit():
    closeConnections()
    exit()


##################################### MAIN ##################################### 

logger.info("STARTING")

# Only process trophies for games that have completed
gameState = getGameState(args.game_id)
if (gameState == metadata.State.COMPLETE):
    logger.info("Game "+str(args.game_id)+" is complete, proceeding with processing of trophies")
else:
    logger.info("Game "+str(args.game_id)+" is NOT complete. Current game state is "+str(gameState)+". Quitting processing of trophies")
    closeConnectionsAndExit()
    
    
# Load user trophy data for the users who entered this game
logger.info("Loading existing user trophies for users who have entered this game")
loadUserTrophies(args.game_id)

logger.debug("Loaded User Trophies: "+(str(userTrophies)))


logger.info("Processing First Global Win Trophy")
updateFirstGlobalWinTrophy(args.game_id)

logger.info("Processing First Competition Win Trophy")
updateFirstCompWinTrophy(args.game_id)

logger.info("Processing First Friend Win Trophy")
updateFirstFriendWinTrophy(args.game_id)

logger.info("Processing First H2H Win Trophy")
updateFirstH2HWinTrophy(args.game_id)

logger.info("Processing Banked 3 Times Trophy")
processBanked3TimesTrophy(args.game_id)

logger.info("Processing Played 10 Games Trophy")
processPlayed10GamesTrophy(args.game_id)

logger.info("Processing Played 50 Games Trophy")
processPlayed50GamesTrophy(args.game_id)

logger.info("Processing Perfect Game Trophy")
processPerfectGameTrophy(args.game_id)

logger.info("Processing Invited 5 Users Trophy")
processInvited5UsersTrophy(args.game_id)


closeConnections()

logger.info("FINISHED")


