'''
Created on 8 Jan 2015

@author: chris
'''
# Handles interaction with game table
class GameDao:

    
    # Constructor
    def __init__(self, db):
        self.db = db
        
    
    # Create Message Of The Day
    def getGameType(self, game_id):
        getGameIDSQL = "SELECT game_type FROM game WHERE game_id = %s"
        cursor = self.db.cursor()
        cursor.execute(getGameIDSQL, game_id)
        (gameType,) = cursor.fetchone()
        cursor.close()
        return gameType