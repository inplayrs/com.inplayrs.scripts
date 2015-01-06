'''
Created on 6 Jan 2015

@author: chris
'''

# Handles interaction with motd table, which stores Messages Of The Day
class MotdDao(object):

    
    # Constructor
    def __init__(self, db):
        self.db = db
        
    
    # Create Message Of The Day
    def create(self, user_id, message):
        insertMotdSql = "INSERT INTO motd(user, message) VALUES (%s, %s)"
        cursor = self.db.cursor()
        cursor.execute(insertMotdSql, (user_id, message))
        cursor.close()

            
            