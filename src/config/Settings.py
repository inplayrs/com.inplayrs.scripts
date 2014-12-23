'''
SETTINGS CONFIG FILE
'''

# Logging 
LOGGING_BASE_DIRECTORY = '/var/tmp/{UserName}/inplayrs/logs/scripts'
LOGGING_INTERVAL = 1
LOGGING_INTERVAL_TYPE = 'midnight'
LOGGING_BACKUP_COUNT = 14


# Check for new users to be sent welcome emails every 300 seconds (5mins)
WELCOME_EMAIL_CHECK_INTERVAL = 300


# Folders to store data imported from feeds
BASE_DATA_FOLDER = '/var/tmp/{UserName}/inplayrs/data'

dataFeedInterval = {'preplay' : 600, 'inplay' : 10}

dataFeeds = [
                {
                 'category' : 'soccer',
                 'competition' : 'premier_league',
                 'fileName' : 'preplay_odds.xml',
                 'url' : 'http://www.goalserve.com/getfeed/5d9ac1a5a7c048809742407788fe4527/soccernew/england_shedule?odds=bet365',
                 'type' : 'preplay'
                 },
                {
                 'category' : 'soccer',
                 'competition' : 'champions_league',
                 'fileName' : 'preplay_odds.xml',
                 'url' : 'http://www.goalserve.com/getfeed/5d9ac1a5a7c048809742407788fe4527/soccernew/eurocups_shedule?odds=bet365',
                 'type' : 'preplay'
                 },
                {
                 'category' : 'soccer',
                 'competition' : 'all',
                 'fileName' : 'inplay_odds.xml',
                 'url' : 'http://www.goalserve.com/getfeed/5d9ac1a5a7c048809742407788fe4527/lines/soccer-inplay',
                 'type' : 'inplay'
                 },
                {
                 'category' : 'soccer',
                 'competition' : 'all',
                 'fileName' : 'inplay_scores.xml',
                 'url' : 'http://www.goalserve.com/getfeed/5d9ac1a5a7c048809742407788fe4527/soccernew/home',
                 'type' : 'inplay'
                 },
                {
                 'category' : 'soccer',
                 'competition' : 'premier_league',
                 'fileName' : 'inplay_commentaries.xml',
                 'url' : 'http://www.goalserve.com/getfeed/5d9ac1a5a7c048809742407788fe4527/commentaries/epl.xml',
                 'type' : 'inplay'
                 }
            ]

