#!/usr/bin/env python3
'''
@author: chris

Description: Checks for any users who have not yet received a welcome email and sends 
             them an email

'''

import argparse
import logging
import os
import sys
import inspect
import pymysql
import re
import mandrill
import time
import traceback

# Add the parent directory to sys.path so we can import local modules
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

# Import local modules
import config.Connections
import config.Settings
import util.IPUtils as IPUtils

# Get script name
scriptName = str(os.path.basename(__file__)).replace(".py", "")

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("env", choices=['local', 'dev', 'prod'], metavar="env", help="Environment (local/dev/prod)")
parser.add_argument("-d", "--debug", help="Debug mode", action="store_true")
parser.add_argument("-f", "--force", help="Forces script to actually send emails when not running against the prod environment", action="store_true")
args = parser.parse_args()

# Get logger
loggingLevel = (logging.DEBUG if args.debug else logging.INFO)
logger = IPUtils.getLogger(scriptName, loggingLevel)

# load DB config and connect to DB
dbConfig = config.Connections.dbConfig.get(args.env)
db = pymysql.connect(host=dbConfig['host'], port=dbConfig['port'], user=dbConfig['user'], passwd=dbConfig['pass'], db=dbConfig['db'])
db.autocommit(1)


############################### GLOBAL VARIABLES ############################### 

mandrill_client = mandrill.Mandrill(config.Connections.MANDRILL_API_KEY)

# Very basic email validator - checks we have only one @ symbol and at least one . in the domain
validEmailRegex = re.compile(r"[^@]+@[^@]+\.[^@]+")


################################## FUNCTIONS ################################### 

#
# markUserReceivedWelcomeEmail: Marks a user as having received the welcome email
#
def markUserReceivedWelcomeEmail(email):
    cursor = db.cursor() 
    cursor.execute("UPDATE user SET welcome_email = 1 WHERE email = %s", email)
    cursor.close()

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

while True:
    try:
        # Get all user emails for users who have not yet received a welcome email
        logger.info("Checking for users who need a welcome email")
        cursor = db.cursor()
        cursor.execute("SELECT email FROM user WHERE welcome_email  = 0 AND email IS NOT NULL")
        for row in cursor.fetchall():
            # Process each user email
            email = row[0]
            
            if validEmailRegex.match(email):
                # Prepare email content
                message = {
                    'subject': 'Welcome to INPLAYRS!',
                    'to': [{'email': email}],
                    'from_name': 'INPLAYRS',
                    'from_email': 'support@inplayrs.com',
                    'headers': {'Reply-To': 'support@inplayrs.com'},
                    'track_opens': True, 'track_clicks': True}
                
                content = []
                
                # Only actually send the email if we are running against Prod or if we are forcing 
                if (args.env == 'prod' or args.force):
                    mandrill_client.messages.send_template(
                        template_name='inplayrs welcome',
                        template_content=content, message=message)
                    
                    logger.info("Welcome email sent to "+email)
                    markUserReceivedWelcomeEmail(email)
                else:
                    logger.info("Would have sent welcome email to "+email+".  To actually send emails when not running against Prod, please use the -f option")
                
            else:
                logger.error("Bad email address, cannot send welcome email: "+email)
            
        cursor.close()
        logger.info("Sleeping for "+str(config.Settings.WELCOME_EMAIL_CHECK_INTERVAL)+" seconds")
        time.sleep(config.Settings.WELCOME_EMAIL_CHECK_INTERVAL) 
        
    except (KeyboardInterrupt, SystemExit):
        logger.info("Received KeyboardInterrupt/SystemExit")
        exit()
    
    except Exception:
        # Catch any exceptions and retry on next run
        logger.error("Caught Exception: "+traceback.format_exc()+"\n"+str(sys.exc_info()[0]))
        time.sleep(config.Settings.WELCOME_EMAIL_CHECK_INTERVAL) 
        
    finally:
        logger.info("STOPPING")
        closeConnections()

