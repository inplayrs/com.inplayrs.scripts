'''
CONNECTIONS CONFIG FILE
'''

import metadata.Env

# Connection config for each environment
dbConfig = {
    "local": {"env" : metadata.Env.LOCAL,
              "host" : "localhost",
              "port" : 3306,
              "user" : "AdminScripts",
              "pass" : "fP$xshw@83n1BB#z44",
              "db" : "ipdb"},
                 
    "dev" : {"env" : metadata.Env.DEV,
             "host" : "ipdb-dev.inplayrs.com",
             "port" : 3306,
             "user" : "AdminScripts",
             "pass" : "fP$xshw@83n1BB#z44",
             "db" : "ipdb"},
    
    "prod" : {"env" : metadata.Env.PROD,
              "host" : "ipdb.inplayrs.com",
              "port" : 3306,
              "user" : "AdminScripts",
              "pass" : "fP$xshw@83n1BB#z44",
              "db" : "ipdb"}        
}


# Access keys for user AdminScripts
AWS_ACCESS_KEY_ID = "AKIAJ43EZFEVCZTRVTKA"
AWS_SECRET_ACCESS_KEY = "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"

# Key to connect to Mandrill to send emails
MANDRILL_API_KEY = '8inFrylMNNZo7cBe2rNE8g'