'''
Created on 7 Sep 2014

@author: chris
'''
import metadata.Env

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


        
        