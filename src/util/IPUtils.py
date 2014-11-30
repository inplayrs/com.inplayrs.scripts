'''
Created on 23 Nov 2014

@author: chris
'''
from base64 import decodebytes
from boto.s3.key import Key
from io import BytesIO
import imghdr
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import pwd

# Import local modules
import config.Config as Config


#
# fast_iter: Function to iterate through an xml file, applying a function to each element
#
def fast_iter(context, func):
    for event, elem in context:
        func(elem)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context
    
    
#
# saveByteStringToFile: Saves a byteString to a file on local disk 
#    
def saveByteStringToFile(byteString, fileName):
    # wb = open file for writing in binary mode
    fh = open(fileName, "wb")
    fh.write(byteString)
    fh.close()
    

#
# saveBase64EncodedStringToFile: Saves a base 64 encoded string to a file on local disk
#
def saveBase64EncodedStringToFile(base64String, fileName):
    saveByteStringToFile(decodebytes(str(base64String).encode('ascii')), fileName)
 
    
#
# saveBase64EncodedImageToFile: Validates that the base64 encoded string is an image before saving to file
# Returns the file type of the image saved
#
def saveBase64EncodedImageToFile(base64String, fileName):
    byteString = decodebytes(str(base64String).encode('ascii'))
    fileType = imghdr.what(None, byteString)
    
    # Only save to file if this is a valid image
    if (fileType == None):
        return fileType

    saveByteStringToFile(byteString, fileName) 
    return fileType 


#
# saveByteStringToAmazonS3: Saves a byteString to the specified Amazon S3 bucket
#
def saveByteStringToAmazonS3(byteString, fileName, s3bucket):
    k = Key(s3bucket)
    k.key = fileName
    k.set_contents_from_file(BytesIO(byteString))


#
# saveBase64EncodedStringToAmazonS3: saves a base 64 encoded string to the specified Amazon S3 bucket
#
def saveBase64EncodedStringToAmazonS3(base64String, fileName, s3bucket):
    saveByteStringToAmazonS3(decodebytes(str(base64String).encode('ascii')), fileName, s3bucket)


#
# saveBase64EncodedImageToAmazonS3: Validates that the base64 encoded string is an image before saving to the specified Amazon S3 bucket
# Returns the file type of the image saved
#
def saveBase64EncodedImageToAmazonS3(base64String, fileName, s3bucket):
    byteString = decodebytes(str(base64String).encode('ascii'))
    fileType = imghdr.what(None, byteString)
    
    # Only save to file if this is a valid image
    if (fileType == None):
        return fileType
    
    saveByteStringToAmazonS3(decodebytes(str(base64String).encode('ascii')), fileName, s3bucket)
    return fileType


#
# getLogger: Creates log directory if it doesn't exist and returns a logger
#
def getLogger(scriptName, loggingLevel):
    logDirectory = Config.LOGGING_BASE_DIRECTORY+'/'+scriptName
    
    # Replace UserName with name of user running the process
    logDirectory = logDirectory.replace("{UserName}", pwd.getpwuid(os.getuid()).pw_name)
    
    # Create log directory if it doesn't already exist
    if not os.path.exists(logDirectory):
        os.makedirs(logDirectory)
        
    logPath = logDirectory+'/'+scriptName+'.log'
    
    logHandler = TimedRotatingFileHandler(logPath,
                                          when=Config.LOGGING_INTERVAL_TYPE,
                                          interval=Config.LOGGING_INTERVAL,
                                          backupCount=Config.LOGGING_BACKUP_COUNT)
    logHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logHandler.setLevel(loggingLevel)
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=loggingLevel) # Logs to stdout
    logger = logging.getLogger()
    logger.addHandler(logHandler)
    return logger


