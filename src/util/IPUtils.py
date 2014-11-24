'''
Created on 23 Nov 2014

@author: chris
'''
from base64 import decodebytes
from boto.s3.key import Key
from io import BytesIO

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
    fh = open(fileName, "wb")
    fh.write(byteString)
    fh.close()
    

#
# saveBase64EncodedStringToFile: Saves a base 64 encoded string to a file on local disk
#
def saveBase64EncodedStringToFile(base64String, fileName):
    saveByteStringToFile(decodebytes(str(base64String).encode('ascii')), fileName)
    

#
# saveByteStringToAmazonS3: Saves a byteString to the specified Amazon S3 bucket
#
def saveByteStringToAmazonS3(byteString, fileName, s3bucket):
    k = Key(s3bucket)
    k.key = fileName
    k.set_contents_from_file(BytesIO(byteString))


#
# saveBase64EncodedStringToAmazonS3: aves a base 64 encoded string to the specified Amazon S3 bucket
#
def saveBase64EncodedStringToAmazonS3(base64String, fileName, s3bucket):
    saveByteStringToAmazonS3(decodebytes(str(base64String).encode('ascii')), fileName, s3bucket)

