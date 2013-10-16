from boto import *
from boto.s3.connection import S3Connection
import time
aws_access_key="blabla"
aws_secret_key="blabla"
bucket_to_watch="my_bucket"
sleep_in_seconds=5

conn = S3Connection(aws_access_key, aws_secret_key)
mybucket = conn.get_bucket(bucket_to_watch) 
# Substitute in your bucket name

# monitoring loop
while True:
 time.sleep(sleep_in_seconds) 
 file_list=mybucket.list()
 # check number of items in bucket
 if len(file_list) >0:
  # do something - prep the file in bucket
  pass
  
