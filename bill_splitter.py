from __future__ import print_function
import boto3
import os
import uuid
import awsbillparse as bs

s3_client = boto3.client('s3')

def handler(event, context):
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        key_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
        s3_client.download_file(bucket, key, key_path)

        bs.bill_split_json(key_path)
        os.remove(key_path)
