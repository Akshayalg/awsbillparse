import sys
import pandas as pd
import gzip
from __future__ import print_function
import boto3
import os
import uuid
import json
import zipfile
import io


def awsbill_convert_new_to_old(src_file, dest_file):

    #Assumptions
    #  src_file is the .gz file
    #  There is only one file inside the .gz which is a csv

    #Get reference for contents inside .gz file
    csv_file = gzip.open(src_file)

    fields = ['bill/InvoiceId', 'bill/PayerAccountId', 'lineItem/UsageAccountId', 'product/ProductName',
              'lineItem/UsageType', 'lineItem/Operation', 'lineItem/AvailabilityZone', 'lineItem/LineItemDescription',
              'lineItem/UsageStartDate', 'lineItem/UsageEndDate', 'lineItem/UsageAmount', 'lineItem/BlendedRate',
              'lineItem/BlendedCost', 'lineItem/UnblendedRate', 'lineItem/UnblendedCost', 'lineItem/ResourceId']

    #Dictionary for columnHeader:type
    column_types = {'bill/InvoiceId': 'category', 'bill/PayerAccountId': 'category',
                        'lineItem/UsageAccountId': 'category', 'lineItem/UsageStartDate': 'object',
                        'lineItem/UsageEndDate': 'object', 'lineItem/UsageType': 'category',
                        'lineItem/Operation': 'category', 'lineItem/AvailabilityZone': 'category',
                        'lineItem/ResourceId': 'category','lineItem/UsageAmount': 'float64',
                        'lineItem/UnblendedRate': 'float64', 'lineItem/UnblendedCost': 'float64',
                        'lineItem/BlendedRate': 'float64', 'lineItem/BlendedCost': 'float64',
                        'lineItem/LineItemDescription': 'category','product/ProductName': 'category'}

    #Read the input csv file to retrieve selected column values
    csv_data = pd.read_csv(csv_file, usecols = fields, dtype=column_types)

    #JUST RENAME THE COLUMN HEADERS
    csv_data.columns=['InvoiceId', 'PayerAccountId', 'LinkedAccountId', 'ProductName',
                      'UsageType', 'Operation', 'AvailabilityZone', 'ItemDescription',
                      'UsageStartDate', 'UsageEndDate', 'UsageQuantity', 'BlendedRate',
                      'BlendedCost', 'UnBlendedRate', 'UnBlendedCost', 'ResourceId']
    csv_data.to_csv(dest_file, encoding='utf-8', index=False, compression='gzip')


def bill_split_json(input_path):

    #Reading the config json file
    with open(input_path) as f:
        data = json.load(f)


    #Retrieve the input csv file to parse 
    download_path = '/tmp/{}{}'.format(uuid.uuid4(), data["fileName"])
    s3_client.download_file(data["source"], data["fileName"], download_path)

    cols = pd.DataFrame()

    #Check whether the input file is zip file or not
    fname_list = data["fileName"].split(".")
    if "zip" in fname_list:
        zip_name = zipfile.ZipFile(download_path, 'r')
        csv_file = fname_list[0]+".csv"
        zp = zip_name.open(csv_file)
        
        #Dividing the dataframes into chunks
        csv_data = pd.read_csv(zp, iterator=True, chunksize=1000)
        for df in csv_data:
            for i in data["splits"][0]["accounts"]:
                cols = cols.append(df.loc[df['SubscriptionId'] == int(i["id"])])

    else:
        csv_file = data["fileName"]
        csv_data = pd.read_csv(download_path)
        for i in data["splits"][0]["accounts"]:
            cols = cols.append(csv_data.loc[csv_data['SubscriptionId'] == int(i["id"])])

    zip_file = csv_file+'.zip'
    upload_path = '/tmp/{}{}'.format(uuid.uuid4(), zip_file)
    csv_path = '/tmp/{}'.format( csv_file)

    os.chdir('/tmp')
    #Insert the parsed values to the output csv file
    cols.to_csv(csv_path, index = False)

    #Zip the csv file 
    with zipfile.ZipFile(upload_path, 'w') as zp:
        zp.write(csv_file)

    #upload the output zip file to the destination bucket
    s3_client.upload_file(upload_path, data["splits"][0]["destinationBucket"],'{}-filtered'.format(zip_file))
    os.remove(download_path)
    os.remove(upload_path)
