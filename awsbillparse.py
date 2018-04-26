import sys
import pandas as pd
import gzip


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

