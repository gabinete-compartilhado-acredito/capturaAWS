#!/usr/bin/env python3

"""
This script takes no parameters.

It downloads all permanent tables (whose names do not start with 'temp')
and saves them as normal JSONs (not dynamoDB JSONs) in files of name 
according to the table. The JSONs are saved to the hard-coded folder.

Written by Henrique S. Xavier, hsxavier@if.usp.br, on 26/sep/2019.
"""

import sys
import boto3
from dynamodb_json import json_util as dyjson 
import json

# Docstring output:
if len(sys.argv) != 1 + 0: 
    print(__doc__)
    sys.exit(1)

# Hard-coded stuff:
dynamoDB_folder = 'dynamoDB/' 
    
# Start dynamoDB client:
dynamoDB = boto3.client('dynamodb')

# Get list of permanent tables in dynamoDB:
table_list_raw = dynamoDB.list_tables()
table_list = filter(lambda s: s[:4] != 'temp', table_list_raw['TableNames'])

for table_name in table_list:

    print('Downloading table ' + table_name + '...')
    
    # Get all table items:
    table_content = dynamoDB.scan(TableName=table_name)

    # Convert dynamo json to normal json:
    table_json = dyjson.loads(json.dumps(table_content))

    # Get items in table:
    table_items = table_json['Items']

    # Save it to file:
    with open(dynamoDB_folder + table_name + '.json', 'w') as f:
        json.dump(table_items, f, indent=1)

