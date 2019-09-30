import json
from google.cloud import storage
from google.cloud import bigquery
import boto3
import os
from bigquery_schema_generator.generate_schema import SchemaGenerator

# For debugging:
debug = False

# Get authentication key for google cloud:
client = boto3.client('s3')
a = client.get_object(
                  Bucket='config-lambda', 
                  Key='layers/google-cloud-storage/gabinete-compartilhado.json')
open('/tmp/key.json', 'w').write(a['Body'].read().decode('utf-8'))

# Start clients for google cloud services:
bq = bigquery.Client(project='gabinete-compartilhado')
storage = storage.Client(project='gabinete-compartilhado')

# Temp files to create schema:
RAW_DATA = '/tmp/raw.json'
SCHEMA =   '/tmp/schema.json'


def add_bigquery(table_name, table_path, dataset_name, schema):
    """
    Given a dataset name and a table_name (in bigquery), create
    a table in bigquery with schema given in 'schema' and 
    the data stored in Google Storage path 'table_path'.
    """
    
    ds = bq.dataset(dataset_name)
    
    # Create dataset if non-existent:
    try:
        bq.create_dataset(ds)
    except:
        pass
    
    # Create a local object (bigQuery table):
    table_ref = ds.table(table_name)
    table = bigquery.Table(table_ref)
    
    # Configure the bigquery table:
    external_config = bigquery.ExternalConfig('NEWLINE_DELIMITED_JSON')
    external_config.schema = schema
    # external_config.autodetect = True
    external_config.ignore_unknown_values = True
    external_config.max_bad_records = 100
    source_uris = [table_path] 
    external_config.source_uris = source_uris
    table.external_data_configuration = external_config
    
    # create table (first delete existent table):
    try:
        bq.delete_table(table)
    except Exception as e:
        print(e)
        pass
    
    bq.create_table(table)
    print('Table Cr')


def save_raw_data_to_local(bucket, prefix):
    """
    Save the first 100 entries in the database to a temp file
    RAW_DATA to use it to build a schema for the data. 
    The data is loaded from AWS S3's bucket and prefix.
    """
    print(bucket, prefix)
    
    # Open temp file and save the first 100 items in it:
    open(RAW_DATA, 'w').write('')
    # print(client.list_objects_v2(Bucket=bucket,Prefix=prefix))
    for i, obj in enumerate(client.list_objects_v2(Bucket=bucket,
                                                  Prefix=prefix)['Contents']):
        if i > 100:
            print('breaking')
            break
    
        if debug:
            print(obj)
        
        a = client.get_object(Bucket=bucket, Key=obj['Key'])['Body'].read().decode()
        open(RAW_DATA, 'a+').write(a + '\n')


def generate_schema(replace_time_types=True, extra_types=[]):
    """
    Generate BigQuery schema by first using BigQuery SchemaGenerator and 
    then only keeping TIME and related types, besides extra_types passed to
    the function. Everything else is set to string.
    """
    generator = SchemaGenerator()
    schema_map, error_logs = generator.deduce_schema(open(RAW_DATA, 'r'))
    schema = generator.flatten_schema(schema_map)

    
    if replace_time_types:
        time_types = ['TIMESTAMP', 'DATE', 'TIME',] + extra_types

        for column in schema:
            # if column['type'] == 'RECORD'
            if column['type'] in time_types:
                column['type'] = 'STRING'
                
    print(schema)
    
    return list(map(lambda x: bigquery.SchemaField.from_api_repr(x), schema))


def lambda_handler(event, context):
    
    # Pre-save first few data entries to a tempo file to create the schema afterwards:
    save_raw_data_to_local(event['bucket_name'], event['prefix'])
    # Create the schema:
    schema = generate_schema(extra_types=event['extra_types'])
    # Add to bigquery:
    add_bigquery(event['name'], event['path'], event['dataset_name'], schema)