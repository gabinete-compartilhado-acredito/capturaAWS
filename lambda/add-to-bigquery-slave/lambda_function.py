# TODO:
# - Currently, CSV format is supported only when building schema from local files.
# - The detection of data format (NJSON or CSV) is currently via temp_file extension 
#   that is set when loading a local file. This is bad, it should be an event parameter. 
# - The need to write a header to a CSV temp file is decided by the existence of the 
#   temp file. Not very pleseant for multiple calls in a local machine.

import json
from google.cloud import storage
from google.cloud import bigquery
import boto3
import os
from bigquery_schema_generator.generate_schema import SchemaGenerator

# To run locally (not in AWS):
local = False
# For debugging:
debug = False
# Option from where to get data for schema:
read_from_aws = False

# Get authentication key for google cloud:
client = boto3.client('s3')
a = client.get_object(
                  Bucket='config-lambda', 
                  Key='layers/google-cloud-storage/gabinete-compartilhado.json')
open('/tmp/key.json', 'w').write(a['Body'].read().decode('utf-8'))

def add_bigquery(temp_data, table_name, table_path, dataset_name, schema):
    """
    Given a dataset name and a table_name (in bigquery), create
    a table in bigquery with schema given in 'schema' and 
    the data stored in Google Storage path 'table_path'.
    
    It deduces the file format (NJSON or CSV) from temp_data.
    """

    if local:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/key.json'
    bq = bigquery.Client(project='gabinete-compartilhado')
    
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
    if os.path.exists(temp_data):
        external_config = bigquery.ExternalConfig('NEWLINE_DELIMITED_JSON')
    elif os.path.exists(temp_data.replace('.json', '.csv')):
        external_config = bigquery.ExternalConfig('CSV')
    else:
        raise Exception('unknown temp_data file extension')

    
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

def append_njson_first_lines(temp_data, file, max_bytes):
    """
    Given a fine path 'file' and a maximum number of characters/bytes, 
    writes all lines in 'file' to temp file until the character/byte counts
    surpasses 'max_bytes'.
    """
    
    # Read first lines of file:
    with open(file, 'r') as f:
        records = f.readlines(max_bytes)
    first_records = ''.join(records)
    
    # Append to temp file:
    with open(temp_data, 'a+') as f:
        f.write(first_records)

def append_csv_first_lines(temp_data, file, max_bytes):
    """
    Given a fine path 'file' and a maximum number of characters/bytes, 
    writes all lines in 'file' to temp file until the character/byte counts
    in 'file' surpasses 'max_bytes'. 
    
    PS: It does not write the first line of 'file' to the temp file 
    if the temp file already exists.
    """
    import os.path
    
    # Change file extension:
    temp_data = temp_data.replace('.json', '.csv')
    
    # Check if temp file already exists (if so, we expect it to have a header):
    starting_temp = not os.path.exists(temp_data)
    
    # Read first lines of file (skip header if it already is in temp file):
    with open(file, 'r') as f:
        records = f.readlines(max_bytes)
        if not starting_temp:
            records = records[1:]

    first_records = ''.join(records)
    
    # Append to temp file:
    with open(temp_data, 'a+') as f:
        f.write(first_records)

def append_first_lines(temp_data, file, max_bytes):
    """
    Given a fine path 'file' and a maximum number of characters/bytes, 
    writes all lines in 'file' to temp file until the character/byte counts
    in 'file' surpasses 'max_bytes'. 
    
    It works with csv and newline-delimited jsons, and it is meant to 
    concatenate multiple files of the same type.
    """
    
    if file.split('.')[-1] == 'csv':
        append_csv_first_lines(temp_data, file, max_bytes)
        
    elif file.split('.')[-1] == 'json':
        append_njson_first_lines(temp_data, file, max_bytes)
        
    else:
        raise Exception('Unknown file type.')

def save_raw_data_to_local_GCP(temp_data, bucket, prefix):
    """
    Save the first 100 entries (i.e. files) in the database to a temp file
    to use it to build a schema for the data. 
    The data is loaded from Google Storage's bucket and prefix.
    """

    print(bucket, prefix)
    
    # Open temp file and save the first 100 items in it:
    open(temp_data, 'w').write('')

    if local:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/key.json'
    gcp_storage = storage.Client(project='gabinete-compartilhado')

    b = gcp_storage.get_bucket(bucket)
    blob_iterator = b.list_blobs(prefix=prefix)  
    for i, obj in enumerate(blob_iterator):
        if i > 100:
            print('breaking')
            break
    
        if debug:
            print(obj)
        
        a = obj.download_as_string().decode('utf-8')
        if len(a) > 0:
            open(temp_data, 'a+').write(a + '\n')
        # DEBUG:
        #return a

def save_raw_data_to_local_AWS(temp_data, bucket, prefix):
    """
    Save the first 100 entries (i.e. files) in the database to a temp file
    to use it to build a schema for the data. 
    The data is loaded from AWS S3's bucket and prefix.
    """
    print(bucket, prefix)
    
    # Open temp file and save the first 100 items in it:
    open(temp_data, 'w').write('')
    # print(client.list_objects_v2(Bucket=bucket,Prefix=prefix))
    for i, obj in enumerate(client.list_objects_v2(Bucket=bucket, Prefix=prefix)['Contents']):
        if i > 100:
            print('breaking')
            break
    
        if debug:
            print(obj)
        
        a = client.get_object(Bucket=bucket, Key=obj['Key'])['Body'].read().decode()
        open(temp_data, 'a+').write(a + '\n')
        # DEBUG:
        #return a

def save_raw_data_to_local_local(temp_data, bucket, prefix, max_bytes):
    """
    Save the first 100 files found locally to a temp file
    to use it to build a schema for the data. 
    The data is loaded from file with a given 'prefix'.
    
    PS: It only uses the first lines while 'max_bytes' 
    (characters/bytes read) are not reached.
    """
    from glob import glob

    print(bucket, prefix)

    file_list = glob(prefix + '*')
    for i, file in enumerate(file_list):
        if i > 100:
            print('breaking')
            break

        if debug:
            print(file)

        append_first_lines(temp_data, file, max_bytes)

def save_raw_data_to_local(temp_data, bucket, prefix, max_bytes):
    """
    Save data to local temp file to use it later to build 
    the schema.
    
    If bucket == 'local', read raw data locally. Else, depending 
    on global boolean variable 'read_from_aws', use data 
    stored in AWS to build the schema; otherwise, use data stored 
    in Google storage.
    
    'max_bytes' is only used if reading local files. 
    
    PS: 
    - currently, CSV format is supported only when loading local files.
    - currently, max_bytes is only used with local files.
    """
    
    if bucket == 'local':
        save_raw_data_to_local_local(temp_data, bucket, prefix, max_bytes)

    else:
        if read_from_aws == True:
            save_raw_data_to_local_AWS(temp_data, bucket, prefix)
        else:
            save_raw_data_to_local_GCP(temp_data, bucket, prefix)

def generate_schema(temp_data, replace_time_types=True, extra_types=[]):
    """
    Generate BigQuery schema by first using BigQuery SchemaGenerator and 
    then only keeping TIME and related types, besides extra_types passed to
    the function. Everything else is set to string.
    """

    # Find out what data format to read:
    if os.path.exists(temp_data):
        generator = SchemaGenerator(keep_nulls=True)
    elif os.path.exists(temp_data.replace('.json', '.csv')):
        generator = SchemaGenerator(input_format='csv', keep_nulls=True)
        temp_data = temp_data.replace('.json', '.csv')
    else:
        raise Exception('unknown temp_data file extension')
    
    # Deduce schema:
    with open(temp_data, 'r') as f:
        schema_map, error_logs = generator.deduce_schema(f)
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
    
    RAW_DATA = '/tmp/raw.json'
    
    # Pre-save first few data entries to a tempo file to create the schema afterwards:
    save_raw_data_to_local(RAW_DATA, event['bucket_name'], event['prefix'], event['max_bytes'])
    
    # Create the schema:
    schema = generate_schema(RAW_DATA, extra_types=event['extra_types'])

    # Add to bigquery:
    add_bigquery(RAW_DATA, event['name'], event['path'], event['dataset_name'], schema)
