from dynamodb_json import json_util as dyjson 
from pyathena import connect
from collections import defaultdict
import importlib
import boto3
import joblib
import pandas as pd
import json

# Specific processing modules:
import req_classifier
import dou_sorter_common_functions

debug = True


def brasilia_time():
    """
    No matter where the code is ran, always return the time 
    in Brasilia (UTC -3).
    """
    brasilia_time = pd.Timestamp.now('UTC') - pd.Timedelta(hours=3)
    return brasilia_time


def today_string(fmt='%Y-%m-%d'):
    """
    Return a string with the current day in Brasilia, 
    in format `fmt` (default %Y-%m-%d).
    """
    return brasilia_time().strftime(fmt)


def yesterday_string(fmt='%Y-%m-%d'):
    """
    Return a string with yesterday in Brasilia, 
    in format `fmt` (default %Y-%m-%d).
    """
    return (brasilia_time() - pd.Timedelta(days=1)).strftime(fmt)


def fill_in_tokens(string):
    """
    Replace hard-coded placeholders [e.g. %(var)s] by hard-coded variable
    values in `string`.
    """
    replacements = {'today': today_string(), 'yesterday': yesterday_string()}
    filled = string % replacements
    return filled


def list_fill_in_tokens(string_list):
    """
    Given a list of strings `string_list`, return a list in which
    each string had their placeholders [e.g. %(var)s] replaced 
    by values given by the `fill_in_tokens` function.
    """
    filled = [fill_in_tokens(string) for string in string_list]
    return filled


def list_s3_files(bucket, prefix):
    """
    Returns a list of files in AWS in a given `bucket` and with a given `prefix`.
    """
        
    s3 = boto3.client('s3')

    if type(prefix) != list:
        prefix = [prefix]
    
    # Loop over prefixes:
    file_list = []
    for p in prefix:
        
        # Load one prefix:
        response  = s3.list_objects_v2(Bucket=bucket, Prefix=p)
        if response['KeyCount'] > 0:
            file_list = file_list + [d['Key'] for d in response['Contents']]
            while response['IsTruncated']:
                response  = s3.list_objects_v2(Bucket=bucket, Prefix=p, StartAfter=file_list[-1])
                file_list = file_list + [d['Key'] for d in response['Contents']]    
    
    return file_list


def get_file_s3(bucket, key):
    """
    Retrieve content of file stored in AWS S3.
    
    
    Input
    -----
    
    bucket : str
        The name of the AWS S3 bucket where the file is stored.
        
    key : str
        The full path to the file inside the bucket.
        
    
    Returns
    -------
    
    A string with the file's content.    
    """
    
    client = boto3.client('s3')
    return client.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')


def load_njson(newline_delimited_json):
    """
    Given a string `newline_delimited_json` in newline-delimited 
    JSON format, load it and return a list of dicts.
    """
    data = json.loads('[' + newline_delimited_json.replace('\n', ',') + ']')
    return data


def get_field(key_value_pair_list, key):
    """
    Given a list of key-value pairs (dicts with keys 'key' and 'value'), 
    find the entry that has the provided `key` and return its value.
    
    If no `key` is found, return None.
    
    It assumes that each `key` only appears one in `key_value_pair_list`,
    so the first appearance is returned.
    """
    entry = list(filter(lambda d: d['key'] == key, key_value_pair_list))
    
    if len(entry) == 0:
        return None
    
    return entry[0]['value']


def keyvalue_to_structure(raw_data, key_list):
    """
    Given a list of dicts containing key-value pairs `raw_data`, 
    a list of relevant keys `key_list`, returns a dict where the keys 
    are those in `key_list` and values are given by the values 
    associated to each key.
    """
    structured_data = {}
    for key in key_list:
        structured_data[key] = get_field(raw_data, key)
        
    return structured_data


def s3_file_to_dict_list(bucket, path, key_list=None, honorary_keys=None):
    """
    Load one newline-delimited JSON file stored in S3 as a list of dicts.
    
    Input
    -----
    
    bucket : str 
        S3 data bucket where to look for the data.
    
    path : str
        S3 file key that identifies the file (it looks like a file path).
        
    key_list : list of str or ints (default None)
        If a list is provided here, the function assumes the data 
        in S3 is in the form of key-value pairs (each nJSON line is 
        a dict containing a field called 'key' and another called 
        'value'). Each pair is then converted into an entry in a dict.
        If `None`, does not process the data.
    
    honorary_keys : list of str or ints (default None)
        If a list is provided here, the function looks for extra 
        keys in the nJSON lines other than 'key' and 'value', 
        and copies them to the dict to return. It assumes such 
        keys have the same values in all nJSON lines. It is only
        used if `key_list` != None.
        
    Returns
    -------
    
    structured_data : list of dicts
        The data in S3 nJSON file parsed into a list of dicts.
        If key_list is provided, the list contains only one entry:
        that with the data from the S3 file.
    """
    # Load the content of the file as a string:
    content  = get_file_s3(bucket, path)
    # Transform the content into a list of dicts:
    raw_data = load_njson(content)
    
    # If it is not a key-value pair, return list of dicts:
    if key_list == None:
        structured_data = raw_data
    
    # Else, parse into a dict with selected keys:
    else:
        data_dict = keyvalue_to_structure(raw_data, key_list)
        
        # If there are other entries in raw_data other than 'key' and 'value', extract them here:
        if honorary_keys != None:
            for honorary in honorary_keys:
                data_dict[honorary] = raw_data[0][honorary]
        
        structured_data = [data_dict]
        
    return structured_data


def load_s3_njson(bucket, prefix, key_list, honorary_list):
    """
    Load data stored in AWS S3 as newline-delimited JSON files 
    into a list of dicts.
    
    Check docstring of `s3_file_to_dict_list` for more info.
    """
    # Get list of files in bucket and with prefix:
    s3_file_list = list_s3_files(bucket, prefix)
    
    # Load data from all files:
    structured_data = []
    for s3_file in s3_file_list:
        structured_data = structured_data + s3_file_to_dict_list(bucket, s3_file, key_list, honorary_list)
    
    return structured_data
    

def save_to_tmp(bucket, key, string=True):
    """
    Get a file from AWS S3 and save it to /tmp/ folder.
    
    Input
    -----
    
    bucket : str
        The AWS bucket where the file is stored.
        
    key : str
        The path to the file in the AWS bucket.
    
    string : bool (default True)
        Wheter or not the data in file is text.
    """
    mode = 'w' if string else 'wb'
    
    # Get data:
    s3 = boto3.client('s3')
    a = s3.get_object(Bucket=bucket, Key=key)
    data = a['Body'].read()
    if string:
        data = data.decode('utf-8')
        
    # Save to temp folder:
    filename = '/tmp/' + key.split('/')[-1]
    with open(filename, mode) as f:
        f.write(data)
    
    return filename


def athena_to_pandas(query, query_cols):
    """
    Runs an AWS Athena query and return their results as a Pandas DataFrame.
    
    
    Input
    -----
    
    query : str
        The query to run in Athena.
        
    query_cols : list of str
        The names of the columns returned by Athena.
        
    
    Returns
    -------
    
    df : Pandas DataFrame
        The results of the query, with columns labeled according to `query_cols`.
    """
    
    # Get AWS security credentials:
    s3 = boto3.client('s3')
    a = s3.get_object(Bucket='config-lambda', Key='aws_accessKeys.json')
    aws_key = json.loads(a['Body'].read().decode('utf-8'))

    # Conecta à Athena com pacote do Joe.
    cursor = connect(aws_access_key_id=aws_key['aws_access_key_id'],
                         aws_secret_access_key=aws_key['aws_secret_access_key'],
                         s3_staging_dir='s3://stagging-random/',
                         region_name='us-east-1').cursor()

    # Executa a query:
    data  = cursor.execute(query).fetchall() 
    df    = pd.DataFrame(data, columns=query_cols)
    
    return df


def add_process_date(record):
    """
    Given a dict `record`, add a key 'process_date' with the current time 
    as value.
    """
    process_date = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    record['process_date'] = process_date
    return record


def pandas_to_njson(df):
    """
    Given a Pandas DataFrame `df`, transform it to newline-elimited JSON 
    and add a key 'process_date' with the current date and time as value.
    """
    records   = df.to_dict(orient='records')
    json_list = [json.dumps(add_process_date(record), ensure_ascii=False) for record in records]
    njson     = '\n'.join(json_list)
    
    return njson


def save_to_s3(bucket, key, body):
    """
    Save a string `body` to a file `key` in the AWS S3 `bucket`.
    """
    client = boto3.client('s3')
    s3_log = client.put_object(Body=body, Bucket=bucket, Key=key)
    
    if debug:
        print('s3_log:', s3_log)

        
def copy_s3_to_storage_gcp(bucket, key):
    """
    Inputs:
    - 'bucket': the bucket in which the file is stored in AWS and GCP;
    - 'key': the "file path" of the file.
    
    This function calls the lambda function that copies the file from AWS
    to GCP storage.
    """
    params = {'bucket': bucket, 'key': key}
    
    lambd = boto3.client('lambda')
    
    if debug:
        print('Invoking write-to-storage-gcp...')
    # Order lambda to save this result to storage (Google):
    lambd.invoke(
     FunctionName='arn:aws:lambda:us-east-1:085250262607:function:write-to-storage-gcp:JustLambda',
     InvocationType='Event',
     Payload=json.dumps(params))


def lambda_handler(event, context):
    print(event)
    
    # Seleciona um arquivo do dynamo:
    if debug:
        print('Load processing config...')
    dynamodb = boto3.client('dynamodb')
    response = dynamodb.get_item(TableName=event['table_name'], Key=event['key'])

    # Lê o arquivo do dynamo (retorna uma lista de dicionários ou um dicionário):
    response = dyjson.loads(response)
    if debug == True:
        print("dict of dynamo Table:") 
        print(response)
    config = defaultdict(lambda: None, response['Item'])

    # Load model:
    if debug:
        print('Load code...')
    if config['code'] != None:
        code_file = save_to_tmp(config['code']['bucket'], config['code']['key'], string=False)
        code = joblib.load(code_file)

    # Load data:

    # S3:
    if config['input_data']['type'] == 's3_file':
        if debug:
            print('Load CSV data from S3...')
        input_file = save_to_tmp()
        input_data = pd.read_csv(input_file)

    # Athena:
    elif config['input_data']['type'] == 'athena':
        if debug:
            print('Load input data from Athena...')
        input_data = athena_to_pandas(config['input_data']['query'], config['input_data']['query_cols'])

    # S3 nJSON:
    if config['input_data']['type'] == 's3_njson':
        if debug:
            print('Load nJSON data from S3...')
        key_list        = config['input_data']['key_list']
        honorary_list   = config['input_data']['honorary_list']
        input_data_keys = list_fill_in_tokens(config['input_data']['key'])
        input_data = pd.DataFrame(load_s3_njson(config['input_data']['bucket'], 
                                                input_data_keys, 
                                                key_list, 
                                                honorary_list))
        if 'col_names' in config['input_data']:
            # Rename DataFrame columns:
            mapper = {k:v for k,v in zip(key_list + honorary_list, config['input_data']['col_names']) if k != v}
            input_data = input_data.rename(mapper=mapper, axis=1)

            
    # Exit if no data is found:
    if len(input_data) == 0:
        print('No data retrieved.')
        return 0

    # Process data:
    if debug:
        print('Process data...')
    em = importlib.import_module(config['name'].replace('-', '_'))
    output_data = em.process(code, input_data)


    # Output processed data:
    if debug:
        print('Prepare data for output...')
    njson = pandas_to_njson(output_data)

    output_metadata = {}
    if 'now' in config['output_data']['key_pars']:
        output_metadata['now'] = pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%S')
    output_metadata['today'] = today_string()
    output_metadata['yesterday'] = yesterday_string()

    output_key = config['output_data']['key'] % output_metadata

    if config['output_data']['type'] == 's3_gcp_file' or config['output_data']['type'] == 's3_file':
        if debug:
            print('Save file to S3...')
        save_to_s3(config['output_data']['bucket'], output_key, njson)

    if config['output_data']['type'] == 's3_gcp_file':
        if debug:
            print('Copy data from S3 to GCP storage...')
        copy_s3_to_storage_gcp(config['output_data']['bucket'], output_key)