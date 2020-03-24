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

debug = True


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
            print('Load input data from S3...')
        input_file = save_to_tmp(config['input_data']['bucket'], config['input_data']['key'])
        input_data = pd.read_csv(input_file)
    # Athena:
    elif config['input_data']['type'] == 'athena':
        if debug:
            print('Load input data from Athena...')
        input_data = athena_to_pandas(config['input_data']['query'], config['input_data']['query_cols'])

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
    output_metadata = {'now': pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%S')}
    output_key = config['output_data']['key'] % output_metadata

    if config['output_data']['type'] == 's3_gcp_file' or config['output_data']['type'] == 's3_file':
        if debug:
            print('Save file to S3...')
        save_to_s3(config['output_data']['bucket'], output_key, njson)

    if config['output_data']['type'] == 's3_gcp_file':
        if debug:
            print('Copy data from S3 to GCP storage...')
        copy_s3_to_storage_gcp(config['output_data']['bucket'], output_key)