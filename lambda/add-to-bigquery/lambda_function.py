import json
from google.cloud import storage
from google.cloud import bigquery
import boto3
import os
from bigquery_schema_generator.generate_schema import SchemaGenerator


client = boto3.client('s3')
a = client.get_object(
                  Bucket='config-lambda', 
                  Key='layers/google-cloud-storage/gabinete-compartilhado.json')
open('/tmp/key.json', 'w').write(a['Body'].read().decode('utf-8'))

bq = bigquery.Client(project='gabinete-compartilhado')
storage = storage.Client(project='gabinete-compartilhado')

lambd = boto3.client('lambda')


RAW_DATA = '/tmp/raw.json'
SCHEMA =   '/tmp/schema.json'


def lambda_handler(event, context):
    
    bucket_name = event['bucket-name']
    prefix = event['prefix']
    dataset_name = event['dataset_name']
    
    bucket = storage.get_bucket(bucket_name)
    
    blobs = bucket.list_blobs(prefix=prefix)

    tables = map(lambda x: 
                    {'path': '/'.join(['gs:/',
                              bucket_name, 
                              prefix[:-1], 
                              x.name.split(prefix)[1].split('/')[0],
                              '*']),
                      'prefix': '/'.join([
                              prefix[:-1], 
                              x.name.split(prefix)[1].split('/')[0], '']),
                      'name': x.name.split(prefix)[1].split('/')[0].replace('-', '_')},
                    blobs)
                    
    
    tables = list({v['name']:v for v in tables}.values())
                    
    print(tables)
    
    for t in tables:
        
        
        lambd.invoke(FunctionName='arn:aws:lambda:us-east-1:085250262607:function:add-to-bigquery-slave:PROD',
                     InvocationType='Event',
                     Payload=json.dumps({
                          "bucket_name": bucket_name,
                          "prefix": t['prefix'],
                          "name": t['name'],
                          "path": t['path'],
                          "dataset_name": dataset_name,
                          "extra_types": []
                     }))

       
