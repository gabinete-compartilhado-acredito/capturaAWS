import json
from google.cloud import storage
import boto3

client = boto3.client('s3')
a = client.get_object(
                  Bucket='config-lambda', 
                  Key='layers/google-cloud-storage/gabinete-compartilhado.json')
open('/tmp/key.json', 'w').write(a['Body'].read().decode('utf-8'))

def get_file_s3(bucket, key):
    
    return client.get_object(
            Bucket=bucket,
            Key=key
        )['Body'].read().decode('utf-8')
        
def upload_to_storage_gcp(bucket, key, data):
    
    storage_client = storage.Client(project='gabinete-compartilhado')
    
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(key)
    
    blob.upload_from_string(data)   

def lambda_handler(event, context):
    
    print(event)
    
    bucket = event['bucket']
    key = event['key']
    
  
    data = get_file_s3(bucket, key)
    upload_to_storage_gcp(bucket, key, data)

    return event

