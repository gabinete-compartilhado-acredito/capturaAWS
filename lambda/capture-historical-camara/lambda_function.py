import boto3
import json

def lambda_handler(event, context):
    
    dynamo_table = 'capture_urls'
    capture_type = 'historical'
    begins_with = 'camara'
    
    client = boto3.client('lambda')

    response = client.invoke(
            FunctionName='call-step-functions',
            InvocationType='Event',
            Payload= json.dumps({'dynamo_table': dynamo_table, 'capture_type': capture_type,
                     'begins_with': begins_with})
                     
        )
    