import boto3
import json

def lambda_handler(event, context):
    
    client = boto3.client('lambda')
    
    # Start capture of camara data:
    dynamo_table = 'capture_urls'
    capture_type = 'monthly'
    begins_with = 'camara'
    response = client.invoke(
            FunctionName='call-step-functions',
            InvocationType='Event',
            Payload= json.dumps({'dynamo_table':dynamo_table, 'capture_type': capture_type,
                     'begins_with': begins_with})
        )
    print('Triggered '+begins_with+' with response:')
    print(response)
        
    # Start capture of senado data:
    dynamo_table = 'capture_urls'
    capture_type = 'monthly'
    begins_with = 'senado'
    response = client.invoke(
            FunctionName='call-step-functions',
            InvocationType='Event',
            Payload= json.dumps({'dynamo_table':dynamo_table, 
                'capture_type': capture_type,
                'begins_with': begins_with})
        )
    print('Triggered '+begins_with+' with response:')
    print(response)

    # Start capture of executivo data:
    dynamo_table = 'capture_urls'
    capture_type = 'monthly'
    begins_with = 'executivo'
    response = client.invoke(
            FunctionName='call-step-functions',
            InvocationType='Event',
            Payload= json.dumps({'dynamo_table':dynamo_table, 
                'capture_type': capture_type,
                'begins_with': begins_with})
        )
    print('Triggered '+begins_with+' with response:')
    print(response)
