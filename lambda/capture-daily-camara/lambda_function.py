import boto3
import json

def lambda_handler(event, context):
    
    client = boto3.client('lambda')
    
    # Start capture of camara data:
    capture_type = 'daily'
    begins_with = 'camara'
    response = client.invoke(
            FunctionName='call-step-functions',
            InvocationType='Event',
            Payload= json.dumps({'capture_type': capture_type,
                     'begins_with': begins_with})
        )
    print('Triggered '+begins_with+' with response:')
    print(response)
        
    # Start capture of senado data:    
    capture_type = 'daily'
    begins_with = 'senado'
    response = client.invoke(
            FunctionName='call-step-functions',
            InvocationType='Event',
            Payload= json.dumps({
                'capture_type': capture_type,
                'begins_with': begins_with})
        )
    print('Triggered '+begins_with+' with response:')
    print(response)

    # Start capture of executivo data:
    capture_type = 'daily'
    begins_with = 'executivo'
    response = client.invoke(
            FunctionName='call-step-functions',
            InvocationType='Event',
            Payload= json.dumps({
                'capture_type': capture_type,
                'begins_with': begins_with})
        )
    print('Triggered '+begins_with+' with response:')
    print(response)
