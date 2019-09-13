import boto3
import json

def lambda_handler(event, context):
    
    client = boto3.client('lambda')
    
    capture_type = 'live'
    begins_with = 'camara'

    response = client.invoke(
            FunctionName='call-step-functions',
            InvocationType='Event',
            Payload= json.dumps({'capture_type': capture_type,
                     'begins_with': begins_with})
                     
        )
        
    capture_type = 'live'
    begins_with = 'senado'

    response = client.invoke(
            FunctionName='call-step-functions',
            InvocationType='Event',
            Payload= json.dumps({
                'capture_type': capture_type,
                'begins_with': begins_with})
                     
        )

