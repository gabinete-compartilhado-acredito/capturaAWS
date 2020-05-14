import boto3
import json

def lambda_handler(event, context):
    
    client = boto3.client('lambda')
    
    capture_list = [
        {'dynamo_table': 'python_process', 'capture_type': 'daily_9am', 'begins_with': 'dou'}]
    
    for capture_dict in capture_list:
        dynamo_table = capture_dict['dynamo_table']
        capture_type = capture_dict['capture_type']
        begins_with  = capture_dict['begins_with']
    
        #print(dynamo_table, capture_type, begins_with)
    
        response = client.invoke(
                FunctionName='call-step-functions',
                InvocationType='Event',
                Payload= json.dumps({'dynamo_table':dynamo_table, 'capture_type': capture_type,
                         'begins_with': begins_with})
            )
    

