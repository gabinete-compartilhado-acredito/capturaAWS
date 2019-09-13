import json
import boto3

def lambda_handler(event, context):
    
    dynamodb = boto3.resource('dynamodb')
    
    print(event)
    
    dynamodb.Table(event['dynamo_table_name']).delete()
    
    return event
