import boto3
import json

def invoke_lambda(dynamo_table, capture_type, begins_with):

    client = boto3.client('lambda')
    
    # Call lambda function:
    response = client.invoke(
        FunctionName='call-step-functions',
        InvocationType='Event',
        Payload= json.dumps({'dynamo_table':dynamo_table, 'capture_type': capture_type, 'begins_with': begins_with}))
    
    # Log:
    print(dynamo_table + ': Triggered '+begins_with+' with response:')
    print(response)


def lambda_handler(event, context):
    
    # Seleção de serviços de captura (configurados no DynamoDB) que serão rodados diariamente:
    capture_services = [{'dynamo_table': 'capture_urls', 'capture_type':'daily', 'begins_with':'camara'},
                        {'dynamo_table': 'capture_urls', 'capture_type':'daily', 'begins_with':'senado'},
                        {'dynamo_table': 'capture_urls', 'capture_type':'daily', 'begins_with':'executivo'},
                        {'dynamo_table': 'python_process', 'capture_type':'daily', 'begins_with':''},
                        {'dynamo_table': 'capture_urls', 'capture_type':'daily', 'begins_with':'twitter'}]
    
    # Loop de ativação das capturas listadas acima:
    for service in capture_services:
        invoke_lambda(service['dynamo_table'], service['capture_type'], service['begins_with'])
