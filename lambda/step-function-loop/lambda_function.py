import json

def lambda_handler(event, context):
    
    params = event['params']
    
    print(params)
    
    params['order'] = int(params['order']) - 1 
    
    return params