import boto3
import json
#import random
import time


def get_parameters(dynamo_table, capture_type, name):
    """
    Selects all dynamoDB items in table `dynamo_table` that have the input 
    'capture_type' and that have the string 'name' in its name.
    """
    dynamodb = boto3.resource('dynamodb')
    table    = dynamodb.Table(dynamo_table)
    response = table.scan()
    filters  = lambda x: (x['capture_type'] == capture_type) & (name in x['name'])

    return list(filter(filters, response['Items']))


def order_by_dependence(parameters):
    """
    Takes a list of parameters from a dynamoDB table and organize them by dependence.
    The output is a list of lists; for each sub-list there is a root parameter 
    (that do not depend on anything) and the parameters that do depend 
    """
    # Selects all table items that does not depend on others:
    roots = [leaf for leaf in parameters if 'dependence' not in leaf.keys() or leaf['dependence']==None]
    # Selects all table items that does depend on others:
    leafs = [leaf for leaf in parameters if 'dependence' in leaf.keys() and leaf['dependence']!=None]

    graphs = []
    for root in roots:
        
        # A graph starts with a root:
        graph = [root]
        branches = [root['name']]
        for leaf in leafs:
            # If a leaf depends on any parameter present in that graph, add it to that graph:
            if leaf['dependence']['name'] in branches:
                graph.append(leaf)
                branches.append(leaf['name'])
        # Put this graph (that starts with a certain root) in the list of graphs:
        graphs.append(graph)
        
    return graphs
        

def lambda_handler(event, context):
    # Get input (basically the data set to capture and the periodicity):
    capture_type = event['capture_type']
    begins_with  = event['begins_with']
    dynamo_table = event['dynamo_table']
    
    lambd      = boto3.client('lambda')
    req_event  = {"table_name": dynamo_table, "key": {"name": {"S": None}, "capture_type": {"S": None}}}
    # Get all the requested table items (here called 'parameters'):
    parameters = get_parameters(dynamo_table, capture_type, begins_with)

    # Loop over all parameters, after ordering them:
    for graph in order_by_dependence(parameters):
        for leaf in graph:

            # Set parametrize-API-requests input:
            req_event['key']['capture_type']['S'] = leaf['capture_type']
            req_event['key']['name']['S'] = leaf['name']
            # Sleep before parametrizing dependences:
            if 'dependence' in leaf.keys() and leaf['dependence'] != None:
                #print('sleeping...')
                time.sleep(int(leaf['dependence']['wait']))

            print (req_event)
            #print('Starting', leaf['name'])
            
            if dynamo_table == 'capture_urls':
                # Call parametrize-API-requests:
                lambd.invoke(
                    FunctionName='arn:aws:lambda:us-east-1:085250262607:function:parametrize-API-requests:JustLambda',
                    #FunctionName='arn:aws:lambda:us-east-1:085250262607:function:parametrize-API-requests:DEV',
                    InvocationType='Event',
                    Payload=json.dumps(req_event))
            
            elif dynamo_table == 'python_process':
                # Call parametrize-API-requests:
                lambd.invoke(
                    FunctionName='arn:aws:lambda:us-east-1:085250262607:function:python-process:PROD',
                    #FunctionName='arn:aws:lambda:us-east-1:085250262607:function:python-process:DEV',
                    InvocationType='Event',
                    Payload=json.dumps(req_event))
                
            else:
                raise Exception('Unknown dynamo Table')
