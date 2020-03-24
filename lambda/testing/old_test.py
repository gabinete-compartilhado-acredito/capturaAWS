import json
import boto3

def SNSwarning(function, message):
    """
    Given two strings as input parameters: 
    -- function (the name of the function where the message originated from)
    -- message  (the message)
    Post the function's name and message to SNS, which in turn send it to Slack's channel sistemas. 
    """
    event = {"function": function, "message":  message}
    # Call SNSwarning lambda to post warning:
    lambd = boto3.client('lambda')
    lambd.invoke(
     FunctionName='arn:aws:lambda:us-east-1:085250262607:function:SNSwarning:JustLambda',
     #FunctionName='arn:aws:lambda:us-east-1:085250262607:function:SNSwarning:DEV',
     InvocationType='Event',
     Payload=json.dumps(event))


def lambda_handler(event, context):
    d = {'a':'one entry', 'b':'another entry'}
    SNSwarning('testing', 'Chamando SNSwarning:JustLambda')    
