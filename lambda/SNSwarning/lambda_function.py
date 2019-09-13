import json
import boto3

# To easily use this function in other lambdas, add the following function there:
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
     InvocationType='Event',
     Payload=json.dumps(event))



def lambda_handler(event, context):
    """
    This function posts to Slack's sistemas channel the message
    from the function, both listed in event.
    """
    # Get input:
    function = event['function']
    message  = event['message']
    
    # Create a dict with metadata and the slack message as a list inside a key:
    payload = dict(
        casa = "sys",
        descricao_post= 'Warning da AWS',
        media={'type':    'slack',
               'channel': 'sistemas'},
        data=[{"function": function, "message": message}]
        )
    # Create a json from the structure above:
    json_payload = json.dumps(payload, ensure_ascii=False)

    # Publish to SNS:
    sns = boto3.client('sns')
    sns.publish(TopicArn='arn:aws:sns:us-east-1:085250262607:slack-test',
                Message=json_payload)

