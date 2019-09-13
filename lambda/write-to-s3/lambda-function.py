import boto3

def lambda_handler(event, context):

    client = boto3.client('s3')
    client.put_object(Body=event['body'],
                      Bucket=event['bucket'], 
                      Key=event['key'])
