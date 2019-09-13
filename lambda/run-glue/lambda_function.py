import boto3

def lambda_handler(event, context):

    print(event)

    glue = boto3.client('glue')

    try:
        glue.delete_crawler(
            Name=event['glue-name'],
        )
    except Exception as e:
        print(e)
        pass
    
    glue.create_crawler(
        Name=event['glue-name'],
        Role='glue-titan',
        DatabaseName=event['glue-name'],
        Targets={
            'S3Targets': [
                {
                    'Path': event['glue-path']
                },
            ]}
        )

    glue.start_crawler(Name=event['glue-name'])