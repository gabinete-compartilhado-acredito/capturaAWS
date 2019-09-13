import boto3

def lambda_handler(event, a):
    table_filter = 'temp-cap'
    dynamo = boto3.client('dynamodb')
    tables = dynamo.list_tables()['TableNames']
    tables = [t for t in tables if table_filter in t]
    print(len(tables))
    if len(tables) > 5:
        for t in tables:
            try:
                dynamo.delete_table(TableName=t)
            except:
                continue