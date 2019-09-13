import boto3
import sys
from datetime import datetime, timedelta
import calendar
import json
import gc
from pprint import pprint
dynamo = boto3.client('dynamodb')
ses = boto3.client('ses')
sns = boto3.client('sns')
email_from = 'e_ariel@hotmail.es'
email_to = 'earielli@itshell.org'
email_cc = 'e_ariel@hotmail.es'
emaiL_subject = 'Dynamodb backup sucessful in the  '
email_body = 'Dynamodb backup sucessful in the  '
current_time = datetime.now()
def make_backup(name):
    try:
        response = dynamo.create_backup(
            TableName=name,
            BackupName=name+'_bkp_'+ '%s-%s-%s_%s.%s.%s' % (current_time.year, current_time.month, current_time.day,current_time.hour, current_time.minute, current_time.second)
            #time.strftime("%Y%m%d%H")
        )
        print(response)
        return 0
    except Exception as e:
        print(e)
        sys.exit("Se produjo un error realizando el paso backup!")
        
def delete_backup(name):
    try:
        print("Deleting")
        print(current_time)
        print(current_time - timedelta(days=7))
    
        check = dynamo.list_backups(
            TableName=name,
            Limit=100,
            TimeRangeUpperBound = current_time - timedelta(days=1)
            #TimeRangeLowerBound = datetime(2015, 1, 1)
        )
        
        print(check)
        for backup in check['BackupSummaries']:
            arn = backup['BackupArn']
            print("ARN to delete: "+arn)
            deletedArn = dynamo.delete_backup(
                BackupArn=arn
            )
            print(deletedArn['BackupDescription']['BackupDetails']['BackupStatus'])
    except:
        sys.exit("Se produjo un error realizando el paso de limpieza de backups viejos!!")

# response is a function for dynamodb this develop the backup of table.
def lambda_handler(event, context):
    
    event['TableName'] = 'capture_urls'
    if make_backup(event['TableName']) == 0:
        delete_backup(event['TableName'])