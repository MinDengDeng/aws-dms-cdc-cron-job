import json
import boto3

def lambda_handler(event, context):
    print("Cron job trigger")
    print(event)
    
    client = boto3.client('dms')
    response = client.start_replication_task(
        ReplicationTaskArn='arn:aws:dms:us-east-1:316498120700:task:Z2PT2WKSXZUGLUL2YFWZC3S54PDHGWYKFKVDJWI',
        StartReplicationTaskType='reload-target'
    )

    return {
        'statusCode': 200,
        'body': json.dumps('StartReplicationTask Successfully!')
    }
