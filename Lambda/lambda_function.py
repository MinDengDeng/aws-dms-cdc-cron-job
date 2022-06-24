import json
import boto3
import os
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
historyTable = dynamodb.Table(os.environ['dynamodb_table'])

def add_dynamodb_item(task_arn, event_time):
    print("save item to dynamodb")
    print(historyTable)
    historyTable.put_item(
        Item={
            'taskArn': task_arn,
            'scheduledTime': event_time
        }
    )
    return

def start_replication_task(dms_tasks_list, event_time):
    client = boto3.client('dms')
    for _task in dms_tasks_list:
        _task = _task.strip()
        _replication_task = client.describe_replication_tasks(
                Filters=[
                    {
                        'Name': 'replication-task-arn',
                        'Values': [_task]
                    },
                ],
                WithoutSettings=True|False
            )
        _load_type = _replication_task['ReplicationTasks'][0]['MigrationType']
        _status = _replication_task['ReplicationTasks'][0]['Status']
        print(_task)
        print(_load_type)
        print(_status)
        if _load_type != 'full-load':
            continue
        if _status == 'ready':
            response = client.start_replication_task(
                ReplicationTaskArn=_task,
                StartReplicationTaskType='start-replication'
            )
            add_dynamodb_item(_task, event_time)
        elif _status == 'stopped':
            response = client.start_replication_task(
                ReplicationTaskArn=_task,
                StartReplicationTaskType='reload-target'
            )
            add_dynamodb_item(_task, event_time)
        else:
            continue
    return

def lambda_handler(event, context):
    print("Cron job trigger")
    print(event)
    _dms_tasks_list = os.environ['dms_tasks'].split(',')
    _event_time = event['time']
    start_replication_task(_dms_tasks_list, _event_time)

    return {
        'statusCode': 200,
        'body': json.dumps('StartReplicationTask Successfully!')
    }
