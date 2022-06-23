import json
import boto3
import os

def lambda_handler(event, context):
    print("Cron job trigger")
    # print time and trigger type

    _dms_tasks_list = os.environ['dms_tasks'].split(',')

    client = boto3.client('dms')
    for _task in _dms_tasks_list:
        # retrieve the replication task's status
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
        elif _status == 'stopped':
            response = client.start_replication_task(
                ReplicationTaskArn=_task,
                StartReplicationTaskType='reload-target'
            )
        else:
            continue

    return {
        'statusCode': 200,
        'body': json.dumps('StartReplicationTask Successfully!')
    }
