import os
import json

import boto3

SQS_QUEUE_NAME_DOWNSTREAM = os.environ["SQS_QUEUE_NAME_DOWNSTREAM"]
SQS_RESOURCE = boto3.resource("sqs")
SQS_QUEUE = SQS_RESOURCE.get_queue_by_name(QueueName=SQS_QUEUE_NAME_DOWNSTREAM)


def lambda_handler(event, context) -> None:
    """Writes to DynamoDB that do not cause change to record will not show up in DynamoDB Streams.
    DynamoDB Stream is truly CDC.
    """
    assert (
        len(event["Records"]) == 1
    ), f"Should only be a batch size of 1 message but got {(event['Records'])}"
    record = event["Records"][0]
    event_name = record["eventName"]
    if event_name == "MODIFY":
        SQS_QUEUE.send_message(MessageBody=json.dumps(record))
    elif event_name == "INSERT":
        raise ValueError(f"INSERT should not happen. Record is {record}")
    elif event_name == "REMOVE":
        raise ValueError(f"REMOVE should not happen. Record is {record}")
    else:
        raise ValueError(f"Got {event_name}, which was unexpected. Record is {record}")
    # how many times does Lambda retry DynamoDB stream and will it move onto next record?
    # put messages into SQS
