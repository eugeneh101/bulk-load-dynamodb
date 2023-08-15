import json
import os

import boto3

DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
DYNAMODB_RESOURCE = boto3.resource("dynamodb")
DYNAMODB_TABLE = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE_NAME)


def lambda_handler(event, context) -> None:
    assert (
        len(event["Records"]) == 1
    ), f"Should only be a batch size of 1 message but got {(event['Records'])}"
    record = json.loads(event["Records"][0]["body"])
    DYNAMODB_TABLE.update_item(
        Key={"pk": record["pk"], "sk": record["sk"]},
        UpdateExpression="SET amount = amount - :payment, payment = if_not_exists(payment, :zero) + :payment",
        ExpressionAttributeValues={":payment": record["payment"], ":zero": 0},
    )
    # if payment is negative or weird, raise a problem such as thru DLQ or metric
