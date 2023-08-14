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
        UpdateExpression="SET amount = amount - :payment",
        ExpressionAttributeValues={":payment": record["payment"]}
        # UpdateExpression="SET attribute2 = :attribute2, new_attribute= :new_attribute",
        # ExpressionAttributeValues={":attribute2": “another_string”, ":new_attribute": “yet_another_string”}
    )  # update expression can be SET, REMOVE; it appears if UpdateExpression contains a variable not in ExpressionAttribute, then it might be a noop with no error

    # figure out if wrong value in ExpressionAttributeValues
    # if payment is negative or weird, raise a problem
