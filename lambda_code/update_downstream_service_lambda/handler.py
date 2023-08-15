import json


def lambda_handler(event, context) -> None:
    assert (
        len(event["Records"]) == 1
    ), f"Should only be a batch size of 1 message but got {(event['Records'])}"
    record = event["Records"][0]
    if record["eventName"] == "MODIFY":
        print(record)
    elif record["eventName"] == "INSERT":
        raise Exception("INSERT should not happen")
    elif record["eventName"] == "REMOVE":
        raise Exception("REMOVE should not happen")
    else:
        raise Exception(f"Got {record['eventName']}, which was unexpected")
    # how many times does Lambda retry DynamoDB stream and will it move onto next record?
    # put messages into SQS
