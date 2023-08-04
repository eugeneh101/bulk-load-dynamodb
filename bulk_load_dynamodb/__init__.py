import json

from aws_cdk import (
    BundlingOptions,
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from constructs import Construct


class BulkLoadDynamodbStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.dynamodb_table = dynamodb.Table(
            self,
            "DynamodbTable",
            table_name=environment["DYNAMODB_TABLE_NAME"],
            partition_key=dynamodb.Attribute(
                name="pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.s3_bucket = s3.Bucket(
            self,
            "S3Bucket",
            bucket_name=environment["S3_BUCKET_NAME"],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        data_files = s3_deploy.BucketDeployment(
            self,
            "data_files",
            destination_bucket=self.s3_bucket,
            destination_key_prefix="data/",  # hard coded
            sources=[s3_deploy.Source.asset("./data")],  # hard coded
            prune=True,  ### it seems that delete Lambda uses a different IAM role
            retain_on_delete=False,
            memory_limit=1024,  # need more RAM for large files
        )

        self.split_data_lambda = _lambda.Function(
            self,
            "SplitDataLambda",
            function_name="split_data",  # hard coded
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/split_data_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(60),
            memory_size=1024,
            environment={
                "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                "PARTITION_ROW_COUNT": json.dumps(environment["PARTITION_ROW_COUNT"]),
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )
        self.load_data_lambda = _lambda.Function(
            self,
            "LoadDataLambda",
            function_name="load_data",  # hard coded
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/load_data_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(
                900
            ),  # depends on whether DynamoDB table is cold or warm
            memory_size=1024,
            environment={
                "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                "DYNAMODB_TABLE_NAME": environment["DYNAMODB_TABLE_NAME"],
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )
        self.sort_runtimes_lambda = _lambda.Function(
            self,
            "SortRuntimesLambda",
            function_name="sort_runtimes",  # hard coded
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.InlineCode(
                code="def lambda_handler(event, context): return sorted(event)"
            ),
            handler="index.lambda_handler",
            timeout=Duration.seconds(1),  # should be instantaneous
            log_retention=logs.RetentionDays.ONE_MONTH,
        )

        # build Step Function definition
        split_data = sfn_tasks.LambdaInvoke(
            self,
            "split_data",
            lambda_function=self.split_data_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "key": "data/dynamodb_data_balanced.csv"
                }  # or use data/dynamodb_data_hotkey.csv
            ),
            payload_response_only=True,  # don't want Lambda invocation metadata
            task_timeout=sfn.Timeout.duration(self.split_data_lambda.timeout),
            retry_on_service_exceptions=False,
        )
        load_data = sfn_tasks.LambdaInvoke(
            self,
            "load_data",
            lambda_function=self.load_data_lambda,
            payload=sfn.TaskInput.from_object(
                {"key": sfn.JsonPath.string_at("$")}  # hard coded
            ),
            payload_response_only=True,  # don't want Lambda invocation metadata
            task_timeout=sfn.Timeout.duration(self.load_data_lambda.timeout),
            retry_on_service_exceptions=False,
        )
        sort_runtimes = sfn_tasks.LambdaInvoke(
            self,
            "sort_runtimes",
            lambda_function=self.sort_runtimes_lambda,
            payload_response_only=True,  # don't want Lambda invocation metadata
            task_timeout=sfn.Timeout.duration(self.sort_runtimes_lambda.timeout),
            retry_on_service_exceptions=False,
        )
        map_state = sfn.Map(
            self, "parallel_load_data", max_concurrency=10, items_path="$"
        )
        map_state.iterator(load_data)
        sfn_definition = split_data.next(map_state).next(sort_runtimes)
        self.state_machine = sfn.StateMachine(
            self,
            "parallel-dynamodb-load",
            state_machine_name="parallel-dynamodb-load",
            definition=sfn_definition,
        )

        # connect AWS resources together
        self.s3_bucket.grant_read_write(self.split_data_lambda)
        self.s3_bucket.grant_read_write(self.load_data_lambda)
        self.dynamodb_table.grant_write_data(self.load_data_lambda)