from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
)
from constructs import Construct

class DataPipelineStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # S3 Bucket for storing data
        data_bucket = s3.Bucket(self, "AutomationBucket")

        # SQS Queue for triggering report generation
        report_queue = sqs.Queue(self, "ReportQueue")

        # Lambda function for data scraping
        data_scraper_lambda = _lambda.Function(
            self, "DataScraperLambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="data_scraper.handler",
            code=_lambda.Code.from_asset("lambda", bundling={
                'image': _lambda.Runtime.PYTHON_3_8.bundling_image,
                'command': [
                    "bash", "-c",
                    "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                ]
            }),
            environment={
                "BUCKET_NAME": data_bucket.bucket_name,
            }
        )

        # Lambda function for report generation
        report_generator_lambda = _lambda.Function(
            self, "ReportGeneratorLambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="report_generator.handler",
            code=_lambda.Code.from_asset("lambda", bundling={
                'image': _lambda.Runtime.PYTHON_3_8.bundling_image,
                'command': [
                    "bash", "-c",
                    "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                ]
            }),
            environment={
                "BUCKET_NAME": data_bucket.bucket_name,
                "QUEUE_URL": report_queue.queue_url,
            }
        )

        # Grant permissions
        data_bucket.grant_read_write(data_scraper_lambda)
        data_bucket.grant_read_write(report_generator_lambda)
        report_queue.grant_consume_messages(report_generator_lambda)

        # Event triggers
        data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(report_queue)
        )

        # CloudWatch event rule for daily data scraping
        daily_trigger = events.Rule(
            self, "DailyTrigger",
            schedule=events.Schedule.rate(Duration.days(1))
        )
        daily_trigger.add_target(targets.LambdaFunction(data_scraper_lambda))
