from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_lambda_event_sources as eventsources
import aws_cdk.aws_stepfunctions as sfn
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration)
import amazon_textract_idp_cdk_constructs as tcdk


class AnalyzeIDStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope,
            construct_id,
            description=
            "IDP CDK constructs sample for Textract AnalyzeID (SO9217)",
            **kwargs)

        script_location = os.path.dirname(__file__)
        s3_id_prefix = "id-uploads"
        s3_output_prefix = "textract-output"

        # BEWARE! This is a demo/POC setup, remote the auto_delete_objects=True and
        document_bucket = s3.Bucket(self,
                                    "SchademCdkIdpStackPaystubW2",
                                    auto_delete_objects=True,
                                    removal_policy=RemovalPolicy.DESTROY)

        s3_event_source = eventsources.S3EventSource(
            document_bucket,
            events=[s3.EventType.OBJECT_CREATED],
            filters=[s3.NotificationKeyFilter(prefix=s3_id_prefix)])

        # ID
        decider_task_id = tcdk.TextractPOCDecider(
            self,
            f"IDDecider",
        )

        textract_sync_task_id = tcdk.TextractGenericSyncSfnTask(
            self,
            "TextractSyncID",
            s3_output_bucket=document_bucket.bucket_name,
            s3_output_prefix=s3_output_prefix,
            textract_api="ANALYZEID",
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            lambda_log_level="DEBUG",
            timeout=Duration.hours(24),
            input=sfn.TaskInput.from_object({
                "Token":
                sfn.JsonPath.task_token,
                "ExecutionId":
                sfn.JsonPath.string_at('$$.Execution.Id'),
                "Payload":
                sfn.JsonPath.entire_payload,
            }),
            result_path="$.textract_result")

        workflow_chain_id = sfn.Chain \
            .start(decider_task_id) \
            .next(textract_sync_task_id)

        state_machine = sfn.StateMachine(self,
                                         f'AnalyzeID',
                                         definition=workflow_chain_id)

        # The StartThrottle triggers based on event_source (in this case S3 OBJECT_CREATED)
        # and handles all the complexity of making sure the limits or bottlenecks are not exceeded
        tcdk.SFExecutionsStartThrottle(
            self,
            "ExecutionThrottle",
            state_machine_arn=state_machine.state_machine_arn,
            executions_concurrency_threshold=550,
            sqs_batch=10,
            lambda_log_level="ERROR",
            event_source=[s3_event_source])

        # OUTPUT
        CfnOutput(self,
                  "IDDocumentUploadLocation",
                  value=f"s3://{document_bucket.bucket_name}/{s3_id_prefix}/")
        current_region = Stack.of(self).region
        CfnOutput(
            self,
            'StepFunctionFlowLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}"
        )
