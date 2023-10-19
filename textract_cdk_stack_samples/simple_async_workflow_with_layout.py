from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
from aws_cdk import CfnOutput, Stack, Duration, CfnParameter
import amazon_textract_idp_cdk_constructs as tcdk


class SimpleAsyncWorkflowLayout(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope,
            construct_id,
            description="IDP CDK constructs sample for Textract Asynchronous APIs (SO9217)",
            **kwargs,
        )

        script_location = os.path.dirname(__file__)
        s3_upload_prefix = "uploads"
        s3_output_prefix = "textract-output"
        s3_temp_output_prefix = "textract-temp-output"

        s3_output_bucket = CfnParameter(
            self,
            "uploadBucketName",
            type="String",
            description="S3 bucket where uploaded files will be stored.",
        )

        # We adopt the bucket created from the outer Cloudformation Stack for SageMaker Studio
        document_bucket = s3.Bucket.from_bucket_name(
            self,
            "TextractSimpleAsyncWorkflow",
            bucket_name=s3_output_bucket.value_as_string,
        )
        workflow_name = "SimpleAsyncWorkflow"

        decider_task = tcdk.TextractPOCDecider(
            self,
            f"{workflow_name}-Decider",
        )

        textract_async_task = tcdk.TextractGenericAsyncSfnTask(
            self,
            "TextractAsync",
            s3_output_bucket=s3_output_bucket.value_as_string,
            s3_temp_output_prefix=s3_temp_output_prefix,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            lambda_log_level="DEBUG",
            timeout=Duration.hours(24),
            input=sfn.TaskInput.from_object(
                {
                    "Token": sfn.JsonPath.task_token,
                    "ExecutionId": sfn.JsonPath.string_at("$$.Execution.Id"),
                    "Payload": sfn.JsonPath.entire_payload,
                }
            ),
            result_path="$.textract_result",
        )

        textract_async_to_json = tcdk.TextractAsyncToJSON(
            self,
            "AsyncToJSON",
            lambda_memory_mb=1024,
            s3_output_prefix=s3_output_prefix,
            s3_output_bucket=s3_output_bucket.value_as_string,
        )

        # TBD Linearizer
        # textract_linearized_layout = tcdk.TextractAsyncJSONToLayout(
        #     self,
        #     "AsyncToJSON",
        #     lambda_memory_mb=1024,
        #     s3_output_prefix=s3_output_prefix,
        #     s3_output_bucket=s3_output_bucket)

        async_chain = sfn.Chain.start(textract_async_task).next(
            textract_async_to_json
        )  # .next(textract_linearized_layout)

        workflow_chain = sfn.Chain.start(decider_task).next(async_chain)

        # GENERIC
        state_machine = sfn.StateMachine(self, workflow_name, definition=workflow_chain)

        lambda_step_start_step_function = lambda_.DockerImageFunction(
            self,
            "LambdaStartStepFunctionGeneric",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, "../lambda/start_with_all_features")
            ),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
            environment={"STATE_MACHINE_ARN": state_machine.state_machine_arn},
        )

        lambda_step_start_step_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[state_machine.state_machine_arn],
            )
        )

        document_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(lambda_step_start_step_function),  # type: ignore
            s3.NotificationKeyFilter(prefix=s3_upload_prefix),
        )

        # OUTPUT
        CfnOutput(
            self,
            "DocumentUploadLocation",
            value=f"s3://{document_bucket.bucket_name}/{s3_upload_prefix}/",
        )
        CfnOutput(
            self,
            "StartStepFunctionLambdaLogGroup",
            value=lambda_step_start_step_function.log_group.log_group_name,
        )
        current_region = Stack.of(self).region
        CfnOutput(
            self,
            "StepFunctionFlowLink",
            value=f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}",  # noqa: E501
        )
