from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration)
import amazon_textract_idp_cdk_constructs as tcdk


class AnalyzeIDStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        script_location = os.path.dirname(__file__)
        s3_id_prefix = "id-uploads"
        s3_output_prefix = "textract-output"

        # BEWARE! This is a demo/POC setup, remote the auto_delete_objects=True and
        document_bucket = s3.Bucket(
            self,
            "AnalyzeIDBucket",
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY)
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

        lambda_step_start_step_function_id = lambda_.DockerImageFunction(
            self,
            "LambdaStartStepFunctionID",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/startstepfunction')),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
            environment={"STATE_MACHINE_ARN": state_machine.state_machine_arn})

        lambda_step_start_step_function_id.add_to_role_policy(
            iam.PolicyStatement(actions=['states:StartExecution'],
                                resources=[state_machine.state_machine_arn]))

        document_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(
                lambda_step_start_step_function_id),  #type: ignore
            s3.NotificationKeyFilter(prefix=s3_id_prefix))

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
