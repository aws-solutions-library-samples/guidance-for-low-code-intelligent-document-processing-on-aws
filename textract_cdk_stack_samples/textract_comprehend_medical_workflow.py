from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
import aws_cdk.aws_healthlake as healthlake
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration)
import amazon_textract_idp_cdk_constructs as tcdk


class TextractComprehendMedicalWorkflow(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope,
            construct_id,
            description=
            "IDP CDK constructs sample for Amazon HealthLake integration (SO9217)",
            **kwargs)

        script_location = os.path.dirname(__file__)

        # BEWARE! This is a demo/POC setup, remove the auto_delete_objects=True and
        document_bucket = s3.Bucket(self,
                                    "TextractComprehendMedicalWorkflow",
                                    auto_delete_objects=True,
                                    removal_policy=RemovalPolicy.DESTROY)
        s3_output_bucket = document_bucket.bucket_name
        comprehend_medical_operations = ['ICD10', 'RXNORM', 'SNOMEDCT', 'DETECT_ENTITIES_V2', 'DETECT_PHI']

        for cm_task in comprehend_medical_operations:
            s3_upload_prefix = f'uploads-{cm_task.lower()}'
            s3_output_prefix = f'textract-output-json-{cm_task.lower()}'
            s3_temp_output_prefix = f'temp-{cm_task.lower()}'
            workflow_name = f'TextractComprehendMedical-{cm_task.lower()}'

            decider_task = tcdk.TextractPOCDecider(
                self,
                f"{workflow_name}-Decider",
            )

            textract_async_task = tcdk.TextractGenericAsyncSfnTask(
                self,
                f'TextractAsync{cm_task}',
                s3_output_bucket=s3_output_bucket,
                s3_temp_output_prefix=s3_temp_output_prefix,
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

            textract_async_to_json = tcdk.TextractAsyncToJSON(
                self,
                f'TextractAsyncToJSON2{cm_task}',
                s3_output_prefix=s3_output_prefix,
                s3_output_bucket=s3_output_bucket)

            comprehendmedical_task = tcdk.TextractComprehendMedical(
                self,
                f'ComprehendMedical{cm_task}',
                comprehend_medical_job_type=cm_task)

            workflow_chain = sfn.Chain \
                .start(decider_task) \
                .next(textract_async_task) \
                .next(textract_async_to_json) \
                .next(comprehendmedical_task)

            # GENERIC
            state_machine = sfn.StateMachine(self,
                                             workflow_name,
                                             definition=workflow_chain)

            lambda_step_start_step_function = lambda_.DockerImageFunction(
                self,
                f'LambdaStartStepFunctionGeneric{cm_task}',
                code=lambda_.DockerImageCode.from_image_asset(
                    os.path.join(script_location, '../lambda/startstepfunction')),
                memory_size=128,
                architecture=lambda_.Architecture.X86_64,
                environment={"STATE_MACHINE_ARN": state_machine.state_machine_arn})

            lambda_step_start_step_function.add_to_role_policy(
                iam.PolicyStatement(actions=['states:StartExecution'],
                                    resources=[state_machine.state_machine_arn]))

            document_bucket.add_event_notification(
                s3.EventType.OBJECT_CREATED,
                s3n.LambdaDestination(
                    lambda_step_start_step_function),  #type: ignore
                s3.NotificationKeyFilter(prefix=s3_upload_prefix))

            # OUTPUT
            CfnOutput(
                self,
                f'DocumentUploadLocation{cm_task}',
                value=f"s3://{document_bucket.bucket_name}/{s3_upload_prefix}/")
            CfnOutput(
                self,
                f'StartStepFunctionLambdaLogGroup{cm_task}',
                value=lambda_step_start_step_function.log_group.log_group_name)
            current_region = Stack.of(self).region
            CfnOutput(
                self,
                f'StepFunctionFlowLink{cm_task}',
                value=
                f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}"
            )
