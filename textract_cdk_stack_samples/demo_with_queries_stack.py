from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration)
import amazon_textract_idp_cdk_constructs as tcdk


class DemoQueries(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        script_location = os.path.dirname(__file__)
        s3_upload_prefix = "uploads"
        s3_output_prefix = "textract-output"
        s3_csv_output_prefix = "csv-output"
        s3_temp_output_prefix = "textract-temp-output"

        # BEWARE! This is a demo/POC setup, remote the auto_delete_objects=True and
        document_bucket = s3.Bucket(self,
                                    "TextratcQueries",
                                    auto_delete_objects=True,
                                    removal_policy=RemovalPolicy.DESTROY)
        s3_output_bucket = document_bucket.bucket_name
        workflow_name = "Queries"

        decider_task = tcdk.TextractPOCDecider(
            self,
            f"{workflow_name}-Decider",
        )

        textract_sync_task = tcdk.TextractGenericSyncSfnTask(
            self,
            "TextractSync",
            s3_output_bucket=document_bucket.bucket_name,
            s3_output_prefix=s3_output_prefix,
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

        textract_async_task = tcdk.TextractGenericAsyncSfnTask(
            self,
            "TextractAsync",
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
            "TextractAsyncToJSON2",
            s3_output_prefix=s3_output_prefix,
            s3_output_bucket=s3_output_bucket)

        lambda_random_function = lambda_.DockerImageFunction(
            self,
            "RandomIntFunction",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/random_number')),
            memory_size=128)

        task_random_number = tasks.LambdaInvoke(
            self,
            'Randomize',
            lambda_function=lambda_random_function,  #type: ignore
            timeout=Duration.seconds(900),
            payload_response_only=True,
            result_path='$.Random')

        generate_csv = tcdk.TextractGenerateCSV(
            self,
            "GenerateCsvTask",
            csv_s3_output_bucket=document_bucket.bucket_name,
            csv_s3_output_prefix=s3_csv_output_prefix,
            lambda_log_level="DEBUG",
            output_type='CSV',
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            input=sfn.TaskInput.from_object({
                "Token":
                sfn.JsonPath.task_token,
                "ExecutionId":
                sfn.JsonPath.string_at('$$.Execution.Id'),
                "Payload":
                sfn.JsonPath.entire_payload,
            }),
            result_path="$.csv_output_location")

        async_chain = sfn.Chain.start(textract_async_task).next(
            textract_async_to_json)

        random_choice = sfn.Choice(self, 'Choice') \
                           .when(sfn.Condition.number_greater_than('$.Random.randomNumber', 50), async_chain)\
                           .otherwise(textract_sync_task)

        textract_sync_task.next(generate_csv)
        async_chain.next(generate_csv)

        workflow_chain = sfn.Chain \
            .start(decider_task) \
            .next(task_random_number) \
            .next(random_choice) \

        # GENERIC
        state_machine = sfn.StateMachine(self,
                                         f'Queries',
                                         definition=workflow_chain)

        lambda_step_start_step_function = lambda_.DockerImageFunction(
            self,
            "LambdaStartStepFunctionGeneric",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/start_queries')),
            memory_size=128,
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
            "DocumentUploadLocation",
            value=f"s3://{document_bucket.bucket_name}/{s3_upload_prefix}/")
        CfnOutput(self,
                  "StateMachineARN",
                  value=textract_sync_task.state_machine.state_machine_arn)
        CfnOutput(
            self,
            "StartStepFunctionLambdaLogGroup",
            value=lambda_step_start_step_function.log_group.log_group_name)
        current_region = Stack.of(self).region
        CfnOutput(
            self,
            'StepFunctionFlowLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}"
        )
