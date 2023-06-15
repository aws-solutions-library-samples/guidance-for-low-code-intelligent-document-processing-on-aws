from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration)
import amazon_textract_idp_cdk_constructs as tcdk


class AnalyzeExpenseStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope,
            construct_id,
            description=
            "IDP CDK constructs sample for Textract AnalyzeExpense (SO9217)",
            **kwargs)

        script_location = os.path.dirname(__file__)
        s3_expense_prefix = "expense-uploads"
        s3_output_prefix = "textract-output"

        # BEWARE! This is a demo/POC setup, remote the auto_delete_objects=True and
        document_bucket = s3.Bucket(
            self,
            "SchademCdkIdpStackPaystubW2",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL)

        # EXPENSE
        decider_task_expense = tcdk.TextractPOCDecider(
            self,
            f"ExpenseDecider",
        )

        textract_sync_task_expense = tcdk.TextractGenericSyncSfnTask(
            self,
            "TextractSyncExpense",
            s3_output_bucket=document_bucket.bucket_name,
            s3_output_prefix=s3_output_prefix,
            textract_api="EXPENSE",
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

        workflow_chain_expense = sfn.Chain \
            .start(decider_task_expense) \
            .next(textract_sync_task_expense)

        state_machine = sfn.StateMachine(self,
                                         f'Expense',
                                         definition=workflow_chain_expense)

        lambda_step_start_step_function_expense = lambda_.DockerImageFunction(
            self,
            "LambdaStartStepFunctionExpense",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/startstepfunction')),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
            environment={"STATE_MACHINE_ARN": state_machine.state_machine_arn})

        lambda_step_start_step_function_expense.add_to_role_policy(
            iam.PolicyStatement(actions=['states:StartExecution'],
                                resources=[state_machine.state_machine_arn]))

        document_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(
                lambda_step_start_step_function_expense),  #type: ignore
            s3.NotificationKeyFilter(prefix=s3_expense_prefix))

        # OUTPUT
        CfnOutput(
            self,
            "ExpenseDocumentUploadLocation",
            value=f"s3://{document_bucket.bucket_name}/{s3_expense_prefix}/")
        current_region = Stack.of(self).region
        CfnOutput(
            self,
            'StepFunctionFlowLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}"
        )
