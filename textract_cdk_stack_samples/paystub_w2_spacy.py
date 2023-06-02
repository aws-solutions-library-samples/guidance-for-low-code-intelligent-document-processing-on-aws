from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ecr as ecr
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration, Aws)
import amazon_textract_idp_cdk_constructs as tcdk
import pathlib


class PaystubAndW2Spacy(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        script_location = os.path.dirname(__file__)
        s3_upload_prefix = "uploads"
        s3_output_prefix = "textract-output"
        s3_temp_output_prefix = "textract-temp-output"
        s3_csv_output_prefix = "csv_output"
        s3_txt_output_prefix = "txt_output"

        current_path = pathlib.Path(__file__).parent.resolve()

        # BEWARE! This is a demo/POC setup, remove the auto_delete_objects=True and
        document_bucket = s3.Bucket(
            self,
            "S3PaystubW2",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY)
        s3_output_bucket = document_bucket.bucket_name
        workflow_name = "PaystubW2"

        decider_task = tcdk.TextractPOCDecider(
            self,
            f"{workflow_name}-Decider",
        )

        configurator_task = tcdk.TextractClassificationConfigurator(
            self,
            f"{workflow_name}-Configurator",
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

        textract_sync_task_with_config = tcdk.TextractGenericSyncSfnTask(
            self,
            "TextractSyncWithConfig",
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

        # Reference to a classification model with AWS_PAYSTUBS, AWS_W2, AWS_ID
        # Example how to use a public container in Lambda (just wrap it in a Dockerfile)
        classification_custom_docker: lambda_.IFunction = lambda_.DockerImageFunction(
            self,
            "ClassificationCustomDocker",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(current_path,
                             '../lambda/classification_docker/')),
            memory_size=10240,
            architecture=lambda_.Architecture.X86_64,
            timeout=Duration.seconds(900),
            environment={"LOG_LEVEL": "DEBUG"})

        spacy_classification_task = tcdk.SpacySfnTask(
            self,
            "Classification",
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            docker_image_function=classification_custom_docker,
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
            result_path="$.classification")

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

        textract_async_task_with_config = tcdk.TextractGenericAsyncSfnTask(
            self,
            "TextractAsyncWithConfig",
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

        textract_async_with_config_to_json = tcdk.TextractAsyncToJSON(
            self,
            "TextractAsyncWithConfigToJSON",
            s3_output_prefix=s3_output_prefix,
            s3_output_bucket=s3_output_bucket)

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

        generate_text = tcdk.TextractGenerateCSV(
            self,
            "GenerateText",
            csv_s3_output_bucket=document_bucket.bucket_name,
            csv_s3_output_prefix=s3_txt_output_prefix,
            output_type='LINES',
            lambda_log_level="DEBUG",
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            input=sfn.TaskInput.from_object({
                "Token":
                sfn.JsonPath.task_token,
                "ExecutionId":
                sfn.JsonPath.string_at('$$.Execution.Id'),
                "Payload":
                sfn.JsonPath.entire_payload,
            }),
            result_path="$.txt_output_location")

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

        task_random_number2 = tasks.LambdaInvoke(
            self,
            'Randomize2',
            lambda_function=lambda_random_function,  #type: ignore
            timeout=Duration.seconds(900),
            payload_response_only=True,
            result_path='$.Random')

        #### ANALYZE ID Task ##################
        # textract_sync_task_id2 = tcdk.TextractGenericSyncSfnTask(
        #     self,
        #     "TextractSyncID2",
        #     s3_output_bucket=document_bucket.bucket_name,
        #     s3_output_prefix=s3_output_prefix,
        #     textract_api="ANALYZEID",
        #     integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
        #     lambda_log_level="DEBUG",
        #     timeout=Duration.hours(24),
        #     input=sfn.TaskInput.from_object({
        #         "Token":
        #         sfn.JsonPath.task_token,
        #         "ExecutionId":
        #         sfn.JsonPath.string_at('$$.Execution.Id'),
        #         "Payload":
        #         sfn.JsonPath.entire_payload,
        #     }),
        #     result_path="$.textract_result")
        #### ANALYZE ID Task ##################

        # Define the Asynchronous flow with calling TextractAsync first, then converting the paginated JSON to one JSON file
        async_chain = sfn.Chain.start(textract_async_task).next(
            textract_async_to_json)

        # Define the Asynchronous flow with calling TextractAsync first, then converting the paginated JSON to one JSON file when calling Textract again with the document-type specific configuration
        async_chain_with_config = sfn.Chain.start(
            textract_async_task_with_config).next(
                textract_async_with_config_to_json)

        # just random choice for value 0-100
        random_choice = sfn.Choice(self, 'Choice') \
                           .when(sfn.Condition.number_greater_than('$.Random.randomNumber', 50), async_chain)\
                           .otherwise(textract_sync_task)

        # just random choice for value 0-100
        random_choice2 = sfn.Choice(self, 'Choice2') \
                           .when(sfn.Condition.number_greater_than('$.Random.randomNumber', 50), async_chain_with_config)\
                           .otherwise(textract_sync_task_with_config)

        # route according to document type
        doc_type_choice = sfn.Choice(self, 'RouteDocType') \
                           .when(sfn.Condition.string_equals('$.classification.documentType', 'NONE'), sfn.Pass(self, 'DocumentTypeNotClear'))\
                           .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_OTHER'), sfn.Pass(self, 'SendToOpenSearch'))\
                           .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_PAYSTUBS'), configurator_task)\
                           .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_W2'), configurator_task)\
                           .otherwise(sfn.Fail(self, "DocumentTypeNotImplemented"))
                           # .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_ID'), textract_sync_task_id2) \

        # route according to number of queries
        number_queries_choice = sfn.Choice(self, 'NumberQueriesChoice') \
            .when(sfn.Condition.and_(sfn.Condition.is_present('$.numberOfQueries'),
                                    sfn.Condition.number_greater_than('$.numberOfQueries', 15)),
                  async_chain_with_config) \
            .when(sfn.Condition.and_(sfn.Condition.is_present('$.numberOfQueries'),
                                    sfn.Condition.number_greater_than('$.numberOfQueries', 30)),
                  sfn.Fail(self, 'TooManyQueries',
                           error="TooManyQueries",
                           cause="Too many queries. > 30. See https://docs.aws.amazon.com/textract/latest/dg/limits.html")) \
            .otherwise(task_random_number2)

        # route according to number of pages (this flow only supports 1 page documents)
        number_pages_choice = sfn.Choice(self, 'NumberPagesChoice') \
            .when(sfn.Condition.and_(sfn.Condition.is_present('$.numberOfPages'),
                                     sfn.Condition.number_greater_than('$.numberOfPages', 1)),
                  sfn.Fail(self, "NumberOfPagesFail", error="NumberOfPagesError", cause="number of pages > 1")) \
            .otherwise(task_random_number)

        textract_sync_task.next(generate_text)
        textract_async_to_json.next(generate_text)
        generate_text.next(spacy_classification_task)
        spacy_classification_task.next(doc_type_choice)
        configurator_task.next(number_queries_choice)
        task_random_number2.next(random_choice2)
        task_random_number.next(random_choice)
        async_chain_with_config.next(generate_csv)
        textract_sync_task_with_config.next(generate_csv)

        workflow_chain = sfn.Chain \
            .start(decider_task) \
            .next(number_pages_choice)

        # GENERIC
        state_machine = sfn.StateMachine(self,
                                         workflow_name,
                                         definition=workflow_chain)

        lambda_step_start_step_function = lambda_.DockerImageFunction(
            self,
            "LambdaStartStepFunctionGeneric",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/startstepfunction')),
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
            value=f"s3://{document_bucket.bucket_name}/{s3_upload_prefix}/",
            export_name=f"{Aws.STACK_NAME}-DocumentUploadLocation")
        current_region = Stack.of(self).region
        CfnOutput(
            self,
            'DynamoDBConfiguratorLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/dynamodbv2/home?region={current_region}#item-explorer?initialTagKey=&table={configurator_task.configuration_table_name}",
            export_name=f"{Aws.STACK_NAME}-DynamoDBConfiguratorLink")
        CfnOutput(
            self,
            'StepFunctionFlowLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}",
            export_name=f"{Aws.STACK_NAME}-StepFunctionFlowLink")
