from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
import typing
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration, Aws)
import amazon_textract_idp_cdk_constructs as tcdk
import pathlib


class DocumentSplitterWorkflow(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        current_path = pathlib.Path(__file__).parent.resolve()
        
        script_location = os.path.dirname(__file__)
        s3_upload_prefix = "uploads"
        s3_output_prefix = "textract-output"
        s3_txt_output_prefix = "textract-text-output"
        s3_csv_output_prefix = "textract-csv-output"

        # BEWARE! This is a demo/POC setup, remove the auto_delete_objects=True
        # to make sure the data is not lost
        # S3 bucket
        document_bucket = s3.Bucket(
            self,
            "TextractSimpleSyncWorkflow",
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY)
        s3_output_bucket = document_bucket.bucket_name
        workflow_name = "DocumentSplitterWorkflow"

        ##### DEFINE Step Functions tasks ###############
        # Step Functions task to set document mime type and number of pages
        decider_task = tcdk.TextractPOCDecider(
            self,
            f"{workflow_name}-Decider",
        )

        # Step Functions task to split documents into single pages
        document_splitter_task = tcdk.DocumentSplitter(
            self,
            "DocumentSplitter",
            s3_output_bucket=s3_output_bucket,
            s3_output_prefix=s3_output_prefix)

        # Step Functions task to call Textract
        textract_sync_task = tcdk.TextractGenericSyncSfnTask(
            self,
            "TextractSyncOCR",
            s3_output_bucket=s3_output_bucket,
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

        # Step Functions task to generate text file from Textract JSON output
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

        # Classification wrapper around public Docker container for classification
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

        # Step Functions task for classification
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

        # Step Functions task to configure Textract call based on classification result
        configurator_task = tcdk.TextractClassificationConfigurator(
            self,
            f"{workflow_name}-Configurator",
        )

        # Step Functions task to call Textract
        textract_queries_sync_task = tcdk.TextractGenericSyncSfnTask(
            self,
            "TextractSyncQueries",
            s3_output_bucket=s3_output_bucket,
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

        # Generate CSV from Textract JSON
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
            result_path="$.csv_output_location",
        )

        lambda_generate_classification_mapping: lambda_.IFunction = lambda_.DockerImageFunction(
            self,
            "LambdaGenerateClassificationMapping",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location,
                             '../lambda/map_classifications_lambda/')),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
            environment={})
        task_generate_classification_mapping = tasks.LambdaInvoke(
            self,
            "TaskGenerateClassificationMapping",
            lambda_function=lambda_generate_classification_mapping,
            output_path='$.Payload')

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

        #### ANALYZE ID GENERATE CSV ##################
        # analyze_id_generate_csv: lambda_.IFunction = lambda_.DockerImageFunction(
        #     self,
        #     "LambdaAnalzyeIDGenerateCSV",
        #     code=lambda_.DockerImageCode.from_image_asset(
        #         os.path.join(script_location,
        #                      '../lambda/analyze_id_generatecsv/')),
        #     memory_size=1024,
        #     architecture=lambda_.Architecture.X86_64,
        #     environment={
        #         "CSV_S3_OUTPUT_BUCKET": document_bucket.bucket_name,
        #         "CSV_S3_OUTPUT_PREFIX": s3_csv_output_prefix
        #     })
        # analyze_id_generate_csv.add_to_role_policy(
        #     iam.PolicyStatement(actions=['s3:GetObject', 's3:ListObject'],
        #                         resources=[f"arn:aws:s3:::{s3_output_bucket}/*", 
        #                         f"arn:aws:s3:::{s3_output_bucket}/"]))
        # analyze_id_generate_csv.add_to_role_policy(
        #     iam.PolicyStatement(actions=['s3:PutObject'],
        #                         resources=[f"arn:aws:s3:::{s3_output_bucket}/{s3_csv_output_prefix}/*"]))
        # task_analyze_id_generate_csv = tasks.LambdaInvoke(
        #     self,
        #     "TaskAnalzyeIDGenerateCSV",
        #     lambda_function=analyze_id_generate_csv,
        #     output_path='$.Payload')
        #### ANALYZE ID GENERATE CSV ##################
        
        
        #### Step Functions Flow Definition #########
        
        # Routing based on document type
        doc_type_choice = sfn.Choice(self, 'RouteDocType') \
                       .when(sfn.Condition.string_equals('$.classification.documentType', 'NONE'), task_generate_classification_mapping)\
                       .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_OTHER'), task_generate_classification_mapping)\
                       .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_PAYSTUBS'), configurator_task)\
                       .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_W2'), configurator_task)\
                       .otherwise(sfn.Fail(self, "DocumentTypeNotImplemented"))
                    #   .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_ID'), textract_sync_task_id2) \
                    
        # Map state to process pages in parallel
        # Creates manifest 
        # Generates S3 path from S3 Document Splitter Output Bucket and Output Path
        map = sfn.Map(
            self,
            "Map State",
            items_path=sfn.JsonPath.string_at('$.pages'),
            parameters={
                "manifest": {
                    "s3Path":
                    sfn.JsonPath.string_at("States.Format('s3://{}/{}/{}', \
                  $.documentSplitterS3OutputBucket, \
                  $.documentSplitterS3OutputPath, \
                  $$.Map.Item.Value)")
                },
                "mime": sfn.JsonPath.string_at('$.mime'),
                "numberOfPages": 1
            })

        # Classify and route
        textract_sync_task.next(generate_text) \
            .next(spacy_classification_task) \
            .next(doc_type_choice)
            
        ### ANALYZE ID GENERATE CSV
        # textract_sync_task_id2.next(task_analyze_id_generate_csv) \
        #     .next(task_generate_classification_mapping)
        ### ANALYZE ID GENERATE CSV
        
        configurator_task.next(textract_queries_sync_task) \
            .next(generate_csv) \
            .next(task_generate_classification_mapping)

        map.iterator(textract_sync_task)

        workflow_chain = sfn.Chain \
            .start(decider_task) \
            .next(document_splitter_task) \
            .next(map)

        state_machine = sfn.StateMachine(self,
                                         workflow_name,
                                         definition=workflow_chain)
                                         
        ###### Step Functions definition end ###############

        # Lambda function to start workflow on new object at S3 bucket/prefix location
        lambda_step_start_step_function = lambda_.DockerImageFunction(
            self,
            "LambdaStartStepFunctionGeneric",
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

        # CloudFormation OUTPUT
        CfnOutput(
            self,
            "DocumentUploadLocation",
            value=f"s3://{document_bucket.bucket_name}/{s3_upload_prefix}/",
            export_name=f"{Aws.STACK_NAME}-DocumentUploadLocation")
        current_region = Stack.of(self).region
        CfnOutput(
            self,
            'StepFunctionFlowLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}",
            export_name=f"{Aws.STACK_NAME}-StepFunctionFlowLink")
