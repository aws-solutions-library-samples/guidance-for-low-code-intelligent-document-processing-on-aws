from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
import aws_cdk.custom_resources as custom_resources
from aws_cdk import CfnOutput, RemovalPolicy, Stack, Duration, CustomResource, Aws
import amazon_textract_idp_cdk_constructs as tcdk


class LendingWorkflow(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope,
            construct_id,
            description="IDP CDK constructs sample for Textract AnalyzeLending and \
                subsequent CSV generation with extraction path for AnalzyeLending unknown doc-types (SO9217)",
            **kwargs,
        )

        script_location = os.path.dirname(__file__)
        s3_upload_prefix = "uploads"
        s3_output_prefix = "textract-output"
        s3_temp_output_prefix = "textract-temp-output"
        s3_txt_output_prefix = "textract-txt-output"
        s3_csv_output_prefix = "textract-csv-output"

        # BEWARE! This is a demo/POC setup, remove the auto_delete_objects=True
        # to maintain objects/document after destorying the stack
        document_bucket = s3.Bucket(
            self,
            "TextractSimpleSyncWorkflow",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        s3_output_bucket = document_bucket.bucket_name
        workflow_name = "LendingWorkflow"

        # DOCUMENT TYPE CONFIGURATION SETUP ####
        # Configuration table for Document-Type to Textract features mapping
        configuration_table = dynamodb.Table(
            self,
            "TextractConfigurationTable",
            partition_key=dynamodb.Attribute(
                name="DOCUMENT_TYPE", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        configuration_init_function: lambda_.IFunction = lambda_.DockerImageFunction(  # type: ignore
            self,
            "ConfigurationInitFunction",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(
                    script_location, "../lambda/cfn_custom_configurator_prefill"
                )
            ),
            memory_size=128,
            timeout=Duration.seconds(600),
            architecture=lambda_.Architecture.X86_64,
            environment={
                "LOG_LEVEL": "DEBUG",
                "CONFIGURATION_TABLE": configuration_table.table_name,
            },
        )

        configuration_init_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:PutItem", "dynamodb:GetItem"],
                resources=[configuration_table.table_arn],
            )
        )

        provider = custom_resources.Provider(
            self,
            "Provider",
            on_event_handler=configuration_init_function,
        )

        CustomResource(self, "Resource", service_token=provider.service_token)

        # END OF DOCUMENT TYPE CONFIGURATION SETUP ####
        decider_task = tcdk.TextractPOCDecider(
            self,
            f"{workflow_name}-Decider",
        )

        textract_lending_task = tcdk.TextractGenericAsyncSfnTask(
            self,
            "TextractAnalzyeLending",
            s3_output_bucket=s3_output_bucket,
            s3_temp_output_prefix=s3_temp_output_prefix,
            textract_api="LENDING",
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

        textract_lending_to_json = tcdk.TextractAsyncToJSON(
            self,
            "GenerateLendingJSON",
            s3_output_prefix=s3_output_prefix,
            s3_output_bucket=s3_output_bucket,
            textract_api="LENDING",
        )

        generate_lending_csv = tcdk.TextractGenerateCSV(
            self,
            "GenerateLendingCSV",
            csv_s3_output_bucket=document_bucket.bucket_name,
            csv_s3_output_prefix=s3_csv_output_prefix,
            output_type="CSV",
            textract_api="LENDING",
            meta_data_to_append=["DOCUMENT_ID"],
            lambda_log_level="DEBUG",
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            input=sfn.TaskInput.from_object(
                {
                    "Token": sfn.JsonPath.task_token,
                    "ExecutionId": sfn.JsonPath.string_at("$$.Execution.Id"),
                    "Payload": sfn.JsonPath.entire_payload,
                }
            ),
            result_path="$.txt_output_location",
        )

        textract_sync_task = tcdk.TextractGenericSyncSfnTask(
            self,
            "TextractSyncOCR",
            s3_output_bucket=s3_output_bucket,
            s3_output_prefix=s3_output_prefix,
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

        generate_text = tcdk.TextractGenerateCSV(
            self,
            "GenerateText",
            csv_s3_output_bucket=document_bucket.bucket_name,
            csv_s3_output_prefix=s3_txt_output_prefix,
            output_type="LINES",
            lambda_log_level="DEBUG",
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            input=sfn.TaskInput.from_object(
                {
                    "Token": sfn.JsonPath.task_token,
                    "ExecutionId": sfn.JsonPath.string_at("$$.Execution.Id"),
                    "Payload": sfn.JsonPath.entire_payload,
                }
            ),
            result_path="$.txt_output_location",
        )

        classification_custom_docker: lambda_.IFunction = lambda_.DockerImageFunction(  # type: ignore
            self,
            "ClassificationCustomDocker",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(
                    script_location, "../lambda/lending_sample_classification/"
                )
            ),
            memory_size=10240,
            architecture=lambda_.Architecture.X86_64,
            timeout=Duration.seconds(900),
            environment={"LOG_LEVEL": "DEBUG"},
        )

        spacy_classification_task = tcdk.SpacySfnTask(
            self,
            "Classification",
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            docker_image_function=classification_custom_docker,
            lambda_log_level="DEBUG",
            timeout=Duration.hours(24),
            input=sfn.TaskInput.from_object(
                {
                    "Token": sfn.JsonPath.task_token,
                    "ExecutionId": sfn.JsonPath.string_at("$$.Execution.Id"),
                    "Payload": sfn.JsonPath.entire_payload,
                }
            ),
            result_path="$.classification",
        )

        configurator_task = tcdk.TextractClassificationConfigurator(
            self,
            f"{workflow_name}-Configurator",
            configuration_table=configuration_table,
        )

        textract_queries_async_task = tcdk.TextractGenericAsyncSfnTask(
            self,
            "TextractAsyncQueries",
            s3_output_bucket=s3_output_bucket,
            s3_temp_output_prefix=s3_output_prefix,
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
            "TextractAsyncToJSON2",
            s3_output_prefix=s3_output_prefix,
            s3_output_bucket=s3_output_bucket,
        )

        generate_csv = tcdk.TextractGenerateCSV(
            self,
            "GenerateCsvTask",
            csv_s3_output_bucket=document_bucket.bucket_name,
            csv_s3_output_prefix=s3_csv_output_prefix,
            lambda_log_level="DEBUG",
            output_type="CSV",
            meta_data_to_append=["DOCUMENT_ID"],
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            input=sfn.TaskInput.from_object(
                {
                    "Token": sfn.JsonPath.task_token,
                    "ExecutionId": sfn.JsonPath.string_at("$$.Execution.Id"),
                    "Payload": sfn.JsonPath.entire_payload,
                }
            ),
            result_path="$.csv_output_location",
        )

        unclassified_lambda_sfn: lambda_.IFunction = lambda_.DockerImageFunction(  # type: ignore
            self,
            "LendingUnclassifiedNumber",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, "../lambda/lending-unclassified")
            ),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
            environment={"LOG_LEVEL": "DEBUG"},
        )

        unclassified_lambda_sfn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:List*"],
                resources=[
                    f"arn:aws:s3:::{s3_output_bucket}",
                    f"arn:aws:s3:::{s3_output_bucket}/*",
                ],
            )
        )

        unclassified_lambda_task = tasks.LambdaInvoke(
            self,
            "UnclassifiedNumberTask",
            lambda_function=unclassified_lambda_sfn,
            output_path="$.Payload",
        )

        # The set_meta_data_function sets the document_id, so the process can later add that to the CSV output
        set_meta_data_function: lambda_.IFunction = lambda_.DockerImageFunction(  # type: ignore
            self,
            "SetMetaDataFunction",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, "../lambda/set-manifest-meta-data")
            ),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
            environment={"LOG_LEVEL": "ERROR"},
        )

        set_meta_data_task = tasks.LambdaInvoke(
            self,
            "SetMetaData",
            lambda_function=set_meta_data_function,
            output_path="$.Payload",
        )

        lambda_generate_classification_mapping: lambda_.IFunction = lambda_.DockerImageFunction(  # type: ignore
            self,
            "LambdaGenerateClassificationMapping",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, "../lambda/map_classifications_lambda/")
            ),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
            environment={},
        )

        task_generate_classification_mapping = tasks.LambdaInvoke(
            self,
            "TaskGenerateClassificationMapping",
            lambda_function=lambda_generate_classification_mapping,
            output_path="$.Payload",
        )

        analyze_lending_post_processing_chain = textract_lending_to_json.next(
            generate_lending_csv
        )

        doc_type_choice = (
            sfn.Choice(self, "RouteDocType")
            .when(
                sfn.Condition.string_equals("$.classification.documentType", "NONE"),
                task_generate_classification_mapping,
            )
            .when(
                sfn.Condition.string_equals(
                    "$.classification.documentType", "CONTACT_FORM"
                ),
                configurator_task,
            )
            .when(
                sfn.Condition.string_equals(
                    "$.classification.documentType", "HOMEOWNERS_INSURANCE_APPLICATION"
                ),
                configurator_task,
            )
            .otherwise(task_generate_classification_mapping)
        )

        configurator_task.next(textract_queries_async_task).next(
            textract_async_to_json
        ).next(generate_csv).next(task_generate_classification_mapping)

        textract_sync_task.next(generate_text).next(spacy_classification_task).next(
            doc_type_choice
        )

        map = sfn.Map(
            self,
            "Unclassified Documents Map State",
            items_path=sfn.JsonPath.string_at("$.unclassifiedDocsArray"),
            parameters={
                "manifest": {
                    "s3Path": sfn.JsonPath.string_at(
                        "States.Format('s3://{}/{}/{}', \
                  $.unclassifiedDocsBucket, \
                  $.unclassifiedDocsPrefix, \
                  $$.Map.Item.Value)"
                    ),
                    "metaData": sfn.JsonPath.string_at("$.manifest.metaData"),
                },
                "mime": sfn.JsonPath.string_at("$.mime"),
                "numberOfPages": 1,
            },
        )
        map.iterator(textract_sync_task)

        unclassified_chain = sfn.Chain.start(unclassified_lambda_task).next(map)

        # unclassified_state_machine = sfn.StateMachine(
        #     self, "UnclassifiedStateMachine", definition_body=sfn.DefinitionBody.from_chainable(unclassified_chain))

        # unclassified_task = tasks.StepFunctionsStartExecution(
        #     self,
        #     "UnclassifiedProcessing",
        #     state_machine=unclassified_state_machine,
        #     integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
        #     input=sfn.TaskInput.from_object({
        #         "Token":
        #         sfn.JsonPath.task_token,
        #         "ExecutionId":
        #         sfn.JsonPath.string_at('$$.Execution.Id'),
        #         "Payload":
        #         sfn.JsonPath.entire_payload,
        #     }))

        parallel_tasks = (
            sfn.Parallel(self, "parallel")
            .branch(analyze_lending_post_processing_chain)
            .branch(unclassified_chain)
        )

        workflow_chain = (
            sfn.Chain.start(set_meta_data_task)
            .next(decider_task)
            .next(textract_lending_task)
            .next(parallel_tasks)
        )

        # GENERIC
        state_machine = sfn.StateMachine(
            self,
            workflow_name,
            definition_body=sfn.DefinitionBody.from_chainable(workflow_chain),
        )

        lambda_step_start_step_function = lambda_.DockerImageFunction(
            self,
            "LambdaStartStepFunctionGeneric",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, "../lambda/startstepfunction")
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
            export_name=f"{Aws.STACK_NAME}-DocumentUploadLocation",
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
            export_name=f"{Aws.STACK_NAME}-StepFunctionFlowLink",
        )
