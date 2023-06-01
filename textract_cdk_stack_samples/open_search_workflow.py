from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
import amazon_textract_idp_cdk_constructs as tcdk
from uuid import uuid4
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration)
from aws_solutions_constructs.aws_lambda_opensearch import LambdaToOpenSearch


class OpenSearchWorkflow(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        script_location = os.path.dirname(__file__)
        s3_upload_prefix = "uploads"
        s3_output_prefix = "textract-output"
        s3_txt_output_prefix = "textract-text-output"
        s3_temp_output_prefix = "textract-temp"

        # BEWARE! This is a demo/POC setup, remove the auto_delete_objects=True and
        document_bucket = s3.Bucket(self,
                                    "OpenSearchWorkflowBucket",
                                    auto_delete_objects=True,
                                    removal_policy=RemovalPolicy.DESTROY)
        s3_output_bucket = document_bucket.bucket_name
        workflow_name = "OpenSearchWorkflow"
        current_region = Stack.of(self).region

        decider_task = tcdk.TextractPOCDecider(
            self,
            f"{workflow_name}-Decider",
        )

        document_splitter_task = tcdk.DocumentSplitter(
            self,
            "DocumentSplitter",
            s3_output_bucket=s3_output_bucket,
            s3_output_prefix=s3_output_prefix,
            max_number_of_pages_per_doc=2500,
        )

        textract_async_task = tcdk.TextractGenericAsyncSfnTask(
            self,
            "TextractAsync",
            s3_output_bucket=s3_output_bucket,
            s3_temp_output_prefix=s3_temp_output_prefix,
            textract_async_call_max_retries=100,
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

        generate_open_search_batch = tcdk.TextractGenerateCSV(
            self,
            "GenerateOpenSearchBatch",
            csv_s3_output_bucket=document_bucket.bucket_name,
            csv_s3_output_prefix=s3_txt_output_prefix,
            lambda_memory_mb=10240,
            output_type='OPENSEARCH_BATCH',
            opensearch_index_name='books-index',
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
            result_path="$.opensearch_output_location")

        lambda_opensearch_push = lambda_.DockerImageFunction(  #type: ignore
            self,
            "LambdaOpenSearchPush",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/opensearch_push/')),
            memory_size=10240,
            timeout=Duration.seconds(900),
            architecture=lambda_.Architecture.X86_64,
            environment={})

        lambda_opensearch_push.add_to_role_policy(
            iam.PolicyStatement(actions=['s3:Get*'], resources=["*"]))

        task_lambda_opensearch_push = tasks.LambdaInvoke(
            self,
            'OpenSearchPushInvoke',
            lambda_function=lambda_opensearch_push,  #type: ignore
            timeout=Duration.seconds(900),
            payload_response_only=True,
            result_path='$.OpenSearchPush')

        lambda_to_opensearch = LambdaToOpenSearch(
            self,
            'OpenSearchResources',
            existing_lambda_obj=lambda_opensearch_push,
            open_search_domain_name='idp-cdk-opensearch',
            cognito_domain_name=f"idp-cdk-os-{str(uuid4())}"
            # open_search_domain_props=CfnDomainProps(
            #     cluster_config=opensearch.CfnDomain.ClusterConfigProperty(
            #         instance_type="m5.xlarge.search"), ))
        )

        lambda_opensearch_mapping: lambda_.IFunction = lambda_.DockerImageFunction(  #type: ignore
            self,
            "LambdaOpenSearchMapping",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location,
                             '../lambda/map_opensearch_lambda/')),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
            environment={})

        task_opensearch_mapping = tasks.LambdaInvoke(
            self,
            "TaskOpenSearchMapping",
            lambda_function=lambda_opensearch_mapping,
            output_path='$.Payload')

        # Setting meta-data for the SearchIndex
        set_meta_data_function: lambda_.IFunction = lambda_.DockerImageFunction(  #type: ignore
            self,
            'SetMetaDataFunction',
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location,
                             '../lambda/set-manifest-meta-data-opensearch')),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
            environment={"LOG_LEVEL": "ERROR"})

        set_meta_data_task = tasks.LambdaInvoke(
            self,
            'SetMetaData',
            lambda_function=set_meta_data_function,
            output_path='$.Payload')

        ## Creating the StepFunction workflow
        async_chain = sfn.Chain \
                         .start(textract_async_task) \
                         .next(textract_async_to_json)

        # textract_async_to_json.next(generate_open_search_batch) \
        #     .next(task_opensearch_mapping)
        textract_async_to_json \
            .next(set_meta_data_task) \
            .next(generate_open_search_batch)

        generate_open_search_batch \
            .next(task_lambda_opensearch_push) \
            .next(task_opensearch_mapping)

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
                "originFileURI": sfn.JsonPath.string_at('$.originFileURI')
            })

        map.iterator(async_chain)

        workflow_chain = sfn.Chain \
            .start(decider_task) \
            .next(document_splitter_task) \
            .next(map)

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
            "DocumentUploadLocation",
            value=f"s3://{document_bucket.bucket_name}/{s3_upload_prefix}/")
        CfnOutput(
            self,
            'StepFunctionFlowLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}"
        )
        CfnOutput(
            self,
            'OpenSearchDashboard',
            value=
            f"https://{lambda_to_opensearch.open_search_domain.attr_domain_endpoint}/states/_dashboards"
        )
        CfnOutput(
            self,
            'OpenSearchLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/aos/home?region={current_region}#/opensearch/domains/{lambda_to_opensearch.open_search_domain.domain_name}"
        )
        # CfnOutput(self,
        #           "EC2_DB_BASTION_PUBLIC_DNS",
        #           value=ec2_db_bastion.instance_public_dns_name
        #           )  #pyright: ignore [reportOptionalMemberAccess]
