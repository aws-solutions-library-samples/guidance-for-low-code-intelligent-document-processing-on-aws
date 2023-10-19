from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
from aws_cdk import CfnOutput, RemovalPolicy, Stack, Duration
import amazon_textract_idp_cdk_constructs as tcdk


class SubTaskWorkflow(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        script_location = os.path.dirname(__file__)
        s3_upload_prefix = "uploads"
        s3_output_prefix = "textract-output"
        # s3_txt_output_prefix = "textract-text-output"
        s3_temp_output_prefix = "textract-temp"

        # BEWARE! This is a demo/POC setup, remove the auto_delete_objects=True and
        document_bucket = s3.Bucket(
            self,
            "SubTaskWorkflowBucket",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        s3_output_bucket = document_bucket.bucket_name
        workflow_name = "SubTaskWorkflow"

        decider_task = tcdk.TextractPOCDecider(
            self,
            f"{workflow_name}-Decider",
        )

        document_splitter_task = tcdk.DocumentSplitter(
            self,
            "DocumentSplitter",
            s3_output_bucket=s3_output_bucket,
            s3_output_prefix=s3_output_prefix,
        )

        lambda_random_function = lambda_.DockerImageFunction(
            self,
            "RandomIntFunction",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, "../lambda/random_number")
            ),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
        )

        task_random_number = tasks.LambdaInvoke(
            self,
            "Randomize",
            lambda_function=lambda_random_function,  # type: ignore
            payload_response_only=True,
            result_path="$.Random",
        )

        textract_sync_task = tcdk.TextractGenericSyncSfnTask(
            self,
            "TextractSyncOCR",
            s3_output_bucket=s3_output_bucket,
            s3_output_prefix=s3_output_prefix,
            textract_async_call_max_retries=100000,
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

        textract_async_task = tcdk.TextractGenericAsyncSfnTask(
            self,
            "TextractAsync",
            s3_output_bucket=s3_output_bucket,
            s3_temp_output_prefix=s3_temp_output_prefix,
            textract_async_call_max_retries=100000,
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

        # generate_text = tcdk.TextractGenerateCSV(
        #     self,
        #     "GenerateText",
        #     csv_s3_output_bucket=document_bucket.bucket_name,
        #     csv_s3_output_prefix=s3_txt_output_prefix,
        #     output_type='LINES',
        #     lambda_log_level="DEBUG",
        #     integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
        #     input=sfn.TaskInput.from_object({
        #         "Token":
        #         sfn.JsonPath.task_token,
        #         "ExecutionId":
        #         sfn.JsonPath.string_at('$$.Execution.Id'),
        #         "Payload":
        #         sfn.JsonPath.entire_payload,
        #     }),
        #     result_path="$.txt_output_location")

        # vpc = ec2.Vpc(self,
        #               "Vpc",
        #               ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"))

        lambda_opensearch_mapping: lambda_.IFunction = lambda_.DockerImageFunction(  # type: ignore
            self,
            "LambdaOpenSearchMapping",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, "../lambda/map_opensearch_lambda/")
            ),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
            environment={},
        )
        task_opensearch_mapping = tasks.LambdaInvoke(
            self,
            "TaskOpenSearchMapping",
            lambda_function=lambda_opensearch_mapping,
            output_path="$.Payload",
        )

        # EC2 to access the DB
        # sg = ec2.SecurityGroup(self, 'SSH', vpc=vpc, allow_all_outbound=True)
        # sg.add_ingress_rule(ec2.Peer.prefix_list('<some-prefix>'),
        #                     ec2.Port.tcp(22))

        # instance_role = iam.Role(
        #     self,
        #     'RdsDataRole',
        #     assumed_by=iam.ServicePrincipal('ec2.amazonaws.com'),
        #     managed_policies=[
        #         iam.ManagedPolicy.from_aws_managed_policy_name(
        #             'AmazonRDSDataFullAccess')
        #     ])

        # mine = ec2.MachineImage.generic_linux(
        #     {'us-east-1': "<some-ec2-instance>"})
        # ec2_db_bastion = ec2.Instance(
        #     self,
        #     'DbBastion',
        #     instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3,
        #                                       ec2.InstanceSize.XLARGE),
        #     machine_image=mine,
        #     security_group=sg,
        #     key_name='<some-key-name>',
        #     role=instance_role,  #type: ignore
        #     vpc=vpc,
        #     vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC))

        # ec2_db_bastion.add_security_group(
        # .cast(rds.ServerlessCluster, csv_to_aurora_task.db_cluster).
        #     _security_groups[0])  #pyright: ignore [reportOptionalMemberAccess]

        async_chain = sfn.Chain.start(textract_async_task).next(textract_async_to_json)

        random_choice = (
            sfn.Choice(self, "Choice")
            .when(
                sfn.Condition.number_greater_than("$.Payload.Random.randomNumber", 50),
                async_chain,
            )
            .otherwise(textract_sync_task)
        )
        textract_async_to_json.next(task_opensearch_mapping)
        textract_sync_task.next(task_opensearch_mapping)

        generate_text_chain = sfn.Chain.start(random_choice)

        map = sfn.Map(
            self,
            "Map State",
            items_path=sfn.JsonPath.string_at("$.pages"),
            parameters={
                "manifest": {
                    "s3Path": sfn.JsonPath.string_at(
                        "States.Format('s3://{}/{}/{}', \
                  $.documentSplitterS3OutputBucket, \
                  $.documentSplitterS3OutputPath, \
                  $$.Map.Item.Value)"
                    )
                },
                "mime": sfn.JsonPath.string_at("$.mime"),
                "numberOfPages": 1,
            },
        )

        map.iterator(task_random_number)

        # Define a state machine with one Pass state
        child = sfn.StateMachine(
            self,
            "ChildStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(
                sfn.Chain.start(generate_text_chain)
            ),
        )

        # the Choice up to GenerateText are encapsulated
        task = tasks.StepFunctionsStartExecution(
            self,
            "ChildTask",
            state_machine=child,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            input=sfn.TaskInput.from_object(
                {
                    "token": sfn.JsonPath.task_token,
                    "Payload": sfn.JsonPath.entire_payload,
                }
            ),
        )

        task_random_number.next(task)

        workflow_chain = (
            sfn.Chain.start(decider_task)
            .next(set_meta_data_task)
            .next(document_splitter_task)
            .next(map)
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
            value=f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}",   # noqa: E501
        )
        # CfnOutput(self,
        #           "EC2_DB_BASTION_PUBLIC_DNS",
        #           value=ec2_db_bastion.instance_public_dns_name
        #           )  #pyright: ignore [reportOptionalMemberAccess]
