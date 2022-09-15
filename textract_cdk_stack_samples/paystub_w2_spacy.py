from constructs import Construct
import os
import typing
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_rds as rds
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration)
import amazon_textract_idp_cdk_constructs as tcdk


class PaystubAndW2Spacy(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        script_location = os.path.dirname(__file__)
        s3_upload_prefix = "uploads"
        s3_output_prefix = "textract-output"
        s3_temp_output_prefix = "textract-temp-output"
        s3_csv_output_prefix = "csv_output"
        s3_txt_output_prefix = "txt_output"

        # VPC
        # vpc = ec2.Vpc.from_lookup(self, 'defaultVPC', is_default=True)

        vpc = ec2.Vpc(self, "Vpc", cidr="10.0.0.0/16")

        # BEWARE! This is a demo/POC setup, remove the auto_delete_objects=True and
        document_bucket = s3.Bucket(self,
                                    "S3PaystubW2",
                                    auto_delete_objects=True,
                                    removal_policy=RemovalPolicy.DESTROY)
        s3_output_bucket = document_bucket.bucket_name
        workflow_name = "SpacyDemoIDP"

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

        spacy_classification_task = tcdk.SpacySfnTask(
            self,
            "Classification",
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

        csv_to_aurora_task = tcdk.CSVToAuroraTask(
            self,
            "CsvToAurora",
            vpc=vpc,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            lambda_log_level="DEBUG",
            timeout=Duration.hours(24),
            input=sfn.TaskInput.from_object({
                "Token":
                sfn.JsonPath.task_token,
                "ExecutionId":
                sfn.JsonPath.string_at('$$.Execution.Id'),
                "Payload":
                sfn.JsonPath.entire_payload
            }),
            result_path="$.textract_result")

        lambda_random_function = lambda_.DockerImageFunction(
            self,
            "RandomIntFunction",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/random_number')),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64)

        task_random_number = tasks.LambdaInvoke(
            self,
            'Randomize',
            lambda_function=lambda_random_function,  #type: ignore
            timeout=Duration.seconds(900),
            payload_response_only=True,
            result_path='$.Random')

        textract_sync_task_id2 = tcdk.TextractGenericSyncSfnTask(
            self,
            "TextractSyncID2",
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

        textract_sync_task_expense2 = tcdk.TextractGenericSyncSfnTask(
            self,
            "TextractSyncExpense2",
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

        task_random_number2 = tasks.LambdaInvoke(
            self,
            'Randomize2',
            lambda_function=lambda_random_function,  #type: ignore
            timeout=Duration.seconds(900),
            payload_response_only=True,
            result_path='$.Random')

        # EC2 to access the DB
        # sg = ec2.SecurityGroup(self, 'SSH', vpc=vpc, allow_all_outbound=True)
        # sg.add_ingress_rule(ec2.Peer.prefix_list('somelist'),
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
        #     {'us-east-1': "someAMI"})
        # ec2_db_bastion = ec2.Instance(
        #     self,
        #     'DbBastion',
        #     instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3,
        #                                       ec2.InstanceSize.XLARGE),
        #     machine_image=mine,
        #     security_group=sg,
        #     key_name='somekey',
        #     role=instance_role,  #type: ignore
        #     vpc=vpc,
        #     vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC))

        # ec2_db_bastion.add_security_group(
        #     typing.cast(rds.ServerlessCluster, csv_to_aurora_task.db_cluster).
        #     _security_groups[0])  #pyright: ignore [reportOptionalMemberAccess]

        async_chain = sfn.Chain.start(textract_async_task).next(
            textract_async_to_json)

        async_chain_with_config = sfn.Chain.start(
            textract_async_task_with_config).next(
                textract_async_with_config_to_json)

        random_choice = sfn.Choice(self, 'Choice') \
                           .when(sfn.Condition.number_greater_than('$.Random.randomNumber', 50), async_chain)\
                           .otherwise(textract_sync_task)

        random_choice2 = sfn.Choice(self, 'Choice2') \
                           .when(sfn.Condition.number_greater_than('$.Random.randomNumber', 50), async_chain_with_config)\
                           .otherwise(textract_sync_task_with_config)

        doc_type_choice = sfn.Choice(self, 'RouteDocType') \
                           .when(sfn.Condition.string_equals('$.classification.documentType', 'NONE'), sfn.Pass(self, 'DocumentTypeNotClear'))\
                           .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_OTHER'), sfn.Pass(self, 'SendToOpenSearch'))\
                           .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_ID'), textract_sync_task_id2) \
                           .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_EXPENSE'), textract_sync_task_expense2) \
                           .otherwise(configurator_task)

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
        generate_csv.next(csv_to_aurora_task)

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
        CfnOutput(self,
                  "SpacyCallLambdaLogGroup",
                  value=spacy_classification_task.spacy_sync_lambda_log_group.
                  log_group_name)
        CfnOutput(self,
                  "TextractSyncLambdaLogGroup",
                  value=textract_sync_task.textract_sync_lambda_log_group.
                  log_group_name)
        CfnOutput(self,
                  "TextractSyncWithConfigLambdaLogGroup",
                  value=textract_sync_task_with_config.
                  textract_sync_lambda_log_group.log_group_name)
        # CfnOutput(self,
        #           "DashboardLink",
        #           valu e=textract_sync_task.dashboard_name)
        CfnOutput(self,
                  "StateMachineARN",
                  value=textract_sync_task.state_machine.state_machine_arn)
        CfnOutput(self,
                  "CSVtoAuroraLambdaLogGroup",
                  value=csv_to_aurora_task.csv_to_aurora_lambda_log_group.
                  log_group_name)
        CfnOutput(self,
                  "GenerateCSVLambdaLogGroup",
                  value=generate_text.generate_csv_log_group.log_group_name)
        CfnOutput(
            self,
            "StartStepFunctionLambdaLogGroup",
            value=lambda_step_start_step_function.log_group.log_group_name)
        # CfnOutput(self,
        #           "ConfiguratorTable",
        #           value=configurator_task.configuration_table.table_name)
        CfnOutput(self,
                  "ConfiguratorFunctionArn",
                  value=configurator_task.configurator_function.function_arn)
        CfnOutput(self,
                  "ConfiguratorFunctionLogGroup",
                  value=configurator_task.configurator_function_log_group_name)
        CfnOutput(self,
                  "DBClusterARN",
                  value=csv_to_aurora_task.db_cluster.cluster_arn)
        CfnOutput(self,
                  "DBClusterSecretARN",
                  value=typing.cast(rds.ServerlessCluster,
                                    csv_to_aurora_task.db_cluster).secret.
                  secret_arn)  #pyright: ignore [reportOptionalMemberAccess]
        CfnOutput(self,
                  "DBClusterEndpoint",
                  value=typing.cast(
                      rds.ServerlessCluster,
                      csv_to_aurora_task.db_cluster).cluster_endpoint.hostname
                  )  #pyright: ignore [reportOptionalMemberAccess]
        CfnOutput(
            self,
            "DBClusterSecurityGroup",
            value=typing.cast(
                rds.ServerlessCluster,
                csv_to_aurora_task.db_cluster)._security_groups[0].
            security_group_id)  #pyright: ignore [reportOptionalMemberAccess]

        # CfnOutput(self,
        #           "EC2_DB_BASTION_PUBLIC_DNS",
        #           value=ec2_db_bastion.instance_public_dns_name
        #           )  #pyright: ignore [reportOptionalMemberAccess]
        current_region = Stack.of(self).region
        CfnOutput(
            self,
            'StepFunctionFlowLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}"
        )
        CfnOutput(self,
                  'ClassifictionConfiguration',
                  value=configurator_task.configuration_table_name)
