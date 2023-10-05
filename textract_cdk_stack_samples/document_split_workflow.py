from constructs import Construct
import os
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_rds as rds
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_lambda_event_sources as eventsources
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
import typing
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration)
import amazon_textract_idp_cdk_constructs as tcdk


class DocumentSplitterWorkflow(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope,
            construct_id,
            description=
            "IDP CDK constructs sample for splitting large documents, classifying and extracting informaiton based on the doc type (SO9217)",
            **kwargs)

        script_location = os.path.dirname(__file__)
        s3_upload_prefix = "uploads"
        s3_output_prefix = "textract-output"
        s3_txt_output_prefix = "textract-text-output"
        s3_csv_output_prefix = "textract-csv-output"

        # BEWARE! This is a demo/POC setup, remove the auto_delete_objects=True and
        document_bucket = s3.Bucket(self,
                                    "TextractSimpleSyncWorkflow",
                                    auto_delete_objects=True,
                                    removal_policy=RemovalPolicy.DESTROY)
        s3_output_bucket = document_bucket.bucket_name
        workflow_name = "DocumentSplitterWorkflow"

        decider_task = tcdk.TextractPOCDecider(
            self,
            f"{workflow_name}-Decider",
        )

        document_splitter_task = tcdk.DocumentSplitter(
            self,
            "DocumentSplitter",
            s3_output_bucket=s3_output_bucket,
            s3_output_prefix=s3_output_prefix)

        textract_sync_task = tcdk.TextractGenericSyncSfnTask(
            self,
            "TextractSyncOCR",
            s3_output_bucket=s3_output_bucket,
            s3_output_prefix=s3_output_prefix,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            enable_cloud_watch_metrics_and_dashboard=True,
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

        # spacy_classification_task = tcdk.ComprehendGenericSyncSfnTask(
        #     self,
        #     "Classification",
        #     s3_output_bucket=document_bucket.bucket_name,
        #     s3_output_prefix=s3_comprehend_output_prefix,
        #     s3_input_bucket=document_bucket.bucket_name,
        #     s3_input_prefix=s3_output_prefix,
        #     comprehend_classifier_arn=
        #     'arn:aws:comprehend:us-east-1:913165245630:document-classifier-endpoint/asdf',
        #     integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
        #     lambda_log_level="DEBUG",
        #     input=sfn.TaskInput.from_object({
        #         "Token":
        #         sfn.JsonPath.task_token,
        #         "ExecutionId":
        #         sfn.JsonPath.string_at('$$.Execution.Id'),
        #         "Payload":
        #         sfn.JsonPath.entire_payload,
        #     }),
        #     result_path="$.classification")
        configurator_task = tcdk.TextractClassificationConfigurator(
            self,
            f"{workflow_name}-Configurator",
        )

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

        vpc = ec2.Vpc(self,
                      "Vpc",
                      ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"))

        rds_aurora_serverless = tcdk.RDSAuroraServerless(self,
                                                         "RDSAuroraServerless",
                                                         vpc=vpc)

        csv_to_aurora_task = tcdk.CSVToAuroraTask(
            self,
            "CsvToAurora",
            db_cluster=rds_aurora_serverless.db_cluster,
            vpc=vpc,
            aurora_security_group=rds_aurora_serverless.aurora_security_group,
            lambda_security_group=rds_aurora_serverless.lambda_security_group,
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

        lambda_generate_classification_mapping: lambda_.IFunction = lambda_.DockerImageFunction(  #type: ignore
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
        #     typing.cast(rds.ServerlessCluster, csv_to_aurora_task.db_cluster).
        #     _security_groups[0])  #pyright: ignore [reportOptionalMemberAccess]

        doc_type_choice = sfn.Choice(self, 'RouteDocType') \
                    .when(sfn.Condition.string_equals('$.classification.documentType', 'NONE'), task_generate_classification_mapping) \
                    .when(sfn.Condition.string_equals('$.classification.documentType', 'AWS_OTHER'), task_generate_classification_mapping)\
                    .otherwise(configurator_task)

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

        textract_sync_task.next(generate_text) \
            .next(spacy_classification_task) \
            .next(doc_type_choice)

        configurator_task.next(textract_queries_sync_task) \
            .next(generate_csv) \
            .next(csv_to_aurora_task) \
            .next(task_generate_classification_mapping)

        map.iterator(textract_sync_task)

        workflow_chain = sfn.Chain \
            .start(decider_task) \
            .next(document_splitter_task) \
            .next(map)
        # .next(textract_sync_task1) \

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
            "StartStepFunctionLambdaLogGroup",
            value=lambda_step_start_step_function.log_group.log_group_name)
        current_region = Stack.of(self).region
        CfnOutput(
            self,
            'StepFunctionFlowLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}"
        )
        # CfnOutput(self,
        #           "EC2_DB_BASTION_PUBLIC_DNS",
        #           value=ec2_db_bastion.instance_public_dns_name
        #           )  #pyright: ignore [reportOptionalMemberAccess]
        CfnOutput(self,
                  "DBClusterARN",
                  value=csv_to_aurora_task.db_cluster.cluster_arn)
        CfnOutput(self,
                  "DBClusterSecretARN",
                  value=typing.cast(rds.ServerlessCluster,
                                    csv_to_aurora_task.db_cluster).secret.
                  secret_arn)  #pyright: ignore [reportOptionalMemberAccess]
