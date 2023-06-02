from constructs import Construct
import os
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration)
import amazon_textract_idp_cdk_constructs as tcdk


class TestWorkflow(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        configurator_task = tcdk.TextractClassificationConfigurator(
            self,
            f"Configurator",
        )
