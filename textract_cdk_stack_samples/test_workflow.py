from constructs import Construct
from aws_cdk import (Stack)
import amazon_textract_idp_cdk_constructs as tcdk


class TestWorkflow(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        tcdk.TextractClassificationConfigurator(
            self,
            f"Configurator",
        )
