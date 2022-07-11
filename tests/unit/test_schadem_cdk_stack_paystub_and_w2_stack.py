import aws_cdk as core
import aws_cdk.assertions as assertions

from schadem_cdk_stack_paystub_and_w2.schadem_cdk_stack_paystub_and_w2_stack import SchademCdkStackPaystubAndW2Stack

# example tests. To run these tests, uncomment this file along with the example
# resource in schadem_cdk_stack_paystub_and_w2/schadem_cdk_stack_paystub_and_w2_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = SchademCdkStackPaystubAndW2Stack(app, "schadem-cdk-stack-paystub-and-w2")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
