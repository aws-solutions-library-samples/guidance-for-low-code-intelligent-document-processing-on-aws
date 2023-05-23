# Amazon Textract IDP Stack Samples
<!--BEGIN STABILITY BANNER-->

---

![Stability: Experimental](https://img.shields.io/badge/stability-Experimental-important.svg?style=for-the-badge)

> All classes are under active development and subject to non-backward compatible changes or removal in any
> future version. These are not subject to the [Semantic Versioning](https://semver.org/) model.
> This means that while you may use them, you may need to update your source code when upgrading to a newer version of this package.

---
<!--END STABILITY BANNER-->

# Deployment

This is a collection of sample workflows designed to showcase the usage of the [Amazon Textract IDP CDK Constructs](https://github.com/aws-samples/amazon-textract-idp-cdk-constructs/)

The samples use the [AWS Cloud Development Kit (AWS CDK)](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html).
Also it requires Docker.

You can spin up a [AWS Cloud9](https://aws.amazon.com/cloud9/) instance, which has the AWS CDK and docker already set up.

After cloning the repository, install the dependencies:

```
pip install -r requirements.txt
```

Then deploy a stack, for example:

```
cdk deploy DemoQueries
```


At the moment there are 10 stacks available:

* SimpleSyncWorkflow - very easy setup, calls Textract Sync, expects single page
* SimpleAsyncWorkflow - easy, but with Async and can take multi-page
* SimpleSyncAndAsyncWorkflow  - both async and sync
* PaystubAndW2Spacy - information extraction with classification using a [Spacy](https://spacy.io/) model.
* PaystubAndW2Comprehend - information extraction with classification using a Comprehend model.
* InsuranceStack - including A2I Construct call
* AnalyzeID - only calling AnalyzeID
* AnalyzeExpense - only calling AnalyzeExpense
* DemoQueries - workflow with calling Textract + Queries for alldocs
* DocumentSplitterWorkflow - Example of splitting a multi-page document, classifying each page and extraction information depending on the document type for the page
* LendingWorkflow - Example of using the [Amazon Textract Analyze Lending API](https://docs.aws.amazon.com/textract/latest/dg/API_StartLendingAnalysis.html) to extract information from mortgage document, then generate a CSV and process pages that were marked UNCLASSIFIED by the Analzye Lending API, process them in a separate branch, extract information and generate a CSV as well
* OpenSearchWorkflow - Example of indexing a large number of files into an OpenSearch service


# Sample Workflows


## Document Splitter Workflow


Deploy using
```bash
cdk deploy DocumentSplitterWorkflow
```

This samples includes a new component called DocumentSpliter, which takes and input document of type TIFF or PDF and outputs each individual page to an S3 location and adds the list of filenames to an array. 

That array is then used in a [Step Functions Map state](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-map-state.html) and processed in parallel. Each iteration classifies the page and then in case of a W2 or paystub routes to an extraction process or not. At the end all the W2s and Paystubs are extracted and the map returns and array with the page numbers and their classification result.

<img alt="DocumentSplitter" width="400px" src="images/DocumentSplitter_graph.svg" />

When you look at the execution in the AWS Web Console under Step Functions and look at the execution, you may not see the correct rending in the "Graph Insepctor" while the "Execution event history" is still loading indicated by the process circle spinning next to the "Execution event history" text. Wait for it to finish.

We are planning to have a better UI experience in the future.


## Paystub And W2 Spacy

Deploy using
```bash
cdk deploy PaystubAndW2Spacy
```

This sample showcases a number of components, including classification using Comprehend and routing based on the document type, followed by configuration based on the document types.

It is called Paystub and W2, because those are the ones configured in the RouteDocType and the DemoIDP-Configurator.

At the moment it does single page, check the [Document Splitter Workflow](#document-splitter-workflow)

Check the API definition for the Constructs at: https://github.com/aws-samples/amazon-textract-idp-cdk-constructs/blob/main/API.md

From top to bottom:

1. DemoIDP-Decicer - Takes in the manifest, usually a link to a file on S3. Look at the bottom and the lambda_start_step_function code to see how the workflow is triggered.
1. NumberOfPagesChoise - Choice state failing the flow if the number of pages > 1
1. Randomize and Randomize2 - Just a function to generate a random number 0-100, so we can route between sync and async for demo purposes. (Will increase throughput as well, but that is not the main purpose, it is just to demo both async and sync in one flow)
1. TextractAsync - calls Textract async through Start*. When passed features are passed, will call AnalyzeDocument, otherwise DetectText. The flow is configured to first only call with text. The process abstract calling Textract and waiting for the SNS Notification and output to OutputConfig
1. TextractSync - similar to TextractAsync, but calling the Textract sync API (DetectText, AnalyzeDocument)
1. TextractAsyncToJSON2 - TextractAsync will store paginated JSON files to OutputConfig. This step combines them to one JSON.
1. GenerateText - Takes the Textract JSON and outputs all LINES into a text file.
1. Classification - Uses the generated text file from GenerateText and sends to the Comprehend endpoint defined by the ARN.
1. RouteDocType - routes based on the classification result, aka the document type. Unless it is ID, Expense or AWS_OTHER or not known, we send to further Textract processing
1. DemoIDP-Configurator - based the document type, pulls configuration from DynamoDB what to call Textract with (e. g. specific queries and/or with forms and/or with tables) 
1. Then we repeat essentially the calls to Textract like at the top, but this time we do have the configuration set with queries, forms and/or tables
1. GenerateCsvTask - output is one CSV from queries and forms information to a structure that includes also confidence scores and bounding box information
1. CsvToAurora - sends the generated CSV to a Serverless Aurora RDS Cluster

The Aurora RDS Cluster runs in a private VPC. To get there, check the commented section for EC2 in the sample stack. Put in your setting for Security Groups, AMI and keypair. (We'll make it easier in the future)

<img alt="PaystubAndW2Spacy_graph" width="800px" src="images/PaystubAndW2Spacy_graph.svg" />

Simple example of a flow only calling synchronous Textract for DetectText.

## Paystub And W2 Comprehend

Deploy using
```bash
cdk deploy PaystubAndW2Comprehend
```

This sample showcases a number of components, including classification using Comprehend and routing based on the document type, followed by configuration based on the document types.
It is called Paystub and W2, because those are the ones configured in the RouteDocType and the DemoIDP-Configurator.

At the moment it does single page, check the [Document Splitter Workflow](#document-splitter-workflow)

Here is the flow:

<img alt="PaystubAndW2Comprehend" width="800px" src="images/PaystubAndW2Comprehend_graph.svg" />


## Simple Async Workflow

Deploy using
```bash
cdk deploy SimpleAsyncWorkflow
```

Very basic workflow to demonstrate AsyncProcessing. This out-of-the-box will only call with DetectText, generating OCR output.
When you are interested in running specific queries or features like forms or tables on a set of documents, look at [DemoQueries](#demo-queries)

<img alt="SimpleAsyncWorkflow" width="400px" src="images/SimpleAsyncWorkflow_graph.svg" />

## Demo Queries

Deploy using
```bash
cdk deploy DemoQueries
```

Basic workflow to demonstrate how Sync and Async can be routed based on numberOfPages and numberOfQueries and how the workflow can be triggered with queries. 
Calls AnalyzeDocument with the 2 sample queries. Obviously, modify to your own needs. The location in the code where queries are configed when starting the workflow in the [lambda/start_queries/app/start_execution.py](https://github.com/aws-samples/amazon-textract-idp-cdk-stack-samples/blob/471905e06786e0def269695d5585b39a0d77b825/lambda/start_queries/app/start_execution.py#L52) when kicking off the Step Functions workflow.
The GenerateCsvTask will output one CSV file to S3 with key/value, confidence scores and bounding box information based on the forms and queries output.

<img alt="DemoQueries" width="400px" src="images/DemoQueries_graph.svg" />

## Insurance

Deploy using
```bash
cdk deploy InsuranceStack
```

Simple flow including A2I 

<img alt="Insurance" width="300px" src="images/Insurance_graph.svg" />


## Simple Sync Workflow 

Deploy using
```bash
cdk deploy SimpleAsyncWorkflow
```

<img alt="PaystubAndW2Spacy_graph" width="400px" src="images/SimpleSyncWorkflow_graph.svg" />

Simple flow calling the Textract AnalzyeID API.

## Analyze ID

Deploy using
```bash
cdk deploy AnalyzeID
```

<img alt="AnalyzeID_graph" width="300px" src="images/AnalyzeID_graph.svg" />

Simple flow calling the Textract AnalyzeExpense API.

## Analyze Expense

Deploy using
```bash
cdk deploy AnalyzeExpense
```

<img alt="AnalyzeExpense_graph" width="300px" src="images/AnalyzeExpense_graph.svg" />

## Analyze Lending

Example of using the [Amazon Textract Analyze Lending API](https://docs.aws.amazon.com/textract/latest/dg/API_StartLendingAnalysis.html) to extract information from mortgage document, then generate a CSV and process pages that were marked UNCLASSIFIED by the Analzye Lending API, process them in a separate branch, extract information and generate a CSV as well

![Lending Workflow](https://amazon-textract-public-content.s3.us-east-2.amazonaws.com/idp-cdk-samples/LendingWorkflow.svg)

Deploy using 
```bash
cdk deploy LendingWorkflow
```

The workflow uses a custom classification model to identify the HOMEOWNERS_INSURANCE_APPLICATION and CONTACT_FORM. The classifier ist just trained on the sample images and for demo purposes only.

```bash
aws s3 cp s3://amazon-textract-public-content/idp-cdk-samples/lending_console_demo_with_contacts.pdf $(aws cloudformation list-exports --query 'Exports[?Name==`LendingWorkflow-DocumentUploadLocation`].Value' --output text)
```

then open the StepFunction flow. 
```bash
aws cloudformation list-exports --query 'Exports[?Name==`LendingWorkflow-StepFunctionFlowLink`].Value' --output text
```

## OpenSearchWorkflow

This is an example how to populate an [OpenSearch](https://opensearch.org/) service with data from documents.
The index pattern includes:
* content -> the text from the page
* page -> the number of the page in the document
* uri -> the source file used for indexing
* _id -> <origin_document_name>_<page_number> - this means a subsequent processing of the same file-name will overwrite the content

![OpenSearch Workflow](https://amazon-textract-public-content.s3.us-east-2.amazonaws.com/idp-cdk-samples/OpenSearchWorkflowGraph.png)

Deploy using 
```bash
cdk deploy OpenSearchWorkflow
```

The workflow first splits the document into chunks of max 3000 pages, because that is the limit of the Textract service for asynchronous processing.
Each chunk is then send to [StartDocumentAnalysis](https://docs.aws.amazon.com/textract/latest/dg/API_StartDocumentAnalysis.html) extracing the OCR information from the page. The meta-data added to the context of the StepFunction workflow includes information required for creating the [OpenSearch bulk import](https://opensearch.org/docs/latest/api-reference/document-apis/bulk/) file, including ORIGIN_FILE_NAME and START_PAGE_NUMBER.


# Create your own workflows

Take a look at the sample workflows. Copy one as a starting point and go from there.

