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

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
pip install -r requirements.txt
```

Now start Docker and login to your Docker account.

The CDK Constructs will build Docker containers during the deploy cycle. 

At this point you can now synthesize the CloudFormation template for this code.

At the moment there are 9 stacks available:

* SimpleSyncWorkflow - very easy setup, calls Textract Sync
* SimpleAsyncWorkflow - easy, but with Async
* SimpleSyncAndAsyncWorkflow  - both async and sync
* SpacyPaystubW2 - with classification using Spacy
* InsuranceStack - including A2I Construct call
* AnalyzeID - only calling AnalyzeID
* AnalyzeExpense - only calling AnalyzeExpense
* DemoQueries - workflow with calling Textract + Queries for alldocs
* PaystubAndW2 - using Comprehend classification

```
cdk synth (sample-stack-name)
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

Deploy the stack with following command
```
cdk deploy (sample-stack-name)
```

if you have not already setup your environment, run below command before cdk deploy
```
cdk bootstrap aws://<account>/<region>
```

You are all set with your deployment!

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!

# Create your own workflows

Take a look at the sample workflows. Copy one as a starting point and go from there.

