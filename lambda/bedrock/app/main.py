"""
kicks off Step Function executions
"""
import json
import logging
import os
import textractmanifest as tm
import boto3
import jinja2
from uuid import uuid4

from typing import Tuple
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute
from pynamodb.exceptions import DoesNotExist

logger = logging.getLogger(__name__)

version = "0.0.1"
s3 = boto3.client("s3")
bedrock_rt = boto3.client("bedrock-runtime")


def split_s3_path_to_bucket_and_key(s3_path: str) -> Tuple[str, str]:
    if len(s3_path) > 7 and s3_path.lower().startswith("s3://"):
        s3_bucket, s3_key = s3_path.replace("s3://", "").split("/", 1)
        return (s3_bucket, s3_key)
    else:
        raise ValueError(
            f"s3_path: {s3_path} is no s3_path in the form of s3://bucket/key."
        )


def get_file_from_s3(s3_path: str, range=None) -> bytes:
    s3_bucket, s3_key = split_s3_path_to_bucket_and_key(s3_path)
    if range:
        o = s3.get_object(Bucket=s3_bucket, Key=s3_key, Range=range)
    else:
        o = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    return o.get("Body").read()


class ThrottlingException(Exception):
    pass


class ServiceQuotaExceededException(Exception):
    pass


class ModelTimeoutException(Exception):
    pass


class ModelNotReadyException(Exception):
    pass


class BedrockPromptConfig(Model):
    class Meta:
        table_name = os.environ["BEDROCK_CONFIGURATION_TABLE"]
        region = boto3.Session().region_name

    id = UnicodeAttribute(hash_key=True, attr_name="id")
    prompt_template = UnicodeAttribute(attr_name="p")


def call_bedrock(prompt_data, model_id):
    # formatting body for claude
    body = json.dumps(
        {"prompt": "Human:" + prompt_data + "\nAssistant:", "max_tokens_to_sample": 8191, "temperature": 0}
    )
    accept = "application/json"
    contentType = "application/json"
    response = bedrock_rt.invoke_model(
        body=body, modelId=model_id, accept=accept, contentType=contentType
    )
    response_body = json.loads(response.get("body").read())
    return response_body


def lambda_handler(event, _):
    """
    Reads a jinja2 template from the DDB table passed in as BEDROCK_CONFIGURATION_TABLE
    if the FIXED_KEY is empty, it takes the classification result to find the prompt.
    if the FIXED_KEY is set, it will always execute that prompt, which is useful for classification
    """
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))
    bedrock_model_id = os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-v2")
    fixed_key = os.environ.get("FIXED_KEY", None)
    s3_output_bucket = os.environ.get('S3_OUTPUT_BUCKET')
    s3_output_prefix = os.environ.get('S3_OUTPUT_PREFIX')
    if not s3_output_bucket:
        raise Exception("no S3_OUTPUT_BUCKET set")
    if not s3_output_prefix:
        raise Exception("no S3_OUTPUT_PREFIX set")

    try:
        if "Payload" in event and "manifest" in event["Payload"]:
            manifest: tm.IDPManifest = tm.IDPManifestSchema().load(event["Payload"]["manifest"])  # type: ignore
        elif "manifest" in event:
            manifest: tm.IDPManifest = tm.IDPManifestSchema().load(event["manifest"])  # type: ignore else:
        else:
            manifest: tm.IDPManifest = tm.IDPManifestSchema().load(event)  # type: ignore

        # fixed_key for classification and non fixed key for extraction
        if fixed_key:
            ddb_key = fixed_key
        else:
            if "classification" in event and "documentType" in event["classification"]:
                ddb_key = event["classification"]["documentType"]
                logger.debug(f"document_type: {ddb_key}")
            else:
                raise ValueError(
                    f"no [classification][documentType] given in event: {event}"
                )

        # load from DDB
        try:
            ddb_prompt_entry: BedrockPromptConfig = BedrockPromptConfig.get(ddb_key)
        except DoesNotExist:
            raise ValueError(f"no DynamoDB item with key: '{ddb_key}' was found")
        prompt_template = ddb_prompt_entry.prompt_template

        # load text document from S3
        if (
            "txt_output_location" in event
            and "TextractOutputCSVPath" in event["txt_output_location"]
        ):
            document_text_path = event["txt_output_location"]["TextractOutputCSVPath"]
        else:
            raise ValueError(
                "no ['txt_output_location']['TextractOutputCSVPath'] to get the text file from "
            )

        document_text = get_file_from_s3(s3_path=document_text_path).decode('utf-8')
        # apply template
        jinja_template = jinja2.Template(prompt_template)
        template_vars = {
            "document_text": document_text
        }
        prompt = jinja_template.render(template_vars)

        logger.debug(prompt)

        response = call_bedrock(
            prompt_data=prompt, model_id=bedrock_model_id
        )
        if "completion" in response:
            output_text = response['completion']
        else:
            output_text = ""

        logger.debug(response)

        s3_filename, _ = os.path.splitext(os.path.basename(manifest.s3_path))
        output_bucket_key = s3_output_prefix + "/" + s3_filename + str(uuid4()) + ".json"
        s3.put_object(Body=bytes(
            output_text.encode('UTF-8')),
                    Bucket=s3_output_bucket,
                    Key=output_bucket_key)

    except bedrock_rt.exceptions.ThrottlingException as te:
        raise ThrottlingException(te)

    except bedrock_rt.exceptions.ModelNotReadyException as mnr:
        raise ModelNotReadyException(mnr)

    except bedrock_rt.exceptions.ModelTimeoutException as mt:
        raise ModelTimeoutException(mt)

    except bedrock_rt.exceptions.ServiceQuotaExceededException as sqe:
        raise ServiceQuotaExceededException(sqe)

    event['bedrock_output'] = f"s3://{s3_output_bucket}/{output_bucket_key}"
    return event
