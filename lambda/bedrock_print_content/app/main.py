"""
kicks off Step Function executions
"""
import json
import logging
import os
import boto3

from typing import Tuple

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


def lambda_handler(event, _):
    """
    take the Bedrock classification results and adds to the Step Function context
    """
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    # load text document from S3
    if "bedrock_output" in event:
        document_text_path = event["bedrock_output"]
    else:
        raise ValueError(
            "no ['bedrock_output'] to get the text file from "
        )

    document_text = get_file_from_s3(s3_path=document_text_path).decode('utf-8')
    lines = document_text.splitlines()[1:-1]
    result_str = '\n'.join(lines)
    # expect format
    # {"CLASSIFICATION": "VALUE"}
    classification_json = json.loads(result_str)
    document_type = classification_json['CLASSIFICATION']

    event.setdefault('classification', {})['documentType'] = document_type

    return event
