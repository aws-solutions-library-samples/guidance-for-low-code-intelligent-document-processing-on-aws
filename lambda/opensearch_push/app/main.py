"""
kicks off Step Function executions
"""
import json
import logging
import os
import boto3
import json
from typing import Tuple
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, ConnectionTimeout

logger = logging.getLogger(__name__)

version = "0.0.1"
s3 = boto3.client('s3')


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
    return o.get('Body').read()


class ErrorInBulkImport(Exception):
    pass


class OpenSearchConnectionTimeout(Exception):
    pass


def lambda_handler(event, _):
    """This Lambda requires the DOMAIN_ENDPOINT and the input file in the event at
    event["opensearch_output_location"]["TextractOutputCSVPath"])
    """
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))
    domain_endpoint = os.environ.get('DOMAIN_ENDPOINT', 'NONE')
    domain_port = os.environ.get('DOMAIN_PORT', '443')
    region = os.environ.get('AWS_REGION', 'NONE')

    if not domain_endpoint:
        raise ValueError(f"no domain_endpoint: {domain_endpoint}")

    credentials = boto3.Session().get_credentials()
    auth = AWSV4SignerAuth(credentials, region)

    response = ""
    try:
        client = OpenSearch(hosts=[{
            'host': domain_endpoint,
            'port': int(domain_port)
        }],
                            http_auth=auth,
                            use_ssl=True,
                            verify_certs=True,
                            connection_class=RequestsHttpConnection)

        document = get_file_from_s3(s3_path=event["opensearch_output_location"]
                                    ["TextractOutputCSVPath"]).decode('utf-8')
        response = client.bulk(body=document)
    except ConnectionTimeout as ct:
        raise OpenSearchConnectionTimeout(ct)

    logger.error(response)

    if response['errors']:
        logger.error(f"errors {response}")
        raise ErrorInBulkImport('ErrorInBulkImport')
    else:
        return {
            "numberItems":
            0 if not "items" in response else len(response["items"])
        }
