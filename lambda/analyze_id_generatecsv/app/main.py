# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import boto3
from typing import Tuple
import json
import datetime
import analzyeidtocsv.analzye_id_to_csv as id_to_csv

logger = logging.getLogger(__name__)
version = "0.0.1"
s3_client = boto3.client('s3')
step_functions_client = boto3.client(service_name='stepfunctions')


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
        o = s3_client.get_object(Bucket=s3_bucket, Key=s3_key, Range=range)
    else:
        o = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    return o.get('Body').read()


def lambda_handler(event, _):
    # takes and even which includes a location to a Textract JSON schema file and generates CSV based on Query results + FORMS results
    # in the form of
    # filename, page, datetime, key, value

    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.debug(f"version: {version}")
    logger.debug(json.dumps(event))
    csv_s3_output_prefix = os.environ.get('CSV_S3_OUTPUT_PREFIX')
    output_type = os.environ.get('OUTPUT_TYPE', 'CSV')
    csv_s3_output_bucket = os.environ.get('CSV_S3_OUTPUT_BUCKET')

    logger.info(f"CSV_S3_OUTPUT_PREFIX: {csv_s3_output_prefix} \n\
        CSV_S3_OUTPUT_BUCKET: {csv_s3_output_bucket} \n\
            OUTPUT_TYPE: {output_type}")
    if not csv_s3_output_prefix or not csv_s3_output_bucket:
        raise ValueError(
            f"require CSV_S3_OUTPUT_PREFIX and CSV_S3_OUTPUT_BUCKET")
    if not 'Payload' in event and 'textract_result' in event[
            'Payload'] and not 'TextractOutputJsonPath' in event['Payload'][
                'textract_result']:
        raise ValueError(
            f"no 'TextractOutputJsonPath' in event['textract_result]")
    # FIXME: hard coded result location
    s3_path = event['Payload']['textract_result']['TextractOutputJsonPath']
    classification = ""
    if 'classification' in event['Payload'] and event['Payload'][
            'classification'] and 'documentType' in event['Payload'][
                'classification']:
        classification = event['Payload']['classification']['documentType']

    base_filename = os.path.basename(s3_path)
    base_filename_no_suffix, _ = os.path.splitext(base_filename)
    file_json = get_file_from_s3(s3_path=s3_path).decode('utf-8')
    timestamp = datetime.datetime.now().astimezone().replace(
        microsecond=0).isoformat()
    result_value = id_to_csv.convert_analyze_id_to_csv_string(
        file_json,
        timestamp=timestamp,
        classification=classification,
        base_filename=base_filename)

    csv_s3_output_key = f"{csv_s3_output_prefix}/{timestamp}/{base_filename_no_suffix}.csv"

    s3_client.put_object(Body=bytes(result_value.encode('UTF-8')),
                         Bucket=csv_s3_output_bucket,
                         Key=csv_s3_output_key)
    logger.debug(
        f"TextractOutputCSVPath: s3://{csv_s3_output_bucket}/{csv_s3_output_key}"
    )
