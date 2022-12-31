"""
kicks off Step Function executions
"""
import json
import logging
import os
import json
import boto3
import textractmanifest as tm

logger = logging.getLogger(__name__)


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    if 'Payload' in event and 'manifest' in event['Payload']:
        manifest: tm.IDPManifest = tm.IDPManifestSchema().load(
            event['Payload']['manifest'])  #type: ignore
    elif 'manifest' in event:
        manifest: tm.IDPManifest = tm.IDPManifestSchema().load(
            event['manifest'])  #type: ignore
    else:
        manifest: tm.IDPManifest = tm.IDPManifestSchema().load(
            event)  #type: ignore

    split_documents_unclassified_prefix = os.environ.get(
        'SPLIT_DOCUMENTS_UNCLASSIFIED_PREFIX',
        "splitDocuments/UNCLASSIFIED/").strip('/')

    mime: str = ""
    if 'mime' in event:
        mime = event['mime']

    textract_temp_output_json_path = event["textract_result"][
        'TextractTempOutputJsonPath']
    bucket, prefix = textract_temp_output_json_path.replace("s3://",
                                                            "").split("/", 1)

    prefix = os.path.join(prefix, split_documents_unclassified_prefix)

    # now figure out how many files there are
    s3 = boto3.client("s3")
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    unclassified_pages = list()
    for page in pages:
        logger.debug(f"page: {page}")
        if 'Contents' in page:
            for content in page['Contents']:
                if 'Key' in content:
                    unclassified_pages.extend(content['Key'].split('/')[-1:])

    return {
        'manifest': tm.IDPManifestSchema().dump(manifest),
        'unclassifiedDocsBucket': bucket,
        'unclassifiedDocsPrefix': prefix,
        'unclassifiedDocsArray': unclassified_pages,
        'mime': mime
    }
