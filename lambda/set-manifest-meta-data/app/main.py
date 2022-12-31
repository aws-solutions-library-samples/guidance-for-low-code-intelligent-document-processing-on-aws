"""
kicks off Step Function executions
"""
import json
import logging
import os
import json
import textractmanifest as tm
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    # The manifest can be at
    # event
    # event['manifest']
    # event['Payload']['manifest']
    #
    if 'Payload' in event and 'manifest' in event['Payload']:
        manifest: tm.IDPManifest = tm.IDPManifestSchema().load(
            event['Payload']['manifest'])  #type: ignore
    elif 'manifest' in event:
        manifest: tm.IDPManifest = tm.IDPManifestSchema().load(
            event['manifest'])  #type: ignore
    else:
        manifest: tm.IDPManifest = tm.IDPManifestSchema().load(
            event)  #type: ignore
    s3_path = urlparse(manifest.s3_path) if manifest.s3_path else ""
    if s3_path and s3_path.netloc:
        try:
            doc_id, _ = os.path.splitext(os.path.basename(s3_path.path))
            if not manifest.meta_data:
                manifest.meta_data = list()
            manifest.meta_data.append(
                tm.MetaData(key="DOCUMENT_ID", value=doc_id))
        except Exception as e:
            logger.error(e)
            s3_path = ""

    return tm.IDPManifestSchema().dump(manifest)
