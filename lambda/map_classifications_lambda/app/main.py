"""
kicks off Step Function executions
"""
import json
import logging
import os
import json

logger = logging.getLogger(__name__)


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    classification = event["classification"]['documentType']
    file_name = event['manifest']['s3Path']
    s3_filename, _ = os.path.splitext(os.path.basename(file_name))

    if s3_filename and classification:
        return {s3_filename: classification}
    else:
        return {}
