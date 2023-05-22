import json
import logging
import os
import json
import textractmanifest as tm
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def lambda_handler(event, _):
    """This is a sample how to set meta-data during the processing. It is used in the OpenSearchWorkflow example in the map state to seed information for the subsequent OpenSearchBulkPush taks. It sets meta-data to be used by the OpenSearchBulk-file generator to set metadata for the search index
    Sample implementation. The OpenSearchBulk-file generator requires ORIGIN_FILE_NAME, START_PAGE_NUMBER
    All other meta-data is passed through down to the index. In this sample we also add ORIGIN_FILE_URI 
    """
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
    origin_file_uri = urlparse(event['originFileURI'])
    if s3_path and s3_path.netloc:
        # The expected file pattern is "<start_page_number>-<end_page_number>.suffix"
        pages, _ = os.path.splitext(os.path.basename(s3_path.path))
        start_page, _ = pages.split("-")

        origin_file_name, _ = os.path.splitext(
            os.path.basename(origin_file_uri.path))

        if not manifest.meta_data:
            manifest.meta_data = list()
        # The OpenSearchBulk importer takes
        # _id = document_id + "_" + page_number has to be generated in the bulk_generator as we deal with multi-page docs here
        # link = s3_path to original document (need to pass that in as well)
        # starting_page_number (used to calculate the page in the bulk uploader impl)
        # origin_file_name = name of the original document
        # content is the content of the page and does not have to be here
        manifest.meta_data.append(
            tm.MetaData(key="ORIGIN_FILE_NAME", value=origin_file_name))
        manifest.meta_data.append(
            tm.MetaData(key="START_PAGE_NUMBER", value=start_page))
        manifest.meta_data.append(
            tm.MetaData(key="ORIGIN_FILE_URI", value=event['originFileURI']))

    event['manifest'] = tm.IDPManifestSchema().dump(manifest)
    return event
