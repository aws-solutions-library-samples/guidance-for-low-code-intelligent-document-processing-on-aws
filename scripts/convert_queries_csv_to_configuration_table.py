import textractmanifest as tm
import csv
from typing import List
import glob
import os
"""
The script generates a default_config.csv file that can be used by the 'cfn_custom_configurator_prefill' Lambda function.
It takes all the queries.csv file from the data folder and uses the subfolder name as the document types.

The folder structure under data is
./data
|- <DOCUMENT_TYPE_NAME_1>
  |- queries.csv
|- <DOCUMENT_TYPE_NAME_2>
  |- queries.csv

The queries.csv file should have the form of
alias,question
"""
current_dir = os.path.dirname(__file__)
ddb_items = list()
for query in glob.glob("**/*"):
    document_type = query.split('/')[-1:][0]
    if os.path.exists(os.path.join(query, "queries.csv")):
        queries: List[tm.Query] = list()
        with open(os.path.join(query, "queries.csv")) as queries_f:
            csv_reader = csv.reader(queries_f)
            manifest = None
            for query in csv_reader:
                queries.append(tm.Query(alias=query[0], text=query[1]))
                manifest = tm.IDPManifest(queries_config=queries,
                                          textract_features=['QUERIES'])
            if manifest:
                ddb_items.append(
                    [document_type,
                     tm.IDPManifestSchema().dumps(manifest)])

with open("default_config.csv", 'w') as output_csv:
    csv_writer = csv.writer(output_csv, quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerows(ddb_items)
