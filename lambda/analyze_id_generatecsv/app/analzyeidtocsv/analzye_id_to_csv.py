import csv
import io
import json
import trp.trp2_analyzeid as t2


def convert_analyze_id_to_csv_string(analzye_id_json: str,
                                     timestamp="",
                                     classification="",
                                     base_filename="") -> str:
    trp2_doc: t2.TAnalyzeIdDocument = t2.TAnalyzeIdDocumentSchema().load(
        json.loads(analzye_id_json))  #type: ignore
    key_value_list = trp2_doc.get_values_as_list()  #type: ignore
    csv_output = io.StringIO()
    csv_writer = csv.writer(csv_output,
                            delimiter=",",
                            quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)

    csv_writer.writerows([[timestamp, classification, base_filename] + x
                          for x in key_value_list])
    return csv_output.getvalue()
