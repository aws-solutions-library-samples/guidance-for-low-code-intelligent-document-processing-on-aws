import csv
import io
import json
import trp.trp2_analyzeid as t2

# Output is same format as for Query or Forms CSV generation
# timestamp, classification, filename text, page, key, key_confidence, value, value_confidence, key_bb_top, key_bb_height, k_bb_width, k_bb_left, v_bb_top, v_bb_height, v_bb_width, v_bb_left,
# trp2_doc.get_values_as_list() returns:
# page_number, key, value, confidence, normalized_value, normalized_value_type
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

    result_rows = [[
        timestamp, classification, base_filename, x[0], x[1], "1", x[2], x[3],
        0, 0, 0, 0, 0, 0, 0, 0
    ] for x in key_value_list]
    print(result_rows)

    csv_writer.writerows(result_rows)
    return csv_output.getvalue()
