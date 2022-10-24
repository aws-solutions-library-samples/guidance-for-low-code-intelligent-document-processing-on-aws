import os
import sys

current_script_path = os.path.dirname(os.path.abspath(__file__))

sys_path = os.path.join(current_script_path, "../app/")
sys.path.append(sys_path)

import analzyeidtocsv.analzye_id_to_csv as id_to_csv


def test_analyze_id_to_csv(caplog):

    csv = ""
    test_json = 'data/analyzeIDResponse.json'

    with open(os.path.join(current_script_path, test_json),
              'r',
              encoding="utf-8") as input_file:
        csv = id_to_csv.convert_analyze_id_to_csv_string(
            input_file.read(), "timestamp", "classification", "base_filename")

    assert csv
