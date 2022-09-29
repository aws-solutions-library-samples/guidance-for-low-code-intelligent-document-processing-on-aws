import csv
import sys

if len(sys.argv) < 2:
    print("need a file as input")
    sys.exit(1)
file_name = sys.argv[1]
with open(file_name) as input_file:
    csv_reader = csv.reader(input_file)
    for line in csv_reader:
        print(f"{line[4]} = {line[6]}")
        
        