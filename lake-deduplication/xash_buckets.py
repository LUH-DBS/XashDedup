import datetime
import json
import sys

import db_handler
#from DuplicateTableDetectionSort import DuplicateTableDetectionSort as DuplicateTableDetection
from DuplicateTableDetection import DuplicateTableDetection

data = db_handler.getTableData(int(sys.argv[1]), int(sys.argv[2]), True)
tables_buckets = {}
enable_print = False

counter_superkey = 0
counter_fp = 0
duplicates = []
duplicate_tables = []



print("Comparing tables between " + sys.argv[1] + " and " + sys.argv[2])
### TIME TRACKING START ###
start = datetime.datetime.now()
### TIME TRACKING END ###

# Group tables into buckets by no of cols
for tableId in range(int(sys.argv[1]), int(sys.argv[2])):
    if len(data[0][tableId][0]) not in tables_buckets:  # Check if key exists
        tables_buckets[len(data[0][tableId][0])] = []
    tables_buckets[len(data[0][tableId][0])].append(tableId)

for num_cols, tableIds in tables_buckets.items():
    for tableIdt1 in tableIds:
        for tableIdt2 in tableIds:
            if tableIdt1 < tableIdt2:
                DuplicateTableDetection.compareTables(tableIdt1, tableIdt2, data)

### TIME TRACKING START ###
stop = datetime.datetime.now()
time_diff = stop - start
print("Computation took (ms): " + str(int(time_diff.total_seconds() * 1000)))
### TIME TRACKING END ###

print("Found duplicates (JSON):")
#print(json.dumps(DuplicateTableDetection.duplicates))
print("\n\nFound duplicate tables (JSON):")
print(json.dumps(DuplicateTableDetection.duplicate_tables))
print("FP: " + str(DuplicateTableDetection.getFp()))
print("SUM: " + str(DuplicateTableDetection.getSum()))
print("\nMemory: " + str(db_handler.getMemory()))