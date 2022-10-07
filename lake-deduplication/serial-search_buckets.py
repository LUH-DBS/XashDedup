import datetime
import json
import sys
from collections import defaultdict

import db_handler

data = db_handler.getTableData(int(sys.argv[1]), int(sys.argv[2]), False)
super_keys = defaultdict(dict)
tables_buckets = {}
enable_print = False

duplicates = []
duplicate_tables = []
counter_comp = 0


def compareTables(t1, t2):
    global counter_comp
    global duplicates
    global duplicate_tables

    t1_data = data[t1]
    t2_data = data[t2]

    duplicates_local = []

    # Compare num of columns:
    if len(t1_data[0]) != len(t2_data[0]):
        return None  # Number of columns is different
    # End compare num of columns

    for row_t1 in t1_data:
        complete_row_t1 = []
        complete_row_t1 = t1_data[row_t1]
        complete_row_t1 = list(complete_row_t1.values())
        for row_t2 in t2_data:
            complete_row_t2 = []
            complete_row_t2 = t2_data[row_t2]
            if len(t2_data) < 1:
                if enable_print:
                    print("Empty table")
                continue
            complete_row_t2 = list(complete_row_t2.values())

            complete_row_t1.sort()
            complete_row_t2.sort()

            # Compare values by value:
            if len(complete_row_t1) > len(complete_row_t2):
                bigger_row = complete_row_t1
                smaller_row = complete_row_t2
            else:
                bigger_row = complete_row_t2
                smaller_row = complete_row_t1

            fail = False
            for i in range(0, len(bigger_row)):
                if i >= len(smaller_row):
                    # fail
                    if enable_print:
                        print("Fail i")
                    fail = True
                    break
                if bigger_row[i] != smaller_row[i]:
                    # fail, different values
                    fail = True
                    break
            if not fail:
                if enable_print:
                    print("Dup row")
                duplicates.append({"tableid_1": t1, "rowid_1": row_t1, "tableid_2": t2, "rowid_2": row_t2})
                duplicates_local.append({"tableid_1": t1, "rowid_1": row_t1, "tableid_2": t2, "rowid_2": row_t2})
            else:
                if enable_print:
                    print("fail - False positive")
            counter_comp = counter_comp + 1

    num_rows_min = min(len(t1_data), len(t2_data))
    if len(duplicates_local) >= num_rows_min and num_rows_min > 0:
        t1_dup = []
        t2_dup = []
        for value in duplicates_local:
            t1_dup.append(value['rowid_1'])
            t2_dup.append(value['rowid_2'])

        if (len(set(t1_dup)) >= len(t1_data) or len(set(t2_dup)) >= len(t2_data)):
            if enable_print:
                print("found duplicate table: " + str(t1) + " and " + str(t2))
            duplicate_tables.append((t1, t2))


print("Comparing tables between " + sys.argv[1] + " and " + sys.argv[2])
### TIME TRACKING START ###
start = datetime.datetime.now()
### TIME TRACKING END ###

# Group tables into buckets by no of cols
for tableId in range(int(sys.argv[1]), int(sys.argv[2])):
    if len(data[tableId][0]) not in tables_buckets:  # Check if key exists
        tables_buckets[len(data[tableId][0])] = []
    tables_buckets[len(data[tableId][0])].append(tableId)

for num_cols, tableIds in tables_buckets.items():
    for tableIdt1 in tableIds:
        for tableIdt2 in tableIds:
            if tableIdt1 < tableIdt2:
                compareTables(tableIdt1, tableIdt2)

### TIME TRACKING START ###
stop = datetime.datetime.now()
time_diff = stop - start
print("Computation took (ms): " + str(int(time_diff.total_seconds() * 1000)))
### TIME TRACKING END ###

print("Found duplicates (JSON):")
print(json.dumps(duplicates))
print("\n\nFound duplicate tables (JSON):")
print(json.dumps(duplicate_tables))
print("SUM: " + str(counter_comp))
