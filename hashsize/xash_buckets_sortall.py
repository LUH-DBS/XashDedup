# In this file all row attributes will be sorted first

import datetime
import json
import sys

import db_handler

data = db_handler.getTableData(int(sys.argv[1]), int(sys.argv[2]), True)
tables_buckets = {}
enable_print = False

counter_superkey = 0
counter_fp = 0
duplicates = []
duplicate_tables = []


def compareTables(t1, t2):
    global counter_fp
    global counter_superkey
    global duplicates
    global duplicate_tables

    duplicates_local = []

    t1_data = data[1][t1]
    t2_data = data[1][t2]

    # Compare num of columns:
    if len(data[0][t1][0]) != len(data[0][t2][0]):
        return None  # Number of columns is different
    # End compare num of columns

    for row_t1 in t1_data:
        super_key_t1 = t1_data[row_t1]
        for row_t2 in t2_data:
            super_key_t2 = t2_data[row_t2]
            if len(t2_data) < 1:
                if enable_print:
                    print("Empty table")
                continue

            # Compare super keys:
            if super_key_t1 == super_key_t2:
                counter_superkey = counter_superkey + 1

                # Check values to check false positive
                rowvalues_t1 = data[0][t1][row_t1]
                rowvalues_t2 = data[0][t2][row_t2]

                ## Duplicate detection
                if len(rowvalues_t1) > len(rowvalues_t2):
                    bigger_row = rowvalues_t1
                    smaller_row = rowvalues_t2
                else:
                    bigger_row = rowvalues_t2
                    smaller_row = rowvalues_t1

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
                    counter_fp = counter_fp + 1
                ## End duplicate

    num_rows_min = min(len(t1_data), len(t2_data))
    if len(duplicates_local) >= num_rows_min and num_rows_min > 0:
        t1_dup = []
        t2_dup = []
        for value in duplicates_local:
            t1_dup.append(value['rowid_1'])
            t2_dup.append(value['rowid_2'])

        if len(set(t1_dup)) >= len(t1_data) or len(set(t2_dup)) >= len(t2_data):
            if enable_print:
                print("found duplicate table: " + str(t1) + " and " + str(t2))
            duplicate_tables.append((t1, t2))


print("Comparing tables between " + sys.argv[1] + " and " + sys.argv[2])
### TIME TRACKING START ###
start = datetime.datetime.now()
### TIME TRACKING END ###

# Sort row values
for tableid in data[0]:
    for rowid in data[0][tableid]:
        data[0][tableid][rowid] = list(data[0][tableid][rowid].values())
        data[0][tableid][rowid].sort()

# Group tables into buckets by no of cols
for tableId in range(int(sys.argv[1]), int(sys.argv[2])):
    if len(data[0][tableId][0]) not in tables_buckets:  # Check if key exists
        tables_buckets[len(data[0][tableId][0])] = []
    tables_buckets[len(data[0][tableId][0])].append(tableId)

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
print("FP: " + str(counter_fp))
print("SUM: " + str(counter_superkey))
print("\nMemory: " + str(db_handler.getMemory()))
