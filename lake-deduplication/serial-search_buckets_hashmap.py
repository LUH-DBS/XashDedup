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

    matched_rows = dict()

    # Compare num of columns:
    if len(t1_data[0]) != len(t2_data[0]):
        return None  # Number of columns is different
    # End compare num of columns

    if len(t1_data[0]) == 0 or len(t2_data[0]) == 0:
        return None

    column_mapping = defaultdict(set)

    for row_t1 in t1_data:
        complete_row_t1 = []
        complete_row_t1 = t1_data[row_t1]
        complete_row_t1 = list(complete_row_t1.values())

        # generate and store
        map1 = defaultdict(set)
        for i in range(len(complete_row_t1)):
            map1[complete_row_t1[i]].add(i)

        if len(t1_data) > len(t2_data):
            bigger_table = t1_data
            smaller_table = t2_data
        else:
            bigger_table = t2_data
            smaller_table = t1_data

        for row_t2 in t2_data:
            counter_comp = counter_comp+1
            complete_row_t2 = t2_data[row_t2]
            if len(t2_data) < 1:
                if enable_print:
                    print("Empty table")
                continue
            complete_row_t2 = list(complete_row_t2.values())

            # generate and store
            map2 = defaultdict(set)
            for i in range(len(complete_row_t2)):
                map2[complete_row_t2[i]].add(i)

            count = {}
            for i in complete_row_t2:
                if i in count:
                    count[i] += 1
                else:
                    count[i] = 1

            fail = False

            # Compare values by value:
            for i in complete_row_t1:
                if i not in count or count[i] == 0:
                    fail = True
                    break
                else:
                    count[i] -= 1
                    found_cm = False
                    for y in map1[i]:
                        if y in column_mapping:
                            for j in column_mapping[y]:
                                if j in map2[i]:
                                    found_cm = True
                                    break
                        else:
                            found_cm = True
                    if not found_cm:
                        fail = True # Column mapping mismatch
                        break

            if not fail:
                for i in complete_row_t1:
                    for y in map1[i]:
                        if y not in column_mapping:
                            column_mapping[y].update(map2[i])

                if len(t1_data) > len(t2_data):
                    row_bigger = row_t2
                    row_smaller = row_t1
                else:
                    row_bigger = row_t1
                    row_smaller = row_t2

                duplicates.append({"tableid_1": t1, "rowid_1": row_t1, "tableid_2": t2, "rowid_2": row_t2})
                matched_rows[row_bigger] = row_smaller
            ## End duplicate

    if len(smaller_table) > 0 and len(smaller_table) == len(matched_rows):
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
#print(json.dumps(duplicates))
print("\n\nFound duplicate tables (JSON):")
#print(json.dumps(duplicate_tables))
print("SUM: " + str(counter_comp))
