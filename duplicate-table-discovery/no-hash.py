import copy
import csv
import datetime
import sys
from collections import defaultdict

import psycopg2

input_table = sys.argv[1]

rows = dict()
row_map = defaultdict(
    list)  # Row map is a kind of index where the first column value maps to a list of rows in which this is the first column value
i = 0

conn = psycopg2.connect(user="postgres", password="", database="postgres", host="localhost", port=5432, sslmode="disable")
conn.autocommit = True
print("Successfully connected!")
cursor = conn.cursor()

print("Started...")
### TIME TRACKING START ###
start = datetime.datetime.now()
### TIME TRACKING END ###

map1 = defaultdict(set)
with open(input_table) as csv_file:
    reader = csv.reader(csv_file)
    count = {}
    for row in reader:
        rows[i] = row
        row_map[row[0]].append(i)

        for y in range(len(row)):
            map1[row[y]].add(y)

        count[i] = {}
        for v in row:
            if v in count[i]:
                count[i][v] += 1
            else:
                count[i][v] = 1

        i = i + 1

in_clause = ""
for v in rows.values():
    if v[0] == 'None':
        v[0] = ''
    v[0]  = v[0].replace("'", "''") # escape quotes
    in_clause = in_clause + "'" + str(v[0]) + "',"  # take first column value

# print(in_clause[:-1])

where_clause = ""
cursor.execute(
    f'SELECT tableid, rowid FROM "mate_main_tokenized" WHERE tokenized IN ({in_clause[:-1]}) GROUP BY tableid, rowid LIMIT 15000')
# cursor.execute(f'SELECT tableid, rowid FROM (SELECT tableid, rowid FROM "mate_main_tokenized" WHERE tokenized IN ({in_clause[:-1]}) LIMIT 5000000) as t1 GROUP BY tableid, rowid LIMIT 50000')
tableIdRowId_to_load = defaultdict(set)
for result in cursor:
    tableIdRowId_to_load[result[0]].add(str(result[1]))

for tableId in tableIdRowId_to_load:
    where_clause = where_clause + "(tableid=" + str(tableId) + " AND rowid IN (" + ','.join(
        tableIdRowId_to_load[tableId]) + ")) OR "

if len(where_clause) == 0:
    exit()  # no results

# print(where_clause[:-4])
cursor.execute(
    f'SELECT tableid, rowid, colid, tokenized FROM "mate_main_tokenized" WHERE ({where_clause[:-4]}) ORDER BY tableid, rowid, colid')
tmp_rowid = -1
tmp_tableid = -1
row = []
tableIds_length_to_load = set()
dup = []
duplicate_tables = []
counter_sum = 0
counter_sum_2 = 0

column_mapping = defaultdict(lambda: defaultdict(set))

# row_map needs to be an index map; row is sorted
def checkIfExists(row, row_map, inputRows, tid, rowMap2):
    global counter_sum
    global counter_sum_2
    global count
    global column_mapping

    counter_sum_2 = counter_sum_2 + 1
    fail = False
    for rowId in row_map[row[0]]:
        counter_sum = counter_sum + 1
        fail = False

        # Check values
        rowvalues_t1 = row
        rowvalues_t2 = inputRows[rowId]

        if len(rowvalues_t1) != len(rowvalues_t2):
            continue

        count_copy = copy.deepcopy(count[rowId])

        # Duplicate detection
        for i in rowvalues_t1:
            # Check if value in hashmap
            if i not in count_copy or count_copy[i] == 0:
                fail = True
                break
            else:
                count_copy[i] -= 1

                found_cm = False
                for j in map1[i]:
                    if j in column_mapping[tid]:
                        for h in column_mapping[tid][j]:
                            if h in rowMap2[i]:
                                found_cm = True
                                break
                    else:
                        found_cm = True
                if not found_cm:
                    fail = True # Column mapping mismatch
                    break

        if not fail:
            for h in rowvalues_t1:
                for j in map1[h]:
                    if j not in column_mapping:
                        column_mapping[tid][j].update(map2[h])

            return [True, rowId]

    return [False, 0]


for result in cursor:
    if (tmp_tableid != -1 and tmp_tableid != result[0]) or (tmp_rowid != -1 and tmp_rowid != result[1]):
        # print("New row: ")
        # print(row)
        map2 = defaultdict(set)
        for y in range(len(row)):
            map2[row[y]].add(y)

        cIE = checkIfExists(row, row_map, rows, tmp_tableid, map2)
        if cIE[0]:
            # print("row exists in input: tableid: "+str(tmp_tableid)+" rowid: "+str(tmp_rowid))
            dup.append((cIE[1], (tmp_tableid, tmp_rowid)))
            tableIds_length_to_load.add(tmp_tableid)
        row = []
    tmp_tableid = result[0]
    tmp_rowid = result[1]
    row.append(str(result[3]))

# Gleicher aufruf wie aus schleife
if tmp_tableid != -1:
    map2 = defaultdict(set)
    for y in range(len(row)):
        map2[row[y]].add(y)

    cIE = checkIfExists(row, row_map, rows, tmp_tableid, map2)
    if cIE[0]:
        # print("row exists in input: tableid: "+str(tmp_tableid)+" rowid: "+str(tmp_rowid))
        dup.append((cIE[1], (tmp_tableid, tmp_rowid)))
        tableIds_length_to_load.add(tmp_tableid)

    duplicates = defaultdict(list)
    for i in dup:
        duplicates[i[1][0]].append((i[0], i[1][1]))

    if len(dup) > 0:
        # Check duplicate rows for duplicate tables
        in_clause_tableids = ', '.join(str(s) for s in tableIds_length_to_load)
        # Get number of rows in table:
        cursor.execute(
            f'SELECT tableid, MAX(rowid) FROM "mate_main_tokenized" WHERE tableid IN ({in_clause_tableids}) GROUP BY tableid')
        for result in cursor:
            t1_dup = []
            t2_dup = []
            for value in duplicates[result[0]]:
                t1_dup.append(value[0])
                t2_dup.append(value[1])

            if (len(set(t1_dup)) >= len(rows[0]) or len(set(t2_dup)) >= result[1]):
                # print("found duplicate table: " + str(result[0]))
                duplicate_tables.append(result[0])

### TIME TRACKING START ###
stop = datetime.datetime.now()
time_diff = stop - start
print("Computation took (ms): " + str(int(time_diff.total_seconds() * 1000)))
### TIME TRACKING END ###

if len(duplicate_tables) == 0:
    print("NO DUPLICATE TABLES FOUND")

print("Found duplicates (JSON):")
# print(json.dumps(dup))
print("\n\nFound duplicate tables (JSON):")
# print(json.dumps(duplicate_tables))
print("SUM: " + str(counter_sum))
print("SUM 2: " + str(counter_sum_2))
