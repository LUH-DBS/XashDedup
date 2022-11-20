import datetime
import hashlib
import json
import sys
from collections import defaultdict
from typing import Dict, Tuple

import db_handler
from DuplicateTableDetection import DuplicateTableDetection


def generate_MD5_hash(hash_dict: Dict, token: str, hash_size: int) -> Tuple[Dict, int]:
    """Calculates MD5 Hash for token.

    Parameters
    ----------
    hash_dict : Dict
        Dictionary of already computed hash values.

    token : str
        Input token.

    hash_size : int
        Number of bits.

    Returns
    -------
    Tuple[Dict, int]
        Dict: Updated hash_dict.
        int: Hash for given token.
    """
    if token in hash_dict:
        return hash_dict, hash_dict[token]
    if hash_size == 128:
        hasher = hashlib.md5()
    hasher.update(token.encode('UTF-8'))
    md5h = int(hasher.hexdigest(), 16)
    hash_dict[token] = md5h
    return hash_dict, md5h


data_tmp = db_handler.getTableData(int(sys.argv[1]), int(sys.argv[2]), False)
super_keys = defaultdict(dict)
data = [data_tmp, super_keys]
tables_buckets = {}
enable_print = False

counter_superkey = 0
counter_fp = 0
duplicates = []
duplicate_tables = []

## Generate md5hash in structure ##
for tableid, table in data[0].items():
    for rowid, row in table.items():
        md5hash = 0
        for colid, col in row.items():
            md5hash = md5hash | generate_MD5_hash(dict(), str(col), 128)[1]
        data[1][tableid][rowid] = md5hash


## -- ##

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
#print(json.dumps(duplicates))
print("\n\nFound duplicate tables (JSON):")
#print(json.dumps(duplicate_tables))
print("FP: " + str(DuplicateTableDetection.getFp()))
print("SUM: " + str(DuplicateTableDetection.getSum()))
print("\nMemory: " + str(db_handler.getMemory()))
