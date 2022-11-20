import datetime
import json
import sys
from collections import defaultdict
from typing import Dict, Tuple

import pyhash

import db_handler
from DuplicateTableDetection import DuplicateTableDetection


def generate_CITY_hash(hash_dict: Dict, token: str, hash_size: int) -> Tuple[Dict, int]:
    """Calculates CITY Hash for token.

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
        hasher = pyhash.city_128()
    elif hash_size == 256:
        hasher = pyhash.city_fingerprint_256()
    cityh = hasher(token)
    hash_dict[token] = cityh
    return hash_dict, cityh


data_tmp = db_handler.getTableData(int(sys.argv[1]), int(sys.argv[2]), False)
super_keys = defaultdict(dict)
data = [data_tmp, super_keys]
tables_buckets = {}
enable_print = False

counter_superkey = 0
counter_fp = 0
duplicates = []
duplicate_tables = []

## Generate simhash in structure ##
for tableid, table in data[0].items():
    for rowid, row in table.items():
        simhash = 0
        for colid, col in row.items():
            simhash = simhash | generate_CITY_hash(dict(), str(col), 128)[1]
        data[1][tableid][rowid] = simhash


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
