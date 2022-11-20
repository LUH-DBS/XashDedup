import datetime
import json
import re
import sys
from collections import defaultdict
from typing import Dict, Tuple, List

from simhash import Simhash

import db_handler
from DuplicateTableDetection import DuplicateTableDetection


def get_simhash_features(s: str) -> List[str]:
    """Returns SIM Hash features.

    Parameters
    ----------
    s : str
        Input value.

    Returns
    -------
    List[str]
        Features.
    """
    width = 3
    s = s.lower()
    s = re.sub(r'[^\w]+', '', s)
    return [s[i:i + width] for i in range(max(len(s) - width + 1, 1))]


def generate_SIM_hash(hash_dict: Dict, token: str, hash_size: int) -> Tuple[Dict, int]:
    """Calculates SIM Hash for token.

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
    simh = Simhash(get_simhash_features(token), f=hash_size).value
    hash_dict[token] = simh
    return hash_dict, simh


data_tmp = db_handler.getTableData(int(sys.argv[1]), int(sys.argv[2]), False)
super_keys = defaultdict(dict)
data = [data_tmp, super_keys]
tables_buckets = {}
enable_print = False

## Generate simhash in structure ##
for tableid, table in data[0].items():
    for rowid, row in table.items():
        simhash = 0
        for colid, col in row.items():
            simhash = simhash | generate_SIM_hash(dict(), str(col), 128)[1]
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