import datetime
import json
import math
import re
import sys
from collections import defaultdict, Counter
from typing import Dict, Tuple, List
from DuplicateTableDetection import DuplicateTableDetection

import numpy as np
from simhash import Simhash

import db_handler


def XASH(token: str, hash_size: int = 128) -> int:
    """Computes XASH for given token.

    Parameters
    ----------
    token : str
        Token.

    hash_size : int
        Number of bits.

    Returns
    -------
    int
        XASH value.
    """
    number_of_ones = 5
    char = [' ', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
            'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    segment_size_dict = {64: 1, 128: 3, 256: 6, 512: 13}
    segment_size = segment_size_dict[hash_size]
    length_bit_start = 37 * segment_size
    result = 0
    cnt_dict = Counter(token)
    selected_chars = [y[0] for y in sorted(cnt_dict.items(), key=lambda x: (x[1], x[0]), reverse=False)[:number_of_ones]]
    for c in selected_chars:
        if c not in char:
            continue
        indices = [i for i, ltr in enumerate(token) if ltr == c]
        mean_index = np.mean(indices)
        token_size = len(token)
        for i in np.arange(segment_size):
            if mean_index <= ((i + 1) * token_size / segment_size):
                location = char.index(c) * segment_size + i
                break
        result = result | int(math.pow(2, location))

    # rotation
    '''
    n = int(result)
    d = int((length_bit_start * (len(token) % (hash_size - length_bit_start))) / (
            hash_size - length_bit_start))
    int_bits = int(length_bit_start)
    x = n << d
    y = n >> (int_bits - d)
    r = int(math.pow(2, int_bits))
    result = int((x | y) % r)
    '''

    result = int(result) | int(math.pow(2, len(token) % (hash_size - length_bit_start)) * math.pow(2, length_bit_start))

    return result


data_tmp = db_handler.getTableData(int(sys.argv[1]), int(sys.argv[2]),False)
super_keys = defaultdict(dict)
data = [data_tmp,super_keys]
tables_buckets = {}
enable_print = False
duplicate_tables_only = False

counter_superkey = 0
counter_fp = 0
duplicates = []
duplicate_tables = []

## Generate simhash in structure ##
for tableid, table in data[0].items():
    for rowid, row in table.items():
        simhash = 0
        for colid, col in row.items():
            simhash = simhash | XASH(str(col),128)
        data[1][tableid][rowid] = simhash

## -- ##


print("Comparing tables between "+sys.argv[1]+" and "+sys.argv[2])
### TIME TRACKING START ###
start = datetime.datetime.now()
### TIME TRACKING END ###

# Group tables into buckets by no of cols
for tableId in range(int(sys.argv[1]),int(sys.argv[2])):
    if len(data[0][tableId][0]) not in tables_buckets: # Check if key exists
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