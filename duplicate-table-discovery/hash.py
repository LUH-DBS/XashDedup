import copy
import csv
import datetime
import hashlib
import math
import re
import sys
from collections import defaultdict, Counter
from typing import Dict, Tuple, List

import numpy as np
import psycopg2
import pyhash
# hash_type = "xash"
# hash_type = "cityhash"
from simhash import Simhash

hash_type = sys.argv[1]
input_table = sys.argv[2]

conn = psycopg2.connect(user="postgres", password="", database="postgres", host="localhost", port=5432, sslmode="disable")
conn.autocommit = True
print("Successfully connected!")
cursor = conn.cursor()


def genHash(token, hash_function):
    if hash_type == "xash":
        return XASH(str(token))
    elif hash_type == "cityhash":
        return generate_CITY_hash(dict(), str(token), 128)[1]
    elif hash_type == "md5":
        return generate_MD5_hash(dict(), str(token), 128)[1]
    elif hash_type == "simhash":
        return generate_SIM_hash(dict(), str(token), 128)[1]


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
    if hash_size == 64:
        hasher = pyhash.city_64()
    elif hash_size == 128:
        hasher = pyhash.city_128()
    elif hash_size == 256:
        hasher = pyhash.city_fingerprint_256()
    cityh = hasher(token)
    hash_dict[token] = cityh
    return hash_dict, cityh


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
    result_ = 0
    cnt_dict = Counter(token)
    selected_chars = [y[0] for y in
                      sorted(cnt_dict.items(), key=lambda x: (x[1], x[0]), reverse=False)[:number_of_ones]]
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
        result_ = result_ | int(math.pow(2, location))

    # rotation
    n = int(result_)
    d = int((length_bit_start * (len(token) % (hash_size - length_bit_start))) / (
            hash_size - length_bit_start))
    int_bits = int(length_bit_start)
    x = n << d
    y = n >> (int_bits - d)
    r = int(math.pow(2, int_bits))
    result_ = int((x | y) % r)

    result_ = int(result_) | int(
        math.pow(2, len(token) % (hash_size - length_bit_start)) * math.pow(2, length_bit_start))

    return result_


column_mapping = defaultdict(lambda: defaultdict(set))


def fpCheck(rowArray1, rowArray2, tid, hashmap, rowMap2):
    global map1
    global column_mapping

    if len(rowArray1) != len(rowArray2):
        return False

    # Check values to check false positive
    for i in rowArray2:
        # Check if value in hashmap
        if i not in hashmap or hashmap[i] == 0:
            return False
        else:
            hashmap[i] -= 1

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
                return False # Column mapping mismatch

    for h in rowArray1: # rowArray1 or 2?
        for j in map1[h]:
            if j not in column_mapping:
                column_mapping[tid][j].update(map2[h])

    return True


print("Started...")
### TIME TRACKING START ###
start = datetime.datetime.now()
### TIME TRACKING END ###

i = 0
counter_fp = 0
counter_superkey = 0
rows = defaultdict(dict)
superKeyMapping = defaultdict(list)
dup = []
duplicate_tables = []

map1 = defaultdict(set)
with open(input_table) as csv_file:
    reader = csv.reader(csv_file)
    count = {}
    for row in reader:
        rows[0][i] = row
        rows[1][i] = 0
        for cell in row:
            rows[1][i] = rows[1][i] | genHash(str(cell), hash_type)
        superKeyMapping[int(rows[1][i])].append(i)  # Map super key to rowid

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
for v in rows[1].values():
    in_clause = in_clause + "'" + str(np.binary_repr(v).zfill(128)) + "',"

print(in_clause[:-1])

cursor.execute(
    f'SELECT mate_main_tokenized.tableid, mate_main_tokenized.rowid, mate_main_tokenized.colid, mate_main_tokenized.tokenized, mate_main_tokenized_hashes.super_key_{hash_type} '
    f'FROM "mate_main_tokenized", "mate_main_tokenized_hashes" '
    f'WHERE mate_main_tokenized_hashes.super_key_{hash_type} IN ({in_clause[:-1]}) '
    f'AND mate_main_tokenized.tableid = mate_main_tokenized_hashes.tableid AND mate_main_tokenized.rowid = mate_main_tokenized_hashes.rowid '
    f'ORDER BY tableid, rowid, colid LIMIT 1000000')
# cursor.execute(f'SELECT tableid, rowid, colid, tokenized, super_key FROM "wikipediatables_mate_main_tokenized" WHERE super_key IN ({in_clause[:-1]}) ORDER BY tableid, rowid, colid')
# results = cursor.fetchall()
tmp_rowid = -1
tmp_tableid = -1
tmp_superkey = 0
row = []
tableIds_length_to_load = set()
for result in cursor:
    # Build complete rows from column values
    if (tmp_tableid != -1 and tmp_tableid != result[0]) or (tmp_rowid != -1 and tmp_rowid != result[1]):
        map2 = defaultdict(set)
        for y in range(len(row)):
            map2[row[y]].add(y)

        for rowId in superKeyMapping[int(tmp_superkey, 2)]:
            count_copy = copy.deepcopy(count[rowId])

            if fpCheck(rows[0][rowId], row, tmp_tableid, count_copy, map2): # Check for false positive
                dup.append((rowId, (tmp_tableid, tmp_rowid)))
                tableIds_length_to_load.add(tmp_tableid)
            else:
                counter_fp = counter_fp + 1
            counter_superkey = counter_superkey + 1
        row = []
    tmp_tableid = result[0]
    tmp_rowid = result[1]
    tmp_superkey = result[4]
    row.append(str(result[3]))

if tmp_tableid != -1:  # check that at least one row is found
    map2 = defaultdict(set)
    for y in range(len(row)):
        map2[row[y]].add(y)

    for rowId in superKeyMapping[int(tmp_superkey, 2)]:
        count_copy = copy.deepcopy(count[rowId])

        if fpCheck(rows[0][rowId], row, tmp_tableid, count_copy, map2):
            dup.append((rowId, (tmp_tableid, tmp_rowid)))
            tableIds_length_to_load.add(tmp_tableid)
        else:
            counter_fp = counter_fp + 1
        counter_superkey = counter_superkey + 1

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
print("FP: " + str(counter_fp))
print("SUM: " + str(counter_superkey))
