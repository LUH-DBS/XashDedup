import hashlib
import math
import re
from collections import Counter
from typing import Dict, Tuple, List

import numpy as np
import psycopg2
import psycopg2.extras
import pyhash
from simhash import Simhash


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
        result = result | int(math.pow(2, location))

    # rotation
    n = int(result)
    d = int((length_bit_start * (len(token) % (hash_size - length_bit_start))) / (
            hash_size - length_bit_start))
    int_bits = int(length_bit_start)
    x = n << d
    y = n >> (int_bits - d)
    r = int(math.pow(2, int_bits))
    result = int((x | y) % r)

    result = int(result) | int(math.pow(2, len(token) % (hash_size - length_bit_start)) * math.pow(2, length_bit_start))

    return result


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


def generate_index(main_table: str = 'main_tokenized',
                   hash_table: str = 'mate_main_tokenized_hashes',
                   super_key_column: str = 'super_key',
                   hash_size: int = 128
                   ) -> None:
    """Generates MATE index and stores it in DB.

    Parameters
    ----------
    main_table : str
        Main inverted index table.

    hash_table : str
        Super Key table

    super_key_column : str

    hash_size : int
        Number of bits.
    """
    conn = psycopg2.connect(user="postgres", password="", database="postgres", host="localhost", port=5432, sslmode="disable")
    conn.autocommit = True
    print("Successfully connected!")
    cur = conn.cursor()
    cur2 = conn.cursor()

    increments = 25000  # increments
    tmp_min_tableid = 72766911  # start table
    max_table = 145533822  # end table
    tmp_max_tableid = tmp_min_tableid + increments
    while True:
        print("Querying (" + str(tmp_min_tableid) + "," + str(tmp_max_tableid) + ")")
        cur.execute(
            f'SELECT tableid, rowid, colid, tokenized, super_key FROM {main_table} WHERE tableid > {tmp_min_tableid} AND tableid < {tmp_max_tableid} ORDER BY tableid, rowid')

        tmp_rowid = -1
        tmp_tableid = -1
        super_key_cityhash = 0
        super_key_simhash = 0
        super_key_md5 = 0
        super_key_xash = 0
        params_list = []

        for result in cur:
            if (tmp_tableid != -1 and tmp_tableid != result[0]) or (tmp_rowid != -1 and tmp_rowid != result[1]):
                super_key_cityhash = np.binary_repr(super_key_cityhash).zfill(128)
                super_key_simhash = np.binary_repr(super_key_simhash).zfill(128)
                super_key_md5 = np.binary_repr(super_key_md5).zfill(128)
                super_key_xash = np.binary_repr(super_key_xash).zfill(128)
                params_list.append((tmp_tableid, tmp_rowid, super_key_cityhash, super_key_simhash, super_key_md5,
                                    super_key_xash, super_key_cityhash, super_key_simhash, super_key_md5,
                                    super_key_xash))
                super_key_cityhash = 0
                super_key_simhash = 0
                super_key_md5 = 0
                super_key_xash = 0

            tmp_tableid = result[0]
            tmp_rowid = result[1]
            super_key_cityhash = super_key_cityhash | generate_CITY_hash(dict(), str(result[3]), hash_size)[1]
            super_key_simhash = super_key_simhash | generate_SIM_hash(dict(), str(result[3]), hash_size)[1]
            super_key_md5 = super_key_md5 | generate_MD5_hash(dict(), str(result[3]), hash_size)[1]
            super_key_xash = super_key_xash | XASH(str(result[3]), hash_size)

        if tmp_tableid != -1:
            super_key_cityhash = np.binary_repr(super_key_cityhash).zfill(128)
            super_key_simhash = np.binary_repr(super_key_simhash).zfill(128)
            super_key_md5 = np.binary_repr(super_key_md5).zfill(128)
            super_key_xash = np.binary_repr(super_key_xash).zfill(128)
            params_list.append((tmp_tableid, tmp_rowid, super_key_cityhash, super_key_simhash, super_key_md5,
                                super_key_xash, super_key_cityhash, super_key_simhash, super_key_md5, super_key_xash))

        tmp_min_tableid = tmp_max_tableid
        tmp_max_tableid = tmp_max_tableid + increments

        print("Executing insert query")
        psycopg2.extras.execute_batch(cur2,
                                      f'INSERT INTO {hash_table} (tableid, rowid, super_key_cityhash, super_key_simhash, super_key_md5, super_key_xash) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (tableid, rowid) DO UPDATE SET super_key_cityhash = %s, super_key_simhash = %s, super_key_md5 = %s, super_key_xash = %s;',
                                      params_list)

        if tmp_min_tableid > max_table:
            print("finished")
            break


generate_index("mate_main_tokenized")
