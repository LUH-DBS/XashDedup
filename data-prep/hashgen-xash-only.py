import math
from collections import Counter

import numpy as np
import psycopg2
import psycopg2.extras


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

    super_key_column : str

    hash_size : int
        Number of bits.
    """
    conn = psycopg2.connect(user="postgres", password="", database="postgres", host="localhost", port=5432, sslmode="disable")
    conn.autocommit = True
    print("Successfully connected!")
    cur = conn.cursor()
    cur2 = conn.cursor()

    increments = 75000  # increments
    tmp_min_tableid = 135000000  # start table
    max_table = 150000000  # end table
    tmp_max_tableid = tmp_min_tableid + increments
    while True:
        print("Querying (" + str(tmp_min_tableid) + "," + str(tmp_max_tableid) + ")")
        cur.execute(
            f'SELECT tableid, rowid, colid, tokenized, super_key FROM {main_table} WHERE tableid >= {tmp_min_tableid} AND tableid <= {tmp_max_tableid} ORDER BY tableid, rowid')

        tmp_rowid = -1
        tmp_tableid = -1
        super_key_xash = 0
        params_list = []

        for result in cur:
            if (tmp_tableid != -1 and tmp_tableid != result[0]) or (tmp_rowid != -1 and tmp_rowid != result[1]):
                super_key_xash = np.binary_repr(super_key_xash).zfill(128)
                if result[4] != super_key_xash:
                    params_list.append((super_key_xash, tmp_tableid, tmp_rowid))
                super_key_xash = 0

            tmp_tableid = result[0]
            tmp_rowid = result[1]
            super_key_xash = super_key_xash | XASH(str(result[3]), hash_size)

        if tmp_tableid != -1:
            params_list.append((np.binary_repr(super_key_xash).zfill(128), tmp_tableid, tmp_rowid))

        tmp_min_tableid = tmp_max_tableid
        tmp_max_tableid = tmp_max_tableid + increments

        print("Executing insert query")
        psycopg2.extras.execute_batch(cur2,
                                      f'UPDATE {hash_table} SET super_key_xash = %s WHERE tableid = %s AND rowid = %s;',
                                      params_list)

        if tmp_min_tableid > max_table:
            print("finished")
            break


generate_index("mate_main_tokenized")
