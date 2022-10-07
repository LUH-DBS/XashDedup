# Lake Deduplication

_sortall sorts row values before running the algorithm, while the other version only sorts all row values on super key
match.

serial-search is the version without any hash.

As the algorithms expect a range of table ids, the command is `python3 xash_buckets.py 1 10000`.