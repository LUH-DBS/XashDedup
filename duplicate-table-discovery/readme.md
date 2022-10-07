# Duplicate Table Discovery

## Using super keys

The duplicate table discovery algorithm expects an input table provided as a csv file. To run the discovery algorithm
using a hash function, `python3 hash.py xash table.csv` can be executed. Instead of xash, simhash, cityhash and md5 can
be provided as an argument.

## Without super keys

`python3 no-hash.py table.csv`

----

## Tables

Example tables are provided as csv files in this folder or can be retrieved from a database
using `python3 table_to_csv.py {tableid}`.