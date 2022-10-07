# Data Preparation

## Super Keys

### Duplicate Table Discovery

The algorithm requires the super keys for all hash functions used to be saved in the database. To generate the super
keys, use `python3 hashgen_new.py`.

### Lake deduplication

The super keys can either be generated when running the algorithm or in a pre-processing step. To generate only the
super key, use `python3 hashgen-xash-only`.

## Datasets

### DWTC

Follow the instructions from https://github.com/LUH-DBS/MATE.

### Wikipedia

Files obtained from https://databus.dbpedia.org/dbpedia/text/raw-tables saved as "raw-tables_lang=en.ttl" can be
inserted into a database using `python3 wikipedia.py`. The super keys need to be generated as described above.