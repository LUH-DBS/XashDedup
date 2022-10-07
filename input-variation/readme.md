# Input format

xash*.py includes the super key generation by OR'ing the hash values.  
concat*.py includes the super key generation by concatenating the row values.

_per_bucket groups the number of false positives by the number of columns of the tables between 1-20 columns.
_20-1cols uses the tables with 20 columns and executes test with 1-20 columns on these tables.

As the algorithms expect a range of table ids, the usage is for example `python3 concat.py 1 10000`.