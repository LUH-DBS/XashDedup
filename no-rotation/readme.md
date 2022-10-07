# Disabling rotation

on*.py includes the original super key generation with rotation.  
off*.py includes the super key generation without rotation.

_per_bucket groups the number of false positives by the number of columns of the tables between 1-20 columns.

As the algorithms expect a range of table ids, the usage is for example `python3 off.py 1 10000`.