# Disabling rotation

on*.py includes the original super key generation with rotation.  
off*.py includes the super key generation without rotation.

_20-1cols uses the tables with 20 columns and executes test with 1-20 columns on these tables.

As the algorithms expect a range of table ids, the usage is for example `python3 off.py 1 10000`.