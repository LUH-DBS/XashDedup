import sys
from collections import defaultdict

import psycopg2

conn = psycopg2.connect(user="postgres", password="", database="postgres", host="localhost", port=5432, sslmode="disable")
conn.autocommit = True
print("Successfully connected!")
cursor = conn.cursor()

collectMemory = True
memory = 0


def getTableData(t1, t2, returnDict=False):
    global memory
    rowValues = defaultdict(lambda: defaultdict(dict))
    rowSuperKeys = defaultdict(dict)

    cursor.execute(
        f'SELECT tableid, rowid, colid, tokenized, super_key FROM "wikipediatables_mate_main_tokenized" WHERE tableid >= {t1} AND tableid <= {t2}')
    results = cursor.fetchall()

    if collectMemory:
        memory = memory + sys.getsizeof(results)

    for row in results:
        rowValues[row[0]][row[1]][row[2]] = str(row[3])
        rowSuperKeys[row[0]][row[1]] = int(row[4], 2)  # convert to int

    if returnDict:
        return [rowValues, rowSuperKeys]
    else:
        return rowValues


def getTableDataSuperkeyOnly(t1, t2):
    global memory
    rowSuperKeys = defaultdict(dict)

    cursor.execute(
        f'SELECT tableid, rowid, super_key, count(colid) as no_cols FROM "wikipediatables_mate_main_tokenized" WHERE tableid >= {t1} AND tableid <= {t2} GROUP BY tableid, rowid, super_key')
    results = cursor.fetchall()

    if collectMemory:
        memory = memory + sys.getsizeof(results)

    for row in results:
        rowSuperKeys[row[0]][row[1]] = [row[2], row[3]]  # key 2 is super key

    return rowSuperKeys


def getRowValues(t1, t1_rowid, t2, t2_rowid):
    global memory
    rowValues = defaultdict(lambda: defaultdict(dict))

    cursor.execute(
        f'SELECT tableid, rowid, colid, tokenized FROM "wikipediatables_mate_main_tokenized" WHERE (tableid = {t1} AND rowid = {t1_rowid}) OR (tableid = {t2} AND rowid = {t2_rowid})')
    results = cursor.fetchall()

    if collectMemory:
        memory = memory + sys.getsizeof(results)

    for row in results:
        rowValues[row[0]][row[1]][row[2]] = row[3]

    return rowValues


def getRowValuesAll(tableIdRowId):
    global memory
    rowValues = defaultdict(lambda: defaultdict(dict))
    rowSuperKeys = defaultdict(dict)

    sql = ""
    for tableId, rowId in tableIdRowId:
        sql = sql + "(rowid = " + str(rowId) + " AND tableid = " + str(tableId) + ") OR "

    print()

    cursor.execute(
        f'SELECT tableid, rowid, colid, tokenized FROM "wikipediatables_mate_main_tokenized" WHERE ' + sql[:-4])
    results = cursor.fetchall()

    if collectMemory:
        memory = memory + sys.getsizeof(results)

    for row in results:
        rowValues[row[0]][row[1]][row[2]] = row[3]

    return rowValues


def getMemory():
    return memory
