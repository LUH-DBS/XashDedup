import csv
import sys

import psycopg2

tableid = sys.argv[1]

conn = psycopg2.connect(user="postgres", password="", database="postgres", host="localhost", port=5432, sslmode="disable")
conn.autocommit = True
print("Successfully connected!")
cursor = conn.cursor()

cursor.execute(
    f'SELECT rowid, colid, tokenized FROM "mate_main_tokenized" WHERE "tableid" = {tableid} ORDER BY rowid, colid')
results = cursor.fetchall()
rows = []
tmp_rowid = results[0][0]
row_items = []
for result in results:
    if tmp_rowid != result[0]:
        rows.append(row_items)
        row_items = []
    tmp_rowid = result[0]
    row_items.append(str(result[2]))

rows.append(row_items)

with open('table.csv', 'w', newline='', encoding='utf-8') as file:
    csv_writer = csv.writer(file, delimiter=',')
    csv_writer.writerows(rows)
