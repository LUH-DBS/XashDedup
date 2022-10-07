# Prepare DBpedia wikipedia tables "raw-tables_lang=en.ttl" file and insert in tokenized form in specified table

import re
import signal

import pandas as panda
import psycopg2


class TimeoutException(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutException


signal.signal(signal.SIGALRM, timeout_handler)

p = re.compile(".*(<table.*<\/table>).*")
conn = psycopg2.connect(user="postgres", password="", database="postgres", host="localhost", port=5432, sslmode="disable")
conn.autocommit = True
print("Successfully connected!")
cursor = conn.cursor()

with open(r'../raw-tables_lang=en.ttl', encoding='utf-8') as infile:
    tableid = 0
    for line in infile:
        signal.alarm(10)
        if "<table" in line:
            result = p.search(line)
            html = result.group(1).replace('\\', "")
            try:
                tables = panda.read_html(html)
                rowid = -1
                tableid = tableid + 1

                for row in tables[0].values.tolist():
                    rowid = rowid + 1
                    colid = -1
                    for value in row:
                        colid = colid + 1
                        cursor.execute(
                            f'INSERT INTO "wikipediatables_mate_main_tokenized" ("tokenized", "tableid", "colid", "rowid", "super_key") VALUES (%s, %s, %s, %s, \'00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\');',
                            (str(value).lower(), tableid, colid, rowid))

            except TimeoutException:
                print("Timeout. Skipping table")
                continue
            except Exception as e:
                print(type(e), e)
                print("error")
                signal.alarm(0)
            else:
                signal.alarm(0)
