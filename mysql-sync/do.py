import sys
import time
import os
import re
import shutil
import requests
from itertools import islice
from mysql.connector import connect, Error

connection = connect(host=os.environ['SCOPECREEP_DB_HOST'],
        port=32939,
        user='admin',
        password=os.environ['SCOPECREEP_DB_PASSWORD'],
        database='DATA')


table = sys.argv[1]


primaryKeys = []
projection = []

class Upsert(object):
    def __init__(self):
        self.keys = []
        self.vals = []

    def to_sql(self):
        return """
            UPDATE {0} SET {1} WHERE {2};
        """.format(table, ", ".join(self.vals), " AND ".join(self.keys)).strip()

def prepare_val(v):
    if v == None:
        return "null"
    elif isinstance(v, str):
        return "'{0}'".format(v)
    return v

with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM " + table)
    for i in range(0, len(cursor.column_names)):
        col = cursor.column_names[i]
        if col == "{0}_id".format(table):
            primaryKeys += [col]
        elif col in ('insert_timestamp','update_timestamp', 'description', 'hubspot_company_id'):
            continue
        else:
            projection += [col]

    upserts = []
    for row in cursor.fetchall():
        upsert = Upsert()
        for i in range(0, len(row)):
            val = row[i]
            col = cursor.column_names[i]
            if col in primaryKeys:
                upsert.keys += ["{0} = {1}".format(col, val)]
            elif col in projection:
                v = prepare_val(val)
                upsert.vals += ["{0} = {1}".format(col, prepare_val(val))]
        upserts += [upsert]
    for upsert in upserts:
        print(upsert.to_sql())


