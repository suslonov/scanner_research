#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg
from datetime import datetime
import json
import pandas as pd

def fetch_with_description(cursor):
    return [{n[0]: v for n, v in zip(cursor.description, row)} for row in cursor.fetchall()]

KEY_FILE = '../keys/postgres_db.sec'
with open(KEY_FILE, 'r') as f:
    users = json.load(f)

db_name = "graph-node"
db_schema = "sgd1"

conn = psycopg.connect(
    host=users["postgres"]["host"],
    port=users["postgres"]["port"],
    dbname=db_name,
    user=users["postgres"]["user"],
    password=users["postgres"]["password"])


s = "SELECT * FROM information_schema.tables  WHERE table_schema = '" + db_schema + "'"
# s = "SELECT * FROM information_schema.tables"
tables = fetch_with_description(conn.execute(s))
df_tables = pd.DataFrame.from_records(tables)

s = "SELECT * FROM " + db_schema + ".token"
tokens = fetch_with_description(conn.execute(s))
df_tokens = pd.DataFrame.from_records(tokens)

s = "SELECT * FROM " + db_schema + ".pair"
pairs = fetch_with_description(conn.execute(s))
df_pairs = pd.DataFrame.from_records(pairs)

print(len(pairs))
print(df_pairs["created_at_block_number"].max())
print(df_pairs["created_at_timestamp"].max())
print(datetime.utcfromtimestamp(int(df_pairs["created_at_timestamp"].max())))

conn.close()
