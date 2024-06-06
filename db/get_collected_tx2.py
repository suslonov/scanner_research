#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import json
import MySQLdb
import pandas as pd
import numpy as np

def fetch_with_description(cursor):
    return [{n[0]: v for n, v in zip(cursor.description, row)} for row in cursor.fetchall()]

def fetch_to_df(cursor, table_name, index_list):
    sql = "select * from " + table_name
    cursor.execute(sql)
    res = fetch_with_description(cursor)
    return pd.DataFrame.from_records(res, index = index_list)

KEY_FILE = '../keys/aws_db.sec'
with open(KEY_FILE, 'r') as f:
    users = json.load(f)

db_host = users["aws"]["ip"]
db_user = users["aws"]["user"]
db_passwd = users["aws"]["password"]
db_name = "mev_bot_poc_2"

db_connection = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
cursor = db_connection.cursor()

df_attacks = fetch_to_df(cursor, "attacks", ["id"])
df_blocks = fetch_to_df(cursor, "blocks", ["id"])
df_bundles = fetch_to_df(cursor, "bundles", ["id"])
df_competitor_bundles = fetch_to_df(cursor, "competitor_bundles", ["id"])
df_competitor_sandwich_swap_attack_components = fetch_to_df(cursor, "competitor_sandwich_swap_attack_components", ["id"])
df_competitor_sandwich_swap_attack_transactions = fetch_to_df(cursor, "competitor_sandwich_swap_attack_transactions", ["id"])
df_sandwich_swap_attack_components = fetch_to_df(cursor, "sandwich_swap_attack_components", ["id"])
df_sandwich_swap_attack_transactions = fetch_to_df(cursor, "sandwich_swap_attack_transactions", ["id"])
df_sandwich_swap_targets = fetch_to_df(cursor, "sandwich_swap_targets", ["id"])
df_whitelisted_tokens = fetch_to_df(cursor, "whitelisted_tokens", ["id"])


db_connection.close()

# df_blocks = fetch_to_df(cursor, "blocks", ["id"])
# df_blocks.iloc[-1]

