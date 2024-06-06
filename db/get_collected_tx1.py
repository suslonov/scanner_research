#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import json
import MySQLdb
import pandas as pd
import numpy as np

def fetch_with_description(cursor):
    return [{n[0]: v for n, v in zip(cursor.description, row)} for row in cursor.fetchall()]

KEY_FILE = '../keys/aws_db.sec'
with open(KEY_FILE, 'r') as f:
    users = json.load(f)

db_host = users["aws"]["ip"]
db_user = users["aws"]["user"]
db_passwd = users["aws"]["password"]
db_name = "mev_bot_poc"
# db_name = "mev_bot_poc_2"

db_connection = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
cursor = db_connection.cursor()

s1 = "select * from sandwiches"
cursor.execute(s1)
l1 = fetch_with_description(cursor)

s21 = "select distinct target_token_in_address from sandwiches"
cursor.execute(s21)
l21 = fetch_with_description(cursor)

s22 = "select distinct target_token_out_address from sandwiches"
cursor.execute(s22)
l22 = fetch_with_description(cursor)




df = pd.DataFrame.from_records(l1, index = ["id"])
print(df["status"].unique())

df1 = df.loc[df["status"] == 'not sandwiched']



db_connection.close()

df1.iloc[0]["target_tx_hash"]

# df2 = df1.loc[(df1["target_token_in_address"] == "0x06997789943ba32eB6E73b0a9A424971C3d2E23e") |
#              (df1["target_token_out_address"] == "0x06997789943ba32eB6E73b0a9A424971C3d2E23e")]

df2 = df1.loc[(df1["target_token_in_address"] == "0xcc6c4f450f1d4aec71c46f240a6bd50c4e556b8a") |
             (df1["target_token_out_address"] == "0xcc6c4f450f1d4aec71c46f240a6bd50c4e556b8a")]


ddd = df.loc[df["target_tx_hash"]=="0xffc140cda2582cc10e3c28c66469cb7874cf499b54aaa3896a1807f7eae89248"]
ddd.iloc[0]

df["token"] = ""
df.loc[(df["target_token_in_address"] != "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), "token"] = df.loc[(df["target_token_in_address"] != "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), "target_token_in_address"]
df.loc[(df["target_token_out_address"] != "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), "token"] = df.loc[(df["target_token_out_address"] != "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), "target_token_out_address"]

df_not_sandwich = df.loc[df["status"] == "not sandwiched"].groupby(["token"])["updateTime"].min().to_frame()
df_complete = df.loc[df["status"] == "complete"].groupby(["token"])["updateTime"].max().to_frame()
df_complete.rename(columns={"updateTime": "lastSandwich"}, inplace=True)
df_not_sandwich.rename(columns={"updateTime": "notSandwich"}, inplace=True)

df_complete["notSandwich"] = df_complete["lastSandwich"]
df_complete.update(df_not_sandwich["notSandwich"])
df_complete["timeDiff"] = df_complete["notSandwich"] - df_complete["lastSandwich"]

df_not_sandwich["lastSandwich"] = df_not_sandwich["notSandwich"]
df_not_sandwich.update(df_complete["lastSandwich"])
df_not_sandwich["timeDiff"] = df_not_sandwich["notSandwich"] - df_not_sandwich["lastSandwich"]


dfqqq = df1.groupby(["pair_address"])["pair_address"].count()
dfqqq.sort_values()

qqq = df1.loc[df1["pair_address"] == '0xF2E157A75Afb5f27536EcE5CD1734477Ec105b4f']
