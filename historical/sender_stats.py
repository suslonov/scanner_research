#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
import json
import pandas as pd
import numpy as np

from db.bot_db import DBMySQL
from db.remote import open_remote_port, close_remote_port, RemoteServer

PARAMETERS_FILE = "~/git/scanner_research/llm_contract_scanner/parameters.json"
CSV_FILE = '/media/Data/csv/sniping_test_'


with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None


with RemoteServer(remote=REMOTE) as server:
    with DBMySQL(port=server.local_bind_port) as db:
            



df_minblocks = df_price_history.groupby(["pair_id"])["block_number"].min()

df_minblocks = df_minblocks.to_frame()
df_minblocks["pair_id"] = df_minblocks.index
minblocks_pairs = [tuple(p) for p in df_minblocks.values]

df_price_history["block_pair"] = list(zip(df_price_history["block_number"], df_price_history["pair_id"]))

df_minblocks_trades = df_price_history.loc[(df_price_history["block_pair"].isin(minblocks_pairs)) &
                     (df_price_history["operation"] == "swap V2")].groupby("pair_id")["pair_id"].count()


print(len(df_minblocks))
print(len(df_minblocks_trades))
print(len(df_minblocks_trades)/len(df_minblocks))



df_senders = df_price_history.loc[(df_price_history["block_pair"].isin(minblocks_pairs)) &
                     (df_price_history["operation"] == "swap V2")].groupby("sender")["sender"].count()

df_senders.sort_values(inplace = True, ascending=False)

df_senders.iloc[:10]
df_senders.sum()


qq = df_price_history.loc[(df_price_history["block_pair"].isin(minblocks_pairs)) &
                     (df_price_history["operation"] == "swap V2") &
                     (df_price_history["sender"] == "0x7a250d5630b4cf539739df2c5dacb4c659f2488d")]