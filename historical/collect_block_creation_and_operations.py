#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
import json
import pandas as pd
import numpy as np
import requests
from _utils.web3connect import web3connect2

from db.bot_db import DBMySQL
from db.remote import RemoteServer

HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_GETCONTRACTCREATION = 'http://api.etherscan.io/api?module=contract&action=getcontractcreation&contractaddresses={}&apikey={}'
KEY_FILE = '../keys/alchemy.sec'
PARAMETERS_FILE = "~/git/scanner_research/parameters.json"
CSV_FILE = '/media/Data/csv/'

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None

with RemoteServer(remote=REMOTE) as server:
    with DBMySQL(port=server.local_bind_port) as db:
        tokens = db.get_tokens()
        
df_tokens = pd.DataFrame.from_records(tokens, index = ["token"])

with RemoteServer(remote=REMOTE) as server:
    with DBMySQL(port=server.local_bind_port) as db:
        pairs = db.get_pairs()
        
df_pairs = pd.DataFrame.from_records(pairs, index = ["pair_id"])

sql = "select pair_id, operation, min(block_number) block_number from t_event_history2 group by pair_id, operation"
with RemoteServer(remote=REMOTE) as server:
    with DBMySQL(port=server.local_bind_port) as db:
        first_all = db.exec_sql_dict_list(sql)

df_events = pd.DataFrame.from_records(first_all, index = ["pair_id", "operation"])
df_events1 = df_events.unstack()

df_events1.columns = [c[1] for c in df_events1.columns]

with open(os.path.expanduser(parameters["ETHERSCAN_KEY_FILE"]), 'r') as f:
    k1 = f.readline()
    ETHERSCAN_KEY = k1.strip('\n')

w3_direct, cached_session, w3, latest_block, _ = web3connect2(KEY_FILE)
df_tokens["creation_block"] = np.nan

ii = 0
print(len(df_tokens))
while (ii + 1) * 5  < len(df_tokens):
    print(ii*5)
    tokens_str = "".join([t + "," for t in list(df_tokens.index[ii*5:(ii+1)*5])])[:-1]
    res = requests.get(ETHERSCAN_GETCONTRACTCREATION.format(tokens_str, ETHERSCAN_KEY), headers=HEADERS)
    d = res.json()
    for t in d["result"]:
        transaction = w3_direct.eth.get_transaction(t["txHash"])
        df_tokens.loc[t["contractAddress"], "creation_block"] = transaction["blockNumber"]
    ii += 1
    
df_tokens[["creation_block"]].to_csv(CSV_FILE + "df_tokens_with_creation.csv")

df_events1["token0"] = np.nan
df_events1["token1"] = np.nan
df_events1["token0"].update(df_pairs["token0"])
df_events1["token1"].update(df_pairs["token1"])
df_events1.sort_index(inplace=True, ascending=False)
df_events1["creation_block"] = np.nan


for i in df_events1.index:
    if df_events1.loc[i,"token0"] in df_tokens.index:
        if df_events1.loc[i,"token1"] in df_tokens.index:
            df_events1.loc[i,"creation_block"] = max(df_tokens.loc[df_events1.loc[i,"token0"], "creation_block"],
                                             df_tokens.loc[df_events1.loc[i,"token1"], "creation_block"])
        else:
            df_events1.loc[i,"creation_block"] = df_tokens.loc[df_events1.loc[i,"token0"], "creation_block"]
    elif df_events1.loc[i,"token1"] in df_tokens.index:
            df_events1.loc[i,"creation_block"] = df_tokens.loc[df_events1.loc[i,"token1"], "creation_block"]


df_pairs["creation_block"] = np.nan

ii = 0
len_pairs = len(df_pairs)
for ii in range(4000):
    print(ii*5)
    
    df_pairs.iloc[len_pairs -(ii+1) * 5:len_pairs -ii * 5]["pair"]
    tokens_str = "".join([t + "," for t in list(df_pairs.iloc[len_pairs -(ii+1) * 5:len_pairs -ii * 5]["pair"])])[:-1]
    res = requests.get(ETHERSCAN_GETCONTRACTCREATION.format(tokens_str, ETHERSCAN_KEY), headers=HEADERS)
    d = res.json()
    for t in d["result"]:
        transaction = w3_direct.eth.get_transaction(t["txHash"])
        df_pairs.loc[df_pairs["pair"] == t["contractAddress"], "creation_block"] = transaction["blockNumber"]
    ii += 1

df_pairs.rename(columns={"creation_block": "pair_creation_block"}, inplace=True)
df_pairs_not_nan = df_pairs["pair_creation_block"].dropna(inplace=False).to_frame()
df_events1["pair_creation_block"] = np.nan
df_events1["pair_creation_block"].update(df_pairs_not_nan["pair_creation_block"])


df_events1[:10000].to_csv(CSV_FILE + "creation_mint_burn_swap_first_block10000.csv")

