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
TEST_AMOUNT_ETH = 0.05 #ETH
TEST_AMOUNT_ETH18 = TEST_AMOUNT_ETH * 1e18 #ETH

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None

s = "select first_block_number, (select min(block_number) from t_event_history where "
s += "t_event_history.pair_id=t_pairs.pair_id "
s += "and operation='burn') burn_block_number "
s += "from t_pairs where pair_id >= 260000 and pair_id < 290000"


with RemoteServer(remote=REMOTE) as server:
    with DBMySQL(port=server.local_bind_port) as db:
        fields, l = db.exec_sql_plain_list(s)

df = pd.DataFrame(l, columns=fields)

df["life_span"] = df["burn_block_number"] - df["first_block_number"]
df["life_span"].mean()
df.loc[df["life_span"] <=100, "life_span"].mean()
df.loc[df["life_span"] <=1000, "life_span"].mean()

df["life_span"].std()
df.loc[df["life_span"] <=100, "life_span"].std()

df["life_span"].hist(bins=100)
df.loc[df["life_span"] <=100, "life_span"].hist(bins=100)
df.loc[df["life_span"] <=1000, "life_span"].hist(bins=100)

df.loc[df["life_span"] <=100, "life_span"].count()