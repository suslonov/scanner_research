#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
import json
import pandas as pd
import numpy as np

from db.bot_db import DBMySQL
from db.remote import RemoteServer
from _utils.uniswap import amount_out_v2

PARAMETERS_FILE = "~/git/scanner_research/parameters.json"
TEST_RATIO = 0.05

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None


def get_pair(pair_id):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_pair(pair_id)

def get_token(token):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_token(token)

def get_pair_history(pair_id):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_event_history(pair_id)

pair_id = 302944

pair_data = get_pair(pair_id)
token_data = get_token(pair_data[0]["token"])
pair_history = get_pair_history(pair_id)
pair_history = {p["event_id"]: p for p in pair_history}

print(pair_data[0])
print(token_data)
dtypes = {'pair_id': "int64", 'transactionHash': "object", 'block_number': "int", 'timeStamp': "datetime64", 'operation': "object",
       'sender': "object", 'amount0': "object", 'amount1': "object", 'amount0In': "object", 'amount1In': "object", 'amount0Out': "object",
       'amount1Out': "object"}
df_pair_history = pd.DataFrame.from_dict(pair_history, orient="index", dtype="object")

df_pair_history["reserve0"] = (
    df_pair_history["amount0"].where(df_pair_history["operation"] == "mint", 0).cumsum() -
    df_pair_history["amount0"].where(df_pair_history["operation"] == "burn", 0).cumsum() +
    df_pair_history["amount0In"].cumsum() -
    df_pair_history["amount0Out"].cumsum())
df_pair_history["reserve1"] = (
    df_pair_history["amount1"].where(df_pair_history["operation"] == "mint", 0).cumsum() -
    df_pair_history["amount1"].where(df_pair_history["operation"] == "burn", 0).cumsum() +
    df_pair_history["amount1In"].cumsum() -
    df_pair_history["amount1Out"].cumsum())

is_token_0 = pair_data[0]["token"] == pair_data[0]["token0"]
if token_data[0]["decimals"] <= 18:
    multiplier = 10 ** (18 - token_data[0]["decimals"])
    multiplier1 = 1
else:
    multiplier = 1
    multiplier1 = 10 ** (token_data[0]["decimals"] - 18)

zero_block_number = df_pair_history["block_number"].min()
(liquidity0, liquidity1) = tuple(df_pair_history.loc[df_pair_history["block_number"] == zero_block_number][["amount0","amount1"]].sum())

if is_token_0:
    test_amount_eth18 = int(liquidity1 * TEST_RATIO)
    df_pair_history["price_simple"] = (
        np.double(df_pair_history["reserve0"] * multiplier) /
        np.double(df_pair_history["reserve1"]) / multiplier1)
    df_pair_history.loc[df_pair_history["operation"]=="swap V2", "price_trade"] = (
        np.double((df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount0In"] +
        df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount0Out"]) * multiplier) /
        np.double(df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount1In"] +
        df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount1Out"]) / multiplier1)
    df_pair_history["price_amount_buy"] = amount_out_v2(
        test_amount_eth18,
        np.double(df_pair_history["reserve1"]) * multiplier1,
        np.double(df_pair_history["reserve0"]) * multiplier) / test_amount_eth18
    test_amount_token = test_amount_eth18 * np.double(df_pair_history["reserve0"]) * multiplier / np.double(df_pair_history["reserve1"] / multiplier1)
    df_pair_history["price_amount_sell"] = test_amount_token / amount_out_v2(
        test_amount_token,
        np.double(df_pair_history["reserve0"]) * multiplier,
        np.double(df_pair_history["reserve1"]) * multiplier1)
else:
    test_amount_eth18 = int(liquidity0 * TEST_RATIO)
    df_pair_history["price_simple"] = (
        np.double(df_pair_history["reserve1"] * multiplier) / 
        np.double(df_pair_history["reserve0"]) / multiplier1)
    df_pair_history.loc[df_pair_history["operation"]=="swap V2", "price_trade"] = (
        np.double((df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount1In"] +
        df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount1Out"]) * multiplier) / 
        np.double(df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount0In"] +
        df_pair_history.loc[df_pair_history["operation"]=="swap V2","amount0Out"]) /  multiplier1)
    df_pair_history["price_amount_buy"] = amount_out_v2(
        test_amount_eth18,
        np.double(df_pair_history["reserve0"]) * multiplier1,
        np.double(df_pair_history["reserve1"]) * multiplier) / test_amount_eth18
    test_amount_token = test_amount_eth18 * np.double(df_pair_history["reserve1"]) * multiplier / np.double(df_pair_history["reserve0"] / multiplier1)
    df_pair_history["price_amount_sell"] = test_amount_token / amount_out_v2(
        test_amount_token,
        np.double(df_pair_history["reserve1"]) * multiplier,
        np.double(df_pair_history["reserve0"]) * multiplier1)

first_trade_id = df_pair_history.loc[(df_pair_history["block_number"] == zero_block_number) &
                                                    (df_pair_history["operation"] == "swap V2")].index.min()

TAKE_PROFITS = [0.05, 0.1, 0.25, 0.5, 1]
MAX_POSITION_TERM = 100

first_block_number_after_1 = df_pair_history.loc[df_pair_history["block_number"] > zero_block_number, "block_number"].min()
last1 = df_pair_history.loc[df_pair_history["block_number"] == first_block_number_after_1].index.max()
(price_trade1, price_amount_buy1, price_amount_sell1) = tuple(df_pair_history.loc[last1, ["price_trade", "price_amount_buy", "price_amount_sell"]])

traders_count0 = df_pair_history.loc[(df_pair_history["block_number"] == zero_block_number) &
                                            (df_pair_history["operation"] == "swap V2")]["sender"].nunique()

liquidity_id = df_pair_history.loc[(df_pair_history["block_number"] == zero_block_number) &
                                                    (df_pair_history["operation"] == "mint")].index.min()
first_lp = df_pair_history.loc[liquidity_id, "sender"]
other_traders_count0 = df_pair_history.loc[(df_pair_history["block_number"] == zero_block_number) &
                                            (df_pair_history["operation"] == "swap V2") &
                                            (df_pair_history["sender"] != first_lp)]["sender"].nunique()


take_profit_results = {}
take_profit_blocks = {}
for take_profit in TAKE_PROFITS:
    take_profit_block = df_pair_history.loc[(df_pair_history["price_amount_sell"] < price_amount_buy1  / (1 + take_profit)) &
                                            (df_pair_history["block_number"] <= zero_block_number + MAX_POSITION_TERM) &
                                            (df_pair_history["operation"]=="swap V2"),
                                            "block_number"].min()
    
    if pd.isnull(take_profit_block):
        take_profit_block = df_pair_history.loc[(df_pair_history["block_number"] > zero_block_number + MAX_POSITION_TERM) &
                                                (df_pair_history["operation"]=="swap V2"), "block_number"].min()
        
    if pd.isnull(take_profit_block):
        take_profit_result = - test_amount_eth18
    else:
        take_profit_point = df_pair_history.loc[df_pair_history["block_number"] == take_profit_block].index.max()
        take_profit_price = df_pair_history.loc[take_profit_point, "price_amount_sell"]
        take_profit_results["Take_profit" + str(take_profit)] = test_amount_eth18 * (price_amount_buy1 / take_profit_price - 1)
        take_profit_blocks["TP_block" + str(take_profit)] = take_profit_block - zero_block_number
