#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
script for all bundles in the DB

"""

from datetime import datetime, timedelta
import time
import json
import MySQLdb
import pandas as pd
import numpy as np

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from requests_cache import CachedSession, SQLiteCache, NEVER_EXPIRE
from web3 import Web3

from _utils.utils import HTTPProviderCached
from _utils.uniswap import WETH
from _utils.etherscan import get_contract_sync

KEY_FILE = '../keys/alchemy.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
REQUEST_CACHE = '/media/Data/eth/eth'
CSV_FILE = "/media/Data/csv/all_attack_dynamics.csv"
WETH = Web3.to_checksum_address(WETH)

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    ETHERSCAN_KEY = k1.strip('\n')

def connect():
    w3_0 = Web3(Web3.HTTPProvider(alchemy_url))
    latest_block = w3_0.eth.get_block("latest")
    backend = SQLiteCache(REQUEST_CACHE)
    session = CachedSession(backend=backend, expire_after=NEVER_EXPIRE, allowable_methods=('GET', 'POST', 'HEAD'))
    w3_1 = Web3(HTTPProviderCached(alchemy_url, request_kwargs={'timeout': 1e9}, session=session))
    return w3_0, session, w3_1, latest_block

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
db_name = "mev_bot_poc_2_1"

db_connection = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
cursor = db_connection.cursor()

# df_attacks = fetch_to_df(cursor, "attacks", ["id"])
# df_blocks = fetch_to_df(cursor, "blocks", ["id"])
# df_bundles = fetch_to_df(cursor, "bundles", ["id"])
# df_competitor_bundles = fetch_to_df(cursor, "competitor_bundles", ["id"])
# df_competitor_sandwich_swap_attack_components = fetch_to_df(cursor, "competitor_sandwich_swap_attack_components", ["id"])
# df_competitor_sandwich_swap_attack_transactions = fetch_to_df(cursor, "competitor_sandwich_swap_attack_transactions", ["id"])
df_sandwich_swap_attack_components = fetch_to_df(cursor, "sandwich_swap_attack_components", ["id"])
# df_sandwich_swap_attack_transactions = fetch_to_df(cursor, "sandwich_swap_attack_transactions", ["id"])
df_sandwich_swap_targets = fetch_to_df(cursor, "sandwich_swap_targets", ["id"])
# df_whitelisted_tokens = fetch_to_df(cursor, "whitelisted_tokens", ["id"])

db_connection.close()

first_target_time_idx = df_sandwich_swap_targets["createTime"].idxmin()
# df_first_block = df_sandwich_swap_targets.groupby(["pairAddress"])["createTime"].idxmin()
# df_first_touch = df_sandwich_swap_targets.loc[df_first_block][["txHash", "createTime", "pairAddress", "token0Address", "token1Address"]]
# df_first_touch.sort_values(["createTime"], inplace=True)

# if len(df_first_touch.loc[(df_first_touch.token0Address != WETH) 
#                           & (df_first_touch.token1Address != WETH)]) != 0:
#     print("not WETH swaps in the database",
#           df_first_touch.loc[(df_first_touch.token0Address != WETH) &
#                           (df_first_touch.token1Address != WETH)])
    

last_block = "latest"
w3_direct, cached_session, w3, latest_block = connect()
WETH = Web3.to_checksum_address(WETH)

contract_storage = {}
token_storage = {}
pair_storage = {}
abi_storage = {}
run_context = {"w3_direct": w3_direct, 
                "cached_session": cached_session, 
                "w3": w3,
                "etherscan_key": ETHERSCAN_KEY,
                "contract_storage": contract_storage,
                "abi_storage": abi_storage,
                }

tnx = w3.eth.get_transaction(df_sandwich_swap_targets.loc[first_target_time_idx]["txHash"])
start_block = tnx["blockNumber"] - 1
if last_block == "latest":
    last_block = latest_block["number"]

MAX_HISTORY_SECONDS = 12 * 201

df_sandwich_swap_attack_components["blockNumber"] = np.nan
# collected_price_data = {}
collected_shifted_price_data = {}
block_number = start_block
while block_number <= last_block:
    try:
        block_prices = {}
        print(block_number)
        block = w3.eth.get_block(block_number)
        block_hash = block["hash"].hex()
        block_time = datetime.utcfromtimestamp(block["timestamp"])
        df_current_block_sandwiches = df_sandwich_swap_attack_components.loc[(df_sandwich_swap_attack_components["createTime"] <= block_time + timedelta(seconds=12))
                                                                             & (df_sandwich_swap_attack_components["createTime"] >= block_time - timedelta(seconds=MAX_HISTORY_SECONDS))
                                                                             & (df_sandwich_swap_attack_components["position"] == "front-run")]
        for i in df_current_block_sandwiches.index:
            pairAddress = df_current_block_sandwiches.loc[i, "pairAddress"]
            if pairAddress in block_prices:
                price = block_prices[pairAddress]["price"]
            else:
                pair_contract, _ = get_contract_sync(pairAddress, context=run_context, w3=w3, session=cached_session, abi_type="pair")
                reserves = pair_contract.functions.getReserves().call(block_identifier=block_hash)
                if pairAddress in pair_storage:
                    token0 = pair_storage[pairAddress]["token0"]
                    token1 = pair_storage[pairAddress]["token1"]
                else:
                    token0 = pair_contract.functions.token0().call()
                    token1 = pair_contract.functions.token0().call()
                    pair_storage[pairAddress] = {"token0": token0, "token1": token1}
                
                if token0 != WETH:
                    if not token0 in token_storage:
                        token_contract, _ = get_contract_sync(token0, context=run_context, w3=w3, session=cached_session, abi_type="token")
                        decimals = token_contract.functions.decimals().call()
                        token_storage[token0] = {"decimals": decimals, "symbol": token_contract.functions.symbol().call(), "name": token_contract.functions.name().call()}
                    else:
                        decimals = token_storage[token0]["decimals"]
                    price = reserves[0]/reserves[1] * 10 ** (18 - decimals)
                    token = token0
                elif token1 != WETH:
                    continue
                else:
                    if not token1 in token_storage:
                        token_contract, _ = get_contract_sync(token1, context=run_context, w3=w3, session=cached_session, abi_type="token")
                        decimals = token_contract.functions.decimals().call()
                        token_storage[token1] = {"decimals": decimals, "symbol": token_contract.functions.symbol().call(), "name": token_contract.functions.name().call()}
                    else:
                        decimals = token_storage[token1]["decimals"]
                    price = reserves[1]/reserves[0] * 10 ** (18 - decimals)
                    token = token1
                block_prices[pairAddress] = {"price": price}
            if np.isnan(df_sandwich_swap_attack_components.loc[i, "blockNumber"]):
                df_sandwich_swap_attack_components.loc[i, "blockNumber"] = block_number
                first_block_for_this_sandwich = block_number
            else:
                first_block_for_this_sandwich = df_sandwich_swap_attack_components.loc[i, "blockNumber"]
            block_shift = int(block_number - first_block_for_this_sandwich)
            if not block_shift in collected_shifted_price_data:
                collected_shifted_price_data[block_shift] = {}
            collected_shifted_price_data[block_shift][(token, i)] = price
        print(len(collected_shifted_price_data[block_shift]), "pairs")
        block_number += 1
    except Exception as e:
        print(e)
        time.sleep(10)

        
df_results = pd.DataFrame.from_dict(collected_shifted_price_data)
df_results.to_csv(CSV_FILE)
