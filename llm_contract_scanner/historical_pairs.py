#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from datetime import datetime
import time
import pytz
import pandas as pd
import numpy as np
import json
import requests
from _utils.etherscan import get_contract_sync, get_token_transactions
from _utils.uniswap import V2_FACTORY, WETH, amount_out_v2
from db.bot_db import DBMySQL
from db.remote import open_remote_port, close_remote_port, RemoteServer
from _utils.web3connect import web3connect2

PARAMETERS_FILE = "~/git/scanner_research/parameters.json"
REQUEST_CACHE = '/media/Data/eth/eth'
# REQUEST_CACHE = '/tmp'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
KEY_FILE = '../keys/alchemy.sec'
KEY_FILE1 = '../keys/alchemy1.sec'
HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_CONTRACT_CODE_REQUEST = "https://api.etherscan.io/api?module=contract&action=getsourcecode&address={}&apikey={}"

SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
BURN_TOPIC = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496"
MINT_TOPIC = "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f"

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)
with open(os.path.expanduser(parameters["OPENAI_KEY_FILE"]), 'r') as f:
    openai_key = f.read().strip()

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    ETHERSCAN_KEY = k1.strip('\n')

def remote_server_from_parameters():
    if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
        return parameters["DB_SERVER"]
    else:
        return None

def get_new_pair_tokens(i, run_context):
    pair_address = run_context['contract_storage'][V2_FACTORY["UNISWAP_V2_FACTORY"]].functions.allPairs(i).call()
    get_contract_sync(pair_address, w3=run_context["w3"], context=run_context, abi_type="pair")
    token0 = run_context['contract_storage'][pair_address].functions.token0().call()
    token1 = run_context['contract_storage'][pair_address].functions.token1().call()
    return pair_address, token0, token1

def get_token_data(token0, token1, run_context):
    if token0 == WETH:
        token = token1
    elif token1 == WETH:
        token = token0
    else:
        token = None

    if token is None:
        token_data = {"token0": token0,
                      "token1": token1,
                      "token": token,
                      "full_data": False}
        properties = None
    else:

# !!! get contract abi before !!!
        addr_contract, _ = get_contract_sync(token, w3=run_context["w3"], session=run_context["cached_session"], context=run_context, abi_type="token")
        if addr_contract is None:
            addr_contract, _ = get_contract_sync(token, w3=run_context["w3_direct"], context=run_context, abi_type="token")
        if addr_contract is None:
            token_data = {"token0": token0,
                          "token1": token1,
                          "full_data": False}
            properties = None
        else:
            try:
                token_name = addr_contract.functions.name().call()
            except:
                token_name = ""
            try:
                token_symbol = addr_contract.functions.symbol().call()
            except:
                token_symbol = ""
            try:
                token_decimals = int(addr_contract.functions.decimals().call())
                token_decimals = min(token_decimals, 100)
            except:
                token_decimals = 18
            try:
                _maxWalletSize = addr_contract.functions._maxWalletSize().call()
                properties = {"_maxWalletSize": int(_maxWalletSize)}
            except:
                properties = None
            if properties is None: 
                try:
                    _maxWalletSize = addr_contract.functions._maxWalletToken().call()
                    properties = {"_maxWalletSize": int(_maxWalletSize)}
                except:
                    properties = None
            if properties is None: 
                try:
                    _maxWalletSize = addr_contract.functions.maxWalletLimit().call()
                    properties = {"_maxWalletSize": int(_maxWalletSize)}
                except:
                    properties = None
    
            token_data = {
                "token0": token0,
                "token1": token1,
                "token": token,
                "token_name": token_name,
                "token_symbol": token_symbol,
                "decimals": token_decimals,
                "full_data": True,
                }

    return token, token_data, properties

def pair_operations(pair_address, run_context, start_block=None):
    for i in range(5):
        try:
            transactions = get_token_transactions(pair_address,
                                                  etherscan_key=run_context["etherscan_key"],
                                                  session=run_context["cached_session"], start_block=start_block)
            break
        except:
            time.sleep(0.2)
    else:
        raise Exception("Etherscan error")
    data_list = []
    found_swaps = False
    first_block_number = None
    if transactions is None:
        return data_list, found_swaps, first_block_number
    for trnx in transactions:
        if not first_block_number:
            first_block_number = int(trnx["blockNumber"], 0)
        if trnx["topics"][0] == SWAP_TOPIC:
            found_swaps = True
            data_list.append({"transactionHash": trnx["transactionHash"],
                              "block_number": int(trnx["blockNumber"], 0),
                              "timeStamp": datetime.utcfromtimestamp(int(trnx["timeStamp"], 0)),
                              "operation": "swap V2",
                              "sender": trnx["topics"][1][:2]+trnx["topics"][1][-40:],
                              "amount0In": int(trnx["data"][:66], 0),
                              "amount1In": int("0x" + trnx["data"][66:130], 0),
                              "amount0Out": int("0x" + trnx["data"][130:194], 0),
                              "amount1Out": int("0x" + trnx["data"][195:258], 0),
                             })
        elif trnx["topics"][0] == MINT_TOPIC:
            data_list.append({"transactionHash": trnx["transactionHash"],
                              "block_number": int(trnx["blockNumber"], 0),
                              "timeStamp": datetime.utcfromtimestamp(int(trnx["timeStamp"], 0)),
                              "operation": "mint",
                              "sender": trnx["topics"][1][:2]+trnx["topics"][1][-40:],
                              "amount0": int(trnx["data"][:66], 0),
                              "amount1": int("0x" + trnx["data"][66:130], 0),
                             })
        elif trnx["topics"][0] == BURN_TOPIC:
            data_list.append({"transactionHash": trnx["transactionHash"],
                              "block_number": int(trnx["blockNumber"], 0),
                              "timeStamp": datetime.utcfromtimestamp(int(trnx["timeStamp"], 0)),
                              "operation": "burn",
                              "sender": trnx["topics"][1][:2]+trnx["topics"][1][-40:],
                              "amount0": int(trnx["data"][:66], 0),
                              "amount1": int("0x" + trnx["data"][66:130], 0),
                             })
    return data_list, found_swaps, first_block_number

def analyze_events(data_list, token0_is_the_token):
    analytics = {}
    df = pd.DataFrame.from_records(data_list)
    if len(df) == 0:
        return analytics

    if token0_is_the_token:
        df["price_w"] = np.where(df['amount1In']==0, df['amount0In'], df['amount0Out'])
        df["price_q"] = np.where(df['amount1In']==0, df['amount1Out'], df['amount1In'])
    else:
        df["price_w"] = np.where(df['amount0Out']==0, df['amount1Out'], df['amount1In'])
        df["price_q"] = np.where(df['amount0Out']==0, df['amount0In'], df['amount0Out'])

    dff = df

    if len(dff) == 0:
        return analytics

    mint_count = dff.loc[dff["operation"] == "mint", "amount0"].count()
    mint_amount0 = dff.loc[dff["operation"] == "mint", "amount0"].sum()
    mint_amount1 = dff.loc[dff["operation"] == "mint", "amount1"].sum()
    burn_count = dff.loc[dff["operation"] == "burn", "amount0"].count()
    burn_amount0 = dff.loc[dff["operation"] == "burn", "amount0"].sum()
    burn_amount1 = dff.loc[dff["operation"] == "burn", "amount1"].sum()
    if token0_is_the_token:
        trades_buy_count = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0Out"] > 0), "amount0Out"].count()
        trades_buy_amount = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0Out"] > 0), "amount0Out"].sum()
        trades_buy_amount_2 = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0Out"] > 0), "amount1In"].sum()
    else:
        trades_buy_count = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1Out"] > 0), "amount1Out"].count()
        trades_buy_amount = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1Out"] > 0), "amount1Out"].sum()
        trades_buy_amount_2 = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1Out"] > 0), "amount0In"].sum()
    
    if token0_is_the_token:
        trades_sell_count = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0In"] > 0), "amount0In"].count()
        trades_sell_amount = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0In"] > 0), "amount0In"].sum()
        trades_sell_amount_2 = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0In"] > 0), "amount1Out"].sum()
    else:
        trades_sell_count = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1In"] > 0), "amount1In"].count()
        trades_sell_amount = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1In"] > 0), "amount1In"].sum()
        trades_sell_amount_2 = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1In"] > 0), "amount0Out"].sum()
    
    dfff = df.loc[df["operation"] == "swap V2"].groupby(["block_number"]).agg({"price_w": sum, "price_q":sum})
    dfff["price"] = dfff["price_w"] / (dfff["price_q"].replace(0, np.nan))
    # dff_prev = dfff.loc[dfff.index < p_block]
    dff_prev = dfff
    
    if len(dff_prev) > 1:
        try:
            _trend = np.polyfit(np.arange(len(dff_prev)), dff_prev["price"].values, 1)
            trend = _trend[0]
            if np.isnan(trend):
                trend = None
        except:
            trend = None
    else:
        trend = None

    # dff_after = dfff.loc[dfff.index > p_block]
    dff_after = dfff
    price_after_avg = dff_after["price"].mean()
    price_after_max = dff_after["price"].max()

    analytics["mint_count"] = mint_count
    analytics["mint_amount0"] = mint_amount0
    analytics["mint_amount1"] = mint_amount1
    analytics["burn_count"] = burn_count
    analytics["burn_amount0"] = burn_amount0
    analytics["burn_amount1"] = burn_amount1
    analytics["trades_buy_count"] = trades_buy_count
    analytics["trades_buy_amount"] = trades_buy_amount
    analytics["trades_buy_amount_2"] = trades_buy_amount_2
    analytics["trades_sell_count"] = trades_sell_count
    analytics["trades_sell_amount"] = trades_sell_amount
    analytics["trades_sell_amount_2"] = trades_sell_amount_2
    analytics["trend"] = trend
    analytics["price_after_avg"] = price_after_avg
    analytics["price_after_max"] = price_after_max
    
    return analytics

def create_tables():

    with RemoteServer(remote=remote_server_from_parameters()) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            # db.create_tables(["t_pairs2", "t_event_history2", "t_contract_code2"])
            db.create_tables(["t_contract_code2"])

def update_events():  
    remote = remote_server_from_parameters()

    w3_direct, cached_session, w3, latest_block, _ = web3connect2(KEY_FILE, KEY_FILE1)
    abi_storage = {}
    contract_storage = {}
    run_context = {"w3_direct": w3_direct, 
                    "cached_session": cached_session, 
                    "w3": w3,
                    "delay": 0,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "contract_storage": contract_storage,
                    }

    
    with RemoteServer(remote=remote) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            max_blocks = db.get_max_block_times()
    print("max_blocks", len(max_blocks))

    with RemoteServer(remote=remote) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            pairs = db.get_pairs_with_contracts()
    print("pairs", len(pairs))

    max_blocks_dict = {}
    for p in max_blocks:
        max_blocks_dict[p["pair_id"]] = (p["max_block_timeStamp"].replace(tzinfo=pytz.UTC).timestamp(), p["max_block_number"])

    update_pairs = {}
    for p in pairs:
        contract, _ = get_contract_sync(p["pair"], session=cached_session, w3=w3, context=run_context, abi_type="pair")
        _, _, timestamp = contract.functions.getReserves().call()
        if not p["pair_id"] in max_blocks_dict:
            update_pairs[p["pair_id"]] = {}
            (update_pairs[p["pair_id"]]["data_dict"],
             update_pairs[p["pair_id"]]["swaps"], 
             _) = pair_operations(p["pair"], run_context)
        elif timestamp > max_blocks_dict[p["pair_id"]][0]:
            update_pairs[p["pair_id"]] = {}
            (update_pairs[p["pair_id"]]["data_dict"],
             update_pairs[p["pair_id"]]["swaps"], 
             _) = pair_operations(p["pair"], run_context, max_blocks_dict[p["pair_id"]][1]+1)
            
####!!!!!!!!!!!!!! write it


 
def collect_data1(N_blocks, block_shift = 100):
    remote = remote_server_from_parameters()

    last_block = None
    # N_blocks = 1000000
    # block_shift = 1000

    w3_direct, cached_session, w3, latest_block, _ = web3connect2(KEY_FILE, KEY_FILE1)
    abi_storage = {}
    contract_storage = {}
    run_context = {"w3_direct": w3_direct, 
                    "cached_session": cached_session, 
                    "w3": w3,
                    "delay": 0,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "contract_storage": contract_storage,
                    }
    
    get_contract_sync(V2_FACTORY["UNISWAP_V2_FACTORY"], session=cached_session, w3=w3, context=run_context)

    if not last_block:
        latest_block_number = latest_block["number"] - block_shift
    else:
        latest_block_number = last_block
    start_block_number = latest_block_number - N_blocks
    start_block = w3.eth.get_block(start_block_number)
    start_block_hash = start_block["hash"]

    end_block = w3.eth.get_block(latest_block_number)
    end_block_hash = end_block["hash"]

    number_pairs_start = run_context['contract_storage'][V2_FACTORY["UNISWAP_V2_FACTORY"]].functions.allPairsLength().call(block_identifier=start_block_hash)
    number_pairs_end = run_context['contract_storage'][V2_FACTORY["UNISWAP_V2_FACTORY"]].functions.allPairsLength().call(block_identifier=end_block_hash)
    
    _collect_data(remote, run_context, number_pairs_start, number_pairs_end)

def collect_data2(number_pairs_start, number_pairs_end):
    remote = remote_server_from_parameters()

    w3_direct, cached_session, w3, latest_block, _ = web3connect2(KEY_FILE, KEY_FILE1)
    abi_storage = {}
    contract_storage = {}
    run_context = {"w3_direct": w3_direct, 
                    "cached_session": cached_session, 
                    "w3": w3,
                    "delay": 0,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "contract_storage": contract_storage,
                    }
    
    _, _ = get_contract_sync(V2_FACTORY["UNISWAP_V2_FACTORY"], session=cached_session, w3=w3, context=run_context)
   
    _collect_data(remote, run_context, number_pairs_start, number_pairs_end)

def _collect_data(remote, run_context, number_pairs_start, number_pairs_end):
    pair_dict = {}
    df_pairs = pd.DataFrame.from_records(simply_get_pairs(), index=["pair_id"])
    print(number_pairs_start-1, number_pairs_end)
    next_pair_id = number_pairs_start-1
    with RemoteServer(remote=remote) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            pair_exists = db.check_lock_pair(next_pair_id)
    for pair_id in range(number_pairs_start-1, number_pairs_end):
        if pair_id in df_pairs.index:
            continue
        print(pair_id)
        if next_pair_id != pair_id:
            with RemoteServer(remote=remote) as server:
                with DBMySQL(port=server.local_bind_port) as db:
                    pair_exists = db.check_lock_pair(pair_id)
        if pair_exists:
            continue

        pair_address, token0, token1 = get_new_pair_tokens(pair_id, run_context)
        pair_address = pair_address.lower()
        token0 = token0.lower()
        token1 = token1.lower()
        
        token, pair_dict[pair_address], properties = get_token_data(token0, token1, run_context)
        
        if pair_dict[pair_address]["full_data"]:
            (pair_dict[pair_address]["data_dict"],
             pair_dict[pair_address]["swaps"], 
             pair_dict[pair_address]["first_block_number"]) = pair_operations(pair_address, run_context)
            
            if pair_dict[pair_address]["swaps"]:
                try:
                    pair_dict[pair_address]["price_analytics"] = analyze_events(pair_dict[pair_address]["data_dict"], token == token0)
                except:
                    pair_dict[pair_address]["price_analytics"] = {}
            else:
                pair_dict[pair_address]["price_analytics"] = {}
            # pair_dict[pair_address]["chat_gpt"] = {}
            pair_dict[pair_address]["competitors"] = {}

        with RemoteServer(remote=remote) as server:
            with DBMySQL(port=server.local_bind_port) as db:
                if not token is None:
                    db.add_event_history(pair_id, pair_dict[pair_address]["data_dict"])
                    db.update_json("t_pairs2", pair_id, "price_analytics", pair_dict[pair_address]["price_analytics"], condition="pair_id")
                    # db.update_json("t_pairs2", pair_id, "chat_gpt", pair_dict[pair_address]["chat_gpt"])
                    db.update_json("t_pairs2", pair_id, "competitors", pair_dict[pair_address]["competitors"], condition="pair_id")
                db.add_pair(pair_id, pair_address, pair_dict[pair_address])
                if not properties is None:
                    db.update_json("t_tokens2", token, "properties", properties, "token")

                next_pair_id = pair_id + 1
                pair_exists = db.check_lock_pair(next_pair_id)


def recalc_data():
    with RemoteServer(remote=remote_server_from_parameters()) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            pairs = db.get_pairs()
    
            for p in pairs:
                event_history = db.get_event_history(p["pair_id"])
                print(p["pair_id"], p["pair"])
    
                price_analytics = analyze_events(event_history, p["token"] == p["token0"])
                db.update_json("t_pairs2", p["pair_id"], "price_analytics", price_analytics)


def collect_contract_texts(step, start_pair=None, end_pair=320000):
    w3_direct, cached_session, w3, latest_block, _ = web3connect2(KEY_FILE, KEY_FILE1)
    abi_storage = {}
    contract_storage = {}
    run_context = {"w3_direct": w3_direct, 
                    "cached_session": cached_session, 
                    "w3": w3,
                    "delay": 0,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "contract_storage": contract_storage,
                    "start_pair": start_pair,
                    "end_pair": end_pair,
                    }
    while True:
        try:
            _collect_contract_texts(step, run_context)
            break
        except Exception as e:
            print(e)

def _collect_contract_texts(step, run_context):
    start_pair = run_context["start_pair"]
    end_pair = run_context["end_pair"]
    remote = remote_server_from_parameters()

    with RemoteServer(remote=remote) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            # pairs = db.get_pairs()
            pairs = db.get_pairs_no_text()

    print(len(pairs))
    ii = 0
    with RemoteServer(remote=remote) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            for p in pairs[::step]:
                if (not start_pair is None and p["pair_id"] > start_pair) or (not end_pair is None and p["pair_id"] < end_pair):
                    continue
                print(ii, p["pair_id"])
                if p["token"] is None:
                    continue
                check_contract_code = db.check_contract_code(p["token"])
                if check_contract_code[0] and check_contract_code[1]:
                    continue
                res = requests.post(ETHERSCAN_CONTRACT_CODE_REQUEST.format(p["token"], ETHERSCAN_KEY), headers=HEADERS)
                d = res.json()
                source_code = d["result"][0]["SourceCode"]
                abi = d["result"][0]["ABI"]
                if not source_code: # or not "_maxWalletSize" in source_code:
                    # print(d["result"])
                    continue
                # contract, _ = get_contract_sync(p["token"], w3=run_context["w3"], session=run_context["cached_session"], context=run_context)
                # if not "_maxWalletSize" in contract.functions:
                #     continue
                # _maxWalletSize = contract.functions._maxWalletSize().call()
                # db.update_json("t_tokens2", p["token"], "properties", {"_maxWalletSize": _maxWalletSize}, "token")
                if check_contract_code[0] == 0:
                    db.add_contract_code(p["token"], source_code, abi)
                elif check_contract_code[1] == 0 and abi:
                    db.add_contract_abi(p["token"], abi)
                ii += 1
                run_context["start_pair"] = p["pair_id"]


def simply_get_pairs():
    with RemoteServer(remote=remote_server_from_parameters()) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_pairs()
 
def get_pairs_with_contracts():
    with RemoteServer(remote=remote_server_from_parameters()) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_pairs_with_contracts()

def get_one_pair_history(pair_id):
    with RemoteServer(remote=remote_server_from_parameters()) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_event_history(pair_id)
 
def get_pair_range_history(pair_id_start, pair_id_end):
    with RemoteServer(remote=remote_server_from_parameters()) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_event_history_many(pair_id_start, pair_id_end)

def clean_for_reload(pair_id_start, pair_id_end):
    with RemoteServer(remote=remote_server_from_parameters()) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            print(db.clean_for_reload(pair_id_start, pair_id_end))


def get_price_analytics(pair_id):
    with RemoteServer(remote=remote_server_from_parameters()) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_json(pair_id, "price_analytics")
 
def calculate_gaps():
    pairs = simply_get_pairs()
    df_pairs = pd.DataFrame.from_records(pairs, index=["pair_id"])

    df_pairs["p_id"] = df_pairs.index
    df_pairs["gap"] = df_pairs["p_id"] - df_pairs["p_id"].shift(1) - 1
    print(df_pairs.index.min())
    print(df_pairs.index.max())
    print( df_pairs.loc[df_pairs["gap"] > 0][["gap"]])

def main1():
    collect_data1(200000, 1000)

    clean_for_reload(30000, 130000)

    collect_contract_texts(-1)
    

def main2():
    pairs = simply_get_pairs()
    df_pairs = pd.DataFrame.from_records(pairs, index=["pair_id"])

    price_analytics = get_price_analytics(296845)
    price_history = get_one_pair_history(296845)
    df_price_history = pd.DataFrame.from_records(price_history, index=["event_id"])

    
    df_price_history.loc[df_price_history["operation"] == "burn", "amount0"] = -df_price_history.loc[df_price_history["operation"] == "burn", "amount0"]
    df_price_history.loc[df_price_history["operation"] == "burn", "amount1"] = -df_price_history.loc[df_price_history["operation"] == "burn", "amount1"]

    df_price_history["reserve0"] = df_price_history["amount0"].fillna(0).cumsum() + df_price_history["amount0In"].fillna(0).cumsum() - df_price_history["amount0Out"].fillna(0).cumsum()
    df_price_history["reserve1"] = df_price_history["amount1"].fillna(0).cumsum() + df_price_history["amount1In"].fillna(0).cumsum() - df_price_history["amount1Out"].fillna(0).cumsum()
    df_price_history["price_simple"] = df_price_history["reserve1"] / df_price_history["reserve0"]
    
    test_amount = 0.1 * 1e18 #ETH
    df_price_history["price_amount"] = amount_out_v2(test_amount, df_price_history["reserve0"], df_price_history["reserve1"]) / test_amount

    df_price_history["price_simple"].plot()
    (1/df_price_history["price_simple"]).plot()

    df_price_history["price_amount"].plot()





def garbage_zone():

    remote = remote_server_from_parameters()
    
    with RemoteServer(remote=remote) as server:
        with DBMySQL(port=server.local_bind_port) as db:
                pairs1 = db.get_pairs_no_text()
    df_pairs1 = pd.DataFrame.from_records(pairs1, index=["pair_id"])
 
    
    w3_direct, cached_session, w3, latest_block, _ = web3connect2(KEY_FILE)
    abi_storage = {}
    contract_storage = {}
    run_context = {"w3": w3,
                    "delay": 0,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "contract_storage": contract_storage,
                    }

    addr = "0x740D276c0F8aAeC2eae38cd4EE5E8612ec164BBE"
    contr, _ = get_contract_sync(addr, w3=w3, context=run_context)

    block = w3.eth.get_block(18897800)
    block_hash = block["hash"].hex()
    r = contr.functions.getReserves().call(block_identifier=block_hash)
    print(r)



