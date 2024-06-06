#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
import json
import pandas as pd
import numpy as np

from db.bot_db import DBMySQL
from db.remote import open_remote_port, close_remote_port, RemoteServer
from _utils.uniswap import WETH
from _utils.web3connect import web3connect2

REQUEST_CACHE = '/media/Data/eth/eth'
PARAMETERS_FILE = "~/git/scanner_research/parameters.json"
CSV_DIR = '/media/Data/csv/'
# CSV_DIR = '/home/anton/tmp/'
KEY_FILE = '../keys/alchemy.sec'
KEY_FILE1 = '../keys/alchemy1.sec'

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None

def get_pairs_with_contracts(pair_id_start=None, pair_id_end=None):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_pairs_with_contracts(pair_id_start, pair_id_end, [WETH])

def get_pair_range_history(pair_id_start, pair_id_end):
    if pair_id_end == pair_id_start:
        pair_id_end = pair_id_start + 1
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_event_history_many(pair_id_start, pair_id_end)


def test_rag_pull():

    df_burn_block_trades_before = None
    df_first_burns = None
    
    for ii in range(10):
        start_pair = 200000 + ii * 10000
        end_pair = 210000 + ii * 10000
        
        pairs = get_pairs_with_contracts(start_pair, end_pair)
        price_history = get_pair_range_history(start_pair, end_pair)
    
        df_pairs = pd.DataFrame.from_records(pairs, index=["pair_id"])
        df_price_history = pd.DataFrame.from_records(price_history)
        df_price_history.set_index(["event_id"], inplace=True, drop=False)
    
        df_first_mint = df_price_history.loc[df_price_history["operation"] == "mint"].groupby(["pair_id"])[["event_id", "block_number"]].min()
        df_first_burn = df_price_history.loc[df_price_history["operation"] == "burn"].groupby(["pair_id"])[["event_id", "block_number"]].min()
        df_first_burn["transactionHash"] = df_price_history.loc[df_first_burn["event_id"], "transactionHash"].values
        if df_first_burns is None:
            df_first_burns = df_first_burn
        else:
            df_first_burns = pd.concat([df_first_burns, df_first_burn])

        df_first_burn["pair_block"] = list(zip(df_first_burn.index, df_first_burn["block_number"]))
        df_price_history["pair_block"] = list(zip(df_price_history["pair_id"], df_price_history["block_number"]))
    
        df_burn_block_trades = df_price_history.loc[np.isin(df_price_history["pair_block"], df_first_burn["pair_block"]) & (df_price_history["operation"] == "swap V2")]
        df_burn_block_trades["our_case"] = False
        
        for i in df_burn_block_trades.index:
            pair_id = df_burn_block_trades.loc[i, "pair_id"]
            if not pair_id in df_pairs.index:
                continue
            if df_pairs.loc[pair_id, "token"] == df_pairs.loc[pair_id, "token0"]:
                df_burn_block_trades.loc[i, "our_case"] = df_burn_block_trades.loc[i, "amount0In"] != 0 and df_first_burn.loc[pair_id, "event_id"] > i
            else:
                df_burn_block_trades.loc[i, "our_case"] = df_burn_block_trades.loc[i, "amount1In"] != 0 and df_first_burn.loc[pair_id, "event_id"] > i
               
    
        df_burn_block_trades_before_i = df_burn_block_trades.loc[df_burn_block_trades["our_case"]].copy()
        df_burn_block_trades_before_i["token"] = np.nan
        df_burn_block_trades_before_i["mint_block"] = 0
        for i in df_burn_block_trades_before_i.index:
            pair_id = df_burn_block_trades.loc[i, "pair_id"]
            df_burn_block_trades_before_i.loc[i, "token"] = df_pairs.loc[pair_id, "token"]
            df_burn_block_trades_before_i.loc[i, "mint_block"] = int(df_first_mint.loc[pair_id, "block_number"])
            
    
        if df_burn_block_trades_before is None:
            df_burn_block_trades_before = df_burn_block_trades_before_i
        else:
            df_burn_block_trades_before = pd.concat([df_burn_block_trades_before, df_burn_block_trades_before_i])
        print(ii, len(df_burn_block_trades_before_i))


    blocks = {}
    w3_direct, cached_session, w3, latest_block, _ = web3connect2(KEY_FILE, KEY_FILE1)
    
    df_burn_block_trades_before["maxPriorityFeePerGas"] = np.nan
    df_burn_block_trades_before["effectiveGasPrice"] = np.nan
    df_burn_block_trades_before["baseFeePerGas"] = np.nan
    df_burn_block_trades_before["gasUsed"] = np.nan
    df_burn_block_trades_before["maxPriorityFeePerGasBurn"] = np.nan
    df_burn_block_trades_before["effectiveGasPriceBurn"] = np.nan
    df_burn_block_trades_before["index"] = np.nan
    df_burn_block_trades_before["indexBurn"] = np.nan
    # df_burn_block_trades_before["bribe"] = np.nan
    # df_burn_block_trades_before["bribeBurn"] = np.nan
    for ii, i in enumerate(df_burn_block_trades_before.index):
        print(ii, i)
        transactionHash = df_burn_block_trades_before.loc[i, "transactionHash"]
        block_number = int(df_burn_block_trades_before.loc[i, "block_number"])
        pair_id = df_burn_block_trades_before.loc[i, "pair_id"]
        if block_number in blocks:
            block = blocks[block_number]
        else:
            block = w3.eth.get_block(block_number, full_transactions=False)
        transaction = w3.eth.get_transaction(transactionHash)
        df_burn_block_trades_before.loc[i, "index"] = transaction["transactionIndex"]
        receipt = w3.eth.get_transaction_receipt(transactionHash)
        if "maxPriorityFeePerGas" in transaction:
            df_burn_block_trades_before.loc[i, "maxPriorityFeePerGas"] = transaction["maxPriorityFeePerGas"]/1e9
        df_burn_block_trades_before.loc[i, "effectiveGasPrice"] = receipt["effectiveGasPrice"]/1e9
        df_burn_block_trades_before.loc[i, "gasUsed"] = int(receipt["gasUsed"])
        df_burn_block_trades_before.loc[i, "baseFeePerGas"] = block["baseFeePerGas"]/1e9
        
        # for l in receipt["logs"]:
        #     if l["address"] == '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' and l["topics"][0].hex() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
        #         if l["topics"][2].hex()[-40:] == block.miner[-40:]:
        #             print("bribe", i, int(l["data"].hex(),0))
        #             df_burn_block_trades_before.loc[i, "bribe"] = int(l["data"].hex(),0)/1e18

        transactionHash2 = df_first_burns.loc[pair_id, "transactionHash"]
        transaction2 = w3.eth.get_transaction(transactionHash2)
        df_burn_block_trades_before.loc[i, "indexBurn"] = transaction2["transactionIndex"]
        receipt2 = w3.eth.get_transaction_receipt(transactionHash2)
        if "maxPriorityFeePerGas" in transaction2:
            df_burn_block_trades_before.loc[i, "maxPriorityFeePerGasBurn"] = transaction2["maxPriorityFeePerGas"]/1e9
        df_burn_block_trades_before.loc[i, "effectiveGasPriceBurn"] = receipt2["effectiveGasPrice"]/1e9
        # for l in receipt2["logs"]:
        #     if l["address"] == '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' and l["topics"][0].hex() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
        #         if l["topics"][2].hex()[-40:] == block.miner[-40:]:
        #             print("bribe", i, int(l["data"].hex(),0))
        #             df_burn_block_trades_before.loc[i, "bribeBurn"] = int(l["data"].hex(),0)/1e18

    df_burn_block_trades_before["age"] = df_burn_block_trades_before["block_number"] - df_burn_block_trades_before["mint_block"]
    df_burn_block_trades_before["effectivePriorityFee"] = df_burn_block_trades_before["effectiveGasPrice"] - df_burn_block_trades_before["baseFeePerGas"]
    df_burn_block_trades_before["effectivePriorityFeeBurn"] = df_burn_block_trades_before["effectiveGasPriceBurn"] - df_burn_block_trades_before["baseFeePerGas"]
    columns = ["pair_id", "token", "transactionHash", "block_number", "mint_block", "age",
               "timeStamp", "sender", "amount0In", "amount1In", "amount0Out", "amount1Out", "baseFeePerGas",
               "gasUsed", "maxPriorityFeePerGas", "effectiveGasPrice", "effectivePriorityFee",
               "maxPriorityFeePerGasBurn", "effectiveGasPriceBurn", "effectivePriorityFeeBurn", "index", "indexBurn"]

    df_burn_block_trades_before.to_csv(CSV_DIR + "burn_block_trades_before.csv", columns=columns)
