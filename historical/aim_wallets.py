#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from datetime import datetime
import pandas as pd
import numpy as np
from _utils.etherscan import get_contract_sync, get_token_transactions, get_token_transfers
from web3 import Web3
from _utils.uniswap import V2_FACTORY

CSV_FILE = "/media/Data/csv/aim_wallets"
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
KEY_FILE = '../keys/alchemy.sec'

SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
BURN_TOPIC = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496"
MINT_TOPIC = "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f"


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
    return w3_0, latest_block

def get_new_pair_tokens(i, run_context):
    pair_address = run_context['contract_storage'][V2_FACTORY["UNISWAP_V2_FACTORY"]].functions.allPairs(i).call()
    get_contract_sync(pair_address, w3=run_context["w3"], context=run_context, abi_type="pair")
    token0 = run_context['contract_storage'][pair_address].functions.token0().call()
    token1 = run_context['contract_storage'][pair_address].functions.token1().call()
    # get_contract_sync(token0, w3=run_context["w3"], context=run_context, abi_type="token")
    # get_contract_sync(token1, w3=run_context["w3"], context=run_context, abi_type="token")
    # token0_symbol = run_context['contract_storage'][token0].functions.symbol().call()
    # token0_name = run_context['contract_storage'][token0].functions.name().call()
    # token1_symbol = run_context['contract_storage'][token1].functions.symbol().call()
    # token1_name = run_context['contract_storage'][token1].functions.name().call()
    
    # return pair_address, token0, token0_symbol, token0_name, token1, token1_symbol, token1_name
    return pair_address, token0, token1

def get_wallet_row(df, token):
    row = df.loc[df["contractAddress"] == token]
    if len(row) == 0:
        return None, None, None, None, None, None
    else:
        return row.index[0], int(row["blockNumber"].values[0]), int(row["value"].values[0]), row["tokenName"].values[0], row["tokenSymbol"].values[0], int(row["tokenDecimal"].values[0])

wallets = ["0xE03a775f364612688C1C897eFCf84812f9b14e5C",
         "0x9e678213687F03B73D931aAe4019409C6A052050",
         "0x0863433f1cfe32e73630C8D626D9CB04fc733f79",
         "0x2d307C7154Bef1a2e6Ae5CE68d2F839c60Ea0B50"]

N_blocks = 100000
w3, latest_block = connect()
abi_storage = {}
contract_storage = {}
run_context = { "w3": w3,
                "delay": 0,
                "etherscan_key": ETHERSCAN_KEY,
                "abi_storage": abi_storage,
                "contract_storage": contract_storage,
                }
get_contract_sync(V2_FACTORY["UNISWAP_V2_FACTORY"], w3=w3, context=run_context)

latest_block_number = latest_block["number"]
start_block_number = latest_block_number - N_blocks
start_block = w3.eth.get_block(start_block_number)
start_block_hash = start_block["hash"]

number_pairs_start = run_context['contract_storage'][V2_FACTORY["UNISWAP_V2_FACTORY"]].functions.allPairsLength().call(block_identifier=start_block_hash)
number_pairs_end = run_context['contract_storage'][V2_FACTORY["UNISWAP_V2_FACTORY"]].functions.allPairsLength().call()

data_dict = {}
for w in wallets:
    transactions = get_token_transfers(w, ETHERSCAN_KEY)
    df = pd.DataFrame.from_records(transactions)
    data_dict[w] = df
    df.to_csv(CSV_FILE + w + ".csv")

pair_dict = {}
for i in range(number_pairs_start-1, number_pairs_end):
    pair_address, token0, token1 = get_new_pair_tokens(i, run_context)
    token0 = token0.lower()
    token1 = token1.lower()
    
    found = False
    for iii, w in enumerate(wallets):
        idx, block_number, value, token_name, token_symbol, decimals = get_wallet_row(data_dict[w], token0)
        if idx: 
            pair_dict[pair_address] = {"in": 1,
                                       "token0": token0,
                                       "token1": token1,
                                       "token": token0,
                                       "block_number": block_number,
                                       "value": value,
                                       "token_name": token_name,
                                       "token_symbol": token_symbol,
                                       "decimals": decimals,
                                       "wallet": iii}
            found = True
            break
    if found:
        continue
    for iii, w in enumerate(wallets):
        idx, block_number, value, token_name, token_symbol, decimals = get_wallet_row(data_dict[w], token1)
        if idx: 
            pair_dict[pair_address] = {"in": 1,
                                       "token0": token0,
                                       "token1": token1,
                                       "token": token1,
                                       "block_number": block_number,
                                       "value": value,
                                       "token_name": token_name,
                                       "token_symbol": token_symbol,
                                       "decimals": decimals,
                                       "wallet": iii}
            break
    if not found:
        pair_dict[pair_address] = {"in": 0, "token0": token0, "token1": token1}

df_pair_dict = pd.DataFrame.from_dict(pair_dict).T
df_pair_dict.to_csv(CSV_FILE + "_pairs" + ".csv")



for iii, p in enumerate(pair_dict):
    transactions = get_token_transactions(p, ETHERSCAN_KEY)
    if len(transactions) == 0:
        continue
    if not pair_dict[p]["in"]:
        p_block = int(transactions[0]["blockNumber"], 0) + 1
    else:
        p_block = pair_dict[p]["block_number"]
    print(iii)

    data_list = []
    found_swaps = False
    for trnx in transactions:
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
    if not found_swaps:
        continue
    df = pd.DataFrame.from_records(data_list)

    if not "token" in pair_dict[p] or pair_dict[p]["token"] == pair_dict[p]["token0"]:
        # df["price"] = np.where(np.isnan(df['amount1In']) | df['amount1In']==0, df['amount0In'] / df['amount1Out'], df['amount0Out'] / df['amount1In'])
        df["price_w"] = np.where(df['amount1In']==0, df['amount0In'], df['amount0Out'])
        df["price_q"] = np.where(df['amount1In']==0, df['amount1Out'], df['amount1In'])
    else:
        # df["price"] = np.where(np.isnan(df['amount0Out']) | df['amount0Out']==0, df['amount1Out'] / df['amount0In'], df['amount1In'] / df['amount0Out'])
        df["price_w"] = np.where(df['amount0Out']==0, df['amount1Out'], df['amount1In'])
        df["price_q"] = np.where(df['amount0Out']==0, df['amount0In'], df['amount0Out'])

    dff = df.loc[df["block_number"] < p_block]

    height = p_block - df["block_number"].min()
    if len(dff) > 0:
        mint_count = dff.loc[dff["operation"] == "mint", "amount0"].count()
        mint_amount0 = dff.loc[dff["operation"] == "mint", "amount0"].sum()
        mint_amount1 = dff.loc[dff["operation"] == "mint", "amount1"].sum()
        burn_count = dff.loc[dff["operation"] == "burn", "amount0"].count()
        burn_amount0 = dff.loc[dff["operation"] == "burn", "amount0"].sum()
        burn_amount1 = dff.loc[dff["operation"] == "burn", "amount1"].sum()
        if not "token" in pair_dict[p] or pair_dict[p]["token"] == pair_dict[p]["token0"]:
            trades_buy_count = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0Out"] > 0), "amount0Out"].count()
            trades_buy_amount = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0Out"] > 0), "amount0Out"].sum()
            trades_buy_amount_2 = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0Out"] > 0), "amount1In"].sum()
        else:
            trades_buy_count = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1Out"] > 0), "amount1Out"].count()
            trades_buy_amount = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1Out"] > 0), "amount1Out"].sum()
            trades_buy_amount_2 = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1Out"] > 0), "amount0In"].sum()
        
        if not "token" in pair_dict[p] or pair_dict[p]["token"] == pair_dict[p]["token0"]:
            trades_sell_count = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0In"] > 0), "amount0In"].count()
            trades_sell_amount = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0In"] > 0), "amount0In"].sum()
            trades_sell_amount_2 = dff.loc[(dff["operation"] == "swap V2")&(dff["amount0In"] > 0), "amount1Out"].sum()
        else:
            trades_sell_count = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1In"] > 0), "amount1In"].count()
            trades_sell_amount = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1In"] > 0), "amount1In"].sum()
            trades_sell_amount_2 = dff.loc[(dff["operation"] == "swap V2")&(dff["amount1In"] > 0), "amount0Out"].sum()
    
    dfff = df.loc[df["operation"] == "swap V2"].groupby(["block_number"]).agg({"price_w": sum, "price_q":sum})
    dfff["price"] = dfff["price_w"] / (dfff["price_q"].replace(0, np.nan))
    dff_prev = dfff.loc[dfff.index < p_block]
    
    if len(dff_prev) > 1:
        try:
            _trend = np.polyfit(np.arange(len(dff_prev)), dff_prev["price"].values, 1)
            trend = _trend[0]
        except:
            trend = np.nan
    else:
        trend = np.nan

    dff_after = dfff.loc[dfff.index > p_block]
    price_after_avg = dff_after["price"].mean()
    price_after_max = dff_after["price"].max()

    pair_dict[p]["height"] = height
    pair_dict[p]["mint_count"] = mint_count
    pair_dict[p]["mint_amount0"] = mint_amount0
    pair_dict[p]["mint_amount1"] = mint_amount1
    pair_dict[p]["burn_count"] = burn_count
    pair_dict[p]["burn_amount0"] = burn_amount0
    pair_dict[p]["burn_amount1"] = burn_amount1
    pair_dict[p]["trades_buy_count"] = trades_buy_count
    pair_dict[p]["trades_buy_amount"] = trades_buy_amount
    pair_dict[p]["trades_buy_amount_2"] = trades_buy_amount_2
    pair_dict[p]["trades_sell_count"] = trades_sell_count
    pair_dict[p]["trades_sell_amount"] = trades_sell_amount
    pair_dict[p]["trades_sell_amount_2"] = trades_sell_amount_2
    pair_dict[p]["trend"] = trend
    pair_dict[p]["price_after_avg"] = price_after_avg
    pair_dict[p]["price_after_max"] = price_after_max
            
df_pair_dict = pd.DataFrame.from_dict(pair_dict).T
df_pair_dict.to_csv(CSV_FILE + "_pairs" + ".csv")
