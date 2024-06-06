#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import json
import MySQLdb
import pandas as pd
import numpy as np

CSV_FILE = '/home/anton/tmp/sniping_test_'
CSV_FILE = '/media/Data/csv/sniping_test_'
TAKE_PROFITS = [0.1, 0.2, 0.3, 0.5, 1, 2, 3, 4]
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

NO_GAS = 1 # 1 means "with gas"
NO_TRADES = 1 # 1 means "with trades"
CONSTANT_GAS = 0
# CONSTANT_GAS = 160000 * 40 * 1e9

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

db_host = users["bot_db1"]["ip"]
db_user = users["bot_db1"]["user"]
db_passwd = users["bot_db1"]["password"]
# db_name = users["bot_db2"]["database"]
db_name = "sniper_pre_chatgpt"

db_connection = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
cursor = db_connection.cursor()

# sql = "show tables"
# cursor.execute(sql)
# q = cursor.fetchall()
# print(q)

df_app_states = fetch_to_df(cursor, "app_states", ["id"])
df_dry_run_buys = fetch_to_df(cursor, "dry_run_buys", ["id"])
df_dry_run_sells = fetch_to_df(cursor, "dry_run_sells", ["id"])
df_etherscan_data = fetch_to_df(cursor, "etherscan_data", ["id"])
df_factories = fetch_to_df(cursor, "factories", ["id"])
df_pairs = fetch_to_df(cursor, "pairs", ["id"])
df_tokens = fetch_to_df(cursor, "tokens", ["id"])

# df_dry_run_sells.columns
df_dry_run_buy_errors = df_dry_run_buys.groupby(["error"])["error"].count()
df_dry_run_sell_errors = df_dry_run_sells.groupby(["error"])["error"].count()
df_dry_run_sell_K_errors = df_dry_run_sells.loc[df_dry_run_sells["error"] == "UniswapV2: K,UniswapV2: K"]
evil_pairs = df_dry_run_sell_K_errors.groupby(["pairId"])["pairId"].count()
df_evil_pairs = df_pairs.loc[evil_pairs.index]



df_dry_run_sells["weth_buy_expenses"] = np.nan
df_dry_run_sells["eth_sell_expenses"] = np.nan
df_dry_run_sells["weth_sell_result"] = np.nan
df_dry_run_sells["sell_result_no_gas"] = np.nan
df_dry_run_sells["sell_result"] = np.nan

df_dry_run_buy_no_errors = df_dry_run_buys.loc[pd.isnull(df_dry_run_buys["error"])].copy()
df_dry_run_sell_no_errors = df_dry_run_sells.loc[pd.isnull(df_dry_run_sells["error"])]
df_dry_run_buy_no_errors.sort_values(["buyBlockNumber"])
df_dry_run_buy_no_errors["buy_block"] = df_dry_run_buy_no_errors.groupby(["pairId"]).cumcount()

results = {}
print(len(df_dry_run_buy_no_errors.index))
for ii, i in enumerate(df_dry_run_buy_no_errors.index):
    print(ii, i)
    pair = df_pairs.loc[df_dry_run_buy_no_errors.loc[i, "pairId"], "address"]
    buy_block = df_dry_run_buy_no_errors.loc[i, "buy_block"] 
    token0Address = df_pairs.loc[df_dry_run_buy_no_errors.loc[i, "pairId"], "token0Address"]
    token1Address = df_pairs.loc[df_dry_run_buy_no_errors.loc[i, "pairId"], "token1Address"]
    wasAbleToBuySellSimulate = df_pairs.loc[df_dry_run_buy_no_errors.loc[i, "pairId"], "wasAbleToBuySellSimulate"]
    zeroBlockTradeTokenBuyTax = df_pairs.loc[df_dry_run_buy_no_errors.loc[i, "pairId"], "zeroBlockTradeTokenBuyTax"]
    zeroBlockTradeTokenSellTax = df_pairs.loc[df_dry_run_buy_no_errors.loc[i, "pairId"], "zeroBlockTradeTokenSellTax"]
    
    if token0Address == WETH:
        token = token1Address
    else:
        token = token0Address

    df = df_dry_run_sell_no_errors.loc[df_dry_run_sell_no_errors["buyId"] == i].copy()
    if CONSTANT_GAS:
        eth_buy_expenses = -CONSTANT_GAS
    else:
        eth_buy_expenses = int(df_dry_run_buy_no_errors.loc[i]["ethBalanceAfter"]) - int(df_dry_run_buy_no_errors.loc[i]["ethBalanceBefore"])
    if eth_buy_expenses > 0:
        print("incorrect gas")
        break
    weth_buy_expenses = int(df_dry_run_buy_no_errors.loc[i]["wethBalanceAfter"]) - int(df_dry_run_buy_no_errors.loc[i]["wethBalanceBefore"])

    if len(df) == 0:
        result = {
            "token": token,
            "buy_block": buy_block,
            # "first_sell_result": (eth_buy_expenses + weth_buy_expenses)/1e18,
            # "last_sell_result": (eth_buy_expenses + weth_buy_expenses)/1e18,
            "best_sell_result": (eth_buy_expenses + weth_buy_expenses)/1e18,
            "worst_sell_result": (eth_buy_expenses + weth_buy_expenses)/1e18,
            }
        results[i] = result
        continue

    df["weth_buy_expenses"] = weth_buy_expenses
    if CONSTANT_GAS:
        df["eth_sell_expenses"] = -CONSTANT_GAS
    else:
        df["eth_sell_expenses"] = df.apply(lambda row: int(row["ethBalanceAfter"]) - int(row["ethBalanceBefore"]), axis=1)
    df["weth_sell_result"] = df.apply(lambda row: int(row["wethBalanceAfter"]) - int(row["wethBalanceBefore"]), axis=1)
    df["sell_result_no_gas"] = df["weth_sell_result"] + weth_buy_expenses
    df["sell_result"] = df["sell_result_no_gas"] * NO_TRADES +  (eth_buy_expenses + df["eth_sell_expenses"]) * NO_GAS
    df["tp_check"] = (df["sell_result_no_gas"] +  (eth_buy_expenses + df["eth_sell_expenses"])) / (-weth_buy_expenses - eth_buy_expenses)

    if df["eth_sell_expenses"].max() > 0:
        print("incorrect gas")
        break
    
    id_first = df.index.min()
    id_last = df.index.max()
    id_max = df["sell_result"].idxmax()
    id_min = df["sell_result"].idxmin()
    
    take_profit_results = {}
    take_profit_blocks = {}
    for take_profit in TAKE_PROFITS:
        take_profit_signal_block = df.loc[df["tp_check"] >= take_profit, "sellBlockNumber"].min()
        id_take_profit = df.loc[df["sellBlockNumber"] > take_profit_signal_block].index.min()
        if pd.isnull(take_profit_signal_block) or pd.isnull(id_take_profit):
            take_profit_blocks["TP_block" + str(take_profit)] = int(df.loc[id_last, "sellBlockNumber"]) - int(df.loc[id_last, "buyBlockNumber"])
            if take_profit_blocks["TP_block" + str(take_profit)] < 100:
                take_profit_blocks["TP_block" + str(take_profit)] = None
                take_profit_results["Take_profit" + str(take_profit)] = (eth_buy_expenses * NO_GAS + weth_buy_expenses * NO_TRADES)/1e18
            else:
                take_profit_results["Take_profit" + str(take_profit)] = df.loc[id_last, "sell_result"]/1e18
        else:
            take_profit_blocks["TP_block" + str(take_profit)] = int(df.loc[id_take_profit, "sellBlockNumber"]) - int(df.loc[id_take_profit, "buyBlockNumber"])
            take_profit_results["Take_profit" + str(take_profit)] = df.loc[id_take_profit, "sell_result"]/1e18


    result = {
        "token": token,
        "buy_block": buy_block,
        'wasAbleToBuySellSimulate': wasAbleToBuySellSimulate,
        'zeroBlockTradeTokenBuyTax': zeroBlockTradeTokenBuyTax,
        'zeroBlockTradeTokenSellTax': zeroBlockTradeTokenSellTax,
        # "first_sell_block": int(df.loc[id_first, "sellBlockNumber"]) - int(df.loc[id_first, "buyBlockNumber"]),
        # "first_sell_result": df.loc[id_first, "sell_result"]/1e18,
        "last_sell_block": int(df.loc[id_last, "sellBlockNumber"]) - int(df.loc[id_last, "buyBlockNumber"]),
        "last_sell_result": df.loc[id_last, "sell_result"]/1e18,
        "best_sell_block": int(df.loc[id_max, "sellBlockNumber"]) - int(df.loc[id_max, "buyBlockNumber"]),
        "best_sell_result": df.loc[id_max, "sell_result"]/1e18,
        "worst_sell_block": int(df.loc[id_min, "sellBlockNumber"]) - int(df.loc[id_min, "buyBlockNumber"]),
        "worst_sell_result": df.loc[id_min, "sell_result"]/1e18,
        }
    results[i] = result | take_profit_results | take_profit_blocks

columns = ['token', 'buy_block', 'wasAbleToBuySellSimulate', 'zeroBlockTradeTokenBuyTax', 'zeroBlockTradeTokenSellTax']
# columns += ['first_sell_result']
columns += ["Take_profit" + str(t) for t in TAKE_PROFITS]
columns += ['last_sell_result']
columns += ['best_sell_result', 'worst_sell_result']
# columns += ['first_sell_block']
columns += ["TP_block" + str(t) for t in TAKE_PROFITS]
columns += ['last_sell_block']
columns += ['best_sell_block', 'worst_sell_block']
df_results = pd.DataFrame.from_dict(results, orient="index", columns=columns)

df_results.to_csv(CSV_FILE + "dry_run_" + db_name + ("_g1" if CONSTANT_GAS else "_g0") + ("" if NO_GAS else "_ng") + ("" if NO_TRADES else "_nt") + datetime.now().strftime("%d%m") + ".csv")



tokens_list = df_results.loc[df_results['Take_profit4']>0, "token"].unique()
####################################################################### after story










import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from db.bot_db import DBMySQL
from db.remote import open_remote_port, close_remote_port, RemoteServer
from _utils.uniswap import amount_out_v2

PARAMETERS_FILE = "~/git/scanner_research/llm_contract_scanner/parameters.json"
TEST_RATIO = 0.05
with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None

with RemoteServer(remote=REMOTE) as server:
    with DBMySQL(port=server.local_bind_port) as db:
        tokens = db.get_tokens()
        pairs = db.get_pairs()
df_pairs = pd.DataFrame.from_records(pairs, index=["pair_id"])
df_pairs = df_pairs.loc[np.isin(df_pairs["token"], tokens_list)]
df_tokens = pd.DataFrame.from_records(tokens, index=["token"])

with RemoteServer(remote=REMOTE) as server:
    with DBMySQL(port=server.local_bind_port) as db:
        price_history = db.get_event_history_many(df_pairs.index.min(), df_pairs.index.max()+1)

df_price_history = pd.DataFrame.from_records(price_history, index=["event_id"])
results_after = {}
for pair_id in df_pairs.index:
    # pair_id = df_pairs.index.min()
    pair_data = df_pairs.loc[pair_id]
    token_data = df_tokens.loc[pair_data["token"]]
    df_pair_history = df_price_history.loc[df_price_history["pair_id"] == pair_id].copy()
    
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
    
    is_token_0 = pair_data["token"] == pair_data["token0"]
    if token_data["decimals"] <= 18:
        multiplier = 10 ** (18 - token_data["decimals"])
        multiplier1 = 1
    else:
        multiplier = 1
        multiplier1 = 10 ** (token_data["decimals"] - 18)
    
    zero_block_number = df_pair_history["block_number"].min()
    (liquidity0, liquidity1) = tuple(df_pair_history.loc[df_pair_history["block_number"] == zero_block_number][["amount0","amount1"]].sum())
    df_pair_history["block_shift"] = df_pair_history["block_number"] - zero_block_number
    df_pair_history["event_id"] = df_pair_history.index
    
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
    
    df = df_pair_history.loc[df_pair_history.groupby(["block_shift"])["event_id"].max()][["block_shift", "price_amount_sell"]].set_index(["block_shift"], drop=True)
    df["price_amount_sell"] = 1 / df["price_amount_sell"]

    results_after[pair_id] = df

dff = pd.concat(results_after, axis=1)
dff.sort_index(inplace=True)
dff.columns = pd.MultiIndex.from_tuples([(c[0], df_pairs.loc[c[0], "token"])for c in dff.columns])
dff.to_csv(CSV_FILE + "dry_run_after400.csv")

dft = df_results.loc[df_results['Take_profit4']>0] # ---------------
dft["pair_id"] = np.nan
for i in dft.index:
    try:
        dft.loc[i, "pair_id"] = df_pairs.loc[df_pairs["token"] == dft.loc[i, "token"]].index
    except:
        pass

dft.to_csv(CSV_FILE + "dry_run_after400_tokens.csv")