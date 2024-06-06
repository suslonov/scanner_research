#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
import json
import pandas as pd
import numpy as np

from db.bot_db import DBMySQL
from db.remote import open_remote_port, close_remote_port, RemoteServer
from _utils.uniswap import amount_out_v2, amount_in_v2, WETH

PARAMETERS_FILE = "~/git/scanner_research/parameters.json"
CSV_FILE = '/media/Data/csv/sniping_test_'
# CSV_FILE = '/home/anton/tmp/sniping_test_'
SHORT_TERM_BLOCKS_LIMIT = 100
MAX_POSITION_ETH = 0.1
MIN_POSITION_ETH = 0.001
TAKE_PROFITS = [0.1, 0.5, 1, 2, 3]
MAX_POSITION_TERM = 100

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

def get_token_max_wallets():
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_tokens_with_property("_maxWalletSize")

def get_pair_range_history(pair_id_start, pair_id_end):
    if pair_id_end == pair_id_start:
        pair_id_end = pair_id_start + 1
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_event_history_many(pair_id_start, pair_id_end)

def save_results_to_db(analytics):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            for i in analytics:
                # try:
                    db.update_json("t_pairs", i, "price_analytics", 
                                   {a: None if pd.isnull(analytics[i][1][a]) else analytics[i][1][a] for a in analytics[i][1]})
                # except:
                #     print(i, analytics[i][1])


def backtest1(start_pair, end_pair, test_ratio):

    # test_ratio = 0.05
    # start_pair = 270000
    # end_pair = 271000
    # end_pair = 280000
    # i = start_pair

    # start_pair = 280000
    # end_pair = 290000
    # end_pair = 300000
    
    pairs = get_pairs_with_contracts(start_pair, end_pair)
    price_history = get_pair_range_history(start_pair, end_pair)
    token_max_wallets = get_token_max_wallets()
    max_wallet_size = {w["token"]: w["_maxWalletSize"] for w in token_max_wallets}

    df_pairs = pd.DataFrame.from_records(pairs, index=["pair_id"])
    df_price_history = pd.DataFrame.from_records(price_history, index=["event_id"])

    df_price_history.loc[df_price_history["operation"] == "burn", "amount0"] = -df_price_history.loc[df_price_history["operation"] == "burn", "amount0"]
    df_price_history.loc[df_price_history["operation"] == "burn", "amount1"] = -df_price_history.loc[df_price_history["operation"] == "burn", "amount1"]

# price is always Ntokens/Neth

    analytics = {}
    for i in df_pairs.loc[(df_pairs.index >= start_pair) & (df_pairs.index < end_pair)].index:
        print(i)
        if not df_pairs.loc[i, "swaps"]:
            continue
        is_token_0 = df_pairs.loc[i, "token"] == df_pairs.loc[i, "token0"]
        if df_pairs.loc[i, "decimals"] <= 18:
            multiplier = 10 ** (18 - df_pairs.loc[i, "decimals"])
            multiplier1 = 1
        else:
            multiplier = 1
            multiplier1 = 10 ** (df_pairs.loc[i, "decimals"] - 18)
            
        df_price_history_i_pair = df_price_history.loc[df_price_history["pair_id"]==i]
       
        df_price_history_i_pair["reserve0"] = (
            df_price_history_i_pair["amount0"].where(df_price_history_i_pair["operation"] == "mint", 0).cumsum() -
            df_price_history_i_pair["amount0"].where(df_price_history_i_pair["operation"] == "burn", 0).cumsum() +
            df_price_history_i_pair["amount0In"].cumsum() -
            df_price_history_i_pair["amount0Out"].cumsum())
        df_price_history_i_pair["reserve1"] = (
            df_price_history_i_pair["amount1"].where(df_price_history_i_pair["operation"] == "mint", 0).cumsum() -
            df_price_history_i_pair["amount1"].where(df_price_history_i_pair["operation"] == "burn", 0).cumsum() +
            df_price_history_i_pair["amount1In"].cumsum() -
            df_price_history_i_pair["amount1Out"].cumsum())

        zero_block_number = df_price_history_i_pair["block_number"].min()
        last_block_number = df_price_history_i_pair["block_number"].max()
        first_block_number = df_price_history_i_pair.loc[df_price_history_i_pair["operation"] == "swap V2", "block_number"].min()
        first_block_number_after_1 = df_price_history_i_pair.loc[df_price_history_i_pair["block_number"] > first_block_number, "block_number"].min()
        first_block_number_after_5 = df_price_history_i_pair.loc[df_price_history_i_pair["block_number"] >= first_block_number + 5, "block_number"].min()
        first_block_number_after_10 = df_price_history_i_pair.loc[df_price_history_i_pair["block_number"] >= first_block_number + 10, "block_number"].min()

        last0 = df_price_history_i_pair.loc[df_price_history_i_pair["block_number"] == zero_block_number].index.max()
        last1 = df_price_history_i_pair.loc[df_price_history_i_pair["block_number"] == first_block_number].index.max()
        last2 = df_price_history_i_pair.loc[df_price_history_i_pair["block_number"] == first_block_number_after_1].index.max()
        last5 = df_price_history_i_pair.loc[df_price_history_i_pair["block_number"] == first_block_number_after_5].index.max()
        last10 = df_price_history_i_pair.loc[df_price_history_i_pair["block_number"] == first_block_number_after_10].index.max()

        (liquidity0, liquidity1) = tuple(df_price_history_i_pair.loc[df_price_history_i_pair["block_number"] == zero_block_number][["amount0","amount1"]].sum())

        no_sells = 0
        if is_token_0:
            if df_price_history_i_pair["amount0Out"].sum() == 0:
                no_sells = 1
            test_amount_eth18 = int(min(liquidity1 * test_ratio, MAX_POSITION_ETH * 1e18))
            if df_pairs.loc[i, "token"] in max_wallet_size:
                max_token_wallet_size = np.double(max_wallet_size[df_pairs.loc[i, "token"]])
                test_amount_eth18_1 = max_token_wallet_size * multiplier * np.double(df_price_history_i_pair.loc[last1, "reserve1"]) / multiplier1 / np.double(df_price_history_i_pair.loc[last1, "reserve0"] * multiplier)
                test_amount_eth18 = int(min(test_amount_eth18, test_amount_eth18_1))
            if test_amount_eth18 < MIN_POSITION_ETH * 1e18:
                continue
           
            test_amount_token = test_amount_eth18 * np.double(df_price_history_i_pair["reserve0"]) * multiplier / np.double(df_price_history_i_pair["reserve1"] / multiplier1)
            df_price_history_i_pair["price_simple"] = (
                np.double(df_price_history_i_pair["reserve0"] * multiplier) /
                np.double(df_price_history_i_pair["reserve1"]) / multiplier1)
            df_price_history_i_pair.loc[df_price_history_i_pair["operation"]=="swap V2", "price_trade"] = (
                np.double((df_price_history_i_pair.loc[df_price_history_i_pair["operation"]=="swap V2","amount0In"] +
                df_price_history_i_pair.loc[df_price_history_i_pair["operation"]=="swap V2","amount0Out"]) * multiplier) /
                np.double(df_price_history_i_pair.loc[df_price_history_i_pair["operation"]=="swap V2","amount1In"] +
                df_price_history_i_pair.loc[df_price_history_i_pair["operation"]=="swap V2","amount1Out"]) / multiplier1)
            df_price_history_i_pair["price_amount_buy"] = amount_out_v2(
                test_amount_eth18,
                np.double(df_price_history_i_pair["reserve1"]) * multiplier1,
                np.double(df_price_history_i_pair["reserve0"]) * multiplier) / test_amount_eth18
            df_price_history_i_pair["price_amount_sell"] = test_amount_token / amount_out_v2(
                test_amount_token,
                np.double(df_price_history_i_pair["reserve0"]) * multiplier,
                np.double(df_price_history_i_pair["reserve1"]) * multiplier1)
        else:
            if df_price_history_i_pair["amount1Out"].sum() == 0:
                no_sells = 1
            test_amount_eth18 = int(min(liquidity0 * test_ratio, MAX_POSITION_ETH * 1e18))
            if df_pairs.loc[i, "token"] in max_wallet_size:
                max_token_wallet_size = np.double(max_wallet_size[df_pairs.loc[i, "token"]])
                test_amount_eth18_1 = max_token_wallet_size / multiplier1 * np.double(df_price_history_i_pair.loc[last1, "reserve0"]) * multiplier / np.double(df_price_history_i_pair.loc[last1, "reserve1"] / multiplier1)
                test_amount_eth18 = int(min(test_amount_eth18, test_amount_eth18_1))
            if test_amount_eth18 < MIN_POSITION_ETH * 1e18:
                continue
             
            test_amount_token = test_amount_eth18 * np.double(df_price_history_i_pair["reserve1"]) * multiplier / np.double(df_price_history_i_pair["reserve0"] / multiplier1)
            df_price_history_i_pair["price_simple"] = (
                np.double(df_price_history_i_pair["reserve1"] * multiplier) / 
                np.double(df_price_history_i_pair["reserve0"]) / multiplier1)
            df_price_history_i_pair.loc[df_price_history_i_pair["operation"]=="swap V2", "price_trade"] = (
                np.double((df_price_history_i_pair.loc[df_price_history_i_pair["operation"]=="swap V2","amount1In"] +
                df_price_history_i_pair.loc[df_price_history_i_pair["operation"]=="swap V2","amount1Out"]) * multiplier) / 
                np.double(df_price_history_i_pair.loc[df_price_history_i_pair["operation"]=="swap V2","amount0In"] +
                df_price_history_i_pair.loc[df_price_history_i_pair["operation"]=="swap V2","amount0Out"]) /  multiplier1)
            df_price_history_i_pair["price_amount_buy"] = amount_out_v2(
                test_amount_eth18,
                np.double(df_price_history_i_pair["reserve0"]) * multiplier1,
                np.double(df_price_history_i_pair["reserve1"]) * multiplier) / test_amount_eth18
            df_price_history_i_pair["price_amount_sell"] = test_amount_token / amount_out_v2(
                test_amount_token,
                np.double(df_price_history_i_pair["reserve1"]) * multiplier,
                np.double(df_price_history_i_pair["reserve0"]) * multiplier1)

        (amount0In0, amount1In0, amount0Out0, amount1Out0) = tuple(df_price_history_i_pair.loc[df_price_history_i_pair["block_number"] == zero_block_number][["amount0In", "amount1In", "amount0Out", "amount1Out"]].sum())
        trades_count0 = df_price_history_i_pair.loc[(df_price_history_i_pair["block_number"] == zero_block_number) &
                                                    (df_price_history_i_pair["operation"] == "swap V2")]["block_number"].count()
        first_trade_id = df_price_history_i_pair.loc[(df_price_history_i_pair["block_number"] == zero_block_number) &
                                                    (df_price_history_i_pair["operation"] == "swap V2")].index.min()
        traders_count0 = df_price_history_i_pair.loc[(df_price_history_i_pair["block_number"] == zero_block_number) &
                                            (df_price_history_i_pair["operation"] == "swap V2")]["sender"].nunique()
        liquidity_id = df_price_history_i_pair.loc[(df_price_history_i_pair["block_number"] == zero_block_number) &
                                                    (df_price_history_i_pair["operation"] == "mint")].index.min()
        
        if pd.isnull(liquidity_id):
            analytics[i] = (df_pairs.loc[i, "token"], {
                "history_length":int(last_block_number - zero_block_number),
                })
            continue

        first_lp = df_price_history_i_pair.loc[liquidity_id, "sender"]
        other_traders_count0 = df_price_history_i_pair.loc[(df_price_history_i_pair["block_number"] == zero_block_number) &
                                            (df_price_history_i_pair["operation"] == "swap V2") &
                                            (df_price_history_i_pair["sender"] != first_lp)]["sender"].nunique()
        
        if not pd.isnull(first_trade_id):
            df_price_history_i_pair.loc[first_trade_id]
            if is_token_0:
                first_trade_price = (np.double(df_price_history_i_pair.loc[first_trade_id,"amount0In"] +
                    df_price_history_i_pair.loc[first_trade_id,"amount0Out"]) * multiplier /
                    np.double(df_price_history_i_pair.loc[first_trade_id,"amount1In"] +
                    df_price_history_i_pair.loc[first_trade_id,"amount1Out"]) / multiplier1)
                first_trade_size = df_price_history_i_pair.loc[first_trade_id,"amount1In"] + df_price_history_i_pair.loc[first_trade_id,"amount1Out"]
            else:
                first_trade_price = (np.double(df_price_history_i_pair.loc[first_trade_id,"amount1In"] +
                    df_price_history_i_pair.loc[first_trade_id,"amount1Out"]) * multiplier /
                    np.double(df_price_history_i_pair.loc[first_trade_id,"amount0In"] +
                    df_price_history_i_pair.loc[first_trade_id,"amount0Out"]) / multiplier1)
                first_trade_size = df_price_history_i_pair.loc[first_trade_id,"amount0In"] + df_price_history_i_pair.loc[first_trade_id,"amount0Out"]
        else:
            first_trade_price = None
            first_trade_size = None

        try:
            max_point = df_price_history_i_pair.loc[(df_price_history_i_pair["operation"] == "swap V2")
                                                & (df_price_history_i_pair["block_number"] - zero_block_number <= SHORT_TERM_BLOCKS_LIMIT), "price_amount_sell"].idxmin()
            price_sell_best = df_price_history_i_pair.loc[max_point, "price_amount_sell"]
        except:
            max_point = None; price_sell_best = None
        try:
            (price_trade0, price_amount_buy0, price_amount_sell0) = tuple(df_price_history_i_pair.loc[last0, ["price_trade", "price_amount_buy", "price_amount_sell"]])
        except:
            (price_trade0, price_amount_buy0, price_amount_sell0) = (None, None, None)
        try:
            (price_trade1, price_amount_buy1, price_amount_sell1) = tuple(df_price_history_i_pair.loc[last1, ["price_trade", "price_amount_buy", "price_amount_sell"]])
        except:
            (price_trade1, price_amount_buy1, price_amount_sell1) = (None, None, None)
        try:
            (price_trade2, price_amount_buy2, price_amount_sell2) = tuple(df_price_history_i_pair.loc[last2, ["price_trade", "price_amount_buy", "price_amount_sell"]])
        except:
            (price_trade2, price_amount_buy2, price_amount_sell2) = (None, None, None)
        try:
            (price_trade5, price_amount_buy5, price_amount_sell5) = tuple(df_price_history_i_pair.loc[last5, ["price_trade", "price_amount_buy", "price_amount_sell"]])
        except:
            (price_trade5, price_amount_buy5, price_amount_sell5) = (None, None, None)
        try:
            (price_trade10, price_amount_buy10, price_amount_sell10) = tuple(df_price_history_i_pair.loc[last10, ["price_trade", "price_amount_buy", "price_amount_sell"]])
        except:
            (price_trade10, price_amount_buy10, price_amount_sell10) = (None, None, None)

        if is_token_0:
            analytics[i] = (df_pairs.loc[i, "token"], {
                "history_length":int(last_block_number - zero_block_number),
                "liquidity_ETH": liquidity1,
                "liquidity_token": liquidity0,
                "amount_ETH_In0": amount1In0,
                "amount_token_In0": amount0In0,
                "amount_ETH_Out0": amount1Out0,
                "amount_token_Out0": amount0Out0,
                "first_trade_price": first_trade_price,
                "first_trade_size": first_trade_size,
                "test_amount_eth": test_amount_eth18,
                "trades_count0": trades_count0,
                "traders_count0": traders_count0,
                "other_traders_count0": other_traders_count0,
                "price_trade0": price_trade0,
                "price_amount_buy0": price_amount_buy0,
                "price_amount_sell0": price_amount_sell0,
                "price_trade1": price_trade1,
                "price_amount_buy1": price_amount_buy1,
                "price_amount_sell1": price_amount_sell1,
                "price_trade2": price_trade2,
                "price_amount_buy2": price_amount_buy2,
                "price_amount_sell2": price_amount_sell2,
                "price_trade5": price_trade5,
                "price_amount_sell5": price_amount_sell5,
                "price_trade10": price_trade10,
                "price_amount_sell10": price_amount_sell10,
                "sell_optimum": (int(df_price_history.loc[max_point, "block_number"] - first_block_number) if max_point else None),
                "price_sell_best": price_sell_best,
                "first_block_number": first_block_number,
                "no_sells": no_sells,
                })
        else:
            analytics[i] = (df_pairs.loc[i, "token"], {
                "history_length":int(last_block_number - zero_block_number),
                "liquidity_ETH": liquidity0,
                "liquidity_token": liquidity1,
                "amount_ETH_In0": amount0In0,
                "amount_token_In0": amount1In0,
                "amount_ETH_Out0": amount0Out0,
                "amount_token_Out0": amount1Out0,
                "trades_count0": trades_count0,
                "first_trade_price": first_trade_price,
                "first_trade_size": first_trade_size,
                "test_amount_eth": test_amount_eth18,
                "traders_count0": traders_count0,
                "other_traders_count0": other_traders_count0,
                "price_trade0": price_trade0,
                "price_amount_buy0": price_amount_buy0,
                "price_amount_sell0": price_amount_sell0,
                "price_trade1": price_trade1,
                "price_amount_buy1": price_amount_buy1,
                "price_amount_sell1": price_amount_sell1,
                "price_trade2": price_trade2,
                "price_amount_buy2": price_amount_buy2,
                "price_amount_sell2": price_amount_sell2,
                "price_trade5": price_trade5,
                "price_amount_sell5": price_amount_sell5,
                "price_trade10": price_trade10,
                "price_amount_sell10": price_amount_sell10,
                "sell_optimum": (int(df_price_history.loc[max_point, "block_number"] - first_block_number) if max_point else None),
                "price_sell_best": price_sell_best,
                "first_block_number": first_block_number,
                "no_sells": no_sells,
                })


        take_profit_results = {}
        take_profit_blocks = {}
        if not price_amount_buy1 is None:
            for take_profit in TAKE_PROFITS:
                if no_sells:
                    take_profit_results["Take_profit" + str(take_profit)] = - test_amount_eth18
                    take_profit_blocks["TP_block" + str(take_profit)] = MAX_POSITION_TERM
                    continue
                    
                take_profit_block = df_price_history_i_pair.loc[(df_price_history_i_pair["price_amount_sell"] < price_amount_buy1  / (1 + take_profit)) & # price is Ntokens/Neth
                                                        (df_price_history_i_pair["block_number"] <= first_block_number + MAX_POSITION_TERM) &
                                                        (df_price_history_i_pair["operation"]=="swap V2"),
                                                        "block_number"].min()
                
                if pd.isnull(take_profit_block):
                    take_profit_block = df_price_history_i_pair.loc[(df_price_history_i_pair["block_number"] >= first_block_number + MAX_POSITION_TERM) &
                                                            (df_price_history_i_pair["operation"]=="swap V2"), "block_number"].min()

                if pd.isnull(take_profit_block):
                    take_profit_results["Take_profit" + str(take_profit)] = - test_amount_eth18
                    take_profit_blocks["TP_block" + str(take_profit)] = MAX_POSITION_TERM
                else:
                    take_profit_block = min(take_profit_block, first_block_number + MAX_POSITION_TERM + 1)
                    take_profit_point = df_price_history_i_pair.loc[df_price_history_i_pair["block_number"] <= take_profit_block].index.max()
                    take_profit_price = df_price_history_i_pair.loc[take_profit_point, "price_amount_sell"]
                    take_profit_results["Take_profit" + str(take_profit)] = test_amount_eth18 * (price_amount_buy1 / take_profit_price - 1)
                    take_profit_blocks["TP_block" + str(take_profit)] = int(take_profit_block - first_block_number)
                
        analytics[i] = (analytics[i][0],  analytics[i][1] | take_profit_results | take_profit_blocks)


    # save_results_to_db(analytics)
    results = {}
    for i in analytics:
        results[analytics[i][0]] = analytics[i][1]
        results[df_pairs.loc[i, "token"]]["decimals"] = df_pairs.loc[i, "decimals"]
        results[df_pairs.loc[i, "token"]]["is_token_0"] = 1 if df_pairs.loc[i, "token"] == df_pairs.loc[i, "token0"] else 0
        results[df_pairs.loc[i, "token"]]["pair_id"] = i
        results[df_pairs.loc[i, "token"]]["zero_block_number"] = int(df_pairs.loc[i, "first_block_number"])

    columns = ['history_length', 'liquidity_ETH', 'liquidity_token', 'amount_ETH_In0',
        'amount_token_In0', 'amount_ETH_Out0', 'amount_token_Out0',
        'trades_count0', 'first_trade_price', "first_trade_size", "traders_count0", "other_traders_count0",
        'price_trade0', 'price_amount_buy0', 'price_amount_sell0',
        'price_trade1', 'price_amount_buy1', 'price_amount_sell1',
        'price_trade2', 'price_amount_buy2', 'price_amount_sell2',
        'price_trade5', 'price_amount_sell5', 'price_trade10', 'price_amount_sell10',
        'sell_optimum', 'price_sell_best', 'decimals', 'is_token_0', 'pair_id',
        'zero_block_number', 'test_amount_eth', "first_block_number", "no_sells"]
    columns += ["Take_profit" + str(t) for t in TAKE_PROFITS]
    columns += ["TP_block" + str(t) for t in TAKE_PROFITS]

    df_results = pd.DataFrame.from_dict(results, orient="index", columns=columns)
    df_results[["Take_profit" + str(t) for t in TAKE_PROFITS]] = df_results[["Take_profit" + str(t) for t in TAKE_PROFITS]]/1e18
    df_results['test_amount_eth'] = df_results['test_amount_eth']/1e18

    df_results.to_csv(CSV_FILE + str(start_pair) + "_" + str(end_pair) + "_" + str(test_ratio) + ".csv")

def main():
    # backtest1(270000, 280000, 0.1)
    backtest1(270000, 280000, 0.05)
    # backtest1(280000, 290000, 0.05)

if __name__ == '__main__':
    main()
