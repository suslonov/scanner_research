#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from datetime import datetime
import pandas as pd
from _utils.etherscan import get_contract_sync, get_token_transactions

CSV_FILE = "/media/Data/csv/pair"
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'

SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
BURN_TOPIC = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496"
MINT_TOPIC = "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f"

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    ETHERSCAN_KEY = k1.strip('\n')


pairs = ["0xabc8756A67e9846901f2377ee79af8B917f97FE3",
         "0x050D267a0EEc42E5E79D5378E2866C0f5B45b6a3",
         "0x0CB1025c0C45eB9398f5692242C6c7DfE383ddFb",
         "0xC0a1B9BbDDDDb518621033Ff7B9F2a11788229Bf",
         "0xaFa69359D3B488E5000c55A1dfb0E05D89af9eb2",
         "0x19195732A723320eDF29F78e2F1a9Bd342284C9f",
         "0x4bDE6800BF2df6C3AD3E0EF1A8537dCc9A63789b",
         "0xaaB2C7fD040C82AE062AA8A930C13cb6d61bD7f5",
         "0x65f99C6c466ae863ed87eA2622cbFAF988C3A9D4",
         "0x52eDd8661177C672023079dA5f8601A4260c1dAa",
         "0x0461BE2DA7D674cC90168baa40B22B582D7416B9",
         "0x357b5fd66ede61137642a752B76d4614034735D7",
         "0x859DC5a3863094C65512587720eB35327bDb6861",
         "0xC32a2564f68aE27E082c0789C24299F182866562"]


for p in pairs:
    
    p = "0xd05Bef4C7f9c1e53960937733A8594bb4CfB2ede"
    
    transactions = get_token_transactions(p, ETHERSCAN_KEY)
    
    data_list = []
    for trnx in transactions:
        if trnx["topics"][0] == SWAP_TOPIC:
            data_list.append({"transactionHash": trnx["transactionHash"],
                              "block": int(trnx["blockNumber"], 0),
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
                              "block": int(trnx["blockNumber"], 0),
                              "timeStamp": datetime.utcfromtimestamp(int(trnx["timeStamp"], 0)),
                              "operation": "mint",
                              "sender": trnx["topics"][1][:2]+trnx["topics"][1][-40:],
                              "amount0": int(trnx["data"][:66], 0),
                              "amount1": int("0x" + trnx["data"][66:130], 0),
                             })
#!!!  sender is not correct for mint and burn
        elif trnx["topics"][0] == BURN_TOPIC:
            data_list.append({"transactionHash": trnx["transactionHash"],
                              "block": int(trnx["blockNumber"], 0),
                              "timeStamp": datetime.utcfromtimestamp(int(trnx["timeStamp"], 0)),
                              "operation": "burn",
                              "sender": trnx["topics"][1][:2]+trnx["topics"][1][-40:],
                              "amount0": int(trnx["data"][:66], 0),
                              "amount1": int("0x" + trnx["data"][66:130], 0),
                             })

    df = pd.DataFrame.from_records(data_list)

    # df.to_csv(CSV_FILE + p + ".csv")


    df.loc[(df["block"] == 18719772) & (df["operation"] == "swap V2")]["amount1In"] / df.loc[(df["block"] == 18719772) & (df["operation"] == "swap V2")]["amount0Out"]

    df.groupby(["sender"])[["amount0In", "amount0Out"]].count()
    df.groupby(["sender"])[["amount0In", "amount0Out"]].sum()/1e18
    df.groupby(["sender"])[["amount1In", "amount1Out"]].sum()/1e18
    df.groupby(["operation"])[["amount0"]].sum()/1e18
    
    
