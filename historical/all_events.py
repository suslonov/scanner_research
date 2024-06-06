#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
import json
import pandas as pd
import numpy as np

from db.bot_db import DBMySQL
from db.remote import open_remote_port, close_remote_port, RemoteServer
from _utils.web3connect import web3connect2
from _utils.uniswap import WETH
from _utils.etherscan import get_contract_sync, etherscan_get_internals
from _utils.utils import s64

REQUEST_CACHE = '/media/Data/eth/eth'
PARAMETERS_FILE = "~/git/scanner_research/parameters.json"
CSV_DIR = '/media/Data/csv/'
# CSV_FILE = '/home/anton/tmp/sniping_test_'
KEY_FILE = '../keys/alchemy.sec'
KEY_FILE1 = '../keys/alchemy1.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None

def create_tables():
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            db.create_tables(["t_attakers", "t_sandwich_tx", "t_sandwich_logs", "t_filter_events", "t_processed_blocks"])

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    ETHERSCAN_KEY = k1.strip('\n')

def process_block(block_number, run_context):
    w3 = run_context["w3"]
    block = w3.eth.get_block(block_number, full_transactions=True)

    if len(block["transactions"]) == 0:
        return {}, [], {}, {}

    miner = block["miner"]
    base_fee_per_gas = block["baseFeePerGas"]
    
    _from_to_hashes = {}
    for transaction in block["transactions"]:
        if "to" in transaction and not transaction["to"] is None:
            if transaction["to"] in run_context["multisender_attackers"]:
                transaction_from = None
            else:
                transaction_from = transaction["from"]
            
            if (transaction_from, transaction["to"]) in run_context["attaker_status"] and run_context["attaker_status"][(transaction_from, transaction["to"])] == 0:
                continue
            if not (transaction_from, transaction["to"]) in _from_to_hashes:
                _from_to_hashes[(transaction_from, transaction["to"])] = {"count": 1,
                                                                            "tx_counter": 0,
                                                                            "min_index": transaction["transactionIndex"],
                                                                            "max_index": transaction["transactionIndex"]}
            else:
                _from_to_hashes[(transaction_from, transaction["to"])]["count"] += 1
                _from_to_hashes[(transaction_from, transaction["to"])]["min_index"] = min(_from_to_hashes[(transaction_from, transaction["to"])]["min_index"],
                                                                                            transaction["transactionIndex"])
                _from_to_hashes[(transaction_from, transaction["to"])]["max_index"] = max(_from_to_hashes[(transaction_from, transaction["to"])]["max_index"],
                                                                                            transaction["transactionIndex"])
    from_to_hashes = {f: _from_to_hashes[f] for f in _from_to_hashes if _from_to_hashes[f]["count"] > 1}

    block_transactions = {}
    block_events = []
    block_attakers = {}

    for from_to in from_to_hashes:
        from_to_events = []
        block_attakers[from_to] = {"status": 1}
        min_index = None
        max_index = None
        for ti in range(from_to_hashes[from_to]["min_index"], from_to_hashes[from_to]["max_index"]+1):
            transaction = block["transactions"][ti]
            if (transaction["from"] != from_to[0] and not from_to[0] is None) or transaction["to"] != from_to[1]:
                continue
            transaction_hash = transaction["hash"].hex()
            if transaction["transactionIndex"] != ti:
                print("something wrong with transaction index", block_number, ti, transaction["transactionIndex"])
                break
            if not transaction_hash in block_transactions:
                receipt = w3.eth.get_transaction_receipt(transaction_hash)
                if receipt["status"] != 1:
                    continue
                tx_logs = []

                for e in receipt["logs"]:
                    e = dict(e)
                    e["transactionHash"] = e["transactionHash"].hex()
                    e["topics"] = [bt.hex() for bt in e["topics"]]
                    e["role"] = 1

                    if not e["topics"][0] in run_context["topic_filter"]:
                        if not e["topics"][0] in run_context["unknown_topics"]:
                            run_context["unknown_topics"][e["topics"][0]] = transaction_hash
                        # print("unknown topic in sandwich-like transactions", transaction_hash, e["topics"][0])
                        tx_logs.append(e)
                    elif not run_context["topic_filter"][e["topics"][0]]["note"] is None:
                        tx_logs.append(e)
                    elif (e["topics"][0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and len(e["topics"]) > 2 and
                          (e["topics"][1] == "0x0000000000000000000000000000000000000000000000000000000000000000" or 
                          e["topics"][2] == "0x0000000000000000000000000000000000000000000000000000000000000000")):
                        tx_logs.append(e) #!!! special case: Tranfer as NFT mint/burn
                    elif (e["topics"][0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and len(e["topics"]) > 1 and
                          e["topics"][2][-40:] == e["address"][-40:].lower()):
                        tx_logs.append(e) #!!! special case: Tranfer tax
    
                if len(tx_logs) > 0:
                    if max_index is None or max_index < ti:
                        max_index = ti
                    if min_index is None or min_index > ti:
                        min_index = ti
                    from_to_events.extend(tx_logs)
                block_transactions[transaction_hash] = {"block_number": block_number,
                                                        "from": transaction["from"],
                                                        "to": transaction["to"],
                                                        "role": 1,
                                                        "gas_burnt": base_fee_per_gas * receipt["gasUsed"] /1e18,
                                                        "gas_overpay": (receipt["effectiveGasPrice"] - base_fee_per_gas) * receipt["gasUsed"] /1e18,
                                                        "attacker": from_to,
                                                        "roles": {}}
                block_transactions[transaction_hash]["roles"][from_to] = 1
        from_to_hashes[from_to]["min_index"] = min_index
        from_to_hashes[from_to]["max_index"] = max_index
        if len(from_to_events) == 0:
            block_attakers[from_to] = {"status": 0}
        else:
            block_events.extend(from_to_events)


    for transaction in block["transactions"]:
        transaction_hash = transaction["hash"].hex()
        for from_to in from_to_hashes:
            if block_attakers[from_to]["status"] == 0:
                continue
            if (transaction["transactionIndex"] > from_to_hashes[from_to]["min_index"] and
                transaction["transactionIndex"] < from_to_hashes[from_to]["max_index"]):
                if (transaction["from"] == from_to[0] or from_to[0] is None) and transaction["to"] == from_to[1]:
                    continue
                if "to" in transaction:
                    receipt = w3.eth.get_transaction_receipt(transaction["hash"])
                    tx_logs = []
                    for e in receipt["logs"]:
                        e = dict(e)
                        e["transactionHash"] = e["transactionHash"].hex()
                        e["topics"] = [bt.hex() for bt in e["topics"]]
                        e["role"] = 0
    
                        if not e["topics"][0] in run_context["topic_filter"]:
                            tx_logs.append(e)
                        elif not run_context["topic_filter"][e["topics"][0]]["note"] is None:
                            tx_logs.append(e)
                        elif (e["topics"][0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and len(e["topics"]) > 2 and
                              (e["topics"][1] == "0x0000000000000000000000000000000000000000000000000000000000000000" or 
                              e["topics"][2] == "0x0000000000000000000000000000000000000000000000000000000000000000")):
                            tx_logs.append(e) #!!! special case: Tranfer as NFT mint/burn
                        elif (e["topics"][0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and len(e["topics"]) > 1 and
                              e["topics"][2][-40:] == e["address"][-40:].lower()):
                            tx_logs.append(e) #!!! special case: Tranfer tax
                    
                    if len(tx_logs) == 0:
                        break
                    block_events.extend(tx_logs)
                    if not transaction_hash in block_transactions:
                        block_transactions[transaction_hash] = {"block_number": block_number,
                                               "from": transaction["from"],
                                               "to": transaction["to"],
                                               "role": 0,
                                               "gas_burnt": base_fee_per_gas * receipt["gasUsed"] /1e18,
                                               "gas_overpay": (receipt["effectiveGasPrice"] - base_fee_per_gas) * receipt["gasUsed"] /1e18,
                                               "attacker": from_to,
                                               "roles": {}}
                    block_transactions[transaction_hash]["roles"][from_to] = 0
                    from_to_hashes[from_to]["tx_counter"] += 1
                    
    hashes_to_delete = []
    for from_to in from_to_hashes:
        if not from_to_hashes[from_to]["tx_counter"]:
            block_attakers[from_to] = {"status": 0}
            for t in block_transactions:
                if block_transactions[t]["attacker"] == from_to:
                    hashes_to_delete.append(t)

    block_transactions = {t: block_transactions[t] for t in block_transactions if not t in hashes_to_delete}
    block_events = [e for e in block_events if not e["transactionHash"] in hashes_to_delete]
    
    if len([1 for a in block_attakers if block_attakers[a]["status"] == 1]):
        internal_transactions = etherscan_get_internals(etherscan_key=run_context["etherscan_key"],
                                                        block_number=block_number, address=miner,
                                                        session=(run_context["cached_session"] if "cached_session" in run_context else None))
        for itx in internal_transactions:
            if itx["to"] == miner.lower() and itx["hash"] in block_transactions and block_transactions[itx["hash"]]["role"] == 1:
                block_transactions[itx["hash"]]["direct_bribe"] = int(itx["value"]) / 1e18

    block_bundles = {(block_number, from_to[0], from_to[1]):
                     {"transactions": [], "tx_counter": from_to_hashes[from_to]["tx_counter"]}
                     for from_to in from_to_hashes if from_to_hashes[from_to]["tx_counter"] > 0}
    for t in block_transactions:
        if block_transactions[t]["role"] == 1:
            if (block_number, block_transactions[t]["from"], block_transactions[t]["to"]) in block_bundles:
                block_bundles[(block_number, block_transactions[t]["from"], block_transactions[t]["to"])]["transactions"].append(t)
            elif (block_number, None, block_transactions[t]["to"]) in block_bundles:
                block_bundles[(block_number, None, block_transactions[t]["to"])]["transactions"].append(t)
                
    return block_transactions, block_events, block_attakers, block_bundles


def main():

    last_block = None
    N_blocks = 100000

    # last_block = 19161500 # range 1
    # N_blocks = 100000

    # last_block = 19250000 # range 2
    # N_blocks = 200000

    # last_block = 19096571+500
    # N_blocks = 1000

    w3_direct, cached_session, w3, latest_block, _ = web3connect2(KEY_FILE, KEY_FILE1)

    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            topic_filter = db.get_topic_filters()
            attakers_list = db.get_attackers()
    attakers = {}
    multisender_attackers = []
    for a in attakers_list:
        attakers[a["tx_from"], a["tx_to"]] = a["status"]
        if a["tx_from"] is None and a["status"] == 1:
            multisender_attackers.append(a["tx_to"])
 
    abi_storage = {}
    contract_storage = {}
    unknown_topics = {}
    run_context = {"w3_direct": w3_direct, 
                    "cached_session": cached_session, 
                    "w3": w3,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "contract_storage": contract_storage,
                    "attaker_status": attakers,
                    "multisender_attackers": multisender_attackers,
                    "topic_filter": topic_filter,
                    "unknown_topics": unknown_topics,
                    }


    latest_block_number = latest_block["number"]
    print(latest_block_number)
    if last_block is None:
        last_block = latest_block_number

    block_number = last_block
    new_transactions = {}
    new_events = []
    new_attackers = set()
    new_bundles = {}
    while block_number > last_block - N_blocks:
        block_transactions, block_events, block_attackers, block_bundles = process_block(block_number, run_context)
        # block_transactions1, block_events1, block_attackers1 = filter_data(run_context, block_number, block_transactions, block_events, block_attackers)
        print(block_number, block_number - (last_block - N_blocks), len(block_transactions), len(block_events), len(block_bundles))

        new_transactions.update(block_transactions)
        new_bundles.update(block_bundles)
        new_events.extend(block_events)
        for a in block_attackers:
            if block_attackers[a]["status"] == 1:
                new_attackers.add(a)
        
        # write block transactions
        block_number -= 1
        


def management(run_context, new_events, new_transactions, new_bundles):
    
    print(len(run_context["unknown_topics"]))
    print(run_context["unknown_topics"].keys())
    print(run_context["unknown_topics"])
    unknown_topics = run_context["unknown_topics"]

    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            db.add_topic_filter('0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0','Collect', "Collect (index_topic_1 address owner, address recipient, index_topic_2 int24 tickLower, index_topic_3 int24 tickUpper, uint128 amount0, uint128 amount1)")


            
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            db.add_attacker(None, "0xb0000000aa4f00af1200c8b2befb6300853f0069", 1, "from Eli")
            db.add_attacker("0x2C169DFe5fBbA12957Bdd0Ba47d9CEDbFE260CA7", "0xc662c410C0ECf747543f5bA90660f6ABeBD9C8c4", 0)


    def get_signature(t):
        import requests
        url = "https://www.4byte.directory/signatures/?bytes4_signature=" + t[:10] + "&sort=id"
        res = requests.get(url)
    
        l = res.text.split("\n")
    
        for ll in l:
            if ll.find('<td class="text_signature">') >= 0:
                signature = ll[ll.find(">")+1:ll.find("<", ll.find(">"))]
                if signature:
                    return signature
        return None
    
    def get_signature_xyz(t):
        import requests
        url = "https://api.openchain.xyz/signature-database/v1/lookup?event=" + t + "&filter=true"
        res = requests.get(url)
        d = res.json()
        if not "result" in d:
            return None
        if not d["result"]["event"][t]:
            return None
        return d["result"]["event"][t][0]["name"]


    event_counter = {}
    for e in new_events:
        topic = e["topics"][0]
        if not topic in event_counter:
            event_counter[topic] = {"attacker": 0, "victim": 0}
        if e["role"] == 1:
            event_counter[topic]["attacker"] += 1
        else:
            event_counter[topic]["victim"] += 1
    for e in event_counter:
        if e in run_context["topic_filter"]:
            print(e, event_counter[e], run_context["topic_filter"][e]["note"])
        else:
            print(e, event_counter[e])

    event_counter = {}
    for e in new_events:
        topic = e["topics"][0]
        if not topic in event_counter:
            event_counter[topic] = 0
        if e["role"] == 1:
            event_counter[topic] += 1
    for e in event_counter:
        if not e in run_context["topic_filter"] and event_counter[e] >= 10:
            print(event_counter[e], ";", e, run_context["unknown_topics"][e], ";", get_signature_xyz(e))

    pairs_V2 = {}
    collect_V2 = []
    for ii, e in enumerate(new_events):
        if e["topics"][0] == "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822" and e["role"] == 1:
            if not e["address"] in pairs_V2:
                contract, _ = get_contract_sync(e["address"], w3=run_context["w3"], context=run_context, abi_type="pair")
                token0 = contract.functions.token0().call()
                token1 = contract.functions.token1().call()
                pairs_V2[e["address"]] = (token0, token1)
            else:
                (token0, token1) = pairs_V2[e["address"]]
            if token0.lower() == WETH or token1.lower() == WETH:
                continue
            collect_V2.append({"address": e["address"],
                               "blockNumber": e["blockNumber"],
                               "transactionHash": e["transactionHash"],
                               "token0": token0, "token1": token1,
                               "amount0In": int(e["data"].hex()[:66],0),
                               "amount1In": int("0x"+e["data"].hex()[66:130],0),
                               "amount0Out": int("0x"+e["data"].hex()[130:194],0),
                               "amount1Out": int("0x"+e["data"].hex()[194:258],0),
                               "attacker0": new_transactions[e["transactionHash"]]["attacker"][0],
                               "attacker1": new_transactions[e["transactionHash"]]["attacker"][1],
                               })

    pairs_V3 = {}
    collect_V3 = []
    
    for ii, e in enumerate(new_events):
        if e["topics"][0] == "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" and e["role"] == 1:
        
            if not e["address"] in pairs_V3:
                contract, _ = get_contract_sync(e["address"], w3=run_context["w3"], context=run_context, abi_type="pair")
                token0 = contract.functions.token0().call()
                token1 = contract.functions.token1().call()
                # fee = contract.functions.fee().call()
                pairs_V3[e["address"]] = (token0, token1)
            else:
                (token0, token1) = pairs_V3[e["address"]]
            if token0.lower() == WETH or token1.lower() == WETH:
                continue
            collect_V3.append({"address": e["address"],
                               "blockNumber": e["blockNumber"],
                               "transactionHash": e["transactionHash"],
                               "token0": token0, "token1": token1,
                               "amount0": s64(int(e["data"].hex()[:66],0)),
                               "amount1": s64(int("0x"+e["data"].hex()[66:130],0)),
                               "attacker0": new_transactions[e["transactionHash"]]["attacker"][0],
                               "attacker1": new_transactions[e["transactionHash"]]["attacker"][1],
                               })

    df_V2 = pd.DataFrame.from_records(collect_V2)
    df_V2.to_csv(CSV_DIR + "V2notWETH.csv", index=False)
    
    tokens = list(pd.concat([pd.Series(df_V2.token0.unique()), pd.Series(df_V2.token1.unique())]).unique())
    tokens_dict = {}
    for t in tokens:
        contract, _ = get_contract_sync(t, w3=run_context["w3"], context=run_context, abi_type="pair")
        try:
            tokens_dict[t] = contract.functions.name().call()
        except:
            pass


    df_V3 = pd.DataFrame.from_records(collect_V3)
    df_V3.to_csv(CSV_DIR + "V3notWETH.csv", index=False)
    
    tokens = list(pd.concat([pd.Series(df_V3.token0.unique()), pd.Series(df_V3.token1.unique())]).unique())
    tokens_dict = {}
    for t in tokens:
        contract, _ = get_contract_sync(t, w3=run_context["w3"], context=run_context, abi_type="pair")
        try:
            tokens_dict[t] = contract.functions.name().call()
        except:
            pass


def reports(run_context, new_events, new_transactions, new_bundles, last_block):    # for b in new_bundles:

    stablecoins = {"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(): "USD Coin",
                   "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower(): "Tether USD",
                   "0x6B175474E89094C44Da98b954EedeAC495271d0F".lower(): "Dai"}
    coin_decimals = {"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(): 1e6,
                     "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower(): 1e6,
                     "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599".lower(): 1e8}

    USDWETHpairs = [(min(st, WETH), max(st, WETH)) for st in stablecoins]
    FIXED_WETH_RATE = 2500
    pairs_VXXX = {}

    TOPICS_TO_PROCESS = {
        "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65": "withdraw",
        "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c": "deposit",
        "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": "uniswap_V2",
        "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67": "uniswap_V3",
        "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83": "pancake_V3",
        "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde": "mint",
        "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0": "collect",
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": "transfer",
        }
    tx_to_exclude = ["0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD",
                     "0x6F8e33DE59EcaDae8461122b87BdbD7e0A632BeC",
                     "0xbf1BA985CF1692CaD4f1192270f649Bf1355fBfe",
                     "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
                     "0x364e94A1bF09Fc6498e979e7715Bb13fa6e9F807",
                     ]

    def make_properties(event, trnx, token0=None, token1=None):
        if (TOPICS_TO_PROCESS[event] in ["withdraw", "deposit", "transfer"]):
            properties = {"saldo": {"eth": 0},
                    "capital": {"eth": 0},
                    "rates": {},
                    "direct_bribe": 0,
                    "gas_burnt": 0,
                    "gas_overpay": 0,
                    "txs": set(),
                    "mint_burn_V3": 0,
                    "mint_burn_NFT": 0,
                    "uniswap_V2": 0,
                    "uniswap_V3": 0,
                    "pancake_V3": 0,
                    }
            return properties        
        elif TOPICS_TO_PROCESS[event] in ["uniswap_V2", "uniswap_V3", "mint", "collect", "pancake_V3"]:
            properties = {"saldo": {token0: 0, token1: 0, "eth": 0},
                    "capital": {token0: 0, token1: 0, "eth": 0},
                    "rates": {},
                    "direct_bribe": 0,
                    "gas_burnt": 0,
                    "gas_overpay": 0,
                    "txs": set(),
                    "mint_burn_V3": 0,
                    "mint_burn_NFT": 0,
                    "uniswap_V2": 0,
                    "uniswap_V3": 0,
                    "pancake_V3": 0,
                    }
            return properties
    
    def get_two_tokensV2(address):
        if not address in pairs_VXXX:
            try:
                contract, _ = get_contract_sync(address, w3=run_context["w3"], context=run_context, abi_type="pair")
                token0 = contract.functions.token0().call().lower()
                token1 = contract.functions.token1().call().lower()
            except:
                return (None, None)
            pairs_VXXX[address] = (token0, token1)
        return pairs_VXXX[address]

    def get_two_tokensV3(address):
        if not address in pairs_VXXX:
            try:
                contract, _ = get_contract_sync(address, w3=run_context["w3"], context=run_context, abi_type="pool")
                token0 = contract.functions.token0().call().lower()
                token1 = contract.functions.token1().call().lower()
            except:
                return (None, None)
            pairs_VXXX[address] = (token0, token1)
        return pairs_VXXX[address]

    def get_two_tokens_other(address):
        if not address in pairs_VXXX:
            try:
                contract, _ = get_contract_sync(address, w3=run_context["w3"], context=run_context)
                token0 = contract.functions.token0().call().lower()
                token1 = contract.functions.token1().call().lower()
            except:
                return (None, None)
            pairs_VXXX[address] = (token0, token1)
        return pairs_VXXX[address]

    def update_gas(transaction_hash, bundle):
        if not transaction_hash in bundle["txs"]:
            bundle["txs"].add(transaction_hash)
            bundle["gas_burnt"] += new_transactions[transaction_hash]["gas_burnt"]
            bundle["saldo"]["eth"] -= new_transactions[transaction_hash]["gas_burnt"]
            bundle["gas_overpay"] += new_transactions[transaction_hash]["gas_overpay"]
            bundle["saldo"]["eth"] -= new_transactions[transaction_hash]["gas_overpay"]
            if "direct_bribe" in new_transactions[transaction_hash]:
                bundle["direct_bribe"] += new_transactions[transaction_hash]["direct_bribe"]
                bundle["saldo"]["eth"] -= new_transactions[transaction_hash]["direct_bribe"]
            if bundle["saldo"]["eth"] < -bundle["capital"]["eth"]:
                bundle["capital"]["eth"] = -bundle["saldo"]["eth"]
    

    # iiii = 1000
    processed_bundles = {}
    # for ii, e in enumerate(new_events[:iiii]):
    for ii, e in enumerate(new_events):
        # if (e["transactionHash"] == "0xad20b98f98ce90a79c7f92d6879ac3d104e58394eefbdfefb890b23b450bfb5f" and
        #     e["topics"][0] == "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0"):
        #     break
        # else:
        #     continue
        
        if e["role"] != 1:
            continue
        if not e["topics"][0] in TOPICS_TO_PROCESS:
            continue
        # print(ii, TOPICS_TO_PROCESS[e["topics"][0]], e["transactionHash"])

        if new_transactions[e["transactionHash"]]["to"] in tx_to_exclude:
            continue

        if new_transactions[e["transactionHash"]]["to"] in run_context["multisender_attackers"]:
            transaction_from = None
        else:
            transaction_from = new_transactions[e["transactionHash"]]["from"]
        if not (e["blockNumber"], transaction_from,
                          new_transactions[e["transactionHash"]]["to"]) in processed_bundles:
            bundle = new_bundles[(e["blockNumber"], transaction_from,
                              new_transactions[e["transactionHash"]]["to"])].copy()
            processed_bundles[(e["blockNumber"], transaction_from,
                              new_transactions[e["transactionHash"]]["to"])] = bundle
        else:
            bundle = processed_bundles[(e["blockNumber"], transaction_from,
                              new_transactions[e["transactionHash"]]["to"])]

        if TOPICS_TO_PROCESS[e["topics"][0]] == "transfer":

            if not "saldo" in bundle:
                bundle.update(make_properties(e["topics"][0], new_transactions[e["transactionHash"]]))

            if (e["topics"][1] == "0x0000000000000000000000000000000000000000000000000000000000000000" or
                e["topics"][2] == "0x0000000000000000000000000000000000000000000000000000000000000000"):
                bundle["mint_burn_NFT"] += 1
            elif  e["topics"][2][-40:] == e["address"][-40:].lower():
                token = e["address"].lower()
                if not token in bundle["saldo"]:
                    bundle["saldo"][token] = 0
                    bundle["capital"][token] = 0
                try:
                    bundle["saldo"][token] -= int(e["data"].hex(),0)/(coin_decimals[token] if token in coin_decimals else 1e18)
                except:
                    pass

                if bundle["saldo"][token] < -bundle["capital"][token]:
                    bundle["capital"][token] = -bundle["saldo"][token]

            update_gas(e["transactionHash"], bundle)
        elif TOPICS_TO_PROCESS[e["topics"][0]] == "withdraw":

            if not "saldo" in bundle:
                bundle.update(make_properties(e["topics"][0], new_transactions[e["transactionHash"]]))

            if not WETH in bundle["saldo"]:
                bundle["saldo"][WETH] = 0
                bundle["capital"][WETH] = 0

            try:
                amount = int(e["data"].hex(), 0) / 1e18
            except:
                try:
                    amount = int(e["topics"][2], 0) / 1e18
                except:
                    pass

            bundle["saldo"][WETH] -= amount
            bundle["saldo"]["eth"] += amount

            update_gas(e["transactionHash"], bundle)

            if bundle["saldo"][WETH] < -bundle["capital"][WETH]:
                bundle["capital"][WETH] = -bundle["saldo"][WETH]

        elif TOPICS_TO_PROCESS[e["topics"][0]] == "deposit":

            if not "saldo" in bundle:
                bundle.update(make_properties(e["topics"][0], new_transactions[e["transactionHash"]]))

            if not WETH in bundle["saldo"]:
                bundle["saldo"][WETH] = 0
                bundle["capital"][WETH] = 0

            try:
                amount = int(e["data"].hex(), 0) / 1e18
            except:
                try:
                    amount = int(e["topics"][2], 0) / 1e18
                except:
                    pass

            bundle["saldo"][WETH] += amount
            bundle["saldo"]["eth"] -= amount

            update_gas(e["transactionHash"], bundle)

            if bundle["saldo"][WETH] < -bundle["capital"][WETH]:
                bundle["capital"][WETH] = -bundle["saldo"][WETH]

        elif TOPICS_TO_PROCESS[e["topics"][0]] == "mint":
            (token0, token1) = get_two_tokensV3(e["address"])
            if token0 is None:
                continue
            if not "saldo" in bundle:
                bundle.update(make_properties(e["topics"][0], new_transactions[e["transactionHash"]], token0, token1))
            bundle["mint_burn_V3"] += 1
            if not token0 in bundle["saldo"]:
                bundle["saldo"][token0] = 0 
                bundle["capital"][token0] = 0 
            if not token1 in bundle["saldo"]:
                bundle["saldo"][token1] = 0
                bundle["capital"][token1] = 0

            update_gas(e["transactionHash"], bundle)

            bundle["saldo"][token0] -= int("0x"+e["data"].hex()[130:194],0)/(coin_decimals[token0] if token0 in coin_decimals else 1e18)
            bundle["saldo"][token1] -= int("0x"+e["data"].hex()[194:258],0)/(coin_decimals[token1] if token1 in coin_decimals else 1e18)

            if bundle["saldo"][token0] < -bundle["capital"][token0]:
                bundle["capital"][token0] = -bundle["saldo"][token0]
            if bundle["saldo"][token1] < -bundle["capital"][token1]:
                bundle["capital"][token1] = -bundle["saldo"][token1]
            
        elif TOPICS_TO_PROCESS[e["topics"][0]] == "collect":
            (token0, token1) = get_two_tokensV3(e["address"])
            if token0 is None:
                continue
            if not "saldo" in bundle:
                bundle.update(make_properties(e["topics"][0], new_transactions[e["transactionHash"]], token0, token1))
            bundle["mint_burn_V3"] += 1
            if not token0 in bundle["saldo"]:
                bundle["saldo"][token0] = 0 
                bundle["capital"][token0] = 0 
            if not token1 in bundle["saldo"]:
                bundle["saldo"][token1] = 0 
                bundle["capital"][token1] = 0 

            update_gas(e["transactionHash"], bundle)

            bundle["saldo"][token0] += int("0x"+e["data"].hex()[66:130],0)/(coin_decimals[token0] if token0 in coin_decimals else 1e18)
            bundle["saldo"][token1] += int("0x"+e["data"].hex()[130:194],0)/(coin_decimals[token1] if token1 in coin_decimals else 1e18)

            if bundle["saldo"][token0] < -bundle["capital"][token0]:
                bundle["capital"][token0] = -bundle["saldo"][token0]
            if bundle["saldo"][token1] < -bundle["capital"][token1]:
                bundle["capital"][token1] = -bundle["saldo"][token1]

        elif TOPICS_TO_PROCESS[e["topics"][0]] == "uniswap_V2":
            (token0, token1) = get_two_tokensV2(e["address"])
            if token0 is None:
                continue

            if not "saldo" in bundle:
                bundle.update(make_properties(e["topics"][0], new_transactions[e["transactionHash"]], token0, token1))
            bundle[TOPICS_TO_PROCESS[e["topics"][0]]] += 1
            if not token0 in bundle["saldo"]:
                bundle["saldo"][token0] = 0 
                bundle["capital"][token0] = 0 
            if not token1 in bundle["saldo"]:
                bundle["saldo"][token1] = 0 
                bundle["capital"][token1] = 0 

            update_gas(e["transactionHash"], bundle)

            bundle["saldo"][token0] -= int(e["data"].hex()[:66],0)/(coin_decimals[token0] if token0 in coin_decimals else 1e18)
            bundle["saldo"][token0] += int("0x"+e["data"].hex()[130:194],0)/(coin_decimals[token0] if token0 in coin_decimals else 1e18)
            bundle["saldo"][token1] -= int("0x"+e["data"].hex()[66:130],0)/(coin_decimals[token1] if token1 in coin_decimals else 1e18)
            bundle["saldo"][token1] += int("0x"+e["data"].hex()[194:258],0)/(coin_decimals[token1] if token1 in coin_decimals else 1e18)
            r1 = (int("0x"+e["data"].hex()[66:130], 0) +  int("0x"+e["data"].hex()[194:258], 0)) * (coin_decimals[token0] if token0 in coin_decimals else 1e18)
            r2 = (int(e["data"].hex()[:66], 0) + int("0x"+e["data"].hex()[130:194], 0)) * (coin_decimals[token1] if token1 in coin_decimals else 1e18)
            # if r1 != 0 and r2 !=0 and not (token0, token1) in bundle["rates"]:
            if r1 != 0 and r2 !=0:
                bundle["rates"][(token0, token1)] =  r1 / r2
                                                     
            if bundle["saldo"][token0] < -bundle["capital"][token0]:
                bundle["capital"][token0] = -bundle["saldo"][token0]
            if bundle["saldo"][token1] < -bundle["capital"][token1]:
                bundle["capital"][token1] = -bundle["saldo"][token1]

        elif TOPICS_TO_PROCESS[e["topics"][0]] in ["uniswap_V3", "pancake_V3"]:
            if TOPICS_TO_PROCESS[e["topics"][0]] == "uniswap_V3":
                (token0, token1) = get_two_tokensV3(e["address"])
            else:
                (token0, token1) = get_two_tokens_other(e["address"])
                
            if token0 is None:
                continue

            if not "saldo" in bundle:
                bundle.update(make_properties(e["topics"][0], new_transactions[e["transactionHash"]], token0, token1))
            bundle[TOPICS_TO_PROCESS[e["topics"][0]]] += 1
            if not WETH in bundle["saldo"]:
                bundle["saldo"][WETH] = 0 
                bundle["capital"][WETH] = 0 
            if not token0 in bundle["saldo"]:
                bundle["saldo"][token0] = 0 
                bundle["capital"][token0] = 0 
            if not token1 in bundle["saldo"]:
                bundle["saldo"][token1] = 0 
                bundle["capital"][token1] = 0 

            update_gas(e["transactionHash"], bundle)

            bundle["saldo"][token0] -= s64(int(e["data"].hex()[:66],0))/(coin_decimals[token0] if token0 in coin_decimals else 1e18)
            bundle["saldo"][token1] -= s64(int("0x"+e["data"].hex()[66:130],0))/(coin_decimals[token1] if token1 in coin_decimals else 1e18)
            r1 = s64(int("0x"+e["data"].hex()[66:130],0)) * (coin_decimals[token0] if token0 in coin_decimals else 1e18)
            r2 = s64(int(e["data"].hex()[:66],0)) * (coin_decimals[token1] if token1 in coin_decimals else 1e18)
            # if r1 != 0 and r2 !=0 and not (token0, token1) in bundle["rates"]:
            if r1 != 0 and r2 !=0:
                bundle["rates"][(token0, token1)] =  abs(r1 / r2)
            if bundle["saldo"][token0] < -bundle["capital"][token0]:
                bundle["capital"][token0] = -bundle["saldo"][token0]
            if bundle["saldo"][token1] < -bundle["capital"][token1]:
                bundle["capital"][token1] = -bundle["saldo"][token1]
        # print(bundle["saldo"])
        # if e["transactionHash"] in ['0x55209539ce6c2c25be3aa2d173a4706ba2cb35761b71541422707ce1c5de29dd', '0x893fead53256eca599e209e25e4f022e92e8ba11d067fb58c3e113791b45e770']:
        #     print(bundle, r1, r2)


    def revert_rate(tokenA, pair, rate):
        if tokenA == pair[0]:
            return rate
        else:
            if rate == 0:
                return 1e100
            else:
                return 1/rate

    def find_rate(tokenA, tokenB, rates):
        # number of tokens B for 1 token A
        # two step max
        if tokenA == tokenB:
            return 1
        pair = (min(tokenA, tokenB), max(tokenA, tokenB))
        if pair in rates:
            return revert_rate(tokenA, pair, rates[pair])
        if tokenA in stablecoins and tokenB in stablecoins:
            return 1
        if tokenA in stablecoins:
            for tokenS in stablecoins:
                if tokenS != tokenA:
                    pair1 = (min(tokenS, tokenB), max(tokenS, tokenB))
                    if pair1 in rates:
                        return revert_rate(tokenS, pair1, rates[pair1])
        if tokenB in stablecoins:
            for tokenS in stablecoins:
                if tokenS != tokenB:
                    pair1 = (min(tokenS, tokenA), max(tokenS, tokenA))
                    if pair1 in rates:
                        return revert_rate(tokenA, pair1, rates[pair1])
        for p in rates:
            if tokenA in p and not tokenB in p:
                if tokenA == p[0]:
                    pair1 = (min(p[1], tokenB), max(p[1], tokenB))
                else:
                    pair1 = (min(p[0], tokenB), max(p[0], tokenB))
                if pair1 in rates:
                    return revert_rate(tokenA, p, rates[p]) / revert_rate(tokenB, pair1, rates[pair1])
                    # return rates[pair]
            elif tokenB in p and not tokenA in p:
                if tokenB == p[0]:
                    pair1 = (min(p[1], tokenA), max(p[1], tokenA))
                else:
                    pair1 = (min(p[0], tokenA), max(p[0], tokenA))
                if pair1 in rates:
                    return revert_rate(tokenA, pair1, rates[pair1]) / revert_rate(tokenB, p, rates[p])
                    # return rates[pair]
        return None

    # new_bundles[(19096571, "0x1264F83b093abbF840eA80a361988D19C7f5a686", "0xb0baBabE78a9be0810fAdf99Dd2eD31ed12568bE")]
    # processed_bundles[(19096571, "0x1264F83b093abbF840eA80a361988D19C7f5a686", "0xb0baBabE78a9be0810fAdf99Dd2eD31ed12568bE")]
    # bundle = new_bundles[(19097060, '0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13', '0x6b75d8AF000000e20B7a7DDf000Ba900b4009A80')]
    # bundle = processed_bundles[(19097060, '0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13', '0x6b75d8AF000000e20B7a7DDf000Ba900b4009A80')]
    
    for ii, b in enumerate(processed_bundles):
        bundle = processed_bundles[b]
        
        bundle["irreducible_tokens"] = False
        bundle["base_token"] = None
        if not "rates" in bundle or len(bundle["rates"]) == 0:
            continue
        bundle["capital_1"] = {}
        if WETH in bundle["capital"]:
            bundle["base_token"] = WETH
        else:
            for st in stablecoins:
                if st in bundle["capital"]:
                    bundle["base_token"] = st
        if bundle["base_token"] is None:
            continue
        
        for st in stablecoins:
            if WETH in bundle["capital"] and st in bundle["capital"]:
                if find_rate(WETH, st, bundle["rates"]) is None:
                    if WETH < st:
                        bundle["rates"][(WETH, st)] = FIXED_WETH_RATE
                    else:
                        bundle["rates"][(st, WETH)] = 1/FIXED_WETH_RATE
            for st1 in stablecoins:
                if st != st1 and st1 in bundle["capital"] and st in bundle["capital"]:
                    if find_rate(st1, st, bundle["rates"]) is None:
                        bundle["rates"][min(st1, st), max(st1, st)] = 1
        
        bundle["total_capital"] = 0
        bundle["total"] = 0
        for c in bundle["capital"]:
            if (c == "eth" or c == WETH) and bundle["base_token"] == WETH:
                rate = 1
            elif bundle["base_token"] == c:
                rate = 1
            else:
                rate = find_rate(bundle["base_token"], c, bundle["rates"])
                if rate is None:
                    bundle["irreducible_tokens"] = True
                    continue
            bundle["capital_1"][c] = bundle["capital"][c] / rate
            bundle["total_capital"] += bundle["capital_1"][c]
            bundle["total"] += bundle["saldo"][c] / rate

        if bundle["base_token"] in stablecoins:
            bundle["total_capital"] = bundle["total_capital"] / FIXED_WETH_RATE
            bundle["total"] = bundle["total"] / FIXED_WETH_RATE
        bundle["eth_capital"] = bundle["capital"]["eth"]
        bundle["eth_total"] = bundle["saldo"]["eth"]
        max_capital = max(list(bundle["capital_1"].values()))
        bundle["start_token"] = list(bundle["capital_1"].keys())[list(bundle["capital_1"].values()).index(max_capital)]
        bundle["complexity"] = len(bundle["transactions"])
        bundle["N_start_tokens"] = len([1 for c in bundle["capital"] if bundle["capital"][c] > 0 and c != 'eth'])

    list_processed_bundles = []
    for b in processed_bundles:
        bundle = processed_bundles[b]
        if not "rates" in bundle or len(bundle["rates"]) == 0:
            continue
        try:
            pb = {'block_number': b[0],
                 'attacker0': b[1],
                 'attacker1': b[2],
                 'tx_counter': bundle['tx_counter'],
                 'base_token': bundle['base_token'],
                 'start_token': bundle['start_token'],
                 'complexity': bundle['complexity'],
                 "transactions": bundle['transactions'],
                 'mint_burn_V3': bundle['mint_burn_V3'],
                 "mint_burn_NFT": bundle["mint_burn_NFT"],
                 'uniswap_V2': bundle['uniswap_V2'],
                 'uniswap_V3': bundle['uniswap_V3'],
                 'pancake_V3': bundle['pancake_V3'],
                 'N_start_tokens': bundle['N_start_tokens'],
                 "irreducible_tokens": (1 if bundle["irreducible_tokens"] else 0),

                 'total_capital': bundle['total_capital'],
                 'eth_capital': bundle['eth_capital'],
                 'direct_bribe': bundle['direct_bribe'],
                 'gas_burnt': bundle['gas_burnt'],
                 'gas_overpay': bundle['gas_overpay'],
                 'total': bundle['total'],
                 'eth_total': bundle['eth_total'],
                  }
            pb["before_bribes"] = bundle['total'] + bundle['direct_bribe'] + bundle['gas_overpay']
            if pb["before_bribes"] > 0:
                pb["bribes_ratio"] = (bundle['direct_bribe'] + bundle['gas_overpay']) / pb["before_bribes"]
            list_processed_bundles.append(pb)
        except:
            continue

    df = pd.DataFrame.from_records(list_processed_bundles)
    df.to_csv(CSV_DIR + "bundles_history_V2_V3" +str(last_block)+ ".csv", index=False)

    # q = df.corr()

    # tx_hashes = ['0xa4614b49b4bc6e35fe1344e3f6da918ca11f3f307a28bd9c62cd0d7c24f3793e', '0xf60055fb86168921e27ec824c569e272d43687448e24f0bc41609df1294772a2']
    # for e in new_events:
    #     if e["transactionHash"] in tx_hashes:
    #         if e["topics"][0] in TOPICS_TO_PROCESS:
    #             print(e)


    # b = 19224963, "0xe75eD6F453c602Bd696cE27AF11565eDc9b46B0D", "0x00000000009E50a7dDb7a7B0e2ee6604fd120E49"
    # processed_bundles[b]
    

    list_analytics = []
    for b in processed_bundles:
        bundle = processed_bundles[b]
        if not "rates" in bundle or len(bundle["rates"]) == 0:
            continue
        try:
            pb = {'block_number': b[0],
                 'attacker0': b[1],
                 'attacker1': b[2],
                 'is_jared': b[2] == "0x6b75d8AF000000e20B7a7DDf000Ba900b4009A80",
                 'thick_sandwich': bundle['tx_counter'] > 1,
                 'base_token_weth': bundle['base_token'] == WETH,
                 'base_stablecoin': bundle['base_token'] in stablecoins,
                 'other_base_token': not (bundle['base_token'] in stablecoins) and bundle['base_token'] != WETH,
                 'total_capital<01': bundle['total_capital'] < 0.1,
                 'total_capital01-1': bundle['total_capital'] < 1 and bundle['total_capital'] > 0.1,
                 'total_capital1-10': bundle['total_capital'] < 10 and bundle['total_capital'] > 1,
                 'total_capital10-100': bundle['total_capital'] < 100 and bundle['total_capital'] > 10,
                 'total_capital10-1000': bundle['total_capital'] < 1000 and bundle['total_capital'] > 100,
                 'total_capital>1000': bundle['total_capital'] > 1000,
                 'gas<01': bundle['gas_burnt'] < 0.1,
                 'gas>01': bundle['gas_burnt'] > 0.1,
                 'start_token_not_weth': bundle['start_token'] != WETH,
                 'start_token_stablecoin': bundle['start_token'] in stablecoins,
                 'complex_bundle': bundle['complexity'] > 2,
                 '2_start_tokens': bundle['N_start_tokens'] > 2,
                 '3_start_tokens': bundle['N_start_tokens'] > 3,
                 'mint_burn_V3': bundle['mint_burn_V3'] > 0,
                 "mint_burn_NFT": bundle["mint_burn_NFT"] > 0,
                 'uniswap_V2': bundle['uniswap_V2'] > 0,
                 'uniswap_V3': bundle['uniswap_V3'] > 0,
                 'pancake_V3': bundle['pancake_V3'] > 0,
                 "irreducible_tokens": bundle["irreducible_tokens"],

                 'direct_bribe': bundle['direct_bribe'],
                 'gas_burnt': bundle['gas_burnt'],
                 'gas_overpay': bundle['gas_overpay'],
                 'total': bundle['total'],
                  }
            pb["before_bribes"] = bundle['total'] + bundle['direct_bribe'] + bundle['gas_overpay']
            if pb["before_bribes"] > 0:
                pb["bribes_ratio"] = (bundle['direct_bribe'] + bundle['gas_overpay']) / pb["before_bribes"]
            list_analytics.append(pb)
        except:
            continue

    df_analytics = pd.DataFrame.from_records(list_analytics, index=['block_number', 'attacker0', 'attacker1'])

    columns = ['is_jared', 'thick_sandwich', 'base_token_weth', 'base_stablecoin',
       'other_base_token', 'total_capital<01', 'total_capital01-1',
       'total_capital1-10', 'total_capital10-100', 'total_capital10-1000',
       'total_capital>1000', 'gas<01', 'gas>01', 'start_token_not_weth',
       'start_token_stablecoin', 'complex_bundle', '2_start_tokens',
       '3_start_tokens', 'mint_burn_V3', 'mint_burn_NFT', 'uniswap_V2',
       'uniswap_V3', 'pancake_V3', 'irreducible_tokens']

    report = []
    win_count = df_analytics.loc[(df["total"]>0), "total"].count()

    ccc = {
        "feature": None,
        "count": df_analytics["total"].count(),
        "share": df_analytics["total"].count()/len(df),
        "win_ratio_if_true": win_count / (len(df_analytics) - win_count),
        "win_ratio_if_false": None,
        "bribes_if_true": df_analytics["bribes_ratio"].mean(),
        "bribes_if_false": None,
        "bribes_if_true_success_only": df_analytics.loc[df_analytics["total"]>0, "bribes_ratio"].mean(),
        "bribes_if_false_success_only": None,
        }
    report.append(ccc)

    for c in columns:
        count_if_true = df_analytics.loc[df_analytics[c], "total"].count()
        count_if_false = df_analytics.loc[~df_analytics[c], "total"].count()
        win_count_if_true = df_analytics.loc[df_analytics[c] & (df_analytics["total"]>0), "total"].count()
        win_count_if_false = df_analytics.loc[~df_analytics[c] & (df_analytics["total"]>0), "total"].count()
        
        ccc = {
            "feature": c,
            "count": count_if_true,
            "share": count_if_true/len(df),
            "win_ratio_if_true": win_count_if_true / (count_if_true - win_count_if_true),
            "win_ratio_if_false": win_count_if_false / (count_if_false - win_count_if_false),
            "bribes_if_true": df_analytics.loc[df_analytics[c], "bribes_ratio"].mean(),
            "bribes_if_false": df_analytics.loc[~df_analytics[c], "bribes_ratio"].mean(),
            "bribes_if_true_success_only": df_analytics.loc[df_analytics[c] & (df_analytics["total"]>0), "bribes_ratio"].mean(),
            "bribes_if_false_success_only": df_analytics.loc[~df_analytics[c] & (df_analytics["total"]>0), "bribes_ratio"].mean(),
            # "best_searcher": df_analytics.loc[df_analytics[c]].groupby(["attacker1"])["bribes_ratio"].mean().idxmin()
            }
        report.append(ccc)
        # print(c, ccc["best_searcher"])           bundle["saldo"][token0] += int("0x"+e["data"].hex()[66:130],0)/(coin_decimals[token0] if token0 in coin_decimals else 1e18)
            bundle["saldo"][token1] += int("0x"+e["data"].hex()[130:194],0)/(coin_decimals[token1] if token1 in coin_decimals else 1e18)


    for c in columns:
        for cc in columns:
            count_if_true = df_analytics.loc[df_analytics[c] & df_analytics[cc], "total"].count()
            count_if_false = df_analytics.loc[~(df_analytics[c] & df_analytics[cc]), "total"].count()
            win_count_if_true = df_analytics.loc[df_analytics[c] & df_analytics[cc] & (df_analytics["total"]>0), "total"].count()
            win_count_if_false = df_analytics.loc[~(df_analytics[c] & df_analytics[cc]) & (df_analytics["total"]>0), "total"].count()
            
            ccc = {
                "feature": c + "+" + cc,
                "count": count_if_true,
                "share": count_if_true/len(df),
                "win_ratio_if_true": win_count_if_true / (count_if_true - win_count_if_true),
                "win_ratio_if_false": win_count_if_false / (count_if_false - win_count_if_false),
                "bribes_if_true": df_analytics.loc[df_analytics[c] & df_analytics[cc], "bribes_ratio"].mean(),
                "bribes_if_false": df_analytics.loc[~(df_analytics[c] & df_analytics[cc]), "bribes_ratio"].mean(),
                "bribes_if_true_success_only": df_analytics.loc[df_analytics[c] & df_analytics[cc] & (df_analytics["total"]>0), "bribes_ratio"].mean(),
                "bribes_if_false_success_only": df_analytics.loc[~(df_analytics[c] & df_analytics[cc]) & (df_analytics["total"]>0), "bribes_ratio"].mean(),
                }
            report.append(ccc)

    df_report = pd.DataFrame.from_records(report)
    df_report.to_csv(CSV_DIR + "bundles_summary" +str(last_block)+ ".csv", index=False)


    # for a in new_attackers:
    #     if a[1] == "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD":
    #         print(a)

    # for e in new_events:
    #     if e["topics"][0] in TOPICS_TO_PROCESS and TOPICS_TO_PROCESS[e["topics"][0]] == "deposit" and e["role"] == 1:
    #         if e["data"].hex() == "0x":
    #             print(e)
    
    report = []
    for a in df_analytics.groupby(["attacker1"])["total"].count().sort_values(ascending=False).index[:21]:
        for c in columns[1:]:
            count_if_true = df_analytics.loc[df_analytics[c] & (df_analytics.index.get_level_values('attacker1') == a), "total"].count()
            count_if_false = df_analytics.loc[~(df_analytics[c] & (df_analytics.index.get_level_values('attacker1') == a)), "total"].count()
            win_count_if_true = df_analytics.loc[df_analytics[c] & (df_analytics.index.get_level_values('attacker1') == a) & (df_analytics["total"]>0), "total"].count()
            win_count_if_false = df_analytics.loc[~(df_analytics[c] & (df_analytics.index.get_level_values('attacker1') == a)) & (df_analytics["total"]>0), "total"].count()
            
            ccc = {
                "attacker": a,
                "feature": c,
                "count": count_if_true,
                "share": count_if_true/len(df),
                "win_ratio_if_true": win_count_if_true / (count_if_true - win_count_if_true),
                "win_ratio_if_false": win_count_if_false / (count_if_false - win_count_if_false),
                "bribes_if_true": df_analytics.loc[df_analytics[c] & (df_analytics.index.get_level_values('attacker1') == a), "bribes_ratio"].mean(),
                "bribes_if_false": df_analytics.loc[(~df_analytics[c]) & (df_analytics.index.get_level_values('attacker1') == a), "bribes_ratio"].mean(),
                "bribes_if_true_success_only": df_analytics.loc[df_analytics[c] & (df_analytics.index.get_level_values('attacker1') == a) & (df_analytics["total"]>0), "bribes_ratio"].mean(),
                "bribes_if_false_success_only": df_analytics.loc[(~df_analytics[c]) & (df_analytics.index.get_level_values('attacker1') == a) & (df_analytics["total"]>0), "bribes_ratio"].mean(),
                }
            report.append(ccc)

    df_report = pd.DataFrame.from_records(report)
    df_report.to_csv(CSV_DIR + "attackers_summary" +str(last_block)+ ".csv", index=False)


   
    report = []
    for a in df_analytics.groupby(["attacker1"])["total"].count().sort_values(ascending=False).index[:21]:
        for c in columns[1:]:
            count_if_true = df_analytics.loc[df_analytics[c] & (df_analytics.index.get_level_values('attacker1') == a), "total"].count()
            count_if_false = df_analytics.loc[~(df_analytics[c] & (df_analytics.index.get_level_values('attacker1') == a)), "total"].count()
            win_count_if_true = df_analytics.loc[df_analytics[c] & (df_analytics.index.get_level_values('attacker1') == a) & (df_analytics["total"]>0), "total"].count()
            win_count_if_false = df_analytics.loc[~(df_analytics[c] & (df_analytics.index.get_level_values('attacker1') == a)) & (df_analytics["total"]>0), "total"].count()
            
            ccc = {
                "attacker": a,
                "feature": c,
                "count": count_if_true,
                "share": count_if_true/len(df),
                "win_ratio_if_true": win_count_if_true / (count_if_true - win_count_if_true),
                "win_ratio_if_false": win_count_if_false / (count_if_false - win_count_if_false),
                "total_if_true": df_analytics.loc[df_analytics[c] & (df_analytics.index.get_level_values('attacker1') == a), "total"].sum(),
                "total_if_false": df_analytics.loc[(~df_analytics[c]) & (df_analytics.index.get_level_values('attacker1') == a), "total"].sum(),
                }
            report.append(ccc)

    df_report = pd.DataFrame.from_records(report)
    df_report.to_csv(CSV_DIR + "attackers_totals" +str(last_block)+ ".csv", index=False)

    df_attackers_list = df.loc[(df["mint_burn_V3"]>0) & ((df["uniswap_V2"]==1) | (df["uniswap_V3"]==1)) & (df["start_token"] == WETH)]
    df_attackers_list.set_index(["block_number", "attacker0", "attacker1"], inplace=True, drop = False)

    super_inners = []
    current_transaction_hash = ""
    for e in new_events:
        if (new_transactions[e["transactionHash"]]["block_number"],
            new_transactions[e["transactionHash"]]["attacker"][0],
            new_transactions[e["transactionHash"]]["attacker"][1]) in df_attackers_list.index and e["role"] == 0:

            if current_transaction_hash != e["transactionHash"]:
                current_transaction_hash = e["transactionHash"]
                super_inner_empty = {"block_number": new_transactions[e["transactionHash"]]["block_number"],
                                     "attacker0": new_transactions[e["transactionHash"]]["attacker"][0],
                                     "attacker1": new_transactions[e["transactionHash"]]["attacker"][1],
                                     "transactionHash": e["transactionHash"]}
            else:
                super_inner_empty = None

            if not e["topics"][0] in TOPICS_TO_PROCESS:
                continue
    
            if new_transactions[e["transactionHash"]]["to"] in tx_to_exclude:
                continue
    
    
            super_inner = {"block_number": new_transactions[e["transactionHash"]]["block_number"],
                                     "attacker0": new_transactions[e["transactionHash"]]["attacker"][0],
                                     "attacker1": new_transactions[e["transactionHash"]]["attacker"][1],
                                     "transactionHash": e["transactionHash"]}

            if TOPICS_TO_PROCESS[e["topics"][0]] == "mint":
                (token0, token1) = get_two_tokensV3(e["address"])
                super_inner["event"] = TOPICS_TO_PROCESS[e["topics"][0]]
                super_inner["token0"] = token0
                super_inner["token1"] = token1
                super_inner["amount0"] = int("0x"+e["data"].hex()[130:194],0)/(coin_decimals[token0] if token0 in coin_decimals else 1e18)
                super_inner["amount1"] = int("0x"+e["data"].hex()[194:258],0)/(coin_decimals[token1] if token1 in coin_decimals else 1e18)
                if not super_inner_empty is None:
                    super_inners.append(super_inner_empty)
                super_inners.append(super_inner)
 
            elif TOPICS_TO_PROCESS[e["topics"][0]] == "collect" :
                (token0, token1) = get_two_tokensV3(e["address"])
                super_inner["event"] = TOPICS_TO_PROCESS[e["topics"][0]]
                super_inner["token0"] = token0
                super_inner["token1"] = token1
                super_inner["amount0"] = int("0x"+e["data"].hex()[66:130],0)/(coin_decimals[token0] if token0 in coin_decimals else 1e18)
                super_inner["amount1"] = int("0x"+e["data"].hex()[130:194],0)/(coin_decimals[token1] if token1 in coin_decimals else 1e18)
                if not super_inner_empty is None:
                    super_inners.append(super_inner_empty)
                super_inners.append(super_inner)

            elif TOPICS_TO_PROCESS[e["topics"][0]] in ["uniswap V3", "pancake V3"]:
                (token0, token1) = get_two_tokensV3(e["address"])
                super_inner["event"] = TOPICS_TO_PROCESS[e["topics"][0]]
                super_inner["token0"] = token0
                super_inner["token1"] = token1
                super_inner["amount0"] = s64(int(e["data"].hex()[:66],0))/(coin_decimals[token0] if token0 in coin_decimals else 1e18)
                super_inner["amount1"] = s64(int("0x"+e["data"].hex()[66:130],0))/(coin_decimals[token1] if token1 in coin_decimals else 1e18)
                if not super_inner_empty is None:
                    super_inners.append(super_inner_empty)
                super_inners.append(super_inner)

            elif TOPICS_TO_PROCESS[e["topics"][0]] == "uniswap_V2":
                (token0, token1) = get_two_tokensV3(e["address"])
                super_inner["event"] = TOPICS_TO_PROCESS[e["topics"][0]]
                super_inner["token0"] = token0
                super_inner["token1"] = token1
                super_inner["amount0In"] = int(e["data"].hex()[:66],0)/(coin_decimals[token0] if token0 in coin_decimals else 1e18)
                super_inner["amount0Out"] = int("0x"+e["data"].hex()[130:194],0)/(coin_decimals[token0] if token0 in coin_decimals else 1e18)
                super_inner["amount1In"] = int("0x"+e["data"].hex()[66:130],0)/(coin_decimals[token1] if token1 in coin_decimals else 1e18)
                super_inner["amount1Out"] = int("0x"+e["data"].hex()[194:258],0)/(coin_decimals[token1] if token1 in coin_decimals else 1e18)
                if not super_inner_empty is None:
                    super_inners.append(super_inner_empty)
                super_inners.append(super_inner)

    df_super_inners = pd.DataFrame.from_records(super_inners)
    df_super_inners.to_csv(CSV_DIR + "super_inners" +str(last_block)+ ".csv", index=False)