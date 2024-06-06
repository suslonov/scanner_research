#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
import json
from web3 import Web3
from _utils.etherscan import get_contract_sync
from db.bot_db import DBMySQL
from db.remote import open_remote_port, close_remote_port, RemoteServer
from _utils.utils import HTTPProviderCached
from requests_cache import CachedSession, SQLiteCache, NEVER_EXPIRE

PARAMETERS_FILE = "~/git/scanner_research/llm_contract_scanner/parameters.json"

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)


def remote_server_from_parameters():
    if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
        return parameters["DB_SERVER"]
    else:
        return None

remote = remote_server_from_parameters()

# with RemoteServer(remote=remote) as server:
#     with DBMySQL(port=server.local_bind_port) as db:
#         db.create_tables("t_tokens")


KEY_FILE = '../keys/alchemy.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
REQUEST_CACHE = '/media/Data/eth/eth'

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    w3_url = k1.strip('\n')
    k2 = f.readline()
    w3_wss = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    ETHERSCAN_KEY = k1.strip('\n')

def connect(w3_url):
    w3_0 = Web3(Web3.HTTPProvider(w3_url))
    latest_block = w3_0.eth.get_block("latest")
    backend = SQLiteCache(REQUEST_CACHE)
    session = CachedSession(backend=backend, expire_after=NEVER_EXPIRE, allowable_methods=('GET', 'POST', 'HEAD'))
    w3_1 = Web3(HTTPProviderCached(w3_url, request_kwargs={'timeout': 1e9}, session=session))
    return w3_0, session, w3_1, latest_block


w3_direct, cached_session, w3, latest_block = connect(w3_url)
abi_storage = {}
contract_storage = {}
run_context = {"w3_direct": w3_direct, 
                    "cached_session": cached_session, 
                    "w3": w3,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "contract_storage": contract_storage,
                    }


with RemoteServer(remote=remote) as server:
    with DBMySQL(port=server.local_bind_port) as db:
        l = db.get_tokens_without_property("_maxWalletSize")

with RemoteServer(remote=remote) as server:
    with DBMySQL(port=server.local_bind_port) as db:
        for i, ll in enumerate(l[::-1]):
            contract_code = db.get_contract_code(ll["token"])[1]
            if not contract_code or not "_maxWalletSize" in contract_code:
                continue
            contract, _ = get_contract_sync(ll["token"], w3=w3, session=cached_session, context=run_context, abi_type="token")
            if "_maxWalletSize" in contract.functions:
                try:
                    _maxWalletSize = contract.functions._maxWalletSize().call()
                    db.update_json("t_tokens", ll["token"], "properties", {"_maxWalletSize": _maxWalletSize}, "token")
                except:
                    pass
            elif "_maxWalletToken" in contract.functions:
                try:
                    _maxWalletSize = contract.functions._maxWalletToken().call()
                    db.update_json("t_tokens", ll["token"], "properties", {"_maxWalletSize": _maxWalletSize}, "token")
                except:
                    pass
            else:
                continue
            print(i)


with RemoteServer(remote=remote) as server:
    with DBMySQL(port=server.local_bind_port) as db:
        qq = db.get_tokens_with_property("_maxWalletSize")
with RemoteServer(remote=remote) as server:
    with DBMySQL(port=server.local_bind_port) as db:
        l = db.get_tokens_without_property("_maxWalletSize")

