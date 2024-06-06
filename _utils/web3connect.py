#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from typing import Any
from web3 import Web3
from web3 import HTTPProvider
from web3.types import RPCEndpoint
from web3._utils.encoding import FriendlyJsonSerde
from eth_utils import to_bytes
from requests_cache import CachedSession, SQLiteCache, NEVER_EXPIRE

REQUEST_CACHE = '/media/Data/eth/eth'


class HTTPProviderCached(HTTPProvider):
    def encode_rpc_request(self, method: RPCEndpoint, params: Any) -> bytes:
        rpc_dict = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or [],
            "id": 1,
        }
        encoded = FriendlyJsonSerde().json_encode(rpc_dict)
        return to_bytes(text=encoded)

def web3connect2(key_file, key_file1=None):

    with open(key_file, 'r') as f:
        k1 = f.readline()
        alchemy_url = k1.strip('\n')
        k2 = f.readline()
        alchemy_wss = k2.strip('\n')
    
    if not key_file1 is None:
        with open(key_file1, 'r') as f:
            k1 = f.readline()
            alchemy_direct_url = k1.strip('\n')
            k2 = f.readline()
            alchemy_direct_wss = k2.strip('\n')
    else:
        alchemy_direct_url = alchemy_url
        alchemy_direct_wss = alchemy_wss
        
    w3_0 = Web3(Web3.HTTPProvider(alchemy_direct_url))
    latest_block = w3_0.eth.get_block("latest")

    if not key_file1 is None and os.path.exists(REQUEST_CACHE + ".sqlite"):
        backend = SQLiteCache(REQUEST_CACHE)
        session = CachedSession(backend=backend, expire_after=NEVER_EXPIRE, allowable_methods=('GET', 'POST', 'HEAD'))
        w3_1 = Web3(HTTPProviderCached(alchemy_url, request_kwargs={'timeout': 1e9}, session=session))
    else:
        session = None
        w3_1 = w3_0
    
    uris = {"alchemy_url": alchemy_url, "alchemy_wss": alchemy_wss, "alchemy_direct_url": alchemy_direct_url, "alchemy_direct_wss": alchemy_direct_wss}
        
    return w3_0, session, w3_1, latest_block, uris
