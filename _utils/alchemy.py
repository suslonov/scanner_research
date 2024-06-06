#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import json

MAX_RETRY = 10
HEADERS = {'Content-Type': "application/json"}

TRACE_TRANSACTION = {
    "id": 3,
    "jsonrpc": "2.0",
    "params": [],
    "method": "trace_transaction"
    }


def trace_transaction(url, tx_hash, session=None):
    TRACE_TRANSACTION["params"] = [tx_hash]
    # try:
    for i in range(MAX_RETRY):
        if session:
            res = session.post(url, headers=HEADERS, data=json.dumps(TRACE_TRANSACTION), force_refresh = (i > 0))
        else:
            res = requests.post(url, headers=HEADERS, data=json.dumps(TRACE_TRANSACTION))
        d = res.json()
        if "result" in d:
            return d["result"]
        else:
            time.sleep(0.1)
    return None
    # except:
    #     print("trace_transaction error", res.status_code)
    #     return None


