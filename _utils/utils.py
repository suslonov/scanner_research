#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading

MAX_RETRY = 10
RED = "\033[1;31m"
GREEN = "\033[0;32m"
BLUE = "\033[0;34m"
RESET_COLOR = "\033[0;0m"

def hex_to_gwei(hex_value):
    try:
        gwei_value = round(int(hex_value, 0)/1000000000, 9)
    except:
        gwei_value = round(int(hex_value)/1000000000, 9)
    return gwei_value

def hex_to_eth(hex_value):
    try:
        eth_value = round(int(hex_value, 0)/1000000000000000000, 18)
    except:
        eth_value = round(int(hex_value)/1000000000000000000, 18)
    return eth_value

def gwei_to_wei(gwei):
    return int(gwei * 1000000000)

def eth_to_wei(eth):
    return int(eth * 1000000000 * 1000000000)

class AtomicInteger():
    def __init__(self, value=0):
        self._value = int(value)
        self._lock = threading.Lock()
        
    def inc(self, d=1):
        with self._lock:
            self._value += int(d)
            return self._value

    def dec(self, d=1):
        return self.inc(-d)    

    def update(self, d):
        with self._lock:
            if self._value < d:
                self._value = d
            return self._value

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, v):
        with self._lock:
            self._value = int(v)
            return self._value

def s64(q):
    return -(q & 0x8000000000000000000000000000000000000000000000000000000000000000) | (q & 0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff)

def wrap_with_try(f, *args, **kwargs):
    try:
        return f(*args, **kwargs)
    except:
        return None

def bytes_to_int(byte_string):
    res = 0
    for i in range(len(byte_string)):
        res = (res << 8) + int(byte_string[i])
    return res
