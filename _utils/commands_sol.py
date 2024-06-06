#!/usr/bin/env python3
# -*- coding, utf-8 -*-

UNISWAP_UNIVERSAL_ROUTER_COMMANDS = [
    ("V3_SWAP_EXACT_IN", 0x00, ["address", "uint256", "uint256", "bytes", "bool"]),
    ("V3_SWAP_EXACT_OUT", 0x01, ["address", "uint256", "uint256", "bytes", "bool"]),
    ("PERMIT2_TRANSFER_FROM", 0x02),
    ("PERMIT2_PERMIT_BATCH", 0x03),
    ("SWEEP", 0x04),
    ("TRANSFER", 0x05),
    ("PAY_PORTION", 0x06),
    ("V2_SWAP_EXACT_IN", 0x08, ["address", "uint256", "uint256", "address[]", "bool"]),
    ("V2_SWAP_EXACT_OUT", 0x09, ["address", "uint256", "uint256", "address[]", "bool"]),
    ("PERMIT2_PERMIT", 0x0a),
    ("WRAP_ETH", 0x0b),
    ("UNWRAP_WETH", 0x0c),
    ("PERMIT2_TRANSFER_FROM_BATCH", 0x0d),
    ("BALANCE_CHECK_ERC20", 0x0e),
    ("SEAPORT_V1_5", 0x10),
    ("LOOKS_RARE_V2", 0x11),
    ("NFTX", 0x12),
    ("CRYPTOPUNKS", 0x13),
    ("OWNER_CHECK_721", 0x15),
    ("OWNER_CHECK_1155", 0x16),
    ("SWEEP_ERC721", 0x17),
    ("X2Y2_721", 0x18),
    ("SUDOSWAP", 0x19),
    ("NFT20", 0x1a),
    ("X2Y2_1155", 0x1b),
    ("FOUNDATION", 0x1c),
    ("SWEEP_ERC1155", 0x1d),
    ("ELEMENT_MARKET", 0x1e),
    ("SEAPORT_V1_4", 0x20),
    ("EXECUTE_SUB_PLAN", 0x21),
    ("APPROVE_ERC20", 0x22),
]

def uniswap_universal_router_command_to_code(command):
    for p in UNISWAP_UNIVERSAL_ROUTER_COMMANDS:
        if p[0] == command:
            return p[1]
    return None

def uniswap_universal_router_command_abi(command):
    for p in UNISWAP_UNIVERSAL_ROUTER_COMMANDS:
        if p[0] == command and len(p) > 2:
            return p[2]
    return None

def uniswap_universal_router_code_to_command(code):
    for p in UNISWAP_UNIVERSAL_ROUTER_COMMANDS:
        if p[1] == code:
            return p[0]
    return None
