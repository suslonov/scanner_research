#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class Servers():
    rsynergy1_mysql = {"ssh_host": "crowd.mil.r-synergy.com",
                       "ssh_port": 22,
                       "ssh_username": "toshick",
                       "ssh_pkey": "~/.ssh/id_rsa",
                       "local_bind_address": None,
                       "bind_address": '127.0.0.1',
                       "bind_port": 3306,
                       "SSH_TIMEOUT": 30.0}

    rsynergy2_mysql = {"ssh_host": "ramses.mil.r-synergy.com",
                       "ssh_port": 22,
                       "ssh_username": "anton",
                       "ssh_pkey": "~/.ssh/id_rsa",
                       "local_bind_address": None,
                       "bind_address": '127.0.0.1',
                       "bind_port": 3306,
                       "SSH_TIMEOUT": 30.0}

    aws_215_mysql = {"ssh_host": "10.0.1.215",
                       "ssh_port": 22,
                       "ssh_username": "ubuntu",
                       "ssh_pkey": "~/.ssh/id_rsa",
                       "local_bind_address": None,
                       "bind_address": '127.0.0.1',
                       "bind_port": 3306,
                       "SSH_TIMEOUT": 30.0}

    rsynergy_mysqlconnect = {"ssh_host": "crowd.mil.r-synergy.com",
                       "ssh_port": 22,
                       "ssh_username": "sqlconnect",
                       "ssh_pkey": "~/.ssh/id_rsa",
                       "local_bind_address": None,
                       "bind_address": '127.0.0.1',
                       "bind_port": 3306,
                       "SSH_TIMEOUT": 30.0}

    rsynergy2_mysqlconnect = {"ssh_host": "ramses.mil.r-synergy.com",
                       "ssh_port": 22,
                       "ssh_username": "sqlconnect",
                       "ssh_pkey": "~/.ssh/id_rsa",
                       "local_bind_address": None,
                       "bind_address": '127.0.0.1',
                       "bind_port": 3306,
                       "SSH_TIMEOUT": 30.0}

    aws_ib = {"ssh_host": "10.0.1.153",
              "ssh_port": 22,
              "ssh_username": "ubuntu",
              "ssh_pkey": "~/.ssh/id_rsa",
              "local_bind_address": ('127.0.0.1', None),
              "bind_address": '127.0.0.1',
              "bind_port": 4002,
              "SSH_TIMEOUT": 30.0}

    aws_ib_live = {"ssh_host": "10.0.1.215",
              "ssh_port": 22,
              "ssh_username": "ubuntu",
              "ssh_pkey": "~/.ssh/id_rsa",
              "local_bind_address": ('127.0.0.1', None),
              "bind_address": '127.0.0.1',
              "bind_port": 4001,
              "SSH_TIMEOUT": 30.0}

#ports = {4001: "Live IB gateway", 7496: "Live IB TWS", 4002: "Paper trading IB gateway", 7497: "Paper trading IB TWS"}
