#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Feeder documentation.

This scripts implements the feeders using ZeroMQ,
that gets the data from MySQL dbs/shards and publish them to ZeroMQ broker.

usage: feeder.py [-h] [-cf CONFIG_FILE] -pn
                 {auditlog,login,transaction,mapping,check,media,report,fbuser}
                 [-sn {0,1,2,3,4,5,6,7}] [-lu LAST_UPDATED] [-n]



optional arguments:
  -h, --help            show this help message and exit

  -cf CONFIG_FILE, --config_file CONFIG_FILE
                        config file to be used (default:
                        /var/bi/feeders/config/feeders.yaml)

  -pn {auditlog,login,transaction,mapping,check,media,report,fbuser},
  --poller_name {auditlog,login,transaction,mapping,check,media,report,fbuser}
                        poller name to be used (default: None)

  -sn {0,1,2,3,4,5,6,7}, --shard_number {0,1,2,3,4,5,6,7}
                        shard number associated to the poller, required only
                        if the poller gets that from a sharded table (default:
                        -1)

  -lu LAST_UPDATED, --last_updated LAST_UPDATED
                        string datetime in UTC to be used as start point,not
                        required (default look in recovery (default: None)

  -n, --now             Use the actual UTC date. (default: False)

"""

__author__ = "Nicolas Estrada"
__version__ = "0.0.1"
