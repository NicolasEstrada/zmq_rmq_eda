#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Broker documentation.

This scripts implements a broker using ZeroMQ to receive and route messages.

Example:
    Broker usage example as follows:
    usage: python broker.py

"""

__author__ = "Nicolas Estrada"
__version__ = "0.0.1"

import sys

import zmq

from message_profiler import MessageProfiler

# Getting context and defining bindings
context = zmq.Context()

rcv = context.socket(zmq.PULL)
pub = context.socket(zmq.XPUB)

rcv.bind("tcp://*:10001")
pub.bind("tcp://*:12001")

try:
    with MessageProfiler(True) as mp:
        # Broker (receive and deliver)
        while True:
            rkey, message = rcv.recv_multipart()
            bytes = sys.getsizeof(rkey + message)
            mp.msg_received(bytes)

            # print("Received and sending message [%s] RKEY: [%s]" % (message, rkey))
            # processing, persistence?, ACK handling ?

            pub.send_multipart([rkey, message])
            mp.msg_sent(bytes)
except:
    rcv.close()
    pub.close()
    context.term()