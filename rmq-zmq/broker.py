#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Broker documentation.

This scripts implements a broker using ZeroMQ to receive and route messages.

Example:
    Broker usage example as follows:
    usage: python borker.py

"""

__author__ = "Nicolas Estrada"
__version__ = "0.0.1"

import zmq

# Getting context and defining bindings
context = zmq.Context()

rcv = context.socket(zmq.PULL)
pub = context.socket(zmq.XPUB)

rcv.bind("tcp://*:10001")
pub.bind("tcp://*:12001")

try: 
    # Broker (receive and deliver)
    while True:
        rkey, message = rcv.recv_multipart()
        print("Received and sending message [%s] RKEY: [%s]" % (message, rkey))

        # processing, persistence?, ACK handling ?

        pub.send_multipart([rkey, message])
except:
    rcv.close()
    pub.close()
    context.term()