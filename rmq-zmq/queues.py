#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Queues documentation.

This scripts implements a queue using ZeroMQ to receive and deliver messages.

Example:
    Queues usage example as follows:
    usage: python queues.py

"""

__author__ = "Nicolas Estrada"
__version__ = "0.0.1"

import sys

import zmq

from message_profiler import MessageProfiler

# Getting context and defining bindings
context = zmq.Context()

queue = context.socket(zmq.SUB)
pub = context.socket(zmq.PUSH)

queue.connect("tcp://localhost:11001")
queue.setsockopt(zmq.SUBSCRIBE, 'routing_key.example')

pub.bind("tcp://*:12001")

try:
    with MessageProfiler(True) as mp:
        # Broker (receive and deliver)
        while True:
            rkey, message = queue.recv_multipart()
            bytes = sys.getsizeof(rkey + message)
            mp.msg_received(bytes)

            # print("Received and sending message [%s] RKEY: [%s]" % (message, rkey))
            # processing, persistence?, ACK handling ?

            pub.send_multipart([rkey, message])
            mp.msg_sent(bytes)
except:
    queue.close()
    pub.close()
    context.term()
